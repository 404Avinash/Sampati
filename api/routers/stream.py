"""
decode_sih / api / routers / stream.py
───────────────────────────────────────
WebSocket endpoint for real-time War Room dashboard updates.

Protocol:
  Client → Server  (control messages):
    { "action": "pause" }
    { "action": "resume" }
    { "action": "inject", "scenario": "fan_out" | "fan_in" | "scatter_gather" | "velocity" | "random" }
    { "action": "ping" }

  Server → Client  (event messages):
    { "type": "connected", "message": "..." }
    { "type": "txn_tick",  "txn_id": "...", "graph": {...}, "metrics": {...} }
    { "type": "fraud_alert", "alert": {...}, "metrics": {...} }
    { "type": "metrics_tick", "metrics": {...} }
    { "type": "pong" }
    { "type": "error", "message": "..." }
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status
from config.settings import settings

from confluent_kafka import Consumer, KafkaError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/ws", tags=["websocket"])


class ConnectionManager:
    """Tracks active WebSocket connections and manages fanout broadcasting."""

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        if len(self._connections) >= settings.api.ws_max_connections:
            await ws.accept()
            await ws.send_json({"type": "error", "message": "Too many active connections"})
            await ws.close(code=status.WS_1008_POLICY_VIOLATION)
            logger.warning("WS connection rejected: connection limit reached (%d)", settings.api.ws_max_connections)
            return False

        await ws.accept()
        self._connections.add(ws)
        logger.info("WS client connected | total=%d", len(self._connections))
        return True

    def disconnect(self, ws: WebSocket) -> None:
        self._connections.discard(ws)
        logger.info("WS client disconnected | total=%d", len(self._connections))

    async def send(self, ws: WebSocket, payload: dict) -> None:
        try:
            await ws.send_json(payload)
        except Exception as e:
            logger.debug("WS send failed: %s", e)
            self.disconnect(ws)

    @property
    def active_connections(self) -> int:
        return len(self._connections)

    async def broadcast(self, payload: dict) -> None:
        if not self._connections:
            return
        
        dead_connections = set()
        for ws in self._connections:
            try:
                await ws.send_json(payload)
            except Exception:
                dead_connections.add(ws)
                
        for ws in dead_connections:
            self.disconnect(ws)

connection_manager = ConnectionManager()

_latest_metrics = {}
_recent_alerts = []
_consumer_task = None

async def start_kafka_consumer():
    """Background task to consume events from Kafka and broadcast to WebSockets."""
    global _latest_metrics, _recent_alerts
    
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'websocket_broadcaster',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe(['graph.events', 'txn.verdicts'])
    
    logger.info("WebSocket Kafka Consumer started")
    
    while True:
        try:
            msg = await asyncio.to_thread(consumer.poll, 0.5)
            if msg is None:
                continue
            if msg.error():
                continue
                
            payload = json.loads(msg.value().decode('utf-8'))
            msg_type = payload.get("type")
            
            # Update cache if applicable
            if "metrics" in payload:
                _latest_metrics = payload["metrics"]
            if msg_type == "fraud_alert":
                _recent_alerts.insert(0, payload.get("alert", {}))
                if len(_recent_alerts) > 100:
                    _recent_alerts.pop()
                    
            await connection_manager.broadcast(payload)
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Error in WS Kafka Consumer: %s", e)
            
    consumer.close()

def get_latest_metrics():
    return _latest_metrics

def get_recent_alerts():
    return _recent_alerts


@router.websocket("/stream")
async def websocket_stream(websocket: WebSocket) -> None:
    """WebSocket endpoint for the War Room dashboard."""
    from api.main import get_emitter
    
    accepted = await connection_manager.connect(websocket)
    if not accepted:
        return

    try:
        # Send welcome + initial state
        await websocket.send_json({
            "type":    "connected",
            "message": "Connected to UPI Fraud Prevention War Room",
            "metrics": _latest_metrics,
            "recent_alerts": _recent_alerts[:10],
        })

        # Listen for client control messages
        while True:
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                msg = json.loads(raw)
                action = msg.get("action", "")

                if action == "pause":
                    get_emitter().pause()
                    await websocket.send_json({"type": "ack", "action": "paused"})

                elif action == "resume":
                    get_emitter().resume()
                    await websocket.send_json({"type": "ack", "action": "resumed"})

                elif action == "inject":
                    scenario = msg.get("scenario", "random")
                    # Send an API request or tell the emitter directly
                    # We will implement an async inject method in TransactionEmitter later
                    # For now, acknowledge
                    await websocket.send_json({
                        "type":    "ack",
                        "action":  "injected",
                        "scenario": scenario,
                        "txn_count": 0,
                    })

                elif action == "ping":
                    await websocket.send_json({"type": "pong"})

                else:
                    await websocket.send_json({
                        "type":    "error",
                        "message": f"Unknown action: {action}",
                    })

            except asyncio.TimeoutError:
                # Send heartbeat ping if no messages in 30s
                await websocket.send_json({"type": "heartbeat"})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.warning("WS error: %s", e)
    finally:
        connection_manager.disconnect(websocket)
