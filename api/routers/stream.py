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

if TYPE_CHECKING:
    from pipeline.stream_processor import StreamProcessor

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


connection_manager = ConnectionManager()


def make_broadcaster(ws: WebSocket) -> "BroadcastFn":
    """
    Create a bound broadcast coroutine for a specific WebSocket connection.
    This is registered with the StreamProcessor so it receives all events.
    """
    async def broadcaster(payload: dict) -> None:
        await connection_manager.send(ws, payload)
    return broadcaster  # type: ignore[return-value]


@router.websocket("/stream")
async def websocket_stream(
    websocket: WebSocket,
    processor: "StreamProcessor | None" = None,
) -> None:
    """
    WebSocket endpoint for the War Room dashboard.

    Dependency injection of the processor is handled via app.state in main.py.
    We use a workaround here since FastAPI WS DI is limited.
    """
    # Access processor from app state (set in main.py lifespan)
    from api.main import get_processor
    proc = get_processor()

    accepted = await connection_manager.connect(websocket)
    if not accepted:
        return

    broadcaster = make_broadcaster(websocket)
    proc.add_broadcaster(broadcaster)

    try:
        # Send welcome + initial state
        await websocket.send_json({
            "type":    "connected",
            "message": "Connected to UPI Fraud Prevention War Room",
            "metrics": proc.metrics,
            "recent_alerts": proc.recent_alerts[:10],
        })

        # Listen for client control messages
        while True:
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                msg = json.loads(raw)
                action = msg.get("action", "")

                if action == "pause":
                    proc.pause()
                    await websocket.send_json({"type": "ack", "action": "paused"})

                elif action == "resume":
                    proc.resume()
                    await websocket.send_json({"type": "ack", "action": "resumed"})

                elif action == "inject":
                    scenario = msg.get("scenario", "random")
                    count = await proc.inject_fraud_scenario(scenario)
                    await websocket.send_json({
                        "type":    "ack",
                        "action":  "injected",
                        "scenario": scenario,
                        "txn_count": count,
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
        proc.remove_broadcaster(broadcaster)
        connection_manager.disconnect(websocket)
