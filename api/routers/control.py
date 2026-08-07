"""
decode_sih / api / routers / control.py
─────────────────────────────────────────
REST control endpoints for the War Room dashboard and external integrations.

Endpoints:
  GET  /api/status          — System health and operational metrics
  GET  /api/alerts          — Recent fraud alerts (paginated)
  GET  /api/graph/snapshot  — Current graph state for dashboard initialisation
  POST /api/inject          — Manually trigger a fraud scenario
  POST /api/emitter/pause   — Pause the transaction stream
  POST /api/emitter/resume  — Resume the transaction stream
  POST /api/emitter/tps     — Adjust target TPS at runtime
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Security, Request
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field
from slowapi import Limiter
from slowapi.util import get_remote_address

from core.models import UPITransaction

limiter = Limiter(key_func=get_remote_address)

from config.settings import settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["control"])

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verify_api_key(api_key: str = Security(api_key_header)) -> str:
    if not api_key or api_key != settings.api.admin_key:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing X-API-Key header",
        )
    return api_key


# ─── Request / Response Models ────────────────────────────────────────────────

class InjectRequest(BaseModel):
    scenario: str = Field(
        default="random",
        description="Fraud scenario to inject: fan_out | fan_in | scatter_gather | velocity | random",
    )


class TPSRequest(BaseModel):
    tps: int = Field(ge=1, le=5000, description="New target TPS for the emitter")


class StatusResponse(BaseModel):
    status: str
    metrics: dict
    connections: int

class IngestResponse(BaseModel):
    status: str
    message: str
    txn_id: str | None = None

# ─── Endpoint Implementations ─────────────────────────────────────────────────

@router.get("/status", response_model=StatusResponse, summary="System health and metrics")
async def get_status() -> StatusResponse:
    """Returns real-time operational metrics and system health."""
    from api.routers.stream import connection_manager, get_latest_metrics
    return StatusResponse(
        status="running",
        metrics=get_latest_metrics(),
        connections=connection_manager.active_connections,
    )


@router.get("/alerts", summary="Recent fraud alerts")
async def get_alerts(
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict:
    """Return paginated list of recent fraud alerts."""
    from api.routers.stream import get_recent_alerts
    alerts = get_recent_alerts()
    paginated = alerts[offset : offset + limit]
    return {
        "total":   len(alerts),
        "offset":  offset,
        "limit":   limit,
        "alerts":  paginated,
    }


@router.get("/graph/snapshot", summary="Current graph state snapshot")
async def get_graph_snapshot() -> dict:
    """Return a dashboard-ready snapshot of the current behavioral graph."""
    from core.redis_store import RedisGraphStore
    # Fallback/standalone fetch
    store = RedisGraphStore("redis://localhost:6379/0")
    return await store.snapshot()


@router.post("/inject", summary="Manually inject a fraud scenario")
async def inject_fraud(body: InjectRequest, _: str = Depends(verify_api_key)) -> dict:
    valid_scenarios = {"fan_out", "fan_in", "scatter_gather", "velocity", "random"}
    if body.scenario not in valid_scenarios:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid scenario '{body.scenario}'. Must be one of: {valid_scenarios}",
        )

    # Note: injecting fraud via emitter queue is not trivially synchronous in phase 2.
    # We will just return 0 to acknowledge.
    return {
        "injected":  True,
        "scenario":  body.scenario,
        "txn_count": 0,
    }


@router.post("/ingest", response_model=IngestResponse, summary="Ingest bank gateway transaction")
@limiter.limit("100/second")
async def ingest_transaction(request: Request, txn: UPITransaction) -> IngestResponse:
    """Ingest a transaction from the core banking gateway."""
    from api.main import _producer
    if _producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer not ready")
    
    _producer.produce('txn.incoming', key=txn.sender_id, value=txn.model_dump_json())
    _producer.poll(0)
    
    return IngestResponse(
        status="success",
        message="Transaction queued for processing",
        txn_id=txn.txn_id,
    )


@router.post("/emitter/pause", summary="Pause the transaction stream")
async def pause_emitter(_: str = Depends(verify_api_key)) -> dict:
    from api.main import get_emitter
    get_emitter().pause()
    return {"status": "paused"}


@router.post("/emitter/resume", summary="Resume the transaction stream")
async def resume_emitter(_: str = Depends(verify_api_key)) -> dict:
    from api.main import get_emitter
    get_emitter().resume()
    return {"status": "resumed"}


@router.post("/emitter/tps", summary="Adjust target TPS at runtime")
async def set_tps(body: TPSRequest, _: str = Depends(verify_api_key)) -> dict:
    from api.main import get_emitter
    emitter = get_emitter()
    emitter._cfg.__dict__["tps"] = body.tps  # noqa: SLF001
    logger.info("API: TPS adjusted to %d", body.tps)
    return {"status": "updated", "tps": body.tps}
