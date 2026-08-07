"""
decode_sih / api / main.py
───────────────────────────
FastAPI application entry point.

Responsibilities:
  - Application factory with async lifespan context manager
  - CORS configuration
  - Router registration
  - Static files (dashboard HTML/CSS/JS)
  - StreamProcessor singleton lifecycle management
  - Global accessor for the processor (avoids circular imports)

Running locally:
    uvicorn api.main:app --reload --port 8000

Or via the convenience script:
    python scripts/run_server.py
"""

from __future__ import annotations

import logging
import logging.config
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

import asyncio
import os
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from prometheus_fastapi_instrumentator import Instrumentator
import time
import json
import redis.asyncio as redis
from confluent_kafka.admin import AdminClient

from config.settings import settings
from pipeline.stream_processor import StreamProcessor

START_TIME = time.time()

if os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.redis import RedisInstrumentor
    from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor

    resource = Resource.create({"service.name": os.getenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=decode_api").split("=")[1]})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    
    RedisInstrumentor().instrument()
    ConfluentKafkaInstrumentor().instrument()

# ─── Logging configuration ────────────────────────────────────────────────────

logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "detailed": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "detailed",
            "stream": "ext://sys.stdout",
        },
    },
    "root": {
        "level": settings.app.log_level.value,
        "handlers": ["console"],
    },
})

logger = logging.getLogger(__name__)

# ─── Processor singleton ──────────────────────────────────────────────────────
# We store it at module level rather than on app.state to avoid the circular
# import that would arise from importing 'app' in the router modules.

from emitter.transaction_emitter import TransactionEmitter

_emitter: TransactionEmitter | None = None
_produce_task: asyncio.Task | None = None
_producer = None

def get_emitter() -> TransactionEmitter:
    if _emitter is None:
        raise RuntimeError("TransactionEmitter not initialised")
    return _emitter


# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Startup: initialise and start the stream processing pipeline.
    Shutdown: gracefully stop the pipeline and cancel the task.
    """
    global _emitter, _produce_task, _producer

    logger.info("=" * 60)
    logger.info("🚀 UPI Fraud Prevention System — API Gateway (Phase 2)")
    logger.info("=" * 60)

    from confluent_kafka import Producer
    from api.routers.stream import start_kafka_consumer
    
    _emitter = TransactionEmitter()
    _producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    _ws_consumer_task = asyncio.create_task(start_kafka_consumer())
    
    async def produce_loop():
        try:
            async for txn in _emitter.stream():
                _producer.produce('txn.incoming', key=txn.sender_id, value=txn.model_dump_json())
                _producer.poll(0)
        except asyncio.CancelledError:
            pass

    _produce_task = asyncio.create_task(produce_loop())

    yield  # Application is running

    logger.info("Shutting down — stopping Emitter and WS Consumer...")
    _emitter.stop()
    if _produce_task and not _produce_task.done():
        _produce_task.cancel()
    if _ws_consumer_task and not _ws_consumer_task.done():
        _ws_consumer_task.cancel()
    if _producer:
        _producer.flush()
    logger.info("✅ System stopped cleanly")


# ─── Application Factory ──────────────────────────────────────────────────────

app = FastAPI(
    title="UPI Fraud Prevention — War Room API",
    description=(
        "Real-time behavioral graph engine for detecting structural UPI fraud topologies. "
        "Built for SIH 2024 PS2: Intelligent UPI Fraud Prevention."
    ),
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from api.routers.control import limiter

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

if os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
    FastAPIInstrumentor.instrument_app(app)

# Instrument FastAPI with Prometheus metrics
Instrumentator().instrument(app).expose(app)

# ─── CORS ─────────────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.api.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Routers ──────────────────────────────────────────────────────────────────

from api.routers import control, dashboard, investigate, stream, ai

app.include_router(control.router)
app.include_router(dashboard.router)
app.include_router(investigate.router)
app.include_router(stream.router)
app.include_router(ai.router)

# ─── Static Files (Dashboard) ─────────────────────────────────────────────────

DASHBOARD_DIR = Path(__file__).parent.parent / "dashboard"
STATIC_DIR    = DASHBOARD_DIR / "static"

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ─── Root Route ───────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
async def serve_dashboard() -> FileResponse:
    """Serve the War Room dashboard HTML."""
    index_path = DASHBOARD_DIR / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))
    # Fallback if dashboard not built yet
    from fastapi.responses import HTMLResponse
    return HTMLResponse(
        content="<h1>War Room API Running</h1><p>See <a href='/docs'>/docs</a></p>",
        status_code=200,
    )


@app.get("/health", tags=["system"])
async def health_check() -> dict:
    """Kubernetes/Docker health check endpoint."""
    uptime = time.time() - START_TIME
    return {"status": "healthy", "version": "0.1.0", "uptime_seconds": round(uptime, 2)}


@app.get("/ready", tags=["system"])
async def readiness_check() -> Response:
    """Readiness probe: checks Redis and Kafka connectivity."""
    status = {"status": "ready", "redis": "ok", "kafka": "ok"}
    try:
        r = redis.from_url(settings.app.redis_url)
        await r.ping()
        await r.close()
    except Exception as e:
        status["redis"] = f"error: {str(e)}"
        status["status"] = "not_ready"
        
    try:
        # Simple Kafka check
        admin = AdminClient({'bootstrap.servers': settings.app.kafka_bootstrap})
        admin.list_topics(timeout=2.0)
    except Exception as e:
        status["kafka"] = f"error: {str(e)}"
        status["status"] = "not_ready"
        
    return Response(
        content=json.dumps(status), 
        status_code=200 if status["status"] == "ready" else 503, 
        media_type="application/json"
    )
