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
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from config.settings import settings
from pipeline.stream_processor import StreamProcessor

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

_processor: StreamProcessor | None = None
_pipeline_task: asyncio.Task | None = None


def get_processor() -> StreamProcessor:
    if _processor is None:
        raise RuntimeError("StreamProcessor not initialised — is the lifespan running?")
    return _processor


# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Startup: initialise and start the stream processing pipeline.
    Shutdown: gracefully stop the pipeline and cancel the task.
    """
    global _processor, _pipeline_task

    logger.info("=" * 60)
    logger.info("🚀 UPI Fraud Prevention System — Starting Up")
    logger.info("=" * 60)
    logger.info("Environment : %s", settings.app.env)
    logger.info("Target TPS  : %d", settings.emitter.tps)
    logger.info("Window      : %ds", settings.graph.window_seconds)
    logger.info("Fan-Out     : ≥%d receivers", settings.graph.fanout_threshold)
    logger.info("Fan-In      : ≥%d senders",   settings.graph.fanin_threshold)
    logger.info("BLOCK at    : %.2f risk score", settings.risk.block_threshold)
    logger.info("=" * 60)

    _processor = StreamProcessor()
    _pipeline_task = asyncio.create_task(_processor.run())

    yield  # Application is running

    logger.info("Shutting down — stopping StreamProcessor...")
    _processor.stop()
    if _pipeline_task and not _pipeline_task.done():
        _pipeline_task.cancel()
        try:
            await asyncio.wait_for(_pipeline_task, timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
    logger.info("✅ StreamProcessor stopped cleanly")


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

# ─── CORS ─────────────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.api.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Routers ──────────────────────────────────────────────────────────────────

from api.routers import control, stream  # noqa: E402 (after app creation to avoid circular)

app.include_router(control.router)
app.include_router(stream.router)

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
    return {"status": "healthy", "version": "0.1.0"}
