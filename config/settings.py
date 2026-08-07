"""
decode_sih / config / settings.py
──────────────────────────────────
Centralised, type-safe application configuration powered by Pydantic Settings.
All values are read from environment variables or a .env file at project root.
Never hard-code values anywhere else in the codebase — import from here.

Usage:
    from config.settings import settings

    if settings.risk_block_threshold > 0.8:
        ...
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppEnv(StrEnum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class LogLevel(StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class AppSettings(BaseSettings):
    """Top-level application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="APP_",
        case_sensitive=False,
        extra="ignore",
    )

    env: AppEnv = AppEnv.DEVELOPMENT
    log_level: LogLevel = LogLevel.INFO
    host: str = "0.0.0.0"
    port: int = Field(default=8000, ge=1024, le=65535)


class EmitterSettings(BaseSettings):
    """Transaction stream emitter settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="EMITTER_",
        case_sensitive=False,
        extra="ignore",
    )

    tps: Annotated[int, Field(ge=1, le=10_000)] = 100
    """Target transactions per second the emitter will produce."""

    fraud_rate: Annotated[float, Field(ge=0.0, le=1.0)] = 0.02
    """Fraction of emitted transactions that are synthetic fraud injections."""

    dataset_path: Path = Path("data/raw")
    """Directory containing Kaggle CSV dataset files."""

    @field_validator("dataset_path", mode="before")
    @classmethod
    def validate_dataset_path(cls, v: str | Path) -> Path:
        p = Path(v)
        p.mkdir(parents=True, exist_ok=True)
        return p


class GraphSettings(BaseSettings):
    """Behavioral graph engine settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="GRAPH_",
        case_sensitive=False,
        extra="ignore",
    )

    max_nodes: Annotated[int, Field(ge=1000, le=10_000_000)] = 100_000
    """Maximum number of account nodes kept in memory."""

    fanout_threshold: Annotated[int, Field(ge=2, le=100)] = 5
    """Minimum unique recipients within the window to classify as Fan-Out fraud."""

    fanin_threshold: Annotated[int, Field(ge=2, le=100)] = 5
    """Minimum unique senders within the window to classify as Fan-In (mule) fraud."""

    window_seconds: Annotated[int, Field(ge=5, le=3600)] = 60
    """Sliding time window (seconds) used by the graph engine for pattern analysis."""

    scatter_gather_hops: Annotated[int, Field(ge=2, le=10)] = 3
    """Maximum hop depth for Scatter-Gather / Smurfing topology detection."""

    latency_budget_ms: Annotated[int, Field(ge=10, le=5000)] = 200
    """Hard SLA: detection must complete within this many milliseconds."""


class RiskSettings(BaseSettings):
    """Risk scoring and interdiction thresholds."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="RISK_",
        case_sensitive=False,
        extra="ignore",
    )

    block_threshold: Annotated[float, Field(ge=0.0, le=1.0)] = 0.85
    """Risk score at or above which a transaction is BLOCKED outright."""

    flag_threshold: Annotated[float, Field(ge=0.0, le=1.0)] = 0.60
    """Risk score at or above which a transaction is FLAGGED for review."""

    model_path: Path = Path("data/models/risk_scorer.pkl")

    @field_validator("block_threshold")
    @classmethod
    def block_must_exceed_flag(cls, v: float, info: object) -> float:  # noqa: ANN001
        # Can't validate cross-field here easily; do runtime check in scorer
        return v


class APISettings(BaseSettings):
    """API and WebSocket settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="API_",
        case_sensitive=False,
        extra="ignore",
    )

    cors_origins: list[str] = ["http://localhost:3000", "http://localhost:8000"]
    admin_key: str = Field(
        default="demo-secret-key-123",
        description="Static token required for mutating control endpoints",
    )
    ws_max_connections: Annotated[int, Field(ge=1, le=1000)] = 50
    ws_broadcast_interval_ms: Annotated[int, Field(ge=50, le=5000)] = 100


# ─── Singleton instances (import these everywhere) ────────────────────────────

class Settings:
    """
    Aggregated settings container — single import point.

    Example:
        from config.settings import settings
        print(settings.app.port)
        print(settings.graph.fanout_threshold)
    """

    def __init__(self) -> None:
        self.app = AppSettings()
        self.emitter = EmitterSettings()
        self.graph = GraphSettings()
        self.risk = RiskSettings()
        self.api = APISettings()

    def __repr__(self) -> str:
        return (
            f"<Settings env={self.app.env} "
            f"tps={self.emitter.tps} "
            f"fanout_threshold={self.graph.fanout_threshold}>"
        )


settings = Settings()
