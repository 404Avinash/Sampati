"""
decode_sih / core / metrics.py
──────────────────────────────
Prometheus custom metrics for the stream processing pipeline.
"""

from prometheus_client import Counter, Histogram

TXN_PROCESSED_TOTAL = Counter(
    "txn_processed_total",
    "Total number of transactions processed by the worker"
)

FRAUD_ALERTS_TOTAL = Counter(
    "fraud_alerts_total",
    "Total number of fraud alerts generated",
    ["pattern", "verdict"]
)

DETECTION_LATENCY_SECONDS = Histogram(
    "detection_latency_seconds",
    "End-to-end detection latency in seconds",
    ["pattern"]
)

SLA_BREACH_TOTAL = Counter(
    "sla_breach_total",
    "Total number of detections that breached the latency SLA"
)

def record_verdict(pattern: str, verdict: str, latency_ms: float, within_sla: bool) -> None:
    """Record metrics for a fraud verdict."""
    FRAUD_ALERTS_TOTAL.labels(pattern=pattern, verdict=verdict).inc()
    DETECTION_LATENCY_SECONDS.labels(pattern=pattern).observe(latency_ms / 1000.0)
    if not within_sla:
        SLA_BREACH_TOTAL.inc()
