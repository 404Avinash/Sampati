"""
decode_sih / pipeline / metrics.py
────────────────────────────────────
Latency tracking and operational metrics for the stream pipeline.

Uses a lock-free circular buffer for O(1) percentile approximation.
No external dependencies (no Prometheus, no StatsD) — pure Python.
Can be wired to those systems later if needed.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field


@dataclass
class LatencyTracker:
    """
    Tracks detection latency using a fixed-size circular buffer.
    Thread-safe via threading.Lock (not asyncio.Lock, since metrics
    may be read from a sync context for logging).
    """

    window_size: int = 1000  # Last N measurements

    _latencies: deque[float] = field(default_factory=lambda: deque(maxlen=1000))
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _sla_budget_ms: float = 200.0
    _breach_count: int = 0
    _total_count: int = 0

    def record(self, latency_ms: float) -> None:
        with self._lock:
            self._latencies.append(latency_ms)
            self._total_count += 1
            if latency_ms > self._sla_budget_ms:
                self._breach_count += 1

    @property
    def avg_ms(self) -> float:
        with self._lock:
            if not self._latencies:
                return 0.0
            return sum(self._latencies) / len(self._latencies)

    @property
    def p50_ms(self) -> float:
        return self._percentile(50)

    @property
    def p95_ms(self) -> float:
        return self._percentile(95)

    @property
    def p99_ms(self) -> float:
        return self._percentile(99)

    @property
    def max_ms(self) -> float:
        with self._lock:
            return max(self._latencies, default=0.0)

    @property
    def sla_compliance_rate(self) -> float:
        with self._lock:
            if self._total_count == 0:
                return 1.0
            return 1.0 - (self._breach_count / self._total_count)

    @property
    def breach_count(self) -> int:
        with self._lock:
            return self._breach_count

    def to_dict(self) -> dict:
        return {
            "avg_ms":              round(self.avg_ms, 2),
            "p50_ms":              round(self.p50_ms, 2),
            "p95_ms":              round(self.p95_ms, 2),
            "p99_ms":              round(self.p99_ms, 2),
            "max_ms":              round(self.max_ms, 2),
            "sla_compliance_rate": round(self.sla_compliance_rate, 4),
            "breach_count":        self.breach_count,
            "total_measured":      self._total_count,
        }

    def _percentile(self, p: int) -> float:
        with self._lock:
            if not self._latencies:
                return 0.0
            sorted_lats = sorted(self._latencies)
            idx = int(len(sorted_lats) * p / 100)
            return sorted_lats[min(idx, len(sorted_lats) - 1)]


@dataclass
class ThroughputCounter:
    """
    Sliding-window TPS counter.
    Maintains counts in 1-second buckets for the last N seconds.
    """

    window_seconds: int = 10

    _buckets: deque[tuple[int, int]] = field(default_factory=deque)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record(self, count: int = 1) -> None:
        now_bucket = int(time.time())
        with self._lock:
            if self._buckets and self._buckets[-1][0] == now_bucket:
                last_bucket, last_count = self._buckets[-1]
                self._buckets[-1] = (last_bucket, last_count + count)
            else:
                self._buckets.append((now_bucket, count))
            # Evict old buckets
            cutoff = now_bucket - self._window_seconds
            while self._buckets and self._buckets[0][0] < cutoff:
                self._buckets.popleft()

    @property
    def _window_seconds(self) -> int:
        return self.window_seconds

    @property
    def current_tps(self) -> float:
        now_bucket = int(time.time())
        cutoff = now_bucket - self.window_seconds
        with self._lock:
            total = sum(cnt for ts, cnt in self._buckets if ts >= cutoff)
        return total / self.window_seconds

    @property
    def total_processed(self) -> int:
        with self._lock:
            return sum(cnt for _, cnt in self._buckets)
