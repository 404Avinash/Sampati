"""
decode_sih / pipeline / stream_processor.py
─────────────────────────────────────────────
The Async Stream Processing Pipeline — the central coordinator that wires:

  TransactionEmitter → BehavioralGraphEngine → PatternDetectorRegistry
      → Explainability → Alert Queue → WebSocket Broadcaster

Data flow for each transaction:
  1. INGEST    : Receive transaction from emitter
  2. GRAPH     : Add transaction to behavioral graph (O(1))
  3. DETECT    : Run all pattern detectors concurrently (asyncio.gather)
  4. EXPLAIN   : Enrich any alerts with causal explanation payload
  5. ACT       : Update graph node flags, update transaction status
  6. BROADCAST : Push alert + graph snapshot to all connected WS clients
  7. METRICS   : Record latency, TPS, SLA compliance

The pipeline runs as a long-lived asyncio Task. The FastAPI lifespan
starts it on app startup and cancels it on shutdown.

Performance design decisions:
  - The hot path (steps 1-3) is pure async coroutines, no blocking I/O.
  - Pattern detectors run concurrently — total latency ≈ max(individual latencies).
  - WebSocket broadcast is async fire-and-forget — detection is never blocked
    waiting for slow clients.
  - Graph eviction runs in a separate background task every 30s.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Callable, Coroutine

from config.settings import settings
from core.explainability import build_rich_explanation, format_audit_log_line
from core.graph_engine import BehavioralGraphEngine
from core.storage import InMemoryGraphStore
from core.models import (
    FraudAlert,
    PipelineMetrics,
    RiskVerdict,
    TransactionStatus,
    UPITransaction,
)
from core.pattern_detector import detector_registry
from emitter.transaction_emitter import TransactionEmitter
from pipeline.metrics import LatencyTracker, ThroughputCounter

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Type for WebSocket broadcaster callbacks registered by the API layer
BroadcastFn = Callable[[dict], Coroutine[None, None, None]]


class StreamProcessor:
    """
    The central async stream processing coordinator.

    Usage (in FastAPI lifespan):
        processor = StreamProcessor()
        task = asyncio.create_task(processor.run())
        ...
        processor.stop()
        await task
    """

    def __init__(self) -> None:
        store = InMemoryGraphStore()
        self._graph     = BehavioralGraphEngine(store=store)
        self._emitter   = TransactionEmitter()
        self._running   = False
        self._paused    = False

        # Metrics
        self._latency_tracker   = LatencyTracker(window_size=1000)
        self._throughput_counter = ThroughputCounter(window_seconds=10)
        self._total_processed: int = 0
        self._total_alerts: int    = 0
        self._total_blocked: int   = 0
        self._total_flagged: int   = 0
        self._start_time: float    = 0.0

        # Recent alerts ring buffer (for dashboard initial state)
        self._recent_alerts: deque[dict] = deque(maxlen=100)

        # Broadcaster callbacks registered by WebSocket handler
        self._broadcasters: list[BroadcastFn] = []

        logger.info("StreamProcessor initialised")

    # ─── Lifecycle ────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """
        Main pipeline loop. Runs until stop() is called.
        This should be run as an asyncio.Task.
        """
        self._running    = True
        self._start_time = time.time()
        logger.info("🚀 StreamProcessor started")

        # Start background maintenance tasks
        eviction_task = asyncio.create_task(self._eviction_loop())
        metrics_task  = asyncio.create_task(self._metrics_broadcast_loop())

        try:
            async for txn in self._emitter.stream():
                if not self._running:
                    break
                while self._paused:
                    await asyncio.sleep(0.05)

                await self._process_transaction(txn)

        except asyncio.CancelledError:
            logger.info("StreamProcessor task cancelled")
        except Exception as e:
            logger.exception("StreamProcessor crashed due to an unhandled exception: %s", e)
        finally:
            eviction_task.cancel()
            metrics_task.cancel()
            logger.info(
                "StreamProcessor stopped | processed=%d alerts=%d",
                self._total_processed,
                self._total_alerts,
            )

    def stop(self) -> None:
        self._running = False
        self._emitter.stop()
        logger.info("StreamProcessor stop requested")

    def pause(self) -> None:
        self._paused = True
        self._emitter.pause()

    def resume(self) -> None:
        self._paused = False
        self._emitter.resume()

    # ─── Broadcaster Registration ─────────────────────────────────────────────

    def add_broadcaster(self, fn: BroadcastFn) -> None:
        """Register a WebSocket broadcast callback. Called by the WS handler."""
        self._broadcasters.append(fn)

    def remove_broadcaster(self, fn: BroadcastFn) -> None:
        try:
            self._broadcasters.remove(fn)
        except ValueError:
            pass

    # ─── Core Transaction Processing ─────────────────────────────────────────

    async def _process_transaction(self, txn: UPITransaction) -> None:
        """Execute the full processing pipeline for a single transaction."""
        ingest_start = time.time()

        # ── 1. GRAPH: Add to behavioral graph ────────────────────────────────
        await self._graph.add_transaction(txn)

        # ── 2. DETECT: Run all pattern detectors concurrently ────────────────
        alerts: list[FraudAlert] = await detector_registry.run_all(txn, self._graph)

        # ── 3. PROCESS ALERTS ────────────────────────────────────────────────
        for alert in alerts:
            await self._handle_alert(alert)

        # ── 4. METRICS ───────────────────────────────────────────────────────
        latency_ms = (time.time() - ingest_start) * 1000
        self._latency_tracker.record(latency_ms)
        self._throughput_counter.record(1)
        self._total_processed += 1

        # ── 5. BROADCAST (fire-and-forget, two tiers) ─────────────────────────
        if self._broadcasters:
            # Tier A — Lightweight geo arc payload, EVERY transaction.
            # Keeps the geo map alive at full TPS with minimal overhead.
            # Uses full IDs so the geo hash maps accounts consistently with fraud_alert.accounts.
            geo_payload = {
                "type":     "geo_tick",
                "sender":   txn.sender_id,
                "receiver": txn.receiver_id,
                "amount":   txn.amount_rupees,
            }
            asyncio.create_task(self._broadcast(geo_payload))

            # Tier B — Full txn_tick with graph snapshot + metrics, every 5th txn.
            # The graph snapshot is expensive (sorts 200 nodes); no need to do it per-txn.
            if self._total_processed % 5 == 0:
                snapshot = await self._graph.snapshot_for_dashboard()
                payload = {
                    "type":     "txn_tick",
                    "txn_id":   txn.txn_id,
                    "sender":   txn.sender_id,
                    "receiver": txn.receiver_id,
                    "amount":   txn.amount_rupees,
                    "graph":    snapshot,
                    "metrics":  await self.get_metrics(),
                }
                asyncio.create_task(self._broadcast(payload))


    async def _handle_alert(self, alert: FraudAlert) -> None:
        """Process a fraud alert: update graph, enrich, log, broadcast."""
        # Build rich explanation
        explanation = build_rich_explanation(alert)
        dashboard_payload = explanation["dashboard_payload"]

        # Update counters
        self._total_alerts += 1
        if alert.verdict == RiskVerdict.BLOCK:
            self._total_blocked += 1
        elif alert.verdict == RiskVerdict.FLAG:
            self._total_flagged += 1

        # Flag/block implicated accounts in the graph
        should_block = alert.verdict == RiskVerdict.BLOCK
        for account_id in alert.implicated_accounts:
            await self._graph.flag_account(
                account_id, alert.pattern, alert.risk_score, block=should_block
            )

        # Write to forensic audit log
        logger.warning(format_audit_log_line(alert))

        # Cache for dashboard initial-state queries
        self._recent_alerts.appendleft(dashboard_payload)

        # Broadcast alert to all connected WS clients
        payload = {
            "type":    "fraud_alert",
            "alert":   dashboard_payload,
            "metrics": await self.get_metrics(),
        }
        asyncio.create_task(self._broadcast(payload))

    # ─── Background Tasks ─────────────────────────────────────────────────────

    async def _eviction_loop(self) -> None:
        """Periodically evict stale graph edges to bound memory usage."""
        while self._running:
            await asyncio.sleep(30)
            removed = await self._graph.global_eviction()
            if removed:
                logger.debug("Eviction loop: removed %d stale edges", removed)

    async def _metrics_broadcast_loop(self) -> None:
        """Push operational metrics to dashboard every second."""
        while self._running:
            await asyncio.sleep(1.0)
            if self._broadcasters:
                payload = {
                    "type":    "metrics_tick",
                    "metrics": await self.get_metrics(),
                }
                asyncio.create_task(self._broadcast(payload))

    # ─── Helpers ──────────────────────────────────────────────────────────────

    async def _broadcast(self, payload: dict) -> None:
        """Fire-and-forget broadcast to all registered WebSocket clients."""
        if not self._broadcasters:
            return
        results = await asyncio.gather(
            *[fn(payload) for fn in self._broadcasters],
            return_exceptions=True,
        )
        # Silently remove dead broadcasters
        dead = [
            self._broadcasters[i]
            for i, r in enumerate(results)
            if isinstance(r, Exception)
        ]
        for fn in dead:
            self.remove_broadcaster(fn)

    async def get_metrics(self) -> dict:
        elapsed = time.time() - self._start_time if self._start_time else 0
        lat = self._latency_tracker.to_dict()
        node_count = await self._graph.store.get_node_count()
        edge_count = await self._graph.store.get_edge_count()
        return {
            "uptime_s":           round(elapsed, 1),
            "total_processed":    self._total_processed,
            "total_alerts":       self._total_alerts,
            "total_blocked":      self._total_blocked,
            "total_flagged":      self._total_flagged,
            "current_tps":        round(self._throughput_counter.current_tps, 1),
            "avg_latency_ms":     lat["avg_ms"],
            "p99_latency_ms":     lat["p99_ms"],
            "sla_compliance":     lat["sla_compliance_rate"],
            "sla_breaches":       lat["breach_count"],
            "graph_nodes":        node_count,
            "graph_edges":        edge_count,
            "emitter":            self._emitter.stats,
        }

    # ─── Public Read API (for REST endpoints) ─────────────────────────────────

    @property
    def recent_alerts(self) -> list[dict]:
        return list(self._recent_alerts)

    @property
    def graph(self) -> BehavioralGraphEngine:
        return self._graph

    @property
    def emitter(self) -> TransactionEmitter:
        return self._emitter

    async def inject_fraud_scenario(self, scenario: str = "random") -> int:
        """
        Manually inject a fraud scenario. Called by the /inject-fraud API endpoint.
        Returns the number of transactions injected.
        """
        from emitter.fraud_injector import (
            ATTACK_GENERATORS,
            generate_fan_in_attack,
            generate_fan_out_attack,
            generate_scatter_gather_attack,
            generate_velocity_abuse_attack,
            random_attack,
        )

        scenario_map = {
            "fan_out":        generate_fan_out_attack,
            "fan_in":         generate_fan_in_attack,
            "scatter_gather": generate_scatter_gather_attack,
            "velocity":       generate_velocity_abuse_attack,
            "random":         random_attack,
        }

        gen_fn = scenario_map.get(scenario, random_attack)
        count = 0
        async for txn in gen_fn():
            await self._process_transaction(txn)
            count += 1

        logger.info("Manual fraud injection: scenario=%s txns=%d", scenario, count)
        return count
