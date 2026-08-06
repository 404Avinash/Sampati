"""
decode_sih / core / pattern_detector.py
─────────────────────────────────────────
Structural fraud pattern detection algorithms.

Each detector is an independent, stateless coroutine that receives:
  - The incoming transaction (the "trigger")
  - A reference to the graph engine (for neighbourhood queries)
  - The current unix timestamp

And returns either a FraudAlert or None.

Why stateless detectors?
  - They can be composed, run in parallel, and unit-tested independently.
  - Adding a new pattern (e.g., Cyclic Round-Trip) requires adding one function
    and registering it in PatternDetectorRegistry — no changes to the pipeline.

Detection heuristics:
  ┌─────────────────┬──────────────────────────────────────────────────────────┐
  │ Pattern         │ Structural Signature                                     │
  ├─────────────────┼──────────────────────────────────────────────────────────┤
  │ FAN_OUT         │ sender → N≥threshold receivers within window T           │
  │ FAN_IN          │ N≥threshold senders → single receiver within window T    │
  │ SCATTER_GATHER  │ sender → intermediaries → single collector, ≤H hops      │
  │ MULE_CHAIN      │ A→B→C→D linear forwarding within short window            │
  │ VELOCITY_ABUSE  │ sender issues >V txns in W seconds (even across accounts)│
  │ ROUND_TRIP      │ funds return to origin through ≥2 intermediary hops      │
  └─────────────────┴──────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from typing import Callable, Coroutine

from config.settings import settings
from core.graph_engine import BehavioralGraphEngine
from core.models import FraudAlert, FraudPattern, RiskVerdict, UPITransaction

logger = logging.getLogger(__name__)

# Type alias for a detector coroutine
DetectorFn = Callable[
    [UPITransaction, BehavioralGraphEngine, float],
    Coroutine[None, None, FraudAlert | None],
]


# ─── Risk Score Calculation Helpers ───────────────────────────────────────────


def _compute_risk_score(
    pattern: FraudPattern,
    magnitude: float,       # e.g. out-degree, velocity count — normalised 0-1
    amount_normalised: float,  # transaction amount normalised 0-1
) -> float:
    """
    Deterministic, interpretable risk scoring.
    Weights are calibrated to push high-confidence structural patterns above
    the BLOCK threshold (0.85) while keeping borderline cases in FLAG zone.
    """
    pattern_base_weight = {
        FraudPattern.FAN_OUT:        0.72,
        FraudPattern.FAN_IN:         0.68,
        FraudPattern.SCATTER_GATHER: 0.80,
        FraudPattern.MULE_CHAIN:     0.65,
        FraudPattern.VELOCITY_ABUSE: 0.55,
        FraudPattern.ROUND_TRIP:     0.75,
    }
    base = pattern_base_weight.get(pattern, 0.5)
    score = base + (0.15 * magnitude) + (0.10 * amount_normalised)
    return round(min(score, 1.0), 4)


def _verdict_from_score(score: float) -> RiskVerdict:
    cfg = settings.risk
    if score >= cfg.block_threshold:
        return RiskVerdict.BLOCK
    if score >= cfg.flag_threshold:
        return RiskVerdict.FLAG
    return RiskVerdict.CLEAR


def _cypher_query(pattern: FraudPattern, account_ids: list[str]) -> str:
    """Generate a Cypher-style audit query for regulator-readable explainability."""
    ids = ", ".join(f'"{a}"' for a in account_ids[:5])
    match = {
        FraudPattern.FAN_OUT:        f"MATCH (s {{id: {ids}}})-[t:SENT]->() WHERE t.ts > now()-{settings.graph.window_seconds}s RETURN s, collect(t)",
        FraudPattern.FAN_IN:         f"MATCH ()-[t:SENT]->(r {{id: {ids}}}) WHERE t.ts > now()-{settings.graph.window_seconds}s RETURN r, collect(t)",
        FraudPattern.SCATTER_GATHER: f"MATCH p=(s {{id: {ids}}})-[*1..{settings.graph.scatter_gather_hops}]->(c) WHERE s<>c RETURN p",
        FraudPattern.MULE_CHAIN:     f"MATCH p=(a {{id: {ids}}})-[:SENT*1..5]->(z) WHERE a<>z RETURN p",
        FraudPattern.VELOCITY_ABUSE: f"MATCH (a {{id: {ids}}})-[t:SENT]->() WHERE t.ts > now()-60s WITH a, count(t) as cnt WHERE cnt > 20 RETURN a, cnt",
        FraudPattern.ROUND_TRIP:     f"MATCH p=(origin)-[:SENT*2..6]->(origin) WHERE origin.id IN [{ids}] RETURN p",
    }
    return match.get(pattern, f"MATCH (n) WHERE n.id IN [{ids}] RETURN n")


# ─── Individual Detectors ─────────────────────────────────────────────────────


async def detect_fan_out(
    txn: UPITransaction,
    graph: BehavioralGraphEngine,
    detection_start: float,
) -> FraudAlert | None:
    """
    Fan-Out: One sender disperses funds to N≥threshold distinct receivers
    within the sliding window. Classic smurfing / money mule scatter pattern.
    """
    threshold = settings.graph.fanout_threshold
    window_start = time.time() - settings.graph.window_seconds

    outbound = await graph.get_outbound_edges(txn.sender_id, since_ts=window_start)
    unique_receivers = {e.receiver_id for e in outbound if not e.is_empty}
    count = len(unique_receivers)

    if count < threshold:
        return None

    # Gather all implicated transactions
    all_txn_ids: list[str] = []
    for edge in outbound:
        all_txn_ids.extend(t[2] for t in edge.txns)

    total_amount = sum(e.total_amount_paise for e in outbound)
    magnitude = min(count / (threshold * 3), 1.0)   # normalise
    amount_norm = min(total_amount / 10_000_000, 1.0)  # cap at ₹1L
    score = _compute_risk_score(FraudPattern.FAN_OUT, magnitude, amount_norm)
    verdict = _verdict_from_score(score)

    if verdict == RiskVerdict.CLEAR:
        return None

    latency_ms = (time.time() - detection_start) * 1000

    return FraudAlert(
        triggered_by_txn=txn.txn_id,
        pattern=FraudPattern.FAN_OUT,
        verdict=verdict,
        risk_score=score,
        implicated_accounts=[txn.sender_id] + list(unique_receivers),
        implicated_transactions=all_txn_ids[:50],
        explanation_text=(
            f"Account {txn.sender_id[:12]}… sent money to {count} distinct accounts "
            f"within {settings.graph.window_seconds} seconds, totalling "
            f"₹{total_amount / 100:.2f}. "
            f"Fan-Out threshold is {threshold} unique recipients. "
            f"This matches the structural signature of a mule scatter operation."
        ),
        explanation_cypher=_cypher_query(
            FraudPattern.FAN_OUT, [txn.sender_id] + list(unique_receivers)
        ),
        detection_latency_ms=round(latency_ms, 2),
        within_sla=latency_ms <= settings.graph.latency_budget_ms,
    )


async def detect_fan_in(
    txn: UPITransaction,
    graph: BehavioralGraphEngine,
    detection_start: float,
) -> FraudAlert | None:
    """
    Fan-In: N≥threshold distinct senders funnel funds into one receiver.
    Signature of a mule collector account aggregating scattered proceeds.
    """
    threshold = settings.graph.fanin_threshold
    window_start = time.time() - settings.graph.window_seconds

    inbound = await graph.get_inbound_edges(txn.receiver_id, since_ts=window_start)
    unique_senders = {e.sender_id for e in inbound if not e.is_empty}
    count = len(unique_senders)

    if count < threshold:
        return None

    all_txn_ids: list[str] = []
    for edge in inbound:
        all_txn_ids.extend(t[2] for t in edge.txns)

    total_amount = sum(e.total_amount_paise for e in inbound)
    magnitude = min(count / (threshold * 3), 1.0)
    amount_norm = min(total_amount / 10_000_000, 1.0)
    score = _compute_risk_score(FraudPattern.FAN_IN, magnitude, amount_norm)
    verdict = _verdict_from_score(score)

    if verdict == RiskVerdict.CLEAR:
        return None

    latency_ms = (time.time() - detection_start) * 1000

    return FraudAlert(
        triggered_by_txn=txn.txn_id,
        pattern=FraudPattern.FAN_IN,
        verdict=verdict,
        risk_score=score,
        implicated_accounts=list(unique_senders) + [txn.receiver_id],
        implicated_transactions=all_txn_ids[:50],
        explanation_text=(
            f"Account {txn.receiver_id[:12]}… received funds from {count} distinct "
            f"accounts within {settings.graph.window_seconds} seconds, totalling "
            f"₹{total_amount / 100:.2f}. "
            f"Fan-In threshold is {threshold} unique senders. "
            f"This matches the structural signature of a mule collector account."
        ),
        explanation_cypher=_cypher_query(
            FraudPattern.FAN_IN, list(unique_senders) + [txn.receiver_id]
        ),
        detection_latency_ms=round(latency_ms, 2),
        within_sla=latency_ms <= settings.graph.latency_budget_ms,
    )


async def detect_scatter_gather(
    txn: UPITransaction,
    graph: BehavioralGraphEngine,
    detection_start: float,
) -> FraudAlert | None:
    """
    Scatter-Gather / Smurfing:
    Funds are split across intermediary accounts (scatter) and then consolidated
    back into a single collector (gather) within H hops.

    Detection:
    1. Expand sender neighbourhood up to H hops.
    2. Check if any leaf node in the subgraph is also a receiver of multiple
       intermediate nodes — this is the "gather" convergence point.
    """
    max_hops = settings.graph.scatter_gather_hops
    min_paths = 3  # Minimum convergence paths to the collector

    subgraph = await graph.get_neighbourhood(txn.sender_id, max_hops=max_hops)

    if len(subgraph.account_ids) < min_paths + 2:
        return None

    # Count how many accounts in the subgraph have multiple inbound edges
    # from other subgraph members — these are the potential "gather" nodes
    gather_candidates: dict[str, int] = defaultdict(int)
    for account_id in subgraph.account_ids:
        inbound = await graph.get_inbound_edges(account_id)
        for edge in inbound:
            if edge.sender_id in set(subgraph.account_ids):
                gather_candidates[account_id] += 1

    confirmed_gatherers = {
        acc: cnt for acc, cnt in gather_candidates.items() if cnt >= min_paths
    }

    if not confirmed_gatherers:
        return None

    magnitude = min(len(confirmed_gatherers) / 3, 1.0)
    amount_norm = min(
        sum(edge.total_amount_paise for edge in await graph.get_outbound_edges(txn.sender_id))
        / 10_000_000,
        1.0,
    )
    score = _compute_risk_score(FraudPattern.SCATTER_GATHER, magnitude, amount_norm)
    verdict = _verdict_from_score(score)

    if verdict == RiskVerdict.CLEAR:
        return None

    latency_ms = (time.time() - detection_start) * 1000

    return FraudAlert(
        triggered_by_txn=txn.txn_id,
        pattern=FraudPattern.SCATTER_GATHER,
        verdict=verdict,
        risk_score=score,
        implicated_accounts=subgraph.account_ids[:20],
        implicated_transactions=subgraph.txn_ids[:50],
        explanation_text=(
            f"Scatter-Gather pattern detected originating from "
            f"{txn.sender_id[:12]}…. Funds were split across "
            f"{len(subgraph.account_ids)} intermediate accounts and reconverged "
            f"at {len(confirmed_gatherers)} collector node(s) within "
            f"{max_hops} hops. This matches the Smurfing topology used to "
            f"evade per-transaction reporting thresholds."
        ),
        explanation_cypher=_cypher_query(
            FraudPattern.SCATTER_GATHER, subgraph.account_ids
        ),
        detection_latency_ms=round(latency_ms, 2),
        within_sla=latency_ms <= settings.graph.latency_budget_ms,
    )


async def detect_velocity_abuse(
    txn: UPITransaction,
    graph: BehavioralGraphEngine,
    detection_start: float,
) -> FraudAlert | None:
    """
    Velocity Abuse: Sender issues an abnormally high number of transactions
    within a short burst window (30 seconds). Indicates automated fraud tooling.
    """
    burst_window = 30  # seconds
    velocity_threshold = 20  # transactions in 30s

    burst_start = time.time() - burst_window
    outbound = await graph.get_outbound_edges(txn.sender_id, since_ts=burst_start)
    txn_count = sum(e.txn_count for e in outbound)

    if txn_count < velocity_threshold:
        return None

    magnitude = min(txn_count / (velocity_threshold * 2), 1.0)
    score = _compute_risk_score(FraudPattern.VELOCITY_ABUSE, magnitude, 0.0)
    verdict = _verdict_from_score(score)

    if verdict == RiskVerdict.CLEAR:
        return None

    latency_ms = (time.time() - detection_start) * 1000
    all_txn_ids = [t[2] for e in outbound for t in e.txns]

    return FraudAlert(
        triggered_by_txn=txn.txn_id,
        pattern=FraudPattern.VELOCITY_ABUSE,
        verdict=verdict,
        risk_score=score,
        implicated_accounts=[txn.sender_id],
        implicated_transactions=all_txn_ids[:50],
        explanation_text=(
            f"Account {txn.sender_id[:12]}… issued {txn_count} transactions "
            f"within 30 seconds. Normal UPI velocity threshold is "
            f"{velocity_threshold} transactions per 30 seconds. "
            f"This pattern is consistent with automated fraud tooling."
        ),
        explanation_cypher=_cypher_query(FraudPattern.VELOCITY_ABUSE, [txn.sender_id]),
        detection_latency_ms=round(latency_ms, 2),
        within_sla=latency_ms <= settings.graph.latency_budget_ms,
    )


# ─── Detector Registry ────────────────────────────────────────────────────────


class PatternDetectorRegistry:
    """
    Registry of all active pattern detectors.

    To add a new detector:
    1. Define an async function with the DetectorFn signature above.
    2. Register it here.

    The pipeline calls run_all() which executes all detectors concurrently
    via asyncio.gather() and returns any alerts they produced.
    """

    def __init__(self) -> None:
        self._detectors: list[tuple[str, DetectorFn]] = [
            ("fan_out",        detect_fan_out),
            ("fan_in",         detect_fan_in),
            ("scatter_gather", detect_scatter_gather),
            ("velocity_abuse", detect_velocity_abuse),
        ]
        logger.info(
            "PatternDetectorRegistry initialised with %d detectors: %s",
            len(self._detectors),
            [name for name, _ in self._detectors],
        )

    def register(self, name: str, fn: DetectorFn) -> None:
        """Register a new detector at runtime."""
        self._detectors.append((name, fn))
        logger.info("Registered new detector: %s", name)

    async def run_all(
        self,
        txn: UPITransaction,
        graph: BehavioralGraphEngine,
    ) -> list[FraudAlert]:
        """
        Run all detectors concurrently against a transaction.
        Returns all non-None alerts, deduplicated by pattern type.
        """
        detection_start = time.time()

        results = await asyncio.gather(
            *[fn(txn, graph, detection_start) for _, fn in self._detectors],
            return_exceptions=False,
        )

        alerts = [r for r in results if r is not None]

        if alerts:
            logger.info(
                "🚨 %d alert(s) for txn %s: %s",
                len(alerts),
                txn.txn_id[:8],
                [a.pattern for a in alerts],
            )

        return alerts


# ─── Module-level singleton ───────────────────────────────────────────────────

detector_registry = PatternDetectorRegistry()
