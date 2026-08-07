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
import math
import time
from collections import defaultdict
from typing import Callable, Coroutine

import structlog

from config.settings import settings
from core.baseline import BaselineEngine
from core.graph_engine import BehavioralGraphEngine
from core.models import FraudAlert, FraudPattern, RiskVerdict, UPITransaction

logger = structlog.get_logger(__name__)

baseline_engine = BaselineEngine(redis_url=settings.app.redis_url)

# Type alias for a detector coroutine
DetectorFn = Callable[
    [UPITransaction, BehavioralGraphEngine, float, int, int],
    Coroutine[None, None, FraudAlert | None],
]


# ─── Risk Score Calculation Helpers ───────────────────────────────────────────


def _compute_risk_score(
    pattern: FraudPattern,
    magnitude: float,       # e.g. out-degree, velocity count — normalised 0-1
    amount_paise: int = 0,  # raw amount for continuous tiering
) -> float:
    """
    Deterministic, continuous risk scoring.
    Uses logarithmic scaling for the transaction amount to create smooth, 
    non-linear risk curves that are extremely hard for fraudsters to threshold-test.
    """
    pattern_base_weight = {
        FraudPattern.FAN_OUT:        0.72,
        FraudPattern.FAN_IN:         0.68,
        FraudPattern.SCATTER_GATHER: 0.80,
        FraudPattern.MULE_CHAIN:     0.85,
        FraudPattern.VELOCITY_ABUSE: 0.65,
        FraudPattern.ROUND_TRIP:     0.90,
    }
    base = pattern_base_weight.get(pattern, 0.5)

    # Continuous Logarithmic Amount Tiering
    amount_rupees = max(amount_paise / 100, 1) # avoid log(0)
    log_amt = math.log10(amount_rupees)
    
    if log_amt < 2:
        amount_tier = 0.80
    else:
        # Scale linearly between log=2 (Rs 100) and log=5 (Rs 1 Lakh)
        amount_tier = 0.80 + (min(log_amt, 5.0) - 2.0) * (0.4 / 3.0)

    score = (base + (0.20 * magnitude)) * amount_tier
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
    out_degree: int,
    in_degree: int,
) -> FraudAlert | None:
    """
    Fan-Out: One sender disperses funds to N≥threshold distinct receivers
    within the sliding window. Classic smurfing / money mule scatter pattern.
    """
    threshold = settings.graph.fanout_threshold
    
    # Update baseline on every transaction to maintain accurate statistics
    is_outlier, z_score = await baseline_engine.update(f"out:{txn.sender_id}", float(out_degree))
    
    if out_degree < threshold:
        latency_ms = (time.time() - detection_start) * 1000
        logger.info("decision", txn_id=txn.txn_id, pattern="FAN_OUT", score=0.0, latency_ms=round(latency_ms, 2), verdict="CLEAR", account_ids=[txn.sender_id])
        return None

    # Wait, the threshold is met via the fast path!
    # Now we do the heavy lifting to get the full subgraph for the explanation
    window_start = time.time() - settings.graph.window_seconds
    outbound = await graph.get_outbound_edges(txn.sender_id, since_ts=window_start)
    unique_receivers = {e.receiver_id for e in outbound if not e.is_empty}

    # Gather all implicated transactions
    all_txn_ids: list[str] = []
    for edge in outbound:
        all_txn_ids.extend(t[2] for t in edge.txns)

    total_amount = sum(e.total_amount_paise for e in outbound)
    count = len(unique_receivers)
    magnitude = min(count / (threshold * 3), 1.0)   # normalise
    score = _compute_risk_score(
        FraudPattern.FAN_OUT, magnitude, amount_paise=txn.amount_paise,
    )
    verdict = _verdict_from_score(score)

    if verdict == RiskVerdict.CLEAR:
        latency_ms = (time.time() - detection_start) * 1000
        logger.info("decision", txn_id=txn.txn_id, pattern="FAN_OUT", score=score, latency_ms=round(latency_ms, 2), verdict="CLEAR", account_ids=[txn.sender_id] + list(unique_receivers))
        return None

    # Apply baseline suppression if not an outlier
    baseline_stats = await baseline_engine.get_baseline(f"out:{txn.sender_id}")
    explanation_suffix = ""
    if not is_outlier:
        score = min(score, settings.risk.block_threshold - 0.05)
        verdict = _verdict_from_score(score)
        explanation_suffix = (
            f" However, account's own baseline is {baseline_stats.mean:.2f}±{baseline_stats.stddev:.2f}; "
            f"current reading is {out_degree} (z-score={z_score:.2f}). "
            f"Escalation suppressed due to baseline."
        )

    latency_ms = (time.time() - detection_start) * 1000
    
    logger.info("decision", txn_id=txn.txn_id, pattern="FAN_OUT", score=score, latency_ms=round(latency_ms, 2), verdict=verdict.value, account_ids=[txn.sender_id] + list(unique_receivers))

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
            f"{explanation_suffix}"
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
    out_degree: int,
    in_degree: int,
) -> FraudAlert | None:
    """
    Fan-In: N≥threshold distinct senders funnel funds into one receiver.
    Signature of a mule collector account aggregating scattered proceeds.
    """
    threshold = settings.graph.fanin_threshold
    
    # Update baseline on every transaction to maintain accurate statistics
    is_outlier, z_score = await baseline_engine.update(f"in:{txn.receiver_id}", float(in_degree))
    
    if in_degree < threshold:
        latency_ms = (time.time() - detection_start) * 1000
        logger.info("decision", txn_id=txn.txn_id, pattern="FAN_IN", score=0.0, latency_ms=round(latency_ms, 2), verdict="CLEAR", account_ids=[txn.receiver_id])
        return None

    # Threshold met via fast path; fetch full subgraph
    window_start = time.time() - settings.graph.window_seconds
    inbound = await graph.get_inbound_edges(txn.receiver_id, since_ts=window_start)
    unique_senders = {e.sender_id for e in inbound if not e.is_empty}

    all_txn_ids: list[str] = []
    for edge in inbound:
        all_txn_ids.extend(t[2] for t in edge.txns)

    total_amount = sum(e.total_amount_paise for e in inbound)
    count = len(unique_senders)
    magnitude = min(count / (threshold * 3), 1.0)
    score = _compute_risk_score(
        FraudPattern.FAN_IN, magnitude, amount_paise=txn.amount_paise,
    )
    verdict = _verdict_from_score(score)

    if verdict == RiskVerdict.CLEAR:
        latency_ms = (time.time() - detection_start) * 1000
        logger.info("decision", txn_id=txn.txn_id, pattern="FAN_IN", score=score, latency_ms=round(latency_ms, 2), verdict="CLEAR", account_ids=list(unique_senders) + [txn.receiver_id])
        return None

    # Apply baseline suppression if not an outlier
    baseline_stats = await baseline_engine.get_baseline(f"in:{txn.receiver_id}")
    explanation_suffix = ""
    if not is_outlier:
        score = min(score, settings.risk.block_threshold - 0.05)
        verdict = _verdict_from_score(score)
        explanation_suffix = (
            f" However, account's own baseline is {baseline_stats.mean:.2f}±{baseline_stats.stddev:.2f}; "
            f"current reading is {in_degree} (z-score={z_score:.2f}). "
            f"Escalation suppressed due to baseline."
        )

    latency_ms = (time.time() - detection_start) * 1000
    
    logger.info("decision", txn_id=txn.txn_id, pattern="FAN_IN", score=score, latency_ms=round(latency_ms, 2), verdict=verdict.value, account_ids=list(unique_senders) + [txn.receiver_id])

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
            f"{explanation_suffix}"
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
    out_degree: int,
    in_degree: int,
) -> FraudAlert | None:
    """
    Scatter-Gather / Smurfing:
    Funds are split across intermediary accounts (scatter) and then consolidated
    back into a single collector (gather) within H hops.

    Detection:
    1. Expand sender neighbourhood up to H hops — WITHIN the last 60 seconds only.
    2. Check if any node in the subgraph receives from ≥5 other subgraph members.
       min_paths=5 eliminates false positives from naturally dense random graphs.
    """
    max_hops = settings.graph.scatter_gather_hops
    min_paths = 5  # Raised from 3 → eliminates false positives in larger account pools

    # Only consider recent edges (last 60s) to avoid historical false convergences
    since_ts = time.time() - settings.graph.window_seconds

    subgraph = await graph.get_neighbourhood(txn.sender_id, max_hops=max_hops)

    if len(subgraph.account_ids) < min_paths + 2:
        latency_ms = (time.time() - detection_start) * 1000
        logger.info("decision", txn_id=txn.txn_id, pattern="SCATTER_GATHER", score=0.0, latency_ms=round(latency_ms, 2), verdict="CLEAR", account_ids=[txn.sender_id])
        return None

    subgraph_set = set(subgraph.account_ids)

    gather_candidates: dict[str, int] = defaultdict(int)
    for account_id in subgraph.account_ids:
        # Only count recent inbound edges to avoid old traffic creating false convergences
        inbound = await graph.get_inbound_edges(account_id, since_ts=since_ts)
        for edge in inbound:
            if edge.sender_id in subgraph_set:
                gather_candidates[account_id] += 1

    confirmed_gatherers = {
        acc: cnt for acc, cnt in gather_candidates.items() if cnt >= min_paths
    }

    if not confirmed_gatherers:
        latency_ms = (time.time() - detection_start) * 1000
        logger.info("decision", txn_id=txn.txn_id, pattern="SCATTER_GATHER", score=0.0, latency_ms=round(latency_ms, 2), verdict="CLEAR", account_ids=subgraph.account_ids[:20])
        return None

    magnitude = min(len(confirmed_gatherers) / 3, 1.0)
    score = _compute_risk_score(
        FraudPattern.SCATTER_GATHER, magnitude, amount_paise=txn.amount_paise,
    )
    verdict = _verdict_from_score(score)

    if verdict == RiskVerdict.CLEAR:
        latency_ms = (time.time() - detection_start) * 1000
        logger.info("decision", txn_id=txn.txn_id, pattern="SCATTER_GATHER", score=score, latency_ms=round(latency_ms, 2), verdict="CLEAR", account_ids=subgraph.account_ids[:20])
        return None

    latency_ms = (time.time() - detection_start) * 1000
    
    logger.info("decision", txn_id=txn.txn_id, pattern="SCATTER_GATHER", score=score, latency_ms=round(latency_ms, 2), verdict=verdict.value, account_ids=subgraph.account_ids[:20])

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
    out_degree: int,
    in_degree: int,
) -> FraudAlert | None:
    """
    Velocity Abuse: Sender issues an abnormally high number of transactions
    within a short burst window (30 seconds). Indicates automated fraud tooling.

    Threshold: 30 txns in 30s. At 30 TPS across 5000 accounts, a single
    account sends ~0.006 txns/s on average — hitting 30 in 30s requires
    a 150x burst above normal, which is unambiguously machine-driven.
    """
    burst_window = 30  # seconds
    velocity_threshold = 30  # transactions in 30s (raised from 20)

    burst_start = time.time() - burst_window
    outbound = await graph.get_outbound_edges(txn.sender_id, since_ts=burst_start)
    txn_count = sum(e.txn_count for e in outbound)

    if txn_count < velocity_threshold:
        latency_ms = (time.time() - detection_start) * 1000
        logger.info("decision", txn_id=txn.txn_id, pattern="VELOCITY_ABUSE", score=0.0, latency_ms=round(latency_ms, 2), verdict="CLEAR", account_ids=[txn.sender_id])
        return None

    magnitude = min(txn_count / (velocity_threshold * 2), 1.0)
    score = _compute_risk_score(FraudPattern.VELOCITY_ABUSE, magnitude, amount_paise=txn.amount_paise)
    verdict = _verdict_from_score(score)

    if verdict == RiskVerdict.CLEAR:
        latency_ms = (time.time() - detection_start) * 1000
        logger.info("decision", txn_id=txn.txn_id, pattern="VELOCITY_ABUSE", score=score, latency_ms=round(latency_ms, 2), verdict="CLEAR", account_ids=[txn.sender_id])
        return None

    latency_ms = (time.time() - detection_start) * 1000
    all_txn_ids = [t[2] for e in outbound for t in e.txns]
    
    logger.info("decision", txn_id=txn.txn_id, pattern="VELOCITY_ABUSE", score=score, latency_ms=round(latency_ms, 2), verdict=verdict.value, account_ids=[txn.sender_id])

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

async def detect_round_trip(
    txn: UPITransaction,
    graph: BehavioralGraphEngine,
    detection_start: float,
    out_degree: int,
    in_degree: int,
) -> FraudAlert | None:
    """
    Round-Trip: Funds leave an account and return to it via ≥2 intermediaries
    within a tight 30-second burst window.

    WHY tighter window: With 5000 accounts over a 60s window, random traffic
    creates enough edges that ANY pair of accounts has a path back within 4
    hops just by chance. Restricting to 30s eliminates virtually all noise —
    a genuine round-trip is fast (seconds), not historical coincidence.

    Also requires: in_degree >= 1 (money is actually coming IN to sender this
    window) and at least 2 intermediate hops (A→B→A is not laundering).
    """
    if in_degree < 1:
        return None

    # Only look at edges in the last 30 seconds (tight burst window)
    burst_since = time.time() - 30
    max_hops = 4
    min_intermediaries = 2  # Must have at least A→B→C→A (2 intermediate nodes)

    visited = {txn.sender_id, txn.receiver_id}
    # frontier: (node_id, hop_count)
    frontier: set[tuple[str, int]] = {(txn.receiver_id, 1)}
    cycle_found = False
    cycle_hops = 0

    for _ in range(max_hops):
        next_frontier: set[tuple[str, int]] = set()
        for node, hops in frontier:
            # Only traverse recent edges (30s window)
            outbound = await graph.get_outbound_edges(node, since_ts=burst_since)
            for edge in outbound:
                if edge.is_empty:
                    continue
                if edge.receiver_id == txn.sender_id and hops >= min_intermediaries:
                    cycle_found = True
                    cycle_hops = hops + 1
                    break
                if edge.receiver_id not in visited:
                    next_frontier.add((edge.receiver_id, hops + 1))
                    visited.add(edge.receiver_id)
            if cycle_found:
                break
        if cycle_found:
            break
        frontier = next_frontier
        if not frontier:
            break

    if not cycle_found:
        return None
        
    score = _compute_risk_score(FraudPattern.ROUND_TRIP, 1.0, amount_paise=txn.amount_paise)
    verdict = _verdict_from_score(score)
    
    if verdict == RiskVerdict.CLEAR:
        return None
        
    latency_ms = (time.time() - detection_start) * 1000
    logger.warning("decision", txn_id=txn.txn_id, pattern="ROUND_TRIP", score=score, latency_ms=round(latency_ms, 2), verdict=verdict.value, account_ids=[txn.sender_id, txn.receiver_id])

    return FraudAlert(
        triggered_by_txn=txn.txn_id,
        pattern=FraudPattern.ROUND_TRIP,
        verdict=verdict,
        risk_score=score,
        implicated_accounts=list(visited),
        implicated_transactions=[txn.txn_id],
        explanation_text=(
            f"Round-Trip cycle detected: Funds sent by {txn.sender_id[:12]}… to "
            f"{txn.receiver_id[:12]}… create a closed loop within {max_hops+1} hops. "
            f"This matches the structural signature of fake transaction history generation."
        ),
        explanation_cypher=_cypher_query(FraudPattern.ROUND_TRIP, [txn.sender_id]),
        detection_latency_ms=round(latency_ms, 2),
        within_sla=latency_ms <= settings.graph.latency_budget_ms,
    )

async def detect_mule_chain(
    txn: UPITransaction,
    graph: BehavioralGraphEngine,
    detection_start: float,
    out_degree: int,
    in_degree: int,
) -> FraudAlert | None:
    """
    Mule Chain: Linear forwarding of funds (A → B → C → D).
    Signature of layering to distance funds from the origin.

    Two key constraints to eliminate false positives:
    1. Amount forwarding ratio: each hop must forward ≥70% of the previous
       hop's amount. This distinguishes actual fund forwarding from coincidental
       chains where unrelated amounts happen to follow the same path.
    2. Time window: only follow edges from the last 60 seconds. Old traffic
       creating historical chains in a large account pool is normal — fast
       sequential forwarding within 60s is not.
    3. Minimum chain length: 4 hops (A→B→C→D→E) — 3 hops is too short.
    """
    min_chain_length = 4  # Raised from 3 to reduce false positives
    max_hops = 6
    min_forwarding_ratio = 0.70  # Each hop must forward ≥70% of previous amount

    # Only consider edges in the last 60 seconds
    since_ts = time.time() - settings.graph.window_seconds

    current_node = txn.receiver_id
    chain = [txn.sender_id, txn.receiver_id]
    prev_amount = txn.amount_paise

    for hop in range(max_hops):
        outbound = await graph.get_outbound_edges(current_node, since_ts=since_ts)
        if not outbound:
            break

        # Sort by amount descending — actual forwarding uses the largest amount
        outbound.sort(key=lambda e: e.total_amount_paise, reverse=True)
        best_edge = outbound[0]

        if best_edge.is_empty or best_edge.receiver_id in chain:
            break

        # Amount continuity check: must forward ≥70% of what was received
        if prev_amount > 0 and best_edge.total_amount_paise < prev_amount * min_forwarding_ratio:
            break  # Amounts diverge — this is not a forwarding chain

        chain.append(best_edge.receiver_id)
        prev_amount = best_edge.total_amount_paise
        current_node = best_edge.receiver_id

    if len(chain) < min_chain_length + 1:
        return None
        
    magnitude = min((len(chain) - 2) / 3, 1.0)
    score = _compute_risk_score(FraudPattern.MULE_CHAIN, magnitude, amount_paise=txn.amount_paise)
    verdict = _verdict_from_score(score)
    
    if verdict == RiskVerdict.CLEAR:
        return None
        
    latency_ms = (time.time() - detection_start) * 1000
    logger.warning("decision", txn_id=txn.txn_id, pattern="MULE_CHAIN", score=score, latency_ms=round(latency_ms, 2), verdict=verdict.value, account_ids=chain)

    return FraudAlert(
        triggered_by_txn=txn.txn_id,
        pattern=FraudPattern.MULE_CHAIN,
        verdict=verdict,
        risk_score=score,
        implicated_accounts=chain,
        implicated_transactions=[txn.txn_id],
        explanation_text=(
            f"Mule Chain detected: Rapid linear forwarding of funds across {len(chain)} hops "
            f"originating from {txn.sender_id[:12]}…. This matches the structural signature "
            f"of layering to obfuscate fund origins."
        ),
        explanation_cypher=_cypher_query(FraudPattern.MULE_CHAIN, chain),
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
            ("round_trip",     detect_round_trip),
            ("mule_chain",     detect_mule_chain),
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
        self, txn: UPITransaction, graph: BehavioralGraphEngine, out_degree: int, in_degree: int
    ) -> list[FraudAlert]:
        """Run all registered detectors concurrently."""
        detection_start = time.time()
        tasks = [
            asyncio.create_task(fn(txn, graph, detection_start, out_degree, in_degree))
            for _, fn in self._detectors
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        alerts: list[FraudAlert] = []
        for name_fn, result in zip(self._detectors, results):
            name, _ = name_fn
            if isinstance(result, Exception):
                logger.error(
                    "Detector '%s' raised an exception for txn %s: %s",
                    name, txn.txn_id[:8], result, exc_info=result,
                )
            elif result is not None:
                alerts.append(result)

        if alerts:
            logger.info(
                "\U0001f6a8 %d alert(s) for txn %s: %s",
                len(alerts),
                txn.txn_id[:8],
                [a.pattern for a in alerts],
            )

        return alerts


# ─── Module-level singleton ───────────────────────────────────────────────────

detector_registry = PatternDetectorRegistry()
