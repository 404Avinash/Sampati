"""
decode_sih / emitter / fraud_injector.py
──────────────────────────────────────────
Controlled synthetic fraud pattern injector.

UPDATED:
  - Fan-Out attack: 20–35 receivers (well above new threshold of 15)
  - Fan-In attack:  15–25 senders  (well above threshold of 12)
  - Velocity burst: 35–80 txns     (well above threshold of 30)
  - ATTACK_GENERATORS only includes patterns that are clearly unambiguous
    in normal traffic (fan_out, fan_in, velocity). Mule chain and round trip
    are available for MANUAL demo injection only via /api/inject.
  - Reduced dormancy from 8→4 so attacks are visible sooner in demos.
"""

from __future__ import annotations

import asyncio
import random
from datetime import datetime, timezone
from typing import AsyncGenerator

from core.models import FraudPattern, UPITransaction
from emitter.distributions import (
    MULE_ACCOUNT_POOL,
    LEGITIMATE_ACCOUNT_POOL,
    MERCHANT_ACCOUNT_POOL,
    MULE_DORMANCY_TRANSACTIONS,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_txn(
    sender: str,
    receiver: str,
    amount_paise: int,
    pattern: FraudPattern,
) -> UPITransaction:
    return UPITransaction(
        sender_id=sender,
        receiver_id=receiver,
        amount_paise=amount_paise,
        timestamp=_now(),
        is_synthetic=True,
        injected_pattern=pattern,
    )


def _make_warmup_txn(sender: str) -> UPITransaction:
    """Normal-looking small purchase from mule account during dormancy."""
    receiver = random.choice(MERCHANT_ACCOUNT_POOL)
    amount = random.randint(5_000, 50_000)  # ₹50–₹500
    return UPITransaction(
        sender_id=sender,
        receiver_id=receiver,
        amount_paise=amount,
        timestamp=_now(),
        is_synthetic=True,
    )


async def _warmup_mule(mule_id: str) -> AsyncGenerator[UPITransaction, None]:
    """Warm up a mule account with MULE_DORMANCY_TRANSACTIONS normal transactions."""
    for _ in range(MULE_DORMANCY_TRANSACTIONS):
        yield _make_warmup_txn(mule_id)
        await asyncio.sleep(random.uniform(0.1, 0.4))


# ─── Attack Scenario Generators ───────────────────────────────────────────────


async def generate_fan_out_attack(
    num_receivers: int | None = None,
) -> AsyncGenerator[UPITransaction, None]:
    """
    Fan-Out scatter attack.
    Sends to 20-35 unique receivers — well above the threshold of 15.
    Each transaction is large (₹5,000–₹20,000) to make it obvious in the dashboard.
    """
    n = num_receivers or random.randint(20, 35)
    sender = random.choice(MULE_ACCOUNT_POOL)
    safe_pool = [a for a in LEGITIMATE_ACCOUNT_POOL if a != sender]
    receivers = random.sample(safe_pool, min(n, len(safe_pool)))
    amounts = [random.randint(5_000_00, 20_000_00) for _ in receivers]  # ₹5k–₹20k

    # Phase 1: dormancy warmup
    async for txn in _warmup_mule(sender):
        yield txn

    # Phase 2: rapid fan-out scatter (tight burst — 50–200ms between txns)
    for receiver, amount in zip(receivers, amounts):
        yield _make_txn(sender, receiver, amount, FraudPattern.FAN_OUT)
        await asyncio.sleep(random.uniform(0.05, 0.20))


async def generate_fan_in_attack(
    num_senders: int | None = None,
) -> AsyncGenerator[UPITransaction, None]:
    """
    Fan-In mule aggregation attack.
    15-25 feeders funnel funds to one collector — above threshold of 12.
    """
    n = num_senders or random.randint(15, 25)
    collector = random.choice(MULE_ACCOUNT_POOL)
    safe_pool = [a for a in LEGITIMATE_ACCOUNT_POOL if a != collector]
    senders = random.sample(safe_pool, min(n, len(safe_pool)))
    amounts = [random.randint(5_000_00, 30_000_00) for _ in senders]

    # Phase 1: collector makes normal transactions to look legitimate
    async for txn in _warmup_mule(collector):
        yield txn

    # Phase 2: fan-in aggregation
    for sender, amount in zip(senders, amounts):
        yield _make_txn(sender, collector, amount, FraudPattern.FAN_IN)
        await asyncio.sleep(random.uniform(0.1, 0.5))


async def generate_scatter_gather_attack(
    num_intermediaries: int | None = None,
) -> AsyncGenerator[UPITransaction, None]:
    """
    Scatter-Gather (Smurfing) attack:
    Origin → split across 6-10 intermediaries → re-converge at collector.
    The intermediaries re-forward within 5 seconds (tight window = strong signal).
    """
    n = num_intermediaries or random.randint(6, 10)
    origin = random.choice(MULE_ACCOUNT_POOL)
    collector = random.choice([m for m in MULE_ACCOUNT_POOL if m != origin])
    safe_pool = [a for a in LEGITIMATE_ACCOUNT_POOL if a not in (origin, collector)]
    intermediaries = random.sample(safe_pool, min(n, len(safe_pool)))

    total_amount = random.randint(50_000_00, 200_000_00)
    splits = _uneven_split(total_amount, n)

    # Scatter phase: origin → intermediaries (fast)
    for inter, amount in zip(intermediaries, splits):
        yield _make_txn(origin, inter, amount, FraudPattern.SCATTER_GATHER)
        await asyncio.sleep(random.uniform(0.05, 0.15))

    # Brief layering delay (< 30s so it stays in burst window)
    await asyncio.sleep(random.uniform(2.0, 5.0))

    # Gather phase: intermediaries → collector (tight)
    for inter, amount in zip(intermediaries, splits):
        gather_amount = int(amount * random.uniform(0.88, 0.98))
        if gather_amount > 0:
            yield _make_txn(inter, collector, gather_amount, FraudPattern.SCATTER_GATHER)
            await asyncio.sleep(random.uniform(0.05, 0.20))


async def generate_velocity_abuse_attack(
    burst_count: int | None = None,
) -> AsyncGenerator[UPITransaction, None]:
    """
    Velocity abuse burst: 35-80 transactions in rapid succession.
    Well above the threshold of 30 in 30s. Simulates automated fraud tooling.
    """
    count = burst_count or random.randint(35, 80)
    sender = random.choice(MULE_ACCOUNT_POOL)
    safe_pool = [a for a in LEGITIMATE_ACCOUNT_POOL if a != sender]
    receivers = random.choices(safe_pool, k=count)

    for receiver in receivers:
        amount = random.randint(1_000_00, 5_000_00)  # ₹1k–₹5k
        yield _make_txn(sender, receiver, amount, FraudPattern.VELOCITY_ABUSE)
        await asyncio.sleep(random.uniform(0.01, 0.04))  # 10-40ms between txns


async def generate_mule_chain_attack(
    chain_length: int | None = None,
) -> AsyncGenerator[UPITransaction, None]:
    """
    Mule Chain: A → B → C → D linear forwarding with amount continuity.
    For manual demo injection only — too subtle for auto-injection at low TPS.
    """
    length = chain_length or random.randint(4, 6)
    chain = random.sample(MULE_ACCOUNT_POOL, min(length + 1, len(MULE_ACCOUNT_POOL)))
    amount = random.randint(10_000_00, 50_000_00)  # ₹10k–₹50k

    for i in range(len(chain) - 1):
        sender = chain[i]
        receiver = chain[i + 1]
        # Each hop slightly reduces amount (fee skimming) — creates forwarding ratio
        hop_amount = int(amount * (0.92 ** i))
        yield _make_txn(sender, receiver, max(hop_amount, 100_00), FraudPattern.MULE_CHAIN)
        await asyncio.sleep(random.uniform(0.5, 2.0))  # Slower — layering


async def generate_round_trip_attack() -> AsyncGenerator[UPITransaction, None]:
    """
    Round-Trip: A → B → C → A within a tight time window.
    For manual demo injection only.
    """
    if len(MULE_ACCOUNT_POOL) < 3:
        return
    a, b, c = random.sample(MULE_ACCOUNT_POOL, 3)
    amount = random.randint(5_000_00, 20_000_00)

    yield _make_txn(a, b, amount, FraudPattern.ROUND_TRIP)
    await asyncio.sleep(random.uniform(0.3, 1.0))
    yield _make_txn(b, c, int(amount * 0.95), FraudPattern.ROUND_TRIP)
    await asyncio.sleep(random.uniform(0.3, 1.0))
    yield _make_txn(c, a, int(amount * 0.90), FraudPattern.ROUND_TRIP)


# ─── Attack Scheduler ─────────────────────────────────────────────────────────
# Only unambiguous patterns for automatic injection.
# Mule chain and round trip are available for manual /api/inject demos.

ATTACK_GENERATORS = [
    (generate_fan_out_attack,        "Fan-Out Scatter"),
    (generate_fan_in_attack,         "Fan-In Aggregation"),
    (generate_scatter_gather_attack, "Scatter-Gather Smurfing"),
    (generate_velocity_abuse_attack, "Velocity Abuse Burst"),
]

MANUAL_ATTACK_GENERATORS = {
    "fan_out":        generate_fan_out_attack,
    "fan_in":         generate_fan_in_attack,
    "scatter_gather": generate_scatter_gather_attack,
    "velocity":       generate_velocity_abuse_attack,
    "mule_chain":     generate_mule_chain_attack,
    "round_trip":     generate_round_trip_attack,
}


async def random_attack() -> AsyncGenerator[UPITransaction, None]:
    """Randomly select and execute one attack scenario."""
    generator_fn, name = random.choice(ATTACK_GENERATORS)
    import logging
    logging.getLogger(__name__).info("💉 Injecting fraud scenario: %s", name)
    async for txn in generator_fn():
        yield txn


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _uneven_split(total: int, n: int) -> list[int]:
    """Split total into n uneven non-zero parts."""
    if n <= 1:
        return [total]
    cuts = sorted(random.sample(range(1, total), min(n - 1, total - 1)))
    parts = [cuts[0]] + [cuts[i] - cuts[i - 1] for i in range(1, len(cuts))] + [total - cuts[-1]]
    return [max(100, p) for p in parts][:n]
