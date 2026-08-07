"""
decode_sih / emitter / fraud_injector.py
──────────────────────────────────────────
Controlled synthetic fraud pattern injector.

This module generates coordinated fraud sequences — not individual fraudulent
transactions, but structurally coherent attack scenarios (mule networks,
scatter-gather operations, velocity bursts) that the graph engine should detect.

Each injector is a coroutine that yields UPITransaction objects. The emitter
calls these on a schedule determined by the configured fraud_rate.

Design principle: injectors must produce ground-truth labelled transactions
(injected_pattern is set) so that recall/precision metrics can be computed
during evaluation.
"""

from __future__ import annotations

import asyncio
import random
import uuid
from datetime import datetime, timezone
from typing import AsyncGenerator

from core.models import FraudPattern, UPITransaction
from emitter.distributions import MULE_ACCOUNT_POOL, LEGITIMATE_ACCOUNT_POOL, MERCHANT_ACCOUNT_POOL, MULE_DORMANCY_TRANSACTIONS


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
    """A normal-looking transaction from a mule account during its dormancy phase.
    Small amount, goes to a merchant — looks like an everyday UPI payment."""
    receiver = random.choice(MERCHANT_ACCOUNT_POOL)
    amount = random.randint(5_000, 50_000)   # \u20b950 – \u20b9500 (small purchase)
    return UPITransaction(
        sender_id=sender,
        receiver_id=receiver,
        amount_paise=amount,
        timestamp=_now(),
        is_synthetic=True,
        # No injected_pattern — these look legitimate in the graph history
    )


async def _warmup_mule(mule_id: str) -> None:
    """Warm up a mule account with MULE_DORMANCY_TRANSACTIONS normal transactions.
    Caller should yield these; defined separately to keep generator signatures clean."""
    for _ in range(MULE_DORMANCY_TRANSACTIONS):
        yield _make_warmup_txn(mule_id)
        await asyncio.sleep(random.uniform(0.08, 0.4))


# ─── Attack Scenario Generators ───────────────────────────────────────────────


async def generate_fan_out_attack(
    num_receivers: int | None = None,
) -> AsyncGenerator[UPITransaction, None]:
    """
    Simulate a Fan-Out scatter attack.

    Phase 1 (Dormancy): The mule account makes N normal-looking small purchases.
    Phase 2 (Attack):   The same account rapidly sends large amounts to many distinct
                        receivers — the structural signature the detector catches.

    The dormancy phase builds up a legitimate-looking behavioral history in the
    graph engine so the detection is meaningful, not trivially obvious.
    """
    n = num_receivers or random.randint(6, 12)
    sender = random.choice(MULE_ACCOUNT_POOL)
    safe_pool = [a for a in LEGITIMATE_ACCOUNT_POOL if a != sender]
    receivers = random.sample(safe_pool, min(n, len(safe_pool)))
    amounts = [random.randint(50_000_00, 200_000_00) for _ in receivers]

    # Phase 1: dormancy warmup
    async for txn in _warmup_mule(sender):
        yield txn

    # Phase 2: rapid fan-out scatter
    for receiver, amount in zip(receivers, amounts):
        yield _make_txn(sender, receiver, amount, FraudPattern.FAN_OUT)
        await asyncio.sleep(random.uniform(0.05, 0.25))


async def generate_fan_in_attack(
    num_senders: int | None = None,
) -> AsyncGenerator[UPITransaction, None]:
    """
    Simulate a Fan-In mule aggregation attack.

    Phase 1 (Dormancy): The collector mule account makes a few normal purchases
                        to establish a behavioral baseline in the graph.
    Phase 2 (Attack):   Multiple feeder accounts rapidly send funds to the same collector.
    """
    n = num_senders or random.randint(5, 10)
    collector = random.choice(MULE_ACCOUNT_POOL)
    safe_pool = [a for a in LEGITIMATE_ACCOUNT_POOL if a != collector]
    senders = random.sample(safe_pool, min(n, len(safe_pool)))
    amounts = [random.randint(5_000_00, 30_000_00) for _ in senders]

    # Phase 1: collector makes normal transactions to look legitimate
    async for txn in _warmup_mule(collector):
        yield txn

    # Phase 2: fan-in attack
    for sender, amount in zip(senders, amounts):
        yield _make_txn(sender, collector, amount, FraudPattern.FAN_IN)
        await asyncio.sleep(random.uniform(0.1, 0.6))


async def generate_scatter_gather_attack(
    num_intermediaries: int | None = None,
    hops: int = 2,
) -> AsyncGenerator[UPITransaction, None]:
    """
    Simulate a Scatter-Gather (Smurfing) attack:
    Origin → split across M intermediaries → re-converge at single collector.

    Real-world context: Organised crime using layered mule accounts to break
    the audit trail between source (stolen funds) and destination (withdrawal).
    """
    n = num_intermediaries or random.randint(3, 7)
    origin = random.choice(MULE_ACCOUNT_POOL)
    collector = random.choice([m for m in MULE_ACCOUNT_POOL if m != origin])
    safe_pool = [a for a in LEGITIMATE_ACCOUNT_POOL if a not in (origin, collector)]
    intermediaries = random.sample(safe_pool, min(n, len(safe_pool)))

    total_amount = random.randint(50_000_00, 200_000_00)
    # Split unevenly to avoid obvious round-number detection
    splits = _uneven_split(total_amount, n)

    # Scatter phase: origin → intermediaries
    for inter, amount in zip(intermediaries, splits):
        yield _make_txn(origin, inter, amount, FraudPattern.SCATTER_GATHER)
        await asyncio.sleep(random.uniform(0.05, 0.2))

    # Brief pause to simulate layering delay
    await asyncio.sleep(random.uniform(1.0, 3.0))

    # Gather phase: intermediaries → collector
    for inter, amount in zip(intermediaries, splits):
        # Slightly reduced amounts (simulate fee skimming / partial withdrawal)
        gather_amount = int(amount * random.uniform(0.85, 0.98))
        if gather_amount > 0:
            yield _make_txn(inter, collector, gather_amount, FraudPattern.SCATTER_GATHER)
            await asyncio.sleep(random.uniform(0.05, 0.3))


async def generate_velocity_abuse_attack(
    burst_count: int | None = None,
) -> AsyncGenerator[UPITransaction, None]:
    """
    Simulate a velocity abuse burst:
    A single account fires many transactions in rapid succession.

    Real-world context: Automated fraud tooling issuing mass payment requests
    or API abuse of a compromised UPI account.
    """
    count = burst_count or random.randint(25, 60)
    sender = random.choice(MULE_ACCOUNT_POOL)
    safe_pool = [a for a in LEGITIMATE_ACCOUNT_POOL if a != sender]
    receivers = random.choices(safe_pool, k=count)

    for receiver in receivers:
        amount = random.randint(100_00, 5_000_00)
        yield _make_txn(sender, receiver, amount, FraudPattern.VELOCITY_ABUSE)
        await asyncio.sleep(random.uniform(0.01, 0.05))  # 10-50ms bursts


# ─── Attack Scheduler ─────────────────────────────────────────────────────────


ATTACK_GENERATORS = [
    (generate_fan_out_attack,        "Fan-Out Scatter"),
    (generate_fan_in_attack,         "Fan-In Aggregation"),
    (generate_scatter_gather_attack, "Scatter-Gather Smurfing"),
    (generate_velocity_abuse_attack, "Velocity Abuse Burst"),
]


async def random_attack() -> AsyncGenerator[UPITransaction, None]:
    """Randomly select and execute one attack scenario."""
    generator_fn, name = random.choice(ATTACK_GENERATORS)
    # Import here to avoid circular at module level
    import logging
    logging.getLogger(__name__).info("💉 Injecting fraud scenario: %s", name)
    async for txn in generator_fn():
        yield txn


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _uneven_split(total: int, n: int) -> list[int]:
    """Split `total` into `n` uneven non-zero parts (avoids suspicious round splits)."""
    if n <= 1:
        return [total]
    cuts = sorted(random.sample(range(1, total), min(n - 1, total - 1)))
    parts = [cuts[0]] + [cuts[i] - cuts[i - 1] for i in range(1, len(cuts))] + [total - cuts[-1]]
    return [max(100, p) for p in parts][:n]
