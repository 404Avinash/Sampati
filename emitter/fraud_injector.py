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
from emitter.distributions import MULE_ACCOUNT_POOL, LEGITIMATE_ACCOUNT_POOL


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


# ─── Attack Scenario Generators ───────────────────────────────────────────────


async def generate_fan_out_attack(
    num_receivers: int | None = None,
) -> AsyncGenerator[UPITransaction, None]:
    """
    Simulate a Fan-Out scatter attack:
    A single mule account rapidly sends small amounts to N legitimate accounts.

    Real-world context: A scammer who has received a large payment via social
    engineering immediately disperses it across dozens of accounts to make
    recovery impossible.
    """
    n = num_receivers or random.randint(6, 15)
    sender = random.choice(MULE_ACCOUNT_POOL)
    receivers = random.sample(LEGITIMATE_ACCOUNT_POOL, min(n, len(LEGITIMATE_ACCOUNT_POOL)))

    # Amounts are small and variable — designed to stay below ₹50,000 reporting threshold
    amounts = [random.randint(5_000_00, 49_000_00) for _ in receivers]

    for receiver, amount in zip(receivers, amounts):
        yield _make_txn(sender, receiver, amount, FraudPattern.FAN_OUT)
        # Very short inter-arrival — the hallmark of automated tooling
        await asyncio.sleep(random.uniform(0.05, 0.3))


async def generate_fan_in_attack(
    num_senders: int | None = None,
) -> AsyncGenerator[UPITransaction, None]:
    """
    Simulate a Fan-In mule aggregation attack:
    Multiple feeder accounts funnel funds into a single mule collector.

    Real-world context: Multiple low-value scam victims each send small amounts
    to a "lottery prize" or "customs fee" account, which is the mule.
    """
    n = num_senders or random.randint(5, 12)
    collector = random.choice(MULE_ACCOUNT_POOL)
    senders = random.sample(LEGITIMATE_ACCOUNT_POOL, min(n, len(LEGITIMATE_ACCOUNT_POOL)))
    amounts = [random.randint(2_000_00, 25_000_00) for _ in senders]

    for sender, amount in zip(senders, amounts):
        yield _make_txn(sender, collector, amount, FraudPattern.FAN_IN)
        await asyncio.sleep(random.uniform(0.1, 0.8))


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
    intermediaries = random.sample(
        LEGITIMATE_ACCOUNT_POOL, min(n, len(LEGITIMATE_ACCOUNT_POOL))
    )

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
    receivers = random.choices(LEGITIMATE_ACCOUNT_POOL, k=count)

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
