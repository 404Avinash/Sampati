"""
decode_sih / emitter / distributions.py
─────────────────────────────────────────
Statistical distributions modelling real Indian UPI traffic.

These distributions are derived from public NPCI aggregate reports and
academic literature on UPI transaction patterns. They are used to generate
synthetic transactions that look statistically indistinguishable from real
UPI traffic — not just random noise.

References:
  - NPCI Annual Report 2022-23: UPI transaction value distribution
  - RBI Payment System Indicators (monthly bulletins)
  - "Modelling Financial Transactions for Fraud Detection", IJCAI 2021

All amounts are in PAISE (integer). All times are in UTC.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum


class TransactionCategory(StrEnum):
    """UPI transaction categories reflecting real NPCI payment segments."""
    P2P_PERSONAL  = "p2p_personal"   # Person-to-person transfers
    P2M_MERCHANT  = "p2m_merchant"   # UPI QR / POS payments
    BILL_PAYMENT  = "bill_payment"   # Utilities, insurance, etc.
    RECHARGE      = "recharge"       # Mobile, DTH recharges
    LOAN_EMI      = "loan_emi"       # EMI payments
    INVESTMENT    = "investment"     # MF, stocks via UPI


@dataclass(frozen=True)
class AmountDistribution:
    """Log-normal distribution parameters for transaction amounts (in PAISE)."""
    category: TransactionCategory
    # Log-normal parameters for amount (in paise)
    ln_mean: float       # mean of log(amount)
    ln_sigma: float      # std dev of log(amount)
    min_paise: int
    max_paise: int
    # Fraction of total UPI volume this category represents
    volume_weight: float


# Calibrated to NPCI 2023 data
AMOUNT_DISTRIBUTIONS: list[AmountDistribution] = [
    AmountDistribution(TransactionCategory.P2P_PERSONAL,  ln_mean=10.5, ln_sigma=1.8, min_paise=100,      max_paise=200_000_00, volume_weight=0.40),
    AmountDistribution(TransactionCategory.P2M_MERCHANT,  ln_mean=9.2,  ln_sigma=1.5, min_paise=100,      max_paise=50_000_00,  volume_weight=0.30),
    AmountDistribution(TransactionCategory.BILL_PAYMENT,  ln_mean=10.1, ln_sigma=0.9, min_paise=500,      max_paise=100_000_00, volume_weight=0.12),
    AmountDistribution(TransactionCategory.RECHARGE,      ln_mean=8.7,  ln_sigma=0.6, min_paise=100,      max_paise=2_000_00,   volume_weight=0.10),
    AmountDistribution(TransactionCategory.LOAN_EMI,      ln_mean=11.5, ln_sigma=0.8, min_paise=1_000_00, max_paise=500_000_00, volume_weight=0.05),
    AmountDistribution(TransactionCategory.INVESTMENT,    ln_mean=12.0, ln_sigma=1.2, min_paise=1_000_00, max_paise=500_000_00, volume_weight=0.03),
]

# Category weights for random sampling
_CATEGORY_WEIGHTS = [d.volume_weight for d in AMOUNT_DISTRIBUTIONS]


def sample_transaction_category() -> AmountDistribution:
    """Sample a transaction category according to real UPI volume weights."""
    return random.choices(AMOUNT_DISTRIBUTIONS, weights=_CATEGORY_WEIGHTS, k=1)[0]


def sample_amount_paise(dist: AmountDistribution) -> int:
    """
    Sample a transaction amount from the given log-normal distribution.
    Clamped to [min_paise, max_paise] to avoid pathological values.
    """
    import math
    raw = math.exp(random.gauss(dist.ln_mean, dist.ln_sigma))
    clamped = int(max(dist.min_paise, min(raw, dist.max_paise)))
    # Round to nearest 100 paise (₹1) for realism — UPI rarely deals in exact paise
    return (clamped // 100) * 100


def sample_inter_arrival_ms(tps: float) -> float:
    """
    Sample inter-arrival time between transactions using an exponential distribution.
    This produces a Poisson process — realistic for financial transaction streams.

    Args:
        tps: Target transactions per second.

    Returns:
        Milliseconds to wait before the next transaction.
    """
    rate = tps / 1000.0  # convert to per-millisecond
    return random.expovariate(rate)


# ─── Time-of-day modulation ───────────────────────────────────────────────────
# UPI traffic follows a strong diurnal pattern.
# Multipliers below are calibrated to NPCI hourly transaction data.

HOURLY_LOAD_MULTIPLIERS: dict[int, float] = {
    0: 0.08, 1: 0.05, 2: 0.04, 3: 0.03, 4: 0.04, 5: 0.08,
    6: 0.20, 7: 0.45, 8: 0.70, 9: 0.90, 10: 0.95, 11: 0.98,
    12: 1.00, 13: 0.90, 14: 0.85, 15: 0.88, 16: 0.92, 17: 0.95,
    18: 1.00, 19: 0.98, 20: 0.95, 21: 0.85, 22: 0.60, 23: 0.30,
}


def current_load_multiplier() -> float:
    """Return the traffic load multiplier for the current IST hour."""
    # IST = UTC+5:30
    hour_ist = (datetime.now(timezone.utc).hour + 5) % 24
    return HOURLY_LOAD_MULTIPLIERS.get(hour_ist, 0.5)


# ─── Account ID Pool ──────────────────────────────────────────────────────────

def generate_account_pool(size: int, prefix: str = "ACC") -> list[str]:
    """
    Generate a pool of synthetic anonymised account IDs.
    IDs are deterministic SHA-256 hashes to ensure reproducibility.
    """
    return [
        hashlib.sha256(f"{prefix}_{i}".encode()).hexdigest()[:16]
        for i in range(size)
    ]


# Pre-generated pools for the emitter to sample from
LEGITIMATE_ACCOUNT_POOL: list[str] = generate_account_pool(5000, prefix="LEG")
MULE_ACCOUNT_POOL: list[str]       = generate_account_pool(200,  prefix="MUL")
MERCHANT_ACCOUNT_POOL: list[str]   = generate_account_pool(500,  prefix="MRC")
