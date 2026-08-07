"""
decode_sih / emitter / distributions.py
─────────────────────────────────────────
Statistical distributions modelling real Indian UPI traffic.

UPDATED: Uses realistic VPA-style account IDs (name@bank format) and a much
larger account pool so that legitimate random traffic NEVER crosses fraud
thresholds by chance (birthday-problem fix).

Pool sizes:
  • 5,000 user accounts  → at 30 TPS, each user sends ~0.36 txns/s avg
    → in 60s window, avg ~3 unique receivers → well below fanout threshold 15
  • 300 named merchants  → realistic Swiggy/Zomato/Amazon style

References:
  - NPCI Annual Report 2022-23
  - RBI Payment System Indicators
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum


class TransactionCategory(StrEnum):
    """UPI transaction categories reflecting real NPCI payment segments."""
    P2P_PERSONAL = "p2p_personal"
    P2M_MERCHANT = "p2m_merchant"
    BILL_PAYMENT = "bill_payment"
    RECHARGE     = "recharge"
    LOAN_EMI     = "loan_emi"
    INVESTMENT   = "investment"


@dataclass(frozen=True)
class AmountDistribution:
    """Log-normal distribution parameters for transaction amounts (in PAISE)."""
    category: TransactionCategory
    ln_mean: float
    ln_sigma: float
    min_paise: int
    max_paise: int
    volume_weight: float


# Calibrated to NPCI 2023 data
AMOUNT_DISTRIBUTIONS: list[AmountDistribution] = [
    AmountDistribution(TransactionCategory.P2P_PERSONAL,  ln_mean=10.5, ln_sigma=1.8, min_paise=100,       max_paise=200_000_00, volume_weight=0.40),
    AmountDistribution(TransactionCategory.P2M_MERCHANT,  ln_mean=9.2,  ln_sigma=1.5, min_paise=100,       max_paise=50_000_00,  volume_weight=0.30),
    AmountDistribution(TransactionCategory.BILL_PAYMENT,  ln_mean=10.1, ln_sigma=0.9, min_paise=500,       max_paise=100_000_00, volume_weight=0.12),
    AmountDistribution(TransactionCategory.RECHARGE,      ln_mean=8.7,  ln_sigma=0.6, min_paise=100,       max_paise=2_000_00,   volume_weight=0.10),
    AmountDistribution(TransactionCategory.LOAN_EMI,      ln_mean=11.5, ln_sigma=0.8, min_paise=1_000_00,  max_paise=500_000_00, volume_weight=0.05),
    AmountDistribution(TransactionCategory.INVESTMENT,    ln_mean=12.0, ln_sigma=1.2, min_paise=1_000_00,  max_paise=500_000_00, volume_weight=0.03),
]

_CATEGORY_WEIGHTS = [d.volume_weight for d in AMOUNT_DISTRIBUTIONS]


def sample_transaction_category() -> AmountDistribution:
    return random.choices(AMOUNT_DISTRIBUTIONS, weights=_CATEGORY_WEIGHTS, k=1)[0]


def sample_amount_paise(dist: AmountDistribution) -> int:
    import math
    raw = math.exp(random.gauss(dist.ln_mean, dist.ln_sigma))
    clamped = int(max(dist.min_paise, min(raw, dist.max_paise)))
    return (clamped // 100) * 100


def sample_inter_arrival_ms(tps: float) -> float:
    rate = tps / 1000.0
    return random.expovariate(rate)


# ─── Time-of-day modulation ───────────────────────────────────────────────────

HOURLY_LOAD_MULTIPLIERS: dict[int, float] = {
    0: 0.08, 1: 0.05, 2: 0.04, 3: 0.03, 4: 0.04, 5: 0.08,
    6: 0.20, 7: 0.45, 8: 0.70, 9: 0.90, 10: 0.95, 11: 0.98,
    12: 1.00, 13: 0.90, 14: 0.85, 15: 0.88, 16: 0.92, 17: 0.95,
    18: 1.00, 19: 0.98, 20: 0.95, 21: 0.85, 22: 0.60, 23: 0.30,
}


def current_load_multiplier() -> float:
    from config.settings import settings  # noqa: PLC0415
    if settings.app.env.value != "production":
        return 1.0
    hour_ist = (datetime.now(timezone.utc).hour + 5) % 24
    return HOURLY_LOAD_MULTIPLIERS.get(hour_ist, 0.5)


# ─── Realistic VPA-style Account ID Generation ────────────────────────────────
# Instead of SHA256 hashes, we generate human-readable UPI VPAs:
#   users    → rahul.s42@okicici, priya.m7@oksbi
#   merchants → swiggy@paytm, amazon@axisbank

_FIRST_NAMES = [
    "rahul", "priya", "amit", "sneha", "rohan", "anjali", "vikram", "pooja",
    "arjun", "neha", "karan", "shreya", "arun", "divya", "suresh", "meera",
    "raj", "sunita", "deepak", "kavya", "nitin", "ritu", "sanjay", "anita",
    "mohit", "nisha", "gaurav", "rekha", "ajay", "usha", "vinay", "geeta",
    "sachin", "lalita", "manish", "shweta", "prakash", "smita", "ashok", "radha",
    "ravi", "leela", "vivek", "saroj", "anil", "kamla", "mukesh", "pushpa",
    "dhruv", "asha", "tarun", "maya", "shiv", "parvati", "harsh", "gita",
    "yash", "kusum", "sumit", "bhavna", "ankit", "mamta", "rohit", "vandana",
    "vishal", "padma", "pankaj", "rani", "alok", "sunanda", "sunil", "mala",
    "rajeev", "hema", "praveen", "nalini", "mahesh", "sarita", "girish", "beena",
    "omkar", "sudha", "hemant", "vimla", "ramesh", "sheela", "dinesh", "laxmi",
    "brijesh", "urvashi", "kedar", "seema", "ganesh", "shobha", "tapan", "charu",
    "abhishek", "tanvi", "devendra", "rashmi", "bhushan", "archana", "umesh", "jayshree",
]

_LAST_INITIALS = "abcdefghijklmnopqrstuvwxyz"

_UPI_BANKS = [
    "okicici", "oksbi", "okhdfc", "okaxis", "paytm", "ybl", "ibl",
]

# Named merchants with real PSPs
_MERCHANTS: list[str] = [
    # Food & Delivery
    "swiggy@icici", "zomato@paytm", "dunzo@ybl", "blinkit@okhdfc",
    "bigbasket@okhdfcbank", "grofers@okaxis", "freshmenu@paytm",
    "dominos@icicipay", "mcdonalds@hdfc", "kfc@axisbank",
    # E-commerce
    "amazon@axisbank", "flipkart@ybl", "myntra@okicici", "nykaa@paytm",
    "ajio@okhdfc", "snapdeal@okaxis", "meesho@paytm", "indiamart@okicici",
    "jiomart@jiopay", "tatacliq@okaxis",
    # Transport
    "ola@oksbi", "uber@paytm", "rapido@okicici", "redbus@okhdfc",
    "irctc@sbi", "makemytrip@okaxis", "goibibo@paytm", "olamoney@paytm",
    # Utilities & Bills
    "tatapower@okhdfc", "bsesbilling@okaxis", "airtel@paytm",
    "jio@paytm", "voda@okicici", "bsnl@oksbi", "hathway@okaxis",
    "indane@oksbi", "hp.gas@okicici", "bharat.petroleum@okhdfc",
    # Finance
    "hdfc.lifeins@okhdfc", "lic@oksbi", "bajajfinance@okaxis",
    "zerodha@okicici", "groww@okhdfc", "upstox@okaxis", "paytmmoney@paytm",
    # Retail & Others
    "dmart@okaxis", "reliancesmartpt@okicici", "spencers@okhdfc",
    "bookmyshow@paytm", "pvrcinemas@okicici", "inoxleisure@okhdfc",
    "pharmeasy@paytm", "netmeds@okicici", "apollomedicals@okhdfc",
    # Govt & Education
    "easemygov@oksbi", "uidai@oksbi", "postoffice@oksbi",
    "byju.learning@okaxis", "unacademy@paytm", "vedantu@okicici",
]


def _generate_user_vpa_pool(size: int) -> list[str]:
    """Generate realistic UPI VPA-style user IDs. Deterministic for reproducibility."""
    pool: list[str] = []
    rng = random.Random(42)  # Fixed seed for reproducibility
    idx = 0
    for first in _FIRST_NAMES:
        for last_initial in _LAST_INITIALS:
            number = rng.randint(1, 99)
            bank = _UPI_BANKS[idx % len(_UPI_BANKS)]
            vpa = f"{first}.{last_initial}{number}@{bank}"
            pool.append(vpa)
            idx += 1
            if len(pool) >= size:
                return pool
    # If we need more, pad with numbered variants
    while len(pool) < size:
        first = rng.choice(_FIRST_NAMES)
        last = rng.choice(_LAST_INITIALS)
        num = rng.randint(100, 9999)
        bank = rng.choice(_UPI_BANKS)
        pool.append(f"{first}.{last}{num}@{bank}")
    return pool


# ─── Pre-generated Pools ──────────────────────────────────────────────────────
# 5,000 user accounts: at 30 TPS in 60s window = 1800 txns split across 5000
# senders → each sender ~0.36 txns/s avg → ~3-4 unique receivers per window
# → WELL below fanout threshold of 15. Normal traffic = zero false positives.

LEGITIMATE_ACCOUNT_POOL: list[str] = _generate_user_vpa_pool(5_000)

# Mule accounts are drawn from OUTSIDE the legitimate pool to make them
# identifiable in post-analysis while still looking like real VPAs.
MULE_ACCOUNT_POOL: list[str] = [
    f"mule{i:04d}@{_UPI_BANKS[i % len(_UPI_BANKS)]}"
    for i in range(200)
]

MERCHANT_ACCOUNT_POOL: list[str] = _MERCHANTS[:]

# Dormancy: mule makes 5 normal-looking purchases before activating
MULE_DORMANCY_TRANSACTIONS: int = 5
