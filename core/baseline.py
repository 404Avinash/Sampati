"""
decode_sih / core / baseline.py
───────────────────────────────
Exponentially Weighted Moving Average (EWMA) for online variance/mean computation.
Used to establish a dynamic per-account baseline for transactional behaviour
that adapts to legitimate changes while resisting fraud poisoning.
"""


import math
from typing import Tuple

import redis.asyncio as redis
from pydantic import BaseModel

class AccountBaseline(BaseModel):
    n_samples: int = 0
    ewma_mean: float = 0.0
    ewma_variance: float = 0.0

    @property
    def mean(self) -> float:
        return self.ewma_mean

    @property
    def stddev(self) -> float:
        return math.sqrt(self.ewma_variance)


class BaselineEngine:
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis = redis.from_url(redis_url, decode_responses=True)

    async def get_baseline(self, account_id: str) -> AccountBaseline:
        data = await self.redis.get(f"baseline:{account_id}")
        if data:
            return AccountBaseline.model_validate_json(data)
        return AccountBaseline()

    async def update(self, account_id: str, value: float) -> Tuple[bool, float]:
        """
        Updates the baseline with a new value using EWMA.
        Returns a tuple: (is_outlier, z_score).
        is_outlier is True if the value is > mean + 2.5*stddev.
        """
        baseline = await self.get_baseline(account_id)
        alpha = 0.15 # Decay factor: heavily weights the last ~6 transactions

        # Calculate z-score before updating, using previous baseline
        z_score = 0.0
        is_outlier = False
        
        if baseline.n_samples > 0:
            stddev = baseline.stddev
            if stddev > 0:
                z_score = (value - baseline.mean) / stddev
            elif value > baseline.mean:
                z_score = value - baseline.mean
                
            # If standard deviation is very small, use a minimum of 1.0
            is_outlier = value > (baseline.mean + 2.5 * max(stddev, 1.0))
        else:
            baseline.ewma_mean = value
            baseline.ewma_variance = 0.0
            is_outlier = False

        # EWMA update (Poisoning resistance)
        # If the transaction is a massive outlier, we reduce alpha drastically 
        # so the fraudster cannot artificially pull up the baseline.
        effective_alpha = alpha if not is_outlier else alpha * 0.05
        
        if baseline.n_samples > 0:
            diff = value - baseline.ewma_mean
            baseline.ewma_mean += effective_alpha * diff
            baseline.ewma_variance = (1 - effective_alpha) * (baseline.ewma_variance + effective_alpha * diff ** 2)

        baseline.n_samples += 1

        # Save back to Redis
        await self.redis.set(f"baseline:{account_id}", baseline.model_dump_json())

        return is_outlier, z_score
