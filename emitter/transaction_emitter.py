"""
decode_sih / emitter / transaction_emitter.py
──────────────────────────────────────────────
Continuous, statistically realistic UPI transaction stream emitter.

Architecture:
  The emitter runs as an async generator that yields UPITransaction objects
  at the configured TPS rate. The pipeline consumes from this generator.

  Two modes:
  1. SYNTHETIC  — Generates brand-new transactions using statistical distributions
                  calibrated to real NPCI/RBI data. The default.
  2. REPLAY     — Reads a Kaggle/real dataset CSV and replays it at configurable
                  speed, preserving original temporal ordering.

  Fraud injection is orthogonal to mode — the injector fires at the configured
  fraud_rate regardless of the base generation mode.

Stream contract:
  - Each yielded UPITransaction is a valid, immutable Pydantic model.
  - Transactions are yielded in monotonically increasing timestamp order.
  - is_synthetic=True on all generated transactions.
  - injected_pattern is set on fraud transactions (ground truth for evaluation).

Usage:
    from emitter.transaction_emitter import TransactionEmitter

    emitter = TransactionEmitter()
    async for txn in emitter.stream():
        await pipeline.ingest(txn)
"""

from __future__ import annotations

import asyncio
import csv
import logging
import random
import time
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import AsyncGenerator

from config.settings import settings
from core.models import UPITransaction
from emitter.distributions import (
    LEGITIMATE_ACCOUNT_POOL,
    MERCHANT_ACCOUNT_POOL,
    current_load_multiplier,
    sample_amount_paise,
    sample_inter_arrival_ms,
    sample_transaction_category,
)
from emitter.fraud_injector import random_attack

logger = logging.getLogger(__name__)


class EmitterMode(StrEnum):
    SYNTHETIC = "synthetic"
    REPLAY    = "replay"


class TransactionEmitter:
    """
    Produces a continuous, realistic stream of UPI transactions.

    The emitter is a stateful async generator. It tracks whether it is
    running or paused and exposes control methods for the API layer.
    """

    def __init__(
        self,
        mode: EmitterMode = EmitterMode.SYNTHETIC,
        dataset_path: Path | None = None,
    ) -> None:
        self._cfg    = settings.emitter
        self._mode   = mode
        self._path   = dataset_path or self._cfg.dataset_path
        self._running = False
        self._paused  = False

        # Rate tracking
        self._total_emitted: int   = 0
        self._fraud_emitted: int   = 0
        self._start_time: float    = 0.0
        self._last_tps_sample: float = 0.0
        self._tps_window_count: int  = 0
        self._current_tps: float     = 0.0

        # Fraud injection state
        self._next_fraud_at: int = self._next_fraud_index()

        logger.info(
            "TransactionEmitter ready | mode=%s tps=%d fraud_rate=%.1f%%",
            self._mode,
            self._cfg.tps,
            self._cfg.fraud_rate * 100,
        )

    # ─── Public Controls ──────────────────────────────────────────────────────

    def pause(self) -> None:
        self._paused = True
        logger.info("Emitter PAUSED")

    def resume(self) -> None:
        self._paused = False
        logger.info("Emitter RESUMED")

    def stop(self) -> None:
        self._running = False
        logger.info("Emitter STOPPED")

    @property
    def stats(self) -> dict:
        elapsed = time.time() - self._start_time if self._start_time else 0
        return {
            "mode":           self._mode,
            "running":        self._running,
            "paused":         self._paused,
            "total_emitted":  self._total_emitted,
            "fraud_emitted":  self._fraud_emitted,
            "fraud_rate":     (self._fraud_emitted / max(self._total_emitted, 1)),
            "current_tps":    round(self._current_tps, 1),
            "elapsed_seconds": round(elapsed, 1),
        }

    # ─── Main Stream ──────────────────────────────────────────────────────────

    async def stream(self) -> AsyncGenerator[UPITransaction, None]:
        """
        Infinite async generator yielding UPI transactions.

        Caller is responsible for breaking the loop (e.g., via asyncio.Task
        cancellation or the stop() control method).
        """
        self._running = True
        self._start_time = time.time()
        self._last_tps_sample = time.time()

        if self._mode == EmitterMode.REPLAY:
            dataset_files = list(self._path.glob("*.csv"))
            if dataset_files:
                logger.info("Replay mode: found %d CSV file(s)", len(dataset_files))
                async for txn in self._replay_stream(dataset_files[0]):
                    yield txn
                return
            else:
                logger.warning(
                    "No CSV files found in %s — falling back to SYNTHETIC mode",
                    self._path,
                )

        # SYNTHETIC mode
        async for txn in self._synthetic_stream():
            yield txn

    # ─── Synthetic Stream ─────────────────────────────────────────────────────

    async def _synthetic_stream(self) -> AsyncGenerator[UPITransaction, None]:
        """Generate an infinite stream of statistically realistic transactions."""
        fraud_queue: list[UPITransaction] = []

        while self._running:
            # Handle pause
            while self._paused:
                await asyncio.sleep(0.1)

            # Drain any pending fraud transactions first (injected attack sequences)
            if fraud_queue:
                txn = fraud_queue.pop(0)
                yield txn
                self._record_emission(is_fraud=True)
                continue

            # Check if it's time to inject a fraud scenario
            if self._total_emitted >= self._next_fraud_at:
                attack_txns = await self._collect_attack()
                fraud_queue.extend(attack_txns)
                self._next_fraud_at = self._total_emitted + self._next_fraud_index()
                logger.debug(
                    "Queued %d fraud txns | next injection in ~%d txns",
                    len(attack_txns),
                    self._next_fraud_at - self._total_emitted,
                )

            # Generate a normal legitimate transaction
            txn = self._generate_legitimate_txn()
            yield txn
            self._record_emission(is_fraud=False)

            # Throttle to target TPS with time-of-day load modulation
            effective_tps = self._cfg.tps * current_load_multiplier()
            delay_ms = sample_inter_arrival_ms(max(effective_tps, 1.0))
            await asyncio.sleep(delay_ms / 1000.0)

    async def _collect_attack(self) -> list[UPITransaction]:
        """Collect all transactions from a random attack generator into a list."""
        txns: list[UPITransaction] = []
        async for txn in random_attack():
            txns.append(txn)
        return txns

    def _generate_legitimate_txn(self) -> UPITransaction:
        """Sample a statistically realistic legitimate UPI transaction."""
        dist = sample_transaction_category()
        amount = sample_amount_paise(dist)

        # P2P: two random accounts
        # P2M: one random account → one merchant account
        use_merchant = dist.category.value.startswith("p2m")
        sender = random.choice(LEGITIMATE_ACCOUNT_POOL)
        receiver = (
            random.choice(MERCHANT_ACCOUNT_POOL)
            if use_merchant
            else random.choice([a for a in LEGITIMATE_ACCOUNT_POOL if a != sender])
        )

        return UPITransaction(
            sender_id=sender,
            receiver_id=receiver,
            amount_paise=amount,
            timestamp=datetime.now(timezone.utc),
            is_synthetic=True,
        )

    # ─── Replay Stream ────────────────────────────────────────────────────────

    async def _replay_stream(
        self,
        csv_path: Path,
    ) -> AsyncGenerator[UPITransaction, None]:
        """
        Replay a Kaggle-format CSV dataset.

        Expected columns (case-insensitive):
          - sender / sender_id / source
          - receiver / receiver_id / dest / destination
          - amount / amount_paise / value
          - timestamp / time / datetime (optional — uses current time if absent)
          - type (optional — determines fraud injection)

        Transactions are replayed at TPS-throttled rate, not at original timing.
        """
        logger.info("Replaying dataset: %s", csv_path.name)
        count = 0

        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            cols = {c.lower().strip() for c in (reader.fieldnames or [])}

            for row in reader:
                if not self._running:
                    break
                while self._paused:
                    await asyncio.sleep(0.1)

                try:
                    txn = self._row_to_transaction(row, cols)
                    if txn:
                        yield txn
                        self._record_emission(is_fraud=txn.injected_pattern is not None)
                        count += 1
                        delay_ms = sample_inter_arrival_ms(self._cfg.tps)
                        await asyncio.sleep(delay_ms / 1000.0)
                except Exception as e:
                    logger.debug("Skipping malformed row: %s", e)

        logger.info("Replay complete — %d transactions emitted from %s", count, csv_path.name)

        # After replay, switch to synthetic to keep stream alive
        logger.info("Switching to SYNTHETIC mode to maintain stream continuity")
        async for txn in self._synthetic_stream():
            yield txn

    @staticmethod
    def _row_to_transaction(
        row: dict[str, str],
        cols: set[str],
    ) -> UPITransaction | None:
        """Map a CSV row dict to a UPITransaction, tolerating column name variations."""
        def get(candidates: list[str]) -> str | None:
            for c in candidates:
                if c in cols:
                    val = row.get(c, "").strip()
                    if val:
                        return val
            return None

        sender   = get(["sender", "sender_id", "nameorig", "source", "from"])
        receiver = get(["receiver", "receiver_id", "namedest", "destination", "dest", "to"])
        amount_s = get(["amount", "amount_paise", "value", "amt"])

        if not sender or not receiver or not amount_s:
            return None

        try:
            # Convert to paise — assume CSV amounts are in rupees unless flagged
            amount_rupees = float(amount_s.replace(",", "").replace("₹", ""))
            amount_paise = int(amount_rupees * 100)
        except ValueError:
            return None

        if amount_paise <= 0:
            return None
        if sender == receiver:
            return None

        return UPITransaction(
            sender_id=sender[:64],
            receiver_id=receiver[:64],
            amount_paise=amount_paise,
            timestamp=datetime.now(timezone.utc),
            is_synthetic=True,
        )

    # ─── Private Helpers ──────────────────────────────────────────────────────

    def _next_fraud_index(self) -> int:
        """Return the number of legitimate transactions before the next fraud injection."""
        if self._cfg.fraud_rate <= 0:
            return 10**9
        # Poisson-distributed fraud events
        avg_spacing = int(1.0 / self._cfg.fraud_rate)
        return max(1, random.randint(avg_spacing // 2, avg_spacing * 2))

    def _record_emission(self, is_fraud: bool) -> None:
        self._total_emitted += 1
        if is_fraud:
            self._fraud_emitted += 1

        # Update TPS estimate every second
        now = time.time()
        self._tps_window_count += 1
        elapsed = now - self._last_tps_sample
        if elapsed >= 1.0:
            self._current_tps = self._tps_window_count / elapsed
            self._tps_window_count = 0
            self._last_tps_sample = now
