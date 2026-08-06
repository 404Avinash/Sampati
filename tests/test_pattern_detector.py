"""
tests / test_pattern_detector.py
──────────────────────────────────
Unit tests for the fraud pattern detection algorithms.
Verifies that each detector fires correctly given a constructed graph state
and does NOT fire on clean graphs (zero false positives on simple cases).
"""

import asyncio
from datetime import datetime, timezone

import pytest

from core.graph_engine import BehavioralGraphEngine
from core.models import FraudPattern, RiskVerdict, UPITransaction
from core.pattern_detector import (
    detect_fan_in,
    detect_fan_out,
    detect_velocity_abuse,
)
import time


def make_txn(sender: str, receiver: str, amount: int = 10_000_00) -> UPITransaction:
    return UPITransaction(
        sender_id=sender,
        receiver_id=receiver,
        amount_paise=amount,
        timestamp=datetime.now(timezone.utc),
        is_synthetic=True,
    )


@pytest.fixture
def engine():
    return BehavioralGraphEngine()


class TestFanOutDetector:
    @pytest.mark.asyncio
    async def test_fires_when_threshold_exceeded(self, engine):
        """Should detect Fan-Out when sender reaches ≥5 unique receivers."""
        sender = "mule_sender"
        # Add 6 transactions to 6 different receivers
        for i in range(6):
            await engine.add_transaction(make_txn(sender, f"receiver_{i}"))

        trigger = make_txn(sender, "receiver_6")
        await engine.add_transaction(trigger)

        alert = await detect_fan_out(trigger, engine, time.time())
        assert alert is not None
        assert alert.pattern == FraudPattern.FAN_OUT
        assert sender in alert.implicated_accounts
        assert alert.verdict in (RiskVerdict.FLAG, RiskVerdict.BLOCK)

    @pytest.mark.asyncio
    async def test_does_not_fire_below_threshold(self, engine):
        """Should NOT detect Fan-Out for only 3 receivers (threshold=5)."""
        sender = "normal_sender"
        for i in range(3):
            await engine.add_transaction(make_txn(sender, f"recv_{i}"))

        trigger = make_txn(sender, "recv_4")
        await engine.add_transaction(trigger)

        alert = await detect_fan_out(trigger, engine, time.time())
        assert alert is None

    @pytest.mark.asyncio
    async def test_alert_includes_all_implicated_accounts(self, engine):
        sender = "scatter_mule"
        receivers = [f"vic_{i}" for i in range(8)]
        for r in receivers:
            await engine.add_transaction(make_txn(sender, r))

        trigger = make_txn(sender, "vic_8")
        await engine.add_transaction(trigger)
        alert = await detect_fan_out(trigger, engine, time.time())

        assert alert is not None
        assert sender in alert.implicated_accounts
        # At least some receivers should appear
        assert len(alert.implicated_accounts) >= 5


class TestFanInDetector:
    @pytest.mark.asyncio
    async def test_fires_when_threshold_exceeded(self, engine):
        """Should detect Fan-In when collector receives from ≥5 unique senders."""
        collector = "mule_collector"
        for i in range(6):
            await engine.add_transaction(make_txn(f"sender_{i}", collector))

        trigger = make_txn("sender_6", collector)
        await engine.add_transaction(trigger)

        alert = await detect_fan_in(trigger, engine, time.time())
        assert alert is not None
        assert alert.pattern == FraudPattern.FAN_IN
        assert collector in alert.implicated_accounts

    @pytest.mark.asyncio
    async def test_does_not_fire_below_threshold(self, engine):
        collector = "normal_account"
        for i in range(2):
            await engine.add_transaction(make_txn(f"sender_{i}", collector))

        trigger = make_txn("sender_2", collector)
        await engine.add_transaction(trigger)

        alert = await detect_fan_in(trigger, engine, time.time())
        assert alert is None


class TestVelocityAbuse:
    @pytest.mark.asyncio
    async def test_fires_on_burst(self, engine):
        """Should detect velocity abuse after 25+ transactions in 30s."""
        sender = "bot_account"
        receivers = [f"vic_{i}" for i in range(25)]
        for r in receivers:
            await engine.add_transaction(make_txn(sender, r, amount=500_00))

        trigger = make_txn(sender, "vic_25", amount=500_00)
        await engine.add_transaction(trigger)

        alert = await detect_velocity_abuse(trigger, engine, time.time())
        assert alert is not None
        assert alert.pattern == FraudPattern.VELOCITY_ABUSE

    @pytest.mark.asyncio
    async def test_does_not_fire_on_normal_velocity(self, engine):
        sender = "normal_user"
        for i in range(5):
            await engine.add_transaction(make_txn(sender, f"friend_{i}"))

        trigger = make_txn(sender, "friend_5")
        await engine.add_transaction(trigger)

        alert = await detect_velocity_abuse(trigger, engine, time.time())
        assert alert is None


class TestAlertStructure:
    @pytest.mark.asyncio
    async def test_alert_has_explanation_text(self, engine):
        sender = "explain_test"
        for i in range(6):
            await engine.add_transaction(make_txn(sender, f"r_{i}"))
        trigger = make_txn(sender, "r_6")
        await engine.add_transaction(trigger)
        alert = await detect_fan_out(trigger, engine, time.time())
        assert alert is not None
        assert len(alert.explanation_text) > 50

    @pytest.mark.asyncio
    async def test_alert_latency_recorded(self, engine):
        sender = "latency_test"
        for i in range(6):
            await engine.add_transaction(make_txn(sender, f"r_{i}"))
        trigger = make_txn(sender, "r_6")
        await engine.add_transaction(trigger)
        alert = await detect_fan_out(trigger, engine, time.time())
        assert alert is not None
        assert alert.detection_latency_ms >= 0
        assert isinstance(alert.within_sla, bool)
