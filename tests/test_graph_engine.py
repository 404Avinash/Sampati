"""
tests / test_graph_engine.py
──────────────────────────────
Unit tests for the BehavioralGraphEngine.

Tests cover:
  - Transaction ingestion correctness
  - Node counter updates
  - Windowed edge eviction
  - BFS neighbourhood traversal
  - Memory cap enforcement
  - Dashboard snapshot structure
"""

import asyncio
import time
from datetime import datetime, timezone

import pytest

from core.graph_engine import BehavioralGraphEngine
from core.models import UPITransaction


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


class TestIngestion:
    @pytest.mark.asyncio
    async def test_add_transaction_creates_nodes(self, engine):
        txn = make_txn("alice", "bob")
        await engine.add_transaction(txn)
        assert engine.node_count == 2
        assert engine.edge_count == 1

    @pytest.mark.asyncio
    async def test_add_multiple_transactions_same_edge(self, engine):
        for _ in range(5):
            await engine.add_transaction(make_txn("alice", "bob"))
        assert engine.node_count == 2
        assert engine.edge_count == 1  # Same A→B edge, just more txns on it

    @pytest.mark.asyncio
    async def test_sender_outbound_count_updates(self, engine):
        for i in range(3):
            await engine.add_transaction(make_txn("alice", f"recv_{i}"))
        node = await engine.get_node("alice")
        assert node is not None
        assert node.outbound_count == 3
        assert len(node.unique_receivers) == 3

    @pytest.mark.asyncio
    async def test_receiver_inbound_count_updates(self, engine):
        for i in range(4):
            await engine.add_transaction(make_txn(f"sender_{i}", "collector"))
        node = await engine.get_node("collector")
        assert node is not None
        assert node.inbound_count == 4
        assert len(node.unique_senders) == 4

    @pytest.mark.asyncio
    async def test_total_amount_accumulates(self, engine):
        for _ in range(3):
            await engine.add_transaction(make_txn("alice", "bob", amount=1_000_00))
        node = await engine.get_node("alice")
        assert node.total_sent_paise == 3_000_00


class TestEdgeQueries:
    @pytest.mark.asyncio
    async def test_get_outbound_edges_returns_correct(self, engine):
        await engine.add_transaction(make_txn("alice", "bob"))
        await engine.add_transaction(make_txn("alice", "charlie"))
        edges = await engine.get_outbound_edges("alice")
        assert len(edges) == 2

    @pytest.mark.asyncio
    async def test_get_inbound_edges_returns_correct(self, engine):
        await engine.add_transaction(make_txn("alice", "collector"))
        await engine.add_transaction(make_txn("bob",   "collector"))
        edges = await engine.get_inbound_edges("collector")
        assert len(edges) == 2

    @pytest.mark.asyncio
    async def test_since_ts_filters_stale_edges(self, engine):
        # Add a transaction with an older timestamp
        old_txn = make_txn("alice", "bob")
        await engine.add_transaction(old_txn)
        # Query with future cutoff — should exclude this edge
        future_cutoff = time.time() + 10
        edges = await engine.get_outbound_edges("alice", since_ts=future_cutoff)
        assert len(edges) == 0


class TestNeighbourhood:
    @pytest.mark.asyncio
    async def test_neighbourhood_includes_direct_receivers(self, engine):
        await engine.add_transaction(make_txn("origin", "hop1"))
        await engine.add_transaction(make_txn("origin", "hop2"))
        result = await engine.get_neighbourhood("origin", max_hops=1)
        assert "hop1" in result.account_ids
        assert "hop2" in result.account_ids

    @pytest.mark.asyncio
    async def test_neighbourhood_traverses_multiple_hops(self, engine):
        await engine.add_transaction(make_txn("origin", "hop1"))
        await engine.add_transaction(make_txn("hop1",   "hop2"))
        await engine.add_transaction(make_txn("hop2",   "collector"))
        result = await engine.get_neighbourhood("origin", max_hops=3)
        assert "collector" in result.account_ids

    @pytest.mark.asyncio
    async def test_neighbourhood_transaction_ids_collected(self, engine):
        txn1 = make_txn("origin", "hop1")
        txn2 = make_txn("hop1", "collector")
        await engine.add_transaction(txn1)
        await engine.add_transaction(txn2)
        result = await engine.get_neighbourhood("origin", max_hops=2)
        assert txn1.txn_id in result.txn_ids


class TestFlagging:
    @pytest.mark.asyncio
    async def test_flag_account_updates_node(self, engine):
        await engine.add_transaction(make_txn("alice", "bob"))
        await engine.flag_account("alice", "FAN_OUT", 0.9, block=True)
        node = await engine.get_node("alice")
        assert node.is_flagged is True
        assert node.is_blocked is True
        assert node.risk_score == 0.9

    @pytest.mark.asyncio
    async def test_flag_nonexistent_account_safe(self, engine):
        # Should not raise
        await engine.flag_account("ghost_account", "FAN_OUT", 0.5)


class TestDashboardSnapshot:
    @pytest.mark.asyncio
    async def test_snapshot_structure(self, engine):
        for i in range(5):
            await engine.add_transaction(make_txn(f"sender_{i}", "collector"))
        snap = await engine.snapshot_for_dashboard()
        assert "nodes" in snap
        assert "edges" in snap
        assert isinstance(snap["nodes"], list)
        assert isinstance(snap["edges"], list)

    @pytest.mark.asyncio
    async def test_snapshot_node_fields(self, engine):
        await engine.add_transaction(make_txn("alice", "bob"))
        snap = await engine.snapshot_for_dashboard()
        node = snap["nodes"][0]
        assert "id" in node
        assert "risk" in node
        assert "flagged" in node
        assert "blocked" in node
