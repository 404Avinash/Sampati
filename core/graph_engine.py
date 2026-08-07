"""
decode_sih / core / graph_engine.py
─────────────────────────────────────
Behavioral Graph Engine — the central nervous system of the fraud detection pipeline.

Architecture:
  • Now completely storage-agnostic via the `GraphStore` interface.
  • Delegates all node/edge mutations and locking to the underlying store.
  • Provides compatibility layers (`EdgeRecord`) for pattern detectors.
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import NamedTuple

from config.settings import settings
from core.models import AccountNode, UPITransaction
from core.storage import GraphStore

logger = logging.getLogger(__name__)


# ─── Internal Edge Representation (For Pattern Compatibility) ───────────────────

@dataclass(slots=True)
class EdgeRecord:
    """
    A single directed edge A → B carrying transaction data.
    Created dynamically from raw `Edge` objects to satisfy the detectors.
    """
    sender_id: str
    receiver_id: str
    txns: deque[tuple[float, int, str]] = field(default_factory=deque)

    def add(self, ts: float, amount_paise: int, txn_id: str) -> None:
        self.txns.append((ts, amount_paise, txn_id))

    @property
    def is_empty(self) -> bool:
        return len(self.txns) == 0

    @property
    def txn_count(self) -> int:
        return len(self.txns)

    @property
    def total_amount_paise(self) -> int:
        return sum(t[1] for t in self.txns)


class SubgraphResult(NamedTuple):
    """Result of a subgraph extraction query for explainability."""
    account_ids: list[str]
    txn_ids: list[str]
    edge_descriptions: list[str]


# ─── Graph Engine ─────────────────────────────────────────────────────────────

class BehavioralGraphEngine:
    """
    Storage-agnostic streaming behavioral graph.
    The engine delegates storage to `self.store` and provides traversal APIs.
    """

    def __init__(self, store: GraphStore) -> None:
        self._cfg = settings.graph
        self.store = store
        logger.info(
            "BehavioralGraphEngine initialised | max_nodes=%d window=%ds (Storage-Agnostic)",
            self._cfg.max_nodes,
            self._cfg.window_seconds,
        )

    # ─── Public API ───────────────────────────────────────────────────────────

    async def add_transaction(self, txn: UPITransaction) -> None:
        """
        Ingest a new transaction and update the behavioral graph state.
        Handles creating nodes if they don't exist and appending edges.
        """
        await self.store.add_edge(
            sender_id=txn.sender_id,
            receiver_id=txn.receiver_id,
            amount_paise=txn.amount_paise,
            txn_id=txn.txn_id,
            ts=txn.timestamp,
        )
        
    async def check_and_add_transaction(self, txn: UPITransaction, window_seconds: int) -> tuple[int, int]:
        """
        Ingest a new transaction atomically, and return the sender's out-degree 
        and receiver's in-degree in the current window.
        Returns: (out_degree, in_degree)
        """
        _, _, out_degree, in_degree = await self.store.check_and_add_edge(
            sender_id=txn.sender_id,
            receiver_id=txn.receiver_id,
            amount_paise=txn.amount_paise,
            txn_id=txn.txn_id,
            ts=txn.timestamp,
            window_seconds=window_seconds,
        )
        return out_degree, in_degree
        
        # Evict stale edges periodically
        cutoff = datetime.fromtimestamp(txn.timestamp.timestamp() - self._cfg.window_seconds, tz=txn.timestamp.tzinfo)
        await self.store.evict_before(cutoff)
        
        node_count = await self.store.get_node_count()
        edge_count = await self.store.get_edge_count()
        logger.debug(
            "Graph ← txn %s | nodes=%d edges=%d",
            txn.txn_id[:8],
            node_count,
            edge_count,
        )

    async def get_outbound_edges(
        self, account_id: str, since_ts: float | None = None
    ) -> list[EdgeRecord]:
        """Return outbound EdgeRecords aggregated by receiver."""
        raw_edges = await self.store.get_out_edges(account_id)
        
        records: dict[str, EdgeRecord] = {}
        for edge in raw_edges:
            ts_float = edge.timestamp.timestamp()
            if since_ts is not None and ts_float < since_ts:
                continue
                
            receiver_id = edge.target_id
            if receiver_id not in records:
                records[receiver_id] = EdgeRecord(sender_id=account_id, receiver_id=receiver_id)
            records[receiver_id].add(ts_float, edge.amount_paise, edge.txn_id)
            
        return list(records.values())

    async def get_inbound_edges(
        self, account_id: str, since_ts: float | None = None
    ) -> list[EdgeRecord]:
        """Return inbound EdgeRecords aggregated by sender."""
        raw_edges = await self.store.get_in_edges(account_id)
        
        records: dict[str, EdgeRecord] = {}
        for edge in raw_edges:
            ts_float = edge.timestamp.timestamp()
            if since_ts is not None and ts_float < since_ts:
                continue
                
            sender_id = edge.target_id
            if sender_id not in records:
                records[sender_id] = EdgeRecord(sender_id=sender_id, receiver_id=account_id)
            records[sender_id].add(ts_float, edge.amount_paise, edge.txn_id)
            
        return list(records.values())

    async def get_node(self, account_id: str) -> AccountNode | None:
        """Retrieve an account node by ID."""
        return await self.store.get_node(account_id)

    async def flag_account(
        self, account_id: str, pattern: FraudPattern, risk_score: float, block: bool = False
    ) -> None:
        """Mark an account as implicated in fraud."""
        await self.store.flag_node(account_id, pattern, risk_score, block)

    async def get_neighbourhood(
        self,
        account_id: str,
        max_hops: int = 2,
    ) -> SubgraphResult:
        """
        BFS traversal from account_id up to max_hops edges.
        """
        visited_accounts: set[str] = set()
        visited_txns: set[str] = set()
        edge_descriptions: list[str] = []
        frontier = {account_id}

        for _ in range(max_hops):
            next_frontier: set[str] = set()
            for node_id in frontier:
                if node_id in visited_accounts:
                    continue
                visited_accounts.add(node_id)

                # Explore outbound edges
                out_edges = await self.get_outbound_edges(node_id)
                for edge in out_edges:
                    if edge.is_empty:
                        continue
                    next_frontier.add(edge.receiver_id)
                    visited_txns.update(t[2] for t in edge.txns)
                    edge_descriptions.append(
                        f"{node_id[:8]}→{edge.receiver_id[:8]} "
                        f"({edge.txn_count} txns, "
                        f"₹{edge.total_amount_paise / 100:.0f} total)"
                    )

                # Explore inbound edges
                in_edges = await self.get_inbound_edges(node_id)
                for edge in in_edges:
                    if edge.is_empty:
                        continue
                    next_frontier.add(edge.sender_id)
                    visited_txns.update(t[2] for t in edge.txns)

            frontier = next_frontier - visited_accounts

        return SubgraphResult(
            account_ids=list(visited_accounts),
            txn_ids=list(visited_txns),
            edge_descriptions=edge_descriptions,
        )

    async def snapshot_for_dashboard(self) -> dict:
        """Delegate snapshotting to the underlying store."""
        return await self.store.snapshot()
