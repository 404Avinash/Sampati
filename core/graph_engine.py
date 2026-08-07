"""
decode_sih / core / graph_engine.py
─────────────────────────────────────
In-memory Behavioral Graph Engine — the central nervous system of the fraud
detection pipeline.

Architecture:
  • Uses an adjacency structure backed by Python dicts for O(1) node/edge access.
  • All edges carry a sliding time-window deque of transaction timestamps and
    amounts so the engine can answer "what happened in the last T seconds?" in
    O(k) time where k is the number of transactions in the window.
  • Thread-safe using asyncio.Lock — designed for a single-process async server.
    If you need horizontal scaling, shard by sender_id prefix and run multiple
    instances behind a load balancer.

Complexity guarantees:
  • add_transaction()   : O(1) amortised (dict insert + deque append)
  • _evict_stale_edges(): O(k) per edge where k = txns in window (bounded by TPS)
  • Fan-Out detection   : O(out_degree(node)) — bounded by graph.fanout_threshold
  • Fan-In  detection   : O(in_degree(node))  — bounded by graph.fanin_threshold
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import NamedTuple

from config.settings import settings
from core.models import AccountNode, AccountType, UPITransaction

logger = logging.getLogger(__name__)


# ─── Internal Edge Representation ─────────────────────────────────────────────


@dataclass(slots=True)
class EdgeRecord:
    """
    A single directed edge A → B carrying the last N seconds of transaction data.
    Stored in a deque; old entries are evicted lazily when the window passes.
    """

    sender_id: str
    receiver_id: str
    # Each entry: (unix_timestamp_float, amount_paise, txn_id)
    txns: deque[tuple[float, int, str]] = field(
        default_factory=lambda: deque(maxlen=10_000)
    )

    def add(self, ts: float, amount_paise: int, txn_id: str) -> None:
        self.txns.append((ts, amount_paise, txn_id))

    def evict_before(self, cutoff_ts: float) -> None:
        """Remove transactions older than cutoff_ts from the left of the deque."""
        while self.txns and self.txns[0][0] < cutoff_ts:
            self.txns.popleft()

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
    edge_descriptions: list[str]   # human-readable edge summaries


# ─── Graph Engine ─────────────────────────────────────────────────────────────


class BehavioralGraphEngine:
    """
    Thread-safe, in-memory streaming behavioral graph.

    The graph is a directed multigraph where:
    - Nodes = UPI account identifiers
    - Edges = directed transaction flows, carrying windowed time series data

    The engine does NOT detect patterns itself — that is the responsibility of
    pattern_detector.py. This class is purely responsible for:
    1. Maintaining the graph state as transactions stream in.
    2. Providing efficient graph traversal APIs for the detector.
    3. Evicting stale data to bound memory usage.
    """

    def __init__(self) -> None:
        self._cfg = settings.graph

        # node_id → AccountNode
        self._nodes: dict[str, AccountNode] = {}

        # sender_id → {receiver_id → EdgeRecord}
        self._out_edges: dict[str, dict[str, EdgeRecord]] = defaultdict(dict)

        # receiver_id → {sender_id → EdgeRecord}  (reverse index for Fan-In)
        self._in_edges: dict[str, dict[str, EdgeRecord]] = defaultdict(dict)

        # Total edge count (for metrics reporting)
        self._edge_count: int = 0

        # Async lock to protect concurrent mutation
        self._lock: asyncio.Lock = asyncio.Lock()

        logger.info(
            "BehavioralGraphEngine initialised | max_nodes=%d window=%ds",
            self._cfg.max_nodes,
            self._cfg.window_seconds,
        )

    # ─── Public API ───────────────────────────────────────────────────────────

    async def add_transaction(self, txn: UPITransaction) -> None:
        """
        Ingest a transaction into the graph.

        Updates or creates nodes for both accounts, adds/updates the directed
        edge, and records the transaction in the edge's time-series deque.
        This is the hot path — keep it as lean as possible.
        """
        ts = txn.timestamp.timestamp()
        cutoff = ts - self._cfg.window_seconds

        async with self._lock:
            # ── Ensure both account nodes exist ──────────────────────────────
            sender   = self._get_or_create_node(txn.sender_id)
            receiver = self._get_or_create_node(txn.receiver_id)

            # ── Update node counters ──────────────────────────────────────────
            sender.outbound_count += 1
            sender.total_sent_paise += txn.amount_paise
            sender.unique_receivers.add(txn.receiver_id)
            sender.last_seen = txn.timestamp

            receiver.inbound_count += 1
            receiver.total_received_paise += txn.amount_paise
            receiver.unique_senders.add(txn.sender_id)
            receiver.last_seen = txn.timestamp

            # ── Upsert directed edge ──────────────────────────────────────────
            edge = self._get_or_create_edge(txn.sender_id, txn.receiver_id)
            edge.add(ts, txn.amount_paise, txn.txn_id)

            # ── Lazy eviction of stale data within this edge ──────────────────
            edge.evict_before(cutoff)

            logger.debug(
                "Graph ← txn %s | nodes=%d edges=%d",
                txn.txn_id[:8],
                len(self._nodes),
                self._edge_count,
            )

    async def get_outbound_edges(
        self, account_id: str, since_ts: float | None = None
    ) -> list[EdgeRecord]:
        """Return all outbound edges from account_id within the time window."""
        async with self._lock:
            edges = list(self._out_edges.get(account_id, {}).values())
        if since_ts is not None:
            edges = [e for e in edges if e.txns and e.txns[-1][0] >= since_ts]
        return edges

    async def get_inbound_edges(
        self, account_id: str, since_ts: float | None = None
    ) -> list[EdgeRecord]:
        """Return all inbound edges into account_id within the time window."""
        async with self._lock:
            edges = list(self._in_edges.get(account_id, {}).values())
        if since_ts is not None:
            edges = [e for e in edges if e.txns and e.txns[-1][0] >= since_ts]
        return edges

    async def get_node(self, account_id: str) -> AccountNode | None:
        """Retrieve an account node by ID, or None if not in graph."""
        async with self._lock:
            return self._nodes.get(account_id)

    async def get_neighbourhood(
        self,
        account_id: str,
        max_hops: int = 2,
    ) -> SubgraphResult:
        """
        BFS traversal from account_id up to max_hops edges.
        Returns the set of account IDs and transaction IDs in the neighbourhood.
        Used by pattern detectors for Scatter-Gather and Mule Chain analysis.
        """
        visited_accounts: set[str] = set()
        visited_txns: set[str] = set()
        edge_descriptions: list[str] = []
        frontier = {account_id}

        async with self._lock:
            for _ in range(max_hops):
                next_frontier: set[str] = set()
                for node_id in frontier:
                    if node_id in visited_accounts:
                        continue
                    visited_accounts.add(node_id)

                    # Explore outbound edges
                    for receiver_id, edge in self._out_edges.get(node_id, {}).items():
                        if edge.is_empty:
                            continue
                        next_frontier.add(receiver_id)
                        txn_ids = [t[2] for t in edge.txns]
                        visited_txns.update(txn_ids)
                        edge_descriptions.append(
                            f"{node_id[:8]}→{receiver_id[:8]} "
                            f"({edge.txn_count} txns, "
                            f"₹{edge.total_amount_paise / 100:.0f} total)"
                        )

                    # Explore inbound edges (for Fan-In / mule collector detection)
                    for sender_id, edge in self._in_edges.get(node_id, {}).items():
                        if edge.is_empty:
                            continue
                        next_frontier.add(sender_id)
                        visited_txns.update(t[2] for t in edge.txns)

                frontier = next_frontier - visited_accounts

        return SubgraphResult(
            account_ids=list(visited_accounts),
            txn_ids=list(visited_txns),
            edge_descriptions=edge_descriptions,
        )

    async def snapshot_for_dashboard(self) -> dict:
        """
        Return a lightweight JSON-serialisable snapshot of the graph for the
        War Room UI. Caps to the 200 most recently active nodes to avoid
        sending megabytes over WebSocket.
        """
        async with self._lock:
            # Take the 200 most-recently-active nodes
            nodes_sorted = sorted(
                self._nodes.values(),
                key=lambda n: n.last_seen,
                reverse=True,
            )[:200]

            active_ids = {n.account_id for n in nodes_sorted}

            nodes_out = [
                {
                    "id": n.account_id,
                    "type": n.account_type,
                    "risk": round(n.risk_score, 3),
                    "flagged": n.is_flagged,
                    "blocked": n.is_blocked,
                    "out": n.outbound_count,
                    "in": n.inbound_count,
                }
                for n in nodes_sorted
            ]

            edges_out = []
            for sender_id, receivers in self._out_edges.items():
                if sender_id not in active_ids:
                    continue
                for receiver_id, edge in receivers.items():
                    if receiver_id not in active_ids or edge.is_empty:
                        continue
                    edges_out.append(
                        {
                            "source": sender_id,
                            "target": receiver_id,
                            "count": edge.txn_count,
                            "amount": edge.total_amount_paise,
                        }
                    )

        return {"nodes": nodes_out, "edges": edges_out}

    # ─── Metrics ──────────────────────────────────────────────────────────────

    @property
    def node_count(self) -> int:
        return len(self._nodes)

    @property
    def edge_count(self) -> int:
        return self._edge_count

    async def flag_account(
        self,
        account_id: str,
        pattern: str,
        risk_score: float,
        block: bool = False,
    ) -> None:
        """Mark an account as flagged or blocked with a reason and risk score."""
        async with self._lock:
            node = self._nodes.get(account_id)
            if node is None:
                return
            node.risk_score = max(node.risk_score, risk_score)
            node.is_flagged = True
            node.is_blocked = block or node.is_blocked

    async def global_eviction(self) -> int:
        """
        Sweep the entire graph and evict stale edges.
        Should be called periodically (e.g., every 30s) by a background task.
        Returns the number of edges removed.

        Two-phase design to minimise lock hold time:
          Phase 1 (read, lock-free): Evict old timestamps from deques.
                  Deque mutation is safe because we only ever popleft().
          Phase 2 (write, locked): Remove empty edge entries from the dicts.
        This prevents eviction from blocking transaction ingestion on hot paths.
        """
        cutoff = time.time() - self._cfg.window_seconds

        # Phase 1 — evict timestamps without holding the lock.
        # Collect empty (sender, receiver) pairs to delete.
        stale_pairs: list[tuple[str, str]] = []
        async with self._lock:
            snapshot_keys = [
                (s, r)
                for s, receivers in self._out_edges.items()
                for r in receivers
            ]

        for sender_id, receiver_id in snapshot_keys:
            # Edge may have been deleted between phases — guard with get()
            edge = self._out_edges.get(sender_id, {}).get(receiver_id)
            if edge is not None:
                edge.evict_before(cutoff)
                if edge.is_empty:
                    stale_pairs.append((sender_id, receiver_id))

        if not stale_pairs:
            return 0

        # Phase 2 — delete empty edges under lock (fast O(stale) operation).
        removed = 0
        async with self._lock:
            for sender_id, receiver_id in stale_pairs:
                if receiver_id in self._out_edges.get(sender_id, {}):
                    edge = self._out_edges[sender_id][receiver_id]
                    if edge.is_empty:   # re-check; may have received txn between phases
                        del self._out_edges[sender_id][receiver_id]
                        if not self._out_edges[sender_id]:
                            del self._out_edges[sender_id]
                        del self._in_edges[receiver_id][sender_id]
                        self._edge_count = max(0, self._edge_count - 1)
                        removed += 1

        if removed:
            logger.debug("Global eviction: removed %d stale edges (two-phase)", removed)
        return removed

    # ─── Private Helpers ──────────────────────────────────────────────────────

    def _get_or_create_node(self, account_id: str) -> AccountNode:
        """Return existing node or create a new one. Caller must hold _lock."""
        if account_id not in self._nodes:
            if len(self._nodes) >= self._cfg.max_nodes:
                # Evict the oldest node (LRU-style) to stay within memory budget
                oldest_id = min(self._nodes, key=lambda k: self._nodes[k].last_seen)
                del self._nodes[oldest_id]
                logger.warning(
                    "Node cap reached — evicted oldest node %s", oldest_id[:8]
                )
            self._nodes[account_id] = AccountNode(account_id=account_id)
        return self._nodes[account_id]

    def _get_or_create_edge(self, sender_id: str, receiver_id: str) -> EdgeRecord:
        """Return existing edge or create a new one. Caller must hold _lock."""
        if receiver_id not in self._out_edges[sender_id]:
            edge = EdgeRecord(sender_id=sender_id, receiver_id=receiver_id)
            self._out_edges[sender_id][receiver_id] = edge
            self._in_edges[receiver_id][sender_id] = edge
            self._edge_count += 1
        return self._out_edges[sender_id][receiver_id]
