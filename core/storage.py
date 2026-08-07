"""
decode_sih / core / storage.py
───────────────────────────────
Storage-agnostic interfaces for the Behavioral Graph Engine.

This module defines the `GraphStore` Protocol, ensuring that the core
pattern detection algorithms are completely decoupled from how nodes and 
edges are persisted (e.g., in-memory dicts vs. Redis).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Protocol, runtime_checkable

from core.models import AccountNode, AccountType, Edge, FraudPattern

logger = logging.getLogger(__name__)


@runtime_checkable
class GraphStore(Protocol):
    """
    The abstraction contract for all graph backends.
    Any store (Redis, Neo4j, etc.) must implement these methods.
    """

    async def add_edge(
        self,
        sender_id: str,
        receiver_id: str,
        amount_paise: int,
        txn_id: str,
        ts: datetime,
    ) -> tuple[AccountNode, AccountNode]:
        """
        Record a directed edge and return the updated sender and receiver nodes.
        """
        ...

    async def check_and_add_edge(
        self,
        sender_id: str,
        receiver_id: str,
        amount_paise: int,
        txn_id: str,
        ts: datetime,
        window_seconds: int,
    ) -> tuple[AccountNode, AccountNode, int, int]:
        """
        Atomically add an edge and return the nodes plus the sender's out-degree 
        and receiver's in-degree within the sliding window.
        Returns: (sender_node, receiver_node, out_degree, in_degree)
        """
        ...

    async def get_out_edges(self, account_id: str) -> list[Edge]:
        """Get all outbound edges for a node."""
        ...

    async def get_in_edges(self, account_id: str) -> list[Edge]:
        """Get all inbound edges for a node."""
        ...

    async def get_node(self, account_id: str) -> AccountNode | None:
        """Fetch a specific node by ID."""
        ...

    async def flag_node(self, account_id: str, pattern: FraudPattern, risk_score: float, block: bool) -> None:
        """Mark a node as implicated in a fraud pattern."""
        ...

    async def evict_before(self, cutoff_ts: datetime) -> int:
        """Purge all edges older than the cutoff. Returns number of edges removed."""
        ...

    async def get_node_count(self) -> int:
        """Return the total number of active nodes."""
        ...

    async def get_edge_count(self) -> int:
        """Return the total number of active edges."""
        ...

    async def snapshot(self) -> dict:
        """Return a serializable snapshot of the entire graph for dashboard visualization."""
        ...


class InMemoryGraphStore(GraphStore):
    """
    A blazing-fast, single-process implementation using Python dictionaries
    and asyncio.Lock. Ideal for local dev, hackathons, and sub-ms benchmarks.
    """

    def __init__(self) -> None:
        self._nodes: dict[str, AccountNode] = {}
        self._lock = asyncio.Lock()
        self._edge_count = 0

    async def add_edge(
        self,
        sender_id: str,
        receiver_id: str,
        amount_paise: int,
        txn_id: str,
        ts: datetime,
    ) -> tuple[AccountNode, AccountNode]:
        async with self._lock:
            # 1. Upsert Sender
            if sender_id not in self._nodes:
                self._nodes[sender_id] = AccountNode(account_id=sender_id, first_seen=ts)
            sender = self._nodes[sender_id]
            sender.last_seen = ts
            
            # 2. Upsert Receiver
            if receiver_id not in self._nodes:
                self._nodes[receiver_id] = AccountNode(account_id=receiver_id, first_seen=ts)
            receiver = self._nodes[receiver_id]
            receiver.last_seen = ts

            # 3. Create Edges
            out_edge = Edge(txn_id=txn_id, target_id=receiver_id, amount_paise=amount_paise, timestamp=ts)
            in_edge = Edge(txn_id=txn_id, target_id=sender_id, amount_paise=amount_paise, timestamp=ts)

            # 4. Update Node State
            sender.out_edges.append(out_edge)
            sender.outbound_count += 1
            sender.total_sent_paise += amount_paise

            receiver.in_edges.append(in_edge)
            receiver.inbound_count += 1
            receiver.total_received_paise += amount_paise

            self._edge_count += 1

            # Return a copy to prevent downstream mutation issues
    async def check_and_add_edge(
        self,
        sender_id: str,
        receiver_id: str,
        amount_paise: int,
        txn_id: str,
        ts: datetime,
        window_seconds: int,
    ) -> tuple[AccountNode, AccountNode, int, int]:
        async with self._lock:
            cutoff = ts.timestamp() - window_seconds
            
            # 1. Upsert Sender
            if sender_id not in self._nodes:
                self._nodes[sender_id] = AccountNode(account_id=sender_id, first_seen=ts)
            sender = self._nodes[sender_id]
            sender.last_seen = ts
            
            # 2. Upsert Receiver
            if receiver_id not in self._nodes:
                self._nodes[receiver_id] = AccountNode(account_id=receiver_id, first_seen=ts)
            receiver = self._nodes[receiver_id]
            receiver.last_seen = ts

            # 3. Create Edges
            out_edge = Edge(txn_id=txn_id, target_id=receiver_id, amount_paise=amount_paise, timestamp=ts)
            in_edge = Edge(txn_id=txn_id, target_id=sender_id, amount_paise=amount_paise, timestamp=ts)

            # 4. Update Node State
            sender.out_edges.append(out_edge)
            sender.outbound_count += 1
            sender.total_sent_paise += amount_paise

            receiver.in_edges.append(in_edge)
            receiver.inbound_count += 1
            receiver.total_received_paise += amount_paise

            self._edge_count += 1
            
            # Calculate degrees in window
            out_degree = len({e.target_id for e in sender.out_edges if e.timestamp.timestamp() >= cutoff})
            in_degree = len({e.target_id for e in receiver.in_edges if e.timestamp.timestamp() >= cutoff})

            return sender.model_copy(deep=True), receiver.model_copy(deep=True), out_degree, in_degree

    async def get_out_edges(self, account_id: str) -> list[Edge]:
        async with self._lock:
            node = self._nodes.get(account_id)
            return list(node.out_edges) if node else []

    async def get_in_edges(self, account_id: str) -> list[Edge]:
        async with self._lock:
            node = self._nodes.get(account_id)
            return list(node.in_edges) if node else []

    async def get_node(self, account_id: str) -> AccountNode | None:
        async with self._lock:
            node = self._nodes.get(account_id)
            return node.model_copy(deep=True) if node else None

    async def flag_node(self, account_id: str, pattern: FraudPattern, risk_score: float, block: bool) -> None:
        async with self._lock:
            if account_id in self._nodes:
                node = self._nodes[account_id]
                node.flags.append(pattern.value)
                node.risk_score = max(node.risk_score, risk_score)
                if block:
                    node.is_blocked = True

    async def evict_before(self, cutoff_ts: datetime) -> int:
        removed_edges = 0
        nodes_to_delete = []

        async with self._lock:
            for account_id, node in self._nodes.items():
                # Filter out_edges
                initial_out = len(node.out_edges)
                node.out_edges = [e for e in node.out_edges if e.timestamp >= cutoff_ts]
                removed_out = initial_out - len(node.out_edges)

                # Filter in_edges
                initial_in = len(node.in_edges)
                node.in_edges = [e for e in node.in_edges if e.timestamp >= cutoff_ts]
                removed_in = initial_in - len(node.in_edges)

                removed_edges += removed_out  # We only count out_edges to avoid double-counting global edges

                if not node.out_edges and not node.in_edges and not node.is_blocked:
                    nodes_to_delete.append(account_id)

            for nid in nodes_to_delete:
                del self._nodes[nid]
                
            self._edge_count -= removed_edges
            return removed_edges

    async def get_node_count(self) -> int:
        async with self._lock:
            return len(self._nodes)

    async def get_edge_count(self) -> int:
        async with self._lock:
            return self._edge_count

    async def snapshot(self) -> dict:
        async with self._lock:
            nodes = []
            links = []
            for n in self._nodes.values():
                # Dashboard payload needs basic attributes
                group = 0
                if n.is_blocked: group = 2
                elif n.flags: group = 1
                
                nodes.append({
                    "id": n.account_id,
                    "group": group,
                    "risk_score": n.risk_score,
                })
                # Add out-edges to links
                for edge in n.out_edges:
                    links.append({
                        "source": n.account_id,
                        "target": edge.target_id,
                        "value": edge.amount_paise / 100.0,
                    })
            return {"nodes": nodes, "links": links}
