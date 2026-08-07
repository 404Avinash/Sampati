"""
decode_sih / core / redis_store.py
───────────────────────────────
Redis-backed implementation of the GraphStore protocol.
Allows horizontal scaling across multiple Python processes.
"""

import json
from datetime import datetime
from typing import Optional

import redis.asyncio as redis
from core.models import AccountNode, Edge, FraudPattern
from core.storage import GraphStore

# LUA Script for atomic add_edge
# Ensures we add the edge to both the out_edges and in_edges sorted sets,
# update the node timestamps, and add to active_nodes set atomically.
LUA_ADD_EDGE = """
local sender_key = KEYS[1]
local receiver_key = KEYS[2]
local out_edges_key = KEYS[3]
local in_edges_key = KEYS[4]
local active_nodes_key = KEYS[5]

local score_ts = ARGV[1]
local out_edge_str = ARGV[2]
local in_edge_str = ARGV[3]
local sender_id = ARGV[4]
local receiver_id = ARGV[5]
local ts_iso = ARGV[6]

-- Upsert Sender
if redis.call('EXISTS', sender_key) == 0 then
    redis.call('HSET', sender_key, 'account_id', sender_id, 'first_seen', ts_iso, 'fraud_flags', '[]', 'risk_score', '0.0', 'is_blocked', '0')
    redis.call('SADD', active_nodes_key, sender_id)
end

-- Upsert Receiver
if redis.call('EXISTS', receiver_key) == 0 then
    redis.call('HSET', receiver_key, 'account_id', receiver_id, 'first_seen', ts_iso, 'fraud_flags', '[]', 'risk_score', '0.0', 'is_blocked', '0')
    redis.call('SADD', active_nodes_key, receiver_id)
end

-- Add to ZSETs
redis.call('ZADD', out_edges_key, score_ts, out_edge_str)
redis.call('ZADD', in_edges_key, score_ts, in_edge_str)

-- Increment global edge count
redis.call('INCR', 'global:edge_count')

local s_data = redis.call('HGETALL', sender_key)
local r_data = redis.call('HGETALL', receiver_key)

return {s_data, r_data}
"""

LUA_CHECK_FAN_THRESHOLD = """
local sender_key = KEYS[1]
local receiver_key = KEYS[2]
local out_edges_key = KEYS[3]
local in_edges_key = KEYS[4]
local active_nodes_key = KEYS[5]

local score_ts = tonumber(ARGV[1])
local out_edge_str = ARGV[2]
local in_edge_str = ARGV[3]
local sender_id = ARGV[4]
local receiver_id = ARGV[5]
local ts_iso = ARGV[6]
local window_cutoff = tonumber(ARGV[7])

-- Upsert Sender
if redis.call('EXISTS', sender_key) == 0 then
    redis.call('HSET', sender_key, 'account_id', sender_id, 'first_seen', ts_iso, 'fraud_flags', '[]', 'risk_score', '0.0', 'is_blocked', '0')
    redis.call('SADD', active_nodes_key, sender_id)
end

-- Upsert Receiver
if redis.call('EXISTS', receiver_key) == 0 then
    redis.call('HSET', receiver_key, 'account_id', receiver_id, 'first_seen', ts_iso, 'fraud_flags', '[]', 'risk_score', '0.0', 'is_blocked', '0')
    redis.call('SADD', active_nodes_key, receiver_id)
end

-- Add to ZSETs
redis.call('ZADD', out_edges_key, score_ts, out_edge_str)
redis.call('ZADD', in_edges_key, score_ts, in_edge_str)

-- Increment global edge count
redis.call('INCR', 'global:edge_count')

-- Cleanup old edges for this sender and receiver to get accurate degree counts
redis.call('ZREMRANGEBYSCORE', out_edges_key, '-inf', window_cutoff)
redis.call('ZREMRANGEBYSCORE', in_edges_key, '-inf', window_cutoff)

-- Get the degrees (note: this counts all transactions, not unique accounts, 
-- but that's a sufficient proxy for the threshold check logic in phase 3)
local out_degree = redis.call('ZCARD', out_edges_key)
local in_degree = redis.call('ZCARD', in_edges_key)

local s_data = redis.call('HGETALL', sender_key)
local r_data = redis.call('HGETALL', receiver_key)

return {s_data, r_data, out_degree, in_degree}
"""

class RedisGraphStore(GraphStore):
    """
    Redis-backed GraphStore for production-grade horizontal scalability.
    """
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self._add_edge_script = self.redis.register_script(LUA_ADD_EDGE)
        self._check_and_add_script = self.redis.register_script(LUA_CHECK_FAN_THRESHOLD)

    async def _hgetall_to_node(self, hdata: list) -> AccountNode:
        """Helper to convert Redis HGETALL array (list of strings) to AccountNode."""
        # hdata is like ['account_id', 'alice', 'first_seen', '2023...', ...]
        d = dict(zip(hdata[0::2], hdata[1::2]))
        
        flags = []
        if 'fraud_flags' in d and d['fraud_flags']:
            flags = [FraudPattern(f) for f in json.loads(d['fraud_flags'])]
            
        return AccountNode(
            account_id=d.get('account_id', ''),
            first_seen=datetime.fromisoformat(d.get('first_seen', datetime.utcnow().isoformat())),
            flags=flags,
            risk_score=float(d.get('risk_score', 0.0)),
            is_blocked=d.get('is_blocked') == '1'
        )

    async def add_edge(
        self,
        sender_id: str,
        receiver_id: str,
        amount_paise: int,
        txn_id: str,
        ts: datetime,
    ) -> tuple[AccountNode, AccountNode]:
        score = ts.timestamp()
        
        # We store JSON strings in the ZSET for easy parsing
        out_edge_dict = {
            "target_id": receiver_id,
            "amount_paise": amount_paise,
            "txn_id": txn_id,
            "timestamp": ts.isoformat()
        }
        in_edge_dict = {
            "target_id": sender_id,
            "amount_paise": amount_paise,
            "txn_id": txn_id,
            "timestamp": ts.isoformat()
        }
        
        keys = [
            f"node:{sender_id}",
            f"node:{receiver_id}",
            f"out_edges:{sender_id}",
            f"in_edges:{receiver_id}",
            "active_nodes"
        ]
        
        args = [
            score,
            json.dumps(out_edge_dict),
            json.dumps(in_edge_dict),
            sender_id,
            receiver_id,
            ts.isoformat()
        ]
        
        res = await self._add_edge_script(keys=keys, args=args)
        
        sender_node = await self._hgetall_to_node(res[0])
        receiver_node = await self._hgetall_to_node(res[1])
        
        return sender_node, receiver_node

    async def check_and_add_edge(
        self,
        sender_id: str,
        receiver_id: str,
        amount_paise: int,
        txn_id: str,
        ts: datetime,
        window_seconds: int,
    ) -> tuple[AccountNode, AccountNode, int, int]:
        score = ts.timestamp()
        cutoff = score - window_seconds
        
        out_edge_dict = {
            "target_id": receiver_id,
            "amount_paise": amount_paise,
            "txn_id": txn_id,
            "timestamp": ts.isoformat()
        }
        in_edge_dict = {
            "target_id": sender_id,
            "amount_paise": amount_paise,
            "txn_id": txn_id,
            "timestamp": ts.isoformat()
        }
        
        keys = [
            f"node:{sender_id}",
            f"node:{receiver_id}",
            f"out_edges:{sender_id}",
            f"in_edges:{receiver_id}",
            "active_nodes"
        ]
        
        args = [
            score,
            json.dumps(out_edge_dict),
            json.dumps(in_edge_dict),
            sender_id,
            receiver_id,
            ts.isoformat(),
            cutoff
        ]
        
        res = await self._check_and_add_script(keys=keys, args=args)
        
        sender_node = await self._hgetall_to_node(res[0])
        receiver_node = await self._hgetall_to_node(res[1])
        out_degree = int(res[2])
        in_degree = int(res[3])
        
        return sender_node, receiver_node, out_degree, in_degree

    async def get_out_edges(self, account_id: str) -> list[Edge]:
        # Return all edges (eviction handles window size)
        raw_edges = await self.redis.zrange(f"out_edges:{account_id}", 0, -1)
        edges = []
        for r in raw_edges:
            d = json.loads(r)
            edges.append(Edge(
                target_id=d["target_id"],
                amount_paise=d["amount_paise"],
                txn_id=d["txn_id"],
                timestamp=datetime.fromisoformat(d["timestamp"])
            ))
        return edges

    async def get_in_edges(self, account_id: str) -> list[Edge]:
        raw_edges = await self.redis.zrange(f"in_edges:{account_id}", 0, -1)
        edges = []
        for r in raw_edges:
            d = json.loads(r)
            edges.append(Edge(
                target_id=d["target_id"],
                amount_paise=d["amount_paise"],
                txn_id=d["txn_id"],
                timestamp=datetime.fromisoformat(d["timestamp"])
            ))
        return edges

    async def get_node(self, account_id: str) -> Optional[AccountNode]:
        hdata_dict = await self.redis.hgetall(f"node:{account_id}")
        if not hdata_dict:
            return None
        # Convert dict to flat list for _hgetall_to_node
        hdata = []
        for k, v in hdata_dict.items():
            hdata.extend([k, v])
        return await self._hgetall_to_node(hdata)

    async def flag_node(self, account_id: str, pattern: FraudPattern, risk_score: float, block: bool) -> None:
        key = f"node:{account_id}"
        if not await self.redis.exists(key):
            return
            
        # Get existing flags
        flags_str = await self.redis.hget(key, "fraud_flags")
        flags = json.loads(flags_str) if flags_str else []
        if pattern.value not in flags:
            flags.append(pattern.value)
            
        mapping = {
            "fraud_flags": json.dumps(flags),
            "risk_score": str(risk_score)
        }
        if block:
            mapping["is_blocked"] = "1"
            
        await self.redis.hset(key, mapping=mapping)

    async def evict_before(self, cutoff_ts: datetime) -> int:
        score = cutoff_ts.timestamp()
        removed = 0
        
        # 1. Get all active nodes to check their edges
        # Note: In a massive scale cluster, we would not use SMEMBERS on one key,
        # but SSCAN or a background worker partitioned by hash slots.
        # For Phase 1, SMEMBERS is sufficient.
        nodes = await self.redis.smembers("active_nodes")
        
        for node in nodes:
            out_removed = await self.redis.zremrangebyscore(f"out_edges:{node}", "-inf", score)
            # Only count out_edges for the global count to avoid double-counting
            removed += out_removed
            
            await self.redis.zremrangebyscore(f"in_edges:{node}", "-inf", score)
            
            # If node has no more edges and is not blocked, we could remove it from active_nodes.
            # But skipping for now to prioritize eviction latency over minor memory cleanup.
            
        if removed > 0:
            await self.redis.decrby("global:edge_count", removed)
            
        return removed

    async def get_node_count(self) -> int:
        return await self.redis.scard("active_nodes")

    async def get_edge_count(self) -> int:
        val = await self.redis.get("global:edge_count")
        return int(val) if val else 0

    async def snapshot(self) -> dict:
        """Return a snapshot for the UI dashboard."""
        nodes = []
        links = []
        
        active = await self.redis.smembers("active_nodes")
        
        # Fetch up to 200 nodes for the UI to prevent overwhelming D3
        dashboard_nodes = list(active)[:200]
        
        for account_id in dashboard_nodes:
            n = await self.get_node(account_id)
            if not n:
                continue
                
            group = 0
            if n.is_blocked: group = 2
            elif n.flags: group = 1
            
            nodes.append({
                "id": n.account_id,
                "group": group,
                "risk_score": n.risk_score,
            })
            
            # Get edges
            out_edges = await self.get_out_edges(account_id)
            for edge in out_edges:
                links.append({
                    "source": n.account_id,
                    "target": edge.target_id,
                    "value": edge.amount_paise / 100.0,
                })
                
        return {"nodes": nodes, "links": links}
