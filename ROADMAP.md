================================================================================
SAMPATI — PRODUCTION-GRADE ROADMAP
From Hackathon Prototype to DPIP-Class Infrastructure
================================================================================

## GUIDING PRINCIPLE
Your USP is topology, not statistics. Every choice below is made to preserve
and scale that USP — not to bolt on sklearn/XGBoost "for credibility." If a
component doesn't make the graph faster, more consistent, or more explainable,
it doesn't belong in this stack.

Current bottleneck in one sentence: everything lives in one Python process's
RAM, protected by one asyncio.Lock. Production means the graph state, the
stream, and the API all become independently scalable, while the eviction and
consistency model stays mathematically well-defined.


================================================================================
PHASE 0 — LOCK DOWN THE CORE ABSTRACTIONS (Week 1)
================================================================================
Before touching infra, freeze the domain model so every later swap (dict ->
Redis -> Neo4j) is a storage-layer change, not a rewrite.

- Define AccountNode, Edge, and PatternMatch as typed, storage-agnostic
  classes (use `pydantic` v2 for validation + serialization, it's fast
  because it's Rust-backed under the hood — `pip install pydantic`).
- Define a GraphStore interface (Python Protocol / ABC) with:
  add_edge(), get_out_edges(), get_in_edges(), evict_before(ts), snapshot().
- Your current in-memory dict becomes InMemoryGraphStore(GraphStore) —
  keep it. It's your dev/test backend and your local-latency benchmark.

This single step is what lets you say to a jury: "we can swap the backend
without touching the detection logic" — and mean it.


================================================================================
PHASE 1 — GRAPH STORAGE THAT SURVIVES A RESTART & SCALES HORIZONTALLY
================================================================================

Problem with plain Python dicts: single process, no persistence, no sharing
across workers, GIL-bound.

RECOMMENDED: Redis (not Neo4j) as the primary hot-path store.
Why Redis over a full graph DB for this use case:
- Your query pattern is shallow (1–3 hop lookups on a 60s window), not deep
  graph traversal. Redis's native structures cover this cheaply:
  - `Hash` per account for node attributes
  - `Sorted Set` (ZSET) per account for out_edges / in_edges, scored by
    transaction timestamp -> O(log N) insert, O(log N + K) range eviction
  - `ZREMRANGEBYSCORE` gives you the 60-second eviction as a single atomic
    command — no background loop needed, no race with the sweep.
- Redis Cluster gives you horizontal sharding by account_id (consistent
  hashing), so you're no longer bound by one process's GIL.
- Sub-millisecond ops, in the same latency class as your current dict.

Libraries:
- `redis-py` (async client: `redis.asyncio`) — pip install redis
- Or `hiredis` as the C parser backend for redis-py (2-3x faster parsing) —
  pip install hiredis
- For Lua-level atomicity of "add edge + check threshold" in one round trip,
  write small Lua scripts and load them with `register_script` — this keeps
  your Fan-Out/Fan-In threshold check atomic under concurrent writes, which
  a Python asyncio.Lock cannot guarantee once you're multi-process.

WHEN TO ADD NEO4J (not instead of Redis, alongside it):
- Redis is your real-time hot path (last 60s, sub-ms).
- Neo4j (or Memgraph, which is Neo4j-compatible but built for streaming and
  is noticeably faster for this exact use case) becomes your COLD / FORENSIC
  layer: multi-hop mule network investigation, "show me every account within
  4 hops of this seed over the last 30 days," case-building for FIU-IND
  reporting. This is not on the block/allow critical path — it's async,
  written to after every verdict.
- Memgraph specifically: it's built in C++, supports streaming ingestion via
  Kafka connectors natively, and has a "dynamic graph algorithms" library
  (MAGE) with built-in community detection / connected-components, which is
  exactly what a mule-cluster investigation needs. This is a much stronger
  jury-facing case than vanilla Neo4j because it's designed for exactly your
  "live streaming graph" pitch.

Bottom line storage architecture:
  Redis (hot, <200ms SLA, real-time verdicts)
      -> async write-behind ->
  Memgraph/Neo4j (cold, forensic graph, investigation & reporting)


================================================================================
PHASE 2 — REPLACE THE SYNTHETIC EMITTER'S DELIVERY MECHANISM WITH A REAL
STREAM (Week 2-3)
================================================================================

Problem: right now transactions are generated and consumed in the same
process. Production needs decoupled ingestion so the bank gateway, the
detection engine, and the dashboard don't share fate.

RECOMMENDED: Redpanda (Kafka-API-compatible, no JVM, no Zookeeper).
Why Redpanda over Kafka here:
- Kafka-wire-compatible so every Python client (`aiokafka`, `confluent-kafka`)
  works unchanged.
- Single binary, no JVM — matters a lot when your "server" story is
  "runs on a ₹50,000 box," which is a real line in your pitch.
- Sub-millisecond p99 produce latency, which keeps your 200ms SLA budget
  almost entirely free for the actual detection logic.

Alternative if you want to stay closest to what enterprises actually run:
Apache Kafka + `confluent-kafka-python` (the C-librdkafka-backed client, NOT
`kafka-python`, which is pure-Python and slow). This matters for the jury —
naming `confluent-kafka` over `kafka-python` signals you know the ecosystem.

Topic design:
- `txn.incoming` — raw transactions from bank gateway / emitter
- `txn.verdicts` — BLOCK/ALLOW/FLAG decisions, keyed by txn_id
- `graph.events` — edge-added events, consumed by the dashboard AND by the
  Memgraph write-behind consumer, so your War Room dashboard is just another
  consumer group, not something bolted onto the detection engine.

This also directly answers a jury question you already anticipated: "how do
you scale to 10k+ TPS?" — partition `txn.incoming` by account_id, run N
detection workers as a consumer group, each owning a shard of the account
space (which maps cleanly onto Redis Cluster's hash slots if you shard both
the same way).


================================================================================
PHASE 3 — DETECTION ENGINE: KEEP IT ALGORITHMIC, MAKE IT CONCURRENT-SAFE
================================================================================

Do NOT introduce sklearn/XGBoost for the core verdict. Your differentiator is
that a verdict is a provable graph fact, not a probability. Keep it that way.
Where statistics DO belong (see Phase 5) is baseline learning for whitelisting
— and even there, prefer explainable statistical methods over black-box ML.

- Move threshold checks into Redis Lua scripts (Phase 1) so "add edge, check
  if out-degree in window >= threshold" is a single atomic server-side op —
  this removes your current asyncio.Lock entirely and is what makes
  horizontal workers safe.
- For Scatter-Gather (multi-hop), use bounded BFS from the triggering node,
  capped at GRAPH_SCATTER_GATHER_HOPS — still O(branching^hops), still
  microseconds at your thresholds. Don't reach for a graph algorithms library
  for this; it's simple enough that a library adds overhead, not value.
- For the cold-path/forensic mule-cluster detection (not on the hot path),
  use Memgraph's MAGE library: `weakly_connected_components`,
  `betweenness_centrality` — these identify "who is the actual hub of this
  laundering network" for investigators, which is a strong demo moment:
  live block in <2ms, followed by "here's the full network we can now see."
- Use `structlog` (pip install structlog) for every detection event so each
  BLOCK verdict emits a structured, queryable log line — this is what makes
  "causal explainability" a technical fact, not a slide claim. Ship these
  logs to the same place as your metrics (Phase 6) so an auditor can replay
  exactly why any transaction was blocked, in order, with timestamps.


================================================================================
PHASE 4 — API LAYER: FROM FastAPI PROTOTYPE TO PRODUCTION SERVICE
================================================================================

Keep FastAPI — it's the right choice, not a downgrade. Harden it:

- `FastAPI` + `uvicorn[standard]` behind `gunicorn` with `uvicorn.workers.UvicornWorker`
  for multi-process serving (N workers = N CPU cores, each holding a
  connection pool to Redis, not its own copy of graph state).
- Define the bank-gateway contract explicitly with Pydantic models:
    Request:  {txn_id, sender_account, receiver_account, amount_paise,
               timestamp, merchant_category?}
    Response: {verdict: BLOCK|ALLOW|FLAG, latency_ms, explanation,
               pattern_type?, evidence: {nodes, edges, hop_distance}}
- Add a `/health` and `/ready` endpoint separately (readiness should check
  Redis connectivity, not just "process is up") — required for any real
  Kubernetes deployment and it's a 10-minute add that signals production
  maturity to a jury.
- Rate-limit and auth the ingestion endpoint with `slowapi` +
  mTLS/API-key auth if this is genuinely meant to sit in front of a bank
  gateway — a jury WILL ask "what stops someone from spamming your API,"
  have an answer that isn't "nothing yet."


================================================================================
PHASE 5 — BASELINE LEARNING FOR FALSE-POSITIVE REDUCTION (NOT SKLEARN SLOP)
================================================================================

This is the piece you flagged as an open gap (salary payouts flagged as
Fan-Out). Solve it with explainable statistics, not a trained black box —
this keeps your "no black box" pitch intact end to end.

- Per-account rolling baseline using an exponentially weighted moving
  average + std-dev of out-degree-per-window, computed incrementally
  (Welford's algorithm — O(1) update per transaction, no batch retraining,
  no sklearn). A transaction only escalates severity if it's a statistical
  outlier AGAINST THAT ACCOUNT'S OWN HISTORY, not just against a global
  threshold.
- This gives you a genuinely strong sentence for the jury: "we don't
  whitelist accounts, we let each account define its own normal, and we
  detect deviation from itself — a compromised salary account would still
  get caught if its behavior changes, even though its baseline is high-degree."
- If you want a slightly more sophisticated version without going near
  "AI": a simple robust z-score (median + MAD instead of mean + stddev) is
  more resistant to the fraud transactions themselves poisoning the baseline.
  Cheap, explainable, defensible in one sentence to a non-technical juror.


================================================================================
PHASE 6 — OBSERVABILITY (THIS IS WHAT SEPARATES "PROTOTYPE" FROM "PRODUCT")
================================================================================

- `Prometheus` (metrics) + `Grafana` (dashboards) — expose txn throughput,
  p50/p95/p99 detection latency, Redis op latency, eviction lag, per-pattern
  block counts. Use `prometheus-fastapi-instrumentator` for zero-boilerplate
  FastAPI metrics.
- `OpenTelemetry` tracing across API -> Redis -> Kafka/Redpanda so a single
  transaction's full path is traceable end to end — this is exactly the kind
  of auditability regulators (RBI/CERT-In) actually ask for, and DPIP being
  a black box is precisely what you're positioned against.
- Your existing War Room dashboard becomes a Grafana panel set + a custom
  React/WebSocket view for the live graph visualization specifically (Grafana
  won't render a force-directed graph well — keep your current dashboard for
  that one view, feed it off the `graph.events` Kafka topic instead of
  in-process state).


================================================================================
PHASE 7 — DEPLOYMENT & INFRA
================================================================================

- Containerize each service independently: api/, detection-worker/,
  emitter/ (becomes a real bank-gateway simulator, not part of the core
  system), dashboard/. `Docker` + a single `docker-compose.yml` for local
  dev, `Kubernetes` manifests (or `Helm` chart) for the "this is how it
  deploys at a bank" story.
- Redis Cluster + Redpanda both have official Helm charts — don't hand-roll
  StatefulSets, use the maintained charts (Bitnami Redis Cluster chart,
  Redpanda's own operator).
- Put actual numbers behind your "runs on a ₹50,000 server" claim: benchmark
  the InMemoryGraphStore vs RedisGraphStore under `locust` or `k6` load
  testing at realistic TPS and put the p99 latency graph directly in your
  jury deck. A benchmark graph beats a claim every time.


================================================================================
PHASE 8 — REGULATORY / COMPLIANCE ARTIFACTS (JURY-FACING, NOT CODE)
================================================================================

- A one-page mapping doc: Fan-Out/Fan-In/Scatter-Gather -> PMLA's
  Placement/Layering/Integration stages, with your structured log format
  shown as a direct FIU-IND STR (Suspicious Transaction Report) field
  mapping. This single artifact does more for jury credibility than another
  feature.
- A data-retention policy statement: hot data (Redis) purges at 60s, cold
  forensic graph (Memgraph) retains per whatever PMLA record-keeping
  requirement you cite — show you've thought about retention, not just
  detection.


================================================================================
SUGGESTED BUILD ORDER (REALISTIC SEQUENCING)
================================================================================
1. Phase 0 (interfaces)                    — 2-3 days
2. Phase 1 (Redis backend + Lua scripts)   — 3-4 days
3. Phase 2 (Redpanda + consumer groups)    — 3-4 days
4. Phase 3 (concurrency-safe detection)    — 2-3 days
5. Phase 4 (API hardening)                 — 2 days
6. Phase 5 (baseline/whitelist stats)      — 2-3 days
7. Phase 6 (observability)                 — 2-3 days
8. Phase 7 (containers/deploy)             — 2-3 days
9. Phase 8 (compliance artifacts)          — 1-2 days

Phases 0-4 are what make the live demo bulletproof and horizontally scalable.
Phases 5-8 are what make the jury believe this could actually replace DPIP's
first two years of work. Do 0-4 first no matter what your remaining time is.


================================================================================
LIBRARY / TOOL SUMMARY (NO SKLEARN, NO BLACK BOXES)
================================================================================
Domain models:        pydantic
Hot graph store:       redis (redis-py, async) + hiredis + Lua scripts
Cold forensic graph:    Memgraph (MAGE algorithms) or Neo4j
Streaming:             Redpanda (Kafka-API compatible) + confluent-kafka / aiokafka
API:                   FastAPI + uvicorn[standard] + gunicorn + slowapi
Baseline stats:        Welford's algorithm / robust z-score (hand-rolled, ~50 lines)
Structured logging:    structlog
Metrics:               Prometheus + Grafana + prometheus-fastapi-instrumentator
Tracing:               OpenTelemetry
Load testing:          locust or k6
Deployment:             Docker + Kubernetes/Helm
================================================================================
