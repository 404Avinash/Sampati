# Sampati: What's Actually Left (The Real 90%)

> Honest version — not the optimistic SIH pitch version.

---

## What Actually Works (~10%)

| Component | Status | Reality Check |
|---|---|---|
| Graph Engine (in-memory) | ✅ Correct | Good data structure, O(1) inserts |
| Pattern Detectors (4 patterns) | ✅ Correct | Logic is sound |
| FastAPI + WebSocket | ✅ Works | Correct async architecture |
| Dashboard UI (structure) | ✅ Works | Renders, connects, shows data |
| Deck.gl geo map | ✅ Works | City dots + arcs fixed |
| SAR Report streaming | ✅ Works | Data-driven, unique per alert |
| Synthetic Emitter | ⚠️ Half-works | Never produces *organic* fraud |

---

## The Root Problem: Two Separate Account Pools

The emitter has **5,000 legitimate accounts** (`LEG_xxxxx`) and **200 mule accounts** (`MUL_xxxxx`) in completely separate namespaces. They never transact with each other normally.

Normal flow: `LEG_xxxxx → LEG_xxxxx`
Fraud flow: `MUL_xxxxx → LEG_xxxxx` (only during injected attacks)

This means:
- Fraud is **never organic** — it is always a manually scheduled injection every ~50 transactions
- The detector always reacts to an obviously synthetic burst, not a gradual pattern change
- In real UPI fraud, mule accounts look completely normal for weeks before activating — our system can never demonstrate this
- `fraud_rate=0.02` + `txn_tick` every 5th transaction = one visible fraud event per ~2.5 minutes in the UI

---

## Track 1: Real-Time Transaction Flow (Immediate)

**Why it feels static right now:**
1. `txn_tick` fires every 5th transaction only → UI updates at 1/5 actual TPS
2. `HOURLY_LOAD_MULTIPLIER` throttles to 3-8% of TPS at night — at 2am IST = 3 TPS effective
3. Mule and legitimate account pools never mix → no organic fraud buildup

**Fixes needed:**
- [ ] Merge mule accounts into the legitimate pool with a behavioral "dormancy" flag
- [ ] Dormant mule accounts transact normally (small P2M payments, recharges) for first N minutes
- [ ] At activation: behavioral shift → rapid fan-out or fan-in in the same account namespace
- [ ] Remove `HOURLY_LOAD_MULTIPLIER` from synthetic/demo mode OR add `APP_ENV=demo` bypass
- [ ] Broadcast EVERY transaction to the geo map (separate from the heavier graph snapshot every 5th)
- [ ] Add live TPS sparkline in the UI header bar

---

## Track 2: ML Layer (Currently 0%)

### 2.1 Behavioral Baseline Modeling
Per-account rolling features need to exist:
```
{avg_amount_7d, tx_per_hour_7d, unique_counterparties_7d, amount_variance, tx_hour_entropy}
```
Anomaly score = Z-score deviation from the account's OWN history, not a global threshold.
Currently: The system has zero account history beyond the last 60-second window.

### 2.2 Link Prediction (Pre-Crime Detection)
- Predict mule activation BEFORE the structural pattern completes
- Tech: Temporal Graph Neural Network (TGNN) or simpler XGBoost on graph features
- Training data needed: Labeled sequences showing account behavior before/during fraud
- None of this exists yet

### 2.3 Risk Score Calibration
Current formula: `base_weight + 0.15*magnitude + 0.10*amount * tier`
A production risk model uses: account age, device fingerprint velocity, geo-velocity (can the person physically be in two cities within the txn timestamps?), counterparty risk inheritance, and behavioral drift from personal baseline.
None of these signals are captured.

### 2.4 Feature Store
Consistent feature computation between training and serving prevents train-serve skew (the #1 reason fraud models degrade silently in production).
Tech: Redis + Feast, or Apache Pinot.
Currently: Features computed ad-hoc inside each detector function, non-reproducible.

---

## Track 3: Data Reality

### 3.1 No Data in Deployment
`data/raw/` is `.gitignored` → Render starts with an empty directory.
System always runs in pure synthetic mode on the server.
Need: Either a small bundled seed CSV (< 5MB) OR a startup bootstrap script.

### 3.2 No Account Behavioral History
Every legitimate account is stateless. The graph engine sees each account for the first time in a fraud sequence and has no baseline to compare against. This makes anomaly detection impossible.

### 3.3 No Evaluation Framework
No pipeline exists to measure detector precision, recall, or F1.
`injected_pattern` is the only label source, but it's never used for evaluation.

### 3.4 Cold-Start Gap
Graph starts empty on every deploy. First 60 seconds = zero alerts.
For a live demo, judges see nothing happening for the first minute.
Need: A bootstrap script that pre-warms the graph with 500+ synthetic historical transactions before the demo starts.

---

## Track 4: Dashboard — The Visual 90%

### 4.1 Live Transaction Ticker
Need: Bloomberg Terminal-style ticker — transaction pills slide in from the right and fade out. Not a scrolling log that stalls at 40 rows.

### 4.2 Network Graph (Most Impactful Visual)
Current: Basic Canvas API force simulation. All nodes look identical.

Need:
- D3.js force simulation with proper physics (spring forces, gravity, charge)
- Node size = transaction volume (bigger = more active mule)
- Node color = animated gradient: green → yellow → orange → red as risk builds
- Animated pulse ring emanating from flagged nodes when detection fires
- Edge thickness = transaction count
- Click a node → full transaction history + risk score timeline
- Mule node "detonation" animation when the pattern completes

This single change is the highest-impact visual improvement possible.

### 4.3 Risk Timeline Chart
Per-account risk score over the last 5 minutes (sparkline per node).
Shows the gradual buildup before detection — this is the story the judges need to see.
Currently: No time-series risk data is stored anywhere.

### 4.4 Attack Anatomy Diagram
When a fraud alert fires: animated Sankey diagram showing the full transaction path.
`Origin Account → [7 intermediaries labeled by city] → Collector Account`
Currently: Only a text explanation is shown.

### 4.5 Pre-Crime Warning Layer
Orange nodes = accounts with elevated risk (0.40–0.84) below the block threshold.
Shows accounts building toward a pattern before the alert fires.
Currently: Binary — either an alert fires or nothing is visible.

---

## Track 5: Backend Hardening

### 5.1 No Persistence
All state is in RAM. Restart = everything lost.
Fix: Redis for hot graph state, PostgreSQL for alert history.

### 5.2 No Authentication
`/docs` and all write endpoints are public. Anyone can call `POST /api/inject`.
Fix: API key header for write endpoints minimum.

### 5.3 Exception Handling Gap
```python
# Current — one detector crash kills the pipeline
await asyncio.gather(*[fn(txn, graph, ts) for _, fn in self._detectors], return_exceptions=False)

# Fix — isolate failures
results = await asyncio.gather(*[fn(txn, graph, ts) for _, fn in self._detectors], return_exceptions=True)
alerts = [r for r in results if isinstance(r, FraudAlert)]
```

### 5.4 No Rate Limiting
A malicious client can open 1000 WebSocket connections and exhaust file descriptors.
Fix: `ws_max_connections` setting exists but is not enforced.

### 5.5 Single Instance
One Python process, one asyncio event loop. No horizontal scale path.
For production: Shard by `sender_id[:2]`, deploy N instances behind a load balancer.

---

## Track 6: The Architecture It Should Be

### Current
```
Synthetic Emitter (Python asyncio)
    → In-memory dict graph
    → Pattern Detectors
    → WebSocket → Browser
```

### Production Target
```
Bank Core Banking (ISO 20022)
    → Apache Kafka (partitioned by sender_id prefix)
    → Apache Flink (200ms SLA, stateful operators)
        ├── Rolling feature computation
        ├── Graph pattern detector (our algorithm, distributed)
        ├── ML inference (<5ms, pre-warmed model)
        └── Alert sink
            → FIU-IND goAML API (STR filing)
            → Bank Fraud Operations (case management)
            → Redis (hot state) + PostgreSQL (cold storage)
            → Grafana (ops observability)
```

We simulate the middle two boxes only. The rest doesn't exist.

---

## Track 7: Regulatory Compliance (The Actual PS-2 Requirement)

| Requirement | Status |
|---|---|
| Real-time interdiction (block before settlement) | Simulated only |
| NPCI fraud analytics API integration | Not built |
| FIU-IND goAML STR filing | Simulated in AI report |
| PMLA 2002 Section 12 compliance | Referenced in text only |
| RBI Master Direction KYC 2016 | Not implemented |
| Account freeze (UAPA Section 51A) | Simulated only |
| Audit trail export (SIEM-compatible) | Log format exists, no export |

### What Would Actually Win the Demo
1. A live mock call to a simulated NPCI intercept endpoint that returns `BLOCKED`
2. A generated PDF that matches the actual FIU-IND XML STR schema
3. An animated timeline: txn received (0ms) → detected (87ms) → blocked (150ms) → STR filed

---

## Track 8: Demo Risks

### Critical: Render Cold Start
Free tier instances sleep after 15 minutes of inactivity.
First wake-up request = 30-50 seconds. Fatal in front of judges.

Options:
- Use UptimeRobot (free) to ping `/health` every 5 minutes
- Upgrade to Render starter plan ($7/month)
- Host on a VPS with persistent uptime (DigitalOcean, Railway)

### What Judges Will Actually Check
| Action | Current Status |
|---|---|
| Open dashboard → see live transactions | Works if not cold-started |
| Graph View → live network forming | Renders but visually basic |
| Geo Map → arcs flying across India | Works now |
| Wait for fraud alert (<30s) | Needs `fraud_rate` tuned up |
| Click alert → read explanation | Works |
| Generate AI Report → read it | Works, data-driven |
| "Show fraud being blocked in real time" | No interdiction UI |
| "Can this scale to real NPCI volume?" | Honest answer: No, not yet |

---

## Priority Stack: What to Do Next

### Highest Impact (do these before anything else)
1. Merge account pools — organic fraud without manual injection
2. Remove load multiplier from demo mode — constant TPS, not throttled at night
3. Tune Render environment: `EMITTER_FRAUD_RATE=0.08`, `GRAPH_FANOUT_THRESHOLD=3`, `GRAPH_FANIN_THRESHOLD=3`
4. Add UptimeRobot keep-alive for the Render instance

### High Impact (this sprint)
5. D3.js network graph with pulse animations
6. Pre-crime orange node layer
7. Exception isolation in pattern detector gather
8. Graph warm-up script (pre-load 500 synthetic transactions on startup)

### Before Demo Day
9. Full end-to-end test 24 hours before
10. Two simultaneous browser tabs stress test
11. Verify geo map works on mobile (judges may use phones)
12. Print a one-page architecture diagram to hand to judges

---

## The Honest Truth

The system demonstrates a sound algorithmic approach — behavioral graph analysis with structural pattern matching is a real technique used in production fraud systems (e.g., Razorpay's risk engine, PayTM's fraud graph).

What it is not: a production system, or even a complete research prototype.

For SIH, judges evaluate:
1. **Does it work in real time?** → Mostly yes, with the liveness fixes
2. **Is the approach technically sound?** → Yes, the graph theory is defensible
3. **Can you explain it clearly?** → The explainability engine helps
4. **Does it look impressive?** → Geo map is great, graph needs work

The goal for the remaining time is not new features. It is making the existing system feel *alive and inevitable* — transactions flowing continuously, fraud emerging naturally, detection firing automatically, explanations appearing without manual triggers.
