# decode_sih — UPI Fraud Prevention War Room

> **Real-time behavioral graph engine for detecting structural UPI fraud topologies.**  
> Built for SIH 2024 — PS2: Intelligent UPI Fraud Prevention using AI-driven real-time transaction monitoring.

---

## The Core Idea

Traditional fraud detection classifies **individual transactions**. This system detects **network patterns** — the mathematical shape of money movement. A scammer can change their phone number 10 times a day. They cannot change the topological fingerprint of a Fan-Out scatter operation.

| | Traditional ML | This System |
|---|---|---|
| Unit of analysis | Individual transaction row | Network subgraph |
| Detection timing | Batch (hours later) | Sub-200ms, before settlement |
| Defeated by | Changing phone numbers | **Nothing** — topology is invariant |
| Explainability | Black box | Causal Cypher audit trail |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  TransactionEmitter                                         │
│  (Kaggle replay or synthetic NPCI-calibrated distributions) │
└─────────────────────┬───────────────────────────────────────┘
                      │ UPITransaction (Pydantic, immutable)
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  StreamProcessor  (async pipeline coordinator)              │
│                                                             │
│  ┌──────────────────┐   ┌──────────────────────────────┐   │
│  │ BehavioralGraph  │   │ PatternDetectorRegistry       │   │
│  │ Engine           │◄──│ (Fan-Out, Fan-In, Scatter-    │   │
│  │ (O(1) ingestion) │   │  Gather, Velocity — async     │   │
│  └──────────────────┘   │  concurrent detection)        │   │
│                         └──────────────┬─────────────────┘  │
│                                        │ FraudAlert          │
│                         ┌──────────────▼─────────────────┐  │
│                         │ ExplainabilityEngine            │  │
│                         │ (plain English + Cypher query)  │  │
│                         └──────────────┬─────────────────┘  │
└──────────────────────────────┬──────────────────────────────┘
                               │ WebSocket broadcast
                               ▼
┌─────────────────────────────────────────────────────────────┐
│  War Room Dashboard                                         │
│  • Live force-directed transaction graph                    │
│  • Real-time TPS / Latency gauges                          │
│  • Alert feed with full causal explanation                  │
│  • One-click attack injection for demo                      │
└─────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
decode_sih/
├── config/              # Pydantic settings management
│   ├── __init__.py
│   └── settings.py      # Type-safe env config (no hardcoded values)
├── core/                # The detection engine
│   ├── models.py        # Domain types: UPITransaction, FraudAlert, etc.
│   ├── graph_engine.py  # In-memory O(1) behavioral graph
│   ├── pattern_detector.py  # Fan-Out, Fan-In, Scatter-Gather, Velocity
│   └── explainability.py    # Causal audit trail generator
├── emitter/             # Transaction stream generator
│   ├── distributions.py     # NPCI-calibrated statistical distributions
│   ├── fraud_injector.py    # Coordinated attack scenario generators
│   └── transaction_emitter.py  # Synthetic + CSV replay emitter
├── pipeline/            # Async coordination
│   ├── stream_processor.py  # Main pipeline loop
│   └── metrics.py           # Latency tracker, TPS counter
├── api/                 # FastAPI backend
│   ├── main.py          # App factory + lifespan
│   └── routers/
│       ├── stream.py    # WebSocket endpoint
│       └── control.py   # REST endpoints
├── dashboard/           # War Room UI
│   ├── index.html
│   └── static/
│       ├── css/dashboard.css
│       └── js/
│           ├── graph.js       # Force-directed canvas graph
│           └── dashboard.js   # WS controller + UI
├── tests/
│   ├── test_graph_engine.py
│   └── test_pattern_detector.py
├── scripts/
│   └── run_server.py
├── data/                # Put your Kaggle dataset CSV here
│   └── raw/
├── .env.example
├── pyproject.toml
└── requirements.txt
```

---

## Quick Start

```bash
# 1. Clone and enter project
cd decode_sih

# 2. Create virtual environment
python3 -m venv .venv && source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Copy and configure environment
cp .env.example .env
# Edit .env if needed (defaults work out of the box)

# 5. Run the server
python scripts/run_server.py
# OR: uvicorn api.main:app --reload --port 8000

# 6. Open the War Room
# http://localhost:8000          ← Live dashboard
# http://localhost:8000/docs     ← API documentation
```

---

## Adding a Real Dataset

Drop a CSV file into `data/raw/`. The emitter auto-detects it.

**Supported column names (case-insensitive):**
- Sender: `sender`, `sender_id`, `nameOrig`, `source`, `from`
- Receiver: `receiver`, `receiver_id`, `nameDest`, `destination`, `to`  
- Amount: `amount`, `value`, `amt` (assumed in Rupees — converted to Paise)

The [PaySim dataset from Kaggle](https://www.kaggle.com/datasets/ealaxi/paysim1) works directly.

---

## Detected Fraud Patterns

| Pattern | Description | Detection Method |
|---|---|---|
| **Fan-Out** | 1 sender → N receivers in window T | Out-degree ≥ threshold in sliding window |
| **Fan-In** | N senders → 1 collector | In-degree ≥ threshold in sliding window |
| **Scatter-Gather** | Split → multi-hop → re-converge | BFS neighbourhood + convergence analysis |
| **Velocity Abuse** | Burst of >20 txns in 30s | Edge count in burst window |

---

## Running Tests

```bash
pytest tests/ -v --tb=short
```

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | War Room dashboard |
| `GET` | `/api/status` | System metrics |
| `GET` | `/api/alerts` | Recent fraud alerts |
| `GET` | `/api/graph/snapshot` | Current graph state |
| `POST` | `/api/inject` | Inject fraud scenario |
| `POST` | `/api/emitter/pause` | Pause stream |
| `POST` | `/api/emitter/resume` | Resume stream |
| `WS` | `/ws/stream` | Real-time event stream |

---

## Design Decisions

- **Amounts in Paise** — All monetary values are `int` in paise (₹1 = 100 paise) to avoid floating-point rounding errors in financial calculations.
- **Immutable transactions** — `UPITransaction` uses `model_config = {"frozen": True}`. Once created, it cannot be mutated. This prevents subtle bugs in concurrent code.
- **Stateless detectors** — Each pattern detector is a pure async function. Adding a new pattern requires one function + one registration call. No changes to the pipeline.
- **Fire-and-forget broadcasting** — WebSocket broadcasts are wrapped in `asyncio.create_task()`. The detection pipeline is never blocked waiting for slow clients.
- **UUIDs, not phone numbers** — Account IDs are UUIDs/hashes. The system doesn't track phone numbers — topology detection is identity-agnostic.

---

## Regulatory Context

When a `BLOCK` verdict is issued, the system generates a complete regulatory action summary including:
- Filing of Suspicious Transaction Report (STR) with **FIU-IND** under Section 12, PMLA 2002
- Account freeze recommendation under **Section 51A, UAPA 1967** (terror financing cases)
- Enhanced Due Diligence requirements per **RBI Master Direction on KYC, 2016**
