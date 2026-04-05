# Intelli AI

**Intelligent MQ Topology Simplification & Modernization**

IBM MQ Hackathon 2026 — Team IntelliAI

---

## What It Does

Intelli AI is a 10-agent LangGraph pipeline that transforms legacy IBM MQ topologies into simplified, standards-compliant, automation-ready architectures. It ingests a raw MQ dataset, builds a NetworkX graph, runs coordinated AI agents to analyse, redesign, validate, and provision the target state, and pauses for human review before generating outputs.

The system enforces the hackathon's core constraint — one queue manager per application — through a hybrid intelligence architecture: deterministic rules guarantee constraint compliance, while LLM agents (powered by Wells Fargo's internal Tachyon client with Gemini 2.0 Flash) handle the decisions that require reasoning: cluster analysis, PCI/payment zone isolation, bridge app placement, and human feedback interpretation.

**Pipeline output includes:** target state topology (CSV), complexity metrics with as-is vs target scoring, AI-generated Architecture Decision Records (ADRs), per-QM MQSC provisioning scripts, a 4-phase migration plan with rollback, regression testing plan, subgraph analysis, and an executive summary report.

---

## How to Run

You need two terminals running simultaneously.

**Terminal 1 — Backend (FastAPI + Uvicorn):**

```bash
cd src
python -m uvicorn backend.api.main:app --host 0.0.0.0 --port 8000
```

**Terminal 2 — Frontend (React + Vite):**

```bash
cd frontend
npm install
npm run dev
```

The frontend runs on `http://localhost:3000` and the backend API on `http://localhost:8000`.

**Environment variables** (create a `.env` file in `src/`):

| Variable | Description |
|----------|-------------|
| `APIGEE_URL` | Tachyon APIGEE gateway URL |
| `CONSUMER_KEY` | Tachyon OAuth consumer key |
| `CONSUMER_SECRET` | Tachyon OAuth consumer secret |
| `API_KEY` | Tachyon API key |
| `USE_CASE_ID` | Tachyon registered use case ID |
| `MODEL` | Model identifier (e.g. `openai/gemini-2.0-flash`) |

The pipeline works without LLM credentials — it falls back to the deterministic rule engine, which enforces all constraints and produces a valid target state. The LLM adds intelligent cluster analysis, richer ADRs, and natural-language feedback interpretation on top of the rule baseline.

---

## Project Structure

```
intelli-ai/
├── src/
│   └── backend/
│       ├── agents/
│       │   └── agents.py              # All 10 agents (~5,000 lines)
│       ├── llm/
│       │   ├── llm_client.py          # Tachyon LLM client with retry & circuit breaker
│       │   └── prompts.py             # Architect system + user prompt templates
│       ├── graph/
│       │   └── mq_graph.py            # NetworkX graph builder + complexity metrics +
│       │                              #   Louvain communities, centrality, entropy,
│       │                              #   subgraph analysis, topology comparison
│       ├── tools/
│       │   └── csv_ingest.py          # Raw CSV → 4 logical tables (vectorized)
│       ├── orchestration/
│       │   ├── state.py               # IntelliAIState TypedDict
│       │   └── workflow.py            # LangGraph StateGraph (full + revise pipelines)
│       └── api/
│           └── main.py                # FastAPI server + chat endpoint
├── frontend/
│   └── src/
│       └── App.jsx                    # React UI with D3.js topology visualizations
├── data/                              # Input CSV datasets
├── .env                               # Tachyon credentials (not committed)
└── requirements.txt                   # Python dependencies
```

---

## Pipeline Flow

The pipeline has two phases. Phase 1 runs on initial upload and pauses at the human review gate. Phase 2 resumes based on the human's decision.

### Phase 1 — Initial Analysis & Design

```
SUPERVISOR → SANITISER → RESEARCHER → ANALYST → ARCHITECT → OPTIMIZER → TESTER
                                                                          │
                                           ┌─── fail (up to 3 retries) ──┘
                                           │
                                           ▼
                                       ARCHITECT (retry)
                                           │
                                           ▼ pass / retries exhausted
                                    HUMAN REVIEW GATE
                                           ⏸ (pipeline pauses)
```

### Phase 2 — Human Decision

```
                                    HUMAN REVIEW GATE
                                     │       │       │
                               APPROVE   REVISE    ABORT
                                  │        │         │
                                  ▼        ▼         ▼
                             Provisioner  Architect  Doc Expert
                                  │       (re-run)   (abort report)
                                  ▼        │
                           Migration       ▼
                            Planner     Optimizer
                                  │        │
                                  ▼        ▼
                             Doc Expert  Tester
                                  │        │
                                  ▼        ▼
                                END    Human Review
                                       (re-pauses)
```

The **Revise** path skips Supervisor, Sanitiser, Researcher, and Analyst since the input data hasn't changed. The Architect receives the human's feedback (natural language), interprets it via the LLM with HIGH/MEDIUM/LOW confidence scoring, and applies targeted changes to the existing target graph rather than redesigning from scratch.

---

## The 10 Agents

| # | Agent | What It Does |
|---|-------|-------------|
| 1 | **Supervisor** | Session initialization, input file validation, routing to Sanitiser |
| 2 | **Sanitiser** | CSV cleanup, deduplication, normalisation, referential integrity checks |
| 3 | **Researcher** | Builds the as-is NetworkX graph; detects violations (multi-QM apps, orphan QMs, cycles); runs Louvain community detection, betweenness centrality (SPOF detection), Shannon entropy analysis; calls LLM anomaly detective |
| 4 | **Analyst** | Computes the 6-factor complexity score (CC, CI, RD, FO, OO, CS) for the as-is topology |
| 5 | **Architect** | Designs the target state topology. Phase A: rule-based 1:1 app-to-QM assignment. Phase B: LLM cluster analysis for PCI isolation, payment-critical zones, bridge app placement. Phase C: LLM reassignments with self-correction (rejects INCORRECT assignments). Generates ADRs. In revision mode, interprets human feedback and applies targeted deltas |
| 6 | **Optimizer** | Two-phase graph optimization. Phase 1: reachability pruning (removes dead channels). Phase 2: weighted MST (removes redundant channels). Also runs Kernighan-Lin bisection, Louvain community detection, SPOF analysis, and topology entropy on the target. Calls LLM design critic and capacity planner |
| 7 | **Tester** | Validates all constraints: 1-QM-per-app, sender/receiver pairs, deterministic channel naming, XMITQ existence, consumer queues, orphan QMs, path completeness. Calls LLM compliance auditor. 0 critical violations = PASS |
| 8 | **Provisioner** | Generates per-QM MQSC scripts (LISTENER, QLOCAL, QREMOTE, XMITQ, SDR/RCVR channels in correct dependency order) and target state CSVs (queue_managers, channels, queues, applications, unified target) |
| 9 | **Migration Planner** | Computes topology diff (QMs added/removed, channels added/removed, apps reassigned). Generates 2,600+ ordered migration steps across 4 phases: CREATE → REROUTE → DRAIN → CLEANUP. Each step has forward MQSC, rollback MQSC, dependencies, and verification commands. Calls LLM risk assessor |
| 10 | **Doc Expert** | Generates the final report, enriches ADRs via LLM, produces executive summary, complexity algorithm documentation, complexity scores CSV, regression testing plan, insights report, migration plan markdown, and subgraph analysis |

---

## Hybrid Intelligence Architecture

The system uses a rules-first, LLM-second approach:

**Rule Engine (deterministic, zero-tolerance)** handles constraint enforcement: 1:1 app-to-QM assignment, channel naming conventions, routing patterns, XMITQ provisioning. This runs in milliseconds on 438 apps with guaranteed 100% compliance.

**LLM Agents (Tachyon / Gemini 2.0 Flash)** handle decisions that require reasoning: which apps belong in PCI-dedicated zones, how to isolate payment-critical workloads, where to place bridge apps that connect clusters, how to interpret human feedback like "consolidate low-traffic QMs." The LLM's output is always validated back through the rule engine and tester — if it suggests something that violates a constraint, the rules catch and reject it.

This means the pipeline produces a valid, constraint-compliant target state even with no LLM available (rule-only fallback), and a richer, more intelligent design when the LLM is available.

---

## Complexity Metric

The 6-factor weighted complexity score measures topology health on a 0–100 scale (higher = more complex):

```
Score = 0.25×CC + 0.25×CI + 0.20×RD + 0.15×FO + 0.05×OO + 0.10×CS
```

| Factor | Weight | What It Measures |
|--------|--------|-----------------|
| CC — Channel Count | 25% | Number of sender channels (each is a failure point) |
| CI — Coupling Index | 25% | Mean QMs per app; ideal = 1.0 after 1:1 enforcement |
| RD — Routing Depth | 20% | Max hops between QMs; fewer hops = less latency |
| FO — Fan-Out Score | 15% | Max outbound channels from one QM; high = bottleneck |
| OO — Orphan Objects | 5% | QMs with no apps + stopped channels; waste |
| CS — Channel Sprawl | 10% | Channels-per-QM ratio; efficiency of channel usage |

Baselines scale with topology size so scores are meaningful regardless of whether the estate has 5 QMs or 500.

---

## Graph Analytics

Beyond the complexity score, the Researcher and Optimizer agents compute:

**Louvain Community Detection** — identifies natural clusters of queue managers based on channel connectivity, with modularity scoring. Used by the Architect to understand existing groupings before redesign.

**Betweenness Centrality (SPOF Detection)** — identifies queue managers through which a disproportionate share of message routes must pass. QMs with betweenness > 2× the mean are flagged as single points of failure.

**Shannon Entropy** — measures how uniform or skewed the QM degree distribution is. High entropy = evenly distributed channels (healthy). Low entropy = hub-and-spoke fragility.

**Subgraph Analysis** — decomposes the topology into connected components, identifying isolated QMs, the largest connected cluster, and fragmentation patterns.

**Topology Comparison** — quantitative before/after diff: QM counts, channel counts, density, average degree, component counts, with percentage reductions.

---

## Frontend

The React UI has 10 tabs providing full visibility into the pipeline:

| Tab | Content |
|-----|---------|
| **Upload** | Drag-and-drop file upload or demo mode |
| **Review** | Human review panel with approve/revise/abort controls and "Ask the Architect" chat |
| **Topology** | Side-by-side as-is vs target D3.js force-directed graphs with app tracing, diff overlay, and full MQ object view (queues, channels, XMITQs) |
| **Metrics** | As-is vs target complexity scores with per-factor breakdown |
| **ADRs** | Architecture Decision Records generated by the Architect and enriched by the Doc Expert |
| **Migration** | 4-phase migration plan with step details, dependencies, and risk assessment |
| **MQSC** | Per-QM MQSC provisioning scripts, searchable and downloadable |
| **CSVs** | All target state CSVs and deliverable documents, downloadable |
| **Report** | Final transformation report with executive summary |
| **Trace** | Ordered agent execution trace showing every step of the pipeline |

The topology tab supports per-app tracing: select any application from the dropdown to see its as-is connections (shared QMs, multiple channels) vs its target state (dedicated QM, standardized routing).

---

## Human-in-the-Loop

The pipeline pauses at the Human Review Gate after the Tester validates the target state. The reviewer sees the complexity reduction, constraint status, ADRs, and topology visualizations.

**Approve** — accepts the design. Pipeline continues to Provisioner → Migration Planner → Doc Expert, generating all output deliverables.

**Revise with Feedback** — the reviewer provides natural-language feedback (e.g. "consolidate low-traffic QMs" or "isolate all PCI apps"). The Architect re-enters revision mode, interprets the feedback via LLM, applies targeted changes, and re-runs through Optimizer and Tester. The pipeline re-pauses at the review gate for another round.

**Ask the Architect** — a chat interface in the Review tab where the reviewer can ask questions about the design before deciding. The Architect responds with specific QM names, channel counts, and rationale from the ADRs.

**Abort** — cancels the pipeline. Doc Expert generates a cancellation report.

---

## MQSC Output

Each queue manager gets its own provisioning script, runnable via `runmqsc QM_NAME < QM_NAME_target.mqsc`. Objects are generated in correct dependency order:

1. LISTENER (TCP port)
2. QLOCAL (application queues)
3. QLOCAL USAGE(XMITQ) (transmission queues)
4. QREMOTE (remote queue definitions with RQMNAME, RNAME, XMITQ)
5. CHANNEL CHLTYPE(SDR) (sender channels with CONNAME)
6. CHANNEL CHLTYPE(RCVR) (receiver channels)
7. START CHANNEL

---

## Migration Plan

The Migration Planner generates ordered steps across 4 phases:

| Phase | What Happens | Risk Level |
|-------|-------------|------------|
| **CREATE** | New listeners, queues, XMITQs, channels | LOW |
| **REROUTE** | Move apps to new QMs, update connections | HIGH |
| **DRAIN** | Wait for old queues to empty (CURDEPTH = 0) | MEDIUM |
| **CLEANUP** | Stop old channels, delete old objects, decommission QMs | MEDIUM |

Each step includes forward MQSC, rollback MQSC, dependency tracking, and a verification command. The LLM risk assessor identifies high-risk steps and recommends maintenance windows.

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check |
| `POST` | `/api/upload` | Upload a single MQ raw data file (CSV or Excel) and run the pipeline |
| `POST` | `/api/demo` | Run pipeline on bundled demo data |
| `GET` | `/api/review/{session_id}` | Get pending review data |
| `POST` | `/api/review/{session_id}` | Submit review decision (approve / revise / abort) |
| `POST` | `/api/chat/{session_id}` | Chat with the Architect AI about the design |
| `GET` | `/api/session/{session_id}` | Retrieve a completed session |
| `GET` | `/api/session/{session_id}/csv/{csv_name}` | Download a target CSV or deliverable file |

### Review Decision Payloads

```json
{"approved": true}
{"approved": false, "feedback": "consolidate low-traffic QMs"}
{"approved": false, "abort": true}
```

---

## Deliverables Generated

| Deliverable | Format | Description |
|-------------|--------|-------------|
| Target topology dataset | CSV | Queue managers, channels, queues, applications in target state |
| Unified target CSV | CSV | Single file matching input schema for automation |
| Complexity algorithm | Markdown | Algorithm description, weights, rationale |
| Complexity scores | CSV | Per-factor as-is vs target breakdown with reductions |
| Topology visualizations | Interactive (D3.js) | As-is and target graphs with diff overlay |
| Architecture Decision Records | Structured JSON + Markdown | AI-generated and enriched ADRs |
| Migration plan | Markdown | 4-phase ordered steps with MQSC and rollback |
| Regression testing plan | Markdown | Test strategy, categories, acceptance criteria |
| Key insights | Markdown | Anomalies, SPOFs, bottlenecks, optimization opportunities |
| Subgraph analysis | Markdown | Connected component decomposition for both states |
| MQSC scripts | Text | Per-QM provisioning scripts |
| Executive summary | Text | AI-generated recommendation with business justification |

---

## Technology Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | LangGraph (StateGraph with conditional edges) |
| LLM | Tachyon (Wells Fargo internal) → Gemini 2.0 Flash |
| Graph Engine | NetworkX (directed graphs, community detection, centrality) |
| Data Processing | Pandas (vectorized CSV ingestion) |
| Backend | FastAPI + Uvicorn |
| Frontend | React 18 + D3.js v7 + Vite |
| State | Python TypedDict (IntelliAIState) |

---

## Dependencies

### Python (Backend)

```
langchain>=0.3.0
langgraph>=0.2.0
langsmith>=0.1.0
fastapi>=0.115.0
uvicorn>=0.30.0
pandas>=2.2.0
networkx>=3.3
pydantic>=2.7.0
python-multipart>=0.0.9
python-dotenv>=1.0.0
numpy>=1.26.0
```

### Node.js (Frontend)

```
react ^18.3.0
react-dom ^18.3.0
d3 ^7.9.0
vite ^5.4.0
```

---

## Team

**Team IntelliAI** — IBM MQ Hackathon 2026

**Mission:** Transform legacy MQ sprawl into intelligent, simplified architectures through autonomous analysis, explainable design decisions, and human-in-the-loop validation.
