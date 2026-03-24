# MQ-TITAN
## MQ Topology Intelligence & Transformation Agent Network

IBM MQ Hackathon 2026 — 10-agent LangGraph pipeline with Groq LLM for transforming legacy MQ topologies.

---

## What It Does

1. Ingests 4 CSV files representing an as-is MQ environment
2. Builds a NetworkX directed graph of the topology
3. Runs **10 coordinated AI agents** to analyse, redesign, validate, and provision
4. **Human-in-the-loop review** — approve, revise with feedback, or abort
5. Produces: target state topology, complexity metrics, AI-generated ADRs, per-QM MQSC scripts, target CSVs, and a **migration plan with rollback**

**Result from sample data: 39.1 → 26.2 complexity score = 33% reduction, zero constraint violations.**

---

## Key Differentiators

- **LLM-Powered Architecture** — Groq (Llama 3.3 70B) reasons about topology and generates ADRs referencing actual entity names. Falls back to deterministic rules if no API key.
- **Valid MQSC Output** — Per-QM scripts with QLOCAL, QREMOTE, XMITQ, LISTENER, SDR/RCVR channels, correct ordering. Runnable via `runmqsc QM_NAME < file.mqsc`.
- **Migration Plan with Rollback** — 4-phase ordered steps (CREATE → REROUTE → DRAIN → CLEANUP) with forward MQSC, rollback MQSC, dependency tracking, and verification commands.
- **Human-in-the-Loop** — Pipeline pauses for human review. Approve to provision, revise with feedback the LLM acts on, or abort with a cancellation report.

---

## Project Structure

```
mq-titan/
├── backend/
│   ├── agents/agents.py          # All 10 agents
│   ├── llm/
│   │   ├── llm_client.py         # Groq API wrapper with retry/fallback
│   │   └── prompts.py            # Architect system + user prompt templates
│   ├── graph/mq_graph.py         # NetworkX graph builder + complexity metrics
│   ├── tools/csv_ingest.py       # 6-step CSV cleanup pipeline
│   ├── orchestration/
│   │   ├── state.py              # MQTitanState TypedDict
│   │   └── workflow.py           # LangGraph StateGraph (10 nodes)
│   └── api/main.py               # FastAPI server
├── frontend/
│   └── src/App.jsx               # React UI with D3.js topology viewer
├── data/sample_input/            # Sample CSV datasets (4 files)
├── .env.example                  # API key template (copy to .env)
├── .gitignore
└── requirements.txt
```

---

## Setup

### Backend
```bash
cd mq-titan
pip install -r requirements.txt
uvicorn backend.api.main:app --reload --port 8000
```

### Frontend
```bash
cd frontend
npm install
npm run dev       # runs on http://localhost:3000
```

### LLM (optional — pipeline works without it)
```bash
pip install groq python-dotenv
cp .env.example .env
# Edit .env and add your Groq API key:
# GROQ_API_KEY=gsk_your_key_here
```

Get a free API key at [console.groq.com](https://console.groq.com) — no credit card required.

---

## Running the Demo

With the backend running, click **"Run Demo"** in the React UI, or:
```
POST http://localhost:8000/api/demo
```

The pipeline runs through all 10 agents and pauses at the **human review gate**. Review the proposed target state, then approve, revise, or abort.

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET  | `/health` | Health check |
| POST | `/api/demo` | Run pipeline on sample data |
| POST | `/api/analyse` | Upload 4 CSVs and run full pipeline |
| GET  | `/api/review/{id}` | Get pending review data |
| POST | `/api/review/{id}` | Submit review: approve / revise / abort |
| GET  | `/api/session/{id}` | Retrieve a completed session |
| GET  | `/api/session/{id}/csv/{name}` | Download a target CSV file |

### Review decisions
```json
{"approved": true}                                 // approve → generate outputs
{"approved": false, "feedback": "merge EU QMs"}     // revise → LLM redesigns
{"approved": false, "abort": true}                  // abort → cancellation report
```

---

## CSV Format

### queue_managers.csv
```
qm_id, qm_name, region, host, description
```

### queues.csv
```
queue_id, queue_name, qm_id, queue_type, usage, description
```

### applications.csv
```
app_id, app_name, qm_id, direction (PRODUCER|CONSUMER), queue_id, description
```

### channels.csv
```
channel_id, channel_name, channel_type (SENDER|RECEIVER), from_qm, to_qm, xmit_queue, status, description
```

---

## The 10 Agents

| # | Agent | Role |
|---|-------|------|
| 1 | **Supervisor** | Session init, input validation |
| 2 | **Sanitiser** | CSV cleanup, dedup, referential integrity |
| 3 | **Researcher** | Graph construction, violation detection |
| 4 | **Analyst** | 5-factor complexity scoring |
| 5 | **Architect** | LLM-powered target state design + ADRs (Groq) |
| 6 | **Optimizer** | Reachability pruning + MST channel reduction |
| 7 | **Tester** | 8 constraint checks, redesign loop trigger |
| 8 | **Provisioner** | Per-QM MQSC scripts + target state CSVs |
| 9 | **Migration Planner** | 4-phase migration with rollback MQSC |
| 10 | **Doc Expert** | Final transformation report |

**Human Review Gate** sits between Tester and Provisioner — pipeline pauses for human approval.

---

## Pipeline Flow

```
Supervisor → Sanitiser → Researcher → Analyst → Architect → Optimizer → Tester
                                                                          │
                                          ┌──── fail (retries left) ──────┘
                                          │
                                          ▼
                                      Architect (retry)
                                          │
                                          ▼ pass / retries exhausted
                                   Human Review Gate
                                    │       │       │
                              approve   revise    abort
                                 │        │         │
                                 ▼        ▼         ▼
                            Provisioner  Architect  Doc Expert
                                 │                  (abort report)
                                 ▼
                          Migration Planner
                                 │
                                 ▼
                            Doc Expert → END
```

---

## Complexity Metric

```
Score = 0.30×CC + 0.25×CI + 0.20×RD + 0.15×FO + 0.10×OO   (normalised 0–100)
```

| Factor | Weight | What it measures | How to improve |
|--------|--------|------------------|----------------|
| CC — Channel Count | 30% | Number of sender channels | Remove unnecessary channels |
| CI — Coupling Index | 25% | Mean QMs per app (ideal = 1.0) | Enforce 1-QM-per-app |
| RD — Routing Depth | 20% | Max hops between QMs | Eliminate multi-hop paths |
| FO — Fan-Out Score | 15% | Max outbound channels from one QM | Consolidate outbound routing |
| OO — Orphan Objects | 10% | QMs with no apps + stopped channels | Remove dead infrastructure |

Baselines scale with topology size. Same baselines used for both as-is and target scoring.

---

## MQSC Output

Each QM gets its own script, runnable via `runmqsc QM_NAME < QM_NAME_target.mqsc`. Objects generated in correct order:

1. LISTENER (TCP port)
2. QLOCAL (application queues)
3. QLOCAL USAGE(XMITQ) (transmission queues)
4. QREMOTE (remote queue definitions with RQMNAME, RNAME, XMITQ)
5. CHANNEL CHLTYPE(SDR) (sender channels with CONNAME)
6. CHANNEL CHLTYPE(RCVR) (receiver channels — same name as sender)
7. START CHANNEL

---

## Migration Plan

The Migration Planner generates ordered steps across 4 phases:

| Phase | What happens | Rollback |
|-------|-------------|----------|
| **CREATE** | New listeners, queues, XMITQs, channels | Delete created objects |
| **REROUTE** | Move apps to new QMs | Revert app configuration |
| **DRAIN** | Wait for old queues to empty (CURDEPTH = 0) | Non-destructive |
| **CLEANUP** | Stop old channels, delete old objects, decommission QMs | Re-create from backup |

Each step includes forward MQSC, rollback MQSC, dependency tracking, and a verification command.

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GROQ_API_KEY` | No | Groq API key for LLM-powered architecture. Pipeline falls back to rules without it. |

---

## License

Built for IBM MQ Hackathon 2026.
