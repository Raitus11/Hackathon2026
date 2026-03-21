# MQ-TITAN
## MQ Topology Intelligence & Transformation Agent Network

IBM MQ Hackathon 2026 — 8-agent LangGraph solution for transforming legacy MQ topologies.

---

## What It Does

1. Ingests 4 CSV files representing an as-is MQ environment
2. Builds a NetworkX directed graph of the topology
3. Runs 8 coordinated AI agents to analyse, redesign, validate, and provision
4. Produces: target state topology, complexity reduction metrics, ADRs, and ready-to-run MQSC scripts

**Result from synthetic demo data: 35.9 → 17.5 complexity score = 51.3% reduction, zero violations.**

---

## Project Structure

```
mq-titan/
├── backend/
│   ├── agents/agents.py          # All 8 agents
│   ├── graph/mq_graph.py         # NetworkX graph builder + complexity metrics
│   ├── tools/csv_ingest.py       # 5-step CSV cleanup pipeline
│   ├── orchestration/
│   │   ├── state.py              # MQTitanState TypedDict
│   │   └── workflow.py           # LangGraph StateGraph
│   └── api/main.py               # FastAPI server
├── frontend/
│   └── src/App.jsx               # React UI with D3.js topology viewer
├── data/sample_input/            # Synthetic CSV datasets (4 files)
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

---

## Running the Demo (no CSV needed)

With the backend running, hit:
```
POST http://localhost:8000/api/demo
```

Or click **"Run Demo"** in the React UI.

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET  | `/health` | Health check |
| POST | `/api/demo` | Run pipeline on synthetic data |
| POST | `/api/analyse` | Upload 4 CSVs and run full pipeline |
| GET  | `/api/session/{id}` | Retrieve a previous session result |

### Upload endpoint — form fields
- `queue_managers` — CSV file
- `queues`         — CSV file
- `applications`   — CSV file
- `channels`       — CSV file

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

## The 8 Agents

| Agent | Role |
|-------|------|
| Supervisor | Session init, routing |
| Researcher | CSV parsing, graph construction, violation detection |
| Analyst | 5-factor complexity scoring |
| Architect | Target state design + ADR authoring |
| Optimizer | Kernighan-Lin channel reduction |
| Tester | Constraint validation + redesign loop trigger |
| Provisioner | MQSC script generation |
| Doc Expert | Final report aggregation |

---

## Complexity Metric

```
Score = 0.30*CC + 0.25*CI + 0.20*RD + 0.15*FO + 0.10*OO   (normalised 0-100)
```
- CC = Channel Count
- CI = Coupling Index (mean QMs per app)
- RD = Routing Depth (max hops)
- FO = Fan-Out Score (max outbound channels per QM)
- OO = Orphan Objects (QMs/channels with no active app)

---


