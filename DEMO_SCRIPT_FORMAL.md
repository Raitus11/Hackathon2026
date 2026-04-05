# Intelli AI — Formal Demo Script

## IBM MQ Intelligent Hackathon 2026 | Team IntelliAI

---

## 1. Solution Overview

Intelli AI is a 10-agent LangGraph pipeline that transforms legacy IBM MQ topologies into simplified, standards-compliant, automation-ready architectures. The system ingests a single raw MQ dataset, builds a directed graph, and orchestrates AI agents to analyse, redesign, validate, and provision a target state — all with human-in-the-loop review.

**Architecture:** Hybrid intelligence — deterministic rules guarantee constraint compliance (1 QM per app, deterministic channel naming, standardised routing), while LLM agents (Tachyon / Gemini 2.0 Flash) handle cluster analysis, compliance zone isolation, feedback interpretation, and documentation generation. The pipeline produces a valid target state even without LLM access (rule-only fallback).

**Technology Stack:** LangGraph (orchestration), NetworkX (graph analytics), Pandas (data processing), FastAPI (backend), React + D3.js (frontend), Tachyon with Gemini 2.0 Flash (LLM).

---

## 2. Demo Environment

| Component | Details |
|-----------|---------|
| Backend | FastAPI + Uvicorn on port 8000 |
| Frontend | React + Vite on port 3000 |
| Dataset | 13,000-row MQ Raw Data CSV (production-scale) |
| LLM | Tachyon (Wells Fargo internal) → Gemini 2.0 Flash |
| Pipeline runtime | ~30 seconds end-to-end for 13,000 rows |

**Startup commands:**

```
Terminal 1: cd src && python -m uvicorn backend.api.main:app --host 0.0.0.0 --port 8000
Terminal 2: cd frontend && npm run dev
```

---

## 3. Input Dataset Profile

| Metric | Value |
|--------|-------|
| Raw rows | 12,696 |
| Queue managers | 259 |
| Queues | 6,864 |
| Applications | 438 (unique app IDs) |
| Application-QM relationships | 12,513 |
| Channels (inferred) | 1,276 (638 sender/receiver pairs) |
| Multi-QM app violations | 310 (apps connected to >1 QM) |
| Orphan QMs | 53 (no apps connected) |
| Cycles detected | 21 |
| As-is complexity score | 100.0 / 100 |

---

## 4. Pipeline Walkthrough

### Step 1: Upload & Supervisor

The user uploads a single CSV or Excel file via the Upload tab. The Supervisor agent validates the file (3,827,730 bytes), generates a session ID, and routes to the Sanitiser.

**What to observe:** File accepted, session initialised, routing logged in Trace tab.

### Step 2: Sanitiser

Cleans, deduplicates, and normalises the raw data. Transforms the single file into four logical tables: queue managers, queues, applications, and channels.

**What to observe:** "Data sanitised: 259 queue_managers, 6864 queues, 12513 applications, 1276 channels. 0 issues found, 0 rows removed."

### Step 3: Researcher

Builds a NetworkX directed graph (nodes: QMs, apps, queues; edges: connects_to, channel, owns). Runs five analysis passes:

| Analysis | Result |
|----------|--------|
| Graph construction | 259 QMs, 438 apps, 638 channels, 310 multi-QM violations, 53 orphan QMs, 21 cycles |
| Subgraph decomposition | 74 connected components, 67 isolated QMs, largest component: 176 QMs / 416 apps |
| Louvain community detection | 80 communities, modularity 0.4262 |
| Betweenness centrality | SPOFs identified: TJR1, WQ22, WL6ER4C |
| Shannon entropy | 3.311 bits (density 0.0182) — skewed hub-and-spoke topology |

Additionally calls the LLM Anomaly Detective which reports 8 anomalies and rates the topology health as CRITICAL — characterised by extreme centralisation onto a few single points of failure and fragmentation.

**What to observe:** Researcher entries in the Trace tab showing all five analysis results.

### Step 4: Analyst

Computes the 6-factor weighted complexity score for the as-is topology:

```
Score = 0.25×CC + 0.25×CI + 0.20×RD + 0.15×FO + 0.05×OO + 0.10×CS
```

| Factor | Value | Interpretation |
|--------|-------|---------------|
| Channel Count (CC) | 638 | High — each channel is a failure point |
| Coupling Index (CI) | 5.16 | Critical — apps connect to 5+ QMs on average |
| Routing Depth (RD) | 79.0 | Extreme — fragmented disconnected components |
| Fan-Out (FO) | 31.0 | Severe — one QM has 31 outbound channels |
| Orphan Objects (OO) | 53.0 | Significant waste |
| Channel Sprawl (CS) | 2.46 | Channels per QM ratio |
| **Total Score** | **100.0 / 100** | Maximum complexity |

**What to observe:** Analyst entry in Trace tab, Metrics tab showing score gauges.

### Step 5: Architect

Designs the target state topology using a three-phase approach:

**Phase A — Rules Baseline:** Deterministic rule engine assigns each of the 438 apps to a dedicated QM (438 apps → 438 QMs). This guarantees 100% compliance with the 1-QM-per-app constraint. No LLM involved — mathematical certainty.

**Phase B — LLM Cluster Analysis:** Sends an 18,000-character cluster-based prompt (~4,600 tokens) to Gemini 2.0 Flash via Tachyon. The LLM analyses mixed-use clusters and proposes reassignments:

- Isolate high-volume PCI bridge apps (VX → QM_PCI_VX, OK → QM_PCI_OK, OJ → QM_PCI_OJ) from mixed-use clusters to enforce compliance boundaries
- Separate payment-critical apps (8SOR → QM_PAY_CRIT_8SOR, 8SCK → QM_PAY_CRIT_8SCK, TGYI → QM_PAY_CRIT_TGYI) to reduce blast radius
- Consolidate PCI apps into dedicated zones

The rule engine validates each LLM reassignment — INCORRECT assignments are rejected with logged explanations.

**Phase C — Target Assembly:** 10–12 reassignments applied, 0 rejected, 3–5 ADRs generated, 4–5 insights produced.

**Channel inference:** Flow-aware producer→consumer analysis infers channels based on actual message flow patterns. Deterministic naming: sender = `FROM_QM.TO_QM`, receiver = `TO_QM.FROM_QM`.

**What to observe:** Architect entries in Trace, Phase A/B/C progression, reassignment decisions with rationale.

### Step 6: Optimizer

Two-phase channel reduction on the target graph:

| Phase | Method | Channels Removed |
|-------|--------|-----------------|
| Phase 1 | Reachability pruning | ~287–305 dead channels |
| Phase 2 | Weighted MST | ~437–439 redundant channels |
| **Total** | | **~724–744 channels removed** |

Also runs:
- Kernighan-Lin bisection (identifies 2 natural clusters, 196 QMs each, 22–25 cross-cluster channels)
- Louvain community detection on target (42–57 natural clusters, modularity 0.89–0.90)
- SPOF analysis on target (10 high-betweenness QMs)
- Topology entropy (1.84–1.94 bits, 21–22% of theoretical max)
- LLM Design Critic (flags critical issues for transparency)
- LLM Capacity Planner (balance score 30–35/100, identifies 4 hotspots)

**What to observe:** Optimizer entries in Trace, channel reduction progression, target complexity score.

### Step 7: Tester

Validates 8 hard constraints on the target state:

| Check | Description | Result |
|-------|-------------|--------|
| V-001 | 1-QM-per-app — each app connects to exactly one QM | PASS |
| V-002 | Sender/receiver channel pairs exist | PASS |
| V-003 | Deterministic channel naming (FROM_QM.TO_QM) | PASS |
| V-004 | XMITQ exists for each sender channel | PASS |
| V-005 | Consumer queues are local | PASS |
| V-006 | No orphan QMs (warning-level) | WARNINGS |
| V-007 | Path completeness | PASS |
| V-009 | No shared QMs (CRITICAL check) | PASS |

Additionally calls the LLM Compliance Auditor (score 35/100, 7–8 findings on HA and SPOF concerns — informational, not constraint violations).

**Result:** PASS — 0 critical violations, 9–22 warnings (orphan QMs flagged for awareness).

**What to observe:** Tester entry in Trace, constraint check results.

### Step 8: Human Review Gate

Pipeline pauses. The frontend displays:

- Complexity reduction: 100.0 → 45.4–48.3 (51.7–54.6% reduction)
- Constraint status: 0 critical violations
- ADR count: 3–5 written
- Architect method used

**Three options:**

| Action | What Happens |
|--------|-------------|
| **Approve** | Pipeline continues to Provisioner → Migration Planner → Doc Expert |
| **Revise with Feedback** | Architect re-runs with human directives, re-validates, re-pauses |
| **Abort** | Doc Expert generates cancellation report |

**Ask the Architect chat:** The reviewer can chat with the Architect AI before deciding. The Architect responds with specific QM names, app IDs, and ADR references from the actual topology.

**What to observe:** Review tab with approve/revise/abort controls, chat panel, complexity score display.

### Step 8a: Revision Workflow (if Revise selected)

When the human provides feedback (e.g. "consolidate low-traffic QMs" or "remove apps with no message flows"), the Revision Architect:

1. Interprets feedback via LLM (confidence: HIGH/MEDIUM/LOW)
2. Parses directives: `aggressive`, `fanout_cap`, `channel_prune_pct`, `consolidate_qm_pct`, `protect_qms`
3. Re-runs Phase A with LLM revision deltas (decommissions specified QMs)
4. Applies feedback-driven consolidation (merges self-contained zero-flow apps)
5. Re-runs Phase C LLM cluster analysis on the updated graph
6. Pipeline continues through Optimizer → Tester → Human Review (re-pauses)

**Demonstrated revision result:** QMs reduced from 446 to 433, warnings reduced from 22 to 9, complexity improved from 48.3 to 45.4.

**What to observe:** Trace entries showing ARCHITECT-REVISION decommissions, ARCHITECT-CONSOLIDATE merges, updated scores.

### Step 9: Provisioner (after Approve)

Generates two categories of output:

**MQSC Scripts:** 3,750 commands across 433 queue managers. Each QM gets its own script in correct dependency order:
1. LISTENER (TCP port)
2. QLOCAL (application queues)
3. QLOCAL USAGE(XMITQ) (transmission queues)
4. QREMOTE (with RQMNAME, RNAME, XMITQ attributes)
5. CHANNEL CHLTYPE(SDR) (sender channels with CONNAME)
6. CHANNEL CHLTYPE(RCVR) (receiver channels)
7. START CHANNEL

Runnable via: `runmqsc QM_NAME < QM_NAME_target.mqsc`

**Target State CSVs:** target_queue_managers, target_channels, target_queues, target_applications, MQ_Raw_Data_Target (unified 29-column CSV matching input schema), target-topology (JSON).

**What to observe:** MQSC tab showing per-QM scripts, CSVs tab showing downloadable files.

### Step 10: Migration Planner

Computes topology diff and generates ordered migration steps:

| Metric | Value |
|--------|-------|
| Total migration steps | 2,605 |
| QMs added | ~330 |
| QMs removed | ~156 |
| Channels added | ~486 |
| Channels removed | ~632 |
| Apps reassigned | ~369 |

**4-Phase Plan:**

| Phase | Steps | Risk | Maintenance Window |
|-------|-------|------|-------------------|
| CREATE | 816 | LOW | Standard business hours |
| REROUTE | 369 | HIGH | Weekend maintenance window |
| DRAIN | 632 | MEDIUM | Combined with REROUTE |
| CLEANUP | 788 | MEDIUM | Post-validation |

Each step includes forward MQSC, rollback MQSC, dependency tracking, and a verification command. The LLM Risk Assessor identifies 5 high-risk steps and recommends execution windows.

**What to observe:** Migration tab showing 4-phase plan, risk assessment, topology diff.

### Step 11: Doc Expert

Generates all deliverable documentation:

| Deliverable | Description |
|-------------|-------------|
| complexity-algorithm.md | 6-factor algorithm with weights and rationale |
| complexity-scores.csv | Per-factor as-is vs target scores with reductions |
| regression-testing-plan.md | Test strategy, categories, acceptance criteria |
| insights.md | Anomalies, SPOFs, bottlenecks, optimisation opportunities |
| migration-plan.md | Phased migration with dependencies and rollback |
| subgraph-analysis.md | Connected component decomposition for both states |
| ADRs (enriched) | 5–10 enterprise-grade Architecture Decision Records |
| Executive summary | AI-generated recommendation with business justification |
| Final report | Complete transformation report |

Additionally calls 3 LLM roles: ADR Enricher, Executive Summariser, and the Doc Expert's own synthesis.

**What to observe:** CSVs tab showing all deliverables available for download, Report tab showing executive summary.

---

## 5. Key Demonstration Points

### Constraint Compliance

The target state enforces all hackathon constraints:

| Constraint | How It's Enforced |
|-----------|------------------|
| 1 QM per application | Rule engine Phase A — mathematical guarantee, validated by Tester V-001 and V-009 |
| Deterministic channel naming | Sender: `FROM_QM.TO_QM`, Receiver: `TO_QM.FROM_QM` — validated by Tester V-003 |
| Message routing pattern | Producer → local queue → XMITQ → sender channel → receiver channel → local queue → consumer — validated by Tester V-004, V-005, V-007 |
| Channels inferred, not assumed | Flow-aware producer→consumer analysis, not cartesian product |
| No assumed information | All decisions derived from input data columns only |

### Complexity Reduction

| Metric | As-Is | Target | Reduction |
|--------|-------|--------|-----------|
| Total complexity score | 100.0 | 45.4 | 54.6% |
| Coupling index | 5.16 | 1.0 | 80.6% |
| Channel count | 638 | 492 | 22.9% |
| Orphan QMs | 53 | 0 | 100% |
| Critical violations | 310+ | 0 | 100% |

### Hybrid Intelligence

| Component | Role | Guarantee |
|-----------|------|-----------|
| Rule engine | Constraint enforcement, baseline assignment | 100% compliance, deterministic, milliseconds |
| LLM (Tachyon/Gemini) | Cluster analysis, PCI isolation, feedback interpretation, ADR generation | Intelligent reasoning, validated by rules |
| Self-correction | LLM proposals checked by rule engine | INCORRECT assignments rejected with explanation |
| Fallback | Full rule-only path if LLM unavailable | Valid target state without any LLM |

### Human-in-the-Loop

| Capability | Description |
|-----------|-------------|
| Review gate | Pipeline pauses for human decision |
| Ask the Architect | Chat with AI about design decisions using real entity names |
| Revise with Feedback | Natural-language directives interpreted by LLM, re-validated |
| Revision guardrails | Architect warns about risks (e.g. decommissioning DR apps) but respects human override |
| Abort | Clean cancellation with documentation |

---

## 6. Deliverables Checklist

| # | Required Deliverable | Status | Location |
|---|---------------------|--------|----------|
| 1 | Target state topology dataset (CSV) | Generated | CSVs tab — target_queue_managers, target_channels, target_queues, target_applications, MQ_Raw_Data_Target |
| 2 | Complexity analysis (as-is vs target) | Generated | Metrics tab (interactive) + complexity-scores.csv + complexity-algorithm.md |
| 3 | Topology visualisations | Generated | Topology tab — side-by-side, diff overlay, per-app trace (D3.js interactive) |
| 4 | Design & decision documentation | Generated | ADRs tab (5–10 ADRs) + insights.md + Report tab (executive summary) |
| 5 | Migration plan | Generated | Migration tab (interactive) + migration-plan.md |
| 6 | Regression testing plan | Generated | regression-testing-plan.md in CSVs tab |
| 7 | Subgraph analysis | Generated | subgraph-analysis.md in CSVs tab |
| 8 | MQSC provisioning scripts | Generated | MQSC tab — per-QM scripts, runnable via runmqsc |

---

## 7. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        FRONTEND (React + D3.js)                     │
│  Upload │ Review │ Topology │ Metrics │ ADRs │ Migration │ MQSC │  │
│  CSVs │ Report │ Trace                                              │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ REST API
┌──────────────────────────▼──────────────────────────────────────────┐
│                     FastAPI (main.py)                                │
│  POST /api/upload │ POST /api/review │ POST /api/chat │ GET /api/…  │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────────┐
│                  LangGraph StateGraph (workflow.py)                  │
│                                                                     │
│  SUPERVISOR → SANITISER → RESEARCHER → ANALYST → ARCHITECT          │
│       │                                              │              │
│       │                        ┌─────────────────────┘              │
│       │                        ▼                                    │
│       │                    OPTIMIZER → TESTER ──┐                   │
│       │                        ▲                │                   │
│       │                        └── retry (×3) ──┘                   │
│       │                                         │                   │
│       │                                    HUMAN REVIEW              │
│       │                                    │    │    │               │
│       │                              APPROVE REVISE ABORT           │
│       │                                │      │      │              │
│       │                          PROVISIONER  │   DOC EXPERT        │
│       │                                │      │                     │
│       │                          MIGRATION    │                     │
│       │                           PLANNER     │                     │
│       │                                │      │                     │
│       │                          DOC EXPERT   │                     │
│       │                                │      │                     │
│       │                               END    END                    │
└─────────────────────────────────────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────────┐
│                   Supporting Modules                                 │
│                                                                     │
│  csv_ingest.py    │ Raw CSV → 4 logical tables (vectorized Pandas)  │
│  mq_graph.py      │ NetworkX graph + Louvain + centrality + entropy │
│  llm_client.py    │ Tachyon client with retry, circuit breaker      │
│  prompts.py       │ LLM prompt templates for 12 AI roles            │
│  state.py         │ IntelliAIState TypedDict (pipeline state)       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 8. LLM Roles

The pipeline uses the LLM across 12 distinct roles, each with its own system prompt, temperature, and max tokens:

| # | Role | Agent | Purpose |
|---|------|-------|---------|
| 1 | Anomaly Detective | Researcher | Detects topology anomalies, rates overall health |
| 2 | Cluster Analyst | Architect | Analyses mixed-use clusters, proposes reassignments |
| 3 | Revision Architect | Architect | Interprets human feedback, applies targeted changes |
| 4 | Design Critic | Optimizer | Reviews target state for critical design flaws |
| 5 | Capacity Planner | Optimizer | Evaluates load balance and identifies hotspots |
| 6 | Compliance Auditor | Tester | Audits HA, security, and regulatory compliance |
| 7 | Risk Assessor | Migration Planner | Rates migration phase risks, recommends windows |
| 8 | ADR Enricher | Doc Expert | Enhances ADRs with enterprise-grade detail |
| 9 | Executive Summariser | Doc Expert | Generates business-level recommendation |
| 10 | Chat Responder | Review (chat) | Answers reviewer questions about the design |
| 11 | Feedback Parser | Architect | Extracts structured directives from natural language |
| 12 | Bridge Analyst | Architect | Identifies bridge apps needing special QM placement |

---

## 9. Future Roadmap

**Day-2 Operations — Topology Lifecycle Management**

| Capability | Description |
|-----------|-------------|
| **Add App** | New application enters the estate. Agent identifies optimal QM placement based on producer/consumer relationships, traffic profile, and compliance zone. Provisions QM, channels, queues. Re-validates via Tester. |
| **Remove App** | Application decommissioned. Agent traces all dependent channels, queues, XMIT objects. Generates CLEANUP MQSC. Evaluates orphaned QM for consolidation or decommission. |
| **Reroute / Reconnect** | Application changes upstream or downstream partners. Agent computes channel delta, checks for SPOF introduction or compliance boundary violations, produces REROUTE migration plan with rollback. |

Same 10-agent pipeline, applied incrementally — topology stays optimised as the estate evolves.

---

*Formal demo script submitted by Team IntelliAI | IBM MQ Intelligent Hackathon 2026*
