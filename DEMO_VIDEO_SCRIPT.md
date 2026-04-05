# Intelli AI — Video Demo Script (3–5 Minutes)

## Internal Recording Guide — Team IntelliAI

---

## Pre-Recording Setup

- Pipeline already run on the 13,000-row production dataset
- Session approved, all outputs generated (do NOT wait for pipeline during recording)
- Browser at `http://localhost:3000`, 1920×1080, browser zoom 90% so all 10 tabs fit
- Terminal visible in background showing uvicorn logs (optional but impressive)
- Start on the **Upload** tab

---

## SCRIPT

### [0:00 – 0:25] THE PROBLEM (25 sec)

**Show:** Upload tab with raw CSV filename visible

> "Enterprise IBM MQ environments grow organically over years. What starts as a few queue managers becomes a dense, tangled network that no one fully understands. This is a real dataset — 13,000 rows, 259 queue managers, 438 applications, 638 channels, and over 300 applications violating the one-QM-per-app constraint. The as-is complexity score is 100 out of 100. It doesn't get worse than this."

**Action:** Click Upload (or show pre-cached session loading)

---

### [0:25 – 1:00] THE PIPELINE (35 sec)

**Show:** Trace tab — scroll through the agent execution log

> "Intelli AI runs 10 coordinated agents in a LangGraph pipeline. The Supervisor validates the upload — 3.8 megabytes, 13,000 rows. The Sanitiser cleans and normalises — 259 queue managers, 6,864 queues, 12,513 applications, 1,276 channels extracted. Zero rows removed."

**Action:** Scroll slowly, pausing on Researcher entries

> "The Researcher builds a NetworkX directed graph and runs four analytics passes. Louvain community detection finds 80 natural clusters with modularity 0.43. Betweenness centrality identifies three single points of failure — TJR1, WQ22, WL6ER4C. Shannon entropy is 3.3 bits — a skewed, fragile topology. The AI Anomaly Detective confirms: health status CRITICAL."

**Action:** Scroll to Analyst

> "The Analyst computes our 6-factor complexity score. As-is: 100 out of 100. Coupling index 5.16 — meaning each app connects to over 5 queue managers on average. Fan-out 31 — one QM has 31 outbound channels. 53 orphan queue managers with no applications."

---

### [1:00 – 1:30] THE ARCHITECT (30 sec)

**Show:** Continue scrolling Trace, pause on Architect entries

> "Now the Architect takes over. Phase A applies our deterministic rule engine — 438 apps, 438 dedicated queue managers. Every application gets exactly one QM. This is the hard constraint, and our rules guarantee it with mathematical certainty."

> "Phase C calls the LLM — Gemini 2.0 Flash via our internal Tachyon client. It analyses clusters and makes intelligent reassignment decisions: isolate PCI bridge apps like VX, OK, and OJ into dedicated compliance zones. Move payment-critical apps 8SOR, 8SCK, and TGYI into their own security boundary. The LLM proposes 10 reassignments. Our rule engine validates each one — 0 rejected, 3 ADRs generated."

---

### [1:30 – 1:55] OPTIMIZER & TESTER (25 sec)

**Show:** Scroll to Optimizer and Tester entries in Trace

> "The Optimizer runs two-phase graph reduction. Phase 1: reachability pruning removes 305 dead channels. Phase 2: weighted Minimum Spanning Tree eliminates 439 redundant channels. Total removed: 744 channels. Kernighan-Lin bisection confirms the topology is a clean DAG — no cycles."

> "The Tester validates 8 hard constraints. One-QM-per-app, sender-receiver pairs, deterministic channel naming, XMITQ existence, consumer queues, orphan QMs, path completeness. Result: PASS — zero critical violations."

---

### [1:55 – 2:25] THE RESULTS (30 sec)

**Show:** Switch to Metrics tab — score gauges and breakdown table

> "The result: complexity drops from 100 to 45.4 — a 54.6% reduction. Coupling index hits a perfect 1.0 — every app owns exactly one queue manager. Channel count drops from 638 to 492. Fan-out reduced. Orphans eliminated."

**Action:** Switch to Topology tab — Side-by-Side view

> "On the left — the as-is topology. A dense hairball of shared queue managers and tangled channels. On the right — the clean target state. Same 438 applications, radically different structure."

**Action:** Select an app from the TRACE APP dropdown (e.g. AlertManager_2)

> "You can trace any application. Here's AlertManager_2 — in the as-is state, buried inside a massive shared cluster. In the target — its own dedicated queue manager, clear ownership, standardised routing."

---

### [2:25 – 2:45] DIFF VIEW (20 sec)

**Show:** Click "Diff Overlay" toggle

> "This is our topology diff. The inner ring is the original backbone — unchanged queue managers. The outer ring is newly created dedicated QMs in green. Red lines are removed channels — over 700 eliminated. Red X markers are decommissioned queue managers. The legend shows the exact counts."

**Action:** Pause 3 seconds on the diff visual — let it breathe

---

### [2:45 – 3:20] HUMAN-IN-THE-LOOP (35 sec)

**Show:** Switch to Review tab — show the review panel and chat

> "This is not a black box. The pipeline pauses at the Human Review Gate. The reviewer sees the complexity reduction, the constraint status, all ADRs, and can chat with the Architect AI before deciding."

**Action:** Type a question in the chat, e.g. "Why did you isolate the PCI apps?" — show the response

> "The Architect responds with real entity names from the data — actual QM IDs, app IDs, channel names. It explains its reasoning using the ADRs it wrote."

**Action:** Show the "Revise with Feedback" button

> "If the reviewer disagrees, they type feedback — say, 'consolidate low-traffic QMs' — and click Revise. The Revision Architect LLM interprets the feedback, applies targeted changes, and the pipeline re-runs through Optimizer and Tester. It re-pauses here for another round. The reviewer stays in control."

**Action:** (If you have the revision run cached) Show the revision trace briefly — decommissioned QMs, updated scores

> "In our test, the revision decommissioned 14 low-traffic QMs, improved the score from 48.3 to 45.4, and reduced warnings from 22 to 9. The Architect flagged the risk — those apps might have seasonal workloads — but respected the human override."

---

### [3:20 – 3:55] DELIVERABLES (35 sec)

**Show:** Switch to CSVs tab — hero cards and deliverable grid

> "On approval, three more agents execute. The Provisioner generates MQSC scripts — 3,750 commands across 433 queue managers. The Migration Planner produces 2,605 ordered steps across four phases: CREATE, REROUTE, DRAIN, CLEANUP. Each step has forward MQSC, rollback MQSC, dependency tracking, and verification commands."

**Action:** Scroll through the deliverable cards

> "The Doc Expert generates all required documentation. Target topology in the exact input CSV format — feed it back through the pipeline to verify. Complexity algorithm with weights and rationale. Complexity scores CSV. Regression testing plan. Strategic insights. Subgraph analysis for both as-is and target states."

**Action:** Switch to Migration tab briefly — show the 4-phase plan and risk assessment

> "The AI Risk Assessor rates each phase — CREATE is low risk, REROUTE is high. It recommends maintenance windows: CREATE during business hours, REROUTE and DRAIN together during a weekend window."

---

### [3:55 – 4:20] MQSC & ADRs (25 sec)

**Show:** Switch to MQSC tab — show a per-QM script

> "Every queue manager gets a production-ready MQSC script. Listeners, local queues, transmission queues, remote queue definitions, sender and receiver channels — all in correct dependency order. Runnable directly via runmqsc."

**Action:** Switch to ADRs tab — show the ADR cards

> "Architecture Decision Records document every major design choice. Why PCI apps were isolated. Why payment-critical workloads got their own zone. Why specific bridge apps were placed where they are. Each ADR has context, rationale, and consequences — enterprise-grade documentation generated by AI."

---

### [4:20 – 4:50] CLOSING (30 sec)

**Show:** Switch to Report tab briefly, then back to Metrics gauges for the closing shot

> "Intelli AI: 10 agents, 12 LLM roles across the pipeline, powered by Tachyon and Gemini. A hybrid intelligence architecture — deterministic rules guarantee compliance, LLM agents add reasoning. From 13,000 rows of legacy MQ sprawl to a fully provisioned, production-ready target state."

> "54.6% complexity reduction. Zero critical violations. Every hackathon deliverable generated — target topology, complexity analysis, visualisations, ADRs, migration plan, regression testing plan, insights, subgraph analysis, MQSC scripts, and an executive summary. All with human-in-the-loop review and AI-powered revision."

> "Team Intelli AI."

**Action:** Hold on the Metrics gauges for 3 seconds — end recording

---

## RECORDING TIPS

1. **Pre-cache both sessions** — the initial run AND the revision run. Switch between them if needed to show the revision workflow.

2. **Screen resolution** — 1920×1080, browser at 90% zoom. All 10 tabs must be visible in the tab bar.

3. **Mouse movements** — slow and deliberate. Don't rush clicks. Let each screen breathe for 2–3 seconds before narrating over it.

4. **Voice** — confident, technical, measured pace. Every sentence delivers a fact or a number. No filler words.

5. **The money shots** (pause on these):
   - Trace tab with all agents listed top to bottom
   - The as-is vs target side-by-side topology
   - The diff overlay (red removed, green added)
   - The complexity score drop: 100 → 45.4
   - The chat response from Architect AI with real entity names
   - The CSVs tab deliverable grid
   - The MQSC scripts

6. **Tab order for the recording:**
   Upload → Trace → Metrics → Topology (side-by-side → app trace → diff) → Review (chat) → CSVs → Migration → MQSC → ADRs → Report → Metrics (closing shot)

7. **Timing target** — aim for 4:00 to 4:30. Under 3:00 feels rushed. Over 5:00 loses attention.

8. **Backup plan** — if anything crashes during recording, have the screenshots ready as a fallback narrated walkthrough.

---

## RECORDING TOOLS

- **OBS Studio** (free) — screen + mic recording, export MP4
- **Windows Game Bar** (Win+G) — quick capture alternative
- Keep file under 100MB for GitHub upload
- If over 100MB, use Git LFS or compress with HandBrake

---

## TAB REFERENCE

| # | Tab | What to Show | Time Spent |
|---|-----|-------------|------------|
| 1 | Upload | File drop, pipeline trigger | 10 sec |
| 2 | Trace | Agent execution log, pipeline flow | 40 sec |
| 3 | Metrics | Score gauges, 6-factor breakdown | 15 sec |
| 4 | Topology | Side-by-side, app trace, diff overlay | 35 sec |
| 5 | Review | Chat with Architect, revise button | 25 sec |
| 6 | CSVs | Deliverable hero cards | 15 sec |
| 7 | Migration | 4-phase plan, risk assessment | 10 sec |
| 8 | MQSC | Per-QM scripts | 10 sec |
| 9 | ADRs | Decision records | 10 sec |
| 10 | Report | Executive summary, closing shot | 10 sec |

---

*Recording guide for Team IntelliAI | IBM MQ Hackathon 2026*
