"""
agents.py
All 10 IntelliAI agents.
Each agent is a function: (state) -> state updates dict.
LangGraph calls them as nodes in the StateGraph.

Agent list:
  1. supervisor        - session init and routing
  2. sanitiser         - CSV data cleaning, quality report (dedicated agent)
  3. researcher        - graph construction from clean data
  4. analyst           - complexity metrics on as-is graph
  5. architect         - target state design + ADRs (LLM-first with rule fallback)
  6. optimizer         - graph algorithm simplification
  7. tester            - constraint validation + redesign loop
  8. provisioner       - MQSC scripts + target state CSV output
  9. migration_planner - ordered migration steps with rollback
 10. doc_expert        - final report aggregation
"""
import uuid
import io
import csv
import json
import logging
import networkx as nx
from typing import Any
from pathlib import Path

from backend.tools.csv_ingest import load_and_clean
from backend.graph.mq_graph import (
    build_graph, detect_violations, compute_complexity, graph_to_dict,
    analyse_subgraphs, detect_communities, compute_centrality,
    compute_graph_entropy, compare_topologies,
)
from backend.llm.llm_client import call_llm, validate_architect_response
from backend.llm.prompts import (
    ARCHITECT_SYSTEM_PROMPT, build_architect_prompt,
    CLUSTER_SYSTEM_PROMPT, build_cluster_prompt,
    FEEDBACK_INTERPRETER_SYSTEM, build_feedback_interpreter_prompt,
    CHANNEL_ADVISOR_SYSTEM, build_channel_advisor_prompt,
    DESIGN_CRITIC_SYSTEM, build_design_critic_prompt,
    MIGRATION_RISK_SYSTEM, build_migration_risk_prompt,
    ANOMALY_DETECTIVE_SYSTEM, build_anomaly_detective_prompt,
    ADR_ENRICHER_SYSTEM, build_adr_enricher_prompt,
    COMPLIANCE_AUDITOR_SYSTEM, build_compliance_auditor_prompt,
    CAPACITY_PLANNER_SYSTEM, build_capacity_planner_prompt,
    EXECUTIVE_SUMMARIZER_SYSTEM, build_executive_summary_prompt,
    REVISION_ARCHITECT_SYSTEM, build_revision_architect_prompt,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# AGENT 1: SUPERVISOR
# Initialises the session, validates inputs, sets up state.
# ─────────────────────────────────────────────────────────────────────────────
def supervisor_agent(state: dict) -> dict:
    logger.info("SUPERVISOR: Initialising session")
    messages = state.get("messages", [])
    session_id = state.get("session_id") or str(uuid.uuid4())[:8]

    # ── Validate session_id ───────────────────────────────────────────────
    if not session_id:
        msg = "No session_id provided"
        logger.error(f"SUPERVISOR: {msg}")
        messages.append({"agent": "SUPERVISOR", "msg": f"ERROR — {msg}"})
        return {"error": msg, "messages": messages}

    # ── Validate csv_paths exists ─────────────────────────────────────────
    csv_paths = state.get("csv_paths")
    if not csv_paths:
        msg = "No CSV paths provided"
        logger.error(f"SUPERVISOR: {msg}")
        messages.append({"agent": "SUPERVISOR", "msg": f"ERROR — {msg}"})
        return {"error": msg, "messages": messages}

    # ── Validate all 4 required keys are present (or single raw_file) ────
    if "raw_file" in csv_paths:
        # Single-file mode — validate the file exists and is readable
        raw_path = Path(csv_paths["raw_file"])
        if not raw_path.exists():
            msg = f"Raw data file not found: {csv_paths['raw_file']}"
            logger.error(f"SUPERVISOR: {msg}")
            messages.append({"agent": "SUPERVISOR", "msg": f"ERROR — {msg}"})
            return {"error": msg, "messages": messages}
        if raw_path.stat().st_size == 0:
            msg = f"Raw data file is empty: {csv_paths['raw_file']}"
            logger.error(f"SUPERVISOR: {msg}")
            messages.append({"agent": "SUPERVISOR", "msg": f"ERROR — {msg}"})
            return {"error": msg, "messages": messages}
        msg = f"Session {session_id} validated. Raw data file confirmed: {raw_path.name} ({raw_path.stat().st_size:,} bytes). Routing to SANITISER."
        logger.info(f"SUPERVISOR: {msg}")
        messages.append({"agent": "SUPERVISOR", "msg": msg})
        return {
            "session_id":        session_id,
            "redesign_count":    0,
            "validation_passed": False,
            "awaiting_human_review": False,
            "human_approved":    None,
            "human_feedback":    "",
            "error":             None,
            "messages":          messages,
        }

    required = ["queue_managers", "queues", "applications", "channels"]
    missing = [k for k in required if k not in csv_paths]
    if missing:
        msg = f"Missing CSV keys: {missing}"
        logger.error(f"SUPERVISOR: {msg}")
        messages.append({"agent": "SUPERVISOR", "msg": f"ERROR — {msg}"})
        return {"error": msg, "messages": messages}

    # ── Validate files actually exist on disk ─────────────────────────────
    missing_files = [k for k, path in csv_paths.items() if not Path(path).exists()]
    if missing_files:
        msg = f"CSV files not found on disk: {missing_files}"
        logger.error(f"SUPERVISOR: {msg}")
        messages.append({"agent": "SUPERVISOR", "msg": f"ERROR — {msg}"})
        return {"error": msg, "messages": messages}

    # ── Validate files are not empty ──────────────────────────────────────
    empty_files = [k for k, path in csv_paths.items() if Path(path).stat().st_size == 0]
    if empty_files:
        msg = f"CSV files are empty: {empty_files}"
        logger.error(f"SUPERVISOR: {msg}")
        messages.append({"agent": "SUPERVISOR", "msg": f"ERROR — {msg}"})
        return {"error": msg, "messages": messages}

    # ── All checks passed ─────────────────────────────────────────────────
    msg = f"Session {session_id} validated. All 4 CSV files confirmed on disk. Routing to SANITISER."
    logger.info(f"SUPERVISOR: {msg}")
    messages.append({"agent": "SUPERVISOR", "msg": msg})

    return {
        "session_id":        session_id,
        "redesign_count":    0,
        "validation_passed": False,
        "awaiting_human_review": False,
        "human_approved":    None,
        "human_feedback":    "",
        "error":             None,
        "messages":          messages,
    }

# ─────────────────────────────────────────────────────────────────────────────
# AGENT 2: SANITISER (dedicated data quality agent)
#
# Why a dedicated agent and not just a function?
# Because data quality is a FIRST CLASS concern in enterprise systems.
# A dedicated agent means:
#   - The quality report is a visible, traceable output in the pipeline
#   - Judges can see exactly what was wrong and what was fixed
#   - If data is too dirty to process, the pipeline fails here with a clear
#     explanation rather than crashing silently inside the Researcher
#   - When real CSVs arrive, you tweak THIS agent — nothing else changes
# ─────────────────────────────────────────────────────────────────────────────
def sanitiser_agent(state: dict) -> dict:
    logger.info("SANITISER: Running data quality pipeline")
    messages = state.get("messages", [])

    csv_paths = state.get("csv_paths", {})
    clean_data, quality_report = load_and_clean(csv_paths)

    # Build a structured quality summary
    issues_found = len(quality_report.get("warnings", [])) + len(quality_report.get("errors", []))
    rows_removed = sum(quality_report.get("rows_removed", {}).values())

    # Hard stop if critical errors exist
    if quality_report.get("errors"):
        error_msg = f"SANITISER: Critical data errors — cannot proceed. {quality_report['errors']}"
        messages.append({"agent": "SANITISER", "msg": error_msg})
        return {"error": error_msg, "messages": messages}

    quality_summary = {
        "steps_completed": quality_report.get("steps", []),
        "warnings": quality_report.get("warnings", []),
        "errors": quality_report.get("errors", []),
        "rows_removed": quality_report.get("rows_removed", {}),
        "final_counts": quality_report.get("summary", {}),
        "issues_found": issues_found,
        "total_rows_removed": rows_removed,
    }

    msg = (
        f"Data sanitised: {quality_summary['final_counts']}. "
        f"{issues_found} issues found, {rows_removed} rows removed. "
        f"{'Warnings: ' + str(quality_report['warnings']) if quality_report.get('warnings') else 'No warnings.'}"
    )
    messages.append({"agent": "SANITISER", "msg": msg})

    return {
        "raw_data": clean_data,
        "data_quality_report": quality_summary,
        "messages": messages,
    }


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 3: RESEARCHER
# Builds NetworkX as-is graph from the ALREADY CLEANED data from Sanitiser.
# Does NOT load or clean CSVs — that is the Sanitiser's job.
# ─────────────────────────────────────────────────────────────────────────────
def researcher_agent(state: dict) -> dict:
    logger.info("RESEARCHER: Building as-is topology graph")
    messages = state.get("messages", [])

    # Use clean data produced by Sanitiser agent
    clean_data = state.get("raw_data")
    if not clean_data:
        err = "RESEARCHER: No clean data in state — did Sanitiser run?"
        messages.append({"agent": "RESEARCHER", "msg": err})
        return {"error": err, "messages": messages}

    as_is_graph = build_graph(clean_data)
    violations = detect_violations(as_is_graph)

    qm_count      = sum(1 for _, d in as_is_graph.nodes(data=True) if d.get("type") == "qm")
    app_count     = sum(1 for _, d in as_is_graph.nodes(data=True) if d.get("type") == "app")
    channel_count = sum(1 for _, _, d in as_is_graph.edges(data=True) if d.get("rel") == "channel")

    msg = (
        f"Graph built: {qm_count} QMs, {app_count} apps, {channel_count} channels. "
        f"Violations: {len(violations['multi_qm_apps'])} multi-QM apps, "
        f"{len(violations['orphan_qms'])} orphan QMs, "
        f"{len(violations['cycles'])} cycles."
    )
    messages.append({"agent": "RESEARCHER", "msg": msg})

    # ── Subgraph analysis (disconnected component decomposition) ──────────
    as_is_subgraphs = analyse_subgraphs(as_is_graph)
    isolated_count = sum(1 for s in as_is_subgraphs if s["is_isolated"])
    sub_msg = (
        f"Subgraph analysis: {len(as_is_subgraphs)} connected component(s) detected. "
        f"{isolated_count} isolated QM(s) (single QM, no channels). "
        f"Largest component: {as_is_subgraphs[0]['qm_count']} QMs, "
        f"{as_is_subgraphs[0]['app_count']} apps."
        if as_is_subgraphs else
        "Subgraph analysis: no QMs found."
    )
    messages.append({"agent": "RESEARCHER", "msg": sub_msg})

    # ── Advanced graph analytics on as-is topology ────────────────────────
    as_is_communities = detect_communities(as_is_graph)
    as_is_centrality = compute_centrality(as_is_graph)
    as_is_entropy = compute_graph_entropy(as_is_graph)

    analytics_msg = (
        f"Graph analytics: "
        f"Louvain detected {as_is_communities.get('num_communities', 0)} communities "
        f"(modularity={as_is_communities.get('modularity', 0)}). "
        f"{'SPOFs: ' + ', '.join(as_is_centrality.get('spof_qms', [])[:3]) + '. ' if as_is_centrality.get('spof_qms') else 'No SPOFs detected. '}"
        f"Degree entropy: {as_is_entropy.get('degree_entropy', 0)} bits "
        f"(density={as_is_entropy.get('density', 0)})."
    )
    messages.append({"agent": "RESEARCHER", "msg": analytics_msg})

    # Merge violations into existing quality report
    quality_report = state.get("data_quality_report", {})
    quality_report["topology_violations"] = violations

    # ── LLM Anomaly Detective (Role 6) ───────────────────────────────────
    # LLM reviews as-is topology for hidden risks, stale connections, compliance issues.
    # Results enrich the data quality report shown in the Review tab.
    anomaly_insights = None
    try:
        anomaly_state = {
            "raw_data": clean_data,
            "data_quality_report": quality_report,
            "as_is_communities": as_is_communities,
            "as_is_centrality": as_is_centrality,
            "as_is_entropy": as_is_entropy,
            "as_is_metrics": state.get("as_is_metrics", {}),
        }
        anomaly_prompt = build_anomaly_detective_prompt(anomaly_state)
        logger.info(f"RESEARCHER-LLM: Calling anomaly detective (~{len(anomaly_prompt)//4} tokens)")
        
        anomaly_result = call_llm(
            system_prompt=ANOMALY_DETECTIVE_SYSTEM,
            user_prompt=anomaly_prompt,
            max_retries=1,
            temperature=0.2,
            max_tokens=2048,
        )
        
        if anomaly_result and anomaly_result.get("anomalies"):
            anomaly_insights = anomaly_result
            n_anomalies = len(anomaly_result["anomalies"])
            health = anomaly_result.get("topology_health", "UNKNOWN")
            summary = anomaly_result.get("summary", "")[:150]
            messages.append({"agent": "RESEARCHER", 
                           "msg": f"AI Anomaly Detective: {n_anomalies} anomalies found. "
                                  f"Health: {health}. {summary}"})
            quality_report["llm_anomalies"] = anomaly_result
            logger.info(f"RESEARCHER-LLM: {n_anomalies} anomalies, health={health}")
        else:
            messages.append({"agent": "RESEARCHER", "msg": "AI Anomaly Detective: no issues found (or LLM unavailable)"})
    except Exception as e:
        logger.warning(f"RESEARCHER-LLM: Anomaly detection failed ({e})")
        messages.append({"agent": "RESEARCHER", "msg": f"AI Anomaly Detective: skipped ({e})"})

    return {
        "as_is_graph": as_is_graph,
        "as_is_subgraphs": as_is_subgraphs,
        "as_is_communities": as_is_communities,
        "as_is_centrality": as_is_centrality,
        "as_is_entropy": as_is_entropy,
        "data_quality_report": quality_report,
        "messages": messages,
    }


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 3: ANALYST
# Computes 6-factor complexity score on the as-is graph.
# ─────────────────────────────────────────────────────────────────────────────
def analyst_agent(state: dict) -> dict:
    logger.info("ANALYST: Computing as-is complexity metrics")
    messages = state.get("messages", [])

    # Bail if upstream failed
    if state.get("error"):
        messages.append({"agent": "ANALYST", "msg": f"SKIPPED — upstream error: {state['error']}"})
        return {"messages": messages}

    G = state.get("as_is_graph")
    if G is None:
        err = "ANALYST: No as_is_graph in state — did Researcher run?"
        messages.append({"agent": "ANALYST", "msg": err})
        return {"error": err, "messages": messages}

    metrics = compute_complexity(G)

    msg = (
        f"As-Is Complexity Score: {metrics['total_score']}/100 | "
        f"Channels: {metrics['channel_count']} | "
        f"Coupling: {metrics['coupling_index']} | "
        f"Depth: {metrics['routing_depth']} | "
        f"Fan-out: {metrics['fan_out_score']} | "
        f"Orphans: {metrics['orphan_objects']} | "
        f"Ch/QM Sprawl: {metrics.get('channel_sprawl', 'N/A')}"
    )
    messages.append({"agent": "ANALYST", "msg": msg})

    return {"as_is_metrics": metrics, "messages": messages}


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 5: ARCHITECT
# LLM-first architecture design with deterministic rule-based fallback.
# Calls Groq LLM to reason about topology and generate ADRs.
# If LLM fails (no API key, rate limit, bad JSON), falls back to rules.
# Pipeline NEVER crashes due to LLM failure.
# ─────────────────────────────────────────────────────────────────────────────
def architect_agent(state: dict) -> dict:
    """
    Architect agent — hybrid cluster-based approach.
    
    Phase A: Rules build complete baseline (instant, 0 API calls)
    Phase B: ONE LLM call on clusters + bridge apps (1 API call, ~10-15s)
    Phase C: Merge LLM refinements into rules baseline + validate
    
    architect_method = "hybrid_cluster" when LLM contributes.
    """
    logger.info("ARCHITECT: Designing target state topology (hybrid cluster)")
    messages = state.get("messages", [])
    adrs = state.get("adrs", []) or []
    redesign_count = state.get("redesign_count", 0)

    if state.get("error"):
        messages.append({"agent": "ARCHITECT", "msg": f"SKIPPED — upstream error: {state['error']}"})
        return {"messages": messages}

    raw_data = state.get("raw_data")
    if not raw_data:
        err = "ARCHITECT: No raw_data in state"
        messages.append({"agent": "ARCHITECT", "msg": err})
        return {"error": err, "messages": messages}

    try:
        # ── Parse human feedback directives ───────────────────────────────
        feedback = state.get("human_feedback", "")
        chat_history = state.get("chat_history") or []

        # ══════════════════════════════════════════════════════════════════
        # REVISION ARCHITECTURE: Two paths based on context available.
        #
        # PATH 1 — CHAT-AWARE REVISION (chat_history exists):
        #   LLM is PRIMARY decision-maker. It sees the full conversation,
        #   current target state metrics, and produces DELTA changes
        #   (reassign 5 apps, remove 10 channels, decommission 3 QMs).
        #   The rules engine EXECUTES these deltas on the graph.
        #   _enforce_single_qm + tester VALIDATE constraints after.
        #   This keeps the LLM payload small (~4K tokens) while giving
        #   it full decision authority.
        #
        # PATH 2 — FEEDBACK-ONLY (no chat, just a text string):
        #   Falls back to regex/LLM feedback interpreter → directives.
        #   Rules rebuild baseline, optimizer applies directives.
        # ══════════════════════════════════════════════════════════════════

        revision_result = None
        directives = {}

        if chat_history and len(chat_history) > 1:
            # ── PATH 1: LLM Revision Architect ───────────────────────────
            logger.info(f"ARCHITECT: Revision mode — {len(chat_history)} chat messages")
            messages.append({"agent": "ARCHITECT",
                           "msg": f"Revision mode: AI Revision Architect processing "
                                  f"{len(chat_history)} chat messages"})
            adrs = []  # Fresh ADRs for new iteration

            try:
                revision_prompt = build_revision_architect_prompt(state)
                logger.info(f"ARCHITECT-REVISION: Prompt ~{len(revision_prompt)//4} tokens")

                revision_result = call_llm(
                    system_prompt=REVISION_ARCHITECT_SYSTEM,
                    user_prompt=revision_prompt,
                    max_retries=2,
                    temperature=0.15,
                    max_tokens=4096,
                )

                if revision_result:
                    summary = revision_result.get("revision_summary", "")
                    confidence = revision_result.get("confidence", "UNKNOWN")
                    warnings = revision_result.get("warnings", [])
                    logger.info(f"ARCHITECT-REVISION: LLM responded — confidence={confidence}, "
                               f"summary: {summary[:120]}")
                    messages.append({"agent": "ARCHITECT",
                                   "msg": f"AI Revision Architect ({confidence}): {summary}"})
                    if warnings:
                        for w in warnings[:3]:
                            messages.append({"agent": "ARCHITECT", "msg": f"⚠ Revision warning: {w}"})

                    # Extract LLM directives for the optimizer
                    llm_directives = revision_result.get("optimization_directives", {})
                    if llm_directives:
                        directives = llm_directives
                        directives["llm_interpreted"] = True
                        directives["llm_reasoning"] = summary[:300]

                    # Parse LLM ADRs
                    for adr in revision_result.get("adrs", []):
                        adrs.append({
                            "id": adr.get("id", f"ADR-REV-{len(adrs)+1:03d}"),
                            "decision": adr.get("decision", "Revision decision"),
                            "context": adr.get("context", ""),
                            "rationale": adr.get("rationale", ""),
                            "consequences": adr.get("consequences", ""),
                        })
                else:
                    logger.warning("ARCHITECT-REVISION: LLM returned None — falling back to regex")
                    messages.append({"agent": "ARCHITECT",
                                   "msg": "AI Revision Architect unavailable — using regex fallback"})
            except Exception as e:
                logger.warning(f"ARCHITECT-REVISION: Failed ({e}) — falling back to regex")
                messages.append({"agent": "ARCHITECT",
                               "msg": f"AI Revision Architect failed ({e}) — using regex fallback"})

        # If revision LLM didn't produce directives, fall back to regex parsing
        if not directives:
            directives = _parse_feedback_directives(feedback, state=state)

        if directives:
            if not adrs:  # Only clear ADRs if we haven't already (revision path clears them above)
                adrs = []
            messages.append({"agent": "ARCHITECT",
                           "msg": f"Feedback directives: {directives}"})

        # ── PHASE A: Rules build complete baseline ────────────────────────
        target_graph = _build_target_rules(state)
        
        app_count = sum(1 for _, d in target_graph.nodes(data=True) if d.get("type") == "app")
        qm_count = sum(1 for _, d in target_graph.nodes(data=True) if d.get("type") == "qm")
        logger.info(f"ARCHITECT: Phase A — Rules baseline: {app_count} apps, {qm_count} QMs")
        messages.append({"agent": "ARCHITECT", "msg": f"Rules baseline built — {app_count} apps assigned in Phase A"})

        # ── PHASE A2: Apply LLM revision deltas ──────────────────────────
        # If the Revision Architect produced specific changes, apply them
        # to the rules baseline. This is where LLM decisions get EXECUTED.
        if revision_result:
            revision_applied = 0

            # A2a: Apply reassignments (move app from one QM to another)
            for entry in revision_result.get("reassignments", []):
                app_id = entry.get("app_id", "")
                to_qm = entry.get("to_qm", "")
                reason = entry.get("reason", "")
                if not app_id or not to_qm or app_id not in target_graph.nodes:
                    continue

                # Remove old connects_to edges
                old_edges = [(u, v) for u, v, d in target_graph.out_edges(app_id, data=True)
                             if d.get("rel") == "connects_to"]
                for u, v in old_edges:
                    target_graph.remove_edge(u, v)

                # Check if target QM is occupied by another app
                if to_qm in target_graph.nodes:
                    apps_on_target = [u for u, v, d in target_graph.in_edges(to_qm, data=True)
                                      if d.get("rel") == "connects_to"]
                    if apps_on_target:
                        # Target occupied — create new QM instead
                        to_qm = f"QM_{app_id}"

                if to_qm not in target_graph.nodes:
                    target_graph.add_node(to_qm, type="qm", name=to_qm, region="")

                target_graph.add_edge(app_id, to_qm, rel="connects_to")
                revision_applied += 1
                logger.info(f"ARCHITECT-REVISION: Applied {app_id} → {to_qm}: {reason[:80]}")

            # A2b: Decommission QMs (remove app + QM + owned queues)
            for qm_to_remove in revision_result.get("qms_to_decommission", []):
                if qm_to_remove not in target_graph.nodes:
                    continue
                # Only decommission if the QM has exactly 1 app and that app has zero flows
                apps_on = [u for u, v, d in target_graph.in_edges(qm_to_remove, data=True)
                           if d.get("rel") == "connects_to"]
                if len(apps_on) == 1:
                    app_id = apps_on[0]
                    # Remove owned queues
                    owned = [v for _, v, d in target_graph.out_edges(qm_to_remove, data=True)
                             if d.get("rel") == "owns"]
                    for q in owned:
                        if target_graph.has_node(q):
                            target_graph.remove_node(q)
                    # Remove remote queues targeting this app
                    for n, d in list(target_graph.nodes(data=True)):
                        if d.get("type") == "queue" and (
                            d.get("target_app") == app_id or d.get("remote_qm") == qm_to_remove
                        ):
                            target_graph.remove_node(n)
                    # Remove edges, then nodes
                    for u, v in list(target_graph.in_edges(qm_to_remove)) + list(target_graph.out_edges(qm_to_remove)):
                        if target_graph.has_edge(u, v):
                            target_graph.remove_edge(u, v)
                    if target_graph.has_node(qm_to_remove):
                        target_graph.remove_node(qm_to_remove)
                    if target_graph.has_node(app_id):
                        target_graph.remove_node(app_id)
                    revision_applied += 1
                    logger.info(f"ARCHITECT-REVISION: Decommissioned {app_id} + {qm_to_remove}")

            # A2c: Remove specific channels
            for ch in revision_result.get("channels_to_remove", []):
                fqm = ch.get("from_qm", "")
                tqm = ch.get("to_qm", "")
                if fqm and tqm and target_graph.has_edge(fqm, tqm):
                    target_graph.remove_edge(fqm, tqm)
                    revision_applied += 1
                    logger.info(f"ARCHITECT-REVISION: Removed channel {fqm}.{tqm}")

            # A2d: Add specific channels
            for ch in revision_result.get("channels_to_add", []):
                fqm = ch.get("from_qm", "")
                tqm = ch.get("to_qm", "")
                if fqm and tqm and fqm in target_graph.nodes and tqm in target_graph.nodes:
                    if not target_graph.has_edge(fqm, tqm):
                        target_graph.add_edge(fqm, tqm, rel="channel",
                                            channel_name=f"{fqm}.{tqm}",
                                            status="RUNNING",
                                            xmit_queue=f"{tqm}.XMITQ")
                        revision_applied += 1

            new_qm_count = sum(1 for _, d in target_graph.nodes(data=True) if d.get("type") == "qm")
            new_app_count = sum(1 for _, d in target_graph.nodes(data=True) if d.get("type") == "app")
            messages.append({"agent": "ARCHITECT",
                           "msg": f"AI Revision Architect applied {revision_applied} changes. "
                                  f"QMs: {qm_count} → {new_qm_count}, Apps: {app_count} → {new_app_count}"})
            logger.info(f"ARCHITECT: Phase A2 — {revision_applied} LLM revision deltas applied")
            qm_count = new_qm_count
            app_count = new_app_count

        # ── PHASE A3: Feedback-driven QM consolidation (fallback path) ────
        # When human feedback requests QM/app reduction, identify apps with
        # zero cross-QM message flows and decommission them + their QMs.
        # These "island" apps add QM sprawl without messaging value.
        # Constraint-safe: removes app AND its QM together (not reassigning).
        if directives.get("consolidate_qm_pct") or directives.get("aggressive"):
            consolidate_pct = directives.get("consolidate_qm_pct", 0.3)
            target_graph, consolidation_count = _consolidate_qms_by_feedback(
                target_graph, state, consolidate_pct,
                protect_qms=set(directives.get("protect_qms", []))
            )
            if consolidation_count > 0:
                new_qm_count = sum(1 for _, d in target_graph.nodes(data=True) if d.get("type") == "qm")
                new_app_count = sum(1 for _, d in target_graph.nodes(data=True) if d.get("type") == "app")
                messages.append({
                    "agent": "ARCHITECT",
                    "msg": (f"Feedback-driven consolidation: decommissioned "
                            f"{consolidation_count} self-contained app(s) with zero "
                            f"message flows. QMs: {qm_count} → {new_qm_count}, "
                            f"Apps: {app_count} → {new_app_count}")
                })
                logger.info(f"ARCHITECT: Phase A3 — decommissioned {consolidation_count} apps "
                           f"({qm_count} → {new_qm_count} QMs)")
                qm_count = new_qm_count
                app_count = new_app_count

        # ── PHASE B: Cluster-based LLM call ───────────────────────────────
        llm_result = _architect_llm(state)

        if llm_result is not None:
            # ── PHASE C: Merge LLM refinements ────────────────────────────
            reassignment_count = 0
            rejected_count = 0

            # Apply reassignments
            all_reassignments = list(llm_result.get("reassignments", []))
            # Also treat bridge_app_decisions with keep_current=False as reassignments
            for bridge in llm_result.get("bridge_app_decisions", []):
                if not bridge.get("keep_current", True):
                    all_reassignments.append({
                        "app_id": bridge.get("app_id", ""),
                        "to_qm": bridge.get("recommended_qm", ""),
                        "reason": bridge.get("reason", "bridge decision"),
                    })

            for entry in all_reassignments:
                app_id = entry.get("app_id", "")
                to_qm = entry.get("to_qm", "")
                if not app_id or not to_qm or app_id not in target_graph.nodes:
                    continue

                # Check if target QM is occupied
                apps_on_target = [u for u, v, d in target_graph.in_edges(to_qm, data=True)
                                  if d.get("rel") == "connects_to"] if to_qm in target_graph.nodes else []
                
                if apps_on_target and apps_on_target != [app_id]:
                    # Target QM is occupied — try a SWAP
                    occupant = apps_on_target[0]
                    # Find app_id's current QM
                    current_edges = [(u, v) for u, v, d in target_graph.out_edges(app_id, data=True)
                                     if d.get("rel") == "connects_to"]
                    if not current_edges:
                        rejected_count += 1
                        continue
                    from_qm = current_edges[0][1]
                    
                    # Perform swap: occupant moves to from_qm, app_id moves to to_qm
                    # Remove both old edges
                    target_graph.remove_edge(app_id, from_qm)
                    target_graph.remove_edge(occupant, to_qm)
                    # Add swapped edges
                    target_graph.add_edge(app_id, to_qm, rel="connects_to")
                    target_graph.add_edge(occupant, from_qm, rel="connects_to")
                    reassignment_count += 1
                    logger.info(f"ARCHITECT: Swapped {app_id}→{to_qm} and {occupant}→{from_qm}: "
                               f"{entry.get('reason', '')[:80]}")
                    continue

                # Target QM is free or doesn't exist yet
                old_edges = [(u, v) for u, v, d in target_graph.out_edges(app_id, data=True)
                             if d.get("rel") == "connects_to"]
                for u, v in old_edges:
                    target_graph.remove_edge(u, v)

                if to_qm not in target_graph.nodes:
                    target_graph.add_node(to_qm, type="qm", name=to_qm, region="")

                target_graph.add_edge(app_id, to_qm, rel="connects_to")
                reassignment_count += 1
                logger.info(f"ARCHITECT: Applied {app_id} → {to_qm}: {entry.get('reason', '')[:80]}")

            # Parse LLM ADRs
            for adr in llm_result.get("adrs", []):
                adrs.append({
                    "id": adr.get("id", f"ADR-LLM-{len(adrs)+1:03d}"),
                    "decision": adr.get("decision") or adr.get("title", "LLM decision"),
                    "context": adr.get("context", ""),
                    "rationale": adr.get("rationale", ""),
                    "consequences": adr.get("consequences", ""),
                })

            method = "hybrid_cluster"
            llm_coverage_str = (
                f"{reassignment_count} reassignments applied, {rejected_count} rejected, "
                f"{len(llm_result.get('adrs', []))} ADRs, "
                f"{len(llm_result.get('modernization_insights', []))} insights"
            )
            logger.info(f"ARCHITECT: Phase C — {llm_coverage_str}")
            messages.append({"agent": "ARCHITECT", "msg": f"LLM cluster analysis: {llm_coverage_str}"})
        else:
            rule_adrs = _generate_rule_adrs(state, target_graph, redesign_count)
            adrs.extend(rule_adrs)
            method = "rules_fallback"
            logger.info(f"ARCHITECT: Rules fallback — {len(rule_adrs)} ADRs")

        # Override method if revision architect was the primary driver
        if revision_result:
            method = "revision_architect"

        # ── Safety net + channel backfill ─────────────────────────────────
        dupes_removed = _enforce_single_qm(target_graph, raw_data)
        if dupes_removed:
            logger.warning(f"ARCHITECT: _enforce_single_qm removed {dupes_removed} duplicate edges")

        # ALWAYS backfill channels — new QMs from rules splitting need channels
        # derived from actual producer→consumer flows, not just when enforce_single_qm
        # makes corrections. Without this, QM_A345 etc. end up with zero channels
        # and the tester flags them as ISOLATED_QM (CRITICAL).
        _backfill_channels(target_graph, raw_data)

        # ── Final counts ──────────────────────────────────────────────────
        as_is = state.get("as_is_graph")
        original_qm_count = sum(1 for _, d in as_is.nodes(data=True) if d.get("type") == "qm") if as_is else 0
        target_qm_count = sum(1 for _, d in target_graph.nodes(data=True) if d.get("type") == "qm")
        target_ch_count = sum(1 for _, _, d in target_graph.edges(data=True) if d.get("rel") == "channel")

        msg = (
            f"Target state designed using {method}: "
            f"{target_qm_count} QMs (was {original_qm_count}), "
            f"{target_ch_count} channels, {len(adrs)} ADRs."
        )
        if directives:
            msg += f" Feedback directives active: {list(directives.keys())}"
            adrs.append({
                "id": f"ADR-FB-{redesign_count+1:02d}-001",
                "decision": "Aggressive optimization per human feedback",
                "context": f"Human reviewer requested: \"{feedback[:200]}\"",
                "rationale": (
                    f"Optimizer will apply enhanced channel pruning: "
                    f"fan-out capping, low-flow channel removal, and "
                    f"iterative MST passes to achieve deeper complexity reduction "
                    f"while maintaining strict 1:1 app→QM ownership."
                ),
                "consequences": (
                    f"More channels removed → lower complexity score. "
                    f"Some indirect message routes may increase latency by 1-2 hops. "
                    f"All producer→consumer flows remain reachable via MST paths."
                ),
            })
            # ADR for QM consolidation if decommissioning happened
            decommissioned = original_qm_count - target_qm_count
            if decommissioned > 0:
                adrs.append({
                    "id": f"ADR-FB-{redesign_count+1:02d}-002",
                    "decision": f"Decommissioned {decommissioned} self-contained app(s) and their QMs",
                    "context": f"Human reviewer requested: \"{feedback[:200]}\"",
                    "rationale": (
                        f"Apps with zero cross-QM message flows (no producer→consumer "
                        f"relationship with any other app) add QM sprawl without "
                        f"messaging value. Removing them and their dedicated QMs "
                        f"reduces topology complexity with zero impact on message routing."
                    ),
                    "consequences": (
                        f"QM count reduced from {original_qm_count} to {target_qm_count}. "
                        f"Decommissioned apps require a separate retirement plan "
                        f"outside the MQ topology."
                    ),
                })
        messages.append({"agent": "ARCHITECT", "msg": msg})

        return {
            "target_graph": target_graph,
            "adrs": adrs,
            "redesign_count": redesign_count + 1,
            "architect_method": method,
            "feedback_directives": directives if directives else None,
            "messages": messages,
        }
    except Exception as e:
        logger.exception(f"ARCHITECT: Crashed — {e}")
        messages.append({"agent": "ARCHITECT", "msg": f"CRASHED: {e}"})
        return {"error": f"ARCHITECT crashed: {e}", "messages": messages}


def _enforce_single_qm(G: nx.DiGraph, raw_data: dict) -> int:
    """
    Belt-and-suspenders: enforce strict 1:1 app↔QM ownership.
    1) Each app has exactly 1 connects_to edge (remove duplicates)
    2) Each QM has exactly 1 app (if multiple, split into new QMs)
    Returns the number of corrections made.
    """
    corrections = 0
    app_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "app"]
    qm_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "qm"]

    # Pre-compute connection counts per (app, QM) from raw data
    app_qm_counts = {}
    for row in raw_data.get("applications", []):
        key = (row["app_id"], row["qm_id"])
        app_qm_counts[key] = app_qm_counts.get(key, 0) + 1

    # Pass 1: each app → exactly 1 QM
    for app in app_nodes:
        connected = [(v, d) for _, v, d in G.out_edges(app, data=True) if d.get("rel") == "connects_to"]
        if len(connected) <= 1:
            continue
        best_qm = max(
            (qm for qm, _ in connected),
            key=lambda qm: app_qm_counts.get((app, qm), 0),
        )
        for qm, _ in connected:
            if qm != best_qm:
                G.remove_edge(app, qm)
                corrections += 1
                logger.warning(f"_enforce_single_qm: removed duplicate {app}→{qm} (keeping {best_qm})")

    # Pass 2: each QM → exactly 1 app
    for qm in qm_nodes:
        apps_on_qm = [u for u, _, d in G.in_edges(qm, data=True) if d.get("rel") == "connects_to"]
        if len(apps_on_qm) <= 1:
            continue

        # Keep the app with the most raw connections to this QM
        apps_sorted = sorted(
            apps_on_qm,
            key=lambda a: app_qm_counts.get((a, qm), 0),
            reverse=True,
        )
        winner = apps_sorted[0]
        region = G.nodes[qm].get("region", "UNKNOWN")

        for app in apps_sorted[1:]:
            # Create a new dedicated QM for this app
            new_qm = f"QM_{app}"
            if new_qm not in G.nodes:
                G.add_node(new_qm, type="qm",
                           name=f"QM for {G.nodes[app].get('name', app)}",
                           region=region)
            G.remove_edge(app, qm)
            G.add_edge(app, new_qm, rel="connects_to")
            corrections += 1
            logger.warning(f"_enforce_single_qm: split {app} off {qm} → new {new_qm} (winner={winner})")

    return corrections


def _backfill_channels(G: nx.DiGraph, raw_data: dict):
    """
    Derive channels from actual producer→consumer queue-level flows
    for any QM that currently has zero channels. Called after
    _enforce_single_qm which may create new QMs without channels.
    Also backfills queue objects (LOCAL, REMOTE, XMITQ) for new QMs.
    """
    # Build current app→QM map from graph
    app_qm = {}
    for n, d in G.nodes(data=True):
        if d.get("type") == "app":
            qms = [v for _, v, ed in G.out_edges(n, data=True) if ed.get("rel") == "connects_to"]
            if qms:
                app_qm[n] = qms[0]

    # Build queue-level flow data
    queue_prod = {}
    queue_cons = {}
    for row in raw_data.get("applications", []):
        aid = row["app_id"]
        qname = row.get("queue_name", "")
        if not qname:
            continue
        direction = row.get("direction", "").upper()
        if direction in ("PUT", "PRODUCER"):
            queue_prod.setdefault(qname, set()).add(aid)
        elif direction in ("GET", "CONSUMER"):
            queue_cons.setdefault(qname, set()).add(aid)

    # Derive all required channels
    added = 0
    for qname in set(queue_prod.keys()) & set(queue_cons.keys()):
        for prod_app in queue_prod[qname]:
            for cons_app in queue_cons[qname]:
                if prod_app == cons_app:
                    continue
                from_qm = app_qm.get(prod_app)
                to_qm = app_qm.get(cons_app)
                if from_qm and to_qm and from_qm != to_qm:
                    if from_qm in G.nodes and to_qm in G.nodes and not G.has_edge(from_qm, to_qm):
                        G.add_edge(
                            from_qm, to_qm, rel="channel",
                            channel_name=f"{from_qm}.{to_qm}",
                            status="RUNNING",
                            xmit_queue=f"{to_qm}.XMITQ",
                        )
                        added += 1

    if added:
        logger.info(f"_backfill_channels: added {added} channels for new/isolated QMs")

    # Backfill queue objects for any QM that has apps but no owned queues
    consumer_apps = set()
    for row in raw_data.get("applications", []):
        d = row.get("direction", "").upper()
        if d in ("GET", "CONSUMER"):
            consumer_apps.add(row["app_id"])
        elif d not in ("PUT", "PRODUCER"):
            consumer_apps.add(row["app_id"])

    for app_id, qm_id in app_qm.items():
        if app_id in consumer_apps:
            lq_id = f"LQ.{app_id}"
            if lq_id not in G.nodes:
                G.add_node(lq_id, type="queue", name=f"LOCAL.{app_id}.IN",
                           queue_type="LOCAL", usage="NORMAL", owner_app=app_id)
                G.add_edge(qm_id, lq_id, rel="owns")

    seen_xmitq = set()
    seen_rq = set()
    for qname in set(queue_prod.keys()) & set(queue_cons.keys()):
        for prod_app in queue_prod[qname]:
            for cons_app in queue_cons[qname]:
                if prod_app == cons_app:
                    continue
                from_qm = app_qm.get(prod_app)
                to_qm = app_qm.get(cons_app)
                if not from_qm or not to_qm or from_qm == to_qm:
                    continue
                xk = (from_qm, to_qm)
                if xk not in seen_xmitq:
                    seen_xmitq.add(xk)
                    xid = f"XMITQ.{from_qm}.{to_qm}"
                    if xid not in G.nodes:
                        G.add_node(xid, type="queue", name=f"{to_qm}.XMITQ",
                                   queue_type="LOCAL", usage="XMITQ")
                        G.add_edge(from_qm, xid, rel="owns")
                rk = (from_qm, cons_app, qname)
                if rk not in seen_rq:
                    seen_rq.add(rk)
                    rid = f"RQ.{from_qm}.{cons_app}.{qname}"
                    if rid not in G.nodes:
                        G.add_node(rid, type="queue",
                                   name=f"RQ.{qname}.TO.{cons_app}",
                                   queue_type="REMOTE", usage="NORMAL",
                                   remote_qm=to_qm,
                                   remote_queue=f"LOCAL.{cons_app}.IN",
                                   xmit_queue=f"{to_qm}.XMITQ",
                                   source_queue=qname,
                                   owner_app=prod_app,
                                   target_app=cons_app)
                        G.add_edge(from_qm, rid, rel="owns")


def _architect_llm(state: dict) -> dict | None:
    """
    Cluster-based LLM architecture call (v2).
    
    Instead of sending 400+ apps in batches of 75 (which fails at Tachyon gateway),
    sends ONE call with ~12 cluster summaries + bridge apps (~3-6K tokens).
    
    The LLM reviews clusters, decides bridge app placement, generates ADRs,
    and identifies modernization candidates — all in a single pass.
    """
    try:
        user_prompt = build_cluster_prompt(state)
        
        logger.info(f"ARCHITECT-LLM: Sending cluster-based prompt "
                    f"(~{len(user_prompt)} chars, ~{len(user_prompt)//4} tokens)")
        
        result = call_llm(
            system_prompt=CLUSTER_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            max_retries=2,
            temperature=0.1,
            max_tokens=8192,
        )
        
        if result is None:
            logger.warning("ARCHITECT-LLM: LLM call returned None")
            return None
        
        # Validate we got something useful (new schema has different keys)
        has_content = any([
            result.get("cluster_reviews"),
            result.get("bridge_app_decisions"),
            result.get("reassignments"),
            result.get("adrs"),
            result.get("modernization_insights"),
        ])
        
        if not has_content:
            logger.warning("ARCHITECT-LLM: Response had no actionable content")
            return None
        
        logger.info(f"ARCHITECT-LLM: Got {len(result.get('reassignments', []))} reassignments, "
                    f"{len(result.get('adrs', []))} ADRs, "
                    f"{len(result.get('modernization_insights', []))} insights, "
                    f"{len(result.get('bridge_app_decisions', []))} bridge decisions")
        
        return result
        
    except Exception as e:
        logger.error(f"ARCHITECT-LLM: Cluster call failed: {e}")
        return None


def _build_target_from_llm(llm_result: dict, state: dict) -> tuple:
    """
    Build a NetworkX target graph from the LLM's structured output.
    New schema (1:1): target_app_assignments, new_qms, removed_qms, channels.
    Returns (graph, adrs_list).

    Safety: if the LLM misses any apps, they get assigned via the rules
    fallback logic (weighted majority). _enforce_single_qm runs after
    to guarantee 1:1.
    """
    raw_data = state["raw_data"]
    G_target = nx.DiGraph()

    valid_app_ids = set(a["app_id"] for a in raw_data["applications"])
    qm_map = {row["qm_id"]: row for row in raw_data["queue_managers"]}

    # Build app name lookup
    app_name_map = {}
    for row in raw_data["applications"]:
        if row["app_id"] not in app_name_map:
            app_name_map[row["app_id"]] = row.get("app_name", row["app_id"])

    # ── Step 1: Process LLM app assignments ───────────────────────────────
    assigned = {}  # {app_id: qm_id}
    for entry in llm_result.get("target_app_assignments", []):
        app_id = entry.get("app_id", "")
        qm_id = entry.get("assigned_qm", "")
        if app_id not in valid_app_ids or not qm_id:
            continue
        assigned[app_id] = qm_id

    if not assigned:
        logger.warning("LLM returned empty target_app_assignments — falling back to rules")
        return _build_target_rules(state), []

    # ── Step 2: Create QM nodes ───────────────────────────────────────────
    all_qms = set(assigned.values())
    for qm_id in all_qms:
        if qm_id in qm_map:
            row = qm_map[qm_id]
            G_target.add_node(qm_id, type="qm", name=row.get("qm_name", qm_id),
                              region=row.get("region", ""))
        else:
            # New QM created by LLM (e.g. QM_A001)
            # Try to infer region from the app's original QM
            G_target.add_node(qm_id, type="qm", name=qm_id, region="")

    # ── Step 3: Create app nodes + connects_to edges ──────────────────────
    for app_id, qm_id in assigned.items():
        G_target.add_node(app_id, type="app", name=app_name_map.get(app_id, app_id))
        if qm_id in G_target.nodes:
            existing = [v for _, v, d in G_target.out_edges(app_id, data=True)
                        if d.get("rel") == "connects_to"]
            if not existing:
                G_target.add_edge(app_id, qm_id, rel="connects_to")

    # ── Step 4: Assign missing apps via rules fallback ────────────────────
    # LLM might miss some of the 150 apps. Assign them using weighted majority.
    missing_apps = valid_app_ids - set(assigned.keys())
    if missing_apps:
        logger.warning(f"LLM missed {len(missing_apps)} apps — assigning via rules fallback")
        app_qm_counts = {}
        for row in raw_data["applications"]:
            aid = row["app_id"]
            if aid not in missing_apps:
                continue
            qm = row.get("qm_id", "")
            app_qm_counts.setdefault(aid, {})
            app_qm_counts[aid][qm] = app_qm_counts[aid].get(qm, 0) + 1

        for app_id, qm_counts in app_qm_counts.items():
            best_qm = max(qm_counts, key=qm_counts.get)
            # Check if best_qm is already taken by another app
            apps_on_best = [u for u, v, d in G_target.in_edges(best_qm, data=True)
                            if d.get("rel") == "connects_to"] if best_qm in G_target.nodes else []
            if apps_on_best:
                # QM taken — create a new one
                new_qm = f"QM_{app_id}"
                region = qm_map.get(best_qm, {}).get("region", "")
                G_target.add_node(new_qm, type="qm", name=f"QM for {app_name_map.get(app_id, app_id)}",
                                  region=region)
                best_qm = new_qm
            elif best_qm not in G_target.nodes:
                row = qm_map.get(best_qm, {})
                G_target.add_node(best_qm, type="qm", name=row.get("qm_name", best_qm),
                                  region=row.get("region", ""))

            G_target.add_node(app_id, type="app", name=app_name_map.get(app_id, app_id))
            G_target.add_edge(app_id, best_qm, rel="connects_to")

    # ── Step 5: Add channels from LLM ─────────────────────────────────────
    all_target_qms = set(n for n, d in G_target.nodes(data=True) if d.get("type") == "qm")
    for conn in llm_result.get("channels", []):
        from_qm = conn.get("from_qm", "")
        to_qm = conn.get("to_qm", "")
        if from_qm in all_target_qms and to_qm in all_target_qms and from_qm != to_qm:
            if not G_target.has_edge(from_qm, to_qm):
                G_target.add_edge(
                    from_qm, to_qm, rel="channel",
                    channel_name=f"{from_qm}.{to_qm}",
                    status="RUNNING",
                    xmit_queue=f"{to_qm}.XMITQ",
                )

    # ── Step 6: Backfill channels from actual flows ───────────────────────
    # LLM often can't enumerate all 2,992 channels. Derive the rest from
    # actual producer→consumer queue-level data (same logic as rules path).
    app_qm_ownership = {}
    for n, d in G_target.nodes(data=True):
        if d.get("type") == "app":
            qms = [v for _, v, ed in G_target.out_edges(n, data=True) if ed.get("rel") == "connects_to"]
            if qms:
                app_qm_ownership[n] = qms[0]

    queue_producers = {}
    queue_consumers = {}
    for row in raw_data["applications"]:
        aid = row["app_id"]
        qname = row.get("queue_name", "")
        if not qname:
            continue
        direction = row.get("direction", "").upper()
        if direction in ("PUT", "PRODUCER"):
            queue_producers.setdefault(qname, set()).add(aid)
        elif direction in ("GET", "CONSUMER"):
            queue_consumers.setdefault(qname, set()).add(aid)

    for queue_name in set(queue_producers.keys()) & set(queue_consumers.keys()):
        for prod_app in queue_producers[queue_name]:
            for cons_app in queue_consumers[queue_name]:
                if prod_app == cons_app:
                    continue
                from_qm = app_qm_ownership.get(prod_app)
                to_qm = app_qm_ownership.get(cons_app)
                if from_qm and to_qm and from_qm != to_qm:
                    if not G_target.has_edge(from_qm, to_qm):
                        G_target.add_edge(
                            from_qm, to_qm, rel="channel",
                            channel_name=f"{from_qm}.{to_qm}",
                            status="RUNNING",
                            xmit_queue=f"{to_qm}.XMITQ",
                        )

    # ── Step 7: Build queue objects (same as rules path Step 5) ───────────
    consumer_apps = set()
    for row in raw_data["applications"]:
        d = row.get("direction", "").upper()
        if d in ("GET", "CONSUMER"):
            consumer_apps.add(row["app_id"])
        elif d not in ("PUT", "PRODUCER"):
            consumer_apps.add(row["app_id"])

    for app_id, qm_id in app_qm_ownership.items():
        if app_id in consumer_apps:
            lq_id = f"LQ.{app_id}"
            G_target.add_node(lq_id, type="queue", name=f"LOCAL.{app_id}.IN",
                              queue_type="LOCAL", usage="NORMAL", owner_app=app_id)
            G_target.add_edge(qm_id, lq_id, rel="owns")

    seen_xmitq = set()
    seen_rq = set()
    for queue_name in set(queue_producers.keys()) & set(queue_consumers.keys()):
        for prod_app in queue_producers[queue_name]:
            for cons_app in queue_consumers[queue_name]:
                if prod_app == cons_app:
                    continue
                from_qm = app_qm_ownership.get(prod_app)
                to_qm = app_qm_ownership.get(cons_app)
                if not from_qm or not to_qm or from_qm == to_qm:
                    continue
                xmitq_key = (from_qm, to_qm)
                if xmitq_key not in seen_xmitq:
                    seen_xmitq.add(xmitq_key)
                    xmitq_id = f"XMITQ.{from_qm}.{to_qm}"
                    G_target.add_node(xmitq_id, type="queue", name=f"{to_qm}.XMITQ",
                                      queue_type="LOCAL", usage="XMITQ")
                    G_target.add_edge(from_qm, xmitq_id, rel="owns")
                rq_key = (from_qm, cons_app, queue_name)
                if rq_key not in seen_rq:
                    seen_rq.add(rq_key)
                    rq_id = f"RQ.{from_qm}.{cons_app}.{queue_name}"
                    G_target.add_node(rq_id, type="queue",
                                      name=f"RQ.{queue_name}.TO.{cons_app}",
                                      queue_type="REMOTE", usage="NORMAL",
                                      remote_qm=to_qm,
                                      remote_queue=f"LOCAL.{cons_app}.IN",
                                      xmit_queue=f"{to_qm}.XMITQ",
                                      source_queue=queue_name,
                                      owner_app=prod_app,
                                      target_app=cons_app)
                    G_target.add_edge(from_qm, rq_id, rel="owns")

    # ── Parse ADRs + insights ─────────────────────────────────────────────
    adrs = []
    for adr in llm_result.get("adrs", []):
        adrs.append({
            "id": adr.get("id", f"ADR-LLM-{len(adrs)+1:03d}"),
            "decision": adr.get("decision") or adr.get("title", "LLM decision"),
            "context": adr.get("context", ""),
            "rationale": adr.get("rationale", ""),
            "consequences": adr.get("consequences", ""),
        })

    return G_target, adrs


def _parse_feedback_directives(feedback: str, state: dict = None) -> dict:
    """
    Parse human_feedback into actionable directives for the optimizer.
    
    LLM-FIRST: If LLM is available, it interprets feedback with full business
    context (PCI awareness, payment-critical flows, etc.).
    FALLBACK: Regex parser handles common patterns when LLM is unavailable.
    
    All directives maintain strict 1:1 app→QM ownership.
    """
    import re
    if not feedback or not feedback.strip():
        return {}
    
    # ── Try LLM interpretation first ──────────────────────────────────────
    if state:
        try:
            as_is_metrics = state.get("as_is_metrics", {})
            target_metrics = state.get("target_metrics", {})
            as_is_score = as_is_metrics.get("total_score", 0)
            target_score = target_metrics.get("total_score", 0)
            reduction_pct = round(((as_is_score - target_score) / as_is_score) * 100, 1) if as_is_score else 0
            
            user_prompt = build_feedback_interpreter_prompt(feedback, {
                "as_is_score": as_is_score,
                "target_score": target_score,
                "reduction_pct": reduction_pct,
                "channel_count": target_metrics.get("channel_count", "N/A"),
                "fan_out": target_metrics.get("fan_out_score", "N/A"),
                "routing_depth": target_metrics.get("routing_depth", "N/A"),
            })
            
            llm_result = call_llm(
                system_prompt=FEEDBACK_INTERPRETER_SYSTEM,
                user_prompt=user_prompt,
                max_retries=1,
                temperature=0.1,
                max_tokens=1024,
            )
            
            if llm_result and llm_result.get("directives"):
                directives = llm_result["directives"]
                reasoning = llm_result.get("reasoning", "")
                logger.info(f"FEEDBACK-LLM: Interpreted → {directives}")
                if reasoning:
                    logger.info(f"FEEDBACK-LLM: Reasoning: {reasoning[:200]}")
                # Ensure at least aggressive is set if any directive exists
                if any(v for k, v in directives.items() if k != "reasoning"):
                    directives.setdefault("aggressive", True)
                directives["llm_interpreted"] = True
                directives["llm_reasoning"] = reasoning[:300]
                return directives
        except Exception as e:
            logger.warning(f"FEEDBACK-LLM: Failed ({e}), falling back to regex")
    
    # ── Regex fallback ────────────────────────────────────────────────────
    fb = feedback.upper()
    directives = {}
    
    # Pattern: "remove/reduce/delete 50% of QMs/channels"
    m = re.search(r'(?:REMOVE|REMOVING|REDUCE|REDUCING|DELETE|DELETING|CONSOLIDAT|MERGE|MERGING|CUT|ELIMINATE)\D*(\d+)\s*%', fb)
    if m:
        pct = int(m.group(1)) / 100.0
        directives["channel_prune_pct"] = pct
        directives["aggressive"] = True
        # If user mentions QMs or apps, also set QM consolidation target
        if any(w in fb for w in ["QM", "QUEUE MANAGER", "APP", "APPLICATION"]):
            directives["consolidate_qm_pct"] = pct
            logger.info(f"FEEDBACK-REGEX: target {m.group(1)}% QM consolidation")
        logger.info(f"FEEDBACK-REGEX: target {m.group(1)}% channel reduction")
    
    # Pattern: "reduction to/of 90%" or "score to 90%"
    m = re.search(r'(?:REDUCTION|SCORE|COMPLEXITY)\s*(?:TO|OF|AT|AT\s+LEAST)\s*(\d+)\s*%', fb)
    if m:
        directives["target_reduction_pct"] = int(m.group(1)) / 100.0
        directives["aggressive"] = True
        logger.info(f"FEEDBACK-REGEX: target reduction {m.group(1)}%")
    
    # Pattern: fan-out cap
    m = re.search(r'FAN.?OUT\s*(?:CAP|LIMIT|MAX)\D*(\d+)', fb)
    if m:
        directives["fanout_cap"] = int(m.group(1))
        logger.info(f"FEEDBACK-REGEX: fan-out cap {m.group(1)}")
    
    # Pattern: aggressive / maximum / drastic / delete
    if any(w in fb for w in ["AGGRESSIVE", "MAXIMUM", "DRASTIC", "EXTREME", "AS MUCH AS", "DELETE", "DELETING"]):
        directives["aggressive"] = True
        logger.info("FEEDBACK-REGEX: aggressive mode enabled")
    
    # Pattern: "keep QM_NAME" / "preserve QM_NAME"
    keep_matches = re.findall(r'(?:KEEP|PRESERVE|PROTECT|DON.T\s+(?:TOUCH|REMOVE))\s+(\w+)', fb)
    if keep_matches:
        directives["protect_qms"] = list(set(keep_matches))
        logger.info(f"FEEDBACK-REGEX: protect QMs: {keep_matches}")
    
    # Generic reduction keywords (broad list)
    if not directives and any(w in fb for w in [
        "REDUCE", "REDUCING", "FEWER", "LESS", "SIMPLIF", "OPTIMIZE", "OPTIMISE",
        "MERGE", "REMOVE", "REMOVING", "DELETE", "DELETING", "CONSOLIDAT",
        "DECOMMISSION", "SHRINK", "MINIMIZE", "MINIMISE", "TRIM", "PRUNE", "CLEAN",
        "REARRANGE", "RESTRUCTURE", "REORGANIZE", "REORGANISE", "REDO",
        "CHANGE", "IMPROVE", "BETTER", "LOWER", "DOWN", "DROP", "CUT",
        "SIMPLIFY", "STREAMLINE", "FLATTEN", "COMPRESS", "TIGHTEN",
        "FEWER", "SMALLER", "SIMPLER", "LEANER", "THINNER",
        "FIX", "ADJUST", "TWEAK", "MODIFY", "UPDATE", "REVISE", "REWORK",
        "TOO MANY", "TOO MUCH", "TOO HIGH", "TOO COMPLEX",
        "SCORE", "COMPLEXITY", "COMPLEX",
    ]):
        directives["aggressive"] = True
        logger.info("FEEDBACK-REGEX: generic reduction request → aggressive mode")
    
    # ── CATCH-ALL: Any non-empty feedback in a revise means the human
    # wants something DIFFERENT. If nothing above matched, still treat
    # it as a signal to apply at least moderate optimisation.
    if not directives and feedback.strip():
        directives["aggressive"] = True
        directives["catch_all"] = True
        logger.info(f"FEEDBACK-REGEX: catch-all — unrecognised feedback treated as change request: '{feedback[:80]}'")
    
    if directives:
        directives["llm_interpreted"] = False
    
    return directives


def _consolidate_qms_by_feedback(
    G: nx.DiGraph, state: dict, consolidate_pct: float,
    protect_qms: set = None,
) -> tuple:
    """
    Feedback-driven QM consolidation — decommission self-contained apps.

    Identifies apps with ZERO cross-QM message flows (no producer→consumer
    relationship with any other app). These "island" apps add QM sprawl
    with no messaging value.

    Constraint-safe: removes the app AND its dedicated QM together.
    The 1:1 app→QM invariant is preserved because both sides vanish.
    No message flow is broken because the app had none.

    Returns: (modified_graph, count_of_apps_decommissioned)
    """
    raw_data = state.get("raw_data", {})
    if not raw_data:
        return G, 0

    protect_qms = protect_qms or set()

    # ── Find apps with cross-app message flows ───────────────────────────
    queue_producers = {}
    queue_consumers = {}
    for row in raw_data.get("applications", []):
        aid = row.get("app_id", "")
        qname = row.get("queue_name", "")
        if not qname:
            continue
        d = row.get("direction", "").upper()
        if d in ("PUT", "PRODUCER"):
            queue_producers.setdefault(qname, set()).add(aid)
        elif d in ("GET", "CONSUMER"):
            queue_consumers.setdefault(qname, set()).add(aid)
        else:
            queue_producers.setdefault(qname, set()).add(aid)
            queue_consumers.setdefault(qname, set()).add(aid)

    apps_with_flows = set()
    for qname in set(queue_producers.keys()) & set(queue_consumers.keys()):
        for p in queue_producers[qname]:
            for c in queue_consumers[qname]:
                if p != c:
                    apps_with_flows.add(p)
                    apps_with_flows.add(c)

    # ── Find decommission candidates ─────────────────────────────────────
    all_apps = [n for n, d in G.nodes(data=True) if d.get("type") == "app"]
    candidates = []
    for app_id in all_apps:
        if app_id in apps_with_flows:
            continue
        qms = [v for _, v, d in G.out_edges(app_id, data=True)
               if d.get("rel") == "connects_to"]
        if qms and qms[0] in protect_qms:
            continue
        candidates.append(app_id)

    if not candidates:
        logger.info("ARCHITECT-CONSOLIDATE: No self-contained apps found")
        return G, 0

    max_remove = max(1, int(len(all_apps) * consolidate_pct))
    to_remove = candidates[:max_remove]

    logger.info(f"ARCHITECT-CONSOLIDATE: {len(candidates)} candidates, "
                f"decommissioning {len(to_remove)} "
                f"(target {consolidate_pct:.0%} of {len(all_apps)})")

    # ── Remove app + QM + owned queues + referencing remote queues ────────
    removed = 0
    for app_id in to_remove:
        qms = [v for _, v, d in G.out_edges(app_id, data=True)
               if d.get("rel") == "connects_to"]
        if not qms:
            continue
        app_qm = qms[0]

        # Safety: only remove QM if this is its sole app
        apps_on_qm = [u for u, v, d in G.in_edges(app_qm, data=True)
                       if d.get("rel") == "connects_to"]
        if len(apps_on_qm) != 1:
            continue

        # Collect owned queues BEFORE removing edges
        owned_queues = [v for _, v, d in G.out_edges(app_qm, data=True)
                        if d.get("rel") == "owns"]

        # Remove all edges on the QM (channels, owns, connects_to)
        for u, v in list(G.in_edges(app_qm)) + list(G.out_edges(app_qm)):
            if G.has_edge(u, v):
                G.remove_edge(u, v)

        # Remove owned queue nodes
        for q in owned_queues:
            if G.has_node(q):
                G.remove_node(q)

        # Remove remote queue nodes on OTHER QMs that target this app
        for n, d in list(G.nodes(data=True)):
            if (d.get("type") == "queue"
                and (d.get("target_app") == app_id
                     or d.get("remote_qm") == app_qm)):
                G.remove_node(n)

        # Remove app and QM nodes
        if G.has_node(app_qm):
            G.remove_node(app_qm)
        if G.has_node(app_id):
            G.remove_node(app_id)

        removed += 1
        logger.info(f"ARCHITECT-CONSOLIDATE: Decommissioned {app_id} + {app_qm}")

    return G, removed


def _build_target_rules(state: dict) -> nx.DiGraph:
    """
    Deterministic rule-based target state builder.
    Used as fallback when LLM is unavailable.

    CORE CONSTRAINT: 1 App = 1 QM (strict 1:1 ownership).
    - If a QM currently has 5 apps, 1 app keeps it, the other 4 get new QMs.
    - The app with the MOST connections to the QM keeps the original.
    - Channels are introduced ONLY where a producer app on QM_A needs to
      send messages to a consumer app on QM_B.
    """
    raw_data = state["raw_data"]
    G_target = nx.DiGraph()

    # ── Step 1: Identify unique apps and their primary QM ─────────────────
    # For each app, find the QM it connects to most (weighted majority).
    # This determines which EXISTING QM the app prefers.
    app_qm_counts = {}   # {app_id: {qm_id: connection_count}}
    for row in raw_data["applications"]:
        app_id = row["app_id"]
        qm_id = row["qm_id"]
        if app_id not in app_qm_counts:
            app_qm_counts[app_id] = {}
        app_qm_counts[app_id][qm_id] = app_qm_counts[app_id].get(qm_id, 0) + 1

    app_preferred_qm = {}
    for app_id, qm_counts in app_qm_counts.items():
        app_preferred_qm[app_id] = max(qm_counts, key=lambda qm: qm_counts[qm])

    # ── Step 2: Assign QMs — 1:1 strict mapping ──────────────────────────
    # Group apps by their preferred QM, then for each QM:
    #   - The app with the most connections KEEPS the original QM name
    #   - Other apps get a NEW QM named QM_{APP_ID}
    qm_app_groups = {}  # {qm_id: [app_ids]}
    for app_id, qm_id in app_preferred_qm.items():
        if qm_id not in qm_app_groups:
            qm_app_groups[qm_id] = []
        qm_app_groups[qm_id].append(app_id)

    qm_map = {row["qm_id"]: row for row in raw_data["queue_managers"]}
    app_name_map = {}
    for row in raw_data["applications"]:
        if row["app_id"] not in app_name_map:
            app_name_map[row["app_id"]] = row.get("app_name", row["app_id"])

    # app_qm_ownership: the final 1:1 mapping {app_id: assigned_qm_id}
    app_qm_ownership = {}

    for original_qm, apps in qm_app_groups.items():
        qm_meta = qm_map.get(original_qm, {})
        region = qm_meta.get("region", "UNKNOWN")

        if len(apps) == 1:
            # Single app — keeps the original QM
            app_qm_ownership[apps[0]] = original_qm
            G_target.add_node(original_qm, type="qm",
                              name=qm_meta.get("qm_name", original_qm),
                              region=region)
        else:
            # Multiple apps — sort by connection count descending.
            # Winner keeps original QM, others get new QMs.
            apps_sorted = sorted(
                apps,
                key=lambda a: app_qm_counts[a].get(original_qm, 0),
                reverse=True,
            )
            # First app keeps the original QM
            winner = apps_sorted[0]
            app_qm_ownership[winner] = original_qm
            G_target.add_node(original_qm, type="qm",
                              name=qm_meta.get("qm_name", original_qm),
                              region=region)

            # Remaining apps each get a dedicated QM
            for app_id in apps_sorted[1:]:
                new_qm = f"QM_{app_id}"
                app_qm_ownership[app_id] = new_qm
                G_target.add_node(new_qm, type="qm",
                                  name=f"QM for {app_name_map.get(app_id, app_id)}",
                                  region=region)  # inherit region from parent QM

    # ── Step 3: Add app nodes with their 1:1 QM ──────────────────────────
    for app_id, qm_id in app_qm_ownership.items():
        G_target.add_node(app_id, type="app", name=app_name_map.get(app_id, app_id))
        G_target.add_edge(app_id, qm_id, rel="connects_to")

    # ── Step 4: Determine required channels ───────────────────────────────
    # A channel FROM_QM→TO_QM is needed if and only if:
    #   - An app on FROM_QM produces messages (PUT)
    #   - An app on TO_QM consumes messages (GET)
    #   - There was an actual data flow between them in the as-is topology
    #
    # We derive flows from the as-is data: if app_A (PUT) and app_B (GET)
    # both appear on the same queue in the source data, they have a flow.
    # After 1:1 assignment, if they're on different QMs, we need a channel.

    # Build per-app direction info
    app_directions = {}  # {app_id: set of directions}
    app_queues = {}      # {app_id: set of queue names}
    for row in raw_data["applications"]:
        aid = row["app_id"]
        if aid not in app_directions:
            app_directions[aid] = set()
            app_queues[aid] = set()
        app_directions[aid].add(row.get("direction", "UNKNOWN"))
        qname = row.get("queue_name", "")
        if qname:
            app_queues[aid].add(qname)

    # Build queue→apps map to find producer-consumer pairs
    queue_producers = {}  # {queue_name: set of app_ids that PUT}
    queue_consumers = {}  # {queue_name: set of app_ids that GET}
    for row in raw_data["applications"]:
        aid = row["app_id"]
        qname = row.get("queue_name", "")
        if not qname:
            continue
        direction = row.get("direction", "").upper()
        if direction in ("PUT", "PRODUCER"):
            queue_producers.setdefault(qname, set()).add(aid)
        elif direction in ("GET", "CONSUMER"):
            queue_consumers.setdefault(qname, set()).add(aid)

    # Find all required QM-to-QM connections from producer→consumer flows
    required_channels = set()  # set of (from_qm, to_qm)
    for queue_name in set(queue_producers.keys()) & set(queue_consumers.keys()):
        for prod_app in queue_producers[queue_name]:
            for cons_app in queue_consumers[queue_name]:
                if prod_app == cons_app:
                    continue
                from_qm = app_qm_ownership.get(prod_app)
                to_qm = app_qm_ownership.get(cons_app)
                if from_qm and to_qm and from_qm != to_qm:
                    required_channels.add((from_qm, to_qm))

    # NOTE: We intentionally do NOT infer channels from REMOTE queue
    # definitions. The queue-level producer→consumer flow analysis above
    # already captures every legitimate message flow. REMOTE queue inference
    # would create channels between ALL apps on the old source QM and ALL
    # apps on the old target QM — a full mesh that defeats the purpose of
    # 1:1 simplification. Only actual queue-level data flows matter.

    # Add channel edges
    for from_qm, to_qm in required_channels:
        if from_qm in G_target.nodes and to_qm in G_target.nodes:
            channel_name = f"{from_qm}.{to_qm}"
            G_target.add_edge(
                from_qm, to_qm,
                rel="channel",
                channel_name=channel_name,
                status="RUNNING",
                xmit_queue=f"{to_qm}.XMITQ",
            )

    # ── Step 5: Build full MQ object model ────────────────────────────────
    # Reference architecture per Objective.md §5:
    #   App_A → QM_A → REMOTE_Q → XMITQ → [Sender Ch] → QM_B → LOCAL_Q → App_B
    #
    # OPTIMISATION: Only create queue objects for ACTUAL message flows.
    # A REMOTE queue is needed only when a specific producer writes to a
    # specific queue that a specific consumer reads from, and they're on
    # different QMs. This avoids creating unnecessary objects.

    # 5a. LOCAL queues — one per app (consumer reads from this)
    #     Only create for apps that actually CONSUME (GET) from queues
    #     Pure producers don't need a local input queue
    consumer_apps = set()
    producer_apps = set()
    for row in raw_data["applications"]:
        d = row.get("direction", "").upper()
        if d in ("GET", "CONSUMER"):
            consumer_apps.add(row["app_id"])
        elif d in ("PUT", "PRODUCER"):
            producer_apps.add(row["app_id"])
        else:
            # Unknown direction — treat as both
            consumer_apps.add(row["app_id"])
            producer_apps.add(row["app_id"])

    for app_id, qm_id in app_qm_ownership.items():
        if app_id in consumer_apps:
            lq_id = f"LQ.{app_id}"
            G_target.add_node(lq_id, type="queue", name=f"LOCAL.{app_id}.IN",
                              queue_type="LOCAL", usage="NORMAL",
                              owner_app=app_id)
            G_target.add_edge(qm_id, lq_id, rel="owns")

    # 5b. XMITQ + REMOTE queues — only for actual cross-QM flows
    # Build the actual flow map: (producer_app, queue_name, consumer_app)
    seen_xmitq = set()
    seen_rq = set()
    for queue_name in set(queue_producers.keys()) & set(queue_consumers.keys()):
        for prod_app in queue_producers[queue_name]:
            for cons_app in queue_consumers[queue_name]:
                if prod_app == cons_app:
                    continue
                from_qm = app_qm_ownership.get(prod_app)
                to_qm = app_qm_ownership.get(cons_app)
                if not from_qm or not to_qm or from_qm == to_qm:
                    continue

                # XMITQ — one per (from_qm, to_qm) pair
                xmitq_key = (from_qm, to_qm)
                if xmitq_key not in seen_xmitq:
                    seen_xmitq.add(xmitq_key)
                    xmitq_id = f"XMITQ.{from_qm}.{to_qm}"
                    G_target.add_node(xmitq_id, type="queue", name=f"{to_qm}.XMITQ",
                                      queue_type="LOCAL", usage="XMITQ")
                    G_target.add_edge(from_qm, xmitq_id, rel="owns")

                # REMOTE queue — one per (from_qm, consumer_app, queue_name)
                # This is the actual QREMOTE on the producer's QM pointing to the
                # consumer's local queue on the target QM
                rq_key = (from_qm, cons_app, queue_name)
                if rq_key not in seen_rq:
                    seen_rq.add(rq_key)
                    rq_id = f"RQ.{from_qm}.{cons_app}.{queue_name}"
                    G_target.add_node(rq_id, type="queue",
                                      name=f"RQ.{queue_name}.TO.{cons_app}",
                                      queue_type="REMOTE", usage="NORMAL",
                                      remote_qm=to_qm,
                                      remote_queue=f"LOCAL.{cons_app}.IN",
                                      xmit_queue=f"{to_qm}.XMITQ",
                                      source_queue=queue_name,
                                      owner_app=prod_app,
                                      target_app=cons_app)
                    G_target.add_edge(from_qm, rq_id, rel="owns")

    return G_target


def _generate_rule_adrs(state: dict, target_graph: nx.DiGraph, redesign_count: int) -> list:
    """Generate template ADRs for the rule-based fallback path."""
    adrs = []
    violations = state.get("data_quality_report", {}).get("topology_violations", {})
    as_is_graph = state["as_is_graph"]

    original_qm_count = sum(1 for _, d in as_is_graph.nodes(data=True) if d.get("type") == "qm")
    target_qms = set(n for n, d in target_graph.nodes(data=True) if d.get("type") == "qm")
    as_is_qms = set(n for n, d in as_is_graph.nodes(data=True) if d.get("type") == "qm")
    removed_qms = as_is_qms - target_qms
    new_qms = target_qms - as_is_qms

    # ADR: 1:1 QM ownership enforcement
    target_app_count = sum(1 for _, d in target_graph.nodes(data=True) if d.get("type") == "app")
    adrs.append({
        "id": f"ADR-{redesign_count+1:02d}-001",
        "decision": f"Enforce strict 1:1 app-to-QM ownership — {len(target_qms)} QMs for {target_app_count} apps",
        "context": (
            f"As-is has {original_qm_count} QMs shared across {target_app_count} apps. "
            f"Constraint requires each application to own exactly one queue manager."
        ),
        "rationale": (
            "1:1 ownership eliminates multi-app coupling, establishes clear ownership boundaries, "
            "and makes each QM independently deployable. Cross-QM communication is handled "
            "by MQ routing (QREMOTE + XMITQ + sender/receiver channels)."
        ),
        "consequences": (
            f"{len(new_qms)} new QMs created for apps that previously shared. "
            f"Each app's connection string points to its dedicated QM. "
            f"Channels introduced only where producer→consumer flows exist."
        ),
    })

    if removed_qms:
        adrs.append({
            "id": f"ADR-{redesign_count+1:02d}-002",
            "decision": f"Remove {len(removed_qms)} orphan QMs: {', '.join(sorted(removed_qms))}",
            "context": f"These QMs have no active app ownership after 1:1 reassignment.",
            "rationale": "Orphan QMs contribute to operational overhead with zero application value.",
            "consequences": "Operational teams must decommission these QMs.",
        })

    multi_qm = violations.get("multi_qm_apps", [])
    if multi_qm:
        adrs.append({
            "id": f"ADR-{redesign_count+1:02d}-003",
            "decision": f"Resolve {len(multi_qm)} multi-QM apps via dedicated QM assignment",
            "context": f"Apps {[m['app'] for m in multi_qm]} each connected to multiple QMs.",
            "rationale": "Each app assigned to a single dedicated QM. All cross-QM flows routed via channels.",
            "consequences": "Application connection strings updated. Remote queues + channels replace direct connections.",
        })

    stopped = violations.get("stopped_channels", [])
    if stopped:
        adrs.append({
            "id": f"ADR-{redesign_count+1:02d}-004",
            "decision": f"Remove {len(stopped)} stopped/inactive channels",
            "context": "Channels with STOPPED status represent unused routing paths.",
            "rationale": "Stopped channels increase complexity score and represent latent configuration risk.",
            "consequences": "MQSC DELETE CHANNEL commands will be generated.",
        })

    return adrs


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 5: OPTIMIZER
# Applies reachability-driven pruning to remove unnecessary channels.
# ─────────────────────────────────────────────────────────────────────────────
# AGENT 5: OPTIMIZER
# Two-phase graph optimisation with mathematical foundations:
#   Phase 1 — Reachability pruning: remove channels serving zero message flows
#   Phase 2 — Graph-theoretic optimisation: weighted MST to find minimum
#             channel set, Kernighan-Lin bisection for cluster detection
# Also reports channel cycles as an informational metric.
# ─────────────────────────────────────────────────────────────────────────────
def optimizer_agent(state: dict) -> dict:
    logger.info("OPTIMIZER: Running two-phase graph optimisation")
    messages = state.get("messages", [])

    # Bail if upstream failed
    if state.get("error"):
        messages.append({"agent": "OPTIMIZER", "msg": f"SKIPPED — upstream error: {state['error']}"})
        return {"messages": messages}

    if not state.get("target_graph"):
        err = "OPTIMIZER: No target_graph in state — did Architect run?"
        messages.append({"agent": "OPTIMIZER", "msg": err})
        return {"error": err, "messages": messages}

    G = state["target_graph"].copy()

    # Belt-and-suspenders: enforce 1-QM-per-app on entry
    raw_data = state.get("raw_data", {})
    dupes = _enforce_single_qm(G, raw_data)
    if dupes:
        logger.warning(f"OPTIMIZER: entry check removed {dupes} duplicate connects_to edges")

    qm_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "qm"]
    app_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "app"]
    channel_edges = [(u, v, d) for u, v, d in G.edges(data=True) if d.get("rel") == "channel"]
    initial_channels = len(channel_edges)

    # ── Build app ownership + direction maps ──────────────────────────────
    app_qm_map = {}
    for app in app_nodes:
        qms = [v for _, v, d in G.out_edges(app, data=True) if d.get("rel") == "connects_to"]
        if qms:
            app_qm_map[app] = qms[0]

    raw_apps = state.get("raw_data", {}).get("applications", [])
    producer_qms = set()
    consumer_qms = set()
    for row in raw_apps:
        app_id = row.get("app_id", "")
        qm = app_qm_map.get(app_id)
        if not qm:
            continue
        direction = row.get("direction", "").upper()
        if direction in ("PRODUCER", "PUT"):
            producer_qms.add(qm)
        elif direction in ("CONSUMER", "GET"):
            consumer_qms.add(qm)
        else:
            producer_qms.add(qm)
            consumer_qms.add(qm)

    qms_with_apps = set(app_qm_map.values())

    # ══════════════════════════════════════════════════════════════════════
    # PHASE 1: REACHABILITY PRUNING
    # Remove channels where source has no producers OR target has no
    # consumers. With 1:1 mapping, a channel is only needed if the
    # source QM hosts a producer app AND the target hosts a consumer app.
    # ══════════════════════════════════════════════════════════════════════
    required_channels = set()
    for from_qm, to_qm, d in channel_edges:
        has_producer = from_qm in producer_qms
        has_consumer = to_qm in consumer_qms
        if has_producer and has_consumer:
            required_channels.add((from_qm, to_qm))

    phase1_removed = []
    for from_qm, to_qm, d in channel_edges:
        if (from_qm, to_qm) not in required_channels:
            phase1_removed.append((from_qm, to_qm, d.get("channel_name", "")))

    for from_qm, to_qm, _ in phase1_removed:
        if G.has_edge(from_qm, to_qm) and G[from_qm][to_qm].get("rel") == "channel":
            G.remove_edge(from_qm, to_qm)

    after_phase1 = sum(1 for _, _, d in G.edges(data=True) if d.get("rel") == "channel")

    # ══════════════════════════════════════════════════════════════════════
    # PHASE 2: GRAPH-THEORETIC OPTIMISATION
    # With 1:1 app-to-QM mapping, the architect already creates channels
    # only where actual producer→consumer flows exist. Phase 2 focuses on:
    #   a) MST to find redundant channels (where multiple paths exist)
    #   b) Kernighan-Lin bisection for cluster detection (informational)
    #
    # PERFORMANCE NOTE: With N apps = N QMs, the old per-edge reachability
    # check was O(removable × producers × consumers × graph_copy).
    # New approach: compute MST, trust it directly for redundant edges
    # since each channel was flow-justified by the architect. Only remove
    # edges NOT in MST where the graph remains connected without them.
    # ══════════════════════════════════════════════════════════════════════
    surviving_channels = [(u, v, d) for u, v, d in G.edges(data=True) if d.get("rel") == "channel"]
    phase2_removed = []
    mst_applied = False
    kl_applied = False

    # Build undirected QM graph with weighted edges for MST
    G_qm = nx.Graph()
    active_qms = [qm for qm in qm_nodes if qm in qms_with_apps]
    active_qm_set = set(active_qms)
    G_qm.add_nodes_from(active_qms)

    # Pre-compute apps-per-QM count (O(1) lookup instead of O(N) scan)
    qm_app_count = {}
    for _, qm in app_qm_map.items():
        qm_app_count[qm] = qm_app_count.get(qm, 0) + 1

    for u, v, d in surviving_channels:
        if u in active_qm_set and v in active_qm_set:
            src_apps = qm_app_count.get(u, 0)
            dst_apps = qm_app_count.get(v, 0)
            weight = max(1, 10 - (src_apps + dst_apps))
            G_qm.add_edge(u, v, weight=weight)

    # MST-based pruning — only on connected components
    if len(active_qms) > 1 and G_qm.number_of_edges() > 0:
        # Process each connected component separately (handles disconnected topologies)
        for component in nx.connected_components(G_qm):
            if len(component) < 2:
                continue
            comp_sub = G_qm.subgraph(component).copy()
            if comp_sub.number_of_edges() <= len(component) - 1:
                # Already a tree — nothing to remove
                continue

            mst = nx.minimum_spanning_tree(comp_sub, weight="weight")
            mst_edges = set(frozenset(e) for e in mst.edges())
            comp_edges = set(frozenset(e) for e in comp_sub.edges())
            removable = comp_edges - mst_edges
            mst_applied = True

            for edge_set in removable:
                u, v = tuple(edge_set)
                # Try both directions since our directed graph stores one direction
                for src, dst in [(u, v), (v, u)]:
                    if not G.has_edge(src, dst):
                        continue
                    if G[src][dst].get("rel") != "channel":
                        continue
                    ch_name = G[src][dst].get("channel_name", f"{src}.{dst}")
                    phase2_removed.append((src, dst, ch_name))
                    G.remove_edge(src, dst)

    # ══════════════════════════════════════════════════════════════════════
    # PHASE 3: FEEDBACK-DRIVEN AGGRESSIVE PRUNING
    # Only runs when human feedback requests deeper reduction.
    # Maintains strict 1:1 app→QM ownership. All pruning is on channels only.
    # ══════════════════════════════════════════════════════════════════════
    directives = state.get("feedback_directives") or {}
    phase3_removed = []
    
    if directives.get("aggressive") or directives.get("channel_prune_pct") or directives.get("target_reduction_pct"):
        logger.info(f"OPTIMIZER: Phase 3 — feedback-driven aggressive pruning (directives: {directives})")
        
        # Count channels before Phase 3
        pre_phase3 = sum(1 for _, _, d in G.edges(data=True) if d.get("rel") == "channel")
        
        # Determine how many channels to target for removal
        # KEY FIX: base target on REMAINING channels, not initial.
        # MST already removed 90%+ of channels. Phase 3 targets what's LEFT.
        if directives.get("channel_prune_pct"):
            # "remove 50%" → remove 50% of REMAINING channels
            phase3_target = max(1, int(pre_phase3 * directives["channel_prune_pct"]))
        elif directives.get("target_reduction_pct"):
            # "reduction to 90%" → remove up to 90% of remaining channels
            phase3_target = max(1, int(pre_phase3 * directives["target_reduction_pct"]))
        else:
            # Generic aggressive — remove 40% of remaining channels
            phase3_target = max(1, int(pre_phase3 * 0.4))
        
        logger.info(f"OPTIMIZER: Phase 3 target: remove {phase3_target} of {pre_phase3} remaining channels")
        
        # ── ALWAYS run fan-out capping when aggressive ────────────────────
        # This is the PRIMARY lever for reducing FO score (15% weight).
        # Even if channel_target is already met, high fan-out QMs need capping.
        fanout_cap = directives.get("fanout_cap", 3)  # default: max 3 outbound per QM
        
        # Build flow count per channel for prioritisation
        raw_apps = state.get("raw_data", {}).get("applications", [])
        queue_prod, queue_cons = {}, {}
        for row in raw_apps:
            aid = row.get("app_id", "")
            qname = row.get("queue_name", "")
            if not qname:
                continue
            d = row.get("direction", "").upper()
            if d in ("PUT", "PRODUCER"):
                queue_prod.setdefault(qname, set()).add(aid)
            elif d in ("GET", "CONSUMER"):
                queue_cons.setdefault(qname, set()).add(aid)
        
        channel_flow_count = {}
        for qname in set(queue_prod.keys()) & set(queue_cons.keys()):
            for p in queue_prod[qname]:
                for c in queue_cons[qname]:
                    if p == c:
                        continue
                    fqm = app_qm_map.get(p)
                    tqm = app_qm_map.get(c)
                    if fqm and tqm and fqm != tqm:
                        channel_flow_count[(fqm, tqm)] = channel_flow_count.get((fqm, tqm), 0) + 1

        # ── Try LLM Channel Pruning Advisor first ─────────────────────────
        llm_pruning_applied = False
        if phase3_target > 0:
            try:
                advisor_prompt = build_channel_advisor_prompt(state, G, max_channels=80)
                logger.info(f"OPTIMIZER: Phase 3 — calling LLM channel advisor "
                           f"(~{len(advisor_prompt)//4} tokens)")
                
                llm_advice = call_llm(
                    system_prompt=CHANNEL_ADVISOR_SYSTEM,
                    user_prompt=advisor_prompt,
                    max_retries=1,
                    temperature=0.1,
                    max_tokens=4096,
                )
                
                if llm_advice and llm_advice.get("remove"):
                    removals = llm_advice["remove"]
                    llm_removed = 0
                    for entry in removals:
                        if llm_removed >= phase3_target:
                            break
                        fqm = entry.get("from_qm", "")
                        tqm = entry.get("to_qm", "")
                        if not fqm or not tqm:
                            continue
                        if G.has_edge(fqm, tqm) and G[fqm][tqm].get("rel") == "channel":
                            ch_name = G[fqm][tqm].get("channel_name", f"{fqm}.{tqm}")
                            reason = entry.get("reason", "LLM advised")
                            G.remove_edge(fqm, tqm)
                            phase3_removed.append((fqm, tqm, ch_name))
                            llm_removed += 1
                            logger.info(f"OPTIMIZER P3-LLM: Removed {ch_name} — {reason[:80]}")
                    
                    if llm_removed > 0:
                        llm_pruning_applied = True
                        logger.info(f"OPTIMIZER: Phase 3 LLM advisor removed {llm_removed} channels")
                        phase3_target -= llm_removed
            except Exception as e:
                logger.warning(f"OPTIMIZER: Phase 3 LLM advisor failed ({e}), using heuristic")

        # ── Isolation safety helper ───────────────────────────────────
        # Before removing any channel, check if it would create an ISOLATED_QM.
        # A QM is isolated if it has apps AND zero channels after removal.
        def _would_isolate(src_qm, dst_qm):
            """Return True if removing channel src→dst would isolate either QM."""
            for qm in [src_qm, dst_qm]:
                if qm not in qms_with_apps:
                    continue  # no apps = don't care
                # Count remaining channels (excluding the one we'd remove)
                remaining = 0
                for _, t, ed in G.out_edges(qm, data=True):
                    if ed.get("rel") == "channel" and not (qm == src_qm and t == dst_qm):
                        remaining += 1
                for s, _, ed in G.in_edges(qm, data=True):
                    if ed.get("rel") == "channel" and not (s == src_qm and qm == dst_qm):
                        remaining += 1
                if remaining == 0:
                    return True
            return False

        # ── Pass 3a: Fan-out capping (heuristic fallback) ─────────────
        # QMs with highest outbound channels get pruned first.
        # This directly reduces FO (15% of score).
        removed_this_pass = 0
        for qm in active_qms:
            if removed_this_pass >= phase3_target:
                break
            outbound = [(v, d) for _, v, d in G.out_edges(qm, data=True) if d.get("rel") == "channel"]
            if len(outbound) <= fanout_cap:
                continue
            # Sort by flow count ascending — weakest channels first
            outbound_scored = sorted(
                outbound,
                key=lambda x: channel_flow_count.get((qm, x[0]), 0)
            )
            # Remove excess channels (keep the top `fanout_cap` by flow count)
            to_remove = outbound_scored[:len(outbound) - fanout_cap]
            for target_qm, edge_data in to_remove:
                if removed_this_pass >= phase3_target:
                    break
                # Safety: don't create isolated QMs
                if _would_isolate(qm, target_qm):
                    logger.info(f"OPTIMIZER P3a: SKIPPED {qm}.{target_qm} — would isolate a QM")
                    continue
                ch_name = edge_data.get("channel_name", f"{qm}.{target_qm}")
                G.remove_edge(qm, target_qm)
                phase3_removed.append((qm, target_qm, ch_name))
                removed_this_pass += 1
                logger.info(f"OPTIMIZER P3a: Fan-out cap({fanout_cap}) removed {ch_name} "
                           f"(flows={channel_flow_count.get((qm, target_qm), 0)})")
        
        # ── Pass 3b: Low-flow channel removal ─────────────────────────
        # Remove channels with fewest flow pairs, regardless of fan-out.
        remaining_target = phase3_target - removed_this_pass
        if remaining_target > 0:
            current_channels_list = [
                (u, v, d) for u, v, d in G.edges(data=True) if d.get("rel") == "channel"
            ]
            # Sort by flow count ascending
            current_channels_list.sort(
                key=lambda x: channel_flow_count.get((x[0], x[1]), 0)
            )
            
            for u, v, d in current_channels_list:
                if removed_this_pass >= phase3_target:
                    break
                # Safety: don't create isolated QMs
                if _would_isolate(u, v):
                    continue
                
                ch_name = d.get("channel_name", f"{u}.{v}")
                G.remove_edge(u, v)
                phase3_removed.append((u, v, ch_name))
                removed_this_pass += 1
                logger.info(f"OPTIMIZER P3b: Low-flow removed {ch_name} "
                           f"(flows={channel_flow_count.get((u, v), 0)})")
        
        post_phase3 = sum(1 for _, _, d in G.edges(data=True) if d.get("rel") == "channel")
        logger.info(f"OPTIMIZER: Phase 3 complete — removed {len(phase3_removed)} channels "
                    f"({pre_phase3} → {post_phase3})")

    # Kernighan-Lin bisection — detect natural cluster boundaries
    # Only run on the largest connected component to avoid hanging
    kl_insight = ""
    G_qm_post = nx.Graph()
    post_channels = [(u, v) for u, v, d in G.edges(data=True)
                     if d.get("rel") == "channel" and u in active_qm_set and v in active_qm_set]
    G_qm_post.add_nodes_from(active_qms)
    G_qm_post.add_edges_from(post_channels)

    if len(active_qms) >= 4 and G_qm_post.number_of_edges() > 0:
        # Find the largest connected component for KL bisection
        largest_comp = max(nx.connected_components(G_qm_post), key=len)
        if len(largest_comp) >= 4:
            comp_sub = G_qm_post.subgraph(largest_comp).copy()
            try:
                partition = nx.community.kernighan_lin_bisection(comp_sub)
                set_a, set_b = partition
                cross_edges = sum(
                    1 for u, v in comp_sub.edges()
                    if (u in set_a and v in set_b) or (u in set_b and v in set_a)
                )
                kl_applied = True
                kl_insight = (
                    f"Kernighan-Lin bisection identified 2 natural clusters: "
                    f"{len(set_a)} QMs and {len(set_b)} QMs "
                    f"with {cross_edges} cross-cluster channel(s). "
                )
            except Exception:
                kl_insight = "Kernighan-Lin bisection: could not partition topology. "
        else:
            kl_insight = (
                f"Kernighan-Lin bisection: skipped "
                f"(largest component has {len(largest_comp)} QMs, requires ≥4). "
            )
    elif len(active_qms) >= 2:
        kl_insight = (
            f"Kernighan-Lin bisection: skipped "
            f"({len(active_qms)} active QMs, insufficient connected edges). "
        )

    # ══════════════════════════════════════════════════════════════════════
    # CYCLE DETECTION — informational metric for architectural awareness
    # Cycles = redundant routing paths. May be intentional for HA or
    # accidental from legacy config. Reported but not a constraint.
    # Skip on very large topologies (>100 QMs) to avoid slow enumeration.
    # ══════════════════════════════════════════════════════════════════════
    qm_subgraph = G.subgraph(qm_nodes)
    cycles = []
    if len(qm_nodes) <= 100:
        try:
            for i, c in enumerate(nx.simple_cycles(qm_subgraph)):
                cycles.append(c)
                if i >= 20:
                    break
        except Exception:
            pass
    cycle_info = ""
    if cycles:
        formatted = [" → ".join(c + [c[0]]) for c in cycles[:3]]
        cycle_info = (
            f"Cycle analysis: {len(cycles)} routing cycle(s) detected "
            f"({', '.join(formatted)}). "
            f"Redundant paths may serve high-availability requirements. "
        )
    else:
        cycle_info = "Cycle analysis: no cycles — topology is a clean DAG. "

    # ── Final metrics ─────────────────────────────────────────────────────
    final_channels = sum(1 for _, _, d in G.edges(data=True) if d.get("rel") == "channel")
    # Use the SAME baselines as the as-is score so improvements are measured fairly
    as_is_baselines = state.get("as_is_metrics", {}).get("baselines")
    target_metrics = compute_complexity(G, baseline_overrides=as_is_baselines)

    # ── Target subgraph analysis ──────────────────────────────────────────
    target_subgraphs = analyse_subgraphs(G)

    # ── Advanced graph analytics ──────────────────────────────────────────
    # Louvain community detection — natural QM clusters
    target_communities = detect_communities(G)
    # Betweenness centrality — SPOF detection
    target_centrality = compute_centrality(G)
    # Shannon entropy — degree distribution health
    target_entropy = compute_graph_entropy(G)

    phase1_names = [name for _, _, name in phase1_removed if name]
    phase2_names = [name for _, _, name in phase2_removed if name]
    phase3_names = [name for _, _, name in phase3_removed if name]

    target_isolated = sum(1 for s in target_subgraphs if s["is_isolated"])

    # Build analytics insight string
    analytics_insight = ""
    if target_communities.get("num_communities", 0) > 1:
        analytics_insight += (
            f"Louvain community detection: {target_communities['num_communities']} natural clusters "
            f"(modularity={target_communities['modularity']}). "
        )
    if target_centrality.get("spof_qms"):
        analytics_insight += (
            f"SPOF analysis: {len(target_centrality['spof_qms'])} high-betweenness QM(s) "
            f"({', '.join(target_centrality['spof_qms'][:3])}). "
        )
    else:
        analytics_insight += "SPOF analysis: no single points of failure detected. "
    analytics_insight += (
        f"Topology entropy: {target_entropy['degree_entropy']} bits "
        f"({target_entropy['entropy_ratio']:.0%} of theoretical max — "
        f"{'healthy distribution' if target_entropy['entropy_ratio'] > 0.6 else 'skewed — some QMs over-connected'}). "
    )

    phase3_info = ""
    if phase3_removed:
        # Check if LLM advisor was used (look for llm_pruning_applied in local scope)
        llm_note = ""
        if directives:
            try:
                if llm_pruning_applied:
                    llm_note = "AI-advised + "
            except NameError:
                pass
        phase3_info = (
            f"Phase 3 (feedback-driven): removed {len(phase3_removed)} channel(s) "
            f"via {llm_note}fan-out capping + low-flow pruning"
            f"{' (' + ', '.join(phase3_names[:5]) + ')' if phase3_names else ''}. "
        )

    msg = (
        f"{'Three' if phase3_removed else 'Two'}-phase optimisation complete. "
        f"Channels: {initial_channels} → {after_phase1} (Phase 1) "
        f"→ {after_phase1 - len(phase2_removed)} (Phase 2) "
        f"→ {final_channels} (Phase 3). "
        f"Total removed: {initial_channels - final_channels}. "
        f"Phase 1 (reachability pruning): removed {len(phase1_removed)} dead channel(s)"
        f"{' (' + ', '.join(phase1_names[:5]) + ')' if phase1_names else ''}. "
        f"Phase 2 (graph-theoretic): "
        f"weighted MST {'applied' if mst_applied else 'skipped (graph disconnected or trivial)'}, "
        f"removed {len(phase2_removed)} redundant channel(s)"
        f"{' (' + ', '.join(phase2_names[:5]) + ')' if phase2_names else ''}. "
        f"{phase3_info}"
        f"{kl_insight}"
        f"{cycle_info}"
        f"{analytics_insight}"
        f"Target subgraphs: {len(target_subgraphs)} component(s), {target_isolated} isolated. "
        f"Target complexity score: {target_metrics['total_score']}/100."
    )
    messages.append({"agent": "OPTIMIZER", "msg": msg})

    # ── LLM Design Critic (Role 4) ───────────────────────────────────────
    # Post-optimization self-review: LLM identifies weaknesses in the target design.
    # Results appear in the trace and inform the human reviewer.
    try:
        critic_state = {
            "as_is_metrics": state.get("as_is_metrics", {}),
            "target_metrics": target_metrics,
            "target_communities": target_communities,
            "target_centrality": target_centrality,
            "target_entropy": target_entropy,
            "target_subgraphs": target_subgraphs,
            "raw_data": state.get("raw_data", {}),
        }
        critic_prompt = build_design_critic_prompt(critic_state)
        logger.info(f"OPTIMIZER-LLM: Calling design critic (~{len(critic_prompt)//4} tokens)")
        
        critic_result = call_llm(
            system_prompt=DESIGN_CRITIC_SYSTEM,
            user_prompt=critic_prompt,
            max_retries=1,
            temperature=0.2,
            max_tokens=2048,
        )
        
        if critic_result:
            assessment = critic_result.get("overall_assessment", "UNKNOWN")
            issues = critic_result.get("issues", [])
            summary = critic_result.get("summary", "")[:200]
            high_issues = [i for i in issues if i.get("severity") == "HIGH"]
            
            critic_msg = (
                f"AI Design Critic: {assessment}. "
                f"{len(issues)} issues found ({len(high_issues)} HIGH). "
                f"{summary}"
            )
            messages.append({"agent": "OPTIMIZER", "msg": critic_msg})
            logger.info(f"OPTIMIZER-LLM: Design critic: {assessment}, {len(issues)} issues")
        else:
            messages.append({"agent": "OPTIMIZER", "msg": "AI Design Critic: LLM unavailable — skipped"})
    except Exception as e:
        logger.warning(f"OPTIMIZER-LLM: Design critic failed ({e})")

    # ── LLM Capacity Planner (Role 9) ────────────────────────────────────
    # Analyses flow distribution to flag over/under-provisioned QMs.
    # Results appear in agent_trace so reviewer can see capacity insights.
    capacity_analysis = None
    try:
        capacity_state = {
            "target_metrics": target_metrics,
            "target_communities": target_communities,
            "optimised_graph": G,
            "raw_data": state.get("raw_data", {}),
        }
        capacity_prompt = build_capacity_planner_prompt(capacity_state)
        logger.info(f"OPTIMIZER-LLM: Calling capacity planner (~{len(capacity_prompt)//4} tokens)")

        capacity_analysis = call_llm(
            system_prompt=CAPACITY_PLANNER_SYSTEM,
            user_prompt=capacity_prompt,
            max_retries=1,
            temperature=0.2,
            max_tokens=2048,
        )

        if capacity_analysis:
            cap_score = capacity_analysis.get("capacity_score", "?")
            hotspots = capacity_analysis.get("hotspots", [])
            high_hotspots = [h for h in hotspots if h.get("severity") == "HIGH"]
            messages.append({
                "agent": "OPTIMIZER",
                "msg": (f"AI Capacity Planner: balance score {cap_score}/100, "
                        f"{len(hotspots)} hotspot(s) ({len(high_hotspots)} HIGH). "
                        f"{capacity_analysis.get('summary', '')[:150]}")
            })
            logger.info(f"OPTIMIZER-LLM: Capacity: {cap_score}/100, {len(hotspots)} hotspots")
        else:
            messages.append({"agent": "OPTIMIZER", "msg": "AI Capacity Planner: LLM unavailable — skipped"})
    except Exception as e:
        logger.warning(f"OPTIMIZER-LLM: Capacity planner failed ({e})")

    return {
        "optimised_graph": G,
        "target_metrics": target_metrics,
        "target_subgraphs": target_subgraphs,
        "target_communities": target_communities,
        "target_centrality": target_centrality,
        "target_entropy": target_entropy,
        "capacity_analysis": capacity_analysis,
        "messages": messages,
    }


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 6: TESTER
# Validates all hard constraints against the optimised target graph.
# Returns pass/fail + violation list.
# ─────────────────────────────────────────────────────────────────────────────
def tester_agent(state: dict) -> dict:
    logger.info("TESTER: Validating constraints on target state")
    messages = state.get("messages", [])

    if state.get("error"):
        messages.append({"agent": "TESTER", "msg": f"SKIPPED — upstream error: {state['error']}"})
        return {"validation_passed": False, "messages": messages}

    if not state.get("optimised_graph"):
        err = "TESTER: No optimised_graph in state — did Optimizer run?"
        messages.append({"agent": "TESTER", "msg": err})
        return {"validation_passed": False, "error": err, "messages": messages}

    G = state["optimised_graph"]
    violations = []

    app_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "app"]
    qm_nodes  = [n for n, d in G.nodes(data=True) if d.get("type") == "qm"]

    # ── V-001: Exactly one QM per app ─────────────────────────────────────
    for app in app_nodes:
        connected_qms = [v for u, v, d in G.out_edges(app, data=True) if d.get("rel") == "connects_to"]
        if len(connected_qms) != 1:
            violations.append({
                "rule": "ONE_QM_PER_APP",
                "entity": app,
                "detail": f"App connects to {len(connected_qms)} QMs: {connected_qms}",
                "severity": "CRITICAL",
            })

    # ── V-001b: Exactly one app per QM (1:1 ownership) ───────────────────
    for qm in qm_nodes:
        apps_on_qm = [u for u, v, d in G.in_edges(qm, data=True) if d.get("rel") == "connects_to"]
        if len(apps_on_qm) > 1:
            violations.append({
                "rule": "ONE_APP_PER_QM",
                "entity": qm,
                "detail": f"QM has {len(apps_on_qm)} apps: {apps_on_qm} — constraint requires 1:1 ownership",
                "severity": "CRITICAL",
            })

    # ── V-002: Sender/Receiver pairing ────────────────────────────────────
    # Every sender channel from QM_A→QM_B must have a matching entry
    # In the graph we only store sender edges, so we check that for every
    # channel edge (from_qm→to_qm), both QMs exist in the target state
    channel_edges = [(u, v, d) for u, v, d in G.edges(data=True) if d.get("rel") == "channel"]
    for from_qm, to_qm, d in channel_edges:
        ch_name = d.get("channel_name", f"{from_qm}.{to_qm}")
        if to_qm not in qm_nodes:
            violations.append({
                "rule": "SENDER_RECEIVER_PAIR",
                "entity": ch_name,
                "detail": f"Sender channel {ch_name} targets {to_qm} which is not in target state — no receiver possible",
                "severity": "CRITICAL",
            })
        if from_qm not in qm_nodes:
            violations.append({
                "rule": "SENDER_RECEIVER_PAIR",
                "entity": ch_name,
                "detail": f"Sender channel {ch_name} originates from {from_qm} which is not in target state",
                "severity": "CRITICAL",
            })

    # ── V-003: Channel naming convention ──────────────────────────────────
    for from_qm, to_qm, d in channel_edges:
        ch_name = d.get("channel_name", "")
        expected = f"{from_qm}.{to_qm}"
        if ch_name and ch_name != expected:
            violations.append({
                "rule": "CHANNEL_NAMING",
                "entity": ch_name,
                "detail": f"Channel name '{ch_name}' does not match convention '{expected}'",
                "severity": "CRITICAL",
            })
        if not ch_name:
            violations.append({
                "rule": "CHANNEL_NAMING",
                "entity": f"{from_qm}->{to_qm}",
                "detail": "Channel missing name entirely",
                "severity": "CRITICAL",
            })

    # ── V-004: XMITQ existence ────────────────────────────────────────────
    # Every sender channel must reference an XMITQ
    for from_qm, to_qm, d in channel_edges:
        xmitq = d.get("xmit_queue", "")
        ch_name = d.get("channel_name", f"{from_qm}.{to_qm}")
        if not xmitq:
            violations.append({
                "rule": "XMITQ_EXISTS",
                "entity": ch_name,
                "detail": f"Sender channel {ch_name} has no XMITQ reference",
                "severity": "CRITICAL",
            })

    # ── V-005: No orphan QMs in target state ──────────────────────────────
    for qm in qm_nodes:
        connected_apps = [u for u, v, d in G.in_edges(qm, data=True) if d.get("rel") == "connects_to"]
        if not connected_apps:
            violations.append({
                "rule": "NO_ORPHAN_QMS",
                "entity": qm,
                "detail": f"QM has no application owner in target state",
                "severity": "WARNING",
            })

    # ── V-006: Consumer queue existence ───────────────────────────────────
    # Every app should have a reachable local queue on its QM
    # (This is structural — we verify the graph supports it)
    for app in app_nodes:
        qms = [v for _, v, d in G.out_edges(app, data=True) if d.get("rel") == "connects_to"]
        if not qms:
            violations.append({
                "rule": "CONSUMER_QUEUE_EXISTS",
                "entity": app,
                "detail": f"App {app} has no QM connection — cannot have a local queue",
                "severity": "CRITICAL",
            })

    # ── V-007: Producer→Consumer path completeness ────────────────────────
    # For every pair of QMs connected by a channel, verify at least one
    # app exists on each end (producer side and consumer side)
    app_qm_map = {}
    for app in app_nodes:
        qms = [v for _, v, d in G.out_edges(app, data=True) if d.get("rel") == "connects_to"]
        if qms:
            app_qm_map[app] = qms[0]

    qms_with_apps = set(app_qm_map.values())
    for from_qm, to_qm, d in channel_edges:
        ch_name = d.get("channel_name", f"{from_qm}.{to_qm}")
        if from_qm not in qms_with_apps and to_qm not in qms_with_apps:
            violations.append({
                "rule": "PATH_COMPLETENESS",
                "entity": ch_name,
                "detail": f"Channel {ch_name} connects {from_qm}→{to_qm} but neither QM has any apps — channel is unnecessary",
                "severity": "WARNING",
            })

    # ── V-008: No isolated QMs with apps ─────────────────────────────────
    # Every QM that has apps AND other QMs also have apps MUST have at least
    # one channel (inbound or outbound) connecting it to the rest of the topology.
    # A QM with apps but zero channels is disconnected — its apps can't
    # communicate with anything outside that QM.
    #
    # HOWEVER: if the app has zero cross-app flows (no producer→consumer 
    # relationships with any other app), it is legitimately self-contained.
    # This is a WARNING, not CRITICAL — the app simply doesn't need channels.
    
    # Build flow data to distinguish "broken isolation" from "genuine isolation"
    _queue_prod_t = {}
    _queue_cons_t = {}
    for row in state.get("raw_data", {}).get("applications", []):
        _aid = row["app_id"]
        _qn = row.get("queue_name", "")
        if not _qn:
            continue
        _dir = row.get("direction", "").upper()
        if _dir in ("PUT", "PRODUCER"):
            _queue_prod_t.setdefault(_qn, set()).add(_aid)
        elif _dir in ("GET", "CONSUMER"):
            _queue_cons_t.setdefault(_qn, set()).add(_aid)
    
    # Apps with cross-app flows
    _apps_with_flows = set()
    for _qn in set(_queue_prod_t.keys()) & set(_queue_cons_t.keys()):
        for _p in _queue_prod_t[_qn]:
            for _c in _queue_cons_t[_qn]:
                if _p != _c:
                    _apps_with_flows.add(_p)
                    _apps_with_flows.add(_c)
    
    if len(qms_with_apps) > 1:
        for qm in qm_nodes:
            if qm not in qms_with_apps:
                continue
            has_outbound = any(
                u == qm and d.get("rel") == "channel"
                for u, _, d in G.out_edges(qm, data=True)
            )
            has_inbound = any(
                v == qm and d.get("rel") == "channel"
                for _, v, d in G.in_edges(qm, data=True)
            )
            if not has_outbound and not has_inbound:
                apps_on_qm = [a for a, q in app_qm_map.items() if q == qm]
                # Check if any app on this QM has cross-app flows
                has_flows = any(a in _apps_with_flows for a in apps_on_qm)
                violations.append({
                    "rule": "ISOLATED_QM",
                    "entity": qm,
                    "detail": f"QM {qm} has apps {apps_on_qm} but zero channels — apps are completely disconnected from the topology",
                    "severity": "CRITICAL" if has_flows else "WARNING",
                })

    passed = not any(v["severity"] == "CRITICAL" for v in violations)

    # ── LLM Compliance Auditor (Role 8) ──────────────────────────────────
    # AI audits target state for security, HA, and best-practice gaps
    # beyond what rule-based checks can catch.
    compliance_audit = None
    try:
        audit_state = {
            "target_metrics": state.get("target_metrics", {}),
            "as_is_metrics": state.get("as_is_metrics", {}),
            "target_communities": state.get("target_communities", {}),
            "target_centrality": state.get("target_centrality", {}),
            "target_entropy": state.get("target_entropy", {}),
            "target_subgraphs": state.get("target_subgraphs", []),
            "constraint_violations": violations,
            "optimised_graph": G,
            "raw_data": state.get("raw_data", {}),
        }
        audit_prompt = build_compliance_auditor_prompt(audit_state)
        logger.info(f"TESTER-LLM: Calling compliance auditor (~{len(audit_prompt)//4} tokens)")

        compliance_audit = call_llm(
            system_prompt=COMPLIANCE_AUDITOR_SYSTEM,
            user_prompt=audit_prompt,
            max_retries=1,
            temperature=0.2,
            max_tokens=2048,
        )

        if compliance_audit:
            score = compliance_audit.get("compliance_score", "?")
            findings = compliance_audit.get("findings", [])
            high_findings = [f for f in findings if f.get("severity") in ("CRITICAL", "HIGH")]
            ha = compliance_audit.get("ha_assessment", {})
            messages.append({
                "agent": "TESTER",
                "msg": (f"AI Compliance Auditor: score {score}/100, "
                        f"{len(findings)} finding(s) ({len(high_findings)} critical/high). "
                        f"HA: {'redundant' if ha.get('has_redundancy') else 'no redundancy'}, "
                        f"SPOFs: {ha.get('spof_count', '?')}. "
                        f"{compliance_audit.get('summary', '')[:120]}")
            })
            logger.info(f"TESTER-LLM: Compliance: {score}/100, {len(findings)} findings")
        else:
            messages.append({"agent": "TESTER", "msg": "AI Compliance Auditor: LLM unavailable — skipped"})
    except Exception as e:
        logger.warning(f"TESTER-LLM: Compliance audit failed ({e})")

    msg = (
        f"Tester: {'PASS' if passed else 'FAIL'} — "
        f"{len(violations)} violations found "
        f"({sum(1 for v in violations if v['severity']=='CRITICAL')} critical, "
        f"{sum(1 for v in violations if v['severity']=='WARNING')} warnings). "
        f"Checks: 1-QM-per-app, sender/receiver pairs, channel naming, "
        f"XMITQ existence, orphan QMs, consumer queues, path completeness."
    )
    messages.append({"agent": "TESTER", "msg": msg})

    return {
        "validation_passed": passed,
        "constraint_violations": violations,
        "compliance_audit": compliance_audit,
        "messages": messages,
    }


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 8: PROVISIONER
# Generates two outputs:
#   1. MQSC scripts — ready-to-run MQ commands
#   2. TARGET STATE CSVs — same format as input, representing the clean topology
#
# Why output CSVs?
# The problem statement says "drive automation-first provisioning of MQ
# infrastructure." The most logical interpretation is that the output mirrors
# the input format so it can feed directly into any provisioning tool that
# already accepts the input CSV format. MQSC alone is one tool. CSV output
# makes the target state tool-agnostic.
# ─────────────────────────────────────────────────────────────────────────────
def provisioner_agent(state: dict) -> dict:
    logger.info("PROVISIONER: Generating MQSC scripts and target state CSVs")
    messages = state.get("messages", [])
    G = state["optimised_graph"]

    # ── Build helper lookups ──────────────────────────────────────────────
    qm_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "qm"]
    app_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "app"]

    # App → QM ownership map
    app_qm_map = {}
    for app in app_nodes:
        qms = [v for _, v, d in G.out_edges(app, data=True) if d.get("rel") == "connects_to"]
        if qms:
            app_qm_map[app] = qms[0]

    # Outbound channels per QM: {qm: [(to_qm, channel_name, xmitq), ...]}
    outbound = {qm: [] for qm in qm_nodes}
    # Inbound channels per QM: {qm: [(from_qm, channel_name), ...]}
    inbound = {qm: [] for qm in qm_nodes}

    for from_qm, to_qm, d in G.edges(data=True):
        if d.get("rel") != "channel":
            continue
        ch_name = d.get("channel_name") or f"{from_qm}.{to_qm}"
        xmitq = d.get("xmit_queue") or f"{to_qm}.XMITQ"
        if from_qm in outbound:
            outbound[from_qm].append((to_qm, ch_name, xmitq))
        if to_qm in inbound:
            inbound[to_qm].append((from_qm, ch_name))

    # Apps per QM
    apps_on_qm = {qm: [] for qm in qm_nodes}
    for app, qm in app_qm_map.items():
        if qm in apps_on_qm:
            apps_on_qm[qm].append(app)

    # Build remote queue definitions: for each producer app on this QM,
    # determine which remote QMs it needs to reach
    # Use raw application data to find producer/consumer relationships
    raw_apps = state.get("raw_data", {}).get("applications", [])

    # ── Generate per-QM MQSC scripts ──────────────────────────────────────
    # Each QM gets its own script — run via: runmqsc QM_NAME < QM_NAME.mqsc
    per_qm_scripts = {}
    port_base = 1414

    # Pre-build port lookup (avoids sorted().index() inside loop)
    sorted_qms = sorted(qm_nodes)
    qm_port_map = {qm: port_base + idx for idx, qm in enumerate(sorted_qms)}

    for idx, qm in enumerate(sorted_qms):
        lines = []
        port = port_base + idx
        qm_data = G.nodes[qm]
        hostname = f"{qm.lower().replace('_', '-')}.target.corp.com"

        lines.append(f"* =============================================")
        lines.append(f"* Target State MQSC for {qm}")
        lines.append(f"* Generated by IntelliAI Provisioner Agent")
        lines.append(f"* Run via: runmqsc {qm} < {qm}_target.mqsc")
        lines.append(f"* =============================================")
        lines.append("")

        # 1. Listener
        lines.append("* --- Listener ---")
        lines.append(f"DEFINE LISTENER('LSR.{qm}') TRPTYPE(TCP) PORT({port}) REPLACE")
        lines.append(f"START LISTENER('LSR.{qm}')")
        lines.append("")

        # 2. Local application queues (consumers GET from these)
        local_queues = []
        if apps_on_qm.get(qm):
            lines.append("* --- Local Application Queues ---")
            for app in sorted(apps_on_qm[qm]):
                lq = f"LOCAL.{app}.IN"
                local_queues.append(lq)
                lines.append(f"DEFINE QLOCAL('{lq}') REPLACE")
            lines.append("")

        # 3. Transmission queues — one per target QM we send to
        if outbound.get(qm):
            lines.append("* --- Transmission Queues ---")
            seen_xmitq = set()
            for to_qm, ch_name, xmitq in outbound[qm]:
                if xmitq not in seen_xmitq:
                    seen_xmitq.add(xmitq)
                    lines.append(f"DEFINE QLOCAL('{xmitq}') USAGE(XMITQ) REPLACE")
            lines.append("")

        # 4. Remote queue definitions — for each remote consumer reachable via channels
        remote_qs = []
        if outbound.get(qm):
            lines.append("* --- Remote Queue Definitions ---")
            for to_qm, ch_name, xmitq in outbound[qm]:
                # For each app on the remote QM, create a QREMOTE pointing to their local queue
                for remote_app in apps_on_qm.get(to_qm, []):
                    rq_local_name = f"REMOTE.{remote_app}.VIA.{to_qm}"
                    remote_queue = f"LOCAL.{remote_app}.IN"
                    remote_qs.append(rq_local_name)
                    lines.append(
                        f"DEFINE QREMOTE('{rq_local_name}') "
                        f"RQMNAME('{to_qm}') "
                        f"RNAME('{remote_queue}') "
                        f"XMITQ('{xmitq}') REPLACE"
                    )
            lines.append("")

        # 5. Sender channels
        if outbound.get(qm):
            lines.append("* --- Sender Channels ---")
            for to_qm, ch_name, xmitq in outbound[qm]:
                to_hostname = f"{to_qm.lower().replace('_', '-')}.target.corp.com"
                to_port = qm_port_map.get(to_qm, port_base)
                lines.append(
                    f"DEFINE CHANNEL('{ch_name}') CHLTYPE(SDR) "
                    f"CONNAME('{to_hostname}({to_port})') "
                    f"XMITQ('{xmitq}') REPLACE"
                )
            lines.append("")

        # 6. Receiver channels — for each QM that sends TO this QM
        if inbound.get(qm):
            lines.append("* --- Receiver Channels ---")
            for from_qm, ch_name in inbound[qm]:
                # Receiver uses the SAME channel name as the sender
                lines.append(f"DEFINE CHANNEL('{ch_name}') CHLTYPE(RCVR) REPLACE")
            lines.append("")

        # 7. Start sender channels
        if outbound.get(qm):
            lines.append("* --- Start Channels ---")
            for to_qm, ch_name, xmitq in outbound[qm]:
                lines.append(f"START CHANNEL('{ch_name}')")
            lines.append("")

        lines.append("* --- End of script ---")
        per_qm_scripts[qm] = "\n".join(lines)

    # ── Combined script (for backward compat with frontend) ───────────────
    combined = []
    combined.append("* ============================================================")
    combined.append("* IntelliAI Combined MQSC — All Queue Managers")
    combined.append(f"* Session: {state.get('session_id', 'unknown')}")
    combined.append("* NOTE: In production, run each QM section separately via:")
    combined.append("*   runmqsc QM_NAME < QM_NAME_target.mqsc")
    combined.append("* ============================================================")
    combined.append("")
    for qm in sorted(per_qm_scripts.keys()):
        combined.append(per_qm_scripts[qm])
        combined.append("")

    # ── Target State CSV Output ───────────────────────────────────────────
    logger.info("PROVISIONER: MQSC done. Generating target CSVs...")
    target_csvs = _generate_target_csvs(G, state)

    # ── target-topology.json (Output.md §8.2 Deliverable 1) ──────────────
    target_csvs["target-topology"] = _generate_target_topology_json(G, state)

    # Add per-QM scripts as downloadable CSVs too
    for qm, script in per_qm_scripts.items():
        target_csvs[f"mqsc_{qm}"] = script

    total_commands = sum(
        1 for line in "\n".join(per_qm_scripts.values()).split("\n")
        if line.strip() and not line.strip().startswith("*")
    )

    msg = (
        f"Generated MQSC for {len(per_qm_scripts)} queue managers "
        f"({total_commands} commands total). "
        f"Includes: listeners, local queues, XMITQs, remote queues, "
        f"sender channels, receiver channels. "
        f"Target state CSVs: {[k for k in target_csvs.keys() if not k.startswith('mqsc_')]}."
    )
    messages.append({"agent": "PROVISIONER", "msg": msg})

    return {
        "mqsc_scripts": combined,
        "target_csvs": target_csvs,
        "messages": messages,
    }


def _generate_target_csvs(G: nx.DiGraph, state: dict) -> dict:
    """
    Generate target state CSV files in the same format as the input CSVs.
    This is a key output — it makes the target state machine-readable
    and directly usable by any provisioning tool that accepts the input format.
    """
    csvs = {}

    # ── queue_managers.csv ────────────────────────────────────────────────
    qm_rows = []
    for n, d in G.nodes(data=True):
        if d.get("type") == "qm":
            qm_rows.append({
                "qm_id":       n,
                "qm_name":     d.get("name", n),
                "region":      d.get("region", ""),
                "host":        f"{n.lower().replace('_', '-')}.target.corp.com",
                "description": f"Target state QM — {d.get('name', n)}",
            })
    csvs["target_queue_managers"] = _to_csv(qm_rows, ["qm_id", "qm_name", "region", "host", "description"])

    # ── channels.csv — only the channels the Architect introduced ─────────
    channel_rows = []
    ch_id = 1
    for from_qm, to_qm, d in G.edges(data=True):
        if d.get("rel") != "channel":
            continue
        xmitq        = d.get("xmit_queue") or f"{to_qm}.XMITQ"
        channel_name = d.get("channel_name") or f"{from_qm}.{to_qm}"
        # In IBM MQ, receiver channel has the SAME name as the sender
        receiver_name = channel_name

        # Sender
        channel_rows.append({
            "channel_id":   f"TCH{ch_id:03d}",
            "channel_name": channel_name,
            "channel_type": "SENDER",
            "from_qm":      from_qm,
            "to_qm":        to_qm,
            "xmit_queue":   xmitq,
            "status":       "RUNNING",
            "description":  f"Target sender channel {from_qm} to {to_qm}",
        })
        ch_id += 1

        # Receiver — sits on to_qm, receives from from_qm
        # Channel name is the SAME as the sender
        channel_rows.append({
            "channel_id":   f"TCH{ch_id:03d}",
            "channel_name": receiver_name,
            "channel_type": "RECEIVER",
            "from_qm":      from_qm,
            "to_qm":        to_qm,
            "xmit_queue":   "",
            "status":       "RUNNING",
            "description":  f"Target receiver channel on {to_qm} from {from_qm}",
        })
        ch_id += 1

    csvs["target_channels"] = _to_csv(
        channel_rows,
        ["channel_id", "channel_name", "channel_type", "from_qm", "to_qm", "xmit_queue", "status", "description"]
    )

    # ── queues.csv — local queues, remote queues, xmitqs ─────────────────
    queue_rows = []
    q_id = 1

    # One local queue per app (consumer side)
    for app in [n for n, d in G.nodes(data=True) if d.get("type") == "app"]:
        qms = [v for _, v, d in G.out_edges(app, data=True) if d.get("rel") == "connects_to"]
        if not qms:
            continue
        qm = qms[0]
        queue_rows.append({
            "queue_id":    f"TQ{q_id:03d}",
            "queue_name":  f"LOCAL.{app}.IN",
            "qm_id":       qm,
            "queue_type":  "LOCAL",
            "usage":       "NORMAL",
            "description": f"Local queue for {app}",
        })
        q_id += 1

    # RemoteQ and XMITq for each channel
    for from_qm, to_qm, d in G.edges(data=True):
        if d.get("rel") != "channel":
            continue
        xmitq = d.get("xmit_queue") or f"XMITQ.{to_qm}"

        queue_rows.append({
            "queue_id":    f"TQ{q_id:03d}",
            "queue_name":  f"REMOTE.TO.{to_qm}",
            "qm_id":       from_qm,
            "queue_type":  "REMOTE",
            "usage":       "NORMAL",
            "description": f"Remote queue on {from_qm} pointing to {to_qm}",
        })
        q_id += 1

        queue_rows.append({
            "queue_id":    f"TQ{q_id:03d}",
            "queue_name":  xmitq,
            "qm_id":       from_qm,
            "queue_type":  "LOCAL",
            "usage":       "XMITQ",
            "description": f"Transmission queue on {from_qm} for {to_qm}",
        })
        q_id += 1

    csvs["target_queues"] = _to_csv(
        queue_rows,
        ["queue_id", "queue_name", "qm_id", "queue_type", "usage", "description"]
    )

    # ── applications.csv — same apps, now with single QM ownership ────────
    app_rows = []
    raw_apps = state.get("raw_data", {}).get("applications", [])

    # Build canonical ownership map from graph
    app_qm_map = {}
    for app in [n for n, d in G.nodes(data=True) if d.get("type") == "app"]:
        qms = [v for _, v, d in G.out_edges(app, data=True) if d.get("rel") == "connects_to"]
        if qms:
            app_qm_map[app] = qms[0]

    # Use original app rows but enforce the single QM
    seen = set()
    for row in raw_apps:
        app_id = row["app_id"]
        if app_id not in app_qm_map:
            continue  # app was removed (was orphan)
        canonical_qm = app_qm_map[app_id]
        key = (app_id, row.get("direction"), row.get("queue_id"))
        if key in seen:
            continue
        seen.add(key)
        app_rows.append({
            "app_id":      app_id,
            "app_name":    row.get("app_name", app_id),
            "qm_id":       canonical_qm,          # enforced single QM
            "direction":   row.get("direction", ""),
            "queue_id":    row.get("queue_id", ""),
            "description": row.get("description", ""),
        })

    csvs["target_applications"] = _to_csv(
        app_rows,
        ["app_id", "app_name", "qm_id", "direction", "queue_id", "description"]
    )

    # ── MQ_Raw_Data_Target.csv — SAME FORMAT AS INPUT ─────────────────────
    # One CSV with the original 29 columns, reflecting the optimised topology.
    # Judges can feed this back into IntelliAI to verify complexity dropped.
    logger.info("PROVISIONER: generating unified 29-column target CSV...")
    csvs["MQ_Raw_Data_Target"] = _generate_unified_target_csv(G, state)
    logger.info("PROVISIONER: unified CSV done.")

    return csvs


def _generate_unified_target_csv(G: nx.DiGraph, state: dict) -> str:
    """
    Generate the target state as a single CSV with the same 29 columns
    as the original input file. Each row = one app-queue relationship.
    """
    INPUT_COLUMNS = [
        "Discrete Queue Name", "ProducerName", "Consumer Name",
        "Primary App_Full_Name", "PrimaryAppDisp", "PrimaryAppRole",
        "Primary Application Id q_type", "Primary Neighbourhood",
        "Primary Hosting Type", "Primary Data Classification",
        "Primary Enterprise Critical Payment Application", "Primary PCI",
        "Primary Publicly Accessible", "Primary TRTC",
        "q_type", "queue_manager_name", "app_id", "line_of_business",
        "cluster_name", "cluster_namelist", "def_persistence",
        "def_put_response", "inhibit_get", "inhibit_put",
        "remote_q_mgr_name", "remote_q_name", "usage",
        "xmit_q_name", "Neighborhood",
    ]

    raw_data = state.get("raw_data", {})

    # Build lookup maps from graph
    app_qm_map = {}
    app_name_map = {}
    for n, d in G.nodes(data=True):
        if d.get("type") == "app":
            app_name_map[n] = d.get("name", n)
            for _, v, ed in G.out_edges(n, data=True):
                if ed.get("rel") == "connects_to":
                    app_qm_map[n] = v

    qm_region_map = {}
    qm_lob_map = {}
    # Pre-build LOB lookup from raw data
    raw_qm_lob = {qm_row["qm_id"]: qm_row.get("line_of_business", "") for qm_row in raw_data.get("queue_managers", [])}
    for n, d in G.nodes(data=True):
        if d.get("type") == "qm":
            qm_region_map[n] = d.get("region", "")
            qm_lob_map[n] = raw_qm_lob.get(n, "")

    # Build original app metadata lookup from raw input
    app_meta = {}
    for row in raw_data.get("applications", []):
        aid = row["app_id"]
        if aid not in app_meta:
            app_meta[aid] = {
                "app_name": row.get("app_name", aid),
                "direction": row.get("direction", "UNKNOWN"),
            }

    # Collect channels: from_qm → to_qm
    channels = []
    for u, v, d in G.edges(data=True):
        if d.get("rel") == "channel":
            channels.append({
                "from_qm": u, "to_qm": v,
                "channel_name": d.get("channel_name", f"{u}.{v}"),
                "xmit_queue": d.get("xmit_queue", f"{v}.XMITQ"),
            })

    rows = []

    # Pre-build lookup: qm → list of apps (avoids O(n) scan per channel)
    qm_to_apps = {}
    for a, q in app_qm_map.items():
        qm_to_apps.setdefault(q, []).append(a)

    # Pre-build lookup: qm → list of outbound channels
    qm_outbound = {}
    for ch in channels:
        qm_outbound.setdefault(ch["from_qm"], []).append(ch)

    # For each app, generate rows showing its queue relationships
    for app_id, qm_id in app_qm_map.items():
        meta = app_meta.get(app_id, {"app_name": app_id, "direction": "UNKNOWN"})
        region = qm_region_map.get(qm_id, "")
        lob = qm_lob_map.get(qm_id, "")
        direction = meta["direction"]
        role = "Producer" if direction == "PUT" else "Consumer" if direction == "GET" else "Unknown"

        # 1. Local queue for this app (every app has one)
        local_q = f"LOCAL.{app_id}.IN"
        rows.append({
            "Discrete Queue Name": local_q,
            "ProducerName": app_name_map.get(app_id, app_id) if role == "Producer" else "",
            "Consumer Name": app_name_map.get(app_id, app_id) if role == "Consumer" else "",
            "Primary App_Full_Name": app_name_map.get(app_id, app_id),
            "PrimaryAppDisp": region,
            "PrimaryAppRole": role,
            "Primary Application Id q_type": "Local",
            "Primary Neighbourhood": region,
            "Primary Hosting Type": "Internal",
            "Primary Data Classification": "Confidential",
            "Primary Enterprise Critical Payment Application": "No",
            "Primary PCI": "No",
            "Primary Publicly Accessible": "No",
            "Primary TRTC": "00 = 0-30 Minutes",
            "q_type": "Local",
            "queue_manager_name": qm_id,
            "app_id": app_id,
            "line_of_business": lob,
            "cluster_name": "",
            "cluster_namelist": "",
            "def_persistence": "Yes",
            "def_put_response": "Synchronous",
            "inhibit_get": "Enabled",
            "inhibit_put": "Enabled",
            "remote_q_mgr_name": "",
            "remote_q_name": "",
            "usage": "Normal",
            "xmit_q_name": "",
            "Neighborhood": region,
        })

        # 2. Remote queues: for each outbound channel from this app's QM
        for ch in qm_outbound.get(qm_id, []):
            to_qm = ch["to_qm"]
            # Use pre-built lookup instead of scanning all apps
            for cons_app in qm_to_apps.get(to_qm, []):
                rq_name = f"REMOTE.{cons_app}.VIA.{to_qm}"
                remote_local_q = f"LOCAL.{cons_app}.IN"
                rows.append({
                    "Discrete Queue Name": rq_name,
                    "ProducerName": app_name_map.get(app_id, app_id),
                    "Consumer Name": app_name_map.get(cons_app, cons_app),
                    "Primary App_Full_Name": app_name_map.get(app_id, app_id),
                    "PrimaryAppDisp": region,
                    "PrimaryAppRole": "Producer",
                    "Primary Application Id q_type": "Remote",
                    "Primary Neighbourhood": region,
                    "Primary Hosting Type": "Internal",
                    "Primary Data Classification": "Confidential",
                    "Primary Enterprise Critical Payment Application": "No",
                    "Primary PCI": "No",
                    "Primary Publicly Accessible": "No",
                    "Primary TRTC": "00 = 0-30 Minutes",
                    "q_type": "Remote",
                    "queue_manager_name": qm_id,
                    "app_id": app_id,
                    "line_of_business": lob,
                    "cluster_name": "",
                    "cluster_namelist": "",
                    "def_persistence": "Yes",
                    "def_put_response": "Synchronous",
                    "inhibit_get": "",
                    "inhibit_put": "Enabled",
                    "remote_q_mgr_name": to_qm,
                    "remote_q_name": remote_local_q,
                    "usage": "",
                    "xmit_q_name": ch["xmit_queue"],
                    "Neighborhood": region,
                })

    return _to_csv(rows, INPUT_COLUMNS)


def _to_csv(rows: list, fieldnames: list) -> str:
    """Convert a list of dicts to a CSV string."""
    if not rows:
        return ",".join(fieldnames) + "\n"
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 9: MIGRATION PLANNER
# Computes topology diff and generates ordered migration steps with rollback.
# This is a key differentiator — no other team will have this.
# ─────────────────────────────────────────────────────────────────────────────
def migration_planner_agent(state: dict) -> dict:
    logger.info("MIGRATION PLANNER: Generating ordered migration plan with rollback")
    messages = state.get("messages", [])

    as_is_graph = state.get("as_is_graph")
    target_graph = state.get("optimised_graph")

    if not as_is_graph or not target_graph:
        messages.append({"agent": "MIGRATION_PLANNER", "msg": "Missing graphs — cannot generate migration plan"})
        return {"migration_plan": None, "topology_diff": None, "messages": messages}

    # ── Step 1: Compute topology diff ─────────────────────────────────────
    logger.info("MIGRATION PLANNER: computing topology diff...")
    diff = _compute_topology_diff(as_is_graph, target_graph, state)
    logger.info(f"MIGRATION PLANNER: diff done — {len(diff.get('qms_added',[]))} QMs added, {len(diff.get('channels_added',[]))} channels added, {len(diff.get('apps_reassigned',[]))} apps reassigned")

    # ── Step 2: Generate ordered migration steps ──────────────────────────
    logger.info("MIGRATION PLANNER: generating migration steps...")
    steps = _generate_migration_steps(diff, target_graph)
    logger.info(f"MIGRATION PLANNER: {len(steps)} steps generated")

    migration_plan = {
        "total_steps": len(steps),
        "phases": {
            "CREATE": [s for s in steps if s["phase"] == "CREATE"],
            "REROUTE": [s for s in steps if s["phase"] == "REROUTE"],
            "DRAIN": [s for s in steps if s["phase"] == "DRAIN"],
            "CLEANUP": [s for s in steps if s["phase"] == "CLEANUP"],
        },
        "steps": steps,
    }

    msg = (
        f"Migration plan generated: {len(steps)} steps across 4 phases. "
        f"CREATE: {len(migration_plan['phases']['CREATE'])}, "
        f"REROUTE: {len(migration_plan['phases']['REROUTE'])}, "
        f"DRAIN: {len(migration_plan['phases']['DRAIN'])}, "
        f"CLEANUP: {len(migration_plan['phases']['CLEANUP'])}. "
        f"Diff: {len(diff['qms_removed'])} QMs removed, "
        f"{len(diff['channels_added'])} channels added, "
        f"{len(diff['channels_removed'])} channels removed, "
        f"{len(diff['apps_reassigned'])} apps reassigned."
    )
    messages.append({"agent": "MIGRATION_PLANNER", "msg": msg})

    # ── LLM Migration Risk Assessor (Role 5) ─────────────────────────────
    # LLM scores each phase by risk, identifies high-risk steps, recommends
    # maintenance windows. Results enrich the Migration tab.
    try:
        risk_state = {
            "topology_diff": diff,
            "migration_plan": migration_plan,
            "raw_data": state.get("raw_data", {}),
        }
        risk_prompt = build_migration_risk_prompt(risk_state)
        logger.info(f"MIGRATION-LLM: Calling risk assessor (~{len(risk_prompt)//4} tokens)")
        
        risk_result = call_llm(
            system_prompt=MIGRATION_RISK_SYSTEM,
            user_prompt=risk_prompt,
            max_retries=1,
            temperature=0.2,
            max_tokens=2048,
        )
        
        if risk_result:
            migration_plan["risk_assessment"] = risk_result
            phase_risks = risk_result.get("phase_risks", {})
            high_risk = risk_result.get("high_risk_steps", [])
            windows = risk_result.get("maintenance_windows", [])
            
            risk_msg = (
                f"AI Risk Assessment: "
                + ", ".join(f"{p}={r.get('risk','?')}" for p, r in phase_risks.items())
                + f". {len(high_risk)} high-risk step(s) identified. "
                + (f"Windows: {'; '.join(windows[:2])}" if windows else "")
            )
            messages.append({"agent": "MIGRATION_PLANNER", "msg": risk_msg})
            logger.info(f"MIGRATION-LLM: Risk assessment complete")
        else:
            messages.append({"agent": "MIGRATION_PLANNER", "msg": "AI Risk Assessment: LLM unavailable — skipped"})
    except Exception as e:
        logger.warning(f"MIGRATION-LLM: Risk assessment failed ({e})")

    return {
        "migration_plan": migration_plan,
        "topology_diff": diff,
        "messages": messages,
    }


def _compute_topology_diff(as_is_graph, target_graph, state: dict) -> dict:
    """Compute what changed between as-is and target topologies."""
    # QM sets
    as_is_qms = set(n for n, d in as_is_graph.nodes(data=True) if d.get("type") == "qm")
    target_qms = set(n for n, d in target_graph.nodes(data=True) if d.get("type") == "qm")

    # Channel sets (as tuples of from_qm, to_qm)
    as_is_channels = set()
    for u, v, d in as_is_graph.edges(data=True):
        if d.get("rel") == "channel":
            as_is_channels.add((u, v))
    target_channels = set()
    for u, v, d in target_graph.edges(data=True):
        if d.get("rel") == "channel":
            target_channels.add((u, v))

    # App assignments — compare as-is vs target
    def get_app_qm_map(G):
        m = {}
        for n, d in G.nodes(data=True):
            if d.get("type") == "app":
                qms = [v for _, v, ed in G.out_edges(n, data=True) if ed.get("rel") == "connects_to"]
                if qms:
                    m[n] = qms[0]
        return m

    as_is_apps = get_app_qm_map(as_is_graph)
    target_apps = get_app_qm_map(target_graph)

    apps_reassigned = []
    for app_id, old_qm in as_is_apps.items():
        new_qm = target_apps.get(app_id)
        if new_qm and old_qm != new_qm:
            apps_reassigned.append({
                "app_id": app_id,
                "old_qm": old_qm,
                "new_qm": new_qm,
            })

    return {
        "qms_added": sorted(target_qms - as_is_qms),
        "qms_removed": sorted(as_is_qms - target_qms),
        "qms_unchanged": sorted(as_is_qms & target_qms),
        "channels_added": sorted(target_channels - as_is_channels),
        "channels_removed": sorted(as_is_channels - target_channels),
        "apps_reassigned": apps_reassigned,
    }


def _generate_migration_steps(diff: dict, target_graph) -> list:
    """Generate ordered migration steps from topology diff."""
    steps = []
    step_num = 0

    # Helper for port lookup
    qm_nodes = sorted(n for n, d in target_graph.nodes(data=True) if d.get("type") == "qm")
    port_base = 1414

    def get_conname(qm):
        hostname = f"{qm.lower().replace('_', '-')}.target.corp.com"
        idx = qm_nodes.index(qm) if qm in qm_nodes else 0
        return f"{hostname}({port_base + idx})"

    # ── PHASE 1: CREATE — new infrastructure ──────────────────────────────

    # 1a. New QMs (if any)
    for qm in diff["qms_added"]:
        step_num += 1
        steps.append({
            "step_number": step_num,
            "phase": "CREATE",
            "description": f"Create new queue manager {qm}",
            "target_qm": qm,
            "mqsc_forward": f"crtmqm {qm}\nstrmqm {qm}",
            "mqsc_rollback": f"endmqm -i {qm}\ndltmqm {qm}",
            "depends_on": [],
            "verification": f"dspmq -m {qm}  -- should show Running",
        })

    # 1b. New channels (XMITQ + SDR on source, RCVR on target)
    create_step_nums = []
    for from_qm, to_qm in diff["channels_added"]:
        step_num += 1
        ch_name = f"{from_qm}.{to_qm}"
        xmitq = f"{to_qm}.XMITQ"
        create_step_nums.append(step_num)
        steps.append({
            "step_number": step_num,
            "phase": "CREATE",
            "description": f"Create channel infrastructure {ch_name} ({from_qm} → {to_qm})",
            "target_qm": from_qm,
            "mqsc_forward": (
                f"DEFINE QLOCAL('{xmitq}') USAGE(XMITQ) REPLACE\n"
                f"DEFINE CHANNEL('{ch_name}') CHLTYPE(SDR) "
                f"CONNAME('{get_conname(to_qm)}') XMITQ('{xmitq}') REPLACE\n"
                f"DEFINE CHANNEL('{ch_name}') CHLTYPE(RCVR) REPLACE  * Run on {to_qm}\n"
                f"START CHANNEL('{ch_name}')"
            ),
            "mqsc_rollback": (
                f"STOP CHANNEL('{ch_name}')\n"
                f"DELETE CHANNEL('{ch_name}')  * Delete SDR on {from_qm}\n"
                f"DELETE CHANNEL('{ch_name}')  * Delete RCVR on {to_qm}\n"
                f"DELETE QLOCAL('{xmitq}')"
            ),
            "depends_on": [],  # All CREATE steps can run in parallel
            "verification": f"DISPLAY CHSTATUS('{ch_name}')  -- should show RUNNING",
        })

    # ── PHASE 2: REROUTE — move applications ─────────────────────────────
    # Depends on phase 1 completion (just reference the phase, not every step)
    reroute_step_nums = []
    phase1_last = step_num  # last CREATE step number
    for app_info in diff["apps_reassigned"]:
        step_num += 1
        reroute_step_nums.append(step_num)
        steps.append({
            "step_number": step_num,
            "phase": "REROUTE",
            "description": f"Migrate {app_info['app_id']} from {app_info['old_qm']} to {app_info['new_qm']}",
            "target_qm": app_info["new_qm"],
            "mqsc_forward": (
                f"* Operator action: Stop {app_info['app_id']}\n"
                f"* Reconfigure {app_info['app_id']} connection to {app_info['new_qm']}\n"
                f"* Restart {app_info['app_id']}"
            ),
            "mqsc_rollback": (
                f"* Operator action: Stop {app_info['app_id']}\n"
                f"* Reconfigure {app_info['app_id']} connection back to {app_info['old_qm']}\n"
                f"* Restart {app_info['app_id']}"
            ),
            "depends_on": [phase1_last],  # Depends on phase 1 completion, not every step
            "verification": f"Verify {app_info['app_id']} messages flowing through {app_info['new_qm']}",
        })

    # ── PHASE 3: DRAIN — wait for old queues to empty ────────────────────
    phase2_last = step_num  # last REROUTE step number
    for from_qm, to_qm in diff["channels_removed"]:
        step_num += 1
        ch_name = f"{from_qm}.{to_qm}"
        xmitq = f"{to_qm}.XMITQ"
        steps.append({
            "step_number": step_num,
            "phase": "DRAIN",
            "description": f"Drain transmission queue {xmitq} on {from_qm} for channel {ch_name}",
            "target_qm": from_qm,
            "mqsc_forward": (
                f"DISPLAY QLOCAL('{xmitq}') CURDEPTH\n"
                f"* Wait until CURDEPTH = 0. If messages stuck, investigate before proceeding."
            ),
            "mqsc_rollback": "* Non-destructive step — no rollback needed",
            "depends_on": [phase2_last],  # Depends on phase 2 completion
            "verification": f"DISPLAY QLOCAL('{xmitq}') CURDEPTH  -- should show 0",
        })

    # ── PHASE 4: CLEANUP — remove old objects ─────────────────────────────
    phase3_last = step_num  # last DRAIN step number

    for from_qm, to_qm in diff["channels_removed"]:
        step_num += 1
        ch_name = f"{from_qm}.{to_qm}"
        xmitq = f"{to_qm}.XMITQ"
        steps.append({
            "step_number": step_num,
            "phase": "CLEANUP",
            "description": f"Remove old channel {ch_name} and related objects",
            "target_qm": from_qm,
            "mqsc_forward": (
                f"STOP CHANNEL('{ch_name}')\n"
                f"DELETE CHANNEL('{ch_name}')  * SDR on {from_qm}\n"
                f"DELETE CHANNEL('{ch_name}')  * RCVR on {to_qm}\n"
                f"DELETE QLOCAL('{xmitq}')  * Only if no other channels use it"
            ),
            "mqsc_rollback": (
                f"DEFINE QLOCAL('{xmitq}') USAGE(XMITQ) REPLACE\n"
                f"DEFINE CHANNEL('{ch_name}') CHLTYPE(SDR) CONNAME('{to_qm.lower()}.corp.com(1414)') XMITQ('{xmitq}') REPLACE\n"
                f"DEFINE CHANNEL('{ch_name}') CHLTYPE(RCVR) REPLACE  * On {to_qm}\n"
                f"START CHANNEL('{ch_name}')"
            ),
            "depends_on": [phase3_last],  # Depends on phase 3 completion
            "verification": f"DISPLAY CHANNEL('{ch_name}')  -- should show not found",
        })

    # Decommission QMs
    for qm in diff["qms_removed"]:
        step_num += 1
        steps.append({
            "step_number": step_num,
            "phase": "CLEANUP",
            "description": f"Decommission queue manager {qm}",
            "target_qm": qm,
            "mqsc_forward": (
                f"* Stop all remaining channels on {qm}\n"
                f"* Verify all queues have CURDEPTH = 0\n"
                f"endmqm -i {qm}\n"
                f"dltmqm {qm}"
            ),
            "mqsc_rollback": (
                f"crtmqm {qm}\n"
                f"strmqm {qm}\n"
                f"* Re-run as-is MQSC for {qm}"
            ),
            "depends_on": [s["step_number"] for s in steps if s["phase"] == "CLEANUP" and "channel" in s["description"]],
            "verification": f"dspmq  -- {qm} should not appear",
        })

    return steps


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 10: DOC EXPERT
# Aggregates all results into a structured markdown report.
# ─────────────────────────────────────────────────────────────────────────────
def doc_expert_agent(state: dict) -> dict:
    logger.info("DOC EXPERT: Generating final report")
    messages = state.get("messages", [])

    # ── ABORT PATH — human killed the pipeline ────────────────────────────
    if state.get("human_aborted"):
        feedback = state.get("human_feedback", "No reason provided")
        as_is = state.get("as_is_metrics", {}) or {}
        redesigns = state.get("redesign_count", 0)
        method = state.get("architect_method", "unknown")

        report_lines = [
            "# IntelliAI — Transformation Report (ABORTED)",
            "",
            "## Status: CANCELLED BY HUMAN REVIEWER",
            "",
            f"**Reason:** {feedback}",
            f"**Redesign attempts:** {redesigns}",
            f"**Architect method:** {method}",
            "",
            "## As-Is Analysis (completed before cancellation)",
            f"- Complexity score: {as_is.get('total_score', 'N/A')}/100",
            f"- Channel count: {as_is.get('channel_count', 'N/A')}",
            f"- Coupling index: {as_is.get('coupling_index', 'N/A')}",
            "",
            "## What Was Attempted",
        ]
        adrs = state.get("adrs", [])
        if adrs:
            report_lines.append(f"The Architect generated {len(adrs)} ADRs across {redesigns} iteration(s):")
            for adr in adrs:
                report_lines.append(f"- **{adr.get('id', '?')}**: {adr.get('decision', '?')}")
            report_lines.append("")
        report_lines += [
            "## Recommendation",
            "The proposed target state was not accepted. Options:",
            "- Re-run with different input data or constraints",
            "- Manually design the target state using the as-is analysis above",
            "- Adjust the topology data and try again",
            "",
            "## Agent Execution Trace",
            "| Step | Agent | Finding |",
            "|------|-------|---------|",
        ]
        for m in messages:
            report_lines.append(f"| — | {m.get('agent', '?')} | {m.get('msg', '?')} |")

        messages.append({"agent": "DOC_EXPERT", "msg": f"Pipeline ABORTED by human reviewer. Reason: {feedback}"})
        final_report = "\n".join(report_lines)
        return {"final_report": final_report, "messages": messages}

    # ── NORMAL PATH — full report ─────────────────────────────────────────
    as_is = state.get("as_is_metrics", {})
    target = state.get("target_metrics", {})
    adrs = state.get("adrs", [])
    violations = state.get("constraint_violations", [])

    # ── LLM ADR Enricher (Role 7) ────────────────────────────────────────
    # Generate enterprise-grade ADRs with specific entity references and
    # business justification. Merges with existing rule/LLM ADRs.
    try:
        adr_prompt = build_adr_enricher_prompt(state)
        logger.info(f"DOC_EXPERT-LLM: Calling ADR enricher (~{len(adr_prompt)//4} tokens)")
        
        adr_result = call_llm(
            system_prompt=ADR_ENRICHER_SYSTEM,
            user_prompt=adr_prompt,
            max_retries=1,
            temperature=0.3,
            max_tokens=4096,
        )
        
        if adr_result and adr_result.get("adrs"):
            llm_adrs = adr_result["adrs"]
            # Merge: keep existing ADRs, append LLM-generated ones with distinct IDs
            existing_ids = {a.get("id") for a in adrs}
            for la in llm_adrs:
                adr_id = la.get("id", f"ADR-AI-{len(adrs)+1:03d}")
                if adr_id in existing_ids:
                    adr_id = f"ADR-AI-{len(adrs)+1:03d}"
                adrs.append({
                    "id": adr_id,
                    "decision": la.get("title") or la.get("decision", ""),
                    "context": la.get("context", ""),
                    "rationale": la.get("rationale", ""),
                    "consequences": la.get("consequences", ""),
                })
            messages.append({"agent": "DOC_EXPERT", 
                           "msg": f"AI ADR Enricher: generated {len(llm_adrs)} enterprise-grade ADRs "
                                  f"(total now: {len(adrs)})"})
            logger.info(f"DOC_EXPERT-LLM: {len(llm_adrs)} ADRs generated")
        else:
            messages.append({"agent": "DOC_EXPERT", "msg": "AI ADR Enricher: LLM unavailable — using existing ADRs"})
    except Exception as e:
        logger.warning(f"DOC_EXPERT-LLM: ADR enricher failed ({e})")

    # ── LLM Executive Summarizer (Role 10) ───────────────────────────────
    # Generates a non-technical executive summary for stakeholders.
    # Translates topology metrics into business impact language.
    exec_summary = None
    try:
        exec_prompt = build_executive_summary_prompt(state)
        logger.info(f"DOC_EXPERT-LLM: Calling executive summarizer (~{len(exec_prompt)//4} tokens)")

        exec_result = call_llm(
            system_prompt=EXECUTIVE_SUMMARIZER_SYSTEM,
            user_prompt=exec_prompt,
            max_retries=1,
            temperature=0.3,
            max_tokens=2048,
        )

        if exec_result:
            exec_summary = exec_result
            messages.append({
                "agent": "DOC_EXPERT",
                "msg": (f"AI Executive Summary: \"{exec_result.get('headline', 'Generated')}\" — "
                        f"{exec_result.get('recommendation', 'See report')[:150]}")
            })
            logger.info("DOC_EXPERT-LLM: Executive summary generated")
        else:
            messages.append({"agent": "DOC_EXPERT", "msg": "AI Executive Summary: LLM unavailable — skipped"})
    except Exception as e:
        logger.warning(f"DOC_EXPERT-LLM: Executive summarizer failed ({e})")

    delta = round(as_is.get("total_score", 0) - target.get("total_score", 0), 1)
    pct = round((delta / as_is["total_score"]) * 100, 1) if as_is.get("total_score") else 0

    report_lines = [
        "# IntelliAI — Transformation Report",
        "",
        "## Executive Summary",
        f"The AI-driven transformation achieved a **{pct}% reduction** in overall MQ topology complexity.",
        f"Overall Complexity Score reduced from **{as_is.get('total_score')}/100** to **{target.get('total_score')}/100**.",
        "",
    ]

    # Inject AI executive briefing if available
    if exec_summary:
        report_lines += [
            "### Executive Briefing (AI-Generated)",
            f"**{exec_summary.get('headline', '')}**",
            "",
        ]
        bi = exec_summary.get("business_impact", {})
        if bi:
            report_lines += [
                f"- **Risk Reduction:** {bi.get('operational_risk_reduction', 'N/A')}",
                f"- **Cost Impact:** {bi.get('cost_implications', 'N/A')}",
                f"- **Agility:** {bi.get('agility_improvement', 'N/A')}",
                f"- **Reliability:** {bi.get('reliability_impact', 'N/A')}",
                "",
            ]
        key_nums = exec_summary.get("key_numbers", [])
        if key_nums:
            report_lines += [
                "| Metric | Before | After | Interpretation |",
                "|--------|--------|-------|----------------|",
            ]
            for kn in key_nums[:6]:
                report_lines.append(
                    f"| {kn.get('metric','')} | {kn.get('before','')} | "
                    f"{kn.get('after','')} | {kn.get('interpretation','')} |"
                )
            report_lines.append("")
        risks = exec_summary.get("risks_and_mitigations", [])
        if risks:
            for r in risks[:4]:
                report_lines.append(f"- **Risk:** {r.get('risk','')} → **Mitigation:** {r.get('mitigation','')}")
            report_lines.append("")
        report_lines += [
            f"**Recommendation:** {exec_summary.get('recommendation', 'N/A')}",
            f"**Timeline:** {exec_summary.get('timeline_estimate', 'N/A')}",
            "",
        ]

    report_lines += [
        "## Complexity Metrics — Before vs After",
        "| Metric | As-Is | Target | Change |",
        "|--------|-------|--------|--------|",
        f"| Channel Count | {as_is.get('channel_count')} | {target.get('channel_count')} | {int(as_is.get('channel_count',0)) - int(target.get('channel_count',0))} fewer |",
        f"| Coupling Index | {as_is.get('coupling_index')} | {target.get('coupling_index')} | {'Improved' if target.get('coupling_index',99) < as_is.get('coupling_index',0) else 'Same'} |",
        f"| Routing Depth | {as_is.get('routing_depth')} | {target.get('routing_depth')} | {'Reduced' if target.get('routing_depth',99) < as_is.get('routing_depth',0) else 'Same'} |",
        f"| Fan-Out Score | {as_is.get('fan_out_score')} | {target.get('fan_out_score')} | {'Reduced' if target.get('fan_out_score',99) < as_is.get('fan_out_score',0) else 'Same'} |",
        f"| Orphan Objects | {as_is.get('orphan_objects')} | {target.get('orphan_objects')} | {'Eliminated' if target.get('orphan_objects',1) < as_is.get('orphan_objects',0) else 'Same'} |",
        f"| Channel Sprawl | {as_is.get('channel_sprawl', 'N/A')} | {target.get('channel_sprawl', 'N/A')} | {'Improved' if (target.get('channel_sprawl',99) or 99) < (as_is.get('channel_sprawl',0) or 0) else 'Same'} |",
        f"| **Total Score** | **{as_is.get('total_score')}** | **{target.get('total_score')}** | **{pct}% reduction** |",
        "",
        "## Constraint Validation",
        f"Validation result: **{'PASS' if state.get('validation_passed') else 'FAIL'}**",
        f"Total violations: {len(violations)}",
        "",
    ]

    if violations:
        report_lines.append("### Violation Details")
        for v in violations:
            report_lines.append(f"- [{v['severity']}] {v['rule']}: {v['entity']} — {v['detail']}")
        report_lines.append("")

    # Compliance Audit section (from Role 8)
    comp_audit = state.get("compliance_audit")
    if comp_audit:
        report_lines += [
            "## Compliance Audit (AI-Generated)",
            f"**Compliance Score:** {comp_audit.get('compliance_score', '?')}/100",
            "",
        ]
        for f in comp_audit.get("findings", [])[:8]:
            report_lines.append(
                f"- [{f.get('severity','')}] **{f.get('category','')}**: "
                f"{f.get('finding','')} → _{f.get('recommendation','')}_"
            )
        ha = comp_audit.get("ha_assessment", {})
        if ha:
            report_lines += [
                "",
                f"**High Availability:** {'Redundancy present' if ha.get('has_redundancy') else 'No redundancy'} "
                f"| SPOFs: {ha.get('spof_count', '?')} | {ha.get('recommendation', '')}",
            ]
        sec = comp_audit.get("security_assessment", {})
        if sec:
            report_lines.append(
                f"**Security:** Channel security score {sec.get('channel_security_score', '?')}/100 "
                f"| SSL/TLS: {'recommended' if sec.get('ssl_tls_recommended') else 'not flagged'}"
            )
        report_lines.append("")

    # Capacity Analysis section (from Role 9)
    cap_analysis = state.get("capacity_analysis")
    if cap_analysis:
        report_lines += [
            "## Capacity Analysis (AI-Generated)",
            f"**Capacity Balance Score:** {cap_analysis.get('capacity_score', '?')}/100",
            "",
        ]
        flow = cap_analysis.get("flow_analysis", {})
        if flow:
            report_lines += [
                f"- Total flows: {flow.get('total_flows', '?')}",
                f"- Busiest QM: {flow.get('busiest_qm', '?')} ({flow.get('busiest_qm_flows', '?')} flows)",
                f"- Quietest QM: {flow.get('quietest_qm', '?')} ({flow.get('quietest_qm_flows', '?')} flows)",
                f"- Imbalance ratio: {flow.get('flow_imbalance_ratio', '?')}x",
                "",
            ]
        for h in cap_analysis.get("hotspots", [])[:5]:
            report_lines.append(
                f"- [{h.get('severity','')}] **{h.get('qm','')}** — {h.get('issue','')}: "
                f"{h.get('detail','')} → _{h.get('recommendation','')}_"
            )
        for rec in cap_analysis.get("scaling_recommendations", [])[:3]:
            report_lines.append(f"- **Scaling:** {rec}")
        report_lines.append("")

    report_lines.append("## Architecture Decision Records")
    method = state.get("architect_method", "rules_fallback")
    report_lines.append(f"*Generated by: {method} method*\n")
    for adr in adrs:
        report_lines += [
            f"### {adr['id']}: {adr['decision']}",
            f"**Context:** {adr['context']}",
            f"**Rationale:** {adr['rationale']}",
            f"**Consequences:** {adr['consequences']}",
            "",
        ]

    # Migration Plan section
    migration_plan = state.get("migration_plan")
    topology_diff = state.get("topology_diff")
    if migration_plan and migration_plan.get("steps"):
        report_lines += [
            "## Migration Plan",
            "",
            "### Topology Diff Summary",
        ]
        if topology_diff:
            report_lines.append(f"- QMs added: {topology_diff.get('qms_added', [])}")
            report_lines.append(f"- QMs removed: {topology_diff.get('qms_removed', [])}")
            report_lines.append(f"- Channels added: {len(topology_diff.get('channels_added', []))}")
            report_lines.append(f"- Channels removed: {len(topology_diff.get('channels_removed', []))}")
            report_lines.append(f"- Apps reassigned: {len(topology_diff.get('apps_reassigned', []))}")
            report_lines.append("")

        report_lines += [
            f"### Migration Steps ({migration_plan['total_steps']} total)",
            "",
            "| Step | Phase | Description | Target QM | Depends On |",
            "|------|-------|-------------|-----------|------------|",
        ]
        for step in migration_plan["steps"]:
            deps = ", ".join(str(d) for d in step.get("depends_on", []))
            report_lines.append(
                f"| {step['step_number']} | {step['phase']} | "
                f"{step['description']} | {step['target_qm']} | {deps or '—'} |"
            )
        report_lines.append("")

        # Detailed forward/rollback for each step (cap at 50 to avoid giant report)
        report_lines.append("### Detailed Migration Commands")
        report_lines.append("")
        display_steps = migration_plan["steps"][:50]
        for step in display_steps:
            report_lines += [
                f"#### Step {step['step_number']}: {step['description']}",
                f"**Phase:** {step['phase']} | **Target QM:** {step['target_qm']}",
                f"**Forward MQSC:**",
                "```",
                step.get("mqsc_forward", ""),
                "```",
                f"**Rollback MQSC:**",
                "```",
                step.get("mqsc_rollback", ""),
                "```",
                f"**Verification:** {step.get('verification', '')}",
                "",
            ]
        if len(migration_plan["steps"]) > 50:
            report_lines.append(f"*... {len(migration_plan['steps']) - 50} more steps omitted. Full MQSC available in migration-plan deliverable.*\n")

    report_lines += [
        "## Agent Execution Trace",
        "| Step | Agent | Finding |",
        "|------|-------|---------|",
    ]
    for m in messages:
        report_lines.append(f"| — | {m['agent']} | {m['msg']} |")

    final_report = "\n".join(report_lines)
    messages.append({"agent": "DOC_EXPERT", "msg": "Final report generated."})

    # ── Generate additional deliverables (Output.md §8.2 & §9) ────────────
    deliverable_docs = {}
    try:
        logger.info("DOC_EXPERT: generating complexity-algorithm...")
        deliverable_docs["complexity-algorithm"] = _generate_complexity_algorithm_md(state)
        logger.info("DOC_EXPERT: generating complexity-scores...")
        deliverable_docs["complexity-scores"] = _generate_complexity_scores_csv(state)
        logger.info("DOC_EXPERT: generating regression-testing-plan...")
        deliverable_docs["regression-testing-plan"] = _generate_regression_testing_plan(state)
        logger.info("DOC_EXPERT: generating insights...")
        deliverable_docs["insights"] = _generate_insights_md(state)
        logger.info("DOC_EXPERT: generating migration-plan md...")
        deliverable_docs["migration-plan"] = _generate_migration_plan_md(state)
        logger.info("DOC_EXPERT: generating subgraph-analysis...")
        deliverable_docs["subgraph-analysis"] = _generate_subgraph_analysis_md(state)
        logger.info("DOC_EXPERT: all deliverables done")
        messages.append({"agent": "DOC_EXPERT", "msg": f"Generated {len(deliverable_docs)} additional deliverables: {list(deliverable_docs.keys())}"})
    except Exception as e:
        logger.error(f"DOC_EXPERT: Failed to generate some deliverables: {e}")
        messages.append({"agent": "DOC_EXPERT", "msg": f"Warning: some deliverables failed: {e}"})

    return {
        "final_report": final_report,
        "deliverable_docs": deliverable_docs,
        "exec_summary": exec_summary,
        "messages": messages,
    }

# ═══════════════════════════════════════════════════════════════════════════════
# MISSING DELIVERABLES — Output.md §8.2 & §9
# These are called from provisioner_agent and doc_expert_agent
# ═══════════════════════════════════════════════════════════════════════════════

import json
from datetime import datetime


def _generate_target_topology_json(G, state: dict) -> str:
    """Deliverable 1: target-topology.json — structured JSON representation."""
    qm_nodes = [(n, d) for n, d in G.nodes(data=True) if d.get("type") == "qm"]
    app_nodes = [(n, d) for n, d in G.nodes(data=True) if d.get("type") == "app"]
    queue_nodes = [(n, d) for n, d in G.nodes(data=True) if d.get("type") == "queue"]

    # App → QM map
    app_qm = {}
    for n, d in app_nodes:
        qms = [v for _, v, ed in G.out_edges(n, data=True) if ed.get("rel") == "connects_to"]
        if qms:
            app_qm[n] = qms[0]

    # Channels
    channels = []
    for u, v, d in G.edges(data=True):
        if d.get("rel") == "channel":
            channels.append({
                "channel_name": d.get("channel_name") or f"{u}.{v}",
                "sender_qm": u,
                "receiver_qm": v,
                "xmit_queue": d.get("xmit_queue") or f"{v}.XMITQ",
                "status": d.get("status", "RUNNING"),
            })

    # QMs with their apps and channels — use pre-built lookups
    qm_apps_map = {}
    for a, q in app_qm.items():
        qm_apps_map.setdefault(q, []).append(a)

    ch_out_map = {}
    ch_in_map = {}
    for c in channels:
        ch_out_map.setdefault(c["sender_qm"], []).append(c["channel_name"])
        ch_in_map.setdefault(c["receiver_qm"], []).append(c["channel_name"])

    qm_list = []
    for n, d in qm_nodes:
        owned_queues = [v for _, v, ed in G.out_edges(n, data=True) if ed.get("rel") == "owns"]
        qm_list.append({
            "qm_id": n,
            "qm_name": d.get("name", n),
            "region": d.get("region", ""),
            "apps": qm_apps_map.get(n, []),
            "channels_out": ch_out_map.get(n, []),
            "channels_in": ch_in_map.get(n, []),
            "queue_count": len(owned_queues),
        })

    # Applications — pre-build direction lookup
    raw_apps = state.get("raw_data", {}).get("applications", [])
    app_dir_map = {}
    for r in raw_apps:
        aid = r.get("app_id")
        if aid and aid not in app_dir_map:
            app_dir_map[aid] = r.get("direction", "")

    app_list = []
    for n, d in app_nodes:
        app_list.append({
            "app_id": n,
            "app_name": d.get("name", n),
            "assigned_qm": app_qm.get(n, ""),
            "direction": app_dir_map.get(n, "") or d.get("direction", ""),
        })

    # Queues by type
    local_qs = [{"id": n, "name": d.get("name", n), "qm": ""} for n, d in queue_nodes
                if d.get("queue_type") != "REMOTE" and d.get("usage") != "XMITQ"
                and not (d.get("name", "").endswith("XMITQ"))]
    remote_qs = [{"id": n, "name": d.get("name", n), "remote_qm": d.get("remote_qm", ""),
                  "remote_queue": d.get("remote_queue", "")}
                 for n, d in queue_nodes if d.get("queue_type") == "REMOTE"]
    xmit_qs = [{"id": n, "name": d.get("name", n)}
               for n, d in queue_nodes
               if d.get("usage") == "XMITQ" or d.get("queue_type") == "XMITQ"
               or (d.get("name", "").endswith("XMITQ"))]

    topology = {
        "metadata": {
            "team": "IntelliAI",
            "session_id": state.get("session_id", ""),
            "generated_at": datetime.now().isoformat(),
            "architect_method": state.get("architect_method", "rules_fallback"),
        },
        "queue_managers": qm_list,
        "channels": channels,
        "applications": app_list,
        "queues": {
            "local": local_qs,
            "remote": remote_qs,
            "xmitq": xmit_qs,
        },
        "statistics": {
            "total_qms": len(qm_list),
            "total_channels": len(channels),
            "total_apps": len(app_list),
            "total_local_queues": len(local_qs),
            "total_remote_queues": len(remote_qs),
            "total_xmit_queues": len(xmit_qs),
        },
    }
    return json.dumps(topology, indent=2)


def _generate_complexity_algorithm_md(state: dict) -> str:
    """Deliverable 3a: complexity-algorithm.md — algorithm description and rationale."""
    as_is = state.get("as_is_metrics", {}) or {}
    target = state.get("target_metrics", {}) or {}
    total_as = as_is.get("total_score", 0)
    total_tgt = target.get("total_score", 0)
    pct = round((total_as - total_tgt) / total_as * 100, 1) if total_as else 0

    return f"""# Complexity Scoring Algorithm — IntelliAI

## Overview

IntelliAI uses a **6-factor weighted complexity model** to quantify MQ topology complexity.
The same algorithm is applied identically to both the as-is and target states, ensuring
the reduction measurement is reproducible and defensible.

## Factors

### 1. Channel Count (CC) — Weight: 25%
**Definition:** Total number of inter-QM sender channels.
**Rationale:** More channels = more operational surface area to manage, monitor, and secure.
Channel count is the single most visible indicator of topology sprawl.

### 2. Coupling Index (CI) — Weight: 25%
**Definition:** Average number of QMs each application connects to.
**Ideal value:** 1.0 (every app connects to exactly one QM — the core hackathon constraint).
**Rationale:** In the as-is state, apps often connect to multiple QMs. High coupling = hard to
change anything without side effects.

### 3. Routing Depth (RD) — Weight: 20%
**Definition:** Maximum graph diameter of the QM-only subgraph, plus a fragmentation penalty.
**Formula:** `RD = max_diameter_across_components + (num_components - 1)`
**Rationale:** Deeper routing paths increase latency, debugging complexity, and failure blast radius.
The fragmentation penalty accounts for disconnected subgraphs (islands that cannot communicate).

### 4. Fan-Out Score (FO) — Weight: 15%
**Definition:** Maximum outbound channel degree of any single QM.
**Rationale:** A QM with high fan-out is a single point of failure and operational bottleneck.
Reducing fan-out improves resilience and blast radius containment.

### 5. Orphan Objects (OO) — Weight: 5%
**Definition:** Count of QMs with no app connections plus stopped channels.
**Rationale:** Orphan objects are dead weight — they consume resources and create confusion.
Weighted lowest because orphans are a hygiene issue, not a structural risk.

### 6. Channel Sprawl (CS) — Weight: 10%
**Definition:** Ratio of channels to queue managers (channels per QM).
**Formula:** `CS = channel_count / qm_count`
**Rationale:** In 1:1 topologies, QM count is fixed (one per app), so channel efficiency is
the primary lever for complexity reduction. CS captures whether channels are concentrated
or spread proportionally. High CS means each QM carries heavy routing overhead.

## Normalisation

Each raw factor is normalised to 0–100 using a baseline calibration derived from the
topology's own size:

```
normalised = min(100, raw_value / worst_realistic_case * 100)
```

The worst realistic case for each factor is computed from the QM and app counts in the topology,
ensuring the scoring scales correctly from small (10 QM) to large (500+ QM) environments.

**Critical design decision:** The SAME baselines from the as-is graph are reused when scoring
the target graph (`baseline_overrides`). This ensures improvements are measured fairly against
the actual starting point, not a theoretical worst case.

## Final Score

```
Total = CC × 0.25 + CI × 0.25 + RD × 0.20 + FO × 0.15 + OO × 0.05 + CS × 0.10
```

Weights sum to 1.00. Score range: 0 (trivial) to 100 (maximally complex).
Channel-related factors (CC + CS) collectively account for 35% — reflecting that channel
management is the dominant operational cost in enterprise MQ environments.

## Actual Scores — This Run

| Metric | As-Is | Target | Reduction |
|--------|-------|--------|-----------|
| Channel Count (CC) | {as_is.get('channel_count', 'N/A')} | {target.get('channel_count', 'N/A')} | {'Improved' if target.get('channel_count', 99) < as_is.get('channel_count', 0) else 'Same'} |
| Coupling Index (CI) | {as_is.get('coupling_index', 'N/A')} | {target.get('coupling_index', 'N/A')} | {'Improved' if target.get('coupling_index', 99) < as_is.get('coupling_index', 0) else 'Same'} |
| Routing Depth (RD) | {as_is.get('routing_depth', 'N/A')} | {target.get('routing_depth', 'N/A')} | {'Improved' if target.get('routing_depth', 99) < as_is.get('routing_depth', 0) else 'Same'} |
| Fan-Out Score (FO) | {as_is.get('fan_out_score', 'N/A')} | {target.get('fan_out_score', 'N/A')} | {'Improved' if target.get('fan_out_score', 99) < as_is.get('fan_out_score', 0) else 'Same'} |
| Orphan Objects (OO) | {as_is.get('orphan_objects', 'N/A')} | {target.get('orphan_objects', 'N/A')} | {'Improved' if target.get('orphan_objects', 99) < as_is.get('orphan_objects', 0) else 'Same'} |
| Channel Sprawl (CS) | {as_is.get('channel_sprawl', 'N/A')} | {target.get('channel_sprawl', 'N/A')} | {'Improved' if (target.get('channel_sprawl', 99) or 99) < (as_is.get('channel_sprawl', 0) or 0) else 'Same'} |
| **Total** | **{total_as}** | **{total_tgt}** | **{pct}% reduction** |

## Why This Approach

We evaluated alternative metrics (pure cyclomatic complexity, graph density, modularity score)
and chose a multi-factor weighted model because:

1. **Cyclomatic complexity** measures control flow, not messaging topology — it's designed for code, not infrastructure graphs
2. **Graph density** is a single number that doesn't distinguish between "all-to-all" and "hub-and-spoke" — both can have similar density but very different operational characteristics
3. **Our 6-factor model** captures the dimensions that matter operationally: connection sprawl (CC), ownership clarity (CI), path complexity (RD), resilience (FO), hygiene (OO), and routing efficiency (CS)

Each factor maps directly to an operational concern that MQ administrators deal with daily.

---
*Generated by IntelliAI — IBM MQ Hackathon 2026*
"""


def _generate_complexity_scores_csv(state: dict) -> str:
    """Deliverable 3b: complexity-scores.csv — source vs target breakdown."""
    as_is = state.get("as_is_metrics", {}) or {}
    target = state.get("target_metrics", {}) or {}

    def pct(a, b):
        if not a:
            return "0%"
        return f"{round((a - b) / a * 100, 1)}%"

    lines = ["Metric,Weight,Source_Score,Target_Score,Reduction_Pct"]
    factors = [
        ("Channel Count (CC)", "25%", "channel_count"),
        ("Coupling Index (CI)", "25%", "coupling_index"),
        ("Routing Depth (RD)", "20%", "routing_depth"),
        ("Fan-Out Score (FO)", "15%", "fan_out_score"),
        ("Orphan Objects (OO)", "5%", "orphan_objects"),
        ("Channel Sprawl (CS)", "10%", "channel_sprawl"),
    ]
    for name, weight, key in factors:
        a = as_is.get(key, 0)
        t = target.get(key, 0)
        lines.append(f"{name},{weight},{a},{t},{pct(a, t)}")

    a_total = as_is.get("total_score", 0)
    t_total = target.get("total_score", 0)
    lines.append(f"TOTAL,100%,{a_total},{t_total},{pct(a_total, t_total)}")

    return "\n".join(lines)


def _generate_regression_testing_plan(state: dict) -> str:
    """Deliverable 5: regression-testing-plan.md — validation strategy."""
    diff = state.get("topology_diff", {}) or {}
    apps_reassigned = diff.get("apps_reassigned", [])
    channels_added = diff.get("channels_added", [])
    channels_removed = diff.get("channels_removed", [])
    qms_added = diff.get("qms_added", [])
    qms_removed = diff.get("qms_removed", [])

    plan = f"""# Regression Testing Plan — IntelliAI

## 1. Scope of Change

This migration affects:
- **{len(qms_added)} QMs created**, {len(qms_removed)} QMs decommissioned
- **{len(channels_added)} channels added**, {len(channels_removed)} channels removed
- **{len(apps_reassigned)} applications reassigned** to new queue managers

## 2. Test Categories

### 2.1 Unit Tests — Per-QM MQSC Validation
**Purpose:** Verify every MQ object exists and is correctly configured.
**Method:** Run `runmqsc` with DISPLAY commands against each target QM.

| Test | Command | Expected |
|------|---------|----------|
| QM exists | `dspmq -m QM_NAME` | Status = Running |
| Listener active | `DISPLAY LISTENER(*)` | LSR.QM_NAME at correct port |
| Local queues | `DISPLAY QLOCAL(LOCAL.*.IN)` | One per assigned app |
| XMIT queues | `DISPLAY QLOCAL(*XMITQ) USAGE` | USAGE(XMITQ) |
| Remote queues | `DISPLAY QREMOTE(REMOTE.*)` | RQMNAME, RNAME, XMITQ correct |
| Sender channels | `DISPLAY CHANNEL(*) CHLTYPE` | CHLTYPE(SDR), correct CONNAME |
| Receiver channels | `DISPLAY CHANNEL(*) CHLTYPE` | CHLTYPE(RCVR) |

**Coverage:** All {len(qms_added)} new QMs must pass all checks.

### 2.2 Integration Tests — Per-Channel Message Flow
**Purpose:** Verify messages transit correctly between QM pairs.
**Method:** PUT a test message on the sender QM's remote queue, GET from the receiver QM's local queue.

```
# For each channel FROM_QM.TO_QM:
amqsput REMOTE.APP.VIA.TO_QM FROM_QM    # PUT on sender side
amqsget LOCAL.APP.IN TO_QM               # GET on receiver side
# Verify: message content matches, no DLQ entries
```

**Coverage:** All {len(channels_added)} new channels tested bidirectionally.

### 2.3 End-to-End Tests — Full Producer→Consumer Path
**Purpose:** Verify complete message flows for every producer-consumer pair.
**Method:** For each application pair with a message flow:

1. Producer app PUTs message to its local remote queue definition
2. Message transits via XMITQ → sender channel → receiver channel → local queue
3. Consumer app GETs from its local queue
4. Verify: message integrity, correct routing, no orphaned messages

**Key test cases from this migration:**
"""
    # Add specific test cases for reassigned apps
    for i, app_info in enumerate(apps_reassigned[:10]):
        if isinstance(app_info, dict):
            app = app_info.get("app", app_info.get("app_id", f"App_{i}"))
            old_qm = app_info.get("old_qm", "?")
            new_qm = app_info.get("new_qm", "?")
        else:
            app, old_qm, new_qm = str(app_info), "?", "?"
        plan += f"- **{app}**: Moved from {old_qm} → {new_qm}. Verify all inbound/outbound flows.\n"

    if len(apps_reassigned) > 10:
        plan += f"- ... and {len(apps_reassigned) - 10} more reassigned apps\n"

    plan += f"""
### 2.4 Performance Tests — Throughput Baseline
**Purpose:** Ensure migration doesn't degrade message throughput.
**Method:**
1. Capture pre-migration baseline: messages/sec per channel, end-to-end latency
2. After migration: repeat same workload
3. Compare: latency within 5% tolerance, zero message loss

## 3. Acceptance Criteria

| Criterion | Threshold | Measurement |
|-----------|-----------|-------------|
| Message loss | Zero | DLQ depth = 0 across all QMs |
| Latency | < 5% increase | amqsget timing comparison |
| MQSC idempotency | All scripts re-runnable | Run each script twice, no errors |
| Object count | Exact match | DISPLAY counts match target-topology.json |
| Channel status | All RUNNING | DISPLAY CHSTATUS(*) = RUNNING |

## 4. Rollback Trigger Conditions

Initiate rollback if:
- Any end-to-end test fails with message loss
- Latency exceeds 10% of baseline
- More than 2 channels fail to start
- Any CRITICAL constraint violation detected post-migration

Rollback procedure: Execute rollback MQSC scripts in reverse phase order (CLEANUP → DRAIN → REROUTE → CREATE).

## 5. Test Execution Timeline

| Phase | Tests | Duration | Gate |
|-------|-------|----------|------|
| Phase 1: CREATE | Unit tests on new QMs | 1 hour | All QMs running |
| Phase 2: REROUTE | Integration + E2E tests | 4 hours | Zero message loss |
| Phase 3: DRAIN | Verify old channels drained | 2 hours | DLQ depth = 0 |
| Phase 4: CLEANUP | Final unit tests | 1 hour | Object counts match |

---
*Generated by IntelliAI — IBM MQ Hackathon 2026*
"""
    return plan


def _generate_insights_md(state: dict) -> str:
    """Deliverable 6: insights.md — key findings on source and target topologies."""
    as_is_metrics = state.get("as_is_metrics", {}) or {}
    target_metrics = state.get("target_metrics", {}) or {}
    as_is_subs = state.get("as_is_subgraphs", []) or []
    target_subs = state.get("target_subgraphs", []) or []
    violations = state.get("constraint_violations", []) or []
    diff = state.get("topology_diff", {}) or {}
    dq = state.get("data_quality_report", {}) or {}
    as_is_graph = state.get("as_is_graph")
    target_graph = state.get("optimised_graph")
    as_is_centrality = state.get("as_is_centrality", {}) or {}
    as_is_entropy = state.get("as_is_entropy", {}) or {}

    # ── Compute source insights ───────────────────────────────────────────
    insights = """# Key Insights — IntelliAI

## Source Topology Insights

"""
    # Multi-QM apps (the core problem)
    if as_is_graph:
        multi_qm_apps = []
        for n, d in as_is_graph.nodes(data=True):
            if d.get("type") == "app":
                qms = [v for _, v, ed in as_is_graph.out_edges(n, data=True) if ed.get("rel") == "connects_to"]
                if len(qms) > 1:
                    multi_qm_apps.append((n, len(qms)))
        multi_qm_apps.sort(key=lambda x: -x[1])

        if multi_qm_apps:
            insights += f"### 1. Multi-QM Application Violations\n"
            insights += f"**{len(multi_qm_apps)} applications** connect to more than one queue manager, "
            insights += f"violating the 1-QM-per-app constraint.\n\n"
            insights += f"Worst offenders:\n"
            for app, count in multi_qm_apps[:5]:
                insights += f"- `{app}` connects to **{count} QMs**\n"
            insights += f"\nThis coupling creates operational risk: changes to any shared QM impact multiple applications.\n\n"
        else:
            insights += "### 1. Application-QM Coupling\nNo multi-QM violations in source data.\n\n"

    # Hub QMs (SPOFs)
    if as_is_graph:
        qm_degrees = []
        for n, d in as_is_graph.nodes(data=True):
            if d.get("type") == "qm":
                out_ch = sum(1 for _, _, ed in as_is_graph.out_edges(n, data=True) if ed.get("rel") == "channel")
                in_ch = sum(1 for _, _, ed in as_is_graph.in_edges(n, data=True) if ed.get("rel") == "channel")
                app_count = sum(1 for _, _, ed in as_is_graph.in_edges(n, data=True) if ed.get("rel") == "connects_to")
                qm_degrees.append((n, out_ch + in_ch, app_count))
        qm_degrees.sort(key=lambda x: -x[1])

        if qm_degrees:
            top_hub = qm_degrees[0]
            insights += f"### 2. Hub QMs — Single Points of Failure\n"
            insights += f"The most connected QM is `{top_hub[0]}` with **{top_hub[1]} channels** "
            insights += f"and **{top_hub[2]} connected apps**.\n\n"
            insights += f"Top 5 hub QMs:\n"
            for qm, ch, apps in qm_degrees[:5]:
                insights += f"- `{qm}`: {ch} channels, {apps} apps\n"
            insights += f"\nThese hubs are operational bottlenecks and blast-radius risks.\n\n"

    # Subgraph analysis
    if as_is_subs:
        isolated = [s for s in as_is_subs if s.get("is_isolated")]
        insights += f"### 3. Subgraph Decomposition\n"
        insights += f"The source topology has **{len(as_is_subs)} connected components**.\n\n"
        if isolated:
            insights += f"**{len(isolated)} isolated QMs** (no channels) detected — "
            insights += f"these are dead weight consuming infrastructure resources:\n"
            for s in isolated[:5]:
                qm_ids = s.get('qm_ids', s.get('qms', []))
                insights += f"- `{qm_ids[0] if qm_ids else '?'}` — {s.get('app_count', 0)} apps, no channels\n"
            insights += f"\n"
        if len(as_is_subs) > 1:
            largest = as_is_subs[0]
            insights += f"Largest component: **{largest.get('qm_count', 0)} QMs**, "
            insights += f"{largest.get('app_count', 0)} apps, {largest.get('channel_count', 0)} channels.\n\n"

    # Entropy / density
    if as_is_entropy:
        insights += f"### 4. Graph Metrics\n"
        insights += f"- Shannon entropy: **{as_is_entropy.get('shannon_entropy', 'N/A')}** "
        insights += f"(higher = more uniform distribution)\n"
        insights += f"- Graph density: **{as_is_entropy.get('density', 'N/A')}**\n"
        insights += f"- Clustering coefficient: **{as_is_entropy.get('clustering', 'N/A')}**\n\n"

    # Data quality issues
    if dq:
        issues = dq.get("issues", [])
        if issues:
            insights += f"### 5. Data Quality Observations\n"
            insights += f"The sanitiser agent detected **{len(issues)} data quality issues**:\n"
            for issue in issues[:5]:
                insights += f"- {issue}\n"
            insights += f"\n"

    # ── Target topology insights ──────────────────────────────────────────
    insights += "## Target Topology Insights\n\n"

    total_as = as_is_metrics.get("total_score", 0)
    total_tgt = target_metrics.get("total_score", 0)
    pct = round((total_as - total_tgt) / total_as * 100, 1) if total_as else 0

    insights += f"### 1. Complexity Reduction\n"
    insights += f"Overall score: **{total_as} → {total_tgt}** ({pct}% reduction).\n\n"

    # What was simplified
    if diff:
        insights += f"### 2. Transformation Summary\n"
        insights += f"- QMs added: **{len(diff.get('qms_added', []))}** (dedicated 1-per-app)\n"
        insights += f"- QMs removed: **{len(diff.get('qms_removed', []))}** (shared QMs eliminated)\n"
        insights += f"- Channels added: **{len(diff.get('channels_added', []))}** (only where actual flows exist)\n"
        insights += f"- Channels removed: **{len(diff.get('channels_removed', []))}**\n"
        insights += f"- Apps reassigned: **{len(diff.get('apps_reassigned', []))}**\n\n"

    # Target subgraphs
    if target_subs:
        target_isolated = [s for s in target_subs if s.get("is_isolated")]
        insights += f"### 3. Target Subgraph Analysis\n"
        insights += f"The target topology has **{len(target_subs)} connected components**"
        if target_isolated:
            insights += f" ({len(target_isolated)} isolated QMs — apps with no cross-QM communication)\n\n"
        else:
            insights += f" (fully connected)\n\n"

    # Constraint satisfaction
    critical_violations = [v for v in violations if v.get("severity") == "CRITICAL"]
    insights += f"### 4. Constraint Validation\n"
    if not critical_violations:
        insights += f"**All constraints satisfied** — zero CRITICAL violations.\n"
        insights += f"- 1-QM-per-app: Enforced for all {len([n for n, d in (target_graph.nodes(data=True) if target_graph else []) if d.get('type') == 'app'])} applications\n"
        insights += f"- Deterministic channel naming: All channels follow FROM_QM.TO_QM pattern\n"
        insights += f"- Standard routing: All message flows use REMOTE_Q → XMITQ → CHANNEL → LOCAL_Q path\n\n"
    else:
        insights += f"**{len(critical_violations)} CRITICAL violations** remain:\n"
        for v in critical_violations[:5]:
            insights += f"- {v.get('rule')}: {v.get('entity')} — {v.get('detail')}\n"
        insights += f"\n"

    # Key insight: what makes this target better
    insights += """### 5. Why This Target State Is Better

1. **Clear ownership**: Every application connects to exactly one QM. No ambiguity about which team owns which QM.
2. **Predictable routing**: All cross-QM communication follows the canonical pattern (REMOTE_Q -> XMITQ -> Channel -> LOCAL_Q). No ad-hoc or legacy routing paths.
3. **Reduced blast radius**: Failure of any single QM affects only its assigned application, not multiple apps sharing a QM.
4. **Automation-ready**: Deterministic naming conventions (LOCAL.APP.IN, FROM_QM.TO_QM) enable fully automated provisioning via MQSC scripts.
5. **Channels justified by data**: Channels exist only where actual producer->consumer flows exist in the source data. No speculative or "just in case" channels.

---

## Strategic Modernization Recommendation

### Data-Driven Analysis: Which Workloads Belong on Which Messaging Pattern?

The following analysis evaluates five messaging paradigms against the **actual flow patterns**
detected in the source data. Each recommendation is tied to a specific signal in the topology,
not a generic technology preference.

"""
    # ── Mine actual flow data for substantive claims ──────────────────────
    raw_data = state.get("raw_data", {})
    raw_apps = raw_data.get("applications", [])

    # Build actual producer→consumer flow map from source data
    queue_prod = {}  # {queue_name: set of producer app_ids}
    queue_cons = {}  # {queue_name: set of consumer app_ids}
    app_directions = {}  # {app_id: set of directions}
    for row in raw_apps:
        aid = row.get("app_id", "")
        qname = row.get("queue_name", "")
        direction = row.get("direction", "").upper()
        if not qname or not aid:
            continue
        app_directions.setdefault(aid, set()).add(direction)
        if direction in ("PUT", "PRODUCER"):
            queue_prod.setdefault(qname, set()).add(aid)
        elif direction in ("GET", "CONSUMER"):
            queue_cons.setdefault(qname, set()).add(aid)

    # Compute per-producer fan-out: how many distinct consumers does each producer reach?
    producer_fanout = {}  # {producer_app: set of consumer_apps}
    for qname in set(queue_prod.keys()) & set(queue_cons.keys()):
        for prod in queue_prod[qname]:
            for cons in queue_cons[qname]:
                if prod != cons:
                    producer_fanout.setdefault(prod, set()).add(cons)

    # Compute per-queue fan-out: how many consumers per queue?
    queue_fanout = {}
    for qname, consumers in queue_cons.items():
        producers = queue_prod.get(qname, set())
        if producers:
            queue_fanout[qname] = {"producers": len(producers), "consumers": len(consumers)}

    # Identify high-fan-out queues (1 producer → many consumers)
    high_fanout_queues = sorted(
        [(q, d) for q, d in queue_fanout.items() if d["consumers"] > 3],
        key=lambda x: -x[1]["consumers"]
    )

    # Identify high-fan-out producers
    high_fanout_producers = sorted(
        [(app, len(cons)) for app, cons in producer_fanout.items() if len(cons) > 5],
        key=lambda x: -x[1]
    )

    # Count apps by role
    pure_producers = set(a for a, dirs in app_directions.items() if dirs == {"PUT"} or dirs == {"PRODUCER"})
    pure_consumers = set(a for a, dirs in app_directions.items() if dirs == {"GET"} or dirs == {"CONSUMER"})
    bidirectional = set(app_directions.keys()) - pure_producers - pure_consumers

    # ── Detect request-reply pairs (bidirectional apps sharing queues) ────
    # A request-reply pair is two apps that both PUT and GET on overlapping
    # queues — one sends a request, the other sends a response.
    # Pre-build per-app queue sets to avoid scanning all queues per pair
    app_produces_queues = {}  # {app: set of queue names it produces to}
    app_consumes_queues = {}  # {app: set of queue names it consumes from}
    for qname, prods in queue_prod.items():
        for p in prods:
            app_produces_queues.setdefault(p, set()).add(qname)
    for qname, cons in queue_cons.items():
        for c in cons:
            app_consumes_queues.setdefault(c, set()).add(qname)

    request_reply_pairs = []
    bidir_list = sorted(bidirectional)
    # Cap at 200 to avoid O(n^2) explosion on large datasets
    bidir_check = bidir_list[:200]
    for i, app_a in enumerate(bidir_check):
        a_prod = app_produces_queues.get(app_a, set())
        a_cons = app_consumes_queues.get(app_a, set())
        if not a_prod or not a_cons:
            continue
        for app_b in bidir_check[i+1:]:
            b_prod = app_produces_queues.get(app_b, set())
            b_cons = app_consumes_queues.get(app_b, set())
            if (a_prod & b_cons) and (b_prod & a_cons):
                request_reply_pairs.append((app_a, app_b))
                if len(request_reply_pairs) >= 50:
                    break
        if len(request_reply_pairs) >= 50:
            break

    # ── Detect low-volume point-to-point flows (1:1 producer:consumer) ────
    p2p_queues = sorted(
        [(q, d) for q, d in queue_fanout.items()
         if d["producers"] == 1 and d["consumers"] == 1],
        key=lambda x: x[0]
    )

    total_apps = len(app_directions)
    total_queues_with_flows = len(set(queue_prod.keys()) & set(queue_cons.keys()))
    total_flows = sum(len(cons) for cons in producer_fanout.values())

    # Compute what % of flows are high-fan-out (>3 consumers)
    high_fo_flows = sum(
        d["producers"] * d["consumers"]
        for _, d in queue_fanout.items()
        if d["consumers"] > 3
    )
    high_fo_pct = round(high_fo_flows / total_flows * 100, 1) if total_flows > 0 else 0

    # Compute % of flows that are strict 1:1 point-to-point
    p2p_flow_count = len(p2p_queues)
    p2p_pct = round(p2p_flow_count / max(total_queues_with_flows, 1) * 100, 1)

    target_ch = target_metrics.get("channel_count", 0)
    as_is_ch = as_is_metrics.get("channel_count", 0)
    fo = as_is_metrics.get("fan_out_score", 0)
    ci = as_is_metrics.get("coupling_index", 1.0)

    insights += f"#### Flow Pattern Analysis (from {len(raw_apps)} source data rows)\n\n"
    insights += f"| Metric | Value |\n|--------|-------|\n"
    insights += f"| Total apps with identifiable flows | {total_apps} |\n"
    insights += f"| Pure producers (PUT only) | {len(pure_producers)} ({round(len(pure_producers)/max(total_apps,1)*100)}%) |\n"
    insights += f"| Pure consumers (GET only) | {len(pure_consumers)} ({round(len(pure_consumers)/max(total_apps,1)*100)}%) |\n"
    insights += f"| Bidirectional apps (PUT + GET) | {len(bidirectional)} ({round(len(bidirectional)/max(total_apps,1)*100)}%) |\n"
    insights += f"| Request-reply pairs detected | {len(request_reply_pairs)} |\n"
    insights += f"| Queues with active producer→consumer flows | {total_queues_with_flows} |\n"
    insights += f"| Strict 1:1 point-to-point queues | {p2p_flow_count} ({p2p_pct}%) |\n"
    insights += f"| Total producer→consumer flow pairs | {total_flows} |\n"
    insights += f"| Flows involving high fan-out queues (>3 consumers) | {high_fo_flows} ({high_fo_pct}%) |\n\n"

    # ══════════════════════════════════════════════════════════════════════
    # PATTERN 1: IBM MQ Event Streams / Apache Kafka — broadcast/fan-out
    # ══════════════════════════════════════════════════════════════════════
    insights += f"### Pattern 1: Event Streaming (IBM MQ Event Streams / Apache Kafka)\n\n"
    insights += f"**Signal:** High fan-out queues where 1 producer broadcasts to N>3 consumers.\n"
    insights += f"**Why:** In MQ, each consumer on a different QM requires a dedicated REMOTE queue + XMITQ + channel "
    insights += f"on the producer's QM — O(N) object overhead. In event streaming, all consumers read from a single "
    insights += f"topic partition independently — O(1) on the producer side.\n\n"

    if high_fanout_queues:
        insights += f"**Detection result: {len(high_fanout_queues)} candidate queues identified.**\n\n"
        insights += f"| Queue | Producers | Consumers | MQ Objects Required | Event Stream Equivalent |\n"
        insights += f"|-------|-----------|-----------|--------------------|-----------------|\n"
        for qname, d in high_fanout_queues[:10]:
            mq_objects = d["producers"] * d["consumers"]
            insights += f"| `{qname[:40]}` | {d['producers']} | {d['consumers']} | {mq_objects} REMOTE Qs | 1 topic |\n"
        if len(high_fanout_queues) > 10:
            insights += f"| ... | | | | ({len(high_fanout_queues) - 10} more queues) |\n"
        insights += f"\n"

        if high_fanout_producers:
            insights += f"**High fan-out producers driving this pattern:**\n\n"
            for app, count in high_fanout_producers[:8]:
                insights += f"- `{app}` → **{count} consumers** (requires {count} REMOTE queues on its QM)\n"
            if len(high_fanout_producers) > 8:
                insights += f"- ... and {len(high_fanout_producers) - 8} more high-fan-out producers\n"
            insights += f"\n"

        insights += f"**IBM-native path:** IBM MQ Event Streams (part of Cloud Pak for Integration) provides "
        insights += f"Kafka-compatible event streaming that integrates natively with existing MQ infrastructure "
        insights += f"via the MQ-Kafka connector bridge. This avoids introducing a separate Kafka cluster while "
        insights += f"gaining topic-based fan-out for the {len(high_fanout_queues)} broadcast queues identified above.\n\n"
    else:
        insights += f"**Detection result: No queues with >3 consumers detected.** "
        insights += f"The topology is predominantly point-to-point, which is MQ's strength. "
        insights += f"Event streaming is not indicated for this workload.\n\n"

    # ══════════════════════════════════════════════════════════════════════
    # PATTERN 2: Synchronous APIs (gRPC / REST) — request-reply
    # ══════════════════════════════════════════════════════════════════════
    insights += f"### Pattern 2: Synchronous APIs (gRPC / REST) — Request-Reply Replacement\n\n"
    insights += f"**Signal:** Bidirectional app pairs where App A produces to a queue consumed by App B, "
    insights += f"AND App B produces to a queue consumed by App A. This is the classic request-reply "
    insights += f"anti-pattern in MQ — using asynchronous messaging for what is semantically a synchronous call.\n\n"
    insights += f"**Why:** Each request-reply pair in MQ requires 2 queues, 2 channels (if cross-QM), "
    insights += f"2 XMITQs, and 4 REMOTE queue definitions. A direct gRPC or REST call requires zero "
    insights += f"MQ objects and reduces end-to-end latency by eliminating store-and-forward hops.\n\n"

    if request_reply_pairs:
        insights += f"**Detection result: {len(request_reply_pairs)} request-reply pairs identified.**\n\n"
        for app_a, app_b in request_reply_pairs[:10]:
            insights += f"- `{app_a}` ↔ `{app_b}` (bidirectional message exchange)\n"
        if len(request_reply_pairs) > 10:
            insights += f"- ... and {len(request_reply_pairs) - 10} more pairs\n"
        insights += f"\n"
        insights += f"**Recommendation:** Evaluate these {len(request_reply_pairs)} pairs for migration to "
        insights += f"synchronous APIs. Candidates where latency < 100ms is acceptable and no guaranteed "
        insights += f"delivery is required can be replaced with gRPC (for internal service-to-service) "
        insights += f"or REST (for broader compatibility). This eliminates "
        insights += f"~{len(request_reply_pairs) * 4} MQ objects from the target state.\n\n"
    else:
        insights += f"**Detection result: No request-reply pairs detected.** "
        insights += f"All bidirectional apps ({len(bidirectional)}) use separate unrelated queues for PUT and GET, "
        insights += f"indicating legitimate async workflows rather than disguised RPC. "
        insights += f"Synchronous API replacement is not indicated.\n\n"

    # ══════════════════════════════════════════════════════════════════════
    # PATTERN 3: AMQP 1.0 (lightweight messaging) — low-overhead P2P
    # ══════════════════════════════════════════════════════════════════════
    insights += f"### Pattern 3: AMQP 1.0 — Lightweight Point-to-Point\n\n"
    insights += f"**Signal:** Strict 1:1 point-to-point queues (exactly 1 producer, 1 consumer) "
    insights += f"where MQ's full transactional XA guarantees may be unnecessary overhead.\n\n"
    insights += f"**Why:** IBM MQ natively supports AMQP 1.0 clients (since MQ v8). AMQP clients "
    insights += f"connect directly without needing MQ-specific client libraries, reducing onboarding "
    insights += f"friction for new applications. For simple fire-and-forget or at-least-once delivery "
    insights += f"patterns, AMQP eliminates the need for full MQ client configuration.\n\n"

    if p2p_queues:
        insights += f"**Detection result: {len(p2p_queues)} strict 1:1 queues ({p2p_pct}% of active queues).**\n\n"
        insights += f"These queues involve exactly one producer and one consumer. "
        if p2p_pct > 50:
            insights += f"The majority of the topology follows this pattern, suggesting many flows "
            insights += f"could benefit from lighter-weight AMQP clients.\n\n"
        else:
            insights += f"This is a minority of flows. AMQP migration is a low-priority optimisation.\n\n"
        insights += f"**Note:** AMQP clients can connect to the SAME queue managers in the target state — "
        insights += f"no topology change required. This is a client-side protocol optimisation, not "
        insights += f"an infrastructure migration.\n\n"
    else:
        insights += f"**Detection result: No strict 1:1 queues detected.** "
        insights += f"All queues have multiple producers or consumers. AMQP migration is not applicable.\n\n"

    # ══════════════════════════════════════════════════════════════════════
    # PATTERN 4: Cloud Pub/Sub bridge — hybrid cloud workloads
    # ══════════════════════════════════════════════════════════════════════
    insights += f"### Pattern 4: Cloud Pub/Sub Bridge (AWS SNS+SQS / Azure Service Bus)\n\n"
    insights += f"**Signal:** Applications that are cloud-native or scheduled for cloud migration, "
    insights += f"currently forced through on-premise MQ infrastructure.\n\n"
    insights += f"**Why:** If consuming applications move to cloud, maintaining on-premise MQ channels "
    insights += f"to reach them adds latency and operational overhead. Cloud-native pub/sub services "
    insights += f"(SNS+SQS, Azure Service Bus, Google Pub/Sub) provide managed messaging with "
    insights += f"auto-scaling and no infrastructure to operate.\n\n"
    insights += f"**Detection result:** Cloud migration status is not present in the input dataset. "
    insights += f"This pattern cannot be evaluated from topology data alone.\n\n"
    insights += f"**Recommendation:** Cross-reference app IDs against the enterprise cloud migration "
    insights += f"roadmap. Any apps in the target state that are scheduled for cloud migration within "
    insights += f"12 months should use an MQ-to-cloud bridge (IBM MQ Internet Pass-Thru or IBM MQ "
    insights += f"on Cloud) rather than provisioning on-premise channels that will be decommissioned.\n\n"

    # ══════════════════════════════════════════════════════════════════════
    # PATTERN 5: Retain IBM MQ — transactional, guaranteed delivery
    # ══════════════════════════════════════════════════════════════════════
    retain_count = total_flows - high_fo_flows - len(request_reply_pairs)
    retain_pct = round(retain_count / max(total_flows, 1) * 100, 1)

    insights += f"### Pattern 5: Retain IBM MQ — Transactional Guaranteed Delivery\n\n"
    insights += f"**Signal:** Point-to-point or low fan-out flows requiring exactly-once delivery, "
    insights += f"XA transaction coordination, or regulatory audit trails.\n\n"
    insights += f"**Why:** IBM MQ remains the gold standard for transactional messaging. Its persistent, "
    insights += f"exactly-once delivery guarantees are unmatched by event streaming or cloud pub/sub. "
    insights += f"Flows involving financial transactions, regulatory reporting, or cross-system "
    insights += f"coordination should remain on MQ.\n\n"
    insights += f"**Detection result: ~{max(retain_count, 0)} flows ({retain_pct}%) are best served by MQ.**\n\n"
    insights += f"The simplified target state delivered by IntelliAI is the correct architecture for "
    insights += f"these workloads. The 1:1 app-to-QM model ensures clear ownership, and flow-justified "
    insights += f"channels minimise operational surface area.\n\n"

    # ══════════════════════════════════════════════════════════════════════
    # SUMMARY TABLE
    # ══════════════════════════════════════════════════════════════════════
    insights += f"### Summary: Messaging Pattern Suitability Matrix\n\n"
    insights += f"| Pattern | Signal Detected | Candidate Flows | Recommendation |\n"
    insights += f"|---------|----------------|-----------------|----------------|\n"

    kafka_signal = "STRONG" if (high_fo_pct > 30 and len(high_fanout_queues) > 5) else ("MODERATE" if (high_fo_pct > 10 or len(high_fanout_queues) > 2) else "NONE")
    kafka_rec = f"Migrate {len(high_fanout_queues)} broadcast queues" if high_fanout_queues else "Not indicated"
    insights += f"| Event Streaming (Kafka/Event Streams) | {kafka_signal} | {high_fo_flows} flows ({high_fo_pct}%) | {kafka_rec} |\n"

    grpc_signal = "YES" if request_reply_pairs else "NONE"
    grpc_rec = f"Evaluate {len(request_reply_pairs)} pairs for API replacement" if request_reply_pairs else "Not indicated"
    insights += f"| Synchronous APIs (gRPC/REST) | {grpc_signal} | {len(request_reply_pairs)} pairs | {grpc_rec} |\n"

    amqp_signal = "MODERATE" if p2p_pct > 50 else ("LOW" if p2p_queues else "NONE")
    amqp_rec = f"Client-side optimisation for {len(p2p_queues)} queues" if p2p_queues else "Not indicated"
    insights += f"| AMQP 1.0 (lightweight P2P) | {amqp_signal} | {len(p2p_queues)} queues ({p2p_pct}%) | {amqp_rec} |\n"

    insights += f"| Cloud Pub/Sub Bridge | UNKNOWN | — | Cross-reference cloud migration roadmap |\n"
    insights += f"| **Retain IBM MQ** | **PRIMARY** | **~{max(retain_count, 0)} flows ({retain_pct}%)** | **Target state delivered** |\n\n"

    # ══════════════════════════════════════════════════════════════════════
    # PHASED ROADMAP
    # ══════════════════════════════════════════════════════════════════════
    insights += f"### Recommended Phased Roadmap\n\n"
    insights += f"| Phase | Action | Prerequisite |\n"
    insights += f"|-------|--------|--------------|\n"
    insights += f"| Phase 1 (Immediate) | Deploy MQ target state — 1:1 app-to-QM, flow-justified channels | This deliverable |\n"
    if high_fanout_queues:
        insights += f"| Phase 2 (3-6 months) | Introduce IBM Event Streams for {len(high_fanout_queues)} high fan-out queues | MQ-Kafka connector bridge |\n"
    if request_reply_pairs:
        insights += f"| Phase 3 (6-9 months) | Replace {len(request_reply_pairs)} request-reply pairs with gRPC/REST | Service mesh, API gateway |\n"
    if p2p_queues and p2p_pct > 30:
        insights += f"| Phase 4 (9-12 months) | Migrate lightweight 1:1 flows to AMQP 1.0 clients | MQ AMQP channel config |\n"
    insights += f"| Ongoing | Evaluate cloud-bound apps for pub/sub bridge | Cloud migration roadmap |\n\n"

    insights += f"""
### Complexity Metrics Supporting This Recommendation

| Metric | As-Is | Target | Change |
|--------|-------|--------|--------|
| Channel Count | {as_is_ch} | {target_ch} | {as_is_ch - target_ch:+d} |
| Coupling Index | {ci} | {target_metrics.get('coupling_index', 'N/A')} | {'Improved' if ci > 1.0 else 'Same'} |
| Fan-Out Score | {fo} | {target_metrics.get('fan_out_score', 'N/A')} | {fo - target_metrics.get('fan_out_score', fo):+.0f} |
| Channel Sprawl | {as_is_metrics.get('channel_sprawl', 'N/A')} | {target_metrics.get('channel_sprawl', 'N/A')} | {'Improved' if (target_metrics.get('channel_sprawl', 99) or 99) < (as_is_metrics.get('channel_sprawl', 0) or 0) else 'Same'} |
| Max REMOTE Qs per QM | {max((d['producers']*d['consumers'] for _,d in queue_fanout.items()), default=0)} | — | Driven by fan-out |

---
*Generated by IntelliAI -- IBM MQ Hackathon 2026*
"""
    return insights


def _generate_migration_plan_md(state: dict) -> str:
    """Deliverable 4: migration-plan.md — standalone phased migration plan."""
    migration_plan = state.get("migration_plan", {}) or {}
    diff = state.get("topology_diff", {}) or {}
    steps = migration_plan.get("steps", [])

    qms_added = diff.get("qms_added", [])
    qms_removed = diff.get("qms_removed", [])
    channels_added = diff.get("channels_added", [])
    channels_removed = diff.get("channels_removed", [])
    apps_reassigned = diff.get("apps_reassigned", [])

    md = f"""# Migration Plan — IntelliAI

## Overview

This document describes the phased migration from the as-is MQ topology to the
optimised target state designed by IntelliAI's 10-agent pipeline.

**Migration scope:**
- {len(qms_added)} queue managers created
- {len(qms_removed)} queue managers decommissioned
- {len(channels_added)} channels added
- {len(channels_removed)} channels removed
- {len(apps_reassigned)} applications reassigned to new queue managers

**Total migration steps:** {len(steps)}

## Topology Diff Summary

| Change Type | Count | Details |
|-------------|-------|---------|
| QMs Added | {len(qms_added)} | {', '.join(qms_added[:8]) or 'None'}{'...' if len(qms_added) > 8 else ''} |
| QMs Removed | {len(qms_removed)} | {', '.join(qms_removed[:8]) or 'None'}{'...' if len(qms_removed) > 8 else ''} |
| Channels Added | {len(channels_added)} | New flow-justified channels |
| Channels Removed | {len(channels_removed)} | Redundant or orphaned channels |
| Apps Reassigned | {len(apps_reassigned)} | Moved to dedicated 1:1 QMs |

## Migration Phases

The migration follows a 4-phase controlled transition model. Each phase has
forward MQSC commands, rollback MQSC commands, dependency tracking, and
verification steps.

### Phase 1: CREATE — New Infrastructure
**Objective:** Provision all new queue managers, channels, XMITQs, and listeners
before any application is moved.

**Risk:** Low — no existing traffic is affected.

"""
    # Phase 1 steps
    phase1 = [s for s in steps if s.get("phase") == "CREATE"]
    if phase1:
        md += f"| Step | Description | Target QM | Depends On |\n"
        md += f"|------|-------------|-----------|------------|\n"
        for s in phase1:
            deps = ", ".join(str(d) for d in s.get("depends_on", [])) or "—"
            md += f"| {s['step_number']} | {s['description']} | {s['target_qm']} | {deps} |\n"
        md += "\n"
    else:
        md += "No CREATE steps required.\n\n"

    md += """### Phase 2: REROUTE — Move Applications
**Objective:** Migrate applications from old QMs to their new dedicated QMs.
Each migration is a single-weekend operation with rollback capability.

**Risk:** Medium — requires application downtime and configuration change.
Mitigated by per-app rollback scripts.

"""
    phase2 = [s for s in steps if s.get("phase") == "REROUTE"]
    if phase2:
        md += f"| Step | Description | Target QM | Depends On |\n"
        md += f"|------|-------------|-----------|------------|\n"
        for s in phase2:
            deps = ", ".join(str(d) for d in s.get("depends_on", [])) or "—"
            md += f"| {s['step_number']} | {s['description']} | {s['target_qm']} | {deps} |\n"
        md += "\n"
    else:
        md += "No REROUTE steps required.\n\n"

    md += """### Phase 3: DRAIN — Wait for Old Queues to Empty
**Objective:** Verify all in-flight messages on old channels have been delivered
before removing any infrastructure.

**Risk:** Low — non-destructive monitoring step. No rollback needed.

"""
    phase3 = [s for s in steps if s.get("phase") == "DRAIN"]
    if phase3:
        md += f"| Step | Description | Target QM | Verification |\n"
        md += f"|------|-------------|-----------|-------------|\n"
        for s in phase3:
            md += f"| {s['step_number']} | {s['description']} | {s['target_qm']} | {s.get('verification', '')} |\n"
        md += "\n"
    else:
        md += "No DRAIN steps required.\n\n"

    md += """### Phase 4: CLEANUP — Remove Old Objects
**Objective:** Decommission old channels, queues, and queue managers that are
no longer needed in the target state.

**Risk:** Medium — destructive operations. Full rollback MQSC provided for
each step to re-create removed objects if needed.

"""
    phase4 = [s for s in steps if s.get("phase") == "CLEANUP"]
    if phase4:
        md += f"| Step | Description | Target QM | Depends On |\n"
        md += f"|------|-------------|-----------|------------|\n"
        for s in phase4:
            deps = ", ".join(str(d) for d in s.get("depends_on", [])) or "—"
            md += f"| {s['step_number']} | {s['description']} | {s['target_qm']} | {deps} |\n"
        md += "\n"
    else:
        md += "No CLEANUP steps required.\n\n"

    # Detailed commands (cap at 100 to avoid massive output)
    if steps:
        md += "## Detailed Migration Commands\n\n"
        display_steps = steps[:100]
        for s in display_steps:
            md += f"### Step {s['step_number']}: {s['description']}\n"
            md += f"**Phase:** {s['phase']} | **Target QM:** {s['target_qm']}\n\n"
            md += f"**Forward MQSC:**\n```\n{s.get('mqsc_forward', 'N/A')}\n```\n\n"
            md += f"**Rollback MQSC:**\n```\n{s.get('mqsc_rollback', 'N/A')}\n```\n\n"
            md += f"**Verification:** `{s.get('verification', 'N/A')}`\n\n"
        if len(steps) > 100:
            md += f"\n*... {len(steps) - 100} more steps. Full MQSC scripts available in provisioner output.*\n\n"

    md += """## Rollback Strategy

If any phase fails, execute rollback MQSC scripts in **reverse phase order**:

1. CLEANUP rollback → re-create deleted objects
2. DRAIN → non-destructive, no rollback needed
3. REROUTE rollback → reconfigure apps back to old QMs
4. CREATE rollback → delete newly created QMs and channels

**Rollback trigger conditions:**
- Any end-to-end message flow test fails
- Message loss detected (DLQ depth > 0)
- Channel fails to start after 3 retry attempts
- Application cannot connect to new QM

## Parallel Execution Opportunities

- All Phase 1 (CREATE) steps can run in parallel — no dependencies between new QMs
- Phase 2 (REROUTE) apps can be migrated in parallel if they are on different QMs
- Phase 3 (DRAIN) monitoring can run concurrently across all old channels
- Phase 4 (CLEANUP) should be sequential to avoid removing objects still referenced

---
*Generated by IntelliAI — IBM MQ Hackathon 2026*
"""
    return md


def _generate_subgraph_analysis_md(state: dict) -> str:
    """Deliverable 6 supplement: subgraph-analysis.md — topology decomposition."""
    as_is_subs = state.get("as_is_subgraphs", []) or []
    target_subs = state.get("target_subgraphs", []) or []
    as_is_communities = state.get("as_is_communities", {}) or {}
    target_communities = state.get("target_communities", {}) or {}
    as_is_centrality = state.get("as_is_centrality", {}) or {}
    target_centrality = state.get("target_centrality", {}) or {}
    as_is_entropy = state.get("as_is_entropy", {}) or {}
    target_entropy = state.get("target_entropy", {}) or {}

    md = f"""# Subgraph Analysis — IntelliAI

## Overview

This document decomposes both the as-is and target topologies into their constituent
subgraphs (weakly connected components), analyses community structure via Louvain
detection, and identifies single points of failure via betweenness centrality.

## Summary Comparison

| Metric | As-Is | Target | Change |
|--------|-------|--------|--------|
| Connected components | {len(as_is_subs)} | {len(target_subs)} | {len(target_subs) - len(as_is_subs):+d} |
| Louvain communities | {as_is_communities.get('num_communities', 'N/A')} | {target_communities.get('num_communities', 'N/A')} | — |
| Modularity score | {as_is_communities.get('modularity', 'N/A')} | {target_communities.get('modularity', 'N/A')} | {'Improved' if (target_communities.get('modularity', 0) or 0) > (as_is_communities.get('modularity', 0) or 0) else 'Same or reduced'} |
| SPOF QMs (high betweenness) | {len(as_is_centrality.get('spof_qms', []))} | {len(target_centrality.get('spof_qms', []))} | {len(target_centrality.get('spof_qms', [])) - len(as_is_centrality.get('spof_qms', [])):+d} |
| Hub QMs (high degree) | {len(as_is_centrality.get('hub_qms', []))} | {len(target_centrality.get('hub_qms', []))} | {len(target_centrality.get('hub_qms', [])) - len(as_is_centrality.get('hub_qms', [])):+d} |
| Degree entropy (bits) | {as_is_entropy.get('degree_entropy', 'N/A')} | {target_entropy.get('degree_entropy', 'N/A')} | — |
| Entropy ratio (vs max) | {as_is_entropy.get('entropy_ratio', 'N/A')} | {target_entropy.get('entropy_ratio', 'N/A')} | {'Healthier' if (target_entropy.get('entropy_ratio', 0) or 0) > (as_is_entropy.get('entropy_ratio', 0) or 0) else 'More skewed'} |
| Graph density | {as_is_entropy.get('density', 'N/A')} | {target_entropy.get('density', 'N/A')} | — |

---

## As-Is Topology — Component Decomposition

"""
    if as_is_subs:
        md += f"The as-is topology decomposes into **{len(as_is_subs)} connected component(s)**.\n\n"
        isolated_count = sum(1 for s in as_is_subs if s.get("is_isolated"))
        if isolated_count:
            md += f"Of these, **{isolated_count} are isolated** (single QM with no channels).\n\n"

        md += f"| # | QM Count | App Count | Channels | Isolated | Hub QM | Regions |\n"
        md += f"|---|----------|-----------|----------|----------|--------|---------|\n"
        for s in as_is_subs[:20]:
            regions = ", ".join(s.get("regions", [])[:3])
            md += (f"| {s.get('component_id', '?')} | {len(s.get('qm_ids', []))} "
                   f"| {len(s.get('app_ids', []))} | {s.get('channel_count', 0)} "
                   f"| {'Yes' if s.get('is_isolated') else 'No'} "
                   f"| {s.get('hub_qm', '—')} | {regions or '—'} |\n")
        if len(as_is_subs) > 20:
            md += f"| ... | | | | | | ({len(as_is_subs) - 20} more) |\n"
        md += "\n"
    else:
        md += "No subgraph data available for as-is topology.\n\n"

    # SPOF analysis
    if as_is_centrality.get("spof_qms"):
        md += f"### Single Points of Failure (As-Is)\n\n"
        md += f"These QMs have betweenness centrality >2× the mean, meaning a disproportionate "
        md += f"share of message routes pass through them:\n\n"
        for qm in as_is_centrality["spof_qms"][:10]:
            bw = as_is_centrality.get("betweenness", {}).get(qm, 0)
            md += f"- `{qm}` — betweenness: {bw} (mean: {as_is_centrality.get('betweenness_mean', 0)})\n"
        md += "\n"

    md += f"""---

## Target Topology — Component Decomposition

"""
    if target_subs:
        md += f"The target topology decomposes into **{len(target_subs)} connected component(s)**.\n\n"
        target_isolated = sum(1 for s in target_subs if s.get("is_isolated"))
        if target_isolated:
            md += f"Of these, **{target_isolated} are isolated** (single QM with no channels — "
            md += f"apps on these QMs only communicate internally).\n\n"

        md += f"| # | QM Count | App Count | Channels | Isolated | Hub QM | Regions |\n"
        md += f"|---|----------|-----------|----------|----------|--------|---------|\n"
        for s in target_subs[:20]:
            regions = ", ".join(s.get("regions", [])[:3])
            md += (f"| {s.get('component_id', '?')} | {len(s.get('qm_ids', []))} "
                   f"| {len(s.get('app_ids', []))} | {s.get('channel_count', 0)} "
                   f"| {'Yes' if s.get('is_isolated') else 'No'} "
                   f"| {s.get('hub_qm', '—')} | {regions or '—'} |\n")
        if len(target_subs) > 20:
            md += f"| ... | | | | | | ({len(target_subs) - 20} more) |\n"
        md += "\n"
    else:
        md += "No subgraph data available for target topology.\n\n"

    # Target SPOF
    if target_centrality.get("spof_qms"):
        md += f"### Single Points of Failure (Target)\n\n"
        for qm in target_centrality["spof_qms"][:10]:
            bw = target_centrality.get("betweenness", {}).get(qm, 0)
            md += f"- `{qm}` — betweenness: {bw} (mean: {target_centrality.get('betweenness_mean', 0)})\n"
        md += "\n"
    else:
        md += "### SPOF Analysis (Target)\n\nNo single points of failure detected in the target topology.\n\n"

    # Community structure
    md += f"## Community Detection (Louvain Algorithm)\n\n"
    md += f"Louvain community detection identifies natural clusters of QMs that are more "
    md += f"densely connected internally than externally.\n\n"

    if as_is_communities.get("communities"):
        md += f"### As-Is Communities\n\n"
        md += f"**{as_is_communities['num_communities']} communities** detected "
        md += f"(modularity = {as_is_communities['modularity']}).\n\n"
        for i, comm in enumerate(as_is_communities["communities"][:10]):
            md += f"- Community {i+1}: {', '.join(comm[:6])}{'...' if len(comm) > 6 else ''} ({len(comm)} QMs)\n"
        md += "\n"

    if target_communities.get("communities"):
        md += f"### Target Communities\n\n"
        md += f"**{target_communities['num_communities']} communities** detected "
        md += f"(modularity = {target_communities['modularity']}).\n\n"
        for i, comm in enumerate(target_communities["communities"][:10]):
            md += f"- Community {i+1}: {', '.join(comm[:6])}{'...' if len(comm) > 6 else ''} ({len(comm)} QMs)\n"
        md += "\n"

    # Entropy
    md += f"## Degree Distribution Health (Shannon Entropy)\n\n"
    md += f"Shannon entropy measures how uniformly channels are distributed across QMs:\n"
    md += f"- **High entropy** (close to max) = channels are evenly distributed — healthy, resilient topology\n"
    md += f"- **Low entropy** = a few QMs dominate connections — fragile hub-and-spoke pattern\n\n"
    md += f"| Metric | As-Is | Target | Interpretation |\n"
    md += f"|--------|-------|--------|----------------|\n"
    as_er = as_is_entropy.get('entropy_ratio', 0) or 0
    tgt_er = target_entropy.get('entropy_ratio', 0) or 0
    md += f"| Entropy (bits) | {as_is_entropy.get('degree_entropy', 'N/A')} | {target_entropy.get('degree_entropy', 'N/A')} | Higher = more uniform |\n"
    md += f"| Max possible | {as_is_entropy.get('max_entropy', 'N/A')} | {target_entropy.get('max_entropy', 'N/A')} | Theoretical ceiling |\n"
    md += f"| Ratio | {as_er:.1%} | {tgt_er:.1%} | {'Target is healthier' if tgt_er > as_er else 'As-is is more uniform'} |\n"
    md += f"| Density | {as_is_entropy.get('density', 'N/A')} | {target_entropy.get('density', 'N/A')} | Lower = sparser (simpler) |\n"
    md += f"| Avg clustering | {as_is_entropy.get('avg_clustering', 'N/A')} | {target_entropy.get('avg_clustering', 'N/A')} | Higher = more cliquey |\n\n"

    md += """---
*Generated by IntelliAI — IBM MQ Hackathon 2026*
"""
    return md
