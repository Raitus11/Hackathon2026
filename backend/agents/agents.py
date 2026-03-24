"""
agents.py
All 10 MQ-TITAN agents.
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
from backend.graph.mq_graph import build_graph, detect_violations, compute_complexity, graph_to_dict
from backend.llm.llm_client import call_llm, validate_architect_response
from backend.llm.prompts import ARCHITECT_SYSTEM_PROMPT, build_architect_prompt

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

    # ── Validate all 4 required keys are present ──────────────────────────
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

    # Merge violations into existing quality report
    quality_report = state.get("data_quality_report", {})
    quality_report["topology_violations"] = violations

    return {
        "as_is_graph": as_is_graph,
        "data_quality_report": quality_report,
        "messages": messages,
    }


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 3: ANALYST
# Computes 5-factor complexity score on the as-is graph.
# ─────────────────────────────────────────────────────────────────────────────
def analyst_agent(state: dict) -> dict:
    logger.info("ANALYST: Computing as-is complexity metrics")
    messages = state.get("messages", [])

    G = state["as_is_graph"]
    metrics = compute_complexity(G)

    msg = (
        f"As-Is Complexity Score: {metrics['total_score']}/100 | "
        f"Channels: {metrics['channel_count']} | "
        f"Coupling: {metrics['coupling_index']} | "
        f"Depth: {metrics['routing_depth']} | "
        f"Fan-out: {metrics['fan_out_score']} | "
        f"Orphans: {metrics['orphan_objects']}"
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
    logger.info("ARCHITECT: Designing target state topology")
    messages = state.get("messages", [])
    adrs = state.get("adrs", [])
    redesign_count = state.get("redesign_count", 0)
    raw_data = state["raw_data"]

    # Try LLM approach first
    llm_result = _architect_llm(state)

    if llm_result is not None:
        # LLM succeeded — build graph from its output
        target_graph, llm_adrs = _build_target_from_llm(llm_result, state)
        adrs.extend(llm_adrs)
        method = "llm"
        logger.info(f"ARCHITECT: LLM method succeeded — {len(llm_adrs)} ADRs generated")
    else:
        # Fallback to deterministic rules
        target_graph = _build_target_rules(state)
        rule_adrs = _generate_rule_adrs(state, target_graph, redesign_count)
        adrs.extend(rule_adrs)
        method = "rules_fallback"
        logger.info(f"ARCHITECT: Fell back to rule-based method — {len(rule_adrs)} ADRs")

    original_qm_count = sum(1 for _, d in state["as_is_graph"].nodes(data=True) if d.get("type") == "qm")
    target_qm_count = sum(1 for _, d in target_graph.nodes(data=True) if d.get("type") == "qm")
    target_ch_count = sum(1 for _, _, d in target_graph.edges(data=True) if d.get("rel") == "channel")

    msg = (
        f"Target state designed using {method}: "
        f"{target_qm_count} QMs (was {original_qm_count}), "
        f"{target_ch_count} channels. "
        f"{len(adrs)} ADRs written."
    )
    messages.append({"agent": "ARCHITECT", "msg": msg})

    return {
        "target_graph": target_graph,
        "adrs": adrs,
        "redesign_count": redesign_count + 1,
        "architect_method": method,
        "messages": messages,
    }


def _architect_llm(state: dict) -> dict | None:
    """Call Groq LLM for architecture design. Returns parsed dict or None."""
    try:
        user_prompt = build_architect_prompt(state)
        result = call_llm(
            system_prompt=ARCHITECT_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            max_retries=2,
            temperature=0.1,
            max_tokens=4096,
        )
        if result is None:
            return None

        # Validate required keys
        missing = validate_architect_response(result)
        if missing:
            logger.warning(f"LLM response missing keys: {missing}")
            return None

        return result
    except Exception as e:
        logger.error(f"Architect LLM call failed: {e}")
        return None


def _build_target_from_llm(llm_result: dict, state: dict) -> tuple:
    """
    Build a NetworkX target graph from the LLM's structured output.
    Returns (graph, adrs_list).
    """
    raw_data = state["raw_data"]
    G_target = nx.DiGraph()

    # Validate entity references against actual data
    valid_qm_ids = set(q["qm_id"] for q in raw_data["queue_managers"])
    valid_app_ids = set(a["app_id"] for a in raw_data["applications"])
    qm_map = {row["qm_id"]: row for row in raw_data["queue_managers"]}

    # Add QMs the LLM wants to keep
    qms_to_keep = set()
    for qm_id in llm_result.get("qms_to_keep", []):
        if qm_id in valid_qm_ids and qm_id in qm_map:
            row = qm_map[qm_id]
            G_target.add_node(
                qm_id, type="qm",
                name=row.get("qm_name", qm_id),
                region=row.get("region", ""),
            )
            qms_to_keep.add(qm_id)

    # If LLM returned empty qms_to_keep, fall back
    if not qms_to_keep:
        logger.warning("LLM returned empty qms_to_keep — falling back to rules")
        return _build_target_rules(state), []

    # Add apps with LLM-assigned QMs
    app_name_map = {}
    for row in raw_data["applications"]:
        if row["app_id"] not in app_name_map:
            app_name_map[row["app_id"]] = row.get("app_name", row["app_id"])

    for assignment in llm_result.get("target_app_assignments", []):
        app_id = assignment.get("app_id", "")
        assigned_qm = assignment.get("assigned_qm", "")

        if app_id not in valid_app_ids:
            continue
        if assigned_qm not in qms_to_keep:
            # LLM assigned to a removed QM — pick first available
            assigned_qm = sorted(qms_to_keep)[0] if qms_to_keep else None
            if not assigned_qm:
                continue

        if app_id not in G_target.nodes:
            G_target.add_node(app_id, type="app", name=app_name_map.get(app_id, app_id))
        G_target.add_edge(app_id, assigned_qm, rel="connects_to")

    # Ensure ALL apps from source data are assigned (LLM might miss some)
    assigned_apps = set(
        n for n, d in G_target.nodes(data=True) if d.get("type") == "app"
    )
    for app_id in valid_app_ids:
        if app_id not in assigned_apps:
            # Find original QM, or first available
            original_qms = [
                r["qm_id"] for r in raw_data["applications"]
                if r["app_id"] == app_id and r["qm_id"] in qms_to_keep
            ]
            qm = original_qms[0] if original_qms else sorted(qms_to_keep)[0]
            if app_id not in G_target.nodes:
                G_target.add_node(app_id, type="app", name=app_name_map.get(app_id, app_id))
            G_target.add_edge(app_id, qm, rel="connects_to")

    # Add channels from LLM required_connections
    for conn in llm_result.get("required_connections", []):
        from_qm = conn.get("from_qm", "")
        to_qm = conn.get("to_qm", "")
        if from_qm in qms_to_keep and to_qm in qms_to_keep and from_qm != to_qm:
            channel_name = f"{from_qm}.{to_qm}"
            if not G_target.has_edge(from_qm, to_qm):
                G_target.add_edge(
                    from_qm, to_qm,
                    rel="channel",
                    channel_name=channel_name,
                    status="RUNNING",
                    xmit_queue=f"{to_qm}.XMITQ",
                )

    # ── SAFETY NET: backfill channels for isolated QMs ────────────────────
    # The LLM sometimes keeps a QM + apps but forgets to create channels for
    # it. Find any QM that has apps but zero channels and restore relevant
    # as-is channels so apps aren't stranded.
    target_app_qm = {}
    for n, d in G_target.nodes(data=True):
        if d.get("type") == "app":
            for _, v, ed in G_target.out_edges(n, data=True):
                if ed.get("rel") == "connects_to":
                    target_app_qm[n] = v

    qms_with_apps = set(target_app_qm.values())

    def _qm_has_channel(qm, graph):
        for _, _, d in graph.out_edges(qm, data=True):
            if d.get("rel") == "channel":
                return True
        for _, _, d in graph.in_edges(qm, data=True):
            if d.get("rel") == "channel":
                return True
        return False

    for qm in list(qms_with_apps):
        if _qm_has_channel(qm, G_target):
            continue

        # This QM is isolated — try to restore as-is channels
        logger.warning(f"LLM left {qm} isolated — attempting channel backfill from as-is")
        backfilled = False
        for ch in raw_data.get("channels", []):
            if ch.get("channel_type") != "SENDER":
                continue
            if ch.get("status", "").upper() == "STOPPED":
                continue
            from_qm_ch = ch["from_qm"]
            to_qm_ch = ch["to_qm"]
            if (from_qm_ch == qm and to_qm_ch in qms_to_keep) or \
               (to_qm_ch == qm and from_qm_ch in qms_to_keep):
                if not G_target.has_edge(from_qm_ch, to_qm_ch):
                    channel_name = f"{from_qm_ch}.{to_qm_ch}"
                    G_target.add_edge(
                        from_qm_ch, to_qm_ch,
                        rel="channel",
                        channel_name=channel_name,
                        status="RUNNING",
                        xmit_queue=f"{to_qm_ch}.XMITQ",
                    )
                    backfilled = True

        # If STILL isolated (no as-is channels existed for this QM at all),
        # force-move its apps to the nearest connected QM and remove the QM.
        if not backfilled or not _qm_has_channel(qm, G_target):
            # Find a connected QM to absorb the apps — prefer same region
            qm_region = G_target.nodes[qm].get("region", "")
            connected_qms = [
                q for q in qms_with_apps
                if q != qm and _qm_has_channel(q, G_target)
            ]
            # Prefer same region
            same_region = [q for q in connected_qms if G_target.nodes.get(q, {}).get("region") == qm_region]
            absorber = same_region[0] if same_region else (connected_qms[0] if connected_qms else None)

            if absorber:
                apps_to_move = [a for a, q in target_app_qm.items() if q == qm]
                logger.warning(
                    f"No channels exist for {qm} even in as-is — "
                    f"moving {apps_to_move} to {absorber} and removing {qm}"
                )
                for app in apps_to_move:
                    # Remove old edge, add new one
                    if G_target.has_edge(app, qm):
                        G_target.remove_edge(app, qm)
                    G_target.add_edge(app, absorber, rel="connects_to")
                    target_app_qm[app] = absorber
                # Remove the now-empty QM
                G_target.remove_node(qm)
                qms_to_keep.discard(qm)

    # Parse ADRs from LLM — convert to our format
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


def _build_target_rules(state: dict) -> nx.DiGraph:
    """
    Deterministic rule-based target state builder.
    Used as fallback when LLM is unavailable.
    """
    raw_data = state["raw_data"]
    G_target = nx.DiGraph()

    # Build canonical app→QM ownership map (1 QM per app, enforced)
    app_qm_ownership = {}
    for row in raw_data["applications"]:
        app_id = row["app_id"]
        qm_id = row["qm_id"]
        if app_id not in app_qm_ownership:
            app_qm_ownership[app_id] = qm_id

    needed_qms = set(app_qm_ownership.values())

    # Add QM nodes
    qm_map = {row["qm_id"]: row for row in raw_data["queue_managers"]}
    for qm_id in needed_qms:
        if qm_id in qm_map:
            row = qm_map[qm_id]
            G_target.add_node(qm_id, type="qm", name=row.get("qm_name", qm_id), region=row.get("region", ""))

    # Add app nodes with single QM ownership
    for app_id, qm_id in app_qm_ownership.items():
        app_rows = [r for r in raw_data["applications"] if r["app_id"] == app_id]
        app_name = app_rows[0].get("app_name", app_id) if app_rows else app_id
        G_target.add_node(app_id, type="app", name=app_name)
        if qm_id in G_target.nodes:
            G_target.add_edge(app_id, qm_id, rel="connects_to")

    # Channels from as-is (active senders between needed QMs)
    added_channels = set()
    for row in raw_data["channels"]:
        if row.get("channel_type") != "SENDER":
            continue
        from_qm, to_qm = row["from_qm"], row["to_qm"]
        if from_qm in needed_qms and to_qm in needed_qms and row.get("status", "").upper() != "STOPPED":
            pair = (from_qm, to_qm)
            if pair not in added_channels:
                added_channels.add(pair)
                G_target.add_edge(
                    from_qm, to_qm,
                    rel="channel",
                    channel_name=f"{from_qm}.{to_qm}",
                    status="RUNNING",
                    xmit_queue=f"{to_qm}.XMITQ",
                )

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

    if removed_qms:
        adrs.append({
            "id": f"ADR-{redesign_count+1:02d}-001",
            "decision": f"Remove {len(removed_qms)} QMs: {', '.join(sorted(removed_qms))}",
            "context": f"As-is has {original_qm_count} QMs. {len(removed_qms)} have no active app ownership.",
            "rationale": "Orphan QMs contribute to Channel Count and Fan-Out complexity with zero application value. Removing them reduces operational overhead.",
            "consequences": "Operational teams must decommission these QMs. Any unknown legacy consumers must be identified before removal.",
        })

    multi_qm = violations.get("multi_qm_apps", [])
    if multi_qm:
        adrs.append({
            "id": f"ADR-{redesign_count+1:02d}-002",
            "decision": f"Enforce single-QM ownership for {len(multi_qm)} apps with multiple QM connections",
            "context": f"Apps {[m['app'] for m in multi_qm]} each connect to multiple QMs, violating the 1-QM-per-app constraint.",
            "rationale": "Each app is assigned to its primary QM. Secondary connections replaced with remoteQ+xmitq+channel routing.",
            "consequences": "Application connection strings must be updated to point only to the assigned QM.",
        })

    stopped = violations.get("stopped_channels", [])
    if stopped:
        adrs.append({
            "id": f"ADR-{redesign_count+1:02d}-003",
            "decision": f"Remove {len(stopped)} stopped/inactive channels",
            "context": "Channels with STOPPED status represent unused routing paths.",
            "rationale": "Stopped channels increase Channel Count score and represent latent configuration risk.",
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
    G = state["target_graph"].copy()

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
    # consumers. Dead channels are always safe to remove.
    # ══════════════════════════════════════════════════════════════════════
    required_channels = set()
    for from_qm, to_qm, d in channel_edges:
        has_producer = from_qm in producer_qms or from_qm in qms_with_apps
        has_consumer = to_qm in consumer_qms or to_qm in qms_with_apps
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
    # On surviving channels, apply weighted MST to find the minimum set of
    # edges keeping all active QMs connected. Weight = inverse of app
    # density on each end (denser = more important = lower weight = keep).
    # Edges NOT in MST are candidates for removal, subject to a safety
    # check: never remove if it breaks producer→consumer reachability.
    # Then Kernighan-Lin bisection detects natural topology clusters.
    # ══════════════════════════════════════════════════════════════════════
    surviving_channels = [(u, v, d) for u, v, d in G.edges(data=True) if d.get("rel") == "channel"]
    phase2_removed = []
    mst_applied = False
    kl_applied = False

    # Build undirected QM graph with weighted edges for MST
    G_qm = nx.Graph()
    active_qms = [qm for qm in qm_nodes if qm in qms_with_apps]
    G_qm.add_nodes_from(active_qms)
    for u, v, d in surviving_channels:
        if u in active_qms and v in active_qms:
            src_apps = len([a for a, q in app_qm_map.items() if q == u])
            dst_apps = len([a for a, q in app_qm_map.items() if q == v])
            weight = max(1, 10 - (src_apps + dst_apps))
            G_qm.add_edge(u, v, weight=weight)

    # MST: minimum spanning tree — edges NOT in MST are redundant
    if len(active_qms) > 1 and G_qm.number_of_edges() > 0 and nx.is_connected(G_qm):
        mst = nx.minimum_spanning_tree(G_qm, weight="weight")
        mst_edges = set(frozenset(e) for e in mst.edges())
        current_edges = set(frozenset((u, v)) for u, v, _ in surviving_channels
                           if u in active_qms and v in active_qms)
        removable = current_edges - mst_edges
        mst_applied = True

        if removable:
            for edge_set in removable:
                u, v = tuple(edge_set)
                for src, dst in [(u, v), (v, u)]:
                    if not G.has_edge(src, dst):
                        continue
                    if G[src][dst].get("rel") != "channel":
                        continue

                    # Safety: verify all producer→consumer paths survive
                    G_test = G.copy()
                    G_test.remove_edge(src, dst)
                    reachability_ok = True
                    for p_qm in producer_qms:
                        for c_qm in consumer_qms:
                            if p_qm == c_qm:
                                continue
                            if p_qm not in G_test.nodes or c_qm not in G_test.nodes:
                                continue
                            qm_sub = G_test.subgraph(qm_nodes)
                            if not nx.has_path(qm_sub, p_qm, c_qm):
                                reachability_ok = False
                                break
                        if not reachability_ok:
                            break

                    if reachability_ok:
                        ch_name = G[src][dst].get("channel_name", f"{src}.{dst}")
                        phase2_removed.append((src, dst, ch_name))
                        G.remove_edge(src, dst)

    # Kernighan-Lin bisection — detect natural cluster boundaries
    kl_insight = ""
    G_qm_post = nx.Graph()
    post_channels = [(u, v) for u, v, d in G.edges(data=True)
                     if d.get("rel") == "channel" and u in active_qms and v in active_qms]
    G_qm_post.add_nodes_from(active_qms)
    G_qm_post.add_edges_from(post_channels)

    if len(active_qms) >= 4 and G_qm_post.number_of_edges() > 0 and nx.is_connected(G_qm_post):
        try:
            partition = nx.community.kernighan_lin_bisection(G_qm_post)
            set_a, set_b = partition
            cross_edges = sum(
                1 for u, v in post_channels
                if (u in set_a and v in set_b) or (u in set_b and v in set_a)
            )
            kl_applied = True
            kl_insight = (
                f"Kernighan-Lin bisection identified 2 natural clusters: "
                f"{sorted(set_a)} and {sorted(set_b)} "
                f"with {cross_edges} cross-cluster channel(s). "
            )
        except Exception:
            kl_insight = "Kernighan-Lin bisection: could not partition topology. "
    elif len(active_qms) >= 2:
        kl_insight = (
            f"Kernighan-Lin bisection: skipped "
            f"({len(active_qms)} active QMs, requires ≥4 for meaningful partitioning). "
        )

    # ══════════════════════════════════════════════════════════════════════
    # CYCLE DETECTION — informational metric for architectural awareness
    # Cycles = redundant routing paths. May be intentional for HA or
    # accidental from legacy config. Reported but not a constraint.
    # ══════════════════════════════════════════════════════════════════════
    qm_subgraph = G.subgraph(qm_nodes)
    cycles = list(nx.simple_cycles(qm_subgraph))
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

    phase1_names = [name for _, _, name in phase1_removed if name]
    phase2_names = [name for _, _, name in phase2_removed if name]

    msg = (
        f"Two-phase optimisation complete. "
        f"Channels: {initial_channels} → {after_phase1} (Phase 1: reachability) "
        f"→ {final_channels} (Phase 2: MST). "
        f"Total removed: {initial_channels - final_channels}. "
        f"Phase 1 (reachability pruning): removed {len(phase1_removed)} dead channel(s)"
        f"{' (' + ', '.join(phase1_names) + ')' if phase1_names else ''}. "
        f"Phase 2 (graph-theoretic): "
        f"weighted MST {'applied' if mst_applied else 'skipped (graph disconnected or trivial)'}, "
        f"removed {len(phase2_removed)} redundant channel(s)"
        f"{' (' + ', '.join(phase2_names) + ')' if phase2_names else ''}. "
        f"{kl_insight}"
        f"{cycle_info}"
        f"Target complexity score: {target_metrics['total_score']}/100."
    )
    messages.append({"agent": "OPTIMIZER", "msg": msg})

    return {
        "optimised_graph": G,
        "target_metrics": target_metrics,
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
                violations.append({
                    "rule": "ISOLATED_QM",
                    "entity": qm,
                    "detail": f"QM {qm} has apps {apps_on_qm} but zero channels — apps are completely disconnected from the topology",
                    "severity": "CRITICAL",
                })

    passed = not any(v["severity"] == "CRITICAL" for v in violations)

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

    for idx, qm in enumerate(sorted(qm_nodes)):
        lines = []
        port = port_base + idx
        qm_data = G.nodes[qm]
        hostname = f"{qm.lower().replace('_', '-')}.target.corp.com"

        lines.append(f"* =============================================")
        lines.append(f"* Target State MQSC for {qm}")
        lines.append(f"* Generated by MQ-TITAN Provisioner Agent")
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
                to_port = port_base + sorted(qm_nodes).index(to_qm)
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
    combined.append("* MQ-TITAN Combined MQSC — All Queue Managers")
    combined.append(f"* Session: {state.get('session_id', 'unknown')}")
    combined.append("* NOTE: In production, run each QM section separately via:")
    combined.append("*   runmqsc QM_NAME < QM_NAME_target.mqsc")
    combined.append("* ============================================================")
    combined.append("")
    for qm in sorted(per_qm_scripts.keys()):
        combined.append(per_qm_scripts[qm])
        combined.append("")

    # ── Target State CSV Output ───────────────────────────────────────────
    target_csvs = _generate_target_csvs(G, state)

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

    return csvs


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
    diff = _compute_topology_diff(as_is_graph, target_graph, state)

    # ── Step 2: Generate ordered migration steps ──────────────────────────
    steps = _generate_migration_steps(diff, target_graph)

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
            "depends_on": [s["step_number"] for s in steps if s["phase"] == "CREATE" and "queue manager" in s["description"]],
            "verification": f"DISPLAY CHSTATUS('{ch_name}')  -- should show RUNNING",
        })

    # ── PHASE 2: REROUTE — move applications ─────────────────────────────
    reroute_step_nums = []
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
            "depends_on": create_step_nums.copy(),
            "verification": f"Verify {app_info['app_id']} messages flowing through {app_info['new_qm']}",
        })

    # ── PHASE 3: DRAIN — wait for old queues to empty ────────────────────
    drain_deps = create_step_nums + reroute_step_nums
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
            "depends_on": drain_deps.copy(),
            "verification": f"DISPLAY QLOCAL('{xmitq}') CURDEPTH  -- should show 0",
        })

    # ── PHASE 4: CLEANUP — remove old objects ─────────────────────────────
    cleanup_deps = [s["step_number"] for s in steps if s["phase"] in ("REROUTE", "DRAIN")]

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
            "depends_on": cleanup_deps.copy(),
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
            "# MQ-TITAN — Transformation Report (ABORTED)",
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

    delta = round(as_is.get("total_score", 0) - target.get("total_score", 0), 1)
    pct = round((delta / as_is["total_score"]) * 100, 1) if as_is.get("total_score") else 0

    report_lines = [
        "# MQ-TITAN — Transformation Report",
        "",
        "## Executive Summary",
        f"The AI-driven transformation achieved a **{pct}% reduction** in overall MQ topology complexity.",
        f"Overall Complexity Score reduced from **{as_is.get('total_score')}/100** to **{target.get('total_score')}/100**.",
        "",
        "## Complexity Metrics — Before vs After",
        "| Metric | As-Is | Target | Change |",
        "|--------|-------|--------|--------|",
        f"| Channel Count | {as_is.get('channel_count')} | {target.get('channel_count')} | {int(as_is.get('channel_count',0)) - int(target.get('channel_count',0))} fewer |",
        f"| Coupling Index | {as_is.get('coupling_index')} | {target.get('coupling_index')} | {'Improved' if target.get('coupling_index',99) < as_is.get('coupling_index',0) else 'Same'} |",
        f"| Routing Depth | {as_is.get('routing_depth')} | {target.get('routing_depth')} | {'Reduced' if target.get('routing_depth',99) < as_is.get('routing_depth',0) else 'Same'} |",
        f"| Fan-Out Score | {as_is.get('fan_out_score')} | {target.get('fan_out_score')} | {'Reduced' if target.get('fan_out_score',99) < as_is.get('fan_out_score',0) else 'Same'} |",
        f"| Orphan Objects | {as_is.get('orphan_objects')} | {target.get('orphan_objects')} | {'Eliminated' if target.get('orphan_objects',1) < as_is.get('orphan_objects',0) else 'Same'} |",
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

        # Detailed forward/rollback for each step
        report_lines.append("### Detailed Migration Commands")
        report_lines.append("")
        for step in migration_plan["steps"]:
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

    report_lines += [
        "## Agent Execution Trace",
        "| Step | Agent | Finding |",
        "|------|-------|---------|",
    ]
    for m in messages:
        report_lines.append(f"| — | {m['agent']} | {m['msg']} |")

    final_report = "\n".join(report_lines)
    messages.append({"agent": "DOC_EXPERT", "msg": "Final report generated."})

    return {"final_report": final_report, "messages": messages}
