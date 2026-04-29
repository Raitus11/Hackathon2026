"""
backend/solver/adapters.py

Converts between the existing IntelliAI pipeline's graph representation
(NetworkX DiGraph from backend.graph.mq_graph.build_graph) and the
solver's SolverInput / SolverOutput types.

This is the integration layer that lets the CP-SAT solver replace the
optimizer's MST-based Phase 2 without touching anything upstream.

KEY DESIGN DECISIONS:

1. Flow detection by queue NAME, not queue ID.
   Each row in raw_data["applications"] has both queue_id (physical, per-QM)
   and queue_name (logical, shared across QMs). A flow is a (producer_qm,
   consumer_qm) pair where producer and consumer use the same queue NAME.
   Multiple producers/consumers on the same queue → multiple flows.

2. Soft penalties from business metadata.
   Pulled from raw_data["app_metadata"] when the Architect's Business
   Context Translator hasn't run yet (rule-based fallback). When the
   LLM has run, pass its output to apply_solver_output instead.

3. Apply mode: REPLACE_CHANNELS.
   We delete all existing 'channel' edges from the input graph and add
   only the channels the solver chose. App→QM, QM→Queue, and node-level
   data are untouched. This matches the existing optimizer's effect
   on the graph's channel set.

USAGE:
    from backend.solver.adapters import (
        solver_input_from_graph,
        apply_solver_output,
    )

    inp = solver_input_from_graph(target_graph, raw_data)
    out = solve(inp)
    optimised_graph = apply_solver_output(target_graph, out, inp)
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Optional

import networkx as nx

from backend.solver.cpsat_solver import SolverInput, SolverOutput

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Graph → SolverInput
# ─────────────────────────────────────────────────────────────────────────────

def solver_input_from_graph(
    G: nx.DiGraph,
    raw_data: dict,
    soft_penalties_from_llm: Optional[dict] = None,
    alpha: float = 1.0,
    beta: float = 0.3,
    gamma: float = 1.0,
    time_budget_s: float = 30.0,
) -> tuple[SolverInput, dict]:
    """Convert the existing graph + raw_data into a SolverInput.

    Returns (solver_input, flow_metadata) where flow_metadata is a list
    aligned with solver_input.flows that records, per flow:
        - queue_name (the logical queue this flow carries messages on)
        - producer_app_ids (list of apps producing to that queue on src QM)
        - consumer_app_ids (list of apps consuming from that queue on tgt QM)

    flow_metadata is needed by apply_solver_output to attach human-readable
    annotations to the resulting channels for the UI and ADRs.

    Args:
        G: target graph from Architect (has 1:1 app→QM assignments)
        raw_data: state["raw_data"] dict from Sanitiser
        soft_penalties_from_llm: optional dict from Business Context Translator;
            if provided, used directly. If None, computed from app_metadata
            via rules-based fallback.
        alpha, beta, gamma, time_budget_s: solver parameters
    """
    # ── Extract QMs from the graph ───────────────────────────────────────
    qm_ids = [n for n, d in G.nodes(data=True) if d.get("type") == "qm"]
    if not qm_ids:
        raise ValueError("solver_input_from_graph: no QM nodes in graph")

    # ── Build app → QM map (1:1 expected; keep first if multi) ───────────
    app_qm: dict[str, str] = {}
    for app, qm, edata in G.edges(data=True):
        if edata.get("rel") != "connects_to":
            continue
        if app in app_qm:
            # Should not happen under strict 1:1; warn but keep first
            logger.warning(
                f"solver_input: app {app} has multiple QMs in graph "
                f"({app_qm[app]} and {qm}); keeping {app_qm[app]}"
            )
            continue
        app_qm[app] = qm

    if not app_qm:
        logger.warning("solver_input: no app→QM edges found in graph")

    # ── Detect flows by joining producers and consumers on queue_name ────
    apps_rows = raw_data.get("applications", [])

    # producers[queue_name] = set of app_ids that PRODUCE to this queue
    # consumers[queue_name] = set of app_ids that CONSUME from this queue
    producers: dict[str, set[str]] = defaultdict(set)
    consumers: dict[str, set[str]] = defaultdict(set)

    for row in apps_rows:
        app_id = row.get("app_id", "")
        queue_name = row.get("queue_name", "")
        direction = (row.get("direction", "") or "").upper()
        if not app_id or not queue_name:
            continue
        if direction in ("PRODUCER", "PUT"):
            producers[queue_name].add(app_id)
        elif direction in ("CONSUMER", "GET"):
            consumers[queue_name].add(app_id)
        # UNKNOWN direction: skip — we can't determine flow direction

    # Build flows: for each queue, every (producer_qm, consumer_qm) pair
    # where producer_qm != consumer_qm is a flow.
    # Collapse duplicates: same (src, tgt) pair from different queues = ONE flow
    # in the solver, but flow_metadata records all queue/app evidence.
    flow_to_metadata: dict[tuple[str, str], dict] = {}

    for queue_name in set(producers) | set(consumers):
        prod_apps = producers.get(queue_name, set())
        cons_apps = consumers.get(queue_name, set())

        # Map apps to QMs (skip apps not in app_qm — they had no graph edge)
        prod_qms_with_apps: dict[str, list[str]] = defaultdict(list)
        for app in prod_apps:
            qm = app_qm.get(app)
            if qm:
                prod_qms_with_apps[qm].append(app)

        cons_qms_with_apps: dict[str, list[str]] = defaultdict(list)
        for app in cons_apps:
            qm = app_qm.get(app)
            if qm:
                cons_qms_with_apps[qm].append(app)

        # Cross-product, skipping same-QM pairs (no channel needed)
        for src_qm, prod_app_list in prod_qms_with_apps.items():
            for tgt_qm, cons_app_list in cons_qms_with_apps.items():
                if src_qm == tgt_qm:
                    continue
                key = (src_qm, tgt_qm)
                if key not in flow_to_metadata:
                    flow_to_metadata[key] = {
                        "queues": [],
                        "producer_apps": [],
                        "consumer_apps": [],
                    }
                flow_to_metadata[key]["queues"].append(queue_name)
                flow_to_metadata[key]["producer_apps"].extend(prod_app_list)
                flow_to_metadata[key]["consumer_apps"].extend(cons_app_list)

    # Convert to ordered list (solver expects list-of-tuples)
    flows: list[tuple[str, str]] = []
    flow_metadata: list[dict] = []
    for (src, tgt), meta in flow_to_metadata.items():
        flows.append((src, tgt))
        flow_metadata.append({
            "src_qm": src,
            "tgt_qm": tgt,
            "queues": sorted(set(meta["queues"])),
            "producer_apps": sorted(set(meta["producer_apps"])),
            "consumer_apps": sorted(set(meta["consumer_apps"])),
        })

    # ── Compile soft penalties ───────────────────────────────────────────
    soft_penalties: dict[tuple[int, tuple[str, str]], float] = {}
    if soft_penalties_from_llm is not None:
        # Trust the LLM output (validated upstream); just pass through.
        soft_penalties = soft_penalties_from_llm
    else:
        # Rules-based fallback: penalize routing through a QM whose dominant
        # business classification differs from the flow's apps' classification.
        # This is a placeholder for the Business Context Translator — does
        # the right kind of thing without claiming combinatorial intelligence.
        soft_penalties = _rule_based_soft_penalties(
            flows, flow_metadata, raw_data, app_qm
        )

    inp = SolverInput(
        qms=qm_ids,
        flows=flows,
        soft_penalties=soft_penalties,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        time_budget_s=time_budget_s,
    )

    logger.info(
        f"solver_input: {len(qm_ids)} QMs, {len(flows)} flows, "
        f"{len(soft_penalties)} soft penalties; "
        f"α={alpha}, β={beta}, γ={gamma}, budget={time_budget_s}s"
    )

    return inp, flow_metadata


def _rule_based_soft_penalties(
    flows: list[tuple[str, str]],
    flow_metadata: list[dict],
    raw_data: dict,
    app_qm: dict[str, str],
) -> dict[tuple[int, tuple[str, str]], float]:
    """Rules-based soft penalties for compliance-aware routing.

    Penalize a flow f traversing edge (i, j) when QM j has apps with a
    dominant data classification different from the flow's apps' dominant
    classification. Rule of thumb only — the LLM's Business Context Translator
    is the real source of these weights, this is just a sane fallback.

    Returns dict {(flow_idx, edge): penalty_weight}.
    """
    app_metadata = raw_data.get("app_metadata", {})
    if not app_metadata:
        return {}

    # Compute dominant classification per QM
    qm_classification: dict[str, str] = {}
    qm_apps: dict[str, list[str]] = defaultdict(list)
    for app, qm in app_qm.items():
        qm_apps[qm].append(app)

    for qm, apps in qm_apps.items():
        # Mode of data_classification across apps on this QM
        classes = [
            app_metadata.get(a, {}).get("data_classification", "UNKNOWN")
            for a in apps
        ]
        if classes:
            qm_classification[qm] = max(set(classes), key=classes.count)
        else:
            qm_classification[qm] = "UNKNOWN"

    penalties: dict[tuple[int, tuple[str, str]], float] = {}

    for f_idx, (src, tgt) in enumerate(flows):
        meta = flow_metadata[f_idx]
        # Flow's classification = mode of all participating apps' classification
        all_apps = meta["producer_apps"] + meta["consumer_apps"]
        flow_classes = [
            app_metadata.get(a, {}).get("data_classification", "UNKNOWN")
            for a in all_apps
        ]
        if not flow_classes:
            continue
        flow_class = max(set(flow_classes), key=flow_classes.count)

        # Penalize traversal through any QM whose classification differs
        # AND is more sensitive than UNKNOWN
        for qm, qm_class in qm_classification.items():
            if qm == src or qm == tgt:
                # Endpoints are fixed; only intermediate hops count
                continue
            if qm_class != flow_class and qm_class != "UNKNOWN" and flow_class != "UNKNOWN":
                # Penalty applies to any incoming edge to this QM for this flow
                for src_edge in [n for n in qm_classification if n != qm]:
                    penalties[(f_idx, (src_edge, qm))] = 0.5
                    # 0.5 = mild. LLM would produce graded weights up to ~10.

    return penalties


# ─────────────────────────────────────────────────────────────────────────────
# SolverOutput → Graph mutations
# ─────────────────────────────────────────────────────────────────────────────

def apply_solver_output(
    G: nx.DiGraph,
    out: SolverOutput,
    inp: SolverInput,
    flow_metadata: Optional[list[dict]] = None,
) -> nx.DiGraph:
    """Apply the solver's chosen channels to the graph.

    Returns a NEW graph with:
        - All existing nodes preserved.
        - All existing 'connects_to' (app→QM) and 'owns' (QM→queue) edges preserved.
        - All existing 'channel' (QM→QM) edges REMOVED.
        - Solver-chosen channels ADDED with deterministic name FROM_QM.TO_QM
          and metadata recording which flows traverse each channel.

    If solver did not find a feasible solution (status not OPTIMAL or FEASIBLE),
    returns the input graph unchanged and logs a warning. Caller should check
    out.status before relying on this.
    """
    if out.status not in ("OPTIMAL", "FEASIBLE"):
        logger.warning(
            f"apply_solver_output: solver status={out.status}; "
            f"returning graph unchanged"
        )
        return G.copy()

    H = G.copy()

    # Remove existing channel edges
    channel_edges_to_remove = [
        (u, v) for u, v, d in H.edges(data=True) if d.get("rel") == "channel"
    ]
    H.remove_edges_from(channel_edges_to_remove)
    logger.info(
        f"apply_solver_output: removed {len(channel_edges_to_remove)} "
        f"existing channels"
    )

    # Build channel → flows-using-it map
    channel_to_flows: dict[tuple[str, str], list[int]] = defaultdict(list)
    for f_idx, route in out.flow_routes.items():
        for edge in route:
            channel_to_flows[edge].append(f_idx)

    # Add chosen channels with metadata
    for (src_qm, tgt_qm) in out.channels_chosen:
        if src_qm not in H.nodes or tgt_qm not in H.nodes:
            logger.warning(
                f"apply_solver_output: channel {src_qm}->{tgt_qm} references "
                f"missing node; skipping"
            )
            continue

        flows_on_this = channel_to_flows.get((src_qm, tgt_qm), [])

        # Channel name follows the deterministic convention from the brief:
        # SENDER channel: FROM_QM.TO_QM
        channel_name = f"{src_qm}.{tgt_qm}"
        xmit_queue = f"{tgt_qm}.XMITQ"  # standard naming pattern

        # Aggregate flow metadata for the human-readable annotation
        annotation_evidence = []
        for f_idx in flows_on_this:
            if flow_metadata and f_idx < len(flow_metadata):
                m = flow_metadata[f_idx]
                annotation_evidence.append({
                    "src_qm": m["src_qm"],
                    "tgt_qm": m["tgt_qm"],
                    "queues": m["queues"],
                    "producer_apps": m["producer_apps"],
                    "consumer_apps": m["consumer_apps"],
                })

        H.add_edge(
            src_qm, tgt_qm,
            rel="channel",
            channel_name=channel_name,
            status="RUNNING",
            xmit_queue=xmit_queue,
            # NEW: solver provenance — for ADRs and counterfactuals
            solver_chosen=True,
            flows_using=flows_on_this,
            flow_evidence=annotation_evidence,
        )

    logger.info(
        f"apply_solver_output: added {len(out.channels_chosen)} solver-chosen "
        f"channels"
    )

    return H


# ─────────────────────────────────────────────────────────────────────────────
# Convenience: full pipeline
# ─────────────────────────────────────────────────────────────────────────────

def run_solver_on_graph(
    G: nx.DiGraph,
    raw_data: dict,
    *,
    soft_penalties_from_llm: Optional[dict] = None,
    alpha: float = 1.0,
    beta: float = 0.3,
    gamma: float = 1.0,
    time_budget_s: float = 30.0,
) -> tuple[nx.DiGraph, SolverOutput, dict]:
    """End-to-end: graph in → optimised graph + solver output + flow metadata out.

    The convenience function the optimizer_agent will call.
    """
    from backend.solver.cpsat_solver import solve

    inp, flow_metadata = solver_input_from_graph(
        G, raw_data,
        soft_penalties_from_llm=soft_penalties_from_llm,
        alpha=alpha, beta=beta, gamma=gamma,
        time_budget_s=time_budget_s,
    )
    out = solve(inp)
    optimised = apply_solver_output(G, out, inp, flow_metadata=flow_metadata)

    return optimised, out, {
        "input": inp,
        "flow_metadata": flow_metadata,
    }
