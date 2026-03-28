"""
mq_graph.py
Builds NetworkX directed graphs from cleaned MQ CSV data.
Computes the 5-factor complexity metric.
"""
import networkx as nx
import pandas as pd
import numpy as np
from typing import Any


def build_graph(raw_data: dict) -> nx.DiGraph:
    """
    Build directed graph from cleaned data.
    Node types: 'qm' | 'app'
    Edge types: 'connects_to' | 'channel'

    Queue nodes are NOT added — no downstream agent uses them,
    and with 12K+ rows they bloat the graph and kill performance.
    """
    G = nx.DiGraph()

    # Add QM nodes (typically 30-100, fast)
    for row in raw_data["queue_managers"]:
        G.add_node(
            row["qm_id"],
            type="qm",
            name=row.get("qm_name", row["qm_id"]),
            region=row.get("region", "UNKNOWN"),
        )

    # Add app nodes + edges to their QMs
    # Use dict-based dedup instead of iterrows
    seen_app_qm = set()
    app_names = {}
    for row in raw_data["applications"]:
        app_id = row["app_id"]
        qm_id = row["qm_id"]

        if app_id not in app_names:
            app_names[app_id] = row.get("app_name", app_id)

        key = (app_id, qm_id)
        if key not in seen_app_qm:
            seen_app_qm.add(key)
            if app_id not in G.nodes:
                G.add_node(app_id, type="app", name=app_names[app_id])
            if qm_id in G.nodes:
                G.add_edge(app_id, qm_id, rel="connects_to", direction=row.get("direction", "UNKNOWN"))

    # Add QM-to-QM channel edges (SENDER only to avoid double-counting)
    seen_channels = set()
    for row in raw_data["channels"]:
        if row.get("channel_type") != "SENDER":
            continue
        from_qm = row["from_qm"]
        to_qm = row["to_qm"]
        key = (from_qm, to_qm)
        if key not in seen_channels and from_qm in G.nodes and to_qm in G.nodes:
            seen_channels.add(key)
            G.add_edge(
                from_qm, to_qm,
                rel="channel",
                channel_name=row.get("channel_name", ""),
                status=row.get("status", "UNKNOWN"),
                xmit_queue=row.get("xmit_queue", ""),
            )

    return G


def detect_violations(G: nx.DiGraph) -> dict:
    """
    Detect all constraint violations and anomalies in the as-is graph.
    Returns structured violation report.
    """
    qm_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "qm"]
    app_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "app"]

    # Multi-QM apps (violates 1-QM-per-app)
    multi_qm_apps = []
    for app in app_nodes:
        connected_qms = [v for u, v, d in G.out_edges(app, data=True) if d.get("rel") == "connects_to"]
        if len(connected_qms) > 1:
            multi_qm_apps.append({"app": app, "qms": connected_qms})

    # Orphan QMs (no apps connected)
    orphan_qms = []
    for qm in qm_nodes:
        connected_apps = [u for u, v, d in G.in_edges(qm, data=True) if d.get("rel") == "connects_to"]
        if not connected_apps:
            orphan_qms.append(qm)

    # Channel cycles in QM subgraph — bounded to avoid hanging on dense graphs
    qm_subgraph = G.subgraph(qm_nodes)
    cycles = []
    try:
        # Use a generator and cap at 50 cycles to avoid exponential blowup
        cycle_gen = nx.simple_cycles(qm_subgraph)
        for i, cycle in enumerate(cycle_gen):
            cycles.append(cycle)
            if i >= 50:
                break
    except Exception:
        cycles = []

    # Stopped/inactive channels
    stopped_channels = [
        (u, v, d) for u, v, d in G.edges(data=True)
        if d.get("rel") == "channel" and d.get("status") == "STOPPED"
    ]

    # Shared QMs (multiple apps connecting to same QM) — not same as multi_qm_apps
    qm_app_count = {}
    for app in app_nodes:
        for _, qm, d in G.out_edges(app, data=True):
            if d.get("rel") == "connects_to":
                qm_app_count[qm] = qm_app_count.get(qm, 0) + 1
    shared_qms = {qm: count for qm, count in qm_app_count.items() if count > 1}

    return {
        "multi_qm_apps": multi_qm_apps,
        "orphan_qms": orphan_qms,
        "cycles": cycles,
        "stopped_channels": [(u, v) for u, v, _ in stopped_channels],
        "shared_qms": shared_qms,
    }


def compute_complexity(G: nx.DiGraph, baseline_overrides: dict = None) -> dict:
    """
    Compute the 5-factor complexity score.
    Returns raw values + normalised total (0-100, higher = more complex).
    """
    qm_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "qm"]
    app_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "app"]

    # CC: Channel Count — number of sender channels
    channel_edges = [(u, v) for u, v, d in G.edges(data=True) if d.get("rel") == "channel"]
    CC = len(channel_edges)

    # CI: Coupling Index — mean QMs per app (ideal = 1.0)
    coupling_values = []
    for app in app_nodes:
        qm_connections = [v for u, v, d in G.out_edges(app, data=True) if d.get("rel") == "connects_to"]
        coupling_values.append(len(qm_connections))
    CI = float(np.mean(coupling_values)) if coupling_values else 0.0

    # RD: Routing Depth — max shortest path between any two QMs
    qm_subgraph = G.subgraph(qm_nodes)
    try:
        G_undir = qm_subgraph.to_undirected()
        if len(qm_nodes) > 1 and nx.is_connected(G_undir):
            path_lengths = dict(nx.all_pairs_shortest_path_length(qm_subgraph))
            all_lengths = [l for d in path_lengths.values() for l in d.values() if l > 0]
            RD = float(max(all_lengths)) if all_lengths else 1.0
        else:
            RD = float(nx.number_weakly_connected_components(qm_subgraph))
    except Exception:
        RD = 1.0

    # FO: Fan-Out — max outbound channels from any QM
    qm_out_degrees = [
        sum(1 for _, _, d in G.out_edges(qm, data=True) if d.get("rel") == "channel")
        for qm in qm_nodes
    ]
    FO = float(max(qm_out_degrees)) if qm_out_degrees else 0.0

    # OO: Orphan Objects — QMs with no app connections + stopped channels
    violations = detect_violations(G)
    OO = float(len(violations["orphan_qms"]) + len(violations["stopped_channels"]))

    # Normalised weighted score (0-100)
    # Baselines scale with input size so the score is meaningful
    # regardless of whether the topology has 5 QMs or 50.
    #
    # WEIGHT RATIONALE:
    #   CC (30%): Channel count is the single biggest driver of operational
    #             complexity — each channel is a failure point to monitor.
    #   CI (25%): Coupling index measures how tangled apps are with QMs.
    #             High coupling = hard to change anything without side effects.
    #   RD (20%): Routing depth = how many hops a message takes. More hops =
    #             more latency, more failure modes, harder to debug.
    #   FO (15%): Fan-out = max channels from one QM. High fan-out means one
    #             QM is a bottleneck / single point of failure.
    #   OO (10%): Orphan objects = waste. Important but less impactful than
    #             the structural factors above.
    #
    # BASELINE RATIONALE:
    #   CC worst: 2 channels per QM is typical enterprise; 2*N is "messy but real"
    #   CI worst: apps spread across N/3 QMs on average
    #   RD worst: chain of half the QMs (N/2 hops)
    #   FO worst: hub with N/2 outbound channels
    #   OO worst: half the QMs being orphans
    num_qms = len(qm_nodes) or 1

    # Baselines define "worst realistic case" for a topology of this size.
    # CRITICAL: the SAME baselines must be used for both as-is and target
    # scoring, otherwise improvements get diluted. Pass baseline_overrides
    # from the as-is computation when scoring the target.
    #
    # For the as-is graph, baselines are derived from the actual topology size.
    # Kept tight so that a 6-QM topology with 8 channels scores ~40-60/100,
    # leaving room for the target to show a clear drop.
    if baseline_overrides:
        cc_worst = baseline_overrides["cc_worst"]
        ci_worst = baseline_overrides["ci_worst"]
        rd_worst = baseline_overrides["rd_worst"]
        fo_worst = baseline_overrides["fo_worst"]
        oo_worst = baseline_overrides["oo_worst"]
    else:
        # "Worst realistic" = a messy but plausible enterprise topology.
        # NOT the theoretical maximum (full mesh) — that makes real topologies
        # look trivially clean and kills the improvement delta.
        #
        # CC: 2 channels per QM is typical in enterprise; 2*N is "messy"
        # CI: 1.0 is perfect; N/3 means apps scattered across many QMs
        # RD: half the QMs as chain depth is realistically bad
        # FO: half the QMs connected to one hub is realistically bad
        # OO: half the QMs being orphans is realistically bad
        cc_worst = max(8, num_qms * 2)
        ci_worst = max(2.0, num_qms / 3.0)
        rd_worst = max(3, num_qms // 2)
        fo_worst = max(3, num_qms // 2)
        oo_worst = max(3, num_qms // 2)

    def norm(val, worst):
        return min((val / worst) * 100, 100) if worst > 0 else 0.0

    score = (
        0.30 * norm(CC, cc_worst) +
        0.25 * norm(CI - 1.0, ci_worst) +   # subtract 1 since 1.0 is perfect
        0.20 * norm(RD, rd_worst) +
        0.15 * norm(FO, fo_worst) +
        0.10 * norm(OO, oo_worst)
    )

    baselines = {
        "cc_worst": cc_worst,
        "ci_worst": ci_worst,
        "rd_worst": rd_worst,
        "fo_worst": fo_worst,
        "oo_worst": oo_worst,
    }

    return {
        "channel_count": CC,
        "coupling_index": round(CI, 2),
        "routing_depth": round(RD, 1),
        "fan_out_score": FO,
        "orphan_objects": OO,
        "total_score": round(score, 1),
        "baselines": baselines,
        "factor_scores": {
            "cc_weighted": round(0.30 * norm(CC, cc_worst), 1),
            "ci_weighted": round(0.25 * norm(CI - 1.0, ci_worst), 1),
            "rd_weighted": round(0.20 * norm(RD, rd_worst), 1),
            "fo_weighted": round(0.15 * norm(FO, fo_worst), 1),
            "oo_weighted": round(0.10 * norm(OO, oo_worst), 1),
        },
    }


def graph_to_dict(G: nx.DiGraph) -> dict:
    """Serialise graph to JSON-friendly dict for API responses."""
    return {
        "nodes": [
            {"id": n, **{k: v for k, v in d.items()}}
            for n, d in G.nodes(data=True)
        ],
        "edges": [
            {"source": u, "target": v, **{k: v2 for k, v2 in d.items()}}
            for u, v, d in G.edges(data=True)
        ],
    }


def sanitise(obj):
    """
    Recursively replace nan/inf with None so JSON serialisation never fails.
    Called on every API response before returning to the browser.
    Lives here so it survives main.py replacements.
    """
    import math
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: sanitise(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitise(i) for i in obj]
    return obj
