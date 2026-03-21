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
    Node types: 'qm' | 'app' | 'queue'
    Edge types: 'connects_to' | 'channel' | 'owns'
    """
    G = nx.DiGraph()

    qm_df = pd.DataFrame(raw_data["queue_managers"])
    app_df = pd.DataFrame(raw_data["applications"])
    queue_df = pd.DataFrame(raw_data["queues"])
    channel_df = pd.DataFrame(raw_data["channels"])

    # Add QM nodes
    for _, row in qm_df.iterrows():
        G.add_node(
            row["qm_id"],
            type="qm",
            name=row.get("qm_name", row["qm_id"]),
            region=row.get("region", "UNKNOWN"),
        )

    # Add app nodes + edges to their QMs
    seen_apps = {}
    for _, row in app_df.iterrows():
        app_id = row["app_id"]
        qm_id = row["qm_id"]

        if app_id not in G.nodes:
            G.add_node(app_id, type="app", name=row.get("app_name", app_id))

        if qm_id in G.nodes:
            # Track unique QM connections per app
            if app_id not in seen_apps:
                seen_apps[app_id] = set()
            if qm_id not in seen_apps[app_id]:
                seen_apps[app_id].add(qm_id)
                G.add_edge(app_id, qm_id, rel="connects_to", direction=row.get("direction", "UNKNOWN"))

    # Add queue nodes + ownership edges
    for _, row in queue_df.iterrows():
        q_id = row["queue_id"]
        qm_id = row["qm_id"]
        G.add_node(q_id, type="queue", name=row.get("queue_name", q_id), usage=row.get("usage", "NORMAL"))
        if qm_id in G.nodes:
            G.add_edge(qm_id, q_id, rel="owns")

    # Add QM-to-QM channel edges
    for _, row in channel_df.iterrows():
        from_qm = row["from_qm"]
        to_qm = row["to_qm"]
        ctype = row.get("channel_type", "")
        # Only add SENDER channels as directed edges to avoid double-counting
        if ctype == "SENDER" and from_qm in G.nodes and to_qm in G.nodes:
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

    # Channel cycles in QM subgraph
    qm_subgraph = G.subgraph(qm_nodes)
    cycles = list(nx.simple_cycles(qm_subgraph))

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


def compute_complexity(G: nx.DiGraph) -> dict:
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
    # Normalisation baselines derived from worst-case estimates
    def norm(val, worst):
        return min((val / worst) * 100, 100) if worst > 0 else 0.0

    score = (
        0.30 * norm(CC, 20) +
        0.25 * norm(CI - 1.0, 3.0) +   # subtract 1 since 1.0 is perfect
        0.20 * norm(RD, 8) +
        0.15 * norm(FO, 10) +
        0.10 * norm(OO, 10)
    )

    return {
        "channel_count": CC,
        "coupling_index": round(CI, 2),
        "routing_depth": round(RD, 1),
        "fan_out_score": FO,
        "orphan_objects": OO,
        "total_score": round(score, 1),
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
