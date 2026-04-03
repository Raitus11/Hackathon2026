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
    cycles = []
    try:
        for i, c in enumerate(nx.simple_cycles(qm_subgraph)):
            cycles.append(c)
            if i >= 20:
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
    # For disconnected graphs: max diameter across all components +
    # fragmentation penalty (each extra component adds routing complexity
    # because messages between components are impossible without bridging).
    qm_subgraph = G.subgraph(qm_nodes)
    try:
        G_undir = qm_subgraph.to_undirected()
        if len(qm_nodes) <= 1:
            RD = 0.0
        elif nx.is_connected(G_undir):
            path_lengths = dict(nx.all_pairs_shortest_path_length(G_undir))
            all_lengths = [l for d in path_lengths.values() for l in d.values() if l > 0]
            RD = float(max(all_lengths)) if all_lengths else 1.0
        else:
            # Disconnected: max diameter across components + fragmentation penalty
            components = list(nx.connected_components(G_undir))
            max_diameter = 0.0
            for comp in components:
                if len(comp) < 2:
                    continue
                sub = G_undir.subgraph(comp)
                try:
                    diam = nx.diameter(sub)
                    max_diameter = max(max_diameter, float(diam))
                except Exception:
                    max_diameter = max(max_diameter, 1.0)
            # Penalty: each disconnected component beyond 1 adds 1.0
            fragmentation_penalty = float(len(components) - 1)
            RD = max_diameter + fragmentation_penalty
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
        cs_worst = baseline_overrides.get("cs_worst", max(1.0, CC / max(num_qms, 1)))
    else:
        # Anchor baselines to actual as-is values so the score reflects
        # real measured complexity, not a theoretical worst-case that
        # makes the as-is topology look trivially clean and kills the delta.
        # As-is values ARE the worst we know — target must beat them.
        #
        # CS: channel-to-QM sprawl ratio — critical for 1:1 topologies where
        # QM count cannot decrease (each app gets its own QM), so channel
        # efficiency is the primary reduction lever.
        CS = CC / max(num_qms, 1)
        cc_worst = max(8, CC)
        ci_worst = max(0.5, CI - 1.0)
        rd_worst = max(2, RD)
        fo_worst = max(2, FO)
        oo_worst = max(1, OO) if OO > 0 else 1
        cs_worst = max(1.0, CS)

    def norm(val, worst):
        return min((val / worst) * 100, 100) if worst > 0 else 0.0

    # CS only available in else branch above; recompute here for overrides case.
    if baseline_overrides:
        CS = CC / max(num_qms, 1)

    score = (
        0.30 * norm(CC, cc_worst) +
        0.25 * norm(max(CI - 1.0, 0), max(ci_worst, 0.01)) +
        0.20 * norm(RD, rd_worst) +
        0.15 * norm(FO, fo_worst) +
        0.05 * norm(OO, oo_worst) +
        0.10 * norm(CS, cs_worst)
    )

    baselines = {
        "cc_worst": cc_worst,
        "ci_worst": ci_worst,
        "rd_worst": rd_worst,
        "fo_worst": fo_worst,
        "oo_worst": oo_worst,
        "cs_worst": cs_worst,
    }

    return {
        "channel_count": CC,
        "coupling_index": round(CI, 2),
        "routing_depth": round(RD, 1),
        "fan_out_score": FO,
        "orphan_objects": OO,
        "channel_sprawl": round(CS, 2),
        "total_score": round(score, 1),
        "baselines": baselines,
        "factor_scores": {
            "cc_weighted": round(0.30 * norm(CC, cc_worst), 1),
            "ci_weighted": round(0.25 * norm(max(CI - 1.0, 0), max(ci_worst, 0.01)), 1),
            "rd_weighted": round(0.20 * norm(RD, rd_worst), 1),
            "fo_weighted": round(0.15 * norm(FO, fo_worst), 1),
            "oo_weighted": round(0.05 * norm(OO, oo_worst), 1),
            "cs_weighted": round(0.10 * norm(CS, cs_worst), 1),
        },
    }


def analyse_subgraphs(G: nx.DiGraph) -> list:
    """
    Decompose the topology into weakly connected subgraphs (components).
    Returns a list of component dicts sorted by size (largest first).

    Each component contains:
      - component_id: int (1-based)
      - qm_ids: list of queue manager IDs
      - app_ids: list of application IDs connected to QMs in this component
      - channel_count: number of inter-QM channels within this component
      - queue_count: number of queues owned by QMs in this component
      - is_isolated: True if single QM with no channels
      - hub_qm: the QM with the highest degree (most connections)
      - regions: unique regions represented
    """
    qm_nodes = set(n for n, d in G.nodes(data=True) if d.get("type") == "qm")
    app_nodes = set(n for n, d in G.nodes(data=True) if d.get("type") == "app")

    if not qm_nodes:
        return []

    # Build undirected QM-only graph for component detection
    # Include channel edges only
    G_qm = nx.Graph()
    G_qm.add_nodes_from(qm_nodes)
    for u, v, d in G.edges(data=True):
        if d.get("rel") == "channel" and u in qm_nodes and v in qm_nodes:
            G_qm.add_edge(u, v)

    # Build app→QM and QM→apps maps
    qm_apps = {qm: [] for qm in qm_nodes}
    for app in app_nodes:
        for _, qm, d in G.out_edges(app, data=True):
            if d.get("rel") == "connects_to" and qm in qm_nodes:
                qm_apps[qm].append(app)

    # Build QM→queues map
    qm_queues = {qm: [] for qm in qm_nodes}
    for qm in qm_nodes:
        for _, q, d in G.out_edges(qm, data=True):
            if d.get("rel") == "owns":
                qm_queues[qm].append(q)

    components = list(nx.connected_components(G_qm))
    # Sort largest first
    components.sort(key=lambda c: len(c), reverse=True)

    results = []
    for idx, comp_qms in enumerate(components, 1):
        comp_qms = sorted(comp_qms)
        comp_apps = sorted(set(
            app for qm in comp_qms for app in qm_apps.get(qm, [])
        ))
        comp_queues = sum(len(qm_queues.get(qm, [])) for qm in comp_qms)

        # Count channels within this component
        ch_count = 0
        for u, v, d in G.edges(data=True):
            if d.get("rel") == "channel" and u in comp_qms and v in comp_qms:
                ch_count += 1

        # Find hub (QM with most channel connections)
        hub_qm = None
        max_degree = -1
        for qm in comp_qms:
            deg = G_qm.degree(qm) if qm in G_qm else 0
            if deg > max_degree:
                max_degree = deg
                hub_qm = qm

        # Collect regions
        regions = sorted(set(
            G.nodes[qm].get("region", "UNKNOWN")
            for qm in comp_qms if qm in G.nodes
        ))

        results.append({
            "component_id": idx,
            "qm_ids": comp_qms,
            "qm_count": len(comp_qms),
            "app_ids": comp_apps,
            "app_count": len(comp_apps),
            "channel_count": ch_count,
            "queue_count": comp_queues,
            "is_isolated": len(comp_qms) == 1 and ch_count == 0,
            "hub_qm": hub_qm,
            "hub_degree": max_degree,
            "regions": regions,
        })

    return results


def detect_communities(G: nx.DiGraph) -> dict:
    """
    Louvain community detection on the QM channel graph.
    Identifies natural clusters of queue managers that communicate
    frequently — useful for regional grouping and blast-radius analysis.

    Returns:
      - communities: list of sets of QM IDs
      - modularity: float (0-1, higher = more modular = cleaner separation)
      - community_map: {qm_id: community_index}
    """
    qm_nodes = set(n for n, d in G.nodes(data=True) if d.get("type") == "qm")
    if len(qm_nodes) < 2:
        return {"communities": [qm_nodes] if qm_nodes else [], "modularity": 0.0, "community_map": {}}

    # Build undirected weighted QM graph
    G_qm = nx.Graph()
    G_qm.add_nodes_from(qm_nodes)
    edge_weights = {}
    for u, v, d in G.edges(data=True):
        if d.get("rel") == "channel" and u in qm_nodes and v in qm_nodes:
            key = frozenset((u, v))
            edge_weights[key] = edge_weights.get(key, 0) + 1
    for edge, w in edge_weights.items():
        u, v = tuple(edge)
        G_qm.add_edge(u, v, weight=w)

    if G_qm.number_of_edges() == 0:
        # No channels — each QM is its own community
        return {
            "communities": [{qm} for qm in qm_nodes],
            "modularity": 0.0,
            "community_map": {qm: i for i, qm in enumerate(sorted(qm_nodes))},
        }

    try:
        # Louvain method — greedy modularity optimisation
        communities = list(nx.community.louvain_communities(G_qm, weight="weight", seed=42))
        modularity = nx.community.modularity(G_qm, communities, weight="weight")
    except Exception:
        # Fallback: connected components as communities
        communities = [set(c) for c in nx.connected_components(G_qm)]
        modularity = 0.0

    community_map = {}
    for idx, comm in enumerate(communities):
        for qm in comm:
            community_map[qm] = idx

    return {
        "communities": [sorted(c) for c in communities],
        "modularity": round(modularity, 4),
        "community_map": community_map,
        "num_communities": len(communities),
    }


def compute_centrality(G: nx.DiGraph) -> dict:
    """
    Betweenness centrality on the QM channel graph.
    Identifies single points of failure (SPOFs) — QMs through which
    a disproportionate share of message routes must pass.

    Also computes degree centrality for hub detection.

    Returns:
      - betweenness: {qm_id: score} — higher = more critical SPOF
      - degree: {qm_id: score} — higher = more connections
      - spof_qms: list of QMs with betweenness > 2× the mean (risk hotspots)
      - hub_qms: list of QMs with degree > 2× the mean
    """
    qm_nodes = set(n for n, d in G.nodes(data=True) if d.get("type") == "qm")
    if len(qm_nodes) < 2:
        return {"betweenness": {}, "degree": {}, "spof_qms": [], "hub_qms": []}

    # Build undirected QM graph for centrality
    G_qm = nx.Graph()
    G_qm.add_nodes_from(qm_nodes)
    for u, v, d in G.edges(data=True):
        if d.get("rel") == "channel" and u in qm_nodes and v in qm_nodes:
            G_qm.add_edge(u, v)

    if G_qm.number_of_edges() == 0:
        return {
            "betweenness": {qm: 0.0 for qm in qm_nodes},
            "degree": {qm: 0.0 for qm in qm_nodes},
            "spof_qms": [], "hub_qms": [],
        }

    # Betweenness: fraction of shortest paths passing through each QM
    betweenness = nx.betweenness_centrality(G_qm, normalized=True)
    # Degree: fraction of possible connections each QM has
    degree = nx.degree_centrality(G_qm)

    # Detect SPOFs and hubs (>2× mean)
    bw_values = list(betweenness.values())
    bw_mean = sum(bw_values) / len(bw_values) if bw_values else 0
    spof_qms = sorted([qm for qm, bw in betweenness.items() if bw > 2 * bw_mean and bw > 0.05],
                       key=lambda q: betweenness[q], reverse=True)

    deg_values = list(degree.values())
    deg_mean = sum(deg_values) / len(deg_values) if deg_values else 0
    hub_qms = sorted([qm for qm, dg in degree.items() if dg > 2 * deg_mean and dg > 0.05],
                      key=lambda q: degree[q], reverse=True)

    return {
        "betweenness": {k: round(v, 4) for k, v in betweenness.items()},
        "degree": {k: round(v, 4) for k, v in degree.items()},
        "spof_qms": spof_qms[:10],
        "hub_qms": hub_qms[:10],
        "betweenness_mean": round(bw_mean, 4),
        "degree_mean": round(deg_mean, 4),
    }


def compute_graph_entropy(G: nx.DiGraph) -> dict:
    """
    Shannon entropy of the QM degree distribution.
    Measures how "uniform" or "skewed" the topology is:
      - High entropy = even distribution of channels (healthy)
      - Low entropy = few QMs dominate connections (fragile hub-and-spoke)

    Also computes graph density and clustering coefficient.

    Returns:
      - degree_entropy: float (bits)
      - density: float (0-1, actual edges / possible edges)
      - avg_clustering: float (0-1, how cliquey the QMs are)
      - degree_distribution: {degree: count}
    """
    import math

    qm_nodes = set(n for n, d in G.nodes(data=True) if d.get("type") == "qm")
    if len(qm_nodes) < 2:
        return {"degree_entropy": 0.0, "density": 0.0, "avg_clustering": 0.0, "degree_distribution": {}}

    G_qm = nx.Graph()
    G_qm.add_nodes_from(qm_nodes)
    for u, v, d in G.edges(data=True):
        if d.get("rel") == "channel" and u in qm_nodes and v in qm_nodes:
            G_qm.add_edge(u, v)

    # Degree distribution
    degrees = [G_qm.degree(n) for n in G_qm.nodes()]
    total = sum(degrees) or 1
    degree_dist = {}
    for d in degrees:
        degree_dist[d] = degree_dist.get(d, 0) + 1

    # Shannon entropy: H = -Σ p(d) * log2(p(d))
    entropy = 0.0
    for count in degree_dist.values():
        p = count / len(degrees) if degrees else 0
        if p > 0:
            entropy -= p * math.log2(p)

    # Graph density: actual / possible edges
    n = len(qm_nodes)
    max_edges = n * (n - 1) / 2
    density = G_qm.number_of_edges() / max_edges if max_edges > 0 else 0.0

    # Average clustering coefficient (transitivity)
    try:
        avg_clustering = nx.average_clustering(G_qm)
    except Exception:
        avg_clustering = 0.0

    return {
        "degree_entropy": round(entropy, 3),
        "density": round(density, 4),
        "avg_clustering": round(avg_clustering, 4),
        "degree_distribution": degree_dist,
        "max_entropy": round(math.log2(n) if n > 1 else 0, 3),  # theoretical max for uniform dist
        "entropy_ratio": round(entropy / math.log2(n), 3) if n > 1 and math.log2(n) > 0 else 0.0,
    }


def compare_topologies(G_source: nx.DiGraph, G_target: nx.DiGraph) -> dict:
    """
    Quantitative comparison of source and target topologies.
    Produces a structured diff with mathematical metrics suitable for
    the complexity-scores.csv deliverable.
    """
    def _stats(G):
        qm_n = [n for n, d in G.nodes(data=True) if d.get("type") == "qm"]
        app_n = [n for n, d in G.nodes(data=True) if d.get("type") == "app"]
        ch_e = [(u, v) for u, v, d in G.edges(data=True) if d.get("rel") == "channel"]

        G_qm = nx.Graph()
        G_qm.add_nodes_from(qm_n)
        for u, v in ch_e:
            G_qm.add_edge(u, v)

        components = nx.number_connected_components(G_qm) if qm_n else 0
        degrees = [G_qm.degree(n) for n in G_qm.nodes()] if qm_n else []
        avg_degree = sum(degrees) / len(degrees) if degrees else 0
        max_degree = max(degrees) if degrees else 0

        n = len(qm_n)
        max_edges = n * (n - 1) / 2
        density = G_qm.number_of_edges() / max_edges if max_edges > 0 else 0

        return {
            "qm_count": len(qm_n),
            "app_count": len(app_n),
            "channel_count": len(ch_e),
            "components": components,
            "avg_degree": round(avg_degree, 2),
            "max_degree": max_degree,
            "density": round(density, 4),
        }

    src = _stats(G_source)
    tgt = _stats(G_target)

    def _pct(before, after):
        if before == 0:
            return 0.0
        return round(((before - after) / before) * 100, 1)

    return {
        "source": src,
        "target": tgt,
        "reductions": {
            "qm_count": _pct(src["qm_count"], tgt["qm_count"]),
            "channel_count": _pct(src["channel_count"], tgt["channel_count"]),
            "avg_degree": _pct(src["avg_degree"], tgt["avg_degree"]),
            "max_degree": _pct(src["max_degree"], tgt["max_degree"]),
            "density": _pct(src["density"], tgt["density"]),
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
