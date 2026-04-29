"""
backend/solver/required_pairs.py

Derive the set of required directed (src_qm, tgt_qm) connectivity pairs from
raw_data + the architect's app→QM assignments.

Replaces the multi-commodity flow logic in adapters.py. The reformulation:

  OLD (multi-commodity flow):
    For each (producer_qm, consumer_qm) pair on each queue, treat it as a
    commodity. Variables: x_{ij} per channel + r^f_{ij} per flow per edge.
    At 480 QMs and 5K flows: ~10^9 binary variables. Unsolvable.

  NEW (directed connectivity pairs):
    For each unique (producer_qm, consumer_qm) pair where the two QMs share
    at least one queue with appropriate roles, mark (producer_qm, consumer_qm)
    as required. We need *some* directed path between them in the channel
    graph. Variables: only x_{ij}. At 480 QMs: ~230K binary variables, but
    we don't even need them — we use a polynomial-time per-source tree
    construction (see steiner_solver.py).

The required-pairs problem is Directed Steiner Network. The per-source
decomposition is Minimum Steiner Arborescence. We use shortest-path-tree
(Takahashi-Matsuyama 1980, 2-approximation) with optional Dreyfus-Wagner
refinement (1972, exact for small terminal counts).

Cites:
  - Takahashi & Matsuyama 1980, "An approximate solution for the Steiner
    problem in graphs", Mathematica Japonica 24:573-577.
  - Dreyfus & Wagner 1972, "The Steiner problem in graphs", Networks 1:195-207.

USAGE:
    from backend.solver.required_pairs import derive_required_pairs

    pairs, metadata = derive_required_pairs(target_graph, raw_data)
    # pairs: list[tuple[src_qm, tgt_qm]] -- unique, no self-pairs
    # metadata: list[dict] aligned with pairs, each entry records the
    #           queues + producer apps + consumer apps that justify this pair
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Optional

import networkx as nx

logger = logging.getLogger(__name__)


def derive_required_pairs(
    G: nx.DiGraph,
    raw_data: dict,
) -> tuple[list[tuple[str, str]], list[dict]]:
    """Compute the required directed connectivity pairs from the target graph
    and raw_data.

    A pair (src_qm, tgt_qm) is *required* iff there exists at least one
    queue Q and at least one app pair (a_p on src_qm, a_c on tgt_qm)
    such that a_p produces to Q and a_c consumes from Q. Same-QM pairs
    (src_qm == tgt_qm) are excluded — no channel needed.

    Args:
        G: target graph from architect, with 1:1 app→QM connects_to edges
           and qm/app/queue node types.
        raw_data: state["raw_data"] with applications/queues/channels lists.

    Returns:
        (pairs, metadata):
          pairs: list of unique (src_qm, tgt_qm) tuples
          metadata: list of dicts, parallel to pairs, each with keys:
            - src_qm, tgt_qm
            - queues:         sorted unique list of queue names justifying this pair
            - producer_apps:  sorted unique list of producer app_ids
            - consumer_apps:  sorted unique list of consumer app_ids
    """
    # Build app→QM map from the graph (1:1 expected; warn if multiple)
    app_qm: dict[str, str] = {}
    for app, qm, edata in G.edges(data=True):
        if edata.get("rel") != "connects_to":
            continue
        if app in app_qm:
            logger.warning(
                f"required_pairs: app {app} has multiple QMs in graph "
                f"({app_qm[app]} and {qm}); keeping {app_qm[app]}"
            )
            continue
        app_qm[app] = qm

    if not app_qm:
        logger.warning("required_pairs: no app→QM edges found in graph")
        return [], []

    # Group producers and consumers by queue name (logical queue, not physical)
    producers: dict[str, set[str]] = defaultdict(set)  # queue_name → app_ids
    consumers: dict[str, set[str]] = defaultdict(set)

    for row in raw_data.get("applications", []):
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

    # Aggregate pair → metadata
    pair_meta: dict[tuple[str, str], dict] = {}

    for qname in set(producers) | set(consumers):
        prod_apps = producers.get(qname, set())
        cons_apps = consumers.get(qname, set())

        # Map apps to QMs (skip apps not in app_qm)
        prod_by_qm: dict[str, list[str]] = defaultdict(list)
        for a in prod_apps:
            qm = app_qm.get(a)
            if qm:
                prod_by_qm[qm].append(a)

        cons_by_qm: dict[str, list[str]] = defaultdict(list)
        for a in cons_apps:
            qm = app_qm.get(a)
            if qm:
                cons_by_qm[qm].append(a)

        # Cross-product, skip same-QM pairs
        for src_qm, p_list in prod_by_qm.items():
            for tgt_qm, c_list in cons_by_qm.items():
                if src_qm == tgt_qm:
                    continue
                key = (src_qm, tgt_qm)
                meta = pair_meta.setdefault(key, {
                    "queues": set(),
                    "producer_apps": set(),
                    "consumer_apps": set(),
                })
                meta["queues"].add(qname)
                meta["producer_apps"].update(p_list)
                meta["consumer_apps"].update(c_list)

    # Materialize ordered list (deterministic ordering for reproducibility)
    pairs: list[tuple[str, str]] = []
    metadata: list[dict] = []
    for (src, tgt) in sorted(pair_meta.keys()):
        meta = pair_meta[(src, tgt)]
        pairs.append((src, tgt))
        metadata.append({
            "src_qm": src,
            "tgt_qm": tgt,
            "queues": sorted(meta["queues"]),
            "producer_apps": sorted(meta["producer_apps"]),
            "consumer_apps": sorted(meta["consumer_apps"]),
        })

    logger.info(
        f"required_pairs: derived {len(pairs)} unique (src, tgt) pairs "
        f"from {len(producers)} producer-queues and {len(consumers)} consumer-queues "
        f"across {len(app_qm)} apps on {len(set(app_qm.values()))} QMs"
    )

    return pairs, metadata


def group_pairs_by_source(
    pairs: list[tuple[str, str]],
) -> dict[str, list[str]]:
    """Reorganize pairs by source QM. Returns {src_qm: [tgt_qm, ...]}.

    This is the form the per-source Steiner solver consumes: for each source,
    a list of targets it must reach.
    """
    by_src: dict[str, list[str]] = defaultdict(list)
    for src, tgt in pairs:
        by_src[src].append(tgt)
    # Sort target lists for determinism
    return {s: sorted(set(ts)) for s, ts in by_src.items()}
