"""
backend/solver/reachability_validator.py

Validates that a target topology preserves all required directed-pair
reachability. Standalone module so it can be:

  1. Used by tester_agent as a new V-005 validation rule.
  2. Run independently as a forensic tool on existing pipeline outputs.
  3. Used as a regression invariant: every solver/optimizer run, regardless
     of method, must produce a target where this validator returns no
     violations. If the legacy MST path violates it, that's a real bug
     this validator surfaces.

USAGE:
    from backend.solver.reachability_validator import (
        find_unreachable_pairs,
        format_violation,
    )

    unreachable = find_unreachable_pairs(target_graph, raw_data)
    if unreachable:
        for (src, tgt, reason) in unreachable:
            print(format_violation(src, tgt, reason))
"""
from __future__ import annotations

import logging
from typing import Optional

import networkx as nx

logger = logging.getLogger(__name__)


def find_unreachable_pairs(
    G: nx.DiGraph,
    raw_data: dict,
    max_report: int = 100,
) -> list[tuple[str, str, str]]:
    """Return pairs (src_qm, tgt_qm, reason) that should have a directed
    channel path in G but don't.

    Args:
        G: target topology (must have qm/app/queue nodes; channel edges
           identified by edata["rel"]=="channel").
        raw_data: state["raw_data"] for required-pair derivation.
        max_report: cap the number of returned violations to keep output
                    manageable when something is badly broken.

    Returns:
        list of (src, tgt, reason) tuples. Empty list = topology is valid.
        reason is one of: "missing_src", "missing_tgt", "no_directed_path".
    """
    from backend.solver.required_pairs import derive_required_pairs

    pairs, _ = derive_required_pairs(G, raw_data)
    if not pairs:
        return []

    # Build channel-only directed graph (the relevant subgraph for routing)
    qm_nodes = {n for n, d in G.nodes(data=True) if d.get("type") == "qm"}
    ch_only = nx.DiGraph()
    ch_only.add_nodes_from(qm_nodes)
    for u, v, d in G.edges(data=True):
        if d.get("rel") == "channel" and u in qm_nodes and v in qm_nodes:
            ch_only.add_edge(u, v)

    violations: list[tuple[str, str, str]] = []
    for (s, t) in pairs:
        if s not in ch_only:
            violations.append((s, t, "missing_src"))
        elif t not in ch_only:
            violations.append((s, t, "missing_tgt"))
        elif not nx.has_path(ch_only, s, t):
            violations.append((s, t, "no_directed_path"))
        if len(violations) >= max_report:
            break

    if violations:
        logger.warning(
            f"reachability_validator: {len(violations)} unreachable pair(s) "
            f"in target topology (capped at {max_report}); "
            f"first example: {violations[0]}"
        )

    return violations


def format_violation(src: str, tgt: str, reason: str) -> str:
    """Human-readable error string for a single reachability violation."""
    reasons = {
        "missing_src": f"source QM {src} not present in target",
        "missing_tgt": f"target QM {tgt} not present in target",
        "no_directed_path": f"no directed channel path from {src} to {tgt}",
    }
    return f"REQUIRED_PAIR_REACHABILITY: {src} → {tgt} — {reasons.get(reason, reason)}"


def reachability_summary(
    G: nx.DiGraph,
    raw_data: dict,
) -> dict:
    """Numeric summary of reachability for the target.

    Returns:
        {
          "n_required_pairs": int,
          "n_reachable":      int,
          "n_unreachable":    int,
          "reachability_ratio": float in [0, 1],
        }
    """
    from backend.solver.required_pairs import derive_required_pairs

    pairs, _ = derive_required_pairs(G, raw_data)
    n_total = len(pairs)
    if n_total == 0:
        return {
            "n_required_pairs": 0,
            "n_reachable": 0,
            "n_unreachable": 0,
            "reachability_ratio": 1.0,
        }

    qm_nodes = {n for n, d in G.nodes(data=True) if d.get("type") == "qm"}
    ch_only = nx.DiGraph()
    ch_only.add_nodes_from(qm_nodes)
    for u, v, d in G.edges(data=True):
        if d.get("rel") == "channel" and u in qm_nodes and v in qm_nodes:
            ch_only.add_edge(u, v)

    n_reachable = 0
    for (s, t) in pairs:
        if s in ch_only and t in ch_only and nx.has_path(ch_only, s, t):
            n_reachable += 1

    return {
        "n_required_pairs": n_total,
        "n_reachable": n_reachable,
        "n_unreachable": n_total - n_reachable,
        "reachability_ratio": n_reachable / n_total,
    }
