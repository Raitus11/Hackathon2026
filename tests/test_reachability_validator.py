"""
tests/test_reachability_validator.py

Tests for the reachability validator. The two crucial cases:

  1. A topology produced by a correct optimizer (no missing pairs) → valid.
  2. A topology that LOOKS connected but breaks specific directed pairs
     (the kind of breakage the legacy MST can produce) → flagged.
"""
import networkx as nx
import pytest

from backend.solver.reachability_validator import (
    find_unreachable_pairs,
    format_violation,
    reachability_summary,
)


def _build_simple_graph():
    """Three apps, three QMs, simple producer/consumer pairs."""
    G = nx.DiGraph()
    for q in ["QM_A", "QM_B", "QM_C"]:
        G.add_node(q, type="qm")
    for a in ["APP_1", "APP_2", "APP_3"]:
        G.add_node(a, type="app")
    G.add_edge("APP_1", "QM_A", rel="connects_to")
    G.add_edge("APP_2", "QM_B", rel="connects_to")
    G.add_edge("APP_3", "QM_C", rel="connects_to")
    return G


def _simple_raw_data():
    """APP_1 produces to Q1, APP_2 consumes from Q1.
    APP_2 produces to Q2, APP_3 consumes from Q2.
    APP_3 produces to Q3, APP_1 consumes from Q3.
    Required pairs: A→B, B→C, C→A (the directed cycle).
    """
    return {
        "applications": [
            {"app_id": "APP_1", "qm_id": "QM_A", "queue_name": "Q1", "direction": "PRODUCER"},
            {"app_id": "APP_2", "qm_id": "QM_B", "queue_name": "Q1", "direction": "CONSUMER"},
            {"app_id": "APP_2", "qm_id": "QM_B", "queue_name": "Q2", "direction": "PRODUCER"},
            {"app_id": "APP_3", "qm_id": "QM_C", "queue_name": "Q2", "direction": "CONSUMER"},
            {"app_id": "APP_3", "qm_id": "QM_C", "queue_name": "Q3", "direction": "PRODUCER"},
            {"app_id": "APP_1", "qm_id": "QM_A", "queue_name": "Q3", "direction": "CONSUMER"},
        ],
        "queue_managers": [],
        "queues": [],
        "channels": [],
        "app_metadata": {},
    }


def test_valid_topology_has_no_violations():
    """All three required pairs (A→B, B→C, C→A) have direct channels."""
    G = _build_simple_graph()
    G.add_edge("QM_A", "QM_B", rel="channel")
    G.add_edge("QM_B", "QM_C", rel="channel")
    G.add_edge("QM_C", "QM_A", rel="channel")

    violations = find_unreachable_pairs(G, _simple_raw_data())
    assert violations == []

    summary = reachability_summary(G, _simple_raw_data())
    assert summary["n_required_pairs"] == 3
    assert summary["n_reachable"] == 3
    assert summary["n_unreachable"] == 0
    assert summary["reachability_ratio"] == 1.0


def test_directed_cycle_is_valid_via_multi_hop():
    """Only A→B and B→C exist; C→A required but routed via... nothing.
    This should be caught."""
    G = _build_simple_graph()
    G.add_edge("QM_A", "QM_B", rel="channel")
    G.add_edge("QM_B", "QM_C", rel="channel")
    # Missing: C→A. Required pair (C, A) has no path.

    violations = find_unreachable_pairs(G, _simple_raw_data())
    assert len(violations) == 1
    src, tgt, reason = violations[0]
    assert src == "QM_C"
    assert tgt == "QM_A"
    assert reason == "no_directed_path"


def test_undirected_optimizer_failure_caught():
    """SIMULATE THE LEGACY MST BUG: undirected MST believes the topology is
    connected because there's an undirected path A-B-C-A. But required pair
    (B, A) needs DIRECTED B→A. With only A→B, B→C, C→A:
      B reaches C (B→C) ✓
      B reaches A? B→C→A ✓  ← actually reachable
      A reaches B (A→B) ✓
      A reaches C? A→B→C ✓  ← reachable
      C reaches A (C→A) ✓
      C reaches B? C→A→B ✓  ← reachable
    So a 3-cycle in directed form IS strongly connected. Bad example for
    the bug. Let me use a different one.

    Better: A→B, A→C. Required pair (B, A). No path B→A, no path B→C either.
    """
    G = _build_simple_graph()
    G.add_edge("QM_A", "QM_B", rel="channel")
    G.add_edge("QM_A", "QM_C", rel="channel")

    # Required pairs from raw_data: A→B, B→C, C→A.
    # B→C: B has no outbound, unreachable.
    # C→A: C has no outbound, unreachable.

    violations = find_unreachable_pairs(G, _simple_raw_data())
    assert len(violations) == 2
    pair_keys = {(s, t) for s, t, _ in violations}
    assert ("QM_B", "QM_C") in pair_keys
    assert ("QM_C", "QM_A") in pair_keys


def test_format_violation_human_readable():
    s = format_violation("QM_X", "QM_Y", "no_directed_path")
    assert "QM_X" in s
    assert "QM_Y" in s
    assert "no directed channel path" in s


def test_summary_partial_reachability():
    G = _build_simple_graph()
    G.add_edge("QM_A", "QM_B", rel="channel")  # A→B works
    # B→C and C→A required but missing

    summary = reachability_summary(G, _simple_raw_data())
    assert summary["n_required_pairs"] == 3
    assert summary["n_reachable"] == 1
    assert summary["n_unreachable"] == 2
    assert summary["reachability_ratio"] == pytest.approx(1/3, abs=0.01)


def test_empty_graph_handled():
    """No required pairs (no apps) → empty violations, ratio 1.0."""
    G = nx.DiGraph()
    G.add_node("QM_A", type="qm")
    raw = {"applications": [], "queue_managers": [], "queues": [],
           "channels": [], "app_metadata": {}}

    assert find_unreachable_pairs(G, raw) == []
    summary = reachability_summary(G, raw)
    assert summary["n_required_pairs"] == 0
    assert summary["reachability_ratio"] == 1.0


def test_max_report_caps_violation_list():
    """When many pairs are broken, find_unreachable_pairs caps at max_report."""
    G = nx.DiGraph()
    qms = [f"QM_{i}" for i in range(20)]
    apps = [f"APP_{i}" for i in range(20)]
    for q, a in zip(qms, apps):
        G.add_node(q, type="qm")
        G.add_node(a, type="app")
        G.add_edge(a, q, rel="connects_to")
    # No channels at all → every required pair is unreachable

    raw = {
        "applications": [
            {"app_id": apps[i], "qm_id": qms[i],
             "queue_name": f"Q.{i}.{(i+1)%20}", "direction": "PRODUCER"}
            for i in range(20)
        ] + [
            {"app_id": apps[(i+1) % 20], "qm_id": qms[(i+1) % 20],
             "queue_name": f"Q.{i}.{(i+1)%20}", "direction": "CONSUMER"}
            for i in range(20)
        ],
        "queue_managers": [], "queues": [], "channels": [],
        "app_metadata": {},
    }

    violations = find_unreachable_pairs(G, raw, max_report=5)
    assert len(violations) == 5  # capped


# ─────────────────────────────────────────────────────────────────────────────
# The big one: regression that the Steiner solver's output passes the validator
# ─────────────────────────────────────────────────────────────────────────────

def test_steiner_solver_output_passes_validator():
    """The Steiner solver's output topology MUST have all required pairs
    reachable. This is the load-bearing correctness invariant.
    """
    import os
    os.environ["INTELLIAI_USE_SOLVER"] = "1"
    # Import after env set so the hook reads the right flags
    from tests.test_optimizer_hook_steiner import _build_synthetic_state
    from backend.solver.optimizer_hook import run_solver_phase

    G, state = _build_synthetic_state(n_apps=80, n_queues_per_app=3, seed=7)
    state["solver_strategy"] = {"time_budget_s": 30.0}

    result = run_solver_phase(G, state)
    assert result is not None

    optimised = result["graph"]
    violations = find_unreachable_pairs(optimised, state["raw_data"])
    assert violations == [], (
        f"Steiner solver output broke {len(violations)} required pair(s)! "
        f"First: {violations[0] if violations else None}"
    )

    summary = reachability_summary(optimised, state["raw_data"])
    assert summary["reachability_ratio"] == 1.0
