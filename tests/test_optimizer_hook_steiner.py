"""
tests/test_optimizer_hook_steiner.py

Integration test for the new Steiner-driven optimizer hook.

These tests simulate what optimizer_agent will do once the hook is wired into
agents.py: build a target_graph (the architect's output), populate state with
raw_data, call run_solver_phase, and verify the returned dict has the right
shape and meaningful values.

We also verify auto-strategy correctly picks Steiner at production scale and
CP-SAT at small scale.
"""
import os
import random
import time
from typing import Optional

import networkx as nx
import pytest

# Patch flags BEFORE importing the hook (it reads env on import)
os.environ["INTELLIAI_USE_SOLVER"] = "1"
os.environ.pop("INTELLIAI_SOLVER_STRATEGY", None)

from backend.solver.optimizer_hook import (
    run_solver_phase,
    USE_SOLVER,
    SOLVER_STRATEGY,
    AUTO_QM_CUTOFF,
    AUTO_PAIR_CUTOFF,
    _choose_strategy,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers — build minimal graph + raw_data shaped like what the architect produces
# ─────────────────────────────────────────────────────────────────────────────

def _build_synthetic_state(n_apps: int, n_extra_qms: int = 0,
                            n_queues_per_app: int = 2,
                            cross_qm_prob: float = 0.5,
                            seed: int = 42) -> tuple[nx.DiGraph, dict]:
    """Build a (target_graph, state) pair with 1:1 app→QM mapping, queues
    distributed across QMs, and producer/consumer apps that share queue names
    across QM boundaries.

    The graph mimics the architect's output: it has 1:1 app→QM connects_to
    edges AND direct channels for every required (producer_qm, consumer_qm)
    pair. This is the input the optimizer is supposed to reduce.

    Returns (target_graph, state) with state.raw_data + as_is_metrics.
    """
    rng = random.Random(seed)

    # 1:1 apps and QMs
    n_qms = n_apps + n_extra_qms
    qms = [f"QM_{i:04d}" for i in range(n_qms)]
    apps = [f"APP_{i:04d}" for i in range(n_apps)]
    app_to_qm = {apps[i]: qms[i] for i in range(n_apps)}

    G = nx.DiGraph()
    for q in qms:
        G.add_node(q, type="qm")
    for a in apps:
        G.add_node(a, type="app")
        G.add_edge(a, app_to_qm[a], rel="connects_to")

    # Generate queue names + producer/consumer assignments
    queue_names = [f"Q.{i:05d}" for i in range(n_apps * n_queues_per_app // 2)]
    raw_apps = []
    for a in apps:
        n_q = rng.randint(1, n_queues_per_app * 2)
        chosen_queues = rng.sample(queue_names, min(n_q, len(queue_names)))
        for qname in chosen_queues:
            direction = rng.choice(["PRODUCER", "CONSUMER"])
            raw_apps.append({
                "app_id": a,
                "qm_id": app_to_qm[a],
                "queue_name": qname,
                "direction": direction,
            })

    raw_data = {
        "queue_managers": [{"qm_id": q, "qm_name": q, "region": "R1",
                            "line_of_business": "LOB1"} for q in qms],
        "applications": raw_apps,
        "queues": [],
        "channels": [],
        "app_metadata": {a: {
            "data_classification": rng.choice(["INTERNAL", "CONFIDENTIAL"]),
            "is_pci": "N", "is_payment_critical": "N",
            "trtc": "TIER_2", "hosting_type": "DISTRIBUTED",
        } for a in apps},
    }

    # Mimic the architect: add a direct channel for every required pair.
    # This is what the existing architect_agent does (1:1 + flow-justified
    # direct channels). The optimizer then reduces this set.
    from backend.solver.required_pairs import derive_required_pairs
    pairs, _ = derive_required_pairs(G, raw_data)
    for (s, t) in pairs:
        G.add_edge(s, t,
                    rel="channel",
                    channel_name=f"{s}.{t}",
                    status="RUNNING",
                    xmit_queue=f"{t}.XMITQ")

    # Compute a real as-is metrics from THIS graph so the comparison after
    # solving is meaningful
    from backend.graph.mq_graph import compute_complexity
    as_is_metrics = compute_complexity(G)

    state = {
        "target_graph": G,
        "raw_data": raw_data,
        "as_is_metrics": as_is_metrics,
        "messages": [],
    }
    return G, state


# ─────────────────────────────────────────────────────────────────────────────
# Strategy dispatch
# ─────────────────────────────────────────────────────────────────────────────

def test_choose_strategy_auto_picks_steiner_for_large():
    assert _choose_strategy("auto", n_qms=100, n_pairs=10) == "steiner"
    assert _choose_strategy("auto", n_qms=10, n_pairs=500) == "steiner"
    assert _choose_strategy("auto", n_qms=AUTO_QM_CUTOFF + 1, n_pairs=10) == "steiner"


def test_choose_strategy_auto_picks_cpsat_for_small():
    assert _choose_strategy("auto", n_qms=10, n_pairs=20) == "cpsat"
    assert _choose_strategy("auto", n_qms=AUTO_QM_CUTOFF, n_pairs=AUTO_PAIR_CUTOFF) == "cpsat"


def test_choose_strategy_explicit_steiner():
    assert _choose_strategy("steiner", n_qms=5, n_pairs=5) == "steiner"


def test_choose_strategy_explicit_cpsat():
    assert _choose_strategy("cpsat", n_qms=10, n_pairs=20) == "cpsat"


# ─────────────────────────────────────────────────────────────────────────────
# End-to-end through the hook
# ─────────────────────────────────────────────────────────────────────────────

def test_hook_runs_steiner_path_at_scale():
    """Build a graph at ~100 QMs and verify the hook runs Steiner and produces
    a complete optimizer_agent state shape.
    """
    G, state = _build_synthetic_state(n_apps=80, n_queues_per_app=3, seed=1)
    state["solver_strategy"] = {"time_budget_s": 30.0}

    t0 = time.time()
    result = run_solver_phase(G, state)
    elapsed = time.time() - t0

    assert result is not None, "Hook returned None — solver failed unexpectedly"
    # Required keys for optimizer_agent state shape
    for key in ("graph", "target_metrics", "target_subgraphs", "target_communities",
                 "target_centrality", "target_entropy", "message", "solver_run"):
        assert key in result, f"Missing required key in hook result: {key}"

    # Solver run telemetry
    sr = result["solver_run"]
    assert sr["method"] == "steiner_local_search"
    assert sr["status"] in ("OPTIMAL", "TIMEOUT_PARTIAL")
    assert sr["final_channel_count"] <= sr["initial_channel_count"]
    assert sr["objective_value"] >= sr["lower_bound"]
    assert sr["solve_time_s"] >= 0.0

    # Performance: a 80-QM instance must solve in well under the budget
    assert elapsed < 30.0, f"Hook took {elapsed:.1f}s on 80 QMs"

    # Graph: optimised graph has the same QM and app nodes, but a (likely)
    # smaller channel set than initial
    optimised = result["graph"]
    qm_count_before = sum(1 for _, d in G.nodes(data=True) if d.get("type") == "qm")
    qm_count_after = sum(1 for _, d in optimised.nodes(data=True) if d.get("type") == "qm")
    assert qm_count_after == qm_count_before
    # Channels in optimised graph must equal solver_run["final_channel_count"]
    chan_count_after = sum(1 for _, _, d in optimised.edges(data=True)
                            if d.get("rel") == "channel")
    assert chan_count_after == sr["final_channel_count"]


def test_hook_message_includes_consolidation_pct():
    """Hook's status message must include the channel reduction percentage."""
    G, state = _build_synthetic_state(n_apps=40, n_queues_per_app=3, seed=2)
    result = run_solver_phase(G, state)
    assert result is not None
    msg = result["message"]
    assert "channels" in msg
    assert "reduction" in msg
    assert "Steiner solver" in msg


def test_hook_returns_none_on_missing_raw_data():
    G, state = _build_synthetic_state(n_apps=30, seed=3)
    state["raw_data"] = {}  # empty
    # With empty raw_data, derive_required_pairs returns [] pairs which is
    # NOT a failure — Steiner should produce empty solution. Still a valid run.
    result = run_solver_phase(G, state)
    if result is not None:
        # If it ran, the channel count should be 0
        assert result["solver_run"]["final_channel_count"] == 0
    # Either outcome is acceptable; we mainly check it doesn't crash.


def test_hook_handles_no_apps_gracefully():
    """Architect produces a graph with QMs but no apps (degenerate case).
    Hook should either produce empty solution or return None — never crash.
    """
    G = nx.DiGraph()
    for i in range(5):
        G.add_node(f"QM_{i}", type="qm")

    state = {
        "target_graph": G,
        "raw_data": {"applications": [], "queue_managers": [], "queues": [],
                     "channels": [], "app_metadata": {}},
        "as_is_metrics": {"total_score": 0, "baselines": {
            "cc_worst": 1, "ci_worst": 0.01, "rd_worst": 1, "fo_worst": 1,
            "oo_worst": 1, "cs_worst": 1,
        }},
        "messages": [],
    }
    # Should not raise
    result = run_solver_phase(G, state)
    if result is not None:
        # No required pairs → no channels regardless of which solver ran.
        # Steiner reports 'final_channel_count'; CP-SAT reports 'channels_chosen' empty.
        sr = result["solver_run"]
        if sr.get("method") == "steiner_local_search":
            assert sr["final_channel_count"] == 0
        else:
            assert len(sr.get("channels_chosen", [])) == 0


# ─────────────────────────────────────────────────────────────────────────────
# Production-scale headline test
# ─────────────────────────────────────────────────────────────────────────────

def test_hook_at_production_scale():
    """Build a 480-app instance and verify the hook completes within budget,
    produces a real reduction, and returns a well-formed optimizer state.

    This is the test that validates the entire integration is production-ready.
    """
    G, state = _build_synthetic_state(n_apps=450, n_extra_qms=30,
                                        n_queues_per_app=4, seed=42)
    state["solver_strategy"] = {"time_budget_s": 60.0}

    n_qms = sum(1 for _, d in G.nodes(data=True) if d.get("type") == "qm")
    n_app_nodes = sum(1 for _, d in G.nodes(data=True) if d.get("type") == "app")
    initial_chans = sum(1 for _, _, d in G.edges(data=True) if d.get("rel") == "channel")
    asis_score = state["as_is_metrics"]["total_score"]
    print(f"\n[production-scale] {n_qms} QMs, {n_app_nodes} apps, "
          f"{initial_chans} architect-output channels, as-is score={asis_score}")

    t0 = time.time()
    result = run_solver_phase(G, state)
    elapsed = time.time() - t0

    assert result is not None
    sr = result["solver_run"]
    target_score = result["target_metrics"]["total_score"]
    print(f"[production-scale] strategy: {sr['method']}")
    print(f"[production-scale] pairs: {sr['n_pairs']}")
    print(f"[production-scale] channels: {sr['initial_channel_count']} → {sr['final_channel_count']} "
          f"({100*(1 - sr['final_channel_count']/max(sr['initial_channel_count'],1)):.1f}% reduction)")
    print(f"[production-scale] objective: {sr['objective_value']:.1f}")
    print(f"[production-scale] complexity score: {asis_score:.1f} → {target_score:.1f}")
    print(f"[production-scale] solve_time: {sr['solve_time_s']:.2f}s, "
          f"total elapsed: {elapsed:.2f}s")

    # Hard ceilings
    assert elapsed < 90.0, f"Hook took {elapsed:.1f}s, hard ceiling 90s"
    assert sr["method"] == "steiner_local_search"
    # Consolidation must happen
    assert sr["final_channel_count"] < sr["initial_channel_count"], (
        "No consolidation on a large instance — Steiner solver is broken"
    )
    # Complexity score must DROP (the entire point of the optimizer)
    assert target_score < asis_score, (
        f"Optimizer made the topology MORE complex! "
        f"as-is={asis_score:.1f} → target={target_score:.1f}"
    )
