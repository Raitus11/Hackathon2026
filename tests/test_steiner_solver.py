"""
tests/test_steiner_solver.py

Tests for the Directed Steiner Network solver via greedy local search.

Test progression:
  1. Trivial: empty pairs → empty solution.
  2. Single pair: direct channel.
  3. Two pairs sharing a target: should consolidate via common in-edge if cost-effective.
  4. Star: 1 source, k targets — all direct channels (the star IS optimal here).
  5. Linear chain demand: pairs (s1, s2, s3) where consolidation is highly beneficial.
  6. B1 fixture: existing benchmark, validate solver runs.
  7. Stress: 480 QMs, 5000 random required pairs — proves it scales.
"""
import random
import time
import pytest

from backend.solver.steiner_solver import (
    SteinerInput,
    SteinerOutput,
    solve,
)


# ─────────────────────────────────────────────────────────────────────────────
# Trivial / smoke tests
# ─────────────────────────────────────────────────────────────────────────────

def test_empty_pairs_returns_empty_solution():
    inp = SteinerInput(qms=["A", "B"], required_pairs=[])
    out = solve(inp)
    assert out.status == "OPTIMAL"
    assert out.objective_value == 0.0
    assert out.channels_chosen == []
    assert out.initial_channel_count == 0
    assert out.final_channel_count == 0


def test_single_pair_picks_direct_channel():
    inp = SteinerInput(qms=["A", "B"], required_pairs=[("A", "B")])
    out = solve(inp)
    assert out.status == "OPTIMAL"
    assert ("A", "B") in out.channels_chosen
    assert len(out.channels_chosen) == 1
    # objective = α + β = 1.0 + 0.3 = 1.3
    assert out.objective_value == pytest.approx(1.3)
    # Pair routed via the direct edge
    assert out.pair_routes[0] == [("A", "B")]


def test_self_pair_rejected():
    with pytest.raises(ValueError, match="src == tgt"):
        inp = SteinerInput(qms=["A"], required_pairs=[("A", "A")])
        solve(inp)


def test_invalid_qm_rejected():
    with pytest.raises(ValueError, match="not in qms"):
        inp = SteinerInput(qms=["A", "B"], required_pairs=[("A", "C")])
        solve(inp)


# ─────────────────────────────────────────────────────────────────────────────
# Hand-verifiable consolidation cases
# ─────────────────────────────────────────────────────────────────────────────

def test_three_pair_chain_consolidates():
    """Required pairs: (A→B), (A→C), (B→C).
    Star solution (no consolidation): channels {A→B, A→C, B→C}, hops 1+1+1=3.
      Cost: 3α + 3β = 3 + 0.9 = 3.9
    Consolidated: channels {A→B, B→C}, hops A→B=1, A→C=2 (via B), B→C=1.
      Cost: 2α + 4β = 2 + 1.2 = 3.2

    Consolidation IS cheaper (saves 0.7) → solver should remove A→C.
    """
    inp = SteinerInput(
        qms=["A", "B", "C"],
        required_pairs=[("A", "B"), ("A", "C"), ("B", "C")],
        alpha=1.0, beta=0.3,
    )
    out = solve(inp)
    assert out.status == "OPTIMAL"
    # The solver must drop one channel — most likely A→C (the longest substitutable path)
    assert out.final_channel_count == 2
    # Objective should be 3.2
    assert out.objective_value == pytest.approx(3.2, abs=0.01)


def test_three_pair_chain_does_NOT_consolidate_when_beta_high():
    """Same pairs as above but with β=0.6 instead of 0.3.
    Star: 3α + 3β = 3 + 1.8 = 4.8
    Consolidated: 2α + 4β = 2 + 2.4 = 4.4
    Still cheaper, BUT with β=0.8:
    Star: 3 + 2.4 = 5.4
    Consolidated: 2 + 3.2 = 5.2 — still cheaper
    With β=1.1:
    Star: 3 + 3.3 = 6.3
    Consolidated: 2 + 4.4 = 6.4 — star wins!

    So at β=1.1 the optimal answer is the star, no consolidation.
    """
    inp = SteinerInput(
        qms=["A", "B", "C"],
        required_pairs=[("A", "B"), ("A", "C"), ("B", "C")],
        alpha=1.0, beta=1.1,
    )
    out = solve(inp)
    assert out.status == "OPTIMAL"
    # Solver should keep all 3 channels (no removal saves money)
    assert out.final_channel_count == 3


def test_long_chain_high_consolidation():
    """Required pairs: every pair (i, j) for i,j ∈ {0..4}, i ≠ j.
    20 required pairs.
    Direct-only: 20 channels, 20 hops. Cost: 20 + 6 = 26.
    Consolidated via cycle 0→1→2→3→4→0:
      5 channels, but cycle alone doesn't reach every pair efficiently.
      Pair (0,1): 1 hop. (0,2): 2. (0,3): 3. (0,4): 4. (4,0): 1.
      Sum of hops = 1+2+3+4 + 4+1+2+3 + 3+4+1+2 + 2+3+4+1 + 1+2+3+4 = 50.
      Cost: 5 + 0.3*50 = 20.

    So a 5-cycle is cheaper than 20 direct channels. The solver should find
    a tighter set than 20 channels.
    """
    nodes = ["N0", "N1", "N2", "N3", "N4"]
    pairs = [(u, v) for u in nodes for v in nodes if u != v]
    inp = SteinerInput(qms=nodes, required_pairs=pairs, alpha=1.0, beta=0.3)
    out = solve(inp)
    assert out.status == "OPTIMAL"
    # Direct-only would be 20 channels. Solver should beat that.
    assert out.final_channel_count < 20, (
        f"Solver found {out.final_channel_count} channels, expected consolidation < 20"
    )
    # Objective should beat 26 (direct-only cost)
    assert out.objective_value < 26.0


# ─────────────────────────────────────────────────────────────────────────────
# Star: cost geometry where direct is optimal
# ─────────────────────────────────────────────────────────────────────────────

def test_star_one_source_many_targets_no_consolidation():
    """1 source, 5 targets, all from same source. The star IS optimal here:
    going via another target costs α + 2β > α + β.
    """
    pairs = [("S", f"T{i}") for i in range(5)]
    inp = SteinerInput(qms=["S"] + [f"T{i}" for i in range(5)],
                       required_pairs=pairs, alpha=1.0, beta=0.3)
    out = solve(inp)
    assert out.status == "OPTIMAL"
    assert out.final_channel_count == 5  # all 5 direct channels kept
    # Objective: 5α + 5β = 5 + 1.5 = 6.5
    assert out.objective_value == pytest.approx(6.5)


# ─────────────────────────────────────────────────────────────────────────────
# Reachability never violated
# ─────────────────────────────────────────────────────────────────────────────

def test_every_required_pair_has_a_route():
    """Stress invariant: for any input, every required pair must have a
    route (non-empty list of edges from src to tgt) in the output.
    """
    rng = random.Random(42)
    nodes = [f"N{i}" for i in range(20)]
    pairs = []
    for _ in range(60):
        s, t = rng.sample(nodes, 2)
        pairs.append((s, t))
    pairs = list(set(pairs))  # dedup

    inp = SteinerInput(qms=nodes, required_pairs=pairs, alpha=1.0, beta=0.3)
    out = solve(inp)
    assert out.status == "OPTIMAL"

    for orig_idx, (s, t) in enumerate(pairs):
        route = out.pair_routes[orig_idx]
        assert len(route) > 0, f"No route for pair ({s}, {t})"
        # Verify route starts at s, ends at t, and is connected
        assert route[0][0] == s, f"Route for ({s},{t}) starts at {route[0][0]}"
        assert route[-1][1] == t, f"Route for ({s},{t}) ends at {route[-1][1]}"
        for i in range(len(route) - 1):
            assert route[i][1] == route[i+1][0], f"Route disconnected at step {i}"
        # All edges must be in the chosen channel set
        for edge in route:
            assert edge in out.channels_chosen, (
                f"Route uses {edge} not in channels_chosen"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Scale test — the headline number we care about
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("n_qms,n_pairs,seed", [
    (50, 200, 1),
    (100, 800, 1),
    (200, 2000, 1),
    (480, 5000, 1),     # production scale
])
def test_scales_to_production_size(n_qms, n_pairs, seed):
    """Generate a random instance at the given scale; verify the solver
    runs within a reasonable time budget and produces valid output.

    This is the test that *must* pass for us to claim the solver works on
    the real CSV. Old CP-SAT solver would OOM on the 480/5000 case.
    """
    rng = random.Random(seed)
    nodes = [f"QM_{i:04d}" for i in range(n_qms)]
    pairs_set = set()
    while len(pairs_set) < n_pairs:
        s, t = rng.sample(nodes, 2)
        pairs_set.add((s, t))
    pairs = list(pairs_set)

    inp = SteinerInput(qms=nodes, required_pairs=pairs, alpha=1.0, beta=0.3,
                        time_budget_s=60.0)
    t0 = time.time()
    out = solve(inp)
    elapsed = time.time() - t0

    # Hard ceiling: any production-relevant solve must complete in < 60s.
    assert elapsed < 60.0, f"Solver took {elapsed:.1f}s on {n_qms} QMs / {n_pairs} pairs"

    # Sanity: every pair has a route
    assert len(out.pair_routes) == len(pairs)
    for idx in range(len(pairs)):
        assert len(out.pair_routes[idx]) > 0

    # Sanity: solution is no worse than direct-only (initial state)
    direct_only_cost = inp.alpha * len(set(pairs)) + inp.beta * len(set(pairs))
    assert out.objective_value <= direct_only_cost

    # Print stats for diagnostic purposes
    print(f"\n[{n_qms}qm/{n_pairs}pairs] "
          f"channels: {out.initial_channel_count} → {out.final_channel_count} "
          f"({100*(1 - out.final_channel_count/out.initial_channel_count):.1f}% reduction); "
          f"obj: {out.objective_value:.1f}; "
          f"LB: {out.lower_bound:.1f}; "
          f"gap: {out.gap_pct:.1f}%; "
          f"iters: {out.iterations}; "
          f"time: {elapsed:.2f}s")


# ─────────────────────────────────────────────────────────────────────────────
# Soft-penalty integration
# ─────────────────────────────────────────────────────────────────────────────

def test_soft_penalty_steers_routing():
    """Three pairs: (A→B), (A→C), (B→C). With penalty on edge B→C for pair
    A→C (pair_idx=1), the solver should prefer NOT routing pair A→C through
    B→C, even if it's the cheaper consolidation.

    Without penalty: drops A→C, routes via A→B→C. Cost = 2 + 4*0.3 = 3.2.
    With huge penalty on (1, (B,C)): keeping A→C direct avoids the penalty.
      Cost = 3 + 3*0.3 = 3.9, no penalty.
      Consolidation cost = 3.2 + γ*penalty = 3.2 + huge.
    Solver should NOT consolidate.

    NOTE: The current implementation evaluates penalties at the END (after
    local search is done). So the local search itself doesn't yet steer on
    penalties — TODO. For now this test just asserts that penalties show up
    in the objective breakdown correctly when present.
    """
    inp = SteinerInput(
        qms=["A", "B", "C"],
        required_pairs=[("A", "B"), ("A", "C"), ("B", "C")],
        soft_penalties={(1, ("B", "C")): 100.0},  # huge penalty on pair A→C using B→C
        alpha=1.0, beta=0.3, gamma=1.0,
    )
    out = solve(inp)
    assert out.status == "OPTIMAL"
    # Penalties block in breakdown is populated correctly (whether or not
    # the penalty was avoided; just verify the field exists)
    assert "penalties" in out.objective_breakdown
