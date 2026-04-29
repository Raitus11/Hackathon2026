"""
tests/test_solver_b1.py

Runs the CP-SAT solver on benchmark B1 and verifies expected properties.

This is the Day 1 gate test: if this passes today, the Day 3 hard gate
(solver passes B1-B5) is on track.

Run with:
    cd <repo_root>
    PYTHONPATH=. python tests/test_solver_b1.py

Or via pytest:
    pytest tests/test_solver_b1.py -v
"""
import sys
import os

# Allow running standalone without pytest installed
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from backend.solver.cpsat_solver import solve
from benchmarks.B1.fixture import get_input, EXPECTED, QMS, FLOWS


def test_b1_solves():
    """B1 must solve to OPTIMAL within budget, with sane channel/hop counts."""
    inp = get_input(time_budget_s=10.0)
    out = solve(inp)

    # Print run summary first — useful for any failure
    print(f"\n{'='*60}")
    print(f"B1 Solver Run")
    print(f"{'='*60}")
    print(f"Status:           {out.status}")
    print(f"Integer optimum:  {out.integer_optimum:.2f}")
    print(f"LP bound:         {out.lp_bound:.2f}")
    print(f"Gap:              {out.gap_pct:.1f}%")
    print(f"Channels chosen:  {len(out.channels_chosen)}")
    print(f"Solve time:       {out.solve_time_s:.2f}s (CP-SAT) + {out.lp_time_s:.2f}s (LP)")
    print(f"Branches:         {out.branches}")
    print(f"Breakdown:        channels={out.objective_breakdown['channels']:.2f}, "
          f"hops={out.objective_breakdown['hops']:.2f}, "
          f"penalties={out.objective_breakdown['penalties']:.2f}")
    print(f"\nChannels in solution:")
    for (i, j) in sorted(out.channels_chosen):
        print(f"  {i} -> {j}")
    print(f"\nFlow routes:")
    for f_idx, route in sorted(out.flow_routes.items()):
        src, tgt = FLOWS[f_idx]
        path_str = " -> ".join([src] + [edge[1] for edge in route])
        print(f"  Flow {f_idx} ({src} -> {tgt}): {path_str}  ({len(route)} hops)")
    print(f"{'='*60}\n")

    # ── Assertions ────────────────────────────────────────────────────
    assert out.status == EXPECTED["status"], (
        f"Expected status={EXPECTED['status']}, got {out.status}"
    )
    assert out.integer_optimum <= EXPECTED["max_objective"], (
        f"Integer optimum {out.integer_optimum} exceeds upper bound "
        f"{EXPECTED['max_objective']} — formulation likely wrong"
    )
    assert out.lp_bound > 0, "LP bound is 0 — relaxation failed"
    assert out.lp_bound <= out.integer_optimum + 1e-6, (
        f"LP bound {out.lp_bound} > integer optimum {out.integer_optimum} — "
        f"impossible; LP must be a lower bound"
    )
    assert EXPECTED["min_channels"] <= len(out.channels_chosen) <= EXPECTED["max_channels"], (
        f"Channel count {len(out.channels_chosen)} outside [{EXPECTED['min_channels']}, "
        f"{EXPECTED['max_channels']}]"
    )
    assert out.gap_pct <= EXPECTED["max_gap_pct"], (
        f"Gap {out.gap_pct}% > {EXPECTED['max_gap_pct']}% — solver did not "
        f"converge close enough"
    )

    # Every flow must be routed
    assert len(out.flow_routes) == len(FLOWS), (
        f"Expected {len(FLOWS)} flow routes, got {len(out.flow_routes)}"
    )
    for f_idx, route in out.flow_routes.items():
        assert len(route) >= 1, f"Flow {f_idx} has empty route"
        # First edge starts at src, last edge ends at tgt
        src, tgt = FLOWS[f_idx]
        assert route[0][0] == src, f"Flow {f_idx} route doesn't start at {src}: {route}"
        assert route[-1][1] == tgt, f"Flow {f_idx} route doesn't end at {tgt}: {route}"
        # Every edge in route must be in chosen channels
        for edge in route:
            assert edge in out.channels_chosen, (
                f"Flow {f_idx} uses edge {edge} not in channels_chosen"
            )

    # Solve time must be reasonable on a 5-QM problem
    assert out.solve_time_s < inp.time_budget_s, (
        f"Solver hit time budget — formulation is too slow even on B1"
    )

    print("✓ All B1 assertions passed.")


def test_b1_lp_bound_tightness():
    """Optional: report how tight the LP bound is on B1.

    Not a hard assertion — just informational. On a small clean problem
    we expect the LP gap to be modest (< 30%). If it's huge, our formulation
    has weak relaxation properties and we should think about cutting planes.
    """
    inp = get_input(time_budget_s=10.0)
    out = solve(inp)
    print(f"\nLP bound tightness on B1: {out.gap_pct:.1f}% gap")
    print(f"  Integer: {out.integer_optimum:.2f}")
    print(f"  LP:      {out.lp_bound:.2f}")
    if out.gap_pct > 30:
        print(f"  ⚠  LP bound is loose. Consider strengthening the formulation.")
    else:
        print(f"  ✓  LP bound is tight enough for credible optimality claims.")


def test_b1_no_flows_edge_case():
    """Edge case: solver with empty flows should return empty solution."""
    from backend.solver.cpsat_solver import SolverInput
    inp = SolverInput(qms=["QM_A", "QM_B"], flows=[])
    out = solve(inp)
    assert out.status == "OPTIMAL"
    assert out.integer_optimum == 0.0
    assert out.lp_bound == 0.0
    assert len(out.channels_chosen) == 0
    assert len(out.flow_routes) == 0
    print("✓ Empty-flows edge case handled.")


def test_b1_validation_errors():
    """Solver must reject malformed inputs."""
    from backend.solver.cpsat_solver import SolverInput
    # Flow references unknown QM
    try:
        solve(SolverInput(qms=["QM_A", "QM_B"], flows=[("QM_A", "QM_C")]))
        assert False, "Expected ValueError for unknown QM in flow"
    except ValueError as e:
        assert "QM_C" in str(e)
    # Self-flow
    try:
        solve(SolverInput(qms=["QM_A"], flows=[("QM_A", "QM_A")]))
        assert False, "Expected ValueError for self-flow"
    except ValueError as e:
        assert "src == tgt" in str(e)
    # Empty QMs
    try:
        solve(SolverInput(qms=[], flows=[("QM_A", "QM_B")]))
        assert False, "Expected ValueError for empty qms"
    except ValueError as e:
        assert "empty" in str(e).lower()
    print("✓ Input validation works.")


if __name__ == "__main__":
    test_b1_validation_errors()
    test_b1_no_flows_edge_case()
    test_b1_solves()
    test_b1_lp_bound_tightness()
    print("\n✓✓✓ All tests passed. Solver works on B1. Day 1 gate cleared.")
