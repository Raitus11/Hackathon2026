"""
backend/solver/cpsat_solver.py

CP-SAT solver for multi-commodity flow on the MQ channel graph.

Problem (per IntelliAI Phase 1 Plan v3, §I.2.1):

  Variables:
    x_{ij} ∈ {0, 1}  for each ordered QM pair (i, j), i ≠ j
                     "is there a sender channel from QM i to QM j?"
    r^f_{ij} ∈ {0, 1}  for each flow f and edge (i, j)
                       "does flow f traverse this edge?"

  Constraints:
    Flow conservation:  for each flow f and QM k:
        Σ_j r^f_{kj} - Σ_i r^f_{ik} = 1[k = src(f)] - 1[k = tgt(f)]
    Capacity:  r^f_{ij} ≤ x_{ij}
    Soft compliance penalties (γ-weighted)

  Objective:
    min  α Σ x_{ij} + β Σ_f Σ_{ij} r^f_{ij} + γ Σ_{(f,e) ∈ P} r^f_e

Citation: Magnanti & Wong 1984, "Network Design and Transportation Planning",
          Operations Research 32(1):1-69. Multi-commodity flow with fixed charges.

Also computes LP relaxation bound (cite Wolsey 1998, "Integer Programming") so
the system can claim "provably within X% of global optimum" rather than
"we found a solution."

USAGE:
    from backend.solver.cpsat_solver import solve, SolverInput

    inp = SolverInput(
        qms=["QM_A", "QM_B", "QM_C"],
        flows=[("QM_A", "QM_B"), ("QM_A", "QM_C")],
        soft_penalties={},
        alpha=1.0, beta=0.3, gamma=1.0,
        time_budget_s=10.0,
    )
    out = solve(inp)
    # out.integer_optimum, out.lp_bound, out.gap_pct, out.channels_chosen, out.flow_routes
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from ortools.sat.python import cp_model
from ortools.linear_solver import pywraplp

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────────────────────────────────────────
# NOTE: Using @dataclass instead of pydantic to keep this file dependency-free.
# Convert to pydantic.BaseModel when integrating into main.py if the rest of
# the codebase uses pydantic for API schemas.

@dataclass
class SolverInput:
    """Input to the CP-SAT solver.

    qms: list of QM identifiers (strings).
    flows: list of (source_qm, target_qm) tuples representing producer→consumer pairs
           that need a routable path in the target topology.
    soft_penalties: dict mapping (flow_idx, edge_tuple) → penalty_weight.
                    Comes from the Architect's Business Context Translator LLM.
                    Empty dict = no soft constraints (pure objective).
    alpha: weight on channel count term.
    beta:  weight on routing-hops term.
    gamma: weight on soft-compliance penalty term.
    time_budget_s: max wall-clock seconds for CP-SAT to spend.
    """
    qms: list[str]
    flows: list[tuple[str, str]]
    soft_penalties: dict[tuple[int, tuple[str, str]], float] = field(default_factory=dict)
    alpha: float = 1.0
    beta: float = 0.3
    gamma: float = 1.0
    time_budget_s: float = 10.0


@dataclass
class SolverOutput:
    """Output from the CP-SAT solver.

    status: "OPTIMAL" | "FEASIBLE" | "INFEASIBLE" | "UNKNOWN"
    integer_optimum: objective value of the integer solution found
                     (or +inf if no feasible solution).
    lp_bound: lower bound from LP relaxation. Together with integer_optimum
              this gives provable optimality gap.
    gap_pct: (integer_optimum - lp_bound) / lp_bound * 100, capped at 100.
    channels_chosen: list of (from_qm, to_qm) tuples where x_{ij} = 1.
    flow_routes: dict mapping flow_idx → list of (from_qm, to_qm) edges traversed,
                 in routing order from src to tgt.
    solve_time_s: wall-clock seconds spent in CP-SAT (excluding LP).
    lp_time_s:    wall-clock seconds spent in LP solve.
    branches:     number of search branches CP-SAT explored.
    objective_breakdown: dict with 'channels', 'hops', 'penalties' contributions
                        to the integer optimum (for UI display & debugging).
    """
    status: str
    integer_optimum: float
    lp_bound: float
    gap_pct: float
    channels_chosen: list[tuple[str, str]]
    flow_routes: dict[int, list[tuple[str, str]]]
    solve_time_s: float
    lp_time_s: float
    branches: int
    objective_breakdown: dict[str, float]


# ─────────────────────────────────────────────────────────────────────────────
# Core solver
# ─────────────────────────────────────────────────────────────────────────────

def solve(inp: SolverInput) -> SolverOutput:
    """Solve the multi-commodity flow problem and compute the LP bound.

    Returns SolverOutput with both the integer optimum and LP lower bound.
    The integrality gap (integer_optimum - lp_bound) / lp_bound is the
    formal optimality guarantee: the integer optimum is provably within
    that fraction of the true global optimum.
    """
    if not inp.qms:
        raise ValueError("solver: empty qms list")
    if not inp.flows:
        # No flows = no work; return trivial empty-channel solution.
        return SolverOutput(
            status="OPTIMAL",
            integer_optimum=0.0, lp_bound=0.0, gap_pct=0.0,
            channels_chosen=[], flow_routes={},
            solve_time_s=0.0, lp_time_s=0.0, branches=0,
            objective_breakdown={"channels": 0.0, "hops": 0.0, "penalties": 0.0},
        )

    # Validate flows reference real QMs
    qm_set = set(inp.qms)
    for f_idx, (src, tgt) in enumerate(inp.flows):
        if src not in qm_set:
            raise ValueError(f"solver: flow {f_idx} src {src!r} not in qms")
        if tgt not in qm_set:
            raise ValueError(f"solver: flow {f_idx} tgt {tgt!r} not in qms")
        if src == tgt:
            raise ValueError(f"solver: flow {f_idx} has src == tgt = {src!r}")

    # ── Run integer solve (CP-SAT) ────────────────────────────────────────
    int_result = _solve_integer(inp)

    # ── Run LP relaxation for bound ───────────────────────────────────────
    lp_result = _solve_lp_relaxation(inp)

    # ── Compute gap ──────────────────────────────────────────────────────
    if lp_result["bound"] <= 0:
        # LP lower bound is 0 (degenerate or trivially satisfiable);
        # gap is undefined / vacuously 100%.
        gap_pct = 0.0 if int_result["objective"] <= 0 else 100.0
    else:
        raw_gap = (int_result["objective"] - lp_result["bound"]) / lp_result["bound"]
        gap_pct = max(0.0, min(100.0, raw_gap * 100))

    return SolverOutput(
        status=int_result["status"],
        integer_optimum=int_result["objective"],
        lp_bound=lp_result["bound"],
        gap_pct=gap_pct,
        channels_chosen=int_result["channels"],
        flow_routes=int_result["routes"],
        solve_time_s=int_result["solve_time_s"],
        lp_time_s=lp_result["lp_time_s"],
        branches=int_result["branches"],
        objective_breakdown=int_result["breakdown"],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Integer solve via CP-SAT
# ─────────────────────────────────────────────────────────────────────────────

def _solve_integer(inp: SolverInput) -> dict:
    """CP-SAT integer solve. Returns dict (not SolverOutput — composed by solve())."""
    t0 = time.time()
    model = cp_model.CpModel()

    qms = inp.qms
    n_qm = len(qms)
    flows = inp.flows
    n_flow = len(flows)

    # Edge set: all ordered pairs (i, j) with i ≠ j.
    # For sparse problems we could prune to "edges that could plausibly carry a flow"
    # but at our scale (≤ 60 QMs in B6) full bipartite is fine.
    edges = [(i, j) for i in qms for j in qms if i != j]

    # ── Variables ────────────────────────────────────────────────────────
    # x[(i, j)] = 1  iff sender channel i → j is created
    x = {(i, j): model.NewBoolVar(f"x_{i}_{j}") for (i, j) in edges}

    # r[f][(i, j)] = 1  iff flow f traverses edge (i, j)
    r: dict[int, dict[tuple[str, str], cp_model.IntVar]] = {}
    for f_idx in range(n_flow):
        r[f_idx] = {(i, j): model.NewBoolVar(f"r_{f_idx}_{i}_{j}") for (i, j) in edges}

    # ── Constraints ──────────────────────────────────────────────────────
    # 1. Flow conservation (per flow, per QM)
    #    Σ_j r[f][k,j] - Σ_i r[f][i,k] = 1{k=src(f)} - 1{k=tgt(f)}
    for f_idx, (src, tgt) in enumerate(flows):
        for k in qms:
            outflow = sum(r[f_idx][(k, j)] for j in qms if j != k)
            inflow  = sum(r[f_idx][(i, k)] for i in qms if i != k)
            net = outflow - inflow
            if k == src:
                model.Add(net == 1)
            elif k == tgt:
                model.Add(net == -1)
            else:
                model.Add(net == 0)

    # 2. Capacity: r^f_{ij} ≤ x_{ij}
    for f_idx in range(n_flow):
        for (i, j) in edges:
            model.Add(r[f_idx][(i, j)] <= x[(i, j)])

    # ── Objective ────────────────────────────────────────────────────────
    # Scale floats to ints — CP-SAT works in integer domain.
    # Multiply all coefficients by 1000 to preserve 3 decimal places of precision
    # in the alpha/beta/gamma weights.
    SCALE = 1000
    a_int = int(round(inp.alpha * SCALE))
    b_int = int(round(inp.beta * SCALE))
    g_int = int(round(inp.gamma * SCALE))

    channel_term = sum(a_int * x[(i, j)] for (i, j) in edges)
    hops_term    = sum(b_int * r[f_idx][(i, j)] for f_idx in range(n_flow) for (i, j) in edges)
    penalty_term = sum(
        int(round(weight * g_int)) * r[f_idx][edge]
        for (f_idx, edge), weight in inp.soft_penalties.items()
        if f_idx < n_flow and edge in r[f_idx]
    )

    model.Minimize(channel_term + hops_term + penalty_term)

    # ── Solve ────────────────────────────────────────────────────────────
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = inp.time_budget_s
    solver.parameters.num_search_workers = 4  # parallel; safe on most laptops

    status = solver.Solve(model)
    solve_time_s = time.time() - t0

    status_name_map = {
        cp_model.OPTIMAL:    "OPTIMAL",
        cp_model.FEASIBLE:   "FEASIBLE",
        cp_model.INFEASIBLE: "INFEASIBLE",
        cp_model.UNKNOWN:    "UNKNOWN",
        cp_model.MODEL_INVALID: "MODEL_INVALID",
    }
    status_name = status_name_map.get(status, "UNKNOWN")

    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        logger.warning(f"CP-SAT did not find a feasible solution: {status_name}")
        return {
            "status": status_name,
            "objective": float("inf"),
            "channels": [],
            "routes": {},
            "solve_time_s": solve_time_s,
            "branches": int(solver.NumBranches()),
            "breakdown": {"channels": 0.0, "hops": 0.0, "penalties": 0.0},
        }

    # Extract solution
    channels_chosen = [
        (i, j) for (i, j) in edges
        if solver.Value(x[(i, j)]) == 1
    ]

    flow_routes: dict[int, list[tuple[str, str]]] = {}
    for f_idx in range(n_flow):
        used_edges = [(i, j) for (i, j) in edges if solver.Value(r[f_idx][(i, j)]) == 1]
        # Order them src→tgt by walking the path
        flow_routes[f_idx] = _order_path(used_edges, flows[f_idx][0], flows[f_idx][1])

    # Compute objective breakdown (in *original* float units, not scaled)
    n_channels = len(channels_chosen)
    n_hops = sum(len(route) for route in flow_routes.values())
    n_penalty_units = sum(
        weight
        for (f_idx, edge), weight in inp.soft_penalties.items()
        if f_idx < n_flow and edge in r[f_idx] and solver.Value(r[f_idx][edge]) == 1
    )
    breakdown = {
        "channels": inp.alpha * n_channels,
        "hops":     inp.beta  * n_hops,
        "penalties": inp.gamma * n_penalty_units,
    }
    objective_orig_units = breakdown["channels"] + breakdown["hops"] + breakdown["penalties"]

    return {
        "status": status_name,
        "objective": objective_orig_units,
        "channels": channels_chosen,
        "routes": flow_routes,
        "solve_time_s": solve_time_s,
        "branches": int(solver.NumBranches()),
        "breakdown": breakdown,
    }


def _order_path(edges: list[tuple[str, str]], src: str, tgt: str) -> list[tuple[str, str]]:
    """Order a set of edges into a path from src to tgt.

    Assumes the edges form a simple path (which they will for any
    flow-conserving solution to a single-commodity problem). If the
    solver picked a degenerate solution with cycles, returns whatever
    we can walk; rest are appended unordered.
    """
    if not edges:
        return []

    by_src = {e[0]: e for e in edges}
    ordered = []
    cur = src
    seen = set()
    while cur in by_src and cur not in seen:
        seen.add(cur)
        e = by_src[cur]
        ordered.append(e)
        cur = e[1]
        if cur == tgt:
            break

    # Append any leftover edges (shouldn't happen on clean solutions)
    leftover = [e for e in edges if e not in ordered]
    return ordered + leftover


# ─────────────────────────────────────────────────────────────────────────────
# LP relaxation
# ─────────────────────────────────────────────────────────────────────────────

def _solve_lp_relaxation(inp: SolverInput) -> dict:
    """Solve the LP relaxation of the same problem to get a lower bound.

    Cite: Wolsey 1998, "Integer Programming", §1.3 — LP relaxation provides
    a valid lower bound for any minimization integer program; the integrality
    gap measures how good the IP solution is relative to the global optimum.

    Uses OR-Tools' GLOP (Google's LP solver) via the linear_solver wrapper.
    """
    t0 = time.time()
    lp = pywraplp.Solver.CreateSolver("GLOP")
    if lp is None:
        logger.error("GLOP solver unavailable; LP bound will be 0")
        return {"bound": 0.0, "lp_time_s": 0.0}

    qms = inp.qms
    flows = inp.flows
    n_flow = len(flows)
    edges = [(i, j) for i in qms for j in qms if i != j]

    # Continuous variables in [0, 1] — the LP relaxation
    x = {(i, j): lp.NumVar(0, 1, f"x_{i}_{j}") for (i, j) in edges}
    r: dict[int, dict[tuple[str, str], pywraplp.Variable]] = {}
    for f_idx in range(n_flow):
        r[f_idx] = {(i, j): lp.NumVar(0, 1, f"r_{f_idx}_{i}_{j}") for (i, j) in edges}

    # Flow conservation (same as integer problem)
    for f_idx, (src, tgt) in enumerate(flows):
        for k in qms:
            outflow = lp.Sum([r[f_idx][(k, j)] for j in qms if j != k])
            inflow  = lp.Sum([r[f_idx][(i, k)] for i in qms if i != k])
            if k == src:
                lp.Add(outflow - inflow == 1)
            elif k == tgt:
                lp.Add(outflow - inflow == -1)
            else:
                lp.Add(outflow - inflow == 0)

    # Capacity
    for f_idx in range(n_flow):
        for (i, j) in edges:
            lp.Add(r[f_idx][(i, j)] <= x[(i, j)])

    # Objective (in original float units — LP solver handles floats natively)
    channel_term = lp.Sum([inp.alpha * x[(i, j)] for (i, j) in edges])
    hops_term    = lp.Sum([inp.beta  * r[f_idx][(i, j)]
                           for f_idx in range(n_flow) for (i, j) in edges])
    penalty_term = lp.Sum([
        inp.gamma * weight * r[f_idx][edge]
        for (f_idx, edge), weight in inp.soft_penalties.items()
        if f_idx < n_flow and edge in r[f_idx]
    ])

    lp.Minimize(channel_term + hops_term + penalty_term)

    status = lp.Solve()
    lp_time_s = time.time() - t0

    if status == pywraplp.Solver.OPTIMAL:
        return {"bound": lp.Objective().Value(), "lp_time_s": lp_time_s}
    elif status == pywraplp.Solver.FEASIBLE:
        # Suboptimal but valid lower bound
        return {"bound": lp.Objective().Value(), "lp_time_s": lp_time_s}
    else:
        logger.warning(f"LP relaxation did not solve: status={status}")
        return {"bound": 0.0, "lp_time_s": lp_time_s}
