"""
backend/solver/optimizer_hook.py

Drop-in replacement Phase 2 for optimizer_agent.

THREE SOLVER PATHS (auto-selected):

  STEINER (default for ≥ ~50 QMs or ≥ ~200 required pairs):
    Greedy local-search on the directed Steiner network formulation.
    Polynomial time, 2-approximate (Charikar et al. 1999). Scales to
    480 QMs / 5000 pairs in ~10 seconds. See steiner_solver.py.

  CP-SAT (small benchmarks only):
    Exact integer programming on the multi-commodity flow formulation
    (Magnanti & Wong 1984). Tight LP bound for small instances. Does
    NOT scale: 480 QMs OOMs. Kept for B1-style benchmark fixtures
    and for unit-testing the Steiner solver against a known optimum
    on small cases.

  MST (legacy fallback, only if both solvers refuse to run):
    Original Phase 2 in agents.py. Undirected MST on the QM graph.
    NOT reachability-correct in general — see Day 2 analysis. Retained
    only because some demos still assume its output shape.

USAGE — wire into optimizer_agent in agents.py near the top:

    from backend.solver.optimizer_hook import run_solver_phase, USE_SOLVER
    if USE_SOLVER:
        result = run_solver_phase(G, state)
        if result is not None:
            messages.append({"agent": "OPTIMIZER", "msg": result["message"]})
            return {
                "optimised_graph":    result["graph"],
                "target_metrics":     result["target_metrics"],
                "target_subgraphs":   result["target_subgraphs"],
                "target_communities": result["target_communities"],
                "target_centrality":  result["target_centrality"],
                "target_entropy":     result["target_entropy"],
                "messages":           messages,
                "solver_run":         result["solver_run"],
            }
        # Solver returned None → fall through to legacy MST path below

    # (existing Phase 1/2/3 code continues unchanged)

ENV CONTROLS:
  INTELLIAI_USE_SOLVER=1            — enable solver (default OFF for safety)
  INTELLIAI_SOLVER_STRATEGY=auto    — auto|steiner|cpsat (default auto)
  INTELLIAI_SOLVER_TIME_BUDGET=60   — seconds; default 60

WHY THIS DESIGN:
  Marcus's principle: never make a critical-path change without a working
  escape hatch. The flag-defaults-OFF keeps the existing demo working
  unchanged. The auto-strategy picks Steiner at production scale where
  CP-SAT can't run, and CP-SAT at benchmark scale where its tighter LP
  bound is useful.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import networkx as nx

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Feature flags
# ─────────────────────────────────────────────────────────────────────────────
# Default: solver OFF. Existing pipeline runs MST.
# To enable: set INTELLIAI_USE_SOLVER=1
USE_SOLVER = os.environ.get("INTELLIAI_USE_SOLVER", "0") == "1"

# Strategy: 'auto' | 'steiner' | 'cpsat'
#   auto    — Steiner if (n_qms > 30 or n_pairs > 200), else CP-SAT
#   steiner — always Steiner
#   cpsat   — always CP-SAT (warns if instance too large)
SOLVER_STRATEGY = os.environ.get("INTELLIAI_SOLVER_STRATEGY", "auto").lower()

# Cutoff for auto-strategy
AUTO_QM_CUTOFF = 30
AUTO_PAIR_CUTOFF = 200


# ─────────────────────────────────────────────────────────────────────────────
# Default solver hyperparameters
# ─────────────────────────────────────────────────────────────────────────────
# These match the IntelliAI Phase 1 Plan v3 §I.2.1 defaults.
DEFAULT_ALPHA = 1.0          # weight on channel count
DEFAULT_BETA  = 0.3          # weight on routing hops
DEFAULT_GAMMA = 1.0          # weight on soft compliance penalties
DEFAULT_TIME_BUDGET_S = float(os.environ.get("INTELLIAI_SOLVER_TIME_BUDGET", "60"))


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_solver_phase(G: nx.DiGraph, state: dict) -> Optional[dict]:
    """Replace optimizer Phases 1-3 with a solver pass (Steiner or CP-SAT).

    Auto-strategy: dispatches to Steiner for production-scale instances,
    CP-SAT for small benchmarks. Override via INTELLIAI_SOLVER_STRATEGY env.

    Returns:
      dict with all fields the optimizer_agent normally produces, OR
      None if the solver invocation failed and the caller should fall
      through to the legacy MST path.

    On success the returned dict has an additional 'solver_run' field
    capturing solver telemetry for the UI's solver-result panel.
    """
    raw_data = state.get("raw_data") or {}
    if not raw_data:
        logger.warning("OPTIMIZER-SOLVER: no raw_data in state; falling through")
        return None

    # Pull hyperparameters
    soft_penalties_from_llm = state.get("business_context_penalties")
    solver_strategy_state = state.get("solver_strategy") or {}
    alpha = solver_strategy_state.get("alpha", DEFAULT_ALPHA)
    beta = solver_strategy_state.get("beta", DEFAULT_BETA)
    gamma = solver_strategy_state.get("gamma", DEFAULT_GAMMA)
    time_budget_s = solver_strategy_state.get("time_budget_s", DEFAULT_TIME_BUDGET_S)

    # Pre-extract n_qms and n_pairs to pick auto-strategy without doing
    # full work twice
    n_qms = sum(1 for _, d in G.nodes(data=True) if d.get("type") == "qm")
    # Number of pairs: cheap to compute
    try:
        from backend.solver.required_pairs import derive_required_pairs
        pairs_preview, _ = derive_required_pairs(G, raw_data)
        n_pairs = len(pairs_preview)
    except Exception as e:
        logger.warning(f"OPTIMIZER-SOLVER: pair-count preview failed ({e}); "
                        f"defaulting to Steiner")
        n_pairs = 1_000_000  # force Steiner

    # Pick strategy
    chosen = _choose_strategy(SOLVER_STRATEGY, n_qms, n_pairs)

    logger.info(
        f"OPTIMIZER-SOLVER: dispatching strategy={chosen} "
        f"(n_qms={n_qms}, n_pairs={n_pairs}, "
        f"α={alpha}, β={beta}, γ={gamma}, budget={time_budget_s}s)"
    )

    if chosen == "steiner":
        return _run_steiner_phase(
            G, raw_data, state,
            soft_penalties_from_llm=soft_penalties_from_llm,
            alpha=alpha, beta=beta, gamma=gamma,
            time_budget_s=time_budget_s,
        )
    elif chosen == "cpsat":
        return _run_cpsat_phase(
            G, raw_data, state,
            soft_penalties_from_llm=soft_penalties_from_llm,
            alpha=alpha, beta=beta, gamma=gamma,
            time_budget_s=time_budget_s,
        )
    else:
        logger.error(f"OPTIMIZER-SOLVER: unknown strategy {chosen!r}")
        return None


def _choose_strategy(requested: str, n_qms: int, n_pairs: int) -> str:
    """Resolve the strategy keyword to a concrete solver name.

    'auto' picks Steiner for large instances, CP-SAT for small.
    """
    if requested == "steiner":
        return "steiner"
    if requested == "cpsat":
        if n_qms > AUTO_QM_CUTOFF or n_pairs > AUTO_PAIR_CUTOFF:
            logger.warning(
                f"OPTIMIZER-SOLVER: forced CP-SAT on instance with "
                f"n_qms={n_qms}, n_pairs={n_pairs} — likely to OOM or timeout. "
                f"Consider strategy='auto' or 'steiner'."
            )
        return "cpsat"
    # 'auto' or unknown
    if n_qms > AUTO_QM_CUTOFF or n_pairs > AUTO_PAIR_CUTOFF:
        return "steiner"
    return "cpsat"


# ─────────────────────────────────────────────────────────────────────────────
# Shared: compute downstream analytics on the optimised graph
# ─────────────────────────────────────────────────────────────────────────────

def _compute_target_analytics(optimised: nx.DiGraph, asis_baselines: Optional[dict]) -> Optional[dict]:
    """Compute target_metrics + subgraphs + communities + centrality + entropy
    on the optimised graph. Returns dict with these keys, or None on import failure.
    """
    try:
        from backend.graph.mq_graph import (
            compute_complexity,
            analyse_subgraphs,
            detect_communities,
            compute_centrality,
            compute_graph_entropy,
        )
    except ImportError as e:
        try:
            from backend.graph import (
                compute_complexity,
                analyse_subgraphs,
                detect_communities,
                compute_centrality,
                compute_graph_entropy,
            )
        except ImportError:
            logger.exception(f"OPTIMIZER-SOLVER: cannot import mq_graph functions ({e})")
            return None

    return {
        "target_metrics":     compute_complexity(optimised, baseline_overrides=asis_baselines),
        "target_subgraphs":   analyse_subgraphs(optimised),
        "target_communities": detect_communities(optimised),
        "target_centrality":  compute_centrality(optimised),
        "target_entropy":     compute_graph_entropy(optimised),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Steiner path — production scale, polynomially fast
# ─────────────────────────────────────────────────────────────────────────────

def _run_steiner_phase(
    G: nx.DiGraph,
    raw_data: dict,
    state: dict,
    *,
    soft_penalties_from_llm: Optional[dict],
    alpha: float, beta: float, gamma: float,
    time_budget_s: float,
) -> Optional[dict]:
    """Invoke the directed Steiner solver and produce optimizer_agent state."""
    try:
        from backend.solver.steiner_adapters import run_steiner_on_graph
    except ImportError as e:
        logger.error(f"OPTIMIZER-SOLVER: cannot import Steiner adapters ({e})")
        return None

    # ── Capture architect's actual channel count BEFORE solving ──────────
    # out.initial_channel_count is the count of unique required pairs (what
    # the solver starts from, conceptually). The architect's *actual* graph
    # may have more channels (e.g., backfill for new QMs that have no
    # required-pair representation yet). For reporting we want the actual
    # count operators see.
    architect_channel_count = sum(
        1 for _, _, d in G.edges(data=True) if d.get("rel") == "channel"
    )

    try:
        optimised, out, debug = run_steiner_on_graph(
            G, raw_data,
            soft_penalties_from_llm=soft_penalties_from_llm,
            alpha=alpha, beta=beta, gamma=gamma,
            time_budget_s=time_budget_s,
        )
    except Exception as e:
        logger.exception(f"OPTIMIZER-SOLVER: Steiner invocation failed: {e}")
        return None

    if out.status not in ("OPTIMAL", "TIMEOUT_PARTIAL"):
        logger.warning(f"OPTIMIZER-SOLVER: Steiner status={out.status}; falling through")
        return None

    asis = state.get("as_is_metrics") or {}
    analytics = _compute_target_analytics(optimised, asis.get("baselines"))
    if analytics is None:
        return None

    # ── Integrity check: optimised graph channel count == solver decision ─
    # This catches a class of bug where downstream code accidentally adds or
    # removes channels between the solver returning and the graph being
    # returned to the optimizer agent. If they ever diverge, every downstream
    # claim ("75% reduction") becomes a lie. We assert and log loudly.
    actual_channel_count = sum(
        1 for _, _, d in optimised.edges(data=True) if d.get("rel") == "channel"
    )
    if actual_channel_count != out.final_channel_count:
        logger.error(
            f"OPTIMIZER-SOLVER: INTEGRITY FAILURE — solver decided "
            f"{out.final_channel_count} channels but optimised graph has "
            f"{actual_channel_count} channels. This is a bug. The reduction "
            f"numbers reported to the UI will be inconsistent with reality."
        )

    # ── Pull as-is channel count for the comparison ──────────────────────
    as_is_graph = state.get("as_is_graph")
    asis_channel_count = (
        sum(1 for _, _, d in as_is_graph.edges(data=True) if d.get("rel") == "channel")
        if as_is_graph is not None else None
    )

    asis_score = asis.get("total_score", 0)
    target_score = analytics["target_metrics"].get("total_score", 0)
    if asis_score:
        score_pct = (asis_score - target_score) / asis_score * 100  # positive = reduction
        if score_pct >= 0:
            score_str = f"{asis_score:.1f} → {target_score:.1f} (-{score_pct:.1f}%, reduction)"
        else:
            score_str = f"{asis_score:.1f} → {target_score:.1f} (+{-score_pct:.1f}%, INCREASE)"
    else:
        score_str = f"target={target_score:.1f}"

    # Channel reduction relative to architect's output (what the solver actually
    # received) AND relative to as-is (what the user actually had before).
    # Format: negative = reduction, positive = increase. Phrasing made
    # explicit because "+34.8%" vs "-34.8%" is easy to misread.
    def _fmt_change(before: int, after: int) -> str:
        if before <= 0:
            return "n/a"
        delta_pct = (after - before) / before * 100
        if delta_pct < 0:
            return f"{abs(delta_pct):.1f}% reduction"
        elif delta_pct > 0:
            return f"{delta_pct:.1f}% INCREASE"
        else:
            return "no change"

    arch_to_solver_str = _fmt_change(architect_channel_count, actual_channel_count)
    arch_to_solver_pct = (
        (actual_channel_count - architect_channel_count) / max(architect_channel_count, 1) * 100
    )

    if asis_channel_count and asis_channel_count > 0:
        asis_to_solver_str = _fmt_change(asis_channel_count, actual_channel_count)
        asis_to_solver_pct = (
            (actual_channel_count - asis_channel_count) / asis_channel_count * 100
        )
        channel_str = (
            f"channels: as-is={asis_channel_count} → "
            f"architect={architect_channel_count} → "
            f"solver={actual_channel_count} "
            f"({asis_to_solver_str} vs as-is, "
            f"{arch_to_solver_str} vs architect)"
        )
    else:
        asis_to_solver_pct = None
        channel_str = (
            f"channels: architect={architect_channel_count} → "
            f"solver={actual_channel_count} "
            f"({arch_to_solver_str})"
        )

    # ── Honest gap reporting (don't clamp to 100%) ────────────────────────
    # The LP-bound gap can exceed 100% on dense uniform-cost instances where
    # the LP relaxation is loose. Clamping makes a 200% gap look like a
    # 100% gap, which is dishonest. Show the real number.
    if out.lower_bound > 0:
        true_lp_gap_pct = (out.objective_value - out.lower_bound) / out.lower_bound * 100
    else:
        true_lp_gap_pct = float('inf') if out.objective_value > 0 else 0.0

    message = (
        f"Steiner solver: {channel_str}; "
        f"obj={out.objective_value:.1f}, LB={out.lower_bound:.1f}, "
        f"LP_gap={true_lp_gap_pct:.0f}% "
        f"(alg max gap ≤{out.max_optimality_gap_pct:.0f}%, Charikar 1999), "
        f"t={out.solve_time_s:.1f}s, iters={out.iterations}; "
        f"complexity {score_str}"
    )

    # Log a detailed summary at INFO so it's visible in operator logs
    # without needing to inspect the messages array.
    logger.info(f"OPTIMIZER-SOLVER: {channel_str}")
    logger.info(
        f"OPTIMIZER-SOLVER: complexity {score_str}; "
        f"V-009 reachability check is performed by tester_agent next"
    )
    logger.info(
        f"OPTIMIZER-SOLVER: solve_time={out.solve_time_s:.2f}s, "
        f"iterations={out.iterations}, "
        f"objective={out.objective_value:.2f}, "
        f"lower_bound={out.lower_bound:.2f}, "
        f"LP_gap_pct={true_lp_gap_pct:.1f} (uncapped — large values mean "
        f"the LP bound is loose, not that the solution is bad), "
        f"algorithmic_max_gap_pct={out.max_optimality_gap_pct:.0f} "
        f"(Charikar et al. 1999, 2-approx)"
    )

    return {
        "graph":              optimised,
        "target_metrics":     analytics["target_metrics"],
        "target_subgraphs":   analytics["target_subgraphs"],
        "target_communities": analytics["target_communities"],
        "target_centrality":  analytics["target_centrality"],
        "target_entropy":     analytics["target_entropy"],
        "message": message,
        "solver_run": {
            "method":               "steiner_local_search",
            "status":               out.status,
            "objective_value":      out.objective_value,
            "lower_bound":          out.lower_bound,
            # gap_pct is the legacy clamped value; lp_gap_pct_uncapped is honest
            "gap_pct":              out.gap_pct,
            "lp_gap_pct_uncapped":  true_lp_gap_pct,
            "max_optimality_gap_pct": out.max_optimality_gap_pct,
            "approximation_ratio":  "2-approx (Charikar et al. 1999)",
            "channels_chosen":      list(out.channels_chosen),
            "pair_routes":          {str(k): v for k, v in out.pair_routes.items()},
            "objective_breakdown":  out.objective_breakdown,
            "iterations":           out.iterations,
            "initial_channel_count": out.initial_channel_count,
            "final_channel_count":  out.final_channel_count,
            # Comparison context (so UI can show the full reduction story)
            "architect_channel_count": architect_channel_count,
            "asis_channel_count":   asis_channel_count,
            "actual_channel_count": actual_channel_count,
            "integrity_check_passed": (actual_channel_count == out.final_channel_count),
            # NEGATIVE = reduction, POSITIVE = increase. Be explicit.
            "delta_pct_vs_asis": asis_to_solver_pct,
            "delta_pct_vs_architect": arch_to_solver_pct,
            "solve_time_s":         out.solve_time_s,
            "alpha":                alpha,
            "beta":                 beta,
            "gamma":                gamma,
            "time_budget_s":        time_budget_s,
            "n_pairs":              len(debug["input"].required_pairs),
            "n_qms":                len(debug["input"].qms),
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# CP-SAT path — small benchmarks only, kept as escape hatch
# ─────────────────────────────────────────────────────────────────────────────

def _run_cpsat_phase(
    G: nx.DiGraph,
    raw_data: dict,
    state: dict,
    *,
    soft_penalties_from_llm: Optional[dict],
    alpha: float, beta: float, gamma: float,
    time_budget_s: float,
) -> Optional[dict]:
    """Invoke the legacy CP-SAT solver on the multi-commodity flow formulation.

    Only use on small instances. See the module docstring.
    """
    try:
        from backend.solver.adapters import run_solver_on_graph
    except ImportError as e:
        logger.error(f"OPTIMIZER-SOLVER: cannot import CP-SAT adapters ({e})")
        return None

    try:
        optimised, out, debug = run_solver_on_graph(
            G, raw_data,
            soft_penalties_from_llm=soft_penalties_from_llm,
            alpha=alpha, beta=beta, gamma=gamma,
            time_budget_s=time_budget_s,
        )
    except Exception as e:
        logger.exception(f"OPTIMIZER-SOLVER: CP-SAT invocation failed: {e}")
        return None

    if out.status not in ("OPTIMAL", "FEASIBLE"):
        logger.warning(f"OPTIMIZER-SOLVER: CP-SAT status={out.status}; falling through")
        return None

    asis = state.get("as_is_metrics") or {}
    analytics = _compute_target_analytics(optimised, asis.get("baselines"))
    if analytics is None:
        return None

    n_channels = len(out.channels_chosen)
    asis_score = asis.get("total_score", 0)
    target_score = analytics["target_metrics"].get("total_score", 0)
    if asis_score:
        pct = (asis_score - target_score) / asis_score * 100
        score_str = f"{asis_score:.1f} → {target_score:.1f} ({pct:+.1f}%)"
    else:
        score_str = f"target={target_score:.1f}"

    message = (
        f"CP-SAT solver: {n_channels} channels chosen "
        f"(integer={out.integer_optimum:.2f}, LP={out.lp_bound:.2f}, "
        f"gap={out.gap_pct:.1f}%, t={out.solve_time_s:.1f}s); "
        f"complexity {score_str}"
    )

    return {
        "graph":              optimised,
        "target_metrics":     analytics["target_metrics"],
        "target_subgraphs":   analytics["target_subgraphs"],
        "target_communities": analytics["target_communities"],
        "target_centrality":  analytics["target_centrality"],
        "target_entropy":     analytics["target_entropy"],
        "message": message,
        "solver_run": {
            "method":          "cpsat_mcnf",
            "status":          out.status,
            "integer_optimum": out.integer_optimum,
            "lp_bound":        out.lp_bound,
            "gap_pct":         out.gap_pct,
            "channels_chosen": list(out.channels_chosen),
            "flow_routes":     {str(k): v for k, v in out.flow_routes.items()},
            "solve_time_s":    out.solve_time_s,
            "lp_time_s":       out.lp_time_s,
            "branches":        out.branches,
            "objective_breakdown": out.objective_breakdown,
            "alpha":           alpha,
            "beta":            beta,
            "gamma":           gamma,
            "time_budget_s":   time_budget_s,
            "n_flows":         len(debug["input"].flows),
            "n_qms":           len(debug["input"].qms),
        },
    }
