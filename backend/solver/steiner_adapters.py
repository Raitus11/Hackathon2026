"""
backend/solver/steiner_adapters.py

Bridges between the architect's nx.DiGraph and the directed Steiner solver.
Symmetric to backend/solver/adapters.py but for the new formulation.

WHAT THIS REPLACES:
  adapters.py builds a SolverInput for the multi-commodity flow CP-SAT
  formulation: every (producer_qm, consumer_qm) pair becomes a flow with
  its own routing variables. At 480 QMs / 5K pairs, this produces ~10^9
  binary variables — unsolvable. See cpsat_solver.py for the math.

  steiner_adapters.py builds a SteinerInput for the directed Steiner network
  formulation: each unique (src_qm, tgt_qm) pair is just a connectivity
  requirement, not a flow. The solver picks the channel set; routing is
  derived from the channel set via BFS. See steiner_solver.py for the math.

WHAT THIS DOES:
  1. derive_steiner_input_from_graph:
       graph + raw_data → SteinerInput + per-pair metadata
  2. apply_steiner_output_to_graph:
       graph + SteinerOutput + metadata → optimised graph
  3. run_steiner_on_graph:
       Convenience: graph + raw_data → optimised graph + SteinerOutput

CITES:
  - Charikar et al. 1999 — see steiner_solver.py for the algorithmic core.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Optional

import networkx as nx

from backend.solver.required_pairs import derive_required_pairs
from backend.solver.steiner_solver import (
    SteinerInput,
    SteinerOutput,
    solve as solve_steiner,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Graph → SteinerInput
# ─────────────────────────────────────────────────────────────────────────────

def derive_steiner_input_from_graph(
    G: nx.DiGraph,
    raw_data: dict,
    soft_penalties_from_llm: Optional[dict] = None,
    alpha: float = 1.0,
    beta: float = 0.3,
    gamma: float = 1.0,
    time_budget_s: float = 30.0,
) -> tuple[SteinerInput, list[dict]]:
    """Build a SteinerInput from the target graph + raw_data.

    Returns:
      (SteinerInput, pair_metadata)
        SteinerInput.required_pairs is sorted, deduplicated.
        pair_metadata is a list parallel to required_pairs, each entry recording
        the queues + producer apps + consumer apps that justify that pair.
    """
    # Extract QMs from the graph
    qm_ids = [n for n, d in G.nodes(data=True) if d.get("type") == "qm"]
    if not qm_ids:
        raise ValueError("derive_steiner_input: no QM nodes in graph")

    # Derive required pairs (handles app→QM mapping, queue-name join, dedup)
    pairs, metadata = derive_required_pairs(G, raw_data)

    # Compile soft penalties
    if soft_penalties_from_llm is not None:
        # Trust the LLM output (validated upstream)
        soft_penalties = soft_penalties_from_llm
    else:
        # Rules-based fallback (carried over from adapters.py logic but keyed
        # by required-pair index instead of multi-flow index)
        soft_penalties = _rule_based_soft_penalties(pairs, metadata, raw_data, G)

    inp = SteinerInput(
        qms=qm_ids,
        required_pairs=pairs,
        soft_penalties=soft_penalties,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        time_budget_s=time_budget_s,
    )

    logger.info(
        f"steiner_input: {len(qm_ids)} QMs, {len(pairs)} required pairs, "
        f"{len(soft_penalties)} soft penalties; "
        f"α={alpha}, β={beta}, γ={gamma}, budget={time_budget_s}s"
    )

    return inp, metadata


def _rule_based_soft_penalties(
    pairs: list[tuple[str, str]],
    metadata: list[dict],
    raw_data: dict,
    G: nx.DiGraph,
) -> dict[tuple[int, tuple[str, str]], float]:
    """Rules-based fallback for soft compliance penalties.

    DESIGN NOTE: Earlier iterations of this function emitted a penalty for
    EVERY (pair × qm × incoming_edge_to_qm) combination, producing O(|R|·|V|²)
    entries — at production scale (480 QMs, 2000 pairs) that's 460M dict
    entries which OOMs / hangs.

    The materialization of (pair, edge) penalties only makes sense when the
    real solver needs each entry. Our solver looks up penalties by (pair_idx,
    edge) via dict.__contains__, so any edge-key not present is treated as
    zero penalty. We can therefore emit penalties LAZILY: only for edges that
    are plausibly used. Two approaches:

      (A) Emit only for edges originating from a QM in the pair's "home"
          class — that limits emission to |R| * |V| entries at most.
      (B) Return empty and let the LLM-driven Business Context Translator
          populate this in future. Rule-based has never been the source of
          truth; it's a placeholder.

    Until the BCT is written (Day 4 ticket), we go with (B): empty dict.
    Empty penalties means the rule-based path produces a solver result with
    γ-term = 0, which is honest about what we're doing. The Architect's BCT
    will replace this when implemented, and the SteinerInput.soft_penalties
    schema is unchanged so swapping is trivial.
    """
    if not raw_data.get("app_metadata"):
        return {}
    # See design note above. Returning empty rather than emitting an O(|R|·|V|²)
    # dict that hangs at scale. The Business Context Translator agent will
    # populate this from explicit business rules + LLM judgment.
    logger.info(
        f"steiner_adapters: rule-based penalties returning empty "
        f"({len(pairs)} pairs would produce O(|R|*|V|^2) entries; "
        f"awaiting Business Context Translator)"
    )
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# SteinerOutput → optimised graph
# ─────────────────────────────────────────────────────────────────────────────

def apply_steiner_output_to_graph(
    G: nx.DiGraph,
    out: SteinerOutput,
    pair_metadata: list[dict],
    pairs: list[tuple[str, str]],
) -> nx.DiGraph:
    """Apply the Steiner solver's chosen channels to the graph.

    Returns a NEW graph with:
      - All existing nodes preserved.
      - All existing 'connects_to' (app→QM) and 'owns' (QM→queue) edges preserved.
      - All existing 'channel' (QM→QM) edges REMOVED.
      - Solver-chosen channels ADDED with deterministic name FROM_QM.TO_QM,
        xmit queue {tgt_qm}.XMITQ (matches the existing convention; verify
        against Output.md when available — see Day 1 risk #5), and metadata
        recording which pairs traverse each channel.

    Pairs whose route goes through a channel are recorded as `pairs_using` on
    the channel's edge metadata; this lets the UI explain "this channel exists
    because pairs X, Y, Z need to route through it."
    """
    if out.status not in ("OPTIMAL", "TIMEOUT_PARTIAL"):
        logger.warning(
            f"apply_steiner_output: status={out.status}; returning graph unchanged"
        )
        return G.copy()

    H = G.copy()

    # Remove existing channel edges
    channel_edges_to_remove = [
        (u, v) for u, v, d in H.edges(data=True) if d.get("rel") == "channel"
    ]
    H.remove_edges_from(channel_edges_to_remove)
    logger.info(
        f"apply_steiner_output: removed {len(channel_edges_to_remove)} "
        f"existing channels"
    )

    # Build channel → pairs-using-it map from the route info
    channel_to_pair_idxs: dict[tuple[str, str], list[int]] = defaultdict(list)
    for pair_idx, route in out.pair_routes.items():
        for edge in route:
            channel_to_pair_idxs[edge].append(pair_idx)

    # Add chosen channels
    added = 0
    for (src_qm, tgt_qm) in out.channels_chosen:
        if src_qm not in H.nodes or tgt_qm not in H.nodes:
            logger.warning(
                f"apply_steiner_output: channel {src_qm}->{tgt_qm} references "
                f"missing node; skipping"
            )
            continue

        pair_idxs = channel_to_pair_idxs.get((src_qm, tgt_qm), [])

        # Aggregate evidence: which pairs route through this channel
        evidence = []
        for pair_idx in pair_idxs:
            if pair_idx < len(pair_metadata):
                m = pair_metadata[pair_idx]
                evidence.append({
                    "src_qm": m["src_qm"],
                    "tgt_qm": m["tgt_qm"],
                    "queues": m["queues"],
                    "producer_apps": m["producer_apps"],
                    "consumer_apps": m["consumer_apps"],
                })

        channel_name = f"{src_qm}.{tgt_qm}"
        xmit_queue = f"{tgt_qm}.XMITQ"  # convention; verify per Day 1 risk #5

        H.add_edge(
            src_qm, tgt_qm,
            rel="channel",
            channel_name=channel_name,
            status="RUNNING",
            xmit_queue=xmit_queue,
            # Solver provenance
            solver_chosen=True,
            solver_method="steiner_local_search",
            pairs_using=pair_idxs,
            pair_evidence=evidence,
        )
        added += 1

    logger.info(
        f"apply_steiner_output: added {added} solver-chosen channels"
    )

    return H


# ─────────────────────────────────────────────────────────────────────────────
# Convenience: full pipeline
# ─────────────────────────────────────────────────────────────────────────────

def run_steiner_on_graph(
    G: nx.DiGraph,
    raw_data: dict,
    *,
    soft_penalties_from_llm: Optional[dict] = None,
    alpha: float = 1.0,
    beta: float = 0.3,
    gamma: float = 1.0,
    time_budget_s: float = 30.0,
) -> tuple[nx.DiGraph, SteinerOutput, dict]:
    """End-to-end: graph in → optimised graph + Steiner output + debug dict out.

    Drop-in replacement for adapters.run_solver_on_graph with the Steiner
    formulation underneath.
    """
    inp, pair_metadata = derive_steiner_input_from_graph(
        G, raw_data,
        soft_penalties_from_llm=soft_penalties_from_llm,
        alpha=alpha, beta=beta, gamma=gamma,
        time_budget_s=time_budget_s,
    )
    out = solve_steiner(inp)
    optimised = apply_steiner_output_to_graph(G, out, pair_metadata, inp.required_pairs)

    return optimised, out, {
        "input": inp,
        "pair_metadata": pair_metadata,
    }
