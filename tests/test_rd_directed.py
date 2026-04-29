"""
tests/test_rd_directed.py

Regression test for the routing-depth bug fix in mq_graph.compute_complexity.

THE BUG (pre-fix):
    RD was computed on `qm_subgraph.to_undirected()`, which treated MQ SENDER
    channels as bidirectional. This produced identical RD scores for two
    topologies with very different operational behavior:

        Topology X: A↔B, B↔C   (both directions)  → undirected RD = 2 ✓
        Topology Y: A→B, B→C   (one direction)    → undirected RD = 2 ✗

    In Topology Y, C cannot reach A. The metric should reflect that.

THE FIX:
    Compute shortest paths on the directed graph. Report the directed diameter
    (D_max over reachable pairs) plus a fragmentation penalty proportional to
    the unreachable-pair ratio, scaled by N so the penalty is commensurate
    with diameter (which is O(log N) to O(√N) typically).

    RD = D_max + λ * (unreachable_pairs / N*(N-1)) * N

    with λ=1. Cite: Newman 2010, "Networks: An Introduction" §6.10
    (directed diameter); Latora & Marchiori 2001 PRL 87:198701
    (efficiency-based handling of disconnection).

These tests exercise the cases where directed and undirected RD diverge,
plus sanity checks on a fully-bidirectional topology where they should agree.
"""
import networkx as nx
import pytest

from backend.graph.mq_graph import compute_complexity


def _build_graph_from_qms_and_channels(qms, channels):
    """Tiny helper: build a graph with QM nodes and directed channel edges.

    No apps, no queues — just QMs and channels — because the RD metric
    is purely a property of the QM-to-QM directed subgraph.
    """
    G = nx.DiGraph()
    for q in qms:
        G.add_node(q, type="qm")
    for src, tgt in channels:
        G.add_edge(src, tgt, rel="channel")
    return G


# ─────────────────────────────────────────────────────────────────────────────
# THE BUG-CATCHING TESTS
# These tests fail under the old undirected-projection code and pass under
# the directed-graph code.
# ─────────────────────────────────────────────────────────────────────────────

def test_one_way_chain_has_unreachable_pairs():
    """A→B→C: 6 directed pairs, only 3 reachable. Old code reported RD=2; new
    code reports diameter=2 plus fragmentation penalty for the 3 unreachable
    pairs (B→A, C→A, C→B).
    """
    G = _build_graph_from_qms_and_channels(
        qms=["A", "B", "C"],
        channels=[("A", "B"), ("B", "C")],
    )
    m = compute_complexity(G)

    diag = m["rd_diagnostics"]
    # Reachable pairs: A→B, A→C, B→C. Unreachable: B→A, C→A, C→B.
    assert diag["unreachable_pairs"] == 3
    assert diag["directed_diameter"] == 2.0
    # 3 unreachable / 6 total = 0.5; penalty = 0.5 * N = 0.5 * 3 = 1.5
    assert diag["unreachable_ratio"] == 0.5
    assert m["routing_depth"] == pytest.approx(2.0 + 1.5, abs=0.01)


def test_bidirectional_chain_has_no_unreachable_pairs():
    """A↔B↔C: every directed pair is reachable. RD = directed diameter (2),
    no fragmentation penalty.
    """
    G = _build_graph_from_qms_and_channels(
        qms=["A", "B", "C"],
        channels=[("A", "B"), ("B", "A"), ("B", "C"), ("C", "B")],
    )
    m = compute_complexity(G)

    diag = m["rd_diagnostics"]
    assert diag["unreachable_pairs"] == 0
    assert diag["directed_diameter"] == 2.0
    assert m["routing_depth"] == pytest.approx(2.0, abs=0.01)


def test_old_code_would_have_treated_these_as_equal():
    """Smoking gun: build the same graph 'undirected-ly' (X) and 'directed-ly' (Y)
    and confirm they now produce *different* RD scores. Under the old code
    these were identical.
    """
    G_bidir = _build_graph_from_qms_and_channels(
        qms=["A", "B", "C"],
        channels=[("A", "B"), ("B", "A"), ("B", "C"), ("C", "B")],
    )
    G_oneway = _build_graph_from_qms_and_channels(
        qms=["A", "B", "C"],
        channels=[("A", "B"), ("B", "C")],
    )
    m_bidir = compute_complexity(G_bidir)
    m_oneway = compute_complexity(G_oneway)

    # Under the old (buggy) code: m_bidir["routing_depth"] == m_oneway["routing_depth"]
    # Under the fixed code: they must differ — the one-way chain has the higher RD
    assert m_oneway["routing_depth"] > m_bidir["routing_depth"], (
        f"Bug regression: one-way chain RD ({m_oneway['routing_depth']}) "
        f"should be greater than bidirectional chain RD ({m_bidir['routing_depth']}). "
        f"This means the directed-graph RD computation has regressed."
    )


def test_disconnected_components_directed():
    """{A→B} and {C→D} disconnected.
    8 directed (u,v) pairs in {A,B,C,D}^2 \\ diagonal:
      Reachable: A→B, C→D = 2
      Unreachable: 6
    Diameter of reachable part: 1.
    Penalty: 6/12 * 4 = 2.0
    RD = 1 + 2 = 3.
    """
    G = _build_graph_from_qms_and_channels(
        qms=["A", "B", "C", "D"],
        channels=[("A", "B"), ("C", "D")],
    )
    m = compute_complexity(G)
    diag = m["rd_diagnostics"]

    assert diag["unreachable_pairs"] == 10  # 12 total - 2 reachable
    assert diag["directed_diameter"] == 1.0
    # Test against the unrounded diagnostic fields (source of truth);
    # routing_depth is rounded to 1 decimal in the return dict so we
    # can't pin its value exactly when fragmentation_penalty is fractional.
    assert diag["unreachable_ratio"] == pytest.approx(10/12, abs=0.001)
    expected_rd_unrounded = 1.0 + (10/12) * 4
    assert m["routing_depth"] == pytest.approx(expected_rd_unrounded, abs=0.1)


def test_strongly_connected_cycle():
    """Directed cycle A→B→C→D→A: every QM reaches every other. RD = diameter.
    Diameter of a 4-cycle: 3 (longest shortest path is 3 hops, e.g. A→B→C→D).
    """
    G = _build_graph_from_qms_and_channels(
        qms=["A", "B", "C", "D"],
        channels=[("A", "B"), ("B", "C"), ("C", "D"), ("D", "A")],
    )
    m = compute_complexity(G)
    diag = m["rd_diagnostics"]

    assert diag["unreachable_pairs"] == 0
    assert diag["directed_diameter"] == 3.0
    assert m["routing_depth"] == pytest.approx(3.0, abs=0.01)


def test_isolated_qm_with_apps_marked_unreachable():
    """{A→B, C isolated}: 6 directed pairs.
    Reachable: A→B = 1.
    Unreachable: A→C, B→A, B→C, C→A, C→B = 5.
    Diameter: 1. Penalty: 5/6 * 3 = 2.5. RD = 3.5.
    """
    G = _build_graph_from_qms_and_channels(
        qms=["A", "B", "C"],
        channels=[("A", "B")],
    )
    m = compute_complexity(G)
    diag = m["rd_diagnostics"]

    assert diag["unreachable_pairs"] == 5
    assert diag["directed_diameter"] == 1.0
    assert m["routing_depth"] == pytest.approx(1.0 + (5/6) * 3, abs=0.01)


def test_single_qm_rd_zero():
    """One QM with no channels: trivially RD=0, no pairs to be reachable."""
    G = _build_graph_from_qms_and_channels(qms=["A"], channels=[])
    m = compute_complexity(G)
    assert m["routing_depth"] == 0.0
    assert m["rd_diagnostics"]["unreachable_pairs"] == 0


def test_empty_graph():
    """Zero QMs: graceful degradation, no exceptions."""
    G = nx.DiGraph()
    m = compute_complexity(G)
    assert m["routing_depth"] == 0.0


def test_baseline_overrides_dont_break_diagnostics():
    """When called with baseline_overrides (target-side scoring), the
    rd_diagnostics block must still be populated correctly. Regression for
    a hoisting bug where defaults could shadow real values.
    """
    G = _build_graph_from_qms_and_channels(
        qms=["A", "B", "C"],
        channels=[("A", "B"), ("B", "C")],
    )
    # First pass: compute as-is to get baselines
    asis = compute_complexity(G)
    # Second pass: compute target with baseline overrides
    target = compute_complexity(G, baseline_overrides=asis["baselines"])

    # Both must report the same RD diagnostics (same graph)
    assert target["rd_diagnostics"]["unreachable_pairs"] == 3
    assert target["rd_diagnostics"]["directed_diameter"] == 2.0
    assert target["routing_depth"] == asis["routing_depth"]


# ─────────────────────────────────────────────────────────────────────────────
# Sanity checks — full mq_graph pipeline
# ─────────────────────────────────────────────────────────────────────────────

def test_minimal_compute_complexity_runs():
    """Smoke test: compute_complexity on a small realistic graph runs without
    error and returns the expected keys, including the new rd_diagnostics."""
    G = _build_graph_from_qms_and_channels(
        qms=["QM_A", "QM_B"],
        channels=[("QM_A", "QM_B"), ("QM_B", "QM_A")],
    )
    m = compute_complexity(G)
    assert "channel_count" in m
    assert "routing_depth" in m
    assert "rd_diagnostics" in m
    assert set(m["rd_diagnostics"].keys()) == {
        "directed_diameter", "unreachable_pairs", "unreachable_ratio",
        "fragmentation_penalty"
    }
