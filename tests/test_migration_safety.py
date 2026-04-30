"""
tests/test_migration_safety.py

Unit tests for backend/migration/migration_safety.py.

Tests cover:
  - classify_app: TCP_CLIENT default, BINDINGS heuristic for mainframe hosting
  - compute_migration_safety: per-app dicts, summary, graph mutation, CSV export
  - Edge cases: empty graph, no app metadata
"""
import sys
import os
sys.path.insert(0, "/home/claude/item_d")

import networkx as nx

from backend.migration.migration_safety import (
    classify_app,
    compute_migration_safety,
    to_csv_string,
    CLASS_TCP_CLIENT,
    CLASS_BINDINGS,
    CLASS_SNA_OUT_OF_SCOPE,
    CLASS_PINNED_REVIEW,
    ALL_CLASSES,
)


# ─────────────────────────────────────────────────────────────────────────────
# classify_app
# ─────────────────────────────────────────────────────────────────────────────

def test_classify_default_is_tcp_client():
    cls, reason = classify_app("APP_001", {}, raw_data={})
    assert cls == CLASS_TCP_CLIENT
    assert "TCP_CLIENT" in reason or "Default" in reason


def test_classify_empty_metadata_is_tcp_client():
    cls, reason = classify_app("APP_001", {}, None)
    assert cls == CLASS_TCP_CLIENT


def test_classify_distributed_hosting_is_tcp_client():
    cls, reason = classify_app("APP_001",
                                {"hosting_type": "DISTRIBUTED"}, {})
    assert cls == CLASS_TCP_CLIENT


def test_classify_mainframe_hosting_is_bindings():
    cls, reason = classify_app("APP_001",
                                {"hosting_type": "MAINFRAME"}, {})
    assert cls == CLASS_BINDINGS
    assert "MAINFRAME" in reason or "z/OS" in reason


def test_classify_zos_variants_are_bindings():
    for value in ("Z/OS", "ZOS", "z/OS", "zos", "Z OS"):
        cls, _ = classify_app("APP_X", {"hosting_type": value}, {})
        assert cls == CLASS_BINDINGS, f"Expected BINDINGS for hosting_type={value!r}"


def test_classify_unknown_hosting_is_tcp_client():
    cls, _ = classify_app("APP_001",
                          {"hosting_type": "UNKNOWN"}, {})
    assert cls == CLASS_TCP_CLIENT


def test_classify_reason_explains_heuristic():
    """The reason for a BINDINGS classification must explicitly say it's a
    heuristic — so a panel reviewer cannot mistake it for an authoritative
    determination."""
    _, reason = classify_app("APP_001", {"hosting_type": "MAINFRAME"}, {})
    assert "heuristic" in reason.lower() or "may be wrong" in reason.lower() or \
           "manual confirmation" in reason.lower()


# ─────────────────────────────────────────────────────────────────────────────
# compute_migration_safety
# ─────────────────────────────────────────────────────────────────────────────

def _build_simple_graph():
    """Three apps, three QMs (1:1), one channel A→B."""
    G = nx.DiGraph()
    for q in ("QM_A", "QM_B", "QM_C"):
        G.add_node(q, type="qm")
    for a in ("APP_A", "APP_B", "APP_C"):
        G.add_node(a, type="app")
    G.add_edge("APP_A", "QM_A", rel="connects_to")
    G.add_edge("APP_B", "QM_B", rel="connects_to")
    G.add_edge("APP_C", "QM_C", rel="connects_to")
    G.add_edge("QM_A", "QM_B", rel="channel")
    # one local queue per QM
    for qm in ("QM_A", "QM_B", "QM_C"):
        lq = f"LQ.{qm}"
        G.add_node(lq, type="queue")
        G.add_edge(qm, lq, rel="owns")
    return G


def test_compute_safety_returns_full_shape():
    G = _build_simple_graph()
    raw = {"app_metadata": {}}

    result = compute_migration_safety(G, raw)

    assert "summary" in result
    assert "per_app" in result
    assert "method" in result
    assert "notes" in result
    assert result["method"] == "rules_based_v1"

    # Summary fields
    s = result["summary"]
    assert s["total_apps"] == 3
    assert sum(s["by_class"].values()) == 3
    assert set(s["by_class"].keys()) == set(ALL_CLASSES)


def test_compute_safety_default_all_tcp_client():
    G = _build_simple_graph()
    raw = {"app_metadata": {}}
    result = compute_migration_safety(G, raw)
    assert result["summary"]["by_class"][CLASS_TCP_CLIENT] == 3
    assert result["summary"]["by_class"][CLASS_BINDINGS] == 0


def test_compute_safety_mainframe_heuristic_applied():
    G = _build_simple_graph()
    raw = {"app_metadata": {
        "APP_A": {"hosting_type": "MAINFRAME"},
        "APP_B": {"hosting_type": "DISTRIBUTED"},
        "APP_C": {},
    }}
    result = compute_migration_safety(G, raw)
    by_class = result["summary"]["by_class"]
    assert by_class[CLASS_TCP_CLIENT] == 2
    assert by_class[CLASS_BINDINGS] == 1


def test_compute_safety_writes_to_graph_nodes():
    G = _build_simple_graph()
    raw = {"app_metadata": {"APP_A": {"hosting_type": "MAINFRAME"}}}
    compute_migration_safety(G, raw)

    # APP_A node has all 5 fields written
    a = G.nodes["APP_A"]
    assert a["migration_class"] == CLASS_BINDINGS
    assert "MAINFRAME" in a["migration_class_reason"] or "heuristic" in a["migration_class_reason"].lower()
    assert a["migration_independent"] is True  # 1:1 case
    assert a["dependency_cluster"] == ["APP_A"]
    assert isinstance(a["estimated_drain_window_s"], int)
    assert a["estimated_drain_window_s"] > 0


def test_compute_safety_per_app_sorted_by_id():
    G = _build_simple_graph()
    raw = {"app_metadata": {}}
    result = compute_migration_safety(G, raw)
    ids = [r["app_id"] for r in result["per_app"]]
    assert ids == sorted(ids)


def test_compute_safety_independence_strict_1to1():
    """In strict 1:1 mode every app is independent (cluster size 1)."""
    G = _build_simple_graph()
    raw = {"app_metadata": {}}
    result = compute_migration_safety(G, raw)

    assert result["summary"]["independent_count"] == 3
    assert result["summary"]["non_independent_count"] == 0
    assert result["summary"]["max_dependency_cluster_size"] == 1

    for row in result["per_app"]:
        assert row["migration_independent"] is True
        assert row["dependency_cluster"] == [row["app_id"]]


def test_compute_safety_multitenancy_detected():
    """If two apps share a QM (violating 1:1, e.g. during Phase 2 cutover),
    they are flagged as non-independent and share a dependency cluster."""
    G = nx.DiGraph()
    G.add_node("QM_SHARED", type="qm")
    G.add_node("APP_A", type="app")
    G.add_node("APP_B", type="app")
    G.add_edge("APP_A", "QM_SHARED", rel="connects_to")
    G.add_edge("APP_B", "QM_SHARED", rel="connects_to")

    result = compute_migration_safety(G, {"app_metadata": {}})

    a = next(r for r in result["per_app"] if r["app_id"] == "APP_A")
    b = next(r for r in result["per_app"] if r["app_id"] == "APP_B")

    assert a["migration_independent"] is False
    assert b["migration_independent"] is False
    assert a["dependency_cluster"] == ["APP_A", "APP_B"]
    assert b["dependency_cluster"] == ["APP_A", "APP_B"]
    assert result["summary"]["max_dependency_cluster_size"] == 2
    assert result["summary"]["non_independent_count"] == 2


def test_compute_safety_drain_window_grows_with_outbound_channels():
    """A QM with more outbound channels has a larger drain window estimate."""
    G_simple = nx.DiGraph()
    G_simple.add_node("QM_A", type="qm")
    G_simple.add_node("APP_A", type="app")
    G_simple.add_edge("APP_A", "QM_A", rel="connects_to")

    r_simple = compute_migration_safety(G_simple, {"app_metadata": {}})
    drain_simple = r_simple["per_app"][0]["estimated_drain_window_s"]

    # Now add 3 outbound channels from QM_A
    G_busy = nx.DiGraph()
    G_busy.add_node("QM_A", type="qm")
    G_busy.add_node("APP_A", type="app")
    G_busy.add_edge("APP_A", "QM_A", rel="connects_to")
    for i in range(3):
        target = f"QM_T{i}"
        G_busy.add_node(target, type="qm")
        G_busy.add_edge("QM_A", target, rel="channel")

    r_busy = compute_migration_safety(G_busy, {"app_metadata": {}})
    drain_busy = r_busy["per_app"][0]["estimated_drain_window_s"]

    assert drain_busy > drain_simple


def test_compute_safety_empty_graph():
    G = nx.DiGraph()
    result = compute_migration_safety(G, {})
    assert result["summary"]["total_apps"] == 0
    assert result["per_app"] == []
    assert "skipped" in result["notes"].lower() or "no apps" in result["notes"].lower()


def test_compute_safety_no_raw_data():
    """Defensive: caller passes None or {} for raw_data."""
    G = _build_simple_graph()
    result = compute_migration_safety(G, None)
    assert result["summary"]["total_apps"] == 3
    assert all(r["migration_class"] == CLASS_TCP_CLIENT for r in result["per_app"])


# ─────────────────────────────────────────────────────────────────────────────
# CSV export
# ─────────────────────────────────────────────────────────────────────────────

def test_to_csv_string_has_header():
    G = _build_simple_graph()
    result = compute_migration_safety(G, {"app_metadata": {}})
    csv_str = to_csv_string(result)
    lines = csv_str.strip().split("\n")
    assert lines[0].startswith("app_id,target_qm,migration_class,")
    assert len(lines) == 1 + 3  # header + 3 apps


def test_to_csv_string_pipe_separated_clusters():
    """dependency_cluster column uses pipe so it survives the comma delimiter."""
    G = nx.DiGraph()
    G.add_node("QM_X", type="qm")
    G.add_node("APP_A", type="app")
    G.add_node("APP_B", type="app")
    G.add_edge("APP_A", "QM_X", rel="connects_to")
    G.add_edge("APP_B", "QM_X", rel="connects_to")

    result = compute_migration_safety(G, {})
    csv_str = to_csv_string(result)
    # Both apps should reference APP_A|APP_B in their cluster column
    assert "APP_A|APP_B" in csv_str


def test_to_csv_string_empty():
    csv_str = to_csv_string({"per_app": []})
    lines = csv_str.strip().split("\n")
    assert len(lines) == 1  # just the header


if __name__ == "__main__":
    # Run all tests
    import inspect
    tests = [(name, obj) for name, obj in globals().items()
             if name.startswith("test_") and callable(obj)]
    passed = 0
    failed = []
    for name, fn in tests:
        try:
            fn()
            print(f"✓ {name}")
            passed += 1
        except AssertionError as e:
            print(f"✗ {name}: {e}")
            failed.append((name, str(e)))
        except Exception as e:
            print(f"✗ {name}: {type(e).__name__}: {e}")
            failed.append((name, f"{type(e).__name__}: {e}"))
    print(f"\n{passed}/{len(tests)} passed")
    if failed:
        sys.exit(1)
