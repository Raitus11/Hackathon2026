"""
tests/test_migration_safety.py

Unit tests for backend/migration/migration_safety.py.

Tests cover:
  - classify_app: TCP_CLIENT default, BINDINGS heuristic for mainframe hosting
  - compute_migration_safety: per-app dicts, summary, graph mutation, CSV export
  - _classify_apps_pure: read-only sibling that does NOT mutate the graph
  - Drain-window cap (Diego: prevent absurd estimates on outlier QMs)
  - CSV injection defense (Aisha: OWASP formula injection)
  - Edge cases: empty graph, no app metadata
"""
import networkx as nx

from backend.migration.migration_safety import (
    classify_app,
    compute_migration_safety,
    _classify_apps_pure,
    to_csv_string,
    _csv_safe,
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


# ─────────────────────────────────────────────────────────────────────────────
# Round 1 — Drain-window cap (Diego)
# ─────────────────────────────────────────────────────────────────────────────

def test_drain_window_capped_at_120s():
    """A QM with 50 outbound channels would compute to 10 + 5*50 = 260s
    without the cap. With cap = 120, the result must be 120."""
    G = nx.DiGraph()
    G.add_node("QM_BUSY", type="qm")
    G.add_node("APP_BUSY", type="app")
    G.add_edge("APP_BUSY", "QM_BUSY", rel="connects_to")
    # 50 outbound channels — uncapped formula = 260s
    for i in range(50):
        target = f"QM_TARGET_{i:02d}"
        G.add_node(target, type="qm")
        G.add_edge("QM_BUSY", target, rel="channel")

    result = compute_migration_safety(G, {"app_metadata": {}})
    drain = result["per_app"][0]["estimated_drain_window_s"]
    assert drain == 120, f"Expected drain capped at 120s, got {drain}s"


def test_drain_window_below_cap_unchanged():
    """A QM with a few channels should produce a number below the cap,
    and the cap should not artificially inflate it."""
    G = nx.DiGraph()
    G.add_node("QM_SMALL", type="qm")
    G.add_node("APP_SMALL", type="app")
    G.add_edge("APP_SMALL", "QM_SMALL", rel="connects_to")
    # 3 outbound channels: 10 + 5*3 = 25s, well below 120 cap
    for i in range(3):
        target = f"QM_T{i}"
        G.add_node(target, type="qm")
        G.add_edge("QM_SMALL", target, rel="channel")

    result = compute_migration_safety(G, {"app_metadata": {}})
    drain = result["per_app"][0]["estimated_drain_window_s"]
    assert drain == 25, f"Expected 25s for 3 channels, got {drain}s"


# ─────────────────────────────────────────────────────────────────────────────
# Round 1 — CSV injection defense (Aisha)
# ─────────────────────────────────────────────────────────────────────────────

def test_csv_safe_neutralizes_formula_prefix():
    """Cells starting with =, +, -, @ are formula triggers in Excel.
    The sanitizer must prefix a single quote so Excel treats them as text."""
    assert _csv_safe("=cmd|'/c calc'!A1").startswith("'=")
    assert _csv_safe("+1+1").startswith("'+")
    assert _csv_safe("-2*3").startswith("'-")
    assert _csv_safe("@SUM(A1:A10)").startswith("'@")


def test_csv_safe_passthrough_for_normal_values():
    """Normal cells must NOT be modified."""
    assert _csv_safe("APP_001") == "APP_001"
    assert _csv_safe("QM_A042") == "QM_A042"
    assert _csv_safe("TCP_CLIENT") == "TCP_CLIENT"
    assert _csv_safe("Default: assumed TCP_CLIENT.") == "Default: assumed TCP_CLIENT."
    # Numbers and bools still pass through as their str() form
    assert _csv_safe(42) == "42"
    assert _csv_safe(True) == "True"


def test_csv_safe_handles_none_and_empty():
    assert _csv_safe(None) == ""
    assert _csv_safe("") == ""


def test_to_csv_string_sanitizes_injected_classification_reason():
    """Threat model: an attacker controls app_metadata.hosting_type which
    flows through into migration_class_reason. They set it to a value
    starting with '=' to inject a formula. The CSV writer must defang it.
    """
    G = nx.DiGraph()
    G.add_node("QM_X", type="qm")
    G.add_node("APP_EVIL", type="app")
    G.add_edge("APP_EVIL", "QM_X", rel="connects_to")

    # Even though the current heuristic doesn't propagate hosting_type
    # verbatim into the reason, future Phase 2 paths might. Test that
    # ANY '=' in a row value gets sanitized.
    result = compute_migration_safety(G, {"app_metadata": {}})
    # Inject an evil reason directly into the safety dict (simulating a
    # future code path where reason flows from external data)
    evil = "=cmd|'/c calc'!A1"
    result["per_app"][0]["migration_class_reason"] = evil

    csv_str = to_csv_string(result)
    # The sanitized form (single-quote-prefixed) MUST appear
    assert "'" + evil in csv_str, f"Sanitized form not found in CSV: {csv_str!r}"
    # The cell, when split out, must START with a single quote — not '='
    # (parse the CSV row to be sure)
    import csv as csv_mod
    from io import StringIO
    rows = list(csv_mod.reader(StringIO(csv_str)))
    # Header is row 0, evil app is row 1, reason is column 3
    reason_cell = rows[1][3]
    assert reason_cell.startswith("'="), (
        f"Reason cell must be prefixed with single quote to neutralize the formula. "
        f"Got: {reason_cell!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Round 1 — Pure function (Eleanor)
# ─────────────────────────────────────────────────────────────────────────────

def test_classify_apps_pure_does_not_mutate_graph():
    """_classify_apps_pure must be read-only on the graph. It returns the
    safety dict but does NOT write migration_class etc. onto graph nodes.
    """
    G = _build_simple_graph()
    raw = {"app_metadata": {"APP_A": {"hosting_type": "MAINFRAME"}}}

    # Snapshot node attributes before
    before = {n: dict(d) for n, d in G.nodes(data=True)}

    result = _classify_apps_pure(G, raw)

    # Result is correct
    assert result["summary"]["total_apps"] == 3
    assert any(r["migration_class"] == CLASS_BINDINGS for r in result["per_app"])

    # But graph is UNCHANGED
    after = {n: dict(d) for n, d in G.nodes(data=True)}
    assert before == after, (
        f"Graph nodes were mutated by _classify_apps_pure!\n"
        f"Before: {before}\n"
        f"After:  {after}"
    )

    # Specifically: APP_A node must NOT have migration_class
    assert "migration_class" not in G.nodes["APP_A"]


def test_compute_migration_safety_does_mutate_graph():
    """The public compute_migration_safety MUST mutate (this is its job)."""
    G = _build_simple_graph()
    raw = {"app_metadata": {"APP_A": {"hosting_type": "MAINFRAME"}}}

    compute_migration_safety(G, raw)

    # APP_A node should now have migration_class
    assert G.nodes["APP_A"]["migration_class"] == CLASS_BINDINGS


def test_pure_and_annotating_return_same_dict():
    """compute_migration_safety should return identical data to
    _classify_apps_pure for the same input — they share the computation.
    """
    # Use two separate graph copies because compute_migration_safety mutates
    G1 = _build_simple_graph()
    G2 = _build_simple_graph()
    raw = {"app_metadata": {}}

    pure_result = _classify_apps_pure(G1, raw)
    annotating_result = compute_migration_safety(G2, raw)

    assert pure_result == annotating_result


if __name__ == "__main__":
    # Run all tests
    import sys
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
