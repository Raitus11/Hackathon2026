"""
tests/test_solver_adapter.py

End-to-end test of the solver adapter:
    raw_data (csv_ingest format)
    → build_graph (mq_graph)
    → solver_input_from_graph
    → solve (CP-SAT)
    → apply_solver_output
    → verify the resulting graph has correct channels

This exercises the full integration the optimizer_agent will use,
without requiring the real demo CSV. Synthetic but matches the
exact format csv_ingest.load_and_clean produces.

Run:
    cd <repo_root>
    $env:PYTHONPATH = "."
    python tests\test_solver_adapter.py
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import networkx as nx

from backend.solver.adapters import (
    solver_input_from_graph,
    apply_solver_output,
    run_solver_on_graph,
)
from backend.solver.cpsat_solver import solve


# ─────────────────────────────────────────────────────────────────────────────
# Synthetic raw_data — matches csv_ingest.load_and_clean output exactly
# ─────────────────────────────────────────────────────────────────────────────
# Scenario:
#   3 QMs (QM_A, QM_B, QM_C), 4 apps:
#     A001 produces to Q.PAYMENT on QM_A
#     A002 consumes from Q.PAYMENT on QM_B  (cross-QM flow A→B)
#     A003 produces to Q.AUDIT on QM_A
#     A004 consumes from Q.AUDIT on QM_C    (cross-QM flow A→C)
#   No same-QM flows.
#   Expected solver output: 2 direct channels (A→B and A→C), or possibly
#   a hub pattern if α/β favors it.

SYNTHETIC_RAW_DATA = {
    "queue_managers": [
        {"qm_id": "QM_A", "qm_name": "QM_A", "region": "EAST", "line_of_business": "PAYMENTS"},
        {"qm_id": "QM_B", "qm_name": "QM_B", "region": "WEST", "line_of_business": "PAYMENTS"},
        {"qm_id": "QM_C", "qm_name": "QM_C", "region": "EAST", "line_of_business": "AUDIT"},
    ],
    "queues": [
        {"queue_id": "Q_001", "queue_name": "Q.PAYMENT", "qm_id": "QM_A",
         "queue_type": "LOCAL", "usage": "NORMAL"},
        {"queue_id": "Q_002", "queue_name": "Q.PAYMENT", "qm_id": "QM_B",
         "queue_type": "LOCAL", "usage": "NORMAL"},
        {"queue_id": "Q_003", "queue_name": "Q.AUDIT", "qm_id": "QM_A",
         "queue_type": "LOCAL", "usage": "NORMAL"},
        {"queue_id": "Q_004", "queue_name": "Q.AUDIT", "qm_id": "QM_C",
         "queue_type": "LOCAL", "usage": "NORMAL"},
    ],
    "applications": [
        # Producer of payments on A
        {"app_id": "A001", "app_name": "PaymentProducer", "qm_id": "QM_A",
         "queue_id": "Q_001", "queue_name": "Q.PAYMENT", "direction": "PUT"},
        # Consumer of payments on B
        {"app_id": "A002", "app_name": "PaymentConsumer", "qm_id": "QM_B",
         "queue_id": "Q_002", "queue_name": "Q.PAYMENT", "direction": "GET"},
        # Producer of audit on A
        {"app_id": "A003", "app_name": "AuditProducer", "qm_id": "QM_A",
         "queue_id": "Q_003", "queue_name": "Q.AUDIT", "direction": "PUT"},
        # Consumer of audit on C
        {"app_id": "A004", "app_name": "AuditConsumer", "qm_id": "QM_C",
         "queue_id": "Q_004", "queue_name": "Q.AUDIT", "direction": "GET"},
    ],
    "channels": [
        # Source channels — these will all be removed by the solver
        {"channel_id": "CH_S1", "channel_name": "QM_A.QM_B", "channel_type": "SENDER",
         "from_qm": "QM_A", "to_qm": "QM_B", "status": "RUNNING", "xmit_queue": "QM_B.XMITQ"},
        {"channel_id": "CH_S2", "channel_name": "QM_A.QM_C", "channel_type": "SENDER",
         "from_qm": "QM_A", "to_qm": "QM_C", "status": "RUNNING", "xmit_queue": "QM_C.XMITQ"},
        # Existing redundant channel that solver should remove
        {"channel_id": "CH_S3", "channel_name": "QM_B.QM_C", "channel_type": "SENDER",
         "from_qm": "QM_B", "to_qm": "QM_C", "status": "RUNNING", "xmit_queue": "QM_C.XMITQ"},
    ],
    "app_metadata": {
        "A001": {"data_classification": "CONFIDENTIAL", "is_pci": "YES"},
        "A002": {"data_classification": "CONFIDENTIAL", "is_pci": "YES"},
        "A003": {"data_classification": "INTERNAL", "is_pci": "NO"},
        "A004": {"data_classification": "INTERNAL", "is_pci": "NO"},
    },
}


def build_target_graph_synthetically():
    """Build a graph as if Architect had already run with strict 1:1 placement.

    Each app on its own dedicated QM. We simulate that the Architect
    has already produced the target graph; the solver / adapter is what
    we are testing.
    """
    G = nx.DiGraph()

    # QM nodes (target QMs — one per app under strict 1:1; for this test
    # we re-use the source QM_A, QM_B, QM_C names as if QM_A is A001's
    # dedicated QM, etc. In production the Architect would name them.
    # The solver doesn't care about QM names, only the topology.)
    for qm_id in ["QM_A", "QM_B", "QM_C"]:
        G.add_node(qm_id, type="qm", name=qm_id, region="EAST")

    # App nodes + connects_to edges (1:1)
    # In our synthetic example we have 4 apps but only 3 QMs. For a true
    # 1:1 we'd need a 4th QM. Add it here as QM_AUDIT_PROD for A003.
    G.add_node("QM_AUDIT_PROD", type="qm", name="QM_AUDIT_PROD", region="EAST")

    G.add_node("A001", type="app", name="PaymentProducer")
    G.add_edge("A001", "QM_A", rel="connects_to", direction="PUT")

    G.add_node("A002", type="app", name="PaymentConsumer")
    G.add_edge("A002", "QM_B", rel="connects_to", direction="GET")

    G.add_node("A003", type="app", name="AuditProducer")
    G.add_edge("A003", "QM_AUDIT_PROD", rel="connects_to", direction="PUT")

    G.add_node("A004", type="app", name="AuditConsumer")
    G.add_edge("A004", "QM_C", rel="connects_to", direction="GET")

    # Pre-existing channels that the solver will replace
    G.add_edge("QM_A", "QM_B", rel="channel", channel_name="QM_A.QM_B",
               status="RUNNING", xmit_queue="QM_B.XMITQ")
    G.add_edge("QM_A", "QM_C", rel="channel", channel_name="QM_A.QM_C",
               status="RUNNING", xmit_queue="QM_C.XMITQ")
    G.add_edge("QM_B", "QM_C", rel="channel", channel_name="QM_B.QM_C",
               status="RUNNING", xmit_queue="QM_C.XMITQ")
    G.add_edge("QM_AUDIT_PROD", "QM_C", rel="channel",
               channel_name="QM_AUDIT_PROD.QM_C",
               status="RUNNING", xmit_queue="QM_C.XMITQ")

    return G


def synthetic_raw_data_aligned_with_graph():
    """Adjust raw_data to match the synthetic target graph (A003 on QM_AUDIT_PROD)."""
    rd = {k: list(v) if isinstance(v, list) else dict(v)
          for k, v in SYNTHETIC_RAW_DATA.items()}

    # Update A003's qm_id to QM_AUDIT_PROD (matches graph)
    new_apps = []
    for app in rd["applications"]:
        a = dict(app)
        if a["app_id"] == "A003":
            a["qm_id"] = "QM_AUDIT_PROD"
        new_apps.append(a)
    rd["applications"] = new_apps

    return rd


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_flow_detection():
    """Adapter must detect the 2 cross-QM flows from raw_data."""
    G = build_target_graph_synthetically()
    raw_data = synthetic_raw_data_aligned_with_graph()

    inp, flow_meta = solver_input_from_graph(G, raw_data)

    # Print first
    print(f"\nFlow detection:")
    print(f"  QMs:   {inp.qms}")
    print(f"  Flows: {inp.flows}")
    for fm in flow_meta:
        print(f"    {fm['src_qm']} -> {fm['tgt_qm']}  queues={fm['queues']}  "
              f"prods={fm['producer_apps']}  cons={fm['consumer_apps']}")

    # Should detect 2 flows: A001→A002 (Q.PAYMENT) and A003→A004 (Q.AUDIT)
    # Translated to QMs: QM_A→QM_B and QM_AUDIT_PROD→QM_C
    assert len(inp.flows) == 2, f"Expected 2 flows, got {len(inp.flows)}: {inp.flows}"
    assert ("QM_A", "QM_B") in inp.flows
    assert ("QM_AUDIT_PROD", "QM_C") in inp.flows

    # Verify metadata
    for fm in flow_meta:
        if fm["src_qm"] == "QM_A":
            assert "Q.PAYMENT" in fm["queues"]
            assert "A001" in fm["producer_apps"]
            assert "A002" in fm["consumer_apps"]
        elif fm["src_qm"] == "QM_AUDIT_PROD":
            assert "Q.AUDIT" in fm["queues"]
            assert "A003" in fm["producer_apps"]
            assert "A004" in fm["consumer_apps"]

    print("✓ Flow detection works correctly.")


def test_solve_and_apply():
    """Full pipeline: build graph → solve → apply → verify channels."""
    G = build_target_graph_synthetically()
    raw_data = synthetic_raw_data_aligned_with_graph()

    print(f"\nInitial graph:")
    print(f"  Nodes: {sorted(G.nodes())}")
    print(f"  Channel edges before: "
          f"{sorted([(u,v) for u,v,d in G.edges(data=True) if d.get('rel')=='channel'])}")

    optimised, out, debug = run_solver_on_graph(G, raw_data, time_budget_s=10.0)

    print(f"\nSolver output:")
    print(f"  Status:           {out.status}")
    print(f"  Integer optimum:  {out.integer_optimum:.2f}")
    print(f"  LP bound:         {out.lp_bound:.2f}")
    print(f"  Gap:              {out.gap_pct:.1f}%")
    print(f"  Channels chosen:  {sorted(out.channels_chosen)}")

    print(f"\nResulting graph:")
    new_channels = [(u, v) for u, v, d in optimised.edges(data=True)
                    if d.get("rel") == "channel"]
    print(f"  Channel edges after:  {sorted(new_channels)}")

    # ── Assertions ────────────────────────────────────────────────────
    assert out.status == "OPTIMAL", f"Solver status: {out.status}"

    # With only 2 disjoint flows, the only viable solutions involve direct
    # channels (any hub would route both flows but each flow only has one
    # producer-consumer pair, so the cheapest is direct). Expect exactly 2
    # channels.
    assert len(out.channels_chosen) == 2, (
        f"Expected 2 channels (one per flow, direct routing); got "
        f"{len(out.channels_chosen)}: {out.channels_chosen}"
    )
    assert ("QM_A", "QM_B") in out.channels_chosen
    assert ("QM_AUDIT_PROD", "QM_C") in out.channels_chosen

    # Resulting graph should contain those 2 channels and NO others
    assert len(new_channels) == 2, (
        f"Resulting graph has {len(new_channels)} channel edges; expected 2"
    )
    assert ("QM_A", "QM_B") in new_channels
    assert ("QM_AUDIT_PROD", "QM_C") in new_channels

    # Existing irrelevant channel QM_B→QM_C should be REMOVED
    assert ("QM_B", "QM_C") not in new_channels, (
        "Solver should have removed the orphan QM_B->QM_C channel"
    )

    # Channel metadata
    for u, v, d in optimised.edges(data=True):
        if d.get("rel") == "channel":
            assert d.get("solver_chosen") is True
            assert d.get("channel_name") == f"{u}.{v}"
            assert d.get("xmit_queue") == f"{v}.XMITQ"
            assert "flows_using" in d
            assert len(d["flows_using"]) >= 1, f"Channel {u}->{v} has no flows"
            assert "flow_evidence" in d
            for ev in d["flow_evidence"]:
                assert ev["src_qm"]
                assert ev["tgt_qm"]
                assert ev["queues"]

    # All non-channel edges must be preserved
    orig_app_qm = [(u,v) for u,v,d in G.edges(data=True) if d.get("rel")=="connects_to"]
    new_app_qm  = [(u,v) for u,v,d in optimised.edges(data=True) if d.get("rel")=="connects_to"]
    assert sorted(orig_app_qm) == sorted(new_app_qm), (
        "App→QM edges must be preserved by the adapter"
    )

    # All nodes preserved
    assert set(G.nodes()) == set(optimised.nodes()), "Node set must be preserved"

    print("✓ End-to-end adapter works correctly.")


def test_no_flows():
    """Edge case: graph with apps but no producer-consumer pairs across QMs."""
    G = nx.DiGraph()
    G.add_node("QM_A", type="qm", name="QM_A")
    G.add_node("QM_B", type="qm", name="QM_B")
    G.add_node("A001", type="app", name="App1")
    G.add_edge("A001", "QM_A", rel="connects_to", direction="PUT")
    # Pre-existing channel that should be removed (no flows justify it)
    G.add_edge("QM_A", "QM_B", rel="channel", channel_name="QM_A.QM_B")

    raw_data = {
        "applications": [
            {"app_id": "A001", "qm_id": "QM_A", "queue_name": "Q.X", "direction": "PUT"},
            # No consumer anywhere — A001 produces but nobody consumes.
            # This is a degenerate case but the adapter must handle it.
        ],
        "app_metadata": {},
    }

    inp, flow_meta = solver_input_from_graph(G, raw_data)
    assert len(inp.flows) == 0, f"Expected 0 flows, got {len(inp.flows)}"

    out = solve(inp)
    optimised = apply_solver_output(G, out, inp, flow_metadata=flow_meta)

    new_channels = [(u, v) for u, v, d in optimised.edges(data=True)
                    if d.get("rel") == "channel"]
    assert len(new_channels) == 0, (
        f"With no flows, no channels should exist; found {new_channels}"
    )

    print("✓ Empty-flows edge case handled.")


def test_node_preservation():
    """Adapter must preserve all non-QM nodes (apps, queues) and their edges."""
    G = build_target_graph_synthetically()
    # Add a queue node for thoroughness
    G.add_node("Q_XYZ", type="queue", name="Q.XYZ")
    G.add_edge("QM_A", "Q_XYZ", rel="owns")

    raw_data = synthetic_raw_data_aligned_with_graph()

    optimised, out, _ = run_solver_on_graph(G, raw_data, time_budget_s=10.0)

    # Queue node and edge preserved
    assert "Q_XYZ" in optimised.nodes()
    assert optimised.nodes["Q_XYZ"]["type"] == "queue"
    assert optimised.has_edge("QM_A", "Q_XYZ")
    assert optimised.edges["QM_A", "Q_XYZ"]["rel"] == "owns"

    print("✓ Non-channel nodes and edges preserved.")


if __name__ == "__main__":
    test_flow_detection()
    test_no_flows()
    test_node_preservation()
    test_solve_and_apply()
    print("\n✓✓✓ All adapter tests passed. Integration ready.")
