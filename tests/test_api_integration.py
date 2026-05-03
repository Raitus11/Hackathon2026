"""
tests/test_api_integration.py

End-to-end integration test for the IntelliAI API.

This test exercises the full LangGraph pipeline + main.py response builder +
API surface. It complements the 29 module-level tests in test_migration_safety.py
(which verify migration_safety in isolation): this test verifies the *contract*
between the architect, the response builder, and any API client.

WHY THIS TEST EXISTS
====================
The bug class this catches: a future refactor that drops migration_safety from
state["migration_safety"], or that breaks the architect → _build_response wiring,
would pass every unit test in test_migration_safety.py — because those tests
import compute_migration_safety directly. They never exercise the architect agent
or the response builder.

Marco's persona feedback in the 13-persona review put this exact gap on the
critical list:
    "You have no test for the architect-agent integration path. Without this,
     a regression in the architect that drops migration_safety silently passes
     all 20 [now 29] unit tests."

This test runs the full pipeline against a synthetic 12-row CSV (smallest
that exercises every code path), waits for completion, and asserts the
end-to-end shape including migration_safety presence and consistency.

DESIGN CHOICES
==============
- Uses FastAPI TestClient (synchronous httpx) — no live server needed.
- The pipeline still runs in a background daemon thread; this test polls
  /api/session/{id}/progress until status="done" or a 90s timeout fires.
- A 90s timeout is generous; on a small CSV the pipeline typically completes
  in 5-15s. The buffer accommodates LLM latency variations.
- Synthetic CSV is built with the EXACT column schema from csv_ingest.py.
  Three apps, two QMs, one cross-QM flow. Smallest topology that produces
  a non-empty migration_safety block.

USAGE
=====
    pytest tests/test_api_integration.py -v
"""
import json
import time
import uuid
import zipfile
from io import BytesIO, StringIO
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


# ─────────────────────────────────────────────────────────────────────────────
# Synthetic CSV — smallest topology that exercises migration_safety
# ─────────────────────────────────────────────────────────────────────────────
#
# Layout:
#   APP_A (PRODUCER on PAYMENT.IN)  on QM001
#   APP_B (CONSUMER from PAYMENT.IN) on QM002   ← cross-QM flow
#   APP_C (PRODUCER on AUDIT.IN)    on QM001
#   APP_D (CONSUMER from AUDIT.IN)  on QM002   ← cross-QM flow
#
# Producing this CSV directly mirrors csv_ingest.load_and_clean's expected
# columns. Anything missing here would be caught by the column-existence
# checks at csv_ingest.py:50-55.
#
# We include hosting_type=MAINFRAME on APP_A specifically to exercise the
# BINDINGS classification path. Without it, every app is TCP_CLIENT and the
# by_class assertion is uninteresting.

_CSV_HEADER = (
    "queue_manager_name,app_id,q_type,Discrete Queue Name,PrimaryAppRole,"
    "remote_q_mgr_name,remote_q_name,xmit_q_name,Neighborhood,line_of_business,"
    "Primary App_Full_Name,PrimaryAppDisp,ProducerName,"
    "Primary Data Classification,Primary Enterprise Critical Payment Application,"
    "Primary PCI,Primary TRTC,Primary Hosting Type"
)

_CSV_ROWS = [
    # APP_A: producer of PAYMENT.IN on QM001 (mainframe-hosted -> BINDINGS)
    "QM001,APP_A,LOCAL,PAYMENT.IN,PRODUCER,,,,EAST,PAYMENTS,App A,App A Display,App A,INTERNAL,N,N,T2,MAINFRAME",
    # APP_B: consumer of PAYMENT.IN on QM002 (distributed -> TCP_CLIENT)
    "QM002,APP_B,LOCAL,PAYMENT.IN,CONSUMER,,,,WEST,PAYMENTS,App B,App B Display,App B,INTERNAL,N,N,T2,DISTRIBUTED",
    # APP_C: producer of AUDIT.IN on QM001 (distributed -> TCP_CLIENT)
    "QM001,APP_C,LOCAL,AUDIT.IN,PRODUCER,,,,EAST,AUDIT,App C,App C Display,App C,INTERNAL,N,N,T3,DISTRIBUTED",
    # APP_D: consumer of AUDIT.IN on QM002 (distributed -> TCP_CLIENT)
    "QM002,APP_D,LOCAL,AUDIT.IN,CONSUMER,,,,WEST,AUDIT,App D,App D Display,App D,INTERNAL,N,N,T3,DISTRIBUTED",
    # Remote queue references so xmit channels get inferred
    "QM001,APP_A,REMOTE,RQ.PAYMENT,PRODUCER,QM002,PAYMENT.IN,QM002.XMITQ,EAST,PAYMENTS,App A,App A Display,App A,INTERNAL,N,N,T2,MAINFRAME",
    "QM001,APP_C,REMOTE,RQ.AUDIT,PRODUCER,QM002,AUDIT.IN,QM002.XMITQ,EAST,AUDIT,App C,App C Display,App C,INTERNAL,N,N,T3,DISTRIBUTED",
]

SYNTHETIC_CSV_TEXT = _CSV_HEADER + "\n" + "\n".join(_CSV_ROWS) + "\n"


# ─────────────────────────────────────────────────────────────────────────────
# Test fixtures
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def client():
    """Return a TestClient bound to the running FastAPI app.

    The import is inside the fixture so that environment setup (e.g. SOLVER
    flags) happens before the workflow module is loaded.
    """
    import os
    os.environ.setdefault("INTELLIAI_USE_SOLVER", "1")
    from backend.api.main import app
    return TestClient(app)


@pytest.fixture
def synthetic_csv_bytes():
    """A bytes blob ready to upload via POST /api/upload."""
    return SYNTHETIC_CSV_TEXT.encode("utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _wait_for_pipeline(client: TestClient, session_id: str, timeout_s: float = 90.0) -> dict:
    """Poll /progress until status != 'running' or timeout. Return the final
    progress dict. Raises pytest.fail on timeout — that's a real test failure.
    """
    deadline = time.time() + timeout_s
    last_status = "running"
    while time.time() < deadline:
        r = client.get(f"/api/session/{session_id}/progress")
        if r.status_code == 404:
            # Session created but not yet registered — wait a beat
            time.sleep(0.2)
            continue
        assert r.status_code == 200, f"progress endpoint returned {r.status_code}: {r.text}"
        body = r.json()
        last_status = body["status"]
        if last_status != "running":
            return body
        time.sleep(0.5)
    pytest.fail(
        f"Pipeline did not complete within {timeout_s}s (last status: {last_status})"
    )


def _upload_csv_and_wait(client, csv_bytes) -> tuple[str, dict]:
    """POST CSV, return (session_id, final_progress_dict). Pipeline complete."""
    files = {"file": ("test_data.csv", csv_bytes, "text/csv")}
    r = client.post("/api/upload", files=files)
    assert r.status_code == 200, f"upload failed: {r.status_code} {r.text}"
    body = r.json()
    assert "session_id" in body
    session_id = body["session_id"]
    progress = _wait_for_pipeline(client, session_id)
    return session_id, progress


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_health_endpoint(client):
    """Sanity: /health responds before we touch anything else."""
    r = client.get("/health")
    assert r.status_code == 200


def test_full_pipeline_returns_migration_safety(client, synthetic_csv_bytes):
    """The PRIMARY end-to-end test.

    Upload CSV → wait for pipeline → GET /api/session/{id} → assert response
    contains migration_safety with the right shape and values.

    Catches: any regression that drops migration_safety from _build_response,
    from the architect's return, or from the state schema.
    """
    session_id, progress = _upload_csv_and_wait(client, synthetic_csv_bytes)
    assert progress["status"] == "done", (
        f"Pipeline failed: {progress.get('error')}"
    )

    # Pipeline complete; fetch the full result
    r = client.get(f"/api/session/{session_id}")
    assert r.status_code == 200, f"GET session failed: {r.status_code} {r.text}"
    response = r.json()

    # ── migration_safety must be present and non-null ─────────────────
    assert "migration_safety" in response, (
        "migration_safety missing from response. _build_response in main.py "
        "may have dropped it."
    )
    safety = response["migration_safety"]
    assert safety is not None, (
        "migration_safety is null. Architect agent likely failed to compute "
        "it; check agent_trace for '⚠ migration_safety computation failed'."
    )

    # ── Shape checks ──────────────────────────────────────────────────
    for required_key in ("summary", "per_app", "method", "notes"):
        assert required_key in safety, f"safety missing key: {required_key}"

    summary = safety["summary"]
    for k in ("total_apps", "by_class", "independent_count",
              "non_independent_count", "max_dependency_cluster_size"):
        assert k in summary, f"summary missing key: {k}"

    for cls in ("TCP_CLIENT", "BINDINGS", "SNA_OUT_OF_SCOPE", "PINNED_REVIEW"):
        assert cls in summary["by_class"], f"by_class missing {cls}"

    # ── Value sanity ──────────────────────────────────────────────────
    # Synthetic CSV has 4 apps. After dedup in the architect, expect 4 in target.
    assert summary["total_apps"] == 4, (
        f"Expected 4 apps, got {summary['total_apps']}. Per-app: "
        f"{[r['app_id'] for r in safety['per_app']]}"
    )
    assert sum(summary["by_class"].values()) == summary["total_apps"]

    # APP_A has hosting_type=MAINFRAME → BINDINGS heuristic should fire
    assert summary["by_class"]["BINDINGS"] >= 1, (
        f"Expected at least 1 BINDINGS app (APP_A hosting_type=MAINFRAME), "
        f"got by_class={summary['by_class']}"
    )

    # Strict 1:1 → all apps independent
    assert summary["independent_count"] == summary["total_apps"], (
        f"Expected all apps independent under 1:1, got "
        f"{summary['independent_count']}/{summary['total_apps']}"
    )
    assert summary["max_dependency_cluster_size"] == 1

    # per_app must be sorted, must match total_apps
    per_app = safety["per_app"]
    assert len(per_app) == summary["total_apps"]
    app_ids = [row["app_id"] for row in per_app]
    assert app_ids == sorted(app_ids), "per_app should be sorted by app_id"


def test_response_has_all_critical_keys(client, synthetic_csv_bytes):
    """SECONDARY: defensive shape check on the entire response.

    Marco's persona feedback: a regression that drops ANY critical field
    silently breaks the UI without breaking unit tests. This test asserts
    every key the frontend depends on is present.
    """
    session_id, _ = _upload_csv_and_wait(client, synthetic_csv_bytes)
    r = client.get(f"/api/session/{session_id}")
    assert r.status_code == 200
    response = r.json()

    # Critical keys the frontend reads — extracted from App.jsx
    critical_keys = [
        "session_id",
        "as_is_graph",
        "target_graph",
        "as_is_metrics",
        "target_metrics",
        "complexity_reduction",
        "validation_passed",
        "constraint_violations",
        "adrs",
        "agent_trace",
        "target_csvs",
        "solver_run",
        "compliance_audit",
        "migration_safety",
        "architect_method",
        "awaiting_human_review",
    ]

    missing = [k for k in critical_keys if k not in response]
    assert not missing, (
        f"Response missing critical keys: {missing}. "
        f"Present keys: {sorted(response.keys())}"
    )


def test_target_graph_app_nodes_carry_migration_class(client, synthetic_csv_bytes):
    """Per-app graph mutation must propagate to the serialized target_graph.

    The architect calls compute_migration_safety which writes migration_class
    onto each app node. graph_to_dict serializes those attributes for the
    frontend. This test catches the case where graph_to_dict strips the new
    fields, or where the architect path that builds the graph forgets to
    call migration_safety.
    """
    session_id, _ = _upload_csv_and_wait(client, synthetic_csv_bytes)
    r = client.get(f"/api/session/{session_id}")
    response = r.json()
    target_graph = response.get("target_graph") or {}

    # Find app nodes
    app_nodes = [
        n for n in target_graph.get("nodes", [])
        if n.get("type") == "app"
    ]
    assert len(app_nodes) >= 4, (
        f"Expected at least 4 app nodes in target_graph, got {len(app_nodes)}"
    )

    # Every app node must have migration_class etc.
    for node in app_nodes:
        for required_attr in ("migration_class", "migration_independent",
                               "estimated_drain_window_s"):
            assert required_attr in node, (
                f"app node {node.get('id')} missing {required_attr}. "
                f"Either compute_migration_safety wasn't called, or "
                f"graph_to_dict isn't preserving the new attributes."
            )


def test_architect_emits_migration_safety_log_line(client, synthetic_csv_bytes):
    """The architect agent must emit a 'Migration safety: N apps classified' log
    line into agent_trace. This is the visible signal in the live agent stream
    that migration_safety was computed.
    """
    session_id, _ = _upload_csv_and_wait(client, synthetic_csv_bytes)
    r = client.get(f"/api/session/{session_id}")
    response = r.json()

    trace = response.get("agent_trace") or []
    architect_messages = [
        m["msg"] for m in trace
        if (m.get("agent") or "").upper() == "ARCHITECT"
    ]

    assert any("Migration safety" in m for m in architect_messages), (
        f"Expected architect to log 'Migration safety: ...' message. "
        f"Architect messages found: {architect_messages}"
    )


def test_evidence_bundle_includes_migration_classification_csv(client, synthetic_csv_bytes):
    """The evidence bundle ZIP must include target_migration_classification.csv.

    Verifies the provisioner path writes the new CSV into target_csvs and
    that the bundle exporter picks it up.
    """
    session_id, _ = _upload_csv_and_wait(client, synthetic_csv_bytes)

    # Approve the design so the provisioner runs (it's gated behind approval)
    approve_r = client.post(
        f"/api/review/{session_id}",
        json={"decision": "approve", "feedback": ""},
    )
    # Approval may 200 or may take additional time; either way wait again
    if approve_r.status_code == 200:
        # Pipeline resumes; wait for it to finish all the way through provisioner
        _wait_for_pipeline(client, session_id, timeout_s=60.0)

    # Evidence bundle download
    r = client.get(f"/api/session/{session_id}/evidence")
    assert r.status_code == 200, f"evidence download failed: {r.status_code}"
    assert r.headers["content-type"].startswith("application/"), (
        f"unexpected content-type: {r.headers.get('content-type')}"
    )

    # Read the zip contents
    zf = zipfile.ZipFile(BytesIO(r.content))
    names = zf.namelist()

    # target_migration_classification CSV (extension may or may not be there
    # depending on bundle naming convention)
    matching = [n for n in names if "migration_classification" in n.lower()]
    assert matching, (
        f"target_migration_classification CSV not in evidence bundle. "
        f"Bundle contents: {names}"
    )

    # Quick sanity: the CSV has the right columns
    csv_content = zf.read(matching[0]).decode("utf-8")
    first_line = csv_content.split("\n")[0]
    expected_cols = [
        "app_id", "target_qm", "migration_class", "migration_class_reason",
        "migration_independent", "dependency_cluster",
        "estimated_drain_window_s",
    ]
    for col in expected_cols:
        assert col in first_line, (
            f"Column '{col}' not in CSV header: {first_line!r}"
        )


def test_migration_safety_is_null_safe_when_architect_fails(client):
    """Defensive: if the architect can't run (e.g. malformed CSV), the
    response should still be parseable. migration_safety may be null but
    the response shape should not crash.

    We send an effectively empty CSV — just a header, no rows. Pipeline
    should fail or produce an empty topology. Either way the response
    must be well-formed.
    """
    bad_csv = (_CSV_HEADER + "\n").encode("utf-8")
    files = {"file": ("empty.csv", bad_csv, "text/csv")}
    r = client.post("/api/upload", files=files)
    if r.status_code != 200:
        # Upload rejected — that's fine, defensive validation working
        return

    body = r.json()
    session_id = body["session_id"]
    progress = _wait_for_pipeline(client, session_id, timeout_s=60.0)

    # Either pipeline fails (status="failed") OR completes with empty topology
    if progress["status"] == "failed":
        # Acceptable — we just want to confirm the system doesn't crash
        return

    r = client.get(f"/api/session/{session_id}")
    assert r.status_code == 200
    response = r.json()
    # migration_safety may be null or have total_apps=0; both are fine
    safety = response.get("migration_safety")
    if safety is not None:
        assert safety["summary"]["total_apps"] == 0
