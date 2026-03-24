"""
main.py
FastAPI server — MQ-TITAN pipeline with human review gate.

Flow:
  POST /api/demo or /api/analyse
    → Runs pipeline up to human_review_gate
    → Returns with awaiting_human_review: true
    → Frontend shows review panel to user

  GET  /api/review/{session_id}
    → Returns the pending review data (metrics, ADRs, graphs)

  POST /api/review/{session_id}
    → Human submits {approved: true} or {approved: false, feedback: "..."}
    → If approved: continues pipeline to provisioner → doc_expert
    → If rejected: injects feedback and reruns from architect
    → Returns final result
"""
import os
import uuid
import shutil
import logging
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional

from backend.orchestration.workflow import mq_titan_workflow
from backend.graph.mq_graph import graph_to_dict, sanitise

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="MQ-TITAN API",
    description="MQ Topology Intelligence & Transformation Agent Network",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

# In-memory session store
# Stores both the pipeline result state and the final response
sessions: dict = {}        # session_id -> full pipeline state (for resume)
responses: dict = {}       # session_id -> API response (for GET endpoints)


# ── Pydantic model for review submission ─────────────────────────────────────
class ReviewDecision(BaseModel):
    approved: bool
    abort: bool = False            # True = stop pipeline entirely, generate failure report
    feedback: Optional[str] = ""   # required if approved=False and abort=False


# ── Helpers ──────────────────────────────────────────────────────────────────
def _build_response(session_id: str, result: dict) -> dict:
    """Build the standard API response dict from a pipeline result."""
    as_is_graph_data  = graph_to_dict(result["as_is_graph"])     if result.get("as_is_graph")     else {}
    target_graph_data = graph_to_dict(result["optimised_graph"]) if result.get("optimised_graph") else {}

    return sanitise({
        "session_id":            session_id,
        "as_is_graph":           as_is_graph_data,
        "target_graph":          target_graph_data,
        "as_is_metrics":         result.get("as_is_metrics"),
        "target_metrics":        result.get("target_metrics"),
        "complexity_reduction":  _calc_reduction(result),
        "validation_passed":     result.get("validation_passed"),
        "constraint_violations": result.get("constraint_violations", []),
        "adrs":                  result.get("adrs", []),
        "mqsc_scripts":          result.get("mqsc_scripts", []),
        "final_report":          result.get("final_report"),
        "agent_trace":           result.get("messages", []),
        "target_csvs":           result.get("target_csvs", {}),
        "data_quality":          result.get("data_quality_report", {}),
        "awaiting_human_review": result.get("awaiting_human_review", False),
        "human_approved":        result.get("human_approved"),
        "human_aborted":         result.get("human_aborted", False),
        "architect_method":      result.get("architect_method"),
        "migration_plan":        result.get("migration_plan"),
        "topology_diff":         result.get("topology_diff"),
    })


def _run_pipeline(session_id: str, csv_paths: dict) -> dict:
    """Run the full pipeline and return the raw state result."""
    initial_state = {
        "session_id":            session_id,
        "csv_paths":             csv_paths,
        "redesign_count":        0,
        "validation_passed":     False,
        "awaiting_human_review": False,
        "human_approved":        None,
        "human_feedback":        "",
        "messages":              [],
        "adrs":                  [],
    }

    result = mq_titan_workflow.invoke(initial_state)

    # Single place to catch supervisor/pipeline errors
    if result.get("error"):
        raise HTTPException(status_code=422, detail=result["error"])
    
    return result


def _calc_reduction(result: dict) -> dict:
    as_is  = result.get("as_is_metrics", {}) or {}
    target = result.get("target_metrics", {}) or {}
    before = as_is.get("total_score", 0)
    after  = target.get("total_score", 0)
    pct    = round(((before - after) / before) * 100, 1) if before else 0
    return {"before": before, "after": after, "reduction_pct": pct}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "MQ-TITAN", "version": "2.0.0"}


@app.post("/api/demo")
def run_demo():
    """Run pipeline on synthetic CSV data. Pauses at human review gate."""
    base = Path("data/sample_input")
    csv_paths = {
        "queue_managers": str(base / "queue_managers.csv"),
        "queues":         str(base / "queues.csv"),
        "applications":   str(base / "applications.csv"),
        "channels":       str(base / "channels.csv"),
    }

    session_id = "DEMO"

    try:
        result = _run_pipeline(session_id, csv_paths)
    except Exception as e:
        logger.exception("Demo pipeline error")
        raise HTTPException(status_code=500, detail=str(e))

    # Store raw state for resume after human review
    sessions[session_id] = result

    response = _build_response(session_id, result)
    responses[session_id] = response
    return JSONResponse(content=response)


@app.post("/api/analyse")
async def analyse(
    queue_managers: UploadFile = File(...),
    queues:         UploadFile = File(...),
    applications:   UploadFile = File(...),
    channels:       UploadFile = File(...),
):
    """Upload 4 CSVs. Runs pipeline and pauses at human review gate."""
    session_id = str(uuid.uuid4())[:8]
    session_dir = UPLOAD_DIR / session_id
    session_dir.mkdir(exist_ok=True)

    csv_paths = {}
    for name, upload in [
        ("queue_managers", queue_managers),
        ("queues",         queues),
        ("applications",   applications),
        ("channels",       channels),
    ]:
        dest = session_dir / f"{name}.csv"
        with open(dest, "wb") as f:
            shutil.copyfileobj(upload.file, f)
        csv_paths[name] = str(dest)

    try:
        result = _run_pipeline(session_id, csv_paths)
    except Exception as e:
        logger.exception("Pipeline error")
        raise HTTPException(status_code=500, detail=str(e))

    if result.get("error"):
        raise HTTPException(status_code=422, detail=result["error"])

    sessions[session_id] = result
    response = _build_response(session_id, result)
    responses[session_id] = response
    return JSONResponse(content=response)


@app.get("/api/review/{session_id}")
def get_pending_review(session_id: str):
    """
    Get the pending human review data for a session.
    Called by the frontend to populate the review panel.
    """
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    result = sessions[session_id]

    if not result.get("awaiting_human_review"):
        raise HTTPException(status_code=400, detail="Session is not awaiting human review")

    asis   = result.get("as_is_metrics", {}) or {}
    target = result.get("target_metrics", {}) or {}

    return sanitise({
        "session_id":            session_id,
        "awaiting_human_review": True,
        "as_is_metrics":         asis,
        "target_metrics":        target,
        "complexity_reduction":  _calc_reduction(result),
        "adrs":                  result.get("adrs", []),
        "constraint_violations": result.get("constraint_violations", []),
        "validation_passed":     result.get("validation_passed"),
        "redesign_count":        result.get("redesign_count", 0),
        "agent_trace":           result.get("messages", []),
        "architect_method":      result.get("architect_method"),
        "as_is_graph":           graph_to_dict(result["as_is_graph"])     if result.get("as_is_graph")     else {},
        "target_graph":          graph_to_dict(result["optimised_graph"]) if result.get("optimised_graph") else {},
    })


@app.post("/api/review/{session_id}")
def submit_review(session_id: str, decision: ReviewDecision):
    """
    Human submits one of three decisions:

    Approve:  pipeline continues → provisioner → migration_planner → doc_expert → final output
    Revise:   feedback injected → reruns from architect → pauses again at review gate
    Abort:    pipeline stops → doc_expert generates failure/cancellation report → final output
    """
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    result = sessions[session_id]

    if not result.get("awaiting_human_review"):
        raise HTTPException(status_code=400, detail="Session is not awaiting human review")

    if not decision.approved and not decision.abort and not decision.feedback:
        raise HTTPException(status_code=400, detail="Revision requires a feedback reason")

    # Inject human decision into stored state
    result["human_approved"]        = decision.approved
    result["human_feedback"]        = decision.feedback or ""
    result["human_aborted"]         = decision.abort
    result["awaiting_human_review"] = False

    # Resume pipeline from human_review node
    # LangGraph invoke with the updated state picks up from human_review
    # conditional edge and routes accordingly
    try:
        final_result = mq_titan_workflow.invoke(result)
    except Exception as e:
        logger.exception("Pipeline resume error")
        raise HTTPException(status_code=500, detail=str(e))

    # Store updated state
    sessions[session_id] = final_result
    response = _build_response(session_id, final_result)
    responses[session_id] = response

    return JSONResponse(content=response)


@app.get("/api/session/{session_id}")
def get_session(session_id: str):
    if session_id not in responses:
        raise HTTPException(status_code=404, detail="Session not found")
    return responses[session_id]


@app.get("/api/session/{session_id}/csv/{csv_name}")
def download_target_csv(session_id: str, csv_name: str):
    """Download a specific target state CSV file."""
    from fastapi.responses import PlainTextResponse
    if session_id not in responses:
        raise HTTPException(status_code=404, detail="Session not found")
    csvs = responses[session_id].get("target_csvs", {})
    if csv_name not in csvs:
        raise HTTPException(
            status_code=404,
            detail=f"CSV '{csv_name}' not found. Available: {list(csvs.keys())}"
        )
    return PlainTextResponse(
        content=csvs[csv_name],
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={csv_name}.csv"}
    )
