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
from typing import Optional, List

from backend.orchestration.workflow import mq_titan_workflow
from backend.agents.agents import provisioner_agent, migration_planner_agent, doc_expert_agent
from backend.graph.mq_graph import graph_to_dict, sanitise
from backend.llm.llm_client import call_llm_chat

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


class ChatMessage(BaseModel):
    role: str       # "user" or "assistant"
    content: str

class ChatRequest(BaseModel):
    message: str
    history: List[ChatMessage] = []


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
        "awaiting_human_review": (
            result.get("awaiting_human_review", False)
            and not result.get("human_approved")
            and not result.get("human_aborted")
        ),
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

    try:
        if decision.abort:
            # Abort: just run doc_expert for cancellation report
            updates = doc_expert_agent(result)
            result.update(updates)
        elif decision.approved:
            # Approve: run the 3 remaining agents directly
            # (Avoids re-running the full pipeline from supervisor)
            updates = provisioner_agent(result)
            result.update(updates)
            updates = migration_planner_agent(result)
            result.update(updates)
            updates = doc_expert_agent(result)
            result.update(updates)
        else:
            # Reject: re-run full pipeline with feedback injected
            # (Architect needs to redesign based on feedback)
            final_result = mq_titan_workflow.invoke(result)
            # Force flags after re-run (pipeline may overwrite them)
            final_result["human_approved"]        = None  # reset for next review
            final_result["human_feedback"]        = ""
            final_result["human_aborted"]         = False
            final_result["awaiting_human_review"] = True  # will pause at gate again
            result = final_result
    except Exception as e:
        logger.exception("Pipeline resume error")
        raise HTTPException(status_code=500, detail=str(e))

    # Store updated state
    sessions[session_id] = result
    response = _build_response(session_id, result)
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

# ── Chat with Architect AI ───────────────────────────────────────────────────

CHAT_SYSTEM_PROMPT = """You are the MQ-TITAN Architect AI. You designed the proposed target state for an IBM MQ topology.
The human reviewer is asking you questions about your design decisions before approving or requesting changes.

Answer concisely and specifically. Reference actual QM names, app IDs, and channel names from the topology.
If the reviewer suggests changes, acknowledge them and explain what the impact would be.
Keep responses under 150 words. Be direct and technical."""


@app.post("/api/chat/{session_id}")
def chat_with_architect(session_id: str, req: ChatRequest):
    """Chat with the Architect AI about the current design."""
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    result = sessions[session_id]

    # Build context about the current design
    adrs = result.get("adrs", [])
    as_is = result.get("as_is_metrics", {}) or {}
    target = result.get("target_metrics", {}) or {}
    method = result.get("architect_method", "rules_fallback")
    violations = result.get("constraint_violations", [])

    adr_summary = "\n".join(
        f"- {a.get('id','')}: {a.get('decision','')}" for a in adrs
    ) if adrs else "No ADRs."

    violation_summary = "\n".join(
        f"- [{v.get('severity','')}] {v.get('rule','')}: {v.get('detail','')}"
        for v in violations[:5]
    ) if violations else "All constraints passed."

    context = (
        f"Design method: {method}\n"
        f"As-is score: {as_is.get('total_score', 'N/A')}/100\n"
        f"Target score: {target.get('total_score', 'N/A')}/100\n"
        f"ADRs:\n{adr_summary}\n"
        f"Constraint status:\n{violation_summary}"
    )

    system = f"{CHAT_SYSTEM_PROMPT}\n\n## CURRENT DESIGN CONTEXT:\n{context}"

    # Build message history for LLM
    llm_messages = []
    for msg in req.history:
        llm_messages.append({"role": msg.role, "content": msg.content})
    llm_messages.append({"role": "user", "content": req.message})

    # Try LLM call
    reply = call_llm_chat(
        system_prompt=system,
        messages=llm_messages,
        max_tokens=512,
        temperature=0.3,
    )

    if not reply:
        # Fallback — generate a contextual response based on user's question
        q = req.message.lower()
        adr_text = "; ".join(f"{a.get('id','')}: {a.get('decision','')}" for a in adrs[:3])

        if any(w in q for w in ["why", "reason", "explain", "rationale"]):
            reply = (
                f"My decisions were based on the {method} method. "
                f"Key rationale: {adrs[0].get('rationale', 'optimise topology') if adrs else 'reduce complexity'}. "
                f"The target achieves {target.get('total_score', '?')}/100 vs {as_is.get('total_score', '?')}/100 as-is."
            )
        elif any(w in q for w in ["change", "keep", "remove", "move", "don't", "dont", "stop"]):
            reply = (
                f"I hear your concern. If we adjust the design, it could affect the complexity score "
                f"(currently {target.get('total_score', '?')}/100). "
                f"Type your specific changes and click Revise — I'll redesign with your constraints in mind."
            )
        elif any(w in q for w in ["qm", "queue manager", "channel", "app"]):
            reply = (
                f"Current design decisions: {adr_text or 'none recorded'}. "
                f"The target has {target.get('channel_count', '?')} channels "
                f"(down from {as_is.get('channel_count', '?')}). "
                f"Ask about a specific QM or app for details."
            )
        elif any(w in q for w in ["score", "metric", "complexity", "reduction"]):
            reply = (
                f"Complexity breakdown — As-is: {as_is.get('total_score', '?')}/100, "
                f"Target: {target.get('total_score', '?')}/100. "
                f"Channels: {as_is.get('channel_count', '?')} → {target.get('channel_count', '?')}. "
                f"Coupling: {as_is.get('coupling_index', '?')} → {target.get('coupling_index', '?')}. "
                f"{'All constraints pass.' if not violations else f'{len(violations)} constraint violations remain.'}"
            )
        else:
            reply = (
                f"I designed this topology using {method}. "
                f"Score: {as_is.get('total_score', '?')} → {target.get('total_score', '?')} "
                f"({result.get('complexity_reduction', {}).get('reduction_pct', '?')}% reduction). "
                f"Decisions: {adr_text or 'none'}. "
                f"You can ask about specific QMs, channels, scores, or tell me what to change."
            )

    return JSONResponse(content={"reply": reply})
