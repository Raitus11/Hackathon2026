"""
main.py
FastAPI server — IntelliAI pipeline with human review gate.

Flow:
  POST /api/upload (single .xlsx/.csv file — PRIMARY mode)
    → Saves file, runs pipeline up to human_review_gate
    → Returns with awaiting_human_review: true

  POST /api/analyse (legacy 4-CSV upload — still supported)
    → Same flow as before

  GET  /api/review/{session_id}
    → Returns the pending review data

  POST /api/review/{session_id}
    → Human submits approve/revise/abort decision
"""
import os
import uuid
import shutil
import logging
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List

from backend.orchestration.workflow import intelli_ai_workflow, intelli_ai_revise_workflow
from backend.agents.agents import provisioner_agent, migration_planner_agent, doc_expert_agent
from backend.graph.mq_graph import graph_to_dict, sanitise
from backend.llm.llm_client import call_llm_chat



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="IntelliAI API",
    description="MQ Topology Intelligence & Transformation Agent Network",
    version="3.0.0",
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
sessions: dict = {}        # session_id -> full pipeline state (for resume)
responses: dict = {}       # session_id -> API response (for GET endpoints)


# ── Pydantic models ──────────────────────────────────────────────────────────
class ReviewDecision(BaseModel):
    approved: bool
    abort: bool = False
    feedback: Optional[str] = ""
    chat_history: Optional[List[dict]] = None  # Full chat conversation for revision context

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    message: str
    history: List[ChatMessage] = []


# ── Helpers ──────────────────────────────────────────────────────────────────
def _build_response(session_id: str, result: dict) -> dict:
    """Build the standard API response dict from a pipeline result."""
    as_is_graph_data  = graph_to_dict(result["as_is_graph"])     if result.get("as_is_graph")     else {}
    target_graph_data = graph_to_dict(result["optimised_graph"]) if result.get("optimised_graph") else {}

    # Merge deliverable_docs into target_csvs so they're downloadable from the CSVs tab
    target_csvs = result.get("target_csvs", {}) or {}
    deliverable_docs = result.get("deliverable_docs", {}) or {}
    for key, content in deliverable_docs.items():
        if content:
            target_csvs[key] = content

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
        "target_csvs":           target_csvs,
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
        "as_is_subgraphs":      result.get("as_is_subgraphs", []),
        "target_subgraphs":     result.get("target_subgraphs", []),
        "as_is_communities":    result.get("as_is_communities", {}),
        "target_communities":   result.get("target_communities", {}),
        "as_is_centrality":     result.get("as_is_centrality", {}),
        "target_centrality":    result.get("target_centrality", {}),
        "as_is_entropy":        result.get("as_is_entropy", {}),
        "target_entropy":       result.get("target_entropy", {}),
        "compliance_audit":     result.get("compliance_audit"),
        "capacity_analysis":    result.get("capacity_analysis"),
        "exec_summary":         result.get("exec_summary"),
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

    # Limit recursion: 7 linear agents + 3 retry cycles × 3 agents + review + outputs = ~25 max
    result = intelli_ai_workflow.invoke(
        initial_state,
        config={"recursion_limit": 50},
    )

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
    return {"status": "ok", "service": "IntelliAI", "version": "3.0.0"}


@app.post("/api/upload")
async def upload_single_file(file: UploadFile = File(...)):
    """
    Upload a single MQ Raw Data file (CSV or Excel).
    csv_ingest auto-detects the format and transforms into 4 logical tables.
    """
    session_id = str(uuid.uuid4())[:8]
    session_dir = UPLOAD_DIR / session_id
    session_dir.mkdir(exist_ok=True)

    # Preserve original extension so csv_ingest can detect format
    original_name = file.filename or "raw_data.csv"
    ext = Path(original_name).suffix.lower() or ".csv"
    dest = session_dir / f"raw_data{ext}"
    with open(dest, "wb") as f:
        shutil.copyfileobj(file.file, f)

    csv_paths = {"raw_file": str(dest)}

    try:
        result = _run_pipeline(session_id, csv_paths)
    except Exception as e:
        logger.exception("Pipeline error")
        raise HTTPException(status_code=500, detail=str(e))

    # Debug: log which state keys have non-None values
    state_keys = [k for k in result.keys() if result.get(k) is not None]
    logger.info(f"Upload pipeline done. Non-None state keys: {state_keys}")

    sessions[session_id] = result
    response = _build_response(session_id, result)
    responses[session_id] = response
    return JSONResponse(content=response)


@app.post("/api/demo")
def run_demo():
    """Run pipeline on the bundled demo CSV."""
    session_id = "DEMO"

    demo_csv = Path("data/MQ_Raw_Data.csv")
    if not demo_csv.exists():
        raise HTTPException(
            status_code=404,
            detail="No demo data found. Place MQ_Raw_Data.csv in data/ or upload via the UI."
        )

    csv_paths = {"raw_file": str(demo_csv)}

    try:
        result = _run_pipeline(session_id, csv_paths)
    except Exception as e:
        logger.exception("Demo pipeline error")
        raise HTTPException(status_code=500, detail=str(e))

    sessions[session_id] = result
    response = _build_response(session_id, result)
    responses[session_id] = response
    return JSONResponse(content=response)


@app.get("/api/review/{session_id}")
def get_pending_review(session_id: str):
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
        "as_is_subgraphs":      result.get("as_is_subgraphs", []),
        "target_subgraphs":     result.get("target_subgraphs", []),
        "as_is_centrality":     result.get("as_is_centrality", {}),
        "target_centrality":    result.get("target_centrality", {}),
        "as_is_entropy":        result.get("as_is_entropy", {}),
        "target_entropy":       result.get("target_entropy", {}),
        "as_is_communities":    result.get("as_is_communities", {}),
        "target_communities":   result.get("target_communities", {}),
        "topology_diff":        result.get("topology_diff", {}),
    })


@app.post("/api/review/{session_id}")
def submit_review(session_id: str, decision: ReviewDecision):
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    result = sessions[session_id]

    if not result.get("awaiting_human_review"):
        raise HTTPException(status_code=400, detail="Session is not awaiting human review")

    if not decision.approved and not decision.abort and not decision.feedback:
        raise HTTPException(status_code=400, detail="Revision requires a feedback reason")

    result["human_approved"]        = decision.approved
    result["human_feedback"]        = decision.feedback or ""
    result["human_aborted"]         = decision.abort
    result["awaiting_human_review"] = False
    result["chat_history"]          = decision.chat_history  # Full chat for LLM revision context

    # Debug: what's in the stored state?
    state_keys = [k for k in result.keys() if result.get(k) is not None]
    logger.info(f"Review submit. Decision: approved={decision.approved}, abort={decision.abort}")
    logger.info(f"Stored state keys: {state_keys}")
    logger.info(f"Has optimised_graph: {'optimised_graph' in result and result['optimised_graph'] is not None}")
    logger.info(f"Has target_graph: {'target_graph' in result and result['target_graph'] is not None}")
    logger.info(f"Has as_is_graph: {'as_is_graph' in result and result['as_is_graph'] is not None}")

    try:
        if decision.abort:
            updates = doc_expert_agent(result)
            result.update(updates)
        elif decision.approved:
            updates = provisioner_agent(result)
            result.update(updates)
            updates = migration_planner_agent(result)
            result.update(updates)
            updates = doc_expert_agent(result)
            result.update(updates)
        else:
            # Revise: run architect → optimizer → tester → human_review via LangGraph
            # Skips supervisor/sanitiser/researcher/analyst (data unchanged)
            # Reset redesign_count so tester retries work on this revision cycle
            result["redesign_count"] = 0
            logger.info(f"Revise: reset redesign_count=0, feedback='{(decision.feedback or '')[:80]}'")
            result = intelli_ai_revise_workflow.invoke(
                result,
                config={"recursion_limit": 50},
            )
    except Exception as e:
        logger.exception("Pipeline resume error")
        raise HTTPException(status_code=500, detail=str(e))

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

CHAT_SYSTEM_PROMPT = """You are the IntelliAI Architect AI. You designed the proposed target state for an IBM MQ topology.
The human reviewer is asking you questions about your design decisions before approving or requesting changes.

Answer concisely and specifically. Reference actual QM names, app IDs, and channel names from the topology.
If the reviewer suggests changes, acknowledge them and explain what the impact would be.
Keep responses under 150 words. Be direct and technical."""


@app.post("/api/chat/{session_id}")
def chat_with_architect(session_id: str, req: ChatRequest):
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    result = sessions[session_id]

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

    llm_messages = []
    for msg in req.history:
        llm_messages.append({"role": msg.role, "content": msg.content})
    llm_messages.append({"role": "user", "content": req.message})

    reply = call_llm_chat(
        system_prompt=system,
        messages=llm_messages,
        max_tokens=512,
        temperature=0.3,
    )

    if not reply:
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
                f"({_calc_reduction(result).get('reduction_pct', '?')}% reduction). "
                f"Decisions: {adr_text or 'none'}. "
                f"You can ask about specific QMs, channels, scores, or tell me what to change."
            )

    return JSONResponse(content={"reply": reply})
