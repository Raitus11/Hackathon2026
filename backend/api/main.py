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
import re
import uuid
import shutil
import logging
from pathlib import Path
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


# ── Live Agent Progress Streaming (A3) ──────────────────────────────────────
# Background pipelines run in worker threads; the frontend polls /progress
# every ~500ms while the pipeline runs. To capture progress without
# modifying every agent, we install a custom logging handler that mirrors
# meaningful log records (from agents + solver) into the global progress
# dict, indexed by session_id via a ContextVar set at pipeline launch.

import time
import threading
from contextvars import ContextVar
from datetime import datetime, timezone

# session_id → {
#   "status": "running" | "done" | "failed",
#   "events": [ {agent, message, timestamp_ms, sequence} ... ],
#   "started_at_ms": <int>,
#   "ended_at_ms": <int|None>,
#   "error": <str|None>,
# }
progress: dict = {}

# ContextVar lets logger handler know which session generated this log record
# (set by _run_pipeline_async before invoking the workflow).
_current_session_id: ContextVar[Optional[str]] = ContextVar("_current_session_id", default=None)
_progress_lock = threading.Lock()
_event_sequence = [0]  # mutable counter for monotonic event sequencing


# Loggers we mirror to the progress feed. Other loggers (httpx, groq, etc.)
# are noise to the user — they care about agent activity.
_PROGRESS_LOGGERS = (
    "backend.agents.agents",
    "backend.solver.optimizer_hook",
    "backend.solver.steiner_adapters",
    "backend.solver.required_pairs",
    "backend.orchestration.workflow",
)

# Agent-name extraction patterns: most agent log lines start with
# "AGENT_NAME: ..." or "AGENT-LLM: ..." — we parse this to enrich events.
_AGENT_NAME_RE = re.compile(r"^([A-Z][A-Z0-9_-]+(?:-LLM)?):\s*(.*)$", re.DOTALL)


class ProgressLogHandler(logging.Handler):
    """Capture agent + solver log records into the per-session progress dict.

    Activates only when _current_session_id ContextVar is set (i.e., we're
    inside a pipeline run). Filters by logger name to keep noise out.
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            session_id = _current_session_id.get()
            if session_id is None or session_id not in progress:
                return
            if not any(record.name == n or record.name.startswith(n + ".")
                       for n in _PROGRESS_LOGGERS):
                return
            msg = record.getMessage()

            # Try to extract agent name and content from the log message.
            # Falls back to using the logger name if pattern doesn't match.
            agent = None
            content = msg
            m = _AGENT_NAME_RE.match(msg.strip())
            if m:
                agent = m.group(1)
                content = m.group(2).strip()
            else:
                # Default agent label by logger name
                if "optimizer_hook" in record.name:
                    agent = "OPTIMIZER-SOLVER"
                elif "steiner_adapters" in record.name:
                    agent = "OPTIMIZER-SOLVER"
                elif "required_pairs" in record.name:
                    agent = "OPTIMIZER-SOLVER"
                elif "workflow" in record.name:
                    agent = "WORKFLOW"
                else:
                    agent = "PIPELINE"

            with _progress_lock:
                _event_sequence[0] += 1
                event = {
                    "sequence": _event_sequence[0],
                    "session_id": session_id,
                    "agent": agent,
                    "message": content[:500],  # trim very long messages
                    "level": record.levelname,
                    "timestamp_ms": int(record.created * 1000),
                    "elapsed_ms": int(record.created * 1000) - progress[session_id]["started_at_ms"],
                }
                progress[session_id]["events"].append(event)
                # Keep events bounded; demos rarely produce >1000 events
                if len(progress[session_id]["events"]) > 2000:
                    progress[session_id]["events"] = progress[session_id]["events"][-1000:]
        except Exception:
            # Logging handlers must NEVER raise — they'd break the entire
            # logging system. Swallow and continue.
            pass


# Install the handler once at import time.
_progress_handler = ProgressLogHandler()
_progress_handler.setLevel(logging.INFO)
logging.getLogger().addHandler(_progress_handler)
# Make sure relevant loggers actually pass through INFO
for _name in _PROGRESS_LOGGERS:
    logging.getLogger(_name).setLevel(logging.INFO)


def _start_progress(session_id: str) -> None:
    """Initialize a progress entry for a new pipeline run."""
    with _progress_lock:
        progress[session_id] = {
            "status": "running",
            "events": [],
            "started_at_ms": int(time.time() * 1000),
            "ended_at_ms": None,
            "error": None,
        }


def _finish_progress(session_id: str, error: Optional[str] = None) -> None:
    """Mark a pipeline run as completed (or failed)."""
    with _progress_lock:
        if session_id not in progress:
            return
        progress[session_id]["status"] = "failed" if error else "done"
        progress[session_id]["ended_at_ms"] = int(time.time() * 1000)
        progress[session_id]["error"] = error


def _run_pipeline_async(session_id: str, csv_paths: dict) -> None:
    """Run the pipeline in a background worker thread. Captures progress
    via the logging handler; results land in `sessions[session_id]` and
    `responses[session_id]` when complete.
    """
    token = _current_session_id.set(session_id)
    try:
        result = _run_pipeline(session_id, csv_paths)
        sessions[session_id] = result
        responses[session_id] = _build_response(session_id, result)
        _finish_progress(session_id)
    except HTTPException as e:
        logger.exception(f"Pipeline error for session {session_id}")
        _finish_progress(session_id, error=str(e.detail))
    except Exception as e:
        logger.exception(f"Pipeline error for session {session_id}")
        _finish_progress(session_id, error=str(e))
    finally:
        _current_session_id.reset(token)


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
        # New telemetry surfaced for Solver and Compliance UI tabs.
        # solver_run: full telemetry from Steiner/CP-SAT (channel cascade, gap, citations).
        # compliance_audit: LLM auditor output (score, findings, HA/security assessments).
        "solver_run":            result.get("solver_run"),
        "compliance_audit":      result.get("compliance_audit"),
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
    Upload a single MQ Raw Data file (CSV or Excel) and start the pipeline
    asynchronously. Returns the session_id immediately so the frontend can
    poll /api/session/{session_id}/progress while the pipeline runs.
    Once the progress endpoint reports status="done", the full result is
    available at /api/session/{session_id}.
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

    # Initialize progress tracking and launch the pipeline in a worker thread.
    # We use a daemon thread (not FastAPI's BackgroundTasks because that runs
    # AFTER the response is sent — we need the work to start NOW so progress
    # events begin immediately). Daemon=True ensures the thread doesn't block
    # uvicorn shutdown.
    _start_progress(session_id)
    worker = threading.Thread(
        target=_run_pipeline_async,
        args=(session_id, csv_paths),
        name=f"pipeline-{session_id}",
        daemon=True,
    )
    worker.start()

    return JSONResponse(content={
        "session_id": session_id,
        "status": "running",
        "progress_url": f"/api/session/{session_id}/progress",
        "result_url": f"/api/session/{session_id}",
    })


@app.get("/api/session/{session_id}/progress")
def get_session_progress(session_id: str, since_seq: int = 0):
    """Return progress events for a running pipeline. Frontend polls this
    every ~500ms while status="running".

    `since_seq` lets clients request only events newer than the last one
    they saw — reduces payload size on long polls.

    Response shape:
      {
        "session_id": ...,
        "status": "running" | "done" | "failed",
        "events": [ {sequence, agent, message, timestamp_ms, elapsed_ms, level} ... ],
        "elapsed_ms": <total time elapsed, even if still running>,
        "error": <str|None, present only if status="failed">,
        "result_ready": <bool, true if responses[session_id] is populated>,
      }
    """
    if session_id not in progress:
        raise HTTPException(status_code=404, detail="Session not found")

    p = progress[session_id]
    with _progress_lock:
        events = [e for e in p["events"] if e["sequence"] > since_seq]
        elapsed = (p["ended_at_ms"] or int(time.time() * 1000)) - p["started_at_ms"]
        return {
            "session_id": session_id,
            "status": p["status"],
            "events": events,
            "event_count": len(p["events"]),
            "elapsed_ms": elapsed,
            "error": p["error"],
            "result_ready": session_id in responses,
        }


@app.post("/api/demo")
def run_demo():
    """Run pipeline on the bundled demo CSV. Same async pattern as /api/upload."""
    session_id = "DEMO"

    demo_csv = Path("data/MQ_Raw_Data.csv")
    if not demo_csv.exists():
        raise HTTPException(
            status_code=404,
            detail="No demo data found. Place MQ_Raw_Data.csv in data/ or upload via the UI."
        )

    csv_paths = {"raw_file": str(demo_csv)}

    # Clear any prior demo run from results so the frontend doesn't see stale data
    sessions.pop(session_id, None)
    responses.pop(session_id, None)

    _start_progress(session_id)
    worker = threading.Thread(
        target=_run_pipeline_async,
        args=(session_id, csv_paths),
        name=f"pipeline-{session_id}",
        daemon=True,
    )
    worker.start()

    return JSONResponse(content={
        "session_id": session_id,
        "status": "running",
        "progress_url": f"/api/session/{session_id}/progress",
        "result_url": f"/api/session/{session_id}",
    })


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
        # Solver and Compliance telemetry — required by Solver and Compliance UI tabs.
        "solver_run":           result.get("solver_run"),
        "compliance_audit":     result.get("compliance_audit"),
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


# ── Evidence Bundle download ────────────────────────────────────────────────
# Production-grade forensic artifact: a single zip containing every piece of
# evidence about a pipeline run. The kind of thing a senior architect or
# auditor expects to receive after a production change. Each file is a real
# artifact derived from session state, not a marketing document.
#
# Bundle contents (manifest.json lists exact contents):
#   manifest.json              — what's in the bundle, generated_at, session_id
#   solver_run.json            — full Steiner/CP-SAT telemetry (channels, gap, citations)
#   compliance_findings.json   — LLM auditor output (score, findings, HA, security)
#   target_topology.json       — full target graph (QMs, channels, queues, apps)
#   as_is_metrics.json         — complexity score breakdown for the input
#   target_metrics.json        — complexity score breakdown for the target
#   architecture_decisions.md  — ADRs in IETF/AWS Well-Architected format
#   audit_log.txt              — agent message trace, in-order
#   constraint_violations.json — engineering rule check results
#   mqsc_commands.txt          — concatenated provisioner output (if generated)
#
# All content is sanitised via mq_graph.sanitise() before zipping. No env
# vars, secrets, or credentials are ever in session state, but the sanitiser
# is the safety net.

@app.get("/api/session/{session_id}/evidence")
def download_evidence_bundle(session_id: str):
    """Generate and stream a forensic evidence zip for the given pipeline session."""
    import io
    import json
    import zipfile
    from datetime import datetime, timezone
    from fastapi.responses import StreamingResponse

    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    state = sessions[session_id]

    # Build manifest first — describes the bundle without depending on contents
    manifest = {
        "bundle_format_version": "1.0",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "session_id": session_id,
        "pipeline": {
            "validation_passed": state.get("validation_passed"),
            "redesign_count": state.get("redesign_count", 0),
            "architect_method": state.get("architect_method"),
            "human_approved": state.get("human_approved"),
        },
        "summary": {
            "as_is_score": (state.get("as_is_metrics") or {}).get("total_score"),
            "target_score": (state.get("target_metrics") or {}).get("total_score"),
            "channels_asis": (state.get("solver_run") or {}).get("asis_channel_count"),
            "channels_target": (state.get("solver_run") or {}).get("actual_channel_count"),
            "compliance_score": (state.get("compliance_audit") or {}).get("compliance_score"),
            "v009_pass": not any(
                v.get("rule") == "REQUIRED_PAIR_REACHABILITY"
                for v in (state.get("constraint_violations") or [])
            ),
        },
        "files": [
            "manifest.json",
            "solver_run.json",
            "compliance_findings.json",
            "target_topology.json",
            "as_is_metrics.json",
            "target_metrics.json",
            "architecture_decisions.md",
            "audit_log.txt",
            "constraint_violations.json",
            "mqsc_commands.txt",
        ],
        "provenance": {
            "generator": "IntelliAI Phase 1 — MQ-TITAN",
            "solver": "directed Steiner network (Charikar et al. 1999, J.Algorithms 33:73-91)",
            "compliance_auditor": "llama-3.3-70b-versatile via Groq",
            "validation_invariants": [
                "V-009 REQUIRED_PAIR_REACHABILITY (per-pair directed BFS)",
                "ONE_QM_PER_APP", "ONE_APP_PER_QM",
                "SENDER_RECEIVER_PAIR", "CHANNEL_NAMING",
                "XMITQ_EXISTS", "NO_ORPHAN_QMS",
                "CONSUMER_QUEUE_EXISTS", "PATH_COMPLETENESS",
                "ISOLATED_QM",
            ],
        },
    }

    # Build target_topology from optimised_graph
    target_topology = {}
    if state.get("optimised_graph") is not None:
        try:
            target_topology = graph_to_dict(state["optimised_graph"])
        except Exception as e:
            target_topology = {"error": f"Failed to serialize optimised_graph: {e}"}

    # Build ADRs as Markdown in IETF/AWS Well-Architected format
    adr_md_lines = ["# Architecture Decision Records", "",
                    f"_Generated by IntelliAI on {manifest['generated_at_utc']}_",
                    f"_Session: `{session_id}`_", ""]
    for i, adr in enumerate(state.get("adrs") or [], 1):
        adr_md_lines.append(f"## ADR-{i:03d}: {adr.get('decision') or adr.get('title') or 'Untitled'}")
        adr_md_lines.append("")
        adr_md_lines.append(f"**Status:** Proposed")
        adr_md_lines.append(f"**Date:** {manifest['generated_at_utc']}")
        adr_md_lines.append("")
        if adr.get("context"):
            adr_md_lines.append("### Context")
            adr_md_lines.append(adr["context"])
            adr_md_lines.append("")
        if adr.get("rationale"):
            adr_md_lines.append("### Decision")
            adr_md_lines.append(adr["rationale"])
            adr_md_lines.append("")
        if adr.get("consequences"):
            adr_md_lines.append("### Consequences")
            adr_md_lines.append(adr["consequences"])
            adr_md_lines.append("")
        adr_md_lines.append("---")
        adr_md_lines.append("")
    architecture_decisions_md = "\n".join(adr_md_lines) if state.get("adrs") else (
        "# Architecture Decision Records\n\n_No ADRs were generated in this run._\n"
    )

    # Build audit_log.txt from messages
    audit_lines = [
        f"# Audit Log — IntelliAI Pipeline Session {session_id}",
        f"# Generated at: {manifest['generated_at_utc']}",
        f"# Validation: {'PASS' if state.get('validation_passed') else 'FAIL'}",
        f"# Redesign iterations: {state.get('redesign_count', 0)}",
        "#" + "=" * 78,
        "",
    ]
    for i, m in enumerate(state.get("messages") or [], 1):
        if isinstance(m, dict):
            agent = m.get("agent", "PIPELINE")
            msg = m.get("msg") or m.get("message") or json.dumps(m)
        else:
            agent = "PIPELINE"
            msg = str(m)
        audit_lines.append(f"[{i:04d}] {agent:>14} | {msg}")
    audit_log_txt = "\n".join(audit_lines)

    # Build mqsc_commands.txt from mqsc_scripts
    mqsc_scripts = state.get("mqsc_scripts") or []
    if mqsc_scripts:
        mqsc_chunks = [
            f"* IntelliAI Combined MQSC — Session {session_id}",
            f"* Generated at: {manifest['generated_at_utc']}",
            f"* Total scripts: {len(mqsc_scripts)}",
            "*" + "=" * 78,
            "",
        ]
        for i, script in enumerate(mqsc_scripts, 1):
            if isinstance(script, dict):
                qm = script.get("qm_name") or script.get("qm") or f"script_{i}"
                content = script.get("content") or script.get("mqsc") or json.dumps(script)
            else:
                qm = f"script_{i}"
                content = str(script)
            mqsc_chunks.append(f"* --- Script {i}: {qm} ---")
            mqsc_chunks.append(content)
            mqsc_chunks.append("")
        mqsc_commands_txt = "\n".join(mqsc_chunks)
    else:
        mqsc_commands_txt = "* No MQSC scripts were generated in this session.\n"

    # Build all-content dict, sanitise once
    raw_files = {
        "manifest.json":              json.dumps(manifest, indent=2, default=str),
        "solver_run.json":            json.dumps(sanitise(state.get("solver_run") or {}), indent=2, default=str),
        "compliance_findings.json":   json.dumps(sanitise(state.get("compliance_audit") or {}), indent=2, default=str),
        "target_topology.json":       json.dumps(sanitise(target_topology), indent=2, default=str),
        "as_is_metrics.json":         json.dumps(sanitise(state.get("as_is_metrics") or {}), indent=2, default=str),
        "target_metrics.json":        json.dumps(sanitise(state.get("target_metrics") or {}), indent=2, default=str),
        "architecture_decisions.md":  architecture_decisions_md,
        "audit_log.txt":              audit_log_txt,
        "constraint_violations.json": json.dumps(sanitise(state.get("constraint_violations") or []), indent=2, default=str),
        "mqsc_commands.txt":          mqsc_commands_txt,
    }

    # Write zip in memory
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for name, content in raw_files.items():
            zf.writestr(name, content)
    buf.seek(0)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"intelliai_evidence_{session_id[:8]}_{timestamp}.zip"

    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
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
