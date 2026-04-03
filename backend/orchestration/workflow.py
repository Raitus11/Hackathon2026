"""
workflow.py
LangGraph StateGraph — 10 agents with human review gate.

PHASE 1 (initial upload — POST /api/upload):
  supervisor → sanitiser → researcher → analyst →
  architect → optimizer → tester → [retry loop if needed] → human_review → END

  The pipeline STOPS at human_review. The state (including optimised_graph,
  as_is_metrics, etc.) is stored in sessions[]. Frontend shows review panel.

PHASE 2 (human decision — POST /api/review):
  main.py injects the decision into the stored state and:
    - APPROVE: calls provisioner → migration_planner → doc_expert directly
    - REVISE:  re-invokes this workflow via revise_workflow (architect → optimizer → tester → human_review → END)
    - ABORT:   calls doc_expert directly

The REVISE workflow is a separate compiled graph that starts at architect
(skipping supervisor/sanitiser/researcher/analyst since data hasn't changed).
"""
import logging
from langgraph.graph import StateGraph, END
from backend.orchestration.state import IntelliAIState
from backend.agents.agents import (
    supervisor_agent,
    sanitiser_agent,
    researcher_agent,
    analyst_agent,
    architect_agent,
    optimizer_agent,
    tester_agent,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# HUMAN REVIEW GATE
# ─────────────────────────────────────────────────────────────────────────────
def human_review_gate(state: dict) -> dict:
    messages = state.get("messages", [])
    asis   = state.get("as_is_metrics", {}) or {}
    target = state.get("target_metrics", {}) or {}

    before = asis.get("total_score", 0)
    after  = target.get("total_score", 0)
    pct    = round(((before - after) / before) * 100, 1) if before else 0

    method = state.get("architect_method", "rules_fallback")
    msg = (
        f"HUMAN REVIEW REQUIRED — "
        f"Complexity: {before} → {after} ({pct}% reduction). "
        f"Architect method: {method}. "
        f"{len(state.get('adrs', []))} ADRs written. "
        f"Approve to provision or reject with reason."
    )
    messages.append({"agent": "HUMAN_REVIEW", "msg": msg})
    logger.info("HUMAN_REVIEW_GATE: Pipeline pausing for human decision.")

    return {
        "awaiting_human_review": True,
        "human_approved":        None,
        "messages":              messages,
    }


# ─────────────────────────────────────────────────────────────────────────────
# ROUTING: after tester
# ─────────────────────────────────────────────────────────────────────────────
def route_after_tester(state: IntelliAIState) -> str:
    passed = state.get("validation_passed")
    count = state.get("redesign_count", 0)
    logger.info(f"ROUTE: after tester — passed={passed}, redesign_count={count}")

    if not passed:
        if count >= 3:
            logger.info("ROUTE: → human_review (retries exhausted)")
            return "human_review"
        logger.info("ROUTE: → architect (retry)")
        return "architect"
    logger.info("ROUTE: → human_review (passed)")
    return "human_review"


# ─────────────────────────────────────────────────────────────────────────────
# FULL PIPELINE (Phase 1 — initial upload)
# supervisor → sanitiser → researcher → analyst → architect → optimizer → tester
#   ↳ retry loop: tester → architect (up to 3 times)
#   ↳ human_review → END
# ─────────────────────────────────────────────────────────────────────────────
def build_workflow() -> StateGraph:
    wf = StateGraph(IntelliAIState)

    wf.add_node("supervisor",    supervisor_agent)
    wf.add_node("sanitiser",     sanitiser_agent)
    wf.add_node("researcher",    researcher_agent)
    wf.add_node("analyst",       analyst_agent)
    wf.add_node("architect",     architect_agent)
    wf.add_node("optimizer",     optimizer_agent)
    wf.add_node("tester",        tester_agent)
    wf.add_node("human_review",  human_review_gate)

    wf.set_entry_point("supervisor")
    wf.add_edge("supervisor",  "sanitiser")
    wf.add_edge("sanitiser",   "researcher")
    wf.add_edge("researcher",  "analyst")
    wf.add_edge("analyst",     "architect")
    wf.add_edge("architect",   "optimizer")
    wf.add_edge("optimizer",   "tester")

    wf.add_conditional_edges("tester", route_after_tester, {
        "architect":    "architect",
        "human_review": "human_review",
    })

    wf.add_edge("human_review", END)

    return wf.compile()


# ─────────────────────────────────────────────────────────────────────────────
# REVISE PIPELINE (Phase 2 — human clicked Revise)
# architect → optimizer → tester → retry loop → human_review → END
# Skips supervisor/sanitiser/researcher/analyst since data is unchanged.
# ─────────────────────────────────────────────────────────────────────────────
def build_revise_workflow() -> StateGraph:
    wf = StateGraph(IntelliAIState)

    wf.add_node("architect",     architect_agent)
    wf.add_node("optimizer",     optimizer_agent)
    wf.add_node("tester",        tester_agent)
    wf.add_node("human_review",  human_review_gate)

    wf.set_entry_point("architect")
    wf.add_edge("architect", "optimizer")
    wf.add_edge("optimizer", "tester")

    wf.add_conditional_edges("tester", route_after_tester, {
        "architect":    "architect",
        "human_review": "human_review",
    })

    wf.add_edge("human_review", END)

    return wf.compile()


# Compile both workflows at import time
# Exported names match main.py imports
intelli_ai_workflow = build_workflow()
intelli_ai_revise_workflow = build_revise_workflow()


