"""
workflow.py
LangGraph StateGraph — 10 agents with human review gate.

Two-phase execution:

  Phase 1 (auto):
    supervisor → sanitiser → researcher → analyst →
    architect → optimizer → tester
      - If tester FAILS and retries remaining  → back to architect (auto retry)
      - If tester FAILS and retries exhausted  → doc_expert (report failure)
      - If tester PASSES → human_review_gate (pause for human approval)

  Phase 2 (human-triggered via POST /api/review):
    human_review_gate
      - APPROVE → provisioner → migration_planner → doc_expert → END
      - REJECT  → architect (feedback injected, LLM can reason about it)
                → optimizer → tester → human_review_gate
"""
from langgraph.graph import StateGraph, END
from backend.orchestration.state import MQTitanState
from backend.agents.agents import (
    supervisor_agent,
    sanitiser_agent,
    researcher_agent,
    analyst_agent,
    architect_agent,
    optimizer_agent,
    tester_agent,
    provisioner_agent,
    migration_planner_agent,
    doc_expert_agent,
)


# ─────────────────────────────────────────────────────────────────────────────
# HUMAN REVIEW GATE
# Pauses the pipeline. Sets awaiting_human_review = True.
# Frontend detects this and shows the review panel.
# Pipeline resumes when human POSTs to /api/review/{session_id}
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

    return {
        "awaiting_human_review": True,
        "human_approved":        None,
        "messages":              messages,
    }


# ─────────────────────────────────────────────────────────────────────────────
# ROUTING FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────
def route_after_tester(state: MQTitanState) -> str:
    """
    After Tester:
    - Violations + retries remaining  → architect  (auto retry)
    - Violations + retries exhausted  → human_review (let human guide LLM)
    - All passed                      → human_review (approval gate)
    """
    if not state.get("validation_passed"):
        if state.get("redesign_count", 0) >= 3:
            # Retries exhausted. With LLM, human can now provide guidance.
            return "human_review"
        return "architect"
    return "human_review"


def route_after_human_review(state: MQTitanState) -> str:
    """
    After human decision:
    - Approved  → provisioner (continue to output chain)
    - Aborted   → doc_expert  (generate cancellation report, skip provisioning)
    - Rejected  → architect   (feedback injected — LLM will reason about it)
    - Pending   → doc_expert  (fallback, should not happen)
    """
    if state.get("human_aborted"):
        return "doc_expert"
    approved = state.get("human_approved")
    if approved is True:
        return "provisioner"
    if approved is False:
        return "architect"
    return "doc_expert"


# ─────────────────────────────────────────────────────────────────────────────
# GRAPH CONSTRUCTION
# ─────────────────────────────────────────────────────────────────────────────
def build_workflow() -> StateGraph:
    workflow = StateGraph(MQTitanState)

    workflow.add_node("supervisor",          supervisor_agent)
    workflow.add_node("sanitiser",           sanitiser_agent)
    workflow.add_node("researcher",          researcher_agent)
    workflow.add_node("analyst",             analyst_agent)
    workflow.add_node("architect",           architect_agent)
    workflow.add_node("optimizer",           optimizer_agent)
    workflow.add_node("tester",              tester_agent)
    workflow.add_node("human_review",        human_review_gate)
    workflow.add_node("provisioner",         provisioner_agent)
    workflow.add_node("migration_planner",   migration_planner_agent)
    workflow.add_node("doc_expert",          doc_expert_agent)

    # Linear phase 1
    workflow.set_entry_point("supervisor")
    workflow.add_edge("supervisor",  "sanitiser")
    workflow.add_edge("sanitiser",   "researcher")
    workflow.add_edge("researcher",  "analyst")
    workflow.add_edge("analyst",     "architect")
    workflow.add_edge("architect",   "optimizer")
    workflow.add_edge("optimizer",   "tester")

    # Tester decision
    workflow.add_conditional_edges(
        "tester",
        route_after_tester,
        {
            "architect":    "architect",    # auto retry
            "human_review": "human_review", # pass → approval gate (or retries exhausted)
        }
    )

    # Human review decision (phase 2)
    workflow.add_conditional_edges(
        "human_review",
        route_after_human_review,
        {
            "provisioner": "provisioner",
            "architect":   "architect",
            "doc_expert":  "doc_expert",
        }
    )

    # Output chain: provisioner → migration_planner → doc_expert → END
    workflow.add_edge("provisioner",       "migration_planner")
    workflow.add_edge("migration_planner", "doc_expert")
    workflow.add_edge("doc_expert",        END)

    return workflow.compile()


mq_titan_workflow = build_workflow()
