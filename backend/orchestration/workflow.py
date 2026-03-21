"""
workflow.py
LangGraph StateGraph — 9 agents with human review gate.

Two-phase execution:

  Phase 1 (auto):
    supervisor → sanitiser → researcher → analyst →
    architect → optimizer → tester
      - If tester FAILS and retries remain → back to architect (auto retry)
      - If tester FAILS and retries exhausted → doc_expert (report failure,
        note that LLM integration on Day 3 will enable human-guided recovery)
      - If tester PASSES → human_review_gate (pause for human approval)

  Phase 2 (human-triggered via POST /api/review):
    human_review_gate
      - APPROVE → provisioner → doc_expert → END
      - REJECT  → architect (feedback injected, only meaningful after Day 3 LLM)
                → optimizer → tester → human_review_gate

NOTE ON 3-RETRY EXHAUSTION:
  When the Tester fails 3 times, the deterministic Architect cannot act on
  human feedback — it has no LLM to reason about it. So we route to doc_expert
  with a clear failure report rather than pretending the feedback loop works.
  After Day 3 (LLM plugged into Architect), this route changes to human_review
  so the human can provide guidance the LLM Architect can act on.
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

    msg = (
        f"HUMAN REVIEW REQUIRED — "
        f"Complexity: {before} → {after} ({pct}% reduction). "
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
    - Violations + retries exhausted  → doc_expert (honest failure report)
    - All passed                      → human_review (approval gate)
    """
    if not state.get("validation_passed"):
        if state.get("redesign_count", 0) >= 3:
            # Retries exhausted. Deterministic Architect cannot use human feedback.
            # Route to doc_expert with failure report.
            # TODO Day 3: change this to "human_review" after LLM is plugged in.
            return "doc_expert"
        return "architect"
    return "human_review"


def route_after_human_review(state: MQTitanState) -> str:
    """
    After human decision:
    - Approved  → provisioner
    - Rejected  → architect (feedback injected — only useful after Day 3 LLM)
    - Pending   → doc_expert (fallback, should not happen)
    """
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

    workflow.add_node("supervisor",   supervisor_agent)
    workflow.add_node("sanitiser",    sanitiser_agent)
    workflow.add_node("researcher",   researcher_agent)
    workflow.add_node("analyst",      analyst_agent)
    workflow.add_node("architect",    architect_agent)
    workflow.add_node("optimizer",    optimizer_agent)
    workflow.add_node("tester",       tester_agent)
    workflow.add_node("human_review", human_review_gate)
    workflow.add_node("provisioner",  provisioner_agent)
    workflow.add_node("doc_expert",   doc_expert_agent)

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
            "human_review": "human_review", # pass → approval gate
            "doc_expert":   "doc_expert",   # retries exhausted → failure report
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

    # Output chain
    workflow.add_edge("provisioner", "doc_expert")
    workflow.add_edge("doc_expert",  END)

    return workflow.compile()


mq_titan_workflow = build_workflow()
