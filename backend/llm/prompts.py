"""
prompts.py
Architect agent prompt templates and graph-to-text serialisation.
Feeds topology data to Groq LLM for AI-driven architecture decisions.
"""
import pandas as pd
from typing import Optional

# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT — IBM MQ domain expert persona
# ─────────────────────────────────────────────────────────────────────────────
ARCHITECT_SYSTEM_PROMPT = """You are a senior IBM MQ infrastructure architect with 20 years of experience designing enterprise messaging topologies. You are analysing an existing IBM MQ deployment and designing an optimised target state.

## YOUR CONSTRAINTS — EVERY DECISION MUST SATISFY ALL OF THESE:

1. ONE QM PER APP: Every application ID connects to exactly one queue manager. No exceptions.
2. REMOTE QUEUE PATTERN: Data producers write to a REMOTE queue definition (QREMOTE) on their local QM. The QREMOTE points to the consumer's LOCAL queue on the target QM via an XMITQ.
3. TRANSMISSION QUEUES: Every QREMOTE uses an XMITQ to route messages to the target QM. One XMITQ per target QM.
4. CHANNEL NAMING: Sender channels are named {fromQM}.{toQM}. Receiver channels use the SAME name on the remote QM.
5. SENDER/RECEIVER PAIRS: Every channel connection has a SENDER on the source QM and a RECEIVER on the destination QM.
6. CONSUMER LOCAL QUEUES: Consumers GET from LOCAL queues on their own QM. These are fed by inbound channels.

## YOUR OBJECTIVES (in priority order):

1. Fix all constraint violations in the as-is topology
2. AGGRESSIVELY minimise the number of queue managers — remove every QM that has zero apps after consolidation. A QM with no apps is waste. Do NOT keep QMs "for future use" or "regional presence" — if no app needs it, remove it.
3. AGGRESSIVELY minimise channels — a channel is ONLY needed if a PRODUCER app on QM_A sends messages to a CONSUMER app on QM_B. If no such flow exists, the channel must be removed. Do NOT keep channels "for convenience."
4. Preserve all application message flows (no app loses ability to communicate)
5. Prefer regional affinity (keep apps on QMs in their region where possible)
6. TARGET: achieve at least 30% complexity reduction. If you keep too many QMs or channels, the score won't improve enough. Be bold.

## OUTPUT FORMAT:

Return a JSON object with exactly this structure:
{
  "design_decisions": [
    {
      "id": "DD-001",
      "type": "CONSOLIDATE_APP | REMOVE_QM | ADD_CHANNEL | REMOVE_CHANNEL | REASSIGN_APP",
      "affected_entities": ["QM_NAME", "APP_ID"],
      "description": "What you are doing",
      "rationale": "Why — referencing specific data from the topology"
    }
  ],
  "adrs": [
    {
      "id": "ADR-001",
      "title": "Short title of the decision",
      "context": "The specific problem in the as-is topology with entity names and data",
      "decision": "What change is being made",
      "rationale": "Why this approach over alternatives. Mention trade-offs.",
      "consequences": "What changes as a result. Reference affected apps, queues, channels by name."
    }
  ],
  "target_app_assignments": [
    {
      "app_id": "APP001",
      "assigned_qm": "QM_LONDON",
      "reason": "Brief reason for this assignment"
    }
  ],
  "qms_to_remove": ["QM_ORPHAN1"],
  "qms_to_keep": ["QM_LONDON", "QM_PARIS"],
  "required_connections": [
    {
      "from_qm": "QM_LONDON",
      "to_qm": "QM_PARIS",
      "serving_apps": ["APP001", "APP005"],
      "direction": "Messages flow from APP001 on QM_LONDON to APP005 on QM_PARIS"
    }
  ]
}

## RULES FOR YOUR REASONING:

- NEVER reference entities that don't exist in the provided topology data
- ALWAYS explain WHY you chose one QM over another for app consolidation
- If an app PUTs to queues on multiple QMs, consolidate to the QM with the majority of its connections
- If tie, prefer the QM with fewer existing apps (load balance)
- An orphan QM is one with zero apps AND zero active message flows after consolidation — it MUST be removed
- Only remove a QM if ALL its apps have been reassigned and ALL its message flows are served by other paths
- Every ADR must reference at least 2 specific entity names from the topology
- IMPORTANT: when multiple apps in the same region share a QM, consolidate them onto ONE QM and remove the rest. Do not keep QMs with only 1 app if another QM in the same region can host it.
- Stopped/inactive channels MUST be removed — they are dead weight
- qms_to_remove should contain EVERY QM not in qms_to_keep. Do not leave QMs unlisted.
- CRITICAL: every QM in qms_to_keep MUST appear in at least one required_connection (either as from_qm or to_qm). A QM with apps but no channels is ISOLATED and useless — either connect it or move its apps to another QM and remove it.
- If a QM in the as-is topology has CONSUMER apps but NO inbound channel feeding it, that QM is already broken. Move its apps to a QM that IS connected, and remove the broken QM."""


# ─────────────────────────────────────────────────────────────────────────────
# USER PROMPT TEMPLATE
# ─────────────────────────────────────────────────────────────────────────────
USER_PROMPT_TEMPLATE = """Analyse the following IBM MQ topology and design an optimised target state.

## AS-IS TOPOLOGY SUMMARY

Queue Managers ({num_qms} total):
{qm_summary}

Applications ({num_apps} total):
{app_summary}

Channels ({num_channels} total):
{channel_summary}

Queues ({num_queues} total):
{queue_summary}

## DETECTED VIOLATIONS

{violations_list}

## CURRENT COMPLEXITY SCORE: {as_is_score}/100

Factor breakdown:
- Channel Count (CC): {cc_score} (weight: 30%) — reduce by removing unnecessary channels
- Coupling Index (CI): {ci_score} (weight: 25%) — reduce by enforcing 1-QM-per-app
- Routing Depth (RD): {rd_score} (weight: 20%) — reduce by eliminating multi-hop paths
- Fan-Out (FO): {fo_score} (weight: 15%) — reduce by consolidating outbound channels
- Orphan Objects (OO): {oo_score} (weight: 10%) — reduce by removing orphan QMs and stopped channels

YOUR TARGET: reduce the total score by at least 30%. Be aggressive — remove every QM and channel that is not strictly required by an active application message flow.

{human_feedback_section}

Design the optimised target state. Return ONLY valid JSON matching the specified schema."""


def build_architect_prompt(state: dict) -> str:
    """
    Serialise pipeline state into the user prompt for the Architect LLM.
    Converts graph data into readable text the LLM can reason about.
    """
    raw_data = state["raw_data"]
    violations = state.get("data_quality_report", {}).get("topology_violations", {})
    metrics = state.get("as_is_metrics", {})

    # ── QM summary ────────────────────────────────────────────────────────
    qm_lines = []
    qm_list = raw_data.get("queue_managers", [])
    app_list = raw_data.get("applications", [])

    for qm in qm_list:
        qm_id = qm["qm_id"]
        apps_on_qm = [a for a in app_list if a.get("qm_id") == qm_id]
        app_ids = list(set(a["app_id"] for a in apps_on_qm))
        qm_lines.append(
            f"  - {qm.get('qm_name', qm_id)} (id: {qm_id}, "
            f"region: {qm.get('region', 'unknown')}, "
            f"apps: {len(app_ids)} — {app_ids[:5]}{'...' if len(app_ids) > 5 else ''})"
        )

    # ── App summary — show QM connections and direction ───────────────────
    app_groups = {}
    for row in app_list:
        aid = row["app_id"]
        if aid not in app_groups:
            app_groups[aid] = {
                "app_name": row.get("app_name", aid),
                "qm_ids": set(),
                "directions": set(),
                "queues": [],
            }
        app_groups[aid]["qm_ids"].add(row.get("qm_id", ""))
        app_groups[aid]["directions"].add(row.get("direction", "UNKNOWN"))
        if row.get("queue_id"):
            app_groups[aid]["queues"].append(row.get("queue_id", ""))

    app_lines = []
    for aid, info in app_groups.items():
        violation_flag = " ⚠ MULTI-QM VIOLATION" if len(info["qm_ids"]) > 1 else ""
        app_lines.append(
            f"  - {aid} ({info['app_name']}): "
            f"QMs={sorted(info['qm_ids'])}, "
            f"direction={','.join(info['directions'])}, "
            f"queues={info['queues'][:3]}{violation_flag}"
        )

    # ── Channel summary ───────────────────────────────────────────────────
    channels = raw_data.get("channels", [])
    ch_lines = []
    for ch in channels:
        ch_lines.append(
            f"  - {ch.get('channel_name', '?')}: "
            f"{ch.get('from_qm', '?')}→{ch.get('to_qm', '?')} "
            f"type={ch.get('channel_type', '?')} "
            f"status={ch.get('status', 'unknown')}"
        )

    # ── Queue summary ─────────────────────────────────────────────────────
    queues = raw_data.get("queues", [])
    q_lines = []
    for q in queues[:20]:  # Cap to avoid token overflow
        q_lines.append(
            f"  - {q.get('queue_name', '?')} on {q.get('qm_id', '?')} "
            f"type={q.get('queue_type', '?')} usage={q.get('usage', 'NORMAL')}"
        )
    if len(queues) > 20:
        q_lines.append(f"  ... and {len(queues) - 20} more queues")

    # ── Violations ────────────────────────────────────────────────────────
    v_lines = []
    if violations:
        multi_qm = violations.get("multi_qm_apps", [])
        if multi_qm:
            for v in multi_qm:
                v_lines.append(
                    f"  - MULTI_QM_APP: {v['app']} connects to {len(v['qms'])} QMs: {v['qms']}"
                )
        orphans = violations.get("orphan_qms", [])
        if orphans:
            v_lines.append(f"  - ORPHAN_QMS: {orphans}")
        stopped = violations.get("stopped_channels", [])
        if stopped:
            v_lines.append(f"  - STOPPED_CHANNELS: {stopped}")
    if not v_lines:
        v_lines = ["  None detected"]

    # ── Human feedback (for reject-and-redo loops) ────────────────────────
    feedback = state.get("human_feedback", "")
    feedback_section = ""
    if feedback:
        feedback_section = (
            f"\n## HUMAN REVIEWER FEEDBACK (address this in your redesign)\n"
            f"{feedback}\n"
        )

    return USER_PROMPT_TEMPLATE.format(
        num_qms=len(qm_list),
        qm_summary="\n".join(qm_lines) if qm_lines else "  (none)",
        num_apps=len(app_groups),
        app_summary="\n".join(app_lines) if app_lines else "  (none)",
        num_channels=len(channels),
        channel_summary="\n".join(ch_lines) if ch_lines else "  (none)",
        num_queues=len(queues),
        queue_summary="\n".join(q_lines) if q_lines else "  (none)",
        violations_list="\n".join(v_lines),
        as_is_score=metrics.get("total_score", "N/A"),
        cc_score=metrics.get("channel_count", "N/A"),
        ci_score=metrics.get("coupling_index", "N/A"),
        rd_score=metrics.get("routing_depth", "N/A"),
        fo_score=metrics.get("fan_out_score", "N/A"),
        oo_score=metrics.get("orphan_objects", "N/A"),
        human_feedback_section=feedback_section,
    )
