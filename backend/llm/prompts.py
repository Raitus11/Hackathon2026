"""
prompts.py
Architect agent prompt templates and graph-to-text serialisation.
Feeds topology data to LLM for AI-driven architecture decisions.

Designed for Tachyon (production) with Groq/Llama 3.3 fallback.
Token-optimised: summarises 13K rows into ~3K tokens of structured text.
"""
from typing import Optional
from collections import Counter

# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT — IBM MQ domain expert persona
# ─────────────────────────────────────────────────────────────────────────────
ARCHITECT_SYSTEM_PROMPT = """You are a senior IBM MQ infrastructure architect designing a target state topology from a legacy MQ environment. You must produce a VALID, CONSTRAINT-COMPLIANT design.

## HARD CONSTRAINTS — VIOLATING ANY OF THESE MAKES THE DESIGN INVALID

### C1: STRICT 1:1 APP-TO-QM OWNERSHIP
- Each application ID gets its own DEDICATED queue manager. No sharing.
- N apps = N queue managers. No exceptions.
- The app with the most connections to an existing QM keeps that QM's name.
- All other apps get a new QM named QM_{APP_ID}.
- Orphan QMs (zero apps after reassignment) are removed.

### C2: CANONICAL MESSAGE FLOW (must be followed exactly)
```
Producer App → [Server Connection] → QM_A
  QM_A has: QREMOTE(name) RQMNAME(QM_B) RNAME(LOCAL.{consumer}.IN) XMITQ({QM_B}.XMITQ)
  QM_A has: QLOCAL({QM_B}.XMITQ) USAGE(XMITQ)
  QM_A has: CHANNEL({QM_A}.{QM_B}) CHLTYPE(SDR) XMITQ({QM_B}.XMITQ)
  → network →
  QM_B has: CHANNEL({QM_A}.{QM_B}) CHLTYPE(RCVR)
  QM_B has: QLOCAL(LOCAL.{consumer}.IN)
Consumer App → [Server Connection] → QM_B → GET(LOCAL.{consumer}.IN)
```

### C3: CHANNEL RULES
- Channels do NOT exist in input data — you introduce them.
- A channel is needed ONLY when a producer app on QM_A writes to a queue consumed by an app on QM_B (and QM_A ≠ QM_B).
- Sender channel name: {FROM_QM}.{TO_QM}
- Receiver channel: same name, on the receiver QM.
- One XMITQ per target QM (shared by all REMOTE queues targeting that QM).

### C4: QUEUE RULES
- LOCAL queue: one per consumer app on its QM. Named LOCAL.{APP_ID}.IN.
- REMOTE queue: one per (producer_app, queue_name, consumer_app) flow where they are on different QMs.
- XMITQ: one per (source_QM, target_QM) pair. Named {TARGET_QM}.XMITQ.

## YOUR OBJECTIVES (priority order)

1. Enforce strict 1:1 app-to-QM ownership (C1)
2. Determine channels from actual producer→consumer queue-level flows only (C3)
3. Preserve ALL application message flows — zero broken paths
4. Remove orphan QMs that have zero apps after 1:1 reassignment
5. Respect regional affinity where possible
6. Produce Architecture Decision Records (ADRs) explaining your reasoning
7. Surface insights: SPOFs, hub QMs, orphan objects, anti-patterns in the as-is topology

## OUTPUT FORMAT — RETURN ONLY VALID JSON

{
  "target_app_assignments": [
    {
      "app_id": "A000",
      "assigned_qm": "QM007",
      "region": "Risk & Compliance",
      "reason": "Highest connection count (12) to QM007 among 20 QMs"
    }
  ],
  "new_qms": ["QM_A001", "QM_A002"],
  "removed_qms": ["QM030"],
  "channels": [
    {
      "from_qm": "QM007",
      "to_qm": "QM_A010",
      "reason": "A000 on QM007 produces to Q.ORDERS consumed by A010 on QM_A010"
    }
  ],
  "design_decisions": [
    {
      "id": "DD-001",
      "type": "ASSIGN_DEDICATED_QM | CREATE_CHANNEL | REMOVE_QM",
      "affected_entities": ["A000", "QM007"],
      "description": "What you are doing",
      "rationale": "Why — reference specific apps, queues, flow counts"
    }
  ],
  "adrs": [
    {
      "id": "ADR-001",
      "title": "Enforce 1:1 app-to-QM ownership",
      "context": "As-is has N apps across M shared QMs with avg coupling of X",
      "decision": "Create N dedicated QMs, one per app",
      "rationale": "Eliminates multi-app coupling, enables independent deployment",
      "consequences": "QM count changes from M to N. Channels derived from actual flows."
    }
  ],
  "insights": [
    {
      "type": "SPOF | HUB | ANTI_PATTERN | ORPHAN | OBSERVATION",
      "entity": "QM017",
      "detail": "QM017 hosts 112 apps — single point of failure affecting 74% of apps."
    }
  ]
}

## REASONING RULES

- Reference ACTUAL entity names from the provided topology data only.
- For 1:1 assignment: pick the QM where the app has the MOST queue connections. Ties → fewer total apps on QM → alphabetical.
- A channel exists ONLY where a specific producer app writes to a queue consumed by a specific consumer app, and they are on DIFFERENT QMs after 1:1 assignment.
- Do NOT create channels "for convenience" or "for future use."
- Every ADR must reference at least 2 specific entity names.
- Insights should surface non-obvious findings: hubs, SPOFs, regional imbalances, orphans, anti-patterns.
- removed_qms = QMs with zero apps after 1:1 reassignment.
- new_qms = QMs you create (named QM_{APP_ID}) for apps that couldn't keep their original."""


# ─────────────────────────────────────────────────────────────────────────────
# USER PROMPT TEMPLATE — token-optimised for large datasets
# ─────────────────────────────────────────────────────────────────────────────
USER_PROMPT_TEMPLATE = """Analyse this IBM MQ topology and design a 1:1 app-to-QM target state.

## AS-IS SUMMARY

{num_apps} apps across {num_qms} queue managers.
{num_queues} queues ({num_local} local, {num_remote} remote, {num_alias} alias).
{num_channels} inferred channels. {num_flows} producer→consumer flows across {num_flow_pairs} unique app pairs.

### Coupling (every app here connects to multiple QMs — this MUST be fixed)
{coupling_summary}

### QM Load Distribution (apps per QM — shows sharing that must be eliminated)
{qm_load_summary}

### Top Producers by Fan-Out
{flow_summary}

### Regional Distribution
{region_summary}

## DETECTED VIOLATIONS
{violations_list}

## COMPLEXITY SCORE: {as_is_score}/100
- Channel Count (CC): {cc_raw} channels (weighted: {cc_score}/25)
- Coupling Index (CI): {ci_raw} QMs/app (weighted: {ci_score}/25) — target is 1.0
- Routing Depth (RD): {rd_raw} hops (weighted: {rd_score}/20)
- Fan-Out (FO): {fo_raw} max outbound (weighted: {fo_score}/15)
- Orphan Objects (OO): {oo_raw} (weighted: {oo_score}/5)
- Channel Sprawl (CS): {cs_raw} ch/QM (weighted: {cs_score}/10)

{human_feedback_section}

Design the target state. Each app gets exactly ONE dedicated QM. Channels only where actual producer→consumer flows exist. Return ONLY valid JSON matching the schema."""


def build_architect_prompt(state: dict) -> str:
    """
    Serialise pipeline state into the user prompt for the Architect LLM.
    Token-optimised: summarises 13K rows into ~3K tokens of structured text.
    """
    raw_data = state["raw_data"]
    violations = state.get("data_quality_report", {}).get("topology_violations", {})
    metrics = state.get("as_is_metrics", {})
    factor_scores = metrics.get("factor_scores", {})

    qm_list = raw_data.get("queue_managers", [])
    app_list = raw_data.get("applications", [])
    queue_list = raw_data.get("queues", [])
    channel_list = raw_data.get("channels", [])

    # ── Coupling summary (apps → QM counts) ───────────────────────────────
    app_qm_map = {}
    app_qm_counts = {}
    for row in app_list:
        aid = row["app_id"]
        qm = row.get("qm_id", "")
        app_qm_map.setdefault(aid, set()).add(qm)
        key = (aid, qm)
        app_qm_counts[key] = app_qm_counts.get(key, 0) + 1

    coupling_lines = []
    multi_qm_apps = sorted(
        [(aid, qms) for aid, qms in app_qm_map.items() if len(qms) > 1],
        key=lambda x: -len(x[1])
    )
    for aid, qms in multi_qm_apps[:20]:
        qm_scores = {qm: app_qm_counts.get((aid, qm), 0) for qm in qms}
        best_qm = max(qm_scores, key=qm_scores.get)
        coupling_lines.append(
            f"  {aid}: {len(qms)} QMs, best={best_qm}({qm_scores[best_qm]}), "
            f"others=[{', '.join(sorted(qms - {best_qm})[:4])}]"
        )
    if len(multi_qm_apps) > 20:
        coupling_lines.append(f"  ... and {len(multi_qm_apps) - 20} more")
    if not coupling_lines:
        coupling_lines = ["  None — all apps already on single QMs"]

    # ── QM load distribution ──────────────────────────────────────────────
    qm_app_sets = {}
    for row in app_list:
        qm_app_sets.setdefault(row.get("qm_id", ""), set()).add(row["app_id"])

    qm_region_map = {qm["qm_id"]: qm.get("region", "?") for qm in qm_list}

    qm_load_lines = []
    for qm_id, apps in sorted(qm_app_sets.items(), key=lambda x: -len(x[1]))[:12]:
        qm_load_lines.append(
            f"  {qm_id}: {len(apps)} apps, region={qm_region_map.get(qm_id, '?')}"
        )
    if len(qm_app_sets) > 12:
        qm_load_lines.append(f"  ... and {len(qm_app_sets) - 12} more QMs")

    # ── Producer→Consumer flow summary ────────────────────────────────────
    queue_prod = {}
    queue_cons = {}
    for row in app_list:
        aid = row["app_id"]
        d = row.get("direction", "UNKNOWN").upper()
        qname = row.get("queue_name", "")
        if not qname:
            continue
        if d in ("PUT", "PRODUCER"):
            queue_prod.setdefault(qname, set()).add(aid)
        elif d in ("GET", "CONSUMER"):
            queue_cons.setdefault(qname, set()).add(aid)

    flow_pairs = set()
    total_flows = 0
    for qname in set(queue_prod.keys()) & set(queue_cons.keys()):
        for p in queue_prod[qname]:
            for c in queue_cons[qname]:
                if p != c:
                    flow_pairs.add((p, c))
                    total_flows += 1

    app_fanout = Counter()
    for p, _ in flow_pairs:
        app_fanout[p] += 1
    flow_lines = []
    for aid, count in app_fanout.most_common(10):
        flow_lines.append(f"  {aid}: → {count} consumers")
    if len(app_fanout) > 10:
        flow_lines.append(f"  ... and {len(app_fanout) - 10} more producers")
    if not flow_lines:
        flow_lines = ["  No producer→consumer flows detected"]

    # ── Regional distribution ─────────────────────────────────────────────
    region_app_count = Counter()
    for qm in qm_list:
        r = qm.get("region", "UNKNOWN")
        n_apps = len(qm_app_sets.get(qm["qm_id"], set()))
        region_app_count[r] += n_apps
    region_lines = [f"  {r}: {c} app connections" for r, c in region_app_count.most_common()]
    if not region_lines:
        region_lines = ["  No regional data"]

    # ── Queue type counts ─────────────────────────────────────────────────
    q_types = Counter(q.get("queue_type", "UNKNOWN") for q in queue_list)

    # ── Violations ────────────────────────────────────────────────────────
    v_lines = []
    if violations:
        multi_qm = violations.get("multi_qm_apps", [])
        if multi_qm:
            v_lines.append(
                f"  MULTI_QM_APPS: {len(multi_qm)} apps on multiple QMs "
                f"(worst: {multi_qm[0]['app']} on {len(multi_qm[0]['qms'])} QMs)"
            )
        orphans = violations.get("orphan_qms", [])
        if orphans:
            v_lines.append(f"  ORPHAN_QMS: {len(orphans)} — {orphans[:5]}")
        shared = violations.get("shared_qms", {})
        if shared:
            worst_shared = max(shared.items(), key=lambda x: x[1])
            v_lines.append(
                f"  SHARED_QMS: {len(shared)} QMs host multiple apps "
                f"(worst: {worst_shared[0]} with {worst_shared[1]} apps)"
            )
        stopped = violations.get("stopped_channels", [])
        if stopped:
            v_lines.append(f"  STOPPED_CHANNELS: {len(stopped)}")
    if not v_lines:
        v_lines = ["  None detected"]

    # ── Human feedback ────────────────────────────────────────────────────
    feedback = state.get("human_feedback", "")
    feedback_section = ""
    if feedback:
        feedback_section = (
            f"\n## HUMAN REVIEWER FEEDBACK (address this in your redesign)\n"
            f"{feedback}\n"
        )

    return USER_PROMPT_TEMPLATE.format(
        num_qms=len(qm_list),
        num_apps=len(app_qm_map),
        num_queues=len(queue_list),
        num_local=q_types.get("LOCAL", 0),
        num_remote=q_types.get("REMOTE", 0),
        num_alias=q_types.get("ALIAS", 0),
        num_channels=len(channel_list),
        num_flows=total_flows,
        num_flow_pairs=len(flow_pairs),
        coupling_summary="\n".join(coupling_lines),
        qm_load_summary="\n".join(qm_load_lines),
        flow_summary="\n".join(flow_lines),
        region_summary="\n".join(region_lines),
        violations_list="\n".join(v_lines),
        as_is_score=metrics.get("total_score", "N/A"),
        cc_raw=metrics.get("channel_count", "N/A"),
        cc_score=factor_scores.get("cc_weighted", "N/A"),
        ci_raw=metrics.get("coupling_index", "N/A"),
        ci_score=factor_scores.get("ci_weighted", "N/A"),
        rd_raw=metrics.get("routing_depth", "N/A"),
        rd_score=factor_scores.get("rd_weighted", "N/A"),
        fo_raw=metrics.get("fan_out_score", "N/A"),
        fo_score=factor_scores.get("fo_weighted", "N/A"),
        oo_raw=metrics.get("orphan_objects", "N/A"),
        oo_score=factor_scores.get("oo_weighted", "N/A"),
        cs_raw=metrics.get("channel_sprawl", "N/A"),
        cs_score=factor_scores.get("cs_weighted", "N/A"),
        human_feedback_section=feedback_section,
    )
