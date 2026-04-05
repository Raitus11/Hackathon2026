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


# ─────────────────────────────────────────────────────────────────────────────
# CLUSTER-BASED PROMPT — Single LLM call for 400+ app topologies
# Instead of sending 400 apps, sends ~12 cluster summaries + bridge apps.
# ─────────────────────────────────────────────────────────────────────────────

CLUSTER_SYSTEM_PROMPT = """You are a senior IBM MQ architect. A rules engine has already:
1. Assigned every app to its dedicated QM (1:1 strict ownership)
2. Derived all channels from actual producer→consumer flows
3. Detected communities (clusters of tightly-connected QMs)
4. Identified bridge apps that connect clusters

Your job is NOT to redo the assignments. The rules engine handled that correctly.
Your job is to provide ARCHITECTURAL INTELLIGENCE that rules cannot:

## TASK 1: CLUSTER REVIEW
For each cluster, assess whether the apps grouped together make architectural sense.
Flag any apps that should move to a different cluster. Explain WHY.
PAY SPECIAL ATTENTION TO:
- PCI-compliant apps mixed with non-PCI apps (compliance boundary violation)
- Payment-critical apps co-located with non-critical batch jobs (blast radius risk)
- Restricted-data apps sharing clusters with lower-classification apps
- Low-latency apps (TRTC <30min) placed far from their consumers

## TASK 2: BRIDGE APP DECISIONS
Bridge apps sit between clusters — these are the genuinely hard decisions.
For each, recommend whether the rules assignment is correct or should change.
Consider business criticality: a PCI payment gateway bridging clusters is higher risk than a logging service.

## TASK 3: ARCHITECTURE DECISION RECORDS
Generate ADRs referencing SPECIFIC entity names from the data. No generic statements.
Include business rationale: "Isolate PCI apps A041, A078 into dedicated cluster to maintain PCI-DSS compliance boundary."

## TASK 4: MODERNIZATION INSIGHTS
Identify:
- Queue pairs that are disguised synchronous RPC calls (bidirectional)
- Fan-out patterns that would benefit from Kafka/event streaming
- Hub QMs that are single points of failure
- Apps that could use direct gRPC instead of MQ
- PCI boundary violations that need architectural remediation

## OUTPUT FORMAT — RETURN ONLY VALID JSON
{
  "cluster_reviews": [
    {
      "cluster_id": 0,
      "assessment": "Well-grouped — all payment-processing apps",
      "move_recommendations": [
        {"app_id": "X", "from_cluster": 0, "to_cluster": 2, "reason": "X is a logging service, not payment"}
      ]
    }
  ],
  "bridge_app_decisions": [
    {
      "app_id": "BRIDGE_APP",
      "current_qm": "QM_X",
      "recommended_qm": "QM_Y",
      "reason": "Produces 80% of messages to cluster 2",
      "keep_current": false
    }
  ],
  "reassignments": [
    {
      "app_id": "A001",
      "from_qm": "QM_OLD",
      "to_qm": "QM_NEW",
      "reason": "Co-locate with dependent apps to eliminate 3 cross-QM channels"
    }
  ],
  "adrs": [
    {
      "id": "ADR-001",
      "title": "Payment cluster isolation",
      "context": "Apps A001, A002, A003 form a payment pipeline across 3 QMs",
      "decision": "Co-locate on cluster 1 QMs to minimize cross-cluster latency",
      "rationale": "Payment flows are latency-sensitive",
      "consequences": "Reduces cross-cluster channels by 4"
    }
  ],
  "modernization_insights": [
    {
      "type": "KAFKA_CANDIDATE | GRPC_CANDIDATE | SPOF | ANTI_PATTERN",
      "entities": ["APP_X", "APP_Y"],
      "detail": "APP_X fans out to 12 consumers — classic pub/sub for Kafka",
      "recommendation": "Replace fan-out with Kafka topic"
    }
  ],
  "design_decisions": [
    {
      "id": "DD-001",
      "type": "REASSIGN | MODERNIZE",
      "affected_entities": ["A001", "QM007"],
      "description": "What you are doing",
      "rationale": "Why — reference specific apps, queues, flow counts"
    }
  ]
}

## RULES
- Reference ONLY entity names from the provided data.
- Every ADR must reference at least 2 specific entity names.
- Reassignments must not break 1:1 ownership.
- Keep reassignment count small (5-15). The rules engine got 90%+ right.
- Focus on the HARD cases — bridges, hubs, anti-patterns."""


def build_cluster_prompt(state: dict) -> str:
    """
    Build a cluster-level prompt for the LLM.
    Sends ~12 cluster summaries + bridge apps instead of 400+ individual apps.
    Total: ~3-6K tokens regardless of app count.
    """
    from collections import Counter
    
    raw_data = state["raw_data"]
    communities = state.get("as_is_communities", {})
    centrality = state.get("as_is_centrality", {})
    metrics = state.get("as_is_metrics", {})

    app_list = raw_data.get("applications", [])
    qm_list = raw_data.get("queue_managers", [])

    # ── Build app→QM ownership (weighted majority) ────────────────────────
    app_qm_counts = {}
    for row in app_list:
        aid = row["app_id"]
        qm = row["qm_id"]
        app_qm_counts.setdefault(aid, {})
        app_qm_counts[aid][qm] = app_qm_counts[aid].get(qm, 0) + 1

    app_preferred_qm = {}
    for aid, qm_counts in app_qm_counts.items():
        app_preferred_qm[aid] = max(qm_counts, key=lambda q: qm_counts[q])

    qm_region = {qm["qm_id"]: qm.get("region", "UNKNOWN") for qm in qm_list}

    # ── Community data ────────────────────────────────────────────────────
    community_map = communities.get("community_map", {})
    community_list = communities.get("communities", [])

    community_apps = {}
    for aid, qm in app_preferred_qm.items():
        cluster = community_map.get(qm, -1)
        community_apps.setdefault(cluster, []).append(aid)

    # ── Flow analysis ─────────────────────────────────────────────────────
    queue_prod, queue_cons = {}, {}
    for row in app_list:
        aid, qname = row["app_id"], row.get("queue_name", "")
        if not qname:
            continue
        d = row.get("direction", "").upper()
        if d in ("PUT", "PRODUCER"):
            queue_prod.setdefault(qname, set()).add(aid)
        elif d in ("GET", "CONSUMER"):
            queue_cons.setdefault(qname, set()).add(aid)

    flow_pairs = set()
    cross_cluster_flows = []
    for qname in set(queue_prod.keys()) & set(queue_cons.keys()):
        for p in queue_prod[qname]:
            for c in queue_cons[qname]:
                if p != c:
                    flow_pairs.add((p, c))
                    p_cl = community_map.get(app_preferred_qm.get(p, ""), -1)
                    c_cl = community_map.get(app_preferred_qm.get(c, ""), -1)
                    if p_cl != c_cl and p_cl >= 0 and c_cl >= 0:
                        cross_cluster_flows.append((p, c, p_cl, c_cl, qname))

    # ── Bridge apps ───────────────────────────────────────────────────────
    app_cluster_conns = {}
    for p, c, p_cl, c_cl, _ in cross_cluster_flows:
        app_cluster_conns.setdefault(p, set()).add(c_cl)
        app_cluster_conns.setdefault(c, set()).add(p_cl)

    bridge_apps = []
    for aid, clusters in app_cluster_conns.items():
        if len(clusters) >= 2:
            own_qm = app_preferred_qm.get(aid, "")
            bridge_apps.append({
                "app_id": aid, "assigned_qm": own_qm,
                "own_cluster": community_map.get(own_qm, -1),
                "connects_to_clusters": sorted(clusters),
                "strength": app_qm_counts.get(aid, {}).get(own_qm, 0),
            })
    bridge_apps.sort(key=lambda x: len(x["connects_to_clusters"]), reverse=True)

    app_fanout = Counter(p for p, _ in flow_pairs)
    bidir_pairs = [(p, c) for p, c in flow_pairs if (c, p) in flow_pairs and p < c]

    # ── Business metadata lookup ──────────────────────────────────────────
    app_meta = raw_data.get("app_metadata", {})
    
    def _app_tag(aid):
        """Compact business tag for an app: A084 (PaymentGW, PCI, CRITICAL)"""
        m = app_meta.get(aid, {})
        tags = []
        # Neighborhood/domain
        nb = m.get("neighborhood", "")
        if nb and nb != "UNKNOWN":
            tags.append(nb)
        # PCI
        if m.get("is_pci", "").upper() == "YES":
            tags.append("PCI")
        # Payment critical
        if m.get("is_payment_critical", "").upper() == "YES":
            tags.append("CRITICAL")
        # Data classification
        dc = m.get("data_classification", "")
        if dc and dc != "UNKNOWN" and dc.upper() == "RESTRICTED":
            tags.append("Restricted")
        # TRTC (latency requirement)
        trtc = m.get("trtc", "")
        if "0-30" in trtc:
            tags.append("Low-latency")
        return f"{aid} ({', '.join(tags)})" if tags else aid

    # ── Build prompt ──────────────────────────────────────────────────────
    lines = [
        f"## TOPOLOGY OVERVIEW",
        f"{len(app_preferred_qm)} apps, {len(set(app_preferred_qm.values()))} QMs after 1:1 assignment.",
        f"{len(flow_pairs)} flows, {len(cross_cluster_flows)} cross-cluster.",
        f"Complexity: {metrics.get('total_score', 'N/A')}/100, "
        f"Modularity: {communities.get('modularity', 'N/A')}",
        f"SPOFs: {centrality.get('spof_qms', [])[:5]}",
        f"Hubs: {centrality.get('hub_qms', [])[:5]}",
    ]

    # ── PCI / Criticality summary ─────────────────────────────────────────
    if app_meta:
        pci_apps = [a for a, m in app_meta.items() if m.get("is_pci", "").upper() == "YES"]
        critical_apps = [a for a, m in app_meta.items() if m.get("is_payment_critical", "").upper() == "YES"]
        restricted_apps = [a for a, m in app_meta.items() if m.get("data_classification", "").upper() == "RESTRICTED"]
        low_latency_apps = [a for a, m in app_meta.items() if "0-30" in m.get("trtc", "")]
        
        lines += [
            "",
            f"## BUSINESS CONTEXT",
            f"  PCI-compliant apps: {len(pci_apps)} — {', '.join(sorted(pci_apps)[:8])}{'...' if len(pci_apps)>8 else ''}",
            f"  Payment-critical apps: {len(critical_apps)} — {', '.join(sorted(critical_apps)[:8])}{'...' if len(critical_apps)>8 else ''}",
            f"  Restricted data apps: {len(restricted_apps)}",
            f"  Low-latency (TRTC <30min): {len(low_latency_apps)}",
        ]
        
        # Check if PCI apps are scattered across clusters
        pci_clusters = set()
        for a in pci_apps:
            qm = app_preferred_qm.get(a, "")
            cl = community_map.get(qm, -1)
            if cl >= 0:
                pci_clusters.add(cl)
        if len(pci_clusters) > 1:
            lines.append(f"  WARNING: PCI apps are spread across {len(pci_clusters)} clusters — consider isolating")
        
        # LOB distribution
        lob_counter = Counter(m.get("line_of_business", "?") for m in app_meta.values())
        lines.append(f"  Lines of business: {dict(lob_counter.most_common(5))}")

    lines += ["", f"## CLUSTERS ({len(community_list)} communities)"]

    for idx, comm_qms in enumerate(community_list):
        apps_in = community_apps.get(idx, [])
        regions = sorted(set(qm_region.get(qm, "?") for qm in comm_qms))
        top = sorted([(a, app_fanout.get(a, 0)) for a in apps_in], key=lambda x: -x[1])[:5]
        top_str = ", ".join(f"{_app_tag(a)}(→{f})" for a, f in top if f > 0)
        
        # Per-cluster business profile
        cluster_pci = sum(1 for a in apps_in if app_meta.get(a, {}).get("is_pci", "").upper() == "YES")
        cluster_critical = sum(1 for a in apps_in if app_meta.get(a, {}).get("is_payment_critical", "").upper() == "YES")
        cluster_neighborhoods = Counter(app_meta.get(a, {}).get("neighborhood", "?") for a in apps_in)
        top_neighborhoods = ", ".join(f"{n}({c})" for n, c in cluster_neighborhoods.most_common(3))
        
        lines.append(f"  C{idx}: {len(comm_qms)} QMs, {len(apps_in)} apps, regions={regions}")
        lines.append(f"    QMs: {', '.join(sorted(comm_qms)[:10])}{'...' if len(comm_qms)>10 else ''}")
        if cluster_pci or cluster_critical:
            lines.append(f"    Business: {cluster_pci} PCI, {cluster_critical} payment-critical, domains: {top_neighborhoods}")
        if top_str:
            lines.append(f"    Top producers: {top_str}")

    lines += ["", f"## BRIDGE APPS ({len(bridge_apps)} connecting multiple clusters)"]
    for ba in bridge_apps[:30]:
        lines.append(f"  {_app_tag(ba['app_id'])}: on {ba['assigned_qm']} (C{ba['own_cluster']}), "
                     f"→ clusters {ba['connects_to_clusters']}, strength={ba['strength']}")
    if len(bridge_apps) > 30:
        lines.append(f"  ... and {len(bridge_apps)-30} more")

    lines += ["", f"## CROSS-CLUSTER FLOW DENSITY"]
    fc = Counter((p_cl, c_cl) for _, _, p_cl, c_cl, _ in cross_cluster_flows)
    for (f, t), cnt in fc.most_common(15):
        lines.append(f"  C{f} → C{t}: {cnt} flows")

    lines += ["", f"## HIGH FAN-OUT (Kafka candidates)"]
    for aid, cnt in app_fanout.most_common(10):
        lines.append(f"  {_app_tag(aid)}: → {cnt} consumers, on {app_preferred_qm.get(aid,'?')}")

    if bidir_pairs:
        lines += ["", f"## BIDIRECTIONAL PAIRS (RPC candidates, {len(bidir_pairs)} pairs)"]
        for p, c in bidir_pairs[:10]:
            lines.append(f"  {_app_tag(p)} ↔ {_app_tag(c)}")
        if len(bidir_pairs) > 10:
            lines.append(f"  ... and {len(bidir_pairs)-10} more")

    # ── Reassignment constraints ──────────────────────────────────────────
    # Tell the LLM which QMs are occupied so it doesn't suggest invalid moves.
    # Also tell it how to make valid reassignments (swap, not overwrite).
    lines += ["", "## REASSIGNMENT CONSTRAINTS"]
    lines.append("Every QM already has exactly 1 app assigned (1:1 rule).")
    lines.append("To move app A from QM_X to QM_Y, you MUST ALSO move the app currently on QM_Y.")
    lines.append("Alternatively, suggest moving app A to a NEW QM name (e.g. QM_CLUSTER1_A001).")
    lines.append("If you suggest a reassignment to an occupied QM without swapping, it will be rejected.")
    lines.append("")
    lines.append("OCCUPIED QMs (sample — ALL original QMs are occupied):")
    # Show a sample of QM→app mappings so the LLM understands the constraint
    qm_to_app = {}
    for aid, qm in app_preferred_qm.items():
        qm_to_app[qm] = aid
    for qm in sorted(qm_to_app.keys())[:20]:
        lines.append(f"  {qm} ← {qm_to_app[qm]}")
    if len(qm_to_app) > 20:
        lines.append(f"  ... all {len(qm_to_app)} QMs are occupied")

    feedback = state.get("human_feedback", "")
    if feedback:
        lines += ["", f"## HUMAN FEEDBACK", feedback]

    lines += ["", "Analyse this topology. Focus on bridge apps, cluster coherence, modernization.", 
              "Return ONLY valid JSON."]

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# FEEDBACK INTERPRETER — LLM parses human feedback into structured directives
# ─────────────────────────────────────────────────────────────────────────────

FEEDBACK_INTERPRETER_SYSTEM = """You are an IBM MQ architect assistant. The human reviewer has given feedback on a proposed MQ topology design. Parse their feedback into structured JSON directives.

CONSTRAINTS YOU CANNOT BREAK:
- Every app must have exactly 1 dedicated QM (strict 1:1 ownership)
- QM count cannot be reduced (N apps = N QMs)
- The only lever is CHANNEL PRUNING — removing unnecessary inter-QM channels

AVAILABLE DIRECTIVES:
- channel_prune_pct: float 0.0-1.0 — fraction of channels to remove (e.g. "remove 50%" = 0.5)
- target_reduction_pct: float 0.0-1.0 — desired complexity score reduction (e.g. "90% reduction" = 0.9)
- fanout_cap: int — max outbound channels per QM (default 3)
- aggressive: bool — enable aggressive pruning mode
- protect_pci: bool — do NOT prune channels serving PCI-compliant apps
- protect_payment_critical: bool — do NOT prune channels serving payment-critical apps
- protect_apps: list of app IDs — do NOT prune channels touching these apps
- protect_qms: list of QM names — do NOT prune channels touching these QMs
- priority_metric: one of "channels", "fanout", "routing_depth" — which factor to prioritise reducing

Return ONLY valid JSON. Example:
{
  "directives": {
    "aggressive": true,
    "channel_prune_pct": 0.5,
    "protect_pci": true,
    "fanout_cap": 3,
    "priority_metric": "channels"
  },
  "reasoning": "The reviewer wants drastic reduction but needs PCI flows intact..."
}"""


def build_feedback_interpreter_prompt(feedback: str, metrics: dict) -> str:
    """Build the user prompt for the feedback interpreter LLM call."""
    return f"""## CURRENT METRICS
As-is complexity: {metrics.get('as_is_score', 'N/A')}/100
Target complexity: {metrics.get('target_score', 'N/A')}/100
Current reduction: {metrics.get('reduction_pct', 'N/A')}%
Channel count: {metrics.get('channel_count', 'N/A')}
Fan-out (max): {metrics.get('fan_out', 'N/A')}
Routing depth: {metrics.get('routing_depth', 'N/A')}

## HUMAN FEEDBACK
{feedback}

Parse this feedback into structured directives. Return ONLY valid JSON."""


# ─────────────────────────────────────────────────────────────────────────────
# CHANNEL PRUNING ADVISOR — LLM decides which channels are safe to remove
# ─────────────────────────────────────────────────────────────────────────────

CHANNEL_ADVISOR_SYSTEM = """You are a senior IBM MQ architect. Given a list of channels with their flow counts and business metadata, decide which channels can be safely removed to reduce topology complexity.

RULES:
1. NEVER suggest removing a channel that is the ONLY path between a producer and its consumer
2. Prefer removing channels with the FEWEST flows
3. PCI-compliant and payment-critical flows should be preserved unless explicitly told otherwise
4. High fan-out QMs (many outbound channels) should be prioritised for pruning
5. Keep the topology connected — don't create isolated QMs that need channels

Return ONLY valid JSON:
{
  "remove": [
    {"from_qm": "QM_A", "to_qm": "QM_B", "reason": "Only 1 non-critical flow, alternative path exists via QM_C"},
    ...
  ],
  "keep": [
    {"from_qm": "QM_X", "to_qm": "QM_Y", "reason": "Critical PCI flow, no alternative path"}
  ],
  "summary": "Recommended removing N channels. Preserved M critical paths..."
}"""


def build_channel_advisor_prompt(state: dict, target_graph, max_channels: int = 80) -> str:
    """
    Build prompt for the channel pruning advisor.
    Sends channel list with flow counts + business context.
    Capped at max_channels to stay within token limits.
    """
    import networkx as nx
    
    raw_data = state.get("raw_data", {})
    app_meta = raw_data.get("app_metadata", {})
    
    # Build app→QM map from graph
    app_qm = {}
    for n, d in target_graph.nodes(data=True):
        if d.get("type") == "app":
            for _, v, ed in target_graph.out_edges(n, data=True):
                if ed.get("rel") == "connects_to":
                    app_qm[n] = v
                    break
    
    # Build flow data per channel
    queue_prod, queue_cons = {}, {}
    for row in raw_data.get("applications", []):
        aid = row["app_id"]
        qname = row.get("queue_name", "")
        if not qname:
            continue
        d = row.get("direction", "").upper()
        if d in ("PUT", "PRODUCER"):
            queue_prod.setdefault(qname, set()).add(aid)
        elif d in ("GET", "CONSUMER"):
            queue_cons.setdefault(qname, set()).add(aid)
    
    # Count flows per channel + track business metadata
    channel_info = {}
    for qname in set(queue_prod.keys()) & set(queue_cons.keys()):
        for p in queue_prod[qname]:
            for c in queue_cons[qname]:
                if p == c:
                    continue
                fqm = app_qm.get(p)
                tqm = app_qm.get(c)
                if fqm and tqm and fqm != tqm:
                    key = (fqm, tqm)
                    if key not in channel_info:
                        channel_info[key] = {"flows": 0, "apps": set(), "pci": False, "critical": False}
                    channel_info[key]["flows"] += 1
                    channel_info[key]["apps"].add(p)
                    channel_info[key]["apps"].add(c)
                    # Check business metadata
                    for a in [p, c]:
                        meta = app_meta.get(a, {})
                        if meta.get("is_pci", "").upper() == "YES":
                            channel_info[key]["pci"] = True
                        if meta.get("is_payment_critical", "").upper() == "YES":
                            channel_info[key]["critical"] = True
    
    # Get actual channels from graph
    graph_channels = [(u, v, d) for u, v, d in target_graph.edges(data=True) if d.get("rel") == "channel"]
    
    # Per-QM fan-out
    qm_fanout = {}
    for u, v, d in graph_channels:
        qm_fanout[u] = qm_fanout.get(u, 0) + 1
    
    # Build channel lines (sorted by flow count ascending — weakest first)
    ch_list = []
    for u, v, d in graph_channels:
        info = channel_info.get((u, v), {"flows": 0, "pci": False, "critical": False})
        ch_list.append({
            "from": u, "to": v,
            "flows": info["flows"],
            "pci": info.get("pci", False),
            "critical": info.get("critical", False),
            "src_fanout": qm_fanout.get(u, 0),
        })
    
    ch_list.sort(key=lambda x: x["flows"])
    ch_list = ch_list[:max_channels]  # cap for token budget
    
    # Feedback
    feedback = state.get("human_feedback", "")
    directives = state.get("feedback_directives", {}) or {}
    
    lines = [
        f"## TOPOLOGY: {len(app_qm)} apps, {len(set(app_qm.values()))} QMs, {len(graph_channels)} channels",
        f"## HUMAN FEEDBACK: {feedback}" if feedback else "",
        f"## DIRECTIVES: {directives}" if directives else "",
        "",
        f"## CHANNELS ({len(ch_list)} shown, sorted weakest-first)",
        "from_qm | to_qm | flows | pci | critical | src_fanout",
        "---|---|---|---|---|---",
    ]
    
    for ch in ch_list:
        lines.append(
            f"{ch['from']} | {ch['to']} | {ch['flows']} | "
            f"{'PCI' if ch['pci'] else '-'} | "
            f"{'CRIT' if ch['critical'] else '-'} | "
            f"{ch['src_fanout']}"
        )
    
    lines += ["", "Decide which channels to remove. Return ONLY valid JSON."]
    
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# ROLE 4: DESIGN CRITIC — Post-optimizer self-review
# ─────────────────────────────────────────────────────────────────────────────

DESIGN_CRITIC_SYSTEM = """You are a senior IBM MQ architect reviewing a proposed target state topology.
Given the before/after metrics and topology summary, identify weaknesses and suggest improvements.

Focus on:
1. QMs with excessive fan-out (>5 channels) — suggest routing consolidation
2. PCI/payment-critical apps that lost direct channels — flag as risk
3. Disconnected components that should be connected
4. Cluster imbalances (one cluster has 80% of traffic)
5. Missed optimization opportunities

Return ONLY valid JSON:
{
  "issues": [
    {"severity": "HIGH|MEDIUM|LOW", "entity": "QM_X or APP_Y", "issue": "...", "suggestion": "..."}
  ],
  "overall_assessment": "GOOD|NEEDS_WORK|CRITICAL_ISSUES",
  "summary": "2-3 sentence overview"
}"""


def build_design_critic_prompt(state: dict) -> str:
    """Build prompt for the design critic. Runs after optimizer, before human review."""
    as_is = state.get("as_is_metrics", {})
    target = state.get("target_metrics", {})
    communities = state.get("target_communities", {})
    centrality = state.get("target_centrality", {})
    entropy = state.get("target_entropy", {})
    subgraphs = state.get("target_subgraphs", []) or []
    app_meta = state.get("raw_data", {}).get("app_metadata", {})
    
    as_score = as_is.get("total_score", 0)
    tgt_score = target.get("total_score", 0)
    pct = round(((as_score - tgt_score) / as_score) * 100, 1) if as_score else 0
    
    pci_count = sum(1 for m in app_meta.values() if m.get("is_pci", "").upper() == "YES")
    crit_count = sum(1 for m in app_meta.values() if m.get("is_payment_critical", "").upper() == "YES")
    
    lines = [
        f"## METRICS COMPARISON",
        f"As-is score: {as_score}/100 → Target: {tgt_score}/100 ({pct}% reduction)",
        f"Channels: {as_is.get('channel_count','?')} → {target.get('channel_count','?')}",
        f"Fan-out (max): {as_is.get('fan_out_score','?')} → {target.get('fan_out_score','?')}",
        f"Routing depth: {as_is.get('routing_depth','?')} → {target.get('routing_depth','?')}",
        f"Coupling: {as_is.get('coupling_index','?')} → {target.get('coupling_index','?')}",
        f"",
        f"## BUSINESS CONTEXT",
        f"PCI apps: {pci_count}, Payment-critical: {crit_count}",
        f"",
        f"## TARGET TOPOLOGY",
        f"Communities: {communities.get('num_communities', '?')}, modularity: {communities.get('modularity', '?')}",
        f"SPOFs: {centrality.get('spof_qms', [])[:5]}",
        f"Hubs: {centrality.get('hub_qms', [])[:5]}",
        f"Components: {len(subgraphs)}, isolated: {sum(1 for s in subgraphs if s.get('is_isolated'))}",
        f"Entropy ratio: {entropy.get('entropy_ratio', '?')}",
        f"",
        f"Review this design. Identify weaknesses and suggest improvements.",
        f"Return ONLY valid JSON.",
    ]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# ROLE 5: MIGRATION RISK ASSESSOR — Scores migration steps by risk
# ─────────────────────────────────────────────────────────────────────────────

MIGRATION_RISK_SYSTEM = """You are an IBM MQ migration specialist. Given a migration plan with step details and business metadata, assess the risk of each phase and reorder steps within phases for safety.

RISK FACTORS:
- PCI-compliant apps: migrate LAST within each phase, need dedicated maintenance window
- Payment-critical apps: HIGH risk, migrate during low-traffic window
- High fan-out QMs: migrate carefully — many downstream consumers affected
- Apps with bidirectional flows: must coordinate both sides simultaneously

Return ONLY valid JSON:
{
  "phase_risks": {
    "CREATE": {"risk": "LOW|MEDIUM|HIGH", "reason": "..."},
    "REROUTE": {"risk": "LOW|MEDIUM|HIGH", "reason": "..."},
    "DRAIN": {"risk": "LOW|MEDIUM|HIGH", "reason": "..."},
    "CLEANUP": {"risk": "LOW|MEDIUM|HIGH", "reason": "..."}
  },
  "high_risk_steps": [
    {"step_description": "...", "risk": "HIGH", "reason": "...", "mitigation": "..."}
  ],
  "recommended_sequence": "Brief description of recommended ordering",
  "maintenance_windows": ["Phase X should run during weekend maintenance", "..."]
}"""


def build_migration_risk_prompt(state: dict) -> str:
    """Build prompt for migration risk assessment."""
    diff = state.get("topology_diff", {}) or {}
    plan = state.get("migration_plan", {}) or {}
    app_meta = state.get("raw_data", {}).get("app_metadata", {})
    
    lines = [
        f"## MIGRATION SUMMARY",
        f"QMs added: {len(diff.get('qms_added', []))}",
        f"QMs removed: {len(diff.get('qms_removed', []))}",
        f"Channels added: {len(diff.get('channels_added', []))}",
        f"Channels removed: {len(diff.get('channels_removed', []))}",
        f"Apps reassigned: {len(diff.get('apps_reassigned', []))}",
        f"Total steps: {plan.get('total_steps', 0)}",
        f"",
        f"## PHASE BREAKDOWN",
    ]
    
    for phase_name in ["CREATE", "REROUTE", "DRAIN", "CLEANUP"]:
        steps = plan.get("phases", {}).get(phase_name, [])
        lines.append(f"{phase_name}: {len(steps)} steps")
        for s in steps[:5]:
            lines.append(f"  - {s.get('description', '?')[:100]}")
        if len(steps) > 5:
            lines.append(f"  ... and {len(steps)-5} more")
    
    # Business metadata for reassigned apps
    reassigned = diff.get("apps_reassigned", [])
    if reassigned and app_meta:
        lines += ["", f"## REASSIGNED APPS — BUSINESS CONTEXT"]
        for r in reassigned[:20]:
            app_id = r.get("app_id", "")
            meta = app_meta.get(app_id, {})
            tags = []
            if meta.get("is_pci", "").upper() == "YES": tags.append("PCI")
            if meta.get("is_payment_critical", "").upper() == "YES": tags.append("PAYMENT-CRITICAL")
            if tags:
                lines.append(f"  {app_id}: {', '.join(tags)} — {r.get('from_qm','?')} → {r.get('to_qm','?')}")
    
    lines += ["", "Assess risk and recommend safe ordering. Return ONLY valid JSON."]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# ROLE 6: ANOMALY DETECTIVE — As-is topology anomaly detection
# ─────────────────────────────────────────────────────────────────────────────

ANOMALY_DETECTIVE_SYSTEM = """You are an IBM MQ topology analyst. Given a summary of the as-is MQ environment, identify anomalies, anti-patterns, and hidden risks.

LOOK FOR:
1. Apps connecting to multiple QMs (should be 1:1) — which connections are likely stale?
2. QMs with zero apps — orphaned, should be decommissioned
3. Circular channel dependencies — routing loops
4. Extreme fan-out (one QM serving >10 consumers) — SPOF risk
5. Inconsistent naming patterns — suggests organic growth
6. Apps that only produce OR only consume but never both — potential dead endpoints
7. Clusters of QMs with no cross-cluster channels — isolated islands
8. PCI/critical apps on shared QMs — compliance risk

Return ONLY valid JSON:
{
  "anomalies": [
    {"type": "STALE_CONNECTION|ORPHAN|CIRCULAR|SPOF|NAMING|DEAD_ENDPOINT|ISOLATION|COMPLIANCE",
     "severity": "HIGH|MEDIUM|LOW",
     "entities": ["QM_X", "APP_Y"],
     "detail": "...",
     "recommendation": "..."}
  ],
  "topology_health": "HEALTHY|DEGRADED|CRITICAL",
  "summary": "2-3 sentence overview of key findings"
}"""


def build_anomaly_detective_prompt(state: dict) -> str:
    """Build prompt for anomaly detection on the as-is topology."""
    raw_data = state.get("raw_data", {})
    violations = state.get("data_quality_report", {}).get("topology_violations", {})
    communities = state.get("as_is_communities", {})
    centrality = state.get("as_is_centrality", {})
    entropy = state.get("as_is_entropy", {})
    metrics = state.get("as_is_metrics", {})
    app_meta = raw_data.get("app_metadata", {})
    
    app_list = raw_data.get("applications", [])
    qm_list = raw_data.get("queue_managers", [])
    ch_list = raw_data.get("channels", [])
    
    # Apps per QM
    from collections import Counter
    app_qm_counter = Counter()
    app_directions = {}
    for row in app_list:
        app_qm_counter[row["qm_id"]] += 1
        aid = row["app_id"]
        d = row.get("direction", "UNKNOWN").upper()
        app_directions.setdefault(aid, set()).add(d)
    
    # Multi-QM apps
    app_qm_set = {}
    for row in app_list:
        app_qm_set.setdefault(row["app_id"], set()).add(row["qm_id"])
    multi_qm = {a: qms for a, qms in app_qm_set.items() if len(qms) > 1}
    
    # Uni-directional apps
    produce_only = [a for a, dirs in app_directions.items() if dirs <= {"PUT", "PRODUCER"}]
    consume_only = [a for a, dirs in app_directions.items() if dirs <= {"GET", "CONSUMER"}]
    
    lines = [
        f"## AS-IS TOPOLOGY",
        f"{len(qm_list)} QMs, {len(set(r['app_id'] for r in app_list))} unique apps, {len(ch_list)} channels",
        f"Complexity score: {metrics.get('total_score', '?')}/100",
        f"",
        f"## VIOLATIONS DETECTED",
        f"Multi-QM apps: {len(multi_qm)} — {list(multi_qm.keys())[:10]}",
        f"Orphan QMs: {violations.get('orphan_qms', [])[:10]}",
        f"Cycles: {len(violations.get('cycles', []))}",
        f"",
        f"## QM LOAD DISTRIBUTION",
    ]
    
    for qm, count in app_qm_counter.most_common(10):
        lines.append(f"  {qm}: {count} app connections")
    empty_qms = [qm["qm_id"] for qm in qm_list if app_qm_counter.get(qm["qm_id"], 0) == 0]
    if empty_qms:
        lines.append(f"  Empty QMs (0 apps): {empty_qms[:10]}")
    
    lines += [
        f"",
        f"## DIRECTIONAL ANALYSIS",
        f"Produce-only apps: {len(produce_only)} — {produce_only[:8]}",
        f"Consume-only apps: {len(consume_only)} — {consume_only[:8]}",
        f"",
        f"## GRAPH ANALYTICS",
        f"Communities: {communities.get('num_communities', '?')}, modularity: {communities.get('modularity', '?')}",
        f"SPOFs: {centrality.get('spof_qms', [])[:5]}",
        f"Hubs: {centrality.get('hub_qms', [])[:5]}",
        f"Entropy ratio: {entropy.get('entropy_ratio', '?')}",
    ]
    
    if app_meta:
        pci = [a for a, m in app_meta.items() if m.get("is_pci", "").upper() == "YES"]
        lines += [
            f"",
            f"## BUSINESS CONTEXT",
            f"PCI apps: {len(pci)} — {pci[:10]}",
        ]
        # Check if PCI apps share QMs
        pci_qms = set()
        for a in pci:
            for qm in app_qm_set.get(a, set()):
                pci_qms.add(qm)
        shared_pci_qms = [qm for qm in pci_qms if app_qm_counter.get(qm, 0) > 1]
        if shared_pci_qms:
            lines.append(f"  PCI apps on SHARED QMs (compliance risk): {shared_pci_qms[:5]}")
    
    lines += ["", "Identify anomalies and hidden risks. Return ONLY valid JSON."]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# ROLE 7: ADR ENRICHER — Generate enterprise-grade Architecture Decision Records
# ─────────────────────────────────────────────────────────────────────────────

ADR_ENRICHER_SYSTEM = """You are a senior enterprise architect writing Architecture Decision Records (ADRs) for an IBM MQ topology transformation.

Given the transformation summary (what changed, why, business context), produce 5-8 high-quality ADRs.

Each ADR must:
- Reference SPECIFIC entity names (apps, QMs, channels) from the data
- Explain the business justification, not just technical rationale
- Note consequences including risks and trade-offs
- Be suitable for an enterprise Architecture Review Board

Return ONLY valid JSON:
{
  "adrs": [
    {
      "id": "ADR-001",
      "title": "Short decision title",
      "status": "ACCEPTED",
      "context": "Why this decision was needed — reference specific entities and metrics",
      "decision": "What was decided",
      "rationale": "Why this option was chosen over alternatives",
      "consequences": "What this means — both positive and negative",
      "compliance": "How this satisfies constraints (1:1, routing, naming)"
    }
  ]
}"""


def build_adr_enricher_prompt(state: dict) -> str:
    """Build prompt for enterprise-grade ADR generation."""
    as_is = state.get("as_is_metrics", {})
    target = state.get("target_metrics", {})
    diff = state.get("topology_diff", {}) or {}
    method = state.get("architect_method", "rules_fallback")
    communities = state.get("target_communities", {})
    app_meta = state.get("raw_data", {}).get("app_metadata", {})
    
    as_score = as_is.get("total_score", 0)
    tgt_score = target.get("total_score", 0)
    pct = round(((as_score - tgt_score) / as_score) * 100, 1) if as_score else 0
    
    lines = [
        f"## TRANSFORMATION SUMMARY",
        f"Method: {method}",
        f"Complexity: {as_score} → {tgt_score} ({pct}% reduction)",
        f"Channels: {as_is.get('channel_count','?')} → {target.get('channel_count','?')}",
        f"Fan-out: {as_is.get('fan_out_score','?')} → {target.get('fan_out_score','?')}",
        f"QMs added: {len(diff.get('qms_added',[]))}, removed: {len(diff.get('qms_removed',[]))}",
        f"Apps reassigned: {len(diff.get('apps_reassigned',[]))}",
        f"Channels added: {len(diff.get('channels_added',[]))}, removed: {len(diff.get('channels_removed',[]))}",
        f"Communities: {communities.get('num_communities','?')}",
        f"",
        f"## KEY CHANGES",
    ]
    
    for r in (diff.get("apps_reassigned", []) or [])[:10]:
        app_id = r.get("app_id", "")
        meta = app_meta.get(app_id, {})
        tags = []
        if meta.get("is_pci", "").upper() == "YES": tags.append("PCI")
        if meta.get("is_payment_critical", "").upper() == "YES": tags.append("CRITICAL")
        tag_str = f" [{','.join(tags)}]" if tags else ""
        lines.append(f"  {app_id}{tag_str}: {r.get('from_qm','?')} → {r.get('to_qm','?')}")
    
    feedback = state.get("human_feedback", "")
    if feedback:
        lines += [f"", f"## HUMAN FEEDBACK", f"{feedback[:300]}"]
    
    lines += ["", "Generate 5-8 enterprise-grade ADRs. Return ONLY valid JSON."]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# ROLE 8: COMPLIANCE AUDITOR
# Post-approval audit of target state against MQ best practices, security
# standards, and HA patterns. Surfaces anything the rules engine missed.
# ─────────────────────────────────────────────────────────────────────────────
COMPLIANCE_AUDITOR_SYSTEM = """You are an IBM MQ compliance and security auditor. Given a target state MQ topology, assess it against enterprise best practices, security standards, and high-availability patterns.

You MUST return ONLY valid JSON matching this schema:
{
  "compliance_score": <int 0-100>,
  "findings": [
    {
      "category": "SECURITY" | "HA" | "NAMING" | "ROUTING" | "BEST_PRACTICE",
      "severity": "CRITICAL" | "HIGH" | "MEDIUM" | "LOW" | "INFO",
      "finding": "<what the auditor found>",
      "recommendation": "<what should be done>",
      "affected_entities": ["<QM or channel names>"]
    }
  ],
  "ha_assessment": {
    "has_redundancy": <bool>,
    "spof_count": <int>,
    "recommendation": "<HA recommendation>"
  },
  "security_assessment": {
    "ssl_tls_recommended": <bool>,
    "auth_gaps": ["<list of auth concerns>"],
    "channel_security_score": <int 0-100>
  },
  "summary": "<2-3 sentence overall assessment>"
}

Focus areas:
- Channel security: Are channels using SSL/TLS? Are there auth mechanisms?
- High availability: Are there single points of failure? Are critical QMs redundant?
- Naming conventions: Do channel names follow FROM_QM.TO_QM deterministic pattern?
- Routing efficiency: Are there unnecessary hops or circular routes?
- Best practices: XMITQ configuration, dead-letter queues, max message depth settings
- Segregation: Are PCI/payment-critical apps isolated appropriately?

Be specific — reference actual QM names and channel names from the topology."""


def build_compliance_auditor_prompt(state: dict) -> str:
    """Build the compliance auditor prompt from target state data."""
    target = state.get("target_metrics", {})
    as_is = state.get("as_is_metrics", {})
    communities = state.get("target_communities", {})
    centrality = state.get("target_centrality", {})
    entropy = state.get("target_entropy", {})
    subgraphs = state.get("target_subgraphs", [])
    violations = state.get("constraint_violations", [])
    raw_data = state.get("raw_data", {})

    # Build QM and channel summary from the graph
    G = state.get("optimised_graph")
    qm_list = []
    channel_list = []
    if G:
        for n, d in G.nodes(data=True):
            if d.get("type") == "qm":
                apps = [u for u, v, ed in G.in_edges(n, data=True) if ed.get("rel") == "connects_to"]
                out_ch = sum(1 for _, _, ed in G.out_edges(n, data=True) if ed.get("rel") == "channel")
                in_ch = sum(1 for _, _, ed in G.in_edges(n, data=True) if ed.get("rel") == "channel")
                qm_list.append(f"  {n}: {len(apps)} app(s), {out_ch} outbound / {in_ch} inbound channels, region={d.get('region','?')}")
        for u, v, d in G.edges(data=True):
            if d.get("rel") == "channel":
                channel_list.append(f"  {d.get('channel_name', f'{u}.{v}')}: {u} → {v}")

    # PCI/critical app detection
    pci_apps = []
    for row in raw_data.get("applications", []):
        tags = []
        if str(row.get("is_pci", "")).upper() == "YES":
            tags.append("PCI")
        if str(row.get("is_payment_critical", "")).upper() == "YES":
            tags.append("PAYMENT-CRITICAL")
        if tags:
            pci_apps.append(f"  {row['app_id']}: {', '.join(tags)}")

    lines = [
        "## TARGET STATE TOPOLOGY FOR COMPLIANCE AUDIT",
        f"QM count: {target.get('qm_count', len(qm_list))}",
        f"Channel count: {target.get('channel_count', len(channel_list))}",
        f"Complexity score: {target.get('total_score', '?')}/100",
        f"SPOF QMs: {', '.join(centrality.get('spof_qms', [])) or 'none'}",
        f"Communities: {communities.get('num_communities', '?')}",
        f"Entropy ratio: {entropy.get('entropy_ratio', '?')}",
        f"Existing violations: {len(violations)}",
        "",
        "## QUEUE MANAGERS (sample, first 30)",
        *qm_list[:30],
        "",
        "## CHANNELS (sample, first 40)",
        *channel_list[:40],
    ]

    if pci_apps:
        lines += ["", "## PCI / PAYMENT-CRITICAL APPS", *pci_apps[:20]]

    if violations:
        lines += [
            "", "## EXISTING CONSTRAINT VIOLATIONS",
            *[f"  [{v['severity']}] {v['rule']}: {v['detail']}" for v in violations[:10]]
        ]

    lines += ["", "Audit this topology. Return ONLY valid JSON."]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# ROLE 9: CAPACITY PLANNER
# Analyzes message flow volumes and app distribution to identify capacity
# imbalances — over-provisioned QMs (wasted infra) and under-provisioned
# QMs (risk of message backlog).
# ─────────────────────────────────────────────────────────────────────────────
CAPACITY_PLANNER_SYSTEM = """You are an IBM MQ capacity planning specialist. Given a target state MQ topology with message flow data, analyze the capacity profile and identify imbalances.

You MUST return ONLY valid JSON matching this schema:
{
  "capacity_score": <int 0-100, where 100 = perfectly balanced>,
  "hotspots": [
    {
      "qm": "<QM name>",
      "issue": "OVER_PROVISIONED" | "UNDER_PROVISIONED" | "BOTTLENECK" | "IDLE",
      "severity": "HIGH" | "MEDIUM" | "LOW",
      "detail": "<explanation>",
      "recommendation": "<specific action>"
    }
  ],
  "flow_analysis": {
    "total_flows": <int>,
    "busiest_qm": "<QM name>",
    "busiest_qm_flows": <int>,
    "quietest_qm": "<QM name>",
    "quietest_qm_flows": <int>,
    "flow_imbalance_ratio": <float, max/min>
  },
  "scaling_recommendations": [
    "<actionable recommendation for capacity planning>"
  ],
  "summary": "<2-3 sentence capacity assessment>"
}

Focus areas:
- QMs handling disproportionately many producer→consumer flows (bottleneck risk)
- QMs with zero or minimal flows (wasted infrastructure)
- Fan-out concentration — one QM routing to many others
- Message volume distribution across clusters/communities
- Recommendations for horizontal scaling or QM consolidation

Be specific — use actual QM names and flow counts."""


def build_capacity_planner_prompt(state: dict) -> str:
    """Build the capacity planner prompt from topology and flow data."""
    target = state.get("target_metrics", {})
    raw_data = state.get("raw_data", {})
    communities = state.get("target_communities", {})

    G = state.get("optimised_graph")
    if not G:
        return "No target graph available for capacity analysis."

    # Build per-QM flow statistics
    app_qm_map = {}
    for n, d in G.nodes(data=True):
        if d.get("type") == "app":
            qms = [v for _, v, ed in G.out_edges(n, data=True) if ed.get("rel") == "connects_to"]
            if qms:
                app_qm_map[n] = qms[0]

    # Count flows per QM (producer and consumer sides)
    queue_producers = {}
    queue_consumers = {}
    for row in raw_data.get("applications", []):
        aid = row.get("app_id", "")
        qname = row.get("queue_name", "")
        if not qname:
            continue
        d = row.get("direction", "").upper()
        if d in ("PUT", "PRODUCER"):
            queue_producers.setdefault(qname, set()).add(aid)
        elif d in ("GET", "CONSUMER"):
            queue_consumers.setdefault(qname, set()).add(aid)

    # Count cross-QM flows per QM
    qm_outbound_flows = {}  # {qm: count of outbound flows}
    qm_inbound_flows = {}   # {qm: count of inbound flows}
    for qname in set(queue_producers.keys()) & set(queue_consumers.keys()):
        for p in queue_producers[qname]:
            for c in queue_consumers[qname]:
                if p == c:
                    continue
                fqm = app_qm_map.get(p)
                tqm = app_qm_map.get(c)
                if fqm and tqm and fqm != tqm:
                    qm_outbound_flows[fqm] = qm_outbound_flows.get(fqm, 0) + 1
                    qm_inbound_flows[tqm] = qm_inbound_flows.get(tqm, 0) + 1

    qm_nodes = [n for n, d in G.nodes(data=True) if d.get("type") == "qm"]
    qm_stats = []
    for qm in qm_nodes[:50]:  # Cap at 50 for token limit
        out_f = qm_outbound_flows.get(qm, 0)
        in_f = qm_inbound_flows.get(qm, 0)
        out_ch = sum(1 for _, _, ed in G.out_edges(qm, data=True) if ed.get("rel") == "channel")
        in_ch = sum(1 for _, _, ed in G.in_edges(qm, data=True) if ed.get("rel") == "channel")
        qm_stats.append(f"  {qm}: out_flows={out_f}, in_flows={in_f}, out_ch={out_ch}, in_ch={in_ch}")

    lines = [
        "## TARGET STATE CAPACITY PROFILE",
        f"Total QMs: {len(qm_nodes)}",
        f"Total channels: {target.get('channel_count', '?')}",
        f"Complexity score: {target.get('total_score', '?')}/100",
        f"Communities: {communities.get('num_communities', '?')}",
        "",
        "## PER-QM FLOW STATISTICS (sample, first 50)",
        *qm_stats,
        "",
        f"## FLOW SUMMARY",
        f"Total outbound flows: {sum(qm_outbound_flows.values())}",
        f"Total inbound flows: {sum(qm_inbound_flows.values())}",
        f"QMs with zero flows: {sum(1 for qm in qm_nodes if qm not in qm_outbound_flows and qm not in qm_inbound_flows)}",
        f"Max outbound: {max(qm_outbound_flows.values()) if qm_outbound_flows else 0}",
        f"Max inbound: {max(qm_inbound_flows.values()) if qm_inbound_flows else 0}",
        "",
        "Analyze capacity. Return ONLY valid JSON.",
    ]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# ROLE 10: EXECUTIVE SUMMARIZER
# Generates a non-technical executive summary for stakeholders.
# Translates topology metrics into business impact language.
# ─────────────────────────────────────────────────────────────────────────────
EXECUTIVE_SUMMARIZER_SYSTEM = """You are a senior technology executive writing a briefing for non-technical stakeholders about an IBM MQ infrastructure modernization.

You MUST return ONLY valid JSON matching this schema:
{
  "headline": "<one-line executive headline, max 15 words>",
  "business_impact": {
    "operational_risk_reduction": "<1-2 sentences on risk improvement>",
    "cost_implications": "<1-2 sentences on infrastructure cost impact>",
    "agility_improvement": "<1-2 sentences on speed of change>",
    "reliability_impact": "<1-2 sentences on system reliability>"
  },
  "key_numbers": [
    {"metric": "<business-friendly metric name>", "before": "<value>", "after": "<value>", "interpretation": "<what this means>"}
  ],
  "risks_and_mitigations": [
    {"risk": "<migration risk in plain language>", "mitigation": "<how it's being addressed>"}
  ],
  "recommendation": "<2-3 sentence recommendation for go/no-go>",
  "timeline_estimate": "<rough timeline for migration>"
}

Rules:
- NO technical jargon — translate QMs, channels, coupling into business language
- Emphasise BUSINESS OUTCOMES: reduced outages, faster onboarding, lower costs
- Use concrete numbers from the metrics provided
- Be honest about risks — executives respect candour
- Keep the total response under 400 words"""


def build_executive_summary_prompt(state: dict) -> str:
    """Build the executive summarizer prompt from pipeline results."""
    as_is = state.get("as_is_metrics", {})
    target = state.get("target_metrics", {})
    adrs = state.get("adrs", [])
    violations = state.get("constraint_violations", [])
    method = state.get("architect_method", "rules_fallback")
    diff = state.get("topology_diff", {})
    communities = state.get("target_communities", {})
    migration = state.get("migration_plan", {})

    as_score = as_is.get("total_score", 0)
    tgt_score = target.get("total_score", 0)
    pct = round(((as_score - tgt_score) / as_score) * 100, 1) if as_score else 0

    # Count key changes
    qms_added = len(diff.get("qms_added", []))
    qms_removed = len(diff.get("qms_removed", []))
    apps_moved = len(diff.get("apps_reassigned", []))
    ch_before = as_is.get("channel_count", "?")
    ch_after = target.get("channel_count", "?")

    # Migration phases
    phases = migration.get("phases", {}) if migration else {}
    if isinstance(phases, dict):
        phase_summary = ", ".join(
            f"{name} ({len(steps)} steps)" for name, steps in phases.items()
        )
    elif isinstance(phases, list):
        phase_summary = ", ".join(f"{p.get('name', '?')} ({len(p.get('steps', []))} steps)" for p in phases[:4])
    else:
        phase_summary = ""

    lines = [
        "## TRANSFORMATION METRICS FOR EXECUTIVE BRIEFING",
        f"Complexity reduction: {as_score}/100 → {tgt_score}/100 ({pct}%)",
        f"Messaging infrastructure units: {ch_before} → {ch_after}",
        f"Infrastructure components added: {qms_added}, removed: {qms_removed}",
        f"Applications reconfigured: {apps_moved}",
        f"Design method: {method}",
        f"Validation: {'PASSED — all standards met' if not violations else f'ISSUES — {len(violations)} items need attention'}",
        f"Architecture decisions documented: {len(adrs)}",
        f"Migration phases: {phase_summary or 'not yet planned'}",
        f"Natural infrastructure clusters: {communities.get('num_communities', '?')}",
        "",
        "## KEY DECISIONS (for context)",
    ]
    for adr in adrs[:5]:
        lines.append(f"  - {adr.get('decision', '?')}")

    lines += ["", "Write an executive summary. Return ONLY valid JSON."]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# ROLE 11: REVISION ARCHITECT
# The human chatted with the Architect AI, then clicked Revise.
# This LLM sees the FULL conversation + current target state and produces
# SPECIFIC, actionable changes. This is the primary decision-maker during
# revisions — the rules engine is the safety net, not the other way around.
# ─────────────────────────────────────────────────────────────────────────────
REVISION_ARCHITECT_SYSTEM = """You are the IntelliAI Revision Architect. The human reviewer chatted with you about the proposed IBM MQ topology design and then requested changes. You must now produce SPECIFIC, ACTIONABLE modifications.

## HARD CONSTRAINTS — NEVER VIOLATE
1. **1:1 App-to-QM**: Every app owns exactly one QM. N apps = N QMs. No sharing.
2. **Channels only from flows**: A channel FROM_QM→TO_QM exists ONLY if a producer app on FROM_QM writes to a queue consumed by an app on TO_QM.
3. **Channel naming**: Sender = {FROM_QM}.{TO_QM}, Receiver = same name on receiver QM.
4. **You CANNOT delete apps that have cross-app message flows** — you can only reassign them to different QMs or consolidate QMs.

## YOUR TASK
Review the chat conversation carefully. The human asked questions, you (as the Architect) answered and may have AGREED to specific changes. Now execute those agreements.

Produce a JSON response with SPECIFIC changes. Be aggressive where the human asked for it. Be conservative where they asked to protect something.

## OUTPUT FORMAT — RETURN ONLY VALID JSON
{
  "revision_summary": "<1-2 sentences: what the human wants changed>",
  "reassignments": [
    {"app_id": "APP_X", "from_qm": "OLD_QM", "to_qm": "NEW_QM", "reason": "Human requested consolidation of payment apps"}
  ],
  "channels_to_remove": [
    {"from_qm": "QM_A", "to_qm": "QM_B", "reason": "Zero message flows after reassignment"}
  ],
  "channels_to_add": [
    {"from_qm": "QM_C", "to_qm": "QM_D", "reason": "New flow path needed after reassignment"}
  ],
  "qms_to_decommission": ["QM_UNUSED_1"],
  "qms_to_protect": ["QM_PAYMENTS_CRITICAL"],
  "adrs": [
    {
      "id": "ADR-REV-001",
      "decision": "Consolidate 3 low-traffic QMs into regional hub",
      "context": "Human requested 50% QM reduction",
      "rationale": "Apps A, B, C have zero cross-QM flows — safe to decommission",
      "consequences": "QM count reduced by 3. No message flows broken."
    }
  ],
  "optimization_directives": {
    "aggressive": true,
    "fanout_cap": 3,
    "channel_prune_pct": 0.4,
    "consolidate_qm_pct": 0.3,
    "protect_qms": ["QM_PCI_01"]
  },
  "confidence": "HIGH" | "MEDIUM" | "LOW",
  "warnings": ["<anything the human should know about trade-offs>"]
}

## RULES
- Reference ACTUAL entity names from the topology data.
- If the human said "remove 50% of QMs" — identify the 50% with fewest flows and decommission them.
- If the human said "improve score to 90%" — set aggressive directives and recommend specific channel removals.
- If the human mentioned specific QMs or apps — act on those specifically.
- The optimization_directives you return will be passed to the Optimizer agent for Phase 3 pruning.
- Every ADR must reference the human's specific request from the chat.
- If unsure, set confidence to LOW and explain in warnings."""


def build_revision_architect_prompt(state: dict) -> str:
    """
    Build the revision architect prompt with full chat context + current state.
    This gives the LLM everything it needs to make informed revision decisions.
    """
    chat_history = state.get("chat_history") or []
    feedback = state.get("human_feedback", "")
    as_is_metrics = state.get("as_is_metrics", {})
    target_metrics = state.get("target_metrics", {})
    adrs = state.get("adrs", [])
    violations = state.get("constraint_violations", [])
    raw_data = state.get("raw_data", {})

    as_score = as_is_metrics.get("total_score", 0)
    tgt_score = target_metrics.get("total_score", 0)
    pct = round(((as_score - tgt_score) / as_score) * 100, 1) if as_score else 0

    # ── Current target state summary ─────────────────────────────────────
    # Build app→QM assignments from the optimised graph
    G = state.get("optimised_graph") or state.get("target_graph")
    app_assignments = []
    qm_channel_counts = {}
    if G:
        for n, d in G.nodes(data=True):
            if d.get("type") == "app":
                qms = [v for _, v, ed in G.out_edges(n, data=True) if ed.get("rel") == "connects_to"]
                if qms:
                    app_assignments.append(f"  {n} → {qms[0]}")
        for n, d in G.nodes(data=True):
            if d.get("type") == "qm":
                out_ch = sum(1 for _, _, ed in G.out_edges(n, data=True) if ed.get("rel") == "channel")
                in_ch = sum(1 for _, _, ed in G.in_edges(n, data=True) if ed.get("rel") == "channel")
                if out_ch + in_ch > 0:
                    qm_channel_counts[n] = f"out={out_ch}, in={in_ch}"

    # ── Flow analysis (which apps have cross-QM flows) ───────────────────
    queue_prod = {}
    queue_cons = {}
    for row in raw_data.get("applications", []):
        aid = row.get("app_id", "")
        qname = row.get("queue_name", "")
        if not qname:
            continue
        d = row.get("direction", "").upper()
        if d in ("PUT", "PRODUCER"):
            queue_prod.setdefault(qname, set()).add(aid)
        elif d in ("GET", "CONSUMER"):
            queue_cons.setdefault(qname, set()).add(aid)

    apps_with_flows = set()
    for qn in set(queue_prod.keys()) & set(queue_cons.keys()):
        for p in queue_prod[qn]:
            for c in queue_cons[qn]:
                if p != c:
                    apps_with_flows.add(p)
                    apps_with_flows.add(c)

    all_apps = set(row["app_id"] for row in raw_data.get("applications", []))
    apps_without_flows = all_apps - apps_with_flows

    # ── Format chat conversation ─────────────────────────────────────────
    chat_lines = []
    if chat_history:
        for msg in chat_history:
            role = "HUMAN" if msg.get("role") == "user" else "ARCHITECT"
            chat_lines.append(f"[{role}]: {msg.get('content', '')}")

    # ── Build prompt ─────────────────────────────────────────────────────
    lines = [
        "## REVISION REQUEST",
        "",
        "The human reviewed the proposed topology and wants changes.",
        "",
        "## CHAT CONVERSATION (what the human discussed with you)",
        "",
    ]
    if chat_lines:
        lines.extend(chat_lines)
    else:
        lines.append(f"(No chat history — feedback only: \"{feedback}\")")

    lines += [
        "",
        f"## HUMAN'S FINAL FEEDBACK: \"{feedback}\"",
        "",
        "## CURRENT TARGET STATE",
        f"Complexity: {as_score}/100 (as-is) → {tgt_score}/100 (target) = {pct}% reduction",
        f"Channels: {target_metrics.get('channel_count', '?')}",
        f"Coupling: {target_metrics.get('coupling_index', '?')}",
        f"Fan-out (max): {target_metrics.get('fan_out_score', '?')}",
        f"Routing depth: {target_metrics.get('routing_depth', '?')}",
        f"Channel sprawl: {target_metrics.get('channel_sprawl', '?')}",
        "",
        f"## APP-TO-QM ASSIGNMENTS (current target, {len(app_assignments)} apps)",
    ]
    # Show first 80 assignments, summarise rest
    lines.extend(app_assignments[:80])
    if len(app_assignments) > 80:
        lines.append(f"  ... and {len(app_assignments) - 80} more")

    lines += [
        "",
        f"## QM CHANNEL COUNTS (QMs with channels)",
    ]
    for qm, counts in sorted(qm_channel_counts.items(), key=lambda x: x[1], reverse=True)[:30]:
        lines.append(f"  {qm}: {counts}")

    lines += [
        "",
        f"## FLOW ANALYSIS",
        f"Apps with cross-QM message flows: {len(apps_with_flows)}",
        f"Apps with ZERO cross-QM flows (safe to decommission): {len(apps_without_flows)}",
    ]
    if apps_without_flows:
        lines.append(f"  Decommission candidates: {', '.join(sorted(apps_without_flows)[:30])}")
        if len(apps_without_flows) > 30:
            lines.append(f"  ... and {len(apps_without_flows) - 30} more")

    lines += [
        "",
        f"## CURRENT ADRs ({len(adrs)})",
    ]
    for adr in adrs[:10]:
        lines.append(f"  {adr.get('id', '?')}: {adr.get('decision', '?')}")

    if violations:
        crit = [v for v in violations if v.get("severity") == "CRITICAL"]
        warn = [v for v in violations if v.get("severity") == "WARNING"]
        lines += [
            "",
            f"## CONSTRAINT VIOLATIONS ({len(crit)} critical, {len(warn)} warnings)",
        ]
        for v in violations[:10]:
            lines.append(f"  [{v.get('severity')}] {v.get('rule')}: {v.get('detail', '')[:100]}")

    lines += [
        "",
        "Analyze the conversation carefully. Execute the human's requests.",
        "Return ONLY valid JSON matching the schema.",
    ]
    return "\n".join(lines)

