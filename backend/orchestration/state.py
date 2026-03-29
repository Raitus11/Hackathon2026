from typing import TypedDict, Optional, Any


class ComplexityMetrics(TypedDict):
    channel_count: float
    coupling_index: float
    routing_depth: float
    fan_out_score: float
    orphan_objects: float
    total_score: float


class ConstraintViolation(TypedDict):
    rule: str
    entity: str
    detail: str
    severity: str  # CRITICAL | WARNING


class ADR(TypedDict):
    id: str
    decision: str
    context: str
    rationale: str
    consequences: str


class MQTitanState(TypedDict):
    # ── Input ──────────────────────────────────────────
    session_id: str
    csv_paths: dict

    # ── Sanitiser output ───────────────────────────────
    raw_data: Optional[dict]
    data_quality_report: Optional[dict]

    # ── Researcher output ──────────────────────────────
    as_is_graph: Optional[Any]         # nx.DiGraph
    as_is_subgraphs: Optional[list]    # list of component dicts from analyse_subgraphs()
    as_is_communities: Optional[dict]  # Louvain community detection results
    as_is_centrality: Optional[dict]   # betweenness + degree centrality
    as_is_entropy: Optional[dict]      # Shannon entropy + density + clustering

    # ── Analyst output ─────────────────────────────────
    as_is_metrics: Optional[ComplexityMetrics]

    # ── Architect output ───────────────────────────────
    target_graph: Optional[Any]        # nx.DiGraph
    adrs: Optional[list]               # list[ADR]
    redesign_count: int
    human_feedback: Optional[str]      # feedback from human rejection
    architect_method: Optional[str]    # "llm" or "rules_fallback" — tracks which path was used

    # ── Optimizer output ───────────────────────────────
    optimised_graph: Optional[Any]     # nx.DiGraph
    target_metrics: Optional[ComplexityMetrics]
    target_subgraphs: Optional[list]   # list of component dicts from analyse_subgraphs()
    target_communities: Optional[dict] # Louvain community detection results
    target_centrality: Optional[dict]  # betweenness + degree centrality
    target_entropy: Optional[dict]     # Shannon entropy + density + clustering

    # ── Tester output ──────────────────────────────────
    validation_passed: bool
    constraint_violations: Optional[list]

    # ── Human review gate ──────────────────────────────
    awaiting_human_review: bool        # True when pipeline is paused
    human_approved: Optional[bool]     # True=approved False=rejected None=pending
    human_aborted: Optional[bool]      # True = human killed the pipeline entirely

    # ── Provisioner output ─────────────────────────────
    mqsc_scripts: Optional[list]
    target_csvs: Optional[dict]

    # ── Migration Planner output ───────────────────────
    migration_plan: Optional[dict]     # Ordered steps with forward/rollback MQSC
    topology_diff: Optional[dict]      # Before/after diff for review panel

    # ── Doc Expert output ──────────────────────────────
    final_report: Optional[str]

    # ── Error tracking ─────────────────────────────────
    error: Optional[str]
    messages: Optional[list]
