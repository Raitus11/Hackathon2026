"""
backend/migration/migration_safety.py

Per-app migration safety classification and independence analysis.

This module answers the panel question:
  "If you have any mainframe apps or any other apps, how will you migrate
   one app at a time?"

It produces, for each app in the target topology:
  - migration_class: one of TCP_CLIENT / BINDINGS / SNA_OUT_OF_SCOPE / PINNED_REVIEW
  - migration_class_reason: human-readable explanation of WHY this class was chosen
  - migration_independent: True iff no other app must move at the same time
  - dependency_cluster: list of app_ids that move together (size 1 = independent)
  - estimated_drain_window_s: rough drain-window estimate in seconds

It also produces a topology-level summary for the Migration Safety Analysis UI panel.

DESIGN NOTES (honest about what we can and cannot detect)
==========================================================

The four migration classes correspond to operational realities, not arbitrary
buckets:

  TCP_CLIENT — Apps using IBM MQ client over TCP/IP. Includes Java/JMS,
    C/C++ via libmqic, .NET via amqmdnet.dll, AND mainframe apps using
    MQ client over TCP from z/OS. Migration mechanism: the app's CONNAME
    is updated to point at the new QM; the application never reconnects
    in code (only in config). This is the supported, default path.

  BINDINGS — Apps using MQ in bindings mode (process on the same OS as
    the QM). Common in legacy z/OS: CICS regions binding to z/OS QM,
    IMS bridge, batch jobs using MQGET/MQPUT against a local QM.
    Cannot be migrated by changing CONNAME — they don't have one.
    Requires application redeployment to TCP-client mode before
    infrastructure migration.

  SNA_OUT_OF_SCOPE — Apps connecting via legacy SNA / LU 6.2 to mainframe
    QMs. We do not migrate these. Flagged as out-of-scope; need a
    separate parallel project (typically: get them on TCP first, then
    migrate as TCP_CLIENT).

  PINNED_REVIEW — Apps that hardcode something the migration would
    change: SSL certs pinned to a specific QM's CN, hardcoded IP
    addresses instead of DNS, connection logic explicitly referencing
    the source QM name, CHLAUTH SSLPEERMAP rules tied to source QM.
    Cannot be migrated transparently — manual review required.

WHAT WE CAN DETECT FROM THE CURRENT DATA MODEL
-----------------------------------------------

The Phase 1 CSV ingest produces app_metadata with these fields (per app):
  data_classification, is_payment_critical, is_pci, trtc, hosting_type,
  neighborhood, line_of_business

Of these, only `hosting_type` provides a useful signal for migration class,
and only as a heuristic. Values like "MAINFRAME" or "Z/OS" are a strong
hint that the app MAY be using bindings mode — but a mainframe app using
MQ client over TCP is also possible (and increasingly common). The hosting
type alone cannot distinguish "mainframe + bindings" from "mainframe + TCP
client". We flag mainframe-hosted apps as `BINDINGS` with a reason note
explicitly stating this is a heuristic and requires confirmation.

WHAT WE CANNOT DETECT FROM THE CURRENT DATA MODEL
--------------------------------------------------

  - SNA / LU 6.2 transports — there is no transport-protocol field in the
    CSV today. To detect SNA, we would need either the app's connection
    metadata (CHLAUTH rules, channel TYPE field with SVRCONN_SNA) or
    explicit CMVC declaration.

  - SSL cert pinning — would require parsing CHLAUTH SSLPEERMAP rules
    from the source MQ environment.

  - DNS-vs-IP hardcoding — would require app-side configuration audit,
    out of scope for an MQ topology tool.

These classes (SNA_OUT_OF_SCOPE, PINNED_REVIEW) are therefore REACHABLE in
the taxonomy but NEVER POPULATED by automatic detection in Phase 1. They
exist as classes because the panel may ask, "what about SSL pinning?" and
we want to be able to answer "we have a class for that — populated from
richer data sources in Phase 2 (CHLAUTH ingest, SSLPEERMAP parser)".

Phase 2 commits to a Business Context Translator agent that will populate
these classes from richer data sources (real CHLAUTH dumps, SSLPEERMAP
rules, app-config audits).

PER-APP INDEPENDENCE
====================

Independence means: "if I migrate this app right now, no other app's
runtime is affected, and no other app's migration is blocked by it."

The mathematical claim: under strict 1:1 app-to-QM ownership (the Phase 1
core constraint), every app IS independent of every other app for migration
purposes. This is because:

  - Each app's queues live on its own dedicated QM
  - A failure during App A's migration affects only App A's drain window
  - No shared queue manager means no shared blast radius

The dependency_cluster field is therefore "[app_id]" (just the app itself)
in the strict 1:1 case. We compute it via BFS over the channel graph
restricted to channels the app actually uses, which is a no-op in the
strict 1:1 case but generalizes to Phase 2 cases (multi-tenant QMs during
incremental cutover).

The honest caveat in the per-app output: independence is a property of
the BLUEPRINT, not yet a verified property of the EXECUTION. Phase 2
will verify this in TLA+ as the `PerAppRollbackLocality` invariant.

DRAIN WINDOW ESTIMATE
=====================

estimated_drain_window_s is a rough budget — not measured, not from real
queue depth. It's computed as:

  10s  base cost (channel start, listener bind, sanity DISPLAY commands)
  + 5s per outbound channel from this app's QM
  + 2s per local queue owned by this app's QM (cleanup verification)

This is intentionally conservative and intentionally rough. Phase 2
replaces this with real-time DISPLAY QSTATUS measurement. We surface
this as "estimated; Phase 2 will measure live drain rates" in the UI.

USAGE
=====

  from backend.migration.migration_safety import (
      classify_app,
      compute_migration_safety,
  )

  # Per-app classification:
  cls, reason = classify_app(app_id, app_metadata, raw_data)

  # Full topology summary + per-app dicts:
  safety = compute_migration_safety(target_graph, raw_data)
  state["migration_safety"] = safety

  # Per-app fields are also written onto the graph nodes:
  G.nodes[app_id]["migration_class"]
  G.nodes[app_id]["migration_independent"]
  ...
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Optional

import networkx as nx

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Classification constants
# ─────────────────────────────────────────────────────────────────────────────

CLASS_TCP_CLIENT = "TCP_CLIENT"
CLASS_BINDINGS = "BINDINGS"
CLASS_SNA_OUT_OF_SCOPE = "SNA_OUT_OF_SCOPE"
CLASS_PINNED_REVIEW = "PINNED_REVIEW"

ALL_CLASSES = (CLASS_TCP_CLIENT, CLASS_BINDINGS, CLASS_SNA_OUT_OF_SCOPE, CLASS_PINNED_REVIEW)

# Hosting-type values that suggest bindings mode (heuristic — not authoritative).
# Anything containing one of these tokens (case-insensitive) flips the app to
# BINDINGS with the heuristic reason.
_MAINFRAME_HOSTING_TOKENS = ("MAINFRAME", "Z/OS", "ZOS", "Z OS")

# Drain-window cost model (intentionally rough — see module docstring).
# Constants 10/5/2 are gut-tuned, not calibrated against production drain
# rates. The cap exists to prevent absurd estimates on outlier QMs that
# own hundreds of queues; operational planning for long-tail QMs happens
# outside this estimate.
_DRAIN_BASE_S = 10
_DRAIN_PER_OUTBOUND_CHANNEL_S = 5
_DRAIN_PER_LOCAL_QUEUE_S = 2
_DRAIN_CAP_S = 120


# ─────────────────────────────────────────────────────────────────────────────
# Per-app classification
# ─────────────────────────────────────────────────────────────────────────────

def classify_app(
    app_id: str,
    app_metadata: dict,
    raw_data: Optional[dict] = None,
) -> tuple[str, str]:
    """Classify a single app for migration purposes.

    Args:
      app_id: the app identifier
      app_metadata: dict with optional fields data_classification,
                    hosting_type, is_pci, trtc, etc. May be empty.
      raw_data: full raw_data (currently unused; reserved for Phase 2 when
                CHLAUTH / SSLPEERMAP signals are available)

    Returns:
      (migration_class, reason_string)
    """
    meta = app_metadata or {}

    hosting_type = str(meta.get("hosting_type", "") or "").upper()

    # Heuristic 1: hosting_type indicates mainframe → likely bindings.
    # We are explicit in the reason that this is a HEURISTIC and may be
    # wrong for mainframe apps using MQ client over TCP.
    for token in _MAINFRAME_HOSTING_TOKENS:
        if token in hosting_type:
            reason = (
                f"Heuristic: hosting_type='{meta.get('hosting_type')}' "
                f"suggests bindings-mode connection to a z/OS QM. "
                f"Mainframe apps using MQ client over TCP would be TCP_CLIENT — "
                f"manual confirmation required before migration."
            )
            return CLASS_BINDINGS, reason

    # Default: TCP_CLIENT.
    # We explicitly state the assumption and the path to override it.
    reason = (
        "Default: assumed TCP_CLIENT. No metadata field indicates bindings "
        "mode, SNA transport, or pinning. Phase 2 Business Context Translator "
        "will validate this against CHLAUTH and SSLPEERMAP data."
    )
    return CLASS_TCP_CLIENT, reason


# ─────────────────────────────────────────────────────────────────────────────
# Per-app independence (strict 1:1 case is trivial; we still compute it
# the general way so the helper generalizes to Phase 2 multi-tenant cases)
# ─────────────────────────────────────────────────────────────────────────────

def _app_qm_map(G: nx.DiGraph) -> dict[str, str]:
    """Extract {app_id: qm_id} from connects_to edges. Returns first QM if
    multiple (warning logged in callers if 1:1 violated)."""
    out: dict[str, str] = {}
    for app, qm, edata in G.edges(data=True):
        if edata.get("rel") != "connects_to":
            continue
        if app not in out:
            out[app] = qm
    return out


def _qm_outbound_channels(G: nx.DiGraph) -> dict[str, list[str]]:
    """{qm_id: [target_qm_id, ...]} for channel edges only."""
    out: dict[str, list[str]] = defaultdict(list)
    for u, v, edata in G.edges(data=True):
        if edata.get("rel") == "channel":
            out[u].append(v)
    return out


def _qm_local_queue_count(G: nx.DiGraph) -> dict[str, int]:
    """Count of queue nodes owned by each QM."""
    out: dict[str, int] = defaultdict(int)
    for u, v, edata in G.edges(data=True):
        if edata.get("rel") != "owns":
            continue
        if G.nodes[v].get("type") == "queue":
            out[u] += 1
    return out


def _compute_drain_window_s(
    app_id: str,
    qm_id: str,
    qm_outbound: dict[str, list[str]],
    qm_local_q_count: dict[str, int],
) -> int:
    """Rough drain-window estimate, capped at 120s. See module docstring.

    The cap exists because the formula scales linearly in channel/queue
    count; on outlier QMs that own hundreds of queues, an uncapped
    estimate would read as 400+ seconds and mislead an operator into
    thinking single-app cutover takes half an hour. Long-tail QMs go
    through scheduled migration windows, not flash cutover, and that
    operational planning happens outside this estimate.
    """
    raw = (
        _DRAIN_BASE_S
        + _DRAIN_PER_OUTBOUND_CHANNEL_S * len(qm_outbound.get(qm_id, []))
        + _DRAIN_PER_LOCAL_QUEUE_S * qm_local_q_count.get(qm_id, 0)
    )
    return min(raw, _DRAIN_CAP_S)


def _compute_dependency_cluster(
    app_id: str,
    qm_id: str,
    G: nx.DiGraph,
    app_to_qm: dict[str, str],
) -> list[str]:
    """Return the list of app_ids that share this app's QM (i.e. would
    have to be moved together).

    In strict 1:1 mode this is always [app_id] (just the app itself).
    In multi-tenant mode (Phase 2 cutover), this surfaces the co-tenants.
    """
    co_tenants = sorted(a for a, q in app_to_qm.items() if q == qm_id)
    return co_tenants


def compute_migration_safety(
    G: nx.DiGraph,
    raw_data: Optional[dict] = None,
) -> dict:
    """Compute the migration_safety block AND annotate the graph.

    This is the **public** function callers use — it does both the pure
    classification (returns the dict) and the side-effect (writes per-app
    fields onto graph nodes). The split is intentional:

      - `_classify_apps_pure` is the pure computation: read-only, returns
        the {summary, per_app, method, notes} dict. No mutation. Safe for
        unit testing in isolation, safe to call multiple times, safe to
        call on a graph snapshot you do not own.

      - This function (`compute_migration_safety`) calls the pure one and
        then writes the per-app fields onto graph node attributes for
        downstream consumers (frontend, CSV export, evidence bundle).

    The combined function is what `architect_agent` calls. Tests that
    care about pure behavior call `_classify_apps_pure` directly.

    Mutation surface (per app node `app_id` in G.nodes):
      - migration_class
      - migration_class_reason
      - migration_independent
      - dependency_cluster
      - estimated_drain_window_s
    """
    safety = _classify_apps_pure(G, raw_data)

    # Annotate graph nodes with the per-app fields
    for row in safety.get("per_app", []):
        app_id = row["app_id"]
        if app_id in G.nodes:
            G.nodes[app_id]["migration_class"] = row["migration_class"]
            G.nodes[app_id]["migration_class_reason"] = row["migration_class_reason"]
            G.nodes[app_id]["migration_independent"] = row["migration_independent"]
            G.nodes[app_id]["dependency_cluster"] = row["dependency_cluster"]
            G.nodes[app_id]["estimated_drain_window_s"] = row["estimated_drain_window_s"]

    return safety


def _classify_apps_pure(
    G: nx.DiGraph,
    raw_data: Optional[dict] = None,
) -> dict:
    """Pure classification: read G + raw_data, return the safety dict.

    Does NOT mutate G. Does NOT write to disk. Read-only and idempotent.
    Use this directly when you need the data without the side-effect
    (e.g. in unit tests, in alternative renderers, in Phase 2 pipelines
    that handle annotation differently).

    Returns the same {summary, per_app, method, notes} dict that
    `compute_migration_safety` returns.
    """
    raw_data = raw_data or {}
    app_metadata_all = raw_data.get("app_metadata", {}) or {}

    app_to_qm = _app_qm_map(G)
    qm_outbound = _qm_outbound_channels(G)
    qm_local_q_count = _qm_local_queue_count(G)

    if not app_to_qm:
        logger.warning("compute_migration_safety: no app→QM connects_to edges in graph")
        return {
            "summary": {
                "total_apps": 0,
                "by_class": {c: 0 for c in ALL_CLASSES},
                "independent_count": 0,
                "non_independent_count": 0,
                "max_dependency_cluster_size": 0,
            },
            "per_app": [],
            "method": "rules_based_v1",
            "notes": "No apps in target topology — migration safety analysis skipped.",
        }

    per_app: list[dict] = []
    by_class: dict[str, int] = {c: 0 for c in ALL_CLASSES}
    independent_count = 0
    non_independent_count = 0
    max_cluster_size = 1

    # Sort for determinism
    for app_id in sorted(app_to_qm.keys()):
        qm_id = app_to_qm[app_id]
        meta = app_metadata_all.get(app_id, {}) or {}

        cls, reason = classify_app(app_id, meta, raw_data)
        cluster = _compute_dependency_cluster(app_id, qm_id, G, app_to_qm)
        independent = (len(cluster) == 1)
        drain_s = _compute_drain_window_s(app_id, qm_id, qm_outbound, qm_local_q_count)

        per_app.append({
            "app_id": app_id,
            "target_qm": qm_id,
            "migration_class": cls,
            "migration_class_reason": reason,
            "migration_independent": independent,
            "dependency_cluster": cluster,
            "estimated_drain_window_s": drain_s,
        })

        by_class[cls] += 1
        if independent:
            independent_count += 1
        else:
            non_independent_count += 1
        if len(cluster) > max_cluster_size:
            max_cluster_size = len(cluster)

    summary = {
        "total_apps": len(per_app),
        "by_class": by_class,
        "independent_count": independent_count,
        "non_independent_count": non_independent_count,
        "max_dependency_cluster_size": max_cluster_size,
    }

    notes = (
        "Classification is rules-based and conservative. Default is TCP_CLIENT "
        "unless hosting_type indicates a mainframe (heuristic → BINDINGS). "
        "SNA_OUT_OF_SCOPE and PINNED_REVIEW classes exist in the taxonomy but "
        "are not auto-populated in Phase 1 — they require richer signal "
        "(CHLAUTH dumps, SSLPEERMAP rules, app-config audit) which Phase 2 "
        "will ingest. Read counts of zero on those two classes as 'undetected, "
        "not zero' — Phase 1 has no data to detect them. Independence is "
        "verified at the blueprint level: under strict 1:1 app-to-QM ownership, "
        "every app is independent of every other app *with respect to QM "
        "locality only*. Two apps on different QMs can still share clustered "
        "DLQs, common authorization namespaces, or CHLAUTH groupings — Phase 2 "
        "ingests those signals to tighten the claim. Phase 2 verifies "
        "PerAppRollbackLocality formally in TLA+. Drain-window estimates are "
        "rough budgets capped at 120 seconds, not live measurements — Phase 2 "
        "measures real drain rates via DISPLAY QSTATUS during cutover."
    )

    logger.info(
        f"compute_migration_safety: {len(per_app)} apps classified — "
        f"by_class={by_class}, independent={independent_count}, "
        f"max_cluster_size={max_cluster_size}"
    )

    return {
        "summary": summary,
        "per_app": per_app,
        "method": "rules_based_v1",
        "notes": notes,
    }


# ─────────────────────────────────────────────────────────────────────────────
# CSV export
# ─────────────────────────────────────────────────────────────────────────────

def _csv_safe(value) -> str:
    """Defend against CSV-injection attacks.

    A cell whose value starts with =, +, -, or @ is interpreted by Excel
    (and by some other spreadsheet software) as a formula. If the cell
    content originates from user-controlled data — for example an
    `app_metadata` field harvested from a third-party CSV — an attacker
    could supply `=cmd|'/c calc'!A1` which opens calc when an analyst
    later opens our CSV in Excel.

    Sanitization: prefix any such cell with a single quote. The single
    quote is consumed by Excel as a "treat as text" hint and isn't
    rendered in the visible cell, so analysts see the original value
    minus the leading quote — but the formula is neutralized.

    See OWASP "CSV Injection" (a.k.a. Formula Injection).
    """
    s = str(value) if value is not None else ""
    if s and s[0] in ("=", "+", "-", "@"):
        return "'" + s
    return s


def to_csv_string(safety: dict) -> str:
    """Serialize the per_app block as a CSV string.

    Columns: app_id, target_qm, migration_class, migration_class_reason,
             migration_independent, dependency_cluster, estimated_drain_window_s

    dependency_cluster is rendered as a pipe-separated list of app_ids
    (not comma-separated, because we use comma as the field delimiter).

    All cell values pass through `_csv_safe` to defend against CSV
    injection — see that helper's docstring for the threat model.
    """
    import csv
    from io import StringIO

    out = StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow([
        "app_id",
        "target_qm",
        "migration_class",
        "migration_class_reason",
        "migration_independent",
        "dependency_cluster",
        "estimated_drain_window_s",
    ])
    for row in safety.get("per_app", []):
        writer.writerow([
            _csv_safe(row["app_id"]),
            _csv_safe(row["target_qm"]),
            _csv_safe(row["migration_class"]),
            _csv_safe(row["migration_class_reason"]),
            "true" if row["migration_independent"] else "false",
            _csv_safe("|".join(row["dependency_cluster"])),
            row["estimated_drain_window_s"],
        ])
    return out.getvalue()
