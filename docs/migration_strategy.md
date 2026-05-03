# Migration Strategy — IntelliAI / MQ-TITAN Phase 1

**Audience:** IBM MQ veterans, panel reviewers, Wells Fargo platform operators.
**Scope:** This document explains *how* the target topology produced by Phase 1
can be migrated to safely, one application at a time. It is the answer to the
question:

> *"If you have any mainframe apps or any other apps, how will you migrate
> one app at a time?"*

Phase 1 produces the migration-safe blueprint. Phase 2 executes it. This
document specifies the mechanism, the per-app classification taxonomy, the
independence property, and the constraints we explicitly do not solve.

---

## 1. The Mechanism — IBM MQ XMITQ + Remote Queue

One-app-at-a-time migration uses the standard IBM MQ Distributed Queuing
pattern:

- Each application's queues live on a **dedicated queue manager** (the strict
  1:1 ownership constraint enforced by the IntelliAI rules engine).
- Traffic between QMs flows over **sender/receiver channel pairs** with a
  transmission queue (XMITQ) on the source side.
- During an app's cutover window, traffic to its queues is **routed
  transparently** through the original QM via XMITQ + Remote Queue
  definitions on the source QM pointing at the new target QM.
- **The application code does not need to change** — only its `CONNAME`
  configuration. Apps reconnect all the time during cutover (that is the
  entire point of CCDT and reconnection-attempt config); what does *not*
  change is application source or build artifacts. Apps that hardcode
  the source QM name into deployment artifacts (JNDI bindings, hardcoded
  factory names) are flagged `PINNED_REVIEW` because they require code
  change.
- **Channel naming convention:** `<source_qm>.<target_qm>` (dot-only).
  IBM MQ enforces a 20-character channel-name limit; the dot separator
  is preferred over `.TO.` because it consumes one character instead of
  three, leaving more headroom for descriptive QM names. The provisioner
  emits this convention consistently in both `{qm}_target.mqsc` and
  `{qm}_diff.mqsc`.

This is operationally familiar to any MQ veteran. We are not inventing a
new migration mechanism; we are producing a topology blueprint that allows
the standard mechanism to work safely.

For ops teams: the provisioner emits two scripts per QM. `{qm}_target.mqsc`
is the full target state with `REPLACE` on every DEFINE — use this on
greenfield QMs or when the migration window allows full reapplication.
`{qm}_diff.mqsc` shows only the channel deltas — adds, removes — using
`STOP CHANNEL ... MODE(QUIESCE)` for in-flight message draining and
`NOREPLACE` on additions (so a manually-created channel of the same name
surfaces as a conflict instead of being silently overwritten). On a live
QM, run the diff. The full target script is for cold deployments.

---

## 2. App Classification — Four Migration Classes

Every app in the target topology is classified into exactly one of four
migration classes. The classification appears:
- on the app's node in the target topology graph (visible as a colored
  badge in the Topology UI when filtering),
- in the per-app rows of the **Migration Safety Analysis** panel,
- in the `target_migration_classification.csv` export inside the
  Evidence Bundle.

### 2.1 TCP_CLIENT (default, fully supported)

Apps using IBM MQ client over TCP/IP. This includes:

- Java/JMS clients
- C/C++ clients via `libmqic`
- .NET clients via `amqmdnet.dll`
- **Mainframe apps using MQ client over TCP from z/OS** — modern z/OS apps
  with TCP connectivity to distributed MQ are TCP_CLIENT, not bindings.

**Migration mechanism:** Update the app's `CONNAME` configuration to point
at the new target QM. The application binary does not change. Connection
string (`QMGR/CONNAME/CHANNEL`) is the only mutable artifact.

**This is the supported, default path.** In Phase 1 absent contrary signals,
every app is classified as TCP_CLIENT.

### 2.2 BINDINGS (special handling required)

Apps using MQ in **bindings mode** — a process running on the same OS
as its queue manager, accessing it via shared memory and OS IPC rather
than over TCP. Common in legacy z/OS environments:

- **CICS regions** binding to z/OS QM
- **IMS bridge**
- **Batch jobs** using `MQGET`/`MQPUT` against a local QM
- **FASTPATH-bound apps** using `MQCNO_FASTPATH_BINDING` for performance:
  these are bindings-mode apps with an additional cost — the
  re-architecture to TCP client mode typically incurs a 10–30%
  throughput hit because FASTPATH bypasses the queue manager's process
  boundary. Migration is still possible; the throughput trade-off
  needs explicit acknowledgment.

Bindings-mode apps **cannot be migrated by changing CONNAME — they don't
have one.** They communicate via OS-level mechanisms tied to the
co-located QM.

**Migration mechanism:** Application redeployment to TCP-client mode is
required *before* infrastructure migration. This is an application-team
ticket, not an infrastructure ticket.

**Phase 1 detection:** Heuristic on the `hosting_type` field. Apps where
`hosting_type` contains `MAINFRAME`, `Z/OS`, or `ZOS` are flagged as
BINDINGS. The classification reason explicitly states this is a heuristic
and requires manual confirmation, because **a mainframe app using MQ
client over TCP looks identical in our current data model.** This is
honest about the data we have.

### 2.3 SNA_OUT_OF_SCOPE

Apps connecting via legacy **SNA / LU 6.2** transports to mainframe QMs.

**We do not migrate these.** The class exists so the panel can ask "what
about SNA?" and we can answer "we have a class for that, and they are
explicitly out-of-scope for this migration."

The standard treatment is to **first migrate them onto TCP** (an
application-team project), then run them through the IntelliAI pipeline as
TCP_CLIENT.

**Phase 1 detection:** Not auto-populated. The data model does not yet
include transport-protocol fields. Phase 2 ingests CHLAUTH dumps and
SSLPEERMAP rules to populate this class authoritatively.

### 2.4 PINNED_REVIEW

Apps that hardcode something the migration would change. Specifically:

- SSL certificates pinned to a specific source QM's CN
- Hardcoded IP addresses instead of DNS hostnames
- Connection logic that explicitly references the source QM name
  (e.g. config strings like `MQ_SRC_PROD_QM` parsed at runtime)
- CHLAUTH SSLPEERMAP rules tied to source QM identity

Such apps **cannot be migrated transparently.** Manual review and
application-side change is required.

**Phase 1 detection:** Not auto-populated. Detection requires CHLAUTH
parsing, SSLPEERMAP analysis, and app-config audits — Phase 2 work.

### 2.5 What we explicitly say about the un-populated classes

We do **not** populate SNA_OUT_OF_SCOPE or PINNED_REVIEW from heuristics
that would produce false positives. A panel reviewer asking "did you
detect any SNA apps?" gets the honest answer: *"Our current data model
does not include the signals required to detect SNA. The taxonomy
includes the class — Phase 2 ingests CHLAUTH for authoritative
population."* Better than fake-flagging apps to look smart.

**Read counts of zero as "undetected, not zero."** A `SNA_OUT_OF_SCOPE: 0`
or `PINNED_REVIEW: 0` in the panel does **not** mean we checked and found
none. It means we don't have the data to detect them in Phase 1. The
panel notes block calls this out so a reviewer doesn't mistake absence
of detection for absence of the underlying issue.

---

## 3. Per-App Independence — The Mathematical Safety Property

**Claim:** Under the strict 1:1 app-to-QM ownership constraint, every app
in the target topology is independently migratable.

**Proof sketch (verified at the blueprint level in Phase 1; verified in
TLA+ as `PerAppRollbackLocality` in Phase 2):**

1. The Phase 1 rules engine (`_build_target_rules` in `agents.py`) and the
   `_enforce_single_qm` invariant guarantee that each app has exactly one
   `connects_to` edge to exactly one QM.
2. Each QM is owned by exactly one app — by construction. If two apps
   prefer the same QM, the higher-affinity app keeps the original name and
   the other gets a new dedicated QM (`QM_{APP_ID}`).
3. Channels (sender/receiver pairs) are added only between QMs whose owning
   apps have a producer/consumer relationship on a shared queue name in
   the as-is data.

From these three properties:
- Migrating App A1 only touches `QM_A1` and channels OUT of `QM_A1`.
- App A2's queues live on `QM_A2`, and A2's connections are unaffected by
  any change to `QM_A1`.
- A failure during A1's migration only affects A1's drain window.
  A2 through An continue producing and consuming normally.
- Rollback is per-app: undo the MQSC changes that affected `QM_A1` and
  A1 alone.

This is the property the Phase 2 TLA+ specification will encode as
`PerAppRollbackLocality`. Phase 1 surfaces the dependency graph that
*proves no app blocks another's migration*; Phase 2 proves the runtime
mechanism preserves that property under failures.

**Informal statement of the invariant** (for a reviewer who asks
"have you specified PerAppRollbackLocality even informally?"):

> *PerAppRollbackLocality:* For any cutover step targeting app A, the
> post-step state restricted to apps `{1..n} \ A` (every app *except*
> A) equals the pre-step state restricted to `{1..n} \ A`. In words:
> a single-app cutover changes the world only for that one app; the
> migration-relevant state of every other app is bit-identical before
> and after.

This is the prose form of what Phase 2 will mechanically check in TLA+.
Phase 1 cannot verify it (no execution semantics), but the topology
constraints listed above give it a *necessary* substrate to hold.

**Caveat — QM-locality is a necessary, not sufficient, independence
condition.** Two apps on different QMs can still share a clustered
DLQ, a common authorization namespace, or a CHLAUTH grouping that
creates a hidden dependency. Phase 1 has no data on those; we report
"independent" based on QM-locality only. Phase 2's Business Context
Translator ingests cluster resources, common namespaces, and CHLAUTH
groupings to tighten the claim.

### What "independence" reports in the IntelliAI output

For each app, the Migration Safety panel reports:

- `migration_independent: true | false`
- `dependency_cluster: [app_id, ...]` — which apps must move together
  (the app itself is always in this list; size 1 means independent)

In strict 1:1 mode, every cluster has size 1 and every app is independent.

The cluster size > 1 case exists for **Phase 2 multi-tenant cutover
scenarios** — when an app is temporarily co-located with another during
incremental migration. The same code computes cluster size correctly in
both cases.

---

## 4. Drain Window Estimates

For each app the panel reports `estimated_drain_window_s`. This is a
**rough budget**, not a measurement:

```
window = 10s (base: channel start, listener bind, sanity DISPLAY commands)
       + 5s × (outbound channels from this app's QM)
       + 2s × (local queues on this app's QM)
window = min(window, 120s)
```

The constants 10/5/2 are **gut-tuned, not calibrated against
production drain rates** — they are placeholders that produce sensible
numbers for typical app sizes. The cap at 120 seconds (two minutes)
exists because the formula scales linearly with channel and queue count;
on outlier QMs that own hundreds of queues, an uncapped estimate would
read as 400+ seconds and mislead an operator into thinking a single-app
cutover is half an hour. Long-tail QMs go through scheduled migration
windows, not flash cutover, and the operational planning happens
outside this estimate.

This is intentionally conservative and intentionally imprecise. **Phase 2
replaces it with real-time measurement** via `DISPLAY QSTATUS(*) IPPROCS
CURDEPTH` against the source QM during the cutover scheduling step.

Surfacing this as an estimate now lets ops teams *plan* — "App A1 needs
about a 30-second window" — without claiming false precision.

---

## 5. XA Transactions — Documented Constraint

Cross-QM XA two-phase-commit transactions require XA recovery state
inquiry during cutover. They are **out of scope for Phase 2 as well**
in the current plan.

The migration window for an app participating in cross-QM XA must align
with periods where no XA transaction is in flight. Most Wells Fargo apps
do not use cross-QM XA. We **document this constraint** rather than
pretending to solve it.

If the panel asks "what about XA?", the answer is:

> "XA recovery across QMs is documented as an out-of-scope constraint
> for the infrastructure migration. The standard pattern is to schedule
> cutover during XA-quiescent windows. We don't claim to detect or
> automate XA-aware scheduling in Phase 1 or 2 — that's a third-phase
> ticket if Wells Fargo decides it's worth investing in."

---

## 6. The Verbal Answer (~60 seconds)

To the question *"If you have any mainframe apps or any other apps, how
will you migrate one app at a time?"*:

> Migration is one-app-at-a-time by design. The mechanism is the standard
> IBM MQ XMITQ + Remote Queue pattern with sender/receiver channels —
> each app gets its dedicated QM, traffic routes transparently via the
> original QM during cutover, and the application code does not need to
> change; only its CONNAME configuration. A typical drain window is
> roughly thirty seconds, scaling with outbound channel count and queue
> count, and shown per-app in the Migration Safety panel — but Phase 2
> measures real drain rates against the source QM during cutover rather
> than relying on this estimate.
>
> For mainframe apps specifically, we classify by client transport
> rather than by platform. Mainframe apps using MQ client over TCP are
> treated identically to distributed TCP clients. Mainframe apps using
> bindings mode — CICS, IMS, batch on z/OS — require application
> redeployment first; we flag those as BINDINGS, including FASTPATH-
> bound apps which face additional throughput considerations on TCP
> migration. Mainframe apps using legacy SNA transports are flagged
> out-of-scope. Channel security — CHLAUTH and SSL/TLS on the new
> channel pairs — is part of the per-channel MQSC output and verified
> at provisioning; migration safety classifies the application,
> channel security verifies the channel.
>
> Per-app rollback locality is what makes this safe. Phase 2 will verify
> it formally in TLA+; Phase 1 surfaces the dependency graph that
> demonstrates no app blocks another's migration.
>
> The piece I am least confident about is whether our hosting_type
> heuristic over-flags mainframe-over-TCP apps as BINDINGS. In production
> data we would validate that against a sample of twenty to thirty
> mainframe apps before relying on the classification — Phase 2 ingests
> CHLAUTH and SSLPEERMAP rules to authoritatively populate the four
> classes.

---

*Document version: 1.0 — Phase 1 deliverable, included in the IntelliAI
evidence bundle as part of the per-session forensic export.*
