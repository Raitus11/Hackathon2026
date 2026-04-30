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
- **The application never reconnects in code** — only its `CONNAME`
  configuration changes. From the application's perspective, queues that
  used to be local are now remote, but `MQOPEN` / `MQPUT` / `MQGET`
  continue to work identically.

This is operationally familiar to any MQ veteran. We are not inventing a
new migration mechanism; we are producing a topology blueprint that allows
the standard mechanism to work safely.

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
```

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

## 6. The 90-Second Verbal Answer

To the question *"If you have any mainframe apps or any other apps, how
will you migrate one app at a time?"*:

> Migration is one-app-at-a-time by design. The mechanism is the standard
> IBM MQ XMITQ + Remote Queue pattern with sender/receiver channels —
> each app gets its dedicated QM, traffic gets routed transparently via
> the original QM during the cutover window, and the application never
> reconnects in code because only its CONNAME configuration changes.
> Phase 1 produces the migration-safe blueprint; Phase 2 executes it.
>
> For mainframe apps specifically, we classify by client transport
> rather than by platform. Mainframe apps using MQ client over TCP are
> treated identically to distributed TCP clients — same migration path.
> Mainframe apps using bindings mode — CICS, IMS, batch on z/OS —
> cannot be migrated by changing CONNAME and require application
> redeployment first; we flag those as BINDINGS for special handling.
> Mainframe apps using legacy SNA transports are flagged out-of-scope.
> The classification per app is in the Topology view, in the Migration
> Safety panel, and in the evidence bundle CSV.
>
> One-at-a-time is safe because of per-app rollback locality. Migrating
> App A1 doesn't touch App A2's queues, channels, or routing. A failure
> during A1's migration only affects A1; A2 through An continue
> producing and consuming normally. Phase 2 verifies this property
> formally in TLA+ as the PerAppRollbackLocality invariant. Phase 1
> surfaces the dependency graph that proves no app blocks another's
> migration.

---

*Document version: 1.0 — Phase 1 deliverable, included in the IntelliAI
evidence bundle as part of the per-session forensic export.*
