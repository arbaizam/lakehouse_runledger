# runledger governance

> **Status: proposal** for the design review. Nothing here is decided. It builds on controls the
> design already has ([design.md](design.md)), names the gaps, and suggests the least process that
> keeps hundreds of jobs consistent with a small team. Open questions are in [§12](#12-open-questions-for-the-review).

## In short

A small team running hundreds of jobs can't govern by reviewing jobs. Governance has to live in the
bundle template, the manifest and CI, and be measured with the ledger's own tables.

If we do only five things before the pilot:

1. **Name the people.** A product owner, at least two maintainers, and an owner for every job.
2. **Detect contract changes automatically.** Snapshot tests on schemas, enums, keys and the public
   API, with CODEOWNERS requiring two maintainers ([§4](#4-change-control)).
3. **Give criticality a meaning.** `high` / `medium` / `low` with minimum standards, checked by
   `runledger manifest build --strict` in CI ([§6](#6-job-standards-by-criticality)).
4. **Control the version.** One pinned `runledger_version` per target, rolled out in rings, with
   the version recorded on every run ([§5](#5-versions-compatibility-and-rollout)).
5. **Lock down access and write the kill-switch runbook.** A grants model where jobs can report
   on themselves but can't change what was expected of them ([§8](#8-access-retention-and-operations)).

The scorecard, rule lifecycle, alert routing and retention follow in v1 ([§13](#13-phasing)).

---

## 1. The problem and the principles

The ratio of jobs to engineers creates five risks:

- **Blast radius.** One library release, schema change or changed default reaches every job at
  once. A bad release could fail hundreds of tasks, or silence every alert.
- **Attention.** Nobody can review hundreds of jobs by hand. A control that needs a person to look
  at each job won't happen.
- **Trust.** Noisy alerts and wrong verdicts get ignored quickly, and then the real ones are
  ignored too.
- **Drift.** Without strong defaults, every job ends up configured a little differently:
  different SLAs, severities, rules and library versions.
- **Bus factor.** A custom library maintained by two or three people has to survive one of them
  leaving.

**Principles:**

1. **Govern by default, not by meeting.** Standards live in the bundle template, the manifest and
   CI. A job that uses the template is compliant without anyone reviewing it.
2. **Strict where the blast radius is, light everywhere else.** Contracts that every job depends
   on get formal change control. Everything else is a normal PR.
3. **Proportional to criticality.** A few critical jobs get the full treatment. The long tail gets
   zero-code coverage (rung 0) and nothing more is required of it.
4. **Measure with the ledger itself.** The `rl_*` tables already know which jobs are owned,
   instrumented, validated and noisy. A scorecard view replaces audits.

## 2. What the design already gives us

Much of the control surface already exists. This proposal leans on it rather than adding process.

| Control in the design | Governance use |
|---|---|
| `schema/tables.py` as the single source; generated DDL; CI drift check | Every schema change goes through one reviewed file |
| `runledger.schema_version` + a minimum version per client; tables-first migration order | Hundreds of clients can upgrade safely |
| ruff `banned-api` rule | Spark Connect compatibility can't regress silently |
| Manifest generated from the bundle; `manifest_version` with `effective_from` / `effective_to` | An auditable history of what was expected, and when |
| `owner`, `criticality`, `sla_max_duration` in `runledger.yml` | Ownership and tiering per job |
| `rule_key` + `definition_hash` | Every threshold change is visible in the data |
| `effective_policy` stored on every result | Proof of whether a rule was enforced, trialled or muted |
| `context_sources` and config provenance | Proof of where each setting came from |
| Shadow mode, the canary, `reconcile explain` | New jobs can't page anyone until their verdicts are trusted |
| The template's dependency, and the bundle mutator (v1) | One place to pin the library version for the fleet |
| Tables never auto-created; setup runs as a privileged principal | Only admins change structure |

The gaps this proposal fills are listed in [§11](#11-proposed-changes-to-the-design).

## 3. Roles

With a small team these are hats, not headcount. One person can wear several, but each hat has a
named holder.

| Role | Held by | Responsible for |
|---|---|---|
| **Product owner** | one named person, plus a deputy | The final call on *Decided* items and phase gates; MVP scope; accepting contract changes |
| **Maintainers** | at least two people | Library and template code, releases, schema migrations. Two, so that no release or review depends on one person |
| **Platform on-duty** | a weekly rotation among maintainers | Platform alerts (canary, reconciler, delivery); triage of noisy alerts; emergency kill switches |
| **Job owner** | the `owner` in the manifest (a team or a person) | The job's SLA, criticality, suites and thresholds; responding to its alerts |
| **Data owner** (optional) | whoever owns a published dataset | Approving `critical` rules on that dataset. Matters if stewards own thresholds (Q-V3) |

Today *Decided* in the decision log means "set by the product owner", but nobody is named.

Q-O3 asks who owns the shared schema and the reconciler. Proposal: the maintainers, deployed
centrally once per environment, not once per team.

## 4. Change control

### 4.1 Three classes of change

| Class | Covers | Needs |
|---|---|---|
| **1 · Contract** | Table and view schemas. Key formats (`run_key`, `slot_key`, `rule_key`) and the `definition_hash` formula. Status, outcome, verdict and severity values. The public Python API. Config keys, `RUNLEDGER_*` env vars and the `runledger_ctx` format. The CLI. Rule serialization (`to_dict`). The `runledger.yml` and manifest formats | A decision-log entry, an update to design.md, approval by two maintainers, a compatibility and migration note, a release note |
| **2 · Default** | Defaults that change behaviour without any change in the jobs: severity policy, flush time budget, retry counts, shadow period, the redaction key list, which log levels are bridged, alert thresholds | One approval and a release note. A default that would make more jobs fail or page ships as opt-in for one minor release first |
| **3 · Internal** | Everything else | Normal PR review |

**Make the class mechanical, not a judgment call.** Contracts get golden snapshot tests in
`tests/contracts/`:
- the table specs and view columns
- every enum
- `to_dict()` output for each rule type
- `definition_hash` and `rule_key` for a fixed set of rules
- the config key list and env var names
- `runledger --help`

A PR that changes a snapshot is class 1 by definition. CODEOWNERS requires two maintainers on
`tests/contracts/` and `src/lakehouse_runledger/schema/`.

### 4.2 Decision records

- Keep the decision log in [review-brief.md §3](review-brief.md#3-decision-log) until the review
  closes. Then give each decision a short record in `docs/decisions/NNNN-title.md` (context,
  decision, consequences, *revisit if*) and keep §3 as the index.
- Add an owner and a date to every decision.
- The *Revisit if* column is the right idea. Check its triggers at the quarterly review
  ([§10](#10-cadence)) rather than relying on someone remembering.

### 4.3 Phase gates

Each phase ends with a go/no-go by the product owner against written exit criteria:

| Gate | Exit criteria |
|---|---|
| Phase 0 → MVP | Every spike item and every claim in [review-brief §12](review-brief.md#12-platform-claims-to-settle-in-the-design-review) is settled in `platform-notes.md` or has its fallback chosen. D10 is decided |
| MVP → pilot in prod | Test tiers 1–3 green. Canary green for 7 days in dev. Shadow-mode verdicts on the pilot jobs reviewed, with no unexplained false positives |
| Pilot → fleet rollout | Pilots on runledger for 2+ weeks. No task failures caused by the library. Delivery completeness and false-verdict rate within the platform SLOs ([§8.3](#83-platform-slos)). A runbook for every alert |
| Each release | The release checklist ([§5.3](#53-release-rings)) |

**Scope control.** The MVP grew after review round 1 ([review-brief §5](review-brief.md#5-where-im-least-confident-please-push-on-these),
item 6). A feature enters a phase only if a named pilot job needs it, or it closes a correctness or
trust gap. Everything else waits on the *Later* list. For a small team, the maintenance cost of
what we build is a governance concern too; it is part of the D10 decision.

## 5. Versions, compatibility and rollout

### 5.1 Versioning

- **Library:** semantic versioning. Before 1.0 a minor release may break a contract, but it still
  follows the deprecation rule below.
- **Schema:** `runledger.schema_version` is an integer, separate from the library version. Within
  a major version, schema changes are additive only. Never rename, retype or drop a column: add a
  replacement beside it, and drop the old one only in a major release.
- **Views are the read contract.** Dashboards, alerts and saved queries read `rl_v_*` views, not
  base tables. A view can absorb a table change; hundreds of saved queries can't. (The alert
  example in [features.md H6](features.md#h6-databricks-sql-alerts-v1-one-alert-in-mvp) reads
  `rl_validation_results` directly; it should read a view.)
- **Deprecation:** anything removed is deprecated for at least one minor release first. A
  deprecated call logs one WARN event per run and shows in `runledger doctor`.
- **Support window:** the latest two minor versions. The scorecard lists jobs on anything older.

### 5.2 One pinned version for the fleet

- The template's dependency (and the bundle mutator, in v1) installs
  `lakehouse-runledger==${var.runledger_version}`. Upgrading every job in a target is one variable
  change in a reviewed PR. Rolling back is a revert.
- A job may pin its own version, but the scorecard shows it.
- Record the library version on every `rl_task_runs` row, in a new `client_version` column. Today
  only validation results record it, inside `scope`. With the column, "which jobs run which
  version" is one query.

### 5.3 Release rings

| Ring | Who | Soak |
|---|---|---|
| 0 | The canary job and the Tier 4 integration suite (`-t it`) | must pass |
| 1 | Pilot jobs in prod | 3–7 days |
| 2 | All dev and test targets | 1–3 days, can overlap ring 1 |
| 3 | All prod targets | — |

Schema migrations run before ring 0: tables first, then clients, as the design already says.

**Release checklist:**
- the contract snapshot diff is reviewed
- compatibility tests are green: client N−1 against schema N, and client N against schema N
- the release notes list every class 1 and class 2 change
- rollback (reverting `runledger_version`) has been tested

### 5.4 Kill switches

When something goes wrong across the fleet, the switches and the authority to flip them should be
agreed in advance.

| Switch | Effect | Who may flip it in prod | How it is recorded |
|---|---|---|---|
| `RUNLEDGER_SINKS=stdlog` | Stops all Delta writes; logs go to the driver log only | On-duty, without waiting for review | `context_sources`; the gap shows in delivery completeness |
| `RUNLEDGER_VALIDATION_ENFORCE=false` | Validation keeps recording but never fails a task | On-duty during an incident; otherwise a reviewed PR | `effective_policy` on every result |
| `RUNLEDGER_VALIDATION_EVALUATE=false` | Skips validation entirely | Reviewed PR only | `effective_policy`; the "dataset not validated in 24h" alert fires |
| Reconciler alerts back to shadow mode | Stops paging on verdicts | On-duty | Reconciler config in the bundle |
| Previous `runledger_version` | Rolls back the library | On-duty | Bundle variable history; `client_version` |

An emergency flip needs a follow-up PR within one business day that either reverts it or makes it
permanent.

**Open (G-Q5):** these switches are env vars or job parameters, so flipping one fleet-wide means
redeploying every bundle. If the jobs live in many bundles, that is slow during an incident. An
option is a small control file on a UC Volume, read at `init()` within the existing 1s detection
budget, that holds fleet-wide overrides. It costs one read per task.

## 6. Job standards by criticality

The manifest already has `criticality`. Give its values a meaning, and keep the long tail cheap.

| Requirement | `high` | `medium` | `low` (default) |
|---|---|---|---|
| In a manifest (bundle or `manifest add`) | required | required | required |
| `owner` | required | required | required (a bundle-level default is fine) |
| SLA and grace | required | required | optional |
| Instrumentation | rung 2+, with `@rl.step` on publish steps | rung 2+ | rung 0 is enough |
| Validation of published datasets | an enforced suite with at least: schema contract, `row_count(min=…)`, `not_null` and `unique` on keys | a suite, in trial mode at least | none required |
| Gating consumers | write-audit-publish (v1) where downstream jobs read the data | — | — |
| Alerts | the on-call channel, any hour | the team channel, business hours | dashboard only |
| Shadow period | 7 days | 7 days | 7 days |

The alert rows depend on Q-O1 and Q-O2.

**Checked in CI, not in review.** A new `--strict` flag on `runledger manifest build` runs on
every bundle PR. It fails when:
- a job has no owner after defaults are applied
- a `high` or `medium` job has no SLA or grace
- a `high` job lists no `expected_datasets`
- an `exclude` has no `reason`
- a waiver has expired

**Default criticality.** Unclassified jobs default to `low`, so onboarding hundreds of existing
jobs doesn't wait on classifying them. The trade-off is that an unclassified critical job gets no
alerts. The scorecard lists every job still on the default so they get classified over time
(G-Q4).

**Waivers.** A job that can't meet its tier yet gets a waiver in `runledger.yml`, with a reason,
an owner and an expiry date. An expired waiver fails `--strict`.

```yaml
# runledger.yml
defaults: {grace_minutes: 30, owner: data-eng, criticality: low}
jobs:
  orders_daily:
    criticality: high
    sla_max_duration_minutes: 90
    tasks:
      publish: {expected_datasets: [main.silver.orders]}
    waivers:
      - {requirement: enforced_suite, reason: "thresholds being tuned", owner: data-eng, expires: 2027-01-15}
```

**Prod deploys come from CI only.** `runledger manifest sync` runs at deploy time, so a deploy
from a laptop would record a manifest that matches no commit.

## 7. Data quality rule governance

### 7.1 Rule lifecycle

| Stage | Policy | Leaves the stage when |
|---|---|---|
| **Trial** | `evaluate` on, `enforce` off, `notify` off | 7 days or 10 runs, whichever is later, and its failures have been reviewed |
| **Enforced** | `enforce` on, `notify` on | the rule is retired |
| **Retired** | removed from the suite | its history stays under its `rule_key` |

- A new rule, or a tighter threshold on an enforced rule, starts in trial. A looser threshold can
  go straight to enforced.
- A rule in trial for more than 30 days is flagged: promote it or delete it.
- `critical` halts downstream work. It needs a one-line justification in the rule's description,
  and the data owner's approval where there is one.

```sql
-- Rules still in trial (enforce off) after 30 days
SELECT rule_key, dataset, min(evaluated_at) AS first_seen
FROM rl_validation_results
GROUP BY rule_key, dataset
HAVING min(evaluated_at) < now() - INTERVAL 30 DAYS
   AND NOT max_by(effective_policy.enforce, evaluated_at);
```

### 7.2 Identity and ownership

- `error` and `critical` rules must set an explicit `key`. A derived key changes when a dataset or
  column is renamed, and that splits the rule's history in two.
- Naming convention: `<dataset_short>.<columns>.<check>`, e.g. `orders.order_id.unique`.
- Shared suites live in one module in git (rung 5), with the job owner as CODEOWNER. A threshold
  change is a code review; `definition_hash` then makes it visible in the data.

## 8. Access, retention and operations

### 8.1 Environments and grants

- One `rl_*` schema per environment catalog, for example `ops_dev.runledger` and
  `ops_prod.runledger`, so dev jobs can't write to the prod ledger. Q-E1 decides the layout across
  workspaces.
- Grant to groups, never to individuals:

| Group or principal | Grants | Why |
|---|---|---|
| `runledger-admins` | owns the schema; runs setup, migrations and maintenance | Only admins change structure or delete rows |
| `runledger-writers` (the job run-as principals) | `MODIFY` on `rl_task_runs`, `rl_events`, `rl_metrics`, `rl_validation_results`, `rl_validation_samples` | Jobs report their results |
| the setup job's principal | `MODIFY` on `rl_manifest` | Only a deploy declares what is expected |
| the reconciler's principal | `SELECT` on `rl_manifest` and the system tables; `MODIFY` on `rl_slot_observations`; read access to Jobs | Only the reconciler draws conclusions |
| `runledger-readers` | `SELECT` on the `rl_v_*` views | Everyone reads through the contract |
| `runledger-sample-readers` | `SELECT` on `rl_validation_samples` | Samples may hold PII |

Writers get no access to `rl_manifest` or `rl_slot_observations`. A job can report on itself, but
it can't change what was expected of it or the verdict about it.

**`MODIFY` isn't write-only** (claim C7, Q-G6). With one team on shared tables that is acceptable,
given two cheap controls:
- **Detect.** A weekly check of `DESCRIBE HISTORY` on each `rl_*` table flags any operation other
  than appends, `OPTIMIZE` and the maintenance job's deletes. The design is append-only, so
  anything else is a mistake or worse.
- **Prevent, if Q-G1 requires immutable evidence.** Set `delta.appendOnly = true` on
  `rl_task_runs`, `rl_validation_results`, `rl_manifest` and `rl_slot_observations`. Retention on
  those tables then means lifting the property inside the maintenance job, or keeping everything
  (they are small). Add to the Phase 0 spike: check that the property works with liquid clustering
  and predictive optimization.

Per-team schemas with union views are worth it only if a second team with different data
permissions adopts runledger.

### 8.2 Retention and classification

Starting points until Q-G1 and Q-G2 are answered:

| Table | Retention | Classification |
|---|---|---|
| `rl_task_runs`, `rl_validation_results`, `rl_slot_observations`, `rl_metrics` | 13 months | internal |
| `rl_events` | 90 days | internal; free text may hold sensitive data, so redaction applies |
| `rl_validation_samples` | 14 days | restricted |
| `rl_manifest` | all versions | internal |

- Thirteen months allows year-over-year comparison of quality trends.
- Every table is tagged in UC with `owner` and `classification`.
- The redaction key list is a class 2 default. Adding a key is cheap; removing one needs review.
- Samples are off in prod unless `sample_columns` is set. Later: automatically exclude columns
  that carry a PII tag in UC.

### 8.3 Platform SLOs

The maintainers own the platform's health, separately from any job's.

| SLO | Target | Source |
|---|---|---|
| Canary freshness | < 30 min | canary job |
| Reconciler freshness | < 30 min | `rl_slot_observations` |
| Delivery completeness | ≥ 99.9% of rows per day | `rl_v_delivery_completeness` |
| Task failures caused by the library | 0 | `rl_v_failure_groups` where the top frame is in `lakehouse_runledger`, plus any jump in `FAILED_BEFORE_INIT` after a release |
| Verdicts that alerted and were later superseded | < 1% of alerting slots per week | `rl_slot_observations.supersedes` |

### 8.4 Alerts

- **Route by owner and criticality.** One SQL Alert per destination, filtered on the manifest's
  `owner` and `criticality`, instead of one shared destination. For a small team that is a handful
  of alerts. If owners grow past a handful, that is the trigger to revisit D3 and route in a
  `Notifier`.
- **Every alert links a runbook.** The first step is `runledger reconcile explain <slot_key>` or
  the failing `rule_key`.
- **Noise budget.** An alert that fires more than three times in a week with no action taken gets
  a review of that job's SLA, grace or thresholds at the weekly on-duty review.

## 9. Measuring it: the scorecard

Add a view, `rl_v_governance_scorecard`, and a dashboard page:

| Metric | Computed from | Target |
|---|---|---|
| Prod jobs in no manifest | `system.lakeflow.jobs` anti-joined with `rl_manifest` | 0 |
| Jobs on the default criticality | `rl_manifest` | trending down |
| `high` and `medium` jobs with no SLA | `rl_manifest` | 0 |
| `high` jobs uninstrumented | `rl_v_expected_vs_actual`, verdict `UNINSTRUMENTED` | 0 |
| `high` expected datasets with no enforced result in 24h | `rl_manifest.expected_datasets` × `rl_validation_results` | 0 |
| Rules in trial for more than 30 days | the [§7.1](#71-rule-lifecycle) query | 0 |
| Rules with an `ERROR` outcome in the last 7 days | `rl_validation_results` | 0 |
| Jobs on unsupported library versions | `rl_task_runs.client_version` | 0 |
| Expired waivers | `rl_manifest` | 0 |
| Non-append operations on `rl_*` tables | the Delta history check | 0 unexplained |
| Alerts per job per week | alert history | within the noise budget |

The first metric matters most. It is the only one that catches jobs created outside a bundle,
which the rest of the system can't see.

## 10. Cadence

| When | Who | What | Time |
|---|---|---|---|
| Every PR | CI | DDL drift, the banned-API rule, contract snapshots, compatibility tests, `manifest build --strict` | — |
| Weekly | on-duty | Platform alerts, noisy alerts, non-append operations | 15 min |
| Monthly | maintainers and job owners | The scorecard: promote or delete trial rules, classify jobs, close waivers | 30 min |
| Quarterly | product owner and maintainers | *Revisit if* triggers, the support window, retention, SLOs | 45 min |
| Each release | maintainers | Release checklist and rings ([§5.3](#53-release-rings)) | — |

## 11. Proposed changes to the design

| # | Change | Where | Phase |
|---|---|---|---|
| 1 | Name the product owner and two maintainers; add owner and date to the decision log | review-brief §3 | now |
| 2 | CODEOWNERS and contract snapshot tests | repo, `tests/contracts/` | MVP (implementation step 1) |
| 3 | `client_version` column on `rl_task_runs` | design › Data model | MVP (cheaper before the first table exists) |
| 4 | `runledger_version` bundle variable; release rings | design › DAB template | MVP; the mutator pins it in v1 |
| 5 | The grants model in `03_grants.sql`, with separate principals for setup and the reconciler | design › DAB template, Risks | MVP |
| 6 | Defined `criticality` values; `owner` required with a bundle default; `manifest build --strict` | features G1 | MVP |
| 7 | Alerts and dashboards read views only; fix the H6 example | features H6 | MVP |
| 8 | A written kill-switch runbook | new `docs/runbooks/` | before the pilot |
| 9 | Waivers with expiry dates in `runledger.yml` | features G1 | v1 |
| 10 | `rl_v_governance_scorecard` and a dashboard page | features H3, H5 | v1; the unmanaged-jobs metric fits the MVP coverage view |
| 11 | Alert routing by owner and criticality; runbook links in alerts | features H6 | v1 |
| 12 | Rule lifecycle; explicit `key` for `error` and `critical` rules, enforced by a lint | features E5, E8 | convention now; lint in v1 |
| 13 | Retention per table, UC tags, the Delta history check | features H7 | v1 |
| 14 | `delta.appendOnly` on the evidence tables | Phase 0 spike | depends on Q-G1 |
| 15 | A fleet control file for kill switches without a redeploy | design › Config | open (G-Q5) |

## 12. Open questions for the review

| # | Question | Why it matters |
|---|---|---|
| G-Q1 | Who is the product owner, and who are the (at least two) maintainers? | Every gate and class 1 change needs a named approver |
| G-Q2 | Does the small team own every job, or do other teams own some of them? | One team: shared tables and one alert destination are fine. Several: owner routing and Q-G6 matter more |
| G-Q3 | Roughly how many bundles do the jobs live in? | Whether one `runledger_version` variable and redeploy-based kill switches are practical |
| G-Q4 | Default criticality `low` (quiet, may miss an unclassified critical job) or `medium` (safer, noisier)? | Alert volume on day one |
| G-Q5 | Kill switches by redeploy, or a fleet control file read at `init()`? | Incident response time vs one extra read per task |
| G-Q6 | Who approves `critical` rules and thresholds: engineers or data owners? | Same as Q-V3; sets the reviewer for rule changes |

This proposal also suggests answers to existing questions, to confirm or overturn:
- **Q-G3** (who reads): everyone reads views; samples are restricted ([§8.1](#81-environments-and-grants)).
- **Q-G4** (job identities): grant to a writers group, whatever the run-as principals turn out to be.
- **Q-G6** (`MODIFY` on shared tables): accept, with detection, and append-only tables if Q-G1 requires it.
- **Q-O3** (who owns the schema and reconciler): the maintainers, deployed once per environment.

It depends on: Q-G1 and Q-G2 (retention, immutability, classification), Q-O1 and Q-O2 (alert
destinations and hours), Q-E1 (catalog layout), Q-E8 (share of jobs in bundles) and Q-E9 (where
the CI checks run).

## 13. Phasing

Governance should grow with adoption, not ahead of it.

| Phase | Governance in place |
|---|---|
| **Now (design review)** | Roles named; change classes agreed; decision log has owners and dates; this document accepted or changed |
| **Phase 0** | Spike items added: `delta.appendOnly` with liquid clustering; how fast a fleet-wide redeploy is |
| **MVP** | Contract snapshots and CODEOWNERS; `client_version`; `runledger_version` and rings; grants model; criticality tiers and `manifest build --strict`; views as the read contract; kill-switch runbook |
| **Pilot** | Phase gate criteria checked; weekly on-duty review starts |
| **v1** | Scorecard; rule lifecycle lint; waivers; alert routing and runbooks; retention, tags and the history check; monthly and quarterly reviews start |
| **Later** | Fleet control file if G-Q5 says so; automatic PII exclusion from samples; per-team schemas if a second team adopts |
