# Design review brief: lakehouse_runledger

**Purpose:** prepare a working session to refine the plan with a senior data engineer.
**Status:** planning only. Nothing is built, and no decision below is final until this review.
**Revision:** updated after review round 1, an external architecture review of the plan. What changed is in §11. Platform claims to settle in this review are in §12.
**Read time:** about 20 minutes for this brief. [design.md](design.md) has the details; [features.md](features.md) gives an example for every feature.

---

## 1. The proposal in one paragraph

We'd build a Python library and a Databricks Asset Bundle template that give every Databricks Workflows task four things:

- **Structured run records:** started and finished, status, duration, errors and metrics.
- **First-class data validation:** declarative rules compiled into one Spark pass. Results are typed and tied to severity, and a failure can fail the task.
- **Never-start detection:** a manifest of expected runs, built from the bundle, plus a reconciler that flags runs that are missing or that failed before any code ran.
- **Databricks-native reporting:** Unity Catalog Delta tables, an AI/BI dashboard and SQL Alerts. Webhooks come later.

Adoption is the main design constraint. A team gets value with zero lines of code (the manifest and reconciler), then one line, then decorators, and only later the full API.

## 2. Goals and non-goals

**Goals**
- One consistent record of what every workflow task did, and what it should have done, queryable with SQL.
- Data quality checks that are cheap to write, cheap to run, and that can stop bad data.
- Works the same on serverless, on classic job clusters and in Lakeflow Declarative Pipelines.
- A low barrier to entry. Teams adopt it because it's easier than not using it.

**Non-goals** (please confirm)
- Not a general application monitoring or tracing system, and not a replacement for Datadog or Splunk.
- Not a replacement for native job notifications, retries or system tables. We build on them.
- No logging from Spark executors or UDFs. Logging is driver-only.
- No row-level lineage. Unity Catalog lineage already covers that.
- Not a scheduler or orchestrator.

---

## 3. Decision log

**Status values:**
- **Decided:** set by the product owner. Revisit only with a strong reason.
- **Proposed:** a design choice to challenge.
- **Open:** needs answers from section 6.

| # | Decision | Status | Why | Revisit if… |
|---|---|---|---|---|
| D1 | Build a new package; the old databricks_event_logger is a feature reference only | Decided | The old design had structural limits: one INSERT per event, one wide table, validation only in docs | — |
| D2 | Sinks: console, stdout/logging, UC Delta, plus a pluggable interface | Decided | Dev ergonomics plus a durable store | — |
| D3 | Alerting is Databricks-native first (SQL Alerts, dashboards); webhooks later through a `Notifier` seam | Decided | No secrets or egress needed in tasks; one place to manage alerts | Alerts must arrive in under a minute |
| D4 | Validation is built in, Python-first; YAML suites come later | Decided | Lowest barrier for engineers; rules stay serializable | Data stewards (not engineers) own the thresholds; see Q-V3 |
| D5 | Never-start detection through a bundle-generated manifest and a reconciler | Decided | Covers failures the library can never see | Jobs are mostly triggered from outside Databricks; see Q-N1 |
| D6 | One table per grain (runs, events, metrics, validation results, samples, manifest) instead of one wide table | Proposed | Typed evidence, simpler alert SQL, different retention and grants per table | The team prefers one table for simplicity |
| D7 | Append-only writes; current state comes from views; no MERGE on the hot path | Proposed | Hundreds of concurrent tasks never conflict; a crash can't leave a half-updated row | — |
| D8 | Writes go through Spark `createDataFrame` + `writeTo().append()`, buffered, with a stable per-table batch identity (`txnAppId`/`txnVersion`), per-table acknowledgments and a time budget on every flush | Proposed (revised, A9/A10) | Inside the task's own compute with its own identity; Spark Connect-safe; logging can't stall the job | Spike shows commit latency is too high, or a flush can't be abandoned safely |
| D9 | Context comes mainly from job parameters (`{{job.run_id}}` …); auto-detection is a fallback | Proposed | Dynamic references behave the same on all compute | Spike shows auto-detection is reliable on serverless |
| D10 | Validation engine: (a) our own compiler, (b) wrap DQX, or (c) our rule API and result format with a swappable engine (DQX could be one engine) | **Open** (widened, A16) | Control over cost, typed results, error semantics and severity policy | The DQX comparison (§7) shows it meets the result format, error semantics and gating needs |
| D11 | Minimum Databricks Runtime 15.4 LTS, because `VARIANT` columns need it | **Open** | Free-form fields without JSON strings | Older runtimes are in use; see Q-E3 |
| D13 | Three separate statuses: execution, data quality, and telemetry completeness | Proposed (new, A5/A8/A15) | Repairs, caught failures, observe mode and sink outages each made a single status misleading | — |
| D14 | The reconciler uses an evidence model: every verdict carries a confidence; missing telemetry is reported as `TELEMETRY_GAP`; observations can be superseded; shadow mode and a canary | Proposed (new, A5–A7) | False "failed" or "missed" verdicts would destroy trust | Too complex for the MVP; could ship with fewer verdicts |
| D15 | In LDP, severity and action are separate: `error` → fail the update; dropping rows needs an explicit opt-in; aggregate checks are observational | Proposed (new, A2/A3) | Reusing a suite must never quietly turn "reject" into "delete" | — |
| D16 | Gating uses write-audit-publish: validate the exact Delta version that will be published | Proposed (new, A14) | Validating a DataFrame and then writing it can validate data that differs from what gets published | Teams find a staging step too heavy; it could stay opt-in |
| D12 | Naming: install `lakehouse-runledger`, import `lakehouse_runledger`; the CLI, `rl_` tables and `RUNLEDGER_*` env vars use the short name | Proposed | Short names where people type or query them | — |
| D17 | Governance by default: named roles, three change classes with contract snapshot tests, one pinned version per target rolled out in rings, criticality tiers checked in CI, a grants model, and a scorecard view ([governance.md](governance.md)) | Proposed (new) | A small team can't review hundreds of jobs by hand; standards must live in the template, the manifest and CI | A second team with separate permissions adopts runledger; owners outgrow a handful of alert destinations |

---

## 4. Build vs. adopt: what already exists

We should only build what isn't already available. This table is my current understanding; please correct it.

| Existing option | What it gives us | Gap for our needs | Proposed use |
|---|---|---|---|
| **System tables** (`system.lakeflow.*`) | Job, task and run history and outcomes, platform-side | No app-level events, metrics or validation; can lag (to verify) | **Reuse:** the reconciler backfill, crash inference, job names in views |
| **Native job notifications / retries / run-if** | Alerts and recovery when a task fails | Nothing on data quality or runs that never happened; no history to query | **Keep as-is:** validation failures surface as task failures |
| **LDP expectations** | Row-level checks in pipelines, results in the event log | Pipelines only; row-level only; no aggregate checks | **Integrate:** export our rules as expectations and ingest the event log |
| **Databricks Labs DQX** | PySpark data-quality rules, valid/invalid split, YAML/dict checks, profiling | Unclear: severity policy, task context, single-pass cost, results schema, long-term support | **Evaluate in the spike:** could replace our rule engine (D10) while we keep context, storage and policy |
| **Data quality monitoring / Lakehouse Monitoring** | Table-level profiling, drift, freshness and completeness monitoring | Table-centric, not run-centric; doesn't fail a task | **Complement:** link from our dashboard; possibly feeds later anomaly rules |
| **Great Expectations / Soda** | Mature DQ ecosystems | Heavier setup; separate results store; not tied to workflow context | Not proposed unless the team already uses one |
| **OpenTelemetry / an existing observability stack** | Traces and logs in a central tool | Not queryable next to the data in UC | Later: an exporter, if a central tool is a requirement |

**The key decision from this table is D10: build the rule engine or wrap DQX.**
- Wrapping DQX probably saves 2–3 engineer-weeks and adds an external dependency.
- Building our own gives single-pass cost control and exactly the result shape the alerts need.

My recommendation is a one-day time-boxed comparison during the Phase 0 spike, before deciding.

---

## 5. Where I'm least confident (please push on these)

1. **Commit latency on short tasks.** STARTED and FINISHED are at least two Delta commits per task, roughly 3–10 seconds on serverless. Is that acceptable for tasks that run in seconds? The alternative is "finish-only" mode, which gives up crash visibility.
2. **Small files at high concurrency.** Many tasks appending small batches relies on predictive optimization or auto-compaction. Is predictive optimization enabled for us?
3. **Notebook end-of-run hooks.** Detecting a notebook's end reliably (IPython exception hooks, `dbutils.notebook.exit`) is the most fragile part of the one-liner. If it's unreliable, notebooks need an explicit `rl.finish()`, which raises the barrier.
4. **Reconciler accuracy.** Expected slots from cron are straightforward. File-arrival, continuous and externally triggered jobs are not, and false "missed" alerts would hurt trust quickly.
5. **Decorators that trigger Spark jobs.** `@rl.expect` runs validation when the function returns. `suite.explain()` now shows that work in advance, and gating is steered toward write-audit-publish. Is the decorator still the right *default* for validation, or should it be reserved for observing?
6. **The scope of the MVP.** After review round 1 it gained something visible for pilots (coverage view, one alert, canary, smoke tests), error isolation and a fuller reconciler. Async decorators moved to v1. Is it now too big? The first candidates to cut would be the reconciler's less common verdicts or the logging bridge.
7. **Rule-error isolation cost.** Bisecting after a runtime error is capped at 3 extra passes. Is that acceptable on the largest tables (Q-S3), or should a runtime error just mark the whole suite `ERROR`?

---

## 6. Key questions about our needs and environment

Each question says what changes in the plan depending on the answer.

### Scale and workload

| # | Question | Why it matters |
|---|---|---|
| Q-S1 | Roughly how many jobs and tasks run per day, and what is the peak number running at once? | Commit volume, small files, whether batching is enough or a streaming sink (Zerobus) is needed |
| Q-S2 | What mix of task types: notebooks, Python wheels/scripts, SQL tasks, dbt, LDP pipelines, `run_job` chains? | Where the one-liner vs decorators matter most; SQL and dbt tasks can't run Python, so only the manifest covers them |
| Q-S3 | Typical and largest row counts of datasets we'd validate? | Exact vs approximate uniqueness, partition filters, whether one pass stays cheap |
| Q-S4 | How short are the shortest tasks? | Whether commit latency (section 5, item 1) matters |
| Q-S5 | Are any workloads streaming or continuous? | Lifecycle model (a continuous job never "finishes") and periodic flushing |

### Platform and environment

| # | Question | Why it matters |
|---|---|---|
| Q-E1 | Which cloud, how many workspaces (dev/test/prod), and do they share a metastore? | One shared `rl_*` schema vs one per workspace; cross-workspace dashboards |
| Q-E2 | Is Unity Catalog enabled everywhere? Any Hive-metastore-only jobs? | The design assumes UC managed tables |
| Q-E3 | Which Databricks Runtime versions are in use, and is serverless approved for jobs? | The 15.4 LTS floor (D11); pyspark 3.5 vs 4.x testing |
| Q-E4 | For classic clusters, which access mode: dedicated (single-user) or standard (shared)? | Standard mode has Spark Connect-like restrictions that affect context detection |
| Q-E5 | Are system tables (`system.lakeflow`, `system.access`) enabled and readable by a service principal? | Reconciler backfill, crash detection, job-name enrichment |
| Q-E6 | Is predictive optimization enabled on the target catalog? | Small-file handling (section 5, item 2) |
| Q-E7 | Is there a serverless SQL warehouse available for alerts and dashboards? | DAB alerts and the dashboard need one |
| Q-E8 | Are Databricks Asset Bundles the standard for deploying jobs? What share of jobs are bundled vs created in the UI or Terraform? | The manifest is generated from bundles; non-bundle jobs need another source |
| Q-E9 | What CI/CD do we use (GitHub Actions, Azure DevOps, other)? | Where the DDL drift check, integration tests and manifest sync run |
| Q-E10 | How do we distribute internal Python packages: private PyPI or Artifactory, a UC Volume wheel, or workspace files? Can jobs reach PyPI? | How teams install the library; affects "zero-wiring" |
| Q-E11 | Are jobs triggered by anything outside Databricks (ADF, Airflow, Control-M, an API)? | Never-start detection needs to know the real schedule owner |

### Data quality needs

| # | Question | Why it matters |
|---|---|---|
| Q-V1 | Which checks matter most today? (nulls, duplicates, row counts, freshness, reconciliation to source, schema drift, business rules) | MVP rule set ordering |
| Q-V2 | What DQ tooling exists already (DQX, Great Expectations, LDP expectations, custom checks)? | Build vs. wrap (D10) and migration effort |
| Q-V3 | Who owns rules and thresholds: engineers in code, or data stewards or analysts? | If stewards, YAML or table-driven suites move up from "Later" |
| Q-V4 | When a critical check fails, should downstream tasks always stop, or sometimes continue with a flag? | Default severity policy, condition-task integration |
| Q-V5 | Is reconciliation against source systems (counts and totals vs an upstream extract) a core need? | `reconcile` might move from v1 into the MVP |
| Q-V6 | Should failing-row samples be stored at all? | PII exposure vs debugging value; samples could be off by default |

### Governance, security and compliance

| # | Question | Why it matters |
|---|---|---|
| Q-G1 | Are there regulatory or audit requirements for retaining run and validation evidence (how long, immutable)? | Retention defaults; whether deletes are allowed; table properties |
| Q-G2 | What data classification applies to log contents? Can free-text messages or samples contain PII or confidential data? | Redaction rules, sample defaults, grants on the samples table |
| Q-G3 | Who should be able to read the `rl_*` tables: all engineers, owners only, auditors? | Grants model; per-team row filters |
| Q-G4 | Which identity do jobs run as (service principals per team, or shared)? | MODIFY grants on `rl_*`; reconciler permissions |
| Q-G5 | Is outbound network access from jobs allowed (for future webhooks)? | Whether in-task webhooks are viable or alerts must stay in Databricks |
| Q-G6 | With shared `rl_*` tables, every producing job needs `MODIFY`, so it can change other teams' rows (claim C7). Is that acceptable, or do we need per-team schemas with union views? | Grants model and the bundle template layout |

[governance.md](governance.md) proposes answers to Q-G3, Q-G4, Q-G6 and Q-O3, and adds its own
questions G-Q1 to G-Q6 (roles, job ownership, number of bundles, default criticality, kill
switches, rule approvers).

### Alerting and operations

| # | Question | Why it matters |
|---|---|---|
| Q-O1 | Who receives alerts (team channels, an on-call rotation, email), and through which tool (Teams, Slack, PagerDuty, ServiceNow)? | Notification destinations; whether a webhook notifier is needed early |
| Q-O2 | What alert latency is acceptable: minutes, or hourly? | SQL Alert schedule vs in-task notifiers; reconciler frequency |
| Q-O3 | Who owns the shared `rl_*` schema and the reconciler job (a platform team, or each domain team)? | Central vs per-team deployment of the bundle template |
| Q-O4 | Is there an existing alert-fatigue problem? | Default alert set and severity thresholds |

### Adoption

| # | Question | Why it matters |
|---|---|---|
| Q-A1 | How many teams and engineers would adopt it, and are they notebook-first or package-first? | Where to invest: the notebook one-liner or decorators |
| Q-A2 | Is there a standard job or bundle template teams already copy? | The best place to ship the zero-wiring setup |
| Q-A3 | Who are the first two or three pilot pipelines, and what does success look like for them? | MVP acceptance criteria |
| Q-A4 | Do teams use an existing logging standard (stdlib `logging` with a set format)? | The logging bridge might deliver most of the value on day one |

---

## 7. Phase 0 spike: what we must verify on our own workspace

Time box: about one week. Each item has a fallback, so none of them blocks the design.

| Item | How to verify | Fallback |
|---|---|---|
| Dynamic value references (`{{job.run_id}}`, `{{task.run_id}}`, repair count) and whether a job-level parameter can use `{{task.*}}` | Bundle job that prints its widgets/argv on serverless and classic | Per-task parameters (still injected by the mutator) |
| Context auto-detection on serverless (`dbruntime`, runtime version env var) | Same job, also logs the detection results | Parameters only |
| `writeTo().append()` with `txnAppId`/`txnVersion` on UC managed tables under Spark Connect | Write the same batch twice and count the rows | Dedup view |
| `parse_json` staging into `VARIANT` | Append a staged batch, read it back | STRING columns plus `parse_json` in views |
| Commit latency for 1–4 small appends on serverless | Time it | Finish-only lifecycle mode for very short tasks |
| Notebook end hooks (IPython exception hook, `dbutils.notebook.exit` classification) | Notebook scenarios: success, raise, exit | Require `rl.finish()` in notebooks |
| System-table lag and `result_state` values for skipped or upstream-failed tasks | Compare with the Jobs API over a day | Jobs API only |
| `databricks bundle validate -o json` shape; DAB Python mutators; DAB `alerts` resource | CLI version check plus a sample bundle | Manifest from YAML; alerts created by an SDK task |
| Rule-error isolation (A1) | A suite with one rule that fails analysis and one that raises at runtime (e.g. a bad cast); check that the others still produce results | Treat a runtime error as `ERROR` for the whole suite |
| Flush time budget (A10) | Abandon a flush mid-write under Spark Connect; check that a late commit plus replay leaves no duplicates | Synchronous flush with a documented worst case |
| Partial delivery and lost acknowledgment (A9) | Fail one of two table writes; drop an acknowledgment and replay | Dedup views and the completeness view only |
| Repairs (A8) | Run fail → repair → success; inspect run ids and attempt numbers in the Jobs API and system tables | Effective attempt by latest start time |
| Write-audit-publish mechanism (A14) | Compare staging + `INSERT OVERWRITE` vs a shallow-clone swap on one table | Staging + publish |
| Claims C1–C12 (§12) | Settle in the review where possible; test the rest here | See §12 |
| **DQX comparison (D10)** | Build each option against our needs: result format, error semantics, blocking before the write, supported compute (serverless, classic, LDP) and total cost, on a real table. Not just "five rules" (A16) | Decide (a), (b) or (c) |

---

## 8. Rough effort (for planning only)

These are order-of-magnitude numbers for one experienced engineer, assuming the spike finds no surprises. Please sanity-check them.

| Phase | Scope | Rough effort |
|---|---|---|
| Phase 0 | Spike plus the DQX comparison | ~1 week |
| MVP (0.1) | Core, Delta sink, seven rules with error isolation, decorators, logging bridge, manifest v0 + reconciler with evidence model, canary, one alert, minimal bundle | ~7–9 weeks (≈2–3 fewer if DQX is wrapped); grew after review round 1 |
| v1.0 | Remaining rules, samples, LDP, crash inference, dashboard, alerts, maintenance, Tier 4 CI | ~6–8 weeks |
| Pilot | Two or three pipelines in production, feedback, docs | ~2 weeks, overlapping v1 |

---

## 9. Suggested agenda (90 minutes)

1. **10 min:** goals, non-goals, and the adoption-first principle (sections 1–2).
2. **15 min:** build vs. adopt, ending in a direction for D10 (section 4).
3. **20 min:** walk the environment and scale questions (section 6: S, E). Capture answers live.
4. **15 min:** data quality and governance questions (section 6: V, G).
5. **15 min:** challenge the uncertain areas (section 5), the proposed decisions D6–D9 and D13–D16, and settle what you can in §12.
6. **10 min:** confirm the MVP scope and the pilot pipelines (Q-A3).
7. **5 min:** assign owners for the Phase 0 spike items.

## 10. Outputs we want from the session

- [ ] Answers, or owners, for every section-6 question
- [ ] D10 direction (build, wrap DQX, or decide after the spike)
- [ ] D6–D9 and D13–D16 accepted or changed
- [ ] §12 claims: each settled, or assigned to the spike
- [ ] D11 runtime floor confirmed
- [ ] MVP scope confirmed or trimmed
- [ ] Pilot pipelines named
- [ ] Governance model (D17, [governance.md](governance.md)) accepted or changed; product owner and maintainers named (G-Q1)
- [ ] Spike owners and dates
- [ ] Changes to make in design.md and features.md

---

## 11. Review round 1: what changed

An external architecture review (Codex) raised 17 challenges (A1–A17) and 7 ideas (B1–B7). This table records what each one changed in the plan, so that today's discussion can confirm or overturn the changes.

| Ref | Finding (short) | Disposition | What changed | Where |
|---|---|---|---|---|
| A1 | One combined aggregation can't isolate a failing rule | Accepted | Preflight analysis (no Spark job); bisect re-run on runtime errors (capped); rules that can't be evaluated fail closed | design › Validation engine; features E16 |
| A2 | In LDP, reusing a suite turned `error` into dropping rows | Accepted | Severity separate from LDP action; `error` → fail; dropping needs opt-in; `mostly` rejected for expectations | design › Validation engine › LDP; features F1, E4 |
| A3 | Aggregate checks in LDP don't gate publication | Accepted | Labelled observational; gating goes through a validation task or write-audit-publish | features F2 |
| A4 | Ingestion skipped failed pipeline runs; unrecorded counts | Accepted | Ingest task runs with `run_if: ALL_DONE`; update errors ingested; NULL + reason instead of 0 | features F3; DAB template |
| A5 | Missing telemetry ≠ crash or failed-before-init | Accepted | Evidence model with confidence levels; `TELEMETRY_GAP`; canary as evidence | design › Never-start; features G2, G5 |
| A6 | Not every task starts in the job's window; failure handlers are excluded on good runs | Accepted | Match slot → job run → task eligibility; `EXCLUDED`, `UPSTREAM_FAILED`, `PENDING`, `OBSERVATION_INCOMPLETE` | design › Never-start |
| A7 | Late evidence; identities lacked the workspace | Accepted | Workspace-qualified keys; manifests with effective dates; observations can be superseded in a new `rl_slot_observations` table | design › Data model, Never-start |
| A8 | "Worst status ever" mishandles repairs and caught exceptions | Accepted | Effective attempt; only uncaught failures propagate; separate quality status | design › Data model, Write path; features B5 |
| A9 | Txn dedup ≠ complete delivery; process id broke replay | Accepted | Stable batch identity per table (no process id); per-table acknowledgments; `rl_v_delivery_completeness` | design › Write path; features D2 |
| A10 | Logging could block; the 60s interval only fires when something is logged | Accepted | Time budget per flush; buffer byte cap; durability stated honestly; background flusher is a spike question | design › Write path; features D1 |
| A11 | Additive migrations could disable older clients | Accepted | Compatibility = "has at least my columns"; migration order; metadata conflicts retryable | design › Write path; features D3 |
| A12 | Sample rows could leak through fallback logs and spill; MODIFY isn't write-only | Accepted | Samples never in fallback; spill has the same controls; Q-G6 added | design › Write path; risks |
| A13 | "Failed rows" is ambiguous for uniqueness; approx can't prove zero | Accepted | Three uniqueness metrics with a stated basis; approx is observational only | design › Validation engine; features E2, E10, E12 |
| A14 | Validated data may not equal published data | Accepted, extended | Write-audit-publish gate (v1, MVP candidate) | design › Validation engine; features E13 |
| A15 | Observe mode, evaluation and alerting were conflated | Accepted | Separate `evaluate` / `enforce` / `notify` settings; effective policy stored per result; alerts filter on it | features E5, H6 |
| A16 | The DQX comparison was too narrow | Accepted | D10 widened to three options with full criteria | §3 D10, §7 |
| A17 | The MVP postponed visible value | Partially accepted | MVP adds coverage view, one alert, canary and smoke tests; async decorators moved to v1; the logging bridge stays in the MVP (cheap, high value) | design › Roadmap |
| B1 | Record the scope of each validation | Adopted (MVP) | `scope` on every result | features E15 |
| B2 | Independent canary | Adopted (MVP) | Canary job and staleness alert | features G5 |
| B3 | Explain verdicts; preview slots | Adopted (MVP) | `reconcile explain` / `preview`; shadow mode | features G4 |
| B4 | Pipeline-events system table | To evaluate (v1) | Depends on claim C5 | design › Validation engine › LDP |
| B5 | Explain the validation plan before running | Adopted (MVP) | `suite.explain(df)` | features E14 |
| B6 | Referential-integrity rule | Adopted (v1) | `c.references(...)` | features E2 |
| B7 | Stable business rule identity | Adopted (MVP) | `rule_key` + `definition_hash` | design › Data model |

## 12. Platform claims to settle in the design review

The reviewer cited Databricks or DQX documentation for these claims, but none of them has been checked against our workspace. Each one changes part of the design. Please settle what you can in the review; anything left goes to the Phase 0 spike.

**Columns:**
- **Plan confidence:** how sure the plan's author is, before any check.
- **If false:** what changes in the design.
- **How to check:** a quick test or the documentation to read.

| # | Claim | Source cited by reviewer | Plan confidence | If false | How to check |
|---|---|---|---|---|---|
| C1 | Serverless runs with ANSI SQL mode on by default, so invalid casts and overflows raise errors instead of returning NULL | [Serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations) | Medium | Runtime errors are rarer; the bisect path is still needed on classic with ANSI on | `SELECT CAST('x' AS INT)` on serverless vs classic; read `spark.sql.ansi.enabled` |
| C2 | Serverless job queries have no execution timeout by default | Same page | Medium | Less risk of an unbounded flush; the time budget stays | Documentation; check the default `STATEMENT_TIMEOUT` behaviour for jobs |
| C3 | DataFrame caching is available on serverless as a preview, requiring environment version 5+ | [Serverless DataFrame cache](https://docs.databricks.com/aws/en/compute/serverless/dataframe-cache) | Low (the plan assumed no cache) | Keep "no cache"; if true, offer opt-in caching and revisit the env v2+ floor | `df.cache()` on serverless env v4 vs v5 |
| C4 | `expect_or_fail` expectations don't record the normal expectation metrics in the event log | [Expectations](https://docs.databricks.com/aws/en/ldp/expectations#expectation-tracking-metrics) | Medium | Ingestion can report counts for failed updates | Run a pipeline with a failing `expect_or_fail`; inspect `flow_progress` |
| C5 | A regional pipeline-events system table exists (Beta), with an example dashboard | [Monitor and query events](https://docs.databricks.com/aws/en/ldp/monitor-and-query-events) | Low (new to the plan) | Keep per-pipeline event-log publishing | Check whether the table is available in our region and metastore; compare its latency with the event log |
| C6 | DQX provides per-check summary metrics, fingerprints, stored results, and a Beta `FailPipeline` action | [DQX summary metrics](https://databrickslabs.github.io/dqx/docs/guide/summary_metrics/), [actions](https://databrickslabs.github.io/dqx/docs/guide/actions_and_alerts/) | Low (not yet evaluated) | Strengthens option (a) for D10 | Part of the D10 comparison |
| C7 | UC `MODIFY` on a table permits updating and deleting rows (and works with SELECT), so a shared table isn't write-only between teams | [UC privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/privileges-reference) | High | Shared tables are fine as planned | Documentation; Q-G6 decides what we do about it |
| C8 | Repairs re-run tasks inside the original job run (same job run id, new task run ids) | [Repair API](https://docs.databricks.com/api/jobs/v2/repair) | High | Effective-attempt logic keys on something else | Spike: fail → repair → success, then inspect the ids |
| C9 | "Excluded by `run_if`" and "upstream failed" are distinct task states in the Jobs API and system tables | [run_if dependencies](https://docs.databricks.com/aws/en/jobs/run-if) | Medium | The reconciler can't separate `EXCLUDED` from `UPSTREAM_FAILED`; it reports both as not run | Spike: a job with a failure-handler task; inspect `result_state` / `termination_details` |
| C10 | The default `run_if` is ALL_SUCCESS, so a task after a failed pipeline is skipped unless it uses `ALL_DONE` | Same page | High | No change needed (but ALL_DONE is harmless) | Documentation |
| C11 | Concurrent metadata changes (e.g. `ALTER TABLE ADD COLUMNS`) can fail concurrent blind appends | [Isolation levels and write conflicts](https://docs.databricks.com/aws/en/optimizations/isolation) | High | The retry rule for metadata conflicts isn't needed | Documentation; spike during the migration test |
| C12 | Delta skips a batch whose `(txnAppId, txnVersion)` it has already committed; keeping that identity stable is the writer's job | [Idempotent table writes](https://docs.databricks.com/aws/en/structured-streaming/delta-lake#use-foreachbatch-for-idempotent-table-writes) | High for `foreachBatch`; **unverified for batch `writeTo` under Spark Connect** | Fall back to dedup views | Already a Phase 0 item: write the same batch twice and count the rows |

Plan claims from before the review that still need the same treatment, and are already on the Phase 0 list:
- `VARIANT` needs DBR 15.3+, which sets the 15.4 LTS floor
- the names of the dynamic value references
- whether a job-level parameter can use `{{task.*}}`
- the IPython exception hook in notebooks, and how `dbutils.notebook.exit` is classified
- whether the DAB `alerts` resource and Python mutators are supported
