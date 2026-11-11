# Design review brief: lakehouse_runledger

**Purpose:** prepare a working session to refine the plan with a senior data engineer.
**Status:** planning only. Nothing is built, and no decision below is final until this review.
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
| D8 | Writes go through Spark `createDataFrame` + `writeTo().append()`, buffered, with `txnAppId` for idempotency | Proposed | Inside the task's own compute with its own identity; Spark Connect-safe | Spike shows commit latency is too high; see R3 |
| D9 | Context comes mainly from job parameters (`{{job.run_id}}` …); auto-detection is a fallback | Proposed | Dynamic references behave the same on all compute | Spike shows auto-detection is reliable on serverless |
| D10 | Our own rule compiler (one `agg()` pass) instead of wrapping an existing data-quality library | **Open** | Control over cost, typed results and severity policy | DQX covers our needs; see section 4 |
| D11 | Minimum Databricks Runtime 15.4 LTS, because `VARIANT` columns need it | **Open** | Free-form fields without JSON strings | Older runtimes are in use; see Q-E3 |
| D12 | Naming: install `lakehouse-runledger`, import `lakehouse_runledger`; the CLI, `rl_` tables and `RUNLEDGER_*` env vars use the short name | Proposed | Short names where people type or query them | — |

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
5. **Decorators that trigger Spark jobs.** `@rl.expect` runs validation when the function returns. That's convenient, but hidden compute cost can surprise people. Is it the right default, or should validation always be an explicit call?
6. **The scope of the MVP.** It includes the core, the Delta sink, seven rules, the decorators and manifest v0. Is that the right slice, or should the MVP drop the manifest, or drop validation?

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
| **DQX comparison (D10)** | Implement the five MVP rules in DQX on one real table; compare cost, result shape and ergonomics | Decide build vs. wrap |

---

## 8. Rough effort (for planning only)

These are order-of-magnitude numbers for one experienced engineer, assuming the spike finds no surprises. Please sanity-check them.

| Phase | Scope | Rough effort |
|---|---|---|
| Phase 0 | Spike plus the DQX comparison | ~1 week |
| MVP (0.1) | Core, Delta sink, seven rules, decorators, logging bridge, manifest v0, minimal bundle | ~5–7 weeks (≈3 fewer if DQX is wrapped) |
| v1.0 | Remaining rules, samples, LDP, crash inference, dashboard, alerts, maintenance, Tier 4 CI | ~6–8 weeks |
| Pilot | Two or three pipelines in production, feedback, docs | ~2 weeks, overlapping v1 |

---

## 9. Suggested agenda (90 minutes)

1. **10 min:** goals, non-goals, and the adoption-first principle (sections 1–2).
2. **15 min:** build vs. adopt, ending in a direction for D10 (section 4).
3. **20 min:** walk the environment and scale questions (section 6: S, E). Capture answers live.
4. **15 min:** data quality and governance questions (section 6: V, G).
5. **15 min:** challenge the uncertain areas (section 5) and the proposed decisions D6–D9.
6. **10 min:** confirm the MVP scope and the pilot pipelines (Q-A3).
7. **5 min:** assign owners for the Phase 0 spike items.

## 10. Outputs we want from the session

- [ ] Answers, or owners, for every section-6 question
- [ ] D10 direction (build, wrap DQX, or decide after the spike)
- [ ] D6–D9 accepted or changed
- [ ] D11 runtime floor confirmed
- [ ] MVP scope confirmed or trimmed
- [ ] Pilot pipelines named
- [ ] Spike owners and dates
- [ ] Changes to make in design.md and features.md
