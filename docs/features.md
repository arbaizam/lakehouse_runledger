# runledger feature list

Each feature comes with a short example. The **Phase** column follows the roadmap in [design.md](design.md):

| Phase | Meaning |
|---|---|
| **MVP** | 0.1 |
| **v1** | 1.0 |
| **Later** | planned after 1.0 |

Revised after review round 1. Changed or new features carry *(review …)* tags that point to
[review-brief.md §11](review-brief.md#11-review-round-1-what-changed).

Examples assume `import lakehouse_runledger as rl` and `from lakehouse_runledger import checks as c`.

## Contents

- [A. Getting started and ease of use](#a-getting-started-and-ease-of-use)
- [B. Events, spans and task lifecycle](#b-events-spans-and-task-lifecycle)
- [C. Run context](#c-run-context)
- [D. Delivery and sinks](#d-delivery-and-sinks)
- [E. Data validation](#e-data-validation)
- [F. Lakeflow Declarative Pipelines (LDP)](#f-lakeflow-declarative-pipelines-ldp)
- [G. Never-start detection](#g-never-start-detection)
- [H. Storage, dashboards and alerts](#h-storage-dashboards-and-alerts)
- [I. Extension points](#i-extension-points)

---

## A. Getting started and ease of use

### A1. Zero-config local output (MVP)

When the code isn't running on Databricks, everything prints to the console. No setup is needed.

```python
rl.init(app_name="orders_etl")
rl.info("hello")
```
```text
12:00:01 INFO  orders_etl  hello
```

### A2. One-line notebook instrumentation (MVP)

The first cell is the only change. It does five things:
- resolves context
- writes STARTED
- installs exit hooks
- bridges `logging`
- writes FINISHED when the notebook ends or fails

```python
import lakehouse_runledger.auto
```

### A3. `@rl.task`: instrument a wheel or script entry point (MVP)

`@rl.task` works bare or with arguments. It reads `--runledger_ctx` from `sys.argv` without touching your own arguments.

```python
@rl.task
def main(): ...
```

```python
@rl.task(app_name="orders_etl")
def main(): ...
```

### A4. `@rl.step`: a timed span per function (MVP)

Each call records a span with name, duration, status and exception. Sync functions in the MVP; async functions in v1.

```python
@rl.step
def load_orders():
    return spark.table("main.raw.orders")
```

```python
@rl.step(name="orders.publish", dataset="main.silver.orders")
def publish(df):
    df.write.mode("overwrite").saveAsTable("main.silver.orders")
```

### A5. Per-cell spans in notebooks (v1, opt-in)

Every notebook cell becomes a span, with no other code changes.

```python
rl.init(cell_spans=True)
```

### A6. `%%rl_step` cell magic (v1)

```python
%%rl_step load_orders
df = spark.table("main.raw.orders")
```

### A7. Logging bridge (MVP)

Your existing `logging` calls and `warnings` are captured into `rl_events`. WARNING and above is captured by default.

```python
import logging

rl.init(capture_logging="INFO")
logging.getLogger("orders").info("fetched %d files", 12)  # → rl_events row, level INFO
```

### A8. Zero-wiring bundle mutator (v1, opt-in)

The mutator injects `runledger_ctx` and the library dependency into every task in the bundle.

```yaml
# databricks.yml
python:
  mutators:
    - "lakehouse_runledger.bundle:inject_context"
```

### A9. `runledger doctor` (MVP)

Shows what was resolved and where each value came from.

```bash
runledger doctor
```
```text
job_run_id   = 8812731   (job_param)
task_key     = load      (job_param)
compute_type = serverless (runtime)
sinks        = delta,stdlog (code)
⚠ rl_validation_samples not found. Run `runledger setup`.
```

---

## B. Events, spans and task lifecycle

### B1. Task lifecycle rows (MVP)

`STARTED` is written immediately, so a crash still leaves a trace. `FINISHED` carries:
- status
- duration
- error
- `validation_summary`
- delivery health

```sql
SELECT status, duration_ms FROM ops.runledger.rl_v_task_runs WHERE job_id = 123;
```

### B2. Spans with nesting and lineage (MVP)

```python
with rl.span("load"):
    with rl.span("load.orders"):  # parent_span_id = "load"
        ...
```

### B3. Log events with attributes (MVP)

```python
rl.info("file loaded", path="/Volumes/raw/orders/2026-11-10.csv", rows=10_000)
rl.warn("late file", minutes_late=42)
rl.error("bad header", columns=["a", "b"])  # ERROR events flush immediately
```

### B4. Metrics (MVP)

```python
rl.metric("rows_written", 150_000, unit="rows", dataset="main.silver.orders")

with rl.span("load") as s:
    s.metric("files_read", 12)
```

### B5. Status rollup (MVP) *(review A8)*

Three statuses are tracked separately: execution, quality and telemetry completeness.
- Only *uncaught* failures make a parent span FAILED.
- A caught child failure is still recorded, and sets `had_caught_failures` on the parent.
- Data-quality outcomes roll up into the quality status, not the execution status.

```python
with rl.span("load"):                       # execution SUCCEEDED, had_caught_failures = true
    try:
        with rl.span("load.primary"):       # FAILED (recorded in rl_events)
            read_primary()
    except SourceDown:
        read_replica()

with rl.span("publish"):                    # execution SUCCEEDED, quality WARNED
    rl.validate(df).accepted_values("status", ["NEW"], severity="warn").run()
```

Job-level views use each task's **effective attempt**, so a failed task that is repaired successfully turns the job run green, while the failed attempt stays in history.

### B6. Exception capture and fingerprints (MVP)

Each error records its class, its message, and a fingerprint (exception type plus frame locations). The fingerprint groups recurring failures.

```python
@rl.step
def parse():
    raise ValueError("bad date '2026-13-01'")


# rl_events.error = {error_class: "ValueError", message: "...", fingerprint: "9f1c…"}
```

### B7. Redaction and size caps (MVP)

Values under sensitive key names are masked, and oversized values are truncated with an explicit marker.

```python
rl.info("connecting", host="db1", api_token="abc123")  # api_token → "[REDACTED]"
```

### B8. Correct notebook endings (MVP)

`dbutils.notebook.exit()` counts as success. An uncaught exception in a cell counts as FAILED.

```python
dbutils.notebook.exit("done")  # FINISHED, status SUCCEEDED
```

### B9. Ambient logger (MVP)

There's no logger object to pass around.

```python
def helper():
    rl.current().metric("cache_hits", 42)
```

### B10. Thread propagation (v1)

Spans keep the right parent inside thread pools.

```python
with rl.ThreadPoolExecutor(max_workers=8) as pool:
    pool.map(load_partition, partitions)  # each call nests under the current span
```

---

## C. Run context

### C1. Context from job parameters, with provenance (MVP)

One compact parameter carries all the ids. It works the same on serverless, classic and wheel tasks.

```yaml
base_parameters:
  runledger_ctx: "job_id={{job.id}};job_run_id={{job.run_id}};task_key={{task.name}};task_run_id={{task.run_id}}"
```

```python
rl.current().context.job_run_id  # 8812731
rl.current().context.sources["job_run_id"]  # "job_param"
```

### C2. Explicit overrides (MVP)

```python
rl.init(env="prod", job_id=123)
```

### C3. Environment variable overrides (MVP)

```bash
RUNLEDGER_ENV=dev
```

```bash
RUNLEDGER_SINKS=console
```

```bash
RUNLEDGER_CATALOG=ops_dev
```

### C4. Compute-type detection (MVP)

Each run is classified as `serverless`, `classic`, `ldp` or `local`. The value is stored on every run row.

```sql
SELECT compute_type, avg(duration_ms) FROM rl_v_task_runs GROUP BY 1;
```

### C5. Optional SDK enrichment (v1)

```python
rl.init(enrich_from_api=True)  # one Jobs API call: job_name, run_as, trigger
```

---

## D. Delivery and sinks

### D1. Buffered, batched, bounded flushing (MVP) *(review A10)*

Each flush makes about one commit per table. A flush happens on any of:
- 1000 rows or 16 MB buffered
- 60 seconds since the last flush (checked when something is logged)
- an ERROR event
- the end of a suite
- `finish()` or process exit

A flush never blocks the job for longer than its time budget.

```python
rl.init(config=rl.Config(flush_max_rows=2000, flush_interval_s=30, flush_timeout_s=30))
```

STARTED rows, ERROR events and suite results are durable immediately. Other events become durable at the next trigger, which may be the next log call or the end of the task.

### D2. Idempotent Delta appends with stable batch identity (MVP) *(review A9)*

- Each batch keeps the same identity (`runledger:{run_key}:{table}`, batch number) through retries, late commits and spill replay, so it is never written twice.
- Tables are never auto-created.
- Each table is acknowledged separately. The FINISHED row records rows sent and acknowledged, and `rl_v_delivery_completeness` shows any gaps.

```python
rl.init(config=rl.Config(catalog="ops", schema="runledger", sinks=["delta", "stdlog"]))
```

```sql
SELECT run_key, table_name, rows_sent, rows_present FROM rl_v_delivery_completeness WHERE rows_present < rows_sent;
```

### D3. Schema compatibility check, circuit breaker and graceful fallback (MVP) *(review A11, A12)*

- A table is compatible if it has every column the client writes. Extra nullable columns, added by a newer migration, are fine.
- A missing or incompatible table sends output to stdout with a single hint, and the job keeps running.
- Fallback output carries identities and counts, never sample rows or sensitive attributes.

```text
runledger: table ops.runledger.rl_events not found; writing to driver log instead. Run `runledger setup`.
```

### D4. Retries and delivery health (MVP)

```python
rl.current().health  # Health(emitted=812, flushed=812, dropped=0, retries=1, flush_failures=0)
```

### D5. Strict mode (MVP)

Logging failures raise. This is useful in CI.

```python
rl.init(config=rl.Config(strict=True))
```

### D6. Console, stdlog and memory sinks (MVP)

```python
rl.init(config=rl.Config(sinks=["console"]))  # local development
```

```python
sink = rl.MemorySink()
rl.init(sinks=[sink])  # tests
assert sink.events[0].name == "load"
```

### D7. Custom sinks (MVP)

```python
class MySink:
    name = "my_sink"
    record_types = frozenset({rl.EventRecord})

    def validate(self): ...
    def write(self, batch): ...
    def close(self): ...


rl.init(sinks=["delta", MySink()])
```

### D8. Spill to a UC Volume (v1)

Records that still fail after retries are saved to a Volume and recovered by the maintenance job.

```python
rl.init(config=rl.Config(spill_path="/Volumes/ops/runledger/rl_spill"))
```

---

## E. Data validation

### E1. Fluent validation, one Spark pass (MVP)

```python
(
    rl.validate(df, dataset="main.raw.orders")
    .row_count(min=1)
    .not_null("order_id", "customer_id")
    .unique("order_id", severity="critical")
    .run()
)
```

### E2. Rule catalog

| Rule | Phase | Example |
|---|---|---|
| Schema contract | MVP | `c.schema({"order_id": "bigint", "amount": "decimal(18,2)"}, allow_extra=True)` |
| Not null | MVP | `c.not_null("order_id", "customer_id")` |
| Unique (single or composite key) | MVP | `c.unique("order_id")` · `c.unique("account", "as_of_date", basis="rows_in_duplicate_groups")` |
| Row-count bounds | MVP | `c.row_count(min=1, max=50_000_000)` |
| Accepted values | MVP | `c.accepted_values("status", ["NEW", "SHIPPED"])` |
| Range | MVP | `c.in_range("amount", min=0, max=1_000_000)` |
| SQL boolean expression | MVP | `c.expr("ship_date >= order_date", name="ship_after_order")` |
| Regex match | v1 | `c.matches("email", r"^[^@]+@[^@]+$")` |
| Freshness | v1 | `c.freshness("updated_at", max_age="6h")` |
| Null ratio | v1 | `c.null_ratio("phone", max=0.2)` |
| Statistic bounds | v1 | `c.stat_bounds("amount", mean=(90, 110), sum=(1e6, None))` |
| Referential integrity *(review B6)* | v1 | `c.references("customer_id", "main.ref.customers", "id")` (null keys skipped unless `allow_null=False`) |
| Custom SQL | v1 | `c.sql("SELECT count(*) FROM {df} WHERE amount < 0", max=0)` |
| Source/target reconciliation | v1 | `c.reconcile(source=lambda: spark.table("raw.orders"), keys=["order_id"], measures=["amount"])` |

**Uniqueness reports three numbers** *(review A13)*. For keys `[A, A, B]` they are `duplicate_keys=1`, `excess_rows=1` and `rows_in_duplicate_groups=2`. Tolerances say which basis they use (default `excess_rows`), and the result stores it.

### E3. Severity policy (MVP)

| Severity | When the rule fails, or can't be evaluated *(review A1)* |
|---|---|
| `info` | Recorded only |
| `warn` | Quality status WARNED |
| `error` | Raises `ValidationFailed` after the whole suite runs |
| `critical` | Raises `ValidationHalted` |

```python
.unique("order_id", severity="critical")
```

```python
rl.init(validation=rl.ValidationPolicy(fail_on="critical"))  # e.g. looser in dev
```

### E4. Tolerances (MVP)

```python
c.not_null("phone", mostly=0.95)          # pass if at least 95% of rows are non-null
c.in_range("amount", min=0, max_failures=10)
```

Tolerances work in jobs and in aggregate checks. They can't be expressed as LDP row expectations, so `@rlp.expectations` rejects them with a clear message.

### E5. Evaluate, enforce and notify: three separate settings (MVP) *(review A15)*

| Setting | Off means |
|---|---|
| `evaluate` | checks don't run; no Spark cost |
| `enforce` | results are recorded, nothing raises (observe mode for new rules) |
| `notify` | results never trigger alerts |

```python
rl.init(validation=rl.ValidationPolicy(enforce=False))         # observe mode: run and record
```

```python
rl.init(validation=rl.ValidationPolicy(evaluate=False))        # e.g. a fast dev loop: skip checks
```

```bash
RUNLEDGER_VALIDATION_NOTIFY=false
```

Every result stores the policy in effect, and alerts only consider results with `enforce` and `notify` on.

### E6. Manual handling (MVP)

```python
result = rl.validate(df).not_null("id").run(on_failure="return")
if not result.passed:
    notify_team(result.failures)
```

### E7. Validation decorators (MVP)

```python
@rl.expect(c.not_null("order_id"), c.row_count(min=1))   # validates the returned DataFrame
def clean(df): ...

@rl.expect_input("df", orders_suite)                       # validates an argument first
def publish(df): ...
```

A decorator triggers Spark work when the function returns. Use `suite.explain(df)` (E14) to see that work in advance. To gate a write, prefer the publish gate (E13).

### E8. Reusable, versioned, serializable suites with stable rule identity (MVP) *(review B7)*

```python
orders_suite = rl.Suite(name="orders_silver", version="3", dataset="main.silver.orders",
                        rules=[c.not_null("order_id", key="orders.id_present"),
                               c.unique("order_id")])
orders_suite.run(df)
orders_suite.to_dict()            # JSON-safe; the basis for YAML suites later
```

- `key` is the rule's stable business identity. It is derived automatically when it isn't given.
- `definition_hash` changes when a threshold changes, so history stays continuous and every change stays visible.

### E9. Failing-row samples (v1)

Up to N failing rows per rule are written to `rl_validation_samples`. A column allow-list controls what is kept.

```python
rl.validate(df, sample_rows=20, sample_columns=["order_id", "status"]).not_null("status").run()
```

### E10. Quarantine split (v1)

```python
good, bad = orders_suite.split(df)        # bad has an _rl_failed_rules column
bad.write.mode("append").saveAsTable("main.quarantine.orders")
good.write.mode("overwrite").saveAsTable("main.silver.orders")
```

For uniqueness, every row in a duplicate group goes to `bad` by default. Use `c.unique("order_id", keep="first")` to keep one row per key.

### E11. Condition-task integration (v1)

Validation status is published as a task value, so later tasks can branch on it.

```yaml
condition_task:
  op: EQUAL_TO
  left: "{{tasks.validate.values.runledger.validation_status}}"
  right: "PASSED"
```

### E12. Cost controls for very large tables (v1)

```python
rl.validate(df, where="as_of_date = '2026-11-10'").unique("id", approx=True).run()
```

Approximate uniqueness is **observational**: it can warn, but it never blocks *(review A13)*.

### E13. Write-audit-publish gate (v1; MVP candidate) *(review A14)*

The suite validates exactly the data that will be published. If an enforced rule fails, the target is left untouched.

```python
with rl.publish_gate("main.silver.orders", suite=orders_suite) as gate:
    gate.write(clean(raw))           # 1. write to staging
# 2. audit: the suite runs on that exact Delta version
# 3. publish: the target is updated only if no enforced rule failed; otherwise ValidationFailed
```

### E14. `suite.explain(df)`: see the validation plan before running (MVP) *(review B5)*

```python
orders_suite.explain(df)
```
```text
1 Spark pass (aggregate): not_null(order_id), row_count, unique(order_id) [exact]
+1 pass only if duplicates are found: rows_in_duplicate_groups
+1 pass only if a row rule fails: failing-row samples (20/rule)
Columns read: order_id, status, amount   Filter: as_of_date = '2026-11-10'
```

### E15. Validation scope recorded with every result (MVP) *(review B1)*

Each result records the population it judged:
- the filter and sample fraction
- approx flags
- the source table and its Delta version
- the engine and library version

```sql
SELECT rule_key, observed_value, scope:source_version FROM rl_validation_results
WHERE dataset = 'main.silver.orders' AND scope:filter = 'as_of_date = current_date()';
```

### E16. Rule errors are isolated and fail closed (MVP) *(review A1)*

- A broken rule doesn't silence the others. Preflight catches missing columns and type errors without running a Spark job.
- When a runtime error happens, the rules are re-run in halves to find the culprit.
- An `error` or `critical` rule that can't be evaluated counts as a failure.

```text
rule orders.amount_positive -> ERROR (analysis: column `amount_usd` not found)   [fails closed: severity=error]
rule orders.id_present      -> PASS
rule orders.id_unique       -> PASS
```

---

## F. Lakeflow Declarative Pipelines (LDP)

### F1. Export row rules as expectations (v1) *(review A2)*

Severity and pipeline action are separate:
- By default `warn` → keep rows and record, and `error`/`critical` → fail the update.
- Rows are dropped only when a rule asks for it explicitly.

```python
from pyspark import pipelines as dp
import lakehouse_runledger.ldp as rlp

@dp.table(name="orders_silver")
@rlp.expectations(orders_suite)                      # error -> expect_or_fail (never silently drop)
def orders_silver():
    return spark.readStream.table("orders_bronze")

c.accepted_values("status", ["NEW", "SHIPPED"], ldp_action="drop")   # explicit opt-in to dropping
```

### F2. Aggregate checks inside a pipeline (v1, observational) *(review A3)*

`row_count`, `unique`, `freshness` and `reconcile` run as a private materialized view.

These checks **observe** the published data. They can't stop publication. For gating, add a validation task between the pipeline and its consumers, or use the publish gate (E13).

```python
rlp.aggregate_checks(orders_suite, source="orders_silver")
```

### F3. Event-log ingestion (v1) *(review A4)*

Pipeline expectation results, update status and update errors flow into the same tables as job results. The task runs even when the pipeline fails. Counts the pipeline didn't record are stored as NULL with a reason, never as 0.

```yaml
- task_key: ingest_pipeline_results
  depends_on: [{task_key: run_pipeline}]
  run_if: ALL_DONE
  python_wheel_task: {package_name: lakehouse_runledger, entry_point: runledger,
                      parameters: ["ingest-pipeline", "--pipeline-id", "${resources.pipelines.orders.id}"]}
```

---

## G. Never-start detection

### G1. A manifest built from the bundle (MVP)

The bundle already knows every job, task and schedule. Overrides are optional.

```yaml
# runledger.yml
defaults: {grace_minutes: 30}
jobs:
  orders_daily:
    sla_max_duration_minutes: 90
    criticality: high
    owner: data-eng
    tasks:
      publish: {expected_datasets: [main.silver.orders]}
```

```bash
runledger manifest build
```

```bash
runledger manifest sync       # appends a new manifest_version to rl_manifest
```

### G2. A reconciler that reports only what the evidence supports (MVP; some verdicts in v1) *(review A5, A6, A7)*

Each verdict carries a confidence: CONFIRMED, INFERRED or UNKNOWN.

| Verdict | Meaning |
|---|---|
| `MISSED_SCHEDULE` | No job run matched the expected slot (requires a complete API scan) |
| `EXCLUDED` | The task's `run_if` excluded it, e.g. a failure handler on a good run (info, never alerts) |
| `UPSTREAM_FAILED` | Not run because an upstream task failed |
| `PENDING` | Waiting on upstream tasks that are still running (not late) |
| `FAILED_BEFORE_INIT` | The platform says the task failed, and there's no STARTED row while the logging path was healthy |
| `UNINSTRUMENTED` | Ran, but doesn't use runledger yet (info) |
| `TELEMETRY_GAP` | Our records are missing or incomplete, and the cause isn't proven |
| `OBSERVATION_INCOMPLETE` | The platform data couldn't be read completely; no conclusion |
| `CRASHED` (v1) | STARTED, never FINISHED, and the platform says the task failed |
| `SLA_BREACH` (v1) | Still running past the SLA |

```sql
SELECT slot_key, verdict, cause_confidence FROM rl_v_expected_vs_actual
WHERE verdict NOT IN ('OK', 'EXCLUDED', 'PENDING') AND slot_ts > now() - INTERVAL 1 DAY;
```

Verdicts can be superseded. A slot marked missed that later matches a delayed run is updated, and the history is kept.

### G3. Jobs outside a bundle (v1)

```bash
runledger manifest add --job-id 123 --sla 2h --schedule "0 0 6 * * ?" --tz America/Chicago
```

### G4. Shadow mode and explanations (MVP) *(review B3)*

New jobs are observed without alerting for a trial period, so teams can check verdicts before anyone is paged. Every verdict can be explained.

```bash
runledger reconcile preview --job orders_daily          # upcoming expected slots
```

```bash
runledger reconcile explain expected:1234:987:publish:2026-11-10T06:00
```
```text
verdict EXCLUDED (CONFIRMED)
  manifest v14 (effective 2026-11-01): publish depends_on [validate], run_if ALL_SUCCESS
  matched job run 5551 (triggered 06:00:04); validate result_state FAILED -> publish not eligible
  -> reclassified as UPSTREAM_FAILED
```

### G5. Canary (MVP) *(review B2)*

A tiny scheduled job writes a known record through the library, then checks that it reaches the views. It also checks that the reconciler ran recently.
- If either goes stale, a separate alert fires.
- Canary health is the evidence that lets "failed before init" be INFERRED instead of UNKNOWN.

```yaml
resources:
  jobs:
    runledger_canary: {schedule: {quartz_cron_expression: "0 */15 * * * ?", timezone_id: UTC}}
```

---

## H. Storage, dashboards and alerts

### H1. Typed Unity Catalog tables and DDL (MVP)

There are six tables, all generated from one spec module:
- `rl_task_runs`
- `rl_events`
- `rl_metrics`
- `rl_validation_results`
- `rl_validation_samples`
- `rl_manifest`

They use liquid clustering and carry a schema-version property.

```bash
runledger ddl --catalog ops --schema runledger > 01_tables.sql
```

### H2. Additive schema migrations (MVP)

```python
from lakehouse_runledger.schema import EVENTS, add_columns_sql

add_columns_sql(EVENTS, spark.table("ops.runledger.rl_events").columns, "ops", "runledger")
```

### H3. Views (MVP)

| View | What it shows |
|---|---|
| `rl_v_task_runs` | Current status per task, using the effective attempt (repairs handled) |
| `rl_v_job_runs` | Worst effective-attempt status per job run |
| `rl_v_validation_latest` | Latest result per `rule_key` |
| `rl_v_failure_groups` | Failures grouped by fingerprint |
| `rl_v_expected_vs_actual` | Latest verdict per expected slot |
| `rl_v_delivery_completeness` | Rows reported vs rows present, per task and table |

```sql
SELECT fingerprint, error_class, count, last_seen FROM rl_v_failure_groups ORDER BY count DESC;
```

### H4. Bundle template: setup in three commands (MVP minimal; v1 full)

```bash
databricks bundle init <runledger-repo>/bundle_template
```

```bash
databricks bundle deploy -t dev
```

```bash
databricks bundle run runledger_setup -t dev
```

### H5. AI/BI dashboard (v1)

The dashboard has four pages:
- **Overview:** success rate, p50/p95 duration, unfinished and crashed runs
- **Failures:** top fingerprints, with run links
- **Data quality:** pass rates, observed-value trends, samples
- **Delivery health**

### H6. Databricks SQL alerts (v1; one alert in MVP)

Alerts go to email, Teams or Slack through notification destinations. Validation alerts only consider results with `enforce` and `notify` on. Reconciler alerts skip shadow-mode slots and UNKNOWN verdicts.

```sql
-- "Critical validation failure in the last 15 minutes" (includes rules that couldn't be evaluated)
SELECT count(*) AS failures FROM rl_validation_results
WHERE outcome IN ('FAIL', 'ERROR') AND severity = 'critical'
  AND effective_policy.enforce AND effective_policy.notify
  AND evaluated_at > now() - INTERVAL 15 MINUTES;
```

The MVP ships one alert: an expected run that is missing or failed before init, at CONFIRMED or INFERRED confidence. The canary has its own staleness alert.

### H7. Retention and maintenance job (v1)

```yaml
variables: {retention_days_events: 90, retention_days_samples: 14}
```

---

## I. Extension points

### I1. Notifier seam for webhooks (protocol in v1; implementations Later)

```python
class TeamsNotifier:
    def wants(self, signal):
        return signal.kind == "ValidationFailed" and signal.severity == "critical"

    def send(self, signal):
        post(dbutils.secrets.get("ops", "teams_webhook"), signal.summary())


rl.init(notifiers=[TeamsNotifier()])
```

### I2. YAML and table-driven suites (Later)

```yaml
# suites/orders_silver.yml
name: orders_silver
version: "3"
dataset: main.silver.orders
rules:
  - {type: not_null, columns: [order_id]}
  - {type: unique, columns: [order_id], severity: critical}
```
```python
rl.Suite.from_yaml("suites/orders_silver.yml").run(df)
```

### I3. Other sinks and exporters (Later)

These sinks and exporters are planned:
- `ZerobusSink`, for near-real-time streaming ingestion
- `StatementApiSink`, for producers outside Spark
- OpenTelemetry export

### I4. Anomaly rules from history (Later)

```python
c.anomaly(
    "row_count", baseline_days=28, max_zscore=3
)  # compared with rl_validation_results history
```
