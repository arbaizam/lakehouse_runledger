# lakehouse_runledger

Run records, data checks and missing-run detection for **Databricks Workflows** (Lakeflow Jobs):
serverless, classic job clusters and Lakeflow Declarative Pipelines.

> Install as `lakehouse-runledger`, import as `lakehouse_runledger` (alias `rl`). The CLI, table
> prefix (`rl_`), env vars (`RUNLEDGER_*`) and config keys use the short name **runledger**.
>
> **Status: planning.** This repo holds the design only; no code yet.
> Everything is in one document: **[docs/design.md](docs/design.md)**.

| Start here | |
|---|---|
| Reviewing the design | [Review guide](docs/design.md#review-guide): agenda, outputs, what to read |
| What it is, in 5 minutes | [Part 1 · Overview](docs/design.md#part-1--overview) |
| Open decisions and questions | [Part 5 · For the review](docs/design.md#part-5--for-the-review) |
| How the team runs it | [§14 Governance](docs/design.md#14-governance) |

## The idea in 30 seconds (proposed API)

```python
import lakehouse_runledger as rl
from lakehouse_runledger import checks as c


@rl.task  # STARTED/FINISHED, status, duration, errors
def main():
    publish(clean(load_orders()))


@rl.step  # a timed span per function
def load_orders():
    return spark.table("main.raw.orders")


@rl.step
@rl.expect(c.not_null("order_id"), c.unique("order_id", severity="critical"))
def clean(df):  # returned DataFrame validated in one Spark pass
    return df.dropDuplicates(["order_id"])
```

Results land in seven Unity Catalog Delta tables (`rl_task_runs`, `rl_events`, `rl_metrics`,
`rl_validation_results`, …). A bundle template adds a dashboard, SQL alerts and a reconciler that
also catches tasks that **never started**.
