# lakehouse_runledger

Event logging and first-class data validation for **Databricks Workflows** (Lakeflow Jobs):
serverless, classic job clusters and Lakeflow Declarative Pipelines.

> Install as `lakehouse-runledger`, import as `lakehouse_runledger` (alias `rl`). The CLI, table
> prefix (`rl_`), env vars (`RUNLEDGER_*`) and config keys use the short name **runledger**.
>
> Status: **planning**. This repo holds design documents only; no code yet.
> See [docs/design.md](docs/design.md) for the design and roadmap, and
> [docs/features.md](docs/features.md) for every feature with an example, and
> [docs/review-brief.md](docs/review-brief.md) for open decisions and questions.

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

Results land in Unity Catalog Delta tables (`rl_task_runs`, `rl_events`, `rl_metrics`,
`rl_validation_results`, …). A bundle template adds a dashboard, SQL alerts and a reconciler
that also catches tasks that **never started**.

