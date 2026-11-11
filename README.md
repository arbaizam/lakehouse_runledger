# runledger

Event logging and first-class data validation for **Databricks Workflows** (Lakeflow Jobs):
serverless, classic job clusters and Lakeflow Declarative Pipelines.

> Status: **pre-alpha**. Only the table specs and `runledger ddl` exist so far.
> See [docs/design.md](docs/design.md) for the design and roadmap, and
> [docs/features.md](docs/features.md) for every feature with an example.

## The idea in 30 seconds

```python
import runledger as rl
from runledger import checks as c


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

## Development

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -e ".[dev]"
.venv/Scripts/python -m pytest
runledger ddl --catalog main --schema ops
```
