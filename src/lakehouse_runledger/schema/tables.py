"""Single source of truth for every runledger table.

Everything that touches table shape derives from the specs in this module:
the DDL generator, the Delta sink's write schema and ``validate()`` check,
the bundle template SQL, and the documentation. Change a table here and
nowhere else.

Types are Databricks SQL type strings. This module must import without pyspark.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

SCHEMA_VERSION_PROPERTY = "runledger.schema_version"

_COLUMN_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_MAX_CLUSTER_KEYS = 4  # Delta liquid clustering limit


@dataclass(frozen=True)
class Column:
    """One table column."""

    name: str
    type: str
    nullable: bool = True
    comment: str = ""

    def __post_init__(self) -> None:
        if not _COLUMN_NAME.match(self.name):
            raise ValueError(f"column name must be lower snake_case: {self.name!r}")
        if not self.type.strip():
            raise ValueError(f"column {self.name!r} has an empty type")


@dataclass(frozen=True)
class TableSpec:
    """Shape and storage settings of one table.

    ``key`` is the logical name (``"events"``); the physical name is
    ``prefix + key`` (``"rl_events"`` by default), so teams can choose a prefix.
    """

    key: str
    comment: str
    columns: tuple[Column, ...]
    cluster_by: tuple[str, ...] = ()
    version: int = 1
    properties: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        names = [c.name for c in self.columns]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise ValueError(f"table {self.key!r} has duplicate columns: {duplicates}")
        missing = [k for k in self.cluster_by if k not in names]
        if missing:
            raise ValueError(f"table {self.key!r} clusters by unknown columns: {missing}")
        if len(self.cluster_by) > _MAX_CLUSTER_KEYS:
            raise ValueError(f"table {self.key!r} has more than {_MAX_CLUSTER_KEYS} cluster keys")
        if self.version < 1:
            raise ValueError(f"table {self.key!r} version must be >= 1")
        object.__setattr__(self, "properties", MappingProxyType(dict(self.properties)))

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(f"table {self.key!r} has no column {name!r}")

    def table_name(self, prefix: str = "rl_") -> str:
        return f"{prefix}{self.key}"

    def table_properties(self) -> dict[str, str]:
        """Properties written on CREATE, including the schema version marker."""
        return {**self.properties, SCHEMA_VERSION_PROPERTY: str(self.version)}


# --------------------------------------------------------------------------------------
# Reusable column groups
# --------------------------------------------------------------------------------------

ERROR_STRUCT = (
    "STRUCT<error_class: STRING, message: STRING, fingerprint: STRING, frames: ARRAY<STRING>>"
)

# Copied onto every run-scoped table so dashboards and alerts rarely need joins.
_FILTER_COLUMNS = (
    Column("job_id", "BIGINT", comment="Databricks job id."),
    Column("job_run_id", "BIGINT", comment="Databricks job run id."),
    Column("task_key", "STRING", comment="Task key within the job."),
    Column("env", "STRING", comment="Deployment environment, e.g. dev or prod."),
)

_RUN_KEY = Column(
    "run_key",
    "STRING",
    nullable=False,
    comment="One task attempt: job:{workspace_id}:{task_run_id}, ldp:{pipeline}:{update} "
    "or local:{uuid}.",
)

_EVENT_DATE = Column("event_date", "DATE", nullable=False, comment="UTC date of the record.")

_DEFAULT_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}


# --------------------------------------------------------------------------------------
# Tables
# --------------------------------------------------------------------------------------

TASK_RUNS = TableSpec(
    key="task_runs",
    comment="One row per task-run lifecycle transition (STARTED, FINISHED, RECONCILED).",
    columns=(
        Column("record_id", "STRING", nullable=False, comment="Unique id of this row (uuid7)."),
        _RUN_KEY,
        Column("transition", "STRING", nullable=False, comment="STARTED, FINISHED or RECONCILED."),
        Column(
            "status",
            "STRING",
            nullable=False,
            comment="RUNNING, SUCCEEDED, WARNED, FAILED, CANCELLED, or a reconciler status.",
        ),
        Column("source", "STRING", nullable=False, comment="library, reconciler or ldp_event_log."),
        Column("workspace_id", "BIGINT"),
        *_FILTER_COLUMNS,
        Column("task_run_id", "BIGINT"),
        Column("parent_run_key", "STRING", comment="Run key of a calling job (run_job_task)."),
        Column("job_name", "STRING"),
        Column("attempt", "INT"),
        Column("repair_count", "INT"),
        Column("trigger_type", "STRING"),
        Column("pipeline_id", "STRING"),
        Column("pipeline_update_id", "STRING"),
        Column("compute_type", "STRING", comment="serverless, classic, ldp or local."),
        Column("runtime_version", "STRING"),
        Column("cluster_id", "STRING"),
        Column("python_version", "STRING"),
        Column("runledger_version", "STRING"),
        Column("app_name", "STRING"),
        Column("app_version", "STRING"),
        Column("run_as", "STRING"),
        Column("params", "MAP<STRING, STRING>", comment="Task parameters, redacted."),
        Column("tags", "MAP<STRING, STRING>"),
        Column(
            "context_sources",
            "MAP<STRING, STRING>",
            comment="Which provider supplied each context field.",
        ),
        Column("started_at", "TIMESTAMP"),
        Column("ended_at", "TIMESTAMP"),
        Column("duration_ms", "BIGINT"),
        Column("error", ERROR_STRUCT),
        Column(
            "validation_summary",
            "STRUCT<total: INT, passed: INT, failed: INT, errored: INT, skipped: INT, "
            "worst_failed_severity: STRING>",
        ),
        Column(
            "delivery",
            "STRUCT<emitted: BIGINT, flushed: BIGINT, dropped: BIGINT, retries: INT, "
            "flush_failures: INT>",
            comment="Logger delivery health at the time of this transition.",
        ),
        Column("event_ts", "TIMESTAMP", nullable=False),
        _EVENT_DATE,
    ),
    cluster_by=("event_date", "job_id"),
    properties=_DEFAULT_PROPERTIES,
)

EVENTS = TableSpec(
    key="events",
    comment="Log lines and span boundaries emitted by instrumented tasks.",
    columns=(
        Column("event_id", "STRING", nullable=False),
        _RUN_KEY,
        Column("span_id", "STRING"),
        Column("parent_span_id", "STRING"),
        Column("seq", "BIGINT", comment="Per-process monotonic sequence number."),
        Column("event_ts", "TIMESTAMP", nullable=False),
        _EVENT_DATE,
        Column("level", "STRING", nullable=False, comment="DEBUG, INFO, WARNING, ERROR, CRITICAL."),
        Column("kind", "STRING", nullable=False, comment="log, span_start, span_end, checkpoint."),
        Column("name", "STRING"),
        Column("message", "STRING"),
        Column("status", "STRING"),
        Column("duration_ms", "BIGINT"),
        Column("error", ERROR_STRUCT),
        Column("attributes", "VARIANT", comment="Free-form attributes, redacted and size-capped."),
        *_FILTER_COLUMNS,
    ),
    cluster_by=("event_date", "job_id"),
    properties=_DEFAULT_PROPERTIES,
)

METRICS = TableSpec(
    key="metrics",
    comment="Narrow numeric facts for trend analysis.",
    columns=(
        Column("metric_id", "STRING", nullable=False),
        _RUN_KEY,
        Column("span_id", "STRING"),
        Column("metric_ts", "TIMESTAMP", nullable=False),
        _EVENT_DATE,
        Column("name", "STRING", nullable=False),
        Column("value", "DOUBLE", nullable=False),
        Column("unit", "STRING"),
        Column("dataset", "STRING"),
        Column("tags", "MAP<STRING, STRING>"),
        *_FILTER_COLUMNS,
    ),
    cluster_by=("event_date", "name"),
    properties=_DEFAULT_PROPERTIES,
)

VALIDATION_RESULTS = TableSpec(
    key="validation_results",
    comment="One row per validation rule evaluation.",
    columns=(
        Column("result_id", "STRING", nullable=False),
        _RUN_KEY,
        Column("span_id", "STRING"),
        Column("suite_name", "STRING"),
        Column("suite_version", "STRING"),
        Column("dataset", "STRING"),
        Column(
            "rule_id",
            "STRING",
            nullable=False,
            comment="Stable hash of rule type and parameters (excludes name and severity).",
        ),
        Column("rule_name", "STRING"),
        Column("rule_type", "STRING", nullable=False),
        Column("rule_columns", "ARRAY<STRING>", comment="Columns the rule inspects."),
        Column("severity", "STRING", nullable=False, comment="info, warn, error or critical."),
        Column("outcome", "STRING", nullable=False, comment="PASS, FAIL, ERROR or SKIPPED."),
        Column("action", "STRING", comment="none, warned, failed_task or halted."),
        Column("total_rows", "BIGINT"),
        Column("failed_rows", "BIGINT"),
        Column("failed_ratio", "DOUBLE"),
        Column("observed_value", "DOUBLE"),
        Column("threshold", "VARIANT"),
        Column("observed", "VARIANT"),
        Column("rule_definition", "VARIANT", comment="Serialized rule (Rule.to_dict())."),
        Column("sample_count", "INT"),
        Column("source", "STRING", nullable=False, comment="spark or ldp_event_log."),
        Column("evaluated_at", "TIMESTAMP", nullable=False),
        Column("duration_ms", "BIGINT"),
        _EVENT_DATE,
        *_FILTER_COLUMNS,
    ),
    cluster_by=("event_date", "dataset"),
    properties=_DEFAULT_PROPERTIES,
)

VALIDATION_SAMPLES = TableSpec(
    key="validation_samples",
    comment="Bounded samples of rows that failed validation. May contain PII: restrict grants.",
    columns=(
        Column("result_id", "STRING", nullable=False),
        _RUN_KEY,
        Column("rule_id", "STRING", nullable=False),
        Column("dataset", "STRING"),
        Column("sampled_at", "TIMESTAMP", nullable=False),
        _EVENT_DATE,
        Column("row_data", "VARIANT", comment="The failing row after column allow/redact lists."),
        Column("row_hash", "STRING"),
        *_FILTER_COLUMNS,
    ),
    cluster_by=("event_date", "dataset"),
    properties=_DEFAULT_PROPERTIES,
)

MANIFEST = TableSpec(
    key="manifest",
    comment="Expected tasks per manifest version; drives never-start detection.",
    columns=(
        Column("manifest_version", "STRING", nullable=False),
        Column("synced_at", "TIMESTAMP", nullable=False),
        Column("bundle_name", "STRING"),
        Column("env", "STRING"),
        Column("job_name", "STRING", nullable=False),
        Column("job_id", "BIGINT", comment="Resolved at sync time."),
        Column("task_key", "STRING", nullable=False),
        Column("schedule_cron", "STRING", comment="Quartz cron expression, if scheduled."),
        Column("schedule_tz", "STRING"),
        Column("trigger_kind", "STRING", comment="schedule, file_arrival, continuous, manual."),
        Column("grace_minutes", "INT", comment="How late a run may start before it is missed."),
        Column("min_runs_per_hours", "INT", comment="For non-cron triggers: one run per N hours."),
        Column("sla_max_duration_minutes", "INT"),
        Column("criticality", "STRING"),
        Column("instrumented", "BOOLEAN", comment="Whether the task is expected to use runledger."),
        Column("expected_datasets", "ARRAY<STRING>"),
        Column("expected_suites", "ARRAY<STRING>"),
        Column("owner", "STRING"),
        Column("active", "BOOLEAN", nullable=False),
    ),
    cluster_by=("job_id",),
    properties=_DEFAULT_PROPERTIES,
)

ALL_TABLES: tuple[TableSpec, ...] = (
    TASK_RUNS,
    EVENTS,
    METRICS,
    VALIDATION_RESULTS,
    VALIDATION_SAMPLES,
    MANIFEST,
)

TABLES_BY_KEY: Mapping[str, TableSpec] = MappingProxyType({t.key: t for t in ALL_TABLES})
