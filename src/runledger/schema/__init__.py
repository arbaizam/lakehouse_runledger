"""Table specs (single source of truth) and DDL generation."""

from runledger.schema.ddl import (
    add_columns_sql,
    create_table_sql,
    render_tables_script,
    table_fqn,
)
from runledger.schema.tables import (
    ALL_TABLES,
    EVENTS,
    MANIFEST,
    METRICS,
    SCHEMA_VERSION_PROPERTY,
    TABLES_BY_KEY,
    TASK_RUNS,
    VALIDATION_RESULTS,
    VALIDATION_SAMPLES,
    Column,
    TableSpec,
)

__all__ = [
    "ALL_TABLES",
    "EVENTS",
    "MANIFEST",
    "METRICS",
    "SCHEMA_VERSION_PROPERTY",
    "TABLES_BY_KEY",
    "TASK_RUNS",
    "VALIDATION_RESULTS",
    "VALIDATION_SAMPLES",
    "Column",
    "TableSpec",
    "add_columns_sql",
    "create_table_sql",
    "render_tables_script",
    "table_fqn",
]
