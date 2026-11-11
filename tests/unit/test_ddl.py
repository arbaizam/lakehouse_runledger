import os
import subprocess
import sys
from pathlib import Path

import pytest

from lakehouse_runledger.cli import main
from lakehouse_runledger.schema import ALL_TABLES, EVENTS, TASK_RUNS
from lakehouse_runledger.schema.ddl import (
    add_columns_sql,
    create_table_sql,
    render_tables_script,
    sql_string,
    table_fqn,
)

SNAPSHOT = Path(__file__).parent / "snapshots" / "tables_main_ops.sql"


def test_fqn_is_quoted_and_prefixed():
    assert table_fqn(EVENTS, "main", "ops") == "`main`.`ops`.`rl_events`"
    assert table_fqn(EVENTS, "main", "ops", prefix="") == "`main`.`ops`.`events`"


@pytest.mark.parametrize(
    ("catalog", "schema", "prefix"),
    [
        ("main; DROP TABLE x", "ops", "rl_"),
        ("main", "ops`", "rl_"),
        ("main", "ops", "rl-"),
        ("", "ops", "rl_"),
        ("main", "1ops", "rl_"),
    ],
)
def test_fqn_rejects_unsafe_identifiers(catalog, schema, prefix):
    with pytest.raises(ValueError):
        table_fqn(EVENTS, catalog, schema, prefix)


def test_create_table_has_constraints_clustering_and_version():
    sql = create_table_sql(TASK_RUNS, "main", "ops")
    assert sql.startswith("CREATE TABLE IF NOT EXISTS `main`.`ops`.`rl_task_runs` (")
    assert "`run_key` STRING NOT NULL" in sql
    assert "CLUSTER BY (`event_date`, `job_id`)" in sql
    assert "'runledger.schema_version' = '1'" in sql
    assert "PARTITIONED BY" not in sql
    assert sql.endswith(";")


def test_sql_string_escapes_quotes_and_backslashes():
    assert sql_string("it's") == "'it\\'s'"
    assert sql_string("a\\b") == "'a\\\\b'"


def test_add_columns_only_lists_missing_and_makes_them_nullable():
    existing = [c for c in EVENTS.column_names if c != "attributes" and c != "event_id"]
    sql = add_columns_sql(EVENTS, existing, "main", "ops")
    assert sql is not None
    assert "`attributes` VARIANT" in sql
    assert "`event_id` STRING COMMENT" not in sql or "NOT NULL" not in sql
    assert "NOT NULL" not in sql
    assert "`run_key`" not in sql


def test_add_columns_none_when_up_to_date():
    upper = [c.upper() for c in EVENTS.column_names]
    assert add_columns_sql(EVENTS, upper, "main", "ops") is None


def test_script_contains_every_table():
    script = render_tables_script("main", "ops")
    for spec in ALL_TABLES:
        assert f"`rl_{spec.key}`" in script
    assert script.count("CREATE TABLE IF NOT EXISTS") == len(ALL_TABLES)


def test_ddl_matches_snapshot():
    """DDL drift guard. Regenerate with RUNLEDGER_UPDATE_SNAPSHOTS=1 after an intended change."""
    actual = render_tables_script("main", "ops", include_version=False)
    if os.environ.get("RUNLEDGER_UPDATE_SNAPSHOTS") == "1":
        SNAPSHOT.write_text(actual, encoding="utf-8", newline="\n")
    assert SNAPSHOT.exists(), "snapshot missing; run with RUNLEDGER_UPDATE_SNAPSHOTS=1"
    assert actual == SNAPSHOT.read_text(encoding="utf-8")


def test_cli_ddl_prints_script(capsys):
    assert main(["ddl", "--catalog", "main", "--schema", "ops"]) == 0
    out = capsys.readouterr().out
    assert "`main`.`ops`.`rl_manifest`" in out


def test_cli_ddl_reports_bad_identifier(capsys):
    assert main(["ddl", "--catalog", "bad name", "--schema", "ops"]) == 2
    assert "catalog must be a simple identifier" in capsys.readouterr().err


def test_import_has_no_side_effects_and_no_pyspark():
    code = (
        "import sys, lakehouse_runledger, lakehouse_runledger.schema, lakehouse_runledger.cli;"
        "assert not any(m == 'pyspark' or m.startswith('pyspark.') for m in sys.modules)"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
