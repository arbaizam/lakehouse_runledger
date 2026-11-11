import pytest

from runledger.schema import ALL_TABLES, SCHEMA_VERSION_PROPERTY, TABLES_BY_KEY, Column, TableSpec
from runledger.schema.tables import TASK_RUNS

RUN_SCOPED = [t for t in ALL_TABLES if t.key != "manifest"]


def test_table_keys_are_unique():
    keys = [t.key for t in ALL_TABLES]
    assert len(keys) == len(set(keys))
    assert set(TABLES_BY_KEY) == set(keys)


@pytest.mark.parametrize("spec", RUN_SCOPED, ids=lambda t: t.key)
def test_run_scoped_tables_share_join_and_filter_columns(spec):
    for name in ("run_key", "event_date", "job_id", "task_key", "env"):
        assert name in spec.column_names, f"{spec.key} lacks {name}"
    assert spec.column("run_key").nullable is False
    assert spec.column("event_date").type == "DATE"
    assert spec.cluster_by[0] == "event_date"


@pytest.mark.parametrize("spec", ALL_TABLES, ids=lambda t: t.key)
def test_every_table_carries_schema_version(spec):
    assert spec.table_properties()[SCHEMA_VERSION_PROPERTY] == str(spec.version)


def test_validation_evidence_is_typed_not_json_strings():
    from runledger.schema import VALIDATION_RESULTS

    assert VALIDATION_RESULTS.column("failed_rows").type == "BIGINT"
    assert VALIDATION_RESULTS.column("observed_value").type == "DOUBLE"
    assert VALIDATION_RESULTS.column("rule_definition").type == "VARIANT"


def test_spec_rejects_duplicate_columns():
    with pytest.raises(ValueError, match="duplicate"):
        TableSpec("t", "c", (Column("a", "INT"), Column("a", "INT")))


def test_spec_rejects_unknown_cluster_key():
    with pytest.raises(ValueError, match="unknown columns"):
        TableSpec("t", "c", (Column("a", "INT"),), cluster_by=("b",))


def test_spec_rejects_too_many_cluster_keys():
    cols = tuple(Column(n, "INT") for n in "abcde")
    with pytest.raises(ValueError, match="cluster keys"):
        TableSpec("t", "c", cols, cluster_by=tuple("abcde"))


@pytest.mark.parametrize("bad", ["Upper", "1abc", "has space", "semi;colon", ""])
def test_column_rejects_non_snake_case_names(bad):
    with pytest.raises(ValueError):
        Column(bad, "INT")


def test_spec_properties_are_read_only():
    with pytest.raises(TypeError):
        TASK_RUNS.properties["x"] = "y"  # type: ignore[index]


def test_column_lookup_errors_name_the_table():
    with pytest.raises(KeyError, match="task_runs"):
        TASK_RUNS.column("nope")
