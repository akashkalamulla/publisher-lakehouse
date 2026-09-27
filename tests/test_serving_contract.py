"""Host-only contracts for the Postgres serving schema and publish imports."""

from __future__ import annotations

import importlib
import sys

import pytest

from publisher_lakehouse.transform.schemas import DQ_RESULT_COLUMNS, GOLD_SPECS
from publisher_lakehouse.transform.serving_schema import (
    GOLD_TABLES,
    GRANT_SCHEMAS,
    POSTGRES_TYPE_MAP,
    columns_for,
    index_ddl,
    postgres_type,
    primary_key_for,
    table_ddl,
)


def test_published_columns_match_gold_names_and_order():
    for table in GOLD_TABLES:
        assert [name for name, _ in columns_for(table)] == [
            name for name, _ in GOLD_SPECS[table].columns
        ]
    assert [name for name, _ in columns_for("dq_results")] == [
        name for name, _ in DQ_RESULT_COLUMNS
    ]


def test_every_spark_type_maps_and_unknown_type_raises():
    assert POSTGRES_TYPE_MAP == {
        "BIGINT": "BIGINT", "INT": "INTEGER", "STRING": "TEXT",
        "BOOLEAN": "BOOLEAN", "DOUBLE": "DOUBLE PRECISION",
        "DATE": "DATE", "TIMESTAMP": "TIMESTAMPTZ",
    }
    for table in GOLD_TABLES:
        for _, kind in GOLD_SPECS[table].columns:
            assert postgres_type(kind) == POSTGRES_TYPE_MAP[kind]
    with pytest.raises(ValueError, match="No Postgres mapping"):
        postgres_type("ARRAY<STRING>")


def test_primary_keys_match_surrogate_and_bridge_grain():
    for table in GOLD_TABLES:
        if table == "bridge_article_author":
            assert primary_key_for(table) == (
                "article_sk", "role", "author_position"
            )
        else:
            assert primary_key_for(table) == (GOLD_SPECS[table].surrogate_key,)


def test_stage_definition_equals_serving_without_indexes():
    for table in GOLD_TABLES:
        assert table_ddl("serving_stage", table) == table_ddl(
            "serving", table
        ).replace('"serving".', '"serving_stage".')
    assert index_ddl("serving")
    assert all('"serving".' in statement for statement in index_ddl("serving"))
    assert all('"serving_stage".' not in statement for statement in index_ddl("serving"))


def test_read_only_grant_targets_are_live_schemas():
    assert GRANT_SCHEMAS == ("serving", "ops")
    assert "serving_stage" not in GRANT_SCHEMAS


def test_imports_do_not_load_pyspark_or_psycopg():
    before = {
        name for name in sys.modules
        if name.startswith("pyspark") or name.startswith("psycopg")
    }
    importlib.import_module("publisher_lakehouse.transform.serving_schema")
    importlib.import_module("publisher_lakehouse.transform.publish")
    after = {
        name for name in sys.modules
        if name.startswith("pyspark") or name.startswith("psycopg")
    }
    assert after == before
