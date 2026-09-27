"""Postgres DDL for the version-pinned gold serving snapshot."""

from __future__ import annotations

import re

from publisher_lakehouse.transform.schemas import (
    DQ_RESULT_COLUMNS,
    GOLD_BUILD_ORDER,
    GOLD_SPECS,
)


POSTGRES_TYPE_MAP = {
    "BIGINT": "BIGINT",
    "INT": "INTEGER",
    "STRING": "TEXT",
    "BOOLEAN": "BOOLEAN",
    "DOUBLE": "DOUBLE PRECISION",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMPTZ",
}
GOLD_TABLES = GOLD_BUILD_ORDER
STAGED_TABLES = GOLD_TABLES + ("dq_results",)
GRANT_SCHEMAS = ("serving", "ops")


def quote_ident(value: str) -> str:
    """Accept only simple identifiers for isolated integration-test schemas."""

    if not re.fullmatch(r"[a-z_][a-z0-9_]*", value):
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return f'"{value}"'


def postgres_type(spark_type: str) -> str:
    try:
        return POSTGRES_TYPE_MAP[spark_type]
    except KeyError as exc:
        raise ValueError(f"No Postgres mapping for Spark type {spark_type!r}") from exc


def columns_for(table: str) -> tuple[tuple[str, str], ...]:
    if table == "dq_results":
        source = DQ_RESULT_COLUMNS
    else:
        source = GOLD_SPECS[table].columns
    return tuple((name, postgres_type(kind)) for name, kind in source)


def primary_key_for(table: str) -> tuple[str, ...]:
    if table == "dq_results":
        return ("dq_run_id", "rule")
    if table == "bridge_article_author":
        return GOLD_SPECS[table].natural_key
    surrogate = GOLD_SPECS[table].surrogate_key
    if surrogate is None:
        raise ValueError(f"No surrogate key for {table}")
    return (surrogate,)


def table_ddl(schema: str, table: str) -> str:
    """Return the identical serving/stage table definition, without indexes."""

    qualified = f"{quote_ident(schema)}.{quote_ident(table)}"
    columns = [
        f"{quote_ident(name)} {kind}" for name, kind in columns_for(table)
    ]
    primary_key = ", ".join(quote_ident(name) for name in primary_key_for(table))
    columns.append(f"PRIMARY KEY ({primary_key})")
    return f"CREATE TABLE IF NOT EXISTS {qualified} ({', '.join(columns)})"


def index_ddl(schema: str) -> tuple[str, ...]:
    """Indexes exist only on live serving tables."""

    targets = [
        ("fact_article", foreign_key)
        for foreign_key, _ in GOLD_SPECS["fact_article"].foreign_keys
    ] + [
        ("fact_article", "publisher"),
        ("bridge_article_author", "publisher"),
        ("bridge_article_author", "author_sk"),
    ]
    return tuple(
        "CREATE INDEX IF NOT EXISTS "
        f"{quote_ident('idx_' + schema + '_' + table + '_' + column)} "
        f"ON {quote_ident(schema)}.{quote_ident(table)} ({quote_ident(column)})"
        for table, column in targets
    )


def ops_ddl(schema: str) -> tuple[str, str]:
    qualified = quote_ident(schema)
    return (
        f"CREATE TABLE IF NOT EXISTS {qualified}.\"layer_status\" ("
        '"layer" TEXT NOT NULL, "table_name" TEXT NOT NULL, '
        '"delta_version" BIGINT NOT NULL, "row_count" BIGINT NOT NULL, '
        '"last_commit_at" TIMESTAMPTZ NOT NULL, '
        '"published_at" TIMESTAMPTZ NOT NULL, '
        'PRIMARY KEY ("layer", "table_name"))',
        f"CREATE TABLE IF NOT EXISTS {qualified}.\"publish_log\" ("
        '"publish_id" TEXT PRIMARY KEY, "started_at" TIMESTAMPTZ NOT NULL, '
        '"finished_at" TIMESTAMPTZ NOT NULL, '
        '"status" TEXT NOT NULL CHECK ("status" IN (\'success\', \'failed\')), '
        '"versions" JSONB NOT NULL, "row_counts" JSONB NOT NULL, "error" TEXT)',
    )


def all_ddl(
    *, serving_schema: str = "serving", stage_schema: str = "serving_stage",
    ops_schema: str = "ops",
) -> tuple[str, ...]:
    schemas = (serving_schema, stage_schema, ops_schema)
    if len(set(schemas)) != len(schemas):
        raise ValueError("Serving, stage, and ops schemas must be distinct")
    statements = [
        f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)}" for schema in schemas
    ]
    statements.extend(table_ddl(serving_schema, name) for name in GOLD_TABLES)
    statements.extend(table_ddl(stage_schema, name) for name in STAGED_TABLES)
    statements.append(table_ddl(ops_schema, "dq_results"))
    statements.extend(ops_ddl(ops_schema))
    statements.extend(index_ddl(serving_schema))
    return tuple(statements)
