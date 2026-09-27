"""Publish a pinned Delta star to a separate Postgres warehouse atomically."""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Mapping
from uuid import uuid4

from publisher_lakehouse.transform.serving_schema import (
    GOLD_TABLES,
    STAGED_TABLES,
    all_ddl,
    columns_for,
    ops_ddl,
    quote_ident,
)
from publisher_lakehouse.transform.session import build_spark, lake_uri


TRACKED_TABLES = (
    "bronze/articles",
    "silver/articles",
    "silver/articles_quarantine",
    "silver/dq_results",
) + tuple(f"gold/{table}" for table in GOLD_TABLES)
JDBC_BATCHSIZE = 1000


@dataclass(frozen=True)
class PublishResult:
    status: str
    publish_id: str | None
    versions: dict[str, int]
    row_counts: dict[str, int]
    changed_tables: int


def lake_sources() -> dict[str, str]:
    """Resolve all tracked sources without importing Spark on the host."""

    return {name: lake_uri(*name.split("/")) for name in TRACKED_TABLES}


def _warehouse_config() -> dict[str, str]:
    names = ("WAREHOUSE_HOST", "WAREHOUSE_DB", "WAREHOUSE_USER", "WAREHOUSE_PASSWORD")
    values = {name: os.environ.get(name, "") for name in names}
    missing = [name for name in names if not values[name]]
    if missing:
        raise RuntimeError(f"Missing warehouse environment variable(s): {', '.join(missing)}")
    return values


def _qualified(schema: str, table: str):
    from psycopg import sql

    return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))


def _latest_success(connection, ops_schema: str):
    from psycopg import sql

    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass(%s)", (f"{ops_schema}.publish_log",))
        if cursor.fetchone()[0] is None:
            return None
        cursor.execute(
            sql.SQL(
                "SELECT publish_id, versions FROM {} "
                "WHERE status = 'success' ORDER BY finished_at DESC, publish_id DESC LIMIT 1"
            ).format(_qualified(ops_schema, "publish_log"))
        )
        return cursor.fetchone()


def _delta_plan(spark, source_uris: Mapping[str, str]):
    from delta.tables import DeltaTable

    versions: dict[str, int] = {}
    commits: dict[str, datetime] = {}
    for name in TRACKED_TABLES:
        latest = DeltaTable.forPath(spark, source_uris[name]).history(1).first()
        versions[name] = int(latest["version"])
        timestamp = latest["timestamp"]
        commits[name] = (
            timestamp.replace(tzinfo=timezone.utc)
            if timestamp.tzinfo is None else timestamp.astimezone(timezone.utc)
        )
    return versions, commits


def _ensure_ddl(connection, *, serving_schema: str, stage_schema: str, ops_schema: str):
    # Keep the attempt log available if a later serving/stage DDL statement fails.
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(ops_schema)}")
            cursor.execute(ops_ddl(ops_schema)[1])
    with connection.transaction():
        with connection.cursor() as cursor:
            for statement in all_ddl(
                serving_schema=serving_schema,
                stage_schema=stage_schema,
                ops_schema=ops_schema,
            ):
                cursor.execute(statement)


def _pinned_frames(spark, source_uris: Mapping[str, str], versions: Mapping[str, int]):
    frames = {}
    counts = {}
    for name in TRACKED_TABLES:
        frame = spark.read.format("delta").option(
            "versionAsOf", versions[name]
        ).load(source_uris[name])
        frames[name] = frame
        counts[name] = frame.count()
    return frames, counts


def _stage_frames(frames, *, stage_schema: str, config: Mapping[str, str]):
    jdbc_url = f"jdbc:postgresql://{config['WAREHOUSE_HOST']}:5432/{config['WAREHOUSE_DB']}"
    for table in STAGED_TABLES:
        source_name = f"gold/{table}" if table in GOLD_TABLES else "silver/dq_results"
        frames[source_name].coalesce(4).write.format("jdbc").option(
            "url", jdbc_url
        ).option("dbtable", f"{stage_schema}.{table}").option(
            "user", config["WAREHOUSE_USER"]
        ).option("password", config["WAREHOUSE_PASSWORD"]).option(
            "driver", "org.postgresql.Driver"
        ).option("batchsize", str(JDBC_BATCHSIZE)).option(
            "numPartitions", "4"
        ).option("truncate", "true").mode("overwrite").save()


def _copy_table(cursor, *, target_schema: str, stage_schema: str, table: str):
    from psycopg import sql

    target = _qualified(target_schema, table)
    stage = _qualified(stage_schema, table)
    columns = sql.SQL(", ").join(
        sql.Identifier(name) for name, _ in columns_for(table)
    )
    cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(target))
    cursor.execute(
        sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
            target, columns, columns, stage
        )
    )


def _write_log(
    cursor, *, ops_schema: str, publish_id: str, started_at: datetime,
    finished_at: datetime, status: str, versions: Mapping[str, int],
    row_counts: Mapping[str, int], error: str | None,
):
    from psycopg import sql
    from psycopg.types.json import Jsonb

    cursor.execute(
        sql.SQL(
            "INSERT INTO {} (publish_id, started_at, finished_at, status, "
            "versions, row_counts, error) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        ).format(_qualified(ops_schema, "publish_log")),
        (
            publish_id, started_at, finished_at, status,
            Jsonb(dict(versions)), Jsonb(dict(row_counts)), error,
        ),
    )


def _swap(
    cursor, *, serving_schema: str, stage_schema: str, ops_schema: str,
    versions: Mapping[str, int], commits: Mapping[str, datetime],
    row_counts: Mapping[str, int], published_at: datetime,
):
    from psycopg import sql

    for table in GOLD_TABLES:
        _copy_table(
            cursor, target_schema=serving_schema, stage_schema=stage_schema,
            table=table,
        )
    _copy_table(
        cursor, target_schema=ops_schema, stage_schema=stage_schema,
        table="dq_results",
    )
    layer_status = _qualified(ops_schema, "layer_status")
    cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(layer_status))
    for name in TRACKED_TABLES:
        layer, table_name = name.split("/", 1)
        cursor.execute(
            sql.SQL(
                "INSERT INTO {} (layer, table_name, delta_version, row_count, "
                "last_commit_at, published_at) VALUES (%s, %s, %s, %s, %s, %s)"
            ).format(layer_status),
            (
                layer, table_name, versions[name], row_counts[name],
                commits[name], published_at,
            ),
        )


def verify_serving(
    connection, *, serving_schema: str, ops_schema: str,
    row_counts: Mapping[str, int],
) -> dict[str, int]:
    """Check all copied counts and all fact/bridge foreign-key relationships."""

    from psycopg import sql
    from publisher_lakehouse.transform.schemas import GOLD_SPECS

    orphan_counts: dict[str, int] = {}
    with connection.cursor() as cursor:
        for table in GOLD_TABLES:
            cursor.execute(
                sql.SQL("SELECT COUNT(*) FROM {}").format(_qualified(serving_schema, table))
            )
            actual = cursor.fetchone()[0]
            expected = row_counts[f"gold/{table}"]
            if actual != expected:
                raise RuntimeError(f"serving.{table} count {actual} != pinned Delta {expected}")
        cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM {}").format(_qualified(ops_schema, "dq_results"))
        )
        actual = cursor.fetchone()[0]
        expected = row_counts["silver/dq_results"]
        if actual != expected:
            raise RuntimeError(f"ops.dq_results count {actual} != pinned Delta {expected}")
        for source_table in ("fact_article", "bridge_article_author"):
            for foreign_key, dimension_name in GOLD_SPECS[source_table].foreign_keys:
                dimension_key = GOLD_SPECS[dimension_name].surrogate_key
                cursor.execute(
                    sql.SQL(
                        "SELECT COUNT(*) FROM {} AS source "
                        "LEFT JOIN {} AS dimension ON source.{} = dimension.{} "
                        "WHERE dimension.{} IS NULL"
                    ).format(
                        _qualified(serving_schema, source_table),
                        _qualified(serving_schema, dimension_name),
                        sql.Identifier(foreign_key), sql.Identifier(dimension_key),
                        sql.Identifier(dimension_key),
                    )
                )
                count = cursor.fetchone()[0]
                orphan_counts[f"{source_table}.{foreign_key}"] = count
                if count:
                    raise RuntimeError(
                        f"{source_table}.{foreign_key} has {count} orphan row(s)"
                    )
    return orphan_counts


def build_publish(
    spark, *, source_uris: Mapping[str, str],
    serving_schema: str = "serving", stage_schema: str = "serving_stage",
    ops_schema: str = "ops", force: bool = False, dry_run: bool = False,
    before_swap: Callable[[object], None] | None = None,
) -> PublishResult:
    """Plan, stage pinned Delta versions, and atomically replace live tables."""

    import psycopg

    for schema in (serving_schema, stage_schema, ops_schema):
        quote_ident(schema)
    if len({serving_schema, stage_schema, ops_schema}) != 3:
        raise ValueError("Serving, stage, and ops schemas must be distinct")
    if set(source_uris) != set(TRACKED_TABLES):
        raise ValueError("source_uris must contain exactly the 11 tracked lake tables")
    config = _warehouse_config()
    started_at = datetime.now(timezone.utc)
    publish_id = started_at.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    connection = psycopg.connect(
        host=config["WAREHOUSE_HOST"], port=5432, dbname=config["WAREHOUSE_DB"],
        user=config["WAREHOUSE_USER"], password=config["WAREHOUSE_PASSWORD"],
        autocommit=True,
    )
    write_phase = False
    versions: dict[str, int] = {}
    row_counts: dict[str, int] = {}
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_lock(hashtext(%s))",
                ("publisher_lakehouse.publish",),
            )
        versions, commits = _delta_plan(spark, source_uris)
        previous = _latest_success(connection, ops_schema)
        previous_versions = previous[1] if previous else {}
        changed = sum(
            previous_versions.get(name) != versions[name] for name in TRACKED_TABLES
        )
        if changed == 0 and not force and not dry_run:
            return PublishResult("skipped", previous[0], versions, {}, 0)
        if dry_run:
            return PublishResult("dry_run", previous[0] if previous else None,
                                 versions, {}, changed)
        write_phase = True
        _ensure_ddl(
            connection, serving_schema=serving_schema, stage_schema=stage_schema,
            ops_schema=ops_schema,
        )
        frames, row_counts = _pinned_frames(spark, source_uris, versions)
        _stage_frames(frames, stage_schema=stage_schema, config=config)
        if before_swap is not None:
            before_swap(connection)
        published_at = datetime.now(timezone.utc)
        with connection.transaction():
            with connection.cursor() as cursor:
                _swap(
                    cursor, serving_schema=serving_schema, stage_schema=stage_schema,
                    ops_schema=ops_schema, versions=versions, commits=commits,
                    row_counts=row_counts, published_at=published_at,
                )
                verify_serving(
                    connection, serving_schema=serving_schema,
                    ops_schema=ops_schema, row_counts=row_counts,
                )
                _write_log(
                    cursor, ops_schema=ops_schema, publish_id=publish_id,
                    started_at=started_at, finished_at=datetime.now(timezone.utc),
                    status="success", versions=versions, row_counts=row_counts,
                    error=None,
                )
        verify_serving(
            connection, serving_schema=serving_schema,
            ops_schema=ops_schema, row_counts=row_counts,
        )
        return PublishResult("success", publish_id, versions, row_counts, changed)
    except Exception as exc:
        if write_phase:
            try:
                with connection.transaction():
                    with connection.cursor() as cursor:
                        _write_log(
                            cursor, ops_schema=ops_schema, publish_id=publish_id,
                            started_at=started_at, finished_at=datetime.now(timezone.utc),
                            status="failed", versions=versions, row_counts=row_counts,
                            error=str(exc),
                        )
            except Exception as log_error:
                exc.add_note(f"Could not record failed publish: {log_error}")
        raise
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish a pinned gold star to Postgres")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    spark = None
    try:
        spark = build_spark("publish-gold-to-warehouse")
        result = build_publish(
            spark, source_uris=lake_sources(),
            force=args.force, dry_run=args.dry_run,
        )
        if result.status == "skipped":
            print(f"Publish: skipped — no lake table changed since {result.publish_id}")
        elif result.status == "dry_run":
            print(f"Publish dry-run: changed lake tables {result.changed_tables}")
            for name, version in result.versions.items():
                print(f"  {name}: version {version}")
        else:
            total_rows = (
                sum(result.row_counts[f"gold/{table}"] for table in GOLD_TABLES)
                + result.row_counts["silver/dq_results"] + len(TRACKED_TABLES) + 1
            )
            print(
                f"Publish {result.publish_id}: tables 7 serving + 3 ops | "
                f"rows {total_rows} | changed lake tables {result.changed_tables} | success"
            )
        return 0
    except Exception as exc:
        print(f"Publish failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
