"""Warehouse integration test; isolated schemas and an explicit opt-in only."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from publisher_lakehouse.transform.gold import build_gold
from publisher_lakehouse.transform.publish import (
    TRACKED_TABLES, build_publish, verify_serving,
)
from publisher_lakehouse.transform.silver import build_silver


pytestmark = pytest.mark.skipif(
    os.environ.get("PL_TEST_WAREHOUSE") != "1",
    reason="PL_TEST_WAREHOUSE=1 is required for warehouse integration",
)
RUN_TS = datetime(2026, 9, 26, 18, 0, tzinfo=timezone.utc)


@pytest.fixture
def warehouse_schemas():
    import psycopg
    from psycopg import sql

    prefix = "pltest_" + uuid4().hex[:10]
    names = {
        "serving_schema": f"{prefix}_serving",
        "stage_schema": f"{prefix}_stage",
        "ops_schema": f"{prefix}_ops",
    }
    config = {
        "host": os.environ["WAREHOUSE_HOST"],
        "port": 5432,
        "dbname": os.environ["WAREHOUSE_DB"],
        "user": os.environ["WAREHOUSE_USER"],
        "password": os.environ["WAREHOUSE_PASSWORD"],
        "autocommit": True,
    }
    with psycopg.connect(**config) as connection:
        with connection.cursor() as cursor:
            for name in names.values():
                cursor.execute(
                    sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name))
                )
            for name in (names["serving_schema"], names["ops_schema"]):
                cursor.execute(
                    sql.SQL("GRANT USAGE ON SCHEMA {} TO grafana_ro").format(
                        sql.Identifier(name)
                    )
                )
                cursor.execute(
                    sql.SQL(
                        "ALTER DEFAULT PRIVILEGES IN SCHEMA {} "
                        "GRANT SELECT ON TABLES TO grafana_ro"
                    ).format(sql.Identifier(name))
                )
    yield names, config
    with psycopg.connect(**config) as connection:
        with connection.cursor() as cursor:
            for name in names.values():
                cursor.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name))
                )


def _source_uris(lake):
    sources = {
        "bronze/articles": lake["bronze_uri"],
        "silver/articles": lake["silver_uri"],
        "silver/articles_quarantine": lake["quarantine_uri"],
        "silver/dq_results": lake["dq_uri"],
    }
    sources.update(
        {f"gold/{table}": f"{lake['gold_root_uri']}/{table}"
         for table in (
             "dim_date", "dim_article_type", "dim_journal", "dim_issue",
             "dim_author", "fact_article", "bridge_article_author",
         )}
    )
    assert set(sources) == set(TRACKED_TABLES)
    return sources


def _silver(spark, lake, when):
    return build_silver(
        spark, publisher=lake["publisher"], bronze_uri=lake["bronze_uri"],
        silver_uri=lake["silver_uri"], quarantine_uri=lake["quarantine_uri"],
        dq_uri=lake["dq_uri"], run_ts=when,
    )


def _gold(spark, lake, when):
    return build_gold(
        spark, publisher=lake["publisher"], silver_uri=lake["silver_uri"],
        gold_root_uri=lake["gold_root_uri"], run_ts=when,
    )


def test_atomic_pinned_publish_skip_change_rollback_and_grafana_role(
    spark, gold_lake, silver_article_factory, bronze_setup, warehouse_schemas
):
    import psycopg
    from psycopg import sql

    schemas, config = warehouse_schemas
    sources = _source_uris(gold_lake)
    records = [
        silver_article_factory(
            "a", scraped_at="2026-09-24T05:00:00Z", english_title="First",
            journal_title="Journal", article_type="Research article",
            authors=[{"name": "Alice", "affiliation": "Lab", "email": ""}],
        ),
        silver_article_factory(
            "b", scraped_at="2026-09-25T05:00:00Z", english_title="Peer",
            journal_title="Journal", article_type="Review article",
            authors=[{"name": "Bob", "affiliation": "Lab", "email": ""}],
        ),
    ]
    bronze_setup(records)
    _silver(spark, gold_lake, RUN_TS)
    _gold(spark, gold_lake, RUN_TS)

    dry = build_publish(spark, source_uris=sources, dry_run=True, **schemas)
    assert dry.status == "dry_run" and dry.changed_tables == len(TRACKED_TABLES)
    with psycopg.connect(**config) as connection:
        assert connection.execute(
            "SELECT to_regclass(%s)",
            (f"{schemas['ops_schema']}.publish_log",),
        ).fetchone()[0] is None

    first = build_publish(spark, source_uris=sources, **schemas)
    assert first.status == "success" and first.changed_tables == len(TRACKED_TABLES)
    with psycopg.connect(**config) as connection:
        stage_fact = f"{schemas['stage_schema']}.fact_article"
        stage_oid = connection.execute(
            "SELECT to_regclass(%s)::oid", (stage_fact,)
        ).fetchone()[0]
        assert stage_oid is not None
        assert connection.execute(
            "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = %s AND contype = 'p'",
            (stage_oid,),
        ).fetchone()[0] == 1
        fact_name = sql.SQL("{}.{}").format(
            sql.Identifier(schemas["serving_schema"]), sql.Identifier("fact_article")
        )
        log_name = sql.SQL("{}.{}").format(
            sql.Identifier(schemas["ops_schema"]), sql.Identifier("publish_log")
        )
        assert connection.execute(
            sql.SQL("SELECT COUNT(*) FROM {}").format(fact_name)
        ).fetchone()[0] == 2
        assert connection.execute(
            sql.SQL("SELECT status FROM {} WHERE publish_id = %s").format(log_name),
            (first.publish_id,),
        ).fetchone()[0] == "success"
        assert connection.execute(
            sql.SQL("SELECT COUNT(*) FROM {}").format(log_name)
        ).fetchone()[0] == 1
        assert all(
            count == 0 for count in verify_serving(
                connection, serving_schema=schemas["serving_schema"],
                ops_schema=schemas["ops_schema"], row_counts=first.row_counts,
            ).values()
        )
        alice_name = sql.SQL("{}.{}").format(
            sql.Identifier(schemas["serving_schema"]), sql.Identifier("dim_author")
        )
        postgres_first_seen = connection.execute(
            sql.SQL("SELECT first_seen_at FROM {} WHERE author_nk = 'alice'").format(
                alice_name
            )
        ).fetchone()[0]
        silver_first_seen = spark.read.format("delta").load(
            gold_lake["silver_uri"]
        ).filter("url_hash = 'a'").first()["first_seen_at"]
        assert postgres_first_seen == silver_first_seen.replace(tzinfo=timezone.utc)

    rerun = build_publish(spark, source_uris=sources, **schemas)
    assert rerun.status == "skipped" and rerun.publish_id == first.publish_id
    with psycopg.connect(**config) as connection:
        assert connection.execute(
            sql.SQL("SELECT COUNT(*) FROM {}").format(log_name)
        ).fetchone()[0] == 1

    # The isolated grants mirror production's read-only role without exposing stage.
    with psycopg.connect(**config) as connection:
        connection.execute(
            sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO grafana_ro").format(
                sql.Identifier(schemas["serving_schema"])
            )
        )
    ro_config = {
        **config, "user": "grafana_ro",
        "password": os.environ["GRAFANA_DB_PASSWORD"],
    }
    with psycopg.connect(**ro_config) as readonly:
        assert readonly.execute(
            sql.SQL("SELECT COUNT(*) FROM {}").format(fact_name)
        ).fetchone()[0] == 2
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            readonly.execute(
                sql.SQL("SELECT * FROM {}.{}").format(
                    sql.Identifier(schemas["stage_schema"]),
                    sql.Identifier("fact_article"),
                )
            )

    changed = silver_article_factory(
        "a", run_id="run-2", scraped_at="2026-09-27T05:00:00Z",
        english_title="Second", journal_title="Journal",
        article_type="Research article",
        authors=[{"name": "Alice", "affiliation": "Lab", "email": ""}],
    )
    bronze_setup([changed], part="run-2")
    _silver(spark, gold_lake, RUN_TS + timedelta(hours=1))
    _gold(spark, gold_lake, RUN_TS + timedelta(hours=1))
    second = build_publish(spark, source_uris=sources, **schemas)
    assert second.status == "success" and second.changed_tables > 0
    with psycopg.connect(**config) as connection:
        assert connection.execute(
            "SELECT to_regclass(%s)::oid", (stage_fact,)
        ).fetchone()[0] == stage_oid
        assert connection.execute(
            sql.SQL("SELECT english_title FROM {} WHERE url_hash = 'a'").format(
                fact_name
            )
        ).fetchone()[0] == "Second"

    third_record = silver_article_factory(
        "a", run_id="run-3", scraped_at="2026-09-28T05:00:00Z",
        english_title="Third", journal_title="Journal",
        article_type="Research article",
        authors=[{"name": "Alice", "affiliation": "Lab", "email": ""}],
    )
    bronze_setup([third_record], part="run-3")
    _silver(spark, gold_lake, RUN_TS + timedelta(hours=2))
    _gold(spark, gold_lake, RUN_TS + timedelta(hours=2))

    def remove_staged_bridge(connection):
        connection.execute(
            sql.SQL("DROP TABLE {}.{}").format(
                sql.Identifier(schemas["stage_schema"]),
                sql.Identifier("bridge_article_author"),
            )
        )

    with pytest.raises(Exception):
        build_publish(
            spark, source_uris=sources, before_swap=remove_staged_bridge, **schemas
        )
    with psycopg.connect(**config) as connection:
        assert connection.execute(
            sql.SQL("SELECT english_title FROM {} WHERE url_hash = 'a'").format(
                fact_name
            )
        ).fetchone()[0] == "Second"
        assert connection.execute(
            sql.SQL("SELECT COUNT(*) FROM {} WHERE status = 'failed'").format(
                log_name
            )
        ).fetchone()[0] == 1
