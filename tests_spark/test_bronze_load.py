"""Delta behavior on local file URIs, with no object-store dependency."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from publisher_lakehouse.transform.bronze import load_bronze


LOADED_AT = datetime(2026, 9, 26, 12, 0, tzinfo=timezone.utc)


def load(spark, local_lake):
    return load_bronze(
        spark,
        publisher=local_lake["publisher"],
        landing_uri=local_lake["landing_uri"],
        table_uri=local_lake["table_uri"],
        raw_html_uri=local_lake["raw_html_uri"],
        loaded_at=LOADED_AT,
    )


def history(spark, local_lake):
    return spark.sql(
        f"DESCRIBE HISTORY delta.`{local_lake['table_uri']}`"
    ).select("version", "operation").collect()


def test_first_load_rerun_and_new_run_keep_history(spark, local_lake, article_factory):
    from pyspark.sql.types import TimestampType

    source = local_lake["write"]([article_factory(f"hash-{i}") for i in range(3)])
    first = load(spark, local_lake)
    assert (first.landed_rows, first.inserted, first.already_present, first.table_rows) == (
        3, 3, 0, 3
    )
    assert first.raw_html_missing == 0

    rows = spark.read.format("delta").load(local_lake["table_uri"])
    assert rows.count() == 3
    assert rows.select("publisher", "ingest_date").distinct().first()[0] == "sciencedirect"
    assert str(rows.select("ingest_date").first()[0]) == "2026-09-26"
    assert isinstance(rows.schema["scraped_at"].dataType, TimestampType)
    assert rows.filter("scraped_at IS NULL").count() == 0
    assert all(row[0].endswith(source.name) for row in rows.select("_source_file").collect())

    second = load(spark, local_lake)
    assert (second.inserted, second.already_present, second.table_rows) == (0, 3, 3)
    assert [row["operation"] for row in history(spark, local_lake)[:3]] == [
        "MERGE", "MERGE", "CREATE TABLE"
    ]

    local_lake["write"]([article_factory("hash-0", run_id="run-2")], part="run-2")
    third = load(spark, local_lake)
    assert (third.landed_rows, third.inserted, third.already_present, third.table_rows) == (
        4, 1, 3, 4
    )
    assert spark.read.format("delta").load(local_lake["table_uri"]).filter(
        "url_hash = 'hash-0'"
    ).count() == 2


def test_duplicate_batch_key_leaves_table_unchanged(spark, local_lake, article_factory):
    local_lake["write"]([article_factory("original")])
    load(spark, local_lake)
    before = history(spark, local_lake)[0]["version"]
    duplicate = article_factory("duplicate", run_id="run-2")
    local_lake["write"]([duplicate, duplicate], part="run-2")
    with pytest.raises(ValueError, match="Duplicate.*duplicate"):
        load(spark, local_lake)
    assert history(spark, local_lake)[0]["version"] == before


def test_malformed_json_leaves_table_unchanged(spark, local_lake, article_factory):
    local_lake["write"]([article_factory("original")])
    load(spark, local_lake)
    before = history(spark, local_lake)[0]["version"]
    bad_file = local_lake["write"]([article_factory("other", run_id="run-2")], part="run-2")
    with bad_file.open("a", encoding="utf-8") as stream:
        stream.write("this is not JSON\n")
    with pytest.raises(Exception, match="MALFORMED|malformed|FAILFAST"):
        load(spark, local_lake)
    assert history(spark, local_lake)[0]["version"] == before


def test_wrong_publisher_is_rejected(spark, local_lake, article_factory):
    local_lake["write"]([article_factory("wrong", publisher="other")])
    with pytest.raises(ValueError, match="publisher different"):
        load(spark, local_lake)


def test_missing_raw_html_is_reported_but_loads(spark, local_lake, article_factory):
    local_lake["write"]([article_factory("with-html")])
    local_lake["write"](
        [article_factory("without-html", run_id="run-2")], part="run-2", html=False
    )
    result = load(spark, local_lake)
    assert (result.inserted, result.raw_html_missing, result.table_rows) == (2, 1, 2)


def test_append_only_property_rejects_delete(spark, local_lake, article_factory):
    local_lake["write"]([article_factory("immutable")])
    load(spark, local_lake)
    uri = local_lake["table_uri"]
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{uri}`").first()
    assert detail["properties"]["delta.appendOnly"] == "true"
    with pytest.raises(Exception):
        spark.sql(f"DELETE FROM delta.`{uri}` WHERE url_hash = 'immutable'")
    assert spark.read.format("delta").load(uri).count() == 1
