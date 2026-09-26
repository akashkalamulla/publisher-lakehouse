"""MERGE planning checks on small local Delta tables."""

from __future__ import annotations

import pytest

from publisher_lakehouse.transform.delta_ops import merge_with_plan


def _table(spark, tmp_path):
    uri = (tmp_path / "ops_table").as_uri()
    spark.sql(f"CREATE TABLE delta.`{uri}` (id INT, value STRING) USING DELTA")
    spark.createDataFrame([(1, "one")], "id INT, value STRING").createOrReplaceTempView(
        "delta_ops_source"
    )
    sql = (
        f"MERGE INTO delta.`{uri}` AS target USING delta_ops_source AS source "
        "ON target.id = source.id WHEN NOT MATCHED THEN INSERT *"
    )
    return uri, sql


def _version(spark, uri):
    return int(spark.sql(f"DESCRIBE HISTORY delta.`{uri}`").first()["version"])


def test_zero_plan_skips_merge_and_version(spark, tmp_path):
    uri, sql = _table(spark, tmp_path)
    before = _version(spark, uri)
    outcome = merge_with_plan(
        spark,
        table_uri=uri,
        source_view="delta_ops_source",
        merge_sql=sql,
        planned_inserts=0,
        planned_updates=0,
    )
    assert outcome.skipped and outcome.version == before
    assert _version(spark, uri) == before
    assert spark.read.format("delta").load(uri).count() == 0


def test_insert_plan_matches_metrics_and_one_version(spark, tmp_path):
    uri, sql = _table(spark, tmp_path)
    before = _version(spark, uri)
    outcome = merge_with_plan(
        spark,
        table_uri=uri,
        source_view="delta_ops_source",
        merge_sql=sql,
        planned_inserts=1,
        planned_updates=0,
    )
    assert not outcome.skipped
    assert (outcome.inserted, outcome.updated, outcome.version) == (1, 0, before + 1)
    assert _version(spark, uri) == before + 1


def test_metric_mismatch_raises(spark, tmp_path):
    uri, sql = _table(spark, tmp_path)
    with pytest.raises(RuntimeError, match="MERGE metrics disagree with plan"):
        merge_with_plan(
            spark,
            table_uri=uri,
            source_view="delta_ops_source",
            merge_sql=sql,
            planned_inserts=2,
            planned_updates=0,
        )
    assert _version(spark, uri) == 1
