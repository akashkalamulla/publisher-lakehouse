"""Local-path checks for Delta maintenance: compaction never changes data."""

from __future__ import annotations

from datetime import datetime

import pytest

from publisher_lakehouse.transform.maintenance import (
    data_fingerprint,
    main,
    run_maintenance,
    snapshot,
)


SCHEMA = "id INT, label STRING, tags ARRAY<STRING>, seen TIMESTAMP"


def _rows(start: int, count: int):
    return [
        (i, f"row-{i}", [f"t{i}", "shared"], datetime(2026, 9, 27, 3, 0, i % 60))
        for i in range(start, start + count)
    ]


def _append(spark, uri: str, rows) -> None:
    spark.createDataFrame(rows, SCHEMA).coalesce(1).write.format("delta").mode("append").save(uri)


def _small_file_table(spark, tmp_path, name: str, appends: int, *, append_only: bool = False) -> str:
    uri = (tmp_path / name).as_uri()
    for index in range(appends):
        _append(spark, uri, _rows(index * 3, 3))
        if append_only and index == 0:
            spark.sql(f"ALTER TABLE delta.`{uri}` SET TBLPROPERTIES ('delta.appendOnly' = 'true')")
    return uri


def _operations(spark, uri: str) -> list[str]:
    from delta.tables import DeltaTable

    return [row["operation"] for row in DeltaTable.forPath(spark, uri).history().collect()]


def test_many_small_appends_are_compacted_without_changing_data(spark, tmp_path):
    uri = _small_file_table(spark, tmp_path, "small_appends", appends=6)
    before = snapshot(spark, uri)
    assert before.num_files == 6 and before.rows == 18

    [report] = run_maintenance(spark, {"t/small_appends": uri})

    assert report.action == "optimized"
    assert report.after.num_files == 1 < report.before.num_files
    assert (report.after.rows, report.after.checksum) == (before.rows, before.checksum)
    assert report.after.version == before.version + 1
    assert _operations(spark, uri)[0] == "OPTIMIZE"
    assert snapshot(spark, uri) == report.after


def test_single_file_table_is_skipped(spark, tmp_path):
    uri = _small_file_table(spark, tmp_path, "single", appends=1)
    [report] = run_maintenance(spark, {"t/single": uri})
    assert (report.action, report.reason) == ("skipped", "single file")
    assert report.after == report.before
    assert "OPTIMIZE" not in _operations(spark, uri)


def test_tables_of_large_files_are_skipped(spark, tmp_path):
    uri = _small_file_table(spark, tmp_path, "large_enough", appends=3)
    [report] = run_maintenance(spark, {"t/large_enough": uri}, small_file_bytes=1)
    assert report.action == "skipped" and report.reason.startswith("average file")
    assert report.after.version == report.before.version


def test_short_vacuum_retention_is_refused_before_any_write(spark, tmp_path, capsys):
    uri = _small_file_table(spark, tmp_path, "vacuum_guard", appends=4)
    before = snapshot(spark, uri)

    with pytest.raises(ValueError, match=r"--vacuum-hours 24 is below the 168-hour \(7-day\) minimum"):
        run_maintenance(spark, {"t/vacuum_guard": uri}, vacuum_hours=24)
    assert main(["--vacuum-hours", "24"]) == 2
    assert "Maintenance refused: --vacuum-hours 24 is below" in capsys.readouterr().err

    assert snapshot(spark, uri) == before
    assert not any(op.startswith(("VACUUM", "OPTIMIZE")) for op in _operations(spark, uri))
    assert spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled", "true") == "true"


def test_dry_run_reports_the_plan_and_writes_nothing(spark, tmp_path):
    many = _small_file_table(spark, tmp_path, "dry_many", appends=5)
    one = _small_file_table(spark, tmp_path, "dry_one", appends=1)
    history = {uri: _operations(spark, uri) for uri in (many, one)}

    reports = run_maintenance(spark, {"t/many": many, "t/one": one}, dry_run=True)

    assert [(r.table, r.action) for r in reports] == [
        ("t/many", "would-optimize"), ("t/one", "would-skip"),
    ]
    assert all(r.after == r.before for r in reports)
    assert {uri: _operations(spark, uri) for uri in (many, one)} == history
    assert snapshot(spark, many).num_files == 5


def test_append_only_table_can_be_compacted_and_stays_append_only(spark, tmp_path):
    uri = _small_file_table(spark, tmp_path, "append_only", appends=4, append_only=True)
    before = snapshot(spark, uri)

    [report] = run_maintenance(spark, {"t/append_only": uri})

    assert report.action == "optimized" and report.after.num_files == 1
    assert (report.after.rows, report.after.checksum) == (before.rows, before.checksum)
    properties = spark.sql(f"DESCRIBE DETAIL delta.`{uri}`").first()["properties"]
    assert properties.get("delta.appendOnly") == "true"
    with pytest.raises(Exception, match="(?i)append"):
        spark.sql(f"DELETE FROM delta.`{uri}` WHERE id = 0")


def test_fingerprint_ignores_row_order_but_not_values(spark):
    rows = _rows(0, 5)
    base = data_fingerprint(spark.createDataFrame(rows, SCHEMA))
    reordered = data_fingerprint(spark.createDataFrame(list(reversed(rows)), SCHEMA).repartition(3))
    changed = data_fingerprint(spark.createDataFrame(rows[:-1] + [(4, "row-x", [], None)], SCHEMA))
    assert reordered == base
    assert changed[0] == base[0] and changed[1] != base[1]
