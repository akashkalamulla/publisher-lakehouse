"""Repeated Delta write, read and time-travel proof on the configured S3 API."""

from __future__ import annotations

import importlib.metadata
import platform
import sys
import traceback

from publisher_lakehouse.transform.session import build_spark, lake_uri


def main() -> int:
    spark = None
    try:
        spark = build_spark("lakehouse-delta-smoke")
        print(f"Python: {platform.python_version()}")
        print(f"Java: {spark._jvm.java.lang.System.getProperty('java.version')}")
        print(f"Spark: {spark.version}")
        print(f"Delta: {importlib.metadata.version('delta-spark')}")
        print(f"Hadoop: {spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")

        from delta.tables import DeltaTable

        path = lake_uri("_smoke", "delta_roundtrip")
        spark.range(3).write.format("delta").mode("overwrite").save(path)
        table = DeltaTable.forPath(spark, path)
        v0 = int(table.history(1).select("version").first()[0])
        initial_count = spark.read.format("delta").load(path).count()
        if initial_count != 3:
            raise AssertionError(f"overwrite read count: expected 3, got {initial_count}")

        spark.range(3, 4).write.format("delta").mode("append").save(path)
        latest_count = spark.read.format("delta").load(path).count()
        if latest_count != 4:
            raise AssertionError(f"latest count: expected 4, got {latest_count}")
        old_count = (
            spark.read.format("delta").option("versionAsOf", v0).load(path).count()
        )
        if old_count != 3:
            raise AssertionError(
                f"versionAsOf={v0} count: expected 3, got {old_count}"
            )

        print(f"Table: {path}")
        print(f"Counts: overwrite=3, latest={latest_count}, versionAsOf={v0}={old_count}")
        print("DESCRIBE HISTORY (last five versions):")
        spark.sql(f"DESCRIBE HISTORY delta.`{path}`").select(
            "version", "timestamp", "operation", "operationMetrics"
        ).limit(5).show(truncate=False)
        print("SMOKE PASS")
        return 0
    except Exception as exc:
        print(f"SMOKE FAIL: {exc}", file=sys.stderr)
        traceback.print_exc()
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
