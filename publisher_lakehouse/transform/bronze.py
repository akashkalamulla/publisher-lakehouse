"""Load landed article JSONL into the append-only bronze Delta table."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from publisher_lakehouse.transform.schemas import (
    BRONZE_KEY,
    BRONZE_PARTITIONS,
    read_ddl,
    table_ddl,
)
from publisher_lakehouse.transform.session import build_spark, lake_uri


@dataclass(frozen=True)
class BronzeLoadResult:
    landed_rows: int
    inserted: int
    already_present: int
    table_rows: int
    raw_html_missing: int
    files_added: int
    table_version: int


def _raw_html_paths(spark, raw_html_uri: str) -> tuple[str, set[str]]:
    """List the publisher's HTML objects once through the session's Hadoop FS."""

    path = spark._jvm.org.apache.hadoop.fs.Path(raw_html_uri)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    base = path.toString().rstrip("/")
    if not filesystem.exists(path):
        return base, set()
    paths: set[str] = set()
    files = filesystem.listFiles(path, True)
    while files.hasNext():
        paths.add(files.next().getPath().toString())
    return base, paths


def load_bronze(
    spark,
    *,
    publisher: str,
    landing_uri: str,
    table_uri: str,
    raw_html_uri: str,
    loaded_at: datetime,
) -> BronzeLoadResult:
    """Validate one publisher's landing files, then insert only unseen run rows."""

    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    if not publisher or "/" in publisher or "\\" in publisher:
        raise ValueError(f"Invalid publisher: {publisher!r}")
    if loaded_at.tzinfo is None or loaded_at.utcoffset() is None:
        raise ValueError("loaded_at must be timezone-aware")

    landing_path = spark._jvm.org.apache.hadoop.fs.Path(landing_uri)
    landing_fs = landing_path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not landing_fs.exists(landing_path):
        raise FileNotFoundError(f"No landed JSONL files at {landing_uri}")

    source = (
        spark.read.schema(read_ddl())
        .option("mode", "FAILFAST")
        .option("recursiveFileLookup", "true")
        .json(landing_uri)
    )
    if not source.inputFiles():
        raise FileNotFoundError(f"No landed JSONL files at {landing_uri}")

    # Recursive lookup avoids Spark's implicit partition column; the date is
    # derived exactly once from each row's physical file path.
    batch = (
        source.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn(
            "_ingest_date_text",
            F.regexp_extract(
                F.col("_source_file"), r"/ingest_date=(\d{4}-\d{2}-\d{2})/", 1
            ),
        )
        .withColumn("ingest_date", F.expr("try_cast(_ingest_date_text AS DATE)"))
        .withColumn("scraped_at", F.expr("try_cast(scraped_at AS TIMESTAMP)"))
        .drop("_ingest_date_text")
    )
    landed_rows = batch.count()
    if not landed_rows:
        raise FileNotFoundError(f"No landed JSONL rows at {landing_uri}")

    bad_dates = batch.filter(F.col("ingest_date").isNull()).count()
    if bad_dates:
        raise ValueError(f"{bad_dates} row(s) have no valid ingest_date=YYYY-MM-DD in their file path")
    bad_timestamps = batch.filter(F.col("scraped_at").isNull()).count()
    if bad_timestamps:
        raise ValueError(f"{bad_timestamps} row(s) have invalid scraped_at timestamps")

    wrong_publisher = batch.filter(~F.col("publisher").eqNullSafe(F.lit(publisher))).count()
    if wrong_publisher:
        raise ValueError(
            f"{wrong_publisher} row(s) have publisher different from {publisher!r}"
        )
    empty_keys = batch.filter(
        F.col("url_hash").isNull()
        | (F.trim(F.col("url_hash")) == "")
        | F.col("run_id").isNull()
        | (F.trim(F.col("run_id")) == "")
    ).count()
    if empty_keys:
        raise ValueError(f"{empty_keys} row(s) have an empty url_hash or run_id")
    duplicate_keys = (
        batch.groupBy("url_hash", "run_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(5)
        .collect()
    )
    if duplicate_keys:
        samples = [
            (row["url_hash"], row["run_id"], row["count"]) for row in duplicate_keys
        ]
        raise ValueError(f"Duplicate (url_hash, run_id) keys in landing batch: {samples}")

    batch = batch.withColumn(
        "raw_html_key",
        F.concat(
            F.lit(f"landing/raw_html/{publisher}/"),
            F.date_format(F.col("ingest_date"), "yyyy-MM-dd"),
            F.lit("/"),
            F.col("url_hash"),
            F.lit(".html.gz"),
        ),
    ).withColumn("_loaded_at", F.lit(loaded_at))

    raw_base, present_html = _raw_html_paths(spark, raw_html_uri)
    raw_prefix = f"landing/raw_html/{publisher}/"
    raw_html_missing = sum(
        f"{raw_base}/{row['raw_html_key'][len(raw_prefix):]}" not in present_html
        for row in batch.select("raw_html_key").collect()
    )
    if raw_html_missing:
        print(f"WARNING: {raw_html_missing} bronze row(s) have no raw HTML object")

    partitions = ", ".join(BRONZE_PARTITIONS)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS delta.`{table_uri}` ({table_ddl()}) "
        f"USING DELTA PARTITIONED BY ({partitions}) "
        "TBLPROPERTIES ('delta.appendOnly' = 'true')"
    )
    table_before = spark.read.format("delta").load(table_uri)
    table_rows_before = table_before.count()
    version_before = int(DeltaTable.forPath(spark, table_uri).history(1).first()["version"])
    already_present = batch.join(
        table_before.select(*BRONZE_KEY), on=list(BRONZE_KEY), how="left_semi"
    ).count()

    view_name = f"_bronze_batch_{uuid4().hex}"
    batch.createOrReplaceTempView(view_name)
    prior_shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
    empty_commit_conf = "spark.databricks.delta.skipRecordingEmptyCommits"
    prior_skip_empty = spark.conf.get(empty_commit_conf, "true")
    try:
        # Four shuffle partitions are enough for the Phase 1 publisher volume.
        spark.conf.set("spark.sql.shuffle.partitions", "4")
        # Keep a zero-insert MERGE in Delta history so its metrics describe this
        # run instead of silently reusing the previous MERGE's metrics.
        spark.conf.set(empty_commit_conf, "false")
        match = " AND ".join(f"target.{key} = source.{key}" for key in BRONZE_KEY)
        spark.sql(
            f"MERGE INTO delta.`{table_uri}` AS target USING {view_name} AS source "
            f"ON {match} WHEN NOT MATCHED THEN INSERT *"
        )
    finally:
        spark.conf.set(empty_commit_conf, prior_skip_empty)
        spark.conf.set("spark.sql.shuffle.partitions", prior_shuffle_partitions)
        spark.catalog.dropTempView(view_name)

    latest = DeltaTable.forPath(spark, table_uri).history(1).first()
    if latest["operation"] != "MERGE":
        raise RuntimeError(f"Expected MERGE history entry, got {latest['operation']}")
    metrics = latest["operationMetrics"]
    for key in ("numTargetRowsInserted", "numTargetFilesAdded"):
        if key not in metrics:
            raise RuntimeError(f"MERGE metric {key} missing; available: {sorted(metrics)}")
    inserted = int(metrics["numTargetRowsInserted"])
    files_added = int(metrics["numTargetFilesAdded"])
    table_version = int(latest["version"])
    if table_version != version_before + 1:
        raise RuntimeError(
            f"MERGE version {table_version} did not follow prior version {version_before}"
        )
    table_rows = spark.read.format("delta").load(table_uri).count()
    old_rows = (
        spark.read.format("delta")
        .option("versionAsOf", version_before)
        .load(table_uri)
        .count()
    )
    if inserted + already_present != landed_rows:
        raise RuntimeError(
            f"Unbalanced load: inserted {inserted} + already present "
            f"{already_present} != landed {landed_rows}"
        )
    if table_rows != table_rows_before + inserted:
        raise RuntimeError(
            f"Table row count {table_rows} != prior {table_rows_before} + inserted {inserted}"
        )
    if old_rows != table_rows_before:
        raise RuntimeError(
            f"Time travel count {old_rows} != prior table count {table_rows_before}"
        )
    return BronzeLoadResult(
        landed_rows=landed_rows,
        inserted=inserted,
        already_present=already_present,
        table_rows=table_rows,
        raw_html_missing=raw_html_missing,
        files_added=files_added,
        table_version=table_version,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load landed JSONL into bronze/articles")
    parser.add_argument("--publisher", required=True)
    parser.add_argument("--hold", action="store_true", help="Keep Spark UI alive until Enter")
    parser.add_argument("--schema", action="store_true", help="Print the Delta table schema")
    args = parser.parse_args(argv)

    spark = None
    try:
        spark = build_spark(f"bronze-{args.publisher}")
        table_uri = lake_uri("bronze", "articles")
        result = load_bronze(
            spark,
            publisher=args.publisher,
            landing_uri=lake_uri("landing", "bronze_jsonl", args.publisher),
            table_uri=table_uri,
            raw_html_uri=lake_uri("landing", "raw_html", args.publisher),
            loaded_at=datetime.now(timezone.utc),
        )
        print(
            f"Bronze load {args.publisher}: landed {result.landed_rows} | "
            f"inserted {result.inserted} | already present {result.already_present} | "
            f"table rows {result.table_rows} | raw HTML missing {result.raw_html_missing} | "
            f"files added {result.files_added} | version {result.table_version}"
        )
        if args.schema:
            spark.read.format("delta").load(table_uri).printSchema()
        print("DESCRIBE HISTORY (last five versions):")
        spark.sql(f"DESCRIBE HISTORY delta.`{table_uri}`").select(
            "version", "timestamp", "operation", "operationMetrics"
        ).limit(5).show(truncate=False)
        if args.hold:
            input("Spark UI is available on port 4040. Press Enter to stop Spark. ")
        return 0
    except Exception as exc:
        print(f"Bronze load failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
