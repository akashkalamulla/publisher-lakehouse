"""Build current, typed articles and quality events from immutable bronze rows."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from publisher_lakehouse.transform.delta_ops import merge_with_plan
from publisher_lakehouse.transform.schemas import (
    BRONZE_KEY,
    CC_LICENSE_PATTERN,
    DQ_RESULT_COLUMNS,
    DQ_RULES,
    HARD_RULES,
    MONTH_LOOKUP,
    QUARANTINE_COLUMNS,
    QUARANTINE_KEY,
    SILVER_BUSINESS_COLUMNS,
    SILVER_COLUMNS,
    SILVER_KEY,
    SOFT_RULES,
    dq_ddl,
    quarantine_ddl,
    silver_ddl,
)
from publisher_lakehouse.transform.session import build_spark, lake_uri


@dataclass(frozen=True)
class SilverResult:
    bronze_rows: int
    candidates: int
    inserted: int
    updated: int
    unchanged: int
    quarantined_new: int
    flagged_rows: int
    silver_rows: int
    silver_version: int
    skipped: bool
    dq_run_id: str


def _text(column, *, collapse: bool = False):
    from pyspark.sql import functions as F

    value = F.trim(column)
    if collapse:
        value = F.regexp_replace(value, r"\s+", " ")
    return F.when(value != "", value)


def _blank(column):
    from pyspark.sql import functions as F

    return F.trim(F.coalesce(column, F.lit(""))) == ""


def _contacts(column):
    from pyspark.sql import functions as F

    cleaned = F.transform(
        F.coalesce(column, F.array()),
        lambda contact: F.struct(
            _text(contact.getField("name")).alias("name"),
            _text(contact.getField("affiliation")).alias("affiliation"),
            _text(contact.getField("email")).alias("email"),
        ),
    )
    return F.filter(cleaned, lambda contact: contact.getField("name").isNotNull())


def _publication_parts(frame, *, source: str, target: str):
    """Parse year, first month, and a calendar-valid day without ANSI failures."""

    from pyspark.sql import functions as F

    month_source = f"{source}_publication_month"
    year_source = f"{source}_publication_year"
    day_source = f"{source}_publication_day"
    month_token = f"_{target}_month_token"
    month_try = f"_{target}_month_try"
    day_try = f"_{target}_day_try"
    month_map = F.create_map(
        *[item for name, number in MONTH_LOOKUP.items() for item in (F.lit(name), F.lit(number))]
    )
    frame = frame.withColumn(
        month_token,
        F.lower(
            F.trim(F.split(F.coalesce(F.col(month_source), F.lit("")), r"[-–—]").getItem(0))
        ),
    )
    frame = frame.withColumn(month_try, F.expr(f"try_cast({month_token} AS INT)"))
    frame = frame.withColumn(f"{target}_year", F.expr(f"try_cast({year_source} AS INT)"))
    frame = frame.withColumn(
        f"{target}_month",
        F.when(F.col(month_try).between(1, 12), F.col(month_try)).otherwise(
            F.element_at(month_map, F.col(month_token))
        ),
    )
    frame = frame.withColumn(day_try, F.expr(f"try_cast({day_source} AS INT)"))
    frame = frame.withColumn(
        f"{target}_month_start",
        F.expr(
            f"try_cast(concat(cast({target}_year AS STRING), '-', "
            f"lpad(cast({target}_month AS STRING), 2, '0'), '-01') AS DATE)"
        ),
    )
    valid_day = F.expr(
        f"try_cast(concat(cast({target}_year AS STRING), '-', "
        f"lpad(cast({target}_month AS STRING), 2, '0'), '-', "
        f"lpad(cast({day_try} AS STRING), 2, '0')) AS DATE)"
    )
    frame = frame.withColumn(f"{target}_day", F.when(valid_day.isNotNull(), F.col(day_try)))
    frame = frame.withColumn(
        f"{source}_date_precision",
        F.when(F.col(f"{target}_day").isNotNull(), F.lit("day"))
        .when(F.col(f"{target}_month_start").isNotNull(), F.lit("month"))
        .when(F.col(f"{target}_year").isNotNull(), F.lit("year")),
    )
    frame = frame.withColumn(
        f"_{target}_month_unparsed",
        _text(F.col(month_source)).isNotNull() & F.col(f"{target}_month").isNull(),
    ).withColumn(
        f"_{target}_month_range",
        F.coalesce(F.col(month_source), F.lit("")).rlike(r"[-–—]"),
    )
    return frame


def _silver_candidates(latest_valid, *, run_ts: datetime):
    """Project every declared business column and derive deterministic flags/hash."""

    from pyspark.sql import functions as F

    frame = latest_valid
    basic_strings = (
        "publisher", "url_hash", "article_url", "source_url", "journal_url",
        "journal_title", "issue_url", "volume", "issue", "article_type",
        "start_page", "last_page", "payload_hash", "raw_html_key",
    )
    for name in basic_strings:
        frame = frame.withColumn(name, _text(F.col(name)))
    for name in (
        "english_title", "foreign_title", "english_abstract", "non_english_abstract"
    ):
        frame = frame.withColumn(name, _text(F.col(name), collapse=True))

    frame = frame.withColumn(
        "volume_num",
        F.when(F.col("volume").rlike(r"^[0-9]+$"), F.expr("try_cast(volume AS INT)")),
    ).withColumn(
        "issue_num",
        F.when(F.col("issue").rlike(r"^[0-9]+$"), F.expr("try_cast(issue AS INT)")),
    )
    doi = F.lower(_text(F.col("doi")))
    doi = _text(F.regexp_replace(doi, r"^(https?://(?:dx\.)?doi\.org/|doi:\s*)", ""))
    frame = frame.withColumn("doi", doi).withColumn(
        "doi_valid", F.coalesce(F.col("doi").rlike(r"^10\.\d{4,9}/\S+$"), F.lit(False))
    )
    article_type_key = F.regexp_replace(
        F.regexp_replace(F.lower(F.col("article_type")), r"[^a-z0-9]+", "_"),
        r"^_+|_+$", "",
    )
    frame = frame.withColumn("article_type_key", _text(article_type_key))
    frame = frame.withColumn(
        "keywords",
        F.array_distinct(
            F.filter(
                F.transform(
                    F.split(F.coalesce(F.col("author_keyword"), F.lit("")), r"\|"),
                    lambda word: F.trim(word),
                ),
                lambda word: word != "",
            )
        ),
    )

    frame = frame.withColumn("_reference_text", _text(F.col("reference_count")))
    frame = frame.withColumn("reference_count", F.expr("try_cast(_reference_text AS INT)"))
    frame = frame.withColumn("_copyright_text", _text(F.col("copyright_year")))
    frame = frame.withColumn(
        "_copyright_try", F.expr("try_cast(_copyright_text AS INT)")
    ).withColumn(
        "copyright_year",
        F.when(F.col("_copyright_try").between(1900, run_ts.year + 1), F.col("_copyright_try")),
    )
    frame = frame.withColumn(
        "_start_page_int", F.expr("try_cast(start_page AS INT)")
    ).withColumn("_last_page_int", F.expr("try_cast(last_page AS INT)"))
    frame = frame.withColumn(
        "page_count",
        F.when(
            F.col("_start_page_int").isNotNull()
            & F.col("_last_page_int").isNotNull()
            & (F.col("_last_page_int") >= F.col("_start_page_int")),
            F.expr(
                "try_cast(cast(_last_page_int AS BIGINT) - "
                "cast(_start_page_int AS BIGINT) + 1 AS INT)"
            ),
        ),
    )

    frame = _publication_parts(frame, source="article", target="article_pub")
    frame = _publication_parts(frame, source="issue", target="issue_pub")
    frame = frame.withColumn("license_url", _text(F.col("license_type")))
    cc_kind = F.regexp_extract(F.col("license_url"), CC_LICENSE_PATTERN, 1)
    cc_version = F.regexp_extract(F.col("license_url"), CC_LICENSE_PATTERN, 2)
    frame = frame.withColumn(
        "license_code",
        F.when(cc_kind != "", F.concat(F.lit("CC "), F.upper(cc_kind), F.lit(" "), cc_version)),
    ).withColumn("has_cc_license", F.col("license_code").isNotNull())
    frame = frame.withColumn("funding_statement", _text(F.col("funding_status")))
    for name in ("authors", "editors", "corporate_authors"):
        frame = frame.withColumn(name, _contacts(F.col(name)))
    frame = frame.withColumn("author_count", F.size(F.col("authors")))
    frame = frame.withColumn("last_changed_at", F.col("scraped_at"))
    frame = frame.withColumn("latest_run_id", _text(F.col("run_id")))

    soft_conditions = {
        "missing_doi": F.col("doi").isNull(),
        "invalid_doi": F.col("doi").isNotNull() & ~F.col("doi_valid"),
        "no_authors": F.col("author_count") == 0,
        "missing_abstract": F.col("english_abstract").isNull()
        & F.col("non_english_abstract").isNull(),
        "unparsed_pub_month": F.col("_article_pub_month_unparsed")
        | F.col("_issue_pub_month_unparsed"),
        "month_range": F.col("_article_pub_month_range") | F.col("_issue_pub_month_range"),
        "bad_reference_count": F.col("_reference_text").isNotNull()
        & F.col("reference_count").isNull(),
        "bad_copyright_year": F.col("_copyright_text").isNotNull()
        & F.col("copyright_year").isNull(),
        "bad_page_range": F.col("_start_page_int").isNotNull()
        & F.col("_last_page_int").isNotNull()
        & (F.col("_last_page_int") < F.col("_start_page_int")),
    }
    frame = frame.withColumn(
        "dq_flags",
        F.filter(
            F.array(*[F.when(soft_conditions[name], F.lit(name)) for name in SOFT_RULES]),
            lambda value: value.isNotNull(),
        ),
    )
    frame = frame.withColumn(
        "_row_hash",
        F.sha2(
            F.to_json(
                F.struct(*[F.col(name).alias(name) for name in SILVER_BUSINESS_COLUMNS]),
                {"ignoreNullFields": "false"},
            ),
            256,
        ),
    ).withColumn("_silver_updated_at", F.lit(run_ts))
    return frame.select(*[name for name, _ in SILVER_COLUMNS])


def _rule_counts(frame, array_column: str, rules: tuple[str, ...]) -> dict[str, int]:
    from pyspark.sql import functions as F

    row = frame.agg(
        *[
            F.sum(F.when(F.array_contains(F.col(array_column), name), 1).otherwise(0))
            .cast("long")
            .alias(name)
            for name in rules
        ]
    ).first()
    return {name: int(row[name] or 0) for name in rules}


def _business_hash_sql(alias: str) -> str:
    """Mirror the DataFrame hash expression for a MERGE target row."""

    fields = ", ".join(
        f"'{name}', {alias}.`{name}`" for name in SILVER_BUSINESS_COLUMNS
    )
    return (
        f"sha2(to_json(named_struct({fields}), "
        "map('ignoreNullFields', 'false')), 256)"
    )


def build_silver(
    spark,
    *,
    publisher: str,
    bronze_uri: str,
    silver_uri: str,
    quarantine_uri: str,
    dq_uri: str,
    run_ts: datetime,
) -> SilverResult:
    """Select latest valid bronze rows, MERGE changes, and audit every DQ rule."""

    from pyspark.sql import Window, functions as F

    if not publisher or "/" in publisher or "\\" in publisher:
        raise ValueError(f"Invalid publisher: {publisher!r}")
    if run_ts.tzinfo is None or run_ts.utcoffset() is None:
        raise ValueError("run_ts must be timezone-aware")

    bronze = spark.read.format("delta").load(bronze_uri).filter(F.col("publisher") == publisher)
    bronze_rows = bronze.count()
    hard_conditions = {
        "missing_article_url": _blank(F.col("article_url")),
        "missing_journal_url": _blank(F.col("journal_url")),
        "missing_title": _blank(F.col("english_title")) & _blank(F.col("foreign_title")),
    }
    checked = bronze.withColumn(
        "dq_reasons",
        F.filter(
            F.array(*[F.when(hard_conditions[name], F.lit(name)) for name in HARD_RULES]),
            lambda value: value.isNotNull(),
        ),
    )
    hard_counts = _rule_counts(checked, "dq_reasons", HARD_RULES)
    failed = checked.filter(F.size(F.col("dq_reasons")) > 0)
    passing = checked.filter(F.size(F.col("dq_reasons")) == 0)

    aggregate = bronze.groupBy(*SILVER_KEY).agg(
        F.min("scraped_at").alias("first_seen_at"),
        F.count("*").alias("_version_count_big"),
    ).withColumn("version_count", F.expr("try_cast(_version_count_big AS INT)"))
    rank = Window.partitionBy(*SILVER_KEY).orderBy(
        F.col("scraped_at").desc(), F.col("run_id").desc()
    )
    latest_valid = (
        passing.withColumn("_rank", F.row_number().over(rank))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        .join(aggregate.select(*SILVER_KEY, "first_seen_at", "version_count"), list(SILVER_KEY))
    )
    candidates = _silver_candidates(latest_valid, run_ts=run_ts)
    candidate_rows = candidates.count()
    flagged_rows = candidates.filter(F.size(F.col("dq_flags")) > 0).count()
    soft_counts = _rule_counts(candidates, "dq_flags", SOFT_RULES)

    quarantine_batch = failed.select(
        *[name for name, _ in QUARANTINE_COLUMNS if name != "_quarantined_at"]
    ).withColumn("_quarantined_at", F.lit(run_ts))
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS delta.`{silver_uri}` ({silver_ddl()}) "
        "USING DELTA PARTITIONED BY (publisher)"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS delta.`{quarantine_uri}` ({quarantine_ddl()}) "
        "USING DELTA PARTITIONED BY (publisher)"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS delta.`{dq_uri}` ({dq_ddl()}) "
        "USING DELTA TBLPROPERTIES ('delta.appendOnly' = 'true')"
    )

    silver_before = spark.read.format("delta").load(silver_uri)
    comparison = candidates.alias("source").join(
        silver_before.select(*SILVER_BUSINESS_COLUMNS, "_row_hash").alias("target"),
        on=[F.col(f"source.{key}") == F.col(f"target.{key}") for key in SILVER_KEY],
        how="left",
    )
    planned_inserts = comparison.filter(F.col("target.url_hash").isNull()).count()
    target_business_hash = F.sha2(
        F.to_json(
            F.struct(
                *[F.col(f"target.{name}").alias(name) for name in SILVER_BUSINESS_COLUMNS]
            ),
            {"ignoreNullFields": "false"},
        ),
        256,
    )
    planned_updates = comparison.filter(
        F.col("target.url_hash").isNotNull()
        & (
            ~F.col("target._row_hash").eqNullSafe(F.col("source._row_hash"))
            | ~target_business_hash.eqNullSafe(F.col("source._row_hash"))
        )
    ).count()
    unchanged = candidate_rows - planned_inserts - planned_updates
    quarantine_before = spark.read.format("delta").load(quarantine_uri)
    planned_quarantine = quarantine_batch.join(
        quarantine_before.select(*QUARANTINE_KEY), on=list(QUARANTINE_KEY), how="left_anti"
    ).count()

    silver_view = f"_silver_batch_{uuid4().hex}"
    quarantine_view = f"_quarantine_batch_{uuid4().hex}"
    candidates.createOrReplaceTempView(silver_view)
    quarantine_batch.createOrReplaceTempView(quarantine_view)
    prior_shuffle = spark.conf.get("spark.sql.shuffle.partitions")
    try:
        spark.conf.set("spark.sql.shuffle.partitions", "4")
        silver_match = " AND ".join(
            f"target.{name} = source.{name}" for name in SILVER_KEY
        )
        silver_outcome = merge_with_plan(
            spark,
            table_uri=silver_uri,
            source_view=silver_view,
            merge_sql=(
                f"MERGE INTO delta.`{silver_uri}` AS target USING {silver_view} AS source "
                f"ON {silver_match} "
                "WHEN MATCHED AND target._row_hash <> source._row_hash THEN UPDATE SET * "
                "WHEN MATCHED AND (target._row_hash IS NULL OR "
                f"{_business_hash_sql('target')} <> source._row_hash) THEN UPDATE SET * "
                "WHEN NOT MATCHED THEN INSERT *"
            ),
            planned_inserts=planned_inserts,
            planned_updates=planned_updates,
        )
        quarantine_match = " AND ".join(
            f"target.{name} = source.{name}" for name in QUARANTINE_KEY
        )
        quarantine_outcome = merge_with_plan(
            spark,
            table_uri=quarantine_uri,
            source_view=quarantine_view,
            merge_sql=(
                f"MERGE INTO delta.`{quarantine_uri}` AS target "
                f"USING {quarantine_view} AS source ON {quarantine_match} "
                "WHEN NOT MATCHED THEN INSERT *"
            ),
            planned_inserts=planned_quarantine,
            planned_updates=0,
        )
    finally:
        spark.conf.set("spark.sql.shuffle.partitions", prior_shuffle)
        spark.catalog.dropTempView(silver_view)
        spark.catalog.dropTempView(quarantine_view)

    dq_run_id = uuid4().hex
    dq_rows = [
        (
            dq_run_id,
            run_ts,
            publisher,
            "silver/articles",
            rule,
            severity,
            hard_counts[rule] if severity == "quarantine" else soft_counts[rule],
            bronze_rows if severity == "quarantine" else candidate_rows,
        )
        for rule, severity in DQ_RULES
    ]
    spark.createDataFrame(dq_rows, schema=dq_ddl()).coalesce(1).write.format(
        "delta"
    ).mode("append").save(dq_uri)

    silver_after = spark.read.format("delta").load(silver_uri).filter(
        F.col("publisher") == publisher
    )
    silver_rows = silver_after.count()
    if silver_outcome.inserted + silver_outcome.updated + unchanged != candidate_rows:
        raise RuntimeError("Silver MERGE counts do not balance with candidate rows")
    valid_keys = passing.select("url_hash").distinct().count()
    if silver_rows != valid_keys:
        raise RuntimeError(f"Silver rows {silver_rows} != distinct valid articles {valid_keys}")
    actual_hash = F.sha2(
        F.to_json(
            F.struct(*[F.col(name).alias(name) for name in SILVER_BUSINESS_COLUMNS]),
            {"ignoreNullFields": "false"},
        ),
        256,
    )
    stale_hashes = silver_after.filter(~F.col("_row_hash").eqNullSafe(actual_hash)).count()
    if stale_hashes:
        raise RuntimeError(f"{stale_hashes} silver row(s) have a stale _row_hash")
    absent_from_silver = bronze.join(
        silver_after.select("url_hash"), on="url_hash", how="left_anti"
    )
    quarantine_after = spark.read.format("delta").load(quarantine_uri).filter(
        F.col("publisher") == publisher
    )
    uncovered = absent_from_silver.join(
        quarantine_after.select(*BRONZE_KEY), on=list(BRONZE_KEY), how="left_anti"
    ).count()
    if uncovered:
        raise RuntimeError(f"{uncovered} bronze row(s) are neither silver nor quarantined")

    return SilverResult(
        bronze_rows=bronze_rows,
        candidates=candidate_rows,
        inserted=silver_outcome.inserted,
        updated=silver_outcome.updated,
        unchanged=unchanged,
        quarantined_new=quarantine_outcome.inserted,
        flagged_rows=flagged_rows,
        silver_rows=silver_rows,
        silver_version=silver_outcome.version,
        skipped=silver_outcome.skipped,
        dq_run_id=dq_run_id,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build silver articles from bronze")
    parser.add_argument("--publisher", required=True)
    parser.add_argument("--hold", action="store_true", help="Keep Spark UI alive until Enter")
    parser.add_argument("--sample", type=int, default=0, metavar="N")
    args = parser.parse_args(argv)
    if args.sample < 0:
        parser.error("--sample must be non-negative")

    spark = None
    try:
        from pyspark.sql import functions as F

        spark = build_spark(f"silver-{args.publisher}")
        silver_uri = lake_uri("silver", "articles")
        dq_uri = lake_uri("silver", "dq_results")
        result = build_silver(
            spark,
            publisher=args.publisher,
            bronze_uri=lake_uri("bronze", "articles"),
            silver_uri=silver_uri,
            quarantine_uri=lake_uri("silver", "articles_quarantine"),
            dq_uri=dq_uri,
            run_ts=datetime.now(timezone.utc),
        )
        print(
            f"Silver {args.publisher}: bronze rows {result.bronze_rows} | "
            f"candidates {result.candidates} | inserted {result.inserted} | "
            f"updated {result.updated} | unchanged {result.unchanged} | "
            f"quarantined {result.quarantined_new} | flagged {result.flagged_rows} | "
            f"silver rows {result.silver_rows} | version {result.silver_version}"
        )
        if args.sample:
            print(f"Sample ({args.sample} rows):")
            spark.read.format("delta").load(silver_uri).filter(
                F.col("publisher") == args.publisher
            ).select(
                "url_hash", "article_type_key", "article_pub_year", "article_pub_month",
                "article_pub_day", "article_date_precision", "reference_count", "doi",
                "license_code", "dq_flags",
            ).orderBy("url_hash").limit(args.sample).show(truncate=40)
        print("DQ results for this run:")
        spark.read.format("delta").load(dq_uri).filter(
            F.col("dq_run_id") == result.dq_run_id
        ).select("rule", "severity", "failed_rows", "total_rows").orderBy(
            "severity", "rule"
        ).show(truncate=False)
        print("DESCRIBE HISTORY silver/articles (last five versions):")
        spark.sql(f"DESCRIBE HISTORY delta.`{silver_uri}`").select(
            "version", "timestamp", "operation", "operationMetrics"
        ).limit(5).show(truncate=False)
        if args.hold:
            input("Spark UI is available on port 4040. Press Enter to stop Spark. ")
        return 0
    except Exception as exc:
        print(f"Silver build failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
