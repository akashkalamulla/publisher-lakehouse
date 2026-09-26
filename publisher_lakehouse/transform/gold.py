"""Build and validate the current article star schema from silver Delta data."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from uuid import uuid4

from publisher_lakehouse.transform.delta_ops import MergeOutcome, merge_with_plan
from publisher_lakehouse.transform.reference import ARTICLE_TYPE_CLASS, SCHOLARLY_CLASSES
from publisher_lakehouse.transform.schemas import GOLD_BUILD_ORDER, GOLD_SPECS, gold_ddl
from publisher_lakehouse.transform.session import build_spark, lake_uri


DATE_START = date(2000, 1, 1)
DATE_END = date(2035, 12, 31)


@dataclass(frozen=True)
class GoldResult:
    outcomes: dict[str, MergeOutcome]
    row_counts: dict[str, int]
    current_journals: int
    unclassified_types: tuple[str, ...]
    integrity: tuple[str, ...]


def _uri(root: str, name: str) -> str:
    return f"{root.rstrip('/')}/{name}"


def _columns(frame, name: str):
    return frame.select(*[column for column, _ in GOLD_SPECS[name].columns])


def _unknown(spark, name: str, **values):
    columns = GOLD_SPECS[name].columns
    return spark.createDataFrame(
        [tuple(values.get(column) for column, _ in columns)], schema=gold_ddl(name)
    )


def _row_hash(frame, name: str):
    from pyspark.sql import functions as F

    business = [column for column, _ in GOLD_SPECS[name].columns if column != "_row_hash"]
    return frame.withColumn(
        "_row_hash",
        F.sha2(
            F.to_json(
                F.struct(*[F.col(column).alias(column) for column in business]),
                {"ignoreNullFields": "false"},
            ),
            256,
        ),
    )


def _create_tables(spark, root: str) -> None:
    for name, spec in GOLD_SPECS.items():
        partitions = (
            f" PARTITIONED BY ({', '.join(spec.partitions)})" if spec.partitions else ""
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS delta.`{_uri(root, name)}` "
            f"({gold_ddl(name)}) USING DELTA{partitions}"
        )


def _duplicates(frame, keys: tuple[str, ...]) -> bool:
    from pyspark.sql import functions as F

    return frame.groupBy(*keys).agg(F.count("*").alias("n")).filter("n > 1").limit(1).count() > 0


def _merge_type1(spark, root: str, name: str, source) -> MergeOutcome:
    """MERGE one row per surrogate key, including its unknown member."""

    from pyspark.sql import functions as F

    spec = GOLD_SPECS[name]
    key = spec.surrogate_key
    if key is None:
        raise ValueError(f"{name} has no surrogate key")
    source = _columns(source, name)
    if _duplicates(source, (key,)):
        raise RuntimeError(f"Surrogate-key collision in source {name}.{key}")
    uri = _uri(root, name)
    target = spark.read.format("delta").load(uri)
    compared = source.alias("source").join(
        target.alias("target"), F.col(f"source.{key}") == F.col(f"target.{key}"), "left"
    )
    inserted = compared.filter(F.col(f"target.{key}").isNull()).count()
    attributes = [column for column, _ in spec.columns if column != key]
    changed = None
    for column in attributes:
        difference = ~F.col(f"source.{column}").eqNullSafe(F.col(f"target.{column}"))
        changed = difference if changed is None else changed | difference
    updated = compared.filter(F.col(f"target.{key}").isNotNull() & changed).count()
    if any(
        compared.filter(
            F.col(f"target.{key}").isNotNull()
            & ~F.col(f"source.{natural}").eqNullSafe(F.col(f"target.{natural}"))
        ).limit(1).count()
        for natural in spec.natural_key if natural in source.columns
    ):
        raise RuntimeError(f"Surrogate-key collision against target {name}.{key}")
    view = f"_gold_{name}_{uuid4().hex}"
    source.createOrReplaceTempView(view)
    try:
        equal = " AND ".join(
            f"target.`{column}` <=> source.`{column}`" for column in attributes
        )
        return merge_with_plan(
            spark,
            table_uri=uri,
            source_view=view,
            merge_sql=(
                f"MERGE INTO delta.`{uri}` AS target USING {view} AS source "
                f"ON target.`{key}` = source.`{key}` "
                f"WHEN MATCHED AND NOT ({equal}) THEN UPDATE SET * "
                "WHEN NOT MATCHED THEN INSERT *"
            ),
            planned_inserts=inserted,
            planned_updates=updated,
        )
    finally:
        spark.catalog.dropTempView(view)


def _date_source(spark):
    from pyspark.sql import functions as F

    days = (DATE_END - DATE_START).days + 1
    day = F.date_add(F.lit(DATE_START).cast("date"), F.col("id").cast("int"))
    dates = spark.range(days).select(day.alias("date"))
    dates = dates.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_sk"),
        "date", F.year("date").alias("year"), F.quarter("date").alias("quarter"),
        F.month("date").alias("month"), F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day"), F.dayofweek("date").alias("day_of_week"),
        F.weekofyear("date").alias("iso_week"),
        (F.dayofmonth("date") == 1).alias("is_month_start"),
    )
    return dates.unionByName(
        _unknown(spark, "dim_date", date_sk=-1, month_name="Unknown", is_month_start=False)
    )


def _article_type_source(spark, silver):
    from pyspark.sql import Window, functions as F

    ranked = silver.filter(F.col("article_type_key").isNotNull()).withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("article_type_key").orderBy(
                F.col("last_changed_at").desc(), F.col("url_hash").desc()
            )
        ),
    ).filter("_rn = 1")
    mapping = F.create_map(
        *[
            item
            for key, content_class in ARTICLE_TYPE_CLASS.items()
            for item in (F.lit(key), F.lit(content_class))
        ]
    )
    types = ranked.select(
        F.xxhash64("article_type_key").alias("article_type_sk"),
        "article_type_key", "article_type",
        F.coalesce(F.element_at(mapping, F.col("article_type_key")), F.lit("unclassified"))
        .alias("content_class"),
    ).withColumn("is_scholarly", F.col("content_class").isin(*SCHOLARLY_CLASSES))
    return types.unionByName(
        _unknown(
            spark, "dim_article_type", article_type_sk=-1,
            article_type_key="Unknown", article_type="Unknown",
            content_class="unclassified", is_scholarly=False,
        )
    )


def _issue_source(spark, silver):
    from pyspark.sql import Window, functions as F

    ranked = silver.filter(F.col("issue_url").isNotNull()).withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("publisher", "issue_url").orderBy(
                F.col("last_changed_at").desc(), F.col("url_hash").desc()
            )
        ),
    ).filter("_rn = 1")
    issue = ranked.select(
        F.xxhash64("publisher", "issue_url").alias("issue_sk"),
        *[F.col(column) for column, _ in GOLD_SPECS["dim_issue"].columns if column != "issue_sk"],
    )
    return issue.unionByName(
        _unknown(
            spark, "dim_issue", issue_sk=-1, publisher="Unknown",
            issue_url="Unknown", journal_url="Unknown", volume="Unknown", issue="Unknown",
            issue_date_precision="Unknown",
        )
    )


def _contacts(silver):
    """Explode all roles, preserving each role's 1-based position and weight base."""

    from pyspark.sql import functions as F

    frames = []
    for role, array_column in (
        ("author", "authors"), ("editor", "editors"),
        ("corporate", "corporate_authors"),
    ):
        frames.append(
            silver.select(
                "publisher", "url_hash", "last_changed_at", "first_seen_at",
                F.lit(role).alias("role"),
                F.size(F.col(array_column)).alias("role_count"),
                F.posexplode(F.col(array_column)).alias("position_zero", "contact"),
            )
        )
    union = frames[0].unionByName(frames[1]).unionByName(frames[2])
    normalized = F.trim(
        F.regexp_replace(
            F.regexp_replace(F.lower(F.col("contact.name")), r"[.,]", ""),
            r"\s+", " ",
        )
    )
    return union.withColumn("author_nk", normalized).filter(F.col("author_nk") != "")


def _author_source(spark, contacts):
    from pyspark.sql import Window, functions as F

    first = contacts.groupBy("author_nk").agg(F.min("first_seen_at").alias("first_seen_at"))
    latest = contacts.withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("author_nk").orderBy(
                F.col("last_changed_at").desc(), F.col("url_hash").desc(),
                F.col("role").desc(), F.col("position_zero").desc(),
            )
        ),
    ).filter("_rn = 1").drop("first_seen_at")
    author = latest.join(first, "author_nk").select(
        F.xxhash64("author_nk").alias("author_sk"), "author_nk",
        F.col("contact.name").alias("display_name"),
        F.col("contact.affiliation").alias("latest_affiliation"),
        F.col("contact.email").alias("latest_email"),
        F.col("first_seen_at"),
    )
    return author.unionByName(
        _unknown(
            spark, "dim_author", author_sk=-1, author_nk="Unknown",
            display_name="Unknown", latest_affiliation="Unknown", latest_email="Unknown",
        )
    )


def _merge_journal(spark, root: str, silver) -> MergeOutcome:
    """Stage changed journals twice so one MERGE expires and inserts SCD2 rows."""

    from pyspark.sql import Window, functions as F

    uri = _uri(root, "dim_journal")
    target = spark.read.format("delta").load(uri)
    current = target.filter(F.col("is_current") & (F.col("journal_sk") != -1))
    articles = silver.filter(F.col("journal_url").isNotNull())
    first = articles.groupBy("publisher", "journal_url").agg(
        F.min("first_seen_at").alias("journal_first_seen")
    )
    latest = articles.withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("publisher", "journal_url").orderBy(
                F.col("last_changed_at").desc(), F.col("url_hash").desc()
            )
        ),
    ).filter("_rn = 1").select(
        "publisher", "journal_url", "journal_title", "last_changed_at"
    )
    candidates = latest.join(first, ["publisher", "journal_url"]).withColumn(
        "_attr_hash",
        F.sha2(
            F.to_json(F.struct(F.col("journal_title")), {"ignoreNullFields": "false"}),
            256,
        ),
    )
    compared = candidates.alias("source").join(
        current.alias("target"),
        (F.col("source.publisher") == F.col("target.publisher"))
        & (F.col("source.journal_url") == F.col("target.journal_url")), "left",
    )
    new = compared.filter(F.col("target.journal_sk").isNull()).select("source.*")
    changed_join = compared.filter(
        F.col("target.journal_sk").isNotNull()
        & ~F.col("source._attr_hash").eqNullSafe(F.col("target._attr_hash"))
    )
    if changed_join.filter(
        F.col("source.last_changed_at") <= F.col("target.effective_from")
    ).limit(1).count():
        raise RuntimeError("Changed journal title has no later effective_from")
    changed = changed_join.select("source.*")
    new_count = new.count()
    changed_count = changed.count()

    def version_rows(frame, start: str):
        return frame.select(
            F.xxhash64("publisher", "journal_url", F.col(start)).alias("journal_sk"),
            "publisher", "journal_url", "journal_title",
            F.col(start).alias("effective_from"),
            F.to_timestamp(F.lit("9999-12-31 00:00:00")).alias("effective_to"),
            F.lit(True).alias("is_current"), "_attr_hash",
        )

    new_rows = version_rows(new, "journal_first_seen")
    changed_rows = version_rows(changed, "last_changed_at")
    unknown_missing = target.filter("journal_sk = -1").limit(1).count() == 0
    unknown = _unknown(
        spark, "dim_journal", journal_sk=-1, publisher="Unknown",
        journal_url="Unknown", journal_title="Unknown",
        effective_from=datetime(2000, 1, 1, tzinfo=timezone.utc),
        effective_to=datetime(9999, 12, 31, tzinfo=timezone.utc),
        is_current=True,
        _attr_hash="Unknown",
    )
    if not unknown_missing:
        unknown = unknown.filter(F.lit(False))

    def stage(frame, *, match: bool):
        return frame.select(
            (F.col("publisher") if match else F.lit(None).cast("string"))
            .alias("merge_publisher"),
            (F.col("journal_url") if match else F.lit(None).cast("string"))
            .alias("merge_journal_url"),
            *[F.col(column) for column, _ in GOLD_SPECS["dim_journal"].columns],
        )

    staged = stage(new_rows, match=True).unionByName(
        stage(changed_rows, match=True)
    ).unionByName(stage(changed_rows, match=False)).unionByName(
        stage(unknown, match=False)
    )
    view = f"_gold_journal_{uuid4().hex}"
    staged.createOrReplaceTempView(view)
    try:
        columns = [column for column, _ in GOLD_SPECS["dim_journal"].columns]
        return merge_with_plan(
            spark,
            table_uri=uri,
            source_view=view,
            merge_sql=(
                f"MERGE INTO delta.`{uri}` AS target USING {view} AS source "
                "ON target.publisher = source.merge_publisher "
                "AND target.journal_url = source.merge_journal_url AND target.is_current "
                "WHEN MATCHED AND target._attr_hash <> source._attr_hash THEN UPDATE SET "
                "target.effective_to = source.effective_from, target.is_current = false "
                f"WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) VALUES "
                f"({', '.join(f'source.`{column}`' for column in columns)})"
            ),
            planned_inserts=new_count + changed_count + int(unknown_missing),
            planned_updates=changed_count,
        )
    finally:
        spark.catalog.dropTempView(view)


def _date_key(column):
    from pyspark.sql import functions as F

    return F.when(
        column.between(F.lit(DATE_START), F.lit(DATE_END)),
        F.date_format(column, "yyyyMMdd").cast("int"),
    ).otherwise(F.lit(-1))


def _fact_source(spark, root: str, silver):
    from pyspark.sql import functions as F

    journal = spark.read.format("delta").load(_uri(root, "dim_journal")).filter(
        F.col("journal_sk") != -1
    )
    issue = spark.read.format("delta").load(_uri(root, "dim_issue"))
    article_type = spark.read.format("delta").load(_uri(root, "dim_article_type"))
    joined = silver.alias("a").join(
        journal.alias("j"),
        (F.col("a.publisher") == F.col("j.publisher"))
        & (F.col("a.journal_url") == F.col("j.journal_url"))
        & (F.col("a.last_changed_at") >= F.col("j.effective_from"))
        & (F.col("a.last_changed_at") < F.col("j.effective_to")),
        "left",
    )
    if joined.filter(F.col("j.journal_sk").isNull()).limit(1).count():
        raise RuntimeError("A silver article has no as-of journal dimension row")
    if _duplicates(joined.select(F.col("a.publisher"), F.col("a.url_hash")),
                   ("publisher", "url_hash")):
        raise RuntimeError("Overlapping journal ranges multiply a fact article")
    joined = joined.join(
        issue.alias("i"),
        (F.col("a.publisher") == F.col("i.publisher"))
        & (F.col("a.issue_url") == F.col("i.issue_url")), "left",
    ).join(
        article_type.alias("t"),
        F.col("a.article_type_key") == F.col("t.article_type_key"), "left",
    )
    published = F.when(
        F.col("a.article_date_precision").isin("day", "month"),
        F.col("a.article_pub_month_start"),
    )
    fact = joined.select(
        F.xxhash64(F.col("a.publisher"), F.col("a.url_hash")).alias("article_sk"),
        F.col("j.journal_sk").alias("journal_sk"),
        F.coalesce(F.col("i.issue_sk"), F.lit(-1)).alias("issue_sk"),
        F.coalesce(F.col("t.article_type_sk"), F.lit(-1)).alias("article_type_sk"),
        _date_key(published).alias("pub_month_date_sk"),
        _date_key(F.to_date(F.col("a.first_seen_at"))).alias("first_seen_date_sk"),
        F.lit(1).alias("article_count"),
        F.col("a.reference_count"), F.col("a.page_count"), F.col("a.author_count"),
        F.size(F.col("a.keywords")).alias("keyword_count"),
        F.col("a.version_count"), F.size(F.col("a.dq_flags")).alias("dq_flag_count"),
        F.col("a.publisher"), F.col("a.url_hash"), F.col("a.doi"),
        F.col("a.english_title"), F.col("a.article_url"),
        F.col("a.article_pub_year").alias("pub_year"),
        F.col("a.article_date_precision").alias("pub_date_precision"),
        F.col("a.license_code"), F.col("a.has_cc_license"),
    )
    return _columns(_row_hash(fact, "fact_article"), "fact_article")


def _bridge_source(contacts):
    from pyspark.sql import functions as F

    bridge = contacts.select(
        F.xxhash64("publisher", "url_hash").alias("article_sk"),
        F.xxhash64("author_nk").alias("author_sk"), "role",
        (F.col("position_zero") + 1).cast("int").alias("author_position"),
        F.col("contact.affiliation").alias("affiliation_at_publication"),
        F.col("contact.email").alias("email_at_publication"),
        (F.lit(1.0) / F.col("role_count")).alias("weight"),
        "publisher",
    )
    return _columns(_row_hash(bridge, "bridge_article_author"), "bridge_article_author")


def _merge_bridge(spark, root: str, publisher: str, source) -> MergeOutcome:
    from pyspark.sql import functions as F

    name = "bridge_article_author"
    uri = _uri(root, name)
    source = _columns(source, name)
    key = GOLD_SPECS[name].natural_key
    if _duplicates(source, key):
        raise RuntimeError("Duplicate bridge natural key in source")
    target = spark.read.format("delta").load(uri).filter(F.col("publisher") == publisher)
    on = [F.col(f"source.{column}") == F.col(f"target.{column}") for column in key]
    compared = source.alias("source").join(target.alias("target"), on, "left")
    inserted = compared.filter(F.col("target.article_sk").isNull()).count()
    attributes = [column for column, _ in GOLD_SPECS[name].columns if column not in key]
    different = None
    for column in attributes:
        part = ~F.col(f"source.{column}").eqNullSafe(F.col(f"target.{column}"))
        different = part if different is None else different | part
    updated = compared.filter(F.col("target.article_sk").isNotNull() & different).count()
    deleted = target.alias("target").join(
        source.alias("source"),
        [F.col(f"target.{column}") == F.col(f"source.{column}") for column in key],
        "left_anti",
    ).count()
    view = f"_gold_bridge_{uuid4().hex}"
    source.createOrReplaceTempView(view)
    try:
        match = " AND ".join(
            f"target.`{column}` = source.`{column}`" for column in key
        )
        equal = " AND ".join(
            f"target.`{column}` <=> source.`{column}`" for column in attributes
        )
        safe_publisher = publisher.replace("'", "''")
        return merge_with_plan(
            spark,
            table_uri=uri, source_view=view,
            merge_sql=(
                f"MERGE INTO delta.`{uri}` AS target USING {view} AS source "
                f"ON {match} "
                "WHEN MATCHED AND target._row_hash <> source._row_hash THEN UPDATE SET * "
                f"WHEN MATCHED AND NOT ({equal}) THEN UPDATE SET * "
                "WHEN NOT MATCHED THEN INSERT * "
                "WHEN NOT MATCHED BY SOURCE AND "
                f"target.publisher = '{safe_publisher}' THEN DELETE"
            ),
            planned_inserts=inserted, planned_updates=updated, planned_deletes=deleted,
        )
    finally:
        spark.catalog.dropTempView(view)


def validate_gold(spark, *, publisher: str, silver_uri: str, gold_root_uri: str) -> tuple[str, ...]:
    """Raise on broken foreign keys, keys, journal ranges, grain, or weights."""

    from pyspark.sql import Window, functions as F

    tables = {
        name: spark.read.format("delta").load(_uri(gold_root_uri, name))
        for name in GOLD_BUILD_ORDER
    }
    silver = spark.read.format("delta").load(silver_uri).filter(
        F.col("publisher") == publisher
    )
    for name, spec in GOLD_SPECS.items():
        if spec.surrogate_key is not None:
            if _duplicates(tables[name], (spec.surrogate_key,)):
                raise RuntimeError(f"Duplicate surrogate key in {name}")
        if name.startswith("dim_"):
            if tables[name].filter(F.col(spec.surrogate_key) == -1).count() != 1:
                raise RuntimeError(f"Unknown member missing or duplicated in {name}")
    fact = tables["fact_article"].filter(F.col("publisher") == publisher)
    bridge = tables["bridge_article_author"].filter(F.col("publisher") == publisher)
    if fact.count() != silver.count():
        raise RuntimeError("Fact count does not equal current silver article count")
    for foreign_key, dimension_name in GOLD_SPECS["fact_article"].foreign_keys:
        dimension_key = GOLD_SPECS[dimension_name].surrogate_key
        missing = fact.join(
            tables[dimension_name].select(F.col(dimension_key).alias(foreign_key)),
            foreign_key, "left_anti",
        ).limit(1).count()
        if missing:
            raise RuntimeError(f"Fact {foreign_key} has no {dimension_name} row")
    for foreign_key, dimension_name in GOLD_SPECS["bridge_article_author"].foreign_keys:
        dimension_key = GOLD_SPECS[dimension_name].surrogate_key
        missing = bridge.join(
            tables[dimension_name].select(F.col(dimension_key).alias(foreign_key)),
            foreign_key, "left_anti",
        ).limit(1).count()
        if missing:
            raise RuntimeError(f"Bridge {foreign_key} has no {dimension_name} row")
    if _duplicates(bridge, GOLD_SPECS["bridge_article_author"].natural_key):
        raise RuntimeError("Duplicate bridge natural key")

    journal = tables["dim_journal"]
    currents = journal.groupBy("publisher", "journal_url").agg(
        F.sum(F.when(F.col("is_current"), 1).otherwise(0)).alias("current_rows")
    )
    if currents.filter("current_rows != 1").limit(1).count():
        raise RuntimeError("Journal must have exactly one current row per natural key")
    if journal.filter(F.col("effective_from") >= F.col("effective_to")).limit(1).count():
        raise RuntimeError("Journal has an empty or reversed validity range")
    ordered = Window.partitionBy("publisher", "journal_url").orderBy("effective_from")
    if journal.withColumn("prior_end", F.lag("effective_to").over(ordered)).filter(
        F.col("prior_end") > F.col("effective_from")
    ).limit(1).count():
        raise RuntimeError("Journal validity ranges overlap")
    weight_errors = bridge.groupBy("article_sk", "role").agg(
        F.sum("weight").alias("total_weight")
    ).filter(F.abs(F.col("total_weight") - 1.0) > 1e-9).limit(1).count()
    if weight_errors:
        raise RuntimeError("Bridge weights do not sum to 1 per article and role")
    expected_bridge = silver.select(
        (F.size("authors") + F.size("editors") + F.size("corporate_authors"))
        .alias("entries")
    ).agg(F.sum("entries").alias("total")).first()["total"] or 0
    if bridge.count() != expected_bridge:
        raise RuntimeError("Bridge row count disagrees with cleaned silver arrays")
    return (
        "fact FKs ok", "bridge FKs ok", "keys unique",
        "one current journal per key", "journal ranges ok", "weights ok",
        "fact and bridge counts ok",
    )


def build_gold(
    spark, *, publisher: str, silver_uri: str, gold_root_uri: str, run_ts: datetime
) -> GoldResult:
    """Load all seven declared gold tables, then validate the star."""

    from pyspark.sql import functions as F

    if not re.fullmatch(r"[A-Za-z0-9_-]+", publisher):
        raise ValueError(f"Invalid publisher: {publisher!r}")
    if run_ts.tzinfo is None or run_ts.utcoffset() is None:
        raise ValueError("run_ts must be timezone-aware")
    silver = spark.read.format("delta").load(silver_uri).filter(
        F.col("publisher") == publisher
    ).cache()
    silver.count()
    _create_tables(spark, gold_root_uri)
    outcomes: dict[str, MergeOutcome] = {}
    prior_shuffle = spark.conf.get("spark.sql.shuffle.partitions")
    try:
        spark.conf.set("spark.sql.shuffle.partitions", "4")
        outcomes["dim_date"] = _merge_type1(
            spark, gold_root_uri, "dim_date", _date_source(spark)
        )
        type_source = _article_type_source(spark, silver).cache()
        try:
            unclassified = tuple(
                row["article_type_key"] for row in type_source.filter(
                    "content_class = 'unclassified' AND article_type_sk != -1"
                ).select("article_type_key").orderBy("article_type_key").collect()
            )
            outcomes["dim_article_type"] = _merge_type1(
                spark, gold_root_uri, "dim_article_type", type_source
            )
        finally:
            type_source.unpersist()
        outcomes["dim_journal"] = _merge_journal(spark, gold_root_uri, silver)
        outcomes["dim_issue"] = _merge_type1(
            spark, gold_root_uri, "dim_issue", _issue_source(spark, silver)
        )
        contacts = _contacts(silver).cache()
        try:
            outcomes["dim_author"] = _merge_type1(
                spark, gold_root_uri, "dim_author", _author_source(spark, contacts)
            )
            outcomes["fact_article"] = _merge_type1(
                spark, gold_root_uri, "fact_article",
                _fact_source(spark, gold_root_uri, silver),
            )
            outcomes["bridge_article_author"] = _merge_bridge(
                spark, gold_root_uri, publisher, _bridge_source(contacts)
            )
        finally:
            contacts.unpersist()
        integrity = validate_gold(
            spark, publisher=publisher, silver_uri=silver_uri, gold_root_uri=gold_root_uri
        )
        row_counts = {
            name: spark.read.format("delta").load(_uri(gold_root_uri, name)).count()
            for name in GOLD_BUILD_ORDER
        }
        current_journals = spark.read.format("delta").load(
            _uri(gold_root_uri, "dim_journal")
        ).filter("is_current AND journal_sk != -1").count()
        return GoldResult(outcomes, row_counts, current_journals, unclassified, integrity)
    finally:
        spark.conf.set("spark.sql.shuffle.partitions", prior_shuffle)
        silver.unpersist()


def _show_queries(spark, root: str, publisher: str) -> None:
    """Print SQL examples using the same joins available to Grafana."""

    for name in GOLD_BUILD_ORDER:
        spark.read.format("delta").load(_uri(root, name)).createOrReplaceTempView(
            f"gold_{name}"
        )
    safe_publisher = publisher.replace("'", "''")
    queries = (
        (
            "1. Articles per journal (current title) x content class",
            f"""
            SELECT jc.journal_title, t.content_class, COUNT(*) AS articles
            FROM gold_fact_article f
            JOIN gold_dim_journal ja ON f.journal_sk = ja.journal_sk
            JOIN gold_dim_journal jc ON jc.publisher = ja.publisher
              AND jc.journal_url = ja.journal_url AND jc.is_current
            JOIN gold_dim_article_type t ON f.article_type_sk = t.article_type_sk
            WHERE f.publisher = '{safe_publisher}'
            GROUP BY jc.journal_title, t.content_class
            ORDER BY articles DESC, jc.journal_title, t.content_class
            """,
        ),
        (
            "2. Scholarly articles per publication month",
            f"""
            SELECT d.date AS publication_month, COUNT(*) AS scholarly_articles
            FROM gold_fact_article f
            JOIN gold_dim_date d ON f.pub_month_date_sk = d.date_sk
            JOIN gold_dim_article_type t ON f.article_type_sk = t.article_type_sk
            WHERE f.publisher = '{safe_publisher}' AND t.is_scholarly AND d.date_sk != -1
            GROUP BY d.date ORDER BY d.date
            """,
        ),
        (
            "3. Top 10 authors by weighted scholarly article count",
            f"""
            SELECT a.display_name, SUM(b.weight) AS weighted_article_count
            FROM gold_bridge_article_author b
            JOIN gold_dim_author a ON b.author_sk = a.author_sk
            JOIN gold_fact_article f ON b.article_sk = f.article_sk
            JOIN gold_dim_article_type t ON f.article_type_sk = t.article_type_sk
            WHERE b.publisher = '{safe_publisher}' AND b.role = 'author'
              AND t.is_scholarly
            GROUP BY a.author_sk, a.display_name
            ORDER BY weighted_article_count DESC, a.display_name LIMIT 10
            """,
        ),
        (
            "4. Share of scholarly articles with a CC license per journal",
            f"""
            SELECT jc.journal_title,
              COUNT(*) AS scholarly_articles,
              SUM(CASE WHEN f.has_cc_license THEN 1 ELSE 0 END) AS cc_articles,
              SUM(CASE WHEN f.has_cc_license THEN 1.0 ELSE 0.0 END) / COUNT(*)
                AS cc_share
            FROM gold_fact_article f
            JOIN gold_dim_journal ja ON f.journal_sk = ja.journal_sk
            JOIN gold_dim_journal jc ON jc.publisher = ja.publisher
              AND jc.journal_url = ja.journal_url AND jc.is_current
            JOIN gold_dim_article_type t ON f.article_type_sk = t.article_type_sk
            WHERE f.publisher = '{safe_publisher}' AND t.is_scholarly
            GROUP BY jc.journal_title ORDER BY cc_share DESC, jc.journal_title
            """,
        ),
    )
    for title, query in queries:
        print(title)
        spark.sql(query).show(100, truncate=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the article gold star schema")
    parser.add_argument("--publisher", required=True)
    parser.add_argument("--queries", action="store_true", help="Print four example SQL queries")
    parser.add_argument("--hold", action="store_true", help="Keep Spark UI alive until Enter")
    args = parser.parse_args(argv)
    spark = None
    try:
        spark = build_spark(f"gold-{args.publisher}")
        root = lake_uri("gold")
        result = build_gold(
            spark, publisher=args.publisher,
            silver_uri=lake_uri("silver", "articles"), gold_root_uri=root,
            run_ts=datetime.now(timezone.utc),
        )
        for name in GOLD_BUILD_ORDER:
            outcome = result.outcomes[name]
            label = f"{name} (SCD2)" if name == "dim_journal" else name
            extra = (
                f" | expired {outcome.updated} | current {result.current_journals}"
                if name == "dim_journal" else f" | updated {outcome.updated}"
            )
            print(
                f"{label:<27} rows {result.row_counts[name]} | "
                f"inserted {outcome.inserted}{extra} | deleted {outcome.deleted} | "
                f"skipped {str(outcome.skipped).lower()} | version {outcome.version}"
            )
        print("Integrity: " + " | ".join(result.integrity))
        print(
            "Unclassified article types: "
            + (", ".join(result.unclassified_types) if result.unclassified_types else "none")
        )
        if args.queries:
            _show_queries(spark, root, args.publisher)
        if args.hold:
            input("Spark UI is available on port 4040. Press Enter to stop Spark. ")
        return 0
    except Exception as exc:
        print(f"Gold build failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
