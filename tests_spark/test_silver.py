"""Silver article selection, typing and quality behavior on local file URIs."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from publisher_lakehouse.transform.schemas import DQ_RULES
from publisher_lakehouse.transform.silver import build_silver


RUN_TS = datetime(2026, 9, 26, 14, 0, tzinfo=timezone.utc)


def _build(spark, lake, run_ts=RUN_TS):
    return build_silver(
        spark,
        publisher=lake["publisher"],
        bronze_uri=lake["bronze_uri"],
        silver_uri=lake["silver_uri"],
        quarantine_uri=lake["quarantine_uri"],
        dq_uri=lake["dq_uri"],
        run_ts=run_ts,
    )


def _silver(spark, lake):
    return spark.read.format("delta").load(lake["silver_uri"])


def _dq(spark, lake, run_id):
    return spark.read.format("delta").load(lake["dq_uri"]).filter(
        f"dq_run_id = '{run_id}'"
    )


def test_latest_noop_new_version_and_hash_repair(
    spark, silver_lake, silver_article_factory, bronze_setup
):
    older = silver_article_factory(
        "same", scraped_at="2026-09-24T05:00:00Z", english_title="Older title"
    )
    newer = silver_article_factory(
        "same", run_id="run-2", scraped_at="2026-09-25T05:00:00Z",
        english_title="Newer title",
    )
    bronze_setup([older])
    bronze_setup([newer], part="run-2")
    first = _build(spark, silver_lake)
    assert (first.bronze_rows, first.candidates, first.inserted, first.updated) == (2, 1, 1, 0)
    row = _silver(spark, silver_lake).first()
    assert row["english_title"] == "Newer title"
    assert row["version_count"] == 2
    assert str(row["first_seen_at"]) == "2026-09-24 05:00:00"
    assert row["latest_run_id"] == "run-2"
    updated_at = row["_silver_updated_at"]

    second = _build(spark, silver_lake, RUN_TS + timedelta(hours=1))
    assert (second.inserted, second.updated, second.unchanged, second.skipped) == (
        0, 0, 1, True
    )
    assert second.silver_version == first.silver_version
    assert _silver(spark, silver_lake).first()["_silver_updated_at"] == updated_at
    assert _dq(spark, silver_lake, first.dq_run_id).count() == len(DQ_RULES)
    assert _dq(spark, silver_lake, second.dq_run_id).count() == len(DQ_RULES)

    newest = silver_article_factory(
        "same", run_id="run-3", scraped_at="2026-09-26T05:00:00Z",
        english_title="Newest title",
    )
    bronze_setup([newest], part="run-3")
    third = _build(spark, silver_lake, RUN_TS + timedelta(hours=2))
    assert (third.inserted, third.updated, third.silver_version) == (
        0, 1, first.silver_version + 1
    )
    assert _silver(spark, silver_lake).first()["english_title"] == "Newest title"

    # A direct business-column edit leaves a stale stored hash; the rerun repairs it.
    spark.sql(
        f"UPDATE delta.`{silver_lake['silver_uri']}` "
        "SET english_title = 'Drifted title' "
        "WHERE url_hash = 'same'"
    )
    repaired = _build(spark, silver_lake, RUN_TS + timedelta(hours=3))
    assert (repaired.inserted, repaired.updated) == (0, 1)
    assert _silver(spark, silver_lake).first()["english_title"] == "Newest title"


def test_typed_values_flags_and_cleaned_authors(
    spark, silver_lake, silver_article_factory, bronze_setup
):
    records = [
        silver_article_factory(
            "july", article_publication_month="July", article_publication_year="2026",
            article_publication_day="", issue_publication_month="Jul",
            issue_publication_year="2026", reference_count="20", start_page="289",
            last_page="291", author_keyword=" a | b ||a ",
            doi="https://doi.org/10.1016/J.X.1",
            license_type="http://creativecommons.org/licenses/by-nc-nd/4.0/",
            volume="73", authors=[
                {"name": "  ", "affiliation": "unused", "email": ""},
                {"name": "  Alice  ", "affiliation": "  Institute  ", "email": ""},
            ],
        ),
        silver_article_factory(
            "jul", article_publication_month="Jul", article_publication_year="2026",
            article_publication_day="31",
        ),
        silver_article_factory(
            "numeric", article_publication_month="7", article_publication_year="2026"
        ),
        silver_article_factory(
            "range", article_publication_month="July-August",
            article_publication_year="2026",
        ),
        silver_article_factory(
            "spring", article_publication_month="Spring", article_publication_year="2026",
            reference_count="abc", copyright_year="1800", start_page="300",
            last_page="290", volume="S1", issue="3-4", authors=[],
        ),
    ]
    bronze_setup(records)
    result = _build(spark, silver_lake)
    assert result.inserted == 5
    rows = {row["url_hash"]: row for row in _silver(spark, silver_lake).collect()}
    july = rows["july"]
    assert (july["article_pub_year"], july["article_pub_month"], july["article_pub_day"]) == (
        2026, 7, None
    )
    assert july["article_date_precision"] == "month"
    assert str(july["article_pub_month_start"]) == "2026-07-01"
    assert july["issue_pub_month"] == 7
    assert (july["reference_count"], july["page_count"], july["volume_num"]) == (20, 3, 73)
    assert july["keywords"] == ["a", "b"]
    assert (july["doi"], july["doi_valid"]) == ("10.1016/j.x.1", True)
    assert (july["license_code"], july["has_cc_license"]) == ("CC BY-NC-ND 4.0", True)
    assert july["author_count"] == 1
    assert july["authors"][0]["name"] == "Alice"
    assert july["authors"][0]["affiliation"] == "Institute"
    assert july["authors"][0]["email"] is None
    assert rows["jul"]["article_pub_month"] == 7
    assert rows["jul"]["article_date_precision"] == "day"
    assert rows["numeric"]["article_pub_month"] == 7
    assert rows["range"]["article_pub_month"] == 7
    assert "month_range" in rows["range"]["dq_flags"]
    spring = rows["spring"]
    assert spring["article_pub_month"] is None
    assert spring["article_date_precision"] == "year"
    assert (spring["reference_count"], spring["copyright_year"], spring["page_count"]) == (
        None, None, None
    )
    assert (spring["volume_num"], spring["issue_num"], spring["author_count"]) == (
        None, None, 0
    )
    assert {
        "unparsed_pub_month", "bad_reference_count", "bad_copyright_year",
        "bad_page_range", "no_authors",
    } <= set(spring["dq_flags"])

    dq = {row["rule"]: row for row in _dq(spark, silver_lake, result.dq_run_id).collect()}
    assert len(dq) == len(DQ_RULES)
    assert all(row["total_rows"] == 5 for row in dq.values())
    for name in (
        "month_range", "unparsed_pub_month", "bad_reference_count",
        "bad_copyright_year", "bad_page_range", "no_authors",
    ):
        assert dq[name]["failed_rows"] == 1
    assert dq["missing_title"]["failed_rows"] == 0


def test_quarantine_keeps_older_valid_version_and_does_not_duplicate(
    spark, silver_lake, silver_article_factory, bronze_setup
):
    no_title = silver_article_factory("invalid", english_title="", foreign_title="")
    older = silver_article_factory(
        "mixed", scraped_at="2026-09-24T05:00:00Z", english_title="Valid older"
    )
    newer_invalid = silver_article_factory(
        "mixed", run_id="run-2", scraped_at="2026-09-25T05:00:00Z",
        english_title="", foreign_title="",
    )
    bronze_setup([no_title, older])
    bronze_setup([newer_invalid], part="run-2")
    first = _build(spark, silver_lake)
    assert (first.bronze_rows, first.candidates, first.quarantined_new, first.silver_rows) == (
        3, 1, 2, 1
    )
    row = _silver(spark, silver_lake).first()
    assert (row["url_hash"], row["english_title"], row["version_count"]) == (
        "mixed", "Valid older", 2
    )
    quarantine = spark.read.format("delta").load(silver_lake["quarantine_uri"])
    assert quarantine.count() == 2
    assert all("missing_title" in row["dq_reasons"] for row in quarantine.collect())
    second = _build(spark, silver_lake, RUN_TS + timedelta(hours=1))
    assert (second.inserted, second.updated, second.quarantined_new) == (0, 0, 0)
    assert second.silver_version == first.silver_version
    assert spark.read.format("delta").load(silver_lake["quarantine_uri"]).count() == 2
    assert _dq(spark, silver_lake, first.dq_run_id).filter(
        "rule = 'missing_title'"
    ).first()["failed_rows"] == 2
    assert _dq(spark, silver_lake, second.dq_run_id).count() == len(DQ_RULES)
