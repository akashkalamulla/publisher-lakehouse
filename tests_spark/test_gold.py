"""End-to-end gold star behavior on local Delta paths."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from publisher_lakehouse.transform.gold import build_gold, validate_gold
from publisher_lakehouse.transform.schemas import GOLD_BUILD_ORDER, GOLD_SPECS
from publisher_lakehouse.transform.silver import build_silver


RUN_TS = datetime(2026, 9, 26, 16, 0, tzinfo=timezone.utc)


def _silver(spark, lake, when=RUN_TS):
    return build_silver(
        spark, publisher=lake["publisher"], bronze_uri=lake["bronze_uri"],
        silver_uri=lake["silver_uri"], quarantine_uri=lake["quarantine_uri"],
        dq_uri=lake["dq_uri"], run_ts=when,
    )


def _gold(spark, lake, when=RUN_TS):
    return build_gold(
        spark, publisher=lake["publisher"], silver_uri=lake["silver_uri"],
        gold_root_uri=lake["gold_root_uri"], run_ts=when,
    )


def _table(spark, lake, name):
    return spark.read.format("delta").load(f"{lake['gold_root_uri']}/{name}")


def test_gold_first_rerun_scd2_bridge_dates_and_author_identity(
    spark, gold_lake, silver_article_factory, bronze_setup
):
    journal_url = "https://example.test/journals/j"
    first = silver_article_factory(
        "a", scraped_at="2026-09-24T05:00:00Z", journal_url=journal_url,
        journal_title="Journal Original", issue_url="https://example.test/issue/1",
        article_type="Research article", article_publication_year="2026",
        article_publication_month="July",
        authors=[
            {"name": "Alice", "affiliation": "Old Lab", "email": "a@example.test"},
            {"name": "Bob", "affiliation": "Institute", "email": ""},
        ],
    )
    older_peer = silver_article_factory(
        "b", scraped_at="2026-09-25T05:00:00Z", journal_url=journal_url,
        journal_title="Journal Original", issue_url="", article_type="Editorial",
        authors=[
            {"name": "ALICE", "affiliation": "New Lab", "email": ""},
            {"name": "Carol", "affiliation": "Institute", "email": ""},
        ],
    )
    bronze_setup([first, older_peer])
    _silver(spark, gold_lake)
    initial = _gold(spark, gold_lake)
    assert initial.integrity
    assert initial.row_counts["fact_article"] == 2
    assert initial.row_counts["bridge_article_author"] == 4
    assert initial.current_journals == 1
    assert initial.unclassified_types == ()
    for name, spec in GOLD_SPECS.items():
        table = _table(spark, gold_lake, name)
        if name.startswith("dim_"):
            assert table.filter(f"{spec.surrogate_key} = -1").count() == 1
            assert table.select(spec.surrogate_key).distinct().count() == table.count()
    fact = {row["url_hash"]: row for row in _table(spark, gold_lake, "fact_article").collect()}
    assert fact["a"]["pub_month_date_sk"] == 20260701
    assert fact["b"]["pub_month_date_sk"] == -1
    assert fact["b"]["issue_sk"] == -1
    authors = _table(spark, gold_lake, "dim_author")
    alice = authors.filter("author_nk = 'alice'").collect()
    assert len(alice) == 1
    assert _table(spark, gold_lake, "bridge_article_author").filter(
        f"author_sk = {alice[0]['author_sk']}"
    ).count() == 2

    rerun = _gold(spark, gold_lake, RUN_TS + timedelta(hours=1))
    assert all(rerun.outcomes[name].skipped for name in GOLD_BUILD_ORDER)
    assert all(
        rerun.outcomes[name].version == initial.outcomes[name].version
        for name in GOLD_BUILD_ORDER
    )

    changed = silver_article_factory(
        "a", run_id="run-2", scraped_at="2026-09-27T05:00:00Z",
        journal_url=journal_url, journal_title="Journal Renamed",
        issue_url="https://example.test/issue/1", article_type="Research article",
        article_publication_year="2026", article_publication_month="July",
        authors=[{"name": "Alice", "affiliation": "Latest Lab", "email": ""}],
    )
    bronze_setup([changed], part="run-2")
    _silver(spark, gold_lake, RUN_TS + timedelta(hours=2))
    evolved = _gold(spark, gold_lake, RUN_TS + timedelta(hours=3))
    assert evolved.integrity
    assert (evolved.outcomes["dim_journal"].inserted,
            evolved.outcomes["dim_journal"].updated) == (1, 1)
    journal = _table(spark, gold_lake, "dim_journal").filter(
        "journal_sk != -1"
    ).orderBy("effective_from").collect()
    assert len(journal) == 2
    assert (journal[0]["is_current"], journal[1]["is_current"]) == (False, True)
    assert journal[0]["effective_to"] == journal[1]["effective_from"]
    assert journal[1]["journal_title"] == "Journal Renamed"
    fact = {row["url_hash"]: row for row in _table(spark, gold_lake, "fact_article").collect()}
    assert fact["a"]["journal_sk"] == journal[1]["journal_sk"]
    assert fact["b"]["journal_sk"] == journal[0]["journal_sk"]
    assert evolved.outcomes["bridge_article_author"].deleted == 1
    bridge = _table(spark, gold_lake, "bridge_article_author")
    assert bridge.count() == 3
    assert bridge.filter(f"article_sk = {fact['a']['article_sk']}").first()["weight"] == 1.0


def test_unmapped_type_and_integrity_guard(
    spark, gold_lake, silver_article_factory, bronze_setup
):
    bronze_setup([
        silver_article_factory(
            "unmapped", journal_title="Journal", article_type="Alien material",
            authors=[],
        )
    ])
    _silver(spark, gold_lake)
    result = _gold(spark, gold_lake)
    assert result.unclassified_types == ("alien_material",)
    article_type = _table(spark, gold_lake, "dim_article_type").filter(
        "article_type_key = 'alien_material'"
    ).first()
    assert (article_type["content_class"], article_type["is_scholarly"]) == (
        "unclassified", False
    )
    fact_uri = f"{gold_lake['gold_root_uri']}/fact_article"
    spark.sql(f"UPDATE delta.`{fact_uri}` SET journal_sk = 987654321")
    with pytest.raises(RuntimeError, match="Fact journal_sk has no dim_journal row"):
        validate_gold(
            spark, publisher=gold_lake["publisher"],
            silver_uri=gold_lake["silver_uri"], gold_root_uri=gold_lake["gold_root_uri"],
        )
