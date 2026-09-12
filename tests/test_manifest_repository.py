"""Offline manifest repository tests against the SQLite dialect.

The upsert construct is dialect-specific, so these run the SQLite branch and
`test_manifest_repository_pg.py` runs the PostgreSQL branch.  SQLite here is a
test-only convenience; PostgreSQL is the development target.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import create_engine, inspect

from publisher_lakehouse.manifest.models import (
    ManifestRow,
    ingestion_manifest,
    metadata,
)
from publisher_lakehouse.manifest.repository import (
    ManifestRepository,
    new_manifest_row,
    stage_rows,
)

from alembic_helpers import upgrade_to_head


NOW = datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc)


@pytest.fixture()
def engine(tmp_path: Path):
    engine = create_engine(f"sqlite:///{tmp_path / 'manifest.db'}")
    upgrade_to_head(engine)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture()
def repository(engine) -> ManifestRepository:
    return ManifestRepository(engine)


def test_upgrade_with_supplied_connection_does_not_read_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import publisher_lakehouse.settings as settings

    def fail_if_environment_is_read():
        raise AssertionError("env should not be read")

    monkeypatch.setattr(settings, "load_environment_settings", fail_if_environment_is_read)
    migration_engine = create_engine(f"sqlite:///{tmp_path / 'migration.db'}")
    try:
        upgrade_to_head(migration_engine)
        assert inspect(migration_engine).has_table("ingestion_manifest")
    finally:
        migration_engine.dispose()


def article_row(**overrides):
    base = new_manifest_row(
        url_hash="hash-1",
        normalised_url="https://www.sciencedirect.com/science/article/pii/S1",
        publisher="sciencedirect",
        url_type="article",
        run_id="20260911120000",
        status="ok",
        now=NOW,
        doi="10.1000/one",
        payload_hash="a" * 64,
        last_http_status=200,
        changed=True,
    )
    base.update(overrides)
    return base


def test_migration_matches_the_model_definition(engine) -> None:
    with engine.connect() as connection:
        context = MigrationContext.configure(connection)
        assert compare_metadata(context, metadata) == []


def test_unsupported_dialect_is_rejected_at_construction() -> None:
    class FakeDialect:
        name = "mysql"

    class FakeEngine:
        dialect = FakeDialect()

    with pytest.raises(ValueError) as excinfo:
        ManifestRepository(FakeEngine())

    assert "mysql" in str(excinfo.value)


def test_batch_upsert_inserts_then_loads(repository: ManifestRepository) -> None:
    assert repository.batch_upsert([article_row()]) == 1

    manifest = repository.load_for_publisher("sciencedirect")

    assert list(manifest) == ["hash-1"]
    stored = manifest["hash-1"]
    assert isinstance(stored, ManifestRow)
    assert stored.status == "ok"
    assert stored.doi == "10.1000/one"
    assert stored.fetch_count == 1


def test_batch_upsert_updates_the_same_url_hash(repository: ManifestRepository) -> None:
    repository.batch_upsert([article_row()])
    later = NOW + timedelta(days=100)

    repository.batch_upsert(
        [
            article_row(
                last_seen_at=later,
                first_seen_at=later,
                last_run_id="20261220120000",
                payload_hash="b" * 64,
                last_changed_at=later,
            )
        ]
    )

    manifest = repository.load_for_publisher("sciencedirect")
    stored = manifest["hash-1"]
    assert len(manifest) == 1
    assert stored.payload_hash == "b" * 64
    assert stored.last_run_id == "20261220120000"
    # first_seen_at is written once and never overwritten.
    assert stored.first_seen_at.replace(tzinfo=timezone.utc) == NOW
    assert stored.fetch_count == 2


def test_a_touch_records_an_outcome_without_counting_a_fetch(
    repository: ManifestRepository,
) -> None:
    repository.batch_upsert([article_row()])
    repository.batch_upsert([article_row(fetch_count=0)])

    assert repository.load_for_publisher("sciencedirect")["hash-1"].fetch_count == 1


def test_a_failed_refetch_does_not_erase_the_last_good_values(
    repository: ManifestRepository,
) -> None:
    repository.batch_upsert([article_row()])

    repository.batch_upsert(
        [
            article_row(
                status="failed",
                doi=None,
                payload_hash=None,
                last_changed_at=None,
                last_http_status=503,
            )
        ]
    )

    stored = repository.load_for_publisher("sciencedirect")["hash-1"]
    assert stored.status == "failed"
    assert stored.last_http_status == 503
    assert stored.doi == "10.1000/one"
    assert stored.payload_hash == "a" * 64
    assert stored.last_changed_at is not None


def test_load_filters_by_publisher(repository: ManifestRepository) -> None:
    repository.batch_upsert(
        [
            article_row(),
            article_row(
                url_hash="hash-2",
                publisher="springer",
                normalised_url="https://link.springer.com/article/10.1007/x",
            ),
        ]
    )

    assert list(repository.load_for_publisher("sciencedirect")) == ["hash-1"]
    assert list(repository.load_for_publisher("springer")) == ["hash-2"]


def test_empty_batch_is_a_no_op(repository: ManifestRepository) -> None:
    assert repository.batch_upsert([]) == 0


def test_unknown_status_is_rejected(repository: ManifestRepository) -> None:
    with pytest.raises(ValueError) as excinfo:
        repository.batch_upsert([article_row(status="probably_fine")])

    assert "probably_fine" in str(excinfo.value)


def test_unknown_column_is_rejected(repository: ManifestRepository) -> None:
    with pytest.raises(ValueError) as excinfo:
        repository.batch_upsert([article_row(bronze_path="data/bronze")])

    assert "bronze_path" in str(excinfo.value)


def test_naive_timestamp_is_rejected_before_it_reaches_sqlite(
    repository: ManifestRepository,
) -> None:
    with pytest.raises(ValueError) as excinfo:
        repository.batch_upsert([article_row(last_seen_at=NOW.replace(tzinfo=None))])

    assert "timezone-aware" in str(excinfo.value)


def test_missing_required_field_is_rejected(repository: ManifestRepository) -> None:
    with pytest.raises(ValueError) as excinfo:
        repository.batch_upsert([article_row(last_run_id=None)])

    assert "last_run_id" in str(excinfo.value)


def test_stage_rows_collapses_repeated_writes_to_one_url() -> None:
    staged = stage_rows(
        [
            article_row(status="blocked"),
            article_row(status="ok"),
            article_row(url_hash="hash-2"),
        ]
    )

    assert [row["url_hash"] for row in staged] == ["hash-1", "hash-2"]
    assert staged[0]["status"] == "ok"


def test_counts_by_status(repository: ManifestRepository) -> None:
    repository.batch_upsert(
        [
            article_row(),
            article_row(url_hash="hash-2", status="blocked"),
            article_row(url_hash="hash-3", url_type="journal"),
        ]
    )

    assert repository.counts_by_status("sciencedirect") == {
        ("article", "blocked"): 1,
        ("article", "ok"): 1,
        ("journal", "ok"): 1,
    }


def test_repository_uses_the_sqlite_upsert(repository: ManifestRepository) -> None:
    assert repository.dialect == "sqlite"


def test_a_batch_writes_in_one_transaction(repository: ManifestRepository) -> None:
    rows = [article_row(url_hash=f"hash-{index}") for index in range(50)]

    assert repository.batch_upsert(rows) == 50
    assert len(repository.load_for_publisher("sciencedirect")) == 50


def test_primary_key_is_url_hash() -> None:
    assert [column.name for column in ingestion_manifest.primary_key] == ["url_hash"]
