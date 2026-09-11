"""PostgreSQL integration tests for the manifest.

Skipped unless ``PL_TEST_DATABASE_URL`` is set, and it must point at a
**separate** database (``publisher_lakehouse_test``), never the working one:
these tests drop and recreate the schema.

    set PL_TEST_DATABASE_URL=postgresql+psycopg://user:pw@localhost:5432/publisher_lakehouse_test
    python -m pytest tests/test_manifest_repository_pg.py

The SQLite suite cannot stand in for this.  The upsert construct is a separate
import per dialect, and SQLite silently accepts naive timestamps where
PostgreSQL stores a real ``timestamp with time zone``.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, inspect, text

from publisher_lakehouse.manifest.models import ManifestRow
from publisher_lakehouse.manifest.repository import ManifestRepository, new_manifest_row

from alembic_helpers import downgrade_to_base, upgrade_to_head


TEST_DATABASE_URL = os.environ.get("PL_TEST_DATABASE_URL", "")

pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="PL_TEST_DATABASE_URL is not set",
)

NOW = datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc)


def guard_database_name(url: str) -> None:
    name = url.rsplit("/", 1)[-1].split("?")[0]
    if name == "publisher_lakehouse":
        raise RuntimeError(
            "PL_TEST_DATABASE_URL points at the working database; "
            "use publisher_lakehouse_test"
        )


@pytest.fixture(scope="module")
def engine():
    guard_database_name(TEST_DATABASE_URL)
    engine = create_engine(TEST_DATABASE_URL)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture()
def migrated_engine(engine):
    """Upgrade from empty for every test, then tear the schema back down."""

    with engine.begin() as connection:
        connection.execute(text("DROP TABLE IF EXISTS ingestion_manifest CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS alembic_version CASCADE"))

    upgrade_to_head(engine)
    try:
        yield engine
    finally:
        downgrade_to_base(engine)
        with engine.begin() as connection:
            connection.execute(text("DROP TABLE IF EXISTS alembic_version CASCADE"))


@pytest.fixture()
def repository(migrated_engine) -> ManifestRepository:
    return ManifestRepository(migrated_engine)


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


def test_alembic_upgrade_from_empty_creates_the_table(migrated_engine) -> None:
    inspector = inspect(migrated_engine)

    assert "ingestion_manifest" in inspector.get_table_names()
    assert inspector.get_pk_constraint("ingestion_manifest")["constrained_columns"] == [
        "url_hash"
    ]
    index_names = {index["name"] for index in inspector.get_indexes("ingestion_manifest")}
    assert "ix_ingestion_manifest_publisher_url_type" in index_names
    assert "ix_ingestion_manifest_publisher_status" in index_names


def test_timestamp_columns_are_timestamp_with_time_zone(migrated_engine) -> None:
    statement = text(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'ingestion_manifest'
          AND column_name IN ('first_seen_at', 'last_seen_at', 'last_changed_at')
        ORDER BY column_name
        """
    )
    with migrated_engine.connect() as connection:
        types = dict(connection.execute(statement).all())

    assert types == {
        "first_seen_at": "timestamp with time zone",
        "last_seen_at": "timestamp with time zone",
        "last_changed_at": "timestamp with time zone",
    }


def test_repository_uses_the_postgresql_upsert(repository: ManifestRepository) -> None:
    assert repository.dialect == "postgresql"


def test_batch_upsert_inserts(repository: ManifestRepository) -> None:
    assert repository.batch_upsert([article_row()]) == 1

    manifest = repository.load_for_publisher("sciencedirect")

    assert list(manifest) == ["hash-1"]
    assert isinstance(manifest["hash-1"], ManifestRow)
    assert manifest["hash-1"].fetch_count == 1


def test_batch_upsert_updates_the_same_url_hash(repository: ManifestRepository) -> None:
    repository.batch_upsert([article_row()])
    later = NOW + timedelta(days=100)

    repository.batch_upsert(
        [
            article_row(
                first_seen_at=later,
                last_seen_at=later,
                last_changed_at=later,
                last_run_id="20261220120000",
                payload_hash="b" * 64,
            )
        ]
    )

    manifest = repository.load_for_publisher("sciencedirect")
    stored = manifest["hash-1"]
    assert len(manifest) == 1
    assert stored.payload_hash == "b" * 64
    assert stored.last_run_id == "20261220120000"
    assert stored.first_seen_at == NOW
    assert stored.last_seen_at == later
    assert stored.fetch_count == 2


def test_round_trip_returns_timezone_aware_timestamps(
    repository: ManifestRepository,
) -> None:
    repository.batch_upsert([article_row()])

    stored = repository.load_for_publisher("sciencedirect")["hash-1"]

    for value in (stored.first_seen_at, stored.last_seen_at, stored.last_changed_at):
        assert value is not None
        assert value.tzinfo is not None
        assert value.utcoffset() is not None
    # Equality, not just awareness: a value stored in local time would still
    # be aware coming back, but would not equal the instant that was written.
    assert stored.last_seen_at == NOW
    assert stored.last_seen_at.astimezone(timezone.utc).isoformat() == NOW.isoformat()


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
    assert stored.doi == "10.1000/one"
    assert stored.payload_hash == "a" * 64


def test_a_batch_of_many_rows_round_trips(repository: ManifestRepository) -> None:
    rows = [article_row(url_hash=f"hash-{index}") for index in range(250)]

    assert repository.batch_upsert(rows) == 250
    assert len(repository.load_for_publisher("sciencedirect")) == 250


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
