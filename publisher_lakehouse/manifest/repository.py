"""Synchronous manifest I/O.

Loaded once at run start and flushed in batches between journals.  Never call
this from inside the per-article async loop: a blocking round trip per article
would serialise the scrape behind the database.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from sqlalchemy import Engine, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from publisher_lakehouse.common.logging import get_logger
from publisher_lakehouse.manifest.models import (
    MANIFEST_COLUMNS,
    MANIFEST_STATUSES,
    ManifestRow,
    ingestion_manifest,
    require_aware,
)


logger = get_logger(__name__)

#: Set from the incoming row on every conflict.
_OVERWRITE_ON_CONFLICT = (
    "normalised_url",
    "publisher",
    "url_type",
    "last_seen_at",
    "last_run_id",
    "status",
    "last_http_status",
)

#: Kept from the existing row when the incoming value is NULL.  A failed
#: re-fetch must not erase the DOI and payload hash of the last good one.
_COALESCE_ON_CONFLICT = ("doi", "payload_hash", "last_changed_at")

_REQUIRED_FIELDS = (
    "url_hash",
    "normalised_url",
    "publisher",
    "url_type",
    "first_seen_at",
    "last_seen_at",
    "last_run_id",
    "status",
)


class ManifestRepository:
    """Batch reads and writes against ``ingestion_manifest``."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        dialect = engine.dialect.name
        if dialect == "postgresql":
            self._insert = pg_insert
        elif dialect == "sqlite":
            self._insert = sqlite_insert
        else:
            raise ValueError(
                f"Unsupported manifest dialect {dialect!r}; "
                "the upsert construct is dialect-specific"
            )
        self._dialect = dialect

    @property
    def dialect(self) -> str:
        return self._dialect

    def load_for_publisher(self, publisher: str) -> dict[str, ManifestRow]:
        """Read the whole manifest for one publisher in a single query."""

        statement = select(ingestion_manifest).where(
            ingestion_manifest.c.publisher == publisher
        )
        with self._engine.connect() as connection:
            rows = connection.execute(statement).mappings().all()

        manifest = {row["url_hash"]: ManifestRow.from_mapping(row) for row in rows}
        logger.info(
            "manifest_loaded",
            publisher=publisher,
            rows=len(manifest),
            dialect=self._dialect,
        )
        return manifest

    def batch_upsert(self, rows: Sequence[Mapping[str, Any]]) -> int:
        """Insert or update manifest rows, keyed on ``url_hash``.

        ``fetch_count`` is added to the stored value rather than replacing it,
        so a caller passes ``1`` for a fetch and ``0`` for a touch that records
        an outcome without a network round trip.
        """

        if not rows:
            return 0

        payload = [self._validated(row) for row in rows]
        statement = self._insert(ingestion_manifest)
        excluded = statement.excluded

        assignments: dict[str, Any] = {
            name: excluded[name] for name in _OVERWRITE_ON_CONFLICT
        }
        for name in _COALESCE_ON_CONFLICT:
            assignments[name] = func.coalesce(
                excluded[name], ingestion_manifest.c[name]
            )
        assignments["fetch_count"] = (
            ingestion_manifest.c.fetch_count + excluded.fetch_count
        )
        # first_seen_at is deliberately absent: it is written once, on insert.

        upsert = statement.on_conflict_do_update(
            index_elements=[ingestion_manifest.c.url_hash],
            set_=assignments,
        )

        with self._engine.begin() as connection:
            connection.execute(upsert, payload)

        logger.info("manifest_flushed", rows=len(payload), dialect=self._dialect)
        return len(payload)

    def counts_by_status(self, publisher: str) -> dict[tuple[str, str], int]:
        """Return ``{(url_type, status): count}`` for the stats command."""

        statement = (
            select(
                ingestion_manifest.c.url_type,
                ingestion_manifest.c.status,
                func.count().label("rows"),
            )
            .where(ingestion_manifest.c.publisher == publisher)
            .group_by(ingestion_manifest.c.url_type, ingestion_manifest.c.status)
            .order_by(ingestion_manifest.c.url_type, ingestion_manifest.c.status)
        )
        with self._engine.connect() as connection:
            return {
                (row.url_type, row.status): row.rows
                for row in connection.execute(statement)
            }

    def _validated(self, row: Mapping[str, Any]) -> dict[str, Any]:
        unknown = set(row) - set(MANIFEST_COLUMNS)
        if unknown:
            raise ValueError(
                f"Unknown manifest column(s): {', '.join(sorted(unknown))}"
            )

        missing = [name for name in _REQUIRED_FIELDS if row.get(name) is None]
        if missing:
            raise ValueError(
                f"Manifest row is missing required field(s): {', '.join(missing)}"
            )

        if row["status"] not in MANIFEST_STATUSES:
            raise ValueError(f"Unknown manifest status: {row['status']!r}")

        complete: dict[str, Any] = {name: row.get(name) for name in MANIFEST_COLUMNS}
        if complete["fetch_count"] is None:
            complete["fetch_count"] = 0

        for field in ("first_seen_at", "last_seen_at"):
            require_aware(complete[field], field)
        if complete["last_changed_at"] is not None:
            require_aware(complete["last_changed_at"], "last_changed_at")

        return complete


def new_manifest_row(
    *,
    url_hash: str,
    normalised_url: str,
    publisher: str,
    url_type: str,
    run_id: str,
    status: str,
    now,
    existing: ManifestRow | None = None,
    doi: str | None = None,
    payload_hash: str | None = None,
    last_http_status: int | None = None,
    changed: bool = False,
    fetched: bool = True,
) -> dict[str, Any]:
    """Build one staged manifest row from a run outcome."""

    require_aware(now, "now")
    return {
        "url_hash": url_hash,
        "normalised_url": normalised_url,
        "publisher": publisher,
        "url_type": url_type,
        "doi": doi,
        "payload_hash": payload_hash,
        "first_seen_at": existing.first_seen_at if existing else now,
        "last_seen_at": now,
        "last_changed_at": now if changed else None,
        "fetch_count": 1 if fetched else 0,
        "last_http_status": last_http_status,
        "last_run_id": run_id,
        "status": status,
    }


def stage_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Collapse repeated writes to one ``url_hash`` within a flush window."""

    staged: dict[str, dict[str, Any]] = {}
    for row in rows:
        staged[row["url_hash"]] = dict(row)
    return list(staged.values())
