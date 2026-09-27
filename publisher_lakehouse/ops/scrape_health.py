"""Copy scraper health from the manifest and run logs into warehouse ``ops``.

The manifest is read through ``ManifestRepository`` on a session whose every
transaction is read-only.  Each push is one warehouse transaction: it upserts
the optional run row and replaces the publisher's manifest snapshot rows.
``publish`` never truncates these tables; they belong to this command.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from publisher_lakehouse.manifest.models import ManifestRow


RUN_COMPLETE_EVENT = "ingestion_run_complete"

#: Real run ids are 14-digit timestamps, so this prefix can never collide.
INCOMPLETE_RUN_PREFIX = "incomplete-"

#: One BIGINT column per ``RunCounters`` field, named identically.  This is a
#: literal on purpose: adding a counter fails ``test_scrape_health`` until the
#: warehouse table is extended with it.
COUNTER_COLUMNS = (
    "input_total",
    "dup_in_file",
    "skipped_in_progress",
    "articles_discovered",
    "dup_in_run_url",
    "dup_in_run_doi",
    "skipped_manifest",
    "fetched",
    "fetch_failed",
    "blocked_gave_up",
    "parse_failed",
    "unchanged_payload",
    "bronze_written",
)

SCRAPE_RUNS_COLUMNS = (
    "run_id",
    "publisher",
    "finished_at",
    "ingest_exit_code",
    "status",
    *COUNTER_COLUMNS,
    "pushed_at",
)
SCRAPE_STATUS_COLUMNS = (
    "publisher",
    "url_type",
    "status",
    "row_count",
    "oldest_last_seen_at",
    "newest_last_seen_at",
    "snapshot_at",
)
SCRAPE_JOURNALS_COLUMNS = (
    "publisher",
    "journal_url",
    "status",
    "last_seen_at",
    "last_changed_at",
    "last_http_status",
    "fetch_count",
    "last_run_id",
    "snapshot_at",
)

_COUNTER_DDL = "".join(f"    {name} BIGINT,\n" for name in COUNTER_COLUMNS)

SCRAPE_DDL = (
    "CREATE TABLE IF NOT EXISTS ops.scrape_runs (\n"
    "    run_id TEXT PRIMARY KEY,\n"
    "    publisher TEXT NOT NULL,\n"
    "    finished_at TIMESTAMPTZ,\n"
    "    ingest_exit_code INT,\n"
    "    status TEXT NOT NULL CHECK (status IN ('complete', 'incomplete')),\n"
    f"{_COUNTER_DDL}"
    "    pushed_at TIMESTAMPTZ NOT NULL\n"
    ")",
    "CREATE TABLE IF NOT EXISTS ops.scrape_status (\n"
    "    publisher TEXT NOT NULL,\n"
    "    url_type TEXT NOT NULL,\n"
    "    status TEXT NOT NULL,\n"
    "    row_count BIGINT NOT NULL,\n"
    "    oldest_last_seen_at TIMESTAMPTZ,\n"
    "    newest_last_seen_at TIMESTAMPTZ,\n"
    "    snapshot_at TIMESTAMPTZ NOT NULL,\n"
    "    PRIMARY KEY (publisher, url_type, status)\n"
    ")",
    "CREATE TABLE IF NOT EXISTS ops.scrape_journals (\n"
    "    publisher TEXT NOT NULL,\n"
    "    journal_url TEXT NOT NULL,\n"
    "    status TEXT NOT NULL,\n"
    "    last_seen_at TIMESTAMPTZ,\n"
    "    last_changed_at TIMESTAMPTZ,\n"
    "    last_http_status INT,\n"
    "    fetch_count BIGINT,\n"
    "    last_run_id TEXT,\n"
    "    snapshot_at TIMESTAMPTZ NOT NULL,\n"
    "    PRIMARY KEY (publisher, journal_url)\n"
    ")",
)


def _insert_sql(table: str, columns: Sequence[str]) -> str:
    names = ", ".join(columns)
    values = ", ".join(f"%({name})s" for name in columns)
    return f"INSERT INTO ops.{table} ({names}) VALUES ({values})"


UPSERT_RUN_SQL = (
    _insert_sql("scrape_runs", SCRAPE_RUNS_COLUMNS)
    + " ON CONFLICT (run_id) DO UPDATE SET "
    + ", ".join(
        f"{name} = EXCLUDED.{name}" for name in SCRAPE_RUNS_COLUMNS if name != "run_id"
    )
)
INSERT_STATUS_SQL = _insert_sql("scrape_status", SCRAPE_STATUS_COLUMNS)
INSERT_JOURNALS_SQL = _insert_sql("scrape_journals", SCRAPE_JOURNALS_COLUMNS)


def read_run_complete_event(path: Path) -> dict[str, Any] | None:
    """Return the last ``ingestion_run_complete`` event in a structlog log.

    The log is UTF-8 and may start with a BOM.  Non-JSON lines, such as a
    traceback, are ignored.  A missing file reads as a log without the event:
    the ingest step may have failed before writing anything.
    """

    if not path.exists():
        return None
    complete = None
    with path.open(encoding="utf-8-sig", errors="replace") as handle:
        for line in handle:
            text = line.strip()
            if not text.startswith("{"):
                continue
            try:
                event = json.loads(text)
            except ValueError:
                continue
            if isinstance(event, dict) and event.get("event") == RUN_COMPLETE_EVENT:
                complete = event
    return complete


def incomplete_run_id(run_log: Path) -> str:
    """Derive the id of a run that never logged its completion event."""

    return INCOMPLETE_RUN_PREFIX + run_log.resolve().parent.name


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _counter(event: Mapping[str, Any], name: str) -> int | None:
    value = event.get(name)
    return None if value is None else int(value)


def run_row(
    run_log: Path,
    *,
    publisher: str,
    ingest_exit_code: int | None,
    pushed_at: datetime,
) -> dict[str, Any]:
    """Map one ingest log to a ``scrape_runs`` row."""

    event = read_run_complete_event(run_log)
    if event is None:
        # The file's last write is the closest record of when the run ended.
        finished_at = (
            datetime.fromtimestamp(run_log.stat().st_mtime, timezone.utc)
            if run_log.exists()
            else pushed_at
        )
        return {
            "run_id": incomplete_run_id(run_log),
            "publisher": publisher,
            "finished_at": finished_at,
            "ingest_exit_code": ingest_exit_code,
            "status": "incomplete",
            **{name: None for name in COUNTER_COLUMNS},
            "pushed_at": pushed_at,
        }

    run_id = event.get("run_id")
    if not run_id:
        raise ValueError(f"{RUN_COMPLETE_EVENT} in {run_log} has no run_id")
    return {
        "run_id": str(run_id),
        "publisher": publisher,
        "finished_at": _timestamp(event.get("timestamp")) or pushed_at,
        "ingest_exit_code": ingest_exit_code,
        "status": "complete",
        **{name: _counter(event, name) for name in COUNTER_COLUMNS},
        "pushed_at": pushed_at,
    }


def status_snapshot(
    rows: Iterable[ManifestRow], *, publisher: str, snapshot_at: datetime
) -> list[dict[str, Any]]:
    """Aggregate one publisher's manifest rows by URL type and status."""

    seen: dict[tuple[str, str], list[datetime]] = {}
    for row in rows:
        seen.setdefault((row.url_type, row.status), []).append(row.last_seen_at)
    return [
        {
            "publisher": publisher,
            "url_type": url_type,
            "status": status,
            "row_count": len(times),
            "oldest_last_seen_at": min(times),
            "newest_last_seen_at": max(times),
            "snapshot_at": snapshot_at,
        }
        for (url_type, status), times in sorted(seen.items())
    ]


def journal_snapshot(
    rows: Iterable[ManifestRow], *, publisher: str, snapshot_at: datetime
) -> list[dict[str, Any]]:
    """Copy one publisher's journal-tier manifest rows."""

    return [
        {
            "publisher": publisher,
            "journal_url": row.normalised_url,
            "status": row.status,
            "last_seen_at": row.last_seen_at,
            "last_changed_at": row.last_changed_at,
            "last_http_status": row.last_http_status,
            "fetch_count": row.fetch_count,
            "last_run_id": row.last_run_id,
            "snapshot_at": snapshot_at,
        }
        for row in sorted(rows, key=lambda row: row.normalised_url)
        if row.url_type == "journal"
    ]


def read_only_manifest_engine(database_url: str):
    """Create a manifest engine whose PostgreSQL sessions cannot write."""

    from sqlalchemy import create_engine, make_url

    connect_args = {}
    if make_url(database_url).get_backend_name() == "postgresql":
        # Every transaction on these sessions is read-only, so a reporting
        # command cannot write to the manifest even by mistake.
        connect_args["options"] = "-c default_transaction_read_only=on"
    return create_engine(database_url, connect_args=connect_args)


def load_manifest_rows(database_url: str, publisher: str) -> list[ManifestRow]:
    """Read one publisher's manifest on a read-only session."""

    from publisher_lakehouse.manifest.repository import ManifestRepository

    engine = read_only_manifest_engine(database_url)
    try:
        return list(ManifestRepository(engine).load_for_publisher(publisher).values())
    finally:
        engine.dispose()


def _add_missing_counter_columns(cursor) -> None:
    """Extend a table created before a counter was added to the literal."""

    cursor.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'ops' AND table_name = 'scrape_runs'"
    )
    existing = {row[0] for row in cursor.fetchall()}
    for name in COUNTER_COLUMNS:
        if name not in existing:
            cursor.execute(f"ALTER TABLE ops.scrape_runs ADD COLUMN {name} BIGINT")


def write_scrape_health(
    connection,
    *,
    publisher: str,
    run: Mapping[str, Any] | None,
    status_rows: Sequence[Mapping[str, Any]],
    journal_rows: Sequence[Mapping[str, Any]],
) -> None:
    """Apply one push in a single transaction on an autocommit connection."""

    with connection.transaction(), connection.cursor() as cursor:
        # Concurrent pushes would otherwise interleave delete and insert.
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext('ops.scrape_health'))")
        for statement in SCRAPE_DDL:
            cursor.execute(statement)
        _add_missing_counter_columns(cursor)
        if run is not None:
            cursor.execute(UPSERT_RUN_SQL, run)
        cursor.execute("DELETE FROM ops.scrape_status WHERE publisher = %s", (publisher,))
        if status_rows:
            cursor.executemany(INSERT_STATUS_SQL, status_rows)
        cursor.execute("DELETE FROM ops.scrape_journals WHERE publisher = %s", (publisher,))
        if journal_rows:
            cursor.executemany(INSERT_JOURNALS_SQL, journal_rows)


def connect_warehouse(settings):
    """Open an autocommit connection to the warehouse as its owning role."""

    import psycopg

    return psycopg.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.db,
        user=settings.user,
        password=settings.password.get_secret_value(),
        autocommit=True,
        connect_timeout=10,
        application_name="publisher-lakehouse push-scrape-health",
    )


@dataclass(frozen=True)
class PushResult:
    publisher: str
    run: Mapping[str, Any] | None
    manifest_rows: int
    status_rows: int
    journal_rows: int

    def summary(self) -> str:
        if self.run is None:
            run = "run not recorded (no --run-log)"
        else:
            code = self.run["ingest_exit_code"]
            run = (
                f"run {self.run['run_id']} {self.run['status']} "
                f"(ingest exit {'unknown' if code is None else code})"
            )
        return (
            f"Scrape health {self.publisher}: {run} | "
            f"manifest rows {self.manifest_rows} | "
            f"status rows {self.status_rows} | journals {self.journal_rows}"
        )


def push_scrape_health(
    publisher: str,
    *,
    database_url: str,
    warehouse,
    run_log: Path | None = None,
    ingest_exit_code: int | None = None,
    now: datetime | None = None,
    manifest_loader=load_manifest_rows,
    connect=connect_warehouse,
) -> PushResult:
    """Record one run, when given its log, and refresh the manifest snapshot."""

    now = now or datetime.now(timezone.utc)
    run = None
    if run_log is not None:
        run = run_row(
            run_log,
            publisher=publisher,
            ingest_exit_code=ingest_exit_code,
            pushed_at=now,
        )
    manifest = manifest_loader(database_url, publisher)
    status_rows = status_snapshot(manifest, publisher=publisher, snapshot_at=now)
    journal_rows = journal_snapshot(manifest, publisher=publisher, snapshot_at=now)
    with connect(warehouse) as connection:
        write_scrape_health(
            connection,
            publisher=publisher,
            run=run,
            status_rows=status_rows,
            journal_rows=journal_rows,
        )
    return PushResult(
        publisher=publisher,
        run=run,
        manifest_rows=len(manifest),
        status_rows=len(status_rows),
        journal_rows=len(journal_rows),
    )
