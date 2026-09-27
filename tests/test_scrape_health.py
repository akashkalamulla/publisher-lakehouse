"""Offline contracts for ``ops push-scrape-health``.

No database is contacted: the manifest loader and the warehouse connection
are replaced with fakes.
"""

from __future__ import annotations

import re
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import publisher_lakehouse.cli as cli
from publisher_lakehouse.ingestion.pipeline import RunCounters
from publisher_lakehouse.manifest.models import ManifestRow
from publisher_lakehouse.ops import scrape_health
from publisher_lakehouse.ops.scrape_health import (
    COUNTER_COLUMNS,
    SCRAPE_DDL,
    SCRAPE_RUNS_COLUMNS,
    incomplete_run_id,
    journal_snapshot,
    push_scrape_health,
    read_run_complete_event,
    run_row,
    status_snapshot,
)
from publisher_lakehouse.settings import WarehouseSettings


NOW = datetime(2026, 9, 27, 10, 0, tzinfo=timezone.utc)
RUN_DIR = "20260927T020000Z-sciencedirect"


def complete_event(run_id: str, **counters: int) -> str:
    values = {name: 0 for name in COUNTER_COLUMNS} | counters
    body = ", ".join(f'"{name}": {value}' for name, value in values.items())
    return (
        f'{{{body}, "event": "ingestion_run_complete", "run_id": "{run_id}", '
        f'"level": "info", "timestamp": "2026-09-27T02:41:07.123456Z"}}'
    )


def write_log(tmp_path: Path, text: str, *, bom: bool = False) -> Path:
    run_dir = tmp_path / RUN_DIR
    run_dir.mkdir()
    path = run_dir / "ingest.err.log"
    path.write_bytes((b"\xef\xbb\xbf" if bom else b"") + text.encode("utf-8"))
    return path


def manifest_row(url: str, url_type: str, status: str, seen: datetime) -> ManifestRow:
    return ManifestRow(
        url_hash=url,
        normalised_url=url,
        publisher="sciencedirect",
        url_type=url_type,
        doi=None,
        payload_hash=None,
        first_seen_at=seen,
        last_seen_at=seen,
        last_changed_at=None,
        fetch_count=2,
        last_http_status=None,
        last_run_id="20260926121235",
        status=status,
    )


def test_scrape_runs_has_one_bigint_column_per_run_counter():
    counters = tuple(field.name for field in fields(RunCounters))

    assert COUNTER_COLUMNS == counters
    assert SCRAPE_RUNS_COLUMNS == (
        "run_id", "publisher", "finished_at", "ingest_exit_code", "status",
        *counters, "pushed_at",
    )
    ddl = SCRAPE_DDL[0]
    for name in counters:
        assert re.search(rf"^\s+{name} BIGINT,$", ddl, re.M), name


def test_parser_takes_the_last_complete_event(tmp_path: Path):
    log = write_log(
        tmp_path,
        "\n".join(
            [
                '{"event": "manifest_loaded", "rows": 242, "level": "info"}',
                complete_event("20260927020000", fetched=1),
                '{"event": "legacy_error", "level": "error"}',
                complete_event("20260927023000", fetched=7),
                '{"event": "manifest_flushed", "level": "info"}',
            ]
        ),
    )

    event = read_run_complete_event(log)

    assert event["run_id"] == "20260927023000"
    assert event["fetched"] == 7


def test_parser_tolerates_bom_and_ignores_non_json_lines(tmp_path: Path):
    log = write_log(
        tmp_path,
        "Volume: 73 | Issue: 3 — progress on stderr\n"
        "{not json at all\n"
        "[1, 2, 3]\n"
        + complete_event("20260927020000", bronze_written=4)
        + "\nTraceback (most recent call last):\n  File \"x.py\"\n",
        bom=True,
    )

    row = run_row(log, publisher="sciencedirect", ingest_exit_code=0, pushed_at=NOW)

    assert row["status"] == "complete"
    assert row["run_id"] == "20260927020000"
    assert row["bronze_written"] == 4
    assert row["finished_at"] == datetime(
        2026, 9, 27, 2, 41, 7, 123456, tzinfo=timezone.utc
    )
    assert row["ingest_exit_code"] == 0
    assert list(row) == list(SCRAPE_RUNS_COLUMNS)


@pytest.mark.parametrize("contents", [None, "", "Traceback (most recent call last):\n"])
def test_log_without_complete_event_is_an_incomplete_run(tmp_path: Path, contents):
    if contents is None:
        (tmp_path / RUN_DIR).mkdir()
        log = tmp_path / RUN_DIR / "ingest.err.log"
    else:
        log = write_log(tmp_path, contents)

    row = run_row(log, publisher="sciencedirect", ingest_exit_code=1, pushed_at=NOW)

    assert row["status"] == "incomplete"
    assert row["run_id"] == f"incomplete-{RUN_DIR}" == incomplete_run_id(log)
    # Real run ids are new_run_id() timestamps: fourteen digits.
    assert not re.fullmatch(r"\d{14}", row["run_id"])
    assert row["ingest_exit_code"] == 1
    assert all(row[name] is None for name in COUNTER_COLUMNS)
    assert row["finished_at"].tzinfo is not None


def test_snapshot_aggregates_by_type_and_status():
    old, mid, new = NOW - timedelta(days=9), NOW - timedelta(days=2), NOW
    rows = [
        manifest_row("https://j/b", "journal", "ok", mid),
        manifest_row("https://j/a", "journal", "failed", old),
        manifest_row("https://a/1", "article", "ok", old),
        manifest_row("https://a/2", "article", "ok", new),
        manifest_row("https://a/3", "article", "blocked", mid),
    ]

    status = status_snapshot(rows, publisher="sciencedirect", snapshot_at=NOW)
    journals = journal_snapshot(rows, publisher="sciencedirect", snapshot_at=NOW)

    assert [
        (r["url_type"], r["status"], r["row_count"], r["oldest_last_seen_at"],
         r["newest_last_seen_at"])
        for r in status
    ] == [
        ("article", "blocked", 1, mid, mid),
        ("article", "ok", 2, old, new),
        ("journal", "failed", 1, old, old),
        ("journal", "ok", 1, mid, mid),
    ]
    assert sum(r["row_count"] for r in status) == len(rows)
    assert all(r["snapshot_at"] == NOW and r["publisher"] == "sciencedirect" for r in status)
    assert [(j["journal_url"], j["status"], j["fetch_count"]) for j in journals] == [
        ("https://j/a", "failed", 2),
        ("https://j/b", "ok", 2),
    ]


def test_module_import_does_not_import_pyspark():
    code = (
        "import sys\n"
        "import publisher_lakehouse.ops.scrape_health\n"
        "assert 'pyspark' not in sys.modules\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


class FakeCursor:
    def __init__(self, log: list, existing_columns: set[str]):
        self.log = log
        self.existing_columns = existing_columns
        self._rows: list = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql: str, params=None):
        self.log.append(("execute", sql, params))
        if "information_schema.columns" in sql:
            self._rows = [(name,) for name in self.existing_columns]

    def executemany(self, sql: str, params):
        self.log.append(("executemany", sql, list(params)))

    def fetchall(self):
        return self._rows


class FakeConnection:
    def __init__(self, existing_columns: set[str] | None = None):
        self.log: list = []
        self.existing_columns = existing_columns or set(SCRAPE_RUNS_COLUMNS)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.log.append(("closed", "", None))
        return False

    @contextmanager
    def transaction(self):
        self.log.append(("begin", "", None))
        yield
        self.log.append(("commit", "", None))

    def cursor(self):
        return FakeCursor(self.log, self.existing_columns)


def push(tmp_path: Path, connection: FakeConnection, *, run_log: Path | None):
    rows = [
        manifest_row("https://j/a", "journal", "ok", NOW),
        manifest_row("https://a/1", "article", "ok", NOW),
    ]
    return push_scrape_health(
        "sciencedirect",
        database_url="postgresql+psycopg://unused",
        warehouse=object(),
        run_log=run_log,
        ingest_exit_code=0,
        now=NOW,
        manifest_loader=lambda url, publisher: rows,
        connect=lambda settings: connection,
    )


def test_push_is_one_transaction_that_upserts_the_run_and_replaces_the_snapshot(
    tmp_path: Path,
):
    log = write_log(tmp_path, complete_event("20260927020000", fetched=3))
    connection = FakeConnection()

    result = push(tmp_path, connection, run_log=log)

    kinds = [entry[0] for entry in connection.log]
    assert kinds[0] == "begin" and kinds[-2:] == ["commit", "closed"]
    assert kinds.count("begin") == 1
    statements = [entry[1] for entry in connection.log if entry[0] in {"execute", "executemany"}]
    assert "pg_advisory_xact_lock" in statements[0]
    assert statements[1:4] == list(SCRAPE_DDL)
    upsert = next(e for e in connection.log if "INSERT INTO ops.scrape_runs" in str(e[1]))
    assert "ON CONFLICT (run_id) DO UPDATE" in upsert[1]
    assert upsert[2]["run_id"] == "20260927020000" and upsert[2]["fetched"] == 3
    deletes = [e for e in connection.log if str(e[1]).startswith("DELETE")]
    assert [(e[1].split()[2], e[2]) for e in deletes] == [
        ("ops.scrape_status", ("sciencedirect",)),
        ("ops.scrape_journals", ("sciencedirect",)),
    ]
    inserted = {
        e[1].split()[2]: e[2] for e in connection.log if e[0] == "executemany"
    }
    assert [r["url_type"] for r in inserted["ops.scrape_status"]] == ["article", "journal"]
    assert [r["journal_url"] for r in inserted["ops.scrape_journals"]] == ["https://j/a"]
    assert result.summary() == (
        "Scrape health sciencedirect: run 20260927020000 complete (ingest exit 0) | "
        "manifest rows 2 | status rows 2 | journals 1"
    )


def test_push_without_run_log_only_refreshes_the_snapshot(tmp_path: Path):
    connection = FakeConnection()

    result = push(tmp_path, connection, run_log=None)

    assert not any("scrape_runs (" in str(e[1]) and "INSERT" in str(e[1]) for e in connection.log)
    assert result.run is None
    assert result.summary().startswith(
        "Scrape health sciencedirect: run not recorded (no --run-log) |"
    )


def test_push_adds_counter_columns_missing_from_an_older_table(tmp_path: Path):
    connection = FakeConnection(set(SCRAPE_RUNS_COLUMNS) - {"bronze_written"})

    push(tmp_path, connection, run_log=None)

    altered = [e[1] for e in connection.log if str(e[1]).startswith("ALTER TABLE")]
    assert altered == ["ALTER TABLE ops.scrape_runs ADD COLUMN bronze_written BIGINT"]


def test_warehouse_settings_default_to_the_host_port(monkeypatch: pytest.MonkeyPatch):
    for name in ("WAREHOUSE_HOST", "WAREHOUSE_PORT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("WAREHOUSE_DB", "warehouse")
    monkeypatch.setenv("WAREHOUSE_USER", "owner")
    monkeypatch.setenv("WAREHOUSE_PASSWORD", "not-a-real-secret")

    settings = WarehouseSettings()
    assert (settings.host, settings.port, settings.db, settings.user) == (
        "127.0.0.1", 5433, "warehouse", "owner",
    )
    assert "not-a-real-secret" not in repr(settings)

    monkeypatch.setenv("WAREHOUSE_HOST", "warehouse")
    monkeypatch.setenv("WAREHOUSE_PORT", "5432")
    assert (WarehouseSettings().host, WarehouseSettings().port) == ("warehouse", 5432)


def test_cli_forwards_options_and_prints_one_summary_line(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    call: dict = {}

    def fake_push(publisher, **kwargs):
        call.update(kwargs, publisher=publisher)
        return scrape_health.PushResult(
            publisher=publisher, run=None, manifest_rows=242, status_rows=1, journal_rows=0
        )

    monkeypatch.setattr(
        cli, "load_environment_settings",
        lambda: SimpleNamespace(database_url="postgresql+psycopg://unused", log_level="INFO"),
    )
    monkeypatch.setattr(cli, "load_warehouse_settings", lambda: "warehouse-settings")
    monkeypatch.setattr(cli, "push_scrape_health", fake_push)
    log = tmp_path / "ingest.err.log"

    result = CliRunner().invoke(
        cli.app,
        [
            "ops", "push-scrape-health", "--publisher", "sciencedirect",
            "--run-log", str(log), "--ingest-exit-code", "3",
        ],
    )

    assert result.exit_code == 0, result.output
    assert call == {
        "publisher": "sciencedirect",
        "database_url": "postgresql+psycopg://unused",
        "warehouse": "warehouse-settings",
        "run_log": log,
        "ingest_exit_code": 3,
    }
    assert result.stdout.splitlines() == [
        "Scrape health sciencedirect: run not recorded (no --run-log) | "
        "manifest rows 242 | status rows 1 | journals 0"
    ]
