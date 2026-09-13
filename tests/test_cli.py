from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import publisher_lakehouse.cli as cli
from publisher_lakehouse.ingestion.pipeline import RunCounters


runner = CliRunner()


@pytest.fixture
def settings_stub(monkeypatch: pytest.MonkeyPatch):
    settings = SimpleNamespace(environment=SimpleNamespace(log_level="INFO"))
    monkeypatch.setattr(cli, "load_settings", lambda: settings)
    return settings


def stderr_events(result) -> list[dict[str, object]]:
    return [json.loads(line) for line in result.stderr.splitlines() if line]


def test_limit_logs_truncation_warning_and_forwards_cli_options(
    monkeypatch: pytest.MonkeyPatch,
    settings_stub,
) -> None:
    counters = RunCounters(
        articles_discovered=1,
        fetched=1,
        bronze_written=1,
    )
    call: dict[str, object] = {}

    async def fake_run_ingestion(publisher: str, **kwargs):
        call["publisher"] = publisher
        call.update(kwargs)
        return counters

    monkeypatch.setattr(cli, "run_ingestion", fake_run_ingestion)

    result = runner.invoke(
        cli.app,
        [
            "ingest",
            "run",
            "--publisher",
            "sciencedirect",
            "--limit",
            "3",
            "--force-refetch",
        ],
    )

    assert result.exit_code == 0, result.output
    assert call == {
        "publisher": "sciencedirect",
        "limit": 3,
        "force_refetch": True,
        "settings": settings_stub,
    }
    events = stderr_events(result)
    warning = next(
        event for event in events if event["event"] == "ingestion_coverage_truncated"
    )
    assert warning["level"] == "warning"
    assert "COVERAGE TRUNCATED" in str(warning["warning"])
    assert warning["limit"] == 3
    assert result.stdout == ""


def test_broken_counter_sum_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch,
    settings_stub,
) -> None:
    async def fake_run_ingestion(publisher: str, **kwargs):
        return RunCounters(articles_discovered=2, bronze_written=1)

    monkeypatch.setattr(cli, "run_ingestion", fake_run_ingestion)

    result = runner.invoke(cli.app, ["ingest", "run"])

    assert result.exit_code != 0
    events = stderr_events(result)
    failure = next(
        event for event in events if event["event"] == "ingestion_counter_sum_invalid"
    )
    assert failure["level"] == "error"
    assert "articles_discovered" in str(failure["error"])
