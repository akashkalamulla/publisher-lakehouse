from __future__ import annotations

import json

from publisher_lakehouse.common.logging import (
    bind_run_id,
    collected_error_events,
    configure_logging,
    get_logger,
    render_error_lines,
    write_error_report,
)


def test_structured_logs_go_to_stderr_leaving_stdout_to_the_browser_layer(
    capsys,
) -> None:
    configure_logging()
    bind_run_id("20260911102610")
    get_logger("test").info("run_started", publisher="sciencedirect")

    captured = capsys.readouterr()
    assert captured.out == ""
    event = json.loads(captured.err)
    assert event["event"] == "run_started"
    assert event["publisher"] == "sciencedirect"
    assert event["run_id"] == "20260911102610"


def test_error_events_keep_run_context_and_legacy_error_line(capsys) -> None:
    configure_logging()
    bind_run_id("20260911102612")
    get_logger("test").error(
        "article_parse_failed",
        article_url="https://example.test/article",
        error_line="legacy message | article",
    )

    emitted = json.loads(capsys.readouterr().err)
    events = collected_error_events()
    assert emitted["run_id"] == "20260911102612"
    assert len(events) == 1
    assert events[0]["article_url"] == "https://example.test/article"
    assert render_error_lines(events) == ["legacy message | article"]


def test_reconfiguring_logging_resets_collected_errors(capsys) -> None:
    configure_logging()
    get_logger("test").error("first_run_failure")
    assert len(collected_error_events()) == 1

    configure_logging()
    assert collected_error_events() == ()
    capsys.readouterr()


def test_error_collection_is_independent_of_the_output_threshold(capsys) -> None:
    configure_logging("CRITICAL")
    get_logger("test").error("hidden_from_the_log_stream", error_line="still collected")

    assert capsys.readouterr().err == ""
    assert render_error_lines() == ["still collected"]


def test_error_rendering_escapes_embedded_newlines() -> None:
    assert render_error_lines(
        [{"event": "failed", "detail": "line one\r\nline two\nline three"}]
    ) == [r"failed | detail=line one\nline two\nline three"]


def test_error_report_is_ordered_one_line_per_event_with_trailing_newline(
    tmp_path,
) -> None:
    path = write_error_report(
        tmp_path / "errors.txt",
        [
            {"event": "first"},
            {"event": "second", "detail": "line one\nline two"},
        ],
    )
    assert path.read_bytes() == b"first\nsecond | detail=line one\\nline two\n"
