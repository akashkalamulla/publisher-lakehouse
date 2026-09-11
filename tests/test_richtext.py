from __future__ import annotations

import json

from xlsxwriter.format import Format

from publisher_lakehouse.common.logging import (
    bind_run_id,
    collected_error_events,
    configure_logging,
    get_logger,
    render_error_lines,
    write_error_report,
)
from publisher_lakehouse.common.richtext import _json_text, flatten, to_segments


def test_rich_list_round_trip_preserves_legacy_flat_text() -> None:
    rich = [
        Format({"bold": True}),
        "Gene",
        Format({"italic": True}),
        " expression",
        Format({"font_script": 1}),
        "2",
        Format({"font_script": 2}),
        "n",
    ]

    segments = to_segments(rich)

    assert flatten(segments) == _json_text(rich)
    assert segments == [
        {
            "text": "Gene",
            "bold": True,
            "italic": False,
            "superscript": False,
            "subscript": False,
        },
        {
            "text": " expression",
            "bold": False,
            "italic": True,
            "superscript": False,
            "subscript": False,
        },
        {
            "text": "2",
            "bold": False,
            "italic": False,
            "superscript": True,
            "subscript": False,
        },
        {
            "text": "n",
            "bold": False,
            "italic": False,
            "superscript": False,
            "subscript": True,
        },
    ]


def test_abstract_style_hybrid_rich_list_round_trip() -> None:
    rich = [
        "Background\n",
        Format({"bold": True}),
        "Important",
        Format({}),
        " finding",
        "\n\nMethods\n",
        Format({"italic": True}),
        "Measured",
    ]
    segments = to_segments(rich)
    assert flatten(segments) == _json_text(rich) == (
        "Background\nImportant finding\n\nMethods\nMeasured"
    )
    assert segments == [
        {
            "text": "Background\n",
            "bold": False,
            "italic": False,
            "superscript": False,
            "subscript": False,
        },
        {
            "text": "Important",
            "bold": True,
            "italic": False,
            "superscript": False,
            "subscript": False,
        },
        {
            "text": " finding",
            "bold": False,
            "italic": False,
            "superscript": False,
            "subscript": False,
        },
        {
            "text": "\n\nMethods\n",
            "bold": False,
            "italic": False,
            "superscript": False,
            "subscript": False,
        },
        {
            "text": "Measured",
            "bold": False,
            "italic": True,
            "superscript": False,
            "subscript": False,
        },
    ]


def test_plain_string_becomes_one_unformatted_segment() -> None:
    assert to_segments("Plain title") == [
        {
            "text": "Plain title",
            "bold": False,
            "italic": False,
            "superscript": False,
            "subscript": False,
        }
    ]


def test_malformed_input_falls_back_without_raising_and_logs_warning(capsys) -> None:
    configure_logging()
    bind_run_id("20260911102611")
    malformed = [Format({"bold": True}), object(), "kept text"]

    segments = to_segments(malformed)

    assert segments == [
        {
            "text": "kept text",
            "bold": False,
            "italic": False,
            "superscript": False,
            "subscript": False,
        }
    ]
    log_event = json.loads(capsys.readouterr().out)
    assert log_event["event"] == "richtext_conversion_fallback"
    assert log_event["level"] == "warning"
    assert log_event["run_id"] == "20260911102611"
    assert collected_error_events() == ()


def test_error_events_keep_run_context_and_legacy_error_line(capsys) -> None:
    configure_logging()
    bind_run_id("20260911102612")
    get_logger("test").error(
        "article_parse_failed",
        article_url="https://example.test/article",
        error_line="legacy message | article",
    )

    emitted = json.loads(capsys.readouterr().out)
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


def test_error_collection_is_independent_of_stdout_threshold(capsys) -> None:
    configure_logging("CRITICAL")
    get_logger("test").error("hidden_from_stdout", error_line="still collected")

    assert capsys.readouterr().out == ""
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
