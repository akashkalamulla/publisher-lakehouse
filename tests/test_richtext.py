from __future__ import annotations

import json

from xlsxwriter.format import Format

from publisher_lakehouse.common.logging import (
    bind_run_id,
    collected_error_events,
    configure_logging,
)
from publisher_lakehouse.common.richtext import _json_text, flatten, to_segments


def unformatted(text: str) -> dict:
    return {
        "text": text,
        "bold": False,
        "italic": False,
        "superscript": False,
        "subscript": False,
    }


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

    segments = to_segments(rich, "Gene expression2n")

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

    segments = to_segments(rich, "ignored because the rich value is usable")

    assert flatten(segments) == _json_text(rich) == (
        "Background\nImportant finding\n\nMethods\nMeasured"
    )
    assert segments == [
        unformatted("Background\n"),
        {
            "text": "Important",
            "bold": True,
            "italic": False,
            "superscript": False,
            "subscript": False,
        },
        unformatted(" finding"),
        unformatted("\n\nMethods\n"),
        {
            "text": "Measured",
            "bold": False,
            "italic": True,
            "superscript": False,
            "subscript": False,
        },
    ]


def test_none_rich_value_uses_the_plain_fallback_without_warning(capsys) -> None:
    configure_logging()
    bind_run_id("20260911102613")
    plain = "A title with no formatting"

    segments = to_segments(None, plain)

    assert segments == [unformatted(plain)]
    assert flatten(segments) == plain
    assert capsys.readouterr().err == ""
    assert collected_error_events() == ()


def test_none_rich_value_with_no_plain_text_is_an_empty_segment(capsys) -> None:
    configure_logging()

    assert to_segments(None, None) == [unformatted("")]
    assert capsys.readouterr().err == ""


def test_plain_string_becomes_one_unformatted_segment() -> None:
    assert to_segments("Plain title", "Plain title") == [unformatted("Plain title")]


def test_malformed_input_falls_back_without_raising_and_logs_warning(capsys) -> None:
    configure_logging()
    bind_run_id("20260911102611")
    malformed = [Format({"bold": True}), object(), "kept text"]

    segments = to_segments(malformed, "plain fallback")

    assert segments == [unformatted("kept text")]
    log_event = json.loads(capsys.readouterr().err)
    assert log_event["event"] == "richtext_conversion_fallback"
    assert log_event["level"] == "warning"
    assert log_event["run_id"] == "20260911102611"
    assert collected_error_events() == ()


def test_unusable_rich_value_falls_back_to_the_plain_text(capsys) -> None:
    configure_logging()

    segments = to_segments([], "plain fallback")

    assert segments == [unformatted("plain fallback")]
    assert json.loads(capsys.readouterr().err)["level"] == "warning"
