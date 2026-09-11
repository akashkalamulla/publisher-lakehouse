"""Convert XlsxWriter rich strings into neutral JSON segments."""

from __future__ import annotations

from typing import Any, TypedDict

from xlsxwriter.format import Format

from publisher_lakehouse.common.logging import get_logger


logger = get_logger(__name__)


class RichTextSegment(TypedDict):
    text: str
    bold: bool
    italic: bool
    superscript: bool
    subscript: bool


def _json_text(value: Any) -> str:
    """Flatten values exactly as the legacy per-journal JSON writer does."""

    if value is None:
        return ""
    if isinstance(value, list):
        return "".join(fragment for fragment in value if isinstance(fragment, str))
    return str(value)


def _segment(text: str, fmt: Format | None = None) -> RichTextSegment:
    font_script = getattr(fmt, "font_script", 0) if fmt is not None else 0
    return {
        "text": text,
        "bold": bool(getattr(fmt, "bold", False)) if fmt is not None else False,
        "italic": bool(getattr(fmt, "italic", False)) if fmt is not None else False,
        "superscript": font_script == 1,
        "subscript": font_script == 2,
    }


def _fallback(rich: Any, reason: str) -> list[RichTextSegment]:
    try:
        text = _json_text(rich)
    except Exception:
        text = ""
    try:
        logger.warning("richtext_conversion_fallback", reason=reason)
    except Exception:
        # Conversion is deliberately total; logging must not turn malformed
        # source data into an ingestion failure.
        pass
    return [_segment(text)]


def to_segments(rich: Any) -> list[RichTextSegment]:
    """Convert a legacy XlsxWriter rich list without changing its flat text.

    Besides regular ``Format, str`` pairs, the legacy abstract builder can
    place unformatted heading or separator strings between pairs.  Those are
    retained as unformatted segments.
    """

    if isinstance(rich, str):
        return [_segment(rich)]
    if not isinstance(rich, list) or not rich:
        return _fallback(rich, "expected a non-empty list or string")

    try:
        segments: list[RichTextSegment] = []
        saw_format = False
        index = 0
        while index < len(rich):
            value = rich[index]
            if isinstance(value, str):
                segments.append(_segment(value))
                index += 1
                continue
            if isinstance(value, Format):
                saw_format = True
                if index + 1 >= len(rich) or not isinstance(rich[index + 1], str):
                    return _fallback(rich, "format is not followed by text")
                segments.append(_segment(rich[index + 1], value))
                index += 2
                continue
            return _fallback(rich, f"unexpected item at index {index}")

        if not saw_format:
            return _fallback(rich, "list contains no format objects")
        return segments
    except Exception as exc:
        return _fallback(rich, f"converter raised {type(exc).__name__}")


def flatten(segments: list[RichTextSegment]) -> str:
    """Flatten neutral segments with the legacy string-fragment semantics."""

    return "".join(segment["text"] for segment in segments)
