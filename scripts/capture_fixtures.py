"""Capture the selected ScienceDirect article pages as HTML test fixtures."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import random
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Mapping, Sequence
from urllib.parse import unquote, urlparse, urlunparse

from bs4 import BeautifulSoup


REPO_ROOT = Path(__file__).resolve().parents[1]
LEGACY_PATH = REPO_ROOT / "CABIACQ.py"
GOLDEN_DIR = REPO_ROOT / "tests" / "fixtures" / "golden" / "sciencedirect"
HTML_DIR = REPO_ROOT / "tests" / "fixtures" / "sciencedirect" / "html"
ISSUE_HTML_DIR = REPO_ROOT / "tests" / "fixtures" / "sciencedirect" / "issue_html"
SELECTION_PATH = HTML_DIR / "SELECTION.json"
EXTRAS_PATH = HTML_DIR / "EXTRAS.txt"
SOURCE_PATH = HTML_DIR / "SOURCE.md"
BLOCK_RESTART_DELAY_SECONDS = 10.0
RICH_TAGS = ("sub", "sup", "i", "em")


@dataclass
class Fixture:
    fixture_name: str
    article_url: str
    pii: str
    journal_title: str
    issue_url: str
    covers: tuple[str, ...]
    source_file: str


@dataclass(frozen=True)
class CaptureResult:
    outcome: str
    byte_count: int = 0


@dataclass(frozen=True)
class IssueCapture:
    slug: str
    issue_url: str
    page_title: str
    byte_count: int


@dataclass(frozen=True)
class RichTextFinding:
    title_tags: tuple[str, ...] = ()
    abstract_tags: tuple[str, ...] = ()

    def summary(self) -> str:
        parts: list[str] = []
        if self.title_tags:
            parts.append("title: " + ", ".join(f"<{tag}>" for tag in self.title_tags))
        if self.abstract_tags:
            parts.append(
                "abstract: " + ", ".join(f"<{tag}>" for tag in self.abstract_tags)
            )
        return "; ".join(parts) if parts else "none"


class WarmupVerificationError(RuntimeError):
    """Raised when an issue-listing session cannot be proven usable."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def configure_console() -> None:
    """Keep legacy Unicode status messages from becoming fetch failures."""

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8", errors="replace", line_buffering=True)


def pii_from_url(url: str) -> str:
    parts = [unquote(part) for part in urlparse(url).path.split("/") if part]
    for index, part in enumerate(parts[:-1]):
        if part.lower() == "pii":
            return parts[index + 1]
    return ""


def is_sciencedirect_url(url: str) -> bool:
    parsed = urlparse(url)
    hostname = (parsed.hostname or "").lower()
    return (
        parsed.scheme.lower() in {"http", "https"}
        and (hostname == "sciencedirect.com" or hostname.endswith(".sciencedirect.com"))
    )


def is_warm_url(url: str) -> bool:
    if not is_sciencedirect_url(url):
        return False
    parts = [part for part in urlparse(url).path.split("/") if part]
    lowered = [part.lower() for part in parts]
    return any(
        section in lowered and lowered.index(section) + 1 < len(parts)
        for section in ("journal", "bookseries")
    )


def derive_warm_url(url: str) -> str:
    """Derive a journal issues or bookseries volumes URL when the slug is present."""

    parsed = urlparse(url)
    parts = [unquote(part) for part in parsed.path.split("/") if part]
    for section, suffix in (("journal", "issues"), ("bookseries", "volumes")):
        try:
            section_index = [part.lower() for part in parts].index(section)
            slug = parts[section_index + 1]
        except (ValueError, IndexError):
            continue
        path = f"/{section}/{slug}/{suffix}"
        return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))
    return ""


def safe_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_") or "manual_extra"


def issue_slug(issue_url: str) -> str:
    return safe_name(unquote(urlparse(issue_url).path))


def load_selection() -> list[Fixture]:
    if not SELECTION_PATH.exists():
        raise FileNotFoundError(
            f"{SELECTION_PATH.relative_to(REPO_ROOT)} does not exist; run "
            "python scripts/select_fixtures.py first"
        )
    with SELECTION_PATH.open("r", encoding="utf-8") as handle:
        document = json.load(handle)
    selected = document.get("selected") if isinstance(document, Mapping) else None
    if not isinstance(selected, list):
        raise ValueError("SELECTION.json does not contain a selected list")

    fixtures: list[Fixture] = []
    for position, record in enumerate(selected, start=1):
        if not isinstance(record, Mapping):
            raise ValueError(f"SELECTION.json item {position} is not an object")
        article_url = str(record.get("article_url") or "").strip()
        pii = str(record.get("pii") or pii_from_url(article_url)).strip()
        issue_url = str(record.get("issue_url") or "").strip()
        if not article_url or not pii or not issue_url:
            raise ValueError(
                f"SELECTION.json item {position} lacks an article URL, PII, or issue URL; "
                "regenerate it with python scripts/select_fixtures.py"
            )
        if not is_sciencedirect_url(article_url) or not is_warm_url(issue_url):
            raise ValueError(f"SELECTION.json item {position} contains a non-ScienceDirect URL")
        covers_value = record.get("covers")
        covers = (
            tuple(str(key) for key in covers_value)
            if isinstance(covers_value, list)
            else ()
        )
        fixtures.append(
            Fixture(
                fixture_name=safe_name(str(record.get("fixture_name") or "fixture")),
                article_url=article_url,
                pii=pii,
                journal_title=str(record.get("journal_title") or "").strip(),
                issue_url=issue_url,
                covers=covers,
                source_file=str(record.get("source_file") or "SELECTION.json"),
            )
        )
    return fixtures


def parse_extra_line(line: str, line_number: int) -> Fixture | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None

    before_comment, marker, comment = stripped.partition("#")
    fields = before_comment.split()
    if not fields:
        return None
    article_url = fields[0]
    if not is_sciencedirect_url(article_url):
        raise ValueError(
            f"EXTRAS.txt line {line_number} is not a ScienceDirect URL: {article_url}"
        )
    pii = pii_from_url(article_url)
    if not pii:
        raise ValueError(
            f"EXTRAS.txt line {line_number} is not a ScienceDirect PII article URL: "
            f"{article_url}"
        )
    remaining_fields = fields[1:]
    issue_url = ""
    if remaining_fields and is_sciencedirect_url(remaining_fields[0]):
        issue_url = remaining_fields.pop(0)
    if not issue_url:
        issue_url = derive_warm_url(article_url)
    if not issue_url:
        raise ValueError(
            f"EXTRAS.txt line {line_number} cannot derive a warm-up URL from "
            f"{article_url}; supply an issue or journal URL as the second URL"
        )
    if not is_warm_url(issue_url):
        raise ValueError(
            f"EXTRAS.txt line {line_number} has an invalid warm-up URL: {issue_url}; "
            "use a ScienceDirect journal or bookseries page"
        )
    requested_name = comment.strip() if marker else " ".join(remaining_fields).strip()
    fixture_name = safe_name(requested_name or "manual_extra")
    return Fixture(
        fixture_name=fixture_name,
        article_url=article_url,
        pii=pii,
        journal_title="Manual extra",
        issue_url=issue_url,
        covers=(fixture_name,),
        source_file="EXTRAS.txt",
    )


def load_extras() -> list[Fixture]:
    if not EXTRAS_PATH.exists():
        return []
    extras: list[Fixture] = []
    for line_number, line in enumerate(
        EXTRAS_PATH.read_text(encoding="utf-8").splitlines(), start=1
    ):
        fixture = parse_extra_line(line, line_number)
        if fixture is not None:
            extras.append(fixture)
    return extras


def merge_fixtures(selected: Sequence[Fixture], extras: Sequence[Fixture]) -> list[Fixture]:
    merged: list[Fixture] = []
    seen_piis: set[str] = set()
    used_names: dict[str, int] = {}
    for fixture in (*selected, *extras):
        folded_pii = fixture.pii.casefold()
        if folded_pii in seen_piis:
            print(f"Skipping duplicate PII {fixture.pii} from {fixture.source_file}")
            continue
        seen_piis.add(folded_pii)

        base_name = safe_name(fixture.fixture_name)
        used_names[base_name] = used_names.get(base_name, 0) + 1
        number = used_names[base_name]
        fixture.fixture_name = base_name if number == 1 else f"{base_name}_{number:02d}"
        merged.append(fixture)
    return merged


def import_legacy_scraper() -> ModuleType:
    print(
        "Warning: importing CABIACQ.py reads the root config.ini and may create "
        "directories under its output_path (known legacy behaviour)."
    )
    spec = importlib.util.spec_from_file_location("cabiacq_fixture_capture", LEGACY_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not create an import specification for {LEGACY_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module

    # CABIACQ.py opens config.ini relative to the process directory.  Resolve
    # that known side effect against the repository root even when this script
    # is launched elsewhere.
    previous_directory = Path.cwd()
    try:
        os.chdir(REPO_ROOT)
        spec.loader.exec_module(module)
    finally:
        os.chdir(previous_directory)

    required = (
        "start_browser",
        "setup_browser_session",
        "fetch_soup_with_page",
        "is_blocked",
        "DELAY_BETWEEN_ARTICLES",
    )
    missing = [name for name in required if not hasattr(module, name)]
    if missing:
        raise ImportError(f"CABIACQ.py is missing required names: {', '.join(missing)}")
    return module


def page_title(soup: BeautifulSoup | None) -> str:
    return soup.title.get_text(" ", strip=True) if soup is not None and soup.title else ""


def normalise_html(html: object) -> str:
    if isinstance(html, bytes):
        text = html.decode("utf-8", errors="replace")
    else:
        text = str(html)
    return text.replace("\r\n", "\n").replace("\r", "\n")


def journal_title_from_soup(soup: BeautifulSoup | None) -> str:
    if soup is None:
        return ""
    meta = soup.find("meta", attrs={"name": "citation_journal_title"})
    if meta is not None:
        return str(meta.get("content") or "").strip()
    publication_title = soup.select_one(".publication-title-link, .publication-title")
    return publication_title.get_text(" ", strip=True) if publication_title else ""


async def stop_browser(browser: object | None) -> None:
    if browser is None:
        return
    try:
        await browser.stop()
    except Exception:
        pass


async def verify_session_cleared(
    legacy: ModuleType, browser: object, issue_url: str
) -> str:
    page = getattr(browser, "main_tab", None)
    if page is None:
        raise WarmupVerificationError(
            f"Could not verify browser warm-up for issue URL {issue_url}: no active tab"
        )
    try:
        title = str(await page.evaluate("document.title") or "")
    except Exception as exc:
        raise WarmupVerificationError(
            f"Could not verify browser warm-up for issue URL {issue_url}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    if legacy.is_blocked(title):
        raise WarmupVerificationError(
            f"Browser warm-up remained blocked for issue URL {issue_url}; "
            f"title={title!r}"
        )
    return title


async def save_issue_page(
    legacy: ModuleType, browser: object, issue_url: str
) -> IssueCapture:
    page = getattr(browser, "main_tab", None)
    if page is None:
        raise WarmupVerificationError(
            f"Could not save warmed issue URL {issue_url}: no active tab"
        )
    try:
        html = await page.get_content()
    except Exception as exc:
        raise WarmupVerificationError(
            f"Could not read warmed issue URL {issue_url}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    if html is None:
        raise WarmupVerificationError(
            f"Could not read warmed issue URL {issue_url}: page returned no HTML"
        )
    html_text = normalise_html(html)
    if not html_text.strip():
        raise WarmupVerificationError(
            f"Could not read warmed issue URL {issue_url}: page returned empty HTML"
        )

    captured_title = page_title(BeautifulSoup(html_text, "lxml"))
    if legacy.is_blocked(captured_title):
        raise WarmupVerificationError(
            f"Warmed issue URL {issue_url} returned a blocked page; "
            f"title={captured_title!r}"
        )

    slug = issue_slug(issue_url)
    byte_count = len(html_text.encode("utf-8"))
    ISSUE_HTML_DIR.mkdir(parents=True, exist_ok=True)
    (ISSUE_HTML_DIR / f"{slug}.html").write_text(
        html_text, encoding="utf-8", newline="\n"
    )
    return IssueCapture(slug, issue_url, captured_title, byte_count)


async def warm_verified_session(
    legacy: ModuleType,
    browser: object,
    issue_url: str,
    issue_captures: dict[str, IssueCapture],
) -> None:
    last_error: WarmupVerificationError | None = None
    for attempt in range(1, 3):
        await legacy.setup_browser_session(browser, issue_url)
        try:
            title = await verify_session_cleared(legacy, browser, issue_url)
            issue_capture = None
            if issue_url not in issue_captures:
                issue_capture = await save_issue_page(legacy, browser, issue_url)
        except WarmupVerificationError as exc:
            last_error = exc
            if attempt == 1:
                print(
                    f"  warm-up verification failed for {issue_url}; waiting "
                    "and retrying once"
                )
                await asyncio.sleep(BLOCK_RESTART_DELAY_SECONDS)
                continue
            break
        print(f"  [setup] Verified issue session. Title: {title[:80]!r}")
        if issue_capture is not None:
            issue_captures[issue_url] = issue_capture
            print(
                f"  [issue] {issue_capture.slug}  saved  "
                f"{issue_capture.byte_count} bytes"
            )
        return
    raise WarmupVerificationError(
        f"Warm-up failed twice for issue URL {issue_url}: {last_error}"
    ) from last_error


async def restart_browser(
    legacy: ModuleType,
    browser: object,
    issue_url: str,
    issue_captures: dict[str, IssueCapture],
) -> object:
    await stop_browser(browser)
    await asyncio.sleep(BLOCK_RESTART_DELAY_SECONDS)
    replacement = await legacy.start_browser()
    try:
        await warm_verified_session(legacy, replacement, issue_url, issue_captures)
    except Exception:
        await stop_browser(replacement)
        raise
    return replacement


async def capture_one(
    legacy: ModuleType,
    browser: object,
    fixture: Fixture,
    issue_captures: dict[str, IssueCapture],
) -> tuple[object, CaptureResult]:
    target = HTML_DIR / f"{fixture.fixture_name}.html"

    for block_attempt in range(1, 5):
        try:
            soup, _page, html = await legacy.fetch_soup_with_page(
                browser, fixture.article_url, wait_id="root"
            )
        except Exception as exc:
            return browser, CaptureResult(f"failed ({type(exc).__name__}: {exc})")

        if soup is None or html is None:
            return browser, CaptureResult("failed (fetch returned no page)")

        title = page_title(soup)
        if legacy.is_blocked(title):
            if block_attempt < 4:
                print(
                    f"  block detected for {fixture.fixture_name} "
                    f"({block_attempt}/4); waiting, restarting, and re-warming "
                    f"on {fixture.issue_url}"
                )
                browser = await restart_browser(
                    legacy, browser, fixture.issue_url, issue_captures
                )
                continue
            return browser, CaptureResult("failed (blocked after 4 attempts)")

        html_text = normalise_html(html)
        target.write_text(html_text, encoding="utf-8", newline="\n")
        if fixture.journal_title == "Manual extra":
            fixture.journal_title = journal_title_from_soup(soup) or fixture.journal_title
        return browser, CaptureResult("saved", len(html_text.encode("utf-8")))

    return browser, CaptureResult("failed")


def element_tags(element: object) -> tuple[str, ...]:
    if element is None or not hasattr(element, "find"):
        return ()
    return tuple(tag for tag in RICH_TAGS if element.find(tag) is not None)


def analyse_rich_text(path: Path) -> RichTextFinding:
    soup = BeautifulSoup(path.read_text(encoding="utf-8"), "lxml")
    title_element = soup.find(class_="title-text")
    if title_element is None:
        title_element = soup.find("h1")
    abstract_element = soup.find(id="abstracts")
    return RichTextFinding(
        title_tags=element_tags(title_element),
        abstract_tags=element_tags(abstract_element),
    )


def print_rich_text_table(
    fixtures: Sequence[Fixture], findings: Mapping[str, RichTextFinding]
) -> None:
    print("\nRich-text coverage")
    headers = ("Fixture", "Title", "Abstract")
    rows = []
    for fixture in fixtures:
        finding = findings.get(fixture.fixture_name)
        title_tags = (
            ", ".join(f"<{tag}>" for tag in finding.title_tags)
            if finding
            else ""
        )
        abstract_tags = (
            ", ".join(f"<{tag}>" for tag in finding.abstract_tags)
            if finding
            else ""
        )
        rows.append(
            (
                fixture.fixture_name,
                title_tags or "-",
                abstract_tags or "-",
            )
        )
    widths = [
        max(len(headers[column]), *(len(row[column]) for row in rows))
        if rows
        else len(headers[column])
        for column in range(3)
    ]
    print("  ".join(headers[index].ljust(widths[index]) for index in range(3)))
    print("  ".join("-" * width for width in widths))
    for row in rows:
        print("  ".join(row[index].ljust(widths[index]) for index in range(3)))

    missing = [
        tag
        for tag in ("sub", "sup")
        if not any(tag in finding.title_tags for finding in findings.values())
    ]
    if missing:
        formatted = " or ".join(f"<{tag}>" for tag in missing)
        print(
            f"No fixture covers {formatted} in a title. Add a matching article "
            "to EXTRAS.txt and re-run capture_fixtures.py."
        )

    found_title_tags = [
        tag
        for tag in RICH_TAGS
        if any(tag in finding.title_tags for finding in findings.values())
    ]
    if found_title_tags:
        print(
            "Captured title rich-text tags: "
            + ", ".join(f"<{tag}>" for tag in found_title_tags)
        )
    else:
        print("No captured fixture contains <sub>, <sup>, <i>, or <em> in its title.")


def markdown_cell(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def corpus_collaboration_editor_counts() -> tuple[int, int, int]:
    total = corporate_author_articles = editor_articles = 0
    for source_path in sorted(GOLDEN_DIR.glob("*.json")):
        with source_path.open("r", encoding="utf-8") as handle:
            document = json.load(handle)
        articles = document.get("articles", []) if isinstance(document, Mapping) else []
        for article in articles:
            if not isinstance(article, Mapping):
                continue
            total += 1
            corporate_author_articles += bool(article.get("corporate_authors"))
            editor_articles += bool(article.get("editors"))
    return total, corporate_author_articles, editor_articles


def write_source(
    fixtures: Sequence[Fixture],
    results: Mapping[str, CaptureResult],
    findings: Mapping[str, RichTextFinding],
    issue_captures: Mapping[str, IssueCapture],
) -> None:
    lines = [
        "# ScienceDirect HTML fixtures",
        "",
        f"Captured {utc_now()} from the article URLs in the golden JSON files",
        "(`tests/fixtures/golden/sciencedirect/`) plus manual extras.",
        "",
        "Regenerate with:",
        "",
        "    python scripts/select_fixtures.py",
        "    python scripts/capture_fixtures.py",
        "",
        "| Fixture | PII | Journal | Covers | Rich text |",
        "|---|---|---|---|---|",
    ]
    for fixture in fixtures:
        finding = findings.get(fixture.fixture_name)
        result = results.get(fixture.fixture_name)
        rich_text = finding.summary() if finding else (result.outcome if result else "not captured")
        lines.append(
            "| "
            + " | ".join(
                markdown_cell(value)
                for value in (
                    f"{fixture.fixture_name}.html",
                    fixture.pii,
                    fixture.journal_title,
                    ", ".join(fixture.covers),
                    rich_text,
                )
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Issue pages",
            "",
            "| Slug | Issue URL | Page title | Byte size |",
            "|---|---|---|---:|",
        ]
    )
    for issue_capture in issue_captures.values():
        lines.append(
            "| "
            + " | ".join(
                markdown_cell(value)
                for value in (
                    issue_capture.slug,
                    issue_capture.issue_url,
                    issue_capture.page_title,
                    issue_capture.byte_count,
                )
            )
            + " |"
        )
    total, corporate_author_articles, editor_articles = (
        corpus_collaboration_editor_counts()
    )
    lines.extend(
        [
            "",
            "## Open question: corporate_authors and editors",
            "",
            f"Across all {total} articles in the three golden files, "
            f"`corporate_authors` is non-empty for {corporate_author_articles} articles "
            f"and `editors` is non-empty for {editor_articles} articles.",
            "",
            "Two hypotheses remain:",
            "",
            "1. These journals genuinely publish no collaboration authors and no editors.",
            "2. `_collect_collaborations` and editor extraction have been returning empty lists incorrectly.",
            "",
            "The bookseries fixture is expected to resolve the editor case because book content carries editors where journal articles do not.",
            "",
            "Gate 4 parse tests must assert whichever answer the captured fixtures establish, rather than assuming either hypothesis.",
        ]
    )
    SOURCE_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def group_by_issue(fixtures: Sequence[Fixture]) -> list[tuple[str, list[Fixture]]]:
    groups: dict[str, list[Fixture]] = {}
    for fixture in fixtures:
        groups.setdefault(fixture.issue_url, []).append(fixture)
    return list(groups.items())


async def capture_all(fixtures: Sequence[Fixture], legacy: ModuleType) -> int:
    results: dict[str, CaptureResult] = {}
    issue_captures: dict[str, IssueCapture] = {}
    abort_error: WarmupVerificationError | None = None
    completed = 0
    for issue_url, issue_fixtures in group_by_issue(fixtures):
        browser: object | None = None
        try:
            browser = await legacy.start_browser()
            await warm_verified_session(
                legacy, browser, issue_url, issue_captures
            )
            for issue_index, fixture in enumerate(issue_fixtures):
                browser, result = await capture_one(
                    legacy, browser, fixture, issue_captures
                )
                results[fixture.fixture_name] = result
                completed += 1
                print(
                    f"[{completed}/{len(fixtures)}] {fixture.fixture_name}  "
                    f"{fixture.pii}  {result.outcome}  {result.byte_count} bytes"
                )
                if issue_index + 1 < len(issue_fixtures):
                    delay = random.uniform(*legacy.DELAY_BETWEEN_ARTICLES)
                    await asyncio.sleep(delay)
        except WarmupVerificationError as exc:
            abort_error = exc
            break
        finally:
            await stop_browser(browser)

    if abort_error is not None:
        for fixture in fixtures:
            results.setdefault(
                fixture.fixture_name,
                CaptureResult(f"not attempted ({abort_error})"),
            )

    findings = {
        fixture.fixture_name: analyse_rich_text(
            HTML_DIR / f"{fixture.fixture_name}.html"
        )
        for fixture in fixtures
        if results[fixture.fixture_name].outcome == "saved"
    }
    print_rich_text_table(fixtures, findings)
    write_source(fixtures, results, findings, issue_captures)
    print(f"\nSource manifest written to {SOURCE_PATH.relative_to(REPO_ROOT)}")
    successes = sum(result.outcome == "saved" for result in results.values())
    print(f"Captured successfully: {successes}/{len(fixtures)} fixture(s).")
    if abort_error is not None:
        print(f"Capture aborted: {abort_error}")
        return 1
    failures = sum(result.outcome != "saved" for result in results.values())
    if failures:
        print(f"Capture completed with {failures} failed fixture(s).")
        return 1
    print(f"Capture completed: {len(fixtures)} fixture(s) saved.")
    return 0


def main() -> int:
    configure_console()
    HTML_DIR.mkdir(parents=True, exist_ok=True)
    fixtures = merge_fixtures(load_selection(), load_extras())
    print(f"Combined fixture count: {len(fixtures)}")
    if not fixtures:
        print("Nothing to capture.")
        write_source([], {}, {}, {})
        return 0
    legacy = import_legacy_scraper()
    return asyncio.run(capture_all(fixtures, legacy))


if __name__ == "__main__":
    raise SystemExit(main())
