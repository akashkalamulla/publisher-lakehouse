"""Select a compact, high-coverage set of ScienceDirect HTML fixtures.

This script is deliberately offline.  It reads the frozen golden JSON files and
writes a capture plan for ``capture_fixtures.py``; it never opens a browser or
makes a network request.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import unquote, urlparse


REPO_ROOT = Path(__file__).resolve().parents[1]
GOLDEN_DIR = REPO_ROOT / "tests" / "fixtures" / "golden" / "sciencedirect"
HTML_DIR = REPO_ROOT / "tests" / "fixtures" / "sciencedirect" / "html"
SELECTION_PATH = HTML_DIR / "SELECTION.json"
EXTRAS_PATH = HTML_DIR / "EXTRAS.txt"
TARGET_FIXTURE_COUNT = 8

EDGE_KEYS = (
    "article_number_no_pages",
    "empty_abstract",
    "collaboration_author",
    "foreign_title",
    "foreign_abstract",
    "no_doi",
    "many_authors",
    "missing_affiliation",
    "missing_email",
    "editor_present",
)
ORDINARY_KEY = "ordinary"
BOOKSERIES_KEY = "bookseries_path_shape"
REQUIRED_KEYS = EDGE_KEYS + (ORDINARY_KEY, BOOKSERIES_KEY)

EXTRAS_TEMPLATE = """\
# One article URL per line. Optionally follow it with the issue or journal URL
# used to warm the browser. Add a fixture name in a trailing # comment.
# Format: ARTICLE_URL [ISSUE_OR_JOURNAL_URL] [# fixture_name]
# If the article URL contains /journal/<slug>/ or /bookseries/<slug>/, the warm
# URL can be omitted and will be derived as the journal issues/bookseries volumes page.
# Canonical /science/article/pii/... URLs do not contain that slug, so they need
# the second URL.
#
# Still needed:
#   foreign_title / foreign_abstract:
#     https://www.sciencedirect.com/journal/revue-veterinaire-clinique/issues
#     https://www.sciencedirect.com/journal/cahiers-de-nutrition-et-de-dietetique/issues
#   bookseries path shape:
#     https://www.sciencedirect.com/bookseries/advances-in-parasitology/volumes
"""


@dataclass(frozen=True)
class Candidate:
    article_url: str
    pii: str
    journal_title: str
    issue_url: str
    article_type: str
    covers: tuple[str, ...]
    source_file: str


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp with an explicit ``Z`` suffix."""

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def value_is_present(value: object) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    return bool(value)


def pii_from_url(url: str) -> str:
    """Extract the PII path component from a ScienceDirect article URL."""

    parts = [unquote(part) for part in urlparse(url).path.split("/") if part]
    for index, part in enumerate(parts[:-1]):
        if part.lower() == "pii":
            return parts[index + 1]
    return ""


def detect_coverage(article: Mapping[str, Any]) -> tuple[str, ...]:
    """Apply the fixture catalogue rules to one flat golden article object."""

    authors_value = article.get("authors")
    authors = authors_value if isinstance(authors_value, list) else []
    corporate_value = article.get("corporate_authors")
    corporate_authors = corporate_value if isinstance(corporate_value, list) else []
    editors_value = article.get("editors")
    editors = editors_value if isinstance(editors_value, list) else []

    matches: dict[str, bool] = {
        "article_number_no_pages": (
            not value_is_present(article.get("start_page"))
            and value_is_present(article.get("article_id"))
        ),
        "empty_abstract": not value_is_present(article.get("english_abstract")),
        "collaboration_author": len(corporate_authors) >= 1,
        "foreign_title": value_is_present(article.get("foreign_title")),
        "foreign_abstract": value_is_present(article.get("non_english_abstract")),
        "no_doi": not value_is_present(article.get("doi")),
        "many_authors": len(authors) >= 8,
        "missing_affiliation": any(
            isinstance(author, Mapping)
            and not value_is_present(author.get("affiliation"))
            for author in authors
        ),
        "missing_email": any(
            isinstance(author, Mapping) and not value_is_present(author.get("email"))
            for author in authors
        ),
        "editor_present": len(editors) >= 1,
    }

    covers = [key for key in EDGE_KEYS if matches[key]]
    if (
        value_is_present(article.get("english_abstract"))
        and 2 <= len(authors) <= 5
        and value_is_present(article.get("start_page"))
        and value_is_present(article.get("doi"))
    ):
        covers.append(ORDINARY_KEY)

    journal_url = str(article.get("journal_url") or "")
    issue_url = str(article.get("issue_url") or "")
    if "/bookseries/" in journal_url.lower() or "/bookseries/" in issue_url.lower():
        covers.append(BOOKSERIES_KEY)

    return tuple(covers)


def load_candidates() -> tuple[list[str], list[Candidate]]:
    source_paths = sorted(GOLDEN_DIR.glob("*.json"), key=lambda path: path.name)
    if not source_paths:
        raise FileNotFoundError(f"No golden JSON files found in {GOLDEN_DIR}")

    candidates: list[Candidate] = []
    seen_piis: set[str] = set()
    for source_path in source_paths:
        with source_path.open("r", encoding="utf-8") as handle:
            document = json.load(handle)
        articles = document.get("articles") if isinstance(document, Mapping) else None
        if not isinstance(articles, list):
            raise ValueError(f"{source_path.name} does not contain an articles list")

        for article_number, article in enumerate(articles, start=1):
            if not isinstance(article, Mapping):
                raise ValueError(
                    f"{source_path.name} article {article_number} is not an object"
                )
            article_url = str(article.get("article_url") or "").strip()
            pii = pii_from_url(article_url)
            if not pii:
                print(
                    f"Warning: skipping article without a PII URL in "
                    f"{source_path.name} at position {article_number}",
                    file=sys.stderr,
                )
                continue
            if pii.casefold() in seen_piis:
                continue
            seen_piis.add(pii.casefold())
            candidates.append(
                Candidate(
                    article_url=article_url,
                    pii=pii,
                    journal_title=str(article.get("journal_title") or "").strip(),
                    issue_url=str(article.get("issue_url") or "").strip(),
                    article_type=str(article.get("article_type") or "").strip(),
                    covers=detect_coverage(article),
                    source_file=source_path.name,
                )
            )

    return [path.name for path in source_paths], candidates


def choose_covering_set(candidates: Sequence[Candidate]) -> list[int]:
    """Greedily cover every catalogue key represented in the corpus."""

    uncovered = {
        key
        for key in REQUIRED_KEYS
        if any(key in candidate.covers for candidate in candidates)
    }
    remaining = set(range(len(candidates)))
    selected: list[int] = []
    while uncovered:
        best_index = max(
            remaining,
            key=lambda index: (
                len(uncovered.intersection(candidates[index].covers)),
                len(candidates[index].covers),
                -index,
            ),
        )
        newly_covered = uncovered.intersection(candidates[best_index].covers)
        if not newly_covered:
            break
        selected.append(best_index)
        remaining.remove(best_index)
        uncovered.difference_update(newly_covered)
    return selected


def choose_candidates(candidates: Sequence[Candidate]) -> list[Candidate]:
    selected_indices = choose_covering_set(candidates)
    selected_index_set = set(selected_indices)

    target = min(TARGET_FIXTURE_COUNT, len(candidates))
    while len(selected_indices) < target:
        represented_journals = {
            candidates[index].journal_title for index in selected_indices
        }
        represented_types = {
            candidates[index].article_type
            for index in selected_indices
            if candidates[index].article_type
        }
        remaining = [
            index for index in range(len(candidates)) if index not in selected_index_set
        ]
        best_index = max(
            remaining,
            key=lambda index: (
                bool(candidates[index].journal_title)
                and candidates[index].journal_title not in represented_journals,
                bool(candidates[index].article_type)
                and candidates[index].article_type not in represented_types,
                -index,
            ),
        )
        selected_indices.append(best_index)
        selected_index_set.add(best_index)

    return [candidates[index] for index in selected_indices]


def safe_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_") or "fixture"


def serialise_selection(selected: Iterable[Candidate]) -> list[dict[str, object]]:
    collisions: dict[str, int] = {}
    records: list[dict[str, object]] = []
    for candidate in selected:
        base_name = safe_name(candidate.covers[0] if candidate.covers else "fixture")
        collisions[base_name] = collisions.get(base_name, 0) + 1
        collision_number = collisions[base_name]
        fixture_name = base_name if collision_number == 1 else f"{base_name}_{collision_number:02d}"
        records.append(
            {
                "fixture_name": fixture_name,
                "article_url": candidate.article_url,
                "pii": candidate.pii,
                "journal_title": candidate.journal_title,
                "issue_url": candidate.issue_url,
                "covers": list(candidate.covers),
                "source_file": candidate.source_file,
            }
        )
    return records


def print_table(records: Sequence[Mapping[str, object]]) -> None:
    rows = [
        (
            str(record["fixture_name"]),
            str(record["pii"]),
            ", ".join(str(key) for key in record["covers"]),
        )
        for record in records
    ]
    headers = ("Fixture", "PII", "Keys covered")
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


def corpus_key_counts(candidates: Sequence[Candidate]) -> dict[str, int]:
    return {
        key: sum(key in candidate.covers for candidate in candidates)
        for key in REQUIRED_KEYS
    }


def print_corpus_counts(counts: Mapping[str, int], article_count: int) -> None:
    print(f"Corpus coverage ({article_count} articles)")
    width = max(len("Key"), *(len(key) for key in counts))
    print(f"{'Key'.ljust(width)}  Count")
    print(f"{'-' * width}  -----")
    for key, count in counts.items():
        print(f"{key.ljust(width)}  {count}")
    print()


def main() -> int:
    source_files, candidates = load_candidates()
    records = serialise_selection(choose_candidates(candidates))
    counts = corpus_key_counts(candidates)
    uncovered = [key for key, count in counts.items() if count == 0]

    HTML_DIR.mkdir(parents=True, exist_ok=True)
    selection = {
        "generated_at": utc_now(),
        "source_files": source_files,
        "selected": records,
        "uncovered_keys": uncovered,
        "manual_extras_needed": bool(uncovered),
    }
    with SELECTION_PATH.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(selection, handle, indent=2, ensure_ascii=False)
        handle.write("\n")

    if not EXTRAS_PATH.exists():
        EXTRAS_PATH.write_text(EXTRAS_TEMPLATE, encoding="utf-8", newline="\n")

    print_corpus_counts(counts, len(candidates))
    print_table(records)
    print(f"\nSelected {len(records)} fixture candidate(s).")
    print(f"Selection written to {SELECTION_PATH.relative_to(REPO_ROOT)}")
    if uncovered:
        print(f"Uncovered keys: {', '.join(uncovered)}")
        print(
            "Add article URLs for these cases to "
            "tests/fixtures/sciencedirect/html/EXTRAS.txt, one article per line. "
            "For canonical PII URLs, add the issue or journal warm-up URL next, "
            "then optionally append '# fixture_name'."
        )
    else:
        print("Uncovered keys: none")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
