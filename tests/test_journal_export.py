from __future__ import annotations

import json
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

from publisher_lakehouse.export.journal_export import (
    bundle_journal_html,
    export_journals,
    group_by_journal,
    load_latest_bronze_records,
    slugify,
    write_journal_json,
)
from publisher_lakehouse.ingestion.writers.raw_store import store_raw_html
from publisher_lakehouse.schemas.article import BronzeArticle


GOLDEN_ROOT = Path(__file__).parent / "fixtures" / "golden" / "sciencedirect"
NOW = datetime(2026, 9, 14, 12, tzinfo=timezone.utc)


def _record(
    *,
    journal_title: str = "Journal One",
    journal_url: str = "https://example.test/journal-one/issues",
    url_hash: str = "article-one",
    english_title: str = "Article one",
) -> dict[str, object]:
    return BronzeArticle(
        journal_title=journal_title,
        journal_url=journal_url,
        article_url=f"https://example.test/science/article/{url_hash}",
        english_title=english_title,
        run_id="20260914120000",
        scraped_at=NOW,
        publisher="sciencedirect",
        url_hash=url_hash,
        source_url=f"https://example.test/science/article/{url_hash}",
    ).model_dump(mode="json")


def _write_bronze_part(
    bronze_root: Path,
    *,
    ingest_date: str,
    run_id: str,
    records: list[dict[str, object]],
) -> None:
    path = (
        bronze_root
        / "sciencedirect"
        / f"ingest_date={ingest_date}"
        / f"part-{run_id}.jsonl"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records),
        encoding="utf-8",
        newline="\n",
    )


def _store_html(raw_root: Path, record: dict[str, object]) -> None:
    store_raw_html(
        f"<html><body>{record['english_title']}</body></html>",
        raw_html_dir=raw_root,
        publisher="sciencedirect",
        run_date=date(2026, 9, 14),
        url_hash=str(record["url_hash"]),
    )


def _ok_manifest(*records: dict[str, object]) -> dict[str, dict[str, str]]:
    return {str(record["url_hash"]): {"status": "ok"} for record in records}


def test_latest_wins(tmp_path: Path) -> None:
    old = _record(english_title="Older article")
    new = _record(english_title="Newer article")
    _write_bronze_part(
        tmp_path / "bronze",
        ingest_date="2026-09-13",
        run_id="20260913120000",
        records=[old],
    )
    _write_bronze_part(
        tmp_path / "bronze",
        ingest_date="2026-09-14",
        run_id="20260914120000",
        records=[new],
    )
    _store_html(tmp_path / "raw_html", new)

    exported = export_journals(
        "sciencedirect",
        bronze_root=tmp_path / "bronze",
        raw_html_root=tmp_path / "raw_html",
        out_dir=tmp_path / "out",
        manifest=_ok_manifest(new),
    )

    json_path, _ = exported[new["journal_url"]]
    assert json.loads(json_path.read_text(encoding="utf-8"))["articles"] == [
        BronzeArticle.model_validate(new).content_payload()
    ]


def test_groups_by_journal(tmp_path: Path) -> None:
    first = _record()
    second = _record(
        journal_title="Journal Two",
        journal_url="https://example.test/journal-two/issues",
        url_hash="article-two",
    )
    _write_bronze_part(
        tmp_path / "bronze",
        ingest_date="2026-09-14",
        run_id="20260914120000",
        records=[first, second],
    )
    _store_html(tmp_path / "raw_html", first)
    _store_html(tmp_path / "raw_html", second)

    latest = load_latest_bronze_records("sciencedirect", tmp_path / "bronze")
    assert set(group_by_journal(latest)) == {
        str(first["journal_url"]),
        str(second["journal_url"]),
    }
    export_journals(
        "sciencedirect",
        bronze_root=tmp_path / "bronze",
        raw_html_root=tmp_path / "raw_html",
        out_dir=tmp_path / "out",
        manifest=_ok_manifest(first, second),
    )

    assert {
        path.name for path in (tmp_path / "out").iterdir() if path.is_dir()
    } == {slugify("Journal One"), slugify("Journal Two")}
    for record in (first, second):
        slug = slugify(str(record["journal_title"]))
        document = json.loads(
            (tmp_path / "out" / slug / f"{slug}.json").read_text(encoding="utf-8")
        )
        assert [article["journal_url"] for article in document["articles"]] == [
            record["journal_url"]
        ]


def test_matches_golden_shape(tmp_path: Path) -> None:
    golden_path = next(GOLDEN_ROOT.glob("Indian_Journal_of_Tuberculosis_*.json"))
    golden = json.loads(golden_path.read_text(encoding="utf-8"))
    records: list[dict[str, object]] = []
    for article in golden["articles"]:
        article_url = str(article["article_url"])
        record = BronzeArticle(
            **article,
            run_id="20260914120000",
            scraped_at=NOW,
            publisher="sciencedirect",
            url_hash=f"golden-{len(records)}",
            source_url=article_url,
        ).model_dump(mode="json")
        records.append(record)

    _write_bronze_part(
        tmp_path / "bronze",
        ingest_date="2026-09-14",
        run_id="20260914120000",
        records=records,
    )
    latest = load_latest_bronze_records("sciencedirect", tmp_path / "bronze")
    journal_url = str(records[0]["journal_url"])
    export_path = write_journal_json(
        journal_url,
        group_by_journal(latest)[journal_url],
        tmp_path / "out",
    )

    assert export_path is not None
    assert export_path.read_bytes() == golden_path.read_bytes()


def test_html_zip_contains_all_articles(tmp_path: Path) -> None:
    records = [_record(url_hash=f"article-{index}") for index in range(3)]
    for record in records:
        _store_html(tmp_path / "raw_html", record)

    zip_path = bundle_journal_html(records, tmp_path / "raw_html", tmp_path / "out")

    assert zip_path is not None
    with zipfile.ZipFile(zip_path) as archive:
        assert len(archive.infolist()) == len(records)
        for record in records:
            member = f"{record['url_hash']}.html"
            assert archive.read(member).decode("utf-8").startswith("<html>")


def test_skips_empty_journal(tmp_path: Path) -> None:
    record = _record()
    _write_bronze_part(
        tmp_path / "bronze",
        ingest_date="2026-09-14",
        run_id="20260914120000",
        records=[record],
    )

    exported = export_journals(
        "sciencedirect",
        bronze_root=tmp_path / "bronze",
        raw_html_root=tmp_path / "raw_html",
        out_dir=tmp_path / "out",
        manifest={str(record["url_hash"]): {"status": "failed"}},
    )

    assert exported == {}
    assert not (tmp_path / "out" / slugify("Journal One")).exists()


def test_rerun_overwrites_byte_identically(tmp_path: Path) -> None:
    record = _record()
    _write_bronze_part(
        tmp_path / "bronze",
        ingest_date="2026-09-14",
        run_id="20260914120000",
        records=[record],
    )
    _store_html(tmp_path / "raw_html", record)
    arguments = {
        "bronze_root": tmp_path / "bronze",
        "raw_html_root": tmp_path / "raw_html",
        "out_dir": tmp_path / "out",
        "manifest": _ok_manifest(record),
    }

    first = export_journals("sciencedirect", **arguments)
    json_path, zip_path = first[str(record["journal_url"])]
    first_json = json_path.read_bytes()
    first_zip = zip_path.read_bytes()
    second = export_journals("sciencedirect", **arguments)

    assert second[str(record["journal_url"])] == (json_path, zip_path)
    assert json_path.read_bytes() == first_json
    assert zip_path.read_bytes() == first_zip
