from __future__ import annotations

import json
from pathlib import Path

import pytest

from publisher_lakehouse.common.logging import configure_logging
from publisher_lakehouse.inputs.loader import (
    UrlTask,
    load_tasks,
    load_tasks_with_stats,
)


PROJECT_ROOT = Path(__file__).parents[1]
LEGACY_URL_FILE = PROJECT_ROOT / "urlDetails.txt"
MIGRATED_CSV = PROJECT_ROOT / "inputs" / "url_details.csv"

JOURNAL = "https://www.sciencedirect.com/journal/harmful-algae/issues"
BOOKSERIES = "https://www.sciencedirect.com/bookseries/advances-in-parasitology/volumes"


def write_csv(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "url_details.csv"
    path.write_text("publisher,url,enabled,note\n" + body, encoding="utf-8")
    return path


def test_rows_become_normalised_tasks(tmp_path: Path) -> None:
    path = write_csv(
        tmp_path,
        f"sciencedirect,{JOURNAL},true,\n"
        f"sciencedirect,{BOOKSERIES},true,bookseries\n",
    )

    tasks = load_tasks("sciencedirect", path)

    assert [task.normalised_url for task in tasks] == [JOURNAL, BOOKSERIES]
    assert all(isinstance(task, UrlTask) for task in tasks)
    assert all(task.publisher == "sciencedirect" for task in tasks)
    assert all(task.url_type == "journal" for task in tasks)
    assert tasks[1].note == "bookseries"


def test_bookseries_row_survives_and_keeps_its_note(tmp_path: Path) -> None:
    path = write_csv(tmp_path, f"sciencedirect,{BOOKSERIES},true,bookseries\n")

    tasks = load_tasks("sciencedirect", path)

    assert len(tasks) == 1
    assert tasks[0].normalised_url == BOOKSERIES
    assert tasks[0].note == "bookseries"
    assert tasks[0].url_type == "journal"


def test_raw_url_is_preserved_while_the_identity_is_normalised(tmp_path: Path) -> None:
    raw = "http://ScienceDirect.com/journal/harmful-algae/issues/#all-issues"
    path = write_csv(tmp_path, f"sciencedirect,{raw},true,\n")

    task = load_tasks("sciencedirect", path)[0]

    assert task.raw_url == raw
    assert task.normalised_url == JOURNAL


def test_disabled_rows_are_dropped(tmp_path: Path) -> None:
    path = write_csv(
        tmp_path,
        f"sciencedirect,{JOURNAL},false,paused\n"
        f"sciencedirect,{BOOKSERIES},true,bookseries\n",
    )

    loaded = load_tasks_with_stats("sciencedirect", path)

    assert [task.normalised_url for task in loaded.tasks] == [BOOKSERIES]
    assert loaded.disabled == 1
    assert loaded.input_total == 2


@pytest.mark.parametrize("flag", ["TRUE", "True", "1", "yes", " true "])
def test_enabled_flag_accepts_common_spellings(tmp_path: Path, flag: str) -> None:
    path = write_csv(tmp_path, f"sciencedirect,{JOURNAL},{flag},\n")
    assert len(load_tasks("sciencedirect", path)) == 1


def test_unrecognised_enabled_flag_raises(tmp_path: Path) -> None:
    path = write_csv(tmp_path, f"sciencedirect,{JOURNAL},maybe,\n")

    with pytest.raises(ValueError) as excinfo:
        load_tasks("sciencedirect", path)

    assert "maybe" in str(excinfo.value)


def test_other_publishers_are_ignored(tmp_path: Path) -> None:
    path = write_csv(
        tmp_path,
        f"sciencedirect,{JOURNAL},true,\n"
        "springer,https://link.springer.com/journal/10123/volumes-and-issues,true,\n",
    )

    loaded = load_tasks_with_stats("sciencedirect", path)

    assert len(loaded.tasks) == 1
    assert loaded.input_total == 1


def test_gate_zero_collapses_urls_that_normalise_to_one_identity(
    tmp_path: Path,
    capsys,
) -> None:
    configure_logging()
    variants = [
        JOURNAL,
        f"{JOURNAL}/",
        "https://sciencedirect.com/journal/harmful-algae/issues",
        f"{JOURNAL}#all-issues",
    ]
    path = write_csv(
        tmp_path,
        "".join(f"sciencedirect,{url},true,\n" for url in variants),
    )

    loaded = load_tasks_with_stats("sciencedirect", path)

    assert len(loaded.tasks) == 1
    assert loaded.input_total == 4
    assert loaded.dup_in_file == 3

    drops = [
        json.loads(line)
        for line in capsys.readouterr().err.splitlines()
        if "input_url_duplicate_dropped" in line
    ]
    assert len(drops) == 3
    # Both sides of every collision are logged, or the operator cannot tell
    # which two lines of their file collided.
    assert [drop["dropped_raw_url"] for drop in drops] == variants[1:]
    assert {drop["kept_raw_url"] for drop in drops} == {JOURNAL}


def test_missing_column_is_reported(tmp_path: Path) -> None:
    path = tmp_path / "url_details.csv"
    path.write_text(f"publisher,url,enabled\nsciencedirect,{JOURNAL},true\n", encoding="utf-8")

    with pytest.raises(ValueError) as excinfo:
        load_tasks("sciencedirect", path)

    assert "note" in str(excinfo.value)


def test_missing_file_is_reported(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_tasks("sciencedirect", tmp_path / "absent.csv")


def test_unclassifiable_url_raises_rather_than_guessing_a_tier(tmp_path: Path) -> None:
    path = write_csv(tmp_path, "sciencedirect,https://www.sciencedirect.com/,true,\n")

    with pytest.raises(ValueError):
        load_tasks("sciencedirect", path)


def test_migrated_file_round_trips_every_legacy_url() -> None:
    legacy = [
        line.strip()
        for line in LEGACY_URL_FILE.read_text(encoding="utf-8-sig").splitlines()
        if line.strip()
    ]
    loaded = load_tasks_with_stats("sciencedirect", MIGRATED_CSV)

    assert len(legacy) == 85
    assert loaded.input_total == 85
    assert loaded.dup_in_file == 0
    assert len(loaded.tasks) == 85
    assert [task.raw_url for task in loaded.tasks] == legacy
    assert sum(1 for task in loaded.tasks if task.note == "bookseries") == 1
