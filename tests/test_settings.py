from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from publisher_lakehouse.settings import (
    DetailsSettings,
    load_environment_settings,
    load_operator_settings,
    load_settings,
)


TEMPORARY_INI = """\
[DETAILS]
output_path = D:\\fixture_output\\

[paths]
url_details = inputs/url_details.csv
raw_html_dir = data/raw_html
bronze_dir = data/bronze
export_dir = data/exports

[ingestion]
bronze_flush_every = 5
manifest_flush_every = 3

[refresh]
journal_days = 0
issue_days = 0
article_days = 90
"""


def write_ini(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "config.ini"
    path.write_text(body, encoding="utf-8")
    return path


def test_settings_import_does_not_read_config_or_create_output(tmp_path: Path) -> None:
    project_root = Path(__file__).parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(project_root), env.get("PYTHONPATH", "")) if part
    )
    code = """
import json
import sys

opened = []
def audit(event, args):
    if event == "open" and args:
        path = str(args[0]).lower()
        if path.endswith((".env", "config.ini")):
            opened.append(path)

sys.addaudithook(audit)
import publisher_lakehouse.settings
print(json.dumps(opened))
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(result.stdout) == []
    assert list(tmp_path.iterdir()) == []


def test_operator_settings_are_read_from_the_given_ini(tmp_path: Path) -> None:
    operator = load_operator_settings(write_ini(tmp_path, TEMPORARY_INI))

    assert operator.details.output_path == Path("D:\\fixture_output")
    assert operator.paths.url_details == Path("inputs/url_details.csv")
    assert operator.paths.export_dir == Path("data/exports")
    assert operator.ingestion.bronze_flush_every == 5
    assert operator.ingestion.manifest_flush_every == 3
    assert operator.refresh.journal_days == 0
    assert operator.refresh.article_days == 90


def test_details_output_path_ignores_a_trailing_separator() -> None:
    with_separator = DetailsSettings(output_path="C:\\CABIACQ_NEW1\\")
    without_separator = DetailsSettings(output_path="C:\\CABIACQ_NEW1")

    assert with_separator.output_path == without_separator.output_path


def test_environment_and_operator_settings_are_loaded_separately(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "sqlite:///environment.db")
    monkeypatch.setenv("LOG_LEVEL", "debug")
    monkeypatch.setenv("OUTPUT_PATH", "C:\\must-not-affect-operator-config")

    settings = load_settings(
        config_file=write_ini(tmp_path, TEMPORARY_INI),
        env_file=None,
    )

    assert settings.environment.database_url == "sqlite:///environment.db"
    assert settings.environment.log_level == "DEBUG"
    assert settings.operator.details.output_path == Path("D:\\fixture_output")


def test_missing_section_names_the_sections_that_are_missing(tmp_path: Path) -> None:
    body = TEMPORARY_INI.split("[ingestion]")[0]

    with pytest.raises(ValueError) as excinfo:
        load_operator_settings(write_ini(tmp_path, body))

    message = str(excinfo.value)
    assert "ingestion" in message
    assert "refresh" in message


def test_missing_config_file_raises_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_operator_settings(tmp_path / "absent.ini")


def test_unknown_config_key_is_rejected(tmp_path: Path) -> None:
    body = TEMPORARY_INI.replace(
        "article_days = 90",
        "article_days = 90\nbookseries_days = 30",
    )

    with pytest.raises(Exception) as excinfo:
        load_operator_settings(write_ini(tmp_path, body))

    assert "bookseries_days" in str(excinfo.value)


@pytest.mark.parametrize("value", ["0", "-1"])
def test_non_positive_flush_sizes_are_rejected(tmp_path: Path, value: str) -> None:
    body = TEMPORARY_INI.replace("bronze_flush_every = 5", f"bronze_flush_every = {value}")

    with pytest.raises(Exception):
        load_operator_settings(write_ini(tmp_path, body))


def test_negative_refresh_window_is_rejected(tmp_path: Path) -> None:
    body = TEMPORARY_INI.replace("article_days = 90", "article_days = -1")

    with pytest.raises(Exception):
        load_operator_settings(write_ini(tmp_path, body))


def test_environment_settings_ignore_the_dotenv_file_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:p@localhost:5432/db")
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("CAPTURE_FIXTURES", raising=False)

    environment = load_environment_settings(env_file=None)

    assert environment.log_level == "INFO"
    assert environment.capture_fixtures is False


def test_missing_database_url_raises_instead_of_defaulting_to_sqlite(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A SQLite default would let a broken PostgreSQL connection string look
    # like a working run that quietly wrote its manifest somewhere else.
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(Exception) as excinfo:
        load_environment_settings(env_file=None)

    assert "database_url" in str(excinfo.value).lower()
