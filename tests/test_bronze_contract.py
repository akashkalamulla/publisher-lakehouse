"""Bronze schema contract that runs without installing PySpark on the host."""

from __future__ import annotations

import re
import subprocess
import sys

from publisher_lakehouse.common.richtext import RichTextSegment
from publisher_lakehouse.schemas.article import BronzeArticle, Contact
from publisher_lakehouse.transform.schemas import (
    BRONZE_SOURCE_COLUMNS,
    CONTACT_DDL,
    RICH_TEXT_DDL,
    raw_html_key,
    read_ddl,
    table_ddl,
)


def test_source_columns_match_bronze_article_order() -> None:
    assert len(BRONZE_SOURCE_COLUMNS) == 39
    assert [name for name, _ in BRONZE_SOURCE_COLUMNS] == list(BronzeArticle.model_fields)


def test_nested_struct_fields_match_models() -> None:
    assert re.findall(r"(\w+):", CONTACT_DDL) == list(Contact.model_fields)
    assert re.findall(r"(\w+):", RICH_TEXT_DDL) == list(RichTextSegment.__annotations__)


def test_plain_strings_and_scraped_at_keep_the_declared_types() -> None:
    types = dict(BRONZE_SOURCE_COLUMNS)
    for name, field in BronzeArticle.model_fields.items():
        if field.annotation is str:
            assert types[name] == "STRING"
    assert types["scraped_at"] == "TIMESTAMP"
    assert "scraped_at STRING" in read_ddl()
    assert "scraped_at TIMESTAMP" in table_ddl()


def test_raw_html_key_uses_phase_one_layout() -> None:
    assert raw_html_key("sciencedirect", "2026-09-26", "abc") == (
        "landing/raw_html/sciencedirect/2026-09-26/abc.html.gz"
    )


def test_transform_imports_do_not_import_pyspark() -> None:
    code = (
        "import sys; "
        "import publisher_lakehouse.transform.schemas; "
        "import publisher_lakehouse.transform.bronze; "
        "assert 'pyspark' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
