"""Silver schema and rule contracts without a PySpark dependency."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from calendar import month_abbr, month_name
from pathlib import Path

from publisher_lakehouse.transform.schemas import (
    CC_LICENSE_PATTERN,
    DQ_RULES,
    MONTH_LOOKUP,
    QUARANTINE_COLUMNS,
    QUARANTINE_KEY,
    SILVER_COLUMNS,
    SILVER_KEY,
    SILVER_PARTITIONS,
)


def test_silver_columns_are_unique_and_include_keys_and_partitions() -> None:
    names = [name for name, _ in SILVER_COLUMNS]
    assert len(names) == len(set(names))
    assert set(SILVER_KEY + SILVER_PARTITIONS) <= set(names)
    assert set(QUARANTINE_KEY) <= {name for name, _ in QUARANTINE_COLUMNS}


def test_dq_rule_names_and_severities_are_valid() -> None:
    names = [name for name, _ in DQ_RULES]
    assert len(names) == len(set(names)) == 12
    assert {severity for _, severity in DQ_RULES} == {"quarantine", "flag"}


def test_month_lookup_covers_all_full_names_and_abbreviations() -> None:
    for number in range(1, 13):
        assert MONTH_LOOKUP[month_name[number].lower()] == number
        assert MONTH_LOOKUP[month_abbr[number].lower()] == number


def test_cc_pattern_matches_golden_license_urls() -> None:
    directory = Path(__file__).parent / "fixtures" / "golden" / "sciencedirect"
    urls = [
        article["license_type"]
        for path in directory.glob("*.json")
        for article in json.loads(path.read_text(encoding="utf-8"))["articles"]
        if article["license_type"].startswith("http")
    ]
    assert urls
    assert all(re.fullmatch(CC_LICENSE_PATTERN, url) for url in urls)
    match = re.fullmatch(
        CC_LICENSE_PATTERN, "http://creativecommons.org/licenses/by-nc-nd/4.0/"
    )
    assert match and match.groups() == ("by-nc-nd", "4.0")


def test_silver_modules_import_without_pyspark() -> None:
    code = (
        "import sys; "
        "import publisher_lakehouse.transform.silver; "
        "import publisher_lakehouse.transform.delta_ops; "
        "assert 'pyspark' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
