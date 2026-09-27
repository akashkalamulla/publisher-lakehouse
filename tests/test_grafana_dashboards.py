"""Static contracts for the provisioned Grafana dashboards."""

from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_FILES = {
    "pl-corpus": ROOT / "docker/grafana/dashboards/corpus_overview.json",
    "pl-data-quality": ROOT / "docker/grafana/dashboards/data_quality.json",
    "pl-pipeline-health": ROOT / "docker/grafana/dashboards/pipeline_health.json",
}
DATASOURCE = {"type": "grafana-postgresql-datasource", "uid": "warehouse"}


def dashboards():
    return {
        uid: json.loads(path.read_text(encoding="utf-8"))
        for uid, path in DASHBOARD_FILES.items()
    }


def panels(items):
    for item in items:
        yield item
        yield from panels(item.get("panels", []))


def queries(dashboard):
    for variable in dashboard["templating"]["list"]:
        yield variable["query"]
    for panel in panels(dashboard["panels"]):
        for target in panel.get("targets", []):
            yield target["rawSql"]


def test_dashboard_json_uids_and_unique_panel_ids():
    for uid, dashboard in dashboards().items():
        assert dashboard["uid"] == uid
        assert dashboard["schemaVersion"] == 41
        ids = [panel["id"] for panel in panels(dashboard["panels"])]
        assert len(ids) == len(set(ids))


def test_every_panel_and_query_variable_uses_warehouse_datasource():
    for dashboard in dashboards().values():
        for variable in dashboard["templating"]["list"]:
            assert variable["datasource"] == DATASOURCE
        for panel in panels(dashboard["panels"]):
            assert panel["datasource"] == DATASOURCE
            for target in panel.get("targets", []):
                assert target["datasource"] == DATASOURCE
        assert "${DS_" not in json.dumps(dashboard)


def test_every_from_and_join_is_qualified_in_serving_or_ops():
    for dashboard in dashboards().values():
        for query in queries(dashboard):
            # EXTRACT(EPOCH FROM ...) uses FROM as SQL syntax, not as a table.
            table_sql = re.sub(r"\bEXTRACT\s*\([^)]*\)", "", query, flags=re.I)
            targets = re.findall(
                r"\b(?:FROM|JOIN)\s+([A-Za-z_][\w.]*)", table_sql, re.I
            )
            assert targets, query
            assert all(target.startswith(("serving.", "ops.")) for target in targets), query
            assert not re.search(r"\b(?:serving_stage|public)\s*\.", query, re.I)


def test_weight_and_share_rounding_casts_to_numeric():
    corpus = dashboards()["pl-corpus"]
    sql_by_title = {
        panel["title"]: panel["targets"][0]["rawSql"]
        for panel in panels(corpus["panels"])
        if panel.get("targets")
    }
    assert re.search(
        r"ROUND\(SUM\(b\.weight\)::numeric,\s*2\)",
        sql_by_title["Top authors (weighted)"],
    )
    for title in ("CC-licensed share (scholarly)", "CC-licensed share by journal (scholarly)"):
        assert re.search(r"\)::numeric,\s*2\s*\)", sql_by_title[title])


def test_every_panel_has_an_explanatory_sentence():
    for dashboard in dashboards().values():
        for panel in panels(dashboard["panels"]):
            description = panel.get("description", "")
            assert description.strip().endswith((".", "?")), panel["title"]


def test_required_dashboard_shapes_and_variables():
    corpus, quality, health = (
        dashboards()[uid]
        for uid in ("pl-corpus", "pl-data-quality", "pl-pipeline-health")
    )
    assert len(list(panels(corpus["panels"]))) == 11
    assert len(list(panels(quality["panels"]))) == 5
    assert len(list(panels(health["panels"]))) == 11
    row = next(panel for panel in corpus["panels"] if panel["type"] == "row")
    assert row["title"] == "Classification review" and row["collapsed"]
    assert [v["name"] for v in corpus["templating"]["list"]] == [
        "publisher", "content_class",
    ]
    content_class = corpus["templating"]["list"][1]
    assert content_class["multi"] and content_class["includeAll"]
    assert quality["time"] == {"from": "now-7d", "to": "now"}


def test_scraper_health_row_sits_below_the_publish_panels_and_reads_scrape_tables():
    health = dashboards()["pl-pipeline-health"]["panels"]
    start = next(i for i, panel in enumerate(health) if panel["type"] == "row")
    row, existing, scraper = health[start], health[:start], health[start + 1:]
    assert row["title"] == "Scraper health" and not row["collapsed"]
    assert [panel["title"] for panel in existing] == [
        "Lake tables", "Last successful publish", "Last publish status",
        "Publish history", "Rows per lake table",
    ]
    assert [(panel["type"], panel["title"]) for panel in scraper] == [
        ("stat", "Last scrape finished"),
        ("stat", "Last run status"),
        ("table", "Last 10 runs"),
        ("barchart", "Manifest rows by type and status"),
        ("table", "Journals needing attention"),
    ]
    row_y = row["gridPos"]["y"]
    assert all(p["gridPos"]["y"] + p["gridPos"]["h"] <= row_y for p in existing)
    assert all(p["gridPos"]["y"] > row_y for p in scraper)

    sql = {panel["title"]: panel["targets"][0]["rawSql"] for panel in scraper}
    for query in sql.values():
        assert re.search(r"\bFROM ops\.scrape_(?:runs|status|journals)\b", query), query
        assert len(re.findall(r"\bROUND\(", query)) == len(re.findall(r"::numeric,", query))
    assert scraper[0]["fieldConfig"]["defaults"]["unit"] == "dateTimeFromNow"
    assert "MAX(finished_at)" in sql["Last scrape finished"]
    assert re.search(r"SELECT status,\s+ingest_exit_code\b", sql["Last run status"])
    assert "LIMIT 10" in sql["Last 10 runs"]
    for counter in (
        "articles_discovered", "fetched", "bronze_written", "skipped_manifest",
        "fetch_failed", "blocked_gave_up", "parse_failed",
    ):
        assert counter in sql["Last 10 runs"]
    attention = sql["Journals needing attention"]
    assert "WHERE status <> 'ok'\n   OR last_seen_at < now() - interval '7 days'" in attention
    assert "ORDER BY last_seen_at ASC" in attention


def test_provisioning_is_read_only_and_credentials_are_interpolated():
    provider = (ROOT / "docker/grafana/provisioning/dashboards/lakehouse.yaml").read_text()
    source = (ROOT / "docker/grafana/provisioning/datasources/warehouse.yaml").read_text()
    assert re.search(r"(?m)^\s*allowUiUpdates:\s*false\s*$", provider)
    assert re.search(r"(?m)^\s*disableDeletion:\s*true\s*$", provider)
    assert re.search(r"(?m)^\s*editable:\s*false\s*$", source)
    assert re.search(r"(?m)^\s*user:\s*grafana_ro\s*$", source)
    assert re.search(r"(?m)^\s*type:\s*grafana-postgresql-datasource\s*$", source)
    assert re.search(r"(?m)^\s*password:\s*\$\{GRAFANA_DB_PASSWORD\}\s*$", source)
    assert "${WAREHOUSE_DB}" in source
    for path in DASHBOARD_FILES.values():
        assert "password" not in path.read_text(encoding="utf-8").lower()


def test_compose_binds_grafana_to_loopback_only():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    grafana = compose.split("  grafana:\n", 1)[1].split("\n  warehouse:", 1)[0]
    assert '"127.0.0.1:3000:3000"' in grafana
    assert "grafana/grafana:13.2.2@sha256:" in grafana
    assert "GF_AUTH_ANONYMOUS_ENABLED: \"false\"" in grafana
    assert "GF_USERS_ALLOW_SIGN_UP: \"false\"" in grafana
