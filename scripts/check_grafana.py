"""Verify provisioned Grafana dashboards through the live Grafana API.

Every panel query runs exactly as the dashboard defines it.  The data checks
assert relationships between panels, so they hold as the corpus grows.  Pass
``--baseline FILE`` to also pin exact values for a known dataset; the file is
a JSON object whose keys are a subset of the "observed values" line.
"""

from __future__ import annotations

import argparse
import base64
import json
import math
import secrets
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


ROOT = Path(__file__).resolve().parents[1]
DASHBOARDS = {
    "pl-corpus": ROOT / "docker/grafana/dashboards/corpus_overview.json",
    "pl-data-quality": ROOT / "docker/grafana/dashboards/data_quality.json",
    "pl-pipeline-health": ROOT / "docker/grafana/dashboards/pipeline_health.json",
}
DATASOURCE = {"type": "grafana-postgresql-datasource", "uid": "warehouse"}
PUBLISHER = "sciencedirect"
LAKE_TABLES = 11
CC_SHARE_TOLERANCE = 0.005

# Per-journal CC and scholarly counts behind "CC-licensed share by journal":
# the panel's FROM and WHERE clauses, returning counts instead of the share.
CC_COUNTS_SQL = """SELECT jc.journal_title AS journal,
       COUNT(*) FILTER (WHERE f.has_cc_license) AS cc_count,
       COUNT(*) AS scholarly
FROM serving.fact_article f
JOIN serving.dim_journal j ON j.journal_sk = f.journal_sk
JOIN serving.dim_journal jc ON jc.publisher = j.publisher
                           AND jc.journal_url = j.journal_url
                           AND jc.is_current
JOIN serving.dim_article_type at ON at.article_type_sk = f.article_type_sk
WHERE f.publisher IN (${publisher:sqlstring})
  AND at.is_scholarly
  AND at.article_type_sk <> -1
  AND f.journal_sk <> -1
GROUP BY jc.journal_title"""


class GrafanaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ROOT / ".env", extra="ignore")

    grafana_admin_user: str
    grafana_admin_password: SecretStr


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


class GrafanaAPI:
    def __init__(self, url: str, settings: GrafanaSettings) -> None:
        self.url = url.rstrip("/")
        credentials = (
            f"{settings.grafana_admin_user}:"
            f"{settings.grafana_admin_password.get_secret_value()}"
        )
        self.authorization = "Basic " + base64.b64encode(
            credentials.encode("utf-8")
        ).decode("ascii")
        self.opener = build_opener(NoRedirect)

    def request(
        self, method: str, path: str, payload: dict | None = None,
        *, authenticate: bool = True,
    ) -> tuple[int, dict | list | str]:
        headers = {"Accept": "application/json"}
        if authenticate:
            headers["Authorization"] = self.authorization
        data = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")
        request = Request(
            self.url + path, data=data, headers=headers, method=method
        )
        try:
            response = self.opener.open(request, timeout=30)
        except HTTPError as exc:
            response = exc
        except URLError as exc:
            raise RuntimeError(f"Grafana is unreachable: {exc.reason}") from exc
        with response:
            body = response.read().decode("utf-8", errors="replace")
            try:
                decoded = json.loads(body)
            except json.JSONDecodeError:
                decoded = body
            return response.status, decoded

    def get(self, path: str) -> dict | list:
        status, body = self.request("GET", path)
        if status != 200 or not isinstance(body, (dict, list)):
            raise RuntimeError(f"GET {path} returned HTTP {status}")
        return body

    def query(
        self, sql: str, *, format: str = "table", start_ms: int, end_ms: int
    ) -> list[dict]:
        status, body = self.request(
            "POST", "/api/ds/query",
            {
                "queries": [{
                    "refId": "A", "datasource": DATASOURCE,
                    "rawSql": sql, "format": format,
                    "intervalMs": 60000, "maxDataPoints": 1000,
                }],
                "from": str(start_ms), "to": str(end_ms),
            },
        )
        if status != 200 or not isinstance(body, dict):
            raise RuntimeError(f"data-source query returned HTTP {status}")
        result = body.get("results", {}).get("A", {})
        if result.get("error"):
            raise RuntimeError(str(result["error"]))
        if result.get("status", 200) != 200:
            raise RuntimeError(f"data-source query status {result['status']}")
        rows: list[dict] = []
        for frame in result.get("frames", []):
            fields = [field["name"] for field in frame["schema"]["fields"]]
            values = frame.get("data", {}).get("values", [])
            rows.extend(dict(zip(fields, record)) for record in zip(*values))
        return rows


def panels(items: list[dict]):
    for item in items:
        if item.get("targets"):
            yield item
        yield from panels(item.get("panels", []))


def quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def substitute(sql: str, classes: list[str]) -> str:
    return (
        sql.replace("${publisher:sqlstring}", quote("sciencedirect"))
        .replace("${content_class:sqlstring}", ",".join(map(quote, classes)))
    )


def only_value(rows: list[dict], field: str):
    if len(rows) != 1:
        raise AssertionError(f"expected one row for {field}, got {len(rows)}")
    return rows[0][field]


def row_count(rows: list[dict]) -> str:
    return f"{len(rows)} {'row' if len(rows) == 1 else 'rows'}"


def manifest_counts(publisher: str) -> dict[tuple[str, str], int]:
    """Read ``{(url_type, status): rows}`` from the manifest, read-only."""

    from publisher_lakehouse.manifest.repository import ManifestRepository
    from publisher_lakehouse.ops.scrape_health import read_only_manifest_engine
    from publisher_lakehouse.settings import load_environment_settings

    engine = read_only_manifest_engine(load_environment_settings().database_url)
    try:
        return ManifestRepository(engine).counts_by_status(publisher)
    finally:
        engine.dispose()


def number(value) -> float:
    if value is None:
        raise AssertionError("value is NULL")
    return float(value)


def load_baseline(path: Path) -> dict:
    baseline = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(baseline, dict) or not baseline:
        raise ValueError(f"{path} must hold a non-empty JSON object")
    return baseline


def same_value(expected, observed) -> bool:
    if isinstance(expected, dict) and isinstance(observed, dict):
        return expected.keys() == observed.keys() and all(
            same_value(expected[key], observed[key]) for key in expected
        )
    if isinstance(expected, bool) or isinstance(observed, bool):
        return expected is observed
    if isinstance(expected, (int, float)) and isinstance(observed, (int, float)):
        return math.isclose(expected, observed, rel_tol=0, abs_tol=1e-9)
    return expected == observed


def baseline_mismatches(expected: dict, observed: dict) -> list[str]:
    problems = []
    for key, value in sorted(expected.items()):
        if key not in observed:
            problems.append(f"{key}: not an observed value")
        elif not same_value(value, observed[key]):
            problems.append(f"{key}: expected {value!r}, observed {observed[key]!r}")
    return problems


def main() -> int:
    # Redirected output on Windows is cp1252; journal titles are not.
    sys.stdout.reconfigure(errors="backslashreplace")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:3000")
    parser.add_argument(
        "--baseline", type=Path, metavar="FILE",
        help="JSON file of exact expected values; without it only invariants run",
    )
    args = parser.parse_args()
    try:
        api = GrafanaAPI(args.url, GrafanaSettings())
    except Exception:
        print("FAIL admin settings: required Grafana credentials are unavailable")
        return 1

    failures = 0

    def check(name: str, action):
        nonlocal failures
        try:
            detail = action()
            print(f"OK {name}" + (f": {detail}" if detail is not None else ""))
            return detail
        except Exception as exc:
            failures += 1
            print(f"FAIL {name}: {exc}")
            return None

    now = datetime.now(timezone.utc)
    start_ms = int((now - timedelta(days=7)).timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)

    def health():
        response = api.get("/api/health")
        assert response.get("database") == "ok", "Grafana database is not ok"
        return "database ok"

    check("Grafana health", health)

    def datasource_health():
        response = api.get("/api/datasources/uid/warehouse/health")
        assert response.get("status", "").lower() == "ok", response.get("message", "unhealthy")
        return response.get("message", "OK")

    check("Warehouse data source health", datasource_health)

    def current_user():
        rows = api.query("SELECT current_user", start_ms=start_ms, end_ms=end_ms)
        assert only_value(rows, "current_user") == "grafana_ro", "unexpected database role"
        return "grafana_ro"

    check("Warehouse query role", current_user)

    loaded: dict[str, dict] = {}

    def dashboard_search():
        found = api.get("/api/search?type=dash-db")
        for uid, path in DASHBOARDS.items():
            item = next((entry for entry in found if entry.get("uid") == uid), None)
            assert item is not None, f"{uid} is missing"
            assert item.get("folderTitle") == "Publisher Lakehouse", f"{uid} has wrong folder"
            loaded[uid] = json.loads(path.read_text(encoding="utf-8"))
        return "three dashboards in Publisher Lakehouse"

    check("Provisioned dashboards", dashboard_search)
    if not loaded:
        loaded = {
            uid: json.loads(path.read_text(encoding="utf-8"))
            for uid, path in DASHBOARDS.items()
        }

    classes: list[str] = []
    results: dict[tuple[str, str], list[dict]] = {}
    for uid, dashboard in loaded.items():
        for variable in dashboard.get("templating", {}).get("list", []):
            name = variable["name"]

            def query_variable(variable=variable, name=name):
                rows = api.query(
                    substitute(variable["query"], classes),
                    start_ms=start_ms, end_ms=end_ms,
                )
                assert rows, "variable returned no data"
                if name == "content_class":
                    classes.extend(str(row["content_class"]) for row in rows)
                    assert classes, "no content classes returned"
                return row_count(rows)

            check(f"{dashboard['title']} / variable {name}", query_variable)

        for item in panels(dashboard["panels"]):
            for target in item["targets"]:
                label = f"{dashboard['title']} / {item['title']} / {target['refId']}"

                def query_panel(item=item, target=target, uid=uid):
                    rows = api.query(
                        substitute(target["rawSql"], classes),
                        format=target.get("format", "table"),
                        start_ms=start_ms, end_ms=end_ms,
                    )
                    assert rows, "panel returned no data"
                    results[(uid, item["title"])] = rows
                    return row_count(rows)

                check(label, query_panel)

    corpus = lambda title: results[("pl-corpus", title)]
    quality = lambda title: results[("pl-data-quality", title)]
    pipeline = lambda title: results[("pl-pipeline-health", title)]
    observed: dict = {}

    def extra_query(sql: str) -> list[dict]:
        return api.query(substitute(sql, classes), start_ms=start_ms, end_ms=end_ms)

    def publication_months():
        scholarly = int(only_value(corpus("Scholarly articles"), "scholarly_articles"))
        months = {
            row["publication_month"]: int(row["articles"])
            for row in corpus("Scholarly articles per publication month")
        }
        unknown = int(only_value(
            corpus("Scholarly articles with unknown publication month"),
            "unknown_publication_month",
        ))
        assert sum(months.values()) + unknown == scholarly, (
            f"months {sum(months.values())} + unknown {unknown} != scholarly {scholarly}"
        )
        observed.update(
            scholarly_articles=scholarly, publication_months=months,
            unknown_publication_month=unknown,
        )
        return f"{sum(months.values())} across {len(months)} months + {unknown} unknown = {scholarly} scholarly"

    check("Invariant: publication months + unknown = scholarly", publication_months)

    def journals():
        stat = int(only_value(corpus("Journals"), "journals"))
        charted = {row["journal"] for row in corpus("Articles per journal by content class")}
        assert stat == len(charted), f"stat {stat} != {len(charted)} charted journals"
        observed.update(journals=stat)
        return f"{stat} = {len(charted)} charted journals"

    check("Invariant: journals stat = journals charted", journals)

    def cc_share():
        stat = number(only_value(corpus("CC-licensed share (scholarly)"), "cc_share"))
        assert 0 <= stat <= 1, f"CC share {stat} is outside [0, 1]"
        shares = {
            row["journal"]: number(row["cc_share"])
            for row in corpus("CC-licensed share by journal (scholarly)")
        }
        counts = {row["journal"]: row for row in extra_query(CC_COUNTS_SQL)}
        assert counts.keys() == shares.keys(), "per-journal counts and shares list different journals"
        for journal, share in shares.items():
            row = counts[journal]
            assert 0 <= share <= 1, f"{journal} CC share {share} is outside [0, 1]"
            exact = row["cc_count"] / row["scholarly"]
            assert abs(share - exact) <= CC_SHARE_TOLERANCE, f"{journal}: panel {share} != {exact:.4f}"
        cc_total = sum(int(row["cc_count"]) for row in counts.values())
        scholarly_total = sum(int(row["scholarly"]) for row in counts.values())
        assert scholarly_total > 0, "no scholarly articles in any journal"
        ratio = cc_total / scholarly_total
        assert abs(stat - ratio) <= CC_SHARE_TOLERANCE, (
            f"stat {stat} != {cc_total}/{scholarly_total} = {ratio:.4f}"
        )
        observed.update(cc_share=stat)
        return f"stat {stat:.2f} vs {cc_total}/{scholarly_total} = {ratio:.4f} across {len(shares)} journals"

    check("Invariant: CC share = sum cc / sum scholarly", cc_share)

    def top_author():
        scholarly = int(only_value(corpus("Scholarly articles"), "scholarly_articles"))
        top = number(corpus("Top authors (weighted)")[0]["weighted_articles"])
        assert top <= scholarly, f"top weighted value {top} > scholarly {scholarly}"
        if scholarly > 0:
            assert top > 0, "top weighted value is not positive"
        observed.update(top_author_weighted_articles=top)
        return f"{top:.2f} <= {scholarly} scholarly"

    check("Invariant: top weighted author <= scholarly", top_author)

    def data_quality():
        runs = int(only_value(quality("DQ runs recorded"), "dq_runs"))
        assert runs >= 1, "no DQ runs recorded"
        latest = quality("Latest check per rule")
        rules_per_run = len({row["rule"] for row in latest})
        assert len(latest) == rules_per_run, (
            f"latest run has {len(latest)} rows for {rules_per_run} distinct rules"
        )
        from publisher_lakehouse.transform.schemas import DQ_RULES

        declared = {name for name, _ in DQ_RULES}
        assert {row["rule"] for row in latest} == declared, (
            "latest run's rules differ from transform/schemas.py"
        )
        rows = int(only_value(
            extra_query(
                "SELECT COUNT(*) AS dq_rows FROM ops.dq_results "
                "WHERE publisher IN (${publisher:sqlstring})"
            ),
            "dq_rows",
        ))
        assert rows == rules_per_run * runs, (
            f"{rows} DQ rows != {rules_per_run} rules x {runs} runs"
        )
        observed.update(
            dq_runs=runs, dq_rows=rows, dq_rules_per_run=rules_per_run,
            quarantined_rows=int(only_value(quality("Quarantined rows (latest run)"), "quarantined_rows")),
            flagged_articles=int(only_value(quality("Rows with at least one soft flag"), "flagged_articles")),
        )
        return f"{rows} rows = {rules_per_run} rules x {runs} runs; latest run has {rules_per_run} rules"

    check("Invariant: DQ rows = rules per run x runs", data_quality)

    def layer_status():
        rows = pipeline("Lake tables")
        assert len(rows) == LAKE_TABLES, f"{len(rows)} layer_status rows, expected {LAKE_TABLES}"
        negative = [row for row in rows if row["row_count"] is None or row["row_count"] < 0]
        assert not negative, f"invalid row counts: {negative}"
        observed.update(lake_table_rows={
            f"{row['layer']}/{row['table_name']}": int(row["row_count"]) for row in rows
        })
        return f"{len(rows)} tables, all row counts >= 0"

    check("Invariant: layer_status", layer_status)

    def publish_log():
        successes = int(only_value(
            extra_query(
                "SELECT COUNT(*) AS successes FROM ops.publish_log WHERE status = 'success'"
            ),
            "successes",
        ))
        assert successes >= 1, "ops.publish_log has no success row"
        assert only_value(pipeline("Last successful publish"), "last_successful_publish") is not None
        latest = only_value(pipeline("Last publish status"), "status")
        assert latest == "success", f"latest publish status is {latest}"
        history = pipeline("Publish history")
        assert history[0]["status"] == latest, "publish history disagrees with the status stat"
        observed.update(last_publish_status=latest)
        return f"{successes} success rows; latest {latest}"

    check("Invariant: publish log", publish_log)

    def journal_freshness():
        rows = pipeline("Journal freshness")
        with_articles = [row for row in rows if row["articles_in_lake"] > 0]
        stat = int(only_value(corpus("Journals"), "journals"))
        assert len(with_articles) == stat, (
            f"{len(with_articles)} journals with articles != Journals stat {stat}"
        )
        days = [row["days_since_newest"] for row in rows]
        assert all(day is None or day >= 0 for day in days), f"negative ages: {days}"
        known = [day for day in days if day is not None]
        assert days[: len(days) - len(known)] == [None] * (len(days) - len(known)), (
            "journals without articles are not listed first"
        )
        assert known == sorted(known, reverse=True), "not sorted stalest first"
        observed.update(freshness_journals=len(rows))
        return f"{len(rows)} current journals; stalest {known[0] if known else 'n/a'} days"

    check("Invariant: journal freshness", journal_freshness)

    def scraper_health():
        finished =only_value(pipeline("Last scrape finished"), "last_scrape_finished")
        assert finished is not None, "ops.scrape_runs has no finished run"
        status = pipeline("Last run status")
        assert len(status) == 1, f"expected one last run, got {len(status)}"
        assert status[0]["status"] in {"complete", "incomplete"}, status[0]
        runs = pipeline("Last 10 runs")
        assert 1 <= len(runs) <= 10, f"{len(runs)} runs"
        assert len({row["run_id"] for row in runs}) == len(runs), "duplicate run ids"
        assert runs[0]["finished_at"] == finished, "latest run is not first"

        counts = manifest_counts(PUBLISHER)
        pushed = {
            (row["url_type"], row["status"]): row["row_count"]
            for row in api.query(
                "SELECT url_type, status, row_count FROM ops.scrape_status "
                f"WHERE publisher = {quote(PUBLISHER)}",
                start_ms=start_ms, end_ms=end_ms,
            )
        }
        assert pushed == counts, f"ops.scrape_status {pushed} != manifest {counts}"
        charted = sum(row["manifest_rows"] for row in pipeline("Manifest rows by type and status"))
        assert charted >= sum(counts.values()), "bar chart is missing manifest rows"
        manifest_journals = sum(n for (url_type, _), n in counts.items() if url_type == "journal")
        journals = only_value(
            api.query(
                "SELECT COUNT(*) AS journal_rows FROM ops.scrape_journals "
                f"WHERE publisher = {quote(PUBLISHER)}",
                start_ms=start_ms, end_ms=end_ms,
            ),
            "journal_rows",
        )
        assert journals == manifest_journals, (
            f"ops.scrape_journals has {journals} rows, manifest has {manifest_journals}"
        )
        return (
            f"last run {status[0]['status']} (exit {status[0]['ingest_exit_code']}); "
            f"{len(runs)} recent runs; scrape_status matches manifest "
            f"({sum(counts.values())} rows); {journals} journal rows = manifest "
            f"{manifest_journals}"
        )

    check("Scraper health", scraper_health)
    print(f"INFO observed values: {json.dumps(observed, sort_keys=True)}")

    if args.baseline is not None:

        def baseline():
            expected = load_baseline(args.baseline)
            problems = baseline_mismatches(expected, observed)
            assert not problems, "; ".join(problems)
            return f"{len(expected)} values match {args.baseline.name}"

        check("Baseline values", baseline)

    def anonymous_refused():
        status, _ = api.request("GET", "/api/search", authenticate=False)
        assert status == 401, f"anonymous search returned HTTP {status}"
        return "HTTP 401"

    check("Anonymous search refused", anonymous_refused)

    def signup_refused():
        suffix = secrets.token_hex(6)
        status, _ = api.request("POST", "/api/user/signup", {
            "name": "Gate 10b probe",
            "email": f"gate10b-{suffix}@example.invalid",
            "user": f"gate10b-{suffix}",
            "password": secrets.token_urlsafe(24),
        }, authenticate=False)
        assert status in {401, 403, 404}, f"sign-up returned HTTP {status}"
        return f"HTTP {status}"

    check("Sign-up refused", signup_refused)

    def dashboard_save_refused():
        response = api.get("/api/dashboards/uid/pl-corpus")
        status, body = api.request("POST", "/api/dashboards/db", {
            "dashboard": response["dashboard"],
            "folderUid": response["meta"]["folderUid"],
            "overwrite": True,
            "message": "Gate 10b provisioned-save guard check",
        })
        message = body.get("message", "") if isinstance(body, dict) else ""
        assert status == 400 and message == "Cannot save provisioned dashboard", (
            f"dashboard save returned HTTP {status}: {message}"
        )
        return f"HTTP {status}"

    check("Provisioned dashboard save refused", dashboard_save_refused)
    print(f"{'FAIL' if failures else 'OK'} summary: {failures} failed checks")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
