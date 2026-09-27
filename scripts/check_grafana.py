"""Verify provisioned Grafana dashboards through the live Grafana API."""

from __future__ import annotations

import argparse
import base64
import json
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:3000")
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

                def query_panel(item=item, target=target):
                    rows = api.query(
                        substitute(target["rawSql"], classes),
                        format=target.get("format", "table"),
                        start_ms=start_ms, end_ms=end_ms,
                    )
                    assert rows, "panel returned no data"
                    results[(uid, item["title"])] = rows
                    return row_count(rows)

                check(label, query_panel)

    def headline():
        corpus = lambda title: results[("pl-corpus", title)]
        quality = lambda title: results[("pl-data-quality", title)]
        pipeline = lambda title: results[("pl-pipeline-health", title)]
        assert only_value(corpus("Scholarly articles"), "scholarly_articles") == 204
        assert only_value(corpus("Journals"), "journals") == 11
        months = {
            row["publication_month"]: row["articles"]
            for row in corpus("Scholarly articles per publication month")
        }
        assert months == {"2026-07": 32, "2026-08": 10, "2026-09": 14, "2026-10": 145}, months
        assert only_value(
            corpus("Scholarly articles with unknown publication month"),
            "unknown_publication_month",
        ) == 3
        assert float(corpus("Top authors (weighted)")[0]["weighted_articles"]) == 1.0
        dairy = next(
            row for row in corpus("CC-licensed share by journal (scholarly)")
            if row["journal"] == "Journal of Dairy Science"
        )
        assert float(dairy["cc_share"]) == 1.0
        assert only_value(quality("DQ runs recorded"), "dq_runs") == 3
        assert len(quality("Latest check per rule")) == 12
        assert len(pipeline("Lake tables")) == 11
        assert only_value(
            api.query(
                "SELECT COUNT(*) AS dq_rows FROM ops.dq_results",
                start_ms=start_ms, end_ms=end_ms,
            ),
            "dq_rows",
        ) == 36
        return "204 scholarly; 11 journals; months 32/10/14/145; 3 unknown; top weight 1.00; Dairy CC 1.00; 36 DQ rows; 3 DQ runs; 11 lake tables"

    check("Known headline values", headline)

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
