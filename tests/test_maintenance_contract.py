"""Host checks for the maintenance DAG and the job's pure guard functions."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from publisher_lakehouse.transform import maintenance
from publisher_lakehouse.transform.maintenance import (
    MIN_VACUUM_HOURS,
    SMALL_FILE_BYTES,
    check_vacuum_hours,
    plan_action,
)


ROOT = Path(__file__).resolve().parents[1]
DAG_SOURCE = (ROOT / "dags" / "maintenance_dags.py").read_text(encoding="utf-8")
TREE = ast.parse(DAG_SOURCE)


def _assignments() -> dict[str, ast.AST]:
    return {
        target.id: statement.value
        for statement in TREE.body
        if isinstance(statement, ast.Assign)
        for target in statement.targets
        if isinstance(target, ast.Name)
    }


def _calls(name: str) -> list[ast.Call]:
    return [
        node for node in ast.walk(TREE)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == name
    ]


def _keyword(call: ast.Call, name: str) -> ast.AST:
    return next(keyword.value for keyword in call.keywords if keyword.arg == name)


def _constant(node: ast.AST):
    if isinstance(node, ast.Name):
        node = _assignments()[node.id]
    assert isinstance(node, ast.Constant), ast.dump(node)
    return node.value


def test_the_single_task_uses_the_lake_writer_pool_and_the_job_image():
    [operator] = _calls("DockerOperator")
    assert _constant(_keyword(operator, "pool")) == "lake_writer"
    assert _constant(_keyword(operator, "image")) == "publisher-lakehouse-job:local"
    assert _constant(_keyword(operator, "retries")) == 0
    timeout = _keyword(operator, "execution_timeout")
    assert isinstance(timeout, ast.Call) and timeout.func.id == "timedelta"


def test_secrets_reach_the_job_only_through_private_environment():
    [operator] = _calls("DockerOperator")
    names = {keyword.arg for keyword in operator.keywords}
    assert "private_environment" in names and "environment" not in names
    env_names = {node.value for node in _assignments()["JOB_ENV_NAMES"].elts}
    assert env_names == {"S3_ENDPOINT_URL", "S3_ACCESS_KEY", "S3_SECRET_KEY", "LAKE_BUCKET"}
    command = _constant(_keyword(operator, "command"))
    assert "publisher_lakehouse.transform.maintenance" in command
    assert "publish" not in command.replace("publisher_lakehouse", "")
    assert "$" not in command and "SECRET" not in command.upper()


def test_schedule_is_weekly_sunday_0300_in_an_explicit_timezone():
    [dag] = _calls("DAG")
    assert _constant(_keyword(dag, "dag_id")) == "lakehouse_maintenance"
    assert _keyword(dag, "catchup").value is False
    assert _keyword(dag, "max_active_runs").value == 1

    schedule = _keyword(dag, "schedule")
    assert isinstance(schedule, ast.Call) and schedule.func.id == "CronTriggerTimetable"
    minute, hour, day, month, weekday = _constant(schedule.args[0]).split()
    assert (minute, hour, day, month, weekday) == ("0", "3", "*", "*", "0")
    timezone = _constant(_keyword(schedule, "timezone"))
    assert timezone == "Asia/Colombo"

    start = _keyword(dag, "start_date")
    assert ast.unparse(start.func) == "pendulum.datetime"
    assert _constant(_keyword(start, "tz")) == timezone

    params = _keyword(dag, "params")
    assert [key.value for key in params.keys] == ["vacuum_hours", "dry_run"]
    vacuum, dry_run = params.values
    assert vacuum.args[0].value == 0 and _constant(_keyword(vacuum, "type")) == "integer"
    assert dry_run.args[0].value is False


def test_command_template_omits_vacuum_when_zero_and_passes_both_params():
    jinja2 = pytest.importorskip("jinja2")
    template = jinja2.Template(_assignments()["MAINTENANCE_COMMAND"].value)
    base = "python -m publisher_lakehouse.transform.maintenance"
    assert template.render(params={"vacuum_hours": 0, "dry_run": False}) == base
    assert template.render(params={"vacuum_hours": 168, "dry_run": True}) == (
        f"{base} --vacuum-hours 168 --dry-run"
    )


@pytest.mark.parametrize("hours", [0, 1, 24, MIN_VACUUM_HOURS - 1, -5])
def test_vacuum_guard_rejects_less_than_seven_days(hours):
    with pytest.raises(ValueError, match="168-hour"):
        check_vacuum_hours(hours)


def test_vacuum_guard_allows_seven_days_or_more_and_defaults_to_off():
    assert MIN_VACUUM_HOURS == 168
    assert check_vacuum_hours(None) is None
    assert check_vacuum_hours(168) == 168
    assert check_vacuum_hours(720) == 720


def test_optimize_plan_needs_several_files_averaging_under_32_mb():
    assert SMALL_FILE_BYTES == 32 * 1024 * 1024
    assert plan_action(0, 0) == "skip"
    assert plan_action(1, 1_000) == "skip"
    assert plan_action(2, 1_000) == "optimize"
    assert plan_action(9, 9 * SMALL_FILE_BYTES - 9) == "optimize"
    assert plan_action(9, 9 * SMALL_FILE_BYTES) == "skip"


def test_the_job_never_disables_the_retention_check():
    source = Path(maintenance.__file__).read_text(encoding="utf-8")
    assert maintenance.RETENTION_CHECK == "spark.databricks.delta.retentionDurationCheck.enabled"
    assert ".conf.set(" not in source and ".config(" not in source
    assert "retentionDurationCheck.enabled\", \"false" not in source
