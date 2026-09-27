"""Weekly Delta compaction of the tracked lake tables; never publishes."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.timetables.trigger import CronTriggerTimetable


JOB_IMAGE = "publisher-lakehouse-job:local"
DOCKER_NETWORK = "publisher-lakehouse_default"
LOCAL_TIMEZONE = "Asia/Colombo"
# Sunday 03:00 local time.
WEEKLY_SCHEDULE = "0 3 * * 0"
# Maintenance reads and rewrites the lake only; it needs no warehouse login.
JOB_ENV_NAMES = ("S3_ENDPOINT_URL", "S3_ACCESS_KEY", "S3_SECRET_KEY", "LAKE_BUCKET")
MAINTENANCE_COMMAND = (
    "python -m publisher_lakehouse.transform.maintenance"
    "{% if params.vacuum_hours %} --vacuum-hours {{ params.vacuum_hours | int }}{% endif %}"
    "{% if params.dry_run %} --dry-run{% endif %}"
)


def _job_environment() -> dict[str, str]:
    return {
        **{name: os.environ[name] for name in JOB_ENV_NAMES},
        "PYTHONPATH": "/opt/app",
        "PYTHONDONTWRITEBYTECODE": "1",
        "SPARK_LOCAL_IP": "0.0.0.0",
    }


with DAG(
    dag_id="lakehouse_maintenance",
    schedule=CronTriggerTimetable(WEEKLY_SCHEDULE, timezone=LOCAL_TIMEZONE),
    start_date=pendulum.datetime(2026, 9, 1, tz=LOCAL_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    params={
        # 0 means no VACUUM; the job refuses anything below 168 hours.
        "vacuum_hours": Param(0, type="integer", minimum=0),
        "dry_run": Param(False, type="boolean"),
    },
    tags=["lakehouse", "maintenance"],
) as lakehouse_maintenance:
    DockerOperator(
        task_id="compact",
        image=JOB_IMAGE,
        command=MAINTENANCE_COMMAND,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        private_environment=_job_environment(),
        auto_remove="success",
        mount_tmp_dir=False,
        pool="lake_writer",
        retries=0,
        execution_timeout=timedelta(hours=2),
    )
