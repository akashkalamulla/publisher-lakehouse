"""Run the lakehouse only for complete, unacknowledged landing batches."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
import docker
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.docker.operators.docker import DockerOperator
from botocore.config import Config

from pl_markers import MARKER_ROOT, pending_marker_keys, processed_key


PUBLISHERS = ["sciencedirect"]
JOB_IMAGE = "publisher-lakehouse-job:local"
DOCKER_NETWORK = "publisher-lakehouse_default"
LAKE_WRITER_POOL = "lake_writer"
JOB_ENV_NAMES = (
    "S3_ENDPOINT_URL",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "LAKE_BUCKET",
    "WAREHOUSE_HOST",
    "WAREHOUSE_DB",
    "WAREHOUSE_USER",
    "WAREHOUSE_PASSWORD",
    "GRAFANA_DB_PASSWORD",
)


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _job_environment() -> dict[str, str]:
    return {
        **{name: os.environ[name] for name in JOB_ENV_NAMES},
        "PYTHONPATH": "/opt/app",
        "PYTHONDONTWRITEBYTECODE": "1",
        "SPARK_LOCAL_IP": "0.0.0.0",
    }


def _log_job_revision() -> None:
    image = docker.from_env().images.get(JOB_IMAGE)
    revision = image.attrs.get("Config", {}).get("Labels", {}).get(
        "org.opencontainers.image.revision"
    )
    if not revision:
        raise RuntimeError(f"{JOB_IMAGE} has no revision label")
    logging.info("Job image %s revision %s", JOB_IMAGE, revision)


def build_lakehouse_dag(publisher: str) -> DAG:
    """Define the same serial marker-driven chain for one publisher."""

    with DAG(
        dag_id=f"lakehouse_{publisher}",
        schedule="@hourly",
        start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,
        params={"force": Param(False, type="boolean")},
        tags=["lakehouse", publisher],
    ) as dag:

        @task.short_circuit(task_id="detect_pending")
        def detect_pending() -> bool:
            context = get_current_context()
            bucket = os.environ["LAKE_BUCKET"]
            prefix = f"{MARKER_ROOT}/{publisher}/"
            pages = _s3_client().get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=prefix
            )
            keys = [row["Key"] for page in pages for row in page.get("Contents", [])]
            pending = pending_marker_keys(keys, publisher)
            context["ti"].xcom_push(key="marker_keys", value=pending)
            forced = context["params"]["force"]
            logging.info("Pending markers for %s: %d; force=%s", publisher, len(pending), forced)
            if pending or forced:
                _log_job_revision()
            return bool(pending or forced)

        def job(task_id: str, command: str) -> DockerOperator:
            return DockerOperator(
                task_id=task_id,
                image=JOB_IMAGE,
                command=command,
                docker_url="unix:///var/run/docker.sock",
                network_mode=DOCKER_NETWORK,
                private_environment=_job_environment(),
                auto_remove="success",
                mount_tmp_dir=False,
                pool="lake_writer",
                retries=1,
                execution_timeout=timedelta(hours=2),
            )

        bronze = job(
            "bronze", f"python -m publisher_lakehouse.transform.bronze --publisher {publisher}"
        )
        silver = job(
            "silver", f"python -m publisher_lakehouse.transform.silver --publisher {publisher}"
        )
        gold = job(
            "gold", f"python -m publisher_lakehouse.transform.gold --publisher {publisher}"
        )
        publish = job("publish", "python -m publisher_lakehouse.transform.publish")

        @task(task_id="acknowledge")
        def acknowledge() -> None:
            context = get_current_context()
            keys = context["ti"].xcom_pull(task_ids="detect_pending", key="marker_keys") or []
            client = _s3_client()
            now = datetime.now(timezone.utc).isoformat()
            for key in keys:
                client.put_object(
                    Bucket=os.environ["LAKE_BUCKET"],
                    Key=processed_key(key),
                    Body=json.dumps({"run_id": context["run_id"], "processed_at": now}).encode("utf-8"),
                    ContentType="application/json",
                )
            logging.info("Acknowledged %d marker(s)", len(keys))

        detect_pending() >> bronze >> silver >> gold >> publish >> acknowledge()

    return dag


for _publisher in PUBLISHERS:
    globals()[f"lakehouse_{_publisher}"] = build_lakehouse_dag(_publisher)
