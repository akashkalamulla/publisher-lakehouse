"""Spark and S3A configuration for the container runtime."""

from __future__ import annotations

import os
from pathlib import PurePosixPath
from urllib.parse import urlsplit


def lake_conf(endpoint: str, access_key: str, secret_key: str) -> dict[str, str]:
    """Return Spark settings without importing PySpark on the host."""

    scheme = urlsplit(endpoint).scheme.lower()
    if scheme not in {"http", "https"}:
        raise ValueError("S3_ENDPOINT_URL must use http or https")
    return {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(scheme == "https").lower(),
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.access.key": access_key,
        "spark.hadoop.fs.s3a.secret.key": secret_key,
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.sql.session.timeZone": "UTC",
    }


def build_spark(app_name: str):
    """Start a local Spark session using jars already inside the image."""

    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")
    missing = [
        name
        for name, value in (
            ("S3_ACCESS_KEY", access_key),
            ("S3_SECRET_KEY", secret_key),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing lake environment variable(s): {', '.join(missing)}")
    endpoint = os.environ.get("S3_ENDPOINT_URL") or "http://localhost:9000"

    from pyspark.sql import SparkSession

    builder = SparkSession.builder.master("local[*]").appName(app_name)
    for key, value in lake_conf(endpoint, access_key, secret_key).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def lake_uri(*parts: str) -> str:
    """Build an S3A URI from the environment's bucket and path components."""

    bucket = os.environ.get("LAKE_BUCKET") or "lakehouse"
    if not parts:
        return f"s3a://{bucket}"
    return f"s3a://{bucket}/{PurePosixPath(*parts)}"
