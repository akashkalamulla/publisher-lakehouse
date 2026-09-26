from __future__ import annotations

import importlib
import sys

from publisher_lakehouse.transform.session import lake_conf, lake_uri


def test_session_import_does_not_import_pyspark() -> None:
    sys.modules.pop("pyspark", None)
    importlib.import_module("publisher_lakehouse.transform.session")
    assert "pyspark" not in sys.modules


def test_lake_conf_has_delta_and_s3a_settings() -> None:
    conf = lake_conf("http://minio:9000", "access", "secret")
    assert conf == {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.access.key": "access",
        "spark.hadoop.fs.s3a.secret.key": "secret",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.sql.session.timeZone": "UTC",
    }
    assert lake_conf("https://store.example", "a", "b")[
        "spark.hadoop.fs.s3a.connection.ssl.enabled"
    ] == "true"


def test_lake_uri_uses_environment_bucket(monkeypatch) -> None:
    monkeypatch.setenv("LAKE_BUCKET", "example-lake")
    assert lake_uri("bronze", "articles") == "s3a://example-lake/bronze/articles"
