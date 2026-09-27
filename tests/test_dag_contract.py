"""Static DAG checks that run in the host test environment without Airflow."""

from __future__ import annotations

import ast
from pathlib import Path


DAG = Path(__file__).resolve().parents[1] / "dags" / "lakehouse_dags.py"
TREE = ast.parse(DAG.read_text(encoding="utf-8"))


def _assignments() -> dict[str, ast.AST]:
    return {
        target.id: statement.value
        for statement in TREE.body
        if isinstance(statement, ast.Assign)
        for target in statement.targets
        if isinstance(target, ast.Name)
    }


def test_writer_tasks_use_the_one_slot_pool():
    jobs = [
        node for node in ast.walk(TREE)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        and node.func.id == "job"
    ]
    assert {node.args[0].value for node in jobs} == {"bronze", "silver", "gold", "publish"}
    operators = [
        node for node in ast.walk(TREE)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        and node.func.id == "DockerOperator"
    ]
    assert len(operators) == 1
    pool = next(keyword.value for keyword in operators[0].keywords if keyword.arg == "pool")
    assert isinstance(pool, ast.Constant) and pool.value == "lake_writer"


def test_job_secrets_use_private_environment_only():
    operators = [
        node for node in ast.walk(TREE)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        and node.func.id == "DockerOperator"
    ]
    assert operators
    assert all(any(kw.arg == "private_environment" for kw in op.keywords) for op in operators)
    assert all(not any(kw.arg == "environment" for kw in op.keywords) for op in operators)
    names = _assignments()["JOB_ENV_NAMES"]
    assert isinstance(names, ast.Tuple)
    assert {node.value for node in names.elts} >= {
        "S3_ACCESS_KEY", "S3_SECRET_KEY", "WAREHOUSE_PASSWORD", "LAKE_BUCKET"
    }


def test_job_image_is_fixed():
    image = _assignments()["JOB_IMAGE"]
    assert isinstance(image, ast.Constant)
    assert image.value == "publisher-lakehouse-job:local"


def test_sciencedirect_is_instantiated():
    publishers = _assignments()["PUBLISHERS"]
    assert isinstance(publishers, ast.List)
    assert [node.value for node in publishers.elts] == ["sciencedirect"]
