"""Run the real Alembic migrations against a test engine.

Tests migrate rather than calling ``metadata.create_all``: a schema created
from the models would hide any drift between the models and the migration that
production actually runs.
"""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import Engine


PROJECT_ROOT = Path(__file__).parents[1]
ALEMBIC_INI = PROJECT_ROOT / "alembic.ini"


def alembic_config(connection) -> Config:
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("script_location", str(PROJECT_ROOT / "migrations"))
    config.attributes["connection"] = connection
    return config


def upgrade_to_head(engine: Engine) -> None:
    with engine.begin() as connection:
        command.upgrade(alembic_config(connection), "head")


def downgrade_to_base(engine: Engine) -> None:
    with engine.begin() as connection:
        command.downgrade(alembic_config(connection), "base")
