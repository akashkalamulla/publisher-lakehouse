"""Alembic environment.

``alembic.ini`` carries no URL.  It is resolved here, in this order:

1. a connection supplied on ``config.attributes["connection"]`` -- no URL is
   resolved at all,
2. a URL set programmatically on the config (used by the integration test),
3. ``-x db_url=...`` on the command line,
4. ``EnvironmentSettings.database_url``, i.e. ``.env``.

That keeps the password out of every committed file.
"""

from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from publisher_lakehouse.manifest.models import metadata
from publisher_lakehouse.settings import load_environment_settings


config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = metadata


def resolve_url() -> str:
    configured = config.get_main_option("sqlalchemy.url", "")
    if configured:
        return configured

    overrides = context.get_x_argument(as_dictionary=True)
    if overrides.get("db_url"):
        return overrides["db_url"]

    url = load_environment_settings().database_url
    if not url:
        raise RuntimeError(
            "No database URL. Set DATABASE_URL in .env or pass -x db_url=..."
        )
    return url


def run_migrations_offline() -> None:
    context.configure(
        url=resolve_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = config.attributes.get("connection", None)
    if connectable is not None:
        context.configure(connection=connectable, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()
        return

    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = resolve_url()
    engine = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
