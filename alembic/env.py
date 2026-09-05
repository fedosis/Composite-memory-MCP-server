"""Alembic environment for the memory-server project.

This is the official migration entrypoint. It mirrors the legacy migration
environment so both script locations can share the same SQLite-safe behavior.
"""

from __future__ import annotations

import json
import os
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from storage.models import Base  # noqa: E402
from storage.sqlite_support import apply_sqlite_pragmas_sync, validate_busy_timeout_ms  # noqa: E402

from memory_server.settings import get_settings  # noqa: E402

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _record_migration_connection(**fields) -> None:
    path = os.environ.get("B2_MIGRATION_CONNECTION_EVENT_PATH")
    if path:
        with Path(path).open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(fields, sort_keys=True) + "\n")


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        apply_sqlite_pragmas_sync(
            connection,
            validate_busy_timeout_ms(get_settings().sqlite_busy_timeout_ms),
            context="Alembic migrations",
        )
        busy_timeout = connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one()
        journal_mode = connection.exec_driver_sql("PRAGMA journal_mode").scalar_one()
        _record_migration_connection(
            event="before_first_ddl",
            busy_timeout=int(busy_timeout),
            journal_mode=str(journal_mode).lower(),
        )
        connection.commit()

        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            transactional_ddl=True,
            render_as_batch=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
