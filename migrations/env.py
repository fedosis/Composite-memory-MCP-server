import json
import os
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

# Add project root to sys.path so storage.models is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Import all storage models to populate Base.metadata
from storage.models import Base  # noqa: E402
from storage.sqlite_support import apply_sqlite_pragmas_sync, validate_busy_timeout_ms

from memory_server.settings import get_settings  # noqa: E402

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Set target metadata from our storage models
target_metadata = Base.metadata


def _record_migration_connection(**fields) -> None:
    path = os.environ.get("B2_MIGRATION_CONNECTION_EVENT_PATH")
    if path:
        with Path(path).open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(fields, sort_keys=True) + "\n")

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
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
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
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
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
