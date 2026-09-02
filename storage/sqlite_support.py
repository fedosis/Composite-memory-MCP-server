"""Shared SQLite connection policy helpers.

The CMMS series fixes require one bounded busy-timeout policy on every DBAPI
connection plus explicit verification of the effective journal mode after WAL
requests. These helpers centralize that behavior for the provider, legacy
adapter, outbox worker, and Alembic migration connection.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import event, text

_MAX_BUSY_TIMEOUT_MS = 60_000
logger = logging.getLogger(__name__)


def validate_busy_timeout_ms(value: Any, *, context: str = "busy_timeout_ms") -> int:
    """Validate a bounded SQLite busy timeout.

    Only positive integers up to 60s are accepted. Booleans and non-integers
    fail closed so a caller cannot accidentally pass a truthy flag or float.
    """

    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{context} must be an integer")
    if value <= 0:
        raise ValueError(f"{context} must be > 0")
    if value > _MAX_BUSY_TIMEOUT_MS:
        raise ValueError(f"{context} must be <= {_MAX_BUSY_TIMEOUT_MS}")
    return value


def install_busy_timeout_listener(engine, busy_timeout_ms: int) -> None:
    """Apply PRAGMA busy_timeout to every DBAPI connection from *engine*."""

    validated = validate_busy_timeout_ms(busy_timeout_ms)
    sync_engine = getattr(engine, "sync_engine", engine)

    def _apply_busy_timeout(dbapi_connection, _connection_record) -> None:
        dbapi_connection.execute(f"PRAGMA busy_timeout={validated}")

    event.listen(sync_engine, "connect", _apply_busy_timeout)


async def apply_sqlite_pragmas_async(
    conn,
    busy_timeout_ms: int,
    *,
    context: str,
    allow_degraded_mode: bool = False,
) -> str:
    """Set and verify WAL/busy_timeout on an async SQLAlchemy connection."""

    validated = validate_busy_timeout_ms(busy_timeout_ms, context=f"{context} busy_timeout_ms")
    journal_mode = (
        await conn.exec_driver_sql("PRAGMA journal_mode=WAL")
    ).scalar_one()
    if str(journal_mode).lower() != "wal":
        if allow_degraded_mode and str(journal_mode).lower() == "memory":
            logger.warning("%s: degraded journal_mode=%r", context, journal_mode)
        else:
            raise RuntimeError(f"{context}: degraded journal_mode={journal_mode!r}")
    await conn.exec_driver_sql("PRAGMA synchronous=NORMAL")
    await conn.exec_driver_sql(f"PRAGMA busy_timeout={validated}")
    effective_timeout = (await conn.exec_driver_sql("PRAGMA busy_timeout")).scalar_one()
    if int(effective_timeout) != validated:
        raise RuntimeError(
            f"{context}: busy_timeout={effective_timeout!r} did not match {validated}"
        )
    return str(journal_mode)


def apply_sqlite_pragmas_sync(
    connection,
    busy_timeout_ms: int,
    *,
    context: str,
    allow_degraded_mode: bool = False,
) -> str:
    """Set and verify WAL/busy_timeout on a synchronous SQLAlchemy connection."""

    validated = validate_busy_timeout_ms(busy_timeout_ms, context=f"{context} busy_timeout_ms")
    journal_mode = connection.execute(text("PRAGMA journal_mode=WAL")).scalar_one()
    if str(journal_mode).lower() != "wal":
        if allow_degraded_mode and str(journal_mode).lower() == "memory":
            logger.warning("%s: degraded journal_mode=%r", context, journal_mode)
        else:
            raise RuntimeError(f"{context}: degraded journal_mode={journal_mode!r}")
    connection.execute(text("PRAGMA synchronous=NORMAL"))
    connection.execute(text(f"PRAGMA busy_timeout={validated}"))
    effective_timeout = connection.execute(text("PRAGMA busy_timeout")).scalar_one()
    if int(effective_timeout) != validated:
        raise RuntimeError(
            f"{context}: busy_timeout={effective_timeout!r} did not match {validated}"
        )
    return str(journal_mode)
