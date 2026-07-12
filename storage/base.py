"""SQLAlchemy declarative base for all storage models."""

from datetime import datetime, timezone

from sqlalchemy.orm import DeclarativeBase


def utcnow() -> datetime:
    """Return the current UTC datetime — callable usable as a SQLAlchemy column default.

    Using this instead of ``datetime.now(timezone.utc)`` inline ensures the
    default is evaluated per-row at INSERT time, not once at module import time.
    """
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    """Declarative base for all storage ORM models."""
