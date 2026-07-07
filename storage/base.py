"""SQLAlchemy declarative base for all storage models."""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Declarative base for all storage ORM models."""
