"""Initial schema — tracked by Alembic.

Creates all tables defined in storage.models using SQLAlchemy's
Base.metadata.create_all(), so that Alembic migrations are self-contained
and do not rely on the application having created tables first.

Revision ID: 0001
Revises:
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Import models to register them with Base.metadata, then create all tables.
    import storage.models  # noqa: F401
    from storage.base import Base

    bind = op.get_bind()
    Base.metadata.create_all(bind)


def downgrade() -> None:
    # Drop all tables in reverse dependency order.
    # SQLite doesn't support DROP ... CASCADE, so we drop in order.
    import storage.models  # noqa: F401
    from storage.base import Base

    bind = op.get_bind()
    Base.metadata.drop_all(bind)
