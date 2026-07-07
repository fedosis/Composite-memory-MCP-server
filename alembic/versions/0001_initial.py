"""Initial schema — tracked by Alembic.

This revision creates the initial tables. The project uses
SQLAlchemy's Base.metadata.create_all() for initial table creation
in production, so this migration is informational.

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
    # Tables are created by SQLAlchemy's create_all() in SQLiteProvider.initialize().
    # This revision exists only to anchor the Alembic chain.
    pass


def downgrade() -> None:
    pass
