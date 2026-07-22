"""Initial schema — tracked by Alembic.

Creates only the set of tables that existed before migration 0003
(beliefs/evidence).  This keeps each migration properly additive so
that migration 0003 can create ``beliefs`` and ``evidence`` without
a ``table already exists`` collision.

Tables created here:
  - facts, entities, decisions, skills, receipts
  - lifecycle_states, lifecycle_events
  - outbox_entries

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


# Tables that existed before migration 0003 added beliefs/evidence.
_INITIAL_TABLES = [
    "facts",
    "entities",
    "decisions",
    "skills",
    "receipts",
    "lifecycle_states",
    "lifecycle_events",
    "outbox_entries",
]


def upgrade() -> None:
    import storage.models  # noqa: F401  — register all current models
    from storage.base import Base

    bind = op.get_bind()
    tables = [Base.metadata.tables[n] for n in _INITIAL_TABLES]
    Base.metadata.create_all(bind, tables=tables)


def downgrade() -> None:
    # Drop only what 0001 created (reverse order, no FK chains).
    for name in reversed(_INITIAL_TABLES):
        op.execute(f"DROP TABLE IF EXISTS {name}")
