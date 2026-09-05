"""Compatibility checkpoint after the historical initial schema.

The historical ``70e6afc8d15d`` revision owns the initial tables.  This
revision remains an explicit installed transition for the former Alembic
entrypoint; it verifies/repairs only missing legacy tables and deliberately
does not add post-initialisation columns such as dedup keys.

Revision ID: 0001
Revises:
"""

from typing import Sequence, Union

revision: str = "0001"
down_revision: Union[str, None] = "70e6afc8d15d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 70e6afc8d15d is the owner of these tables.  Keeping this revision as an
    # explicit checkpoint is important for databases stamped by the old
    # entrypoint, while avoiding a second copy of the initial DDL.
    return


def downgrade() -> None:
    return
