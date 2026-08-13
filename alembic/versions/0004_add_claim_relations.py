"""Create canonical claim_relations table.

Revision ID: 0004
Revises: 0003
Create Date: 2026-08-13

Creates:
  - claim_relations table for canonical inter-claim relations
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "claim_relations" in inspector.get_table_names():
        # The table may already exist: ClaimRelationORM is registered in
        # Base.metadata and SQLiteProvider.initialize() runs create_all()
        # before alembic on a mixed rollout. Make this migration idempotent.
        return
    op.create_table(
        "claim_relations",
        sa.Column("source_id", sa.String(), nullable=False),
        sa.Column("target_id", sa.String(), nullable=False),
        sa.Column("relation_type", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("source_id", "target_id", "relation_type"),
    )
    op.create_index(
        op.f("ix_claim_relations_target_id"),
        "claim_relations",
        ["target_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_claim_relations_relation_type"),
        "claim_relations",
        ["relation_type"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_claim_relations_relation_type"), table_name="claim_relations")
    op.drop_index(op.f("ix_claim_relations_target_id"), table_name="claim_relations")
    op.drop_table("claim_relations")
