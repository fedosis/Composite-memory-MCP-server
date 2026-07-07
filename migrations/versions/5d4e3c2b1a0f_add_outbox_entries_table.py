"""Add outbox_entries table

Revision ID: 5d4e3c2b1a0f
Revises: 70e6afc8d15d
Create Date: 2026-07-07 17:30:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "5d4e3c2b1a0f"
down_revision: Union[str, Sequence[str], None] = "70e6afc8d15d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create outbox_entries table."""
    op.create_table(
        "outbox_entries",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("record_type", sa.String(), nullable=False),
        sa.Column("record_id", sa.String(), nullable=False),
        sa.Column("operation", sa.String(), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("processed_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_outbox_entries_status"), "outbox_entries", ["status"], unique=False
    )
    op.create_index(
        op.f("ix_outbox_entries_record_type"),
        "outbox_entries",
        ["record_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_outbox_entries_record_id"),
        "outbox_entries",
        ["record_id"],
        unique=False,
    )


def downgrade() -> None:
    """Drop outbox_entries table."""
    op.drop_index(op.f("ix_outbox_entries_record_id"), table_name="outbox_entries")
    op.drop_index(
        op.f("ix_outbox_entries_record_type"), table_name="outbox_entries"
    )
    op.drop_index(op.f("ix_outbox_entries_status"), table_name="outbox_entries")
    op.drop_table("outbox_entries")
