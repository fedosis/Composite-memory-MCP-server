"""Create beliefs, evidence tables and beliefs_fts FTS5 virtual table.

Revision ID: 0003
Revises: 0002
Create Date: 2026-07-12

Creates:
  - beliefs table (canonical fields: id, proposition, confidence, source, ...)
  - evidence table (foreign key to beliefs.id)
  - beliefs_fts virtual table (FTS5) indexing proposition
  - Triggers to keep FTS index in sync with beliefs table
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- beliefs table ---
    op.create_table(
        "beliefs",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("proposition", sa.String(2048), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False, server_default="0.5"),
        sa.Column("source", sa.String(128), nullable=False, server_default="system"),
        sa.Column("creator", sa.String(128), nullable=False, server_default="system"),
        sa.Column("source_ids", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("tags", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("last_reinforced_at", sa.DateTime(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("verification_status", sa.String(32), nullable=False, server_default="candidate"),
        sa.Column("lifecycle_state", sa.String(32), nullable=False, server_default="active"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_beliefs_proposition"), "beliefs", ["proposition"])
    op.create_index(op.f("ix_beliefs_lifecycle_state"), "beliefs", ["lifecycle_state"])

    # --- evidence table ---
    op.create_table(
        "evidence",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("belief_id", sa.String(), nullable=False),
        sa.Column("source_type", sa.String(32), nullable=False),
        sa.Column("source_id", sa.String(), nullable=False),
        sa.Column("weight", sa.Float(), nullable=False, server_default="0.5"),
        sa.Column("contributor", sa.String(128), nullable=False, server_default="system"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["belief_id"],
            ["beliefs.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_evidence_belief_id"), "evidence", ["belief_id"])

    # --- FTS5 virtual table ---
    op.execute(
        "CREATE VIRTUAL TABLE IF NOT EXISTS beliefs_fts "
        "USING fts5("
        "proposition, "
        "content=beliefs, "
        "content_rowid=rowid"
        ")"
    )

    # Trigger: when a new row is inserted into beliefs
    op.execute(
        "CREATE TRIGGER IF NOT EXISTS beliefs_ai AFTER INSERT ON beliefs BEGIN "
        "INSERT INTO beliefs_fts(rowid, proposition) "
        "VALUES (new.rowid, new.proposition); "
        "END"
    )

    # Trigger: when a row in beliefs is deleted
    op.execute(
        "CREATE TRIGGER IF NOT EXISTS beliefs_ad AFTER DELETE ON beliefs BEGIN "
        "INSERT INTO beliefs_fts(beliefs_fts, rowid, proposition) "
        "VALUES('delete', old.rowid, old.proposition); "
        "END"
    )

    # Trigger: when a row in beliefs is updated
    op.execute(
        "CREATE TRIGGER IF NOT EXISTS beliefs_au AFTER UPDATE ON beliefs BEGIN "
        "INSERT INTO beliefs_fts(beliefs_fts, rowid, proposition) "
        "VALUES('delete', old.rowid, old.proposition); "
        "INSERT INTO beliefs_fts(rowid, proposition) "
        "VALUES (new.rowid, new.proposition); "
        "END"
    )

    # Populate FTS index with existing data
    op.execute(
        "INSERT INTO beliefs_fts(beliefs_fts, rowid, proposition) "
        "SELECT 'rebuild', rowid, proposition FROM beliefs"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS beliefs_au")
    op.execute("DROP TRIGGER IF EXISTS beliefs_ad")
    op.execute("DROP TRIGGER IF EXISTS beliefs_ai")
    op.execute("DROP TABLE IF EXISTS beliefs_fts")
    op.drop_table("evidence")
    op.drop_table("beliefs")
