"""Add FTS5 virtual table for full-text search on facts.

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-07

Creates:
  - facts_fts virtual table (FTS5) indexing subject, predicate, object
  - Triggers to keep FTS index in sync with facts table
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create the FTS5 virtual table on the facts table
    op.execute(
        "CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts "
        "USING fts5("
        "subject, predicate, object, "
        "content=facts, "
        "content_rowid=rowid"
        ")"
    )

    # Trigger: when a new row is inserted into facts
    op.execute(
        "CREATE TRIGGER IF NOT EXISTS facts_ai AFTER INSERT ON facts BEGIN "
        "INSERT INTO facts_fts(rowid, subject, predicate, object) "
        "VALUES (new.rowid, new.subject, new.predicate, new.object); "
        "END"
    )

    # Trigger: when a row in facts is deleted
    op.execute(
        "CREATE TRIGGER IF NOT EXISTS facts_ad AFTER DELETE ON facts BEGIN "
        "INSERT INTO facts_fts(facts_fts, rowid, subject, predicate, object) "
        "VALUES('delete', old.rowid, old.subject, old.predicate, old.object); "
        "END"
    )

    # Trigger: when a row in facts is updated
    op.execute(
        "CREATE TRIGGER IF NOT EXISTS facts_au AFTER UPDATE ON facts BEGIN "
        "INSERT INTO facts_fts(facts_fts, rowid, subject, predicate, object) "
        "VALUES('delete', old.rowid, old.subject, old.predicate, old.object); "
        "INSERT INTO facts_fts(rowid, subject, predicate, object) "
        "VALUES (new.rowid, new.subject, new.predicate, new.object); "
        "END"
    )

    # Populate FTS index with existing data
    op.execute(
        "INSERT INTO facts_fts(facts_fts, rowid, subject, predicate, object) "
        "SELECT 'rebuild', rowid, subject, predicate, object FROM facts"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS facts_au")
    op.execute("DROP TRIGGER IF EXISTS facts_ad")
    op.execute("DROP TRIGGER IF EXISTS facts_ai")
    op.execute("DROP TABLE IF EXISTS facts_fts")
