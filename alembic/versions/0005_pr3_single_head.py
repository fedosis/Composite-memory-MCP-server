"""Merge both installed migration tracks and reconcile additive schemas.

Revision ID: 0005
Revises: b2f3a4c5d6e7, 6a7b8c9d0e1f
"""

from pathlib import Path

from alembic import util

revision = "0005"
down_revision = ("b2f3a4c5d6e7", "6a7b8c9d0e1f")
branch_labels = None
depends_on = None


def upgrade():
    # A genuinely installed legacy B2 never traversed 0002--0004. Reuse
    # their additive DDL, now compatible with application-created tables.
    # Rebuilding facts FTS also restores the index after B2's table rebuild.
    directory = str(Path(__file__).resolve().parent)
    for filename in ("0002_add_fts5.py", "0003_add_beliefs.py", "0004_add_claim_relations.py"):
        util.load_python_file(directory, filename).upgrade()


def downgrade():
    raise RuntimeError("PR-3 bridge is irreversible; restore a database backup instead")
