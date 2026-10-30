"""Add covering indexes for canonical relation reads.

Revision ID: 0007
Revises: 0006
"""

import re

from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None

_INDEXES = (
    ("s09_order", ("created_at", "source_id", "target_id", "relation_type")),
    ("s09_source_order", ("source_id", "created_at", "target_id", "relation_type")),
    ("s09_target_order", ("target_id", "created_at", "source_id", "relation_type")),
)


def _normalized(sql):
    return re.sub(r"\s+", "", sql).lower()


def upgrade():
    conn = op.get_bind()
    missing = []
    # Validate every occupied name before creating any of the missing indexes.
    for name, columns in _INDEXES:
        ddl = f"CREATE INDEX {name} ON claim_relations ({', '.join(columns)})"
        row = conn.exec_driver_sql(
            "SELECT type, tbl_name, sql FROM sqlite_master WHERE name=?", (name,)
        ).first()
        if row is None:
            missing.append(ddl)
            continue
        keys = conn.exec_driver_sql(f"PRAGMA index_xinfo('{name}')").all()
        expected_keys = [(i, column, 0, "BINARY") for i, column in enumerate(columns)]
        actual_keys = [(r[0], r[2], r[3], r[4]) for r in keys if r[5]]
        if (
            row[0] != "index"
            or row[1] != "claim_relations"
            or row[2] is None
            or _normalized(row[2]) != _normalized(ddl)
            or actual_keys != expected_keys
        ):
            raise RuntimeError(f"S09 index name occupied by a different definition: {name}")
    for ddl in missing:
        conn.exec_driver_sql(ddl)


def downgrade():
    for name, _columns in _INDEXES:
        op.get_bind().exec_driver_sql(f"DROP INDEX IF EXISTS {name}")
