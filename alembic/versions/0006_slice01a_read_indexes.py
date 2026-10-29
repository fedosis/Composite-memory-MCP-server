"""Add the four SLICE-01A creation-key expression indexes.

Revision ID: 0006
Revises: 0005
"""

import re

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None

_KINDS = ("fact", "belief", "decision", "skill")


def _normalized(sql):
    return re.sub(r"\s+", "", sql).lower()


def upgrade():
    conn = op.get_bind()
    missing = []
    # Preflight every name before issuing any DDL. Never hide a collision behind
    # IF NOT EXISTS; Inspector reflection does not preserve SQLite expressions.
    for kind in _KINDS:
        table = f"{kind}s"
        name = f"ix_{table}_created_type_id"
        ddl = f"CREATE INDEX {name} ON {table} (created_at, CAST('{kind}' AS TEXT), id)"
        row = conn.exec_driver_sql("SELECT type, tbl_name, sql FROM sqlite_master WHERE name=?", (name,)).first()
        if row is None:
            missing.append(ddl)
            continue
        keys = conn.exec_driver_sql(f"PRAGMA index_xinfo('{name}')").all()
        expected_keys = [(0, "created_at", 0, "BINARY"), (1, None, 0, "BINARY"), (2, "id", 0, "BINARY")]
        actual_keys = [(r[0], r[2], r[3], r[4]) for r in keys if r[5]]
        if (
            row[0] != "index"
            or row[1] != table
            or row[2] is None
            or re.findall(r"'[^']*'", row[2]) != [f"'{kind}'"]
            or _normalized(row[2]) != _normalized(ddl)
            or actual_keys != expected_keys
        ):
            raise RuntimeError(f"SLICE-01A index name occupied by a different definition: {name}")
    for ddl in missing:
        conn.exec_driver_sql(ddl)


def downgrade():
    for kind in _KINDS:
        op.get_bind().exec_driver_sql(f"DROP INDEX IF EXISTS ix_{kind}s_created_type_id")
