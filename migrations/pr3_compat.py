"""Minimal compatibility DDL for the two historical Alembic tracks.

Existing application-created tables are retained; only missing historical
objects are created. This is not a general schema-drift repair mechanism.
"""

import sqlalchemy as sa
from alembic import op


def create_table_if_missing(name, *elements, **kwargs):
    inspector = sa.inspect(op.get_bind())
    if inspector.has_table(name):
        required = {e.name for e in elements if isinstance(e, sa.Column)}
        actual = {c["name"] for c in inspector.get_columns(name)}
        if not required <= actual:
            raise RuntimeError(f"PR-3 incompatible existing table {name}: missing {sorted(required - actual)}")
        return
    op.create_table(name, *elements, **kwargs)


def create_index_if_missing(name, table_name, columns, **kwargs):
    existing = {i["name"] for i in sa.inspect(op.get_bind()).get_indexes(table_name)}
    if name not in existing:
        op.create_index(name, table_name, columns, **kwargs)
