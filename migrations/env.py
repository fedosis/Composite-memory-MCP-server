"""Obsolete migration entrypoint.

PR-3 has one official Alembic environment at ``alembic/env.py``.  Keeping a
second executable environment under ``migrations/`` would create a divergent
graph, so callers must use ``alembic.ini`` and its configured script location.
"""

raise RuntimeError(
    "migrations/env.py is obsolete; use the official Alembic entrypoint "
    "alembic/env.py via alembic.ini"
)
