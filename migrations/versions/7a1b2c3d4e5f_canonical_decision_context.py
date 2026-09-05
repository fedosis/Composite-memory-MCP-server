"""Canonical decision context backfill (DB-4)

Revision ID: 7a1b2c3d4e5f
Revises: 6a7b8c9d0e1f
Create Date: 2026-09-05 16:30:00.000000

Why this migration exists
-------------------------
DB-4: context was normalized in different ways on different paths. The dedup
key (``decision_dedup_key``) and ``find_existing`` strip the context with
PYTHON ``str.strip()`` semantics, while ``DecisionORM`` stored the RAW context
and the partial unique index ``uq_decisions_context_dedup_active`` indexed the
RAW value. So a row stored with ``context=' ctx '`` was invisible to a
normalized search for ``'ctx'`` and could coexist with the canonical row —
the unique index never saw a collision.

The fix makes ``context`` a CANONICAL column: every seam (ORM write,
repository update/search, dedup key) and every legacy row (this backfill)
must use the exact same contract:

    canonical_context(s) = str(s or "").strip()

Python ``.strip()`` is NOT the same as SQLite ``trim()``: ``trim()`` removes
only ASCII spaces, while Python strips tabs, CR/LF, NBSP, em-space and the
other Unicode whitespace. This migration therefore performs the backfill in
Python, row by row (chunked), and NEVER uses ``trim()``. The contract must
stay in sync with ``storage.dedup.canonical_context``.

Keeper / reference policy (fixed before implementation)
-------------------------------------------------------
- Only ACTIVE rows (candidate/validated/active) participate in collision
  resolution — the same predicate as the partial unique index (W3). Their
  duplicates are resolved DETERMINISTICALLY: keeper = highest confidence,
  then newest ``created_at``, then highest ``id`` (same ordering as
  ``find_existing``). Non-keeper ACTIVE rows are deleted together with their
  own artifacts (receipts ``memory_type='decision'``, outbox
  ``record_type='decision'``): each duplicate's artifact is itself a
  duplicate artifact of the same normalized ingestion event, and reparenting
  it would misattribute provenance; deleting it leaves no dangling
  ``record_id``.
- archived/rejected/inactive rows are NEVER deleted: their context is only
  canonicalized in place and their references are fully preserved. A group of
  two archived + one active stays complete.
- Everything runs in ONE transaction (the alembic env wraps the whole run):
  collisions are resolved BEFORE the canonical ``UPDATE``, so the global
  update can never violate the established partial unique index (a blind
  ``UPDATE ... SET context = trim(context)`` over raw collisions would fail).

Steps:
  1. Load all decision rows, compute the canonical context in Python, and
     detect canonical (context, dedup_key) collisions among ACTIVE rows.
  2. Delete redundant ACTIVE rows (keeper policy above) with their own
     receipts/outbox artifacts — freeing the index slots that the backfill
     UPDATE would otherwise collide on.
  3. Backfill ``context`` to the canonical value on every remaining row
     (active and inactive alike), chunked with bound parameters.
  4. Verify no ACTIVE canonical collisions remain (the partial unique index
     then guarantees them forever).

No schema change: the partial unique index created by 6a7b8c9d0e1f already
operates on ``(context, dedup_key)`` and now sees canonical values.

The revision is idempotent by alembic version stamping (runs exactly once per
database history, inside the single migration transaction).
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7a1b2c3d4e5f"
down_revision: Union[str, Sequence[str], None] = "6a7b8c9d0e1f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

#: Lifecycle states participating in the partial unique index / dedup (W3).
_ACTIVE_STATES = ("candidate", "validated", "active")

#: Batch size for IN-clause and UPDATE chunking (SQLite bound-param limit).
_CHUNK_SIZE = 500


def _canonical_context(context: object) -> str:
    """Python .strip() semantics — MUST mirror storage.dedup.canonical_context.

    Intentionally NOT SQLite trim(): trim() only removes ASCII spaces; tabs,
    newlines and Unicode whitespace are part of the DB-4 bug this migration
    fixes.
    """
    return str(context or "").strip()


def _delete_in_chunks(bind, sql_template: str, ids: list[str]) -> None:
    for i in range(0, len(ids), _CHUNK_SIZE):
        chunk = ids[i : i + _CHUNK_SIZE]
        names = [f"p{j}" for j in range(len(chunk))]
        placeholders = ",".join(f":{n}" for n in names)
        bind.execute(
            sa.text(sql_template.format(placeholders=placeholders)),
            dict(zip(names, chunk)),
        )


def upgrade() -> None:
    """Backfill canonical context; resolve active collisions first."""
    bind = op.get_bind()

    columns = {column["name"] for column in sa.inspect(bind).get_columns("decisions")}
    if "context" not in columns or "dedup_key" not in columns:
        # Cannot be reached after 6a7b8c9d0e1f; guard keeps the migration safe
        # if it is ever run against a hand-crafted schema.
        return

    rows = bind.execute(
        sa.text(
            "SELECT id, context, dedup_key, confidence, created_at, lifecycle_state "
            "FROM decisions"
        )
    ).fetchall()

    # 1+2. Detect canonical collisions among ACTIVE rows and choose the
    # deterministic keeper; schedule the redundant ACTIVE rows for deletion.
    groups: dict[tuple[str, str], list] = {}
    for r in rows:
        groups.setdefault((_canonical_context(r.context), r.dedup_key), []).append(r)

    delete_ids: list[str] = []
    for key, group in groups.items():
        active_rows = [r for r in group if r.lifecycle_state in _ACTIVE_STATES]
        if len(active_rows) <= 1:
            continue

        def sort_key(r):
            return (r.confidence, r.created_at, r.id)

        keeper = max(active_rows, key=sort_key)
        delete_ids.extend(r.id for r in active_rows if r.id != keeper.id)

    if delete_ids:
        _delete_in_chunks(
            bind,
            "DELETE FROM receipts WHERE memory_type='decision' "
            "AND id IN ({placeholders})",
            delete_ids,
        )
        _delete_in_chunks(
            bind,
            "DELETE FROM outbox_entries WHERE record_type='decision' "
            "AND record_id IN ({placeholders})",
            delete_ids,
        )
        _delete_in_chunks(
            bind,
            "DELETE FROM decisions WHERE id IN ({placeholders})",
            delete_ids,
        )

    # 3. Backfill the canonical context on every remaining row. Collisions are
    # already resolved above, so this UPDATE cannot violate the partial unique
    # index. Chunked executemany keeps the migration O(rows), not O(rows^2).
    updates: list[tuple[str, str]] = [
        (canonical, row_id)
        for row_id, raw, _, _, _, _ in rows
        if (canonical := _canonical_context(raw)) != raw
    ]
    for i in range(0, len(updates), _CHUNK_SIZE):
        bind.execute(
            sa.text("UPDATE decisions SET context = :c WHERE id = :id"),
            [{"c": c, "id": rid} for c, rid in updates[i : i + _CHUNK_SIZE]],
        )

    # 4. Postcondition: no two ACTIVE rows share a canonical key. The partial
    # unique index would reject such a state anyway, but assert it cheaply so
    # a logic bug fails with a migration error instead of an index error later.
    collisions = bind.execute(
        sa.text(
            "SELECT context, dedup_key, COUNT(*) FROM decisions "
            "WHERE lifecycle_state IN ('candidate', 'validated', 'active') "
            "GROUP BY context, dedup_key HAVING COUNT(*) > 1 LIMIT 1"
        )
    ).fetchall()
    if collisions:
        raise RuntimeError(
            f"canonical decision context migration left an ACTIVE collision: {collisions[0]!r}"
        )


def downgrade() -> None:
    raise RuntimeError(
        "Canonical decision context migration is irreversible (data backfill); "
        "restore a database backup instead"
    )
