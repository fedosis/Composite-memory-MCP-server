"""Add unique constraint on (context, dedup_key) for decisions

Revision ID: 6a7b8c9d0e1f
Revises: 5d4e3c2b1a0f
Create Date: 2026-08-31 14:50:00.000000

Why this migration exists
-------------------------
B1: the write-path dedup (find_existing -> skip) is a plain check-then-insert;
two concurrent learn() calls can both pass the check before either commits and
then both insert. This migration adds a storage-level uniqueness guard so the
race loser fails the insert instead of creating a duplicate.

W1: the exact (context, choice) key misses the production recurrence pattern
(the same decision re-ingested with a growing parenthetical in `choice`). The
unique key is therefore the NORMALIZED key: (context, dedup_key) where
dedup_key is the whitespace-collapsed 200-char prefix of choice — the same key
used by DecisionRepository.find_existing() and the get_context read-path
dedup.

W3: the index is PARTIAL — it only constrains ACTIVE lifecycle states
(candidate/validated/active). A rejected/archived row must not block
re-ingestion of the same decision.

Steps:
  1. Add the `dedup_key` column (NOT NULL, default '').
  2. Backfill `dedup_key` for existing rows using the same normalization as
     storage.dedup (inlined here so the migration stays self-contained).
  3. Deduplicate existing rows by (context, dedup_key): keep the best row per
     key (active first, then confidence desc, created_at desc, id desc) and
     delete the rest together with their receipts and outbox entries —
     required so the unique index can be created on any pre-existing data.
  4. Create the partial unique index.

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "6a7b8c9d0e1f"
down_revision: Union[str, Sequence[str], None] = "5d4e3c2b1a0f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

#: Must stay in sync with storage/dedup.py (kept inline so the migration is
#: self-contained and immune to later code changes).
_DEDUP_PREFIX_LEN = 200
_ACTIVE_STATES = ("candidate", "validated", "active")

#: Batch size for IN-clause queries (SQLite variable limit is ~999 by default).
_CHUNK_SIZE = 500


def _normalize_choice(choice: str) -> str:
    """Whitespace-collapse + truncate to a stable prefix (see storage.dedup)."""
    if choice is None:
        return ""
    return " ".join(str(choice).split())[:_DEDUP_PREFIX_LEN]


def _delete_in_chunks(bind, sql_template: str, ids: list[str]) -> None:
    """Execute a DELETE/COUNT with an IN-clause in chunks (N5: SQLite vars)."""
    for i in range(0, len(ids), _CHUNK_SIZE):
        chunk = ids[i : i + _CHUNK_SIZE]
        names = [f"p{j}" for j in range(len(chunk))]
        placeholders = ",".join(f":{n}" for n in names)
        bind.execute(
            sa.text(sql_template.format(placeholders=placeholders)),
            dict(zip(names, chunk)),
        )


def upgrade() -> None:
    """Add dedup_key column, backfill, dedupe, and create the partial unique index."""
    bind = op.get_bind()

    # 1. Column (NOT NULL with a constant default — SQLite ADD COLUMN requires
    #    a default for NOT NULL columns).
    op.add_column(
        "decisions",
        sa.Column("dedup_key", sa.String(), nullable=False, server_default=""),
    )

    # 2. Backfill.
    rows = bind.execute(
        sa.text("SELECT id, choice FROM decisions")
    ).fetchall()
    for row_id, choice in rows:
        bind.execute(
            sa.text("UPDATE decisions SET dedup_key = :k WHERE id = :id"),
            {"k": _normalize_choice(choice), "id": row_id},
        )

    # 3. Deduplicate by (context, dedup_key) so the unique index can be
    #    created. Keep the best row per key: ACTIVE rows first, then higher
    #    confidence, then newest (created_at DESC, id DESC).
    decision_rows = bind.execute(
        sa.text(
            "SELECT id, context, dedup_key, confidence, created_at, lifecycle_state "
            "FROM decisions"
        )
    ).fetchall()
    groups: dict[tuple[str, str], list] = {}
    for r in decision_rows:
        groups.setdefault((r.context, r.dedup_key), []).append(r)

    delete_ids: list[str] = []
    for key, group in groups.items():
        if len(group) <= 1:
            continue

        def sort_key(r):
            return (
                r.lifecycle_state in _ACTIVE_STATES,
                r.confidence,
                r.created_at,
                r.id,
            )

        keeper = max(group, key=sort_key)
        delete_ids.extend(r.id for r in group if r.id != keeper.id)

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

    # 4. Partial unique index — only ACTIVE rows participate (W3).
    op.create_index(
        "uq_decisions_context_dedup_active",
        "decisions",
        ["context", "dedup_key"],
        unique=True,
        sqlite_where=sa.text(
            "lifecycle_state IN ('candidate', 'validated', 'active')"
        ),
    )


def downgrade() -> None:
    """Drop the unique index and the dedup_key column."""
    op.drop_index("uq_decisions_context_dedup_active", table_name="decisions")
    op.drop_column("decisions", "dedup_key")
