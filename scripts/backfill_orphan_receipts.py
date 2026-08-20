#!/usr/bin/env python3
"""Backfill missing receipts for existing facts.

This maintenance script reconstructs ``receipts`` rows for facts that already
exist in SQLite but have no matching receipt by id. It is SQL-only, wrapped in a
single transaction, supports ``--dry-run``, and is idempotent.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

SELECT_ORPHAN_FACTS_SQL = """
SELECT
    f.id,
    f.source,
    f.creator,
    f.created_at,
    f.updated_at,
    f.confidence,
    f.verification_status,
    f.lifecycle_state,
    f.version
FROM facts f
LEFT JOIN receipts r ON r.id = f.id
WHERE r.id IS NULL
ORDER BY f.created_at, f.id
""".strip()

COUNT_ORPHANS_SQL = """
SELECT COUNT(*)
FROM facts f
LEFT JOIN receipts r ON r.id = f.id
WHERE r.id IS NULL
""".strip()

INSERT_RECEIPTS_SQL = """
INSERT INTO receipts (
    id,
    memory_type,
    source,
    created_by,
    timestamp,
    confidence,
    verification_status,
    history,
    updated_at,
    lifecycle_state,
    version
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""".strip()

REQUIRED_FACT_FIELDS = (
    "id",
    "creator",
    "created_at",
    "confidence",
    "verification_status",
    "lifecycle_state",
    "version",
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _open_read_only(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _open_read_write(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _count_orphans(conn: sqlite3.Connection) -> int:
    return int(conn.execute(COUNT_ORPHANS_SQL).fetchone()[0])


def _fetch_orphan_facts(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(conn.execute(SELECT_ORPHAN_FACTS_SQL).fetchall())


def _validate_candidate_rows(rows: list[sqlite3.Row]) -> None:
    for row in rows:
        if row["source"] is None:
            raise ValueError(f"NULL source for orphan fact {row['id']}")
        for field in REQUIRED_FACT_FIELDS:
            if row[field] is None:
                raise ValueError(f"Missing required field {field} for orphan fact {row['id']}")


def _build_receipt_payloads(rows: list[sqlite3.Row], run_now: str) -> list[tuple]:
    payloads: list[tuple] = []
    for row in rows:
        payloads.append(
            (
                row["id"],
                "fact",
                row["source"],
                row["creator"],
                row["created_at"],
                row["confidence"],
                row["verification_status"],
                "[]",
                run_now,
                row["lifecycle_state"],
                row["version"],
            )
        )
    return payloads


def _preview_rows(rows: list[sqlite3.Row], out: Callable[[str], None]) -> None:
    if not rows:
        out("candidate rows: none")
        return
    out("candidate rows:")
    for row in rows:
        out(
            "  - "
            f"id={row['id']} source={row['source']} "
            f"created_at={row['created_at']} confidence={row['confidence']}"
        )


def _default_now() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f")


def backfill(
    db_path: Path | str,
    *,
    dry_run: bool = False,
    now: str | None = None,
    out: Callable[[str], None] = print,
) -> dict:
    """Backfill missing receipt rows for facts.

    Args:
        db_path: SQLite database path.
        dry_run: Preview only; perform no writes.
        now: Optional override for receipts.updated_at (tests). Real runs should
            omit this and use one invocation-wide naive UTC timestamp.
        out: Output callback for status lines.
    """
    db_path = Path(db_path).resolve()
    run_now = now or _default_now()

    if dry_run:
        conn = _open_read_only(db_path)
        try:
            before = _count_orphans(conn)
            rows = _fetch_orphan_facts(conn)
            _validate_candidate_rows(rows)
            out("DRY-RUN — no changes written")
            out(f"orphan receipts before: {before}")
            out(f"would insert: {len(rows)}")
            _preview_rows(rows, out)
            out(f"orphan receipts after: {before}")
            return {
                "dry_run": True,
                "before_orphan_count": before,
                "after_orphan_count": before,
                "candidate_count": len(rows),
                "inserted_count": 0,
                "candidate_ids": [row["id"] for row in rows],
                "updated_at": run_now,
            }
        finally:
            conn.close()

    conn = _open_read_write(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        before = _count_orphans(conn)
        rows = _fetch_orphan_facts(conn)
        _validate_candidate_rows(rows)
        payloads = _build_receipt_payloads(rows, run_now)

        out(f"orphan receipts before: {before}")
        out(f"candidate rows: {len(rows)}")
        _preview_rows(rows, out)

        if not payloads:
            after = _count_orphans(conn)
            if after != 0:
                raise RuntimeError(f"Expected zero orphan facts on no-op run, found {after}")
            conn.commit()
            out("inserted receipts: 0")
            out(f"orphan receipts after: {after}")
            return {
                "dry_run": False,
                "before_orphan_count": before,
                "after_orphan_count": after,
                "candidate_count": 0,
                "inserted_count": 0,
                "candidate_ids": [],
                "updated_at": run_now,
            }

        conn.executemany(INSERT_RECEIPTS_SQL, payloads)
        inserted = conn.total_changes
        after = _count_orphans(conn)

        if inserted != len(payloads):
            raise RuntimeError(f"Inserted {inserted} receipts, expected {len(payloads)}")
        if after != 0:
            raise RuntimeError(f"Post-insert orphan count is {after}, expected 0")

        conn.commit()
        out(f"inserted receipts: {inserted}")
        out(f"orphan receipts after: {after}")
        return {
            "dry_run": False,
            "before_orphan_count": before,
            "after_orphan_count": after,
            "candidate_count": len(rows),
            "inserted_count": inserted,
            "candidate_ids": [row["id"] for row in rows],
            "updated_at": run_now,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    root = _repo_root()
    parser = argparse.ArgumentParser(
        description="Backfill missing receipts for facts in SQLite (transactional, idempotent)."
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=root / "data" / "memory.db",
        help=f"SQLite database path (default: {root / 'data' / 'memory.db'})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview missing receipts without writing any rows",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    db_path = Path(args.db).resolve()
    if not db_path.exists():
        print(f"❌ SQLite database not found: {db_path}", file=sys.stderr)
        return 1

    try:
        backfill(db_path, dry_run=args.dry_run)
    except Exception as exc:
        print(f"❌ Backfill failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
