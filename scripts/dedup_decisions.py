#!/usr/bin/env python3
"""Deduplicate the decisions table in the CMMS production DB.

Background
----------
The decisions table accumulated duplicate rows because the ingestion write
path created a fresh row for every (context, choice) pair with no existence
check, and the same real decision was re-ingested across turns (hermes_turn_*)
and by tests. Additionally, test runs leaked junk rows into the production DB
under sources e2e-test / round-trip-test / test-session-42.

NOTE ON SCOPE: this script collapses EXACT (context, choice) duplicates. The
near-duplicate variants of the same decision (same normalized key, different
tail — see storage/dedup.py) are collapsed by the
add_decision_unique_constraint migration (existing data) and by the write/read
paths (new data), not by this script.

This script:
  1. Backs up the DB first (sqlite3 backup API — WAL-safe consistent snapshot).
     The backup happens before the write lock is taken, so a write landing
     between backup and lock is included in the transaction but NOT in the
     backup — a tiny TOCTOU window, acceptable for a safety net.
  2. Keeps ONE row per exact (context, choice) pair — the most recent
     (created_at DESC, tie-break by id). Rows whose pair exists ONLY under
     test sources are removed entirely (test junk in prod).
  3. Deletes the matching receipts (memory_type='decision') and outbox entries
     (record_type='decision'). NOTE: this only prevents FUTURE re-indexing of
     the deleted rows. Already-indexed graph nodes/edges and vector entries
     are NOT removed (the outbox worker has no delete operation) and may
     remain orphaned until a full reindex (scripts/reindex_graph.py).
     Acceptable for this cleanup.
  4. Runs inside a single transaction — any failure rolls back everything.

Usage:
    python scripts/dedup_decisions.py [--db path/to/memory.db] [--dry-run]
"""

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

# Sources known to be test runners that leaked rows into the production DB.
TEST_SOURCES = {"e2e-test", "round-trip-test", "test-session-42"}

#: Batch size for IN-clause queries (SQLite variable limit is ~999 by default).
_CHUNK_SIZE = 500


def backup_db(db_path: Path) -> Path:
    """Create a WAL-safe backup of the DB next to it.

    Uses the sqlite3 online backup API so the snapshot is consistent even if
    the DB is in WAL mode (plain ``cp`` could miss WAL frames).
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    backup_path = db_path.with_name(f"{db_path.name}.bak-{timestamp}")
    if backup_path.exists():
        # Don't clobber an earlier backup from the same day.
        backup_path = db_path.with_name(
            f"{db_path.name}.bak-{timestamp}-{int(datetime.now().timestamp())}"
        )
    src = sqlite3.connect(str(db_path))
    try:
        dst = sqlite3.connect(str(backup_path))
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()
    print(f"[backup] created {backup_path} ({backup_path.stat().st_size} bytes)")
    return backup_path


def count_matching(conn: sqlite3.Connection, sql_template: str, ids: list[str]) -> int:
    """COUNT with an IN-clause, executed in chunks (N5: SQLite variable limit)."""
    total = 0
    for i in range(0, len(ids), _CHUNK_SIZE):
        chunk = ids[i : i + _CHUNK_SIZE]
        placeholders = ",".join("?" * len(chunk))
        total += conn.execute(
            sql_template.format(placeholders=placeholders), chunk
        ).fetchone()[0]
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        default=str(Path(__file__).resolve().parent.parent / "data" / "memory.db"),
        help="Path to the SQLite database (default: repo data/memory.db)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be deleted without modifying the DB",
    )
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    if not db_path.exists():
        print(f"ERROR: database not found: {db_path}", file=sys.stderr)
        return 1

    if not args.dry_run:
        backup_db(db_path)

    # W2: busy_timeout so BEGIN IMMEDIATE waits for a running server's write
    # lock instead of failing instantly with 'database is locked'.
    if args.dry_run:
        # Dry-run is a pure read: open read-only (mode=ro) so it never takes a
        # write lock and never blocks a running server's writers.
        conn = sqlite3.connect(
            f"file:{db_path}?mode=ro", uri=True, timeout=30, isolation_level=None
        )
    else:
        # Autocommit mode; we manage the transaction explicitly.
        conn = sqlite3.connect(str(db_path), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        if not args.dry_run:
            conn.execute("BEGIN IMMEDIATE")
        else:
            # Read-only connection: a plain BEGIN (deferred) or none at all —
            # SELECTs don't need a write lock.
            conn.execute("BEGIN")

        rows = conn.execute(
            "SELECT id, context, choice, source, created_at FROM decisions"
        ).fetchall()
        total = len(rows)
        print(f"[scan] {total} decision rows loaded")

        # Group by exact (context, choice).
        groups: dict[tuple[str, str], list[sqlite3.Row]] = {}
        for row in rows:
            groups.setdefault((row["context"], row["choice"]), []).append(row)

        keep_ids: set[str] = set()
        delete_ids: set[str] = set()
        print("\nper-group plan (kept / deleted):")
        for (context, choice), group in sorted(
            groups.items(), key=lambda kv: -len(kv[1])
        ):
            non_test = [r for r in group if r["source"] not in TEST_SOURCES]
            if non_test:
                # Keep the most recent non-test row (created_at DESC, id
                # tiebreak); a newer test row must not displace a real decision.
                keeper = max(non_test, key=lambda r: (r["created_at"], r["id"]))
                keep_ids.add(keeper["id"])
            else:
                # The pair exists ONLY under test sources — pure test junk.
                keeper = None
            deleted = [
                r["id"] for r in group if keeper is None or r["id"] != keeper["id"]
            ]
            delete_ids.update(deleted)
            print(
                f"  x{len(group):>3}  choice={choice[:60]!r} context_len={len(context)} "
                f"-> keep={keeper['id'][:8] if keeper else 'NONE (test junk)'} "
                f"delete={len(deleted)}"
            )

        # Sanity: every row accounted for exactly once.
        assert len(keep_ids) + len(delete_ids) == total, (
            f"accounting mismatch: keep={len(keep_ids)} delete={len(delete_ids)} "
            f"total={total}"
        )

        delete_list = sorted(delete_ids)
        n_decisions = len(delete_list)
        if n_decisions == 0:
            print("\n[ok] no duplicate decision rows found — nothing to do")
            conn.rollback()
            return 0

        # Matching receipts: id = decision id, memory_type = 'decision'.
        n_receipts = count_matching(
            conn,
            "SELECT COUNT(*) FROM receipts WHERE memory_type='decision' "
            "AND id IN ({placeholders})",
            delete_list,
        )

        # Matching outbox entries: record_type='decision', record_id = decision id.
        n_outbox = count_matching(
            conn,
            "SELECT COUNT(*) FROM outbox_entries WHERE record_type='decision' "
            "AND record_id IN ({placeholders})",
            delete_list,
        )

        print(
            f"\n[totals] decisions: {total} -> {len(keep_ids)} "
            f"(deleting {n_decisions})"
        )
        print(f"[totals] decision receipts to delete: {n_receipts}")
        print(f"[totals] decision outbox entries to delete: {n_outbox}")

        if args.dry_run:
            print("\n[dry-run] no changes written")
            conn.rollback()
            return 0

        conn.executemany(
            "DELETE FROM decisions WHERE id = ?", [(i,) for i in delete_list]
        )
        conn.executemany(
            "DELETE FROM receipts WHERE memory_type='decision' AND id = ?",
            [(i,) for i in delete_list],
        )
        conn.executemany(
            "DELETE FROM outbox_entries "
            "WHERE record_type='decision' AND record_id = ?",
            [(i,) for i in delete_list],
        )

        # Verify the result before committing.
        remaining = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
        remaining_receipts = conn.execute(
            "SELECT COUNT(*) FROM receipts WHERE memory_type='decision'"
        ).fetchone()[0]
        remaining_outbox = conn.execute(
            "SELECT COUNT(*) FROM outbox_entries WHERE record_type='decision'"
        ).fetchone()[0]
        if remaining != len(keep_ids):
            print(
                f"ERROR: post-delete count {remaining} != expected "
                f"{len(keep_ids)} — rolling back",
                file=sys.stderr,
            )
            conn.rollback()
            return 1
        conn.commit()
        print(
            f"\n[committed] decisions={remaining} receipts(decision)="
            f"{remaining_receipts} outbox(decision)={remaining_outbox}"
        )
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
