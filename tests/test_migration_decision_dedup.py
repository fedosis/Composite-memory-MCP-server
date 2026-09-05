"""PR-4 (MIG-2) gate: 6a dedup keeps ONLY candidate/validated/active rows.

The add_decision_unique_constraint migration must deduplicate exclusively
among ACTIVE lifecycle rows (the same predicate as the partial unique index
``uq_decisions_context_dedup_active``). archived/rejected/inactive history —
and its receipts/outbox references — must survive even when it shares a
``(context, dedup_key)`` group with an active row.

The gate runs the OFFICIAL alembic CLI (``alembic upgrade 6a7b8c9d0e1f``) on
a generated collision-rich disposable DB built from the frozen pre-6a schema
fixture (``pr3_5d4e3c2b1a0f.sql``) — the real legacy backups that still exist
are already at revision 6a with their decision history collapsed, so they
cannot exercise the dedup path.

Note on "already applied" revisions: editing a migration that is already
recorded in ``alembic_version`` does NOT re-execute it and does NOT restore
previously deleted history on such DBs; history restoration from a backup is
a separate operator action, out of scope for PR-4.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
PYTHON = REPO / ".venv/bin/python3.12"
if not PYTHON.exists():
    PYTHON = Path(sys.executable)

_ACTIVE = ("candidate", "validated", "active")
_INDEX = "uq_decisions_context_dedup_active"
_REV = "6a7b8c9d0e1f"
_CHUNK = 500


def _ini(tmp_path, db):
    root = Path(os.environ.get("B6_TEST_MIGRATION_ROOT", REPO))
    text = (root / "alembic.ini").read_text().replace("%(here)s", str(root))
    text = text.replace("sqlite:///memory.db", f"sqlite:///{db}")
    path = tmp_path / "alembic.ini"
    path.write_text(text)
    return path


def _run(ini, target=_REV):
    return subprocess.run(
        [str(PYTHON), "-m", "alembic", "-c", str(ini), "upgrade", target],
        cwd=ini.parent,
        env={"HOME": "/tmp", "PATH": "/usr/bin:/bin", "PYTHONPATH": str(REPO)},
        text=True,
        capture_output=True,
        timeout=120,
    )


def _fixture(db):
    """Create the frozen pre-6a schema (decisions without dedup_key/index)."""
    with sqlite3.connect(db) as conn:
        conn.executescript(
            (REPO / "tests/fixtures" / "pr3_5d4e3c2b1a0f.sql").read_text()
        )
        conn.commit()


def _insert(conn, table, values):
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    values = {k: v for k, v in values.items() if k in cols}
    conn.execute(
        f"INSERT INTO {table} ({','.join(values)}) VALUES ({','.join('?' for _ in values)})",
        list(values.values()),
    )


def _seed_collision_rich(db):
    """Seed decisions with active/archived collisions plus references.

    Groups (raw context, dedup_key == choice here):
      g1 'ctx'        -> A1 active(.5, old), A2 active(.9, new) keeper,
                         H1 archived, H2 rejected          [2 archived + 1 active => 3 survive]
      g2 'ctx'        -> H3 archived, H4 inactive          [all-inactive group untouched]
      g3 'ctx2'       -> A3 active(.9, old), A4 active(.7), A5 active(.9, new) keeper
      g4 'ctx3'       -> S1 active singleton (control)
    Every row gets a receipt (memory_type='decision', same id) and an outbox
    entry (record_type='decision') so reference survival can be asserted.
    """
    common = dict(
        source="test",
        creator="test",
        updated_at="2026-01-01 00:00:00",
        verification_status="verified",
        version="1",
        rejected_alternatives="[]",
        reason="r",
    )

    def dec(id_, context, choice, state, confidence, created_at):
        return dict(
            common,
            id=id_,
            context=context,
            choice=choice,
            lifecycle_state=state,
            confidence=confidence,
            created_at=created_at,
        )

    rows = [
        # g1: mixed group — only the ACTIVE rows may be deduplicated.
        dec("g1-a1", "ctx", "choose Caddy", "active", 0.5, "2026-01-01 00:00:00"),
        dec("g1-a2", "ctx", "choose Caddy", "active", 0.9, "2026-01-02 00:00:00"),
        dec("g1-h1", "ctx", "choose Caddy", "archived", 0.8, "2026-01-03 00:00:00"),
        dec("g1-h2", "ctx", "choose Caddy", "rejected", 0.8, "2026-01-04 00:00:00"),
        # g2: all-inactive group shares the key with g1 but must stay complete.
        dec("g2-h3", "ctx", "choose Caddy", "archived", 0.7, "2026-01-05 00:00:00"),
        dec("g2-h4", "ctx", "choose Caddy", "inactive", 0.6, "2026-01-06 00:00:00"),
        # g3: three ACTIVE duplicates -> deterministic keeper (conf .9, newest, id).
        dec("g3-a3", "ctx2", "use Caddy", "active", 0.9, "2026-01-01 00:00:00"),
        dec("g3-a4", "ctx2", "use Caddy", "active", 0.7, "2026-01-02 00:00:00"),
        dec("g3-a5", "ctx2", "use Caddy", "active", 0.9, "2026-01-02 00:00:01"),
        # g4: active singleton control.
        dec("g4-s1", "ctx3", "write decoder", "active", 0.5, "2026-01-01 00:00:00"),
        # archived singleton sharing key with the active singleton (control: both stay).
        dec("g4-h5", "ctx3", "write decoder", "archived", 0.5, "2026-01-02 00:00:00"),
    ]
    with sqlite3.connect(db) as conn:
        for r in rows:
            _insert(conn, "decisions", r)
            _insert(
                conn,
                "receipts",
                dict(
                    id=r["id"],
                    memory_type="decision",
                    source="test",
                    created_by="test",
                    timestamp=r["created_at"],
                    confidence=r["confidence"],
                    verification_status="verified",
                    history="[]",
                    updated_at=r["updated_at"],
                    lifecycle_state=r["lifecycle_state"],
                    version="1",
                ),
            )
            _insert(
                conn,
                "outbox_entries",
                dict(
                    id=f"ob-{r['id']}",
                    record_type="decision",
                    record_id=r["id"],
                    operation="upsert",
                    payload_json="{}",
                    status="pending",
                    retry_count=0,
                    created_at=r["created_at"],
                ),
            )
        conn.commit()


def _decisions(db):
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as c:
        rows = c.execute(
            "SELECT id, lifecycle_state FROM decisions ORDER BY id"
        ).fetchall()
        return rows


def _receipt_ids(db):
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as c:
        return {
            r[0]
            for r in c.execute(
                "SELECT id FROM receipts WHERE memory_type='decision'"
            ).fetchall()
        }


def _outbox_record_ids(db):
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as c:
        return {
            r[0]
            for r in c.execute(
                "SELECT record_id FROM outbox_entries WHERE record_type='decision'"
            ).fetchall()
        }


def test_dedup_preserves_archived_history_and_references(tmp_path):
    db = tmp_path / "collision.db"
    _fixture(db)
    _seed_collision_rich(db)

    run = _run(_ini(tmp_path, db))
    assert run.returncode == 0, run.stderr

    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as c:
        assert c.execute("SELECT version_num FROM alembic_version").fetchall() == [
            (_REV,)
        ]
        assert c.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert c.execute("PRAGMA foreign_key_check").fetchall() == []
        # The partial unique index must exist exactly as the ORM declares it.
        assert c.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name=?", (_INDEX,)
        ).fetchall()

    remaining = dict(_decisions(db))

    # g1: "две archived + одна active остаётся полной" -> all three survive.
    assert remaining.get("g1-h1") == "archived"
    assert remaining.get("g1-h2") == "rejected"
    # The active collision in g1 was resolved deterministically: keeper g1-a2
    # (highest confidence), loser g1-a1 removed.
    assert "g1-a1" not in remaining
    assert remaining.get("g1-a2") == "active"

    # g2: an all-inactive group is never touched by dedup.
    assert remaining.get("g2-h3") == "archived"
    assert remaining.get("g2-h4") == "inactive"

    # g3: only ACTIVE duplicates removed; deterministic keeper g3-a5 wins
    # (confidence .9, newest created_at, highest id among the .9 ties).
    assert remaining.get("g3-a5") == "active"
    assert "g3-a3" not in remaining
    assert "g3-a4" not in remaining

    # Control: active singleton + archived singleton sharing the raw key stay.
    assert remaining.get("g4-s1") == "active"
    assert remaining.get("g4-h5") == "archived"

    # References: receipts/outbox survive for every remaining decision; the
    # artifacts of removed ACTIVE duplicates are removed with them (no dangling
    # record_id pointing at a deleted decision).
    receipts = _receipt_ids(db)
    outbox = _outbox_record_ids(db)
    assert receipts == set(remaining)
    assert outbox == set(remaining)
    # Sanity: the fixture really created those references (test is not vacuous).
    # 11 seeded rows; 3 ACTIVE duplicates removed (g1-a1, g3-a3, g3-a4); 8 survive.
    assert len(remaining) == 8
    assert len(receipts) == 8
    assert len(outbox) == 8


def test_index_blocks_active_duplicate_but_allows_archived(tmp_path):
    """After 6a the partial index enforces W3: active dupes fail, history is re-ingestable."""
    db = tmp_path / "index.db"
    _fixture(db)
    _seed_collision_rich(db)
    run = _run(_ini(tmp_path, db))
    assert run.returncode == 0, run.stderr

    with sqlite3.connect(db) as conn:
        # Active re-ingestion of an existing (context, dedup_key) is rejected.
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO decisions (id, context, choice, rejected_alternatives, reason,"
                " confidence, source, creator, created_at, updated_at, verification_status,"
                " lifecycle_state, version, dedup_key) VALUES"
                " ('new-active', 'ctx', 'choose Caddy', '[]', 'r', 0.9, 'test', 'test',"
                " '2026-01-10 00:00:00', '2026-01-10 00:00:00', 'verified', 'active', '1',"
                " 'choose Caddy')"
            )
        # An archived row with the same key is allowed (partial index skips it).
        conn.execute(
            "INSERT INTO decisions (id, context, choice, rejected_alternatives, reason,"
            " confidence, source, creator, created_at, updated_at, verification_status,"
            " lifecycle_state, version, dedup_key) VALUES"
            " ('new-archived', 'ctx', 'choose Caddy', '[]', 'r', 0.9, 'test', 'test',"
            " '2026-01-10 00:00:00', '2026-01-10 00:00:00', 'verified', 'archived', '1',"
            " 'choose Caddy')"
        )
        conn.commit()
