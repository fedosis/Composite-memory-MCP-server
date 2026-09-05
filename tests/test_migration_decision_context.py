"""PR-8 (DB-4) gate: canonical decision context via the new revision after 6a.

Context is normalized with PYTHON ``str.strip()`` semantics (Unicode
whitespace — space, tab, newline, NBSP, em space, ...). SQLite ``trim()`` is
NOT equivalent and the migration must repeat the Python contract, not SQL
``trim``. The migration runs in ONE transaction: it first detects canonical
collisions among ACTIVE rows, resolves them deterministically (keeper policy
identical to the 6a fix: highest confidence, then newest created_at, then
highest id — removing only the redundant ACTIVE row together with its own
receipt/outbox artifacts), then backfills the canonical context on every
remaining row (active and inactive). archived/rejected/inactive history and
its references are never deleted.

Gate DBs are generated from the frozen post-6a schema fixture
(``pr3_6a7b8c9d0e1f.sql``, which already carries dedup_key + the partial
unique index) because the surviving real backups are already collapsed.
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
_PR4_REV = "6a7b8c9d0e1f"
_NEW_REV = "7a1b2c3d4e5f"
_HEAD = "0005"


def _canonical(context: object) -> str:
    """Python .strip() — MUST mirror storage.dedup.canonical_context."""
    return str(context or "").strip()


def _ini(tmp_path, db):
    root = Path(os.environ.get("B6_TEST_MIGRATION_ROOT", REPO))
    text = (root / "alembic.ini").read_text().replace("%(here)s", str(root))
    text = text.replace("sqlite:///memory.db", f"sqlite:///{db}")
    path = tmp_path / "alembic.ini"
    path.write_text(text)
    return path


def _run(ini, target=_NEW_REV):
    return subprocess.run(
        [str(PYTHON), "-m", "alembic", "-c", str(ini), "upgrade", target],
        cwd=ini.parent,
        env={"HOME": "/tmp", "PATH": "/usr/bin:/bin", "PYTHONPATH": str(REPO)},
        text=True,
        capture_output=True,
        timeout=120,
    )


def _fixture(db):
    """Frozen schema at revision 6a (dedup_key column + partial unique index)."""
    with sqlite3.connect(db) as conn:
        conn.executescript(
            (REPO / "tests/fixtures" / "pr3_6a7b8c9d0e1f.sql").read_text()
        )
        conn.commit()


def _insert(conn, table, values):
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    values = {k: v for k, v in values.items() if k in cols}
    conn.execute(
        f"INSERT INTO {table} ({','.join(values)}) VALUES ({','.join('?' for _ in values)})",
        list(values.values()),
    )


def _seed(db):
    """Seed ACTIVE/archived collisions that only canonicalization reveals.

    RAW contexts must be distinct for ACTIVE rows (the pre-X partial unique
    index is on the RAW (context, dedup_key)); canonical forms collide.

      K1  ('ctx','pick Caddy'): a1 active(.5,old,'ctx'), a2 active(.9,new,
          ' ctx ') keeper, a3 active(.8,'\\tctx\\n'), h1 archived('ctx'),
          h2 archived('\\u00a0ctx\\u00a0')
      K2  ('','empty'): e1 active(''), e2 active(.9,'   ') keeper,
          e3 inactive(' ')
      K3  ('other ctx','other choice'): o1 active canonical control,
          arch1 archived(' other ctx ')
    Every row gets its own receipt/outbox artifact.
    """
    common = dict(
        choice="pick Caddy",
        dedup_key="pick Caddy",
        rejected_alternatives="[]",
        reason="r",
        source="test",
        creator="test",
        updated_at="2026-01-01 00:00:00",
        verification_status="verified",
        version="1",
    )

    def dec(id_, context, state, confidence, created_at, choice="pick Caddy"):
        return dict(
            common,
            id=id_,
            context=context,
            choice=choice,
            dedup_key=" ".join(choice.split())[:200],
            lifecycle_state=state,
            confidence=confidence,
            created_at=created_at,
        )

    rows = [
        dec("a1", "ctx", "active", 0.5, "2026-01-01 00:00:00"),
        dec("a2", " ctx ", "active", 0.9, "2026-01-03 00:00:00"),
        dec("a3", "\tctx\n", "active", 0.8, "2026-01-02 00:00:00"),
        dec("h1", "ctx", "archived", 0.7, "2026-01-04 00:00:00"),
        dec("h2", "\u00a0ctx\u00a0", "archived", 0.7, "2026-01-05 00:00:00"),
        dec("e1", "", "active", 0.5, "2026-01-01 00:00:00", choice="empty"),
        dec("e2", "   ", "active", 0.9, "2026-01-02 00:00:00", choice="empty"),
        dec("e3", " ", "inactive", 0.5, "2026-01-03 00:00:00", choice="empty"),
        dec("o1", "other ctx", "active", 0.6, "2026-01-01 00:00:00", choice="other choice"),
        dec(
            "arch1",
            " other ctx ",
            "archived",
            0.6,
            "2026-01-02 00:00:00",
            choice="other choice",
        ),
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


def _snapshot(db):
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as c:
        rows = c.execute(
            "SELECT id, context, dedup_key, lifecycle_state FROM decisions ORDER BY id"
        ).fetchall()
        receipts = {
            r[0]
            for r in c.execute(
                "SELECT id FROM receipts WHERE memory_type='decision'"
            ).fetchall()
        }
        outbox = {
            r[0]
            for r in c.execute(
                "SELECT record_id FROM outbox_entries WHERE record_type='decision'"
            ).fetchall()
        }
        integrity = c.execute("PRAGMA integrity_check").fetchall()
        fk = c.execute("PRAGMA foreign_key_check").fetchall()
        index = c.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name=?", (_INDEX,)
        ).fetchall()
        version = c.execute("SELECT version_num FROM alembic_version ORDER BY 1").fetchall()
        return rows, receipts, outbox, integrity, fk, index, version


def test_canonical_context_backfill_on_collision_rich_db(tmp_path):
    db = tmp_path / "collision.db"
    _fixture(db)
    _seed(db)
    # RED sanity: fixture really seeds collisions (test is not vacuous).
    rows_before = _snapshot(db)[0]
    assert len(rows_before) == 10

    run = _run(_ini(tmp_path, db))
    assert run.returncode == 0, run.stderr

    rows, receipts, outbox, integrity, fk, index, version = _snapshot(db)
    by_id = {r[0]: r for r in rows}
    assert version == [(_NEW_REV,)]
    assert integrity == [("ok",)]
    assert fk == []
    assert index  # partial unique index survives the data migration

    # Deterministic ACTIVE collision resolution: a2 wins K1 (.9), e2 wins K2
    # (.9); redundant actives a1/a3/e1 removed; ALL inactive rows preserved.
    remaining_ids = set(by_id)
    assert remaining_ids == {
        "a2",
        "h1",
        "h2",
        "e2",
        "e3",
        "o1",
        "arch1",
    }
    assert by_id["a2"][1] == "ctx"
    assert by_id["h1"][1] == "ctx"  # archived history canonicalized, not deleted
    assert by_id["h2"][1] == "ctx"  # NBSP-padded archived row preserved
    assert by_id["e2"][1] == ""  # '   ' -> ''
    assert by_id["e3"][1] == ""  # inactive preserved
    assert by_id["o1"][1] == "other ctx"
    assert by_id["arch1"][1] == "other ctx"

    # Every stored context equals the Python-canonical contract (no raw
    # whitespace variants survive the backfill; SQLite trim would have missed
    # tab/NBSP/newline, Python strip must not).
    for row in rows:
        assert row[1] == _canonical(row[1]), row

    # No two ACTIVE rows share a canonical (context, dedup_key).
    actives = [r for r in rows if r[3] in _ACTIVE]
    keys = [(r[1], r[2]) for r in actives]
    assert len(keys) == len(set(keys)), keys

    # References: every remaining decision keeps its receipt/outbox; the
    # artifacts of removed ACTIVE duplicates are gone (no dangling record_id).
    assert receipts == remaining_ids
    assert outbox == remaining_ids
    assert len(receipts) == 7
    assert len(outbox) == 7


def test_upgrade_collision_rich_copy_to_single_head(tmp_path):
    """Full production path: 6a collision-rich copy -> head (0005)."""
    db = tmp_path / "head.db"
    _fixture(db)
    _seed(db)
    run = _run(_ini(tmp_path, db), target="head")
    assert run.returncode == 0, run.stderr
    rows, receipts, outbox, integrity, fk, index, version = _snapshot(db)
    assert version == [(_HEAD,)]
    assert integrity == [("ok",)]
    assert fk == []
    assert index
    remaining_ids = {r[0] for r in rows}
    assert remaining_ids == {"a2", "h1", "h2", "e2", "e3", "o1", "arch1"}
    assert receipts == remaining_ids
    assert outbox == remaining_ids


def test_index_blocks_active_duplicate_after_canonicalization(tmp_path):
    """After X the stored contexts are canonical, so the partial unique index
    guards canonical (context, dedup_key): a raw insert of the SAME canonical
    key is rejected while an archived row with that key remains allowed (W3).
    (Padding variants are rejected earlier by the application write path —
    the repository canonicalizes before insert; see test_storage_layer.)
    """
    db = tmp_path / "guard.db"
    _fixture(db)
    _seed(db)
    run = _run(_ini(tmp_path, db))
    assert run.returncode == 0, run.stderr

    with sqlite3.connect(db) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO decisions (id, context, choice, rejected_alternatives,"
                " reason, confidence, source, creator, created_at, updated_at,"
                " verification_status, lifecycle_state, version, dedup_key) VALUES"
                " ('dup-active', 'ctx', 'pick Caddy', '[]', 'r', 0.9, 'test',"
                " 'test', '2026-02-01 00:00:00', '2026-02-01 00:00:00', 'verified',"
                " 'active', '1', 'pick Caddy')"
            )
        # Archived row with same canonical key remains re-ingestable (W3).
        conn.execute(
            "INSERT INTO decisions (id, context, choice, rejected_alternatives,"
            " reason, confidence, source, creator, created_at, updated_at,"
            " verification_status, lifecycle_state, version, dedup_key) VALUES"
            " ('dup-archived', 'ctx', 'pick Caddy', '[]', 'r', 0.9, 'test',"
            " 'test', '2026-02-01 00:00:00', '2026-02-01 00:00:00', 'verified',"
            " 'archived', '1', 'pick Caddy')"
        )
        conn.commit()
