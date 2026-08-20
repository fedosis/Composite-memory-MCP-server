"""Tests for scripts/backfill_orphan_receipts.py (SPEC: cmms-orphan-facts)."""

import importlib.util
import sqlite3
from pathlib import Path
from types import ModuleType

import pytest

RECEIPT_COLUMNS = (
    "id",
    "memory_type",
    "source",
    "created_by",
    "timestamp",
    "confidence",
    "verification_status",
    "history",
    "updated_at",
    "lifecycle_state",
    "version",
)


def _load_script() -> ModuleType:
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "backfill_orphan_receipts.py"
    spec = importlib.util.spec_from_file_location("backfill_orphan_receipts", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _make_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE facts ("
        "id TEXT PRIMARY KEY, subject TEXT, predicate TEXT, object TEXT, "
        "source TEXT, creator TEXT NOT NULL, created_at TEXT NOT NULL, "
        "updated_at TEXT NOT NULL, confidence REAL NOT NULL, "
        "verification_status TEXT NOT NULL, lifecycle_state TEXT NOT NULL, "
        "version TEXT NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE receipts ("
        "id TEXT PRIMARY KEY, memory_type TEXT NOT NULL, source TEXT NOT NULL, "
        "created_by TEXT NOT NULL, timestamp TEXT NOT NULL, confidence REAL NOT NULL, "
        "verification_status TEXT NOT NULL, history TEXT NOT NULL, updated_at TEXT NOT NULL, "
        "lifecycle_state TEXT NOT NULL, version TEXT NOT NULL)"
    )
    conn.commit()
    conn.close()


def _insert_fact(
    path: Path,
    *,
    fact_id: str,
    source: str | None = "source-a",
    creator: str = "system",
    created_at: str = "2026-08-20 12:34:56.123456",
    updated_at: str = "2026-08-20 12:34:57.123456",
    confidence: float = 0.8,
    verification_status: str = "candidate",
    lifecycle_state: str = "active",
    version: str = "0.1.0",
) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO facts ("
        "id, subject, predicate, object, source, creator, created_at, updated_at, "
        "confidence, verification_status, lifecycle_state, version"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            fact_id,
            f"subject-{fact_id}",
            "is",
            f"object-{fact_id}",
            source,
            creator,
            created_at,
            updated_at,
            confidence,
            verification_status,
            lifecycle_state,
            version,
        ),
    )
    conn.commit()
    conn.close()


def _insert_receipt(path: Path, values: tuple) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO receipts ("
        + ", ".join(RECEIPT_COLUMNS)
        + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        values,
    )
    conn.commit()
    conn.close()


def _receipt_rows(path: Path) -> list[tuple]:
    conn = sqlite3.connect(path)
    rows = conn.execute(
        "SELECT " + ", ".join(RECEIPT_COLUMNS) + " FROM receipts ORDER BY id"
    ).fetchall()
    conn.close()
    return rows


def _orphan_count(path: Path) -> int:
    conn = sqlite3.connect(path)
    count = conn.execute(
        "SELECT COUNT(*) FROM facts f "
        "LEFT JOIN receipts r ON r.id = f.id "
        "WHERE r.id IS NULL"
    ).fetchone()[0]
    conn.close()
    return int(count)


class TestBackfillOrphanReceipts:
    def test_dry_run_writes_nothing(self, tmp_path):
        mod = _load_script()
        db = tmp_path / "memory.db"
        _make_db(db)
        _insert_fact(db, fact_id="fact-1")
        _insert_fact(db, fact_id="fact-2", source="source-b")
        before_receipts = _receipt_rows(db)
        out: list[str] = []

        result = mod.backfill(db, dry_run=True, out=out.append)

        assert result["dry_run"] is True
        assert result["candidate_count"] == 2
        assert result["inserted_count"] == 0
        assert _receipt_rows(db) == before_receipts
        assert _orphan_count(db) == 2
        assert any("orphan receipts before: 2" in line for line in out)
        assert any("would insert: 2" in line for line in out)
        assert any("orphan receipts after: 2" in line for line in out)

    def test_backfill_inserts_missing_receipts_only(self, tmp_path):
        mod = _load_script()
        db = tmp_path / "memory.db"
        _make_db(db)
        _insert_fact(db, fact_id="fact-1")
        _insert_fact(db, fact_id="fact-2")
        _insert_fact(db, fact_id="fact-3")
        existing = (
            "fact-2",
            "fact",
            "source-a",
            "system",
            "2026-08-20 12:34:56.123456",
            0.8,
            "candidate",
            "[]",
            "2026-08-20 20:00:00.000000",
            "active",
            "0.1.0",
        )
        _insert_receipt(db, existing)

        result = mod.backfill(db)
        rows = _receipt_rows(db)

        assert result["candidate_count"] == 2
        assert result["inserted_count"] == 2
        assert len(rows) == 3
        assert existing in rows
        assert _orphan_count(db) == 0

    def test_inserted_fields_match_fact_fields(self, tmp_path):
        mod = _load_script()
        db = tmp_path / "memory.db"
        _make_db(db)
        _insert_fact(
            db,
            fact_id="fact-9",
            source="curiosity-worker/research",
            creator="system",
            created_at="2026-08-13 09:10:11.123456",
            updated_at="2026-08-13 09:12:13.123456",
            confidence=0.42,
            verification_status="candidate",
            lifecycle_state="active",
            version="0.1.0",
        )

        result = mod.backfill(db, now="2026-08-20 22:33:44.555666")

        assert result["inserted_count"] == 1
        row = _receipt_rows(db)[0]
        assert row == (
            "fact-9",
            "fact",
            "curiosity-worker/research",
            "system",
            "2026-08-13 09:10:11.123456",
            0.42,
            "candidate",
            "[]",
            "2026-08-20 22:33:44.555666",
            "active",
            "0.1.0",
        )

    def test_rerun_is_idempotent(self, tmp_path):
        mod = _load_script()
        db = tmp_path / "memory.db"
        _make_db(db)
        _insert_fact(db, fact_id="fact-1")
        _insert_fact(db, fact_id="fact-2")

        first = mod.backfill(db, now="2026-08-20 10:00:00.000000")
        second = mod.backfill(db, now="2026-08-20 11:00:00.000000")

        assert first["inserted_count"] == 2
        assert second["candidate_count"] == 0
        assert second["inserted_count"] == 0
        assert len(_receipt_rows(db)) == 2
        assert _orphan_count(db) == 0

    def test_null_source_fails_and_rolls_back(self, tmp_path):
        mod = _load_script()
        db = tmp_path / "memory.db"
        _make_db(db)
        _insert_fact(db, fact_id="fact-bad", source=None)

        with pytest.raises(ValueError, match="NULL source"):
            mod.backfill(db)

        assert _receipt_rows(db) == []
        assert _orphan_count(db) == 1

    def test_before_after_counts_reported(self, tmp_path):
        mod = _load_script()
        db = tmp_path / "memory.db"
        _make_db(db)
        _insert_fact(db, fact_id="fact-1")
        out: list[str] = []

        result = mod.backfill(db, now="2026-08-20 01:02:03.000004", out=out.append)

        assert result["before_orphan_count"] == 1
        assert result["after_orphan_count"] == 0
        assert result["inserted_count"] == 1
        joined = "\n".join(out)
        assert "orphan receipts before: 1" in joined
        assert "inserted receipts: 1" in joined
        assert "orphan receipts after: 0" in joined
