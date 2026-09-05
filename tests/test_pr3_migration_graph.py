"""PR-3 gate: frozen real legacy schemas, official CLI, disposable DBs only."""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

REPO = Path(__file__).resolve().parents[1]
PYTHON = REPO / ".venv/bin/python3.12"
if not PYTHON.exists():
    PYTHON = Path(sys.executable)
LEGACY_REVISIONS = ("0001", "0002", "0003", "0004", "70e6afc8d15d", "5d4e3c2b1a0f", "6a7b8c9d0e1f", "b2f3a4c5d6e7")
HEAD = "0005"


def _ini(tmp_path, db):
    root = Path(os.environ.get("B5_TEST_MIGRATION_ROOT", REPO))
    text = (root / "alembic.ini").read_text().replace("%(here)s", str(root))
    text = text.replace("sqlite:///memory.db", f"sqlite:///{db}")
    path = tmp_path / "alembic.ini"
    path.write_text(text)
    return path


def _run(ini, command="upgrade", target="head"):
    return subprocess.run(
        [str(PYTHON), "-m", "alembic", "-c", str(ini), command, target],
        cwd=ini.parent,
        env={"HOME": "/tmp", "PATH": "/usr/bin:/bin", "PYTHONPATH": str(REPO)},
        text=True,
        capture_output=True,
        timeout=60,
    )


def _fixture(db, revision):
    with sqlite3.connect(db) as conn:
        conn.executescript((REPO / "tests/fixtures" / f"pr3_{revision}.sql").read_text())


def _seed(db):
    with sqlite3.connect(db) as c:
        tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        common = dict(
            confidence=0.9,
            source="test",
            creator="test",
            created_at="2026-01-01 00:00:00",
            updated_at="2026-01-01 00:00:00",
            verification_status="verified",
            lifecycle_state="active",
            version="1",
        )

        def insert(table, values):
            cols = {r[1] for r in c.execute(f"PRAGMA table_info({table})")}
            values = {k: v for k, v in values.items() if k in cols}
            c.execute(
                f"INSERT INTO {table} ({','.join(values)}) VALUES ({','.join('?' for _ in values)})",
                list(values.values()),
            )

        insert(
            "facts",
            dict(common, id="f1", subject="sentinel", predicate="p", object="o", dedup_key="sentinel\x1fp\x1fo"),
        )
        insert(
            "decisions",
            dict(
                common,
                id="d1",
                context="ctx",
                choice="choice",
                dedup_key="choice",
                rejected_alternatives="[]",
                reason="test",
            ),
        )
        insert(
            "receipts",
            dict(common, id="f1", memory_type="fact", created_by="test", timestamp=common["created_at"], history="[]"),
        )
        if "outbox_entries" in tables:
            insert(
                "outbox_entries",
                dict(
                    id="o1",
                    record_id="f1",
                    record_type="fact",
                    operation="upsert",
                    payload_json="{}",
                    status="pending",
                    retry_count=0,
                    created_at=common["created_at"],
                ),
            )
        if "beliefs" in tables:
            insert(
                "beliefs",
                dict(
                    common,
                    id="b1",
                    proposition="belief sentinel",
                    source_ids="[]",
                    tags="[]",
                    last_reinforced_at=common["created_at"],
                ),
            )
            insert(
                "evidence",
                dict(
                    id="e1",
                    belief_id="b1",
                    source_type="fact",
                    source_id="f1",
                    weight=0.5,
                    contributor="test",
                    created_at=common["created_at"],
                ),
            )
        if "claim_relations" in tables:
            insert(
                "claim_relations",
                dict(source_id="f1", target_id="f1", relation_type="related_to", created_at=common["created_at"]),
            )


def _snapshot(db):
    with sqlite3.connect(db) as c:
        result = {}
        for (name,) in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
            if name == "alembic_version" or "_fts" in name or name.startswith("sqlite_"):
                continue
            columns = [r[1] for r in c.execute(f"PRAGMA table_info({name})")]
            result[name] = (columns, c.execute(f"SELECT {','.join(columns)} FROM {name} ORDER BY 1").fetchall())
        return result


def _assert_head(db, before):
    with sqlite3.connect(db) as c:
        assert c.execute("SELECT version_num FROM alembic_version").fetchall() == [(HEAD,)]
        assert c.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert c.execute("PRAGMA foreign_key_check").fetchall() == []
        tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert {
            "facts",
            "decisions",
            "outbox_entries",
            "beliefs",
            "evidence",
            "claim_relations",
            "facts_fts",
            "beliefs_fts",
        } <= tables
        assert c.execute("PRAGMA foreign_key_list(evidence)").fetchall()[0][2:5] == ("beliefs", "belief_id", "id")
        assert c.execute("SELECT name FROM sqlite_master WHERE name='uq_facts_spo_active'").fetchone()
        assert c.execute("SELECT name FROM sqlite_master WHERE name='uq_decisions_context_dedup_active'").fetchone()
        for table, (columns, rows) in before.items():
            assert c.execute(f"SELECT {','.join(columns)} FROM {table} ORDER BY 1").fetchall() == rows, table
        if before.get("facts", ([], []))[1]:
            assert c.execute("SELECT subject FROM facts_fts WHERE facts_fts MATCH 'sentinel'").fetchall() == [
                ("sentinel",)
            ]
        for fts in ("facts_fts", "beliefs_fts"):
            c.execute(f"INSERT INTO {fts}({fts},rank) VALUES('integrity-check',1)")


def test_single_official_head():
    root = Path(os.environ.get("B5_TEST_MIGRATION_ROOT", REPO))
    graph = ScriptDirectory.from_config(Config(str(root / "alembic.ini")))
    assert graph.get_heads() == [HEAD]
    assert set(LEGACY_REVISIONS) <= {r.revision for r in graph.walk_revisions()}


def test_empty_database_to_head(tmp_path):
    db = tmp_path / "empty.db"
    run = _run(_ini(tmp_path, db))
    assert run.returncode == 0, run.stderr
    _assert_head(db, {})


@pytest.mark.parametrize("revision", LEGACY_REVISIONS)
def test_frozen_legacy_revision_to_head(tmp_path, revision):
    db = tmp_path / "legacy.db"
    _fixture(db, revision)
    _seed(db)
    before = _snapshot(db)
    ini = _ini(tmp_path, db)
    run = _run(ini)
    assert run.returncode == 0, run.stderr
    _assert_head(db, before)
    run = _run(ini)
    assert run.returncode == 0, run.stderr
    _assert_head(db, before)


def test_runtime_precreated_tables_to_head(tmp_path):
    # Actual ORM production table creation, unversioned mixed rollout.
    from sqlalchemy import create_engine
    from storage.models import Base

    db = tmp_path / "runtime.db"
    engine = create_engine(f"sqlite:///{db}")
    Base.metadata.create_all(engine)
    engine.dispose()
    _seed(db)
    before = _snapshot(db)
    run = _run(_ini(tmp_path, db))
    assert run.returncode == 0, run.stderr
    _assert_head(db, before)


@pytest.mark.parametrize("revision,target", [("head", "base"), ("b2f3a4c5d6e7", "b2f3a4c5d6e7-1")])
def test_irreversible_downgrade_rejected_without_changes(tmp_path, revision, target):
    db = tmp_path / "downgrade.db"
    ini = _ini(tmp_path, db)
    run = _run(ini, "upgrade", revision)
    assert run.returncode == 0, run.stderr
    before = _snapshot(db)
    with sqlite3.connect(db) as c:
        versions = c.execute("SELECT version_num FROM alembic_version ORDER BY 1").fetchall()
    run = _run(ini, "downgrade", target)
    assert run.returncode != 0
    assert "irreversible" in run.stderr.lower()
    assert _snapshot(db) == before
    with sqlite3.connect(db) as c:
        assert c.execute("SELECT version_num FROM alembic_version ORDER BY 1").fetchall() == versions


def test_obsolete_migrations_entrypoint_fails_closed():
    text = (REPO / "migrations" / "env.py").read_text(encoding="utf-8")
    assert "obsolete" in text.lower()
    assert "alembic/env.py" in text
    env = {"HOME": "/tmp", "PATH": "/usr/bin:/bin", "PYTHONPATH": str(REPO)}
    run = subprocess.run(
        [str(PYTHON), "-m", "alembic", "-c", str(REPO / "alembic.ini"), "history"],
        cwd=REPO,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
    )
    # The official entrypoint resolves alembic/env.py and sees the merged graph.
    assert run.returncode == 0, run.stderr
    assert "0005 (head)" in run.stdout


def test_irreversible_downgrade_messages_are_honest():
    for path in (
        REPO / "alembic/versions/0005_pr3_single_head.py",
        REPO / "migrations/versions/b2f3a4c5d6e7_add_fact_dedup_key.py",
        REPO / "migrations/versions/6a7b8c9d0e1f_add_decision_unique_constraint.py",
    ):
        text = path.read_text(encoding="utf-8")
        assert "irreversible" in text.lower()
        assert "restore" in text.lower()
