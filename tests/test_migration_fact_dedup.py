"""Focused verification for Card B2 fact dedup migration."""

from __future__ import annotations

import ast
import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import Mock

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

REPO = Path("/home/shtorm/memory-server")
LIVE_DB = Path("/home/shtorm/.hermes/data/memory.db")
HISTORICAL_SOURCE = REPO / "migrations/versions/70e6afc8d15d_initial_schema.py"
EXPECTED_PINNED_CANONICAL = "unique=1;columns=dedup_key;where=lifecycle_state in ('candidate', 'validated', 'active')"

FIXTURE_SCHEMA_SQL = """
CREATE TABLE facts (
  id VARCHAR NOT NULL PRIMARY KEY,
  subject VARCHAR NOT NULL,
  predicate VARCHAR NOT NULL,
  object VARCHAR NOT NULL,
  confidence FLOAT NOT NULL,
  source VARCHAR NULL,
  creator VARCHAR NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  verification_status VARCHAR NOT NULL,
  lifecycle_state VARCHAR NOT NULL,
  version VARCHAR NOT NULL
);
CREATE TABLE alembic_version (
  version_num VARCHAR(32) NOT NULL PRIMARY KEY
);
CREATE INDEX ix_facts_subject ON facts(subject);
CREATE INDEX ix_facts_predicate ON facts(predicate);
CREATE TABLE receipts (
  id VARCHAR NOT NULL PRIMARY KEY,
  memory_type VARCHAR NOT NULL,
  source VARCHAR NOT NULL,
  created_by VARCHAR NOT NULL,
  timestamp DATETIME NOT NULL,
  confidence FLOAT NOT NULL,
  verification_status VARCHAR NOT NULL,
  history TEXT NOT NULL,
  updated_at DATETIME NOT NULL,
  lifecycle_state VARCHAR NOT NULL,
  version VARCHAR NOT NULL
);
CREATE INDEX ix_receipts_memory_type ON receipts(memory_type);
CREATE INDEX ix_receipts_source ON receipts(source);
CREATE TABLE outbox_entries (
  id VARCHAR NOT NULL PRIMARY KEY,
  record_type VARCHAR NOT NULL,
  record_id VARCHAR NOT NULL,
  operation VARCHAR NOT NULL,
  payload_json TEXT NOT NULL,
  status VARCHAR NOT NULL,
  retry_count INTEGER NOT NULL,
  error VARCHAR NULL,
  created_at DATETIME NOT NULL,
  processed_at DATETIME NULL
);
CREATE INDEX ix_outbox_entries_status ON outbox_entries(status);
CREATE INDEX ix_outbox_entries_record_type ON outbox_entries(record_type);
CREATE INDEX ix_outbox_entries_record_id ON outbox_entries(record_id);
CREATE TABLE decisions (
  id VARCHAR NOT NULL PRIMARY KEY,
  context VARCHAR NOT NULL,
  choice VARCHAR NOT NULL,
  dedup_key VARCHAR NOT NULL,
  rejected_alternatives TEXT NOT NULL,
  reason VARCHAR NOT NULL,
  confidence FLOAT NOT NULL,
  source VARCHAR NULL,
  creator VARCHAR NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  verification_status VARCHAR NOT NULL,
  lifecycle_state VARCHAR NOT NULL,
  version VARCHAR NOT NULL
);
CREATE UNIQUE INDEX uq_decisions_context_dedup_active ON decisions(context, dedup_key)
WHERE lifecycle_state IN ('candidate', 'validated', 'active');
CREATE TABLE lifecycle_states (
  id VARCHAR NOT NULL PRIMARY KEY,
  memory_id VARCHAR NOT NULL,
  memory_type VARCHAR NOT NULL,
  current_state VARCHAR NOT NULL,
  previous_state VARCHAR NULL,
  confidence FLOAT NOT NULL,
  updated_at DATETIME NOT NULL
);
CREATE INDEX ix_lifecycle_states_memory_id ON lifecycle_states(memory_id);
CREATE TABLE lifecycle_events (
  id VARCHAR NOT NULL PRIMARY KEY,
  memory_id VARCHAR NOT NULL,
  memory_type VARCHAR NOT NULL,
  from_state VARCHAR NOT NULL,
  to_state VARCHAR NOT NULL,
  reason VARCHAR NOT NULL,
  triggered_by VARCHAR NOT NULL,
  timestamp DATETIME NOT NULL
);
CREATE INDEX ix_lifecycle_events_memory_id ON lifecycle_events(memory_id);
CREATE TABLE claim_relations (
  source_id VARCHAR NOT NULL,
  target_id VARCHAR NOT NULL,
  relation_type VARCHAR NOT NULL,
  created_at DATETIME NOT NULL,
  PRIMARY KEY (source_id, target_id, relation_type)
);
CREATE TABLE __b2_read_only_probe (
  x INTEGER NOT NULL CHECK (x = 0)
);
"""


def load_migration_module(revision: str):
    path = next(REPO.glob(f"migrations/versions/{revision}_*.py"))
    spec = importlib.util.spec_from_file_location(f"b2_migration_{revision}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


migration = load_migration_module("b2f3a4c5d6e7")
assert migration.guarded_backup is not None
assert migration.sqlite_base_error_code is not None


def historical_fact_column_types(source: Path) -> dict[str, str]:
    tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
    fact_table_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "op"
        and node.func.attr == "create_table"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value == "facts"
    ]
    assert len(fact_table_calls) == 1
    result: dict[str, str] = {}
    for node in fact_table_calls[0].args[1:]:
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "sa"
            and node.func.attr == "Column"
        ):
            continue
        if not node.args or not isinstance(node.args[0], ast.Constant):
            continue
        name = node.args[0].value
        if name not in {"created_at", "updated_at"}:
            continue
        assert len(node.args) >= 2
        type_node = node.args[1]
        assert isinstance(type_node, ast.Call)
        assert isinstance(type_node.func, ast.Attribute)
        assert isinstance(type_node.func.value, ast.Name)
        assert type_node.func.value.id == "sa"
        result[name] = type_node.func.attr
    assert result == {"created_at": "String", "updated_at": "String"}
    return result


def assert_pre_schema_gate(conn: sqlite3.Connection) -> None:
    pre_schema = conn.execute("PRAGMA table_info('facts')").fetchall()
    pre_types = {row[1]: str(row[2]).upper() for row in pre_schema}
    assert pre_types.get("created_at") == "DATETIME"
    assert pre_types.get("updated_at") == "DATETIME"
    historical_types = historical_fact_column_types(HISTORICAL_SOURCE)
    assert historical_types["created_at"] == "String"
    assert historical_types["updated_at"] == "String"
    assert pre_types["created_at"] != historical_types["created_at"].upper()
    assert pre_types["updated_at"] != historical_types["updated_at"].upper()


def write_copy_ini(ini_path: Path, copy_path: Path) -> None:
    copy_path = copy_path.resolve(strict=True)
    assert not copy_path.is_symlink()
    assert copy_path != LIVE_DB.resolve()
    text = (
        "[alembic]\n"
        "script_location = /home/shtorm/memory-server/migrations\n"
        "prepend_sys_path = /home/shtorm/memory-server\n"
        f"sqlalchemy.url = sqlite:////{copy_path.as_posix().lstrip('/')}\n\n"
        "[loggers]\n"
        "keys = root,sqlalchemy,alembic\n\n"
        "[handlers]\n"
        "keys = console\n\n"
        "[formatters]\n"
        "keys = generic\n\n"
        "[logger_root]\n"
        "level = WARNING\n"
        "handlers = console\n"
        "qualname =\n\n"
        "[logger_sqlalchemy]\n"
        "level = WARNING\n"
        "handlers =\n"
        "qualname = sqlalchemy.engine\n\n"
        "[logger_alembic]\n"
        "level = INFO\n"
        "handlers =\n"
        "qualname = alembic\n\n"
        "[handler_console]\n"
        "class = StreamHandler\n"
        "args = (sys.stderr,)\n"
        "level = NOTSET\n"
        "formatter = generic\n\n"
        "[formatter_generic]\n"
        "format = %(levelname)-5.5s [%(name)s] %(message)s\n"
        "datefmt = %H:%M:%S\n"
    )
    ini_path.write_text(text, encoding="utf-8")
    assert ini_path.read_text(encoding="utf-8") == text


def _rows(conn: sqlite3.Connection, table: str, columns: list[str]) -> list[tuple]:
    quoted = ", ".join(columns)
    return [tuple(row) for row in conn.execute(f"SELECT {quoted} FROM {table} ORDER BY rowid ASC")]


def _insert_fixture_rows(conn: sqlite3.Connection) -> None:
    facts = [
        (
            "fact-active-1",
            "Docker",
            "is",
            "container",
            0.90,
            "seed",
            "system",
            "2026-08-30 10:00:00.000000",
            "2026-08-30 10:00:01.000000",
            "candidate",
            "candidate",
            "0.1.0",
        ),
        (
            "fact-active-2",
            "Docker ",
            "is",
            "container",
            0.95,
            "seed",
            "system",
            "2026-08-30 09:00:00.000000",
            "2026-08-30 09:00:01.000000",
            "validated",
            "validated",
            "0.1.0",
        ),
        (
            "fact-active-3",
            " Docker",
            "is",
            "container",
            0.80,
            "seed",
            "system",
            "2026-08-30 08:00:00.000000",
            "2026-08-30 08:00:01.000000",
            "active",
            "active",
            "0.1.0",
        ),
        (
            "fact-active-4",
            "Docker",
            "is",
            "container",
            0.20,
            "seed",
            "system",
            "2026-08-30 07:00:00.000000",
            "2026-08-30 07:00:01.000000",
            "archived",
            "archived",
            "0.1.0",
        ),
        (
            "fact-empty-1",
            "",
            "",
            "",
            0.10,
            None,
            "system",
            "2026-08-28 01:00:00.000000",
            "2026-08-28 01:00:01.000000",
            "archived",
            "archived",
            "0.1.0",
        ),
        (
            "fact-empty-2",
            "",
            " ",
            "",
            0.15,
            "",
            "system",
            "2026-08-28 01:00:00.000000",
            "2026-08-28 01:00:02.000000",
            "archived",
            "archived",
            "0.1.0",
        ),
        (
            "fact-case-upper",
            "Docker",
            "relates-to",
            "Container",
            0.50,
            "seed",
            "system",
            "2026-08-29 10:00:00.000000",
            "2026-08-29 10:00:01.000000",
            "candidate",
            "active",
            "0.1.0",
        ),
        (
            "fact-case-lower",
            "docker",
            "relates-to",
            "container",
            0.51,
            "seed",
            "system",
            "2026-08-29 10:00:00.000000",
            "2026-08-29 10:00:02.000000",
            "candidate",
            "active",
            "0.1.0",
        ),
        (
            "fact-archived-1",
            "Apple",
            "is",
            "fruit",
            0.70,
            "seed",
            "system",
            "2026-08-27 02:00:00.000000",
            "2026-08-27 02:00:01.000000",
            "archived",
            "archived",
            "0.1.0",
        ),
        (
            "fact-archived-2",
            "Apple ",
            "is",
            "fruit",
            0.60,
            "seed",
            "system",
            "2026-08-27 02:00:00.000000",
            "2026-08-27 02:00:02.000000",
            "archived",
            "archived",
            "0.1.0",
        ),
        (
            "fact-tie-a",
            "Tie",
            "same",
            "keeper",
            0.40,
            "seed",
            "system",
            "2026-08-26 05:00:00.000000",
            "2026-08-26 05:00:01.000000",
            "candidate",
            "active",
            "0.1.0",
        ),
        (
            "fact-tie-b",
            "Tie",
            "same",
            "keeper",
            0.40,
            "seed",
            "system",
            "2026-08-26 05:00:00.000000",
            "2026-08-26 05:00:02.000000",
            "candidate",
            "active",
            "0.1.0",
        ),
    ]
    conn.executemany(
        "INSERT INTO facts (id, subject, predicate, object, confidence, source, creator, "
        "created_at, updated_at, verification_status, lifecycle_state, version) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        facts,
    )

    decisions = [
        (
            "decision-1",
            "deployment",
            "use Caddy",
            "use Caddy",
            "[]",
            "simpler",
            0.8,
            "seed",
            "system",
            "2026-08-24 10:00:00.000000",
            "2026-08-24 10:00:01.000000",
            "candidate",
            "active",
            "0.1.0",
        ),
        (
            "decision-2",
            "deployment",
            "use Caddy or Nginx",
            "use Caddy or Nginx",
            "[]",
            "fallback",
            0.6,
            "seed",
            "system",
            "2026-08-24 11:00:00.000000",
            "2026-08-24 11:00:01.000000",
            "validated",
            "active",
            "0.1.0",
        ),
    ]
    conn.executemany(
        (
            "INSERT INTO decisions (id, context, choice, dedup_key, "
            "rejected_alternatives, reason, confidence, source, creator, "
            "created_at, updated_at, verification_status, lifecycle_state, "
            "version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        ),
        decisions,
    )

    receipts = [
        (
            "fact-active-1",
            "fact",
            "seed",
            "system",
            "2026-08-30 10:00:00.000000",
            0.9,
            "candidate",
            "[]",
            "2026-08-30 10:01:00.000000",
            "active",
            "0.1.0",
        ),
        (
            "fact-empty-1",
            "fact",
            "seed",
            "system",
            "2026-08-28 01:00:00.000000",
            0.1,
            "candidate",
            "[]",
            "2026-08-28 01:01:00.000000",
            "archived",
            "0.1.0",
        ),
        (
            "decision-1",
            "decision",
            "seed",
            "system",
            "2026-08-24 10:00:00.000000",
            0.8,
            "candidate",
            "[]",
            "2026-08-24 10:01:00.000000",
            "active",
            "0.1.0",
        ),
    ]
    conn.executemany(
        (
            "INSERT INTO receipts (id, memory_type, source, created_by, "
            "timestamp, confidence, verification_status, history, updated_at, "
            "lifecycle_state, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        ),
        receipts,
    )

    outbox = [
        (
            "outbox-fact-1",
            "fact",
            "fact-active-1",
            "index_fact",
            "{}",
            "pending",
            0,
            None,
            "2026-08-30 10:00:00.000000",
            None,
        ),
        (
            "outbox-fact-2",
            "fact",
            "fact-empty-1",
            "index_fact",
            "{}",
            "pending",
            0,
            None,
            "2026-08-28 01:00:00.000000",
            None,
        ),
        (
            "outbox-decision-1",
            "decision",
            "decision-1",
            "index_decision",
            "{}",
            "pending",
            0,
            None,
            "2026-08-24 10:00:00.000000",
            None,
        ),
    ]
    conn.executemany(
        (
            "INSERT INTO outbox_entries (id, record_type, record_id, "
            "operation, payload_json, status, retry_count, error, "
            "created_at, processed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        ),
        outbox,
    )

    conn.executemany(
        (
            "INSERT INTO lifecycle_states (id, memory_id, memory_type, "
            "current_state, previous_state, confidence, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        ),
        [
            (
                "ls-fact-1",
                "fact-active-1",
                "fact",
                "active",
                "candidate",
                0.9,
                "2026-08-30 10:02:00.000000",
            ),
            (
                "ls-decision-1",
                "decision-1",
                "decision",
                "active",
                "candidate",
                0.8,
                "2026-08-24 10:02:00.000000",
            ),
        ],
    )
    conn.executemany(
        (
            "INSERT INTO lifecycle_events (id, memory_id, memory_type, "
            "from_state, to_state, reason, triggered_by, timestamp) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        ),
        [
            (
                "le-fact-1",
                "fact-active-1",
                "fact",
                "candidate",
                "active",
                "seed",
                "system",
                "2026-08-30 10:03:00.000000",
            ),
            (
                "le-decision-1",
                "decision-1",
                "decision",
                "candidate",
                "active",
                "seed",
                "system",
                "2026-08-24 10:03:00.000000",
            ),
        ],
    )
    conn.executemany(
        "INSERT INTO claim_relations (source_id, target_id, relation_type, created_at) VALUES (?, ?, ?, ?)",
        [
            ("fact-active-1", "fact-empty-1", "related_to", "2026-08-30 10:04:00.000000"),
            ("decision-1", "fact-tie-a", "depends_on", "2026-08-24 10:04:00.000000"),
        ],
    )
    conn.execute("INSERT INTO __b2_read_only_probe(x) VALUES (0)")


def create_fixture_database(db_path: Path, *, pinned_index: str = "absent") -> None:
    assert pinned_index in {"absent", "compatible", "collision", "malformed"}
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(FIXTURE_SCHEMA_SQL)
        conn.execute("DELETE FROM alembic_version")
        conn.execute("INSERT INTO alembic_version(version_num) VALUES (?)", ("6a7b8c9d0e1f",))
        _insert_fixture_rows(conn)
        if pinned_index == "compatible":
            conn.execute("ALTER TABLE facts ADD COLUMN dedup_key VARCHAR")
            rows = conn.execute(
                "SELECT id, subject, predicate, object FROM facts ORDER BY id"
            ).fetchall()
            conn.executemany(
                "UPDATE facts SET dedup_key = ? WHERE id = ?",
                [
                    (
                        migration.fact_dedup_key(row[1], row[2], row[3]),
                        row[0],
                    )
                    for row in rows
                ],
            )
            conn.execute(
                "UPDATE facts SET lifecycle_state = 'archived' "
                "WHERE id IN ('fact-active-2', 'fact-active-3', 'fact-tie-b')"
            )
            conn.execute(
                "CREATE UNIQUE INDEX uq_facts_spo_active ON facts(dedup_key) "
                "WHERE lifecycle_state IN ('candidate', 'validated', 'active')"
            )
        elif pinned_index == "collision":
            conn.execute("CREATE INDEX uq_facts_spo_active ON facts(subject)")
        elif pinned_index == "malformed":
            conn.execute("PRAGMA writable_schema=ON")
            conn.execute(
                (
                    "UPDATE sqlite_master SET sql = "
                    "'CREATE UNIQUE INDEX uq_facts_spo_active ON "
                    "facts(dedup_key) WHERE lifecycle_state IN (candidate, "
                    "validated, active)' WHERE type='index' AND "
                    "name='uq_facts_spo_active'"
                )
            )
            conn.execute("PRAGMA writable_schema=OFF")
        conn.commit()
    assert db_path.exists()


def index_exists_on_copy(db_path: Path, name: str) -> bool:
    with sqlite3.connect(f"file:{db_path.resolve(strict=True)}?mode=ro", uri=True) as conn:
        return any(row[1] == name for row in conn.execute("PRAGMA index_list('facts')"))


def read_index_sql(db_path: Path, name: str) -> str | None:
    with sqlite3.connect(f"file:{db_path.resolve(strict=True)}?mode=ro", uri=True) as conn:
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
            (name,),
        ).fetchone()
    return None if row is None else row[0]


def canonical_pinned_index_sql(sql: str) -> str:
    return migration.canonical_pinned_index_sql(sql)


def exact_index_definition_on_copy(db_path: Path, name: str) -> bool:
    sql = read_index_sql(db_path, name)
    return sql == (
        "CREATE UNIQUE INDEX uq_facts_spo_active ON facts(dedup_key) "
        "WHERE lifecycle_state IN ('candidate', 'validated', 'active')"
    )


def make_backup_fixture(tmp_path: Path) -> tuple[Path, Path]:
    source, destination = tmp_path / "backup-source.db", tmp_path / "backup-destination.db"
    create_fixture_database(source)
    return source, destination


def source_alembic_revision(source: Path) -> str:
    return migration.source_alembic_revision(source)


def source_metadata_snapshot(source: Path) -> dict[str, object]:
    return migration.source_metadata_snapshot(source)


def guarded_backup(source: Path, destination: Path) -> None:
    migration.guarded_backup(source, destination)


def backup_with_captured_version(source: Path, destination: Path) -> str:
    captured = source_alembic_revision(source)
    metadata_before = source_metadata_snapshot(source)
    guarded_backup(source, destination)
    assert source_alembic_revision(source) == captured
    assert source_metadata_snapshot(source) == metadata_before
    with sqlite3.connect(str(destination)) as conn:
        assert str(conn.execute("SELECT version_num FROM alembic_version").fetchone()[0]) == captured
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        destination_schema = tuple(
            tuple(row)
            for row in conn.execute(
                "SELECT type, name, tbl_name, sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type, name"
            )
        )
        destination_facts_schema = tuple(tuple(row) for row in conn.execute("PRAGMA table_info('facts')"))
    assert str(destination.resolve(strict=True)) != metadata_before["realpath"]
    assert destination_schema == metadata_before["schema"]
    assert destination_facts_schema == metadata_before["facts_schema"]
    return captured


def run_index_branch_in_process(db_path: Path) -> Mock:
    migration_module = load_migration_module("b2f3a4c5d6e7")
    engine = sa.create_engine(f"sqlite:///{db_path}")
    with engine.begin() as connection:
        context = MigrationContext.configure(connection)
        operations = Operations(context)
        assert migration_module.current_version(connection) == "6a7b8c9d0e1f"
        real_create_index = operations.create_index
        create_spy = Mock(wraps=real_create_index)
        migration_module.op = operations
        migration_module.op.create_index = create_spy
        if not migration_module.index_exists(connection, migration_module._INDEX_NAME):
            connection.execute(sa.text("ALTER TABLE facts ADD COLUMN dedup_key VARCHAR"))
            saved_indexes = [
                {
                    "name": migration_module._INDEX_NAME,
                    "sql": migration_module._expected_pinned_sql(),
                }
            ]
            migration_module._recreate_saved_indexes(
                connection,
                saved_indexes,
                "absent",
            )
        else:
            saved_indexes = migration_module._save_index_state(connection)
            migration_module._recreate_saved_indexes(
                connection,
                saved_indexes,
                "compatible",
            )
    return create_spy


def run_upgrade(
    db_path: Path,
    ini_path: Path,
    *,
    event_path: Path | None = None,
    fail_before_summary: bool = False,
) -> CompletedProcess:
    expected_url = f"sqlalchemy.url = sqlite:////{db_path.resolve(strict=True).as_posix().lstrip('/')}"
    ini_lines = ini_path.read_text(encoding="utf-8").splitlines()
    assert expected_url in ini_lines, (expected_url, ini_lines)
    env = os.environ.copy()
    if event_path is None:
        env.pop("B2_EVENT_PATH", None)
    else:
        env["B2_EVENT_PATH"] = str(event_path.resolve())
    if fail_before_summary:
        env["B2_FAIL_BEFORE_SUMMARY"] = "1"
    else:
        env.pop("B2_FAIL_BEFORE_SUMMARY", None)
    return subprocess.run(
        [sys.executable, "-m", "alembic", "-c", str(ini_path), "upgrade", "head"],
        cwd=str(REPO),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def source_db_path() -> Path:
    return LIVE_DB


def prepare_subprocess_fixture(tmp_path: Path, *, event_enabled: bool):
    db_path = tmp_path / ("copy-with-events.db" if event_enabled else "copy-without-events.db")
    event_path = tmp_path / "events.jsonl" if event_enabled else None
    create_fixture_database(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        assert_pre_schema_gate(conn)
    ini_path = tmp_path / "alembic_copy.ini"
    write_copy_ini(ini_path, db_path.resolve(strict=True))
    return db_path, ini_path, event_path


def run_forced_limit(db_path: Path, ini_path: Path, tmp_path: Path) -> CompletedProcess:
    assert hasattr(sqlite3, "SQLITE_LIMIT_VARIABLE_NUMBER")
    assert hasattr(sqlite3.Connection, "setlimit")
    assert hasattr(sqlite3.Connection, "getlimit")
    bootstrap = tmp_path / "b2_forced_limit_bootstrap.py"
    event_path = tmp_path / "b2-events.jsonl"
    bootstrap.write_text(
        textwrap.dedent(
            f"""
            import os
            import sqlite3
            import sqlalchemy
            from alembic import command, config

            DB_PATH = {str(db_path.resolve())!r}
            EVENT_PATH = {str(event_path.resolve())!r}
            LIMIT = 32

            def creator():
                raw = sqlite3.connect(DB_PATH)
                raw.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, LIMIT)
                assert raw.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER) == LIMIT
                return raw

            def forced_engine_from_config(configuration, prefix="sqlalchemy.", **kwargs):
                return sqlalchemy.create_engine(
                    "sqlite://", creator=creator, poolclass=sqlalchemy.pool.NullPool
                )

            os.environ["B2_EVENT_PATH"] = EVENT_PATH
            sqlalchemy.engine_from_config = forced_engine_from_config
            command.upgrade(config.Config({str(ini_path.resolve())!r}), "head")
            """
        )
    )
    return subprocess.run([sys.executable, str(bootstrap)], check=False, capture_output=True, text=True)


def test_migration_module_syntax_and_import_smoke():
    assert migration.revision == "b2f3a4c5d6e7"
    assert migration.time.sleep is not None


def test_fact_dedup_helper_handles_none_and_empty_components():
    assert migration.fact_dedup_key(None, "", " ") == "\x1f\x1f"


def test_absent_index_is_created_once_in_process(tmp_path):
    db_path = tmp_path / "absent.db"
    create_fixture_database(db_path, pinned_index="absent")
    with sqlite3.connect(str(db_path)) as conn:
        assert_pre_schema_gate(conn)
        assert conn.execute("SELECT version_num FROM alembic_version").fetchone()[0] == "6a7b8c9d0e1f"
    create_spy = run_index_branch_in_process(db_path)
    assert create_spy.call_count == 1
    assert create_spy.call_args.args[:3] == ("uq_facts_spo_active", "facts", ["dedup_key"])
    assert index_exists_on_copy(db_path, "uq_facts_spo_active")


def test_compatible_index_is_not_created_in_process(tmp_path):
    db_path = tmp_path / "compatible.db"
    create_fixture_database(db_path, pinned_index="compatible")
    with sqlite3.connect(str(db_path)) as conn:
        assert_pre_schema_gate(conn)
        assert conn.execute("SELECT version_num FROM alembic_version").fetchone()[0] == "6a7b8c9d0e1f"
    saved_sql = read_index_sql(db_path, "uq_facts_spo_active")
    create_spy = run_index_branch_in_process(db_path)
    assert create_spy.call_count == 0
    assert read_index_sql(db_path, "uq_facts_spo_active") == saved_sql
    assert exact_index_definition_on_copy(db_path, "uq_facts_spo_active")


def test_backup_retries_then_succeeds_with_real_backup(tmp_path, monkeypatch):
    source, destination = make_backup_fixture(tmp_path)
    real_connect = migration.sqlite3.connect
    captured_revision = source_alembic_revision(source)
    metadata_before = source_metadata_snapshot(source)
    destination_attempts, sleeps = [], []

    def fail_once_then_connect(*args, **kwargs):
        if str(args[0]) == str(destination):
            destination_attempts.append(args[0])
            if len(destination_attempts) == 1:
                raise sqlite3.OperationalError("database is locked")
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(migration.sqlite3, "connect", fail_once_then_connect)
    monkeypatch.setattr(migration.time, "sleep", sleeps.append)
    migration.guarded_backup(source, destination)

    assert len(destination_attempts) == 3
    assert sleeps == [0.1]
    assert source_alembic_revision(source) == captured_revision
    assert source_metadata_snapshot(source) == metadata_before
    with real_connect(str(destination)) as check:
        assert check.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert [row[1] for row in check.execute("PRAGMA table_info('facts')")] == [
            "id",
            "subject",
            "predicate",
            "object",
            "confidence",
            "source",
            "creator",
            "created_at",
            "updated_at",
            "verification_status",
            "lifecycle_state",
            "version",
        ]
        assert check.execute("SELECT version_num FROM alembic_version").fetchone()[0] == captured_revision
        destination_schema = tuple(
            tuple(row)
            for row in check.execute(
                "SELECT type, name, tbl_name, sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type, name"
            )
        )
        destination_facts_schema = tuple(tuple(row) for row in check.execute("PRAGMA table_info('facts')"))
    assert destination_schema == metadata_before["schema"]
    assert destination_facts_schema == metadata_before["facts_schema"]
    assert str(destination.resolve(strict=True)) != metadata_before["realpath"]
    assert not Path(str(destination) + "-wal").exists()
    assert not Path(str(destination) + "-shm").exists()


class CodedOperationalError(sqlite3.OperationalError):
    def __init__(self, message: str, *, code: int | None = None, extended: int | None = None):
        super().__init__(message)
        self._code = code
        self._extended = extended

    @property
    def sqlite_errorcode(self):
        return self._code

    @property
    def sqlite_extended_errorcode(self):
        return self._extended


@pytest.mark.parametrize(
    "code, extended",
    [
        (sqlite3.SQLITE_BUSY, None),
        (sqlite3.SQLITE_LOCKED, None),
        (None, getattr(sqlite3, "SQLITE_BUSY_TIMEOUT", 773)),
        (None, getattr(sqlite3, "SQLITE_LOCKED_SHAREDCACHE", 262)),
    ],
)
def test_backup_retries_base_and_extended_busy_locked_codes(tmp_path, monkeypatch, code, extended):
    source, destination = make_backup_fixture(tmp_path)
    attempts, destination_attempts, source_reads, sleeps = [], [], [], []
    error = CodedOperationalError("not a busy/locked diagnostic", code=code, extended=extended)
    destination_path = str(destination.resolve(strict=False))
    real_connect = migration.sqlite3.connect

    def coded_connect(*args, **kwargs):
        target = str(args[0])
        attempts.append(target)
        if target == destination_path:
            destination_attempts.append(target)
            raise error
        source_reads.append(target)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(migration.sqlite3, "connect", coded_connect)
    monkeypatch.setattr(migration.time, "sleep", sleeps.append)
    with pytest.raises(sqlite3.OperationalError):
        migration.guarded_backup(source, destination)
    assert len(destination_attempts) == 5
    assert len(source_reads) >= 2
    assert len(attempts) == len(destination_attempts) + len(source_reads)
    assert sleeps == [0.1, 0.2, 0.4, 0.8]


def test_sqlite_base_code_normalizes_extended_codes():
    assert migration.sqlite_base_error_code(CodedOperationalError("x", extended=773)) == sqlite3.SQLITE_BUSY
    assert migration.sqlite_base_error_code(CodedOperationalError("x", extended=262)) == sqlite3.SQLITE_LOCKED


def test_backup_cleans_partial_destination_after_locked_failure(tmp_path, monkeypatch):
    source, destination = make_backup_fixture(tmp_path)
    real_connect = migration.sqlite3.connect

    def locked_after_partial(*args, **kwargs):
        if str(args[0]) == str(destination):
            real_connect(str(destination)).close()
            raise sqlite3.OperationalError("database is locked")
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(migration.sqlite3, "connect", locked_after_partial)
    monkeypatch.setattr(migration.time, "sleep", lambda _: None)
    with pytest.raises(sqlite3.OperationalError):
        migration.guarded_backup(source, destination)
    assert not destination.exists()
    assert not Path(str(destination) + "-wal").exists()
    assert not Path(str(destination) + "-shm").exists()


def test_backup_nonretryable_code_with_locked_text_does_not_retry(tmp_path, monkeypatch):
    source, destination = make_backup_fixture(tmp_path)
    attempts, sleeps = [], []
    error = CodedOperationalError("database is locked", code=sqlite3.SQLITE_CONSTRAINT)
    real_connect = migration.sqlite3.connect
    destination_path = str(destination.resolve(strict=False))

    def nonretryable_connect(*args, **kwargs):
        target = str(args[0])
        if target == destination_path:
            attempts.append(target)
            raise error
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(migration.sqlite3, "connect", nonretryable_connect)
    monkeypatch.setattr(migration.time, "sleep", sleeps.append)
    with pytest.raises(sqlite3.OperationalError):
        migration.guarded_backup(source, destination)
    assert len(attempts) == 1
    assert sleeps == []


def test_subprocess_event_enabled(tmp_path):
    db_path, ini_path, event_path = prepare_subprocess_fixture(tmp_path, event_enabled=True)
    run = run_upgrade(db_path, ini_path, event_path=event_path)
    assert run.returncode == 0, run.stderr
    assert "IsADirectoryError" not in run.stderr
    assert event_path is not None and event_path.exists()
    events = [json.loads(line) for line in event_path.read_text().splitlines()]
    assert events


def test_subprocess_event_disabled(tmp_path):
    db_path, ini_path, event_path = prepare_subprocess_fixture(tmp_path, event_enabled=False)
    run = run_upgrade(db_path, ini_path, event_path=event_path)
    assert run.returncode == 0, run.stderr
    assert "IsADirectoryError" not in run.stderr
    assert event_path is None
    assert not (tmp_path / "events.jsonl").exists()


def test_forced_limit_real_env(tmp_path):
    copy_db = tmp_path / "copy.db"
    ini_path = tmp_path / "alembic_copy.ini"
    create_fixture_database(copy_db)
    write_copy_ini(ini_path, copy_db.resolve(strict=True))
    run = run_forced_limit(copy_db, ini_path, tmp_path)
    assert run.returncode == 0, run.stderr
    event_path = tmp_path / "b2-events.jsonl"
    events = [json.loads(line) for line in event_path.read_text().splitlines()]
    observed = [event for event in events if event.get("name") == "observed_limit"]
    assert len(observed) == 1
    assert observed[0]["observed_limit"] == 32
    statement_events = [event for event in events if event.get("name") == "statement"]
    assert statement_events
    assert all("statement" in event and "parameter_count" in event for event in statement_events)
    limit = observed[0]["observed_limit"]
    assert all(event["parameter_count"] <= limit for event in statement_events)
    assert all(event["page_size"] <= 1000 for event in statement_events if event["page_size"] is not None)
    assert all(not event["row_wise"] for event in statement_events)
    assert any(event["kind"] == "case_update" for event in statement_events)


def test_live_db_copy_upgrade_preserves_source_revision(tmp_path):
    source = source_db_path()
    assert source.exists()
    destination = tmp_path / "live-copy.db"
    guarded_backup(source, destination)
    captured = source_alembic_revision(source)
    assert source_alembic_revision(source) == captured
    with sqlite3.connect(str(destination)) as conn:
        assert conn.execute("SELECT version_num FROM alembic_version").fetchone()[0] == captured
