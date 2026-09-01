"""Focused verification for Card B2 fact dedup migration."""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path
from subprocess import CompletedProcess
from typing import Any
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
            rows = conn.execute("SELECT id, subject, predicate, object FROM facts ORDER BY id").fetchall()
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
            conn.execute("ALTER TABLE facts ADD COLUMN dedup_key VARCHAR")
            conn.execute("CREATE UNIQUE INDEX uq_facts_spo_active ON facts(dedup_key)")
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
    return migration.canonical_pinned_index_sql(sql) == EXPECTED_PINNED_CANONICAL


def facts_schema_snapshot(conn: sqlite3.Connection) -> tuple[tuple, ...]:
    return tuple(tuple(row) for row in conn.execute("PRAGMA table_info('facts')"))


def facts_indexes_snapshot(conn: sqlite3.Connection) -> tuple[tuple, ...]:
    result = []
    for row in conn.execute("PRAGMA index_list('facts')").fetchall():
        name = row[1]
        info = tuple(tuple(item) for item in conn.execute(f"PRAGMA index_info({name!r})"))
        sql = conn.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name=?", (name,)).fetchone()[0]
        result.append((name, int(row[2]), row[3], int(row[4]), info, sql))
    return tuple(sorted(result))


FULL_STATE_TABLES = (
    "facts",
    "receipts",
    "outbox_entries",
    "decisions",
    "lifecycle_states",
    "lifecycle_events",
    "claim_relations",
    "__b2_read_only_probe",
)


def full_table_snapshot(conn: sqlite3.Connection, table: str) -> tuple[tuple, ...]:
    return tuple(tuple(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY rowid"))


def _canonical_rows(conn: sqlite3.Connection, table: str) -> list[list[Any]]:
    columns = [row[1] for row in conn.execute(f"PRAGMA table_info({table!r})")]
    quoted = ", ".join('"' + column.replace('"', '""') + '"' for column in columns)
    rows = [list(row) for row in conn.execute(f"SELECT {quoted} FROM {table}")]
    return sorted(rows, key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":")))


def _facts_index_metadata(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    indexes = []
    for row in conn.execute("PRAGMA index_list('facts')"):
        name = str(row[1])
        quoted_name = '"' + name.replace('"', '""') + '"'
        indexes.append(
            {
                "name": name,
                "unique": int(row[2]),
                "origin": row[3],
                "partial": int(row[4]),
                "index_info": [list(item) for item in conn.execute(f"PRAGMA index_info({quoted_name})")],
                "sql": conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='index' AND name=?", (name,)
                ).fetchone()[0],
            }
        )
    return sorted(indexes, key=lambda item: item["name"])


def _sqlite_master_snapshot(conn: sqlite3.Connection) -> list[list[Any]]:
    return [
        list(row)
        for row in conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name, tbl_name, sql"
        )
    ]


def _integrity_check(conn: sqlite3.Connection) -> list[list[Any]]:
    try:
        return [list(row) for row in conn.execute("PRAGMA integrity_check")]
    except sqlite3.DatabaseError as error:
        return [[f"ERROR: {error}"]]


def full_state_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    """Capture every logical B3 state dimension in a canonical form."""
    conn.execute("PRAGMA writable_schema=ON")
    try:
        snapshot: dict[str, Any] = {
            "tables": {table: _canonical_rows(conn, table) for table in FULL_STATE_TABLES},
            "sqlite_master": _sqlite_master_snapshot(conn),
            "facts_table_info": [list(row) for row in conn.execute("PRAGMA table_info('facts')")],
            "facts_indexes": _facts_index_metadata(conn),
            "alembic_version": conn.execute(
                "SELECT version_num FROM alembic_version ORDER BY version_num"
            ).fetchall(),
        }
    finally:
        conn.execute("PRAGMA writable_schema=OFF")
    snapshot["integrity_check"] = _integrity_check(conn)
    return snapshot


def full_state_digest(snapshot: dict[str, Any]) -> str:
    encoded = json.dumps(snapshot, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def assert_full_state_equal(left: dict[str, Any], right: dict[str, Any]) -> None:
    left_digest = full_state_digest(left)
    right_digest = full_state_digest(right)
    if left_digest != right_digest:
        differing = [
            key
            for key in left
            if left.get(key) != right.get(key)
        ]
        if left.get("tables") != right.get("tables"):
            differing.extend(
                f"tables.{table}"
                for table in FULL_STATE_TABLES
                if left["tables"].get(table) != right["tables"].get(table)
            )
        raise AssertionError(
            f"full-state mismatch: sha256 {left_digest} != {right_digest}; dimensions={differing}"
        )
    assert left == right


def parse_summary(stdout: str) -> dict[str, Any]:
    lines = [line for line in stdout.splitlines() if line.startswith("FACT_DEDUPE_SUMMARY ")]
    assert len(lines) == 1
    summary = json.loads(lines[0].split(" ", 1)[1])
    assert set(summary) == {
        "schema",
        "kept",
        "deleted_fact_ids",
        "deleted_fact_count",
        "deleted_receipt_count",
        "deleted_outbox_count",
        "remaining_dupe_keys",
        "remaining_dupe_count",
    }
    assert summary["schema"] == "B2-A3"
    kept = summary["kept"]
    assert set(kept) == {"fact_ids", "fact_count"}
    assert kept["fact_ids"] == sorted(kept["fact_ids"])
    assert summary["deleted_fact_ids"] == sorted(summary["deleted_fact_ids"])
    assert set(kept["fact_ids"]).isdisjoint(summary["deleted_fact_ids"])
    assert kept["fact_count"] == len(kept["fact_ids"])
    assert summary["deleted_fact_count"] == len(summary["deleted_fact_ids"])
    assert summary["remaining_dupe_count"] == len(summary["remaining_dupe_keys"])
    assert summary["remaining_dupe_keys"] == sorted(summary["remaining_dupe_keys"])
    assert all(isinstance(key, str) and key for key in summary["remaining_dupe_keys"])
    return summary


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


def run_downgrade(db_path: Path, ini_path: Path) -> CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "alembic", "-c", str(ini_path), "downgrade", "6a7b8c9d0e1f"],
        cwd=str(REPO),
        env=os.environ.copy(),
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
    assert len(sleeps) + 1 == 2
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


def assert_post_migration_state(db_path: Path, before: dict[str, Any] | None = None) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT version_num FROM alembic_version").fetchone()[0] == "b2f3a4c5d6e7"
        schema = conn.execute("PRAGMA table_info('facts')").fetchall()
        assert [row[1] for row in schema] == [
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
            "dedup_key",
        ]
        assert [row[2] for row in schema] == ["VARCHAR"] * 4 + ["FLOAT"] + ["VARCHAR"] * 8
        assert schema[-1][3] == 1
        assert [row[5] for row in schema] == [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert [(row[1], row[5]) for row in schema if row[5]] == [("id", 1)]
        assert before is None or set(before["facts_indexes"]) <= set(facts_indexes_snapshot(conn))
        if before is not None:
            expected_pre = [list(row) for row in before["facts_schema"]]
            for row in expected_pre:
                if row[1] in {"created_at", "updated_at"}:
                    row[2] = "VARCHAR"
            assert tuple(tuple(row) for row in expected_pre) == tuple(schema[:-1])
        assert exact_index_definition_on_copy(db_path, "uq_facts_spo_active")
        assert before is None or set(row[0] for row in full_table_snapshot(conn, "facts")) <= {
            row[0] for row in before["facts"]
        }
        if before is not None:
            pre_fact_ids = {str(row[0]) for row in before["facts"]}
            post_fact_ids = {str(row[0]) for row in full_table_snapshot(conn, "facts")}
            deleted_fact_ids = pre_fact_ids - post_fact_ids
            pre_owned_receipts = {
                row for row in before["receipts"] if row[1] == "fact" and row[0] in pre_fact_ids
            }
            post_owned_receipts = {
                row for row in full_table_snapshot(conn, "receipts") if row[1] == "fact" and row[0] in pre_fact_ids
            }
            assert post_owned_receipts == {row for row in pre_owned_receipts if row[0] not in deleted_fact_ids}
            assert not {row for row in post_owned_receipts if row[0] in deleted_fact_ids}
            pre_owned_outbox = {
                row for row in before["outbox_entries"] if row[1] == "fact" and row[2] in pre_fact_ids
            }
            post_owned_outbox = {
                row
                for row in full_table_snapshot(conn, "outbox_entries")
                if row[1] == "fact" and row[2] in pre_fact_ids
            }
            assert post_owned_outbox == {row for row in pre_owned_outbox if row[2] not in deleted_fact_ids}
            assert not {row for row in post_owned_outbox if row[2] in deleted_fact_ids}
        assert _rows(conn, "decisions", ["id", "choice"]) == [
            ("decision-1", "use Caddy"),
            ("decision-2", "use Caddy or Nginx"),
        ]
        assert _rows(conn, "lifecycle_states", ["id", "memory_id"]) == [
            ("ls-fact-1", "fact-active-1"),
            ("ls-decision-1", "decision-1"),
        ]
        assert _rows(conn, "claim_relations", ["source_id", "target_id", "relation_type"]) == [
            ("fact-active-1", "fact-empty-1", "related_to"),
            ("decision-1", "fact-tie-a", "depends_on"),
        ]


def test_subprocess_event_enabled(tmp_path):
    db_path, ini_path, event_path = prepare_subprocess_fixture(tmp_path, event_enabled=True)
    with sqlite3.connect(str(db_path)) as conn:
        before: dict[str, Any] = {
            "facts": full_table_snapshot(conn, "facts"),
            "receipts": full_table_snapshot(conn, "receipts"),
            "outbox_entries": full_table_snapshot(conn, "outbox_entries"),
            "facts_schema": facts_schema_snapshot(conn),
            "facts_indexes": facts_indexes_snapshot(conn),
            "decisions": full_table_snapshot(conn, "decisions"),
            "lifecycle_states": full_table_snapshot(conn, "lifecycle_states"),
            "lifecycle_events": full_table_snapshot(conn, "lifecycle_events"),
            "claim_relations": full_table_snapshot(conn, "claim_relations"),
        }
    run = run_upgrade(db_path, ini_path, event_path=event_path)
    assert run.returncode == 0, run.stderr
    assert "IsADirectoryError" not in run.stderr
    assert event_path is not None and event_path.exists()
    events = [json.loads(line) for line in event_path.read_text().splitlines()]
    assert events
    summary = parse_summary(run.stdout)
    assert summary["deleted_fact_count"] > 0
    assert_post_migration_state(db_path, before)
    with sqlite3.connect(str(db_path)) as conn:
        assert full_table_snapshot(conn, "decisions") == before["decisions"]
        assert full_table_snapshot(conn, "lifecycle_states") == before["lifecycle_states"]
        assert full_table_snapshot(conn, "lifecycle_events") == before["lifecycle_events"]
        assert full_table_snapshot(conn, "claim_relations") == before["claim_relations"]


def test_owned_child_reconciliation_rejects_omitted_fact_receipt(tmp_path, monkeypatch):
    db_path = tmp_path / "owned-child-regression.db"
    create_fixture_database(db_path)
    module = load_migration_module("b2f3a4c5d6e7")
    real_delete_ids = module._delete_ids

    def omit_receipt_delete(bind, table, column, ids):
        if table == "receipts":
            return 0
        return real_delete_ids(bind, table, column, ids)

    monkeypatch.setattr(module, "_delete_ids", omit_receipt_delete)
    engine = sa.create_engine(f"sqlite:///{db_path}")
    with pytest.raises(AssertionError):
        with engine.begin() as connection:
            module.op = Operations(MigrationContext.configure(connection, opts={"transactional_ddl": True}))
            module.upgrade()


def test_real_upgrade_context_creates_absent_index_once(tmp_path):
    db_path = tmp_path / "real-index.db"
    create_fixture_database(db_path)
    module = load_migration_module("b2f3a4c5d6e7")
    engine = sa.create_engine(f"sqlite:///{db_path}")
    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection, opts={"transactional_ddl": True}))
        spy = Mock(wraps=operations.create_index)
        module.op = operations
        module.op.create_index = spy
        module.upgrade()
        connection.execute(sa.text("UPDATE alembic_version SET version_num='b2f3a4c5d6e7'"))
    assert spy.call_count == 1
    assert_post_migration_state(db_path)


def test_subprocess_idempotent_noop(tmp_path):
    db_path, ini_path, _ = prepare_subprocess_fixture(tmp_path, event_enabled=False)
    first = run_upgrade(db_path, ini_path)
    assert first.returncode == 0
    second = run_upgrade(db_path, ini_path)
    assert second.returncode == 0
    assert "FACT_DEDUPE_SUMMARY" not in second.stdout
    assert_post_migration_state(db_path)


def test_downgrade_upgraded_copy_resets_version_without_restoring_deleted_rows(tmp_path):
    db_path, ini_path, _ = prepare_subprocess_fixture(tmp_path, event_enabled=False)
    with sqlite3.connect(db_path) as conn:
        before_ids = {row[0] for row in conn.execute("SELECT id FROM facts")}
    upgrade = run_upgrade(db_path, ini_path)
    assert upgrade.returncode == 0, upgrade.stderr
    deleted = set(parse_summary(upgrade.stdout)["deleted_fact_ids"])
    downgrade = run_downgrade(db_path, ini_path)
    assert downgrade.returncode == 0, downgrade.stderr
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT version_num FROM alembic_version").fetchone()[0] == "6a7b8c9d0e1f"
        assert conn.execute("PRAGMA table_info(facts)").fetchall()[-1][1] == "version"
        assert {row[0] for row in conn.execute("SELECT id FROM facts")} == before_ids - deleted
        assert conn.execute("SELECT name FROM sqlite_master WHERE name=?", ("uq_facts_spo_active",)).fetchone() is None


def test_subprocess_failure_rolls_back_everything(tmp_path):
    db_path, ini_path, _ = prepare_subprocess_fixture(tmp_path, event_enabled=False)
    with sqlite3.connect(db_path) as conn:
        before = {
            table: full_table_snapshot(conn, table)
            for table in (
                "facts",
                "receipts",
                "outbox_entries",
                "decisions",
                "lifecycle_states",
                "lifecycle_events",
                "claim_relations",
            )
        }
        before_schema = tuple(
            tuple(row) for row in conn.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")
        )
    run = run_upgrade(db_path, ini_path, fail_before_summary=True)
    assert run.returncode != 0
    assert "FACT_DEDUPE_SUMMARY" not in run.stdout
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT version_num FROM alembic_version").fetchone()[0] == "6a7b8c9d0e1f"
        assert (
            tuple(
                tuple(row)
                for row in conn.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")
            )
            == before_schema
        )
        for table, rows in before.items():
            assert full_table_snapshot(conn, table) == rows


def test_pinned_parser_fails_closed():
    invalid_sql = [
        "CREATE UNIQUE INDEX uq_facts_spo_active ON facts(dedup_key) WHERE lifecycle_state IN (candidate, 'validated', 'active')",  # noqa: E501
        "CREATE UNIQUE INDEX uq_facts_spo_active ON facts(dedup_key) WHERE lifecycle_state IN ('candidate', 'validated', 'active') OR 1=1",  # noqa: E501
        "CREATE UNIQUE INDEX uq_facts_spo_active ON facts(dedup_key) WHERE lifecycle_state IN ('candidate', 'validated', 'active') extra",  # noqa: E501
    ]
    for sql in invalid_sql:
        with pytest.raises(ValueError):
            migration.canonical_pinned_index_sql(sql)


@pytest.mark.parametrize(
    "sql",
    [
        (
            "CREATE UNIQUE INDEX uq_facts_spo_active ON facts(dedup_key) "
            "WHERE lifecycle_state IN ('candidate', 'validated', 'active');"
        ),
        (
            'create unique index uq_facts_spo_active on "facts" ("dedup_key") '
            "where lifecycle_state in ('candidate','validated','active')"
        ),
        (
            "CREATE UNIQUE INDEX uq_facts_spo_active ON [facts]([dedup_key]) "
            "WHERE lifecycle_state IN ( 'candidate' , 'validated' , 'active' )"
        ),
    ],
)
def test_pinned_parser_accepted_compatibility_matrix(sql):
    assert migration.canonical_pinned_index_sql(sql) == EXPECTED_PINNED_CANONICAL


@pytest.mark.parametrize("pinned_index", ["collision", "malformed"])
def test_subprocess_bad_pinned_index_is_atomic_and_has_no_create_event(tmp_path, pinned_index):
    db_path = tmp_path / f"{pinned_index}.db"
    create_fixture_database(db_path, pinned_index=pinned_index)
    ini_path = tmp_path / f"{pinned_index}.ini"
    write_copy_ini(ini_path, db_path.resolve(strict=True))
    event_path = tmp_path / "bad-index-events.jsonl"
    with sqlite3.connect(str(db_path)) as conn:
        before = full_state_snapshot(conn)
        before_version = conn.execute("SELECT version_num FROM alembic_version").fetchone()[0]
    run = run_upgrade(db_path, ini_path, event_path=event_path)
    assert run.returncode != 0
    assert "FACT_DEDUPE_SUMMARY" not in run.stdout
    with sqlite3.connect(str(db_path)) as conn:
        after = full_state_snapshot(conn)
        assert_full_state_equal(before, after)
        assert conn.execute("SELECT version_num FROM alembic_version").fetchone()[0] == before_version
        assert after["integrity_check"] == before["integrity_check"]
        sql = conn.execute("SELECT sql FROM sqlite_master WHERE name='uq_facts_spo_active'").fetchone()[0]
        assert sql is not None
    events = [json.loads(line) for line in event_path.read_text().splitlines()] if event_path.exists() else []
    assert not any(
        event.get("kind") == "index_restore" and "CREATE UNIQUE INDEX" in event.get("statement", "") for event in events
    )


def test_subprocess_event_disabled(tmp_path):
    db_path, ini_path, event_path = prepare_subprocess_fixture(tmp_path, event_enabled=False)
    run = run_upgrade(db_path, ini_path, event_path=event_path)
    assert run.returncode == 0, run.stderr
    assert "IsADirectoryError" not in run.stderr
    assert event_path is None
    assert not (tmp_path / "events.jsonl").exists()
    assert_post_migration_state(db_path)


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


def test_normal_and_forced_limit_have_identical_post_state(tmp_path):
    normal = tmp_path / "normal.db"
    forced = tmp_path / "forced.db"
    normal_ini = tmp_path / "normal.ini"
    forced_ini = tmp_path / "forced.ini"
    create_fixture_database(normal)
    create_fixture_database(forced)
    write_copy_ini(normal_ini, normal.resolve(strict=True))
    write_copy_ini(forced_ini, forced.resolve(strict=True))
    with sqlite3.connect(normal) as conn:
        pre_facts = full_table_snapshot(conn, "facts")
        pre_receipts = full_table_snapshot(conn, "receipts")
        pre_outbox = full_table_snapshot(conn, "outbox_entries")
    regular = run_upgrade(normal, normal_ini)
    constrained = run_forced_limit(forced, forced_ini, tmp_path)
    assert regular.returncode == constrained.returncode == 0
    regular_summary = parse_summary(regular.stdout)
    forced_summary = parse_summary(constrained.stdout)
    assert regular_summary == forced_summary
    expected_deleted = set(regular_summary["deleted_fact_ids"])
    pre_fact_ids = {row[0] for row in pre_facts}
    assert expected_deleted == pre_fact_ids - {
        *regular_summary["kept"]["fact_ids"],
    }
    expected_receipts = {row for row in pre_receipts if row[1] == "fact" and row[0] in expected_deleted}
    expected_outbox = {row for row in pre_outbox if row[1] == "fact" and row[2] in expected_deleted}
    assert regular_summary["deleted_receipt_count"] == len(expected_receipts)
    assert regular_summary["deleted_outbox_count"] == len(expected_outbox)
    assert regular_summary["deleted_receipt_count"] == forced_summary["deleted_receipt_count"]
    assert regular_summary["deleted_outbox_count"] == forced_summary["deleted_outbox_count"]
    with sqlite3.connect(normal) as left, sqlite3.connect(forced) as right:
        left_state = full_state_snapshot(left)
        right_state = full_state_snapshot(right)
    assert_full_state_equal(left_state, right_state)


def test_full_state_equality_regression_detects_non_fact_mutation(tmp_path):
    """The equality proof must not ignore lifecycle/decision-side state."""
    left = tmp_path / "left.db"
    right = tmp_path / "right.db"
    create_fixture_database(left)
    create_fixture_database(right)
    with sqlite3.connect(left) as conn:
        left_state = full_state_snapshot(conn)
    with sqlite3.connect(right) as conn:
        conn.execute("UPDATE lifecycle_events SET reason='mutated' WHERE id='le-fact-1'")
        conn.commit()
        right_state = full_state_snapshot(conn)
    with pytest.raises(AssertionError, match="lifecycle_events"):
        assert_full_state_equal(left_state, right_state)


def test_read_only_source_probe_and_sidecar_contract(tmp_path):
    source, destination = make_backup_fixture(tmp_path)
    with sqlite3.connect(f"file:{source.resolve()}?mode=ro", uri=True) as conn:
        with pytest.raises(sqlite3.OperationalError, match="readonly|read-only"):
            conn.execute("INSERT INTO __b2_read_only_probe(x) VALUES (0)")
    guarded_backup(source, destination)
    assert source_alembic_revision(source) == "6a7b8c9d0e1f"
    assert not Path(str(source) + "-wal").exists() or Path(str(source) + "-wal").is_file()
    assert not Path(str(source) + "-shm").exists() or Path(str(source) + "-shm").is_file()
    assert not Path(str(destination) + "-wal").exists()
    assert not Path(str(destination) + "-shm").exists()


def test_live_db_copy_upgrade_preserves_source_revision(tmp_path):
    source = source_db_path()
    assert source.exists()
    destination = tmp_path / "live-copy.db"
    captured = source_alembic_revision(source)
    metadata_before = source_metadata_snapshot(source)
    guarded_backup(source, destination)
    assert source_alembic_revision(source) == captured
    assert source_metadata_snapshot(source) == metadata_before
    with sqlite3.connect(str(destination)) as conn:
        assert conn.execute("SELECT version_num FROM alembic_version").fetchone()[0] == captured
