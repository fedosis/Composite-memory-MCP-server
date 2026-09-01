"""Persist canonical fact dedup keys and enforce active uniqueness."""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence, Union

import sqlalchemy as sa
from alembic import op
from storage.dedup import fact_dedup_key

revision: str = "b2f3a4c5d6e7"
down_revision: Union[str, Sequence[str], None] = "6a7b8c9d0e1f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_ACTIVE_STATES = ("candidate", "validated", "active")
_INDEX_NAME = "uq_facts_spo_active"
_PAGE_SIZE = 1000
_REQUESTED_DML_BATCH = 400
_MAX_BOUND_PARAMS = 900

_log = logging.getLogger(__name__)


def _record_event(name: str, **fields: Any) -> None:
    event_path = os.environ.get("B2_EVENT_PATH")
    if event_path is not None:
        payload = {"name": name, **fields}
        with Path(event_path).open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(payload, sort_keys=True) + "\n")
    else:
        assert "B2_EVENT_PATH" not in os.environ


def sqlite_base_error_code(exc: sqlite3.OperationalError) -> int | None:
    code = getattr(exc, "sqlite_errorcode", None)
    extended = getattr(exc, "sqlite_extended_errorcode", None)
    candidate = code if code is not None else extended
    if candidate is None:
        return None
    return int(candidate) & 0xFF


def _source_uri(path: Path) -> str:
    return f"file:{path.resolve(strict=True)}?mode=ro"


def source_alembic_revision(source: Path) -> str:
    with sqlite3.connect(_source_uri(source), uri=True) as conn:
        row = conn.execute("SELECT version_num FROM alembic_version").fetchone()
    assert row is not None
    return str(row[0])


def source_metadata_snapshot(source: Path) -> dict[str, Any]:
    resolved = source.resolve(strict=True)
    with sqlite3.connect(_source_uri(resolved), uri=True) as conn:
        schema = tuple(
            tuple(row)
            for row in conn.execute(
                "SELECT type, name, tbl_name, sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type, name"
            )
        )
        facts_schema = tuple(tuple(row) for row in conn.execute("PRAGMA table_info('facts')"))
        version = conn.execute("SELECT version_num FROM alembic_version").fetchone()
    assert version is not None
    return {
        "realpath": str(resolved),
        "schema": schema,
        "facts_schema": facts_schema,
        "alembic_version": str(version[0]),
    }


def guarded_cleanup(destination: Path, source: Path) -> None:
    dst = destination.resolve(strict=False)
    src = source.resolve(strict=True)
    assert not destination.is_symlink()
    assert dst != src
    assert dst.parent == destination.parent.resolve()
    for path in (
        destination,
        Path(str(destination) + "-wal"),
        Path(str(destination) + "-shm"),
    ):
        if path.exists() or path.is_symlink():
            assert not path.is_symlink()
            assert path.resolve(strict=False) != src
            path.unlink()


def guarded_backup(source: Path, destination: Path) -> None:
    src = source.resolve(strict=True)
    dst = destination.resolve(strict=False)
    assert not source.is_symlink()
    assert not destination.is_symlink()
    assert src != dst
    assert dst.parent.exists()
    for sidecar in (Path(str(dst) + "-wal"), Path(str(dst) + "-shm")):
        assert sidecar.resolve(strict=False) != src

    captured_revision = source_alembic_revision(src)
    attempts, delay = 5, 0.1

    for attempt in range(attempts):
        try:
            with sqlite3.connect(_source_uri(src), uri=True) as ro:
                with sqlite3.connect(str(dst)) as out:
                    ro.backup(out)
                    out.commit()
            assert source_alembic_revision(src) == captured_revision
            with sqlite3.connect(str(dst)) as check:
                row = check.execute("SELECT version_num FROM alembic_version").fetchone()
                assert row is not None
                assert str(row[0]) == captured_revision
                assert check.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            return
        except sqlite3.OperationalError as exc:
            base_code = sqlite_base_error_code(exc)
            retryable = base_code in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}
            if base_code is None:
                message = str(exc).strip().lower()
                retryable = message.startswith(("database is locked", "database is busy"))
            if not retryable or attempt == attempts - 1:
                guarded_cleanup(destination, source)
                raise
            guarded_cleanup(destination, source)
            assert not dst.exists()
            assert not Path(str(dst) + "-wal").exists()
            assert not Path(str(dst) + "-shm").exists()
            time.sleep(delay)
            delay *= 2
        except Exception:
            guarded_cleanup(destination, source)
            raise

    raise AssertionError("unreachable backup loop")


def _effective_variable_limit(bind: sa.engine.Connection) -> int:
    raw = bind.connection.connection
    constant = getattr(sqlite3, "SQLITE_LIMIT_VARIABLE_NUMBER", None)
    if constant is not None and hasattr(raw, "getlimit"):
        return min(_MAX_BOUND_PARAMS, raw.getlimit(constant))
    return _MAX_BOUND_PARAMS


def _effective_batch(bind: sa.engine.Connection, *, params_per_row: int, fixed_params: int) -> int:
    limit = _effective_variable_limit(bind)
    if params_per_row < 0 or fixed_params < 0 or fixed_params >= limit:
        raise RuntimeError("invalid B2 parameter budget")
    result = min(_REQUESTED_DML_BATCH, (limit - fixed_params) // max(params_per_row, 1))
    if result < 1:
        raise RuntimeError("B2 effective SQLite limit cannot fit one row")
    return result


def _execute(
    bind: sa.engine.Connection,
    statement: str,
    params: dict[str, Any],
    *,
    kind: str,
    params_per_row: int = 0,
    fixed_params: int = 0,
    page_size: int | None = None,
    row_wise: bool = False,
):
    batch = _effective_batch(bind, params_per_row=params_per_row, fixed_params=fixed_params) if params_per_row else None
    count = len(params)
    if count > _effective_variable_limit(bind):
        raise RuntimeError("B2 parameter guard exceeded effective limit")
    _record_event(
        "statement",
        kind=kind,
        statement=statement,
        parameter_count=count,
        params_per_row=params_per_row,
        fixed_params=fixed_params,
        dml_batch=batch,
        page_size=page_size,
        row_wise=row_wise,
    )
    return bind.execute(sa.text(statement), params)


def _scalar(bind: sa.engine.Connection, statement: str, params: dict[str, Any] | None = None) -> Any:
    result = _execute(bind, statement, params or {}, kind="scalar_select")
    return result.scalar_one()


def _scalar_one_or_none(bind: sa.engine.Connection, statement: str, params: dict[str, Any] | None = None) -> Any:
    result = _execute(bind, statement, params or {}, kind="scalar_select")
    return result.scalar_one_or_none()


def current_version(bind: sa.engine.Connection) -> str:
    return str(_scalar(bind, "SELECT version_num FROM alembic_version"))


def table_exists(bind: sa.engine.Connection, name: str) -> bool:
    return bool(
        _scalar_one_or_none(
            bind,
            "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = :name",
            {"name": name},
        )
    )


def table_info(bind: sa.engine.Connection, name: str) -> list[sa.RowMapping]:
    return list(_execute(bind, f"PRAGMA table_info({name!r})", {}, kind="schema_select").mappings().all())


def primary_key_info(bind: sa.engine.Connection, table: str) -> list[str]:
    rows = _execute(bind, f"PRAGMA table_info({table!r})", {}, kind="schema_select").mappings().all()
    return [row["name"] for row in rows if int(row["pk"]) > 0]


def index_list(bind: sa.engine.Connection, table: str) -> list[dict[str, Any]]:
    return [dict(row) for row in _execute(bind, f"PRAGMA index_list({table!r})", {}, kind="index_select").mappings()]


def index_exists(bind: sa.engine.Connection, name: str) -> bool:
    row = _execute(
        bind, "SELECT 1 FROM sqlite_master WHERE type='index' AND name=:name", {"name": name}, kind="index_select"
    ).scalar_one_or_none()
    return row is not None


def read_index_sql(bind: sa.engine.Connection, name: str) -> str | None:
    return _execute(
        bind, "SELECT sql FROM sqlite_master WHERE type='index' AND name=:name", {"name": name}, kind="index_select"
    ).scalar_one_or_none()


def _normalize_sql_whitespace(text: str) -> str:
    return " ".join(text.split())


def canonical_pinned_index_sql(sql: str) -> str:
    """Parse the deliberately constrained pinned-index compatibility matrix.

    Accepted forms are CREATE UNIQUE INDEX with the pinned name, the facts
    table and dedup_key column (bare, double-quoted, or bracket-quoted), and
    the three single-quoted ACTIVE literals in order. Everything else fails
    closed rather than being normalized heuristically.
    """
    raw = sql.strip()
    if raw.endswith(";"):
        raw = raw[:-1].rstrip()
    identifier = r'(?:facts|"facts"|\[facts\])'
    column = r'(?:dedup_key|"dedup_key"|\[dedup_key\])'
    literal = r"'candidate'\s*,\s*'validated'\s*,\s*'active'"
    pattern = (
        rf"CREATE\s+UNIQUE\s+INDEX\s+{re.escape(_INDEX_NAME)}\s+ON\s+"
        rf"{identifier}\s*\(\s*{column}\s*\)\s+WHERE\s+"
        rf"lifecycle_state\s+IN\s*\(\s*{literal}\s*\)"
    )
    if re.fullmatch(pattern, raw, flags=re.IGNORECASE) is None:
        raise ValueError("malformed pinned index SQL")
    return EXPECTED_PINNED_CANONICAL


EXPECTED_PINNED_CANONICAL = "unique=1;columns=dedup_key;where=lifecycle_state in ('candidate', 'validated', 'active')"


def inspect_pinned_index(bind: sa.engine.Connection) -> str:
    rows = _execute(bind, "PRAGMA index_list('facts')", {}, kind="index_select").mappings().all()
    for row in rows:
        if row["name"] != _INDEX_NAME:
            continue
        sql = read_index_sql(bind, _INDEX_NAME)
        if sql is None:
            raise ValueError("malformed pinned index")
        canonical = canonical_pinned_index_sql(sql)
        columns = [
            item["name"]
            for item in _execute(bind, "PRAGMA index_info('uq_facts_spo_active')", {}, kind="index_select")
            .mappings()
            .all()
        ]
        if canonical == EXPECTED_PINNED_CANONICAL and int(row["unique"]) == 1 and columns == ["dedup_key"]:
            return "compatible"
        raise ValueError("pinned index collision")
    return "absent"


def _tables_with_orphans(bind: sa.engine.Connection) -> None:
    orphan_receipt = _execute(
        bind,
        "SELECT id FROM receipts WHERE memory_type = 'fact' AND id NOT IN (SELECT id FROM facts)",
        {},
        kind="orphan_check",
    ).fetchone()
    if orphan_receipt is not None:
        raise RuntimeError("orphan fact receipt present before migration")
    orphan_outbox = _execute(
        bind,
        "SELECT record_id FROM outbox_entries WHERE record_type = 'fact' AND record_id NOT IN (SELECT id FROM facts)",
        {},
        kind="orphan_check",
    ).fetchone()
    if orphan_outbox is not None:
        raise RuntimeError("orphan fact outbox present before migration")


def _ensure_preflight(bind: sa.engine.Connection) -> str:
    assert current_version(bind) == "6a7b8c9d0e1f"
    assert table_exists(bind, "facts")
    assert table_exists(bind, "receipts")
    assert table_exists(bind, "outbox_entries")
    assert "dedup_key" not in {row["name"] for row in table_info(bind, "facts")}
    _tables_with_orphans(bind)
    return inspect_pinned_index(bind)


def _fact_rows(bind: sa.engine.Connection):
    last_id = ""
    while True:
        page = list(
            _execute(
                bind,
                "SELECT id, subject, predicate, object, confidence, created_at, lifecycle_state "
                "FROM facts WHERE id > :last_id ORDER BY id ASC LIMIT :limit",
                {"last_id": last_id, "limit": _PAGE_SIZE},
                kind="page_select",
                page_size=_PAGE_SIZE,
            )
            .mappings()
            .all()
        )
        if not page:
            break
        yield from page
        last_id = str(page[-1]["id"])


def _update_dedup_keys(bind: sa.engine.Connection) -> None:
    batch_size = _effective_batch(bind, params_per_row=2, fixed_params=1)
    chunk = []
    for row in _fact_rows(bind):
        chunk.append(row)
        if len(chunk) < batch_size:
            continue
        params: dict[str, Any] = {"guard": 1}
        value_rows = []
        for index, row in enumerate(chunk):
            id_key = f"id_{index}"
            key_key = f"key_{index}"
            key = fact_dedup_key(row["subject"], row["predicate"], row["object"])
            params[id_key] = row["id"]
            params[key_key] = key
            value_rows.append(f"(:{id_key}, :{key_key})")
        statement = (
            "WITH data(id, dedup_key) AS (VALUES " + ", ".join(value_rows) + ") UPDATE facts SET dedup_key = "
            "(SELECT data.dedup_key FROM data WHERE data.id = facts.id) "
            "WHERE :guard = 1 AND dedup_key IS NULL "
            "AND id IN (SELECT id FROM data)"
        )
        _execute(
            bind,
            statement,
            params,
            kind="case_update",
            params_per_row=2,
            fixed_params=1,
        )
        chunk = []
    if chunk:
        params = {"guard": 1}
        value_rows = []
        for index, row in enumerate(chunk):
            id_key, key_key = f"id_{index}", f"key_{index}"
            key = fact_dedup_key(row["subject"], row["predicate"], row["object"])
            params[id_key], params[key_key] = row["id"], key
            value_rows.append(f"(:{id_key}, :{key_key})")
        _execute(
            bind,
            "WITH data(id, dedup_key) AS (VALUES " + ", ".join(value_rows) + ") "
            "UPDATE facts SET dedup_key=(SELECT data.dedup_key FROM data WHERE data.id=facts.id) "
            "WHERE :guard=1 AND dedup_key IS NULL AND id IN (SELECT id FROM data)",
            params,
            kind="case_update",
            params_per_row=2,
            fixed_params=1,
        )


def _group_fact_rows(bind: sa.engine.Connection):
    last_key = ""
    last_id = ""
    while True:
        page = list(
            _execute(
                bind,
                "SELECT id, dedup_key, confidence, created_at, lifecycle_state "
                "FROM facts WHERE dedup_key IS NOT NULL AND "
                "(dedup_key > :last_key OR (dedup_key = :last_key AND id > :last_id)) "
                "ORDER BY dedup_key ASC, id ASC LIMIT :limit",
                {"last_key": last_key, "last_id": last_id, "limit": _PAGE_SIZE},
                kind="page_select",
                page_size=_PAGE_SIZE,
            )
            .mappings()
            .all()
        )
        if not page:
            break
        yield from page
        last_key = str(page[-1]["dedup_key"])
        last_id = str(page[-1]["id"])


def _parse_created_at(value: Any) -> tuple[Any, str]:
    if isinstance(value, datetime):
        return value, value.isoformat()
    text = str(value)
    try:
        return datetime.fromisoformat(text), text
    except ValueError:
        return text, text


def _keeper_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    active_rank = 0 if row["lifecycle_state"] in _ACTIVE_STATES else 1
    created_value, created_text = _parse_created_at(row["created_at"])
    return (
        active_rank,
        -float(row["confidence"]),
        created_value,
        str(row["id"]),
        created_text,
    )


def _stream_keeper_decisions(rows):
    current_key: str | None = None
    group: list[dict[str, Any]] = []

    def flush():
        if not group:
            return None
        ordered = sorted(group, key=_keeper_sort_key)
        return str(ordered[0]["id"]), tuple(str(row["id"]) for row in ordered[1:])

    for row in rows:
        key = str(row["dedup_key"])
        if current_key is None or key != current_key:
            decision = flush()
            if decision is not None:
                yield decision
            group = [row]
            current_key = key
        else:
            group.append(row)
    decision = flush()
    if decision is not None:
        yield decision


def _delete_ids(bind: sa.engine.Connection, table: str, column: str, ids: list[str]) -> int:
    deleted = 0
    if not ids:
        return 0
    batch_size = _effective_batch(bind, params_per_row=1, fixed_params=1)
    for start in range(0, len(ids), batch_size):
        chunk = ids[start : start + batch_size]
        params = {"guard": 1}
        placeholders = []
        for index, value in enumerate(chunk):
            name = f"id_{index}"
            params[name] = value
            placeholders.append(f":{name}")
        statement = f"DELETE FROM {table} WHERE :guard = 1 AND {column} IN (" + ", ".join(placeholders) + ")"
        result = _execute(
            bind,
            statement,
            params,
            kind="child_delete" if table != "facts" else "fact_delete",
            params_per_row=1,
            fixed_params=1,
        )
        deleted += int(result.rowcount or 0)
    return deleted


def _save_index_state(bind: sa.engine.Connection) -> list[dict[str, Any]]:
    saved: list[dict[str, Any]] = []
    for row in _execute(bind, "PRAGMA index_list('facts')", {}, kind="index_snapshot").mappings().all():
        name = str(row["name"])
        sql = read_index_sql(bind, name)
        columns = [
            str(item["name"])
            for item in _execute(bind, f"PRAGMA index_info({name!r})", {}, kind="index_snapshot").mappings().all()
        ]
        saved.append(
            {
                "name": name,
                "unique": int(row["unique"]),
                "origin": row["origin"],
                "partial": int(row["partial"]),
                "columns": columns,
                "sql": sql,
                "canonical": None,
            }
        )
        if name == _INDEX_NAME and sql is not None:
            try:
                saved[-1]["canonical"] = canonical_pinned_index_sql(sql)
            except ValueError:
                saved[-1]["canonical"] = None
    return saved


def _full_row_snapshot(bind: sa.engine.Connection, table: str) -> tuple[tuple[Any, ...], ...]:
    """Capture every declared column, not a hand-picked identity projection."""
    return tuple(
        tuple(row) for row in _execute(bind, f"SELECT * FROM {table} ORDER BY rowid", {}, kind="full_row_snapshot")
    )


def _full_state_snapshot(bind: sa.engine.Connection) -> dict[str, Any]:
    tables = (
        "facts",
        "receipts",
        "outbox_entries",
        "decisions",
        "lifecycle_states",
        "lifecycle_events",
        "claim_relations",
    )
    return {
        "rows": {table: _full_row_snapshot(bind, table) for table in tables},
        "facts_schema": tuple(tuple(row) for row in table_info(bind, "facts")),
        "facts_indexes": tuple(tuple(sorted(index.items())) for index in _save_index_state(bind)),
    }


def _assert_untouched_rows(before: dict[str, Any], after: dict[str, Any], tables: Sequence[str]) -> None:
    for table in tables:
        assert after["rows"][table] == before["rows"][table], f"mutated untouched {table} rows"


def _recreate_saved_indexes(
    bind: sa.engine.Connection,
    saved: list[dict[str, Any]],
    pinned_state: str,
) -> None:
    for index in saved:
        if index["name"].startswith("sqlite_autoindex_") and index["origin"] == "pk":
            continue
        if index["name"] == _INDEX_NAME and pinned_state == "compatible" and index_exists(bind, _INDEX_NAME):
            continue
        if index["name"] == _INDEX_NAME and pinned_state == "absent":
            if not index_exists(bind, _INDEX_NAME):
                op.create_index(
                    _INDEX_NAME,
                    "facts",
                    ["dedup_key"],
                    unique=True,
                    sqlite_where=sa.text("lifecycle_state IN ('candidate', 'validated', 'active')"),
                )
            continue
        if index["sql"] is None:
            raise RuntimeError(f"cannot restore saved index {index['name']}")
        if not index_exists(bind, index["name"]):
            _execute(bind, index["sql"], {}, kind="index_restore")


def _expected_pinned_sql() -> str:
    return (
        "CREATE UNIQUE INDEX uq_facts_spo_active ON facts (dedup_key) "
        "WHERE lifecycle_state IN ('candidate', 'validated', 'active')"
    )


def upgrade() -> None:
    bind = op.get_bind()
    raw = bind.connection.connection
    observed_limit = (
        raw.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER)
        if hasattr(raw, "getlimit") and hasattr(sqlite3, "SQLITE_LIMIT_VARIABLE_NUMBER")
        else _MAX_BOUND_PARAMS
    )
    _record_event("observed_limit", observed_limit=observed_limit)

    pinned_state = _ensure_preflight(bind)
    if os.environ.get("B2_FAIL_BEFORE_SUMMARY"):
        raise RuntimeError("B2 fail-before-summary requested")
    saved_indexes = _save_index_state(bind)
    pre_state = _full_state_snapshot(bind)
    pre_rebuild_ids = {str(row[0]) for row in _execute(bind, "SELECT id FROM facts", {}, kind="identity_snapshot")}
    pre_rebuild_count = len(pre_rebuild_ids)

    op.add_column("facts", sa.Column("dedup_key", sa.String(), nullable=True))

    _update_dedup_keys(bind)
    assert int(_scalar(bind, "SELECT COUNT(*) FROM facts")) == int(_scalar(bind, "SELECT COUNT(dedup_key) FROM facts"))

    facts_copy = sa.Table(
        "facts",
        sa.MetaData(),
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("subject", sa.String(), nullable=False),
        sa.Column("predicate", sa.String(), nullable=False),
        sa.Column("object", sa.String(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("source", sa.String(), nullable=True),
        sa.Column("creator", sa.String(), nullable=False),
        sa.Column("created_at", sa.String(), nullable=False),
        sa.Column("updated_at", sa.String(), nullable=False),
        sa.Column("verification_status", sa.String(), nullable=False),
        sa.Column("lifecycle_state", sa.String(), nullable=False),
        sa.Column("version", sa.String(), nullable=False),
        sa.Column("dedup_key", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    with op.batch_alter_table(
        "facts",
        recreate="always",
        copy_from=facts_copy,
        reflect_args=[sa.Column("dedup_key", sa.String(), nullable=True)],
    ) as batch_op:
        batch_op.alter_column("dedup_key", existing_type=sa.String(), nullable=False)

    keeper_ids: list[str] = []
    deleted_fact_ids: list[str] = []
    for keeper_id, group_deletes in _stream_keeper_decisions(_group_fact_rows(bind)):
        keeper_ids.append(keeper_id)
        deleted_fact_ids.extend(group_deletes)
    keeper_ids = sorted(keeper_ids)
    deleted_fact_ids = sorted(set(deleted_fact_ids))
    pre_delete_ids = {str(row[0]) for row in _execute(bind, "SELECT id FROM facts", {}, kind="identity_snapshot")}

    _execute(bind, "CREATE TEMP TABLE b2_deleted_fact_ids (fact_id VARCHAR PRIMARY KEY)", {}, kind="stream_state")
    insert_batch = _effective_batch(bind, params_per_row=1, fixed_params=0)
    for start in range(0, len(deleted_fact_ids), insert_batch):
        chunk = deleted_fact_ids[start : start + insert_batch]
        params = {f"id_{index}": value for index, value in enumerate(chunk)}
        values = ", ".join(f"(:id_{index})" for index in range(len(chunk)))
        _execute(
            bind,
            f"INSERT INTO b2_deleted_fact_ids(fact_id) VALUES {values}",
            params,
            kind="stream_state",
            params_per_row=1,
        )

    def deleted_id_chunks():
        last_id = ""
        while True:
            rows = _execute(
                bind,
                "SELECT fact_id FROM b2_deleted_fact_ids WHERE fact_id > :last_id ORDER BY fact_id LIMIT :limit",
                {"last_id": last_id, "limit": _PAGE_SIZE},
                kind="stream_select",
                page_size=_PAGE_SIZE,
            ).fetchall()
            if not rows:
                return
            yield [str(row[0]) for row in rows]
            last_id = str(rows[-1][0])

    def delete_owned_children(table: str, column: str, kind: str) -> int:
        deleted = 0
        for chunk in deleted_id_chunks():
            params = {f"id_{i}": value for i, value in enumerate(chunk)}
            placeholders = ", ".join(f":id_{i}" for i in range(len(chunk)))
            owner = "memory_type='fact' AND id" if table == "receipts" else "record_type='fact' AND record_id"
            result = _execute(
                bind,
                f"SELECT {column} FROM {table} WHERE {owner} IN ({placeholders})",
                params,
                kind=kind,
                params_per_row=1,
            )
            child_batch = [str(row[0]) for row in result.fetchall()]
            deleted += _delete_ids(bind, table, column, child_batch)
        return deleted

    receipt_deleted = delete_owned_children("receipts", "id", "child_select") if deleted_fact_ids else 0
    outbox_deleted = delete_owned_children("outbox_entries", "record_id", "child_select") if deleted_fact_ids else 0
    fact_deleted = sum(_delete_ids(bind, "facts", "id", chunk) for chunk in deleted_id_chunks())

    assert fact_deleted == len(deleted_fact_ids)
    deleted_receipt_count = receipt_deleted
    deleted_outbox_count = outbox_deleted
    assert pre_delete_ids == pre_rebuild_ids

    _recreate_saved_indexes(bind, saved_indexes, pinned_state)

    assert primary_key_info(bind, "facts") == ["id"]
    columns = table_info(bind, "facts")
    assert len(columns) == 13
    assert [column["name"] for column in columns] == [
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
    assert int(columns[-1]["notnull"]) == 1
    post_ids = {str(row[0]) for row in _execute(bind, "SELECT id FROM facts", {}, kind="identity_scan")}
    assert post_ids == pre_rebuild_ids - set(deleted_fact_ids)
    assert len(post_ids) == pre_rebuild_count - len(deleted_fact_ids)
    for row in _execute(bind, "SELECT subject, predicate, object, dedup_key FROM facts", {}, kind="dedup_verify"):
        assert str(row[3]) == fact_dedup_key(row[0], row[1], row[2])

    if not index_exists(bind, _INDEX_NAME):
        if pinned_state == "absent":
            op.create_index(
                _INDEX_NAME,
                "facts",
                ["dedup_key"],
                unique=True,
                sqlite_where=sa.text("lifecycle_state IN ('candidate', 'validated', 'active')"),
            )
        else:
            _execute(bind, _expected_pinned_sql(), {}, kind="index_restore")

    assert index_exists(bind, _INDEX_NAME)
    saved_pinned_sql = next(
        (index["sql"] for index in saved_indexes if index["name"] == _INDEX_NAME),
        None,
    )
    current_sql = read_index_sql(bind, _INDEX_NAME) or ""
    if saved_pinned_sql is not None:
        assert canonical_pinned_index_sql(current_sql) == canonical_pinned_index_sql(saved_pinned_sql)
    else:
        assert canonical_pinned_index_sql(current_sql) == EXPECTED_PINNED_CANONICAL

    post_state = _full_state_snapshot(bind)
    _assert_untouched_rows(
        pre_state, post_state, ("decisions", "lifecycle_states", "lifecycle_events", "claim_relations")
    )
    deleted_ids = set(deleted_fact_ids)
    pre_fact_ids = {str(row[0]) for row in pre_state["rows"]["facts"]}
    pre_facts = {str(row[0]): row for row in pre_state["rows"]["facts"]}
    post_facts = {str(row[0]): row for row in post_state["rows"]["facts"]}
    expected_survivors = pre_fact_ids - deleted_ids
    assert set(post_facts) == expected_survivors
    for fact_id in expected_survivors:
        assert post_facts[fact_id][:-1] == pre_facts[fact_id], f"mutated fact row {fact_id}"

    pre_receipts = pre_state["rows"]["receipts"]
    post_receipts = post_state["rows"]["receipts"]
    pre_owned_receipts = tuple(
        row for row in pre_receipts if str(row[1]) == "fact" and str(row[0]) in pre_fact_ids
    )
    deleted_receipts = tuple(row for row in pre_owned_receipts if str(row[0]) in deleted_ids)
    surviving_receipts = tuple(row for row in pre_owned_receipts if str(row[0]) not in deleted_ids)
    post_owned_receipts = tuple(
        row for row in post_receipts if str(row[1]) == "fact" and str(row[0]) in pre_fact_ids
    )
    assert sorted(post_owned_receipts) == sorted(surviving_receipts)
    assert not tuple(row for row in post_owned_receipts if str(row[0]) in deleted_ids)
    assert receipt_deleted == len(deleted_receipts)
    assert tuple(row for row in post_receipts if str(row[1]) != "fact") == tuple(
        row for row in pre_receipts if str(row[1]) != "fact"
    ), "mutated or unexpected non-fact receipt row"

    pre_outbox = pre_state["rows"]["outbox_entries"]
    post_outbox = post_state["rows"]["outbox_entries"]
    pre_owned_outbox = tuple(
        row for row in pre_outbox if str(row[1]) == "fact" and str(row[2]) in pre_fact_ids
    )
    deleted_outbox = tuple(row for row in pre_owned_outbox if str(row[2]) in deleted_ids)
    surviving_outbox = tuple(row for row in pre_owned_outbox if str(row[2]) not in deleted_ids)
    post_owned_outbox = tuple(
        row for row in post_outbox if str(row[1]) == "fact" and str(row[2]) in pre_fact_ids
    )
    assert sorted(post_owned_outbox) == sorted(surviving_outbox)
    assert not tuple(row for row in post_owned_outbox if str(row[2]) in deleted_ids)
    assert outbox_deleted == len(deleted_outbox)
    assert tuple(row for row in post_outbox if str(row[1]) != "fact") == tuple(
        row for row in pre_outbox if str(row[1]) != "fact"
    ), "mutated or unexpected non-fact outbox row"
    assert tuple(row for row in post_outbox if str(row[1]) == "decision") == tuple(
        row for row in pre_outbox if str(row[1]) == "decision"
    ), "mutated decision-owned outbox row"
    assert tuple(row for row in post_receipts if str(row[1]) == "decision") == tuple(
        row for row in pre_receipts if str(row[1]) == "decision"
    ), "mutated decision-owned receipt row"

    remaining_dupe_keys = [
        str(row[0])
        for row in _execute(
            bind,
            "SELECT dedup_key, COUNT(*) AS surviving_row_count FROM facts WHERE dedup_key IS NOT NULL "
            "GROUP BY dedup_key HAVING COUNT(*) > 1 ORDER BY dedup_key ASC",
            {},
            kind="remaining_dupe_verify",
        ).fetchall()
    ]
    summary = {
        "schema": "B2-A3",
        "kept": {"fact_ids": keeper_ids, "fact_count": len(keeper_ids)},
        "deleted_fact_ids": sorted(deleted_fact_ids),
        "deleted_fact_count": len(deleted_fact_ids),
        "deleted_receipt_count": deleted_receipt_count,
        "deleted_outbox_count": deleted_outbox_count,
        "remaining_dupe_keys": remaining_dupe_keys,
        "remaining_dupe_count": len(remaining_dupe_keys),
    }
    if os.environ.get("B2_FAIL_BEFORE_SUMMARY"):
        raise RuntimeError("B2 fail-before-summary requested")
    print("FACT_DEDUPE_SUMMARY " + json.dumps(summary, separators=(",", ":"), sort_keys=True), flush=True)


def downgrade() -> None:
    op.drop_index(_INDEX_NAME, table_name="facts")
    op.drop_column("facts", "dedup_key")
