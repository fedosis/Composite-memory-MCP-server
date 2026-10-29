"""Async SQLite provider using SQLAlchemy + aiosqlite.

Per ADR-002: Facts use SQLite/PostgreSQL storage.
Per ADR-010: v0.1a uses SQLite only.

v0.6: Refactored to delegate to the new storage layer
(storage/models + storage/repositories) instead of maintaining
duplicate inline ORM models.
"""

import json
import re
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional, TypeVar

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from storage.base import Base
from storage.models.decision import DecisionORM
from storage.models.fact import FactORM
from storage.models.receipt import MemoryReceiptORM
from storage.models.skill import SkillORM
from storage.outbox import OutboxRepository
from storage.repositories import (
    BeliefRepository,
    DecisionRepository,
    EvidenceRepository,
    FactRepository,
    ReceiptRepository,
    RelationRepository,
    SkillRepository,
)
from storage.sqlite_support import apply_sqlite_pragmas_async, install_busy_timeout_listener, validate_busy_timeout_ms

from memory_server.models import (
    Belief,
    Decision,
    Evidence,
    Fact,
    MemoryReceipt,
    Skill,
)

T = TypeVar("T")


class SQLiteProvider:
    """Async SQLite provider for CRUD operations on facts and receipts.

    v0.6: Delegates to the new storage layer (storage/repositories)
    for all CRUD operations. Keeps the same public interface for
    backward compatibility.

    WAL mode is enabled on initialization for concurrent read performance.
    """

    def __init__(
        self,
        url: str = "sqlite+aiosqlite:///memory.db",
        *,
        busy_timeout_ms: int = 5000,
    ):
        self._url = url
        self._busy_timeout_ms = validate_busy_timeout_ms(busy_timeout_ms)
        self._engine = None
        self._session_factory = None
        self._read_index_capable = False

    @property
    def engine(self):
        """Expose the SQLAlchemy engine for sharing with OutboxWorker."""
        return self._engine

    async def _fts_rebuild_required(
        self,
        conn,
        *,
        source_table: str,
        fts_table: str,
    ) -> bool:
        """Return True when an external-content FTS table needs initial backfill.

        We only rebuild when source rows already exist and the FTS table is still
        empty. This keeps normal startup fast for already-indexed databases while
        preserving correctness for fresh/migrated databases that need an initial
        backfill.
        """
        source_has_rows = bool((await conn.exec_driver_sql(f"SELECT 1 FROM {source_table} LIMIT 1")).first())
        if not source_has_rows:
            return False

        fts_has_rows = bool((await conn.exec_driver_sql(f"SELECT 1 FROM {fts_table} LIMIT 1")).first())
        return not fts_has_rows

    async def initialize(self):
        """Create engine, tables, and session factory with WAL mode."""
        self._engine = create_async_engine(self._url, echo=False)
        install_busy_timeout_listener(self._engine, self._busy_timeout_ms)

        # Enable WAL mode for concurrent read performance
        async with self._engine.connect() as conn:
            await apply_sqlite_pragmas_async(
                conn,
                self._busy_timeout_ms,
                context="SQLiteProvider",
                allow_degraded_mode=self._url
                in {
                    "sqlite+aiosqlite://",
                    "sqlite+aiosqlite:///:memory:",
                },
            )

        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            # Create FTS5 virtual table on the facts table
            await conn.exec_driver_sql(
                "CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts "
                "USING fts5(subject, predicate, object, "
                "content=facts, content_rowid=rowid)"
            )
            # Triggers to keep FTS index in sync
            await conn.exec_driver_sql(
                "CREATE TRIGGER IF NOT EXISTS facts_ai AFTER INSERT ON facts BEGIN "
                "INSERT INTO facts_fts(rowid, subject, predicate, object) "
                "VALUES (new.rowid, new.subject, new.predicate, new.object); END"
            )
            await conn.exec_driver_sql(
                "CREATE TRIGGER IF NOT EXISTS facts_ad AFTER DELETE ON facts BEGIN "
                "INSERT INTO facts_fts(facts_fts, rowid, subject, predicate, object) "
                "VALUES('delete', old.rowid, old.subject, old.predicate, old.object); END"
            )
            await conn.exec_driver_sql(
                "CREATE TRIGGER IF NOT EXISTS facts_au AFTER UPDATE ON facts BEGIN "
                "INSERT INTO facts_fts(facts_fts, rowid, subject, predicate, object) "
                "VALUES('delete', old.rowid, old.subject, old.predicate, old.object); "
                "INSERT INTO facts_fts(rowid, subject, predicate, object) "
                "VALUES (new.rowid, new.subject, new.predicate, new.object); END"
            )
            if await self._fts_rebuild_required(
                conn,
                source_table="facts",
                fts_table="facts_fts",
            ):
                await conn.exec_driver_sql("INSERT INTO facts_fts(facts_fts) VALUES ('rebuild')")
            # Create beliefs FTS5 virtual table
            await conn.exec_driver_sql(
                "CREATE VIRTUAL TABLE IF NOT EXISTS beliefs_fts "
                "USING fts5(proposition, "
                "content=beliefs, content_rowid=rowid)"
            )
            # Triggers to keep beliefs FTS index in sync
            await conn.exec_driver_sql(
                "CREATE TRIGGER IF NOT EXISTS beliefs_ai AFTER INSERT ON beliefs BEGIN "
                "INSERT INTO beliefs_fts(rowid, proposition) "
                "VALUES (new.rowid, new.proposition); END"
            )
            await conn.exec_driver_sql(
                "CREATE TRIGGER IF NOT EXISTS beliefs_ad AFTER DELETE ON beliefs BEGIN "
                "INSERT INTO beliefs_fts(beliefs_fts, rowid, proposition) "
                "VALUES('delete', old.rowid, old.proposition); END"
            )
            await conn.exec_driver_sql(
                "CREATE TRIGGER IF NOT EXISTS beliefs_au AFTER UPDATE ON beliefs BEGIN "
                "INSERT INTO beliefs_fts(beliefs_fts, rowid, proposition) "
                "VALUES('delete', old.rowid, old.proposition); "
                "INSERT INTO beliefs_fts(rowid, proposition) "
                "VALUES (new.rowid, new.proposition); END"
            )
            if await self._fts_rebuild_required(
                conn,
                source_table="beliefs",
                fts_table="beliefs_fts",
            ):
                await conn.exec_driver_sql("INSERT INTO beliefs_fts(beliefs_fts) VALUES ('rebuild')")

        # Immutable for this provider lifetime; external migration requires reopen.
        async with self._engine.connect() as conn:
            self._read_index_capable = await self._check_read_index_capability(conn)

        self._session_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def close(self):
        """Dispose of the engine."""
        if self._engine:
            await self._engine.dispose()

    async def _get_session(self) -> AsyncSession:
        return self._session_factory()

    async def _get_fact_repo(self, session: AsyncSession) -> FactRepository:
        return FactRepository(session)

    async def _get_decision_repo(self, session: AsyncSession) -> DecisionRepository:
        return DecisionRepository(session)

    async def _get_skill_repo(self, session: AsyncSession) -> SkillRepository:
        return SkillRepository(session)

    async def _get_receipt_repo(self, session: AsyncSession) -> ReceiptRepository:
        return ReceiptRepository(session)

    async def _get_belief_repo(self, session: AsyncSession) -> BeliefRepository:
        return BeliefRepository(session)

    async def _get_evidence_repo(self, session: AsyncSession) -> EvidenceRepository:
        return EvidenceRepository(session)

    @staticmethod
    def _utc(value: Any) -> str:
        if isinstance(value, datetime):
            dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        else:
            raw = str(value).replace("Z", "+00:00")
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat(timespec="microseconds")

    @staticmethod
    def _storage_bind(value: Any) -> str:
        """Convert a wire timestamp to SQLite DateTime's canonical UTC text."""
        return datetime.fromisoformat(SQLiteProvider._utc(value)).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f")

    async def _check_read_index_capability(self, conn) -> bool:
        """Check all four indexes and stream all timestamps once at startup.

        A revision stamp does not prove canonical data. External writers must preserve
        the storage format for the lifetime of this provider (no mixed UNION).
        """
        for kind in ("fact", "belief", "decision", "skill"):
            table = f"{kind}s"
            name = f"ix_{table}_created_type_id"
            expected = f"CREATE INDEX {name} ON {table} (created_at, CAST('{kind}' AS TEXT), id)"
            row = (
                await conn.exec_driver_sql("SELECT type, tbl_name, sql FROM sqlite_master WHERE name=?", (name,))
            ).first()
            if row is None or row[0] != "index" or row[1] != table or row[2] is None:
                return False
            if (
                re.findall(r"'[^']*'", row[2]) != [f"'{kind}'"]
                or re.sub(r"\s+", "", row[2]).lower() != re.sub(r"\s+", "", expected).lower()
            ):
                return False
            keys = (await conn.exec_driver_sql(f"PRAGMA index_xinfo('{name}')")).all()
            if [(r[0], r[2], r[3], r[4]) for r in keys if r[5]] != [
                (0, "created_at", 0, "BINARY"),
                (1, None, 0, "BINARY"),
                (2, "id", 0, "BINARY"),
            ]:
                return False
        for table in ("facts", "beliefs", "decisions", "skills"):
            async with conn.stream(text(f"SELECT created_at,updated_at FROM {table}")) as result:
                async for batch in result.partitions(1000):
                    for row in batch:
                        for value in row:
                            try:
                                if not isinstance(value, str) or value != self._storage_bind(value):
                                    return False
                            except (ValueError, TypeError, OverflowError):
                                return False
        return True

    @staticmethod
    def _record(kind: str, row: Any) -> dict[str, Any]:
        data = dict(row._mapping)
        data["memory_type"] = kind
        data["created_at"] = SQLiteProvider._utc(data["created_at"])
        data["updated_at"] = SQLiteProvider._utc(data["updated_at"])
        return data

    async def read_memory(self, session: AsyncSession, memory_id: str, memory_type: str | None = None):
        """Read raw storage columns; an untyped collision is a validation error."""
        from memory_server.application.memory_read_service import ReadRecord
        from memory_server.transports.http.errors import HttpError

        tables = (("fact", "facts"), ("belief", "beliefs"), ("decision", "decisions"), ("skill", "skills"))
        hits: list[tuple[str, dict[str, Any]]] = []
        for kind, table in tables:
            if memory_type is not None and memory_type != kind:
                continue
            result = await session.execute(text(f"SELECT * FROM {table} WHERE id=:id LIMIT 1"), {"id": memory_id})
            row = result.first()
            if row is not None:
                hits.append((kind, self._record(kind, row)))
        if len(hits) == 0:
            raise HttpError("E_NOT_FOUND")
        if len(hits) > 1:
            raise HttpError("E_VALIDATION")
        kind, memory = hits[0]
        receipt = await self.read_receipt_metadata(session, memory_id, kind)
        return ReadRecord(memory=memory, memory_type=kind, receipt=receipt)

    def _key_clause(self, prefix: str, key: Any, op: str, *, kind: str | None = None) -> tuple[str, dict[str, Any]]:
        if key is None:
            return "", {}
        memory_type = f"CAST('{kind}' AS TEXT)" if kind is not None else "memory_type"
        return (
            f" AND (created_at,{memory_type},id) {op} (:{prefix}created_at,:{prefix}memory_type,:{prefix}id)",
            {
                f"{prefix}created_at": self._storage_bind(key.created_at)
                if self._read_index_capable
                else key.created_at,
                f"{prefix}memory_type": key.memory_type,
                f"{prefix}id": key.id,
            },
        )

    async def read_memory_page(self, session: AsyncSession, query: Any, *, after: Any = None, upper: Any = None):
        """Read a metadata-filtered UNION page, with a fixed creation-key upper bound."""
        from memory_server.application.memory_read_service import MemorySlice, ReadKey, ReadRecord
        from memory_server.transports.http.errors import HttpError

        if type(query.limit) is not int or not 1 <= query.limit <= 200:
            raise HttpError("E_VALIDATION")
        conn = await session.connection()
        await conn.run_sync(
            lambda c: c.connection.dbapi_connection.run_async(
                lambda a: a.create_function("s07_timestamp", 1, self._utc, deterministic=True)
            )
        )
        tables = (("fact", "facts"), ("belief", "beliefs"), ("decision", "decisions"), ("skill", "skills"))
        allowed = set(query.memory_types or ())
        params: dict[str, Any] = {}
        selects = []
        for kind, table in tables:
            if allowed and kind not in allowed:
                continue
            clauses = ["1=1"]
            if query.lifecycle_states:
                placeholders = ",".join(":" + kind + "_ls" + str(i) for i in range(len(query.lifecycle_states)))
                clauses.append(f"lifecycle_state IN ({placeholders})")
                params.update({f"{kind}_ls{i}": v for i, v in enumerate(query.lifecycle_states)})
            for field, op, value in (
                ("confidence", ">=", query.min_confidence),
                ("confidence", "<=", query.max_confidence),
                ("created_at", ">=", query.created_from),
                ("created_at", "<=", query.created_to),
                ("updated_at", ">=", query.updated_from),
                ("updated_at", "<=", query.updated_to),
            ):
                if value is not None:
                    name = f"{kind}_{field}_{len(params)}"
                    column = (
                        f"s07_timestamp({field})" if field.endswith("_at") and not self._read_index_capable else field
                    )
                    clauses.append(f"{column} {op} :{name}")
                    if field.endswith("_at") and self._read_index_capable:
                        params[name] = self._storage_bind(value)
                    else:
                        params[name] = self._utc(value) if isinstance(value, datetime) else value
            if query.valid_at is not None:
                # Validity is receipt admission metadata, not a new schema field.
                # Missing bounds are open; use the latest stored admission entry.
                params["valid_at"] = self._utc(query.valid_at)
                for field, op in (("valid_from", "<="), ("valid_to", ">")):
                    expr = f"""(SELECT json_extract(j.value,'$.metadata.admission.{field}')
                      FROM receipts r, json_each(CASE WHEN json_valid(r.history) THEN
                        CASE WHEN json_type(r.history)='array' THEN r.history ELSE '[]' END ELSE '[]' END) j
                      WHERE r.id={table}.id AND r.memory_type='{kind}' AND j.type='object'
                        AND json_type(j.value,'$.metadata.admission')='object'
                      ORDER BY CAST(j.key AS INTEGER) DESC LIMIT 1)"""
                    clauses.append(f"({expr} IS NULL OR s07_timestamp({expr}) {op} :valid_at)")
            common = (
                "id confidence source creator created_at updated_at verification_status lifecycle_state version".split()
            )
            variants = {
                "fact": "subject predicate object dedup_key",
                "belief": "proposition source_ids tags",
                "decision": "context choice rejected_alternatives reason",
                "skill": "name purpose steps constraints validation success_rate",
            }
            columns = common + " ".join(variants.values()).split()
            present = set(common + variants[kind].split())
            fields = ",".join(
                f"s07_timestamp({col}) AS {col}"
                if col in ("created_at", "updated_at") and not self._read_index_capable
                else col
                if col in present
                else f"NULL AS {col}"
                for col in columns
            )
            memory_type = f"CAST('{kind}' AS TEXT)" if self._read_index_capable else f"'{kind}'"
            selects.append(
                (kind, f"SELECT {fields}, {memory_type} AS memory_type FROM {table} WHERE {' AND '.join(clauses)}")
            )

        def build(bound):
            # Push both key predicates into EVERY arm, not just the last arm.
            # The legacy wrapper and ISO normalization remain intact as a unit.
            if self._read_index_capable:
                arms = []
                for kind, arm in selects:
                    low, lowp = self._key_clause("a_", after, ">", kind=kind)
                    high, highp = self._key_clause("u_", bound, "<=", kind=kind)
                    params.update(lowp)
                    params.update(highp)
                    arms.append(arm + low + high)
                return " UNION ALL ".join(arms)
            union = " UNION ALL ".join(arm for _, arm in selects)
            low, lowp = self._key_clause("a_", after, ">")
            high, highp = self._key_clause("u_", bound, "<=")
            params.update(lowp)
            params.update(highp)
            return f"SELECT * FROM ({union}) AS memories WHERE 1=1" + low + high

        sql = build(upper)
        if upper is None:
            maxrow = (
                await session.execute(
                    text(sql + " ORDER BY created_at DESC, memory_type DESC, id DESC LIMIT 1"), params
                )
            ).first()
            upper = None if maxrow is None else ReadKey(self._utc(maxrow.created_at), maxrow.memory_type, maxrow.id)
            sql = build(upper)
        rows = (
            await session.execute(
                text(sql + " ORDER BY created_at ASC, memory_type ASC, id ASC LIMIT :page_limit"),
                {**params, "page_limit": int(query.limit) + 1},
            )
        ).all()
        page_rows = rows[: query.limit]
        receipts = await self._read_receipt_metadata_batch(session, tuple((r.id, r.memory_type) for r in page_rows))
        records = tuple(
            ReadRecord(self._record(r.memory_type, r), r.memory_type, receipts.get((r.id, r.memory_type)))
            for r in page_rows
        )
        return MemorySlice(records, len(rows) > query.limit, upper)

    async def _read_receipt_metadata_batch(self, session: AsyncSession, keys: tuple):
        """One bounded receipt query for returned records, never lookahead."""
        if len(keys) > 200 or len(set(keys)) != len(keys):
            raise ValueError("invalid receipt batch")
        if not keys:
            return {}
        pairs = ",".join(f"(:id{i},:type{i})" for i in range(len(keys)))
        params = {f"id{i}": key[0] for i, key in enumerate(keys)}
        params.update({f"type{i}": key[1] for i, key in enumerate(keys)})
        params["receipt_limit"] = len(keys)
        rows = await session.execute(
            text(
                "SELECT id,memory_type,source,created_by,timestamp,confidence,"
                "verification_status,lifecycle_state,version FROM receipts "
                f"WHERE (id,memory_type) IN ({pairs}) LIMIT :receipt_limit"
            ),
            params,
        )
        receipts = {}
        for row in rows:
            data = dict(row._mapping)
            data["timestamp"] = self._utc(data["timestamp"])
            receipts[(row.id, row.memory_type)] = data
        return receipts

    async def read_fact_records(self, session: AsyncSession, ids: tuple[str, ...]):
        """Hydrate a bounded ordered hit list in the caller's existing snapshot."""
        from memory_server.application.memory_read_service import ReadRecord

        if len(ids) > 100 or len(set(ids)) != len(ids) or any(type(i) is not str or not i for i in ids):
            raise ValueError("invalid fact batch")
        if not ids:
            return ()
        binds = ",".join(f":id{i}" for i in range(len(ids)))
        params = {f"id{i}": ident for i, ident in enumerate(ids)}
        rows = (
            await session.execute(
                text(f"SELECT * FROM facts WHERE id IN ({binds}) LIMIT :fact_limit"),
                {**params, "fact_limit": len(ids)},
            )
        ).all()
        receipts = await self._read_receipt_metadata_batch(session, tuple((i, "fact") for i in ids))
        records = {r.id: ReadRecord(self._record("fact", r), "fact", receipts.get((r.id, "fact"))) for r in rows}
        return tuple(records[i] for i in ids if i in records)

    async def search_facts_in_session(
        self,
        session: AsyncSession,
        *,
        text_query: str | None = None,
        limit: int,
        include_inactive: bool,
        memory_types=(),
        lifecycle_states=(),
        min_confidence=None,
        **kwargs,
    ):
        """Search facts in the caller transaction, applying S08a filters before LIMIT."""
        text_query = text_query if text_query is not None else kwargs.get("text")
        if not memory_types and not lifecycle_states and min_confidence is None:
            repo = await self._get_fact_repo(session)
            return await repo.search(
                text=text_query, limit=limit, lifecycle_state=None if include_inactive else "active"
            )
        if memory_types and "fact" not in memory_types:
            return []
        clauses = []
        params = {"limit": limit}
        if not include_inactive:
            clauses.append("f.lifecycle_state IN ('candidate','validated','active')")
        if lifecycle_states:
            names = []
            for i, value in enumerate(lifecycle_states):
                name = f"ls{i}"
                names.append(f":{name}")
                params[name] = value
            clauses.append("f.lifecycle_state IN (" + ",".join(names) + ")")
        if min_confidence is not None:
            clauses.append("f.confidence >= :min_confidence")
            params["min_confidence"] = min_confidence
        if not text_query or not any(char.isalnum() for char in text_query):
            return []
        terms = [f'"{term.replace(chr(34), chr(34) * 2)}"*' for term in text_query.strip().split()]
        match = " AND ".join(terms)
        params["query"] = match
        where = " AND ".join(clauses) or "1=1"
        try:
            exists = await session.execute(text("SELECT 1 FROM sqlite_master WHERE type='table' AND name='facts_fts'"))
            has_fts = exists.first() is not None
            rows = []
            if has_fts:
                result = await session.execute(
                    text(
                        "SELECT f.* FROM facts_fts JOIN facts f ON facts_fts.rowid=f.rowid "
                        f"WHERE facts_fts MATCH :query AND {where} ORDER BY rank LIMIT :limit"
                    ),
                    params,
                )
                rows = result.mappings().all()
            if not rows:
                escaped = text_query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                like = f"%{escaped}%"
                params.update({"like": like})
                result = await session.execute(
                    text(
                        "SELECT f.* FROM facts f WHERE "
                        "(f.subject LIKE :like ESCAPE '\\\\' "
                        "OR f.predicate LIKE :like ESCAPE '\\\\' "
                        "OR f.object LIKE :like ESCAPE '\\\\') "
                        f"AND {where} ORDER BY f.created_at DESC LIMIT :limit"
                    ),
                    params,
                )
                rows = result.mappings().all()
        except Exception as exc:
            from sqlalchemy.exc import SQLAlchemyError

            if isinstance(exc, SQLAlchemyError):
                raise
            raise
        return [Fact(**dict(row)) for row in rows]

    async def read_memory_records(self, session: AsyncSession, keys):
        """Hydrate typed fact/decision keys in one transaction and stable order."""
        from memory_server.application.memory_read_service import ReadRecord

        if len(keys) > 200 or len(set(keys)) != len(keys):
            raise ValueError("invalid typed key batch")
        if any(
            type(k) is not tuple or len(k) != 2 or k[0] not in ("fact", "decision") or type(k[1]) is not str or not k[1]
            for k in keys
        ):
            raise ValueError("invalid typed key")
        by_type = {kind: [ident for k, ident in keys if k == kind] for kind in ("fact", "decision")}
        found = {}
        for kind, ids in by_type.items():
            if not ids:
                continue
            binds = ",".join(f":{kind}{i}" for i in range(len(ids)))
            params = {f"{kind}{i}": ident for i, ident in enumerate(ids)}
            result = await session.execute(
                text(f"SELECT * FROM {kind}s WHERE id IN ({binds}) LIMIT :n"), {**params, "n": len(ids)}
            )
            for row in result:
                found[(kind, row._mapping["id"])] = ReadRecord(self._record(kind, row), kind, None)
        receipts = await self._read_receipt_metadata_batch(session, tuple((ident, kind) for kind, ident in keys))
        found = {
            key: ReadRecord(record.memory, record.memory_type, receipts.get((key[1], key[0])))
            for key, record in found.items()
        }
        return tuple(found[key] for key in keys if key in found)

    async def read_receipt_metadata(self, session: AsyncSession, memory_id: str, memory_type: str):
        result = await session.execute(
            text(
                "SELECT id,memory_type,source,created_by,timestamp,confidence,"
                "verification_status,lifecycle_state,version FROM receipts "
                "WHERE id=:id AND memory_type=:memory_type LIMIT 1"
            ),
            {"id": memory_id, "memory_type": memory_type},
        )
        row = result.first()
        if row is None:
            return None
        data = dict(row._mapping)
        data["timestamp"] = self._utc(data["timestamp"])
        return data

    async def read_history_page(
        self,
        session: AsyncSession,
        memory_id: str,
        memory_type: str,
        *,
        limit: int,
        after: Any = None,
        upper: Any = None,
    ):
        from storage.repositories.lifecycle_repo import LifecycleRepository

        return await LifecycleRepository(session).get_events_page(
            memory_id, memory_type, limit=limit, after=after, upper=upper
        )

    # --- Fact CRUD ---

    async def create_fact(self, fact: Fact) -> Fact:
        async with await self._get_session() as session:
            repo = await self._get_fact_repo(session)
            result = await repo.create(fact)
            await session.commit()
            return result

    async def get_fact(self, fact_id: str) -> Optional[Fact]:
        async with await self._get_session() as session:
            repo = await self._get_fact_repo(session)
            return await repo.get(fact_id)

    async def search_facts(
        self,
        subject: Optional[str] = None,
        predicate: Optional[str] = None,
        object: Optional[str] = None,
        source: Optional[str] = None,
        text: Optional[str] = None,
        min_confidence: Optional[float] = None,
        max_confidence: Optional[float] = None,
        limit: int = 50,
        include_inactive: bool = False,
        lifecycle_state: Optional[str] = None,
    ) -> list[Fact]:
        async with await self._get_session() as session:
            repo = await self._get_fact_repo(session)
            effective_state = lifecycle_state
            if effective_state is None and not include_inactive:
                effective_state = "active"
            return await repo.search(
                subject=subject,
                predicate=predicate,
                object=object,
                source=source,
                text=text,
                min_confidence=min_confidence,
                max_confidence=max_confidence,
                lifecycle_state=effective_state,
                limit=limit,
            )

    async def update_fact(self, fact_id: str, **kwargs: Any) -> Optional[Fact]:
        async with await self._get_session() as session:
            repo = await self._get_fact_repo(session)
            result = await repo.update(fact_id, **kwargs)
            await session.commit()
            return result

    async def increment_fact_version(self, fact_id: str) -> Optional[Fact]:
        """Increment the revision counter of a fact.

        Backward-compatible with legacy string versions: non-numeric values
        are normalized to revision 1 before incrementing.
        """
        async with await self._get_session() as session:
            repo = await self._get_fact_repo(session)
            result = await repo.increment_version(fact_id)
            await session.commit()
            return result

    async def delete_fact(self, fact_id: str) -> bool:
        async with await self._get_session() as session:
            repo = await self._get_fact_repo(session)
            result = await repo.delete(fact_id)
            await session.commit()
            return result

    # --- Decision CRUD ---

    async def create_decision(self, decision: Decision) -> Decision:
        async with await self._get_session() as session:
            repo = await self._get_decision_repo(session)
            result = await repo.create(decision)
            await session.commit()
            return result

    async def get_decision(self, decision_id: str) -> Optional[Decision]:
        async with await self._get_session() as session:
            repo = await self._get_decision_repo(session)
            return await repo.get(decision_id)

    async def search_decisions(
        self,
        context: Optional[str] = None,
        choice: Optional[str] = None,
        reason: Optional[str] = None,
        source: Optional[str] = None,
        text: Optional[str] = None,
        limit: int = 50,
    ) -> list[Decision]:
        async with await self._get_session() as session:
            repo = await self._get_decision_repo(session)
            return await repo.search(
                context=context, choice=choice, reason=reason, source=source, text=text, limit=limit
            )

    async def delete_decision(self, decision_id: str) -> bool:
        async with await self._get_session() as session:
            repo = await self._get_decision_repo(session)
            result = await repo.delete(decision_id)
            await session.commit()
            return result

    # --- Skill CRUD ---

    async def create_skill(self, skill: Skill) -> Skill:
        async with await self._get_session() as session:
            repo = await self._get_skill_repo(session)
            result = await repo.create(skill)
            await session.commit()
            return result

    async def get_skill(self, skill_id: str) -> Optional[Skill]:
        async with await self._get_session() as session:
            repo = await self._get_skill_repo(session)
            return await repo.get(skill_id)

    async def search_skills(
        self,
        purpose: Optional[str] = None,
        name: Optional[str] = None,
        text: Optional[str] = None,
        min_success_rate: Optional[float] = None,
        limit: int = 50,
    ) -> list[Skill]:
        async with await self._get_session() as session:
            repo = await self._get_skill_repo(session)
            return await repo.search(
                purpose=purpose, name=name, text=text, min_success_rate=min_success_rate, limit=limit
            )

    async def delete_skill(self, skill_id: str) -> bool:
        async with await self._get_session() as session:
            repo = await self._get_skill_repo(session)
            result = await repo.delete(skill_id)
            await session.commit()
            return result

    # --- Receipt CRUD ---

    async def create_receipt(self, receipt: MemoryReceipt) -> MemoryReceipt:
        async with await self._get_session() as session:
            repo = await self._get_receipt_repo(session)
            result = await repo.create(receipt)
            await session.commit()
            return result

    async def get_receipt(self, receipt_id: str) -> Optional[MemoryReceipt]:
        async with await self._get_session() as session:
            repo = await self._get_receipt_repo(session)
            return await repo.get(receipt_id)

    async def search_receipts(
        self,
        source: Optional[str] = None,
        memory_type: Optional[str] = None,
        created_by: Optional[str] = None,
        limit: int = 50,
    ) -> list[MemoryReceipt]:
        async with await self._get_session() as session:
            repo = await self._get_receipt_repo(session)
            return await repo.search(memory_type=memory_type, source=source, created_by=created_by, limit=limit)

    async def count_facts(self, include_inactive: bool = True) -> int:
        async with await self._get_session() as session:
            stmt = select(func.count()).select_from(FactORM)
            if not include_inactive:
                stmt = stmt.where(FactORM.lifecycle_state.in_(("candidate", "validated", "active")))
            result = await session.execute(stmt)
            return int(result.scalar_one() or 0)

    async def count_decisions(self, include_inactive: bool = True) -> int:
        async with await self._get_session() as session:
            stmt = select(func.count()).select_from(DecisionORM)
            if not include_inactive:
                stmt = stmt.where(DecisionORM.lifecycle_state.in_(("candidate", "validated", "active")))
            result = await session.execute(stmt)
            return int(result.scalar_one() or 0)

    async def count_skills(self, include_inactive: bool = True) -> int:
        async with await self._get_session() as session:
            stmt = select(func.count()).select_from(SkillORM)
            if not include_inactive:
                stmt = stmt.where(SkillORM.lifecycle_state.in_(("candidate", "validated", "active")))
            result = await session.execute(stmt)
            return int(result.scalar_one() or 0)

    async def count_receipts(self, include_inactive: bool = True) -> int:
        async with await self._get_session() as session:
            stmt = select(func.count()).select_from(MemoryReceiptORM)
            if not include_inactive:
                stmt = stmt.where(MemoryReceiptORM.lifecycle_state.in_(("candidate", "validated", "active")))
            result = await session.execute(stmt)
            return int(result.scalar_one() or 0)

    async def list_receipt_ids(self, include_inactive: bool = True) -> list[str]:
        async with await self._get_session() as session:
            stmt = select(MemoryReceiptORM.id)
            if not include_inactive:
                stmt = stmt.where(MemoryReceiptORM.lifecycle_state.in_(("candidate", "validated", "active")))
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def list_audit_records(self, include_inactive: bool = True) -> list[dict[str, Any]]:
        active_states = ("candidate", "validated", "active")

        async with await self._get_session() as session:
            receipt_stmt = select(
                MemoryReceiptORM.id,
                MemoryReceiptORM.memory_type,
            )
            if not include_inactive:
                receipt_stmt = receipt_stmt.where(MemoryReceiptORM.lifecycle_state.in_(active_states))
            receipt_rows = await session.execute(receipt_stmt)
            receipt_map = {receipt_id: memory_type for receipt_id, memory_type in receipt_rows.all()}

            def _normalize_rows(rows, memory_type: str) -> list[dict[str, Any]]:
                normalized: list[dict[str, Any]] = []
                for row in rows:
                    normalized.append(
                        {
                            "fact_id": row.id,
                            "memory_type": memory_type,
                            "status": row.lifecycle_state,
                            "confidence": row.confidence,
                            "lifecycle_state": row.lifecycle_state,
                            "verification_status": row.verification_status,
                            "receipt_id": row.id if receipt_map.get(row.id) == memory_type else None,
                            "has_receipt": receipt_map.get(row.id) == memory_type,
                        }
                    )
                return normalized

            fact_stmt = select(
                FactORM.id,
                FactORM.confidence,
                FactORM.lifecycle_state,
                FactORM.verification_status,
            )
            decision_stmt = select(
                DecisionORM.id,
                DecisionORM.confidence,
                DecisionORM.lifecycle_state,
                DecisionORM.verification_status,
            )
            skill_stmt = select(
                SkillORM.id,
                SkillORM.confidence,
                SkillORM.lifecycle_state,
                SkillORM.verification_status,
            )
            if not include_inactive:
                fact_stmt = fact_stmt.where(FactORM.lifecycle_state.in_(active_states))
                decision_stmt = decision_stmt.where(DecisionORM.lifecycle_state.in_(active_states))
                skill_stmt = skill_stmt.where(SkillORM.lifecycle_state.in_(active_states))

            fact_rows = (await session.execute(fact_stmt)).all()
            decision_rows = (await session.execute(decision_stmt)).all()
            skill_rows = (await session.execute(skill_stmt)).all()

            return [
                *_normalize_rows(fact_rows, "fact"),
                *_normalize_rows(decision_rows, "decision"),
                *_normalize_rows(skill_rows, "skill"),
            ]

    async def prune_expired_memories(self, now: datetime | None = None) -> dict[str, Any]:
        """Archive memories whose write-time admission TTL has expired.

        TTL metadata is stored in receipt history under
        ``metadata.admission.expires_at``. Pruning is lifecycle-preserving:
        facts and receipts are marked ``archived`` rather than physically
        deleted, so provenance remains auditable.
        """
        clock = now or datetime.now(timezone.utc)
        pruned_ids: list[str] = []

        async with await self._get_session() as session:
            result = await session.execute(
                select(MemoryReceiptORM).where(MemoryReceiptORM.lifecycle_state != "archived")
            )
            receipts = result.scalars().all()
            for receipt in receipts:
                expires_at = self._receipt_expires_at(receipt)
                if expires_at is None or expires_at > clock:
                    continue

                receipt.lifecycle_state = "archived"
                receipt.verification_status = "archived"
                receipt.updated_at = clock
                fact = await session.get(FactORM, receipt.id)
                if fact is not None:
                    fact.lifecycle_state = "archived"
                    fact.updated_at = clock
                pruned_ids.append(receipt.id)

            await session.commit()

        return {"pruned": len(pruned_ids), "ids": pruned_ids}

    @staticmethod
    def _receipt_expires_at(receipt: MemoryReceiptORM) -> datetime | None:
        try:
            history = json.loads(receipt.history or "[]")
        except json.JSONDecodeError:
            return None
        for entry in history:
            if not isinstance(entry, dict):
                continue
            metadata = entry.get("metadata")
            if not isinstance(metadata, dict):
                continue
            admission = metadata.get("admission")
            if not isinstance(admission, dict):
                continue
            raw_expires = admission.get("expires_at") or admission.get("valid_to")
            if not raw_expires:
                continue
            try:
                parsed = datetime.fromisoformat(str(raw_expires).replace("Z", "+00:00"))
            except ValueError:
                continue
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        return None

    # --- Belief CRUD ---

    async def create_belief(self, belief: Belief, evidence: list[Evidence] | None = None) -> Belief:
        """Create a new belief with optional evidence entries."""
        async with await self._get_session() as session:
            repo = await self._get_belief_repo(session)
            result = await repo.create(belief)
            if evidence:
                ev_repo = await self._get_evidence_repo(session)
                for ev in evidence:
                    ev.belief_id = belief.id
                    await ev_repo.create(ev)
            await session.commit()
            return result

    async def get_belief(self, belief_id: str) -> Optional[Belief]:
        """Get a belief by ID."""
        async with await self._get_session() as session:
            repo = await self._get_belief_repo(session)
            return await repo.get_by_id(belief_id)

    async def search_beliefs(
        self,
        proposition: Optional[str] = None,
        tags: Optional[list[str]] = None,
        lifecycle_state: Optional[str] = None,
        min_confidence: Optional[float] = None,
        source: Optional[str] = None,
        creator: Optional[str] = None,
        source_id: Optional[str] = None,
        limit: int = 10,
        include_inactive: bool = False,
    ) -> list[Belief]:
        """Search beliefs with various filters."""
        async with await self._get_session() as session:
            repo = await self._get_belief_repo(session)
            effective_state = lifecycle_state
            if effective_state is None and not include_inactive:
                effective_state = "active"
            return await repo.search(
                proposition=proposition,
                tags=tags,
                lifecycle_state=effective_state,
                min_confidence=min_confidence,
                source=source,
                creator=creator,
                source_id=source_id,
                limit=limit,
            )

    async def update_belief_confidence(self, belief_id: str, new_confidence: float) -> Optional[Belief]:
        """Update the confidence of a belief."""
        async with await self._get_session() as session:
            repo = await self._get_belief_repo(session)
            result = await repo.update_confidence(belief_id, new_confidence)
            await session.commit()
            return result

    async def update_belief_lifecycle(self, belief_id: str, new_state: str) -> Optional[Belief]:
        """Update the lifecycle state of a belief."""
        async with await self._get_session() as session:
            repo = await self._get_belief_repo(session)
            result = await repo.update_lifecycle_state(belief_id, new_state)
            await session.commit()
            return result

    async def get_claim_dependents(
        self,
        target_id: str,
        relation_type: str = "derived_from",
    ) -> list[str]:
        """Return canonical dependents for a claim relation target."""
        async with await self._get_session() as session:
            repo = RelationRepository(session)
            return await repo.get_dependents(target_id=target_id, relation_type=relation_type)

    async def update_belief_reinforced_at(self, belief_id: str) -> Optional[Belief]:
        """Update the last_reinforced_at timestamp of a belief."""
        async with await self._get_session() as session:
            repo = await self._get_belief_repo(session)
            result = await repo.update_reinforced_at(belief_id)
            await session.commit()
            return result

    async def increment_belief_version(self, belief_id: str) -> Optional[Belief]:
        """Increment the version counter of a belief."""
        async with await self._get_session() as session:
            repo = await self._get_belief_repo(session)
            result = await repo.increment_version(belief_id)
            await session.commit()
            return result

    # --- Outbox ---

    async def add_outbox_entry(
        self,
        record_type: str,
        record_id: str,
        operation: str,
        payload: dict,
    ) -> None:
        """Add an outbox entry for async indexing.

        Creates the entry in its own transaction. For atomicity with
        the primary store write, use create_in_transaction() which
        shares a single session for all operations.

        Args:
            record_type: "fact", "decision", "skill", or "belief".
            record_id: ID of the record in the primary store.
            operation: "index_fact", "index_decision", "index_skill", or "index_belief".
            payload: Data needed for indexing.
        """
        async with await self._get_session() as session:
            repo = OutboxRepository(session)
            await repo.add_entry(
                record_type=record_type,
                record_id=record_id,
                operation=operation,
                payload=payload,
            )
            await session.commit()

    async def create_in_transaction(
        self,
        fact: Optional["Fact"] = None,
        receipt: Optional["MemoryReceipt"] = None,
        outbox_entries: Optional[list[dict]] = None,
        belief: Optional["Belief"] = None,
        evidence_list: Optional[list["Evidence"]] = None,
        relation_entries: Optional[list[dict[str, Any]]] = None,
        transaction_callback: Optional[Callable[[AsyncSession], Awaitable[None]]] = None,
    ) -> None:
        """Create memory records in a single transaction.

        Args:
            fact: Optional Fact to create.
            belief: Optional Belief to create.
            evidence_list: Optional list of Evidence entries (requires belief).
            receipt: Optional MemoryReceipt to create.
            outbox_entries: Optional list of outbox entry dicts, each with:
                record_type, record_id, operation, payload.
            relation_entries: Optional list of claim relation dicts with
                source_id, target_id, relation_type.
            transaction_callback: Optional async callback executed before
                commit while the same session is still open.
        """
        async with await self._get_session() as session:
            if fact is not None:
                fact_repo = await self._get_fact_repo(session)
                await fact_repo.create(fact)
            if belief is not None:
                belief_repo = await self._get_belief_repo(session)
                await belief_repo.create(belief)
                if evidence_list:
                    ev_repo = await self._get_evidence_repo(session)
                    for ev in evidence_list:
                        ev.belief_id = belief.id
                        await ev_repo.create(ev)
            if receipt is not None:
                receipt_repo = await self._get_receipt_repo(session)
                await receipt_repo.create(receipt)
            if outbox_entries:
                outbox_repo = OutboxRepository(session)
                for entry in outbox_entries:
                    await outbox_repo.add_entry(
                        record_type=entry["record_type"],
                        record_id=entry["record_id"],
                        operation=entry["operation"],
                        payload=entry["payload"],
                    )
            if relation_entries:
                relation_repo = RelationRepository(session)
                for entry in relation_entries:
                    await relation_repo.create(
                        source_id=entry["source_id"],
                        target_id=entry["target_id"],
                        relation_type=entry["relation_type"],
                    )
            if transaction_callback is not None:
                await transaction_callback(session)
            await session.commit()
