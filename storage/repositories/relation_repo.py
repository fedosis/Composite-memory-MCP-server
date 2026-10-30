"""Claim relation repository — canonical SQL CRUD for inter-claim relations."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic
from typing import Literal, Optional

from sqlalchemy import event, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from memory_server.providers.graph_provider import GraphBudgetExceeded
from storage.models.relation import ClaimRelationORM

SQL_VM_LIMIT = 200000
PROGRESS_INTERVAL = 1000
SQL_STATEMENTS_MAX = 64
_TYPES = ("supports", "contradicts", "derives", "derived_from")
_TABLES = (("fact", "facts"), ("decision", "decisions"), ("skill", "skills"), ("belief", "beliefs"))
_ORDER = "created_at,source_id,target_id,relation_type"
_KEY = "(r.created_at,r.source_id,r.target_id,r.relation_type)"


@dataclass(frozen=True, order=True)
class RelationKey:
    created_at: str
    source_id: str
    target_id: str
    relation_type: str


@dataclass(frozen=True)
class EndpointMeta:
    id: str
    memory_type: Literal["fact", "belief", "decision", "skill"]
    lifecycle_state: str


@dataclass(frozen=True)
class RelationRow:
    created_at: str
    source_id: str
    target_id: str
    relation_type: str


@dataclass(frozen=True)
class RelationPage:
    rows: tuple[RelationRow, ...]
    upper: RelationKey | None


@dataclass
class RelationReadWork:
    vm_limit: int = SQL_VM_LIMIT
    vm_charged: int = 0
    statements: int = 0
    soft_deadline: float = 0.0
    hard_deadline: float = 0.0
    stop: Literal["work", "soft", "hard"] | None = None

    def check(self, *, statement=False):
        if statement:
            self.statements += 1
        self.vm_charged += PROGRESS_INTERVAL
        now = monotonic()
        if self.hard_deadline and now >= self.hard_deadline:
            self.stop = "hard"
        elif self.stop is None and self.soft_deadline and now >= self.soft_deadline:
            self.stop = "soft"
        elif self.stop is None and (self.vm_charged > self.vm_limit or self.statements > SQL_STATEMENTS_MAX):
            self.stop = "work"
        self.raise_stopped()

    def raise_stopped(self):
        if self.stop == "work":
            raise GraphBudgetExceeded("sql_vm", self.vm_limit, self.vm_charged)
        if self.stop:
            raise TimeoutError


def _relation_wire_ok(source_id: object, target_id: object, relation_type: object) -> int:
    values = (source_id, target_id, relation_type)
    if any(not isinstance(v, str) or not v or len(v) > 1024 for v in values):
        return 0
    if any(v.startswith(("rel_", "ent_", "proj_")) for v in values[:2]):
        return 0
    try:
        return int(len(json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode("utf-8")) <= 1024)
    except (ValueError, UnicodeError):
        return 0


def _eligible(endpoint):
    return "(" + "+".join(f"(EXISTS(SELECT 1 FROM {t} WHERE id={endpoint}))" for _, t in _TABLES) + ")=1"


_EL = (
    _eligible("r.source_id")
    + " AND "
    + _eligible("r.target_id")
    + " AND r.relation_type IN ('supports','contradicts','derives','derived_from')"
    + " AND s09_relation_wire_ok(r.source_id,r.target_id,r.relation_type)=1"
)


def _wire_time(value):
    dt = datetime.fromisoformat(value) if isinstance(value, str) else value
    return dt.replace(tzinfo=UTC).isoformat(timespec="microseconds")


def _key_values(key):
    return (
        datetime.fromisoformat(key.created_at)
        .astimezone(UTC)
        .replace(tzinfo=None)
        .isoformat(sep=" ", timespec="microseconds"),
        key.source_id,
        key.target_id,
        key.relation_type,
    )


def _row(row):
    return RelationRow(_wire_time(row[0]), *row[1:])


class RelationRepository:
    """Repository for canonical claim relations."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def create(
        self,
        source_id: str,
        target_id: str,
        relation_type: str,
    ) -> ClaimRelationORM:
        """Create a relation if it does not already exist."""
        existing = await self._session.get(
            ClaimRelationORM,
            (source_id, target_id, relation_type),
        )
        if existing is not None:
            return existing

        orm = ClaimRelationORM(
            source_id=source_id,
            target_id=target_id,
            relation_type=relation_type,
        )
        self._session.add(orm)
        await self._session.flush()
        await self._session.refresh(orm)
        return orm

    async def get_dependents(
        self,
        target_id: str,
        relation_type: str = "derived_from",
    ) -> list[str]:
        """Return source IDs that depend on the given target."""
        stmt = (
            select(ClaimRelationORM.source_id)
            .where(ClaimRelationORM.target_id == target_id)
            .where(ClaimRelationORM.relation_type == relation_type)
        )
        result = await self._session.execute(stmt)
        return [row[0] for row in result.fetchall()]

    async def get_by_source(
        self,
        source_id: str,
        relation_type: Optional[str] = None,
    ) -> list[ClaimRelationORM]:
        """Return relations originating from a source memory."""
        stmt = select(ClaimRelationORM).where(ClaimRelationORM.source_id == source_id)
        if relation_type is not None:
            stmt = stmt.where(ClaimRelationORM.relation_type == relation_type)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    @asynccontextmanager
    async def _read_window(self, work: RelationReadWork):
        if getattr(self, "_window_work", None) is work:
            yield
            return
        connection = await self._session.connection()
        sync = connection.sync_connection

        async def bridge(function):
            return await connection.run_sync(lambda c: c.connection.dbapi_connection.run_async(function))

        def before(*_):
            work.check(statement=True)

        def progress():
            try:
                work.check()
                return 0
            except (GraphBudgetExceeded, TimeoutError):
                return 1

        self._window_work = work
        event.listen(sync, "before_cursor_execute", before)
        try:
            await bridge(
                lambda driver: driver.create_function("s09_relation_wire_ok", 3, _relation_wire_ok, deterministic=True)
            )
            await bridge(lambda driver: driver.set_progress_handler(progress, PROGRESS_INTERVAL))
            try:
                yield
            except Exception:
                work.raise_stopped()
                raise
        finally:
            event.remove(sync, "before_cursor_execute", before)

            async def reset():
                try:
                    await bridge(lambda driver: driver.set_progress_handler(None, 0))
                    await bridge(lambda driver: driver.create_function("s09_relation_wire_ok", 3, None))
                except BaseException:
                    await connection.invalidate()
                    raise

            cleanup = asyncio.create_task(reset())
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    work.stop = "hard"
            self._window_work = None
            cleanup.result()

    async def get_by_tuple(
        self, source_id: str, target_id: str, relation_type: str, *, work: RelationReadWork
    ) -> RelationRow | None:
        async with self._read_window(work):
            result = await self._session.execute(
                text(
                    "SELECT "
                    + ",".join("r." + k for k in _ORDER.split(","))
                    + " FROM claim_relations r WHERE r.source_id=:s AND r.target_id=:t"
                    + " AND r.relation_type=:r AND "
                    + _EL
                    + " LIMIT 1"
                ),
                {"s": source_id, "t": target_id, "r": relation_type},
            )
            row = result.first()
            return _row(row) if row else None

    async def endpoint_eligibility(self, ids: tuple[str, ...], *, work: RelationReadWork) -> tuple[EndpointMeta, ...]:
        if type(ids) is not tuple or len(ids) > 100:
            raise ValueError("invalid batch")
        if not ids:
            return ()
        params = {f"i{i}": ident for i, ident in enumerate(ids)}
        placeholders = ",".join(":" + key for key in params)
        union = " UNION ALL ".join(
            f"SELECT id,'{kind}' AS kind,lifecycle_state FROM {table} WHERE id IN ({placeholders})"
            for kind, table in _TABLES
        )
        async with self._read_window(work):
            rows = await self._session.execute(
                text(
                    "SELECT id,MIN(kind),MIN(lifecycle_state) FROM ("
                    + union
                    + ") GROUP BY id HAVING COUNT(*)=1 ORDER BY id"
                ),
                params,
            )
            return tuple(EndpointMeta(*row) for row in rows)

    async def read_keyset_page(
        self,
        *,
        limit: int,
        last: RelationKey | None,
        upper: RelationKey | None,
        memory_id: str | None,
        work: RelationReadWork,
    ) -> RelationPage:
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("invalid page")
        async with self._read_window(work):

            async def select_page(*, descending=False, maximum=None, after=None, count=1):
                params = {"n": count, "root": memory_id}
                where = _EL
                for prefix, key, operator in (("a", after, ">"), ("u", maximum, "<=")):
                    if key is not None:
                        names = [f"{prefix}{i}" for i in range(4)]
                        params.update(zip(names, _key_values(key)))
                        where += f" AND {_KEY} {operator} (" + ",".join(":" + n for n in names) + ")"
                order = ",".join("r." + k + (" DESC" if descending else " ASC") for k in _ORDER.split(","))
                columns = ",".join("r." + k for k in _ORDER.split(","))

                def branch(index, condition=""):
                    return (
                        f"SELECT {columns} FROM claim_relations r INDEXED BY {index} WHERE "
                        + where
                        + condition
                        + " ORDER BY "
                        + order
                        + " LIMIT :n"
                    )

                if memory_id is None:
                    sql = branch("s09_order")
                else:
                    sql = (
                        "SELECT * FROM ("
                        + branch("s09_source_order", " AND r.source_id=:root")
                        + ") UNION ALL SELECT * FROM ("
                        + branch("s09_target_order", " AND r.target_id=:root AND r.source_id<>:root")
                        + ") ORDER BY "
                        + order.replace("r.", "")
                        + " LIMIT :n"
                    )
                rows = await self._session.execute(text(sql), params)
                return tuple(_row(row) for row in rows)

            if upper is None:
                highest = await select_page(descending=True)
                if not highest:
                    return RelationPage((), None)
                upper = RelationKey(**vars(highest[0]))
            rows = await select_page(maximum=upper, after=last, count=limit + 1)
            return RelationPage(rows, upper)

    async def expand_incident(
        self,
        memory_id: str,
        *,
        direction: Literal["out", "in", "both"],
        relation_types: tuple[str, ...],
        memory_types: tuple[str, ...],
        lifecycle_states: tuple[str, ...],
        limit: int,
        last: RelationKey | None,
        work: RelationReadWork,
    ) -> RelationPage:
        if (
            type(memory_id) is not str
            or not memory_id
            or direction not in {"out", "in", "both"}
            or type(limit) is not int
            or not 1 <= limit <= 100
            or any(type(values) is not tuple for values in (relation_types, memory_types, lifecycle_states))
        ):
            raise ValueError("invalid incident query")
        params: dict[str, object] = {"root": memory_id, "n": limit + 1}
        after = ""
        if last is not None:
            names = [f"a{i}" for i in range(4)]
            params.update(zip(names, _key_values(last)))
            after = " AND " + _KEY + " > (" + ",".join(":" + name for name in names) + ")"

        allowed_types = tuple(value for value in relation_types if value in _TYPES)
        type_filter = ""
        if relation_types:
            if not allowed_types:
                return RelationPage((), None)
            binds = []
            for index, value in enumerate(allowed_types):
                name = f"rt{index}"
                params[name] = value
                binds.append(":" + name)
            type_filter = " AND r.relation_type IN (" + ",".join(binds) + ")"

        def metadata_filter(endpoint: str, prefix: str) -> str:
            clauses = []
            union = " UNION ALL ".join(
                f"SELECT '{kind}' AS kind,lifecycle_state FROM {table} WHERE id={endpoint}"
                for kind, table in _TABLES
            )
            if memory_types:
                names = []
                for index, value in enumerate(memory_types):
                    name = f"{prefix}m{index}"
                    params[name] = value
                    names.append(":" + name)
                clauses.append("kind IN (" + ",".join(names) + ")")
            if lifecycle_states:
                names = []
                for index, value in enumerate(lifecycle_states):
                    name = f"{prefix}l{index}"
                    params[name] = value
                    names.append(":" + name)
                clauses.append("lifecycle_state IN (" + ",".join(names) + ")")
            if not clauses:
                return ""
            return " AND EXISTS(SELECT 1 FROM (" + union + ") WHERE " + " AND ".join(clauses) + ")"

        columns = ",".join("r." + key for key in _ORDER.split(","))
        order = ",".join("r." + key for key in _ORDER.split(","))

        def branch(index: str, root_clause: str, endpoint: str, extra: str = "") -> str:
            return (
                f"SELECT {columns} FROM claim_relations r INDEXED BY {index} WHERE "
                + _EL
                + root_clause
                + extra
                + after
                + type_filter
                + metadata_filter(endpoint, index)
                + f" ORDER BY {order} LIMIT :n"
            )

        if direction == "out":
            sql = branch("s09_source_order", " AND r.source_id=:root", "r.target_id")
        elif direction == "in":
            sql = branch("s09_target_order", " AND r.target_id=:root", "r.source_id")
        else:
            sql = (
                "SELECT * FROM ("
                + branch("s09_source_order", " AND r.source_id=:root", "r.target_id")
                + ") UNION ALL SELECT * FROM ("
                + branch(
                    "s09_target_order",
                    " AND r.target_id=:root",
                    "r.source_id",
                    " AND r.source_id<>:root",
                )
                + ") ORDER BY "
                + _ORDER
                + " LIMIT :n"
            )
        async with self._read_window(work):
            rows = await self._session.execute(text(sql), params)
            result = tuple(_row(row) for row in rows)
        upper = RelationKey(**vars(result[-1])) if result else None
        return RelationPage(result, upper)

    async def resolve_witnesses(
        self,
        names: tuple[str, ...],
        *,
        limit: int,
        work: RelationReadWork,
    ) -> tuple[tuple[str, Literal["subject", "object"], str, str], ...]:
        if (
            type(names) is not tuple
            or len(names) > 100
            or type(limit) is not int
            or not 1 <= limit <= 101
            or any(
                type(name) is not str
                or not name
                or len(name) > 8192
                or len(name.encode("utf-8")) > 8192
                for name in names
            )
        ):
            raise ValueError("invalid witness query")
        if not names:
            return ()
        params: dict[str, object] = {f"name{index}": name for index, name in enumerate(names)}
        params["n"] = limit
        binds = ",".join(f":name{index}" for index in range(len(names)))
        exact_fact = _eligible("f.id")
        sql = (
            "SELECT id,slot,value,lifecycle_state FROM ("
            "SELECT f.id,'subject' AS slot,f.subject AS value,f.lifecycle_state FROM facts f "
            f"WHERE f.subject IN ({binds}) AND {exact_fact} "
            "UNION ALL "
            "SELECT f.id,'object' AS slot,f.object AS value,f.lifecycle_state FROM facts f "
            f"WHERE f.object IN ({binds}) AND {exact_fact}"
            ") ORDER BY id,slot LIMIT :n"
        )
        async with self._read_window(work):
            rows = await self._session.execute(text(sql), params)
            return tuple((row[0], row[1], row[2], row[3]) for row in rows)
