"""Lifecycle repository — lifecycle state and event tracking."""

import json
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from storage.base import utcnow
from storage.models.lifecycle import LifecycleEventORM, LifecycleStateORM


class LifecycleConflictError(ValueError):
    """The requested revision/state no longer matches; roll back the whole UOW."""


def normalize_version(version: Any) -> int:
    """Canonical revision; support legacy initial and padded integer versions.

    NULL and 0.1.0 denote revision 1. Non-positive integer revisions also
    denote 1. Unknown strings are rejected, never interpreted via SQLite CAST.
    """
    if version is None or version == "0.1.0":
        return 1
    if isinstance(version, int) and not isinstance(version, bool):
        return max(1, version)
    if isinstance(version, str):
        value = version.strip()
        if value == "0.1.0":
            return 1
        if value.isascii() and value.isdecimal():
            return max(1, int(value))
    raise ValueError(f"Unsupported lifecycle version: {version!r}")


async def _cas_transition(
    session: AsyncSession,
    model: Any,
    memory_id: str,
    new_state: str,
    expected_state: str,
    expected_version: int,
    confidence: float | None = None,
) -> Any:
    """Shared fact/belief CAS primitive; no commit or rollback here."""
    row = (await session.execute(
        select(model.version, model.lifecycle_state).where(model.id == memory_id)
    )).one_or_none()
    if row is None or normalize_version(row.version) != expected_version or row.lifecycle_state != expected_state:
        raise LifecycleConflictError(f"expected_version mismatch for {memory_id}: expected {expected_version}")
    values = {
        "lifecycle_state": new_state,
        "version": str(expected_version + 1) if model.__tablename__ == "facts" else expected_version + 1,
        "updated_at": utcnow(),
    }
    if confidence is not None:
        values["confidence"] = confidence
    result = await session.execute(
        update(model)
        .where(model.id == memory_id, model.version == row.version, model.lifecycle_state == expected_state)
        .values(**values)
        .execution_options(synchronize_session=False)
    )
    if result.rowcount != 1:
        raise LifecycleConflictError(f"expected_version mismatch for {memory_id}: expected {expected_version}")
    orm = await session.get(model, memory_id, populate_existing=True)
    return orm.to_pydantic()


class LifecycleRepository:
    """Repository for lifecycle state and event tracking."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def transition(
        self,
        repository: Any,
        *,
        memory_id: str,
        memory_type: str,
        from_state: str,
        to_state: str,
        expected_version: int,
        reason: str = "",
        triggered_by: str = "system",
        confidence: float | None = None,
    ) -> Any:
        """CAS + state history + event in this session; the UOW owner commits.

        Any exception must propagate to the UOW owner, who rolls back the whole
        transaction. Do not catch a conflict and commit earlier batch writes.
        """
        if repository._session is not self._session:
            raise ValueError("Lifecycle repositories must share the UOW session")
        updated = await repository.transition_lifecycle_state(
            memory_id, to_state, from_state, expected_version, confidence=confidence
        )
        await self.set_state(memory_id, memory_type, to_state, from_state, updated.confidence)
        await self.record_event(memory_id, memory_type, from_state, to_state, reason, triggered_by)
        return updated

    async def get_state(self, memory_id: str) -> Optional[str]:
        stmt = (
            select(LifecycleStateORM)
            .where(LifecycleStateORM.memory_id == memory_id)
            .order_by(LifecycleStateORM.updated_at.desc())
            .limit(1)
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        return row.current_state if row else None

    async def set_state(
        self,
        memory_id: str,
        memory_type: str,
        new_state: str,
        previous_state: Optional[str] = None,
        confidence: float = 0.5,
    ) -> None:
        orm = LifecycleStateORM(
            id=str(uuid4()),
            memory_id=memory_id,
            memory_type=memory_type,
            current_state=new_state,
            previous_state=previous_state,
            confidence=confidence,
            updated_at=utcnow(),
        )
        self._session.add(orm)
        await self._session.flush()

    async def record_event(
        self,
        memory_id: str,
        memory_type: str,
        from_state: str,
        to_state: str,
        reason: str = "",
        triggered_by: str = "system",
    ) -> None:
        event = LifecycleEventORM(
            id=str(uuid4()),
            memory_id=memory_id,
            memory_type=memory_type,
            from_state=from_state,
            to_state=to_state,
            reason=reason,
            triggered_by=triggered_by,
            timestamp=utcnow(),
        )
        self._session.add(event)
        await self._session.flush()

    async def get_events(
        self,
        memory_id: str,
        limit: int = 50,
    ) -> list[dict]:
        stmt = (
            select(LifecycleEventORM)
            .where(LifecycleEventORM.memory_id == memory_id)
            .order_by(LifecycleEventORM.timestamp.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return [
            {
                "id": e.id,
                "memory_id": e.memory_id,
                "from_state": e.from_state,
                "to_state": e.to_state,
                "reason": e.reason,
                "triggered_by": e.triggered_by,
                "timestamp": (
                e.timestamp.isoformat()
                if isinstance(e.timestamp, datetime)
                else str(e.timestamp)
            ),
            }
            for e in result.scalars().all()
        ]

    async def get_events_page(self, memory_id, memory_type, *, limit, after=None, upper=None):
        """Bounded SQL-ordered available events, never a reconstructed history.

        Normalize timestamps before SQL keyset predicates. Each source and the
        merged result are capped; JSON parsing CPU itself is not bounded.
        """
        from memory_server.application.memory_read_service import (
            HistoryKey,
            HistorySlice,
            b64,
            canonical,
            key_order,
        )
        from memory_server.transports.http.errors import HttpError

        if type(limit) is not int or not 1 <= limit <= 200:
            raise HttpError("E_VALIDATION")

        def normalized(value):
            if value is None:
                return None
            try:
                dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc).isoformat(timespec="microseconds")
            except (ValueError, TypeError, OverflowError):
                return None

        conn = await self._session.connection()
        await conn.run_sync(
            lambda c: c.connection.dbapi_connection.run_async(
                lambda a: a.create_function("s07_timestamp", 1, normalized, deterministic=True)
            )
        )
        # json_each.type is safe even for a scalar string; never json_type on
        # an unquoted scalar j.value. All extract calls operate on guarded obj.
        cte = """
        WITH receipt_entries AS (
          SELECT r.id AS sid, CAST(j.key AS INTEGER) AS idx, j.type AS entry_type,
                 CASE WHEN j.type='object' THEN j.value ELSE '{}' END AS obj
          FROM receipts r, json_each(CASE WHEN json_valid(r.history)
               THEN CASE WHEN json_type(r.history)='array' THEN r.history ELSE '[]' END ELSE '[]' END) j
          WHERE r.id=:id AND r.memory_type=:type
        ), raw AS (
          SELECT 0 AS rank,sid,idx,sid || ':' || idx AS wire_id,
            COALESCE(json_extract(obj,'$.kind'),'receipt') AS kind,
            COALESCE(json_extract(obj,'$.timestamp'),json_extract(obj,'$.created_at')) AS ts,
            obj AS raw, entry_type='object' AS supported
          FROM receipt_entries
          UNION ALL
          SELECT 1,id,0,id,'lifecycle',timestamp,
            json_object('from_state',from_state,'to_state',to_state,'reason',reason,'actor',triggered_by),1
          FROM lifecycle_events WHERE memory_id=:id AND memory_type=:type
        ), normalized AS (
          SELECT *,s07_timestamp(ts) AS stamp FROM raw
        ), checked AS (
          SELECT *,CASE WHEN stamp IS NULL THEN 0 ELSE 1 END AS present,
            COALESCE(stamp,'') AS sort_ts,
            supported AND kind IN ('receipt','lifecycle','relation','confidence','admission')
              AND (ts IS NULL OR stamp IS NOT NULL)
              AND (json_type(raw,'$.reason') IS NULL OR json_type(raw,'$.reason') IN ('text','null'))
              AND (json_type(raw,'$.actor') IS NULL OR json_type(raw,'$.actor') IN ('text','null'))
              AND (json_type(raw,'$.revision') IS NULL OR json_type(raw,'$.revision') IN ('integer','null'))
              AS eligible
          FROM normalized
        )
        """
        params = {"id": memory_id, "type": memory_type, "n": limit + 1}
        incomplete = bool(
            await self._session.scalar(
                text(cte + "SELECT EXISTS(SELECT 1 FROM checked WHERE NOT eligible OR stamp IS NULL LIMIT 1)"), params
            )
        )
        malformed = await self._session.scalar(
            text("""
          SELECT EXISTS(SELECT 1 FROM receipts WHERE id=:id AND memory_type=:type AND
            CASE WHEN json_valid(history) THEN json_type(history)<>'array' ELSE 1 END LIMIT 1)
        """),
            params,
        )
        incomplete |= bool(malformed)
        order = "(present,sort_ts,rank,sid COLLATE BINARY,idx)"

        def key(row):
            return HistoryKey(
                row["stamp"], b64(canonical(["receipt" if row["rank"] == 0 else "lifecycle", row["sid"], row["idx"]]))
            )

        if upper is None:
            top = (
                (
                    await self._session.execute(
                        text(
                            cte + "SELECT * FROM checked WHERE eligible "
                            "ORDER BY present DESC,sort_ts DESC,rank DESC,sid COLLATE BINARY DESC,idx DESC LIMIT 1"
                        ),
                        params,
                    )
                )
                .mappings()
                .first()
            )
            upper = None if top is None else key(top)
        where = "eligible"
        for name, bound, op in (("a", after, ">"), ("u", upper, "<=")):
            if bound is not None:
                parts = key_order(bound)
                where += f" AND {order} {op} (:{name}0,:{name}1,:{name}2,:{name}3,:{name}4)"
                params.update({name + str(i): v for i, v in enumerate(parts)})
        sql = (
            cte
            + f""", receipt_page AS (
          SELECT * FROM checked WHERE {where} AND rank=0
          ORDER BY present,sort_ts,rank,sid COLLATE BINARY,idx LIMIT :n
        ), lifecycle_page AS (
          SELECT * FROM checked WHERE {where} AND rank=1
          ORDER BY present,sort_ts,rank,sid COLLATE BINARY,idx LIMIT :n
        ) SELECT * FROM (SELECT * FROM receipt_page UNION ALL SELECT * FROM lifecycle_page)
        ORDER BY present,sort_ts,rank,sid COLLATE BINARY,idx LIMIT :n"""
        )
        rows = (await self._session.execute(text(sql), params)).mappings().all()
        events, keys = [], []
        for row in rows[:limit]:
            data = json.loads(row["raw"])
            events.append(
                dict(
                    id=row["wire_id"],
                    kind=row["kind"],
                    timestamp=row["stamp"],
                    actor=data.get("actor", data.get("created_by")),
                    reason=data.get("reason", data.get("note")),
                    revision=data.get("revision"),
                    from_state=data.get("from_state"),
                    to_state=data.get("to_state"),
                    receipt_id=memory_id if row["rank"] == 0 else None,
                    evidence=data.get("evidence"),
                    _raw=data,
                )
            )
            keys.append(key(row))
        has_more = len(rows) > limit
        return HistorySlice(tuple(events), has_more, incomplete or has_more, upper, tuple(keys))
