"""Scoped canonical relation reads. Graph traversal is deliberately not mounted."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from time import monotonic
from typing import TYPE_CHECKING, Callable, Literal

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from storage.repositories.relation_repo import (
    RelationKey,
    RelationReadWork,
    RelationRepository,
    _relation_wire_ok,
)

from memory_server.application.gateway_retrieval import NativeCustody
from memory_server.application.memory_read_service import ReadResources, b64, unb64, utc
from memory_server.control.policy import require_permission
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import (
    GraphReadPort,
    MetricsSink,
    ProfileLeases,
    ProfileSession,
    RequestContext,
)
from memory_server.providers.graph_provider import GraphBudgetExceeded, SimpleGraph
from memory_server.transports.http.errors import HttpError

if TYPE_CHECKING:
    from memory_server.transports.http.routes.graph import RelationCursorCodec

RelationType = Literal["supports", "contradicts", "derives", "derived_from"]
_TYPES = ("supports", "contradicts", "derives", "derived_from")
MEMORY_HANDLE_MAX_BYTES = 1024
REL_ID_MAX_BYTES = 2048
REL_TUPLE_MAX_BYTES = 1024


def wire_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def validate_memory_id(value):
    try:
        if not isinstance(value, str) or not value or len(value) > 1024 or len(value.encode("utf-8")) > 1024:
            raise ValueError
        if value.startswith(("rel_", "ent_", "proj_")):
            raise ValueError
    except (ValueError, UnicodeError):
        raise HttpError("E_VALIDATION") from None
    return value


def relation_id(source_id: str, target_id: str, relation_type: RelationType) -> str:
    if relation_type not in _TYPES or not _relation_wire_ok(source_id, target_id, relation_type):
        raise HttpError("E_VALIDATION")
    return "rel_" + b64(wire_json([source_id, target_id, relation_type]))


def decode_relation_id(value: str) -> tuple[str, str, RelationType]:
    try:
        if not isinstance(value, str) or len(value) > REL_ID_MAX_BYTES or not value.startswith("rel_"):
            raise ValueError
        raw = unb64(value[4:])
        if len(raw) > REL_TUPLE_MAX_BYTES:
            raise ValueError
        data = json.loads(raw.decode("utf-8"))
        if type(data) is not list or len(data) != 3 or any(type(v) is not str for v in data):
            raise ValueError
        if relation_id(*data) != value:
            raise ValueError
        return tuple(data)
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise HttpError("E_VALIDATION") from None


def map_relation(row) -> d.Relation:
    return d.Relation(
        id=relation_id(row.source_id, row.target_id, row.relation_type),
        source_id=row.source_id,
        target_id=row.target_id,
        relation_type=row.relation_type,
        canonical=True,
        revision=None,
        created_at=utc(row.created_at),
    )


@dataclass(frozen=True)
class GraphReadResources:
    canonical: ReadResources
    graph: SimpleGraph | None


ReadBinder = Callable[[ProfileSession], GraphReadResources]
NativeBinder = Callable[[ProfileSession], NativeCustody]


class GraphReadService:
    def __init__(
        self,
        leases: ProfileLeases,
        bind: ReadBinder,
        *,
        native: NativeBinder,
        cursor_codec: RelationCursorCodec,
        metrics: MetricsSink,
        clock: Callable[[], float] = monotonic,
    ):
        self.leases, self.bind, self.native = leases, bind, native
        self.cursor_codec, self.metrics, self.clock = cursor_codec, metrics, clock

    async def execute(self, ctx: RequestContext, command: d.GraphReadCommand) -> d.GraphReadResult:
        if command.operation not in {"relations.for_memory", "relations.list", "relations.get"}:
            raise HttpError("E_VALIDATION")
        if ctx.profile_id != command.profile_id:
            raise HttpError("E_PERMISSION_DENIED")
        require_permission(ctx.principal, "relation.read", profile_id=command.profile_id)
        state = None
        if command.operation == "relations.get":
            ident = decode_relation_id(command.path_ids["id"])
        else:
            ident = command.path_ids.get("id")
            if ident is not None:
                validate_memory_id(ident)
            if command.payload.cursor is not None:
                state = self.cursor_codec.validate(ctx.principal, command, now=self.cursor_codec.clock())
        remaining = ctx.deadline - self.clock()
        if remaining <= 0:
            raise HttpError("E_TIMEOUT")
        work = RelationReadWork(soft_deadline=ctx.deadline - min(0.050, remaining / 10), hard_deadline=ctx.deadline)
        async with self.leases.acquire(ctx) as scope:
            resources = self.bind(scope).canonical

            async def produce():
                async with resources.session_factory() as session:
                    async with session.begin():
                        repo = RelationRepository(session)
                        async with repo._read_window(work):
                            await session.execute(text("BEGIN"))
                            if command.operation == "relations.get":
                                row = await repo.get_by_tuple(*ident, work=work)
                                if row is None:
                                    raise HttpError("E_NOT_FOUND")
                                return map_relation(row)
                            if ident is not None:
                                await resources.provider.read_memory(session, ident)
                            page = await repo.read_keyset_page(
                                limit=command.payload.limit,
                                last=state.last if state else None,
                                upper=state.upper if state else None,
                                memory_id=ident,
                                work=work,
                            )
                            more = len(page.rows) > command.payload.limit
                            rows = page.rows[: command.payload.limit]
                            cursor = None
                            if more:
                                cursor = self.cursor_codec.issue(
                                    ctx,
                                    command,
                                    last=RelationKey(**vars(rows[-1])),
                                    upper=page.upper,
                                    limit=command.payload.limit,
                                    now=self.cursor_codec.clock(),
                                    expires_at=state.expires_at if state else None,
                                )
                            return d.Page[d.Relation](
                                items=[map_relation(row) for row in rows],
                                has_more=more,
                                next_cursor=cursor,
                                request_id=ctx.request_id,
                            )

            producer = asyncio.create_task(produce())
            try:
                result = await asyncio.wait_for(asyncio.shield(producer), max(0, ctx.deadline - self.clock()))
                if self.clock() >= ctx.deadline:
                    raise TimeoutError
                return result
            except (asyncio.CancelledError, TimeoutError):
                work.stop = "hard"
                raise HttpError("E_TIMEOUT") from None
            except GraphBudgetExceeded:
                raise HttpError("E_GRAPH_BACKEND") from None
            except SQLAlchemyError:
                raise HttpError("E_STORAGE") from None
            finally:
                # The lease owns the producer, queued reset, rollback and close.
                # Repeated cancellation cannot release it with SQLite still alive.
                while not producer.done():
                    try:
                        await asyncio.shield(producer)
                    except asyncio.CancelledError:
                        work.stop = "hard"
                    except Exception:
                        break
                if producer.done() and not producer.cancelled():
                    producer.exception()

    async def relations_for_memory(self, ctx: RequestContext, memory_id: str, query: d.PageQuery) -> d.Page[d.Relation]:
        return await self.execute(
            ctx,
            d.COMMAND_TYPES["relations.for_memory"](
                operation="relations.for_memory", profile_id=ctx.profile_id, path_ids={"id": memory_id}, payload=query
            ),
        )

    async def relations_list(self, ctx: RequestContext, query: d.PageQuery) -> d.Page[d.Relation]:
        return await self.execute(
            ctx,
            d.COMMAND_TYPES["relations.list"](
                operation="relations.list", profile_id=ctx.profile_id, path_ids={}, payload=query
            ),
        )

    async def relations_get(self, ctx: RequestContext, relation_id: str) -> d.Relation:
        return await self.execute(
            ctx,
            d.COMMAND_TYPES["relations.get"](
                operation="relations.get", profile_id=ctx.profile_id, path_ids={"id": relation_id}, payload=d.Empty()
            ),
        )


def create_graph_read_port(
    leases: ProfileLeases,
    bind: ReadBinder,
    *,
    native: NativeBinder,
    cursor_codec: RelationCursorCodec,
    metrics: MetricsSink,
) -> GraphReadPort:
    return GraphReadService(leases, bind, native=native, cursor_codec=cursor_codec, metrics=metrics)
