"""Narrow S08 keyword adapter; canonical masked memories, no trace producer."""

from __future__ import annotations

import math
from dataclasses import dataclass
from time import monotonic

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from memory_server.application.errors import UnknownDependencyError
from memory_server.application.memory_read_service import project_for_caller
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import RetrievalPort
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.guards import ReadRouteSpec


@dataclass(frozen=True)
class RetrievalRouteSpec(ReadRouteSpec):
    """Marker accepted only for the catalogue's S08 search operation."""


def _preflight(query):
    # Single-line printable text only: no C0/C1 controls, including LF/TAB/NUL.
    if not query.query.isprintable():
        raise HttpError("E_VALIDATION")
    filters = query.filters
    if (
        filters.memory_types
        or filters.tags
        or filters.lifecycle_states
        or filters.source is not None
        or filters.min_confidence is not None
    ):
        raise HttpError("E_VALIDATION")
    if query.trace or query.mode in ("semantic", "graph"):
        raise HttpError("E_CAPABILITY_UNAVAILABLE")


def _parse(request, body, principal):
    if request.query_params:
        raise HttpError("E_VALIDATION")
    if request.headers.get("content-type", "").split(";")[0].strip().lower() != "application/json":
        raise HttpError("E_UNSUPPORTED_MEDIA_TYPE")
    try:
        query = d.SearchRequest.model_validate_json(body)
        command = d.COMMAND_TYPES["search"](
            operation="search", profile_id=request.path_params["profile"], path_ids={}, payload=query
        )
    except (ValidationError, ValueError, TypeError):
        raise HttpError("E_VALIDATION") from None
    _preflight(query)
    return command


def _result(value):
    return d.SearchResult.model_validate(value.model_dump())


def create_search_routes():
    return (RetrievalRouteSpec("search", "POST", "/api/v1/profiles/{profile}/search", "memory.read", _parse, _result),)


class KeywordRetrievalPort(RetrievalPort):
    """One lease and SQLite snapshot for keyword search and bulk hydration.

    In this build score is not BM25/semantic relevance: keyword search defines
    ordering; score is reserved for S08. Confidence remains Memory.confidence.
    """

    def __init__(self, leases, bind_read, metrics):
        self.leases, self.bind_read, self.metrics = leases, bind_read, metrics

    async def execute(self, ctx, command):
        started = monotonic()
        code = "E_INTERNAL"
        try:
            result = await self._execute(ctx, command)
            code = None
            return result
        except HttpError as exc:
            code = exc.code
            raise
        finally:
            self.metrics.retrieval_completed(ctx, "fts", monotonic() - started, code)

    async def _execute(self, ctx, command):
        if "memory.read" not in ctx.principal.permissions or ctx.profile_id not in ctx.principal.profiles:
            raise HttpError("E_PERMISSION_DENIED")
        if command.operation != "search" or command.profile_id != ctx.profile_id:
            raise HttpError("E_VALIDATION")
        query = command.payload
        _preflight(query)
        try:
            async with self.leases.acquire(ctx) as scope:
                try:
                    scope.application.retrieval
                    resources = self.bind_read(scope)
                except UnknownDependencyError:
                    raise HttpError("E_RUNTIME_NOT_READY") from None
                provider = resources.provider
                if provider is None or resources.session_factory is None:
                    raise HttpError("E_RUNTIME_NOT_READY")
                async with resources.session_factory() as session:
                    async with session.begin():
                        await session.execute(text("BEGIN"))
                        facts = await provider.search_facts_in_session(
                            session, text=query.query, limit=query.limit, include_inactive=query.include_inactive
                        )
                        if not isinstance(facts, list) or len(facts) > query.limit:
                            raise HttpError("E_INTERNAL")
                        ids = tuple(fact.id for fact in facts)
                        if (
                            len(set(ids)) != len(ids)
                            or any(type(i) is not str or not i for i in ids)
                            or any(not math.isfinite(f.confidence) for f in facts)
                        ):
                            raise HttpError("E_INTERNAL")
                        records = await provider.read_fact_records(session, ids)
                        if tuple(r.memory["id"] for r in records) != ids:
                            raise HttpError("E_STORAGE")
                        hits = [
                            d.SearchHit(
                                memory=project_for_caller(ctx, "search", record, "unknown"),
                                score=0.0,
                                rank=rank,
                                source="fts",
                            )
                            for rank, record in enumerate(records, 1)
                        ]
            degraded = (
                []
                if query.mode == "keyword"
                else [
                    d.SourceFailure(source=source, code="E_CAPABILITY_UNAVAILABLE") for source in ("semantic", "graph")
                ]
            )
            return d.SearchResult(
                request_id=ctx.request_id, results=hits, total=len(hits), trace_id=None, degraded=degraded
            )
        except SQLAlchemyError:
            raise HttpError("E_STORAGE") from None
