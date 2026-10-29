"""Application orchestration for the gateway's facts-only search and context."""

from __future__ import annotations

import json
import math
import re
import sqlite3
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from time import monotonic

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from memory_server.application.contracts import ContextCommand
from memory_server.application.errors import UnknownDependencyError
from memory_server.application.memory_read_service import project_for_caller
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import RetrievalPort
from memory_server.transports.http.errors import HttpError

_SAFE_VERSION = re.compile(r"[0-9]+(?:\.[0-9]+)*")
_ACTIVE = frozenset(("candidate", "validated", "active"))


def _masked(value):
    if value is None:
        return None
    if type(value) is not str:
        raise ValueError("invalid maskable field")
    return "[MASKED]"


def _mask_version(value):
    if type(value) is int or value is None:
        return value
    if type(value) is not str:
        raise ValueError("invalid version")
    if len(value) <= 128 and _SAFE_VERSION.fullmatch(value):
        return value
    return "[MASKED]"


def mask_context_package(package: dict) -> dict:
    """Fresh field-wise copy; reject unknown fields instead of passing them through."""
    if type(package) is not dict or tuple(package) != ("facts", "decisions", "total"):
        raise ValueError("invalid context package")
    if (
        type(package["facts"]) is not list
        or type(package["decisions"]) is not list
        or type(package["total"]) is not int
        or package["total"] != len(package["facts"]) + len(package["decisions"])
    ):
        raise ValueError("invalid context package")
    result = {}
    for group, fields, schema in (
        ("facts", ("subject", "predicate", "object", "dedup_key", "source", "creator"), d.LegacyFact),
        ("decisions", ("context", "choice", "reason", "source", "creator"), d.LegacyDecision),
    ):
        result[group] = []
        for item in package[group]:
            if type(item) is not dict or item.keys() != schema.model_fields.keys():
                raise ValueError("invalid context row")
            out = dict(item)
            for name in fields:
                out[name] = _masked(item[name])
            if group == "decisions":
                if type(item["rejected_alternatives"]) is not list:
                    raise ValueError("invalid alternatives")
                out["rejected_alternatives"] = [_masked(value) for value in item["rejected_alternatives"]]
            out["version"] = _mask_version(item["version"])
            result[group].append(out)
    result["total"] = package["total"]
    return result


def _context_json_ready(raw: dict) -> dict:
    """Legacy API JSON may contain naive UTC stamps; normalize only a fresh copy."""
    result = {"facts": [], "decisions": [], "total": raw["total"]}
    for group in ("facts", "decisions"):
        for item in raw[group]:
            out = dict(item)
            for name in ("created_at", "updated_at"):
                stamp = datetime.fromisoformat(item[name])
                out[name] = stamp.replace(tzinfo=UTC).isoformat() if stamp.tzinfo is None else stamp.isoformat()
            result[group].append(out)
    return result


def _preflight(operation, query):
    expected = d.SearchRequest if operation == "search" else d.ContextRequest
    if not isinstance(query, expected):
        raise HttpError("E_VALIDATION")
    if isinstance(query, d.SearchRequest):
        if not query.query.isprintable() or query.filters.source is not None or query.filters.tags:
            raise HttpError("E_VALIDATION")
        if query.mode in ("semantic", "graph"):
            raise HttpError("E_CAPABILITY_UNAVAILABLE")
    if query.trace:
        raise HttpError("E_CAPABILITY_UNAVAILABLE")


class GatewayRetrievalPort(RetrievalPort):
    def __init__(self, leases, bind_read, metrics):
        self.leases, self.bind_read, self.metrics = leases, bind_read, metrics

    @asynccontextmanager
    async def _stage(self, ctx, stage):
        started, code = monotonic(), "E_INTERNAL"
        try:
            yield
            code = None
        except HttpError as exc:
            code = exc.code
            raise
        except (SQLAlchemyError, sqlite3.Error):
            code = "E_STORAGE"
            raise HttpError("E_STORAGE") from None
        finally:
            self.metrics.retrieval_completed(ctx, stage, monotonic() - started, code)

    async def execute(self, ctx, command):
        if "memory.read" not in ctx.principal.permissions or ctx.profile_id not in ctx.principal.profiles:
            raise HttpError("E_PERMISSION_DENIED")
        if command.profile_id != ctx.profile_id or command.operation not in ("search", "context"):
            raise HttpError("E_VALIDATION")
        _preflight(command.operation, command.payload)
        async with self.leases.acquire(ctx) as scope:
            async with self._stage(ctx, "context" if command.operation == "context" else "fts"):
                try:
                    resources = self.bind_read(scope)
                    retrieval = scope.application.retrieval
                except UnknownDependencyError:
                    raise HttpError("E_RUNTIME_NOT_READY") from None
                if resources.provider is None or resources.session_factory is None or retrieval is None:
                    raise HttpError("E_RUNTIME_NOT_READY")
                if command.operation == "search":
                    return await self._search(ctx, command.payload, resources)
                return await self._context(ctx, command.payload, retrieval, resources)

    async def _search(self, ctx, query, resources):
        if query.filters.memory_types and "fact" not in query.filters.memory_types:
            hits = []
        else:
            async with resources.session_factory() as session, session.begin():
                # SQLAlchemy begin() alone does not begin a SQLite read snapshot.
                await session.execute(text("BEGIN"))
                facts = await resources.provider.search_facts_in_session(
                    session,
                    text=query.query,
                    limit=query.limit,
                    include_inactive=query.include_inactive,
                    memory_types=tuple(query.filters.memory_types),
                    lifecycle_states=tuple(query.filters.lifecycle_states),
                    min_confidence=query.filters.min_confidence,
                )
                if not isinstance(facts, list) or len(facts) > query.limit:
                    raise HttpError("E_INTERNAL")
                ids = tuple(f.id for f in facts)
                if (
                    any(type(ident) is not str or not ident for ident in ids)
                    or len(set(ids)) != len(ids)
                    or any(not math.isfinite(f.confidence) for f in facts)
                ):
                    raise HttpError("E_INTERNAL")
                records = await resources.provider.read_fact_records(session, ids)
                if tuple((r.memory_type, r.memory["id"]) for r in records) != tuple(("fact", i) for i in ids):
                    raise HttpError("E_STORAGE")
                hits = [
                    d.SearchHit(
                        memory=project_for_caller(ctx, "search", record, "unknown"), score=0.0, rank=i, source="fts"
                    )
                    for i, record in enumerate(records, 1)
                ]
        degraded = (
            []
            if query.mode == "keyword"
            else [d.SourceFailure(source=source, code="E_CAPABILITY_UNAVAILABLE") for source in ("semantic", "graph")]
        )
        return d.SearchResult(
            request_id=ctx.request_id, results=hits, total=len(hits), trace_id=None, degraded=degraded
        )

    async def _context(self, ctx, query, retrieval, resources):
        raw = await retrieval.get_context(
            ContextCommand(
                task=query.task,
                subject=query.subject,
                max_results=query.max_results,
                include_inactive=query.include_inactive,
            )
        )
        try:
            # Validate shape before DTO normalization or masking could hide missing/extra fields.
            mask_context_package(raw)
            if len(raw["facts"]) > query.max_results or len(raw["decisions"]) > query.max_results:
                raise ValueError("context budget exceeded")
            package = d.ContextPackage.model_validate_json(json.dumps(_context_json_ready(raw)))
            value = package.model_dump(mode="json")
            items = value["facts"] + value["decisions"]
            keys = tuple(("fact", item["id"]) for item in value["facts"]) + tuple(
                ("decision", item["id"]) for item in value["decisions"]
            )
            if len(keys) > 200 or len(set(keys)) != len(keys) or any(type(k[1]) is not str or not k[1] for k in keys):
                raise ValueError("invalid typed keys")
            memories = []
            if keys:
                async with resources.session_factory() as session, session.begin():
                    await session.execute(text("BEGIN"))
                    records = await resources.provider.read_memory_records(session, keys)
                if tuple((r.memory_type, r.memory["id"]) for r in records) != keys:
                    raise HttpError("E_STORAGE")
                for item, record in zip(items, records, strict=True):
                    memory = project_for_caller(ctx, "context", record, "unknown")
                    visible = memory.model_dump(mode="json")
                    if (
                        (not query.include_inactive and memory.lifecycle_state not in _ACTIVE)
                        or any(
                            item[field] != visible[field]
                            for field in ("confidence", "lifecycle_state", "created_at", "updated_at")
                        )
                        or item["verification_status"] != record.memory["verification_status"]
                    ):
                        raise HttpError("E_STORAGE")
                    memories.append(memory)
            return d.ContextResult(
                request_id=ctx.request_id,
                package=d.ContextPackage.model_validate_json(json.dumps(mask_context_package(value))),
                memories=memories,
                trace_id=None,
                estimated_tokens=None,
                tokenizer=None,
                token_count_is_estimate=True,
            )
        except HttpError:
            raise
        except (ValueError, TypeError, KeyError, AttributeError):
            raise HttpError("E_INTERNAL") from None
