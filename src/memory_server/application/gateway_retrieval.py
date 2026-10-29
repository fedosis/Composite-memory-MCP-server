"""Application port for S08 search and context retrieval."""

from __future__ import annotations

import json
import re
from contextlib import asynccontextmanager
from time import monotonic

from sqlalchemy.exc import SQLAlchemyError

from memory_server.application.contracts import ContextCommand
from memory_server.application.errors import UnknownDependencyError
from memory_server.application.memory_read_service import project_for_caller
from memory_server.gateway_contracts import dto as d
from memory_server.transports.http.errors import HttpError

_SAFE_VERSION = re.compile(r"[0-9]+(?:\.[0-9]+)*")


def _masked(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("maskable field is not string")
    return "[MASKED]"


def _mask_version(value):
    if type(value) is int or value is None:
        return value
    if type(value) is str and len(value) <= 128 and _SAFE_VERSION.fullmatch(value):
        return value
    if isinstance(value, bool) or not isinstance(value, str):
        raise ValueError("invalid version")
    return "[MASKED]"


def mask_context_package(package: dict) -> dict:
    if not isinstance(package, dict) or tuple(package) != ("facts", "decisions", "total"):
        raise ValueError("invalid context package")
    if (
        not isinstance(package["facts"], list)
        or not isinstance(package["decisions"], list)
        or type(package["total"]) is not int
    ):
        raise ValueError("invalid context package")
    result = {"facts": [], "decisions": [], "total": package["total"]}
    fact_fields = ("subject", "predicate", "object", "dedup_key", "source", "creator")
    decision_fields = ("context", "choice", "reason", "source", "creator")
    for item in package["facts"]:
        if not isinstance(item, dict):
            raise ValueError("invalid fact")
        out = dict(item)
        for name in fact_fields:
            out[name] = _masked(item.get(name))
        out["version"] = _mask_version(item.get("version"))
        result["facts"].append(out)
    for item in package["decisions"]:
        if not isinstance(item, dict):
            raise ValueError("invalid decision")
        out = dict(item)
        for name in decision_fields:
            out[name] = _masked(item.get(name))
        alternatives = item.get("rejected_alternatives")
        if not isinstance(alternatives, list):
            raise ValueError("invalid alternatives")
        out["rejected_alternatives"] = [_masked(value) for value in alternatives]
        out["version"] = _mask_version(item.get("version"))
        result["decisions"].append(out)
    return result


def _context_json_ready(raw: dict) -> dict:
    """Normalize legacy naive API timestamps to the wire's explicit UTC form."""
    value = json.loads(json.dumps(raw))
    for group in ("facts", "decisions"):
        for item in value.get(group, []):
            for name in ("created_at", "updated_at"):
                stamp = item.get(name)
                if isinstance(stamp, str) and "+" not in stamp and not stamp.endswith("Z"):
                    item[name] = stamp + "+00:00"
    return value


class GatewayRetrievalPort:
    def __init__(self, leases, bind_read, metrics):
        self.leases, self.bind_read, self.metrics = leases, bind_read, metrics

    @asynccontextmanager
    async def _stage(self, ctx, stage):
        started = monotonic()
        code = "E_INTERNAL"
        try:
            yield
            code = None
        except HttpError as exc:
            code = exc.code
            raise
        except SQLAlchemyError:
            code = "E_STORAGE"
            raise HttpError("E_STORAGE") from None
        finally:
            self.metrics.retrieval_completed(ctx, stage, monotonic() - started, code)

    async def execute(self, ctx, command):
        if "memory.read" not in ctx.principal.permissions or ctx.profile_id not in ctx.principal.profiles:
            raise HttpError("E_PERMISSION_DENIED")
        if command.profile_id != ctx.profile_id or command.operation not in ("search", "context"):
            raise HttpError("E_VALIDATION")
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
            async with resources.session_factory() as session:
                async with session.begin():
                    facts = await resources.provider.search_facts_in_session(
                        session,
                        text_query=query.query,
                        limit=query.limit,
                        include_inactive=query.include_inactive,
                        memory_types=tuple(query.filters.memory_types),
                        lifecycle_states=tuple(query.filters.lifecycle_states),
                        min_confidence=query.filters.min_confidence,
                    )
                    ids = tuple(f.id for f in facts)
                    records = await resources.provider.read_fact_records(session, ids)
                    if tuple(r.memory["id"] for r in records) != ids:
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
            else [d.SourceFailure(source=s, code="E_CAPABILITY_UNAVAILABLE") for s in ("semantic", "graph")]
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
            package = d.ContextPackage.model_validate_json(json.dumps(_context_json_ready(raw)))
            masked = mask_context_package(package.model_dump(mode="json"))
            keys = tuple(("fact", item["id"]) for item in masked["facts"]) + tuple(
                ("decision", item["id"]) for item in masked["decisions"]
            )
            if len(keys) > 200 or len(set(keys)) != len(keys) or any(type(k[1]) is not str or not k[1] for k in keys):
                raise ValueError("invalid typed keys")
            if not keys:
                memories = []
            else:
                async with resources.session_factory() as session:
                    async with session.begin():
                        records = await resources.provider.read_memory_records(session, keys)
                if tuple((r.memory_type, r.memory["id"]) for r in records) != keys:
                    raise HttpError("E_STORAGE")
                memories = [project_for_caller(ctx, "context", record, "unknown") for record in records]
            return d.ContextResult(
                request_id=ctx.request_id,
                package=d.ContextPackage.model_validate_json(json.dumps(masked)),
                memories=memories,
                trace_id=None,
                estimated_tokens=None,
                tokenizer=None,
                token_count_is_estimate=True,
            )
        except HttpError:
            raise
        except (ValueError, TypeError, KeyError):
            raise HttpError("E_INTERNAL") from None
