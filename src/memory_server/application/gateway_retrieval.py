"""Application orchestration for the gateway's facts-only search and context."""

from __future__ import annotations

import asyncio
import json
import math
import re
import sqlite3
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from time import monotonic
from typing import Protocol
from uuid import NAMESPACE_DNS, uuid5

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from memory_server.application.contracts import ContextCommand, GraphSearchCommand, SemanticSearchCommand
from memory_server.application.errors import UnknownDependencyError
from memory_server.application.memory_read_service import project_for_caller
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import ProfileSession, RetrievalPort
from memory_server.router.embedding_router import NativeSubmitter
from memory_server.router.hybrid_router import StrictSourceFailed, collect_strict
from memory_server.router.ranking import RankMerger, RankResult
from memory_server.transports.http.errors import HttpError, error_code

_SAFE_VERSION = re.compile(r"[0-9]+(?:\.[0-9]+)*")
_ACTIVE = frozenset(("candidate", "validated", "active"))


def _finite_number(value):
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


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

    if query.trace:
        raise HttpError("E_CAPABILITY_UNAVAILABLE")


class NativeCustody(NativeSubmitter, Protocol):
    async def drain(self) -> None: ...


NativeBinder = Callable[[ProfileSession], NativeCustody]


class GatewayRetrievalPort(RetrievalPort):
    def __init__(self, leases, bind_read, metrics, *, native: NativeBinder | None = None):
        self.leases, self.bind_read, self.metrics = leases, bind_read, metrics
        self.native = native

    @asynccontextmanager
    async def _stage(self, ctx, stage):
        started, code = monotonic(), "E_INTERNAL"
        try:
            state = {"code": None}
            yield state
            code = state["code"]
        except asyncio.CancelledError:
            code = "E_TIMEOUT"
            raise
        except HttpError as exc:
            code = exc.code
            raise
        except (SQLAlchemyError, sqlite3.Error):
            code = "E_STORAGE"
            raise HttpError("E_STORAGE") from None
        except Exception as exc:
            code = error_code(exc)
            raise
        finally:
            self.metrics.retrieval_completed(ctx, stage, monotonic() - started, code)

    async def execute(self, ctx, command):
        if "memory.read" not in ctx.principal.permissions or ctx.profile_id not in ctx.principal.profiles:
            raise HttpError("E_PERMISSION_DENIED")
        if command.profile_id != ctx.profile_id or command.operation not in ("search", "context"):
            raise HttpError("E_VALIDATION")
        _preflight(command.operation, command.payload)
        async with self.leases.acquire(ctx) as scope:
            if command.operation == "search" and command.payload.mode != "keyword":
                try:
                    resources = self.bind_read(scope)
                    retrieval = scope.application.retrieval
                except UnknownDependencyError:
                    raise HttpError("E_RUNTIME_NOT_READY") from None
                if (self.native is None or resources.provider is None
                        or resources.session_factory is None or retrieval is None):
                    raise HttpError("E_RUNTIME_NOT_READY")
                custody = self.native(scope)
                return await self._run_before_deadline(
                    ctx, self._search_sources(ctx, command.payload, resources, retrieval, scope, custody), custody,
                )
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
        return d.SearchResult(
            request_id=ctx.request_id, results=hits, total=len(hits), trace_id=None, degraded=[]
        )

    async def _run_before_deadline(self, ctx, work, custody):
        operation = asyncio.create_task(work)

        async def cleanup():
            if not operation.done():
                operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)
            await custody.drain()

        try:
            done, _ = await asyncio.wait({operation}, timeout=max(0.0, ctx.deadline - monotonic()))
            if not done:
                raise HttpError("E_TIMEOUT")
            return operation.result()
        finally:
            # Keep ONE retained cleanup task through repeated external cancels.
            retained = asyncio.create_task(cleanup())
            cancelled = None
            while not retained.done():
                try:
                    await asyncio.shield(retained)
                except asyncio.CancelledError as exc:
                    cancelled = exc
            retained.result()
            if cancelled is not None:
                raise cancelled

    @staticmethod
    def _deadline(ctx):
        if monotonic() >= ctx.deadline:
            raise HttpError("E_TIMEOUT")

    async def _collect_native(self, ctx, query, retrieval, scope, custody):
        async def semantic():
            if not await retrieval.gateway_semantic_available():
                return None
            async with self._stage(ctx, "semantic") as state:
                outcome = await retrieval.gateway_semantic(
                    SemanticSearchCommand(query=query.query, top_k=query.limit, score_threshold=None), native=custody,
                )
                state["code"] = outcome.code
                return outcome

        async def graph():
            started, code = None, "E_INTERNAL"

            async def native(function, /, *args, kind, **kwargs):
                nonlocal started
                if started is None:
                    started = monotonic()
                return await custody(function, *args, kind=kind, **kwargs)

            try:
                service = scope.application.graph
                outcome = await service.query_bounded(GraphSearchCommand(query=query.query), native=native)
                code = outcome.code
                return outcome
            except UnknownDependencyError:
                return None
            except asyncio.CancelledError:
                code = "E_TIMEOUT"
                raise
            except Exception as exc:
                code = error_code(exc)
                raise
            finally:
                if started is not None:
                    self.metrics.retrieval_completed(ctx, "graph", monotonic() - started, code)

        names = ("semantic", "graph") if query.mode == "hybrid" else (query.mode,)
        tasks = [asyncio.create_task(semantic() if name == "semantic" else graph()) for name in names]
        try:
            values = await asyncio.gather(*tasks, return_exceptions=True)
            for value in values:
                if isinstance(value, BaseException):
                    raise value
            return dict(zip(names, values, strict=True))
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    def _eligible_fact(query, lifecycle_state, confidence):
        filters = query.filters
        return (
            (not filters.memory_types or "fact" in filters.memory_types)
            and (query.include_inactive or lifecycle_state in _ACTIVE)
            and (not filters.lifecycle_states or lifecycle_state in filters.lifecycle_states)
            and (filters.min_confidence is None or confidence >= filters.min_confidence)
        )

    async def _resolve_rows(self, query, provider, session, triples, expected):
        rows = await provider.resolve_fact_rows(session, triples, include_inactive=query.include_inactive)
        seen, groups = set(), {triple: [] for triple in triples}
        for row in rows:
            triple = (row.subject, row.predicate, row.object)
            if (triple not in groups or type(row.id) is not str or not row.id or row.id in seen
                    or not _finite_number(row.confidence)
                    or not isinstance(row.created_at, datetime)):
                raise HttpError("E_INTERNAL")
            seen.add(row.id)
            prior = expected.get(row.id)
            fields = ("subject", "predicate", "object", "lifecycle_state", "confidence")
            if prior is not None and any(getattr(prior, field) != getattr(row, field) for field in fields):
                raise HttpError("E_INTERNAL")
            expected[row.id] = row
            groups[triple].append(row)
        return groups

    async def _resolve_semantic(self, query, provider, session, outcome, expected):
        if outcome is None or outcome.state != "ok":
            return []
        valid = []
        for hit in outcome.results:
            if type(hit) is not dict or type(hit.get("payload")) is not dict:
                continue
            payload, ident, score = hit["payload"], hit.get("id"), hit.get("score")
            triple = tuple(payload.get(field) for field in ("subject", "predicate", "object"))
            if (payload.get("memory_type") != "fact" or type(ident) is not str or not ident
                    or any(type(value) is not str or not value for value in triple)
                    or not _finite_number(score)):
                continue
            valid.append((ident, float(score), triple))
        triples = tuple(dict.fromkeys(item[2] for item in valid))
        groups = await self._resolve_rows(query, provider, session, triples, expected)
        candidates = {}
        for ident, score, triple in valid:
            matched = [row for row in groups[triple] if str(uuid5(NAMESPACE_DNS, f"fact:{row.id}")) == ident]
            if len(matched) > 1:
                raise HttpError("E_INTERNAL")
            if matched:
                row = matched[0]
                if self._eligible_fact(query, row.lifecycle_state, row.confidence):
                    candidates[row.id] = max(score, candidates.get(row.id, -math.inf))
        return list(candidates.items())[:query.limit]

    async def _resolve_graph(self, query, provider, session, outcome, expected):
        if outcome is None or outcome.state != "ok":
            return []
        triples = []
        seen = set()
        for relation in outcome.relations:
            if type(relation) is not dict:
                continue
            triple = tuple(relation.get(field) for field in ("source_name", "relation", "target_name"))
            if any(type(value) is not str or not value for value in triple) or triple in seen:
                continue
            seen.add(triple)
            if len(triples) < query.limit:
                triples.append(triple)
        groups = await self._resolve_rows(query, provider, session, tuple(triples), expected)
        candidates = {}
        for triple in triples:
            for row in groups[triple]:
                if self._eligible_fact(query, row.lifecycle_state, row.confidence):
                    candidates.setdefault(row.id, 0.8)
        return list(candidates.items())[:query.limit]

    async def _hydrate_candidates(self, query, provider, session, batches, expected):
        ids = tuple(dict.fromkeys(ident for batch in batches.values() for ident, _ in batch))
        if len(ids) > 300:
            raise HttpError("E_INTERNAL")
        records = {}
        for start in range(0, len(ids), 100):
            chunk = ids[start:start + 100]
            hydrated = await provider.read_fact_records(session, chunk)
            for record in hydrated:
                key = (record.memory_type, record.memory["id"])
                if key[0] != "fact" or key[1] not in chunk or key[1] in records:
                    raise HttpError("E_INTERNAL")
                row = expected[key[1]]
                if any(record.memory[field] != getattr(row, field)
                       for field in ("subject", "predicate", "object", "lifecycle_state", "confidence")):
                    raise HttpError("E_INTERNAL")
                records[key[1]] = record
            if any(ident not in records for ident in chunk):
                raise HttpError("E_STORAGE")
        return records

    async def _search_sources(self, ctx, query, resources, retrieval, scope, custody):
        self._deadline(ctx)
        outcomes = await self._collect_native(ctx, query, retrieval, scope, custody)
        # Projection threads must be terminated before opening the SQL snapshot.
        await custody.drain()
        self._deadline(ctx)
        if query.mode != "hybrid":
            outcome = outcomes[query.mode]
            if outcome is None:
                raise HttpError("E_CAPABILITY_UNAVAILABLE")
            if outcome.state == "failed":
                raise HttpError(outcome.code)
        merge_started, merge_code = None, "E_INTERNAL"
        try:
            async with resources.session_factory() as session, session.begin():
                await session.execute(text("BEGIN"))
                batches = {"fts": []}
                expected = {}
                if query.mode == "hybrid":
                    async with self._stage(ctx, "fts"):
                        facts = await resources.provider.search_facts_in_session(
                            session, text=query.query, limit=query.limit, include_inactive=query.include_inactive,
                            memory_types=tuple(query.filters.memory_types),
                            lifecycle_states=tuple(query.filters.lifecycle_states),
                            min_confidence=query.filters.min_confidence,
                        )
                        if (type(facts) is not list or len(facts) > query.limit
                                or any(type(f.id) is not str or not f.id or not _finite_number(f.confidence)
                                       for f in facts)
                                or len({f.id for f in facts}) != len(facts)):
                            raise HttpError("E_INTERNAL")
                        batches["fts"] = [(fact.id, 0.0) for fact in facts]
                        expected.update((fact.id, fact) for fact in facts)
                self._deadline(ctx)
                merge_started = monotonic()
                batches["semantic"] = await self._resolve_semantic(
                    query, resources.provider, session, outcomes.get("semantic"), expected,
                )
                batches["graph"] = await self._resolve_graph(
                    query, resources.provider, session, outcomes.get("graph"), expected,
                )
                records = await self._hydrate_candidates(query, resources.provider, session, batches, expected)
                self._deadline(ctx)
                ranked_batches = {}
                for source, batch in batches.items():
                    ranked_batches[source] = [
                        RankResult(content=f"fact:{ident}", score=score, source=source,
                                   confidence=records[ident].memory["confidence"], metadata={"id": ident})
                        for ident, score in batch
                        if self._eligible_fact(query, records[ident].memory["lifecycle_state"],
                                               records[ident].memory["confidence"])
                    ]
                failures = {}
                if query.mode == "hybrid":
                    def callback(source):
                        outcome = outcomes.get(source)
                        if source != "fts" and outcome is None:
                            return None

                        async def collect():
                            if source != "fts" and outcome.state == "failed":
                                raise StrictSourceFailed(outcome.code)
                            return ranked_batches[source]
                        return collect

                    merged = await collect_strict(fts=callback("fts"), semantic=callback("semantic"),
                                                  graph=callback("graph"), merger=RankMerger(), limit=query.limit)
                    ranked, failures = merged.ranked, merged.failures
                else:
                    ranked = sorted(ranked_batches[query.mode], key=lambda item: item.score, reverse=True)[:query.limit]
                hits = [d.SearchHit(memory=project_for_caller(ctx, "search", records[item.metadata["id"]], "unknown"),
                                    score=item.score, rank=index, source=item.source)
                        for index, item in enumerate(ranked, 1)]
            result = d.SearchResult(
                request_id=ctx.request_id, results=hits, total=len(hits), trace_id=None,
                degraded=[d.SourceFailure(source=source, code=failures[source])
                          for source in ("fts", "semantic", "graph") if source in failures],
            )
            merge_code = None
            return result
        except asyncio.CancelledError:
            merge_code = "E_TIMEOUT"
            raise
        except (SQLAlchemyError, sqlite3.Error):
            merge_code = "E_STORAGE"
            raise HttpError("E_STORAGE") from None
        except Exception as exc:
            merge_code = error_code(exc)
            raise
        finally:
            if merge_started is not None:
                self.metrics.retrieval_completed(ctx, "merge", monotonic() - merge_started, merge_code)

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
