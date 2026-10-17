"""MCP server entry point with tool registrations.

v0.6 Phase 4: Uses transactional outbox pattern for async indexing.
- remember() / learn() write to SQL + outbox in same transaction
- Outbox worker polls pending entries and pushes to Qdrant + graph
- Failed entries are retried 3 times, then marked as failed
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mcp.server.fastmcp import FastMCP
from storage.outbox_worker import OutboxWorker

from memory_server.api.add_relation import add_relation as add_relation_fn
from memory_server.api.get_context import get_context as get_context_fn
from memory_server.api.learn import learn as learn_fn
from memory_server.api.remember import remember as remember_fn
from memory_server.api.search import search as search_fn
from memory_server.application.application import MemoryApplication
from memory_server.application.contracts import (
    AuditCommand,
    BeliefPolicy,
    GraphPolicy,
    LifecycleCommand,
    MemoryPolicy,
    ReflectionPolicy,
    RetrievalPolicy,
)
from memory_server.evaluation.auditor import collect_persisted_audit_state
from memory_server.evaluation.confidence import ConfidenceEngine
from memory_server.evaluation.decay import DecayEngine
from memory_server.evaluation.metrics import get_collector
from memory_server.evaluation.validator import Validator
from memory_server.models import Belief
from memory_server.paths import (
    StorageLayout,
    StorageResolutionInputs,
    resolve_storage_layout,
    serialize_layout_redacted,
    validate_write_target,
)
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.router.graph_router import GraphRouter
from memory_server.runtime.dependencies import RuntimeDependencies
from memory_server.runtime.errors import RuntimeNotReadyError
from memory_server.runtime.runtime_factory import RuntimeFactory
from memory_server.services.ingestion_service import reinforce_belief_item
from memory_server.services.lifecycle_service import (
    LifecycleService,
    LifecycleTransitionRequest,
)
from memory_server.settings import get_settings

if TYPE_CHECKING:
    from memory_server.providers.lancedb_provider import LanceDBProvider
    from memory_server.providers.qdrant_provider import QdrantProvider
    from memory_server.router.embedding_router import EmbeddingRouter
    from memory_server.router.hybrid_router import HybridRouter

logger = logging.getLogger(__name__)


def _install_mcp_shutdown_report_shape() -> None:
    """Keep the runtime's single deadline schedule observable from MCP."""
    from memory_server.runtime import runtime as runtime_module

    report_type = runtime_module.ShutdownReport
    if getattr(report_type, "_mcp_deadline_shape", False):
        return

    class MCPShutdownReport(report_type):
        _mcp_deadline_shape = True

        def __init__(
            self,
            phases=None,
            flushed=0,
            failed=0,
            requeued=0,
            unsettled=0,
            retained_resources=(),
            lock_retained=False,
            code=None,
            errors=(),
        ):
            normalized = dict(phases or {})
            deadlines = tuple(normalized.get("deadlines", ()))
            if "closing" in normalized and len(deadlines) == 4:
                normalized["deadlines"] = (normalized["closing"], *deadlines)
            super().__init__(
                phases=normalized,
                flushed=flushed,
                failed=failed,
                requeued=requeued,
                unsettled=unsettled,
                retained_resources=retained_resources,
                lock_retained=lock_retained,
                code=code,
                errors=errors,
            )

    runtime_module.ShutdownReport = MCPShutdownReport


class _MCPRuntimeHooks:
    """Adapter-neutral hooks for the single runtime-owned shutdown controller."""

    def __init__(self) -> None:
        self.worker_task: asyncio.Task | None = None

    async def stop_admission(self) -> None:
        return None

    async def invalidate_prefetch_generation(self) -> None:
        return None

    async def pause_writer(self) -> None:
        return None

    async def drain_producers(self, deadline: float) -> None:
        if self.worker_task is not None:
            await self.worker_task

    async def settle_writer(self, deadline: float) -> dict[str, int]:
        return {}

    async def flush_final(self, deadline: float) -> dict[str, int]:
        return {}


class MCPComposition:
    """One MCP composition over one runtime-owned resource graph."""

    def __init__(
        self,
        *,
        settings: Any,
        layout: StorageLayout,
        factory: RuntimeFactory,
        runtime: Any,
    ) -> None:
        self.settings = settings
        self.layout = layout
        self.factory = factory
        self.runtime = runtime
        self.app: MemoryApplication | None = None
        self.routers: dict[str, Any] = {}
        self.worker_task: asyncio.Task | None = None

    @classmethod
    def create(cls) -> "MCPComposition":
        settings = get_settings()
        layout = resolve_storage_layout(StorageResolutionInputs(
            mode="standalone",
            data_root=os.environ.get("MEMORY_SERVER_DATA_ROOT") or None,
            sqlite_url=settings.db_url,
            vector_backend=settings.vector_backend,
            lancedb_path=settings.lancedb_path,
            graph_snapshot_path=settings.graph_snapshot_path,
            qdrant_location=settings.qdrant_location,
            vector_collection=settings.vector_collection,
        ))
        hooks = _MCPRuntimeHooks()
        factory = RuntimeFactory.create(
            layout,
            settings=settings,
            dependencies=RuntimeDependencies(hooks=hooks),
        )
        _install_mcp_shutdown_report_shape()
        factory.runtime.preflight()
        return cls(
            settings=settings,
            layout=layout,
            factory=factory,
            runtime=factory.runtime,
        )

    async def start(self) -> None:
        await self.runtime.start()
        dependencies = self.runtime.dependencies
        graph = dependencies.graph
        if graph is not None:
            graph.load_snapshot()

        worker = dependencies.outbox_worker
        graph_router = getattr(worker, "_graph_router", None) if worker is not None else None
        if graph_router is None and graph is not None:
            graph_router = GraphRouter(
                graph=graph,
                max_path_depth=self.factory.config.routing["graph_max_path_depth"],
            )
        self.routers["graph_router"] = graph_router

        confidence = ConfidenceEngine(settings={
            "confidence_source_reliability": self.factory.config.evaluation["confidence"]["source_reliability"],
            "confidence_ttl_days": self.factory.config.evaluation["confidence"]["ttl_days"],
            "confidence_lifecycle_multipliers": self.factory.config.evaluation["confidence"]["lifecycle_multipliers"],
            "confidence_corroboration_boost": self.factory.config.evaluation["confidence"]["corroboration_boost"],
            "confidence_boost_threshold": self.factory.config.evaluation["confidence"]["boost_threshold"],
            "confidence_conflict_penalty": self.factory.config.evaluation["confidence"]["conflict_penalty"],
            "confidence_penalty_threshold": self.factory.config.evaluation["confidence"]["penalty_threshold"],
        })
        validator = Validator(confidence_engine=confidence)
        decay_values = self.factory.config.evaluation["decay"]
        decay = DecayEngine(
            validator=validator,
            settings={
                "ttl_days": self.factory.config.evaluation["ttl_days"],
                "decay_archive_threshold": decay_values["archive_threshold"],
                "decay_stale_ratio": decay_values["stale_ratio"],
                "decay_archive_ratio": decay_values["archive_ratio"],
                "decay_forgotten_ratio": decay_values["forgotten_ratio"],
                "decay_default_ttl_days": decay_values["default_ttl_days"],
                "decay_base": decay_values["base"],
                "decay_confidence_floor": decay_values["confidence_floor"],
            },
        )
        for name, resource in (
            ("validator", validator),
            ("confidence_engine", confidence),
            ("decay_engine", decay),
        ):
            self.runtime.dependencies = self.runtime.dependencies.with_resource(name, resource)

        sqlite = self.runtime.dependencies.sqlite
        vector = self.runtime.dependencies.vector
        audit_state = await collect_persisted_audit_state(
            sqlite=sqlite,
            vector_provider=vector,
        )
        self.app = MemoryApplication(
            sqlite._session_factory,
            policy=MemoryPolicy.capture(self.settings),
            retrieval_provider=sqlite,
            retrieval_policy=RetrievalPolicy.capture(self.settings),
            graph_policy=GraphPolicy.capture(self.settings),
            get_embedding_router=self.get_embedding_router,
            get_hybrid_router=self.get_hybrid_router,
            get_graph_router=self.get_graph_router,
            belief_provider=sqlite,
            belief_policy=BeliefPolicy.capture(self.settings),
            get_graph=self.get_graph,
            reflection_provider=sqlite,
            reflection_policy=ReflectionPolicy.capture(self.settings),
            relation_provider=sqlite,
            lifecycle_provider=sqlite,
            audit_validator=validator,
            audit_confidence_engine=confidence,
            audit_sqlite=sqlite,
            audit_vector_provider=vector,
            audit_state=audit_state,
        )

        if worker is not None:
            self.worker_task = asyncio.create_task(worker.run())
            self.runtime.dependencies.hooks.worker_task = self.worker_task

    async def close(self) -> Any:
        return await self.runtime.close()

    async def refresh_audit_state(self) -> Any:
        if self.app is None:
            raise RuntimeNotReadyError()
        state = await collect_persisted_audit_state(
            sqlite=self.runtime.dependencies.sqlite,
            vector_provider=self.runtime.dependencies.vector,
        )
        audit = self.app.audit
        audit._sqlite_counts = dict(state["sqlite_counts"])
        audit._receipt_ids = set(state["receipt_ids"])
        audit._records = list(state["records"]) if state["records"] is not None else None
        audit._vector_point_count = state["vector_point_count"]
        return audit

    def _dependency(self, name: str) -> Any:
        if self.runtime.state != "READY":
            raise RuntimeNotReadyError()
        value = getattr(self.runtime.dependencies, name)
        if value is None:
            raise RuntimeError(f"E_PROJECTION_UNAVAILABLE: {name}")
        return value

    async def get_embedding_router(self) -> Any:
        router = self.routers.get("embedding_router")
        if router is None:
            from memory_server.router.embedding_router import EmbeddingRouter

            router = EmbeddingRouter(
                vector_provider=self._dependency("vector"),
                embedder=self._dependency("embedder"),
            )
            self.routers["embedding_router"] = router
        return router

    async def get_hybrid_router(self) -> Any:
        router = self.routers.get("hybrid_router")
        if router is None:
            from memory_server.router.hybrid_router import HybridRouter

            router = HybridRouter(
                vector_provider=self._dependency("vector"),
                embedder=self._dependency("embedder"),
                graph=self._dependency("graph"),
            )
            self.routers["hybrid_router"] = router
        return router

    async def get_graph_router(self) -> Any:
        return self.routers.get("graph_router") or self._dependency("graph_router")

    async def get_graph(self) -> Any:
        return self._dependency("graph")


_active_composition: MCPComposition | None = None


def __getattr__(name: str) -> Any:
    """Expose legacy names as read-only views over the active composition."""
    aliases = {
        "_provider": "sqlite",
        "_qdrant": "vector",
        "_lancedb": "vector",
        "_embedder": "embedder",
        "_graph": "graph",
        "_validator_store": "validator",
        "_confidence_engine": "confidence_engine",
        "_decay_engine": "decay_engine",
        "_outbox_worker": "outbox_worker",
    }
    if name in aliases:
        if _active_composition is None:
            return None
        return getattr(_active_composition.runtime.dependencies, aliases[name])
    if name == "_router":
        return None if _active_composition is None else _active_composition.routers.get("embedding_router")
    if name == "_graph_router":
        return None if _active_composition is None else _active_composition.routers.get("graph_router")
    if name == "_hybrid_router":
        return None if _active_composition is None else _active_composition.routers.get("hybrid_router")
    if name == "_root_lock":
        return None if _active_composition is None else _active_composition.runtime.lock
    if name == "_storage_layout":
        return None if _active_composition is None else _active_composition.layout
    if name == "_outbox_task":
        return None if _active_composition is None else _active_composition.worker_task
    if name == "_storage_settings":
        return None if _active_composition is None else _active_composition.settings
    if name == "_storage_cleanup_failed":
        return _active_composition is not None and _active_composition.runtime.state == "FAILED"
    raise AttributeError(name)


PROJECTION_MIGRATION_HINT = "Run `memory-server migrate-profile-storage` to rebuild unavailable projections."


def _projection_error(exc: BaseException) -> str | None:
    message = str(exc)
    if not message.startswith("E_PROJECTION_UNAVAILABLE"):
        return None
    return json.dumps({
        "error": "E_PROJECTION_UNAVAILABLE",
        "message": message,
        "hint": PROJECTION_MIGRATION_HINT,
    })


def _stable_projection_errors(function):
    """Map only projection-unavailable failures at public tool boundaries."""
    @functools.wraps(function)
    async def wrapped(*args, **kwargs):
        try:
            return await function(*args, **kwargs)
        except RuntimeError as exc:
            result = _projection_error(exc)
            if result is not None:
                return result
            raise
    return wrapped


def _runtime_or_raise() -> MCPComposition:
    if _active_composition is None:
        raise RuntimeNotReadyError()
    return _active_composition


def _runtime_admitted(function):
    """Admit each operation through CMMSRuntime and release it in ``finally``."""
    @functools.wraps(function)
    async def wrapped(*args, **kwargs):
        runtime = _runtime_or_raise().runtime
        token = runtime.admit()
        try:
            result = function(*args, **kwargs)
            return await result if inspect.isawaitable(result) else result
        finally:
            runtime.release_admitted(token)
    return wrapped


def _get_storage_settings():
    if _active_composition is not None:
        return _active_composition.settings
    return get_settings()


def _resolve_layout_from_settings(settings: Any) -> StorageLayout:
    return resolve_storage_layout(StorageResolutionInputs(
        mode="standalone",
        data_root=os.environ.get("MEMORY_SERVER_DATA_ROOT") or None,
        sqlite_url=settings.db_url,
        vector_backend=settings.vector_backend,
        lancedb_path=settings.lancedb_path,
        graph_snapshot_path=settings.graph_snapshot_path,
        qdrant_location=settings.qdrant_location,
        vector_collection=settings.vector_collection,
    ))


def _get_storage_layout() -> StorageLayout:
    if _active_composition is not None:
        return _active_composition.layout
    return _resolve_layout_from_settings(get_settings())


def _ensure_storage_ready() -> StorageLayout:
    composition = _runtime_or_raise()
    composition.runtime.preflight()
    if composition.runtime.state != "READY":
        raise RuntimeNotReadyError()
    return composition.layout


def _prepare_store(path: Path) -> None:
    """Compatibility delegate; resource preparation belongs to runtime recipes."""
    validate_write_target(_ensure_storage_ready(), path)


async def _shutdown_storage() -> Any:
    """Compatibility delegate to the single retained runtime cleanup task."""
    return await _runtime_or_raise().close()


@asynccontextmanager
async def lifespan(server: FastMCP):
    """Start one runtime and delegate all cleanup to its S1 controller."""
    global _active_composition
    composition = MCPComposition.create()
    _active_composition = composition
    try:
        await composition.start()
        logger.info("cmms.storage_layout %s", serialize_layout_redacted(composition.layout))
        yield {
            "provider": composition.runtime.dependencies.sqlite,
            "outbox_worker": composition.runtime.dependencies.outbox_worker,
        }
    finally:
        try:
            await composition.close()
        finally:
            if _active_composition is composition:
                _active_composition = None


mcp = FastMCP("CompositeMemoryServer", lifespan=lifespan)


async def _get_provider() -> SQLiteProvider:
    """Read the SQLite dependency owned by the active runtime."""
    return _runtime_or_raise()._dependency("sqlite")


def _get_sqlite_db_url() -> str:
    """Return the frozen SQLite URL captured by the active composition."""
    return _get_storage_layout().sqlite.effective_url


def _get_graph_snapshot_path() -> Path:
    """Return the frozen graph snapshot path captured by the active composition."""
    return _get_storage_layout().graph_snapshot_path


async def _get_lancedb_provider() -> LanceDBProvider:
    return _runtime_or_raise()._dependency("vector")


async def _get_qdrant_provider() -> QdrantProvider:
    return _runtime_or_raise()._dependency("vector")


async def _get_vector_provider():
    """Read the one vector dependency selected by the frozen runtime config."""
    return _runtime_or_raise()._dependency("vector")


def _build_embedder():
    """Read the embedder constructed by the runtime recipe."""
    return _runtime_or_raise()._dependency("embedder")


async def _build_auditor():
    """Return the application audit facade over shared runtime dependencies."""
    return await _runtime_or_raise().refresh_audit_state()


async def _get_router() -> EmbeddingRouter:
    return await _runtime_or_raise().get_embedding_router()


async def _get_outbox_worker() -> OutboxWorker:
    return _runtime_or_raise()._dependency("outbox_worker")


async def _get_graph_router() -> GraphRouter:
    return await _runtime_or_raise().get_graph_router()


async def _get_graph() -> SimpleGraph:
    return await _runtime_or_raise().get_graph()


async def _get_lifecycle_service():
    application = _runtime_or_raise().app
    if application is None:
        raise RuntimeNotReadyError()
    return application.lifecycle


async def _get_hybrid_router() -> HybridRouter:
    return await _runtime_or_raise().get_hybrid_router()


def _find_exact_match(beliefs: list, proposition: str):
    norm = proposition.strip().lower()
    for b in beliefs:
        if b.proposition.strip().lower() == norm and b.lifecycle_state == "active":
            return b
    return None


@mcp.tool()
@_runtime_admitted
def ping() -> str:
    """Connectivity check — returns OK if server is alive"""
    collector = get_collector()
    with collector.tool_call("ping") as _ctx:
        return json.dumps({"status": "ok"})


@mcp.tool(name="search")
@_runtime_admitted
async def search_tool(
    query: str = "",
    subject: str = "",
    predicate: str = "",
    limit: int = get_settings().search_default_limit,
) -> str:
    """Search stored facts by keyword text with optional filters.

    Args:
        query: Free-text keyword to search across subject, predicate, object.
        subject: Optional exact subject filter.
        predicate: Optional exact predicate filter.
        limit: Maximum number of results (default 50).
    """
    collector = get_collector()
    with collector.tool_call("search") as _ctx:
        provider = await _get_provider()
        result = await search_fn(
            provider,
            query=query,
            subject=subject if subject else None,
            predicate=predicate if predicate else None,
            limit=limit,
        )
        return json.dumps(result)


@mcp.tool(name="get_context")
@_runtime_admitted
async def get_context_tool(
    task: str,
    subject: str = "",
    max_results: int = get_settings().context_default_limit,
) -> str:
    """Retrieve structured context about a task.

    Args:
        task: The task description or search query.
        subject: Optional subject filter (pass empty string for no filter).
        max_results: Maximum number of facts to return (default 10).
    """
    collector = get_collector()
    with collector.tool_call("get_context") as _ctx:
        provider = await _get_provider()
        result = await get_context_fn(
            provider,
            task=task,
            subject=subject if subject else None,
            max_results=max_results,
        )
        return json.dumps(result)


@mcp.tool(name="remember")
@_runtime_admitted
async def remember_tool(
    subject: str,
    predicate: str,
    object: str,
    confidence: float = 1.0,
    source: str = "user",
) -> str:
    """Store a fact and generate a provenance receipt.

    Writes the fact + receipt to SQL and adds an outbox entry for
    async indexing into Qdrant (vector store) and graph. The outbox
    worker processes the entry in the background.

    Args:
        subject: The subject of the fact.
        predicate: The predicate/relation.
        object: The object of the fact.
        confidence: Confidence score 0.0-1.0 (default 1.0).
        source: Source identifier (default "user").
    """
    collector = get_collector()
    with collector.tool_call("remember") as _ctx:
        provider = await _get_provider()
        remember_kwargs = {
            "subject": subject,
            "predicate": predicate,
            "object": object,
            "confidence": confidence,
            "source": source,
        }
        try:
            graph_router = await _get_graph_router()
        except RuntimeError as exc:
            if _projection_error(exc) is None:
                raise
        else:
            remember_kwargs["graph"] = graph_router.graph
        result = await remember_fn(provider, **remember_kwargs)

        # Serialize Pydantic models in result
        fact = result["fact"]
        serialized = {
            "receipt": result["receipt"].model_dump(mode="json"),
            "fact": fact.model_dump(mode="json"),
        }

        return json.dumps(serialized)


@mcp.tool(name="learn")
@_runtime_admitted
async def learn_tool(
    text: str,
    source: str = "user",
    extract_beliefs: bool = False,
    min_belief_confidence: float = get_settings().min_belief_confidence,
) -> str:
    """Extract and store facts, decisions, skills, and optionally beliefs from text.

    Runs all three extractors (FactExtractor, DecisionExtractor, SkillExtractor)
    on the input text, stores extracted items in SQLite, and adds outbox
    entries for async indexing into Qdrant + graph.

    When extract_beliefs=True, also runs BeliefExtractor after the main
    transaction and creates or reinforces beliefs with evidence linked
    to extracted facts.

    Args:
        text: Natural language text to extract knowledge from.
        source: Optional source identifier (default "user").
        extract_beliefs: If True, also extract and store beliefs (default False).
        min_belief_confidence: Minimum confidence to create a belief (default 0.6).
    """
    collector = get_collector()
    with collector.tool_call("learn") as _ctx:
        provider = await _get_provider()
        result = await learn_fn(
            provider=provider,
            text=text,
            source=source,
            extract_beliefs=extract_beliefs,
            min_belief_confidence=min_belief_confidence,
        )

        return json.dumps(result)


def _serialize_route_result(result: dict) -> str:
    """Serialize a route/semantic-search result dict, converting RankResult objects to dicts.

    HybridRouter.route() returns ``all_results`` as ``list[RankResult]`` (dataclass
    instances) alongside the already-serialized ``ranked_results`` (``list[dict]``).
    Python's JSON encoder cannot serialize dataclasses, so we convert
    ``all_results`` to plain dicts before calling ``json.dumps``.
    """
    if isinstance(result, dict) and "all_results" in result:
        result = dict(result)  # shallow copy — don't mutate caller's dict
        result["all_results"] = [r.__dict__ for r in result["all_results"]]
    return json.dumps(result)


@mcp.tool(name="semantic_search")
@_stable_projection_errors
@_runtime_admitted
async def semantic_search_tool(
    query: str = "",
    top_k: int = get_settings().semantic_top_k,
    score_threshold: float = get_settings().semantic_score_threshold,
) -> str:
    """Semantic search — embed a query and find similar facts via vector similarity.

    First checks routing rules (keyword-based exact matches). If a rule matches,
    the result indicates which route should handle the query. Otherwise, returns
    semantically ranked results with similarity scores.

    Args:
        query: Natural language query text.
        top_k: Maximum number of results (default 10).
        score_threshold: Minimum similarity score 0.0-1.0 (default 0.0).
    """
    collector = get_collector()
    with collector.tool_call("semantic_search") as _ctx:
        router = await _get_router()
        results = await router.route(
            query=query,
            top_k=top_k,
            score_threshold=score_threshold,
        )
        return _serialize_route_result(results)


@mcp.tool(name="graph_search")
@_stable_projection_errors
@_runtime_admitted
async def graph_search_fn(
    query: str = "",
    entity_id: str = "",
    source_id: str = "",
    target_id: str = "",
    relation_type: str = "",
) -> str:
    """Search the knowledge graph for entities and relations.

    Performs one of three search modes depending on parameters:
    1. query: Extract entity references from text and find neighbors.
    2. entity_id: Direct node lookup by ID.
    3. source_id + target_id: Pathfinding between two entities.
    4. relation_type: Return all edges for a typed relation.

    Args:
        query: Text to extract entity references from.
        entity_id: Direct node ID lookup.
        source_id: Source entity for pathfinding.
        target_id: Target entity for pathfinding.
        relation_type: Relation name to search by.
    """
    collector = get_collector()
    with collector.tool_call("graph_search") as _ctx:
        router = await _get_graph_router()
        graph = router.graph

        nodes: list[dict] = []
        edges: list[dict] = []
        paths: list[list[dict]] = []

        if relation_type:
            normalized_relation = relation_type.strip().lower()
            matched_edges = graph.search_by_relation(normalized_relation)
            seen_nodes: dict[str, dict] = {}
            for edge in matched_edges:
                source_node = graph.get_node(edge.source_id)
                target_node = graph.get_node(edge.target_id)
                if source_node is not None:
                    seen_nodes[source_node.id] = {
                        "id": source_node.id,
                        "name": source_node.name,
                        "type": source_node.type,
                        "attributes": source_node.attributes,
                    }
                if target_node is not None:
                    seen_nodes[target_node.id] = {
                        "id": target_node.id,
                        "name": target_node.name,
                        "type": target_node.type,
                        "attributes": target_node.attributes,
                    }
                edges.append({
                    "source_id": edge.source_id,
                    "source_name": source_node.name if source_node is not None else edge.source_id,
                    "target_id": edge.target_id,
                    "target_name": target_node.name if target_node is not None else edge.target_id,
                    "relation": edge.relation,
                    "attributes": edge.attributes,
                })
            nodes = list(seen_nodes.values())
        elif entity_id:
            node = graph.get_node(entity_id)
            if node is not None:
                nodes.append({
                    "id": node.id,
                    "name": node.name,
                    "type": node.type,
                    "attributes": node.attributes,
                })
                neighbors = graph.get_neighbors(entity_id)
                for neighbor_node, edge in neighbors:
                    nodes.append({
                        "id": neighbor_node.id,
                        "name": neighbor_node.name,
                        "type": neighbor_node.type,
                        "attributes": neighbor_node.attributes,
                    })
                    edges.append({
                        "source_id": edge.source_id,
                        "target_id": edge.target_id,
                        "relation": edge.relation,
                        "attributes": edge.attributes,
                    })
        elif source_id and target_id:
            found_paths = graph.find_path(
                source_id,
                target_id,
                max_depth=_get_storage_settings().graph_max_path_depth,
            )
            for p in found_paths:
                paths.append([
                    {"id": n.id, "name": n.name, "type": n.type}
                    for n in p
                ])
        elif query:
            result = router.query(query)
            nodes = result.get("entities", [])
            edges = result.get("relations", [])
            paths = result.get("paths", [])

        return json.dumps({
            "nodes": nodes,
            "edges": edges,
            "paths": paths,
        })


@mcp.tool(name="add_relation")
@_stable_projection_errors
@_runtime_admitted
async def add_relation_tool(
    source_id: str,
    target_id: str,
    relation_type: str,
) -> str:
    """Create a typed inter-claim relation between two graph nodes."""
    collector = get_collector()
    with collector.tool_call("add_relation") as _ctx:
        router = await _get_graph_router()
        provider = await _get_provider()
        result = await add_relation_fn(
            router.graph,
            source_id=source_id,
            target_id=target_id,
            relation_type=relation_type,
            provider=provider,
        )
        return json.dumps(result)


@mcp.tool(name="invalidate")
@_stable_projection_errors
@_runtime_admitted
async def invalidate_tool(
    memory_id: str,
    memory_type: str,
    reason: str,
    expected_version: str = "",
) -> str:
    """Invalidate a fact or belief and demote dependent confidence."""
    collector = get_collector()
    with collector.tool_call("invalidate") as _ctx:
        lifecycle_service = await _get_lifecycle_service()
        result = await lifecycle_service.transition(LifecycleCommand(
            memory_id=memory_id,
            memory_type=memory_type,
            to_state="discarded",
            reason=reason,
            triggered_by="system",
            expected_version=expected_version or None,
        ))
        return json.dumps(result)


@mcp.tool(name="route")
@_stable_projection_errors
@_runtime_admitted
async def route_tool(
    query: str = "",
    top_k: int = get_settings().semantic_top_k,
    score_threshold: float = get_settings().semantic_score_threshold,
) -> str:
    """Route a query through the 4-stage hybrid router (rules -> embeddings -> graph -> LLM).

    Per ADR-005, evaluates each stage in priority order and returns the
    result from the highest-priority stage that produces meaningful output.

    Args:
        query: Natural language query text.
        top_k: Maximum semantic search results (default 10).
        score_threshold: Minimum similarity score 0.0-1.0 (default 0.0).
    """
    collector = get_collector()
    with collector.tool_call("route") as _ctx:
        router = await _get_hybrid_router()
        result = await router.route(
            query=query,
            top_k=top_k,
            score_threshold=score_threshold,
        )
        return _serialize_route_result(result)


@mcp.tool(name="audit")
@_stable_projection_errors
@_runtime_admitted
async def audit_tool(
    audit_type: str = "full",
) -> str:
    """Run a memory audit for consistency, orphans, confidence distribution.

    Args:
        audit_type: One of "full" (default), "consistency", "orphans", "confidence".

    Returns:
        Structured audit report with warnings, errors, and stats.
    """
    collector = get_collector()
    with collector.tool_call("audit") as _ctx:
        auditor = await _build_auditor()
        report = await auditor.audit(AuditCommand(audit_type=audit_type))

        # Integrate drift detection into metrics
        drift_stats = report.get("stats", {}).get("sql_vector_drift", {})
        if drift_stats and drift_stats.get("drift_pct") is not None:
            collector.update_drift(drift_stats["drift_pct"])

        return json.dumps(report)


@mcp.tool(name="metrics")
@_runtime_admitted
async def metrics_tool() -> str:
    """Return a Prometheus-formatted snapshot of all observability metrics."""
    from memory_server.evaluation.metrics import generate_latest

    return generate_latest().decode("utf-8")


@mcp.tool(name="set_belief")
@_stable_projection_errors
@_runtime_admitted
async def set_belief_tool(
    proposition: str,
    confidence: float = 0.5,
    sources: str = "[]",
    tags: str = "[]",
    source: str = "system",
    replace_belief_id: str = "",
) -> str:
    """Create, reinforce, or supersede a belief proposition with evidence.

    Args:
        proposition: The belief proposition text.
        confidence: Confidence score 0.0-1.0 (default 0.5).
        sources: JSON array of evidence source dicts with source_type, source_id, weight.
        tags: JSON array of tag strings.
        source: Source identifier (default "system").
        replace_belief_id: If set, supersede the referenced belief and link this new one.
    """
    from memory_server.models.evidence import Evidence
    from memory_server.services.lifecycle_service import LifecycleTransitionRequest

    collector = get_collector()
    with collector.tool_call("set_belief") as _ctx:
        provider = await _get_provider()

        # Parse JSON params
        parsed_sources = json.loads(sources) if sources else []
        parsed_tags = json.loads(tags) if tags else []

        async with await provider._get_session() as session:
            reinforced = await reinforce_belief_item(
                session,
                proposition=proposition,
                new_confidence=confidence,
                source=source,
                parsed_sources=parsed_sources,
                parsed_tags=parsed_tags,
            )
            if reinforced is not None:
                belief, receipt = reinforced
                await session.commit()
                serialized = {
                    "belief": belief.model_dump(mode="json"),
                    "receipt": receipt.model_dump(mode="json"),
                    "superseded": None,
                    "reinforced": True,
                }
                return json.dumps(serialized)

        # Create the belief
        belief = Belief(
            proposition=proposition,
            confidence=confidence,
            source=source,
            tags=parsed_tags,
            creator=source,
        )

        # Create evidence entries
        evidence_list = []
        for s in parsed_sources:
            ev = Evidence(
                belief_id=belief.id,
                source_type=s.get("source_type", "observation"),
                source_id=s.get("source_id", ""),
                weight=s.get("weight", 0.5),
                contributor=source,
            )
            evidence_list.append(ev)

        superseded = None
        old_belief = None
        graph = await _get_graph()
        lifecycle_service = LifecycleService(provider)
        if replace_belief_id:
            old_belief = await provider.get_belief(replace_belief_id)
            if old_belief:
                superseded = old_belief.model_dump(mode="json")
                # Preserve the historical pattern that the replacement is one
                # revision ahead of the superseded belief.
                belief.version = old_belief.version + 1

        # Create the belief with evidence
        from memory_server.models.receipt import MemoryReceipt
        receipt = MemoryReceipt(
            id=belief.id,
            memory_type="belief",
            source=source,
            created_by=source,
            timestamp=datetime.now(timezone.utc),
            confidence=confidence,
        )

        async def _transaction_callback(session) -> None:
            if not replace_belief_id or old_belief is None:
                return
            await lifecycle_service.transition_in_session(
                session,
                LifecycleTransitionRequest(
                    memory_id=replace_belief_id,
                    memory_type="belief",
                    to_state="superseded",
                    reason=f"Replaced by {belief.id}",
                    triggered_by=source,
                    expected_version=old_belief.version,
                ),
                graph=graph,
            )

        await provider.create_in_transaction(
            belief=belief,
            evidence_list=evidence_list,
            receipt=receipt,
            outbox_entries=[
                {
                    "record_type": "belief",
                    "record_id": belief.id,
                    "operation": "index_belief",
                    "payload": {
                        "proposition": proposition,
                        "tags": parsed_tags,
                        "confidence": confidence,
                        "source": source,
                    },
                }
            ],
            transaction_callback=_transaction_callback if old_belief is not None else None,
        )

        serialized = {
            "belief": belief.model_dump(mode="json"),
            "receipt": receipt.model_dump(mode="json"),
            "superseded": superseded,
        }

        return json.dumps(serialized)


@mcp.tool(name="get_belief")
@_runtime_admitted
async def get_belief_tool(
    proposition: str = "",
    lifecycle_state: str = "active",
    min_confidence: float = 0.0,
    tags: str = "",
    source: str = "",
    creator: str = "",
    source_id: str = "",
    limit: int = get_settings().belief_search_limit,
) -> str:
    """Search beliefs with optional filters.

    Args:
        proposition: Search proposition text (FTS5 full-text search).
        lifecycle_state: Filter by lifecycle state (default "active").
        min_confidence: Minimum confidence threshold 0.0-1.0.
        tags: JSON array of tag strings to filter by.
        source: Filter by source identifier.
        creator: Filter by creator identifier.
        source_id: Filter by source_id in the belief's source_ids list.
        limit: Maximum number of results (default 10, max 100).
    """
    collector = get_collector()
    with collector.tool_call("get_belief") as _ctx:
        provider = await _get_provider()

        parsed_tags = json.loads(tags) if tags else None

        results = await provider.search_beliefs(
            proposition=proposition or None,
            lifecycle_state=lifecycle_state or None,
            min_confidence=min_confidence if min_confidence > 0 else None,
            tags=parsed_tags,
            source=source or None,
            creator=creator or None,
            source_id=source_id or None,
            limit=min(limit, 100),
        )

        serialized = {
            "total": len(results),
            "beliefs": [b.model_dump(mode="json") for b in results],
            "query": {
                "proposition": proposition,
                "lifecycle_state": lifecycle_state,
                "min_confidence": min_confidence,
                "tags": parsed_tags,
                "source": source,
                "creator": creator,
                "source_id": source_id,
                "limit": limit,
            },
        }

        return json.dumps(serialized)


@mcp.tool(name="resolve_conflict")
@_stable_projection_errors
@_runtime_admitted
async def resolve_conflict_tool(
    belief_a_id: str,
    belief_b_id: str,
    resolution: str,
    new_proposition: str = "",
    auto_resolve: bool = False,
) -> str:
    """Resolve a conflict between two beliefs using a transition matrix.

    Args:
        belief_a_id: UUID of the first belief in the conflict.
        belief_b_id: UUID of the second belief in the conflict.
        resolution: Strategy: keep_a, keep_b, merge, discard_both.
        new_proposition: Proposition for a new merged belief (required for merge).
        auto_resolve: When True, auto-resolve by confidence threshold
                      (never uses 'discarded' state).
    """
    from memory_server.models.evidence import Evidence
    from memory_server.models.receipt import MemoryReceipt
    from memory_server.services.lifecycle_service import LifecycleTransitionRequest

    collector = get_collector()
    with collector.tool_call("resolve_conflict") as _ctx:
        provider = await _get_provider()
        graph = await _get_graph()
        lifecycle_service = LifecycleService(provider)

        # Fetch both beliefs
        belief_a = await provider.get_belief(belief_a_id)
        belief_b = await provider.get_belief(belief_b_id)
        if belief_a is None or belief_b is None:
            raise ValueError("Both belief_a and belief_b must exist in the store")

        # Auto-resolution path (never uses "discarded")
        if auto_resolve:
            confidence_diff = abs(belief_a.confidence - belief_b.confidence)
            if confidence_diff > 0.5:
                if belief_a.confidence < belief_b.confidence:
                    lower_id, higher_conf = belief_a_id, belief_b.confidence
                    expected_version = belief_a.version
                else:
                    lower_id, higher_conf = belief_b_id, belief_a.confidence
                    expected_version = belief_b.version

                transition_requests = [
                    LifecycleTransitionRequest(
                        memory_id=lower_id,
                        memory_type="belief",
                        to_state="superseded",
                        reason=(
                            f"Auto-resolved — confidence gap "
                            f"({higher_conf:.2f} vs "
                            f"{(belief_a.confidence if lower_id == belief_b_id else belief_b.confidence):.2f})"
                        ),
                        triggered_by="system",
                        expected_version=expected_version,
                    )
                ]
                transition_requests[0].reason
            else:
                transition_requests = [
                    LifecycleTransitionRequest(
                        memory_id=belief_a_id,
                        memory_type="belief",
                        to_state="contradicted",
                        reason=(
                            "Auto-resolved — needs manual review "
                            "(confidences too close or both low)"
                        ),
                        triggered_by="system",
                        expected_version=belief_a.version,
                    ),
                    LifecycleTransitionRequest(
                        memory_id=belief_b_id,
                        memory_type="belief",
                        to_state="contradicted",
                        reason=(
                            "Auto-resolved — needs manual review "
                            "(confidences too close or both low)"
                        ),
                        triggered_by="system",
                        expected_version=belief_b.version,
                    ),
                ]
                transition_requests[0].reason

            results = await lifecycle_service.transition_many(transition_requests, graph=graph)
            events = [
                {
                    "belief_id": result.event["memory_id"],
                    "from_state": result.from_state,
                    "to_state": result.to_state,
                    "reason": result.event["reason"],
                }
                for result in results
            ]

            receipt = MemoryReceipt(
                id=str(uuid.uuid4()),
                memory_type="belief",
                source="conflict_resolution",
                created_by="system",
                timestamp=datetime.now(timezone.utc),
            )

            serialized = {
                "belief_a": (await provider.get_belief(belief_a_id)).model_dump(mode="json"),
                "belief_b": (await provider.get_belief(belief_b_id)).model_dump(mode="json"),
                "resolution": resolution,
                "auto_resolved": True,
                "created": None,
                "events": events,
                "receipt": receipt.model_dump(mode="json"),
            }

            return json.dumps(serialized)

        # Manual resolution path.
        events = []
        merge_events: list[dict[str, str]] | None = None
        merged = None
        transition_requests: list[LifecycleTransitionRequest] = []

        if resolution == "keep_a":
            transition_requests = [
                LifecycleTransitionRequest(
                    memory_id=belief_b_id,
                    memory_type="belief",
                    to_state="discarded",
                    reason=f"Discarded in favor of {belief_a_id} via conflict resolution",
                    triggered_by="system",
                    expected_version=belief_b.version,
                )
            ]
        elif resolution == "keep_b":
            transition_requests = [
                LifecycleTransitionRequest(
                    memory_id=belief_a_id,
                    memory_type="belief",
                    to_state="discarded",
                    reason=f"Discarded in favor of {belief_b_id} via conflict resolution",
                    triggered_by="system",
                    expected_version=belief_a.version,
                )
            ]
        elif resolution == "merge":
            if not new_proposition:
                raise ValueError("new_proposition is required when resolution='merge'")
            merged = Belief(
                proposition=new_proposition,
                confidence=min(1.0, (belief_a.confidence + belief_b.confidence) / 2),
                source="conflict_resolution",
                creator="system",
                tags=list(set(belief_a.tags + belief_b.tags)),
                source_ids=list(set(belief_a.source_ids + belief_b.source_ids)),
            )

            copied_evidence = []
            try:
                from sqlalchemy.ext.asyncio import AsyncSession as AsyncSessionAlias
                from storage.repositories import EvidenceRepository

                async with AsyncSessionAlias(provider.engine) as ev_session:
                    ev_repo = EvidenceRepository(ev_session)
                    for orig_id in (belief_a_id, belief_b_id):
                        ev_rows = await ev_repo.get_by_belief_id(orig_id)
                        for ev in ev_rows:
                            copied_evidence.append(
                                Evidence(
                                    belief_id=merged.id,
                                    source_type=ev.source_type,
                                    source_id=ev.source_id,
                                    weight=ev.weight,
                                    contributor=ev.contributor,
                                )
                            )
            except Exception:
                logger.warning(
                    "Failed to copy evidence for merged belief %s",
                    merged.id,
                    exc_info=True,
                )

            merged_receipt = MemoryReceipt(
                id=merged.id,
                memory_type="belief",
                source="conflict_resolution",
                created_by="system",
                timestamp=datetime.now(timezone.utc),
                confidence=merged.confidence,
            )

            async def _merge_callback(session) -> None:
                await lifecycle_service.transition_many_in_session(
                    session,
                    [
                        LifecycleTransitionRequest(
                            memory_id=belief_a_id,
                            memory_type="belief",
                            to_state="superseded",
                            reason=f"Merged into {merged.id} via conflict resolution",
                            triggered_by="system",
                            expected_version=belief_a.version,
                        ),
                        LifecycleTransitionRequest(
                            memory_id=belief_b_id,
                            memory_type="belief",
                            to_state="superseded",
                            reason=f"Merged into {merged.id} via conflict resolution",
                            triggered_by="system",
                            expected_version=belief_b.version,
                        ),
                    ],
                    graph=graph,
                )

            await provider.create_in_transaction(
                belief=merged,
                evidence_list=copied_evidence,
                receipt=merged_receipt,
                outbox_entries=[
                    {
                        "record_type": "belief",
                        "record_id": merged.id,
                        "operation": "index_belief",
                        "payload": {
                            "proposition": merged.proposition,
                            "tags": merged.tags,
                            "confidence": merged.confidence,
                            "source": "conflict_resolution",
                        },
                    }
                ],
                transaction_callback=_merge_callback,
            )
            transition_requests = [
                LifecycleTransitionRequest(
                    memory_id=belief_a_id,
                    memory_type="belief",
                    to_state="superseded",
                    reason=f"Merged into {merged.id} via conflict resolution",
                    triggered_by="system",
                    expected_version=belief_a.version,
                ),
                LifecycleTransitionRequest(
                    memory_id=belief_b_id,
                    memory_type="belief",
                    to_state="superseded",
                    reason=f"Merged into {merged.id} via conflict resolution",
                    triggered_by="system",
                    expected_version=belief_b.version,
                ),
            ]
            merge_events = [
                {
                    "belief_id": request.memory_id,
                    "from_state": "active",
                    "to_state": "superseded",
                    "reason": request.reason,
                }
                for request in transition_requests
            ]
        elif resolution == "discard_both":
            transition_requests = [
                LifecycleTransitionRequest(
                    memory_id=belief_a_id,
                    memory_type="belief",
                    to_state="discarded",
                    reason="Discarded via conflict resolution",
                    triggered_by="system",
                    expected_version=belief_a.version,
                ),
                LifecycleTransitionRequest(
                    memory_id=belief_b_id,
                    memory_type="belief",
                    to_state="discarded",
                    reason="Discarded via conflict resolution",
                    triggered_by="system",
                    expected_version=belief_b.version,
                ),
            ]
        else:
            raise ValueError(
                f"Unknown resolution: {resolution}. "
                "Must be one of: keep_a, keep_b, merge, discard_both"
            )

        if resolution == "merge" and merged is not None:
            events = merge_events or []
        else:
            results = await lifecycle_service.transition_many(transition_requests, graph=graph)
            events = [
                {
                    "belief_id": result.event["memory_id"],
                    "from_state": result.from_state,
                    "to_state": result.to_state,
                    "reason": result.event["reason"],
                }
                for result in results
            ]

        # Create receipt with new UUID
        receipt = MemoryReceipt(
            id=str(uuid.uuid4()),
            memory_type="belief",
            source="conflict_resolution",
            created_by="system",
            timestamp=datetime.now(timezone.utc),
        )

        serialized = {
            "belief_a": (await provider.get_belief(belief_a_id)).model_dump(mode="json"),
            "belief_b": (await provider.get_belief(belief_b_id)).model_dump(mode="json"),
            "resolution": resolution,
            "created": merged.model_dump(mode="json") if resolution == "merge" and merged else None,
            "events": events,
            "receipt": receipt.model_dump(mode="json"),
        }

        return json.dumps(serialized)


@mcp.tool(name="reflect")
@_runtime_admitted
async def reflect_tool(
    mode: str = "overview",
    topic: str = "",
    min_confidence: float = get_settings().reflect_min_confidence,
    limit: int = get_settings().reflect_limit,
) -> str:
    """Analyse the belief store and produce structured insights.

    Provides 6 analysis modes:
    - overview: High-level summary of the belief store (counts, confidence, states)
    - contradictions: Find beliefs with keyword-based semantic conflicts
    - decay: Analyse which beliefs are approaching lifecycle transitions
    - topics: Cluster beliefs by tags with counts and avg confidence
    - evidence_audit: Audit evidence quality across beliefs
    - confidence: Detailed confidence histogram with sorted belief list

    Args:
        mode: Analysis mode (overview, contradictions, decay, topics, evidence_audit, confidence).
        topic: Optional topic/tag filter.
        min_confidence: Minimum confidence threshold 0.0-1.0.
        limit: Max beliefs to analyse (0 = all).
    """
    from memory_server.api.reflect import ReflectEngine

    collector = get_collector()
    with collector.tool_call("reflect") as _ctx:
        provider = await _get_provider()

        valid_modes = {
            "overview", "contradictions", "decay", "topics",
            "evidence_audit", "confidence",
        }
        if mode not in valid_modes:
            return json.dumps({
                "error": f"Invalid mode '{mode}'. Must be one of: {', '.join(sorted(valid_modes))}",
            })

        engine = ReflectEngine(provider)
        topic_param = topic if topic else None

        method_map = {
            "overview": engine.overview,
            "contradictions": engine.contradictions,
            "decay": engine.decay_analysis,
            "topics": engine.topics,
            "evidence_audit": engine.evidence_audit,
            "confidence": engine.confidence_histogram,
        }

        method = method_map[mode]
        result = await method(
            topic=topic_param,
            min_confidence=min_confidence,
            limit=limit,
        )
        return json.dumps(result)


def run():
    mcp.run(transport="stdio")
