"""MCP server entry point with tool registrations.

v0.6 Phase 4: Uses transactional outbox pattern for async indexing.
- remember() / learn() write to SQL + outbox in same transaction
- Outbox worker polls pending entries and pushes to Qdrant + graph
- Failed entries are retried 3 times, then marked as failed
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from mcp.server.fastmcp import FastMCP
from storage.outbox_worker import OutboxWorker
from storage.repositories import LifecycleRepository

from memory_server.api.get_context import get_context as get_context_fn
from memory_server.api.learn import learn as learn_fn
from memory_server.api.remember import remember as remember_fn
from memory_server.api.search import search as search_fn
from memory_server.evaluation.auditor import MemoryAuditor
from memory_server.evaluation.confidence import ConfidenceEngine
from memory_server.evaluation.decay import DecayEngine
from memory_server.evaluation.metrics import get_collector
from memory_server.evaluation.validator import Validator
from memory_server.models import Belief
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.router.graph_router import GraphRouter

if TYPE_CHECKING:
    from memory_server.providers.embedding_provider import SentenceTransformerEmbeddingProvider
    from memory_server.providers.lancedb_provider import LanceDBProvider
    from memory_server.providers.qdrant_provider import QdrantProvider
    from memory_server.router.embedding_router import EmbeddingRouter
    from memory_server.router.hybrid_router import HybridRouter

logger = logging.getLogger(__name__)

# Lazy providers — initialized on first use
_provider: SQLiteProvider | None = None
_qdrant: QdrantProvider | None = None
_lancedb: LanceDBProvider | None = None
_embedder: SentenceTransformerEmbeddingProvider | None = None
_router: EmbeddingRouter | None = None
_graph: SimpleGraph | None = None
_graph_router: GraphRouter | None = None
_hybrid_router: HybridRouter | None = None
_validator_store: Validator | None = None
_confidence_engine: ConfidenceEngine | None = None
_decay_engine: DecayEngine | None = None
_outbox_worker: OutboxWorker | None = None
_outbox_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(server: FastMCP):
    """FastMCP lifespan: start providers + outbox worker on boot, stop gracefully on shutdown."""
    global _outbox_task, _outbox_worker, _provider
    # --- Startup ---
    logger.info("Starting Composite Memory MCP Server...")
    provider = await _get_provider()
    worker = await _get_outbox_worker()

    # Start background polling task (only in lifespan — tests use process_all_pending directly)
    if _outbox_task is None or _outbox_task.done():
        _outbox_task = asyncio.create_task(worker.run())
        logger.info("Outbox worker background task started")

    logger.info("Server initialized — provider ready, outbox worker started")

    yield {"provider": provider, "outbox_worker": worker}

    # --- Shutdown ---
    logger.info("Shutting down Composite Memory MCP Server...")

    if _outbox_task and not _outbox_task.done():
        _outbox_task.cancel()
        try:
            await _outbox_task
        except asyncio.CancelledError:
            pass
        logger.info("Outbox worker task stopped")

    if _outbox_worker:
        await _outbox_worker.close()
        logger.info("Outbox worker connection closed")

    if _provider:
        await _provider.close()
        logger.info("SQLite provider connection closed")


mcp = FastMCP("CompositeMemoryServer", lifespan=lifespan)


async def _get_provider() -> SQLiteProvider:
    """Get or create the SQLiteProvider singleton."""
    global _provider
    if _provider is None:
        _provider = SQLiteProvider(url=_get_sqlite_db_url())
        await _provider.initialize()
    return _provider


def _get_sqlite_db_url() -> str:
    """Return the server SQLite URL and ensure file-backed parent dirs exist."""
    db_url = os.environ.get("MEMORY_SERVER_DB_URL", "sqlite+aiosqlite:///data/memory.db")
    prefix = "sqlite+aiosqlite:///"
    if db_url.startswith(prefix):
        db_path = db_url.removeprefix(prefix)
        if db_path and db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return db_url


async def _get_lancedb_provider() -> LanceDBProvider:
    """Get or create the LanceDBProvider singleton."""
    global _lancedb
    if _lancedb is None:
        from memory_server.providers.lancedb_provider import LanceDBProvider

        _lancedb = LanceDBProvider(db_path="data/lancedb", table="memories")
    return _lancedb


async def _get_qdrant_provider() -> QdrantProvider:
    """Get or create the QdrantProvider singleton (optional server-mode backend)."""
    global _qdrant
    if _qdrant is None:
        from memory_server.providers.qdrant_provider import QdrantProvider

        _qdrant = QdrantProvider(location=":memory:", prefer_grpc=False)
    return _qdrant


def _get_vector_provider():
    """Get the active vector provider — LanceDB by default, Qdrant if configured.

    Controlled by MEMORY_VECTOR_BACKEND env var: 'lancedb' (default) or 'qdrant'.
    """
    import os
    backend = os.environ.get("MEMORY_VECTOR_BACKEND", "lancedb").lower()
    if backend == "qdrant":
        return _get_qdrant_provider()
    return _get_lancedb_provider()


async def _get_router() -> EmbeddingRouter:
    """Get or create the EmbeddingRouter singleton."""
    global _qdrant, _lancedb, _embedder, _router
    if _router is None:
        from memory_server.router.embedding_router import EmbeddingRouter

        vector_provider = await _get_vector_provider()
        if _embedder is None:
            from memory_server.providers.embedding_provider import SentenceTransformerEmbeddingProvider

            _embedder = SentenceTransformerEmbeddingProvider()
        _router = EmbeddingRouter(
            vector_provider=vector_provider,
            embedder=_embedder,
        )
    return _router


async def _get_outbox_worker() -> OutboxWorker:
    """Get or create the OutboxWorker singleton.

    Starts the worker as a background asyncio task on first access.
    The worker polls the outbox table and processes pending entries.
    """
    global _outbox_worker, _outbox_task, _embedder
    if _outbox_worker is None:
        provider = await _get_provider()
        _chart_router = await _get_graph_router()

        # Resolve the active vector provider (LanceDB by default) and
        # embedder so the outbox worker actually writes vectors instead
        # of silently no-oping because both were module-level None.
        vector_provider = await _get_vector_provider()
        if _embedder is None:
            from memory_server.providers.embedding_provider import (
                SentenceTransformerEmbeddingProvider,
            )
            _embedder = SentenceTransformerEmbeddingProvider()

        _outbox_worker = OutboxWorker(
            engine=provider.engine,
            qdrant=vector_provider,
            embedder=_embedder,
            graph_router=_chart_router,
        )
        await _outbox_worker.initialize()
    return _outbox_worker


def _find_exact_match(beliefs: list, proposition: str):
    norm = proposition.strip().lower()
    for b in beliefs:
        if b.proposition.strip().lower() == norm and b.lifecycle_state == "active":
            return b
    return None


@mcp.tool()
def ping() -> str:
    """Connectivity check — returns OK if server is alive"""
    collector = get_collector()
    with collector.tool_call("ping") as _ctx:
        return json.dumps({"status": "ok"})


@mcp.tool(name="search")
async def search_tool(
    query: str = "",
    subject: str = "",
    predicate: str = "",
    limit: int = 50,
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
async def get_context_tool(task: str, subject: str = "", max_results: int = 10) -> str:
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
        result = await remember_fn(
            provider,
            subject=subject,
            predicate=predicate,
            object=object,
            confidence=confidence,
            source=source,
        )

        # Serialize Pydantic models in result
        fact = result["fact"]
        serialized = {
            "receipt": result["receipt"].model_dump(mode="json"),
            "fact": fact.model_dump(mode="json"),
        }

        return json.dumps(serialized)


@mcp.tool(name="learn")
async def learn_tool(
    text: str,
    source: str = "user",
    extract_beliefs: bool = False,
    min_belief_confidence: float = 0.6,
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
async def semantic_search_tool(
    query: str = "",
    top_k: int = 10,
    score_threshold: float = 0.0,
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


async def _get_graph_router() -> GraphRouter:
    """Get or create the GraphRouter singleton."""
    global _graph, _graph_router
    if _graph_router is None:
        _graph = SimpleGraph()
        _graph_router = GraphRouter(graph=_graph)
    return _graph_router


@mcp.tool(name="graph_search")
async def graph_search_fn(
    query: str = "",
    entity_id: str = "",
    source_id: str = "",
    target_id: str = "",
) -> str:
    """Search the knowledge graph for entities and relations.

    Performs one of three search modes depending on parameters:
    1. query: Extract entity references from text and find neighbors.
    2. entity_id: Direct node lookup by ID.
    3. source_id + target_id: Pathfinding between two entities.

    Args:
        query: Text to extract entity references from.
        entity_id: Direct node ID lookup.
        source_id: Source entity for pathfinding.
        target_id: Target entity for pathfinding.
    """
    collector = get_collector()
    with collector.tool_call("graph_search") as _ctx:
        router = await _get_graph_router()
        graph = router.graph

        nodes: list[dict] = []
        edges: list[dict] = []
        paths: list[list[dict]] = []

        if entity_id:
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
            found_paths = graph.find_path(source_id, target_id, max_depth=4)
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


async def _get_hybrid_router() -> HybridRouter:
    """Get or create the HybridRouter singleton."""
    global _qdrant, _lancedb, _embedder, _graph, _hybrid_router
    if _hybrid_router is None:
        from memory_server.router.hybrid_router import HybridRouter

        vector_provider = await _get_vector_provider()
        if _embedder is None:
            from memory_server.providers.embedding_provider import SentenceTransformerEmbeddingProvider

            _embedder = SentenceTransformerEmbeddingProvider()
        if _graph is None:
            _graph = SimpleGraph()
        _hybrid_router = HybridRouter(
            vector_provider=vector_provider,
            embedder=_embedder,
            graph=_graph,
        )
    return _hybrid_router


@mcp.tool(name="route")
async def route_tool(
    query: str = "",
    top_k: int = 10,
    score_threshold: float = 0.0,
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
async def audit_tool(
    audit_type: str = "full",
) -> str:
    """Run a memory audit for consistency, orphans, confidence distribution.

    Args:
        audit_type: One of "full" (default), "consistency", "orphans", "confidence".

    Returns:
        Structured audit report with warnings, errors, and stats.
    """
    global _validator_store, _confidence_engine
    collector = get_collector()
    with collector.tool_call("audit") as _ctx:
        if _validator_store is None:
            _validator_store = Validator()
        if _confidence_engine is None:
            _confidence_engine = ConfidenceEngine()

        graph = _graph or SimpleGraph()
        auditor = MemoryAuditor(
            validator=_validator_store,
            confidence_engine=_confidence_engine,
            graph=graph,
        )
        report = auditor.audit_report(audit_type=audit_type)

        # Integrate drift detection into metrics
        drift_stats = report.get("stats", {}).get("sql_vector_drift", {})
        if drift_stats and drift_stats.get("drift_pct") is not None:
            collector.update_drift(drift_stats["drift_pct"])

        return json.dumps(report)


@mcp.tool(name="metrics")
async def metrics_tool() -> str:
    """Return a Prometheus-formatted snapshot of all observability metrics."""
    from memory_server.evaluation.metrics import generate_latest

    return generate_latest().decode("utf-8")


@mcp.tool(name="set_belief")
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

    collector = get_collector()
    with collector.tool_call("set_belief") as _ctx:
        provider = await _get_provider()

        # Parse JSON params
        parsed_sources = json.loads(sources) if sources else []
        parsed_tags = json.loads(tags) if tags else []

        # Reinforcement: check for existing active belief with same proposition
        existing = await provider.search_beliefs(
            proposition=proposition,
            lifecycle_state=None,
            limit=100,
        )
        match = _find_exact_match(existing, proposition)
        if match:
            # Weighted average confidence
            new_confidence = max(0.0, min(1.0, (match.confidence + confidence) / 2))
            await provider.update_belief_confidence(match.id, new_confidence)
            await provider.update_belief_reinforced_at(match.id)
            from memory_server.models.receipt import MemoryReceipt
            receipt = MemoryReceipt(
                id=match.id,
                memory_type="belief",
                source=source,
                created_by=source,
                timestamp=datetime.now(timezone.utc),
                confidence=new_confidence,
            )
            serialized = {
                "belief": match.model_dump(mode="json"),
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

        # If replace_belief_id is set, supersede the old belief
        superseded = None
        if replace_belief_id:
            old_belief = await provider.get_belief(replace_belief_id)
            if old_belief:
                superseded = old_belief.model_dump(mode="json")
                await provider.update_belief_lifecycle(replace_belief_id, "superseded")
                # Bump version: new belief gets old_belief.version + 1
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
        )

        serialized = {
            "belief": belief.model_dump(mode="json"),
            "receipt": receipt.model_dump(mode="json"),
            "superseded": superseded,
        }

        return json.dumps(serialized)


@mcp.tool(name="get_belief")
async def get_belief_tool(
    proposition: str = "",
    lifecycle_state: str = "active",
    min_confidence: float = 0.0,
    tags: str = "",
    source: str = "",
    creator: str = "",
    source_id: str = "",
    limit: int = 10,
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
            limit=min(limit, 100),
        )

        # Filter by source_id if specified
        if source_id:
            logger.warning(
                "source_id filter applied in-memory (deferred to SQL in v0.7+): source_id=%s",
                source_id,
            )
            results = [b for b in results if source_id in b.source_ids]

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
    from sqlalchemy.ext.asyncio import AsyncSession

    from memory_server.models.evidence import Evidence
    from memory_server.models.receipt import MemoryReceipt

    collector = get_collector()
    with collector.tool_call("resolve_conflict") as _ctx:
        provider = await _get_provider()

        # Fetch both beliefs
        belief_a = await provider.get_belief(belief_a_id)
        belief_b = await provider.get_belief(belief_b_id)
        if belief_a is None or belief_b is None:
            raise ValueError("Both belief_a and belief_b must exist in the store")

        # Auto-resolution path (never uses "discarded")
        if auto_resolve:
            events = []
            confidence_diff = abs(belief_a.confidence - belief_b.confidence)

            async with AsyncSession(provider.engine) as lc_session:
                lifecycle_repo = LifecycleRepository(lc_session)

                if confidence_diff > 0.5:
                    # Lower-confidence belief -> superseded, higher stays active
                    if belief_a.confidence < belief_b.confidence:
                        lower_id, lower_conf = belief_a_id, belief_a.confidence
                        _higher_id, higher_conf = belief_b_id, belief_b.confidence
                        lower_state = belief_a.lifecycle_state
                    else:
                        lower_id, lower_conf = belief_b_id, belief_b.confidence
                        _higher_id, higher_conf = belief_a_id, belief_a.confidence
                        lower_state = (
                            belief_b.lifecycle_state
                            if lower_id == belief_b_id
                            else belief_a.lifecycle_state
                        )
                        # Re-fetch the correct state
                        if lower_id == belief_b_id:
                            lower_state = belief_b.lifecycle_state
                        else:
                            lower_state = belief_a.lifecycle_state

                    await provider.update_belief_lifecycle(lower_id, "superseded")
                    await lifecycle_repo.record_event(
                        memory_id=lower_id,
                        memory_type="belief",
                        from_state=lower_state,
                        to_state="superseded",
                        reason=(
                            f"Auto-resolved — confidence gap "
                            f"({higher_conf:.2f} vs {lower_conf:.2f})"
                        ),
                        triggered_by="system",
                    )
                    events.append({
                        "belief_id": lower_id,
                        "from_state": lower_state,
                        "to_state": "superseded",
                        "reason": (
                            f"Auto-resolved — confidence gap "
                            f"({higher_conf:.2f} vs {lower_conf:.2f})"
                        ),
                    })
                else:
                    # Both -> contradicted
                    for bid, bstate in ((belief_a_id, belief_a.lifecycle_state),
                                        (belief_b_id, belief_b.lifecycle_state)):
                        await provider.update_belief_lifecycle(bid, "contradicted")
                        await lifecycle_repo.record_event(
                            memory_id=bid,
                            memory_type="belief",
                            from_state=bstate,
                            to_state="contradicted",
                            reason=(
                                "Auto-resolved — needs manual review "
                                "(confidences too close or both low)"
                            ),
                            triggered_by="system",
                        )
                        events.append({
                            "belief_id": bid,
                            "from_state": bstate,
                            "to_state": "contradicted",
                            "reason": (
                                "Auto-resolved — needs manual review "
                                "(confidences too close or both low)"
                            ),
                        })

                await lc_session.commit()

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

        # Manual resolution path (existing behavior) — unchanged
        events = []
        merged = None

        # Create a session for lifecycle recording
        async with AsyncSession(provider.engine) as lc_session:
            lifecycle_repo = LifecycleRepository(lc_session)

            if resolution == "keep_a":
                # belief_a stays active, belief_b -> discarded
                await provider.update_belief_lifecycle(belief_b_id, "discarded")
                await lifecycle_repo.record_event(
                    memory_id=belief_b_id,
                    memory_type="belief",
                    from_state=belief_b.lifecycle_state,
                    to_state="discarded",
                    reason=f"Discarded in favor of {belief_a_id} via conflict resolution",
                    triggered_by="system",
                )
                events.append({
                    "belief_id": belief_b_id,
                    "from_state": belief_b.lifecycle_state,
                    "to_state": "discarded",
                    "reason": f"Discarded in favor of {belief_a_id} via conflict resolution",
                })

            elif resolution == "keep_b":
                # belief_b stays active, belief_a -> discarded
                await provider.update_belief_lifecycle(belief_a_id, "discarded")
                await lifecycle_repo.record_event(
                    memory_id=belief_a_id,
                    memory_type="belief",
                    from_state=belief_a.lifecycle_state,
                    to_state="discarded",
                    reason=f"Discarded in favor of {belief_b_id} via conflict resolution",
                    triggered_by="system",
                )
                events.append({
                    "belief_id": belief_a_id,
                    "from_state": belief_a.lifecycle_state,
                    "to_state": "discarded",
                    "reason": f"Discarded in favor of {belief_b_id} via conflict resolution",
                })

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

                # Copy evidence from both original beliefs (best-effort)
                copied_evidence = []
                try:
                    from sqlalchemy.ext.asyncio import AsyncSession as AsyncSessionAlias
                    from storage.repositories import EvidenceRepository
                    async with AsyncSessionAlias(provider.engine) as ev_session:
                        ev_repo = EvidenceRepository(ev_session)
                        for orig_id in (belief_a_id, belief_b_id):
                            ev_rows = await ev_repo.get_by_belief_id(orig_id)
                            for ev in ev_rows:
                                new_ev = Evidence(
                                    belief_id=merged.id,
                                    source_type=ev.source_type,
                                    source_id=ev.source_id,
                                    weight=ev.weight,
                                    contributor=ev.contributor,
                                )
                                copied_evidence.append(new_ev)
                except Exception:
                    logger.warning(
                        "Failed to copy evidence for merged belief %s",
                        merged.id,
                        exc_info=True,
                    )

                # Create merged belief + evidence + receipt + outbox in one transaction
                merged_receipt = MemoryReceipt(
                    id=merged.id,
                    memory_type="belief",
                    source="conflict_resolution",
                    created_by="system",
                    timestamp=datetime.now(timezone.utc),
                    confidence=merged.confidence,
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
                )

                # Transition both originals to superseded
                await provider.update_belief_lifecycle(belief_a_id, "superseded")
                await lifecycle_repo.record_event(
                    memory_id=belief_a_id,
                    memory_type="belief",
                    from_state=belief_a.lifecycle_state,
                    to_state="superseded",
                    reason=f"Merged into {merged.id} via conflict resolution",
                    triggered_by="system",
                )
                events.append({
                    "belief_id": belief_a_id,
                    "from_state": belief_a.lifecycle_state,
                    "to_state": "superseded",
                    "reason": f"Merged into {merged.id} via conflict resolution",
                })

                await provider.update_belief_lifecycle(belief_b_id, "superseded")
                await lifecycle_repo.record_event(
                    memory_id=belief_b_id,
                    memory_type="belief",
                    from_state=belief_b.lifecycle_state,
                    to_state="superseded",
                    reason=f"Merged into {merged.id} via conflict resolution",
                    triggered_by="system",
                )
                events.append({
                    "belief_id": belief_b_id,
                    "from_state": belief_b.lifecycle_state,
                    "to_state": "superseded",
                    "reason": f"Merged into {merged.id} via conflict resolution",
                })

            elif resolution == "discard_both":
                await provider.update_belief_lifecycle(belief_a_id, "discarded")
                await lifecycle_repo.record_event(
                    memory_id=belief_a_id,
                    memory_type="belief",
                    from_state=belief_a.lifecycle_state,
                    to_state="discarded",
                    reason="Discarded via conflict resolution",
                    triggered_by="system",
                )
                events.append({
                    "belief_id": belief_a_id,
                    "from_state": belief_a.lifecycle_state,
                    "to_state": "discarded",
                    "reason": "Discarded via conflict resolution",
                })

                await provider.update_belief_lifecycle(belief_b_id, "discarded")
                await lifecycle_repo.record_event(
                    memory_id=belief_b_id,
                    memory_type="belief",
                    from_state=belief_b.lifecycle_state,
                    to_state="discarded",
                    reason="Discarded via conflict resolution",
                    triggered_by="system",
                )
                events.append({
                    "belief_id": belief_b_id,
                    "from_state": belief_b.lifecycle_state,
                    "to_state": "discarded",
                    "reason": "Discarded via conflict resolution",
                })

            else:
                raise ValueError(
                    f"Unknown resolution: {resolution}. "
                    "Must be one of: keep_a, keep_b, merge, discard_both"
                )

            await lc_session.commit()

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
async def reflect_tool(
    mode: str = "overview",
    topic: str = "",
    min_confidence: float = 0.0,
    limit: int = 50,
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
