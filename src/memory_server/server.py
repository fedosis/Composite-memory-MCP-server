"""MCP server entry point with tool registrations."""

import asyncio
import json
import logging

from mcp.server.fastmcp import FastMCP

from memory_server.api.get_context import get_context as get_context_fn
from memory_server.api.learn import learn as learn_fn
from memory_server.api.remember import remember as remember_fn
from memory_server.api.search import search as search_fn
from memory_server.evaluation.auditor import MemoryAuditor
from memory_server.evaluation.confidence import ConfidenceEngine
from memory_server.evaluation.decay import DecayEngine
from memory_server.evaluation.validator import Validator
from memory_server.providers.embedding_provider import SentenceTransformerEmbeddingProvider
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.providers.qdrant_provider import QdrantProvider
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.router.embedding_router import EmbeddingRouter
from memory_server.router.graph_router import GraphRouter
from memory_server.router.hybrid_router import HybridRouter

logger = logging.getLogger(__name__)

mcp = FastMCP("CompositeMemoryServer")

# Lazy providers — initialized on first use
_provider: SQLiteProvider | None = None
_qdrant: QdrantProvider | None = None
_embedder: SentenceTransformerEmbeddingProvider | None = None
_router: EmbeddingRouter | None = None
_graph: SimpleGraph | None = None
_graph_router: GraphRouter | None = None
_hybrid_router: HybridRouter | None = None
_validator_store: Validator | None = None
_confidence_engine: ConfidenceEngine | None = None
_decay_engine: DecayEngine | None = None


async def _get_provider() -> SQLiteProvider:
    """Get or create the SQLiteProvider singleton."""
    global _provider
    if _provider is None:
        _provider = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
        await _provider.initialize()
    return _provider


async def _get_router() -> EmbeddingRouter:
    """Get or create the EmbeddingRouter singleton."""
    global _qdrant, _embedder, _router
    if _router is None:
        if _qdrant is None:
            _qdrant = QdrantProvider(location=":memory:", prefer_grpc=False)
        if _embedder is None:
            _embedder = SentenceTransformerEmbeddingProvider()
        _router = EmbeddingRouter(
            qdrant_provider=_qdrant,
            embedder=_embedder,
        )
    return _router


@mcp.tool()
def ping() -> str:
    """Connectivity check — returns OK if server is alive"""
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

    Args:
        subject: The subject of the fact.
        predicate: The predicate/relation.
        object: The object of the fact.
        confidence: Confidence score 0.0-1.0 (default 1.0).
        source: Source identifier (default "user").
    """
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

    # Auto-index into Qdrant + graph (best-effort, never crashes)
    await _auto_index_fact(
        fact_id=fact.id,
        subject=fact.subject,
        predicate=fact.predicate,
        object=fact.object,
        source=fact.source,
    )

    return json.dumps(serialized)


@mcp.tool(name="learn")
async def learn_tool(
    text: str,
    source: str = "user",
) -> str:
    """Extract and store facts, decisions, and skills from natural language text.

    Runs all three extractors (FactExtractor, DecisionExtractor, SkillExtractor)
    on the input text and stores extracted items in the memory database.

    Args:
        text: Natural language text to extract knowledge from.
        source: Optional source identifier (default "user").
    """
    provider = await _get_provider()
    result = await learn_fn(
        provider=provider,
        text=text,
        source=source,
    )

    # Auto-index all extracted items (best-effort, never crashes)
    try:
        # Facts -> Qdrant + graph
        for f in result.get("facts", []):
            item = f.get("item", {})
            await _auto_index_fact(
                fact_id=item.get("id", ""),
                subject=item.get("subject", ""),
                predicate=item.get("predicate", ""),
                object=item.get("object", ""),
                source=source,
            )

        # Decisions -> graph
        graph_router = await _get_graph_router()
        for d in result.get("decisions", []):
            item = d.get("item", {})
            try:
                graph_router.sync_decision(
                    choice=item.get("choice", ""),
                    reason=item.get("reason", ""),
                    entities=[item.get("context", "")],
                )
            except Exception:
                logger.warning(
                    "Auto-sync to graph failed for decision %s",
                    item.get("id", ""),
                    exc_info=True,
                )

        # Skills -> graph
        for s in result.get("skills", []):
            item = s.get("item", {})
            try:
                graph_router.sync_skill(
                    purpose=item.get("purpose", ""),
                    steps=item.get("steps", []),
                )
            except Exception:
                logger.warning(
                    "Auto-sync to graph failed for skill %s",
                    item.get("id", ""),
                    exc_info=True,
                )
    except Exception:
        logger.warning("Auto-indexing during learn() failed", exc_info=True)

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
    router = await _get_router()
    results = await router.route(
        query=query,
        top_k=top_k,
        score_threshold=score_threshold,
    )
    return json.dumps(results)


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
    global _qdrant, _embedder, _graph, _hybrid_router
    if _hybrid_router is None:
        if _qdrant is None:
            _qdrant = QdrantProvider(location=":memory:", prefer_grpc=False)
        if _embedder is None:
            _embedder = SentenceTransformerEmbeddingProvider()
        if _graph is None:
            _graph = SimpleGraph()
        _hybrid_router = HybridRouter(
            qdrant_provider=_qdrant,
            embedder=_embedder,
            graph=_graph,
        )
    return _hybrid_router


async def _auto_index_fact(
    fact_id: str,
    subject: str,
    predicate: str,
    object: str,
    source: str,
) -> None:
    """Auto-index a stored fact into Qdrant (vector search) and graph.

    Wrapped in try/except -- never crashes the caller if Qdrant/graph
    is unavailable.

    Args:
        fact_id: Unique fact ID.
        subject: Fact subject.
        predicate: Fact predicate.
        object: Fact object.
        source: Fact source.
    """
    try:
        router = await _get_router()
        qdrant = router._qdrant
        embedder = router._embedder
        graph_router = await _get_graph_router()

        # Build fact text for embedding
        fact_text = f"{subject} {predicate} {object}"

        # Embed (sync call, run in thread to avoid blocking)
        vector = await asyncio.to_thread(embedder.embed, fact_text)

        # Upsert into Qdrant
        await qdrant.upsert(
            point_id=fact_id,
            vector=vector,
            payload={
                "subject": subject,
                "predicate": predicate,
                "object": object,
                "source": source,
                "memory_type": "fact",
            },
        )

        # Sync to graph
        graph_router.sync_fact(subject, predicate, object)
    except Exception:
        logger.warning(
            "Auto-indexing failed for fact %s (%s %s %s)",
            fact_id,
            subject,
            predicate,
            object,
            exc_info=True,
        )


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
    router = await _get_hybrid_router()
    result = await router.route(
        query=query,
        top_k=top_k,
        score_threshold=score_threshold,
    )
    return json.dumps(result)


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
    return json.dumps(report)


def run():
    mcp.run(transport="stdio")
