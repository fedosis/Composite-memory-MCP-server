"""MCP server entry point with tool registrations."""

import json

from mcp.server.fastmcp import FastMCP

from memory_server.api.get_context import get_context as get_context_fn
from memory_server.api.remember import remember as remember_fn
from memory_server.api.search import search as search_fn
from memory_server.providers.embedding_provider import SentenceTransformerEmbeddingProvider
from memory_server.providers.qdrant_provider import QdrantProvider
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.router.embedding_router import EmbeddingRouter

mcp = FastMCP("CompositeMemoryServer")

# Lazy providers — initialized on first use
_provider: SQLiteProvider | None = None
_qdrant: QdrantProvider | None = None
_embedder: SentenceTransformerEmbeddingProvider | None = None
_router: EmbeddingRouter | None = None


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
        _qdrant = QdrantProvider(location=":memory:", prefer_grpc=False)
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
    serialized = {
        "receipt": result["receipt"].model_dump(mode="json"),
        "fact": result["fact"].model_dump(mode="json"),
    }
    return json.dumps(serialized)


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


def run():
    mcp.run(transport="stdio")
