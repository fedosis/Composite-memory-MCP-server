"""Semantic router — embeds queries, searches Qdrant, returns ranked results.

Per ADR-005 routing order:
1. Exact-match rules (keyword-based)
2. Semantic search (embedding-based)
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Protocol

from memory_server.providers.embedding_provider import (
    EmbeddingProvider,
)
from memory_server.providers.exceptions import ProviderSearchError
from memory_server.router.rules import RoutingRuleSet

if TYPE_CHECKING:
    from memory_server.providers.lancedb_provider import LanceDBProvider
    from memory_server.providers.qdrant_provider import QdrantProvider

logger = logging.getLogger(__name__)

DEFAULT_COLLECTION = "memories"
DEFAULT_TOP_K = 10
DEFAULT_SCORE_THRESHOLD = 0.0

# Union type for vector providers. Keep runtime as Any so importing the router
# does not import optional vector backend packages in clean base installs.
if TYPE_CHECKING:
    VectorProvider = QdrantProvider | LanceDBProvider
else:
    VectorProvider = Any


class NativeHandle(Protocol):
    async def result(self) -> Any: ...

    @property
    def terminated(self) -> bool: ...


class NativeSubmitter(Protocol):
    async def __call__(self, function: Callable[..., Any], /, *args: Any,
                       kind: str, **kwargs: Any) -> NativeHandle: ...


@dataclass(frozen=True, slots=True)
class SourceOutcome:
    state: Literal["ok", "failed"]
    results: tuple[dict[str, Any], ...]
    code: Literal["E_VECTOR_BACKEND"] | None


class EmbeddingRouter:
    """Routes queries through rules check then semantic search.

    Args:
        vector_provider: LanceDBProvider or QdrantProvider instance for vector storage.
        embedder: EmbeddingProvider for converting text to vectors.
        rules: Optional RoutingRuleSet (uses defaults if not provided).
        collection: Default collection name.
    """

    def __init__(
        self,
        vector_provider: VectorProvider,
        embedder: EmbeddingProvider | None = None,
        rules: RoutingRuleSet | None = None,
        collection: str = DEFAULT_COLLECTION,
    ) -> None:
        if vector_provider is None or embedder is None:
            raise ValueError("E_CAPABILITY_UNAVAILABLE")
        self._vector_provider = vector_provider
        self._embedder = embedder
        self._rules = rules or RoutingRuleSet.default()
        self._collection = collection

    async def route(
        self,
        query: str,
        top_k: int = DEFAULT_TOP_K,
        score_threshold: float | None = DEFAULT_SCORE_THRESHOLD,
    ) -> dict[str, Any]:
        """Route a query: check rules first, then fall through to semantic search.

        Per ADR-005:
        1. Evaluate routing rules (keyword-based, higher priority wins)
        2. If no rule matches, run semantic search

        Args:
            query: User query text.
            top_k: Max semantic results (default 10).
            score_threshold: Minimum similarity score (default 0.0).

        Returns:
            Dict with either:
            - {"rule_match": {"route": str, "rule_name": str, "matched_keyword": str}}
            - {"semantic_results": list[dict], "total": int}
            - {"error": str}
        """
        if not query or not query.strip():
            return {"error": "Empty query"}

        # Step 1: Check routing rules
        rule_result = self._rules.evaluate(query)
        if rule_result is not None:
            logger.info(
                "routing rule matched",
            )
            return {
                "rule_match": {
                    "route": rule_result.route,
                    "rule_name": rule_result.rule_name,
                    "matched_keyword": rule_result.matched_keyword,
                }
            }

        # Step 2: Semantic search
        logger.info("routing semantic search")
        results = await self.search(query, top_k=top_k, score_threshold=score_threshold)
        return {
            "semantic_results": results,
            "total": len(results),
        }

    async def search(
        self,
        query: str,
        top_k: int = DEFAULT_TOP_K,
        score_threshold: float | None = DEFAULT_SCORE_THRESHOLD,
    ) -> list[dict[str, Any]]:
        """Run semantic search: embed query, search Qdrant, return ranked results.

        Args:
            query: Text to search for.
            top_k: Max results (default 10).
            score_threshold: Minimum similarity score (default 0.0).

        Returns:
            List of result dicts with keys: id, score, payload.
        """
        if not query or not query.strip():
            return []

        # Embed the query
        vector = self._embedder.embed(query)
        logger.debug("embedding dimensions=%d", len(vector))

        # Search vector store — a search failure degrades to a logged []
        # instead of escaping to server/hermes. Embed failures above stay
        # outside the try and keep propagating.
        try:
            results = await self._vector_provider.search(
                collection=self._collection,
                vector=vector,
                limit=top_k,
                score_threshold=score_threshold,
            )
        except ProviderSearchError as exc:
            logger.warning(
                "embedding search failed (%s)", type(exc).__name__
            )
            return []
        except Exception as exc:
            logger.warning(
                "embedding search failed (unexpected %s)",
                type(exc).__name__,
            )
            return []
        logger.debug(
            "semantic_search: top_k=%d found=%d provider=%s",
            top_k, len(results),
            type(self._vector_provider).__name__,
        )
        return results

    async def search_outcome(
        self, query: str, *, top_k: int, score_threshold: float | None,
        native: NativeSubmitter,
    ) -> SourceOutcome:
        # Submission is an authority boundary, not a backend failure.
        handle = await native(self._embedder.embed, query, kind="retrieval-embed")
        try:
            vector = await handle.result()
            results = await self._vector_provider.search(
                collection=self._collection, vector=vector, limit=top_k,
                score_threshold=score_threshold,
            )
        except Exception as exc:
            # LanceDB may wrap a submission failure. Authority stays authoritative
            # even across that public exception chain; never classify it as data.
            cause = exc
            seen = set()
            while cause is not None and id(cause) not in seen:
                seen.add(id(cause))
                code = getattr(cause, "code", None)
                if isinstance(code, str) and (code.startswith("E_RUNTIME_") or code.startswith("E_LOCK_")
                                              or code in {"E_PROFILE_BUSY", "E_RATE_LIMIT"}):
                    raise cause
                cause = cause.__cause__
            return SourceOutcome("failed", (), "E_VECTOR_BACKEND")
        if type(results) not in (list, tuple) or len(results) > top_k:
            return SourceOutcome("failed", (), "E_VECTOR_BACKEND")
        return SourceOutcome("ok", tuple(item for item in results if type(item) is dict), None)
