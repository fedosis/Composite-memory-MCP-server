"""Hybrid router — unified query routing across all backends.

Per ADR-005 routing priority:
1. Stage 1: RulesEngine (exact keyword match)
2. Stage 2: SemanticRouter (embedding similarity)
3. Stage 3: GraphRouter (entity relations)
4. Stage 4: LLM fallback (placeholder)

Each stage is evaluated in order. The highest-priority stage that produces
a meaningful result wins.
"""

from __future__ import annotations

import logging
from typing import Any

from memory_server.providers.embedding_provider import (
    EmbeddingProvider,
    MockEmbeddingProvider,
)
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.providers.qdrant_provider import QdrantProvider
from memory_server.router.graph_router import GraphRouter
from memory_server.router.rules import RoutingRuleSet

logger = logging.getLogger(__name__)

DEFAULT_COLLECTION = "memories"
DEFAULT_TOP_K = 10
DEFAULT_SCORE_THRESHOLD = 0.0


class HybridRouter:
    """Unified query router across rules, semantic search, graph, and LLM fallback.

    Routes queries through 4 stages in priority order, returning the best
    result from the highest-priority stage that produces meaningful output.

    Args:
        qdrant_provider: QdrantProvider for semantic search.
        embedder: EmbeddingProvider for text-to-vector conversion.
        rules: Optional RoutingRuleSet (uses defaults if not provided).
        graph: Optional SimpleGraph instance.
        collection: Default Qdrant collection name.
    """

    def __init__(
        self,
        qdrant_provider: QdrantProvider,
        embedder: EmbeddingProvider | None = None,
        rules: RoutingRuleSet | None = None,
        graph: SimpleGraph | None = None,
        collection: str = DEFAULT_COLLECTION,
    ) -> None:
        self._qdrant = qdrant_provider
        self._embedder = embedder or MockEmbeddingProvider()
        self._rules = rules or RoutingRuleSet.default()
        self._graph = graph or SimpleGraph()
        self._graph_router = GraphRouter(graph=self._graph)
        self._collection = collection

    async def route(
        self,
        query: str,
        top_k: int = DEFAULT_TOP_K,
        score_threshold: float | None = DEFAULT_SCORE_THRESHOLD,
    ) -> dict[str, Any]:
        """Route a query through all stages in priority order.

        Args:
            query: User query text.
            top_k: Max semantic results (default 10).
            score_threshold: Minimum similarity score (default 0.0).

        Returns:
            Dict with stage info and result from the winning stage.
        """
        if not query or not query.strip():
            return self._llm_fallback(query)

        # Stage 1: RulesEngine (keyword-based exact match)
        rule_result = self._rules.evaluate(query)
        if rule_result is not None:
            logger.info(
                "Stage 1: Query '%s' matched rule '%s' -> route '%s'",
                query, rule_result.rule_name, rule_result.route,
            )
            return {
                "stage": 1,
                "route": "rules",
                "rule_match": {
                    "route": rule_result.route,
                    "rule_name": rule_result.rule_name,
                    "matched_keyword": rule_result.matched_keyword,
                },
            }

        # Stage 2: SemanticRouter (embedding similarity)
        vector = self._embedder.embed(query)
        semantic_results = await self._qdrant.search(
            collection=self._collection,
            vector=vector,
            limit=top_k,
            score_threshold=score_threshold,
        )
        if semantic_results:
            logger.info(
                "Stage 2: Query '%s' matched %d semantic results",
                query, len(semantic_results),
            )
            return {
                "stage": 2,
                "route": "semantic",
                "semantic_results": semantic_results,
                "total": len(semantic_results),
            }

        # Stage 3: GraphRouter (entity relations)
        graph_result = self._graph_router.query(query)
        if graph_result.get("entities"):
            logger.info(
                "Stage 3: Query '%s' matched %d entities",
                query, len(graph_result["entities"]),
            )
            return {
                "stage": 3,
                "route": "graph",
                "graph_result": graph_result,
            }

        # Stage 4: LLM fallback (placeholder)
        logger.info("Stage 4: Query '%s' — LLM fallback", query)
        return self._llm_fallback(query)

    # --- Graph integration helpers ---

    def sync_fact(self, subject: str, predicate: str, object: str) -> None:
        """Sync an extracted fact into the graph.

        Args:
            subject: Subject entity name.
            predicate: Relation/predicate.
            object: Object entity name.
        """
        self._graph_router.sync_fact(subject, predicate, object)

    def sync_decision(
        self,
        choice: str,
        reason: str,
        entities: list[str],
    ) -> None:
        """Sync an extracted decision into the graph.

        Args:
            choice: Decision choice text.
            reason: Decision reason.
            entities: List of entity names mentioned.
        """
        self._graph_router.sync_decision(choice, reason, entities)

    # --- Internal ---

    def _llm_fallback(self, query: str) -> dict[str, Any]:
        """Return a placeholder LLM fallback response.

        Args:
            query: Original query string.

        Returns:
            Fallback response dict.
        """
        return {
            "stage": 4,
            "route": "llm_fallback",
            "message": "LLM fallback not configured",
        }
