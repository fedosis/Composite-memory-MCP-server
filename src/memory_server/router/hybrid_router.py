"""Hybrid router — unified query routing across all backends.

Per ADR-005 routing priority:
1. Stage 1: RulesEngine (exact keyword match)
2. Stage 2: SemanticRouter (embedding similarity)
3. Stage 3: GraphRouter (entity relations)
4. Stage 4: LLM fallback (placeholder)

v0.6 Phase 6: route() uses RankMerger to merge and normalize results
from all three sources (FTS, semantic, graph) into a unified ranked
result set.
"""

from __future__ import annotations

import logging
from typing import Any

from memory_server.providers.embedding_provider import (
    EmbeddingProvider,
    MockEmbeddingProvider,
)
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.providers.lancedb_provider import LanceDBProvider
from memory_server.providers.qdrant_provider import QdrantProvider
from memory_server.router.graph_router import GraphRouter
from memory_server.router.ranking import RankMerger, RankResult
from memory_server.router.rules import RoutingRuleSet

logger = logging.getLogger(__name__)

DEFAULT_COLLECTION = "memories"
DEFAULT_TOP_K = 10
DEFAULT_SCORE_THRESHOLD = 0.0

# Union type for vector providers
VectorProvider = QdrantProvider | LanceDBProvider


class HybridRouter:
    """Unified query router across rules, semantic search, graph, and LLM fallback.

    Routes queries through 4 stages. The route() method uses RankMerger to
    merge results from FTS keyword search, semantic search, and graph search
    into a unified ranked result list.

    Args:
        vector_provider: LanceDBProvider or QdrantProvider for semantic search.
        embedder: EmbeddingProvider for text-to-vector conversion.
        rules: Optional RoutingRuleSet (uses defaults if not provided).
        graph: Optional SimpleGraph instance.
        collection: Default collection name.
    """

    def __init__(
        self,
        vector_provider: VectorProvider,
        embedder: EmbeddingProvider | None = None,
        rules: RoutingRuleSet | None = None,
        graph: SimpleGraph | None = None,
        collection: str = DEFAULT_COLLECTION,
    ) -> None:
        self._vector_provider = vector_provider
        self._embedder = embedder or MockEmbeddingProvider()
        self._rules = rules or RoutingRuleSet.default()
        self._graph = graph or SimpleGraph()
        self._graph_router = GraphRouter(graph=self._graph)
        self._collection = collection
        self._merger = RankMerger()
        # FTS provider reference; set externally for FTS search integration
        self._fts_provider = None

    @property
    def fts_provider(self):
        """Get the FTS search provider (set externally)."""
        return self._fts_provider

    @fts_provider.setter
    def fts_provider(self, provider: Any) -> None:
        """Set the FTS search provider (e.g. SQLiteProvider)."""
        self._fts_provider = provider

    async def route(
        self,
        query: str,
        top_k: int = DEFAULT_TOP_K,
        score_threshold: float | None = DEFAULT_SCORE_THRESHOLD,
    ) -> dict[str, Any]:
        """Route a query through all stages and merge results via RankMerger.

        Searches all 3 sources (FTS, semantic, graph) then uses RankMerger
        to normalize scores, deduplicate, and return a unified ranked list.

        Args:
            query: User query text.
            top_k: Max semantic results (default 10).
            score_threshold: Minimum similarity score (default 0.0).

        Returns:
            Dict with:
              - stage: winning stage (1-4)
              - route: winning route name
              - all_results: unified ranked list from RankMerger
              - ranked_results: list of RankResult dicts
              - sources: breakdown of results by source
              - total: total unique results
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
            # Even with a rule match, run all searches and merge
            all_ranked = await self._search_all(query, top_k, score_threshold)
            return {
                "stage": 1,
                "route": "rules",
                "rule_match": {
                    "route": rule_result.route,
                    "rule_name": rule_result.rule_name,
                    "matched_keyword": rule_result.matched_keyword,
                },
                "all_results": all_ranked,
                "ranked_results": [r.__dict__ for r in all_ranked],
                "total": len(all_ranked),
            }

        # Search all sources and merge via RankMerger
        all_ranked = await self._search_all(query, top_k, score_threshold)

        if not all_ranked:
            logger.info("All sources returned no results for '%s'", query)
            return self._llm_fallback(query)

        # Determine winning source from top result
        top_source = all_ranked[0].source
        source_map = {"fts": 1, "semantic": 2, "graph": 3}
        stage = source_map.get(top_source, 4)

        # Count sources
        sources = {"fts": 0, "semantic": 0, "graph": 0}
        for r in all_ranked:
            if r.source in sources:
                sources[r.source] += 1

        logger.info(
            "Route '%s': %d results merged (FTS=%d, semantic=%d, graph=%d)",
            top_source, len(all_ranked), sources["fts"], sources["semantic"], sources["graph"],
        )

        return {
            "stage": stage,
            "route": top_source,
            "all_results": all_ranked,
            "ranked_results": [r.__dict__ for r in all_ranked],
            "sources": sources,
            "total": len(all_ranked),
        }

    async def _search_all(
        self,
        query: str,
        top_k: int = DEFAULT_TOP_K,
        score_threshold: float | None = DEFAULT_SCORE_THRESHOLD,
    ) -> list[RankResult]:
        """Search all three sources (FTS, semantic, graph) and merge.

        Args:
            query: User query text.
            top_k: Max results per source.
            score_threshold: Min similarity score for semantic.

        Returns:
            Unified ranked list of RankResult, deduplicated and sorted.
        """
        fts_results: list[RankResult] = []
        semantic_results: list[RankResult] = []
        graph_results: list[RankResult] = []

        # FTS keyword search (if provider is configured)
        if self._fts_provider is not None:
            try:
                facts = await self._fts_provider.search_facts(text=query, limit=top_k)
                fts_results = RankMerger.fts_from_facts(facts, query)
                logger.debug("FTS returned %d results for '%s'", len(fts_results), query)
            except Exception as e:
                logger.warning("FTS search failed: %s", e)

        # Semantic search (embedding-based)
        try:
            vector = self._embedder.embed(query)
            qdrant_results = await self._vector_provider.search(
                collection=self._collection,
                vector=vector,
                limit=top_k,
                score_threshold=score_threshold,
            )
            semantic_results = RankMerger.semantic_from_vector(qdrant_results)
            logger.debug("Semantic search returned %d results", len(semantic_results))
        except Exception as e:
            logger.warning("Semantic search failed: %s", e)

        # Graph search (entity relation)
        try:
            graph_result = self._graph_router.query(query)
            graph_results = RankMerger.graph_from_router(graph_result)
            logger.debug("Graph search returned %d results", len(graph_results))
        except Exception as e:
            logger.warning("Graph search failed: %s", e)

        # Merge all results
        return self._merger.merge(fts_results, semantic_results, graph_results)

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
