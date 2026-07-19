"""Ranking layer — merges and normalizes results from multiple search providers.

v0.6 Phase 6: RankMerger combines results from FTS keyword search,
semantic/embedding search, and graph search into unified ranked results
with normalized scores.

Each provider produces its own score range. RankMerger normalizes all
scores to 0.0-1.0 and deduplicates by content.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RankResult:
    """A single ranked result from any search provider.

    Attributes:
        content: The text content / fact representation.
        score: Normalized relevance score (0.0 to 1.0).
        source: Which provider produced this result ("fts", "semantic", "graph").
        confidence: The fact's confidence value (0.0 to 1.0), if available.
        metadata: Optional extra data from the source (e.g. fact id, subject,
                  predicate, object, payload, entities).
    """

    content: str
    score: float = 0.0
    source: str = "fts"  # fts | semantic | graph
    confidence: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)


class RankMerger:
    """Merges results from FTS keyword, semantic, and graph search providers.

    Normalizes scores across providers to 0.0-1.0, deduplicates by content,
    and returns a unified sorted result list.
    """

    def __init__(self) -> None:
        self._seen: set[str] = set()

    @staticmethod
    def _normalize_score(
        score: float,
        source: str,
        scores: list[float],
    ) -> float:
        """Normalize a score to 0.0-1.0 based on provider's typical range.

        - FTS native scores are typically unbounded (BM25-like).
          We apply min-max normalization within the batch.
        - Semantic (cosine similarity) is already 0.0-1.0.
        - Graph (entity match count) is normalized by entity count.
        """
        if source == "semantic":
            # Already in 0.0-1.0 range from cosine similarity
            return max(0.0, min(1.0, score))
        if source == "graph":
            # Graph confidence is already 0.0-1.0 per fact or we use
            # entity overlap ratio
            return max(0.0, min(1.0, score))
        # source == "fts": BM25-like unbounded scores
        if not scores:
            return 0.0
        min_s = min(scores)
        max_s = max(scores)
        if max_s - min_s < 0.001:
            return 1.0  # All equal → treat as perfect match
        return (score - min_s) / (max_s - min_s)

    def merge(
        self,
        fts_results: list[RankResult],
        semantic_results: list[RankResult],
        graph_results: list[RankResult],
    ) -> list[RankResult]:
        """Merge and rank results from all three providers.

        Args:
            fts_results: Results from FTS5 keyword search.
            semantic_results: Results from embedding/vector search.
            graph_results: Results from graph entity search.

        Returns:
            Unified ranked list of RankResult, sorted by normalized score
            descending, deduplicated by content.
        """
        self._seen.clear()

        # Normalize scores within each provider's batch
        fts_scores = [r.score for r in fts_results]
        semantic_scores = [r.score for r in semantic_results]
        graph_scores = [r.score for r in graph_results]

        all_results: list[RankResult] = []

        for r in fts_results:
            r.score = self._normalize_score(r.score, "fts", fts_scores)
            all_results.append(r)

        for r in semantic_results:
            r.score = self._normalize_score(r.score, "semantic", semantic_scores)
            all_results.append(r)

        for r in graph_results:
            r.score = self._normalize_score(r.score, "graph", graph_scores)
            all_results.append(r)

        # Deduplicate by content
        seen: set[str] = set()
        deduped: list[RankResult] = []
        for r in sorted(all_results, key=lambda x: x.score, reverse=True):
            content_key = r.content.lower().strip()
            if content_key and content_key not in seen:
                seen.add(content_key)
                deduped.append(r)
            elif not content_key:
                deduped.append(r)  # Keep empty-content entries

        return deduped

    @staticmethod
    def fts_from_facts(
        facts: list[Any],
        query: str = "",
    ) -> list[RankResult]:
        """Convert a list of Fact objects to FTS RankResults.

        The score from FTS5 is not yet available in the Fact model directly,
        so we infer it from confidence. In a real query the FTS5 rank column
        provides the score — here we keep a placeholder until the raw FTS
        score is surfaced.
        """
        results: list[RankResult] = []
        for i, fact in enumerate(facts):
            content = f"{fact.subject} {fact.predicate} {fact.object}"
            # Use position as a rough proxy when FTS5 rank not available;
            # FTS results are ordered by rank, so earlier = better.
            score = 1.0 - (i / max(len(facts), 1) * 0.5)
            results.append(RankResult(
                content=content,
                score=score,
                source="fts",
                confidence=fact.confidence,
                metadata={
                    "id": fact.id,
                    "subject": fact.subject,
                    "predicate": fact.predicate,
                    "object": fact.object,
                },
            ))
        return results

    @staticmethod
    def semantic_from_vector(
        vector_results: list[dict[str, Any]],
    ) -> list[RankResult]:
        """Convert vector store search results to semantic RankResults.

        Works with results from both LanceDBProvider and QdrantProvider.
        """
        return RankMerger.semantic_from_qdrant(vector_results)

    @staticmethod
    def semantic_from_qdrant(
        qdrant_results: list[dict[str, Any]],
    ) -> list[RankResult]:
        """Convert Qdrant search results to semantic RankResults."""
        results: list[RankResult] = []
        for r in qdrant_results:
            payload = r.get("payload", {})
            content = payload.get("content")
            if not content:
                subj = payload.get("subject", "")
                pred = payload.get("predicate", "")
                obj = payload.get("object", "")
                content = f"{subj} {pred} {obj}"
            content = content.strip()
            score = r.get("score", 0.0)
            results.append(RankResult(
                content=content or "No content",
                score=score,
                source="semantic",
                confidence=payload.get("confidence", 1.0),
                metadata={
                    "id": r.get("id", ""),
                    **payload,
                },
            ))
        return results

    @staticmethod
    def graph_from_router(
        graph_result: dict[str, Any],
    ) -> list[RankResult]:
        """Convert GraphRouter query result to graph RankResults."""
        results: list[RankResult] = []
        entities = graph_result.get("entities", [])
        relations = graph_result.get("relations", [])

        # Score entities by how many relations they have
        entity_relation_count: dict[str, int] = {}
        for rel in relations:
            src = rel.get("source_name", "")
            tgt = rel.get("target_name", "")
            entity_relation_count[src] = entity_relation_count.get(src, 0) + 1
            entity_relation_count[tgt] = entity_relation_count.get(tgt, 0) + 1

        for entity in entities:
            name = entity.get("name", "")
            count = entity_relation_count.get(name, 0)
            # Score based on relation count relative to total
            total_rels = len(relations) if relations else 1
            score = min(1.0, count / total_rels) if total_rels > 0 else 1.0
            results.append(RankResult(
                content=name,
                score=score,
                source="graph",
                confidence=1.0,
                metadata=entity,
            ))

        # Add relation-based results
        for rel in relations:
            rel_content = (
                f"{rel.get('source_name', '')} {rel.get('relation', '')} "
                f"{rel.get('target_name', '')}"
            ).strip()
            if rel_content:
                results.append(RankResult(
                    content=rel_content,
                    score=0.8,  # Relations are informative but secondary to entities
                    source="graph",
                    confidence=1.0,
                    metadata=rel,
                ))

        return results
