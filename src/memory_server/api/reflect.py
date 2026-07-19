"""ReflectEngine — belief store analysis and reflection.

Card 002: provides 7 analysis modes (overview, contradictions, relations,
decay_analysis, topics, evidence_audit, confidence_histogram) that answer
"what does the agent currently believe, how confident is it, what conflicts
exist, and what should change?"

Card 001 (v0.9): adds ternary relations mode.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from memory_server.evaluation.relation import (
    RelationClassifier,
    _tokenize,
)
from memory_server.models.belief import Belief
from memory_server.providers.sqlite_provider import SQLiteProvider

logger = logging.getLogger(__name__)


def _naive_dt(dt: datetime | None) -> datetime | None:
    """Strip timezone info from a datetime for safe comparison."""
    if dt is not None and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt


# ---------------------------------------------------------------------------
# Contradiction detection constants (v0.7 keyword heuristic, kept for compat)
# ---------------------------------------------------------------------------

# Stopwords for keyword overlap computation.
# Note: "better"/"worse" ARE stopwords — they're uninformative for keyword
# overlap but remain in the raw proposition text for _has_opposite_sentiment().
STOPWORDS = {
    "is", "the", "a", "an", "be", "to", "of", "in", "it",
    "and", "or", "for", "on", "with", "as", "at", "by",
    "better", "worse", "more", "less", "very", "most",
}

# Sentiment opposition pairs for contradiction detection
# Note: this heuristic does NOT catch structural contradictions where
# both propositions use the same favorable word.
# Full LLM-based detection is v0.8+.
OPPOSITE_SENTIMENT: dict[str, str] = {
    "better": "worse",
    "prefer": "avoid",
    "recommend": "against",
    "like": "dislike",
    "good": "bad",
    "fast": "slow",
    "stable": "unstable",
}

# Timeout guard for large contradiction scans
# O(n²) pairwise comparison: 447 beliefs ≈ 100K pairs, ~1s at Python speed
MAX_CONTRADICTION_PAIRS = 100_000
_MAX_BELIEFS_FOR_CONTRADICTION = 447  # derived: sqrt(2 * MAX_CONTRADICTION_PAIRS)


# ---------------------------------------------------------------------------
# Contradiction helper functions (kept for backward compatibility of imports)
# ---------------------------------------------------------------------------


def _has_opposite_sentiment(a: str, b: str) -> bool:
    """Backward-compatible wrapper for relation module."""
    from memory_server.evaluation.relation import _has_opposite_sentiment as _rel_opposite
    return _rel_opposite(a, b)


def detect_contradictions(beliefs: list) -> list[dict]:
    """Backward-compatible wrapper for ternary classifier.

    Delegates to RelationClassifier and returns legacy format
    with detection_score instead of confidence.
    """
    from memory_server.evaluation.relation import RelationClassifier as _RelCls
    classifier = _RelCls()
    results = classifier.find_contradictions(beliefs)
    legacy = []
    for r in results:
        legacy.append({
            "belief_a_id": r["belief_a_id"],
            "proposition_a": r["proposition_a"],
            "confidence_a": r["confidence_a"],
            "belief_b_id": r["belief_b_id"],
            "proposition_b": r["proposition_b"],
            "confidence_b": r["confidence_b"],
            "overlap_score": r["overlap_score"],
            "detection_score": r["confidence"],
            "detection_method": r["detection_method"],
            "detected_at": r["detected_at"],
        })
    return legacy


# ---------------------------------------------------------------------------
# Confidence histogram helpers
# ---------------------------------------------------------------------------

CONFIDENCE_BUCKETS = [
    ("0.9_1.0", 0.9, 1.0),
    ("0.7_0.9", 0.7, 0.9),
    ("0.5_0.7", 0.5, 0.7),
    ("0.3_0.5", 0.3, 0.5),
    ("0.0_0.3", 0.0, 0.3),
]


def _build_histogram(beliefs: list[Belief]) -> dict[str, int]:
    """Build a confidence histogram with predefined buckets."""
    histogram: dict[str, int] = {name: 0 for name, _, _ in CONFIDENCE_BUCKETS}
    for b in beliefs:
        for name, lo, hi in CONFIDENCE_BUCKETS:
            if lo <= b.confidence <= hi if hi == 1.0 else lo <= b.confidence < hi:
                histogram[name] += 1
                break
    return histogram


# ---------------------------------------------------------------------------
# ReflectEngine
# ---------------------------------------------------------------------------


class ReflectEngine:
    """Analyse the belief store and produce structured insights.

    All seven public methods return a dict suitable for JSON serialization.
    """

    def __init__(self, provider: SQLiteProvider):
        self._provider = provider
        self._classifier = RelationClassifier()

    async def _fetch_beliefs(
        self, topic: str | None = None,
        min_confidence: float = 0.0,
        limit: int = 0,
    ) -> list[Belief]:
        """Fetch beliefs with filters. limit=0 means no limit."""
        return await self._provider.search_beliefs(
            tags=[topic] if topic else None,
            min_confidence=min_confidence if min_confidence > 0 else None,
            lifecycle_state=None,  # all states
            limit=limit if limit > 0 else 10000,  # effectively unlimited
        )

    async def overview(
        self,
        topic: str | None = None,
        min_confidence: float = 0.0,
        limit: int = 0,
    ) -> dict[str, Any]:
        """High-level summary of the belief store."""
        beliefs = await self._fetch_beliefs(topic, min_confidence, limit)

        total = len(beliefs)
        by_state: dict[str, int] = {}
        by_tags: dict[str, int] = {}
        confidences: list[float] = []
        contradiction_count = 0
        stale_count = 0
        no_evidence_count = 0
        oldest = None
        newest = None

        for b in beliefs:
            by_state[b.lifecycle_state] = by_state.get(b.lifecycle_state, 0) + 1

            if b.lifecycle_state == "stale":
                stale_count += 1
            if b.lifecycle_state == "contradicted":
                contradiction_count += 1

            # Topic aggregation: track each tag
            if b.tags:
                for tag in b.tags:
                    by_tags[tag] = by_tags.get(tag, 0) + 1
            else:
                by_tags["untagged"] = by_tags.get("untagged", 0) + 1

            confidences.append(b.confidence)

            # Track age in days
            created = _naive_dt(b.created_at)
            if created:
                now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
                age = (now_naive - created).total_seconds() / 86400.0
                if oldest is None or age > oldest:
                    oldest = age
                if newest is None or age < newest:
                    newest = age

        # Confidence buckets
        histogram = _build_histogram(beliefs)

        # Conflicts section
        contradicted_bs = [b for b in beliefs if b.lifecycle_state == "contradicted"]
        total_conflicts = len(contradicted_bs)
        unresolved = total_conflicts // 2

        # auto_resolvable: active belief pairs with keyword overlap and |c1-c2| > 0.5
        active_bs = [b for b in beliefs if b.lifecycle_state == "active"]
        auto_resolvable = 0
        for i in range(len(active_bs)):
            for j in range(i + 1, len(active_bs)):
                a, b = active_bs[i], active_bs[j]
                tokens_a = _tokenize(a.proposition)
                tokens_b = _tokenize(b.proposition)
                overlap = tokens_a & tokens_b
                if len(overlap) >= 2 and abs(a.confidence - b.confidence) > 0.5:
                    auto_resolvable += 1

        # age_hours_max for contradicted beliefs
        age_hours_max = 0
        now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
        for b in contradicted_bs:
            created = _naive_dt(b.created_at)
            if created:
                age_hours = (now_naive - created).total_seconds() / 3600.0
                if age_hours > age_hours_max:
                    age_hours_max = round(age_hours, 1)

        # Decaying next 7d estimate: count beliefs nearing stale (age > 60% TTL for belief=180d)
        decaying_next_7d = 0
        for b in beliefs:
            created = _naive_dt(b.created_at)
            if created:
                now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
                age = (now_naive - created).total_seconds() / 86400.0
                # belief TTL = 180 days: stale at 70% = 126 days
                stale_ratio = 0.7
                if age >= (180 * stale_ratio * 0.9) and b.lifecycle_state == "active":
                    # within 10% of stale threshold
                    decaying_next_7d += 1

        return {
            "mode": "overview",
            "total_beliefs": total,
            "by_lifecycle_state": dict(sorted(by_state.items())),
            "by_topics": dict(sorted(by_tags.items(), key=lambda x: x[1], reverse=True)),
            "confidence": {
                "high_0.8_1.0": histogram.get("0.9_1.0", 0),  # only 0.9-1.0
                "medium_0.5_0.8": (
                    histogram.get("0.7_0.9", 0) + histogram.get("0.5_0.7", 0)
                ),
                "low_0.0_0.5": histogram.get("0.3_0.5", 0) + histogram.get("0.0_0.3", 0),
                "average": (
                    round(sum(confidences) / max(len(confidences), 1), 4)
                    if confidences else 0.0
                ),
            },
            "contradiction_count": contradiction_count,
            "conflicts": {
                "total": total_conflicts,
                "unresolved": unresolved,
                "auto_resolvable": auto_resolvable,
                "age_hours_max": age_hours_max,
            },
            "stale_count": stale_count,
            "decaying_next_7d": decaying_next_7d,
            "no_evidence_count": no_evidence_count,
            "oldest_belief_days": round(oldest, 1) if oldest is not None else 0,
            "newest_belief_days": round(newest, 1) if newest is not None else 0,
        }

    async def contradictions(
        self,
        topic: str | None = None,
        min_confidence: float = 0.0,
        limit: int = 0,
    ) -> dict[str, Any]:
        """Find beliefs that semantically conflict using ternary classifier.

        Delegates to RelationClassifier and filters for contradiction only.
        """
        beliefs = await self._fetch_beliefs(topic, min_confidence, limit)
        # Only scan active and contradicted beliefs
        scan_beliefs = [
            b for b in beliefs
            if b.lifecycle_state in ("active", "contradicted")
        ]
        pairs = detect_contradictions(scan_beliefs)

        recommendation = (
            "run resolve_conflict(belief_a_id, belief_b_id, 'keep_a')"
            if pairs else ""
        )

        return {
            "mode": "contradictions",
            "total": len(pairs),
            "contradictions": pairs,
            "recommendation": recommendation,
        }

    async def relations(
        self,
        topic: str | None = None,
        min_confidence: float = 0.0,
        limit: int = 0,
        context: str | None = None,
        strict_same_context: bool = True,
    ) -> dict[str, Any]:
        """Find all ternary relations between belief pairs.

        Returns contradiction, entailment, and neutral relationships
        between all pairs of active/contradicted beliefs.

        Args:
            topic: Optional topic/tag filter.
            min_confidence: Minimum confidence threshold.
            limit: Maximum number of beliefs to analyse.
            context: Optional context tag applied to all beliefs for
                     same_context gate evaluation.
            strict_same_context: If True, beliefs without matching context
                                 are classified as neutral.

        Returns:
            Dict with mode, relation counts, and full relation list.
        """
        beliefs = await self._fetch_beliefs(topic, min_confidence, limit)
        scan_beliefs = [
            b for b in beliefs
            if b.lifecycle_state in ("active", "contradicted")
        ]

        # Build contexts dict: if a global context is provided, all beliefs
        # share it (same_context is true). Otherwise, use tags as context.
        contexts: dict[str, str] | None = None
        if context is not None:
            contexts = {b.id: context for b in scan_beliefs}
        # If no explicit context, don't pass contexts (same_context defaults to True)

        all_relations = self._classifier.find_relations(
            scan_beliefs,
            contexts=contexts,
            strict_same_context=strict_same_context,
        )

        # Count by relation type
        contradictions = [r for r in all_relations if r["relation"] == "contradiction"]
        entailments = [r for r in all_relations if r["relation"] == "entailment"]

        recommendation = ""
        if contradictions:
            recommendation = (
                f"Found {len(contradictions)} contradiction(s). "
                f"Run resolve_conflict() to resolve."
            )
        elif entailments:
            recommendation = (
                f"Found {len(entailments)} entailment(s). "
                f"Beliefs are consistent."
            )

        # Also find neutral pairs for context-gated results
        neutral_relation_count = 0
        if strict_same_context and context is not None:
            # Count pairs that would be relations but were gated to neutral
            ungated = self._classifier.find_relations(
                scan_beliefs,
                contexts=contexts,
                strict_same_context=False,
            )
            gated_away = len(ungated) - len(all_relations)
            neutral_relation_count = gated_away

        return {
            "mode": "relations",
            "total": len(all_relations),
            "contradictions": len(contradictions),
            "entailments": len(entailments),
            "neutral_pairs": neutral_relation_count,
            "relations": all_relations,
            "recommendation": recommendation,
        }

    async def decay_analysis(
        self,
        topic: str | None = None,
        min_confidence: float = 0.0,
        limit: int = 0,
    ) -> dict[str, Any]:
        """Analyse which beliefs are approaching lifecycle transitions.

        Uses belief TTL of 180 days for decay calculations.
        """
        beliefs = await self._fetch_beliefs(topic, min_confidence, limit)
        now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
        belief_ttl = 180.0  # days (from decay.py PER_TYPE_TTL)

        stale_now = 0
        stale_7d = 0
        archived_7d = 0
        forgotten_7d = 0
        by_tag_stale: dict[str, int] = {}

        for b in beliefs:
            created = _naive_dt(b.created_at)
            if not created:
                continue
            age = (now_naive - created).total_seconds() / 86400.0

            # Current state is stale
            if b.lifecycle_state == "stale":
                stale_now += 1
                for tag in b.tags or []:
                    by_tag_stale[tag] = by_tag_stale.get(tag, 0) + 1

            # Active → stale within 7 days (70% TTL = 126 days)
            if b.lifecycle_state == "active" and 0.7 * belief_ttl <= age + 7:
                stale_7d += 1

            # Stale → archived within 7 days (100% TTL = 180 days)
            if b.lifecycle_state == "stale" and age + 7 >= belief_ttl:
                archived_7d += 1

            # Archived → forgotten within 7 days (200% TTL = 360 days)
            if b.lifecycle_state == "archived" and age + 7 >= 2.0 * belief_ttl:
                forgotten_7d += 1

        recommendation = ""
        if stale_now > 0:
            recommendation = (
                f"Review {stale_now} stale beliefs: "
                f"run get_belief(lifecycle_state='stale')"
            )
        elif stale_7d > 0:
            recommendation = f"{stale_7d} beliefs approaching stale state"

        return {
            "mode": "decay",
            "stale_now": stale_now,
            "stale_7d": stale_7d,
            "archived_7d": archived_7d,
            "forgotten_7d": forgotten_7d,
            "by_tag_stale": dict(sorted(by_tag_stale.items(), key=lambda x: x[1], reverse=True)),
            "recommendation": recommendation,
        }

    async def topics(
        self,
        topic: str | None = None,
        min_confidence: float = 0.0,
        limit: int = 0,
    ) -> dict[str, Any]:
        """Cluster beliefs by tags/topics."""
        beliefs = await self._fetch_beliefs(topic, min_confidence, limit)

        tag_map: dict[str, dict] = {}
        untagged = 0

        for b in beliefs:
            if b.tags:
                for tag in b.tags:
                    if tag not in tag_map:
                        tag_map[tag] = {"count": 0, "total_confidence": 0.0, "stale": 0}
                    tag_map[tag]["count"] += 1
                    tag_map[tag]["total_confidence"] += b.confidence
                    if b.lifecycle_state == "stale":
                        tag_map[tag]["stale"] += 1
            else:
                untagged += 1

        topics_list = []
        for tag, data in sorted(tag_map.items(), key=lambda x: x[1]["count"], reverse=True):
            topics_list.append({
                "tag": tag,
                "count": data["count"],
                "avg_confidence": round(data["total_confidence"] / data["count"], 4),
                "stale": data["stale"],
            })

        return {
            "mode": "topics",
            "topics": topics_list,
            "untagged_count": untagged,
        }

    async def evidence_audit(
        self,
        topic: str | None = None,
        min_confidence: float = 0.0,
        limit: int = 0,
    ) -> dict[str, Any]:
        """Audit evidence quality across beliefs.

        Uses EvidenceRepository.aggregate_stats() for efficient batch statistics.
        """
        beliefs = await self._fetch_beliefs(topic, min_confidence, limit)
        total = len(beliefs)

        from storage.repositories.evidence_repo import EvidenceRepository

        belief_ids = [b.id for b in beliefs]
        try:
            if hasattr(self._provider, "_get_session"):
                async with await self._provider._get_session() as session:
                    ev_repo = EvidenceRepository(session)
                    stats = await ev_repo.aggregate_stats(belief_ids)
                    # Count zero-weight evidence entries
                    if belief_ids:
                        placeholders = ",".join(f":bid_{i}" for i in range(len(belief_ids)))
                        params = {f"bid_{i}": bid for i, bid in enumerate(belief_ids)}
                        sql_text = (
                            "SELECT COUNT(*) FROM evidence "
                            f"WHERE belief_id IN ({placeholders}) AND weight = 0"
                        )
                        sql = __import__("sqlalchemy").text(sql_text)
                        result = await session.execute(sql, params)
                        zero_weight = result.scalar() or 0
                    else:
                        zero_weight = 0
            else:
                stats = {}
                zero_weight = 0
        except Exception:
            stats = {}
            zero_weight = 0

        with_evidence = 0
        without_evidence = 0
        total_evidence_count = 0
        by_source_type: dict[str, int] = {}

        for bid in belief_ids:
            s = stats.get(bid, {"count": 0})
            if s["count"] > 0:
                with_evidence += 1
                total_evidence_count += s["count"]
                for stype, scount in s.get("by_source_type", {}).items():
                    by_source_type[stype] = by_source_type.get(stype, 0) + scount
            else:
                without_evidence += 1

        recommendation = ""
        if without_evidence > total * 0.5:
            recommendation = (
                f"Most beliefs ({without_evidence}/{total}) lack evidence. "
                "Consider adding source evidence via learn() with sources."
            )
        elif zero_weight > 0:
            recommendation = (
                f"{zero_weight} evidence entries have zero weight. "
                "Run audit with --fix-zero-weight to clean."
            )

        return {
            "mode": "evidence_audit",
            "total": total,
            "with_evidence": with_evidence,
            "without_evidence": without_evidence,
            "avg_evidence_per_belief": (
                round(total_evidence_count / max(with_evidence, 1), 2)
                if with_evidence else 0
            ),
            "by_source_type": dict(sorted(by_source_type.items())),
            "zero_weight_entries": zero_weight,
            "recommendation": recommendation,
        }

    async def confidence_histogram(
        self,
        topic: str | None = None,
        min_confidence: float = 0.0,
        limit: int = 0,
    ) -> dict[str, Any]:
        """Confidence distribution across all beliefs."""
        beliefs = await self._fetch_beliefs(topic, min_confidence, limit)
        histogram = _build_histogram(beliefs)

        # Sort beliefs by confidence descending, include evidence count
        belief_list = []
        for b in sorted(beliefs, key=lambda x: x.confidence, reverse=True):
            ev_count = 0
            if hasattr(self._provider, "_get_session"):
                try:
                    async with await self._provider._get_session() as session:
                        from storage.repositories.evidence_repo import EvidenceRepository
                        ev_repo = EvidenceRepository(session)
                        stats = await ev_repo.aggregate_stats([b.id])
                        ev_count = stats.get(b.id, {}).get("count", 0)
                except Exception:
                    ev_count = 0

            belief_list.append({
                "id": b.id,
                "proposition": b.proposition,
                "confidence": b.confidence,
                "evidence_count": ev_count,
                "lifecycle_state": b.lifecycle_state,
            })

        # Find lowest-confidence bucket count (excluding empty buckets)
        non_zero = [c for c in histogram.values() if c > 0]
        lowest_count = min(non_zero) if non_zero else 0

        recommendation = ""
        if lowest_count == 0:
            recommendation = (
                "All beliefs are in the highest confidence bucket. "
                "Consider reinforcing or challenging them."
            )

        return {
            "mode": "confidence",
            "beliefs": belief_list,
            "histogram": histogram,
            "lowest_count": lowest_count,
            "recommendation": recommendation,
        }
