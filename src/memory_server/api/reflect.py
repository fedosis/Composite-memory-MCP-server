"""ReflectEngine — belief store analysis and reflection.

Card 002: provides 6 analysis modes (overview, contradictions, decay_analysis,
topics, evidence_audit, confidence_histogram) that answer "what does the agent
currently believe, how confident is it, what conflicts exist, and what should
change?"
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from memory_server.models.belief import Belief
from memory_server.providers.sqlite_provider import SQLiteProvider

logger = logging.getLogger(__name__)


def _naive_dt(dt: datetime | None) -> datetime | None:
    """Strip timezone info from a datetime for safe comparison."""
    if dt is not None and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt


# ---------------------------------------------------------------------------
# Contradiction detection constants (v0.7 keyword heuristic)
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
# Contradiction helper functions
# ---------------------------------------------------------------------------


def _tokenize(proposition: str) -> set[str]:
    """Extract significant keywords from a proposition."""
    words = proposition.lower().split()
    return {w.strip(".,!?;:'\"()") for w in words if w not in STOPWORDS and len(w) > 2}


def _has_opposite_sentiment(a: str, b: str) -> bool:
    """Check if two propositions express opposing views on the same topic."""
    words_a = set(a.lower().split())
    words_b = set(b.lower().split())
    for pos, neg in OPPOSITE_SENTIMENT.items():
        if (pos in words_a and neg in words_b) or (neg in words_a and pos in words_b):
            return True
    return False


def detect_contradictions(beliefs: list) -> list[dict]:
    """Find pairs of beliefs with contradictions using multiple heuristics.

    Uses three detection methods:
    - keyword: ≥2 overlapping tokens + opposite sentiment
    - confidence_weighted: detection_score >= 0.3 AND confidence diff > 0.4
    - source_overlap: ≥2 shared evidence source_ids + opposite sentiment

    Requires detection_score >= 0.3 AND at least one condition met.
    For >447 beliefs, logs a warning — caller can sample or accept O(n²).
    """
    if len(beliefs) > _MAX_BELIEFS_FOR_CONTRADICTION:
        logger.warning(
            "Large contradiction scan: %s beliefs, may be slow", len(beliefs)
        )
    now = datetime.now(timezone.utc).isoformat()
    results: list[dict] = []
    for i in range(len(beliefs)):
        for j in range(i + 1, len(beliefs)):
            a, b = beliefs[i], beliefs[j]
            tokens_a = _tokenize(a.proposition)
            tokens_b = _tokenize(b.proposition)
            overlap = tokens_a & tokens_b
            all_tokens = tokens_a | tokens_b

            overlap_score = len(overlap) / max(len(all_tokens), 1)
            confidence_diff = abs(a.confidence - b.confidence)
            confidence_diff_weight = min(confidence_diff * 2.0, 1.0)
            detection_score = overlap_score * confidence_diff_weight

            # Check three detection conditions
            keyword_match = len(overlap) >= 2 and _has_opposite_sentiment(
                a.proposition, b.proposition
            )
            confidence_match = confidence_diff > 0.4
            # Source overlap: check shared source_ids from the denormalized field
            shared_sources = len(set(a.source_ids) & set(b.source_ids))
            source_match = shared_sources >= 2 and _has_opposite_sentiment(
                a.proposition, b.proposition
            )

            # Detection threshold: detection_score >= 0.3 AND at least one condition met
            if detection_score >= 0.3 and (keyword_match or confidence_match or source_match):
                # Detection method: source_overlap > confidence_weighted > keyword
                if source_match:
                    detection_method = "source_overlap"
                elif confidence_match:
                    detection_method = "confidence_weighted"
                else:
                    detection_method = "keyword"

                results.append({
                    "belief_a_id": a.id,
                    "proposition_a": a.proposition,
                    "confidence_a": a.confidence,
                    "belief_b_id": b.id,
                    "proposition_b": b.proposition,
                    "confidence_b": b.confidence,
                    "overlap_score": round(overlap_score, 2),
                    "detection_score": round(detection_score, 2),
                    "detection_method": detection_method,
                    "detected_at": now,
                })
    return results


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

    All six public methods return a dict suitable for JSON serialization.
    """

    def __init__(self, provider: SQLiteProvider):
        self._provider = provider

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
        """Find beliefs that semantically conflict using keyword heuristic."""
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
        if without_evidence > 0:
            recommendation = f"Add evidence to {without_evidence} beliefs with no sources"

        return {
            "mode": "evidence_audit",
            "total": total,
            "with_evidence": with_evidence,
            "without_evidence": without_evidence,
            "avg_evidence_per_belief": round(total_evidence_count / max(with_evidence, 1), 2),
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
        """Detailed confidence histogram across beliefs."""
        beliefs = await self._fetch_beliefs(topic, min_confidence, limit)

        # Sort by confidence descending
        sorted_beliefs = sorted(beliefs, key=lambda b: b.confidence, reverse=True)

        # Build belief list with evidence counts (optional)
        evidence_counts: dict[str, int] = {}
        if beliefs:
            from storage.repositories.evidence_repo import EvidenceRepository

            belief_ids = [b.id for b in beliefs]
            try:
                if hasattr(self._provider, "_get_session"):
                    async with await self._provider._get_session() as session:
                        ev_repo = EvidenceRepository(session)
                        stats = await ev_repo.aggregate_stats(belief_ids)
                        for bid in belief_ids:
                            s = stats.get(bid, {"count": 0})
                            evidence_counts[bid] = s["count"]
            except Exception:
                pass

        belief_list = []
        for b in sorted_beliefs:
            belief_list.append({
                "id": b.id,
                "proposition": b.proposition,
                "confidence": b.confidence,
                "evidence_count": evidence_counts.get(b.id, 0),
                "lifecycle_state": b.lifecycle_state,
            })

        histogram = _build_histogram(beliefs)
        lowest_count = histogram.get("0.0_0.3", 0)

        recommendation = ""
        if lowest_count > 0:
            recommendation = f"Review {lowest_count} beliefs with confidence < 0.3"

        return {
            "mode": "confidence",
            "beliefs": belief_list,
            "histogram": histogram,
            "lowest_count": lowest_count,
            "recommendation": recommendation,
        }
