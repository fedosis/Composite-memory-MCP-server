"""Confidence engine — heuristic scoring for facts and memories.

Computes a 0.0–1.0 confidence score based on:
- Source reliability
- Age/decay over time
- Corroboration from multiple sources
- Conflict penalties
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from memory_server.settings import get_settings

# Settings-derived aliases (single source: settings.py). Snapshot at import;
# kept for backward-compatible imports. Behavior follows Settings via
# ConfidenceEngine.__init__ (live get_settings()).
SOURCE_RELIABILITY: dict[str, float] = dict(get_settings().confidence_source_reliability)
DEFAULT_SOURCE_TYPE = "unknown"  # heuristic default; no Settings field
DEFAULT_TTL_DAYS: float = get_settings().confidence_ttl_days
LIFECYCLE_MULTIPLIER: dict[str, float] = dict(get_settings().confidence_lifecycle_multipliers)

_OLD_LIFECYCLE_MAP: dict[str, str] = {
    "trusted": "active",
    "deprecated": "stale",
}


class ConfidenceEngine:
    """Heuristic confidence scoring engine.

    Scores facts based on source reliability, age, corroboration,
    conflict signals, and lifecycle state.

    Lifecycle state factor: active > stale > archived for scoring.
    """

    def __init__(
        self,
        source_reliability: dict[str, float] | None = None,
        ttl_days: float | None = None,
        lifecycle_multipliers: dict[str, float] | None = None,
        corroboration_boost: float | None = None,
        boost_threshold: int | None = None,
        conflict_penalty: float | None = None,
        penalty_threshold: int | None = None,
    ) -> None:
        _s = get_settings()
        self._source_reliability = source_reliability or dict(_s.confidence_source_reliability)
        self._ttl_days = _s.confidence_ttl_days if ttl_days is None else ttl_days
        self._lifecycle_multipliers = lifecycle_multipliers or dict(_s.confidence_lifecycle_multipliers)
        # NOTE: float values live under *_value names because the scoring
        # methods below are also named _corroboration_boost/_conflict_penalty;
        # an instance attribute with the same name would shadow the method.
        self._corroboration_boost_value = (
            _s.confidence_corroboration_boost
            if corroboration_boost is None else corroboration_boost
        )
        self._boost_threshold = (
            _s.confidence_boost_threshold if boost_threshold is None else boost_threshold
        )
        self._conflict_penalty_value = (
            _s.confidence_conflict_penalty
            if conflict_penalty is None else conflict_penalty
        )
        self._penalty_threshold = (
            _s.confidence_penalty_threshold if penalty_threshold is None else penalty_threshold
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def score_fact(self, fact_data: dict[str, Any]) -> float:
        """Compute a heuristic confidence score for a fact.

        Parameters expected in *fact_data*:

        - ``source_type`` (str): one of the keys in SOURCE_RELIABILITY.
        - ``created_at`` (datetime, optional): when the fact was created.
        - ``subject`` / ``predicate`` / ``object`` (str): the SPO triple.
        - ``corroboration_count`` (int, optional): number of corroborating sources.
        - ``conflict_count`` (int, optional): number of conflicting facts.
        - ``lifecycle_state`` (str, optional): lifecycle state for weighting.

        Returns:
            A float in [0.0, 1.0].
        """
        # 1. Source reliability (base)
        source_type = fact_data.get("source_type", DEFAULT_SOURCE_TYPE)
        base = self._source_reliability.get(
            source_type,
            self._source_reliability.get(DEFAULT_SOURCE_TYPE, 0.3),
        )

        # 2. Age decay
        created_at = fact_data.get("created_at")
        age = self._compute_age(created_at)
        age_factor = self._age_decay(age)

        # 3. Corroboration boost
        corr_count = fact_data.get("corroboration_count", 0)
        corr_boost = self._corroboration_boost(corr_count)

        # 4. Conflict penalty
        conflict_count = fact_data.get("conflict_count", 0)
        conflict_penalty = self._conflict_penalty(conflict_count)

        # 5. Lifecycle state multiplier
        lifecycle_state = fact_data.get("lifecycle_state", "active")
        # Normalize old values
        lifecycle_state = _OLD_LIFECYCLE_MAP.get(lifecycle_state, lifecycle_state)
        lifecycle_mult = self._lifecycle_multipliers.get(lifecycle_state, 1.0)

        score = (base * age_factor + corr_boost - conflict_penalty) * lifecycle_mult
        return max(0.0, min(1.0, score))

    def corroboration(
        self,
        facts: list[dict[str, Any]],
    ) -> float:
        """Compute corroboration strength across a list of facts.

        Two facts corroborate if they share the same (subject, predicate,
        object) triple.  Returns a float in [0.0, 1.0] where 0 = no
        corroboration, 1 = three or more sources agree.
        """
        if not facts:
            return 0.0

        spo_map: dict[tuple[str, str, str], set[str]] = {}
        for f in facts:
            key = (
                f.get("subject", ""),
                f.get("predicate", ""),
                f.get("object", ""),
            )
            source = f.get("source", str(id(f)))
            spo_map.setdefault(key, set()).add(source)

        max_agree = max(len(v) for v in spo_map.values()) if spo_map else 0
        if max_agree <= 1:
            return 0.0
        # Scale: 2 sources = 0.5, 3+ sources = 1.0
        return min(1.0, (max_agree - 1) / 2.0)

    def conflict_detection(
        self,
        facts: list[dict[str, Any]],
    ) -> list[tuple[int, int]]:
        """Detect pairs of facts that contradict each other.

        Returns list of ``(index_a, index_b)`` tuples where the two
        facts share the same subject and predicate but disagree on
        the object.
        """
        conflicts: list[tuple[int, int]] = []
        n = len(facts)
        for i in range(n):
            for j in range(i + 1, n):
                a = facts[i]
                b = facts[j]
                if (
                    a.get("subject") == b.get("subject")
                    and a.get("predicate") == b.get("predicate")
                    and a.get("object") != b.get("object")
                ):
                    conflicts.append((i, j))
        return conflicts

    def score_belief(self, evidence: list[Any]) -> float:
        """Compute belief confidence as weighted average of evidence weights."""
        if not evidence:
            return 0.5
        weights = [e.weight if hasattr(e, 'weight') else e.get('weight', 0.5) for e in evidence]
        return max(0.0, min(1.0, sum(weights) / len(weights)))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _compute_age(self, created_at: Any) -> float:
        """Compute age in days (float).  Returns 0 for missing timestamps."""
        if created_at is None:
            return 0.0
        if isinstance(created_at, datetime):
            now = datetime.now(timezone.utc)
            delta = now - created_at
            return max(0.0, delta.total_seconds() / 86400.0)
        return 0.0

    def _age_decay(self, age_days: float) -> float:
        """Apply exponential decay based on age vs TTL.

        Fresh facts (age near 0) return 1.0.
        Older facts asymptote toward a minimum of 0.3.
        """
        if age_days <= 0:
            return 1.0
        ratio = age_days / self._ttl_days
        # Exponential decay: e^{-ratio}, floored at 0.3
        decayed = max(0.3, 1.0 * (2.0 ** (-ratio)))
        return decayed

    def _corroboration_boost(self, count: int) -> float:
        """Return a confidence boost based on corroboration count.

        0 sources → 0.0
        1 source → 0.0
        2 sources → 0.05
        3+ sources → 0.10
        """
        if count >= self._boost_threshold + 1:
            return self._corroboration_boost_value
        if count == self._boost_threshold:
            return self._corroboration_boost_value / 2.0
        return 0.0

    def _conflict_penalty(self, count: int) -> float:
        """Return a confidence penalty based on conflict count.

        0 conflicts → 0.0
        1 conflict → 0.10
        2+ conflicts → 0.20
        """
        if count >= self._penalty_threshold + 1:
            return self._conflict_penalty_value
        if count == self._penalty_threshold:
            return self._conflict_penalty_value / 2.0
        return 0.0
