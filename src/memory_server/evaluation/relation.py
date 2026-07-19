"""RelationClassifier — ternary belief relation classifier.

Replaces the binary contradiction detector with ternary classification:
    contradiction | entailment | neutral

Heuristic-based for v0.9; LLM-based deferred to v1.0.

Usage:
    classifier = RelationClassifier()
    result = classifier.classify_pair(belief_a, belief_b)
    results = classifier.find_relations(beliefs)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from memory_server.models.belief import Belief

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants (shared with reflect.py for backward compatibility)
# ---------------------------------------------------------------------------

STOPWORDS: set[str] = {
    "is", "the", "a", "an", "be", "to", "of", "in", "it",
    "and", "or", "for", "on", "with", "as", "at", "by",
    "better", "worse", "more", "less", "very", "most",
}

# Sentiment opposition pairs
OPPOSITE_SENTIMENT: dict[str, str] = {
    "better": "worse",
    "prefer": "avoid",
    "recommend": "against",
    "like": "dislike",
    "good": "bad",
    "fast": "slow",
    "stable": "unstable",
}

# Build reverse lookup for OPPOSITE_SENTIMENT
# Maps each word to its opposite, including the reverse direction
_OPPOSITE_MAP: dict[str, str] = {}
for pos, neg in OPPOSITE_SENTIMENT.items():
    _OPPOSITE_MAP[pos] = neg
    _OPPOSITE_MAP[neg] = pos

# All sentiment words for quick membership check
_ALL_SENTIMENT_WORDS: set[str] = set(_OPPOSITE_MAP.keys())

# Timeout guard for large scans
MAX_RELATION_PAIRS = 100_000
_MAX_BELIEFS = 447  # derived: sqrt(2 * MAX_RELATION_PAIRS)

# Default context-divergence penalty
_CONTEXT_PENALTY = 0.5


def _tokenize(proposition: str) -> set[str]:
    """Extract significant keywords from a proposition."""
    words = proposition.lower().split()
    return {w.strip(".,!?;:'\"()") for w in words if w not in STOPWORDS and len(w) > 2}


def _first_subject_token(proposition: str) -> str | None:
    """Find the first significant non-sentiment token in a proposition.

    Used to detect the grammatical "subject" of a proposition for
    distinguishing contradiction from structural inversion.

    Returns the first word that is not a stopword and not a sentiment word,
    or None if no such token exists.
    """
    words = proposition.lower().split()
    for w in words:
        cleaned = w.strip(".,!?;:'\"()")
        if cleaned and cleaned not in STOPWORDS and cleaned not in _ALL_SENTIMENT_WORDS and len(cleaned) > 2:
            return cleaned
    return None


def _has_opposite_sentiment(a: str, b: str) -> bool:
    """Check if two propositions express opposing views on the same topic.

    True when one proposition contains a positive word and the other
    contains its defined opposite (e.g. better↔worse, like↔dislike).
    Does NOT fire when both propositions contain both words of a pair.
    """
    words_a = set(a.lower().split())
    words_b = set(b.lower().split())

    for pos, neg in OPPOSITE_SENTIMENT.items():
        a_has_pos = pos in words_a
        a_has_neg = neg in words_a
        b_has_pos = pos in words_b
        b_has_neg = neg in words_b

        # Both propositions contain BOTH words of the pair → no clear opposition
        if (a_has_pos and a_has_neg) or (b_has_pos and b_has_neg):
            continue

        # One has positive, the other has negative → true opposition
        if a_has_pos and b_has_neg:
            return True
        if a_has_neg and b_has_pos:
            return True

    return False


def _has_same_subject(a: str, b: str) -> bool:
    """Check if both propositions have the same first significant token.

    "Docker is better than Podman"  → first subject: "docker"
    "Docker is worse than Podman"   → first subject: "docker"  → MATCH → contradiction

    "Docker is better than Podman"  → first subject: "docker"
    "Podman is worse than Docker"   → first subject: "podman"  → NO MATCH → entailment
    """
    subject_a = _first_subject_token(a)
    subject_b = _first_subject_token(b)
    if subject_a is None or subject_b is None:
        return False
    return subject_a == subject_b


def _has_shared_sentiment(a: str, b: str) -> bool:
    """Check if both propositions share the same sentiment direction.

    True when:
    - Both contain positive sentiment words
    - Both contain negative sentiment words
    """
    words_a = set(a.lower().split())
    words_b = set(b.lower().split())

    a_sent = {w for w in words_a if w in _ALL_SENTIMENT_WORDS}
    b_sent = {w for w in words_b if w in _ALL_SENTIMENT_WORDS}

    if not a_sent or not b_sent:
        return False

    a_directions: set[str] = set()
    b_directions: set[str] = set()

    for w in a_sent:
        if w in OPPOSITE_SENTIMENT:  # it's a "positive" key
            a_directions.add("pos")
        elif _OPPOSITE_MAP.get(w):  # it's a "negative" value
            a_directions.add("neg")

    for w in b_sent:
        if w in OPPOSITE_SENTIMENT:
            b_directions.add("pos")
        elif _OPPOSITE_MAP.get(w):
            b_directions.add("neg")

    # Both have only positive sentiment words, or both only negative
    if a_directions == {"pos"} and b_directions == {"pos"}:
        return True
    if a_directions == {"neg"} and b_directions == {"neg"}:
        return True

    return False


# ---------------------------------------------------------------------------
# RelationResult — typed dict equivalent
# ---------------------------------------------------------------------------


class RelationResult:
    """Result of a ternary relation classification between two beliefs.

    Attributes:
        relation: One of "contradiction", "entailment", "neutral".
        confidence: Confidence in the classification (0.0-1.0).
        same_context: Whether the two beliefs share the same context.
        overlap_score: Jaccard keyword overlap score.
        detection_method: Which heuristic was used.
        detected_at: ISO-8601 timestamp.
        belief_a_id: ID of first belief.
        proposition_a: Text of first belief.
        confidence_a: Confidence of first belief.
        belief_b_id: ID of second belief.
        proposition_b: Text of second belief.
        confidence_b: Confidence of second belief.
    """

    __slots__ = (
        "relation", "confidence", "same_context", "overlap_score",
        "detection_method", "detected_at",
        "belief_a_id", "proposition_a", "confidence_a",
        "belief_b_id", "proposition_b", "confidence_b",
    )

    def __init__(
        self,
        relation: str,
        confidence: float,
        same_context: bool,
        overlap_score: float,
        detection_method: str,
        belief_a_id: str = "",
        proposition_a: str = "",
        confidence_a: float = 0.0,
        belief_b_id: str = "",
        proposition_b: str = "",
        confidence_b: float = 0.0,
    ):
        self.relation = relation
        self.confidence = round(confidence, 2)
        self.same_context = same_context
        self.overlap_score = round(overlap_score, 2)
        self.detection_method = detection_method
        self.detected_at = datetime.now(timezone.utc).isoformat()
        self.belief_a_id = belief_a_id
        self.proposition_a = proposition_a
        self.confidence_a = confidence_a
        self.belief_b_id = belief_b_id
        self.proposition_b = proposition_b
        self.confidence_b = confidence_b

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dict."""
        return {
            "relation": self.relation,
            "confidence": self.confidence,
            "same_context": self.same_context,
            "overlap_score": self.overlap_score,
            "detection_method": self.detection_method,
            "detected_at": self.detected_at,
            "belief_a_id": self.belief_a_id,
            "proposition_a": self.proposition_a,
            "confidence_a": self.confidence_a,
            "belief_b_id": self.belief_b_id,
            "proposition_b": self.proposition_b,
            "confidence_b": self.confidence_b,
        }

    def to_legacy_contradiction_dict(self) -> dict[str, Any]:
        """Return a dict compatible with the old contradiction output format.

        Used for backward compatibility with detect_contradictions().
        """
        return {
            "belief_a_id": self.belief_a_id,
            "proposition_a": self.proposition_a,
            "confidence_a": self.confidence_a,
            "belief_b_id": self.belief_b_id,
            "proposition_b": self.proposition_b,
            "confidence_b": self.confidence_b,
            "overlap_score": self.overlap_score,
            "detection_score": self.confidence,
            "detection_method": self.detection_method,
            "detected_at": self.detected_at,
        }


# ---------------------------------------------------------------------------
# RelationClassifier
# ---------------------------------------------------------------------------


class RelationClassifier:
    """Ternary belief relation classifier.

    Classifies pairs of beliefs into one of three relations:
    - contradiction: Beliefs cannot both be true.
    - entailment: One belief logically implies the other.
    - neutral: No clear logical relation.

    For v0.9, uses keyword heuristics. LLM-based classification
    is deferred to v1.0.
    """

    def __init__(self) -> None:
        self._sentiment_pairs = dict(OPPOSITE_SENTIMENT)
        self._stopwords = set(STOPWORDS)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def classify_pair(
        self,
        belief_a: Belief,
        belief_b: Belief,
        context_a: str | None = None,
        context_b: str | None = None,
        strict_same_context: bool = True,
    ) -> RelationResult:
        """Classify the relation between two beliefs.

        Args:
            belief_a: First belief.
            belief_b: Second belief.
            context_a: Optional context for belief_a (tag, conversation ID, etc.).
            context_b: Optional context for belief_b.
            strict_same_context: If True (default), different contexts → neutral.

        Returns:
            RelationResult with ternary classification.
        """
        # --- Step 1: same_context gate ---
        same_context, context_penalty = self._check_same_context(
            context_a, context_b
        )

        if not same_context and strict_same_context:
            return RelationResult(
                relation="neutral",
                confidence=0.2,
                same_context=False,
                overlap_score=0.0,
                detection_method="context_gate",
                belief_a_id=belief_a.id,
                proposition_a=belief_a.proposition,
                confidence_a=belief_a.confidence,
                belief_b_id=belief_b.id,
                proposition_b=belief_b.proposition,
                confidence_b=belief_b.confidence,
            )

        # --- Step 2: Tokenization ---
        tokens_a = _tokenize(belief_a.proposition)
        tokens_b = _tokenize(belief_b.proposition)
        overlap = tokens_a & tokens_b
        union = tokens_a | tokens_b
        overlap_score = len(overlap) / max(len(union), 1)

        # --- Step 3: Sentiment analysis ---
        opposite = _has_opposite_sentiment(
            belief_a.proposition, belief_b.proposition
        )
        same_subject = _has_same_subject(
            belief_a.proposition, belief_b.proposition
        )
        shared_sentiment = _has_shared_sentiment(
            belief_a.proposition, belief_b.proposition
        )

        # --- Step 4: Classification ---
        result = self._classify(
            overlap_score=overlap_score,
            overlap_count=len(overlap),
            opposite=opposite,
            same_subject=same_subject,
            shared_sentiment=shared_sentiment,
            same_context=same_context,
            context_penalty=context_penalty,
            belief_a=belief_a,
            belief_b=belief_b,
        )
        return result

    def find_relations(
        self,
        beliefs: list[Belief],
        contexts: dict[str, str] | None = None,
        strict_same_context: bool = True,
    ) -> list[dict[str, Any]]:
        """Find all ternary relations between pairs of beliefs.

        Args:
            beliefs: List of Belief instances to compare pairwise.
            contexts: Optional dict mapping belief_id -> context string.
            strict_same_context: Passed to classify_pair.

        Returns:
            List of RelationResult dicts for all pairs where a relation
            (contradiction or entailment) was found (neutral pairs excluded).
        """
        if len(beliefs) > _MAX_BELIEFS:
            logger.warning(
                "Large relation scan: %s beliefs, may be slow", len(beliefs)
            )

        results: list[dict[str, Any]] = []
        for i in range(len(beliefs)):
            for j in range(i + 1, len(beliefs)):
                a, b = beliefs[i], beliefs[j]
                ctx_a = contexts.get(a.id) if contexts else None
                ctx_b = contexts.get(b.id) if contexts else None

                result = self.classify_pair(
                    a, b,
                    context_a=ctx_a,
                    context_b=ctx_b,
                    strict_same_context=strict_same_context,
                )

                if result.relation != "neutral":
                    results.append(result.to_dict())

        return results

    def find_contradictions(
        self,
        beliefs: list[Belief],
        contexts: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """Find only contradiction relations (backward-compatible helper)."""
        all_rels = self.find_relations(
            beliefs, contexts=contexts, strict_same_context=False
        )
        return [r for r in all_rels if r["relation"] == "contradiction"]

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _check_same_context(
        self,
        context_a: str | None,
        context_b: str | None,
    ) -> tuple[bool, float]:
        """Check if two beliefs share the same context.

        Returns:
            Tuple of (same_context: bool, penalty: float).
            penalty is 0.0 if same_context, else _CONTEXT_PENALTY.
        """
        if context_a is None or context_b is None:
            return True, 0.0
        if context_a == context_b:
            return True, 0.0
        return False, _CONTEXT_PENALTY

    def _classify(
        self,
        overlap_score: float,
        overlap_count: int,
        opposite: bool,
        same_subject: bool,
        shared_sentiment: bool,
        same_context: bool,
        context_penalty: float,
        belief_a: Belief,
        belief_b: Belief,
    ) -> RelationResult:
        """Main classification logic.

        Priority order:
        1. Opposite sentiment + same subject + overlap ≥ 2 → contradiction
           ("Docker is better than Podman" vs "Docker is worse than Podman")
        2. Opposite sentiment + different subject + overlap ≥ 2 → entailment (structural inversion)
           ("Docker is better than Podman" vs "Podman is worse than Docker")
        3. Shared sentiment + overlap ≥ 2 → entailment
           ("Python is great for AI" vs "Python is excellent for ML")
        4. Overlap ≥ 1 → borderline entailment (low confidence)
        5. Otherwise → neutral
        """
        # --- Contradiction ---
        # A: "Docker is better than Podman"
        # B: "Docker is worse than Podman"
        # Same subject, opposite sentiment, same topic tokens
        if opposite and same_subject and overlap_count >= 2:
            confidence = min(overlap_score * 0.9 + 0.2, 1.0)
            if not same_context:
                confidence *= (1.0 - context_penalty)
            return RelationResult(
                relation="contradiction",
                confidence=confidence,
                same_context=same_context,
                overlap_score=overlap_score,
                detection_method="keyword",
                belief_a_id=belief_a.id,
                proposition_a=belief_a.proposition,
                confidence_a=belief_a.confidence,
                belief_b_id=belief_b.id,
                proposition_b=belief_b.proposition,
                confidence_b=belief_b.confidence,
            )

        # --- Entailment via structural inversion ---
        # A: "Docker is better than Podman"
        # B: "Podman is worse than Docker"
        # Different subject, opposite sentiment (scalar inversion = same meaning)
        if opposite and not same_subject and overlap_count >= 2:
            confidence = min(overlap_score + 0.3, 1.0)
            if not same_context:
                confidence *= (1.0 - context_penalty)
            return RelationResult(
                relation="entailment",
                confidence=confidence,
                same_context=same_context,
                overlap_score=overlap_score,
                detection_method="entailment_inversion",
                belief_a_id=belief_a.id,
                proposition_a=belief_a.proposition,
                confidence_a=belief_a.confidence,
                belief_b_id=belief_b.id,
                proposition_b=belief_b.proposition,
                confidence_b=belief_b.confidence,
            )

        # --- Entailment via shared sentiment ---
        # A: "Python is great for AI"
        # B: "Python is excellent for machine learning"
        if shared_sentiment and overlap_count >= 2:
            confidence = min(overlap_score * 0.7 + 0.2, 1.0)
            if not same_context:
                confidence *= (1.0 - context_penalty)
            return RelationResult(
                relation="entailment",
                confidence=confidence,
                same_context=same_context,
                overlap_score=overlap_score,
                detection_method="entailment_keyword",
                belief_a_id=belief_a.id,
                proposition_a=belief_a.proposition,
                confidence_a=belief_a.confidence,
                belief_b_id=belief_b.id,
                proposition_b=belief_b.proposition,
                confidence_b=belief_b.confidence,
            )

        # --- Borderline: low-overlap entailment ---
        if overlap_count >= 1:
            confidence = min(overlap_score * 0.5, 0.35)
            if not same_context:
                confidence *= (1.0 - context_penalty)
            if confidence > 0.0:
                return RelationResult(
                    relation="entailment",
                    confidence=confidence,
                    same_context=same_context,
                    overlap_score=overlap_score,
                    detection_method="borderline",
                    belief_a_id=belief_a.id,
                    proposition_a=belief_a.proposition,
                    confidence_a=belief_a.confidence,
                    belief_b_id=belief_b.id,
                    proposition_b=belief_b.proposition,
                    confidence_b=belief_b.confidence,
                )

        # --- Neutral ---
        return RelationResult(
            relation="neutral",
            confidence=0.0 if same_context else 0.1,
            same_context=same_context,
            overlap_score=overlap_score,
            detection_method="none",
            belief_a_id=belief_a.id,
            proposition_a=belief_a.proposition,
            confidence_a=belief_a.confidence,
            belief_b_id=belief_b.id,
            proposition_b=belief_b.proposition,
            confidence_b=belief_b.confidence,
        )


# ---------------------------------------------------------------------------
# Backward-compatible wrapper
# ---------------------------------------------------------------------------


def detect_contradictions(beliefs: list[Belief]) -> list[dict[str, Any]]:
    """Backward-compatible contradiction detection.

    Uses RelationClassifier internally but returns the old output format
    (detection_score, detection_method, etc.) for backward compatibility.

    Deprecated. Use RelationClassifier().find_contradictions() instead.
    """
    classifier = RelationClassifier()
    results = classifier.find_contradictions(beliefs)
    # Convert to legacy format with detection_score
    legacy_results = []
    for r in results:
        legacy_results.append({
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
    return legacy_results


def detect_relations(beliefs: list[Belief]) -> list[dict[str, Any]]:
    """Convenience wrapper for find_relations."""
    classifier = RelationClassifier()
    return classifier.find_relations(beliefs)
