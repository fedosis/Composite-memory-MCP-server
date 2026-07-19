"""Tests for Card 001: Ternary Relation Classifier.

Tests cover:
- RelationClassifier.classify_pair() — contradiction, entailment, neutral
- same_context gate logic
- Backward-compatible detect_contradictions()
- Edge cases: empty input, borderline cases
"""

import pytest
from datetime import datetime, timezone

from memory_server.evaluation.relation import (
    RelationClassifier,
    RelationResult,
    detect_contradictions,
    detect_relations,
    _tokenize,
    _has_opposite_sentiment,
    _has_same_subject,
    _has_shared_sentiment,
)
from memory_server.models.belief import Belief

# =========================================================================
# Helper
# =========================================================================


def _make_belief(
    proposition: str,
    confidence: float = 0.5,
    tags: list[str] | None = None,
    lifecycle_state: str = "active",
    source_ids: list[str] | None = None,
    created_at: datetime | None = None,
) -> Belief:
    return Belief(
        proposition= proposition,
        confidence=confidence,
        tags=tags or [],
        lifecycle_state=lifecycle_state,
        source_ids=source_ids or [],
        created_at=created_at or datetime.now(timezone.utc),
    )


# =========================================================================
# Helper function tests
# =========================================================================


class TestHelperFunctions:
    """Unit tests for the helper functions used by RelationClassifier."""

    def test_tokenize_removes_stopwords(self):
        tokens = _tokenize("The Docker is running")
        assert "the" not in tokens
        assert "is" not in tokens
        assert "docker" in tokens
        assert "running" in tokens

    def test_tokenize_removes_punctuation(self):
        tokens = _tokenize("Docker is better than Podman!")
        assert "docker" in tokens
        assert "podman" in tokens
        assert "than" in tokens  # "than" is 4 chars, not a stopword
        assert "better" not in tokens  # stopword
        assert len(tokens) == 3  # docker, podman, than

    def test_tokenize_short_words(self):
        tokens = _tokenize("Docker is OK for IT")
        assert "ok" not in tokens  # len <= 2
        assert "it" not in tokens  # len <= 2
        assert "docker" in tokens

    def test_has_opposite_sentiment_true(self):
        """better↔worse across two propositions."""
        assert _has_opposite_sentiment(
            "Docker is better than Podman",
            "Docker is worse than Podman",
        )

    def test_has_opposite_sentiment_prefer_avoid(self):
        assert _has_opposite_sentiment(
            "I prefer Docker",
            "I avoid Docker",
        )

    def test_has_opposite_sentiment_false_same_direction(self):
        """Both positive → not opposite."""
        assert not _has_opposite_sentiment(
            "Docker is better than Podman",
            "Docker is great for containers",
        )

    def test_has_opposite_sentiment_false_no_overlap(self):
        assert not _has_opposite_sentiment(
            "Docker is good",
            "Caddy is a web server",
        )

    def test_has_same_subject(self):
        """'Docker is better...' and 'Docker is worse...' → same subject."""
        assert _has_same_subject(
            "Docker is better than Podman",
            "Docker is worse than Podman",
        )

    def test_has_same_subject_different(self):
        """'Docker is better...' and 'Podman is worse...' → different subject."""
        assert not _has_same_subject(
            "Docker is better than Podman",
            "Podman is worse than Docker",
        )

    def test_has_same_subject_no_subject(self):
        """No significant subject token → False."""
        assert not _has_same_subject(
            "Running better than expected",
            "Working worse than before",
        )

    def test_has_shared_sentiment_both_positive(self):
        assert _has_shared_sentiment(
            "Docker is better than Podman",
            "Docker is good for containers",
        )

    def test_has_shared_sentiment_one_no_sentiment(self):
        """One proposition has no sentiment words → no shared sentiment."""
        assert not _has_shared_sentiment(
            "Docker is better than Podman",
            "Docker runs on Linux",
        )


# =========================================================================
# RelationClassifier ternary classification
# =========================================================================


class TestTernaryClassification:
    """Core ternary classification tests."""

    def setup_method(self):
        self.classifier = RelationClassifier()

    def test_contradiction_same_subject_opposite(self):
        """'Docker is better' vs 'Docker is worse' with same subject → contradiction."""
        a = _make_belief("Docker is better than Podman", confidence=0.9)
        b = _make_belief("Docker is worse than Podman", confidence=0.3)
        result = self.classifier.classify_pair(a, b)
        assert result.relation == "contradiction"
        assert result.confidence >= 0.3

    def test_contradiction_multiple_tokens(self):
        """Opposite sentiment with same subject and multiple overlapping tokens."""
        a = _make_belief("Docker is better than Podman for containers", confidence=0.9)
        b = _make_belief("Docker is worse than Podman for containers", confidence=0.3)
        result = self.classifier.classify_pair(a, b)
        assert result.relation == "contradiction"
        assert result.confidence >= 0.3
        assert result.detection_method == "keyword"

    def test_entailment_structural_inversion(self):
        """'Docker better than Podman' vs 'Podman worse than Docker' → entailment."""
        a = _make_belief("Docker is better than Podman", confidence=0.8)
        b = _make_belief("Podman is worse than Docker", confidence=0.6)
        result = self.classifier.classify_pair(a, b)
        assert result.relation == "entailment", (
            f"Expected entailment, got {result.relation}. "
            f"This fixes the false positive from the old binary detector."
        )
        assert result.detection_method == "entailment_inversion"

    def test_entailment_shared_sentiment(self):
        """Both positive on the same topic → entailment."""
        a = _make_belief("Python is great for AI", confidence=0.9)
        b = _make_belief("Python is excellent for machine learning", confidence=0.85)
        result = self.classifier.classify_pair(a, b)
        assert result.relation == "entailment"

    def test_neutral_unrelated_topics(self):
        """Completely unrelated propositions → neutral."""
        a = _make_belief("Docker is good for containers")
        b = _make_belief("Caddy is a web server")
        result = self.classifier.classify_pair(a, b)
        assert result.relation == "neutral"

    def test_neutral_no_overlap(self):
        """No overlapping keywords → neutral."""
        a = _make_belief("Python is great")
        b = _make_belief("Caddy is fast")
        result = self.classifier.classify_pair(a, b)
        assert result.relation == "neutral"

    def test_empty_beliefs(self):
        """Empty list returns empty results."""
        assert self.classifier.find_relations([]) == []

    def test_single_belief(self):
        """Single belief returns no pairs."""
        b = _make_belief("Docker is good")
        assert self.classifier.find_relations([b]) == []

    def test_relation_result_has_all_fields(self):
        """RelationResult produces all expected output fields."""
        a = _make_belief("Docker is better than Podman", confidence=0.9)
        b = _make_belief("Docker is worse than Podman", confidence=0.3)
        result = self.classifier.classify_pair(a, b)
        d = result.to_dict()
        assert "relation" in d
        assert "confidence" in d
        assert "same_context" in d
        assert "overlap_score" in d
        assert "detection_method" in d
        assert "detected_at" in d
        assert "belief_a_id" in d
        assert "proposition_a" in d
        assert "belief_b_id" in d
        assert "proposition_b" in d


# =========================================================================
# same_context gate tests
# =========================================================================


class TestSameContextGate:
    """Tests for the same_context gate logic."""

    def setup_method(self):
        self.classifier = RelationClassifier()

    def test_same_context_strict_different_contexts_neutral(self):
        """Different contexts with strict=True → neutral."""
        a = _make_belief("Docker is better than Podman")
        b = _make_belief("Docker is worse than Podman")
        result = self.classifier.classify_pair(
            a, b,
            context_a="docker-discussion",
            context_b="container-discussion",
            strict_same_context=True,
        )
        assert result.relation == "neutral"
        assert result.same_context is False
        assert result.detection_method == "context_gate"

    def test_same_context_strict_matching_contexts(self):
        """Same contexts with strict=True → normal classification."""
        a = _make_belief("Docker is better than Podman")
        b = _make_belief("Docker is worse than Podman")
        result = self.classifier.classify_pair(
            a, b,
            context_a="docker-discussion",
            context_b="docker-discussion",
            strict_same_context=True,
        )
        assert result.relation == "contradiction"
        assert result.same_context is True

    def test_same_context_no_context(self):
        """No context provided → same_context defaults to True."""
        a = _make_belief("Docker is better than Podman")
        b = _make_belief("Docker is worse than Podman")
        result = self.classifier.classify_pair(a, b)
        assert result.same_context is True

    def test_same_context_strict_false_different_contexts(self):
        """Different contexts with strict=False → lowered confidence."""
        a = _make_belief("Docker is better than Podman")
        b = _make_belief("Docker is worse than Podman")
        result_normal = self.classifier.classify_pair(a, b)
        result_gated = self.classifier.classify_pair(
            a, b,
            context_a="ctx-a",
            context_b="ctx-b",
            strict_same_context=False,
        )
        assert result_gated.relation == "contradiction"
        assert result_gated.same_context is False
        # Confidence should be penalized
        assert result_gated.confidence < result_normal.confidence

    def test_same_context_no_context_strict_false(self):
        """No context + strict=False → normal classification."""
        a = _make_belief("Docker is better than Podman")
        b = _make_belief("Docker is worse than Podman")
        result = self.classifier.classify_pair(
            a, b,
            strict_same_context=False,
        )
        assert result.relation == "contradiction"
        assert result.same_context is True


# =========================================================================
# Backward-compatible detect_contradictions tests
# =========================================================================


class TestLegacyDetectContradictions:
    """detect_contradictions() still works and returns legacy format."""

    def test_contradictions_still_detected(self):
        """Genuine contradictions still produce results."""
        beliefs = [
            _make_belief("Docker is better than Podman", confidence=0.9),
            _make_belief("Docker is worse than Podman", confidence=0.3),
        ]
        pairs = detect_contradictions(beliefs)
        assert len(pairs) == 1

    def test_false_positive_fixed(self):
        """The Docker/Podman inversion is no longer a false positive."""
        beliefs = [
            _make_belief("Docker is better than Podman", confidence=0.8),
            _make_belief("Podman is worse than Docker", confidence=0.6),
        ]
        pairs = detect_contradictions(beliefs)
        # This was the false positive — now it's entailment, not contradiction
        assert len(pairs) == 0

    def test_legacy_format(self):
        """Output has detection_score, detection_method, etc."""
        beliefs = [
            _make_belief("Docker is better than Podman", confidence=0.9),
            _make_belief("Docker is worse than Podman", confidence=0.3),
        ]
        pairs = detect_contradictions(beliefs)
        assert len(pairs) == 1
        assert "detection_method" in pairs[0]
        assert "detection_score" in pairs[0]
        assert "overlap_score" in pairs[0]

    def test_no_contradictions_empty(self):
        """Unrelated propositions → no contradictions."""
        beliefs = [
            _make_belief("Docker is great"),
            _make_belief("Caddy is a web server"),
        ]
        assert detect_contradictions(beliefs) == []

    def test_empty_input(self):
        assert detect_contradictions([]) == []

    def test_single_belief_no_pairs(self):
        assert detect_contradictions([_make_belief("Docker is good")]) == []


# =========================================================================
# detect_relations convenience wrapper
# =========================================================================


class TestDetectRelations:
    """detect_relations() returns all non-neutral relations."""

    def test_returns_both_contradiction_and_entailment(self):
        beliefs = [
            _make_belief("Docker is better than Podman", confidence=0.9),
            _make_belief("Docker is worse than Podman", confidence=0.3),
            _make_belief("Podman is worse than Docker", confidence=0.6),
            _make_belief("Caddy is a web server", confidence=0.7),
        ]
        results = detect_relations(beliefs)
        relations = {r["relation"] for r in results}
        assert "contradiction" in relations
        assert "entailment" in relations


# =========================================================================
# Edge cases
# =========================================================================


class TestEdgeCases:
    """Boundary and edge-case tests."""

    def setup_method(self):
        self.classifier = RelationClassifier()

    def test_low_overlap_contradiction(self):
        """Single overlapping token with opposite sentiment."""
        a = _make_belief("Docker is better", confidence=0.9)
        b = _make_belief("Docker is worse", confidence=0.3)
        result = self.classifier.classify_pair(a, b)
        # Only 1 overlapping keyword (docker), opposite sentiment
        # Contradiction requires overlap_count >= 2, so this is borderline entailment
        assert result.relation == "entailment"
        assert result.detection_method == "borderline"

    def test_exact_string_match_identical(self):
        """Identical propositions → shared sentiment + overlap → entailment."""
        a = _make_belief("Docker is better than Podman")
        b = _make_belief("Docker is better than Podman")
        result = self.classifier.classify_pair(a, b)
        assert result.relation == "entailment"

    def test_confidence_range(self):
        """Confidence is always in 0.0-1.0 range."""
        for text_a, text_b in [
            ("Docker is better", "Docker is worse"),
            ("Docker is better than Podman", "Podman is worse than Docker"),
            ("Python is great", "Caddy is fast"),
        ]:
            a = _make_belief(text_a)
            b = _make_belief(text_b)
            result = self.classifier.classify_pair(a, b)
            assert 0.0 <= result.confidence <= 1.0

    def test_same_context_false_but_no_context_provided(self):
        """Same context True when no explicit contexts provided."""
        a = _make_belief("Docker is better")
        b = _make_belief("Docker is worse")
        result = self.classifier.classify_pair(a, b)
        assert result.same_context is True
