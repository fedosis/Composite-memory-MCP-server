"""Tests for ConfidenceEngine (Card 022)."""

from datetime import datetime, timedelta, timezone

import pytest

from memory_server.evaluation.confidence import (
    SOURCE_RELIABILITY,
    ConfidenceEngine,
)


@pytest.fixture
def engine() -> ConfidenceEngine:
    return ConfidenceEngine()


class TestConfidenceEngine:
    """ConfidenceEngine — heuristic scoring."""

    # --- score ranges ---

    def test_score_between_0_and_1(self, engine):
        """All scores must be in [0.0, 1.0]."""
        for source in SOURCE_RELIABILITY:
            score = engine.score_fact({"source_type": source})
            assert 0.0 <= score <= 1.0, f"Source {source} → {score}"

    def test_score_clamps_at_zero(self, engine):
        """Heavy conflicts + old age still produce a score >= 0."""
        old = datetime.now(timezone.utc) - timedelta(days=9999)
        score = engine.score_fact({
            "source_type": "unknown",
            "created_at": old,
            "conflict_count": 10,
        })
        assert score == 0.0

    def test_score_clamps_at_one(self, engine):
        """Perfect conditions cap at 1.0."""
        fresh = datetime.now(timezone.utc)
        score = engine.score_fact({
            "source_type": "verified",
            "created_at": fresh,
            "corroboration_count": 5,
            "conflict_count": 0,
        })
        assert 0.9 <= score <= 1.0

    # --- source reliability ---

    def test_source_reliability_order(self, engine):
        """verified > admin > inferred > extracted > unknown."""
        scores = {
            src: engine.score_fact({"source_type": src})
            for src in ["verified", "admin", "inferred", "extracted", "unknown"]
        }
        assert scores["verified"] > scores["admin"]
        assert scores["admin"] > scores["inferred"]
        assert scores["inferred"] > scores["extracted"]
        assert scores["extracted"] > scores["unknown"]

    def test_default_source_is_unknown(self, engine):
        """Omitting source_type defaults to 'unknown'."""
        score = engine.score_fact({})
        unknown_score = engine.score_fact({"source_type": "unknown"})
        assert score == unknown_score

    def test_custom_source_reliability(self):
        """Custom reliability weights override defaults."""
        custom = ConfidenceEngine(source_reliability={"custom": 0.95})
        score = custom.score_fact({"source_type": "custom"})
        assert score == pytest.approx(0.95 * 1.0, abs=0.01)

    # --- age decay ---

    def test_age_decay_fresh(self, engine):
        """Fresh fact (now) gets no decay penalty."""
        fresh = datetime.now(timezone.utc)
        score = engine.score_fact({"source_type": "verified", "created_at": fresh})
        # base 0.9 * 1.0 age factor
        assert score == pytest.approx(0.9, abs=0.02)

    def test_age_decay_old(self, engine):
        """Very old fact is decayed toward 0.3 minimum age factor."""
        old = datetime.now(timezone.utc) - timedelta(days=365 * 10)
        score = engine.score_fact({"source_type": "verified", "created_at": old})
        # base 0.9 * 0.3 (age floor) = 0.27
        # actual: 0.9 * max(0.3, 2^(-3650/90)) = 0.9 * 0.3 = 0.27
        assert score == pytest.approx(0.27, abs=0.02)

    def test_age_decay_monotonic(self, engine):
        """Older facts never score higher than fresher ones (same params)."""
        now = datetime.now(timezone.utc)
        scores = []
        for days in [0, 10, 30, 90, 180]:
            created = now - timedelta(days=days)
            scores.append(
                engine.score_fact({
                    "source_type": "verified",
                    "created_at": created,
                })
            )
        for i in range(len(scores) - 1):
            assert scores[i] >= scores[i + 1], (
                f"Aged fact scored higher at index {i}"
            )

    # --- corroboration ---

    def test_corroboration_no_facts(self, engine):
        """Empty list returns 0."""
        assert engine.corroboration([]) == 0.0

    def test_corroboration_single(self, engine):
        """Single fact → 0 corroboration."""
        facts = [{"subject": "S", "predicate": "P", "object": "O", "source": "a"}]
        assert engine.corroboration(facts) == 0.0

    def test_corroboration_two_same_spo(self, engine):
        """Two facts with same SPO → 0.5."""
        facts = [
            {"subject": "S", "predicate": "P", "object": "O", "source": "a"},
            {"subject": "S", "predicate": "P", "object": "O", "source": "b"},
        ]
        assert engine.corroboration(facts) == 0.5

    def test_corroboration_three_same_spo(self, engine):
        """Three+ same SPO → 1.0."""
        facts = [
            {"subject": "S", "predicate": "P", "object": "O", "source": "a"},
            {"subject": "S", "predicate": "P", "object": "O", "source": "b"},
            {"subject": "S", "predicate": "P", "object": "O", "source": "c"},
        ]
        assert engine.corroboration(facts) == 1.0

    def test_corroboration_different_spo(self, engine):
        """Different SPO → no corroboration."""
        facts = [
            {"subject": "A", "predicate": "P", "object": "O", "source": "a"},
            {"subject": "B", "predicate": "P", "object": "O", "source": "b"},
        ]
        assert engine.corroboration(facts) == 0.0

    def test_corroboration_boost_applied(self, engine):
        """Corroboration_count boosts the final score."""
        base = engine.score_fact({"source_type": "unknown", "created_at": None})
        boosted = engine.score_fact({
            "source_type": "unknown",
            "corroboration_count": 3,
        })
        assert boosted > base

    # --- conflict detection ---

    def test_conflict_detection_no_conflicts(self, engine):
        """No conflicts for identical facts."""
        facts = [
            {"subject": "S", "predicate": "P", "object": "O"},
            {"subject": "S", "predicate": "P", "object": "O"},
        ]
        assert engine.conflict_detection(facts) == []

    def test_conflict_detection_finds(self, engine):
        """Conflicting object for same subject+predicate."""
        facts = [
            {"subject": "S", "predicate": "P", "object": "O1"},
            {"subject": "S", "predicate": "P", "object": "O2"},
        ]
        conflicts = engine.conflict_detection(facts)
        assert len(conflicts) == 1
        assert conflicts[0] == (0, 1)

    def test_conflict_detection_skips_different_subject(self, engine):
        """Different subjects are not conflicts."""
        facts = [
            {"subject": "A", "predicate": "P", "object": "O1"},
            {"subject": "B", "predicate": "P", "object": "O1"},
        ]
        assert engine.conflict_detection(facts) == []

    def test_conflict_detection_multi(self, engine):
        """Multiple conflict pairs."""
        facts = [
            {"subject": "S", "predicate": "P", "object": "O1"},
            {"subject": "S", "predicate": "P", "object": "O2"},
            {"subject": "S", "predicate": "P", "object": "O3"},
        ]
        conflicts = engine.conflict_detection(facts)
        assert len(conflicts) == 3

    def test_conflict_penalty_applied(self, engine):
        """Conflict_count lowers the final score."""
        base = engine.score_fact({"source_type": "verified"})
        penalised = engine.score_fact({
            "source_type": "verified",
            "conflict_count": 2,
        })
        assert penalised < base
