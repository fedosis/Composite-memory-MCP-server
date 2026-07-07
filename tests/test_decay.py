"""Tests for DecayEngine (Card 024).

Note: Tests use small hour-based TTLs for quick verification.
"""

from datetime import datetime, timedelta, timezone

import pytest

from memory_server.evaluation.decay import PER_TYPE_TTL, DecayEngine

# TTLs in hours for testing — passed as days to DecayEngine
# (decay engine works in days, so 1/24 day = 1 hour)
ONE_HOUR = 1.0 / 24.0
TWO_HOURS = 2.0 / 24.0
THREE_HOURS = 3.0 / 24.0


@pytest.fixture
def engine() -> DecayEngine:
    return DecayEngine()


@pytest.fixture
def hour_engine() -> DecayEngine:
    """Engine with hour-based TTLs for fast testing."""
    return DecayEngine(
        per_type_ttl={
            "fact": ONE_HOUR,
            "decision": TWO_HOURS,
            "skill": THREE_HOURS,
            "entity": THREE_HOURS,
        },
    )


class TestDecayEngine:
    """DecayEngine — time-based confidence decay and TTL expiration."""

    # --- decay curve ---

    def test_decay_fresh(self, engine):
        """Fresh item (now) suffers no decay."""
        now = datetime.now(timezone.utc)
        score = engine.decay({
            "type": "fact",
            "created_at": now,
            "confidence": 1.0,
        })
        # 2^(-0/90) = 1.0, so 1.0 * 1.0 = 1.0
        assert score == pytest.approx(1.0, abs=0.01)

    def test_decay_partial(self, hour_engine):
        """Item at half-TTL has decayed but > 0."""
        past = datetime.now(timezone.utc) - timedelta(hours=0.5)  # half of ONE_HOUR
        score = hour_engine.decay({
            "type": "fact",
            "created_at": past,
            "confidence": 1.0,
        })
        # 2^(-0.5/1) = 2^(-0.5) ≈ 0.707
        assert score == pytest.approx(0.707, abs=0.02)
        assert 0.5 < score < 1.0

    def test_decay_zero_confidence(self, engine):
        """Zero confidence stays zero."""
        past = datetime.now(timezone.utc) - timedelta(days=10)
        score = engine.decay({
            "type": "fact",
            "created_at": past,
            "confidence": 0.0,
        })
        assert score == 0.0

    def test_decay_very_old(self, hour_engine):
        """Very old item hits the 0.1 floor."""
        past = datetime.now(timezone.utc) - timedelta(days=365)
        score = hour_engine.decay({
            "type": "fact",
            "created_at": past,
            "confidence": 1.0,
        })
        # Should be at floor of 0.1
        assert score == pytest.approx(0.1, abs=0.01)

    def test_decay_monotonic(self, engine):
        """Older items never score higher than newer ones."""
        now = datetime.now(timezone.utc)
        scores = []
        for days in [0, 10, 30, 90, 180]:
            created = now - timedelta(days=days)
            scores.append(
                engine.decay({
                    "type": "fact",
                    "created_at": created,
                    "confidence": 1.0,
                })
            )
        for i in range(len(scores) - 1):
            assert scores[i] >= scores[i + 1], f"Not monotonic at index {i}"

    # --- TTL expiration ---

    def test_get_expired_empty(self, engine):
        """No registered items → empty."""
        assert engine.get_expired() == []

    def test_get_expired_fresh(self, hour_engine):
        """Freshly registered items are not expired."""
        now = datetime.now(timezone.utc)
        hour_engine.register("f1", "fact", now)
        assert hour_engine.get_expired() == []

    def test_get_expired_past_ttl(self, hour_engine):
        """Item older than TTL is expired."""
        past = datetime.now(timezone.utc) - timedelta(hours=2)  # > ONE_HOUR
        hour_engine.register("f1", "fact", past)
        expired = hour_engine.get_expired()
        assert len(expired) == 1
        assert expired[0]["id"] == "f1"

    def test_get_expired_mixed(self, hour_engine):
        """Some expired, some not."""
        now = datetime.now(timezone.utc)
        past = now - timedelta(hours=3)
        hour_engine.register("fresh", "fact", now)
        hour_engine.register("old", "fact", past)
        expired = hour_engine.get_expired()
        ids = {e["id"] for e in expired}
        assert "old" in ids
        assert "fresh" not in ids

    def test_per_type_ttl_values(self):
        """Default TTL values are reasonable."""
        assert PER_TYPE_TTL["fact"] == 90.0
        assert PER_TYPE_TTL["decision"] == 180.0
        assert PER_TYPE_TTL["skill"] == 365.0
        assert PER_TYPE_TTL["entity"] == 365.0

    # --- archive threshold ---

    def test_should_archive_below_threshold(self, engine):
        """Confidence below threshold → should archive."""
        past = datetime.now(timezone.utc) - timedelta(days=500)
        result = engine.should_archive({
            "type": "fact",
            "created_at": past,
            "confidence": 1.0,
        })
        # Very old → decayed below threshold OR past TTL
        assert result is True

    def test_should_not_archive_fresh(self, engine):
        """Fresh high-confidence → should not archive."""
        result = engine.should_archive({
            "type": "fact",
            "created_at": datetime.now(timezone.utc),
            "confidence": 0.9,
        })
        assert result is False

    def test_should_archive_exceeded_ttl(self, hour_engine):
        """Age exceeds TTL → should archive even with high confidence."""
        past = datetime.now(timezone.utc) - timedelta(hours=2)  # > ONE_HOUR
        result = hour_engine.should_archive({
            "type": "fact",
            "created_at": past,
            "confidence": 0.9,
        })
        assert result is True

    def test_should_archive_none(self, engine):
        """None item → should not archive."""
        assert engine.should_archive(None) is False

    def test_should_archive_empty(self, engine):
        """Empty dict → should not archive (no created_at)."""
        assert engine.should_archive({}) is False

    def test_custom_archive_threshold(self):
        """Custom threshold is used."""
        past = datetime.now(timezone.utc) - timedelta(days=1)
        engine = DecayEngine(archive_threshold=0.9)
        result = engine.should_archive({
            "type": "fact",
            "created_at": past,
            "confidence": 0.85,
        })
        assert result is True  # below 0.9 threshold

    # --- registration ---

    def test_register_and_update(self, engine):
        """Register then update confidence."""
        engine.register("f1", "fact")
        engine.update_confidence("f1", 0.5)
        assert engine._items["f1"]["confidence"] == 0.5

    def test_update_nonexistent(self, engine):
        """Updating confidence for unregistered item does nothing."""
        engine.update_confidence("nonexistent", 0.5)  # no error

    # --- get_ttl ---

    def test_get_ttl(self, engine):
        """get_ttl returns correct value."""
        assert engine.get_ttl("fact") == 90.0
        assert engine.get_ttl("decision") == 180.0
        assert engine.get_ttl("nonexistent") == 90.0  # default
