"""Tests for DecayEngine (Card 024) — v0.6 Lifecycle State transitions.

Note: Tests use small hour-based TTLs for quick verification.
"""

import logging
from datetime import datetime, timedelta, timezone

import pytest

from memory_server.evaluation.decay import PER_TYPE_TTL, DecayEngine
from memory_server.evaluation.validator import Validator as EvValidator

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


@pytest.fixture
def validator_engine() -> DecayEngine:
    """Engine with a shared validator for lifecycle transition testing."""
    v = EvValidator()
    return DecayEngine(
        per_type_ttl={
            "fact": ONE_HOUR,
        },
        validator=v,
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

    # ================================================================
    # Lifecycle state transitions (v0.6)
    # ================================================================

    # --- active → stale (after 70% of TTL) ---

    def test_tick_active_to_stale(self, validator_engine):
        """Active item at 70%+ TTL transitions to stale."""
        past = datetime.now(timezone.utc) - timedelta(hours=0.75)  # 75% of ONE_HOUR
        validator_engine.register("f1", "fact", past, lifecycle_state="active")
        new_state = validator_engine.tick("f1")
        assert new_state == "stale"
        assert validator_engine.get_lifecycle_state("f1") == "stale"
        # Validator should reflect it too
        status = validator_engine._validator.get_status("f1")
        assert status["status"] == "stale"

    def test_tick_active_below_70(self, validator_engine):
        """Active item below 70% TTL stays active."""
        past = datetime.now(timezone.utc) - timedelta(hours=0.5)  # 50% of ONE_HOUR
        validator_engine.register("f1", "fact", past, lifecycle_state="active")
        new_state = validator_engine.tick("f1")
        assert new_state is None
        assert validator_engine.get_lifecycle_state("f1") == "active"

    # --- stale → archived (after 100% of TTL) ---

    def test_tick_stale_to_archived(self, validator_engine):
        """Stale item at 100%+ TTL transitions to archived."""
        past = datetime.now(timezone.utc) - timedelta(hours=1.5)  # 150% of ONE_HOUR
        validator_engine.register("f1", "fact", past, lifecycle_state="stale")
        new_state = validator_engine.tick("f1")
        assert new_state == "archived"
        assert validator_engine.get_lifecycle_state("f1") == "archived"

    def test_tick_stale_below_100(self, validator_engine):
        """Stale item below 100% TTL stays stale."""
        past = datetime.now(timezone.utc) - timedelta(hours=0.5)  # 50% of ONE_HOUR
        validator_engine.register("f1", "fact", past, lifecycle_state="stale")
        new_state = validator_engine.tick("f1")
        assert new_state is None

    # --- archived → forgotten (after 200% of TTL) ---

    def test_tick_archived_to_forgotten(self, validator_engine):
        """Archived item at 200%+ TTL transitions to forgotten."""
        past = datetime.now(timezone.utc) - timedelta(hours=3.0)  # 300% of ONE_HOUR
        validator_engine.register("f1", "fact", past, lifecycle_state="archived")
        new_state = validator_engine.tick("f1")
        assert new_state == "forgotten"
        assert validator_engine.get_lifecycle_state("f1") == "forgotten"

    def test_tick_archived_at_150(self, validator_engine):
        """Archived item at 150% TTL stays archived (< 200%)."""
        past = datetime.now(timezone.utc) - timedelta(hours=1.5)  # 150% of ONE_HOUR
        validator_engine.register("f1", "fact", past, lifecycle_state="archived")
        new_state = validator_engine.tick("f1")
        assert new_state is None

    # --- tick_all ---

    def test_tick_all_mixed(self, validator_engine):
        """tick_all transitions items at the right thresholds."""
        now = datetime.now(timezone.utc)
        # Fresh active item — no transition
        validator_engine.register("fresh", "fact", now, lifecycle_state="active")
        # Active beyond 70% — should go stale
        past_stale = now - timedelta(hours=0.75)
        validator_engine.register("aging", "fact", past_stale, lifecycle_state="active")
        # Already stale beyond 100% — should go archived
        past_archived = now - timedelta(hours=1.5)
        validator_engine.register("old", "fact", past_archived, lifecycle_state="stale")

        transitions = validator_engine.tick_all()
        trans_map = {t["id"]: t["new_state"] for t in transitions}

        assert "fresh" not in trans_map
        assert trans_map["aging"] == "stale"
        assert trans_map["old"] == "archived"

    # --- update_lifecycle_state ---

    def test_update_lifecycle_state(self, engine):
        """update_lifecycle_state updates stored state."""
        engine.register("f1", "fact")
        assert engine._items["f1"]["lifecycle_state"] == "active"
        engine.update_lifecycle_state("f1", "stale")
        assert engine._items["f1"]["lifecycle_state"] == "stale"

    def test_update_lifecycle_state_nonexistent(self, engine):
        """Updating lifecycle state for unregistered item does nothing."""
        engine.update_lifecycle_state("nonexistent", "stale")  # no error

    # --- get_lifecycle_state ---

    def test_get_lifecycle_state(self, validator_engine):
        """get_lifecycle_state returns authoritative state from validator."""
        past = datetime.now(timezone.utc) - timedelta(hours=0.75)
        validator_engine.register("f1", "fact", past, lifecycle_state="active")
        # Before tick
        assert validator_engine.get_lifecycle_state("f1") == "active"
        # After tick
        validator_engine.tick("f1")
        assert validator_engine.get_lifecycle_state("f1") == "stale"

    def test_get_lifecycle_state_nonexistent(self, engine):
        """get_lifecycle_state for unregistered item returns None."""
        assert engine.get_lifecycle_state("nonexistent") is None

    # --- tick with unregistered item ---

    def test_tick_unregistered(self, engine):
        """tick on unregistered item returns None."""
        assert engine.tick("nonexistent") is None


class TestDecayTransitionFailures:
    """Card 2, D8: cumulative transition_failures counter + logged skips."""

    def test_counter_starts_zero_and_is_readonly(self, engine):
        assert engine.transition_failures == 0
        with pytest.raises(AttributeError):
            engine.transition_failures = 5

    def test_active_to_stale_failure_then_success_then_failure(
        self, validator_engine, monkeypatch, caplog
    ):
        """Branch 1: fail → 1, successful tick keeps 1, second failure → 2."""
        caplog.set_level(logging.WARNING, logger="memory_server.evaluation.decay")
        past = datetime.now(timezone.utc) - timedelta(hours=0.75)
        validator_engine.register("f1", "fact", past, lifecycle_state="active")

        original = validator_engine._validator.mark_stale
        state = {"fail": True}

        def flaky_mark_stale(*args, **kwargs):
            if state["fail"]:
                raise ValueError("validator refused (injected)")
            return original(*args, **kwargs)

        monkeypatch.setattr(validator_engine._validator, "mark_stale", flaky_mark_stale)

        # 1st failure: tick returns None, counter 1, log has id/state/reason.
        assert validator_engine.tick("f1") is None
        assert validator_engine.transition_failures == 1
        assert "decay transition skipped" in caplog.text
        assert "f1" in caplog.text
        assert "active" in caplog.text
        assert "Decay:" in caplog.text  # reason is the pre-try local

        # Successful tick: transitions, counter stays 1 (no reset on success).
        state["fail"] = False
        assert validator_engine.tick("f1") == "stale"
        assert validator_engine.transition_failures == 1

        # 2nd failure (new active item): counter → 2.
        validator_engine.register("f2", "fact", past, lifecycle_state="active")
        state["fail"] = True
        assert validator_engine.tick("f2") is None
        assert validator_engine.transition_failures == 2

    def test_stale_to_archived_failure_then_success_then_failure(
        self, validator_engine, monkeypatch, caplog
    ):
        """Branch 5: hoisted reason local, KeyError path, cumulative counter."""
        caplog.set_level(logging.WARNING, logger="memory_server.evaluation.decay")
        past = datetime.now(timezone.utc) - timedelta(hours=1.5)
        validator_engine.register("f1", "fact", past, lifecycle_state="stale")

        original = validator_engine._validator.archive
        state = {"fail": True}

        def flaky_archive(*args, **kwargs):
            if state["fail"]:
                raise KeyError("validator refused (injected)")
            return original(*args, **kwargs)

        monkeypatch.setattr(validator_engine._validator, "archive", flaky_archive)

        # 1st failure: counter 1, reason from the hoisted pre-try local
        # (no UnboundLocalError), id + state in the log.
        assert validator_engine.tick("f1") is None
        assert validator_engine.transition_failures == 1
        assert "decay transition skipped" in caplog.text
        assert "f1" in caplog.text
        assert "stale" in caplog.text
        assert "TTL expired:" in caplog.text

        # Successful tick: counter stays 1.
        state["fail"] = False
        assert validator_engine.tick("f1") == "archived"
        assert validator_engine.transition_failures == 1

        # 2nd failure (new stale item): counter → 2.
        validator_engine.register("f2", "fact", past, lifecycle_state="stale")
        state["fail"] = True
        assert validator_engine.tick("f2") is None
        assert validator_engine.transition_failures == 2


class TestDecayNaiveUtcRoundTrip:
    """ROUTE-1: naive timestamps (SQLite round trip) are treated as UTC.

    Regression: ``now - naive`` raised TypeError, so decay/get_expired/tick
    crashed on any item read back from SQLite.
    """

    def test_decay_naive_equals_aware(self, hour_engine):
        aware_past = datetime.now(timezone.utc) - timedelta(minutes=30)
        naive_past = aware_past.replace(tzinfo=None)  # what SQLite returns

        aware_item = {"type": "fact", "created_at": aware_past, "confidence": 1.0}
        naive_item = {"type": "fact", "created_at": naive_past, "confidence": 1.0}

        aware_score = hour_engine.decay(aware_item)
        naive_score = hour_engine.decay(naive_item)
        assert naive_score == pytest.approx(aware_score)
        assert 0.0 < naive_score < 1.0

    def test_get_expired_with_naive_created_at(self, hour_engine):
        past = (datetime.now(timezone.utc) - timedelta(hours=2)).replace(
            tzinfo=None
        )
        hour_engine.register("naive-expired", "fact", past, lifecycle_state="active")
        expired = hour_engine.get_expired()
        ids = [e["id"] for e in expired]
        assert "naive-expired" in ids

    def test_get_expired_aware_and_naive_agree(self, hour_engine):
        aware_past = datetime.now(timezone.utc) - timedelta(hours=2)
        hour_engine.register("aware-expired", "fact", aware_past, "active")
        hour_engine.register(
            "naive-expired", "fact", aware_past.replace(tzinfo=None), "active"
        )
        ids = {e["id"] for e in hour_engine.get_expired()}
        assert {"aware-expired", "naive-expired"} <= ids

    def test_tick_with_naive_created_at(self, validator_engine):
        past = (datetime.now(timezone.utc) - timedelta(minutes=45)).replace(
            tzinfo=None
        )
        validator_engine.register("f", "fact", past, lifecycle_state="active")
        # 45 min >= 70% of the 1h TTL -> active -> stale
        assert validator_engine.tick("f") == "stale"

    def test_should_archive_with_naive_created_at(self, hour_engine):
        past = (datetime.now(timezone.utc) - timedelta(hours=2)).replace(
            tzinfo=None
        )
        item = {"type": "fact", "id": "f", "created_at": past, "confidence": 1.0}
        assert hour_engine.should_archive(item) is True
