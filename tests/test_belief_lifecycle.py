"""Tests for belief lifecycle integration (Card 001).

Tests the lifecycle state transitions, decay, and LIFECYCLE_MULTIPLIER
integration for belief-specific states.
"""

from datetime import datetime, timedelta, timezone

import pytest

from memory_server.evaluation.confidence import LIFECYCLE_MULTIPLIER
from memory_server.evaluation.decay import PER_TYPE_TTL, DecayEngine
from memory_server.evaluation.validator import (
    _VALID_TRANSITIONS,
    Validator as EvValidator,
    is_valid_transition,
    normalize_lifecycle_state,
)
from memory_server.models.receipt import LifecycleState

# TTL in hours for testing
ONE_HOUR = 1.0 / 24.0
TWO_HOURS = 2.0 / 24.0


@pytest.fixture
def engine() -> DecayEngine:
    return DecayEngine()


class TestLifecycleStateEnum:
    """LifecycleState enum must include belief-specific states."""

    def test_belief_states_present(self):
        assert hasattr(LifecycleState, "SUPERSEDED")
        assert hasattr(LifecycleState, "CONTRADICTED")
        assert hasattr(LifecycleState, "DISCARDED")

    def test_belief_state_values(self):
        assert LifecycleState.SUPERSEDED.value == "superseded"
        assert LifecycleState.CONTRADICTED.value == "contradicted"
        assert LifecycleState.DISCARDED.value == "discarded"

    def test_belief_state_enum_members(self):
        """Belief-specific states are members of the enum."""
        assert "superseded" in LifecycleState._value2member_map_
        assert "contradicted" in LifecycleState._value2member_map_
        assert "discarded" in LifecycleState._value2member_map_


class TestLifecycleTransitions:
    """Transition matrix from validator must include belief-specific rules."""

    def test_active_to_superseded(self):
        assert is_valid_transition("active", "superseded")

    def test_active_to_contradicted(self):
        assert is_valid_transition("active", "contradicted")

    def test_active_to_discarded(self):
        assert is_valid_transition("active", "discarded")

    def test_superseded_to_stale(self):
        assert is_valid_transition("superseded", "stale")

    def test_superseded_to_discarded(self):
        assert is_valid_transition("superseded", "discarded")

    def test_contradicted_to_active(self):
        assert is_valid_transition("contradicted", "active")

    def test_contradicted_to_stale(self):
        assert is_valid_transition("contradicted", "stale")

    def test_contradicted_to_discarded(self):
        assert is_valid_transition("contradicted", "discarded")

    def test_discarded_to_archived(self):
        assert is_valid_transition("discarded", "archived")

    def test_forward_only_guarantee(self):
        """Belief states follow the lifecycle forward."""
        # Cannot go backward
        assert not is_valid_transition("superseded", "active")
        assert not is_valid_transition("discarded", "active")
        assert not is_valid_transition("contradicted", "superseded")

    def test_valid_transitions_contains_belief_keys(self):
        assert "superseded" in _VALID_TRANSITIONS
        assert "contradicted" in _VALID_TRANSITIONS
        assert "discarded" in _VALID_TRANSITIONS

    def test_superseded_transition_set(self):
        assert _VALID_TRANSITIONS["superseded"] == {"stale", "discarded"}

    def test_contradicted_transition_set(self):
        assert _VALID_TRANSITIONS["contradicted"] == {"active", "stale", "discarded"}

    def test_discarded_transition_set(self):
        assert _VALID_TRANSITIONS["discarded"] == {"archived"}


class TestLifecycleMultiplier:
    """Belief-specific lifecycle multipliers in confidence engine."""

    def test_superseded_multiplier(self):
        assert LIFECYCLE_MULTIPLIER.get("superseded") == 0.3

    def test_contradicted_multiplier(self):
        assert LIFECYCLE_MULTIPLIER.get("contradicted") == 0.3

    def test_discarded_multiplier(self):
        assert LIFECYCLE_MULTIPLIER.get("discarded") == 0.0

    def test_active_multiplier(self):
        assert LIFECYCLE_MULTIPLIER.get("active") == 1.0

    def test_stale_multiplier(self):
        assert LIFECYCLE_MULTIPLIER.get("stale") == 0.6


class TestPerTypeTTL:
    """PER_TYPE_TTL must include belief type."""

    def test_belief_ttl(self):
        assert "belief" in PER_TYPE_TTL
        assert PER_TYPE_TTL["belief"] == 180.0

    def test_belief_ttl_value(self):
        assert PER_TYPE_TTL["belief"] == 180.0


class TestDecayEngineBelief:
    """DecayEngine tick() must handle belief-specific states."""

    def test_superseded_decays_to_stale(self):
        """A superseded belief at 70%+ TTL transitions to stale."""
        v = EvValidator()
        hour_engine = DecayEngine(
            per_type_ttl={"belief": ONE_HOUR},
            validator=v,
        )

        past = datetime.now(timezone.utc) - timedelta(hours=1.5)  # 150% of ONE_HOUR
        hour_engine.register(
            item_id="belief-1",
            item_type="belief",
            created_at=past,
            lifecycle_state="superseded",
        )
        # Manually set lifecycle state in the validator
        v.deprecate("belief-1", reason="Test superseded decay")

        new_state = hour_engine.tick("belief-1")
        assert new_state == "stale"

    def test_contradicted_decays_to_stale(self):
        """A contradicted belief at 70%+ TTL transitions to stale."""
        v = EvValidator()
        hour_engine = DecayEngine(
            per_type_ttl={"belief": ONE_HOUR},
            validator=v,
        )

        past = datetime.now(timezone.utc) - timedelta(hours=1.5)
        hour_engine.register(
            item_id="belief-2",
            item_type="belief",
            created_at=past,
            lifecycle_state="contradicted",
        )

        new_state = hour_engine.tick("belief-2")
        assert new_state == "stale", f"Expected stale, got {new_state}"

    def test_discarded_decays_to_archived(self):
        """A discarded belief at 100%+ TTL transitions to archived."""
        v = EvValidator()
        hour_engine = DecayEngine(
            per_type_ttl={"belief": ONE_HOUR},
            validator=v,
        )

        past = datetime.now(timezone.utc) - timedelta(hours=2)  # 200% of ONE_HOUR
        hour_engine.register(
            item_id="belief-3",
            item_type="belief",
            created_at=past,
            lifecycle_state="discarded",
        )

        new_state = hour_engine.tick("belief-3")
        assert new_state == "archived", f"Expected archived, got {new_state}"

    def test_active_belief_decays_normally(self):
        """An active belief can still decay to stale like other types."""
        v = EvValidator()
        hour_engine = DecayEngine(
            per_type_ttl={"belief": ONE_HOUR},
            validator=v,
        )

        past = datetime.now(timezone.utc) - timedelta(hours=1.5)  # 150% of ONE_HOUR
        hour_engine.register(
            item_id="belief-4",
            item_type="belief",
            created_at=past,
            lifecycle_state="active",
        )

        new_state = hour_engine.tick("belief-4")
        assert new_state == "stale", f"Expected stale, got {new_state}"

    def test_fresh_belief_no_transition(self):
        """A fresh belief should not transition."""
        v = EvValidator()
        hour_engine = DecayEngine(
            per_type_ttl={"belief": ONE_HOUR},
            validator=v,
        )

        now = datetime.now(timezone.utc)
        hour_engine.register(
            item_id="belief-5",
            item_type="belief",
            created_at=now,
            lifecycle_state="active",
        )

        new_state = hour_engine.tick("belief-5")
        assert new_state is None, f"Expected None, got {new_state}"

    def test_normalize_belief_state(self):
        """normalize_lifecycle_state should pass belief states through."""
        assert normalize_lifecycle_state("superseded") == "superseded"
        assert normalize_lifecycle_state("contradicted") == "contradicted"
        assert normalize_lifecycle_state("discarded") == "discarded"

    def test_register_belief_with_decay_engine(self, engine):
        """Registering a belief with DecayEngine works."""
        engine.register(
            item_id="belief-reg-1",
            item_type="belief",
            lifecycle_state="active",
        )
        assert engine.get_lifecycle_state("belief-reg-1") == "active"
