"""Tests for Validator (Card 023) — v0.6 Lifecycle State Engine.

Tests cover:
- Every valid transition works
- Every invalid transition is rejected
- active→stale via decay
- stale→archived via TTL
- archived→forgotten via extended TTL
- Backward compatibility (old lifecycle values still work)
"""

import pytest

from memory_server.evaluation.validator import (
    Validator,
    is_valid_transition,
    normalize_lifecycle_state,
)
from memory_server.models.receipt import LifecycleState, VerificationStatus


@pytest.fixture
def validator() -> Validator:
    return Validator()


@pytest.fixture
def registered_validator() -> Validator:
    v = Validator()
    v.register("fact-1", confidence=0.5)
    v.register("fact-2", confidence=0.75)
    v.register("fact-3", confidence=0.9)
    v.register("fact-4", confidence=0.3)
    return v


class TestTransitionRules:
    """is_valid_transition — transition validation logic."""

    def test_valid_transitions(self):
        """All valid forward transitions are accepted."""
        assert is_valid_transition("candidate", "validated")
        assert is_valid_transition("validated", "active")
        assert is_valid_transition("active", "stale")
        assert is_valid_transition("stale", "archived")
        assert is_valid_transition("archived", "forgotten")

    def test_invalid_transitions_skipping_states(self):
        """Skipping states is rejected."""
        assert not is_valid_transition("candidate", "active")
        assert not is_valid_transition("candidate", "stale")
        assert not is_valid_transition("candidate", "archived")
        assert not is_valid_transition("candidate", "forgotten")
        assert not is_valid_transition("validated", "stale")
        assert not is_valid_transition("validated", "archived")
        assert not is_valid_transition("validated", "forgotten")
        assert not is_valid_transition("active", "archived")
        assert not is_valid_transition("active", "forgotten")
        assert not is_valid_transition("stale", "forgotten")

    def test_terminal_state(self):
        """Forgotten is terminal — no transitions from it."""
        assert not is_valid_transition("forgotten", "candidate")
        assert not is_valid_transition("forgotten", "validated")
        assert not is_valid_transition("forgotten", "active")
        assert not is_valid_transition("forgotten", "stale")
        assert not is_valid_transition("forgotten", "archived")

    def test_backward_transitions_rejected(self):
        """All backward transitions are rejected."""
        assert not is_valid_transition("validated", "candidate")
        assert not is_valid_transition("active", "validated")
        assert not is_valid_transition("active", "candidate")
        assert not is_valid_transition("stale", "active")
        assert not is_valid_transition("archived", "stale")
        assert not is_valid_transition("forgotten", "archived")

    def test_unknown_state(self):
        """Unknown states have no valid transitions."""
        assert not is_valid_transition("unknown", "candidate")

    def test_backward_compat_old_values(self):
        """Old values (trusted, deprecated) normalize correctly."""
        assert normalize_lifecycle_state("trusted") == "active"
        assert normalize_lifecycle_state("deprecated") == "stale"

    def test_backward_compat_in_transitions(self):
        """Transitions using old values work through normalization."""
        assert is_valid_transition("trusted", "stale")  # trusted → active → stale ✓
        assert is_valid_transition("deprecated", "archived")  # deprecated → stale → archived ✓
        assert not is_valid_transition("trusted", "candidate")  # backward ✗


class TestValidator:
    """Validator — v0.6 lifecycle management."""

    # --- register ---

    def test_register_default_candidate(self, validator):
        """Registering without status defaults to CANDIDATE."""
        validator.register("f1")
        status = validator.get_status("f1")
        assert status["status"] == "candidate"
        assert status["confidence"] == 0.5
        assert len(status["history"]) == 1
        assert status["history"][0]["note"] == "Created"

    def test_register_with_status(self, validator):
        """Registering with a specific status."""
        validator.register("f1", initial_status=LifecycleState.VALIDATED, confidence=0.8)
        status = validator.get_status("f1")
        assert status["status"] == "validated"
        assert status["confidence"] == 0.8

    def test_get_status_not_found(self, validator):
        """Getting status for unregistered fact raises KeyError."""
        with pytest.raises(KeyError, match="not registered"):
            validator.get_status("nonexistent")

    # --- validate lifecycle: candidate -> validated ---

    def test_validate_candidate_above_threshold(self, registered_validator):
        """Candidate with confidence >= 0.7 is promoted to validated."""
        result = registered_validator.validate("fact-2")  # 0.75
        assert result == LifecycleState.VALIDATED
        status = registered_validator.get_status("fact-2")
        assert status["status"] == "validated"

    def test_validate_candidate_below_threshold(self, registered_validator):
        """Candidate with confidence < 0.7 stays candidate."""
        result = registered_validator.validate("fact-1")  # 0.5
        assert result == LifecycleState.CANDIDATE
        status = registered_validator.get_status("fact-1")
        assert status["status"] == "candidate"

    def test_validate_low_confidence(self, registered_validator):
        """Very low confidence (0.3) stays candidate."""
        result = registered_validator.validate("fact-4")
        assert result == LifecycleState.CANDIDATE

    def test_validate_already_validated(self, registered_validator):
        """Already validated stays validated."""
        registered_validator.validate("fact-2")  # promote to validated
        result = registered_validator.validate("fact-2")  # try again
        assert result == LifecycleState.VALIDATED

    def test_validate_with_custom_threshold(self):
        """Custom validate threshold works."""
        v = Validator(validate_threshold=0.5)
        v.register("f1", confidence=0.5)
        result = v.validate("f1")
        assert result == LifecycleState.VALIDATED

    # --- activate lifecycle: validated -> active ---

    def test_activate_validated_with_corroboration(self, registered_validator):
        """Validated with high confidence + corroboration → active."""
        registered_validator.validate("fact-3")  # 0.9 → validated
        registered_validator.set_corroboration_count("fact-3", 2)
        result = registered_validator.activate("fact-3")
        assert result == LifecycleState.ACTIVE
        status = registered_validator.get_status("fact-3")
        assert status["status"] == "active"

    def test_activate_without_corroboration(self, registered_validator):
        """Validated but insufficient corroboration → stays validated."""
        registered_validator.validate("fact-3")  # 0.9 → validated
        result = registered_validator.activate("fact-3")  # corr=0
        assert result == LifecycleState.VALIDATED

    def test_activate_not_validated(self, registered_validator):
        """Candidate cannot be activated directly."""
        result = registered_validator.activate("fact-1")  # still candidate
        assert result == LifecycleState.CANDIDATE

    def test_activate_with_corroboration_below_min(self, registered_validator):
        """Corroboration < 2 prevents activation."""
        registered_validator.validate("fact-3")
        registered_validator.set_corroboration_count("fact-3", 1)
        result = registered_validator.activate("fact-3")
        assert result == LifecycleState.VALIDATED

    # --- mark_stale lifecycle: active -> stale ---

    def test_mark_stale_active(self, registered_validator):
        """Active fact can be marked stale."""
        # Promote to active first
        registered_validator.validate("fact-3")
        registered_validator.set_corroboration_count("fact-3", 2)
        registered_validator.activate("fact-3")
        result = registered_validator.mark_stale("fact-3", reason="Decay test")
        assert result == LifecycleState.STALE
        status = registered_validator.get_status("fact-3")
        assert status["status"] == "stale"
        assert status["history"][-1]["note"] == "Decay test"

    def test_mark_stale_not_active(self, registered_validator):
        """Non-active fact cannot be marked stale."""
        with pytest.raises(KeyError):
            registered_validator.mark_stale("fact-nonexistent")
        result = registered_validator.mark_stale("fact-1")  # still candidate
        assert result == LifecycleState.CANDIDATE

    # --- archive lifecycle: stale -> archived ---

    def test_archive_stale(self, registered_validator):
        """Stale fact can be archived."""
        # Promote to stale
        registered_validator.validate("fact-3")
        registered_validator.set_corroboration_count("fact-3", 2)
        registered_validator.activate("fact-3")
        registered_validator.mark_stale("fact-3")
        result = registered_validator.archive("fact-3", reason="TTL test")
        assert result == LifecycleState.ARCHIVED
        status = registered_validator.get_status("fact-3")
        assert status["status"] == "archived"

    def test_archive_not_stale(self, registered_validator):
        """Non-stale fact cannot be archived."""
        result = registered_validator.archive("fact-2")  # still candidate
        assert result == LifecycleState.CANDIDATE

    # --- forget lifecycle: archived -> forgotten ---

    def test_forget_archived(self, registered_validator):
        """Archived fact can be forgotten."""
        # Promote through full chain
        registered_validator.validate("fact-3")
        registered_validator.set_corroboration_count("fact-3", 2)
        registered_validator.activate("fact-3")
        registered_validator.mark_stale("fact-3")
        registered_validator.archive("fact-3")
        result = registered_validator.forget("fact-3", reason="Extended TTL test")
        assert result == LifecycleState.FORGOTTEN
        status = registered_validator.get_status("fact-3")
        assert status["status"] == "forgotten"

    def test_forget_not_archived(self, registered_validator):
        """Non-archived fact cannot be forgotten."""
        result = registered_validator.forget("fact-1")  # still candidate
        assert result == LifecycleState.CANDIDATE

    def test_forget_terminal(self, validator):
        """Forgotten state is terminal."""
        validator.register("f1", initial_status=LifecycleState.FORGOTTEN)
        # Any further transition should raise ValueError (via _transition)
        with pytest.raises(ValueError, match="Invalid transition"):
            validator._transition("f1", LifecycleState.CANDIDATE, "test")

    # --- transition validation via _transition ---

    def test_invalid_transition_raises(self, validator):
        """Invalid transition raises ValueError."""
        validator.register("f1")
        # Candidate → active is invalid (must go through validated)
        with pytest.raises(ValueError, match="Invalid transition"):
            validator._transition("f1", LifecycleState.ACTIVE, "try skip")

    def test_valid_transition_succeeds(self, validator):
        """Valid forward transition works."""
        validator.register("f1", confidence=0.9)
        result = validator.validate("f1")
        assert result == LifecycleState.VALIDATED

    # --- deprecate (backward compat) ---

    def test_deprecate_candidate(self, registered_validator):
        """Candidate can be deprecated → stale."""
        result = registered_validator.deprecate("fact-1", reason="Outdated")
        assert result == LifecycleState.STALE
        status = registered_validator.get_status("fact-1")
        assert status["status"] == "stale"
        assert status["history"][-1]["note"] == "Outdated"

    def test_deprecate_already_stale(self, registered_validator):
        """Already stale stays stale."""
        registered_validator.deprecate("fact-1")
        result = registered_validator.deprecate("fact-1")
        assert result == LifecycleState.STALE

    def test_deprecate_archived(self, registered_validator):
        """Archived fact stays archived."""
        # Get to archived
        registered_validator.validate("fact-3")
        registered_validator.set_corroboration_count("fact-3", 2)
        registered_validator.activate("fact-3")
        registered_validator.mark_stale("fact-3")
        registered_validator.archive("fact-3")
        result = registered_validator.deprecate("fact-3")
        assert result == LifecycleState.ARCHIVED

    # --- trust (backward compat wrapper) ---

    def test_trust_backward_compat(self, registered_validator):
        """Old trust() method maps to activate()."""
        registered_validator.validate("fact-3")
        registered_validator.set_corroboration_count("fact-3", 2)
        result = registered_validator.trust("fact-3")
        assert result == LifecycleState.ACTIVE

    # --- full lifecycle ---

    def test_full_lifecycle(self, validator):
        """candidate → validated → active → stale → archived → forgotten."""
        validator.register("f1", confidence=0.9)
        assert validator.get_status("f1")["status"] == "candidate"

        validator.set_corroboration_count("f1", 2)
        validator.validate("f1")
        assert validator.get_status("f1")["status"] == "validated"

        validator.activate("f1")
        assert validator.get_status("f1")["status"] == "active"

        validator.mark_stale("f1", reason="Decay")
        assert validator.get_status("f1")["status"] == "stale"

        validator.archive("f1", reason="TTL")
        assert validator.get_status("f1")["status"] == "archived"

        validator.forget("f1", reason="Extended TTL")
        assert validator.get_status("f1")["status"] == "forgotten"

        # Verify history tracks all steps
        status = validator.get_status("f1")
        assert len(status["history"]) == 6  # created → validated → active → stale → archived → forgotten

    # --- get_all ---

    def test_get_all(self, registered_validator):
        """get_all returns all registered entries."""
        all_entries = registered_validator.get_all()
        assert len(all_entries) == 4
        fact_ids = {e["fact_id"] for e in all_entries}
        assert fact_ids == {"fact-1", "fact-2", "fact-3", "fact-4"}
