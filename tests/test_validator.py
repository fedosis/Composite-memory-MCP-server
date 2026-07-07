"""Tests for Validator (Card 023)."""

import pytest

from memory_server.evaluation.validator import Validator
from memory_server.models.receipt import VerificationStatus


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


class TestValidator:
    """Validator — verification lifecycle management."""

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
        validator.register("f1", initial_status=VerificationStatus.VALIDATED, confidence=0.8)
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
        assert result == VerificationStatus.VALIDATED
        status = registered_validator.get_status("fact-2")
        assert status["status"] == "validated"

    def test_validate_candidate_below_threshold(self, registered_validator):
        """Candidate with confidence < 0.7 stays candidate."""
        result = registered_validator.validate("fact-1")  # 0.5
        assert result == VerificationStatus.CANDIDATE
        status = registered_validator.get_status("fact-1")
        assert status["status"] == "candidate"

    def test_validate_low_confidence(self, registered_validator):
        """Very low confidence (0.3) stays candidate."""
        result = registered_validator.validate("fact-4")
        assert result == VerificationStatus.CANDIDATE

    def test_validate_already_validated(self, registered_validator):
        """Already validated stays validated."""
        registered_validator.validate("fact-2")  # promote to validated
        result = registered_validator.validate("fact-2")  # try again
        assert result == VerificationStatus.VALIDATED

    def test_validate_with_custom_threshold(self):
        """Custom validate threshold works."""
        v = Validator(validate_threshold=0.5)
        v.register("f1", confidence=0.5)
        result = v.validate("f1")
        assert result == VerificationStatus.VALIDATED

    # --- trust lifecycle: validated -> trusted ---

    def test_trust_validated_with_corroboration(self, registered_validator):
        """Validated with high confidence + corroboration → trusted."""
        registered_validator.validate("fact-3")  # 0.9 → validated
        registered_validator.set_corroboration_count("fact-3", 2)
        result = registered_validator.trust("fact-3")
        assert result == VerificationStatus.TRUSTED
        status = registered_validator.get_status("fact-3")
        assert status["status"] == "trusted"

    def test_trust_without_corroboration(self, registered_validator):
        """Validated but insufficient corroboration → stays validated."""
        registered_validator.validate("fact-3")  # 0.9 → validated
        result = registered_validator.trust("fact-3")  # corr=0
        assert result == VerificationStatus.VALIDATED

    def test_trust_not_validated(self, registered_validator):
        """Candidate cannot be trusted directly."""
        result = registered_validator.trust("fact-1")  # still candidate
        assert result == VerificationStatus.CANDIDATE

    def test_trust_with_corroboration_below_min(self, registered_validator):
        """Corroboration < 2 prevents trust."""
        registered_validator.validate("fact-3")
        registered_validator.set_corroboration_count("fact-3", 1)
        result = registered_validator.trust("fact-3")
        assert result == VerificationStatus.VALIDATED

    # --- deprecate lifecycle: * -> deprecated ---

    def test_deprecate_candidate(self, registered_validator):
        """Candidate can be deprecated."""
        result = registered_validator.deprecate("fact-1", reason="Outdated")
        assert result == VerificationStatus.DEPRECATED
        status = registered_validator.get_status("fact-1")
        assert status["status"] == "deprecated"
        assert status["history"][-1]["note"] == "Outdated"

    def test_deprecate_already_deprecated(self, registered_validator):
        """Already deprecated stays deprecated."""
        registered_validator.deprecate("fact-1")
        result = registered_validator.deprecate("fact-1")
        assert result == VerificationStatus.DEPRECATED

    def test_deprecate_archived(self, registered_validator):
        """Archived fact remains archived."""
        registered_validator.deprecate("fact-1")
        registered_validator.archive("fact-1")
        result = registered_validator.deprecate("fact-1")
        assert result == VerificationStatus.ARCHIVED

    # --- archive lifecycle: deprecated -> archived ---

    def test_archive_deprecated(self, registered_validator):
        """Deprecated fact can be archived."""
        registered_validator.deprecate("fact-1")
        result = registered_validator.archive("fact-1")
        assert result == VerificationStatus.ARCHIVED
        status = registered_validator.get_status("fact-1")
        assert status["status"] == "archived"

    def test_archive_not_deprecated(self, registered_validator):
        """Non-deprecated fact cannot be archived."""
        result = registered_validator.archive("fact-2")
        # fact-2 is still CANDIDATE — no transition
        assert result == VerificationStatus.CANDIDATE

    # --- full lifecycle ---

    def test_full_lifecycle(self, validator):
        """candidate → validated → trusted → deprecated → archived."""
        validator.register("f1", confidence=0.9)
        assert validator.get_status("f1")["status"] == "candidate"

        validator.set_corroboration_count("f1", 2)
        validator.validate("f1")
        assert validator.get_status("f1")["status"] == "validated"

        validator.trust("f1")
        assert validator.get_status("f1")["status"] == "trusted"

        validator.deprecate("f1", reason="Superseded")
        assert validator.get_status("f1")["status"] == "deprecated"

        validator.archive("f1")
        assert validator.get_status("f1")["status"] == "archived"

        # Verify history tracks all steps
        status = validator.get_status("f1")
        assert len(status["history"]) == 5  # created → validated → trusted → deprecated → archived

    # --- get_all ---

    def test_get_all(self, registered_validator):
        """get_all returns all registered entries."""
        all_entries = registered_validator.get_all()
        assert len(all_entries) == 4
        fact_ids = {e["fact_id"] for e in all_entries}
        assert fact_ids == {"fact-1", "fact-2", "fact-3", "fact-4"}
