"""Unit tests for Belief and Evidence Pydantic models (Card 001)."""

import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from memory_server.models import Belief, Evidence


class TestBeliefModel:
    def test_minimal_construction(self):
        b = Belief(proposition="The sky is blue")
        assert b.proposition == "The sky is blue"
        assert b.confidence == 0.5  # default
        assert b.source == "system"  # default
        assert b.creator == "system"  # default
        assert b.source_ids == []
        assert b.tags == []
        assert isinstance(b.created_at, datetime)
        assert isinstance(b.updated_at, datetime)
        assert isinstance(b.last_reinforced_at, datetime)
        assert b.version == 1  # default
        assert b.verification_status == "candidate"
        assert b.lifecycle_state == "active"
        assert b.id is not None

    def test_full_construction(self):
        now = datetime.now(timezone.utc)
        b = Belief(
            proposition="Docker runs on OMV8",
            confidence=0.9,
            source="manual",
            creator="alice",
            source_ids=["fact-123", "fact-456"],
            tags=["docker", "deployment"],
            created_at=now,
            updated_at=now,
            last_reinforced_at=now,
            version=3,
            verification_status="validated",
            lifecycle_state="active",
        )
        assert b.proposition == "Docker runs on OMV8"
        assert b.confidence == 0.9
        assert b.source == "manual"
        assert b.creator == "alice"
        assert b.source_ids == ["fact-123", "fact-456"]
        assert b.tags == ["docker", "deployment"]
        assert b.version == 3
        assert b.verification_status == "validated"

    def test_confidence_range(self):
        b = Belief(proposition="Test", confidence=0.75)
        assert b.confidence == 0.75

    def test_confidence_out_of_range_high(self):
        with pytest.raises(ValidationError):
            Belief(proposition="Test", confidence=1.5)

    def test_confidence_out_of_range_low(self):
        with pytest.raises(ValidationError):
            Belief(proposition="Test", confidence=-0.1)

    def test_confidence_boundary(self):
        b1 = Belief(proposition="A", confidence=0.0)
        b2 = Belief(proposition="B", confidence=1.0)
        assert b1.confidence == 0.0
        assert b2.confidence == 1.0

    def test_proposition_min_length(self):
        with pytest.raises(ValidationError):
            Belief(proposition="")  # min_length=1

    def test_proposition_max_length(self):
        with pytest.raises(ValidationError):
            Belief(proposition="x" * 2049)  # max_length=2048

    def test_proposition_max_length_boundary(self):
        b = Belief(proposition="x" * 2048)
        assert len(b.proposition) == 2048

    def test_version_default(self):
        b = Belief(proposition="Test")
        assert b.version == 1

    def test_version_min_value(self):
        with pytest.raises(ValidationError):
            Belief(proposition="Test", version=0)  # ge=1

    def test_json_round_trip(self):
        now = datetime.now(timezone.utc)
        b = Belief(
            proposition="Test proposition",
            confidence=0.8,
            source="test",
            creator="tester",
            tags=["tag1", "tag2"],
            created_at=now,
        )
        data = json.loads(b.model_dump_json())
        restored = Belief.model_validate(data)
        assert restored.proposition == b.proposition
        assert restored.confidence == b.confidence
        assert restored.source == b.source
        assert restored.creator == b.creator
        assert restored.tags == b.tags
        assert restored.created_at == b.created_at

    def test_from_attributes_config(self):
        assert Belief.model_config.get("from_attributes") is True

    def test_missing_required(self):
        with pytest.raises(ValidationError):
            Belief()  # missing proposition

    def test_unique_ids(self):
        b1 = Belief(proposition="A")
        b2 = Belief(proposition="B")
        assert b1.id != b2.id


class TestEvidenceModel:
    def test_minimal_construction(self):
        e = Evidence(belief_id="b1", source_type="fact", source_id="f1")
        assert e.belief_id == "b1"
        assert e.source_type == "fact"
        assert e.source_id == "f1"
        assert e.weight == 0.5  # default
        assert e.contributor == "system"  # default
        assert isinstance(e.created_at, datetime)
        assert e.note is None

    def test_full_construction(self):
        now = datetime.now(timezone.utc)
        e = Evidence(
            belief_id="b2",
            source_type="observation",
            source_id="obs-001",
            weight=0.8,
            contributor="alice",
            created_at=now,
            note="Confirmed by user",
        )
        assert e.belief_id == "b2"
        assert e.source_type == "observation"
        assert e.source_id == "obs-001"
        assert e.weight == 0.8
        assert e.contributor == "alice"
        assert e.note == "Confirmed by user"

    def test_weight_range(self):
        e = Evidence(belief_id="b3", source_type="fact", source_id="f3", weight=0.75)
        assert e.weight == 0.75

    def test_weight_out_of_range_high(self):
        with pytest.raises(ValidationError):
            Evidence(belief_id="b4", source_type="fact", source_id="f4", weight=1.5)

    def test_weight_out_of_range_low(self):
        with pytest.raises(ValidationError):
            Evidence(belief_id="b5", source_type="fact", source_id="f5", weight=-0.1)

    def test_weight_boundary(self):
        e1 = Evidence(belief_id="b6", source_type="fact", source_id="f6", weight=0.0)
        e2 = Evidence(belief_id="b7", source_type="fact", source_id="f7", weight=1.0)
        assert e1.weight == 0.0
        assert e2.weight == 1.0

    def test_json_round_trip(self):
        e = Evidence(
            belief_id="b8",
            source_type="decision",
            source_id="d1",
            weight=0.6,
            contributor="bot",
            note="Auto-extracted",
        )
        data = json.loads(e.model_dump_json())
        restored = Evidence.model_validate(data)
        assert restored.belief_id == e.belief_id
        assert restored.source_type == e.source_type
        assert restored.source_id == e.source_id
        assert restored.weight == e.weight
        assert restored.contributor == e.contributor
        assert restored.note == e.note

    def test_from_attributes_config(self):
        assert Evidence.model_config.get("from_attributes") is True

    def test_unique_ids(self):
        e1 = Evidence(belief_id="b", source_type="fact", source_id="f")
        e2 = Evidence(belief_id="b", source_type="fact", source_id="f")
        assert e1.id != e2.id

    def test_missing_required(self):
        with pytest.raises(ValidationError):
            Evidence()  # missing belief_id, source_type, source_id
