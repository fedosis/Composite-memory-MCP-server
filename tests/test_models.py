"""Tests for Pydantic data models (Card 002)."""

import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from memory_server.models import (
    Decision,
    Entity,
    Fact,
    MemoryReceipt,
    Skill,
    VerificationStatus,
)


class TestEntity:
    def test_minimal_construction(self):
        e = Entity(id="e1", type="server", name="web01")
        assert e.id == "e1"
        assert e.type == "server"
        assert e.name == "web01"
        assert e.attributes == {}
        assert isinstance(e.created_at, datetime)
        assert isinstance(e.updated_at, datetime)

    def test_with_attributes(self):
        e = Entity(
            id="e2",
            type="server",
            name="db01",
            attributes={"ip": "10.0.0.1", "role": "primary"},
        )
        assert e.attributes["ip"] == "10.0.0.1"

    def test_json_round_trip(self):
        e = Entity(id="e3", type="service", name="api", attributes={"port": 8080})
        data = json.loads(e.model_dump_json())
        restored = Entity.model_validate(data)
        assert restored.id == e.id
        assert restored.type == e.type
        assert restored.name == e.name
        assert restored.attributes == e.attributes

    def test_from_attributes_config(self):
        """Model must have from_attributes=True for SQLAlchemy compat."""
        assert Entity.model_config.get("from_attributes") is True

    def test_empty_strings_allowed(self):
        e = Entity(id="e4", type="", name="")
        assert e.type == ""
        assert e.name == ""

    def test_missing_required_fields(self):
        with pytest.raises(ValidationError):
            Entity(id="e5")  # missing 'type' and 'name'


class TestFact:
    def test_minimal_construction(self):
        f = Fact(id="f1", subject="Docker", predicate="runs_on", object="OMV8")
        assert f.subject == "Docker"
        assert f.predicate == "runs_on"
        assert f.object == "OMV8"
        assert f.confidence == 1.0  # default
        assert f.source is None
        assert isinstance(f.created_at, datetime)

    def test_confidence_range(self):
        f = Fact(id="f2", subject="A", predicate="is", object="B", confidence=0.75)
        assert f.confidence == 0.75

    def test_confidence_out_of_range_high(self):
        with pytest.raises(ValidationError):
            Fact(id="f3", subject="A", predicate="is", object="B", confidence=1.5)

    def test_confidence_out_of_range_low(self):
        with pytest.raises(ValidationError):
            Fact(id="f4", subject="A", predicate="is", object="B", confidence=-0.1)

    def test_confidence_boundary(self):
        f1 = Fact(id="f5", subject="A", predicate="is", object="B", confidence=0.0)
        f2 = Fact(id="f6", subject="A", predicate="is", object="B", confidence=1.0)
        assert f1.confidence == 0.0
        assert f2.confidence == 1.0

    def test_json_round_trip(self):
        now = datetime.now(timezone.utc)
        f = Fact(
            id="f7",
            subject="Test",
            predicate="has",
            object="Value",
            confidence=0.5,
            source="test",
            created_at=now,
        )
        data = json.loads(f.model_dump_json())
        restored = Fact.model_validate(data)
        assert restored.subject == f.subject
        assert restored.confidence == f.confidence
        assert restored.source == f.source
        assert restored.created_at == f.created_at

    def test_from_attributes_config(self):
        assert Fact.model_config.get("from_attributes") is True


class TestDecision:
    def test_minimal_construction(self):
        d = Decision(
            id="d1",
            context="Choose a web server",
            choice="Caddy",
            reason="Better Docker integration",
        )
        assert d.context == "Choose a web server"
        assert d.choice == "Caddy"
        assert d.reason == "Better Docker integration"
        assert d.rejected_alternatives == []
        assert d.source is None

    def test_with_alternatives(self):
        d = Decision(
            id="d2",
            context="Database selection",
            choice="PostgreSQL",
            rejected_alternatives=["MySQL", "SQLite"],
            reason="ACID compliance",
        )
        assert len(d.rejected_alternatives) == 2

    def test_empty_alternatives(self):
        d = Decision(
            id="d3", context="Test", choice="A", rejected_alternatives=[], reason="None"
        )
        assert d.rejected_alternatives == []

    def test_json_round_trip(self):
        d = Decision(
            id="d4",
            context="Caching",
            choice="Redis",
            rejected_alternatives=["Memcached"],
            reason="Persistence support",
            source="architect",
        )
        data = json.loads(d.model_dump_json())
        restored = Decision.model_validate(data)
        assert restored.choice == d.choice
        assert restored.rejected_alternatives == d.rejected_alternatives
        assert restored.source == d.source

    def test_from_attributes_config(self):
        assert Decision.model_config.get("from_attributes") is True

    def test_missing_required(self):
        with pytest.raises(ValidationError):
            Decision(id="d5")  # missing context, choice, reason


class TestSkill:
    def test_minimal_construction(self):
        s = Skill(
            id="s1",
            name="deploy-docker",
            purpose="Deploy a Docker container",
            steps=["git pull", "docker compose up -d"],
        )
        assert s.name == "deploy-docker"
        assert s.version == "1.0.0"  # default
        assert len(s.steps) == 2
        assert s.constraints == []
        assert s.validation == []
        assert s.success_rate == 0.0  # default

    def test_full_construction(self):
        s = Skill(
            id="s2",
            name="backup-db",
            version="2.1.0",
            purpose="Backup PostgreSQL",
            steps=["pg_dump", "compress", "upload"],
            constraints=["needs 10GB free", "run during off-peak"],
            validation=["verify checksum", "test restore"],
            success_rate=0.95,
        )
        assert s.version == "2.1.0"
        assert s.success_rate == 0.95

    def test_success_rate_range(self):
        with pytest.raises(ValidationError):
            Skill(
                id="s3",
                name="bad",
                purpose="test",
                steps=["step1"],
                success_rate=1.5,
            )

    def test_empty_steps_allowed(self):
        """Spec says 'steps must be non-empty' — let's make it non-empty per spec test."""
        with pytest.raises(ValidationError):
            Skill(
                id="s4",
                name="test",
                purpose="test",
                steps=[],
            )

    def test_version_default(self):
        s = Skill(id="s5", name="test", purpose="test", steps=["step1"])
        assert s.version == "1.0.0"

    def test_json_round_trip(self):
        s = Skill(
            id="s6",
            name="test-skill",
            version="1.0.0",
            purpose="testing",
            steps=["a", "b"],
            constraints=["c1"],
            validation=["v1"],
            success_rate=0.8,
        )
        data = json.loads(s.model_dump_json())
        restored = Skill.model_validate(data)
        assert restored.name == s.name
        assert restored.steps == s.steps

    def test_from_attributes_config(self):
        assert Skill.model_config.get("from_attributes") is True


class TestMemoryReceipt:
    def test_minimal_construction(self):
        now = datetime.now(timezone.utc)
        r = MemoryReceipt(
            id="r1",
            memory_type="fact",
            source="agent1",
            created_by="test-session",
            timestamp=now,
        )
        assert r.memory_type == "fact"
        assert r.source == "agent1"
        assert r.created_by == "test-session"
        assert r.confidence == 1.0  # default
        assert r.verification_status == VerificationStatus.UNVERIFIED
        assert r.history == []

    def test_full_construction(self):
        now = datetime.now(timezone.utc)
        r = MemoryReceipt(
            id="r2",
            memory_type="decision",
            source="user",
            created_by="alice",
            timestamp=now,
            confidence=0.8,
            verification_status=VerificationStatus.CANDIDATE,
            history=[{"previous_status": "unverified"}],
        )
        assert r.verification_status == VerificationStatus.CANDIDATE
        assert r.history == [{"previous_status": "unverified"}]

    def test_verification_status_values(self):
        """All enum values must be accessible."""
        assert VerificationStatus.UNVERIFIED.value == "unverified"
        assert VerificationStatus.CANDIDATE.value == "candidate"
        assert VerificationStatus.VALIDATED.value == "validated"
        assert VerificationStatus.TRUSTED.value == "trusted"
        assert VerificationStatus.DEPRECATED.value == "deprecated"
        assert VerificationStatus.ARCHIVED.value == "archived"

    def test_json_round_trip(self):
        now = datetime.now(timezone.utc)
        r = MemoryReceipt(
            id="r3",
            memory_type="skill",
            source="system",
            created_by="bot",
            timestamp=now,
            confidence=0.9,
            verification_status=VerificationStatus.VALIDATED,
            history=[{"v": 1}],
        )
        data = json.loads(r.model_dump_json())
        restored = MemoryReceipt.model_validate(data)
        assert restored.memory_type == r.memory_type
        assert restored.verification_status == r.verification_status
        assert restored.history == r.history

    def test_from_attributes_config(self):
        assert MemoryReceipt.model_config.get("from_attributes") is True

    def test_invalid_confidence(self):
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError):
            MemoryReceipt(
                id="r4",
                memory_type="fact",
                source="test",
                created_by="test",
                timestamp=now,
                confidence=2.0,
            )

    def test_default_timestamp(self):
        r = MemoryReceipt(
            id="r5",
            memory_type="fact",
            source="src",
            created_by="usr",
            timestamp=datetime.now(timezone.utc),
        )
        assert r.timestamp is not None
