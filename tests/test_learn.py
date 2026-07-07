"""Tests for learn() MCP tool (Card 015)."""

import pytest

from memory_server.api.learn import learn
from memory_server.models import Decision, Fact, Skill
from memory_server.providers.sqlite_provider import SQLiteProvider


@pytest.fixture
async def provider():
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()
    yield p
    await p.close()


@pytest.mark.asyncio
class TestLearn:
    async def test_learn_extracts_and_stores_facts(self, provider):
        """learn() with 'X is Y' text extracts and stores facts."""
        result = await learn(provider, text="Docker is container")
        assert "facts" in result
        assert len(result["facts"]) >= 1
        f = result["facts"][0]
        assert "receipt" in f
        assert "item" in f
        assert f["item"]["subject"] == "Docker"
        assert f["item"]["predicate"] == "is"
        assert f["item"]["object"] == "container"

        # Verify it's stored in DB
        stored_fact = await provider.get_fact(f["receipt"]["id"])
        assert stored_fact is not None
        assert stored_fact.subject == "Docker"

    async def test_learn_extracts_and_stores_decisions(self, provider):
        """learn() with decision text extracts and stores decisions."""
        result = await learn(
            provider, text="we decided to use Caddy because it is simpler"
        )
        assert "decisions" in result
        assert len(result["decisions"]) >= 1
        d = result["decisions"][0]
        assert "receipt" in d
        assert "item" in d
        assert d["item"]["choice"] == "use Caddy"
        assert "simpler" in d["item"]["reason"]

        # Verify stored in DB
        stored_decision = await provider.get_decision(d["receipt"]["id"])
        assert stored_decision is not None
        assert stored_decision.choice == "use Caddy"

    async def test_learn_extracts_and_stores_skills(self, provider):
        """learn() with skill text extracts and stores skills."""
        result = await learn(
            provider,
            text="to deploy docker, do: 1) pull image, 2) run container",
        )
        assert "skills" in result
        assert len(result["skills"]) >= 1
        s = result["skills"][0]
        assert "receipt" in s
        assert "item" in s
        assert s["item"]["purpose"] == "deploy docker"
        assert "pull image" in s["item"]["steps"]

        # Verify stored in DB
        stored_skill = await provider.get_skill(s["receipt"]["id"])
        assert stored_skill is not None
        assert stored_skill.purpose == "deploy docker"

    async def test_learn_empty_text(self, provider):
        """Empty text returns no extractions."""
        result = await learn(provider, text="")
        assert result["facts"] == []
        assert result["decisions"] == []
        assert result["skills"] == []
        assert len(result["receipts"]) == 0

    async def test_learn_with_source(self, provider):
        """Source parameter is passed through to all extracted items."""
        result = await learn(
            provider,
            text="Docker is container. decided to use Caddy because simple",
            source="test-session-1",
        )
        # Check fact source
        for f in result["facts"]:
            assert f["receipt"]["source"] == "test-session-1"
        # Check decision source
        for d in result["decisions"]:
            assert d["receipt"]["source"] == "test-session-1"

    async def test_learn_receipts_have_correct_memory_type(self, provider):
        """Each receipt should reflect its memory type."""
        result = await learn(
            provider, text="Python is great. decided to rewrite because slow"
        )
        for f in result["facts"]:
            assert f["receipt"]["memory_type"] == "fact"
        for d in result["decisions"]:
            assert d["receipt"]["memory_type"] == "decision"

    async def test_learn_multiple_extractions_from_single_text(self, provider):
        """One text can produce facts, decisions, and skills simultaneously."""
        result = await learn(
            provider,
            text=(
                "Docker is container. "
                "decided to use Caddy because simple. "
                "to deploy, do: 1) pull image, 2) run."
            ),
        )
        assert len(result["facts"]) >= 1
        assert len(result["decisions"]) >= 1
        assert len(result["skills"]) >= 1

    async def test_learn_returns_receipts_list(self, provider):
        """Top-level receipts list should track all operations."""
        result = await learn(
            provider,
            text="Docker is container. decided to use Caddy because simple",
        )
        total = len(result["facts"]) + len(result["decisions"]) + len(result["skills"])
        assert len(result["receipts"]) == total
