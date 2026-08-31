"""Tests for get_context MCP tool (Card 004)."""

import pytest

from memory_server.api.get_context import get_context
from memory_server.models import Decision, Fact
from memory_server.providers.sqlite_provider import SQLiteProvider


@pytest.fixture
async def provider():
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()
    # Seed some facts
    await p.create_fact(Fact(id="f1", subject="Docker", predicate="runs_on", object="OMV8"))
    await p.create_fact(Fact(id="f2", subject="Caddy", predicate="uses", object="Port 443"))
    await p.create_fact(Fact(id="f3", subject="Nginx", predicate="is", object="Reverse proxy"))
    yield p
    await p.close()


@pytest.mark.asyncio
class TestGetContext:
    async def test_get_context_returns_structured_result(self, provider):
        result = await get_context(provider, task="Docker")
        assert isinstance(result, dict)
        assert "facts" in result
        assert "total" in result
        assert isinstance(result["facts"], list)
        assert isinstance(result["total"], int)

    async def test_get_context_matches_facts(self, provider):
        result = await get_context(provider, task="Docker")
        assert result["total"] >= 1
        subjects = [f["subject"] for f in result["facts"]]
        assert "Docker" in subjects

    async def test_get_context_no_results(self, provider):
        result = await get_context(provider, task="XYZZZDoesNotExist")
        assert result["total"] == 0
        assert result["facts"] == []

    async def test_get_context_with_subject_filter(self, provider):
        result = await get_context(provider, task="runs_on", subject="Docker")
        assert result["total"] >= 1
        for f in result["facts"]:
            assert f["subject"] == "Docker"

    async def test_get_context_respects_max_results(self, provider):
        # Add more facts
        for i in range(10):
            await provider.create_fact(
                Fact(id=f"fextra_{i}", subject=f"Topic{i}", predicate="is", object="Test")
            )
        result = await get_context(provider, task="", max_results=3)
        assert len(result["facts"]) <= 3

    async def test_get_context_allows_passing_subject(self, provider):
        """Subject can be passed as a search parameter."""
        result = await get_context(provider, task="", subject="Caddy")
        assert result["total"] >= 1
        assert any(f["subject"] == "Caddy" for f in result["facts"])

    async def test_get_context_returns_decisions(self, provider):
        await provider.create_decision(
            Decision(
                id="d1",
                context="Docker",
                choice="Use OMV8 for Docker",
                reason="OMV8 handles Docker containers well",
            )
        )
        result = await get_context(provider, task="Docker")
        assert result["decisions"], "expected at least one decision"
        assert result["decisions"][0]["context"] == "Docker"
        assert result["decisions"][0]["choice"] == "Use OMV8 for Docker"
        assert result["total"] == len(result["facts"]) + len(result["decisions"])

    async def test_get_context_no_decisions_when_no_match(self, provider):
        await provider.create_decision(
            Decision(
                id="d2",
                context="Postgres",
                choice="Keep Postgres",
                reason="Already deployed",
            )
        )
        result = await get_context(provider, task="XYZZZDoesNotExist")
        assert result["decisions"] == []
        assert result["total"] == 0

    async def test_get_context_excludes_inactive_decisions(self, provider):
        await provider.create_decision(
            Decision(
                id="d3",
                context="Caddy",
                choice="Use Caddy",
                reason="Docker integration",
                lifecycle_state="archived",
            )
        )
        default_result = await get_context(provider, task="Caddy")
        assert default_result["decisions"] == []

        inclusive_result = await get_context(
            provider, task="Caddy", include_inactive=True
        )
        assert any(d["id"] == "d3" for d in inclusive_result["decisions"])
        assert inclusive_result["total"] == (
            len(inclusive_result["facts"]) + len(inclusive_result["decisions"])
        )
