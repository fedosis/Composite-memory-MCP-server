"""Tests for search MCP tool (Card 005)."""

import pytest

from memory_server.api.search import search
from memory_server.models import Fact
from memory_server.providers.sqlite_provider import SQLiteProvider


@pytest.fixture
async def provider():
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()
    # Seed facts
    await p.create_fact(Fact(id="s1", subject="Docker", predicate="runs_on", object="OMV8", confidence=1.0, source="manual"))
    await p.create_fact(Fact(id="s2", subject="Caddy", predicate="uses", object="Port 443", confidence=0.9, source="auto"))
    await p.create_fact(Fact(id="s3", subject="Nginx", predicate="is", object="Reverse proxy", confidence=0.8, source="manual"))
    await p.create_fact(Fact(id="s4", subject="Docker", predicate="uses", object="Containers", confidence=0.95, source="auto"))
    yield p
    await p.close()


@pytest.mark.asyncio
class TestSearch:
    async def test_exact_match(self, provider):
        result = await search(provider, query="Docker")
        assert result["total"] >= 1
        subjects = [f["subject"] for f in result["results"]]
        assert "Docker" in subjects

    async def test_partial_like_match(self, provider):
        result = await search(provider, query="Port")
        assert result["total"] >= 1
        objects = [f["object"] for f in result["results"]]
        assert any("Port" in o for o in objects)

    async def test_no_results(self, provider):
        result = await search(provider, query="XYZZZDoesNotExist")
        assert result["total"] == 0
        assert result["results"] == []

    async def test_filtered_search_by_subject(self, provider):
        result = await search(provider, query="", subject="Docker")
        assert result["total"] == 2
        for f in result["results"]:
            assert f["subject"] == "Docker"

    async def test_filtered_search_by_predicate(self, provider):
        result = await search(provider, query="", predicate="uses")
        assert result["total"] >= 2
        for f in result["results"]:
            assert f["predicate"] == "uses"

    async def test_limit_respected(self, provider):
        result = await search(provider, query="", limit=1)
        assert len(result["results"]) <= 1

    async def test_combined_filters(self, provider):
        result = await search(provider, query="", subject="Docker", predicate="uses")
        assert result["total"] == 1
        assert result["results"][0]["id"] == "s4"
