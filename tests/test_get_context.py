"""Tests for get_context MCP tool (Card 004)."""

from datetime import datetime, timedelta, timezone

import pytest

import memory_server.api.get_context as get_context_module
from memory_server.api.get_context import get_context
from memory_server.models import Decision, Fact
from memory_server.providers.sqlite_provider import SQLiteProvider


async def _drop_decision_dedup_index(provider: SQLiteProvider) -> None:
    """Drop the DB-level unique index so tests can seed duplicate data.

    The partial unique index on (context, dedup_key) makes it impossible to
    insert two ACTIVE rows sharing a normalized key — which is exactly its
    job. The read-path dedup tests need to seed such pre-migration legacy data
    (duplicates already in the table), so they drop the index first, the same
    way a pre-migration production DB would look.
    """
    async with provider.engine.begin() as conn:
        await conn.exec_driver_sql(
            "DROP INDEX IF EXISTS uq_decisions_context_dedup_active"
        )


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
        # Seed a decision, then prove the wiring both ways: a matching task must
        # surface it (positive proof — red on the pre-change decisions stub),
        # a non-matching task must return nothing (negative proof).
        await provider.create_decision(
            Decision(
                id="d2",
                context="Postgres",
                choice="Keep Postgres",
                reason="Already deployed",
            )
        )

        matching = await get_context(provider, task="Postgres")
        assert any(d["id"] == "d2" for d in matching["decisions"]), (
            "decision search is not wired: matching task returned no decisions"
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

    async def test_get_context_dedups_identical_decisions(self, provider):
        """Duplicate (context, choice) decisions collapse to the best row.

        Regression: the decisions table accumulated the same decision once per
        turn/session, and get_context returned every copy — flooding the
        injected context block with identical entries. Seeds pre-migration
        legacy data (duplicate rows), so the DB-level unique index is dropped
        first.
        """
        await _drop_decision_dedup_index(provider)
        await provider.create_decision(
            Decision(
                id="d_old",
                context="Docker",
                choice="Use OMV8 for Docker",
                reason="OMV8 handles Docker containers well",
                created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            )
        )
        await provider.create_decision(
            Decision(
                id="d_new",
                context="Docker",
                choice="Use OMV8 for Docker",
                reason="OMV8 handles Docker containers well",
                created_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
            )
        )
        result = await get_context(provider, task="Docker")
        assert len(result["decisions"]) == 1
        assert result["decisions"][0]["id"] == "d_new"

    async def test_get_context_dedups_near_duplicate_decisions(self, provider):
        """Near-duplicate choices (same normalized prefix) collapse (W1).

        Regression: the regex extractor captures a *growing parenthetical*
        into ``choice`` (travel-agent decision re-ingested with choice lengths
        1381/1393/3610/6401 chars), so exact (context, choice) equality never
        fires and the context block floods with near-duplicates. The read path
        must collapse rows sharing the normalized dedup key — the same key the
        write path and the DB index use.
        """
        from storage.dedup import decision_dedup_key

        await _drop_decision_dedup_index(provider)
        # Both choices are >= 200 chars and share their first 200 collapsed
        # chars — the observed production pattern (growing parenthetical).
        prefix = (
            "Use OMV8 for Docker (the NAS platform with containers, plugins, "
            "RAID management, and a growing parenthetical that keeps "
            "accumulating more configuration detail with every re-ingest "
            "across turns and cron runs until the extractor captures the "
            "entire accumulated context in one long choice string"
        )
        choice_short = prefix + ")"
        choice_long = prefix + ", plus more options, and even more tail)"
        assert decision_dedup_key("Docker", choice_short) == decision_dedup_key(
            "Docker", choice_long
        ), "test precondition: variants must share the normalized dedup key"

        await provider.create_decision(
            Decision(
                id="d_short",
                context="Docker",
                choice=choice_short,
                reason="Docker integration",
                created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            )
        )
        await provider.create_decision(
            Decision(
                id="d_long",
                context="Docker",
                choice=choice_long,
                reason="Docker integration",
                created_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
            )
        )
        result = await get_context(provider, task="Docker")
        assert len(result["decisions"]) == 1, (
            "near-duplicate variants must collapse to a single decision (W1)"
        )
        assert result["decisions"][0]["id"] == "d_long"

    async def test_get_context_dedup_prefers_higher_confidence(self, provider):
        """A high-confidence decision wins over a newer low-confidence dup (W4).

        Regression: dedup kept the NEWEST row regardless of confidence and the
        provider filters confidence >= 0.8 AFTER dedup, so a newer
        low-confidence copy evicted an older high-confidence one — making it
        permanently invisible. The dedup winner must be the higher-confidence
        row, tie-breaking to newest.
        """
        from storage.dedup import decision_dedup_key

        await _drop_decision_dedup_index(provider)
        prefix = (
            "Use OMV8 for Docker (the NAS platform with containers, plugins, "
            "RAID management, and a growing parenthetical that keeps "
            "accumulating more configuration detail with every re-ingest "
            "across turns and cron runs until the extractor captures the "
            "entire accumulated context in one long choice string"
        )
        choice_high = prefix + ")"
        choice_low = prefix + ", re-ingested low-confidence variant)"
        assert decision_dedup_key("Docker", choice_high) == decision_dedup_key(
            "Docker", choice_low
        ), "test precondition: variants must share the normalized dedup key"

        await provider.create_decision(
            Decision(
                id="d_high_old",
                context="Docker",
                choice=choice_high,
                reason="Confirmed",
                confidence=0.95,
                created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            )
        )
        await provider.create_decision(
            Decision(
                id="d_low_new",
                context="Docker",
                choice=choice_low,
                reason="Uncertain",
                confidence=0.5,
                created_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
            )
        )
        result = await get_context(provider, task="Docker")
        assert len(result["decisions"]) == 1
        assert result["decisions"][0]["id"] == "d_high_old", (
            "higher-confidence decision must not be hidden behind a "
            "low-confidence duplicate (W4)"
        )

    async def test_get_context_dedup_still_fills_budget(self, provider, monkeypatch):
        """The ×4 over-fetch is REQUIRED for dedup to still fill the budget (N1).

        Seeds 11 distinct decisions, each duplicated 4x (44 rows), ordered so
        the 10 newest rows are only 3 distinct pairs. With the over-fetch
        factor forced to 1, the budget cannot fill; with the real factor the
        budget fills with 10 distinct decisions. This pins the factor: without
        it, the assertion on the factor-1 run would pass trivially.
        """
        await _drop_decision_dedup_index(provider)
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        for j in range(11):  # 11 distinct decisions
            for c in range(4):  # each duplicated 4x
                await provider.create_decision(
                    Decision(
                        id=f"d_{j}_{c}",
                        context=f"Topic{j}",
                        choice=f"Choice{j}",
                        reason="dup",
                        created_at=base + timedelta(hours=1000 - (j * 4 + c)),
                    )
                )

        # Without over-fetch (factor 1): the 10 newest rows are 3 distinct
        # pairs, so dedup cannot fill the budget.
        monkeypatch.setattr(get_context_module, "DEDUP_OVERFETCH_FACTOR", 1)
        under = await get_context(provider, task="", max_results=10)
        assert len(under["decisions"]) < 10, (
            "over-fetch factor must be needed to fill the budget (N1)"
        )

        # With the real ×4 factor, the budget fills with 10 distinct decisions.
        monkeypatch.setattr(get_context_module, "DEDUP_OVERFETCH_FACTOR", 4)
        full = await get_context(provider, task="", max_results=10)
        assert len(full["decisions"]) == 10
        assert {d["choice"] for d in full["decisions"]} == {
            f"Choice{j}" for j in range(10)
        }
