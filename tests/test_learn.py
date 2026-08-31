"""Tests for learn() MCP tool (Card 015)."""

import asyncio

import pytest
from storage.repositories import DecisionRepository

from memory_server.api.learn import learn
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

    async def test_learn_dedups_identical_decisions_across_sources(self, provider):
        """Ingesting the same (context, choice) twice creates only one row.

        Regression: the same decision was re-ingested every turn (under
        different hermes_turn_* session sources) and by tests, growing the
        decisions table with exact duplicates. The write path must deduplicate
        cross-source, not per-source.
        """
        text = "we decided to use Caddy because it is simpler"
        first = await learn(provider, text=text, source="test-session-1")
        assert len(first["decisions"]) == 1

        second = await learn(provider, text=text, source="test-session-2")
        assert second["decisions"] == [], (
            "duplicate (context, choice) ingestion should be skipped"
        )

        stored = await provider.search_decisions(limit=50)
        caddy = [d for d in stored if d.choice == "use Caddy"]
        assert len(caddy) == 1, "expected exactly one stored 'use Caddy' decision"

    async def test_learn_dedups_near_duplicate_choice_variants(self, provider):
        """Near-duplicate choice variants (same normalized prefix) collapse (W1).

        Regression: the regex extractor captures a *growing parenthetical* into
        ``choice`` (e.g. the travel-agent decision re-ingested with choice
        lengths 1381/1393/3610/6401 chars), so exact (context, choice) matching
        never fires and the same decision is re-ingested every turn. The write
        path must skip variants that share the normalized dedup key
        (context.strip(), whitespace-collapsed 200-char prefix of choice).
        """
        from storage.dedup import decision_dedup_key

        # Both choices are >= 200 chars and share their first 200 collapsed
        # chars — the observed production pattern (growing parenthetical).
        prefix = (
            "use Caddy (the lightweight web server with automatic HTTPS, "
            "plugins, Docker integration, and a parenthetical that keeps "
            "growing with every re-ingest of the same turn across many "
            "sessions and cron runs until the extractor captures the whole "
            "accumulated context in one choice string"
        )
        choice_v1 = prefix + ")"
        choice_v2 = prefix + ", plus more options, and even more tail)"
        assert decision_dedup_key("", choice_v1) == decision_dedup_key("", choice_v2), (
            "test precondition: variants must share the normalized dedup key"
        )

        text_v1 = f"we decided to {choice_v1} because it is simpler"
        text_v2 = f"we decided to {choice_v2} because it is simpler"
        first = await learn(provider, text=text_v1, source="test-session-1")
        assert len(first["decisions"]) == 1

        second = await learn(provider, text=text_v2, source="test-session-2")
        assert second["decisions"] == [], (
            "near-duplicate choice variant should be skipped (W1)"
        )

        stored = await provider.search_decisions(limit=50)
        variants = [d for d in stored if d.choice.startswith("use Caddy (the lightweight")]
        assert len(variants) == 1, (
            "expected exactly one stored decision for the near-duplicate pair"
        )

    async def test_learn_reingests_decision_after_rejection(self, provider):
        """A rejected decision must not permanently block re-ingestion (W3).

        Regression: find_existing matched ANY row, so a rejected/archived
        decision permanently blocked re-ingestion of the same (context, choice)
        — combined with the prefetch confidence filter (>= 0.8) and regex-mode
        fixed 0.5 confidence, the decision became permanently invisible. Only
        ACTIVE rows may participate in write-path dedup.
        """
        text = "we decided to use Caddy because it is simpler"
        first = await learn(provider, text=text, source="test-session-1")
        assert len(first["decisions"]) == 1
        decision_id = first["decisions"][0]["receipt"]["id"]

        # Simulate the lifecycle transition to rejected.
        async with provider.engine.begin() as conn:
            await conn.exec_driver_sql(
                "UPDATE decisions SET lifecycle_state='rejected', "
                "verification_status='rejected' WHERE id = ?",
                (decision_id,),
            )

        second = await learn(provider, text=text, source="test-session-2")
        assert len(second["decisions"]) == 1, (
            "re-ingestion after rejection must create a fresh active row (W3)"
        )

        stored = await provider.search_decisions(limit=50)
        caddy = [d for d in stored if d.choice == "use Caddy"]
        assert len(caddy) == 2, (
            "expected the rejected row plus a fresh active row"
        )
        active = [
            d for d in caddy if d.lifecycle_state in ("candidate", "validated", "active")
        ]
        assert len(active) == 1, "exactly one of the two rows must be active"
        assert active[0].id != decision_id

    async def test_learn_concurrent_same_decision_creates_single_row(
        self, tmp_path, monkeypatch
    ):
        """Concurrent learn() calls of the same decision create exactly one row (B1).

        Regression: the write-path dedup was a plain check-then-insert. Two
        concurrent learn() calls (parallel Hermes learn calls, or the
        background turn writer overlapping an MCP learn tool call) could both
        pass find_existing() before either commits, then both insert. The
        partial unique index on (context, dedup_key) must make the race loser
        fail with IntegrityError, which the ingestion service treats as a
        duplicate — no second row, receipt, or outbox entry.

        The test forces the race deterministically: find_existing is gated so
        BOTH callers pass the existence check before either inserts.
        """
        db_path = tmp_path / "race.db"
        url = f"sqlite+aiosqlite:///{db_path}"
        provider_a = SQLiteProvider(url=url)
        provider_b = SQLiteProvider(url=url)
        await provider_a.initialize()
        await provider_b.initialize()
        try:
            # Decision-only text: contains no " is ", so no fact is extracted
            # and no write happens before the gated find_existing — otherwise
            # the two gated transactions would deadlock on the fact insert's
            # write lock (task A waiting for release, task B waiting for A's
            # lock).
            text = "we decided to use Caddy because Nginx proved too complex"

            first_arrived = asyncio.Event()
            second_arrived = asyncio.Event()
            release = asyncio.Event()
            call_count = 0
            original_find = DecisionRepository.find_existing

            async def gated_find(self, context, choice):
                nonlocal call_count
                call_count += 1
                result = await original_find(self, context, choice)
                if call_count == 1:
                    first_arrived.set()
                else:
                    second_arrived.set()
                await release.wait()
                return result

            monkeypatch.setattr(DecisionRepository, "find_existing", gated_find)

            async def do_learn(p: SQLiteProvider, source: str) -> dict:
                return await learn(p, text=text, source=source)

            task_a = asyncio.create_task(do_learn(provider_a, "race-a"))
            await asyncio.wait_for(first_arrived.wait(), timeout=10)
            task_b = asyncio.create_task(do_learn(provider_b, "race-b"))
            await asyncio.wait_for(second_arrived.wait(), timeout=10)
            release.set()
            results = await asyncio.gather(task_a, task_b)

            # Exactly one of the two callers won the insert; the loser saw the
            # unique index fire and skipped (no crash).
            decision_counts = [len(r["decisions"]) for r in results]
            assert sorted(decision_counts) == [0, 1], (
                f"expected one winner and one race loser, got {decision_counts}"
            )

            stored = await provider_a.search_decisions(limit=50)
            caddy = [d for d in stored if d.choice == "use Caddy"]
            assert len(caddy) == 1, (
                "concurrent learn() must produce exactly one row (B1)"
            )

            # The loser must not have leaked a receipt or outbox entry.
            receipts = await provider_a.search_receipts(
                memory_type="decision", limit=50
            )
            assert len(receipts) == 1, "race loser must not create a receipt"
        finally:
            await provider_a.close()
            await provider_b.close()
