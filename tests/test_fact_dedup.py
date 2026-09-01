"""B1 storage deduplication and reinforcement tests."""

from datetime import datetime, timezone

import pytest
from sqlalchemy import text
from storage.dedup import (
    ACTIVE_LIFECYCLE_STATES,
    fact_dedup_key,
    normalize_choice,
    normalize_spo_component,
)
from storage.repositories import DecisionRepository, FactRepository, ReceiptRepository

from memory_server.models import Decision, Fact, MemoryReceipt
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.services.ingestion_service import reinforce_memory_item


@pytest.fixture
async def provider():
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()
    yield p
    await p.close()


def make_fact(fact_id="fact-1", confidence=0.4, **kwargs):
    return Fact(
        id=fact_id, subject=kwargs.get("subject", "Docker"),
        predicate=kwargs.get("predicate", "is"),
        object=kwargs.get("object", "container"), confidence=confidence,
        source="seed", created_at=datetime.now(timezone.utc),
    )


def make_decision(decision_id="decision-1", confidence=0.4):
    return Decision(
        id=decision_id, context="deployment", choice="use Caddy",
        reason="simpler", confidence=confidence, source="seed",
    )


@pytest.mark.asyncio
async def test_normalize_spo_component_is_whitespace_only_and_case_sensitive():
    assert normalize_spo_component("  Docker  is\ncontainer ") == "Docker is container"
    assert normalize_spo_component(None) == ""
    assert normalize_spo_component("Docker") != normalize_spo_component("docker")
    assert normalize_spo_component("\tDocker  is\tcontainer") == "Docker is container"


def test_fact_key_uses_shared_normalizer_and_unit_separator():
    assert fact_dedup_key(" Docker ", "is", "container") == "Docker\x1fis\x1fcontainer"
    key = fact_dedup_key("Docker  is", "x", "y")
    assert key == fact_dedup_key("Docker is", "x", "y")
    assert key != fact_dedup_key("docker is", "x", "y")
    assert key == fact_dedup_key("Docker\tis", "x", "y")
    assert normalize_choice("  use\tCaddy ") == "use Caddy"


def test_active_lifecycle_states_are_pinned():
    assert ACTIVE_LIFECYCLE_STATES == ("candidate", "validated", "active")


@pytest.mark.asyncio
async def test_learn_identical_fact_reinforces_without_duplicate(provider):
    from memory_server.api.learn import learn

    first = await learn(provider, "Docker is container", source="one")
    second = await learn(provider, "Docker is container", source="two")
    assert len(first["facts"]) == len(second["facts"]) == 1
    stored = await provider.search_facts(include_inactive=True)
    assert len(stored) == 1
    assert second["facts"][0]["item"]["version"] == 2
    assert second["facts"][0]["receipt"]["history"][-1]["kind"] == "reinforce"
    assert second["facts"][0]["receipt"]["history"][-1]["source"] == "two"


@pytest.mark.asyncio
async def test_seeded_outer_and_internal_whitespace_reinforces(provider):
    from memory_server.api.learn import learn

    fact = await provider.create_fact(
        make_fact(subject=" Docker ", object=" container  image ")
    )
    result = await learn(provider, "Docker is container image", source="learned")
    assert len(result["facts"]) == 1
    assert result["facts"][0]["item"]["id"] == fact.id
    assert result["facts"][0]["receipt"]["history"][-1]["kind"] == "reinforce"
    assert len(await provider.search_facts(include_inactive=True)) == 1


@pytest.mark.asyncio
async def test_confidence_max_and_receipt_history_persist_in_fresh_session(provider):
    fact = await provider.create_fact(make_fact(confidence=0.6))
    async with provider._session_factory() as session:
        async with session.begin():
            item, receipt = await reinforce_memory_item(
                session, memory_type="fact", item_id=fact.id,
                new_confidence=0.5, source="lower",
            )
    assert item.confidence == 0.6
    assert receipt.history[-1]["confidence"] == 0.6
    async with provider._session_factory() as session:
        fresh = await FactRepository(session).get(fact.id)
        fresh_receipt = await ReceiptRepository(session).get(fact.id)
    assert fresh.confidence == 0.6
    assert fresh.version == 2
    assert fresh_receipt.history[-1] == receipt.history[-1]
    assert set(fresh_receipt.history[-1]) == {
        "confidence", "kind", "source", "previous_confidence", "timestamp"
    }


@pytest.mark.asyncio
async def test_decision_reinforcement_lower_confidence_keeps_max(provider):
    decision = await provider.create_decision(make_decision(confidence=0.8))
    async with provider._session_factory() as session:
        async with session.begin():
            item, receipt = await reinforce_memory_item(
                session, memory_type="decision", item_id=decision.id,
                new_confidence=0.4, source="lower",
            )
    assert item.confidence == 0.8
    assert receipt.confidence == 0.8
    assert receipt.history[-1]["previous_confidence"] == 0.8


@pytest.mark.asyncio
async def test_decision_reinforcement_persists_freshness_and_exact_history(provider):
    decision = await provider.create_decision(make_decision(confidence=0.6))
    before = decision.updated_at
    async with provider._session_factory() as session:
        async with session.begin():
            _, receipt = await reinforce_memory_item(
                session, memory_type="decision", item_id=decision.id,
                new_confidence=0.5, source="freshness",
            )
    async with provider._session_factory() as session:
        fresh = await DecisionRepository(session).get(decision.id)
        fresh_receipt = await ReceiptRepository(session).get(decision.id)
    entry = fresh_receipt.history[-1]
    assert fresh.updated_at > before
    assert fresh_receipt.updated_at > before
    assert entry == receipt.history[-1]
    assert set(entry) == {
        "confidence", "kind", "source", "previous_confidence", "timestamp"
    }
    assert entry["confidence"] == 0.6


@pytest.mark.asyncio
async def test_confidence_increases_and_receipt_is_created_when_absent(provider):
    fact = await provider.create_fact(make_fact(confidence=0.4))
    async with provider._session_factory() as session:
        async with session.begin():
            item, receipt = await reinforce_memory_item(
                session, memory_type="fact", item_id=fact.id,
                new_confidence=0.5, source="higher",
            )
    assert item.confidence == 0.5
    assert receipt.id == fact.id
    assert receipt.history[-1]["previous_confidence"] == 0.4


@pytest.mark.asyncio
async def test_decision_reinforcement_bumps_semver_and_persists(provider):
    decision = await provider.create_decision(make_decision())
    async with provider._session_factory() as session:
        async with session.begin():
            item, receipt = await reinforce_memory_item(
                session, memory_type="decision", item_id=decision.id,
                new_confidence=0.8, source="review",
            )
    assert item.version == "0.2.0"
    assert item.confidence == 0.8
    async with provider._session_factory() as session:
        fresh = await DecisionRepository(session).get(decision.id)
        fresh_receipt = await ReceiptRepository(session).get(decision.id)
    assert fresh.version == "0.2.0"
    assert fresh.confidence == 0.8
    assert fresh_receipt.history[-1]["kind"] == "reinforce"


@pytest.mark.asyncio
async def test_non_active_fact_does_not_block_reingestion(provider):
    from memory_server.api.learn import learn

    fact = await provider.create_fact(make_fact())
    await provider.update_fact(fact.id, lifecycle_state="archived")
    result = await learn(provider, "Docker is container", source="new")
    assert len(result["facts"]) == 1
    assert result["facts"][0]["item"]["id"] != fact.id
    assert len(await provider.search_facts(include_inactive=True)) == 2


@pytest.mark.asyncio
async def test_duplicate_decision_is_skipped_but_direct_reinforcement_works(provider):
    from memory_server.api.learn import learn

    first = await learn(provider, "we decided to use Caddy because it is simpler")
    second = await learn(provider, "we decided to use Caddy because it is simpler")
    assert len(first["decisions"]) == 1
    assert second["decisions"] == []
    decision = (await provider.search_decisions(limit=20))[0]
    async with provider._session_factory() as session:
        async with session.begin():
            reinforced, _ = await reinforce_memory_item(
                session, memory_type="decision", item_id=decision.id,
                new_confidence=0.9, source="direct",
            )
    assert reinforced.confidence == 1.0


@pytest.mark.asyncio
async def test_unsupported_type_missing_item_and_stale_guard_are_fail_closed(provider):
    fact = await provider.create_fact(make_fact(confidence=0.4))
    decision = await provider.create_decision(make_decision(confidence=0.7))
    before_facts = [item.model_dump(mode="json") for item in await provider.search_facts(include_inactive=True)]
    before_decisions = [item.model_dump(mode="json") for item in await provider.search_decisions(limit=20)]
    before_receipts = [item.model_dump(mode="json") for item in await provider.search_receipts(limit=20)]
    async with provider._session_factory() as session:
        async with session.begin():
            with pytest.raises(ValueError):
                await reinforce_memory_item(
                    session, memory_type="facts", item_id=fact.id,
                    new_confidence=0.9, source="bad",
                )
            with pytest.raises(ValueError):
                await reinforce_memory_item(
                    session, memory_type="other", item_id=fact.id,
                    new_confidence=0.9, source="bad",
                )
            with pytest.raises(LookupError):
                await reinforce_memory_item(
                    session, memory_type="fact", item_id="missing",
                    new_confidence=0.9, source="bad",
                )
            with pytest.raises(ValueError, match="stale"):
                await reinforce_memory_item(
                    session, memory_type="fact", item_id=fact.id,
                    new_confidence=0.9, source="stale", previous_confidence=0.3,
                )
    fresh = await provider.get_fact(fact.id)
    assert fresh.confidence == 0.4
    assert fresh.version == 1
    assert await provider.search_receipts(memory_type="fact", limit=20) == []
    assert [item.model_dump(mode="json") for item in await provider.search_facts(include_inactive=True)] == before_facts
    assert [item.model_dump(mode="json") for item in await provider.search_decisions(limit=20)] == before_decisions
    assert [item.model_dump(mode="json") for item in await provider.search_receipts(limit=20)] == before_receipts
    assert decision.confidence == 0.7


@pytest.mark.asyncio
async def test_reinforcement_does_not_add_outbox_entry(provider):
    fact = await provider.create_fact(make_fact())
    async with provider._session_factory() as session:
        async with session.begin():
            await reinforce_memory_item(
                session, memory_type="fact", item_id=fact.id,
                new_confidence=0.8, source="review",
            )
    async with provider._session_factory() as session:
        count = (await session.execute(
            text("SELECT count(*) FROM outbox_entries")
        )).scalar_one()
    assert count == 0


@pytest.mark.asyncio
async def test_repeated_whitespace_lookup_has_one_matching_row(provider):
    fact = await provider.create_fact(make_fact())
    async with provider._session_factory() as session:
        repo = FactRepository(session)
        found_variant = await repo.find_existing("  Docker\n", " is ", "container ")
        found_exact = await repo.find_existing("Docker", "is", "container")
    assert found_variant is not None
    assert found_exact is not None
    assert found_variant.id == fact.id
    assert found_exact.id == fact.id
    assert len(await provider.search_facts(include_inactive=True)) == 1


@pytest.mark.asyncio
async def test_stale_guard_preserves_existing_receipt(provider):
    fact = await provider.create_fact(make_fact())
    async with provider._session_factory() as session:
        async with session.begin():
            await ReceiptRepository(session).create(MemoryReceipt(
                id=fact.id, memory_type="fact", source="seed", created_by="learn",
                timestamp=datetime.now(timezone.utc), confidence=0.4,
            ))
            with pytest.raises(ValueError):
                await reinforce_memory_item(
                    session, memory_type="fact", item_id=fact.id,
                    new_confidence=0.9, source="stale", previous_confidence=0.2,
                )
    receipt = (await provider.search_receipts(memory_type="fact", limit=20))[0]
    assert receipt.confidence == 0.4
    assert receipt.history == []


@pytest.mark.asyncio
async def test_find_existing_orders_active_candidates_and_rejects_case_change(provider):
    low = await provider.create_fact(make_fact("low", confidence=0.4))
    high = await provider.create_fact(make_fact("high", confidence=0.8))
    async with provider._session_factory() as session:
        found = await FactRepository(session).find_existing("Docker", "is", "container")
    assert found.id == high.id
    await provider.update_fact(high.id, lifecycle_state="rejected")
    async with provider._session_factory() as session:
        found = await FactRepository(session).find_existing("docker", "is", "container")
    assert found is None
    assert low.id != high.id


@pytest.mark.asyncio
async def test_rejected_fact_does_not_block_reingestion(provider):
    from memory_server.api.learn import learn

    fact = await provider.create_fact(make_fact())
    await provider.update_fact(fact.id, lifecycle_state="rejected")
    result = await learn(provider, "Docker is container", source="rejected-retry")
    assert len(result["facts"]) == 1
    assert result["facts"][0]["item"]["id"] != fact.id
    assert len(await provider.search_facts(include_inactive=True)) == 2


@pytest.mark.asyncio
async def test_repeated_whitespace_ingestion_reinforces_one_row(provider):
    from memory_server.api.learn import learn

    first = await learn(provider, "Docker is container", source="canonical")
    second = await learn(provider, "  Docker\n\tis   container  ", source="variant")
    assert len(first["facts"]) == len(second["facts"]) == 1
    assert second["facts"][0]["item"]["id"] == first["facts"][0]["item"]["id"]
    assert second["facts"][0]["receipt"]["history"][-1]["kind"] == "reinforce"
    assert len(await provider.search_facts(include_inactive=True)) == 1


@pytest.mark.asyncio
async def test_decision_stale_guard_preserves_storage(provider):
    decision = await provider.create_decision(make_decision(confidence=0.7))
    before_decision = (await provider.get_decision(decision.id)).model_dump(mode="json")
    before_receipts = [item.model_dump(mode="json") for item in await provider.search_receipts(limit=20)]
    async with provider._session_factory() as session:
        async with session.begin():
            with pytest.raises(ValueError, match="stale"):
                await reinforce_memory_item(
                    session, memory_type="decision", item_id=decision.id,
                    new_confidence=0.9, source="stale", previous_confidence=0.6,
                )
    assert (await provider.get_decision(decision.id)).model_dump(mode="json") == before_decision
    assert [item.model_dump(mode="json") for item in await provider.search_receipts(limit=20)] == before_receipts


@pytest.mark.asyncio
async def test_fact_reinforcement_returns_persisted_contract(provider):
    fact = await provider.create_fact(make_fact(confidence=0.4))
    async with provider._session_factory() as session:
        async with session.begin():
            item, receipt = await reinforce_memory_item(
                session, memory_type="fact", item_id=fact.id,
                new_confidence=0.9, source="contract",
            )
    assert item.id == fact.id
    assert item.version == 2
    assert receipt.id == fact.id
    assert receipt.history[-1]["kind"] == "reinforce"


@pytest.mark.asyncio
async def test_decision_reinforcement_returns_persisted_contract(provider):
    decision = await provider.create_decision(make_decision(confidence=0.4))
    async with provider._session_factory() as session:
        async with session.begin():
            item, receipt = await reinforce_memory_item(
                session, memory_type="decision", item_id=decision.id,
                new_confidence=0.8, source="contract",
            )
    assert item.id == decision.id
    assert item.version == "0.2.0"
    assert receipt.id == decision.id
    assert receipt.history[-1]["kind"] == "reinforce"


@pytest.mark.asyncio
async def test_reinforcement_receipt_history_appends(provider):
    fact = await provider.create_fact(make_fact(confidence=0.4))
    async with provider._session_factory() as session:
        async with session.begin():
            _, first_receipt = await reinforce_memory_item(
                session, memory_type="fact", item_id=fact.id,
                new_confidence=0.5, source="first",
            )
    async with provider._session_factory() as session:
        async with session.begin():
            _, second_receipt = await reinforce_memory_item(
                session, memory_type="fact", item_id=fact.id,
                new_confidence=0.6, source="second",
            )
    assert len(second_receipt.history) == 2
    assert second_receipt.history[0] == first_receipt.history[-1]
    assert second_receipt.history[1]["source"] == "second"


@pytest.mark.asyncio
async def test_fact_reinforcement_updates_timestamp(provider):
    fact = await provider.create_fact(make_fact(confidence=0.4))
    before = fact.updated_at
    async with provider._session_factory() as session:
        async with session.begin():
            item, _ = await reinforce_memory_item(
                session, memory_type="fact", item_id=fact.id,
                new_confidence=0.5, source="timestamp",
            )
    assert item.updated_at > before


@pytest.mark.asyncio
async def test_fact_reinforcement_history_has_exact_contract_shape(provider):
    fact = await provider.create_fact(make_fact(confidence=0.4))
    async with provider._session_factory() as session:
        async with session.begin():
            _, receipt = await reinforce_memory_item(
                session, memory_type="fact", item_id=fact.id,
                new_confidence=0.8, source="shape",
            )
    entry = receipt.history[-1]
    assert set(entry) == {
        "confidence", "kind", "source", "previous_confidence", "timestamp"
    }
    assert entry["confidence"] == 0.8
    assert entry["previous_confidence"] == 0.4
    assert entry["source"] == "shape"
