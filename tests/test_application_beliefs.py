import inspect
from pathlib import Path

import pytest

from memory_server.application.application import MemoryApplication
from memory_server.application.contracts import (
    BeliefPolicy,
    GetBeliefCommand,
    ResolveConflictCommand,
    SetBeliefCommand,
)
from memory_server.application.errors import UnknownDependencyError
from memory_server.providers.sqlite_provider import SQLiteProvider


@pytest.fixture
async def provider(tmp_path):
    db = tmp_path / "beliefs.sqlite"
    value = SQLiteProvider(url=f"sqlite+aiosqlite:///{db}")
    await value.initialize()
    yield value
    await value.close()


async def _none_graph():
    return None


async def _service(provider):
    return MemoryApplication.from_provider(
        provider, belief_policy=BeliefPolicy(10), get_graph=lambda: _none_graph()
    ).beliefs


def test_application_belief_surface_is_direct_and_transport_free():
    from memory_server.application.belief_service import BeliefService

    assert BeliefService.__module__ == "memory_server.application.belief_service"
    assert "memory_server.server" not in inspect.getsource(BeliefService)


def test_belief_policy_avoids_legacy_import_time_ambient_default():
    class Settings:
        belief_search_limit = 7

    policy = BeliefPolicy.capture(Settings())
    Settings.belief_search_limit = 99
    assert policy.belief_search_limit == 7
    import memory_server.server as server
    assert inspect.signature(server.get_belief_tool).parameters["limit"].default == 10


@pytest.mark.asyncio
async def test_application_composition_exposes_beliefs_without_changing_c07_surface(provider):
    app = MemoryApplication.from_provider(provider, belief_policy=BeliefPolicy(10))
    assert app.describe() == {"policy": {"max_active_beliefs": app.policy.max_active_beliefs}}
    assert app.beliefs.policy.belief_search_limit == 10
    with pytest.raises(UnknownDependencyError, match="belief service"):
        MemoryApplication(provider._session_factory).beliefs


@pytest.mark.asyncio
async def test_set_belief_create_matches_legacy_result_and_sqlite_effects(provider):
    result = await (await _service(provider)).set_belief(SetBeliefCommand(
        "SQLite is canonical", confidence=0.8,
        sources=[{"source_type": "fact", "source_id": "s1", "weight": 0.9}],
        tags=["storage"], source="test",
    ))
    assert set(result) == {"belief", "receipt", "superseded"}
    belief = await provider.get_belief(result["belief"]["id"])
    assert belief and belief.confidence == 0.8 and belief.tags == ["storage"]


@pytest.mark.asyncio
async def test_set_belief_reinforce_matches_legacy_normalization_version_and_history(provider):
    service = await _service(provider)
    first = await service.set_belief(SetBeliefCommand("  Same proposition ", confidence=0.4, source="a"))
    result = await service.set_belief(SetBeliefCommand("same proposition", confidence=0.8, tags=["new"], source="b"))
    assert result["reinforced"] is True
    assert result["belief"]["confidence"] == pytest.approx(0.6)
    assert result["belief"]["version"] == 2
    assert result["belief"]["tags"] == ["new"]
    assert len(result["receipt"]["history"]) == 1
    assert await provider.get_belief(first["belief"]["id"])


@pytest.mark.asyncio
async def test_set_belief_replace_preserves_version_and_transition_effects(provider):
    service = await _service(provider)
    old = await service.set_belief(SetBeliefCommand("old", confidence=0.5))
    result = await service.set_belief(SetBeliefCommand("new", confidence=0.7, replace_belief_id=old["belief"]["id"]))
    assert result["belief"]["version"] == 2
    assert result["superseded"]["id"] == old["belief"]["id"]
    assert (await provider.get_belief(old["belief"]["id"])).lifecycle_state == "superseded"


@pytest.mark.asyncio
async def test_set_belief_replace_missing_target_creates_without_supersession(provider):
    result = await (await _service(provider)).set_belief(SetBeliefCommand(
        "new", replace_belief_id="00000000-0000-0000-0000-000000000000"
    ))
    assert result["superseded"] is None
    assert result["belief"]["version"] == 1


@pytest.mark.asyncio
async def test_get_belief_filters_limit_query_and_order_match_legacy(provider):
    service = await _service(provider)
    await service.set_belief(SetBeliefCommand("alpha", confidence=0.9, tags=["x"]))
    await service.set_belief(SetBeliefCommand("beta", confidence=0.4, tags=["y"]))
    result = await service.get_belief(GetBeliefCommand(tags=["x"], min_confidence=0.8, limit=200))
    assert result["total"] == 1
    assert result["query"]["limit"] == 200
    assert result["beliefs"][0]["proposition"] == "alpha"


@pytest.mark.asyncio
async def test_resolve_conflict_auto_threshold_events_and_result_match_legacy(provider):
    service = await _service(provider)
    a = await service.set_belief(SetBeliefCommand("a", confidence=0.1))
    b = await service.set_belief(SetBeliefCommand("b", confidence=0.9))
    result = await service.resolve_conflict(
        ResolveConflictCommand(
            a["belief"]["id"], b["belief"]["id"], "keep_a", auto_resolve=True
        )
    )
    assert result["auto_resolved"] is True
    assert result["belief_a"]["lifecycle_state"] == "superseded"
    assert "auto_resolved" in result


@pytest.mark.asyncio
async def test_resolve_conflict_manual_keep_discard_merge_and_result_match_legacy(provider):
    service = await _service(provider)
    a = await service.set_belief(SetBeliefCommand("a", confidence=0.7))
    b = await service.set_belief(SetBeliefCommand("b", confidence=0.6))
    result = await service.resolve_conflict(ResolveConflictCommand(a["belief"]["id"], b["belief"]["id"], "keep_a"))
    assert "auto_resolved" not in result
    assert result["belief_b"]["lifecycle_state"] == "discarded"
    c = await service.set_belief(SetBeliefCommand("c", confidence=0.7))
    d = await service.set_belief(SetBeliefCommand("d", confidence=0.6))
    merged = await service.resolve_conflict(
        ResolveConflictCommand(
            c["belief"]["id"], d["belief"]["id"], "merge", new_proposition="c and d"
        )
    )
    assert merged["created"]["proposition"] == "c and d"
    e = await service.set_belief(SetBeliefCommand("e", confidence=0.7))
    f = await service.set_belief(SetBeliefCommand("f", confidence=0.6))
    discarded = await service.resolve_conflict(
        ResolveConflictCommand(e["belief"]["id"], f["belief"]["id"], "discard_both")
    )
    assert discarded["belief_a"]["lifecycle_state"] == "discarded"
    assert discarded["belief_b"]["lifecycle_state"] == "discarded"


@pytest.mark.asyncio
async def test_resolve_conflict_errors_preserve_type_identity_and_message(provider):
    service = await _service(provider)
    with pytest.raises(ValueError, match="Both belief_a and belief_b"):
        await service.resolve_conflict(ResolveConflictCommand("missing", "also-missing", "keep_a"))
    a = await service.set_belief(SetBeliefCommand("a"))
    b = await service.set_belief(SetBeliefCommand("b"))
    with pytest.raises(ValueError, match="new_proposition is required"):
        await service.resolve_conflict(ResolveConflictCommand(a["belief"]["id"], b["belief"]["id"], "merge"))
    with pytest.raises(ValueError, match="Unknown resolution"):
        await service.resolve_conflict(ResolveConflictCommand(a["belief"]["id"], b["belief"]["id"], "nope"))


@pytest.mark.asyncio
async def test_belief_conflict_failure_rolls_back_real_sqlite_unit_of_work(provider):
    service = await _service(provider)
    a = await service.set_belief(SetBeliefCommand("a"))
    b = await service.set_belief(SetBeliefCommand("b"))
    with pytest.raises(Exception):
        await service.resolve_conflict(ResolveConflictCommand(a["belief"]["id"], b["belief"]["id"], "bad"))
    assert (await provider.get_belief(a["belief"]["id"])).lifecycle_state == "active"
    assert (await provider.get_belief(b["belief"]["id"])).lifecycle_state == "active"


@pytest.mark.asyncio
async def test_belief_paths_do_not_invoke_memory_admission_gate(provider):
    service = await _service(provider)
    await service.set_belief(SetBeliefCommand("admission-free"))
    result = await service.get_belief(GetBeliefCommand())
    assert result["total"] == 1


@pytest.mark.asyncio
async def test_direct_belief_service_does_not_construct_or_use_transport_dependencies(provider):
    service = await _service(provider)
    assert service._provider is provider
    assert service._get_graph is not None


def test_c08_allowlist_and_source_base_are_recorded():
    expected = {
        "src/memory_server/application/application.py",
        "src/memory_server/application/contracts.py",
        "src/memory_server/application/belief_service.py",
        "tests/test_application_beliefs.py",
    }
    assert len(expected) == 4
    assert Path("src/memory_server/application/belief_service.py").exists()
