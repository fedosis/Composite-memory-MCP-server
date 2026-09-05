"""Tests for belief lifecycle integration (Card 001).

Tests the lifecycle state transitions, decay, and LIFECYCLE_MULTIPLIER
integration for belief-specific states.
"""

from datetime import datetime, timedelta, timezone

import pytest

from memory_server.evaluation.confidence import LIFECYCLE_MULTIPLIER
from memory_server.evaluation.decay import PER_TYPE_TTL, DecayEngine
from memory_server.evaluation.validator import (
    _VALID_TRANSITIONS,
    is_valid_transition,
    normalize_lifecycle_state,
)
from memory_server.evaluation.validator import (
    Validator as EvValidator,
)
from memory_server.models.receipt import LifecycleState

# TTL in hours for testing
ONE_HOUR = 1.0 / 24.0
TWO_HOURS = 2.0 / 24.0


@pytest.fixture
def engine() -> DecayEngine:
    return DecayEngine()


class TestLifecycleStateEnum:
    """LifecycleState enum must include belief-specific states."""

    def test_belief_states_present(self):
        assert hasattr(LifecycleState, "SUPERSEDED")
        assert hasattr(LifecycleState, "CONTRADICTED")
        assert hasattr(LifecycleState, "DISCARDED")

    def test_belief_state_values(self):
        assert LifecycleState.SUPERSEDED.value == "superseded"
        assert LifecycleState.CONTRADICTED.value == "contradicted"
        assert LifecycleState.DISCARDED.value == "discarded"

    def test_belief_state_enum_members(self):
        """Belief-specific states are members of the enum."""
        assert "superseded" in LifecycleState._value2member_map_
        assert "contradicted" in LifecycleState._value2member_map_
        assert "discarded" in LifecycleState._value2member_map_


class TestLifecycleTransitions:
    """Transition matrix from validator must include belief-specific rules."""

    def test_active_to_superseded(self):
        assert is_valid_transition("active", "superseded")

    def test_active_to_contradicted(self):
        assert is_valid_transition("active", "contradicted")

    def test_active_to_discarded(self):
        assert is_valid_transition("active", "discarded")

    def test_superseded_to_stale(self):
        assert is_valid_transition("superseded", "stale")

    def test_superseded_to_discarded(self):
        assert is_valid_transition("superseded", "discarded")

    def test_contradicted_to_active(self):
        assert is_valid_transition("contradicted", "active")

    def test_contradicted_to_stale(self):
        assert is_valid_transition("contradicted", "stale")

    def test_contradicted_to_discarded(self):
        assert is_valid_transition("contradicted", "discarded")

    def test_discarded_to_archived(self):
        assert is_valid_transition("discarded", "archived")

    def test_forward_only_guarantee(self):
        """Belief states follow the lifecycle forward."""
        # Cannot go backward
        assert not is_valid_transition("superseded", "active")
        assert not is_valid_transition("discarded", "active")
        assert not is_valid_transition("contradicted", "superseded")

    def test_valid_transitions_contains_belief_keys(self):
        assert "superseded" in _VALID_TRANSITIONS
        assert "contradicted" in _VALID_TRANSITIONS
        assert "discarded" in _VALID_TRANSITIONS

    def test_superseded_transition_set(self):
        assert _VALID_TRANSITIONS["superseded"] == {"stale", "discarded"}

    def test_contradicted_transition_set(self):
        assert _VALID_TRANSITIONS["contradicted"] == {"active", "stale", "discarded"}

    def test_discarded_transition_set(self):
        assert _VALID_TRANSITIONS["discarded"] == {"archived"}


class TestLifecycleMultiplier:
    """Belief-specific lifecycle multipliers in confidence engine."""

    def test_superseded_multiplier(self):
        assert LIFECYCLE_MULTIPLIER.get("superseded") == 0.3

    def test_contradicted_multiplier(self):
        assert LIFECYCLE_MULTIPLIER.get("contradicted") == 0.3

    def test_discarded_multiplier(self):
        assert LIFECYCLE_MULTIPLIER.get("discarded") == 0.0

    def test_active_multiplier(self):
        assert LIFECYCLE_MULTIPLIER.get("active") == 1.0

    def test_stale_multiplier(self):
        assert LIFECYCLE_MULTIPLIER.get("stale") == 0.6


class TestPerTypeTTL:
    """PER_TYPE_TTL must include belief type."""

    def test_belief_ttl(self):
        assert "belief" in PER_TYPE_TTL
        assert PER_TYPE_TTL["belief"] == 180.0

    def test_belief_ttl_value(self):
        assert PER_TYPE_TTL["belief"] == 180.0


class TestDecayEngineBelief:
    """DecayEngine tick() must handle belief-specific states."""

    def test_superseded_decays_to_stale(self):
        """A superseded belief at 70%+ TTL transitions to stale."""
        v = EvValidator()
        hour_engine = DecayEngine(
            per_type_ttl={"belief": ONE_HOUR},
            validator=v,
        )

        past = datetime.now(timezone.utc) - timedelta(hours=1.5)  # 150% of ONE_HOUR
        hour_engine.register(
            item_id="belief-1",
            item_type="belief",
            created_at=past,
            lifecycle_state="superseded",
        )
        # Manually set lifecycle state in the validator
        v.deprecate("belief-1", reason="Test superseded decay")

        new_state = hour_engine.tick("belief-1")
        assert new_state == "stale"

    def test_contradicted_decays_to_stale(self):
        """A contradicted belief at 70%+ TTL transitions to stale."""
        v = EvValidator()
        hour_engine = DecayEngine(
            per_type_ttl={"belief": ONE_HOUR},
            validator=v,
        )

        past = datetime.now(timezone.utc) - timedelta(hours=1.5)
        hour_engine.register(
            item_id="belief-2",
            item_type="belief",
            created_at=past,
            lifecycle_state="contradicted",
        )

        new_state = hour_engine.tick("belief-2")
        assert new_state == "stale", f"Expected stale, got {new_state}"

    def test_discarded_decays_to_archived(self):
        """A discarded belief at 100%+ TTL transitions to archived."""
        v = EvValidator()
        hour_engine = DecayEngine(
            per_type_ttl={"belief": ONE_HOUR},
            validator=v,
        )

        past = datetime.now(timezone.utc) - timedelta(hours=2)  # 200% of ONE_HOUR
        hour_engine.register(
            item_id="belief-3",
            item_type="belief",
            created_at=past,
            lifecycle_state="discarded",
        )

        new_state = hour_engine.tick("belief-3")
        assert new_state == "archived", f"Expected archived, got {new_state}"

    def test_active_belief_decays_normally(self):
        """An active belief can still decay to stale like other types."""
        v = EvValidator()
        hour_engine = DecayEngine(
            per_type_ttl={"belief": ONE_HOUR},
            validator=v,
        )

        past = datetime.now(timezone.utc) - timedelta(hours=1.5)  # 150% of ONE_HOUR
        hour_engine.register(
            item_id="belief-4",
            item_type="belief",
            created_at=past,
            lifecycle_state="active",
        )

        new_state = hour_engine.tick("belief-4")
        assert new_state == "stale", f"Expected stale, got {new_state}"

    def test_fresh_belief_no_transition(self):
        """A fresh belief should not transition."""
        v = EvValidator()
        hour_engine = DecayEngine(
            per_type_ttl={"belief": ONE_HOUR},
            validator=v,
        )

        now = datetime.now(timezone.utc)
        hour_engine.register(
            item_id="belief-5",
            item_type="belief",
            created_at=now,
            lifecycle_state="active",
        )

        new_state = hour_engine.tick("belief-5")
        assert new_state is None, f"Expected None, got {new_state}"

    def test_normalize_belief_state(self):
        """normalize_lifecycle_state should pass belief states through."""
        assert normalize_lifecycle_state("superseded") == "superseded"
        assert normalize_lifecycle_state("contradicted") == "contradicted"
        assert normalize_lifecycle_state("discarded") == "discarded"

    def test_register_belief_with_decay_engine(self, engine):
        """Registering a belief with DecayEngine works."""
        engine.register(
            item_id="belief-reg-1",
            item_type="belief",
            lifecycle_state="active",
        )
        assert engine.get_lifecycle_state("belief-reg-1") == "active"


@pytest.fixture(params=["fact", "belief"])
async def cas_file_store(tmp_path, request):
    from memory_server.models import Belief, Fact
    from memory_server.providers.sqlite_provider import SQLiteProvider

    url = f"sqlite+aiosqlite:///{tmp_path / 'boundary.db'}"
    writer, observer = SQLiteProvider(url=url), SQLiteProvider(url=url)
    await writer.initialize()
    await observer.initialize()
    kind = request.param
    if kind == "fact":
        memory = Fact(id="boundary-fact", subject="CAS", predicate="has", object="one owner")
        await writer.create_fact(memory)
    else:
        memory = Belief(id="boundary-belief", proposition="CAS has one owner")
        await writer.create_belief(memory)
    try:
        yield writer, observer, kind, memory
    finally:
        await observer.close()
        await writer.close()


async def _read_cas_memory(provider, kind, memory_id):
    if kind == "fact":
        return await provider.get_fact(memory_id)
    return await provider.get_belief(memory_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["event", "commit"])
async def test_file_sqlite_real_failure_rolls_back_entire_transition(cas_file_store, failure):
    from sqlalchemy import event, text
    from sqlalchemy.exc import IntegrityError

    from memory_server.services.lifecycle_service import LifecycleService

    writer, observer, kind, memory = cas_file_store
    counts = {"event_insert": 0, "commit": 0}

    def enable_fk(dbapi_connection, connection_record, connection_proxy):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    event.listen(writer.engine.sync_engine, "checkout", enable_fk)
    async with await writer._get_session() as session:
        if failure == "event":
            await session.execute(text(
                "CREATE TRIGGER reject_event BEFORE INSERT ON lifecycle_events "
                "BEGIN SELECT RAISE(ABORT, 'event rejected'); END"
            ))
        else:
            await session.execute(text("CREATE TABLE cas_parent (id INTEGER PRIMARY KEY)"))
            await session.execute(text(
                "CREATE TABLE cas_deferred (parent_id INTEGER REFERENCES cas_parent(id) "
                "DEFERRABLE INITIALLY DEFERRED)"
            ))
            await session.execute(text(
                "CREATE TRIGGER reject_commit AFTER INSERT ON lifecycle_events "
                "BEGIN INSERT INTO cas_deferred VALUES (42); END"
            ))
        await session.commit()

    def after_sql(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO lifecycle_events"):
            counts["event_insert"] += 1

    def before_commit(conn):
        counts["commit"] += 1

    event.listen(writer.engine.sync_engine, "after_cursor_execute", after_sql)
    event.listen(writer.engine.sync_engine, "commit", before_commit)
    try:
        with pytest.raises(IntegrityError, match="event rejected|FOREIGN KEY constraint failed"):
            await LifecycleService(writer).transition(memory.id, kind, "stale", expected_version=1)
    finally:
        event.remove(writer.engine.sync_engine, "after_cursor_execute", after_sql)
        event.remove(writer.engine.sync_engine, "commit", before_commit)
        event.remove(writer.engine.sync_engine, "checkout", enable_fk)
    if failure == "commit":
        assert counts == {"event_insert": 1, "commit": 1}
    else:
        assert counts == {"event_insert": 0, "commit": 0}
    current = await _read_cas_memory(observer, kind, memory.id)
    assert (current.lifecycle_state, current.version) == ("active", 1)
    async with await observer._get_session() as session:
        assert (await session.execute(text("SELECT count(*) FROM lifecycle_events"))).scalar_one() == 0
        assert (await session.execute(text("SELECT count(*) FROM lifecycle_states"))).scalar_one() == 0
        if failure == "commit":
            assert (await session.execute(text("SELECT count(*) FROM cas_deferred"))).scalar_one() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("finish", ["commit", "rollback", "batch_conflict"])
async def test_session_api_leaves_whole_uow_to_caller(cas_file_store, finish):
    from sqlalchemy import text

    from memory_server.services.lifecycle_service import LifecycleService, LifecycleTransitionRequest

    writer, observer, kind, memory = cas_file_store
    service = LifecycleService(writer)
    request = LifecycleTransitionRequest(memory.id, kind, "stale", expected_version=1)
    async with await writer._get_session() as session:
        if finish == "batch_conflict":
            with pytest.raises(ValueError, match="expected_version mismatch"):
                async with session.begin():
                    await service.transition_many_in_session(session, [request, request])
        else:
            result = await service.transition_in_session(session, request)
            assert (result.memory.lifecycle_state, result.memory.version) == ("stale", 2)
            before = await _read_cas_memory(observer, kind, memory.id)
            assert (before.lifecycle_state, before.version) == ("active", 1)
            assert session.in_transaction()
            if finish == "commit":
                await session.commit()
            else:
                await session.rollback()
    current = await _read_cas_memory(observer, kind, memory.id)
    expected = ("stale", 2) if finish == "commit" else ("active", 1)
    assert (current.lifecycle_state, current.version) == expected
    async with await observer._get_session() as session:
        expected_count = int(finish == "commit")
        assert (await session.execute(text("SELECT count(*) FROM lifecycle_events"))).scalar_one() == expected_count
        assert (await session.execute(text("SELECT count(*) FROM lifecycle_states"))).scalar_one() == expected_count



@pytest.mark.asyncio
async def test_cas_zero_affected_rows_is_conflict(cas_file_store):
    from sqlalchemy import text

    from memory_server.services.lifecycle_service import LifecycleService

    writer, observer, kind, memory = cas_file_store
    table = "facts" if kind == "fact" else "beliefs"
    async with await writer._get_session() as session:
        await session.execute(text(
            f"CREATE TRIGGER ignore_transition BEFORE UPDATE ON {table} "
            "BEGIN SELECT RAISE(IGNORE); END"
        ))
        await session.commit()
    with pytest.raises(ValueError, match="expected_version mismatch"):
        await LifecycleService(writer).transition(memory.id, kind, "stale", expected_version=1)
    current = await _read_cas_memory(observer, kind, memory.id)
    assert (current.lifecycle_state, current.version) == ("active", 1)
    async with await observer._get_session() as session:
        assert (await session.execute(text("SELECT count(*) FROM lifecycle_events"))).scalar_one() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_version", ["garbage", "1.5", True])
async def test_unknown_expected_version_is_rejected(cas_file_store, expected_version):
    from memory_server.services.lifecycle_service import LifecycleService

    writer, observer, kind, memory = cas_file_store
    with pytest.raises(ValueError, match="Unsupported lifecycle version"):
        await LifecycleService(writer).transition(memory.id, kind, "stale", expected_version=expected_version)
    current = await _read_cas_memory(observer, kind, memory.id)
    assert (current.lifecycle_state, current.version) == ("active", 1)


@pytest.mark.asyncio
async def test_propagation_wins_against_stale_dependent_transition(cas_file_store):
    import asyncio

    from storage.repositories import LifecycleRepository, RelationRepository

    from memory_server.models import Fact
    from memory_server.services.lifecycle_service import LifecycleService

    writer, observer, kind, child = cas_file_store
    parent = Fact(id="cas-parent", subject="Parent", predicate="supports", object="Child")
    await writer.create_fact(parent)
    async with await writer._get_session() as session:
        await RelationRepository(session).create(child.id, parent.id, "derived_from")
        await session.commit()
    loaded, release = asyncio.Event(), asyncio.Event()

    class PausedService(LifecycleService):
        async def _load_memory(self, *args, **kwargs):
            result = await super()._load_memory(*args, **kwargs)
            loaded.set()
            await asyncio.wait_for(release.wait(), timeout=5)
            return result

    task = asyncio.create_task(PausedService(observer).transition(child.id, kind, "stale", expected_version=1))
    try:
        await asyncio.wait_for(loaded.wait(), timeout=5)
        result = await LifecycleService(writer).transition(parent.id, "fact", "superseded", expected_version=1)
        assert len(result.propagated) == 1
        release.set()
        with pytest.raises(ValueError, match="expected_version mismatch"):
            await asyncio.wait_for(task, timeout=5)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
    current = await _read_cas_memory(observer, kind, child.id)
    assert (current.lifecycle_state, current.version) == ("active", 2)
    assert current.confidence == pytest.approx(child.confidence * 0.8)
    async with await observer._get_session() as session:
        events = await LifecycleRepository(session).get_events(child.id)
    assert len(events) == 1
    assert events[0]["reason"] == "parent_invalidated"
