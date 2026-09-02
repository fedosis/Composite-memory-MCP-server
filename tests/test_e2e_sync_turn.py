"""No-network E2E tests for the REAL sync_turn -> WriterQueue ->
_handle_batch_write -> MemoryIngestionService.learn() -> file SQLite path
(card B3c-e2e, tests/test_e2e_sync_turn.py — the ONLY file this card creates).

Approved implementation contract: workspace/cmms-sync-turn-e2e/DETAIL.md rev 3.
Behavior seams are STRICTLY the LLM callable (factory return) and the
network/outbox boundary; sync_turn, WriterQueue, _handle_batch_write, learn,
repositories, and SQLite writes are never stubbed (SPEC lines 22).
"""

import logging
import os
import socket
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import httpx
import pytest
from storage.repositories import DecisionRepository, FactRepository, ReceiptRepository

# ---- collection-time environment contract (D-F1) -------------------------
# provider.py evaluates get_settings() at import (provider.py:81:
# DEFAULT_DB_URL = get_settings().db_url); get_settings() is @lru_cache'd
# (settings.py:361-364) and Settings() validates EVERY MEMORY_SERVER_* var —
# a hostile host value (e.g. MEMORY_SERVER_VECTOR_BACKEND=bogus) raises a
# pydantic ValidationError at import, BEFORE any fixture can run. Remove ALL
# MEMORY_SERVER_* vars NOW, before any memory_server import below, so a
# direct focused pytest run is collection-safe without the G1 wrapper.
# _restore_ambient_env (§5.5) restores the snapshot at module teardown.
_ENV_SNAPSHOT: dict[str, str] = {}
for _k in [k for k in os.environ if k.startswith("MEMORY_SERVER_")]:
    _ENV_SNAPSHOT[_k] = os.environ.pop(_k)

import memory_server.plugins.hermes.provider as provider_mod  # noqa: E402
from memory_server.extractors.llm_response import ExtractedResult  # noqa: E402
from memory_server.plugins.hermes.provider import HermesProvider, _run_async  # noqa: E402

TEXT_DOCKER = "Docker is container"
SOURCE_TURN = "hermes_turn_e2e"
TURN_ID = "e2e"


def test_extract_turn_text_filters_non_conversational_roles():
    messages = [
        {"role": "user", "content": "user text"},
        {"role": "tool", "content": "n if event_path is not None"},
        {"role": "assistant", "content": "assistant text"},
        {"role": "system", "content": "system instructions"},
    ]

    assert HermesProvider._extract_turn_text(messages) == "user text\nassistant text"


def test_extract_turn_text_keeps_legacy_dict_branch_unchanged():
    assert HermesProvider._extract_turn_text({
        "user_content": "user text",
        "assistant_content": "assistant text",
    }) == "user text\nassistant text"


def test_extract_turn_text_accepts_only_structured_roleless_legacy_items():
    messages = [
        {"user_content": "legacy user", "assistant_content": "legacy assistant"},
        {"content": "ambiguous roleless content"},
    ]

    assert HermesProvider._extract_turn_text(messages) == (
        "legacy user\nlegacy assistant"
    )


def test_extract_turn_text_flattens_text_blocks():
    messages = [{
        "role": "assistant",
        "content": [
            {"type": "text", "text": "first"},
            {"type": "image", "url": "ignored"},
            {"type": "text", "text": "second"},
        ],
    }]

    assert HermesProvider._extract_turn_text(messages) == "first\nsecond"


# ---------------------------------------------------------------------------
# §1.1 Cleanup registry (suite-owned resources; SPEC fixture step 1)
# ---------------------------------------------------------------------------
_CLEANUP_REGISTRY: list[dict] = []   # [{db_url, provider}] — suite-owned resources


def _register(db_url: str, provider) -> None:
    _CLEANUP_REGISTRY.append({"db_url": db_url, "provider": provider})


def _unregister(provider) -> None:
    for i, entry in enumerate(_CLEANUP_REGISTRY):
        if entry["provider"] is provider:
            _CLEANUP_REGISTRY.pop(i)
            return


def _db_url_for(provider) -> str | None:      # registry still owns the temp URL
    for entry in _CLEANUP_REGISTRY:
        if entry["provider"] is provider:
            return entry["db_url"]
    return None


def _drain_registry() -> None:                # module-scoped autouse finalizer backstop
    """Tear down every provider whose test never reached its own finally.

    D-F3: each entry STAYS registered through its ENTIRE `_e2e_teardown()`
    call — `_db_url_for(provider)` must resolve the temp path for teardown's
    deletion step, so clearing the registry first would silently skip it.
    The entry is removed ONLY AFTER the cleanup attempt completes, via
    `_unregister` in a nested finally. Backstop errors are aggregated WITHOUT
    destroying any entry's metadata; each entry is still removed after its
    attempt, so no entry can be torn down twice.
    """
    errors: list[str] = []
    while _CLEANUP_REGISTRY:
        provider = _CLEANUP_REGISTRY[-1]["provider"]   # LIFO ordering preserved
        try:
            _e2e_teardown(provider)   # entry still registered -> db_url visible
        except Exception as exc:      # noqa: BLE001 - aggregated below
            errors.append(str(exc))
        finally:
            _unregister(provider)     # removed ONLY after the cleanup attempt
    if errors:
        raise AssertionError("cleanup backstop errors: " + "; ".join(errors))


# ---------------------------------------------------------------------------
# §1.2 Fixture: env sanitization FIRST + tripwire + sentinel + factory seam
#      + initialize (SPEC "Mandatory fixture patch order" steps 0-4)
# ---------------------------------------------------------------------------
def _make_provider(tmp_path, monkeypatch, *, spy=None, extraction_cfg=None,
                   flush_interval: float = 0.05):
    # ---- step 0 (VERY FIRST executable fixture operation, D-F1): env ----
    # ---- sanitization BEFORE any provider construction --------------------
    # initialize() STEP 2 calls get_settings() (provider.py:354) and
    # HermesPluginConfig.from_dict gives MEMORY_SERVER_DB_URL/MAX_FACTS env
    # precedence over the config dict (config.py:96-105); the resolver reads
    # the five MEMORY_SERVER_* extraction knobs on every resolution
    # (resolver.py:48-53). The repo's autouse conftest fixture clears the
    # get_settings lru_cache before every test (tests/conftest.py:6-25), so
    # initialize() builds a FRESH Settings from the CURRENT env — a hostile
    # leftover would flow straight in. Never blanked to "" — empty strings
    # break int parsing in plugin config (config.py:103-105). monkeypatch
    # restores the original env at test teardown; the module-level snapshot
    # (§5.1) already removed everything at collection, so this loop only
    # defends the runtime window (belt and braces, P-F2).
    for var in [k for k in os.environ if k.startswith("MEMORY_SERVER_")]:
        monkeypatch.delenv(var, raising=False)

    # ---- step 1: unique temp file SQLite URL + cleanup registry -------------
    db_url = f"sqlite+aiosqlite:///{tmp_path}/e2e.db"
    provider = HermesProvider()
    _register(db_url, provider)   # BEFORE initialize (W2: failed-init reachable)

    # ---- step 2: network deny + patch _supports_background_outbox at the
    # import site BEFORE initialize (SPEC gates 2-3) --------------------------
    _install_network_tripwire(monkeypatch)   # idempotent under the autouse fixture
    sentinel = {"calls": 0}

    def sentinel_supports(db_url):           # patch-order sentinel
        sentinel["calls"] += 1
        return False

    monkeypatch.setattr(provider_mod, "_supports_background_outbox", sentinel_supports)

    # ---- step 3: resolver stays REAL; only the factory return is replaced
    # for spy cases; regex mode uses the REAL factory fast path (None) -------
    if spy is not None:
        def recording_factory(cfg, *, hermes_home=""):
            return spy
        monkeypatch.setattr(provider_mod, "build_llm_extractor_from_cfg",
                            recording_factory)

    # ---- step 4: initialize (ONLY now) --------------------------------------
    config = {"db_url": db_url,
              "writer": {"flush_interval": flush_interval, "max_batch": 50}}
    if extraction_cfg:
        config.update(extraction_cfg)
    try:
        provider.initialize(session_id=TURN_ID, config=config,
                            hermes_home=str(tmp_path))
    except Exception:
        raise     # W2: provider stays registered; test finally tears down

    # patch-order sentinel PROVEN (SPEC gate 3): initialize() evaluated the
    # PATCHED function exactly once (provider.py:386).
    assert sentinel["calls"] == 1, (
        f"patch-order sentinel: _supports_background_outbox called "
        f"{sentinel['calls']}x (must be 1 — patch installed BEFORE initialize?)"
    )
    # no-network guarantee: the outbox/embedder/vector branch never ran
    assert provider._outbox_worker is None and provider._outbox_task is None
    # P-F2: the REAL provider URL equals the registered temp URL (no SQLite
    # implementation replaced; SQLiteProvider._url is set from the constructor
    # arg, sqlite_provider.py:61). This assertion is also the D-F1 proof that
    # no env-derived db_url leaked into the config.
    assert provider._provider is not None
    assert provider._provider._url == db_url, (
        f"provider URL {provider._provider._url!r} != registered temp URL "
        f"{db_url!r} (MEMORY_SERVER_DB_URL env override leaked into config?)"
    )
    if spy is not None:
        assert provider._llm_extractor is spy, "spy not cached by initialize"
    return provider


# ---------------------------------------------------------------------------
# §2.1 Turn driver + spy result helpers
# ---------------------------------------------------------------------------
def _turn(provider, text: str = TEXT_DOCKER) -> None:
    provider.sync_turn(user_content="", assistant_content="", session_id=TURN_ID,
                       messages=[{"role": "user", "content": text},
                                 {"role": "assistant", "content": ""}])


def _turn_without_messages(provider, user_content: str, assistant_content: str = "") -> None:
    provider.sync_turn(
        user_content=user_content,
        assistant_content=assistant_content,
        session_id=TURN_ID,
    )


def _docker_result(confidence: float = 0.85) -> ExtractedResult:
    return ExtractedResult(
        facts=({"subject": "Docker", "predicate": "is", "object": "container",
                "confidence": confidence},),
        decisions=(),
    )


# ---------------------------------------------------------------------------
# §3 Bounded persistence predicate design (P-F3, SPEC gate 4) + DB read
#    helpers over the REAL session factory
# ---------------------------------------------------------------------------
def _bounded_wait(label: str, predicate, *, provider, deadline: float = 10.0,
                  interval: float = 0.05) -> None:
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(interval)
    w = provider._writer
    raise AssertionError(
        f"{label}: timed out queued={w.total_queued} flushed={w.total_flushed} "
        f"failed={w.total_failed} last_writer_error={_last_writer_error()!r}"
    )


def _fact_row(provider, subject: str = "Docker"):
    async def _go():
        async with provider._provider._session_factory() as session:
            rows = await FactRepository(session).search(subject=subject)
            return rows[0] if rows else None
    return _run_async(_go(), timeout=10.0)


def _count_facts(provider, subject: str | None = None) -> int:
    async def _go():
        async with provider._provider._session_factory() as session:
            repo = FactRepository(session)
            if subject is None:
                return len(await repo.search(limit=500))
            return len(await repo.search(subject=subject))
    return _run_async(_go(), timeout=10.0)


def _count_decisions(provider) -> int:
    async def _go():
        async with provider._provider._session_factory() as session:
            return len(await DecisionRepository(session).search(limit=500))
    return _run_async(_go(), timeout=10.0)


def _count_receipts(provider, source: str = SOURCE_TURN) -> int:
    async def _go():
        async with provider._provider._session_factory() as session:
            return len(await ReceiptRepository(session).search(source=source))
    return _run_async(_go(), timeout=10.0)


def _receipt_for(provider, receipt_id: str):
    async def _go():
        async with provider._provider._session_factory() as session:
            return await ReceiptRepository(session).get(receipt_id)
    return _run_async(_go(), timeout=10.0)


# ---------------------------------------------------------------------------
# §4.1 Teardown / cleanup matrix (SPEC fixture step 6; PLAN D4)
# ---------------------------------------------------------------------------
def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def _e2e_teardown(provider, *, step_log: list | None = None,
                  primary: BaseException | None = None,
                  preconditions: tuple = ()) -> None:
    """Nested error-accumulating teardown; every step runs even after an
    earlier step raises; errors are raised ONCE at the end as an aggregate,
    chained `from primary` when a primary exception is active.

    Registry lifecycle (D-F3): this helper NEVER touches the registry entry —
    it stays available through the whole call so the step-1 capture can read
    the db_url for the step-6 temp-file deletion; `_unregister` (test finally)
    or `_drain_registry` (backstop) removes the entry only AFTER this function
    returns or raises.
    """
    errors: list[str] = []

    def run(label: str, fn) -> None:
        if step_log is not None:
            step_log.append(label)
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 - collected into aggregate
            errors.append(f"{label}: {exc}")

    def require_step(label: str, cond: bool, msg: str) -> None:
        run(label, lambda: _require(cond, msg))

    # 0. preconditions (SPEC scenario 7: join the timed-out to_thread fake by
    # an explicit event BEFORE any teardown step / engine dispose)
    for label, fn in preconditions:
        run(label, lambda: _require(bool(fn()), f"{label} precondition failed"))

    # 1. CAPTURE the resources needed for POST-CLEARANCE proof (None-guarded,
    # D-W3): writer_task/engine/pool/db_path must survive shutdown, which
    # nulls the provider attributes that would otherwise expose them. The
    # registry entry is read but NOT removed — it stays available through the
    # whole call (D-F3). The full PROVIDER POST-STATE pre-assertions
    # (extractor_runtime/llm_extractor/settings/graph/qdrant/lancedb/embedder
    # + outbox handles meaningful pre-state) live in test 7a's body, not here.
    writer = provider._writer
    writer_task = writer._task if writer is not None else None
    sqlite_provider = provider._provider
    engine = sqlite_provider.engine if sqlite_provider is not None else None
    pool_before = engine.pool if engine is not None else None
    db_url = _db_url_for(provider)
    db_path = (Path(db_url[len("sqlite+aiosqlite:///"):])
               if db_url is not None else None)

    # 2. bounded writer flush
    if writer is not None:
        run("writer.flush", lambda: _run_async(writer.flush(), timeout=5.0))

    # 3. session-end hook (flush under its own 30s bound)
    run("on_session_end", lambda: provider.on_session_end([]))

    # 4. provider shutdown (writer task dispose -> provider.close() ->
    #    engine dispose)
    run("shutdown", lambda: provider.shutdown())

    # 5. assertion steps — EVERY provider-owned reference the lifecycle
    #    promises to clear is gone (P2-F2; provider.py:531-584/727)
    require_step("assert _provider cleared", provider._provider is None,
                 "_provider not cleared")
    require_step("assert writer attr cleared", provider._writer is None,
                 "_writer not cleared")
    require_step("assert extractor_runtime cleared",
                 provider._extractor_runtime is None,
                 "_extractor_runtime not cleared")
    require_step("assert llm_extractor cleared",
                 provider._llm_extractor is None, "_llm_extractor not cleared")
    require_step("assert settings cleared", provider._settings is None,
                 "_settings not cleared")
    require_step("assert graph cleared", provider._graph is None,
                 "_graph not cleared")
    require_step("assert qdrant cleared", provider._qdrant is None,
                 "_qdrant not cleared")
    require_step("assert lancedb cleared", provider._lancedb is None,
                 "_lancedb not cleared")
    require_step("assert embedder cleared", provider._embedder is None,
                 "_embedder not cleared")
    require_step("assert outbox handles None",
                 provider._outbox_worker is None and provider._outbox_task is None,
                 "outbox handles retained")
    require_step("assert _initialized False", provider._initialized is False,
                 "_initialized still True")
    require_step("assert _shut_down True", provider._shut_down is True,
                 "_shut_down not set (shutdown did not verify clean)")
    require_step("assert writer task disposed",
                 writer_task is None or writer_task.done(),
                 "writer background task still running")
    if engine is not None and pool_before is not None:
        require_step(
            "assert engine disposed (pool identity + checkedout)",
            engine.pool is not pool_before and engine.pool.checkedout() == 0,
            "engine pool identity unchanged or checked-out connections remain",
        )
    # 6. temp-file CLEANUP — deletion invariant (D-F4). Runs AFTER engine
    #    disposal (step 4). The path was captured in step 1 from the registry
    #    entry, which the caller keeps registered through this whole call
    #    (D-F3). unlink is missing_ok=True: a provider that never
    #    materialized the file (e.g. the 5b gate-drop) still passes; creation
    #    proof is the per-test persistence assertions / 7a's pre-disposal
    #    existence assert.
    if db_path is not None:
        run("remove temp file", lambda: db_path.unlink(missing_ok=True))
        require_step("assert temp file removed", not db_path.exists(),
                     f"temp DB file left behind: {db_path}")

    # 7. aggregate raise — the FIRST failure is never masked and no later
    #    cleanup action was skipped
    if errors:
        aggregate = AssertionError("teardown errors: " + "; ".join(errors))
        if primary is not None:
            raise aggregate from primary
        raise aggregate


# ---------------------------------------------------------------------------
# §4.2 Log capture plumbing (no src seam)
# ---------------------------------------------------------------------------
class _LogCapture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.format(record)  # materialize exc_text like pytest caplog.text
        self.records.append(record)


_WRITER_CAPTURE: _LogCapture | None = None


def _last_writer_error() -> str:
    """Most recent ERROR record on the writer logger, or '' when none."""
    if _WRITER_CAPTURE is None:
        return ""
    for record in reversed(_WRITER_CAPTURE.records):
        if record.levelno >= logging.ERROR:
            return record.getMessage()
    return ""


# ---------------------------------------------------------------------------
# §5.4 Network tripwire (SPEC gate 2)
# ---------------------------------------------------------------------------
def _install_network_tripwire(monkeypatch) -> None:
    def _deny(name: str):
        def _boom(*args, **kwargs):
            raise AssertionError(f"NETWORK TRIPWIRE: {name} reached")
        return _boom

    monkeypatch.setattr(socket.socket, "connect", _deny("socket.socket.connect"))
    monkeypatch.setattr(socket, "create_connection", _deny("socket.create_connection"))
    monkeypatch.setattr(socket, "getaddrinfo", _deny("socket.getaddrinfo"))
    monkeypatch.setattr(httpx.Client, "__init__", _deny("httpx.Client.__init__"))
    monkeypatch.setattr(httpx.AsyncClient, "__init__", _deny("httpx.AsyncClient.__init__"))


# ---------------------------------------------------------------------------
# §5.5 Autouse fixtures (function-scoped unless noted)
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _writer_capture():
    global _WRITER_CAPTURE
    handler = _LogCapture()                       # test-local handler on the writer logger
    wlog = logging.getLogger("memory_server.plugins.hermes.writer")
    old_level = wlog.level
    wlog.addHandler(handler)
    wlog.setLevel(logging.DEBUG)
    _WRITER_CAPTURE = handler
    yield
    wlog.setLevel(old_level)
    wlog.removeHandler(handler)
    _WRITER_CAPTURE = None


@pytest.fixture(autouse=True)
def _network_tripwire(monkeypatch):
    _install_network_tripwire(monkeypatch)
    yield


@pytest.fixture(autouse=True, scope="module")
def _restore_ambient_env():
    """Restore the MEMORY_SERVER_* vars removed at collection time (§5.1).

    Runs after the module's last test. The repo's tests/conftest.py clears
    the get_settings lru_cache around every test, so no stale cached Settings
    can outlive this restore (D-F1); restoring keeps later modules in a
    full-suite run unaffected by this module's strip.
    """
    yield
    os.environ.update(_ENV_SNAPSHOT)
    _ENV_SNAPSHOT.clear()


@pytest.fixture(autouse=True, scope="module")
def _registry_backstop():
    yield
    _drain_registry()


# ---------------------------------------------------------------------------
# §2.2 The ten tests (SPEC scenario matrix: 1, 2, 3, 4a-c, 5, 6, 7a-b)
# ---------------------------------------------------------------------------
def test_e2e_create_then_reinforce(tmp_path, monkeypatch):
    """SPEC scenario 1: turn1 creates, turn2 reinforces the SAME row."""
    provider = None
    try:
        def spy(text):  # noqa: ARG001
            return _docker_result()

        provider = _make_provider(tmp_path, monkeypatch, spy=spy)
        assert provider._llm_extractor is spy
        assert provider._extractor_runtime is not None
        assert provider._extractor_runtime.llm_timeout_seconds == 15.0
        assert provider._extractor_runtime.llm_max_input_chars == 8000
        assert provider._extractor_runtime.llm_confidence_gate == 0.7

        _turn(provider)
        _run_async(provider._writer.flush(), timeout=5.0)
        _bounded_wait("turn1 fact persisted",
                      lambda: _count_facts(provider, "Docker") >= 1,
                      provider=provider)
        assert _count_facts(provider) == 1, "more than one active fact row"
        row1 = _fact_row(provider)
        assert row1 is not None and row1.version == 1
        fact_id = row1.id
        receipt1 = _receipt_for(provider, fact_id)
        assert receipt1 is not None and receipt1.id == fact_id
        assert receipt1.confidence == 0.85
        assert receipt1.source == SOURCE_TURN

        _turn(provider)  # semantically identical second turn
        _run_async(provider._writer.flush(), timeout=5.0)
        _bounded_wait("turn2 reinforced",
                      lambda: (r := _fact_row(provider)) is not None
                      and r.version == 2,
                      provider=provider)
        assert _count_facts(provider) == 1, "duplicate active row created"
        row2 = _fact_row(provider)
        assert row2.id == fact_id, "fact id not stable across reinforcement"
        assert row2.confidence == 0.85, "max(old, new) policy with equal values"
        assert row2.version == 2
        assert row2.updated_at > row1.updated_at, "updated_at not bumped"

        assert _count_receipts(provider) == 1, "single receipt expected"
        # re-read AFTER turn2: the reinforce entry was appended to history by
        # ReceiptRepository.update(..., history=...) (ingestion_service.py:94-96)
        receipt2 = _receipt_for(provider, fact_id)
        assert receipt2 is not None and receipt2.history
        entry = receipt2.history[-1]
        # B1 reinforce entry shape (ingestion_service.py:91-92); timestamp is
        # an ISO string we cannot predict — compare it structurally.
        assert isinstance(entry, dict), entry
        assert entry.get("confidence") == 0.85
        assert entry.get("kind") == "reinforce"
        assert entry.get("source") == SOURCE_TURN
        assert entry.get("previous_confidence") == 0.85
        ts = entry.get("timestamp")
        assert isinstance(ts, str) and datetime.fromisoformat(ts).tzinfo is not None
    finally:
        if provider is not None:
            try:
                _e2e_teardown(provider, primary=sys.exc_info()[1])
            finally:
                _unregister(provider)


def test_e2e_invocation_exactly_once_per_learn(tmp_path, monkeypatch):
    """SPEC scenario 2: one spy invocation per learn(); batching != retries;
    the SAME validated combined result feeds facts AND decisions."""
    provider = None
    try:
        spy_calls: list[str] = []
        text_a, text_b = "Postgres is tool", "Clickhouse is tool"

        def derived_spy(text: str) -> ExtractedResult:
            spy_calls.append(text)
            subject = text.split()[0]
            return ExtractedResult(
                facts=({"subject": subject, "predicate": "is", "object": "tool",
                        "confidence": 0.85},),
                decisions=({"context": f"prefer {subject}", "choice": "use it",
                            "reason": f"{subject} fits", "alternatives": [],
                            "confidence": 0.85},),
            )

        provider = _make_provider(tmp_path, monkeypatch, spy=derived_spy)
        _turn(provider, text_a)
        _turn(provider, text_b)
        _run_async(provider._writer.flush(), timeout=5.0)
        _bounded_wait("both turns processed",
                      lambda: len(spy_calls) == 2
                      and _count_facts(provider, "Postgres") >= 1
                      and _count_facts(provider, "Clickhouse") >= 1,
                      provider=provider)
        assert spy_calls == [text_a + "\n", text_b + "\n"], spy_calls
        assert len(spy_calls) == 2, "callable invoked more than once per learn()"
        assert _count_facts(provider) == 2
        assert _count_decisions(provider) == 2
    finally:
        if provider is not None:
            try:
                _e2e_teardown(provider, primary=sys.exc_info()[1])
            finally:
                _unregister(provider)


def test_e2e_sync_turn_without_messages_persists_fact(tmp_path, monkeypatch):
    """Omitted messages uses the supported turn dict path and persists memory."""
    provider = None
    try:
        provider = _make_provider(tmp_path, monkeypatch, spy=None,
                                  extraction_cfg={"extraction_mode": "regex"})
        _turn_without_messages(provider, user_content=TEXT_DOCKER)
        provider.on_session_switch(new_session_id="next-session")
        _bounded_wait(
            "no-messages fact persisted",
            lambda: _count_facts(provider, "Docker") >= 1 and provider._writer.total_flushed >= 1,
            provider=provider,
        )
        row = _fact_row(provider)
        assert row is not None
        assert (row.subject, row.predicate, row.object) == ("Docker", "is", "container")
        assert provider._writer.total_queued == 1
        assert provider._writer.total_failed == 0
        assert provider._writer.total_requeued == 0
    finally:
        if provider is not None:
            try:
                _e2e_teardown(provider, primary=sys.exc_info()[1])
            finally:
                _unregister(provider)


def test_e2e_regex_fallback_when_no_callable(tmp_path, monkeypatch):
    """SPEC scenario 3: llm_extractor=None -> regex fallback, zero LLM calls."""
    provider = None
    try:
        provider = _make_provider(tmp_path, monkeypatch, spy=None,
                                  extraction_cfg={"extraction_mode": "regex"})
        assert provider._llm_extractor is None, "regex mode must cache None"
        _turn(provider)
        _run_async(provider._writer.flush(), timeout=5.0)
        _bounded_wait("regex fact persisted",
                      lambda: _count_facts(provider, "Docker") >= 1,
                      provider=provider)
        row = _fact_row(provider)
        assert (row.subject, row.predicate, row.object) == ("Docker", "is", "container")
        assert row.confidence == 0.5, "regex facts use confidence 0.5"
        assert _count_facts(provider) == 1
    finally:
        if provider is not None:
            try:
                _e2e_teardown(provider, primary=sys.exc_info()[1])
            finally:
                _unregister(provider)


def test_e2e_callable_error_fallback(tmp_path, monkeypatch, caplog):
    """SPEC scenario 4a: callable exception -> regex fallback, no propagation."""
    provider = None
    try:
        def boom(text):  # noqa: ARG001
            raise RuntimeError("spy boom")

        provider = _make_provider(tmp_path, monkeypatch, spy=boom)
        caplog.set_level(logging.ERROR,
                         logger="memory_server.services.ingestion_service")
        _turn(provider)  # must NOT raise
        _run_async(provider._writer.flush(), timeout=5.0)
        _bounded_wait("regex fallback fact persisted",
                      lambda: _count_facts(provider, "Docker") >= 1,
                      provider=provider)
        assert _fact_row(provider).confidence == 0.5
        assert "LLM extraction failed" in caplog.text
    finally:
        if provider is not None:
            try:
                _e2e_teardown(provider, primary=sys.exc_info()[1])
            finally:
                _unregister(provider)


def test_e2e_callable_timeout_fallback(tmp_path, monkeypatch, caplog):
    """SPEC scenario 4b: callable timeout -> regex fallback; nondefault
    timeout reaches learn (elapsed proof); worker joined by explicit event."""
    provider = None
    release = threading.Event()
    finished = threading.Event()
    try:
        def slow_spy(text):  # noqa: ARG001
            try:
                release.wait(timeout=30)
            finally:
                finished.set()
            return _docker_result()

        provider = _make_provider(tmp_path, monkeypatch, spy=slow_spy,
                                  extraction_cfg={"llm_timeout_seconds": 0.2})
        caplog.set_level(logging.WARNING,
                         logger="memory_server.services.ingestion_service")
        t0 = time.monotonic()
        _turn(provider)
        _run_async(provider._writer.flush(), timeout=5.0)
        _bounded_wait("regex fallback fact persisted",
                      lambda: _count_facts(provider, "Docker") >= 1,
                      provider=provider)
        elapsed = time.monotonic() - t0
        assert elapsed < 2.0, f"timeout did not reach learn: elapsed={elapsed:.2f}s"
        assert "LLM extraction timed out" in caplog.text
    finally:
        release.set()
        if provider is not None:
            try:
                _e2e_teardown(
                    provider, primary=sys.exc_info()[1],
                    preconditions=(("timeout worker joined after release",
                                    lambda: finished.wait(5.0)),),
                )
            finally:
                _unregister(provider)


def test_e2e_callable_invalid_fallback(tmp_path, monkeypatch, caplog):
    """SPEC scenario 4c: invalid callable result -> regex fallback."""
    provider = None
    try:
        def invalid_spy(text):  # noqa: ARG001
            return {"facts": "nope", "decisions": []}

        provider = _make_provider(tmp_path, monkeypatch, spy=invalid_spy)
        caplog.set_level(logging.WARNING,
                         logger="memory_server.services.ingestion_service")
        _turn(provider)
        _run_async(provider._writer.flush(), timeout=5.0)
        _bounded_wait("regex fallback fact persisted",
                      lambda: _count_facts(provider, "Docker") >= 1,
                      provider=provider)
        assert _fact_row(provider).confidence == 0.5
        assert "invalid response" in caplog.text
    finally:
        if provider is not None:
            try:
                _e2e_teardown(provider, primary=sys.exc_info()[1])
            finally:
                _unregister(provider)


def test_e2e_b3b_passthrough_nondefaults(tmp_path, monkeypatch):
    """SPEC scenario 5 (subchecks 5a/5b/5c): nondefault timeout/max-input/
    confidence-gate observed at the real learn boundary through the seams.
    5c (timeout) is delegated to test_e2e_callable_timeout_fallback."""
    provider = None
    spy_calls: list[str] = []
    try:
        def spy(text: str) -> ExtractedResult:
            spy_calls.append(text)
            return _docker_result()

        provider = _make_provider(tmp_path, monkeypatch, spy=spy,
                                  extraction_cfg={"llm_timeout_seconds": 0.2,
                                                  "llm_max_input_chars": 40,
                                                  "llm_confidence_gate": 0.9})
        assert provider._extractor_runtime.llm_timeout_seconds == 0.2
        assert provider._extractor_runtime.llm_max_input_chars == 40
        assert provider._extractor_runtime.llm_confidence_gate == 0.9

        long_text = ("Docker is container and this sentence is deliberately "
                     "long enough to exceed forty characters")
        expected_extracted = HermesProvider._extract_turn_text(
            [{"role": "user", "content": long_text},
             {"role": "assistant", "content": ""}]
        )
        assert len(expected_extracted) > 40

        _turn(provider, long_text)
        _run_async(provider._writer.flush(), timeout=5.0)
        # 5a — max-input truncation: A2's tail truncation at the learn
        # boundary (ingestion_service.py:327-329); the default 8000 would NOT
        # truncate, proving the nondefault llm_max_input_chars reached learn.
        _bounded_wait("truncated call recorded",
                      lambda: len(spy_calls) == 1
                      and spy_calls[0] == expected_extracted[-40:],
                      provider=provider)
        assert spy_calls[0] == expected_extracted[-40:]
        # 5b — confidence gate: 0.85 < 0.9 -> NOT persisted (LLM mode gate,
        # ingestion_service.py:397-402). The control contrast (default gate
        # persists 0.85) is scenario 1; default max_input not truncating is
        # also scenario 1.
        assert _count_facts(provider, "Docker") == 0, (
            "0.85 fact persisted under llm_confidence_gate=0.9"
        )
        # no decisions from the spy -> nothing else persisted
        assert _count_receipts(provider) == 0
    finally:
        if provider is not None:
            try:
                _e2e_teardown(provider, primary=sys.exc_info()[1])
            finally:
                _unregister(provider)


def test_e2e_bounded_failure_diagnostics(tmp_path, monkeypatch, caplog):
    """SPEC scenario 6: bounded failure diagnostics with bounded retries."""
    provider = None
    try:
        import storage.repositories.fact_repo as fact_repo_mod

        attempts = {"n": 0}

        async def failing_create(self, fact):  # noqa: ARG001
            attempts["n"] += 1
            raise RuntimeError(f"learn boom {attempts['n']}")

        monkeypatch.setattr(fact_repo_mod.FactRepository, "create", failing_create)
        provider = _make_provider(tmp_path, monkeypatch, spy=None,
                                  extraction_cfg={"extraction_mode": "regex"})

        caplog.set_level(logging.ERROR,
                         logger="memory_server.plugins.hermes.writer")
        _turn_without_messages(provider, user_content=TEXT_DOCKER)
        provider.on_session_switch(new_session_id="next-session")
        _bounded_wait(
            "failure counted",
            lambda: provider._writer.total_failed >= 1 and provider._writer.total_requeued == 2,
            provider=provider,
        )
        w = provider._writer
        assert w.total_queued == 1
        assert w.total_flushed == 0
        assert w.total_failed == 1
        assert w.total_requeued == 2
        assert w.failed_items and w.failed_items[0]["turn_id"] == TURN_ID
        assert "learn boom 3" in w.failed_items[0]["error"]
        assert "failed turn e2e after 3 attempts" in caplog.text
        assert _count_facts(provider, "Docker") == 0
        assert _count_receipts(provider) == 0
        assert attempts["n"] == 3
    finally:
        if provider is not None:
            try:
                _e2e_teardown(provider, primary=sys.exc_info()[1])
            finally:
                _unregister(provider)


def test_e2e_teardown_contract(tmp_path, monkeypatch):
    """SPEC scenario 7a: teardown task/refs/engine-DISPOSED/no thread remains.

    Teardown is the observable-under-test, so it is invoked IN the body; the
    finally uses the SAME uniform nested try/finally as tests 1-6/7b (D2-W1)
    and re-runs teardown idempotently, so `_unregister` is guaranteed even if
    either teardown call raises its aggregate (D-F5).
    """
    provider = None
    try:
        def spy(text):  # noqa: ARG001
            return _docker_result()

        provider = _make_provider(tmp_path, monkeypatch, spy=spy)
        _turn(provider)
        _run_async(provider._writer.flush(), timeout=5.0)
        _bounded_wait("turn1 fact persisted",
                      lambda: _count_facts(provider, "Docker") >= 1,
                      provider=provider)

        # CAPTURE before teardown (D4 step-1 capture, None-guarded)
        writer = provider._writer
        writer_task = writer._task
        engine = provider._provider.engine
        pool_before = engine.pool
        assert provider._extractor_runtime is not None, "capture must be meaningful"
        assert provider._llm_extractor is spy
        assert provider._settings is not None
        assert provider._graph is None and provider._qdrant is None
        assert provider._lancedb is None and provider._embedder is None
        # D-F4 creation proof: the test-owned SQLite file EXISTS pre-disposal
        # (a persisted fact implies a real connection); teardown step 6 then
        # removes it and asserts ABSENCE (deletion invariant).
        db_url_7a = _db_url_for(provider)
        assert db_url_7a is not None, "registry entry missing before teardown"
        db_path_7a = Path(db_url_7a[len("sqlite+aiosqlite:///"):])
        assert db_path_7a.exists(), f"temp DB file not created: {db_path_7a}"

        # 7a's observable-under-test: run teardown IN the body so the
        # post-state assertions below observe the REAL cleaned resources
        # (refs / engine / writer task). The finally re-runs teardown
        # idempotently inside the uniform nested try/finally (D2-W1) — same
        # shape as 7b's outer-finally safety; idempotency verified (7b).
        _e2e_teardown(provider)

        # provider-owned references cleared (P2-F2)
        assert provider._provider is None
        assert provider._writer is None
        assert provider._extractor_runtime is None
        assert provider._llm_extractor is None
        assert provider._settings is None
        assert provider._graph is None
        assert provider._qdrant is None
        assert provider._lancedb is None
        assert provider._embedder is None
        assert provider._outbox_worker is None and provider._outbox_task is None
        assert provider._initialized is False
        assert provider._shut_down is True
        # captured writer task disposed (cancelled tasks are done)
        assert writer_task is None or writer_task.done()
        # captured engine DISPOSED: pool identity change + zero checked-out
        assert engine.pool is not pool_before
        assert engine.pool.checkedout() == 0
    finally:
        # Uniform nested finally (D2-W1/D-F5): `_e2e_teardown` inside the
        # nested try, `_unregister` in the inner finally — `_unregister` runs
        # even when EITHER teardown call (the in-body call or this idempotent
        # re-run) raises its aggregate; the registry entry is removed only
        # here, AFTER the cleanup attempt (D-F3).
        if provider is not None:
            try:
                _e2e_teardown(provider, primary=sys.exc_info()[1])
            finally:
                _unregister(provider)


def test_e2e_cleanup_continues_after_first_flush_raise(tmp_path, monkeypatch):
    """SPEC scenario 7b / fixture step 6: the first flush exception must not
    skip later cleanup; aggregate raises with the first failure not masked."""
    provider = None
    try:
        def spy(text):  # noqa: ARG001
            return _docker_result()

        # flush_interval=10.0: the background _run() task must NOT consume the
        # wrapper's first-call-raise before teardown's step-2 flush
        provider = _make_provider(tmp_path, monkeypatch, spy=spy,
                                  flush_interval=10.0)
        real_flush = provider._writer.flush
        flush_calls = {"n": 0}

        async def first_boom_flush():
            flush_calls["n"] += 1
            if flush_calls["n"] == 1:
                raise RuntimeError("injected first flush boom")
            return await real_flush()

        provider._writer.flush = first_boom_flush
        _turn(provider)  # one queued turn; drained by the REAL flush at call 2+

        step_log: list[str] = []
        with pytest.raises(AssertionError) as excinfo:
            _e2e_teardown(provider, step_log=step_log)
        assert "injected first flush boom" in str(excinfo.value), (
            "first failure must not be masked"
        )
        for label in ("writer.flush", "on_session_end", "shutdown",
                      "assert _provider cleared", "assert writer task disposed",
                      "assert writer attr cleared", "assert outbox handles None",
                      "assert engine disposed (pool identity + checkedout)",
                      "remove temp file", "assert temp file removed"):
            assert label in step_log, (label, step_log)
        assert flush_calls["n"] >= 2, "real flush must run after the boom"
        # D-F3: the deliberate aggregate teardown must NOT have consumed the
        # registry entry — its metadata (db_url) survives for the cleanup and
        # for the backstop, which still needs it to prove the temp path.
        assert _db_url_for(provider) is not None, (
            "aggregate teardown consumed the registry entry before cleanup"
        )
        # final state on the REAL resources is clean
        assert provider._provider is None
        assert provider._writer is None
        assert provider._shut_down is True
    finally:
        # idempotent re-run (outer finally safety) inside a nested
        # try/finally: _unregister runs regardless of the second teardown's
        # outcome (D-F5), and the removal is verified independently.
        if provider is not None:
            try:
                _e2e_teardown(provider)
            finally:
                _unregister(provider)
                assert _db_url_for(provider) is None, (
                    "registry entry survived _unregister"
                )
