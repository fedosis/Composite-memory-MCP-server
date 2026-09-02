"""B3b provider-wiring tests — init order, four-kwargs passthrough, fail-closed lifecycle.

Covers DETAIL (cmms-provider-wiring) sections 1-6: single-Settings identity
across both module aliases, resolver/factory exactly-once caching, the four
explicit llm_* kwargs on the writer path, busy_timeout wiring proofs, per-
initialization-step failure injection (FP0a-FP3c), POISONED/CLOSE_FAILED
lifecycle states, and single-owner engine disposal ([R4-F1]).

Canonical focused run (round-2, C-W1): the bare repo command
`pytest tests/test_provider_wiring.py -x -q` can resolve the STALE installed
`memory_server` package from site-packages and fail before importing this
file's subject. Always run the source-tree gate:
`PYTHONPATH=src pytest tests/test_provider_wiring.py -x -q`.
"""

import asyncio
import inspect
import logging
import threading
import time

import pytest
import sqlalchemy
import storage.outbox_worker as storage_outbox_mod
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncEngine
from storage.models.fact import FactORM
from storage.outbox_worker import OutboxWorker

import memory_server.plugins.hermes.config as config_mod
import memory_server.services.ingestion_service as svc_mod
from memory_server.plugins.hermes import provider as provider_mod
from memory_server.plugins.hermes.provider import HermesProvider
from memory_server.plugins.hermes.resolver import ExtractorRuntimeConfig
from memory_server.providers import sqlite_provider as sqlite_provider_mod
from memory_server.providers.embedding_provider import MockEmbeddingProvider
from memory_server.providers.lancedb_provider import LanceDBProvider
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.settings import get_settings as real_get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Test stub worker classes (test-owned; replaced into storage.outbox_worker)
# ---------------------------------------------------------------------------


class _StubOutboxWorker:
    """Minimal worker stub: construction + stop only; initialize/run patched per case."""

    def __init__(self, *args, **kwargs):
        pass

    def stop(self) -> None:
        pass

    async def initialize(self) -> None:
        pass

    async def run(self) -> None:
        pass


class _RaisingRunOutboxWorker:
    """Stub worker whose run() terminates IMMEDIATELY by raising."""

    def __init__(self, *args, **kwargs):
        self._stop_requested = False

    def stop(self) -> None:
        self._stop_requested = True

    async def initialize(self) -> None:
        pass

    async def run(self) -> None:
        raise RuntimeError("run-crashed")


class _UnstoppableOutboxWorker:
    """Stub worker: run() ignores stop() AND swallows CancelledError until allow_exit."""

    instances: list["_UnstoppableOutboxWorker"] = []

    def __init__(self, *args, **kwargs):
        self._stop_requested = False
        self._allow_exit = False
        _UnstoppableOutboxWorker.instances.append(self)

    def stop(self) -> None:
        self._stop_requested = True

    async def initialize(self) -> None:
        pass

    async def run(self) -> None:
        while True:
            if self._allow_exit:
                return
            try:
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                if self._allow_exit:
                    raise
                continue


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _init_outbox_capable_provider(tmp_path, monkeypatch) -> HermesProvider:
    """File db + lancedb mock; returns an INITIALIZED HermesProvider (outbox-capable)."""
    monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")
    import memory_server.providers.embedding_provider as embedding_module

    monkeypatch.setattr(
        embedding_module, "SentenceTransformerEmbeddingProvider", MockEmbeddingProvider
    )  # established pattern, tests/test_hermes_provider.py:344-352
    provider = HermesProvider()
    provider.initialize(
        session_id="test",
        config={
            "db_url": f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}",
            "path": str(tmp_path),
        },
        hermes_home=str(tmp_path),
    )
    return provider


def _init_inmemory_provider(tmp_path, monkeypatch) -> HermesProvider:
    """In-memory DB (`sqlite+aiosqlite://` — outbox skipped)."""
    monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")
    import memory_server.providers.embedding_provider as embedding_module

    monkeypatch.setattr(
        embedding_module, "SentenceTransformerEmbeddingProvider", MockEmbeddingProvider
    )
    provider = HermesProvider()
    provider.initialize(
        session_id="test",
        config={"db_url": "sqlite+aiosqlite://", "path": str(tmp_path)},
        hermes_home=str(tmp_path),
    )
    return provider


def _async_raise(msg: str):
    """Return an async function that raises RuntimeError(msg) — for async-site injection."""

    async def _raise(*args, **kwargs):
        raise RuntimeError(msg)

    return _raise


def _start_test_loop():
    """Start a dedicated daemon-thread event loop for the POISONED stub tests."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(
        target=loop.run_forever, daemon=True, name="test-owned-loop"
    )
    thread.start()
    return loop, thread


def _deterministic_extraction_env(monkeypatch):
    """Remove extraction env overrides so the REAL resolver returns its defaults."""
    for name in (
        "MEMORY_SERVER_EXTRACTION_MODE",
        "MEMORY_SERVER_LLM_MODEL",
        "MEMORY_SERVER_LLM_TIMEOUT_SECONDS",
        "MEMORY_SERVER_LLM_MAX_INPUT_CHARS",
        "MEMORY_SERVER_LLM_CONFIDENCE_GATE",
    ):
        monkeypatch.delenv(name, raising=False)


def _queue_turn(provider, text: str, turn_id: str = "t1") -> None:
    """Queue ONE turn (with extractable text) and flush it via the real writer path."""
    provider.sync_turn(
        user_content="",
        assistant_content="",
        session_id=turn_id,
        messages=[{"role": "user", "content": text}],
    )
    provider.on_session_switch(new_session_id="t2")


# ---------------------------------------------------------------------------
# Spy builders — DELEGATING wrappers recording into a caller-owned ordered log
# ---------------------------------------------------------------------------


def spy_get_settings(log):
    """Delegating wrapper for the provider-module get_settings alias."""
    orig = provider_mod.get_settings

    def wrapper():
        result = orig()
        log.append(("get_settings:provider", result))
        return result

    return wrapper


def spy_config_get_settings(log):
    """Delegating wrapper for the config-module get_settings alias."""
    orig = config_mod.get_settings

    def wrapper():
        result = orig()
        log.append(("get_settings:config", result))
        return result

    return wrapper


def spy_resolver(log):
    """Delegating wrapper for provider_mod.resolve_extractor_settings."""
    orig = provider_mod.resolve_extractor_settings

    def wrapper(cfg, settings):
        result = orig(cfg, settings)
        log.append(("resolver", (cfg, settings)))
        return result

    return wrapper


def spy_factory(log, factory_returns):
    """Delegating wrapper for provider_mod.build_llm_extractor_from_cfg."""
    orig = provider_mod.build_llm_extractor_from_cfg

    def wrapper(cfg, *, hermes_home):
        result = orig(cfg, hermes_home=hermes_home)
        log.append(("factory", (cfg, hermes_home)))
        factory_returns.append(result)
        return result

    return wrapper


def spy_create_engine(log):
    """Delegating wrapper for sqlite_provider.create_async_engine (records each call)."""
    orig = sqlite_provider_mod.create_async_engine

    def wrapper(*args, **kwargs):
        engine = orig(*args, **kwargs)
        log.append(("create_async_engine", engine))
        return engine

    return wrapper


def spy_engine_dispose(log):
    """Class-level delegating wrapper for AsyncEngine.dispose (records each dispose)."""
    orig = AsyncEngine.dispose

    async def wrapper(self):
        log.append(("dispose", self))
        return await orig(self)

    return wrapper


def spy_provider_close(log):
    """Delegating wrapper for SQLiteProvider.close (class-level)."""
    orig = SQLiteProvider.close

    async def wrapper(self):
        log.append(("provider_close", self))
        return await orig(self)

    return wrapper


def spy_worker_close(log):
    """Delegating wrapper for OutboxWorker.close (class-level)."""
    orig = OutboxWorker.close

    async def wrapper(self):
        log.append(("worker_close", self))
        return await orig(self)

    return wrapper


def spy_writer_flush(log):
    """Delegating wrapper for WriterQueue.flush (records the drained count)."""
    from memory_server.plugins.hermes.writer import WriterQueue

    orig = WriterQueue.flush

    async def wrapper(self):
        n = await orig(self)
        log.append(("writer_flush", n))
        return n

    return wrapper


# ---------------------------------------------------------------------------
# 1. Single-Settings identity + order across BOTH module aliases (SPEC item 2)
# ---------------------------------------------------------------------------


def test_single_settings_identity_and_order_across_both_aliases(tmp_path, monkeypatch):
    # Determinism setup: MEMORY_SERVER_DB_URL unset so the config-module
    # fallback (config.py:96-99) fires; PATH/MAX_FACTS unset; outbox-capable
    # lancedb mock. Config data has NO db_url.
    monkeypatch.delenv("MEMORY_SERVER_DB_URL", raising=False)
    monkeypatch.delenv("MEMORY_SERVER_PATH", raising=False)
    monkeypatch.delenv("MEMORY_SERVER_MAX_FACTS", raising=False)
    _deterministic_extraction_env(monkeypatch)
    monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")
    import memory_server.providers.embedding_provider as embedding_module

    monkeypatch.setattr(
        embedding_module, "SentenceTransformerEmbeddingProvider", MockEmbeddingProvider
    )

    log: list[tuple[str, object]] = []
    factory_returns: list[object] = []
    monkeypatch.setattr(provider_mod, "get_settings", spy_get_settings(log))
    monkeypatch.setattr(config_mod, "get_settings", spy_config_get_settings(log))
    monkeypatch.setattr(provider_mod, "resolve_extractor_settings", spy_resolver(log))
    monkeypatch.setattr(
        provider_mod, "build_llm_extractor_from_cfg", spy_factory(log, factory_returns)
    )

    provider = HermesProvider()
    try:
        provider.initialize(
            session_id="identity",
            config={"path": str(tmp_path)},  # NO db_url — config fallback fires
            hermes_home=str(tmp_path),
        )
    except Exception:
        provider.shutdown()
        raise

    # Flag rule: the config-module leg MUST have fired (non-vacuous proof).
    assert log and log[0][0] == "get_settings:config", (
        f"config-module get_settings leg did not fire (log={log})"
    )
    events = [name for name, _ in log]
    assert events[0:4] == [
        "get_settings:config",
        "get_settings:provider",
        "resolver",
        "factory",
    ], f"init order wrong: {events}"

    settings_instance = log[0][1]
    assert settings_instance is not None
    for name, payload in log:
        if name.startswith("get_settings"):
            assert payload is settings_instance, "a second distinct Settings object was returned"

    # Direct real call returns the SAME cached instance.
    assert real_get_settings() is settings_instance

    # Resolver: exactly once, cfg is provider._config, settings is S.
    resolver_entries = [p for name, p in log if name == "resolver"]
    assert len(resolver_entries) == 1
    resolver_cfg, resolver_settings = resolver_entries[0]
    assert resolver_settings is settings_instance
    assert resolver_cfg is provider._config

    # Factory: exactly once, cfg is the resolver's frozen return, hermes_home passed.
    factory_entries = [p for name, p in log if name == "factory"]
    assert len(factory_entries) == 1
    factory_cfg, factory_home = factory_entries[0]
    assert factory_cfg is provider._extractor_runtime
    assert factory_home == str(tmp_path)
    assert len(factory_returns) == 1
    assert provider._llm_extractor is factory_returns[0]

    # Invariant: provider non-None -> extractor_runtime non-None.
    assert provider._provider is not None
    assert provider._extractor_runtime is not None

    try:
        # Lifecycle extension in the SAME proof: repeated initialize, a REAL
        # writer flush, on_session_switch, on_session_end — counts stay 1.
        provider.initialize(
            session_id="identity2",
            config={"path": str(tmp_path)},
            hermes_home=str(tmp_path),
        )
        assert len([p for name, p in log if name == "resolver"]) == 1
        assert len([p for name, p in log if name == "factory"]) == 1

        _queue_turn(provider, "Alice is a tester", turn_id="t1")
        assert len([p for name, p in log if name == "resolver"]) == 1
        assert len([p for name, p in log if name == "factory"]) == 1

        provider.on_session_end(messages=[{"role": "user", "content": "Bob is a dev"}])
        assert len([p for name, p in log if name == "resolver"]) == 1
        assert len([p for name, p in log if name == "factory"]) == 1

        for name, payload in log:
            if name.startswith("get_settings"):
                assert payload is settings_instance
    finally:
        provider.shutdown()


# ---------------------------------------------------------------------------
# 2. MCP path unchanged — five args only, no extraction kwargs (SPEC item 3/4)
# ---------------------------------------------------------------------------


async def test_mcp_path_unchanged_five_args_only(monkeypatch):
    import memory_server.api.learn as learn_mod

    captured: dict = {}

    async def recorder(self, **kwargs):
        captured.update(kwargs)
        return {"facts": [], "decisions": [], "skills": [], "beliefs": [], "receipts": []}

    monkeypatch.setattr(svc_mod.MemoryIngestionService, "learn", recorder)

    class _StubProvider:
        _session_factory = None

    await learn_mod.learn(
        provider=_StubProvider(),  # type: ignore[arg-type]
        text="Alice is a tester",
        source="mcp",
        extract_beliefs=False,
        min_belief_confidence=0.6,
    )
    # Exactly the five MCP args — no extraction keys — service defaults apply.
    assert set(captured) == {"text", "source", "extract_beliefs", "min_belief_confidence"}
    assert "llm_extractor" not in captured
    assert captured["text"] == "Alice is a tester"
    assert captured["source"] == "mcp"


# ---------------------------------------------------------------------------
# 3. Writer path — four explicit kwargs, both legs (SPEC items 3/4, EXPECTED-EFFECTIVE MAP)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "case", ["default-regex", "env-llm", "yaml-llm", "cached-none"]
)
def test_writer_path_passes_four_explicit_kwargs(tmp_path, monkeypatch, case):
    monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")
    import memory_server.providers.embedding_provider as embedding_module

    monkeypatch.setattr(
        embedding_module, "SentenceTransformerEmbeddingProvider", MockEmbeddingProvider
    )

    def fake_extractor(text: str):
        return None

    config_data: dict = {
        "db_url": "sqlite+aiosqlite://",
        "path": str(tmp_path),
    }

    if case == "default-regex":
        _deterministic_extraction_env(monkeypatch)
    elif case == "env-llm":
        monkeypatch.setenv("MEMORY_SERVER_EXTRACTION_MODE", "llm")
        monkeypatch.setenv("MEMORY_SERVER_LLM_MODEL", "test")
        monkeypatch.setenv("MEMORY_SERVER_LLM_TIMEOUT_SECONDS", "0.3")
        monkeypatch.setenv("MEMORY_SERVER_LLM_MAX_INPUT_CHARS", "100")
        monkeypatch.setenv("MEMORY_SERVER_LLM_CONFIDENCE_GATE", "0.55")

        def fake_factory(cfg, *, hermes_home):
            return fake_extractor

        monkeypatch.setattr(provider_mod, "build_llm_extractor_from_cfg", fake_factory)
    elif case == "yaml-llm":
        _deterministic_extraction_env(monkeypatch)
        config_data.update(
            {
                "extraction_mode": "llm",
                "llm_model": "test",
                "llm_timeout_seconds": 0.3,
                "llm_max_input_chars": 100,
                "llm_confidence_gate": 0.55,
            }
        )

        def fake_factory_yaml(cfg, *, hermes_home):
            return fake_extractor

        monkeypatch.setattr(provider_mod, "build_llm_extractor_from_cfg", fake_factory_yaml)
    elif case == "cached-none":
        _deterministic_extraction_env(monkeypatch)

        def stub_resolver(cfg, settings):
            return ExtractorRuntimeConfig(
                extraction_mode="regex",
                llm_model=None,
                llm_timeout_seconds=None,
                llm_max_input_chars=None,
                llm_confidence_gate=None,
            )

        monkeypatch.setattr(provider_mod, "resolve_extractor_settings", stub_resolver)
        # factory stays REAL: regex mode -> None
    else:  # pragma: no cover
        raise AssertionError(f"unknown case {case}")

    # Leg 1 spy: provider -> api.learn (delegating).
    import memory_server.api.learn as learn_mod

    leg1_calls: list[dict] = []
    orig_learn = learn_mod.learn

    async def learn_recorder(**kwargs):
        leg1_calls.append(kwargs)
        return await orig_learn(**kwargs)

    monkeypatch.setattr(learn_mod, "learn", learn_recorder)

    # Leg 2 spy: api.learn -> MemoryIngestionService.learn (non-delegating).
    leg2_calls: list[dict] = []

    async def service_recorder(self, **kwargs):
        leg2_calls.append(kwargs)
        return {"facts": [], "decisions": [], "skills": [], "beliefs": [], "receipts": []}

    monkeypatch.setattr(svc_mod.MemoryIngestionService, "learn", service_recorder)

    provider = HermesProvider()
    try:
        provider.initialize(
            session_id="writer", config=config_data, hermes_home=str(tmp_path)
        )
        _queue_turn(provider, "Alice is a tester", turn_id="t1")

        # ---- Leg 1: the provider passes ALL FOUR explicit keys ----
        assert len(leg1_calls) == 1
        call = leg1_calls[0]
        assert "llm_extractor" in call
        assert "llm_timeout_seconds" in call
        assert "llm_max_input_chars" in call
        assert "llm_confidence_gate" in call
        assert call["llm_extractor"] is provider._llm_extractor
        assert call["llm_timeout_seconds"] == provider._extractor_runtime.llm_timeout_seconds
        assert call["llm_max_input_chars"] == provider._extractor_runtime.llm_max_input_chars
        assert call["llm_confidence_gate"] == provider._extractor_runtime.llm_confidence_gate

        # ---- Leg 2: effective service-boundary semantics per EXPECTED-EFFECTIVE MAP ----
        assert len(leg2_calls) == 1
        svc_kw = leg2_calls[0]

        if case == "default-regex":
            assert provider._extractor_runtime.extraction_mode == "regex"
            assert provider._extractor_runtime.llm_timeout_seconds == 15.0
            assert provider._extractor_runtime.llm_max_input_chars == 8000
            assert provider._extractor_runtime.llm_confidence_gate == 0.7
            # Leg 1 passed llm_extractor=None EXPLICITLY (a VALUE)...
            assert call["llm_extractor"] is None
            # ...and Leg 2 omitted ONLY the None extractor; timeout/max/gate forwarded.
            assert "llm_extractor" not in svc_kw
            assert svc_kw.get("llm_timeout_seconds", 15.0) == 15.0
            assert svc_kw.get("llm_max_input_chars", 8000) == 8000
            assert svc_kw.get("llm_confidence_gate", 0.7) == 0.7
        elif case in ("env-llm", "yaml-llm"):
            assert call["llm_extractor"] is fake_extractor
            assert call["llm_timeout_seconds"] == 0.3
            assert call["llm_max_input_chars"] == 100
            assert call["llm_confidence_gate"] == 0.55
            assert svc_kw["llm_extractor"] is fake_extractor
            assert svc_kw["llm_timeout_seconds"] == 0.3
            assert svc_kw["llm_max_input_chars"] == 100
            assert svc_kw["llm_confidence_gate"] == 0.55
        else:  # cached-none — resolver-boundary stub, NOT a real resolver default
            assert call["llm_extractor"] is None
            assert call["llm_timeout_seconds"] is None
            assert call["llm_max_input_chars"] is None
            assert call["llm_confidence_gate"] is None
            # Leg 2 omits ALL four None-valued kwargs -> A2 defaults apply.
            assert "llm_extractor" not in svc_kw
            assert "llm_timeout_seconds" not in svc_kw
            assert "llm_max_input_chars" not in svc_kw
            assert "llm_confidence_gate" not in svc_kw
            assert svc_kw.get("llm_extractor") is None
            assert svc_kw.get("llm_timeout_seconds", 15.0) == 15.0
            assert svc_kw.get("llm_max_input_chars", 8000) == 8000
            assert svc_kw.get("llm_confidence_gate", 0.7) == 0.7
    finally:
        provider.shutdown()


async def test_cached_none_effective_defaults_at_real_service_boundary(
    tmp_path, monkeypatch
):
    """C-W2: cached-None effective values OBSERVED at the real A2 boundary.

    The cached-none case in test_writer_path_passes_four_explicit_kwargs uses
    a NON-delegating service recorder, so its 15.0/8000/0.7 effective values
    come from the test's dict.get(...) fallback. This test DELEGATES to the
    REAL MemoryIngestionService.learn and reads the effective defaults from
    the real service signature (ingestion_service.py:239-242 +
    LLM_CONFIDENCE_GATE) — the actual values a cached-None resolution
    produces at the service boundary — while KEEPING the separate leg-1
    explicit-key assertion.
    """
    monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")
    import memory_server.providers.embedding_provider as embedding_module

    monkeypatch.setattr(
        embedding_module, "SentenceTransformerEmbeddingProvider", MockEmbeddingProvider
    )
    _deterministic_extraction_env(monkeypatch)

    def stub_resolver(cfg, settings):
        return ExtractorRuntimeConfig(
            extraction_mode="regex",
            llm_model=None,
            llm_timeout_seconds=None,
            llm_max_input_chars=None,
            llm_confidence_gate=None,
        )

    monkeypatch.setattr(provider_mod, "resolve_extractor_settings", stub_resolver)
    # factory stays REAL: regex mode -> None

    # Leg 1 spy: provider -> api.learn (delegating).
    import memory_server.api.learn as learn_mod

    leg1_calls: list[dict] = []
    orig_learn = learn_mod.learn

    async def learn_recorder(**kwargs):
        leg1_calls.append(kwargs)
        return await orig_learn(**kwargs)

    monkeypatch.setattr(learn_mod, "learn", learn_recorder)

    # Leg 2 spy: api.learn -> REAL MemoryIngestionService.learn (delegating).
    leg2_calls: list[dict] = []
    orig_svc_learn = svc_mod.MemoryIngestionService.learn

    async def service_recorder(self, **kwargs):
        leg2_calls.append(kwargs)
        return await orig_svc_learn(self, **kwargs)

    monkeypatch.setattr(svc_mod.MemoryIngestionService, "learn", service_recorder)

    provider = None
    try:
        provider = _init_inmemory_provider(tmp_path, monkeypatch)
        assert provider._llm_extractor is None
        assert provider._extractor_runtime.llm_timeout_seconds is None
        assert provider._extractor_runtime.llm_max_input_chars is None
        assert provider._extractor_runtime.llm_confidence_gate is None

        _queue_turn(provider, "Alice is a tester", turn_id="t1")

        # Leg 1 (kept, not weakened): ALL FOUR explicit keys, explicit None.
        assert len(leg1_calls) == 1
        call = leg1_calls[0]
        assert call["llm_extractor"] is None
        assert call["llm_timeout_seconds"] is None
        assert call["llm_max_input_chars"] is None
        assert call["llm_confidence_gate"] is None

        # Leg 2: the REAL service received NO extraction kwargs (omission rule).
        assert len(leg2_calls) == 1
        svc_kw = leg2_calls[0]
        assert "llm_extractor" not in svc_kw
        assert "llm_timeout_seconds" not in svc_kw
        assert "llm_max_input_chars" not in svc_kw
        assert "llm_confidence_gate" not in svc_kw

        # Effective values ARE the real A2 defaults — read from the REAL
        # service signature, not from test-supplied fallbacks.
        sig = inspect.signature(orig_svc_learn)
        assert sig.parameters["llm_extractor"].default is None
        assert sig.parameters["llm_timeout_seconds"].default == 15.0
        assert sig.parameters["llm_max_input_chars"].default == 8000
        assert (
            sig.parameters["llm_confidence_gate"].default
            == svc_mod.LLM_CONFIDENCE_GATE
        )
        assert svc_mod.LLM_CONFIDENCE_GATE == 0.7

        # The REAL service consumed the cached-None resolution end-to-end:
        # regex facts stored (no extractor, no provider substitution).
        async with provider._provider._session_factory() as session:
            row = await session.execute(
                sqlalchemy.text("SELECT count(*) FROM facts WHERE subject = 'Alice'")
            )
            assert row.scalar() >= 1
    finally:
        if provider is not None:
            provider.shutdown()


# ---------------------------------------------------------------------------
# 4. None callable reaches the regex path — zero callable invocations (SPEC item 4)
# ---------------------------------------------------------------------------


def test_none_callable_reaches_regex_path_zero_invocations(tmp_path, monkeypatch):
    _deterministic_extraction_env(monkeypatch)

    kwargs_calls: list[dict] = []
    results: list[dict] = []
    orig_learn = svc_mod.MemoryIngestionService.learn

    async def recorder(self, **kwargs):
        result = await orig_learn(self, **kwargs)
        kwargs_calls.append(kwargs)
        results.append(result)
        return result

    monkeypatch.setattr(svc_mod.MemoryIngestionService, "learn", recorder)

    provider = None
    try:
        provider = _init_inmemory_provider(tmp_path, monkeypatch)
        # REAL factory in regex mode -> cached None (a VALUE, never substituted).
        assert provider._llm_extractor is None

        _queue_turn(provider, "Alice is a tester", turn_id="t1")

        assert len(kwargs_calls) == 1
        assert "llm_extractor" not in kwargs_calls[0]
        assert results and results[0]["facts"], "regex path must produce stored facts"
    finally:
        if provider is not None:
            provider.shutdown()


# ---------------------------------------------------------------------------
# 5. Resolved llm_timeout_seconds reaches A2's wait_for end-to-end (env/yaml)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("case", ["env", "yaml"])
async def test_llm_timeout_reaches_service_wait_for(tmp_path, monkeypatch, caplog, case):
    monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")
    import memory_server.providers.embedding_provider as embedding_module

    monkeypatch.setattr(
        embedding_module, "SentenceTransformerEmbeddingProvider", MockEmbeddingProvider
    )

    slow_calls: list[str] = []

    def slow_extractor(text: str):
        slow_calls.append(text)
        time.sleep(5)  # far beyond the resolved 0.3s timeout
        return None

    def fake_factory(cfg, *, hermes_home):
        return slow_extractor

    monkeypatch.setattr(provider_mod, "build_llm_extractor_from_cfg", fake_factory)

    config_data: dict = {
        "db_url": f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}",
        "path": str(tmp_path),
    }
    if case == "env":
        monkeypatch.setenv("MEMORY_SERVER_EXTRACTION_MODE", "llm")
        monkeypatch.setenv("MEMORY_SERVER_LLM_MODEL", "test")
        monkeypatch.setenv("MEMORY_SERVER_LLM_TIMEOUT_SECONDS", "0.3")
        monkeypatch.setenv("MEMORY_SERVER_LLM_MAX_INPUT_CHARS", "100")
    else:
        _deterministic_extraction_env(monkeypatch)
        config_data.update(
            {
                "extraction_mode": "llm",
                "llm_model": "test",
                "llm_timeout_seconds": 0.3,
                "llm_max_input_chars": 100,
            }
        )

    provider = None
    try:
        provider = HermesProvider()
        provider.initialize(
            session_id="timeout", config=config_data, hermes_home=str(tmp_path)
        )
        assert provider._llm_extractor is slow_extractor

        caplog.set_level(logging.WARNING)
        start = time.monotonic()
        _queue_turn(provider, "Alice is a tester", turn_id="t1")
        elapsed = time.monotonic() - start

        # A2's asyncio.wait_for(asyncio.to_thread(...), timeout=0.3) fired:
        assert elapsed < 2.0, f"resolved timeout not applied end-to-end: {elapsed:.2f}s"
        assert len(slow_calls) == 1
        assert len(slow_calls[0]) <= 100, "callable input not tail-truncated to 100 chars"
        assert "timed out" in caplog.text.lower()

        # Regex fallback result returned: facts actually stored.
        async with provider._provider._session_factory() as session:
            row = await session.execute(
                sqlalchemy.text("SELECT count(*) FROM facts WHERE subject = 'Alice'")
            )
            assert row.scalar() >= 1
    finally:
        if provider is not None:
            provider.shutdown()


# ---------------------------------------------------------------------------
# 6/7/8. busy_timeout proofs (SPEC items 5/6)
# ---------------------------------------------------------------------------


def test_sqlite_constructor_spy_busy_timeout_before_initialize(tmp_path, monkeypatch):
    log: list[tuple[str, object]] = []
    orig_init = SQLiteProvider.__init__

    def rec_init(self, *args, **kwargs):
        log.append(("__init__", (kwargs.get("url"), kwargs.get("busy_timeout_ms"))))
        return orig_init(self, *args, **kwargs)

    monkeypatch.setattr(SQLiteProvider, "__init__", rec_init)
    monkeypatch.setattr(sqlite_provider_mod, "create_async_engine", spy_create_engine(log))

    provider = None
    try:
        provider = _init_outbox_capable_provider(tmp_path, monkeypatch)

        ctor_entries = [p for name, p in log if name == "__init__"]
        assert ctor_entries, "SQLiteProvider construction not observed"
        url, busy_timeout_ms = ctor_entries[0]
        assert busy_timeout_ms == 60000
        assert url == f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}"

        create_entries = [p for name, p in log if name == "create_async_engine"]
        assert len(create_entries) == 1, "engine rebuilt or duplicated"

        ctor_idx = next(i for i, e in enumerate(log) if e[0] == "__init__")
        create_idx = next(
            i for i, e in enumerate(log) if e[0] == "create_async_engine"
        )
        assert ctor_idx < create_idx, "busy_timeout_ms must be passed BEFORE initialize"
    finally:
        if provider is not None:
            provider.shutdown()


async def test_busy_timeout_pragma_on_writer_path_connection(tmp_path, monkeypatch):
    provider = None
    try:
        # In-memory (StaticPool) provider: the outbox worker is skipped, so the
        # pool deterministically returns the init connection (the one PRAGMA'd
        # at sqlite_provider.py:104) on every checkout — no race with the
        # outbox worker's polling sessions. The factory IS the one learn() uses.
        provider = _init_inmemory_provider(tmp_path, monkeypatch)
        p = provider._require_provider()
        # Fresh checkout from the SAME factory MemoryIngestionService uses
        # (api/learn.py constructs the service with provider._session_factory).
        async with p._session_factory() as session:
            conn = await session.connection()
            row = (await conn.exec_driver_sql("PRAGMA busy_timeout")).first()
            assert row[0] == 60000
    finally:
        if provider is not None:
            provider.shutdown()


async def test_two_connection_lock_conflict_bounded(tmp_path, monkeypatch):
    provider = None
    engine = None
    try:
        provider = _init_outbox_capable_provider(tmp_path, monkeypatch)
        p = provider._require_provider()
        engine = p.engine
        session_a = None
        session_b = None
        try:
            # A: hold a REAL write lock on the writer-path pool
            session_a = p._session_factory()
            conn_a = await session_a.connection()
            await conn_a.exec_driver_sql("BEGIN IMMEDIATE")      # acquires the write lock
            await conn_a.execute(
                insert(FactORM).values(id="lock-a", subject="s", predicate="p", object="o")
            )
            # HOLD — no commit (WAL: writer holds the write lock from first write until commit)

            # B: SECOND, freshly checked-out session from the SAME factory (the pool
            # creates a new connection; its default busy_timeout is the dialect 5000ms —
            # §4.1). TEST-SUPPORT step: apply the configured timeout to B (same statement
            # as sqlite_provider.py:104) so the 60s busy wait is exercised.
            session_b = p._session_factory()
            conn_b = await session_b.connection()
            await conn_b.exec_driver_sql("PRAGMA busy_timeout=60000")
            assert (await conn_b.exec_driver_sql("PRAGMA busy_timeout")).first()[0] == 60000

            async def conflicting_write():
                await conn_b.exec_driver_sql("BEGIN IMMEDIATE")  # blocks ~60s on A's lock
                await conn_b.execute(
                    insert(FactORM).values(id="lock-b", subject="s", predicate="p", object="o")
                )
                await session_b.commit()

            start = time.monotonic()
            try:
                await asyncio.wait_for(conflicting_write(), timeout=63.0)  # outer bound: >60, <=65
                pytest.fail("conflicting write unexpectedly completed while A held the lock")
            except sqlalchemy.exc.OperationalError as exc:
                elapsed = time.monotonic() - start
                assert "locked" in str(exc).lower() or "busy" in str(exc).lower()
                assert elapsed >= 55.0, "busy handler did not actually wait ~60s (dialect default?)"
                assert elapsed < 63.0, "wait exceeded the outer bound"
        finally:
            # BOTH sessions/transactions closed in finally, even on timeout/SQLite
            # error; the None-guards make this safe when an acquisition step failed.
            if session_a is not None:
                await session_a.rollback()
                await session_a.close()
            if session_b is not None:
                await session_b.rollback()
                await session_b.close()
    finally:
        if provider is not None:
            provider.shutdown()                                # outer lifecycle — mandatory
        # Pool fully returned AND released: checked AFTER the full teardown
        # (outbox stopped -> writer flush -> engine disposed), so the outbox
        # worker's own polling session can no longer hold a connection.
        if engine is not None:
            assert engine.pool.checkedout() == 0


# ---------------------------------------------------------------------------
# 9. Failure injection after EACH initialization step (SPEC item 7, §3.6)
# ---------------------------------------------------------------------------

FP_PARAMS = [
    "config-construction",
    "settings-lookup",
    "db-url-resolution",
    "resolver",
    "factory",
    "provider-construction",
    "provider-initialize",
    "writer-start",
    "outbox-auxiliary",
    "outbox-initialize",
    "outbox-scheduling",
]

FP_MESSAGE = {
    "config-construction": "fp-config",
    "settings-lookup": "fp-settings",
    "db-url-resolution": "fp-dburl",
    "resolver": "fp-resolver",
    "factory": "fp-factory",
    "provider-construction": "fp-provider-ctor",
    "provider-initialize": "fp-provider-init",
    "writer-start": "fp-writer",
    "outbox-auxiliary": "fp-aux",
    "outbox-initialize": "fp-outbox-init",
    "outbox-scheduling": "fp-outbox-schedule",
}

# Post-rollback expectations. NOTE: the §3.6 table describes state AT the
# failure boundary; the rollback teardown (step 5) clears the cached
# extraction state (_settings/_extractor_runtime/_llm_extractor) for EVERY
# case, so `_settings` is always None after initialize() raises. `_config`
# is never cleared by the teardown — it is set in every case except
# config-construction (where it was never assigned).
FP_EXPECTED = {
    "config-construction": {"config": False, "dispose": 0, "provider_close": 0},
    "settings-lookup": {"config": True, "dispose": 0, "provider_close": 0},
    "db-url-resolution": {"config": True, "dispose": 0, "provider_close": 0},
    "resolver": {"config": True, "dispose": 0, "provider_close": 0},
    "factory": {"config": True, "dispose": 0, "provider_close": 0},
    "provider-construction": {"config": True, "dispose": 0, "provider_close": 0},
    "provider-initialize": {"config": True, "dispose": 1, "provider_close": 1},
    "writer-start": {"config": True, "dispose": 1, "provider_close": 1},
    "outbox-auxiliary": {"config": True, "dispose": 1, "provider_close": 1},
    "outbox-initialize": {"config": True, "dispose": 1, "provider_close": 1},
    "outbox-scheduling": {"config": True, "dispose": 1, "provider_close": 1},
}


@pytest.mark.parametrize("fpoint", FP_PARAMS)
def test_fp_init_step_failure_rolls_back(tmp_path, monkeypatch, fpoint):
    log: list[tuple[str, object]] = []
    monkeypatch.setattr(OutboxWorker, "close", spy_worker_close(log))
    monkeypatch.setattr(SQLiteProvider, "close", spy_provider_close(log))
    monkeypatch.setattr(AsyncEngine, "dispose", spy_engine_dispose(log))

    # Outbox-capable setup so FP3 cases reach the outbox boundary.
    monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")
    import memory_server.providers.embedding_provider as embedding_module

    monkeypatch.setattr(
        embedding_module, "SentenceTransformerEmbeddingProvider", MockEmbeddingProvider
    )

    if fpoint == "config-construction":
        def _raise_cfg(*args, **kwargs):
            raise RuntimeError("fp-config")

        monkeypatch.setattr(
            provider_mod.HermesPluginConfig, "from_dict", classmethod(_raise_cfg)
        )
    elif fpoint == "settings-lookup":
        def _raise_settings():
            raise RuntimeError("fp-settings")

        monkeypatch.setattr(provider_mod, "get_settings", _raise_settings)
    elif fpoint == "db-url-resolution":
        def _raise_dburl(self, hermes_home):
            raise RuntimeError("fp-dburl")

        monkeypatch.setattr(
            provider_mod.HermesPluginConfig, "resolve_db_url", _raise_dburl
        )
    elif fpoint == "resolver":
        def _raise_resolver(cfg, settings):
            raise RuntimeError("fp-resolver")

        monkeypatch.setattr(provider_mod, "resolve_extractor_settings", _raise_resolver)
    elif fpoint == "factory":
        def _raise_factory(cfg, *, hermes_home):
            raise RuntimeError("fp-factory")

        monkeypatch.setattr(provider_mod, "build_llm_extractor_from_cfg", _raise_factory)
    elif fpoint == "provider-construction":
        def _raise_ctor(self, *args, **kwargs):
            raise RuntimeError("fp-provider-ctor")

        monkeypatch.setattr(sqlite_provider_mod.SQLiteProvider, "__init__", _raise_ctor)
    elif fpoint == "provider-initialize":
        async def _raise_provider_init(self):
            # Engine already created (sqlite_provider.py:98) before the raise.
            self._engine = sqlite_provider_mod.create_async_engine(self._url, echo=False)
            raise RuntimeError("fp-provider-init")

        monkeypatch.setattr(
            sqlite_provider_mod.SQLiteProvider, "initialize", _raise_provider_init
        )
    elif fpoint == "writer-start":
        monkeypatch.setattr(provider_mod.WriterQueue, "start", _async_raise("fp-writer"))
    elif fpoint == "outbox-auxiliary":
        monkeypatch.setattr(provider_mod, "_get_embedder", _async_raise("fp-aux"))
    elif fpoint == "outbox-initialize":
        monkeypatch.setattr(storage_outbox_mod, "OutboxWorker", _StubOutboxWorker)
        monkeypatch.setattr(_StubOutboxWorker, "initialize", _async_raise("fp-outbox-init"))
    elif fpoint == "outbox-scheduling":
        monkeypatch.setattr(storage_outbox_mod, "OutboxWorker", _StubOutboxWorker)

        # B3b impl detail: the run task is scheduled via HermesProvider's
        # dedicated seam (_schedule_outbox_task -> call_soon_threadsafe +
        # ensure_future on the shared loop; the actual asyncio.Task is what the
        # fail-closed stop verifies). Inject exactly at that boundary.
        def _raise_schedule(self):
            raise RuntimeError("fp-outbox-schedule")

        monkeypatch.setattr(HermesProvider, "_schedule_outbox_task", _raise_schedule)
    else:  # pragma: no cover
        raise AssertionError(f"unknown fpoint {fpoint}")

    provider = HermesProvider()
    with pytest.raises(RuntimeError, match=FP_MESSAGE[fpoint]):
        provider.initialize(
            session_id="fp",
            config={
                "db_url": f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}",
                "path": str(tmp_path),
            },
            hermes_home=str(tmp_path),
        )

    exp = FP_EXPECTED[fpoint]
    assert provider._initialized is False
    assert provider._extractor_runtime is None
    assert provider._llm_extractor is None
    assert provider._settings is None  # cached extraction state cleared by rollback
    assert (provider._config is not None) is exp["config"]
    assert len([e for e in log if e[0] == "dispose"]) == exp["dispose"]
    assert len([e for e in log if e[0] == "provider_close"]) == exp["provider_close"]
    assert len([e for e in log if e[0] == "worker_close"]) == 0

    # Subsequent shutdown completes as a no-op (no close-failure marker on the
    # normal path).
    provider.shutdown()
    assert provider._cleanup_failed is False


# ---------------------------------------------------------------------------
# 10. Run future that terminates by RAISING is verified termination (B-F3)
# ---------------------------------------------------------------------------


def test_stop_outbox_worker_run_future_raises_verified_termination(tmp_path, monkeypatch):
    log: list[tuple[str, object]] = []
    monkeypatch.setattr(OutboxWorker, "close", spy_worker_close(log))
    monkeypatch.setattr(SQLiteProvider, "close", spy_provider_close(log))
    monkeypatch.setattr(AsyncEngine, "dispose", spy_engine_dispose(log))
    monkeypatch.setattr(storage_outbox_mod, "OutboxWorker", _RaisingRunOutboxWorker)

    provider = None
    try:
        provider = _init_outbox_capable_provider(tmp_path, monkeypatch)

        # Oracle: the run task terminated by RAISING (distinct from timeout).
        # Read the stored exception ON the shared loop (thread-safe), awaiting
        # the task's real completion; the exception is consumed, never
        # re-raised by teardown.
        async def _get_run_exception():
            try:
                await asyncio.shield(provider._outbox_task)
            except RuntimeError:
                pass
            return provider._outbox_task.exception()

        exc = provider_mod._run_async(_get_run_exception(), timeout=10.0)
        assert isinstance(exc, RuntimeError)
        assert str(exc) == "run-crashed"

        # shutdown() returns NORMALLY — the run-future exception is consumed by
        # the stop routine (§3.3), never re-raised, never POISONED.
        provider.shutdown()

        assert provider._outbox_task is None
        assert provider._outbox_worker is None
        assert provider._outbox_stop_failed is False
        assert provider._cleanup_failed is False
        assert len([e for e in log if e[0] == "worker_close"]) == 0
        assert len([e for e in log if e[0] == "provider_close"]) == 1
        assert len([e for e in log if e[0] == "dispose"]) == 1
        assert provider._initialized is False
        assert provider._shut_down is True
    finally:
        if provider is not None:
            provider.shutdown()


# ---------------------------------------------------------------------------
# 11/12. POISONED state machine — test-owned loop (T5/T7/T8/T9/T11/T12)
# ---------------------------------------------------------------------------


def _poisoned_setup(tmp_path, monkeypatch):
    """Test-owned loop + unstoppable worker stub; returns (loop, provider, worker)."""
    _UnstoppableOutboxWorker.instances.clear()
    loop, _thread = _start_test_loop()
    monkeypatch.setattr(provider_mod, "_get_loop", lambda: loop)
    monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")
    import memory_server.providers.embedding_provider as embedding_module

    monkeypatch.setattr(
        embedding_module, "SentenceTransformerEmbeddingProvider", MockEmbeddingProvider
    )
    monkeypatch.setattr(storage_outbox_mod, "OutboxWorker", _UnstoppableOutboxWorker)

    provider = HermesProvider()
    provider.initialize(
        session_id="poison",
        config={
            "db_url": f"sqlite+aiosqlite:///{tmp_path / 'memory.db'}",
            "path": str(tmp_path),
        },
        hermes_home=str(tmp_path),
    )
    worker = _UnstoppableOutboxWorker.instances[-1]
    return loop, provider, worker


def _poisoned_teardown(loop, provider, worker) -> None:
    if worker is not None:
        worker._allow_exit = True
    try:
        provider.shutdown()
    except Exception:
        pass
    loop.call_soon_threadsafe(loop.stop)


def test_poisoned_unstoppable_outbox_retry(tmp_path, monkeypatch):
    log: list[tuple[str, object]] = []
    monkeypatch.setattr(OutboxWorker, "close", spy_worker_close(log))
    monkeypatch.setattr(SQLiteProvider, "close", spy_provider_close(log))
    monkeypatch.setattr(AsyncEngine, "dispose", spy_engine_dispose(log))

    loop, provider, worker = _poisoned_setup(tmp_path, monkeypatch)
    try:
        # shutdown() -> POISONED at ~15s (10s + 5s) -> contract-breach RuntimeError.
        with pytest.raises(RuntimeError, match="outbox worker failed to stop"):
            provider.shutdown()

        # POISONED entry: EVERY resource retained; close-spy log ZERO.
        assert provider._outbox_stop_failed is True
        assert provider._outbox_task is not None
        assert provider._outbox_worker is worker
        assert provider._initialized is False
        assert provider._shut_down is False
        assert provider._provider is not None
        assert provider._writer is not None
        assert provider._lancedb is not None
        assert len([e for e in log if e[0] == "dispose"]) == 0
        assert len([e for e in log if e[0] == "provider_close"]) == 0
        assert len([e for e in log if e[0] == "worker_close"]) == 0

        # T9: repeated shutdown while unstoppable raises again; no resource touched.
        with pytest.raises(RuntimeError, match="outbox worker failed to stop"):
            provider.shutdown()
        assert len([e for e in log if e[0] == "dispose"]) == 0
        assert len([e for e in log if e[0] == "provider_close"]) == 0
        assert len([e for e in log if e[0] == "worker_close"]) == 0

        # T8: allow the run loop to exit -> retry completes the reverse cleanup.
        worker._allow_exit = True
        provider.shutdown()
        assert provider._outbox_stop_failed is False
        assert provider._outbox_task is None
        assert provider._outbox_worker is None
        assert provider._lancedb is None
        assert provider._writer is None
        assert provider._provider is None
        assert provider._initialized is False
        assert provider._shut_down is True
        assert len([e for e in log if e[0] == "provider_close"]) == 1
        assert len([e for e in log if e[0] == "dispose"]) == 1
        assert len([e for e in log if e[0] == "worker_close"]) == 0

        # Third shutdown() is a no-op (T13).
        provider.shutdown()
        assert len([e for e in log if e[0] == "provider_close"]) == 1
        assert len([e for e in log if e[0] == "dispose"]) == 1
    finally:
        _poisoned_teardown(loop, provider, worker)


def test_poisoned_guards_initialize_is_available_require_provider(
    tmp_path, monkeypatch, caplog
):
    loop, provider, worker = _poisoned_setup(tmp_path, monkeypatch)
    try:
        with pytest.raises(RuntimeError, match="outbox worker failed to stop"):
            provider.shutdown()

        # T11: re-initialize while POISONED raises.
        with pytest.raises(RuntimeError, match="cannot re-initialize"):
            provider.initialize(
                session_id="x", config={"path": str(tmp_path)}, hermes_home=str(tmp_path)
            )

        # T12: is_available() False; _require_provider() raises.
        assert provider.is_available() is False
        with pytest.raises(RuntimeError, match="poisoned"):
            provider._require_provider()

        # T12 via the retained writer: flush -> _handle_batch_write ->
        # _require_provider -> poison raise -> batch dropped, no write.
        caplog.set_level(logging.WARNING)
        _queue_turn(provider, "Alice is a tester", turn_id="t1")
        assert provider._writer.total_failed >= 1
        assert "flush failed" in caplog.text.lower()
    finally:
        _poisoned_teardown(loop, provider, worker)


# ---------------------------------------------------------------------------
# 13. T6 guard unit (outer bound expired with a live task)
# ---------------------------------------------------------------------------


def test_mark_poisoned_t6_unit():
    provider = HermesProvider()
    provider._outbox_task = object()  # type: ignore[assignment]
    assert provider._mark_poisoned_if_live_task() is True
    assert provider._outbox_stop_failed is True
    assert provider._initialized is False
    assert provider._shut_down is False

    # No task -> returns False, marker untouched.
    provider._outbox_task = None
    provider._outbox_worker = None
    provider._outbox_stop_failed = False
    assert provider._mark_poisoned_if_live_task() is False
    assert provider._outbox_stop_failed is False


# ---------------------------------------------------------------------------
# 14. Shutdown reverse order — final flush BEFORE engine dispose ([R4-F1])
# ---------------------------------------------------------------------------


def test_shutdown_reverse_order_flush_before_dispose(tmp_path, monkeypatch):
    log: list[tuple[str, object]] = []
    monkeypatch.setattr(OutboxWorker, "close", spy_worker_close(log))
    monkeypatch.setattr(SQLiteProvider, "close", spy_provider_close(log))
    monkeypatch.setattr(AsyncEngine, "dispose", spy_engine_dispose(log))
    monkeypatch.setattr(provider_mod.WriterQueue, "flush", spy_writer_flush(log))

    provider = None
    try:
        provider = _init_outbox_capable_provider(tmp_path, monkeypatch)
        engine_before = provider._provider.engine

        _queue_turn(provider, "Alice is a tester", turn_id="t1")
        provider.shutdown()
    finally:
        if provider is not None:
            provider.shutdown()

    # [R4-F1] worker.close() NEVER invoked.
    assert len([e for e in log if e[0] == "worker_close"]) == 0
    # provider.close() exactly once.
    assert len([e for e in log if e[0] == "provider_close"]) == 1
    # Engine disposed exactly once, on the SAME engine the writer path used.
    disposes = [p for name, p in log if name == "dispose"]
    assert len(disposes) == 1
    assert disposes[0] is engine_before
    # Ordered log: the FINAL writer flush precedes the dispose AND drained (n>=1).
    assert any(n >= 1 for name, n in log if name == "writer_flush")
    last_flush_idx = max(i for i, e in enumerate(log) if e[0] == "writer_flush")
    dispose_idx = next(i for i, e in enumerate(log) if e[0] == "dispose")
    assert last_flush_idx < dispose_idx

    # All resource attrs released.
    assert provider._provider is None
    assert provider._writer is None
    assert provider._outbox_worker is None
    assert provider._outbox_task is None
    assert provider._qdrant is None
    assert provider._lancedb is None
    assert provider._embedder is None
    assert provider._graph is None
    assert provider._extractor_runtime is None
    assert provider._llm_extractor is None
    assert provider._settings is None
    assert provider._initialized is False
    assert provider._shut_down is True


# ---------------------------------------------------------------------------
# 15/16. CLOSE_FAILED: failed closes retain handles and block CLEAN (T15/T16)
# ---------------------------------------------------------------------------


def test_cleanup_provider_close_failure_retains_handle_and_blocks_clean(
    tmp_path, monkeypatch
):
    log: list[tuple[str, object]] = []
    real_close = SQLiteProvider.close
    close_calls = {"n": 0}

    async def flaky_close(self):
        close_calls["n"] += 1
        if close_calls["n"] == 1:
            raise RuntimeError("close-fp")
        return await real_close(self)

    monkeypatch.setattr(SQLiteProvider, "close", flaky_close)
    monkeypatch.setattr(AsyncEngine, "dispose", spy_engine_dispose(log))

    provider = None
    try:
        provider = _init_inmemory_provider(tmp_path, monkeypatch)

        # T15: close fails -> handle RETAINED, cleanup NOT reported CLEAN.
        with pytest.raises(
            RuntimeError, match=r"cleanup failed; resources retained \(close-failed\)"
        ):
            provider.shutdown()
        assert provider._cleanup_failed is True
        assert provider._provider is not None
        assert provider._shut_down is False
        assert len([e for e in log if e[0] == "dispose"]) == 0  # failing call never disposed

        # T16: retry -> the RETAINED provider close succeeds -> CLEAN.
        provider.shutdown()
        assert provider._provider is None
        assert provider._cleanup_failed is False
        assert provider._shut_down is True
        assert len([e for e in log if e[0] == "dispose"]) == 1

        # Third shutdown no-op.
        provider.shutdown()
        assert len([e for e in log if e[0] == "dispose"]) == 1
    finally:
        if provider is not None:
            provider.shutdown()


def test_cleanup_auxiliary_close_failure_retains_handle_and_blocks_clean(
    tmp_path, monkeypatch
):
    log: list[tuple[str, object]] = []
    real_lancedb_close = LanceDBProvider.close
    close_calls = {"n": 0}

    async def flaky_lancedb_close(self):
        close_calls["n"] += 1
        if close_calls["n"] == 1:
            raise RuntimeError("lancedb-close-fp")
        return await real_lancedb_close(self)

    monkeypatch.setattr(LanceDBProvider, "close", flaky_lancedb_close)
    monkeypatch.setattr(SQLiteProvider, "close", spy_provider_close(log))
    monkeypatch.setattr(AsyncEngine, "dispose", spy_engine_dispose(log))

    provider = None
    try:
        provider = _init_outbox_capable_provider(tmp_path, monkeypatch)

        # T15: lancedb close fails -> handle RETAINED, BUT cleanup CONTINUES:
        # writer shutdown + provider_close exactly once + engine disposed once.
        with pytest.raises(
            RuntimeError, match=r"cleanup failed; resources retained \(close-failed\)"
        ):
            provider.shutdown()
        assert provider._cleanup_failed is True
        assert provider._lancedb is not None
        assert provider._shut_down is False
        assert len([e for e in log if e[0] == "provider_close"]) == 1
        assert len([e for e in log if e[0] == "dispose"]) == 1

        # T16: retry -> retained lancedb close succeeds -> CLEAN; no re-dispose.
        provider.shutdown()
        assert provider._lancedb is None
        assert provider._cleanup_failed is False
        assert provider._shut_down is True
        assert len([e for e in log if e[0] == "provider_close"]) == 1
        assert len([e for e in log if e[0] == "dispose"]) == 1

        provider.shutdown()
        assert len([e for e in log if e[0] == "dispose"]) == 1
    finally:
        if provider is not None:
            provider.shutdown()


def test_cleanup_failed_operational_guards_block_use(tmp_path, monkeypatch):
    """C-F1 regression: CLOSE_FAILED instances are fail-closed for use.

    After a REAL close failure (T15): is_available() is False,
    _require_provider() raises, and the operational entry points (prefetch,
    queue_prefetch, handle_tool_call, the write batch handler) fail closed —
    all BEFORE the retry shutdown (T16) succeeds.
    """
    log: list[tuple[str, object]] = []
    real_close = SQLiteProvider.close
    close_calls = {"n": 0}

    async def flaky_close(self):
        close_calls["n"] += 1
        if close_calls["n"] == 1:
            raise RuntimeError("close-fp")
        return await real_close(self)

    monkeypatch.setattr(SQLiteProvider, "close", flaky_close)
    monkeypatch.setattr(AsyncEngine, "dispose", spy_engine_dispose(log))

    provider = None
    try:
        provider = _init_inmemory_provider(tmp_path, monkeypatch)
        # Even a cached context must NOT be served while the instance is
        # LOCKED — the guard fires before the cache lookup.
        provider._context_cache["prefetch"] = "cached-context"

        # T15: real close failure -> CLOSE_FAILED, provider retained.
        with pytest.raises(
            RuntimeError, match=r"cleanup failed; resources retained \(close-failed\)"
        ):
            provider.shutdown()
        assert provider._cleanup_failed is True
        assert provider._provider is not None
        assert provider._shut_down is False

        # CLOSE_FAILED fail-closed guards: locked for use until T16.
        assert provider.is_available() is False
        with pytest.raises(RuntimeError, match="close-failed"):
            provider._require_provider()

        # Operational entry points fail closed WITHOUT touching the retained
        # provider: prefetch returns no context, tools return an error.
        assert provider.prefetch("some query") == ""
        provider.queue_prefetch("some query")  # no-op — must not raise or schedule
        tool_result = provider.handle_tool_call("search", {"query": "x"})
        assert "error" in tool_result
        assert "close-failed" in tool_result

        # The write path raises through _require_provider (batch dropped).
        with pytest.raises(RuntimeError, match="close-failed"):
            asyncio.run(
                provider._handle_batch_write(
                    [([{"role": "user", "content": "Alice is a tester"}], "t1")]
                )
            )

        # None of the guarded calls released the retained engine.
        assert len([e for e in log if e[0] == "dispose"]) == 0

        # T16: retry shutdown -> the RETAINED close succeeds -> CLEAN.
        provider.shutdown()
        assert provider._provider is None
        assert provider._cleanup_failed is False
        assert provider._shut_down is True
        assert len([e for e in log if e[0] == "dispose"]) == 1
    finally:
        if provider is not None:
            provider.shutdown()


# ---------------------------------------------------------------------------
# 17/18. Lifecycle idempotence (SPEC item 8)
# ---------------------------------------------------------------------------


def test_two_initialize_shutdown_cycles(tmp_path, monkeypatch):
    log: list[tuple[str, object]] = []
    monkeypatch.setattr(SQLiteProvider, "close", spy_provider_close(log))
    monkeypatch.setattr(AsyncEngine, "dispose", spy_engine_dispose(log))
    monkeypatch.setattr(sqlite_provider_mod, "create_async_engine", spy_create_engine(log))
    monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")
    import memory_server.providers.embedding_provider as embedding_module

    monkeypatch.setattr(
        embedding_module, "SentenceTransformerEmbeddingProvider", MockEmbeddingProvider
    )

    provider = HermesProvider()
    try:
        for cycle in range(2):
            provider.initialize(
                session_id=f"cycle-{cycle}",
                config={
                    "db_url": f"sqlite+aiosqlite:///{tmp_path / 'cycle.db'}",
                    "path": str(tmp_path),
                },
                hermes_home=str(tmp_path),
            )
            engine = provider._provider.engine
            provider.shutdown()
            assert provider._provider is None
            assert provider._writer is None
            # File db -> AsyncAdaptedQueuePool: no leaked connections after the
            # full teardown (outbox stopped -> writer flush -> engine disposed).
            assert engine.pool.checkedout() == 0
    finally:
        provider.shutdown()

    assert len([e for e in log if e[0] == "provider_close"]) == 2
    disposes = [p for name, p in log if name == "dispose"]
    assert len(disposes) == 2
    assert disposes[0] is not disposes[1], "cycles must use TWO distinct engines"
    assert len([e for e in log if e[0] == "create_async_engine"]) == 2


def test_shutdown_twice_harmless(tmp_path, monkeypatch):
    log: list[tuple[str, object]] = []
    monkeypatch.setattr(AsyncEngine, "dispose", spy_engine_dispose(log))

    provider = None
    try:
        provider = _init_inmemory_provider(tmp_path, monkeypatch)
        provider.shutdown()
        provider.shutdown()
    finally:
        if provider is not None:
            provider.shutdown()

    assert provider._shut_down is True
    assert provider._provider is None
    assert provider._writer is None
    assert provider._extractor_runtime is None
    assert provider._llm_extractor is None
    assert len([e for e in log if e[0] == "dispose"]) == 1
