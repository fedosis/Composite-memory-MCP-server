"""C03 frozen config: deep-frozen snapshot, ambient independence, no secrets.

Boundaries crossed: ``settings.py:390-393`` (cached process settings),
``lancedb_provider.py:344-365`` (the legacy ambient read the C03 factory must not
inherit; provider injection itself is C04's scope), ``runtime/config.py`` (C03)
and ``runtime/runtime_factory.py`` (C03). The shared fixture/helpers live in
``tests/test_runtime_factory.py``; no live Hermes config, DB, credential or
endpoint is read.

C04 additions cross ``runtime_factory.py`` (recipes now hand the frozen snapshot
to the constructors) and the guarded ambient reads of
``providers/{lancedb,qdrant,embedding}_provider.py`` and
``evaluation/{decay,confidence}.py``: supplied injection is used verbatim, an
omitted injection keeps the legacy ``get_settings()`` resolution exactly.
"""
from __future__ import annotations

import ast
import json
import textwrap
from dataclasses import FrozenInstanceError, asdict
from pathlib import Path
from types import MappingProxyType, SimpleNamespace

import pytest

from memory_server.paths import serialize_layout_redacted
from tests.test_runtime_factory import coherent_layout, factory_api, isolated  # noqa: F401

SECRETS = {"OPENAI_API_KEY": "sk-c03-openai-secret", "QDRANT_API_KEY": "qdrant-c03-secret",
           "MEMORY_SERVER_LLM_API_KEY": "llm-c03-secret"}


def config_api():
    from memory_server.runtime.config import RuntimeConfig

    return RuntimeConfig


def _settings(**overrides):
    from memory_server.settings import Settings

    values = dict(
        _env_file=None, db_url="sqlite+aiosqlite:///data/memory.db", vector_collection="c03-memories",
        ttl_days={"fact": 5.0, "decision": 7.0, "skill": 9.0, "entity": 11.0, "belief": 13.0},
    )
    values.update(overrides)
    return Settings(**values)


def test_config_snapshot_is_deep_frozen_and_copies_nested_mappings(tmp_path):
    layout = coherent_layout(tmp_path)
    settings = _settings()
    config = config_api().capture(layout, settings)
    # The snapshot COPIES nested mappings instead of aliasing the live model.
    settings.ttl_days["fact"] = 999.0
    settings.vector_collection = "mutated-after-capture"
    settings.confidence_source_reliability["verified"] = 0.01
    assert config.evaluation["ttl_days"]["fact"] == 5.0
    assert config.evaluation["confidence"]["source_reliability"]["verified"] == 0.9
    assert config.vector_collection == "c03-memories"
    assert isinstance(config.evaluation["ttl_days"], MappingProxyType)
    assert isinstance(config.evaluation["confidence"]["source_reliability"], MappingProxyType)
    assert isinstance(config.outbox, MappingProxyType)
    with pytest.raises(TypeError):
        config.evaluation["ttl_days"]["fact"] = 1.0
    with pytest.raises(TypeError):
        config.outbox["max_retries"] = 7
    with pytest.raises(FrozenInstanceError):
        config.sqlite_url = "sqlite+aiosqlite:///other.db"
    # The frozen StorageLayout is captured as-is, never re-resolved.
    assert config.layout is layout and config.sqlite_url == layout.sqlite.effective_url
    dto = config.as_dict()
    assert json.loads(json.dumps(dto, default=str))["vector_backend"] == "lancedb"
    assert {"sqlite_url", "sqlite_busy_timeout_ms", "vector_metric", "qdrant_port", "outbox", "evaluation"} <= set(dto)


async def test_ambient_env_mutation_after_create_does_not_change_dependencies(tmp_path, monkeypatch):
    from memory_server.providers.lancedb_provider import LanceDBProvider
    from memory_server.settings import get_settings

    layout = coherent_layout(tmp_path)
    # Baseline contrast: the legacy provider follows ambient env mutated after the
    # create-time snapshot (lancedb_provider.py:344-365). This characterization is
    # green on base and after C03: it is the defect the runtime must not inherit.
    monkeypatch.setenv("MEMORY_SERVER_LANCEDB_PATH", str(tmp_path / "ambient-lancedb"))
    get_settings.cache_clear()
    assert LanceDBProvider()._db_path == str(tmp_path / "ambient-lancedb")
    monkeypatch.setenv("MEMORY_SERVER_DB_URL", f"sqlite+aiosqlite:///{tmp_path / 'mutated.db'}")
    monkeypatch.setenv("MEMORY_SERVER_LANCEDB_PATH", str(tmp_path / "mutated-lancedb"))
    monkeypatch.setenv("MEMORY_SERVER_GRAPH_SNAPSHOT_PATH", str(tmp_path / "mutated-graph.json"))
    get_settings.cache_clear()
    factory = factory_api().create(layout)
    snapshot = factory.config
    runtime = factory.runtime
    assert runtime.config is snapshot and snapshot.sqlite_url == layout.sqlite.effective_url
    assert snapshot.vector_local_path == layout.vector.local_path
    await runtime.start()
    assert runtime.started
    assert runtime.dependencies.sqlite._url == layout.sqlite.effective_url
    assert runtime.dependencies.vector._db_path == str(layout.vector.local_path)
    assert (tmp_path / "root" / "data" / "memory.db").exists()
    assert not (tmp_path / "mutated.db").exists() and not (tmp_path / "mutated-lancedb").exists()
    assert not (tmp_path / "mutated-graph.json").exists()
    await runtime.close()
    assert not (tmp_path / "mutated-lancedb").exists()


async def test_secrets_are_absent_from_dto_logs_and_reports(tmp_path, monkeypatch):
    for key, value in SECRETS.items():
        monkeypatch.setenv(key, value)
    layout = coherent_layout(tmp_path)
    config = config_api().capture(layout)
    factory = factory_api().create(layout, config=config)
    runtime = factory.runtime
    await runtime.start()
    report = await runtime.close()
    remote = coherent_layout(
        tmp_path, name="remote-root", vector_backend="qdrant",
        qdrant_location="http://c03user:http-c03-secret@qdrant.invalid:6333",
    )
    blob = (
        repr(config)
        + repr(factory.describe())
        + repr(report)
        + json.dumps(asdict(report), default=str)
        + json.dumps(config.as_dict(), default=str)
        + json.dumps(serialize_layout_redacted(remote), default=str)
    )
    for value in SECRETS.values():
        assert value not in blob
    assert "http-c03-secret" not in json.dumps(serialize_layout_redacted(remote), default=str)
    # Secrets stay private in the environment accessors, never in a config DTO.
    assert not [name for name in config.as_dict() if "key" in name or "secret" in name]
    from memory_server.settings import get_llm_api_key, get_openai_api_key, get_qdrant_api_key

    assert get_openai_api_key() == SECRETS["OPENAI_API_KEY"]
    assert get_qdrant_api_key() == SECRETS["QDRANT_API_KEY"]
    assert get_llm_api_key() == SECRETS["MEMORY_SERVER_LLM_API_KEY"]
    assert remote.vector.kind == "remote" and remote.vector.qdrant_location not in blob


# --------------------------------------------------------------------------- #
# C04 — explicit provider/evaluator dependencies (narrow additive injection)
#
# Injection contract: each edited constructor takes ONE optional value. When it
# is supplied (by the factory, from the create-time snapshot) it is used verbatim
# and no ambient ``get_settings()`` happens; when it is omitted the legacy
# ambient resolution runs exactly as before, so every direct construction keeps
# today's defaults and precedence. Providers take a settings-shaped source,
# engines take an explicit values mapping keyed by the Settings field names they
# read. No field rename, no interface change, no new precedence rule.
# --------------------------------------------------------------------------- #

# Modules and classes whose constructors must carry the guarded injection.
PROVIDER_MODULES = {
    "src/memory_server/providers/lancedb_provider.py": ("LanceDBProvider",),
    "src/memory_server/providers/qdrant_provider.py": ("QdrantProvider",),
    "src/memory_server/providers/embedding_provider.py": (
        "SentenceTransformerEmbeddingProvider",
        "OpenAIEmbeddingProvider",
    ),
}
ENGINE_MODULES = {
    "src/memory_server/evaluation/decay.py": ("DecayEngine",),
    "src/memory_server/evaluation/confidence.py": ("ConfidenceEngine",),
}
# The two guarded forms; every other ``get_settings()`` in those constructors is
# an ambient read the runtime path must not have.
PROVIDER_GUARD = "settings if settings is not None else get_settings()"
ENGINE_GUARD = "SimpleNamespace(**settings) if settings is not None else get_settings()"
RUNTIME_CONSTRUCTORS = (
    "LanceDBProvider",
    "QdrantProvider",
    "OpenAIEmbeddingProvider",
    "SentenceTransformerEmbeddingProvider",
)
FACTORY_RELATIVE = "src/memory_server/runtime/runtime_factory.py"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def engine_values(config):
    """Explicit engine values taken verbatim from the frozen create-time snapshot.

    Keys are the Settings field names each engine reads, so each mapping is a
    drop-in replacement for the ambient resolution used before C04.
    """
    evaluation = config.evaluation
    decay, confidence = evaluation["decay"], evaluation["confidence"]
    return (
        {
            "ttl_days": evaluation["ttl_days"],
            "decay_archive_threshold": decay["archive_threshold"],
            "decay_stale_ratio": decay["stale_ratio"],
            "decay_archive_ratio": decay["archive_ratio"],
            "decay_forgotten_ratio": decay["forgotten_ratio"],
            "decay_default_ttl_days": decay["default_ttl_days"],
            "decay_base": decay["base"],
            "decay_confidence_floor": decay["confidence_floor"],
        },
        {
            "confidence_source_reliability": confidence["source_reliability"],
            "confidence_ttl_days": confidence["ttl_days"],
            "confidence_lifecycle_multipliers": confidence["lifecycle_multipliers"],
            "confidence_corroboration_boost": confidence["corroboration_boost"],
            "confidence_boost_threshold": confidence["boost_threshold"],
            "confidence_conflict_penalty": confidence["conflict_penalty"],
            "confidence_penalty_threshold": confidence["penalty_threshold"],
        },
    )


def ambient_calls(source):
    """AST nodes of the ``get_settings()`` calls in a module or code snippet."""
    return [
        node
        for node in ast.walk(ast.parse(textwrap.dedent(source)))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "get_settings"
    ]


def init_sources(relative, classes):
    """Source text of ``__init__`` for each named class of a module."""
    source = (repo_root() / relative).read_text()
    lines = source.splitlines()
    found = {}
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ClassDef) and node.name in classes:
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == "__init__":
                    found[node.name] = "\n".join(lines[item.lineno - 1:item.end_lineno])
    return found


def ambient_references(source):
    """AST nodes of a module (or a dedented snippet) that reference ``get_settings``."""
    found = []
    for node in ast.walk(ast.parse(textwrap.dedent(source))):
        if isinstance(node, ast.Name) and node.id == "get_settings":
            found.append(node)
        elif isinstance(node, ast.Attribute) and node.attr == "get_settings":
            found.append(node)
        elif isinstance(node, ast.alias) and node.name.split(".")[-1] == "get_settings":
            found.append(node)
    return found


def test_runtime_engines_use_injected_values_not_ambient_settings(tmp_path, monkeypatch):
    """Baseline contrast: the legacy engine follows ambient mutation, an injected one does not."""
    from memory_server.evaluation.confidence import ConfidenceEngine
    from memory_server.evaluation.decay import DecayEngine
    from memory_server.settings import get_settings

    layout = coherent_layout(tmp_path)
    config = config_api().capture(layout)  # create() is the one allowed ambient read
    decay_values, confidence_values = engine_values(config)
    assert decay_values["decay_stale_ratio"] == 0.7 and decay_values["decay_base"] == 2.0
    assert confidence_values["confidence_ttl_days"] == 90.0

    # Mutate the ambient environment AFTER the create-time snapshot.
    monkeypatch.setenv("MEMORY_SERVER_DECAY_STALE_RATIO", "0.5")
    monkeypatch.setenv("MEMORY_SERVER_DECAY_BASE", "7.0")
    monkeypatch.setenv("MEMORY_SERVER_TTL_DAYS__FACT", "1.5")
    monkeypatch.setenv("MEMORY_SERVER_CONFIDENCE_TTL_DAYS", "7")
    get_settings.cache_clear()

    # Behavioural baseline (unchanged code): a non-injected engine follows the
    # mutated ambient settings — the runtime must not inherit this.
    legacy_decay, legacy_confidence = DecayEngine(), ConfidenceEngine()
    assert legacy_decay._stale_ratio == 0.5 and legacy_decay._decay_base == 7.0
    assert legacy_decay.get_ttl("fact") == 1.5
    assert legacy_confidence._ttl_days == 7.0
    assert legacy_confidence.score_fact(
        {"source_type": "verified", "created_at": None}) == pytest.approx(0.9)

    # The injected engines use the create-time values verbatim and ignore the
    # mutation for every value they read.
    injected_decay = DecayEngine(settings=decay_values)
    injected_confidence = ConfidenceEngine(settings=confidence_values)
    assert injected_decay._stale_ratio == 0.7
    assert injected_decay._archive_threshold == 0.2
    assert injected_decay._archive_ratio == 1.0
    assert injected_decay._forgotten_ratio == 2.0
    assert injected_decay._default_ttl_days == 90.0
    assert injected_decay._decay_base == 2.0
    assert injected_decay._confidence_floor == 0.1
    assert injected_decay.get_ttl("fact") == 90.0 and injected_decay.get_ttl("belief") == 180.0
    assert injected_confidence._ttl_days == 90.0
    assert injected_confidence._source_reliability["verified"] == 0.9
    assert injected_confidence._lifecycle_multipliers["active"] == 1.0
    assert injected_confidence._corroboration_boost_value == pytest.approx(0.10)
    assert injected_confidence._boost_threshold == 2 and injected_confidence._penalty_threshold == 1
    assert injected_confidence.score_fact(
        {"source_type": "verified", "created_at": None}) == pytest.approx(0.9)

    # Injection is read-only: neither the ambient settings nor the frozen
    # snapshot change because an injected engine was built.
    assert get_settings().decay_stale_ratio == 0.5
    assert config.evaluation["decay"]["stale_ratio"] == 0.7


def test_provider_constructor_defaults_are_unchanged_without_injection(tmp_path, monkeypatch):
    """Legacy parity: omitting the injection (or passing None) keeps today's resolution."""
    from memory_server.evaluation.confidence import ConfidenceEngine
    from memory_server.evaluation.decay import DecayEngine
    from memory_server.providers.embedding_provider import (
        OpenAIEmbeddingProvider,
        SentenceTransformerEmbeddingProvider,
    )
    from memory_server.providers.lancedb_provider import LanceDBProvider
    from memory_server.providers.qdrant_provider import QdrantProvider
    from memory_server.settings import get_settings

    lancedb_path = tmp_path / "legacy-lancedb"
    monkeypatch.setenv("MEMORY_SERVER_LANCEDB_PATH", str(lancedb_path))
    monkeypatch.setenv("MEMORY_SERVER_VECTOR_COLLECTION", "legacy-table")
    monkeypatch.setenv("MEMORY_SERVER_VECTOR_METRIC", "l2")
    monkeypatch.setenv("MEMORY_SERVER_VECTOR_SIZE", "77")
    monkeypatch.setenv("MEMORY_SERVER_QDRANT_LOCATION", ":memory:")
    monkeypatch.setenv("MEMORY_SERVER_EMBEDDING_MODEL", "legacy-model")
    monkeypatch.setenv("MEMORY_SERVER_EMBEDDING_BATCH_SIZE", "9")
    monkeypatch.setenv("MEMORY_SERVER_OPENAI_EMBEDDING_MODEL", "legacy-openai")
    monkeypatch.setenv("MEMORY_SERVER_OPENAI_EMBEDDING_VECTOR_SIZE", "2048")
    monkeypatch.setenv("MEMORY_SERVER_DECAY_STALE_RATIO", "0.5")
    monkeypatch.setenv("MEMORY_SERVER_CONFIDENCE_TTL_DAYS", "12")
    get_settings.cache_clear()

    # The parameter absent: every direct construction keeps today's resolution.
    provider = LanceDBProvider()
    assert provider._db_path == str(lancedb_path)
    assert provider._table_name == "legacy-table"
    assert provider._metric == "l2"
    assert provider._vector_size == 77
    assert Path(provider._db_path).is_relative_to(tmp_path)  # scratch only

    local = SentenceTransformerEmbeddingProvider()
    assert (local._model_name, local._device, local._batch_size) == ("legacy-model", None, 9)
    assert local._model is None  # lazy: no model load, no download

    remote = OpenAIEmbeddingProvider()
    assert (remote._model, remote._vector_size) == ("legacy-openai", 2048)
    assert remote._client is None  # lazy: no client, no endpoint

    qdrant = QdrantProvider()  # location ":memory:" from Settings: no endpoint
    assert (qdrant._collection, qdrant._vector_size) == ("legacy-table", 77)

    assert DecayEngine()._stale_ratio == 0.5
    assert ConfidenceEngine()._ttl_days == 12.0


def test_injection_does_not_change_precedence_or_defaults(tmp_path, monkeypatch):
    """Explicit arg > injected value > ambient, and the ambient precedence is untouched."""
    from memory_server.evaluation.decay import (
        ARCHIVE_RATIO,
        FORGOTTEN_RATIO,
        PER_TYPE_TTL,
        STALE_RATIO,
        DecayEngine,
    )
    from memory_server.providers.lancedb_provider import LanceDBProvider
    from memory_server.settings import get_settings

    layout = coherent_layout(tmp_path)
    config = config_api().capture(layout)
    decay_values, confidence_values = engine_values(config)

    # settings.py:7-11 unchanged: canonical env > .env file > field default.
    (tmp_path / ".env").write_text(
        "MEMORY_SERVER_VECTOR_SIZE=11\nMEMORY_SERVER_DECAY_STALE_RATIO=0.11\n"
    )
    monkeypatch.setenv("MEMORY_SERVER_VECTOR_COLLECTION", "env-collection")
    monkeypatch.setenv("MEMORY_SERVER_DECAY_STALE_RATIO", "0.33")
    get_settings.cache_clear()
    ambient = get_settings()
    assert ambient.vector_collection == "env-collection"
    assert ambient.vector_size == 11  # .env beats the field default
    assert ambient.decay_stale_ratio == 0.33  # env beats .env
    assert config.vector_collection == "memories"  # create-time snapshot, not live

    injected = SimpleNamespace(
        lancedb_path=tmp_path / "injected-lancedb",
        vector_collection="injected-collection",
        vector_metric="cosine",
        vector_size=5,
        qdrant_location=":memory:",
        qdrant_port=6333,
        qdrant_prefer_grpc=False,
        embedding_model="injected-model",
        embedding_device=None,
        embedding_batch_size=1,
        embedding_base_url=None,
        openai_embedding_model="injected-openai",
        openai_embedding_vector_size=8,
    )
    provider = LanceDBProvider(settings=injected)
    assert (provider._table_name, provider._vector_size) == ("injected-collection", 5)
    assert provider._metric == "cosine"
    # An explicit argument still outranks the injected value, an explicit None
    # injection is the same path as an absent one, and omitting the injection
    # still follows the ambient value.
    assert LanceDBProvider(table="explicit", settings=injected)._table_name == "explicit"
    assert LanceDBProvider(settings=None)._table_name == "env-collection"
    assert (LanceDBProvider()._table_name, LanceDBProvider()._vector_size) == ("env-collection", 11)

    base_engine = DecayEngine(settings=decay_values)
    overridden = DecayEngine(
        per_type_ttl={"fact": 1.0}, archive_threshold=0.8, stale_ratio=0.9, decay_base=3.0,
        settings=decay_values,
    )
    assert base_engine._stale_ratio == 0.7  # injected beats ambient (0.33)
    assert base_engine.get_ttl("fact") == 90.0
    assert overridden.get_ttl("fact") == 1.0  # the per-call override still wins
    assert (overridden._stale_ratio, overridden._archive_threshold) == (0.9, 0.8)
    assert overridden._decay_base == 3.0
    assert overridden.get_ttl("belief") == 180.0  # un-overridden key from the injection

    # The engines copy what they are given: the frozen snapshot stays frozen and
    # the module-level legacy aliases are neither removed nor rewritten.
    from memory_server.evaluation.confidence import ConfidenceEngine

    engine = ConfidenceEngine(settings=confidence_values)
    engine._source_reliability["verified"] = 0.01
    assert confidence_values["confidence_source_reliability"]["verified"] == 0.9
    with pytest.raises(TypeError):
        config.evaluation["decay"]["stale_ratio"] = 0.0
    assert (STALE_RATIO, ARCHIVE_RATIO, FORGOTTEN_RATIO) == (0.7, 1.0, 2.0)
    assert PER_TYPE_TTL["fact"] == 90.0


async def test_no_new_ambient_settings_reads_remain_on_the_runtime_path(tmp_path, monkeypatch):
    """Static and behavioural: the runtime path resolves ambient settings only at create()."""
    root = repo_root()
    factory_source = (root / FACTORY_RELATIVE).read_text()
    # Static: no recipe imports or resolves ambient settings...
    assert ambient_references(factory_source) == []
    calls = []
    for node in ast.walk(ast.parse(factory_source)):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in RUNTIME_CONSTRUCTORS:
            calls.append(node)
    # ...and every provider the runtime builds is handed the frozen snapshot.
    assert {node.func.id for node in calls} == set(RUNTIME_CONSTRUCTORS)
    for node in calls:
        assert "settings" in {keyword.arg for keyword in node.keywords}, ast.dump(node)

    # Static: the ambient resolution of every edited constructor is the single
    # guarded fallback; anything else would be a new ambient read.
    for relative, classes in PROVIDER_MODULES.items():
        sources = init_sources(relative, classes)
        assert set(sources) == set(classes), relative
        for name, source in sources.items():
            assert source.count(PROVIDER_GUARD) == 1, (relative, name)
            assert len(ambient_calls(source)) == 1, (relative, name)
    for relative, classes in ENGINE_MODULES.items():
        sources = init_sources(relative, classes)
        assert set(sources) == set(classes), relative
        for name, source in sources.items():
            assert source.count(ENGINE_GUARD) == 1, (relative, name)
            assert len(ambient_calls(source)) == 1, (relative, name)

    # Documented drift: the graph provider has no ambient read at all (its
    # snapshot path was always an explicit argument), so it carries no injection
    # parameter and the recipe already passes the frozen value.
    graph_source = (root / "src/memory_server/providers/graph_provider.py").read_text()
    assert ambient_references(graph_source) == []
    assert "SimpleGraph(snapshot_path=runtime.config.graph_snapshot_path)" in factory_source

    # Behavioural: an invalid ambient value set AFTER create() proves the legacy
    # ambient resolution is still live (a direct construction now fails), while
    # the runtime path — built only from the create-time snapshot — starts.
    from pydantic import ValidationError

    from memory_server.providers.lancedb_provider import LanceDBProvider
    from memory_server.settings import get_settings

    layout = coherent_layout(tmp_path)
    factory = factory_api().create(layout)  # create-time capture: the allowed read
    monkeypatch.setenv("MEMORY_SERVER_VECTOR_SIZE", "0")  # rejected: must be > 0
    get_settings.cache_clear()
    with pytest.raises(ValidationError):
        LanceDBProvider()  # the legacy path still resolves ambient settings

    runtime = factory.runtime
    await runtime.start()
    assert runtime.started
    assert runtime.dependencies.vector._db_path == str(factory.config.vector_local_path)
    assert runtime.dependencies.vector._vector_size == factory.config.vector_size
    assert runtime.dependencies.embedder._model is None  # lazy: no model load
    report = await runtime.close()
    assert report.code is None
    assert Path(runtime.dependencies.sqlite._url.split("///")[-1]).is_relative_to(tmp_path)
