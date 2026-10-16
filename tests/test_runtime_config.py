"""C03 frozen config: deep-frozen snapshot, ambient independence, no secrets.

Boundaries crossed: ``settings.py:390-393`` (cached process settings),
``lancedb_provider.py:344-365`` (the legacy ambient read the C03 factory must not
inherit; provider injection itself is C04's scope), ``runtime/config.py`` (C03)
and ``runtime/runtime_factory.py`` (C03). The shared fixture/helpers live in
``tests/test_runtime_factory.py``; no live Hermes config, DB, credential or
endpoint is read.
"""
from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, asdict
from types import MappingProxyType

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
