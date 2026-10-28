import json

import pytest

from memory_server.api.remember import remember
from memory_server.plugins.hermes.provider import HermesProvider
from memory_server.providers.lancedb_provider import LanceDBProvider


@pytest.mark.asyncio
async def test_server_audit_uses_persisted_sql_state(seam02_composition):
    import memory_server.server as server

    async with seam02_composition() as composition:
        provider = composition.runtime.dependencies.sqlite
        assert provider._url == composition.layout.sqlite.effective_url
        await remember(
            provider,
            subject="ServerAudit",
            predicate="verifies",
            object="persisted state",
            source="test",
        )

        search_result = json.loads(await server.search_tool(query="ServerAudit", limit=5))
        audit_result = json.loads(await server.audit_tool(audit_type="full"))

        assert search_result["total"] == 1
        assert audit_result["stats"]["total_facts"] == 1
        assert audit_result["stats"]["total_receipts"] == 1
        assert all(
            "SQLite provider not configured" not in warning
            for warning in audit_result["warnings"]
        )
        assert all(
            "No receipt store available" not in warning
            for warning in audit_result["warnings"]
        )
        assert all("orphan records" not in warning for warning in audit_result["warnings"])
        assert all("MemoryReceipt" not in error for error in audit_result["errors"])


@pytest.mark.asyncio
async def test_server_full_audit_surfaces_persisted_low_confidence_and_lifecycle(
    seam02_composition
):
    import memory_server.server as server

    async with seam02_composition() as composition:
        provider = composition.runtime.dependencies.sqlite
        assert provider._url == composition.layout.sqlite.effective_url
        stored = await remember(
            provider,
            subject="ServerLowConfidence",
            predicate="needs",
            object="audit coverage",
            confidence=0.1,
            source="test",
        )
        await provider.update_fact(stored["fact"].id, lifecycle_state="invalid_state")

        audit_result = json.loads(await server.audit_tool(audit_type="full"))

        assert audit_result["stats"]["confidence"]["total"] >= 1
        assert stored["fact"].id in audit_result["stats"]["confidence"]["low_confidence"]
        assert any(stored["fact"].id in warning for warning in audit_result["warnings"])
        assert any(
            error == f"Item '{stored['fact'].id}' has invalid lifecycle state 'invalid_state'"
            for error in audit_result["errors"]
        )


def test_hermes_provider_audit_uses_persisted_sql_state(tmp_path):
    provider = HermesProvider()
    provider.initialize(
        session_id="audit-alignment",
        config={
            "db_url": f"sqlite+aiosqlite:///{tmp_path / 'plugin-memory.db'}",
        },
        hermes_home=str(tmp_path),
    )

    provider.handle_tool_call(
        "remember",
        {
            "subject": "PluginAudit",
            "predicate": "verifies",
            "object": "persisted state",
            "confidence": 1.0,
            "source": "test",
        },
    )

    search_result = json.loads(provider.handle_tool_call("search", {"query": "PluginAudit"}))
    audit_result = json.loads(provider.handle_tool_call("audit", {"audit_type": "full"}))

    assert search_result["total"] == 1
    assert audit_result["stats"]["total_facts"] == 1
    assert audit_result["stats"]["total_receipts"] == 1
    assert all(
        "SQLite provider not configured" not in warning
        for warning in audit_result["warnings"]
    )
    assert all(
        "No receipt store available" not in warning
        for warning in audit_result["warnings"]
    )
    assert all("orphan records" not in warning for warning in audit_result["warnings"])
    assert all("MemoryReceipt" not in error for error in audit_result["errors"])

    provider.shutdown()


def test_hermes_provider_full_audit_surfaces_persisted_low_confidence(tmp_path):
    provider = HermesProvider()
    provider.initialize(
        session_id="audit-low-confidence",
        config={
            "db_url": f"sqlite+aiosqlite:///{tmp_path / 'plugin-low-confidence.db'}",
        },
        hermes_home=str(tmp_path),
    )

    remember_result = json.loads(
        provider.handle_tool_call(
            "remember",
            {
                "subject": "PluginLowConfidence",
                "predicate": "needs",
                "object": "audit coverage",
                "confidence": 0.1,
                "source": "test",
            },
        )
    )
    fact_id = remember_result["fact"]["id"]

    audit_result = json.loads(provider.handle_tool_call("audit", {"audit_type": "full"}))

    assert audit_result["stats"]["confidence"]["total"] >= 1
    assert fact_id in audit_result["stats"]["confidence"]["low_confidence"]
    assert any(fact_id in warning for warning in audit_result["warnings"])

    provider.shutdown()


def test_hermes_provider_audit_loads_graph_snapshot_and_active_vector_backend(tmp_path, monkeypatch):
    monkeypatch.setenv("MEMORY_VECTOR_BACKEND", "lancedb")

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    graph_path = data_dir / "graph.json"
    graph_path.write_text(
        json.dumps(
            {
                "nodes": {
                    "pluginaudit": {
                        "id": "pluginaudit",
                        "type": "entity",
                        "name": "PluginAudit",
                        "attributes": {},
                    }
                },
                "edges": [],
            }
        ),
        encoding="utf-8",
    )

    async def seed_vectors():
        vector_provider = LanceDBProvider(db_path=str(data_dir / "lancedb"), table="memories")
        try:
            ok = await vector_provider.upsert(
                point_id="pluginaudit",
                vector=[0.0] * 384,
                payload={"subject": "PluginAudit"},
            )
            assert ok is True
        finally:
            await vector_provider.close()

    import asyncio

    asyncio.run(seed_vectors())

    provider = HermesProvider()
    provider.initialize(
        session_id="audit-graph-vector",
        config={
            "db_url": f"sqlite+aiosqlite:///{tmp_path / 'plugin-memory.db'}",
            "path": str(tmp_path),
        },
        hermes_home=str(tmp_path),
    )

    provider.handle_tool_call(
        "remember",
        {
            "subject": "PluginAudit",
            "predicate": "verifies",
            "object": "graph and vector wiring",
            "confidence": 1.0,
            "source": "test",
        },
    )

    audit_result = json.loads(provider.handle_tool_call("audit", {"audit_type": "full"}))

    assert audit_result["stats"]["total_graph_nodes"] == 1
    assert audit_result["stats"]["total_qdrant_points"] == 1
    assert all(
        "Qdrant provider not configured" not in warning
        for warning in audit_result["warnings"]
    )
    assert all(
        "SQL/graph drift detected" not in warning
        for warning in audit_result["warnings"]
    )
    assert all(
        "SQL/vector drift detected" not in warning
        for warning in audit_result["warnings"]
    )

    provider.shutdown()
