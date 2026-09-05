"""Shared pytest fixtures."""

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    """Clear the process-global Settings cache before and after every test.

    ``get_settings()`` is lru_cached; env-var mutations made by tests only
    take effect after ``cache_clear()``. Clearing around every test prevents
    cross-test leakage of env overrides (PLAN Risks #12).

    The repo's pre-commit smoke hook (release-candidate + ping) runs pytest
    from an environment where ``memory_server`` may not be importable; the
    fixture no-ops there.
    """
    try:
        from memory_server.settings import get_settings
    except ImportError:
        yield
        return
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def graph_test_isolation(tmp_path, monkeypatch):
    """Isolate graph tests from the process singleton and live snapshot."""
    from memory_server import server as server_module
    from memory_server.settings import get_settings

    live_snapshot = Path("data/graph.json").resolve()
    live_before = live_snapshot.read_bytes() if live_snapshot.exists() else None
    snapshot = tmp_path / "graph.json"
    monkeypatch.setenv("MEMORY_SERVER_GRAPH_SNAPSHOT_PATH", str(snapshot))
    get_settings.cache_clear()
    server_module._graph = None
    server_module._graph_router = None
    try:
        yield snapshot
    finally:
        server_module._graph = None
        server_module._graph_router = None
        get_settings.cache_clear()
        live_after = live_snapshot.read_bytes() if live_snapshot.exists() else None
        assert live_after == live_before
