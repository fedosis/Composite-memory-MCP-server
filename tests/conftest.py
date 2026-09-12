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
def synthetic_storage_env(tmp_path, monkeypatch):
    """Synthetic-Settings isolation harness (IMPL slice S0, DETAIL 14.1).

    Every provider-touching test in the profile-isolation modules MUST request
    this fixture *before* constructing a ``HermesProvider``. It scrubs the
    deployment environment, pins the working directory to ``tmp_path``, injects
    one synthetic ``Settings`` instance into every module-local
    ``get_settings`` reference — proving that injection by assertion — and makes
    every resolved store path fail the test if it lands inside a live CMMS data
    root. The repository's smoke hook runs pytest where ``memory_server`` may
    not be importable; the fixture then reports the blocker instead of silently
    passing.
    """
    from tests.synthetic_storage_env import activate_synthetic_storage_env

    return activate_synthetic_storage_env(tmp_path, monkeypatch)


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
