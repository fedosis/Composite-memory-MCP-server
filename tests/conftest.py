"""Shared pytest fixtures."""

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
