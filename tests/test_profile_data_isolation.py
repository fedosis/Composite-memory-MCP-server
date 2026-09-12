"""Behavioral regression coverage for native profile storage isolation.

Safety (IMPL slice S0, DETAIL 14.1): every test here runs through the
``synthetic_storage_env`` fixture from ``tests/conftest.py``. That fixture
scrubs the deployment environment, pins the working directory to the pytest
temporary root, injects one synthetic ``Settings`` instance into every
module-local ``get_settings`` reference and proves the injection by assertion
*before* any ``HermesProvider`` is constructed. Store paths are then guarded:
a resolved path inside ``/home/shtorm/.hermes/data`` or
``/home/shtorm/memory-server/data`` fails the test instead of touching data.

The module deliberately uses only API that exists at baseline ``da1cd0e``
(``HermesProvider``, ``initialize``/``shutdown`` and the attribute names of the
real store providers), so the same file can be executed against a clean
baseline checkout to record the mandatory behavioral RED.
"""
from __future__ import annotations

from pathlib import Path

from memory_server.plugins.hermes.provider import HermesProvider


def _sqlite_path(provider: HermesProvider) -> Path:
    assert provider._provider is not None
    prefix = "sqlite+aiosqlite:///"
    assert provider._provider._url.startswith(prefix)
    return Path(provider._provider._url[len(prefix):])


def _contained(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def test_real_provider_keeps_all_store_paths_inside_each_profile(
    tmp_path: Path, synthetic_storage_env
) -> None:
    """One install path must not make two profiles share projections."""
    env = synthetic_storage_env
    env.assert_injection()

    installation = env.install_dir
    profile_a = tmp_path / "profile-a"
    profile_b = tmp_path / "profile-b"
    profile_a.mkdir()
    profile_b.mkdir()
    env.assert_not_live(
        installation,
        profile_a,
        profile_b,
        Path.cwd(),
        label="synthetic fixture root",
    )

    providers: list[HermesProvider] = []
    observed: list[tuple[Path, Path, Path]] = []
    config = {
        "path": str(installation),
        "db_url": "sqlite+aiosqlite:///data/memory.db",
        "writer": {"flush_interval": 0.01, "max_batch": 1},
    }

    try:
        for label, home in (("profile-a", profile_a), ("profile-b", profile_b)):
            provider = HermesProvider()
            providers.append(provider)
            provider.initialize(
                session_id=f"profile-isolation-{label}",
                config=config,
                hermes_home=str(home),
            )
            # Runtime guard: no resolved store path may be a live store, and
            # every one of them must stay inside this profile's home.
            env.assert_provider_synthetic(provider, profile_home=home)
            assert provider._lancedb is not None
            assert provider._graph is not None
            observed.append(
                (
                    _sqlite_path(provider),
                    Path(provider._lancedb._db_path),
                    Path(provider._graph._snapshot_path),
                )
            )
    finally:
        for provider in reversed(providers):
            provider.shutdown()
            assert provider._outbox_task is None
            assert provider._outbox_worker is None
            assert provider._writer is None
            assert provider._provider is None
            assert provider._lancedb is None

    paths_a, paths_b = observed
    assert _contained(paths_a[0], profile_a), observed
    assert _contained(paths_b[0], profile_b), observed
    assert _contained(paths_a[1], profile_a), observed
    assert _contained(paths_b[1], profile_b), observed
    assert _contained(paths_a[2], profile_a), observed
    assert _contained(paths_b[2], profile_b), observed
    assert paths_a[0] != paths_b[0]
    assert paths_a[1] != paths_b[1]
    assert paths_a[2] != paths_b[2]


def test_native_provider_legacy_in_memory_config_initializes_like_baseline(
    synthetic_storage_env,
) -> None:
    """SPEC-5 regression: legacy standalone in-memory config must still work.

    Baseline ``da1cd0e`` accepted ``config={"db_url": "sqlite+aiosqlite://"}``
    with no ``hermes_home``: SQLite ran in standalone in-memory mode, no
    profile root was claimed and the polling outbox worker was skipped
    (``_supports_background_outbox`` refuses in-memory URLs). The profile-layout
    rewrite must not reject that configuration, and the CWD-relative standalone
    root must land in the synthetic temporary directory.
    """
    env = synthetic_storage_env
    env.assert_injection()

    provider = HermesProvider()
    try:
        provider.initialize(
            session_id="legacy-standalone-in-memory",
            config={"db_url": "sqlite+aiosqlite://"},
        )
        assert provider._initialized is True
        assert provider._provider is not None
        assert provider._provider._url == "sqlite+aiosqlite://"
        # In-memory URL: no background outbox worker, no projection store yet.
        assert provider._outbox_task is None
        assert provider._outbox_worker is None
        assert provider._lancedb is None
        # No file-backed store exists at all, so there is no path to guard
        # beyond the in-memory URL asserted above.
        env.assert_provider_synthetic(provider, require_paths=False)
        layout = getattr(provider, "_storage_layout", None)
        if layout is not None:
            assert layout.mode == "standalone"
            assert env.root == Path(layout.data_root)
            assert layout.sqlite.local_path is None
    finally:
        provider.shutdown()

    assert provider._initialized is False
    assert provider._provider is None
    assert provider._writer is None
    assert provider._shut_down is True
