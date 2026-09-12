"""Synthetic storage-settings harness for the profile-isolation test modules.

Safety contract (IMPL_RESTART_PLAN slice S0, DETAIL 14.1):

* every test that constructs a ``HermesProvider`` activates
  :func:`activate_synthetic_storage_env` *before* the provider is built;
* the deployment environment keys that can place stores on the live machine are
  removed, and the live extraction/LLM keys are removed with them so a synthetic
  test can never reach a real model endpoint;
* the working directory is moved into the pytest temporary root, and
  ``HOME`` / ``HERMES_HOME`` / ``TMPDIR`` are scrubbed and re-pinned inside that
  root, so the standalone/CWD-relative defaults and the live Hermes config
  cannot be reached — in this process or in any child it spawns;
* exactly one synthetic ``Settings`` instance is built and injected into every
  module-local ``get_settings`` reference; the injection is proven by assertion
  before the caller constructs anything;
* the sentence-transformers embedder is replaced with ``MockEmbeddingProvider``
  so no test downloads or calls a live model;
* every resolved store path is asserted to be outside the live CMMS data roots.

The guards raise ``AssertionError`` (a test failure), never a warning: a store
path that resolves into a live root must stop the run instead of touching data.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

LIVE_DATA_ROOTS: tuple[Path, ...] = (
    Path("/home/shtorm/.hermes/data"),
    Path("/home/shtorm/memory-server/data"),
)

# Storage-placement keys listed in IMPL_TEST_EVIDENCE.txt.
STORAGE_ENV_KEYS: tuple[str, ...] = (
    "MEMORY_SERVER_STORAGE_MODE",
    "MEMORY_SERVER_DATA_ROOT",
    "MEMORY_SERVER_DB_URL",
    "MEMORY_SERVER_PATH",
    "MEMORY_SERVER_LANCEDB_PATH",
    "MEMORY_SERVER_GRAPH_SNAPSHOT_PATH",
    "MEMORY_SERVER_VECTOR_BACKEND",
    "MEMORY_VECTOR_BACKEND",
    "MEMORY_GRAPH_SNAPSHOT_PATH",
    "MEMORY_SERVER_QDRANT_LOCATION",
    "MEMORY_QDRANT_URL",
    "MEMORY_SERVER_VECTOR_COLLECTION",
)

# Live extraction/embedding keys: a synthetic test must not reach a real model.
LIVE_MODEL_ENV_KEYS: tuple[str, ...] = (
    "MEMORY_SERVER_EXTRACTION_MODE",
    "MEMORY_SERVER_LLM_API_KEY",
    "MEMORY_SERVER_LLM_BASE_URL",
    "MEMORY_SERVER_LLM_MODEL",
    "MEMORY_SERVER_LLM_TIMEOUT_SECONDS",
    "MEMORY_SERVER_LLM_MAX_INPUT_CHARS",
    "MEMORY_SERVER_LLM_CONFIDENCE_GATE",
    "MEMORY_SERVER_EMBEDDING_BASE_URL",
    "OPENAI_API_KEY",
)

DEPLOYMENT_ENV_KEYS: tuple[str, ...] = STORAGE_ENV_KEYS + LIVE_MODEL_ENV_KEYS

# B02 hole H-B: process-level keys that let code inside a test process (or a
# child process) resolve a live machine path or live Hermes config. They are
# scrubbed *before* any memory_server import and re-pinned into the synthetic
# root, so HOME/HERMES_HOME/TMPDIR are synthetic and provably so.
PROCESS_ENV_KEYS: tuple[str, ...] = (
    "HOME",
    "HERMES_HOME",
    "TMPDIR",
)

SCRUBBED_ENV_KEYS: tuple[str, ...] = DEPLOYMENT_ENV_KEYS + PROCESS_ENV_KEYS


_SQLITE_PREFIX = "sqlite+aiosqlite:///"


def lexical(path: str | os.PathLike[str]) -> Path:
    """Return an absolute lexical path (no symlink resolution)."""
    return Path(os.path.abspath(os.path.normpath(os.path.expanduser(str(path)))))


def live_root_for(path: str | os.PathLike[str]) -> Path | None:
    """Return the live CMMS data root containing ``path``, else ``None``."""
    candidate = lexical(path)
    for root in LIVE_DATA_ROOTS:
        anchor = lexical(root)
        if candidate == anchor:
            return anchor
        try:
            candidate.relative_to(anchor)
        except ValueError:
            continue
        return anchor
    return None


def assert_not_live(path: str | os.PathLike[str], *, label: str = "path") -> Path:
    """Fail the test when ``path`` resolves inside a live CMMS data root."""
    candidate = lexical(path)
    root = live_root_for(candidate)
    if root is not None:
        raise AssertionError(
            f"live-store guard: {label} resolves inside the live CMMS data root "
            f"{root}: {candidate}"
        )
    return candidate


def _require_contained(path: Path, root: Path, *, label: str) -> None:
    candidate = lexical(path)
    anchor = lexical(root)
    if candidate != anchor:
        try:
            candidate.relative_to(anchor)
        except ValueError as exc:
            raise AssertionError(
                f"profile-containement guard: {label} is outside {anchor}: {candidate}"
            ) from exc
    assert_not_live(candidate, label=label)


def _presence(exclude: Mapping[str, str] | None = None) -> tuple[str, ...]:
    pinned = exclude or {}
    return tuple(
        key
        for key in SCRUBBED_ENV_KEYS
        if key not in pinned and os.environ.get(key) is not None
    )


@dataclass(frozen=True)
class SyntheticStorageEnv:
    """Handle returned by :func:`activate_synthetic_storage_env`."""

    root: Path
    install_dir: Path
    fallback_repo_root: Path
    settings: Any
    scrubbed_keys: tuple[str, ...]
    present_before_scrub: tuple[str, ...]
    pinned_env: Mapping[str, str] = field(default_factory=dict)
    injected_modules: tuple[Any, ...] = field(default=())
    home_dir: Path | None = None
    hermes_home_dir: Path | None = None
    tmp_dir: Path | None = None

    @property
    def injected_module_names(self) -> tuple[str, ...]:
        return tuple(module.__name__ for module in self.injected_modules)

    def assert_injection(self) -> None:
        """Prove the synthetic Settings instance is the one every module sees."""
        assert self.settings is not None
        for module in self.injected_modules:
            resolved = getattr(module, "get_settings")()
            assert resolved is self.settings, (
                f"settings injection failed: {module.__name__}.get_settings() "
                "did not return the synthetic Settings instance"
            )
        for key, expected in self.pinned_env.items():
            assert os.environ.get(key) == expected, (
                f"synthetic harness pin drifted for {key}: "
                f"{os.environ.get(key)!r} != {expected!r}"
            )
        leaked = _presence(exclude=self.pinned_env)
        assert leaked == (), (
            f"deployment environment keys leaked into the synthetic harness: {leaked!r}"
        )
        # B02 hole H-B: HOME / HERMES_HOME / TMPDIR must be synthetic, inside
        # the synthetic root, and must drive ``expanduser`` for this process.
        for key, pinned_path in (
            ("HOME", self.home_dir),
            ("HERMES_HOME", self.hermes_home_dir),
            ("TMPDIR", self.tmp_dir),
        ):
            if pinned_path is None:
                continue
            assert os.environ.get(key) == str(pinned_path), (
                f"synthetic harness {key} pin drifted: "
                f"{os.environ.get(key)!r} != {str(pinned_path)!r}"
            )
            _require_contained(pinned_path, self.root, label=f"synthetic {key}")
        if self.home_dir is not None:
            assert lexical("~") == lexical(self.home_dir), (
                "synthetic harness HOME does not drive expanduser: "
                f"{lexical('~')} != {lexical(self.home_dir)}"
            )
        assert_not_live(self.root, label="pytest temporary root")
        assert_not_live(self.install_dir, label="synthetic install dir")
        assert_not_live(self.fallback_repo_root, label="synthetic repo-root fallback")
        assert lexical(Path.cwd()) == self.root, (
            f"synthetic harness cwd drifted: {lexical(Path.cwd())} != {self.root}"
        )
        for label, candidate in (
            ("settings.lancedb_path", self.settings.lancedb_path),
            ("settings.graph_snapshot_path", self.settings.graph_snapshot_path),
        ):
            resolved = Path(candidate)
            assert_not_live(
                resolved if resolved.is_absolute() else self.root / resolved,
                label=label,
            )

    def assert_not_live(self, *paths: str | os.PathLike[str], label: str = "path") -> None:
        for path in paths:
            assert_not_live(path, label=label)

    def assert_provider_synthetic(
        self,
        provider: Any,
        *,
        profile_home: Path | None = None,
        require_paths: bool = True,
    ) -> tuple[tuple[str, Path], ...]:
        """Assert every store path a live provider would open is synthetic.

        ``require_paths=False`` is for a provider that legitimately owns no
        file-backed store (the legacy in-memory SQLite case); the caller must
        then assert the absence itself, which is what the caller does.
        """
        observed = provider_store_paths(provider)
        if require_paths:
            assert observed, "provider exposes no store path — nothing to guard"
        for label, path in observed:
            assert_not_live(path, label=label)
            if profile_home is not None:
                _require_contained(path, profile_home, label=label)
        return observed


def provider_store_paths(provider: Any) -> tuple[tuple[str, Path], ...]:
    """Return the paths a provider instance would read/write, in stable order."""
    observed: list[tuple[str, Path]] = []

    layout = getattr(provider, "_storage_layout", None)
    if layout is not None:
        observed.append(("layout.data_root", Path(layout.data_root)))
        observed.append(("layout.root_lock_path", Path(layout.root_lock_path)))
        observed.append(("layout.graph_snapshot_path", Path(layout.graph_snapshot_path)))
        observed.append(("layout.graph_lock_path", Path(layout.graph_lock_path)))
        if layout.sqlite.local_path is not None:
            observed.append(("layout.sqlite.local_path", Path(layout.sqlite.local_path)))
        if layout.vector.local_path is not None:
            observed.append(("layout.vector.local_path", Path(layout.vector.local_path)))

    sqlite_provider = getattr(provider, "_provider", None)
    db_url = getattr(sqlite_provider, "_url", None)
    if isinstance(db_url, str) and db_url.startswith(_SQLITE_PREFIX):
        path_part = db_url[len(_SQLITE_PREFIX):]
        if path_part and path_part != ":memory:":
            observed.append(("provider.db_url", Path(path_part)))

    lancedb = getattr(provider, "_lancedb", None)
    lancedb_path = getattr(lancedb, "_db_path", None)
    if lancedb_path:
        observed.append(("lancedb._db_path", Path(str(lancedb_path))))

    graph = getattr(provider, "_graph", None)
    snapshot_path = getattr(graph, "_snapshot_path", None)
    if snapshot_path:
        observed.append(("graph._snapshot_path", Path(str(snapshot_path))))

    return tuple(observed)


def purge_deployment_env(monkeypatch: Any) -> tuple[str, ...]:
    """Remove every deployment/model/process env key; return those present."""
    present = _presence()
    for key in SCRUBBED_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    return present


def _install_mock_embedder(monkeypatch: Any) -> None:
    import memory_server.providers.embedding_provider as embedding_module
    from memory_server.providers.embedding_provider import MockEmbeddingProvider

    monkeypatch.setattr(
        embedding_module,
        "SentenceTransformerEmbeddingProvider",
        MockEmbeddingProvider,
    )


def activate_synthetic_storage_env(tmp_path: Path, monkeypatch: Any) -> SyntheticStorageEnv:
    """Scrub the deployment environment and inject synthetic Settings.

    Returns a guard handle. Call this before constructing any provider; the
    function itself asserts the injection, the CWD pin, the HOME/HERMES_HOME/
    TMPDIR pins and every synthetic root.
    """
    # B02 hole H-B: scrub the deployment/model/process environment *before*
    # importing any memory_server module, so nothing module-local can capture a
    # live HOME/HERMES_HOME/TMPDIR, a live store path or a live credential.
    present_before = purge_deployment_env(monkeypatch)

    import memory_server.paths as paths_module
    import memory_server.settings as settings_module
    from memory_server.plugins.hermes import config as config_module
    from memory_server.plugins.hermes import provider as provider_module

    root = lexical(tmp_path)
    install_dir = root / "cmms-install"
    fallback_repo_root = root / "unexpected-install-fallback"
    install_dir.mkdir()
    fallback_repo_root.mkdir()
    profiles_root = root / "profiles"
    profiles_root.mkdir()

    # B02 hole H-B: synthetic HOME/HERMES_HOME/TMPDIR, inside the synthetic root.
    # Distinct name from the ``<root>/home`` profile homes the tests build, so
    # the process HOME never collides with a profile home.
    home_dir = root / "synthetic-home"
    hermes_home_dir = home_dir / ".hermes"
    tmp_dir = root / "tmp"
    for pinned_path in (home_dir, hermes_home_dir, tmp_dir):
        pinned_path.mkdir(parents=True, exist_ok=True)

    monkeypatch.chdir(root)

    settings_module.get_settings.cache_clear()
    settings = settings_module.Settings(
        _env_file=None,  # B02 H-B: never read a .env from the CWD/deployment
        vector_backend="lancedb",
        lancedb_path=Path("data/lancedb"),
        graph_snapshot_path=Path("data/graph.json"),
        outbox_compact_interval_seconds=3600,
    )

    injected_modules: tuple[Any, ...] = (settings_module, config_module, provider_module)
    for module in injected_modules:
        monkeypatch.setattr(module, "get_settings", lambda: settings, raising=True)
    for module in (paths_module, config_module, provider_module):
        if hasattr(module, "cmms_repo_root"):
            monkeypatch.setattr(module, "cmms_repo_root", lambda: fallback_repo_root, raising=False)

    _install_mock_embedder(monkeypatch)

    # Pin every storage key this harness can honour to a synthetic value.
    pinned_env = {
        "MEMORY_SERVER_PATH": str(install_dir),
        "MEMORY_SERVER_VECTOR_BACKEND": "lancedb",
        "MEMORY_SERVER_LANCEDB_PATH": "data/lancedb",
        "MEMORY_SERVER_GRAPH_SNAPSHOT_PATH": "data/graph.json",
        # B02 hole H-B: process-level keys, so nothing in-process or in a child
        # process can resolve a live machine path or live Hermes config.
        "HOME": str(home_dir),
        "HERMES_HOME": str(hermes_home_dir),
        "TMPDIR": str(tmp_dir),
    }
    for key, value in pinned_env.items():
        monkeypatch.setenv(key, value)

    env = SyntheticStorageEnv(
        root=root,
        install_dir=install_dir,
        fallback_repo_root=fallback_repo_root,
        settings=settings,
        scrubbed_keys=SCRUBBED_ENV_KEYS,
        present_before_scrub=present_before,
        pinned_env=pinned_env,
        injected_modules=injected_modules,
        home_dir=home_dir,
        hermes_home_dir=hermes_home_dir,
        tmp_dir=tmp_dir,
    )
    for module in injected_modules:
        assert getattr(module, "get_settings")() is settings
    env.assert_injection()
    return env


def describe_environment(env: SyntheticStorageEnv) -> Mapping[str, object]:
    """Redacted description of the synthetic environment (for evidence logs)."""
    return {
        "root": str(env.root),
        "install_dir": str(env.install_dir),
        "cwd": str(lexical(Path.cwd())),
        "injected_modules": list(env.injected_module_names),
        "scrubbed_keys": list(env.scrubbed_keys),
        "keys_present_before_scrub": list(env.present_before_scrub),
        "live_data_roots": [str(lexical(root)) for root in LIVE_DATA_ROOTS],
    }


def assert_no_live_paths(paths: Iterable[str | os.PathLike[str]], *, label: str = "path") -> None:
    """Assert a whole collection of resolved paths is outside the live roots."""
    for path in paths:
        assert_not_live(path, label=label)
