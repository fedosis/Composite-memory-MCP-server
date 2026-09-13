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

import json
import os
from pathlib import Path

import pytest

from memory_server.paths import (
    StorageLayoutError,
    StorageResolutionInputs,
    ValueOrigin,
    classify_artifact_nofollow,
    inspect_component_chain_nofollow,
    resolve_sqlite_location,
    resolve_storage_layout,
    serialize_layout_redacted,
    validate_write_target,
)
from memory_server.plugins.hermes.provider import HermesProvider, _run_async


def _sqlite_path(provider: HermesProvider) -> Path:
    assert provider._provider is not None
    prefix = "sqlite+aiosqlite:///"
    assert provider._provider._url.startswith(prefix)
    return Path(provider._provider._url[len(prefix) :])


def _contained(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def test_real_provider_keeps_all_store_paths_inside_each_profile(tmp_path: Path, synthetic_storage_env) -> None:
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


# S1 resolver contracts use only synthetic temporary roots.  They deliberately
# exercise lstat-based inspection rather than provider mocks.
def test_s1_nofollow_chain_reports_link_without_following(tmp_path: Path) -> None:
    anchor = tmp_path / "home"
    anchor.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    link = anchor / "link"
    link.symlink_to(outside, target_is_directory=True)
    assert classify_artifact_nofollow(link) == "symlink"
    identities = inspect_component_chain_nofollow(link, anchor=anchor)
    assert identities[-1].kind == "symlink"
    assert identities[-1].raw_link_target == str(outside)
    assert not any(identity.lexical_path == str(outside) for identity in identities)


def test_s1_symlink_parent_is_rejected(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (home / "data").symlink_to(outside, target_is_directory=True)
    with pytest.raises(StorageLayoutError) as error:
        resolve_storage_layout(StorageResolutionInputs(profile_home=home, data_root="data"))
    assert error.value.code == "E_PATH_SYMLINK_PARENT"


def test_s1_profile_home_must_exist_and_be_real_directory(tmp_path: Path) -> None:
    with pytest.raises(StorageLayoutError) as error:
        resolve_storage_layout(StorageResolutionInputs(profile_home=tmp_path / "missing"))
    assert error.value.code == "E_HERMES_HOME_REQUIRED"
    target = tmp_path / "file"
    target.write_text("x")
    with pytest.raises(StorageLayoutError) as error:
        resolve_storage_layout(StorageResolutionInputs(profile_home=target))
    assert error.value.code == "E_HERMES_HOME_REQUIRED"


def test_s1_profile_root_and_external_root_policy(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    layout = resolve_storage_layout(StorageResolutionInputs(profile_home=home, data_root="nested"))
    assert layout.data_root == home / "nested"
    with pytest.raises(StorageLayoutError) as error:
        resolve_storage_layout(StorageResolutionInputs(profile_home=home, data_root=tmp_path / "other"))
    assert error.value.code == "E_PROFILE_ROOT_EXTERNAL"
    shared = tmp_path / "shared"
    shared.mkdir()
    shared_layout = resolve_storage_layout(StorageResolutionInputs(mode="shared", data_root=shared))
    assert shared_layout.mode == "shared"
    with pytest.raises(StorageLayoutError) as error:
        resolve_storage_layout(
            StorageResolutionInputs(mode="shared", data_root=shared, lancedb_path=tmp_path / "split")
        )
    assert error.value.code == "E_SHARED_SPLIT_LAYOUT"


def test_s1_sqlite_forms_preserve_query_and_memory(tmp_path: Path) -> None:
    origin = ValueOrigin("yaml", "db_url")
    relative = resolve_sqlite_location(
        "sqlite+aiosqlite:///data/memory.db?timeout=5", data_root=tmp_path, origin=origin
    )
    assert relative.local_path == tmp_path / "data/memory.db"
    assert relative.effective_url == "sqlite+aiosqlite:///" + str(tmp_path / "data/memory.db") + "?timeout=5"
    absolute = resolve_sqlite_location("sqlite:////var/tmp/memory.db?mode=ro", data_root=tmp_path, origin=origin)
    assert absolute.local_path == Path("/var/tmp/memory.db")
    assert absolute.query == "mode=ro"
    assert resolve_sqlite_location("sqlite+aiosqlite://", data_root=tmp_path, origin=origin).kind == "memory"
    remote = resolve_sqlite_location("postgresql://db/app?password=secret", data_root=tmp_path, origin=origin)
    assert remote.kind == "remote" and remote.effective_url == "postgresql://db/app?password=secret"
    file_uri = resolve_sqlite_location("file:/tmp/existing.db?mode=ro", data_root=tmp_path, origin=origin)
    assert file_uri.kind == "file_uri" and file_uri.effective_url == "file:/tmp/existing.db?mode=ro"


def test_s1_qdrant_profile_fails_closed(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    with pytest.raises(StorageLayoutError) as error:
        resolve_storage_layout(
            StorageResolutionInputs(profile_home=home, vector_backend="qdrant", qdrant_location="http://qdrant:6333")
        )
    assert error.value.code == "E_QDRANT_PROFILE_NAMESPACE_UNDEFINED"
    layout = resolve_storage_layout(
        StorageResolutionInputs(
            mode="shared", data_root=home, vector_backend="qdrant", qdrant_location="http://qdrant:6333"
        )
    )
    assert layout.vector.kind == "remote"


def test_s1_layout_origins_are_snapshot_and_serialization_is_redacted(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    origins = {"sqlite": ValueOrigin("env", "MEMORY_SERVER_DB_URL")}
    layout = resolve_storage_layout(
        StorageResolutionInputs(
            profile_home=home, origins=origins, sqlite_url="sqlite:///data/memory.db?api_key=secret&mode=ro"
        )
    )
    origins["new"] = ValueOrigin("yaml", "new")
    assert "new" not in layout.origins
    report = serialize_layout_redacted(layout)
    assert "secret" not in str(report)
    assert "api_key" in str(report)


def test_s1_write_target_rejects_root_and_symlink(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    layout = resolve_storage_layout(StorageResolutionInputs(profile_home=home))
    with pytest.raises(StorageLayoutError) as error:
        validate_write_target(layout, layout.data_root)
    assert error.value.code == "E_FORBIDDEN_TARGET_ROOT"
    link = home / "link"
    link.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(StorageLayoutError) as error:
        validate_write_target(layout, link)
    assert error.value.code == "E_PATH_FINAL_SYMLINK_UNSAFE"


@pytest.mark.parametrize("mode", ["bogus", "PROFILE", "sharedish"])
def test_s1_invalid_storage_mode_is_rejected(tmp_path: Path, mode: str) -> None:
    with pytest.raises(StorageLayoutError, match="unsupported storage mode") as error:
        resolve_storage_layout(StorageResolutionInputs(mode=mode, data_root=tmp_path))  # type: ignore[arg-type]
    assert error.value.code == "E_STORAGE_MODE_INVALID"


def test_s1_blank_home_and_blank_sqlite_are_distinct_contracts(tmp_path: Path) -> None:
    with pytest.raises(StorageLayoutError) as error:
        resolve_storage_layout(StorageResolutionInputs(profile_home=""))
    assert error.value.code == "E_HERMES_HOME_REQUIRED"
    location = resolve_sqlite_location("", data_root=tmp_path, origin=ValueOrigin("yaml", "db_url"))
    assert location.kind == "memory" and location.local_path is None


def test_s1_sqlite_uri_filename_is_opaque_and_shape_preserved(tmp_path: Path) -> None:
    url = "sqlite+aiosqlite:///file:memory.db?uri=true&cache=shared"
    location = resolve_sqlite_location(url, data_root=tmp_path, origin=ValueOrigin("yaml", "db_url"))
    assert location.kind == "file_uri"
    assert location.local_path is None
    assert location.effective_url == url


def test_s1_shared_root_missing_tail_is_allowed_but_standalone_relative_is_not(tmp_path: Path) -> None:
    shared = tmp_path / "missing" / "tail"
    layout = resolve_storage_layout(StorageResolutionInputs(mode="shared", data_root=shared))
    assert layout.data_root == shared
    with pytest.raises(StorageLayoutError) as error:
        resolve_storage_layout(StorageResolutionInputs(mode="standalone", data_root="relative"))
    assert error.value.code == "E_STANDALONE_ROOT_RELATIVE"


def test_s1_profile_explicit_external_overrides_are_legacy_compatible(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    external = tmp_path / "legacy"
    origin = ValueOrigin("yaml", "legacy_path")
    layout = resolve_storage_layout(
        StorageResolutionInputs(
            profile_home=home,
            lancedb_path=external / "vectors",
            graph_snapshot_path=external / "graph.json",
            sqlite_url=f"sqlite+aiosqlite:////{str(external / 'db.sqlite').lstrip('/')}",
            origins={"vector": origin, "graph": origin, "sqlite": origin},
        )
    )
    assert layout.vector.local_path == external / "vectors"
    assert layout.graph_snapshot_path == external / "graph.json"
    assert "legacy-split-layout" in " ".join(layout.compatibility)
    with pytest.raises(StorageLayoutError) as error:
        resolve_storage_layout(
            StorageResolutionInputs(
                mode="shared", data_root=home, lancedb_path=external / "vectors", origins={"vector": origin}
            )
        )
    assert error.value.code == "E_SHARED_SPLIT_LAYOUT"


def test_s1_sqlite_final_symlink_and_special_are_not_accepted(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    outside = tmp_path / "outside.db"
    outside.write_text("x")
    link = home / "link.db"
    link.symlink_to(outside)
    with pytest.raises(StorageLayoutError) as error:
        resolve_storage_layout(StorageResolutionInputs(profile_home=home, sqlite_url="sqlite+aiosqlite:///link.db"))
    assert error.value.code == "E_PATH_FINAL_SYMLINK_UNSAFE"


def test_s1_redaction_removes_qdrant_userinfo_and_query(tmp_path: Path) -> None:
    layout = resolve_storage_layout(
        StorageResolutionInputs(
            mode="shared",
            data_root=tmp_path,
            vector_backend="qdrant",
            qdrant_location="https://user:pass@qdrant.invalid:6333/api?api_key=secret&tenant=x",
        )
    )
    report = serialize_layout_redacted(layout)
    assert "user" not in str(report) and "pass" not in str(report)
    assert "secret" not in str(report)


def test_s1_write_target_rejects_special_and_hardlink(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    layout = resolve_storage_layout(StorageResolutionInputs(profile_home=home))
    regular = home / "regular"
    regular.write_text("x")
    hard = home / "hard"
    hard.hardlink_to(regular)
    with pytest.raises(StorageLayoutError) as error:
        validate_write_target(layout, hard)
    assert error.value.code == "E_PATH_HARDLINK_UNSAFE"


@pytest.mark.parametrize(
    "name",
    [
        "data/memory.db-wal",
        "data/memory.db-shm",
        "data/memory.db-journal",
        "data/graph.json.lock",
        ".cmms-storage.lock",
    ],
)
def test_s1_parent_rejects_unsafe_sidecar_before_runtime_io(tmp_path, name):
    home = tmp_path / "profile"
    (home / "data").mkdir(parents=True)
    outside = tmp_path / "external"
    outside.write_bytes(b"sentinel")
    (home / name).symlink_to(outside)
    with pytest.raises(StorageLayoutError):
        resolve_storage_layout(StorageResolutionInputs(profile_home=home))
    assert outside.read_bytes() == b"sentinel"


@pytest.mark.parametrize("url", ["sqlite+aiosqlite:///../escape.db", "sqlite:///../../escape.db"])
def test_s1_parent_relative_sql_escape_is_never_legacy(tmp_path, url):
    with pytest.raises(StorageLayoutError):
        resolve_storage_layout(
            StorageResolutionInputs(
                profile_home=tmp_path, sqlite_url=url, origins={"sqlite": ValueOrigin("yaml", "db_url")}
            )
        )


def test_s1_parent_external_sql_symlink_is_fatal(tmp_path):
    home = tmp_path / "profile"
    home.mkdir()
    target = tmp_path / "target.db"
    target.write_bytes(b"sentinel")
    link = tmp_path / "legacy.db"
    link.symlink_to(target)
    with pytest.raises(StorageLayoutError):
        resolve_storage_layout(
            StorageResolutionInputs(
                profile_home=home,
                sqlite_url=f"sqlite+aiosqlite:///{link}",
                origins={"sqlite": ValueOrigin("yaml", "db_url")},
            )
        )


def test_s1_parent_regular_absolute_overrides_warn_without_disabling(tmp_path):
    home = tmp_path / "profile"
    home.mkdir()
    layout = resolve_storage_layout(
        StorageResolutionInputs(
            profile_home=home,
            sqlite_url=f"sqlite+aiosqlite:///{tmp_path / 'db'}",
            lancedb_path=tmp_path / "vectors",
            graph_snapshot_path=tmp_path / "graph",
            origins={key: ValueOrigin("yaml", key) for key in ("sqlite", "vector", "graph")},
        )
    )
    assert set(layout.compatibility) == {
        "legacy-split-layout:sqlite",
        "legacy-split-layout:vector",
        "legacy-split-layout:graph",
    }
    assert layout.unavailable_projections == frozenset()


def test_s1_parent_classification_does_not_follow_parent(tmp_path):
    external = tmp_path / "external"
    external.mkdir()
    (external / "file").write_text("sentinel")
    link = tmp_path / "link"
    link.symlink_to(external)
    with pytest.raises(StorageLayoutError):
        classify_artifact_nofollow(link / "file")


def test_s1_parent_runtime_relative_root_uses_one_layout(synthetic_storage_env, monkeypatch):
    from memory_server.plugins.hermes import provider as module

    home = synthetic_storage_env.root / "home"
    home.mkdir()
    provider = HermesProvider()
    try:
        provider.initialize("s1-root", hermes_home=str(home), config={"data_root": "nested"})
        layout = provider._storage_layout
        assert layout.sqlite.local_path == home / "nested/data/memory.db"
        synthetic_storage_env.assert_provider_synthetic(provider, profile_home=home)

        def no_settings():
            raise AssertionError("late settings read after layout frozen")

        monkeypatch.setattr(module, "get_settings", no_settings)
        assert module._resolve_cmms_data_path(provider, "data/graph.json") == layout.graph_snapshot_path
        assert module._resolve_cmms_data_path(provider, "data/lancedb") == layout.vector.local_path
        with pytest.raises(ValueError):
            module._resolve_cmms_data_path(provider, "../escape")
    finally:
        provider.shutdown()


@pytest.mark.parametrize("projection", ["vector", "graph"])
def test_s1_parent_final_link_degraded_startup_preserves_entry(synthetic_storage_env, projection):
    from memory_server.plugins.hermes import provider as module

    home = synthetic_storage_env.root / "home"
    (home / "data").mkdir(parents=True)
    external = synthetic_storage_env.root / "external"
    external.mkdir()
    sentinel = external / "sentinel"
    sentinel.write_bytes(b"do-not-touch")
    link = home / ("data/lancedb" if projection == "vector" else "data/graph.json")
    link.symlink_to(external)
    before = (link.lstat().st_ino, os.readlink(link), sentinel.read_bytes(), sorted(external.iterdir()))
    provider = HermesProvider()
    try:
        provider.initialize("s1-link", hermes_home=str(home))
        synthetic_storage_env.assert_provider_synthetic(provider, profile_home=home)
        assert provider._outbox_task is None and provider._outbox_worker is None
        with pytest.raises(RuntimeError, match="E_PROJECTION_UNAVAILABLE"):
            module._run_async(
                module._get_vector_provider(provider) if projection == "vector" else module._get_graph(provider)
            )
        result = provider.handle_tool_call("remember", {"subject": "s1", "predicate": "owns", "object": projection})
        assert "error" not in result.lower(), result

        async def pending():
            from sqlalchemy import text

            async with provider._provider.engine.connect() as conn:
                return (await conn.execute(text("SELECT status FROM outbox_entries"))).scalars().all()

        assert module._run_async(pending()) == ["pending"]
    finally:
        provider.shutdown()
    assert before == (link.lstat().st_ino, os.readlink(link), sentinel.read_bytes(), sorted(external.iterdir()))


# --------------------------------------------------------------------------- #
# B02 adversarial guards (holes H-A / H-B from the B01 gate evidence)
# --------------------------------------------------------------------------- #


@pytest.fixture
def cwd_decoy_live_graph(tmp_path: Path, monkeypatch):
    """CWD that *looks* like the deployment checkout (``data/graph.json``).

    H-A: ``graph_test_isolation`` used to resolve ``Path("data/graph.json")``
    relative to the process working directory and ``read_bytes()`` it. When
    pytest is started from the deployment checkout that path IS the live store.
    This fixture reproduces the shape with a synthetic decoy and installs a
    tripwire that fails the test if the decoy is ever read.
    """
    decoy_cwd = tmp_path / "deployment-like"
    decoy_graph = decoy_cwd / "data" / "graph.json"
    decoy_graph.parent.mkdir(parents=True)
    decoy_graph.write_bytes(b"decoy-live-graph-not-a-store")

    monkeypatch.chdir(decoy_cwd)

    real_read_bytes = Path.read_bytes
    decoy_lexical = str(decoy_graph.resolve())

    def guarded_read_bytes(self: Path) -> bytes:
        if str(self.resolve()) == decoy_lexical:
            raise AssertionError(
                "graph fixture read a CWD-relative live-store candidate: "
                f"{decoy_lexical}"
            )
        return real_read_bytes(self)

    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)
    return decoy_graph


def test_graph_test_isolation_never_reads_a_cwd_relative_store(
    cwd_decoy_live_graph, graph_test_isolation
) -> None:
    """B02 / H-A: the graph fixture must be CWD-independent and fail closed."""
    resolved = Path(str(graph_test_isolation)).resolve()
    assert resolved.name == "graph.json"
    assert not str(resolved).startswith(str(cwd_decoy_live_graph.parent.resolve())), (
        f"snapshot escaped the pytest temporary root: {resolved}"
    )
    assert resolved != cwd_decoy_live_graph.resolve()


def test_synthetic_harness_pins_home_hermes_home_and_tmpdir(synthetic_storage_env) -> None:
    """B02 / H-B: HOME, HERMES_HOME and TMPDIR must be pinned synthetically."""
    from tests.synthetic_storage_env import lexical

    env = synthetic_storage_env
    env.assert_injection()
    root = lexical(env.root)
    pinned = {}
    for key in ("HOME", "HERMES_HOME", "TMPDIR"):
        raw = os.environ.get(key)
        assert raw, f"{key} is not pinned by the synthetic harness"
        resolved = lexical(raw)
        assert resolved == root or root in resolved.parents, (
            f"{key}={resolved} is outside the synthetic root {root}"
        )
        pinned[key] = resolved
    assert lexical("~").resolve() == pinned["HOME"].resolve()
    assert str(pinned["HOME"]) != "/home/shtorm"
    assert str(pinned["HERMES_HOME"]) != "/home/shtorm/.hermes"


def test_synthetic_harness_child_process_inherits_only_synthetic_home(synthetic_storage_env) -> None:
    """B02 / H-B: a child process must not see the real HOME/HERMES_HOME/TMPDIR."""
    import subprocess
    import sys

    script = (
        "import os;print('|'.join(str(os.environ.get(k)) for k in "
        "('HOME', 'HERMES_HOME', 'TMPDIR')))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    home, hermes_home, tmpdir = proc.stdout.strip().split("|")
    root = str(synthetic_storage_env.root.resolve())
    for key, value in (("HOME", home), ("HERMES_HOME", hermes_home), ("TMPDIR", tmpdir)):
        assert value and value != "None", f"{key} missing in the child environment"
        assert str(Path(value).resolve()).startswith(root), (
            f"child {key}={value} is outside the synthetic root {root}"
        )


@pytest.mark.parametrize(
    ("tool_name", "args"),
    [
        ("semantic_search", {"query": "missing"}),
        ("graph_search", {"query": "missing"}),
        ("route", {"query": "missing"}),
        ("audit", {}),
    ],
)
def test_projection_tools_return_stable_migration_error(synthetic_storage_env, tool_name, args):
    """Projection-dependent provider tools never turn degradation into success."""
    home = synthetic_storage_env.root / "home"
    (home / "data").mkdir(parents=True)
    external = synthetic_storage_env.root / "external"
    external.mkdir()
    unavailable = {"graph"} if tool_name == "graph_search" else {"vector"}
    if tool_name == "audit":
        unavailable = {"vector", "graph"}
    for projection in unavailable:
        link = home / ("data/lancedb" if projection == "vector" else "data/graph.json")
        link.symlink_to(external, target_is_directory=True)
    provider = HermesProvider()
    try:
        provider.initialize("s3-07-tools", hermes_home=str(home))
        result = json.loads(provider.handle_tool_call(tool_name, args))
        assert result["error"] == "E_PROJECTION_UNAVAILABLE"
        assert result["hint"] == "Run `memory-server migrate-profile-storage` to rebuild unavailable projections."
        assert result["message"].startswith("E_PROJECTION_UNAVAILABLE:")
        assert result not in (None, [], "")
    finally:
        provider.shutdown()


def test_degraded_provider_write_commits_sql_and_pending_outbox(synthetic_storage_env):
    """The real remember path remains durable while projection workers are absent."""
    home = synthetic_storage_env.root / "home"
    (home / "data").mkdir(parents=True)
    external = synthetic_storage_env.root / "external"
    external.mkdir()
    (home / "data/graph.json").symlink_to(external, target_is_directory=True)
    provider = HermesProvider()
    try:
        provider.initialize("s3-07-write", hermes_home=str(home))
        result = json.loads(provider.handle_tool_call("remember", {
            "subject": "degraded", "predicate": "writes", "object": "safely",
        }))
        assert result["fact"]["subject"] == "degraded"

        async def inspect_rows():
            from sqlalchemy import text
            async with provider._provider.engine.connect() as conn:
                facts = (await conn.execute(text("SELECT subject FROM facts"))).scalars().all()
                statuses = (await conn.execute(text("SELECT status FROM outbox_entries"))).scalars().all()
                return facts, statuses

        facts, statuses = _run_async(inspect_rows())
        assert facts == ["degraded"]
        assert statuses == ["pending"]
        assert provider._outbox_worker is None and provider._outbox_task is None
    finally:
        provider.shutdown()
