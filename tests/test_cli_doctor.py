"""Tests for the CMMS data-consolidation doctor check (cli.py)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from memory_server.cli import (
    _collect_profile_homes,
    _do_doctor,
    _profile_data_dirs,
)


class TestCollectProfileHomes:
    """Test discovery of root + per-profile HERMES_HOME dirs."""

    def test_root_only(self, tmp_path):
        homes = _collect_profile_homes(str(tmp_path))
        assert homes == [("default", tmp_path)]

    def test_profiles_are_discovered(self, tmp_path):
        (tmp_path / "profiles" / "invest-agent").mkdir(parents=True)
        (tmp_path / "profiles" / "invest-agent" / "config.yaml").write_text("a: 1\n")
        (tmp_path / "profiles" / "travel-agent").mkdir(parents=True)
        (tmp_path / "profiles" / "travel-agent" / "config.yaml").write_text("a: 1\n")
        # A dir without config.yaml must be skipped
        (tmp_path / "profiles" / "empty").mkdir()

        homes = _collect_profile_homes(str(tmp_path))
        labels = [label for label, _ in homes]
        assert labels == ["default", "invest-agent", "travel-agent"]


class TestProfileDataDirs:
    """Test detection of per-profile fragmentation dirs."""

    def test_no_data_dirs(self, tmp_path):
        assert _profile_data_dirs(tmp_path) == []

    def test_lancedb_detected(self, tmp_path):
        (tmp_path / "data" / "lancedb").mkdir(parents=True)
        found = _profile_data_dirs(tmp_path)
        assert len(found) == 1
        assert str(found[0]).endswith("data/lancedb")

    def test_graph_json_detected(self, tmp_path):
        (tmp_path / "data").mkdir(parents=True)
        (tmp_path / "data" / "graph.json").write_text("{}")
        found = _profile_data_dirs(tmp_path)
        assert len(found) == 1
        assert str(found[0]).endswith("data/graph.json")

    def test_both_detected(self, tmp_path):
        (tmp_path / "data" / "lancedb").mkdir(parents=True)
        (tmp_path / "data" / "graph.json").write_text("{}")
        assert len(_profile_data_dirs(tmp_path)) == 2


class TestDoDoctor:
    """Doctor contract: raw/effective layout diagnostics."""

    @pytest.fixture(autouse=True)
    def clean(self, monkeypatch):
        for n in (
            "MEMORY_SERVER_PATH",
            "MEMORY_SERVER_STORAGE_MODE",
            "MEMORY_SERVER_DATA_ROOT",
            "MEMORY_SERVER_DB_URL",
        ):
            monkeypatch.delenv(n, raising=False)

    def cfg(self, home, text):
        home.mkdir(parents=True, exist_ok=True)
        (home / "config.yaml").write_text(text)

    def test_s5_02c_missing_path_uses_explicit_default_and_is_ok(self, tmp_path):
        """A missing installation path uses the documented default without
        changing profile-owned storage placement or causing a doctor error.
        """
        from memory_server.cli import _doctor_report

        self.cfg(
            tmp_path,
            "memory:\n  providers:\n    memory_server:\n"
            "      plugin: memory_server.plugins.hermes.provider.HermesProvider\n",
        )
        row = _doctor_report(str(tmp_path), env={})["profiles"][0]
        assert row["status"] == "OK"
        assert row["code"] is None
        assert row["canonical_origins"]["installation"] == "default"
        out = []
        assert _do_doctor(str(tmp_path), out=out.append) == 0
        assert out == ["OK default: layout is coherent"]

    def test_s5_02c_non_cmms_profile_is_skipped_cleanly(self, tmp_path):
        """A home without the CMMS provider is not reported as a CMMS
        diagnostic and therefore remains a clean doctor result.
        """
        from memory_server.cli import _doctor_report

        self.cfg(tmp_path, "model:\n  default: x\n")
        report = _doctor_report(str(tmp_path), env={})
        assert report["status"] == "OK"
        assert report["profiles"] == []
        out = []
        assert _do_doctor(str(tmp_path), out=out.append) == 0
        assert out == []

    def test_s402_matrix(self, tmp_path, monkeypatch):
        from memory_server.cli import _doctor_report

        self.cfg(
            tmp_path,
            "memory:\n  providers:\n    memory_server:\n      path: /external/install\n"
            "      storage_mode: profile\n      data_root: .\n",
        )
        assert _doctor_report(str(tmp_path), env={})["profiles"][0]["status"] == "OK"
        monkeypatch.setenv("MEMORY_SERVER_STORAGE_MODE", "shared")
        monkeypatch.setenv("MEMORY_SERVER_DATA_ROOT", str(tmp_path / "shared"))
        row = _doctor_report(str(tmp_path))["profiles"][0]
        assert row["raw"]["storage_mode"] == "profile"
        assert row["effective"]["storage_mode"] == "shared"
        assert row["effective_origins"]["storage_mode"] == "env"

    @pytest.mark.parametrize("mode,root", [("profile", "."), ("shared", "/tmp/shared"), ("standalone", ".")])
    def test_s402_modes(self, tmp_path, mode, root):
        from memory_server.cli import _doctor_report

        self.cfg(
            tmp_path,
            f"memory:\n  providers:\n    memory_server:\n      path: /external/install\n"
            f"      storage_mode: {mode}\n      data_root: {root}\n",
        )
        assert _doctor_report(str(tmp_path), env={})["profiles"][0]["status"] == "OK"

    def test_s402_legacy_warn(self, tmp_path):

        self.cfg(
            tmp_path,
            "memory:\n  providers:\n    memory_server:\n      path: /external/install\n"
            "      storage_mode: profile\n      data_root: .\n"
            "      lancedb_path: /foreign/lancedb\n",
        )
        out = []
        assert _do_doctor(str(tmp_path), out=out.append) == 1
        assert "WARN" in "\n".join(out)

    def test_s402_link_unknown_and_json(self, tmp_path, monkeypatch):
        from memory_server.cli import _doctor_report

        self.cfg(tmp_path, "memory:\n  providers:\n    memory_server:\n      path: /external/install\n")
        (tmp_path / "data").mkdir()
        (tmp_path / "data" / "lancedb").symlink_to(tmp_path / "foreign")
        before = sorted(str(x.relative_to(tmp_path)) for x in tmp_path.rglob("*"))
        row = _doctor_report(str(tmp_path), env={})["profiles"][0]
        after = sorted(str(x.relative_to(tmp_path)) for x in tmp_path.rglob("*"))
        assert before == after
        assert row["status"] == "ERROR"
        assert row["code"] == "E_PROJECTION_UNAVAILABLE"
        assert row["safe_diagnostics"]["vector"]["count"] == "unknown"
        from typer.testing import CliRunner

        monkeypatch.setenv("MEMORY_SERVER_DB_URL", "sqlite+aiosqlite:///x.db?token=SECRET")
        result = CliRunner().invoke(
            __import__("memory_server.cli", fromlist=["app"]).app, ["doctor", "--hermes-home", str(tmp_path), "--json"]
        )
        assert result.exit_code == 1
        payload = json.loads(result.stdout)
        assert set(payload) == {"status", "profiles", "migration_hint"}
        assert "SECRET" not in result.stdout
        assert (
            payload["migration_hint"]
            == "Run `memory-server migrate-profile-storage` to rebuild unavailable projections."
        )


class TestInstallUninstallBackupRestore:
    """CORE-1/2: install backs up ONLY on the first switch; uninstall restores
    the original provider — or removes the key when there was none.

    Everything runs against a temporary HERMES_HOME; the real user Hermes
    config is never touched.
    """

    def _write_config(self, home: Path, *, provider: str | None) -> None:
        home.mkdir(parents=True, exist_ok=True)
        provider_line = f"  provider: {provider}\n" if provider is not None else ""
        (home / "config.yaml").write_text(
            "model:\n"
            "  default: x\n"
            "memory:\n"
            f"{provider_line}"
            "  providers:\n"
            "    other_memory:\n"
            "      plugin: some.other.Provider\n",
            encoding="utf-8",
        )

    def _provider(self, home: Path) -> str | None:
        from memory_server.cli import (
            _config_path,
            _current_memory_provider,
            _load_config,
        )

        data = _load_config(_config_path(str(home)))
        return _current_memory_provider(data)

    def test_original_to_install_to_install_to_uninstall(self, tmp_path):
        """Second install must NOT overwrite the first install's backup —
        uninstall restores the ORIGINAL provider, not 'memory_server'."""
        from memory_server.cli import (
            _backup_path,
            _do_install,
            _do_uninstall,
        )

        home = tmp_path / "hermes-home"
        self._write_config(home, provider="openai_memory")

        # First install: switch away from the original provider.
        out1 = []
        assert _do_install(str(home), False, out=out1.append) == 0
        assert self._provider(home) == "memory_server"
        back = _backup_path(str(home))
        assert back.is_file()
        assert back.read_text().strip() == "openai_memory"

        # Second install (re-install): provider already memory_server —
        # the backup must be left untouched.
        out2 = []
        assert _do_install(str(home), False, out=out2.append) == 0
        assert back.read_text().strip() == "openai_memory", "re-install clobbered the original provider backup"

        # Uninstall restores the ORIGINAL provider from the untouched backup.
        out3 = []
        assert _do_uninstall(str(home), False, out=out3.append) == 0
        assert self._provider(home) == "openai_memory"
        assert not back.exists()
        assert "openai_memory" in "\n".join(out3)

    def test_missing_provider_key_to_install_to_uninstall(self, tmp_path):
        """With no prior memory.provider key, install records an explicit
        absence marker and uninstall removes the key entirely (absence
        restored)."""
        from memory_server.cli import (
            ABSENT_PROVIDER_MARKER,
            _backup_path,
            _do_install,
            _do_uninstall,
        )

        home = tmp_path / "hermes-home-2"
        self._write_config(home, provider=None)
        assert self._provider(home) is None

        out1 = []
        assert _do_install(str(home), False, out=out1.append) == 0
        assert self._provider(home) == "memory_server"
        back = _backup_path(str(home))
        assert back.read_text().strip() == ABSENT_PROVIDER_MARKER, "absence must be recorded with the explicit marker"

        out2 = []
        assert _do_uninstall(str(home), False, out=out2.append) == 0
        # The provider KEY is gone again — absence restored, not 'memory_server'.
        assert self._provider(home) is None
        assert not back.exists()

    def test_dry_run_does_not_write_backup_or_config(self, tmp_path):
        from memory_server.cli import _backup_path, _do_install, _do_uninstall

        home = tmp_path / "hermes-home-3"
        self._write_config(home, provider="original_provider")
        cfg = home / "config.yaml"
        before = cfg.read_bytes()

        out = []
        assert _do_install(str(home), True, out=out.append) == 0
        assert not _backup_path(str(home)).exists()
        assert cfg.read_bytes() == before
        assert self._provider(home) == "original_provider"

        # Dry-run uninstall after a REAL install must preview the restore.
        assert _do_install(str(home), False, out=out.append) == 0
        out2 = []
        assert _do_uninstall(str(home), True, out=out2.append) == 0
        assert self._provider(home) == "memory_server"  # still active (dry)
        assert _backup_path(str(home)).exists()
        assert "original_provider" in "\n".join(out2)

    def test_uninstall_does_not_clobber_manually_changed_provider(self, tmp_path):
        """If the user switched providers after install, uninstall leaves the
        manual choice alone."""
        from memory_server.cli import (
            _config_path,
            _do_install,
            _do_uninstall,
            _load_config,
            _save_config,
        )

        home = tmp_path / "hermes-home-4"
        self._write_config(home, provider="original_provider")
        assert _do_install(str(home), False, out=lambda s: None) == 0

        # Simulate the user switching to a different provider post-install.
        data = _load_config(_config_path(str(home)))
        data["memory"]["provider"] = "manual_choice"
        _save_config(_config_path(str(home)), data)

        out = []
        assert _do_uninstall(str(home), False, out=out.append) == 0
        assert self._provider(home) == "manual_choice"


def _s402_cfg(path="/external/install", mode="profile", root=".", extra=""):
    return (
        "memory:\n  provider: memory_server\n  providers:\n    memory_server:\n"
        "      plugin: memory_server.plugins.hermes.provider.HermesProvider\n"
        f"      path: {path}\n      storage_mode: {mode}\n      data_root: {root}\n{extra}"
    )


def test_s402_raw_effective_matrix_and_origins(tmp_path, monkeypatch):
    from memory_server.cli import _doctor_report

    (tmp_path / "config.yaml").write_text(_s402_cfg())
    raw = _doctor_report(str(tmp_path), env={})["profiles"][0]
    assert raw["status"] == "OK" and raw["raw"]["storage_mode"] == "profile"
    monkeypatch.setenv("MEMORY_SERVER_STORAGE_MODE", "shared")
    monkeypatch.setenv("MEMORY_SERVER_DATA_ROOT", str(tmp_path / "shared"))
    row = _doctor_report(str(tmp_path))["profiles"][0]
    assert row["effective"]["storage_mode"] == "shared"
    assert row["effective_origins"]["storage_mode"] == "env"


@pytest.mark.parametrize("mode,root", [("profile", "."), ("shared", "/tmp/shared"), ("standalone", ".")])
def test_s402_profile_shared_standalone_statuses(tmp_path, mode, root):
    from memory_server.cli import _doctor_report

    (tmp_path / "config.yaml").write_text(_s402_cfg(mode=mode, root=root))
    assert _doctor_report(str(tmp_path), env={})["profiles"][0]["status"] == "OK"


def test_s402_installation_path_is_not_storage_root(tmp_path):
    from memory_server.cli import _doctor_report

    (tmp_path / "config.yaml").write_text(_s402_cfg(path="/not-the-repo"))
    row = _doctor_report(str(tmp_path), env={})["profiles"][0]
    assert row["status"] == "OK" and row["canonical_origins"]["installation"] == "yaml"


def test_s402_legacy_split_warns_nonzero(tmp_path):

    (tmp_path / "config.yaml").write_text(_s402_cfg(extra="      lancedb_path: /foreign/lancedb\n"))
    out = []
    assert _do_doctor(str(tmp_path), out=out.append) == 1
    assert "WARN" in "\n".join(out)


def test_s402_no_link_and_unknown_are_read_only(tmp_path):
    from memory_server.cli import _doctor_report

    (tmp_path / "config.yaml").write_text(_s402_cfg())
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "lancedb").symlink_to(tmp_path / "foreign")
    before = sorted(str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*"))
    row = _doctor_report(str(tmp_path), env={})["profiles"][0]
    after = sorted(str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*"))
    assert before == after
    assert row["status"] == "ERROR" and row["code"] == "E_PROJECTION_UNAVAILABLE"
    assert row["safe_diagnostics"]["vector"]["count"] == "unknown"


def test_s402_json_redaction_and_exact_hint(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from memory_server.cli import app

    (tmp_path / "config.yaml").write_text(_s402_cfg())
    monkeypatch.setenv("MEMORY_SERVER_DB_URL", "sqlite+aiosqlite:///x.db?token=SECRET")
    result = CliRunner().invoke(app, ["doctor", "--hermes-home", str(tmp_path), "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert set(payload) == {"status", "profiles", "migration_hint"}
    assert "SECRET" not in result.stdout
    assert (
        payload["migration_hint"] == "Run `memory-server migrate-profile-storage` to rebuild unavailable projections."
    )
