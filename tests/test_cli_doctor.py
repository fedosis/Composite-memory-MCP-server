"""Tests for the CMMS data-consolidation doctor check (cli.py)."""

from __future__ import annotations

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
    """Test the doctor scan over a synthetic HERMES_HOME."""

    @pytest.fixture(autouse=True)
    def _no_memory_server_path_env(self, monkeypatch):
        """Keep doctor deterministic: env override tests opt in explicitly."""
        monkeypatch.delenv("MEMORY_SERVER_PATH", raising=False)

    def _write_config(self, home: Path, *, path: str | None, use_cmms: bool = True):
        home.mkdir(parents=True, exist_ok=True)
        if not use_cmms:
            (home / "config.yaml").write_text("model:\n  default: x\n")
            return
        path_line = f"      path: {path}\n" if path is not None else ""
        (home / "config.yaml").write_text(
            "memory:\n"
            "  provider: memory_server\n"
            "  providers:\n"
            "    memory_server:\n"
            "      plugin: memory_server.plugins.hermes.provider.HermesProvider\n"
            "      enabled: true\n"
            f"{path_line}"
        )

    def test_clean_profile_reports_zero(self, tmp_path, capsys):
        repo_root = str(Path(__file__).resolve().parents[1])
        self._write_config(tmp_path, path=repo_root)
        problems = _do_doctor(str(tmp_path), out=lambda s: print(s))
        assert problems == 0
        assert "All CMMS profiles point at shared data root" in capsys.readouterr().out

    def test_missing_path_reports_problem(self, tmp_path, capsys):
        self._write_config(tmp_path, path=None)
        problems = _do_doctor(str(tmp_path), out=lambda s: print(s))
        assert problems == 1
        assert "path is missing" in capsys.readouterr().out

    def test_non_repo_path_reports_problem(self, tmp_path, capsys):
        self._write_config(tmp_path, path="/tmp/not-the-repo")
        problems = _do_doctor(str(tmp_path), out=lambda s: print(s))
        assert problems == 1
        assert "must point at the shared CMMS repo root" in capsys.readouterr().out

    def test_per_profile_data_dir_reports_problem(self, tmp_path, capsys):
        repo_root = str(Path(__file__).resolve().parents[1])
        self._write_config(tmp_path, path=repo_root)
        (tmp_path / "data" / "lancedb").mkdir(parents=True)
        problems = _do_doctor(str(tmp_path), out=lambda s: print(s))
        assert problems == 1
        assert "per-profile data dir exists" in capsys.readouterr().out

    def test_non_cmms_profile_skipped(self, tmp_path, capsys):
        self._write_config(tmp_path, path=None, use_cmms=False)
        problems = _do_doctor(str(tmp_path), out=lambda s: print(s))
        assert problems == 0
        assert "All CMMS profiles point at shared data root" in capsys.readouterr().out

    def test_profile_and_root_both_checked(self, tmp_path, capsys):
        repo_root = str(Path(__file__).resolve().parents[1])
        self._write_config(tmp_path, path=repo_root)
        prof = tmp_path / "profiles" / "travel-agent"
        self._write_config(prof, path=None)  # missing path → problem
        problems = _do_doctor(str(tmp_path), out=lambda s: print(s))
        assert problems == 1
        assert "travel-agent" in capsys.readouterr().out

    # --- W1: env override must be visible, not blind the doctor -------------

    def test_env_override_warns_and_validates_config(self, tmp_path, capsys, monkeypatch):
        """Env overriding config is reported, and the raw config path is
        validated too (env may mask a bad config value)."""
        repo_root = str(Path(__file__).resolve().parents[1])
        monkeypatch.setenv("MEMORY_SERVER_PATH", repo_root)
        self._write_config(tmp_path, path="/tmp/not-the-repo")
        problems = _do_doctor(str(tmp_path), out=lambda s: print(s))
        out = capsys.readouterr().out
        assert problems == 1
        assert "MEMORY_SERVER_PATH env overrides config path" in out
        assert "must point at the shared CMMS repo root" in out

    def test_env_override_bad_env_is_problem(self, tmp_path, capsys, monkeypatch):
        """A bad env value is a real problem even when config is valid."""
        repo_root = str(Path(__file__).resolve().parents[1])
        monkeypatch.setenv("MEMORY_SERVER_PATH", "/tmp/not-the-repo")
        self._write_config(tmp_path, path=repo_root)
        problems = _do_doctor(str(tmp_path), out=lambda s: print(s))
        out = capsys.readouterr().out
        assert problems == 1
        assert "must point at the shared CMMS repo root" in out
        assert "env overrides config path" in out

    def test_env_equal_to_config_is_quiet(self, tmp_path, capsys, monkeypatch):
        """Env matching the config path is not flagged as an override."""
        repo_root = str(Path(__file__).resolve().parents[1])
        monkeypatch.setenv("MEMORY_SERVER_PATH", repo_root)
        self._write_config(tmp_path, path=repo_root)
        problems = _do_doctor(str(tmp_path), out=lambda s: print(s))
        out = capsys.readouterr().out
        assert problems == 0
        assert "All CMMS profiles point at shared data root" in out
        assert "env overrides" not in out


class TestInstallUninstallBackupRestore:
    """CORE-1/2: install backs up ONLY on the first switch; uninstall restores
    the original provider — or removes the key when there was none.

    Everything runs against a temporary HERMES_HOME; the real user Hermes
    config is never touched.
    """

    def _write_config(self, home: Path, *, provider: str | None) -> None:
        home.mkdir(parents=True, exist_ok=True)
        provider_line = (
            f"  provider: {provider}\n" if provider is not None else ""
        )
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
        assert back.read_text().strip() == "openai_memory", (
            "re-install clobbered the original provider backup"
        )

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
        assert back.read_text().strip() == ABSENT_PROVIDER_MARKER, (
            "absence must be recorded with the explicit marker"
        )

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
