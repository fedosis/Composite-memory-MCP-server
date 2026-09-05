"""Regression tests for standalone-script database target resolution."""

import importlib
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

COMMON = Path(__file__).resolve().parents[1] / "scripts" / "_common.py"


def _load_common(monkeypatch):
    monkeypatch.delenv("MEMORY_SERVER_DB_URL", raising=False)
    sys.modules.pop("_common", None)
    monkeypatch.syspath_prepend(str(COMMON.parent))
    return importlib.import_module("_common")


def test_default_live_checkout_db_is_rejected(monkeypatch):
    common = _load_common(monkeypatch)

    with pytest.raises(RuntimeError, match="refusing live memory-server DB target"):
        common.get_db_url()


def test_explicit_non_live_sqlite_url_is_allowed(monkeypatch, tmp_path):
    common = _load_common(monkeypatch)
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'temporary.db'}"
    monkeypatch.setenv("MEMORY_SERVER_DB_URL", db_url)

    assert common.get_db_url() == db_url


def test_explicit_live_sqlite_url_is_rejected(monkeypatch):
    common = _load_common(monkeypatch)
    monkeypatch.setenv(
        "MEMORY_SERVER_DB_URL",
        "sqlite+aiosqlite:////home/shtorm/memory-server/data/memory.db",
    )

    with pytest.raises(RuntimeError, match="refusing live memory-server DB target"):
        common.get_db_url()


def test_default_db_in_independent_checkout_is_allowed(tmp_path):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    shutil.copy2(COMMON, scripts / "_common.py")
    script = tmp_path / "check_default.py"
    script.write_text(
        "from _common import get_db_url\n"
        "print(get_db_url())\n",
        encoding="utf-8",
    )
    env = {"PATH": os.environ["PATH"], "PYTHONPATH": str(scripts)}

    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == (
        f"sqlite+aiosqlite:///{tmp_path / 'data' / 'memory.db'}"
    )
