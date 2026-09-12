"""Cross-process storage-lock contract tests.

Every test runs through the ``synthetic_storage_env`` fixture (IMPL slice S0):
the deployment environment is scrubbed, the working directory is pinned to the
pytest temporary root and the guard fails the test if any lock root or resolved
path lands inside a live CMMS data root.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from memory_server.storage_lock import (
    MaintenanceStorageLocks,
    RuntimeStorageLock,
    StorageLockError,
    inspect_existing_lock_readonly,
)


def test_shared_runtime_locks_and_exclusive_refusal(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="lock root")

    a = RuntimeStorageLock.acquire(tmp_path)
    b = RuntimeStorageLock.acquire(tmp_path)
    assert inspect_existing_lock_readonly(tmp_path).availability in {"unknown", "held"}
    with pytest.raises(StorageLockError):
        MaintenanceStorageLocks.acquire([tmp_path], timeout=0.02)
    b.release()
    a.release()
    m = MaintenanceStorageLocks.acquire([tmp_path])
    m.release()


def test_multi_root_acquisition_is_canonical(
    tmp_path: Path, synthetic_storage_env
) -> None:
    env = synthetic_storage_env
    env.assert_injection()

    roots = [tmp_path / "b", tmp_path / "a"]
    env.assert_not_live(*roots, label="lock root")
    m = MaintenanceStorageLocks.acquire(roots)
    m.release()


def test_unsafe_lock_entry_rejected(tmp_path: Path, synthetic_storage_env) -> None:
    env = synthetic_storage_env
    env.assert_injection()
    env.assert_not_live(tmp_path, label="lock root")

    target = tmp_path / "target"
    target.mkdir()
    (tmp_path / ".cmms-storage.lock").symlink_to(target)
    with pytest.raises(StorageLockError):
        RuntimeStorageLock.acquire(tmp_path)
