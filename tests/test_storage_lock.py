"""Contract tests for real no-follow directory and root-lock primitives."""

from __future__ import annotations

import multiprocessing
import os
import stat
from pathlib import Path

import pytest

from memory_server.storage_lock import (
    MaintenanceStorageLocks,
    RuntimeStorageLock,
    StorageLockError,
    inspect_existing_lock_readonly,
    open_directory_nofollow,
    read_file_nofollow,
)


def _hold_shared(root: str, ready: multiprocessing.Queue, release: multiprocessing.Event) -> None:
    lock = RuntimeStorageLock.acquire(Path(root), timeout=2)
    ready.put("ready")
    release.wait(5)
    lock.release()


def test_shared_locks_coexist_and_exclusive_blocks_then_succeeds(tmp_path: Path, synthetic_storage_env) -> None:
    synthetic_storage_env.assert_injection()
    ready: multiprocessing.Queue[str] = multiprocessing.Queue()
    release = multiprocessing.Event()
    proc = multiprocessing.Process(target=_hold_shared, args=(str(tmp_path), ready, release))
    proc.start()
    try:
        assert ready.get(timeout=3) == "ready"
        local = RuntimeStorageLock.acquire(tmp_path, timeout=0.2)
        assert inspect_existing_lock_readonly(tmp_path).availability == "held"
        local.release()
        with pytest.raises(StorageLockError) as exc:
            MaintenanceStorageLocks.acquire([tmp_path], timeout=0.05)
        assert exc.value.code == "E_LOCK_TIMEOUT"
        release.set()
        proc.join(3)
        assert proc.exitcode == 0
        maintenance = MaintenanceStorageLocks.acquire([tmp_path], timeout=1)
        maintenance.release()
    finally:
        release.set()
        proc.join(3)
        if proc.is_alive():
            proc.terminate()


def test_multi_root_is_canonical_deduplicated_and_released_reverse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, synthetic_storage_env
) -> None:
    synthetic_storage_env.assert_injection()
    calls: list[tuple[str, str]] = []
    import memory_server.storage_lock as module

    original = module._acquire

    def recording(root: Path, mode: str, timeout: float):
        calls.append((str(root), mode))
        return original(root, mode, timeout)

    monkeypatch.setattr(module, "_acquire", recording)
    a, b = tmp_path / "a", tmp_path / "b"
    locks = MaintenanceStorageLocks.acquire([b, a, a, b], timeout=1)
    try:
        expected = sorted({a.absolute(), b.absolute()}, key=lambda p: os.fsencode(str(p)))
        assert [Path(root) for root, _ in calls] == expected
    finally:
        locks.release()


def test_directory_helper_requires_explicit_create_and_sets_private_mode(tmp_path: Path) -> None:
    missing = tmp_path / "nested" / "root"
    with pytest.raises(StorageLockError) as exc:
        with open_directory_nofollow(missing):
            pass
    assert exc.value.code == "E_PATH_ABSENT"
    with open_directory_nofollow(missing, create=True) as fd:
        assert os.fstat(fd).st_mode & 0o777 == 0o700
    assert stat.S_ISDIR(os.lstat(missing).st_mode)


def test_no_follow_rejects_symlink_parent_and_special_lock_entry(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    (tmp_path / "link").symlink_to(target, target_is_directory=True)
    with pytest.raises(StorageLockError) as exc:
        with open_directory_nofollow(tmp_path / "link" / "child", create=True):
            pass
    assert exc.value.code == "E_PATH_SYMLINK_PARENT"

    fifo = tmp_path / ".cmms-storage.lock"
    os.mkfifo(fifo)
    with pytest.raises(StorageLockError) as exc:
        RuntimeStorageLock.acquire(tmp_path, timeout=0.1)
    assert exc.value.code == "E_LOCK_ENTRY_UNSAFE"


def test_lock_hardlink_and_race_are_fail_closed(tmp_path: Path) -> None:
    first = tmp_path / "first"
    first.write_bytes(b"")
    os.link(first, tmp_path / ".cmms-storage.lock")
    with pytest.raises(StorageLockError) as exc:
        RuntimeStorageLock.acquire(tmp_path, timeout=0.1)
    assert exc.value.code == "E_LOCK_ENTRY_UNSAFE"


def test_readonly_inspection_does_not_create_and_metadata_is_not_evidence(tmp_path: Path) -> None:
    assert inspect_existing_lock_readonly(tmp_path).availability == "unknown"
    assert not (tmp_path / ".cmms-storage.lock").exists()
    lock_path = tmp_path / ".cmms-storage.lock"
    lock_path.write_text('{"pid": 999, "mode": "exclusive"}')
    inspection = inspect_existing_lock_readonly(tmp_path)
    assert inspection.availability == "available"
    assert inspection.owner is None


def test_safe_bounded_file_read_rejects_symlink_and_honors_limit(tmp_path: Path) -> None:
    regular = tmp_path / "regular"
    regular.write_bytes(b"0123456789")
    assert read_file_nofollow(regular, max_bytes=4) == b"0123"
    (tmp_path / "link").symlink_to(regular)
    with pytest.raises(StorageLockError) as exc:
        read_file_nofollow(tmp_path / "link", max_bytes=4)
    assert exc.value.code == "E_PATH_SYMLINK_FINAL"


def test_permission_and_unsupported_codes_are_stable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import memory_server.storage_lock as module

    monkeypatch.setattr(module, "_LOCKING_SUPPORTED", False)
    with pytest.raises(StorageLockError) as exc:
        RuntimeStorageLock.acquire(tmp_path, timeout=0.1)
    assert exc.value.code == "E_LOCK_UNSUPPORTED"

    monkeypatch.setattr(module, "_LOCKING_SUPPORTED", True)
    monkeypatch.setattr(module.os, "open", lambda *args, **kwargs: (_ for _ in ()).throw(PermissionError()))
    with pytest.raises(StorageLockError) as exc:
        RuntimeStorageLock.acquire(tmp_path, timeout=0.1)
    assert exc.value.code == "E_LOCK_PERMISSION"


def test_s1_absent_root_lock_is_unknown(tmp_path):
    assert inspect_existing_lock_readonly(tmp_path / "absent").availability == "unknown"


def test_s1_file_helper_refuses_truncating_hardlink_before_mutation(tmp_path):
    from memory_server.storage_lock import open_file_nofollow

    source = tmp_path / "source"
    source.write_bytes(b"preserve")
    hard = tmp_path / "hard"
    hard.hardlink_to(source)
    with pytest.raises(StorageLockError):
        with open_file_nofollow(hard, flags=os.O_RDWR | os.O_TRUNC):
            pass
    assert source.read_bytes() == b"preserve"


def test_s1_directory_swap_is_detected_without_entering_link(tmp_path, monkeypatch):
    import memory_server.storage_lock as module

    before = tmp_path / "before"
    before.mkdir()
    target = tmp_path / "target"
    target.mkdir()
    real_open = os.open

    def swap(name, flags, *args, **kwargs):
        if name == "before" and kwargs.get("dir_fd") is not None:
            before.rename(tmp_path / "saved")
            before.symlink_to(target)
        return real_open(name, flags, *args, **kwargs)

    monkeypatch.setattr(module.os, "open", swap)
    with pytest.raises(StorageLockError):
        with open_directory_nofollow(before / "must-not-exist", create=True):
            pass
    assert list(target.iterdir()) == []
