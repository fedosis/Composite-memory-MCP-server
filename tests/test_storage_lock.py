"""Contract tests for real no-follow directory and root-lock primitives."""

from __future__ import annotations

import fcntl
import importlib
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


# ---------------------------------------------------------------------------
# S2-04 -- lifetime ownership of every lock, the applicable graph lock without
#          inode replacement, and the complete /proc writer inventory
#
# Every node below that references a symbol which does not exist at the card's
# parent commit is a labelled MISSING-CAPABILITY node: it fails at BASE with
# AttributeError naming the absent symbol, never with a collection ImportError,
# because every new import happens inside the test body. The behavioural nodes
# for the same contract live in tests/test_profile_migration.py and drive the
# already-approved public entrypoint surface.
# ---------------------------------------------------------------------------


def _s204_storage_lock():
    return importlib.import_module("memory_server.storage_lock")


def _s204_own_namespace() -> str:
    return os.readlink("/proc/self/ns/mnt")


def _s204_upgraded_holder(root: str, ready, release) -> None:
    lock = RuntimeStorageLock.acquire(Path(root), timeout=3)
    ready.put(os.getpid())
    release.wait(8)
    lock.release()


def _s204_fake_proc(
    proc_root: Path,
    pid: int,
    fds: dict[int, str],
    *,
    namespace: str,
    uid: int,
    readable: bool = True,
) -> Path:
    entry = proc_root / str(pid)
    (entry / "ns").mkdir(parents=True)
    (entry / "fd").mkdir()
    (entry / "status").write_text(f"Name:\tsynthetic\nUid:\t{uid}\t{uid}\t{uid}\t{uid}\n")
    (entry / "ns" / "mnt").symlink_to(namespace)
    for descriptor, label in fds.items():
        (entry / "fd" / str(descriptor)).symlink_to(label)
    if not readable:
        os.chmod(entry / "fd", 0o000)
    return entry


def test_s204_graph_lock_is_held_without_inode_replacement_and_never_unlinked(tmp_path: Path) -> None:
    module = _s204_storage_lock()
    graph = tmp_path / "graph.lock"
    graph.write_bytes(b"")
    inode = os.lstat(graph).st_ino
    locks = module.MaintenanceStorageLocks.acquire([tmp_path], timeout=1, graph_lock_path=graph)
    competing = os.open(graph, os.O_RDWR)
    try:
        assert os.lstat(graph).st_ino == inode
        with pytest.raises(BlockingIOError):
            fcntl.flock(competing, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        os.close(competing)
        locks.release()
    assert os.lstat(graph).st_ino == inode
    assert graph.exists()


def test_s204_release_is_refused_inside_a_critical_section_and_retains_protection(tmp_path: Path) -> None:
    module = _s204_storage_lock()
    lock_path = tmp_path / ".cmms-storage.lock"
    locks = module.MaintenanceStorageLocks.acquire([tmp_path], timeout=1)
    inode = os.lstat(lock_path).st_ino
    locks.begin_critical_section()
    with pytest.raises(StorageLockError) as unsafe:
        locks.release()
    assert unsafe.value.code == "E_LOCK_RELEASE_UNSAFE"
    assert locks.held is True
    with pytest.raises(StorageLockError) as competitor:
        module.MaintenanceStorageLocks.acquire([tmp_path], timeout=0.05)
    assert competitor.value.code == "E_LOCK_TIMEOUT"
    locks.end_critical_section()
    locks.release()
    assert locks.released is True
    locks.release()
    assert os.lstat(lock_path).st_ino == inode
    successor = module.MaintenanceStorageLocks.acquire([tmp_path], timeout=1)
    try:
        assert os.lstat(lock_path).st_ino == inode
    finally:
        successor.release()


def test_s204_proc_inventory_matches_raw_labels_without_following_a_referent(tmp_path: Path) -> None:
    module = _s204_storage_lock()
    proc = tmp_path / "proc"
    referent = tmp_path / "legacy-referent"
    referent.mkdir()
    link = tmp_path / "lancedb"
    link.symlink_to(referent, target_is_directory=True)
    database = tmp_path / "memory.db"
    database.write_bytes(b"db")
    _s204_fake_proc(
        proc,
        4242,
        {3: str(link), 4: str(database), 5: "socket:[1]"},
        namespace=_s204_own_namespace(),
        uid=os.getuid(),
    )
    _s204_fake_proc(
        proc,
        4243,
        {3: "socket:[2]"},
        namespace="mnt:[999999]",
        uid=0,
        readable=False,
    )
    (proc / "locks").write_text("")
    inventory = module.scan_writer_inventory(
        {"legacy_referent": str(link), "source": str(database)},
        proc_root=proc,
        exclude_pids=(os.getpid(),),
    )
    assert inventory.covered is True
    assert inventory.code is None
    raw = {record.raw_label for record in inventory.records}
    assert raw == {str(link), str(database)}
    assert str(referent) not in raw
    assert inventory.excluded_foreign_namespace == 1
    assert inventory.excluded_foreign_credential == 0
    os.chmod(proc / "4243" / "fd", 0o700)


def test_s204_incomplete_same_namespace_coverage_fails_closed(tmp_path: Path) -> None:
    module = _s204_storage_lock()
    proc = tmp_path / "proc"
    _s204_fake_proc(
        proc,
        5150,
        {3: "socket:[9]"},
        namespace=_s204_own_namespace(),
        uid=os.getuid(),
        readable=False,
    )
    (proc / "locks").write_text("")
    inventory = module.scan_writer_inventory(
        {"source": str(tmp_path / "memory.db")}, proc_root=proc, exclude_pids=(os.getpid(),)
    )
    assert inventory.covered is False
    assert inventory.code == "E_WRITER_STATE_UNKNOWN"
    assert inventory.gaps
    assert any("descriptor table of pid 5150" in gap for gap in inventory.gaps)
    os.chmod(proc / "5150" / "fd", 0o700)


def test_s204_missing_proc_root_is_unsupported_and_fails_closed(tmp_path: Path) -> None:
    module = _s204_storage_lock()
    inventory = module.scan_writer_inventory(
        {"source": str(tmp_path / "memory.db")}, proc_root=tmp_path / "absent"
    )
    assert inventory.covered is False
    assert inventory.code == "E_WRITER_INVENTORY_UNSUPPORTED"


def test_s204_real_upgraded_shared_holder_is_reported_and_stays_unsignalled(tmp_path: Path) -> None:
    module = _s204_storage_lock()
    lock_path = tmp_path / ".cmms-storage.lock"
    ready: multiprocessing.Queue[int] = multiprocessing.Queue()
    release = multiprocessing.Event()
    proc = multiprocessing.Process(target=_s204_upgraded_holder, args=(str(tmp_path), ready, release))
    proc.start()
    child = None
    try:
        child = ready.get(timeout=6)
        inventory = module.scan_writer_inventory(
            {"root_lock": str(lock_path)},
            lock_label_keys=("root_lock",),
            proc_root="/proc",
            exclude_pids=(os.getpid(),),
        )
        assert inventory.covered is True
        holders = inventory.upgraded_holders
        assert [record.pid for record in holders] == [child]
        assert holders[0].raw_label == str(lock_path)
        assert inventory.legacy_writers == ()
        table = module.read_lock_table(proc_root="/proc")
        assert any(
            record.pid == child and record.kind == "flock" and record.mode == "read" for record in table
        )
        with pytest.raises(StorageLockError) as timeout:
            module.MaintenanceStorageLocks.acquire([tmp_path], timeout=0.2)
        assert timeout.value.code == "E_LOCK_TIMEOUT"
        assert proc.is_alive()
    finally:
        release.set()
        proc.join(8)
        if proc.is_alive():
            proc.terminate()
    assert proc.exitcode == 0
