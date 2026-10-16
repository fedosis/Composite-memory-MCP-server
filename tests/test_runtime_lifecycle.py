"""C02 lifecycle coordination, not WriterQueue evidence.

Boundaries: runtime/runtime.py start/cleanup; storage_lock.py:310-395 real EX.
The historical server.py:133-140 RED proves blocking acquisition ONLY, not
cancellation settlement. New settlement assertions exercise the retained tasks.
"""
import asyncio
import builtins
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from memory_server.runtime import CMMSRuntime, RuntimeDependencies
from memory_server.storage_lock import RuntimeStorageLock, StorageLockError


class Clock:
    def __init__(self):
        self.now, self.timers = 100.0, []

    def __call__(self):
        return self.now

    async def sleep(self, delay):
        future = asyncio.get_running_loop().create_future()
        self.timers.append((self.now + delay, future))
        await future

    async def advance(self, amount):
        self.now += amount
        for end, future in self.timers:
            if end <= self.now and not future.done():
                future.set_result(None)
        await pump()


async def pump():
    for _ in range(40):
        await asyncio.sleep(0)  # scheduling only; no wall-clock progress


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    scratch = Path(os.environ['TMPDIR']).parent.resolve()
    assert tmp_path.resolve().is_relative_to(scratch)
    for name in ('HOME', 'HERMES_HOME', 'TMPDIR'):
        assert Path(os.environ[name]).resolve().is_relative_to(scratch)
    from memory_server import settings
    monkeypatch.setattr(settings, 'get_settings', lambda: pytest.fail('ambient settings read'))
    original = builtins.open

    def guarded(file, *args, **kwargs):
        if isinstance(file, (str, Path)):
            path = Path(file).resolve()
            assert not ('.hermes' in path.parts and not path.is_relative_to(scratch)), path
        return original(file, *args, **kwargs)

    monkeypatch.setattr(builtins, 'open', guarded)


class Resource:
    def __init__(self, name, events, fail=False):
        self.name, self.events, self.fail, self.closed = name, events, fail, False

    async def close(self):
        self.events.append(self.name)
        if self.fail:
            raise ValueError('close failure')
        self.closed = True


def runtime(path, dependencies=None, **kwargs):
    clock = kwargs.pop('clock', Clock())
    return CMMSRuntime.create(path, dependencies, config={'nested': {'value': 1}},
                              clock=clock, sleep=clock.sleep, **kwargs)


def unlocked(path):
    lock = RuntimeStorageLock.acquire_exclusive(path, timeout=0)
    lock.release_exclusive()


def held(path):
    with pytest.raises(StorageLockError) as caught:
        RuntimeStorageLock.acquire_exclusive(path, timeout=0)
    assert caught.value.code == 'E_LOCK_TIMEOUT'


async def test_create_opens_no_lock_and_no_resources(tmp_path):
    config = {'nested': {'value': 1}}
    r = CMMSRuntime.create(tmp_path, config=config)
    config['nested']['value'] = 2
    assert r.config['nested']['value'] == 1
    with pytest.raises(TypeError):
        r.config['nested']['value'] = 3
    assert r.state == 'NEW' and list(tmp_path.iterdir()) == []
    assert r.runtime_id and r.profile_id


async def test_start_acquires_ex_before_any_constructor(tmp_path):
    calls = []

    def build(r):
        held(tmp_path)
        calls.append('build')
        return Resource('sqlite', calls)

    r = runtime(tmp_path, RuntimeDependencies(recipes=(('sqlite', build),)))
    await r.start()
    assert calls == ['build'] and r.started
    await r.close()
    assert calls == ['build', 'sqlite']
    unlocked(tmp_path)


async def test_start_is_idempotent_and_concurrent_start_joins_one_start(tmp_path):
    gate, calls = asyncio.Event(), []

    async def build(r):
        calls.append('build')
        await gate.wait()
        return Resource('sqlite', calls)

    r = runtime(tmp_path, RuntimeDependencies(recipes=(('sqlite', build),)))
    a, b = asyncio.create_task(r.start()), asyncio.create_task(r.start())
    await pump()
    gate.set()
    assert await a is r and await b is r and await r.start() is r
    assert calls == ['build']
    await r.close()


async def failed_start(path, fail_close=False):
    events, error = [], ValueError('constructor failure')
    resource = Resource('sqlite', events, fail_close)

    def fail(r):
        raise error

    d = RuntimeDependencies(recipes=(('sqlite', lambda r: resource),
                                     ('vector', lambda r: Resource('vector', events)), ('embedder', fail),
                                     ('graph', lambda r: pytest.fail('later recipe'))))
    r = runtime(path, d)
    with pytest.raises(ValueError) as got:
        await r.start()
    assert got.value is error and r.state == 'FAILED'
    return r, error, resource, events


async def test_start_failure_rolls_back_acquired_subset_and_reraises_original(tmp_path):
    r, error, resource, events = await failed_start(tmp_path)
    assert events == ['vector', 'sqlite'] and resource.closed
    assert error.cleanup_report.code is None and r.lock is None


async def test_start_failure_after_lock_acquired_releases_lock_when_provable(tmp_path):
    r, error, _, _ = await failed_start(tmp_path)
    assert not error.cleanup_report.lock_retained
    unlocked(tmp_path)
    r.dependencies = RuntimeDependencies()
    assert await r.start() is r
    await r.close()


async def test_start_cleanup_failure_retains_lock_and_reports_cleanup_failed(tmp_path):
    r, error, resource, _ = await failed_start(tmp_path, True)
    assert error.cleanup_report.code == 'E_RUNTIME_CLEANUP_FAILED'
    assert error.cleanup_report.retained_resources == ('sqlite',)
    held(tmp_path)
    with pytest.raises(RuntimeError, match='E_RUNTIME_FAILED'):
        await r.start()
    resource.fail = False
    assert (await r.close()).code is None
    unlocked(tmp_path)


async def test_close_during_starting_joins_and_never_publishes_ready(tmp_path):
    gate = asyncio.Event()

    async def build(r):
        await gate.wait()
        assert not r.started
        return Resource('sqlite', [])

    r = runtime(tmp_path, RuntimeDependencies(recipes=(('sqlite', build),)))
    starter = asyncio.create_task(r.start())
    await pump()
    closer = asyncio.create_task(r.close())
    await pump()
    gate.set()
    await asyncio.gather(starter, return_exceptions=True)
    assert (await closer).code is None and r.state == 'CLOSED'
    assert r._start_task.done()
    unlocked(tmp_path)


async def test_cancelled_start_settles_and_never_abandons_ex(tmp_path):
    # Real subprocess contention, unlike lifecycle protocol doubles.
    code = ('import sys; from pathlib import Path; '
            'from memory_server.storage_lock import RuntimeStorageLock; '
            'l=RuntimeStorageLock.acquire_exclusive(Path(sys.argv[1])); '
            'print("HELD",flush=True); sys.stdin.readline(); l.release_exclusive()')
    proc = subprocess.Popen([sys.executable, '-B', '-c', code, str(tmp_path)], cwd=tmp_path,
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert proc.stdout.readline().strip() == 'HELD'
        r = runtime(tmp_path)
        start = asyncio.create_task(r.start())
        scheduled = time.monotonic()
        await pump()
        delay = time.monotonic() - scheduled
        print(f'RUNTIME heartbeat_delay_seconds={delay:.6f}')
        assert delay < .05  # baseline observable, not settlement proof
        start.cancel()
        with pytest.raises(asyncio.CancelledError):
            await start
        await pump()
        assert r._cleanup_task is not None
        assert (await r.close()).code is None and r._start_task.done()
    finally:
        proc.communicate('\n', timeout=3)
        assert proc.returncode == 0
    unlocked(tmp_path)


async def test_close_new_is_closed_and_terminal(tmp_path):
    r = runtime(tmp_path)
    first = await r.close()
    assert r.state == 'CLOSED' and await r.close() is first
    with pytest.raises(RuntimeError, match='E_RUNTIME_FAILED'):
        await r.start()


async def test_failed_denies_operations_but_allows_cleanup_retry(tmp_path):
    r, _, resource, _ = await failed_start(tmp_path, True)
    with pytest.raises(RuntimeError, match='E_RUNTIME_FAILED'):
        r.admit()
    resource.fail = False
    assert (await r.close()).code is None
    unlocked(tmp_path)


def test_runtime_package_imports_without_transports_or_optional_backends(tmp_path):
    code = '''import builtins, sys
blocked={'lancedb','numpy','pyarrow','qdrant_client','sentence_transformers','mcp'}
old=builtins.__import__
def imp(name,*a,**k):
 if name.split('.')[0] in blocked or 'plugins' in name or name=='memory_server.server':
  raise AssertionError(name)
 return old(name,*a,**k)
builtins.__import__=imp
import memory_server.runtime
assert 'memory_server.server' not in sys.modules
'''
    subprocess.run([sys.executable, '-B', '-c', code], check=True, cwd=tmp_path, timeout=5)
