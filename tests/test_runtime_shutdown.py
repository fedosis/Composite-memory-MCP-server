"""S1 coordinator tests; protocol doubles never prove WriterQueue settlement.

Boundary: runtime/runtime.py _cleanup/_wait; SQLite integration additionally
crosses providers/sqlite_provider.py:97-115, close(), real engine transactions.
"""
import asyncio
import concurrent.futures
import subprocess
import sys
import time
from pathlib import Path

import pytest

from memory_server.runtime import RuntimeDependencies
from tests.test_runtime_lifecycle import Clock, Resource, held, pump, runtime, unlocked
from tests.test_runtime_lifecycle import isolated as isolated  # share scratch/config guard


class Hooks:
    """Counts returned by final flush are cumulative; settle is readiness only."""
    def __init__(self):
        self.events, self.result = [], {'flushed': 3, 'failed': 0, 'unsettled': 0}

    def stop_admission(self):
        self.events.append('stop')

    def invalidate_prefetch_generation(self):
        self.events.append('invalidate')

    def pause_writer(self):
        self.events.append('pause')

    async def drain_producers(self, deadline):
        self.events.append(('drain', deadline))

    async def settle_writer(self, deadline):
        self.events.append(('settle', deadline))
        return {'flushed': 2, 'unsettled': 0}

    async def flush_final(self, deadline):
        self.events.append(('flush', deadline))
        return self.result


async def running(path, hooks=None, clock=None, **resources):
    hooks, clock = hooks or Hooks(), clock or Clock()
    r = runtime(path, RuntimeDependencies(hooks=hooks, **resources), clock=clock)
    await r.start()
    return r, hooks, clock


async def test_s1_phase_ceilings_are_absolute_and_scheduling_delay_counts(tmp_path):
    r, h, clock = await running(tmp_path)
    close = asyncio.create_task(r.close())
    await pump()
    report = await close
    assert report.phases['deadlines'] == (110, 115, 125, 130)
    assert [e for e in h.events if isinstance(e, tuple)] == [('drain', 110), ('settle', 125), ('flush', 125)]
    r, h, clock = await running(tmp_path)
    original = h.pause_writer

    def delayed():
        original()
        clock.now += 26

    h.pause_writer = delayed
    report = await r.close()
    assert report.code == 'E_RUNTIME_SHUTDOWN_TIMEOUT' and r.lock is not None
    assert not any(isinstance(e, tuple) and e[0] == 'flush' for e in h.events)
    assert (await r.close()).code is None


async def test_s1_cooperative_drain_stops_at_ceiling_not_sleep(tmp_path):
    r, h, clock = await running(tmp_path)
    gate = asyncio.Event()
    h.drain_producers = lambda deadline: gate.wait()
    waiter = asyncio.create_task(r.close())
    await pump()
    assert not waiter.done() and clock.now == 100
    await clock.advance(10)
    assert (await waiter).code is None and clock.now == 110
    assert ('flush', 125) in h.events


async def test_s1_live_task_retains_lock_and_resources_and_returns_shutdown_timeout(tmp_path):
    resource, gate = Resource('sqlite', []), asyncio.Event()
    r, _, clock = await running(tmp_path, sqlite=resource)

    async def resistant():
        try:
            await gate.wait()
        except asyncio.CancelledError:
            await gate.wait()

    task = asyncio.create_task(resistant())
    executor = concurrent.futures.Future()
    executor.set_running_or_notify_cancel()
    r.register_admitted(task)
    r.register_executor(executor)
    waiter = asyncio.create_task(r.close())
    await pump()
    await clock.advance(10)
    await clock.advance(5)
    report = await waiter
    assert report.code == 'E_RUNTIME_SHUTDOWN_TIMEOUT' and report.unsettled == 2
    assert not resource.closed and not task.done() and not executor.done()
    held(tmp_path)
    gate.set()
    executor.set_result(None)
    await task
    assert (await r.close()).code is None


async def test_s1_terminated_task_exception_is_recorded_not_liveness(tmp_path):
    r, _, _ = await running(tmp_path)

    async def failed():
        raise ValueError('SECRET')

    task = asyncio.create_task(failed())
    r.register_admitted(task)
    await pump()
    report = await r.close()
    assert report.code is None and report.errors == ('ValueError',)
    assert 'SECRET' not in repr(report)


async def test_s1_final_flush_runs_after_drain_with_dependencies_live(tmp_path):
    resource = Resource('sqlite', [])
    r, h, _ = await running(tmp_path, sqlite=resource)

    async def flush(deadline):
        assert not resource.closed and ('drain', 110) in h.events
        held(tmp_path)
        return {'flushed': 3}

    h.flush_final = flush
    report = await r.close()
    assert report.flushed == 3 and resource.closed and not report.lock_retained


@pytest.mark.parametrize('outcome', ['pending', 'unsettled'])
async def test_s1_uncertain_flush_retains_lock_and_denies_reinitialize(tmp_path, outcome):
    resource, gate = Resource('sqlite', []), asyncio.Event()
    r, h, clock = await running(tmp_path, sqlite=resource)
    calls = []

    async def flush(deadline):
        calls.append(deadline)
        if outcome == 'unsettled' and not gate.is_set():
            return {'unsettled': 1}
        await gate.wait()
        return {'flushed': 3}

    h.flush_final = flush
    waiter = asyncio.create_task(r.close())
    await pump()
    await clock.advance(25)
    assert (await waiter).code == 'E_RUNTIME_SHUTDOWN_TIMEOUT'
    assert not resource.closed
    held(tmp_path)
    with pytest.raises(RuntimeError, match='E_RUNTIME_FAILED'):
        await r.start()
    gate.set()
    assert (await r.close()).flushed == 3
    assert calls == ([125] if outcome == 'pending' else [125, 150])
    unlocked(tmp_path)


async def test_s1_close_order_vector_embedder_graph_sqlite_last(tmp_path):
    events = []
    resources = {n: Resource(n, events) for n in ('sqlite', 'graph', 'embedder', 'vector')}
    r, _, _ = await running(tmp_path, **resources)
    assert (await r.close()).code is None
    assert events == ['vector', 'embedder', 'graph', 'sqlite']


async def test_s1_never_calls_outbox_worker_close(tmp_path):
    from storage.outbox_worker import OutboxWorker
    worker = OutboxWorker.__new__(OutboxWorker)
    worker.stop = lambda: None
    worker.close = lambda: pytest.fail('borrowed engine disposed by worker')
    resource = Resource('sqlite', [])
    r, _, _ = await running(tmp_path, outbox_worker=worker, sqlite=resource, borrowed_engine=True)
    assert (await r.close()).code is None and resource.closed


async def test_s1_duplicate_close_returns_cached_result(tmp_path):
    r, h, _ = await running(tmp_path)
    h.result['failed'] = 1
    a, b = await asyncio.gather(r.close(), r.close())
    assert a is b and await r.close() is a and a.code == 'E_RUNTIME_FLUSH_FAILED'
    assert h.events.count(('flush', 125)) == 1


async def test_s1_cleanup_retry_resumes_without_rerunning_successful_steps(tmp_path):
    events, gate = [], asyncio.Event()
    vector, sql = Resource('vector', events), Resource('sqlite', events)

    async def slow_close():
        events.append('sqlite')
        await gate.wait()
        sql.closed = True

    sql.close = slow_close
    r, h, clock = await running(tmp_path, vector=vector, sqlite=sql)
    waiter = asyncio.create_task(r.close())
    await pump()
    await clock.advance(30)
    assert (await waiter).code == 'E_RUNTIME_CLEANUP_FAILED'
    held(tmp_path)
    gate.set()
    assert (await r.close()).code is None
    assert events == ['vector', 'sqlite'] and h.events.count(('flush', 125)) == 1


async def test_s1_terminal_writer_failures_yield_flush_failed_with_counts_only(tmp_path):
    r, h, _ = await running(tmp_path)
    h.result.update(failed=2, requeued=1, failed_records=['SECRET TURN'])
    report = await r.close()
    assert report.code == 'E_RUNTIME_FLUSH_FAILED' and report.failed == 2 and report.requeued == 1
    assert 'SECRET TURN' not in repr(report) and not report.lock_retained
    unlocked(tmp_path)


async def test_s1_external_waiter_cancellation_does_not_cancel_cleanup(tmp_path):
    r, h, _ = await running(tmp_path)
    gate = asyncio.Event()

    async def flush(deadline):
        await gate.wait()
        return {'flushed': 1}

    h.flush_final = flush
    waiter = asyncio.create_task(r.close())
    await pump()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert not r._cleanup_task.cancelled()
    gate.set()
    result = await r.close()
    assert result.flushed == 1 and r.state == 'CLOSED'


async def test_runtime_deadline_uses_monotonic_not_wall_clock(tmp_path, monkeypatch):
    monkeypatch.setattr(time, 'time', lambda: pytest.fail('wall clock used'))
    r, _, clock = await running(tmp_path)
    assert (await r.close()).phases['closing'] == clock.now


@pytest.mark.parametrize('_mode', ['integration'])
def test_s1_no_post_close_commit_and_no_double_engine_dispose(tmp_path, _mode):
    proc = subprocess.run([sys.executable, '-B', __file__, str(tmp_path)], cwd=tmp_path,
                          capture_output=True, text=True, timeout=10)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    print(proc.stdout)
    assert 'SQLITE commits=1 disposals=1 post_close_denied=1' in proc.stdout


async def integration(path):
    from sqlalchemy import event
    from storage.outbox_worker import OutboxWorker

    from memory_server.providers.sqlite_provider import SQLiteProvider
    events = []
    provider = SQLiteProvider(url=f'sqlite+aiosqlite:///{path / "test.db"}')

    async def build(r):
        held(path)
        assert path.resolve().is_relative_to(Path(__import__('os').environ['TMPDIR']).parent)
        print('SQLITE path=' + str(path / 'test.db'))
        await provider.initialize()
        event.listen(provider.engine.sync_engine, 'engine_disposed', lambda e: events.append('dispose'))
        return provider

    h = Hooks()

    async def flush(deadline):
        async with provider.engine.begin() as conn:
            await conn.exec_driver_sql('CREATE TABLE c02 (value INTEGER)')
            await conn.exec_driver_sql('INSERT INTO c02 VALUES (1)')
        events.append('commit')
        return {'flushed': 1}

    h.flush_final = flush
    d = RuntimeDependencies(hooks=h, borrowed_engine=True, recipes=(
        ('sqlite', build), ('outbox_worker', lambda r: OutboxWorker(engine=r.dependencies.sqlite.engine))))
    r = runtime(path, d)
    await r.start()
    assert (await r.close()).code is None
    await r.close()
    with pytest.raises(RuntimeError, match='E_RUNTIME_CLOSING'):
        r.admit()
    import sqlite3
    with sqlite3.connect(path / 'test.db') as conn:
        assert conn.execute('SELECT count(*) FROM c02').fetchone()[0] == 1
    assert events == ['commit', 'dispose']
    unlocked(path)
    print('SQLITE commits=1 disposals=1 post_close_denied=1')


if __name__ == '__main__':
    asyncio.run(integration(Path(sys.argv[1])))
