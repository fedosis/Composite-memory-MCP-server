"""Reproducible SLICE01A synthetic SQLite measurements (no HTTP/LLM)."""

import asyncio
import hashlib
import importlib.util
import json
import os
import sqlite3
import statistics
import subprocess
import sys
import time
import types
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import event

from memory_server.providers.sqlite_provider import SQLiteProvider

ROOT = Path("/home/shtorm/.hermes/workspace/gw-slice01a")
OUT = ROOT / "ops/slice01a_evidence"
SCRATCH = Path(os.environ["TMPDIR"]) / "slice01a-measure"
SCRATCH.mkdir(exist_ok=True)
assert os.environ["PYTHONPATH"] == f"{ROOT}/src:{ROOT}"
spec = importlib.util.spec_from_file_location("read_tests", ROOT / "tests/gateway/test_memory_read_indexes.py")
t = importlib.util.module_from_spec(spec)
spec.loader.exec_module(t)
source = subprocess.check_output(
    ["git", "show", "ad50ec3:src/memory_server/providers/sqlite_provider.py"], cwd=ROOT, text=True
)
(OUT / "baseline_sqlite_provider.py.txt").write_text(source)
mod = types.ModuleType("slice01a_baseline_provider")
exec(compile(source, "ad50ec3/sqlite_provider.py", "exec"), mod.__dict__)
Baseline = mod.SQLiteProvider
RESULT = {
    "python": sys.version,
    "sqlite": sqlite3.sqlite_version,
    "seed": "4 x 3000 ORM rows; ties, common IDs, microseconds 0/1/999999; no receipts",
    "notes": "Warm cache; async provider/session; 9 samples, median final 7; synthetic data, not a production SLO.",
}


def save():
    (OUT / "measurements.json").write_text(json.dumps(RESULT, indent=2, default=str))


def clone(src, dst):
    if dst.exists():
        dst.unlink()
    with sqlite3.connect(src) as a, sqlite3.connect(dst) as b:
        a.backup(b)


def snapshot(db):
    with sqlite3.connect(db) as conn:
        rows = {}
        for table in ("facts", "beliefs", "decisions", "skills"):
            data = conn.execute(f"SELECT * FROM {table} ORDER BY id").fetchall()
            rows[table] = {"count": len(data), "sha256": hashlib.sha256(repr(data).encode()).hexdigest()}
        fts = {}
        for table, term in (("facts_fts", "s"), ("beliefs_fts", "prop")):
            data = conn.execute(f"SELECT rowid FROM {table} WHERE {table} MATCH ? ORDER BY rowid", (term,)).fetchall()
            fts[table] = {"count": len(data), "sha256": hashlib.sha256(repr(data).encode()).hexdigest()}
        unique = conn.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='index' AND sql LIKE 'CREATE UNIQUE%' ORDER BY name"
        ).fetchall()
        others = conn.execute(
            "SELECT name,type,sql FROM sqlite_master WHERE name NOT LIKE 'ix_%_created_type_id' "
            "AND name NOT LIKE 'sqlite_autoindex%' AND name NOT LIKE 'alembic_version' ORDER BY name"
        ).fetchall()
        return {
            "rows": rows,
            "fts_match": fts,
            "unique": unique,
            "schema_without_new_indexes_and_stamp": others,
            "integrity": conn.execute("PRAGMA integrity_check").fetchall(),
            "fk_check": conn.execute("PRAGMA foreign_key_check").fetchall(),
        }


async def measure():
    db = SCRATCH / "base12k.db"
    if db.exists():
        db.unlink()
    p = SQLiteProvider(f"sqlite+aiosqlite:///{db}")
    await p.initialize()
    await t._seed_12k(p)
    await p.close()
    RESULT["base_snapshot"] = snapshot(db)
    new = SQLiteProvider(f"sqlite+aiosqlite:///{db}")
    old = Baseline(f"sqlite+aiosqlite:///{db}")
    await new.initialize()
    await old.initialize()
    assert new._read_index_capable
    latencies = {}
    for label, provider in (("before_ad50ec3", old), ("after", new)):
        for limit in (1, 200):
            samples = []
            for _ in range(9):
                started = time.perf_counter()
                page = await t._page(provider, t._query(limit))
                samples.append((time.perf_counter() - started) * 1000)
                assert len(page.records) == limit
            latencies[f"{label}_limit{limit}"] = {"ms": samples, "median_final7_ms": statistics.median(samples[2:])}
    RESULT["page_latency"] = latencies
    save()

    plans = []
    for label, provider in (("before_ad50ec3", old), ("after", new)):
        captured = []

        def trace(conn, cursor, sql, params, context, many):
            if sql.startswith("SELECT") and "ORDER BY" in sql:
                captured.append((sql, params))

        event.listen(provider.engine.sync_engine, "before_cursor_execute", trace)
        for limit in (1, 200):
            for kinds in ([], ["fact"]):
                for filters in (
                    {},
                    dict(created_from=t.BASE, created_to=t.BASE + t.timedelta(days=7)),
                    dict(updated_from=t.BASE, updated_to=t.BASE + t.timedelta(days=7)),
                    dict(lifecycle_states=["active"]),
                    dict(valid_at=t.BASE),
                ):
                    captured.clear()
                    q = t._query(limit, memory_types=kinds, **filters)
                    first = await t._page(provider, q)
                    await t._page(provider, q, after=t._next_after(first), upper=first.upper)
                    await t._page(provider, q, after=t._next_after(first))
                    assert len(captured) == 5
                    for role, (sql, params) in zip(
                        ("first_max", "first_page", "continuation_page", "after_max", "after_page"), captured
                    ):
                        plan = t._eqp_lines(sql, params, db)
                        plans.append(
                            dict(
                                version=label,
                                limit=limit,
                                kinds=kinds or list(t.KINDS),
                                filters=filters,
                                role=role,
                                sql=sql,
                                params=params,
                                eqp=plan,
                            )
                        )
        event.remove(provider.engine.sync_engine, "before_cursor_execute", trace)
    (OUT / "eqp.json").write_text(json.dumps(plans, indent=2, default=str))
    RESULT["eqp_statements"] = len(plans)
    await new.close()
    await old.close()

    caps = {}
    for count in (12000, 200000):
        capdb = SCRATCH / f"cap{count}.db"
        clone(db, capdb)
        if count > 12000:
            # Extend canonical seed rows in SQL; every table has exactly 50k.
            with sqlite3.connect(capdb) as conn:
                for kind in t.KINDS:
                    table = kind + "s"
                    names = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
                    quoted = ",".join('"' + n + '"' for n in names)
                    projection = ",".join(
                        "id || '-extra-' || ?"
                        if n == "id"
                        else "dedup_key || '-extra-' || ?"
                        if n == "dedup_key"
                        else '"' + n + '"'
                        for n in names
                    )
                    binds = 2 if "dedup_key" in names else 1
                    remaining = 47000
                    batch = 0
                    while remaining:
                        amount = min(3000, remaining)
                        conn.execute(
                            f"INSERT INTO {table} ({quoted}) SELECT {projection} "
                            f"FROM {table} ORDER BY id LIMIT {amount}",
                            tuple([str(batch)] * binds),
                        )
                        remaining -= amount
                        batch += 1
                conn.commit()
        samples = []
        for _ in range(3):
            p = SQLiteProvider(f"sqlite+aiosqlite:///{capdb}")
            # Instrument the real startup call separately, still run initialize.
            original = p._check_read_index_capability
            elapsed = []

            async def timed(conn):
                started = time.perf_counter()
                result = await original(conn)
                elapsed.append((time.perf_counter() - started) * 1000)
                return result

            p._check_read_index_capability = timed
            started = time.perf_counter()
            await p.initialize()
            total = (time.perf_counter() - started) * 1000
            assert p._read_index_capable and len(elapsed) == 1
            await p.close()
            samples.append({"capability_ms": elapsed[0], "initialize_ms": total})
        with sqlite3.connect(capdb) as conn:
            actual = {kind: conn.execute(f"SELECT count(*) FROM {kind}s").fetchone()[0] for kind in t.KINDS}
        assert sum(actual.values()) == count
        caps[str(count)] = {
            "rows": actual,
            "samples": samples,
            "median_capability_ms": statistics.median(r["capability_ms"] for r in samples),
            "median_initialize_ms": statistics.median(r["initialize_ms"] for r in samples),
        }
        RESULT["startup"] = caps
        save()

    # Same 12k seed, one new index per mutation table. Disable auto-checkpoint so
    # WAL byte growth is observed before closing the connection. Defaults remain
    # SQLite WAL synchronous=NORMAL; each workload has its own immutable clone.
    writes = []
    for indexed in (False, True):
        for txn in (1, 200):
            for action in ("INSERT", "UPDATE", "DELETE"):
                path = SCRATCH / f"write-{indexed}-{txn}-{action}.db"
                clone(db, path)
                with sqlite3.connect(path) as conn:
                    conn.execute("PRAGMA journal_mode=WAL")
                    conn.execute("PRAGMA synchronous=NORMAL")
                    if not indexed:
                        for name in t.INDEXES.values():
                            conn.execute(f"DROP INDEX {name}")
                    conn.commit()
                    conn.execute("VACUUM")
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    conn.execute("PRAGMA wal_autocheckpoint=0")
                    before_bytes = path.stat().st_size
                    times = []
                    start = time.perf_counter()
                    for i in range(800):
                        kind = t.KINDS[i % 4]
                        table = kind + "s"
                        n = i // 4
                        ident = f"memory-{n:04d}"
                        one = time.perf_counter()
                        if action == "UPDATE":
                            conn.execute(
                                f"UPDATE {table} SET created_at=?,updated_at=? WHERE id=?",
                                ("2025-01-01 00:00:00.000001", "2025-01-02 00:00:00.000001", ident),
                            )
                        elif action == "DELETE":
                            conn.execute(f"DELETE FROM {table} WHERE id=?", (ident,))
                        else:
                            names = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
                            cols = ",".join('"' + n + '"' for n in names)
                            projection = ",".join(
                                "id || '-insert'"
                                if n == "id"
                                else "dedup_key || '-insert'"
                                if n == "dedup_key"
                                else '"' + n + '"'
                                for n in names
                            )
                            conn.execute(
                                f"INSERT INTO {table} ({cols}) SELECT {projection} FROM {table} WHERE id=?", (ident,)
                            )
                        if (i + 1) % txn == 0:
                            conn.commit()
                        times.append((time.perf_counter() - one) * 1000)
                    conn.commit()
                    elapsed = (time.perf_counter() - start) * 1000
                    wal = Path(str(path) + "-wal").stat().st_size
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    after_bytes = path.stat().st_size
                writes.append(
                    dict(
                        indexed=indexed,
                        transaction_size=txn,
                        operation=action,
                        mutations=800,
                        total_ms=elapsed,
                        mean_per_row_ms=elapsed / 800,
                        median_statement_and_commit_ms=statistics.median(times),
                        db_before_bytes=before_bytes,
                        db_after_checkpoint_bytes=after_bytes,
                        wal_before_checkpoint_bytes=wal,
                    )
                )
    RESULT["writes"] = writes
    save()

    mig = SCRATCH / "migration-copy.db"
    clone(db, mig)
    with sqlite3.connect(mig) as conn:
        for name in t.INDEXES.values():
            conn.execute(f"DROP INDEX {name}")
        conn.execute("CREATE TABLE alembic_version (version_num VARCHAR(32) PRIMARY KEY)")
        conn.execute("INSERT INTO alembic_version VALUES ('0005')")
        conn.commit()
    initial = snapshot(mig)
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", f"sqlite:///{mig}")
    steps = []
    for action, target in (("upgrade", "head"), ("upgrade", "head"), ("downgrade", "0005")):
        started = time.perf_counter()
        getattr(command, action)(cfg, target)
        elapsed = (time.perf_counter() - started) * 1000
        snap = snapshot(mig)
        assert snap == initial
        with sqlite3.connect(mig) as conn:
            stamp = conn.execute("SELECT version_num FROM alembic_version").fetchall()
            indexes = conn.execute(
                "SELECT name,sql FROM sqlite_master WHERE name LIKE 'ix_%_created_type_id' ORDER BY name"
            ).fetchall()
            xinfo = {n: conn.execute(f"PRAGMA index_xinfo({n})").fetchall() for n, _ in indexes}
        assert len(indexes) == (0 if action == "downgrade" else 4)
        assert stamp == [("0005" if action == "downgrade" else "0006",)]
        steps.append(
            dict(action=action, target=target, ms=elapsed, stamp=stamp, indexes=indexes, xinfo=xinfo, preservation=snap)
        )
    RESULT["migration"] = steps
    # Last name is occupied: no earlier index may be created by the failed run.
    with sqlite3.connect(mig) as conn:
        conn.execute("CREATE INDEX ix_skills_created_type_id ON skills (id)")
        conn.commit()
    try:
        command.upgrade(cfg, "head")
    except RuntimeError as exc:
        assert "occupied" in str(exc)
        with sqlite3.connect(mig) as conn:
            present = conn.execute("SELECT name FROM sqlite_master WHERE name LIKE 'ix_%_created_type_id'").fetchall()
            assert present == [("ix_skills_created_type_id",)]
            assert conn.execute("SELECT version_num FROM alembic_version").fetchone() == ("0005",)
        RESULT["migration_collision"] = {"error": str(exc), "remaining_indexes": present, "no_partial_DDL": True}
    else:
        raise AssertionError("collision unexpectedly accepted")
    save()
    print(json.dumps(RESULT, indent=2, default=str), flush=True)


if __name__ == "__main__":
    asyncio.run(measure())
