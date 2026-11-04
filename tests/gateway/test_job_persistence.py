"""S18-P disk-backed seam/schema tests; predicate SQL is not the future H01 service."""

from __future__ import annotations

import importlib.util
import sqlite3
import stat
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier

import pytest

from memory_server.control.store import ControlError, ControlStore
from memory_server.gateway_contracts.ports import ProfileRecord

TS = "2026-01-01T00:00:00.000000Z"
JOB = (
    "INSERT INTO jobs (id,profile_id,subject,token_id,operation,created_at,updated_at,"
    "request_id,dto_snapshot,validated_request) VALUES (?,?,?,?,?,?,?,?,?,?)"
)
REQUEST = (
    "INSERT INTO job_requests (subject,profile_id,route_operation,key,job_id,digest,accepted_snapshot,"
    "request_id,ticket_id,created_at,updated_at,expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
)


def job(tx, key="a", **changes):
    values = dict(
        id=key * 48,
        profile_id="p",
        subject="s",
        token_id="t",
        operation="outbox_replay",
        created_at=TS,
        updated_at=TS,
        request_id="r",
        dto_snapshot='{"original":true}',
        validated_request="{}",
    )
    values.update(changes)
    return tx.execute(JOB, tuple(values.values()))


def request(tx, key="k", job_key="a", **changes):
    values = dict(
        subject="s",
        profile_id="p",
        route_operation="outbox.replay",
        key=key,
        job_id=job_key * 48,
        digest="d" * 64,
        accepted_snapshot='{"accepted":true}',
        request_id="r",
        ticket_id="ticket",
        created_at=TS,
        updated_at=TS,
        expires_at="2026-01-02T00:00:00.000000Z",
    )
    values.update(changes)
    return tx.execute(REQUEST, tuple(values.values()))


@pytest.fixture
def store(tmp_path):
    with ControlStore(tmp_path / "control") as owner:
        owner._configure((ProfileRecord("p", tmp_path / "profile", True, "Unicode ' Я", "", {}, 1),))
        yield owner


def storage_error():
    return pytest.raises(ControlError, match="^Control storage unavailable$")


def test_s18_borrowed_transaction_atomicity(store):
    assert callable(getattr(store, "borrow_transaction", None)), "S18P_SEAM: public borrow seam required"
    with store.borrow_transaction() as tx:
        assert job(tx) == 1, "S18P_ATOMIC: insert rowcount"
        request(tx)
        detached = tx.fetchone("SELECT * FROM jobs WHERE id=?", ("a" * 48,))
        assert detached["id"] == "a" * 48
        assert tx.fetchone("SELECT id FROM jobs WHERE id=?", ("b" * 48,)) is None
        assert isinstance(tx.fetchmany("SELECT id FROM jobs LIMIT ?", (1,), limit=1), tuple)
        with pytest.raises(TypeError):
            detached["id"] = "changed"
        assert not any(hasattr(tx, name) for name in ("commit", "rollback", "cursor", "connection"))

    class Cancel(BaseException):
        pass

    with pytest.raises(Cancel), store.borrow_transaction() as tx:
        job(tx, "b")
        raise Cancel()
    with storage_error(), store.borrow_transaction() as tx:
        job(tx, "b")
        with storage_error():
            request(tx, key="bad", job_key="c")
    with store.borrow_transaction() as tx:
        assert tx.fetchone("SELECT count(*) AS n FROM jobs")["n"] == 1, "S18P_ATOMIC: poisoned rollback"
    root = store.root
    store.close()
    with ControlStore(root) as reopened, reopened.borrow_transaction() as tx:
        assert tx.fetchone("SELECT dto_snapshot FROM jobs")["dto_snapshot"] == detached["dto_snapshot"]
        assert tx.fetchone("SELECT ticket_id FROM job_requests")["ticket_id"] == "ticket"


def test_s18_borrowed_transaction_lifetime(store):
    context = store.borrow_transaction()
    with context as tx:
        job(tx)
        with storage_error():
            context.__enter__()
        with storage_error(), store.borrow_transaction():
            pass
        with ThreadPoolExecutor(1) as pool:

            def foreign():
                with storage_error():
                    tx.fetchone("SELECT 1")

            pool.submit(foreign).result()
        assert tx.fetchone("SELECT count(*) AS n FROM jobs")["n"] == 1, "S18P_SEAM: outer owner preserved"
    with storage_error():
        tx.fetchone("SELECT 1")
    with storage_error():
        context.__enter__()
    with store._transaction():
        with storage_error(), store.borrow_transaction():
            pass
    forbidden = (
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT x",
        "PRAGMA user_version",
        "CREATE TABLE x(a)",
        "ATTACH ':memory:' AS x",
        "WITH x AS (SELECT 1) SELECT * FROM x",
        "-- comment\nSELECT 1",
        "/* comment */ SELECT 1",
        "SELECT 1; SELECT 2",
        "SELECT 1; -- comment",
    )
    for sql in forbidden:
        with storage_error(), store.borrow_transaction() as tx:
            tx.fetchone(sql)
    for sql in ("SELECT 1", "UPDATE jobs SET revision=1 RETURNING id", "INSERT INTO jobs DEFAULT VALUES;"):
        with storage_error(), store.borrow_transaction() as tx:
            tx.execute(sql)
    for limit in (True, 0, -1, 1001, 1.0):
        with storage_error(), store.borrow_transaction() as tx:
            tx.fetchmany("SELECT 1 LIMIT ?", (1,), limit=limit)
    store.close()
    with storage_error(), store.borrow_transaction():
        pass


def test_s18_job_schema_constraints(store):
    with sqlite3.connect(store.root / "control.sqlite") as disk:
        names = {r[0] for r in disk.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"jobs", "job_requests"} <= names, "S18P_DDL: jobs tables required"
        assert disk.execute("PRAGMA user_version").fetchone()[0] == 1, "S18P_V1_CREATE"
        assert disk.execute("PRAGMA foreign_key_list(job_requests)").fetchall()
        columns = disk.execute("PRAGMA table_info(jobs)").fetchall()
        assert next(r[4] for r in columns if r[1] == "recovery_metadata") == "'{\"v\":1}'"
        assert all(r[2] == "TEXT" for r in columns if r[1].endswith("_at"))
        assert [r[2] for r in disk.execute("PRAGMA index_xinfo(job_requests_expiry)") if r[5]] == [
            "phase",
            "pending_outcome",
            "expires_at",
            "job_id",
        ]
    with store.borrow_transaction() as tx:
        job(tx)
        request(tx)
        assert tx.fetchone("SELECT recovery_metadata FROM jobs")["recovery_metadata"] == '{"v":1}'
    for change in (
        {"id": "A" * 48},
        {"profile_id": "missing"},
        {"subject": ""},
        {"token_id": ""},
        {"operation": "bad"},
        {"created_at": 1767225600},
        {"updated_at": "2025-12-31T00:00:00.000000Z"},
        {"created_at": "2026-01-01T00:00:00Z"},
        {"created_at": "2026-01-01T00:00:00.000000+00:00"},
    ):
        with storage_error(), store.borrow_transaction() as tx:
            job(tx, "b", **change)
    for assignment in (
        "status='bad'",
        "revision=-1",
        "progress=1.1",
        "slot_held=0",
        "phase='claimed'",
        "status='running'",
        "started_at='2025-12-31T00:00:00.000000Z'",
    ):
        with storage_error(), store.borrow_transaction() as tx:
            tx.execute("UPDATE jobs SET " + assignment)
    for change in ({"digest": "D" * 64}, {"subject": "other"}, {"expires_at": TS}, {"route_operation": "bad"}):
        with storage_error(), store.borrow_transaction() as tx:
            request(tx, key="other", **change)
    with store.borrow_transaction() as tx:
        tx.execute("UPDATE jobs SET status='interrupted',phase='reconcile',slot_held=0,finished_at=?", (TS,))
        assert tx.fetchone("SELECT started_at FROM jobs")["started_at"] is None
    assert stat.S_IMODE(store.root.stat().st_mode) == 0o700
    assert all(stat.S_IMODE(p.stat().st_mode) == 0o600 for p in store.root.iterdir())


def test_s18_reservation_predicates(store):
    with store.borrow_transaction() as tx:
        job(tx)
        request(tx)
    for _ in range(10):
        with store.borrow_transaction() as tx:
            row = tx.fetchone(
                "SELECT * FROM job_requests WHERE subject=? AND profile_id=? AND route_operation=? AND key=? LIMIT 1",
                ("s", "p", "outbox.replay", "k"),
            )
            assert row["digest"] == "d" * 64 and row["accepted_snapshot"] == '{"accepted":true}', "S18P_ATOMIC"
    with storage_error(), store.borrow_transaction() as tx:
        request(tx, digest="e" * 64)
    with store.borrow_transaction() as tx:
        job(tx, "b")
        request(tx, "another", "b")  # Same digest, different key is legal.
        assert tx.fetchone("SELECT sum(slot_held) AS n FROM jobs")["n"] == 2


def test_s18_claim_cas_predicates(store):
    with store.borrow_transaction() as tx:
        job(tx)
        job(tx, "b")
    barrier = Barrier(2)

    def attempt():
        with ControlStore(store.root) as owner:
            barrier.wait(timeout=10)
            with owner.borrow_transaction() as tx:
                return tx.execute(
                    "UPDATE jobs SET status='running',phase='claimed',started_at=?,revision=revision+1 "
                    "WHERE id=? AND subject=? AND profile_id=? AND status='queued' AND revision=?",
                    (TS, "a" * 48, "s", "p", 0),
                )

    with ThreadPoolExecutor(2) as pool:
        futures = [pool.submit(attempt) for _ in range(2)]
        assert sorted(f.result(timeout=20) for f in futures) == [0, 1], "S18P_CAS: single winner"
    with storage_error(), store.borrow_transaction() as tx:
        tx.execute("UPDATE jobs SET status='running',phase='claimed',started_at=? WHERE id=?", (TS, "b" * 48))
    with store.borrow_transaction() as tx:
        tx.execute(
            "UPDATE jobs SET status='succeeded',phase='terminal',slot_held=0,finished_at=?,revision=revision+1 "
            "WHERE id=? AND status='running' AND revision=1",
            (TS, "a" * 48),
        )
        assert (
            tx.execute(
                "UPDATE jobs SET revision=revision+1 WHERE id=? AND status='running' AND revision=1",
                ("a" * 48,),
            )
            == 0
        ), "S18P_CAS: stale terminal predicate"


def test_s18_retention_predicates(store):
    with store.borrow_transaction() as tx:
        job(tx)
        request(tx)
        tx.execute("UPDATE jobs SET status='succeeded',phase='terminal',slot_held=0,finished_at=?", (TS,))
    predicate = (
        "SELECT key FROM job_requests WHERE phase='final' AND pending_outcome=0 AND expires_at<=? COLLATE BINARY "
        "AND job_id IN (SELECT id FROM jobs WHERE status IN ('succeeded','failed','interrupted')) "
        "ORDER BY expires_at COLLATE BINARY,job_id LIMIT ?"
    )
    for expiry in ("2026-01-02T00:00:00.000000Z", "2026-02-01T00:00:00.000000Z", "2027-01-01T00:00:00.000000Z"):
        instant = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
        with store.borrow_transaction() as tx:
            tx.execute("UPDATE job_requests SET expires_at=?", (expiry,))
            assert not tx.fetchmany(predicate, (expiry, 100), limit=100), "S18P_RETENTION: pending retained"
            tx.execute("UPDATE job_requests SET phase='final',outcome_snapshot='{}',pending_outcome=0")
            for offset in (-1, 0, 1):
                now = instant + timedelta(microseconds=offset)
                text = now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                assert bool(tx.fetchmany(predicate, (text, 100), limit=100)) == (now >= instant), "S18P_RETENTION"
            tx.execute("UPDATE job_requests SET pending_outcome=1")
    with store.borrow_transaction() as tx:
        tx.execute("UPDATE job_requests SET pending_outcome=0")
        assert tx.execute("DELETE FROM job_requests WHERE key=?", ("k",)) == 1
        assert tx.fetchone("SELECT count(*) AS n FROM jobs")["n"] == 1, "S18P_RETENTION: job survives"


@pytest.mark.parametrize("revision", ["2c99972af9839aadc81efadc21d1ada738e0a627", "6776be6"])
def test_s18_control_jobs_migration_rehearsal(tmp_path, revision):
    repo = Path(__file__).resolve().parents[2]
    source = subprocess.run(
        ["git", "show", revision + ":src/memory_server/control/store.py"],
        cwd=repo,
        check=True,
        capture_output=True,
    ).stdout
    frozen = tmp_path / "frozen.py"
    frozen.write_bytes(source)
    spec = importlib.util.spec_from_file_location("s18_frozen", frozen)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    root = tmp_path / "legacy"
    with module.ControlStore(root) as old:
        old._configure((ProfileRecord("p", tmp_path / "profile", True, "Unicode ' Я", "", {}, 1),))
        old._mutate("p", expected_generation=1, operation="metadata", display_name="changed Я", description="")

    def legacy_rows():
        with sqlite3.connect(root / "control.sqlite") as disk:
            return tuple(disk.execute("SELECT * FROM " + name).fetchall() for name in ("registry_state", "profiles"))

    before = legacy_rows()
    with ControlStore(root) as owner, owner.borrow_transaction() as tx:
        job(tx)
        request(tx)
    with module.ControlStore(root) as old:
        assert old._get("p").display_name == "changed Я", "S18P_REHEARSAL: frozen owner profile read"
    with ControlStore(root) as owner, owner.borrow_transaction() as tx:
        assert tx.fetchone("SELECT ticket_id FROM job_requests")["ticket_id"] == "ticket", "S18P_REHEARSAL"
    assert legacy_rows() == before, "S18P_REHEARSAL: legacy values preserved"


@pytest.mark.parametrize("malformation", ["table", "index", "version"])
def test_s18_incompatible_schema(tmp_path, malformation):
    root = tmp_path / "invalid"
    ControlStore(root).close()
    with sqlite3.connect(root / "control.sqlite") as disk:
        if malformation == "table":
            disk.execute("DROP TABLE job_requests")
            disk.execute("DROP TABLE jobs")
            disk.execute("CREATE TABLE jobs(id TEXT)")
        elif malformation == "index":
            disk.execute("DROP INDEX jobs_profile_active")
            disk.execute("CREATE UNIQUE INDEX jobs_profile_active ON jobs(profile_id) WHERE status='queued'")
        else:
            disk.execute("PRAGMA user_version=99")
        before = disk.execute("SELECT type,name,sql FROM sqlite_master ORDER BY name").fetchall()
    with storage_error():
        ControlStore(root)
    with sqlite3.connect(root / "control.sqlite") as disk:
        assert disk.execute("SELECT type,name,sql FROM sqlite_master ORDER BY name").fetchall() == before, "S18P_DDL"
