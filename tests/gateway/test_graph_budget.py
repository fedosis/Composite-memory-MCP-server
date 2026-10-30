"""Real async SQLite query-plan, VM interruption and custody checks (PR A)."""

from __future__ import annotations

import asyncio
import importlib
from datetime import UTC, datetime
from time import monotonic

import pytest
from sqlalchemy import event, text
from storage.repositories.relation_repo import RelationRepository

from memory_server.gateway_contracts import dto as d
from tests.gateway.test_graph_reads import F, G, get, relation_auth, seed
from tests.gateway.test_http_retrieval import live_server, read_scope


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1000, 10000, 50000])
async def test_sql_fetch_and_work_bound(tmp_path, count):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        resources = await seed(holder, auth, ids=(F, G), relations=[])
        async with resources.session_factory() as session:
            async with session.begin():
                rows = [dict(id=f"row-{i:06}") for i in range(count)]
                await session.execute(
                    text(
                        "INSERT INTO facts(id,subject,predicate,object,dedup_key,confidence,creator,"
                        "created_at,updated_at,verification_status,lifecycle_state,version) VALUES "
                        "(:id,'s','p','o',:id,0.8,'test','2026-10-30 00:00:00.000000',"
                        "'2026-10-30 00:00:00.000000','candidate','active','1')"
                    ),
                    rows,
                )
                await session.execute(
                    text(
                        "INSERT INTO claim_relations SELECT :root,id,'supports','2026-10-30 00:00:00.000000' "
                        "FROM facts WHERE id LIKE 'row-%'"
                    ),
                    dict(root=F),
                )
        statements, plans = [], []
        async with await read_scope(holder, auth) as scope:
            engine = scope.runtime.dependencies.sqlite.engine

            def trace(conn, cursor, statement, parameters, context, many):
                statements.append(statement)

            event.listen(engine.sync_engine, "before_cursor_execute", trace)
            try:
                for route in ("relations?limit=200", f"memories/{F}/relations?limit=200"):
                    status, body = await get(port, auth, route)
                    assert status == 200, body
                    assert len(body["items"]) == 200 and body["has_more"] is True
                async with resources.session_factory() as session:
                    for index, where in (
                        ("s09_order", ""),
                        ("s09_source_order", "WHERE source_id=:root"),
                        ("s09_target_order", "WHERE target_id=:root"),
                    ):
                        result = await session.execute(
                            text(
                                "EXPLAIN QUERY PLAN SELECT created_at,source_id,target_id,relation_type "
                                f"FROM claim_relations INDEXED BY {index} {where} "
                                "ORDER BY created_at,source_id,target_id,relation_type LIMIT 201"
                            ),
                            dict(root=F),
                        )
                        plans.extend(str(row) for row in result)
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", trace)
        assert all(
            index in "\n".join(statements)
            for index in ("INDEXED BY s09_order", "INDEXED BY s09_source_order", "INDEXED BY s09_target_order")
        )
        assert "USE TEMP B-TREE" not in "\n".join(plans)
        repo_module = importlib.import_module("storage.repositories.relation_repo")
        assert hasattr(repo_module, "RelationReadWork"), "SQL VM guard is absent"
        async with resources.session_factory() as session:
            work = repo_module.RelationReadWork()
            page = await RelationRepository(session).read_keyset_page(
                limit=200, last=None, upper=None, memory_id=F, work=work
            )
            assert len(page.rows) == 201
            assert work.vm_charged <= 200000 and work.statements <= 64
            print(
                f"SQL_BOUND count={count} vm_charged={work.vm_charged} "
                f"statements={work.statements} rows={len(page.rows)}"
            )


@pytest.mark.asyncio
async def test_missing_index_is_storage_not_budget(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        resources = await seed(holder, auth)
        async with resources.session_factory() as session:
            await session.execute(text("DROP INDEX s09_order"))
            await session.commit()
        status, body = await get(port, auth, "relations")
        assert (status, body["error"]["code"]) == (503, "E_STORAGE")


@pytest.mark.asyncio
async def test_vm_interrupt_drains_and_resets_same_connection(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        resources = await seed(holder, auth)
        status, body = await get(port, auth, "relations")
        assert status == 200, body
        module = importlib.import_module("storage.repositories.relation_repo")
        from memory_server.providers.graph_provider import GraphBudgetExceeded

        async with resources.session_factory() as session:
            repository = RelationRepository(session)
            work = module.RelationReadWork(vm_limit=5000)
            with pytest.raises(GraphBudgetExceeded):
                async with repository._read_window(work):
                    await session.execute(
                        text(
                            "WITH RECURSIVE x(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM x "
                            "WHERE n<1000000) SELECT SUM(n) FROM x"
                        )
                    )
            assert work.stop == "work" and work.vm_charged <= 6000
            assert (await session.execute(text("SELECT 42"))).scalar_one() == 42
            # A leaked progress callback would interrupt the same real driver.
            value = await session.execute(
                text(
                    "WITH RECURSIVE x(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM x WHERE n<10000) SELECT MAX(n) FROM x"
                )
            )
            assert value.scalar_one() == 10000
            print(f"ASYNC_RESET_OK charged={work.vm_charged} stop={work.stop}")


@pytest.mark.asyncio
async def test_orphan_prefix_budget_failure_is_not_empty_success(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        resources = await seed(holder, auth)
        async with resources.session_factory() as session:
            async with session.begin():
                await session.execute(
                    text("INSERT INTO claim_relations VALUES (:s,:t,'supports','2026-10-29 00:00:00.000000')"),
                    [dict(s=f"orphan-{i:06}", t=F) for i in range(10000)],
                )
        status, body = await get(port, auth, "relations?limit=200")
        assert (status, body["error"]["code"]) == (503, "E_GRAPH_BACKEND")


@pytest.mark.asyncio
async def test_repeated_cancellation_retains_producer_lease(tmp_path, monkeypatch):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        await seed(holder, auth)
        status, body = await get(port, auth, "relations")
        assert status == 200, body
        from memory_server.gateway_contracts.ports import RequestContext

        principal = await holder.app.dependencies.tokens.authenticate(auth["Authorization"][7:], now=datetime.now(UTC))
        reader = holder.app.dependencies.ports["relations.list"]
        entered, release = asyncio.Event(), asyncio.Event()
        original = RelationRepository.read_keyset_page

        async def delayed(repo, **kwargs):
            result = await original(repo, **kwargs)
            entered.set()
            await release.wait()
            return result

        monkeypatch.setattr(RelationRepository, "read_keyset_page", delayed)
        ctx = RequestContext("cancel-s09", principal, "alpha", None, monotonic() + 20)
        task = asyncio.create_task(reader.relations_list(ctx, d.PageQuery()))
        await asyncio.wait_for(entered.wait(), 10)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert holder.app.dependencies.leases.snapshot()["alpha"].admissions == 1
        assert not task.done()
        release.set()
        with pytest.raises(Exception) as error:
            await task
        assert error.value.code == "E_TIMEOUT"
        assert holder.app.dependencies.leases.snapshot()["alpha"].admissions == 0
