"""Provider typed-error contract tests (Card 3a).

RED phase: every public method of LanceDBProvider / QdrantProvider must
convert backend failures into ProviderWriteError / ProviderSearchError.
Legitimate non-error semantics (empty search [], delete-missing True,
already-exists False, missing-collection degradation) stay unchanged.
"""

import shutil
import sys
import tempfile
import types

# lancedb/numpy/pyarrow/qdrant_client are imported at COLLECTION time and
# stashed so the fixtures can restore them: test_ping's
# test_server_import_does_not_import_optional_vector_backends pops these
# module trees from sys.modules mid-session (re-import would panic).
import httpx
import lancedb
import numpy
import pyarrow
import pytest
from qdrant_client.http.exceptions import UnexpectedResponse

from memory_server.providers.exceptions import (
    ProviderError,
    ProviderSearchError,
    ProviderWriteError,
)
from memory_server.providers.lancedb_provider import LanceDBProvider
from memory_server.providers.qdrant_provider import QdrantProvider

_VECTOR_ROOTS = {m.__name__ for m in (lancedb, numpy, pyarrow)} | {"qdrant_client"}
_VECTOR_MODULES = {n: mod for n, mod in sys.modules.items() if n.split(".")[0] in _VECTOR_ROOTS}


@pytest.fixture(autouse=True)
def _restore_vector_modules():
    sys.modules.update(_VECTOR_MODULES)
    yield

# Stub helpers — async/sync split (mandatory, see PLAN Task 3)


def raise_sync(exc):
    """SYNC stub raising exc WHEN CALLED — for everything executed via
    asyncio.to_thread / self._run (Qdrant _client.<method>, LanceDB
    db.<method> / table.<method>). An async stub here would make to_thread
    return an unawaited coroutine and NO exception would raise."""

    def _f(*a, **k):
        raise exc

    return _f


def raise_async(exc):
    """ASYNC stub raising exc when awaited — for internals awaited DIRECTLY
    (LanceDB _get_db / _get_table / _ensure_table). Never use this for
    _client.<method> (to_thread path)."""

    async def _f(*a, **k):
        raise exc

    return _f


class _Chain:
    """Method-chain stub: calls return self; terminal ops (execute/to_list) raise."""

    def __init__(self, exc):
        self._exc = exc

    def __call__(self, *a, **k):
        return self

    def __getattr__(self, name):
        if name in ("execute", "to_list"):
            return raise_sync(self._exc)
        return self


class _BadTable:
    """LanceDB table stub whose backend ops raise exc (sync, thread-safe)."""

    def __init__(self, exc):
        self._exc = exc

    def __getattr__(self, name):
        if name in ("count_rows", "delete", "optimize"):
            return raise_sync(self._exc)
        return _Chain(self._exc)  # search(), merge_insert()


def _ur(status_code: int, reason: str) -> UnexpectedResponse:
    """UnexpectedResponse factory (qdrant-client 1.18.0 ctor requires ALL
    FOUR args: status_code, reason_phrase, content, headers)."""
    return UnexpectedResponse(
        status_code=status_code,
        reason_phrase=reason,
        content=b"",
        headers=httpx.Headers({}),
    )


# Fixtures


@pytest.fixture
async def lancedb_provider():
    """LanceDB provider in a tempdir with self._db pre-initialized.

    Pre-initializing _db means body-failure patches (A2) hit the operation,
    not acquisition: _get_db is cached and _ensure_table's table_names check
    succeeds against the real db.
    """
    tmp_dir = tempfile.mkdtemp(prefix="lancedb_exc_test_")
    p = LanceDBProvider(db_path=tmp_dir, table="test_memories")
    await p._get_db()  # connects and ensures table "test_memories"
    yield p
    shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.fixture
def qdrant_provider():
    """In-memory Qdrant provider (auto-creates default collection)."""
    return QdrantProvider(location=":memory:", prefer_grpc=False)


# Class A1 — LanceDB acquisition failure (per-method, parameterized)

_LANCE_A1_ROWS = [
    # (method, call kwargs, patch target, expected class)
    ("create_collection", {"name": "t2"}, "_get_db", ProviderWriteError),
    ("delete_collection", {"name": "test_memories"}, "_get_db", ProviderWriteError),
    ("list_collections", {}, "_get_db", ProviderSearchError),
    ("count_points", {}, "_get_table", ProviderSearchError),
    ("upsert", {"point_id": "p1", "vector": [0.1] * 384}, "_get_table", ProviderWriteError),
    ("upsert_batch", {"points": [{"id": "p1", "vector": [0.1] * 384}]}, "_get_table", ProviderWriteError),
    ("search", {"vector": [0.1] * 384}, "_get_table", ProviderSearchError),
    ("scroll", {}, "_get_table", ProviderSearchError),
    ("delete", {"point_id": "p1"}, "_get_table", ProviderWriteError),
    ("optimize", {}, "_get_table", ProviderWriteError),
]


class TestLanceDBAcquisitionFailure:
    """A1: backend acquisition failure converts to the mapped typed error."""

    @pytest.mark.parametrize("method, kwargs, target, expected", _LANCE_A1_ROWS)
    async def test_acquisition_failure(
        self, lancedb_provider, monkeypatch, method, kwargs, target, expected
    ):
        provider = lancedb_provider
        monkeypatch.setattr(provider, target, raise_async(RuntimeError("backend down")))
        with pytest.raises(expected):
            await getattr(provider, method)(**kwargs)


# Class A2 — LanceDB operation-body failure (per-method, parameterized)

_LANCE_A2_ROWS = [
    # (method, call kwargs, db/table attr to break, expected class)
    ("create_collection", {"name": "t2"}, "table_names", ProviderWriteError),
    ("delete_collection", {"name": "test_memories"}, "drop_table", ProviderWriteError),
    ("list_collections", {}, "table_names", ProviderSearchError),
    ("count_points", {}, "open_table", ProviderSearchError),
    ("upsert", {"point_id": "p1", "vector": [0.1] * 384}, "open_table", ProviderWriteError),
    ("upsert_batch", {"points": [{"id": "p1", "vector": [0.1] * 384}]}, "open_table", ProviderWriteError),
    ("search", {"vector": [0.1] * 384}, "open_table", ProviderSearchError),
    ("scroll", {}, "open_table", ProviderSearchError),
    ("delete", {"point_id": "p1"}, "open_table", ProviderWriteError),
    ("optimize", {}, "open_table", ProviderWriteError),
]


class TestLanceDBBodyFailure:
    """A2: the operation's OWN backend call failure converts per method."""

    @pytest.mark.parametrize("method, kwargs, target, expected", _LANCE_A2_ROWS)
    async def test_body_failure(
        self, lancedb_provider, monkeypatch, method, kwargs, target, expected
    ):
        provider = lancedb_provider
        exc = RuntimeError("body down")
        db = await provider._get_db()
        if target == "table_names":
            monkeypatch.setattr(db, "table_names", raise_sync(exc))
        elif target == "drop_table":
            monkeypatch.setattr(db, "drop_table", raise_sync(exc))
        else:
            monkeypatch.setattr(db, "open_table", lambda name: _BadTable(exc))
        with pytest.raises(expected):
            await getattr(provider, method)(**kwargs)


# Class A3 — typed write-error → search-class conversion


class TestLanceDBSearchClassConversion:
    """A3: search-class public methods convert even typed write errors."""

    async def test_typed_write_error_becomes_search_error(
        self, lancedb_provider, monkeypatch
    ):
        provider = lancedb_provider
        monkeypatch.setattr(
            provider,
            "_ensure_table",
            raise_async(ProviderWriteError("ensure failed")),
        )
        with pytest.raises(ProviderSearchError):
            await provider.search(vector=[0.1] * 384)
        with pytest.raises(ProviderSearchError):
            await provider.count_points()


# Class B — Qdrant per-method matrix (the _client call IS the operation body)

_QDRANT_B_ROWS = [
    # (method, call kwargs, patched _client attr, expected class)
    ("create_collection", {"name": "c2"}, "get_collections", ProviderWriteError),
    ("delete_collection", {"name": "memories"}, "delete_collection", ProviderWriteError),
    ("list_collections", {}, "get_collections", ProviderSearchError),
    ("count_points", {}, "count", ProviderSearchError),
    ("upsert", {"point_id": "p1", "vector": [0.1] * 384}, "upsert", ProviderWriteError),
    ("upsert_batch", {"points": [{"id": "p1", "vector": [0.1] * 384}]}, "upsert", ProviderWriteError),
    ("search", {"vector": [0.1] * 384}, "query_points", ProviderSearchError),
    ("scroll", {}, "scroll", ProviderSearchError),
    ("delete", {"point_id": "p1"}, "delete", ProviderWriteError),
]


class TestQdrantMethodMatrix:
    """B: every public Qdrant method converts _client failure to typed error."""

    @pytest.mark.parametrize("method, kwargs, attr, expected", _QDRANT_B_ROWS)
    async def test_client_failure(
        self, qdrant_provider, monkeypatch, method, kwargs, attr, expected
    ):
        provider = qdrant_provider
        monkeypatch.setattr(
            provider._client, attr, raise_sync(RuntimeError("backend down"))
        )
        with pytest.raises(expected):
            await getattr(provider, method)(**kwargs)


# Class C — classification tests (real ApiException / ValueError semantics)


class TestQdrantClassification:
    """C: HTTP ApiException (404/409) and :memory: ValueError classification."""

    async def test_delete_collection_404_returns_true(self, qdrant_provider, monkeypatch):
        provider = qdrant_provider
        monkeypatch.setattr(
            provider._client, "delete_collection", raise_sync(_ur(404, "Not Found"))
        )
        assert await provider.delete_collection("memories") is True

    async def test_create_collection_409_returns_false(self, qdrant_provider, monkeypatch):
        provider = qdrant_provider
        monkeypatch.setattr(
            provider._client,
            "get_collections",
            lambda *a, **k: types.SimpleNamespace(collections=[]),
        )
        monkeypatch.setattr(
            provider._client, "create_collection", raise_sync(_ur(409, "Conflict"))
        )
        assert await provider.create_collection("c2") is False

    async def test_search_404_returns_empty(self, qdrant_provider, monkeypatch):
        provider = qdrant_provider
        monkeypatch.setattr(
            provider._client, "query_points", raise_sync(_ur(404, "Not Found"))
        )
        assert await provider.search(vector=[0.1] * 384) == []

    async def test_scroll_404_returns_empty(self, qdrant_provider, monkeypatch):
        provider = qdrant_provider
        monkeypatch.setattr(
            provider._client, "scroll", raise_sync(_ur(404, "Not Found"))
        )
        assert await provider.scroll() == []

    async def test_delete_point_404_returns_true(self, qdrant_provider, monkeypatch):
        provider = qdrant_provider
        monkeypatch.setattr(
            provider._client, "delete", raise_sync(_ur(404, "Not Found"))
        )
        assert await provider.delete(point_id="p1") is True

    async def test_upsert_409_raises_write_error(self, qdrant_provider, monkeypatch):
        """No over-broad 409: conflict on upsert is a genuine failure."""
        provider = qdrant_provider
        monkeypatch.setattr(
            provider._client, "upsert", raise_sync(_ur(409, "Conflict"))
        )
        with pytest.raises(ProviderWriteError):
            await provider.upsert(point_id="p1", vector=[0.1] * 384)

    async def test_delete_point_409_raises_write_error(self, qdrant_provider, monkeypatch):
        """No over-broad 409: conflict is special-cased ONLY for create_collection."""
        provider = qdrant_provider
        monkeypatch.setattr(
            provider._client, "delete", raise_sync(_ur(409, "Conflict"))
        )
        with pytest.raises(ProviderWriteError):
            await provider.delete(point_id="p1")

    async def test_memory_search_missing_collection_returns_empty(
        self, qdrant_provider, monkeypatch
    ):
        provider = qdrant_provider
        monkeypatch.setattr(
            provider._client,
            "query_points",
            raise_sync(ValueError("Collection nope not found")),
        )
        assert await provider.search(vector=[0.1] * 384) == []

    async def test_memory_create_already_exists_returns_false(
        self, qdrant_provider, monkeypatch
    ):
        provider = qdrant_provider
        monkeypatch.setattr(
            provider._client,
            "get_collections",
            lambda *a, **k: types.SimpleNamespace(collections=[]),
        )
        monkeypatch.setattr(
            provider._client,
            "create_collection",
            raise_sync(ValueError("Collection x already exists")),
        )
        assert await provider.create_collection("c2") is False


class TestLanceDBRealPathClassification:
    """C: LanceDB real-path legit semantics preserved."""

    async def test_legit_semantics(self, lancedb_provider):
        provider = lancedb_provider
        # delete missing table → True
        assert await provider.delete_collection("nope_missing") is True
        # delete missing point id → True
        assert await provider.delete(point_id="nope-point") is True
        # create already-exists → False
        assert await provider.create_collection("test_memories") is False
        # search empty table → []
        assert await provider.search(vector=[0.1] * 384) == []


class TestHierarchy:
    """Base ProviderError catches both subclasses (for sub-card 3b)."""

    def test_base_catches_subclasses(self):
        for exc in (ProviderWriteError("write failed"), ProviderSearchError("search failed")):
            with pytest.raises(ProviderError):
                raise exc
