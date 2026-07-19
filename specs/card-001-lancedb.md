# Card 001: LanceDB Vector Store

**Phase:** v0.10
**Status:** Implementation
**Depends on:** v0.9 (Ternary Relation Classifier), ADR-015

## Objective

Replace Qdrant as the default vector store backend with LanceDB. Qdrant is
preserved as an optional server-mode backend for deployments that require it.

## Background

Benchmark CUR-CMMS-VECTORSTORE-BENCH-001 (5K records) confirmed LanceDB as
the best local-first vector storage option. CUR-CMMS-QDRANT-ALT-001 validated
the architectural fit.

## Specification

### 1. LanceDBProvider

**File:** `src/memory_server/providers/lancedb_provider.py`

Implements the same async interface as `QdrantProvider` (`create_collection`,
`delete_collection`, `list_collections`, `upsert`, `upsert_batch`, `search`,
`scroll`, `delete`, `close`).

#### Constructor

```python
LanceDBProvider(
    db_path: str = "data/lancedb",
    table: str = "memories",
    metric: str = "cosine",     # cosine | l2 | dot
    vector_size: int = 384,
) -> None
```

- `db_path`: Path to the LanceDB database directory (created if not exists).
- `table`: Default table name.
- `metric`: Distance metric for vector comparison.
- `vector_size`: Dimensionality of vectors (default 384 = all-MiniLM-L6-v2).

#### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `create_collection()` | `(name, vector_size?, distance?) -> bool` | Create a new LanceDB table with the given name and vector config |
| `delete_collection()` | `(name) -> bool` | Drop a table |
| `list_collections()` | `() -> list[str]` | List all table names |
| `upsert()` | `(collection?, point_id, vector?, payload?) -> bool` | Insert or replace a single vector with metadata |
| `upsert_batch()` | `(points, collection?) -> bool` | Insert multiple vectors at once |
| `search()` | `(collection?, vector?, limit?, score_threshold?, filter_?) -> list[dict]` | Nearest neighbor search with optional metadata filtering |
| `scroll()` | `(collection?, limit?, filter_?) -> list[dict]` | List all vectors in a table |
| `delete()` | `(collection?, point_id) -> bool` | Delete a vector by ID |
| `close()` | `() -> None` | Close the database connection |

#### Search with Filtering

Support LanceDB's native filter expressions via optional `filter_` parameter.
Accepts LanceDB-compatible filter strings (e.g. `"subject = 'Docker'"`).

When `filter_` is a dict, it's converted to an equivalent LanceDB filter
expression for simple field equality checks.

### 2. Embedding Dependencies

- Required: `numpy`, `lancedb>=0.12.0`
- NOT required: `torch`, `sentence-transformers` (embeddings come from the
  existing `EmbeddingProvider` abstraction)

### 3. Default Backend Change

- `server.py`: `LanceDBProvider` becomes the default vector store.
- Qdrant: preserved as optional `[qdrant]` extra with `QdrantProvider`.
- Environment variable `MEMORY_VECTOR_BACKEND=lancedb|qdrant` chooses the
  backend at startup.

### 4. Integration Points

- **`server.py`**: Add `_get_lancedb_provider()` function. LanceDB as default,
  Qdrant as fallback if env var or config indicates.
- **`embedding_router.py`**: Accept `LanceDBProvider | QdrantProvider` union type.
- **`hybrid_router.py`**: Same union type update.
- **`outbox_worker.py`**: Accept `LanceDBProvider | QdrantProvider` union type.
- **`ranking.py`**: `semantic_from_qdrant()` → rename to `semantic_from_vector()`
  or keep backward-compatible alias.

### 5. Configuration

```toml
# pyproject.toml
[project.optional-dependencies]
lancedb = ["lancedb>=0.12.0"]
```

## Test Plan

### LanceDBProvider Tests (`tests/test_lancedb_provider.py`)

Same coverage as `test_qdrant_provider.py`:

1. **Collection Management** — create, delete, list, duplicate create returns False,
   default table exists
2. **Point Operations** — upsert + search, score threshold, empty collection,
   ranking, scroll, scroll with filter, delete, delete nonexistent, upsert without payload

### Qdrant Test Compatibility

Existing `test_qdrant_provider.py` tests remain unchanged and continue to pass
(`QdrantProvider` is preserved).

## Migration

No automatic migration from Qdrant to LanceDB. Users already using Qdrant
(server-mode) keep it via `[qdrant]` extra. New deployments default to LanceDB.

## Files Changed

| File | Action |
|------|--------|
| `src/memory_server/providers/lancedb_provider.py` | **CREATE** — LanceDBProvider |
| `src/memory_server/providers/__init__.py` | **UPDATE** — export LanceDBProvider |
| `src/memory_server/server.py` | **UPDATE** — add _get_lancedb_provider(), LanceDB as default |
| `src/memory_server/router/embedding_router.py` | **UPDATE** — union type for provider |
| `src/memory_server/router/hybrid_router.py` | **UPDATE** — union type for provider |
| `storage/outbox_worker.py` | **UPDATE** — union type for provider |
| `src/memory_server/router/ranking.py` | **UPDATE** — semantic_from_qdrant→semantic_from_vector alias |
| `pyproject.toml` | **UPDATE** — add lancedb optional dependency |
| `tests/test_lancedb_provider.py` | **CREATE** — LanceDBProvider tests |
| `docs/ADR.md` | **UPDATE** — ADR-015 |
