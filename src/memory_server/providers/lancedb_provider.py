"""LanceDB vector database provider.

Wraps lancedb for local-first persistent vector storage.
Default vector config: 384 dimensions, cosine distance (all-MiniLM-L6-v2 compatible).

No torch/sentence-transformers required — embeddings come from the existing
EmbeddingProvider abstraction.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from typing import Any

from memory_server.providers.exceptions import (
    ProviderSearchError,
    ProviderWriteError,
)

logger = logging.getLogger(__name__)

DEFAULT_TABLE = "memories"
DEFAULT_VECTOR_SIZE = 384

# Restricted filter contract (CORE-5/6, PROV-4).
#
# The provider turns caller-supplied filter dicts into LanceDB filter
# expressions. Two rules keep that translation safe:
#   1. Field identifiers are validated against the *actual* table schema
#      (allowlist), and only scalar, non-payload columns are accepted.
#   2. Values are rendered as typed literals with proper escaping — never
#      interpolated raw into the expression string.
# Qdrant-style must/should/must_not are fully supported (AND/OR/NOT), never
# silently ignored. Unsupported expressions raise ProviderSearchError before
# reaching the backend.
_FIELD_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# Columns that can never be filter targets: `vector` is the embedding itself
# and `_metadata` is a JSON *string* column (LanceDB cannot filter inside a
# JSON string — it is not a struct/map column).
_NON_FILTERABLE_COLUMNS = frozenset({"vector", "_metadata"})
_SUPPORTED_FILTER_KEYS = frozenset({"must", "should", "must_not"})
# Match conditions whose shape we cannot translate (e.g. qdrant range/geo).
_SUPPORTED_MATCH_KEYS = frozenset({"value"})


def _quote_literal(value: Any) -> str:
    """Render a Python scalar as a safe LanceDB filter literal.

    Strings are single-quoted with embedded quotes doubled (LanceDB filter
    syntax follows SQL string-literal escaping: ``'it''s'`` == ``it's``).
    Booleans/numbers are rendered unquoted. Any other type is rejected —
    a list/dict/None can never be a safe equality literal.
    """
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    raise ProviderSearchError(
        "unsupported filter value type "
        f"{type(value).__name__!r}: only str/int/float/bool are allowed"
    )


def _validate_field_identifier(field: Any, schema_names: list[str]) -> str:
    """Validate a filter field identifier against the table schema.

    Returns the canonical identifier or raises ProviderSearchError.
    """
    if not isinstance(field, str) or not _FIELD_IDENTIFIER_RE.match(field):
        raise ProviderSearchError(
            f"invalid filter field identifier {field!r}: expected a plain "
            "identifier matching [A-Za-z_][A-Za-z0-9_]*"
        )
    if field not in schema_names:
        allowed = [n for n in schema_names if n not in _NON_FILTERABLE_COLUMNS]
        raise ProviderSearchError(
            f"filter field {field!r} not present in table schema; "
            f"filterable columns: {sorted(allowed) or 'none'}"
        )
    if field in _NON_FILTERABLE_COLUMNS:
        raise ProviderSearchError(
            f"filter field {field!r} is not filterable "
            f"(excluded: {sorted(_NON_FILTERABLE_COLUMNS)})"
        )
    return field


def _validate_filter_value(field: str, value: Any, schema) -> None:
    """Validate that ``value`` is comparable to the column's scalar type."""
    col_type = schema.field(field).type
    import pyarrow as pa

    if pa.types.is_string(col_type) or pa.types.is_large_string(col_type):
        if not isinstance(value, str):
            raise ProviderSearchError(
                f"filter field {field!r} is a string column; got "
                f"{type(value).__name__} value {value!r} — use a string"
            )
    elif pa.types.is_boolean(col_type):
        if not isinstance(value, bool):
            raise ProviderSearchError(
                f"filter field {field!r} is a boolean column; got "
                f"{type(value).__name__} value {value!r} — use True/False"
            )
    elif pa.types.is_integer(col_type):
        if isinstance(value, bool) or not isinstance(value, int):
            raise ProviderSearchError(
                f"filter field {field!r} is an integer column; got "
                f"{type(value).__name__} value {value!r} — use an int"
            )
    elif pa.types.is_floating(col_type):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ProviderSearchError(
                f"filter field {field!r} is a float column; got "
                f"{type(value).__name__} value {value!r} — use a number"
            )
    else:
        raise ProviderSearchError(
            f"filter field {field!r} has non-scalar column type "
            f"{col_type} — filtering is unsupported"
        )


def _compile_condition(cond: Any, schema_names: list[str], schema) -> str:
    """Compile one Qdrant-style FieldCondition (key + match.value)."""
    if not isinstance(cond, dict):
        raise ProviderSearchError(
            "unsupported filter condition "
            f"{cond!r}: expected dict with 'key' and 'match'"
        )
    if "key" not in cond or "match" not in cond:
        raise ProviderSearchError(
            "unsupported filter condition "
            f"{cond!r}: expected 'key' and 'match'"
        )
    extra = set(cond) - {"key", "match"}
    if extra:
        raise ProviderSearchError(
            f"unsupported filter condition keys {sorted(extra)}: "
            "only 'key' and 'match' are supported"
        )
    match = cond["match"]
    if not isinstance(match, dict):
        raise ProviderSearchError(
            "unsupported filter match "
            f"{match!r}: expected dict like {{'value': ...}}"
        )
    extra = set(match) - _SUPPORTED_MATCH_KEYS
    if extra:
        raise ProviderSearchError(
            f"unsupported filter match keys {sorted(extra)}: "
            "only equality match {'value': ...} is supported"
        )
    field = _validate_field_identifier(cond["key"], schema_names)
    value = match.get("value")
    _validate_filter_value(field, value, schema)
    return f"{field} = {_quote_literal(value)}"


def _dict_to_filter(filter_: dict[str, Any], schema_names: list[str], schema) -> str | None:
    """Compile a restricted dict filter into a LanceDB filter expression.

    Supported shapes:
      * Flat equality:   {"field": value, ...}          -> AND of equalities
      * Qdrant-style:    {"must": [...], "should": [...], "must_not": [...]}
        - must     -> each condition ANDed
        - should   -> conditions ORed (fully supported, not ignored)
        - must_not -> each condition negated and ANDed
    Mixed flat + structured input and any other expression (range/geo/nested)
    raise ProviderSearchError *before* the backend is called.
    """
    if not isinstance(filter_, dict):
        raise ProviderSearchError(
            f"unsupported filter type {type(filter_).__name__}: "
            "expected dict or LanceDB filter string"
        )

    structured_keys = _SUPPORTED_FILTER_KEYS & set(filter_)
    flat_keys = set(filter_) - _SUPPORTED_FILTER_KEYS

    if structured_keys:
        if flat_keys:
            raise ProviderSearchError(
                "mixed structured/flat filter is unsupported; pass either "
                "must/should/must_not or plain field equalities, not both "
                f"(flat keys: {sorted(flat_keys)})"
            )
        # Qdrant-style: each condition is {'key': ..., 'match': {'value': ...}}.
        # Semantics follow Qdrant Filter: must = AND, should = OR,
        # must_not = NOT (each negated, all ANDed). Empty groups contribute
        # nothing; groups are combined with AND.
        groups: list[str] = []
        must_items = [_compile_condition(i, schema_names, schema)
                      for i in (filter_.get("must", []) or [])]
        if must_items:
            groups.append("(" + " AND ".join(
                f"({m})" for m in must_items
            ) + ")")
        should_items = [_compile_condition(i, schema_names, schema)
                        for i in (filter_.get("should", []) or [])]
        if should_items:
            groups.append("(" + " OR ".join(
                f"({s})" for s in should_items
            ) + ")")
        must_not_items = [_compile_condition(i, schema_names, schema)
                          for i in (filter_.get("must_not", []) or [])]
        if must_not_items:
            groups.append("(" + " AND ".join(
                f"(NOT {m})" for m in must_not_items
            ) + ")")
        if not groups:
            return None  # all groups empty -> match all
        return " AND ".join(groups)

    # Flat equality: {field: value, ...}. Values must be scalars.
    parts = []
    for field, value in filter_.items():
        f = _validate_field_identifier(field, schema_names)
        if isinstance(value, (dict, list)):
            raise ProviderSearchError(
                f"unsupported filter value for field {f!r}: {value!r} — "
                "only scalar equality filters are supported"
            )
        _validate_filter_value(f, value, schema)
        parts.append(f"{f} = {_quote_literal(value)}")
    if not parts:
        return None
    return " AND ".join(parts)


def _compile_filter(
    filter_: dict | str | None, schema_names: list[str], schema
) -> str | None:
    """Restricted entry point: compile a filter for a known table schema."""
    if filter_ is None:
        return None
    if isinstance(filter_, str):
        # Trusted LanceDB-native filter string. Kept as the documented escape
        # hatch for callers that manage their own expressions; the restricted
        # dict path above is what validates identifiers/literals.
        return filter_
    return _dict_to_filter(filter_, schema_names, schema)


def _normalize_metric(metric: str) -> str:
    """Normalize metric name to LanceDB format."""
    mapping = {
        "cosine": "cosine",
        "l2": "l2",
        "euclid": "l2",
        "euclidean": "l2",
        "dot": "dot",
        "dot_product": "dot",
    }
    return mapping.get(metric.lower(), "cosine")


class LanceDBProvider:
    """Provider wrapping LanceDB for vector storage and search.

    Args:
        db_path: Path to the LanceDB database directory (default "data/lancedb").
        table: Default table name (default "memories").
        metric: Distance metric (default "cosine"). Options: cosine, l2, dot.
        vector_size: Default vector dimensionality (default 384).
    """

    def __init__(
        self,
        db_path: str | None = None,
        table: str | None = None,
        metric: str | None = None,
        vector_size: int | None = None,
    ) -> None:
        # None → resolve from Settings (defaults equal the previous module
        # constants; explicit args still win).
        from memory_server.settings import get_settings

        settings = get_settings()
        self._db_path = db_path if db_path is not None else str(settings.lancedb_path)
        self._table_name = table if table is not None else settings.vector_collection
        metric_value = metric if metric is not None else settings.vector_metric
        self._metric = _normalize_metric(metric_value)
        self._vector_size = (
            vector_size if vector_size is not None else settings.vector_size
        )
        self._db: Any = None  # lazy-init

    async def _get_db(self):
        """Lazy-init the LanceDB database connection."""
        if self._db is None:
            import lancedb

            self._db = await asyncio.to_thread(lancedb.connect, self._db_path)
            # Ensure default table exists
            await self._ensure_table(self._table_name, db=self._db)
        return self._db

    async def _ensure_table(self, name: str, db=None) -> bool:
        """Create the table if it doesn't exist.

        Raises ProviderWriteError on failure: a down backend must surface as
        a typed error on the first public operation, not a silent False.
        """
        if db is None:
            db = await self._get_db()
        try:
            table_names = await asyncio.to_thread(db.table_names)
            if name in table_names:
                return True

            # Create with an empty batch to define the schema
            import pyarrow as pa

            schema = pa.schema([
                pa.field("id", pa.utf8()),
                pa.field("vector", pa.list_(pa.float32(), self._vector_size)),
                pa.field("_metadata", pa.utf8()),  # JSON-encoded payload
            ])
            await asyncio.to_thread(
                db.create_table, name, schema=schema, exist_ok=True,
            )
            logger.info(
                "Created table '%s' (vector_size=%d, metric=%s)",
                name,
                self._vector_size,
                self._metric,
            )
            return True
        except Exception as exc:
            logger.warning("Failed to ensure table '%s': %s", name, exc)
            raise ProviderWriteError(
                f"failed to ensure lancedb table '{name}': {exc}"
            ) from exc

    async def _run(self, func, *args, **kwargs):
        """Run a blocking LanceDB call in a thread."""
        return await asyncio.to_thread(func, *args, **kwargs)

    async def _get_table(self, name: str | None = None):
        """Get a LanceDB table by name (or default)."""
        table_name = name or self._table_name
        db = await self._get_db()
        await self._ensure_table(table_name)
        return await asyncio.to_thread(db.open_table, table_name)

    # ------------------------------------------------------------------
    # Table (collection) management
    # ------------------------------------------------------------------

    async def create_collection(
        self,
        name: str,
        vector_size: int | None = None,
        distance: str | None = None,
    ) -> bool:
        """Create a new table (collection).

        Args:
            name: Table name.
            vector_size: Vector dimensionality (default: provider default).
            distance: Distance metric (default: provider default). Ignored
                      in v0.10 — LanceDB metric is set at provider level.

        Returns:
            True if created, False if already exists or error.
        """
        try:
            db = await self._get_db()
            table_names = await asyncio.to_thread(db.table_names)
            if name in table_names:
                return False

            import pyarrow as pa

            vs = vector_size or self._vector_size
            schema = pa.schema([
                pa.field("id", pa.utf8()),
                pa.field("vector", pa.list_(pa.float32(), vs)),
                pa.field("_metadata", pa.utf8()),
            ])
            await asyncio.to_thread(
                db.create_table, name, schema=schema, exist_ok=True,
            )
            return True
        except ProviderWriteError:
            raise
        except Exception as exc:
            logger.error("Failed to create table '%s': %s", name, exc)
            raise ProviderWriteError(
                f"lancedb create_collection failed: {exc}"
            ) from exc

    async def delete_collection(self, name: str) -> bool:
        """Delete a table (collection).

        Returns:
            True if deleted, False if not found or error.
        """
        try:
            db = await self._get_db()
            await asyncio.to_thread(db.drop_table, name, ignore_missing=True)
            return True
        except ProviderWriteError:
            raise
        except Exception as exc:
            logger.warning("Failed to delete table '%s': %s", name, exc)
            raise ProviderWriteError(
                f"lancedb delete_collection failed: {exc}"
            ) from exc

    async def list_collections(self) -> list[str]:
        """List all table names."""
        try:
            db = await self._get_db()
            return await asyncio.to_thread(db.table_names)
        except Exception as exc:
            logger.error("Failed to list tables: %s", exc)
            raise ProviderSearchError(
                f"lancedb list_collections failed: {exc}"
            ) from exc

    async def count_points(self, collection: str | None = None) -> int:
        """Return the number of vector rows in the collection/table."""
        try:
            table = await self._get_table(collection)
            return int(await asyncio.to_thread(table.count_rows))
        except Exception as exc:
            logger.error("Failed to count points: %s", exc)
            raise ProviderSearchError(
                f"lancedb count_points failed: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Point operations
    # ------------------------------------------------------------------

    async def upsert(
        self,
        collection: str | None = None,
        point_id: str | int = "",
        vector: list[float] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> bool:
        """Upsert a single point with vector and optional payload.

        Args:
            collection: Table name (default: provider default).
            point_id: Unique point ID (generated if empty).
            vector: Embedding vector.
            payload: Optional metadata payload.

        Returns:
            True on success.
        """
        pid = str(point_id) if point_id else str(uuid.uuid4())
        try:
            table = await self._get_table(collection)
            import pyarrow as pa

            data = pa.table({
                "id": pa.array([pid], type=pa.utf8()),
                "vector": pa.array([vector or []], type=pa.list_(pa.float32())),
                "_metadata": pa.array(
                    [json.dumps(payload or {})], type=pa.utf8()
                ),
            })
            # True upsert by id: merge_insert replaces matching rows and
            # inserts new ones. Plain add(mode="append") would create
            # duplicate rows on retry (outbox worker retries failed entries).
            await self._run(
                table.merge_insert("id")
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute,
                data,
            )
            return True
        except ProviderWriteError:
            raise
        except Exception as exc:
            logger.error("Failed to upsert point %s: %s", pid, exc)
            raise ProviderWriteError(f"lancedb upsert failed: {exc}") from exc

    async def upsert_batch(
        self,
        points: list[dict[str, Any]],
        collection: str | None = None,
    ) -> bool:
        """Upsert multiple points at once.

        Each dict should have keys: id (str|int), vector (list[float]), payload (dict, optional).

        Args:
            points: List of point dicts.
            collection: Table name (default: provider default).

        Returns:
            True on success.
        """
        try:
            table = await self._get_table(collection)
            import pyarrow as pa

            # Deduplicate by id within the batch: merge_insert does not
            # collapse duplicate keys inside a single payload, so two entries
            # for the same id in one batch would insert both rows. Last
            # occurrence wins (matching dict upsert semantics).
            seen: dict[str, dict[str, Any]] = {}
            for p in points:
                seen[str(p.get("id", str(uuid.uuid4())))] = p
            points = list(seen.values())

            ids = [str(p.get("id", str(uuid.uuid4()))) for p in points]
            vectors = [p.get("vector", []) for p in points]
            metadatas = [json.dumps(p.get("payload", {})) for p in points]

            data = pa.table({
                "id": pa.array(ids, type=pa.utf8()),
                "vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "_metadata": pa.array(metadatas, type=pa.utf8()),
            })
            # True upsert by id — same reasoning as upsert(): avoid
            # duplicate rows when an entry is reprocessed after a retry.
            await self._run(
                table.merge_insert("id")
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute,
                data,
            )
            return True
        except ProviderWriteError:
            raise
        except Exception as exc:
            logger.error("Failed to batch upsert: %s", exc)
            raise ProviderWriteError(f"lancedb upsert_batch failed: {exc}") from exc

    async def search(
        self,
        collection: str | None = None,
        vector: list[float] | None = None,
        limit: int = 10,
        score_threshold: float | None = None,
        filter_: dict | str | None = None,
    ) -> list[dict[str, Any]]:
        """Search for nearest neighbors.

        Args:
            collection: Table name (default: provider default).
            vector: Query vector.
            limit: Max results (default 10).
            score_threshold: Minimum similarity score (optional).
            filter_: LanceDB filter expression or dict (optional).

        Returns:
            List of result dicts with keys: id, score, payload.
        """
        if not vector:
            return []

        try:
            table = await self._get_table(collection)
            import numpy as np  # lazy: optional dep, only needed for search
            lance_filter = None
            if filter_ is not None:
                schema = await self._run(lambda: table.schema)
                # Restricted contract: dict filters are validated/compiled here
                # (allowlist identifiers, safe literals) BEFORE reaching backend;
                # unsupported expressions raise ProviderSearchError.
                lance_filter = _compile_filter(
                    filter_, list(schema.names), schema
                )
            query = table.search(np.array(vector, dtype=np.float32))

            if self._metric == "cosine":
                query = query.metric("cosine")
            elif self._metric == "l2":
                query = query.metric("l2")
            elif self._metric == "dot":
                query = query.metric("dot")

            if lance_filter:
                query = query.where(lance_filter, prefilter=True)

            # where() is applied before limit(): the prefilter narrows the
            # candidate set and only then is the top-k limit taken.
            results = await self._run(query.limit(limit).to_list)

            parsed = []
            for r in results:
                raw_distance = r.get("_distance", 0.0)
                if self._metric in ("cosine", "dot"):
                    # LanceDB (installed 0.34) reports distance = 1 - sim
                    # (cosine similarity, or the raw dot product for the dot
                    # metric), monotone DECREASING in similarity. Expose
                    # score = 1 - distance/2 = (1 + sim)/2 — monotone
                    # INCREASING in similarity. No abs(): a negative
                    # distance (dot > 1 on unnormalized vectors) must rank
                    # ABOVE distance 0, not collapse into a tie with the
                    # abs() of a large positive distance.
                    similarity = 1.0 - (raw_distance / 2.0)
                else:  # l2
                    # L2 distance: [0, +inf), lower = closer.
                    similarity = 1.0 / (1.0 + raw_distance)

                if score_threshold is not None and similarity < score_threshold:
                    continue

                payload_raw = r.get("_metadata", "{}")
                if isinstance(payload_raw, str):
                    try:
                        payload = json.loads(payload_raw)
                    except (json.JSONDecodeError, TypeError):
                        payload = {}
                elif isinstance(payload_raw, dict):
                    payload = payload_raw
                else:
                    payload = {}

                parsed.append({
                    "id": str(r.get("id", "")),
                    "score": similarity,
                    "payload": payload,
                })

            return parsed
        except ProviderSearchError:
            raise
        except Exception as exc:
            logger.error("Search failed: %s", exc)
            raise ProviderSearchError(f"lancedb search failed: {exc}") from exc

    async def scroll(
        self,
        collection: str | None = None,
        limit: int = 100,
        filter_: dict | str | None = None,
    ) -> list[dict[str, Any]]:
        """Scroll through all points in a table.

        Args:
            collection: Table name (default: provider default).
            limit: Max points to return (default 100).
            filter_: LanceDB filter expression or dict (optional).

        Returns:
            List of point dicts with keys: id, payload.
        """
        try:
            table = await self._get_table(collection)
            lance_filter = None
            if filter_ is not None:
                schema = await self._run(lambda: table.schema)
                # Same restricted filter contract as search(): unsupported
                # expressions raise before the backend is called.
                lance_filter = _compile_filter(
                    filter_, list(schema.names), schema
                )
            query = table.search()
            if lance_filter:
                query = query.where(lance_filter, prefilter=True)
            # where() before limit(): the limit applies to filtered rows.
            results = await self._run(query.limit(limit).to_list)

            parsed = []
            for r in results:
                payload_raw = r.get("_metadata", "{}")
                if isinstance(payload_raw, str):
                    try:
                        payload = json.loads(payload_raw)
                    except (json.JSONDecodeError, TypeError):
                        payload = {}
                elif isinstance(payload_raw, dict):
                    payload = payload_raw
                else:
                    payload = {}

                parsed.append({
                    "id": str(r.get("id", "")),
                    "payload": payload,
                })

            return parsed
        except ProviderSearchError:
            raise
        except Exception as exc:
            logger.error("Scroll failed: %s", exc)
            raise ProviderSearchError(f"lancedb scroll failed: {exc}") from exc

    async def delete(
        self,
        collection: str | None = None,
        point_id: str | int = "",
    ) -> bool:
        """Delete a point by ID.

        Args:
            collection: Table name (default: provider default).
            point_id: Point ID to delete.

        Returns:
            True if deleted, False if not found or error.
        """
        try:
            table = await self._get_table(collection)
            pid = str(point_id)
            # Safe literal: point_id may contain quotes/injection-like text —
            # it must be escaped as a string literal, never interpolated raw.
            await self._run(table.delete, f"id = {_quote_literal(pid)}")
            return True
        except ProviderSearchError:
            raise
        except ProviderWriteError:
            raise
        except Exception as exc:
            logger.warning("Failed to delete point %s: %s", point_id, exc)
            raise ProviderWriteError(f"lancedb delete failed: {exc}") from exc

    async def optimize(
        self,
        collection: str | None = None,
        cleanup_older_than: Any | None = None,
        delete_unverified: bool = False,
    ) -> bool:
        """Compact the table and prune old versions (LanceDB VACUUM).

        Every upsert/merge creates a new table version; without periodic
        compaction the ``_versions`` directory accumulates full copies of the
        data and can grow orders of magnitude larger than the live dataset.

        Args:
            collection: Table name (default: provider default).
            cleanup_older_than: Minimum age of versions to delete
                (default: LanceDB default, ~7 days).
            delete_unverified: Allow deleting files newer than 7 days
                (default False — safe for concurrent writers).

        Returns:
            True on success.
        """
        try:
            table = await self._get_table(collection)
            await self._run(
                table.optimize,
                cleanup_older_than=cleanup_older_than,
                delete_unverified=delete_unverified,
            )
            logger.info(
                "LanceDB table '%s' optimized (compaction + prune)",
                table.name,
            )
            return True
        except ProviderWriteError:
            raise
        except Exception as exc:
            logger.warning("LanceDB optimize failed: %s", exc)
            raise ProviderWriteError(f"lancedb optimize failed: {exc}") from exc

    async def close(self) -> None:
        """Close the underlying LanceDB database connection."""
        self._db = None
        logger.debug("LanceDB connection closed")
