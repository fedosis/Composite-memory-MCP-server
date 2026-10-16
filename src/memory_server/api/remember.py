"""MCP tool: remember — store a fact and generate a provenance receipt.

Compatibility facade (ARCHITECTURE_SOURCE §38): the public signature, parameter
names, order and defaults are unchanged. The body only builds a
``RememberCommand`` and delegates to the transport-free application service,
which owns the single-transaction ingestion call and the best-effort
``derived_from`` graph block. No business logic and no SQL remain here.
"""

from typing import Any, Optional

from memory_server.admission import AdmissionDecision
from memory_server.application import MemoryService, RememberCommand
from memory_server.providers.graph_provider import SimpleGraph
from memory_server.providers.sqlite_provider import SQLiteProvider


async def remember(
    provider: SQLiteProvider,
    subject: str,
    predicate: str,
    object: str,
    confidence: float = 1.0,
    source: str = "user",
    metadata: Optional[dict[str, Any]] = None,
    admission: AdmissionDecision | None = None,
    graph: SimpleGraph | None = None,
) -> dict:
    """Store a fact and return a provenance receipt.

    Delegates to the application service, which reuses
    ``MemoryIngestionService`` for single-transaction writes (fact + receipt +
    outbox entry are committed atomically).

    Metadata is pass-through except for ``metadata.evidence``, which uses this
    contract when present:

    - method: str
    - sources: list[str]
    - session_id: str
    - confidence: float
    - source_date: str
    - derived_from: list[str]
    - claim_type: fact|authority|state

    Args:
        provider: Initialized SQLiteProvider instance.
        subject: The subject of the fact (required, non-empty).
        predicate: The predicate/relation (required, non-empty).
        object: The object of the fact (required, non-empty).
        confidence: Confidence score 0.0-1.0 (default 1.0).
        source: Source identifier (default "user").
        metadata: Optional extra metadata (stored in receipt history).
        admission: Optional write-time admission/tagging decision. When
            supplied, its metadata is persisted under ``metadata.admission``.
        graph: Optional graph engine for the best-effort ``derived_from`` sync.

    Returns:
        Dict with 'receipt' (MemoryReceipt) and 'fact' (Fact).

    Raises:
        ValueError: If subject, predicate, or object are empty, or
                    confidence is outside [0, 1].
    """
    command = RememberCommand(
        subject=subject,
        predicate=predicate,
        object=object,
        confidence=confidence,
        source=source,
        metadata=metadata,
        admission=admission,
    )
    return await MemoryService(provider._session_factory).remember(command, graph=graph)
