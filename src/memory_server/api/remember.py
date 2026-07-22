"""MCP tool: remember — store a fact and generate a provenance receipt.

Thin wrapper around MemoryIngestionService.remember() which handles
the entire write (fact + receipt + outbox) in one transaction.
"""

from typing import Any, Optional

from memory_server.admission import AdmissionDecision
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.services.ingestion_service import MemoryIngestionService


async def remember(
    provider: SQLiteProvider,
    subject: str,
    predicate: str,
    object: str,
    confidence: float = 1.0,
    source: str = "user",
    metadata: Optional[dict[str, Any]] = None,
    admission: AdmissionDecision | None = None,
) -> dict:
    """Store a fact and return a provenance receipt.

    Delegates to MemoryIngestionService for single-transaction writes
    (fact + receipt + outbox entry are committed atomically).

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

    Returns:
        Dict with 'receipt' (MemoryReceipt) and 'fact' (Fact).

    Raises:
        ValueError: If subject, predicate, or object are empty, or
                    confidence is outside [0, 1].
    """
    receipt_metadata: dict[str, Any] = dict(metadata or {})
    if admission is not None:
        receipt_metadata["admission"] = admission.to_metadata()

    svc = MemoryIngestionService(provider._session_factory)
    return await svc.remember(
        subject=subject,
        predicate=predicate,
        object=object,
        confidence=confidence,
        source=source,
        metadata=receipt_metadata or None,
    )
