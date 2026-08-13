"""MCP tool: remember — store a fact and generate a provenance receipt.

Thin wrapper around MemoryIngestionService.remember() which handles
the entire write (fact + receipt + outbox) in one transaction.
"""

from typing import Any, Optional

from memory_server.admission import AdmissionDecision
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.services.ingestion_service import MemoryIngestionService

_ALLOWED_CLAIM_TYPES = {"fact", "authority", "state"}


def _normalize_evidence(metadata: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize metadata.evidence while preserving other keys."""
    evidence = metadata.get("evidence")
    if evidence is None:
        return metadata
    if not isinstance(evidence, dict):
        raise ValueError("metadata.evidence must be a dict")

    normalized_evidence = dict(evidence)

    if "derived_from" in normalized_evidence:
        derived_from = normalized_evidence["derived_from"]
        if not isinstance(derived_from, list):
            raise ValueError("metadata.evidence.derived_from must be a list[str]")
        if any(not isinstance(item, str) or not item for item in derived_from):
            raise ValueError("metadata.evidence.derived_from must be a list[str]")
        normalized_evidence["derived_from"] = list(derived_from)

    if "claim_type" in normalized_evidence:
        claim_type = normalized_evidence["claim_type"]
        if not isinstance(claim_type, str):
            raise ValueError("metadata.evidence.claim_type must be one of: fact, authority, state")
        normalized_claim_type = claim_type.strip().lower()
        if normalized_claim_type not in _ALLOWED_CLAIM_TYPES:
            raise ValueError("metadata.evidence.claim_type must be one of: fact, authority, state")
        normalized_evidence["claim_type"] = normalized_claim_type

    metadata["evidence"] = normalized_evidence
    return metadata


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

    Returns:
        Dict with 'receipt' (MemoryReceipt) and 'fact' (Fact).

    Raises:
        ValueError: If subject, predicate, or object are empty, or
                    confidence is outside [0, 1].
    """
    receipt_metadata: dict[str, Any] = dict(metadata or {})
    receipt_metadata = _normalize_evidence(receipt_metadata)
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
