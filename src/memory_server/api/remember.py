"""MCP tool: remember — store a fact and generate a provenance receipt.

Wraps SQLiteProvider.create_fact with input validation and receipt
generation. Returns both the stored fact and the receipt.
"""

from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from memory_server.models import Fact, MemoryReceipt, VerificationStatus
from memory_server.providers.sqlite_provider import SQLiteProvider


async def remember(
    provider: SQLiteProvider,
    subject: str,
    predicate: str,
    object: str,
    confidence: float = 1.0,
    source: str = "user",
    metadata: Optional[dict[str, Any]] = None,
) -> dict:
    """Store a fact and return a provenance receipt.

    Args:
        provider: Initialized SQLiteProvider instance.
        subject: The subject of the fact (required, non-empty).
        predicate: The predicate/relation (required, non-empty).
        object: The object of the fact (required, non-empty).
        confidence: Confidence score 0.0-1.0 (default 1.0).
        source: Source identifier (default "user").
        metadata: Optional extra metadata (stored in receipt history).

    Returns:
        Dict with 'receipt' (MemoryReceipt) and 'fact' (Fact).

    Raises:
        ValueError: If subject, predicate, or object are empty, or
                    confidence is outside [0, 1].
    """
    # --- Validation ---
    if not subject:
        raise ValueError("'subject' is required and cannot be empty")
    if not predicate:
        raise ValueError("'predicate' is required and cannot be empty")
    if not object:
        raise ValueError("'object' is required and cannot be empty")
    if confidence < 0.0 or confidence > 1.0:
        raise ValueError(
            f"'confidence' must be between 0.0 and 1.0, got {confidence}"
        )

    # --- Create Fact ---
    now = datetime.now(timezone.utc)
    fact_id = str(uuid4())
    fact = Fact(
        id=fact_id,
        subject=subject,
        predicate=predicate,
        object=object,
        confidence=confidence,
        source=source,
        created_at=now,
    )
    stored_fact = await provider.create_fact(fact)

    # --- Create Receipt ---
    receipt_history = []
    if metadata:
        receipt_history.append({"metadata": metadata, "timestamp": now.isoformat()})

    receipt = MemoryReceipt(
        id=fact_id,
        memory_type="fact",
        source=source,
        created_by="user",
        timestamp=now,
        confidence=confidence,
        verification_status=VerificationStatus.CANDIDATE,
        history=receipt_history,
    )
    stored_receipt = await provider.create_receipt(receipt)

    return {
        "receipt": stored_receipt,
        "fact": stored_fact,
    }
