"""MemoryReceipt model — provenance metadata for every memory operation."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class VerificationStatus(str, Enum):
    """Verification status of a memory entry."""

    UNVERIFIED = "unverified"
    CANDIDATE = "candidate"
    VALIDATED = "validated"
    TRUSTED = "trusted"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


class MemoryReceipt(BaseModel):
    """Provenance receipt for every memory operation.

    Per ADR-008: every memory object must carry source, creator,
    timestamp, confidence, verification status, and history.

    Canonical fields per spec: id, type, content, source, creator,
    created_at, updated_at, confidence, verification_status,
    lifecycle_state, version.
    """

    id: str
    memory_type: str
    source: str
    created_by: str
    timestamp: datetime
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    verification_status: VerificationStatus = VerificationStatus.UNVERIFIED
    history: list[Any] = []
    updated_at: datetime = Field(default_factory=lambda: datetime.now())
    lifecycle_state: str = "active"
    version: str = "0.1.0"

    model_config = ConfigDict(from_attributes=True)
