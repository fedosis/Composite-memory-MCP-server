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
    """

    id: str
    memory_type: str
    source: str
    created_by: str
    timestamp: datetime
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    verification_status: VerificationStatus = VerificationStatus.UNVERIFIED
    history: list[Any] = []

    model_config = ConfigDict(from_attributes=True)
