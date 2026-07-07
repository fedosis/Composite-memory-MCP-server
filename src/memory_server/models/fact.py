"""Fact model — a verified statement (subject-predicate-object)."""

from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class Fact(BaseModel):
    """A verified factual statement with subject-predicate-object structure.

    Example: Docker -> runs_on -> OMV8

    Canonical fields per spec: id, type, content, source, creator,
    created_at, updated_at, confidence, verification_status,
    lifecycle_state, version.
    """

    id: str
    subject: str
    predicate: str
    object: str
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    source: Optional[str] = None
    creator: str = "system"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    verification_status: str = "candidate"
    lifecycle_state: str = "active"
    version: str = "0.1.0"

    model_config = ConfigDict(from_attributes=True)
