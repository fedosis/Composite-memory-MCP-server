"""Fact model — a verified statement (subject-predicate-object)."""

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


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
    dedup_key: Optional[str] = None
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    source: Optional[str] = None
    creator: str = "system"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    verification_status: str = "candidate"
    lifecycle_state: str = "active"
    version: int = 1

    @field_validator("version", mode="before")
    @classmethod
    def _normalize_version(cls, value: object) -> int:
        """Accept legacy string versions while storing an integer revision."""
        if value is None:
            return 1
        if isinstance(value, int):
            return max(1, value)
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.isdigit():
                return max(1, int(stripped))
            # Legacy semantic-version strings (for example "0.1.0") map to 1.
            return 1
        raise ValueError("version must be an integer or numeric string")

    model_config = ConfigDict(from_attributes=True)
