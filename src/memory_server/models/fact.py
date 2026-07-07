"""Fact model — a verified statement (subject-predicate-object)."""

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class Fact(BaseModel):
    """A verified factual statement with subject-predicate-object structure.

    Example: Docker -> runs_on -> OMV8
    """

    id: str
    subject: str
    predicate: str
    object: str
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    source: Optional[str] = None
    created_at: datetime = datetime.now(timezone.utc)

    model_config = ConfigDict(from_attributes=True)
