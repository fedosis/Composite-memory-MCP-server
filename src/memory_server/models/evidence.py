"""Evidence model — a supporting piece of evidence for a belief."""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field


class Evidence(BaseModel):
    """A piece of evidence supporting a belief, with source attribution and weight."""

    model_config = ConfigDict(from_attributes=True)

    id: str = Field(default_factory=lambda: str(__import__("uuid").uuid4()))
    belief_id: str = Field(...)
    source_type: str = Field(...)          # "fact" | "decision" | "observation" | "user_statement"
    source_id: str = Field(...)
    weight: float = Field(default=0.5, ge=0.0, le=1.0)
    contributor: str = Field(default="system")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    note: str | None = None
