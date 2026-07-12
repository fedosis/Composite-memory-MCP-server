"""Belief model — a proposition the agent holds as true with confidence and evidence."""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field


class Belief(BaseModel):
    """A proposition the agent holds as true, with confidence and source attribution.

    Unlike Facts (SPO triples), a Belief is a free-form proposition with
    explicit confidence, evidence chain, and revision history (integer version).
    """

    model_config = ConfigDict(from_attributes=True)

    id: str = Field(default_factory=lambda: str(__import__("uuid").uuid4()))
    proposition: str = Field(..., min_length=1, max_length=2048)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    source: str = Field(default="system")
    creator: str = Field(default="system")
    source_ids: list[str] = Field(default_factory=list)  # denormalized from Evidence
    tags: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_reinforced_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = Field(default=1, ge=1)  # integer revision counter
    verification_status: str = "candidate"
    lifecycle_state: str = "active"
