"""Validator — verification lifecycle management for memory items.

VerificationStatus lifecycle::

    candidate → validated → trusted
         ↓                        ↓
    deprecated → archived    deprecated → archived
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from memory_server.evaluation.confidence import ConfidenceEngine
from memory_server.models.receipt import VerificationStatus


class Validator:
    """Manages the verification lifecycle for facts and memory items.

    Each fact starts as ``candidate`` after ingestion.  The ``validate()``
    method promotes it to ``validated`` if its confidence meets the
    threshold.  ``trust()`` further promotes to ``trusted`` when
    confidence is high *and* corroboration is sufficient.
    """

    def __init__(
        self,
        confidence_engine: ConfidenceEngine | None = None,
        validate_threshold: float = 0.7,
        trust_threshold: float = 0.85,
        trust_corroboration_min: int = 2,
    ) -> None:
        self._engine = confidence_engine or ConfidenceEngine()
        self._validate_threshold = validate_threshold
        self._trust_threshold = trust_threshold
        self._trust_corroboration_min = trust_corroboration_min

        # In-memory status store: {fact_id: {status, history, …}}
        self._store: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Lifecycle transitions
    # ------------------------------------------------------------------

    def register(
        self,
        fact_id: str,
        initial_status: VerificationStatus = VerificationStatus.CANDIDATE,
        confidence: float = 0.5,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Register a new fact in the validator's in-memory store.

        Called automatically when a fact is created via ``remember()``
        or ``learn()``.
        """
        now = datetime.now(timezone.utc)
        entry: dict[str, Any] = {
            "fact_id": fact_id,
            "status": initial_status,
            "confidence": confidence,
            "history": [
                {
                    "status": initial_status.value,
                    "timestamp": now.isoformat(),
                    "note": "Created",
                }
            ],
            "corroboration_count": 0,
            "conflict_count": 0,
            "created_at": now,
            "updated_at": now,
        }
        if metadata:
            entry.update(metadata)
        self._store[fact_id] = entry

    def validate(self, fact_id: str) -> VerificationStatus:
        """Promote a candidate to validated if confidence >= threshold.

        Returns the new status.
        """
        entry = self._get_entry(fact_id)
        if entry["status"] not in (
            VerificationStatus.CANDIDATE,
            VerificationStatus.UNVERIFIED,
        ):
            return entry["status"]

        if entry["confidence"] >= self._validate_threshold:
            return self._transition(fact_id, VerificationStatus.VALIDATED, "Validated")
        return entry["status"]

    def trust(self, fact_id: str) -> VerificationStatus:
        """Promote validated to trusted if confidence >= 0.85 AND corroboration >= 2.

        Returns the new status.
        """
        entry = self._get_entry(fact_id)
        if entry["status"] != VerificationStatus.VALIDATED:
            return entry["status"]

        if (
            entry["confidence"] >= self._trust_threshold
            and entry.get("corroboration_count", 0) >= self._trust_corroboration_min
        ):
            return self._transition(fact_id, VerificationStatus.TRUSTED, "Trusted")
        return entry["status"]

    def deprecate(self, fact_id: str, reason: str = "Conflict resolution") -> VerificationStatus:
        """Mark a fact as deprecated (e.g., when conflict resolved against it).

        Returns the new status.
        """
        entry = self._get_entry(fact_id)
        if entry["status"] in (
            VerificationStatus.DEPRECATED,
            VerificationStatus.ARCHIVED,
        ):
            return entry["status"]
        return self._transition(fact_id, VerificationStatus.DEPRECATED, reason)

    def archive(self, fact_id: str) -> VerificationStatus:
        """Move a deprecated fact to archived.

        Returns the new status.
        """
        entry = self._get_entry(fact_id)
        if entry["status"] != VerificationStatus.DEPRECATED:
            return entry["status"]
        return self._transition(fact_id, VerificationStatus.ARCHIVED, "Archived")

    def get_status(self, fact_id: str) -> dict[str, Any]:
        """Return the current status and full history for a fact.

        Returns a dict with keys: ``fact_id``, ``status``, ``confidence``,
        ``history``, ``created_at``, ``updated_at``.
        """
        entry = self._get_entry(fact_id)
        status_val = (
            entry["status"].value
            if isinstance(entry["status"], VerificationStatus)
            else entry["status"]
        )
        return {
            "fact_id": entry["fact_id"],
            "status": status_val,
            "confidence": entry["confidence"],
            "history": entry["history"],
            "created_at": entry.get("created_at"),
            "updated_at": entry.get("updated_at"),
        }

    def set_confidence(self, fact_id: str, confidence: float) -> None:
        """Update the confidence score for a fact."""
        entry = self._get_entry(fact_id)
        entry["confidence"] = max(0.0, min(1.0, confidence))
        entry["updated_at"] = datetime.now(timezone.utc)

    def set_corroboration_count(self, fact_id: str, count: int) -> None:
        """Update the corroboration count."""
        entry = self._get_entry(fact_id)
        entry["corroboration_count"] = count
        entry["updated_at"] = datetime.now(timezone.utc)

    def set_conflict_count(self, fact_id: str, count: int) -> None:
        """Update the conflict count."""
        entry = self._get_entry(fact_id)
        entry["conflict_count"] = count
        entry["updated_at"] = datetime.now(timezone.utc)

    def get_all(self) -> list[dict[str, Any]]:
        """Return all registered entries."""
        return [
            {
                "fact_id": entry["fact_id"],
                "status": (
                    entry["status"].value
                    if isinstance(entry["status"], VerificationStatus)
                    else entry["status"]
                ),
                "confidence": entry["confidence"],
                "corroboration_count": entry.get("corroboration_count", 0),
                "conflict_count": entry.get("conflict_count", 0),
            }
            for entry in self._store.values()
        ]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_entry(self, fact_id: str) -> dict[str, Any]:
        """Get the entry or raise KeyError."""
        if fact_id not in self._store:
            raise KeyError(f"Fact '{fact_id}' not registered")
        return self._store[fact_id]

    def _transition(
        self,
        fact_id: str,
        new_status: VerificationStatus,
        note: str,
    ) -> VerificationStatus:
        """Record a status transition in the history."""
        entry = self._store[fact_id]
        entry["status"] = new_status
        entry["updated_at"] = datetime.now(timezone.utc)
        entry["history"].append(
            {
                "status": new_status.value,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "note": note,
            }
        )
        return new_status
