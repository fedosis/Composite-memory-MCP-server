"""Validator — verification lifecycle management for memory items.

LifecycleState (v0.6 spec)::

    candidate → validated → active → stale → archived → forgotten

Each state is terminal for backward transitions — once promoted,
an item can only move forward in the lifecycle.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from memory_server.evaluation.confidence import ConfidenceEngine
from memory_server.models.receipt import LifecycleState, VerificationStatus

# Valid forward transitions — any from→to pair not in this mapping is invalid
_VALID_TRANSITIONS: dict[str, set[str]] = {
    "candidate": {"validated"},
    "validated": {"active"},
    "active": {"stale", "superseded", "contradicted", "discarded"},
    "stale": {"archived"},
    "archived": {"forgotten"},
    "forgotten": set(),  # terminal
    # Belief-specific transitions
    "superseded": {"stale", "discarded"},
    "contradicted": {"active", "stale", "discarded"},
    "discarded": {"archived"},
}

# Backward compatibility: old values that appear in stored data
_OLD_VALUE_MAP: dict[str, str] = {
    "trusted": "active",
    "deprecated": "stale",
}


def normalize_lifecycle_state(state: str) -> str:
    """Normalize a lifecycle state string, handling old values.

    Old v0.5 values are mapped:
        "trusted"    → "active"
        "deprecated" → "stale"
    """
    return _OLD_VALUE_MAP.get(state, state)


def is_valid_transition(from_state: str, to_state: str) -> bool:
    """Check if a lifecycle state transition is valid.

    Args:
        from_state: Current lifecycle state (old values auto-normalized).
        to_state: Desired target state.

    Returns:
        True if the transition is allowed.
    """
    normalized_from = normalize_lifecycle_state(from_state)
    allowed = _VALID_TRANSITIONS.get(normalized_from, set())
    return to_state in allowed


class Validator:
    """Manages the verification lifecycle for facts and memory items.

    Each fact starts as ``candidate`` after ingestion.  The ``validate()``
    method promotes it to ``validated`` if its confidence meets the
    threshold.  ``activate()`` further promotes to ``active`` when
    confidence is high *and* corroboration is sufficient.

    The decay engine drives ``active → stale → archived → forgotten``
    transitions via ``mark_stale()``, ``archive()``, and ``forget()``.
    """

    def __init__(
        self,
        confidence_engine: ConfidenceEngine | None = None,
        validate_threshold: float = 0.7,
        activate_threshold: float = 0.85,
        activate_corroboration_min: int = 2,
    ) -> None:
        self._engine = confidence_engine or ConfidenceEngine()
        self._validate_threshold = validate_threshold
        self._activate_threshold = activate_threshold
        self._activate_corroboration_min = activate_corroboration_min

        # In-memory status store: {fact_id: {status, history, …}}
        self._store: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Lifecycle transitions
    # ------------------------------------------------------------------

    def register(
        self,
        fact_id: str,
        initial_status: LifecycleState = LifecycleState.CANDIDATE,
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

    def validate(self, fact_id: str) -> LifecycleState:
        """Promote a candidate to validated if confidence >= threshold.

        Returns the new state.
        """
        entry = self._get_entry(fact_id)
        current = normalize_lifecycle_state(entry["status"])
        if current not in ("candidate",):
            # Already past candidate — no op
            return entry["status"]

        if entry["confidence"] >= self._validate_threshold:
            return self._transition(fact_id, LifecycleState.VALIDATED, "Validated")
        return entry["status"]

    def activate(self, fact_id: str) -> LifecycleState:
        """Promote validated to active if confidence >= 0.85 AND corroboration >= 2.

        Replaces old ``trust()`` — "trusted" maps to "active" for backward compat.

        Returns the new state.
        """
        entry = self._get_entry(fact_id)
        current = normalize_lifecycle_state(entry["status"])
        if current != "validated":
            return entry["status"]

        if (
            entry["confidence"] >= self._activate_threshold
            and entry.get("corroboration_count", 0) >= self._activate_corroboration_min
        ):
            return self._transition(fact_id, LifecycleState.ACTIVE, "Activated")
        return entry["status"]

    def mark_stale(self, fact_id: str, reason: str = "Decay threshold reached") -> LifecycleState:
        """Promote active to stale (triggered by decay engine).

        Returns the new state.
        """
        entry = self._get_entry(fact_id)
        current = normalize_lifecycle_state(entry["status"])
        if current != "active":
            return entry["status"]
        return self._transition(fact_id, LifecycleState.STALE, reason)

    def archive(self, fact_id: str, reason: str = "TTL expired") -> LifecycleState:
        """Promote stale to archived (triggered by TTL expiry).

        Returns the new state.
        """
        entry = self._get_entry(fact_id)
        current = normalize_lifecycle_state(entry["status"])
        if current != "stale":
            return entry["status"]
        return self._transition(fact_id, LifecycleState.ARCHIVED, reason)

    def forget(self, fact_id: str, reason: str = "Extended TTL expired") -> LifecycleState:
        """Promote archived to forgotten (after additional TTL).

        Returns the new state.
        """
        entry = self._get_entry(fact_id)
        current = normalize_lifecycle_state(entry["status"])
        if current != "archived":
            return entry["status"]
        return self._transition(fact_id, LifecycleState.FORGOTTEN, reason)

    def deprecate(self, fact_id: str, reason: str = "Conflict resolution") -> LifecycleState:
        """Mark a fact as stale (e.g., when conflict resolved against it).

        Backward-compatible wrapper — old ``deprecated`` state maps to ``stale``.

        This is a forced transition — any state before ``stale`` is moved
        to ``stale`` directly (bypassing normal transition validation),
        matching the old behavior where ``deprecate`` was a jump from any state.

        Returns the new state.
        """
        entry = self._get_entry(fact_id)
        current = normalize_lifecycle_state(entry["status"])
        # If already at stale or further, no op
        stale_idx = list(LifecycleState).index(LifecycleState.STALE)
        current_idx = list(LifecycleState).index(LifecycleState(current))
        if current_idx >= stale_idx:
            return LifecycleState(current)
        # Force transition — bypasses normal transition validation
        return self._force_transition(fact_id, LifecycleState.STALE, reason)

    # ------------------------------------------------------------------
    # Query methods
    # ------------------------------------------------------------------

    def get_status(self, fact_id: str) -> dict[str, Any]:
        """Return the current status and full history for a fact.

        Returns a dict with keys: ``fact_id``, ``status`` (normalized),
        ``confidence``, ``history``, ``created_at``, ``updated_at``.
        """
        entry = self._get_entry(fact_id)
        raw = entry["status"]
        normalized = normalize_lifecycle_state(
            raw.value if isinstance(raw, (LifecycleState, VerificationStatus)) else raw
        )
        return {
            "fact_id": entry["fact_id"],
            "status": normalized,
            "confidence": entry["confidence"],
            "history": entry["history"],
            "created_at": entry.get("created_at"),
            "updated_at": entry.get("updated_at"),
        }

    def get_all(self) -> list[dict[str, Any]]:
        """Return all registered entries with normalized states."""
        return [
            {
                "fact_id": entry["fact_id"],
                "status": normalize_lifecycle_state(
                    entry["status"].value
                    if isinstance(entry["status"], (LifecycleState, VerificationStatus))
                    else entry["status"]
                ),
                "confidence": entry["confidence"],
                "corroboration_count": entry.get("corroboration_count", 0),
                "conflict_count": entry.get("conflict_count", 0),
            }
            for entry in self._store.values()
        ]

    # ------------------------------------------------------------------
    # Confidence / corroboration mutation
    # ------------------------------------------------------------------

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
        new_state: LifecycleState,
        note: str,
    ) -> LifecycleState:
        """Record a state transition in the history.

        Validates the transition before applying.
        """
        entry = self._store[fact_id]
        current = normalize_lifecycle_state(
            entry["status"].value
            if isinstance(entry["status"], (LifecycleState, VerificationStatus))
            else entry["status"]
        )

        if not is_valid_transition(current, new_state.value):
            raise ValueError(
                f"Invalid transition: {current} → {new_state.value}. "
                f"Allowed from {current}: {sorted(_VALID_TRANSITIONS.get(current, set()))}"
            )

        entry["status"] = new_state
        entry["updated_at"] = datetime.now(timezone.utc)
        entry["history"].append(
            {
                "status": new_state.value,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "note": note,
            }
        )
        return new_state

    def _force_transition(
        self,
        fact_id: str,
        new_state: LifecycleState,
        note: str,
    ) -> LifecycleState:
        """Record a state transition in history WITHOUT validation.

        Used for forced transitions like ``deprecate()`` where the
        caller wants to jump to a state regardless of normal rules.
        """
        entry = self._store[fact_id]
        entry["status"] = new_state
        entry["updated_at"] = datetime.now(timezone.utc)
        entry["history"].append(
            {
                "status": new_state.value,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "note": note,
            }
        )
        return new_state

    def trust(self, fact_id: str) -> LifecycleState:
        """Backward-compatible wrapper — old ``trust()`` now calls ``activate()``.

        ``trusted`` maps to ``active`` in the v0.6 lifecycle.
        """
        return self.activate(fact_id)
