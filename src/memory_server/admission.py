"""Memory admission gate and write-time tagging for v0.11.

The gate is intentionally rule-first. It separates low-signal ephemeral notes
from durable/important memories and emits structured metadata that later
retrieval/admission layers can use to avoid tool-call drift.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any


class MemoryTag(str, Enum):
    """Coarse retention class assigned before writing memory."""

    EPHEMERAL = "ephemeral"
    DURABLE = "durable"
    IMPORTANT = "important"


_NOISE_PHRASES = {
    "ok",
    "okay",
    "thanks",
    "thank you",
    "thx",
    "yes",
    "no",
    "done",
    "cool",
    "great",
    "got it",
}

_TRANSIENT_TERMS = {
    "temporary",
    "temp",
    "today",
    "tonight",
    "now",
    "scratch",
    "черновик",
    "временно",
    "сегодня",
}

_STYLE_TERMS = {
    "brief",
    "concise",
    "verbose",
    "verbosity",
    "tone",
    "style",
    "format",
    "terminal",
    "markdown",
    "response",
    "responses",
    "кратко",
    "стиль",
    "формат",
}

_WORKFLOW_TERMS = {
    "workflow",
    "process",
    "tdd",
    "test",
    "tests",
    "review",
    "protocol",
    "работать",
    "процесс",
    "ревью",
}

_IMPORTANT_TERMS = {
    "important",
    "critical",
    "always",
    "never",
    "must",
    "do not",
    "don't",
    "cannot",
    "нельзя",
    "важно",
    "обязательно",
    "никогда",
    "всегда",
}

_SECURITY_TERMS = {
    "secret",
    "password",
    "token",
    "api key",
    "credential",
    "security",
    "safety",
    "rollback",
    "logging",
    "approval",
    "permission",
    "delete",
    "destructive",
    "public",
    "private",
}


@dataclass(frozen=True)
class AdmissionDecision:
    """Decision returned by :class:`MemoryAdmissionGate`."""

    admitted: bool
    tag: MemoryTag
    ttl_days: int | None
    score: float
    reason_codes: list[str]
    metadata: dict[str, Any]

    def to_metadata(self) -> dict[str, Any]:
        """Return JSON-serialisable metadata for receipt history."""
        return {
            "admitted": self.admitted,
            "tag": self.tag.value,
            "ttl_days": self.ttl_days,
            "score": self.score,
            "reason_codes": list(self.reason_codes),
            **self.metadata,
        }


class MemoryAdmissionGate:
    """Rule-based write admission and tagging layer.

    v0 deliberately uses deterministic heuristics rather than an opaque model:
    it is auditable, testable, and good enough to prevent obvious MEMDRIFT from
    low-signal or preference-only memories.
    """

    def classify(
        self,
        text: str,
        *,
        source_scope: str = "user",
        now: datetime | None = None,
        force: bool = False,
    ) -> AdmissionDecision:
        """Classify memory text before writing.

        Args:
            text: Candidate memory text.
            source_scope: High-level provenance scope (user/system/import/etc.).
            now: Clock override for tests.
            force: If true, admit otherwise-ephemeral text but keep its tag/TTL.
        """
        timestamp = now or datetime.now(timezone.utc)
        normalized = " ".join(text.strip().split())
        lower = normalized.lower()
        reason_codes: list[str] = []

        tag = MemoryTag.DURABLE
        ttl_days: int | None = 365
        score = 0.65

        if self._is_low_signal(lower) or any(term in lower for term in _TRANSIENT_TERMS):
            tag = MemoryTag.EPHEMERAL
            ttl_days = 1
            score = 0.05
            reason_codes.append("low_signal")
        elif any(term in lower for term in _IMPORTANT_TERMS):
            tag = MemoryTag.IMPORTANT
            ttl_days = None
            score = 0.95
            reason_codes.append("explicit_importance")
        else:
            reason_codes.append("durable_signal")

        memory_kind = self._memory_kind(lower, tag)
        risk_tags = self._risk_tags(lower)
        admission_tags = self._admission_tags(memory_kind, tag, risk_tags)
        authority_level = self._authority_level(memory_kind, tag)
        epistemic_status = "migrated/imported" if source_scope in {"import", "MEMORY.md"} else "explicit_user_statement"
        expires_at = None
        if ttl_days is not None:
            expires_at = (timestamp + timedelta(days=ttl_days)).isoformat()

        admitted = force or tag is not MemoryTag.EPHEMERAL
        metadata = {
            "memory_kind": memory_kind,
            "epistemic_status": epistemic_status,
            "authority_level": authority_level,
            "state_status": "active",
            "source_scope": source_scope,
            "risk_tags": risk_tags,
            "admission_tags": admission_tags,
            "valid_from": timestamp.isoformat(),
            "valid_to": expires_at,
            "expires_at": expires_at,
            "confidence": score,
        }
        return AdmissionDecision(
            admitted=admitted,
            tag=tag,
            ttl_days=ttl_days,
            score=score,
            reason_codes=reason_codes,
            metadata=metadata,
        )

    @staticmethod
    def _is_low_signal(lower: str) -> bool:
        if not lower:
            return True
        stripped = lower.strip(" .!?,;:-")
        if stripped in _NOISE_PHRASES:
            return True
        words = stripped.replace(",", " ").split()
        return len(words) <= 3 and all(word in _NOISE_PHRASES for word in words)

    @staticmethod
    def _memory_kind(lower: str, tag: MemoryTag) -> str:
        if tag is MemoryTag.IMPORTANT and any(term in lower for term in _SECURITY_TERMS):
            return "system_policy"
        if "prefer" in lower or "prefers" in lower or "предпоч" in lower:
            if any(term in lower for term in _STYLE_TERMS):
                return "user_preference_style"
            if any(term in lower for term in _WORKFLOW_TERMS):
                return "user_preference_workflow"
            return "user_preference"
        if tag is MemoryTag.EPHEMERAL:
            return "episodic_observation"
        if any(term in lower for term in {"project", "repo", "docker", "python", "postgres", "sqlite"}):
            return "project_fact"
        return "user_constraint_explicit" if tag is MemoryTag.IMPORTANT else "summary_observation"

    @staticmethod
    def _risk_tags(lower: str) -> list[str]:
        tags: list[str] = []
        if any(term in lower for term in {"approval", "permission", "confirm", "ask"}):
            tags.append("approval_sensitive")
        if any(term in lower for term in {"public", "private", "share", "visibility"}):
            tags.append("visibility_sensitive")
        if any(term in lower for term in {"delete", "destructive", "overwrite", "reset"}):
            tags.append("destructive_sensitive")
        if any(
            term in lower for term in {"secret", "password", "token", "api key", "credential", "security", "safety"}
        ):
            tags.append("security_sensitive")
        if any(term in lower for term in {"logging", "rollback"}):
            tags.append("logging_sensitive")
            if "security_sensitive" not in tags:
                tags.append("security_sensitive")
        return tags

    @staticmethod
    def _admission_tags(memory_kind: str, tag: MemoryTag, risk_tags: list[str]) -> list[str]:
        if tag is MemoryTag.EPHEMERAL:
            return ["answer_content_ok"]
        if memory_kind == "user_preference_style":
            return ["style_only", "answer_content_ok"]
        tags = ["answer_content_ok", "planning_ok", "memory_write_ok"]
        if tag is MemoryTag.IMPORTANT and not risk_tags:
            tags.append("tool_parameter_ok")
        return tags

    @staticmethod
    def _authority_level(memory_kind: str, tag: MemoryTag) -> str:
        if tag is MemoryTag.EPHEMERAL:
            return "tentative_inference"
        if memory_kind.startswith("user_preference"):
            return "confirmed_user_preference"
        if memory_kind == "project_fact":
            return "stable_project_fact"
        if memory_kind == "system_policy":
            return "authoritative_user_constraint"
        return "tentative_inference"
