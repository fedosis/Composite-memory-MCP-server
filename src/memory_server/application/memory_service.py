"""Transport-free, runtime-agnostic application use cases for memory writes.

Reuse, not reimplementation: every write goes through the existing
``MemoryIngestionService`` (``services/ingestion_service.py:275``), which owns
the single transaction (``:322-323``), the dedup key (``:347``), the savepoint
recovery (``:352-367``), the receipt (``:375-386``) and the outbox entry
(``:413-424``). This module contains no SQL, no repository write, no new ORM
model and no second transaction; ``FactRepository`` is used read-only for the
best-effort ``derived_from`` graph block, exactly as the facade's
``provider.get_fact`` did before the move (``sqlite_provider.py:222-225``).

The service is constructed from an explicit ``session_factory`` only. The one
narrow injection is ``max_active_beliefs`` through ``MemoryPolicy``, captured
once at construction; after construction, mutating ``os.environ`` and clearing
the process-global settings cache (``settings.py:390-393``) cannot change this
instance's behaviour. There is no ``get_settings()`` read in deep logic
(ARCHITECTURE_SOURCE §32).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from storage.repositories import FactRepository

from memory_server.admission import MemoryAdmissionGate
from memory_server.application.contracts import (
    BulkImportCommand,
    LearnCommand,
    MemoryPolicy,
    RememberCommand,
    validate_remember_command,
)
from memory_server.services.ingestion_service import MemoryIngestionService

_ALLOWED_CLAIM_TYPES = {"fact", "authority", "state"}


def _normalize_evidence(metadata: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize metadata.evidence while preserving other keys."""
    evidence = metadata.get("evidence")
    if evidence is None:
        return metadata
    if not isinstance(evidence, dict):
        raise ValueError("metadata.evidence must be a dict")

    normalized_evidence = dict(evidence)

    if "derived_from" in normalized_evidence:
        derived_from = normalized_evidence["derived_from"]
        if not isinstance(derived_from, list):
            raise ValueError("metadata.evidence.derived_from must be a list[str]")
        if any(not isinstance(item, str) or not item for item in derived_from):
            raise ValueError("metadata.evidence.derived_from must be a list[str]")
        normalized_evidence["derived_from"] = list(derived_from)

    if "claim_type" in normalized_evidence:
        claim_type = normalized_evidence["claim_type"]
        if not isinstance(claim_type, str):
            raise ValueError("metadata.evidence.claim_type must be one of: fact, authority, state")
        normalized_claim_type = claim_type.strip().lower()
        if normalized_claim_type not in _ALLOWED_CLAIM_TYPES:
            raise ValueError("metadata.evidence.claim_type must be one of: fact, authority, state")
        normalized_evidence["claim_type"] = normalized_claim_type

    metadata["evidence"] = normalized_evidence
    return metadata


def parse_memory_md(path: str | Path) -> list[str]:
    """Extract importable memory entries from a Markdown MEMORY.md file.

    The parser intentionally keeps v0 conservative: bullet/list items become
    candidate memories, headings and fenced code blocks are ignored.

    Moved here verbatim from ``api/bulk_import.py:13-37`` (strangler move, not
    a copy); ``memory_server.api.bulk_import`` re-exports this function so the
    existing import path keeps resolving.
    """
    memory_path = Path(path)
    entries: list[str] = []
    in_fence = False
    for raw_line in memory_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence or not line or line.startswith("#"):
            continue
        if line.startswith(("- ", "* ")):
            entry = line[2:].strip()
        elif len(line) > 40:
            entry = line
        else:
            continue
        if entry:
            entries.append(entry)
    return entries


class MemoryService:
    """Application use cases over the shared session factory.

    Explicit dependencies only: a ``session_factory`` and an optional
    ``MemoryPolicy``. ``policy=None`` captures the ambient policy ONCE, at
    construction, and never again.
    """

    def __init__(self, session_factory: Any, *, policy: MemoryPolicy | None = None) -> None:
        self._session_factory = session_factory
        self._policy = policy if policy is not None else MemoryPolicy.capture()

    @property
    def session_factory(self) -> Any:
        return self._session_factory

    @property
    def policy(self) -> MemoryPolicy:
        return self._policy

    async def _get_fact(self, fact_id: str) -> Any:
        """Read one fact through the shared session factory (read-only).

        Equivalent to the facade's pre-move ``provider.get_fact(fact_id)``:
        the same ``FactRepository`` over the same session factory
        (``sqlite_provider.py:222-225``).
        """
        async with self._session_factory() as session:
            return await FactRepository(session).get(fact_id)

    async def remember(self, command: RememberCommand, *, graph: Any | None = None) -> dict:
        """Store a fact + receipt + outbox entry and sync the graph best-effort.

        Raises:
            InvalidCommandError: If subject, predicate, or object are empty, or
                confidence is outside [0, 1] (a ``ValueError`` subclass).
        """
        validate_remember_command(command)

        receipt_metadata: dict[str, Any] = dict(command.metadata or {})
        receipt_metadata = _normalize_evidence(receipt_metadata)
        if command.admission is not None:
            receipt_metadata["admission"] = command.admission.to_metadata()

        service = MemoryIngestionService(
            self._session_factory,
            max_active_beliefs=self._policy.max_active_beliefs,
        )
        result = await service.remember(
            subject=command.subject,
            predicate=command.predicate,
            object=command.object,
            confidence=command.confidence,
            source=command.source,
            metadata=receipt_metadata or None,
        )

        evidence = receipt_metadata.get("evidence") if receipt_metadata else None
        derived_from = evidence.get("derived_from") if isinstance(evidence, dict) else None
        if derived_from:
            try:
                if graph is None:
                    return result

                fact = result["fact"]
                fact_node_name = f"{fact.subject} {fact.predicate} {fact.object}"

                if graph.get_node(fact.id) is None:
                    graph.add_node(
                        id=fact.id,
                        type="fact",
                        name=fact_node_name,
                        attributes={
                            "subject": fact.subject,
                            "predicate": fact.predicate,
                            "object": fact.object,
                            "confidence": fact.confidence,
                            "source": fact.source,
                        },
                    )

                for parent_id in derived_from:
                    if graph.get_node(parent_id) is None:
                        try:
                            parent_fact = await self._get_fact(parent_id)
                        except Exception:
                            parent_fact = None
                        parent_name = (
                            f"{parent_fact.subject} {parent_fact.predicate} {parent_fact.object}"
                            if parent_fact is not None
                            else parent_id
                        )
                        parent_attrs = {
                            "subject": parent_fact.subject,
                            "predicate": parent_fact.predicate,
                            "object": parent_fact.object,
                            "confidence": parent_fact.confidence,
                            "source": parent_fact.source,
                        } if parent_fact is not None else {}
                        graph.add_node(
                            id=parent_id,
                            type="fact",
                            name=parent_name,
                            attributes=parent_attrs,
                        )

                    edge_exists = any(
                        edge.source_id == fact.id
                        and edge.target_id == parent_id
                        and edge.relation == "derived_from"
                        for edge in graph.search_by_relation("derived_from")
                    )
                    if not edge_exists:
                        graph.add_edge(
                            source_id=fact.id,
                            target_id=parent_id,
                            relation="derived_from",
                        )
            except Exception:
                # Graph sync is best-effort; SQL write has already succeeded.
                pass

        return result

    async def learn(self, command: LearnCommand) -> dict:
        """Extract and store facts/decisions/skills (+ beliefs) for one text.

        The forwarding rule of ``api/learn.py:64-76`` is preserved here: a
        ``llm_*`` kwarg is forwarded to the service IF AND ONLY IF its value is
        not ``None``. Unconditional forwarding of ``None`` stays FORBIDDEN —
        it would degrade the service to its regex fallback instead of its
        ``15.0``/``8000``/``LLM_CONFIDENCE_GATE`` defaults.
        """
        kwargs: dict[str, Any] = {
            "text": command.text,
            "source": command.source,
            "extract_beliefs": command.extract_beliefs,
            "min_belief_confidence": command.min_belief_confidence,
        }
        if command.llm_extractor is not None:
            kwargs["llm_extractor"] = command.llm_extractor
        if command.llm_timeout_seconds is not None:
            kwargs["llm_timeout_seconds"] = command.llm_timeout_seconds
        if command.llm_max_input_chars is not None:
            kwargs["llm_max_input_chars"] = command.llm_max_input_chars
        if command.llm_confidence_gate is not None:
            kwargs["llm_confidence_gate"] = command.llm_confidence_gate

        service = MemoryIngestionService(
            self._session_factory,
            max_active_beliefs=self._policy.max_active_beliefs,
        )
        return await service.learn(**kwargs)

    async def bulk_import(self, command: BulkImportCommand) -> dict[str, Any]:
        """Import durable/important entries from a MEMORY.md file.

        A library use case only — it is NOT published as a tool (no
        ``@mcp.tool``, no schema file, no CLI command, no transport
        registration). Ephemeral entries are skipped before any database write.
        """
        admission_gate = command.gate or MemoryAdmissionGate()
        imported: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []

        for entry in parse_memory_md(command.path):
            decision = admission_gate.classify(entry, source_scope=command.source)
            if not decision.admitted:
                skipped.append({"text": entry, "decision": decision.to_metadata()})
                continue
            result = await self.remember(
                RememberCommand(
                    subject=command.source,
                    predicate="contains",
                    object=entry,
                    confidence=decision.score,
                    source=command.source,
                    admission=decision,
                )
            )
            imported.append({
                "text": entry,
                "fact_id": result["fact"].id,
                "decision": decision.to_metadata(),
            })

        return {
            "imported": len(imported),
            "skipped": len(skipped),
            "items": imported,
            "skipped_items": skipped,
        }
