"""Ingestion service — transactional boundary for memory writes.

Ensures fact/decision/skill + receipt + outbox entry are committed atomically.
Per CMMS-001: replaces the old pattern of N separate transactions per ingestion
with a single transaction via session.begin().
"""

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from storage.outbox import OutboxRepository
from storage.repositories import (
    DecisionRepository,
    FactRepository,
    ReceiptRepository,
    SkillRepository,
)

from memory_server.extractors.decision_extractor import DecisionExtractor
from memory_server.extractors.fact_extractor import FactExtractor
from memory_server.extractors.skill_extractor import SkillExtractor
from memory_server.models import Decision, Fact, MemoryReceipt, Skill, VerificationStatus


class MemoryIngestionService:
    """Transactional service for memory ingestion (remember + learn).

    All writes within a single call happen inside one database transaction.
    If any part fails, the entire operation rolls back — no orphaned data.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._session_factory = session_factory

    async def remember(
        self,
        subject: str,
        predicate: str,
        object: str,
        confidence: float = 1.0,
        source: str = "user",
        metadata: dict[str, Any] | None = None,
    ) -> dict:
        """Store a fact + receipt + outbox entry in a single transaction.

        Args:
            subject: The subject of the fact (required, non-empty).
            predicate: The predicate/relation (required, non-empty).
            object: The object of the fact (required, non-empty).
            confidence: Confidence score 0.0-1.0 (default 1.0).
            source: Source identifier (default "user").
            metadata: Optional extra metadata (stored in receipt history).

        Returns:
            Dict with 'receipt' (MemoryReceipt) and 'fact' (Fact).

        Raises:
            ValueError: If subject, predicate, or object are empty, or
                        confidence is outside [0, 1].
        """
        if not subject:
            raise ValueError("'subject' is required and cannot be empty")
        if not predicate:
            raise ValueError("'predicate' is required and cannot be empty")
        if not object:
            raise ValueError("'object' is required and cannot be empty")
        if confidence < 0.0 or confidence > 1.0:
            raise ValueError(
                f"'confidence' must be between 0.0 and 1.0, got {confidence}"
            )

        async with self._session_factory() as session:
            async with session.begin():
                now = datetime.now(timezone.utc)
                fact_id = str(uuid4())

                # Create fact
                fact = Fact(
                    id=fact_id,
                    subject=subject,
                    predicate=predicate,
                    object=object,
                    confidence=confidence,
                    source=source,
                    created_at=now,
                )
                fact_repo = FactRepository(session)
                stored_fact = await fact_repo.create(fact)

                # Create receipt
                receipt_history: list[dict[str, Any]] = []
                if metadata:
                    receipt_history.append(
                        {"metadata": metadata, "timestamp": now.isoformat()}
                    )
                receipt = MemoryReceipt(
                    id=fact_id,
                    memory_type="fact",
                    source=source,
                    created_by="user",
                    timestamp=now,
                    confidence=confidence,
                    verification_status=VerificationStatus.CANDIDATE,
                    history=receipt_history,
                )
                receipt_repo = ReceiptRepository(session)
                stored_receipt = await receipt_repo.create(receipt)

                # Write outbox entry (same transaction!)
                outbox_repo = OutboxRepository(session)
                await outbox_repo.add_entry(
                    record_type="fact",
                    record_id=fact_id,
                    operation="index_fact",
                    payload={
                        "subject": subject,
                        "predicate": predicate,
                        "object": object,
                        "source": source,
                    },
                )

            # Transaction committed here by session.begin() context manager
            return {"receipt": stored_receipt, "fact": stored_fact}

    async def learn(
        self,
        text: str,
        source: str = "user",
    ) -> dict:
        """Extract and store facts, decisions, skills in one transaction.

        Runs FactExtractor, DecisionExtractor, and SkillExtractor on the
        input text, then stores all extracted items + receipts + outbox
        entries in a single database transaction.

        Args:
            text: Natural language text to analyze and extract knowledge from.
            source: Source identifier (default "user").

        Returns:
            Dict with keys: facts, decisions, skills, receipts.
        """
        facts_result: list[dict] = []
        decisions_result: list[dict] = []
        skills_result: list[dict] = []
        receipts: list[dict] = []

        if not text or not text.strip():
            return {
                "facts": facts_result,
                "decisions": decisions_result,
                "skills": skills_result,
                "receipts": receipts,
            }

        now = datetime.now(timezone.utc)

        # Run all extractors first (no DB writes yet)
        fact_extractor = FactExtractor()
        extracted_facts = fact_extractor.extract(text)

        decision_extractor = DecisionExtractor()
        extracted_decisions = decision_extractor.extract(text)

        skill_extractor = SkillExtractor()
        extracted_skills = skill_extractor.extract(text)

        # Write everything in one transaction
        async with self._session_factory() as session:
            async with session.begin():
                fact_repo = FactRepository(session)
                decision_repo = DecisionRepository(session)
                skill_repo = SkillRepository(session)
                receipt_repo = ReceiptRepository(session)
                outbox_repo = OutboxRepository(session)

                for ef in extracted_facts:
                    fact_id = str(uuid4())
                    fact = Fact(
                        id=fact_id,
                        subject=ef.get("subject", ""),
                        predicate=ef.get("predicate", ""),
                        object=ef.get("object", ""),
                        confidence=ef.get("confidence", 0.5),
                        source=source,
                        created_at=now,
                    )
                    stored_fact = await fact_repo.create(fact)

                    receipt = MemoryReceipt(
                        id=fact_id,
                        memory_type="fact",
                        source=source,
                        created_by="learn",
                        timestamp=now,
                        confidence=ef.get("confidence", 0.5),
                        verification_status=VerificationStatus.CANDIDATE,
                    )
                    stored_receipt = await receipt_repo.create(receipt)

                    await outbox_repo.add_entry(
                        record_type="fact",
                        record_id=fact_id,
                        operation="index_fact",
                        payload={
                            "subject": ef.get("subject", ""),
                            "predicate": ef.get("predicate", ""),
                            "object": ef.get("object", ""),
                            "source": source,
                        },
                    )

                    facts_result.append({
                        "receipt": stored_receipt.model_dump(mode="json"),
                        "item": stored_fact.model_dump(mode="json"),
                    })
                    receipts.append(stored_receipt.model_dump(mode="json"))

                for ed in extracted_decisions:
                    decision_id = str(uuid4())
                    decision = Decision(
                        id=decision_id,
                        context=ed.get("context", ""),
                        choice=ed.get("choice", ""),
                        rejected_alternatives=ed.get("alternatives", []),
                        reason=ed.get("reason", ""),
                        source=source,
                        created_at=now,
                    )
                    stored_decision = await decision_repo.create(decision)

                    receipt = MemoryReceipt(
                        id=decision_id,
                        memory_type="decision",
                        source=source,
                        created_by="learn",
                        timestamp=now,
                        confidence=ed.get("confidence", 0.5),
                        verification_status=VerificationStatus.CANDIDATE,
                    )
                    stored_receipt = await receipt_repo.create(receipt)

                    await outbox_repo.add_entry(
                        record_type="decision",
                        record_id=decision_id,
                        operation="index_decision",
                        payload={
                            "choice": ed.get("choice", ""),
                            "reason": ed.get("reason", ""),
                            "context": ed.get("context", ""),
                        },
                    )

                    decisions_result.append({
                        "receipt": stored_receipt.model_dump(mode="json"),
                        "item": stored_decision.model_dump(mode="json"),
                    })
                    receipts.append(stored_receipt.model_dump(mode="json"))

                for es in extracted_skills:
                    skill_id = str(uuid4())
                    skill = Skill(
                        id=skill_id,
                        name="",
                        version="1.0.0",
                        purpose=es.get("purpose", ""),
                        steps=es.get("steps", []),
                        constraints=es.get("constraints", []),
                        validation=[],
                        success_rate=es.get("confidence", 0.5),
                        created_at=now,
                    )
                    stored_skill = await skill_repo.create(skill)

                    receipt = MemoryReceipt(
                        id=skill_id,
                        memory_type="skill",
                        source=source,
                        created_by="learn",
                        timestamp=now,
                        confidence=es.get("confidence", 0.5),
                        verification_status=VerificationStatus.CANDIDATE,
                    )
                    stored_receipt = await receipt_repo.create(receipt)

                    await outbox_repo.add_entry(
                        record_type="skill",
                        record_id=skill_id,
                        operation="index_skill",
                        payload={
                            "purpose": es.get("purpose", ""),
                            "steps": es.get("steps", []),
                        },
                    )

                    skills_result.append({
                        "receipt": stored_receipt.model_dump(mode="json"),
                        "item": stored_skill.model_dump(mode="json"),
                    })
                    receipts.append(stored_receipt.model_dump(mode="json"))

            # Transaction committed here by session.begin() context manager
            return {
                "facts": facts_result,
                "decisions": decisions_result,
                "skills": skills_result,
                "receipts": receipts,
            }
