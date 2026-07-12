"""Ingestion service — transactional boundary for memory writes.

Ensures fact/decision/skill + receipt + outbox entry are committed atomically.
Per CMMS-001: replaces the old pattern of N separate transactions per ingestion
with a single transaction via session.begin().

Card 003: Learn-to-Belief Bridge — optional belief extraction after the
main transaction, using a separate session for belief CRUD.
"""

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from storage.outbox import OutboxRepository
from storage.repositories import (
    BeliefRepository,
    DecisionRepository,
    EvidenceRepository,
    FactRepository,
    ReceiptRepository,
    SkillRepository,
)

from memory_server.extractors.belief_extractor import BeliefExtractor
from memory_server.extractors.decision_extractor import DecisionExtractor
from memory_server.extractors.fact_extractor import FactExtractor
from memory_server.extractors.skill_extractor import SkillExtractor
from memory_server.models import (
    Belief,
    Decision,
    Evidence,
    Fact,
    MemoryReceipt,
    Skill,
    VerificationStatus,
)

logger = logging.getLogger(__name__)

# Soft limit: maximum active beliefs before extraction is skipped
MAX_ACTIVE_BELIEFS = 500


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
        extract_beliefs: bool = False,
        min_belief_confidence: float = 0.6,
    ) -> dict:
        """Extract and store facts, decisions, skills in one transaction.

        Runs FactExtractor, DecisionExtractor, and SkillExtractor on the
        input text, then stores all extracted items + receipts + outbox
        entries in a single database transaction.

        When extract_beliefs=True, also runs BeliefExtractor AFTER the
        main transaction (outside its scope) and creates or reinforces
        beliefs with evidence linked to extracted facts.

        Args:
            text: Natural language text to analyze and extract knowledge from.
            source: Source identifier (default "user").
            extract_beliefs: If True, also extract and store beliefs (default False).
            min_belief_confidence: Minimum confidence to create a belief (default 0.6).

        Returns:
            Dict with keys: facts, decisions, skills, beliefs, receipts.
        """
        facts_result: list[dict] = []
        decisions_result: list[dict] = []
        skills_result: list[dict] = []
        beliefs_result: list[dict] = []
        receipts: list[dict] = []

        if not text or not text.strip():
            return {
                "facts": facts_result,
                "decisions": decisions_result,
                "skills": skills_result,
                "beliefs": beliefs_result,
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

                created_facts: list[Fact] = []

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
                    created_facts.append(stored_fact)

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

        # --- Belief extraction (outside the main transaction) ---
        if extract_beliefs:
            try:
                beliefs_result = await self._extract_and_store_beliefs(
                    text=text,
                    source=source,
                    min_belief_confidence=min_belief_confidence,
                    created_facts=created_facts,
                )
            except Exception:
                logger.exception("Belief extraction failed — learn() results still returned")
                # Don't fail — existing learn() results are still valid

        return {
            "facts": facts_result,
            "decisions": decisions_result,
            "skills": skills_result,
            "beliefs": beliefs_result,
            "receipts": receipts,
        }

    async def _extract_and_store_beliefs(
        self,
        text: str,
        source: str,
        min_belief_confidence: float,
        created_facts: list[Fact],
    ) -> list[dict]:
        """Extract beliefs from text and store/reinforce them.

        Runs outside the main learn() transaction. Uses its own session
        for belief CRUD. Skips if soft limit (500 active beliefs) is reached.
        """
        beliefs_result: list[dict] = []

        async with self._session_factory() as session:
            belief_repo = BeliefRepository(session)
            ev_repo = EvidenceRepository(session)

            # 1. Check soft limit
            active_beliefs = await belief_repo.search(
                lifecycle_state="active",
                limit=0,
            )
            active_count = len(active_beliefs) if active_beliefs else 0
            if active_count >= MAX_ACTIVE_BELIEFS:
                logger.warning(
                    "Active beliefs (%s) at limit (%s): skipping belief extraction",
                    active_count,
                    MAX_ACTIVE_BELIEFS,
                )
                return beliefs_result

            # 2. Build content-based evidence linking map
            proposition_to_fact_id: dict[str, str] = {}
            for f in created_facts:
                key = f"{f.subject} {f.predicate} {f.object}"
                proposition_to_fact_id[key] = f.id

            # 3. Run BeliefExtractor
            extractor = BeliefExtractor(llm_extractor=None)  # No LLM in service layer
            extracted = await extractor.extract(text)

            # 4. Process each extracted belief
            for eb in extracted:
                if eb.confidence < min_belief_confidence:
                    continue

                # Check for existing active belief (case-insensitive exact match)
                existing = await belief_repo.search(
                    proposition=eb.proposition,
                    lifecycle_state="active",
                    limit=100,
                )
                match = _find_exact_match(existing, eb.proposition)

                if match:
                    # Reinforcement: weighted average confidence
                    new_confidence = max(
                        0.0,
                        min(
                            1.0,
                            (match.confidence * match.version + eb.confidence)
                            / (match.version + 1),
                        ),
                    )
                    await belief_repo.update_confidence(match.id, new_confidence)
                    await belief_repo.update_reinforced_at(match.id)
                    await belief_repo.increment_version(match.id)

                    # Link evidence to reinforced belief
                    actual_source_ids = [
                        proposition_to_fact_id[ref]
                        for ref in eb.source_refs
                        if ref in proposition_to_fact_id
                    ]
                    for fid in actual_source_ids:
                        ev = Evidence(
                            belief_id=match.id,
                            source_type="fact",
                            source_id=fid,
                            weight=eb.confidence,
                            contributor=source,
                        )
                        await ev_repo.create(ev)

                    await session.commit()

                    beliefs_result.append({
                        "belief": match.model_dump(mode="json"),
                        "extracted": eb.model_dump(mode="json"),
                        "reinforced": True,
                    })
                else:
                    # Create new belief
                    belief = Belief(
                        proposition=eb.proposition,
                        confidence=eb.confidence,
                        source=source,
                        tags=eb.tags,
                        creator=source,
                    )

                    # Build evidence from linked facts
                    actual_source_ids = [
                        proposition_to_fact_id[ref]
                        for ref in eb.source_refs
                        if ref in proposition_to_fact_id
                    ]
                    evidence_list = [
                        Evidence(
                            belief_id=belief.id,
                            source_type="fact",
                            source_id=fid,
                            weight=eb.confidence,
                            contributor=source,
                        )
                        for fid in actual_source_ids
                    ]

                    # Create receipt
                    receipt = MemoryReceipt(
                        id=belief.id,
                        memory_type="belief",
                        source=source,
                        created_by="learn",
                        timestamp=datetime.now(timezone.utc),
                        confidence=eb.confidence,
                    )

                    # Store belief + evidence + receipt + outbox in one transaction
                    await belief_repo.create(belief)
                    for ev in evidence_list:
                        ev.belief_id = belief.id
                        await ev_repo.create(ev)

                    receipt_repo = ReceiptRepository(session)
                    await receipt_repo.create(receipt)

                    # Write outbox entry for async indexing
                    outbox_repo = OutboxRepository(session)
                    await outbox_repo.add_entry(
                        record_type="belief",
                        record_id=belief.id,
                        operation="index_belief",
                        payload={
                            "proposition": eb.proposition,
                            "tags": eb.tags,
                            "confidence": eb.confidence,
                            "source": source,
                        },
                    )

                    await session.commit()

                    beliefs_result.append({
                        "belief": belief.model_dump(mode="json"),
                        "extracted": eb.model_dump(mode="json"),
                        "reinforced": False,
                    })

        return beliefs_result


def _find_exact_match(beliefs: list, proposition: str):
    """Case-insensitive exact match for proposition in belief list."""
    norm = proposition.strip().lower()
    for b in beliefs:
        if b.proposition.strip().lower() == norm and b.lifecycle_state == "active":
            return b
    return None
