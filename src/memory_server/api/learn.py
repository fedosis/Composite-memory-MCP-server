"""MCP tool: learn — extract and store facts, decisions, and skills from free text.

Runs FactExtractor, DecisionExtractor, and SkillExtractor on the input
text, then stores all extracted items via SQLiteProvider. Returns
structured results with receipts per item type.
"""

from datetime import datetime, timezone
from uuid import uuid4

from memory_server.extractors.decision_extractor import DecisionExtractor
from memory_server.extractors.fact_extractor import FactExtractor
from memory_server.extractors.skill_extractor import SkillExtractor
from memory_server.models import Decision, Fact, MemoryReceipt, Skill, VerificationStatus
from memory_server.providers.sqlite_provider import SQLiteProvider


async def learn(
    provider: SQLiteProvider,
    text: str,
    source: str = "user",
) -> dict:
    """Extract facts, decisions, and skills from natural language text and store them.

    Args:
        provider: Initialized SQLiteProvider instance.
        text: Natural language text to analyze and extract knowledge from.
        source: Source identifier (default "user").

    Returns:
        Dict with keys:
            - facts: list of {receipt, item} for extracted facts
            - decisions: list of {receipt, item} for extracted decisions
            - skills: list of {receipt, item} for extracted skills
            - receipts: flat list of all receipts
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

    # --- Extract facts ---
    fact_extractor = FactExtractor()
    extracted_facts = fact_extractor.extract(text)
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
        stored_fact = await provider.create_fact(fact)

        receipt = MemoryReceipt(
            id=fact_id,
            memory_type="fact",
            source=source,
            created_by="learn",
            timestamp=now,
            confidence=ef.get("confidence", 0.5),
            verification_status=VerificationStatus.CANDIDATE,
        )
        stored_receipt = await provider.create_receipt(receipt)

        facts_result.append({
            "receipt": stored_receipt.model_dump(mode="json"),
            "item": stored_fact.model_dump(mode="json"),
        })
        receipts.append(stored_receipt.model_dump(mode="json"))

    # --- Extract decisions ---
    decision_extractor = DecisionExtractor()
    extracted_decisions = decision_extractor.extract(text)
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
        stored_decision = await provider.create_decision(decision)

        receipt = MemoryReceipt(
            id=decision_id,
            memory_type="decision",
            source=source,
            created_by="learn",
            timestamp=now,
            confidence=ed.get("confidence", 0.5),
            verification_status=VerificationStatus.CANDIDATE,
        )
        stored_receipt = await provider.create_receipt(receipt)

        decisions_result.append({
            "receipt": stored_receipt.model_dump(mode="json"),
            "item": stored_decision.model_dump(mode="json"),
        })
        receipts.append(stored_receipt.model_dump(mode="json"))

    # --- Extract skills ---
    skill_extractor = SkillExtractor()
    extracted_skills = skill_extractor.extract(text)
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
        stored_skill = await provider.create_skill(skill)

        receipt = MemoryReceipt(
            id=skill_id,
            memory_type="skill",
            source=source,
            created_by="learn",
            timestamp=now,
            confidence=es.get("confidence", 0.5),
            verification_status=VerificationStatus.CANDIDATE,
        )
        stored_receipt = await provider.create_receipt(receipt)

        skills_result.append({
            "receipt": stored_receipt.model_dump(mode="json"),
            "item": stored_skill.model_dump(mode="json"),
        })
        receipts.append(stored_receipt.model_dump(mode="json"))

    return {
        "facts": facts_result,
        "decisions": decisions_result,
        "skills": skills_result,
        "receipts": receipts,
    }
