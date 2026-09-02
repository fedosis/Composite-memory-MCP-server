"""MCP tool: learn — extract and store facts, decisions, and skills from free text.

Thin wrapper around MemoryIngestionService.learn() which handles
all extraction and single-transaction writes.
"""

from typing import Callable, Optional

from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.services.ingestion_service import MemoryIngestionService


async def learn(
    provider: SQLiteProvider,
    text: str,
    source: str = "user",
    extract_beliefs: bool = False,
    min_belief_confidence: float = 0.6,
    llm_extractor: Optional[Callable[[str], object]] = None,
    llm_timeout_seconds: Optional[float] = None,
    llm_max_input_chars: Optional[int] = None,
    llm_confidence_gate: Optional[float] = None,
) -> dict:
    """Extract facts, decisions, skills, and optionally beliefs from natural language text.

    Delegates to MemoryIngestionService for single-transaction writes:
    all extracted items + receipts + outbox entries are committed atomically.

    When extract_beliefs=True, also runs belief extraction AFTER the main
    transaction (outside its scope) and creates/reinforces beliefs with
    evidence linked to extracted facts.

    The four optional llm_* kwargs are forwarded to
    MemoryIngestionService.learn() ONLY when not None; None means
    "not supplied" and the service defaults apply (A2 owns defaults).

    Args:
        provider: Initialized SQLiteProvider instance.
        text: Natural language text to analyze and extract knowledge from.
        source: Source identifier (default "user").
        extract_beliefs: If True, also extract and store beliefs (default False).
        min_belief_confidence: Minimum confidence to create a belief (default 0.6).
        llm_extractor: Optional callable(text) performing ONE combined LLM
            extraction per call (forwarded to the service when not None).
        llm_timeout_seconds: Outer timeout for the LLM call (forwarded when not None).
        llm_max_input_chars: Tail truncation of the callable input (forwarded when not None).
        llm_confidence_gate: LLM-mode confidence gate (forwarded when not None).

    Returns:
        Dict with keys:
            - facts: list of {receipt, item} for extracted facts
            - decisions: list of {receipt, item} for extracted decisions
            - skills: list of {receipt, item} for extracted skills
            - beliefs: list of {belief, extracted, reinforced} (when extract_beliefs=True)
            - receipts: flat list of all receipts
    """
    svc = MemoryIngestionService(provider._session_factory)
    kwargs = {
        "text": text,
        "source": source,
        "extract_beliefs": extract_beliefs,
        "min_belief_confidence": min_belief_confidence,
    }
    # Forwarding rule (PLAN §3): a kwarg is forwarded to the service IF AND
    # ONLY IF its value is not None. Unconditional forwarding of None is
    # FORBIDDEN — the service degrades a None timeout/max_input to regex
    # fallback (ingestion_service.py:321-339 TypeError checks + :352-358
    # fallback) instead of its 15.0/8000 defaults.
    if llm_extractor is not None:
        kwargs["llm_extractor"] = llm_extractor
    if llm_timeout_seconds is not None:
        kwargs["llm_timeout_seconds"] = llm_timeout_seconds
    if llm_max_input_chars is not None:
        kwargs["llm_max_input_chars"] = llm_max_input_chars
    if llm_confidence_gate is not None:
        kwargs["llm_confidence_gate"] = llm_confidence_gate
    return await svc.learn(**kwargs)
