"""MCP tool: learn — extract and store facts, decisions, and skills from text.

Compatibility facade (ARCHITECTURE_SOURCE §38): the public signature, parameter
names, order and defaults are unchanged. The body only builds a
``LearnCommand`` and delegates to the transport-free application service, which
owns the atomic extraction write. The forwarding rule for the four optional
``llm_*`` kwargs lives in the application service and is unchanged.
"""

from typing import Callable, Optional

from memory_server.application import LearnCommand, MemoryService
from memory_server.providers.sqlite_provider import SQLiteProvider


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
    *,
    native_submitter=None,
) -> dict:
    """Extract facts, decisions, skills, and optionally beliefs from natural language text.

    Delegates to the application service, which reuses
    ``MemoryIngestionService`` for single-transaction writes: all extracted
    items + receipts + outbox entries are committed atomically.

    When extract_beliefs=True, also runs belief extraction AFTER the main
    transaction (outside its scope) and creates/reinforces beliefs with
    evidence linked to extracted facts.

    The four optional llm_* kwargs are forwarded to the application service
    ONLY when not None; None means "not supplied" and the service defaults
    apply (A2 owns defaults).

    Args:
        provider: Initialized SQLiteProvider instance.
        text: Natural language text to analyze and extract knowledge from.
        source: Source identifier (default "user").
        extract_beliefs: If True, also extract and store beliefs (default False).
        min_belief_confidence: Minimum confidence to create a belief (default 0.6).
        llm_extractor: Optional callable(text) performing ONE combined LLM
            extraction per call (forwarded when not None).
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
    command = LearnCommand(
        text=text,
        source=source,
        extract_beliefs=extract_beliefs,
        min_belief_confidence=min_belief_confidence,
        llm_extractor=llm_extractor,
        llm_timeout_seconds=llm_timeout_seconds,
        llm_max_input_chars=llm_max_input_chars,
        llm_confidence_gate=llm_confidence_gate,
    )
    return await MemoryService(
        provider._session_factory,
        native_submitter=native_submitter,
    ).learn(command)
