"""BeliefExtractor — extract belief propositions from text using LLM.

Uses DI pattern matching existing extractors (FactExtractor, etc.).
When llm_extractor is None (default/test mode), returns empty list.

ExtractedBelief fields:
- proposition: The belief statement (free-form, 1 sentence)
- confidence: 0.0-1.0 — LLM-estimated confidence
- source_refs: List of proposition texts that support this belief
  (used for content-based evidence linking to fact IDs)
- tags: Relevant categories (max 3)
- reasoning: Why this proposition was extracted as a belief
"""

from __future__ import annotations

from typing import Awaitable, Callable

from pydantic import BaseModel, Field

# Type for an LLM extraction callable: async callable taking (text, system_prompt)
# and returning list of dicts. Supports both sync and async via the adapter in extract().
LLMExtractorFn = Callable[[str, str], Awaitable[list[dict]]]


class ExtractedBelief(BaseModel):
    """A belief proposition extracted from text, before storage."""

    proposition: str = Field(
        ..., min_length=1, max_length=2048,
        description="The belief statement (concise, declarative, 1 sentence)",
    )
    confidence: float = Field(
        default=0.5, ge=0.0, le=1.0,
        description="0.0-1.0 — how explicitly the belief is stated vs inferred",
    )
    source_refs: list[str] = Field(
        default_factory=list,
        description="Proposition texts of extracted facts supporting this belief",
    )
    tags: list[str] = Field(
        default_factory=list,
        description="Relevant categories/topics (max 3)",
    )
    reasoning: str | None = Field(
        default=None,
        description="Why this proposition was considered a belief from the text",
    )


class BeliefExtractor:
    """Extract belief propositions from natural language text using LLM.

    Args:
        llm_extractor: Optional callable(text, system_prompt) -> list[dict] for LLM
            extraction. Each dict must have keys: proposition, confidence,
            source_refs, tags, reasoning. When None, returns [].
    """

    SYSTEM_PROMPT = """You are a belief extraction system. Given a text, extract
belief propositions — statements the author appears to hold as true.

For each belief:
1. proposition: concise declarative statement (1 sentence)
2. confidence: 0.0-1.0 based on how explicitly stated vs inferred
   - 0.9-1.0: explicitly stated ("I use Docker for everything")
   - 0.6-0.8: strongly implied ("Docker makes deployment easy")
   - 0.3-0.5: weakly implied ("I've heard Docker is good")
   - 0.0-0.2: speculative
3. source_refs: array of fact proposition texts that support this belief
   (e.g. "I use Docker", "Docker runs on OMV8"). Use factual statements
   from the text that directly support this belief.
4. tags: relevant categories (max 3)
5. reasoning: why this proposition is considered a belief

Extract 0-5 beliefs per text. Return JSON array."""

    def __init__(self, llm_extractor: LLMExtractorFn | None = None):
        self._llm = llm_extractor

    async def extract(self, text: str) -> list[ExtractedBelief]:
        """Extract belief propositions from the given text.

        Args:
            text: Raw text to extract beliefs from.

        Returns:
            List of ExtractedBelief objects (empty if no LLM configured or no beliefs found).
        """
        if not text or not text.strip():
            return []

        if self._llm is None:
            return []  # Test/default mode: no LLM available

        result = await self._llm(text, self.SYSTEM_PROMPT)
        if not result:
            return []

        return [ExtractedBelief(**item) for item in result]
