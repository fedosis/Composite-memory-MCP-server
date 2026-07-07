"""FactExtractor — extract subject-predicate-object triples from text.

Supports two extraction modes:
1. Default regex (pattern: "X is Y") for fast, reproducible testing.
2. LLM-powered extraction via any callable(text) -> list[dict] interface.

Confidence scoring:
- Regex mode: 0.5 (pattern-based, less reliable)
- LLM mode: 0.7-0.9 configurable (default 0.85)
"""

import re
from typing import Callable, Optional

# Type for an LLM extraction callable: takes raw text, returns list of SPO dicts
LLMExtractorFn = Callable[[str], list[dict]]


class FactExtractor:
    """Extract subject-predicate-object facts from text.

    Args:
        llm_extractor: Optional callable(text) -> list[dict] for LLM extraction.
            Each dict must have keys: subject, predicate, object.
        llm_confidence: Confidence score for LLM-extracted facts (0.0-1.0).
            Default 0.85. Ignored for regex-based extraction.
    """

    # Pattern: "<Subject> is <Object>"  (case-insensitive, word-boundaried)
    _REGEX_PATTERN = re.compile(r"(\w[\w\s]*?)\s+is\s+(\w[\w\s]*)", re.IGNORECASE)
    _REGEX_CONFIDENCE = 0.5

    def __init__(
        self,
        llm_extractor: Optional[LLMExtractorFn] = None,
        llm_confidence: float = 0.85,
    ):
        self._llm_extractor = llm_extractor
        self._llm_confidence = max(0.7, min(0.9, llm_confidence))

    def extract(self, text: str) -> list[dict]:
        """Extract facts from the given text.

        Args:
            text: Raw text to extract facts from.

        Returns:
            List of dicts with keys: subject, predicate, object, confidence.
        """
        if not text or not text.strip():
            return []

        facts: list[dict] = []

        # 1. Regex extraction: "X is Y" patterns
        for match in self._REGEX_PATTERN.finditer(text):
            subject = match.group(1).strip()
            obj = match.group(2).strip()
            facts.append(
                {
                    "subject": subject,
                    "predicate": "is",
                    "object": obj,
                    "confidence": self._REGEX_CONFIDENCE,
                }
            )

        # 2. LLM extraction (if configured)
        if self._llm_extractor:
            llm_facts = self._llm_extractor(text)
            if llm_facts:
                for fact in llm_facts:
                    facts.append(
                        {
                            "subject": fact.get("subject", ""),
                            "predicate": fact.get("predicate", ""),
                            "object": fact.get("object", ""),
                            "confidence": self._llm_confidence,
                        }
                    )

        return facts
