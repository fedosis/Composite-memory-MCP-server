"""DecisionExtractor — extract decisions from text.

Extracts decision records with context, choice, alternatives, and reason.

Supports two modes:
1. Default regex (pattern: "decided to X because Y") for testing.
2. LLM-powered extraction via any callable(text) -> list[dict] interface.

Confidence scoring:
- Regex mode: 0.5 (pattern-based)
- LLM mode: 0.7-0.9 configurable (default 0.85)
"""

import re
from typing import Callable, Optional

LLMExtractorFn = Callable[[str], list[dict]]


class DecisionExtractor:
    """Extract decisions from text.

    Args:
        llm_extractor: Optional callable(text) -> list[dict] for LLM extraction.
            Each dict may have keys: context, choice, alternatives, reason.
        llm_confidence: Confidence score for LLM-extracted decisions (0.0-1.0).
            Default 0.85.
    """

    # Pattern: "decided to <choice> because <reason>"
    # Non-greedy on reason, stops at sentence boundary
    _REGEX_PATTERN = re.compile(
        r"decided\s+to\s+(.+?)\s+because\s+(.+?)(?:[.?!]|$)", re.IGNORECASE
    )
    _REGEX_CONFIDENCE = 0.5

    def __init__(
        self,
        llm_extractor: Optional[LLMExtractorFn] = None,
        llm_confidence: float = 0.85,
    ):
        self._llm_extractor = llm_extractor
        self._llm_confidence = max(0.7, min(0.9, llm_confidence))

    def extract(self, text: str) -> list[dict]:
        """Extract decisions from the given text.

        Args:
            text: Raw text to extract decisions from.

        Returns:
            List of dicts with keys: context, choice, alternatives, reason,
            confidence.
        """
        if not text or not text.strip():
            return []

        decisions: list[dict] = []

        # 1. Regex extraction: "decided to X because Y"
        for match in self._REGEX_PATTERN.finditer(text):
            choice = match.group(1).strip()
            reason = match.group(2).strip().rstrip(".?!")
            decisions.append(
                {
                    "context": "",
                    "choice": choice,
                    "alternatives": [],
                    "reason": reason,
                    "confidence": self._REGEX_CONFIDENCE,
                }
            )

        # 2. LLM extraction (if configured)
        if self._llm_extractor:
            llm_decisions = self._llm_extractor(text)
            if llm_decisions:
                for d in llm_decisions:
                    decisions.append(
                        {
                            "context": d.get("context", ""),
                            "choice": d.get("choice", ""),
                            "alternatives": d.get("alternatives", []),
                            "reason": d.get("reason", ""),
                            "confidence": self._llm_confidence,
                        }
                    )

        return decisions
