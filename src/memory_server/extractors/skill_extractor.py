"""SkillExtractor — extract procedural knowledge from text.

Extracts skills with purpose, steps, and constraints.

Supports two modes:
1. Default regex: "to <purpose>, do: 1) <step1>, 2) <step2>" pattern.
   Constraints detected via "must <constraint>" patterns.
2. LLM-powered extraction via any callable(text) -> list[dict] interface.

Confidence scoring:
- Regex mode: 0.5 (pattern-based)
- LLM mode: 0.7-0.9 configurable (default 0.85)
"""

import re
from typing import Callable, Optional

LLMExtractorFn = Callable[[str], list[dict]]


class SkillExtractor:
    """Extract skills (procedural knowledge) from text.

    Args:
        llm_extractor: Optional callable(text) -> list[dict] for LLM extraction.
            Each dict may have keys: purpose, steps, constraints.
        llm_confidence: Confidence score for LLM-extracted skills (0.0-1.0).
            Default 0.85.
    """

    # Pattern: "to <purpose>, do: <steps>"
    _SKILL_PATTERN = re.compile(
        r"to\s+(.+?),\s+do:\s+(.+)", re.IGNORECASE
    )
    _STEP_PATTERN = re.compile(
        r"(?:\d+\)|\d+\.)\s*(.+?)(?=\s*(?:\d+\)|\d+\.|$))", re.IGNORECASE
    )
    _CONSTRAINT_PATTERN = re.compile(
        r"must\s+(.+?)(?:[.?!]|,|$)", re.IGNORECASE
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
        """Extract skills from the given text.

        Args:
            text: Raw text to extract skills from.

        Returns:
            List of dicts with keys: purpose, steps, constraints, confidence.
        """
        if not text or not text.strip():
            return []

        skills: list[dict] = []

        # 1. Regex extraction
        for match in self._SKILL_PATTERN.finditer(text):
            purpose = match.group(1).strip()
            body = match.group(2).strip()

            # Extract steps from numbered list
            steps = self._parse_steps(body)

            # Extract constraints from full text
            constraints = self._parse_constraints(text)

            skills.append(
                {
                    "purpose": purpose,
                    "steps": steps,
                    "constraints": constraints,
                    "confidence": self._REGEX_CONFIDENCE,
                }
            )

        # 2. LLM extraction (if configured)
        if self._llm_extractor:
            llm_skills = self._llm_extractor(text)
            if llm_skills:
                for s in llm_skills:
                    skills.append(
                        {
                            "purpose": s.get("purpose", ""),
                            "steps": s.get("steps", []),
                            "constraints": s.get("constraints", []),
                            "confidence": self._llm_confidence,
                        }
                    )

        return skills

    def _parse_steps(self, body: str) -> list[str]:
        """Parse numbered steps from the body text."""
        steps = []
        for step_match in self._STEP_PATTERN.finditer(body):
            step_text = step_match.group(1).strip().strip(",.?!")
            if step_text:
                steps.append(step_text)
        return steps

    def _parse_constraints(self, text: str) -> list[str]:
        """Parse 'must X' constraints from text."""
        constraints = []
        for cm in self._CONSTRAINT_PATTERN.finditer(text):
            constraint_text = cm.group(1).strip()
            if constraint_text:
                constraints.append(constraint_text)
        return constraints
