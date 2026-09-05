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

import logging
import re
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

LLMExtractorFn = Callable[[str], list[dict]]


def _normalize_steps(value: Any) -> list[str]:
    """Coerce a raw steps value to a clean list of non-empty strings.

    Malformed LLM entries (missing key, non-list, or list containing
    non-string items) become ``[]`` — the caller then skips the skill as
    unrecognized instead of letting the model constructor crash.
    """
    if not isinstance(value, list):
        return []
    steps: list[str] = []
    for item in value:
        if isinstance(item, str) and item.strip():
            steps.append(item.strip())
    return steps


def _normalize_string_list(value: Any) -> list[str]:
    """Coerce constraints/validation lists the same defensive way."""
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        if isinstance(item, str) and item.strip():
            out.append(item.strip())
    return out


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

        Only structurally valid skills are returned (SVC-3): an entry must
        have a non-empty string purpose and at least one recognized step.
        Anything malformed is skipped here, before the ingestion service ever
        builds a Skill model, so a bad body can never crash learn().
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
                    if not isinstance(s, dict):
                        continue
                    purpose = s.get("purpose", "")
                    if not isinstance(purpose, str) or not purpose.strip():
                        continue  # malformed: no usable purpose
                    steps = _normalize_steps(s.get("steps"))
                    if not steps:
                        # Malformed / no recognized step — skip safely
                        # (never reach the Skill constructor).
                        continue
                    skills.append(
                        {
                            "purpose": purpose.strip(),
                            "steps": steps,
                            "constraints": _normalize_string_list(
                                s.get("constraints")
                            ),
                            "confidence": self._llm_confidence,
                        }
                    )

        return skills

    def _parse_steps(self, body: str) -> list[str]:
        """Parse numbered steps from the body text.

        A body that contains no numbered list is treated as a single
        unnumbered step (``do: run the backup``), so a valid regex match can
        never produce an empty step list that would fail the Skill model's
        ``steps: min_length=1`` at construction time (SVC-3).
        """
        steps = []
        for step_match in self._STEP_PATTERN.finditer(body):
            step_text = step_match.group(1).strip().strip(", .?!")
            if step_text:
                steps.append(step_text)
        if not steps and body.strip():
            # Unnumbered body -> one step.
            steps = [body.strip().strip(", .?!")]
        return steps

    def _parse_constraints(self, text: str) -> list[str]:
        """Parse 'must X' constraints from text."""
        constraints = []
        for cm in self._CONSTRAINT_PATTERN.finditer(text):
            constraint_text = cm.group(1).strip()
            if constraint_text:
                constraints.append(constraint_text)
        return constraints
