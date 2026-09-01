"""DecisionExtractor — extract decisions from text.

Extracts decision records with context, choice, alternatives, and reason.

Supports two modes:
1. Default regex (pattern: "decided to X because Y") for testing.
2. LLM-powered extraction driven by the pipeline-v3 service adapter
   contract (Card A2): at the service boundary the injected callable is a
   closure over the A1-validated ExtractedResult, adapted to this
   extractor's per-kind list[dict] shape. The service drives
   extract(text, include_regex=False) in LLM mode so ONLY the validated
   LLM list (possibly empty) is consumed — the regex pass contributes no
   items. Standalone use (unit tests) keeps the combined regex+LLM
   behavior via the default include_regex=True.

Confidence scoring:
- Regex mode: 0.5 (pattern-based)
- LLM mode: 0.7-0.9 configurable (default 0.85)
"""

import re
from typing import Callable, Optional

from memory_server.extractors.llm_response import ExtractedResult

# Shared protocol (pipeline R3): text -> validated combined result or None.
# At the A2 service boundary the DI closures adapt the validated
# ExtractedResult into the per-kind list[dict] shape the extractors consume.
LLMExtractorFn = Callable[[str], ExtractedResult | None]


class DecisionExtractor:
    """Extract decisions from text.

    Pipeline-v3 service adapter contract (Card A2): the injected
    llm_extractor is NOT the raw LLM callable. The service validates ONE
    combined result (A1 validate_llm_result) and injects a closure over
    that validated ExtractedResult which adapts the frozen decisions tuple
    to this extractor's list[dict] DI shape. In LLM mode the service calls
    extract(text, include_regex=False), so the regex branch contributes no
    items and the result is exactly the validated LLM decisions list
    (empty included) — regardless of any confidence gate.

    Args:
        llm_extractor: Optional callable(text) -> list[dict] — the
            pipeline-v3 adapter closure over the validated ExtractedResult
            (decisions slice). Each dict may carry a per-item confidence;
            absent -> llm_confidence default.
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

    def extract(self, text: str, include_regex: bool = True) -> list[dict]:
        """Extract decisions from the given text.

        Args:
            text: Raw text to extract decisions from.
            include_regex: When False (pipeline-v3 LLM mode at the service
                boundary) the regex pass is skipped entirely — only the
                llm_extractor closure contributes items, so a validated
                EMPTY LLM list stores nothing regardless of any confidence
                gate. Default True preserves the legacy combined regex+LLM
                behavior for standalone/unit use.

        Returns:
            List of dicts with keys: context, choice, alternatives, reason,
            confidence.
        """
        if not text or not text.strip():
            return []

        decisions: list[dict] = []

        # 1. Regex extraction: "decided to X because Y". Skipped in LLM
        #    mode — the service drives include_regex=False so the regex
        #    pass never contributes items to the validated LLM result
        #    (SPEC AC3).
        if include_regex:
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
                    # D5: honor a per-item confidence key; absent -> the
                    # constructor default (0.85). The defensive clamp applies
                    # only to DIRECT extract() unit inputs — the service path
                    # A1-validates first, so malformed values never reach it.
                    conf = d.get("confidence")
                    confidence = (
                        float(conf) if conf is not None else self._llm_confidence
                    )
                    confidence = max(0.0, min(1.0, confidence))
                    decisions.append(
                        {
                            "context": d.get("context", ""),
                            "choice": d.get("choice", ""),
                            "alternatives": d.get("alternatives", []),
                            "reason": d.get("reason", ""),
                            "confidence": confidence,
                        }
                    )

        return decisions
