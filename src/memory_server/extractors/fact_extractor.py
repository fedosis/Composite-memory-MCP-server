"""FactExtractor — extract subject-predicate-object triples from text.

Supports two extraction modes:
1. Default regex (pattern: "X is Y") for fast, reproducible testing.
2. LLM-powered extraction driven by the pipeline-v3 service adapter
   contract (Card A2): at the service boundary the injected callable is a
   closure over the A1-validated ExtractedResult, adapted to this
   extractor's per-kind list[dict] shape. The service drives
   extract(text, include_regex=False) in LLM mode so ONLY the validated
   LLM list (possibly empty) is consumed — the regex pass contributes no
   items. Standalone use (unit tests) keeps the combined regex+LLM
   behavior via the default include_regex=True.

Confidence scoring:
- Regex mode: 0.5 (pattern-based, less reliable)
- LLM mode: 0.7-0.9 configurable (default 0.85)
"""

import re
from typing import Callable, Optional

from memory_server.extractors.llm_response import ExtractedResult

# Shared protocol (pipeline R3): text -> validated combined result or None.
# At the A2 service boundary the DI closures adapt the validated
# ExtractedResult into the per-kind list[dict] shape the extractors consume.
LLMExtractorFn = Callable[[str], ExtractedResult | None]


class FactExtractor:
    """Extract subject-predicate-object facts from text.

    Pipeline-v3 service adapter contract (Card A2): the injected
    llm_extractor is NOT the raw LLM callable. The service validates ONE
    combined result (A1 validate_llm_result) and injects a closure over
    that validated ExtractedResult which adapts the frozen facts tuple to
    this extractor's list[dict] DI shape. In LLM mode the service calls
    extract(text, include_regex=False), so the regex branch contributes no
    items and the result is exactly the validated LLM facts list (empty
    included) — regardless of any confidence gate.

    Args:
        llm_extractor: Optional callable(text) -> list[dict] — the
            pipeline-v3 adapter closure over the validated ExtractedResult
            (facts slice). Each dict may carry a per-item confidence;
            absent -> llm_confidence default.
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

    def extract(self, text: str, include_regex: bool = True) -> list[dict]:
        """Extract facts from the given text.

        Args:
            text: Raw text to extract facts from.
            include_regex: When False (pipeline-v3 LLM mode at the service
                boundary) the regex pass is skipped entirely — only the
                llm_extractor closure contributes items, so a validated
                EMPTY LLM list stores nothing regardless of any confidence
                gate. Default True preserves the legacy combined regex+LLM
                behavior for standalone/unit use.

        Returns:
            List of dicts with keys: subject, predicate, object, confidence.
        """
        if not text or not text.strip():
            return []

        facts: list[dict] = []

        # 1. Regex extraction: "X is Y" patterns. Skipped in LLM mode —
        #    the service drives include_regex=False so the regex pass never
        #    contributes items to the validated LLM result (SPEC AC3).
        if include_regex:
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
                    # D5: honor a per-item confidence key; absent -> the
                    # constructor default (0.85). The defensive clamp applies
                    # only to DIRECT extract() unit inputs — the service path
                    # A1-validates first, so malformed values never reach it.
                    conf = fact.get("confidence")
                    confidence = (
                        float(conf) if conf is not None else self._llm_confidence
                    )
                    confidence = max(0.0, min(1.0, confidence))
                    facts.append(
                        {
                            "subject": fact.get("subject", ""),
                            "predicate": fact.get("predicate", ""),
                            "object": fact.get("object", ""),
                            "confidence": confidence,
                        }
                    )

        return facts
