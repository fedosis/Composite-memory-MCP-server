"""Strict validation of LLM extraction responses (SPEC item 1)."""
import json
import math
import re
from dataclasses import dataclass
from typing import Any

from memory_server.extractors.noise_filter import EDGE_JUNK

_FENCE_RE = re.compile(r"^```(?:json)?\s*\n?(.*?)\n?\s*```$", re.DOTALL)


@dataclass(frozen=True)
class ExtractedResult:
    """Validated combined extraction result (facts + decisions)."""
    facts: tuple[dict[str, Any], ...]
    decisions: tuple[dict[str, Any], ...]


def _clean(value: str) -> str:
    # str-only (callers gate isinstance first — review #4): collapse ANY
    # Unicode whitespace + edge-strip EDGE_JUNK (SPEC item 1 Unicode policy).
    return " ".join(value.strip(EDGE_JUNK).split())


def _confidence_ok(value: object) -> bool:
    # bool is NOT a number (True/False are ints in Python); NaN/Inf are NOT
    # valid (json.loads parses NaN/Infinity/-Infinity by default!).
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    if not math.isfinite(float(value)):
        return False
    return 0.0 <= float(value) <= 1.0


def _fact_ok(item: object) -> bool:
    if not isinstance(item, dict):
        return False
    for key in ("subject", "predicate", "object"):
        v = item.get(key)
        # non-string REJECTED before any stringification — `5` is invalid,
        # never `"5"`; non-empty after whitespace-collapse + edge-strip.
        if not isinstance(v, str) or _clean(v) == "":
            return False
    if "confidence" in item and not _confidence_ok(item["confidence"]):
        return False
    return True


def _decision_ok(item: object) -> bool:
    if not isinstance(item, dict):
        return False
    for key in ("context", "choice", "reason"):   # strings (may be "")
        if not isinstance(item.get(key), str):
            return False
    alts = item.get("alternatives")               # REQUIRED list of strings
    if not isinstance(alts, list) or not all(isinstance(a, str) for a in alts):
        return False
    if "confidence" in item and not _confidence_ok(item["confidence"]):
        return False
    return True


def validate_llm_result(raw: object) -> ExtractedResult | None:
    """Parse + strictly validate one combined LLM result.

    Accepts a raw JSON string (bare, or EXACTLY one fenced block) or an
    already-parsed dict (parent D4 carry-forward: defense in depth — test
    doubles and Card B's factory may return either). Returns ExtractedResult
    on success, None on ANY malformed input (caller falls back to regex).
    Never coerces.
    """
    if isinstance(raw, str):
        s = raw.strip()
        if s.startswith("```"):
            m = _FENCE_RE.match(s)
            if m is None:
                return None        # prefix/suffix text, multiple fences, ...
            payload = m.group(1)
        else:
            payload = s
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            return None
    elif isinstance(raw, dict):
        data = raw
    else:
        return None                # None, list, int, bool, float -> invalid
    if not isinstance(data, dict):
        return None
    facts, decisions = data.get("facts"), data.get("decisions")
    if not isinstance(facts, list) or not isinstance(decisions, list):
        return None                # missing key OR wrong type -> malformed
    if not all(_fact_ok(f) for f in facts):
        return None                # ANY malformed item -> WHOLE response invalid
    if not all(_decision_ok(d) for d in decisions):
        return None
    return ExtractedResult(tuple(facts), tuple(decisions))
