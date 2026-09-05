"""Service-level integration tests for the Card A2 LLM extraction boundary.

Every test constructs MemoryIngestionService directly from
provider._session_factory — the exact DI slot api/learn.py:42 uses — so the
new learn() kwargs (llm_extractor, llm_timeout_seconds, llm_max_input_chars,
llm_confidence_gate) are reachable. The payload literals are A1-valid by
construction (every required key, correct types), so the success/gate tests
exercise the LLM DI path — never a silent regex fallback.
"""

import time
from unittest.mock import Mock

import pytest
from storage.dedup import fact_dedup_key

import memory_server.services.ingestion_service as svc
from memory_server.extractors.llm_response import ExtractedResult
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.services.ingestion_service import MemoryIngestionService


@pytest.fixture
async def provider():
    """In-memory SQLite provider; mirrors tests/test_learn.py's fixture
    exactly (same url, initialize/yield/close lifecycle). `_session_factory`
    is the DI slot api/learn.py:42 passes to MemoryIngestionService; every
    service-level test constructs the service from it."""
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()
    yield p
    await p.close()


# facts = subject/predicate/object (non-empty strings) + finite confidence;
# decisions = context/choice/reason (strings) + alternatives (REQUIRED list
# of strings) + finite confidence. Underspecified fixtures would silently
# hit the regex fallback — these are A1-valid by construction.
VALID_FULL_PAYLOAD = {
    "facts": [
        {"subject": "Docker", "predicate": "is", "object": "container",
         "confidence": 0.9},
    ],
    "decisions": [
        {"context": "we decided to use Caddy because simple",
         "choice": "use Caddy", "reason": "simple",
         "alternatives": [], "confidence": 0.8},
    ],
}

GATE_PAYLOAD = {
    "facts": [
        {"subject": "Docker", "predicate": "is", "object": "container",
         "confidence": 0.4},
        {"subject": "Python", "predicate": "is", "object": "great",
         "confidence": 0.9},
    ],
    "decisions": [
        {"context": "we decided to use Caddy because simple",
         "choice": "use Caddy", "reason": "simple",
         "alternatives": [], "confidence": 0.4},
        {"context": "we decided to use Caddy because simple",
         "choice": "use Caddy", "reason": "simple",
         "alternatives": ["use nginx"], "confidence": 0.9},
    ],
}

EMPTY_VALID_PAYLOAD = {"facts": [], "decisions": []}

# Parent-card-verified fixture text: regex facts+decisions, NO skills.
REGEX_TEXT = "Docker is container. we decided to use Caddy because simple."


def payload_with_fact_confidence(value):
    """A1-valid payload EXCEPT the fact confidence is replaced by `value`
    (string/bool/NaN/Inf parameterization; ALL other fields valid)."""
    return {
        "facts": [
            {"subject": "Docker", "predicate": "is", "object": "container",
             "confidence": value},
        ],
        "decisions": [
            {"context": "we decided to use Caddy because simple",
             "choice": "use Caddy", "reason": "simple",
             "alternatives": [], "confidence": 0.8},
        ],
    }


class _RecordingExtractor:
    """Service-level test double: records the llm_extractor closure it was
    constructed with (mirrors the real extractors' DI slot)."""

    instances: list["_RecordingExtractor"] = []

    def __init__(self, *args, **kwargs):
        self.llm_extractor = kwargs.get("llm_extractor")
        _RecordingExtractor.instances.append(self)

    def extract(self, text: str, include_regex: bool = True) -> list[dict]:
        return self.llm_extractor(text) if self.llm_extractor else []


@pytest.mark.asyncio
async def test_same_validated_result_structural(monkeypatch, provider):
    """ONE callable call -> ONE validator call -> ONE shared validated
    ExtractedResult -> BOTH closures read from it; neither extractor
    receives the original callable or re-invokes it (closure cell capture)."""
    monkeypatch.setattr(svc, "FactExtractor", _RecordingExtractor)
    monkeypatch.setattr(svc, "DecisionExtractor", _RecordingExtractor)
    _RecordingExtractor.instances.clear()

    validator_calls: list = []
    real_validate = svc.validate_llm_result

    def spy_validate(raw):
        result = real_validate(raw)
        validator_calls.append(result)
        return result

    monkeypatch.setattr(svc, "validate_llm_result", spy_validate)

    spy = Mock(return_value=VALID_FULL_PAYLOAD)
    service = MemoryIngestionService(provider._session_factory)
    await service.learn(REGEX_TEXT, llm_extractor=spy)

    assert spy.call_count == 1               # one original callable call
    assert len(validator_calls) == 1         # one validator call
    captured = validator_calls[0]
    assert isinstance(captured, ExtractedResult)

    fact_double, dec_double = _RecordingExtractor.instances
    f_clos, d_clos = fact_double.llm_extractor, dec_double.llm_extractor
    assert f_clos is not spy and d_clos is not spy  # neither gets the callable
    assert f_clos is not None and d_clos is not None

    # closure inspection: the lambdas capture `llm_result` as their ONLY
    # free variable; the SAME validated object is in both cells (is).
    assert len(f_clos.__closure__) == 1 and len(d_clos.__closure__) == 1
    assert f_clos.__closure__[0].cell_contents is captured
    assert d_clos.__closure__[0].cell_contents is captured

    # invoking both closures reads the shared object; no re-invocation
    assert f_clos("ignored") == list(captured.facts)
    assert d_clos("ignored") == list(captured.decisions)
    assert f_clos("ignored")[0] is captured.facts[0]      # element identity
    assert d_clos("ignored")[0] is captured.decisions[0]  # survives list()
    assert spy.call_count == 1               # still exactly one call


@pytest.mark.asyncio
async def test_factory_validated_result_skips_second_validation(monkeypatch, provider):
    """A factory-provided ExtractedResult is consumed without revalidation."""
    validated = ExtractedResult(
        facts=(
            {
                "subject": "Helios",
                "predicate": "powers",
                "object": "Aurora",
                "confidence": 0.93,
            },
        ),
        decisions=(
            {
                "context": "deployment review",
                "choice": "choose Helios",
                "reason": "lower operational risk",
                "alternatives": ["choose Atlas"],
                "confidence": 0.88,
            },
        ),
    )
    validator_calls: list = []
    real_validate = svc.validate_llm_result

    def spy_validate(raw):
        validator_calls.append(raw)
        return real_validate(raw)

    monkeypatch.setattr(svc, "validate_llm_result", spy_validate)
    spy = Mock(return_value=validated)
    service = MemoryIngestionService(provider._session_factory)

    result = await service.learn(REGEX_TEXT, llm_extractor=spy)

    assert spy.call_count == 1
    assert validator_calls == []
    assert result["facts"][0]["item"] == {
        "id": result["facts"][0]["item"]["id"],
        "subject": "Helios",
        "predicate": "powers",
        "object": "Aurora",
        "dedup_key": fact_dedup_key("Helios", "powers", "Aurora"),
        "confidence": 0.93,
        "source": "user",
        "creator": "user",
        "created_at": result["facts"][0]["item"]["created_at"],
        "updated_at": result["facts"][0]["item"]["updated_at"],
        "verification_status": "candidate",
        "lifecycle_state": "active",
        "version": 1,
    }
    assert result["decisions"][0]["item"] == {
        "id": result["decisions"][0]["item"]["id"],
        "context": "deployment review",
        "choice": "choose Helios",
        "rejected_alternatives": ["choose Atlas"],
        "reason": "lower operational risk",
        "source": "user",
        "creator": "user",
        "created_at": result["decisions"][0]["item"]["created_at"],
        "updated_at": result["decisions"][0]["item"]["updated_at"],
        "confidence": 0.88,
        "verification_status": "candidate",
        "lifecycle_state": "active",
        "version": "0.1.0",
    }
    assert result["decisions"][0]["receipt"]["confidence"] == 0.88


@pytest.mark.asyncio
async def test_raw_dict_result_still_validates(monkeypatch, provider):
    """A raw dict from the callable still goes through the validator."""
    validator_calls: list = []
    real_validate = svc.validate_llm_result

    def spy_validate(raw):
        result = real_validate(raw)
        validator_calls.append(raw)
        return result

    monkeypatch.setattr(svc, "validate_llm_result", spy_validate)
    spy = Mock(return_value={
        "facts": [{
            "subject": "DictSubject",
            "predicate": "is",
            "object": "DictObject",
            "confidence": 0.91,
        }],
        "decisions": [{
            "context": "dict context",
            "choice": "choose DictChoice",
            "reason": "dict reason",
            "alternatives": ["dict alternative"],
            "confidence": 0.89,
        }],
    })
    service = MemoryIngestionService(provider._session_factory)

    result = await service.learn(REGEX_TEXT, llm_extractor=spy)

    assert spy.call_count == 1
    assert len(validator_calls) == 1
    assert result["facts"][0]["item"]["subject"] == "DictSubject"
    assert result["facts"][0]["item"]["predicate"] == "is"
    assert result["facts"][0]["item"]["object"] == "DictObject"
    assert result["facts"][0]["item"]["confidence"] == 0.91
    assert result["decisions"][0]["item"]["context"] == "dict context"
    assert result["decisions"][0]["item"]["choice"] == "choose DictChoice"
    assert result["decisions"][0]["item"]["rejected_alternatives"] == [
        "dict alternative"
    ]
    assert result["decisions"][0]["item"]["reason"] == "dict reason"
    assert result["decisions"][0]["receipt"]["confidence"] == 0.89


@pytest.mark.asyncio
async def test_one_combined_call_stores_both(monkeypatch, provider):
    """Real committed A2a extractors: one combined call stores facts AND
    decisions from the SAME validated result; the LLM-mode gate drops the
    regex 0.5 items."""
    validator_calls: list = []
    real_validate = svc.validate_llm_result

    def spy_validate(raw):
        result = real_validate(raw)
        validator_calls.append(result)
        return result

    monkeypatch.setattr(svc, "validate_llm_result", spy_validate)

    spy = Mock(return_value=VALID_FULL_PAYLOAD)
    service = MemoryIngestionService(provider._session_factory)
    result = await service.learn(REGEX_TEXT, llm_extractor=spy)

    assert spy.call_count == 1
    assert len(validator_calls) == 1
    captured = validator_calls[0]
    assert isinstance(captured, ExtractedResult)

    assert len(result["facts"]) == 1
    f = result["facts"][0]["item"]
    assert f["subject"] == "Docker"
    assert f["predicate"] == "is"
    assert f["object"] == "container"
    assert f["confidence"] == 0.9

    assert len(result["decisions"]) == 1
    d = result["decisions"][0]["item"]
    assert d["context"] == "we decided to use Caddy because simple"
    assert d["choice"] == "use Caddy"
    assert d["reason"] == "simple"
    assert d["rejected_alternatives"] == []
    # SVC-1: item and receipt carry the SAME extraction confidence (0.8) —
    # the item no longer keeps the model default 1.0 while the receipt says
    # 0.8.
    assert d["confidence"] == 0.8
    assert result["decisions"][0]["receipt"]["confidence"] == 0.8

    # stored content matches the captured validated object
    assert result["facts"][0]["item"]["subject"] == captured.facts[0]["subject"]
    assert result["decisions"][0]["item"]["choice"] == captured.decisions[0]["choice"]


@pytest.mark.asyncio
async def test_tail_truncation(provider):
    """llm_max_input_chars truncates the callable input to the TAIL; the
    extractors still run on the FULL text."""
    long_text = "Docker is container. " + ("x" * 10000)
    spy = Mock(return_value=VALID_FULL_PAYLOAD)
    service = MemoryIngestionService(provider._session_factory)
    await service.learn(long_text, llm_extractor=spy, llm_max_input_chars=100)
    spy.assert_called_once_with(long_text[-100:])

    spy2 = Mock(return_value=VALID_FULL_PAYLOAD)
    await service.learn(long_text, llm_extractor=spy2)  # default 8000
    spy2.assert_called_once_with(long_text[-8000:])


@pytest.mark.asyncio
async def test_callable_raises_falls_back_to_regex(provider):
    """A callable exception never crashes learn() — regex results stored."""
    spy = Mock(side_effect=RuntimeError("boom"))
    service = MemoryIngestionService(provider._session_factory)
    result = await service.learn(REGEX_TEXT, llm_extractor=spy)
    assert spy.call_count == 1
    assert len(result["facts"]) == 1
    assert result["facts"][0]["item"]["subject"] == "Docker"
    assert result["facts"][0]["item"]["confidence"] == 0.5
    assert len(result["decisions"]) == 1
    assert result["decisions"][0]["item"]["choice"] == "use Caddy"
    assert result["decisions"][0]["receipt"]["confidence"] == 0.5  # regex 0.5


@pytest.mark.asyncio
async def test_timeout_falls_back_to_regex(provider):
    """A blocking callable is bounded by llm_timeout_seconds: learn() returns
    regex results well within 2x the timeout. The abandoned worker thread may
    finish later (thread-may-finish-later); pytest may wait for it at exit."""
    def blocking(text):
        time.sleep(5.0)
        return VALID_FULL_PAYLOAD

    service = MemoryIngestionService(provider._session_factory)
    start = time.monotonic()
    result = await service.learn(REGEX_TEXT, llm_extractor=blocking,
                                 llm_timeout_seconds=1.0)
    elapsed = time.monotonic() - start
    assert elapsed < 2.0, elapsed
    assert len(result["facts"]) == 1
    assert result["facts"][0]["item"]["confidence"] == 0.5
    assert len(result["decisions"]) == 1


@pytest.mark.asyncio
async def test_validator_exception_falls_back_to_regex(monkeypatch, provider):
    """A validator exception is caught inside the boundary: learn() completes
    with regex results; the callable WAS invoked once (fallback at
    validation, not at the call)."""
    def bad_validate(raw):
        raise ValueError("validator broken")

    monkeypatch.setattr(svc, "validate_llm_result", bad_validate)
    spy = Mock(return_value=VALID_FULL_PAYLOAD)
    service = MemoryIngestionService(provider._session_factory)
    result = await service.learn(REGEX_TEXT, llm_extractor=spy)
    assert spy.call_count == 1
    assert len(result["facts"]) == 1
    assert result["facts"][0]["item"]["confidence"] == 0.5
    assert len(result["decisions"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["8000", 8000.5, True, None])
async def test_malformed_llm_max_input_chars_falls_back(provider, bad):
    """Malformed llm_max_input_chars is rejected inside the boundary: the
    callable is never invoked, learn() completes with regex results."""
    spy = Mock(return_value=VALID_FULL_PAYLOAD)
    service = MemoryIngestionService(provider._session_factory)
    result = await service.learn(REGEX_TEXT, llm_extractor=spy,
                                 llm_max_input_chars=bad)
    assert spy.call_count == 0
    assert len(result["facts"]) == 1
    assert result["facts"][0]["item"]["confidence"] == 0.5
    assert len(result["decisions"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [0, -1, float("nan"), float("inf"), "15", True])
async def test_bad_llm_timeout_seconds_falls_back(provider, bad):
    """Non-positive/non-finite/malformed llm_timeout_seconds disables the LLM
    path for the batch (callable NOT invoked); learn() never crashes."""
    spy = Mock(return_value=VALID_FULL_PAYLOAD)
    service = MemoryIngestionService(provider._session_factory)
    result = await service.learn(REGEX_TEXT, llm_extractor=spy,
                                 llm_timeout_seconds=bad)
    assert spy.call_count == 0
    assert len(result["facts"]) == 1
    assert result["facts"][0]["item"]["confidence"] == 0.5
    assert len(result["decisions"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_conf", ["0.8", True, float("nan"), float("inf")])
async def test_malformed_confidence_service_path_whole_response_fallback(
    provider, bad_conf
):
    """String/bool/NaN/Inf confidence at the SERVICE path invalidates the
    WHOLE response (A1) — regex fallback, never an item-level clamp."""
    spy = Mock(return_value=payload_with_fact_confidence(bad_conf))
    service = MemoryIngestionService(provider._session_factory)
    result = await service.learn(REGEX_TEXT, llm_extractor=spy)
    assert spy.call_count == 1
    assert len(result["facts"]) == 1
    assert result["facts"][0]["item"]["confidence"] == 0.5
    assert len(result["decisions"]) == 1
    # write-loop reality: decision items carry the model default 1.0; the
    # regex 0.5 confidence lands in the receipt (write region byte-identical)
    assert result["decisions"][0]["receipt"]["confidence"] == 0.5
    # no stored item carries the bad value or any clamped/coerced value
    for f in result["facts"]:
        assert f["item"]["confidence"] not in (bad_conf,)
    for d in result["decisions"]:
        # decision item confidence is the write-loop model default (1.0),
        # never derived from the input; the REAL stored confidence (receipt)
        # must be the regex 0.5, not the bad value
        assert d["receipt"]["confidence"] not in (bad_conf,)


@pytest.mark.asyncio
async def test_empty_valid_result_stores_nothing(provider):
    """A valid EMPTY LLM result stores NOTHING in LLM mode — no regex
    leakage (the gate drops the regex 0.5 items); SkillExtractor controlled
    (REGEX_TEXT yields no skills)."""
    spy = Mock(return_value=EMPTY_VALID_PAYLOAD)
    service = MemoryIngestionService(provider._session_factory)
    result = await service.learn(REGEX_TEXT, llm_extractor=spy)
    assert result["facts"] == []
    assert result["decisions"] == []
    assert result["skills"] == []
    assert result["receipts"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("gate", [0.0, 0.5])
async def test_empty_valid_result_stores_nothing_at_low_gate(provider, gate):
    """BLOCKER regression (code-review round 1): a valid EMPTY LLM result
    stores NOTHING even at the lowest accepted/custom llm_confidence_gate
    values — 0.0 (lowest) and 0.5 (exactly the regex confidence, the value
    the old gate-based approach leaked). In LLM mode the regex pass is
    skipped (include_regex=False), so no regex item can enter the list
    regardless of the gate."""
    spy = Mock(return_value=EMPTY_VALID_PAYLOAD)
    service = MemoryIngestionService(provider._session_factory)
    result = await service.learn(REGEX_TEXT, llm_extractor=spy,
                                 llm_confidence_gate=gate)
    assert result["facts"] == []
    assert result["decisions"] == []
    assert result["skills"] == []
    assert result["receipts"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("gate,exp_facts,exp_decisions", [
    (0.0, 2, 2),   # low custom gate: every validated LLM item passes
    (0.7, 1, 1),   # required default threshold: 0.9 kept, 0.4 dropped
    (0.9, 1, 1),   # >= boundary: the item exactly AT the gate passes
    (0.91, 0, 0),  # just above: the 0.9 items drop too
])
async def test_llm_confidence_gate_boundary(provider, gate, exp_facts, exp_decisions):
    """llm_confidence_gate boundary semantics: >= comparison; low custom
    values keep validated LLM items (regex items are never in the list in
    LLM mode — only LLM items are gated). The payload decisions are
    DISTINCT (different choices) so the write-loop dedup cannot collapse
    them — the counts reflect the gate alone."""
    payload = {
        "facts": [
            {"subject": "Docker", "predicate": "is", "object": "container",
             "confidence": 0.4},
            {"subject": "Python", "predicate": "is", "object": "great",
             "confidence": 0.9},
        ],
        "decisions": [
            {"context": "web server", "choice": "use Caddy", "reason": "simple",
             "alternatives": [], "confidence": 0.4},
            {"context": "database", "choice": "use PostgreSQL", "reason": "ACID",
             "alternatives": [], "confidence": 0.9},
        ],
    }
    spy = Mock(return_value=payload)
    service = MemoryIngestionService(provider._session_factory)
    result = await service.learn(REGEX_TEXT, llm_extractor=spy,
                                 llm_confidence_gate=gate)
    assert len(result["facts"]) == exp_facts
    assert len(result["decisions"]) == exp_decisions
    if exp_facts == 0:
        assert result["receipts"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("gate", [float("nan"), float("inf")])
async def test_llm_confidence_gate_non_finite_stores_nothing(provider, gate):
    """A non-finite llm_confidence_gate (NaN/Inf) drops EVERY validated LLM
    item (no item >= an undefined threshold) — learn() never crashes and no
    regex item leaks. Pure regex mode is unaffected (the gate is not
    applied there)."""
    spy = Mock(return_value=GATE_PAYLOAD)
    service = MemoryIngestionService(provider._session_factory)
    result = await service.learn(REGEX_TEXT, llm_extractor=spy,
                                 llm_confidence_gate=gate)
    assert result["facts"] == []
    assert result["decisions"] == []
    assert result["receipts"] == []

    # regex mode: gate ignored
    service2 = MemoryIngestionService(provider._session_factory)
    result2 = await service2.learn("Docker is container",
                                   llm_confidence_gate=gate)
    assert len(result2["facts"]) == 1
    assert result2["facts"][0]["item"]["confidence"] == 0.5


@pytest.mark.asyncio
async def test_llm_only_confidence_gate(provider):
    """GATE_PAYLOAD (0.4/0.9): the 0.4 items are dropped, the 0.9 items are
    stored; regex mode without a callable keeps 0.5 items unchanged."""
    spy = Mock(return_value=GATE_PAYLOAD)
    service = MemoryIngestionService(provider._session_factory)
    result = await service.learn(REGEX_TEXT, llm_extractor=spy)

    assert len(result["facts"]) == 1
    assert result["facts"][0]["item"]["subject"] == "Python"
    assert result["facts"][0]["item"]["confidence"] == 0.9

    assert len(result["decisions"]) == 1
    d = result["decisions"][0]["item"]
    # the surviving item is the 0.9 variant (alternatives from it) — the
    # 0.4 variant was dropped by the gate; confidence lands in the receipt
    assert d["rejected_alternatives"] == ["use nginx"]
    assert result["decisions"][0]["receipt"]["confidence"] == 0.9

    # regex mode: gate NOT applied
    service2 = MemoryIngestionService(provider._session_factory)
    result2 = await service2.learn("Docker is container")
    assert len(result2["facts"]) == 1
    assert result2["facts"][0]["item"]["confidence"] == 0.5


@pytest.mark.asyncio
async def test_noise_filter_wired_in_regex_mode(provider):
    """A1 noise filter runs in regex mode (compat boundary): demonstrative
    prefix 'This folder is home' -> 0 facts; 'Docker is container' -> 1."""
    service = MemoryIngestionService(provider._session_factory)
    result = await service.learn("This folder is home")
    assert result["facts"] == []

    result2 = await service.learn("Docker is container")
    assert len(result2["facts"]) == 1
    assert result2["facts"][0]["item"]["subject"] == "Docker"


@pytest.mark.asyncio
async def test_legacy_no_callable_path_unchanged(provider):
    """Without a callable, behavior matches the documented legacy regex
    path (fact at confidence 0.5)."""
    service = MemoryIngestionService(provider._session_factory)
    result = await service.learn("Docker is container")
    assert len(result["facts"]) == 1
    assert result["facts"][0]["item"]["confidence"] == 0.5


@pytest.mark.asyncio
async def test_empty_input_never_invokes_llm(provider):
    """Orchestration sits AFTER the empty-text early return: whitespace-only
    input never invokes the callable."""
    spy = Mock(return_value=VALID_FULL_PAYLOAD)
    service = MemoryIngestionService(provider._session_factory)
    result = await service.learn("   ", llm_extractor=spy)
    assert spy.call_count == 0
    assert result["facts"] == []
    assert result["decisions"] == []
    assert result["receipts"] == []
