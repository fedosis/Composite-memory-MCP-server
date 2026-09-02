"""Tests for the strict LLM response validator (Card A1)."""

import json

from memory_server.extractors.llm_response import (
    ExtractedResult,
    _clean,
    validate_llm_result,
)


def _fact(subject, predicate, obj, confidence=None):
    item = {"subject": subject, "predicate": predicate, "object": obj}
    if confidence is not None:
        item["confidence"] = confidence
    return item


def _decision(context, choice, reason, alternatives, confidence=None):
    item = {
        "context": context,
        "choice": choice,
        "reason": reason,
        "alternatives": alternatives,
    }
    if confidence is not None:
        item["confidence"] = confidence
    return item


def _payload(facts, decisions, extra=None):
    data = {"facts": facts, "decisions": decisions}
    if extra is not None:
        data.update(extra)
    return json.dumps(data)


def _conf_payload(conf_raw):
    """Raw-text payload with a raw confidence literal (NaN/Infinity/...)."""
    return (
        '{"facts": [{"subject": "A", "predicate": "b", "object": "c", '
        '"confidence": %s}], "decisions": []}' % conf_raw
    )


class TestValidateLlmResult:
    """Edge suite for validate_llm_result (SPEC item 1, D2)."""

    # --- valid results ---

    def test_empty_result_valid(self):
        """Empty facts+decisions lists are VALID (intentional empty extraction)."""
        result = validate_llm_result('{"facts": [], "decisions": []}')
        assert result == ExtractedResult((), ())

    def test_one_list_empty_valid(self):
        """One empty list is valid."""
        fact = _fact("Python", "is", "great")
        decision = _decision("ctx", "choice", "reason", [])
        assert validate_llm_result(_payload([], [decision])) == ExtractedResult((), (decision,))
        assert validate_llm_result(_payload([fact], [])) == ExtractedResult((fact,), ())

    def test_valid_with_items(self):
        """Fact and decision with all fields + confidence are returned."""
        fact = _fact("Python", "is", "great", 0.8)
        decision = _decision("ctx", "choice", "reason", ["a", "b"], 0.9)
        result = validate_llm_result(_payload([fact], [decision]))
        assert result == ExtractedResult((fact,), (decision,))

    def test_confidence_optional_and_valid_values(self):
        """Missing confidence valid; 0.0/0.5/0.7/1.0 valid."""
        fact = _fact("A", "is", "B")
        assert validate_llm_result(_payload([fact], [])) is not None
        for value in (0.0, 0.5, 0.7, 1.0):
            f = _fact("A", "is", "B", value)
            assert validate_llm_result(_payload([f], [])) == ExtractedResult((f,), ())

    def test_unknown_keys_ignored(self):
        """Unknown top-level and item keys are ignored (forward compat)."""
        fact = dict(_fact("A", "is", "B"), extra=1)
        result = validate_llm_result(_payload([fact], [], extra={"extra": 1}))
        assert result == ExtractedResult((fact,), ())

    def test_bare_json_and_whitespace_padded(self):
        """Bare JSON string and whitespace-padded input are valid."""
        payload = '{"facts": [], "decisions": []}'
        assert validate_llm_result(payload) == ExtractedResult((), ())
        assert validate_llm_result("  \n" + payload + "\n  ") == ExtractedResult((), ())

    def test_fenced_json_both_forms(self):
        """Exactly one fenced block: ```json and bare ``` both valid."""
        payload = '{"facts": [], "decisions": []}'
        assert validate_llm_result("```json\n" + payload + "\n```") == ExtractedResult((), ())
        assert validate_llm_result("```\n" + payload + "\n```") == ExtractedResult((), ())

    def test_already_parsed_dict(self):
        """Already-parsed dict input is validated directly (D6)."""
        fact = _fact("A", "is", "B")
        result = validate_llm_result({"facts": [fact], "decisions": []})
        assert result == ExtractedResult((fact,), ())

    # --- Unicode punctuation exact sets (D3) ---

    def test_unicode_subject_punct_to_empty_invalid(self):
        """Edge-junk-only subject strips to empty -> whole response None."""
        assert validate_llm_result(_payload([_fact("؟", "is", "B")], [])) is None
        assert validate_llm_result(_payload([_fact("«»", "is", "B")], [])) is None

    def test_unicode_subject_quoted_valid(self):
        """Quoted subject «сервер» edge-strips to non-empty -> valid."""
        fact = _fact("«сервер»", "is", "B")
        assert validate_llm_result(_payload([fact], [])) == ExtractedResult((fact,), ())

    def test_unicode_subject_trailing_punct_valid(self):
        """Trailing U+FF01 / U+060C edge-stripped -> non-empty -> valid."""
        assert validate_llm_result(_payload([_fact("факт！", "is", "B")], [])) is not None
        fact = _fact("abc،", "is", "B")
        assert validate_llm_result(_payload([fact], [])) == ExtractedResult((fact,), ())

    def test_clean_strips_shared_edge_junk(self):
        """_clean pins the shared EDGE_JUNK strip mechanism directly."""
        assert _clean("abc،") == "abc"
        assert _clean("сервер؟") == "сервер"

    def test_internal_punct_schema_valid(self):
        """Internal sentence punctuation/newlines are NOT schema-rejected."""
        fact = _fact("хорошо。быстро", "is", "B")
        assert validate_llm_result(_payload([fact], [])) == ExtractedResult((fact,), ())
        fact2 = _fact("fast\nreally", "is", "B")
        assert validate_llm_result(_payload([fact2], [])) == ExtractedResult((fact2,), ())

    # --- malformed -> whole response None ---

    def test_missing_top_level_key(self):
        """Missing facts or decisions -> None."""
        assert validate_llm_result('{"facts": []}') is None
        assert validate_llm_result('{"decisions": []}') is None

    def test_top_level_not_dict(self):
        """Top-level list/None/int/bool/float -> None."""
        for payload in ("[]", "null", "123", "true", "1.5", '"x"'):
            assert validate_llm_result(payload) is None

    def test_facts_decisions_not_list(self):
        """facts/decisions wrong list type (dict, str) -> None."""
        assert validate_llm_result('{"facts": {}, "decisions": []}') is None
        assert validate_llm_result('{"facts": "x", "decisions": []}') is None
        assert validate_llm_result('{"facts": [], "decisions": {}}') is None
        assert validate_llm_result('{"facts": [], "decisions": "x"}') is None

    def test_fact_item_not_dict(self):
        """Fact item list/str -> None."""
        assert validate_llm_result(_payload([["A"]], [])) is None
        assert validate_llm_result(_payload(["A"], [])) is None

    def test_fact_missing_field(self):
        """Missing subject/predicate/object -> None."""
        assert validate_llm_result(_payload([{"subject": "A", "predicate": "b"}], [])) is None
        assert validate_llm_result(_payload([{"predicate": "b", "object": "c"}], [])) is None
        assert validate_llm_result(_payload([{"subject": "A", "object": "c"}], [])) is None

    def test_fact_field_wrong_type_before_stringification(self):
        """Non-string components rejected BEFORE stringification (review #4)."""
        for item in (
            _fact(5, "is", "c"),
            _fact("A", "is", 5),
            _fact("A", True, "c"),
            _fact("A", "is", 3.14),
            _fact("A", "is", []),
            _fact(None, "is", "c"),
        ):
            assert validate_llm_result(_payload([item], [])) is None

    def test_fact_field_empty_after_clean(self):
        """Empty-after-clean subject/object -> None."""
        assert validate_llm_result(_payload([_fact("", "is", "c")], [])) is None
        assert validate_llm_result(_payload([_fact("   ", "is", "c")], [])) is None
        assert validate_llm_result(_payload([_fact("A", "is", "")], [])) is None

    def test_decision_item_not_dict(self):
        """Decision item list/str -> None."""
        assert validate_llm_result(_payload([], [["d"]])) is None
        assert validate_llm_result(_payload([], ["d"])) is None

    def test_decision_missing_field(self):
        """Missing context/choice/reason -> None."""
        assert validate_llm_result(_payload([], [{"choice": "c", "reason": "r", "alternatives": []}])) is None
        assert validate_llm_result(_payload([], [{"context": "x", "reason": "r", "alternatives": []}])) is None
        assert validate_llm_result(_payload([], [{"context": "x", "choice": "c", "alternatives": []}])) is None

    def test_decision_field_wrong_type(self):
        """Decision field wrong type -> None."""
        item = _decision(5, "c", "r", [])
        assert validate_llm_result(_payload([], [item])) is None

    def test_alternatives_required(self):
        """alternatives missing/None/str/non-string element -> None; [] and strings valid."""
        base = {"context": "x", "choice": "c", "reason": "r"}
        assert validate_llm_result(_payload([], [dict(base)])) is None
        assert validate_llm_result(_payload([], [dict(base, alternatives=None)])) is None
        assert validate_llm_result(_payload([], [dict(base, alternatives="x")])) is None
        assert validate_llm_result(_payload([], [dict(base, alternatives=[5])])) is None
        assert validate_llm_result(_payload([], [dict(base, alternatives=[])])) is not None
        assert validate_llm_result(_payload([], [dict(base, alternatives=["a", "«b»"])])) is not None

    def test_confidence_invalid_whole_response(self):
        """Any invalid confidence -> WHOLE response None."""
        for conf in ("\"0.8\"", "null", "[]", "true", "false", "-0.1", "1.5"):
            assert validate_llm_result(_conf_payload(conf)) is None

    def test_confidence_non_finite_whole_response(self):
        """NaN/Infinity/-Infinity via json.loads -> whole response None (isfinite)."""
        for conf in ("NaN", "Infinity", "-Infinity"):
            assert validate_llm_result(_conf_payload(conf)) is None

    def test_extremely_large_integer_confidence_whole_response(self):
        """Huge integer confidence is invalid in dict and raw-JSON forms."""
        fact = _fact("A", "b", "c", 10**400)
        assert validate_llm_result({"facts": [fact], "decisions": []}) is None

        payload = (
            '{"facts":[{"subject":"A","predicate":"b","object":"c",'
            '"confidence":' + "9" * 400 + '}],"decisions":[]}'
        )
        assert validate_llm_result(payload) is None

    def test_deeply_nested_json_is_invalid(self):
        """Deeply nested JSON returns None instead of escaping RecursionError."""
        payload = json.dumps([[[[]]]])
        for _ in range(1996):
            payload = "[" + payload + "]"
        assert validate_llm_result(payload) is None

    def test_extremely_long_integer_literal_is_invalid(self):
        """JSON integers beyond Python's conversion limit return None."""
        payload = (
            '{"facts":[{"subject":"A","predicate":"b","object":"c",'
            '"confidence":' + "9" * 4301 + '}],"decisions":[]}'
        )
        assert validate_llm_result(payload) is None

    def test_any_malformed_item_invalidates_whole(self):
        """One bad item discards the WHOLE response (no partial extraction)."""
        good_fact = _fact("A", "is", "B")
        bad_fact = _fact(5, "is", "B")
        good_decision = _decision("x", "c", "r", [])
        bad_decision = _decision("x", "c", "r", [5])
        assert validate_llm_result(_payload([good_fact, bad_fact], [])) is None
        assert validate_llm_result(_payload([], [good_decision, bad_decision])) is None
        assert validate_llm_result(_payload([good_fact], [bad_decision])) is None

    # --- fenced JSON negatives ---

    def test_fence_prefix_invalid(self):
        """Prefix text before the fence -> None."""
        payload = '{"facts": [], "decisions": []}'
        assert validate_llm_result("prefix\n```json\n" + payload + "\n```") is None

    def test_fence_suffix_invalid(self):
        """Suffix text after the fence -> None."""
        payload = '{"facts": [], "decisions": []}'
        assert validate_llm_result("```json\n" + payload + "\n```\nsuffix") is None

    def test_double_fence_invalid(self):
        """Multiple fences -> None."""
        payload = '{"facts": [], "decisions": []}'
        double = "```json\n" + payload + "\n```\n```\n" + payload + "\n```"
        assert validate_llm_result(double) is None

    def test_non_json_text_invalid(self):
        """Non-JSON bare text -> None."""
        assert validate_llm_result("not json at all") is None

    def test_fence_around_non_json_invalid(self):
        """Fence around non-JSON payload -> None."""
        assert validate_llm_result("```json\nnot json\n```") is None
