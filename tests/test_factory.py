from __future__ import annotations

import inspect
import logging
import math
import typing
from pathlib import Path
from unittest.mock import Mock

import httpx
import pytest

import memory_server.plugins.hermes.llm_factory as factory
from memory_server.extractors.llm_response import ExtractedResult
from memory_server.plugins.hermes.llm_factory import LLMExtractorFn, build_llm_extractor_from_cfg
from memory_server.plugins.hermes.resolver import ExtractorRuntimeConfig


class FakeClient:
    instances: list["FakeClient"] = []
    plan: list[object] = []

    def __init__(self, *args, **kwargs):
        self.calls, self.closed = [], False
        type(self).instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def close(self):
        self.closed = True

    def post(self, url, *, headers, json, timeout):
        self.calls.append({"url": url, "headers": headers, "json": json, "timeout": timeout})
        action = type(self).plan[len(self.calls) - 1]
        if isinstance(action, BaseException):
            raise action
        return action


def response(status=200, payload=None, body=None):
    if body is not None:
        return httpx.Response(status, json=body)
    content = payload if payload is not None else '{"facts": [], "decisions": []}'
    return httpx.Response(status, json={
        "choices": [{"message": {"content": content}}],
        "usage": {},
    })


def make_cfg(mode="llm", model="model", timeout=5.0, max_input_chars=8000):
    return ExtractorRuntimeConfig(mode, model, timeout, max_input_chars, 0.5)


def build_valid(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    return build_llm_extractor_from_cfg(make_cfg())


@pytest.fixture(autouse=True)
def fake_transport(monkeypatch):
    FakeClient.instances = []
    FakeClient.plan = []
    monkeypatch.setattr(factory.httpx, "Client", FakeClient)


def test_signature_and_alias():
    sig = inspect.signature(build_llm_extractor_from_cfg)
    assert list(sig.parameters) == ["cfg", "hermes_home"]
    assert sig.parameters["cfg"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert sig.parameters["cfg"].default is inspect.Parameter.empty
    assert sig.parameters["hermes_home"].kind is inspect.Parameter.KEYWORD_ONLY
    assert sig.parameters["hermes_home"].default == str(Path.home() / ".hermes")
    assert inspect.get_annotations(build_llm_extractor_from_cfg, eval_str=False)["return"] == "LLMExtractorFn | None"
    expected = typing.Callable[[str], ExtractedResult | None]
    assert typing.get_type_hints(build_llm_extractor_from_cfg)["return"] == expected | None
    assert LLMExtractorFn == expected
    assert typing.get_origin(LLMExtractorFn) is typing.get_origin(expected)
    assert ExtractorRuntimeConfig.__dataclass_params__.frozen


def test_regex_is_zero_touch(monkeypatch, caplog):
    monkeypatch.setattr(factory, "get_openai_api_key", lambda: pytest.fail("credential touched"))
    monkeypatch.setattr(factory.httpx, "Client", lambda: pytest.fail("client touched"))
    monkeypatch.setattr(factory.logger, "warning", lambda *a, **k: pytest.fail("warning touched"))
    assert build_llm_extractor_from_cfg(make_cfg(mode="regex")) is None
    assert not FakeClient.instances
    assert not caplog.records


@pytest.mark.parametrize(
    "cfg, env, home, category",
    [
        (make_cfg(model=None), {}, "/tmp/home", "missing_model"),
        (make_cfg(), {"OPENAI_API_KEY": ""}, "/tmp/home", "missing_api_key"),
        (None, {"OPENAI_API_KEY": "key"}, "/tmp/home", "invalid_mode"),
        (make_cfg(mode="bad"), {"OPENAI_API_KEY": "key"}, "/tmp/home", "invalid_mode"),
        (make_cfg(), {"OPENAI_API_KEY": "key", "OPENAI_BASE_URL": "http://["}, "/tmp/home", "invalid_base_url"),
        (make_cfg(timeout=0), {"OPENAI_API_KEY": "key"}, "/tmp/home", "invalid_timeout"),
        (make_cfg(timeout=math.nan), {"OPENAI_API_KEY": "key"}, "/tmp/home", "invalid_timeout"),
        (make_cfg(), {"OPENAI_API_KEY": "key"}, object(), "invalid_home"),
    ],
)
def test_build_failures_are_closed(monkeypatch, caplog, cfg, env, home, category):
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    with caplog.at_level(logging.WARNING, logger=factory.logger.name):
        assert build_llm_extractor_from_cfg(cfg, hermes_home=home) is None
    assert not FakeClient.instances
    assert len(caplog.records) == 1
    assert f"category={category}" in caplog.text


@pytest.mark.parametrize("mode", [[], {}, ["llm"]])
def test_unhashable_modes_fail_closed(monkeypatch, caplog, mode):
    cfg = make_cfg(mode=mode)
    monkeypatch.setenv("OPENAI_API_KEY", "key")
    with caplog.at_level(logging.WARNING):
        assert build_llm_extractor_from_cfg(cfg) is None
    assert not FakeClient.instances
    assert len(caplog.records) == 1
    assert "category=invalid_mode mode=unknown" in caplog.text


@pytest.mark.parametrize(
    "failure",
    [httpx.ConnectError("x"), httpx.ReadError("x"), httpx.WriteError("x"), httpx.PoolTimeout("x")],
)
def test_retryable_transport_retries_three_times(monkeypatch, caplog, failure):
    failure.request = httpx.Request("POST", "https://unit.test")
    FakeClient.plan = [failure, failure, failure]
    with caplog.at_level(logging.WARNING):
        assert build_valid(monkeypatch)("input") is None
    assert len(FakeClient.instances[-1].calls) == 3
    assert FakeClient.instances[-1].closed
    assert len(caplog.records) == 1
    assert "category=retryable_transport_exhausted mode=llm" in caplog.text


@pytest.mark.parametrize("status", [429, 500, 599])
def test_retryable_status_retries_three_times(monkeypatch, caplog, status):
    FakeClient.plan = [response(status), response(status), response(status)]
    with caplog.at_level(logging.WARNING):
        assert build_valid(monkeypatch)("input") is None
    assert len(FakeClient.instances[-1].calls) == 3
    assert FakeClient.instances[-1].closed
    assert len(caplog.records) == 1
    assert "retryable_http_exhausted" in caplog.text


@pytest.mark.parametrize("failure", [httpx.ReadTimeout("x"), httpx.TimeoutException("x"), RuntimeError("secret-error")])
def test_non_retryable_exceptions_are_single_attempt(monkeypatch, caplog, failure):
    failure.request = httpx.Request("POST", "https://unit.test") if isinstance(failure, httpx.RequestError) else None
    FakeClient.plan = [failure]
    with caplog.at_level(logging.WARNING):
        assert build_valid(monkeypatch)("input") is None
    assert len(FakeClient.instances[-1].calls) == 1
    assert len(caplog.records) == 1


@pytest.mark.parametrize("status", [400, 404])
def test_non_retryable_status_is_single_attempt(monkeypatch, caplog, status):
    FakeClient.plan = [response(status)]
    with caplog.at_level(logging.WARNING):
        assert build_valid(monkeypatch)("input") is None
    assert len(FakeClient.instances[-1].calls) == 1
    assert len(caplog.records) == 1
    assert "non_retryable_http" in caplog.text


def test_pooltimeout_precedes_timeout(monkeypatch):
    assert issubclass(httpx.PoolTimeout, httpx.TimeoutException)
    FakeClient.plan = [httpx.PoolTimeout("x")] * 3
    assert build_valid(monkeypatch)("x") is None
    assert len(FakeClient.instances[-1].calls) == 3
    assert FakeClient.instances[-1].closed
    FakeClient.plan = [httpx.ReadTimeout("x")]
    assert build_valid(monkeypatch)("x") is None
    assert len(FakeClient.instances[-1].calls) == 1
    assert FakeClient.instances[-1].closed


def test_validator_once_first_and_after_retry(monkeypatch):
    payload = '{"facts": [], "decisions": []}'
    validator = Mock(return_value=ExtractedResult((), ()))
    monkeypatch.setattr(factory, "validate_llm_result", validator)
    FakeClient.plan = [response(200, payload)]
    assert build_valid(monkeypatch)("x") == ExtractedResult((), ())
    validator.assert_called_once_with(payload)
    validator.reset_mock()
    FakeClient.plan = [httpx.ReadError("x"), response(200, payload)]
    FakeClient.plan[0].request = httpx.Request("POST", "https://unit.test")
    assert build_valid(monkeypatch)("x") == ExtractedResult((), ())
    validator.assert_called_once_with(payload)


@pytest.mark.parametrize("body", ["not-json", {"facts": [{"bad": 1}], "decisions": []}])
def test_parse_and_validation_failures_close_client(monkeypatch, caplog, body):
    item = response(payload=body) if isinstance(body, dict) else httpx.Response(200, content=body.encode())
    FakeClient.plan = [item]
    with caplog.at_level(logging.WARNING):
        assert build_valid(monkeypatch)("x") is None
    assert FakeClient.instances[-1].closed
    assert len(caplog.records) == 1


def test_validator_none_and_exception_close_client(monkeypatch, caplog):
    for validator in [Mock(return_value=None), Mock(side_effect=ValueError("validator-secret"))]:
        FakeClient.plan = [response()]
        monkeypatch.setattr(factory, "validate_llm_result", validator)
        with caplog.at_level(logging.WARNING):
            assert build_valid(monkeypatch)("x") is None
        assert FakeClient.instances[-1].closed
        if validator.side_effect:
            assert "validation_failure" in caplog.text
    assert len(caplog.records) == 2


def test_valid_response_returns_frozen_result_and_closes(monkeypatch):
    FakeClient.plan = [response()]
    result = build_valid(monkeypatch)("x")
    assert result == ExtractedResult((), ())
    assert FakeClient.instances[-1].closed


def test_real_chat_completion_body_extracts_content(monkeypatch):
    payload = '{"facts": [{"subject": "Пользователь", "predicate": "любит", "object": "чай"}], "decisions": []}'
    FakeClient.plan = [response(payload=payload)]
    result = build_valid(monkeypatch)("x")
    assert result is not None
    assert result.facts[0]["object"] == "чай"


def test_missing_chat_completion_content_is_validation_failure(monkeypatch, caplog):
    FakeClient.plan = [response(body={"choices": [{"message": {}}], "usage": {}})]
    with caplog.at_level(logging.WARNING):
        assert build_valid(monkeypatch)("x") is None
    assert "category=validation_failure mode=llm" in caplog.text


def test_fenced_chat_completion_content_is_validated_as_string(monkeypatch):
    payload = "```json" + chr(10) + '{"facts": [], "decisions": []}' + chr(10) + "```"
    FakeClient.plan = [response(payload=payload)]
    assert build_valid(monkeypatch)("x") == ExtractedResult((), ())


def test_sentinel_and_mutation_proof(monkeypatch):
    sentinel = "BEGIN::" + ("x" * 100) + "::END"
    cfg_a = make_cfg(model="model-A", max_input_chars=3)
    monkeypatch.setenv("OPENAI_API_KEY", "key-A")
    monkeypatch.setenv("OPENAI_BASE_URL", "https://first.example/v1")
    fn = build_llm_extractor_from_cfg(cfg_a)
    cfg_b = make_cfg(model="model-B", max_input_chars=999)
    cfg_a = cfg_b
    monkeypatch.setenv("OPENAI_API_KEY", "key-B")
    monkeypatch.setenv("OPENAI_BASE_URL", "https://second.example/v1")
    FakeClient.plan = [response()]
    fn(sentinel)
    call = FakeClient.instances[-1].calls[0]
    assert call["json"]["messages"][0]["role"] == "system"
    assert "Return ONLY JSON" in call["json"]["messages"][0]["content"]
    assert call["json"]["messages"][1]["role"] == "user"
    assert call["json"]["messages"][1]["content"] == sentinel
    assert call["json"]["model"] == "model-A"
    assert call["headers"]["Authorization"] == "Bearer key-A"
    assert call["url"] == "https://first.example/v1/chat/completions"


def test_warning_does_not_leak_secrets_or_body(monkeypatch, caplog):
    secret = "secret-key-123"
    body = "request-sentinel-and-response-body"
    url = "https://secret.example/v1?token=hidden"
    monkeypatch.setenv("OPENAI_API_KEY", secret)
    monkeypatch.setenv("OPENAI_BASE_URL", url)
    # URL is rejected at build, so none of its sensitive text may be logged.
    with caplog.at_level(logging.WARNING):
        assert build_llm_extractor_from_cfg(make_cfg()) is None
    assert secret not in caplog.text
    assert "Bearer" not in caplog.text
    assert body not in caplog.text
    assert url not in caplog.text


def test_invocation_failure_does_not_leak_request_or_response(monkeypatch, caplog):
    secret = "fake-key-like-secret-456"
    sentinel = "request-sentinel-789"
    response_body = "response-body-fake-key-012"
    exception_text = "transport-exception-sensitive-345"
    monkeypatch.setenv("OPENAI_API_KEY", secret)
    monkeypatch.setenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
    FakeClient.plan = [RuntimeError(f"{exception_text}: {sentinel} {response_body}")]
    extractor = build_llm_extractor_from_cfg(make_cfg())
    assert extractor is not None
    with caplog.at_level(logging.WARNING):
        assert extractor(sentinel) is None
    for value in [secret, sentinel, response_body, "Bearer", exception_text]:
        assert value not in caplog.text


def test_client_construction_and_context_fail_closed(monkeypatch, caplog):
    class BrokenClient:
        def __init__(self):
            raise RuntimeError("constructor-secret")

    monkeypatch.setattr(factory.httpx, "Client", BrokenClient)
    with caplog.at_level(logging.WARNING):
        assert build_valid(monkeypatch)("x") is None
    assert len(caplog.records) == 1
    assert "client_failure" in caplog.text

    class BrokenEnter(FakeClient):
        def __enter__(self):
            raise RuntimeError("enter-secret")

    monkeypatch.setattr(factory.httpx, "Client", BrokenEnter)
    with caplog.at_level(logging.WARNING):
        assert build_valid(monkeypatch)("x") is None
    assert len(caplog.records) == 2
    assert "enter-secret" not in caplog.text
