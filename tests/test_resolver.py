import inspect
import os
from dataclasses import FrozenInstanceError, fields

import pytest

from memory_server.plugins.hermes.config import HermesPluginConfig
from memory_server.plugins.hermes.resolver import (
    ExtractorRuntimeConfig,
    resolve_extractor_settings,
)
from memory_server.settings import Settings

ENV = {
    "extraction_mode": "MEMORY_SERVER_EXTRACTION_MODE",
    "llm_model": "MEMORY_SERVER_LLM_MODEL",
    "llm_timeout_seconds": "MEMORY_SERVER_LLM_TIMEOUT_SECONDS",
    "llm_max_input_chars": "MEMORY_SERVER_LLM_MAX_INPUT_CHARS",
    "llm_confidence_gate": "MEMORY_SERVER_LLM_CONFIDENCE_GATE",
}
DEFAULT = {
    "extraction_mode": "regex",
    "llm_model": None,
    "llm_timeout_seconds": 15.0,
    "llm_max_input_chars": 8000,
    "llm_confidence_gate": 0.7,
}


@pytest.fixture
def clear_extraction_env(monkeypatch):
    for key in ENV.values():
        monkeypatch.delenv(key, raising=False)


def make(cfg=None, settings=None):
    return resolve_extractor_settings(
        HermesPluginConfig.from_dict(cfg or {}), Settings(**(settings or {}))
    )


@pytest.mark.parametrize(
    "field,env_value,cfg_value,settings_value,expected",
    [
        pytest.param(
            "extraction_mode", "llm", "auto", "regex", "llm", id="mode-env-yaml-settings"
        ),
        pytest.param(
            "llm_model", " env-model ", "yaml-model", "settings-model", "env-model",
            id="model-env-yaml-settings",
        ),
        pytest.param("llm_timeout_seconds", "2.5", 3.5, 4.5, 2.5, id="timeout-env-yaml-settings"),
        pytest.param("llm_max_input_chars", "3", 4, 5, 3, id="max-env-yaml-settings"),
        pytest.param("llm_confidence_gate", "0.2", 0.3, 0.4, 0.2, id="confidence-env-yaml-settings"),
    ],
)
def test_env_beats_yaml_and_settings(
    monkeypatch, clear_extraction_env, field, env_value, cfg_value,
    settings_value, expected,
):
    monkeypatch.setenv(ENV[field], env_value)
    assert getattr(make({field: cfg_value}, {field: settings_value}), field) == expected


@pytest.mark.parametrize(
    "field,cfg_value,settings_value,expected",
    [
        pytest.param("extraction_mode", "auto", "llm", "auto", id="mode-yaml-settings"),
        pytest.param("llm_model", " yaml-model ", "settings-model", "yaml-model", id="model-yaml-settings"),
        pytest.param("llm_timeout_seconds", 3.5, 4.5, 3.5, id="timeout-yaml-settings"),
        pytest.param("llm_max_input_chars", -4, 5, -4, id="max-yaml-settings-negative"),
        pytest.param("llm_confidence_gate", 0.3, 0.4, 0.3, id="confidence-yaml-settings"),
    ],
)
def test_yaml_beats_settings(clear_extraction_env, field, cfg_value, settings_value, expected):
    assert getattr(make({field: cfg_value}, {field: settings_value}), field) == expected


@pytest.mark.parametrize(
    "field,settings_value,expected",
    [
        pytest.param("extraction_mode", "llm", "llm", id="mode-settings-default"),
        pytest.param("llm_model", " settings-model ", "settings-model", id="model-settings-default"),
        pytest.param("llm_timeout_seconds", 4.5, 4.5, id="timeout-settings-default"),
        pytest.param("llm_max_input_chars", 0, 0, id="max-settings-default-zero"),
        pytest.param("llm_confidence_gate", 0.4, 0.4, id="confidence-settings-default"),
    ],
)
def test_settings_beats_default(clear_extraction_env, field, settings_value, expected):
    assert getattr(make(settings={field: settings_value}), field) == expected


@pytest.mark.parametrize(
    "field,env_value,cfg_value,settings_value,expected",
    [
        pytest.param("extraction_mode", "bad", "auto", "llm", "auto", id="mode-invalid-env-yaml"),
        pytest.param("llm_model", "   ", " yaml-model ", "settings-model", "yaml-model", id="model-blank-env-yaml"),
        pytest.param("llm_timeout_seconds", "nan", 3.0, 4.0, 3.0, id="timeout-nan-env-yaml"),
        pytest.param("llm_timeout_seconds", "+inf", 3.0, 4.0, 3.0, id="timeout-plus-inf-env-yaml"),
        pytest.param("llm_timeout_seconds", "-inf", 3.0, 4.0, 3.0, id="timeout-minus-inf-env-yaml"),
        pytest.param("llm_confidence_gate", "nan", 0.3, 0.4, 0.3, id="confidence-nan-env-yaml"),
        pytest.param("llm_confidence_gate", "+inf", 0.3, 0.4, 0.3, id="confidence-plus-inf-env-yaml"),
        pytest.param("llm_confidence_gate", "-inf", 0.3, 0.4, 0.3, id="confidence-minus-inf-env-yaml"),
        pytest.param("llm_max_input_chars", "3.5", 4, 5, 4, id="max-float-env-yaml"),
    ],
)
def test_invalid_env_falls_through_to_yaml(
    monkeypatch, clear_extraction_env, field, env_value, cfg_value,
    settings_value, expected,
):
    monkeypatch.setenv(ENV[field], env_value)
    assert getattr(make({field: cfg_value}, {field: settings_value}), field) == expected


@pytest.mark.parametrize(
    "field,cfg_value,settings_value,expected",
    [
        pytest.param("extraction_mode", "BAD", "llm", "llm", id="mode-invalid-yaml-settings"),
        pytest.param("llm_model", "  ", " settings-model ", "settings-model", id="model-blank-yaml-settings"),
        pytest.param("llm_timeout_seconds", False, 4.0, 4.0, id="timeout-bool-yaml-settings"),
        pytest.param("llm_timeout_seconds", "not-a-number", 4.0, 4.0, id="timeout-malformed-yaml-settings"),
        pytest.param("llm_confidence_gate", True, 0.4, 0.4, id="confidence-bool-yaml-settings"),
        pytest.param("llm_confidence_gate", "not-a-number", 0.4, 0.4, id="confidence-malformed-yaml-settings"),
        pytest.param("llm_max_input_chars", True, 5, 5, id="max-bool-yaml-settings"),
        pytest.param("llm_max_input_chars", 3.5, 5, 5, id="max-float-yaml-settings"),
    ],
)
def test_invalid_yaml_falls_through_to_settings(
    clear_extraction_env, field, cfg_value, settings_value, expected,
):
    assert getattr(make({field: cfg_value}, {field: settings_value}), field) == expected


@pytest.mark.parametrize(
    "field,settings_value,expected",
    [
        pytest.param("extraction_mode", "BAD", "regex", id="mode-invalid-settings-default"),
        pytest.param("llm_model", "  ", None, id="model-blank-settings-default"),
        pytest.param("llm_timeout_seconds", False, 15.0, id="timeout-bool-settings-default"),
        pytest.param("llm_timeout_seconds", float("nan"), 15.0, id="timeout-nan-settings-default"),
        pytest.param("llm_timeout_seconds", float("inf"), 15.0, id="timeout-inf-settings-default"),
        pytest.param("llm_confidence_gate", True, 0.7, id="confidence-bool-settings-default"),
        pytest.param("llm_confidence_gate", float("nan"), 0.7, id="confidence-nan-settings-default"),
        pytest.param("llm_confidence_gate", float("-inf"), 0.7, id="confidence-minus-inf-settings-default"),
        pytest.param("llm_max_input_chars", True, 8000, id="max-bool-settings-default"),
        pytest.param("llm_max_input_chars", 3.5, 8000, id="max-float-settings-default"),
        pytest.param("llm_max_input_chars", "not-a-number", 8000, id="max-string-settings-default"),
    ],
)
def test_invalid_settings_falls_through_to_default(clear_extraction_env, field, settings_value, expected):
    assert getattr(make(settings={field: settings_value}), field) == expected


@pytest.mark.parametrize("token", ["regex", "llm", "auto"], ids=["regex", "llm", "auto"])
def test_exact_mode_tokens(clear_extraction_env, token):
    assert make(settings={"extraction_mode": token}).extraction_mode == token


@pytest.mark.parametrize("value", [" REGEX ", "LLM", "", "auto "])
def test_mode_rejects_non_exact_tokens(clear_extraction_env, value):
    assert make(settings={"extraction_mode": value}).extraction_mode == "regex"


@pytest.mark.parametrize("field,values", [
    pytest.param("llm_timeout_seconds", [0.1, 1, 10], id="finite-positive-timeout"),
    pytest.param("llm_confidence_gate", [0, 0.5, 1], id="finite-bounded-confidence"),
    pytest.param("llm_max_input_chars", [0, -7, 3], id="all-integer-max-input"),
])
def test_valid_numeric_boundaries(clear_extraction_env, field, values):
    for value in values:
        assert getattr(make(settings={field: value}), field) == value


@pytest.mark.parametrize("field,env_value,cfg_value,expected", [
    pytest.param("llm_timeout_seconds", str(10**1000), 21.0, 21.0, id="timeout-env-huge-invalid-yaml-wins"),
    pytest.param("llm_confidence_gate", str(10**1000), 0.61, 0.61, id="confidence-env-huge-invalid-yaml-wins"),
])
def test_huge_integer_env_falls_through_to_yaml(
    monkeypatch, clear_extraction_env, field, env_value, cfg_value, expected
):
    monkeypatch.setenv(ENV[field], env_value)
    assert getattr(make(cfg={field: cfg_value}), field) == expected


@pytest.mark.parametrize("field,cfg_value,settings_value,expected", [
    pytest.param(
        "llm_timeout_seconds", 10**1000, 22.0, 22.0,
        id="timeout-yaml-huge-invalid-settings-wins",
    ),
    pytest.param("llm_confidence_gate", 10**1000, 0.62, 0.62, id="confidence-yaml-huge-invalid-settings-wins"),
])
def test_huge_integer_yaml_falls_through_to_settings(clear_extraction_env, field, cfg_value, settings_value, expected):
    assert getattr(make(cfg={field: cfg_value}, settings={field: settings_value}), field) == expected


@pytest.mark.parametrize("field,settings_value,expected", [
    pytest.param("llm_timeout_seconds", 10**1000, 15.0, id="timeout-settings-huge-invalid-default-wins"),
    pytest.param("llm_confidence_gate", 10**1000, 0.7, id="confidence-settings-huge-invalid-default-wins"),
])
def test_huge_integer_settings_falls_through_to_default(clear_extraction_env, field, settings_value, expected):
    assert getattr(make(settings={field: settings_value}), field) == expected


def test_dto_is_exact_and_frozen(clear_extraction_env):
    assert [f.name for f in fields(ExtractorRuntimeConfig)] == list(DEFAULT)
    with pytest.raises(FrozenInstanceError):
        make().llm_timeout_seconds = 4.0


def test_signature_is_exact():
    sig = inspect.signature(resolve_extractor_settings)
    assert list(sig.parameters) == ["cfg", "settings"]
    assert all(p.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD for p in sig.parameters.values())
    assert all(p.default is inspect.Parameter.empty for p in sig.parameters.values())
    assert sig.return_annotation is ExtractorRuntimeConfig


def test_resolver_source_has_no_forbidden_settings_lookup_reference():
    from pathlib import Path
    source = Path(resolve_extractor_settings.__code__.co_filename).read_text(encoding="utf-8")
    assert "get_settings" not in source


def test_snapshot_reads_each_key_once(monkeypatch):
    calls = []
    original = os.environ.get

    def counted(key, default=None):
        if key in ENV.values():
            calls.append(key)
        return original(key, default)

    monkeypatch.setattr(os.environ, "get", counted)
    resolve_extractor_settings(HermesPluginConfig.from_dict({}), Settings())
    assert calls == list(ENV.values())


def test_inputs_and_environment_are_unchanged(monkeypatch, clear_extraction_env):
    cfg = HermesPluginConfig.from_dict({"llm_model": " yaml "})
    settings = Settings(llm_model=" setting ")
    before_cfg, before_settings, before_env = repr(cfg), repr(settings), dict(os.environ)
    resolve_extractor_settings(cfg, settings)
    assert repr(cfg) == before_cfg and repr(settings) == before_settings
    assert dict(os.environ) == before_env


def test_deterministic_and_no_cache(clear_extraction_env):
    cfg = HermesPluginConfig.from_dict({"llm_max_input_chars": 3})
    first = resolve_extractor_settings(cfg, Settings())
    second = resolve_extractor_settings(cfg, Settings())
    assert first == second
    assert resolve_extractor_settings(cfg, Settings(llm_max_input_chars=4)).llm_max_input_chars == 3
