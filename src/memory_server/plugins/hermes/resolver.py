"""Resolve extraction settings without side effects or global caching."""

import os
from dataclasses import dataclass
from typing import Callable, Literal, TypeVar

from memory_server.plugins.hermes.config import HermesPluginConfig
from memory_server.settings import (
    Settings,
    _coerce_confidence_gate,
    _coerce_extraction_mode,
    _coerce_max_input_chars,
    _coerce_timeout_seconds,
)

T = TypeVar("T")


@dataclass(frozen=True)
class ExtractorRuntimeConfig:
    extraction_mode: Literal["regex", "llm", "auto"]
    llm_model: str | None
    llm_timeout_seconds: float
    llm_max_input_chars: int
    llm_confidence_gate: float


def _coerce_model(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    result = value.strip()
    return result or None


def _first_valid(
    candidates: tuple[object, ...], parser: Callable[[object], T | None], default: T
) -> T:
    for candidate in candidates:
        parsed = parser(candidate)
        if parsed is not None:
            return parsed
    return default


def resolve_extractor_settings(
    cfg: HermesPluginConfig, settings: Settings
) -> ExtractorRuntimeConfig:
    env = {
        "extraction_mode": os.environ.get("MEMORY_SERVER_EXTRACTION_MODE"),
        "llm_model": os.environ.get("MEMORY_SERVER_LLM_MODEL"),
        "llm_timeout_seconds": os.environ.get("MEMORY_SERVER_LLM_TIMEOUT_SECONDS"),
        "llm_max_input_chars": os.environ.get("MEMORY_SERVER_LLM_MAX_INPUT_CHARS"),
        "llm_confidence_gate": os.environ.get("MEMORY_SERVER_LLM_CONFIDENCE_GATE"),
    }
    return ExtractorRuntimeConfig(
        extraction_mode=_first_valid(
            (env["extraction_mode"], cfg.extraction_mode, settings.extraction_mode),
            _coerce_extraction_mode,
            "regex",
        ),
        llm_model=_first_valid(
            (env["llm_model"], cfg.llm_model, settings.llm_model),
            _coerce_model,
            None,
        ),
        llm_timeout_seconds=_first_valid(
            (env["llm_timeout_seconds"], cfg.llm_timeout_seconds,
             settings.llm_timeout_seconds),
            _coerce_timeout_seconds,
            15.0,
        ),
        llm_max_input_chars=_first_valid(
            (env["llm_max_input_chars"], cfg.llm_max_input_chars,
             settings.llm_max_input_chars),
            _coerce_max_input_chars,
            8000,
        ),
        llm_confidence_gate=_first_valid(
            (env["llm_confidence_gate"], cfg.llm_confidence_gate,
             settings.llm_confidence_gate),
            _coerce_confidence_gate,
            0.7,
        ),
    )
