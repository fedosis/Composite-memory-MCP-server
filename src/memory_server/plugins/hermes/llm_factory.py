from __future__ import annotations

import json
import logging
import math
import os
from pathlib import Path
from typing import Callable
from urllib.parse import urlparse

import httpx

from memory_server.extractors.llm_response import ExtractedResult, validate_llm_result
from memory_server.plugins.hermes.resolver import ExtractorRuntimeConfig
from memory_server.settings import get_openai_api_key

logger = logging.getLogger(__name__)
LLMExtractorFn = Callable[[str], ExtractedResult | None]
_DEFAULT_BASE_URL = "https://api.openai.com/v1"
_MAX_ATTEMPTS = 3
_RETRYABLE_EXCEPTIONS = (httpx.ConnectError, httpx.ReadError,
                         httpx.WriteError, httpx.PoolTimeout)
_SYSTEM_PROMPT = """Return ONLY JSON, no prose, no markdown fences:
{"facts": [{"subject": str, "predicate": str, "object": str, "confidence": float 0..1}],
 "decisions": [{"context": str, "choice": str, "reason": str, "alternatives": [str]}]}
- Facts are durable subject-predicate-object statements about the user, their
  projects, systems, decisions, preferences — stated as concrete claims.
- SKIP ephemeral chatter, code fragments, commands, tool output, syntax noise,
  line-level trivia, anything that is not a durable claim.
- If nothing durable is found, return {"facts": [], "decisions": []}.
- Confidence: 1.0 for explicitly stated facts, 0.7-0.9 for inferred."""


def _warn(category: str, *, mode: object = None) -> None:
    safe_mode = mode if isinstance(mode, str) and mode in {"llm", "auto"} else "unknown"
    logger.warning("LLM extractor failure category=%s mode=%s", category, safe_mode)


def _valid_base_url(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    candidate = value.strip()
    try:
        parsed = urlparse(candidate)
    except ValueError:
        return None
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        return None
    return candidate.rstrip("/")


def _capture(cfg: ExtractorRuntimeConfig, hermes_home: str | Path,
             mode: str) -> tuple[str, str, str, float, Path] | None:
    if not isinstance(cfg, ExtractorRuntimeConfig):
        _warn("invalid_config", mode=mode)  # noqa: E702
        return None
    model, timeout = cfg.llm_model, cfg.llm_timeout_seconds
    if not isinstance(model, str) or not model.strip():
        _warn("missing_model", mode=mode)  # noqa: E702
        return None
    if (not isinstance(timeout, (int, float)) or isinstance(timeout, bool)):
        _warn("invalid_timeout", mode=mode)  # noqa: E702
        return None
    timeout = float(timeout)
    if not math.isfinite(timeout) or timeout <= 0:
        _warn("invalid_timeout", mode=mode)  # noqa: E702
        return None
    try:
        home = Path(hermes_home)
    except (TypeError, ValueError, OSError):
        _warn("invalid_home", mode=mode)  # noqa: E702
        return None
    key = get_openai_api_key()                    # exactly once, build time
    if not isinstance(key, str) or not key.strip():
        _warn("missing_api_key", mode=mode)  # noqa: E702
        return None
    configured = os.environ.get("OPENAI_BASE_URL")
    base = _valid_base_url(configured)
    if base is None:
        if configured is not None and configured.strip():
            _warn("invalid_base_url", mode=mode)  # noqa: E702
            return None
        base = _DEFAULT_BASE_URL
    return model.strip(), key, base, timeout, home


class _RetryableHTTP(Exception):  # noqa: N818
    pass


class _TerminalHTTP(Exception):  # noqa: N818
    pass


def _request_once(client: httpx.Client, *, base_url: str, model: str,
                  key: str, text: str, timeout: float) -> object:
    response = client.post(
        f"{base_url}/chat/completions",
        headers={"Authorization": f"Bearer {key}",
                 "Content-Type": "application/json"},
        json={"model": model, "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": text},
        ],
              "temperature": 0},
        timeout=timeout,
    )
    if response.status_code == 429 or 500 <= response.status_code <= 599:
        raise _RetryableHTTP(response.status_code)
    if not 200 <= response.status_code < 300:
        raise _TerminalHTTP(response.status_code)
    return response.json()


def _invoke(*, base_url: str, model: str, key: str, text: str,
            timeout: float, mode: str) -> ExtractedResult | None:
    try:
        with httpx.Client() as client:
            for attempt in range(1, _MAX_ATTEMPTS + 1):
                try:
                    raw = _request_once(client, base_url=base_url, model=model,
                                        key=key, text=text, timeout=timeout)
                except _RetryableHTTP:
                    if attempt == _MAX_ATTEMPTS:
                        _warn("retryable_http_exhausted", mode=mode)  # noqa: E702
                        return None
                # PoolTimeout is inside this tuple and therefore before broad
                # TimeoutException; it receives three attempts.
                except _RETRYABLE_EXCEPTIONS:
                    if attempt == _MAX_ATTEMPTS:
                        _warn("retryable_transport_exhausted", mode=mode)  # noqa: E702
                        return None
                except httpx.TimeoutException:
                    _warn("non_retryable_timeout", mode=mode)  # noqa: E702
                    return None
                except _TerminalHTTP:
                    _warn("non_retryable_http", mode=mode)  # noqa: E702
                    return None
                except (json.JSONDecodeError, ValueError, TypeError):
                    _warn("response_parse_failure", mode=mode)
                    return None
                except Exception:
                    _warn("unexpected_failure", mode=mode)  # noqa: E702
                    return None
                else:
                    try:
                        content = raw["choices"][0]["message"]["content"]  # type: ignore[index]
                    except (KeyError, IndexError, TypeError):
                        _warn("validation_failure", mode=mode)
                        return None
                    try:
                        result = validate_llm_result(content)  # exactly once
                    except (ValueError, TypeError):
                        _warn("validation_failure", mode=mode)
                        return None
                    if result is None:
                        _warn("validation_failure", mode=mode)
                    return result
    except Exception:
        # Includes Client construction, __enter__, and __exit__. A constructed
        # client is owned by `with` and is closed on every ordinary return path.
        _warn("client_failure", mode=mode)  # noqa: E702
        return None


def build_llm_extractor_from_cfg(
    cfg: ExtractorRuntimeConfig, *,
    hermes_home: str | Path = str(Path.home() / ".hermes"),
) -> LLMExtractorFn | None:
    mode = getattr(cfg, "extraction_mode", None)
    if mode == "regex":                         # zero-touch fast path
        return None
    if not isinstance(mode, str) or mode not in {"llm", "auto"}:
        _warn("invalid_mode", mode=mode)  # noqa: E702
        return None
    captured = _capture(cfg, hermes_home, mode)
    if captured is None:
        return None
    model, key, base_url, timeout, captured_home = captured
    del captured_home                         # explicit context, never reread

    def extractor(text: str) -> ExtractedResult | None:
        if not isinstance(text, str):
            _warn("invalid_input", mode=mode)  # noqa: E702
            return None
        return _invoke(base_url=base_url, model=model, key=key, text=text,
                       timeout=timeout, mode=mode)
    return extractor
