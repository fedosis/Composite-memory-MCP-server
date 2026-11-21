"""Contract tests for the canonical CMMS native schema v1 (slice T1).

These tests pin the transcribed constants to the live provider and pin the
generated plugin artifacts to the canonical payload. Everything runs on
``tmp_path``; no live CMMS data, config, or lock is touched.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from memory_server.native import gen_schema, schema_v1
from memory_server.plugins.hermes.provider import HermesProvider

REPO_ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_DIR = REPO_ROOT / "plugin-dist" / "memory_server_proxy"
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_DOCUMENT_KEYS = {"v", "tools", "prompt", "schema_hash"}


def test_tool_names_exactly_15() -> None:
    assert len(schema_v1.TOOL_NAMES) == 15
    assert len(set(schema_v1.TOOL_NAMES)) == 15
    assert "add_relation" not in schema_v1.TOOL_NAMES
    live_names = [schema["name"] for schema in HermesProvider().get_tool_schemas()]
    assert list(schema_v1.TOOL_NAMES) == live_names


def test_schemas_match_embedded_provider() -> None:
    schema_v1.assert_matches_provider(HermesProvider())


def test_prompt_matches_embedded() -> None:
    assert schema_v1.SYSTEM_PROMPT == HermesProvider().system_prompt_block()


def test_schema_hash_stable() -> None:
    first = schema_v1.schema_hash()
    second = schema_v1.schema_hash()
    assert first == second
    assert _HEX64.match(first), f"not lowercase hex sha256: {first!r}"
    assert len(first) == 64


def test_plugin_artifacts_match_canonical() -> None:
    payload = schema_v1.canonical_payload()

    document = json.loads((ARTIFACT_DIR / gen_schema.SCHEMA_FILENAME).read_text(encoding="utf-8"))
    assert set(document) == _DOCUMENT_KEYS
    assert document["v"] == payload["v"] == schema_v1.PROTO_SCHEMA_VERSION
    assert document["tools"] == payload["tools"]
    assert document["prompt"] == payload["prompt"]
    assert document["schema_hash"] == schema_v1.schema_hash()

    prompt_text = (ARTIFACT_DIR / gen_schema.PROMPT_FILENAME).read_text(encoding="utf-8")
    assert prompt_text == schema_v1.SYSTEM_PROMPT + "\n"


def test_gen_schema_check_detects_drift(tmp_path: Path) -> None:
    out = tmp_path / "plugin-dist" / "memory_server_proxy"
    gen_schema.write_artifacts(out)

    assert gen_schema.main(["--out", str(out), "--check"]) == 0

    prompt_path = out / gen_schema.PROMPT_FILENAME
    pristine = prompt_path.read_text(encoding="utf-8")
    prompt_path.write_text(pristine + "drifted line\n", encoding="utf-8")

    assert gen_schema.main(["--out", str(out), "--check"]) == 1
    # --check must never repair the files behind the caller's back.
    assert prompt_path.read_text(encoding="utf-8") == pristine + "drifted line\n"

    prompt_path.write_text(pristine, encoding="utf-8")

    assert gen_schema.main(["--out", str(out), "--check"]) == 0
