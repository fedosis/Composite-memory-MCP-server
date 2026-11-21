"""Emit the canonical schema artifacts shipped with the Hermes plugin distribution.

The artifacts are pure functions of :mod:`memory_server.native.schema_v1`:

* ``schema_v1.json``   — canonical payload plus its ``schema_hash``
* ``system_prompt.txt`` — the static system-prompt block, newline-terminated

``--check`` recomputes both in memory and compares them byte-for-byte with what
is on disk, so drift fails the build instead of silently shipping stale files.
"""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from memory_server.native import schema_v1

SCHEMA_FILENAME = "schema_v1.json"
PROMPT_FILENAME = "system_prompt.txt"


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def render_schema_json() -> str:
    """Render ``schema_v1.json`` exactly as it must exist on disk."""
    payload: dict[str, Any] = schema_v1.canonical_payload()
    document = {
        "v": payload["v"],
        "tools": payload["tools"],
        "prompt": payload["prompt"],
        "schema_hash": schema_v1.schema_hash(),
    }
    return json.dumps(document, ensure_ascii=False, indent=2) + "\n"


def render_system_prompt() -> str:
    """Render ``system_prompt.txt`` exactly as it must exist on disk."""
    return schema_v1.SYSTEM_PROMPT + "\n"


def write_artifacts(out_dir: Path) -> dict[str, str]:
    """Write both artifacts into ``out_dir`` and return their digests.

    Returns sha256 of each written file plus the canonical ``schema_hash``.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    schema_text = render_schema_json()
    prompt_text = render_system_prompt()
    (out / SCHEMA_FILENAME).write_text(schema_text, encoding="utf-8", newline="\n")
    (out / PROMPT_FILENAME).write_text(prompt_text, encoding="utf-8", newline="\n")

    return {
        SCHEMA_FILENAME: _sha256_text(schema_text),
        PROMPT_FILENAME: _sha256_text(prompt_text),
        "schema_hash": schema_v1.schema_hash(),
    }


def _diff_summary(name: str, expected: str, actual: str) -> str:
    diff = list(
        difflib.unified_diff(
            expected.splitlines(),
            actual.splitlines(),
            fromfile=f"canonical/{name}",
            tofile=f"on-disk/{name}",
            lineterm="",
            n=1,
        )
    )
    limit = 20
    if len(diff) > limit:
        diff = diff[:limit] + [f"... ({len(diff) - limit} more diff lines)"]
    return "\n".join(diff)


def check_artifacts(out_dir: Path) -> int:
    """Return 0 when ``out_dir`` matches the canonical schema, 1 on drift."""
    out = Path(out_dir)
    expected = {
        SCHEMA_FILENAME: render_schema_json(),
        PROMPT_FILENAME: render_system_prompt(),
    }

    problems: list[str] = []
    for name, want in expected.items():
        path = out / name
        try:
            have = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            problems.append(f"missing artifact: {path}")
            continue
        except OSError as exc:
            problems.append(f"unreadable artifact: {path} ({exc})")
            continue
        if have != want:
            problems.append(
                f"drift in {path}\n" + _diff_summary(name, want, have)
            )

    if problems:
        print("schema artifacts are out of date:", file=sys.stderr)
        for problem in problems:
            print(problem, file=sys.stderr)
        print(
            f"regenerate with: python -m memory_server.native.gen_schema --out {out}",
            file=sys.stderr,
        )
        return 1

    print(f"schema artifacts up to date: {out} (schema_hash={schema_v1.schema_hash()})")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="memory_server.native.gen_schema",
        description="Generate or verify the canonical CMMS schema v1 artifacts.",
    )
    parser.add_argument(
        "--out",
        required=True,
        type=Path,
        metavar="DIR",
        help="directory to write (or verify) schema_v1.json and system_prompt.txt",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="do not write; exit 1 if the artifacts on disk differ from the canonical schema",
    )
    args = parser.parse_args(argv)

    if args.check:
        return check_artifacts(args.out)

    digests = write_artifacts(args.out)
    for name in (SCHEMA_FILENAME, PROMPT_FILENAME):
        print(f"{args.out / name}: sha256={digests[name]}")
    print(f"schema_hash={digests['schema_hash']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
