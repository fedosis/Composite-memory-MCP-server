"""Lint changed Python files; preserve and distinguish baseline-only drift."""

import collections
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path("/home/shtorm/.hermes/workspace/gw-slice01a")
BASE = Path(os.environ["TMPDIR"]) / "slice01a-ad50ec3-baseline"
OUT = ROOT / "ops/slice01a_evidence"
PYTHON = "/home/shtorm/.hermes/workspace/.venvs/cmms-verify/bin/python"
env = dict(os.environ, PYTHONPATH=f"{ROOT}/src:{ROOT}", PYTHONDONTWRITEBYTECODE="1")
files = subprocess.check_output(["git", "diff", "ad50ec3", "--name-only"], cwd=ROOT, text=True).splitlines()
files += ["tests/gateway/test_memory_read_indexes.py", "alembic/versions/0006_slice01a_read_indexes.py"]
files = sorted(set(p for p in files if p.endswith(".py") and not p.startswith("ops/")))
all_results = {}
for label, directory in [("current", ROOT), ("baseline", BASE)]:
    paths = [p for p in files if (directory / p).exists()]
    cmd = [PYTHON, "-m", "ruff", "check", "--output-format=json", *paths]
    result = subprocess.run(cmd, cwd=directory, env=env, capture_output=True, text=True)
    (OUT / f"ruff_{label}.json").write_text(result.stdout)
    issues = json.loads(result.stdout)
    all_results[label] = collections.Counter(
        (str(Path(x["filename"]).relative_to(directory)), x["code"], x["location"]["row"], x["message"]) for x in issues
    )
    (OUT / f"ruff_{label}.command.json").write_text(
        json.dumps(
            {"cwd": str(directory), "PYTHONPATH": env["PYTHONPATH"], "argv": cmd, "exit_code": result.returncode},
            indent=2,
        )
    )
new = all_results["current"] - all_results["baseline"]
summary = {
    "current_diagnostics": sum(all_results["current"].values()),
    "baseline_diagnostics": sum(all_results["baseline"].values()),
    "new_diagnostics": sum(new.values()),
    "preexisting": [list(k) for k in all_results["baseline"]],
    "new": [list(k) for k in new],
}
(OUT / "ruff_comparison.json").write_text(json.dumps(summary, indent=2))
print(json.dumps(summary, indent=2))
sys.exit(bool(new))
