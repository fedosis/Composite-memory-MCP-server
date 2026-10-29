"""Run a recorded gate with the mandated absolute import path."""

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path("/home/shtorm/.hermes/workspace/gw-slice01a")
OUT = ROOT / "ops/slice01a_evidence"
PYTHON = "/home/shtorm/.hermes/workspace/.venvs/cmms-verify/bin/python"
name, *args = sys.argv[1:]
env = dict(os.environ, PYTHONPATH=f"{ROOT}/src:{ROOT}", PYTHONDONTWRITEBYTECODE="1")
cmd = [PYTHON, "-m", "pytest", "-p", "no:cacheprovider", f"--junitxml={OUT}/{name}.xml", *args]
(OUT / f"{name}.command.json").write_text(
    json.dumps(
        {
            "cwd": str(ROOT),
            "env": {k: env[k] for k in ("PYTHONPATH", "PYTHONDONTWRITEBYTECODE", "TMPDIR")},
            "argv": cmd,
        },
        indent=2,
    )
)
with (OUT / f"{name}.log").open("w") as log:
    result = subprocess.run(cmd, env=env, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
print(name, "exit", result.returncode, flush=True)
print((OUT / f"{name}.log").read_text()[-18000:])
sys.exit(result.returncode)
