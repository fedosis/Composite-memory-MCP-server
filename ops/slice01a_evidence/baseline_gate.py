"""Re-run the immutable baseline archive without changing the active worktree."""

import io
import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path

ROOT = Path("/home/shtorm/.hermes/workspace/gw-slice01a")
OUT = ROOT / "ops/slice01a_evidence"
BASE = Path(os.environ["TMPDIR"]) / "slice01a-ad50ec3-baseline"
BASE.mkdir(exist_ok=True)
archive = subprocess.check_output(["git", "archive", "ad50ec3"], cwd=ROOT)
with tarfile.open(fileobj=io.BytesIO(archive)) as stream:
    stream.extractall(BASE, filter="data")
# Python -m starts from cwd. The alias permits original src-layout imports to
# take precedence over the mandated absolute PYTHONPATH; child tests themselves
# prepend their own absolute source_root as in the unchanged baseline helper.
alias = BASE / "memory_server"
if not alias.exists():
    alias.symlink_to(BASE / "src/memory_server", target_is_directory=True)
env = dict(os.environ, PYTHONPATH=f"{ROOT}/src:{ROOT}", PYTHONDONTWRITEBYTECODE="1")
PYTHON = "/home/shtorm/.hermes/workspace/.venvs/cmms-verify/bin/python"
check = [
    PYTHON,
    "-c",
    "import memory_server.profile_migration as m; import storage.models.fact as f; "
    "print(m.__file__, f.__file__, m.ACCEPTED_SQLITE_SCHEMA_REVISIONS); "
    'assert m.ACCEPTED_SQLITE_SCHEMA_REVISIONS == frozenset({"0005"}); '
    'assert not any(i.name == "ix_facts_created_type_id" for i in f.FactORM.__table__.indexes)',
]
print(subprocess.check_output(check, cwd=BASE, env=env, text=True), flush=True)
cmd = [
    PYTHON,
    "-m",
    "pytest",
    "-p",
    "no:cacheprovider",
    f"--junitxml={OUT}/baseline_recheck.xml",
    "tests/test_profile_migration.py",
    "tests/test_sqlite_provider.py",
    "-q",
]
(OUT / "baseline_recheck.command.json").write_text(
    json.dumps(
        {
            "source": "git archive ad50ec3",
            "cwd": str(BASE),
            "env": {"PYTHONPATH": env["PYTHONPATH"], "TMPDIR": env["TMPDIR"]},
            "check": check,
            "argv": cmd,
        },
        indent=2,
    )
)
with (OUT / "baseline_recheck.log").open("w") as log:
    result = subprocess.run(cmd, cwd=BASE, env=env, stdout=log, stderr=subprocess.STDOUT)
print("exit", result.returncode, flush=True)
print((OUT / "baseline_recheck.log").read_text()[-14000:], flush=True)
sys.exit(result.returncode)
