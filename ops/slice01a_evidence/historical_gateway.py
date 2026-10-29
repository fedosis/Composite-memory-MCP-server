"""Full gateway gate, with the actual ad50ec3 provider as the equivalence oracle."""

import dataclasses
import json
import os
import subprocess
import sys
import types
from pathlib import Path

import pytest

ROOT = Path("/home/shtorm/.hermes/workspace/gw-slice01a")
OUT = ROOT / "ops/slice01a_evidence"
assert os.environ["PYTHONPATH"] == f"{ROOT}/src:{ROOT}"
source = subprocess.check_output(
    ["git", "show", "ad50ec3:src/memory_server/providers/sqlite_provider.py"], cwd=ROOT, text=True
)
mod = types.ModuleType("immutable_ad50ec3")
exec(compile(source, "ad50ec3/sqlite_provider.py", "exec"), mod.__dict__)


class ReferencePlugin:
    module = None

    def pytest_collection_modifyitems(self, items):
        for item in items:
            if item.module.__name__.endswith("test_memory_read_indexes"):
                self.module = item.module
                self.module.REFERENCE_PROVIDER = mod.SQLiteProvider
                self.module.EQUIVALENCE_TRACE = []

    def pytest_sessionfinish(self, session, exitstatus):
        assert self.module is not None
        trace = self.module.EQUIVALENCE_TRACE

        def encode(value):
            if dataclasses.is_dataclass(value):
                return dataclasses.asdict(value)
            raise TypeError(type(value).__name__)

        (OUT / "equivalence_pages.json").write_text(json.dumps(trace, default=encode, separators=(",", ":")))
        print(f"Historical reference: ad50ec3; compared pages={len(trace)}; output=equivalence_pages.json")


args = ["-p", "no:cacheprovider", "tests/gateway/", "-q", f"--junitxml={OUT}/gateway_final.xml"]
(OUT / "gateway_final.command.json").write_text(
    json.dumps(
        {
            "cwd": str(ROOT),
            "PYTHONPATH": os.environ["PYTHONPATH"],
            "argv": [sys.executable, str(Path(__file__).resolve())],
            "pytest_args": args,
            "reference": "git show ad50ec3:src/memory_server/providers/sqlite_provider.py (unmodified class)",
        },
        indent=2,
    )
)
sys.exit(pytest.main(args, plugins=[ReferencePlugin()]))
