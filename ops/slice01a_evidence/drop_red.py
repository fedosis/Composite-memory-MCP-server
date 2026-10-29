"""Expected RED: run the SAME positive EQP test after external DROP INDEX."""

import json
import os
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path("/home/shtorm/.hermes/workspace/gw-slice01a")
OUT = ROOT / "ops/slice01a_evidence"
assert os.environ["PYTHONPATH"] == f"{ROOT}/src:{ROOT}"


class DropAfterInitialization:
    def pytest_runtest_call(self, item):
        provider, db = item.funcargs["seeded"]
        assert provider._read_index_capable is True
        with sqlite3.connect(db) as conn:
            for name in item.module.INDEXES.values():
                conn.execute(f"DROP INDEX {name}")
            conn.commit()
        assert provider._read_index_capable is True
        print("External DROP after initialize; immutable capability=True; raw path remains selected.")


args = [
    "-p",
    "no:cacheprovider",
    "tests/gateway/test_memory_read_indexes.py::test_real_read_page_eqp_uses_each_index_without_temp_sort",
    "-q",
    f"--junitxml={OUT}/drop_red.xml",
]
(OUT / "drop_red.command.json").write_text(
    json.dumps(
        {
            "cwd": str(ROOT),
            "PYTHONPATH": os.environ["PYTHONPATH"],
            "argv": [sys.executable, str(Path(__file__).resolve())],
            "pytest_args": args,
            "expected_exit": 1,
        },
        indent=2,
    )
)
result = pytest.main(args, plugins=[DropAfterInitialization()])
assert result == 1, f"Expected a real failing positive EQP test, got exit {result}"
sys.exit(result)
