"""Recheck observed profile failures on original and current code, no mocks."""

import json
import os
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path("/home/shtorm/.hermes/workspace/gw-slice01a")
OUT = ROOT / "ops/slice01a_evidence"
BASE = Path(os.environ["TMPDIR"]) / "slice01a-ad50ec3-baseline"
PYTHON = "/home/shtorm/.hermes/workspace/.venvs/cmms-verify/bin/python"
env = dict(os.environ, PYTHONPATH=f"{ROOT}/src:{ROOT}", PYTHONDONTWRITEBYTECODE="1")
nodes = set()
for name in ("provider_profile_graph", "provider_profile_graph_final"):
    for case in ET.parse(OUT / f"{name}.xml").iter("testcase"):
        if case.find("failure") is not None:
            nodes.add("tests/test_profile_migration.py::" + case.attrib["name"])
results = []
for label, directory, repeats in [("baseline", BASE, 5), ("current", ROOT, 1)]:
    for attempt in range(repeats):
        stem = f"recheck_{label}_{attempt + 1}"
        cmd = [PYTHON, "-m", "pytest", "-p", "no:cacheprovider", f"--junitxml={OUT}/{stem}.xml", "-q", *sorted(nodes)]
        (OUT / f"{stem}.command.json").write_text(
            json.dumps({"cwd": str(directory), "PYTHONPATH": env["PYTHONPATH"], "argv": cmd}, indent=2)
        )
        with (OUT / f"{stem}.log").open("w") as log:
            proc = subprocess.run(cmd, cwd=directory, env=env, stdout=log, stderr=subprocess.STDOUT)
        tree = ET.parse(OUT / f"{stem}.xml")
        failures = [
            {"name": case.attrib["name"], "message": case.find("failure").attrib.get("message", "")}
            for case in tree.iter("testcase")
            if case.find("failure") is not None
        ]
        result = {
            "version": label,
            "attempt": attempt + 1,
            "exit": proc.returncode,
            "tested": len(nodes),
            "failures": failures,
        }
        results.append(result)
        (OUT / "failure_rechecks.json").write_text(json.dumps(results, indent=2))
        print(json.dumps(result), flush=True)
