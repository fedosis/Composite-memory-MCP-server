"""Build-time catalogue projection checks, including optimized Python."""
from __future__ import annotations

import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize("optimized", [False, True])
@pytest.mark.parametrize("field,value", [
    ("permission", "profile.read"), ("method", "GET"),
    ("path", "/tampered"), ("operation", "unknown.operation"),
])
def test_catalog_mismatch_rejected_at_build(tmp_path, optimized, field, value):
    script = '''
import sys
from dataclasses import replace
from types import SimpleNamespace
from memory_server.gateway_contracts.ports import HttpConfig
from memory_server.transports.http import app
specs = list(app.S06_ROUTE_SPECS)
specs[4] = replace(specs[4], **{sys.argv[2]: sys.argv[3]})
app.S06_ROUTE_SPECS = tuple(specs)
access = SimpleNamespace(record_unauthenticated=lambda: None,
                         publish_committed=lambda: None, saturation=lambda: None)
deps = SimpleNamespace(access=access)
try:
    app.create_http_app(HttpConfig(control_path=sys.argv[1]), deps)
except ValueError:
    print("CATALOG_MISMATCH_REJECTED")
else:
    sys.exit("tampered spec accepted")
'''
    result = subprocess.run(
        [sys.executable, *(["-O"] if optimized else []), "-c", script, str(tmp_path), field, value],
        capture_output=True, text=True, env=os.environ.copy(), check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "CATALOG_MISMATCH_REJECTED"
