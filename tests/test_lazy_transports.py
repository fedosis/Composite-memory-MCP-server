"""Fresh-process probes: native imports must never load stdio/MCP eagerly."""

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def probe(code):
    env = {**os.environ, "PYTHONPATH": f"{ROOT / 'src'}:{ROOT}", "PYTHONDONTWRITEBYTECODE": "1"}
    result = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


BLOCK = '''
import sys
from importlib.abc import MetaPathFinder
class NoMCP(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "mcp" or fullname.startswith("mcp."):
            raise ModuleNotFoundError("blocked missing mcp", name="mcp")
sys.meta_path.insert(0, NoMCP())
'''


def test_lazy_package_import_without_mcp():
    probe(BLOCK + '''
import memory_server.transports as t
assert t.__all__ == ["mcp", "run"]
assert "memory_server.transports.mcp_stdio" not in sys.modules
try:
    t.unknown
except AttributeError:
    pass
else:
    raise AssertionError("unknown export")
''')


def test_lazy_export_preserves_dependency_failure():
    probe(BLOCK + '''
import memory_server.transports as t
for name in ("mcp", "run"):
    try:
        getattr(t, name)
    except ModuleNotFoundError as exc:
        assert exc.name == "mcp" and str(exc) == "blocked missing mcp"
    else:
        raise AssertionError("dependency failure suppressed")
''')


def test_lazy_exports_with_dependencies():
    probe('''
import memory_server.transports as t
from memory_server.transports import mcp_stdio
assert t.mcp is mcp_stdio.mcp
assert t.run is mcp_stdio.run
''')


def test_native_path_does_not_import_stdio():
    probe(BLOCK + '''
from memory_server.plugins.hermes.provider import HermesProvider
assert "memory_server.transports.mcp_stdio" not in sys.modules
assert "mcp" not in sys.modules
''')
