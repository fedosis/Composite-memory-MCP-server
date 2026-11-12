import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent
VENV = Path('/home/shtorm/.hermes/workspace/gw-harness/venv')
tree, tag, *targets = sys.argv[1:]
gate = ROOT / tag
gate.mkdir(parents=True, exist_ok=False)
for name in ('home', 'hermes', 'data', 'tmp'):
    (gate / name).mkdir()
env = dict(PATH=f'{VENV}/bin:/usr/local/bin:/usr/bin:/bin', HOME=str(gate/'home'), HERMES_HOME=str(gate/'hermes'), TMPDIR=str(gate/'tmp'), XDG_DATA_HOME=str(gate/'data'), TZ='UTC', LC_ALL='C.UTF-8', PYTHONDONTWRITEBYTECODE='1', PYTHONPATH=f'{tree}/src:{tree}', MEMORY_SERVER_STORAGE_MODE='standalone', MEMORY_SERVER_DATA_ROOT=str(gate/'data'), MEMORY_SERVER_DB_URL=f'sqlite+aiosqlite:///{gate}/data/memory.db', MEMORY_SERVER_LANCEDB_PATH=str(gate/'data/lancedb'), MEMORY_SERVER_GRAPH_SNAPSHOT_PATH=str(gate/'data/graph.json'), HF_HUB_OFFLINE='1', TRANSFORMERS_OFFLINE='1')
argv = [str(VENV/'bin/python'), '-m', 'pytest', '-q', '-p', 'no:cacheprovider', *targets, f'--basetemp={gate}/tmp/bt', f'--junitxml={gate}/junit.xml', '--tb=short']
if tag.endswith('-trace'):
    env['PYTHONPATH'] += f':{ROOT}'
    argv += ['-p', 'startup_trace']
start = time.monotonic()
with (gate/'output.log').open('w') as output:
    result = subprocess.run(argv, cwd=tree, env=env, stdout=output, stderr=subprocess.STDOUT)
meta = dict(argv=argv, cwd=tree, env=env, exit=result.returncode, seconds=time.monotonic()-start, head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=tree, text=True).strip())
meta['test_sha256'] = {p: hashlib.sha256((Path(tree)/p).read_bytes()).hexdigest() for p in ('tests/gateway/test_s19p_prerequisites.py', 'tests/gateway/test_admission_dry_run.py')}
meta['sha256'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (gate/'output.log', gate/'junit.xml') if p.exists()}
if (gate/'junit.xml').exists():
    meta['suites'] = [s.attrib for s in ET.parse(gate/'junit.xml').iter('testsuite')]
(gate/'result.json').write_text(json.dumps(meta, indent=2)+'\n')
print(json.dumps(meta, indent=2))
sys.exit(result.returncode)
