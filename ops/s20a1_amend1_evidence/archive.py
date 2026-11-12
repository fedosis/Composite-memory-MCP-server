import hashlib
import json
from pathlib import Path
import shutil
import subprocess

root = Path('/home/shtorm/.hermes/workspace/gw-s20')
scratch = Path(__file__).resolve().parent
archive = root / 'ops/s20a1_amend1_evidence'
archive.mkdir(parents=True, exist_ok=True)
files = ['tests/gateway/test_s19p_prerequisites.py', 'tests/gateway/test_admission_dry_run.py']
old = subprocess.check_output(['git', 'show', f'9639098:{files[0]}'], cwd=root, text=True)
new = (root/files[0]).read_text()
replacement = '''        # S20_RULINGS_009: S20_DETAIL.md:191 moves the configured callback before provider recipes.
        assert custody.dependencies.outbox_worker is None, "S19P_STARTUP_PREPROVIDER outbox_worker"
        assert custody.dependencies.sqlite is None, "S19P_STARTUP_PREPROVIDER sqlite"
        assert custody.dependencies.vector is None, "S19P_STARTUP_PREPROVIDER vector"
        assert custody.dependencies.graph is None, "S19P_STARTUP_PREPROVIDER graph"'''
assert old.replace('        assert custody.dependencies.outbox_worker is not None', replacement) == new
print('S19P_LITERAL_PRESERVATION PASS: entire file differs only by the authorized replacement')
for tag in ('original-candidate', 'candidate-1', 'candidate-final-1', 'candidate-final-2', 'candidate-final-3', 'base-overlay', 'base-overlay-trace', 's20a1-targets', 'amended-files'):
    target = archive/tag
    target.mkdir(exist_ok=True)
    meta = json.loads((scratch/tag/'result.json').read_text())
    for name in ('output.log', 'junit.xml', 'result.json'):
        source = scratch/tag/name
        if name in meta['sha256']:
            assert hashlib.sha256(source.read_bytes()).hexdigest() == meta['sha256'][name]
        shutil.copyfile(source, target/name)
    if tag not in ('original-candidate', 'candidate-1'):
        for path in files:
            assert hashlib.sha256((root/path).read_bytes()).hexdigest() == meta['test_sha256'][path]
    print(tag, meta['exit'], meta['suites'])
for name in ('run.py', 'startup_trace.py', 'archive.py'):
    shutil.copyfile(scratch/name, archive/name)
argv = ['/home/shtorm/.hermes/workspace/gw-harness/venv/bin/ruff', 'check', *files, 'tests/gateway/test_online_rebuild.py', 'tests/gateway/test_rebuild_recovery.py']
result = subprocess.run(argv, cwd=root, capture_output=True, text=True)
(archive/'ruff.log').write_text(result.stdout + result.stderr)
(archive/'ruff.json').write_text(json.dumps(dict(argv=argv, cwd=str(root), exit=result.returncode), indent=2)+'\n')
assert result.returncode == 0
assert not subprocess.check_output(['git', 'diff', '9639098', '--', 'src', 'storage', 'migrations', 'pyproject.toml'], cwd=root)
print('PRODUCTION_UNCHANGED PASS')
manifest = {str(p.relative_to(archive)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(archive.rglob('*')) if p.is_file() and p.name != 'SHA256SUMS.json'}
(archive/'SHA256SUMS.json').write_text(json.dumps(manifest, indent=2)+'\n')
print('ARCHIVE_VERIFIED', archive)
