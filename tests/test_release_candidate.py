"""Release-candidate packaging and documentation checks."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RELEASE_VERSION = "0.11.0b1"


def _read_text(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_release_version_is_consistent_and_pep440_beta():
    pyproject = tomllib.loads(_read_text("pyproject.toml"))
    init_text = _read_text("src/memory_server/__init__.py")

    assert pyproject["project"]["version"] == RELEASE_VERSION
    assert pyproject["project"]["readme"] == "README.md"
    assert f'__version__ = "{RELEASE_VERSION}"' in init_text
    assert re.fullmatch(r"\d+\.\d+\.\d+b\d+", RELEASE_VERSION)
    assert pyproject["project"]["requires-python"] == ">=3.11"


def test_runtime_dependency_metadata_covers_server_startup_metrics_imports():
    """Clean wheel installs must pull metrics imports used by memory-server serve."""
    pyproject = tomllib.loads(_read_text("pyproject.toml"))
    dependencies = {dep.split(">=", maxsplit=1)[0].lower() for dep in pyproject["project"]["dependencies"]}

    assert "prometheus-client" in dependencies
    assert "opentelemetry-api" in dependencies


def test_runtime_dependency_metadata_keeps_vector_backends_optional():
    """Base installs must not require optional vector/embed backend packages."""
    pyproject = tomllib.loads(_read_text("pyproject.toml"))
    dependencies = {dep.split(">=", maxsplit=1)[0].lower() for dep in pyproject["project"]["dependencies"]}

    assert "qdrant-client" not in dependencies
    assert "lancedb" not in dependencies
    assert "pyarrow" not in dependencies
    assert "numpy" not in dependencies
    assert "sentence-transformers" not in dependencies


def test_release_metadata_has_owner_approved_mit_attribution():
    pyproject = tomllib.loads(_read_text("pyproject.toml"))
    project = pyproject["project"]
    authors = project["authors"]
    classifiers = set(project["classifiers"])
    urls = project["urls"]
    license_text = _read_text("LICENSE")
    authors_text = _read_text("AUTHORS.md")
    readme = _read_text("README.md")

    assert project["license"] == {"text": "MIT"}
    assert {author["name"] for author in authors} == {"Shtorm", "FedosIS"}
    assert {author.get("email") for author in authors if author["name"] == "FedosIS"} == {"fedosis@gmail.com"}
    assert all(author.get("email") != "https://www.moltbook.com/u/shtorm" for author in authors)
    assert "Development Status :: 4 - Beta" in classifiers
    assert "License :: OSI Approved :: MIT License" in classifiers
    assert "Programming Language :: Python :: 3.11" in classifiers
    assert "Programming Language :: Python :: 3.12" in classifiers
    assert urls["Homepage"] == "https://github.com/fedosis/Composite-memory-MCP-server"
    assert urls["Repository"] == "https://github.com/fedosis/Composite-memory-MCP-server"
    assert "MIT License" in license_text
    assert "Copyright (c) 2026 Shtorm, FedosIS" in license_text
    assert "Shtorm" in authors_text
    assert "AI agent, primary code author" in authors_text
    assert "https://www.moltbook.com/u/shtorm" in authors_text
    assert "FedosIS" in authors_text
    assert "project owner, initiator, and maintainer" in authors_text
    assert "fedosis@gmail.com" in authors_text
    assert "[AUTHORS.md](AUTHORS.md)" in readme


def test_changelog_documents_v011_beta_features_and_limits():
    changelog = _read_text("CHANGELOG.md")

    assert re.search(r"^## \[?0\.11\.0b1\]?", changelog, flags=re.MULTILINE)
    assert "LongMemEval-S" in changelog
    assert "Memory Admission Gate" in changelog
    assert "Known limitations" in changelog
    assert "github prerelease tag" in changelog.lower()
    assert "not published to pypi" in changelog.lower()
    assert "official mcp registry" in changelog.lower()
    assert "smithery" in changelog.lower()
    assert "glama" in changelog.lower()
    assert "not published" in changelog.lower()


def test_readme_has_clean_first_run_mcp_and_hermes_paths():
    readme = _read_text("README.md")

    assert "## First-run install" in readme
    assert "python3.11 -m venv .venv" in readme
    assert "pip install ." in readme
    assert "memory-server serve" in readme
    assert "mcpServers" in readme
    assert "memory-server install-hermes-plugin --hermes-home ~/.hermes/profiles/coder" in readme
    assert "hermes gateway restart" in readme


def test_manifest_in_includes_changelog():
    """MANIFEST.in ensures CHANGELOG.md lands in the sdist tarball."""
    manifest = _read_text("MANIFEST.in")
    assert "include CHANGELOG.md" in manifest


def test_sdist_contains_changelog():
    """Build and verify the sdist tarball actually carries CHANGELOG.md."""
    import subprocess
    import sys
    import tarfile

    subprocess.run(
        [sys.executable, "-m", "build", "--sdist"],
        cwd=ROOT,
        capture_output=True,
        check=True,
    )
    sdists = sorted(ROOT.glob("dist/*.tar.gz"))
    assert sdists, "no sdist found after build"
    with tarfile.open(str(sdists[-1])) as tf:
        names = tf.getnames()
    # Strip leading directory to get relative filenames
    rel = {"/".join(n.split("/")[1:]) for n in names}
    assert "CHANGELOG.md" in rel, f"CHANGELOG.md not in sdist: {sorted(rel)}"


def test_ci_python_versions_are_aligned():
    ci = _read_text(".github/workflows/ci.yml")

    assert "release-artifacts" in ci
    assert 'python-version: "3.12"' in ci
    # All CI jobs should reference the same Python version
    count_312 = ci.count('python-version: "3.12"')
    assert count_312 >= 5, f"Expected ≥5 references to 3.12, got {count_312}"

    assert "python -m build" in ci
    assert "pip install dist/*.whl" in ci
    assert "python -c \"import memory_server, storage; assert memory_server.__version__ == '0.11.0b1'\"" in ci
    assert "memory-server --help" in ci


def test_ci_clean_wheel_smoke_exercises_serve_startup():
    """Release CI must start memory-server serve from the clean wheel and call ping."""
    ci = _read_text(".github/workflows/ci.yml")

    assert "StdioServerParameters" in ci
    assert "ClientSession" in ci
    assert 'os.path.join(venv_bin, "memory-server")' in ci
    assert "asyncio.wait_for(main(), timeout=15)" in ci
    assert "cwd=tmpdir" in ci
    assert 'await session.call_tool("ping", arguments={})' in ci
