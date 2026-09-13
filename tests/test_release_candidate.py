"""Release-candidate packaging and documentation checks."""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
RELEASE_VERSION = "0.12.0b1"


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


def test_sdist_contains_changelog(tmp_path):
    """Build and verify the sdist tarball actually carries CHANGELOG.md."""
    import shutil
    import subprocess
    import sys
    import tarfile

    build_root = tmp_path / "project"
    shutil.copytree(
        ROOT,
        build_root,
        ignore=shutil.ignore_patterns(".git", ".venv", "dist", "*.egg-info", "__pycache__"),
    )
    subprocess.run(
        [sys.executable, "-m", "build", "--sdist", "--no-isolation"],
        cwd=build_root,
        capture_output=True,
        check=True,
    )
    sdists = sorted((build_root / "dist").glob("*.tar.gz"))
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
    assert "python -c \"import memory_server, storage; assert memory_server.__version__ == '0.12.0b1'\"" in ci
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


def test_s405_documented_cli_flags_are_registered_by_the_real_app():
    from inspect import signature

    from memory_server.cli import doctor, migrate_profile_storage

    doctor_flags = {param.name for param in signature(doctor).parameters.values()}
    migrate_flags = {param.name for param in signature(migrate_profile_storage).parameters.values()}
    assert {"hermes_home", "json_output"} <= doctor_flags
    assert {
        "hermes_home", "source_sql", "target_root", "strategy", "run_id", "apply",
        "confirm_target", "attest_runtimes_stopped", "confirm_embedding_plan",
        "allow_network_embedding", "resume", "rollback", "json_output",
    } <= migrate_flags
    usage = _read_text("docs/USAGE.md")
    for flag in (
        "--hermes-home", "--source-sql", "--target-root", "--strategy", "--run-id",
        "--apply", "--confirm-target", "--attest-runtimes-stopped",
        "--confirm-embedding-plan", "--allow-network-embedding", "--resume", "--rollback", "--json",
    ):
        assert flag in usage


def test_s405_exit_code_table_matches_committed_mapping():
    from memory_server.profile_migration import EXIT_CODE_CONDITIONS, exit_code_for_diagnostic

    usage = _read_text("docs/USAGE.md")
    expected = {
        0: "doctor fully OK; side-effect-free dry-run with no blockers; apply/resume complete; rollback verified",
        1: "doctor WARN/ERROR or dry-run/precondition blocker before mutation",
        2: "Typer/CLI usage error",
        3: "apply/resume failed after run directory/manifest creation; manifest is resumable or rollback-capable",
        4: "rollback failed or publication ambiguity requires manual escalation",
        5: "invalid/tampered/unknown manifest",
        6: "lock/runtime-writer state cannot be proven safe",
    }
    assert set(EXIT_CODE_CONDITIONS) == set(range(7))
    assert exit_code_for_diagnostic("E_PATH_OUTSIDE_ROOT") == 1
    assert exit_code_for_diagnostic("CLI_USAGE", phase="usage") == 2
    assert exit_code_for_diagnostic("E_MIGRATION_STAGE_FAILED", phase="apply") == 3
    assert exit_code_for_diagnostic("E_PUBLICATION_AMBIGUOUS", phase="rollback") == 4
    assert exit_code_for_diagnostic("E_MANIFEST_TAMPERED", phase="manifest") == 5
    assert exit_code_for_diagnostic("E_LOCK_ENTRY_UNSAFE", phase="lock") == 6
    for code, text in expected.items():
        assert f"`{code}`" in usage
        assert text in usage


def test_s405_documented_json_shape_is_owned_by_cli_payload_builder():
    from pathlib import Path
    from types import SimpleNamespace

    from memory_server.cli import _migration_dry_run_payload
    from memory_server.profile_migration import EmbeddingPlan, MigrationRequest

    request = MigrationRequest(Path("/synthetic/hermes"), run_id="a" * 32)
    plan = SimpleNamespace(
        report={
            "schema_version": 1,
            "mode": "dry-run",
            "strategy": "rebuild-from-profile-sql",
            "lock_availability": "unknown",
        },
        embedding=EmbeddingPlan(digest="b" * 64),
        layout=SimpleNamespace(data_root=Path("/synthetic/data")),
    )
    payload = _migration_dry_run_payload(plan, request)
    assert {
        "schema_version", "mode", "strategy", "lock_availability", "run_id", "profile_home", "embedding"
    } <= payload.keys()
    usage = _read_text("docs/USAGE.md")
    keys = (
        "schema_version", "mode", "strategy", "source_sql", "target", "lock_availability",
        "embedding", "warnings", "blockers", "planned_operations", "proposed_manifest_path",
    )
    for key in keys:
        assert f'"{key}"' in usage


def test_s405_adr_records_a6_bounded_rebuild_divergence_verbatim():
    adr = _read_text("docs/ADR.md")
    record = (
        'rebuild creates the "decides" edge whenever BOTH endpoints exist in the eligible corpus, '
        'which can include edges the runtime incremental path would have dropped; '
        'runtime/outbox semantics are unchanged.'
    )
    assert " ".join(record.split()) in " ".join(adr.split())


def test_s405_operator_docs_pin_n2_n3_and_safe_stop_obligations():
    paths = ("docs/INTEGRATION.md", "docs/USAGE.md", "README.md", "CHANGELOG.md")
    docs = "\n".join(_read_text(path) for path in paths)
    for phrase in (
        "WAL/SHM/journal", "Never manually checkpoint", "Never delete WAL/SHM", "Never copy WAL/SHM",
        "degraded-safe", "recall is unavailable until migration", "without following them",
        "independently verified stopped", "root locks are complete only after full rollout",
        "lock_availability: unknown", "preserve-only; not imported", "API cost",
    ):
        assert phrase.lower() in docs.lower()


def test_s405_docs_do_not_expose_live_home_or_secret_configuration():
    paths = ("docs/ADR.md", "docs/INTEGRATION.md", "docs/USAGE.md", "README.md", "CHANGELOG.md")
    docs = "\n".join(_read_text(path) for path in paths)
    assert "/home/shtorm" not in docs
    assert "BEGIN PRIVATE KEY" not in docs
    assert "api_key:" not in docs.lower()
    assert "password:" not in docs.lower()


def test_s405_manifest_states_and_recovery_claims_match_code_contract():
    from memory_server.profile_migration import _CHECKPOINTS, _RUN_STATUSES

    usage = _read_text("docs/USAGE.md")
    for state in _CHECKPOINTS | _RUN_STATUSES:
        assert f"`{state}`" in usage
    for phrase in ("quarantine", "source preservation", "resume digest", "config_digest", "rolled_back"):
        assert phrase.lower() in usage.lower()
