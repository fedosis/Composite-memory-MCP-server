# Paired refactor characterization harnesses

The C07, C08, and C09 application tests are generated/compared against legacy behavior using external subject/oracle inputs. TODO: procedure not recovered from the refactor project. Do not invent oracle-generation steps. These modules intentionally skip during ordinary pytest collection unless their required environment is provided:

- C07 (`tests/test_application_retrieval.py`): `C07_SUBJECT`, `C07_ORACLE`, `C07_SCRATCH`.
- C08 (`tests/test_application_beliefs.py`): `C08_SUBJECT`, `C08_ORACLE`, `C08_SCRATCH`.
- C09 (`tests/test_application_operations.py`): `C09_SUBJECT`, `C09_ORACLE`, `C09_SCRATCH`.

Run them only from the recovered harness procedure with disposable scratch storage and the appropriate subject/oracle. The tests are not silently treated as passing when that setup is absent.

## MCP default coverage and optional C00 parity

`tests/test_mcp_contract_parity.py` and `tests/test_mcp_runtime_shutdown.py`
self-provision storage with `tmp_path` / `tmp_path_factory`; neither module skips
at import time or requires ambient storage paths. Subprocesses use the current
`sys.executable`, overridden with `CMMS_MCP_PYTHON` if needed. A missing,
non-executable, failing, or timed-out interpreter fails with a clear diagnostic.
Child environments disable bytecode writes and place HOME, HERMES_HOME, TMPDIR,
SQLite, LanceDB and graph snapshots beneath their disposable root.

Default parity coverage keeps the 16-tool inventory, all 21 happy-path success
and transcript-hash assertions, all 16 invalid-input cases (error flags/prefixes
except the existing ping/metrics exceptions), graph key order/set, projection
JSON/error flags, non-projection reraising, imports, and single-service dispatch.
All shutdown assertions run by default: lock exclusion before construction,
lock inode/reacquisition, S1 schedule/ownership, close order, admission errors,
CLI compatibility, path containment, owned worker/compaction/native retention,
and absence of a create_task fallback. The former C10 future gate is retired;
the candidate's compatibility alias is allowed but a duplicate graph function
body is forbidden by AST inspection.

Only C00 comparisons request `CMMS_C00_ORACLE`: initialize equality, full tool
schemas and contract-file inventory, happy/invalid normalized wire equality,
invalid oracle error flags, projection normalized equality, and exact graph
payload equality. The original oracle SHA-256 pin is unchanged. Missing env
skips only those cases with a reason naming `CMMS_C00_ORACLE`; a supplied but
missing or hash-mismatched oracle fails. No oracle-generation procedure is
invented here.

To keep test artifacts outside a checkout, set `PYTHONDONTWRITEBYTECODE=1`,
set `TMPDIR` to disposable storage, disable pytest's cache with
`-p no:cacheprovider`, and set `--basetemp` under that same disposable storage.
