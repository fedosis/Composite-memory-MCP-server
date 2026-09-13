# S5-02D summary

Cause: the pinned harness constructs `/tmp/test_outbox_<uuid>.db` at `tests/test_outbox.py:44`; the certified sandbox refuses the later SQLite open. Its migration test constructs the project-tree DB and INI at `tests/test_outbox.py:1226-1227`, then writes the INI with `open(..., "w")`/`stream.write(...)` at `tests/test_outbox.py:1230-1231`. The captured exception is `sqlite3.OperationalError: unable to open database file`.

Harness-only change: `tests/test_outbox.py` adds `_sandbox_tmp_root(fallback)` and routes both `_make_engine_and_factory` and the migration test through it. It does not modify production code or assertions. The diff contains only scratch-path/helper/context lines; assertion text and expressions are unchanged.

Derived provenance: `/tmp/cmms-baseline-run` came from the pinned archive at SHA `da1cd0e98b64aa2dfbd02dde7ddf70e8d67ce975`; patch SHA-256 is in `S5-02D_HARNESS.patch.sha256`. No production file and no assertion differs between pinned and derived except harness scratch-path lines and the minimum older-file context/import lines needed to apply that same harness patch.

Counts:
- pristine pinned reproduction: 16 failed, 5 passed, 26 errors; PRODUCER_EXIT=1; RUNNER_EXIT=1.
- derived baseline: 6 failed, 41 passed, 0 setup errors; PRODUCER_EXIT=1; RUNNER_EXIT=1.
- HEAD control: 47 passed; PRODUCER_EXIT=0; RUNNER_EXIT=0.
- mutant (resolver forced to `/tmp`): 16 failed, 5 passed, 26 errors; PRODUCER_EXIT=1; RUNNER_EXIT=1.

The derived six failures are a known baseline deviation: the old pinned test uses SentenceTransformer and the certified run cannot load Hugging Face files offline. This card does not alter that behavior or mask it. The mutant/control pair demonstrates that the path repair, rather than hidden assertions, removes the setup-error path on HEAD.

Pinned-root integrity: final check reports rev-parse `da1cd0e98b64aa2dfbd02dde7ddf70e8d67ce975`, empty `git status --porcelain=v1`, and empty `git diff --stat`.

Deviation: the originally requested “all green” applies to HEAD certification; the derived baseline has zero setup errors but six genuine old-embedder failures, so it is not green. No production change was needed.
