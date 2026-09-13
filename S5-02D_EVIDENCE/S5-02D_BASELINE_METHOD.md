# S5-02D baseline measurement method

Pinned baseline root: `/tmp/cmms-baseline-da1cd0e`
Pinned SHA: `da1cd0e98b64aa2dfbd02dde7ddf70e8d67ce975`
Harness patch SHA-256: recorded in `S5-02D_HARNESS.patch.sha256`.

`/tmp/cmms-baseline-run` was created from `git -C /tmp/cmms-baseline-da1cd0e archive HEAD | tar -x -C /tmp/cmms-baseline-run` and received the same harness-only scratch-path change: SQLite and migration INI scratch artifacts resolve through `_sandbox_tmp_root(...)`, which uses `GATE_TMP` when configured and otherwise the pytest fallback.

No production file and no assertion differs between the pinned tree and the derived copy except the harness scratch-path lines (plus the minimum import/helper/context lines required to apply that harness patch to the older pinned test file). The derived copy is not a replacement for the pinned root and was used only for measurement.

Measured baseline runs:
- Derived: 0 setup errors; `6 failed, 41 passed` (the six failures are the old baseline's unavailable Hugging Face embedder, not setup errors).
- Pinned: `16 failed, 5 passed, 26 errors`; setup errors are the denied SQLite scratch writes.

The pinned root remained read-only and was checked again at the end.
