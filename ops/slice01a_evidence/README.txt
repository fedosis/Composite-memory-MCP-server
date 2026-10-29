SLICE-01A evidence — interpretation

Code commit: 66511bdb8d8e3ce0af3a6a92684250d85c0ceb15 (base ad50ec3).
The implementation is delivered, but FULL acceptance is STOP, not unconditional GREEN.

Completed green controls:
  gateway_final: 716 passed, 1 warning. No deselection; full tests/gateway/.
  Historical oracle is the actual unmodified SQLiteProvider loaded from git show ad50ec3, not a mock or a reimplementation. All other gateway tests use the real new provider. Full unfiltered traversals: 12,000 pages at limit 1 and 60 pages at limit 200 on the same unchanged 12k data; 13,407 compared pages across all filter cases. equivalence_pages.json stores both complete key sequences per page, fixed upper, after and has_more. summarize.py validates links/order/counts.
  targets_final_v2: 119 passed / 2 deselected (only the two expensive equivalence cases, both covered in gateway_final).
  sqlite_graph_heads: 43 passed (SQLite provider + migration graph + decision-context migrations).
  s203_final: 23 passed, 3 skipped, 3310 deselected. Earlier s203 failed collection because an optional dependency was absent; not counted as a pass.
  s305_changed_stamps: 16 passed / 367 deselected.
  recheck_current_1: all 12 nodes that had failed across the two full post-change profile runs passed on replay. This does NOT turn either full run green.
  ruff_clean: clean changed production/tests excluding the known-drift LanceDB test module, plus all new evidence scripts.
  ruff_comparison: 8 current diagnostics, the same 8 baseline diagnostics, 0 new diagnostics. The complete changed-file ruff invocation still exits 1, not GREEN.

Expected RED:
  red-final.log: 2 failed before production changes on ad50ec3 (missing new index; real max/page TEMP sort).
  drop_red.log / drop_red.xml: 1 actual failure of the SAME positive EQP test after external DROP INDEX. Capability stays True, raw SQL stays selected; no fallback can make the positive EQP assertion pass.

Actual full-run failures (not concealed or relabelled as GREEN):
  baseline_profile_sqlite: 282 passed / 4 failed.
  baseline_recheck (immutable archive, original imports asserted): 283 passed / 3 failed.
  provider_profile_graph: 296 passed / 8 failed (before optional openai installation).
  provider_profile_graph_final: 300 passed / 4 failed (after openai installation). All four stop at unchanged writer-inventory failure handling, src/memory_server/profile_migration.py:2913; exact node names/messages appear in test_results_summary.json and the respective JUnit/logs.
  The baseline and current failed-node sets vary between runs. Baseline rechecks demonstrate instability at the old code too; a shared failure category is not proof that every newly observed node failed on the original run. failure_rechecks.json records five baseline replays and one current replay without broadening scope or mocking inventory.
  changed_providers: 121 passed / 1 failed, test_s302_real_store_validation_cases[corrupt] at tests/test_lancedb_provider.py:736. baseline_lance reproduces that same assertion on ad50ec3.
  hook_drift: 1 failure at tests/test_release_candidate.py:109. README/hook unchanged; all commits explicitly record the authorized bypass reason.
  The initial quick/quick_final runs contain authoring failures fixed before the completed green controls. The early full gateway run was interrupted and is explicitly NOT counted (gateway.aborted.txt).
  git diff --cached --check reports trailing whitespace inside verbatim pytest logs/JUnit failure tracebacks. Those raw evidence bytes are intentionally preserved, not cleaned or passed off as source formatting errors; the implementation-only diff passed --check before its commit.

Measurements:
  Unprefixed measurements.json / measure.log / eqp.json are the final repeat after our pytest processes ended. concurrent_* preserves the earlier contended sample. Synthetic data, not a live profile and not an SLO. SQLite/Python versions and exact environment freeze are included.
  EQP covers actual wide max/page SQL and bound parameters, first/continuation/after-max, 1/200, four/one kinds, created/updated/lifecycle/valid_at. 200 plans recorded; 60 post-change time/unfiltered plans asserted indexed and no TEMP. Lifecycle beliefs explicitly uses its lifecycle index plus TEMP; a lone lifecycle-filtered facts max may also scan/sort. No no-TEMP guarantee applies to lifecycle or JSON valid_at.
  Write workload uses 800 real mutations on independent 12k clones, transaction sizes 1/200, WAL NORMAL and auto-checkpoint disabled during measurement; INSERT includes its PRAGMA table_info overhead, identical in both variants. Pre-workload VACUUM only compacts benchmark clones, never the migration rollback. Single-run write numbers include noise, not a universal slowdown percentage.
  Startup timings instrument the actual once-per-initialize method and full initialize on already-populated 12k/200k databases (not the empty initialization preceding a seed).
  Official Alembic upgrade/repeat/downgrade uses a 12k copy at 0005 with new indexes absent. Every step checks all row hashes, actual FTS MATCH results, unique definitions, all other schema objects, integrity_check and foreign_key_check. Collision preflight refuses the last occupied name without creating earlier indexes. No downgrade below 0005.

Scope/remaining decision:
  No HTTP mounting or forbidden production files changed. 0005 files still read via legacy fallback but no longer qualify as profile-migration sources until offline upgrade; provider-created files remain unstamped.
  No fix to writer-inventory, LanceDB validation, old lint drift or the release hook is included. Achieving unconditional full profile/ruff GREEN needs separate scope authorization. Startup cost at 200k is measured, not hidden behind a per-page cache or a disabled safety check.
  All exact commands: COMMANDS.txt and *.command.json; all result lists: test_results_summary.json. SHA256SUMS covers the evidence files except itself.
