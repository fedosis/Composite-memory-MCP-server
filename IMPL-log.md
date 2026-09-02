# IMPL log

## cmms-series-fixes — regression tests round 2

- Reworked `tests/test_cmms_series_review_gates.py` to exercise file-backed ingestion/provider races, WriterQueue persistence under lock, full B2 migration accounting, real subprocess guard probes, worker claim selection/conditional-update contention, and a driver-real degraded-WAL seam.
- Added the production claim seam in `storage/outbox.py`: `OutboxRepository.claim_between_select_and_update` is invoked after candidate selection and before the conditional update; it is unset in normal operation.
- RED evidence: copied the new gate file into a detached worktree at parent `9a9dbed`; actual result was `12 failed, 17 passed`.
- GREEN evidence on `62d6b0b` after the rework: `29 passed`.
- Scope explicitly excludes `STATE.md`, `SPEC.md`, `PLAN.md`, FTS5 changes, and unrelated untracked files.

## cmms-series-fixes — regression tests round 3

- F1/W7 now parse durable receipt history, require at least two monotonic reinforcement entries, cover both synchronized session sources, and record/assert one initial winner plus one explicit loser recovery.
- F3 now drives `WriterQueue` through `HermesProvider._handle_batch_write` and canonical `learn()` ingestion under a real file lock; the bounded retry outcome includes the concrete `OperationalError` classification.
- F6 identity/postcondition probes now use the real B2 fixture and Alembic upgrade, with file-backed triggers creating invariant violations detected by production guards in normal and `python -O` subprocesses.
- W9 now records the exact Alembic connection's `busy_timeout` and effective journal mode before migration DDL, while contention is held against the same file-backed fixture.
- RED evidence: detached worktree at parent `9a9dbed` with the strengthened gate file produced `9 failed, 20 passed`.
- GREEN evidence on `c5547a9` after round-3 changes: `29 passed` (`10.08s`).
- Warning policy: normal focused command is `PYTHONPATH=src:. .venv/bin/python -m pytest tests/test_cmms_series_review_gates.py -q`; `-W error` is intentionally documented as environmental and exits 3 during pytest configuration because pytest-asyncio emits `PytestDeprecationWarning` when `asyncio_default_fixture_loop_scope` is unset.
- Scope explicitly excludes `STATE.md`, `SPEC.md`, `PLAN.md`, FTS5 changes, and unrelated untracked files.
