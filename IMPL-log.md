# IMPL log

## cmms-series-fixes — regression tests round 2

- Reworked `tests/test_cmms_series_review_gates.py` to exercise file-backed ingestion/provider races, WriterQueue persistence under lock, full B2 migration accounting, real subprocess guard probes, worker claim selection/conditional-update contention, and a driver-real degraded-WAL seam.
- Added the production claim seam in `storage/outbox.py`: `OutboxRepository.claim_between_select_and_update` is invoked after candidate selection and before the conditional update; it is unset in normal operation.
- RED evidence: copied the new gate file into a detached worktree at parent `9a9dbed`; actual result was `12 failed, 17 passed`.
- GREEN evidence on `62d6b0b` after the rework: `29 passed`.
- Scope explicitly excludes `STATE.md`, `SPEC.md`, `PLAN.md`, FTS5 changes, and unrelated untracked files.
