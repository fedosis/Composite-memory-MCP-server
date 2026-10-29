# S08a — implementation and verification

## Scope / changes (paths relative to repository)
- `src/memory_server/transports/http/routes/retrieval.py:19,41,71`: separate parsers/results, canonical operation map, preflight; compatibility alias retained.
- `src/memory_server/transports/http/app.py:53`: S08 search/context whitelist; parser AND result callable identity; unchanged S07 catalogue restrictions.
- `src/memory_server/runtime/read_composition.py:21`: S07 reader only for memory routes; one gateway port for search/context.
- `src/memory_server/application/gateway_retrieval.py:123,142`: permission/profile/operation checks, one lease, stage after lease; explicit SQLite BEGIN for search + hydration.
- `src/memory_server/application/gateway_retrieval.py:45,185`: fresh field-wise package mask, native get_context, bounded typed hydration and membership/metadata race rejection.
- `src/memory_server/providers/sqlite_provider.py:481,553`: session-aware filtered FTS/LIKE; typed bulk <=200, <=2 data SELECT + one receipts batch; empty batch issues no SQL.
- `tests/gateway/test_http_retrieval.py:76,177,248,286,341,396,435,461`: TCP regressions, filter matrix/parity, preflight, audit/lease/metrics custody, SQL/snapshot and injected DB/audit failures.
- `tests/gateway/test_context_equivalence.py:152,258,285,333`: independent direct API/service/HTTP oracle, field canaries, races, typed batches, 100+100 budget, empty context and revealer masking.
- `tests/gateway/test_slice_runtime.py:333,640,730`: only stale metadata/audit/composition expectations and mount negatives.
- Provider S07/01A read/page/hydration section and 01B snapshot test are byte-identical to dcc422a; 3/2 statement assertions unchanged.
- No new retrieval sources, schema/index/router/native API/contract changes; no push/rebase/amend.

## Filter matrix (real TCP, alpha + beta, real SQLite)
- `source != null` (including empty string), nonempty tags (including [empty string]): 422 E_VALIDATION, zero audit/lease; reader + revealer.
- `source=null`, absent/empty lists: no restriction; confidence 0.0 takes the new filtered path, not truthiness bypass.
- belief-only: 200/0; fact+belief = fact; keyword remains facts-only.
- Active/candidate/validated, archived/rejected, unknown; literal state OR, independent active AND, confidence >=0/0.9/1; combined filters tested.
- include_inactive=true removes only the default activity predicate; false+archived=empty; true+archived=archived.
- FTS and no-FTS LIKE: unfiltered top-1 is ineligible; filtered LIMIT=1 returns the later eligible row.
- Neutral-filter parity vs FactRepository.search: IDs/order and limit+1 has_more-equivalent for quotes, *, AND, %, _, backslash, punctuation and no facts_fts.
- Four expected FTS OperationalError markers fall back with identical filters; locked/I/O errors produce E_STORAGE.
- keyword/hybrid share results; hybrid degraded is exactly semantic,graph. semantic/graph/trace=true: 503 before audit/lease.

## Context mask / consistency
- Fact: subject,predicate,object,dedup_key,source,creator; decision: context,choice,reason,source,creator and every rejected alternative.
- Nonnull dedup_key canary is verified on actual no-FTS LIKE output, not null-to-null; independent oracle never calls production mask.
- Literal safe-version regex copied from memory_read_service.py:239; 0.1.0/1/007/1.2.3 preserved; v1/1.2.3-rc1/empty masked; integer fact versions remain integers.
- Package keys/row key order/list order/total preserved; raw input unchanged. Token fields null/null/true.
- Exact typed K: facts then decisions, same ID across types/profiles; each root has distinguishable visible metadata.
- DELETE, inactive/confidence/timestamp UPDATE between native retrieval and hydration: whole response E_STORAGE, no partial package.
- JSON-mode metadata comparison avoids datetime/string mismatches; context does NOT claim a shared snapshot with native get_context reads.

## Executed evidence
- Interpreter exclusively `/home/shtorm/.hermes/workspace/.venvs/cmms-verify/bin/python` (3.11.15); absolute PYTHONPATH; disposable roots.
- Baseline dcc422a: 2 FAILED as required, assertions 422==200 and 404==200. Final identical regression file: 2 PASSED.
- Baseline archive: 138 production blobs verified against git object IDs; commands/environment/test hash in regressions-and-manifest.json.
- Full `tests/gateway/`: 803 PASSED, 642 warnings, 1061.17s; exit 0 (full-final.log).
- Latest strengthened test bodies rerun across gateway excluding only the unchanged two costly keyset cases: 801 PASSED, 2 deselected; exit 0 (final-nonheavy2.log).
- After source mutations and restoration: HTTP/context/slice 67 PASSED; exit 0 (post-mutations.log).
- Native get_context/fact repository/application retrieval: 21 PASSED, 1 SKIPPED; exit 0 (native-regressions.log).
- Ruff all 8 touched Python files: PASS; git diff --check: PASS.
- Earlier bounded full/nonheavy attempts timed out; these are superseded by completed runs, not classified as test failures. Slow keyset[1]: 855.71s.

## Source mutation controls
- Eight mutations killed; restored byte-for-byte in finally, hashes recorded; positives were green before and after.
- ignore-filter / filter-after-limit / return-empty: 2 failed each (FTS + LIKE).
- remove-nested-mask / drop-decision-hydration / reverse-context-order: 2 failed each (FTS + LIKE).
- swap-parser / disable-parser-identity: 1 failed each; runtime response/mount assertions, not collection errors.
- Artifacts: `/home/shtorm/.hermes/workspace/cmms-http-gateway-plan/ops/s08a_evidence/`; none committed under branch ops except this report.

## Pre-existing failures / hook
- `tests/test_release_candidate.py:109`: README expectation fails on BOTH exact baseline and current tree: 1 FAILED each (hook-baseline.log / hook-current.log).
- Authorized hook bypass reason: this unrelated baseline README assertion; explicit pinned-interpreter gateway/native/ruff checks ran separately.

## QUESTION / STILL OPEN
- QUESTION: owner-authorized C4-vs-T2 source/tags contract reconciliation remains separate; CONTRACTS.md was not modified.
- STILL OPEN outside S08a: S08b semantic/graph/resolver and S11 trace; independent external review not performed in this run.
