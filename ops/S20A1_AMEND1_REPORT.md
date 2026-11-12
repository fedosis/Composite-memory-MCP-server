# S20A1 AMEND1 — prior-stage gate amendments

## Scope and authority

Два prior-stage гейта амендированы по `ops/S20_RULINGS_009.md` (§1–3): S19P проверяет pre-provider callback window вместо устаревшего post-provider ожидания; S17 атрибутирует SQL сессии preview request path, а не всем фоновым задачам процесса.

Authority files прочитаны из `/home/shtorm/.hermes/workspace/cmms-http-gateway-plan/` (в implementation worktree их нет):

- `ops/S20_RULINGS_009.md:16–44` — разрешение tests-only amendments.
- `ops/S20_DETAIL.md:182–193`, особенно :191 — callback перед provider recipes.
- `ops/S19P_DETAIL.md:494` — конфликтующее старое post-provider ожидание, superseded указанным S20 ruling.
- `ops/s20a_evidence/r4/evidence/full.json` — исторический прогон exit 2; не выдаётся за текущий GREEN.
- Paired evidence: `/home/shtorm/.hermes/cache/scratch/s20a1_paired/{base,cand}.log`; исходный candidate fail в `cand.log:36–83`. Историческая сводка BASE/candidate приведена в ruling; полный gateway suite в этой задаче не повторялся.

Implementation worktree: `/home/shtorm/.hermes/workspace/gw-s20`, branch `feat/s20-projection-rebuild`.
Exact candidate parent: `96390988562128c69d66a4d2d358570270066cbf`.
Exact BASE: `9b07be1262cc7b1185bd97596a934f998ad592d2`.
История не переписана. Production (`src`, `storage`, `migrations`, `pyproject.toml`) не менялся. Push не выполнялся.

## Changes and production boundaries

1. `tests/gateway/test_s19p_prerequisites.py:2754–2758`: обязательный комментарий ruling и отдельные assertions `outbox_worker/sqlite/vector/graph is None` с маркером `S19P_STARTUP_PREPROVIDER`. Остальной файл дословно сохранён: выполнена программная проверка равенства исходного файла с единственной разрешённой заменой (`S19P_LITERAL_PRESERVATION PASS`). Включая STARTING, custody, admissions/spawn, повторный configure, foreign runtime/profile и single-call assertions.
   Реальная граница: `src/memory_server/runtime/runtime.py:388–401` — configured callback вызывается до цикла recipes; `read_composition.py` вызывает реальный `JobCoordinator.reconcile_startup` под настоящим runtime custody.
2. `tests/gateway/test_admission_dry_run.py:165–227`: wrapper вокруг настоящего `AsyncSession.__init__`, фильтр `traceback.extract_stack()` по production HTTP frames или `inspection_service.py::{execute,_admission,_preview}`. Всегда вызывается исходный SQLAlchemy constructor; fake FS/SQL нет. Исходные тексты, scope matrix и все остальные assertions не менялись. Ephemeral по-прежнему требует ноль атрибутированных сессий.
   Positive control: реальный HTTP POST `/api/v1/profiles/alpha/memories/{F}/preview`, operation=invalidate, с отсутствующим fact. Ответ обязан быть 404/E_NOT_FOUND, а счётчик обязан увеличиться. Это не просто создание AsyncSession из теста: запрос проходит production HTTP handler и `src/memory_server/application/inspection_service.py:202–211,261–263`, где `_preview` открывает настоящий `session_factory()` для SQL snapshot. Admission classification (`inspection_service.py:224–259`) SQL-free при любом теге, поэтому non-ephemeral admission не годится как SQL-positive control; использован mutation preview через тот же transport/inspection boundary.

Test-file SHA256 (одинаковые во всех final candidate и BASE overlay прогонах):

- S19P: `b45c983a703f2a5bbf3d7e41286db805d85ea1d6472f9fc263b1ddbbd0c1b780`
- S17: `8cbc99b6e39c99c540e774483d141fd0d3fae3d71f3a416b737721b813dda916`

## Execution evidence

Долговечная копия: `ops/s20a1_amend1_evidence/`. В каждом каталоге прогона находятся `output.log`, `junit.xml`, `result.json`. Последний содержит полный фактический argv, cwd, явный environment, exit, HEAD, test SHA256, log/JUnit SHA256 и JUnit suite counts. Общий `SHA256SUMS.json` покрывает все архивные evidence файлы. При архивировании хеши log/JUnit и final test overlay повторно проверены.

Runner: `/home/shtorm/.hermes/profiles/coder/cache/scratch/s20a1-amend1/run.py`, архивирован без изменения. Каждый запуск использовал отдельные HOME/HERMES_HOME/XDG_DATA_HOME/TMPDIR/data и pytest basetemp; offline HF/transformers, standalone SQLite, явный PYTHONPATH текущего tree. Внешние реальные providers не подменялись этим amendment.

Общий фактический argv:

    /home/shtorm/.hermes/workspace/gw-harness/venv/bin/python -m pytest -q -p no:cacheprovider <targets> --basetemp=<gate>/tmp/bt --junitxml=<gate>/junit.xml --tb=short

`<gate>` = `/home/shtorm/.hermes/profiles/coder/cache/scratch/s20a1-amend1/<run>`.
Node targets (в этом порядке):

    tests/gateway/test_s19p_prerequisites.py::test_s19p_startup_callback_before_ready
    tests/gateway/test_admission_dry_run.py::test_s17_admission_matches_real_gate

| Run | cwd | targets | exit | actual result |
| --- | --- | --- | --- | --- |
| original-candidate | implementation worktree | оба node targets, исходные тесты | 1 | 2 failed |
| candidate-1 | implementation worktree | оба node targets, промежуточный overlay | 1 | 1 passed, 1 failed |
| candidate-final-1 | implementation worktree | оба node targets, final overlay | 0 | 2 passed |
| candidate-final-2 | implementation worktree | оба node targets, final overlay | 0 | 2 passed |
| candidate-final-3 | implementation worktree | оба node targets, final overlay | 0 | 2 passed |
| base-overlay | BASE worktree | оба node targets, final overlay | 1 | S19P failed, S17 passed |
| base-overlay-trace | BASE worktree | оба node targets, final overlay, `-p startup_trace` | 1 | S19P failed, S17 passed |
| s20a1-targets | implementation worktree | test_online_rebuild.py + test_rebuild_recovery.py | 0 | 206 passed, 0 failed/errors/skipped |
| amended-files | implementation worktree | test_s19p_prerequisites.py + test_admission_dry_run.py | 0 | 134 passed, 0 failed/errors/skipped |

BASE worktree cwd: `/home/shtorm/.hermes/profiles/coder/cache/scratch/s20a1-amend1/base`. Detached exact BASE с overlay только двух изменённых test files. В final candidate прогонах HEAD остаётся exact candidate parent; незакоммиченный final test overlay идентифицирован SHA256 выше. Последующие commits добавляют только этот проверенный overlay и отчёт/evidence, не другую production версию.

### RED / characterization

- Исходный candidate действительно запущен до edits: S17 падает на `S17_GATE_PARITY ephemeral zero SQL sessions` (1 глобальная сессия); S19P падает через `LeaseError: E_RUNTIME_FAILED` на старом несовместимом callback. Это выполненный RED, а не вывод из старого лога.
- Промежуточный `candidate-1` честно сохранён: S19P уже зелёный; S17 positive-control payload не содержал обязательных nullable полей DTO, поэтому получил 422 вместо 404. Payload исправлен согласно существующему production DTO/transport contract, final три прогона выполнены после исправления.
- На exact BASE final S17 instrumentation verdict не меняет: PASS, включая SQL-positive control. S19P final assertion намеренно несовместим со старым post-provider порядком: FAIL. Это characterization, не production regression и не BASE GREEN.
- BASE daemon скрывает исходный AssertionError через E_RUNTIME_FAILED. Поэтому отдельно выполнен read-only `sys.settrace` diagnostic plugin (архив `startup_trace.py`), без изменения тестов/production/исключений. `base-overlay-trace/output.log:33` фиксирует `test_s19p_prerequisites.py:2755: S19P_STARTUP_PREPROVIDER outbox_worker`, где реальный OutboxWorker не None. Обычный и диагностический прогоны имеют одинаковый verdict.

### SHA256 log / JUnit

| Run | output.log | junit.xml |
| --- | --- | --- |
| original-candidate | 170b99ebfbcdded057accf55343277c36c14e7bf6c7b99dd7cc263c506b24c30 | 9385c6e23a8818aa263c998d62de31f8074724976ca0c204f86b8e18521ec227 |
| candidate-final-1 | 9293c688a0567740066ed2723b56e7a761424ed612485b790f69d926c75255e2 | a8e7eb00bf3453133b99e2a3da33cb0ae252bc03c6b78697b8dd93a6206f9048 |
| candidate-final-2 | 19e6d7686c2c4843ba3bd04281d165219baaecace2809e09e62b2d20b3d6bcc7 | c07c1283dc428a228bc79d504eccbc802896e4afbc5d47d3147d49f24e81b048 |
| candidate-final-3 | bce8edefd4b702fdcf3176441bd4710357e7a7d84d163081bf1fdc79f43513af | 86ac1f02dc5c0f8042e0b4dbacb067eb062059ccd235605f679a73813976f8c6 |
| base-overlay | 7ffa703d48574666eca8939fe0695fdbdbf2310b8f5603bae44a8fcf9a4c70b7 | 6d240a758938aaac6778102a0e30ade65d4917afdacd7c02b2eb21c90c970f75 |
| base-overlay-trace | 8456efab2f8d8694b6637981fe10918be0031300af4ee3f27a266bb5995ef5f6 | 256969955bf3e292405d58193ed0fdeacc57ec468fa18c5d23cedfb15b6982a8 |
| s20a1-targets | 570d56f39c5921c5cc74b0118fe959dc0197090ce783b3bc71c78024afb1b856 | 46c4dc057f94f746916f982181b5b54f62a0fdebb292b6c742c3dd9cbfaf4fa1 |
| amended-files | c994e890c8ead5e51a843ef73ebcb089fb896aad93717da2b0df28521ce0e717 | 69ad51ee8e796c537270fa8d71f07de24eda2a189c1e5d53d9f6f448dcb6810b |

Ruff: `/home/shtorm/.hermes/workspace/gw-harness/venv/bin/ruff check` по обоим amended файлам и обоим S20A1 target-файлам, cwd implementation worktree, exit 0, `All checks passed!`; argv/log в `ruff.json`/`ruff.log`. `git diff --check` — exit 0. Git commit hook не нашёл pytest в своём PATH и пропустил свой pytest; это не выдано за тестирование: реальные прогоны выше выполнены явным harness interpreter.

## Commits and final acceptance

- `10d9b9f88d71ff02f5de17bd75e01a4dd2606f80` — `test(s19p): assert pre-provider startup callback window`.
- Следующий отдельный commit — `test(s17): attribute preview SQL sessions to the request path`; включает S17 amendment и этот отчёт с evidence.
- Финальные проверки после commits: clean `git status --porcelain -uall`, отсутствие production diff относительно exact candidate parent и сверка archived SHA256 обязательны перед выдачей результата. Evidence logs относятся к идентичному test/production содержимому, а не к повторному запуску после documentation-only additions.

## NOT RUN / residual

- Полный `tests/` и полный gateway paired suite повторно NOT RUN; исторические full.* и paired logs не заменяют текущую проверку полного набора.
- Security/crash-recovery review, resource acceptance, R6 residual и полный DETAIL §7 за пределами amendment; этот GREEN не означает merge/READY для S20A1.
- Positive control доказывает атрибуцию SQL на реальном HTTP mutation-preview boundary. Он не доказывает иной, несуществующий SQL path для non-ephemeral admission.
- Матрица BASE намеренно RED для S19P: approved S20 порядок отсутствует в BASE.
- Сохраняются существующие pytest-asyncio/LanceDB deprecation warnings.
- `S20A1_READY.md` в workspace не найден; не создан и чужой plan-worktree не изменялся. Явная строка об amendment двух prior-stage gates приведена в начале этого owner report. Merge/READY не объявляется.

STATUS: S20A1 AMEND1 GREEN
