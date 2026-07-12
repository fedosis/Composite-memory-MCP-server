# Architectural Review: Card 001 — Belief Model

**Reviewer:** Hermes Agent
**Date:** 2026-07-12
**Project:** Composite Memory MCP Server v0.6
**Spec:** specs/card-001-belief-model.md
**Verdict:** **Changes Requested**

---

## Executive Summary

Spec Card 001 adds a Belief Store — a conceptually valuable addition that fills a real gap in the existing memory model (facts record *what happened*, beliefs model *what the agent holds to be true*). The spec is well-structured with clear motivation, data model, MCP tool signatures, and acceptance criteria.

**However, the spec has 5 blocking issues** that must be resolved before implementation can begin: a fundamental state machine conflict with the lifecycle engine, pattern drift on Pydantic models, missing contract schemas, no outbox integration, and undefined conflict detection. Additionally, there are several major and minor issues that will cause significant rework if not addressed upfront.

---

## 1. Соответствие существующей архитектуре (v0.6)

### ✅ Что сделано правильно

- **SQLAlchemy storage**: Correctly targets the existing canonical store (v0.6 Phase 2/3) with Alembic migration
- **Outbox pattern awareness**: Mentions atomic writes (though doesn't fully specify them)
- **FastMCP SDK**: Tools are defined as MCP tools, consistent with server.py registration pattern
- **Repository pattern**: Evidence as derived store is a sound architectural decision

### ❌ Проблемы

#### 🔴 CRITICAL: BeliefStatus vs LifecycleState — конфликт двух state-машин

Это **самая серьёзная архитектурная проблема** в spec.

Spec определяет:

```
BeliefStatus: ACTIVE | SUPERSEDED | CONTRADICTED | DISCARDED
```

А существующий lifecycle engine (v0.6 Phase 5) использует:

```
LifecycleState: candidate → validated → active → stale → archived → forgotten
```

Spec говорит: "*Lifecycle engine: beliefs follow active → stale → archived lifecycle (Phase 5 model)*" — но затем определяет ортогональную систему статусов.

**Проблемы:**
- `SUPERSEDED`, `CONTRADICTED`, `DISCARDED` не существуют в lifecycle engine
- `STALE`, `ARCHIVED`, `FORGOTTEN` не существуют в BeliefStatus
- Что происходит с belief в статусе CONTRADICTED — он может перейти в stale через decay?
- Как lifecycle engine обрабатывает belief в статусе DISCARDED?
- Две параллельные state-машины для одной сущности = гарантированные data integrity issues

**Решение:** Выбрать одну модель. Рекомендуется:
- Сократить `BeliefStatus` до `ACTIVE` (все существующие lifecycle states применимы)
- Заменить `SUPERSEDED`/`CONTRADICTED`/`DISCARDED` на lifecycle events (LifecycleEventORM)
- `resolve_conflict` меняет lifecycle_state → "discarded" через LifecycleRepository.record_event()

Или наоборот — если Belief использует свой статус, уберите ссылку на lifecycle engine из spec.

#### 🔴 CRITICAL: `@dataclass` вместо Pydantic — нарушение конвенции моделей

Spec использует `@dataclass` для Belief и Evidence. Весь существующий код использует **Pydantic v2**:

| Файл | Модель | Тип |
|------|--------|-----|
| models/fact.py | `Fact` | `pydantic.BaseModel` |
| models/decision.py | `Decision` | `pydantic.BaseModel` |
| models/skill.py | `Skill` | `pydantic.BaseModel` |
| models/receipt.py | `MemoryReceipt` | `pydantic.BaseModel` |
| **spec** | **Belief, Evidence** | **`@dataclass`** |

**Последствия:**
- Нет `model_dump(mode="json")` для сериализации (используется во всех существующих tool-ах)
- Нет `ConfigDict(from_attributes=True)` для ORM mapping
- Нет Pydantic validation (confidence range, proposition non-empty)
- Нельзя встраивать в существующие return-форматы

**Решение:** Использовать `pydantic.BaseModel` с теми же конвенциями, что Fact/Decision/Skill (model_config, Field с ge/le, default_factory для datetime).

#### 🔴 CRITICAL: Нет JSON Schema контрактов

Каждый существующий tool имеет `.schema.json` в `contracts/`:

```
contracts/remember.schema.json
contracts/search.schema.json
contracts/learn.schema.json
...
```

Для `set_belief`, `get_belief`, `resolve_conflict` нет ни одного schema-файла. Все три должны быть добавлены в формате contracts/*.schema.json с:
- `"version": "0.7.0"`
- `"input"` / `"output"` секции
- Ссылки на `common.schema.json#/definitions/`
- `"errors"` секция с error_envelope

#### 🔴 CRITICAL: Нет интеграции с outbox

Outbox worker (outbox_worker.py) обрабатывает только:
```
"index_fact", "index_decision", "index_skill"
```

Belief operations не добавлены:
- OutboxEntryORM.record_type: нет `"belief"` в enum-like usage
- OutboxWorker: нет `_process_index_belief()`
- SQLiteProvider.create_in_transaction(): не поддерживает belief + outbox

Belief изменения не будут индексироваться в Qdrant/graph. Если beliefs не нужно индексировать — это должно быть явно указано в spec.

#### 🟡 MAJOR: Не определён механизм обнаружения конфликтов

Acceptance Criteria #6: "*Contradiction auto-detection on belief creation (two active beliefs with opposing propositions)*"

Spec не определяет:
1. Что считается "opposing proposition" — exact string match? Semantic similarity? LLM-based?
2. Какой порог? 
3. Когда происходит проверка — на create, на reinforce, на запросе?
4. Как авто-детекция интегрируется с `resolve_conflict` tool?

Это нетривиальная NLP/AI задача. Для v0.7 разумно начать с exact string match и ручного resolve_conflict.

---

## 2. Полнота data model — достаточно ли полей Belief и Evidence?

### ✅ Что есть

- `proposition` — core content
- `confidence` (0.0-1.0) — consistent with existing system
- `source_ids` — provenance links
- `version` — revision tracking
- `last_reinforced_at` — useful for decay
- Evidence `source_type` / `source_id` — fine-grained attribution
- Evidence `weight` — evidential strength

### ❌ Чего не хватает

#### 🔴 CRITICAL: Нет полей ADR-008 compliance

ADR-008 требует для каждого memory объекта:
- `source` ✅ (в Evidence есть, в Belief — нет)
- `creator` ❌ **Отсутствует в Belief**
- `timestamp` ✅ (created_at/updated_at есть)
- `confidence` ✅
- `verification_status` ❌ **Отсутствует в Belief**
- `history` ❌ **Отсутствует в Belief**

Сравнение:

```
Fact:      id, subject, predicate, object, confidence, source, creator, created_at, updated_at, verification_status, lifecycle_state, version
Belief:    id, proposition, confidence, source_ids, created_at, updated_at, last_reinforced_at, version, status
           ↑ missing source, creator, verification_status, lifecycle_state
```

**Решение:** Добавить `source`, `creator`, `verification_status`, `lifecycle_state` в Belief.

#### 🟡 MAJOR: Evidence.weight — не определена семантика агрегации

Spec: "weight 0.0-1.0: how strongly this evidence supports the belief"

Не указано:
- Как weight влияет на belief.confidence? Среднее? Взвешенное по source reliability?
- Используется ли существующий ConfidenceEngine?
- Кто устанавливает weight — пользователь при set_belief? LLM?

**Решение:** Специфицировать формулу или сослаться на ConfidenceEngine.

#### 🟡 MAJOR: Нет категоризации/тегов для Belief

Факты структурированы SPO (subject-predicate-object), решения имеют context/choice/reason, навыки имеют purpose/steps. Belief имеет только свободный текст `proposition`. Это затруднит поиск по subject/теме.

**Решение:** Добавить опциональные `tags: list[str]` или `subject: str` для группировки.

#### 🟢 MINOR: `last_reinforced_at` отсутствует в DB schema

Storage секция spec показывает таблицу:
```
beliefs (id, proposition, confidence, status, version, created_at, updated_at, last_reinforced_at)
```

Но `last_reinforced_at` — верно, не критично, просто стоит проверить консистентность.

---

## 3. Консистентность MCP API

### ✅ Согласовано

- Все три tool возвращают JSON — consistent with existing tools
- `set_belief` имеет аналог `remember` (создание + опциональное обновление)
- `get_belief` имеет аналог `search` (фильтрация + лимит)
- Используют FastMCP SDK

### ❌ Проблемы

#### 🔴 CRITICAL: Нет параметра `source` на `set_belief`

`remember` имеет `source: str = "user"`. `set_belief` не имеет. Как отследить, кто создал belief?

#### 🟡 MAJOR: Return format не соответствует конвенциям

Существующие tool-ы возвращают полные объекты:

```
remember → {"receipt": {full MemoryReceipt}, "fact": {full Fact with timestamps}}
search   → {"results": [...full facts...], "total": N}
```

`set_belief` return (spec):
```json
{
  "belief": {
    "id": "uuid",
    "proposition": "...",
    "confidence": 0.85,
    "version": 1,
    "status": "active"
  }
}
```

**Проблемы:**
- Нет `created_at`, `updated_at`, `source`, `creator`, `source_ids` — половина полей модели
- Должен возвращать полный объект как все остальные tool-ы
- Нет receipts (ADR-008 требует receipt на каждую memory операцию)

#### 🟡 MAJOR: `replace` параметр — неоднозначная семантика

```
Behaviour:
- Если replace указан И proposition совпадает → supersede
- Если replace не указан И proposition существует → reinforce
- Если новый → create
```

Проблема: `replace` содержит proposition текста, а не ID. Что если proposition похожи но не идентичны? "User prefers Docker" vs "User prefers Docker Compose" — это replace или новый belief?

**Решение:** Использовать `replace_belief_id: str` (UUID) вместо `replace: string` (proposition) — однозначная идентификация.

#### 🟡 MAJOR: `get_belief.proposition` — "Fuzzy match" не определён

Существующий search использует FTS5. Что такое "fuzzy match" для beliefs? LIKE? FTS5? Levenshtein?

**Решение:** Специфицировать FTS5 match или LIKE.

#### 🟢 MINOR: Нет error responses

Все существующие schema имеют `"errors"` секцию. Для новых tool-ов нет ни error codes, ни error messages.

---

## 4. Интеграция с существующими компонентами

### ✅ Позитивно

- Spec упоминает lifecycle engine integration
- Spec упоминает remember/learn bridge
- Evidence как derived store — умный дизайн, использует существующие fact/decision references

### ❌ Проблемы

#### 🔴 CRITICAL: Lifecycle engine не знает о "belief"

- `PER_TYPE_TTL` (decay.py) — нет "belief" типа
- `LifecycleRepository` — не обрабатывает memory_type="belief"
- `Storage/models/__init__.py` — нет BeliefORM
- Validator не регистрирует belief
- LifecycleState enum не включает SUPERSEDED/CONTRADICTED/DISCARDED

#### 🔴 CRITICAL: remember() → conflict record — неопределён

Spec: "*if a remembered fact contradicts an active belief, auto-create a ConflictRecord*"

- Что такое ConflictRecord? Не существует.
- Где хранится? Нет таблицы.
- Кто его читает? resolve_conflict? Как?
- Как определяется противоречие между fact (SPO triple) и belief (proposition)?

#### 🔴 CRITICAL: learn() → belief bridge — неопределён

Spec: "*extracted knowledge can optionally become a belief if confidence > 0.7*"

- Модификация существующих extractor-ов (FactExtractor, DecisionExtractor, SkillExtractor)?
- Отдельный BeliefExtractor?
- Какие extracted fields → proposition?
- Кто решает "optionally" — LLM? Threshold?

#### 🟡 MAJOR: SQLiteProvider не имеет CRUD для Belief

- Нет `create_belief()`, `get_belief()`, `search_beliefs()`, `update_belief()`, `delete_belief()`
- Нет `_get_belief_repo()`, `_get_evidence_repo()` 
- Нет `create_in_transaction()` for belief + evidence + outbox

---

## 5. Риски и проблемы — что может пойти не так?

### Высокий риск

| Риск | Описание | Вероятность | Влияние |
|------|----------|-------------|---------|
| **State machine шаблон** | BeliefStatus vs LifecycleState — две параллельных state-машины | Высокая | Критическое — data integrity |
| **Undefined conflict detection** | "Opposing propositions" без алгоритма | Высокая | resolve_conflict tool бесполезен без auto-detection |
| **Pattern drift** | @dataclass вместо Pydantic, нет source/creator | Высокая | Медиум — рефакторинг после реализации |
| **Commitment to MCP without schema** | Три tool без JSON Schema контрактов | Высокая | Разойдётся с конвенцией v0.6 |

### Средний риск

| Риск | Описание |
|------|----------|
| **Outbox blindness** | Belief изменения не индексируются, что может быть неочевидно |
| **Confidence duplication** | Belief.confidence вычисляется иначе, чем ConfidenceEngine.score_fact() |
| **Reinforcement weight ambiguity** | "Weighted average" без формулы → произвольная имплементация |
| **Evidence как derived store** | Хорошая идея, но rebuild process не определён — risk of data loss при rebuild |
| **No belief lifecycle decay** | Beliefs не проходят stale→archived→forgotten, засоряют БД |

---

## 6. Порядок implementation

### Spec-предлагаемый порядок: model → tools → integration → tests

**Проблема:** Tools не могут работать без интеграции. `set_belief` требует storage layer, который требует outbox для async indexing. lifecycle engine integration требуется до resolve_conflict. learn() bridge требуется до learn-to-believe.

### Рекомендуемый порядок

```
Step 1: Pydantic модели (Belief, Evidence, BeliefORM, EvidenceORM)
  - Файлы: models/belief.py, storage/models/belief.py

Step 2: Alembic migration (beliefs + evidence tables)

Step 3: Repository layer (BeliefRepository, EvidenceRepository)
  - Файлы: storage/repositories/belief_repo.py, evidence_repo.py

Step 4: SQLiteProvider CRUD
  - create_belief, get_belief, search_beliefs, update_belief
  - create_evidence, get_evidence, search_evidence
  - create_in_transaction() with belief + evidence + outbox

Step 5: Outbox integration
  - OutboxEntryORM: record_type="belief"
  - OutboxWorker: _process_index_belief()

Step 6: Lifecycle integration
  - PER_TYPE_TTL["belief"] = 180.0
  - Storage/models/__init__.py: register BeliefORM, EvidenceORM

Step 7: MCP tools
  - server.py: @mcp.tool(name="set_belief"), get_belief, resolve_conflict
  - contracts/set_belief.schema.json, etc.

Step 8: remember()/learn() bridge
  - Conflict detection (starts with exact match)
  - Optional belief creation in learn()

Step 9: Tests
  - Unit: models, repo, provider
  - Integration: MCP tools
  - Edge: conflict detection, reinforcement, version supersession
```

---

## Конкретные правки (Changes Requested)

### Блокирующие (необходимо исправить до реализации)

| # | Что | Как исправить |
|---|-----|---------------|
| **CR-01** | BeliefStatus vs LifecycleState conflict | Сократить BeliefStatus до ACTIVE. Использовать lifecycle_state для stale/archived/forgotten. SUPERSEDED/CONTRADICTED/DISCARDED — через LifecycleEventORM с новым type="belieft_state_change" или расширить lifecycle_state до ["active", "superseded", "contradicted", "discarded", "stale", "archived", "forgotten"] |
| **CR-02** | @dataclass → Pydantic | Переписать Belief/Evidence как `pydantic.BaseModel` с `model_config=ConfigDict(from_attributes=True)`. Добавить `Field(ge=0.0, le=1.0)` для confidence/weight |
| **CR-03** | Отсутствуют JSON Schema контракты | Добавить `contracts/set_belief.schema.json`, `get_belief.schema.json`, `resolve_conflict.schema.json` в формате v0.7.0 |
| **CR-04** | Нет outbox integration | Добавить: OutboxEntryORM record_type="belief", OutboxWorker._process_index_belief(), и описание в spec |
| **CR-05** | Undefined conflict detection | Специфицировать: точное совпадение proposition (string match) для v0.7. Убрать "auto-contradiction detection on belief creation" из Acceptance Criteria или определить exact match |

### Основные (должны быть исправлены до merge)

| # | Что | Как исправить |
|---|-----|---------------|
| **CR-06** | Нет source/creator/verification_status | Добавить `source: str`, `creator: str`, `verification_status: str = "candidate"`, `lifecycle_state: str = "active"` в Belief |
| **CR-07** | Set_belief нет `source` параметра | Добавить `source: str = "system"` в input |
| **CR-08** | Return format урезан | Возвращать полный Belief (все поля, включая timestamps). Добавить receipt аналогично remember |
| **CR-09** | `replace` → `replace_belief_id` | Изменить на UUID-based replacement для однозначности |
| **CR-10** | Нет SQLiteProvider CRUD | Добавить create_belief, get_belief, search_beliefs, update_belief в spec |
| **CR-11** | Нет belief в decay engine | Добавить "belief" в PER_TYPE_TTL |

### Косметические / на усмотрение

| # | Что | Зачем |
|---|-----|-------|
| **CR-12** | Weight aggregation formula | Указать: weighted average по Evidence.weight (нормализованный к sum) |
| **CR-13** | ConflictRecord definition | Если нужен — определить модель и таблицу |
| **CR-14** | FTS5 for belief proposition search | Использовать facts_fts-подобный FTS5 индекс для beliefs |
| **CR-15** | Add contributor to Evidence | ADR-008 compliance |
| **CR-16** | Add tags/category to Belief | Облегчает поиск/группировку |

---

## Summary

| Criterion | Rating | Key Issue |
|-----------|--------|-----------|
| 1. Соответствие архитектуре v0.6 | ❌ **Blocking** | BeliefStatus vs LifecycleState conflict; @dataclass vs Pydantic |
| 2. Полнота data model | 🟡 **Major gaps** | Missing source/creator/verification_status; undefined weight aggregation |
| 3. Консистентность MCP API | 🟡 **Major gaps** | No JSON Schemas; missing `source` param; truncated return format |
| 4. Интеграция с компонентами | ❌ **Blocking** | No outbox; no lifecycle; undefined ConflictRecord; undefined learn bridge |
| 5. Риски | ⚠️ **High** | Two state machines; undefined conflict detection; pattern drift |
| 6. Порядок implementation | 🟡 **Needs reorder** | Tools before integration won't work |

## Вердикт: **Changes Requested**

**5 blocking issues (CR-01 to CR-05)** must be resolved before implementation begins. **6 major issues (CR-06 to CR-11)** should be resolved before merge. The core concept is valuable and well-motivated, but the spec needs:
1. State machine reconciliation with the lifecycle engine
2. Pydantic model convention compliance
3. JSON Schema contracts
4. Outbox + lifecycle integration plan
5. Concrete conflict detection mechanism (even if simple)

After applying these changes, re-review is recommended. The recommended implementation order (Repository → Provider → Outbox → Lifecycle → MCP tools → Bridge → Tests) will avoid the chicken-and-egg problem of implementing tools that depend on infrastructure not yet built.
