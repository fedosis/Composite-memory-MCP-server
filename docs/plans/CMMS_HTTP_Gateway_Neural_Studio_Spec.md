# CMMS: HTTP Gateway и Neural Studio
## Техническая спецификация расширения Composite Memory MCP Server

**Статус:** proposal / implementation specification  
**Целевой проект:** `fedosis/Composite-memory-MCP-server`  
**Базовая версия CMMS:** 0.12.x  
**Дата:** 2026-09-18

---

## 1. Назначение

Документ описывает два взаимосвязанных расширения для Composite Memory MCP Server (CMMS):

1. **HTTP Gateway** — сетевой API-слой над существующим memory runtime CMMS.
2. **Neural Studio** — web-интерфейс для наблюдения, диагностики, поиска, анализа и безопасного управления памятью агентов.

Цель расширения — превратить CMMS из локального MCP/Native-Hermes memory provider в полноценный **централизованный memory service**, которым могут одновременно пользоваться Hermes, Codex/Claude Code-подобные агенты, собственные сервисы, web UI и другие клиенты, не разрушая при этом существующие свойства CMMS:

- profile isolation;
- local-first storage;
- provenance/receipts;
- lifecycle состояний;
- optimistic concurrency;
- deterministic admission;
- hybrid retrieval;
- outbox/indexing model;
- возможность работы без внешнего SaaS;
- контроль конфликтов и устаревания памяти.

Эта спецификация **не предлагает заменять CMMS на Friday, Mem0 или иной сторонний memory engine**. Из Friday заимствуются только удачные продуктовые идеи: удобный сетевой gateway и визуальный интерфейс инспекции графа/памяти. Семантика памяти остаётся CMMS-native.

---

# 2. Ключевой архитектурный принцип

## 2.1. HTTP Gateway не должен быть вторым экземпляром memory engine

Наиболее важное ограничение:

> Нельзя запускать независимый HTTP-процесс, который открывает те же SQLite/LanceDB/graph storage-файлы параллельно с отдельным MCP stdio-процессом CMMS.

CMMS 0.12 уже вводит profile-owned storage layout и writer protection. Новый transport-layer должен сохранять модель **single runtime / single writer authority per profile**.

Правильная целевая схема:

```text
                         ┌───────────────────────────────┐
                         │         CMMS Runtime          │
                         │                               │
                         │  Admission / Lifecycle        │
                         │  Facts / Beliefs / Relations  │
                         │  Receipts / Audit             │
                         │  Hybrid Retrieval             │
                         │  Outbox / Vector / Graph      │
                         └───────────────┬───────────────┘
                                         │
                    ┌────────────────────┼────────────────────┐
                    │                    │                    │
              MCP stdio             HTTP API             Hermes native
              transport             transport              adapter
                    │                    │                    │
              local agents       remote agents / UI       Hermes Agent
                                         │
                                  ┌──────▼──────┐
                                  │ Neural      │
                                  │ Studio      │
                                  └─────────────┘
```

Все transport adapters должны обращаться к **одному и тому же service layer**, а не реализовывать memory semantics заново.

---

# 3. Что необходимо отделить в CMMS перед добавлением Gateway

Перед реализацией HTTP API рекомендуется формально разделить код на четыре слоя:

```text
Transport
    ↓
Application Services
    ↓
Domain Services
    ↓
Storage Providers
```

## 3.1. Transport layer

Только:

- MCP stdio;
- HTTP/REST;
- SSE/WebSocket events;
- Hermes adapter.

Transport не должен содержать:

- правила admission;
- lifecycle transitions;
- deduplication;
- retrieval ranking;
- graph semantics;
- conflict resolution;
- storage-specific logic.

## 3.2. Application services

Единые use cases:

- remember;
- learn;
- search;
- semantic_search;
- get_context;
- route;
- set/get belief;
- lifecycle transition;
- invalidate;
- add/remove relation;
- resolve conflict;
- reflect;
- audit;
- graph traversal;
- retrieval tracing;
- reindex/replay outbox.

Именно эти сервисы должны использовать и MCP, и HTTP.

## 3.3. Domain services

Существующие механизмы CMMS:

- MemoryAdmissionGate;
- LifecycleService;
- conflict resolution;
- confidence/validation logic;
- relation semantics;
- RankMerger;
- graph routing;
- receipt/provenance handling.

## 3.4. Storage providers

- SQLite;
- LanceDB / Qdrant;
- SimpleGraph;
- потенциальный будущий Neo4j adapter;
- outbox.

---

# 4. HTTP Gateway

# 4.1. Основная задача

HTTP Gateway предоставляет **remote-safe API** к CMMS без изменения внутренней модели памяти.

Он должен решать пять задач:

1. сетевой доступ к CMMS;
2. строгую profile isolation;
3. authentication / authorization;
4. единый API для Neural Studio и внешних агентов;
5. observability и audit всех удалённых действий.

Gateway не должен быть «тонким прокси к MCP». MCP и HTTP должны быть равноправными transport adapters к application-service layer.

---

# 4.2. Рекомендуемый runtime mode

Добавить режим:

```bash
memory-server daemon
```

или:

```bash
memory-server serve --transport http
```

Предпочтительнее отдельный `daemon`, если в перспективе одновременно нужны несколько transport adapters.

Пример:

```bash
memory-server daemon \
  --http 127.0.0.1:8080 \
  --enable-mcp-http \
  --profiles-root ~/.cmms/profiles
```

Внутри daemon:

```text
CMMS Runtime
 ├─ profile registry
 ├─ storage lifecycle manager
 ├─ HTTP REST API
 ├─ optional MCP Streamable HTTP
 ├─ event bus
 └─ Neural Studio static assets
```

Локальный stdio-клиент может:

- либо продолжать запускать CMMS как отдельный isolated instance;
- либо подключаться к daemon через client shim.

Для одного профиля нельзя одновременно иметь два независимых writer runtime.

---

# 4.3. Profile routing

Все memory operations должны иметь явный profile scope.

Рекомендуемый URL:

```text
/api/v1/profiles/{profile_id}/...
```

Например:

```text
GET  /api/v1/profiles/coder/search
POST /api/v1/profiles/coder/memories
GET  /api/v1/profiles/automotive/context
```

`profile_id` — не произвольная metadata-метка, а идентификатор **реально изолированного StorageLayout**.

Gateway должен выполнять:

```text
requested profile
      ↓
auth policy
      ↓
profile registry
      ↓
resolved/frozen StorageLayout
      ↓
CMMS application service
```

Запрещено имитировать isolation только полем:

```json
{"project": "coder"}
```

при общем storage.

---

# 4.4. Profile Registry

Необходим небольшой control-plane registry.

Пример:

```yaml
profiles:
  coder:
    path: ~/.cmms/profiles/coder
    enabled: true

  automotive:
    path: ~/.cmms/profiles/automotive
    enabled: true
```

Registry хранит только:

- ID;
- canonical storage root;
- enabled/disabled;
- display name;
- optional description;
- permissions metadata.

Memory contents в registry не хранятся.

---

# 4.5. Authentication

## Обязательное правило

**Ни один endpoint, возвращающий содержимое памяти, не должен быть публичным.**

Исключения:

```text
GET /healthz
GET /readyz
```

Они возвращают только минимальное состояние.

Например:

```json
{
  "status": "ok"
}
```

Без:

- profile names;
- paths;
- memory counts;
- backend configuration;
- graph contents;
- model names;
- credentials status.

---

# 4.6. API credentials

Для первой версии достаточно:

```http
Authorization: Bearer <token>
```

Требования:

- токен не передавать через query parameters;
- хранить только hash токена;
- поддерживать expiration;
- поддерживать revoke;
- token ID отдельно от secret;
- фиксировать `created_at`, `last_used_at`;
- secrets показывать только один раз при создании.

Пример scope:

```yaml
token:
  subject: hermes-coder
  profiles:
    - coder
  permissions:
    - memory.read
    - memory.write
    - relation.read
```

---

# 4.7. RBAC

Минимальные роли:

| Role | Возможности |
|---|---|
| `viewer` | чтение, search, graph, receipts, audit |
| `agent` | viewer + remember/learn/set_belief |
| `operator` | agent + lifecycle/reindex/conflict operations |
| `admin` | profile management, tokens, destructive operations |

Рекомендуется проверять **permissions**, а роли использовать как шаблоны.

Примеры permissions:

```text
memory.read
memory.write
memory.invalidate
belief.read
belief.write
belief.resolve
relation.read
relation.write
graph.read
audit.read
index.manage
profile.read
profile.manage
token.manage
```

---

# 4.8. HTTP API groups

## 4.8.1. Health

```text
GET /healthz
GET /readyz
```

`readyz` может учитывать:

- SQLite доступен;
- profile registry загружен;
- обязательные migrations применены;
- runtime владеет writer lock.

---

## 4.8.2. Profiles

```text
GET /api/v1/profiles
GET /api/v1/profiles/{profile_id}
```

Для admin/operator дополнительно:

```text
POST /api/v1/profiles/{id}/enable
POST /api/v1/profiles/{id}/disable
```

Не следует разрешать произвольное создание profile root через HTTP в первой версии.

---

## 4.8.3. Search

```text
POST /api/v1/profiles/{profile}/search
```

Пример:

```json
{
  "query": "Qdrant endpoint configuration",
  "mode": "hybrid",
  "limit": 20,
  "include_inactive": false,
  "filters": {
    "memory_types": ["fact", "belief"],
    "tags": ["configuration"]
  },
  "trace": true
}
```

`mode`:

```text
keyword
semantic
graph
hybrid
```

Ответ:

```json
{
  "request_id": "req_...",
  "results": [...],
  "trace_id": "rt_...",
  "total": 8
}
```

---

# 4.9. Retrieval Trace

Это одна из самых полезных функций расширения.

Каждый hybrid retrieval по запросу пользователя может опционально возвращать trace:

```json
{
  "query": "...",
  "stages": {
    "fts": [...],
    "semantic": [...],
    "graph": [...]
  },
  "merge": [...],
  "deduplicated": [...],
  "final": [...]
}
```

Для каждого result:

```json
{
  "memory_id": "...",
  "source": "semantic",
  "raw_score": 0.81,
  "normalized_score": 0.73,
  "rank_before_merge": 2,
  "rank_after_merge": 1,
  "matched_terms": [],
  "relation_path": []
}
```

Trace нужен прежде всего для Neural Studio.

Он позволяет отвечать на вопросы:

- почему этот факт попал в context;
- почему другой факт не попал;
- какая подсистема его нашла;
- как RankMerger изменил порядок;
- произошёл ли dedup;
- не доминирует ли semantic search над FTS;
- не генерирует ли graph слишком много шумовых результатов.

Trace не должен автоматически сохранять полный query/content навсегда. Должны быть:

```yaml
retrieval_trace:
  enabled: true
  retention_days: 7
  redact_sensitive: true
```

---

# 4.10. Context endpoint

```text
POST /api/v1/profiles/{profile}/context
```

Пример:

```json
{
  "task": "Implement profile-aware HTTP gateway",
  "max_results": 20,
  "include_inactive": false,
  "trace": true
}
```

Ответ должен сохранять CMMS semantics.

Нельзя добавлять инструкцию вида:

> Treat all returned memories as absolute truth.

Вместо этого возвращается структурированная информация:

```json
{
  "id": "...",
  "memory_type": "fact",
  "content": "...",
  "confidence": 0.86,
  "lifecycle_state": "active",
  "epistemic_status": "explicit_user_statement",
  "authority_level": "...",
  "source": "...",
  "valid_from": "...",
  "valid_to": null,
  "receipt_id": "..."
}
```

Интерпретация остаётся задачей агента.

---

# 4.11. Memory write

```text
POST /api/v1/profiles/{profile}/memories
```

Пример:

```json
{
  "type": "fact",
  "subject": "CMMS",
  "predicate": "uses",
  "object": "profile-owned storage",
  "confidence": 0.95,
  "source": "agent",
  "tags": ["architecture"]
}
```

Все write-запросы проходят через:

```text
Auth
 ↓
Profile isolation
 ↓
Admission
 ↓
Dedup
 ↓
Canonical write
 ↓
Receipt
 ↓
Outbox
 ↓
Async indexes
```

HTTP Gateway **не имеет bypass-пути** для прямой записи в SQLite/vector/graph.

---

# 4.12. Idempotency

Для remote clients обязательна поддержка:

```http
Idempotency-Key: <UUID>
```

Особенно для:

- remember;
- learn;
- set_belief;
- add_relation;
- lifecycle transitions;
- invalidate;
- conflict resolution.

При повторной доставке клиент должен получить исходный результат, а не второй объект памяти.

Рекомендуемое окно:

```text
24 часа
```

с возможностью настройки.

---

# 4.13. Optimistic concurrency

Любая mutation существующей памяти должна поддерживать:

```json
{
  "expected_version": 12
}
```

Если версия изменилась:

```http
409 Conflict
```

Ответ:

```json
{
  "error": {
    "code": "E_VERSION_CONFLICT",
    "message": "Memory version changed",
    "expected": 12,
    "actual": 13
  }
}
```

Neural Studio всегда должен использовать этот механизм.

---

# 4.14. Lifecycle API

```text
POST /api/v1/profiles/{profile}/memories/{id}/transition
```

Пример:

```json
{
  "to_state": "stale",
  "reason": "Superseded by updated documentation",
  "expected_version": 4
}
```

Для point invalidation:

```text
POST /api/v1/profiles/{profile}/memories/{id}/invalidate
```

Ответ должен показывать propagation:

```json
{
  "memory_id": "...",
  "from_state": "active",
  "to_state": "discarded",
  "propagated": [
    {
      "memory_id": "...",
      "action": "confidence_demoted",
      "reason": "parent_invalidated"
    }
  ]
}
```

---

# 4.15. Relations API

```text
GET    /api/v1/profiles/{profile}/memories/{id}/relations
POST   /api/v1/profiles/{profile}/relations
DELETE /api/v1/profiles/{profile}/relations/{relation_id}
```

Тип relations должен использовать canonical CMMS vocabulary, а не произвольный Cypher label.

Например:

```text
supports
contradicts
derives
derived_from
```

Если перечень расширяется — через schema/enum versioning.

---

# 4.16. Beliefs API

```text
GET  /api/v1/profiles/{profile}/beliefs
POST /api/v1/profiles/{profile}/beliefs
GET  /api/v1/profiles/{profile}/beliefs/{id}
```

Conflict resolution:

```text
POST /api/v1/profiles/{profile}/conflicts/{id}/resolve
```

Neural Studio должен показывать:

- оба утверждения;
- evidence;
- confidence;
- provenance;
- timestamps;
- lifecycle;
- рекомендуемые допустимые actions.

Но UI не должен автоматически принимать решение за пользователя без явной настройки.

---

# 4.17. Receipts / provenance

```text
GET /api/v1/profiles/{profile}/memories/{id}/receipt
GET /api/v1/profiles/{profile}/memories/{id}/history
```

История должна давать возможность восстановить:

```text
candidate created
 ↓
validated
 ↓
active
 ↓
relation added
 ↓
confidence changed
 ↓
stale
 ↓
superseded
```

Это основа inspectability CMMS.

---

# 4.18. Graph API

Graph API должен отражать **knowledge relationships**, а не являться отдельной истиной.

Рекомендуемые endpoints:

```text
GET  /api/v1/profiles/{profile}/graph/neighborhood
POST /api/v1/profiles/{profile}/graph/path
GET  /api/v1/profiles/{profile}/graph/entity/{entity_id}
```

Пример neighborhood:

```text
GET .../graph/neighborhood?memory_id=<id>&depth=2&limit=100
```

Ответ:

```json
{
  "nodes": [...],
  "edges": [...]
}
```

Каждый edge должен иметь:

```json
{
  "id": "...",
  "source_id": "...",
  "target_id": "...",
  "relation_type": "derived_from",
  "canonical": true
}
```

Если graph backend является projection/cache, API должен уметь это обозначать:

```json
{
  "graph_consistency": {
    "canonical_store": "sqlite",
    "projection": "simplegraph",
    "outbox_lag": 0
  }
}
```

---

# 4.19. Audit API

```text
GET /api/v1/profiles/{profile}/audit
```

Возможные секции:

```text
memory_health
lifecycle
contradictions
orphan_relations
missing_receipts
index_lag
expired_memory
duplicate_candidates
graph_projection
vector_projection
```

---

# 4.20. Index / outbox operations

Read:

```text
GET /api/v1/profiles/{profile}/indexes/status
GET /api/v1/profiles/{profile}/outbox
```

Operator-only:

```text
POST /api/v1/profiles/{profile}/indexes/rebuild
POST /api/v1/profiles/{profile}/outbox/replay
```

Эти операции потенциально дорогие, поэтому должны возвращать job ID:

```json
{
  "job_id": "job_...",
  "status": "queued"
}
```

---

# 4.21. Async jobs

Для долгих maintenance tasks:

```text
GET /api/v1/jobs/{job_id}
```

Пример:

```json
{
  "status": "running",
  "progress": 0.62,
  "operation": "vector_rebuild"
}
```

Jobs не должны использовать отдельного memory writer в обход main runtime.

---

# 4.22. Events

Neural Studio выиграет от live updates.

Предпочтительно:

```text
GET /api/v1/events
```

через **Server-Sent Events (SSE)**.

События:

```text
memory.created
memory.updated
memory.transitioned
memory.invalidated
belief.conflict_detected
relation.created
outbox.pending
outbox.applied
index.degraded
profile.lock_error
audit.warning
```

SSE проще WebSocket и достаточно для преимущественно server→client потока.

WebSocket имеет смысл добавлять только при появлении real-time bidirectional use case.

---

# 4.23. Error contract

Все API errors должны иметь единый формат:

```json
{
  "error": {
    "code": "E_PROFILE_NOT_FOUND",
    "message": "Profile does not exist",
    "request_id": "req_...",
    "details": {}
  }
}
```

Стабильные error codes важнее текста.

Основные категории:

```text
E_AUTH_REQUIRED
E_PERMISSION_DENIED
E_PROFILE_NOT_FOUND
E_PROFILE_DISABLED
E_WRITER_ACTIVE
E_VERSION_CONFLICT
E_VALIDATION
E_ADMISSION_REJECTED
E_NOT_FOUND
E_STORAGE
E_VECTOR_BACKEND
E_GRAPH_BACKEND
E_OUTBOX
E_RATE_LIMIT
E_REQUEST_TOO_LARGE
```

---

# 4.24. Pagination

Для коллекций:

```text
?limit=50&cursor=...
```

Cursor-based pagination предпочтительнее offset.

Нельзя загружать все memories/edges одним запросом.

Hard limits:

```text
memory list: <= 200
graph nodes: <= 500
graph edges: <= 1000
search top_k: <= 100
```

Значения должны быть configurable.

---

# 4.25. Request limits

Должны существовать:

- max body size;
- max learn input;
- max query length;
- max batch size;
- rate limits per token;
- concurrency limits для дорогих retrieval/index operations.

---

# 4.26. CORS

По умолчанию:

```text
CORS disabled
```

При использовании встроенного Neural Studio same-origin CORS вообще не требуется.

Если внешний frontend:

```yaml
cors:
  allowed_origins:
    - https://memory.example.com
```

Нельзя использовать:

```text
Access-Control-Allow-Origin: *
```

для authenticated memory API.

---

# 4.27. TLS

Gateway рекомендуется слушать:

```text
127.0.0.1
```

по умолчанию.

Для remote deployment:

```text
Caddy / nginx / Traefik
        ↓ TLS
CMMS HTTP Gateway
```

Не следует по умолчанию публиковать CMMS напрямую на `0.0.0.0`.

---

# 4.28. Audit log доступа

HTTP access audit должен быть отдельным от memory provenance.

Записывать:

```text
timestamp
request_id
token_id
client_id
profile_id
operation
memory_id
result
latency
remote address — optional/configurable
```

Не записывать:

- bearer token;
- API keys;
- полный sensitive payload без необходимости.

---

# 5. Neural Studio

# 5.1. Роль

Neural Studio — это не «чат с памятью».

Это **операторская консоль и memory debugger**.

Основные задачи:

1. видеть, что хранит агент;
2. понимать, почему память была записана;
3. понимать, почему память была извлечена;
4. видеть устаревшие/конфликтующие записи;
5. анализировать граф взаимосвязей;
6. контролировать lifecycle;
7. диагностировать vector/graph projection;
8. отслеживать outbox;
9. безопасно исправлять проблемы;
10. сравнивать поведение retrieval до и после изменений.

---

# 5.2. Принцип read-first

Первая версия Neural Studio должна быть преимущественно read-only.

Mutation operations добавляются отдельно и требуют:

- соответствующей permission;
- preview;
- явного подтверждения;
- reason;
- expected_version;
- audit record.

Особенно:

```text
invalidate
discard
archive
relation delete
conflict resolution
reindex
bulk operations
```

---

# 5.3. Главный экран — Profile Overview

Показывать:

```text
Profile: automotive
Status: healthy

Facts               12 482
Beliefs              1 203
Active               11 902
Stale                   430
Contradicted              8
Discarded               104

Vector index lag           0
Graph projection lag       0
Outbox pending             0
Conflicts unresolved       3
Audit warnings             2
```

Дополнительно:

- storage backend;
- vector backend;
- graph backend;
- storage root — только admin и в безопасном виде;
- schema version;
- last migration;
- last successful index update;
- writer status.

---

# 5.4. Memory Explorer

Единый интерфейс просмотра:

```text
Facts
Beliefs
Decisions
Skills
```

Фильтры:

```text
type
lifecycle state
confidence
source
tag
authority level
epistemic status
created range
updated range
validity
risk tag
```

Поиск должен позволять:

```text
keyword
semantic
hybrid
```

---

# 5.5. Memory detail page

Для конкретного memory item:

## Header

```text
ID
Type
Lifecycle
Version
Confidence
Created
Updated
Valid from/to
Source
```

## Main content

Atomic claim.

## Provenance

```text
source
created_by
receipt
derived_from
claim_type
evidence
```

## Relations

```text
supports
contradicts
derived_from
derives
```

## Lifecycle timeline

```text
candidate
 ↓
validated
 ↓
active
 ↓
stale
```

С указанием:

```text
timestamp
actor
reason
version
```

## Index state

```text
SQLite: canonical
Vector: indexed
Graph: indexed
Outbox: committed
```

---

# 5.6. Provenance Timeline

Отдельное представление особенно полезно для сложных знаний.

Пример:

```text
Fact A — imported from README
       │
       ├── supports
       ▼
Belief B — "CMMS uses local vector storage"
       │
       ├── derived_from
       ▼
Decision C — "use LanceDB for profile X"
```

При invalidation Fact A UI должен заранее показать:

```text
Potentially affected dependents: 4
```

До выполнения операции.

---

# 5.7. Graph Explorer

Это визуальная часть, аналогичная идее Friday Neural Studio, но основанная на CMMS semantics.

## Требования

Интерактивный graph:

```text
pan
zoom
select
expand node
collapse
filter
path search
depth
relation type
memory type
lifecycle state
```

Nodes различать по типу:

```text
Entity
Fact
Belief
Decision
Skill
```

Edges:

```text
supports
contradicts
derives
derived_from
```

Цветовая схема не должна быть единственным способом различения — нужны labels/icons для accessibility.

---

# 5.8. Graph consistency

Studio должен явно различать:

```text
canonical relations
projection graph
```

Если SQLite — canonical relation store, а SimpleGraph/Neo4j — projection, нельзя визуально представлять projection как независимую истину.

Панель:

```text
Canonical edges: 4502
Projected edges: 4499
Missing projection: 3
Extra projection: 0
Last sync: ...
```

Кнопка operator:

```text
Rebuild graph projection
```

---

# 5.9. Path Explorer

Пользователь выбирает:

```text
Source memory/entity
Target memory/entity
Max depth
Relation filters
```

Studio показывает:

```text
A
 └─ derived_from
     B
      └─ supports
          C
```

Каждый edge кликабелен и связан с canonical relation record.

---

# 5.10. Conflict Center

Специализированная страница:

```text
Unresolved contradictions
Belief conflicts
Possible duplicate facts
Supersession candidates
```

Карточка конфликта:

```text
Claim A
confidence
source
age
evidence

vs.

Claim B
confidence
source
age
evidence
```

Доступные actions должны соответствовать CMMS API:

```text
keep A
keep B
merge
mark contradicted
discard both
defer
```

UI не должен подменять reasoning engine.

---

# 5.11. Retrieval Lab

Одна из наиболее ценных функций Studio.

Пользователь вводит query:

```text
"Какой vector backend используется по умолчанию?"
```

Studio выполняет hybrid retrieval и показывает параллельно:

### FTS

```text
#1 Fact 123       score...
#2 Fact 871       score...
```

### Semantic

```text
#1 Fact 871       0.88
#2 Belief 45      0.79
```

### Graph

```text
#1 Fact 123       path: CMMS -> vector_backend -> LanceDB
```

### RankMerger

```text
final rank
source contributions
normalized scores
dedup result
```

Это позволяет отлаживать retrieval как инженерную систему, а не гадать по ответу LLM.

---

# 5.12. Context Preview

Отдельный режим:

```text
Task → get_context()
```

Studio показывает **точно тот context package**, который получил бы агент.

Полезно для:

- отладки prompt contamination;
- выявления stale memory;
- оценки лишнего контекста;
- проверки profile isolation;
- измерения token footprint.

Можно показывать:

```text
estimated tokens
memory count
confidence distribution
source distribution
inactive excluded
```

Token estimate следует маркировать как estimate, если используется приближённый tokenizer.

---

# 5.13. Admission Inspector

Для нового candidate text:

```text
Paste candidate memory
      ↓
Dry-run admission
```

Показывать:

```text
admitted: true
tag: durable
TTL: 365d
memory_kind: ...
authority_level: ...
risk_tags: ...
reason_codes: ...
```

Важно: dry-run не должен записывать memory.

Это сильно упрощает настройку MemoryAdmissionGate.

---

# 5.14. Lifecycle Inspector

Для memory item показывать допустимые переходы.

Например:

```text
ACTIVE
 ├─ stale
 ├─ archived
 └─ discarded
```

Недопустимые состояния UI не предлагает.

При переходе:

```text
Reason: mandatory
Expected version: automatic
Preview propagation: shown
```

---

# 5.15. Outbox Monitor

Экран:

```text
Pending
Processing
Failed
Applied
```

Для каждого event:

```text
event_id
memory_id
target backend
attempts
last error
next retry
created_at
```

Operator actions:

```text
retry
replay
inspect
```

Bulk replay — только admin/operator с подтверждением.

---

# 5.16. Index Health

Показывать отдельно:

```text
SQLite canonical store
Vector index
Graph projection
```

Например:

```text
SQLite records       12000
Vector records       11998
Graph relations       4500
Outbox pending           2
```

Studio должен выявлять drift.

---

# 5.17. Audit Dashboard

Визуализация результатов `audit`.

Секции:

- lifecycle anomalies;
- orphan relations;
- missing evidence;
- expired memories;
- inconsistent versions;
- missing vector records;
- missing graph edges;
- unresolved conflicts;
- unusually low/high confidence;
- admission anomalies.

Каждая warning должна вести к конкретному объекту.

---

# 5.18. Memory Quality Dashboard

Предлагаемая новая аналитика:

```text
memory growth/day
active/stale ratio
contradiction rate
dedup rate
admission reject rate
average retrieval age
average confidence
index lag
outbox failures
```

Это operational metrics, а не «quality score».

Не следует вводить единый искусственный `Memory Quality = 87%`, если нет валидированной методики.

---

# 5.19. Benchmark UI

CMMS уже имеет LongMemEval-S harness.

Studio может добавить frontend над существующим benchmark subsystem:

```text
Dataset
Run configuration
Backend
Embedding model
Target:
  raw
  source
  canonical
```

Показывать:

```text
Recall@K
MRR
Hit rate
Latency
shared-subset comparison
```

Результаты разных конфигураций нельзя сравнивать без отображения:

- dataset version;
- embedding model;
- backend;
- top_k;
- thresholds;
- code revision.

---

# 5.20. Retrieval Trace History

Опционально хранить короткое окно traces.

Экран позволяет выбрать:

```text
trace A
vs
trace B
```

Например до/после:

```text
new embedding model
ranking change
graph routing change
```

Это даст CMMS нормальный regression-debugging workflow.

---

# 5.21. Live Events

Через SSE:

```text
17:21 memory.created
17:21 outbox.pending
17:21 vector.indexed
17:21 graph.indexed
17:24 conflict.detected
```

Фильтры по:

```text
profile
event type
memory id
severity
```

---

# 5.22. Search по ID

Studio должен иметь global quick search:

```text
fact UUID
belief UUID
receipt UUID
relation UUID
trace ID
request ID
```

Это значительно ускоряет debugging.

---

# 5.23. Safe mutation UX

Для опасной операции:

```text
Discard memory?
```

Показывать:

```text
Current state: active
Version: 7
Dependent memories: 3
Vector projection: yes
Graph relations: 4
```

Требовать:

```text
reason
confirmation
```

После операции показывать receipt/audit event.

---

# 5.24. Bulk operations

Не включать в MVP.

Позже:

```text
bulk archive
bulk reindex
bulk tag
```

Но:

- с dry-run;
- с exact selection count;
- с maximum batch;
- с job;
- с rollback там, где реалистично;
- с audit record.

---

# 5.25. Sensitive data handling

Studio не должен автоматически отображать потенциальные secrets.

Рекомендуется поддержать:

```text
risk_tags: credential, secret, pii
```

При наличии такого tag UI показывает masked view:

```text
sk-************
```

Raw reveal:

- отдельная permission;
- click-to-reveal;
- audit event;
- желательно выключено по умолчанию.

---

# 5.26. UI authentication

Для локального Studio:

```text
localhost + bearer bootstrap
```

Для remote:

предпочтительно session cookie после login/token exchange:

```text
HttpOnly
Secure
SameSite=Strict
```

Не хранить permanent admin token в browser `localStorage`.

OIDC можно добавить позже.

---

# 6. Control Plane vs Memory Plane

Рекомендуется разделить данные.

## Memory plane

Per-profile:

```text
facts
beliefs
receipts
relations
lifecycle
vector
graph
outbox
```

## Control plane

Global:

```text
profile registry
API tokens
RBAC
HTTP audit log
UI sessions
async jobs metadata
```

Control-plane данные не должны смешиваться с пользовательскими memories.

Возможный storage:

```text
~/.cmms/control/control.sqlite
```

---

# 7. Proposed API map

```text
/healthz
/readyz

/api/v1/profiles
/api/v1/profiles/{profile}

/api/v1/profiles/{profile}/search
/api/v1/profiles/{profile}/context
/api/v1/profiles/{profile}/memories
/api/v1/profiles/{profile}/memories/{id}
/api/v1/profiles/{profile}/memories/{id}/receipt
/api/v1/profiles/{profile}/memories/{id}/history
/api/v1/profiles/{profile}/memories/{id}/transition
/api/v1/profiles/{profile}/memories/{id}/invalidate

/api/v1/profiles/{profile}/beliefs
/api/v1/profiles/{profile}/beliefs/{id}
/api/v1/profiles/{profile}/conflicts
/api/v1/profiles/{profile}/conflicts/{id}/resolve

/api/v1/profiles/{profile}/relations
/api/v1/profiles/{profile}/relations/{id}

/api/v1/profiles/{profile}/graph/neighborhood
/api/v1/profiles/{profile}/graph/path

/api/v1/profiles/{profile}/audit
/api/v1/profiles/{profile}/reflect
/api/v1/profiles/{profile}/indexes/status
/api/v1/profiles/{profile}/outbox

/api/v1/profiles/{profile}/retrieval/trace/{id}

/api/v1/jobs/{id}
/api/v1/events
```

---

# 8. Optional MCP over HTTP

После появления daemon имеет смысл добавить стандартный сетевой MCP transport, если версия MCP SDK CMMS поддерживает актуальный Streamable HTTP transport.

Это отдельная возможность:

```text
Remote MCP client
       ↓
MCP Streamable HTTP
       ↓
same CMMS Runtime
```

Важно:

- не писать собственный несовместимый «MCP-like HTTP protocol»;
- не дублировать memory logic;
- REST остаётся удобным API для Studio и обычных интеграций;
- MCP HTTP нужен именно MCP-клиентам.

---

# 9. Recommended internal interfaces

Пример интерфейса application layer:

```python
class MemoryApplication:
    async def remember(...)
    async def learn(...)
    async def search(...)
    async def semantic_search(...)
    async def route(...)
    async def get_context(...)
    async def invalidate(...)
    async def transition(...)
    async def add_relation(...)
    async def resolve_conflict(...)
    async def reflect(...)
    async def audit(...)
```

Transport:

```python
MCPAdapter(app)
HTTPAdapter(app)
HermesAdapter(app)
```

Это позволяет тестировать semantics один раз.

---

# 10. Observability

Gateway должен генерировать:

```text
request latency
request counts
response status
profile
operation
retrieval latency
FTS latency
vector latency
graph latency
rank merge latency
outbox lag
index errors
auth failures
version conflicts
```

Не использовать memory content как Prometheus labels.

Пример metrics:

```text
cmms_http_requests_total
cmms_http_request_duration_seconds
cmms_retrieval_duration_seconds
cmms_outbox_pending
cmms_vector_index_lag
cmms_graph_index_lag
cmms_version_conflicts_total
```

---

# 11. Threat model

Минимально учитывать:

## 11.1. Unauthorized memory read

Защита:

- auth required;
- scoped tokens;
- no public memory endpoints.

## 11.2. Cross-profile leakage

Защита:

- profile-aware auth;
- profile-specific StorageLayout;
- integration tests.

## 11.3. Replay duplicate writes

Защита:

- idempotency keys.

## 11.4. Concurrent destructive update

Защита:

- expected_version.

## 11.5. Credential leakage

Защита:

- no token in query string;
- redacted logs;
- hash stored tokens;
- masked Studio.

## 11.6. Browser attack

Защита:

- same-origin UI;
- CSP;
- Secure/HttpOnly cookies;
- SameSite;
- CSRF protection для cookie-authenticated mutations;
- no wildcard CORS.

## 11.7. Expensive query DoS

Защита:

- top_k limits;
- graph depth limits;
- request size;
- rate limiting;
- timeout;
- concurrency limits.

## 11.8. Memory poisoning

Gateway сам не решает проблему, но Studio должен облегчать обнаружение:

- provenance;
- admission result;
- confidence;
- source;
- contradictions;
- lifecycle;
- receipt history.

---

# 12. Neural Studio frontend architecture

Необязательно привязываться к конкретному frontend framework.

Важнее архитектура:

```text
Browser
  ↓
same-origin HTTPS
  ↓
HTTP Gateway
  ↓
CMMS Application Services
```

Studio можно:

- собирать как статический SPA;
- отдавать самим CMMS gateway;
- держать frontend отдельно в dev mode.

Production предпочтительно same-origin:

```text
https://memory.example/
https://memory.example/api/v1/...
```

Это упрощает CORS и session security.

---

# 13. Suggested navigation

```text
CMMS Neural Studio
│
├─ Profiles
│   └─ Overview
│
├─ Memory
│   ├─ Facts
│   ├─ Beliefs
│   ├─ Decisions
│   └─ Skills
│
├─ Retrieval
│   ├─ Search
│   ├─ Retrieval Lab
│   ├─ Context Preview
│   └─ Traces
│
├─ Knowledge Graph
│   ├─ Explorer
│   └─ Path Finder
│
├─ Integrity
│   ├─ Conflicts
│   ├─ Lifecycle
│   ├─ Provenance
│   ├─ Audit
│   └─ Outbox
│
├─ Evaluation
│   ├─ LongMemEval
│   └─ Benchmark Runs
│
└─ Administration
    ├─ Profiles
    ├─ API Tokens
    ├─ Indexes
    ├─ Jobs
    └─ Runtime
```

---

# 14. Что не следует переносить из Friday

При заимствовании идей Neural Studio/Gateway важно не копировать слабые места.

## Не делать

### 14.1. «Project» как обычная metadata вместо настоящей isolation

CMMS уже имеет более правильную profile-owned модель.

### 14.2. Public GET для facts/graph/search

Memory содержимое всегда требует auth.

### 14.3. Graph отдельно от retrieval semantics

Graph в CMMS должен продолжать участвовать в retrieval и lifecycle/provenance, а UI лишь визуализирует его.

### 14.4. Инструкцию «memory = absolute truth»

Каждый memory item имеет epistemic state, confidence, provenance и lifecycle.

### 14.5. Отдельный graph database как самостоятельный источник истины без reconciliation

Canonical source должен быть чётко определён.

### 14.6. Скрывать storage failure за HTTP 200

Если canonical write не состоялся, операция не может считаться successful.

---

# 15. Этапы реализации

## Phase 0 — Service-layer refactor

Цель:

```text
MCP handlers ≠ business logic
```

Выделить application services.

Acceptance:

- MCP tests проходят;
- поведение не меняется;
- transport-independent tests.

---

## Phase 1 — Read-only HTTP Gateway

Реализовать:

- daemon;
- auth;
- profile routing;
- health;
- profiles;
- search;
- get_context;
- memory read;
- receipts;
- relations;
- graph read;
- audit;
- index/outbox status.

Это минимальный безопасный фундамент Studio.

---

## Phase 2 — Retrieval tracing

Добавить:

- trace generation;
- FTS/vector/graph breakdown;
- RankMerger trace;
- short retention;
- request correlation.

После этого можно строить Retrieval Lab.

---

## Phase 3 — Neural Studio read-only

Реализовать:

- profile dashboard;
- memory explorer;
- detail page;
- provenance;
- graph;
- retrieval lab;
- context preview;
- audit;
- outbox/index monitor.

На этом этапе Studio уже имеет высокую практическую ценность без destructive API.

---

## Phase 4 — Controlled mutations

Добавить:

- remember;
- learn;
- lifecycle transition;
- invalidate;
- relation edit;
- conflict resolution.

Обязательно:

```text
permission
reason
expected_version
audit
confirmation
```

---

## Phase 5 — Maintenance operations

Добавить:

- reindex;
- replay outbox;
- background jobs;
- progress;
- live events.

---

## Phase 6 — Remote deployment hardening

Добавить:

- robust token management;
- rate limiting;
- TLS deployment guide;
- CSP;
- CSRF;
- hardened headers;
- remote-access tests;
- security review.

---

## Phase 7 — Optional MCP Streamable HTTP

Добавить стандартный remote MCP transport поверх того же runtime.

---

# 16. Acceptance criteria — HTTP Gateway

Gateway можно считать готовым к beta, если выполняются все условия:

- [ ] HTTP и MCP используют один application-service layer.
- [ ] Нельзя открыть один profile двумя independent writer runtime.
- [ ] Все memory endpoints требуют authentication.
- [ ] Token permissions ограничиваются profile scope.
- [ ] Cross-profile tests подтверждают отсутствие leakage.
- [ ] Write endpoints поддерживают idempotency.
- [ ] Mutation endpoints поддерживают expected_version.
- [ ] Все ошибки имеют стабильные error codes.
- [ ] Ни один failure canonical storage не возвращается как success.
- [ ] Relations проходят schema validation.
- [ ] CORS по умолчанию закрыт.
- [ ] Secrets не попадают в logs.
- [ ] Request size/top_k/graph depth ограничены.
- [ ] HTTP actions имеют request_id.
- [ ] Remote mutations попадают в audit.
- [ ] Outbox/index state доступен для диагностики.

---

# 17. Acceptance criteria — Neural Studio

Studio можно считать готовым к beta, если:

- [ ] Видны все profile health indicators.
- [ ] Memory Explorer поддерживает facts/beliefs/decisions/skills.
- [ ] Memory detail показывает provenance и lifecycle history.
- [ ] Graph view использует canonical IDs.
- [ ] Graph view различает canonical relation и projection state.
- [ ] Retrieval Lab показывает FTS/vector/graph contributions.
- [ ] Context Preview показывает фактический agent context.
- [ ] Audit warnings кликабельны.
- [ ] Outbox/index lag видим.
- [ ] Conflict Center показывает evidence обеих сторон.
- [ ] Mutation требует confirmation + reason.
- [ ] Mutation использует expected_version.
- [ ] Destructive operations требуют повышенной permission.
- [ ] Sensitive memory маскируется.
- [ ] UI не хранит permanent admin token в localStorage.

---

# 18. Предлагаемая структура репозитория

Один из вариантов:

```text
src/memory_server/
├─ application/
│  ├─ memory_service.py
│  ├─ retrieval_service.py
│  ├─ belief_service.py
│  ├─ graph_service.py
│  └─ admin_service.py
│
├─ transports/
│  ├─ mcp_stdio.py
│  ├─ http/
│  │  ├─ app.py
│  │  ├─ auth.py
│  │  ├─ dependencies.py
│  │  ├─ errors.py
│  │  ├─ schemas.py
│  │  └─ routes/
│  └─ hermes.py
│
├─ control/
│  ├─ profiles.py
│  ├─ tokens.py
│  ├─ jobs.py
│  └─ audit.py
│
└─ studio/
   └─ static/
```

Frontend source при желании отдельно:

```text
studio/
├─ src/
├─ package.json
└─ dist/
```

---

# 19. Testing strategy

## 19.1. Contract tests

Одинаковый use case вызывается через:

```text
application service
MCP
HTTP
```

Результат должен быть семантически эквивалентен.

## 19.2. Profile isolation tests

Создать:

```text
profile A
profile B
```

Записать одинаковые subject/predicate/object с разными значениями.

Проверить:

- search;
- semantic search;
- graph;
- context;
- audit;
- Studio;
- SSE.

Ни один result не должен пересечь границу profile.

## 19.3. Idempotency tests

Один и тот же POST повторить:

```text
10 раз
```

Должен существовать один canonical object.

## 19.4. Race tests

Два клиента обновляют version 4.

Один получает success, второй:

```text
409 E_VERSION_CONFLICT
```

## 19.5. Security tests

Проверить:

- no-auth;
- wrong profile token;
- revoked token;
- CORS;
- CSRF;
- oversized payload;
- graph depth abuse;
- injection attempts;
- secret logging.

## 19.6. Projection drift tests

Искусственно нарушить vector/graph projection.

Studio/Audit должны увидеть drift.

---

# 20. Практическая ценность для CMMS

HTTP Gateway решает:

```text
local-only MCP
        ↓
network-accessible memory service
```

Neural Studio решает:

```text
opaque memory behavior
        ↓
observable / debuggable memory behavior
```

Вместе они добавляют CMMS отсутствующий control/observability plane, не меняя его ядро.

Целевая модель:

```text
                         ┌──────────────────────────┐
                         │      Neural Studio       │
                         │ graph / trace / audit    │
                         └────────────┬─────────────┘
                                      │
                                 HTTPS / SSE
                                      │
┌─────────────┐                 ┌─────▼──────┐
│ Remote      │──── HTTPS ─────►│ HTTP       │
│ agents      │                 │ Gateway    │
└─────────────┘                 └─────┬──────┘
                                      │
┌─────────────┐                       │
│ MCP agents  │──── MCP ──────────────┤
└─────────────┘                       │
                                      ▼
                           ┌─────────────────────┐
                           │ CMMS Application    │
                           │ Services            │
                           └──────────┬──────────┘
                                      │
          ┌───────────────────────────┼─────────────────────┐
          ▼                           ▼                     ▼
       SQLite                    Vector index            Graph
      canonical                 LanceDB/Qdrant         projection
          │                           │                     │
          └──────────────────── Outbox ─────────────────────┘
```

Главное отличие от простой «web-панели над vector DB»:

**Neural Studio должен визуализировать семантику CMMS — provenance, lifecycle, conflicts, retrieval fusion и projection consistency.**

Именно это делает его инженерным инструментом для memory system, а не декоративным graph viewer.

---

# 21. Приоритет функций

Если реализовывать последовательно, максимальную отдачу дадут:

1. **единственный daemon/runtime + HTTP transport**;
2. **profile-scoped authentication**;
3. **read-only memory/provenance API**;
4. **retrieval trace**;
5. **Retrieval Lab + Context Preview**;
6. **Graph Explorer**;
7. **Outbox/Index Health**;
8. **Conflict Center**;
9. **controlled lifecycle mutations**;
10. **remote MCP transport**.

Retrieval Trace и Context Preview для CMMS имеют даже большую практическую ценность, чем сам красивый graph UI: они дают возможность непосредственно понять, **почему агент вспомнил именно это**.

---

# 22. Краткое итоговое требование

Расширение должно следовать принципу:

> **HTTP Gateway exposes CMMS; Neural Studio explains CMMS. Neither reimplements CMMS.**

Gateway — transport/control plane.

Studio — observability/operations plane.

Canonical memory semantics остаются в существующих CMMS services.

Это позволяет сохранить сильные стороны CMMS и добавить то, что особенно удачно показано в Friday: доступность центральной памяти по сети и визуальную inspectability, но без упрощения profile isolation, lifecycle и provenance.
