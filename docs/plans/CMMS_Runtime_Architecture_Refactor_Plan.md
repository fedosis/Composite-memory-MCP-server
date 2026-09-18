# CMMS Runtime Architecture Refactor
## План структурного рефакторинга перед HTTP Gateway и Neural Studio

**Проект:** `fedosis/Composite-memory-MCP-server`  
**Предлагаемый релиз:** `v0.13.x`  
**Тип изменения:** internal architecture refactor / no intended user-visible behavior change  
**Дата:** 2026-09-18

---

# 1. Цель

Подготовить CMMS к дальнейшему развитию как централизованного memory service с несколькими transport adapters:

- MCP stdio;
- Hermes native provider;
- HTTP Gateway;
- в перспективе MCP Streamable HTTP;
- Neural Studio как web client.

При этом необходимо сохранить текущие свойства CMMS:

- local-first;
- profile isolation;
- single-writer protection;
- deterministic storage layout;
- lifecycle semantics;
- admission rules;
- transactional outbox;
- SQLite as canonical structured store;
- vector/graph projections;
- provenance/receipts;
- hybrid retrieval;
- backward compatibility существующих MCP/Hermes сценариев.

Главная задача рефакторинга:

> **Отделить memory runtime и application services от transport-specific кода.**

После рефакторинга любой transport должен быть тонким adapter поверх одного и того же application/runtime layer.

---

# 2. Почему рефакторинг нужен до HTTP Gateway

Текущая архитектура уже содержит полезное разделение:

```text
api/
services/
providers/
router/
evaluation/
models/
```

Однако `server.py` одновременно выполняет слишком много ролей:

```text
server.py
 ├─ создаёт FastMCP
 ├─ владеет startup/shutdown
 ├─ владеет StorageLayout
 ├─ захватывает RuntimeStorageLock
 ├─ создаёт SQLiteProvider
 ├─ создаёт vector provider
 ├─ создаёт embedding provider
 ├─ создаёт graph provider
 ├─ создаёт routers
 ├─ создаёт outbox worker
 ├─ запускает background task
 ├─ содержит singleton/global runtime state
 └─ регистрирует MCP tools
```

Для одного stdio MCP runtime это приемлемо.

Для будущей схемы:

```text
MCP
HTTP
Hermes
Neural Studio
```

такой подход создаёт риск:

- дублирования startup/shutdown logic;
- нескольких независимых writer runtime;
- обхода storage locks;
- расхождения поведения между transports;
- появления transport-specific business logic;
- cross-profile leakage;
- усложнения тестирования;
- невозможности надёжно управлять несколькими profile runtimes в одном daemon.

---

# 3. Главный архитектурный принцип

Целевая схема:

```text
                    ┌───────────────────────────┐
                    │        CMMSRuntime        │
                    │                           │
                    │ StorageLayout             │
                    │ StorageLock               │
                    │ SQLiteProvider            │
                    │ VectorProvider            │
                    │ EmbeddingProvider         │
                    │ GraphProvider             │
                    │ HybridRouter              │
                    │ OutboxWorker              │
                    │ Runtime lifecycle         │
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │    ApplicationServices    │
                    │                           │
                    │ MemoryService             │
                    │ RetrievalService          │
                    │ BeliefService             │
                    │ LifecycleService          │
                    │ RelationService           │
                    │ AuditService              │
                    │ GraphService              │
                    └─────────────┬─────────────┘
                                  │
         ┌────────────────────────┼────────────────────────┐
         │                        │                        │
 ┌───────▼────────┐      ┌────────▼────────┐      ┌────────▼────────┐
 │ MCP stdio      │      │ HTTP Gateway    │      │ Hermes native  │
 │ adapter        │      │ adapter         │      │ adapter        │
 └────────────────┘      └─────────────────┘      └─────────────────┘
```

Правило:

> Transport не владеет memory storage и не реализует memory semantics.

---

# 4. Scope

В рамках данного релиза необходимо:

1. выделить явный runtime object;
2. удалить transport ownership над providers;
3. убрать module-level singleton runtime state из `server.py`;
4. выделить application-service layer;
5. сделать MCP transport тонким adapter;
6. подготовить RuntimeManager для multi-profile daemon;
7. сохранить существующий storage layout и lock semantics;
8. сохранить существующий outbox pattern;
9. сохранить существующие MCP contracts;
10. сохранить Hermes integration behavior;
11. добавить transport-independent integration tests.

---

# 5. Non-goals

В этот рефакторинг **не следует включать**:

- HTTP Gateway;
- Neural Studio;
- auth/RBAC;
- SSE/WebSocket;
- remote MCP;
- OIDC;
- новый graph backend;
- переход на Neo4j;
- изменение SQLite schema без необходимости;
- изменение admission policy;
- изменение lifecycle semantics;
- изменение claim relation vocabulary;
- изменение vector ranking;
- изменение embedding model;
- изменение LongMemEval scoring;
- переработку всех provider interfaces;
- migration существующих user data, если storage format не меняется.

Если изменение не необходимо для transport/runtime separation — оно не входит в Phase 0.

---

# 6. Инварианты

После рефакторинга должны оставаться верными следующие инварианты.

## 6.1. Canonical data

SQLite остаётся canonical structured source для тех данных, для которых он является canonical storage сейчас.

## 6.2. Projection model

Vector и graph остаются projections/caches там, где это соответствует текущей архитектуре.

## 6.3. Outbox

Canonical write и outbox publication не должны быть разделены transport layer.

## 6.4. Single writer

Один profile storage root не может одновременно принадлежать двум independent writer runtimes.

## 6.5. Profile isolation

Provider одного profile не должен быть доступен из runtime другого profile.

## 6.6. Transport equivalence

Одинаковый application use case должен иметь одинаковую семантику независимо от MCP/Hermes/HTTP.

---

# 7. Целевая структура пакетов

Предлагаемая структура:

```text
src/memory_server/
│
├─ runtime/
│  ├─ __init__.py
│  ├─ runtime.py
│  ├─ runtime_factory.py
│  ├─ runtime_manager.py
│  ├─ dependencies.py
│  └─ errors.py
│
├─ application/
│  ├─ __init__.py
│  ├─ memory_service.py
│  ├─ retrieval_service.py
│  ├─ belief_service.py
│  ├─ relation_service.py
│  ├─ graph_service.py
│  ├─ audit_service.py
│  └─ application.py
│
├─ transports/
│  ├─ __init__.py
│  ├─ mcp_stdio.py
│  └─ hermes.py
│
├─ services/
│  ├─ ingestion_service.py
│  └─ lifecycle_service.py
│
├─ providers/
├─ router/
├─ evaluation/
├─ models/
├─ extractors/
├─ api/
│
├─ paths.py
├─ settings.py
├─ storage_lock.py
├─ cli.py
└─ server.py
```

На переходном этапе старые `api/*` могут остаться совместимыми facades.

---

# 8. `CMMSRuntime`

# 8.1. Ответственность

`CMMSRuntime` отвечает только за **runtime resources и lifecycle процесса**.

Он должен владеть:

```text
StorageLayout
RuntimeStorageLock
SQLiteProvider
VectorProvider
EmbeddingProvider
GraphProvider
Routers
OutboxWorker
Outbox background task
evaluation dependencies
```

Он не должен владеть:

```text
MCP objects
HTTP app
request context
RBAC
UI logic
transport serialization
```

---

# 8.2. Базовый интерфейс

Пример:

```python
class CMMSRuntime:
    def __init__(
        self,
        layout: StorageLayout,
        settings: Settings,
        dependencies: RuntimeDependencies,
    ):
        ...

    async def start(self) -> None:
        ...

    async def close(self) -> None:
        ...

    @property
    def started(self) -> bool:
        ...

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
```

---

# 8.3. Runtime dependencies

Рекомендуется явный dependency container:

```python
@dataclass
class RuntimeDependencies:
    sqlite: SQLiteProvider
    vector: VectorProvider | None
    embedder: EmbeddingProvider | None
    graph: GraphProvider | None
    graph_router: GraphRouter | None
    embedding_router: EmbeddingRouter | None
    hybrid_router: HybridRouter | None
    outbox_worker: OutboxWorker | None
    confidence_engine: ConfidenceEngine
    decay_engine: DecayEngine
    validator: Validator
```

Важно:

- container создаётся runtime factory;
- transport не модифицирует его;
- тесты могут подставлять fake/in-memory dependencies.

---

# 9. Runtime Factory

Отдельный factory должен отвечать за создание runtime dependencies.

```python
class RuntimeFactory:
    async def create(
        self,
        layout: StorageLayout,
        settings: Settings,
    ) -> CMMSRuntime:
        ...
```

Factory отвечает за:

- validate storage paths;
- create/open SQLite;
- create vector backend;
- create graph backend;
- create embedder;
- build routers;
- build outbox worker;
- construct evaluation services.

Factory не запускает transport.

---

# 10. Runtime lifecycle

Порядок startup должен быть детерминирован.

## 10.1. Start

```text
resolve/freeze StorageLayout
        ↓
validate storage root
        ↓
acquire RuntimeStorageLock
        ↓
open canonical SQLite
        ↓
initialize optional projections
        ↓
initialize routers
        ↓
initialize outbox worker
        ↓
start background outbox task
        ↓
runtime READY
```

## 10.2. Shutdown

```text
mark runtime CLOSING
        ↓
stop accepting new operations
        ↓
signal outbox worker stop
        ↓
await outbox task termination
        ↓
close vector provider
        ↓
close embedding provider
        ↓
close graph provider if needed
        ↓
close SQLite
        ↓
release RuntimeStorageLock
        ↓
runtime CLOSED
```

Storage lock нельзя освобождать раньше подтверждённого закрытия writer resources.

---

# 11. Runtime states

Рекомендуется явная state machine:

```text
NEW
 ↓
STARTING
 ↓
READY
 ↓
CLOSING
 ↓
CLOSED
```

Дополнительно:

```text
FAILED
```

Недопустимые операции должны завершаться стабильной ошибкой.

Например:

```text
E_RUNTIME_NOT_READY
E_RUNTIME_CLOSING
E_RUNTIME_FAILED
```

---

# 12. Удаление module globals

Текущие состояния вида:

```python
_provider
_qdrant
_lancedb
_embedder
_graph
_graph_router
_hybrid_router
_outbox_worker
_outbox_task
_root_lock
_storage_layout
```

не должны быть runtime authority.

После refactor:

```python
runtime.dependencies.sqlite
runtime.dependencies.vector
runtime.dependencies.graph
runtime.dependencies.hybrid_router
runtime.outbox_task
runtime.storage_lock
runtime.layout
```

Допустим временный compatibility wrapper, но он не должен создавать второй runtime.

---

# 13. Application Layer

# 13.1. Назначение

Application layer описывает **use cases**, а не transports.

Transport adapters вызывают application services.

---

# 13.2. `MemoryApplication`

Удобно иметь composition root:

```python
class MemoryApplication:
    def __init__(self, runtime: CMMSRuntime):
        self.memory = MemoryService(runtime)
        self.retrieval = RetrievalService(runtime)
        self.beliefs = BeliefService(runtime)
        self.relations = RelationService(runtime)
        self.graph = GraphService(runtime)
        self.audit = AuditService(runtime)
        self.lifecycle = LifecycleApplicationService(runtime)
```

---

# 14. MemoryService

Отвечает за:

```text
remember
learn
bulk_import — если оставляется публичным
```

Не должен:

- управлять transport;
- читать env;
- создавать providers.

Порядок remember:

```text
request
 ↓
validation
 ↓
admission
 ↓
dedup
 ↓
canonical transaction
 ↓
receipt
 ↓
outbox
 ↓
response
```

---

# 15. RetrievalService

Отвечает за:

```text
search
semantic_search
hybrid route
get_context
```

Пример:

```python
class RetrievalService:
    async def search(...)
    async def semantic_search(...)
    async def route(...)
    async def get_context(...)
```

Это особенно важно для будущего Retrieval Trace.

---

# 16. BeliefService

Перенести сюда transport-independent:

```text
set_belief
get_belief
resolve_conflict
reflect-related belief operations
```

Если `reflect` является broader evaluation operation — можно оставить отдельным `ReflectionService`.

---

# 17. RelationService

Отвечает за:

```text
add_relation
remove_relation — если появится
list_relations
relation validation
```

Canonical relation vocabulary должен проверяться здесь, а не в transport.

---

# 18. GraphService

Отвечает за:

```text
graph_search
neighborhood
path traversal
projection status
```

Даже если на текущем этапе MCP exposes только `graph_search`, будущий HTTP transport сможет использовать этот service без прямого доступа к provider.

---

# 19. AuditService

Отвечает за:

```text
audit
metrics snapshots
projection health
outbox health
```

Prometheus exposition может остаться отдельным transport concern, но сбор данных — application/domain layer.

---

# 20. LifecycleService

Существующий `LifecycleService` является domain/service компонентом и не должен переписываться без необходимости.

При необходимости создать application wrapper:

```python
class LifecycleApplicationService:
    async def transition(...)
    async def invalidate(...)
```

который:

- проверяет input;
- вызывает domain lifecycle;
- формирует transport-independent result DTO.

---

# 21. DTO / command-query contracts

Application layer должен принимать обычные Pydantic/dataclass commands.

Например:

```python
class SearchQuery(BaseModel):
    query: str
    limit: int = 10
    include_inactive: bool = False
```

```python
class RememberCommand(BaseModel):
    subject: str
    predicate: str
    object: str
    confidence: float = 0.5
    source: str = "manual"
    tags: list[str] = []
```

MCP/HTTP преобразуют transport payload → application DTO.

---

# 22. Transport-independent responses

Не возвращать из application layer:

- MCP-specific content blocks;
- HTTP Response;
- status code;
- raw JSON string, если можно вернуть typed DTO.

Вместо:

```python
return json.dumps({...})
```

предпочтительно:

```python
return SearchResult(...)
```

Serialization — задача transport.

---

# 23. MCP Adapter

После refactor `mcp_stdio.py` должен быть максимально тонким.

Пример:

```python
@mcp.tool()
async def search(query: str, limit: int = 10):
    result = await app.retrieval.search(
        SearchQuery(query=query, limit=limit)
    )
    return result.model_dump()
```

MCP adapter отвечает за:

- MCP registration;
- MCP-compatible schemas;
- transport serialization;
- mapping exceptions to current MCP-compatible behavior.

Не отвечает за:

- provider creation;
- storage locks;
- outbox;
- lifecycle semantics.

---

# 24. `server.py` после refactor

Вариант A:

`server.py` остаётся backward-compatible entrypoint:

```python
from memory_server.transports.mcp_stdio import mcp

def main():
    mcp.run()
```

То есть файл становится composition/entrypoint, а не местом, где живёт runtime.

---

# 25. Hermes Adapter

Hermes integration должен использовать тот же application/runtime layer.

Цель:

```text
Hermes callback
 ↓
Hermes adapter
 ↓
MemoryApplication
 ↓
CMMSRuntime
```

Не должно быть отдельной реализации memory semantics для Hermes.

---

# 26. RuntimeManager

`RuntimeManager` нужен для будущего multi-profile daemon.

Даже если v0.13 не запускает HTTP daemon, abstraction стоит добавить и протестировать.

---

# 27. RuntimeManager API

Пример:

```python
class RuntimeManager:
    async def get(self, profile_id: str) -> CMMSRuntime:
        ...

    async def start_profile(self, profile_id: str) -> CMMSRuntime:
        ...

    async def stop_profile(self, profile_id: str) -> None:
        ...

    async def close_all(self) -> None:
        ...
```

---

# 28. RuntimeManager ownership

Manager должен гарантировать:

```text
one profile_id
      ↓
one resolved StorageLayout
      ↓
one CMMSRuntime
      ↓
one writer lock
```

Внутри одного daemon повторный `get(profile)` возвращает существующий runtime.

---

# 29. Profile identity

Нельзя строить RuntimeManager на mutable global environment.

Нужен explicit profile descriptor:

```python
@dataclass(frozen=True)
class ProfileDescriptor:
    profile_id: str
    data_root: Path
    settings_overrides: Mapping[str, Any]
```

---

# 30. Замороженный StorageLayout

После создания runtime:

```text
StorageLayout immutable
```

Нельзя менять:

- SQLite path;
- LanceDB path;
- Qdrant collection/location;
- graph snapshot;
- data root

через последующую mutation settings.

Для изменения profile storage требуется новый runtime/migration.

---

# 31. Profile-scoped settings

Настройки рекомендуется разделить:

```text
Global daemon settings
Profile runtime settings
Transport settings
```

Пример будущей модели:

```python
@dataclass(frozen=True)
class RuntimeConfig:
    profile_id: str
    storage: StorageConfig
    vector: VectorConfig
    graph: GraphConfig
    embedding: EmbeddingConfig
```

---

# 32. Не читать ENV внутри глубокой логики

`os.environ` / `get_settings()` следует читать в composition root/factory.

Плохой вариант:

```python
async def search(...):
    backend = os.environ["MEMORY_VECTOR_BACKEND"]
```

Предпочтительный:

```python
service.runtime.config.vector.backend
```

Это критично для нескольких profile runtimes в одном процессе.

---

# 33. Outbox ownership

OutboxWorker должен принадлежать runtime.

Нельзя:

```text
global one outbox worker
```

для нескольких profiles.

Нужно:

```text
Runtime A
 └─ OutboxWorker A

Runtime B
 └─ OutboxWorker B
```

Каждый worker использует providers своего runtime.

---

# 34. Background task ownership

Все background tasks должны регистрироваться runtime.

Пример:

```python
runtime.tasks = {
    "outbox": task,
}
```

На shutdown runtime обязан проверить termination всех owned tasks.

---

# 35. Projection availability

Существующее поведение `E_PROJECTION_UNAVAILABLE` следует сохранить, но mapping ошибок перенести.

Domain/application:

```python
raise ProjectionUnavailable(...)
```

MCP adapter:

```text
ProjectionUnavailable → current MCP-compatible error payload
```

Будущий HTTP adapter:

```text
ProjectionUnavailable → HTTP 503 + E_PROJECTION_UNAVAILABLE
```

---

# 36. Error model

Рекомендуется ввести typed internal exceptions:

```text
CMMSRuntimeError
RuntimeNotReady
RuntimeClosing
StorageOwnershipError
ProjectionUnavailable
ValidationError
VersionConflict
AdmissionRejected
MemoryNotFound
```

Transport adapters преобразуют их в свои форматы.

---

# 37. Backward compatibility

В v0.13 пользовательские команды должны продолжить работать:

```bash
memory-server serve
```

MCP tool names и основные contracts не должны меняться.

Hermes plugin installation/use не должен требовать новых действий.

---

# 38. Compatibility facade для `api/`

Сейчас `api/remember.py`, `api/search.py`, `api/get_context.py` и др. уже используются.

На первом этапе можно сохранить:

```python
async def search(provider, ...):
    ...
```

или превратить их в thin compatibility wrappers к application services.

Не обязательно удалять `api/` сразу.

Цель — избежать большого одномоментного diff.

---

# 39. Refactor strategy

Рекомендуется **strangler-style refactor**, а не big-bang rewrite.

```text
старый server path
       ↓
выделить runtime
       ↓
выделить application service
       ↓
переключить MCP tool на service
       ↓
добавить тесты
       ↓
удалить старую логику
```

Для каждого use case отдельно.

---

# 40. Карточки реализации

Оптимально разбить Phase 0 на 6 основных карточек.

---

# CARD 1 — `CMMSRuntime` и lifecycle ownership

## Цель

Извлечь из `server.py`:

- storage layout;
- root lock;
- provider lifecycle;
- outbox lifecycle;
- shutdown handling.

## Изменения

Создать:

```text
runtime/runtime.py
runtime/dependencies.py
runtime/errors.py
```

Перенести:

```text
_ensure_storage_ready
_get_storage_layout
provider initialization
outbox startup
shutdown_storage
```

в runtime/factory.

## Не менять

- MCP tools;
- memory behavior;
- provider contracts.

## Acceptance

- `CMMSRuntime.start()` открывает runtime;
- `close()` закрывает;
- async context manager работает;
- lock удерживается до полного shutdown;
- повторный close безопасен или детерминированно запрещён;
- startup failure корректно освобождает только безопасно освобождаемые ресурсы.

---

# CARD 2 — RuntimeFactory / dependency construction

## Цель

Убрать provider construction из transport layer.

## Создать

```text
runtime/runtime_factory.py
```

## Factory строит

- SQLiteProvider;
- vector provider;
- embedder;
- graph;
- routers;
- outbox;
- evaluator dependencies.

## Acceptance

- runtime можно создать без MCP;
- test factory может использовать fake providers;
- никаких provider globals не требуется.

---

# CARD 3 — Application Services

## Цель

Создать transport-independent API.

## Создать

```text
application/application.py
application/memory_service.py
application/retrieval_service.py
application/belief_service.py
application/relation_service.py
application/graph_service.py
application/audit_service.py
```

## Перенести use cases

Минимум:

```text
remember
learn
search
semantic_search
get_context
route
graph_search
add_relation
set_belief
get_belief
resolve_conflict
reflect
audit
invalidate
```

## Acceptance

Каждый use case можно вызвать напрямую:

```python
runtime = await factory.create(...)
await runtime.start()

app = MemoryApplication(runtime)
result = await app.retrieval.search(...)

await runtime.close()
```

без MCP.

---

# CARD 4 — MCP transport adapter

## Цель

Превратить `server.py` в transport composition root.

## Создать

```text
transports/mcp_stdio.py
```

## MCP tools должны

- parse args;
- создать application DTO;
- вызвать service;
- serialize result;
- map internal error.

## Acceptance

Существующие MCP contract tests проходят без изменения expected behavior.

---

# CARD 5 — RuntimeManager / profile runtime isolation

## Цель

Подготовить architecture для multi-profile daemon.

## Создать

```text
runtime/runtime_manager.py
```

## Проверить

- два разных profile;
- два StorageLayout;
- два root lock;
- разные SQLite DB;
- разные vector namespaces;
- разные graph projections;
- разные outbox workers.

## Acceptance

Cross-profile integration test должен гарантировать отсутствие утечки.

---

# CARD 6 — Hermes adapter + regression hardening

## Цель

Подключить Hermes к новому application/runtime layer.

## Проверить

- auto-recall;
- auto-retain;
- session boundary flush;
- profile storage resolution;
- shutdown;
- existing plugin wiring.

## Дополнительно

- удалить устаревшие globals;
- удалить duplicate construction paths;
- обновить architecture docs;
- добавить migration notes: data migration not required.

---

# 41. Dependency graph карточек

```text
CARD 1 Runtime
   │
   ▼
CARD 2 Factory
   │
   ▼
CARD 3 Application
   │
   ├─────────────┐
   ▼             ▼
CARD 4 MCP    CARD 5 RuntimeManager
   │             │
   └──────┬──────┘
          ▼
      CARD 6 Hermes + hardening
```

---

# 42. Разбиение на более мелкие подкарточки

Если worker sessions должны быть короткими, можно разбить на 10–12 задач:

```text
R1 Runtime state machine
R2 Storage lock ownership
R3 Provider factory
R4 Outbox ownership
R5 Memory application service
R6 Retrieval application service
R7 Belief/relation/lifecycle services
R8 MCP adapter
R9 RuntimeManager
R10 multi-profile tests
R11 Hermes adapter
R12 cleanup/docs
```

---

# 43. Размер одной карточки

Рекомендуемый предел:

```text
production diff: ~300–800 LOC
tests: ~200–600 LOC
```

Допускается больше для механического extraction/refactor, если semantics не меняется.

Избегать карточек >1500 LOC meaningful diff без промежуточного review.

---

# 44. Test strategy

Рефакторинг считается безопасным только при расширенной regression matrix.

---

# 45. Unit tests — Runtime

Проверить:

```text
start once
double start
close
double close
start failure
provider init failure
outbox init failure
task cancellation
lock acquisition failure
lock retained on unsafe shutdown
```

---

# 46. Runtime state tests

Для каждого state проверить допустимые операции:

| State | Search | Write | Start | Close |
|---|---:|---:|---:|---:|
| NEW | no | no | yes | optional |
| STARTING | no | no | no | controlled |
| READY | yes | yes | no/idempotent | yes |
| CLOSING | no | no | no | wait |
| CLOSED | no | no | no | idempotent |
| FAILED | no | no | controlled | cleanup |

---

# 47. Storage lock tests

Проверить:

1. runtime A захватывает root;
2. runtime B пытается открыть тот же root;
3. B получает deterministic ownership error;
4. после clean shutdown A — B может стартовать;
5. после failed shutdown lock не освобождается преждевременно.

---

# 48. Application service tests

Каждый service тестируется без MCP.

Например:

```python
result = await app.memory.remember(...)
```

Проверить:

- canonical write;
- receipt;
- outbox entry;
- result DTO.

---

# 49. Contract equivalence tests

Для одного use case:

```text
direct application call
MCP tool call
Hermes path
```

должны приводить к эквивалентному состоянию storage.

---

# 50. MCP regression tests

Существующие tool names:

```text
ping
remember
search
semantic_search
get_context
learn
graph_search
route
audit
metrics
set_belief
get_belief
resolve_conflict
reflect
```

должны остаться совместимыми.

Если дополнительные tools уже существуют в текущем релизе — их также включить.

---

# 51. Multi-profile isolation tests

Создать:

```text
profile_a
profile_b
```

В оба записать:

```text
subject = "project"
predicate = "database"
```

но разные object.

Проверить:

```text
keyword search
semantic search
hybrid route
get_context
graph
beliefs
audit
outbox
receipts
```

Ни один результат не должен пересекать profile boundary.

---

# 52. Outbox tests

Проверить отдельно для двух runtimes:

```text
A.write → A.outbox only
B.write → B.outbox only
```

Worker A не индексирует записи B.

---

# 53. Projection tests

Проверить:

- LanceDB path per profile;
- Qdrant collection/namespace per profile;
- graph snapshot/path per profile;
- projection unavailable behavior;
- rebuild behavior после runtime refactor.

---

# 54. Failure injection

Добавить controlled fakes:

```text
SQLite open failure
vector open failure
graph failure
outbox worker failure
embedding initialization failure
shutdown failure
```

Цель — доказать корректный resource ownership.

---

# 55. CLI compatibility tests

Проверить:

```bash
memory-server --help
memory-server serve
memory-server doctor
```

и существующие management commands.

---

# 56. Hermes compatibility tests

Существующие Hermes tests должны проходить.

Особое внимание:

- environment/profile resolution;
- plugin registration;
- native provider startup;
- cleanup;
- no duplicate runtime.

---

# 57. Observability during refactor

Добавить structured log events:

```text
cmms.runtime.starting
cmms.runtime.ready
cmms.runtime.closing
cmms.runtime.closed
cmms.runtime.failed
cmms.runtime.lock_acquired
cmms.runtime.lock_released
```

Не логировать secrets.

---

# 58. Runtime identity

Каждому runtime присвоить:

```text
runtime_id
profile_id
```

Для debug logging.

Например:

```text
runtime_id=rt_...
profile_id=coder
```

Это пригодится после появления daemon.

---

# 59. Metrics

Необязательно добавлять новые Prometheus metrics в первой карточке, но полезны:

```text
cmms_runtime_state
cmms_runtime_start_total
cmms_runtime_start_failures_total
cmms_runtime_shutdown_failures_total
cmms_runtime_outbox_tasks
```

---

# 60. Configuration cleanup

Рефакторинг должен сократить чтение global settings из случайных мест.

Целевой поток:

```text
CLI / Hermes / future daemon
        ↓
load settings
        ↓
resolve ProfileDescriptor
        ↓
resolve frozen StorageLayout
        ↓
RuntimeFactory
        ↓
CMMSRuntime
```

---

# 61. `get_settings()` policy

После refactor:

- можно оставить global cached settings для standalone CLI compatibility;
- domain/application services не должны сами повторно читать settings;
- multi-profile RuntimeManager должен получать explicit config.

---

# 62. Storage path policy

Нельзя позволять runtime component самостоятельно вычислять свой path независимо от `StorageLayout`.

Только:

```python
runtime.layout.sqlite_path
runtime.layout.vector_path
runtime.layout.graph_snapshot_path
```

---

# 63. Composition roots

Должны существовать отдельные composition roots.

## MCP standalone

```text
CLI
 ↓
resolve standalone config
 ↓
RuntimeFactory
 ↓
CMMSRuntime
 ↓
MemoryApplication
 ↓
MCPAdapter
```

## Hermes

```text
Hermes plugin
 ↓
resolve Hermes profile config
 ↓
RuntimeFactory / existing runtime integration
 ↓
MemoryApplication
 ↓
HermesAdapter
```

## Future daemon

```text
daemon
 ↓
ProfileRegistry
 ↓
RuntimeManager
 ↓
CMMSRuntime(s)
 ↓
MemoryApplication(s)
 ↓
HTTPAdapter
```

---

# 64. Запрет на скрытый second writer

После refactor нужно добавить архитектурный regression test:

> Любой transport обязан получать runtime через composition root/RuntimeManager; transport не имеет права самостоятельно открывать canonical storage.

Можно дополнительно проверять импортами/ruff rule или code review checklist.

---

# 65. Backward-compatible transition

Рекомендуемая последовательность merge:

1. добавить `CMMSRuntime`, пока старый server ещё работает;
2. написать parity tests;
3. переключить provider initialization на runtime;
4. выделить application services;
5. переключить MCP;
6. переключить Hermes;
7. удалить globals;
8. удалить dead compatibility code.

---

# 66. Что нельзя делать в одном PR

Не совмещать:

```text
runtime extraction
+
new HTTP Gateway
+
schema migration
+
retrieval algorithm changes
```

Иначе regression attribution становится практически невозможной.

---

# 67. Требования к PR review

Каждая карточка проходит минимум:

```text
implementation review
architecture review
regression review
```

Для критических областей:

- storage lock;
- profile isolation;
- outbox;
- shutdown;

нужен отдельный targeted review.

---

# 68. Рекомендуемые review questions

Для каждого PR:

1. Может ли код открыть storage в обход runtime?
2. Может ли появиться второй writer?
3. Есть ли скрытое чтение env?
4. Есть ли shared mutable global state?
5. Может ли profile A получить provider B?
6. Изменились ли memory semantics?
7. Изменился ли MCP contract?
8. Может ли shutdown освободить lock слишком рано?
9. Может ли background task пережить runtime?
10. Есть ли parity test?

---

# 69. Acceptance criteria — Phase 0

Рефакторинг завершён, если:

- [ ] `CMMSRuntime` владеет runtime resources.
- [ ] `RuntimeStorageLock` не принадлежит MCP transport.
- [ ] Providers не хранятся как transport-level globals.
- [ ] OutboxWorker принадлежит runtime.
- [ ] Background tasks принадлежат runtime.
- [ ] Application services вызываются без MCP.
- [ ] MCP transport стал thin adapter.
- [ ] Hermes использует общий application/runtime layer.
- [ ] RuntimeManager поддерживает минимум два isolated profiles.
- [ ] StorageLayout frozen per runtime.
- [ ] Cross-profile tests проходят.
- [ ] Existing MCP contracts проходят.
- [ ] Existing Hermes tests проходят.
- [ ] No data migration required.
- [ ] Retrieval output parity подтверждён.
- [ ] Lifecycle behavior parity подтверждён.
- [ ] Outbox behavior parity подтверждён.
- [ ] Shutdown lock safety подтверждён.
- [ ] `server.py` больше не является memory engine.

---

# 70. Definition of Done

Условный `v0.13` можно считать завершённым, когда выполнен сценарий:

```python
descriptor = ProfileDescriptor(
    profile_id="automotive",
    data_root=Path("..."),
    settings_overrides={},
)

runtime = await RuntimeFactory(...).create(descriptor)

async with runtime:
    app = MemoryApplication(runtime)

    await app.memory.remember(...)
    results = await app.retrieval.search(...)
    context = await app.retrieval.get_context(...)
```

и этот код:

- не импортирует MCP;
- не импортирует Hermes;
- не зависит от HTTP;
- использует тот же memory semantics, что MCP.

---

# 71. Future readiness test

После Phase 0 создание HTTP Gateway должно выглядеть примерно так:

```python
@app.post("/api/v1/profiles/{profile}/search")
async def search(profile: str, request: SearchRequest):
    runtime = await runtime_manager.get(profile)
    app_service = MemoryApplication(runtime)

    return await app_service.retrieval.search(
        request.to_query()
    )
```

Если HTTP implementation требует:

- прямого открытия SQLite;
- прямого создания Qdrant/LanceDB;
- прямого создания graph;
- отдельного outbox worker;
- копирования MCP handlers;

значит Phase 0 выполнена неправильно.

---

# 72. Ожидаемый объём изменений

Оценка:

```text
production code:
  ~1 500–2 500 LOC meaningful diff

tests:
  ~1 000–2 000 LOC

docs:
  ~300–700 LOC
```

Количество затронутых файлов:

```text
15–25
```

Новых основных модулей:

```text
6–10
```

---

# 73. Оценка риска

## Высокий риск

- storage lock ownership;
- runtime shutdown;
- multi-profile resource separation;
- outbox ownership.

## Средний риск

- provider factory;
- transport-independent error mapping;
- Hermes integration.

## Низкий риск

- file/package movement;
- MCP wrapper simplification;
- DTO extraction.

---

# 74. Рекомендуемый порядок review

```text
1. Runtime ownership
2. Lock/shutdown safety
3. Provider factory
4. Application services
5. MCP parity
6. Profile isolation
7. Hermes parity
8. cleanup
```

---

# 75. Rollback strategy

Так как storage schema не меняется:

rollback должен быть code-level.

Если v0.13 имеет критическую regression:

```text
checkout previous release
restart old CMMS
use same data
```

Это одна из причин не совмещать refactor со schema migration.

---

# 76. Migration strategy

В идеале:

```text
Data migration: none
Config migration: none
User action: none
```

Если появляются новые optional runtime config fields — defaults должны воспроизводить текущее standalone behavior.

---

# 77. Changelog wording

Пример:

```text
### Changed
- Refactored CMMS runtime ownership into transport-independent CMMSRuntime.
- MCP stdio and Hermes adapters now use shared application services.
- Added internal RuntimeManager abstraction for isolated profile runtimes.
- No storage format or public MCP contract changes intended.
```

---

# 78. Architecture Decision Record

Стоит добавить ADR:

## ADR — Transport-independent runtime ownership

**Decision**

All transports MUST access memory through `CMMSRuntime` + application services.

**Rationale**

Prevent duplicate writers, storage divergence, and transport-specific semantics.

**Consequences**

Positive:

- HTTP becomes straightforward;
- multi-profile daemon becomes safe;
- easier testing;
- easier resource ownership reasoning.

Negative:

- additional abstraction layer;
- temporary refactor complexity.

---

# 79. Architecture guardrails

После v0.13 зафиксировать правила:

```text
1. Transport code MUST NOT instantiate storage providers.
2. Transport code MUST NOT acquire storage locks.
3. Transport code MUST NOT start outbox workers.
4. Application services MUST NOT read environment variables.
5. Runtime configuration MUST be explicit and immutable after start.
6. One profile root MUST map to one writer runtime.
7. Memory semantics MUST live outside transports.
```

---

# 80. Проверка готовности к следующей фазе

Перед началом HTTP Gateway ответить `YES` на всё:

- Есть ли явный `CMMSRuntime`?
- Можно ли поднять CMMS без MCP?
- Может ли application service выполнить remember/search напрямую?
- Может ли один процесс иметь два isolated runtimes?
- Невозможно ли случайно открыть один profile root дважды?
- Владеет ли runtime outbox worker?
- Полностью ли transport-independent lifecycle?
- Совпадают ли MCP results до/после?
- Работает ли Hermes через тот же application layer?
- Нет ли business logic в MCP adapter?

Если хотя бы на один вопрос ответ `NO`, HTTP Gateway лучше ещё не начинать.

---

# 81. Итоговая архитектура после Phase 0

```text
                     ┌──────────────────────┐
                     │     CLI / Config     │
                     └──────────┬───────────┘
                                │
                     ┌──────────▼───────────┐
                     │ RuntimeFactory       │
                     └──────────┬───────────┘
                                │
                     ┌──────────▼───────────┐
                     │ CMMSRuntime          │
                     │                      │
                     │ StorageLayout        │
                     │ StorageLock          │
                     │ Providers            │
                     │ Routers              │
                     │ Outbox               │
                     └──────────┬───────────┘
                                │
                     ┌──────────▼───────────┐
                     │ MemoryApplication    │
                     │                      │
                     │ Memory              │
                     │ Retrieval           │
                     │ Belief              │
                     │ Lifecycle           │
                     │ Relation            │
                     │ Graph               │
                     │ Audit               │
                     └──────────┬───────────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                 │
        ┌─────▼─────┐     ┌─────▼──────┐    ┌────▼─────┐
        │ MCP stdio │     │ Hermes     │    │ Future   │
        │ adapter   │     │ adapter    │    │ HTTP     │
        └───────────┘     └────────────┘    └──────────┘
```

---

# 82. Основной принцип релиза

> **v0.13 должен изменить архитектуру, но не поведение памяти.**

Если после refactor пользователь замечает новые semantics памяти — refactor слишком широкий.

Если после refactor HTTP Gateway можно добавить без прямого доступа к providers/storage — refactor выполнил свою задачу.

---

# 83. Практический порядок реализации

Рекомендуемый порядок:

```text
Week / iteration 1
  CARD 1 — Runtime ownership
  CARD 2 — RuntimeFactory

Week / iteration 2
  CARD 3 — Application services
  CARD 4 — MCP adapter

Week / iteration 3
  CARD 5 — RuntimeManager/profile isolation
  CARD 6 — Hermes + hardening
```

При агентной разработке это лучше вести не как один giant task, а как последовательность небольших карточек с independent review.

---

# 84. Итог

Этот refactor является **обязательной подготовительной фазой** перед HTTP Gateway и Neural Studio.

Он решает фундаментальную проблему:

```text
сейчас:
transport ≈ runtime owner

должно быть:
transport → application → runtime
```

После завершения Phase 0 дальнейшее развитие становится значительно безопаснее:

```text
HTTP Gateway
Neural Studio
remote MCP
multi-profile daemon
```

смогут добавляться как новые adapters/control-plane components, не затрагивая core memory semantics.
