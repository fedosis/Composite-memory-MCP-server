# Планы развития CMMS

Раздел содержит планы развития Composite Memory MCP Server (CMMS), которые
описывают будущие изменения архитектуры и продукта. Документы в этом разделе —
**планы и спецификации, а не описание текущего поведения**.

Текущий код на момент добавления раздела: `bdebd4e` (release 0.12.0).
Ни один из планов ниже ещё не реализован.

## Статусы

| Статус | Значение |
|---|---|
| `proposal` | черновик/предложение, не утверждено к реализации |
| `approved` | утверждено, работа не начата |
| `in progress` | реализуется |
| `done` | реализовано и влито |
| `deferred` | отложено |

## Состав раздела

| План | Целевой релиз | Статус | Зависит от |
|---|---|---|---|
| [CMMS Runtime Architecture Refactor](CMMS_Runtime_Architecture_Refactor_Plan.md) | `v0.13.x` | `proposal` | — |
| [HTTP Gateway и Neural Studio](CMMS_HTTP_Gateway_Neural_Studio_Spec.md) | после `v0.13.x` | `proposal` | Runtime Architecture Refactor |

## Порядок реализации

```text
CMMS Runtime Architecture Refactor (v0.13, Phase 0)
        ↓
HTTP Gateway                       (Phase 1–2, 4, 6)
        ↓
Neural Studio read-only            (Phase 3)
        ↓
Controlled mutations / maintenance (Phase 4–5)
        ↓
Remote deployment hardening        (Phase 6)
        ↓
Optional MCP Streamable HTTP       (Phase 7)
```

Рефакторинг runtime является **обязательной подготовительной фазой** перед HTTP
Gateway: спека Gateway прямо требует, чтобы MCP и HTTP были равноправными
transport-адаптерами над одним application-service layer, а не двумя
независимыми writer runtime.

## Краткое содержание планов

### 1. CMMS Runtime Architecture Refactor (`v0.13.x`)

Внутренний рефакторинг архитектуры без изменения наблюдаемого поведения памяти.
Разделяет код на слои `transport → application services → runtime → storage
providers`, убирает из `server.py` роль memory engine и module-level globals,
вводит `CMMSRuntime`, `RuntimeFactory`, `MemoryApplication` и `RuntimeManager`.

- 6 основных карточек: CARD 1 runtime/lifecycle, CARD 2 factory, CARD 3
  application services, CARD 4 MCP adapter, CARD 5 RuntimeManager/profile
  isolation, CARD 6 Hermes adapter + hardening.
- Инварианты: SQLite canonical, vector/graph как projections, outbox вместе с
  canonical write, один writer на profile root, profile isolation, transport
  equivalence.
- Non-goals: сам HTTP Gateway, Neural Studio, auth/RBAC, SSE/WebSocket, remote
  MCP, смена vector/graph backend, миграция данных.
- Критерий успеха: `v0.13` меняет архитектуру, но не семантику памяти; HTTP
  Gateway добавляется без прямого доступа к providers/storage.

### 2. HTTP Gateway и Neural Studio

Два связанных расширения поверх refactored runtime:

- **HTTP Gateway** — сетевой API-слой: `memory-server daemon`,
  routing `/api/v1/profiles/{profile_id}/...`, profile registry, bearer-токены и
  RBAC (viewer/agent/operator/admin), optimistic concurrency
  (`expected_version`), idempotency keys, audit access log, стабильные error
  codes, retrieval trace, SSE-события, лимиты запросов, TLS через reverse proxy.
- **Neural Studio** — операторская консоль и memory debugger: Profile Overview,
  Memory Explorer, provenance/lifecycle timeline, Graph Explorer с явным
  разделением canonical relations и projection, Conflict Center, Retrieval Lab
  (FTS/vector/graph contributions + RankMerger), Context Preview, Admission
  Inspector, Outbox/Index health, Audit и Benchmark UI.

Ключевое требование: **HTTP Gateway exposes CMMS; Neural Studio explains CMMS;
neither reimplements CMMS.** Семантика памяти остаётся в существующих сервисах
CMMS; control plane (profile registry, токены, RBAC, HTTP audit, jobs) хранится
отдельно от memory plane.

## Отношение к другим документам

- [PLAN.md](../../PLAN.md) — фактическая история исполненных фаз v0.1–v0.6.
- [docs/architecture.md](../architecture.md) — текущая архитектура.
- [docs/ADR.md](../ADR.md) — принятые архитектурные решения; после реализации
  планов сюда стоит добавить ADR «Transport-independent runtime ownership»
  (см. раздел 78 плана рефакторинга).
- [specs/](../../specs/) — спецификации отдельных карточек реализации.
