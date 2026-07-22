# Composite Memory MCP Server

## Техническое описание и план реализации

> **Статус:** Исторический/справочный документ. Основные архитектурные решения актуальны для v0.11; технологический стек и roadmap обновлены в соответствии с текущей реализацией (LanceDB по умолчанию, SimpleGraph).

Версия документа: 0.1 Draft

> **Статус v0.11.0b1:** этот документ сохраняет исходный дизайн и roadmap, но
> ниже явно отмечены текущие границы runtime. GitHub prerelease опубликован;
> PyPI, официальный MCP Registry, Smithery и Glama не опубликованы. Базовый
> runtime использует SQLite/FTS5, LanceDB по умолчанию или Qdrant опционально
> для semantic search, и in-memory SimpleGraph для графа. Neo4j/Graphiti —
> будущая линия развития, не подключенная в v0.11. Hermes — опциональная
> интеграция `[hermes]`, см. `docs/INTEGRATION.md`.

## 1. Назначение

Composite Memory MCP Server (CMMS) --- независимый MCP-сервер
долговременной памяти для AI-агентов.

Цели: - отделить память от конкретного агента; - объединить фактическую,
семантическую, процедурную и графовую память; - обеспечить накопление,
проверку и развитие знаний.

Поддерживаемые клиенты: - Hermes Agent - OpenClaw - Claude Code -
MCP-совместимые агенты

## 2. Архитектура

    AI Agents
        |
        MCP
        |
    Composite Memory Server
        |
    Memory Orchestrator
        |
    +---------+---------+---------+
    |         |         |         |
    Facts   Vector              Graph              Skills
    SQL/FTS5 LanceDB/Qdrant     SimpleGraph memory Git

## 3. Технологический стек

Основной язык: - Python 3.11+

Причины: - MCP SDK; - Pydantic; - AI/ML ecosystem; - LLM API
integration.

Хранилища: - SQLite/FTS5 --- факты, метаданные и базовый keyword search; -
LanceDB/Qdrant --- опциональные embeddings и semantic search (LanceDB по
умолчанию в server mode, Qdrant через `MEMORY_VECTOR_BACKEND=qdrant`); -
SimpleGraph сейчас как in-memory graph layer, Neo4j/Graphiti как дальнейшая
линия развития графа, не подключенная в runtime v0.11; - Git --- версии skills.

## 4. Внешний интерфейс MCP

### memory.get_context()

Назначение: Получение рабочего контекста перед рассуждением агента.

Вход:

``` json
{
 "task":"deploy service",
 "agent":"hermes"
}
```

Выход:

``` json
{
 "facts":[],
 "decisions":[],
 "skills":[],
 "warnings":[]
}
```

### memory.search()

Активный поиск по памяти.

Поддерживает: - keyword search; - semantic search; - graph lookup.

### memory.remember()

Явное сохранение факта или предпочтения пользователя.

### memory.learn()

Извлечение знаний из опыта.

Pipeline:

Session -\> Extractor -\> Validation -\> Storage

### memory.inspect()

Диагностика состояния памяти.

## 5. Memory Receipt

Каждый объект памяти должен содержать:

-   id;
-   type;
-   source;
-   created_by;
-   timestamp;
-   confidence;
-   verification state;
-   history.

## 6. Модель данных

### Entity

Объект мира.

Примеры: - сервер; - сервис; - автомобиль; - программа.

### Fact

Проверяемое утверждение.

Модель: Subject + Predicate + Object

Пример: Docker -\> runs_on -\> OMV8

### Decision

Архитектурное решение.

Содержит: - выбор; - альтернативы; - причины; - источник.

### Skill

Процедурное знание.

Содержит: - цель; - шаги; - ограничения; - тесты; - версию.

## 7. Lifecycle памяти

Текущие состояния v0.11:

Candidate -\> Validated -\> Active -\> Stale -\> Archived -\> Forgotten

Исторические названия `Trusted` и `Deprecated` соответствуют текущим
`Active` и `Stale`.

## 8. Conflict resolution

При конфликте сравниваются:

-   источник;
-   confidence;
-   дата;
-   проверка.

Старые данные архивируются, а не удаляются.

## 9. Roadmap

### v0.1a

-   MCP API
-   Pydantic models
-   SQLite backend
-   get_context
-   search
-   remember

### v0.2

-   LanceDB (по умолчанию) + Qdrant (опционально)
-   embeddings
-   semantic router

### v0.3

-   LLM extractors
-   learn()

### v0.4

-   SimpleGraph (in-memory) — граф знаний
-   entity relations

### v0.5+

-   confidence engine
-   validation
-   decay
-   memory auditor
