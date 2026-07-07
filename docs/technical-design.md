# Composite Memory MCP Server

## Техническое описание и план реализации

Версия документа: 0.1 Draft

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
    Facts   Vector    Graph    Skills
    SQL     Qdrant    DB       Git

## 3. Технологический стек

Основной язык: - Python 3.12+

Причины: - MCP SDK; - Pydantic; - AI/ML ecosystem; - LLM API
integration.

Хранилища: - SQLite/PostgreSQL --- факты и метаданные; - Qdrant ---
embeddings и semantic search; - Neo4j/Graphiti --- граф знаний; - Git
--- версии skills.

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

Рекомендуемые состояния:

Captured -\> Candidate -\> Validated -\> Trusted -\> Deprecated -\>
Archived

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

-   Qdrant
-   embeddings
-   semantic router

### v0.3

-   LLM extractors
-   learn()

### v0.4

-   graph database
-   entity relations

### v0.5+

-   confidence engine
-   validation
-   decay
-   memory auditor
