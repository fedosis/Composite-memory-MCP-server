# Architecture Decision Records

# Composite Memory MCP Server

Version: 0.1 Draft

------------------------------------------------------------------------

# ADR-001: Independent MCP Memory Server instead of embedded agent memory

## Status

Accepted

## Context

Agent-specific memory systems create dependency on a particular agent
implementation, limit interoperability, and make long-term evolution
difficult.

Options:

1.  Implement memory inside Hermes.
2.  Implement an independent MCP memory service.

## Decision

Create an independent Composite Memory MCP Server.

Architecture:

    Hermes
    OpenClaw
    Claude Code

          |
          MCP

          |

    Memory Server

## Reasons

-   memory becomes an independent resource;
-   multiple agents can share the same knowledge base;
-   internal storage can evolve without changing clients;
-   enables a unified personal knowledge graph.

## Consequences

Advantages: - portability; - extensibility; - agent independence.

Disadvantages: - additional service; - more complex API design.

------------------------------------------------------------------------

# ADR-002: Composite Memory instead of single Vector RAG

## Status

Accepted

## Context

A vector database answers mainly:

"What is semantically similar?"

It does not efficiently represent: - dependencies; - causality; -
decisions; - procedures.

## Decision

Use specialized memory backends:

    Facts       SQLite/PostgreSQL

    Semantic    Qdrant

    Graph       Neo4j/Graphiti

    Skills      Git

## Reasons

Different memory types require different storage models.

Examples:

"Server IP?" → SQL

"Previous Caddy problems?" → Vector search

"Which services depend on proxy network?" → Graph

"How to deploy service?" → Skill repository

------------------------------------------------------------------------

# ADR-003: Python as primary implementation language

## Status

Accepted

## Decision

Use Python 3.12+.

## Reasons

Python provides:

-   MCP SDK;
-   Pydantic;
-   strongest AI ecosystem;
-   LLM integration;
-   embedding libraries.

The project is mainly orchestration, not high-performance computing.

## Consequences

Advantages: - rapid development; - easy AI integration.

Disadvantage: - lower raw performance.

------------------------------------------------------------------------

# ADR-004: MCP as external contract

## Status

Accepted

## Decision

All agents communicate with memory only through MCP.

No direct access to internal databases.

## Reasons

Allows:

-   backend replacement;
-   multiple agents;
-   stable interface.

------------------------------------------------------------------------

# ADR-005: Hybrid memory routing

## Status

Accepted

## Context

Pure LLM routing: - expensive; - slow; - unpredictable.

Pure rules: - do not scale.

## Decision

Use layered routing:

    Query

     |
    Rules

     |
    Embedding Router

     |
    Graph Lookup

     |
    LLM fallback

## Responsibilities

Rules: exact deterministic routing.

Embeddings: semantic similarity.

Graph: relations and dependencies.

LLM: complex reasoning.

------------------------------------------------------------------------

# ADR-006: Entity / Fact / Decision / Skill memory model

## Status

Accepted

## Decision

Use four primary knowledge entities.

## Entity

Represents an object.

Examples: - server; - software; - vehicle; - project.

## Fact

Verified statement.

Structure:

    Subject
    Predicate
    Object

Example:

    Docker -> runs_on -> OMV8

## Decision

Represents a chosen solution and its reasoning.

Example:

    Choice:
    Use Caddy

    Reason:
    Better Docker integration

## Skill

Represents procedural knowledge.

Contains: - purpose; - steps; - constraints; - validation.

------------------------------------------------------------------------

# ADR-007: Separate Learn and Store operations

## Status

Accepted

## Context

Direct agent writes can corrupt memory.

## Decision

Use:

    learn()

    Extractor

    Validation

    store()

## Reasons

Learn: understands and classifies knowledge.

Store: persists validated objects.

------------------------------------------------------------------------

# ADR-008: Provenance and Memory Receipt are mandatory

## Status

Accepted

## Decision

Every memory object must contain:

-   source;
-   creator;
-   timestamp;
-   confidence;
-   verification status;
-   history.

## Reasons

Required for:

-   trust;
-   conflict resolution;
-   auditing.

------------------------------------------------------------------------

# ADR-009: Git as procedural memory backend

## Status

Accepted

## Context

Skills require:

-   versioning;
-   history;
-   rollback.

## Decision

Store skills in Git.

Example:

    skills/

    docker-deployment/
        v1
        v2
        tests

## Reasons

Git naturally provides:

-   version control;
-   diff;
-   rollback.

------------------------------------------------------------------------

# ADR-010: Incremental implementation strategy

## Status

Accepted

## v0.1a

Implement:

-   MCP API;
-   Pydantic models;
-   SQLite backend;
-   search;
-   remember;
-   get_context.

## v0.2

Add:

-   embeddings;
-   Qdrant;
-   semantic router.

## v0.3

Add:

-   learn();
-   LLM extractors.

## v0.4

Add:

-   graph database;
-   entity relations.

## v0.5+

Add:

-   confidence engine;
-   validation;
-   decay;
-   memory auditor.

------------------------------------------------------------------------

# ADR-011: Belief Model — propositional knowledge with lifecycle

## Status

Accepted (v0.7)

## Context

The original memory model (facts, decisions, skills) captures structured
knowledge but lacks a way to represent abstract, propositional knowledge
with quantifiable confidence — "what the agent should believe" rather
than "what the agent observed."

Beliefs differ from facts:
- Facts are structured (subject-predicate-object) and verifiable.
- Beliefs are unstructured propositions that may be true with some
  confidence, derived from multiple sources of evidence.

## Decision

Introduce a Belief entity as a first-class memory type alongside facts,
decisions, and skills.

### Data Model

```
Belief
├── id (UUID)
├── proposition (str)         — Free-text belief statement
├── confidence (float 0-1)    — How sure the agent is
├── source (str)              — Origin identifier ("learn", "system", etc.)
├── creator (str)             — Who created it
├── tags (list[str])          — Categorical labels
├── source_ids (list[str])    — Evidence source references
├── lifecycle_state (str)     — active | superseded | contradicted | discarded | stale | archived | forgotten
├── version (int)             — Monotonic version for supersede chain
├── reinforced_at (datetime)  — Last reinforcement timestamp
└── created_at / updated_at   — Timestamps
```

### Lifecycle Integration

Beliefs share the same lifecycle engine as facts (DecayEngine, Validator)
with belief-specific transitions:

| From | To | Condition |
|------|----|-----------|
| active | superseded | Replaced by a higher-confidence belief |
| active | contradicted | Conflicting belief with similar confidence |
| active | stale | TTL exceeded (180 days for beliefs) |
| superseded | stale | TTL exceeded |
| contradicted | stale | TTL exceeded |
| superseded | discarded | Manual cleanup |
| contradicted | discarded | Manual cleanup |
| discarded | archived | TTL expired |
| stale | archived | TTL expired |
| archived | forgotten | TTL expired |

### Evidence / Outbox

Beliefs use the same `Evidence` model and outbox pattern as facts:
- Evidence entries link beliefs to their source facts (source_type, source_id, weight).
- Outbox entries enable async indexing into vector and graph stores.
- `create_in_transaction()` atomically writes belief + evidence + receipt + outbox.

## Consequences

Advantages:
- Agents can express abstract, uncertain knowledge alongside structured facts.
- Evidence provenance enables auditability and conflict resolution.
- Shared lifecycle infrastructure reduces maintenance burden.
- Outbox pattern ensures reliable async indexing.

Disadvantages:
- Additional storage for belief tables and evidence joins.
- Conflict detection is heuristic (keyword-based, not LLM) in v0.7;
  full semantic contradiction detection deferred to v0.8+.

------------------------------------------------------------------------

# ADR-012: Reflection — belief store analysis tools

## Status

Accepted (v0.7)

## Context

Agents accumulate beliefs over time, but without introspection tools
they cannot answer questions like:
- "What do I currently believe?"
- "Which beliefs conflict?"
- "Which beliefs are decaying?"
- "Is my evidence sufficient?"

## Decision

Implement the `reflect()` MCP tool with 6 analysis modes, backed by the
`ReflectEngine` class.

### Reflect Modes

| Mode | Function | Output |
|------|----------|--------|
| `overview` | `overview()` | Total count, lifecycle distribution, confidence buckets, conflict stats, decaying-next-7d estimate, oldest/newest age |
| `contradictions` | `contradictions()` | List of contradictory belief pairs with overlap scores |
| `decay` | `decay_analysis()` | Stale-now/7d counts, archived-7d, forgotten-7d, by-tag stale breakdown |
| `topics` | `topics()` | Tag clusters with count, avg confidence, stale count |
| `evidence_audit` | `evidence_audit()` | With/without evidence counts, avg per belief, by source type, zero-weight entries |
| `confidence` | `confidence_histogram()` | Sorted belief list with evidence counts, 5-bucket histogram |

### Contradiction Detection

Three detection methods (heuristic, v0.7):

1. **Keyword match** — ≥2 overlapping tokens + opposite sentiment
   (stopwords filtered, sentiment pairs like better/worse, like/dislike).
2. **Confidence-weighted** — detection_score ≥ 0.3 AND confidence diff > 0.4.
3. **Source overlap** — ≥2 shared evidence source_ids + opposite sentiment.

### Design Decisions

- All modes load beliefs from the provider via `search_beliefs()` — no
  separate aggregation tables (performance acceptable for <10K beliefs).
- Contradiction detection uses O(n²) pairwise scan with a guard at 447
  beliefs (~100K pairs) — a warning is logged beyond this threshold.
- Histogram uses 5 fixed buckets (0.0-0.3, 0.3-0.5, 0.5-0.7, 0.7-0.9, 0.9-1.0).
- Decay analysis uses belief TTL of 180 days, stale threshold at 70% (126 days).
- All modes accept optional `topic`, `min_confidence`, and `limit` filters.

## Consequences

Advantages:
- Agents gain full introspection into their belief state.
- Contradiction detection enables proactive conflict resolution.
- Decay analysis provides early warning for knowledge that needs refreshing.
- Evidence audit reveals knowledge-quality gaps.
- All modes return empty-store gracefully (no crashes on fresh databases).

Disadvantages:
- O(n²) contradiction scan does not scale to tens of thousands of beliefs.
- Keyword-based contradiction detection has false positives/negatives.
- Decay analysis is snapshot-based, not event-driven — requires periodic
  polling via reflect().

------------------------------------------------------------------------

# ADR-013: Hermes MemoryProvider Integration — Dual Access Path

## Status

Accepted (v0.8)

## Context

CMMS was designed as an MCP-only memory service per ADR-004 (all agents communicate with memory only through MCP). This works for external agents (Claude Code, OpenClaw) but imposes severe limitations within Hermes:

- **No auto-recall**: Hermes calls `prefetch()` before every turn to inject context; MCP-only servers cannot participate
- **No auto-retain**: Hermes calls `sync_turn()` after every turn; MCP-only servers miss ongoing conversations
- **No session hooks**: CMMS cannot flush writer queues on session end/switch
- **Tool isolation**: CMMS tools appear under `mcp_*` namespace

Hermes already has a solved pattern: the native `MemoryProvider` ABC. Three providers (builtin, Honcho, Hindsight) implement it.

## Decision

CMMS supports **two access paths**:

### Path 1: MCP (for external agents) — ADR-004 preserved
- MCP transport at `src/memory_server/server.py`
- All 14 tools available over stdio/HTTP MCP
- JSON Schema 2020-12 contract validation (`contracts/`)

### Path 2: Hermes MemoryProvider (v0.8)
- Native `MemoryProvider` plugin at `plugins/hermes/provider.py`
- Same 14 tools as first-class Hermes tools (no `mcp_` prefix)
- Lifecycle: `initialize`, `prefetch`, `queue_prefetch`, `sync_turn`, `on_session_end`, `on_session_switch`, `shutdown`
- Background writer queue for non-blocking writes
- Single storage engine shared between both paths

### Architecture

```
External Agents              Hermes Agent
     |                            |
     MCP                     MemoryProvider
     |                            |
     v                            v
server.py                  plugins/hermes/provider.py
     |                            |
     +------ CMMS Service Layer --+
                    |
           Storage (SQLite/Qdrant/Graph)
```

### Contract Mapping

| Hermes Method | CMMS Service | Description |
|--------------|--------------|-------------|
| `initialize(session_id, **kwargs)` | Connect + start writer queue | kwargs: `hermes_home`, `platform`, `agent_context` |
| `system_prompt_block()` | Static provider info | Memory usage tips |
| `prefetch(query)` | `get_context()` | Fast cached recall |
| `queue_prefetch(query)` | Queue next-turn context load | Background |
| `sync_turn(user, asst, *, messages)` | `learn()` via writer queue | Async batch |
| `get_tool_schemas()` | All 14 CMMS tools | No `mcp_` prefix |
| `handle_tool_call(name, args, **kwargs)` | Route to service layer | In-process |
| `on_session_end(messages)` | Flush writer queue | Cleanup |
| `on_session_switch(new_id, *, ...)` | Flush + update cache | Rotation |
| `shutdown()` | Flush, stop, close | Clean exit |

### Security Boundary
- MemoryProvider is **in-process** (same process, same permissions)
- No new network-accessible endpoints
- MCP contract unchanged for external agents
- Profile isolation via `hermes_home` in initialize kwargs

## Reasons
- Eliminates gap between "memory provider" and "MCP memory server" within Hermes
- No data duplication — single storage engine
- External agents continue using MCP unchanged
- Pattern proven by Hindsight provider in production

## Consequences

Advantages:
- Hermes gains auto-recall, auto-retain, session hooks
- All 14 tools become first-class Hermes tools
- No MCP transport overhead for Hermes-to-CMMS calls

Disadvantages:
- Two code paths to maintain (MCP + MemoryProvider)
- Risk of divergence between paths
- Sync ABC methods need background thread bridge for async CMMS operations

------------------------------------------------------------------------

# ADR-014: Ternary Relation Model — contradiction|entailment|neutral

## Status

Accepted (v0.9)

## Context

ADR-011 and ADR-012 established a belief store with keyword-based binary
contradiction detection using three heuristics (keyword match,
confidence-weighted, source overlap). This has two major flaws:

1. **False positives**: "Docker is better than Podman" vs "Podman is worse
   than Docker" is detected as a contradiction, but the second proposition
   is logically *entailed* by the first (they express the same sentiment).
2. **Binary only**: The system cannot distinguish between logical
   contradiction (A = ¬A), entailment (A ⇒ B), and unrelated propositions.
   All non-contradictions are treated identically.

Additionally, there is no **context gate** — beliefs from different
conversational contexts are compared without accounting for context
differences, producing spurious contradictions.

Curiosity worker findings:
- CUR-CMMS-LLM-CONFLICT-001: current binary detection produces false positives
- CUR-CMMS-RELATION-001: ternary relation model needed with same_context gate

### Prior Art

The Natural Language Inference (NLI) literature defines the standard
three-way classification used by all major NLI datasets (SNLI, MNLI,
ANLI): contradiction, entailment, and neutral. This card adopts the same
taxonomy for belief relations.

## Decision

Replace the binary `detect_contradictions()` function with a ternary
`RelationClassifier` that outputs one of:

| Relation | Meaning | Example |
|----------|---------|---------|
| `contradiction` | Two beliefs cannot both be true | "Docker is better than Podman" vs "Docker is worse than Podman" |
| `entailment` | One belief logically implies the other | "Docker is better than Podman" vs "Podman is worse than Docker" |
| `neutral` | No logical relation or different context | "Docker is great" vs "Caddy is a web server" |

### same_context Gate

The classifier accepts `context_a` and `context_b` parameters. When
contexts differ (different tags, source domains, or explicit context
strings), the classifier applies the **same_context gate**:

- If `context_a` and `context_b` are explicitly provided and differ, the
  gate is `same_context: false`.
  - With `strict_same_context=true` (default): relation → `neutral`
    regardless of semantic content.
  - With `strict_same_context=false`: relation is computed normally but
    `confidence` is reduced by a context-divergence penalty.
- If contexts are not provided or match, `same_context: true` and
  classification proceeds normally.

### Classification Algorithm (v0.9 — heuristic)

No LLM is used yet (deferred to v1.0). The heuristic algorithm:

1. **Tokenize** both propositions (lowercase, remove stopwords).
2. **Compute overlap** — Jaccard similarity of keyword tokens.
3. **Detect sentiment direction** — check OPPOSITE_SENTIMENT pairs
   (better/worse, like/dislike, good/bad, etc.).
4. **Detect shared sentiment** — check if both use the same direction
   words (both positive or both negative about the same topic).
5. **Classify**:
   - If overlap ≥ 2 AND opposite sentiment → `contradiction`
   - If overlap ≥ 2 AND same sentiment (both positive words OR both
     negative words on the same topic) → `entailment`
   - If overlap ≥ 1 AND neutral words → borderline (confidence < 0.5)
   - Else → `neutral`
6. **Apply same_context gate** — adjust relation/confidence based on
   context match.

### Integration

- `ReflectEngine.contradictions()` is preserved for backward compatibility
  but internally delegates to `RelationClassifier` filtering for
  `contradiction` relations.
- New `ReflectEngine.relations()` mode returns the full ternary output.
- `detect_contradictions()` is replaced by `RelationClassifier.classify_pair()`
  and `RelationClassifier.find_relations()`.
- The `detect_contradictions` module is maintained as a thin wrapper
  until v1.0 removal.

### Schema Changes

- `mode=relations` added to reflect input enum.
- Output schema includes `relation`, `confidence`, and `same_context` fields.
- Backward-compatible: `mode=contradictions` continues to work.

## Reasons

- Ternary classification matches the NLI standard and enables richer
  belief analysis.
- Entailment detection fixes the false positive Docker/Podman case.
- same_context gate prevents spurious cross-context contradictions.
- Heuristic approach is fast (no LLM calls), suitable for v0.9.
- Backward compatibility avoids breaking existing consumers.

## Consequences

Advantages:
- Correct classification of entailment (fixes false positives).
- Context isolation prevents cross-context noise.
- Richer introspection: agents can ask "which beliefs support which".
- Same API contract — existing callers unchanged.

Disadvantages:
- Heuristic entailment detection is less accurate than LLM-based
  (deferred to v1.0).
- Additional computation for same_context comparison.
- Tests need rewriting for the new classification.

------------------------------------------------------------------------

# General architectural principles

Composite Memory MCP Server must be:

-   independent from agents;
-   independent from specific LLMs;
-   extensible;
-   auditable;
-   capable of knowledge evolution.
