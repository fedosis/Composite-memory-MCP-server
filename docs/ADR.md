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

# General architectural principles

Composite Memory MCP Server must be:

-   independent from agents;
-   independent from specific LLMs;
-   extensible;
-   auditable;
-   capable of knowledge evolution.
