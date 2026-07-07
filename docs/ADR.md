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

# General architectural principles

Composite Memory MCP Server must be:

-   independent from agents;
-   independent from specific LLMs;
-   extensible;
-   auditable;
-   capable of knowledge evolution.
