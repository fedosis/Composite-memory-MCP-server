# DECISIONS.md — Project Journal: Composite Memory MCP Server

Date format: YYYY-MM-DD
Each entry: decision, alternatives considered, rationale, outcome.

---

## 2026-07-07 — Project inception

### Decision 001: Initial project structure

**Context:** Start of v0.1a. Need to decide where the source lives and how to organise.

**Alternatives:**
1. Monorepo inside ~/memory-server/ — chosen
2. Workspace inside ~/.hermes/workspace/ — rejected, project must be agent-independent per ADR-001
3. Separate directory per phase — rejected, overkill for v0.1a

**Rationale:** ADR-001 mandates agent independence. The repo lives at `~/memory-server/`, accessible to any agent. Both default and coder profiles can work with it.

**Outcome:** `~/memory-server/` is the project root. Git remote: `git@github.com:fedosis/Composite-memory-MCP-server.git` (SSH).

---

### Decision 002: Project journal format

**Context:** Need to track operational decisions (not architecture) in real time.

**Alternatives:**
1. ADR-only — rejected, ADRs are permanent architecture records, too heavy for daily decisions
2. CHANGELOG.md — rejected, release-focused, not decision-focused
3. DECISIONS.md — chosen, lightweight operational journal

**Rationale:** Per Fedos instruction — «все принимаемые решения вести в отдельном журнале». DECISIONS.md is that journal. ADRs stay in docs/ADR.md for permanent architecture.

**Outcome:** DECISIONS.md at project root. Append-only. One entry per decision with alternatives and rationale.

---

### Decision 003: Two-stage review workflow

**Context:** Need to define when and how codex-alter-ego reviews the work.

**Alternatives:**
1. Review only before merge — rejected, too late for architecture fixes
2. Review only architecture — rejected, misses code-level bugs
3. Architecture review (before code) + Code review (after tests pass) — chosen

**Rationale:** Per project-development-protocol (AGENTS.md). Architecture review validates the spec before a line is written. Code review validates the implementation meets the spec after tests pass.

**Outcome:** Every card goes through:
  1. Architecture review (codex-alter-ego) → card from todo → ready
  2. Test-driven implementation (coder profile)
  3. Tests pass → Code review (codex-alter-ego) → card from review → done
  4. squash-merge to main → push

---

### Decision 004: First card scope — v0.1a core

**Context:** Roadmap says v0.1a = MCP API + SQLite backend + get_context/search/remember.

**Scope:** Break v0.1a into implementation cards:
  - Card 001: Project skeleton (pyproject.toml, project structure, MCP entry point)
  - Card 002: Data models (Pydantic models: Entity, Fact, Decision, Skill, MemoryReceipt)
  - Card 003: SQLite provider (CRUD for facts + receipts)
  - Card 004: MCP tools — get_context (with SQLite provider)
  - Card 005: MCP tools — search (keyword + SQL)
  - Card 006: MCP tools — remember (direct store)
  - Card 007: Integration tests + docs

**Alternatives:**
1. One giant card for all of v0.1a — rejected, too complex for single review pass
2. Two cards (backend + frontend) — rejected, MCP is the only interface, no separate frontend
3. 7 small cards as above — chosen, each is independently reviewable and testable

**Outcome:** 7 cards for v0.1a. Card 001 starts execution.
