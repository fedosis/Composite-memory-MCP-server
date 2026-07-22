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

---

## 2026-07-22 — Operations governance

### Decision 005: Normalisation of external signals into roadmap decisions

**Context:** The project receives feature suggestions, complaints, and change requests from multiple channels — Moltbook, Reddit, GitHub issues, and the team's own operational pain. Without a consistent filter, roadmap priority is determined by whoever shouts loudest or whichever channel has the most upvotes. This is unsustainable for a project that aims at a narrow, stable core.

**Proposed approach (normalisation chain):** Every external signal MUST pass through a 6-stage normalisation pipeline before it enters the roadmap:

1. **Signal** — raw input (e.g. "add batch delete", "search is too slow", "why no Redis backend")
2. **Problem** — what _actual_ user pain or operational gap does this signal reveal?
3. **Proposed approach** — what concrete change would address the problem (not necessarily the one suggested by the signal)
4. **Evidence** — data or reasoning supporting the problem and the proposed approach (metrics, user count, logs, discussion link)
5. **Confidence** — low / medium / high, based on evidence quality and sample size
6. **Roadmap impact** — what gets deferred or dropped if this is accepted

Only entries with **Confidence ≥ medium** and an explicit **roadmap impact** assessment proceed into roadmap grooming.

**Rationale:**
- Feature requests by vote alone introduce survivorship bias — the most vocal users are not representative. Moltbook posts reflect a power-user subset; Reddit upvotes reflect a drive-by audience; GitHub stars reflect passive interest, not committed users.
- The project goal is a **narrow, stable core** with replaceable policies/adapters. Every added feature is a permanent maintenance liability that narrows the space for future adapters.
- This is a project-governance decision for Composite Memory MCP Server: it governs how the project translates external noise into bounded work assignments without architectural change.
- Source bias is explicitly tracked: each entry records where the signal originated so patterns can be audited later.

**Alternatives considered:**
1. Accept all requests and prioritise by community votes — rejected, amplifies vocal minority, works against narrow-core principle
2. Gate all requests behind a paid sponsorship — rejected, premature monetisation for v0.1a
3. Direct maintainer veto on everything — rejected, single point of failure, undermines community trust
4. **normalisation chain (chosen)** — objective, auditable, reproducible filter

**Outcome:**
External signals are no longer accepted as direct roadmap items. Every signal → problem translation is recorded as a short entry with the 6-field header (signal → problem → proposed approach → evidence → confidence → roadmap impact). Roadmap grooming runs against those entries, not against raw GitHub issues or social-media posts. Feature requests must be backed by evidence; no upvote count alone qualifies.
