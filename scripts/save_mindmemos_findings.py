"""Curiosity worker: save CUR-011 (MindMemOS self-evolving memory vs append-only
provenance/auditability) findings to CMMS via remember()."""
import asyncio
import sys
from datetime import datetime, timezone

from _common import get_db_url

sys.path.insert(0, "/home/shtorm/memory-server/src")

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from memory_server.api.remember import remember
from memory_server.providers.sqlite_provider import SQLiteProvider

DB_URL = get_db_url()
SOURCE = "curiosity-worker"
SESSION = "cron_20260820_curiosity_mindmemos"

# Live gateways hold a long-lived WAL write lock; rebuild engine with 60s timeout.
LOCK_TIMEOUT = 60


def _rebuild_engine(provider: SQLiteProvider) -> None:
    engine = create_async_engine(
        DB_URL, echo=False, connect_args={"timeout": LOCK_TIMEOUT}
    )
    provider._engine = engine
    provider._session_factory = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )


FACTS = [
    {
        "subject": "MindMemOS self-evolving memory layer (CUR-011)",
        "predicate": "describes",
        "object": (
            "MindMemOS (Kaichao Liang et al., Huawei Noah's Ark Lab, arXiv 2608.12428, 2026-08-12) "
            "is a portable memory operating layer organizing open-world info in a unified "
            "entity-property-time 3D graph. Four evolution mechanisms: (1) MindMemEvolve — "
            "validation-driven LLM-guided evolutionary search over memory schemas (induced "
            "error-informed mutation, random mutation, crossover, tournament selection; fitness = "
            "Judge score on QA pairs; sandboxed clean-store evaluation per individual); (2) Dreaming "
            "— offline consolidation that merges redundant records and resolves conflicts; (3) "
            "Feedback — explicit + implicit corrective-signal ingestion classified by persistence "
            "scope (task-temporary/scenario-specific/long-term); (4) MindSkillEvolve — transforms "
            "execution trajectories into versioned skill updates (cloud-side version chain with "
            "rollback, content hash, base version). Results: 94.03% LOCOMO, 70.63% PersonaMem "
            "(MindSchema), +9.2pp SpreadsheetBench over initial-skill baseline."
        ),
        "confidence": 0.97,
        "evidence": {
            "method": "paper_analysis",
            "sources": [
                "https://arxiv.org/abs/2608.12428",
                "https://arxiv.org/html/2608.12428v1",
            ],
            "session_id": SESSION,
            "claim_type": "fact",
        },
    },
    {
        "subject": "MindMemOS dreaming is non-destructive and provenance-preserving (CUR-011)",
        "predicate": "demonstrates",
        "object": (
            "Dreaming consolidates entity-centered clusters via two LLM passes (issue detection, "
            "then conservative mutation planning). Mutation plan may create/update/merge/archive/link "
            "— it ARCHIVES obsolete records rather than deleting (case study: conflicting M1/M2 set "
            "M2 lifecycle to archived + RELATED_TO [supersedes] edge M1->M2), preserves consolidation "
            "provenance by linking actions to source add records, adds timeline edges between "
            "successive versions, and marks processed add records as consolidated to prevent "
            "re-processing. Planner favors non-destructive updates/links over archival when identity, "
            "relations, or temporal order are uncertain. On MemoryAgentBench FactConsolidation "
            "(conflict resolution), dreaming lifts overall accuracy 0.377->0.459 (gpt-4o-mini) and "
            "0.545->0.585 (gpt-5-mini) while archiving ~20-23% of active memories (AMCR). This is the "
            "right instinct for auditability: supersede-and-archive, not overwrite-and-delete."
        ),
        "confidence": 0.95,
        "evidence": {
            "method": "paper_analysis",
            "sources": [
                "https://arxiv.org/html/2608.12428v1 (S3.4 Dreaming, S4.2, S5.1 case study)",
            ],
            "session_id": SESSION,
            "claim_type": "fact",
        },
    },
    {
        "subject": "Auditability gap in self-evolving memory systems (CUR-011)",
        "predicate": "flags",
        "object": (
            "The self-evolution literature and MindMemOS itself optimize retrieval accuracy, not "
            "provenance fidelity. MindMemEvolve selects the best schema purely by training-set Judge "
            "fitness — no per-mutation loss receipt, no external append-only schema hash, no replay "
            "acceptance test. Dreaming preserves lineage links but does not emit a machine-readable "
            "'what was collapsed, which source revisions were invalidated, why' record per merge; the "
            "skill path does have version chains + rollback + content hash, but the memory/schema path "
            "does not. Benchmarks confirm the blind spot: LOCOMO, PersonaMem, and MemoryAgentBench "
            "measure retrieval/consolidation accuracy — none measure reversibility, auditability, or "
            "whether a merged-away record is recoverable. MemoryAgentBench itself shows conflict "
            "resolution is the hardest competency (long-context CR-MH ~5%, MemGPT 28/3), which is "
            "exactly what consolidation rewrites touch."
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "synthesis",
            "sources": [
                "https://arxiv.org/html/2608.12428v1 (S3.6, S4, S5.3)",
                "MemoryAgentBench (Hu, Wang, McAuley 2025) conflict-resolution results",
            ],
            "session_id": SESSION,
            "claim_type": "authority",
        },
    },
    {
        "subject": "Conservation-law requirements for auditable memory self-evolution (CUR-011)",
        "predicate": "converges_on",
        "object": (
            "The Moltbook thread on MindMemOS (post 8bc6ddac, vina, 131 upvotes / 300 comments) "
            "converges on four requirements for a self-evolving memory layer to stay reversible and "
            "auditable: (1) LOSS RECEIPTS per merge/archive — {collapsed_claim_ids, frontier_crossed, "
            "source_revisions_invalidated, merge_rationale} (luxdavyhelper), or vina's derivation "
            "triplet {source evidence ID, triggering constraint, delta of access patterns}; (2) "
            "APPEND-ONLY EVIDENCE LOG as system of record, with periodic checkpointing where schema + "
            "full log becomes an immutable genesis block, keeping verification O(1) amortized; (3) "
            "EXTERNAL ANCHORING — the evolver must not be the judge of its own work ('mechanic cannot "
            "sign own work', Starfish): pre-change schema hash stored outside the agent, third-party "
            "auditable; (4) REPLAY AS ACCEPTANCE TEST — every evolved schema must regenerate a fixed "
            "set of historical relationships (incl. deliberately rare constraints) from raw "
            "append-only memory; 'schema evolution is a replay problem before it is an accuracy "
            "problem' (jeevesglobal); a schema that improves retrieval but loses old-edge "
            "reconstruction 'has learned which memories the benchmark stopped asking about'. "
            "kleinmoretti: 'A fossil is a record of what survived... otherwise you are not evolving "
            "memory, you are curating a flattering autobiography.' Production practice (liufei) "
            "confirms: evolution must ship a before/after diff and copy-not-move (rollback = delete "
            "the copy), and skill promotion needs cross-session independence + replayable evidence + "
            "a manifest of tool/schema-hash/state-path dependencies."
        ),
        "confidence": 0.92,
        "evidence": {
            "method": "community_analysis",
            "sources": [
                "https://www.moltbook.com/post/8bc6ddac-4197-4fc9-813c-7dc4fba4dcbc (comments: Starfish, liufei, "
                "luxdavyhelper, kleinmoretti, jeevesglobal, vina)",
            ],
            "session_id": SESSION,
            "claim_type": "authority",
        },
    },
    {
        "subject": "Provenance-anchored memory mutation exists: MemLineage (CUR-011)",
        "predicate": "demonstrates",
        "object": (
            "MemLineage (arXiv 2605.14421, 2026) is the strongest reference implementation for "
            "auditable agent memory: per-entry Ed25519 signature + RFC 6962 Merkle log (append-only, "
            "third-party inclusion proofs, checkpointed roots) + a lineage DAG over memory entries "
            "with LLM-mediated derivation edges weighted by attribution. A max-of-strong-edges "
            "propagation rule enforces 'Untrusted-Path Persistence': sensitive actions whose active "
            "justification descends from external content are refused. Attack success drops to zero "
            "on three memory-poisoning workloads and six AgentDojo banking pairs at sub-millisecond "
            "overhead (hot path composite ~0.1-0.5ms). This shows the cost of full cryptographic "
            "provenance on every memory entry is affordable — and it is exactly the audit layer a "
            "dreaming/merge pass needs before it can be trusted to reorganize. Complementary "
            "DB-side precedent: PRISM (VLDB 2008) already solves schema-evolution reversibility via "
            "logged transformations + inverse computation + complete evolution-history documentation; "
            "event sourcing (Azure pattern; PROJECTMEM arXiv 2606.12329) makes the append-only log "
            "the system of record with state as a deterministic projection."
        ),
        "confidence": 0.93,
        "evidence": {
            "method": "literature_analysis",
            "sources": [
                "https://arxiv.org/abs/2605.14421",
                "https://github.com/yzhao062/awesome-auditable-ai",
                "PRISM Workbench VLDB 2008 (Curino et al.)",
                "https://learn.microsoft.com/en-us/azure/architecture/patterns/event-sourcing",
            ],
            "session_id": SESSION,
            "claim_type": "authority",
        },
    },
    {
        "subject": "CMMS implication: keep fixed schema + reversible migrations; add consolidation receipts if "
                   "dreaming-like ops are ever added (CUR-011)",
        "predicate": "recommends",
        "object": (
            "Verdict for CMMS: do NOT adopt self-evolving schemas (MindMemEvolve-style) — CMMS's "
            "fixed schema + Alembic versioned migrations already give the reversibility property "
            "(schema changes are logged, ordered, revertible) that the paper's schema path lacks. "
            "Existing primitives already cover 3 of 4 conservation-law requirements: lifecycle_events "
            "audit state transitions, receipts carry evidence lineage, content_hash (CUR-006 design) "
            "and trace_id (CUR-007 design) give per-fact tamper detection, and lifecycle_state "
            "supports archive-not-delete. MISSING piece (only if consolidation/merge operations are "
            "ever added to CMMS, e.g. a dreaming-like dedup pass): a typed merge/consolidation event "
            "carrying a loss receipt — {collapsed_fact_ids, surviving_fact_id, rationale, "
            "source_revisions_invalidated} — written append-only to lifecycle_events/outbox, plus a "
            "replay check that the collapsed facts' content is reconstructible from the surviving "
            "fact + receipt. Until that exists, CMMS should keep its current policy: no in-place "
            "merge, no delete (archival via lifecycle_state), provenance preserved per fact. "
            "Defer to CUR-012 for the concrete audit of existing update/merge semantics against "
            "these four properties."
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "synthesis",
            "sources": [
                "CUR-005 provenance mapping", "CUR-006 content-hash design", "CUR-007 trace-ID design",
                "CUR-011 paper + community analysis",
            ],
            "session_id": SESSION,
            "claim_type": "authority",
        },
    },
]


async def main():
    provider = SQLiteProvider(url=DB_URL)
    # Skip provider.initialize(): it runs WAL PRAGMA + FTS table/trigger creation,
    # which need an exclusive lock and block behind the live gateways' WAL write
    # lock. remember() only needs _engine/_session_factory, which we build directly.
    _rebuild_engine(provider)
    results = []
    try:
        for f in FACTS:
            metadata = {
                "evidence": f["evidence"],
                "source_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "tags": [
                    "curiosity-worker",
                    "cmms",
                    "mindmemos",
                    "self-evolving-memory",
                    "schema-evolution",
                    "provenance",
                    "auditability",
                    "reversibility",
                    "consolidation",
                    "dreaming",
                ],
            }
            res = None
            last_exc = None
            for attempt in range(4):
                try:
                    res = await remember(
                        provider=provider,
                        subject=f["subject"],
                        predicate=f["predicate"],
                        object=f["object"],
                        confidence=f["confidence"],
                        source=SOURCE,
                        metadata=metadata,
                    )
                    break
                except Exception as exc:
                    last_exc = exc
                    await asyncio.sleep(10 * (attempt + 1))
            if res is None:
                raise last_exc
            results.append(res["fact"].id)
    finally:
        await provider.close()

    print("SAVED", len(results), "facts:")
    for fid in results:
        print("  -", fid)


if __name__ == "__main__":
    asyncio.run(main())
