"""Curiosity worker: save CUR-012 (audit of CMMS update/merge/consolidation
semantics vs CUR-011 conservation laws) findings to CMMS via remember()."""
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
SESSION = "cron_20260820_curiosity_merge_audit"

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
        "subject": "CMMS update/merge/consolidation audit — verdict (CUR-012)",
        "predicate": "concludes",
        "object": (
            "Mapping CMMS's EXISTING mutation machinery onto the four CUR-011 conservation-law "
            "requirements shows PARTIAL coverage. What exists: lifecycle_events = the only true "
            "append-only audit log (state transitions only; atomic; reason + triggered_by + "
            "expected_version race check); receipts = write-once provenance (ReceiptRepository has "
            "create/get/search only — no update/append path, verified in code; live-DB updated_at vs "
            "timestamp diffs are a +10800s UTC+3 timezone artifact, NOT mutations); outbox_entries = "
            "append-only index journal (223,477 rows, status-mutated but never deleted); "
            "merge/supersede paths KEEP originals (superseded/discarded, never hard-deleted). "
            "What is missing: structured loss receipt, lineage edges, audit of confidence mutations, "
            "tombstones for hard deletes, genesis checkpoint, replay test. PRODUCTION STATE: the "
            "entire lifecycle/merge machinery is UNEXERCISED — live DB has 0 lifecycle_events, 0 "
            "lifecycle_states, 0 claim_relations, all 222,699 facts 'active', 1 belief, and no MCP "
            "tool exposes delete (tool surface: search, get_context, remember, learn, semantic_search, "
            "graph_search, add_relation, invalidate, route, audit, metrics, set_belief, get_belief, "
            "resolve_conflict, reflect). This is a latent-capability audit, not observed behavior."
        ),
        "confidence": 0.95,
        "evidence": {
            "method": "code_inspection + live_db_audit",
            "sources": [
                "storage/repositories/fact_repo.py", "storage/repositories/receipt_repo.py",
                "storage/repositories/lifecycle_repo.py", "storage/repositories/belief_repo.py",
                "storage/repositories/evidence_repo.py", "storage/repositories/relation_repo.py",
                "src/memory_server/services/lifecycle_service.py",
                "src/memory_server/services/ingestion_service.py",
                "src/memory_server/server.py (tools list)", "storage/outbox.py",
                "storage/outbox_worker.py", "sqlite3 read-only live DB queries",
            ],
            "session_id": SESSION,
            "claim_type": "fact",
        },
    },
    {
        "subject": "CMMS merge semantics — what resolve_conflict/set_belief actually preserve (CUR-012)",
        "predicate": "preserves",
        "object": (
            "resolve_conflict (resolution='merge'): creates a NEW merged belief (confidence = avg of "
            "the two, tags/source_ids union), copies evidence rows from BOTH originals "
            "(EvidenceRepository.get_by_belief_id + create — best-effort try/except that logs a "
            "warning on failure, i.e. silent partial-copy risk), then supersedes both originals via "
            "LifecycleService with reason 'Merged into {merged.id} via conflict resolution' and "
            "expected_version race checks; originals KEPT, not deleted. set_belief "
            "(replace_belief_id): supersedes the old belief ('Replaced by {new.id}'), new belief "
            "version = old.version + 1. GAPS: (a) the merged/replaced belief's receipt has NO history "
            "entry recording collapsed_ids; (b) NO claim_relations edge from merged → originals — "
            "lineage is only in reason strings of the originals' lifecycle events (forward traversal "
            "by scanning events; reverse new→old requires parsing prose); (c) resolve_conflict builds "
            "an ephemeral MemoryReceipt (random UUID) that is returned in the JSON response but NEVER "
            "persisted — the conflict-resolution audit record is lost on the next call unless the "
            "caller stores it."
        ),
        "confidence": 0.92,
        "evidence": {
            "method": "code_inspection",
            "sources": [
                "src/memory_server/server.py:698-843 (set_belief), 911-1231 (resolve_conflict)",
                "src/memory_server/services/lifecycle_service.py:159-238 (_transition_in_session)",
            ],
            "session_id": SESSION,
            "claim_type": "fact",
        },
    },
    {
        "subject": "CMMS unlogged mutation paths (CUR-012)",
        "predicate": "flags",
        "object": (
            "Four mutation paths write NO audit trail: (a) BELIEF REINFORCEMENT (set_belief + "
            "learn(extract_beliefs=True)) — weighted-average confidence + version bump + "
            "last_reinforced_at, no lifecycle event, the old confidence is not recoverable from any "
            "log; (b) CONFIDENCE DEMOTION via LifecycleService._propagate_dependents (invalidated "
            "parent → derived dependents ×0.8, floor 0.1) — records a same-state lifecycle event "
            "(from_state == to_state) with reason 'parent_invalidated' but NO old/new confidence "
            "values, so the demotion is not reversible from the log; (c) FactRepository.update / "
            "provider.update_fact — generic setattr mutation with zero audit and no version bump; not "
            "exposed as an MCP tool today, only reachable via the propagation path, but a latent "
            "silent-mutation trap; (d) HARD DELETES (delete_fact / delete_decision / delete_skill, "
            "provider sqlite_provider.py:261/298/334) — no tombstone, no lifecycle event, and the "
            "linked receipt/outbox rows are orphaned (no FK cascade in the ORM)."
        ),
        "confidence": 0.93,
        "evidence": {
            "method": "code_inspection",
            "sources": [
                "src/memory_server/server.py:728-755 (reinforcement), 940-1027 (auto_resolve)",
                "src/memory_server/services/lifecycle_service.py:240-321 (_propagate_dependents)",
                "storage/repositories/fact_repo.py:137-179 (update/delete)",
                "src/memory_server/providers/sqlite_provider.py:242-266 (update_fact/delete_fact)",
                "storage/repositories/belief_repo.py:166-212",
                "storage/repositories/decision_repo.py:53", "storage/repositories/skill_repo.py:41",
            ],
            "session_id": SESSION,
            "claim_type": "fact",
        },
    },
    {
        "subject": "CMMS conservation-law coverage map (CUR-012)",
        "predicate": "maps",
        "object": (
            "Req1 LOSS RECEIPTS per merge {collapsed_ids, frontier, invalidated revisions, rationale}: "
            "PARTIAL — originals are preserved (superseded) and their event reason strings name the "
            "merge target + expected_version acts as frontier, but there is no structured loss-receipt "
            "record and the merge's own receipt is ephemeral (never persisted). Req2 APPEND-ONLY "
            "EVIDENCE LOG + genesis checkpoints: PARTIAL — lifecycle_events / receipts / "
            "outbox_entries are append-only, but no genesis checkpoint exists (content_hash/trace_id "
            "designs from CUR-006/CUR-007 are NOT implemented) and reinforcement/confidence/direct-"
            "update mutations never enter any log. Req3 EXTERNAL ANCHORING (evolver must not judge its "
            "own work; pre-change schema hash outside the agent): ABSENT — no schema hash anywhere, "
            "no outside-the-agent anchor, and the mutator is the verifier (LifecycleService applies "
            "its own confidence demotions; auditor is internal-only). Req4 REPLAY AS ACCEPTANCE "
            "TEST: NOT TESTABLE — reinforcement and propagation demotion destroy old confidence "
            "values (irreconstructible from logs), merge is only partially replayable (originals "
            "kept, but collapsed_ids unstructured and evidence-copy best-effort)."
        ),
        "confidence": 0.94,
        "evidence": {
            "method": "code_inspection + synthesis",
            "sources": [
                "CUR-011 (four conservation-law requirements)",
                "CUR-012 audit of lifecycle_service.py / server.py / repos (see CUR-012 verdict fact)",
            ],
            "session_id": SESSION,
            "claim_type": "fact",
        },
    },
    {
        "subject": "CMMS merge/consolidation gap list (CUR-012)",
        "predicate": "lists",
        "object": (
            "(1) No typed loss-receipt primitive — extend lifecycle_events with a payload column or "
            "add merge_events table carrying {collapsed_ids, expected_versions/frontier, rationale, "
            "actor}; persist the currently-ephemeral resolve_conflict receipt → design handed to "
            "CUR-013. (2) No lineage edges for merges/supersessions — write claim_relations "
            "(source=merged, target=original, 'merged_from'/'supersedes') in the same transaction as "
            "the merge. (3) Confidence mutations (reinforcement, propagation demotion) not audited "
            "with before/after values → extend lifecycle_events payload or dedicated event. "
            "(4) provider.update_fact is a silent-mutation trap → route through LifecycleService or "
            "require an event. (5) Hard-delete paths (delete_fact/decision/skill) have no tombstone → "
            "route through 'forgotten' lifecycle + tombstone outbox entry, or remove the methods. "
            "(6) Genesis checkpoint + external anchor: gated on CUR-006/007/008 implementation + "
            "Fedos approval (schema migration of the live shared DB). (7) Replay-acceptance test "
            "impossible today — requires gap (3) fixed first → design handed to CUR-014."
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "design",
            "sources": [
                "CUR-012 audit (conservation-law coverage map)",
                "CUR-006 (content_hash design)", "CUR-007 (authority/trace design)",
                "CUR-008 (shared-DB blocker)",
            ],
            "session_id": SESSION,
            "claim_type": "fact",
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
                    "lifecycle",
                    "merge",
                    "consolidation",
                    "conservation-laws",
                    "auditability",
                    "append-only",
                    "loss-receipt",
                    "replay",
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
