"""Curiosity worker: save CUR-013 (design of a typed merge/consolidation
loss-receipt primitive for CMMS) findings to CMMS via remember()."""
import asyncio
from datetime import datetime, timezone

from _common import get_db_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from memory_server.api.remember import remember
from memory_server.providers.sqlite_provider import SQLiteProvider

DB_URL = get_db_url()
SOURCE = "curiosity-worker"
SESSION = "cron_20260821_curiosity_loss_receipt_design"

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
        "subject": "CMMS loss-receipt primitive design — verdict (CUR-013)",
        "predicate": "chooses",
        "object": (
            "Extend lifecycle_events with event_type + payload columns (Option A) instead of "
            "a new merge_events table (Option B). Rationale: lifecycle_events is already the "
            "only true append-only audit log (CUR-012); a merge is an event in the same log, "
            "not a parallel log — Option B would need its own append-only guarantees, a second "
            "write path, and cross-log joins. The live table has 0 rows (CUR-012), so nullable "
            "additive columns need no backfill. Same-state (from==to) events already exist as a "
            "precedent: _propagate_dependents records active->active events for confidence "
            "demotion, so an active->active 'merge' event on the merged belief is consistent "
            "with the codebase's own conventions. record_event() in lifecycle_repo.py is a "
            "single choke point for the write path; get_events() gains the new fields for the "
            "audit surface. Fits the same Alembic migration wave as CUR-006/007 (0005); gated "
            "on CUR-008 + Fedos approval for the live-DB touch."
        ),
        "confidence": 0.93,
        "evidence": {
            "method": "code_inspection + design",
            "sources": [
                "storage/models/lifecycle.py (LifecycleEventORM: id, memory_id, memory_type, from_state, to_state, "
                "reason, triggered_by, timestamp — NO payload column)",
                "storage/repositories/lifecycle_repo.py:51-71 (record_event — single write choke point)",
                "src/memory_server/services/lifecycle_service.py:240-321 (_propagate_dependents same-state event "
                "precedent)",
                "src/memory_server/server.py:1057-1173 (resolve_conflict merge branch), 1147-1164 (dead "
                "transition_requests rebuild)",
                "CUR-011 (conservation-law Req1: loss receipts {collapsed_ids, frontier, invalidated revisions, "
                "rationale})",
                "CUR-012 (lifecycle_events = only append-only log; 0 rows live)",
            ],
            "session_id": SESSION,
            "claim_type": "fact",
        },
    },
    {
        "subject": "CMMS loss-receipt schema design (CUR-013)",
        "predicate": "specifies",
        "object": (
            "Two additive columns on lifecycle_events: event_type String nullable default "
            "'transition' (values: transition | merge | consolidate) and payload Text nullable "
            "(JSON). ORM: LifecycleEventORM gains both; get_events() returns them (parsed). "
            "record_event() gains event_type='transition' + payload=None params, json.dumps "
            "inside. LOSS-RECEIPT PAYLOAD SCHEMA (schema='loss-receipt/1'): {kind: "
            "'merge'|'consolidate', collapsed_ids: [uuid_a, uuid_b], frontier: {uuid_a: "
            "version_a, uuid_b: version_b} (expected_versions at merge time = invalidated "
            "revisions), confidences: {uuid_a: ca, uuid_b: cb} (input confidences — makes "
            "merged confidence min(1,(ca+cb)/2) replayable, Req4), rationale: str (resolution "
            "reason / new proposition), actor: str (triggered_by), merged_id: uuid, "
            "evidence_copy: {status: 'full'|'partial', missing: [uuid]} — records the CUR-012 "
            "silent partial-copy risk as an observable field instead of a log warning}. "
            "Migration idempotent per 0004 pattern (inspector column check before ALTER TABLE "
            "ADD COLUMN; SQLite create_all() will NOT add columns to the existing live table). "
            "No backfill needed (0 rows). Consolidate kind reserved for future CUR-011 "
            "dreaming/consolidation."
        ),
        "confidence": 0.92,
        "evidence": {
            "method": "design (grounded in code)",
            "sources": [
                "storage/models/lifecycle.py (current columns)",
                "alembic/versions/0004_add_claim_relations.py (idempotency pattern)",
                "CUR-006 (0005 migration wave + backfill approach)",
                "CUR-011 (loss-receipt field requirements from Moltbook community consensus)",
                "CUR-012 (0 lifecycle_events in live DB; evidence-copy best-effort try/except)",
            ],
            "session_id": SESSION,
            "claim_type": "fact",
        },
    },
    {
        "subject": "CMMS loss-receipt write-path design (CUR-013)",
        "predicate": "specifies",
        "object": (
            "resolve_conflict merge branch (server.py:1057-1173): inside _merge_callback (same "
            "transaction as create_in_transaction, which already supports relation_entries and "
            "runs relations BEFORE the callback): (1) lifecycle event on merged.id — "
            "memory_type='belief', from=to='active', event_type='merge', payload=loss-receipt "
            "{collapsed_ids, frontier, confidences, rationale, actor, merged_id, "
            "evidence_copy}; (2) originals' supersede transitions each get payload "
            "{merged_id: merged.id} — structured reverse pointer replacing prose-only parsing; "
            "(3) claim_relations lineage edges, direction convention source=NEWER/derived "
            "item: (merged.id, a, 'merged_from'), (merged.id, b, 'merged_from'), (a, merged.id, "
            "'superseded_by'), (b, merged.id, 'superseded_by') — 4 rows, composite PK "
            "(source,target,type) allows both; bidirectional traversal works with the current "
            "RelationRepository API (get_by_source / get_dependents); (4) PERSIST the "
            "currently-ephemeral resolution MemoryReceipt (server.py:1214-1220) via "
            "ReceiptRepository(session).create inside the callback, history=[{method: "
            "'resolve_conflict', resolution, belief_a_id, belief_b_id, collapsed_ids, "
            "frontier, rationale, session_id}] — closes CUR-012 gap (c). set_belief replace "
            "path: 'supersedes' edge (new->old) + history entry {method: 'set_belief', "
            "replaced_belief_id, replaced_version} on the (already-persisted) belief receipt + "
            "payload {replaced_by} on the old belief's supersede event. ALL resolution "
            "branches (keep_a/keep_b/discard_both/auto_resolve) persist their currently-"
            "ephemeral receipt; lineage edges only for merge + replace. Cleanup: delete dead "
            "transition_requests rebuild (1147-1164) and no-op 'transition_requests[0].reason' "
            "statements (970, 996); evidence-copy try/except writes evidence_copy.status into "
            "the payload instead of a silent warning. No create_in_transaction signature "
            "change needed."
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "code_inspection + design",
            "sources": [
                "src/memory_server/server.py:1057-1173 (_merge_callback, create_in_transaction call)",
                "src/memory_server/server.py:698-843 (set_belief replace path)",
                "src/memory_server/providers/sqlite_provider.py:660-718 (create_in_transaction: relation_entries "
                "before callback)",
                "storage/repositories/relation_repo.py (create idempotent per (source,target,type); get_by_source; "
                "get_dependents)",
                "storage/repositories/receipt_repo.py (write-once create)",
                "CUR-012 gap list items (1)+(2)+(c)",
            ],
            "session_id": SESSION,
            "claim_type": "fact",
        },
    },
    {
        "subject": "CMMS loss-receipt gating + handoff (CUR-013)",
        "predicate": "hands_off",
        "object": (
            "Implementation handed to CUR-015. Gating: schema columns on lifecycle_events "
            "touch the live shared memory.db (three gateways symlink the same DB) so they must "
            "ship AFTER CUR-008 (profile isolation) with Fedos approval, in the same 0005 "
            "migration wave as CUR-006 content_hash + CUR-007 trace_id (all additive, one "
            "write-lock window). write-path changes (edges, persisted receipt, dead-code "
            "cleanup) are code-only and could land independently of the migration, but the "
            "payload column is required for the loss receipt, so ship together. Req4 replay "
            "gains merge reconstructibility from confidences+frontier in the payload; "
            "consolidate kind reserved for future dreaming. No new MCP tool required — the "
            "primitive is a write-path + audit-surface change."
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "design",
            "sources": [
                "CUR-013 verdict/schema/write-path facts",
                "CUR-008 (shared-DB symlink blocker)",
                "CUR-006 (content_hash design)", "CUR-007 (trace_ID design)",
                "CUR-011 (consolidation/dreaming future work)",
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
                    "loss-receipt",
                    "merge",
                    "consolidation",
                    "lifecycle",
                    "conservation-laws",
                    "auditability",
                    "append-only",
                    "provenance",
                    "design",
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
