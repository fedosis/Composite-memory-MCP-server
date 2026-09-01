"""Curiosity worker: save CUR-TTL-LEASE-CAUSALITY-000 findings to CMMS via remember()."""
import asyncio
import sys
from datetime import datetime, timezone

from _common import get_db_url

sys.path.insert(0, "/home/shtorm/memory-server/src")

from memory_server.api.remember import remember
from memory_server.providers.sqlite_provider import SQLiteProvider

DB_URL = get_db_url()
SOURCE = "curiosity-worker/research"
SESSION = "cron_20260817_curiosity_ttl_lease_causality"

FACTS = [
    {
        "subject": "TTL lease for dead-agent reassignment (vina, Moltbook 2026-08-17)",
        "predicate": "is",
        "object": (
            "non-determinism injection: wall-clock expiry is a proxy for time-passed, not "
            "for agent-death; a slow/GC-paused/partitioned agent can resume after its lease "
            "expired and perform a stale write, racing whoever took over. Post title: "
            "'Your TTL Leases Are Just Sophisticated Race Conditions'"
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "web_search",
            "sources": ["https://www.moltbook.com/post/6aaefc69-230b-4730-9fd2-84143949ffb2"],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Correct alternative to TTL-lease reassignment",
        "predicate": "is",
        "object": (
            "atomic conditional writes (compare-and-swap / expected_version) + version "
            "vectors + idempotent state transitions — causality, not clocks"
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "inference",
            "sources": [
                "https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html",
                "https://amturing.acm.org/p558-lamport.pdf",
            ],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Lamport 1978 (Time, Clocks, Ordering of Events)",
        "predicate": "establishes",
        "object": (
            "physical time is the wrong frame for distributed ordering; the happened-before "
            "partial ordering + logical clocks capture causality, not chronology"
        ),
        "confidence": 0.95,
        "evidence": {
            "method": "web_search",
            "sources": ["https://amturing.acm.org/p558-lamport.pdf"],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Kleppmann 2016 distributed-locking fencing tokens",
        "predicate": "shows",
        "object": (
            "a TTL lease alone cannot prevent the stale-write race between a paused "
            "lock-holder and its successor; the resource must reject stale ops via a "
            "monotonically increasing fencing token checked at write time; Redlock lacks "
            "fencing tokens and is unsafe for correctness"
        ),
        "confidence": 0.95,
        "evidence": {
            "method": "web_search",
            "sources": [
                "https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html",
                "https://hackernoon.com/the-fencing-gap-why-your-distributed-lock-isnt-safe-and-how-to-fix-it",
            ],
            "session_id": SESSION,
        },
    },
    {
        "subject": "CMMS ttl_days / expires_at usage (admission.py, reflect.py)",
        "predicate": "is",
        "object": (
            "legit decay (case a), not lease-as-liveness (case b): ttl_days is assigned by "
            "MemoryTag (DURABLE=365, EPHEMERAL=1, IMPORTANT=None) for fact/belief expiry and "
            "belief decay (active->stale->archived->forgotten), single-writer, monotonic, safe"
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "observation",
            "sources": [
                "/home/shtorm/memory-server/src/memory_server/admission.py",
                "/home/shtorm/memory-server/src/memory_server/api/reflect.py",
            ],
            "session_id": SESSION,
        },
    },
    {
        "subject": "CMMS already implements the correct primitives (version + expected_version + lifecycle state "
                   "machine)",
        "predicate": "has_but_underuses",
        "object": (
            "lifecycle_service.py provides version counter (increment_version), expected_version "
            "optimistic-concurrency/fencing check (_validate_expected_version raises on mismatch), "
            "lifecycle_state transition validation, lifecycle_events audit — exactly 'atomic "
            "conditional writes + version vectors + idempotent transitions'; but they are largely "
            "dormant: expected_version defaults to None, version rarely increments on plain "
            "remember() writes, lifecycle_events = 0"
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "observation",
            "sources": [
                "/home/shtorm/memory-server/src/memory_server/services/lifecycle_service.py",
                "/home/shtorm/memory-server/src/memory_server/api/remember.py",
            ],
            "session_id": SESSION,
        },
    },
    {
        "subject": "TTL-as-liveness hazard boundary for Hermes (ARP multi-agent ownership)",
        "predicate": "recommends",
        "object": (
            "keep ttl_days as decay-only; do NOT introduce lease-as-liveness for multi-agent "
            "ownership (task/fact reassignment) — that is vina's race condition; if a lease is "
            "needed, use heartbeat + fencing token and treat expiry as a stale-hint to re-verify, "
            "not proof of death; activate expected_version fencing on coordinating transitions and "
            "increment version + record lifecycle_event on every mutate"
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "inference",
            "sources": [
                "https://www.moltbook.com/post/6aaefc69-230b-4730-9fd2-84143949ffb2",
                "~/.hermes/workspace/findings/cur-ttl-lease-causality-000.md",
            ],
            "session_id": SESSION,
        },
    },
]


async def main():
    provider = SQLiteProvider(url=DB_URL)
    await provider.initialize()
    results = []
    try:
        for f in FACTS:
            metadata = {
                "evidence": f["evidence"],
                "source_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "tags": [
                    "curiosity-worker",
                    "ttl-lease-causality",
                    "distributed-systems",
                    "fencing-tokens",
                    "version-vectors",
                    "cmms",
                    "causality",
                ],
            }
            res = await remember(
                provider=provider,
                subject=f["subject"],
                predicate=f["predicate"],
                object=f["object"],
                confidence=f["confidence"],
                source=SOURCE,
                metadata=metadata,
            )
            results.append(res["fact"].id)
    finally:
        await provider.close()

    print("SAVED", len(results), "facts:")
    for fid in results:
        print("  -", fid)


if __name__ == "__main__":
    asyncio.run(main())
