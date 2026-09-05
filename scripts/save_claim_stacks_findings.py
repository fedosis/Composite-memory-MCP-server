"""Curiosity worker: save CUR-CLAIM-STACKS-000 findings to CMMS via remember()."""
import asyncio
from datetime import datetime, timezone

from _common import get_db_url

from memory_server.api.remember import remember
from memory_server.providers.sqlite_provider import SQLiteProvider

DB_URL = get_db_url()
SOURCE = "curiosity-worker/research"
SESSION = "cron_20260813_curiosity_claim_stacks"

FACTS = [
    {
        "subject": "Claim-stack architecture (research agents)",
        "predicate": "proposes",
        "object": (
            "Atomic discardable claim units (artifact -> extraction -> transformation "
            "-> conclusion) instead of annotated essays; citation list is UI, claim stack "
            "is the interface"
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "web_search",
            "sources": [
                "https://alan.norbauer.com/articles/github-stacks-with-jujutsu/",
                "https://github.com/ARA-Labs/Agent-Native-Research-Artifact",
            ],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Hermes Evidence Trail Convention",
        "predicate": "is_already",
        "object": (
            "claim-level, not essay-level: each CMMS fact carries method/sources/"
            "session_id/confidence/source_date + provenance receipt"
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "observation",
            "sources": ["SOUL.md Evidence Trail Convention", "CMMS facts+receipts schema"],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Claim-stack gap in Hermes CMMS",
        "predicate": "lacks",
        "object": (
            "provenance derivation chain (derived_from), typed inter-claim relations "
            "(supports/contradicts/derives), and active lifecycle discardability "
            "(0 lifecycle_events)"
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "observation",
            "sources": ["CMMS facts/receipts/lifecycle_events schema inspection"],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Atomic claim decomposition",
        "predicate": "is_canonical_first_step_in",
        "object": (
            "FActScore, MiniCheck, AFEV (2506.07446), ACV, EMULATE (ACL 2025), "
            "FIRE (NAACL 2025), PaperTrail (2602.21045), VeriScore"
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "web_search",
            "sources": [
                "https://arxiv.org/html/2506.07446v1",
                "https://aclanthology.org/2025.fever-1.13.pdf",
                "https://github.com/mbzuai-nlp/fire",
                "https://arxiv.org/html/2602.21045v1",
            ],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Trust Node (Robin Good) counterpoint",
        "predicate": "warns",
        "object": (
            "Connect claims with typed relations (supports/contradicts/example-of), "
            "don't just stack them; a stack without relations is disconnected opinions"
        ),
        "confidence": 0.8,
        "evidence": {
            "method": "web_search",
            "sources": [
                "https://robingood.substack.com/p/the-trust-node-how-to-structure-your-expertise-so-ai-agents-can-find-trust-and-cite-you",
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
                "tags": ["curiosity-worker", "claim-stacks", "provenance", "memory-architecture"],
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
