"""Curiosity worker: save CUR-COT-VERIFIER-000 findings to CMMS via remember()."""
import asyncio
import sys
from datetime import datetime, timezone

from _common import get_db_url

sys.path.insert(0, "/home/shtorm/memory-server/src")

from memory_server.api.remember import remember
from memory_server.providers.sqlite_provider import SQLiteProvider

DB_URL = get_db_url()
SOURCE = "curiosity-worker/research"
SESSION = "cron_20260815_curiosity_cot_verifier"

FACTS = [
    {
        "subject": "Online CoT verifier learnability (Balcan et al. 2603.03538)",
        "predicate": "establishes",
        "object": (
            "Online-learning framework for chain-of-thought verifiers: soundness errors "
            "(accept incorrect reasoning) vs completeness errors (reject correct reasoning) "
            "are asymmetric; soundness-completeness Littlestone dimension gives tight "
            "mistake bounds + Pareto frontier under a soundness budget"
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "web_search",
            "sources": [
                "https://arxiv.org/abs/2603.03538",
                "https://arxiv.org/html/2603.03538v3",
            ],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Offline/statistical verifier learning",
        "predicate": "fails_under",
        "object": (
            "generator-verifier feedback-loop distribution shift: reasoning traces are "
            "generated adaptively conditioned on prior verifier feedback, deviating from "
            "any fixed training distribution (vina's thesis confirmed)"
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "web_search",
            "sources": ["https://arxiv.org/html/2603.03538v3"],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Hermes response-verification pipeline (cove_check.py + response_check.py)",
        "predicate": "is",
        "object": (
            "static non-learning regex verifiers = offline-demonstration class that the "
            "paper argues breaks under distribution shift"
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "observation",
            "sources": [
                "~/.hermes/scripts/cove_check.py",
                "~/.hermes/scripts/response_check.py",
            ],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Full online-learned verifier for Hermes chat",
        "predicate": "is_recommended_as",
        "object": (
            "NOT recommended: no label oracle in autonomous cron, per-message token/latency "
            "cost vs router local-tier, non-realizable fuzzy correctness (no ground-truth "
            "for sycophancy/tone), false-blocking = social cost of silence"
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "inference",
            "sources": [
                "https://arxiv.org/abs/2603.03538",
                "CUR-AI-SLOP-MARKERS-001 (LLM-judge overconfidence drift)",
            ],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Recommended middle path for response-verification",
        "predicate": "should_adopt",
        "object": (
            "Hybrid: (1) explicit asymmetric soundness-vs-completeness metrics, bias to "
            "abstention (safe side) over unsafe output; (2) drift-monitoring/rule-curation "
            "loop auditing static regex against fresh generator outputs (weak online "
            "learning over rules, no trained verifier model); extends CUR-AI-SLOP-MARKERS"
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "inference",
            "sources": [
                "https://arxiv.org/abs/2603.03538",
                "~/.hermes/workspace/findings/cur-cot-verifier-000.md",
            ],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Verifier soundness vs completeness in boosting",
        "predicate": "shows",
        "object": (
            "wrapped-generator error rate is governed by verifier soundness; completeness "
            "errors lead to abstention/rejection (safe side) — soundness is the higher-stakes "
            "error type to minimize"
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "web_search",
            "sources": ["https://arxiv.org/html/2603.03538v3"],
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
                    "cot-verifier",
                    "response-verification",
                    "online-learning",
                    "soundness-completeness",
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
