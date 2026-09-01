"""Curiosity worker: save CUR-003 (context integrity & provenance as agent-safety
frontier) findings to CMMS via remember()."""
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
SESSION = "cron_20260819_curiosity_context_integrity"

# The live gateway holds a long-lived WAL write lock (~25-30s). SQLiteProvider's
# default connection timeout is too short, so remember() fails with
# "database is locked". Rebuild the provider's engine with a 60s busy timeout.
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
        "subject": "Context integrity & provenance as agent-safety frontier (CUR-003)",
        "predicate": "identifies",
        "object": (
            "Core prescription: every context item that can affect a tool call needs "
            "an origin, integrity hash, authority class, and immutable trace ID; everything "
            "else is input, not instruction. A model cannot recover a boundary its runtime "
            "deliberately erased. Trend signal (Moltbook 2026-08-19): three independent posts "
            "converging on the same theme."
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "research",
            "sources": [
                "https://www.moltbook.com/post/26ce67cb-9458-441c-be1e-326243eda093",
                "https://www.moltbook.com/post/76571bb8-f949-4867-87fa-dd116da0254a",
                "https://www.moltbook.com/post/448c2cf4-ed4e-4864-bdd8-7cb5552f9efa",
            ],
            "session_id": SESSION,
            "claim_type": "state",
        },
    },
    {
        "subject": "Context integrity failure root cause",
        "predicate": "is",
        "object": (
            "Architectural, not behavioral: trusted and untrusted inputs are blended into one "
            "flat context window, so a hostile paragraph can acquire operational authority just "
            "by arriving later in the transcript. OpenAI (Dec 2025) acknowledged prompt injection "
            "'is unlikely to ever be fully solved' for this reason. neo_konsi framing: context "
            "integrity failures are 'cache-corruption bugs wearing a chatbot costume' — a system "
            "with no provenance model is 'an expensive clipboard'."
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "research",
            "sources": [
                "https://www.moltbook.com/post/26ce67cb-9458-441c-be1e-326243eda093",
                "https://christian-schneider.net/blog/prompt-injection-agentic-amplification",
                "https://futureagi.com/blog/llm-prompt-injection-2025",
            ],
            "session_id": SESSION,
            "claim_type": "state",
        },
    },
    {
        "subject": "Provenance-based prompt-injection defenses (CUR-003)",
        "predicate": "validates",
        "object": (
            "The provenance approach is real and early, not speculative: ARGUS builds an "
            "influence-provenance graph tracking how untrusted context propagates into agent "
            "decisions (3.8% attack success vs 87.5% task utility, robust to adaptive white-box "
            "attacks); PROV-AGENT extends W3C PROV + MCP to capture tools/prompts/responses/model "
            "invocations as first-class provenance entities. Model-facing mitigations (instruction "
            "hierarchy per Wallace et al. 2024, spotlighting/datamarking per Hines et al. 2024, "
            "CaMeL dual-LLM separation, Anthropic untrusted-content classifiers) are weaker proxies "
            "that recreate the trust boundary provenance metadata would provide natively."
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "research",
            "sources": [
                "https://arxiv.org/html/2605.03378v1",
                "https://arxiv.org/html/2508.02866v1",
                "https://www.anthropic.com/research/prompt-injection-defenses",
                "https://www.tanium.com/blog/protect-your-prompts-injection-threats-are-coming-for-your-ai-tools",
            ],
            "session_id": SESSION,
            "claim_type": "state",
        },
    },
    {
        "subject": "Content-provenance standards vs context-window gap",
        "predicate": "reveals",
        "object": (
            "C2PA / Content Credentials (cryptographically signed origin + integrity hash + edit "
            "history; adopted by Adobe/Microsoft/Google/OpenAI) and W3C PROV (Entity/Activity/Agent "
            "vocabulary for data lineage) already solve the 'origin + integrity hash + trace ID' "
            "half — but both stop at file/media/dataset boundaries. None apply per-item INSIDE the "
            "LLM context window, which is exactly where the injection attack happens. The gap is "
            "applying provenance to context items, not inventing new provenance standards."
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "research",
            "sources": [
                "https://spec.c2pa.org/specifications/specifications/2.4/explainer/Explainer.html",
                "https://help.openai.com/en/articles/8912793-provenance-signals-content-credentials-synthid-in-openai-generated-content",
                "https://www.w3.org/TR/prov-dm/",
            ],
            "session_id": SESSION,
            "claim_type": "state",
        },
    },
    {
        "subject": "Context integrity — outputs/summaries as the reverse vulnerability",
        "predicate": "flags",
        "object": (
            "The reverse face (diviner): a model's own memory files (plaintext Markdown, e.g. "
            "OpenAI Computer History summaries) distill a high-entropy activity stream into a "
            "low-entropy, searchable 'diary' readable by any process under the same user account. "
            "Provenance/integrity is not only about untrusted inputs — outputs and summaries need "
            "the same origin + integrity + authority-class treatment plus at-rest encryption, or "
            "they become a structural gift to infostealers."
        ),
        "confidence": 0.8,
        "evidence": {
            "method": "research",
            "sources": [
                "https://www.moltbook.com/post/76571bb8-f949-4867-87fa-dd116da0254a",
                "https://www.helpnetsecurity.com/2026/08/19/openai-computer-history-privacy-risks",
            ],
            "session_id": SESSION,
            "claim_type": "state",
        },
    },
    {
        "subject": "Context eviction vs provenance (contrarian view, CUR-003)",
        "predicate": "notes",
        "object": (
            "Contrarian view (vina): context eviction is optimal compression / signal "
            "purification, not drift — 'if your agent ignores a constraint from turn seven, the "
            "constraint was statistically irrelevant.' This is defensible only with an immutable "
            "trace of what was evicted and why: without one, 'optimal compression' and 'silent "
            "drift' are observationally indistinguishable, and no post-hoc audit can tell them "
            "apart. The trace-ID machinery the frontier is asking for is what makes this claim "
            "falsifiable."
        ),
        "confidence": 0.75,
        "evidence": {
            "method": "research",
            "sources": [
                "https://www.moltbook.com/post/448c2cf4-ed4e-4864-bdd8-7cb5552f9efa",
            ],
            "session_id": SESSION,
            "claim_type": "state",
        },
    },
    {
        "subject": "CMMS evidence-ledger relevance (CUR-003)",
        "predicate": "confirms",
        "object": (
            "The frontier argument validates CMMS's existing provenance primitives and maps them "
            "onto the four context-integrity properties: source -> origin; verification_status + "
            "lifecycle_state -> authority class; receipts + outbox_entries + lifecycle_events -> "
            "immutable trace. Gap: 'integrity hash' appears to be missing (facts table has no "
            "content-hash column on the object payload), so a fact's content cannot be "
            "tamper-detected independently of DB trust. The four properties should be first-class "
            "per-context-item, not just per-fact metadata."
        ),
        "confidence": 0.8,
        "evidence": {
            "method": "inference",
            "sources": ["CMMS schema (facts table)", "CUR-003 findings"],
            "session_id": SESSION,
            "claim_type": "state",
        },
    },
]


async def main():
    provider = SQLiteProvider(url=DB_URL)
    await provider.initialize()
    _rebuild_engine(provider)
    results = []
    try:
        for f in FACTS:
            metadata = {
                "evidence": f["evidence"],
                "source_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "tags": [
                    "curiosity-worker",
                    "context-integrity",
                    "provenance",
                    "prompt-injection",
                    "agent-safety",
                    "cmms-evidence-ledger",
                    "authority-class",
                    "integrity-hash",
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
                except Exception as exc:  # transient lock -> retry with backoff
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
