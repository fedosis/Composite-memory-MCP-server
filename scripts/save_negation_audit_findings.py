"""Curiosity worker: save CUR-002 (SOUL.md + AGENTS.md negation audit) findings to CMMS via remember()."""
import asyncio
from datetime import datetime, timezone

from _common import get_db_url

from memory_server.api.remember import remember
from memory_server.providers.sqlite_provider import SQLiteProvider

DB_URL = get_db_url()
SOURCE = "curiosity-worker"
SESSION = "cron_20260819_curiosity_negation_audit"

FACTS = [
    {
        "subject": "SOUL.md + AGENTS.md negation audit (CUR-002)",
        "predicate": "classifies",
        "object": (
            "~30 negation sites across SOUL.md and AGENTS.md, split into two buckets: "
            "REWRITE (14 rules have a clean, equally-clear positive equivalent) vs KEEP "
            "(negation is clearest/most compact, or no clean positive equivalent exists). "
            "Verdict: 'unless X' -> 'only when X' and 'without X' -> 'with X' transforms are "
            "zero-loss and highest-value; security red lines and exclusion enumerations stay "
            "negated as hard stop-signals, consistent with CUR-001."
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "inference",
            "sources": ["~/.hermes/SOUL.md", "~/.hermes/AGENTS.md", "CUR-001"],
            "session_id": SESSION,
            "claim_type": "state",
        },
    },
    {
        "subject": "Negated rules with clean positive rewrites (CUR-002)",
        "predicate": "drafts",
        "object": (
            "SOUL.md: (1) no_unsolicited_next_steps 'Do not propose next steps, follow-ups, or "
            "side tasks unless explicitly asked' -> 'Propose next steps, follow-ups, or side tasks "
            "only when explicitly asked'; (2) no_recap_by_default 'Do not summarize completed work "
            "unless the user asked' -> 'Summarize completed work only when the user asked'; "
            "(3) usefulness_is_scope_bound 'Being helpful does not mean expanding the task' -> "
            "'Being helpful means staying within the requested scope'; (4) 'Do not restate the "
            "obvious' -> 'Say only what adds information'; (5) 'Do not sound corporate. Do not "
            "sound needy.' -> 'Sound plain-spoken and self-assured.'; (6) 'Never send half-baked "
            "replies.' -> 'Send only complete, considered replies.'. AGENTS.md: (7) 'Don't run "
            "destructive commands without asking' -> 'Ask before running destructive commands'; "
            "(8) reread_rules 'Do NOT manually reread startup files unless:' -> 'Reread startup "
            "files only when:'; (9) 'Без approve не merge' -> 'Merge только после approve'; "
            "(10) 'You're not sure whether to respond — this means don't' -> 'When unsure, stay "
            "silent.'; (11) 'Don't respond multiple times' -> 'Respond once.'; (12) 'Do not store "
            "raw excerpts ... unless Fedos explicitly asks' -> 'Store raw excerpts only when Fedos "
            "explicitly asks for a quoted audit.'; (13) 'Не смешивать пояснения и контент в одном "
            "сообщении' -> 'Держать пояснения и контент в разных сообщениях.'; (14) 'Не дублировать' "
            "-> 'Проверять на дубли перед добавлением.'"
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "inference",
            "sources": ["~/.hermes/SOUL.md", "~/.hermes/AGENTS.md"],
            "session_id": SESSION,
            "claim_type": "state",
        },
    },
    {
        "subject": "Negations to keep as-is (CUR-002)",
        "predicate": "retains",
        "object": (
            "Security hard stop-signals: 'Do not fabricate facts, outputs, prices, availability, "
            "or certainty'; 'Do not type or reveal passwords, API keys, payment data, or secrets'; "
            "'Don't exfiltrate private data. Ever.' — positive equivalents ('report only verified', "
            "'keep secrets out of output') are weaker (fabricate != fail-to-verify) or lose "
            "stop-signal force. Exclusion enumerations: 'Do not use SOUL.md/MEMORY.md/AGENTS.md as "
            "durable fact storage'; 'no terminal output, browser output, or tool chatter' in group "
            "chats; 'No markdown tables'; 'No headers' — positive equivalents lose per-file/channel "
            "specificity. Idiomatic/technical: 'If you do not know, say so plainly'; 'not "
            "retrievable'; 'not their voice, not their proxy'; 'don't just reply HEARTBEAT_OK'."
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "inference",
            "sources": ["~/.hermes/SOUL.md", "~/.hermes/AGENTS.md"],
            "session_id": SESSION,
            "claim_type": "state",
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
                    "negation",
                    "positive-reformulation",
                    "prompt-engineering",
                    "soul-md",
                    "agents-md",
                    "rule-audit",
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
