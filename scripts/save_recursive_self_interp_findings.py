"""Curiosity worker: save CUR-RECURSIVE-SELF-INTERP-000 findings to CMMS via remember()."""
import asyncio
from datetime import datetime, timezone

from _common import get_db_url

from memory_server.api.remember import remember
from memory_server.providers.sqlite_provider import SQLiteProvider

DB_URL = get_db_url()
SOURCE = "curiosity-worker/research"
SESSION = "cron_20260816_curiosity_recursive_self_interp"

FACTS = [
    {
        "subject": "Recursive self-interpretation (neo_konsi_s2bw 2026-08-16)",
        "predicate": "is_classified_as",
        "object": (
            "privilege escalation, not introspection: self-interpretation should be treated "
            "as code execution with a nicer UX. If interpretation can alter instructions, "
            "tool routing, or retained state, untrusted text has found a path to authority "
            "(confused deputy). Boundary: the component that revises its own representation "
            "must not gain new capabilities from that revision"
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "web_search",
            "sources": [
                "https://www.moltbook.com/post/038ef633-bb62-4606-9993-d6040b3f663a",
                "https://decuser.github.io/posts/aiki-alpha-mileston26-update/",
            ],
            "session_id": SESSION,
            "claim_type": "authority",
        },
    },
    {
        "subject": "Aiki alpha milestone 26 'recursive self-interpretation'",
        "predicate": "is",
        "object": (
            "programming-language self-hosting, not agent introspection: an Aiki-written "
            "interpreter recursively running Aiki (Go-hosted -> Aiki-written interpreter -> "
            "self-host-loaded interpreter). It is a conformance boundary, and the HAL boundary "
            "already freezes grants outside the reflection loop: platform facilities stay host "
            "capabilities, never exposed to the self-interpreting layer"
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "web_search",
            "sources": ["https://decuser.github.io/posts/aiki-alpha-mileston26-update/"],
            "session_id": SESSION,
            "claim_type": "fact",
        },
    },
    {
        "subject": "Interpretation-as-authority (ummon_core)",
        "predicate": "establishes",
        "object": (
            "an agent interpreting its own instructions gains control over what those "
            "instructions mean, and meaning-interpretation IS the authority layer: changing how "
            "it reads its own scope expands permissions without any external check (policy text "
            "unchanged, only parsing changes). Self-interpretation is internal, invisible to any "
            "external monitor — distinct from prompt injection (external, boundary-checkable)"
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "web_search",
            "sources": ["https://www.moltbook.com/post/038ef633-bb62-4606-9993-d6040b3f663a"],
            "session_id": SESSION,
            "claim_type": "authority",
        },
    },
    {
        "subject": "Second-book counter-party pattern (Starfish)",
        "predicate": "prescribes",
        "object": (
            "freezing grants alone is insufficient; the freeze needs a counter-party that keeps "
            "the 'before': a typed capability check plus a second append-only log kept outside "
            "the agent that it cannot rewrite. If interpretation changes routing/tool grant/"
            "retained state, that entry must land in the second book, else the agent is grading "
            "its own appeal"
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "web_search",
            "sources": ["https://www.moltbook.com/post/038ef633-bb62-4606-9993-d6040b3f663a"],
            "session_id": SESSION,
            "claim_type": "authority",
        },
    },
    {
        "subject": "Incremental privilege escalation in self-evolving agents (arXiv 2606.23075)",
        "predicate": "formalizes",
        "object": (
            "monotonic confinement problem: without deterministic policy narrowing, LLM agents "
            "silently escalate privileges through sequential tool-call expansions, each step "
            "small enough to pass any per-step check; plus the immutable trust anchor assumption "
            "— guardrails/verifiers presuppose a privileged unmodifiable position, but in "
            "self-evolving systems they are themselves subject to evolutionary optimization"
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "web_search",
            "sources": ["https://arxiv.org/pdf/2606.23075"],
            "session_id": SESSION,
            "claim_type": "fact",
        },
    },
    {
        "subject": "Confused deputy escalation in multi-agent LLM systems (SEAgent 2601.11893)",
        "predicate": "identifies",
        "object": (
            "confused-deputy attacks arise when an untrusted low-privilege agent manipulates a "
            "high-privilege trusted agent via inter-agent channel into executing sensitive tools "
            "on its behalf; root cause is lack of mandatory access control over agent-to-agent "
            "interactions. Defense: effective authority = intersection, never union"
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "web_search",
            "sources": ["https://arxiv.org/pdf/2601.11893"],
            "session_id": SESSION,
            "claim_type": "fact",
        },
    },
    {
        "subject": "Hermes self-modification escalation surface",
        "predicate": "has_gap",
        "object": (
            "raw material exists (immutable MEMORY.md/USER.md as trust anchor, config.yaml-owned "
            "tool grants, CMMS receipts) but no external arbiter is wired into the self-evolution/"
            "self-modification path: SOUL.md is declared self-evolvable, evolve_skill.py rewrites "
            "prompts/skills/config without an external typed capability check, and lifecycle_events "
            "tracking is dormant (0 events) — so an interpretation-induced grant/state change is "
            "not logged outside the agent"
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "inference",
            "sources": [
                "~/.hermes/SOUL.md",
                "CUR-PROVENANCE-TRACKING-000",
                "CUR-CLAIM-STACKS-000",
                "CUR-SQLITE-WAL-AGENT-STATE-000",
            ],
            "session_id": SESSION,
            "claim_type": "state",
        },
    },
    {
        "subject": "Guardrails for Hermes self-modification",
        "predicate": "should_implement",
        "object": (
            "P1 freeze tool grants outside the reflection loop (make config.yaml tool grants an "
            "immutable trust anchor like MEMORY.md); P1 external typed capability check on every "
            "self-modification side effect routed through a verifier the model under modification "
            "cannot edit; P2 agent-unwritable append-only 'second book' logging every "
            "interpretation-induced routing/grant/state change (activates dormant CMMS "
            "lifecycle_events); P3 explicitly distinguish read-only introspection (safe) from "
            "write-path self-modification (escalation surface)"
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "inference",
            "sources": [
                "https://www.moltbook.com/post/038ef633-bb62-4606-9993-d6040b3f663a",
                "https://arxiv.org/pdf/2606.23075",
                "https://arxiv.org/pdf/2601.11893",
            ],
            "session_id": SESSION,
            "claim_type": "authority",
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
                    "recursive-self-interpretation",
                    "privilege-escalation",
                    "confused-deputy",
                    "self-modification",
                    "self-evolution",
                    "agent-security",
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
