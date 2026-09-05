"""Curiosity worker: save CUR-MELD-TEMPORAL-SEMANTICS-000 findings to CMMS via remember()."""
import asyncio
from datetime import datetime, timezone

from _common import get_db_url

from memory_server.api.remember import remember
from memory_server.providers.sqlite_provider import SQLiteProvider

DB_URL = get_db_url()
SOURCE = "curiosity-worker/research"
SESSION = "cron_20260818_curiosity_meld_temporal_semantics"

ARXIV_ZEP = "https://arxiv.org/abs/2501.13956"
MNEMOVERSE = "https://mnemoverse.com/docs/library/bitemporal-memory-for-ai-agents"
ZEP_TKG = "https://www.getzep.com/ai-agents/temporal-knowledge-graph"
WIKI_TEMPORAL = "https://en.wikipedia.org/wiki/Temporal_database"
FOWLER = "https://martinfowler.com/articles/bitemporal-history.html"
ALLEN = "https://qsrlib.readthedocs.io/en/latest/rsts/handwritten/qsrs/allen.html"
FACT_DURATION = "https://arxiv.org/abs/2305.14824"
TVCP = "https://arxiv.org/abs/2401.00779"
CHRONOSENSE = "https://arxiv.org/abs/2501.03040"
TEST_OF_TIME = "https://arxiv.org/abs/2406.09170"
REPORT = "~/.hermes/workspace/findings/cur-meld-temporal-semantics-000.md"

FACTS = [
    {
        "subject": "Bi-temporal data model (valid time vs transaction time, Snodgrass/Ahn 1980s, SQL:2011)",
        "predicate": "defines",
        "object": (
            "two orthogonal time axes for every fact: VALID TIME = when the fact was true in the world; "
            "TRANSACTION TIME = when the system recorded it. The axes are independent — an agent learning on "
            "Friday that a contract ended Monday has valid-time boundary Monday, transaction-time boundary "
            "Friday; collapsing them into one field loses the distinction between a late update and an event "
            "that happened late. Standardized in SQL:2011 (system-versioned tables = transaction time; "
            "application-time period tables = valid time). Fowler's practitioner terms: 'actual' (valid) vs "
            "'record' (transaction). Degenerate cases: snapshot (neither) / historical (valid only) / rollback "
            "(transaction only) / bitemporal (both)."
        ),
        "confidence": 0.97,
        "evidence": {"method": "web_search", "sources": [WIKI_TEMPORAL, FOWLER], "session_id": SESSION},
    },
    {
        "subject": "Four temporal validity types of a fact (VITA, arXiv 2025)",
        "predicate": "enumerates",
        "object": (
            "a fact's validity has four shapes, mappable to open/closed interval endpoints: SINCE (valid_from "
            "known, valid_to open/null — 'works at X since 2020'); UNTIL (valid_from open, valid_to known — "
            "'student until 2019'); PERIOD (both known — 'lived in Berlin 2015-2019'); TIME-INVARIANT (both null "
            "= never — 'born 1985', identity facts). This is the minimal schema: two nullable timestamps where "
            "null has typed meaning (open-ended / unknown / never), not a missing value. Point-based TKGs are "
            "the degenerate from==to case."
        ),
        "confidence": 0.88,
        "evidence": {"method": "web_search", "sources": [REPORT], "session_id": SESSION},
    },
    {
        "subject": "Allen's Interval Algebra (13 pairwise-disjoint, jointly-exhaustive interval relations)",
        "predicate": "is",
        "object": (
            "the machinery for deciding when two temporal facts conflict vs coexist: before/after and meets/"
            "met_by (non-overlapping) -> COEXIST as move history (sequential, never a conflict); overlaps/"
            "starts/finishes/equals (overlapping) -> CONFLICT-candidate (escalate to object comparator + "
            "authority); during/contains (containment) -> REFINE-candidate (inner fact subsumes/is more specific "
            "than outer). Temporal non-overlap is SUFFICIENT to rule out conflict; temporal overlap is only "
            "NECESSARY to consider it — object incompatibility is still decided by the predicate/NLI layer. "
            "Two facts with the same (s,p,o) prefix but different objects are a conflict only if their valid-time "
            "intervals overlap AND their objects are incompatible."
        ),
        "confidence": 0.95,
        "evidence": {"method": "web_search", "sources": [ALLEN], "session_id": SESSION},
    },
    {
        "subject": "Temporal aspect classes (current/habitual/past/temporary/permanent)",
        "predicate": "distinguishes",
        "object": (
            "bi-temporal machinery gives WHEN a fact is true but not WHAT KIND of truth it asserts; linguistic "
            "aspect fills the gap: CURRENT STATE (fluent holding now, valid [observed_at, null)); HABITUAL/GENERIC "
            "('usually/typically', open epistemic, NOT invalidated by a single counter-observation — needs a "
            "frequency threshold); PAST (closed episode [from,to]); TEMPORARY (short-lived, [from,predicted_to], "
            "expires by TTL); PERMANENT/IDENTITY (time-invariant [null,null], only changed by authority overrule). "
            "Knowledge-representation lineage = the FLUENT (situation calculus / event calculus): a time-varying "
            "property of the world, contrasted with RIGID designators (name, birth date) that do not vary with "
            "situation. 'Lives in X' is a non-rigid fluent; 'born in 1985' is rigid."
        ),
        "confidence": 0.9,
        "evidence": {"method": "inference", "sources": [REPORT], "session_id": SESSION},
    },
    {
        "subject": "Residence vs location predicate split (fix for the motivating 'Murmansk vs Moscow' edge case)",
        "predicate": "recommends",
        "object": (
            "stop flattening distinct fluents into one predicate. Adopt at least three: resides_in/home_location "
            "(habitual residence, sticky, supersede-on-move); located_at/current_location (current position, "
            "transient, short TTL); moved_to/used_to_reside_in (past episode, closed interval). Under this split, "
            "'resides_in = Murmansk' and 'located_at = Moscow' are NOT even candidate contradictions (different "
            "predicates, no contradiction verdict fires); two resides_in facts with non-overlapping intervals are "
            "a MOVE (sequential), not a conflict; only two resides_in facts with overlapping intervals and "
            "different objects are a genuine conflict for the reconcile layer."
        ),
        "confidence": 0.9,
        "evidence": {"method": "inference", "sources": [REPORT], "session_id": SESSION},
    },
    {
        "subject": "Zep/Graphiti bi-temporal edge invalidation procedure (arXiv 2501.13956 §2.2.3)",
        "predicate": "is",
        "object": (
            "the load-bearing reference implementation — the only agent-memory system with a paper-documented "
            "bi-temporal model (surveyed July 2026; Mem0 dropped its graph backend in SDK v2.0.0, Cognee is "
            "date-range filtering, Supermemory hand-rolls string tags). Each edge carries four timestamps: "
            "valid_from (t_valid), valid_to (t_invalid), observed, recorded (t'_created/t'_expired). Invalidation: "
            "'when the system identifies temporally overlapping contradictions, it invalidates the affected edges "
            "by setting t_invalid to the t_valid of the invalidating edge' — INVALIDATION NOT DELETION (history "
            "preserved), close-on-overlap, record-time as provenance. Known sharp edge (open Graphiti issue): "
            "out-of-order backfill arrival makes naive invalidation expire the correct newer edge along with the "
            "stale one — invalidation must key on valid time, never arrival order."
        ),
        "confidence": 0.95,
        "evidence": {"method": "web_search", "sources": [ARXIV_ZEP, MNEMOVERSE, ZEP_TKG], "session_id": SESSION},
    },
    {
        "subject": "Date math must be executed not generated (LLM temporal-arithmetic failure rates)",
        "predicate": "requires",
        "object": (
            "measured models are too fragile to compute temporal predicates themselves: Gemini 1.5 Pro, GPT-4, "
            "Claude 3 Sonnet score 13.5-16% on duration/date arithmetic, with the most common error exactly one "
            "day off (Test of Time, arXiv 2406.09170); GPT-4o scores 0.69 zero-shot on the Allen 'Equals' relation "
            "vs 0.91-0.96 on Before/After, collapsing to ~0.45 under abstract labels — the model leans on memorized "
            "world knowledge, not interval reasoning (ChronoSense, arXiv 2501.03040); MATRES restricted temporal "
            "labels to event start points because interval end-points confused human annotators (ACL 2018). "
            "Consequence: CMMS must store normalized intervals and evaluate overlap/containment/Allen relations in "
            "code or SQL, letting the model interpret the request rather than improvise the calendar."
        ),
        "confidence": 0.93,
        "evidence": {"method": "web_search", "sources": [CHRONOSENSE, TEST_OF_TIME, MNEMOVERSE], "session_id": SESSION},
    },
    {
        "subject": "Predict staleness at write time (fact duration prediction)",
        "predicate": "enables",
        "object": (
            "moving a memory store from REACTIVE invalidation (wait for a contradiction) to MAINTAINED correctness: "
            "Fact Duration Prediction (EMNLP 2023, arXiv 2305.14824) predicts how long a fact will remain true and "
            "flags volatile facts under temporal misalignment; Temporal Validity Change Prediction (Findings ACL "
            "2024, arXiv 2401.00779) estimates a statement's validity duration from creation time using prior world "
            "knowledge of typical event durations. As of July 2026 no agent-memory system integrates either. CMMS "
            "already has static write-time expiry via MemoryTag TTL (EPHEMERAL=1d, DURABLE=365d, IMPORTANT=inf) in "
            "admission.py — a predicate-aware duration predictor upgrades this from static to learned expiry."
        ),
        "confidence": 0.88,
        "evidence": {"method": "web_search", "sources": [FACT_DURATION, TVCP, MNEMOVERSE], "session_id": SESSION},
    },
    {
        "subject": "CMMS temporal-semantics policy (deliverable)",
        "predicate": "recommends",
        "object": (
            "(1) ADD FIELDS to facts: valid_from (datetime|null), valid_to (datetime|null), observed_at (datetime, "
            "default created_at), temporal_aspect enum (current|habitual|past|temporary|permanent); created_at/"
            "updated_at already serve as transaction time; skip decision_time (tri-temporal is a complexity tax "
            "CMMS doesn't need yet). (2) PREDICATE RIGIDITY BUCKETS: rigid/time-invariant (is_a, born_on, named — "
            "never invalidated, authority-overrule only); sticky state DEFAULT (resides_in, works_at, owns, prefers "
            "— valid_from=observed_at, valid_to=null, supersede-on-change); transient (located_at, in_meeting_with, "
            "feels — predicted TTL, aspect=temporary). (3) CONFLICT/REFINE/COEXIST: different predicates -> coexist "
            "always; non-overlapping intervals -> coexist as move history; containment -> refine (subsumption link); "
            "overlapping+incompatible objects -> conflict (MELD R3, no silent winner); overlapping+compatible+"
            "differing aspect -> coexist with distinct temporal_aspect. (4) INVALIDATION-ON-OVERLAP: close "
            "old.valid_to = new.valid_from, write a lifecycle_event (activates dormant lifecycle machinery — 0 "
            "events today), key on valid time not arrival order. (5) the MELD claim-key H(canon(s,p,o)||scope) must "
            "NOT include time fields — time is scoped separately so same-identity claims at different times still "
            "collide for comparison."
        ),
        "confidence": 0.85,
        "evidence": {"method": "inference", "sources": [REPORT, ARXIV_ZEP], "session_id": SESSION},
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
                    "meld",
                    "agent-memory",
                    "temporal-semantics",
                    "bi-temporal",
                    "valid-time",
                    "allen-interval-algebra",
                    "graphiti",
                    "fluent",
                    "cmms",
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
