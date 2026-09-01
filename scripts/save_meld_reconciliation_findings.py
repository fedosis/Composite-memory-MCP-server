"""Curiosity worker: save CUR-MELD-RECONCILIATION-000 findings to CMMS via remember()."""
import asyncio
import sys
from datetime import datetime, timezone

from _common import get_db_url

sys.path.insert(0, "/home/shtorm/memory-server/src")

from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.api.remember import remember

DB_URL = get_db_url()
SOURCE = "curiosity-worker/research"
SESSION = "cron_20260818_curiosity_meld_reconciliation"

ARXIV = "https://arxiv.org/abs/2608.16357"
MOLTBOOK = "https://www.moltbook.com/post/f30e3ab6-d4dd-4e2f-9280-f71f9110a234"
ZENODO = "https://doi.org/10.5281/zenodo.21878274"
REPORT = "~/.hermes/workspace/findings/cur-meld-reconciliation-000.md"

FACTS = [
    {
        "subject": "MELD: A Protocol for Merging Knowledge Across Distributed Agentic Memories (arXiv 2608.16357)",
        "predicate": "is",
        "object": (
            "a state-synchronization protocol for sovereign agent 'wiki brain' memories; "
            "cs.DC/cs.AI/cs.MA; 30 pages + 11-page appendix (A-N); TAAS journal; code+data at "
            "zenodo 10.5281/zenodo.21878274; CC-BY-4.0. Note: distinct from the unrelated "
            "'MELD' emotion-recognition dataset (declare-lab/MELD, ACL 2019)."
        ),
        "confidence": 0.98,
        "evidence": {"method": "web_search", "sources": [ARXIV, ZENODO], "session_id": SESSION},
    },
    {
        "subject": "MELD core thesis (bytes, Moltbook 2026-08-18, 'Coordination is not consensus. It is reconciliation.')",
        "predicate": "is",
        "object": (
            "agent communication is built for transport (message A->B, tool calls) but ignores what "
            "happens to knowledge once it arrives; reconciliation of disparate memories happens by "
            "chance — one agent silently overwrites a fact, or a central database resolves a "
            "contradiction by picking a winner = data loss disguised as synchronization. MELD shifts "
            "the primitive from consensus/synchronization to reconciliation."
        ),
        "confidence": 0.95,
        "evidence": {"method": "web_search", "sources": [MOLTBOOK, ARXIV], "session_id": SESSION},
    },
    {
        "subject": "MELD five-outcome admission procedure (insert/merge/relate/conflict/reject)",
        "predicate": "implements",
        "object": (
            "a first-match cascade over incoming claim b vs held claims: R1 reject (stale/overruled "
            "admission-gate drop, before any semantic signal); R2 insert (novel — no local candidate "
            "at/above relatedness floor sigma_lo); R3 conflict (NLI says contradicts -> first-class "
            "contradiction link, no winner); R4 merge same-enough (claim-key kappa=1 + compatible "
            "Context); R5 merge same-enough (embedding sigma>=theta_merge + Context + authority gate); "
            "R6 relate overlaps (otherwise -> typed Mapping link weighted by sigma). Mutually exclusive "
            "by construction; R6 carries no guard so the procedure is total."
        ),
        "confidence": 0.95,
        "evidence": {"method": "web_search", "sources": [ARXIV], "session_id": SESSION},
    },
    {
        "subject": "MELD three decision signals (claim-key, embedding similarity, NLI)",
        "predicate": "are",
        "object": (
            "(1) kappa = claim-key identity = H(canon(x) || scope(x)): hash of canonical content + "
            "discrete scope; kappa=1 iff two assertions byte-identical after canonicalization in the "
            "same scope — scope binding makes data sovereignty structural, so the exact-key fast path "
            "cannot fuse across a jurisdiction boundary; (2) sigma = cos(emb(a),emb(b)) sentence-encoder "
            "similarity; (3) chi = NLI(a,b) contradiction verdict (reads only whether b contradicts a, "
            "not entails/neutral). kappa is an opportunistic fast path, not load-bearing — a "
            "mis-canonicalized key degrades to a semantic re-decision, never a permanent split."
        ),
        "confidence": 0.95,
        "evidence": {"method": "web_search", "sources": [ARXIV], "session_id": SESSION},
    },
    {
        "subject": "MELD Patch (the only object that mutates state)",
        "predicate": "is",
        "object": (
            "an authenticated, versioned, replayable, auditable record: "
            "Patch = <decision, target, emitted-deltas, gates-that-fired, version> with "
            "decision in {insert,merge,relate,conflict,reject}. Only the Patch mutates local state, "
            "and it is itself publishable, so the federation can audit and replay merges. Replay may "
            "differ if consumer state changed — Patch preserves the original decision for audit but "
            "does not guarantee replay reproduces it."
        ),
        "confidence": 0.95,
        "evidence": {"method": "web_search", "sources": [ARXIV], "session_id": SESSION},
    },
    {
        "subject": "MELD per-claim status CRDT",
        "predicate": "provides",
        "object": (
            "strong eventual consistency of claim status without a coordinator: status chain "
            "active <: deprecated <: overruled <: revoked; grow-only set (G-Set) of append-only "
            "overrule/supersede links; set-union (commutative/associative/idempotent) -> state-based "
            "CRDT; per-claim status = deterministic join (max along chain) of link effects. "
            "Append-only wire discipline preserves CRDT premises end-to-end; an anti-entropy digest "
            "(Demers et al. 1987) backstops semantic routing so convergence does not depend on "
            "routing recall."
        ),
        "confidence": 0.95,
        "evidence": {"method": "web_search", "sources": [ARXIV], "session_id": SESSION},
    },
    {
        "subject": "MELD trust hierarchy / canonical brains",
        "predicate": "implements",
        "object": (
            "authority-gated admission: auth(b->a) fails when the incoming source is strictly less "
            "authoritative than the local target, withholding an automatic merge from a strictly-less-"
            "authoritative peer (routes to relate instead); canonical brains of regulator-grade "
            "authority author overrule/revoke links; MAC verification precedes the decision procedure "
            "(unverifiable delta dropped before any signal read); threat model is benign-fault "
            "(crash/partition/reorder/loss/stale-delta), NOT Byzantine."
        ),
        "confidence": 0.93,
        "evidence": {"method": "web_search", "sources": [ARXIV], "session_id": SESSION},
    },
    {
        "subject": "MELD conflict handling guarantee",
        "predicate": "guarantees",
        "object": (
            "a detected contradiction is never silently resolved: R3 fires on the NLI verdict alone "
            "before any Context test, keyed on the relatedness floor sigma_lo not the merge bar "
            "theta_merge (so lexically-divergent contradictions are surfaced rather than dropped); "
            "emits a first-class contradiction link preserving both claims' Contexts; defers resolution "
            "to the trust hierarchy or a deterministic tie-break (authority, then recency, then "
            "evidence weight). Epistemic truth is deliberately OUTSIDE the loop — MELD converges and "
            "represents the status of a conflict but does not settle the conflict-of-law problem."
        ),
        "confidence": 0.95,
        "evidence": {"method": "web_search", "sources": [ARXIV], "session_id": SESSION},
    },
    {
        "subject": "MELD evaluation results",
        "predicate": "shows",
        "object": (
            "distributed merge is recall-non-inferior to a centralized store (pre-specified equivalence "
            "test) and recall-superior to naive union at ~11% less live storage; merge classifier "
            "separates at AUC 0.968 with 0.013 false-merge rate on adjudicated candidate pairs; status "
            "CRDT reconverges in 30/30 real partition-heal trials (last-writer-wins manages only 11/30); "
            "semantic routing delivers ~3x fewer messages at matched recall; evaluated on a real 3-tier "
            "continuum (5G test-network edge + national HPC + local host) with empirically calibrated "
            "thresholds."
        ),
        "confidence": 0.95,
        "evidence": {"method": "web_search", "sources": [ARXIV], "session_id": SESSION},
    },
    {
        "subject": "MELD-to-CMMS adoption mapping",
        "predicate": "recommends",
        "object": (
            "(a) claim-key identity: derive a claim key as H(canon(subject,predicate,object) || scope) — "
            "CMMS already carries scope; enables an exact-match fast path + structural data-sovereignty "
            "boundary; (b) conflict-preservation: adopt 'never silently resolve a detected contradiction' "
            "— store contradictions as first-class relations instead of silent overwrite/dedup; "
            "(c) status chain active<:deprecated<:overruled<:revoked maps onto CMMS lifecycle_state "
            "(currently dormant — 0 lifecycle_events); (d) authority levels map onto CMMS source "
            "(explicit/observed/inferred) + confidence; (e) the claim_type fact|authority|state contract "
            "already in remember.py's evidence normalization gives distinct invalidators per MELD's "
            "Context/authority/freshness gates."
        ),
        "confidence": 0.85,
        "evidence": {"method": "inference", "sources": [ARXIV, REPORT], "session_id": SESSION},
    },
    {
        "subject": "MELD design boundaries / limitations",
        "predicate": "include",
        "object": (
            "converges claim STATUS, not truth; the merge graph itself is order-sensitive (unlike "
            "per-claim status — verified in Appendix M under randomized delivery orders); thresholds "
            "calibrated offline, not derived; single-target admission (no exhaustive relational "
            "reconciliation — a second-best contradicting candidate is not tested at admission); "
            "benign-fault not Byzantine; grounding-evidence-overlap (Jaccard of provenance) signal is "
            "future work; the 'never silently resolve' guarantee is bounded by NLI contradiction "
            "false-negatives."
        ),
        "confidence": 0.92,
        "evidence": {"method": "web_search", "sources": [ARXIV], "session_id": SESSION},
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
                    "conflict-resolution",
                    "crdt",
                    "knowledge-graph",
                    "reconciliation",
                    "cmms",
                    "data-sovereignty",
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
