"""Curiosity worker: save CUR-MELD-PREDICATE-TAXONOMY-000 findings to CMMS via remember()."""
import asyncio
import sys
from datetime import datetime, timezone

from _common import get_db_url

sys.path.insert(0, "/home/shtorm/memory-server/src")

from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.api.remember import remember

DB_URL = get_db_url()
SOURCE = "curiosity-worker/research"
SESSION = "cron_20260818_curiosity_meld_predicate_taxonomy"

OWL_REF = "https://www.w3.org/TR/owl-ref"
WIKIDATA_CONSTRAINTS = "https://www.wikidata.org/wiki/Help:Property_constraints_portal"
NATLOG = "https://nlp.stanford.edu/~manning/papers/natlog-cm.pdf"
MELTWATER = "https://underthehood.meltwater.com/blog/2020/06/29/the-record-linking-pipeline-for-our-knowledge-graph-part-1"
TMS_WIKI = "https://en.wikipedia.org/wiki/Reason_maintenance"
SEP_DEFEASIBLE = "https://plato.stanford.edu/entries/reasoning-defeasible"
LEX_SPECIALIS = "https://en.wikipedia.org/wiki/Lex_specialis"
REPORT = "~/.hermes/workspace/findings/cur-meld-predicate-taxonomy-000.md"

FACTS = [
    {
        "subject": "NLI 3-way verdict is the wrong grain for memory reconciliation",
        "predicate": "establishes",
        "object": (
            "MELD's chi signal reads only entail/neutral/contradict, but two memory claims stand in at least "
            "seven distinct logical relations, and which one holds is decided FIRST by the predicate class, "
            "SECOND by how objects are compared, and only THIRD by surface contradiction. NLI cannot distinguish: "
            "(a) move history (sequential 'lives in Murmansk' vs 'lives in Moscow'), (b) habitual-vs-current aspect "
            "difference, (c) different predicates (resides_in vs located_at), or (d) genuine conflict (overlapping + "
            "incompatible) -- it returns 'contradict' for all four. The fix is a predicate taxonomy: typed comparators "
            "that compute set-theoretic relations directly for structured predicates, reserving the LLM/NLI classifier "
            "only for unstructured prose."
        ),
        "confidence": 0.92,
        "evidence": {"method": "inference", "sources": [REPORT], "session_id": SESSION},
    },
    {
        "subject": "OWL property characteristics (W3C 2004) formalize the single-value vs multi-value dimension",
        "predicate": "provides",
        "object": (
            "owl:FunctionalProperty = at most ONE value per subject; the entailment rule 'if S P O1 and S P O2 and "
            "P is functional then O1 owl:sameAs O2' makes two distinct literal values an INCONSISTENCY (conflict). "
            "owl:InverseFunctionalProperty = value uniquely identifies subject (identity key -> merge). "
            "owl:SymmetricProperty / owl:TransitiveProperty give consistency checks and closure, not conflict. "
            "This is the formal foundation for the arity split: born_on/current_status/resides_in are functional "
            "(single-value -> two values = conflict); likes/owns/friend_of/worked_at are non-functional "
            "(multi-value -> two values = coexist)."
        ),
        "confidence": 0.97,
        "evidence": {"method": "web_search", "sources": [OWL_REF], "session_id": SESSION},
    },
    {
        "subject": "Wikidata property constraints are a production-grade typed-comparator library",
        "predicate": "is",
        "object": (
            "Wikidata runs the largest human-curated set of typed property constraints on a live KG and the vocabulary "
            "maps ~1:1 onto reconcile-layer comparators: single-value (Q19474404) = functional/rigid predicates; "
            "distinct-values (Q21502410) = inverse-functional/identity keys; conflicts-with (Q21502838) = inter-"
            "predicate gate; difference-within-range (Q21502408) = NUMERIC TOLERANCE comparator; multi-value "
            "(Q21510857) = set predicates; single-best-value (Q52060874) = PARTIAL OVERRULE (multiple allowed, one "
            "preferred, losers ranked not revoked); separator (P4155) = composite key; allowed-units (Q21514353) = "
            "unit-normalization pre-step for numeric comparison."
        ),
        "confidence": 0.95,
        "evidence": {"method": "web_search", "sources": [WIKIDATA_CONSTRAINTS], "session_id": SESSION},
    },
    {
        "subject": "MacCartney & Manning natural logic 7-way entailment relations (B)",
        "predicate": "defines",
        "object": (
            "the conflict-class space richer than NLI's 3-way, with set-theoretic definitions over denotations x,y: "
            "equivalence (x=y) -> merge; forward entailment (x subset y) -> refine (new more specific); reverse "
            "entailment (x superset y) -> refine (new more general, subsumption link); negation (x^y=empty AND "
            "x+y=U) -> full contradiction (R3, no winner); alternation (x^y=empty AND x+y!=U) -> exclusive "
            "alternatives -> supersede/pick by authority; cover (x^y!=empty AND x+y=U) -> partial overlap -> partial "
            "overrule/split range; independence (else) -> coexist. The three exclusion relations (negation/alternation/"
            "cover) are exactly what the monotonicity calculus (only containment) was extended with, and exactly the "
            "distinctions NLI's single 'contradict' label conflates."
        ),
        "confidence": 0.95,
        "evidence": {"method": "web_search", "sources": [NATLOG], "session_id": SESSION},
    },
    {
        "subject": "Entity resolution assigns a per-attribute comparator, never one universal comparator",
        "predicate": "demonstrates",
        "object": (
            "production dedup systems (Dedupe, Splink, Zingg, Meltwater's KG record-linking pipeline) assign each "
            "attribute a comparator by its type: string -> edit-distance/fuzzy (WRatio, Jaro-Winkler, phonetic), "
            "numeric -> tolerance, date -> interval math, categorical -> exact + taxonomy. Amperity's 'Fusion' resolves "
            "conflicts within matched clusters at MULTIPLE confidence levels rather than assuming a canonical value is "
            "reachable. This is the direct model for 'typed comparator': predicate class -> comparator -> outcome."
        ),
        "confidence": 0.93,
        "evidence": {"method": "web_search", "sources": [MELTWATER], "session_id": SESSION},
    },
    {
        "subject": "Truth-maintenance systems (Doyle 1979, de Kleer 1986) give the retraction semantics of partial overrule",
        "predicate": "provide",
        "object": (
            "a TMS/ATMS tracks beliefs WITH their justifications (dependencies); on contradiction it performs "
            "dependency-directed backtracking: find the assumption(s) behind the defeated belief, retract exactly them, "
            "and propagate retraction to every belief derived from them. Maps directly onto CMMS: justification = "
            "provenance + derived_from edges (CMMS already stores both); partial overrule = retract the defeated "
            "sub-claim and transitively re-evaluate its derived_from descendants, NOT delete the world. Retraction is "
            "monotone and local -- overrule the assumption, not everything downstream."
        ),
        "confidence": 0.93,
        "evidence": {"method": "web_search", "sources": [TMS_WIKI], "session_id": SESSION},
    },
    {
        "subject": "Defeasible logic / argumentation (Dung 1995, Vreeswijk 1997) formalize priority-ordered overrule",
        "predicate": "formalizes",
        "object": (
            "defeasible reasoning handles the case where one claim DEFEATS another without either being false: arguments "
            "attack each other and PREFERENCE relations among arguments (authority, specificity, reliability, recency) "
            "decide which survives. Dung's admissibility captures 'the one who laughs last, laughs best' -- defeat "
            "resolved by the highest-priority undefeated argument. This is the formal basis for supersede and "
            "partial-overrule: a precedence ordering resolves the conflict without declaring the loser false "
            "(loser is deprecated, not revoked)."
        ),
        "confidence": 0.92,
        "evidence": {"method": "web_search", "sources": [SEP_DEFEASIBLE], "session_id": SESSION},
    },
    {
        "subject": "Legal antinomy canons (lex specialis / lex posterior / lex superior) are the authority hierarchy for partial overrule",
        "predicate": "are",
        "object": (
            "three conflict-resolution maxims that give the operational precedence order for the authority/normative "
            "predicate class: lex specialis (specialia generalibus non derogant -- the specific overrides the general) = "
            "PARTIAL OVERRULE (narrow, don't revoke -- a specific rule derogates a general rule only to the extent of "
            "the specific's scope); lex posterior (the later overrides the earlier) = supersession by recency; "
            "lex superior (the higher authority overrides the lower) = supersession by authority. Lex specialis is "
            "partial overrule by definition and maps onto CMMS scope (specificity = narrower scope)."
        ),
        "confidence": 0.94,
        "evidence": {"method": "web_search", "sources": [LEX_SPECIALIS], "session_id": SESSION},
    },
    {
        "subject": "Ten predicate classes with arity, rigidity, comparator, and admissible outcomes (deliverable)",
        "predicate": "enumerates",
        "object": (
            "(1) identity/rigid (is_a, named, born_on, has_pid): single-value, rigid, exact+ID, merge->conflict, "
            "authority-overrule only; (2) attribution/role (works_at, member_of, is_married_to): single, sticky, exact "
            "entity, merge->supersede-on-change; (3) preference (prefers, likes, dislikes): MULTI, sticky, set-membership "
            "+ VALENCE (like/neutral/dislike), merge->coexist->conflict(same object opposite valence); (4) current state "
            "transient fluent (located_at, in_meeting_with, feels): single, transient, exact+temporal, merge->supersede->"
            "conflict(overlap+diff), TTL; (5) habitual state sticky fluent (resides_in, speaks, uses): single, sticky, "
            "exact+temporal, merge->coexist(move)->refine(containment)->conflict; (6) numeric (age, price, count): "
            "single, transient, NUMERIC TOLERANCE |v1-v2|<=eps + unit normalize, merge->supersede(recency)->conflict; "
            "(7) range/interval (height_range, valid_price_range): single, rigid-ish, Allen interval algebra, merge->"
            "refine(containment)->PARTIAL-OVERRULE(overlap->split)->conflict(disjoint); (8) location/geo (lives_in "
            "region): multi, sticky, geospatial containment, refine(Murmansk subset Russia)->coexist(disjoint "
            "sequential)->conflict(same-time disjoint); (9) ownership (owns, has): multi, sticky, set-membership, "
            "merge->coexist->supersede(transfer); (10) authority/normative (may_do, is_admin_of, policy_applies): "
            "varies, sticky, NLI + authority/scope precedence, merge->PARTIAL-OVERRULE(lex specialis)->supersede(lex "
            "superior/posterior)->conflict."
        ),
        "confidence": 0.85,
        "evidence": {"method": "inference", "sources": [REPORT], "session_id": SESSION},
    },
    {
        "subject": "Seven reconciliation outcomes (conflict-class space)",
        "predicate": "defines",
        "object": (
            "merge (claim-key kappa=1 or equivalence) -> single fact dedup; refine (forward/reverse entailment or "
            "containment) -> refines/subsumes link, keep both; coexist (independence, or different predicate, or "
            "different valid-time) -> no relation, keep both; supersede (lex posterior/lex superior) -> close old "
            "valid_to, old -> deprecated; partial-overrule (lex specialis / range overlap) -> split range or narrow "
            "scope, deprecate only the defeated slice, propagate derived_from retraction; conflict (negation, "
            "overlapping+incompatible) -> MELD R3 first-class contradict link, NO silent winner; alternation "
            "(exclusive alternatives) -> alternative_of link, non-preferred -> deprecated (single-best-value). "
            "Supersede and partial-overrule are the two ORDERED replacements beyond MELD's R1-R6, distinguished from "
            "conflict which is unordered (no winner)."
        ),
        "confidence": 0.88,
        "evidence": {"method": "inference", "sources": [REPORT], "session_id": SESSION},
    },
    {
        "subject": "Partial overrule (the missing outcome) made precise",
        "predicate": "is",
        "object": (
            "defeats a claim only over the overlapping scope, leaving the non-overlapping portion active; the "
            "reconcile-layer name for lex specialis and range splitting, distinct from full conflict (no winner) and "
            "full supersession (whole-claim replacement). Three triggers: (1) range overlap (cover) -- 'price <= 800' "
            "overruled by 'price <= 500 from date D' splits the interval and deprecates only the [500,800] slice; "
            "(2) specific-over-general -- a contractor restriction narrows a general 'employees may access repo' "
            "policy WITHOUT revoking it; (3) same-predicate higher-authority narrower-scope claim overrules only the "
            "intersection. Implementation = TMS dependency-directed backtracking over derived_from (transitively "
            "re-evaluate descendants of the defeated slice). Distinguished from single-best-value: partial overrule "
            "SPLITS-and-demotes, single-best-value only RANKS-and-demotes."
        ),
        "confidence": 0.87,
        "evidence": {"method": "inference", "sources": [REPORT, LEX_SPECIALIS], "session_id": SESSION},
    },
    {
        "subject": "CMMS mapping: what exists vs what's missing for the predicate taxonomy to fire",
        "predicate": "recommends",
        "object": (
            "EXISTS and reusable: source (explicit/observed/inferred) + confidence = the authority axis (lex superior "
            "= source-type + confidence ordering); lifecycle_state (active/deprecated/overruled/revoked) = the outcome "
            "states (currently dormant, 0 lifecycle_events); claim_type (fact/authority/state) in remember.py = a coarse "
            "3-class seed of the taxonomy; derived_from edges = the TMS retraction substrate; scope = the lex specialis "
            "specificity field. MISSING: (1) no predicate registry mapping free-form predicate strings -> (class, arity, "
            "rigidity, comparator) -- current save-scripts use arbitrary predicates (defines/recommends/is/requires/"
            "enables/shows/guarantees/implements/provides/are); (2) no comparator implementation (remember() does no "
            "value-level comparison, silent append = MELD's reconciliation-by-chance); (3) claim_type 3-valued and never "
            "consulted by any merge path; (4) no ordered-replacement outcome (supersede/partial-overrule unrepresented). "
            "Order: (a) register predicate vocabulary defaulting unknown -> # coexist (conservative); (b) implement cheap "
            "comparators in code (exact/set/numeric-tolerance/Allen-interval/geospatial, zero LLM); (c) wire supersede + "
            "partial-overrule outcomes; (d) gate the 7-way LLM classifier behind structured comparators."
        ),
        "confidence": 0.85,
        "evidence": {"method": "inference", "sources": [REPORT], "session_id": SESSION},
    },
    {
        "subject": "Hard constraint: structured comparators must be executed in code, not generated by the LLM",
        "predicate": "requires",
        "object": (
            "inherited from CUR-MELD-TEMPORAL-SEMANTICS-000 §7: models score 13.5-16% on date/duration arithmetic "
            "(Test of Time) and 0.69 zero-shot on Allen 'Equals' (ChronoSense), so numeric-tolerance, Allen-interval, "
            "and geospatial-containment comparators must be computed over normalized typed values in code/SQL, never "
            "delegated to the LLM. The LLM classifier is reserved for the 7-way B-relation on unstructured (authority/"
            "prose) predicates only. This is what makes the predicate taxonomy cheap: classes 1-9 resolve in code with "
            "zero LLM cost."
        ),
        "confidence": 0.93,
        "evidence": {"method": "inference", "sources": [REPORT], "session_id": SESSION},
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
                    "predicate-taxonomy",
                    "conflict-resolution",
                    "typed-comparator",
                    "natural-logic",
                    "owl",
                    "wikidata",
                    "partial-overrule",
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
