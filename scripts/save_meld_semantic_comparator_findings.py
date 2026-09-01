"""Curiosity worker: save CUR-MELD-SEMANTIC-COMPARATOR-000 findings to CMMS via remember()."""
import asyncio
import sys
from datetime import datetime, timezone

from _common import get_db_url

sys.path.insert(0, "/home/shtorm/memory-server/src")

from memory_server.api.remember import remember
from memory_server.providers.sqlite_provider import SQLiteProvider

DB_URL = get_db_url()
SOURCE = "curiosity-worker/research"
SESSION = "cron_20260819_curiosity_meld_semantic_comparator"

DEBERTA_GH = "https://github.com/microsoft/DeBERTa"
NLI_DEBERTA_HF = "https://huggingface.co/cross-encoder/nli-deberta-v3-base"
STRESS = "https://aclanthology.org/C18-1198.pdf"
LUO_EMNLP = "https://lucian.uchicago.edu/blogs/mingxiang/wp-content/blogs.dir/197/files/2023/01/2023.Luoetal.EMNLP_.pdf"
NATLOG = "https://nlp.stanford.edu/pubs/natlog-iwcs09.pdf"
HALLUC = "https://aclanthology.org/2025.sdp-1.34.pdf"
LORESLM = "https://aclanthology.org/2026.loreslm-1.17.pdf"
BRENN = "https://mbrenndoerfer.com/writing/hallucination-detection"
REPORT = "~/.hermes/workspace/findings/cur-meld-semantic-comparator-000.md"

FACTS = [
    {
        "subject": "NLI cross-encoder models are the production default for 3-way entailment",
        "predicate": "establishes",
        "object": (
            "sentence-transformers cross-encoder/nli-* family (fine-tuned on SNLI + MultiNLI) is the standard "
            "off-the-shelf 3-way classifier. MNLI accuracy: nli-deberta-v3-large 91.8/91.9 matched/mismatched, "
            "nli-deberta-v3-base 90.6/90.7 (community re-measure 0.8853 on dev mismatched), nli-deberta-v3-small "
            "88.3/87.7, roberta-large-mnli 90.2 (GLUE). DeBERTa-v3 (2022) still outperforms newer ModernBERT on "
            "NLI-style hallucination detection (ACL 2025), so it remains the right default. All are local, "
            "dependency-light, and output a softmax over contradiction/entailment/neutral usable as a calibrated "
            "signal, not just argmax."
        ),
        "confidence": 0.95,
        "evidence": {"method": "web_search", "sources": [DEBERTA_GH, NLI_DEBERTA_HF], "session_id": SESSION},
    },
    {
        "subject": "NLI contradiction-recall is bounded by two opposite negation/antonym failure modes",
        "predicate": "demonstrates",
        "object": (
            "headline MNLI accuracy (~91%) is NOT contradiction-recall and hides two opposite failures that both "
            "matter for 'never silently resolve': (a) surface-negation FALSE POSITIVES -- Naik et al. 2018 (ACL "
            "C18-1198, STRESS) show strong negation words cause models to predict contradiction for neutral/entailed "
            "pairs, accuracy drops 23.4%/23.38% matched/mismatched; (b) antonym/lexical-negation FALSE NEGATIVES -- "
            "antonyms without explicit 'not' are NOT detected as contradiction (5% of errors); Luo et al. EMNLP 2023 "
            "show below-chance on simple sentences and composition failure binding 'not' to a predicate; MoNLI "
            "(Geiger 2020) shows models fail to reverse entailment under negation. The false-negative direction is "
            "the dangerous one: a contradiction phrased without 'not' gets missed and silently resolved."
        ),
        "confidence": 0.93,
        "evidence": {"method": "web_search", "sources": [STRESS, LUO_EMNLP], "session_id": SESSION},
    },
    {
        "subject": "Even task-fine-tuned NLI has ~18% contradiction false-negative rate in-domain",
        "predicate": "quantifies",
        "object": (
            "ACL 2025 (sdp-1.34) coarse-grained hallucination detection: DeBERTa-v3-large fine-tuned on the task "
            "reaches contradiction precision 0.84 / recall 0.82 / F1 0.83 -- i.e. ~18% of true contradictions are "
            "missed AFTER fine-tuning on that domain. Off-the-shelf (un-fine-tuned) contradiction-recall on arbitrary "
            "agent-memory prose is worse. Consequence: a bare 3-way NLI cross-encoder is a cheap first-pass "
            "contradiction detector, not a guarantee; the 'never silently resolve' invariant needs a calibrated "
            "probability threshold, a lexical antonym/negation supplement, and escalation for high-stakes pairs."
        ),
        "confidence": 0.9,
        "evidence": {"method": "web_search", "sources": [HALLUC], "session_id": SESSION},
    },
    {
        "subject": "The 7-way natural-logic relation set has no off-the-shelf production model",
        "predicate": "establishes",
        "object": (
            "MacCartney & Manning's 7 basic entailment relations (equivalence x=y -> merge; forward entailment x "
            "subset y "
            "-> refine; reverse entailment x superset y -> refine; negation x^y=empty AND x+y=U -> full conflict R3; "
            "alternation x^y=empty AND x+y!=U -> exclusive alternatives supersede; cover x^y!=empty AND x+y=U -> "
            "partial "
            "overrule; independence else -> coexist) have exactly one full implementation: NatLog (Stanford "
            "2007-2009), "
            "a Java research system. Its 7-way comes from word-level lexical relations + monotonicity projection over "
            "a "
            "syntactic parse -- i.e. the typed-comparator idea, NOT a semantic LLM. There is no maintained Python/PyPI "
            "7-way classifier. The 7-way on unstructured prose must be reconstructed, not downloaded."
        ),
        "confidence": 0.92,
        "evidence": {"method": "web_search", "sources": [NATLOG], "session_id": SESSION},
    },
    {
        "subject": "A prompt-based 7-way classifier is feasible but the least reliable option",
        "predicate": "concludes",
        "object": (
            "LLM-as-judge work shows (a) self-inconsistency across runs (Rating Roulette arXiv 2510.27106) and (b) low "
            "class-wise consistency on fine-grained entailment labels -- contradiction consistency 0.62-0.79 and "
            "neutral "
            "the LOWEST (~0.45-0.48) across Aya/Qwen/LLaMA/Claude/GPT (LoResLM 2026). Distinguishing forward vs "
            "reverse "
            "entailment vs negation vs alternation vs cover requires set-theoretic reasoning LLMs are weak at; "
            "gpt-3.5-turbo was only shown comparable-or-better than SOTA on the binary/3-way entailment subset "
            "(SummaC), "
            "not 7-way. Therefore 7-way should be derived from set-theoretic checks in code for structured predicates, "
            "and unstructured prose should stay at 3-way NLI + overlap heuristics, escalating to prompt-based 7-way "
            "only "
            "for rare high-stakes boundary pairs."
        ),
        "confidence": 0.88,
        "evidence": {"method": "web_search", "sources": [LORESLM], "session_id": SESSION},
    },
    {
        "subject": "Cost/latency ordering for the semantic comparator",
        "predicate": "orders",
        "object": (
            "per-pair: structured comparators (classes 1-9) = $0 and microseconds (pure code, deterministic); "
            "cross-encoder NLI (DeBERTa-v3) = $0 per call, local, ~5-50 ms/pair GPU / ~100-300 ms/pair CPU, one-time "
            "~1 GB RAM model load; sentence-embedding cosine sigma = $0 reusing CMMS embedder, ~1-10 ms/pair; "
            "prompt-based LLM (3-way or 7-way) = ~$0.0005-0.01/call at DeepSeek V4-flash pricing, ~1-3 s/call. The "
            "cross-encoder is the right DEFAULT semantic comparator (local, zero marginal cost, ms latency, calibrated "
            "softmax); the prompt-based LLM is reserved as fallback/escalation, not default -- it is 100-1000x slower "
            "and introduces self-inconsistency."
        ),
        "confidence": 0.85,
        "evidence": {"method": "inference", "sources": [REPORT, BRENN], "session_id": SESSION},
    },
    {
        "subject": "Recommended architecture: two-stage semantic comparator, not one model",
        "predicate": "recommends",
        "object": (
            "(1) sigma gate first in code: sigma = cos(emb(a), emb(b)) reusing CMMS embedder; if sigma < sigma_lo the "
            "claims are unrelated -> coexist/relate without invoking any classifier, screening out the majority of "
            "pairs. "
            "(2) chi = 3-way cross-encoder NLI (nli-deberta-v3-base default, -large for authority/normative class), "
            "using "
            "the CALIBRATED p(contradict) never argmax: contradict -> R3 conflict, entail+high-sigma -> merge/refine "
            "candidate, neutral -> relate/coexist. (3) lexical antonym/negation supplement in code (WordNet antonym + "
            "explicit negation tokens) as a parallel contradiction signal to catch the NLI false-negative direction. "
            "(4) 7-way only where needed via set-theoretic decomposition; do NOT ask an LLM for 7-way by default."
        ),
        "confidence": 0.87,
        "evidence": {"method": "inference", "sources": [REPORT], "session_id": SESSION},
    },
    {
        "subject": "Recommended thresholds for the reconcile layer (starting points, must be calibrated on CMMS data)",
        "predicate": "specifies",
        "object": (
            "sigma_lo (relatedness floor, below -> unrelated) = 0.35-0.50, tune so true conflicts aren't dropped; "
            "theta_merge (merge bar, sigma above + context match) = 0.85-0.92, MELD merge classifier AUC 0.968 / "
            "false-merge 0.013 on HotpotQA; chi_contradict (contradiction gate on p(contradict)) = 0.70-0.80, "
            "threshold "
            "p_contradict directly per Brenndoerfer and tune on a labeled validation set watching false-negative rate. "
            "The single most important calibration target is the contradiction false-negative rate on antonym/lexical-"
            "negation pairs -- it determines whether the 'never silently resolve' invariant actually holds."
        ),
        "confidence": 0.82,
        "evidence": {"method": "inference", "sources": [REPORT, BRENN], "session_id": SESSION},
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
                    "reconcile-layer",
                    "semantic-comparator",
                    "nli",
                    "natural-logic",
                    "contradiction-detection",
                    "deberta",
                    "cross-encoder",
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
