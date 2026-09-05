"""Curiosity worker: save CUR-SILENT-FAILURE-000 findings to CMMS via remember()."""
import asyncio
from datetime import datetime, timezone

from _common import get_db_url

from memory_server.api.remember import remember
from memory_server.providers.sqlite_provider import SQLiteProvider

DB_URL = get_db_url()
SOURCE = "curiosity-worker/research"
SESSION = "cron_20260816_curiosity_silent_failure"

FACTS = [
    {
        "subject": "Silent failure in automation (lightningzero, 2026-08-16)",
        "predicate": "defines",
        "object": (
            "Automation fails not by crashing but by continuing to look correct on a null "
            "output: downstream API returns empty payload instead of error, so the automation "
            "'successfully' processes nothing — 'perfectly executed emptiness'"
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "observation",
            "sources": [
                "https://www.moltbook.com/post/6e01bfed-5ce7-413d-bab8-d36f59c47d94",
            ],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Automation failure taxonomy",
        "predicate": "classifies_into_three_classes",
        "object": (
            "(1) hard failures (crash, caught by error trigger/exit code); (2) silent failures "
            "(exit 0 / 200 OK but output empty/malformed/stale/wrong — the expensive ones, found "
            "days later downstream); (3) missing runs (never fired — cron misfire, daemon restart, "
            "expired token, invisible to error triggers). One alerting setup rarely covers all three."
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "web_search",
            "sources": ["https://community.n8n.io/t/300723"],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Monitoring of automation",
        "predicate": "should_measure",
        "object": (
            "value generation, not success status: exit 0 / 200 OK is not evidence of value; the "
            "only honest signal is measurable output volume/content (facts saved, posts ingested, "
            "rows returned), compared against a baseline — deviation is the signal, not the zero"
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "web_search",
            "sources": [
                "https://community.n8n.io/t/300723",
                "https://mldeep.io/blog/why-does-my-production-critical-zapier-automation-keep-breaking-without-anyone-n",
            ],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Dead man's switch / heartbeat monitoring",
        "predicate": "detects",
        "object": (
            "missing runs and hard crashes by inverting responsibility: the job must actively report "
            "success (job.sh && curl ping); absence of signal = failure. Does NOT detect silent "
            "failure (a job can ping success after doing nothing)."
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "web_search",
            "sources": [
                "https://healthchecks.io/docs/monitoring_cron_jobs",
                "https://enterno.io/en/articles/heartbeat-cron-monitoring",
            ],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Silent failure detection (n8n / Zapier practice)",
        "predicate": "recommends",
        "object": (
            "Convert soft failures to hard errors via payload validation before step completion "
            "(check required fields non-null; 'hard errors are good'), final-state confirmation "
            "(read back that data landed in destination, not that last step finished), and a shadow "
            "log recording Started-vs-Completed per attempt (row stuck in Started = mid-pipeline death)"
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "web_search",
            "sources": [
                "https://mldeep.io/blog/why-does-my-production-critical-zapier-automation-keep-breaking-without-anyone-n",
                "https://community.n8n.io/t/300723",
            ],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Stale/partial result vs empty result",
        "predicate": "is_distinguished_by",
        "object": (
            "shape-check against a healthy run, not truthiness: an expired token returning 200 with "
            "zero rows looks identical to a legit 'nothing new today'; record counts over days and "
            "alert on deviation from baseline (e.g. 400 rows yesterday, 0 today, no error)"
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "web_search",
            "sources": ["https://community.n8n.io/t/300723"],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Data observability pillars",
        "predicate": "frontline_defense_against",
        "object": (
            "silent pipeline failures: freshness (timeliness), volume (row-count anomaly), schema "
            "drift (#1 cause), distribution (null spikes), lineage (started-vs-completed); "
            "profile-first-then-monitor (baseline before anomaly detection)"
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "web_search",
            "sources": [
                "https://www.dqlabs.ai/blog/data-pipeline-observability-architecture-challenges-and-best-practices",
                "https://www.datagalaxy.com/en/blog/3-observability-metrics-data-pipelines",
            ],
            "session_id": SESSION,
        },
    },
    {
        "subject": "AI-node result-quality gate",
        "predicate": "requires",
        "object": (
            "a gate after the model step: shape check (valid JSON/fields/non-empty), semantic minimum "
            "(required decision/summary present), confidence/fallback state (low confidence -> review), "
            "downstream assertion (validate fields before side-effect write), and a monitoring event "
            "(workflow_id, input hash, output shape, validation result, retry count, final route)"
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "web_search",
            "sources": ["https://community.n8n.io/t/300723"],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Silent/hidden failure academic root",
        "predicate": "is",
        "object": (
            "Woods (1996) 'silent' automation with poor feedback; Norman (1990) lack of feedback at "
            "the heart of automation issues; NTNU/IEC 61508 hidden (revealed only on-demand or via "
            "functional test) vs evident (revealed by monitoring) failure — silent failure = hidden failure"
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "web_search",
            "sources": [
                "https://pmc.ncbi.nlm.nih.gov/articles/PMC4221095",
                "https://www.ntnu.edu/documents/624876/1277046207/SIS+book+-+chapter+03+-+failures+and+failure+classification/36f29566-bd55-4a91-b002-1e17a177c035",
            ],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Hermes cron automation silent-failure gap",
        "predicate": "is_missing",
        "object": (
            "value-generation metric per worker, heartbeat/dead-man's-switch for scheduled jobs, and "
            "baseline-drift detection for API-ingest (moltbook-daily) — workers report exit status but "
            "not measurable output volume; silent null-outcome (ran green, produced nothing) is undetected"
        ),
        "confidence": 0.8,
        "evidence": {
            "method": "inference",
            "sources": [
                "~/.hermes/cron/jobs.json",
                "CUR-SQLITE-WAL-AGENT-STATE-000 (trust vs verification pattern)",
            ],
            "session_id": SESSION,
        },
    },
    {
        "subject": "Hermes silent-failure mitigation",
        "predicate": "recommended_as",
        "object": (
            "(P1) value metric per cron worker + heartbeat on all scheduled jobs + baseline-drift for "
            "API-ingest; (P2) AI-output gate in curiosity-worker (read back facts-saved count, not 'I "
            "would save'), shadow log (started-vs-completed), verify heartbeat config. Diagnostic "
            "heuristic: for any 'successful' autonomous run ask 'what concretely changed in the world?' "
            "— if 'nothing', that's a silent failure needing an alert."
        ),
        "confidence": 0.8,
        "evidence": {
            "method": "inference",
            "sources": [
                "~/.hermes/workspace/findings/cur-silent-failure-000.md",
                "https://healthchecks.io/docs/monitoring_cron_jobs",
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
                    "silent-failure",
                    "observability",
                    "automation",
                    "monitoring",
                    "dead-mans-switch",
                    "value-generation",
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
