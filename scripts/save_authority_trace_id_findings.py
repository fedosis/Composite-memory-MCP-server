"""Curiosity worker: save CUR-007 (authority-class + self-authenticating trace-ID
design) findings to CMMS via remember()."""
import asyncio
from datetime import datetime, timezone

from _common import get_db_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from memory_server.api.remember import remember
from memory_server.providers.sqlite_provider import SQLiteProvider

DB_URL = get_db_url()
SOURCE = "curiosity-worker"
SESSION = "cron_20260820_curiosity_authority_trace_id"

# Live gateways hold a long-lived WAL write lock; rebuild engine with 60s timeout.
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
        "subject": "CMMS authority-class field design (CUR-007)",
        "predicate": "specifies",
        "object": (
            "Promote claim_type from receipt metadata.evidence to a first-class facts.claim_type "
            "column (enum fact|authority|state, default 'fact') AND add a NEW facts.authority_class "
            "column capturing the source's provenance standing, orthogonal to verification_status. "
            "authority_class enum: primary (first-hand/direct observation or self-assertion), derived "
            "(inferred/computed from other facts), attributed (second-hand, cited to a named external "
            "source), self_asserted (the system's own operational rule/identity), unknown (legacy/default). "
            "Maps to W3C PROV: primary~prov:PrimarySource, derived~prov:Derivation/prov:Revision, "
            "attributed~prov:Attribution(wasAttributedTo). verification_status (unverified/candidate/"
            "validated/trusted/deprecated/archived) STAYS as lifecycle VERIFICATION trust and must NOT be "
            "repurposed as authority — that conflation is the bug CUR-005 flagged. creator already captures "
            "WHO; authority_class captures the epistemic standing of that creator's claim. Write path: "
            "ingestion_service passes metadata.evidence.claim_type through to FactORM.claim_type; "
            "authority_class defaults to 'unknown' or is inferred from source type (user/tool -> primary, "
            "agent synthesis -> derived, web/docs citation -> attributed)."
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "design",
            "sources": [
                "src/memory_server/models/receipt.py (ReceiptEvidence.claim_type, VerificationStatus enum)",
                "src/memory_server/api/remember.py (_ALLOWED_CLAIM_TYPES, _normalize_evidence)",
                "storage/models/fact.py (FactORM columns)",
                "W3C PROV-DM / PROV-O (prov:PrimarySource, prov:Derivation, prov:Attribution)",
            ],
            "session_id": SESSION,
            "claim_type": "authority",
        },
    },
    {
        "subject": "Self-authenticating trace-ID field design (CUR-007)",
        "predicate": "specifies",
        "object": (
            "Add facts.trace_id as a self-describing, content-addressed, self-authenticating identifier; "
            "KEEP id (UUID4) as the DB locator to avoid breaking the FK graph (receipts.id=fact.id, "
            "outbox.record_id, claim_relations source/target, evidence.source_id, graph node ids all "
            "reference id, and duplicate facts must not collapse). trace_id = 'sha2-256:' + "
            "base58(sha256(canonical_json({subject,predicate,object,source,creator,created_at,nonce}))). "
            "'sha2-256:' is a multicodec-style self-describing algorithm tag (IPFS CID/multihash pattern). "
            "nonce = 8-16 random bytes guarantees uniqueness across identical facts. This is the KERI "
            "'self-addressing identifier' (SAID) idea: the ID commits to content + author + instance, so "
            "tampering with any of subject/predicate/object/source/creator is detectable by "
            "recompute-and-compare WITHOUT trusting the DB. Optional phase-2: append an Ed25519 signature "
            "(trace_id + '.' + sign(creator_key, trace_id)) for non-repudiable authorship (KERI SCID model) "
            "— requires per-agent keypairs, defer until CUR-008 profile isolation lands. Two-layer "
            "separation: content_hash (CUR-006) answers 'same content?'; trace_id answers 'same instance, "
            "by this author?'. Verification: verify_trace_id(fact) recomputes from stored fields+nonce and "
            "compares; NULL for legacy rows -> UNKNOWN."
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "design",
            "sources": [
                "KERI draft-ssmith-keri-00 (SCID/SAID, self-certifying + self-addressing identifiers)",
                "IPFS CID spec / multihash / multicodec (self-describing content-addressed identifiers)",
                "storage/models/fact.py, storage/models/receipt.py (id shared as PK across tables)",
                "CUR-006 (content_hash design, sha256 canonicalization)",
            ],
            "session_id": SESSION,
            "claim_type": "authority",
        },
    },
    {
        "subject": "claim_type 'authority' vs authority_class naming hazard (CUR-007)",
        "predicate": "flags",
        "object": (
            "Two distinct 'authority' concepts must not be conflated in the schema: (1) "
            "claim_type='authority' means a NORMATIVE rule/directive/preference (has force, e.g. 'always "
            "use X', a user preference) as opposed to claim_type='fact' (descriptive assertion) or 'state' "
            "(system snapshot); (2) authority_class='primary'/'derived'/'attributed'/'self_asserted' means "
            "the SOURCE's epistemic standing (how the author knows). They are orthogonal axes: a "
            "primary-source observation can be a fact, a derived synthesis can assert an authority-rule, "
            "etc. Any implementation must keep these two fields separate and not overload the string "
            "'authority' across both."
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "design",
            "sources": [
                "src/memory_server/api/remember.py (_ALLOWED_CLAIM_TYPES={fact,authority,state})",
                "CUR-005 (verification_status conflation finding)",
            ],
            "session_id": SESSION,
            "claim_type": "fact",
        },
    },
    {
        "subject": "CUR-007 implementation dependency ordering",
        "predicate": "requires",
        "object": (
            "authority-class work (claim_type + authority_class columns) is INDEPENDENT of CUR-006 and can "
            "be migrated on its own. trace_id is derived directly from the full identity tuple "
            "(subject/predicate/object/source/creator/created_at/nonce), so it does NOT strictly require "
            "content_hash, but shipping both in one Alembic migration (0006) is cleaner since the two hashes "
            "share the same canonicalization helper. BOTH are gated on CUR-008 (profile isolation / "
            "shared-DB symlink fix) before migrating the live shared memory.db under three gateways' WAL "
            "locks, and on Fedos approval (schema migration = self-modification of memory-server). Legacy "
            "backfill: claim_type='fact', authority_class='unknown', trace_id=NULL (recompute only after "
            "CUR-006 backfill restores content_hash, if trace_id is chosen to embed it)."
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "design",
            "sources": [
                "CUR-006 (content_hash + canonicalization helper)",
                "CUR-008 (shared-DB symlink blocker)",
                "alembic/versions/0001..0004 migration pattern",
            ],
            "session_id": SESSION,
            "claim_type": "authority",
        },
    },
]


async def main():
    provider = SQLiteProvider(url=DB_URL)
    # Skip provider.initialize(): it runs WAL PRAGMA + FTS table/trigger creation,
    # which need an exclusive lock and block behind the live gateways' WAL write
    # lock. remember() only needs _engine/_session_factory, which we build directly.
    _rebuild_engine(provider)
    results = []
    try:
        for f in FACTS:
            metadata = {
                "evidence": f["evidence"],
                "source_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "tags": [
                    "curiosity-worker",
                    "cmms",
                    "authority-class",
                    "trace-id",
                    "self-authenticating",
                    "self-certifying",
                    "content-addressed",
                    "provenance",
                    "schema-design",
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
                except Exception as exc:
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
