"""Curiosity worker: save CUR-006 (content integrity hash gap + design) findings to CMMS via remember()."""
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
SESSION = "cron_20260820_curiosity_content_hash"

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
        "subject": "CMMS facts carry no content integrity hash (CUR-006)",
        "predicate": "confirms",
        "object": (
            "Definitive: the facts table has NO content-hash/digest column. Verified against "
            "storage/models/fact.py (FactORM: id, subject, predicate, object, confidence, source, "
            "creator, created_at, updated_at, verification_status, lifecycle_state, version) and the "
            "live DB (sqlite3 '.schema facts' = same 12 columns). No hash column exists on receipts, "
            "beliefs, evidence, skills, decisions, entities, lifecycle_states, lifecycle_events, or "
            "claim_relations either. fact id is str(uuid4()) — random, not content-addressed or signed. "
            "The only hashlib.sha256 in the codebase is embedding_provider.py:69 (deterministic embedding "
            "seed), not an integrity hash. Write path remember()/learn() -> MemoryIngestionService -> "
            "FactRepository.create() -> FactORM computes no hash at any point."
        ),
        "confidence": 0.97,
        "evidence": {
            "method": "code_inspection",
            "sources": [
                "storage/models/fact.py", "storage/models/receipt.py", "storage/models/belief.py",
                "storage/models/skill.py", "storage/models/decision.py", "storage/models/entity.py",
                "storage/models/lifecycle.py", "storage/models/relation.py",
                "src/memory_server/services/ingestion_service.py", "sqlite3 .schema facts",
            ],
            "session_id": SESSION,
            "claim_type": "fact",
        },
    },
    {
        "subject": "Content integrity hash — what to hash (CUR-006)",
        "predicate": "designates",
        "object": (
            "Hash the immutable content payload + origin: subject, predicate, object, source. "
            "EXCLUDE mutable fields that legitimately change over a fact's life: confidence "
            "(reinforcement), version (increments), lifecycle_state (transitions), verification_status "
            "(verification), created_at/updated_at. Canonicalization: sha256 over "
            "json.dumps({subject,predicate,object,source}, sort_keys=True, separators=(',',':'), "
            "ensure_ascii=False).encode('utf-8').hexdigest(). This makes the hash stable across "
            "legitimate mutations while detecting any unauthorized change to the statement or its origin."
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "design",
            "sources": ["CUR-005 provenance mapping", "CUR-006 schema inspection"],
            "session_id": SESSION,
            "claim_type": "authority",
        },
    },
    {
        "subject": "Content integrity hash — write path + verification (CUR-006)",
        "predicate": "specifies",
        "object": (
            "Single choke point: compute the hash in FactRepository.create() (covers both remember() and "
            "learn(), the only two write paths) when fact.content_hash is empty; FactRepository.update() "
            "must recompute when subject/predicate/object/source change. Add content_hash: Optional[str] "
            "to the pydantic Fact model and a String(64) column to FactORM. Read-time verification: "
            "FactRepository.verify_integrity(fact_id) recomputes and compares, returning match/mismatch/"
            "unknown(NULL for legacy rows); auto-verify on get() stays OFF by default for backward "
            "compatibility. Helper lives in a new storage/integrity.py."
        ),
        "confidence": 0.9,
        "evidence": {
            "method": "design",
            "sources": ["storage/repositories/fact_repo.py", "src/memory_server/services/ingestion_service.py"],
            "session_id": SESSION,
            "claim_type": "authority",
        },
    },
    {
        "subject": "Content integrity hash — migration + backfill (CUR-006)",
        "predicate": "requires",
        "object": (
            "Alembic migration 0005_add_fact_content_hash.py: op.add_column('facts', "
            "sa.Column('content_hash', sa.String(64), nullable=True)), then a PYTHON backfill (SQLite has "
            "no sha256() builtin — iterate rows, compute, UPDATE). Existing rows keep NULL (verify -> "
            "UNKNOWN) until backfilled. New installs get the column automatically via "
            "Base.metadata.create_all() in SQLiteProvider.initialize(). Keep nullable (NOT NULL would need "
            "a SQLite table-rebuild). Note the version-type inconsistency surfaced alongside: pydantic "
            "Fact.version is int=1 but FactORM.version is String default '0.1.0' and increment_version does "
            "int() parsing on it — resolve in the same migration."
        ),
        "confidence": 0.85,
        "evidence": {
            "method": "design",
            "sources": ["alembic/versions/0001..0004", "src/memory_server/providers/sqlite_provider.py"],
            "session_id": SESSION,
            "claim_type": "authority",
        },
    },
    {
        "subject": "Fact.version type inconsistency (CUR-006 surfaced)",
        "predicate": "flags",
        "object": (
            "Latent schema bug found during CUR-006: pydantic memory_server/models/fact.py declares "
            "version: int = 1 (with a normalizer that maps legacy '0.1.0' strings to 1), but "
            "storage/models/fact.py FactORM declares version: Mapped[str] with default '0.1.0', and "
            "FactRepository.increment_version does int(str(raw_version).strip()) on the string column. "
            "Reads round-trip through the int normalizer, but the column type is wrong and new writes still "
            "default to the legacy string '0.1.0'. Fix alongside the content_hash migration."
        ),
        "confidence": 0.95,
        "evidence": {
            "method": "code_inspection",
            "sources": [
                "src/memory_server/models/fact.py", "storage/models/fact.py",
                "storage/repositories/fact_repo.py",
            ],
            "session_id": SESSION,
            "claim_type": "fact",
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
                    "integrity-hash",
                    "content-hash",
                    "provenance",
                    "tamper-detection",
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
