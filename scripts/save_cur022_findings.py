import asyncio
import sys
from _common import get_db_url
from datetime import datetime, timezone
sys.path.insert(0, "/home/shtorm/memory-server/src")
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.api.remember import remember

DB_URL = get_db_url()
SOURCE = "curiosity-worker"
SESSION = "cron_20260822_cur022_sqlite_wal_version_correlation"

def rebuild(provider):
    engine = create_async_engine(DB_URL, echo=False, connect_args={"timeout": 60})
    provider._engine = engine
    provider._session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

FACTS = [
    ("CMMS SQLite runtime inventory (CUR-022)", "uses", "The Hermes application virtualenv and system Python both report SQLite 3.53.1 (Python 3.11.15); the installed sqlite3 CLI reports 3.45.1. This is a material tool/runtime mismatch: CLI diagnostics are not testing the same SQLite library as the running Hermes/SQLAlchemy/aiosqlite process.", 0.99, ["live python sqlite3.sqlite_version", "live hermes venv sqlite3.sqlite_version", "live sqlite3 --version"]),
    ("CMMS WAL-reset bug exposure (CUR-022)", "is_not_exposed_to_known_fixed_bug_by", "SQLite's official WAL documentation and release history say the rare WAL-reset corruption bug was fixed in 3.53.0; the running application is on 3.53.1, so this specific known bug is not the best explanation for the 2026-08-21 malformed-image errors. The current app is nevertheless behind the official 3.53.4 release dated 2026-07-24.", 0.98, ["https://sqlite.org/wal.html", "https://www.sqlite.org/changes.html", "live Python runtime version"]),
    ("CMMS malformed-image errors (CUR-022)", "correlate_with", "The available error evidence shows two database disk image is malformed failures on 2026-08-21 at 20:17:39 UTC+3-equivalent log time: an INSERT through SQLAlchemy/aiosqlite and a SELECT through the same stack. Both occurred while the default Hermes PID 65846 owned memory.db, memory.db-wal, and memory.db-shm. Read-only quick_check/integrity_check and an independent native backup later passed, so the evidence supports an intermittent WAL/concurrency or transient shared-state episode, not persistent page corruption; it does not prove which writer/checkpoint event caused it.", 0.99, ["/home/shtorm/.hermes/logs/errors.log", "CUR-021 read-only checks and backup", "live fuser/PID inspection"]),
    ("CMMS WAL ownership snapshot (CUR-022)", "shows", "At 2026-08-22 11:01 MSK, only default Hermes PID 65846 had open descriptors for the shared memory.db, WAL, and SHM; coder, invest-agent, lander, travel-agent, and travel-agent-dev had no matching CMMS descriptors in the inspected process snapshot. The database was WAL/NORMAL with wal_autocheckpoint=1000, size 278,597,632 bytes and WAL size 8,866,272 bytes.", 0.99, ["live /proc fd inspection", "live PRAGMA journal_mode/synchronous/wal_autocheckpoint", "live stat/fuser"]),
    ("CMMS SQLite investigation (CUR-022)", "recommends", "Before migration, standardize diagnostics on the application runtime (SQLite 3.53.1), capture timestamped PID-to-fd ownership and db/WAL/SHM sizes, and log checkpoint results (busy/frames/checkpointed) around every writer attempt. Run the same read-only integrity/backup checks through the app's Python/aiosqlite library, not only the older 3.45.1 CLI. Upgrade the app runtime to the latest 3.53.x patch after compatibility testing, then correlate future errors with the owning PID and WAL epoch; do not infer corruption from a single transient ORM exception.", 0.97, ["https://sqlite.org/wal.html", "https://www.sqlite.org/changes.html", "https://docs.python.org/3/library/sqlite3.html"]),
]

async def main():
    provider = SQLiteProvider(url=DB_URL)
    rebuild(provider)
    ids=[]
    try:
        for subject,predicate,obj,confidence,sources in FACTS:
            metadata={"evidence":{"method":"live runtime/process/database inspection plus official SQLite/Python documentation","sources":sources,"session_id":SESSION,"claim_type":"fact"},"source_date":datetime.now(timezone.utc).strftime("%Y-%m-%d"),"tags":["curiosity-worker","cur-022","cmms","sqlite","wal","checkpoint","runtime-version","corruption","provenance"]}
            last=None
            for attempt in range(5):
                try:
                    res=await remember(provider=provider,subject=subject,predicate=predicate,object=obj,confidence=confidence,source=SOURCE,metadata=metadata)
                    ids.append(res["fact"].id)
                    break
                except Exception as exc:
                    last=exc
                    await asyncio.sleep(8*(attempt+1))
            else:
                raise last
        print("saved", ids)
    finally:
        await provider.close()

asyncio.run(main())
