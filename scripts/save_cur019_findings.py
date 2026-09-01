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
SESSION = "cron_20260822_cur019_invest_agent_activation_check"

def rebuild(provider):
    engine = create_async_engine(DB_URL, echo=False, connect_args={"timeout": 60})
    provider._engine = engine
    provider._session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

FACTS = [
    (
        "Invest-agent CMMS activation E2E (CUR-019)",
        "proves",
        "A real authenticated POST to the invest-agent API server at /v1/chat/completions on 2026-08-22 returned HTTP "
        "200 with the exact response OK (model deepseek-v4-flash; 18,543 total tokens). This triggered an actual "
        "per-message AIAgent creation and is the required end-to-end activation stimulus, not just a health check.",
        0.99,
        [
            "live authenticated API request 2026-08-22",
            "Hermes API server response",
        ],
    ),
    (
        "Invest-agent CMMS activation E2E (CUR-019)",
        "confirms",
        "Before the stimulus, invest-agent PID 53294 had no memory.db, WAL/SHM, LanceDB, or graph file descriptors "
        "despite a live process and valid memory_server config. The official Hermes lifecycle and local source show "
        "activation is per-agent/message: is_available() is import-only, then initialize_all() calls "
        "HermesProvider.initialize(), which resolves the profile-relative sqlite+aiosqlite:///data/memory.db, "
        "initializes SQLiteProvider, starts WriterQueue and outbox, and logs activation. Therefore descriptor absence "
        "at idle is expected and does not indicate a broken db_url.",
        0.98,
        [
            "live /proc fd inspection before request",
            "memory-server/src/memory_server/plugins/hermes/provider.py:247-336",
            "hermes-agent/agent/agent_init.py:1798-1867",
            "Hermes memory-provider docs",
        ],
    ),
    (
        "Invest-agent effective CMMS ownership (CUR-019)",
        "requires",
        "The E2E request proved the API route and model execution, but the post-request descriptor/log verification "
        "could not be completed in the same cron turn because the local terminal safety guard rejected follow-up "
        "inspection commands as an attempted in-process gateway restart. The effective configuration remains: "
        "HERMES_HOME=/home/shtorm/.hermes/profiles/invest-agent; provider=memory_server; plugin "
        "path=/home/shtorm/memory-server; no explicit db_url, so default resolves to the profile data/memory.db path "
        "(currently a symlink to /home/shtorm/memory-server/data/memory.db). A separate read-only inspection is still "
        "needed to capture post-request fds and the exact activation log lines.",
        0.94,
        [
            "invest-agent config.yaml",
            "memory-server/src/memory_server/plugins/hermes/config.py:137-150",
            "terminal safety-guard result",
        ],
    ),
    (
        "CUR-019 follow-up gap",
        "recommends",
        "Run a read-only post-activation check outside the in-process cron terminal guard: inspect invest-agent PID "
        "fds for memory.db/WAL/SHM/LanceDB/graph and grep its log for Memory provider memory_server activated and "
        "HermesProvider initialized. Record the resolved db_url and whether the outbox worker owns the shared "
        "database. Do not migrate the schema based only on the pre-request idle state.",
        0.96,
        [
            "CUR-019 E2E check",
            "CUR-018 verification procedure",
        ],
    ),
]



async def main():
    provider = SQLiteProvider(url=DB_URL)
    rebuild(provider)
    ids=[]
    try:
        for subject,predicate,obj,confidence,sources in FACTS:
            metadata={
                "evidence":
                {
                    "method":
                    "official Hermes docs + local source inspection + authenticated live API E2E request",
                    "sources":
                    sources,
                    "session_id":
                    SESSION,
                    "claim_type":
                    "fact",
                },
                "source_date":
                datetime.now(timezone.utc).strftime('%Y-%m-%d'),
                "tags":
                [
                    "curiosity-worker",
                    "cur-019",
                    "cmms",
                    "hermes",
                    "memory-provider",
                    "invest-agent",
                    "gateway",
                    "activation",
                    "e2e",
                    "sqlite",
                    "provenance",
                ],
            }

            last=None
            for attempt in range(4):
                try:
                    res=await remember(
                        provider=provider,
                        subject=subject,
                        predicate=predicate,
                        object=obj,
                        confidence=confidence,
                        source=SOURCE,
                        metadata=metadata,
                    )

                    ids.append(res["fact"].id)
                    break
                except Exception as exc:
                    last=exc
                    await asyncio.sleep(10*(attempt+1))
            else:
                raise last
        print("saved", ids)
    finally:
        await provider.close()

asyncio.run(main())
