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
SESSION = "cron_20260821_cur018_invest_agent_provider_trace"

def rebuild(provider):
    engine = create_async_engine(DB_URL, echo=False, connect_args={"timeout": 60})
    provider._engine = engine
    provider._session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

FACTS = [
    (
        "Invest-agent CMMS activation trace (CUR-018)",
        "shows",
        "The configured invest-agent gateway (PID 43000, cwd /home/shtorm/.hermes/profiles/invest-agent) had no open "
        "memory.db, memory.db-wal, memory.db-shm, LanceDB, or graph descriptors at inspection time, while the default "
        "gateway (PID 43133) had all of them. This establishes that invest-agent was not an active CMMS database "
        "owner at that moment; configuration presence alone did not imply provider initialization. The process was "
        "alive, but no per-message AIAgent/provider instance had opened the database.",
        0.96,
        [
            "live ps/fd inspection 2026-08-21",
            "CUR-017 gateway inventory",
        ],
    ),
    (
        "Hermes memory_server activation path (CUR-018)",
        "requires",
        "Hermes agent initialization reads memory.provider, loads the named plugin, calls is_available(), adds it "
        "only if available, then calls MemoryManager.initialize_all(session_id, hermes_home, platform, and identity "
        "kwargs). The CMMS HermesProvider itself resolves the default relative sqlite+aiosqlite:///data/memory.db "
        "against the supplied hermes_home, constructs SQLiteProvider, starts WriterQueue and the outbox worker, and "
        "logs 'HermesProvider initialized'. Therefore the provider is lazy/per-agent rather than "
        "gateway-startup-global: a gateway can run with no CMMS file descriptor until a message creates an AIAgent "
        "that reaches this path.",
        0.95,
        [
            "memory-server/src/memory_server/plugins/hermes/provider.py:268-328",
            "hermes-agent/agent/agent_init.py:1791-1867",
            "Hermes memory-provider docs",
        ],
    ),
    (
        "Invest-agent CMMS configuration (CUR-018)",
        "configures",
        "invest-agent config.yaml has memory.memory_enabled=true, memory.provider=memory_server, "
        "providers.memory_server.enabled=true, plugin=memory_server.plugins.hermes.provider.HermesProvider, and "
        "path=/home/shtorm/memory-server. No db_url is configured, so CMMS falls back to "
        "sqlite+aiosqlite:///data/memory.db and resolves it relative to the active HERMES_HOME "
        "(/home/shtorm/.hermes/profiles/invest-agent). The profile's data/memory.db path resolves by symlink to "
        "/home/shtorm/memory-server/data/memory.db, but that path is only used after provider initialize runs.",
        0.97,
        [
            "/home/shtorm/.hermes/profiles/invest-agent/config.yaml:18-32",
            "memory-server/src/memory_server/plugins/hermes/config.py:44-92,137-150",
        ],
    ),
    (
        "CUR-018 diagnosis and verification procedure",
        "recommends",
        "The missing-descriptor finding is not evidence of a broken db_url or failed provider: it is best explained "
        "by lazy activation plus no active agent/message in invest-agent at inspection. To distinguish that from a "
        "startup/config propagation failure, run a real message through invest-agent, then inspect its gateway PID "
        "descriptors and logs for 'Memory provider memory_server activated' and 'HermesProvider initialized'. If "
        "still absent, enable HERMES_PLUGINS_DEBUG=1 before restart, run the profile's hermes memory status/doctor, "
        "and capture the provider-init exception. Do not migrate or split the shared DB based on descriptor absence "
        "alone.",
        0.94,
        [
            "live process inspection",
            "hermes-agent/plugins/memory/__init__.py:261-365",
            "hermes-agent/agent/agent_init.py:1802-1869",
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
                    "local source inspection + live process/fd inspection + official Hermes docs",
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
                    "cur-018",
                    "cmms",
                    "hermes",
                    "memory-provider",
                    "invest-agent",
                    "gateway",
                    "configuration",
                    "lazy-initialization",
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
                assert last is not None
                raise last
        print("saved", ids)
    finally:
        await provider.close()

asyncio.run(main())
