"""Save CUR-026 Tourvisor API findings to CMMS."""
import asyncio
from datetime import datetime, timezone

from _common import get_db_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from memory_server.api.remember import remember
from memory_server.providers.sqlite_provider import SQLiteProvider

DB_URL = get_db_url()
SOURCE = "curiosity-worker"
SESSION = "cron_20260824_curiosity_cur026_tourvisor_api"

def rebuild(provider):
    engine = create_async_engine(DB_URL, echo=False, connect_args={"timeout": 60})
    provider._engine = engine
    provider._session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

FACTS = [
    (
        "Tourvisor current search API — public contract",
        "documents",
        "The official tv-search-gateway v1.2.1 documentation at https://api.tourvisor.ru/search/docs documents "
        "JSON/UTF-8 responses, JWT authorization, and separately billable search, hotel descriptions, room "
        "descriptions, and hot-tour services. Search is asynchronous: start search, poll status/results, and "
        "optionally continue the search; each continuation makes more operator calls and counts as another search "
        "query.",
        0.97,
    ),
    (
        "Tourvisor API public quotas — CUR-026",
        "limits",
        "The official docs state 120 requests/minute for reference methods and hotel/room descriptions, 300 "
        "requests/minute for all other methods, and an introductory mode capped at 300 requests/day. Search "
        "entitlement includes 3,000 search queries/day; overage is charged. Reference, tour-data, hotel-info, and "
        "hot-tour methods do not count against that daily search quota.",
        0.97,
    ),
    (
        "Tourvisor API search constraints — CUR-026",
        "constrains",
        "Current public docs specify date range no more than 21 days, nights 1..28 with range no more than 10, adults "
        "1..6, up to 3 child ages, up to 30 hotel IDs, and currency as ISO code; USD/EUR search is not directly "
        "supported and uses CU where applicable. Search results include hotel/tour/operator/meal/room IDs, prices, "
        "fuel charges, and charter/direct flags.",
        0.96,
    ),
    (
        "Tourvisor API commercial pricing — CUR-026",
        "advertises",
        "The official B2B API page currently advertises starting prices of 5,000 RUB/month for tour search, 3,000 "
        "RUB/month for hot tours, and 3,000 RUB/month for hotel descriptions. The page does not publicly expose every "
        "module price, exact overage schedule, SLA, or production contract terms.",
        0.94,
    ),
    (
        "Tourvisor API integration blocker — CUR-026",
        "requires",
        "Before implementing production Touragent Helper integration, obtain a vendor contract/credentials pack: "
        "production base URL and OpenAPI file, JWT issuance/refresh rules, module entitlements, actualization and "
        "quota billing, concurrency/overage rules, sandbox account, SLA/support, data/image licensing, Russia-network "
        "access test, and request/response corpus. Do not reuse the legacy authlogin/authpass pattern from a "
        "third-party PHP wrapper because current official docs specify JWT.",
        0.98,
    ),
]



async def main():
    provider = SQLiteProvider(url=DB_URL)
    rebuild(provider)
    ids=[]
    try:
        for subject,predicate,obj,confidence in FACTS:
            metadata={
                "tags":
                [
                    "curiosity-worker",
                    "cur-026",
                    "tourvisor",
                    "travel-agent",
                    "api",
                    "commercial-contract",
                ],
                "source_date":
                datetime.now(timezone.utc).strftime('%Y-%m-%d'),
                "evidence":
                {
                    "method":
                    "web research",
                    "sources":
                    [
                        "https://api.tourvisor.ru/search/docs",
                        "https://tourvisor.ru/b2b/ddapi",
                        "https://tourvisor.ru/b2b/tariffs",
                        "https://wiki.tourvisor.ru/export",
                    ],
                    "session_id":
                    SESSION,
                    "claim_type":
                    "fact",
                },
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
                raise RuntimeError(f"remember failed after retries: {last!r}")
    finally:
        await provider.close()
    print("SAVED",len(ids),"facts")
    print("\n".join(ids))

if __name__ == "__main__":
    asyncio.run(main())
