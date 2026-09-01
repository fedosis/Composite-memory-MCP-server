import asyncio, sys
from _common import get_db_url
from datetime import datetime, timezone
sys.path.insert(0, '/home/shtorm/memory-server/src')
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from memory_server.providers.sqlite_provider import SQLiteProvider
from memory_server.api.remember import remember

DB_URL=get_db_url()
SOURCE='curiosity-worker'
SESSION='cron_20260824_cur035_tourvisor_acceptance_pack'
SOURCES=['https://api.tourvisor.ru/search/docs','https://support.tourvisor.ru/gateway/259-poluchenie-tokenov-avtorizatsii','https://tourvisor.ru/b2b/ddapi','https://tourvisor.ru/b2b/tariffs','https://tourvisor.ru/wiki/export','https://github.com/max-kut/tourvisor']
FACTS=[
('Tourvisor CUR-035 public authentication','requires','The current official tv-search-gateway documentation says the API uses JWT authentication: create a token in the Tourvisor PRO travel-agent account under Settings > Integrations > API Integration, then send Authorization: Bearer <JWT> on every request. Token expiry is optional and a token is not shown again after the creation dialog closes.',0.99),
('Tourvisor CUR-035 acceptance-flow contract','supports','The documented integration flow is asynchronous: load references, start a tour search and receive searchId, poll status/progress, read incremental or final results, fetch tour details, then call tour flights for flight and final-price actualization. Continuation search performs additional operator requests and is counted as another search request.',0.99),
('Tourvisor CUR-035 public limits','limits','Public docs state 120 requests/min for references and hotel/room descriptions, 300 requests/min for other methods, 300 requests/day in introductory mode, and 3,000 counted search requests/day included before overage. Counted calls include search start, continuation, and flight/price actualization; result retrieval is excluded.',0.99),
('Tourvisor CUR-035 commercial blocker','blocks','Public pages expose module starting prices and a 10-day Tourvisor PRO trial, but do not verify an API sandbox, vendor-issued credential set, JWT refresh semantics, SLA, concurrency/overage contract, IP/Russia-network guarantee, or complete API entitlement pricing. Credentialed vendor evidence is required before production integration.',0.98),
('Tourvisor CUR-035 legacy-auth warning','warns','A public unfinished PHP wrapper shows an older login/password pattern and the separate legacy order-export API uses authkey. These are lower-authority or different contracts and must not override the current official JWT search-gateway documentation.',0.98),
]

def rebuild(p):
    e=create_async_engine(DB_URL, echo=False, connect_args={'timeout':60})
    p._engine=e; p._session_factory=async_sessionmaker(e, class_=AsyncSession, expire_on_commit=False)

async def main():
    p=SQLiteProvider(url=DB_URL); rebuild(p); ids=[]
    try:
        for s,pr,o,c in FACTS:
            md={'evidence':{'method':'official Tourvisor API/support/commercial pages plus low-authority wrapper comparison','sources':SOURCES,'session_id':SESSION,'claim_type':'fact'},'source_date':datetime.now(timezone.utc).strftime('%Y-%m-%d'),'tags':['curiosity-worker','cur-035','tourvisor','travel-agent','api','acceptance-pack','vendor-contract']}
            last=None
            for n in range(4):
                try:
                    r=await remember(provider=p, subject=s, predicate=pr, object=o, confidence=c, source=SOURCE, metadata=md)
                    ids.append(str(r['fact'].id)); break
                except Exception as e:
                    last=e; await asyncio.sleep(10*(n+1))
            else: raise last if last is not None else RuntimeError('remember failed')
        print('saved', ids)
    finally:
        await p.close()
asyncio.run(main())
