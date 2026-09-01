import asyncio
import sys
from datetime import datetime, timezone

from _common import get_db_url

sys.path.insert(0, '/home/shtorm/memory-server/src')
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from memory_server.api.remember import remember
from memory_server.providers.sqlite_provider import SQLiteProvider

DB_URL=get_db_url()
SOURCE='curiosity-worker'
SESSION='cron_20260822_cur025_profile_repositories'
FACTS=[
('Hermes profile repositories — verdict','recommends',
 'For eight Hermes profiles, use one private monorepo with explicit profiles/<name>/ directories plus a shared/ layer, '
 'while keeping secrets and live state outside Git. Split into separate repositories when profiles have different '
 'collaborators, trust boundaries, or independent deployment cadence; do not create eight polyrepos by default.',
 0.97,
 ['CUR-025 research archive',
  'https://github.com/NousResearch/hermes-agent',
  'https://12factor.net/config']),
('Hermes configuration as code — tracked boundary','requires',
 'Track redacted config templates, SOUL.md, AGENTS.md, profile-owned skills, custom plugins, README/restore scripts, '
 '.env.example placeholders, and validation tests. Keep .env with real values, auth/OAuth/token files, private keys, '
 'memories, state.db, sessions, response stores, logs, caches, browser profiles, WAL/SHM, virtualenvs, and private '
 'chat exports out of Git. Treat live config.yaml as sensitive until redaction proves otherwise.',
 0.98,
 ['Local Hermes AGENTS.md',
  'https://12factor.net/config',
  'CUR-025 research archive']),
('Hermes profile monorepo versus polyrepo','compares',
 'A monorepo gives one review/backup/scanner policy and atomic shared-skill changes but has a larger disclosure blast '
 'radius. Polyrepo gives stronger access isolation and independent history/release cadence but multiplies CI, scanner, '
 'synchronization, and restore overhead. Personal/work trust boundaries justify separate repos; directory separation '
 'alone is not an access boundary.',
 0.96,
 ['CUR-025 research archive',
  'https://github.com/NousResearch/hermes-agent']),
('Git secret-scanning guardrails for Hermes profiles','requires',
 'Use pre-commit Gitleaks plus CI history scanning, and enable GitHub secret scanning/push protection where available. '
 'GitHub scans repository history and can detect generic/custom/provider patterns; push protection blocks detected '
 'secrets before repository history, but is feature-gated and bypasses are auditable. TruffleHog can complement this '
 'with classification and live-credential validation. Scanners are guardrails, not a substitute for never committing '
 'secrets and rotating any leak.',
 0.98,
 ['https://docs.github.com/en/code-security/concepts/secret-security/secret-scanning',
  'https://docs.github.com/en/code-security/concepts/secret-security/push-protection',
  'https://github.com/gitleaks/gitleaks/blob/master/README.md',
  'https://github.com/trufflesecurity/trufflehog/blob/main/README.md']),
('CUR-025 implementation estimate and restore design','recommends',
 'Initial eight-profile monorepo layout, redaction, scanner, and restore smoke test should take roughly half a day to '
 'one day; ongoing overhead is low with one CI workflow. Restore should require an explicit profile name, render '
 'tracked config templates, inject secrets separately, run hermes doctor, and verify no state DB is tracked. A '
 'concrete remaining decision is whether any profiles cross personal/work access boundaries.',
 0.92,
 ['CUR-025 research archive',
  'Local Hermes AGENTS.md'])]

def rebuild(p):
 e=create_async_engine(DB_URL,echo=False,connect_args={'timeout':60})
 p._engine=e
 p._session_factory=async_sessionmaker(e,class_=AsyncSession,expire_on_commit=False)
async def main():
 p=SQLiteProvider(url=DB_URL)
 rebuild(p)
 ids=[]
 try:
  for s,pr,o,c,srcs in FACTS:
   md={'evidence':{'method':'official docs + upstream/local Hermes source inspection',
                   'sources':srcs,
                   'session_id':SESSION,
                   'claim_type':'fact'},
       'source_date':datetime.now(timezone.utc).strftime('%Y-%m-%d'),
       'tags':['curiosity-worker','cur-025','hermes','profiles','configuration-as-code','git','secrets','monorepo','polyrepo']}
   last=None
   for n in range(4):
    try:
     r=await remember(provider=p,subject=s,predicate=pr,object=o,confidence=c,source=SOURCE,metadata=md)
     ids.append(r['fact'].id)
     break
    except Exception as e:
     last=e
     await asyncio.sleep(10*(n+1))
   else:
    raise last
  print('saved',ids)
 finally:
  await p.close()
asyncio.run(main())
