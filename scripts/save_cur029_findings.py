import asyncio
from datetime import datetime, timezone

from _common import get_db_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from memory_server.api.remember import remember
from memory_server.providers.sqlite_provider import SQLiteProvider

DB_URL=get_db_url()
SOURCE='curiosity-worker'
SESSION='cron_20260823_cur029_trust_boundary'
FACTS=[
('Hermes personal/work repository trust boundary','requires',
 'Repository-level access is the actual confidentiality boundary for GitHub/Git: a collaborator with repository read '
 'access can read the repository contents, and Git clones normally mirror the repository history. Directory names, '
 'CODEOWNERS, branch protection, and path rules govern review/change policy, not read authorization. Therefore '
 'profiles containing work-sensitive material must not share a repository with personal profiles '
 'unless every repository reader is authorized for all of it.',
 0.99,
 ['https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/'
  'repository-access-and-collaboration/permission-levels-for-a-personal-account-repository',
  'https://git-scm.com/book/en/v2/Getting-Started-About-Version-Control',
  'https://docs.github.com/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners']),
('Hermes profile repository split decision','recommends',
 'Keep default, coder, lander, invest-agent, and codex-alter-ego together only if they share the same owner, '
 'collaborators, confidentiality level, and backup/restore policy. Put travel-agent and travel-agent-dev in a separate '
 'private repository when they are treated as a product, may gain collaborators, contain business-specific material, '
 'or need independent deployment/release history. Split personal versus work profiles immediately; split only '
 'within the same trust boundary otherwise.',
 0.97,
 ['CUR-025 findings archive',
  'https://docs.github.com/en/repositories/creating-and-managing-repositories/about-repositories',
  'https://buildkite.com/resources/blog/monorepo-ci-best-practices']),
('Hermes monorepo controls within one trust boundary','requires',
 'For an allowed monorepo, use explicit profiles/<name>/ and shared/ directories, CODEOWNERS and required reviews for '
 'ownership, rulesets to protect branches and block risky paths, one scanner policy, and a restore test. These '
 'controls improve change accountability and prevent accidental commits, but do not make a directory confidential '
 'from someone who can clone the repository.',
 0.98,
 ['https://docs.github.com/repositories/managing-your-repositorys-settings-and-features/'
  'customizing-your-repository/about-code-owners',
  'https://docs.github.com/repositories/configuring-branches-and-merges-in-your-repository/'
  'managing-rulesets/about-rulesets']),
('Hermes Git repository access offboarding risk','warns',
 'Removing a GitHub collaborator revokes repository access and may delete private forks, but GitHub documents that '
 'existing local clones remain and the owner is responsible for confidential data retained there. Offboarding '
 'therefore requires a separate local-clone/credential/data handling process; repository splitting reduces '
 'future exposure but cannot retroactively erase copies or history.',
 0.99,
 ['https://docs.github.com/articles/managing-an-individual-s-access-to-an-organization-repository']),
('CUR-029 concrete profile grouping','recommends',
 'Provisional grouping: personal monorepo = default, coder, invest-agent, lander, codex-alter-ego only if all are '
 'personal and share one access class; product/work repo = travel-agent and travel-agent-dev; use '
 'separate repositories for any profile tied to employer/customer data or an external collaborator. This is a '
 'policy decision, not a technical requirement to create one repository per profile.',
 0.95,
 ['CUR-025 findings archive',
  'CUR-029 research'])
]

def rebuild(p):
    e=create_async_engine(DB_URL, echo=False, connect_args={'timeout':60})
    p._engine=e
    p._session_factory=async_sessionmaker(e, class_=AsyncSession, expire_on_commit=False)

async def main():
    p=SQLiteProvider(url=DB_URL)
    rebuild(p)
    ids=[]
    try:
        for s,pr,o,c,srcs in FACTS:
            md={'evidence':{'method':'official Git/GitHub documentation plus monorepo practice source',
                            'sources':srcs,
                            'session_id':SESSION,
                            'claim_type':'fact'},
                'source_date':datetime.now(timezone.utc).strftime('%Y-%m-%d'),
                'tags':['curiosity-worker','cur-029','hermes','profiles','git','monorepo','polyrepo',
                        'trust-boundary','access-control']}
            last=None
            for n in range(4):
                try:
                    r=await remember(
                        provider=p,
                        subject=s,
                        predicate=pr,
                        object=o,
                        confidence=c,
                        source=SOURCE,
                        metadata=md)
                    ids.append(r['fact'].id)
                    break
                except Exception as e:
                    last=e
                    await asyncio.sleep(10*(n+1))
            else:
                raise last if last is not None else RuntimeError('remember failed without exception')
        print('saved', ids)
    finally:
        await p.close()

asyncio.run(main())
