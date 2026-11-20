# Installing this version as the Hermes memory provider

The provider is the `memory_server` package that Hermes loads from its own
virtualenv. The installation path is fixed and never moves:

```
/home/shtorm/.hermes/hermes-agent/venv/lib/python3.11/site-packages/memory_server
```

The working tree (`/home/shtorm/.hermes/workspace/cmms-upgrade-v0.12.0`) is a
separate checkout used for development and testing. Code reaches the live
provider only through the sync step below.

## Steps

1. **Sync from GitHub into the working tree.**

   ```bash
   cd /home/shtorm/.hermes/workspace/cmms-upgrade-v0.12.0
   git fetch origin --prune
   git pull --ff-only origin cmms-upgrade-v0.12.0
   ```

2. **Install into the live venv, with backups.**

   ```bash
   ./scripts/install-hermes-provider.sh install
   ```

   The script does, in order: preflight checks (branch, clean tracked tree,
   live provider and DB present) → backup of the live provider, the database
   and a git bundle into `~/backups/cmms/install-<stamp>/` → `git pull --ff-only`
   → `rsync --delete` of `src/memory_server/` into the live path → alembic head
   check and, only if the DB is behind, `alembic upgrade head` → verification
   (import from the Hermes interpreter, DB revision, `include_inactive`,
   native `invalidate`).

3. **Reload the service yourself, from a normal shell.**

   The script deliberately does not touch the Hermes service: a restart issued
   from inside a Hermes session kills its own process tree. Run the reload in a
   terminal that is not the agent:

   ```bash
   systemctl --user restart hermes-gateway
   systemctl --user status hermes-gateway --no-pager | head -5
   ```

4. **Check it afterwards.**

   ```bash
   ./scripts/install-hermes-provider.sh verify
   ```

   Expected: `imported …`, `database revision: 0007`, `include_inactive
   present`, `native invalidate present`.

## Rollback

```bash
./scripts/install-hermes-provider.sh rollback
```

Restores the live provider from the most recent backup recorded in
`~/backups/cmms/last-install-backup`. The database is **not** restored
automatically — if a migration ran, copy `memory.db` out of the same backup
directory while the service is stopped.

## Notes

- Two Python environments are involved on purpose: the Hermes venv (3.11) runs
  the provider, while the repo's `.venv` (3.14) runs tests and alembic. The
  provider is pure Python, so the same source serves both.
- The database lives at `/home/shtorm/.hermes/data/memory.db` and is never
  moved by this procedure; only the code path is refreshed.
- `git pull --ff-only` is used deliberately: a divergent history must be
  resolved by a human, not by force.
