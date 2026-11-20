#!/usr/bin/env bash
# CMMS memory provider installer for Hermes.
#
# Keeps the installation path unchanged: the live code is the Hermes venv's
# site-packages/memory_server, refreshed in place from the git repository that
# is synced from GitHub. The database path is never moved either.
#
#   ./install-hermes-provider.sh verify    check state, change nothing
#   ./install-hermes-provider.sh install   sync repo, back up live copy + DB, install
#   ./install-hermes-provider.sh rollback  restore the most recent backup
#
# The script never restarts the Hermes gateway: that kills the process tree it
# would be running under. It prints the command for the operator to run.

set -euo pipefail

REPO="${CMMS_REPO:-/home/shtorm/.hermes/workspace/cmms-upgrade-v0.12.0}"
BRANCH="${CMMS_BRANCH:-cmms-upgrade-v0.12.0}"
REMOTE="${CMMS_REMOTE:-origin}"
HERMES_VENV="${HERMES_VENV:-/home/shtorm/.hermes/hermes-agent/venv}"
PY_VER="$(ls "$HERMES_VENV/lib" | grep -E '^python3\.[0-9]+$' | sort -V | tail -1)"
TARGET="$HERMES_VENV/lib/$PY_VER/site-packages/memory_server"
HERMES_PY="$HERMES_VENV/bin/python"
DB="${CMMS_DB:-/home/shtorm/.hermes/data/memory.db}"
BACKUP_ROOT="${CMMS_BACKUP_ROOT:-/home/shtorm/backups/cmms}"
STAMP="$(date +%Y%m%d-%H%M%S)"

say()  { printf '\033[1m%s\033[0m\n' "$*"; }
ok()   { printf '  ok    %s\n' "$*"; }
warn() { printf '  warn  %s\n' "$*"; }
die()  { printf '  FAIL  %s\n' "$*" >&2; exit 1; }

preflight() {
  say "preconditions"
  git -C "$REPO" rev-parse --git-dir >/dev/null 2>&1 || die "not a git repository: $REPO"
  [ -x "$HERMES_PY" ] || die "no Hermes python at $HERMES_PY"
  [ -d "$TARGET" ] || die "live provider not found at $TARGET"
  [ -f "$DB" ] || die "database not found at $DB"
  command -v rsync >/dev/null || die "rsync is required"
  git -C "$REPO" rev-parse --abbrev-ref HEAD | grep -qx "$BRANCH" \
    || die "repo is on $(git -C "$REPO" rev-parse --abbrev-ref HEAD), expected $BRANCH"
  if [ -n "$(git -C "$REPO" status --porcelain --untracked-files=no)" ]; then
    die "tracked files have uncommitted changes; commit or stash them first"
  fi
  ok "repo on $BRANCH, clean tracked tree"
  ok "live provider: $TARGET"
  ok "database: $DB"
}

db_revision() {
  sqlite3 "$DB" "select version_num from alembic_version;" 2>/dev/null || echo "unknown"
}

verify() {
  say "verification"
  local rev live_rev
  rev="$(cd "$REPO" && "$HERMES_PY" -c "import memory_server, sys; print(getattr(memory_server,'__version__','?'))" 2>/dev/null || true)"
  "$HERMES_PY" - <<'PY' || die "live provider does not import"
import memory_server
from memory_server.plugins.hermes.provider import HermesProvider
src = open(memory_server.__file__).read()
print("  ok    imported", memory_server.__file__)
PY
  live_rev="$(db_revision)"
  ok "database revision: $live_rev"
  if grep -q "include_inactive" "$TARGET/api/search.py"; then
    ok "include_inactive present in live provider"
  else
    warn "include_inactive missing in live provider (older build)"
  fi
  if [ -f "$TARGET/api/invalidate.py" ]; then
    ok "native invalidate present"
  else
    warn "native invalidate missing (older build)"
  fi
  local repo_head live_files
  repo_head="$(git -C "$REPO" rev-parse --short HEAD)"
  live_files="$(find "$TARGET" -name '*.py' -not -path '*/__pycache__/*' | wc -l)"
  ok "repo HEAD $repo_head, live provider has $live_files python files"
}

backup() {
  local dir="$BACKUP_ROOT/install-$STAMP"
  say "backup -> $dir"
  mkdir -p "$dir"
  tar -czf "$dir/live-provider.tar.gz" -C "$(dirname "$TARGET")" "$(basename "$TARGET")"
  sqlite3 "$DB" ".backup '$dir/memory.db'"
  git -C "$REPO" bundle create "$dir/repo.bundle" --all >/dev/null
  ok "live provider, database and git bundle saved"
  printf '%s\n' "$dir" > "$BACKUP_ROOT/last-install-backup"
}

sync_repo() {
  say "sync repository from GitHub"
  git -C "$REPO" fetch "$REMOTE" --prune
  git -C "$REPO" pull --ff-only "$REMOTE" "$BRANCH"
  ok "repo at $(git -C "$REPO" rev-parse --short HEAD) — $(git -C "$REPO" log -1 --format=%s)"
}

install_code() {
  say "install into $TARGET (path unchanged)"
  rsync -a --delete --exclude '__pycache__' --exclude '*.pyc' \
        "$REPO/src/memory_server/" "$TARGET/"
  find "$TARGET" -name '__pycache__' -type d -prune -exec rm -rf {} +
  ok "code copied"
}

migrate() {
  say "database migrations"
  local head current tmp_ini
  current="$(db_revision)"
  if [ ! -x "$REPO/.venv/bin/alembic" ]; then
    warn "no alembic in $REPO/.venv; skipping migration check"
    return
  fi
  head="$("$REPO/.venv/bin/python" - "$REPO" <<'PY'
import sys, pathlib
root = pathlib.Path(sys.argv[1])
sys.path.insert(0, str(root))
from alembic.config import Config
from alembic.script import ScriptDirectory
cfg = Config(str(root / "alembic.ini"))
cfg.set_main_option("script_location", str(root / "migrations"))
print(ScriptDirectory.from_config(cfg).get_current_head())
PY
)"
  ok "database at $current, repository head $head"
  if [ "$current" = "$head" ]; then
    ok "no migration needed"
    return
  fi
  tmp_ini="$(mktemp)"
  sed "s|^sqlalchemy.url = .*|sqlalchemy.url = sqlite:///$DB|" "$REPO/alembic.ini" > "$tmp_ini"
  "$REPO/.venv/bin/alembic" -c "$tmp_ini" upgrade head
  rm -f "$tmp_ini"
  ok "migrated to $head"
}

rollback() {
  say "rollback"
  local dir
  dir="$(cat "$BACKUP_ROOT/last-install-backup" 2>/dev/null || true)"
  [ -n "$dir" ] && [ -d "$dir" ] || die "no recorded backup under $BACKUP_ROOT"
  [ -f "$dir/live-provider.tar.gz" ] || die "backup archive missing: $dir"
  rm -rf "$TARGET"
  tar -xzf "$dir/live-provider.tar.gz" -C "$(dirname "$TARGET")"
  ok "live provider restored from $dir"
  warn "database was not restored; if a migration ran, restore $dir/memory.db manually"
  warn "operator must reload the service to load the restored code"
}

finish() {
  say "done"
  cat <<EOF
Provider code is installed in place; the database is untouched beyond any
migration. The running Hermes process still holds the previous modules, so it
has to be reloaded by the operator from a normal shell — this script does not
touch the service, by design.

See scripts/INSTALL-PROVIDER.md for the operator steps.

Rollback if anything looks wrong:

    $0 rollback

Database: $DB
Backups:  $BACKUP_ROOT
EOF
}

case "${1:-install}" in
  verify)   preflight; verify ;;
  install)  preflight; backup; sync_repo; install_code; migrate; verify; finish ;;
  rollback) rollback ;;
  *)        die "usage: $0 {verify|install|rollback}" ;;
esac
