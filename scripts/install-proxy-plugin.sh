#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# install-proxy-plugin.sh — install the standalone memory_server_proxy plugin.
#
# Copies plugin-dist/memory_server_proxy/ into $HERMES_HOME/plugins/ and writes
# the $HERMES_HOME/memory_server_proxy.json sidecar (0600). With --activate the
# provider is also pointed at the plugin in $HERMES_HOME/config.yaml (backup:
# config.yaml.bak-<UTC>).
#
# The plan is printed before anything is copied: a --dry-run pass runs first
# and the real pass follows only when the dry-run succeeded.
#
# Usage:
#   scripts/install-proxy-plugin.sh --hermes-home DIR --socket PATH \
#       --expected-store-id UUID [--activate] [--dry-run] [--uninstall]
#
# PYTHON=<interpreter> overrides the interpreter (default: python3).
# ---------------------------------------------------------------------------
set -euo pipefail

PYTHON="${PYTHON:-python3}"

usage() {
    echo "usage: $0 --hermes-home DIR --socket PATH --expected-store-id UUID [--activate] [--dry-run] [--uninstall]" >&2
    exit 2
}

HERMES_HOME_ARG=""
SOCKET=""
STORE_ID=""
ACTIVATE=0
DRY_RUN=0
UNINSTALL=0

while [ "$#" -gt 0 ]; do
    case "$1" in
        --hermes-home)
            [ "$#" -ge 2 ] || usage
            HERMES_HOME_ARG="$2"; shift 2 ;;
        --socket)
            [ "$#" -ge 2 ] || usage
            SOCKET="$2"; shift 2 ;;
        --expected-store-id)
            [ "$#" -ge 2 ] || usage
            STORE_ID="$2"; shift 2 ;;
        --activate)
            ACTIVATE=1; shift ;;
        --dry-run)
            DRY_RUN=1; shift ;;
        --uninstall)
            UNINSTALL=1; shift ;;
        -h|--help)
            usage ;;
        *)
            echo "unknown argument: $1" >&2
            usage ;;
    esac
done

if [ -z "${HERMES_HOME_ARG}" ]; then
    echo "missing --hermes-home" >&2
    usage
fi

ARGS=(install-hermes-proxy
      --hermes-home "${HERMES_HOME_ARG}"
      --socket "${SOCKET}"
      --expected-store-id "${STORE_ID}")

if [ "${ACTIVATE}" -eq 1 ]; then ARGS+=(--activate); fi
if [ "${UNINSTALL}" -eq 1 ]; then ARGS+=(--uninstall); fi

echo "==> CMMS native proxy plugin"
echo "    HERMES_HOME: ${HERMES_HOME_ARG}"
echo "    socket:      ${SOCKET}"
echo "    store_id:    ${STORE_ID}"
echo ""

if [ "${DRY_RUN}" -eq 1 ]; then
    exec "${PYTHON}" -m memory_server.native.cli "${ARGS[@]}" --dry-run
fi

echo "==> Plan (dry-run, nothing written yet)"
"${PYTHON}" -m memory_server.native.cli "${ARGS[@]}" --dry-run
echo ""
echo "==> Applying"
exec "${PYTHON}" -m memory_server.native.cli "${ARGS[@]}"
