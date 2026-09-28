#!/usr/bin/env bash
# Run a command with neon-ducklake's secrets exported from the login keyring.
# Never echoes a secret. Exits 2 (MESA local install missing) / 3 (NEON token) / 4 (CyVerse password)
# when unavailable.
#   scripts/with-secrets.sh .venv/bin/neon-ducklake preflight
# Needs the MESA local install in $MESA_HOME (default ~/.mesa): bin/mesa-keyring-common.sh, cyverse-user
# and .venv (python keyring). Keyring entries: service "neon" account "api-token" (NEON token), service
# "cyverse" account <CyVerse user> (password).
set -euo pipefail
set +x
MESA_HOME="${MESA_HOME:-$HOME/.mesa}"
common="$MESA_HOME/bin/mesa-keyring-common.sh"
if [ ! -r "$common" ]; then
  echo "with-secrets: $common not found. It is part of the MESA local install (set MESA_HOME if MESA lives" \
       "elsewhere); install MESA first, then store the secrets with its keyring helpers." >&2
  exit 2
fi
. "$common"   # MESA_HOME, CYVERSE_USERNAME, D-Bus fix, mesa_keyring_password()
export MESA_HOME   # neon_ducklake.env resolves the same install (cyverse-user, mesa-mcp for curation)
[ -z "${CYVERSE_USERNAME:-}" ] || export CYVERSE_USERNAME
if [ ! -x "$MESA_HOME/.venv/bin/python" ]; then
  echo "with-secrets: $MESA_HOME/.venv/bin/python not found (MESA local install incomplete)" >&2
  exit 2
fi
ROOT="${NEON_DUCKLAKE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)}"

if tok="$("$MESA_HOME/.venv/bin/python" -m keyring get neon api-token 2>/dev/null)" && [ -n "$tok" ]; then
  export NEON_TOKEN="$tok"
elif [ "${NEON_REQUIRE_TOKEN:-1}" = 1 ]; then
  echo "with-secrets: NEON token unavailable in keyring (locked? store with $MESA_HOME/bin/neon-keyring-set)" >&2
  exit 3
fi
unset tok

if pw="$(mesa_keyring_password)" && [ -n "$pw" ]; then
  export MESA_MCP_IRODS__USER="$CYVERSE_USERNAME"
  export MESA_MCP_IRODS__PASSWORD="$pw"
elif [ "${IRODS_REQUIRE:-1}" = 1 ]; then
  echo "with-secrets: CyVerse password unavailable in keyring for '${CYVERSE_USERNAME:-}'" \
       "(user from \$CYVERSE_USERNAME or $MESA_HOME/cyverse-user; store with $MESA_HOME/bin/mesa-keyring-set)" >&2
  exit 4
fi
unset pw

export MESA_MCP_DUCKLAKE__CATALOG_DSN="${MESA_MCP_DUCKLAKE__CATALOG_DSN:-duckdb:///$MESA_HOME/ducklake/catalog.duckdb}"
export NEON_MCP_NEON__MAX_CONCURRENCY="${NEON_MCP_NEON__MAX_CONCURRENCY:-8}"
export NEON_MCP_NEON__RATE_LIMIT__MAX_WAIT_S="${NEON_MCP_NEON__RATE_LIMIT__MAX_WAIT_S:-300}"
export NEON_MCP_SERVER__LOG_LEVEL="${NEON_MCP_SERVER__LOG_LEVEL:-warning}"
export NEON_MCP_DOWNLOADS__DIRECTORY="${NEON_MCP_DOWNLOADS__DIRECTORY:-$ROOT/sites}"
exec "$@"
