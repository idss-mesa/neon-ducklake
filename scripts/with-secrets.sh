#!/usr/bin/env bash
# Run a command with neon-ducklake's secrets exported from the login keyring.
# Never echoes a secret. Exits 3 (NEON token) / 4 (CyVerse password) when unavailable.
#   scripts/with-secrets.sh .venv/bin/neon-ducklake preflight
set -euo pipefail
set +x
. "$HOME/.mesa/bin/mesa-keyring-common.sh"   # MESA_HOME, CYVERSE_USERNAME, D-Bus fix, mesa_keyring_password()

if tok="$("$MESA_HOME/.venv/bin/python" -m keyring get neon api-token 2>/dev/null)" && [ -n "$tok" ]; then
  export NEON_TOKEN="$tok"
elif [ "${NEON_REQUIRE_TOKEN:-1}" = 1 ]; then
  echo "with-secrets: NEON token unavailable in keyring (locked? store with ~/.mesa/bin/neon-keyring-set)" >&2
  exit 3
fi
unset tok

if pw="$(mesa_keyring_password)" && [ -n "$pw" ]; then
  export MESA_MCP_IRODS__USER="$CYVERSE_USERNAME"
  export MESA_MCP_IRODS__PASSWORD="$pw"
elif [ "${IRODS_REQUIRE:-1}" = 1 ]; then
  echo "with-secrets: CyVerse password unavailable in keyring for '$CYVERSE_USERNAME'" >&2
  exit 4
fi
unset pw

export MESA_MCP_DUCKLAKE__CATALOG_DSN="${MESA_MCP_DUCKLAKE__CATALOG_DSN:-duckdb:///$MESA_HOME/ducklake/catalog.duckdb}"
export NEON_MCP_NEON__MAX_CONCURRENCY="${NEON_MCP_NEON__MAX_CONCURRENCY:-8}"
export NEON_MCP_NEON__RATE_LIMIT__MAX_WAIT_S="${NEON_MCP_NEON__RATE_LIMIT__MAX_WAIT_S:-300}"
export NEON_MCP_SERVER__LOG_LEVEL="${NEON_MCP_SERVER__LOG_LEVEL:-warning}"
export NEON_MCP_DOWNLOADS__DIRECTORY="${NEON_MCP_DOWNLOADS__DIRECTORY:-/home/tswetnam/neon-ducklake/sites}"
exec "$@"
