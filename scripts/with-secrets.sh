#!/usr/bin/env bash
# Run a command with neon-ducklake's secrets in the environment. Never echoes a secret.
#   scripts/with-secrets.sh .venv/bin/neon-ducklake preflight
#
# Where the secrets come from: NEON_DUCKLAKE_SECRETS=auto|env|keyring (default auto).
#   keyring  the login keyring through the MESA local install (sparky-1): service "neon" account "api-token"
#            (NEON token), service "cyverse" account <CyVerse user> (password). Needs $MESA_HOME (default
#            ~/.mesa) with bin/mesa-keyring-common.sh, cyverse-user and .venv (python keyring).
#   env      never touches the keyring (CyVerse DE VMs): NEON_TOKEN from the environment; iRODS from
#            MESA_MCP_IRODS__USER + MESA_MCP_IRODS__PASSWORD, else the iRODS environment file plus .irodsA
#            written by `iinit` (~/.irods/, or $IRODS_ENVIRONMENT_FILE / $IRODS_AUTHENTICATION_FILE).
#   auto     per secret: what `env` finds, else the keyring.
# Exit codes: 2 MESA local install missing (only when the keyring is needed) or a bad mode, 3 NEON token
# unavailable, 4 CyVerse credentials unavailable.
set -euo pipefail
set +x
MESA_HOME="${MESA_HOME:-$HOME/.mesa}"
export MESA_HOME   # neon_ducklake.env resolves the same install (cyverse-user, mesa-mcp for curation)
ROOT="${NEON_DUCKLAKE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)}"
mode="${NEON_DUCKLAKE_SECRETS:-auto}"
case "$mode" in
  auto|env|keyring) ;;
  *) echo "with-secrets: NEON_DUCKLAKE_SECRETS must be auto, env or keyring (got '$mode')" >&2; exit 2 ;;
esac

irods_env_file="${IRODS_ENVIRONMENT_FILE:-$HOME/.irods/irods_environment.json}"
irods_auth_file="${IRODS_AUTHENTICATION_FILE:-$HOME/.irods/.irodsA}"
# The interpreter that decodes .irodsA (python-irodsclient + mesa-mcp): this checkout's .venv by default.
py="${NEON_DUCKLAKE_PYTHON:-$ROOT/.venv/bin/python}"
[ -x "$py" ] || py="$(command -v python3 || true)"

keyring_loaded=0
load_keyring() {  # the MESA keyring helpers; exits 2 when the MESA local install is missing
  [ "$keyring_loaded" = 1 ] && return 0
  local common="$MESA_HOME/bin/mesa-keyring-common.sh"
  if [ ! -r "$common" ]; then
    echo "with-secrets: $common not found. It is part of the MESA local install (set MESA_HOME if MESA lives" \
         "elsewhere); install MESA first, then store the secrets with its keyring helpers, or provide the" \
         "secrets in the environment (NEON_DUCKLAKE_SECRETS=env, see docs/DE_SANDBOX.md)." >&2
    exit 2
  fi
  . "$common"   # MESA_HOME, CYVERSE_USERNAME, D-Bus fix, mesa_keyring_password()
  if [ ! -x "$MESA_HOME/.venv/bin/python" ]; then
    echo "with-secrets: $MESA_HOME/.venv/bin/python not found (MESA local install incomplete)" >&2
    exit 2
  fi
  keyring_loaded=1
}

# ---------------------------------------------------------------- NEON token
if [ "$mode" != keyring ] && [ -n "${NEON_TOKEN:-}" ]; then
  export NEON_TOKEN
elif [ "$mode" = env ]; then
  if [ "${NEON_REQUIRE_TOKEN:-1}" = 1 ]; then
    echo "with-secrets: NEON_DUCKLAKE_SECRETS=env but NEON_TOKEN is not set" >&2
    exit 3
  fi
else
  load_keyring
  if tok="$("$MESA_HOME/.venv/bin/python" -m keyring get neon api-token 2>/dev/null)" && [ -n "$tok" ]; then
    export NEON_TOKEN="$tok"
  elif [ "${NEON_REQUIRE_TOKEN:-1}" = 1 ]; then
    echo "with-secrets: NEON token unavailable in keyring (locked? store with $MESA_HOME/bin/neon-keyring-set)" >&2
    exit 3
  fi
  unset tok
fi

# ---------------------------------------------------------------- CyVerse / iRODS credentials
# Exports the user (plus host/port/zone when those are unset) from irods_environment.json and the password
# decoded from .irodsA. Returns 1 when either is unusable.
irods_from_env_file() {
  [ -n "$py" ] || return 1
  local fields user pw h p z
  fields="$(IRODS_ENVIRONMENT_FILE="$irods_env_file" "$py" -c '
import json, os
d = json.load(open(os.environ["IRODS_ENVIRONMENT_FILE"]))
for key, var in (("irods_user_name", "USER"), ("irods_host", "HOST"), ("irods_port", "PORT"), ("irods_zone_name", "ZONE")):
    v = str(d.get(key) or "").strip()
    if v and "\n" not in v:
        print(var + "=" + v)
' 2>/dev/null)" || return 1
  user="$(printf '%s\n' "$fields" | sed -n 's/^USER=//p')"
  [ -n "$user" ] && [ "$user" != anonymous ] || return 1
  pw="$(IRODS_AUTHENTICATION_FILE="$irods_auth_file" "$py" -c '
import sys
from mesa_mcp.auth.irods_env import load_irods_password
sys.stdout.write(load_irods_password() or "")
' 2>/dev/null)" || return 1
  [ -n "$pw" ] || return 1
  export MESA_MCP_IRODS__USER="$user"
  export MESA_MCP_IRODS__PASSWORD="$pw"
  export CYVERSE_USERNAME="${CYVERSE_USERNAME:-$user}"
  h="$(printf '%s\n' "$fields" | sed -n 's/^HOST=//p')"
  p="$(printf '%s\n' "$fields" | sed -n 's/^PORT=//p')"
  z="$(printf '%s\n' "$fields" | sed -n 's/^ZONE=//p')"
  [ -z "$h" ] || export MESA_MCP_IRODS__HOST="${MESA_MCP_IRODS__HOST:-$h}"
  [ -z "$p" ] || export MESA_MCP_IRODS__PORT="${MESA_MCP_IRODS__PORT:-$p}"
  [ -z "$z" ] || export MESA_MCP_IRODS__ZONE="${MESA_MCP_IRODS__ZONE:-$z}"
  return 0
}

if [ "$mode" != keyring ] && [ -n "${MESA_MCP_IRODS__USER:-}" ] && [ -n "${MESA_MCP_IRODS__PASSWORD:-}" ]; then
  export MESA_MCP_IRODS__USER MESA_MCP_IRODS__PASSWORD
  export CYVERSE_USERNAME="${CYVERSE_USERNAME:-$MESA_MCP_IRODS__USER}"
elif [ "$mode" != keyring ] && [ -r "$irods_env_file" ] && [ -r "$irods_auth_file" ] && irods_from_env_file; then
  :
elif [ "$mode" = env ]; then
  if [ "${IRODS_REQUIRE:-1}" = 1 ]; then
    echo "with-secrets: NEON_DUCKLAKE_SECRETS=env but neither MESA_MCP_IRODS__USER/MESA_MCP_IRODS__PASSWORD" \
         "nor a readable $irods_env_file with $irods_auth_file (run iinit) is available" >&2
    exit 4
  fi
else
  load_keyring
  if pw="$(mesa_keyring_password)" && [ -n "$pw" ]; then
    export MESA_MCP_IRODS__USER="$CYVERSE_USERNAME"
    export MESA_MCP_IRODS__PASSWORD="$pw"
  elif [ "${IRODS_REQUIRE:-1}" = 1 ]; then
    echo "with-secrets: CyVerse password unavailable in keyring for '${CYVERSE_USERNAME:-}'" \
         "(user from \$CYVERSE_USERNAME or $MESA_HOME/cyverse-user; store with $MESA_HOME/bin/mesa-keyring-set)" >&2
    exit 4
  fi
  unset pw
fi
[ -z "${CYVERSE_USERNAME:-}" ] || export CYVERSE_USERNAME

export MESA_MCP_DUCKLAKE__CATALOG_DSN="${MESA_MCP_DUCKLAKE__CATALOG_DSN:-duckdb:///$MESA_HOME/ducklake/catalog.duckdb}"
export NEON_MCP_NEON__MAX_CONCURRENCY="${NEON_MCP_NEON__MAX_CONCURRENCY:-8}"
export NEON_MCP_NEON__RATE_LIMIT__MAX_WAIT_S="${NEON_MCP_NEON__RATE_LIMIT__MAX_WAIT_S:-300}"
export NEON_MCP_SERVER__LOG_LEVEL="${NEON_MCP_SERVER__LOG_LEVEL:-warning}"
export NEON_MCP_DOWNLOADS__DIRECTORY="${NEON_MCP_DOWNLOADS__DIRECTORY:-$ROOT/sites}"
exec "$@"
