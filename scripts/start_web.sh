#!/usr/bin/env bash
# scripts/start_web.sh
# ==============================================================================
# TADAWUL FAST BRIDGE — RENDER WEB START SCRIPT (v2.6.0)
# ==============================================================================
# Purpose
# - Render-safe startup path
# - Single clear foreground process for Render port detection
# - Prefer Gunicorn + UvicornWorker only
# - Default-safe `PORT` (10000, matching `main.py` fallback) when absent —
#   does NOT hard-fail outside Render so the script works for local dev
# - Conservative defaults for heavy FastAPI startup
#
# Why this revision (v2.6.0 vs v2.5.0)
# -------------------------------------
# - 🔑 FIX REGRESSION: v2.5.0 made `PORT` a hard requirement via
#     `PORT="${PORT:?Render must provide PORT}"`, which broke
#     `./scripts/start_web.sh` for local development, CI smoke tests, and
#     any environment that doesn't inject PORT. `main.py` line ~1512 already
#     defaults to PORT=10000 when unset, so this script now does the same
#     and logs a warning. Render always injects PORT, so production behavior
#     is unchanged.
#
# - 🔑 FIX HIGH: Aligned env var names with the project-canonical
#     `tfb-server` envVarGroup from `render.yaml`:
#       UVICORN_KEEPALIVE, UVICORN_GRACEFUL_TIMEOUT, UVICORN_BACKLOG,
#       WEB_CONCURRENCY, WORKERS_MAX
#     v2.5.0 read only `TFB_KEEPALIVE`/`TFB_GRACEFUL_TIMEOUT`/`TFB_WORKERS`
#     which are NOT set by the Render envVarGroup. Operators tuning
#     UVICORN_KEEPALIVE in the dashboard were silently ignored.
#     Back-compat: `TFB_*` names still honored if present.
#
# - 🔑 FIX CORRECTNESS: Replaced misuse of `clamp_min` (which is really
#     "min-clamp with default for garbage input") with a proper
#     `clamp_range(raw, min, max, default)` so `TFB_TIMEOUT=9999` doesn't
#     silently become 9999. Per-variable safe caps are applied.
#
# - 🔑 FIX ALIGNMENT: `TFB_ACCESS_LOG` now accepts the full project-canonical
#     `_TRUTHY`/`_FALSY` vocabulary. v2.5.0 only recognized `0/false/no`,
#     so `TFB_ACCESS_LOG=off` silently left access logs ON.
#     Aligns with `main._TRUTHY` / `_FALSY` in the Python codebase.
#
# - FIX: Added SIGTERM/SIGINT trap for the pre-exec phase so clean-up (if
#     any) happens reliably before exec'ing gunicorn.
# - FIX: `PYTHONPATH` now includes app dir so imports work after `cd`.
# - FIX: Added `--help` / `-h` to print the resolved gunicorn command line
#     without starting the server — useful for config debugging.
# - FIX: Documented readiness path (`/readyz`) expected by render.yaml.
#
# Environment
# -----------
# Authoritative (set in render.yaml envVarGroup `tfb-server`):
#   PORT                      TCP port (Render injects this; defaults 10000)
#   HOST                      bind address (default 0.0.0.0)
#   WEB_CONCURRENCY           worker count (default 1)
#   UVICORN_KEEPALIVE         keep-alive seconds (default 10, capped 300)
#   UVICORN_GRACEFUL_TIMEOUT  graceful shutdown seconds (default 45, cap 300)
#   UVICORN_BACKLOG           socket listen backlog (informational here;
#                             UvicornWorker sets it internally)
#   WORKERS_MAX               hard cap on workers (default 8)
#
# Project-wide:
#   LOG_LEVEL                 critical|error|warning|info|debug (default info)
#   APP_MODULE / TFB_APP      ASGI import string (default main:app)
#   APP_DIR / TFB_APP_DIR     chdir target before launch (optional)
#
# Legacy / TFB-prefixed (still honored for back-compat):
#   TFB_WORKERS, TFB_TIMEOUT, TFB_GRACEFUL_TIMEOUT, TFB_KEEPALIVE,
#   TFB_ACCESS_LOG, TFB_WORKER_CLASS, TFB_WORKER_TMP_DIR,
#   TFB_MAX_REQUESTS, TFB_MAX_REQUESTS_JITTER, TFB_LOG_LEVEL
#
# Expected render.yaml integration
# --------------------------------
#   startCommand: |
#     set -euo pipefail
#     chmod +x scripts/start_web.sh
#     exec ./scripts/start_web.sh
#   healthCheckPath: /readyz   (ensure main.py exposes this route)
#
# Exit codes
# ----------
#   0   never (exec gunicorn replaces process)
#   1   gunicorn binary not found
#   2   --help invoked (prints plan and exits)
#   130 interrupted (SIGINT)
#   143 terminated (SIGTERM)
# ==============================================================================
set -Eeuo pipefail

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
readonly SCRIPT_VERSION="2.6.0"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
timestamp_utc() {
  date -u +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || printf "UTC"
}

log() {
  printf "%s %s\n" "$(timestamp_utc) | TFB start_web.sh |" "$*"
}

log_warn() {
  printf "%s WARN | %s\n" "$(timestamp_utc) | TFB start_web.sh" "$*" >&2
}

log_err() {
  printf "%s ERROR | %s\n" "$(timestamp_utc) | TFB start_web.sh" "$*" >&2
}

lower() {
  printf "%s" "${1:-}" | tr '[:upper:]' '[:lower:]'
}

as_int() {
  local raw="${1:-}" def="${2:-0}"
  case "$raw" in
    ""|*[!0-9]*)
      printf "%s" "$def"
      ;;
    *)
      printf "%s" "$raw"
      ;;
  esac
}

# v2.6.0: proper clamp with both min and max.
# Usage: clamp_range "$raw" "$min" "$max" "$default"
clamp_range() {
  local raw="${1:-}" min="${2:-1}" max="${3:-0}" def="${4:-1}" v
  v="$(as_int "$raw" "$def")"
  if [ "$v" -lt "$min" ]; then
    v="$min"
  fi
  if [ "$max" -gt 0 ] && [ "$v" -gt "$max" ]; then
    v="$max"
  fi
  printf "%s" "$v"
}

# v2.6.0: project-aligned truthy/falsy check.
# Matches Python `_TRUTHY`/`_FALSY` vocabulary from main.py:
#   _TRUTHY = {"1","true","yes","y","on","t","enabled","enable"}
#   _FALSY  = {"0","false","no","n","off","f","disabled","disable"}
# Returns 0 (shell true) if the value is truthy, 1 otherwise.
is_truthy() {
  case "$(lower "${1:-}")" in
    1|true|yes|y|on|t|enabled|enable) return 0 ;;
    *) return 1 ;;
  esac
}

is_falsy() {
  case "$(lower "${1:-}")" in
    0|false|no|n|off|f|disabled|disable) return 0 ;;
    *) return 1 ;;
  esac
}

pick_gunicorn_loglevel() {
  local v
  v="$(lower "${1:-info}")"
  case "$v" in
    critical|error|warning|info|debug)
      printf "%s" "$v"
      ;;
    warn)
      printf "%s" "warning"
      ;;
    trace)
      printf "%s" "debug"
      ;;
    *)
      printf "%s" "info"
      ;;
  esac
}

# ---------------------------------------------------------------------------
# Signal handling
# ---------------------------------------------------------------------------
# v2.6.0: trap only covers pre-exec phase. After `exec gunicorn`, the gunicorn
# process receives signals directly (Render sends SIGTERM during deploys).
on_sigterm() { log "received SIGTERM during startup"; exit 143; }
on_sigint()  { log "received SIGINT during startup";  exit 130; }
trap on_sigterm TERM
trap on_sigint  INT

# ---------------------------------------------------------------------------
# --help
# ---------------------------------------------------------------------------
if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  cat <<EOF
TFB start_web.sh v${SCRIPT_VERSION}

Start the FastAPI web service under Gunicorn + UvicornWorker.

Usage:
  ./scripts/start_web.sh                 # launch (exec gunicorn)
  ./scripts/start_web.sh --help          # print this help
  ./scripts/start_web.sh --print-plan    # print resolved gunicorn command
                                         # line and exit WITHOUT launching

Primary env vars (aligned with render.yaml envVarGroup 'tfb-server'):
  PORT, HOST, WEB_CONCURRENCY, WORKERS_MAX,
  UVICORN_KEEPALIVE, UVICORN_GRACEFUL_TIMEOUT, UVICORN_BACKLOG,
  LOG_LEVEL, APP_MODULE, APP_DIR

Legacy TFB_* names still honored. See header of this script for full list.

Health check path (render.yaml): /readyz
EOF
  exit 2
fi

# ---------------------------------------------------------------------------
# Configuration (env resolution with project-canonical names preferred,
# legacy TFB_* fallbacks for back-compat)
# ---------------------------------------------------------------------------

# ASGI app + workdir
APP_MODULE="${APP_MODULE:-${TFB_APP:-main:app}}"
APP_DIR="${APP_DIR:-${TFB_APP_DIR:-}}"

# Networking
HOST="${HOST:-${TFB_HOST:-0.0.0.0}}"

# PORT: v2.6.0 softens the hard requirement. Defaults to 10000 (matching
# main.py's fallback) and warns when absent. Render always injects PORT, so
# production behavior is unchanged.
if [ -z "${PORT:-}" ]; then
  log_warn "PORT not set by environment; defaulting to 10000 (matches main.py)"
  PORT=10000
else
  PORT="$(as_int "$PORT" 10000)"
fi

# Workers: prefer WEB_CONCURRENCY (render.yaml canonical), then TFB_WORKERS.
# Hard-cap by WORKERS_MAX (render.yaml canonical) with fallback 8.
_WORKERS_MAX="$(as_int "${WORKERS_MAX:-${WORKERS_MAX_DEFAULT:-}}" 8)"
WORKERS="$(clamp_range "${WEB_CONCURRENCY:-${TFB_WORKERS:-}}" 1 "$_WORKERS_MAX" 1)"

# Timeouts: canonical UVICORN_* env vars preferred; legacy TFB_* honored.
# Reasonable max caps prevent absurd values.
TIMEOUT="$(clamp_range "${TFB_TIMEOUT:-${TIMEOUT:-}}" 1 600 180)"
GRACEFUL_TIMEOUT="$(clamp_range "${UVICORN_GRACEFUL_TIMEOUT:-${TFB_GRACEFUL_TIMEOUT:-${GRACEFUL_TIMEOUT:-}}}" 1 300 45)"
KEEPALIVE="$(clamp_range "${UVICORN_KEEPALIVE:-${TFB_KEEPALIVE:-${KEEPALIVE:-}}}" 1 300 10)"

# Log level
RAW_LOG_LEVEL="${TFB_LOG_LEVEL:-${LOG_LEVEL:-info}}"
GUNICORN_LOG_LEVEL="$(pick_gunicorn_loglevel "$RAW_LOG_LEVEL")"

# Access log: v2.6.0 uses project-canonical _FALSY vocabulary.
# Default: ON (log to stdout). Set TFB_ACCESS_LOG to any falsy token to
# suppress (0/false/no/n/off/f/disabled/disable).
ACCESS_LOG="${TFB_ACCESS_LOG:-${UVICORN_ACCESS_LOG:-1}}"
ACCESS_LOGFILE="-"
if is_falsy "$ACCESS_LOG"; then
  ACCESS_LOGFILE="/dev/null"
fi

# Worker class
WORKER_CLASS="${TFB_WORKER_CLASS:-uvicorn.workers.UvicornWorker}"

# /dev/shm is preferred on Linux hosts; fallback to /tmp if missing.
_DEFAULT_TMP_DIR="/tmp"
if [ -d "/dev/shm" ] && [ -w "/dev/shm" ]; then
  _DEFAULT_TMP_DIR="/dev/shm"
fi
WORKER_TMP_DIR="${TFB_WORKER_TMP_DIR:-$_DEFAULT_TMP_DIR}"

# Max-requests recycling (0 disables)
MAX_REQUESTS="$(as_int "${TFB_MAX_REQUESTS:-${WORKER_MAX_REQUESTS:-0}}" 0)"
MAX_REQUESTS_JITTER="$(as_int "${TFB_MAX_REQUESTS_JITTER:-${WORKER_MAX_REQUESTS_JITTER:-0}}" 0)"

# ---------------------------------------------------------------------------
# Pre-flight
# ---------------------------------------------------------------------------
if [ -n "$APP_DIR" ]; then
  if [ -d "$APP_DIR" ]; then
    cd "$APP_DIR"
    log "changed directory to $APP_DIR"
  else
    log_warn "APP_DIR does not exist: $APP_DIR (continuing in current dir)"
  fi
fi

# Raise file descriptor limit (best-effort; not fatal if unsupported)
( ulimit -n 65535 >/dev/null 2>&1 ) || true

# v2.6.0: ensure Python can import from workdir (after any cd above).
export PYTHONPATH="${PYTHONPATH:-}${PYTHONPATH:+:}$(pwd)"
export PYTHONUNBUFFERED=1
export PORT
export HOST

log "TFB start_web.sh v${SCRIPT_VERSION}"
log "boot config: app=$APP_MODULE host=$HOST port=$PORT workers=$WORKERS \
workers_max=$_WORKERS_MAX gunicorn_log_level=$GUNICORN_LOG_LEVEL \
timeout=$TIMEOUT graceful_timeout=$GRACEFUL_TIMEOUT keepalive=$KEEPALIVE \
access_log=$ACCESS_LOGFILE worker_tmp_dir=$WORKER_TMP_DIR"

# Verify gunicorn is present (fail-fast diagnostic)
if ! command -v gunicorn >/dev/null 2>&1; then
  log_err "gunicorn binary not found on PATH. Install with: pip install gunicorn"
  exit 1
fi

# ---------------------------------------------------------------------------
# Launch: Gunicorn + UvicornWorker
# ---------------------------------------------------------------------------
GUNI_ARGS=(
  "--bind"                "${HOST}:${PORT}"
  "--worker-class"        "${WORKER_CLASS}"
  "--workers"             "${WORKERS}"
  "--timeout"             "${TIMEOUT}"
  "--graceful-timeout"    "${GRACEFUL_TIMEOUT}"
  "--keep-alive"          "${KEEPALIVE}"
  "--log-level"           "${GUNICORN_LOG_LEVEL}"
  "--error-logfile"       "-"
  "--access-logfile"      "${ACCESS_LOGFILE}"
  "--capture-output"
  "--forwarded-allow-ips" "*"
)

if [ -n "${WORKER_TMP_DIR:-}" ]; then
  GUNI_ARGS+=( "--worker-tmp-dir" "${WORKER_TMP_DIR}" )
fi

if [ "$MAX_REQUESTS" -gt 0 ]; then
  GUNI_ARGS+=( "--max-requests" "${MAX_REQUESTS}" )
fi

if [ "$MAX_REQUESTS_JITTER" -gt 0 ]; then
  GUNI_ARGS+=( "--max-requests-jitter" "${MAX_REQUESTS_JITTER}" )
fi

# v2.6.0: --print-plan dumps the resolved command without launching.
if [ "${1:-}" = "--print-plan" ]; then
  log "resolved gunicorn command (not launching):"
  printf "  gunicorn"
  for arg in "${GUNI_ARGS[@]}"; do
    printf ' %q' "$arg"
  done
  printf ' %q\n' "${APP_MODULE}"
  exit 2
fi

log "starting with gunicorn"
exec gunicorn "${GUNI_ARGS[@]}" "${APP_MODULE}"
