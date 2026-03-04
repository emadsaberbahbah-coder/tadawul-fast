#!/usr/bin/env bash
# scripts/start_web.sh
# =============================================================================
# TADAWUL FAST BRIDGE — RENDER-SAFE START WRAPPER (v1.0.0)
# =============================================================================
# Why this file exists:
# - Render startCommand is executing scripts/start_web.sh
# - This must be a REAL shell script (not Python) to avoid confusion.
# - It respects PORT, WEB_CONCURRENCY, and other UVicorn knobs.
#
# Default behavior:
# - Binds to ${PORT:-8000} on 0.0.0.0
# - Workers default 1 (safe), unless WEB_CONCURRENCY is set (>1)
# - Uses python -m uvicorn (so it works in Render venv)
# =============================================================================

set -euo pipefail

as_int() {
  local raw="${1:-}" def="${2:-0}"
  case "$raw" in
    ""|*[!0-9]*) printf "%s" "$def" ;;
    *) printf "%s" "$raw" ;;
  esac
}

clamp_min1() {
  local v
  v="$(as_int "${1:-}" "1")"
  if [ "$v" -lt 1 ]; then v="1"; fi
  printf "%s" "$v"
}

clamp_range() {
  local v lo hi
  v="$(as_int "${1:-}" "${2:-1}")"
  lo="$(as_int "${2:-1}" "1")"
  hi="$(as_int "${3:-64}" "64")"
  if [ "$v" -lt "$lo" ]; then v="$lo"; fi
  if [ "$v" -gt "$hi" ]; then v="$hi"; fi
  printf "%s" "$v"
}

loglvl() {
  local v
  v="$(printf "%s" "${1:-info}" | tr "[:upper:]" "[:lower:]")"
  case "$v" in
    critical|error|warning|info|debug|trace) printf "%s" "$v" ;;
    *) printf "%s" "info" ;;
  esac
}

truthy() {
  case "$(printf "%s" "${1:-}" | tr "[:upper:]" "[:lower:]")" in
    1|true|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

pos_int_or_empty() {
  local raw="${1:-}"
  case "$raw" in
    ""|*[!0-9]*) printf "%s" "" ;;
    *)
      if [ "$raw" -gt 0 ]; then printf "%s" "$raw"; else printf "%s" ""; fi
      ;;
  esac
}

# -----------------------------------------------------------------------------
# Resolve app import string (default main:app)
# -----------------------------------------------------------------------------
APP_IMPORT="${APP_IMPORT_STRING:-}"
if [ -z "$APP_IMPORT" ]; then
  APP_MODULE="${APP_MODULE:-main}"
  APP_VAR="${APP_VAR:-app}"
  APP_IMPORT="${APP_MODULE}:${APP_VAR}"
fi

# -----------------------------------------------------------------------------
# Resolve bind host/port (Render requires PORT)
# -----------------------------------------------------------------------------
HOST="${HOST:-0.0.0.0}"
PORT_RAW="${PORT:-8000}"
PORT_NUM="$(clamp_min1 "$(as_int "$PORT_RAW" "8000")")"

# -----------------------------------------------------------------------------
# Worker policy (safe default 1, clamp with WORKERS_MAX)
# -----------------------------------------------------------------------------
# Prefer WEB_CONCURRENCY (Render conventional). If not set, default 1.
WC_RAW="${WEB_CONCURRENCY:-}"
if [ -z "$WC_RAW" ]; then
  WC_RAW="1"
fi

WORKERS_MAX_RAW="${WORKERS_MAX:-4}"
WORKERS_MAX="$(clamp_range "$WORKERS_MAX_RAW" "1" "32")"
WORKERS="$(clamp_range "$WC_RAW" "1" "$WORKERS_MAX")"

# -----------------------------------------------------------------------------
# Uvicorn knobs
# -----------------------------------------------------------------------------
LL="$(loglvl "${LOG_LEVEL:-info}")"
ACCESS="${UVICORN_ACCESS_LOG:-1}"
KA="$(clamp_range "${UVICORN_KEEPALIVE:-75}" "1" "300")"
GR="$(clamp_range "${UVICORN_GRACEFUL_TIMEOUT:-30}" "1" "180")"
BL="$(clamp_range "${UVICORN_BACKLOG:-2048}" "64" "16384")"

LC="$(pos_int_or_empty "${UVICORN_LIMIT_CONCURRENCY:-}")"
LMR="$(pos_int_or_empty "${UVICORN_LIMIT_MAX_REQUESTS:-}")"
RP="${UVICORN_ROOT_PATH:-}"
LIFE="${UVICORN_LIFESPAN:-on}"

# Optional: factory mode (uvicorn CLI flag)
FACTORY_FLAG=""
if truthy "${APP_FACTORY:-0}"; then
  FACTORY_FLAG="--factory"
fi

# -----------------------------------------------------------------------------
# Build command
# -----------------------------------------------------------------------------
set -- python -m uvicorn "$APP_IMPORT" \
  --host "$HOST" \
  --port "$PORT_NUM" \
  --proxy-headers \
  --forwarded-allow-ips "*" \
  --lifespan "$LIFE" \
  --timeout-keep-alive "$KA" \
  --timeout-graceful-shutdown "$GR" \
  --log-level "$LL" \
  --no-server-header \
  --backlog "$BL"

if [ -n "$FACTORY_FLAG" ]; then
  set -- "$@" "$FACTORY_FLAG"
fi

case "$(printf "%s" "$ACCESS" | tr "[:upper:]" "[:lower:]")" in
  0|false|no|off) set -- "$@" --no-access-log ;;
  *) set -- "$@" --access-log ;;
esac

# Only add workers if > 1 (avoids slow bind / port scan misses on small instances)
if [ "$WORKERS" -gt 1 ]; then
  set -- "$@" --workers "$WORKERS"
fi

if [ -n "$LC" ]; then
  set -- "$@" --limit-concurrency "$LC"
fi

if [ -n "$LMR" ]; then
  set -- "$@" --limit-max-requests "$LMR"
fi

if [ -n "$RP" ]; then
  set -- "$@" --root-path "$RP"
fi

# -----------------------------------------------------------------------------
# Exec
# -----------------------------------------------------------------------------
echo "TFB start_web.sh | app=${APP_IMPORT} host=${HOST} port=${PORT_NUM} workers=${WORKERS} log_level=${LL}"
exec "$@"
