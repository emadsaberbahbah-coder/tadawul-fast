#!/usr/bin/env bash
# scripts/start_web.sh
# ==============================================================================
# TADAWUL FAST BRIDGE — RENDER WEB START SCRIPT (v2.2.0)
# ==============================================================================
# Goals
# - Render-safe: always bind to 0.0.0.0:$PORT (Render port scanner compatible)
# - Deterministic defaults: app=main:app, workers=1, log_level=info
# - Production hardening: proxy headers, sane timeouts, graceful shutdown
# - Configurable: TFB_APP, TFB_WORKERS/WEB_CONCURRENCY, TFB_LOG_LEVEL, etc.
# - Fallback: if gunicorn missing, run uvicorn directly
# ==============================================================================

set -euo pipefail

# -------------------------------
# Helpers
# -------------------------------
log() { printf "%s %s\n" "TFB start_web.sh |" "$*"; }

as_int() {
  # usage: as_int "raw" "default"
  local raw="${1:-}" def="${2:-0}"
  case "$raw" in
    ""|*[!0-9]*) printf "%s" "$def" ;;
    *) printf "%s" "$raw" ;;
  esac
}

clamp_min1() {
  local v; v="$(as_int "${1:-}" "1")"
  if [ "$v" -lt 1 ]; then v="1"; fi
  printf "%s" "$v"
}

lower() { printf "%s" "${1:-}" | tr '[:upper:]' '[:lower:]'; }

pick_loglevel() {
  local v; v="$(lower "${1:-info}")"
  case "$v" in
    critical|error|warning|info|debug|trace) printf "%s" "$v" ;;
    *) printf "%s" "info" ;;
  esac
}

cpu_count() {
  # best-effort; keep safe defaults
  local n="1"
  if command -v getconf >/dev/null 2>&1; then
    n="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)"
  elif command -v nproc >/dev/null 2>&1; then
    n="$(nproc 2>/dev/null || echo 1)"
  fi
  n="$(as_int "$n" "1")"
  if [ "$n" -lt 1 ]; then n="1"; fi
  printf "%s" "$n"
}

calc_workers() {
  # Priority:
  # 1) TFB_WORKERS
  # 2) WEB_CONCURRENCY
  # 3) default 1 (Render free/low mem friendly)
  local explicit="${TFB_WORKERS:-}"
  local wc="${WEB_CONCURRENCY:-}"
  if [ -n "${explicit:-}" ]; then
    clamp_min1 "$explicit"
    return
  fi
  if [ -n "${wc:-}" ]; then
    clamp_min1 "$wc"
    return
  fi

  # Conservative auto-scale is available if you want it:
  # set TFB_AUTOWORKERS=1 to enable (caps at TFB_WORKERS_MAX or 4)
  if [ "${TFB_AUTOWORKERS:-0}" = "1" ]; then
    local cpu maxw w
    cpu="$(cpu_count)"
    maxw="$(as_int "${TFB_WORKERS_MAX:-4}" "4")"
    if [ "$maxw" -lt 1 ]; then maxw="1"; fi
    w="$(( cpu * 2 + 1 ))"
    if [ "$w" -gt "$maxw" ]; then w="$maxw"; fi
    if [ "$w" -lt 1 ]; then w="1"; fi
    printf "%s" "$w"
    return
  fi

  printf "1"
}

# -------------------------------
# Defaults (Render-aligned)
# -------------------------------
APP_MODULE="${TFB_APP:-${APP_MODULE:-main:app}}"
HOST="${TFB_HOST:-0.0.0.0}"
PORT="${PORT:-${TFB_PORT:-10000}}"
PORT="$(as_int "$PORT" "10000")"

WORKERS="$(calc_workers)"
LOG_LEVEL="$(pick_loglevel "${TFB_LOG_LEVEL:-${LOG_LEVEL:-info}}")"

# Timeouts: keep Render happy but allow long requests if needed
TIMEOUT="$(as_int "${TFB_TIMEOUT:-${TIMEOUT:-120}}" "120")"
GRACEFUL_TIMEOUT="$(as_int "${TFB_GRACEFUL_TIMEOUT:-${GRACEFUL_TIMEOUT:-30}}" "30")"
KEEPALIVE="$(as_int "${TFB_KEEPALIVE:-${KEEPALIVE:-5}}" "5")"

# Logging
ACCESS_LOG="${TFB_ACCESS_LOG:-0}"    # 0=off (default), 1=on
ERROR_LOG_LEVEL="${TFB_ERROR_LOG_LEVEL:-${LOG_LEVEL}}"
ERROR_LOG_LEVEL="$(pick_loglevel "$ERROR_LOG_LEVEL")"

# Gunicorn settings
WORKER_CLASS="${TFB_WORKER_CLASS:-uvicorn.workers.UvicornWorker}"
PRELOAD="${TFB_PRELOAD:-0}"          # 1 to enable gunicorn preload

# Uvicorn direct fallback
UVICORN_WORKERS="$(clamp_min1 "${TFB_UVICORN_WORKERS:-1}")"

# -------------------------------
# Startup banner
# -------------------------------
log "app=$APP_MODULE host=$HOST port=$PORT workers=$WORKERS log_level=$LOG_LEVEL timeout=$TIMEOUT graceful_timeout=$GRACEFUL_TIMEOUT keepalive=$KEEPALIVE"

# Attempt to raise file descriptor limit (best-effort)
( ulimit -n 65535 >/dev/null 2>&1 ) || true

export PYTHONUNBUFFERED=1

# -------------------------------
# Prefer gunicorn (recommended on Render)
# -------------------------------
if command -v gunicorn >/dev/null 2>&1; then
  # Build args
  GUNI_ARGS=(
    "--bind" "${HOST}:${PORT}"
    "--worker-class" "${WORKER_CLASS}"
    "--workers" "${WORKERS}"
    "--timeout" "${TIMEOUT}"
    "--graceful-timeout" "${GRACEFUL_TIMEOUT}"
    "--keep-alive" "${KEEPALIVE}"
    "--log-level" "${LOG_LEVEL}"
    "--error-logfile" "-"
    "--access-logfile" "-"
    "--proxy-headers"
    "--forwarded-allow-ips" "*"
  )

  if [ "${ACCESS_LOG}" = "0" ]; then
    # Disable access log noise while keeping error logs
    GUNI_ARGS+=( "--access-logfile" "/dev/null" )
  fi

  if [ "${PRELOAD}" = "1" ]; then
    GUNI_ARGS+=( "--preload" )
  fi

  exec gunicorn "${GUNI_ARGS[@]}" "${APP_MODULE}"
fi

# -------------------------------
# Fallback to uvicorn
# -------------------------------
log "gunicorn not found; falling back to uvicorn"
exec uvicorn "${APP_MODULE}" \
  --host "${HOST}" \
  --port "${PORT}" \
  --workers "${UVICORN_WORKERS}" \
  --log-level "${LOG_LEVEL}" \
  --proxy-headers \
  --forwarded-allow-ips "*"
