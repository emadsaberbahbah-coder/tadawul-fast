#!/usr/bin/env bash
# scripts/start_web.sh
# ==============================================================================
# TADAWUL FAST BRIDGE — RENDER WEB START SCRIPT (v2.5.0)
# ==============================================================================
# Purpose
# - Render-safe startup path
# - Single clear foreground process for Render port detection
# - Prefer Gunicorn + UvicornWorker only
# - Fail fast if PORT is not provided by Render
# - Conservative defaults for heavy FastAPI startup
# ==============================================================================

set -Eeuo pipefail

timestamp_utc() {
  date -u +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || printf "UTC"
}

log() {
  printf "%s %s\n" "$(timestamp_utc) | TFB start_web.sh |" "$*"
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

clamp_min() {
  local raw="${1:-}" min="${2:-1}" def="${3:-1}" v
  v="$(as_int "$raw" "$def")"
  if [ "$v" -lt "$min" ]; then
    v="$min"
  fi
  printf "%s" "$v"
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

APP_MODULE="${TFB_APP:-${APP_MODULE:-main:app}}"
APP_DIR="${TFB_APP_DIR:-${APP_DIR:-}}"

HOST="${TFB_HOST:-0.0.0.0}"
PORT="${PORT:?Render must provide PORT}"

WORKERS="$(clamp_min "${TFB_WORKERS:-${WEB_CONCURRENCY:-1}}" 1 1)"
TIMEOUT="$(clamp_min "${TFB_TIMEOUT:-${TIMEOUT:-180}}" 1 180)"
GRACEFUL_TIMEOUT="$(clamp_min "${TFB_GRACEFUL_TIMEOUT:-${GRACEFUL_TIMEOUT:-45}}" 1 45)"
KEEPALIVE="$(clamp_min "${TFB_KEEPALIVE:-${KEEPALIVE:-10}}" 1 10)"

RAW_LOG_LEVEL="${TFB_LOG_LEVEL:-${LOG_LEVEL:-info}}"
GUNICORN_LOG_LEVEL="$(pick_gunicorn_loglevel "$RAW_LOG_LEVEL")"

ACCESS_LOG="${TFB_ACCESS_LOG:-1}"
ACCESS_LOGFILE="-"
if [ "$(lower "$ACCESS_LOG")" = "0" ] || [ "$(lower "$ACCESS_LOG")" = "false" ] || [ "$(lower "$ACCESS_LOG")" = "no" ]; then
  ACCESS_LOGFILE="/dev/null"
fi

WORKER_CLASS="${TFB_WORKER_CLASS:-uvicorn.workers.UvicornWorker}"
WORKER_TMP_DIR="${TFB_WORKER_TMP_DIR:-/dev/shm}"

MAX_REQUESTS="$(as_int "${TFB_MAX_REQUESTS:-0}" "0")"
MAX_REQUESTS_JITTER="$(as_int "${TFB_MAX_REQUESTS_JITTER:-0}" "0")"

if [ -n "$APP_DIR" ]; then
  if [ -d "$APP_DIR" ]; then
    cd "$APP_DIR"
    log "changed directory to $APP_DIR"
  else
    log "warning: APP_DIR does not exist: $APP_DIR"
  fi
fi

( ulimit -n 65535 >/dev/null 2>&1 ) || true

export PYTHONUNBUFFERED=1
export PORT
export HOST

log "boot config: app=$APP_MODULE host=$HOST port=$PORT workers=$WORKERS gunicorn_log_level=$GUNICORN_LOG_LEVEL timeout=$TIMEOUT graceful_timeout=$GRACEFUL_TIMEOUT keepalive=$KEEPALIVE"

GUNI_ARGS=(
  "--bind" "${HOST}:${PORT}"
  "--worker-class" "${WORKER_CLASS}"
  "--workers" "${WORKERS}"
  "--timeout" "${TIMEOUT}"
  "--graceful-timeout" "${GRACEFUL_TIMEOUT}"
  "--keep-alive" "${KEEPALIVE}"
  "--log-level" "${GUNICORN_LOG_LEVEL}"
  "--error-logfile" "-"
  "--access-logfile" "${ACCESS_LOGFILE}"
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

log "starting with gunicorn"
exec gunicorn "${GUNI_ARGS[@]}" "${APP_MODULE}"
