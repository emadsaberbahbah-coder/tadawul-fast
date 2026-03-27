#!/usr/bin/env bash
# scripts/start_web.sh
# ==============================================================================
# TADAWUL FAST BRIDGE — RENDER WEB START SCRIPT (v2.4.0)
# ==============================================================================
# Goals
# - Render-safe startup with conservative defaults
# - Prefer Gunicorn + UvicornWorker
# - Avoid invalid/unsupported log levels for Gunicorn
# - Keep worker count low by default for heavy app startup
# - Preserve fallback to Uvicorn when Gunicorn is unavailable
# - Keep startup diagnostics clear without doing heavy preflight work
# ==============================================================================

set -euo pipefail

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

is_true() {
  case "$(lower "${1:-}")" in
    1|true|yes|y|on) return 0 ;;
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

pick_uvicorn_loglevel() {
  local v
  v="$(lower "${1:-info}")"
  case "$v" in
    critical|error|warning|info|debug|trace)
      printf "%s" "$v"
      ;;
    warn)
      printf "%s" "warning"
      ;;
    *)
      printf "%s" "info"
      ;;
  esac
}

cpu_count() {
  local n="1"
  if command -v getconf >/dev/null 2>&1; then
    n="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)"
  elif command -v nproc >/dev/null 2>&1; then
    n="$(nproc 2>/dev/null || echo 1)"
  fi
  n="$(as_int "$n" "1")"
  if [ "$n" -lt 1 ]; then
    n="1"
  fi
  printf "%s" "$n"
}

calc_workers() {
  local explicit="${TFB_WORKERS:-}"
  local web_conc="${WEB_CONCURRENCY:-}"
  local auto="${TFB_AUTOWORKERS:-0}"

  if [ -n "$explicit" ]; then
    clamp_min "$explicit" 1 1
    return
  fi

  if [ -n "$web_conc" ]; then
    clamp_min "$web_conc" 1 1
    return
  fi

  # Conservative default on Render / heavy FastAPI apps:
  # start with 1 worker unless explicitly overridden.
  if ! is_true "$auto"; then
    printf "1"
    return
  fi

  local cpu maxw calc
  cpu="$(cpu_count)"
  maxw="$(clamp_min "${TFB_WORKERS_MAX:-2}" 1 2)"
  calc="$(( cpu * 2 + 1 ))"

  if [ "$calc" -gt "$maxw" ]; then
    calc="$maxw"
  fi
  if [ "$calc" -lt 1 ]; then
    calc="1"
  fi

  printf "%s" "$calc"
}

APP_MODULE="${TFB_APP:-${APP_MODULE:-main:app}}"
APP_DIR="${TFB_APP_DIR:-${APP_DIR:-}}"

HOST="${TFB_HOST:-0.0.0.0}"
PORT="${PORT:-${TFB_PORT:-10000}}"
PORT="$(as_int "$PORT" "10000")"

RAW_LOG_LEVEL="${TFB_LOG_LEVEL:-${LOG_LEVEL:-info}}"
GUNICORN_LOG_LEVEL="$(pick_gunicorn_loglevel "$RAW_LOG_LEVEL")"
UVICORN_LOG_LEVEL="$(pick_uvicorn_loglevel "$RAW_LOG_LEVEL")"

WORKERS="$(calc_workers)"
UVICORN_WORKERS="$(clamp_min "${TFB_UVICORN_WORKERS:-1}" 1 1)"

TIMEOUT="$(clamp_min "${TFB_TIMEOUT:-${TIMEOUT:-180}}" 1 180)"
GRACEFUL_TIMEOUT="$(clamp_min "${TFB_GRACEFUL_TIMEOUT:-${GRACEFUL_TIMEOUT:-45}}" 1 45)"
KEEPALIVE="$(clamp_min "${TFB_KEEPALIVE:-${KEEPALIVE:-10}}" 1 10)"

PRELOAD="${TFB_PRELOAD:-0}"                      # default OFF for safer startup
FORCE_UVICORN="${TFB_FORCE_UVICORN:-0}"         # 1 to bypass gunicorn
ACCESS_LOG="${TFB_ACCESS_LOG:-0}"               # 1 to enable
WORKER_CLASS="${TFB_WORKER_CLASS:-uvicorn.workers.UvicornWorker}"
WORKER_TMP_DIR="${TFB_WORKER_TMP_DIR:-/dev/shm}"

MAX_REQUESTS="$(as_int "${TFB_MAX_REQUESTS:-0}" "0")"
MAX_REQUESTS_JITTER="$(as_int "${TFB_MAX_REQUESTS_JITTER:-0}" "0")"

ACCESS_LOGFILE="/dev/null"
if is_true "$ACCESS_LOG"; then
  ACCESS_LOGFILE="-"
fi

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

log "boot config: app=$APP_MODULE host=$HOST port=$PORT workers=$WORKERS uvicorn_workers=$UVICORN_WORKERS raw_log_level=$RAW_LOG_LEVEL gunicorn_log_level=$GUNICORN_LOG_LEVEL uvicorn_log_level=$UVICORN_LOG_LEVEL timeout=$TIMEOUT graceful_timeout=$GRACEFUL_TIMEOUT keepalive=$KEEPALIVE preload=$PRELOAD force_uvicorn=$FORCE_UVICORN"

if ! is_true "$FORCE_UVICORN" && command -v gunicorn >/dev/null 2>&1; then
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

  if is_true "$PRELOAD"; then
    GUNI_ARGS+=( "--preload" )
  fi

  if [ "$MAX_REQUESTS" -gt 0 ]; then
    GUNI_ARGS+=( "--max-requests" "${MAX_REQUESTS}" )
  fi

  if [ "$MAX_REQUESTS_JITTER" -gt 0 ]; then
    GUNI_ARGS+=( "--max-requests-jitter" "${MAX_REQUESTS_JITTER}" )
  fi

  log "starting with gunicorn"
  exec gunicorn "${GUNI_ARGS[@]}" "${APP_MODULE}"
fi

log "gunicorn unavailable or bypassed; falling back to uvicorn"

if command -v uvicorn >/dev/null 2>&1; then
  exec uvicorn "${APP_MODULE}" \
    --host "${HOST}" \
    --port "${PORT}" \
    --workers "${UVICORN_WORKERS}" \
    --log-level "${UVICORN_LOG_LEVEL}" \
    --proxy-headers \
    --forwarded-allow-ips "*"
fi

log "uvicorn binary not found; falling back to python -m uvicorn"
exec python -m uvicorn "${APP_MODULE}" \
  --host "${HOST}" \
  --port "${PORT}" \
  --workers "${UVICORN_WORKERS}" \
  --log-level "${UVICORN_LOG_LEVEL}" \
  --proxy-headers \
  --forwarded-allow-ips "*"
