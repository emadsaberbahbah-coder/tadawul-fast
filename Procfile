#!/usr/bin/env bash
# ==============================================================================
# TADAWUL FAST BRIDGE – RENDER-SAFE WEB LAUNCHER (v6.2.0)
# ==============================================================================
# هدف النسخة دي:
# - تمنع “No open ports detected” على Render
# - ما تفشل قبل فتح البورت بسبب checks اختيارية (DB/Redis/metrics)
# - تشخّص سبب فشل import بوضوح (Full traceback)
# - تلتزم بـ PORT / WEB_CONCURRENCY / LOG_LEVEL مثل Render
#
# IMPORTANT:
# - Render لازم يشغل هذا الملف كـ Start Command:
#     bash scripts/start_web.sh
#   أو:
#     ./scripts/start_web.sh
# ==============================================================================

set -euo pipefail
IFS=$'\n\t'

# ------------------------------------------------------------------------------
# Version / Basics
# ------------------------------------------------------------------------------
readonly SCRIPT_VERSION="6.2.0"
readonly MIN_BASH_MAJOR="4"

if (( BASH_VERSINFO[0] < MIN_BASH_MAJOR )); then
  echo "❌ Bash ${MIN_BASH_MAJOR}.0+ required (found ${BASH_VERSINFO[0]}.${BASH_VERSINFO[1]})." >&2
  exit 1
fi

# ------------------------------------------------------------------------------
# Colors (safe with set -u)
# ------------------------------------------------------------------------------
readonly COLOR_RESET='\033[0m'
readonly COLOR_RED='\033[0;31m'
readonly COLOR_GREEN='\033[0;32m'
readonly COLOR_YELLOW='\033[1;33m'
readonly COLOR_BLUE='\033[0;34m'
readonly COLOR_CYAN='\033[0;36m'
readonly COLOR_BOLD='\033[1m'

log_info()    { echo -e "${COLOR_GREEN}ℹ${COLOR_RESET} $*" >&2; }
log_warn()    { echo -e "${COLOR_YELLOW}⚠${COLOR_RESET} $*" >&2; }
log_error()   { echo -e "${COLOR_RED}✗${COLOR_RESET} $*" >&2; }
log_success() { echo -e "${COLOR_GREEN}✓${COLOR_RESET} $*" >&2; }
log_debug()   { [[ "${LOG_LEVEL:-info}" == "debug" ]] && echo -e "${COLOR_BLUE}🔍${COLOR_RESET} $*" >&2 || true; }

command_exists() { command -v "$1" >/dev/null 2>&1; }

# ------------------------------------------------------------------------------
# Safe env parsing
# ------------------------------------------------------------------------------
as_int() {
  # usage: as_int "raw" "default"
  local raw="${1:-}"
  local def="${2:-0}"
  case "$raw" in
    ""|*[!0-9]*) printf "%s" "$def" ;;
    *) printf "%s" "$raw" ;;
  esac
}

is_true() {
  local v="$(printf "%s" "${1:-}" | tr '[:upper:]' '[:lower:]' | xargs)"
  case "$v" in
    1|true|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

# ------------------------------------------------------------------------------
# Paths / App
# ------------------------------------------------------------------------------
readonly APP_DIR="${APP_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
readonly PYTHON_BIN="${PYTHON_BIN:-python3}"

# Uvicorn/Gunicorn app import string
# IMPORTANT: must be like "main:app"
readonly APP_MODULE="${APP_MODULE:-main:app}"

# Render-friendly networking
readonly HOST="${HOST:-0.0.0.0}"
readonly PORT="$(as_int "${PORT:-8000}" "8000")"

# Logging / mode
readonly LOG_LEVEL="$(printf "%s" "${LOG_LEVEL:-info}" | tr '[:upper:]' '[:lower:]')"
readonly ACCESS_LOG="${ACCESS_LOG:-true}"

# Concurrency preference (Render uses WEB_CONCURRENCY)
readonly WEB_CONCURRENCY="$(as_int "${WEB_CONCURRENCY:-}" "0")"
readonly WORKERS_ENV="$(as_int "${WORKERS:-}" "0")"
readonly WEB_CONCURRENCY_MAX="$(as_int "${WEB_CONCURRENCY_MAX:-8}" "8")"

# Uvicorn tuning (aligned to Render typical flags)
readonly UVICORN_KEEPALIVE="$(as_int "${UVICORN_KEEPALIVE:-75}" "75")"
readonly UVICORN_GRACEFUL_TIMEOUT="$(as_int "${UVICORN_GRACEFUL_TIMEOUT:-30}" "30")"
readonly UVICORN_BACKLOG="$(as_int "${UVICORN_BACKLOG:-2048}" "2048")"

# Optional limits
readonly UVICORN_LIMIT_CONCURRENCY_RAW="${UVICORN_LIMIT_CONCURRENCY:-}"
readonly UVICORN_LIMIT_MAX_REQUESTS_RAW="${UVICORN_LIMIT_MAX_REQUESTS:-}"

# Use Gunicorn?
# Safer defaults: only if (workers>1) AND gunicorn exists OR explicitly enabled.
readonly USE_GUNICORN="${USE_GUNICORN:-}"

# Prefer uvicorn-worker package if installed
readonly WORKER_CLASS="${WORKER_CLASS:-}"

# Optional external services checks (OFF by default to avoid deploy failures)
readonly CHECK_DATABASE="${CHECK_DATABASE:-false}"
readonly CHECK_REDIS="${CHECK_REDIS:-false}"
readonly POSTGRES_URL="${POSTGRES_URL:-}"
readonly REDIS_URL="${REDIS_URL:-}"

# Optional metrics daemon (OFF by default)
readonly ENABLE_METRICS="${ENABLE_METRICS:-false}"
readonly METRICS_PORT="$(as_int "${METRICS_PORT:-9090}" "9090")"

# ------------------------------------------------------------------------------
# cgroup-aware resource helpers (safe)
# ------------------------------------------------------------------------------
get_cpu_cores() {
  if command_exists nproc; then
    nproc 2>/dev/null || echo "1"
    return
  fi
  echo "$(as_int "$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)" 1)"
}

get_memory_limit_mb() {
  # cgroup v2: /sys/fs/cgroup/memory.max (bytes or "max")
  if [[ -f /sys/fs/cgroup/memory.max ]]; then
    local v
    v="$(cat /sys/fs/cgroup/memory.max 2>/dev/null || echo "max")"
    if [[ "$v" != "max" ]]; then
      if [[ "$v" =~ ^[0-9]+$ ]] && (( v > 0 )); then
        echo $(( v / 1024 / 1024 ))
        return
      fi
    fi
  fi

  # cgroup v1: /sys/fs/cgroup/memory/memory.limit_in_bytes
  if [[ -f /sys/fs/cgroup/memory/memory.limit_in_bytes ]]; then
    local v
    v="$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null || echo 0)"
    if [[ "$v" =~ ^[0-9]+$ ]] && (( v > 0 )); then
      echo $(( v / 1024 / 1024 ))
      return
    fi
  fi

  # fallback: free -m (available memory)
  if command_exists free; then
    free -m | awk '/^Mem:/ {print $7}' 2>/dev/null || echo "0"
    return
  fi

  echo "0"
}

calculate_workers() {
  # Conservative: avoid OOM on Render small instances
  local cpu
  local mem_mb
  cpu="$(as_int "$(get_cpu_cores)" 1)"
  mem_mb="$(as_int "$(get_memory_limit_mb)" 0)"

  # baseline: cpu*2+1
  local cpu_workers=$(( cpu * 2 + 1 ))

  # memory-based cap: ~700MB per worker (ML libs installed => heavy)
  local mem_workers=1
  if (( mem_mb > 0 )); then
    mem_workers=$(( mem_mb / 700 ))
    if (( mem_workers < 1 )); then mem_workers=1; fi
  fi

  local w="$cpu_workers"
  if (( mem_workers > 0 && mem_workers < w )); then
    w="$mem_workers"
  fi

  # clamp hard
  if (( w < 1 )); then w=1; fi
  if (( w > WEB_CONCURRENCY_MAX )); then w="$WEB_CONCURRENCY_MAX"; fi

  echo "$w"
}

resolve_workers() {
  # Priority: WEB_CONCURRENCY > WORKERS > calculated
  if (( WEB_CONCURRENCY > 0 )); then
    echo "$(_clamp "$WEB_CONCURRENCY" 1 "$WEB_CONCURRENCY_MAX")"
    return
  fi
  if (( WORKERS_ENV > 0 )); then
    echo "$(_clamp "$WORKERS_ENV" 1 "$WEB_CONCURRENCY_MAX")"
    return
  fi
  echo "$(calculate_workers)"
}

_clamp() {
  local v="$1" lo="$2" hi="$3"
  if (( v < lo )); then echo "$lo"; return; fi
  if (( v > hi )); then echo "$hi"; return; fi
  echo "$v"
}

# ------------------------------------------------------------------------------
# Environment tuning (safe)
# ------------------------------------------------------------------------------
optimize_environment() {
  log_debug "Optimizing environment (safe defaults)..."

  # prevent BLAS thread explosion
  export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
  export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
  export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
  export NUMEXPR_NUM_THREADS="${NUMEXPR_NUM_THREADS:-1}"

  # add app dir to PYTHONPATH
  export PYTHONPATH="${APP_DIR}:${PYTHONPATH:-}"

  # increase FD limit if possible (don’t fail deploy if not permitted)
  local max_fd="${MAX_FD_LIMIT:-65536}"
  local cur_fd
  cur_fd="$(ulimit -n 2>/dev/null || echo 0)"
  if [[ "$cur_fd" =~ ^[0-9]+$ ]] && (( cur_fd > 0 && cur_fd < max_fd )); then
    ulimit -n "$max_fd" 2>/dev/null || log_warn "Could not increase FD limit to ${max_fd}"
  fi
}

# ------------------------------------------------------------------------------
# Mandatory checks (deployment-safe)
# ------------------------------------------------------------------------------
check_python_environment() {
  if ! command_exists "$PYTHON_BIN"; then
    log_error "Python binary not found: ${PYTHON_BIN}"
    return 1
  fi

  # Must be able to import uvicorn
  if ! "$PYTHON_BIN" - <<'PY' >/dev/null 2>&1
import uvicorn
PY
  then
    log_error "Python cannot import uvicorn. (requirements/build issue)"
    return 1
  fi

  log_success "Python environment OK"
  return 0
}

check_app_import() {
  # This is the most important check to diagnose "No open ports detected".
  # Print FULL traceback so Render logs show the true failure.
  log_info "Checking ASGI import: ${APP_MODULE}"
  local module="${APP_MODULE%%:*}"
  local app="${APP_MODULE##*:}"

  "$PYTHON_BIN" - <<PY
import importlib, traceback, sys
mod_name = "${module}"
app_name = "${app}"
try:
    mod = importlib.import_module(mod_name)
    obj = getattr(mod, app_name)
    if obj is None:
        raise RuntimeError(f"{mod_name}:{app_name} is None")
    print("OK")
except Exception as e:
    print("IMPORT_FAILED:", e, file=sys.stderr)
    traceback.print_exc()
    sys.exit(1)
PY
  log_success "ASGI import OK"
  return 0
}

check_database() {
  is_true "$CHECK_DATABASE" || return 0
  [[ -n "$POSTGRES_URL" ]] || { log_warn "CHECK_DATABASE=true but POSTGRES_URL empty; skipping."; return 0; }

  log_info "Checking Postgres connectivity (optional)..."
  "$PYTHON_BIN" - <<PY || { log_error "Database connection failed (optional)"; return 6; }
import asyncio, sys
import asyncpg

async def main():
    conn = await asyncpg.connect("${POSTGRES_URL}", timeout=5.0)
    await conn.close()

asyncio.run(main())
print("DB_OK")
PY
  log_success "Database connection OK"
  return 0
}

check_redis() {
  is_true "$CHECK_REDIS" || return 0
  [[ -n "$REDIS_URL" ]] || { log_warn "CHECK_REDIS=true but REDIS_URL empty; skipping."; return 0; }

  log_info "Checking Redis connectivity (optional)..."
  "$PYTHON_BIN" - <<PY || { log_error "Redis connection failed (optional)"; return 7; }
import asyncio, sys
import redis.asyncio as redis

async def main():
    r = redis.from_url("${REDIS_URL}", socket_timeout=3.0)
    await r.ping()
    await r.close()

asyncio.run(main())
print("REDIS_OK")
PY
  log_success "Redis connection OK"
  return 0
}

# ------------------------------------------------------------------------------
# Optional monitoring (OFF by default)
# ------------------------------------------------------------------------------
setup_monitoring() {
  is_true "$ENABLE_METRICS" || return 0
  if ! command_exists socat; then
    log_warn "ENABLE_METRICS=true but socat not installed; skipping metrics."
    return 0
  fi

  log_info "Starting simple metrics endpoint on :${METRICS_PORT} (optional)..."
  (
    while true; do
      local RESPONSE="HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n# HELP app_up App Status\napp_up 1\n"
      echo -e "$RESPONSE" | socat -t 5 TCP-LISTEN:${METRICS_PORT},reuseaddr,fork STDIO >/dev/null 2>&1 || true
    done
  ) &
  disown || true
}

# ------------------------------------------------------------------------------
# Runner selection
# ------------------------------------------------------------------------------
detect_best_gunicorn_worker() {
  # prefer uvicorn-worker if installed
  if [[ -n "$WORKER_CLASS" ]]; then
    echo "$WORKER_CLASS"
    return
  fi
  if "$PYTHON_BIN" - <<'PY' >/dev/null 2>&1
import uvicorn_worker  # noqa
PY
  then
    echo "uvicorn_worker.UvicornWorker"
    return
  fi
  echo "uvicorn.workers.UvicornWorker"
}

should_use_gunicorn() {
  # Explicit override
  if [[ -n "$USE_GUNICORN" ]]; then
    is_true "$USE_GUNICORN" && return 0 || return 1
  fi

  # Default: use gunicorn only if installed and workers > 1
  command_exists gunicorn || return 1
  local w="$1"
  (( w > 1 )) && return 0
  return 1
}

run_uvicorn() {
  local worker_count="$1"

  log_info "🚀 Launching Uvicorn | app=${APP_MODULE} | bind=${HOST}:${PORT} | workers=${worker_count}"

  local cmd=(
    "$PYTHON_BIN" -m uvicorn
    "${APP_MODULE}"
    --host "${HOST}"
    --port "${PORT}"
    --proxy-headers
    --forwarded-allow-ips "*"
    --lifespan on
    --timeout-keep-alive "${UVICORN_KEEPALIVE}"
    --timeout-graceful-shutdown "${UVICORN_GRACEFUL_TIMEOUT}"
    --backlog "${UVICORN_BACKLOG}"
    --log-level "${LOG_LEVEL}"
    --no-server-header
  )

  # Optional tuning if available
  if "$PYTHON_BIN" -c "import uvloop" >/dev/null 2>&1; then cmd+=(--loop uvloop); fi
  if "$PYTHON_BIN" -c "import httptools" >/dev/null 2>&1; then cmd+=(--http httptools); fi

  # Optional limits
  if [[ "${UVICORN_LIMIT_CONCURRENCY_RAW}" =~ ^[0-9]+$ ]] && (( UVICORN_LIMIT_CONCURRENCY_RAW > 0 )); then
    cmd+=(--limit-concurrency "${UVICORN_LIMIT_CONCURRENCY_RAW}")
  fi
  if [[ "${UVICORN_LIMIT_MAX_REQUESTS_RAW}" =~ ^[0-9]+$ ]] && (( UVICORN_LIMIT_MAX_REQUESTS_RAW > 0 )); then
    cmd+=(--limit-max-requests "${UVICORN_LIMIT_MAX_REQUESTS_RAW}")
  fi

  if (( worker_count > 1 )); then
    cmd+=(--workers "${worker_count}")
  fi

  if is_true "$ACCESS_LOG"; then
    cmd+=(--access-log)
  else
    cmd+=(--no-access-log)
  fi

  # IMPORTANT: exec to keep process in foreground (Render expects this)
  exec "${cmd[@]}"
}

run_gunicorn() {
  local worker_count="$1"
  local worker_class
  worker_class="$(detect_best_gunicorn_worker)"

  log_info "🚀 Launching Gunicorn | app=${APP_MODULE} | bind=${HOST}:${PORT} | workers=${worker_count} | class=${worker_class}"

  local cmd=(
    gunicorn
    "${APP_MODULE}"
    --bind "${HOST}:${PORT}"
    --workers "${worker_count}"
    --worker-class "${worker_class}"
    --worker-connections "$(as_int "${WORKER_CONNECTIONS:-2048}" "2048")"
    --max-requests "$(as_int "${WORKER_MAX_REQUESTS:-0}" "0")"
    --max-requests-jitter "$(as_int "${WORKER_MAX_REQUESTS_JITTER:-0}" "0")"
    --timeout "$(as_int "${WORKER_TIMEOUT:-120}" "120")"
    --graceful-timeout "$(as_int "${WORKER_GRACEFUL_TIMEOUT:-30}" "30")"
    --keepalive "$(as_int "${WORKER_KEEPALIVE:-5}" "5")"
    --backlog "${UVICORN_BACKLOG}"
    --log-level "${LOG_LEVEL}"
    --capture-output
    --access-logfile -
    --error-logfile -
  )

  # IMPORTANT: exec to keep process in foreground
  exec "${cmd[@]}"
}

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
main() {
  echo -e "${COLOR_CYAN}${COLOR_BOLD}"
  echo "==========================================================="
  echo " TADAWUL FAST BRIDGE WEB LAUNCHER v${SCRIPT_VERSION}"
  echo "==========================================================="
  echo -e "${COLOR_RESET}"

  optimize_environment

  check_python_environment || exit 1
  check_app_import || exit 1

  # Optional checks (OFF by default to avoid deploy failures)
  check_database || exit $?
  check_redis || exit $?

  setup_monitoring

  local workers
  workers="$(resolve_workers)"

  log_info "Resolved workers = ${workers} (WEB_CONCURRENCY=${WEB_CONCURRENCY}, WORKERS=${WORKERS_ENV}, MAX=${WEB_CONCURRENCY_MAX})"
  log_info "Boot: HOST=${HOST} PORT=${PORT} LOG_LEVEL=${LOG_LEVEL} ACCESS_LOG=${ACCESS_LOG}"

  if should_use_gunicorn "${workers}"; then
    run_gunicorn "${workers}"
  else
    run_uvicorn "${workers}"
  fi
}

main "$@"
