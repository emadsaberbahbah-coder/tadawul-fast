#!/usr/bin/env sh
# ============================================================
# Tadawul Fast Bridge — PROD SAFE Start Script (v1.4.0)
# ============================================================
# Optimized for high-concurrency FastAPI/Render environments.
# 
# Features:
# - Auto-worker calculation (CPU-aware)
# - Robust Uvicorn flag auto-detection
# - Pre-flight dependency & file checks
# - Signal-safe process execution (exec)
# ============================================================

set -eu

# ----------------------------
# Internal Helpers
# ----------------------------
as_int() {
  raw="${1:-}"
  def="${2:-0}"
  case "$raw" in
    ""|*[!0-9]*) printf "%s" "$def" ;;
    *)           printf "%s" "$raw" ;;
  esac
}

clamp_min_1() {
  v="$(as_int "${1:-}" "1")"
  [ "$v" -lt 1 ] && v="1"
  printf "%s" "$v"
}

loglvl() {
  v="$(printf "%s" "${1:-info}" | tr "[:upper:]" "[:lower:]")"
  case "$v" in
    critical|error|warning|info|debug|trace) printf "%s" "$v" ;;
    *)                                       printf "%s" "info" ;;
  esac
}

pos_int_or_empty() {
  raw="${1:-}"
  case "$raw" in
    ""|*[!0-9]*) printf "%s" "" ;;
    *) [ "$raw" -gt 0 ] && printf "%s" "$raw" || printf "%s" "" ;;
  esac
}

# Advanced: Detect CPU cores for auto-scaling workers
detect_workers() {
  if [ -n "${WEB_CONCURRENCY:-}" ] && [ "$(as_int "$WEB_CONCURRENCY" 0)" -gt 0 ]; then
    printf "%s" "$WEB_CONCURRENCY"
    return 0
  fi
  
  # Try to detect CPU count (Linux/MacOS)
  cpus=1
  if [ -f /proc/cpuinfo ]; then
    cpus=$(grep -c ^processor /proc/cpuinfo)
  elif command -v nproc >/dev/null 2>&1; then
    cpus=$(nproc)
  elif command -v sysctl >/dev/null 2>&1; then
    cpus=$(sysctl -n hw.ncpu || echo 1)
  fi
  
  # Formula: (2 x cores) + 1 is typical for Uvicorn/Gunicorn
  # But for async FastAPI, 1-2 workers per core is safer on constrained RAM
  calc_workers=$((cpus * 1))
  [ "$calc_workers" -lt 1 ] && calc_workers=1
  printf "%s" "$calc_workers"
}

detect_grace_flag() {
  # Uvicorn changed the flag name in recent versions.
  help_text="$(python -m uvicorn --help 2>&1 || true)"
  if echo "$help_text" | grep -q -- "--timeout-graceful-shutdown"; then
    printf "%s" "--timeout-graceful-shutdown"
  elif echo "$help_text" | grep -q -- "--graceful-timeout"; then
    printf "%s" "--graceful-timeout"
  else
    printf "%s" ""
  fi
}

# ----------------------------
# Pre-Flight Checks
# ----------------------------
printf "🚀 Initializing Tadawul Fast Bridge Boot Sequence...\n"

if [ ! -f "main.py" ]; then
  printf "❌ Error: main.py not found in current directory (%s).\n" "$(pwd)"
  exit 1
fi

if ! python -c "import uvicorn" >/dev/null 2>&1; then
  printf "❌ Error: uvicorn package is not installed.\n"
  exit 1
fi

# ----------------------------
# Environment Resolution
# ----------------------------
LL="$(loglvl "${LOG_LEVEL:-info}")"
P="$(clamp_min_1 "$(as_int "${PORT:-8000}" "8000")")"
KA="$(clamp_min_1 "$(as_int "${UVICORN_KEEPALIVE:-75}" "75")")"
GR="$(clamp_min_1 "$(as_int "${UVICORN_GRACEFUL_TIMEOUT:-30}" "30")")"
WC="$(detect_workers)"

ACCESS_RAW="$(printf "%s" "${UVICORN_ACCESS_LOG:-1}" | tr "[:upper:]" "[:lower:]")"
GRACE_FLAG="$(detect_grace_flag)"

# Tuning: Use 'httptools' and 'uvloop' if available for max performance
LOOP_FLAG=""
if python -c "import uvloop" >/dev/null 2>&1; then
  LOOP_FLAG="--loop uvloop"
  printf "✨ Performance: uvloop detected and enabled.\n"
fi

HTTP_FLAG=""
if python -c "import httptools" >/dev/null 2>&1; then
  HTTP_FLAG="--http httptools"
  printf "✨ Performance: httptools detected and enabled.\n"
fi

# ----------------------------
# Argv Construction
# ----------------------------
set -- python -m uvicorn main:app \
  --host 0.0.0.0 \
  --port "$P" \
  --proxy-headers \
  --forwarded-allow-ips "*" \
  --lifespan on \
  --timeout-keep-alive "$KA" \
  --log-level "$LL" \
  --no-server-header

# Add performance flags
[ -n "$LOOP_FLAG" ] && set -- "$@" $LOOP_FLAG
[ -n "$HTTP_FLAG" ] && set -- "$@" $HTTP_FLAG

# Add graceful shutdown logic
if [ -n "$GRACE_FLAG" ]; then
  set -- "$@" "$GRACE_FLAG" "$GR"
fi

# Toggle Access Log
case "$ACCESS_RAW" in
  0|false|no|off) set -- "$@" --no-access-log ;;
  *)              set -- "$@" --access-log ;;
esac

# Worker Scaling
if [ "$WC" -gt 1 ]; then
  printf "🌐 Scaling: Starting with %s workers.\n" "$WC"
  set -- "$@" --workers "$WC"
else
  printf "🌐 Scaling: Single worker mode.\n"
fi

# Pass-through for optional tuning
[ -n "${UVICORN_LIMIT_CONCURRENCY:-}" ] && set -- "$@" --limit-concurrency "$(as_int "$UVICORN_LIMIT_CONCURRENCY" 0)"
[ -n "${UVICORN_LIMIT_MAX_REQUESTS:-}" ] && set -- "$@" --limit-max-requests "$(as_int "$UVICORN_LIMIT_MAX_REQUESTS" 0)"
[ -n "${UVICORN_ROOT_PATH:-}" ] && set -- "$@" --root-path "$UVICORN_ROOT_PATH"

printf "✅ Configuration locked. Executing server...\n"
printf "------------------------------------------------------------\n"

# Use exec to ensure Python receives signals directly from the OS/Render
exec "$@"
