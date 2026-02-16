#!/usr/bin/env sh
# ============================================================
# Tadawul Fast Bridge — PROD SAFE Start Script (v2.4.0)
# ============================================================
# Render/FastAPI bootstrap with defensive checks + smart scaling.
#
# Key upgrades vs your v2.3.0:
# - Always runs from PROJECT ROOT (so main.py/core/routes import paths are correct)
# - Zero “string command” exec (no word-splitting risk) — exec uses argv safely
# - Better Gunicorn vs Uvicorn selection + loglevel mapping (gunicorn has no "trace")
# - Optional backlog support for uvicorn
# - Clear banner + prints resolved mode/workers/port so you can confirm Render is using this script
#
# Start in Render should be:  sh scripts/start_web.sh
# ============================================================

set -eu

SCRIPT_VERSION="2.4.0"

# ----------------------------
# Resolve project root (so "main.py" check is real)
# ----------------------------
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
APP_DIR="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$APP_DIR"

# Ensure imports find project root
PYTHONPATH="${PYTHONPATH:-}"
case ":$PYTHONPATH:" in
  *":$APP_DIR:"*) : ;;
  *) PYTHONPATH="$APP_DIR${PYTHONPATH:+:$PYTHONPATH}" ;;
esac
export PYTHONPATH

# ----------------------------
# Helpers (POSIX sh)
# ----------------------------
as_int() {
  raw="${1:-}"
  def="${2:-0}"
  case "$raw" in
    ""|*[!0-9]*) printf "%s" "$def" ;;
    *)           printf "%s" "$raw" ;;
  esac
}

as_bool() {
  v="$(printf "%s" "${1:-}" | tr "[:upper:]" "[:lower:]")"
  case "$v" in
    1|true|yes|y|on) printf "1" ;;
    *)              printf "0" ;;
  esac
}

clamp_min_1() {
  v="$(as_int "${1:-}" "1")"
  [ "$v" -lt 1 ] && v="1"
  printf "%s" "$v"
}

loglvl_uvicorn() {
  v="$(printf "%s" "${1:-info}" | tr "[:upper:]" "[:lower:]")"
  case "$v" in
    critical|error|warning|info|debug|trace) printf "%s" "$v" ;;
    *)                                      printf "%s" "info" ;;
  esac
}

# Gunicorn doesn't support "trace"
loglvl_gunicorn() {
  v="$(printf "%s" "${1:-info}" | tr "[:upper:]" "[:lower:]")"
  case "$v" in
    critical|error|warning|info|debug) printf "%s" "$v" ;;
    trace)                             printf "%s" "debug" ;;
    *)                                 printf "%s" "info" ;;
  esac
}

have_cmd() { command -v "$1" >/dev/null 2>&1; }

detect_cpus() {
  cpus=1
  if [ -f /proc/cpuinfo ]; then
    cpus="$(grep -c '^processor' /proc/cpuinfo 2>/dev/null || echo 1)"
  elif have_cmd nproc; then
    cpus="$(nproc 2>/dev/null || echo 1)"
  elif have_cmd sysctl; then
    cpus="$(sysctl -n hw.ncpu 2>/dev/null || echo 1)"
  fi
  cpus="$(as_int "$cpus" 1)"
  [ "$cpus" -lt 1 ] && cpus=1
  printf "%s" "$cpus"
}

detect_ram_mb() {
  # Prefer explicit env
  if [ -n "${RAM_MB:-}" ] && [ "$(as_int "$RAM_MB" 0)" -gt 0 ]; then
    printf "%s" "$(as_int "$RAM_MB" 0)"
    return 0
  fi

  # cgroup v2
  if [ -f /sys/fs/cgroup/memory.max ]; then
    v="$(cat /sys/fs/cgroup/memory.max 2>/dev/null || echo "")"
    case "$v" in
      ""|"max") printf "0"; return 0 ;;
      *) bytes="$(as_int "$v" 0)" ;;
    esac
    if [ "$bytes" -gt 0 ]; then
      printf "%s" "$((bytes / 1024 / 1024))"
      return 0
    fi
  fi

  # cgroup v1
  if [ -f /sys/fs/cgroup/memory/memory.limit_in_bytes ]; then
    bytes="$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null || echo 0)"
    bytes="$(as_int "$bytes" 0)"
    if [ "$bytes" -gt 0 ] && [ "$bytes" -lt $((256 * 1024 * 1024 * 1024)) ]; then
      printf "%s" "$((bytes / 1024 / 1024))"
      return 0
    fi
  fi

  printf "0"
}

detect_workers() {
  # Manual override
  if [ -n "${WEB_CONCURRENCY:-}" ] && [ "$(as_int "$WEB_CONCURRENCY" 0)" -gt 0 ]; then
    printf "%s" "$(as_int "$WEB_CONCURRENCY" 1)"
    return 0
  fi

  cpus="$(detect_cpus)"
  max_workers="$(as_int "${WEB_CONCURRENCY_MAX:-4}" "4")"
  [ "$max_workers" -lt 1 ] && max_workers=1

  ram_mb="$(detect_ram_mb)"
  wpg="$(as_int "${WORKERS_PER_GB:-2}" "2")"
  ram_cap="$max_workers"
  if [ "$ram_mb" -gt 0 ]; then
    gb="$((ram_mb / 1024))"
    [ "$gb" -lt 1 ] && gb=1
    ram_cap="$((gb * wpg))"
    [ "$ram_cap" -lt 1 ] && ram_cap=1
  fi

  calc="$cpus"
  [ "$calc" -gt "$max_workers" ] && calc="$max_workers"
  [ "$calc" -gt "$ram_cap" ] && calc="$ram_cap"
  [ "$calc" -lt 1 ] && calc=1

  printf "%s" "$calc"
}

detect_uvicorn_grace_flag() {
  help_text="$(python -m uvicorn --help 2>&1 || true)"
  if printf "%s" "$help_text" | grep -q -- "--timeout-graceful-shutdown"; then
    printf "%s" "--timeout-graceful-shutdown"
  elif printf "%s" "$help_text" | grep -q -- "--graceful-timeout"; then
    printf "%s" "--graceful-timeout"
  else
    printf "%s" ""
  fi
}

# Build perf flags as positional args via echo tokens
detect_uvicorn_perf_flags() {
  if python -c "import uvloop" >/dev/null 2>&1; then
    printf "%s\n" "--loop" "uvloop"
    printf "✨ Performance: uvloop detected and enabled.\n" >&2
  fi
  if python -c "import httptools" >/dev/null 2>&1; then
    printf "%s\n" "--http" "httptools"
    printf "✨ Performance: httptools detected and enabled.\n" >&2
  fi
}

# ----------------------------
# Banner + Preflight
# ----------------------------
printf "🚀 Tadawul Fast Bridge Boot Sequence (start_web.sh v%s)\n" "$SCRIPT_VERSION"
printf "📁 Project root: %s\n" "$APP_DIR"
printf "------------------------------------------------------------\n"

if [ ! -f "main.py" ]; then
  printf "❌ Error: main.py not found in project root (%s).\n" "$APP_DIR"
  exit 1
fi

if ! python -c "import uvicorn" >/dev/null 2>&1; then
  printf "❌ Error: uvicorn package is not installed.\n"
  exit 1
fi

PREFLIGHT="$(as_bool "${PREFLIGHT:-0}")"
if [ "$PREFLIGHT" = "1" ]; then
  printf "🔎 Preflight: importing main:app ...\n"
  if ! python -c "import importlib; m=importlib.import_module('main'); getattr(m,'app')" >/dev/null 2>&1; then
    printf "❌ Preflight failed: cannot import main.app (startup would fail).\n"
    exit 1
  fi
  printf "✅ Preflight: main.app import OK.\n"
fi

# ----------------------------
# Environment Resolution
# ----------------------------
PORT_RAW="$(as_int "${PORT:-8000}" "8000")"
P="$(clamp_min_1 "$PORT_RAW")"

LL_UV="$(loglvl_uvicorn "${LOG_LEVEL:-info}")"
LL_GN="$(loglvl_gunicorn "${LOG_LEVEL:-info}")"

UV_KA="$(clamp_min_1 "$(as_int "${UVICORN_KEEPALIVE:-75}" "75")")"
UV_GR="$(clamp_min_1 "$(as_int "${UVICORN_GRACEFUL_TIMEOUT:-30}" "30")")"
UV_ACCESS_RAW="$(printf "%s" "${UVICORN_ACCESS_LOG:-1}" | tr "[:upper:]" "[:lower:]")"
UV_GRACE_FLAG="$(detect_uvicorn_grace_flag)"

UV_BACKLOG_RAW="$(as_int "${UVICORN_BACKLOG:-0}" "0")"
UV_BACKLOG="$UV_BACKLOG_RAW"

WC="$(detect_workers)"

START_MODE="$(printf "%s" "${START_MODE:-auto}" | tr "[:upper:]" "[:lower:]")"
USE_GUNICORN="0"
if [ "$START_MODE" = "gunicorn" ]; then
  USE_GUNICORN="1"
elif [ "$START_MODE" = "uvicorn" ]; then
  USE_GUNICORN="0"
else
  # auto
  if have_cmd gunicorn || python -c "import gunicorn" >/dev/null 2>&1; then
    USE_GUNICORN="1"
  else
    USE_GUNICORN="0"
  fi
fi

printf "✅ Resolved: port=%s log=%s mode=%s workers=%s\n" "$P" "$LL_UV" "$START_MODE" "$WC"

# ----------------------------
# Start with Gunicorn (preferred when available)
# ----------------------------
if [ "$USE_GUNICORN" = "1" ]; then
  # Ensure gunicorn exists and uvicorn worker class importable
  if (have_cmd gunicorn || python -c "import gunicorn" >/dev/null 2>&1) && python -c "import uvicorn.workers" >/dev/null 2>&1; then
    GT="$(clamp_min_1 "$(as_int "${GUNICORN_TIMEOUT:-120}" "120")")"
    GGT="$(clamp_min_1 "$(as_int "${GUNICORN_GRACEFUL_TIMEOUT:-30}" "30")")"
    GKA="$(clamp_min_1 "$(as_int "${GUNICORN_KEEPALIVE:-5}" "5")")"
    GMR="$(as_int "${GUNICORN_MAX_REQUESTS:-0}" "0")"
    GMRJ="$(as_int "${GUNICORN_MAX_REQUESTS_JITTER:-0}" "0")"

    printf "▶️  Starting with Gunicorn (recommended for prod)\n"
    printf "------------------------------------------------------------\n"

    set -- gunicorn main:app \
      -k "uvicorn.workers.UvicornWorker" \
      -w "$WC" \
      -b "0.0.0.0:$P" \
      --log-level "$LL_GN" \
      --access-logfile "-" \
      --error-logfile "-" \
      --timeout "$GT" \
      --graceful-timeout "$GGT" \
      --keep-alive "$GKA" \
      --worker-tmp-dir "/dev/shm"

    # Optional request recycling
    if [ "$GMR" -gt 0 ]; then
      set -- "$@" --max-requests "$GMR"
    fi
    if [ "$GMRJ" -gt 0 ]; then
      set -- "$@" --max-requests-jitter "$GMRJ"
    fi

    # Propagate root-path if you use reverse proxy subpath
    if [ -n "${UVICORN_ROOT_PATH:-}" ]; then
      set -- "$@" --env "UVICORN_ROOT_PATH=$UVICORN_ROOT_PATH"
    fi

    printf "CMD: %s\n" "$*"
    exec "$@"
  fi

  printf "⚠️  Gunicorn requested/auto-selected but not usable (missing gunicorn or uvicorn.workers). Falling back to Uvicorn.\n"
fi

# ----------------------------
# Start with Uvicorn (fallback / single binary)
# ----------------------------
printf "▶️  Starting with Uvicorn\n"
printf "------------------------------------------------------------\n"

set -- python -m uvicorn main:app \
  --host 0.0.0.0 \
  --port "$P" \
  --proxy-headers \
  --forwarded-allow-ips "*" \
  --lifespan on \
  --timeout-keep-alive "$UV_KA" \
  --log-level "$LL_UV" \
  --no-server-header

# Perf flags (emit tokens one-per-line, append safely)
# shellcheck disable=SC2046
set -- "$@" $(detect_uvicorn_perf_flags || true)

# Graceful flag (uvicorn version differences)
if [ -n "$UV_GRACE_FLAG" ]; then
  set -- "$@" "$UV_GRACE_FLAG" "$UV_GR"
fi

# Access log toggle
case "$UV_ACCESS_RAW" in
  0|false|no|off) set -- "$@" --no-access-log ;;
  *)              set -- "$@" --access-log ;;
esac

# Optional backlog (if you set UVICORN_BACKLOG > 0)
if [ "$UV_BACKLOG" -gt 0 ]; then
  set -- "$@" --backlog "$UV_BACKLOG"
fi

# Concurrency shaping
if [ -n "${UVICORN_LIMIT_CONCURRENCY:-}" ]; then
  lc="$(as_int "$UVICORN_LIMIT_CONCURRENCY" 0)"
  [ "$lc" -gt 0 ] && set -- "$@" --limit-concurrency "$lc"
fi

if [ -n "${UVICORN_LIMIT_MAX_REQUESTS:-}" ]; then
  lmr="$(as_int "$UVICORN_LIMIT_MAX_REQUESTS" 0)"
  [ "$lmr" -gt 0 ] && set -- "$@" --limit-max-requests "$lmr"
fi

if [ -n "${UVICORN_ROOT_PATH:-}" ]; then
  set -- "$@" --root-path "$UVICORN_ROOT_PATH"
fi

# Workers (uvicorn supports --workers)
if [ "$WC" -gt 1 ]; then
  printf "🌐 Scaling: uvicorn workers=%s (auto)\n" "$WC"
  set -- "$@" --workers "$WC"
else
  printf "🌐 Scaling: uvicorn single worker\n"
fi

printf "CMD: %s\n" "$*"
exec "$@"
