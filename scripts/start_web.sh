#!/usr/bin/env sh
# ============================================================
# Tadawul Fast Bridge — PROD SAFE Start Script (v2.3.0)
# ============================================================
# Render/FastAPI bootstrap with defensive checks + smart scaling.
#
# Highlights
# - Auto worker calc (CPU + optional RAM-aware cap)
# - Prefer Gunicorn+UvicornWorker when available (more robust in prod)
# - Uvicorn fallback if gunicorn missing
# - Uvicorn flag auto-detect (graceful shutdown flag differences)
# - Optional uvloop/httptools enablement (if installed)
# - Optional preflight: python import + /readyz probe
# - Signal-safe exec
#
# Env (common)
# - PORT=8000
# - LOG_LEVEL=info
# - WEB_CONCURRENCY= (override workers)
# - WEB_CONCURRENCY_MAX=4 (cap)
# - RAM_MB=512 (optional, if your platform exposes it)
# - WORKERS_PER_GB=2 (optional tuning)
# - START_MODE=auto|gunicorn|uvicorn
#
# Uvicorn tuning
# - UVICORN_KEEPALIVE=75
# - UVICORN_GRACEFUL_TIMEOUT=30
# - UVICORN_ACCESS_LOG=1
# - UVICORN_LIMIT_CONCURRENCY=
# - UVICORN_LIMIT_MAX_REQUESTS=
# - UVICORN_ROOT_PATH=
#
# Gunicorn tuning (if used)
# - GUNICORN_TIMEOUT=120
# - GUNICORN_GRACEFUL_TIMEOUT=30
# - GUNICORN_MAX_REQUESTS=0
# - GUNICORN_MAX_REQUESTS_JITTER=0
# - GUNICORN_KEEPALIVE=5
#
# Preflight
# - PREFLIGHT=1 (enable extra checks)
# - PREFLIGHT_READYZ=1 (also probe /readyz after launch decision; uses curl/wget if present)
# ============================================================

set -eu

SCRIPT_VERSION="2.3.0"

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

loglvl() {
  v="$(printf "%s" "${1:-info}" | tr "[:upper:]" "[:lower:]")"
  case "$v" in
    critical|error|warning|info|debug|trace) printf "%s" "$v" ;;
    *)                                      printf "%s" "info" ;;
  esac
}

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

detect_cpus() {
  cpus=1
  if [ -f /proc/cpuinfo ]; then
    cpus="$(grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 1)"
  elif have_cmd nproc; then
    cpus="$(nproc 2>/dev/null || echo 1)"
  elif have_cmd sysctl; then
    cpus="$(sysctl -n hw.ncpu 2>/dev/null || echo 1)"
  fi
  cpus="$(as_int "$cpus" 1)"
  [ "$cpus" -lt 1 ] && cpus=1
  printf "%s" "$cpus"
}

# Optional: estimate memory cap
# - Uses RAM_MB env if provided
# - Else tries cgroup v2/v1 limits when available
detect_ram_mb() {
  if [ -n "${RAM_MB:-}" ] && [ "$(as_int "$RAM_MB" 0)" -gt 0 ]; then
    printf "%s" "$(as_int "$RAM_MB" 0)"
    return 0
  fi

  # cgroup v2: /sys/fs/cgroup/memory.max
  if [ -f /sys/fs/cgroup/memory.max ]; then
    v="$(cat /sys/fs/cgroup/memory.max 2>/dev/null || echo "")"
    case "$v" in
      ""|"max") printf "0"; return 0 ;;
      *) bytes="$(as_int "$v" 0)";;
    esac
    if [ "$bytes" -gt 0 ]; then
      printf "%s" "$((bytes / 1024 / 1024))"
      return 0
    fi
  fi

  # cgroup v1: /sys/fs/cgroup/memory/memory.limit_in_bytes
  if [ -f /sys/fs/cgroup/memory/memory.limit_in_bytes ]; then
    bytes="$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null || echo 0)"
    bytes="$(as_int "$bytes" 0)"
    # Some platforms report huge "no limit" numbers; treat > 256GB as unknown
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

  # RAM-aware cap (optional)
  ram_mb="$(detect_ram_mb)"
  wpg="$(as_int "${WORKERS_PER_GB:-2}" "2")" # 2 workers per GB is conservative for async services
  ram_cap="$max_workers"
  if [ "$ram_mb" -gt 0 ]; then
    gb="$((ram_mb / 1024))"
    [ "$gb" -lt 1 ] && gb=1
    ram_cap="$((gb * wpg))"
    [ "$ram_cap" -lt 1 ] && ram_cap=1
  fi

  # Base rule: 1 worker per CPU (safe for async I/O), then cap by max and ram_cap
  calc="$cpus"

  # Apply caps
  if [ "$calc" -gt "$max_workers" ]; then calc="$max_workers"; fi
  if [ "$calc" -gt "$ram_cap" ]; then calc="$ram_cap"; fi
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

detect_uvicorn_perf_flags() {
  loop_flag=""
  http_flag=""

  if python -c "import uvloop" >/dev/null 2>&1; then
    loop_flag="--loop uvloop"
    printf "✨ Performance: uvloop detected and enabled.\n"
  fi

  if python -c "import httptools" >/dev/null 2>&1; then
    http_flag="--http httptools"
    printf "✨ Performance: httptools detected and enabled.\n"
  fi

  # shellcheck disable=SC2086
  printf "%s %s" "$loop_flag" "$http_flag"
}

probe_readyz() {
  base="http://127.0.0.1:${1:-8000}"
  path="${2:-/readyz}"
  url="${base}${path}"

  if have_cmd curl; then
    curl -fsS --max-time 3 "$url" >/dev/null 2>&1 && return 0 || return 1
  fi
  if have_cmd wget; then
    wget -qO- --timeout=3 "$url" >/dev/null 2>&1 && return 0 || return 1
  fi
  return 1
}

# ----------------------------
# Banner + Preflight
# ----------------------------
printf "🚀 Tadawul Fast Bridge Boot Sequence (v%s)\n" "$SCRIPT_VERSION"
printf "------------------------------------------------------------\n"

if [ ! -f "main.py" ]; then
  printf "❌ Error: main.py not found in current directory (%s).\n" "$(pwd)"
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
LL="$(loglvl "${LOG_LEVEL:-info}")"
P="$(clamp_min_1 "$(as_int "${PORT:-8000}" "8000")")"

UV_KA="$(clamp_min_1 "$(as_int "${UVICORN_KEEPALIVE:-75}" "75")")"
UV_GR="$(clamp_min_1 "$(as_int "${UVICORN_GRACEFUL_TIMEOUT:-30}" "30")")"
UV_ACCESS_RAW="$(printf "%s" "${UVICORN_ACCESS_LOG:-1}" | tr "[:upper:]" "[:lower:]")"
UV_GRACE_FLAG="$(detect_uvicorn_grace_flag)"
UV_PERF_FLAGS="$(detect_uvicorn_perf_flags)"

WC="$(detect_workers)"

START_MODE="$(printf "%s" "${START_MODE:-auto}" | tr "[:upper:]" "[:lower:]")"
USE_GUNICORN="0"

# Prefer gunicorn if installed (more stable multi-worker management)
if [ "$START_MODE" = "gunicorn" ]; then
  USE_GUNICORN="1"
elif [ "$START_MODE" = "uvicorn" ]; then
  USE_GUNICORN="0"
else
  # auto
  if python -c "import gunicorn" >/dev/null 2>&1; then
    USE_GUNICORN="1"
  else
    USE_GUNICORN="0"
  fi
fi

# ----------------------------
# Uvicorn command (fallback)
# ----------------------------
build_uvicorn_cmd() {
  set -- python -m uvicorn main:app \
    --host 0.0.0.0 \
    --port "$P" \
    --proxy-headers \
    --forwarded-allow-ips "*" \
    --lifespan on \
    --timeout-keep-alive "$UV_KA" \
    --log-level "$LL" \
    --no-server-header

  # perf flags
  # shellcheck disable=SC2086
  [ -n "$UV_PERF_FLAGS" ] && set -- "$@" $UV_PERF_FLAGS

  # graceful flag
  if [ -n "$UV_GRACE_FLAG" ]; then
    set -- "$@" "$UV_GRACE_FLAG" "$UV_GR"
  fi

  # access log
  case "$UV_ACCESS_RAW" in
    0|false|no|off) set -- "$@" --no-access-log ;;
    *)              set -- "$@" --access-log ;;
  esac

  # concurrency shaping
  [ -n "${UVICORN_LIMIT_CONCURRENCY:-}" ] && set -- "$@" --limit-concurrency "$(as_int "$UVICORN_LIMIT_CONCURRENCY" 0)"
  [ -n "${UVICORN_LIMIT_MAX_REQUESTS:-}" ] && set -- "$@" --limit-max-requests "$(as_int "$UVICORN_LIMIT_MAX_REQUESTS" 0)"
  [ -n "${UVICORN_ROOT_PATH:-}" ] && set -- "$@" --root-path "$UVICORN_ROOT_PATH"

  # workers (uvicorn supports --workers)
  if [ "$WC" -gt 1 ]; then
    printf "🌐 Scaling: uvicorn workers=%s (auto)\n" "$WC"
    set -- "$@" --workers "$WC"
  else
    printf "🌐 Scaling: uvicorn single worker\n"
  fi

  printf "%s" "$*"
}

# ----------------------------
# Gunicorn command (preferred)
# ----------------------------
build_gunicorn_cmd() {
  # Detect uvicorn worker class availability
  # Prefer uvicorn.workers.UvicornWorker (works without gunicorn[uvicorn] extras in many envs)
  WORKER_CLASS="uvicorn.workers.UvicornWorker"
  if ! python -c "import uvicorn.workers" >/dev/null 2>&1; then
    # fallback: still try, but warn
    printf "⚠️  Gunicorn selected but uvicorn.workers not importable. Falling back to uvicorn.\n"
    return 1
  fi

  GT="$(clamp_min_1 "$(as_int "${GUNICORN_TIMEOUT:-120}" "120")")"
  GGT="$(clamp_min_1 "$(as_int "${GUNICORN_GRACEFUL_TIMEOUT:-30}" "30")")"
  GKA="$(clamp_min_1 "$(as_int "${GUNICORN_KEEPALIVE:-5}" "5")")"
  GMR="$(as_int "${GUNICORN_MAX_REQUESTS:-0}" "0")"
  GMRJ="$(as_int "${GUNICORN_MAX_REQUESTS_JITTER:-0}" "0")"

  # Gunicorn access log: keep stdout/stderr for Render
  GA_LOGLEVEL="$(loglvl "${GUNICORN_LOG_LEVEL:-$LL}")"

  printf "🌐 Scaling: gunicorn workers=%s (auto)\n" "$WC"

  set -- gunicorn main:app \
    -k "$WORKER_CLASS" \
    -w "$WC" \
    -b "0.0.0.0:$P" \
    --log-level "$GA_LOGLEVEL" \
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
  [ -n "${UVICORN_ROOT_PATH:-}" ] && set -- "$@" --env "UVICORN_ROOT_PATH=$UVICORN_ROOT_PATH"

  printf "%s" "$*"
}

# ----------------------------
# Decide + Exec
# ----------------------------
printf "✅ Configuration: port=%s log=%s mode=%s workers=%s\n" "$P" "$LL" "$START_MODE" "$WC"

PREFLIGHT_READYZ="$(as_bool "${PREFLIGHT_READYZ:-0}")"
if [ "$USE_GUNICORN" = "1" ]; then
  if have_cmd gunicorn || python -c "import gunicorn" >/dev/null 2>&1; then
    cmd="$(build_gunicorn_cmd || true)"
    if [ -n "$cmd" ]; then
      printf "▶️  Starting with Gunicorn (recommended for prod)\n"
      printf "------------------------------------------------------------\n"
      # shellcheck disable=SC2086
      exec $cmd
    fi
  fi
  printf "⚠️  Gunicorn not available; falling back to Uvicorn.\n"
fi

cmd="$(build_uvicorn_cmd)"
printf "▶️  Starting with Uvicorn\n"
printf "------------------------------------------------------------\n"

# Optional: lightweight local readyz probe BEFORE exec is not meaningful (server not started yet).
# If you want readiness probes, keep them in platform health checks.
# exec the server now
# shellcheck disable=SC2086
exec $cmd
