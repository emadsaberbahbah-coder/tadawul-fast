#!/usr/bin/env sh
# ============================================================
# Tadawul Fast Bridge — PROD SAFE Start Script
# - workers + access-log toggle
# - defensive env parsing
# - uvicorn graceful flag auto-detect (version-safe)
# ============================================================

set -eu

# ----------------------------
# Helpers
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
  if [ "$v" -lt 1 ]; then v="1"; fi
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
    *)
      if [ "$raw" -gt 0 ]; then printf "%s" "$raw"; else printf "%s" ""; fi
      ;;
  esac
}

detect_grace_flag() {
  # Your logs show --timeout-graceful-shutdown works, but detect for safety.
  help="$(python -m uvicorn --help 2>&1 || true)"
  if printf "%s" "$help" | grep -q -- "--timeout-graceful-shutdown"; then
    printf "%s" "--timeout-graceful-shutdown"
    return 0
  fi
  if printf "%s" "$help" | grep -q -- "--graceful-timeout"; then
    printf "%s" "--graceful-timeout"
    return 0
  fi
  printf "%s" ""
}

# ----------------------------
# Read env (defensive)
# ----------------------------
LL="$(loglvl "${LOG_LEVEL:-info}")"
P="$(clamp_min_1 "$(as_int "${PORT:-8000}" "8000")")"
KA="$(clamp_min_1 "$(as_int "${UVICORN_KEEPALIVE:-75}" "75")")"
GR="$(clamp_min_1 "$(as_int "${UVICORN_GRACEFUL_TIMEOUT:-30}" "30")")"
WC="$(clamp_min_1 "$(as_int "${WEB_CONCURRENCY:-1}" "1")")"

ACCESS_RAW="$(printf "%s" "${UVICORN_ACCESS_LOG:-1}" | tr "[:upper:]" "[:lower:]")"

LC="$(pos_int_or_empty "${UVICORN_LIMIT_CONCURRENCY:-}")"
LMR="$(pos_int_or_empty "${UVICORN_LIMIT_MAX_REQUESTS:-}")"
BL="$(pos_int_or_empty "${UVICORN_BACKLOG:-}")"
RP="${UVICORN_ROOT_PATH:-}"

GRACE_FLAG="$(detect_grace_flag)"

# ----------------------------
# Build argv (no eval)
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

# graceful shutdown flag (only if supported)
if [ -n "$GRACE_FLAG" ]; then
  set -- "$@" "$GRACE_FLAG" "$GR"
fi

# access log toggle
case "$ACCESS_RAW" in
  0|false|no|off) set -- "$@" --no-access-log ;;
  *)              set -- "$@" --access-log ;;
esac

# workers only if > 1
if [ "$WC" -gt 1 ]; then
  set -- "$@" --workers "$WC"
fi

# optional tuning flags
if [ -n "$LC" ]; then
  set -- "$@" --limit-concurrency "$LC"
fi
if [ -n "$LMR" ]; then
  set -- "$@" --limit-max-requests "$LMR"
fi
if [ -n "$BL" ]; then
  set -- "$@" --backlog "$BL"
fi
if [ -n "$RP" ]; then
  set -- "$@" --root-path "$RP"
fi

exec "$@"
