#!/usr/bin/env python3
"""
scripts/cleanup_cache.py
================================================================================
TADAWUL FAST BRIDGE — DISTRIBUTED CACHE CLEANUP UTILITY (v4.1.0)
================================================================================
STABLE EDITION | SAFE ASYNC | CRON FRIENDLY | PARTIAL-IMPORT RESILIENT

Why this revision (v4.1.0 vs v4.0.1)
-------------------------------------
- 🔑 FIX CRITICAL: v4.0.1 bundled FOUR independent core imports
      (`core.data_engine_v2.get_cache`,
       `core.providers.yahoo_chart_provider.clear_cache`,
       `core.symbols_reader._reader`,
       `core.news_intelligence.clear_cache`) into a single `try/except`
      block. If ANY ONE failed (e.g., a partial deploy without
      `news_intelligence`), `_CORE_AVAILABLE` became `False` and ALL local
      cleanup was silently skipped. v4.1.0 splits this into four independent
      try/except blocks with per-component availability flags
      (`_V2_CACHE_AVAILABLE`, `_YAHOO_CLEAR_AVAILABLE`,
      `_SYMBOLS_READER_AVAILABLE`, `_NEWS_CLEAR_AVAILABLE`).

- 🔑 FIX HIGH: v4.0.1 accessed private attributes
      (`core.symbols_reader._reader._lock` / `._cache.clear()`) because
      `core.symbols_reader` publishes no public `clear_cache()`. v4.1.0 uses
      a 3-tier probe: (1) module-level public `clear_cache()` / `clear_caches()`,
      (2) `get_reader().clear_cache()` / `.clear_caches()`,
      (3) legacy `_reader._lock` + `_reader._cache.clear()` fallback.
      Records which tier succeeded in `stats.namespaces_cleared`.

- 🔑 FIX HIGH: Redis `FLUSHDB` on `--all` is now gated. By default, `--all`
      expands to all known namespace patterns (safe, reversible by namespace).
      To trigger a true `FLUSHDB`, pass `--confirm-flushdb` OR set
      `CLEANUP_ALLOW_FLUSHDB=true`. Without confirmation, `FLUSHDB` is
      refused and the cleanup falls back to pattern-based clearing.

- FIX: `FLUSHDB` is NOT retried. A failed FLUSHDB is logged and surfaces
      to `stats.errors`. Retries on destructive ops compound damage.

- FIX: `maybe_await` now uses `inspect.isawaitable(x)` — the project-wide
      canonical probe (matches `routes.investment_advisor v5.2.0`,
      `routes.enriched_quote v8.5.0`, `audit_data_quality.py v4.5.0`).

- FIX: `_TRUTHY` / `_FALSY` realigned to exact `main._TRUTHY` /
      `main._FALSY` vocabulary. v4.0.1 inline set was missing
      `t` / `enabled` / `enable`.

- FIX: Redis close now wraps `redis.close()` and
      `pool.disconnect(inuse_connections=True)` in `try/finally` blocks
      inside `run_cleanup`, so the connection is always released —
      including on exceptions mid-run.

- FIX: `SERVICE_VERSION = SCRIPT_VERSION` alias exported for cross-script
      consistency (matches `audit_data_quality.py v4.5.0`).

- FIX: Disk-cleanup directory and glob are now env-configurable:
        CLEANUP_DISK_CACHE_DIR  (default: /tmp)
        CLEANUP_DISK_CACHE_GLOB (default: cache_*)

- FIX: Per-component availability is logged at startup so operators can
      see WHY a component was skipped.

- KEEP: CleanupStats dataclass shape, CLI flags (with `--confirm-flushdb`
      ADDED), TraceContext OTEL usage, orjson fallback, FullJitterBackoff
      for non-destructive retries, `redis.asyncio` usage.

CLI examples
------------
- Safe: clear only quotes + history (Redis + local) respecting defaults
    python scripts/cleanup_cache.py

- Clear everything EXCEPT FLUSHDB (pattern-based; reversible by namespace)
    python scripts/cleanup_cache.py --all

- Clear everything INCLUDING Redis FLUSHDB (irreversible!)
    python scripts/cleanup_cache.py --all --confirm-flushdb
    # or: CLEANUP_ALLOW_FLUSHDB=true python scripts/cleanup_cache.py --all

- Dry-run
    python scripts/cleanup_cache.py --all --dry-run

- JSON output for cron/CI parsing
    python scripts/cleanup_cache.py --json

Environment
-----------
- REDIS_URL                 : Redis connection string (optional)
- CORE_TRACING_ENABLED      : 1|true|yes|on|t|enabled to emit OTEL spans
- CLEANUP_ALLOW_FLUSHDB     : 1|true|... to allow `--all` to FLUSHDB
- CLEANUP_DISK_CACHE_DIR    : base dir for disk cleanup (default: /tmp)
- CLEANUP_DISK_CACHE_GLOB   : glob pattern (default: cache_*)

Exit codes
----------
- 0   = success, no errors
- 1   = completed with one or more non-fatal errors
- 130 = interrupted via Ctrl-C
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import inspect
import logging
import os
import random
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        opt = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(v, option=opt).decode("utf-8")

    _HAS_ORJSON = True
except Exception:
    import json

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        return json.dumps(v, indent=indent if indent else None, default=str, ensure_ascii=False)

    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
SCRIPT_VERSION = "4.1.0"
SERVICE_VERSION = SCRIPT_VERSION  # v4.1.0: cross-script alias

# ---------------------------------------------------------------------------
# Project-wide truthy/falsy vocabulary (matches main._TRUTHY / main._FALSY)
# ---------------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}


def _env_bool(name: str, default: bool = False) -> bool:
    """Project-aligned env bool parser."""
    try:
        raw = (os.getenv(name, "") or "").strip().lower()
    except Exception:
        return bool(default)
    if not raw:
        return bool(default)
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return bool(default)


# ---------------------------------------------------------------------------
# Optional Redis (modern redis-py — redis.asyncio)
# ---------------------------------------------------------------------------
try:
    import redis.asyncio as redis_asyncio  # type: ignore
    from redis.asyncio import Redis  # type: ignore

    _REDIS_AVAILABLE = True
except Exception:
    redis_asyncio = None  # type: ignore
    Redis = Any  # type: ignore
    _REDIS_AVAILABLE = False

# ---------------------------------------------------------------------------
# Optional OpenTelemetry (v4.0.0 fix: start_as_current_span is a CM)
# ---------------------------------------------------------------------------
_TRACING_ENABLED = _env_bool("CORE_TRACING_ENABLED", False)

try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    _OTEL_AVAILABLE = True
    _TRACER = trace.get_tracer(__name__)
except Exception:
    _OTEL_AVAILABLE = False
    Status = StatusCode = None  # type: ignore

    class _DummyCM:
        def __enter__(self): return None
        def __exit__(self, *args, **kwargs): return False

    class _DummyTracer:
        def start_as_current_span(self, *args, **kwargs): return _DummyCM()

    _TRACER = _DummyTracer()  # type: ignore

# ---------------------------------------------------------------------------
# Per-component optional core imports (v4.1.0 — independent try/except blocks)
# v4.0.1 monolithic block set _CORE_AVAILABLE=False if ANY component failed.
# v4.1.0 flags each component separately so partial availability works.
# ---------------------------------------------------------------------------
_V2_CACHE_AVAILABLE = False
_V2_CACHE_IMPORT_ERROR: Optional[str] = None
try:
    from core.data_engine_v2 import get_cache as get_v2_cache  # type: ignore
    _V2_CACHE_AVAILABLE = True
except Exception as _e:
    _V2_CACHE_IMPORT_ERROR = f"{type(_e).__name__}: {_e}"
    def get_v2_cache() -> Any:  # type: ignore
        return None

_YAHOO_CLEAR_AVAILABLE = False
_YAHOO_CLEAR_IMPORT_ERROR: Optional[str] = None
try:
    from core.providers.yahoo_chart_provider import clear_cache as clear_yahoo  # type: ignore
    _YAHOO_CLEAR_AVAILABLE = True
except Exception as _e:
    _YAHOO_CLEAR_IMPORT_ERROR = f"{type(_e).__name__}: {_e}"
    async def clear_yahoo() -> None:  # type: ignore
        return None

# core.symbols_reader — NO public clear_cache() in v3.0.0 __all__.
# v4.1.0 uses 3-tier defensive probing (see _clear_symbols_cache below).
_SYMBOLS_READER_AVAILABLE = False
_SYMBOLS_READER_IMPORT_ERROR: Optional[str] = None
try:
    import core.symbols_reader as _core_symbols_reader  # type: ignore
    _SYMBOLS_READER_AVAILABLE = True
except Exception as _e:
    _SYMBOLS_READER_IMPORT_ERROR = f"{type(_e).__name__}: {_e}"
    _core_symbols_reader = None  # type: ignore

_NEWS_CLEAR_AVAILABLE = False
_NEWS_CLEAR_IMPORT_ERROR: Optional[str] = None
try:
    from core.news_intelligence import clear_cache as clear_news  # type: ignore
    _NEWS_CLEAR_AVAILABLE = True
except Exception as _e:
    _NEWS_CLEAR_IMPORT_ERROR = f"{type(_e).__name__}: {_e}"
    async def clear_news() -> None:  # type: ignore
        return None


def _component_availability() -> Dict[str, Dict[str, Any]]:
    return {
        "data_engine_v2.get_cache": {
            "available": _V2_CACHE_AVAILABLE,
            "error": _V2_CACHE_IMPORT_ERROR,
        },
        "yahoo_chart_provider.clear_cache": {
            "available": _YAHOO_CLEAR_AVAILABLE,
            "error": _YAHOO_CLEAR_IMPORT_ERROR,
        },
        "symbols_reader": {
            "available": _SYMBOLS_READER_AVAILABLE,
            "error": _SYMBOLS_READER_IMPORT_ERROR,
        },
        "news_intelligence.clear_cache": {
            "available": _NEWS_CLEAR_AVAILABLE,
            "error": _NEWS_CLEAR_IMPORT_ERROR,
        },
    }


# =============================================================================
# Logging
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("CacheCleanup")


# =============================================================================
# Helpers
# =============================================================================
async def maybe_await(x: Any) -> Any:
    """
    v4.1.0: uses `inspect.isawaitable(x)` — the project-wide canonical
    probe. Previously used `asyncio.iscoroutine(x) or hasattr(x,
    "__await__")` which is slightly less accurate.
    """
    if inspect.isawaitable(x):
        return await x
    return x


class TraceContext:
    """
    Correct OTEL handling (v4.0.0 fix preserved):
    tracer.start_as_current_span() returns a context manager.
    """

    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._cm = None
        self.span = None

    def __enter__(self):
        if _OTEL_AVAILABLE and _TRACING_ENABLED:
            self._cm = _TRACER.start_as_current_span(self.name)
            self.span = self._cm.__enter__()
            if self.span and self.attributes:
                for k, v in self.attributes.items():
                    try:
                        self.span.set_attribute(k, v)
                    except Exception:
                        pass
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if _OTEL_AVAILABLE and _TRACING_ENABLED and self.span and exc_val and Status and StatusCode:
            try:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            except Exception:
                pass
        if self._cm:
            return self._cm.__exit__(exc_type, exc_val, exc_tb)
        return False

    async def __aenter__(self):  # pragma: no cover
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):  # pragma: no cover
        return self.__exit__(exc_type, exc_val, exc_tb)


@dataclass(slots=True)
class CleanupStats:
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    keys_deleted: int = 0
    namespaces_cleared: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    redis_connected: bool = False
    disk_freed_mb: float = 0.0
    flushdb_allowed: bool = False
    flushdb_executed: bool = False

    @property
    def duration_sec(self) -> float:
        if self.end_time:
            return self.end_time - self.start_time
        return time.time() - self.start_time

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": SCRIPT_VERSION,
            "duration_sec": round(self.duration_sec, 3),
            "keys_deleted": self.keys_deleted,
            "namespaces_cleared": self.namespaces_cleared,
            "redis_connected": self.redis_connected,
            "flushdb_allowed": self.flushdb_allowed,
            "flushdb_executed": self.flushdb_executed,
            "disk_freed_mb": round(self.disk_freed_mb, 2),
            "errors": self.errors,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "component_availability": _component_availability(),
        }


class FullJitterBackoff:
    """AWS-style Full Jitter Backoff for async ops. NOT used for FLUSHDB."""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 15.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    async def execute_async(self, func: Callable, *args, **kwargs) -> Any:
        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                res = func(*args, **kwargs)
                return await maybe_await(res)
            except Exception as e:
                last_exc = e
                if attempt >= self.max_retries - 1:
                    raise
                cap = min(self.max_delay, self.base_delay * (2 ** attempt))
                sleep_time = random.uniform(0, cap)
                logger.debug("Retry after error=%s sleep=%.2fs", e, sleep_time)
                await asyncio.sleep(sleep_time)
        raise last_exc  # type: ignore


# =============================================================================
# Symbols-reader defensive clearing (v4.1.0 NEW — 3-tier probe)
# =============================================================================
async def _clear_symbols_cache(stats: CleanupStats) -> Optional[str]:
    """
    3-tier defensive probe — returns the tier label that succeeded, or None.

    Tier 1: module-level public `clear_cache()` / `clear_caches()`
    Tier 2: `get_reader().clear_cache()` / `.clear_caches()`
    Tier 3: legacy private `_reader._lock` + `_reader._cache.clear()`
    """
    if not _SYMBOLS_READER_AVAILABLE or _core_symbols_reader is None:
        return None

    # Tier 1: module-level public API
    for fn_name in ("clear_cache", "clear_caches"):
        fn = getattr(_core_symbols_reader, fn_name, None)
        if callable(fn):
            try:
                await maybe_await(fn())
                return "tier1_module_public"
            except Exception as e:
                stats.errors.append(f"symbols_reader.{fn_name}() failed: {e}")

    # Tier 2: instance via get_reader() → .clear_cache() / .clear_caches()
    get_reader = getattr(_core_symbols_reader, "get_reader", None)
    if callable(get_reader):
        try:
            reader = get_reader()
            if reader is not None:
                for method_name in ("clear_cache", "clear_caches"):
                    m = getattr(reader, method_name, None)
                    if callable(m):
                        try:
                            await maybe_await(m())
                            return "tier2_instance_public"
                        except Exception as e:
                            stats.errors.append(
                                f"symbols_reader.get_reader().{method_name}() failed: {e}"
                            )
        except Exception as e:
            stats.errors.append(f"symbols_reader.get_reader() failed: {e}")

    # Tier 3: legacy private attribute access (v4.0.1 behavior)
    private_reader = getattr(_core_symbols_reader, "_reader", None)
    if private_reader is not None:
        try:
            lock = getattr(private_reader, "_lock", None)
            cache = getattr(private_reader, "_cache", None)
            if lock is not None and cache is not None and hasattr(cache, "clear"):
                with lock:
                    cache.clear()
                return "tier3_private_attrs"
        except Exception as e:
            stats.errors.append(f"symbols_reader._reader private-attr clear failed: {e}")

    # No tier worked — surface a diagnostic
    stats.errors.append(
        "symbols_reader: no clear path available "
        "(no public clear_cache, no get_reader().clear_cache, no _reader._cache)"
    )
    return None


# =============================================================================
# Redis Ops
# =============================================================================
async def _get_redis() -> Optional[Redis]:
    if not _REDIS_AVAILABLE or redis_asyncio is None:
        return None
    url = os.getenv("REDIS_URL", "").strip()
    if not url:
        return None
    try:
        r = redis_asyncio.from_url(url, decode_responses=True, socket_timeout=5.0)
        await r.ping()
        return r
    except Exception as e:
        logger.warning("Redis connect failed: %s", e)
        return None


async def clear_redis_namespace(redis: Redis, pattern: str) -> int:
    """Clear keys matching a pattern using SCAN (non-blocking)."""
    deleted = 0
    cursor = 0
    try:
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=1000)
            if keys:
                deleted += await redis.delete(*keys)
            if cursor == 0:
                break
    except Exception as e:
        logger.error("Redis clear failed pattern=%s err=%s", pattern, e)
    return int(deleted)


async def _close_redis(redis: Redis) -> None:
    """Defensive close. Each step in its own try/except to avoid double-close warnings."""
    try:
        await redis.close()
    except Exception:
        pass
    try:
        pool = getattr(redis, "connection_pool", None)
        if pool is not None and hasattr(pool, "disconnect"):
            await pool.disconnect(inuse_connections=True)
    except Exception:
        pass


# =============================================================================
# Disk cleanup (v4.1.0: env-configurable)
# =============================================================================
async def clear_local_disk_cache() -> float:
    """Clear disk cache directories. Path + glob are env-configurable."""
    freed_mb = 0.0
    base = os.getenv("CLEANUP_DISK_CACHE_DIR", "/tmp").strip() or "/tmp"
    glob_pat = os.getenv("CLEANUP_DISK_CACHE_GLOB", "cache_*").strip() or "cache_*"

    tmp_dir = Path(base)
    if not tmp_dir.exists():
        return 0.0

    for item in tmp_dir.glob(glob_pat):
        if item.is_dir():
            try:
                size = 0
                for f in item.rglob("*"):
                    if f.is_file():
                        try:
                            size += f.stat().st_size
                        except Exception:
                            pass
                freed_mb += size / (1024 * 1024)
                shutil.rmtree(item, ignore_errors=True)
            except Exception as e:
                logger.debug("Disk cleanup failed %s: %s", item, e)

    return freed_mb


# =============================================================================
# Main cleanup orchestration
# =============================================================================
# Canonical namespace → Redis pattern mapping.
_NAMESPACE_PATTERNS: Dict[str, List[str]] = {
    "quotes": ["data_engine:*", "quote:*"],
    "history": ["history:*"],
    "yahoo": ["yahoo:*"],
    "symbols": ["symbols:*"],
    "news": ["news:*"],
    "argaam": ["argaam:*"],
    "ml": ["ml_pred:*"],
}


def _expand_patterns(namespaces: List[str], all_caches: bool, flushdb_allowed: bool) -> List[str]:
    """
    v4.1.0:
    - If `all_caches` AND `flushdb_allowed`: returns ['*'] (FLUSHDB).
    - If `all_caches` but NOT `flushdb_allowed`: expands to every known
      namespace pattern (safe, reversible).
    - Otherwise: expands only requested namespaces.
    """
    if all_caches and flushdb_allowed:
        return ["*"]

    patterns: List[str] = []
    namespace_set: List[str]
    if all_caches:
        namespace_set = list(_NAMESPACE_PATTERNS.keys())
    else:
        namespace_set = [str(x).strip().lower() for x in (namespaces or []) if str(x).strip()]

    for ns in namespace_set:
        pats = _NAMESPACE_PATTERNS.get(ns)
        if pats:
            patterns.extend(pats)
    return patterns


async def run_cleanup(
    namespaces: List[str],
    all_caches: bool = False,
    dry_run: bool = False,
    confirm_flushdb: bool = False,
) -> CleanupStats:
    stats = CleanupStats()
    backoff = FullJitterBackoff()

    flushdb_allowed = bool(confirm_flushdb) or _env_bool("CLEANUP_ALLOW_FLUSHDB", False)
    stats.flushdb_allowed = flushdb_allowed

    # 1) Redis connection
    redis = await _get_redis()
    if redis is not None:
        stats.redis_connected = True
        logger.info("Connected to Redis.")

    try:
        # 2) Local memory cleanup — v4.1.0: per-component gating
        if not dry_run:
            with TraceContext("local_memory_cleanup"):
                # v2 cache
                if (all_caches or "quotes" in namespaces) and _V2_CACHE_AVAILABLE:
                    try:
                        cache = get_v2_cache()
                        if cache is not None and hasattr(cache, "clear"):
                            await maybe_await(cache.clear())
                            stats.namespaces_cleared.append("local:data_engine_v2")
                            logger.info("Local cleared: data_engine_v2 cache")
                    except Exception as e:
                        stats.errors.append(f"data_engine_v2.get_cache().clear() failed: {e}")

                # Yahoo provider cache (async def clear_cache → None)
                if (all_caches or "yahoo" in namespaces) and _YAHOO_CLEAR_AVAILABLE:
                    try:
                        await maybe_await(clear_yahoo())
                        stats.namespaces_cleared.append("local:yahoo_chart")
                        logger.info("Local cleared: yahoo_chart_provider cache")
                    except Exception as e:
                        stats.errors.append(f"yahoo_chart_provider.clear_cache() failed: {e}")

                # Symbols reader — 3-tier defensive probe
                if (all_caches or "symbols" in namespaces) and _SYMBOLS_READER_AVAILABLE:
                    tier = await _clear_symbols_cache(stats)
                    if tier:
                        stats.namespaces_cleared.append(f"local:symbols[{tier}]")
                        logger.info("Local cleared: symbols_reader (via %s)", tier)

                # News intelligence cache (async def clear_cache → None)
                if (all_caches or "news" in namespaces) and _NEWS_CLEAR_AVAILABLE:
                    try:
                        await maybe_await(clear_news())
                        stats.namespaces_cleared.append("local:news")
                        logger.info("Local cleared: news_intelligence cache")
                    except Exception as e:
                        stats.errors.append(f"news_intelligence.clear_cache() failed: {e}")

        # 3) Redis distributed cleanup
        if redis is not None and not dry_run:
            with TraceContext("redis_distributed_cleanup"):
                # v4.1.0: gate FLUSHDB, otherwise expand to all known patterns.
                if all_caches and not flushdb_allowed:
                    logger.warning(
                        "--all passed WITHOUT FLUSHDB confirmation. "
                        "Expanding to known namespace patterns instead. "
                        "To FLUSHDB, pass --confirm-flushdb or set "
                        "CLEANUP_ALLOW_FLUSHDB=true."
                    )

                patterns = _expand_patterns(namespaces, all_caches, flushdb_allowed)

                for pat in patterns:
                    if pat == "*":
                        # FLUSHDB — destructive; DO NOT retry
                        try:
                            await redis.flushdb()
                            stats.namespaces_cleared.append("redis:ALL")
                            stats.flushdb_executed = True
                            logger.info("Redis FLUSHDB executed.")
                        except Exception as e:
                            stats.errors.append(f"Redis FLUSHDB error: {e}")
                        break
                    else:
                        try:
                            deleted = await backoff.execute_async(clear_redis_namespace, redis, pat)
                            stats.keys_deleted += int(deleted)
                            stats.namespaces_cleared.append(f"redis:{pat}")
                            logger.info("Redis pattern=%s deleted=%s", pat, deleted)
                        except Exception as e:
                            stats.errors.append(f"Redis pattern={pat} error: {e}")

        # 4) Disk cleanup
        if not dry_run:
            with TraceContext("disk_cleanup"):
                freed = await clear_local_disk_cache()
                stats.disk_freed_mb += freed
                if freed > 0:
                    base = os.getenv("CLEANUP_DISK_CACHE_DIR", "/tmp").strip() or "/tmp"
                    glob_pat = os.getenv("CLEANUP_DISK_CACHE_GLOB", "cache_*").strip() or "cache_*"
                    stats.namespaces_cleared.append(f"disk:{base}/{glob_pat}")

        # 5) GC
        if not dry_run:
            gc.collect()

    finally:
        # v4.1.0: Redis close ALWAYS runs — even on mid-run exceptions.
        if redis is not None:
            await _close_redis(redis)

    stats.end_time = time.time()
    return stats


# =============================================================================
# Startup diagnostics
# =============================================================================
def _log_startup_diagnostics() -> None:
    avail = _component_availability()
    ok_count = sum(1 for v in avail.values() if v["available"])
    total = len(avail)
    logger.info(
        "Cleanup v%s | orjson=%s | redis_lib=%s | otel=%s | tracing=%s | components=%d/%d",
        SCRIPT_VERSION,
        _HAS_ORJSON,
        _REDIS_AVAILABLE,
        _OTEL_AVAILABLE,
        _TRACING_ENABLED,
        ok_count,
        total,
    )
    for name, info in avail.items():
        if info["available"]:
            logger.info("  [OK]   %s", name)
        else:
            logger.info("  [MISS] %s  (%s)", name, info["error"])


def main() -> int:
    parser = argparse.ArgumentParser(
        description=f"Tadawul Distributed Cache Cleanup v{SCRIPT_VERSION}"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Clear ALL known namespaces. Without --confirm-flushdb, "
             "expands to every known pattern (safe). With --confirm-flushdb, "
             "triggers Redis FLUSHDB (irreversible).",
    )
    parser.add_argument(
        "--confirm-flushdb",
        action="store_true",
        help="Authorize Redis FLUSHDB when --all is used. "
             "(Can also be enabled via env CLEANUP_ALLOW_FLUSHDB=true.)",
    )
    parser.add_argument(
        "--namespaces",
        nargs="*",
        default=["quotes", "history", "ml", "news"],
        help="Specific namespaces to clear (default: quotes history ml news). "
             "Valid: " + ", ".join(sorted(_NAMESPACE_PATTERNS.keys())),
    )
    parser.add_argument("--dry-run", action="store_true", help="Report without clearing")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    parser.add_argument(
        "--json-indent",
        type=int,
        default=2,
        help="JSON indentation (default 2; 0 = compact)",
    )

    args = parser.parse_args()

    _log_startup_diagnostics()

    if args.dry_run:
        scope = "ALL" if args.all else ", ".join(args.namespaces)
        flushdb_mode = (
            "FLUSHDB (confirmed)"
            if (args.all and (args.confirm_flushdb or _env_bool("CLEANUP_ALLOW_FLUSHDB", False)))
            else "pattern-based (safe)"
        )
        logger.info("DRY RUN: scope=%s | redis_mode=%s", scope, flushdb_mode)
        return 0

    try:
        stats = asyncio.run(
            run_cleanup(
                namespaces=args.namespaces,
                all_caches=args.all,
                dry_run=args.dry_run,
                confirm_flushdb=args.confirm_flushdb,
            )
        )

        if args.json:
            print(json_dumps(stats.to_dict(), indent=int(args.json_indent)))
        else:
            logger.info("=" * 60)
            logger.info("CACHE CLEANUP COMPLETE (v%s)", SCRIPT_VERSION)
            logger.info("=" * 60)
            logger.info("Duration:         %.2fs", stats.duration_sec)
            logger.info("Redis Connected:  %s", stats.redis_connected)
            logger.info("FLUSHDB Allowed:  %s", stats.flushdb_allowed)
            logger.info("FLUSHDB Executed: %s", stats.flushdb_executed)
            logger.info("Redis Keys:       %s", stats.keys_deleted)
            logger.info("Disk Freed:       %.2f MB", stats.disk_freed_mb)
            logger.info(
                "Namespaces:       %s",
                ", ".join(stats.namespaces_cleared) if stats.namespaces_cleared else "None",
            )
            if stats.errors:
                logger.warning("Errors:           %d", len(stats.errors))
                for err in stats.errors:
                    logger.error("  - %s", err)
            logger.info("=" * 60)

        return 1 if stats.errors else 0

    except KeyboardInterrupt:
        logger.info("Cleanup interrupted by user.")
        return 130
    except Exception as e:
        logger.exception("Fatal error during cleanup: %s", e)
        return 1


__all__ = [
    "SCRIPT_VERSION",
    "SERVICE_VERSION",
    "CleanupStats",
    "FullJitterBackoff",
    "TraceContext",
    "maybe_await",
    "clear_redis_namespace",
    "clear_local_disk_cache",
    "run_cleanup",
    "main",
]


if __name__ == "__main__":
    sys.exit(main())
