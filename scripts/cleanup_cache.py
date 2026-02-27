#!/usr/bin/env python3
"""
scripts/cleanup_cache.py
===========================================================
TADAWUL FAST BRIDGE – DISTRIBUTED CACHE CLEANUP UTILITY (v4.0.1)
===========================================================
STABLE EDITION | SAFE ASYNC | CRON FRIENDLY

Fixes vs v4.0.0 draft:
- ✅ Import missing Path
- ✅ Correct OpenTelemetry span usage (start_as_current_span context manager)
- ✅ Prefer redis.asyncio (modern redis-py) instead of deprecated aioredis
- ✅ Safe "maybe_await" wrapper (core clear funcs can be sync or async)
- ✅ Robust Redis close/disconnect
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import logging
import os
import random
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

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
# Optional Redis (modern)
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
# Optional OpenTelemetry (fixed usage)
# ---------------------------------------------------------------------------
_TRACING_ENABLED = os.getenv("CORE_TRACING_ENABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}

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
# Try to import core components (optional)
# ---------------------------------------------------------------------------
try:
    from core.data_engine_v2 import get_cache as get_v2_cache
    from core.providers.yahoo_chart_provider import clear_cache as clear_yahoo
    from core.symbols_reader import _reader as symbols_reader_instance
    from core.news_intelligence import clear_cache as clear_news

    _CORE_AVAILABLE = True
except Exception:
    _CORE_AVAILABLE = False

# =============================================================================
# Configuration & Logging
# =============================================================================

SCRIPT_VERSION = "4.0.1"

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
    """If x is awaitable -> await it; else return as-is."""
    if asyncio.iscoroutine(x) or hasattr(x, "__await__"):
        return await x
    return x


class TraceContext:
    """
    Correct OTEL handling:
    tracer.start_as_current_span() returns a *context manager*.
    We must enter it to get a span (if any).
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
            "disk_freed_mb": round(self.disk_freed_mb, 2),
            "errors": self.errors,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


class FullJitterBackoff:
    """AWS-style Full Jitter Backoff for async ops."""
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
                # delete is variadic
                deleted += await redis.delete(*keys)
            if cursor == 0:
                break
    except Exception as e:
        logger.error("Redis clear failed pattern=%s err=%s", pattern, e)
    return int(deleted)


async def _close_redis(redis: Redis) -> None:
    try:
        await redis.close()
    except Exception:
        pass
    try:
        pool = getattr(redis, "connection_pool", None)
        if pool and hasattr(pool, "disconnect"):
            await pool.disconnect(inuse_connections=True)
    except Exception:
        pass


# =============================================================================
# Disk cleanup
# =============================================================================

async def clear_local_disk_cache() -> float:
    """Clear local /tmp cache directories matching cache_*."""
    freed_mb = 0.0
    tmp_dir = Path("/tmp")
    if not tmp_dir.exists():
        return 0.0

    for item in tmp_dir.glob("cache_*"):
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

async def run_cleanup(namespaces: List[str], all_caches: bool = False, dry_run: bool = False) -> CleanupStats:
    stats = CleanupStats()
    backoff = FullJitterBackoff()

    # 1) Redis connection
    redis = await _get_redis()
    if redis:
        stats.redis_connected = True
        logger.info("Connected to Redis.")

    # 2) Local memory cleanup (best-effort)
    if _CORE_AVAILABLE and not dry_run:
        with TraceContext("local_memory_cleanup"):
            try:
                if all_caches or "quotes" in namespaces:
                    cache = get_v2_cache()
                    await maybe_await(getattr(cache, "clear")())
                    stats.namespaces_cleared.append("local:data_engine_v2")

                if all_caches or "yahoo" in namespaces:
                    await maybe_await(clear_yahoo())
                    stats.namespaces_cleared.append("local:yahoo_chart")

                if all_caches or "symbols" in namespaces:
                    try:
                        with symbols_reader_instance._lock:
                            symbols_reader_instance._cache.clear()
                        stats.namespaces_cleared.append("local:symbols")
                    except Exception as e:
                        stats.errors.append(f"Symbols cache clear error: {e}")

                if all_caches or "news" in namespaces:
                    await maybe_await(clear_news())
                    stats.namespaces_cleared.append("local:news")

            except Exception as e:
                stats.errors.append(f"Local cache error: {e}")

    # 3) Redis distributed cleanup
    if redis and not dry_run:
        with TraceContext("redis_distributed_cleanup"):
            patterns: List[str] = []

            if all_caches:
                patterns = ["*"]  # dangerous, but user requested with --all
            else:
                for ns in namespaces:
                    ns = (ns or "").strip().lower()
                    if ns == "quotes":
                        patterns.extend(["data_engine:*", "quote:*"])
                    elif ns == "history":
                        patterns.append("history:*")
                    elif ns == "yahoo":
                        patterns.append("yahoo:*")
                    elif ns == "symbols":
                        patterns.append("symbols:*")
                    elif ns == "news":
                        patterns.append("news:*")
                    elif ns == "argaam":
                        patterns.append("argaam:*")
                    elif ns == "ml":
                        patterns.append("ml_pred:*")

            for pat in patterns:
                if pat == "*":
                    try:
                        await backoff.execute_async(redis.flushdb)
                        stats.namespaces_cleared.append("redis:ALL")
                        logger.info("Redis FLUSHDB executed.")
                    except Exception as e:
                        stats.errors.append(f"Redis FLUSHDB error: {e}")
                    break
                else:
                    deleted = await backoff.execute_async(clear_redis_namespace, redis, pat)
                    stats.keys_deleted += int(deleted)
                    stats.namespaces_cleared.append(f"redis:{pat}")
                    logger.info("Redis pattern=%s deleted=%s", pat, deleted)

    # 4) Disk cleanup
    if not dry_run:
        with TraceContext("disk_cleanup"):
            freed = await clear_local_disk_cache()
            stats.disk_freed_mb += freed
            if freed > 0:
                stats.namespaces_cleared.append("disk:/tmp/cache_*")

    # 5) GC
    if not dry_run:
        gc.collect()

    if redis:
        await _close_redis(redis)

    stats.end_time = time.time()
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=f"Tadawul Distributed Cache Cleanup v{SCRIPT_VERSION}")
    parser.add_argument("--all", action="store_true", help="Clear ALL caches (Redis FLUSHDB + Local Memory)")
    parser.add_argument("--namespaces", nargs="*", default=["quotes", "history", "ml", "news"], help="Specific namespaces to clear")
    parser.add_argument("--dry-run", action="store_true", help="Report what would be done without clearing")
    parser.add_argument("--json", action="store_true", help="Output results in JSON format")

    args = parser.parse_args()

    if args.dry_run:
        logger.info("DRY RUN: Would clear namespaces: %s", ("ALL" if args.all else args.namespaces))
        return 0

    try:
        stats = asyncio.run(run_cleanup(namespaces=args.namespaces, all_caches=args.all, dry_run=args.dry_run))

        if args.json:
            print(json_dumps(stats.to_dict(), indent=2))
        else:
            logger.info("=" * 60)
            logger.info("CACHE CLEANUP COMPLETE (v%s)", SCRIPT_VERSION)
            logger.info("=" * 60)
            logger.info("Duration:     %.2fs", stats.duration_sec)
            logger.info("Redis Keys:   %s", stats.keys_deleted)
            logger.info("Disk Freed:   %.2f MB", stats.disk_freed_mb)
            logger.info("Namespaces:   %s", ", ".join(stats.namespaces_cleared) if stats.namespaces_cleared else "None")
            if stats.errors:
                logger.warning("Errors:       %s", len(stats.errors))
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


if __name__ == "__main__":
    sys.exit(main())
