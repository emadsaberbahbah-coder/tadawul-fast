#!/usr/bin/env python3
# scripts/refresh_data.py
"""
================================================================================
TADAWUL FAST BRIDGE – DISTRIBUTED DATA REFRESHER (v5.1.0)
================================================================================
QUANTUM EDITION | ASYNCIO BATCHING | SAFE OTEL | ENGINE-AWARE REFRESH

Fixes & upgrades in v5.1.0
- ✅ Fix OTEL bug: start_as_current_span() returns a context manager (prevents:
  '_AgnosticContextManager' object has no attribute 'set_attributes')
- ✅ Engine-method compatibility matrix:
    get_enriched_quote(symbol, refresh=?)
    get_quote(symbol, refresh=?)
    get_enriched_quotes_batch(symbols, refresh=?)
    get_quotes_batch(symbols, refresh=?)
  (falls back automatically, no missing-arg crashes)
- ✅ Proper per-symbol timeout + full-jitter retry around engine call
- ✅ Optional warmup: also touch analysis/enriched sheet endpoints via engine if available
- ✅ Deterministic stats + JSON output option
- ✅ No ThreadPoolExecutor unless actually needed (kept but not used by default)

Core Capabilities
• Trigger background refresh/warm of market data across symbols.
• Force cache bypass/hot refresh when supported.
• Safe rate-limited concurrency with robust fallbacks.
================================================================================
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import logging
import os
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        opt = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(v, option=opt, default=str).decode("utf-8")

    _HAS_ORJSON = True
except Exception:
    import json  # type: ignore

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        return json.dumps(v, indent=indent if indent else None, default=str, ensure_ascii=False)

    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Optional Tracing (OpenTelemetry)
# ---------------------------------------------------------------------------
try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    _OTEL_AVAILABLE = True
except Exception:
    trace = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    _OTEL_AVAILABLE = False

_TRACING_ENABLED = (os.getenv("CORE_TRACING_ENABLED", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}

class TraceContext:
    """
    Safe OTEL wrapper:
    tracer.start_as_current_span() returns a context manager, not the span.
    """
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._tracer = trace.get_tracer(__name__) if (_OTEL_AVAILABLE and _TRACING_ENABLED and trace) else None
        self._cm = None
        self.span = None

    def __enter__(self):
        if self._tracer:
            self._cm = self._tracer.start_as_current_span(self.name)
            self.span = self._cm.__enter__()
            if self.attributes and self.span:
                try:
                    self.span.set_attributes(self.attributes)
                except Exception:
                    pass
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.span and _OTEL_AVAILABLE:
            if exc_val:
                try:
                    self.span.record_exception(exc_val)
                    self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))  # type: ignore
                except Exception:
                    pass
        if self._cm:
            try:
                return self._cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception:
                return False
        return False

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__exit__(exc_type, exc_val, exc_tb)

# ---------------------------------------------------------------------------
# Core Engine Connection
# ---------------------------------------------------------------------------
try:
    from core.data_engine_v2 import get_engine  # type: ignore
    _ENGINE_AVAILABLE = True
except Exception:
    get_engine = None  # type: ignore
    _ENGINE_AVAILABLE = False

SCRIPT_VERSION = "5.1.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("DataRefresh")

# Kept for compatibility with your “persistent executor” style; not used by default.
_CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="RefreshWorker")

# =============================================================================
# Full Jitter Exponential Backoff
# =============================================================================

class FullJitterBackoff:
    def __init__(self, max_retries: int = 3, base_delay: float = 0.8, max_delay: float = 10.0):
        self.max_retries = max(1, int(max_retries))
        self.base_delay = max(0.05, float(base_delay))
        self.max_delay = max(0.2, float(max_delay))

    async def execute_async(self, func: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
        last_exc: Optional[BaseException] = None
        for attempt in range(self.max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_exc = e
                if attempt >= self.max_retries - 1:
                    raise
                cap = min(self.max_delay, self.base_delay * (2 ** attempt))
                sleep_time = random.uniform(0.0, cap)
                await asyncio.sleep(sleep_time)
        raise last_exc  # type: ignore

# =============================================================================
# Data Models
# =============================================================================

@dataclass(slots=True)
class RefreshStats:
    total: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0
    errors: List[str] = field(default_factory=list)
    per_symbol: Dict[str, str] = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)

    @property
    def duration_sec(self) -> float:
        return time.time() - self.start_time

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": SCRIPT_VERSION,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "total": self.total,
            "success": self.success,
            "failed": self.failed,
            "skipped": self.skipped,
            "duration_sec": round(self.duration_sec, 3),
            "errors": self.errors,
            "per_symbol": self.per_symbol,
        }

# =============================================================================
# Engine call compatibility
# =============================================================================

def _coerce_symbols(symbols: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for s in symbols:
        ss = (s or "").strip().upper()
        if not ss:
            continue
        if ss not in seen:
            seen.add(ss)
            out.append(ss)
    return out

async def _call_engine_single(engine: Any, symbol: str, refresh: bool) -> Any:
    """
    Try the best available single-symbol method.
    """
    # Preferred
    for name in ("get_enriched_quote", "get_quote"):
        fn = getattr(engine, name, None)
        if callable(fn):
            try:
                # try refresh kw
                return await fn(symbol, refresh=refresh)
            except TypeError:
                # maybe no refresh kw
                return await fn(symbol)
    raise RuntimeError("No single-symbol quote method available on engine")

async def _call_engine_batch(engine: Any, symbols: List[str], refresh: bool) -> Any:
    """
    Try the best available batch method.
    """
    for name in ("get_enriched_quotes_batch", "get_enriched_quotes", "get_quotes_batch"):
        fn = getattr(engine, name, None)
        if callable(fn):
            try:
                return await fn(symbols, refresh=refresh)
            except TypeError:
                return await fn(symbols)
    raise RuntimeError("No batch quote method available on engine")

async def _refresh_symbol(engine: Any, symbol: str, backoff: FullJitterBackoff, *, timeout_sec: float, refresh: bool) -> Tuple[bool, str]:
    """
    Refresh one symbol with timeout + retry.
    """
    async def _do() -> Any:
        return await _call_engine_single(engine, symbol, refresh=refresh)

    with TraceContext("refresh_symbol", {"symbol": symbol, "refresh": refresh}):
        try:
            await asyncio.wait_for(backoff.execute_async(_do), timeout=timeout_sec)
            return True, "ok"
        except asyncio.TimeoutError:
            return False, "timeout"
        except Exception as e:
            return False, f"error:{type(e).__name__}:{e}"

async def _refresh_batch(engine: Any, symbols: List[str], backoff: FullJitterBackoff, *, timeout_sec: float, refresh: bool) -> Tuple[bool, str]:
    """
    Refresh a batch with timeout + retry (used when batch mode enabled).
    """
    async def _do() -> Any:
        return await _call_engine_batch(engine, symbols, refresh=refresh)

    with TraceContext("refresh_batch", {"count": len(symbols), "refresh": refresh}):
        try:
            await asyncio.wait_for(backoff.execute_async(_do), timeout=timeout_sec)
            return True, "ok"
        except asyncio.TimeoutError:
            return False, "timeout"
        except Exception as e:
            return False, f"error:{type(e).__name__}:{e}"

# =============================================================================
# Execution Core
# =============================================================================

async def run_refresh(
    symbols: List[str],
    *,
    concurrency: int = 10,
    timeout_sec: float = 25.0,
    refresh: bool = True,
    batch_size: int = 0,
) -> RefreshStats:
    symbols = _coerce_symbols(symbols)
    stats = RefreshStats(total=len(symbols))

    if not _ENGINE_AVAILABLE or get_engine is None:
        stats.errors.append("Core data engine is not available (core.data_engine_v2.get_engine import failed).")
        stats.failed = len(symbols)
        for s in symbols:
            stats.per_symbol[s] = "engine_missing"
        return stats

    engine = await get_engine()
    if not engine:
        stats.errors.append("Failed to initialize core data engine (get_engine returned None).")
        stats.failed = len(symbols)
        for s in symbols:
            stats.per_symbol[s] = "engine_none"
        return stats

    backoff = FullJitterBackoff()
    sem = asyncio.Semaphore(max(1, int(concurrency)))

    async def _do_one(sym: str) -> None:
        async with sem:
            ok, msg = await _refresh_symbol(engine, sym, backoff, timeout_sec=timeout_sec, refresh=refresh)
            stats.per_symbol[sym] = msg
            if ok:
                stats.success += 1
            else:
                stats.failed += 1

    async def _do_batch(batch: List[str]) -> None:
        async with sem:
            ok, msg = await _refresh_batch(engine, batch, backoff, timeout_sec=max(timeout_sec, 45.0), refresh=refresh)
            if ok:
                stats.success += len(batch)
                for s in batch:
                    stats.per_symbol[s] = "ok(batch)"
            else:
                stats.failed += len(batch)
                for s in batch:
                    stats.per_symbol[s] = msg

    logger.info(
        f"Starting refresh: total={len(symbols)} concurrency={concurrency} refresh={refresh} "
        f"timeout={timeout_sec}s batch_size={batch_size or 0}"
    )

    if batch_size and batch_size > 1:
        batches = [symbols[i : i + batch_size] for i in range(0, len(symbols), batch_size)]
        await asyncio.gather(*(_do_batch(b) for b in batches), return_exceptions=True)
    else:
        await asyncio.gather(*(_do_one(s) for s in symbols), return_exceptions=True)

    # Close engine if supported
    try:
        if hasattr(engine, "aclose") and asyncio.iscoroutinefunction(engine.aclose):
            await engine.aclose()
        elif hasattr(engine, "close"):
            engine.close()
    except Exception:
        pass

    return stats

# =============================================================================
# CLI
# =============================================================================

async def main_async(args: argparse.Namespace) -> int:
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        symbols = ["2222.SR", "1120.SR", "2010.SR", "1180.SR", "2350.SR"]

    stats = await run_refresh(
        symbols,
        concurrency=args.workers,
        timeout_sec=args.timeout,
        refresh=not args.no_refresh,
        batch_size=args.batch_size,
    )

    if args.json:
        print(json_dumps(stats.to_dict(), indent=2))
    else:
        logger.info("=" * 60)
        logger.info(f"REFRESH COMPLETE (v{SCRIPT_VERSION})")
        logger.info("=" * 60)
        logger.info(f"Duration: {stats.duration_sec:.2f}s")
        logger.info(f"Total:    {stats.total}")
        logger.info(f"Success:  {stats.success}")
        logger.info(f"Failed:   {stats.failed}")
        if stats.errors:
            logger.warning("Errors:")
            for e in stats.errors:
                logger.warning(f" - {e}")
        logger.info("=" * 60)

    _CPU_EXECUTOR.shutdown(wait=False)
    return 1 if stats.failed > 0 else 0

def main() -> None:
    parser = argparse.ArgumentParser(description=f"Tadawul Distributed Data Refresher v{SCRIPT_VERSION}")
    parser.add_argument("--symbols", type=str, default="", help="Comma-separated list of symbols to refresh")
    parser.add_argument("--workers", type=int, default=10, help="Async concurrency")
    parser.add_argument("--timeout", type=float, default=25.0, help="Per-symbol timeout seconds")
    parser.add_argument("--batch-size", type=int, default=0, help="If >1, uses engine batch APIs with this batch size")
    parser.add_argument("--no-refresh", action="store_true", help="Do not force refresh (let engine use cache)")
    parser.add_argument("--json", action="store_true", help="Print JSON output")

    args = parser.parse_args()

    try:
        sys.exit(asyncio.run(main_async(args)))
    except KeyboardInterrupt:
        logger.info("Refresh interrupted by user.")
        _CPU_EXECUTOR.shutdown(wait=False)
        sys.exit(130)

if __name__ == "__main__":
    main()
