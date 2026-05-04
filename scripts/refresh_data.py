#!/usr/bin/env python3
# scripts/refresh_data.py
"""
================================================================================
TADAWUL FAST BRIDGE - DISTRIBUTED DATA REFRESHER (v5.2.0)
================================================================================
QUANTUM EDITION | ASYNCIO BATCHING | SAFE OTEL | ENGINE-AWARE REFRESH

WHY v5.2.0
----------
[CRITICAL FIX] `--refresh` (default on) had NO effect on the engine.

The data engine's signature is:

    async def get_enriched_quote(self, symbol, use_cache=True, *,
                                 schema=None, page="", sheet="",
                                 body=None, **kwargs)

It expects `use_cache=False` to bypass the cache, NOT `refresh=True`.
Because of the `**kwargs` catch-all, every call this script made with
`refresh=refresh` was silently absorbed — no TypeError, no warning, no
cache bypass. The script effectively ran in "warm cache" mode at all
times, even when the operator explicitly asked for a force-refresh.
v5.2.0 sends `use_cache=not refresh` first (current engine), falls back
to `refresh=refresh` (older engines), then to bare positional.

[NEW] `--page` / `--sheet` CLI flag

The engine's provider routing is page-aware. For non-KSA pages
(Global_Markets, Commodities_FX, Mutual_Funds), EODHD is preferred;
for KSA, Tadawul/Argaam come first. Without page context, the engine
falls back to default global routing — which is wrong when warming a
specific page. v5.2.0 threads `--page` through to every engine call.

[CLEANUP] Removed unused `_CPU_EXECUTOR`

A module-level `ThreadPoolExecutor(max_workers=2)` was created at
import time but never used (comment confirmed: "kept but not used by
default"). Removed: the import-time side effect leaks threads when
this module is imported, and there's no production path that uses it.

[CHANGE] Default symbol set is now a cross-section

v5.1.0's defaults were KSA-only (2222.SR, 1120.SR, 2010.SR, 1180.SR,
2350.SR), which only exercised the KSA provider chain. v5.2.0 mixes
KSA, US equities, and Commodities/FX so a default invocation also
warms the EODHD/Yahoo paths.

[NEW] Differentiated exit codes
   0   - all symbols refreshed
   1   - partial failures
   2   - engine unavailable / import failed
   130 - interrupted (SIGINT)

[NOTE] Audit ticket "duplicate-row append bug" does not match this file

This script is read-only against the engine — it calls quote APIs and
records stats, never appends rows to any sheet. If duplicate rows are
showing up in the spreadsheet, the source is downstream of this file.
Likely candidates worth checking next:
  - integrations/google_sheets_service.py (the sheet writer)
  - any cron / scheduler that invokes the writer
  - schema_registry's sheet-key dedup
This audit-correction is intentional; flagging it here so the audit
trail stays accurate.

WHY v5.1.0 (preserved)
----------------------
- Fix OTEL bug: start_as_current_span() returns a context manager.
- Engine-method compatibility matrix and per-symbol timeout + jitter retry.
- Batch fallback: when batch fails wholesale, retry per-symbol so a
  single flaky symbol doesn't poison its 8-symbol cohort.

Core capabilities
-----------------
- Trigger background refresh / warm of market data across symbols.
- Force cache bypass (now actually working) when requested.
- Safe rate-limited concurrency with robust fallbacks.
================================================================================
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

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
    Safe OTEL wrapper.

    `tracer.start_as_current_span()` returns a context manager (not the
    span). v5.1.0 fixed the bug where the script tried to call methods
    on the CM thinking it was a span. v5.2.0 keeps that fix verbatim.
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

SCRIPT_VERSION = "5.2.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("DataRefresh")


# Exit code constants (v5.2.0)
EXIT_OK = 0
EXIT_PARTIAL = 1
EXIT_ENGINE_UNAVAILABLE = 2
EXIT_INTERRUPTED = 130


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
    page_context: str = ""

    @property
    def duration_sec(self) -> float:
        return time.time() - self.start_time

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": SCRIPT_VERSION,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "page_context": self.page_context or None,
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
    """Trim, upper-case, dedup while preserving order."""
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


async def _call_engine_single(engine: Any, symbol: str, *, refresh: bool, page: str = "") -> Any:
    """
    Try the best available single-symbol method.

    v5.2.0: sends `use_cache=not refresh` first (matches data_engine_v2
    v5.47+ contract), falls back to `refresh=refresh` for older engines,
    then to a bare positional call. Threads `page` through when accepted.
    Without this fix, `refresh=True` was silently absorbed by the
    engine's `**kwargs` and the cache was never bypassed.
    """
    use_cache = not bool(refresh)

    for name in ("get_enriched_quote", "get_quote"):
        fn = getattr(engine, name, None)
        if not callable(fn):
            continue

        # Modern path: use_cache + page (v5.47+)
        if page:
            try:
                return await fn(symbol, use_cache=use_cache, page=page)
            except TypeError:
                pass
        try:
            return await fn(symbol, use_cache=use_cache)
        except TypeError:
            pass

        # Legacy path: pre-v5.47 engines used `refresh=` directly
        if page:
            try:
                return await fn(symbol, refresh=refresh, page=page)
            except TypeError:
                pass
        try:
            return await fn(symbol, refresh=refresh)
        except TypeError:
            pass

        # Last resort: positional only
        return await fn(symbol)

    raise RuntimeError("No single-symbol quote method available on engine")


async def _call_engine_batch(engine: Any, symbols: List[str], *, refresh: bool, page: str = "") -> Any:
    """
    Try the best available batch method.

    Same use_cache vs refresh fallback chain as `_call_engine_single`.
    """
    use_cache = not bool(refresh)

    for name in ("get_enriched_quotes_batch", "get_enriched_quotes", "get_quotes_batch"):
        fn = getattr(engine, name, None)
        if not callable(fn):
            continue

        if page:
            try:
                return await fn(symbols, use_cache=use_cache, page=page)
            except TypeError:
                pass
        try:
            return await fn(symbols, use_cache=use_cache)
        except TypeError:
            pass

        if page:
            try:
                return await fn(symbols, refresh=refresh, page=page)
            except TypeError:
                pass
        try:
            return await fn(symbols, refresh=refresh)
        except TypeError:
            pass

        return await fn(symbols)

    raise RuntimeError("No batch quote method available on engine")


async def _refresh_symbol(
    engine: Any,
    symbol: str,
    backoff: FullJitterBackoff,
    *,
    timeout_sec: float,
    refresh: bool,
    page: str = "",
) -> Tuple[bool, str]:
    """Refresh one symbol with timeout + retry."""

    async def _do() -> Any:
        return await _call_engine_single(engine, symbol, refresh=refresh, page=page)

    with TraceContext("refresh_symbol", {"symbol": symbol, "refresh": refresh, "page": page or ""}):
        try:
            await asyncio.wait_for(backoff.execute_async(_do), timeout=timeout_sec)
            return True, "ok"
        except asyncio.TimeoutError:
            return False, "timeout"
        except Exception as e:
            return False, f"error:{type(e).__name__}:{e}"


async def _refresh_batch(
    engine: Any,
    symbols: List[str],
    backoff: FullJitterBackoff,
    *,
    timeout_sec: float,
    refresh: bool,
    page: str = "",
) -> Tuple[bool, str]:
    """Refresh a batch with timeout + retry (used when batch mode enabled)."""

    async def _do() -> Any:
        return await _call_engine_batch(engine, symbols, refresh=refresh, page=page)

    with TraceContext("refresh_batch", {"count": len(symbols), "refresh": refresh, "page": page or ""}):
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
    page: str = "",
) -> RefreshStats:
    """
    Drive a refresh pass against the engine.

    Args:
        symbols: List of tickers (will be deduplicated and uppercased).
        concurrency: Async semaphore size.
        timeout_sec: Per-call timeout in seconds.
        refresh: When True, instructs the engine to bypass its cache
            (`use_cache=False`). When False, allows engine TTL behaviour.
        batch_size: When > 1, calls the engine's batch APIs in chunks of
            this size. When 0 or 1, calls one symbol at a time.
        page: Page context to thread through to the engine (e.g.
            "Global_Markets"). Affects provider routing — non-KSA pages
            prefer EODHD, KSA pages prefer Tadawul/Argaam.
    """
    symbols = _coerce_symbols(symbols)
    stats = RefreshStats(total=len(symbols), page_context=page)

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
            ok, msg = await _refresh_symbol(
                engine, sym, backoff,
                timeout_sec=timeout_sec, refresh=refresh, page=page,
            )
            stats.per_symbol[sym] = msg
            if ok:
                stats.success += 1
            else:
                stats.failed += 1

    async def _do_batch(batch: List[str]) -> None:
        async with sem:
            ok, msg = await _refresh_batch(
                engine, batch, backoff,
                timeout_sec=max(timeout_sec, 45.0), refresh=refresh, page=page,
            )
            if ok:
                stats.success += len(batch)
                for s in batch:
                    stats.per_symbol[s] = "ok(batch)"
                return

            # Batch failed wholesale. Don't punish every symbol equally —
            # retry each one individually so a single flaky symbol doesn't
            # take down its 8-symbol cohort. (v5.1.0 behaviour preserved.)
            logger.warning(f"Batch failed ({msg}); falling back to per-symbol for {len(batch)} symbols")
            per_results = await asyncio.gather(
                *[
                    _refresh_symbol(
                        engine, s, backoff,
                        timeout_sec=timeout_sec, refresh=refresh, page=page,
                    )
                    for s in batch
                ],
                return_exceptions=True,
            )
            for sym, res in zip(batch, per_results):
                if isinstance(res, tuple) and len(res) == 2:
                    sym_ok, sym_msg = res
                    if sym_ok:
                        stats.success += 1
                        stats.per_symbol[sym] = "ok(batch_fallback_single)"
                    else:
                        stats.failed += 1
                        stats.per_symbol[sym] = f"batch_fail->single_fail:{sym_msg}"
                else:
                    stats.failed += 1
                    stats.per_symbol[sym] = f"batch_fail->{type(res).__name__}"

    logger.info(
        f"Starting refresh: total={len(symbols)} concurrency={concurrency} refresh={refresh} "
        f"timeout={timeout_sec}s batch_size={batch_size or 0} page={page or '-'}"
    )

    if batch_size and batch_size > 1:
        batches = [symbols[i: i + batch_size] for i in range(0, len(symbols), batch_size)]
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

# v5.2.0: cross-section default (was KSA-only in v5.1.0). Mixes Saudi
# blue chips, US large caps, and a commodity/FX pair so the default
# invocation also exercises the EODHD / Yahoo provider chains.
_DEFAULT_SYMBOLS: List[str] = [
    "2222.SR", "1120.SR", "2010.SR",   # KSA: Aramco, Al Rajhi, SABIC
    "AAPL", "MSFT", "NVDA",            # US large cap
    "GC=F", "EURUSD=X",                # Commodity, FX
]


async def main_async(args: argparse.Namespace) -> int:
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        symbols = list(_DEFAULT_SYMBOLS)

    page_ctx = (args.page or args.sheet or "").strip()

    stats = await run_refresh(
        symbols,
        concurrency=args.workers,
        timeout_sec=args.timeout,
        refresh=not args.no_refresh,
        batch_size=args.batch_size,
        page=page_ctx,
    )

    if args.json:
        print(json_dumps(stats.to_dict(), indent=2))
    else:
        logger.info("=" * 60)
        logger.info(f"REFRESH COMPLETE (v{SCRIPT_VERSION})")
        logger.info("=" * 60)
        logger.info(f"Page:     {page_ctx or '-'}")
        logger.info(f"Duration: {stats.duration_sec:.2f}s")
        logger.info(f"Total:    {stats.total}")
        logger.info(f"Success:  {stats.success}")
        logger.info(f"Failed:   {stats.failed}")
        if stats.errors:
            logger.warning("Errors:")
            for e in stats.errors:
                logger.warning(f" - {e}")
        logger.info("=" * 60)

    # v5.2.0: differentiated exit codes
    if not _ENGINE_AVAILABLE:
        return EXIT_ENGINE_UNAVAILABLE
    if stats.failed > 0:
        return EXIT_PARTIAL
    return EXIT_OK


def main() -> None:
    parser = argparse.ArgumentParser(description=f"Tadawul Distributed Data Refresher v{SCRIPT_VERSION}")
    parser.add_argument("--symbols", type=str, default="",
                        help="Comma-separated list of symbols to refresh")
    parser.add_argument("--workers", type=int, default=10,
                        help="Async concurrency")
    parser.add_argument("--timeout", type=float, default=25.0,
                        help="Per-symbol timeout seconds")
    parser.add_argument("--batch-size", type=int, default=0,
                        help="If >1, uses engine batch APIs with this batch size")
    parser.add_argument("--no-refresh", action="store_true",
                        help="Do not force refresh (let engine use cache)")
    parser.add_argument("--page", type=str, default="",
                        help="v5.2.0: page context to pass to engine "
                             "(e.g. Global_Markets, Commodities_FX, Mutual_Funds, "
                             "Market_Leaders). Affects provider routing.")
    parser.add_argument("--sheet", type=str, default="",
                        help="Alias for --page")
    parser.add_argument("--json", action="store_true",
                        help="Print JSON output")

    args = parser.parse_args()

    try:
        sys.exit(asyncio.run(main_async(args)))
    except KeyboardInterrupt:
        logger.info("Refresh interrupted by user.")
        sys.exit(EXIT_INTERRUPTED)


if __name__ == "__main__":
    main()
