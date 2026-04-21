#!/usr/bin/env python3
# scripts/refresh_data.py
"""
================================================================================
TADAWUL FAST BRIDGE — DISTRIBUTED DATA REFRESHER (v5.2.0)
================================================================================
QUANTUM EDITION | ASYNCIO BATCHING | SAFE OTEL | ENGINE-AWARE REFRESH

Why this revision (v5.2.0 vs v5.1.0)
-------------------------------------
- 🔑 FIX CRITICAL: v5.1.0 passed `refresh=refresh` as a kwarg to engine
     methods. Per `core.data_engine (v6.x) __all__`, the canonical
     cache-control kwarg is `use_cache: bool`, NOT `refresh`. The instance
     method `DataEngineV5.get_enriched_quote(symbol, use_cache=True, *,
     schema, page, sheet, body, **kwargs)` has `**kwargs`, which SILENTLY
     swallows `refresh=True` — no TypeError, no effect. Result: v5.1.0's
     default "force refresh" behavior never actually bypassed the cache,
     AND `--no-refresh` was semantically identical to the default. The
     script's core value proposition was silently broken.
     v5.2.0 passes `use_cache=False` (when refresh requested) to the
     canonical API, which actually bypasses the engine cache.

- 🔑 FIX HIGH: Prefer canonical module-level API
     `core.data_engine.get_enriched_quote(symbol, use_cache, ttl)` /
     `core.data_engine.get_enriched_quotes(symbols, use_cache, ttl)`.
     These are listed in `core.data_engine.__all__` and handle engine
     discovery, caching, retries, and error wrapping internally. v5.1.0
     manually resolved the engine via `get_engine()` and called
     instance methods — a more fragile path.
     v5.2.0 falls back to engine-instance methods only if module-level
     API is unavailable.

- FIX: Engine module discovery falls back across `core.data_engine` and
     `core.data_engine_v2` — matches `main.py _maybe_init_engine` /
     `_maybe_close_engine` pattern.

- FIX: `_TRACING_ENABLED` vocabulary aligned with
     `main._TRUTHY` = {"1","true","yes","y","on","t","enabled","enable"}.
     New `_TRUTHY` / `_FALSY` module constants + `_env_bool` helper.

- FIX: `SCRIPT_VERSION` + `SERVICE_VERSION` alias (cross-script
     consistency with `audit_data_quality.py v4.5.0` / `cleanup_cache.py
     v4.1.0` / `drift_detection.py v4.3.0` / `migrate_schema_v2.py v2.1.0`).

- FIX: `_CPU_EXECUTOR.shutdown()` wrapped in try/finally in `main_async`
     AND `main`. Leak-proof on all exception paths. Idempotent via new
     `_shutdown_executor(wait)` helper matching project convention.

- FIX: CLI flags now accept env var defaults (`REFRESH_SYMBOLS`,
     `REFRESH_CONCURRENCY`, `REFRESH_TIMEOUT`, `REFRESH_BATCH_SIZE`,
     `REFRESH_NO_REFRESH`, `REFRESH_JSON`) for headless/cron operation.

- KEEP: All CLI flags (`--symbols`, `--workers`, `--timeout`,
     `--batch-size`, `--no-refresh`, `--json`). OTEL TraceContext with
     the v4.1.0 `start_as_current_span` context-manager fix.
     FullJitterBackoff. RefreshStats dataclass. Per-symbol + batch modes.
     Default demo symbols.

Core Capabilities
-----------------
• Trigger background refresh/warm of market data across symbols.
• Force cache bypass (use_cache=False) when --refresh is implicit (default)
  or `--no-refresh` for cache-friendly warmup.
• Safe rate-limited concurrency with robust fallbacks.

Env vars
--------
  REFRESH_SYMBOLS          comma-separated list of symbols
  REFRESH_CONCURRENCY      async concurrency (default 10)
  REFRESH_TIMEOUT          per-symbol timeout seconds (default 25.0)
  REFRESH_BATCH_SIZE       use batch API with this batch size (default 0 = per-symbol)
  REFRESH_NO_REFRESH       if truthy, let engine use cache
  REFRESH_JSON             if truthy, print JSON output
  CORE_TRACING_ENABLED     enable OTEL spans

Usage
-----
  python refresh_data.py --symbols 2222.SR,1120.SR --workers 20
  python refresh_data.py --batch-size 50 --json
  REFRESH_SYMBOLS=2222.SR,1120.SR python refresh_data.py

Exit codes
----------
  0 = all symbols refreshed successfully
  1 = at least one symbol failed
  130 = interrupted by user (SIGINT)
================================================================================
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import importlib
import inspect
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
        return json.dumps(
            v, indent=indent if indent else None, default=str, ensure_ascii=False
        )

    _HAS_ORJSON = False


# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
SCRIPT_VERSION = "5.2.0"
SERVICE_VERSION = SCRIPT_VERSION  # v5.2.0: cross-script alias


# ---------------------------------------------------------------------------
# Project-wide truthy/falsy vocabulary (matches main._TRUTHY / _FALSY)
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


def _env_int(name: str, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        raw = (os.getenv(name, "") or "").strip()
        if not raw:
            return default
        v = int(float(raw))
    except Exception:
        return default
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


def _env_float(name: str, default: float, *, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        raw = (os.getenv(name, "") or "").strip()
        if not raw:
            return default
        v = float(raw)
    except Exception:
        return default
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


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

_TRACING_ENABLED = _env_bool("CORE_TRACING_ENABLED", False)


class TraceContext:
    """
    Safe OTEL wrapper.

    tracer.start_as_current_span() returns a context manager; we must
    enter it to obtain the span instance (the v4.1.0 fix pattern).
    """

    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._tracer = (
            trace.get_tracer(__name__)
            if (_OTEL_AVAILABLE and _TRACING_ENABLED and trace)
            else None
        )
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


# =============================================================================
# Core Engine Connection (v5.2.0: canonical module-level API + fallback)
# =============================================================================
# Primary path: core.data_engine module-level functions (v6.x canonical
# public API per __all__). Resilient — handles engine init internally.
#
# Fallback path: engine instance from core.data_engine_v2 or core.data_engine
# and dispatch to instance methods. Matches main.py's engine discovery order.

@dataclass(slots=True)
class _EngineAPI:
    """Bundled references to the canonical quote-fetching API."""
    # Module-level functions (preferred)
    module_path: str = ""
    get_enriched_quote: Optional[Callable[..., Awaitable[Any]]] = None
    get_enriched_quotes: Optional[Callable[..., Awaitable[Any]]] = None
    close_engine: Optional[Callable[..., Awaitable[None]]] = None

    # Engine instance fallback
    get_engine: Optional[Callable[..., Awaitable[Any]]] = None

    @property
    def has_module_api(self) -> bool:
        return callable(self.get_enriched_quote) and callable(self.get_enriched_quotes)


def _discover_engine_api() -> _EngineAPI:
    """
    Probe in priority order:
      1. core.data_engine — exports module-level get_enriched_quote(s) per __all__
      2. core.data_engine_v2 — engine instance path (DataEngineV5)
    Returns an _EngineAPI with whichever primitives were found.
    """
    api = _EngineAPI()

    # Try core.data_engine first (public module-level API)
    try:
        de = importlib.import_module("core.data_engine")
        geq = getattr(de, "get_enriched_quote", None)
        geqs = getattr(de, "get_enriched_quotes", None)
        if callable(geq) and callable(geqs):
            api.module_path = "core.data_engine"
            api.get_enriched_quote = geq
            api.get_enriched_quotes = geqs
            api.close_engine = getattr(de, "close_engine", None)
            api.get_engine = getattr(de, "get_engine", None)
            return api
        # data_engine module exists but without module-level quote API
        # keep get_engine for fallback path
        api.get_engine = getattr(de, "get_engine", None)
        api.close_engine = getattr(de, "close_engine", None)
    except Exception:
        pass

    # Fallback: core.data_engine_v2 engine instance path
    try:
        de2 = importlib.import_module("core.data_engine_v2")
        ge = getattr(de2, "get_engine", None)
        if callable(ge):
            api.get_engine = api.get_engine or ge
            api.close_engine = api.close_engine or getattr(de2, "close_engine", None)
            if not api.module_path:
                api.module_path = "core.data_engine_v2"
    except Exception:
        pass

    return api


_ENGINE_API = _discover_engine_api()
_ENGINE_AVAILABLE = bool(_ENGINE_API.has_module_api or callable(_ENGINE_API.get_engine))


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("DataRefresh")

# Kept for compatibility with project's "persistent executor" pattern; not
# used by default (refresh ops are all asyncio-native).
_CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=2, thread_name_prefix="RefreshWorker"
)


def _shutdown_executor(wait: bool = False) -> None:
    """Idempotent shutdown helper. Safe to call multiple times."""
    try:
        _CPU_EXECUTOR.shutdown(wait=wait, cancel_futures=True)
    except TypeError:
        # python < 3.9 fallback
        try:
            _CPU_EXECUTOR.shutdown(wait=wait)
        except Exception:
            pass
    except Exception:
        pass


# =============================================================================
# Full Jitter Exponential Backoff
# =============================================================================
class FullJitterBackoff:
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 0.8,
        max_delay: float = 10.0,
    ):
        self.max_retries = max(1, int(max_retries))
        self.base_delay = max(0.05, float(base_delay))
        self.max_delay = max(0.2, float(max_delay))

    async def execute_async(
        self, func: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any
    ) -> Any:
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
    engine_source: str = ""
    use_cache: bool = True  # v5.2.0: records the actual cache mode used

    @property
    def duration_sec(self) -> float:
        return time.time() - self.start_time

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": SCRIPT_VERSION,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "engine_source": self.engine_source,
            "use_cache": self.use_cache,
            "total": self.total,
            "success": self.success,
            "failed": self.failed,
            "skipped": self.skipped,
            "duration_sec": round(self.duration_sec, 3),
            "errors": self.errors,
            "per_symbol": self.per_symbol,
        }


# =============================================================================
# Engine call helpers (v5.2.0: use_cache, NOT refresh)
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


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


async def _call_engine_single(
    engine: Any,
    symbol: str,
    *,
    use_cache: bool,
) -> Any:
    """
    Single-symbol quote fetch with canonical signature.

    v5.2.0: The cache-control kwarg is `use_cache: bool` (canonical per
    core.data_engine __all__). Previously v5.1.0 passed `refresh=` which
    was silently swallowed by instance-method **kwargs.

    Priority:
      1. Module-level `core.data_engine.get_enriched_quote(symbol, use_cache, ttl)`
      2. Engine instance `.get_enriched_quote(symbol, use_cache=...)` or fallback
    """
    # Primary: canonical module-level API
    if _ENGINE_API.has_module_api and _ENGINE_API.get_enriched_quote is not None:
        try:
            return await _ENGINE_API.get_enriched_quote(symbol, use_cache=use_cache)
        except TypeError:
            # Older API without use_cache kwarg — fall through
            try:
                return await _ENGINE_API.get_enriched_quote(symbol)
            except Exception:
                pass

    # Fallback: engine instance methods
    if engine is not None:
        for name in ("get_enriched_quote", "get_quote"):
            fn = getattr(engine, name, None)
            if not callable(fn):
                continue
            # Try canonical kwargs first
            try:
                return await fn(symbol, use_cache=use_cache)
            except TypeError:
                pass
            # Try positional use_cache (instance signature: (sym, use_cache, ...))
            try:
                return await fn(symbol, use_cache)
            except TypeError:
                pass
            # Last resort: no cache kwarg
            try:
                return await fn(symbol)
            except Exception:
                continue

    raise RuntimeError("No single-symbol quote method available on engine or module API")


async def _call_engine_batch(
    engine: Any,
    symbols: List[str],
    *,
    use_cache: bool,
) -> Any:
    """
    Batch quote fetch with canonical signature.

    Priority:
      1. Module-level `core.data_engine.get_enriched_quotes(symbols, use_cache, ttl)`
      2. Engine instance batch methods
    """
    # Primary: canonical module-level batch API
    if _ENGINE_API.has_module_api and _ENGINE_API.get_enriched_quotes is not None:
        try:
            return await _ENGINE_API.get_enriched_quotes(symbols, use_cache=use_cache)
        except TypeError:
            try:
                return await _ENGINE_API.get_enriched_quotes(symbols)
            except Exception:
                pass

    # Fallback: engine instance batch methods (canonical order per DataEngineV5)
    if engine is not None:
        for name in (
            "get_enriched_quotes",
            "get_quotes",
            "get_enriched_quotes_batch",
            "get_quotes_batch",
        ):
            fn = getattr(engine, name, None)
            if not callable(fn):
                continue
            try:
                return await fn(symbols, use_cache=use_cache)
            except TypeError:
                pass
            try:
                return await fn(symbols)
            except Exception:
                continue

    raise RuntimeError("No batch quote method available on engine or module API")


async def _refresh_symbol(
    engine: Any,
    symbol: str,
    backoff: FullJitterBackoff,
    *,
    timeout_sec: float,
    use_cache: bool,
) -> Tuple[bool, str]:
    """Refresh one symbol with timeout + retry."""
    async def _do() -> Any:
        return await _call_engine_single(engine, symbol, use_cache=use_cache)

    with TraceContext("refresh_symbol", {"symbol": symbol, "use_cache": use_cache}):
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
    use_cache: bool,
) -> Tuple[bool, str]:
    """Refresh a batch with timeout + retry."""
    async def _do() -> Any:
        return await _call_engine_batch(engine, symbols, use_cache=use_cache)

    with TraceContext(
        "refresh_batch", {"count": len(symbols), "use_cache": use_cache}
    ):
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
    """
    Run a refresh pass over the given symbols.

    `refresh=True` means "force fresh data, bypass cache" → passes
    `use_cache=False` to the canonical engine API.
    """
    symbols = _coerce_symbols(symbols)
    # v5.2.0: invert semantics at the boundary.
    use_cache = not refresh
    stats = RefreshStats(total=len(symbols), use_cache=use_cache)

    if not _ENGINE_AVAILABLE:
        stats.errors.append(
            "Core data engine is not available (neither core.data_engine "
            "nor core.data_engine_v2 could be imported)."
        )
        stats.failed = len(symbols)
        for s in symbols:
            stats.per_symbol[s] = "engine_missing"
        return stats

    # v5.2.0: engine resolution is optional. The canonical module-level API
    # handles engine init internally. We only fetch an instance if the
    # module-level API is unavailable.
    engine: Any = None
    engine_source = ""
    if _ENGINE_API.has_module_api:
        engine_source = f"module:{_ENGINE_API.module_path}"
    else:
        if _ENGINE_API.get_engine is not None:
            try:
                engine = await _maybe_await(_ENGINE_API.get_engine())
                engine_source = f"instance:{_ENGINE_API.module_path}"
            except Exception as e:
                stats.errors.append(f"Failed to initialize engine: {e}")
                stats.failed = len(symbols)
                for s in symbols:
                    stats.per_symbol[s] = "engine_init_failed"
                return stats

            if engine is None:
                stats.errors.append(
                    "Failed to initialize core data engine (get_engine returned None)."
                )
                stats.failed = len(symbols)
                for s in symbols:
                    stats.per_symbol[s] = "engine_none"
                return stats
        else:
            stats.errors.append(
                "No module-level API and no get_engine() callable found."
            )
            stats.failed = len(symbols)
            for s in symbols:
                stats.per_symbol[s] = "no_engine_api"
            return stats

    stats.engine_source = engine_source

    backoff = FullJitterBackoff()
    sem = asyncio.Semaphore(max(1, int(concurrency)))

    async def _do_one(sym: str) -> None:
        async with sem:
            ok, msg = await _refresh_symbol(
                engine, sym, backoff,
                timeout_sec=timeout_sec, use_cache=use_cache,
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
                timeout_sec=max(timeout_sec, 45.0), use_cache=use_cache,
            )
            if ok:
                stats.success += len(batch)
                for s in batch:
                    stats.per_symbol[s] = "ok(batch)"
            else:
                stats.failed += len(batch)
                for s in batch:
                    stats.per_symbol[s] = msg

    logger.info(
        "Starting refresh: total=%d concurrency=%d use_cache=%s "
        "timeout=%.1fs batch_size=%d source=%s",
        len(symbols), concurrency, use_cache, timeout_sec,
        batch_size or 0, engine_source,
    )

    if batch_size and batch_size > 1:
        batches = [
            symbols[i:i + batch_size]
            for i in range(0, len(symbols), batch_size)
        ]
        await asyncio.gather(
            *(_do_batch(b) for b in batches), return_exceptions=True
        )
    else:
        await asyncio.gather(
            *(_do_one(s) for s in symbols), return_exceptions=True
        )

    # Close engine if we own it (instance path)
    if engine is not None:
        try:
            if hasattr(engine, "aclose"):
                aclose_fn = engine.aclose
                if asyncio.iscoroutinefunction(aclose_fn):
                    await aclose_fn()
                else:
                    result = aclose_fn()
                    await _maybe_await(result)
            elif hasattr(engine, "close"):
                close_fn = engine.close
                if asyncio.iscoroutinefunction(close_fn):
                    await close_fn()
                else:
                    close_fn()
        except Exception as e:
            logger.debug("Engine close failed (non-fatal): %s", e)

    # For module-level API, optionally close the shared singleton
    if _ENGINE_API.close_engine is not None and _ENGINE_API.has_module_api:
        try:
            await _maybe_await(_ENGINE_API.close_engine())
        except Exception as e:
            logger.debug("Module close_engine failed (non-fatal): %s", e)

    return stats


# =============================================================================
# CLI
# =============================================================================
async def main_async(args: argparse.Namespace) -> int:
    exit_code = 0
    try:
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
            sys.stdout.write(json_dumps(stats.to_dict(), indent=2) + "\n")
        else:
            logger.info("=" * 60)
            logger.info("REFRESH COMPLETE (v%s)", SCRIPT_VERSION)
            logger.info("=" * 60)
            logger.info("Engine:    %s", stats.engine_source or "(none)")
            logger.info("use_cache: %s (refresh=%s)", stats.use_cache, not stats.use_cache)
            logger.info("Duration:  %.2fs", stats.duration_sec)
            logger.info("Total:     %d", stats.total)
            logger.info("Success:   %d", stats.success)
            logger.info("Failed:    %d", stats.failed)
            if stats.errors:
                logger.warning("Errors:")
                for e in stats.errors:
                    logger.warning(" - %s", e)
            logger.info("=" * 60)

        exit_code = 1 if stats.failed > 0 else 0
    except Exception as e:
        logger.exception("Refresh failed: %s", e)
        exit_code = 1
    finally:
        _shutdown_executor(wait=False)

    return exit_code


def main() -> None:
    parser = argparse.ArgumentParser(
        description=f"Tadawul Distributed Data Refresher v{SCRIPT_VERSION}"
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=os.getenv("REFRESH_SYMBOLS", ""),
        help="Comma-separated symbols. Also: REFRESH_SYMBOLS env var.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=_env_int("REFRESH_CONCURRENCY", 10, lo=1, hi=1000),
        help="Async concurrency. Also: REFRESH_CONCURRENCY env var.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=_env_float("REFRESH_TIMEOUT", 25.0, lo=0.1, hi=600.0),
        help="Per-symbol timeout seconds. Also: REFRESH_TIMEOUT env var.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=_env_int("REFRESH_BATCH_SIZE", 0, lo=0, hi=5000),
        help="If >1, uses engine batch API with this batch size. "
             "Also: REFRESH_BATCH_SIZE env var.",
    )
    parser.add_argument(
        "--no-refresh",
        action="store_true",
        default=_env_bool("REFRESH_NO_REFRESH", False),
        help="Do not force refresh (let engine use cache → use_cache=True). "
             "Also: REFRESH_NO_REFRESH env var.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=_env_bool("REFRESH_JSON", False),
        help="Print JSON output. Also: REFRESH_JSON env var.",
    )

    args = parser.parse_args()

    try:
        sys.exit(asyncio.run(main_async(args)))
    except KeyboardInterrupt:
        logger.info("Refresh interrupted by user.")
        _shutdown_executor(wait=False)
        sys.exit(130)
    except Exception:
        _shutdown_executor(wait=False)
        raise


__all__ = [
    "SCRIPT_VERSION",
    "SERVICE_VERSION",
    "RefreshStats",
    "FullJitterBackoff",
    "TraceContext",
    "run_refresh",
    "main_async",
    "main",
]


if __name__ == "__main__":
    main()
