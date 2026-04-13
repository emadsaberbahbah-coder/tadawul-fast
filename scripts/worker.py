#!/usr/bin/env python3
"""
scripts/worker.py
===========================================================
TADAWUL FAST BRIDGE – ENTERPRISE WORKER ORCHESTRATOR (v4.1.0)
===========================================================
QUANTUM EDITION | ASYNC BATCHING | DISTRIBUTED QUEUES

What's new in v4.1.0:
- ✅ FIXED: _process_payload() now delegates to real sync pipelines
  (run_dashboard_sync, run_market_scan) based on task_type in payload.
- ✅ Persistent ThreadPoolExecutor: CPU-heavy tasks offloaded cleanly.
- ✅ High-Performance JSON (orjson): Blazing fast serialization.
- ✅ Full Jitter Exponential Backoff: Safe retry under burst load.
- ✅ OpenTelemetry Tracing: Granular background task tracking.
- ✅ Redis Pub/Sub polling to trigger sync pipelines.
- ✅ Graceful shutdown, signal trapping, idle fallback.

Supported task_type values in Redis queue payload:
  - "dashboard_sync"  → runs run_dashboard_sync pipeline
  - "market_scan"     → runs run_market_scan pipeline
  - "refresh_data"    → runs refresh_data pipeline
  - (unrecognized)    → logged and skipped safely

Payload shape (JSON pushed to Redis queue "tfb_background_jobs"):
  {
    "task_id": "abc-123",
    "task_type": "dashboard_sync",
    "sheet_name": "Market_Leaders",      # optional, for scoped syncs
    "symbols": ["2222.SR", "AAPL"],      # optional
    "limit": 10,                          # optional
    "horizon": "3m",                      # optional, for market_scan
    "min_confidence": 0.65,              # optional, for market_scan
    "max_risk": 60.0,                    # optional, for market_scan
    "spreadsheet_id": "...",             # optional override
    "mode": "engine"                     # optional
  }
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import importlib
import logging
import os
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Union

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson
    def json_dumps(v: Any, *, indent: int = 0) -> str:
        option = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(v, option=option).decode("utf-8")
    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)
    _HAS_ORJSON = True
except ImportError:
    import json
    def json_dumps(v: Any, *, indent: int = 0) -> str:
        return json.dumps(v, indent=indent if indent else None, default=str)
    def json_loads(data: Union[str, bytes]) -> Any:
        return json.loads(data)
    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Optional Tracing
# ---------------------------------------------------------------------------
try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
    _OTEL_AVAILABLE = True
    tracer = trace.get_tracer(__name__)
except ImportError:
    _OTEL_AVAILABLE = False
    class DummySpan:
        def set_attribute(self, *args, **kwargs): pass
        def set_status(self, *args, **kwargs): pass
        def record_exception(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args, **kwargs): pass
    class DummyTracer:
        def start_as_current_span(self, *args, **kwargs): return DummySpan()
    tracer = DummyTracer()

_TRACING_ENABLED = os.getenv("CORE_TRACING_ENABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}


class TraceContext:
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.tracer = tracer if _OTEL_AVAILABLE and _TRACING_ENABLED else None
        self.span = None

    def __enter__(self):
        if self.tracer:
            self.span = self.tracer.start_as_current_span(self.name)
            if self.attributes:
                self.span.set_attributes(self.attributes)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.span and _OTEL_AVAILABLE:
            if exc_val:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            self.span.end()

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__exit__(exc_type, exc_val, exc_tb)


# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------
try:
    import aioredis
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

SCRIPT_VERSION = "4.1.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Worker")

_CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="JobWorker")
SHUTDOWN_EVENT = asyncio.Event()


# =============================================================================
# Full Jitter Exponential Backoff
# =============================================================================
class FullJitterBackoff:
    """Safe retry mechanism implementing AWS Full Jitter Backoff."""

    def __init__(self, max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 30.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    async def execute_async(self, func: Callable, *args, **kwargs) -> Any:
        last_exc = None
        for attempt in range(self.max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_exc = e
                if attempt == self.max_retries - 1:
                    raise
                temp = min(self.max_delay, self.base_delay * (2 ** attempt))
                sleep_time = random.uniform(0, temp)
                logger.debug(f"Task failed: {e}. Retrying in {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)
        raise last_exc


# =============================================================================
# Sync pipeline dispatchers
# =============================================================================

async def _run_dashboard_sync_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Delegates to run_dashboard_sync pipeline.
    Supports optional scoping by sheet_name and symbols.
    """
    try:
        mod = importlib.import_module("run_dashboard_sync")
    except ImportError:
        try:
            mod = importlib.import_module("scripts.run_dashboard_sync")
        except ImportError:
            logger.error("run_dashboard_sync module not found. Ensure it is in the Python path.")
            return {"ok": False, "error": "module_not_found", "task_type": "dashboard_sync"}

    sheet_name = payload.get("sheet_name") or ""
    symbols = payload.get("symbols") or []
    spreadsheet_id = payload.get("spreadsheet_id") or os.getenv("DEFAULT_SPREADSHEET_ID", "")
    limit = int(payload.get("limit") or 800)

    # Prefer async run function if available, else fall back to sync
    run_fn = (
        getattr(mod, "run_async", None)
        or getattr(mod, "run_dashboard_sync_async", None)
        or getattr(mod, "main_async", None)
    )
    run_sync_fn = (
        getattr(mod, "run", None)
        or getattr(mod, "run_dashboard_sync", None)
        or getattr(mod, "main", None)
    )

    kwargs: Dict[str, Any] = {
        "spreadsheet_id": spreadsheet_id,
        "limit": limit,
    }
    if sheet_name:
        kwargs["sheet_name"] = sheet_name
        kwargs["keys"] = [sheet_name]
    if symbols:
        kwargs["symbols"] = symbols

    if callable(run_fn):
        result = await run_fn(**kwargs)
        return result if isinstance(result, dict) else {"ok": True, "result": str(result)}

    if callable(run_sync_fn):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(_CPU_EXECUTOR, lambda: run_sync_fn(**kwargs))
        return result if isinstance(result, dict) else {"ok": True, "result": str(result)}

    logger.error("run_dashboard_sync: no callable run/run_async/main function found in module.")
    return {"ok": False, "error": "no_callable_found", "task_type": "dashboard_sync"}


async def _run_market_scan_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Delegates to run_market_scan pipeline.
    Passes scan parameters extracted from the payload.
    """
    try:
        mod = importlib.import_module("run_market_scan")
    except ImportError:
        try:
            mod = importlib.import_module("scripts.run_market_scan")
        except ImportError:
            logger.error("run_market_scan module not found. Ensure it is in the Python path.")
            return {"ok": False, "error": "module_not_found", "task_type": "market_scan"}

    spreadsheet_id = payload.get("spreadsheet_id") or os.getenv("DEFAULT_SPREADSHEET_ID", "")
    keys = payload.get("keys") or payload.get("sheet_names") or ["Market_Leaders"]
    top_n = int(payload.get("limit") or payload.get("top_n") or 10)
    horizon = str(payload.get("horizon") or "3m").lower()
    min_confidence = float(payload.get("min_confidence") or 0.65)
    max_risk = float(payload.get("max_risk") or 60.0)
    min_roi = float(payload.get("min_roi") or 0.0)
    mode = str(payload.get("mode") or "engine")

    run_fn = (
        getattr(mod, "run_scan_async", None)
        or getattr(mod, "run_async", None)
        or getattr(mod, "main_async", None)
    )
    run_sync_fn = (
        getattr(mod, "run_scan", None)
        or getattr(mod, "run", None)
        or getattr(mod, "main", None)
    )

    # Build a ScanConfig if the module exposes it, otherwise pass raw kwargs
    scan_config_cls = getattr(mod, "ScanConfig", None)

    if scan_config_cls is not None:
        cfg = scan_config_cls(
            spreadsheet_id=spreadsheet_id,
            keys=keys if isinstance(keys, list) else [keys],
            top_n=top_n,
            horizon=horizon,
            min_confidence=min_confidence,
            max_risk_score=max_risk,
            min_expected_roi=min_roi,
            mode=mode,
        )
        if callable(run_fn):
            rows, meta = await run_fn(cfg)
            return {"ok": True, "rows": len(rows), "meta": meta}
        if callable(run_sync_fn):
            loop = asyncio.get_running_loop()
            rows, meta = await loop.run_in_executor(_CPU_EXECUTOR, lambda: run_sync_fn(cfg))
            return {"ok": True, "rows": len(rows), "meta": meta}
    else:
        kwargs = dict(
            spreadsheet_id=spreadsheet_id,
            keys=keys,
            top_n=top_n,
            horizon=horizon,
            min_confidence=min_confidence,
            max_risk=max_risk,
            min_roi=min_roi,
            mode=mode,
        )
        if callable(run_fn):
            result = await run_fn(**kwargs)
            return result if isinstance(result, dict) else {"ok": True, "result": str(result)}
        if callable(run_sync_fn):
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(_CPU_EXECUTOR, lambda: run_sync_fn(**kwargs))
            return result if isinstance(result, dict) else {"ok": True, "result": str(result)}

    logger.error("run_market_scan: no callable run/run_async/main function found in module.")
    return {"ok": False, "error": "no_callable_found", "task_type": "market_scan"}


async def _run_refresh_data_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Delegates to refresh_data pipeline.
    """
    try:
        mod = importlib.import_module("refresh_data")
    except ImportError:
        try:
            mod = importlib.import_module("scripts.refresh_data")
        except ImportError:
            logger.error("refresh_data module not found.")
            return {"ok": False, "error": "module_not_found", "task_type": "refresh_data"}

    run_fn = getattr(mod, "run_async", None) or getattr(mod, "main_async", None)
    run_sync_fn = getattr(mod, "run", None) or getattr(mod, "main", None)

    sheet_name = payload.get("sheet_name") or ""
    symbols = payload.get("symbols") or []
    kwargs: Dict[str, Any] = {}
    if sheet_name:
        kwargs["sheet_name"] = sheet_name
    if symbols:
        kwargs["symbols"] = symbols

    if callable(run_fn):
        result = await run_fn(**kwargs)
        return result if isinstance(result, dict) else {"ok": True}

    if callable(run_sync_fn):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(_CPU_EXECUTOR, lambda: run_sync_fn(**kwargs))
        return result if isinstance(result, dict) else {"ok": True}

    logger.error("refresh_data: no callable run/main function found in module.")
    return {"ok": False, "error": "no_callable_found", "task_type": "refresh_data"}


# =============================================================================
# Execution Core — REAL IMPLEMENTATION
# =============================================================================

async def _process_payload(payload: Dict[str, Any]) -> None:
    """
    Main task dispatcher. Routes each job to the correct sync pipeline
    based on the task_type field in the Redis queue payload.

    Supported task_type values:
      - "dashboard_sync"  → run_dashboard_sync pipeline
      - "market_scan"     → run_market_scan pipeline
      - "refresh_data"    → refresh_data pipeline
    """
    task_id = payload.get("task_id", "unknown")
    task_type = str(payload.get("task_type") or "dashboard_sync").strip().lower()
    started = time.time()

    with TraceContext("worker_process_task", {"task_id": task_id, "task_type": task_type}):
        logger.info(f"[{task_id}] Starting task type={task_type!r}")

        try:
            if task_type == "dashboard_sync":
                result = await _run_dashboard_sync_task(payload)

            elif task_type == "market_scan":
                result = await _run_market_scan_task(payload)

            elif task_type == "refresh_data":
                result = await _run_refresh_data_task(payload)

            else:
                logger.warning(
                    f"[{task_id}] Unknown task_type={task_type!r}. "
                    "Supported: dashboard_sync, market_scan, refresh_data. Skipping."
                )
                return

            elapsed = round(time.time() - started, 2)
            ok = result.get("ok", True) if isinstance(result, dict) else True
            if ok:
                logger.info(f"[{task_id}] Task type={task_type!r} completed in {elapsed}s. result={json_dumps(result)}")
            else:
                logger.error(f"[{task_id}] Task type={task_type!r} reported failure in {elapsed}s. result={json_dumps(result)}")

        except Exception as e:
            elapsed = round(time.time() - started, 2)
            logger.exception(f"[{task_id}] Task type={task_type!r} raised exception after {elapsed}s: {e}")
            raise  # re-raise so FullJitterBackoff can retry


async def _poll_redis_queue(redis_url: str) -> None:
    logger.info(f"Connecting to Redis queue at {redis_url}")
    redis = await aioredis.from_url(redis_url, decode_responses=True)
    backoff = FullJitterBackoff()

    while not SHUTDOWN_EVENT.is_set():
        try:
            # BLPOP blocks up to 5s waiting for a job
            result = await redis.blpop("tfb_background_jobs", timeout=5)
            if result:
                _, data_raw = result
                try:
                    payload = json_loads(data_raw)
                except Exception as parse_err:
                    logger.error(f"Failed to parse queue payload: {parse_err} | raw={data_raw[:200]!r}")
                    continue

                # Execute with full jitter retry
                await backoff.execute_async(_process_payload, payload)

        except asyncio.TimeoutError:
            continue
        except Exception as e:
            logger.error(f"Queue polling error: {e}")
            await asyncio.sleep(2)

    await redis.close()
    logger.info("Redis connection closed.")


async def main_async(args: argparse.Namespace) -> int:
    logger.info(f"🚀 Starting TFB Worker v{SCRIPT_VERSION}")

    redis_url = os.getenv("REDIS_URL")
    tasks = []

    if _REDIS_AVAILABLE and redis_url:
        logger.info(f"Redis available. Polling queue: tfb_background_jobs")
        tasks.append(asyncio.create_task(_poll_redis_queue(redis_url)))
    else:
        logger.warning(
            "Redis not available or REDIS_URL is empty. "
            "Worker is in idle mode — no jobs will be processed until Redis is configured."
        )

        async def idle_loop():
            while not SHUTDOWN_EVENT.is_set():
                await asyncio.sleep(5)

        tasks.append(asyncio.create_task(idle_loop()))

    # Wait for shutdown signal (SIGTERM or SIGINT)
    await SHUTDOWN_EVENT.wait()
    logger.info("Shutdown signal received. Draining tasks...")

    for t in tasks:
        t.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    _CPU_EXECUTOR.shutdown(wait=False)
    logger.info("Worker stopped cleanly.")
    return 0


def handle_signal(sig, frame):
    logger.info(f"Signal {sig} received. Initiating graceful shutdown...")
    SHUTDOWN_EVENT.set()


def main():
    parser = argparse.ArgumentParser(description=f"Tadawul Distributed Worker v{SCRIPT_VERSION}")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        sys.exit(asyncio.run(main_async(args)))
    except Exception as e:
        logger.exception(f"Fatal worker crash: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
