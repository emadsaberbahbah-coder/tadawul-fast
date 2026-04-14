#!/usr/bin/env python3
"""
scripts/worker.py
===========================================================
TADAWUL FAST BRIDGE – ENTERPRISE WORKER ORCHESTRATOR (v4.2.0)
===========================================================
RECOVERY-ALIGNED • REDIS-COMPATIBLE • TRACE-SAFE • STARTUP-SAFE

What v4.2.0 revises
- FIX: supports modern `redis.asyncio` first, with `aioredis` fallback.
       This matches environments where `redis` is installed but `aioredis`
       is not.
- FIX: TraceContext now uses OpenTelemetry context managers correctly.
- FIX: clean Redis shutdown across both client variants.
- FIX: dead-letter support for exhausted jobs.
- ENH: central worker config via env vars.
- ENH: deterministic pipeline dispatch helpers for dashboard_sync,
       market_scan, and refresh_data.
- SAFE: preserves idle fallback when Redis is unavailable, unless fail-fast
       is explicitly enabled.

Supported task_type values in Redis queue payload:
  - "dashboard_sync"
  - "market_scan"
  - "refresh_data"
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
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

# ---------------------------------------------------------------------------
# High-performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        option = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(v, option=option, default=str).decode("utf-8")

    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)

    _HAS_ORJSON = True
except Exception:
    import json

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        return json.dumps(v, indent=indent if indent else None, default=str)

    def json_loads(data: Union[str, bytes]) -> Any:
        return json.loads(data)

    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Optional tracing
# ---------------------------------------------------------------------------
try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore

    _OTEL_AVAILABLE = True
    _TRACER = trace.get_tracer(__name__)
except Exception:
    trace = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    _OTEL_AVAILABLE = False
    _TRACER = None

_TRACING_ENABLED = os.getenv("CORE_TRACING_ENABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}


class TraceContext:
    """OpenTelemetry context manager that never raises."""

    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._cm = None
        self._span = None

    def __enter__(self):
        if not (_OTEL_AVAILABLE and _TRACING_ENABLED and _TRACER is not None):
            return self
        try:
            self._cm = _TRACER.start_as_current_span(self.name)
            self._span = self._cm.__enter__()
            if self._span is not None and self.attributes:
                for k, v in self.attributes.items():
                    try:
                        self._span.set_attribute(str(k), v)
                    except Exception:
                        pass
        except Exception:
            self._cm = None
            self._span = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self._span is not None and exc_val is not None:
                try:
                    if hasattr(self._span, "record_exception"):
                        self._span.record_exception(exc_val)
                except Exception:
                    pass
                try:
                    if Status is not None and StatusCode is not None and hasattr(self._span, "set_status"):
                        self._span.set_status(Status(StatusCode.ERROR, str(exc_val)))
                except Exception:
                    pass
        finally:
            if self._cm is not None:
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
# Redis client resolution
# ---------------------------------------------------------------------------
_REDIS_LIB = "none"
try:
    import redis.asyncio as redis_async  # type: ignore

    _REDIS_AVAILABLE = True
    _REDIS_LIB = "redis.asyncio"
except Exception:
    try:
        import aioredis as redis_async  # type: ignore

        _REDIS_AVAILABLE = True
        _REDIS_LIB = "aioredis"
    except Exception:
        redis_async = None  # type: ignore
        _REDIS_AVAILABLE = False


SCRIPT_VERSION = "4.2.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Worker")


def _safe_int(v: Any, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        x = int(float(str(v).strip()))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def _safe_float(v: Any, default: float, *, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        x = float(str(v).strip())
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


@dataclass(slots=True)
class WorkerConfig:
    queue_name: str = os.getenv("TFB_WORKER_QUEUE_NAME", "tfb_background_jobs")
    dead_letter_queue: str = os.getenv("TFB_WORKER_DLQ_NAME", "tfb_background_jobs_dead")
    redis_url: str = os.getenv("REDIS_URL", "").strip()
    redis_block_timeout_sec: int = _safe_int(os.getenv("TFB_WORKER_REDIS_BLOCK_TIMEOUT_SEC", "5"), 5, lo=1, hi=60)
    idle_sleep_sec: float = _safe_float(os.getenv("TFB_WORKER_IDLE_SLEEP_SEC", "5"), 5.0, lo=0.5, hi=60.0)
    max_retries: int = _safe_int(os.getenv("TFB_WORKER_MAX_RETRIES", "5"), 5, lo=1, hi=20)
    backoff_base_sec: float = _safe_float(os.getenv("TFB_WORKER_BACKOFF_BASE_SEC", "1.0"), 1.0, lo=0.1, hi=30.0)
    backoff_max_sec: float = _safe_float(os.getenv("TFB_WORKER_BACKOFF_MAX_SEC", "30.0"), 30.0, lo=1.0, hi=300.0)
    cpu_workers: int = _safe_int(os.getenv("TFB_WORKER_CPU_WORKERS", "4"), 4, lo=1, hi=32)
    fail_fast_if_no_redis: bool = os.getenv("TFB_WORKER_FAIL_FAST_NO_REDIS", "false").strip().lower() in {"1", "true", "yes", "y", "on"}


CONFIG = WorkerConfig()
_CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=CONFIG.cpu_workers, thread_name_prefix="TFBWorker")
SHUTDOWN_EVENT = asyncio.Event()


# =============================================================================
# Full jitter exponential backoff
# =============================================================================
class FullJitterBackoff:
    def __init__(self, max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 30.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    async def execute_async(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                return await func(*args, **kwargs)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                last_exc = e
                if attempt >= self.max_retries - 1:
                    raise
                ceiling = min(self.max_delay, self.base_delay * (2 ** attempt))
                sleep_time = random.uniform(0.0, ceiling)
                logger.warning("Task failed on attempt %s/%s: %s. Retrying in %.2fs", attempt + 1, self.max_retries, e, sleep_time)
                await asyncio.sleep(sleep_time)
        raise last_exc if last_exc is not None else RuntimeError("backoff_exhausted")


# =============================================================================
# Generic import / execution helpers
# =============================================================================
def _import_first(candidates: Sequence[str]):
    last_exc: Optional[Exception] = None
    for name in candidates:
        try:
            return importlib.import_module(name)
        except Exception as exc:
            last_exc = exc
    if last_exc is not None:
        raise last_exc
    raise ImportError("No module candidates provided")


async def _run_sync_in_pool(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_CPU_EXECUTOR, lambda: fn(*args, **kwargs))


async def _maybe_call(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if asyncio.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    result = await _run_sync_in_pool(fn, *args, **kwargs)
    if asyncio.iscoroutine(result):
        return await result
    return result


async def _call_first_callable(module: Any, names: Sequence[str], *args: Any, **kwargs: Any) -> Tuple[bool, Any]:
    for name in names:
        fn = getattr(module, name, None)
        if callable(fn):
            return True, await _maybe_call(fn, *args, **kwargs)
    return False, None


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _result_dict(result: Any, *, ok_default: bool = True) -> Dict[str, Any]:
    if isinstance(result, dict):
        return result
    return {"ok": ok_default, "result": str(result)}


# =============================================================================
# Pipeline dispatchers
# =============================================================================
async def _run_dashboard_sync_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        mod = _import_first(("scripts.run_dashboard_sync", "run_dashboard_sync"))
    except Exception:
        logger.exception("run_dashboard_sync module not found")
        return {"ok": False, "error": "module_not_found", "task_type": "dashboard_sync"}

    sheet_name = str(payload.get("sheet_name") or "").strip()
    symbols = _as_list(payload.get("symbols") or [])
    spreadsheet_id = str(payload.get("spreadsheet_id") or os.getenv("DEFAULT_SPREADSHEET_ID", "")).strip()
    limit = _safe_int(payload.get("limit") or 800, 800, lo=1, hi=50000)

    kwargs: Dict[str, Any] = {"spreadsheet_id": spreadsheet_id, "limit": limit}
    if sheet_name:
        kwargs["sheet_name"] = sheet_name
        kwargs["keys"] = [sheet_name]
    if symbols:
        kwargs["symbols"] = symbols

    found, result = await _call_first_callable(
        mod,
        ("run_async", "run_dashboard_sync_async", "main_async", "run", "run_dashboard_sync", "main"),
        **kwargs,
    )
    if not found:
        logger.error("run_dashboard_sync: no callable entrypoint found")
        return {"ok": False, "error": "no_callable_found", "task_type": "dashboard_sync"}
    return _result_dict(result)


async def _run_market_scan_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        mod = _import_first(("scripts.run_market_scan", "run_market_scan"))
    except Exception:
        logger.exception("run_market_scan module not found")
        return {"ok": False, "error": "module_not_found", "task_type": "market_scan"}

    spreadsheet_id = str(payload.get("spreadsheet_id") or os.getenv("DEFAULT_SPREADSHEET_ID", "")).strip()
    keys = _as_list(payload.get("keys") or payload.get("sheet_names") or ["Market_Leaders"])
    top_n = _safe_int(payload.get("limit") or payload.get("top_n") or 10, 10, lo=1, hi=1000)
    horizon = str(payload.get("horizon") or "3m").strip().lower()
    min_confidence = _safe_float(payload.get("min_confidence") or 0.65, 0.65, lo=0.0, hi=1.0)
    max_risk = _safe_float(payload.get("max_risk") or 60.0, 60.0, lo=0.0, hi=100.0)
    min_roi = _safe_float(payload.get("min_roi") or 0.0, 0.0, lo=-1000.0, hi=1000.0)
    mode = str(payload.get("mode") or "engine").strip()

    scan_config_cls = getattr(mod, "ScanConfig", None)
    if scan_config_cls is not None:
        cfg = scan_config_cls(
            spreadsheet_id=spreadsheet_id,
            keys=keys,
            top_n=top_n,
            horizon=horizon,
            min_confidence=min_confidence,
            max_risk_score=max_risk,
            min_expected_roi=min_roi,
            mode=mode,
        )
        found, result = await _call_first_callable(mod, ("run_scan_async", "run_async", "main_async", "run_scan", "run", "main"), cfg)
        if not found:
            logger.error("run_market_scan: no callable entrypoint found")
            return {"ok": False, "error": "no_callable_found", "task_type": "market_scan"}
        if isinstance(result, tuple) and len(result) == 2:
            rows, meta = result
            return {"ok": True, "rows": len(rows or []), "meta": meta}
        return _result_dict(result)

    kwargs = {
        "spreadsheet_id": spreadsheet_id,
        "keys": keys,
        "top_n": top_n,
        "horizon": horizon,
        "min_confidence": min_confidence,
        "max_risk": max_risk,
        "min_roi": min_roi,
        "mode": mode,
    }
    found, result = await _call_first_callable(mod, ("run_scan_async", "run_async", "main_async", "run_scan", "run", "main"), **kwargs)
    if not found:
        logger.error("run_market_scan: no callable entrypoint found")
        return {"ok": False, "error": "no_callable_found", "task_type": "market_scan"}
    return _result_dict(result)


async def _run_refresh_data_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        mod = _import_first(("scripts.refresh_data", "refresh_data"))
    except Exception:
        logger.exception("refresh_data module not found")
        return {"ok": False, "error": "module_not_found", "task_type": "refresh_data"}

    kwargs: Dict[str, Any] = {}
    sheet_name = str(payload.get("sheet_name") or "").strip()
    symbols = _as_list(payload.get("symbols") or [])
    if sheet_name:
        kwargs["sheet_name"] = sheet_name
    if symbols:
        kwargs["symbols"] = symbols

    found, result = await _call_first_callable(mod, ("run_async", "main_async", "run", "main"), **kwargs)
    if not found:
        logger.error("refresh_data: no callable entrypoint found")
        return {"ok": False, "error": "no_callable_found", "task_type": "refresh_data"}
    return _result_dict(result)


# =============================================================================
# Task processing
# =============================================================================
async def _process_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    task_id = str(payload.get("task_id") or "unknown")
    task_type = str(payload.get("task_type") or "dashboard_sync").strip().lower()
    started = time.time()

    with TraceContext("worker_process_task", {"task_id": task_id, "task_type": task_type}):
        logger.info("[%s] Starting task type=%r", task_id, task_type)
        if task_type == "dashboard_sync":
            result = await _run_dashboard_sync_task(payload)
        elif task_type == "market_scan":
            result = await _run_market_scan_task(payload)
        elif task_type == "refresh_data":
            result = await _run_refresh_data_task(payload)
        else:
            logger.warning("[%s] Unknown task_type=%r. Supported: dashboard_sync, market_scan, refresh_data", task_id, task_type)
            return {"ok": False, "error": "unknown_task_type", "task_type": task_type}

        elapsed = round(time.time() - started, 2)
        ok = bool(result.get("ok", True)) if isinstance(result, dict) else True
        if ok:
            logger.info("[%s] Task type=%r completed in %ss. result=%s", task_id, task_type, elapsed, json_dumps(result))
        else:
            logger.error("[%s] Task type=%r failed in %ss. result=%s", task_id, task_type, elapsed, json_dumps(result))
            raise RuntimeError(json_dumps(result))
        return _result_dict(result)


# =============================================================================
# Redis helpers
# =============================================================================
async def _create_redis_client(redis_url: str):
    if not (_REDIS_AVAILABLE and redis_async is not None and redis_url):
        return None
    try:
        if _REDIS_LIB == "redis.asyncio":
            return redis_async.from_url(redis_url, decode_responses=True)
        return await redis_async.from_url(redis_url, decode_responses=True)
    except TypeError:
        return redis_async.from_url(redis_url, decode_responses=True)


async def _close_redis_client(client: Any) -> None:
    if client is None:
        return
    try:
        close_fn = getattr(client, "aclose", None)
        if callable(close_fn):
            await close_fn()
            return
    except Exception:
        pass
    try:
        close_fn = getattr(client, "close", None)
        if callable(close_fn):
            result = close_fn()
            if asyncio.iscoroutine(result):
                await result
    except Exception:
        pass
    try:
        wait_closed = getattr(client, "wait_closed", None)
        if callable(wait_closed):
            result = wait_closed()
            if asyncio.iscoroutine(result):
                await result
    except Exception:
        pass


async def _push_dead_letter(client: Any, raw_payload: str, error_text: str) -> None:
    if client is None or not CONFIG.dead_letter_queue:
        return
    envelope = {
        "failed_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "error": error_text,
        "raw_payload": raw_payload,
    }
    try:
        await client.rpush(CONFIG.dead_letter_queue, json_dumps(envelope))
    except Exception:
        logger.exception("Failed to push payload to dead-letter queue")


async def _poll_redis_queue(redis_url: str) -> None:
    logger.info("Connecting to Redis via %s using %s", redis_url, _REDIS_LIB)
    redis_client = await _create_redis_client(redis_url)
    if redis_client is None:
        raise RuntimeError("redis_client_unavailable")

    backoff = FullJitterBackoff(
        max_retries=CONFIG.max_retries,
        base_delay=CONFIG.backoff_base_sec,
        max_delay=CONFIG.backoff_max_sec,
    )

    try:
        while not SHUTDOWN_EVENT.is_set():
            try:
                result = await redis_client.blpop(CONFIG.queue_name, timeout=CONFIG.redis_block_timeout_sec)
                if not result:
                    continue

                _, data_raw = result
                if isinstance(data_raw, bytes):
                    data_raw = data_raw.decode("utf-8", errors="replace")

                try:
                    payload = json_loads(data_raw)
                    if not isinstance(payload, dict):
                        raise ValueError("payload_not_dict")
                except Exception as parse_err:
                    logger.error("Failed to parse queue payload: %s | raw=%r", parse_err, str(data_raw)[:300])
                    await _push_dead_letter(redis_client, str(data_raw), f"parse_error:{parse_err}")
                    continue

                try:
                    await backoff.execute_async(_process_payload, payload)
                except asyncio.CancelledError:
                    raise
                except Exception as exec_err:
                    logger.exception("Task exhausted retries: %s", exec_err)
                    await _push_dead_letter(redis_client, str(data_raw), f"task_failed:{exec_err}")
            except asyncio.CancelledError:
                raise
            except Exception as poll_err:
                logger.error("Queue polling error: %s", poll_err)
                await asyncio.sleep(2)
    finally:
        await _close_redis_client(redis_client)
        logger.info("Redis connection closed")


async def _idle_loop() -> None:
    while not SHUTDOWN_EVENT.is_set():
        await asyncio.sleep(CONFIG.idle_sleep_sec)


async def main_async(args: argparse.Namespace) -> int:
    del args
    logger.info("🚀 Starting TFB Worker v%s | redis_lib=%s | has_orjson=%s", SCRIPT_VERSION, _REDIS_LIB, _HAS_ORJSON)

    tasks: List[asyncio.Task[Any]] = []
    if _REDIS_AVAILABLE and CONFIG.redis_url:
        logger.info("Redis available. Polling queue=%s", CONFIG.queue_name)
        tasks.append(asyncio.create_task(_poll_redis_queue(CONFIG.redis_url), name="redis-poller"))
    else:
        message = "Redis not available or REDIS_URL is empty. Worker is in idle mode."
        if CONFIG.fail_fast_if_no_redis:
            logger.error(message)
            return 2
        logger.warning(message)
        tasks.append(asyncio.create_task(_idle_loop(), name="idle-loop"))

    await SHUTDOWN_EVENT.wait()
    logger.info("Shutdown signal received. Draining tasks...")

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    _CPU_EXECUTOR.shutdown(wait=False, cancel_futures=True)
    logger.info("Worker stopped cleanly")
    return 0


def _handle_signal(sig: int, _frame: Any) -> None:
    logger.info("Signal %s received. Initiating graceful shutdown...", sig)
    SHUTDOWN_EVENT.set()


def main() -> None:
    parser = argparse.ArgumentParser(description=f"Tadawul Distributed Worker v{SCRIPT_VERSION}")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        raise SystemExit(asyncio.run(main_async(args)))
    except KeyboardInterrupt:
        raise SystemExit(130)
    except SystemExit:
        raise
    except Exception as e:
        logger.exception("Fatal worker crash: %s", e)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
