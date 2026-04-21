#!/usr/bin/env python3
"""
scripts/worker.py
===========================================================
TADAWUL FAST BRIDGE – ENTERPRISE WORKER ORCHESTRATOR (v4.3.0)
===========================================================
RECOVERY-ALIGNED • REDIS-COMPATIBLE • TRACE-SAFE • STARTUP-SAFE

Why this revision (v4.3.0 vs v4.2.0)
-------------------------------------
- 🔑 FIX HIGH: Dispatch now calls the canonical worker entrypoints that
    `run_dashboard_sync v6.5.0` and `run_market_scan v5.3.0` expose:
        run_from_worker_payload_async(payload: Dict[str, Any]) -> Dict
    v4.2.0 duplicated payload parsing in the worker and probed through
    a list of legacy function names. Since the canonical scripts now own
    the payload->config normalization logic, delegating the full payload
    to them eliminates:
      - drift between worker and script when new payload keys are added
      - silently dropped fields (v4.2.0 e.g. passed top_n/horizon/mode but
        missed `timeout_sec` and `concurrency` which are ScanConfig fields)
      - error-handling that differed between worker and script
    For `market_scan`, worker now just forwards the payload. For
    `dashboard_sync`, same. For `refresh_data` (which does NOT yet expose
    a worker entrypoint), the legacy name-probing is retained.

- 🔑 FIX MEDIUM: `SHUTDOWN_EVENT` is no longer created at module import
    time. `asyncio.Event()` binds to whichever event loop is "current" at
    creation, which may not be the loop `main_async` actually runs under.
    v4.3.0 creates the event inside `main_async()` and uses
    `loop.add_signal_handler()` — the canonical asyncio pattern.

- 🔑 FIX MEDIUM: `_CPU_EXECUTOR` is now lazy-initialized. v4.2.0 spawned
    `cpu_workers` threads at module-import time. v4.3.0 uses
    `_get_executor()` that creates the pool only on first use and
    shuts down cleanly (wait=True) on exit.

- FIX: Added project-standard `_TRUTHY`/`_FALSY` vocabulary (matches
    `main._TRUTHY`/`_FALSY`). `_TRACING_ENABLED` and
    `fail_fast_if_no_redis` now use the 8-value set including
    `t`/`enabled`/`enable`.

- FIX: Added `_env_bool`/`_env_int`/`_env_float` helpers.
- FIX: Added `SERVICE_VERSION = SCRIPT_VERSION` alias.
- FIX: Removed dead `Sequence` import from typing (unused after the
    dispatch refactor).

Supported task_type values in Redis queue payload:
  - "dashboard_sync"   -> scripts.run_dashboard_sync v6.5.0
  - "market_scan"      -> scripts.run_market_scan v5.3.0
  - "refresh_data"     -> scripts.refresh_data v5.2.0 (legacy dispatch)

Environment
-----------
  TFB_WORKER_QUEUE_NAME              Redis queue name (default tfb_background_jobs)
  TFB_WORKER_DLQ_NAME                Dead-letter queue name
  REDIS_URL                          Redis connection URL
  TFB_WORKER_REDIS_BLOCK_TIMEOUT_SEC BLPOP block timeout (default 5)
  TFB_WORKER_IDLE_SLEEP_SEC          idle mode poll interval (default 5.0)
  TFB_WORKER_MAX_RETRIES             per-task retry count (default 5)
  TFB_WORKER_BACKOFF_BASE_SEC        retry base delay (default 1.0)
  TFB_WORKER_BACKOFF_MAX_SEC         retry max delay (default 30.0)
  TFB_WORKER_CPU_WORKERS             thread pool size (default 4)
  TFB_WORKER_FAIL_FAST_NO_REDIS      truthy = exit 2 if Redis missing
  CORE_TRACING_ENABLED / TRACING_ENABLED  OpenTelemetry tracing (truthy)
  LOG_LEVEL                          logger level (default INFO)

Exit codes
----------
  0   success / clean shutdown
  1   fatal crash
  2   Redis unavailable + fail-fast enabled
  130 SIGINT
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
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
SCRIPT_VERSION = "4.3.0"
SERVICE_VERSION = SCRIPT_VERSION  # v4.3.0: cross-script alias


# ---------------------------------------------------------------------------
# Project-wide truthy/falsy vocabulary (matches main._TRUTHY / _FALSY)
# ---------------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}


def _env_bool(name: str, default: bool = False) -> bool:
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


def _env_int(
    name: str, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None
) -> int:
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


def _env_float(
    name: str,
    default: float,
    *,
    lo: Optional[float] = None,
    hi: Optional[float] = None,
) -> float:
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
    import json as _json

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        return _json.dumps(
            v, indent=(indent if indent else None), default=str, ensure_ascii=False
        )

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", errors="replace")
        return _json.loads(data)

    _HAS_ORJSON = False


# ---------------------------------------------------------------------------
# Optional tracing (OpenTelemetry)
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

# v4.3.0: uses project-canonical _env_bool
_TRACING_ENABLED = _env_bool("CORE_TRACING_ENABLED", False) or _env_bool(
    "TRACING_ENABLED", False
)


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
                    if (
                        Status is not None
                        and StatusCode is not None
                        and hasattr(self._span, "set_status")
                    ):
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
# Redis client resolution — prefer redis.asyncio, fall back to aioredis
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


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Worker")


# =============================================================================
# Small helpers preserved for call-site compat
# =============================================================================
def _safe_int(
    v: Any, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None
) -> int:
    try:
        x = int(float(str(v).strip()))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def _safe_float(
    v: Any, default: float, *, lo: Optional[float] = None, hi: Optional[float] = None
) -> float:
    try:
        x = float(str(v).strip())
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


# =============================================================================
# Worker config
# =============================================================================
@dataclass(slots=True)
class WorkerConfig:
    queue_name: str = field(
        default_factory=lambda: os.getenv("TFB_WORKER_QUEUE_NAME", "tfb_background_jobs")
    )
    dead_letter_queue: str = field(
        default_factory=lambda: os.getenv("TFB_WORKER_DLQ_NAME", "tfb_background_jobs_dead")
    )
    redis_url: str = field(default_factory=lambda: (os.getenv("REDIS_URL") or "").strip())
    redis_block_timeout_sec: int = field(
        default_factory=lambda: _env_int(
            "TFB_WORKER_REDIS_BLOCK_TIMEOUT_SEC", 5, lo=1, hi=60
        )
    )
    idle_sleep_sec: float = field(
        default_factory=lambda: _env_float(
            "TFB_WORKER_IDLE_SLEEP_SEC", 5.0, lo=0.5, hi=60.0
        )
    )
    max_retries: int = field(
        default_factory=lambda: _env_int("TFB_WORKER_MAX_RETRIES", 5, lo=1, hi=20)
    )
    backoff_base_sec: float = field(
        default_factory=lambda: _env_float(
            "TFB_WORKER_BACKOFF_BASE_SEC", 1.0, lo=0.1, hi=30.0
        )
    )
    backoff_max_sec: float = field(
        default_factory=lambda: _env_float(
            "TFB_WORKER_BACKOFF_MAX_SEC", 30.0, lo=1.0, hi=300.0
        )
    )
    cpu_workers: int = field(
        default_factory=lambda: _env_int("TFB_WORKER_CPU_WORKERS", 4, lo=1, hi=32)
    )
    fail_fast_if_no_redis: bool = field(
        default_factory=lambda: _env_bool("TFB_WORKER_FAIL_FAST_NO_REDIS", False)
    )


CONFIG = WorkerConfig()


# =============================================================================
# Lazy CPU executor (v4.3.0: was module-level eager)
# =============================================================================
_CPU_EXECUTOR: Optional[concurrent.futures.ThreadPoolExecutor] = None


def _get_executor() -> concurrent.futures.ThreadPoolExecutor:
    global _CPU_EXECUTOR
    if _CPU_EXECUTOR is None:
        _CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
            max_workers=CONFIG.cpu_workers, thread_name_prefix="TFBWorker"
        )
    return _CPU_EXECUTOR


def _shutdown_executor() -> None:
    global _CPU_EXECUTOR
    if _CPU_EXECUTOR is None:
        return
    try:
        _CPU_EXECUTOR.shutdown(wait=True, cancel_futures=True)
    except TypeError:
        # Python 3.8 has no cancel_futures
        try:
            _CPU_EXECUTOR.shutdown(wait=True)
        except Exception:
            pass
    except Exception:
        pass
    finally:
        _CPU_EXECUTOR = None


# =============================================================================
# Full-jitter exponential backoff
# =============================================================================
class FullJitterBackoff:
    def __init__(
        self, max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 30.0
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    async def execute_async(
        self, func: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
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
                logger.warning(
                    "Task failed on attempt %s/%s: %s. Retrying in %.2fs",
                    attempt + 1,
                    self.max_retries,
                    e,
                    sleep_time,
                )
                await asyncio.sleep(sleep_time)
        raise last_exc if last_exc is not None else RuntimeError("backoff_exhausted")


# =============================================================================
# Generic import / execution helpers (legacy — used only by refresh_data path)
# =============================================================================
def _import_first(candidates: Tuple[str, ...]) -> Any:
    last_exc: Optional[Exception] = None
    for name in candidates:
        try:
            return importlib.import_module(name)
        except Exception as exc:
            last_exc = exc
    if last_exc is not None:
        raise last_exc
    raise ImportError("No module candidates provided")


async def _run_sync_in_pool(
    fn: Callable[..., Any], *args: Any, **kwargs: Any
) -> Any:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _get_executor(), lambda: fn(*args, **kwargs)
    )


async def _maybe_call(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if asyncio.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    result = await _run_sync_in_pool(fn, *args, **kwargs)
    if asyncio.iscoroutine(result):
        return await result
    return result


async def _call_first_callable(
    module: Any, names: Tuple[str, ...], *args: Any, **kwargs: Any
) -> Tuple[bool, Any]:
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
# v4.3.0: dashboard_sync and market_scan use the canonical
# run_from_worker_payload_async(payload) entrypoint that the respective
# scripts now own. This collapses the worker's payload-parsing duplication
# and ensures new fields added to payloads don't require worker updates.
# Legacy name-probing retained for refresh_data (no worker entrypoint yet).


async def _run_dashboard_sync_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """v4.3.0: delegates to run_dashboard_sync.run_from_worker_payload_async."""
    try:
        mod = _import_first(("scripts.run_dashboard_sync", "run_dashboard_sync"))
    except Exception:
        logger.exception("run_dashboard_sync module not found")
        return {
            "ok": False,
            "error": "module_not_found",
            "task_type": "dashboard_sync",
        }

    # Ensure task_type is set (v6.5.0 validates this)
    payload = dict(payload)
    payload["task_type"] = "dashboard_sync"

    # Preferred: canonical worker entrypoint
    worker_fn = getattr(mod, "run_from_worker_payload_async", None)
    if callable(worker_fn):
        try:
            result = await worker_fn(payload)
            return _normalize_worker_result(result, "dashboard_sync")
        except Exception as e:
            logger.exception("run_dashboard_sync worker entrypoint failed: %s", e)
            return {
                "ok": False,
                "error": f"entrypoint_failed:{e}",
                "task_type": "dashboard_sync",
            }

    # Legacy fallback: name-probe (for pre-v6.5 run_dashboard_sync)
    sheet_name = str(payload.get("sheet_name") or "").strip()
    symbols = _as_list(payload.get("symbols") or [])
    spreadsheet_id = str(
        payload.get("spreadsheet_id") or os.getenv("DEFAULT_SPREADSHEET_ID", "")
    ).strip()
    limit = _safe_int(payload.get("limit") or 800, 800, lo=1, hi=50000)

    kwargs: Dict[str, Any] = {"spreadsheet_id": spreadsheet_id, "limit": limit}
    if sheet_name:
        kwargs["sheet_name"] = sheet_name
        kwargs["keys"] = [sheet_name]
    if symbols:
        kwargs["symbols"] = symbols

    found, result = await _call_first_callable(
        mod,
        (
            "run_async",
            "run_dashboard_sync_async",
            "main_async",
            "run",
            "run_dashboard_sync",
            "main",
        ),
        **kwargs,
    )
    if not found:
        logger.error(
            "run_dashboard_sync: no callable entrypoint found "
            "(tried run_from_worker_payload_async + legacy names)"
        )
        return {
            "ok": False,
            "error": "no_callable_found",
            "task_type": "dashboard_sync",
        }
    return _result_dict(result)


async def _run_market_scan_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """v4.3.0: delegates to run_market_scan.run_from_worker_payload_async."""
    try:
        mod = _import_first(("scripts.run_market_scan", "run_market_scan"))
    except Exception:
        logger.exception("run_market_scan module not found")
        return {
            "ok": False,
            "error": "module_not_found",
            "task_type": "market_scan",
        }

    # Ensure task_type is set (v5.3.0 validates this)
    payload = dict(payload)
    payload["task_type"] = "market_scan"

    # Preferred: canonical worker entrypoint.
    # v5.3.0 fixes: ScanConfig now has timeout_sec + concurrency fields,
    # and ScanConfig.from_worker_payload() handles all normalization. By
    # delegating the whole payload we avoid v4.2.0's bug where the worker
    # built ScanConfig() manually and missed these fields.
    worker_fn = getattr(mod, "run_from_worker_payload_async", None)
    if callable(worker_fn):
        try:
            result = await worker_fn(payload)
            return _normalize_worker_result(result, "market_scan")
        except Exception as e:
            logger.exception("run_market_scan worker entrypoint failed: %s", e)
            return {
                "ok": False,
                "error": f"entrypoint_failed:{e}",
                "task_type": "market_scan",
            }

    # Legacy fallback: use ScanConfig + run_scan
    scan_config_cls = getattr(mod, "ScanConfig", None)
    if scan_config_cls is not None:
        spreadsheet_id = str(
            payload.get("spreadsheet_id")
            or os.getenv("SCAN_SHEET_ID")
            or os.getenv("DEFAULT_SPREADSHEET_ID", "")
        ).strip()
        keys = _as_list(
            payload.get("keys") or payload.get("sheet_names") or ["Market_Leaders"]
        )
        # Prefer from_worker_payload classmethod if exposed (v5.3.0+)
        from_payload = getattr(scan_config_cls, "from_worker_payload", None)
        if callable(from_payload):
            try:
                cfg = from_payload(payload)
            except Exception as e:
                logger.exception("ScanConfig.from_worker_payload failed: %s", e)
                return {
                    "ok": False,
                    "error": f"scan_config_build_failed:{e}",
                    "task_type": "market_scan",
                }
        else:
            # Pre-v5.3 ScanConfig signature (no timeout_sec/concurrency)
            cfg = scan_config_cls(
                spreadsheet_id=spreadsheet_id,
                keys=keys,
                top_n=_safe_int(
                    payload.get("limit") or payload.get("top_n") or 10, 10, lo=1, hi=1000
                ),
                horizon=str(payload.get("horizon") or "3m").strip().lower(),
                min_confidence=_safe_float(
                    payload.get("min_confidence") or 0.65, 0.65, lo=0.0, hi=1.0
                ),
                max_risk_score=_safe_float(
                    payload.get("max_risk") or 60.0, 60.0, lo=0.0, hi=100.0
                ),
                min_expected_roi=_safe_float(
                    payload.get("min_roi") or 0.0, 0.0, lo=-1000.0, hi=1000.0
                ),
                mode=str(payload.get("mode") or "engine").strip(),
            )

        found, result = await _call_first_callable(
            mod,
            ("run_scan_async", "run_async", "main_async", "run_scan", "run", "main"),
            cfg,
        )
        if not found:
            logger.error("run_market_scan: no callable entrypoint found")
            return {
                "ok": False,
                "error": "no_callable_found",
                "task_type": "market_scan",
            }
        if isinstance(result, tuple) and len(result) == 2:
            rows, meta = result
            return {"ok": True, "rows": len(rows or []), "meta": meta}
        return _result_dict(result)

    logger.error("run_market_scan: no ScanConfig class found")
    return {
        "ok": False,
        "error": "scan_config_not_found",
        "task_type": "market_scan",
    }


async def _run_refresh_data_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    v4.3.0: refresh_data v5.2.0 does NOT yet expose a worker entrypoint, so
    this dispatcher retains the legacy name-probe pattern. When refresh_data
    adds run_from_worker_payload_async, prefer it here like the other two.
    """
    try:
        mod = _import_first(("scripts.refresh_data", "refresh_data"))
    except Exception:
        logger.exception("refresh_data module not found")
        return {
            "ok": False,
            "error": "module_not_found",
            "task_type": "refresh_data",
        }

    # v4.3.0 future-proof: prefer canonical entrypoint if it appears
    worker_fn = getattr(mod, "run_from_worker_payload_async", None)
    if callable(worker_fn):
        payload = dict(payload)
        payload["task_type"] = "refresh_data"
        try:
            result = await worker_fn(payload)
            return _normalize_worker_result(result, "refresh_data")
        except Exception as e:
            logger.exception("refresh_data worker entrypoint failed: %s", e)
            return {
                "ok": False,
                "error": f"entrypoint_failed:{e}",
                "task_type": "refresh_data",
            }

    kwargs: Dict[str, Any] = {}
    sheet_name = str(payload.get("sheet_name") or "").strip()
    symbols = _as_list(payload.get("symbols") or [])
    if sheet_name:
        kwargs["sheet_name"] = sheet_name
    if symbols:
        kwargs["symbols"] = symbols

    found, result = await _call_first_callable(
        mod, ("run_refresh", "run_async", "main_async", "run", "main"), **kwargs
    )
    if not found:
        logger.error("refresh_data: no callable entrypoint found")
        return {
            "ok": False,
            "error": "no_callable_found",
            "task_type": "refresh_data",
        }
    return _result_dict(result)


def _normalize_worker_result(result: Any, task_type: str) -> Dict[str, Any]:
    """
    v4.3.0: normalize result from canonical run_from_worker_payload_async.
    These entrypoints return {status, exit_code, summary, errors, task_id}.
    Map to the worker's internal {ok, ...} shape.
    """
    if not isinstance(result, dict):
        return {"ok": True, "result": str(result), "task_type": task_type}

    out = dict(result)
    out.setdefault("task_type", task_type)

    # Translate status/exit_code -> ok if not already set
    if "ok" not in out:
        status = str(out.get("status") or "").strip().lower()
        exit_code = out.get("exit_code", 0)
        if status == "success" or exit_code == 0:
            out["ok"] = True
        elif status in {"failed", "error"}:
            out["ok"] = False
        elif status == "partial":
            # "partial" is a soft failure — the worker treats as ok=True
            # so retries aren't triggered for already-partial results
            out["ok"] = True
        else:
            # Be conservative: exit_code determines
            out["ok"] = int(exit_code or 0) == 0

    return out


# =============================================================================
# Task processing
# =============================================================================
async def _process_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    task_id = str(payload.get("task_id") or "unknown")
    task_type = str(payload.get("task_type") or "dashboard_sync").strip().lower()
    started = time.time()

    with TraceContext(
        "worker_process_task", {"task_id": task_id, "task_type": task_type}
    ):
        logger.info("[%s] Starting task type=%r", task_id, task_type)
        if task_type == "dashboard_sync":
            result = await _run_dashboard_sync_task(payload)
        elif task_type == "market_scan":
            result = await _run_market_scan_task(payload)
        elif task_type == "refresh_data":
            result = await _run_refresh_data_task(payload)
        else:
            logger.warning(
                "[%s] Unknown task_type=%r. Supported: dashboard_sync, "
                "market_scan, refresh_data",
                task_id,
                task_type,
            )
            return {"ok": False, "error": "unknown_task_type", "task_type": task_type}

        elapsed = round(time.time() - started, 2)
        ok = bool(result.get("ok", True)) if isinstance(result, dict) else True
        if ok:
            logger.info(
                "[%s] Task type=%r completed in %ss. result=%s",
                task_id,
                task_type,
                elapsed,
                json_dumps(result),
            )
        else:
            logger.error(
                "[%s] Task type=%r failed in %ss. result=%s",
                task_id,
                task_type,
                elapsed,
                json_dumps(result),
            )
            raise RuntimeError(json_dumps(result))
        return _result_dict(result)


# =============================================================================
# Redis helpers
# =============================================================================
async def _create_redis_client(redis_url: str) -> Any:
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
    # Try aclose() first (redis-py >= 4.2)
    try:
        close_fn = getattr(client, "aclose", None)
        if callable(close_fn):
            await close_fn()
            return
    except Exception:
        pass
    # Fall back to close()
    try:
        close_fn = getattr(client, "close", None)
        if callable(close_fn):
            result = close_fn()
            if asyncio.iscoroutine(result):
                await result
    except Exception:
        pass
    # aioredis compat
    try:
        wait_closed = getattr(client, "wait_closed", None)
        if callable(wait_closed):
            result = wait_closed()
            if asyncio.iscoroutine(result):
                await result
    except Exception:
        pass


async def _push_dead_letter(
    client: Any, raw_payload: str, error_text: str
) -> None:
    if client is None or not CONFIG.dead_letter_queue:
        return
    envelope = {
        "failed_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "error": error_text,
        "raw_payload": raw_payload,
        "worker_version": SCRIPT_VERSION,
    }
    try:
        await client.rpush(CONFIG.dead_letter_queue, json_dumps(envelope))
    except Exception:
        logger.exception("Failed to push payload to dead-letter queue")


async def _poll_redis_queue(redis_url: str, shutdown: asyncio.Event) -> None:
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
        while not shutdown.is_set():
            try:
                result = await redis_client.blpop(
                    CONFIG.queue_name, timeout=CONFIG.redis_block_timeout_sec
                )
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
                    logger.error(
                        "Failed to parse queue payload: %s | raw=%r",
                        parse_err,
                        str(data_raw)[:300],
                    )
                    await _push_dead_letter(
                        redis_client, str(data_raw), f"parse_error:{parse_err}"
                    )
                    continue

                try:
                    await backoff.execute_async(_process_payload, payload)
                except asyncio.CancelledError:
                    raise
                except Exception as exec_err:
                    logger.exception("Task exhausted retries: %s", exec_err)
                    await _push_dead_letter(
                        redis_client, str(data_raw), f"task_failed:{exec_err}"
                    )
            except asyncio.CancelledError:
                raise
            except Exception as poll_err:
                logger.error("Queue polling error: %s", poll_err)
                await asyncio.sleep(2)
    finally:
        await _close_redis_client(redis_client)
        logger.info("Redis connection closed")


async def _idle_loop(shutdown: asyncio.Event) -> None:
    while not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=CONFIG.idle_sleep_sec)
        except asyncio.TimeoutError:
            continue


# =============================================================================
# Main entry
# =============================================================================
async def main_async(args: argparse.Namespace) -> int:
    del args
    logger.info(
        "🚀 Starting TFB Worker v%s | redis_lib=%s | has_orjson=%s "
        "| queue=%s | dlq=%s | cpu_workers=%d",
        SCRIPT_VERSION,
        _REDIS_LIB,
        _HAS_ORJSON,
        CONFIG.queue_name,
        CONFIG.dead_letter_queue,
        CONFIG.cpu_workers,
    )

    # v4.3.0: create the shutdown event INSIDE the running loop, not at
    # module import time (which could bind to a different loop).
    shutdown = asyncio.Event()

    # v4.3.0: use loop signal handlers where possible; fall back to
    # signal.signal() on platforms that don't support add_signal_handler
    # (e.g. Windows).
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except (NotImplementedError, RuntimeError, AttributeError):
            # Windows / non-main-thread — fall back to signal.signal()
            try:
                signal.signal(sig, lambda *_: shutdown.set())
            except Exception:
                pass

    tasks: List[asyncio.Task] = []
    if _REDIS_AVAILABLE and CONFIG.redis_url:
        logger.info("Redis available. Polling queue=%s", CONFIG.queue_name)
        tasks.append(
            asyncio.create_task(
                _poll_redis_queue(CONFIG.redis_url, shutdown), name="redis-poller"
            )
        )
    else:
        message = (
            "Redis not available or REDIS_URL is empty. Worker is in idle mode."
        )
        if CONFIG.fail_fast_if_no_redis:
            logger.error(message)
            return 2
        logger.warning(message)
        tasks.append(
            asyncio.create_task(_idle_loop(shutdown), name="idle-loop")
        )

    await shutdown.wait()
    logger.info("Shutdown signal received. Draining tasks...")

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    _shutdown_executor()
    logger.info("Worker stopped cleanly")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description=f"Tadawul Distributed Worker v{SCRIPT_VERSION}"
    )
    args = parser.parse_args()

    try:
        raise SystemExit(asyncio.run(main_async(args)))
    except KeyboardInterrupt:
        _shutdown_executor()
        raise SystemExit(130)
    except SystemExit:
        _shutdown_executor()
        raise
    except Exception as e:
        logger.exception("Fatal worker crash: %s", e)
        _shutdown_executor()
        raise SystemExit(1)


__all__ = [
    "SCRIPT_VERSION",
    "SERVICE_VERSION",
    "WorkerConfig",
    "CONFIG",
    "FullJitterBackoff",
    "TraceContext",
    "main_async",
    "main",
]


if __name__ == "__main__":
    main()
