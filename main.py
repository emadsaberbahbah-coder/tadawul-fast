#!/usr/bin/env python3
# main.py
"""
main.py
------------------------------------------------------------
Tadawul Fast Bridge – FastAPI Entry Point (v6.6.1)
Mission Critical Edition — OBSERVE + PROTECT + SCALE (Render-Optimized)

🚨 IMPORTANT FIX (your Render crash):
- Handles LOG_FORMAT values like "detailed" safely.
  If LOG_FORMAT is a preset name (detailed/compact/simple/json) OR an invalid format,
  we auto-fallback to a safe default and DO NOT crash at import-time.

Key env knobs (aligned with your Render keys)
- LOG_LEVEL, LOG_JSON, LOG_FORMAT, LOG_DATEFMT
- ENABLE_RATE_LIMITING, MAX_REQUESTS_PER_MINUTE
- ENABLE_SWAGGER, ENABLE_REDOC
- ENABLE_CORS_ALL_ORIGINS, CORS_ORIGINS
- ALLOWED_HOSTS
- INIT_ENGINE_ON_BOOT
- DEFER_ROUTER_MOUNT
- APP_NAME, APP_TITLE, SERVICE_NAME, APP_VERSION, APP_ENV, ENVIRONMENT
"""

from __future__ import annotations

import asyncio
import contextvars
import gc
import inspect
import json
import logging
import os
import sys
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from importlib import import_module
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.routing import Route

# Optional: Rate limiting (slowapi)
try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.errors import RateLimitExceeded
    from slowapi.util import get_remote_address

    HAS_SLOWAPI = True
except Exception:
    HAS_SLOWAPI = False

# Optional: psutil (memory/cpu)
try:
    import psutil  # type: ignore

    _PSUTIL = psutil
except Exception:
    _PSUTIL = None


# -----------------------------------------------------------------------------
# Path safety
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

APP_ENTRY_VERSION = "6.6.1"


# -----------------------------------------------------------------------------
# Helpers (env coercion)
# -----------------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok", "active"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}


def _strip(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def _truthy(v: Any, default: bool = False) -> bool:
    s = _strip(v).lower()
    if not s:
        return default
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _clamp_str(s: Any, max_len: int = 5000) -> str:
    txt = _strip(s)
    if len(txt) <= max_len:
        return txt
    return txt[: max_len - 15] + " ...TRUNCATED"


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _now_riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat(timespec="seconds")


def _env_name() -> str:
    return (_strip(os.getenv("APP_ENV") or os.getenv("ENVIRONMENT") or "production")).lower()


def _service_name() -> str:
    return _strip(os.getenv("APP_TITLE") or os.getenv("APP_NAME") or os.getenv("SERVICE_NAME") or "Tadawul Fast Bridge")


def _service_version() -> str:
    # keep compatibility with your render.yaml: APP_VERSION is set there
    return _strip(os.getenv("SERVICE_VERSION") or os.getenv("APP_VERSION") or APP_ENTRY_VERSION)


def _safe_import(mod_path: str) -> Optional[Any]:
    try:
        return import_module(mod_path)
    except Exception as e:
        logging.getLogger("boot").debug("Optional module skipped: %s (%s)", mod_path, e)
        return None


def _read_proc_status_rss_mb() -> float:
    # Linux-only RSS parse (fallback when psutil is missing)
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    kb = float(parts[1])  # VmRSS: <kB>
                    return kb / 1024.0
    except Exception:
        pass
    return 0.0


def _rss_mb() -> float:
    if _PSUTIL is not None:
        try:
            p = _PSUTIL.Process(os.getpid())
            return float(p.memory_info().rss) / 1024.0 / 1024.0
        except Exception:
            pass
    return _read_proc_status_rss_mb()


def _cpu_pct() -> float:
    if _PSUTIL is not None:
        try:
            p = _PSUTIL.Process(os.getpid())
            return float(p.cpu_percent(interval=None))
        except Exception:
            pass
    return 0.0


# -----------------------------------------------------------------------------
# Request correlation (contextvars)
# -----------------------------------------------------------------------------
_request_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="SYSTEM")


def _get_request_id() -> str:
    try:
        return _request_id_ctx.get()
    except Exception:
        return "SYSTEM"


# -----------------------------------------------------------------------------
# Logging (text or JSON) — SAFE vs LOG_FORMAT="detailed"
# -----------------------------------------------------------------------------
class _RequestIDFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = getattr(record, "request_id", None) or _get_request_id()
        return True


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "lvl": record.levelname,
            "logger": record.name,
            "request_id": getattr(record, "request_id", None) or _get_request_id(),
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


_DEFAULT_FMT_DETAILED = "%(asctime)s | %(levelname)s | [%(request_id)s] %(name)s | %(message)s"
_DEFAULT_FMT_COMPACT = "%(asctime)s | %(levelname)s | %(message)s"
_DEFAULT_FMT_SIMPLE = "%(levelname)s | %(message)s"


def _resolve_text_formatter(fmt_raw: str, datefmt: str) -> logging.Formatter:
    """
    Never raises. If fmt_raw is invalid (like 'detailed'), falls back safely.
    Supports presets + real %-style format strings.
    """
    raw = (fmt_raw or "").strip()
    low = raw.lower()

    # Presets (this is what prevents your crash)
    if low in {"detailed", "detail", "default", "prod", "production"} or not raw:
        fmt = _DEFAULT_FMT_DETAILED
    elif low in {"compact", "short"}:
        fmt = _DEFAULT_FMT_COMPACT
    elif low in {"simple"}:
        fmt = _DEFAULT_FMT_SIMPLE
    elif low in {"json"}:
        # caller should have chosen JSON formatter; still safe fallback
        fmt = _DEFAULT_FMT_DETAILED
    else:
        fmt = raw

    # Validate: must contain at least one %(...)s token, otherwise it's likely invalid
    # (but we still try in case user intentionally wants constant string)
    try:
        return logging.Formatter(fmt, datefmt=datefmt)
    except Exception:
        # ultimate fallback (never crash import)
        return logging.Formatter(_DEFAULT_FMT_DETAILED, datefmt=datefmt)


def _configure_logging() -> None:
    lvl = _strip(os.getenv("LOG_LEVEL") or "INFO").upper()
    json_mode = _truthy(os.getenv("LOG_JSON"), default=False)

    fmt_raw = _strip(os.getenv("LOG_FORMAT"))  # may be "detailed" in Render => now safe
    datefmt = _strip(os.getenv("LOG_DATEFMT")) or "%H:%M:%S"

    root = logging.getLogger()
    root.setLevel(getattr(logging, lvl, logging.INFO))

    # If handlers already exist (uvicorn), update them; else add one
    if not root.handlers:
        root.addHandler(logging.StreamHandler())

    for h in root.handlers:
        h.addFilter(_RequestIDFilter())
        if json_mode:
            h.setFormatter(_JsonFormatter())
        else:
            h.setFormatter(_resolve_text_formatter(fmt_raw, datefmt=datefmt))


_configure_logging()
logger = logging.getLogger("main")
boot_logger = logging.getLogger("boot")


# -----------------------------------------------------------------------------
# System Guardian (event-loop lag + memory pressure + degraded mode)
# -----------------------------------------------------------------------------
class SystemGuardian:
    def __init__(self) -> None:
        self.running: bool = False
        self.degraded_mode: bool = False

        self.latency_ms: float = 0.0
        self.memory_mb: float = 0.0
        self.cpu_pct: float = 0.0

        self.last_gc_ts: float = 0.0
        self.boot_ts_utc: str = _now_utc_iso()
        self.boot_ts_riyadh: str = _now_riyadh_iso()

    def get_load_status(self) -> str:
        if self.latency_ms > 300:
            return "overloaded"
        if self.latency_ms > 100:
            return "heavy"
        return "nominal"

    async def start(self) -> None:
        self.running = True
        self.last_gc_ts = time.time()
        boot_logger.info("🛡️ System Guardian started. TZ=Asia/Riyadh")
        _ = _cpu_pct()

        while self.running:
            t0 = time.perf_counter()
            await asyncio.sleep(2.0)
            lag = (time.perf_counter() - t0 - 2.0) * 1000.0
            self.latency_ms = max(0.0, lag)

            self.memory_mb = _rss_mb()
            self.cpu_pct = _cpu_pct()

            gc_threshold = float(_strip(os.getenv("GC_PRESSURE_MB")) or "420")
            gc_cooldown = float(_strip(os.getenv("GC_COOLDOWN_SEC")) or "60")

            if self.memory_mb >= gc_threshold and (time.time() - self.last_gc_ts) >= gc_cooldown:
                boot_logger.warning(
                    "Memory pressure: %.1fMB (>=%.0f). Triggering gc.collect().",
                    self.memory_mb,
                    gc_threshold,
                )
                gc.collect()
                self.last_gc_ts = time.time()

            enter_ms = float(_strip(os.getenv("DEGRADED_ENTER_LAG_MS")) or "500")
            exit_ms = float(_strip(os.getenv("DEGRADED_EXIT_LAG_MS")) or "150")

            if self.latency_ms >= enter_ms and not self.degraded_mode:
                self.degraded_mode = True
                boot_logger.warning("Entering degraded mode (loop lag %.1fms).", self.latency_ms)
            elif self.latency_ms <= exit_ms and self.degraded_mode:
                self.degraded_mode = False
                boot_logger.info("Exiting degraded mode (loop lag %.1fms).", self.latency_ms)

    def stop(self) -> None:
        self.running = False


guardian = SystemGuardian()


# -----------------------------------------------------------------------------
# Middleware
# -----------------------------------------------------------------------------
class TelemetryMiddleware(BaseHTTPMiddleware):
    """
    - Request ID correlation (X-Request-ID)
    - Performance headers
    - Security headers
    - Optional overload shedding (DEGRADED_SHED_PATHS)
    """

    async def dispatch(self, request: Request, call_next: Callable):
        rid = request.headers.get("X-Request-ID") or (str(uuid.uuid4())[:18])
        token = _request_id_ctx.set(rid)
        request.state.request_id = rid

        t0 = time.perf_counter()

        try:
            shed_enabled = _truthy(os.getenv("DEGRADED_SHED_ENABLED"), default=True)
            shed_paths = _strip(os.getenv("DEGRADED_SHED_PATHS") or "/v1/analysis,/v1/advanced,/v1/advisor").split(",")

            if shed_enabled and guardian.degraded_mode:
                p = request.url.path or ""
                if any(p.startswith(x.strip()) for x in shed_paths if x.strip()):
                    return JSONResponse(
                        status_code=503,
                        content={
                            "status": "degraded",
                            "message": "System under load. Please retry shortly.",
                            "request_id": rid,
                            "load": guardian.get_load_status(),
                            "time_riyadh": _now_riyadh_iso(),
                        },
                    )

            response = await call_next(request)
            return response

        finally:
            # Always attach headers if we have a response object
            # (Starlette will ignore if response wasn't created)
            dt = time.perf_counter() - t0
            try:
                # If response exists in local scope, set headers
                if "response" in locals() and locals()["response"] is not None:
                    resp = locals()["response"]
                    resp.headers["X-Request-ID"] = rid
                    resp.headers["X-Process-Time"] = f"{dt:.4f}s"
                    resp.headers["X-System-Load"] = guardian.get_load_status()

                    resp.headers["X-Content-Type-Options"] = "nosniff"
                    resp.headers["X-Frame-Options"] = "DENY"
                    resp.headers["Referrer-Policy"] = "no-referrer"
                    resp.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
            except Exception:
                pass

            _request_id_ctx.reset(token)


# -----------------------------------------------------------------------------
# Engine bootstrap (resilient)
# -----------------------------------------------------------------------------
async def _init_engine_resilient(app_: FastAPI, max_retries: int = 3) -> None:
    """
    Boots core.data_engine_v2.get_engine() with backoff.
    Never crashes the server; marks state.engine_ready accordingly.
    """
    app_.state.engine_ready = False
    app_.state.engine_error = None

    for attempt in range(1, max_retries + 1):
        try:
            mod = _safe_import("core.data_engine_v2")
            if not mod or not hasattr(mod, "get_engine"):
                app_.state.engine_error = "Engine module not available."
                boot_logger.warning("Engine module not available.")
                return

            get_engine = getattr(mod, "get_engine")
            eng = await get_engine() if inspect.iscoroutinefunction(get_engine) else get_engine()

            if eng:
                app_.state.engine = eng
                app_.state.engine_ready = True
                app_.state.engine_error = None
                boot_logger.info("Data Engine ready: %s", type(eng).__name__)
                return

            raise RuntimeError("Engine returned None")
        except Exception as e:
            boot_logger.warning("Engine boot attempt %d/%d failed: %s", attempt, max_retries, e)
            if attempt < max_retries:
                await asyncio.sleep(1.5 * attempt)

    app_.state.engine_ready = False
    app_.state.engine_error = "CRITICAL: Engine failed to initialize."
    boot_logger.error("%s", app_.state.engine_error)


# -----------------------------------------------------------------------------
# Router mounting (priority + resilience)
# -----------------------------------------------------------------------------
def _mount_routers(app_: FastAPI) -> Dict[str, Any]:
    """
    Mounts routers in priority order. Does not crash if any router fails.
    Returns mount report for /health.
    """
    router_plan: List[Tuple[str, List[str]]] = [
        ("Advanced", ["routes.advanced_analysis", "routes.advanced"]),
        ("Advisor", ["routes.investment_advisor", "routes.advisor"]),
        ("KSA", ["routes.routes_argaam", "routes.argaam"]),
        ("Enriched", ["routes.enriched_quote", "routes.enriched"]),
        ("Analysis", ["routes.ai_analysis", "routes.analysis"]),
        ("System", ["routes.config", "routes.system"]),
    ]

    report: Dict[str, Any] = {"mounted": [], "failed": []}
    for label, candidates in router_plan:
        mounted = False
        last_err = ""
        for mod_path in candidates:
            mod = _safe_import(mod_path)
            if not mod:
                continue
            router_obj = getattr(mod, "router", None) or getattr(mod, "api_router", None)
            if not router_obj:
                continue
            try:
                app_.include_router(router_obj)
                boot_logger.info("✅ Mounted %s via %s", label, mod_path)
                report["mounted"].append({"label": label, "module": mod_path})
                mounted = True
                break
            except Exception as e:
                last_err = str(e)
                boot_logger.error("Failed to mount %s via %s: %s", label, mod_path, e)

        if not mounted:
            boot_logger.warning("⚠️ Router '%s' not mounted. (%s)", label, last_err or "no module found")
            report["failed"].append({"label": label, "error": last_err or "not found"})

    return report


def _route_inventory(app_: FastAPI) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in app_.routes:
        if isinstance(r, Route):
            out.append({"path": r.path, "methods": sorted(list(r.methods or []))})
    return out


# -----------------------------------------------------------------------------
# Settings masking (optional config.py integration)
# -----------------------------------------------------------------------------
def _masked_settings() -> Dict[str, Any]:
    mod = _safe_import("config") or _safe_import("core.config")
    if mod and hasattr(mod, "mask_settings_dict"):
        try:
            return mod.mask_settings_dict()  # type: ignore
        except Exception:
            pass

    envmod = _safe_import("env")
    if envmod and hasattr(envmod, "safe_env_summary"):
        try:
            return envmod.safe_env_summary()  # type: ignore
        except Exception:
            pass

    return {
        "app": {"name": _service_name(), "version": _service_version(), "env": _env_name()},
        "auth": {"tokens_present": bool(_strip(os.getenv("APP_TOKEN") or "") or _strip(os.getenv("BACKUP_APP_TOKEN") or ""))},
        "logging": {
            "log_level": _strip(os.getenv("LOG_LEVEL") or "INFO"),
            "log_json": _truthy(os.getenv("LOG_JSON"), default=False),
            "log_format": _strip(os.getenv("LOG_FORMAT") or ""),
        },
    }


# -----------------------------------------------------------------------------
# App Factory
# -----------------------------------------------------------------------------
def create_app() -> FastAPI:
    app_env = _env_name()
    title = _service_name()
    version = APP_ENTRY_VERSION

    enable_docs = _truthy(os.getenv("ENABLE_SWAGGER"), default=True)
    enable_redoc = _truthy(os.getenv("ENABLE_REDOC"), default=True)

    docs_url = "/docs" if enable_docs else None
    redoc_url = "/redoc" if enable_redoc else None

    limiter = None
    if HAS_SLOWAPI and _truthy(os.getenv("ENABLE_RATE_LIMITING"), default=True):
        rpm = int(float(_strip(os.getenv("MAX_REQUESTS_PER_MINUTE") or "240")))
        limiter = Limiter(key_func=get_remote_address, default_limits=[f"{rpm} per minute"])

    @asynccontextmanager
    async def lifespan(app_: FastAPI):
        app_.state.boot_time_utc = _now_utc_iso()
        app_.state.boot_time_riyadh = _now_riyadh_iso()
        app_.state.boot_completed = False
        app_.state.engine_ready = False
        app_.state.engine_error = None
        app_.state.router_report = {"mounted": [], "failed": []}
        app_.state.routes_snapshot = []

        guardian_task = asyncio.create_task(guardian.start())

        async def _boot():
            await asyncio.sleep(0)

            defer_mount = _truthy(os.getenv("DEFER_ROUTER_MOUNT"), default=False)
            if not defer_mount:
                app_.state.router_report = _mount_routers(app_)
                app_.state.routes_snapshot = _route_inventory(app_)
                boot_logger.info("Route inventory loaded (%d routes).", len(app_.state.routes_snapshot))
            else:
                boot_logger.warning("DEFER_ROUTER_MOUNT=1 -> routers will NOT be mounted at boot.")

            if _truthy(os.getenv("INIT_ENGINE_ON_BOOT"), default=True):
                maxr = int(float(_strip(os.getenv("MAX_RETRIES") or "3")))
                await _init_engine_resilient(app_, max_retries=maxr)

            gc.collect()
            app_.state.boot_completed = True
            boot_logger.info("🚀 Ready v%s | env=%s | mem=%.1fMB | tz=Asia/Riyadh", version, app_env, guardian.memory_mb)

        boot_task = asyncio.create_task(_boot())

        yield

        boot_logger.info("Shutdown initiated.")
        boot_task.cancel()
        guardian.stop()
        try:
            await asyncio.wait_for(guardian_task, timeout=3.0)
        except Exception:
            pass

        eng = getattr(app_.state, "engine", None)
        if eng is not None:
            try:
                if hasattr(eng, "aclose") and inspect.iscoroutinefunction(eng.aclose):
                    await asyncio.wait_for(eng.aclose(), timeout=5.0)
                elif hasattr(eng, "close"):
                    eng.close()
                boot_logger.info("Engine shutdown complete.")
            except Exception as e:
                boot_logger.warning("Engine shutdown issue: %s", e)

    app = FastAPI(title=title, version=version, lifespan=lifespan, docs_url=docs_url, redoc_url=redoc_url)

    if limiter is not None:
        app.state.limiter = limiter
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

    app.add_middleware(TelemetryMiddleware)
    app.add_middleware(GZipMiddleware, minimum_size=512)

    cors_all = _truthy(os.getenv("ENABLE_CORS_ALL_ORIGINS"), default=True)
    cors_origins_raw = _strip(os.getenv("CORS_ORIGINS") or "")
    if cors_all or not cors_origins_raw:
        allow_origins = ["*"]
    else:
        allow_origins = [o.strip() for o in cors_origins_raw.split(",") if o.strip()]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=False,
    )

    if app_env == "production":
        allowed_hosts_raw = _strip(os.getenv("ALLOWED_HOSTS") or "*")
        allowed_hosts = [h.strip() for h in allowed_hosts_raw.split(",") if h.strip()] or ["*"]
        app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed_hosts)

    # -------------------------------------------------------------------------
    # System Endpoints
    # -------------------------------------------------------------------------
    @app.get("/", include_in_schema=False)
    async def root():
        return {"status": "online", "service": title, "version": version, "time_riyadh": _now_riyadh_iso()}

    @app.get("/readyz", include_in_schema=False)
    async def readiness():
        ready = bool(getattr(app.state, "boot_completed", False))
        return JSONResponse(
            status_code=200 if ready else 503,
            content={"ready": ready, "load": guardian.get_load_status(), "time_riyadh": _now_riyadh_iso()},
        )

    @app.get("/healthz", include_in_schema=False)
    async def healthz():
        return {"status": "ok", "time_utc": _now_utc_iso(), "time_riyadh": _now_riyadh_iso()}

    @app.get("/health", tags=["system"])
    async def health(request: Request):
        return {
            "status": "ok" if getattr(app.state, "engine_ready", False) else "degraded",
            "service": {"name": title, "version": version, "env": app_env},
            "vitals": {
                "memory_mb": round(guardian.memory_mb, 1),
                "cpu_pct": round(guardian.cpu_pct, 1),
                "loop_lag_ms": round(guardian.latency_ms, 1),
                "load": guardian.get_load_status(),
                "degraded_mode": bool(guardian.degraded_mode),
            },
            "boot": {
                "boot_time_utc": getattr(app.state, "boot_time_utc", None),
                "boot_time_riyadh": getattr(app.state, "boot_time_riyadh", None),
                "boot_completed": bool(getattr(app.state, "boot_completed", False)),
                "engine_ready": bool(getattr(app.state, "engine_ready", False)),
                "engine_error": getattr(app.state, "engine_error", None),
            },
            "routers": getattr(app.state, "router_report", {"mounted": [], "failed": []}),
            "time": {"utc": _now_utc_iso(), "riyadh": _now_riyadh_iso()},
            "trace_id": getattr(request.state, "request_id", "n/a"),
        }

    @app.get("/system/info", tags=["system"])
    async def system_info():
        return {"status": "ok", "settings": _masked_settings(), "time_riyadh": _now_riyadh_iso()}

    @app.post("/system/mount-routers", tags=["system"])
    async def mount_routers_now():
        report = _mount_routers(app)
        app.state.router_report = report
        app.state.routes_snapshot = _route_inventory(app)
        return {
            "status": "success",
            "mounted": report["mounted"],
            "failed": report["failed"],
            "routes": len(app.state.routes_snapshot),
        }

    # -------------------------------------------------------------------------
    # Error handling
    # -------------------------------------------------------------------------
    @app.exception_handler(StarletteHTTPException)
    async def http_exc_handler(request: Request, exc: StarletteHTTPException):
        logger.warning("HTTP error %s on %s: %s", exc.status_code, request.url.path, exc.detail)
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "status": "error",
                "message": exc.detail,
                "request_id": getattr(request.state, "request_id", "n/a"),
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_exc_handler(request: Request, exc: Exception):
        logger.error("Unhandled crash on %s: %s", request.url.path, _clamp_str(traceback.format_exc()))
        return JSONResponse(
            status_code=500,
            content={
                "status": "critical",
                "message": "System Error",
                "request_id": getattr(request.state, "request_id", "n/a"),
                "time_riyadh": _now_riyadh_iso(),
            },
        )

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn

    port = int(float(_strip(os.getenv("PORT") or "8000")))
    uvicorn.run(app, host="0.0.0.0", port=port)
