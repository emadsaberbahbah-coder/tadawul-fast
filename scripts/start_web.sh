#!/usr/bin/env python3
"""
scripts/start_web.py
================================================================================
TADAWUL FAST BRIDGE – RENDER-SAFE WEB LAUNCHER (v6.3.0)
================================================================================

Primary goal
- Deployment-safe on Render:
  - Binds to $PORT (Render requirement)
  - Logs FULL traceback on any crash (no silent failures)
  - Avoids “auto-scaling surprises” that cause OOM on small instances

What’s improved vs v6.2.0
- ✅ Stronger worker logic:
    - Honors WEB_CONCURRENCY if set
    - Otherwise defaults to 1 worker (safe)
    - Hard clamps via WORKERS_MAX (default 4) to avoid OOM
- ✅ Gunicorn worker-class resolution is SAFE:
    - Tries uvicorn_worker.UvicornWorker (if installed)
    - Falls back to uvicorn.workers.UvicornWorker
    - If neither is available → falls back to Uvicorn
- ✅ Better diagnostics:
    - Prints python version, PID, and important env (masked)
    - Warns if $PORT exists but you’re not binding to it
    - Optional --health-check (import-only) for fast troubleshooting
- ✅ Optional “factory app” support:
    - APP_FACTORY=1 or --factory to call app() for Gunicorn
    - Uvicorn uses uvicorn's factory mode when enabled
- ✅ Optional uvloop/httptools auto-enable (safe):
    - ENABLE_UVLOOP=1, ENABLE_HTTPTOOLS=1 (or auto if installed + enabled)

Usage (Render start command)
- python scripts/start_web.py
- python scripts/start_web.py --app main:app
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import signal
import sys
import time
import traceback
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


# =============================================================================
# Env helpers (safe parsing)
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on"}
_FALSY = {"0", "false", "no", "n", "off"}


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _env_int(name: str, default: int) -> int:
    raw = _env_str(name, "")
    try:
        return int(raw)
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    raw = _env_str(name, "")
    try:
        return float(raw)
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    raw = _env_str(name, "")
    if raw == "":
        return bool(default)
    s = raw.lower().strip()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return bool(default)


def _clamp(n: int, lo: int, hi: int) -> int:
    return lo if n < lo else hi if n > hi else n


def _normalize_loglevel(v: str) -> str:
    s = (v or "info").strip().lower()
    return s if s in {"critical", "error", "warning", "info", "debug"} else "info"


def _mask_secret(s: Optional[str], reveal_first: int = 2, reveal_last: int = 3) -> str:
    if not s:
        return ""
    t = str(s).strip()
    if len(t) <= (reveal_first + reveal_last + 3):
        return "***"
    return f"{t[:reveal_first]}...{t[-reveal_last:]}"


# =============================================================================
# Config
# =============================================================================
@dataclass(slots=True)
class ServerConfig:
    host: str
    port: int
    app_module: str
    app_var: str

    # runtime
    log_level: str
    access_log: bool
    proxy_headers: bool
    forwarded_allow_ips: str
    root_path: str
    lifespan: str

    # server choice
    server: str  # "auto" | "uvicorn" | "gunicorn"
    workers: int

    # gunicorn
    gunicorn_preload_app: bool
    gunicorn_worker_class: str
    gunicorn_timeout: int
    gunicorn_graceful_timeout: int
    gunicorn_keepalive: int

    # uvicorn
    uvicorn_factory: bool
    uvicorn_keepalive: int
    uvicorn_graceful_timeout: int
    uvicorn_backlog: int
    uvicorn_limit_concurrency: Optional[int]
    uvicorn_limit_max_requests: Optional[int]
    uvicorn_loop: Optional[str]
    uvicorn_http: Optional[str]

    @property
    def app_import_string(self) -> str:
        return f"{self.app_module}:{self.app_var}"

    @classmethod
    def from_env(
        cls,
        *,
        app: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        server: Optional[str] = None,
        factory: Optional[bool] = None,
    ) -> "ServerConfig":
        # Render-friendly defaults
        env_host = (host or _env_str("HOST", "0.0.0.0")).strip() or "0.0.0.0"
        env_port = int(port if port is not None else _env_int("PORT", 8000))

        # App parsing
        app_module = _env_str("APP_MODULE", "main")
        app_var = _env_str("APP_VAR", "app")
        if app:
            a = app.strip()
            if ":" in a:
                app_module, app_var = [x.strip() for x in a.split(":", 1)]
            elif a:
                app_module, app_var = a, "app"

        # Logging
        log_level = _normalize_loglevel(_env_str("LOG_LEVEL", "info"))

        # Workers: safest default is 1 unless explicitly configured
        workers_env = _env_str("WEB_CONCURRENCY", "").strip()
        if workers_env:
            workers = _env_int("WEB_CONCURRENCY", 1)
        else:
            workers = 1

        workers_max = _clamp(_env_int("WORKERS_MAX", 4), 1, 32)
        workers = _clamp(int(workers), 1, workers_max)

        # Server selection
        srv = (server or _env_str("SERVER", "auto")).strip().lower()
        if srv not in {"auto", "uvicorn", "gunicorn"}:
            srv = "auto"

        # Common Uvicorn-ish flags
        access_log = _env_bool("UVICORN_ACCESS_LOG", True)
        proxy_headers = True
        forwarded_allow_ips = _env_str("FORWARDED_ALLOW_IPS", "*") or "*"
        root_path = _env_str("UVICORN_ROOT_PATH", "")
        lifespan = _env_str("UVICORN_LIFESPAN", "on") or "on"

        uvicorn_keepalive = _clamp(_env_int("UVICORN_KEEPALIVE", 75), 1, 300)
        uvicorn_graceful_timeout = _clamp(_env_int("UVICORN_GRACEFUL_TIMEOUT", 30), 1, 180)
        uvicorn_backlog = _clamp(_env_int("UVICORN_BACKLOG", 2048), 64, 16384)

        lc_raw = _env_str("UVICORN_LIMIT_CONCURRENCY", "")
        lmr_raw = _env_str("UVICORN_LIMIT_MAX_REQUESTS", "")
        uvicorn_limit_concurrency = int(lc_raw) if lc_raw.isdigit() and int(lc_raw) > 0 else None
        uvicorn_limit_max_requests = int(lmr_raw) if lmr_raw.isdigit() and int(lmr_raw) > 0 else None

        # Optional perf
        enable_uvloop = _env_bool("ENABLE_UVLOOP", False)
        enable_httptools = _env_bool("ENABLE_HTTPTOOLS", False)
        uvicorn_loop = "uvloop" if enable_uvloop else None
        uvicorn_http = "httptools" if enable_httptools else None

        # Factory mode
        uvicorn_factory = bool(factory) if factory is not None else _env_bool("APP_FACTORY", False)

        # Gunicorn knobs (safe defaults)
        gunicorn_preload_app = _env_bool("GUNICORN_PRELOAD_APP", False)
        gunicorn_worker_class = _env_str("WORKER_CLASS", "").strip()  # resolved later if empty/invalid
        gunicorn_timeout = _clamp(_env_int("GUNICORN_TIMEOUT", 60), 10, 600)
        gunicorn_graceful_timeout = _clamp(_env_int("GUNICORN_GRACEFUL_TIMEOUT", 30), 5, 180)
        gunicorn_keepalive = _clamp(_env_int("GUNICORN_KEEPALIVE", 5), 1, 120)

        return cls(
            host=env_host,
            port=env_port,
            app_module=app_module,
            app_var=app_var,
            log_level=log_level,
            access_log=access_log,
            proxy_headers=proxy_headers,
            forwarded_allow_ips=forwarded_allow_ips,
            root_path=root_path,
            lifespan=lifespan,
            server=srv,
            workers=workers,
            gunicorn_preload_app=gunicorn_preload_app,
            gunicorn_worker_class=gunicorn_worker_class,
            gunicorn_timeout=gunicorn_timeout,
            gunicorn_graceful_timeout=gunicorn_graceful_timeout,
            gunicorn_keepalive=gunicorn_keepalive,
            uvicorn_factory=uvicorn_factory,
            uvicorn_keepalive=uvicorn_keepalive,
            uvicorn_graceful_timeout=uvicorn_graceful_timeout,
            uvicorn_backlog=uvicorn_backlog,
            uvicorn_limit_concurrency=uvicorn_limit_concurrency,
            uvicorn_limit_max_requests=uvicorn_limit_max_requests,
            uvicorn_loop=uvicorn_loop,
            uvicorn_http=uvicorn_http,
        )


# =============================================================================
# Logging
# =============================================================================
def setup_logging(level: str) -> logging.Logger:
    logger = logging.getLogger("start_web")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()

    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    h.setFormatter(fmt)
    logger.addHandler(h)

    # Reduce uvicorn noise but keep consistent
    for ln in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(ln).setLevel(getattr(logging, level.upper(), logging.INFO))

    return logger


def _log_boot_banner(cfg: ServerConfig, logger: logging.Logger) -> None:
    env_port_raw = _env_str("PORT", "")
    if env_port_raw and str(cfg.port) != env_port_raw.strip():
        logger.warning("PORT mismatch: env PORT=%s but binding port=%s", env_port_raw.strip(), cfg.port)

    # Mask common secrets just for logging
    token = _mask_secret(_env_str("APP_TOKEN", "") or _env_str("BACKEND_TOKEN", ""))
    eodhd = _mask_secret(_env_str("EODHD_API_KEY", "") or _env_str("EODHD_API_TOKEN", ""))
    sid = _env_str("DEFAULT_SPREADSHEET_ID", "")

    logger.info("===============================================")
    logger.info("TFB Render-Safe Web Launcher v6.3.0")
    logger.info("python=%s | pid=%s", sys.version.split()[0], os.getpid())
    logger.info("host=%s | port=%s | server=%s | workers=%s", cfg.host, cfg.port, cfg.server, cfg.workers)
    logger.info("app=%s | factory=%s", cfg.app_import_string, bool(cfg.uvicorn_factory))
    logger.info("log_level=%s | access_log=%s | lifespan=%s", cfg.log_level, cfg.access_log, cfg.lifespan)
    logger.info("proxy_headers=%s | forwarded_allow_ips=%s | root_path='%s'", cfg.proxy_headers, cfg.forwarded_allow_ips, cfg.root_path)
    if sid:
        logger.info("DEFAULT_SPREADSHEET_ID present (len=%s)", len(sid))
    if token:
        logger.info("APP/BACKEND token detected: %s", token)
    if eodhd:
        logger.info("EODHD key detected: %s", eodhd)
    logger.info("===============================================")


# =============================================================================
# Diagnostics
# =============================================================================
def verify_app_import(cfg: ServerConfig, logger: logging.Logger, *, verify_factory: bool = False) -> bool:
    """
    Import verification that prints FULL traceback.
    If verify_factory=True and APP_FACTORY enabled, it will call app() to validate.
    """
    logger.info("Verifying ASGI import: %s", cfg.app_import_string)
    try:
        module = importlib.import_module(cfg.app_module)
        if not hasattr(module, cfg.app_var):
            logger.error("ASGI variable '%s' not found in module '%s'.", cfg.app_var, cfg.app_module)
            return False

        app_obj = getattr(module, cfg.app_var)
        if app_obj is None:
            logger.error("ASGI variable '%s' is None in '%s'.", cfg.app_var, cfg.app_module)
            return False

        if cfg.uvicorn_factory:
            if not callable(app_obj):
                logger.error("APP_FACTORY enabled but '%s' is not callable in '%s'.", cfg.app_var, cfg.app_module)
                return False
            if verify_factory:
                logger.info("Factory verify enabled: calling %s() once for validation.", cfg.app_var)
                created = app_obj()
                if created is None:
                    logger.error("Factory returned None for %s() in '%s'.", cfg.app_var, cfg.app_module)
                    return False

        logger.info("ASGI import OK.")
        return True
    except Exception as e:
        logger.error("ASGI import FAILED: %s", e)
        traceback.print_exc()
        return False


# =============================================================================
# Server runners
# =============================================================================
def _try_import(module_name: str) -> bool:
    try:
        importlib.import_module(module_name)
        return True
    except Exception:
        return False


def _resolve_gunicorn_worker_class(cfg: ServerConfig, logger: logging.Logger) -> Optional[str]:
    """
    Return a valid gunicorn worker class string, or None if no suitable worker exists.
    """
    if cfg.gunicorn_worker_class:
        # if user forces a worker class, trust it; gunicorn will error with traceback anyway
        return cfg.gunicorn_worker_class

    # Prefer uvicorn-worker package if installed
    candidates = [
        "uvicorn_worker.UvicornWorker",         # external package (uvicorn-worker)
        "uvicorn.workers.UvicornWorker",        # uvicorn built-in gunicorn worker
    ]
    for wc in candidates:
        mod = wc.rsplit(".", 1)[0]
        if _try_import(mod):
            return wc

    logger.warning("No Gunicorn Uvicorn worker class found; falling back to Uvicorn.")
    return None


def run_uvicorn(cfg: ServerConfig, logger: logging.Logger) -> int:
    try:
        import uvicorn  # local import so traceback is visible if missing
    except Exception as e:
        logger.error("Cannot import uvicorn: %s", e)
        traceback.print_exc()
        return 1

    # Optional perf flags (only if requested and installed)
    loop_opt = cfg.uvicorn_loop if (cfg.uvicorn_loop and _try_import(cfg.uvicorn_loop)) else None
    http_opt = cfg.uvicorn_http if (cfg.uvicorn_http and _try_import(cfg.uvicorn_http)) else None

    logger.info(
        "Starting Uvicorn on %s:%s | app=%s | workers=%s | lifespan=%s | loop=%s | http=%s",
        cfg.host,
        cfg.port,
        cfg.app_import_string,
        cfg.workers,
        cfg.lifespan,
        loop_opt or "default",
        http_opt or "default",
    )

    try:
        uvicorn.run(
            cfg.app_import_string,
            host=cfg.host,
            port=cfg.port,
            log_level=cfg.log_level,
            access_log=cfg.access_log,
            proxy_headers=cfg.proxy_headers,
            forwarded_allow_ips=cfg.forwarded_allow_ips,
            root_path=cfg.root_path or "",
            lifespan=cfg.lifespan,
            timeout_keep_alive=cfg.uvicorn_keepalive,
            timeout_graceful_shutdown=cfg.uvicorn_graceful_timeout,
            limit_concurrency=cfg.uvicorn_limit_concurrency,
            limit_max_requests=cfg.uvicorn_limit_max_requests,
            backlog=cfg.uvicorn_backlog,
            workers=(cfg.workers if cfg.workers > 1 else 1),
            server_header=False,
            factory=bool(cfg.uvicorn_factory),
            loop=loop_opt,
            http=http_opt,
        )
        return 0
    except Exception as e:
        logger.error("Uvicorn crashed before binding the port: %s", e)
        traceback.print_exc()
        return 1


def run_gunicorn(cfg: ServerConfig, logger: logging.Logger) -> int:
    try:
        from gunicorn.app.base import BaseApplication
    except Exception as e:
        logger.warning("Gunicorn not available (%s); falling back to Uvicorn.", e)
        return run_uvicorn(cfg, logger)

    worker_class = _resolve_gunicorn_worker_class(cfg, logger)
    if not worker_class:
        return run_uvicorn(cfg, logger)

    # Import app object now (show traceback if it fails)
    try:
        module = importlib.import_module(cfg.app_module)
        app_obj = getattr(module, cfg.app_var)
        if app_obj is None:
            raise RuntimeError(f"{cfg.app_import_string} resolved to None")
        if cfg.uvicorn_factory:
            if not callable(app_obj):
                raise RuntimeError("APP_FACTORY enabled but app object is not callable")
            app_obj = app_obj()
            if app_obj is None:
                raise RuntimeError("Factory app() returned None")
    except Exception as e:
        logger.error("Failed to import ASGI app for Gunicorn: %s", e)
        traceback.print_exc()
        return 1

    logger.info(
        "Starting Gunicorn on %s:%s | workers=%s | worker_class=%s | preload_app=%s",
        cfg.host,
        cfg.port,
        cfg.workers,
        worker_class,
        cfg.gunicorn_preload_app,
    )

    # /dev/shm improves worker temp files in containers when available
    worker_tmp_dir = "/dev/shm" if os.path.isdir("/dev/shm") else None

    options: Dict[str, Any] = {
        "bind": f"{cfg.host}:{cfg.port}",
        "workers": int(cfg.workers),
        "worker_class": worker_class,
        "loglevel": cfg.log_level,
        "accesslog": "-" if cfg.access_log else None,
        "errorlog": "-",
        "timeout": int(cfg.gunicorn_timeout),
        "graceful_timeout": int(cfg.gunicorn_graceful_timeout),
        "keepalive": int(cfg.gunicorn_keepalive),
        "backlog": int(cfg.uvicorn_backlog),
        "preload_app": bool(cfg.gunicorn_preload_app),
    }
    if worker_tmp_dir:
        options["worker_tmp_dir"] = worker_tmp_dir

    # Remove Nones (gunicorn dislikes some)
    options = {k: v for k, v in options.items() if v is not None}

    class StandaloneApplication(BaseApplication):
        def __init__(self, app: Any, opts: Dict[str, Any]):
            self._application = app
            self._options = opts
            super().__init__()

        def load_config(self):
            for k, v in self._options.items():
                self.cfg.set(k, v)

        def load(self):
            return self._application

    try:
        StandaloneApplication(app_obj, options).run()
        return 0
    except Exception as e:
        logger.error("Gunicorn crashed before binding the port: %s", e)
        traceback.print_exc()
        return 1


# =============================================================================
# Signals
# =============================================================================
def install_signal_handlers(logger: logging.Logger) -> None:
    def _handler(signum, _frame):
        try:
            name = signal.Signals(signum).name
        except Exception:
            name = str(signum)
        logger.info("Received %s, shutting down...", name)
        raise SystemExit(0)

    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(s, _handler)
        except Exception:
            pass


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    parser = argparse.ArgumentParser(description="TFB Render-safe launcher")
    parser.add_argument("--app", default="", help='ASGI app import string, e.g. "main:app"')
    parser.add_argument("--host", default="", help="Bind host (default env HOST or 0.0.0.0)")
    parser.add_argument("--port", type=int, default=0, help="Bind port (default env PORT or 8000)")
    parser.add_argument("--server", default="", help="auto|uvicorn|gunicorn (default env SERVER or auto)")
    parser.add_argument("--factory", action="store_true", help="Treat app as factory (call app())")
    parser.add_argument("--health-check", action="store_true", help="Import-check only and exit 0/1")
    parser.add_argument("--skip-import-check", action="store_true", help="Skip import check before launch")
    parser.add_argument("--verify-factory", action="store_true", help="If factory enabled, call app() during import check")
    parser.add_argument("--force-uvicorn", action="store_true", help="Force uvicorn even if gunicorn is available")
    args = parser.parse_args()

    cfg = ServerConfig.from_env(
        app=(args.app.strip() or None),
        host=(args.host.strip() or None),
        port=(args.port if args.port > 0 else None),
        server=(args.server.strip() or None),
        factory=(True if args.factory else None),
    )

    logger = setup_logging(cfg.log_level)
    install_signal_handlers(logger)
    _log_boot_banner(cfg, logger)

    if not args.skip_import_check:
        ok = verify_app_import(cfg, logger, verify_factory=bool(args.verify_factory))
        if args.health_check:
            return 0 if ok else 1
        if not ok:
            logger.error("ASGI import verification failed. Exiting with status 1.")
            return 1
    else:
        if args.health_check:
            logger.error("--health-check requires import check (do not use --skip-import-check).")
            return 1

    # Decide server
    srv = cfg.server
    if args.force_uvicorn:
        srv = "uvicorn"

    if srv == "gunicorn":
        return run_gunicorn(cfg, logger)
    if srv == "uvicorn":
        return run_uvicorn(cfg, logger)

    # auto: prefer gunicorn when workers > 1 (more stable multi-worker on Render)
    if cfg.workers > 1:
        return run_gunicorn(cfg, logger)

    return run_uvicorn(cfg, logger)


if __name__ == "__main__":
    sys.exit(main())
