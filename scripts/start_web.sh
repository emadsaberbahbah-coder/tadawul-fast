#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/start_web.py
================================================================================
TADAWUL FAST BRIDGE — RENDER-SAFE WEB LAUNCHER (v6.4.0)
================================================================================

Core rules (Render-safe)
- Always bind to $PORT when present (Render port scanner requirement).
- Default to a single worker unless explicitly requested (avoid OOM / slow bind).
- Avoid heavy imports at module import-time.
- Print full traceback on any startup failure.
- Provide --health-check import verification (fast, non-network).

Recommended Render startCommand
- python scripts/start_web.py

Optional env vars
- PORT (Render provides)
- HOST (default 0.0.0.0)
- SERVER: auto | uvicorn | gunicorn
- WEB_CONCURRENCY: number of workers (default 1)
- WORKERS_MAX: clamp workers (default 4)
- LOG_LEVEL: info|debug|warning|error|critical
- UVICORN_ACCESS_LOG: 1/0
- UVICORN_KEEPALIVE: seconds (default 75)
- UVICORN_GRACEFUL_TIMEOUT: seconds (default 30)
- UVICORN_BACKLOG: default 2048
- UVICORN_LIMIT_CONCURRENCY: optional
- UVICORN_LIMIT_MAX_REQUESTS: optional
- APP_MODULE / APP_VAR (default main / app)
- APP_FACTORY: 1 to treat app as callable factory
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import signal
import sys
import traceback
from dataclasses import dataclass
from typing import Any, Dict, Optional


# -----------------------------------------------------------------------------
# Env parsing helpers
# -----------------------------------------------------------------------------
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


def _env_bool(name: str, default: bool) -> bool:
    raw = _env_str(name, "")
    if raw == "":
        return bool(default)
    s = raw.lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return bool(default)


def _clamp(n: int, lo: int, hi: int) -> int:
    return lo if n < lo else hi if n > hi else n


def _normalize_loglevel(v: str) -> str:
    s = (v or "info").strip().lower()
    return s if s in {"critical", "error", "warning", "info", "debug", "trace"} else "info"


def _mask(s: str, left: int = 2, right: int = 3) -> str:
    t = (s or "").strip()
    if not t:
        return ""
    if len(t) <= left + right + 3:
        return "***"
    return f"{t[:left]}...{t[-right:]}"


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class ServerConfig:
    host: str
    port: int
    server: str  # auto|uvicorn|gunicorn
    workers: int
    log_level: str

    app_module: str
    app_var: str
    factory: bool

    access_log: bool
    forwarded_allow_ips: str
    root_path: str
    lifespan: str

    keepalive: int
    graceful_timeout: int
    backlog: int
    limit_concurrency: Optional[int]
    limit_max_requests: Optional[int]

    gunicorn_timeout: int
    gunicorn_graceful_timeout: int
    gunicorn_keepalive: int
    gunicorn_preload_app: bool
    worker_class_override: str

    @property
    def app_import_string(self) -> str:
        return f"{self.app_module}:{self.app_var}"

    @classmethod
    def from_env(cls, *, app: Optional[str], host: Optional[str], port: Optional[int], server: Optional[str], factory: Optional[bool]) -> "ServerConfig":
        # PORT: if Render provides it, we MUST use it
        env_port_raw = _env_str("PORT", "")
        resolved_port = None
        if port and port > 0:
            resolved_port = port
        elif env_port_raw.isdigit():
            resolved_port = int(env_port_raw)
        else:
            resolved_port = _env_int("PORT", 8000)

        resolved_host = (host or _env_str("HOST", "0.0.0.0")).strip() or "0.0.0.0"

        # App parsing
        app_module = _env_str("APP_MODULE", "main")
        app_var = _env_str("APP_VAR", "app")
        if app:
            a = app.strip()
            if ":" in a:
                app_module, app_var = [x.strip() for x in a.split(":", 1)]
            else:
                app_module, app_var = a, "app"

        # Logging
        log_level = _normalize_loglevel(_env_str("LOG_LEVEL", "info"))

        # Workers: SAFE default = 1 (Render-friendly)
        web_conc = _env_int("WEB_CONCURRENCY", 1)
        workers_max = _clamp(_env_int("WORKERS_MAX", 4), 1, 32)
        workers = _clamp(web_conc, 1, workers_max)

        # Server selection
        srv = (server or _env_str("SERVER", "auto")).strip().lower()
        if srv not in {"auto", "uvicorn", "gunicorn"}:
            srv = "auto"

        access_log = _env_bool("UVICORN_ACCESS_LOG", True)
        forwarded_allow_ips = _env_str("FORWARDED_ALLOW_IPS", "*") or "*"
        root_path = _env_str("UVICORN_ROOT_PATH", "")
        lifespan = _env_str("UVICORN_LIFESPAN", "on") or "on"

        keepalive = _clamp(_env_int("UVICORN_KEEPALIVE", 75), 1, 300)
        graceful_timeout = _clamp(_env_int("UVICORN_GRACEFUL_TIMEOUT", 30), 1, 180)
        backlog = _clamp(_env_int("UVICORN_BACKLOG", 2048), 64, 16384)

        lc_raw = _env_str("UVICORN_LIMIT_CONCURRENCY", "")
        lmr_raw = _env_str("UVICORN_LIMIT_MAX_REQUESTS", "")
        limit_concurrency = int(lc_raw) if lc_raw.isdigit() and int(lc_raw) > 0 else None
        limit_max_requests = int(lmr_raw) if lmr_raw.isdigit() and int(lmr_raw) > 0 else None

        # Factory mode
        fac = bool(factory) if factory is not None else _env_bool("APP_FACTORY", False)

        # Gunicorn knobs (safe)
        gunicorn_preload_app = _env_bool("GUNICORN_PRELOAD_APP", False)
        worker_class_override = _env_str("WORKER_CLASS", "").strip()
        gunicorn_timeout = _clamp(_env_int("GUNICORN_TIMEOUT", 60), 10, 600)
        gunicorn_graceful_timeout = _clamp(_env_int("GUNICORN_GRACEFUL_TIMEOUT", 30), 5, 180)
        gunicorn_keepalive = _clamp(_env_int("GUNICORN_KEEPALIVE", 5), 1, 120)

        return cls(
            host=resolved_host,
            port=int(resolved_port),
            server=srv,
            workers=int(workers),
            log_level=log_level,
            app_module=app_module,
            app_var=app_var,
            factory=fac,
            access_log=access_log,
            forwarded_allow_ips=forwarded_allow_ips,
            root_path=root_path,
            lifespan=lifespan,
            keepalive=keepalive,
            graceful_timeout=graceful_timeout,
            backlog=backlog,
            limit_concurrency=limit_concurrency,
            limit_max_requests=limit_max_requests,
            gunicorn_timeout=gunicorn_timeout,
            gunicorn_graceful_timeout=gunicorn_graceful_timeout,
            gunicorn_keepalive=gunicorn_keepalive,
            gunicorn_preload_app=gunicorn_preload_app,
            worker_class_override=worker_class_override,
        )


# -----------------------------------------------------------------------------
# Logging setup
# -----------------------------------------------------------------------------
def _setup_logging(level: str) -> logging.Logger:
    logger = logging.getLogger("start_web")
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"))
    logger.addHandler(h)

    # Keep uvicorn logs consistent
    for ln in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(ln).setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger


def _boot_banner(cfg: ServerConfig, logger: logging.Logger) -> None:
    token = _mask(_env_str("APP_TOKEN", "") or _env_str("BACKEND_TOKEN", ""))
    sid = _env_str("DEFAULT_SPREADSHEET_ID", "")

    logger.info("==================================================")
    logger.info("TFB Render-safe launcher v6.4.0")
    logger.info("python=%s | pid=%s", sys.version.split()[0], os.getpid())
    logger.info("bind=%s:%s | server=%s | workers=%s", cfg.host, cfg.port, cfg.server, cfg.workers)
    logger.info("app=%s | factory=%s", cfg.app_import_string, cfg.factory)
    logger.info("log_level=%s | access_log=%s | lifespan=%s", cfg.log_level, cfg.access_log, cfg.lifespan)
    logger.info("root_path='%s' | forwarded_allow_ips=%s", cfg.root_path, cfg.forwarded_allow_ips)
    if sid:
        logger.info("DEFAULT_SPREADSHEET_ID present (len=%s)", len(sid))
    if token:
        logger.info("APP token detected: %s", token)
    logger.info("==================================================")


# -----------------------------------------------------------------------------
# Import verification
# -----------------------------------------------------------------------------
def _verify_import(cfg: ServerConfig, logger: logging.Logger, *, verify_factory: bool) -> bool:
    logger.info("Verifying ASGI import: %s", cfg.app_import_string)
    try:
        module = importlib.import_module(cfg.app_module)
        if not hasattr(module, cfg.app_var):
            logger.error("ASGI var '%s' not found in '%s'", cfg.app_var, cfg.app_module)
            return False
        obj = getattr(module, cfg.app_var)
        if obj is None:
            logger.error("ASGI var '%s' is None in '%s'", cfg.app_var, cfg.app_module)
            return False

        if cfg.factory:
            if not callable(obj):
                logger.error("APP_FACTORY enabled but '%s' is not callable", cfg.app_var)
                return False
            if verify_factory:
                logger.info("Factory verify: calling %s() once", cfg.app_var)
                created = obj()
                if created is None:
                    logger.error("Factory returned None for %s()", cfg.app_var)
                    return False

        logger.info("ASGI import OK")
        return True
    except Exception as e:
        logger.error("ASGI import FAILED: %s", e)
        traceback.print_exc()
        return False


# -----------------------------------------------------------------------------
# Server runners
# -----------------------------------------------------------------------------
def _try_import(mod: str) -> bool:
    try:
        importlib.import_module(mod)
        return True
    except Exception:
        return False


def _resolve_gunicorn_worker_class(cfg: ServerConfig, logger: logging.Logger) -> Optional[str]:
    if cfg.worker_class_override:
        return cfg.worker_class_override

    # Prefer uvicorn-worker package if installed, else uvicorn builtin
    candidates = ["uvicorn_worker.UvicornWorker", "uvicorn.workers.UvicornWorker"]
    for wc in candidates:
        mod = wc.rsplit(".", 1)[0]
        if _try_import(mod):
            return wc

    logger.warning("No gunicorn uvicorn worker available")
    return None


def _run_uvicorn(cfg: ServerConfig, logger: logging.Logger) -> int:
    try:
        import uvicorn
    except Exception as e:
        logger.error("uvicorn import failed: %s", e)
        traceback.print_exc()
        return 1

    # IMPORTANT:
    # - Default to 1 worker unless explicitly set via WEB_CONCURRENCY
    # - Render sometimes mis-detects port if multi-worker startup is slow
    workers = cfg.workers if cfg.workers > 0 else 1

    logger.info("Starting uvicorn: %s:%s workers=%s", cfg.host, cfg.port, workers)
    try:
        uvicorn.run(
            cfg.app_import_string,
            host=cfg.host,
            port=cfg.port,
            log_level=cfg.log_level,
            access_log=cfg.access_log,
            proxy_headers=True,
            forwarded_allow_ips=cfg.forwarded_allow_ips,
            root_path=cfg.root_path or "",
            lifespan=cfg.lifespan,
            timeout_keep_alive=cfg.keepalive,
            timeout_graceful_shutdown=cfg.graceful_timeout,
            backlog=cfg.backlog,
            limit_concurrency=cfg.limit_concurrency,
            limit_max_requests=cfg.limit_max_requests,
            workers=workers,
            server_header=False,
            factory=cfg.factory,
        )
        return 0
    except Exception as e:
        logger.error("uvicorn crashed: %s", e)
        traceback.print_exc()
        return 1


def _run_gunicorn(cfg: ServerConfig, logger: logging.Logger) -> int:
    try:
        from gunicorn.app.base import BaseApplication
    except Exception as e:
        logger.warning("gunicorn not available (%s); fallback to uvicorn", e)
        return _run_uvicorn(cfg, logger)

    worker_class = _resolve_gunicorn_worker_class(cfg, logger)
    if not worker_class:
        return _run_uvicorn(cfg, logger)

    # Import app object (show traceback)
    try:
        module = importlib.import_module(cfg.app_module)
        app_obj = getattr(module, cfg.app_var)
        if cfg.factory:
            if not callable(app_obj):
                raise RuntimeError("APP_FACTORY enabled but app is not callable")
            app_obj = app_obj()
    except Exception as e:
        logger.error("Gunicorn app import failed: %s", e)
        traceback.print_exc()
        return 1

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
        "backlog": int(cfg.backlog),
        "preload_app": bool(cfg.gunicorn_preload_app),
    }
    options = {k: v for k, v in options.items() if v is not None}

    class StandaloneApplication(BaseApplication):
        def __init__(self, app: Any, opts: Dict[str, Any]):
            self._app = app
            self._opts = opts
            super().__init__()

        def load_config(self):
            for k, v in self._opts.items():
                self.cfg.set(k, v)

        def load(self):
            return self._app

    logger.info("Starting gunicorn: %s:%s workers=%s class=%s", cfg.host, cfg.port, cfg.workers, worker_class)
    try:
        StandaloneApplication(app_obj, options).run()
        return 0
    except Exception as e:
        logger.error("gunicorn crashed: %s", e)
        traceback.print_exc()
        return 1


# -----------------------------------------------------------------------------
# Signals
# -----------------------------------------------------------------------------
def _install_signal_handlers(logger: logging.Logger) -> None:
    def _handler(signum, _frame):
        try:
            name = signal.Signals(signum).name
        except Exception:
            name = str(signum)
        logger.info("Received %s -> exiting", name)
        raise SystemExit(0)

    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(s, _handler)
        except Exception:
            pass


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="TFB Render-safe launcher")
    parser.add_argument("--app", default="", help='ASGI app import string, e.g. "main:app"')
    parser.add_argument("--host", default="", help="Bind host override")
    parser.add_argument("--port", type=int, default=0, help="Bind port override")
    parser.add_argument("--server", default="", help="auto|uvicorn|gunicorn")
    parser.add_argument("--factory", action="store_true", help="Treat app var as factory callable")
    parser.add_argument("--health-check", action="store_true", help="Import-check only and exit")
    parser.add_argument("--skip-import-check", action="store_true", help="Skip import verification")
    parser.add_argument("--verify-factory", action="store_true", help="Call app() once during import check")
    args = parser.parse_args()

    cfg = ServerConfig.from_env(
        app=(args.app.strip() or None),
        host=(args.host.strip() or None),
        port=(args.port if args.port > 0 else None),
        server=(args.server.strip() or None),
        factory=(True if args.factory else None),
    )

    logger = _setup_logging(cfg.log_level)
    _install_signal_handlers(logger)
    _boot_banner(cfg, logger)

    if not args.skip_import_check:
        ok = _verify_import(cfg, logger, verify_factory=bool(args.verify_factory))
        if args.health_check:
            return 0 if ok else 1
        if not ok:
            logger.error("Import check failed -> exiting")
            return 1
    else:
        if args.health_check:
            logger.error("--health-check cannot be used with --skip-import-check")
            return 1

    # Decide server:
    # - If SERVER is explicit -> obey it
    # - Else auto:
    #   - prefer gunicorn only when workers > 1 (explicit scaling)
    #   - otherwise uvicorn (fast bind, minimal surprises)
    srv = cfg.server
    if srv == "gunicorn":
        return _run_gunicorn(cfg, logger)
    if srv == "uvicorn":
        return _run_uvicorn(cfg, logger)

    # auto
    if cfg.workers > 1:
        return _run_gunicorn(cfg, logger)
    return _run_uvicorn(cfg, logger)


if __name__ == "__main__":
    raise SystemExit(main())
