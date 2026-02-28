#!/usr/bin/env python3
"""
scripts/start_web.py
================================================================================
TADAWUL FAST BRIDGE – RENDER-SAFE WEB LAUNCHER (v6.2.0)
================================================================================

Primary goal:
- Be DEPLOYMENT-SAFE on Render (binds $PORT, logs full tracebacks, never fails silently)

Key improvements vs your v6.1.1:
- ✅ Reads PORT/WEB_CONCURRENCY/LOG_LEVEL from env by default (Render-friendly)
- ✅ Hard clamps workers to sane limits to avoid OOM crashes on small instances
- ✅ Much stronger diagnostics: prints FULL traceback on import/startup failures
- ✅ Gunicorn preload_app is OFF by default (preload often triggers import-time crashes)
- ✅ Health check import test is optional and lightweight
- ✅ Uvicorn settings aligned with Render defaults (lifespan=on, proxy headers)

NOTE:
This script ONLY helps if Render start command runs it, e.g.:
  python scripts/start_web.py
or:
  python scripts/start_web.py --app main:app

If Render is still running "python -m uvicorn main:app", then the real fix is in main.py/import chain.
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
from typing import Any, Optional


# -----------------------------------------------------------------------------
# Env helpers (safe parsing)
# -----------------------------------------------------------------------------
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
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _clamp(n: int, lo: int, hi: int) -> int:
    if n < lo:
        return lo
    if n > hi:
        return hi
    return n


def _normalize_loglevel(v: str) -> str:
    v = (v or "info").strip().lower()
    return v if v in {"critical", "error", "warning", "info", "debug", "trace"} else "info"


# -----------------------------------------------------------------------------
# Config (simple, deployment-safe: no pydantic dependency or strict patterns)
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class ServerConfig:
    host: str
    port: int
    app_module: str
    app_var: str

    # Runtime
    log_level: str
    access_log: bool
    proxy_headers: bool
    forwarded_allow_ips: str
    root_path: str
    lifespan: str

    # Concurrency
    workers: int
    use_gunicorn: bool
    gunicorn_worker_class: str
    gunicorn_preload_app: bool

    # Timeouts / limits
    timeout_keep_alive: int
    timeout_graceful: int
    limit_concurrency: Optional[int]
    limit_max_requests: Optional[int]
    backlog: int

    @property
    def app_import_string(self) -> str:
        return f"{self.app_module}:{self.app_var}"

    @classmethod
    def from_env(cls, *, app: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None) -> "ServerConfig":
        # Defaults tuned for Render
        env_host = host or _env_str("HOST", "0.0.0.0")
        env_port = int(port if port is not None else _env_int("PORT", 8000))

        # app can be "main:app" OR module+var via env
        app_module = _env_str("APP_MODULE", "main")
        app_var = _env_str("APP_VAR", "app")
        if app:
            if ":" in app:
                app_module, app_var = app.split(":", 1)
                app_module, app_var = app_module.strip(), app_var.strip()

        # Keep concurrency safe: Render small instances crash if you auto-scale too hard
        workers = _env_int("WEB_CONCURRENCY", 1)
        workers = _clamp(workers, 1, 8)

        # If user explicitly asked for gunicorn OR workers>1, try gunicorn (if installed)
        use_gunicorn = _env_bool("USE_GUNICORN", workers > 1)

        # Worker class preference (you installed uvicorn-worker in your log)
        # Fallback chain: env -> uvicorn_worker -> uvicorn.workers.UvicornWorker
        wc_env = _env_str("WORKER_CLASS", "").strip()
        gunicorn_worker_class = wc_env or "uvicorn_worker.UvicornWorker"

        # preload_app commonly triggers import-time failures; keep OFF unless forced
        gunicorn_preload_app = _env_bool("GUNICORN_PRELOAD_APP", False)

        log_level = _normalize_loglevel(_env_str("LOG_LEVEL", "info"))
        access_log = _env_bool("UVICORN_ACCESS_LOG", True)
        proxy_headers = True
        forwarded_allow_ips = _env_str("FORWARDED_ALLOW_IPS", "*")
        root_path = _env_str("UVICORN_ROOT_PATH", "")
        lifespan = _env_str("UVICORN_LIFESPAN", "on")  # match your Render command: --lifespan on

        timeout_keep_alive = _clamp(_env_int("UVICORN_KEEPALIVE", 75), 1, 300)
        timeout_graceful = _clamp(_env_int("UVICORN_GRACEFUL_TIMEOUT", 30), 1, 180)

        # Optional limits (empty -> None)
        lc_raw = _env_str("UVICORN_LIMIT_CONCURRENCY", "")
        lmr_raw = _env_str("UVICORN_LIMIT_MAX_REQUESTS", "")
        limit_concurrency = int(lc_raw) if lc_raw.isdigit() and int(lc_raw) > 0 else None
        limit_max_requests = int(lmr_raw) if lmr_raw.isdigit() and int(lmr_raw) > 0 else None

        backlog = _clamp(_env_int("UVICORN_BACKLOG", 2048), 64, 16384)

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
            workers=workers,
            use_gunicorn=use_gunicorn,
            gunicorn_worker_class=gunicorn_worker_class,
            gunicorn_preload_app=gunicorn_preload_app,
            timeout_keep_alive=timeout_keep_alive,
            timeout_graceful=timeout_graceful,
            limit_concurrency=limit_concurrency,
            limit_max_requests=limit_max_requests,
            backlog=backlog,
        )


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
def setup_logging(level: str) -> logging.Logger:
    logger = logging.getLogger("start_web")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()

    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    h.setFormatter(fmt)
    logger.addHandler(h)

    # Reduce noise from libs
    logging.getLogger("uvicorn").setLevel(getattr(logging, level.upper(), logging.INFO))
    logging.getLogger("uvicorn.error").setLevel(getattr(logging, level.upper(), logging.INFO))
    logging.getLogger("uvicorn.access").setLevel(getattr(logging, level.upper(), logging.INFO))

    return logger


# -----------------------------------------------------------------------------
# Diagnostics
# -----------------------------------------------------------------------------
def verify_app_import(cfg: ServerConfig, logger: logging.Logger) -> bool:
    """
    Import verification that prints FULL tracebacks.
    This is what you need for "No open ports detected" issues.
    """
    logger.info(f"Verifying ASGI import: {cfg.app_import_string}")
    try:
        module = importlib.import_module(cfg.app_module)
        if not hasattr(module, cfg.app_var):
            logger.error(f"ASGI variable '{cfg.app_var}' not found in module '{cfg.app_module}'.")
            return False
        app_obj = getattr(module, cfg.app_var)
        if app_obj is None:
            logger.error(f"ASGI variable '{cfg.app_var}' is None in '{cfg.app_module}'.")
            return False
        logger.info("ASGI import OK.")
        return True
    except Exception as e:
        logger.error(f"ASGI import FAILED: {e}")
        traceback.print_exc()  # critical: show the real reason in Render logs
        return False


# -----------------------------------------------------------------------------
# Launchers
# -----------------------------------------------------------------------------
def run_uvicorn(cfg: ServerConfig, logger: logging.Logger) -> int:
    try:
        import uvicorn  # imported here so failure shows traceback
    except Exception as e:
        logger.error(f"Cannot import uvicorn: {e}")
        traceback.print_exc()
        return 1

    logger.info(
        f"Starting Uvicorn on {cfg.host}:{cfg.port} | app={cfg.app_import_string} | workers={cfg.workers} | lifespan={cfg.lifespan}"
    )

    try:
        # Keep defaults aligned with Render.
        uvicorn.run(
            cfg.app_import_string,
            host=cfg.host,
            port=cfg.port,
            log_level=cfg.log_level,
            access_log=cfg.access_log,
            proxy_headers=cfg.proxy_headers,
            forwarded_allow_ips=cfg.forwarded_allow_ips,
            root_path=cfg.root_path or "",
            lifespan=cfg.lifespan,  # "on" for consistent startup behavior
            timeout_keep_alive=cfg.timeout_keep_alive,
            timeout_graceful_shutdown=cfg.timeout_graceful,
            limit_concurrency=cfg.limit_concurrency,
            limit_max_requests=cfg.limit_max_requests,
            backlog=cfg.backlog,
            workers=(cfg.workers if cfg.workers > 1 else 1),
            server_header=False,
        )
        return 0
    except Exception as e:
        logger.error(f"Uvicorn crashed before binding the port: {e}")
        traceback.print_exc()
        return 1


def run_gunicorn(cfg: ServerConfig, logger: logging.Logger) -> int:
    try:
        from gunicorn.app.base import BaseApplication
    except Exception as e:
        logger.warning(f"Gunicorn not available ({e}); falling back to Uvicorn.")
        return run_uvicorn(cfg, logger)

    # Import app object now (and print traceback if it fails)
    try:
        module = importlib.import_module(cfg.app_module)
        app_obj = getattr(module, cfg.app_var)
    except Exception as e:
        logger.error(f"Failed to import ASGI app for Gunicorn: {e}")
        traceback.print_exc()
        return 1

    # Safer: default preload_app OFF to avoid master-process import crashes
    worker_class = cfg.gunicorn_worker_class
    logger.info(
        f"Starting Gunicorn on {cfg.host}:{cfg.port} | workers={cfg.workers} | worker_class={worker_class} | preload_app={cfg.gunicorn_preload_app}"
    )

    options = {
        "bind": f"{cfg.host}:{cfg.port}",
        "workers": cfg.workers,
        "worker_class": worker_class,
        "loglevel": cfg.log_level,
        "accesslog": "-",  # stdout
        "errorlog": "-",   # stdout
        "timeout": 60,
        "graceful_timeout": cfg.timeout_graceful,
        "keepalive": cfg.timeout_keep_alive,
        "backlog": cfg.backlog,
        "preload_app": bool(cfg.gunicorn_preload_app),
    }

    class StandaloneApplication(BaseApplication):
        def __init__(self, app: Any, options_dict: dict):
            self._application = app
            self._options = options_dict
            super().__init__()

        def load_config(self):
            for k, v in self._options.items():
                if v is None:
                    continue
                self.cfg.set(k, v)

        def load(self):
            return self._application

    try:
        StandaloneApplication(app_obj, options).run()
        return 0
    except Exception as e:
        logger.error(f"Gunicorn crashed before binding the port: {e}")
        traceback.print_exc()
        return 1


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def install_signal_handlers(logger: logging.Logger) -> None:
    def _handler(signum, _frame):
        try:
            name = signal.Signals(signum).name
        except Exception:
            name = str(signum)
        logger.info(f"Received {name}, shutting down...")
        raise SystemExit(0)

    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(s, _handler)
        except Exception:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Tadawul Fast Bridge Web Launcher (Render-safe)")
    parser.add_argument("--app", default="", help='ASGI app import string, e.g. "main:app"')
    parser.add_argument("--host", default="", help="Bind host (default: env HOST or 0.0.0.0)")
    parser.add_argument("--port", type=int, default=0, help="Bind port (default: env PORT or 8000)")
    parser.add_argument("--force-uvicorn", action="store_true", help="Force Uvicorn even if Gunicorn is available")
    parser.add_argument("--skip-import-check", action="store_true", help="Skip ASGI import pre-check")
    args = parser.parse_args()

    cfg = ServerConfig.from_env(
        app=(args.app.strip() or None),
        host=(args.host.strip() or None),
        port=(args.port if args.port > 0 else None),
    )
    logger = setup_logging(cfg.log_level)
    install_signal_handlers(logger)

    logger.info("Boot config:")
    logger.info(f"  host={cfg.host} port={cfg.port}")
    logger.info(f"  app={cfg.app_import_string}")
    logger.info(f"  workers={cfg.workers} use_gunicorn={cfg.use_gunicorn}")
    logger.info(f"  access_log={cfg.access_log} proxy_headers={cfg.proxy_headers} lifespan={cfg.lifespan}")
    logger.info(f"  root_path='{cfg.root_path}' forwarded_allow_ips='{cfg.forwarded_allow_ips}'")

    # Import check is the fastest way to expose the real crash reason in logs
    if not args.skip_import_check:
        if not verify_app_import(cfg, logger):
            logger.error("ASGI import verification failed. Exiting with status 1.")
            return 1

    # Choose server
    if cfg.use_gunicorn and not args.force_uvicorn and cfg.workers > 1:
        return run_gunicorn(cfg, logger)

    return run_uvicorn(cfg, logger)


if __name__ == "__main__":
    sys.exit(main())
