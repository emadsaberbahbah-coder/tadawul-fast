==============================================================================

Procfile — TADAWUL FAST BRIDGE

==============================================================================

Process types

─────────────────────────────────────────────────────────────────────────────

web     FastAPI ASGI app via scripts/start_web.sh

Launcher handles: uvicorn/gunicorn selection, cgroup-aware worker

calculation, optional DB/Redis health checks, uvloop/httptools probing.

Render provides PORT; WEB_CONCURRENCY controls worker count.

worker  Background job processor (scripts/worker.py v4.2.0)

Connects to Redis (REDIS_URL) and processes tfb_background_jobs queue.

Supported task_type values: dashboard_sync | market_scan | refresh_data

Falls back to idle loop when Redis is unavailable.

Render deployment note

─────────────────────────────────────────────────────────────────────────────

render.yaml startCommand already references scripts/start_web.sh directly.

This Procfile is the canonical process declaration for:

Heroku / Railway / Fly.io / local Foreman / local Honcho

Local usage

pip install honcho          # or: brew install foreman

honcho start                # starts web + worker

honcho start web            # web only

honcho start worker         # worker only

==============================================================================

web:    bash scripts/start_web.sh
worker: python scripts/worker.py
