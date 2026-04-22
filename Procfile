# ==============================================================================
# Procfile -- TADAWUL FAST BRIDGE (Aligned with canonical revisions)
# ==============================================================================
#
# Process types
# -----------------------------------------------------------------------------
#
# web     FastAPI ASGI app via scripts/start_web.sh (v2.6.0)
#         Launcher handles: uvicorn/gunicorn selection, cgroup-aware worker
#         calculation, optional DB/Redis health checks, uvloop/httptools probing.
#         Render provides PORT (default 10000, matching main.py fallback);
#         WEB_CONCURRENCY controls worker count.
#         Canonical env group: tfb-server (UVICORN_KEEPALIVE,
#         UVICORN_GRACEFUL_TIMEOUT, UVICORN_BACKLOG, WEB_CONCURRENCY,
#         WORKERS_MAX).
#         Invoked via `./scripts/start_web.sh` to match render.yaml's
#         canonical `exec ./scripts/start_web.sh` and honor the script's
#         `#!/usr/bin/env bash` shebang. The launcher MUST be executable
#         (ensured by render.yaml's pre-step `chmod +x scripts/start_web.sh`).
#
# worker  Background job processor (scripts/worker.py v4.3.0)
#         Connects to Redis (REDIS_URL) and processes tfb_background_jobs queue.
#         Supported task_type values:
#           - dashboard_sync  -> scripts/run_dashboard_sync.py v6.5.0
#                               via run_from_worker_payload_async(payload)
#           - market_scan     -> scripts/run_market_scan.py v5.3.0
#                               via run_from_worker_payload_async(payload)
#           - refresh_data    -> scripts/refresh_data.py v5.2.0
#                               via legacy name-probe (future: canonical
#                               entrypoint once refresh_data exposes it)
#         Falls back to idle loop when Redis is unavailable.
#         Dead-letter queue: tfb_background_jobs_dead (configurable via
#         TFB_WORKER_DLQ_NAME).
#         Exit codes: 0 clean / 1 fatal / 2 Redis-missing-with-fail-fast /
#         130 SIGINT.
#         Runs with PYTHONUNBUFFERED=1 so log output is flushed line-by-line
#         (critical for Honcho/Foreman/Heroku log aggregation, which all
#         pipe stdout -- buffered output would hide progress until flush).
#
# release Optional pre-deploy migration step (scripts/migrate_schema_v2.py).
#         Runs BEFORE the new version starts receiving traffic on Heroku-style
#         PaaS. Commented out by default -- enable after reviewing the migration
#         script's behavior on your dataset. Exit codes: 0 success / 1 validation
#         failed / 2 partial success / 3 write failed. Treat exit 2 as a
#         warning (partial success); exit 1 or 3 should abort the release.
#
# Render deployment note
# -----------------------------------------------------------------------------
# render.yaml defines the web service directly via:
#     startCommand: |
#       chmod +x scripts/start_web.sh
#       exec ./scripts/start_web.sh
# This Procfile is the canonical process declaration for:
#   Heroku / Railway / Fly.io / local Foreman / local Honcho
#
# For Render: to run the worker alongside the web service, add a `workers`
# or `backgroundWorker` block to render.yaml (render.yaml v6.x supports
# `type: worker`). This Procfile does NOT affect Render deployments -- Render
# reads render.yaml, not Procfile.
#
# Local usage
#   pip install honcho          # or: brew install foreman
#   honcho start                # starts web + worker + (release if enabled)
#   honcho start web            # web only
#   honcho start worker         # worker only
#   honcho run release          # run release task one-off
#
# Environment-variable inheritance
# -----------------------------------------------------------------------------
# Honcho/Foreman pass the parent shell's environment to each child process.
# For the `web` process, start_web.sh re-reads PORT/WEB_CONCURRENCY/UVICORN_*
# from env on each restart. For the `worker` process, worker.py reads
# REDIS_URL and the TFB_WORKER_* namespace (see scripts/worker.py header for
# the full list).
#
# ==============================================================================
web:     ./scripts/start_web.sh
worker:  PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 python3 scripts/worker.py
# release: PYTHONUNBUFFERED=1 python3 scripts/migrate_schema_v2.py --apply
