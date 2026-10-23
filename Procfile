# ==============================================================================
# Procfile -- TADAWUL FAST BRIDGE (Aligned with canonical revisions)
# ==============================================================================
# WHY 2026-10-16: align deployment comments with scripts/start_web.sh v2.6.1.
# This repository has no Render Blueprint; Render configuration is managed in
# the Dashboard. Process commands below are unchanged.
#
# Process types
# -----------------------------------------------------------------------------
#
# web     FastAPI ASGI app via scripts/start_web.sh (v2.6.1)
#         Launcher starts Gunicorn + UvicornWorker with bounded worker,
#         timeout and keepalive settings; default worker count is one.
#         Render provides PORT (default 10000, matching main.py fallback);
#         WEB_CONCURRENCY controls worker count.
#         Canonical env group: tfb-server (UVICORN_KEEPALIVE,
#         UVICORN_GRACEFUL_TIMEOUT, UVICORN_BACKLOG, WEB_CONCURRENCY,
#         WORKERS_MAX).
#         Invoked via `./scripts/start_web.sh` to honor the script's
#         `#!/usr/bin/env bash` shebang. The launcher MUST be executable;
#         the documented Render Dashboard Start Command ensures this.
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
# This repository has no render.yaml Blueprint. The documented Render
# Dashboard Start Command is:
#     set -euo pipefail
#     chmod +x scripts/start_web.sh
#     exec ./scripts/start_web.sh
# The Dashboard Health Check Path is /readyz.
# This Procfile is the canonical process declaration for:
#   Heroku / Railway / Fly.io / local Foreman / local Honcho
#
# For Render: a worker requires a separately configured background worker
# service. This Procfile does not configure the Dashboard-managed services.
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
