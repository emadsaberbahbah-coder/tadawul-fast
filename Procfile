# ==============================================================================
# Procfile -- TADAWUL FAST BRIDGE (Aligned with canonical revisions)
# ==============================================================================
#
# Process types
# -----------------------------------------------------------------------------
#
# web     FastAPI ASGI app via scripts/start_web.sh (v2.6.1+)
#         Launcher handles: gunicorn/uvicorn selection, worker calculation,
#         timeout/keep-alive resolution, optional DB/Redis probes, and a single
#         foreground process for platform port detection.
#         Render provides PORT (default 10000 outside Render, matching main.py);
#         WEB_CONCURRENCY controls worker count.
#         Canonical dashboard-managed env group: tfb-server
#         (UVICORN_KEEPALIVE, UVICORN_GRACEFUL_TIMEOUT, UVICORN_BACKLOG,
#         WEB_CONCURRENCY, WORKERS_MAX).
#         Render dashboard Start Command should be:
#             set -euo pipefail
#             chmod +x scripts/start_web.sh
#             exec ./scripts/start_web.sh
#
# worker  Background job processor (scripts/worker.py).
#         Connects to Redis (REDIS_URL) and processes tfb_background_jobs.
#         A `worker:` line in this Procfile does NOT create a Render worker.
#         Render requires a separately configured Background Worker service in
#         the dashboard, with its own deployed SHA, start command and ENV proof.
#         Dead-letter queue: tfb_background_jobs_dead (configurable through
#         TFB_WORKER_DLQ_NAME).
#
# release Optional pre-deploy migration step (scripts/migrate_schema_v2.py).
#         Commented out by default. Review migration behavior and exit-code
#         policy before enabling on a platform that supports release phases.
#
# Render deployment truth
# -----------------------------------------------------------------------------
# This repository currently has NO Render Blueprint (no render.yaml/render.yml).
# Render web/worker resources, health path, auto-deploy branch and environment
# groups are therefore dashboard-managed and must be captured in the deployment
# evidence bundle. This Procfile remains the canonical declaration for local
# Honcho/Foreman and Heroku-style platforms; Render uses the dashboard commands
# actually configured on each service.
#
# Local usage
#   pip install honcho
#   honcho start
#   honcho start web
#   honcho start worker
#   honcho run release
#
# Environment-variable inheritance
# -----------------------------------------------------------------------------
# Honcho/Foreman pass the parent shell environment to child processes.
# start_web.sh re-reads PORT/WEB_CONCURRENCY/UVICORN_* on each restart.
# worker.py reads REDIS_URL and the TFB_WORKER_* namespace.
#
# ==============================================================================
web:     ./scripts/start_web.sh
worker:  PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 python3 scripts/worker.py
# release: PYTHONUNBUFFERED=1 python3 scripts/migrate_schema_v2.py --apply
