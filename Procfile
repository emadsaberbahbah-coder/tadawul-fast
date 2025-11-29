# =============================================================================
# Tadawul Fast Bridge / Stock Market Hub API - Procfile
# Version: 3.2.0 | Environment: Production | Render & Local Friendly
# =============================================================================

# NOTE FOR RENDER:
# - Render will only use the `web:` process for this service.
# - Other process types (health_monitor, cache_worker, etc.) are for
#   separate worker services or local / Heroku-style usage.

# --- Primary Web Process (Render / Production) ---
web: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --workers ${WEB_CONCURRENCY:-2} \
  --loop asyncio \
  --proxy-headers \
  --forwarded-allow-ips="*" \
  --log-level ${LOG_LEVEL:-info} \
  --access-log \
  --timeout-keep-alive 60 \
  --timeout-graceful-shutdown 30 \
  --header x-powered-by:"TadawulFastBridge/3.2.0" \
  --header x-server:"FastAPI-Uvicorn-Render"

# =============================================================================
# BACKGROUND / SUPPORT PROCESSES (NOT USED BY RENDER WEB SERVICE DIRECTLY)
# Use them as separate services or for local/Heroku-style process management.
# =============================================================================

# --- Health Monitor Process (External / Optional) ---
health_monitor: python scripts/health_monitor.py \
  --api-url https://${RENDER_SERVICE_NAME}.onrender.com \
  --interval 60 \
  --timeout 15 \
  --retries 2

# --- Cache Management Worker (Optional Worker Service) ---
cache_worker: python scripts/cache_manager.py \
  --mode worker \
  --cleanup-interval 300 \
  --max-size-mb 100 \
  --backup-enabled true

# --- Backup Scheduler (Optional Worker Service) ---
backup_scheduler: python scripts/backup_manager.py \
  --mode scheduler \
  --interval 3600 \
  --retention 24 \
  --max-backups 10

# =============================================================================
# ENVIRONMENT-SPECIFIC PROFILES (MAINLY FOR LOCAL / HEROKU-STYLE USE)
# =============================================================================

# --- Development Profile (Local Development) ---
dev: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --reload \
  --reload-dir . \
  --log-level debug \
  --loop asyncio \
  --workers 1 \
  --header x-environment:development

# --- Staging Profile (Pre-Production / Local) ---
staging: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers 2 \
  --log-level info \
  --loop asyncio \
  --header x-environment:staging

# --- Single Worker Profile (Troubleshooting / Local) ---
single: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers 1 \
  --log-level debug \
  --loop asyncio \
  --header x-worker:single

# =============================================================================
# PERFORMANCE-OPTIMIZED PROFILES (LOCAL TUNING / LOAD TESTS)
# =============================================================================

# --- High-Performance Profile (More Workers, Limited Requests) ---
web_high_perf: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers 4 \
  --log-level warning \
  --loop asyncio \
  --limit-max-requests 1000 \
  --header x-profile:high-performance

# --- Memory-Optimized Profile (Resource Constrained) ---
web_minimal: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers 1 \
  --log-level warning \
  --loop asyncio \
  --timeout-keep-alive 30 \
  --header x-profile:memory-optimized

# --- Readiness Check Profile (Health Checks / Local) ---
web_ready: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers 1 \
  --log-level error \
  --loop asyncio \
  --header x-purpose:readiness-check

# =============================================================================
# MAINTENANCE & ADMIN PROCESSES (OPTIONAL)
# =============================================================================

# --- Maintenance Mode Process (Serve Simple Maintenance App) ---
maintenance: uvicorn maintenance:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers 1 \
  --log-level info \
  --header x-mode:maintenance

# --- Database Maintenance (If using external DB) ---
# db_maintenance: python scripts/db_maintenance.py \
#   --vacuum-interval 86400 \
#   --analyze-interval 43200

# =============================================================================
# SCHEDULED TASK PROCESSES (OPTIONAL / SEPARATE SERVICES)
# =============================================================================

# --- Cache Warmup Scheduler (Production Optimization) ---
cache_warmup: python scripts/cache_manager.py \
  --mode warmup \
  --symbols-file config/popular_symbols.json \
  --concurrency 5 \
  --interval 1800

# --- Data Refresh Scheduler (Keep Data Current) ---
data_refresh: python scripts/data_refresh.py \
  --interval 900 \
  --batch-size 25 \
  --priority high

# =============================================================================
# MONITORING & ANALYTICS PROCESSES (OPTIONAL)
# =============================================================================

# --- Performance Monitor (Optional) ---
# performance_monitor: python scripts/performance_monitor.py \
#   --interval 60 \
#   --metrics-port 9090 \
#   --alert-threshold 80

# --- Log Aggregator (Optional) ---
# log_aggregator: python scripts/log_aggregator.py \
#   --sources web,cache,worker \
#   --rotation hourly

# =============================================================================
# RENDER-SPECIFIC NOTES
# =============================================================================
# - For the main Render web service, only `web:` above will run.
# - If you want background workers (cache_worker, backup_scheduler, etc.),
#   create separate "Worker" services in Render and point them to those
#   process commands (or copy the same command into Render's Start Command).

# =============================================================================
# TROUBLESHOOTING & DEBUG PROFILES (LOCAL ONLY)
# =============================================================================

# --- Debug Profile (Detailed Logging + Reload) ---
debug: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers 1 \
  --log-level debug \
  --loop asyncio \
  --reload \
  --reload-dir . \
  --header x-mode:debug

# --- Profile Mode (Performance Analysis) ---
profile: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers 1 \
  --log-level info \
  --loop asyncio \
  --header x-mode:profile

# --- Test Mode (Integration Testing) ---
test: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers 1 \
  --log-level warning \
  --loop asyncio \
  --header x-mode:test
