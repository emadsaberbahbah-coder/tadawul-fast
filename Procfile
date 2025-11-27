# =============================================================================
# Stock Market Hub API - Optimized Production Process File
# Version: 3.1.0 | Environment: Production | Render Optimized
# =============================================================================

# --- Primary Web Process (Render Optimized) ---
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
  --header x-powered-by:"TadawulFastBridge/3.1.0" \
  --header x-server:"FastAPI-Uvicorn-Render"

# --- Health Monitor Process (Essential) ---
health_monitor: python scripts/health_monitor.py \
  --api-url https://${RENDER_SERVICE_NAME}.onrender.com \
  --interval 60 \
  --timeout 15 \
  --retries 2

# --- Cache Management Worker (Essential) ---
cache_worker: python scripts/cache_manager.py \
  --mode worker \
  --cleanup-interval 300 \
  --max-size-mb 100 \
  --backup-enabled true

# --- Backup Scheduler (Critical for Data Safety) ---
backup_scheduler: python scripts/backup_manager.py \
  --mode scheduler \
  --interval 3600 \
  --retention 24 \
  --max-backups 10

# =============================================================================
# ENVIRONMENT-SPECIFIC PROFILES
# =============================================================================

# --- Development Profile (Local Development) ---
# Usage: heroku local dev
dev: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --reload \
  --reload-dir . \
  --log-level debug \
  --loop asyncio \
  --workers 1 \
  --header x-environment:development

# --- Staging Profile (Pre-Production) ---
# Usage: heroku local staging
staging: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --workers 2 \
  --log-level info \
  --loop asyncio \
  --header x-environment:staging

# --- Single Worker Profile (Troubleshooting) ---
# Usage: heroku local single
single: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --workers 1 \
  --log-level debug \
  --loop asyncio \
  --header x-worker:single

# =============================================================================
# PERFORMANCE-OPTIMIZED PROFILES
# =============================================================================

# --- High-Performance Profile (4 workers) ---
# Usage: heroku local highperf
web_high_perf: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --workers 4 \
  --log-level warning \
  --loop asyncio \
  --max-requests 1000 \
  --max-requests-jitter 100 \
  --header x-profile:high-performance

# --- Memory-Optimized Profile (Resource Constrained) ---
# Usage: heroku local minimal
web_minimal: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --workers 1 \
  --log-level warning \
  --loop asyncio \
  --timeout-keep-alive 30 \
  --header x-profile:memory-optimized

# --- Readiness Check Profile (Health Checks) ---
# Usage: heroku local readiness
web_ready: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --workers 1 \
  --log-level error \
  --loop asyncio \
  --header x-purpose:readiness-check

# =============================================================================
# MAINTENANCE & ADMIN PROCESSES
# =============================================================================

# --- Maintenance Mode Process ---
# Usage: heroku local maintenance
maintenance: uvicorn maintenance:app \
  --host 0.0.0.0 \
  --port $PORT \
  --workers 1 \
  --log-level info \
  --header x-mode:maintenance

# --- Database Maintenance (If using external DB) ---
# db_maintenance: python scripts/db_maintenance.py \
#   --vacuum-interval 86400 \
#   --analyze-interval 43200

# =============================================================================
# SCHEDULED TASK PROCESSES
# =============================================================================

# --- Cache Warmup Scheduler (Production Optimization) ---
cache_warmup: python scripts/cache_manager.py \
  --mode warmup \
  --symbols-file config/popular_symbols.json \
  --concurrency 5 \
  --interval 1800

# --- Data Refresh Scheduler (Keep data current) ---
data_refresh: python scripts/data_refresh.py \
  --interval 900 \
  --batch-size 25 \
  --priority high

# =============================================================================
# MONITORING & ANALYTICS PROCESSES
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
# RENDER-SPECIFIC OPTIMIZATIONS
# =============================================================================

# --- Render Web Process (Primary for Render Deployment) ---
# This is the default process that Render will use
# Note: Render uses the 'web' process type by default

# --- Render Worker Process (Background Tasks) ---
# worker: python scripts/background_worker.py \
#   --queues high,medium,low \
#   --concurrency 3

# --- Render Cron Process (Scheduled Jobs) ---
# cron_worker: python scripts/cron_worker.py \
#   --schedule-file config/schedules.yaml \
#   --log-level info

# =============================================================================
# TROUBLESHOOTING & DEBUG PROFILES
# =============================================================================

# --- Debug Profile (Detailed Logging) ---
# Usage: heroku local debug
debug: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --workers 1 \
  --log-level debug \
  --loop asyncio \
  --reload \
  --reload-dir . \
  --header x-mode:debug

# --- Profile Mode (Performance Analysis) ---
# Usage: heroku local profile
profile: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --workers 1 \
  --log-level info \
  --loop asyncio \
  --header x-mode:profile

# --- Test Mode (Integration Testing) ---
# Usage: heroku local test
test: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --workers 1 \
  --log-level warning \
  --loop asyncio \
  --header x-mode:test
