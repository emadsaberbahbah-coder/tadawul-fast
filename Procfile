# =============================================================================
# Stock Market Hub API - Enhanced Production Process File
# Version: 2.4.0 | Environment: Production
# =============================================================================

# --- Primary Web Process (Optimized for Production) ---
web: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --proxy-headers \
  --forwarded-allow-ips="*" \
  --log-level info \
  --access-log \
  --loop asyncio \
  --workers ${WEB_CONCURRENCY:-2} \
  --timeout-keep-alive 5 \
  --timeout-graceful-shutdown 10 \
  --max-requests 1000 \
  --max-requests-jitter 100 \
  --header x-powered-by:StockMarketAPI/v2.4.0

# --- Health Monitor Process (Recommended) ---
health: python scripts/health_monitor.py \
  --api-url http://localhost:$PORT \
  --interval 30 \
  --timeout 10

# --- Cache Worker Process (Recommended) ---
cache_worker: python scripts/cache_manager.py \
  --mode worker \
  --cleanup-interval 300 \
  --backup-interval 3600

# --- Backup Scheduler (Critical for Data Safety) ---
backup_scheduler: python scripts/backup_manager.py \
  --mode scheduler \
  --interval 86400 \
  --retention 7

# =============================================================================
# ENVIRONMENT-SPECIFIC PROFILES
# =============================================================================

# --- Development Profile (Local Development) ---
dev: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --reload \
  --reload-dir . \
  --log-level debug \
  --loop asyncio \
  --workers 1

# --- Staging Profile (Pre-Production) ---
staging: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --proxy-headers \
  --forwarded-allow-ips="*" \
  --log-level info \
  --access-log \
  --loop asyncio \
  --workers 2

# --- High-Performance Profile (High Traffic) ---
web_high_perf: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --proxy-headers \
  --forwarded-allow-ips="*" \
  --log-level warning \
  --access-log \
  --loop asyncio \
  --workers ${WEB_CONCURRENCY:-4} \
  --max-requests 5000

# --- Minimal Memory Profile (Resource Constrained) ---
web_minimal: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --log-level warning \
  --loop asyncio \
  --workers 1 \
  --timeout-keep-alive 2

# =============================================================================
# OPTIONAL BACKGROUND PROCESSES
# =============================================================================

# --- Data Sync Worker (Synchronizes external data) ---
# data_sync: python scripts/data_sync_worker.py \
#   --sheets-interval 600 \
#   --quotes-interval 300

# --- Metrics Exporter (Prometheus metrics) ---
# metrics: python scripts/metrics_exporter.py \
#   --port 9090 \
#   --interval 15

# --- Notification Worker (Email/Alerts) ---
# notification_worker: python scripts/notification_worker.py \
#   --queue notifications \
#   --workers 2

# =============================================================================
# MAINTENANCE & ADMIN PROCESSES
# =============================================================================

# --- Maintenance Mode ---
maintenance: uvicorn maintenance:app \
  --host 0.0.0.0 \
  --port $PORT \
  --log-level info \
  --workers 1

# --- Database Maintenance (If using external DB) ---
# db_maintenance: python scripts/db_maintenance.py \
#   --vacuum-interval 86400

# =============================================================================
# UTILITY PROCESSES
# =============================================================================

# --- Cache Warmup (Startup Optimization) ---
cache_warmup: python scripts/cache_manager.py \
  --mode warmup \
  --symbols-file config/popular_symbols.json

# --- Log Aggregator (Centralized logging) ---
# log_aggregator: python scripts/log_aggregator.py \
#   --sources web,worker,cache \
#   --rotation hourly

# --- Performance Monitor (Real-time metrics) ---
# performance_monitor: python scripts/performance_monitor.py \
#   --monitor-interval 30
