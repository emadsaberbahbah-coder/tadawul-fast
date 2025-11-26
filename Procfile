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
  --header x-powered-by:StockMarketAPI/v2.4.0 \
  --header x-server:FastAPI-Uvicorn

# --- Health Monitor Process (Recommended) ---
health_monitor: python scripts/health_monitor.py \
  --api-url http://localhost:$PORT \
  --interval 30 \
  --timeout 10 \
  --retries 3

# --- Cache Management Worker (Recommended) ---
cache_worker: python scripts/cache_manager.py \
  --mode worker \
  --cleanup-interval 300 \
  --backup-interval 3600 \
  --max-memory 512

# --- Backup Scheduler (Critical for Data Safety) ---
backup_scheduler: python scripts/backup_manager.py \
  --mode scheduler \
  --interval 86400 \
  --retention 7 \
  --storage local \
  --max-size 100MB

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
  --workers 1 \
  --header x-environment:development

# --- Staging Profile (Pre-Production) ---
staging: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --proxy-headers \
  --forwarded-allow-ips="*" \
  --log-level info \
  --access-log \
  --loop asyncio \
  --workers 2 \
  --header x-environment:staging

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
  --header server:StockMarketAPI-HP \
  --header x-powered-by:FastAPI-Uvicorn-HighPerf

# --- Minimal Memory Profile (Resource Constrained) ---
web_minimal: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --log-level warning \
  --loop asyncio \
  --workers 1 \
  --timeout-keep-alive 2 \
  --max-requests 1000 \
  --max-requests-jitter 100 \
  --header x-profile:minimal-memory

# --- Readiness Check Profile (Load Balancer Health) ---
web_ready: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --log-level error \
  --loop asyncio \
  --workers 1 \
  --header x-purpose:readiness-check

# =============================================================================
# OPTIONAL BACKGROUND PROCESSES
# =============================================================================

# --- Data Sync Worker (Synchronizes external data) ---
# data_sync: python scripts/data_sync_worker.py \
#   --sheets-interval 600 \
#   --quotes-interval 300 \
#   --batch-size 50

# --- Metrics Exporter (Prometheus metrics) ---
# metrics: python scripts/metrics_exporter.py \
#   --port 9090 \
#   --endpoint /metrics \
#   --interval 15

# --- Email/Notification Worker (Optional) ---
# notification_worker: python scripts/notification_worker.py \
#   --queue notifications \
#   --workers 2 \
#   --retry-attempts 3

# =============================================================================
# SCHEDULED TASK PROCESSES
# =============================================================================

# --- Scheduled Tasks Scheduler (Optional) ---
# scheduler: python scripts/scheduled_tasks.py \
#   --config schedules.json \
#   --log-level info \
#   --max-concurrent 5

# --- Cache Warmup Scheduler (Recommended for Production) ---
cache_warmup: python scripts/cache_manager.py \
  --mode warmup \
  --symbols-file config/popular_symbols.json \
  --concurrency 10 \
  --interval 1800

# =============================================================================
# MAINTENANCE & ADMIN PROCESSES
# =============================================================================

# --- Maintenance Mode Process ---
maintenance: uvicorn maintenance:app \
  --host 0.0.0.0 \
  --port $PORT \
  --log-level info \
  --workers 1 \
  --header x-mode:maintenance

# --- Admin/Management Interface (Optional) ---
# admin: uvicorn admin_dashboard:app \
#   --host 0.0.0.0 \
#   --port 5000 \
#   --log-level info \
#   --workers 1

# --- Database Maintenance (If using external DB) ---
# db_maintenance: python scripts/db_maintenance.py \
#   --vacuum-interval 86400 \
#   --analyze-interval 43200 \
#   --backup-interval 86400

# =============================================================================
# UTILITY & SUPPORT PROCESSES
# =============================================================================

# --- Log Aggregator (Optional) ---
# log_aggregator: python scripts/log_aggregator.py \
#   --sources web,worker,cache \
#   --output logs/aggregated \
#   --rotation hourly \
#   --retention 168

# --- Performance Monitor (Optional) ---
# performance_monitor: python scripts/performance_monitor.py \
#   --monitor-interval 30 \
#   --metrics-port 9091 \
#   --alert-threshold 95

# --- Security Scanner (Optional) ---
# security_scanner: python scripts/security_scanner.py \
#   --scan-interval 3600 \
#   --report-file security_report.json \
#   --max-failures 3

# =============================================================================
# DEPRECATED PROCESSES (FOR REFERENCE)
# =============================================================================

# # --- Legacy Worker Process (Background Tasks) ---
# # worker: python -c "from advanced_analysis import analyzer; analyzer.start_background_worker()"

# # --- Legacy Scheduler Process (Periodic Tasks) ---
# # scheduler: python -c "from advanced_market_dashboard import scheduler; scheduler.start()"

# # --- Legacy Health Monitor Process ---
# # health: python -c "from health_monitor import start_monitoring; start_monitoring()"

# # --- Legacy Cache Warmup Process (Startup) ---
# # cache_warmup: python -c "from cache_manager import warmup_cache; warmup_cache()"

# # --- Legacy Backup Process (Optional) ---
# # backup: python -c "from backup_manager import start_backup_scheduler; start_backup_scheduler()"
