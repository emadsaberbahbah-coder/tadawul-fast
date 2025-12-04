# =============================================================================
# Tadawul Fast Bridge API - Enhanced Production Procfile
# Version: 4.0.0 | Environment: Production-Ready
# =============================================================================
# This Procfile defines process types for production deployment
# Primary process: web (Render/Heroku)
# Additional workers can be deployed as separate services
# =============================================================================

# --- PRIMARY WEB PROCESS (Production) ---
# Optimized for Render/Heroku deployment
web: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers ${WEB_CONCURRENCY:-4} \
  --loop uvloop \
  --http httptools \
  --proxy-headers \
  --forwarded-allow-ips="*" \
  --log-level ${LOG_LEVEL:-info} \
  --access-log \
  --timeout-keep-alive 65 \
  --timeout-graceful-shutdown 30 \
  --limit-concurrency ${MAX_CONCURRENCY:-100} \
  --limit-max-requests ${MAX_REQUESTS:-1000} \
  --header x-powered-by:"TadawulFastBridge/4.0.0" \
  --header x-server:"FastAPI-Uvicorn" \
  --header strict-transport-security:"max-age=31536000; includeSubDomains" \
  --header x-content-type-options:"nosniff" \
  --header x-frame-options:"DENY" \
  --header permissions-policy:"interest-cohort=()" \
  --header referrer-policy:"strict-origin-when-cross-origin"

# --- BACKGROUND WORKERS (Deploy as separate services) ---

# Real-time Data Refresher Worker
data_refresher: python -m workers.data_refresher \
  --interval 300 \
  --batch-size 50 \
  --symbols-file config/priority_symbols.json \
  --concurrency 10 \
  --metrics-port 9091 \
  --log-level info

# Cache Management & Warmup Worker
cache_manager: python -m workers.cache_manager \
  --mode combined \
  --cleanup-interval 600 \
  --warmup-interval 1800 \
  --max-memory-mb 512 \
  --redis-url ${REDIS_URL:-redis://localhost:6379} \
  --log-level info

# AI Analysis Queue Worker
ai_worker: python -m workers.ai_analysis_worker \
  --queue-name ai-analysis \
  --concurrency 3 \
  --timeout 300 \
  --max-retries 3 \
  --log-level info

# Monitoring & Alerting Worker
monitor_worker: python -m workers.monitor \
  --api-url https://${RENDER_SERVICE_NAME}.onrender.com \
  --interval 30 \
  --timeout 10 \
  --health-endpoints /health,/v1/analysis/health,/v1/enriched-quote/health \
  --metrics-port 9092 \
  --alert-webhook ${ALERT_WEBHOOK_URL} \
  --log-level warning

# Scheduled Tasks Scheduler
scheduler: python -m workers.scheduler \
  --tasks-config config/scheduled_tasks.yaml \
  --timezone Asia/Riyadh \
  --log-level info \
  --max-concurrent-tasks 5

# --- MAINTENANCE WORKERS (Optional, scheduled) ---

# Database Maintenance Worker (if applicable)
db_maintainer: python -m workers.db_maintainer \
  --vacuum-interval 86400 \
  --analyze-interval 43200 \
  --backup-interval 21600 \
  --max-backup-age 168 \
  --log-level info

# Log Rotation & Cleanup Worker
log_manager: python -m workers.log_manager \
  --rotation-size 100 \
  --retention-days 7 \
  --compress-old true \
  --max-total-size 1024 \
  --log-level info

# --- PERFORMANCE OPTIMIZATION PROFILES ---

# High-Performance Profile (8 workers, aggressive timeouts)
web_high_perf: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers ${WEB_CONCURRENCY:-8} \
  --loop uvloop \
  --http httptools \
  --limit-concurrency 200 \
  --limit-max-requests 2000 \
  --timeout-keep-alive 30 \
  --log-level warning \
  --header x-profile:high-performance

# Memory-Optimized Profile (for resource-constrained environments)
web_light: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers 1 \
  --loop asyncio \
  --limit-concurrency 50 \
  --limit-max-requests 500 \
  --timeout-keep-alive 20 \
  --log-level warning \
  --no-access-log \
  --header x-profile:memory-optimized

# --- SPECIALIZED API ENDPOINTS ---

# Readiness/Liveness Check Endpoint Only
web_health: uvicorn health:app \
  --host 0.0.0.0 \
  --port ${HEALTH_PORT:-8080} \
  --workers 1 \
  --log-level error \
  --loop asyncio \
  --no-access-log \
  --header x-service:health-check

# Metrics & Monitoring Endpoint
web_metrics: uvicorn metrics:app \
  --host 0.0.0.0 \
  --port ${METRICS_PORT:-9090} \
  --workers 1 \
  --log-level warning \
  --loop asyncio \
  --header x-service:metrics

# --- ENVIRONMENT-SPECIFIC PROFILES ---

# Development Profile (for local development)
dev: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --reload \
  --reload-dir . \
  --reload-include "*.py" \
  --reload-exclude "*.log,*.txt,__pycache__" \
  --loop asyncio \
  --workers 1 \
  --log-level debug \
  --access-log \
  --header x-environment:development \
  --header x-server:local-dev

# Staging Profile
staging: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers 2 \
  --loop uvloop \
  --log-level info \
  --access-log \
  --header x-environment:staging

# --- TROUBLESHOOTING & DEBUG PROFILES ---

# Debug Profile (detailed logging)
debug: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --reload \
  --reload-dir . \
  --loop asyncio \
  --workers 1 \
  --log-level debug \
  --log-config logging_debug.yaml \
  --header x-mode:debug

# Single Worker Profile (for debugging concurrency issues)
single: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers 1 \
  --loop asyncio \
  --log-level debug \
  --header x-worker:single

# --- MAINTENANCE MODE ---

# Maintenance Mode (simple static response)
maintenance: uvicorn maintenance:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers 1 \
  --log-level info \
  --loop asyncio \
  --header x-mode:maintenance \
  --header retry-after:3600

# --- SPECIALIZED WORKFLOWS ---

# Load Testing Profile
load_test: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers ${WEB_CONCURRENCY:-4} \
  --loop uvloop \
  --http httptools \
  --log-level error \
  --no-access-log \
  --limit-concurrency 1000 \
  --header x-mode:load-test

# API Gateway Profile (reverse proxy scenarios)
gateway: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers 2 \
  --loop uvloop \
  --proxy-headers \
  --forwarded-allow-ips="*" \
  --log-level warning \
  --header x-role:api-gateway

# =============================================================================
# SUPPORTING CONFIGURATION FILES
# =============================================================================

# logging_debug.yaml (for debug profile)
# logging:
#   version: 1
#   disable_existing_loggers: false
#   formatters:
#     detailed:
#       format: '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
#   handlers:
#     console:
#       class: logging.StreamHandler
#       formatter: detailed
#   loggers:
#     uvicorn:
#       level: DEBUG
#     uvicorn.error:
#       level: DEBUG
#     uvicorn.access:
#       level: INFO
#       propagate: false

# config/scheduled_tasks.yaml
# tasks:
#   - name: cache_warmup
#     schedule: "*/30 * * * *"  # Every 30 minutes
#     command: "python -m workers.cache_warmup"
#     timeout: 300
#   
#   - name: data_refresh
#     schedule: "*/15 * * * *"  # Every 15 minutes
#     command: "python -m workers.data_refresh"
#     timeout: 600
#   
#   - name: daily_report
#     schedule: "0 8 * * *"  # 8:00 AM daily (Riyadh time)
#     command: "python -m workers.generate_reports"
#     timeout: 1800

# =============================================================================
# DEPLOYMENT NOTES
# =============================================================================
# 
# RENDER.COM:
# - Only the 'web:' process will be executed for the main web service
# - For background workers, create separate "Worker" services
# - Use environment variables in Render dashboard:
#   * WEB_CONCURRENCY: 4 (adjust based on pricing tier)
#   * LOG_LEVEL: info
#   * MAX_CONCURRENCY: 100
#   * MAX_REQUESTS: 1000 (for graceful worker recycling)
# 
# HEROKU:
# - All process types can be scaled independently:
#   * heroku ps:scale web=2 cache_manager=1 data_refresher=1
#   * heroku ps:scale scheduler=1 monitor_worker=1
# 
# DOCKER COMPOSE:
# - Map each process type to a separate service in docker-compose.yml
# 
# LOCAL DEVELOPMENT:
# - Use 'dev' or 'debug' profiles
# - Start workers individually if needed:
#   * honcho start dev
#   * honcho start -f Procfile.dev
# 
# MONITORING:
# - Each worker exposes metrics on separate ports
# - Use Prometheus/Grafana for monitoring
# - Set up alerts for critical failures
# 
# =============================================================================
# ENVIRONMENT VARIABLE REFERENCE
# =============================================================================
# 
# Required:
# PORT: Application port (automatically set by platform)
# 
# Recommended:
# WEB_CONCURRENCY: Number of worker processes (default: 4)
# LOG_LEVEL: Logging level (debug, info, warning, error, critical)
# MAX_CONCURRENCY: Maximum concurrent requests per worker
# MAX_REQUESTS: Restart worker after N requests (memory management)
# REDIS_URL: Redis connection URL for caching
# DATABASE_URL: Database connection URL
# ALERT_WEBHOOK_URL: Webhook for sending alerts
# 
# Optional:
# HEALTH_PORT: Port for health check service (default: 8080)
# METRICS_PORT: Port for metrics service (default: 9090)
# RENDER_SERVICE_NAME: For internal health checks
# 
# =============================================================================
# SECURITY HEADERS EXPLANATION
# =============================================================================
# 
# x-powered-by: Custom header (can be disabled in production)
# x-server: Server identification
# strict-transport-security: Enforce HTTPS
# x-content-type-options: Prevent MIME type sniffing
# x-frame-options: Prevent clickjacking
# permissions-policy: Privacy-focused feature controls
# referrer-policy: Control referrer information
# 
# =============================================================================
# PERFORMANCE TUNING
# =============================================================================
# 
# Optimal worker count: 2-4 x CPU cores
# Keep-alive timeout: 30-65 seconds
# Max concurrent requests: 50-200 per worker
# Max requests per worker: 1000-2000 (prevents memory leaks)
# Use uvloop for better async performance (Linux only)
# Use httptools for faster HTTP parsing
# 
# =============================================================================
