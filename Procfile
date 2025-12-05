# =============================================================================
# Tadawul Fast Bridge API - Procfile
# Version: 3.6.0 | Environment: Production-Ready
# =============================================================================
# Notes:
# - On Render, ONLY the "web:" process is used for the main service.
# - Other process types (data_refresher, cache_manager, etc.) are OPTIONAL
#   templates for Heroku / local / Docker Compose and are safe to keep as docs
#   as long as you don't scale them.
# =============================================================================

# --- PRIMARY WEB PROCESS (Production / Render) ---
# Safe options for uvicorn==0.24.0 + uvicorn[standard]
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
  --limit-concurrency ${MAX_CONCURRENCY:-100} \
  --limit-max-requests ${MAX_REQUESTS:-1000} \
  --header x-powered-by:"TadawulFastBridge/3.6.0" \
  --header x-server:"FastAPI-Uvicorn" \
  --header strict-transport-security:"max-age=31536000; includeSubDomains" \
  --header x-content-type-options:"nosniff" \
  --header x-frame-options:"DENY" \
  --header permissions-policy:"interest-cohort=()" \
  --header referrer-policy:"strict-origin-when-cross-origin"

# =============================================================================
# OPTIONAL WORKERS (only use if you actually implement these modules)
# =============================================================================

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

# =============================================================================
# OPTIONAL MAINTENANCE WORKERS (only if DB / log modules exist)
# =============================================================================

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

# =============================================================================
# PERFORMANCE PROFILES (used manually, not by Render)
# =============================================================================

# High-Performance Profile (8 workers, more throughput)
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

# Memory-Optimized Profile (for small instances)
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

# =============================================================================
# SPECIALIZED API PROFILES (ONLY if these modules exist)
# =============================================================================

# Readiness/Liveness Check-only Service
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

# =============================================================================
# ENVIRONMENT-SPECIFIC PROFILES
# =============================================================================

# Local Development Profile
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

# =============================================================================
# TROUBLESHOOTING & DEBUG PROFILES
# =============================================================================

# Debug Profile (detailed logging + external YAML config)
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

# Single Worker Profile (for concurrency debugging)
single: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers 1 \
  --loop asyncio \
  --log-level debug \
  --header x-worker:single

# =============================================================================
# MAINTENANCE MODE (ONLY if maintenance:app is implemented)
# =============================================================================

maintenance: uvicorn maintenance:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers 1 \
  --log-level info \
  --loop asyncio \
  --header x-mode:maintenance \
  --header retry-after:3600

# =============================================================================
# LOAD TEST / GATEWAY PROFILES (manual use only)
# =============================================================================

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

# API Gateway Profile (if used behind another proxy)
gateway: uvicorn main:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --workers 2 \
  --loop uvloop \
  --proxy-headers \
  --forwarded-allow-ips="*" \
  --log-level warning \
  --header x-role:api-gateway
