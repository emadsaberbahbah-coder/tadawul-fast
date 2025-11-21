# =============================================================================
# Stock Market Hub Sheet API - Production Process File
# =============================================================================

# --- Web Process (Primary) ---
web: uvicorn main:app --host 0.0.0.0 --port $PORT --proxy-headers --forwarded-allow-ips="*" --log-level info --access-log --loop asyncio --workers 2

# --- Web Process (Alternative with Gunicorn for better performance) ---
# web: gunicorn main:app -w 2 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:$PORT --log-level info --access-logfile - --error-logfile - --timeout 120 --keep-alive 5

# --- Worker Process (Background Tasks) ---
worker: python -c "from advanced_analysis import analyzer; analyzer.start_background_worker()"

# --- Scheduler Process (Periodic Tasks) ---
scheduler: python -c "from advanced_market_dashboard import scheduler; scheduler.start()"

# --- Health Monitor Process ---
health: python -c "from health_monitor import start_monitoring; start_monitoring()"

# --- Cache Warmup Process (Startup) ---
cache_warmup: python -c "from cache_manager import warmup_cache; warmup_cache()"

# --- Development Process (Auto-reload) ---
dev: uvicorn main:app --host 0.0.0.0 --port $PORT --reload --log-level debug --loop asyncio

# --- Production with Metrics ---
web_metrics: uvicorn main:app --host 0.0.0.0 --port $PORT --proxy-headers --forwarded-allow-ips="*" --log-level info --access-log --loop asyncio --workers 4 --header x-powered-by:StockMarketAPI

# --- Minimal Memory Profile ---
web_minimal: uvicorn main:app --host 0.0.0.0 --port $PORT --log-level warning --loop asyncio --workers 1

# --- High Performance Profile ---
web_high_perf: uvicorn main:app --host 0.0.0.0 --port $PORT --proxy-headers --forwarded-allow-ips="*" --log-level info --loop asyncio --workers 8 --header server:StockMarketAPI

# --- SSL/TLS Enabled (if using SSL) ---
# web_ssl: uvicorn main:app --host 0.0.0.0 --port $PORT --ssl-keyfile=./ssl/private.key --ssl-certfile=./ssl/certificate.crt --proxy-headers --log-level info

# --- Custom Configuration ---
# Environment-specific processes
web_staging: uvicorn main:app --host 0.0.0.0 --port $PORT --log-level debug --reload --loop asyncio
web_production: uvicorn main:app --host 0.0.0.0 --port $PORT --proxy-headers --forwarded-allow-ips="*" --log-level info --loop asyncio --workers 4

# --- Backup Process ---
backup: python -c "from backup_manager import start_backup_scheduler; start_backup_scheduler()"

# --- Maintenance Mode ---
maintenance: uvicorn maintenance:app --host 0.0.0.0 --port $PORT --log-level info
