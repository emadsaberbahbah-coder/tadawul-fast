# =============================================================================
# Stock Market Hub Sheet API - Production Process File (Simplified)
# =============================================================================

# --- Primary Web Process (FastAPI + Uvicorn) ---
web: uvicorn main:app \
  --host 0.0.0.0 \
  --port $PORT \
  --proxy-headers \
  --forwarded-allow-ips="*" \
  --log-level info \
  --access-log \
  --loop asyncio \
  --workers 2

# =============================================================================
# OPTIONAL PROFILES (COMMENTED OUT - NOT RUN BY RENDER)
# Uncomment ONLY if you create these scripts and want extra paid processes.
# =============================================================================

# # --- Worker Process (Background Tasks) ---
# worker: python -c "from advanced_analysis import analyzer; analyzer.start_background_worker()"

# # --- Scheduler Process (Periodic Tasks) ---
# scheduler: python -c "from advanced_market_dashboard import scheduler; scheduler.start()"

# # --- Health Monitor Process ---
# health: python -c "from health_monitor import start_monitoring; start_monitoring()"

# # --- Cache Warmup Process (Startup) ---
# cache_warmup: python -c "from cache_manager import warmup_cache; warmup_cache()"

# # --- Development Process (Local Use Only) ---
# dev: uvicorn main:app --host 0.0.0.0 --port $PORT --reload --log-level debug --loop asyncio

# # --- Production with Metrics (Alternative) ---
# web_metrics: uvicorn main:app --host 0.0.0.0 --port $PORT --proxy-headers --forwarded-allow-ips="*" --log-level info --access-log --loop asyncio --workers 4 --header x-powered-by:StockMarketAPI

# # --- Minimal Memory Profile (Alternative) ---
# web_minimal: uvicorn main:app --host 0.0.0.0 --port $PORT --log-level warning --loop asyncio --workers 1

# # --- High Performance Profile (Alternative) ---
# web_high_perf: uvicorn main:app --host 0.0.0.0 --port $PORT --proxy-headers --forwarded-allow-ips="*" --log-level info --loop asyncio --workers 8 --header server:StockMarketAPI

# # --- Staging Profile (Alternative) ---
# web_staging: uvicorn main:app --host 0.0.0.0 --port $PORT --log-level debug --reload --loop asyncio

# # --- Explicit Production Profile (Alternative) ---
# web_production: uvicorn main:app --host 0.0.0.0 --port $PORT --proxy-headers --forwarded-allow-ips="*" --log-level info --loop asyncio --workers 4

# # --- Backup Process (Optional) ---
# backup: python -c "from backup_manager import start_backup_scheduler; start_backup_scheduler()"

# # --- Maintenance Mode (Optional) ---
# maintenance: uvicorn maintenance:app --host 0.0.0.0 --port $PORT --log-level info
