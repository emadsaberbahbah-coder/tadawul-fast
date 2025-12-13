# ============================================================
# Render Blueprint - Tadawul Fast Bridge (v4.0.0)
# ============================================================

services:
  - type: web
    name: tadawul-fast-bridge
    env: python
    # plan: starter  # Recommended for production (allows background workers)
    
    # Build & Start
    buildCommand: pip install -r requirements.txt
    startCommand: uvicorn main:app --host 0.0.0.0 --port $PORT --workers 2 --proxy-headers --forwarded-allow-ips "*" --timeout-keep-alive 120 --log-level info
    
    # Health Check
    healthCheckPath: /health
    
    # Environment Variables
    envVars:
      - key: APP_ENV
        value: production
      - key: APP_NAME
        value: Tadawul Fast Bridge
      - key: APP_VERSION
        value: 4.0.0
      - key: PYTHON_VERSION
        value: 3.11.9
      - key: LOG_LEVEL
        value: INFO

      # --- Connectivity ---
      - key: ENABLE_CORS_ALL_ORIGINS
        value: "true"
      - key: HTTP_TIMEOUT_SEC
        value: "25"

      # --- Providers (Secrets - Input in Dashboard) ---
      - key: ENABLED_PROVIDERS
        value: eodhd,fmp,yfinance
      - key: PRIMARY_PROVIDER
        value: eodhd
      - key: EODHD_API_KEY
        sync: false
      - key: FMP_API_KEY
        sync: false
      
      # --- Feature Flags ---
      - key: EODHD_FETCH_FUNDAMENTALS
        value: "1"
      - key: ENABLE_YFINANCE
        value: "1"

      # --- Auth (Secrets - Input in Dashboard) ---
      - key: APP_TOKEN
        sync: false
      - key: BACKUP_APP_TOKEN
        sync: false

      # --- Google Integration (Secrets - Input in Dashboard) ---
      - key: GOOGLE_SHEETS_CREDENTIALS
        sync: false
      - key: GOOGLE_APPS_SCRIPT_BACKUP_URL
        sync: false
      - key: DEFAULT_SPREADSHEET_ID
        sync: false

      # --- Batch Tuning (Optimized for Sheets Limits) ---
      - key: ADV_BATCH_SIZE
        value: "20"
      - key: ADV_BATCH_TIMEOUT_SEC
        value: "60"
      - key: ENRICHED_BATCH_SIZE
        value: "40"
      - key: ENRICHED_BATCH_TIMEOUT_SEC
        value: "45"
      - key: AI_BATCH_SIZE
        value: "20"
      - key: AI_BATCH_TIMEOUT_SEC
        value: "60"
      - key: ADV_BATCH_CONCURRENCY
        value: "5"
