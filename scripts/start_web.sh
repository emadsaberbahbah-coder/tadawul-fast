# ============================================================
# Tadawul Fast Bridge (FastAPI) — Render Blueprint
# PROD-SAFE + Backward-Compatible env var names (v1.1.7)
# ============================================================
#
# v1.1.7 changes vs v1.1.6
# - ✅ Move inline startCommand into repo script: scripts/start_web.sh
# - ✅ Auto-detect uvicorn graceful flag across versions
#
# Notes
# - Secrets must be set in Render Environment UI or via `sync: false` placeholders below.
# - If you rename the service, update BACKEND_BASE_URL/TFB_BASE_URL (or set in Render dashboard).

services:
  - type: web
    name: tadawul-fast-bridge
    env: python
    plan: free
    autoDeploy: true

    buildCommand: pip install -r requirements.txt

    # Clean deterministic startup
    startCommand: sh scripts/start_web.sh

    healthCheckPath: /healthz

    envVars:
      # -------------------------
      # Runtime
      # -------------------------
      - key: PYTHONUNBUFFERED
        value: "1"
      - key: TZ
        value: "Asia/Riyadh"

      # Concurrency / Uvicorn
      - key: WEB_CONCURRENCY
        value: "1"
      - key: UVICORN_KEEPALIVE
        value: "75"
      - key: UVICORN_GRACEFUL_TIMEOUT
        value: "30"
      - key: UVICORN_ACCESS_LOG
        value: "1"

      - key: LOG_LEVEL
        value: "info"
      - key: LOG_JSON
        value: "false"
      - key: DEBUG
        value: "false"

      # Fast-boot controls
      - key: DEFER_ROUTER_MOUNT
        value: "true"
      - key: INIT_ENGINE_ON_BOOT
        value: "true"

      # -------------------------
      # App identity (compat keys)
      # -------------------------
      - key: APP_NAME
        value: "Tadawul Fast Bridge"
      - key: SERVICE_NAME
        value: "Tadawul Fast Bridge"
      - key: APP_TITLE
        value: "Tadawul Fast Bridge"
      - key: APP_ENV
        value: "production"
      - key: ENVIRONMENT
        value: "production"

      # Version (align with deployed)
      - key: APP_VERSION
        value: "5.3.1"
      - key: SERVICE_VERSION
        value: "5.3.1"
      - key: VERSION
        value: "5.3.1"

      # Base URLs
      - key: BACKEND_BASE_URL
        value: "https://tadawul-fast-bridge.onrender.com"
      - key: TFB_BASE_URL
        value: "https://tadawul-fast-bridge.onrender.com"

      # -------------------------
      # Security / auth
      # -------------------------
      - key: REQUIRE_AUTH
        value: "false"
      - key: APP_TOKEN
        sync: false
      - key: BACKUP_APP_TOKEN
        sync: false
      - key: TFB_APP_TOKEN
        sync: false

      # -------------------------
      # Feature flags
      # -------------------------
      - key: AI_ANALYSIS_ENABLED
        value: "true"
      - key: ADVANCED_ANALYSIS_ENABLED
        value: "true"
      - key: ENABLE_RATE_LIMITING
        value: "true"
      - key: MAX_REQUESTS_PER_MINUTE
        value: "240"

      # -------------------------
      # Providers policy (GLOBAL)
      # -------------------------
      - key: ENABLED_PROVIDERS
        value: "eodhd,finnhub"
      - key: PROVIDERS
        value: "eodhd,finnhub"
      - key: PRIMARY_PROVIDER
        value: "eodhd"

      # -------------------------
      # KSA routing order (Option A: quiet + stable)
      # -------------------------
      - key: KSA_PROVIDERS
        value: "yahoo_chart"

      # Hard safety: never use EODHD for KSA
      - key: KSA_DISALLOW_EODHD
        value: "true"

      # Yahoo Chart fallbacks
      - key: ENABLE_YAHOO_CHART_KSA
        value: "true"
      - key: ENABLE_YAHOO_CHART_SUPPLEMENT
        value: "true"

      # yfinance policy (if engine references it)
      - key: ENABLE_YFINANCE
        value: "true"
      - key: ENABLE_YFINANCE_KSA
        value: "false"

      # Yahoo fundamentals toggles (if your engine supports them)
      - key: ENABLE_YAHOO_FUNDAMENTALS_KSA
        value: "true"
      - key: ENABLE_YAHOO_FUNDAMENTALS_GLOBAL
        value: "false"

      # -------------------------
      # Provider keys (SECRETS)
      # -------------------------
      - key: FMP_API_KEY
        sync: false
      - key: EODHD_API_KEY
        sync: false
      - key: FINNHUB_API_KEY
        sync: false
      - key: ALPHA_VANTAGE_API_KEY
        sync: false
      - key: ARGAAM_API_KEY
        sync: false

      # Optional explicit URLs (empty is fine)
      - key: TADAWUL_QUOTE_URL
        value: ""
      - key: TADAWUL_FUNDAMENTALS_URL
        value: ""
      - key: ARGAAM_GATEWAY_URL
        value: ""

      # -------------------------
      # HTTP + caching (compat)
      # -------------------------
      - key: HTTP_TIMEOUT_SEC
        value: "25"
      - key: HTTP_TIMEOUT
        value: "25"

      - key: CACHE_TTL_SEC
        value: "20"
      - key: CACHE_DEFAULT_TTL
        value: "20"

      - key: ENGINE_CACHE_TTL_SEC
        value: "20"

      - key: QUOTE_TTL_SEC
        value: "30"
      - key: FUNDAMENTALS_TTL_SEC
        value: "21600"
      - key: ARGAAM_SNAPSHOT_TTL_SEC
        value: "30"

      # -------------------------
      # Batch controls
      # -------------------------
      - key: ENRICHED_BATCH_SIZE
        value: "40"
      - key: ENRICHED_MAX_TICKERS
        value: "250"
      - key: ENRICHED_BATCH_CONCURRENCY
        value: "5"
      - key: ENRICHED_CONCURRENCY
        value: "5"
      - key: ENRICHED_TIMEOUT_SEC
        value: "45"

      - key: AI_BATCH_SIZE
        value: "20"
      - key: AI_MAX_TICKERS
        value: "500"
      - key: AI_CONCURRENCY
        value: "5"
      - key: AI_TIMEOUT_SEC
        value: "45"

      - key: ADV_BATCH_SIZE
        value: "25"
      - key: ADV_MAX_TICKERS
        value: "500"
      - key: ADV_CONCURRENCY
        value: "6"
      - key: ADV_TIMEOUT_SEC
        value: "45"

      # -------------------------
      # Google Sheets integration (SECRETS)
      # -------------------------
      - key: DEFAULT_SPREADSHEET_ID
        sync: false
      - key: GOOGLE_SHEETS_CREDENTIALS
        sync: false

      # -------------------------
      # CORS
      # -------------------------
      - key: ENABLE_CORS_ALL_ORIGINS
        value: "true"
      - key: CORS_ALL_ORIGINS
        value: "true"
      - key: CORS_ORIGINS
        value: "*"
