# ============================================================================
# TADAWUL FAST BRIDGE -- requirements.txt (v1.1.0)
# ============================================================================
# Render-safe pin set. No hashes, no pip commands, no banner text.
# Aligned with:
#   - main.py v8.11.1, config.py v7.3.0, env.py v7.8.1, worker.py v4.3.0
#   - render.yaml v1.1.0 (CACHE_BACKEND=redis, FUNDAMENTALS_ENABLED=true)
#
# Changes vs v1.0.0 (uploaded baseline)
# --------------------------------------
#   [CRITICAL] grpcio-status bumped 1.62.3 -> 1.64.3 to match grpcio==1.64.3.
#              Mismatched versions caused ImportError on google-api-python-client
#              boot (grpcio and grpcio-status MUST share major.minor).
#   [ORG]      Grouped packages into labeled sections for maintainability.
#   [DOC]      Added header comment + inline notes for transitive deps.
#   [DOC]      Labeled 18 packages under "Reserved / Currently unused".
#              These aren't imported anywhere in the current codebase; they're
#              pinned for forward-compat with planned features (rate-limiting,
#              full-text search, SQL backends, error tracking). Remove any
#              that won't be used within the next release to cut ~30-60s off
#              every Render deploy.
#
# Preserved from v1.0.0
# ---------------------
#   Every package and version pin. The only value change is grpcio-status.
# ============================================================================

# ----------------------------------------------------------------------------
# Web framework (FastAPI core)
# ----------------------------------------------------------------------------
fastapi==0.115.14
starlette==0.41.3
uvicorn[standard]==0.32.1
gunicorn==23.0.0
anyio==4.13.0
python-multipart==0.0.12

# ----------------------------------------------------------------------------
# Pydantic (models + settings)
# ----------------------------------------------------------------------------
pydantic==2.13.2
pydantic-core==2.46.2
pydantic-settings==2.13.1
typing-extensions==4.15.0

# ----------------------------------------------------------------------------
# Performance (JSON, event loop, DNS, date parsing)
# ----------------------------------------------------------------------------
orjson==3.11.8
uvloop==0.21.0; sys_platform != "win32"
httptools==0.6.4; sys_platform != "win32"
ciso8601==2.3.3; sys_platform != "win32"
aiodns==3.6.1; sys_platform != "win32"
rapidfuzz==3.14.5

# ----------------------------------------------------------------------------
# Security / crypto
# ----------------------------------------------------------------------------
cryptography==43.0.3
pyOpenSSL==24.3.0

# ----------------------------------------------------------------------------
# HTTP clients (httpx for internal, requests/aiohttp for provider adapters)
# ----------------------------------------------------------------------------
httpx==0.27.2
httpcore==1.0.9
h2==4.3.0
requests==2.33.1
aiohttp==3.13.5
websockets==13.1

# ----------------------------------------------------------------------------
# Market data providers
# ----------------------------------------------------------------------------
yfinance==0.2.66
pandas==2.3.3
numpy==1.26.4
pandas-market-calendars==4.6.1
exchange-calendars==4.13.2

# ----------------------------------------------------------------------------
# Translation (optional -- news_intelligence.py uses defensive import;
# service degrades gracefully if absent)
# ----------------------------------------------------------------------------
py-googletrans==4.0.0

# ----------------------------------------------------------------------------
# gRPC / protobuf (transitive: required by google-api-python-client and
# google-cloud-*). v1.1.0: grpcio-status bumped to 1.64.3 to match grpcio.
# ----------------------------------------------------------------------------
protobuf==4.25.8
grpcio==1.64.3
grpcio-status==1.64.3
googleapis-common-protos==1.64.0
proto-plus==1.24.0

# ----------------------------------------------------------------------------
# Google APIs (Sheets / Auth / Secret Manager)
# ----------------------------------------------------------------------------
google-api-python-client==2.194.0
google-auth==2.49.2
google-auth-httplib2==0.2.1
google-auth-oauthlib==1.3.1
google-api-core==2.30.3
google-cloud-secret-manager==2.27.0
gspread==6.2.1

# ----------------------------------------------------------------------------
# Cache layer (Redis is the canonical CACHE_BACKEND in render.yaml v1.1.0;
# aiomcache is the optional memcached fallback used by data_engine.py)
# ----------------------------------------------------------------------------
redis==5.3.1
hiredis==3.3.1
aiomcache==0.8.2

# ----------------------------------------------------------------------------
# Observability (Prometheus + OpenTelemetry -- used by 20+ modules)
# ----------------------------------------------------------------------------
prometheus-client==0.21.1
opentelemetry-api==1.27.0
opentelemetry-sdk==1.27.0
opentelemetry-instrumentation==0.48b0
opentelemetry-instrumentation-asgi==0.48b0
opentelemetry-instrumentation-fastapi==0.48b0
opentelemetry-exporter-otlp==1.27.0

# ----------------------------------------------------------------------------
# Config / env file loading (used by env.py v7.8.1 yaml/env-file branches)
# ----------------------------------------------------------------------------
python-dotenv==1.0.1
pyyaml==6.0.3

# ----------------------------------------------------------------------------
# Reserved / currently unused
# ----------------------------------------------------------------------------
# The following packages are pinned for forward-compatibility with planned
# features but are NOT imported anywhere in the current TFB codebase
# (verified via `grep -r 'import X\|from X' *.py`). You can safely remove
# any you know won't be used soon to cut ~30-60 seconds off each Render
# deploy and reduce container image size.
#
# If you remove a package, also remove any explicit dependency pin it
# brought in (e.g., `bcrypt` is only used by `passlib`).
# ----------------------------------------------------------------------------

# Rate limiting (TFB currently uses its own RATE_LIMIT_ENABLED flag + a
# built-in counter; slowapi/limits are the externalized alternative)
slowapi==0.1.9
limits==3.14.1

# Auth / password hashing (TFB uses token-based auth exclusively; these
# would only be needed if you add user accounts with hashed passwords)
PyJWT==2.12.1
passlib==1.7.4
bcrypt==4.3.0

# Structured logging alternatives (TFB uses stdlib logging + _JsonFormatter
# in main.py v8.11.1. loguru and structlog are unused.)
structlog==24.4.0
loguru==0.7.3

# Retry helpers (TFB implements retries inline in provider code; tenacity
# and backoff are unused)
tenacity==9.1.4
backoff==2.2.1

# SQL backend (TFB is stateless + Redis-cached; no Postgres layer)
sqlalchemy==2.0.49
asyncpg==0.30.0

# Error tracking (stub for future Sentry integration)
sentry-sdk==2.58.0

# Scraping / RSS / NLP (news_intelligence uses defensive imports; bs4/lxml/
# feedparser/nltk are stubs for expanded news ingestion)
beautifulsoup4==4.14.3
lxml==5.4.0
feedparser==6.0.12
nltk==3.9.4

# Async filesystem / generic cache (neither imported in current codebase)
aiofiles==24.1.0
aiocache==0.12.3
