# ============================================================
# Procfile — Tadawul Fast Bridge (FastAPI)
# Render/Heroku compatible
# ============================================================
# Notes:
# - Use WEB_CONCURRENCY if you want multiple workers (Render supports it).
# - Keep-alive tuned for Google Sheets / batch calls.
# - Proxy headers enabled for Render/Heroku routing.
# ============================================================

web: sh -c 'exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000} --proxy-headers --forwarded-allow-ips "*" --timeout-keep-alive ${UVICORN_KEEPALIVE:-75} --log-level ${LOG_LEVEL:-info} --workers ${WEB_CONCURRENCY:-1}'
