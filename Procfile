# ============================================================
# Procfile – Tadawul Fast Bridge (FastAPI)
# Deploy-safe for Render/Heroku
# ============================================================

web: uvicorn main:app --host 0.0.0.0 --port $PORT --proxy-headers --forwarded-allow-ips "*" --timeout-keep-alive 75 --log-level info
