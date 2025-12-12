# ============================================================
# Procfile – Tadawul Fast Bridge (FastAPI + Google Sheets)
# v4.0.0 – Unified 9-page dashboard + KSA-safe data engine
# ============================================================

# Web process:
# - Runs FastAPI app from main.py (app = FastAPI(...))
# - Uses platform PORT (Render/Heroku injects $PORT)
# - --proxy-headers + --forwarded-allow-ips="*" for correct client IPs
# - --timeout-keep-alive tuned for Google Sheets / Apps Script bursts
web: uvicorn main:app --host 0.0.0.0 --port $PORT --proxy-headers --forwarded-allow-ips="*" --timeout-keep-alive 60
