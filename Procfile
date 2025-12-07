# Procfile – Tadawul Fast Bridge (FastAPI + Google Sheets)
# This runs your FastAPI app (main.py -> app) on Render
# and is fully compatible with the new data_engine / analysis / enriched routes.

web: uvicorn main:app --host 0.0.0.0 --port $PORT --proxy-headers --forwarded-allow-ips="*"
