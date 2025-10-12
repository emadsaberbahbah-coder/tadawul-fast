# main.py — STAGE 1 (stubbed, never 500)
import os, time
from typing import List, Dict, Any
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

APP_TOKEN = os.getenv("APP_TOKEN", "change-me")
USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)

app = FastAPI(title="tadawul-fast (stub)", version="v1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def _check_auth(request: Request):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = auth.split(" ", 1)[1].strip()
    if token != APP_TOKEN:
        raise HTTPException(status_code=403, detail="invalid token")

@app.get("/")
def root():
    return {
        "ok": True,
        "service": "tadawul-fast (stub)",
        "endpoints": ["/health", "/debug/headers", "/v33/quotes (POST)", "/v33/charts (POST)", "/v33/fund/{code} (GET)"]
    }

@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time())}

@app.get("/debug/headers")
def debug_headers(request: Request):
    # helps verify your Authorization header reaches the server
    auth = request.headers.get("Authorization", "")
    return {"saw_authorization": auth[:32] + ("..." if len(auth) > 32 else ""), "token_matches": auth.endswith(APP_TOKEN)}

@app.post("/v33/quotes")
async def quotes(request: Request):
    # STUB: no external calls; always 200 with predictable shape
    try:
        _check_auth(request)
    except HTTPException:
        raise
    except Exception:
        return {"data": {}}

    try:
        body = await request.json()
    except Exception:
        body = {}

    symbols: List[str] = [str(s).strip().upper() for s in (body.get("symbols") or []) if str(s).strip()]

    # Minimal shape your Apps Script expects
    out: Dict[str, Any] = {}
    for s in symbols:
        out[s] = {
            "price": None,
            "chgPct": None,
            "name": s,
            "sector": None,
            "industry": None,
            "dayHigh": None,
            "dayLow": None,
            "volume": None,
            "marketCap": None,
            "sharesOutstanding": None,
            "dividendYield": None,
            "eps": None,
            "pe": None,
            "beta": None,
        }
    return {"data": out}

@app.post("/v33/charts")
async def charts(request: Request):
    try:
        _check_auth(request)
    except HTTPException:
        raise
    except Exception:
        return {"data": {}}

    try:
        body = await request.json()
    except Exception:
        body = {}
    symbols: List[str] = [str(s).strip().upper() for s in (body.get("symbols") or []) if str(s).strip()]
    out: Dict[str, Any] = {s: [] for s in symbols}
    return {"data": out}

@app.get("/v33/fund/{code}")
async def fund(request: Request, code: str):
    try:
        _check_auth(request)
    except HTTPException:
        raise
    except Exception:
        return {"data": []}
    return {"data": []}
