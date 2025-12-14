"""
test_endpoints.py
===========================================================
SMOKE TESTER for Tadawul Fast Bridge API (v2)

Enhancements
- Supports token via env: TFB_APP_TOKEN or APP_TOKEN
- Supports base url via env: TFB_BASE_URL
- Tests HEAD / and /health (Render-friendly)
"""

import os
import time
import requests

BASE_URL = os.getenv("TFB_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
TOKEN = (os.getenv("TFB_APP_TOKEN") or os.getenv("APP_TOKEN") or "").strip()

KSA_TEST_SYMBOL = "1120.SR"
GLOBAL_TEST_SYMBOL = "AAPL"

class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

def log(msg, typ="info"):
    if typ == "info": print(f"{Colors.OKBLUE}[INFO]{Colors.ENDC} {msg}")
    elif typ == "success": print(f"{Colors.OKGREEN}[PASS]{Colors.ENDC} {msg}")
    elif typ == "fail": print(f"{Colors.FAIL}[FAIL]{Colors.ENDC} {msg}")
    elif typ == "header": print(f"\n{Colors.HEADER}--- {msg} ---{Colors.ENDC}")

def headers():
    h = {}
    if TOKEN:
        h["X-APP-TOKEN"] = TOKEN
    return h

def check_server():
    log("Checking Server Connectivity...", "header")
    try:
        r = requests.get(f"{BASE_URL}/", headers=headers(), timeout=15)
        if r.status_code == 200:
            log("GET / -> Server UP", "success")
        else:
            log(f"GET / -> HTTP {r.status_code}", "fail")
            return False

        # Render sometimes uses HEAD
        rh = requests.head(f"{BASE_URL}/", headers=headers(), timeout=15)
        if rh.status_code in (200, 204):
            log("HEAD / -> OK", "success")
        else:
            log(f"HEAD / -> HTTP {rh.status_code}", "fail")

        return True
    except Exception as e:
        log(f"Cannot connect: {e}", "fail")
        return False

def check_health():
    log("Checking Router Health", "header")
    endpoints = [
        "/health",
        "/v1/enriched/health",
        "/v1/analysis/health",
        "/v1/advanced/health",
        "/v1/argaam/health"
    ]
    for ep in endpoints:
        try:
            r = requests.get(f"{BASE_URL}{ep}", headers=headers(), timeout=20)
            if r.status_code == 200:
                status = (r.json() or {}).get("status", "unknown")
                log(f"{ep.ljust(25)} -> {status}", "success")
            else:
                log(f"{ep.ljust(25)} -> HTTP {r.status_code}", "fail")
        except Exception as e:
            log(f"{ep.ljust(25)} -> Exception: {e}", "fail")

def check_quote(symbol, name, endpoint):
    log(f"Testing {name} ({symbol})", "header")
    start = time.time()
    try:
        url = f"{BASE_URL}{endpoint}?symbol={symbol}"
        r = requests.get(url, headers=headers(), timeout=45)
        dt = time.time() - start

        if r.status_code == 200:
            data = r.json()
            sym_resp = (data.get("symbol") or "").upper()
            price = data.get("last_price") or data.get("price") or data.get("current_price")
            if sym_resp == symbol.upper() and price is not None:
                log(f"Valid response in {dt:.2f}s | price={price}", "success")
            else:
                log("Response returned but missing fields (check data providers)", "fail")
                print(data)
        else:
            log(f"HTTP {r.status_code}: {r.text[:200]}", "fail")
    except Exception as e:
        log(f"Request failed: {e}", "fail")

def check_batch():
    log("Testing Batch /v1/enriched/sheet-rows", "header")
    payload = {"tickers": [KSA_TEST_SYMBOL, GLOBAL_TEST_SYMBOL], "sheet_name": "Test_Script"}
    try:
        start = time.time()
        r = requests.post(f"{BASE_URL}/v1/enriched/sheet-rows", json=payload, headers=headers(), timeout=90)
        dt = time.time() - start

        if r.status_code == 200:
            data = r.json()
            rows = data.get("rows", [])
            if len(rows) == 2:
                log(f"Batch OK (2 tickers) in {dt:.2f}s", "success")
            else:
                log(f"Batch returned {len(rows)} rows (expected 2)", "fail")
                print(data)
        else:
            log(f"Batch HTTP {r.status_code}: {r.text[:200]}", "fail")
    except Exception as e:
        log(f"Batch exception: {e}", "fail")

if __name__ == "__main__":
    print(f"{Colors.HEADER}TADAWUL FAST BRIDGE - ENDPOINT TESTER{Colors.ENDC}")
    print(f"BASE_URL={BASE_URL} | TOKEN={'SET' if TOKEN else 'NOT SET'}")
    if check_server():
        check_health()
        check_quote(KSA_TEST_SYMBOL, "Enriched Quote", "/v1/enriched/quote")
        check_quote(GLOBAL_TEST_SYMBOL, "Enriched Quote", "/v1/enriched/quote")
        check_batch()
    else:
        print("\n❌ Cannot proceed. Run your API first (or set TFB_BASE_URL).")
