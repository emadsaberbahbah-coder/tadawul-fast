"""
test_endpoints.py
===========================================================
SMOKE TESTER for Tadawul Fast Bridge API (v2.0)

Usage:
  python test_endpoints.py
  python test_endpoints.py --base http://127.0.0.1:8000 --token <APP_TOKEN>

Tests:
  - Root connectivity
  - Health endpoints
  - KSA enriched quote
  - Global analysis quote
  - Enriched sheet-rows batch (mix KSA + global)
"""

import argparse
import time
import requests

DEFAULT_BASE = "http://127.0.0.1:8000"
KSA_TEST_SYMBOL = "1120.SR"
GLOBAL_TEST_SYMBOL = "AAPL"

class Colors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"

def log(msg, typ="info"):
    if typ == "info":
        print(f"{Colors.OKBLUE}[INFO]{Colors.ENDC} {msg}")
    elif typ == "success":
        print(f"{Colors.OKGREEN}[PASS]{Colors.ENDC} {msg}")
    elif typ == "fail":
        print(f"{Colors.FAIL}[FAIL]{Colors.ENDC} {msg}")
    elif typ == "header":
        print(f"\n{Colors.HEADER}--- {msg} ---{Colors.ENDC}")

def _headers(token: str | None):
    h = {"User-Agent": "TFB-EndpointTester/2.0"}
    if token:
        h["X-APP-TOKEN"] = token
    return h

def check_server(base, token):
    log("Checking Server Connectivity...", "header")
    try:
        r = requests.get(f"{base}/", headers=_headers(token), timeout=15)
        if r.status_code == 200:
            data = r.json() if "application/json" in r.headers.get("Content-Type", "") else {}
            log(f"Server UP | version={data.get('version','?')}", "success")
            return True
        log(f"Server returned {r.status_code}: {r.text[:120]}", "fail")
        return False
    except Exception as e:
        log(f"Cannot connect: {e}", "fail")
        return False

def check_health(base, token):
    log("Checking Health Endpoints", "header")
    endpoints = [
        "/health",
        "/v1/enriched/health",
        "/v1/analysis/health",
        "/v1/advanced/health",
        "/v1/argaam/health",
        "/v1/legacy/health",
    ]
    for ep in endpoints:
        try:
            r = requests.get(f"{base}{ep}", headers=_headers(token), timeout=20)
            if r.status_code == 200:
                data = r.json()
                log(f"{ep.ljust(22)} -> {data.get('status','ok')}", "success")
            else:
                log(f"{ep.ljust(22)} -> HTTP {r.status_code}", "fail")
        except Exception as e:
            log(f"{ep.ljust(22)} -> Exception: {e}", "fail")

def check_quote(base, token, symbol, name, endpoint):
    log(f"Testing {name} ({symbol})", "header")
    start = time.time()
    try:
        url = f"{base}{endpoint}?symbol={symbol}"
        r = requests.get(url, headers=_headers(token), timeout=35)
        dt = time.time() - start
        if r.status_code != 200:
            log(f"HTTP {r.status_code}: {r.text[:200]}", "fail")
            return
        data = r.json()
        price = data.get("price") or data.get("last_price") or data.get("current_price")
        dq = data.get("data_quality")
        sym = data.get("symbol", "")
        if sym and price is not None:
            log(f"OK in {dt:.2f}s | {sym} price={price} dq={dq}", "success")
        else:
            log(f"Response missing symbol/price in {dt:.2f}s", "fail")
            print(data)
    except Exception as e:
        log(f"Request failed: {e}", "fail")

def check_batch(base, token):
    log("Testing Enriched /sheet-rows Batch (KSA + Global)", "header")
    payload = {"tickers": [KSA_TEST_SYMBOL, GLOBAL_TEST_SYMBOL], "sheet_name": "Test_Script"}
    try:
        start = time.time()
        r = requests.post(f"{base}/v1/enriched/sheet-rows", json=payload, headers=_headers(token), timeout=60)
        dt = time.time() - start
        if r.status_code != 200:
            log(f"Batch HTTP {r.status_code}: {r.text[:200]}", "fail")
            return
        data = r.json()
        rows = data.get("rows", [])
        if len(rows) == 2:
            log(f"Batch OK (2 rows) in {dt:.2f}s", "success")
        else:
            log(f"Batch unexpected row count: {len(rows)}", "fail")
            print(data)
    except Exception as e:
        log(f"Batch exception: {e}", "fail")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=DEFAULT_BASE)
    ap.add_argument("--token", default=None)
    args = ap.parse_args()

    base = args.base.rstrip("/")
    token = args.token

    print(f"{Colors.HEADER}TADAWUL FAST BRIDGE - ENDPOINT TESTER{Colors.ENDC}")
    if not check_server(base, token):
        print("\n❌ Cannot proceed. Please run 'python main.py' in another window.")
        return

    check_health(base, token)
    check_quote(base, token, KSA_TEST_SYMBOL, "KSA / Enriched", "/v1/enriched/quote")
    check_quote(base, token, GLOBAL_TEST_SYMBOL, "Global / AI", "/v1/analysis/quote")
    check_batch(base, token)

if __name__ == "__main__":
    main()
