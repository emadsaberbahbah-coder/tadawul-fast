"""
test_endpoints.py
===========================================================
SMOKE TESTER for Tadawul Fast Bridge API
===========================================================

Usage:
  1. Start your API in a separate terminal:
     python main.py

  2. Run this test script:
     python test_endpoints.py

What it tests:
  - Root connectivity
  - Router loading status
  - KSA Logic (Argaam gateway simulation)
  - Global Logic (EODHD/FMP)
  - Batch processing capabilities
"""

import sys
import time
import requests # standard library or pip install requests

# Configuration
BASE_URL = "http://127.0.0.1:8000"
KSA_TEST_SYMBOL = "1120.SR"  # Al Rajhi
GLOBAL_TEST_SYMBOL = "AAPL"  # Apple

class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

def log(msg, type="info"):
    if type == "info": print(f"{Colors.OKBLUE}[INFO]{Colors.ENDC} {msg}")
    elif type == "success": print(f"{Colors.OKGREEN}[PASS]{Colors.ENDC} {msg}")
    elif type == "fail": print(f"{Colors.FAIL}[FAIL]{Colors.ENDC} {msg}")
    elif type == "header": print(f"\n{Colors.HEADER}--- {msg} ---{Colors.ENDC}")

def check_server():
    log("Checking Server Connectivity...", "header")
    try:
        r = requests.get(f"{BASE_URL}/")
        if r.status_code == 200:
            log("Server is UP", "success")
            return True
        else:
            log(f"Server returned {r.status_code}", "fail")
            return False
    except Exception:
        log("Could not connect to localhost:8000. Is main.py running?", "fail")
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
            r = requests.get(f"{BASE_URL}{ep}")
            if r.status_code == 200:
                data = r.json()
                status = data.get("status", "unknown")
                log(f"{ep.ljust(25)} -> {status}", "success")
            else:
                log(f"{ep.ljust(25)} -> HTTP {r.status_code}", "fail")
        except Exception as e:
            log(f"{ep.ljust(25)} -> Exception: {e}", "fail")

def check_quote(symbol, route_name, endpoint):
    log(f"Testing {route_name} Logic ({symbol})", "header")
    start = time.time()
    try:
        # Note: All V2 routes use query param ?symbol=
        url = f"{BASE_URL}{endpoint}?symbol={symbol}"
        r = requests.get(url)
        duration = time.time() - start
        
        if r.status_code == 200:
            data = r.json()
            # Basic validation
            sym_resp = data.get("symbol", "")
            price = data.get("price") or data.get("last_price") or data.get("current_price")
            quality = data.get("data_quality")
            
            if sym_resp.upper() == symbol.upper() and price is not None:
                log(f"Response valid in {duration:.2f}s", "success")
                print(f"   Price: {price} | Quality: {quality} | Source: {data.get('data_source') or data.get('provider')}")
            else:
                log(f"Response invalid (Missing Price/Symbol mismatch)", "fail")
                print(data)
        else:
            log(f"HTTP {r.status_code}: {r.text}", "fail")
            
    except Exception as e:
        log(f"Request failed: {e}", "fail")

def check_batch():
    log("Testing Batch Processing (Mix)", "header")
    payload = {
        "tickers": [KSA_TEST_SYMBOL, GLOBAL_TEST_SYMBOL],
        "sheet_name": "Test_Script"
    }
    
    try:
        start = time.time()
        r = requests.post(f"{BASE_URL}/v1/enriched/sheet-rows", json=payload)
        duration = time.time() - start
        
        if r.status_code == 200:
            data = r.json()
            rows = data.get("rows", [])
            if len(rows) == 2:
                log(f"Batch processed 2 tickers in {duration:.2f}s", "success")
            else:
                log(f"Batch returned unexpected row count: {len(rows)}", "fail")
        else:
            log(f"Batch HTTP {r.status_code}", "fail")
    except Exception as e:
        log(f"Batch exception: {e}", "fail")

if __name__ == "__main__":
    print(f"{Colors.HEADER}TADAWUL FAST BRIDGE - ENDPOINT TESTER{Colors.ENDC}")
    if check_server():
        check_health()
        check_quote(KSA_TEST_SYMBOL, "KSA/Enriched", "/v1/enriched/quote")
        check_quote(GLOBAL_TEST_SYMBOL, "Global/AI", "/v1/analysis/quote")
        check_batch()
    else:
        print("\n❌ Cannot proceed. Please run 'python main.py' in another window.")
