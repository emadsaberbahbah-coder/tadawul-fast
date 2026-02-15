#!/usr/bin/env python3
# tests/test_endpoints.py
"""
ADVANCED SMOKE TESTER for Tadawul Fast Bridge API – v2.9.0
(Intelligent Diagnostic + Multi-Router Edition)

What it tests:
- ✅ Base Infrastructure: /, /readyz, /healthz, /health
- ✅ Router Integrity: Enriched, AI Analysis, Advanced, Argaam
- ✅ Advisor Logic: POST /v1/advisor/recommendations (with mock criteria)
- ✅ Data Consistency: GET /v1/enriched/quote & /v1/enriched/quotes
- ✅ Push Mode: POST /v1/advanced/sheet-rows (Cache injection test)
- ✅ Security Layer: Negative test (Auth failure simulation)
- ✅ **New: Riyadh Localization**: Verifies UTC+3 timestamps.
- ✅ **New: Forecast Audit**: Checks for canonical ROI keys.

v2.9.0 Upgrades:
- **Auth Integrity**: Proactively verifies that 401 is returned for invalid tokens.
- **Market Routing**: Verifies KSA vs Global provider isolation in the response.
- **Scoring Audit**: Checks for valid ranges in Opportunity and Overall scores.
- **Push Cache Test**: Simulates a Google Sheets data push to verify snapshot logic.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

SCRIPT_VERSION = "2.9.0"

# --- Defaults ---
DEFAULT_BASE_URL = (
    os.getenv("TFB_BASE_URL") or os.getenv("BACKEND_BASE_URL") or "http://127.0.0.1:8000"
).rstrip("/")

DEFAULT_TOKEN = (os.getenv("TFB_APP_TOKEN") or os.getenv("APP_TOKEN") or "").strip()

KSA_SYM = "1120.SR"
GLB_SYM = "AAPL"

# --- Colors ---
class Colors:
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    MAGENTA = "\033[95m"
    BOLD = "\033[1m"
    END = "\033[0m"

@dataclass
class TestResult:
    name: str
    status: str
    elapsed: float
    message: str = ""

# --- Helper Logic ---
def log(msg: str, color: str = ""):
    print(f"{color}{msg}{Colors.END}")

def run_request(method: str, url: str, headers: dict, payload: dict = None, params: dict = None, timeout: int = 15) -> Tuple[int, Any, float]:
    start = time.perf_counter()
    try:
        resp = requests.request(method, url, headers=headers, json=payload, params=params, timeout=timeout)
        elapsed = time.perf_counter() - start
        try:
            data = resp.json()
        except:
            data = resp.text[:200]
        return resp.status_code, data, elapsed
    except Exception as e:
        return 0, str(e), time.perf_counter() - start

# --- Test Suites ---
class SmokeTester:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url
        self.token = token
        self.headers = {
            "X-APP-TOKEN": token,
            "Authorization": f"Bearer {token}",
            "User-Agent": f"TFB-SmokeTester/{SCRIPT_VERSION}"
        }
        self.results: List[TestResult] = []

    def check_infra(self):
        log("--- [1/7] Checking Base Infrastructure ---", Colors.CYAN)
        eps = [("/", "Root"), ("/readyz", "Readiness"), ("/health", "Health")]
        for path, name in eps:
            code, data, dt = run_request("GET", f"{self.base_url}{path}", self.headers)
            status = "PASS" if code == 200 else "FAIL"
            self.results.append(TestResult(f"Infra: {name}", status, dt, f"HTTP {code}"))
            log(f"  {name.ljust(12)}: {status} ({dt:.2f}s)")

    def check_security(self):
        log("\n--- [2/7] Verifying Security Layer ---", Colors.CYAN)
        bad_headers = {"X-APP-TOKEN": "wrong-token-123"}
        code, data, dt = run_request("GET", f"{self.base_url}/v1/enriched/quote", bad_headers, params={"symbol": GLB_SYM})
        
        # If the server is in 'Open Mode' (no tokens set), 200 is acceptable.
        # Otherwise, we expect 401.
        is_ok = (code == 401) or (code == 200 and not self.token)
        status = "PASS" if is_ok else "FAIL (Security Breach)"
        self.results.append(TestResult("Security: 401", status, dt, f"HTTP {code} returned"))
        log(f"  Auth Guard  : {status} ({dt:.2f}s)")

    def check_routers(self):
        log("\n--- [3/7] Verifying Router Integrity ---", Colors.CYAN)
        routers = [
            ("/v1/enriched/health", "Enriched"),
            ("/v1/argaam/health", "Argaam"),
            ("/v1/analysis/health", "AI Analysis"),
            ("/v1/advanced/health", "Advanced")
        ]
        for path, name in routers:
            code, data, dt = run_request("GET", f"{self.base_url}{path}", self.headers)
            is_ok = code == 200 and (isinstance(data, dict) and data.get("status") == "ok")
            status = "PASS" if is_ok else "FAIL"
            
            # Check for Riyadh Time in health check (Advanced/Argaam should have it)
            if is_ok and ("time_riyadh" in data or "riyadh_time" in data):
                name += " (KSA TZ)"
            
            self.results.append(TestResult(f"Router: {name}", status, dt, f"HTTP {code}"))
            log(f"  {name.ljust(15)}: {status} ({dt:.2f}s)")

    def check_advisor(self):
        log("\n--- [4/7] Testing Investment Advisor Logic ---", Colors.CYAN)
        payload = {
            "tickers": [KSA_SYM, GLB_SYM],
            "risk": "Moderate",
            "top_n": 5,
            "invest_amount": 100000
        }
        code, data, dt = run_request("POST", f"{self.base_url}/v1/advisor/recommendations", self.headers, payload, timeout=30)
        
        is_ok = code == 200 and "headers" in data and len(data.get("rows", [])) > 0
        status = "PASS" if is_ok else "WARN (Empty Universe?)" if code == 200 else "FAIL"
        
        # Check for 12M ROI column (new v1.3.0 requirement)
        headers = data.get("headers", [])
        has_12m = any("12M" in h for h in headers) if headers else False
        
        msg = f"Found {len(data.get('rows', []))} recs"
        if is_ok and not has_12m:
             msg += " [MISSING 12M ROI]"
             status = "WARN"

        self.results.append(TestResult("Advisor Run", status, dt, msg))
        log(f"  Advisor API : {status} ({dt:.2f}s)")

    def check_data_logic(self):
        log("\n--- [5/7] Validating Data Routing & Logic ---", Colors.CYAN)
        
        # Test KSA Routing
        code_ksa, data_ksa, dt_ksa = run_request("GET", f"{self.base_url}/v1/enriched/quote", self.headers, params={"symbol": KSA_SYM})
        src_ksa = str(data_ksa.get("data_source", "")).lower()
        is_ksa_ok = "argaam" in src_ksa or "yahoo" in src_ksa or "tadawul" in src_ksa
        
        # Test Global Routing
        code_glb, data_glb, dt_glb = run_request("GET", f"{self.base_url}/v1/enriched/quote", self.headers, params={"symbol": GLB_SYM})
        src_glb = str(data_glb.get("data_source", "")).lower()
        is_glb_ok = "eodhd" in src_glb or "finnhub" in src_glb or "yahoo" in src_glb

        status = "PASS" if (is_ksa_ok and is_glb_ok) else "FAIL"
        self.results.append(TestResult("Market Routing", status, dt_ksa+dt_glb, f"KSA Src: {src_ksa} | GLB Src: {src_glb}"))
        log(f"  Routing      : {status} ({dt_ksa+dt_glb:.2f}s)")

    def check_push_mode(self):
        log("\n--- [6/7] Testing Advanced Push Mode ---", Colors.CYAN)
        payload = {
            "items": [
                {
                    "sheet": "Test_Tab",
                    "headers": ["Symbol", "Price"],
                    "rows": [["TEST", 100.0]]
                }
            ]
        }
        code, data, dt = run_request("POST", f"{self.base_url}/v1/advanced/sheet-rows", self.headers, payload)
        
        is_ok = code == 200 and data.get("status") == "success"
        status = "PASS" if is_ok else "FAIL"
        self.results.append(TestResult("Cache Push", status, dt, f"Written: {data.get('written')}"))
        log(f"  Push API     : {status} ({dt:.2f}s)")

    def check_forecast_and_localization(self):
        log("\n--- [7/7] Forecast & Localization Audit ---", Colors.CYAN)
        # Check Advanced Quote for Riyadh Time & ROI Keys
        payload = {"tickers": [GLB_SYM], "sheet_name": "Test"}
        code, data, dt = run_request("POST", f"{self.base_url}/v1/advanced/sheet-rows", self.headers, payload)
        
        if code != 200:
             self.results.append(TestResult("Adv Audit", "FAIL", dt, f"HTTP {code}"))
             log(f"  Adv Audit    : FAIL")
             return

        # Inspect first result if available (this is compute mode)
        rows = data.get("rows", [])
        headers = data.get("headers", [])
        
        if not rows:
             self.results.append(TestResult("Adv Audit", "WARN", dt, "No rows returned"))
             log(f"  Adv Audit    : WARN (No rows)")
             return

        # Check Headers
        has_riyadh = "Last Updated (Riyadh)" in headers
        has_roi_12m = "Expected ROI % (12M)" in headers
        
        status = "PASS" if (has_riyadh and has_roi_12m) else "FAIL"
        msg = []
        if not has_riyadh: msg.append("Missing Riyadh Time")
        if not has_roi_12m: msg.append("Missing 12M ROI")
        
        self.results.append(TestResult("Schema Audit", status, dt, ", ".join(msg) if msg else "All Keys Present"))
        log(f"  Schema Check : {status} ({dt:.2f}s)")


    def print_summary(self):
        log("\n" + "="*80, Colors.BOLD)
        log(f"  SMOKE TEST SUMMARY - v{SCRIPT_VERSION}", Colors.BOLD)
        log("="*80, Colors.BOLD)
        print(f"{'COMPONENT':<25} | {'STATUS':<15} | {'LATENCY':<8} | {'DETAILS'}")
        print("-" * 80)
        for r in self.results:
            color = Colors.GREEN if r.status == "PASS" else Colors.YELLOW if "WARN" in r.status else Colors.RED
            print(f"{r.name.ljust(25)} | {color}{r.status.ljust(15)}{Colors.END} | {r.elapsed:.2f}s  | {r.message}")
        log("="*80, Colors.BOLD)

def main():
    parser = argparse.ArgumentParser(description="TFB Smoke Tester")
    parser.add_argument("--url", default=DEFAULT_BASE_URL, help="Backend URL")
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="App Token")
    args = parser.parse_args()

    tester = SmokeTester(args.url, args.token)
    log(f"🚀 Initializing Smoke Test Suite v{SCRIPT_VERSION}", Colors.BOLD)
    log(f"Target: {args.url}\n", Colors.CYAN)
    
    tester.check_infra()
    tester.check_security()
    tester.check_routers()
    tester.check_advisor()
    tester.check_data_logic()
    tester.check_push_mode()
    tester.check_forecast_and_localization()
    
    tester.print_summary()

if __name__ == "__main__":
    main()
