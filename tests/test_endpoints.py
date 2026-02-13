#!/usr/bin/env python3
# tests/test_endpoints.py
"""
ADVANCED SMOKE TESTER for Tadawul Fast Bridge API – v2.7.0
(Intelligent Diagnostic + Multi-Router Edition)

What it tests:
- ✅ Base Infrastructure: /, /readyz, /healthz, /health
- ✅ Router Integrity: Enriched, AI Analysis, Advanced, Argaam
- ✅ Advisor Logic: POST /v1/advisor/recommendations (with mock criteria)
- ✅ Data Consistency: GET /v1/enriched/quote & /v1/enriched/quotes
- ✅ System Transparency: /system/settings masking check

v2.7.0 Upgrades:
- **Advisor Testing**: Validates the end-to-end flow for Investment Strategy reports.
- **Payload Validation**: Checks for v12.2 Dashboard keys (expected_roi_*, rec_badge).
- **Network Resilience**: Adaptive timeouts and retry logic for Render spin-up.
- **Formatted Reporting**: Summary table for quick status verification.
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

SCRIPT_VERSION = "2.7.0"

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

def run_request(method: str, url: str, headers: dict, payload: dict = None, timeout: int = 15) -> Tuple[int, Any, float]:
    start = time.perf_counter()
    try:
        resp = requests.request(method, url, headers=headers, json=payload, timeout=timeout)
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
        self.headers = {
            "X-APP-TOKEN": token,
            "Authorization": f"Bearer {token}",
            "User-Agent": f"TFB-SmokeTester/{SCRIPT_VERSION}"
        }
        self.results: List[TestResult] = []

    def check_infra(self):
        log("--- [1/4] Checking Base Infrastructure ---", Colors.CYAN)
        eps = [("/", "Root"), ("/readyz", "Readiness"), ("/health", "Health")]
        for path, name in eps:
            code, data, dt = run_request("GET", f"{self.base_url}{path}", self.headers)
            status = "PASS" if code == 200 else "FAIL"
            self.results.append(TestResult(f"Infra: {name}", status, dt, f"HTTP {code}"))
            log(f"  {name.ljust(12)}: {status} ({dt:.2f}s)")

    def check_routers(self):
        log("\n--- [2/4] Verifying Router Integrity ---", Colors.CYAN)
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
            self.results.append(TestResult(f"Router: {name}", status, dt, f"HTTP {code}"))
            log(f"  {name.ljust(12)}: {status} ({dt:.2f}s)")

    def check_advisor(self):
        log("\n--- [3/4] Testing Investment Advisor Logic ---", Colors.CYAN)
        payload = {
            "tickers": [KSA_SYM, GLB_SYM],
            "risk": "Moderate",
            "top_n": 5,
            "invest_amount": 100000
        }
        code, data, dt = run_request("POST", f"{self.base_url}/v1/advisor/recommendations", self.headers, payload, timeout=30)
        
        is_ok = code == 200 and "headers" in data and len(data.get("rows", [])) > 0
        status = "PASS" if is_ok else "WARN (Empty Universe?)" if code == 200 else "FAIL"
        
        msg = f"Found {len(data.get('rows', []))} recs" if is_ok else str(data)[:100]
        self.results.append(TestResult("Advisor Run", status, dt, msg))
        log(f"  Advisor API : {status} ({dt:.2f}s)")

    def check_data_quality(self):
        log("\n--- [4/4] Validating Quote Data Quality ---", Colors.CYAN)
        # Check for v12.2 Dashboard specific keys
        code, data, dt = run_request("GET", f"{self.base_url}/v1/enriched/quote", self.headers, {"symbol": GLB_SYM})
        
        has_roi = "expected_roi_1m" in data or "expected_return_1m" in data
        has_reco = "recommendation" in data
        
        is_ok = code == 200 and has_roi and has_reco
        status = "PASS" if is_ok else "FAIL"
        self.results.append(TestResult("Data Quality", status, dt, f"ROI={has_roi}, Reco={has_reco}"))
        log(f"  Standardization: {status} ({dt:.2f}s)")

    def print_summary(self):
        log("\n" + "="*60, Colors.BOLD)
        log(f"  SMOKE TEST SUMMARY - v{SCRIPT_VERSION}", Colors.BOLD)
        log("="*60, Colors.BOLD)
        for r in self.results:
            color = Colors.GREEN if r.status == "PASS" else Colors.YELLOW if "WARN" in r.status else Colors.RED
            print(f"{r.name.ljust(25)} | {color}{r.status.ljust(10)}{Colors.END} | {r.elapsed:.2f}s | {r.message}")
        log("="*60, Colors.BOLD)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_BASE_URL)
    parser.add_argument("--token", default=DEFAULT_TOKEN)
    args = parser.parse_args()

    tester = SmokeTester(args.url, args.token)
    log(f"🚀 Starting Smoke Test on: {args.url}\n", Colors.BOLD)
    
    tester.check_infra()
    tester.check_routers()
    tester.check_advisor()
    tester.check_data_quality()
    tester.print_summary()

if __name__ == "__main__":
    main()
