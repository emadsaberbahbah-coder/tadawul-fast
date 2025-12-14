"""
test_endpoints.py
===========================================================
SMOKE TESTER for Tadawul Fast Bridge API (Enhanced)
===========================================================

What it tests (defensive, schema-tolerant):
  - Root connectivity + /health
  - Router health endpoints:
      /v1/enriched/health
      /v1/analysis/health
      /v1/advanced/health
      /v1/argaam/health
      /v1/legacy/health
  - Single-quote endpoints (multiple param aliases supported):
      /v1/enriched/quote?symbol=...
      /v1/analysis/quote?symbol=...
      /v1/argaam/quote?symbol=...
      /v1/quote?symbol=...      (legacy)
  - Sheet endpoints (POST):
      /v1/enriched/sheet-rows
      /v1/analysis/sheet-rows
      /v1/advanced/sheet-rows

Usage:
  # Basic
  python test_endpoints.py

  # Override base url & token (recommended for Render):
  set BASE_URL=https://tadawul-fast-bridge.onrender.com
  set APP_TOKEN=xxxxx-xxxxx
  python test_endpoints.py

Notes:
  - This script NEVER assumes exact response schema. It searches for common keys.
  - It prints clear PASS/FAIL and a short diagnosis for each endpoint.
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
except Exception:
    print("❌ Missing dependency: requests. Install with: pip install requests")
    raise


# -----------------------------
# Configuration (env overrides)
# -----------------------------
BASE_URL = (os.getenv("BASE_URL") or os.getenv("BACKEND_BASE_URL") or "http://127.0.0.1:8000").rstrip("/")
APP_TOKEN = (os.getenv("APP_TOKEN") or os.getenv("X_APP_TOKEN") or "").strip()

KSA_TEST_SYMBOL = os.getenv("KSA_TEST_SYMBOL", "1120.SR")   # Al Rajhi
GLOBAL_TEST_SYMBOL = os.getenv("GLOBAL_TEST_SYMBOL", "AAPL") # Apple

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))
SHEET_NAME = os.getenv("SHEET_NAME", "Test_Script")
VERIFY_TLS = os.getenv("VERIFY_TLS", "true").strip().lower() not in ("0", "false", "no")


# -----------------------------
# Pretty output
# -----------------------------
class Colors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"


def log(msg: str, typ: str = "info") -> None:
    if typ == "info":
        print(f"{Colors.OKBLUE}[INFO]{Colors.ENDC} {msg}")
    elif typ == "success":
        print(f"{Colors.OKGREEN}[PASS]{Colors.ENDC} {msg}")
    elif typ == "warn":
        print(f"{Colors.WARNING}[WARN]{Colors.ENDC} {msg}")
    elif typ == "fail":
        print(f"{Colors.FAIL}[FAIL]{Colors.ENDC} {msg}")
    elif typ == "header":
        print(f"\n{Colors.HEADER}--- {msg} ---{Colors.ENDC}")


# -----------------------------
# HTTP helpers
# -----------------------------
def _headers() -> Dict[str, str]:
    h = {
        "User-Agent": "TadawulFastBridge-EndpointTester/2.0",
        "Accept": "application/json",
    }
    if APP_TOKEN:
        h["X-APP-TOKEN"] = APP_TOKEN
    return h


def _safe_json(resp: requests.Response) -> Tuple[Optional[Dict[str, Any]], str]:
    try:
        return resp.json(), ""
    except Exception:
        return None, (resp.text or "")[:3000]


def _get(url: str, params: Optional[Dict[str, Any]] = None) -> Tuple[int, float, Optional[Dict[str, Any]], str]:
    start = time.time()
    try:
        r = requests.get(url, params=params, headers=_headers(), timeout=HTTP_TIMEOUT, verify=VERIFY_TLS)
        dt = time.time() - start
        data, raw = _safe_json(r)
        return r.status_code, dt, data, raw
    except Exception as e:
        dt = time.time() - start
        return 0, dt, None, str(e)


def _post(url: str, payload: Dict[str, Any]) -> Tuple[int, float, Optional[Dict[str, Any]], str]:
    start = time.time()
    try:
        r = requests.post(url, json=payload, headers=_headers(), timeout=HTTP_TIMEOUT, verify=VERIFY_TLS)
        dt = time.time() - start
        data, raw = _safe_json(r)
        return r.status_code, dt, data, raw
    except Exception as e:
        dt = time.time() - start
        return 0, dt, None, str(e)


# -----------------------------
# Response parsing utilities
# -----------------------------
def _pick_first(d: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in d and d.get(k) is not None:
            return d.get(k)
    return None


def _extract_symbol(d: Dict[str, Any]) -> Optional[str]:
    v = _pick_first(d, ["symbol", "ticker"])
    return str(v).strip() if v is not None else None


def _extract_price(d: Dict[str, Any]) -> Optional[float]:
    v = _pick_first(d, ["current_price", "last_price", "price", "close"])
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _extract_quality(d: Dict[str, Any]) -> Optional[str]:
    v = _pick_first(d, ["data_quality", "quality", "status"])
    return str(v).strip() if v is not None else None


def _extract_source(d: Dict[str, Any]) -> Optional[str]:
    v = _pick_first(d, ["data_source", "provider", "primary_provider"])
    return str(v).strip() if v is not None else None


def _pretty_short_json(d: Dict[str, Any]) -> str:
    try:
        return json.dumps(d, ensure_ascii=False, indent=2)[:2500]
    except Exception:
        return str(d)[:2500]


# -----------------------------
# Tests
# -----------------------------
def check_server() -> bool:
    log(f"Base URL: {BASE_URL}", "info")
    if APP_TOKEN:
        log("Auth: X-APP-TOKEN provided ✅", "info")
    else:
        log("Auth: No token provided (some deployments may reject requests) ⚠️", "warn")

    log("Checking server connectivity", "header")
    code, dt, data, raw = _get(f"{BASE_URL}/")
    if code == 200:
        log(f"GET / -> 200 in {dt:.2f}s", "success")
        if isinstance(data, dict):
            log(f"Service: {data.get('service') or data.get('app') or 'unknown'} | Version: {data.get('version') or 'unknown'}", "info")
        return True

    log(f"GET / failed (HTTP {code}) in {dt:.2f}s: {raw}", "fail")
    return False


def check_health_endpoints() -> None:
    log("Checking health endpoints", "header")
    endpoints = [
        "/health",
        "/v1/enriched/health",
        "/v1/analysis/health",
        "/v1/advanced/health",
        "/v1/argaam/health",
        "/v1/legacy/health",
    ]

    for ep in endpoints:
        code, dt, data, raw = _get(f"{BASE_URL}{ep}")
        if code == 200 and isinstance(data, dict):
            status = data.get("status", "ok")
            ver = data.get("version") or data.get("app_version")
            log(f"{ep.ljust(22)} -> {status} | {ver or ''} ({dt:.2f}s)", "success")
        else:
            msg = raw if raw else ("" if data is None else _pretty_short_json(data))
            log(f"{ep.ljust(22)} -> HTTP {code} ({dt:.2f}s) {msg}", "fail")


def _test_single_quote(endpoint: str, symbol: str, label: str) -> None:
    log(f"Single quote: {label} ({symbol})", "header")

    # Try common param names for maximum compatibility
    param_variants = [
        {"symbol": symbol},
        {"ticker": symbol},
        {"t": symbol},
    ]

    best: Optional[Tuple[int, float, Optional[Dict[str, Any]], str, Dict[str, Any]]] = None
    for params in param_variants:
        code, dt, data, raw = _get(f"{BASE_URL}{endpoint}", params=params)
        if best is None or (code == 200 and (best[0] != 200)):
            best = (code, dt, data, raw, params)
        if code == 200:
            break

    if not best:
        log("Unexpected: no attempts executed", "fail")
        return

    code, dt, data, raw, used_params = best
    if code != 200 or not isinstance(data, dict):
        msg = raw if raw else ("" if data is None else _pretty_short_json(data))
        log(f"HTTP {code} ({dt:.2f}s) using params={used_params}: {msg}", "fail")
        return

    resp_sym = _extract_symbol(data) or ""
    price = _extract_price(data)
    qual = _extract_quality(data) or "unknown"
    src = _extract_source(data) or "unknown"

    # Be tolerant with symbol normalization differences (AAPL vs AAPL.US etc.)
    sym_ok = resp_sym.upper().startswith(symbol.upper().split(".")[0])

    if price is not None and sym_ok:
        log(f"Valid response in {dt:.2f}s | Price={price} | Quality={qual} | Source={src}", "success")
    else:
        log(f"Response received but validation weak (sym='{resp_sym}', price='{price}')", "warn")
        print(_pretty_short_json(data))


def check_quotes() -> None:
    _test_single_quote("/v1/enriched/quote", KSA_TEST_SYMBOL, "Enriched (KSA)")
    _test_single_quote("/v1/analysis/quote", GLOBAL_TEST_SYMBOL, "AI Analysis (Global)")
    _test_single_quote("/v1/argaam/quote", KSA_TEST_SYMBOL, "Argaam Router (KSA)")
    _test_single_quote("/v1/quote", KSA_TEST_SYMBOL, "Legacy /v1/quote (KSA)")


def check_sheet_rows() -> None:
    log("Testing sheet-rows endpoints (batch)", "header")

    tickers = [KSA_TEST_SYMBOL, GLOBAL_TEST_SYMBOL]
    payload = {"tickers": tickers, "sheet_name": SHEET_NAME}

    endpoints = [
        ("/v1/enriched/sheet-rows", "Enriched sheet-rows"),
        ("/v1/analysis/sheet-rows", "AI sheet-rows"),
        ("/v1/advanced/sheet-rows", "Advanced sheet-rows"),
        ("/v1/argaam/sheet-rows", "Argaam sheet-rows"),
    ]

    for ep, label in endpoints:
        code, dt, data, raw = _post(f"{BASE_URL}{ep}", payload)

        if code != 200 or not isinstance(data, dict):
            msg = raw if raw else ("" if data is None else _pretty_short_json(data))
            log(f"{label.ljust(20)} -> HTTP {code} ({dt:.2f}s) {msg}", "fail")
            continue

        headers = data.get("headers") or []
        rows = data.get("rows") or []

        if isinstance(headers, list) and isinstance(rows, list):
            ok = len(rows) >= 1 and len(headers) >= 2
            if ok:
                log(f"{label.ljust(20)} -> OK ({dt:.2f}s) headers={len(headers)} rows={len(rows)}", "success")
            else:
                log(f"{label.ljust(20)} -> Weak payload ({dt:.2f}s) headers={len(headers)} rows={len(rows)}", "warn")
                print(_pretty_short_json(data))
        else:
            log(f"{label.ljust(20)} -> Unexpected schema ({dt:.2f}s)", "warn")
            print(_pretty_short_json(data))


def main() -> int:
    print(f"{Colors.HEADER}TADAWUL FAST BRIDGE - ENDPOINT TESTER (Enhanced){Colors.ENDC}")

    if not check_server():
        print("\n❌ Cannot proceed. Ensure the API is running and BASE_URL is correct.")
        print("   Example: set BASE_URL=http://127.0.0.1:8000")
        print("   Or for Render: set BASE_URL=https://tadawul-fast-bridge.onrender.com")
        return 1

    check_health_endpoints()
    check_quotes()
    check_sheet_rows()

    log("Done.", "header")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
