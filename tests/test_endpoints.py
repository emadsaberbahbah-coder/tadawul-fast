#!/usr/bin/env python3
# test_endpoints.py
"""
test_endpoints.py
===========================================================
SMOKE TESTER for Tadawul Fast Bridge API – v2.1.0 (Hardened)

Enhancements vs your version
- Supports token via env: TFB_APP_TOKEN or APP_TOKEN or BACKUP_APP_TOKEN
- Supports base url via env: TFB_BASE_URL or BACKEND_BASE_URL
- Render-friendly: tests GET /, HEAD /, GET /health
- Adds richer diagnostics:
    • prints server "module/version" fields when present
    • shows response time per endpoint
    • tolerates missing routers (soft-fail)
- Tests BOTH:
    • /v1/enriched/quote (single)
    • /v1/enriched/quotes (batch, new router)
    • /v1/enriched/sheet-rows (Sheets path – if your app exposes it)
    • /v1/argaam/sheet-rows (KSA rows)
- Safe JSON parsing: never crashes on non-JSON responses

Usage
  python test_endpoints.py
  TFB_BASE_URL=https://your-render.onrender.com python test_endpoints.py
  TFB_APP_TOKEN=... python test_endpoints.py

Notes
- If your app doesn't expose /v1/enriched/sheet-rows, the test will SKIP it.
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

import requests

# ---------------------------------------------------------------------
# ENV
# ---------------------------------------------------------------------

BASE_URL = (
    os.getenv("TFB_BASE_URL")
    or os.getenv("BACKEND_BASE_URL")
    or "http://127.0.0.1:8000"
).rstrip("/")

TOKEN = (
    os.getenv("TFB_APP_TOKEN")
    or os.getenv("APP_TOKEN")
    or os.getenv("BACKUP_APP_TOKEN")
    or ""
).strip()

KSA_TEST_SYMBOL = os.getenv("TFB_KSA_TEST_SYMBOL", "1120.SR").strip().upper()
GLOBAL_TEST_SYMBOL = os.getenv("TFB_GLOBAL_TEST_SYMBOL", "AAPL").strip().upper()

TIMEOUT_SHORT = float(os.getenv("TFB_TIMEOUT_SHORT", "15") or "15")
TIMEOUT_MED = float(os.getenv("TFB_TIMEOUT_MED", "25") or "25")
TIMEOUT_LONG = float(os.getenv("TFB_TIMEOUT_LONG", "90") or "90")


# ---------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------

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
    else:
        print(msg)


def headers() -> Dict[str, str]:
    h: Dict[str, str] = {"Accept": "application/json"}
    if TOKEN:
        h["X-APP-TOKEN"] = TOKEN
    return h


def safe_json(resp: requests.Response) -> Optional[Any]:
    try:
        return resp.json()
    except Exception:
        return None


def fmt_dt(dt: float) -> str:
    return f"{dt:.2f}s"


# ---------------------------------------------------------------------
# Core checks
# ---------------------------------------------------------------------

def check_server() -> bool:
    log("Checking Server Connectivity...", "header")

    # GET /
    try:
        t0 = time.time()
        r = requests.get(f"{BASE_URL}/", headers=headers(), timeout=TIMEOUT_SHORT)
        dt = time.time() - t0
        if r.status_code == 200:
            log(f"GET / -> Server UP ({fmt_dt(dt)})", "success")
        else:
            log(f"GET / -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={r.text[:200]!r}", "fail")
            return False
    except Exception as e:
        log(f"GET / -> Cannot connect: {e}", "fail")
        return False

    # HEAD /
    try:
        t0 = time.time()
        rh = requests.head(f"{BASE_URL}/", headers=headers(), timeout=TIMEOUT_SHORT)
        dt = time.time() - t0
        if rh.status_code in (200, 204):
            log(f"HEAD / -> OK ({fmt_dt(dt)})", "success")
        else:
            log(f"HEAD / -> HTTP {rh.status_code} ({fmt_dt(dt)})", "warn")
    except Exception as e:
        log(f"HEAD / -> Exception: {e}", "warn")

    # GET /health
    try:
        t0 = time.time()
        r2 = requests.get(f"{BASE_URL}/health", headers=headers(), timeout=TIMEOUT_SHORT)
        dt = time.time() - t0
        if r2.status_code == 200:
            j = safe_json(r2) or {}
            status = (j.get("status") or "unknown")
            mod = j.get("module") or j.get("app") or ""
            ver = j.get("version") or j.get("app_version") or ""
            extra = f" | {mod} {ver}".strip()
            log(f"GET /health -> {status} ({fmt_dt(dt)}){(' ' + extra) if extra else ''}", "success")
        else:
            log(f"GET /health -> HTTP {r2.status_code} ({fmt_dt(dt)})", "warn")
    except Exception as e:
        log(f"GET /health -> Exception: {e}", "warn")

    return True


def check_health_routers() -> None:
    log("Checking Router Health", "header")
    endpoints = [
        "/health",
        "/v1/enriched/health",
        "/v1/analysis/health",
        "/v1/advanced/health",
        "/v1/argaam/health",
    ]

    for ep in endpoints:
        try:
            t0 = time.time()
            r = requests.get(f"{BASE_URL}{ep}", headers=headers(), timeout=TIMEOUT_MED)
            dt = time.time() - t0

            if r.status_code == 200:
                j = safe_json(r) or {}
                status = j.get("status", "ok")
                ver = j.get("version") or j.get("route_version") or ""
                engine = j.get("engine_available")
                msg = f"{ep.ljust(24)} -> {status} ({fmt_dt(dt)})"
                if ver:
                    msg += f" | v={ver}"
                if engine is not None:
                    msg += f" | engine_available={engine}"
                log(msg, "success")
            elif r.status_code == 404:
                log(f"{ep.ljust(24)} -> SKIP (404 not mounted)", "warn")
            else:
                log(f"{ep.ljust(24)} -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={r.text[:200]!r}", "fail")
        except Exception as e:
            log(f"{ep.ljust(24)} -> Exception: {e}", "fail")


def check_quote(symbol: str, endpoint: str = "/v1/enriched/quote") -> None:
    log(f"Testing Single Quote ({symbol})", "header")
    try:
        t0 = time.time()
        url = f"{BASE_URL}{endpoint}"
        r = requests.get(url, params={"symbol": symbol}, headers=headers(), timeout=TIMEOUT_MED)
        dt = time.time() - t0

        if r.status_code != 200:
            log(f"{endpoint} -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={r.text[:240]!r}", "fail")
            return

        data = safe_json(r)
        if not isinstance(data, dict):
            log(f"{endpoint} -> Non-JSON response ({fmt_dt(dt)})", "fail")
            print(r.text[:500])
            return

        sym_resp = (data.get("symbol") or data.get("ticker") or "").upper()
        price = data.get("last_price") or data.get("price") or data.get("current_price")

        if sym_resp and sym_resp == symbol.upper() and price is not None:
            dq = data.get("data_quality")
            src = data.get("data_source") or data.get("source")
            log(f"OK ({fmt_dt(dt)}) | symbol={sym_resp} price={price} dq={dq} src={src}", "success")
        else:
            log(f"Returned 200 but missing fields ({fmt_dt(dt)}). Inspect payload below:", "warn")
            print(json_pretty(data))
    except Exception as e:
        log(f"Request failed: {e}", "fail")


def json_pretty(obj: Any) -> str:
    try:
        import json
        return json.dumps(obj, indent=2, ensure_ascii=False)[:2500]
    except Exception:
        return str(obj)[:2500]


def check_batch_quotes_router() -> None:
    """
    Tests POST /v1/enriched/quotes (the router you shared).
    """
    log("Testing Batch /v1/enriched/quotes", "header")
    payload = {"symbols": [KSA_TEST_SYMBOL, GLOBAL_TEST_SYMBOL], "sheet_name": "Test_Script"}
    try:
        t0 = time.time()
        r = requests.post(
            f"{BASE_URL}/v1/enriched/quotes",
            json=payload,
            headers=headers(),
            timeout=TIMEOUT_LONG,
        )
        dt = time.time() - t0

        if r.status_code == 404:
            log("/v1/enriched/quotes -> SKIP (404 not mounted)", "warn")
            return

        if r.status_code != 200:
            log(f"/v1/enriched/quotes -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={r.text[:240]!r}", "fail")
            return

        data = safe_json(r) or {}
        status = data.get("status")
        count = data.get("count")
        rows = data.get("rows") or []
        quotes = data.get("quotes") or []
        log(f"/v1/enriched/quotes -> {status} ({fmt_dt(dt)}) | count={count} rows={len(rows)} quotes={len(quotes)}", "success")
    except Exception as e:
        log(f"Batch exception: {e}", "fail")


def check_sheet_rows_enriched() -> None:
    """
    Some projects expose /v1/enriched/sheet-rows; your google_sheets_service calls it.
    If not present, we SKIP.
    """
    log("Testing Batch /v1/enriched/sheet-rows (Sheets payload)", "header")
    payload = {"tickers": [KSA_TEST_SYMBOL, GLOBAL_TEST_SYMBOL], "sheet_name": "Test_Script"}
    try:
        t0 = time.time()
        r = requests.post(f"{BASE_URL}/v1/enriched/sheet-rows", json=payload, headers=headers(), timeout=TIMEOUT_LONG)
        dt = time.time() - t0

        if r.status_code == 404:
            log("/v1/enriched/sheet-rows -> SKIP (404 not mounted)", "warn")
            return

        if r.status_code != 200:
            log(f"/v1/enriched/sheet-rows -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={r.text[:240]!r}", "fail")
            return

        data = safe_json(r)
        if not isinstance(data, dict):
            log(f"/v1/enriched/sheet-rows -> Non-JSON response ({fmt_dt(dt)})", "fail")
            return

        rows = data.get("rows", []) or []
        headers_ = data.get("headers", []) or []
        if len(rows) >= 2 and len(headers_) >= 2:
            log(f"Batch OK ({fmt_dt(dt)}) | rows={len(rows)} headers={len(headers_)}", "success")
        else:
            log(f"Batch returned unexpected shape ({fmt_dt(dt)}). Payload below:", "warn")
            print(json_pretty(data))
    except Exception as e:
        log(f"Batch exception: {e}", "fail")


def check_argaam_sheet_rows() -> None:
    log("Testing KSA /v1/argaam/sheet-rows", "header")
    payload = {"symbols": [KSA_TEST_SYMBOL, "BADSYM"], "sheet_name": "KSA_Tadawul_Market"}
    try:
        t0 = time.time()
        r = requests.post(f"{BASE_URL}/v1/argaam/sheet-rows", json=payload, headers=headers(), timeout=TIMEOUT_LONG)
        dt = time.time() - t0

        if r.status_code == 404:
            log("/v1/argaam/sheet-rows -> SKIP (404 not mounted)", "warn")
            return

        if r.status_code != 200:
            log(f"/v1/argaam/sheet-rows -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={r.text[:240]!r}", "fail")
            return

        data = safe_json(r)
        if isinstance(data, dict) and isinstance(data.get("rows"), list) and isinstance(data.get("headers"), list):
            log(f"OK ({fmt_dt(dt)}) | rows={len(data.get('rows') or [])} headers={len(data.get('headers') or [])}", "success")
        else:
            log(f"Unexpected response shape ({fmt_dt(dt)}). Payload below:", "warn")
            print(json_pretty(data))
    except Exception as e:
        log(f"Argaam sheet-rows exception: {e}", "fail")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

if __name__ == "__main__":
    print(f"{Colors.HEADER}TADAWUL FAST BRIDGE - ENDPOINT TESTER{Colors.ENDC}")
    print(f"BASE_URL={BASE_URL} | TOKEN={'SET' if TOKEN else 'NOT SET'}")
    print(f"KSA_TEST_SYMBOL={KSA_TEST_SYMBOL} | GLOBAL_TEST_SYMBOL={GLOBAL_TEST_SYMBOL}")

    if check_server():
        check_health_routers()
        check_quote(KSA_TEST_SYMBOL, "/v1/enriched/quote")
        check_quote(GLOBAL_TEST_SYMBOL, "/v1/enriched/quote")
        check_batch_quotes_router()
        check_sheet_rows_enriched()
        check_argaam_sheet_rows()
    else:
        print("\n❌ Cannot proceed. Run your API first (or set TFB_BASE_URL).")
