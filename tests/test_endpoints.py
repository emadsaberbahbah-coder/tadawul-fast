#!/usr/bin/env python3
# test_endpoints.py
"""
===========================================================
TFB Endpoint Smoke Tester — v2.2.0 (Live-Route Aligned)
===========================================================
CANONICAL-ROUTE SAFE • RENDER/LOCAL FRIENDLY • PARTIAL-TIMEOUT AWARE

What v2.2.0 revises
- FIX: Replaced older /v1/enriched/quote, /v1/enriched/quotes, /v1/argaam/* route
       assumptions with the canonical live routes currently used by TFB.
- FIX: Added /meta, /readyz, /livez, /openapi.json checks.
- FIX: Added GET/POST sheet-rows checks for current public owners:
       /v1/analysis/sheet-rows
       /v1/advanced/sheet-rows
       /v1/advisor/sheet-rows
       /sheet-rows
       /v1/schema/sheet-spec
- FIX: Treats Insights_Analysis timeout-partial as WARN when the schema contract
       is present, instead of incorrectly failing the route.
- FIX: Supports X-APP-TOKEN, X-API-Key, and Bearer auth headers together.
- FIX: Safe JSON parsing and route-aware payload validation.

Usage
  python test_endpoints.py
  TFB_BASE_URL=https://tadawul-fast-bridge.onrender.com python test_endpoints.py
  TFB_APP_TOKEN=... python test_endpoints.py
"""

from __future__ import annotations

import os
import sys
import time
import uuid
from typing import Any, Dict, Optional, Tuple

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
    os.getenv("TFB_TOKEN")
    or os.getenv("TFB_APP_TOKEN")
    or os.getenv("APP_TOKEN")
    or os.getenv("BACKUP_APP_TOKEN")
    or ""
).strip()

KSA_TEST_SYMBOL = os.getenv("TFB_KSA_TEST_SYMBOL", "1120.SR").strip().upper()
GLOBAL_TEST_SYMBOL = os.getenv("TFB_GLOBAL_TEST_SYMBOL", "AAPL").strip().upper()
TOP10_SYMBOL = os.getenv("TFB_TOP10_TEST_SYMBOL", "NVDA").strip().upper()
COMMODITY_SYMBOL = os.getenv("TFB_COMMODITY_TEST_SYMBOL", "GC=F").strip().upper()

TIMEOUT_SHORT = float(os.getenv("TFB_TIMEOUT_SHORT", "15") or "15")
TIMEOUT_MED = float(os.getenv("TFB_TIMEOUT_MED", "30") or "30")
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
    prefix = {
        "info": f"{Colors.OKBLUE}[INFO]{Colors.ENDC}",
        "success": f"{Colors.OKGREEN}[PASS]{Colors.ENDC}",
        "warn": f"{Colors.WARNING}[WARN]{Colors.ENDC}",
        "fail": f"{Colors.FAIL}[FAIL]{Colors.ENDC}",
        "header": f"\n{Colors.HEADER}---",
    }.get(typ, "")

    if typ == "header":
        sys.stdout.write(f"\n{Colors.HEADER}--- {msg} ---{Colors.ENDC}\n")
    else:
        sys.stdout.write(f"{prefix} {msg}\n")


def auth_headers() -> Dict[str, str]:
    hdrs: Dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": "TFB-Endpoint-Smoke/2.2.0",
        "X-Request-ID": str(uuid.uuid4())[:8],
    }
    if TOKEN:
        hdrs["X-APP-TOKEN"] = TOKEN
        hdrs["X-API-Key"] = TOKEN
        hdrs["Authorization"] = f"Bearer {TOKEN}"
    return hdrs


def safe_json(resp: requests.Response) -> Optional[Any]:
    try:
        return resp.json()
    except Exception:
        return None


def fmt_dt(dt: float) -> str:
    return f"{dt:.2f}s"


def count_safe(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (str, bytes, bytearray)):
        return 1 if value else 0
    try:
        return len(value)  # type: ignore[arg-type]
    except Exception:
        return 1


def unwrap_payload(data: Any) -> Any:
    if not isinstance(data, dict):
        return data
    for key in ("data", "result", "payload"):
        value = data.get(key)
        if isinstance(value, dict):
            if any(k in value for k in ("headers", "keys", "rows", "rows_matrix", "pages")):
                return value
    return data


def warnings_text(data: Any) -> str:
    if not isinstance(data, dict):
        return ""
    warnings = []
    meta = data.get("meta")
    if isinstance(meta, dict) and isinstance(meta.get("warnings"), list):
        warnings.extend(str(x) for x in meta.get("warnings") if str(x).strip())
    if isinstance(data.get("warnings"), list):
        warnings.extend(str(x) for x in data.get("warnings") if str(x).strip())
    return " | ".join(warnings)


def sheet_rows_summary(data: Dict[str, Any]) -> str:
    headers_n = count_safe(data.get("headers"))
    keys_n = count_safe(data.get("keys"))
    rows_n = max(
        count_safe(data.get("rows")),
        count_safe(data.get("row_objects")),
        count_safe(data.get("data")),
        count_safe(data.get("items")),
        count_safe(data.get("quotes")),
    )
    status = str(data.get("status") or "")
    return f"status={status} headers={headers_n} keys={keys_n} rows={rows_n}"


def request_json(method: str, path: str, *, params: Optional[Dict[str, Any]] = None, payload: Optional[Dict[str, Any]] = None, timeout: float = TIMEOUT_MED) -> Tuple[int, Optional[Any], float, str]:
    url = f"{BASE_URL}{path}"
    t0 = time.time()
    resp = requests.request(
        method=method,
        url=url,
        params=params,
        json=payload,
        headers=auth_headers(),
        timeout=timeout,
    )
    dt = time.time() - t0
    return resp.status_code, safe_json(resp), dt, resp.text[:400]


# ---------------------------------------------------------------------
# Core checks
# ---------------------------------------------------------------------
def check_server() -> bool:
    log("Checking Server Connectivity", "header")

    try:
        t0 = time.time()
        r = requests.get(f"{BASE_URL}/", headers=auth_headers(), timeout=TIMEOUT_SHORT)
        dt = time.time() - t0
        if r.status_code == 200:
            log(f"GET / -> Server UP ({fmt_dt(dt)})", "success")
        else:
            log(f"GET / -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={r.text[:200]!r}", "fail")
            return False
    except Exception as exc:
        log(f"GET / -> Cannot connect: {exc}", "fail")
        return False

    try:
        t0 = time.time()
        rh = requests.head(f"{BASE_URL}/", headers=auth_headers(), timeout=TIMEOUT_SHORT)
        dt = time.time() - t0
        if rh.status_code in (200, 204):
            log(f"HEAD / -> OK ({fmt_dt(dt)})", "success")
        else:
            log(f"HEAD / -> HTTP {rh.status_code} ({fmt_dt(dt)})", "warn")
    except Exception as exc:
        log(f"HEAD / -> Exception: {exc}", "warn")

    return True


def check_meta_and_health() -> None:
    log("Checking Meta / Health Routes", "header")
    endpoints = ["/health", "/meta", "/readyz", "/livez", "/openapi.json"]

    for ep in endpoints:
        try:
            t0 = time.time()
            r = requests.get(f"{BASE_URL}{ep}", headers=auth_headers(), timeout=TIMEOUT_MED)
            dt = time.time() - t0
            data = safe_json(r)
            if r.status_code != 200:
                log(f"{ep.ljust(18)} -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={r.text[:220]!r}", "fail")
                continue

            if ep == "/openapi.json":
                if isinstance(data, dict) and isinstance(data.get("paths"), dict):
                    log(f"{ep.ljust(18)} -> paths={len(data.get('paths') or {})} ({fmt_dt(dt)})", "success")
                else:
                    log(f"{ep.ljust(18)} -> 200 but invalid OpenAPI body ({fmt_dt(dt)})", "warn")
                continue

            status = (data or {}).get("status", "ok") if isinstance(data, dict) else "ok"
            ver = (data or {}).get("entry_version") or (data or {}).get("app_version") if isinstance(data, dict) else ""
            eng = (data or {}).get("engine_source") if isinstance(data, dict) else ""
            msg = f"{ep.ljust(18)} -> {status} ({fmt_dt(dt)})"
            if ver:
                msg += f" | v={ver}"
            if eng:
                msg += f" | engine={eng}"
            log(msg, "success")
        except Exception as exc:
            log(f"{ep.ljust(18)} -> Exception: {exc}", "fail")


def check_analysis_market_leaders_get() -> None:
    log("Testing GET /v1/analysis/sheet-rows -> Market_Leaders", "header")
    status, data, dt, raw = request_json(
        "GET",
        "/v1/analysis/sheet-rows",
        params={"page": "Market_Leaders", "limit": 5, "symbols": f"{KSA_TEST_SYMBOL},{GLOBAL_TEST_SYMBOL}"},
        timeout=TIMEOUT_LONG,
    )
    data = unwrap_payload(data)

    if status != 200:
        log(f"HTTP {status} ({fmt_dt(dt)}) | body={raw!r}", "fail")
        return
    if not isinstance(data, dict):
        log(f"Non-JSON or unexpected payload ({fmt_dt(dt)})", "fail")
        return

    headers_n = count_safe(data.get("headers"))
    keys_n = count_safe(data.get("keys"))
    rows_n = max(count_safe(data.get("rows")), count_safe(data.get("row_objects")), count_safe(data.get("data")), count_safe(data.get("quotes")))

    if headers_n >= 70 and keys_n >= 70 and rows_n > 0:
        log(f"OK ({fmt_dt(dt)}) | {sheet_rows_summary(data)}", "success")
    elif headers_n >= 70 and keys_n >= 70:
        log(f"Contract OK but rows empty ({fmt_dt(dt)}) | {sheet_rows_summary(data)}", "warn")
    else:
        log(f"Weak contract ({fmt_dt(dt)}) | {sheet_rows_summary(data)}", "fail")


def check_analysis_global_markets_post() -> None:
    log("Testing POST /v1/analysis/sheet-rows -> Global_Markets", "header")
    status, data, dt, raw = request_json(
        "POST",
        "/v1/analysis/sheet-rows",
        payload={
            "page": "Global_Markets",
            "limit": 5,
            "symbols": [GLOBAL_TEST_SYMBOL, "MSFT", TOP10_SYMBOL],
            "tickers": [GLOBAL_TEST_SYMBOL, "MSFT", TOP10_SYMBOL],
            "direct_symbols": [GLOBAL_TEST_SYMBOL, "MSFT", TOP10_SYMBOL],
            "mode": "live",
        },
        timeout=TIMEOUT_LONG,
    )
    data = unwrap_payload(data)

    if status != 200:
        log(f"HTTP {status} ({fmt_dt(dt)}) | body={raw!r}", "fail")
        return
    if not isinstance(data, dict):
        log(f"Non-JSON or unexpected payload ({fmt_dt(dt)})", "fail")
        return

    headers_n = count_safe(data.get("headers"))
    keys_n = count_safe(data.get("keys"))
    rows_n = max(count_safe(data.get("rows")), count_safe(data.get("row_objects")), count_safe(data.get("data")), count_safe(data.get("quotes")))

    if headers_n >= 70 and keys_n >= 70 and rows_n > 0:
        log(f"OK ({fmt_dt(dt)}) | {sheet_rows_summary(data)}", "success")
    elif headers_n >= 70 and keys_n >= 70:
        log(f"Contract OK but rows empty ({fmt_dt(dt)}) | {sheet_rows_summary(data)}", "warn")
    else:
        log(f"Weak contract ({fmt_dt(dt)}) | {sheet_rows_summary(data)}", "fail")


def check_advanced_insights_post() -> None:
    log("Testing POST /v1/advanced/sheet-rows -> Insights_Analysis", "header")
    status, data, dt, raw = request_json(
        "POST",
        "/v1/advanced/sheet-rows",
        payload={
            "page": "Insights_Analysis",
            "limit": 5,
            "symbols": [KSA_TEST_SYMBOL, GLOBAL_TEST_SYMBOL, COMMODITY_SYMBOL],
            "mode": "live",
        },
        timeout=TIMEOUT_LONG,
    )
    data = unwrap_payload(data)

    if status != 200:
        log(f"HTTP {status} ({fmt_dt(dt)}) | body={raw!r}", "fail")
        return
    if not isinstance(data, dict):
        log(f"Non-JSON or unexpected payload ({fmt_dt(dt)})", "fail")
        return

    headers_n = count_safe(data.get("headers"))
    keys_n = count_safe(data.get("keys"))
    rows_n = max(count_safe(data.get("rows")), count_safe(data.get("row_objects")), count_safe(data.get("data")))
    warn_txt = warnings_text(data)

    if headers_n >= 7 and keys_n >= 7 and rows_n > 0:
        log(f"OK ({fmt_dt(dt)}) | {sheet_rows_summary(data)}", "success")
    elif headers_n >= 7 and keys_n >= 7 and "timeout" in warn_txt.lower():
        log(f"Partial timeout but contract OK ({fmt_dt(dt)}) | warnings={warn_txt}", "warn")
    elif headers_n >= 7 and keys_n >= 7:
        log(f"Contract OK but empty rows ({fmt_dt(dt)}) | {sheet_rows_summary(data)}", "warn")
    else:
        log(f"Weak contract ({fmt_dt(dt)}) | {sheet_rows_summary(data)}", "fail")


def check_advisor_portfolio_post() -> None:
    log("Testing POST /v1/advisor/sheet-rows -> My_Portfolio", "header")
    status, data, dt, raw = request_json(
        "POST",
        "/v1/advisor/sheet-rows",
        payload={
            "page": "My_Portfolio",
            "limit": 5,
            "symbols": [KSA_TEST_SYMBOL, GLOBAL_TEST_SYMBOL],
            "mode": "live",
        },
        timeout=TIMEOUT_LONG,
    )
    data = unwrap_payload(data)

    if status != 200:
        log(f"HTTP {status} ({fmt_dt(dt)}) | body={raw!r}", "fail")
        return
    if not isinstance(data, dict):
        log(f"Non-JSON or unexpected payload ({fmt_dt(dt)})", "fail")
        return

    headers_n = count_safe(data.get("headers"))
    keys_n = count_safe(data.get("keys"))
    rows_n = max(count_safe(data.get("rows")), count_safe(data.get("row_objects")), count_safe(data.get("data")))

    if headers_n >= 70 and keys_n >= 70 and rows_n > 0:
        log(f"OK ({fmt_dt(dt)}) | {sheet_rows_summary(data)}", "success")
    elif headers_n >= 70 and keys_n >= 70:
        log(f"Contract OK but rows empty ({fmt_dt(dt)}) | {sheet_rows_summary(data)}", "warn")
    else:
        log(f"Weak contract ({fmt_dt(dt)}) | {sheet_rows_summary(data)}", "fail")


def check_root_sheet_rows_top10() -> None:
    log("Testing POST /sheet-rows -> Top_10_Investments", "header")
    status, data, dt, raw = request_json(
        "POST",
        "/sheet-rows",
        payload={
            "page": "Top_10_Investments",
            "limit": 5,
            "symbols": [KSA_TEST_SYMBOL, GLOBAL_TEST_SYMBOL, TOP10_SYMBOL],
            "mode": "live",
        },
        timeout=TIMEOUT_LONG,
    )
    data = unwrap_payload(data)

    if status != 200:
        log(f"HTTP {status} ({fmt_dt(dt)}) | body={raw!r}", "fail")
        return
    if not isinstance(data, dict):
        log(f"Non-JSON or unexpected payload ({fmt_dt(dt)})", "fail")
        return

    headers_n = count_safe(data.get("headers"))
    keys_n = count_safe(data.get("keys"))
    rows_n = max(count_safe(data.get("rows")), count_safe(data.get("row_objects")), count_safe(data.get("data")), count_safe(data.get("items")))

    if headers_n >= 80 and keys_n >= 80 and rows_n > 0:
        log(f"OK ({fmt_dt(dt)}) | {sheet_rows_summary(data)}", "success")
    elif headers_n >= 80 and keys_n >= 80:
        log(f"Contract OK but rows empty ({fmt_dt(dt)}) | {sheet_rows_summary(data)}", "warn")
    else:
        log(f"Weak contract ({fmt_dt(dt)}) | {sheet_rows_summary(data)}", "fail")


def check_schema_routes() -> None:
    log("Testing Schema Routes", "header")

    # /v1/schema/pages
    status, data, dt, raw = request_json("GET", "/v1/schema/pages", timeout=TIMEOUT_MED)
    payload = unwrap_payload(data)
    if status == 200:
        if isinstance(payload, dict) and isinstance(payload.get("pages"), list):
            log(f"/v1/schema/pages -> pages={len(payload.get('pages') or [])} ({fmt_dt(dt)})", "success")
        elif isinstance(payload, list):
            log(f"/v1/schema/pages -> pages={len(payload)} ({fmt_dt(dt)})", "success")
        else:
            log(f"/v1/schema/pages -> 200 but unexpected payload ({fmt_dt(dt)})", "warn")
    else:
        log(f"/v1/schema/pages -> HTTP {status} ({fmt_dt(dt)}) | body={raw!r}", "fail")

    # /v1/schema/sheet-spec?page=Data_Dictionary
    status, data, dt, raw = request_json(
        "GET",
        "/v1/schema/sheet-spec",
        params={"page": "Data_Dictionary"},
        timeout=TIMEOUT_MED,
    )
    payload = unwrap_payload(data)
    if status == 200 and isinstance(payload, dict):
        headers_n = count_safe(payload.get("headers"))
        keys_n = count_safe(payload.get("keys"))
        if max(headers_n, keys_n) >= 7:
            log(f"/v1/schema/sheet-spec -> headers={headers_n} keys={keys_n} ({fmt_dt(dt)})", "success")
        else:
            log(f"/v1/schema/sheet-spec -> weak contract ({fmt_dt(dt)})", "warn")
    else:
        log(f"/v1/schema/sheet-spec -> HTTP {status} ({fmt_dt(dt)}) | body={raw!r}", "fail")

    # /v1/schema/data-dictionary and alias fallback
    for path in ("/v1/schema/data-dictionary", "/v1/schema/data_dictionary"):
        status, data, dt, raw = request_json("GET", path, timeout=TIMEOUT_MED)
        payload = unwrap_payload(data)
        if status == 404:
            log(f"{path} -> SKIP (404 not mounted)", "warn")
            continue
        if status == 200 and isinstance(payload, dict):
            headers_n = count_safe(payload.get("headers"))
            rows_n = max(count_safe(payload.get("rows")), count_safe(payload.get("data")), count_safe(payload.get("items")))
            if headers_n == 9 and rows_n > 0:
                log(f"{path} -> 9-column contract rows={rows_n} ({fmt_dt(dt)})", "success")
            elif headers_n == 9:
                log(f"{path} -> 9-column contract but empty rows ({fmt_dt(dt)})", "warn")
            else:
                log(f"{path} -> unexpected contract headers={headers_n} ({fmt_dt(dt)})", "warn")
        else:
            log(f"{path} -> HTTP {status} ({fmt_dt(dt)}) | body={raw!r}", "fail")
        break


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
if __name__ == "__main__":
    sys.stdout.write(f"{Colors.HEADER}TADAWUL FAST BRIDGE - ENDPOINT TESTER v2.2.0{Colors.ENDC}\n")
    sys.stdout.write(f"BASE_URL={BASE_URL} | TOKEN={'SET' if TOKEN else 'NOT SET'}\n")
    sys.stdout.write(f"KSA_TEST_SYMBOL={KSA_TEST_SYMBOL} | GLOBAL_TEST_SYMBOL={GLOBAL_TEST_SYMBOL} | TOP10_SYMBOL={TOP10_SYMBOL}\n")

    if check_server():
        check_meta_and_health()
        check_analysis_market_leaders_get()
        check_analysis_global_markets_post()
        check_advanced_insights_post()
        check_advisor_portfolio_post()
        check_root_sheet_rows_top10()
        check_schema_routes()
    else:
        sys.stdout.write("\nCannot proceed. Run your API first (or set TFB_BASE_URL).\n")
