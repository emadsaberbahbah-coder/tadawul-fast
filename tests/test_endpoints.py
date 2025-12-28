#!/usr/bin/env python3
# test_endpoints.py  (FULL REPLACEMENT)
"""
test_endpoints.py
===========================================================
SMOKE TESTER for Tadawul Fast Bridge API – v2.4.0 (Hardened+)

✅ Highlights
- Token via env: TFB_APP_TOKEN / APP_TOKEN / BACKUP_APP_TOKEN
- Base URL via env: TFB_BASE_URL / BACKEND_BASE_URL (default http://127.0.0.1:8000)
- Render-friendly: tests /healthz + /readyz + /health (soft)
- Rich diagnostics: per-endpoint latency + safe JSON parsing + short body previews
- Soft-fail on 404 (router not mounted) — still reports clearly
- Sends BOTH symbols + tickers AND sheet_name + sheetName to sheet-rows endpoints
- Better auth headers: X-APP-TOKEN + Authorization: Bearer
- Optional strict mode: --strict (treat 404 as failures)
- Exits non-zero if critical connectivity fails (GET /healthz or GET /)

Usage:
  python test_endpoints.py
  TFB_BASE_URL=https://your.onrender.com python test_endpoints.py
  TFB_APP_TOKEN=... python test_endpoints.py
  python test_endpoints.py --strict
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

import requests


# -----------------------------------------------------------------------------
# ENV
# -----------------------------------------------------------------------------
DEFAULT_BASE_URL = (
    os.getenv("TFB_BASE_URL")
    or os.getenv("BACKEND_BASE_URL")
    or "http://127.0.0.1:8000"
).rstrip("/")

DEFAULT_TOKEN = (
    os.getenv("TFB_APP_TOKEN")
    or os.getenv("APP_TOKEN")
    or os.getenv("BACKUP_APP_TOKEN")
    or ""
).strip()

KSA_TEST_SYMBOL = (os.getenv("TFB_KSA_TEST_SYMBOL", "1120.SR") or "1120.SR").strip().upper()
GLOBAL_TEST_SYMBOL = (os.getenv("TFB_GLOBAL_TEST_SYMBOL", "AAPL") or "AAPL").strip().upper()

TIMEOUT_SHORT = float(os.getenv("TFB_TIMEOUT_SHORT", "15") or "15")
TIMEOUT_MED = float(os.getenv("TFB_TIMEOUT_MED", "25") or "25")
TIMEOUT_LONG = float(os.getenv("TFB_TIMEOUT_LONG", "90") or "90")

USER_AGENT = os.getenv("TFB_USER_AGENT", "TadawulFastBridge-EndpointTester/2.4.0") or "TadawulFastBridge-EndpointTester/2.4.0"


# -----------------------------------------------------------------------------
# Colors (auto-disable if NO_COLOR or not a TTY)
# -----------------------------------------------------------------------------
def _colors_enabled() -> bool:
    if os.getenv("NO_COLOR"):
        return False
    try:
        return sys.stdout.isatty()
    except Exception:
        return False


_USE_COLOR = _colors_enabled()


class C:
    HEADER = "\033[95m" if _USE_COLOR else ""
    OKBLUE = "\033[94m" if _USE_COLOR else ""
    OKGREEN = "\033[92m" if _USE_COLOR else ""
    WARNING = "\033[93m" if _USE_COLOR else ""
    FAIL = "\033[91m" if _USE_COLOR else ""
    ENDC = "\033[0m" if _USE_COLOR else ""


def log(msg: str, typ: str = "info") -> None:
    if typ == "info":
        print(f"{C.OKBLUE}[INFO]{C.ENDC} {msg}")
    elif typ == "success":
        print(f"{C.OKGREEN}[PASS]{C.ENDC} {msg}")
    elif typ == "warn":
        print(f"{C.WARNING}[WARN]{C.ENDC} {msg}")
    elif typ == "fail":
        print(f"{C.FAIL}[FAIL]{C.ENDC} {msg}")
    elif typ == "header":
        print(f"\n{C.HEADER}--- {msg} ---{C.ENDC}")
    else:
        print(msg)


def fmt_dt(dt: float) -> str:
    return f"{dt:.2f}s"


def safe_json(resp: requests.Response) -> Optional[Any]:
    try:
        return resp.json()
    except Exception:
        return None


def json_pretty(obj: Any, limit: int = 2500) -> str:
    try:
        return json.dumps(obj, indent=2, ensure_ascii=False)[:limit]
    except Exception:
        return str(obj)[:limit]


def body_preview(resp: requests.Response, limit: int = 240) -> str:
    try:
        t = (resp.text or "").replace("\r", " ").replace("\n", " ")
        return t[:limit]
    except Exception:
        return ""


def build_headers(token: str) -> Dict[str, str]:
    h: Dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }
    if token:
        h["X-APP-TOKEN"] = token
        h["Authorization"] = f"Bearer {token}"
    return h


@dataclass
class Result:
    endpoint: str
    ok: bool
    status_code: Optional[int]
    dt: float
    note: str = ""


def request_any(
    sess: requests.Session,
    method: str,
    url: str,
    *,
    headers: Dict[str, str],
    timeout: float,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[requests.Response], float, Optional[str]]:
    t0 = time.time()
    try:
        r = sess.request(method, url, headers=headers, timeout=timeout, params=params, json=json_body)
        return r, time.time() - t0, None
    except Exception as e:
        return None, time.time() - t0, str(e)


# -----------------------------------------------------------------------------
# Checks
# -----------------------------------------------------------------------------
def check_server(sess: requests.Session, base_url: str, headers: Dict[str, str]) -> Tuple[bool, List[Result]]:
    log("Checking Server Connectivity...", "header")
    results: List[Result] = []

    # GET /
    r, dt, err = request_any(sess, "GET", f"{base_url}/", headers=headers, timeout=TIMEOUT_SHORT)
    if err or r is None:
        results.append(Result("/", False, None, dt, f"Cannot connect: {err}"))
        log(f"GET / -> Cannot connect: {err}", "fail")
        return False, results

    if r.status_code == 200:
        results.append(Result("/", True, r.status_code, dt))
        log(f"GET / -> Server UP ({fmt_dt(dt)})", "success")
    else:
        results.append(Result("/", False, r.status_code, dt, body_preview(r, 200)))
        log(f"GET / -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={body_preview(r, 200)!r}", "fail")
        return False, results

    # HEAD /
    rh, dt, err = request_any(sess, "HEAD", f"{base_url}/", headers=headers, timeout=TIMEOUT_SHORT)
    if err or rh is None:
        results.append(Result("HEAD /", False, None, dt, err or "unknown"))
        log(f"HEAD / -> Exception: {err}", "warn")
    else:
        if rh.status_code in (200, 204):
            results.append(Result("HEAD /", True, rh.status_code, dt))
            log(f"HEAD / -> OK ({fmt_dt(dt)})", "success")
        else:
            results.append(Result("HEAD /", False, rh.status_code, dt, "non-200/204"))
            log(f"HEAD / -> HTTP {rh.status_code} ({fmt_dt(dt)})", "warn")

    # GET /healthz (critical on Render)
    r2, dt, err = request_any(sess, "GET", f"{base_url}/healthz", headers=headers, timeout=TIMEOUT_SHORT)
    if err or r2 is None:
        results.append(Result("/healthz", False, None, dt, err or "unknown"))
        log(f"GET /healthz -> Exception: {err}", "fail")
        return False, results

    if r2.status_code == 200:
        results.append(Result("/healthz", True, r2.status_code, dt))
        log(f"GET /healthz -> ok ({fmt_dt(dt)})", "success")
    else:
        results.append(Result("/healthz", False, r2.status_code, dt, body_preview(r2, 200)))
        log(f"GET /healthz -> HTTP {r2.status_code} ({fmt_dt(dt)}) | body={body_preview(r2, 200)!r}", "fail")
        return False, results

    # GET /health (soft)
    r3, dt, err = request_any(sess, "GET", f"{base_url}/health", headers=headers, timeout=TIMEOUT_SHORT)
    if err or r3 is None:
        results.append(Result("/health", False, None, dt, err or "unknown"))
        log(f"GET /health -> Exception: {err}", "warn")
    else:
        if r3.status_code == 200:
            j = safe_json(r3) or {}
            status = j.get("status") or "unknown"
            ver = j.get("version") or j.get("app_version") or ""
            results.append(Result("/health", True, r3.status_code, dt, f"{status} {ver}".strip()))
            log(f"GET /health -> {status} ({fmt_dt(dt)}) {ver}".strip(), "success")
        else:
            results.append(Result("/health", False, r3.status_code, dt, body_preview(r3, 200)))
            log(f"GET /health -> HTTP {r3.status_code} ({fmt_dt(dt)})", "warn")

    # GET /readyz (soft but useful)
    r4, dt, err = request_any(sess, "GET", f"{base_url}/readyz", headers=headers, timeout=TIMEOUT_MED)
    if err or r4 is None:
        results.append(Result("/readyz", False, None, dt, err or "unknown"))
        log(f"GET /readyz -> Exception: {err}", "warn")
    else:
        if r4.status_code in (200, 503):
            j = safe_json(r4) or {}
            results.append(Result("/readyz", True, r4.status_code, dt, str(j.get("status", ""))))
            log(f"GET /readyz -> HTTP {r4.status_code} ({fmt_dt(dt)}) | {j.get('status','')}", "success")
        else:
            results.append(Result("/readyz", False, r4.status_code, dt, body_preview(r4, 200)))
            log(f"GET /readyz -> HTTP {r4.status_code} ({fmt_dt(dt)})", "warn")

    return True, results


def _handle_404(strict: bool, ep: str) -> None:
    if strict:
        log(f"{ep.ljust(28)} -> FAIL (404 not mounted, strict mode)", "fail")
    else:
        log(f"{ep.ljust(28)} -> SKIP (404 not mounted)", "warn")


def check_router_health(sess: requests.Session, base_url: str, headers: Dict[str, str], *, strict: bool) -> None:
    log("Checking Router Health", "header")
    endpoints = [
        "/v1/enriched/health",
        "/v1/analysis/health",
        "/v1/advanced/health",
        "/v1/argaam/health",
        "/system/settings",
        "/system/routes",
    ]

    for ep in endpoints:
        r, dt, err = request_any(sess, "GET", f"{base_url}{ep}", headers=headers, timeout=TIMEOUT_MED)
        if err or r is None:
            log(f"{ep.ljust(28)} -> Exception: {err}", "fail")
            continue

        if r.status_code == 200:
            j = safe_json(r) or {}
            ver = j.get("version") or j.get("route_version") or ""
            status = j.get("status") or "ok"
            extra = f" v={ver}" if ver else ""
            log(f"{ep.ljust(28)} -> {status} ({fmt_dt(dt)}){extra}", "success")
        elif r.status_code == 404:
            _handle_404(strict, ep)
        else:
            log(f"{ep.ljust(28)} -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={body_preview(r)!r}", "fail")


def check_quote(sess: requests.Session, base_url: str, headers: Dict[str, str], symbol: str) -> None:
    log(f"Testing Single Quote ({symbol})", "header")
    r, dt, err = request_any(
        sess,
        "GET",
        f"{base_url}/v1/enriched/quote",
        headers=headers,
        timeout=TIMEOUT_MED,
        params={"symbol": symbol},
    )
    if err or r is None:
        log(f"/v1/enriched/quote -> Exception: {err}", "fail")
        return

    if r.status_code != 200:
        log(f"/v1/enriched/quote -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={body_preview(r)!r}", "fail")
        return

    data = safe_json(r)
    if not isinstance(data, dict):
        log(f"/v1/enriched/quote -> Non-JSON response ({fmt_dt(dt)})", "fail")
        print(r.text[:500])
        return

    sym_resp = str(data.get("symbol") or data.get("ticker") or "").upper()
    price = data.get("current_price") or data.get("last_price") or data.get("price")
    dq = data.get("data_quality")
    src = data.get("data_source") or data.get("source")
    err_msg = data.get("error")

    if sym_resp and price is not None:
        ok = "OK" if sym_resp == symbol.upper() else "MISMATCH"
        typ = "success" if ok == "OK" else "warn"
        extra = f" err={err_msg}" if err_msg else ""
        log(f"{ok} ({fmt_dt(dt)}) | symbol={sym_resp} price={price} dq={dq} src={src}{extra}", typ)
    else:
        log(f"Returned 200 but missing fields ({fmt_dt(dt)}). Payload:", "warn")
        print(json_pretty(data))


def check_batch_quotes(sess: requests.Session, base_url: str, headers: Dict[str, str], ksa_symbol: str, global_symbol: str, *, strict: bool) -> None:
    log("Testing Batch /v1/enriched/quotes", "header")
    payload = {
        "symbols": [ksa_symbol, global_symbol],
        "tickers": [ksa_symbol, global_symbol],
        "sheet_name": "Test_Script",
        "sheetName": "Test_Script",
    }
    r, dt, err = request_any(
        sess,
        "POST",
        f"{base_url}/v1/enriched/quotes",
        headers=headers,
        timeout=TIMEOUT_LONG,
        json_body=payload,
    )
    if err or r is None:
        log(f"/v1/enriched/quotes -> Exception: {err}", "fail")
        return

    if r.status_code == 404:
        _handle_404(strict, "/v1/enriched/quotes")
        return
    if r.status_code != 200:
        log(f"/v1/enriched/quotes -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={body_preview(r)!r}", "fail")
        return

    data = safe_json(r) or {}
    status = data.get("status")
    count = data.get("count")
    rows = data.get("rows") or []
    quotes = data.get("quotes") or []
    log(f"/v1/enriched/quotes -> {status} ({fmt_dt(dt)}) | count={count} rows={len(rows)} quotes={len(quotes)}", "success")


def check_sheet_rows_enriched(sess: requests.Session, base_url: str, headers: Dict[str, str], ksa_symbol: str, global_symbol: str, *, strict: bool) -> None:
    log("Testing Batch /v1/enriched/sheet-rows", "header")
    payload = {
        "tickers": [ksa_symbol, global_symbol],
        "symbols": [ksa_symbol, global_symbol],
        "sheet_name": "Test_Script",
        "sheetName": "Test_Script",
    }

    r, dt, err = request_any(
        sess,
        "POST",
        f"{base_url}/v1/enriched/sheet-rows",
        headers=headers,
        timeout=TIMEOUT_LONG,
        json_body=payload,
    )
    if err or r is None:
        log(f"/v1/enriched/sheet-rows -> Exception: {err}", "fail")
        return

    if r.status_code == 404:
        _handle_404(strict, "/v1/enriched/sheet-rows")
        return
    if r.status_code != 200:
        log(f"/v1/enriched/sheet-rows -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={body_preview(r)!r}", "fail")
        return

    data = safe_json(r)
    if not isinstance(data, dict):
        log(f"/v1/enriched/sheet-rows -> Non-JSON response ({fmt_dt(dt)})", "fail")
        return

    rows = data.get("rows", []) or []
    headers_ = data.get("headers", []) or []
    if len(rows) >= 1 and len(headers_) >= 1:
        log(f"OK ({fmt_dt(dt)}) | rows={len(rows)} headers={len(headers_)}", "success")
    else:
        log(f"Unexpected shape ({fmt_dt(dt)}). Payload:", "warn")
        print(json_pretty(data))


def check_argaam_sheet_rows(sess: requests.Session, base_url: str, headers: Dict[str, str], ksa_symbol: str, *, strict: bool) -> None:
    log("Testing KSA /v1/argaam/sheet-rows", "header")
    payload = {
        "symbols": [ksa_symbol, "BADSYM"],
        "tickers": [ksa_symbol, "BADSYM"],
        "sheet_name": "KSA_Tadawul_Market",
        "sheetName": "KSA_Tadawul_Market",
    }

    r, dt, err = request_any(
        sess,
        "POST",
        f"{base_url}/v1/argaam/sheet-rows",
        headers=headers,
        timeout=TIMEOUT_LONG,
        json_body=payload,
    )
    if err or r is None:
        log(f"/v1/argaam/sheet-rows -> Exception: {err}", "fail")
        return

    if r.status_code == 404:
        _handle_404(strict, "/v1/argaam/sheet-rows")
        return
    if r.status_code != 200:
        log(f"/v1/argaam/sheet-rows -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={body_preview(r)!r}", "fail")
        return

    data = safe_json(r)
    if isinstance(data, dict) and isinstance(data.get("rows"), list) and isinstance(data.get("headers"), list):
        log(f"OK ({fmt_dt(dt)}) | rows={len(data.get('rows') or [])} headers={len(data.get('headers') or [])}", "success")
    else:
        log(f"Unexpected response shape ({fmt_dt(dt)}). Payload:", "warn")
        print(json_pretty(data))


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", default=DEFAULT_BASE_URL)
    p.add_argument("--token", default=DEFAULT_TOKEN)
    p.add_argument("--ksa", default=KSA_TEST_SYMBOL)
    p.add_argument("--global", dest="global_sym", default=GLOBAL_TEST_SYMBOL)
    p.add_argument("--strict", action="store_true", help="Treat 404 (not mounted) as failures")
    args = p.parse_args()

    base_url = str(args.base_url).rstrip("/")
    token = str(args.token).strip()
    strict = bool(args.strict)

    print(f"{C.HEADER}TADAWUL FAST BRIDGE - ENDPOINT TESTER v2.4.0{C.ENDC}")
    print(f"BASE_URL={base_url} | TOKEN={'SET' if token else 'NOT SET'} | STRICT={'ON' if strict else 'OFF'}")
    print(f"KSA_TEST_SYMBOL={args.ksa} | GLOBAL_TEST_SYMBOL={args.global_sym}")

    sess = requests.Session()
    hdrs = build_headers(token)

    ok, server_results = check_server(sess, base_url, hdrs)
    if not ok:
        print("\n❌ Cannot proceed (server connectivity failed).")
        return 2

    check_router_health(sess, base_url, hdrs, strict=strict)
    check_quote(sess, base_url, hdrs, args.ksa)
    check_quote(sess, base_url, hdrs, args.global_sym)
    check_batch_quotes(sess, base_url, hdrs, args.ksa, args.global_sym, strict=strict)
    check_sheet_rows_enriched(sess, base_url, hdrs, args.ksa, args.global_sym, strict=strict)
    check_argaam_sheet_rows(sess, base_url, hdrs, args.ksa, strict=strict)

    # Critical summary
    fails = [r for r in server_results if not r.ok and r.endpoint in ("/", "/healthz")]
    if fails:
        print("\n❌ Critical checks failed.")
        return 2

    print("\n✅ Smoke test finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
