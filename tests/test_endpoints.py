#!/usr/bin/env python3
# tests/test_endpoints.py  (FULL REPLACEMENT)
"""
SMOKE TESTER for Tadawul Fast Bridge API – v2.6.0 (Prod-aligned + safer)

What it tests (based on your deployed reality):
- GET /, /healthz, /readyz, /health
- Router health:
    /v1/enriched/health
    /v1/analysis/health
    /v1/advanced/health
    /v1/argaam/health
    /system/settings
- Single quote:
    GET /v1/enriched/quote?symbol=...
- Batch quotes (GET):
    GET /v1/enriched/quotes?symbols=SYM1,SYM2,...

Notes
- /system/routes is NOT deployed (404) -> not tested
- /v1/enriched/sheet-rows is NOT deployed (404) -> not tested
- Batch endpoint expects "symbols" query param (NOT "tickers")
- Token header: X-APP-TOKEN (plus Authorization Bearer for back-compat)

Usage:
  python tests/test_endpoints.py
  TFB_BASE_URL=https://tadawul-fast-bridge.onrender.com python tests/test_endpoints.py
  python tests/test_endpoints.py --ksa 1120.SR --global AAPL
  python tests/test_endpoints.py --base-url http://127.0.0.1:8000 --no-color
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests


SCRIPT_VERSION = "2.6.0"

DEFAULT_BASE_URL = (
    os.getenv("TFB_BASE_URL")
    or os.getenv("BACKEND_BASE_URL")
    or os.getenv("BASE_URL")
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

USER_AGENT = os.getenv(
    "TFB_USER_AGENT",
    f"TadawulFastBridge-EndpointTester/{SCRIPT_VERSION}",
) or f"TadawulFastBridge-EndpointTester/{SCRIPT_VERSION}"


def _colors_enabled(force_off: bool = False) -> bool:
    if force_off:
        return False
    if os.getenv("NO_COLOR"):
        return False
    try:
        return sys.stdout.isatty()
    except Exception:
        return False


class C:
    HEADER: str = ""
    OKBLUE: str = ""
    OKGREEN: str = ""
    WARNING: str = ""
    FAIL: str = ""
    ENDC: str = ""


def _init_colors(use_color: bool) -> None:
    C.HEADER = "\033[95m" if use_color else ""
    C.OKBLUE = "\033[94m" if use_color else ""
    C.OKGREEN = "\033[92m" if use_color else ""
    C.WARNING = "\033[93m" if use_color else ""
    C.FAIL = "\033[91m" if use_color else ""
    C.ENDC = "\033[0m" if use_color else ""


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


def json_pretty(obj: Any, limit: int = 2000) -> str:
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
    h: Dict[str, str] = {"Accept": "application/json", "User-Agent": USER_AGENT}
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
) -> Tuple[Optional[requests.Response], float, Optional[str]]:
    t0 = time.time()
    try:
        r = sess.request(method, url, headers=headers, timeout=timeout, params=params)
        return r, time.time() - t0, None
    except Exception as e:
        return None, time.time() - t0, str(e)


def check_server(sess: requests.Session, base_url: str, headers: Dict[str, str]) -> bool:
    log("Checking Server Connectivity...", "header")

    checks = [
        ("/", True, TIMEOUT_MED),
        ("/healthz", True, TIMEOUT_MED),
        ("/health", False, TIMEOUT_MED),
        ("/readyz", False, TIMEOUT_MED),
    ]

    for ep, critical, tmo in checks:
        r, dt, err = request_any(sess, "GET", f"{base_url}{ep}", headers=headers, timeout=tmo)
        if err or r is None:
            log(f"GET {ep} -> Exception: {err}", "fail" if critical else "warn")
            if critical:
                return False
            continue

        if r.status_code == 200:
            j = safe_json(r) or {}
            note = ""
            if ep == "/health":
                note = f"status={j.get('status')} ver={j.get('version')}"
            if ep == "/readyz":
                note = f"status={j.get('status')} engine_ready={j.get('engine_ready')}"
            log(f"GET {ep} -> 200 ({fmt_dt(dt)}) {note}".strip(), "success")
        else:
            log(
                f"GET {ep} -> HTTP {r.status_code} ({fmt_dt(dt)}) body={body_preview(r)!r}",
                "fail" if critical else "warn",
            )
            if critical:
                return False

    return True


def check_router_health(sess: requests.Session, base_url: str, headers: Dict[str, str]) -> None:
    log("Checking Router Health", "header")
    endpoints = [
        "/v1/enriched/health",
        "/v1/analysis/health",
        "/v1/advanced/health",
        "/v1/argaam/health",
        "/system/settings",
    ]
    for ep in endpoints:
        r, dt, err = request_any(sess, "GET", f"{base_url}{ep}", headers=headers, timeout=TIMEOUT_MED)
        if err or r is None:
            log(f"{ep.ljust(26)} -> Exception: {err}", "fail")
            continue

        if r.status_code == 200:
            j = safe_json(r) or {}
            ver = ""
            if isinstance(j, dict):
                ver = str(j.get("version") or j.get("route_version") or "").strip()
                status = str(j.get("status") or "ok").strip()
            else:
                status = "ok"
            extra = f" v={ver}" if ver else ""
            log(f"{ep.ljust(26)} -> {status} ({fmt_dt(dt)}){extra}", "success")
        else:
            log(f"{ep.ljust(26)} -> HTTP {r.status_code} ({fmt_dt(dt)}) body={body_preview(r)!r}", "warn")


def check_quote(sess: requests.Session, base_url: str, headers: Dict[str, str], symbol: str) -> None:
    symbol = (symbol or "").strip().upper()
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
        log(f"/v1/enriched/quote -> HTTP {r.status_code} ({fmt_dt(dt)}) body={body_preview(r)!r}", "fail")
        return

    data = safe_json(r)
    if not isinstance(data, dict):
        log(f"/v1/enriched/quote -> Non-JSON response ({fmt_dt(dt)}) body={body_preview(r)!r}", "fail")
        return

    price = data.get("current_price")
    status = data.get("status")
    src = data.get("data_source")
    dq = data.get("data_quality")
    err_msg = data.get("error") or ""

    log(f"status={status} price={price} dq={dq} src={src} ({fmt_dt(dt)})", "success" if price is not None else "warn")
    if err_msg:
        log(f"warning/error: {err_msg}", "warn")


def check_batch_quotes(sess: requests.Session, base_url: str, headers: Dict[str, str], symbols: List[str]) -> None:
    log("Testing Batch Quotes (GET /v1/enriched/quotes?symbols=...)", "header")
    syms = [str(s or "").strip().upper() for s in (symbols or []) if str(s or "").strip()]
    q = ",".join(syms)

    r, dt, err = request_any(
        sess,
        "GET",
        f"{base_url}/v1/enriched/quotes",
        headers=headers,
        timeout=TIMEOUT_LONG,
        params={"symbols": q},
    )
    if err or r is None:
        log(f"/v1/enriched/quotes -> Exception: {err}", "fail")
        return

    if r.status_code != 200:
        log(f"/v1/enriched/quotes -> HTTP {r.status_code} ({fmt_dt(dt)}) body={body_preview(r)!r}", "fail")
        return

    data = safe_json(r)
    if not isinstance(data, dict):
        log(f"/v1/enriched/quotes -> Non-JSON response ({fmt_dt(dt)}) body={body_preview(r)!r}", "fail")
        return

    items = data.get("items") or []
    log(f"batch status={data.get('status')} count={data.get('count')} items={len(items)} ({fmt_dt(dt)})", "success")

    for it in items[:10]:
        if not isinstance(it, dict):
            continue
        sym = it.get("symbol")
        mkt = it.get("market")
        px = it.get("current_price")
        st = it.get("status")
        src = it.get("data_source")
        log(f"{sym} | {mkt} | price={px} | status={st} | src={src}", "info")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", default=DEFAULT_BASE_URL)
    p.add_argument("--token", default=DEFAULT_TOKEN)
    p.add_argument("--ksa", default=KSA_TEST_SYMBOL)
    p.add_argument("--global", dest="global_sym", default=GLOBAL_TEST_SYMBOL)
    p.add_argument("--no-color", action="store_true", help="Disable ANSI colors")
    p.add_argument("--verbose", action="store_true", help="Print JSON previews for health endpoints")
    args = p.parse_args()

    base_url = str(args.base_url).rstrip("/")
    token = str(args.token).strip()

    _init_colors(_colors_enabled(force_off=bool(args.no_color)))

    print(f"{C.HEADER}TADAWUL FAST BRIDGE - ENDPOINT TESTER v{SCRIPT_VERSION}{C.ENDC}")
    print(f"BASE_URL={base_url} | TOKEN={'SET' if token else 'NOT SET'}")
    print(f"KSA_TEST_SYMBOL={args.ksa} | GLOBAL_TEST_SYMBOL={args.global_sym}")

    sess = requests.Session()
    hdrs = build_headers(token)

    if not check_server(sess, base_url, hdrs):
        print("\n❌ Cannot proceed (server connectivity failed).")
        return 2

    # Router health (optionally show JSON previews)
    check_router_health(sess, base_url, hdrs)
    if args.verbose:
        log("Verbose health JSON previews", "header")
        for ep in ["/v1/enriched/health", "/v1/analysis/health", "/v1/advanced/health", "/v1/argaam/health", "/system/settings"]:
            r, dt, err = request_any(sess, "GET", f"{base_url}{ep}", headers=hdrs, timeout=TIMEOUT_MED)
            if err or r is None:
                log(f"{ep} -> Exception: {err}", "warn")
                continue
            j = safe_json(r)
            log(f"{ep} ({fmt_dt(dt)}) -> {json_pretty(j)}", "info")

    check_quote(sess, base_url, hdrs, str(args.ksa))
    check_quote(sess, base_url, hdrs, str(args.global_sym))
    check_batch_quotes(sess, base_url, hdrs, [str(args.ksa), str(args.global_sym)])

    print("\n✅ Smoke test finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
