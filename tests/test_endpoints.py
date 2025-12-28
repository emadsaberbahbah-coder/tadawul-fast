#!/usr/bin/env python3
# test_endpoints.py  (FULL REPLACEMENT)
"""
test_endpoints.py
===========================================================
SMOKE TESTER for Tadawul Fast Bridge API – v2.5.0 (Render-True)

Key changes (vs older variants)
- ✅ /healthz is the ONLY critical connectivity check (root "/" is SOFT)
- ✅ Confirms /system/settings snapshot: KSA_PROVIDERS must be ["yahoo_chart"]
- ✅ KSA quote must be success + current_price > 0 + error empty
- ✅ GLOBAL quote must be success + market_cap and pe_ttm not null
- ✅ Token from env: TFB_APP_TOKEN only (optional)
- ✅ 404 routes are soft unless --strict
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

DEFAULT_TOKEN = (os.getenv("TFB_APP_TOKEN") or "").strip()

KSA_TEST_SYMBOL = (os.getenv("TFB_KSA_TEST_SYMBOL", "1120.SR") or "1120.SR").strip().upper()
GLOBAL_TEST_SYMBOL = (os.getenv("TFB_GLOBAL_TEST_SYMBOL", "AAPL") or "AAPL").strip().upper()

TIMEOUT_SHORT = float(os.getenv("TFB_TIMEOUT_SHORT", "15") or "15")
TIMEOUT_MED = float(os.getenv("TFB_TIMEOUT_MED", "25") or "25")
TIMEOUT_LONG = float(os.getenv("TFB_TIMEOUT_LONG", "60") or "60")

USER_AGENT = os.getenv("TFB_USER_AGENT", "TadawulFastBridge-EndpointTester/2.5.0") or "TadawulFastBridge-EndpointTester/2.5.0"


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
        # optional; your service currently does not require auth
        h["X-APP-TOKEN"] = token
        h["Authorization"] = f"Bearer {token}"
    return h


@dataclass
class Hit:
    ep: str
    ok: bool
    code: Optional[int]
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


def handle_404(strict: bool, ep: str) -> bool:
    if strict:
        log(f"{ep} -> FAIL (404, strict mode)", "fail")
        return False
    log(f"{ep} -> SKIP (404 not mounted)", "warn")
    return True


# -----------------------------------------------------------------------------
# Phase 0: Connectivity (critical: /healthz only)
# -----------------------------------------------------------------------------
def check_connectivity(sess: requests.Session, base_url: str, headers: Dict[str, str], *, strict: bool) -> Tuple[bool, List[Hit]]:
    log("Connectivity", "header")
    hits: List[Hit] = []

    # SOFT: GET /
    r, dt, err = request_any(sess, "GET", f"{base_url}/", headers=headers, timeout=TIMEOUT_SHORT)
    if err or r is None:
        hits.append(Hit("/", False, None, dt, f"exception: {err}"))
        log(f"GET / -> exception (soft): {err} ({fmt_dt(dt)})", "warn")
    else:
        if r.status_code in (200, 404):
            hits.append(Hit("/", True, r.status_code, dt))
            log(f"GET / -> HTTP {r.status_code} (soft ok) ({fmt_dt(dt)})", "success")
        else:
            hits.append(Hit("/", False, r.status_code, dt, body_preview(r)))
            log(f"GET / -> HTTP {r.status_code} (soft) ({fmt_dt(dt)}) body={body_preview(r)!r}", "warn")

    # CRITICAL: GET /healthz
    r2, dt, err = request_any(sess, "GET", f"{base_url}/healthz", headers=headers, timeout=TIMEOUT_SHORT)
    if err or r2 is None:
        hits.append(Hit("/healthz", False, None, dt, f"exception: {err}"))
        log(f"GET /healthz -> exception: {err} ({fmt_dt(dt)})", "fail")
        return False, hits

    if r2.status_code == 200:
        hits.append(Hit("/healthz", True, r2.status_code, dt))
        log(f"GET /healthz -> ok ({fmt_dt(dt)})", "success")
    else:
        hits.append(Hit("/healthz", False, r2.status_code, dt, body_preview(r2)))
        log(f"GET /healthz -> HTTP {r2.status_code} ({fmt_dt(dt)}) body={body_preview(r2)!r}", "fail")
        return False, hits

    # SOFT: /readyz
    r3, dt, err = request_any(sess, "GET", f"{base_url}/readyz", headers=headers, timeout=TIMEOUT_MED)
    if err or r3 is None:
        hits.append(Hit("/readyz", False, None, dt, f"exception: {err}"))
        log(f"GET /readyz -> exception (soft): {err} ({fmt_dt(dt)})", "warn")
    else:
        if r3.status_code in (200, 503):
            j = safe_json(r3) or {}
            hits.append(Hit("/readyz", True, r3.status_code, dt, str(j.get("status", ""))))
            log(f"GET /readyz -> HTTP {r3.status_code} (soft ok) ({fmt_dt(dt)}) status={j.get('status','')}", "success")
        elif r3.status_code == 404:
            ok_404 = handle_404(strict, "/readyz")
            hits.append(Hit("/readyz", ok_404, 404, dt))
        else:
            hits.append(Hit("/readyz", False, r3.status_code, dt, body_preview(r3)))
            log(f"GET /readyz -> HTTP {r3.status_code} (soft) ({fmt_dt(dt)})", "warn")

    # SOFT: /health
    r4, dt, err = request_any(sess, "GET", f"{base_url}/health", headers=headers, timeout=TIMEOUT_SHORT)
    if err or r4 is None:
        hits.append(Hit("/health", False, None, dt, f"exception: {err}"))
        log(f"GET /health -> exception (soft): {err} ({fmt_dt(dt)})", "warn")
    else:
        if r4.status_code == 200:
            j = safe_json(r4) or {}
            hits.append(Hit("/health", True, 200, dt, f"status={j.get('status','')} ver={j.get('version','')}"))
            log(f"GET /health -> ok (soft) ({fmt_dt(dt)}) ver={j.get('version','')}", "success")
        elif r4.status_code == 404:
            ok_404 = handle_404(strict, "/health")
            hits.append(Hit("/health", ok_404, 404, dt))
        else:
            hits.append(Hit("/health", False, r4.status_code, dt, body_preview(r4)))
            log(f"GET /health -> HTTP {r4.status_code} (soft) ({fmt_dt(dt)})", "warn")

    return True, hits


# -----------------------------------------------------------------------------
# Phase 0: Settings snapshot validation (critical)
# -----------------------------------------------------------------------------
def check_settings_snapshot(sess: requests.Session, base_url: str, headers: Dict[str, str]) -> Tuple[bool, Dict[str, Any]]:
    log("Settings Snapshot", "header")
    r, dt, err = request_any(sess, "GET", f"{base_url}/system/settings", headers=headers, timeout=TIMEOUT_MED)
    if err or r is None:
        log(f"GET /system/settings -> exception: {err}", "fail")
        return False, {}

    if r.status_code != 200:
        log(f"GET /system/settings -> HTTP {r.status_code} ({fmt_dt(dt)}) body={body_preview(r)!r}", "fail")
        return False, {}

    j = safe_json(r)
    if not isinstance(j, dict):
        log("GET /system/settings -> non-JSON object", "fail")
        return False, {}

    snap = j.get("env_snapshot") or {}
    if not isinstance(snap, dict):
        log("env_snapshot missing/invalid", "fail")
        return False, {}

    ksa_providers = snap.get("KSA_PROVIDERS")
    enabled_providers = snap.get("ENABLED_PROVIDERS")
    ver = snap.get("APP_VERSION_RESOLVED", "")
    commit = snap.get("RENDER_GIT_COMMIT", "")

    log(f"APP_VERSION_RESOLVED={ver} | RENDER_GIT_COMMIT={commit}", "info")
    log(f"ENABLED_PROVIDERS={enabled_providers}", "info")
    log(f"KSA_PROVIDERS={ksa_providers}", "info")

    # CRITICAL expectation now
    if ksa_providers != ["yahoo_chart"]:
        log(f"KSA_PROVIDERS is not ['yahoo_chart'] -> got {ksa_providers}", "fail")
        return False, snap

    log("KSA_PROVIDERS is exactly ['yahoo_chart'] ✅", "success")
    return True, snap


# -----------------------------------------------------------------------------
# Phase 0: Quote validations (critical)
# -----------------------------------------------------------------------------
def get_quote(sess: requests.Session, base_url: str, headers: Dict[str, str], symbol: str) -> Tuple[bool, Dict[str, Any]]:
    r, dt, err = request_any(
        sess,
        "GET",
        f"{base_url}/v1/enriched/quote",
        headers=headers,
        timeout=TIMEOUT_MED,
        params={"symbol": symbol},
    )
    if err or r is None:
        log(f"/v1/enriched/quote({symbol}) -> exception: {err}", "fail")
        return False, {}
    if r.status_code != 200:
        log(f"/v1/enriched/quote({symbol}) -> HTTP {r.status_code} ({fmt_dt(dt)}) body={body_preview(r)!r}", "fail")
        return False, {}
    j = safe_json(r)
    if not isinstance(j, dict):
        log(f"/v1/enriched/quote({symbol}) -> non-JSON object", "fail")
        return False, {}
    j["_latency_sec"] = dt
    return True, j


def check_ksa_quote(sess: requests.Session, base_url: str, headers: Dict[str, str], symbol: str) -> bool:
    log(f"KSA Quote Check ({symbol})", "header")
    ok, q = get_quote(sess, base_url, headers, symbol)
    if not ok:
        return False

    price = float(q.get("current_price") or 0)
    status = str(q.get("status") or "").lower()
    err = str(q.get("error") or "").strip()
    src = q.get("data_source")
    dt = q.get("_latency_sec", 0.0)

    log(f"symbol={q.get('symbol')} market={q.get('market')} price={price} status={status} src={src} ({fmt_dt(float(dt))})", "info")

    if price <= 0:
        log("KSA current_price missing/<=0", "fail")
        return False
    if status != "success":
        log(f"KSA status != success (status={status}, error={err})", "fail")
        return False
    if err:
        log(f"KSA error not empty (should be empty now): {err}", "fail")
        return False

    log("KSA quote is clean ✅ (success + price + no warnings)", "success")
    return True


def check_global_quote(sess: requests.Session, base_url: str, headers: Dict[str, str], symbol: str) -> bool:
    log(f"GLOBAL Quote Check ({symbol})", "header")
    ok, q = get_quote(sess, base_url, headers, symbol)
    if not ok:
        return False

    status = str(q.get("status") or "").lower()
    mcap = q.get("market_cap")
    pe = q.get("pe_ttm")
    err = str(q.get("error") or "").strip()
    src = q.get("data_source")
    dt = q.get("_latency_sec", 0.0)

    log(f"symbol={q.get('symbol')} market={q.get('market')} status={status} src={src} ({fmt_dt(float(dt))})", "info")
    log(f"market_cap={mcap} | pe_ttm={pe}", "info")

    if status != "success":
        log(f"GLOBAL status != success (status={status}, error={err})", "fail")
        return False
    if mcap in (None, "", 0, 0.0):
        log("GLOBAL market_cap missing", "fail")
        return False
    if pe in (None, "", 0, 0.0):
        log("GLOBAL pe_ttm missing", "fail")
        return False

    log("GLOBAL fundamentals OK ✅ (market_cap + pe_ttm present)", "success")
    return True


# -----------------------------------------------------------------------------
# Soft router health (optional)
# -----------------------------------------------------------------------------
def check_optional_routes(sess: requests.Session, base_url: str, headers: Dict[str, str], *, strict: bool) -> bool:
    log("Optional Routes (soft)", "header")
    eps = [
        "/system/routes",
        "/v1/enriched/health",
        "/v1/analysis/health",
        "/v1/advanced/health",
        "/v1/argaam/health",
    ]
    all_ok = True
    for ep in eps:
        r, dt, err = request_any(sess, "GET", f"{base_url}{ep}", headers=headers, timeout=TIMEOUT_MED)
        if err or r is None:
            log(f"{ep} -> exception: {err}", "warn")
            all_ok = False
            continue

        if r.status_code == 200:
            j = safe_json(r) or {}
            status = j.get("status") or "ok"
            ver = j.get("version") or j.get("route_version") or ""
            extra = f" v={ver}" if ver else ""
            log(f"{ep} -> {status} ({fmt_dt(dt)}){extra}", "success")
        elif r.status_code == 404:
            all_ok = handle_404(strict, ep) and all_ok
        else:
            log(f"{ep} -> HTTP {r.status_code} ({fmt_dt(dt)}) body={body_preview(r)!r}", "warn")
            all_ok = False

    return all_ok


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

    print(f"{C.HEADER}TADAWUL FAST BRIDGE - ENDPOINT TESTER v2.5.0{C.ENDC}")
    print(f"BASE_URL={base_url} | TOKEN={'SET' if token else 'NOT SET'} | STRICT={'ON' if strict else 'OFF'}")
    print(f"KSA_TEST_SYMBOL={args.ksa} | GLOBAL_TEST_SYMBOL={args.global_sym}")

    sess = requests.Session()
    headers = build_headers(token)

    # Critical: connectivity
    ok_conn, _ = check_connectivity(sess, base_url, headers, strict=strict)
    if not ok_conn:
        print("\n❌ FAIL: Connectivity (healthz) failed.")
        return 2

    # Critical: settings snapshot (KSA providers must be yahoo_chart only)
    ok_settings, _ = check_settings_snapshot(sess, base_url, headers)
    if not ok_settings:
        print("\n❌ FAIL: Settings snapshot does not match expected routing.")
        return 3

    # Critical: KSA quote clean
    ok_ksa = check_ksa_quote(sess, base_url, headers, args.ksa)
    if not ok_ksa:
        print("\n❌ FAIL: KSA quote check failed.")
        return 4

    # Critical: GLOBAL fundamentals exist
    ok_global = check_global_quote(sess, base_url, headers, args.global_sym)
    if not ok_global:
        print("\n❌ FAIL: GLOBAL quote fundamentals check failed.")
        return 5

    # Soft: optional routes
    check_optional_routes(sess, base_url, headers, strict=strict)

    print("\n✅ PASS: Baseline validated (KSA clean + Global fundamentals present).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
