```python
#!/usr/bin/env python3
# test_endpoints.py
"""
test_endpoints.py
===========================================================
SMOKE TESTER for Tadawul Fast Bridge API – v2.3.0 (Hardened)
===========================================================

Enhancements vs v2.1.0
- CLI flags + env support:
    • Base URL:  --base-url or TFB_BASE_URL or BACKEND_BASE_URL
    • Token:     --token or TFB_APP_TOKEN / APP_TOKEN / BACKUP_APP_TOKEN
    • Test symbols: --ksa-symbol / --global-symbol
- Render-friendly connectivity tests:
    • GET /, HEAD /, GET /health (soft)
    • Supports cases where / returns non-JSON (still OK if 200/204)
- Better diagnostics:
    • Per-endpoint response time
    • Prints module/version fields if present
    • Shows 1st error message and data_quality summary for batch calls
    • Soft-fail on 404 "not mounted" routes (SKIP)
- Tests:
    • /v1/enriched/quote (single)
    • /v1/enriched/quotes (batch)
    • /v1/enriched/sheet-rows (Sheets payload)
    • /v1/argaam/health
    • /v1/argaam/sheet-rows (KSA strict)
    • /v1/analysis/health and /v1/analysis/quotes (AI batch)
    • /v1/advanced/health (optional)
- Safe JSON parsing: never crashes on non-JSON or HTML error pages

Usage
  python test_endpoints.py
  python test_endpoints.py --base-url https://your-render.onrender.com --token XXXXX
  TFB_BASE_URL=https://your-render.onrender.com python test_endpoints.py
  TFB_APP_TOKEN=... python test_endpoints.py

Exit code
- 0 if server reachable AND no hard failures
- 2 if server not reachable OR any hard failure (non-404 endpoint failures)

Notes
- If an endpoint isn't mounted, the test will SKIP it (404).
"""

from __future__ import annotations

import argparse
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

# =============================================================================
# Colors / logging
# =============================================================================


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


def fmt_dt(dt: float) -> str:
    return f"{dt:.2f}s"


def json_pretty(obj: Any, limit: int = 2500) -> str:
    try:
        import json

        return json.dumps(obj, indent=2, ensure_ascii=False)[:limit]
    except Exception:
        return str(obj)[:limit]


def safe_json(resp: requests.Response) -> Optional[Any]:
    try:
        return resp.json()
    except Exception:
        return None


# =============================================================================
# ENV + CLI
# =============================================================================


def _env_base_url() -> str:
    return (
        os.getenv("TFB_BASE_URL")
        or os.getenv("BACKEND_BASE_URL")
        or "http://127.0.0.1:8000"
    ).rstrip("/")


def _env_token() -> str:
    return (
        os.getenv("TFB_APP_TOKEN")
        or os.getenv("APP_TOKEN")
        or os.getenv("BACKUP_APP_TOKEN")
        or ""
    ).strip()


def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name)
        return float(v) if v is not None and str(v).strip() else float(default)
    except Exception:
        return float(default)


def build_headers(token: str) -> Dict[str, str]:
    h: Dict[str, str] = {"Accept": "application/json"}
    if token:
        h["X-APP-TOKEN"] = token
    return h


# =============================================================================
# HTTP helpers
# =============================================================================


def _req(
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
        r = requests.request(
            method=method.upper(),
            url=url,
            headers=headers,
            params=params,
            json=json_body,
            timeout=timeout,
        )
        return r, time.time() - t0, None
    except Exception as e:
        return None, time.time() - t0, str(e)


def _shape_hint(obj: Any) -> str:
    if obj is None:
        return "None"
    if isinstance(obj, dict):
        keys = list(obj.keys())[:12]
        return f"dict keys={keys}"
    if isinstance(obj, list):
        return f"list len={len(obj)}"
    return str(type(obj))


def _extract_status_fields(j: Dict[str, Any]) -> str:
    # Useful for your various /health payloads
    status = j.get("status") or j.get("ok") or "unknown"
    mod = j.get("module") or j.get("app") or j.get("service") or ""
    ver = j.get("version") or j.get("route_version") or j.get("app_version") or ""
    parts = [str(status)]
    if mod:
        parts.append(str(mod))
    if ver:
        parts.append(str(ver))
    return " | ".join([p for p in parts if p])


# =============================================================================
# Checks
# =============================================================================


def check_server(base_url: str, token: str, t_short: float) -> bool:
    log("Checking Server Connectivity...", "header")
    h = build_headers(token)

    # GET /
    r, dt, err = _req("GET", f"{base_url}/", headers=h, timeout=t_short)
    if err or r is None:
        log(f"GET / -> Cannot connect: {err}", "fail")
        return False

    if r.status_code == 200:
        log(f"GET / -> Server UP ({fmt_dt(dt)})", "success")
    elif r.status_code in (204,):
        log(f"GET / -> Server UP (204) ({fmt_dt(dt)})", "success")
    else:
        log(f"GET / -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={r.text[:200]!r}", "fail")
        return False

    # HEAD /
    rh, dt2, err2 = _req("HEAD", f"{base_url}/", headers=h, timeout=t_short)
    if err2 or rh is None:
        log(f"HEAD / -> Exception: {err2}", "warn")
    else:
        if rh.status_code in (200, 204):
            log(f"HEAD / -> OK ({fmt_dt(dt2)})", "success")
        else:
            log(f"HEAD / -> HTTP {rh.status_code} ({fmt_dt(dt2)})", "warn")

    # GET /health
    r2, dt3, err3 = _req("GET", f"{base_url}/health", headers=h, timeout=t_short)
    if err3 or r2 is None:
        log(f"GET /health -> Exception: {err3}", "warn")
    else:
        if r2.status_code == 200:
            j = safe_json(r2)
            if isinstance(j, dict):
                log(f"GET /health -> {_extract_status_fields(j)} ({fmt_dt(dt3)})", "success")
            else:
                log(f"GET /health -> 200 but non-JSON ({fmt_dt(dt3)})", "warn")
        else:
            log(f"GET /health -> HTTP {r2.status_code} ({fmt_dt(dt3)})", "warn")

    return True


def check_health_routers(base_url: str, token: str, t_med: float) -> int:
    log("Checking Router Health", "header")
    h = build_headers(token)

    endpoints = [
        "/health",
        "/v1/enriched/health",
        "/v1/analysis/health",
        "/v1/advanced/health",
        "/v1/argaam/health",
    ]

    hard_fail = 0
    for ep in endpoints:
        r, dt, err = _req("GET", f"{base_url}{ep}", headers=h, timeout=t_med)
        if err or r is None:
            log(f"{ep.ljust(24)} -> Exception: {err}", "fail")
            hard_fail += 1
            continue

        if r.status_code == 200:
            j = safe_json(r)
            if isinstance(j, dict):
                engine = j.get("engine_available")
                msg = f"{ep.ljust(24)} -> {_extract_status_fields(j)} ({fmt_dt(dt)})"
                if engine is not None:
                    msg += f" | engine_available={engine}"
                log(msg, "success")
            else:
                log(f"{ep.ljust(24)} -> 200 but non-JSON ({fmt_dt(dt)})", "warn")
        elif r.status_code == 404:
            log(f"{ep.ljust(24)} -> SKIP (404 not mounted)", "warn")
        else:
            log(f"{ep.ljust(24)} -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={r.text[:200]!r}", "fail")
            hard_fail += 1

    return hard_fail


def check_quote(base_url: str, token: str, symbol: str, *, endpoint: str, t_med: float) -> int:
    log(f"Testing Single Quote ({symbol})", "header")
    h = build_headers(token)

    r, dt, err = _req(
        "GET",
        f"{base_url}{endpoint}",
        headers=h,
        timeout=t_med,
        params={"symbol": symbol},
    )
    if err or r is None:
        log(f"{endpoint} -> Exception: {err}", "fail")
        return 1

    if r.status_code == 404:
        log(f"{endpoint} -> SKIP (404 not mounted)", "warn")
        return 0

    if r.status_code != 200:
        log(f"{endpoint} -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={r.text[:240]!r}", "fail")
        return 1

    data = safe_json(r)
    if not isinstance(data, dict):
        log(f"{endpoint} -> Non-JSON response ({fmt_dt(dt)})", "fail")
        print(r.text[:500])
        return 1

    sym_resp = (data.get("symbol") or data.get("ticker") or "").upper()
    price = data.get("current_price") or data.get("last_price") or data.get("price")

    dq = data.get("data_quality")
    src = data.get("data_source") or data.get("source")
    err_msg = data.get("error")

    if sym_resp and price is not None:
        ok_match = sym_resp == symbol.upper() or (symbol.upper().replace(".TADAWUL", "").replace("TADAWUL:", "") in sym_resp)
        if ok_match:
            log(f"OK ({fmt_dt(dt)}) | symbol={sym_resp} price={price} dq={dq} src={src}", "success")
            if err_msg:
                log(f"Note: error field present -> {str(err_msg)[:200]}", "warn")
            return 0

    log(f"Returned 200 but missing expected fields ({fmt_dt(dt)}). Payload below:", "warn")
    print(json_pretty(data))
    return 0


def check_batch_enriched_quotes(base_url: str, token: str, ksa_symbol: str, global_symbol: str, t_long: float) -> int:
    """
    Tests POST /v1/enriched/quotes
    """
    log("Testing Batch /v1/enriched/quotes", "header")
    h = build_headers(token)

    payload = {"symbols": [ksa_symbol, global_symbol], "sheet_name": "Test_Script"}
    r, dt, err = _req(
        "POST",
        f"{base_url}/v1/enriched/quotes",
        headers=h,
        timeout=t_long,
        json_body=payload,
    )
    if err or r is None:
        log(f"/v1/enriched/quotes -> Exception: {err}", "fail")
        return 1

    if r.status_code == 404:
        log("/v1/enriched/quotes -> SKIP (404 not mounted)", "warn")
        return 0

    if r.status_code != 200:
        log(f"/v1/enriched/quotes -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={r.text[:240]!r}", "fail")
        return 1

    data = safe_json(r)
    if not isinstance(data, dict):
        log(f"/v1/enriched/quotes -> 200 but non-JSON ({fmt_dt(dt)})", "warn")
        return 0

    status = data.get("status")
    count = data.get("count")
    rows = data.get("rows") or []
    quotes = data.get("quotes") or []

    # quick diagnostics
    first_err = None
    if isinstance(quotes, list):
        for q in quotes:
            if isinstance(q, dict) and q.get("error"):
                first_err = q.get("error")
                break

    log(
        f"/v1/enriched/quotes -> {status} ({fmt_dt(dt)}) | count={count} rows={len(rows)} quotes={len(quotes)}"
        + (f" | first_error={str(first_err)[:120]!r}" if first_err else ""),
        "success",
    )
    return 0


def check_sheet_rows_enriched(base_url: str, token: str, ksa_symbol: str, global_symbol: str, t_long: float) -> int:
    """
    Tests POST /v1/enriched/sheet-rows (if mounted)
    """
    log("Testing Batch /v1/enriched/sheet-rows (Sheets payload)", "header")
    h = build_headers(token)

    payload = {"tickers": [ksa_symbol, global_symbol], "sheet_name": "Test_Script"}
    r, dt, err = _req(
        "POST",
        f"{base_url}/v1/enriched/sheet-rows",
        headers=h,
        timeout=t_long,
        json_body=payload,
    )
    if err or r is None:
        log(f"/v1/enriched/sheet-rows -> Exception: {err}", "fail")
        return 1

    if r.status_code == 404:
        log("/v1/enriched/sheet-rows -> SKIP (404 not mounted)", "warn")
        return 0

    if r.status_code != 200:
        log(f"/v1/enriched/sheet-rows -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={r.text[:240]!r}", "fail")
        return 1

    data = safe_json(r)
    if not isinstance(data, dict):
        log(f"/v1/enriched/sheet-rows -> Non-JSON response ({fmt_dt(dt)})", "fail")
        return 1

    rows = data.get("rows", []) or []
    headers_ = data.get("headers", []) or []
    st = data.get("status") or "unknown"

    if isinstance(rows, list) and isinstance(headers_, list) and len(headers_) >= 2:
        log(f"Batch OK ({fmt_dt(dt)}) | status={st} rows={len(rows)} headers={len(headers_)}", "success")
        return 0

    log(f"Batch returned unexpected shape ({fmt_dt(dt)}). Payload below:", "warn")
    print(json_pretty(data))
    return 0


def check_argaam_sheet_rows(base_url: str, token: str, ksa_symbol: str, t_long: float) -> int:
    log("Testing KSA /v1/argaam/sheet-rows", "header")
    h = build_headers(token)

    payload = {"symbols": [ksa_symbol, "BADSYM"], "sheet_name": "KSA_Tadawul_Market"}
    r, dt, err = _req(
        "POST",
        f"{base_url}/v1/argaam/sheet-rows",
        headers=h,
        timeout=t_long,
        json_body=payload,
    )
    if err or r is None:
        log(f"/v1/argaam/sheet-rows -> Exception: {err}", "fail")
        return 1

    if r.status_code == 404:
        log("/v1/argaam/sheet-rows -> SKIP (404 not mounted)", "warn")
        return 0

    if r.status_code != 200:
        log(f"/v1/argaam/sheet-rows -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={r.text[:240]!r}", "fail")
        return 1

    data = safe_json(r)
    if isinstance(data, dict) and isinstance(data.get("rows"), list) and isinstance(data.get("headers"), list):
        rows_n = len(data.get("rows") or [])
        heads_n = len(data.get("headers") or [])
        st = data.get("status") or "unknown"
        log(f"OK ({fmt_dt(dt)}) | status={st} rows={rows_n} headers={heads_n}", "success")
        return 0

    log(f"Unexpected response shape ({fmt_dt(dt)}). shape={_shape_hint(data)}", "warn")
    print(json_pretty(data))
    return 0


def check_analysis_quotes(base_url: str, token: str, ksa_symbol: str, global_symbol: str, t_long: float) -> int:
    log("Testing AI /v1/analysis/quotes", "header")
    h = build_headers(token)

    payload = {"tickers": [ksa_symbol, global_symbol], "symbols": []}
    r, dt, err = _req(
        "POST",
        f"{base_url}/v1/analysis/quotes",
        headers=h,
        timeout=t_long,
        json_body=payload,
    )
    if err or r is None:
        log("/v1/analysis/quotes -> Exception: %s" % err, "fail")
        return 1

    if r.status_code == 404:
        log("/v1/analysis/quotes -> SKIP (404 not mounted)", "warn")
        return 0

    if r.status_code != 200:
        log(f"/v1/analysis/quotes -> HTTP {r.status_code} ({fmt_dt(dt)}) | body={r.text[:240]!r}", "fail")
        return 1

    data = safe_json(r)
    if not isinstance(data, dict):
        log(f"/v1/analysis/quotes -> 200 but non-JSON ({fmt_dt(dt)})", "warn")
        return 0

    results = data.get("results") or data.get("items") or data.get("data") or []
    if isinstance(results, list):
        dq_counts: Dict[str, int] = {}
        err_count = 0
        for it in results:
            if isinstance(it, dict):
                dq = str(it.get("data_quality") or "UNKNOWN").upper()
                dq_counts[dq] = dq_counts.get(dq, 0) + 1
                if it.get("error"):
                    err_count += 1
        log(f"/v1/analysis/quotes -> OK ({fmt_dt(dt)}) | items={len(results)} errors={err_count} dq={dq_counts}", "success")
        return 0

    log(f"/v1/analysis/quotes -> Unexpected payload shape ({fmt_dt(dt)}).", "warn")
    print(json_pretty(data))
    return 0


# =============================================================================
# Main
# =============================================================================


def main() -> None:
    ap = argparse.ArgumentParser(description="Smoke test Tadawul Fast Bridge endpoints.")
    ap.add_argument("--base-url", default="", help="Override base URL (else env TFB_BASE_URL/BACKEND_BASE_URL)")
    ap.add_argument("--token", default="", help="Override token (else env TFB_APP_TOKEN/APP_TOKEN/BACKUP_APP_TOKEN)")
    ap.add_argument("--ksa-symbol", default="", help="KSA test symbol (default env TFB_KSA_TEST_SYMBOL or 1120.SR)")
    ap.add_argument("--global-symbol", default="", help="Global test symbol (default env TFB_GLOBAL_TEST_SYMBOL or AAPL)")

    ap.add_argument("--timeout-short", type=float, default=_env_float("TFB_TIMEOUT_SHORT", 15.0))
    ap.add_argument("--timeout-med", type=float, default=_env_float("TFB_TIMEOUT_MED", 25.0))
    ap.add_argument("--timeout-long", type=float, default=_env_float("TFB_TIMEOUT_LONG", 90.0))

    ap.add_argument("--no-analysis", action="store_true", help="Skip /v1/analysis/quotes test")
    ap.add_argument("--no-enriched", action="store_true", help="Skip enriched quote/sheet tests")
    ap.add_argument("--no-argaam", action="store_true", help="Skip argaam tests")

    args = ap.parse_args()

    base_url = (args.base_url.strip() if args.base_url else _env_base_url()).rstrip("/")
    token = args.token.strip() if args.token else _env_token()

    ksa_symbol = (args.ksa_symbol.strip().upper() if args.ksa_symbol else os.getenv("TFB_KSA_TEST_SYMBOL", "1120.SR").strip().upper())
    global_symbol = (args.global_symbol.strip().upper() if args.global_symbol else os.getenv("TFB_GLOBAL_TEST_SYMBOL", "AAPL").strip().upper())

    print(f"{Colors.HEADER}TADAWUL FAST BRIDGE - ENDPOINT TESTER{Colors.ENDC}")
    print(f"BASE_URL={base_url} | TOKEN={'SET' if token else 'NOT SET'}")
    print(f"KSA_TEST_SYMBOL={ksa_symbol} | GLOBAL_TEST_SYMBOL={global_symbol}")

    hard_failures = 0

    if not check_server(base_url, token, args.timeout_short):
        print("\n❌ Cannot proceed. Run your API first (or set TFB_BASE_URL / --base-url).")
        sys.exit(2)

    hard_failures += check_health_routers(base_url, token, args.timeout_med)

    if not args.no_enriched:
        hard_failures += check_quote(base_url, token, ksa_symbol, endpoint="/v1/enriched/quote", t_med=args.timeout_med)
        hard_failures += check_quote(base_url, token, global_symbol, endpoint="/v1/enriched/quote", t_med=args.timeout_med)
        hard_failures += check_batch_enriched_quotes(base_url, token, ksa_symbol, global_symbol, args.timeout_long)
        hard_failures += check_sheet_rows_enriched(base_url, token, ksa_symbol, global_symbol, args.timeout_long)

    if not args.no_argaam:
        hard_failures += check_argaam_sheet_rows(base_url, token, ksa_symbol, args.timeout_long)

    if not args.no_analysis:
        hard_failures += check_analysis_quotes(base_url, token, ksa_symbol, global_symbol, args.timeout_long)

    if hard_failures:
        log(f"Completed with HARD FAILURES = {hard_failures}", "fail")
        sys.exit(2)

    log("All done. No hard failures detected.", "success")
    sys.exit(0)


if __name__ == "__main__":
    main()
```
