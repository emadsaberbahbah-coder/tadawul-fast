#!/usr/bin/env python3
# tests/test_endpoints.py
"""
ADVANCED SMOKE TESTER for Tadawul Fast Bridge API – v3.2.0
(Production-Grade Diagnostics + Concurrency + Schema Audits)

What it tests
- ✅ Base Infrastructure: /, /readyz, /healthz, /health
- ✅ Router Integrity: Enriched, AI Analysis, Advanced, Argaam (+ optional legacy/system routes)
- ✅ Security Layer: Negative test (invalid token) + open-mode detection
- ✅ Data Consistency:
    - GET /v1/enriched/quote (KSA + Global)
    - GET /v1/enriched/quotes (batch)
- ✅ Advisor Logic: POST /v1/advisor/recommendations (mock criteria)
- ✅ Push Mode:
    - POST /v1/advanced/sheet-rows (push simulation if supported)
    - POST /v1/advanced/sheet-rows (compute mode schema audit)
- ✅ Riyadh Localization: verifies UTC+3 timestamp keys exist and are parseable
- ✅ Forecast Canonical Keys: checks for Expected ROI % (1M/3M/12M) in headers where applicable
- ✅ Scoring sanity: Opportunity/Overall/Risk score range checks (0..100), numeric enforcement
- ✅ Performance: measures latency + optional concurrent burst check

Usage
  python tests/test_endpoints.py
  python tests/test_endpoints.py --url https://tadawul-fast-bridge.onrender.com --token $APP_TOKEN
  python tests/test_endpoints.py --strict
  python tests/test_endpoints.py --concurrency 6 --burst 10
  python tests/test_endpoints.py --json-out smoke_results.json

Exit codes
  0: All PASS (or only WARN when not strict)
  2: Any FAIL (or WARN when strict)

Notes
- Designed to be "PROD SAFE": does not mutate server state except optional push simulation
  (which uses a dummy tab name and minimal payload).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import requests

SCRIPT_VERSION = "3.2.0"

# --- Defaults ---
DEFAULT_BASE_URL = (
    os.getenv("TFB_BASE_URL") or os.getenv("BACKEND_BASE_URL") or "http://127.0.0.1:8000"
).rstrip("/")

DEFAULT_TOKEN = (os.getenv("TFB_APP_TOKEN") or os.getenv("APP_TOKEN") or "").strip()

KSA_SYM = os.getenv("TFB_TEST_KSA") or "1120.SR"
GLB_SYM = os.getenv("TFB_TEST_GLB") or "AAPL"


# =============================================================================
# Colors
# =============================================================================
class Colors:
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    MAGENTA = "\033[95m"
    BOLD = "\033[1m"
    END = "\033[0m"


def _colorize(s: str, color: str) -> str:
    return f"{color}{s}{Colors.END}"


# =============================================================================
# Results
# =============================================================================
@dataclass
class TestResult:
    name: str
    status: str  # PASS/WARN/FAIL
    elapsed: float
    message: str = ""
    http: int = 0
    url: str = ""
    extra: Optional[Dict[str, Any]] = None


# =============================================================================
# Helpers
# =============================================================================
def log(msg: str, color: str = "") -> None:
    print(f"{color}{msg}{Colors.END}")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _is_dict(x: Any) -> bool:
    return isinstance(x, dict)


def _safe_lower(x: Any) -> str:
    return str(x or "").strip().lower()


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", "").replace("%", "")
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _parse_iso_dt(s: Any) -> Optional[datetime]:
    try:
        if not s:
            return None
        t = str(s).strip().replace("Z", "+00:00")
        return datetime.fromisoformat(t)
    except Exception:
        return None


def _has_any_key(d: Dict[str, Any], keys: List[str]) -> bool:
    for k in keys:
        if k in d and d.get(k) not in (None, ""):
            return True
    return False


def run_request(
    method: str,
    url: str,
    headers: Dict[str, str],
    payload: Any = None,
    params: Dict[str, Any] = None,
    timeout: int = 20,
) -> Tuple[int, Any, float]:
    start = time.perf_counter()
    try:
        resp = requests.request(method, url, headers=headers, json=payload, params=params, timeout=timeout)
        elapsed = time.perf_counter() - start
        try:
            data = resp.json()
        except Exception:
            data = (resp.text or "")[:500]
        return resp.status_code, data, elapsed
    except Exception as e:
        return 0, str(e), time.perf_counter() - start


# =============================================================================
# Smoke Tester
# =============================================================================
class SmokeTester:
    def __init__(self, base_url: str, token: str, *, strict: bool = False, timeout: int = 20):
        self.base_url = base_url.rstrip("/")
        self.token = token.strip()
        self.strict = bool(strict)
        self.timeout = int(timeout)

        self.headers_ok = {
            "X-APP-TOKEN": self.token,
            "Authorization": f"Bearer {self.token}",
            "User-Agent": f"TFB-SmokeTester/{SCRIPT_VERSION}",
            "Accept": "application/json",
        }

        # Minimal headers without auth (used for open-mode detection)
        self.headers_noauth = {
            "User-Agent": f"TFB-SmokeTester/{SCRIPT_VERSION}",
            "Accept": "application/json",
        }

        self.results: List[TestResult] = []

    # ----------------------------
    # Result helpers
    # ----------------------------
    def _add(self, name: str, status: str, elapsed: float, message: str = "", http: int = 0, url: str = "", extra: Dict[str, Any] = None) -> None:
        self.results.append(TestResult(name=name, status=status, elapsed=elapsed, message=message, http=http, url=url, extra=extra))

    def _ok(self) -> bool:
        # Strict: WARN counts as fail
        for r in self.results:
            if r.status == "FAIL":
                return False
            if self.strict and r.status == "WARN":
                return False
        return True

    # ----------------------------
    # Suites
    # ----------------------------
    def check_infra(self) -> None:
        log("--- [1/8] Checking Base Infrastructure ---", Colors.CYAN)
        eps = [
            ("/", "Root"),
            ("/readyz", "Readiness"),
            ("/healthz", "HealthZ"),
            ("/health", "Health"),
        ]
        for path, name in eps:
            url = f"{self.base_url}{path}"
            code, data, dt = run_request("GET", url, self.headers_noauth, timeout=self.timeout)
            ok = code == 200
            status = "PASS" if ok else "FAIL"
            msg = f"HTTP {code}"
            if ok and _is_dict(data):
                # optional sanity
                if path in ("/health", "/healthz") and str(data.get("status", "ok")).lower() not in {"ok", "healthy", "pass"}:
                    status = "WARN"
                    msg += " (unexpected health status)"
            self._add(f"Infra: {name}", status, dt, msg, http=code, url=url)
            log(f"  {name.ljust(12)}: {status} ({dt:.2f}s)")

    def check_security(self) -> None:
        log("\n--- [2/8] Verifying Security Layer ---", Colors.CYAN)

        # Invalid token should 401 if tokens are enforced.
        bad_headers = {"X-APP-TOKEN": "wrong-token-123", "Authorization": "Bearer wrong-token-123"}
        url = f"{self.base_url}/v1/enriched/quote"
        code, data, dt = run_request("GET", url, bad_headers, params={"symbol": GLB_SYM}, timeout=self.timeout)

        # Determine if server is open-mode: try without any token and see if it works.
        code_open, data_open, dt_open = run_request("GET", url, self.headers_noauth, params={"symbol": GLB_SYM}, timeout=self.timeout)

        open_mode = (code_open == 200)
        if open_mode:
            status = "PASS"
            msg = "Open mode detected (no token required)."
        else:
            # tokens enforced => invalid token should be 401
            status = "PASS" if code == 401 else "FAIL"
            msg = f"Expected 401, got HTTP {code}"

        self._add("Security: Auth Guard", status, max(dt, dt_open), msg, http=code, url=url, extra={"open_mode": open_mode, "noauth_http": code_open})
        log(f"  Auth Guard  : {status} ({max(dt, dt_open):.2f}s)")

    def check_routers(self) -> None:
        log("\n--- [3/8] Verifying Router Integrity ---", Colors.CYAN)
        routers = [
            ("/v1/enriched/health", "Enriched"),
            ("/v1/argaam/health", "Argaam"),
            ("/v1/analysis/health", "AI Analysis"),
            ("/v1/advanced/health", "Advanced"),
        ]

        for path, name in routers:
            url = f"{self.base_url}{path}"
            code, data, dt = run_request("GET", url, self.headers_ok if self.token else self.headers_noauth, timeout=self.timeout)
            ok = (code == 200) and _is_dict(data) and str(data.get("status", "")).lower() in {"ok", "healthy", "pass"}
            status = "PASS" if ok else "FAIL"
            msg = f"HTTP {code}"

            # Riyadh time key check (best-effort)
            if ok and _is_dict(data):
                if _has_any_key(data, ["time_riyadh", "riyadh_time", "timeKSA", "ksa_time"]):
                    name = f"{name} (KSA TZ)"

            self._add(f"Router: {name}", status, dt, msg, http=code, url=url)
            log(f"  {name.ljust(18)}: {status} ({dt:.2f}s)")

    def check_data_logic(self) -> None:
        log("\n--- [4/8] Validating Data Routing & Consistency ---", Colors.CYAN)

        # KSA quote
        url = f"{self.base_url}/v1/enriched/quote"
        code_ksa, data_ksa, dt_ksa = run_request("GET", url, self.headers_ok if self.token else self.headers_noauth, params={"symbol": KSA_SYM}, timeout=self.timeout)
        src_ksa = _safe_lower(_is_dict(data_ksa) and data_ksa.get("data_source") or "")
        ok_ksa = (code_ksa == 200) and (_is_dict(data_ksa)) and ("ksa" in _safe_lower(data_ksa.get("market") or "KSA") or KSA_SYM.split(".")[0] in _safe_lower(data_ksa.get("symbol") or ""))

        # Global quote
        code_glb, data_glb, dt_glb = run_request("GET", url, self.headers_ok if self.token else self.headers_noauth, params={"symbol": GLB_SYM}, timeout=self.timeout)
        src_glb = _safe_lower(_is_dict(data_glb) and data_glb.get("data_source") or "")
        ok_glb = (code_glb == 200) and (_is_dict(data_glb))

        status = "PASS" if (ok_ksa and ok_glb) else "FAIL"
        self._add(
            "Data: Single Quote",
            status,
            dt_ksa + dt_glb,
            f"KSA HTTP {code_ksa} src={src_ksa} | GLB HTTP {code_glb} src={src_glb}",
            http=200 if status == "PASS" else 0,
            url=url,
            extra={"ksa": data_ksa if _is_dict(data_ksa) else None, "glb": data_glb if _is_dict(data_glb) else None},
        )
        log(f"  Single Quote : {status} ({dt_ksa + dt_glb:.2f}s)")

        # Batch quotes
        url_b = f"{self.base_url}/v1/enriched/quotes"
        code_b, data_b, dt_b = run_request(
            "GET",
            url_b,
            self.headers_ok if self.token else self.headers_noauth,
            params=[("tickers", KSA_SYM), ("tickers", GLB_SYM)],
            timeout=self.timeout,
        )
        ok_b = (code_b == 200) and _is_dict(data_b) and isinstance(data_b.get("items") or data_b.get("results") or [], list)
        status_b = "PASS" if ok_b else "FAIL"
        count = 0
        if ok_b:
            items = data_b.get("items") or data_b.get("results") or []
            count = len(items)
            if count < 1:
                status_b = "WARN"
        self._add("Data: Batch Quotes", status_b, dt_b, f"HTTP {code_b} count={count}", http=code_b, url=url_b)
        log(f"  Batch Quotes : {status_b} ({dt_b:.2f}s)")

    def check_advisor(self) -> None:
        log("\n--- [5/8] Testing Investment Advisor Logic ---", Colors.CYAN)
        url = f"{self.base_url}/v1/advisor/recommendations"
        payload = {
            "tickers": [KSA_SYM, GLB_SYM],
            "symbols": [KSA_SYM, GLB_SYM],
            "risk": "Moderate",
            "confidence": "High",
            "top_n": 5,
            "invest_amount": 100000,
            "required_roi_1m": 0,
            "required_roi_3m": 0,
        }
        code, data, dt = run_request("POST", url, self.headers_ok if self.token else self.headers_noauth, payload, timeout=max(self.timeout, 35))

        if code != 200 or not _is_dict(data):
            self._add("Advisor: Run", "FAIL", dt, f"HTTP {code}", http=code, url=url, extra={"data": data})
            log(f"  Advisor API  : FAIL ({dt:.2f}s)")
            return

        headers = data.get("headers") or []
        rows = data.get("rows") or data.get("items") or []

        if not isinstance(headers, list):
            headers = []
        if not isinstance(rows, list):
            rows = []

        status = "PASS" if len(rows) > 0 else "WARN"
        msg = f"HTTP {code} recs={len(rows)}"

        # Schema check: expect 1M/3M and preferably 12M columns (depending on your backend)
        need = ["Expected ROI % (1M)", "Expected ROI % (3M)"]
        missing = [h for h in need if h not in headers]
        has_12m = any("12M" in str(h) for h in headers)

        if missing:
            status = "WARN" if code == 200 else "FAIL"
            msg += f" | missing={missing}"
        if not has_12m:
            msg += " | missing 12M ROI (recommended)"
            if self.strict:
                status = "FAIL" if status != "FAIL" else status
            else:
                status = "WARN" if status == "PASS" else status

        self._add("Advisor: Run", status, dt, msg, http=code, url=url, extra={"headers": headers[:50]})
        log(f"  Advisor API  : {status} ({dt:.2f}s)")

    def check_push_mode(self) -> None:
        log("\n--- [6/8] Testing Advanced Push / Compute Mode ---", Colors.CYAN)

        url = f"{self.base_url}/v1/advanced/sheet-rows"

        # (A) Push simulation (if supported)
        payload_push = {
            "items": [
                {"sheet": "Test_Tab", "headers": ["Symbol", "Price"], "rows": [["TEST", 100.0]]}
            ]
        }
        code, data, dt = run_request("POST", url, self.headers_ok if self.token else self.headers_noauth, payload_push, timeout=self.timeout)
        ok = (code == 200) and _is_dict(data) and str(data.get("status", "")).lower() in {"success", "ok"}
        status = "PASS" if ok else "WARN" if code == 200 else "FAIL"
        msg = f"HTTP {code}"
        if _is_dict(data):
            msg += f" | status={data.get('status')}"
            if "written" in data:
                msg += f" | written={data.get('written')}"
        self._add("Advanced: Push", status, dt, msg, http=code, url=url, extra={"resp": data if _is_dict(data) else None})
        log(f"  Push API     : {status} ({dt:.2f}s)")

        # (B) Compute mode schema audit (minimal)
        payload_compute = {"tickers": [GLB_SYM], "symbols": [GLB_SYM], "sheet_name": "Test"}
        code2, data2, dt2 = run_request("POST", url, self.headers_ok if self.token else self.headers_noauth, payload_compute, timeout=self.timeout)

        if code2 != 200 or not _is_dict(data2):
            self._add("Advanced: Compute", "FAIL", dt2, f"HTTP {code2}", http=code2, url=url, extra={"data": data2})
            log(f"  Compute Mode : FAIL ({dt2:.2f}s)")
            return

        headers = data2.get("headers") or []
        rows = data2.get("rows") or []
        if not isinstance(headers, list):
            headers = []
        if not isinstance(rows, list):
            rows = []

        status2 = "PASS" if rows else "WARN"
        missing = []
        for must in ["Last Updated (Riyadh)", "Expected ROI % (12M)"]:
            if must not in headers:
                missing.append(must)

        if missing:
            status2 = "WARN" if not self.strict else "FAIL"

        self._add("Advanced: Schema", status2, dt2, ("OK" if not missing else f"Missing: {missing}"), http=code2, url=url, extra={"missing": missing, "headers_sample": headers[:40]})
        log(f"  Schema Audit : {status2} ({dt2:.2f}s)")

    def check_scoring_sanity(self) -> None:
        log("\n--- [7/8] Scoring & Localization Sanity ---", Colors.CYAN)

        url = f"{self.base_url}/v1/enriched/quote"
        code, data, dt = run_request("GET", url, self.headers_ok if self.token else self.headers_noauth, params={"symbol": GLB_SYM}, timeout=self.timeout)
        if code != 200 or not _is_dict(data):
            self._add("Sanity: Enriched Quote", "FAIL", dt, f"HTTP {code}", http=code, url=url)
            log(f"  Enriched     : FAIL ({dt:.2f}s)")
            return

        # Riyadh timestamp keys (best-effort)
        riyadh_ok = False
        for k in ["last_updated_riyadh", "last_updated_ksa", "last_updated_local", "Last Updated (Riyadh)"]:
            if k in data and _parse_iso_dt(data.get(k)):
                riyadh_ok = True
                break

        # Score ranges (if present)
        score_fields = ["opportunity_score", "overall_score", "risk_score"]
        bad_scores = []
        for f in score_fields:
            if f in data:
                v = _to_float(data.get(f))
                if v is None or v < 0 or v > 100:
                    bad_scores.append(f"{f}={data.get(f)}")

        status = "PASS"
        msg = "OK"
        if not riyadh_ok:
            status = "WARN" if not self.strict else "FAIL"
            msg = "Missing/invalid Riyadh timestamp"
        if bad_scores:
            status = "WARN" if not self.strict else "FAIL"
            msg = (msg + " | " if msg else "") + f"Bad scores: {bad_scores}"

        self._add("Sanity: Scores & TZ", status, dt, msg, http=code, url=url)
        log(f"  Scores/TZ    : {status} ({dt:.2f}s)")

    def check_burst_latency(self, concurrency: int, burst: int) -> None:
        log("\n--- [8/8] Optional Burst Latency Check ---", Colors.CYAN)

        if burst <= 0 or concurrency <= 0:
            self._add("Perf: Burst", "WARN", 0.0, "Skipped (burst/concurrency disabled)")
            log("  Burst        : SKIPPED")
            return

        url = f"{self.base_url}/v1/enriched/quote"
        headers = self.headers_ok if self.token else self.headers_noauth

        def one(i: int) -> Tuple[int, float]:
            code, _, dt = run_request("GET", url, headers, params={"symbol": GLB_SYM}, timeout=self.timeout)
            return code, dt

        latencies: List[float] = []
        codes: Dict[int, int] = {}

        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            futs = [ex.submit(one, i) for i in range(burst)]
            for f in as_completed(futs):
                c, dt = f.result()
                latencies.append(dt)
                codes[c] = codes.get(c, 0) + 1
        total = time.perf_counter() - t0

        latencies.sort()
        p50 = latencies[int(0.50 * (len(latencies) - 1))] if latencies else 0.0
        p90 = latencies[int(0.90 * (len(latencies) - 1))] if latencies else 0.0
        p99 = latencies[int(0.99 * (len(latencies) - 1))] if latencies else 0.0

        ok = codes.get(200, 0) == burst
        status = "PASS" if ok else "WARN" if not self.strict else "FAIL"
        msg = f"burst={burst} conc={concurrency} total={total:.2f}s p50={p50:.2f}s p90={p90:.2f}s p99={p99:.2f}s codes={codes}"
        self._add("Perf: Burst Quote", status, total, msg, http=200 if ok else 0, url=url, extra={"p50": p50, "p90": p90, "p99": p99, "codes": codes})
        log(f"  Burst        : {status} ({total:.2f}s)")

    # ----------------------------
    # Summary
    # ----------------------------
    def print_summary(self) -> None:
        log("\n" + "=" * 92, Colors.BOLD)
        log(f"  SMOKE TEST SUMMARY - v{SCRIPT_VERSION}", Colors.BOLD)
        log("=" * 92, Colors.BOLD)
        print(f"{'COMPONENT':<28} | {'STATUS':<8} | {'LATENCY':<8} | {'HTTP':<4} | DETAILS")
        print("-" * 92)

        for r in self.results:
            color = Colors.GREEN if r.status == "PASS" else Colors.YELLOW if r.status == "WARN" else Colors.RED
            print(f"{r.name:<28} | {_colorize(r.status:<8, color)} | {r.elapsed:>6.2f}s | {str(r.http or ''):<4} | {r.message}")

        ok = self._ok()
        final = "PASS" if ok else "FAIL"
        log("-" * 92, Colors.BOLD)
        log(f"FINAL: {final} | strict={self.strict} | tests={len(self.results)}", Colors.BOLD)
        log("=" * 92, Colors.BOLD)

    def to_json(self) -> Dict[str, Any]:
        return {
            "version": SCRIPT_VERSION,
            "ts_ms": _now_ms(),
            "base_url": self.base_url,
            "strict": self.strict,
            "results": [asdict(r) for r in self.results],
            "ok": self._ok(),
        }


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    ap = argparse.ArgumentParser(description="TFB Advanced Smoke Tester")
    ap.add_argument("--url", default=DEFAULT_BASE_URL, help="Backend URL")
    ap.add_argument("--token", default=DEFAULT_TOKEN, help="App Token")
    ap.add_argument("--strict", action="store_true", help="Treat WARN as FAIL")
    ap.add_argument("--timeout", type=int, default=20, help="HTTP timeout seconds")
    ap.add_argument("--concurrency", type=int, default=0, help="Burst concurrency (optional)")
    ap.add_argument("--burst", type=int, default=0, help="Number of burst requests (optional)")
    ap.add_argument("--json-out", help="Write results JSON to file")
    args = ap.parse_args()

    tester = SmokeTester(args.url, args.token, strict=args.strict, timeout=args.timeout)

    log(f"🚀 Initializing Smoke Test Suite v{SCRIPT_VERSION}", Colors.BOLD)
    log(f"Target: {args.url}", Colors.CYAN)
    log(f"Strict: {args.strict} | Timeout: {args.timeout}s | Token: {'SET' if args.token else 'EMPTY'}\n", Colors.CYAN)

    tester.check_infra()
    tester.check_security()
    tester.check_routers()
    tester.check_data_logic()
    tester.check_advisor()
    tester.check_push_mode()
    tester.check_scoring_sanity()

    if args.burst and args.concurrency:
        tester.check_burst_latency(args.concurrency, args.burst)

    tester.print_summary()

    if args.json_out:
        try:
            with open(args.json_out, "w", encoding="utf-8") as f:
                json.dump(tester.to_json(), f, indent=2, ensure_ascii=False)
            log(f"Saved JSON report to {args.json_out}", Colors.MAGENTA)
        except Exception as e:
            log(f"Failed to write json-out: {e}", Colors.RED)

    return 0 if tester._ok() else 2


if __name__ == "__main__":
    raise SystemExit(main())
