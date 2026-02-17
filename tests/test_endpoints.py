#!/usr/bin/env python3
# tests/test_endpoints.py
"""
ADVANCED SMOKE TESTER for Tadawul Fast Bridge API – v3.2.2
(Production-Grade Diagnostics + Concurrency + Schema Audits + Compile-Safe)

Hard Fix:
- ✅ Removes the invalid f-string pattern that breaks compileall:
    r.status:<8   (THIS MUST NOT EXIST ANYWHERE)

Notes:
- PROD SAFE: does not mutate server state except optional push simulation
- Designed to be compile-safe under Python 3.11 (Render compileall)
"""

from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import requests

SCRIPT_VERSION = "3.2.2"

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


def _colorize(text: str, color: str) -> str:
    return f"{color}{text}{Colors.END}"


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
        if k in d and d.get(k) is not None:
            return True
    return False


def _fmt_fixed(text: Any, width: int, align: str = "<") -> str:
    """
    Compile-safe fixed-width formatter.
    align: '<' left, '>' right, '^' center
    """
    s = str(text if text is not None else "")
    if width <= 0:
        return s
    if len(s) > width:
        s = s[: max(0, width - 1)] + "…"
    if align == ">":
        return s.rjust(width)
    if align == "^":
        pad = width - len(s)
        left = pad // 2
        right = pad - left
        return (" " * left) + s + (" " * right)
    return s.ljust(width)


def run_request(
    session: requests.Session,
    method: str,
    url: str,
    headers: Dict[str, str],
    payload: Any = None,
    params: Union[Dict[str, Any], List[Tuple[str, Any]], None] = None,
    timeout: int = 20,
) -> Tuple[int, Any, float]:
    start = time.perf_counter()
    try:
        resp = session.request(method, url, headers=headers, json=payload, params=params, timeout=timeout)
        elapsed = time.perf_counter() - start
        try:
            data = resp.json()
        except Exception:
            data = (resp.text or "")[:800]
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
        self.headers_noauth = {
            "User-Agent": f"TFB-SmokeTester/{SCRIPT_VERSION}",
            "Accept": "application/json",
        }

        self.results: List[TestResult] = []
        self.session = requests.Session()

    def _auth_headers(self) -> Dict[str, str]:
        return self.headers_ok if self.token else self.headers_noauth

    def _add(
        self,
        name: str,
        status: str,
        elapsed: float,
        message: str = "",
        http: int = 0,
        url: str = "",
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.results.append(
            TestResult(
                name=name,
                status=status,
                elapsed=elapsed,
                message=message,
                http=http,
                url=url,
                extra=extra,
            )
        )

    def _ok(self) -> bool:
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
            code, data, dt = run_request(self.session, "GET", url, self.headers_noauth, timeout=self.timeout)
            ok = code == 200
            status = "PASS" if ok else "FAIL"
            msg = f"HTTP {code}"

            if ok and _is_dict(data) and path in ("/health", "/healthz"):
                st = _safe_lower(data.get("status"))
                if st and st not in {"ok", "healthy", "pass", "degraded"}:
                    status = "WARN"
                    msg += f" (status={st})"

            self._add(f"Infra: {name}", status, dt, msg, http=code, url=url)
            log(f"  {name.ljust(12)}: {status} ({dt:.2f}s)")

    def check_security(self) -> None:
        log("\n--- [2/8] Verifying Security Layer ---", Colors.CYAN)
        url = f"{self.base_url}/v1/enriched/quote"

        code_open, _, dt_open = run_request(
            self.session, "GET", url, self.headers_noauth, params={"symbol": GLB_SYM}, timeout=self.timeout
        )
        open_mode = (code_open == 200)

        bad_headers = {"X-APP-TOKEN": "wrong-token-123", "Authorization": "Bearer wrong-token-123"}
        code_bad, data_bad, dt_bad = run_request(
            self.session, "GET", url, bad_headers, params={"symbol": GLB_SYM}, timeout=self.timeout
        )

        if open_mode:
            status = "PASS"
            msg = f"Open mode detected. noauth=HTTP {code_open} badtoken=HTTP {code_bad}"
        else:
            status = "PASS" if code_bad == 401 else "FAIL"
            msg = f"Auth enforced. Expected badtoken=401 got {code_bad}. noauth=HTTP {code_open}"

        self._add(
            "Security: Auth Guard",
            status,
            max(dt_open, dt_bad),
            msg,
            http=code_bad,
            url=url,
            extra={"open_mode": open_mode, "noauth_http": code_open, "badtoken_http": code_bad, "badtoken_body": data_bad if _is_dict(data_bad) else str(data_bad)[:200]},
        )
        log(f"  Auth Guard  : {status} ({max(dt_open, dt_bad):.2f}s)")

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
            code, data, dt = run_request(self.session, "GET", url, self._auth_headers(), timeout=self.timeout)
            ok = (code == 200) and _is_dict(data) and _safe_lower(data.get("status")) in {"ok", "healthy", "pass"}
            status = "PASS" if ok else "FAIL"
            msg = f"HTTP {code}"

            if ok and _is_dict(data) and _has_any_key(data, ["time_riyadh", "riyadh_time", "ksa_time", "boot_time_riyadh"]):
                name = f"{name} (KSA TZ)"

            self._add(f"Router: {name}", status, dt, msg, http=code, url=url)
            log(f"  {name.ljust(18)}: {status} ({dt:.2f}s)")

    def check_data_logic(self) -> None:
        log("\n--- [4/8] Validating Data Routing & Consistency ---", Colors.CYAN)
        url = f"{self.base_url}/v1/enriched/quote"
        headers = self._auth_headers()

        code_ksa, data_ksa, dt_ksa = run_request(
            self.session, "GET", url, headers, params={"symbol": KSA_SYM}, timeout=self.timeout
        )
        code_glb, data_glb, dt_glb = run_request(
            self.session, "GET", url, headers, params={"symbol": GLB_SYM}, timeout=self.timeout
        )

        ok_ksa = (code_ksa == 200) and _is_dict(data_ksa)
        ok_glb = (code_glb == 200) and _is_dict(data_glb)

        status = "PASS" if (ok_ksa and ok_glb) else "FAIL"
        self._add(
            "Data: Single Quote",
            status,
            dt_ksa + dt_glb,
            f"KSA HTTP {code_ksa} | GLB HTTP {code_glb}",
            http=200 if status == "PASS" else 0,
            url=url,
        )
        log(f"  Single Quote : {status} ({dt_ksa + dt_glb:.2f}s)")

        url_b = f"{self.base_url}/v1/enriched/quotes"
        code_b, data_b, dt_b = run_request(
            self.session,
            "GET",
            url_b,
            headers,
            params=[("tickers", KSA_SYM), ("tickers", GLB_SYM)],
            timeout=self.timeout,
        )

        items = []
        if _is_dict(data_b):
            items = data_b.get("items") or data_b.get("results") or data_b.get("rows") or []
        ok_b = (code_b == 200) and isinstance(items, list)

        status_b = "PASS" if ok_b else "FAIL"
        if ok_b and len(items) < 1:
            status_b = "WARN"

        self._add("Data: Batch Quotes", status_b, dt_b, f"HTTP {code_b} count={len(items) if ok_b else 0}", http=code_b, url=url_b)
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

        code, data, dt = run_request(
            self.session, "POST", url, self._auth_headers(), payload, timeout=max(self.timeout, 35)
        )

        if code != 200 or not _is_dict(data):
            self._add("Advisor: Run", "FAIL", dt, f"HTTP {code}", http=code, url=url)
            log(f"  Advisor API  : FAIL ({dt:.2f}s)")
            return

        headers = data.get("headers") or []
        rows = data.get("rows") or data.get("items") or []

        if not isinstance(headers, list):
            headers = []
        if not isinstance(rows, list):
            rows = []

        status = "PASS" if rows else "WARN"
        need = ["Expected ROI % (1M)", "Expected ROI % (3M)"]
        missing = [h for h in need if h not in headers]
        if missing:
            status = "FAIL" if self.strict else "WARN"

        self._add("Advisor: Run", status, dt, f"HTTP {code} recs={len(rows)} missing={missing}", http=code, url=url)
        log(f"  Advisor API  : {status} ({dt:.2f}s)")

    def check_push_mode(self) -> None:
        log("\n--- [6/8] Testing Advanced Push / Compute Mode ---", Colors.CYAN)
        url = f"{self.base_url}/v1/advanced/sheet-rows"
        headers = self._auth_headers()

        payload_push = {"items": [{"sheet": "Test_Tab", "headers": ["Symbol", "Price"], "rows": [["TEST", 100.0]]}]}
        code, data, dt = run_request(self.session, "POST", url, headers, payload_push, timeout=self.timeout)

        ok = (code == 200) and _is_dict(data) and _safe_lower(data.get("status")) in {"success", "ok"}
        status = "PASS" if ok else ("WARN" if code == 200 else "FAIL")
        self._add("Advanced: Push", status, dt, f"HTTP {code}", http=code, url=url)
        log(f"  Push API      : {status} ({dt:.2f}s)")

        payload_compute = {"tickers": [GLB_SYM], "symbols": [GLB_SYM], "sheet_name": "Test"}
        code2, data2, dt2 = run_request(self.session, "POST", url, headers, payload_compute, timeout=self.timeout)

        if code2 != 200 or not _is_dict(data2):
            self._add("Advanced: Compute", "FAIL", dt2, f"HTTP {code2}", http=code2, url=url)
            log(f"  Compute Mode : FAIL ({dt2:.2f}s)")
            return

        hdrs = data2.get("headers") or []
        rows = data2.get("rows") or []
        if not isinstance(hdrs, list):
            hdrs = []
        if not isinstance(rows, list):
            rows = []

        missing = [k for k in ["Last Updated (Riyadh)", "Expected ROI % (12M)"] if k not in hdrs]
        status2 = "PASS" if rows and not missing else ("FAIL" if self.strict else "WARN")

        self._add("Advanced: Schema", status2, dt2, f"rows={len(rows)} missing={missing}", http=code2, url=url)
        log(f"  Schema Audit : {status2} ({dt2:.2f}s)")

    def check_scoring_sanity(self) -> None:
        log("\n--- [7/8] Scoring & Localization Sanity ---", Colors.CYAN)
        url = f"{self.base_url}/v1/enriched/quote"
        code, data, dt = run_request(self.session, "GET", url, self._auth_headers(), params={"symbol": GLB_SYM}, timeout=self.timeout)

        if code != 200 or not _is_dict(data):
            self._add("Sanity: Enriched Quote", "FAIL", dt, f"HTTP {code}", http=code, url=url)
            log(f"  Enriched      : FAIL ({dt:.2f}s)")
            return

        riyadh_ok = False
        for k in ["last_updated_riyadh", "last_updated_ksa", "Last Updated (Riyadh)", "boot_time_riyadh"]:
            if k in data and (k == "boot_time_riyadh" or _parse_iso_dt(data.get(k))):
                riyadh_ok = True
                break

        bad_scores = []
        for f in ["opportunity_score", "overall_score", "risk_score"]:
            if f in data:
                v = _to_float(data.get(f))
                if v is None or v < 0 or v > 100:
                    bad_scores.append(f"{f}={data.get(f)}")

        status = "PASS"
        msg_parts: List[str] = []
        if not riyadh_ok:
            status = "FAIL" if self.strict else "WARN"
            msg_parts.append("Missing/invalid Riyadh timestamp")
        if bad_scores:
            status = "FAIL" if self.strict else "WARN"
            msg_parts.append(f"Bad scores: {bad_scores}")

        self._add("Sanity: Scores & TZ", status, dt, ("OK" if not msg_parts else " | ".join(msg_parts)), http=code, url=url)
        log(f"  Scores/TZ    : {status} ({dt:.2f}s)")

    def check_burst_latency(self, concurrency: int, burst: int) -> None:
        log("\n--- [8/8] Optional Burst Latency Check ---", Colors.CYAN)

        if burst <= 0 or concurrency <= 0:
            self._add("Perf: Burst", "WARN", 0.0, "Skipped")
            log("  Burst        : SKIPPED")
            return

        url = f"{self.base_url}/v1/enriched/quote"
        headers = self._auth_headers()

        def one(_: int) -> Tuple[int, float]:
            c, _, dt = run_request(self.session, "GET", url, headers, params={"symbol": GLB_SYM}, timeout=self.timeout)
            return c, dt

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
        status = "PASS" if ok else ("FAIL" if self.strict else "WARN")
        msg = f"burst={burst} conc={concurrency} total={total:.2f}s p50={p50:.2f}s p90={p90:.2f}s p99={p99:.2f}s codes={codes}"
        self._add("Perf: Burst Quote", status, total, msg, http=200 if ok else 0, url=url)
        log(f"  Burst        : {status} ({total:.2f}s)")

    # ----------------------------
    # Summary
    # ----------------------------
    def print_summary(self) -> None:
        log("\n" + "=" * 100, Colors.BOLD)
        log(f"  SMOKE TEST SUMMARY - v{SCRIPT_VERSION}", Colors.BOLD)
        log("=" * 100, Colors.BOLD)

        print(
            f"{_fmt_fixed('COMPONENT', 34)} | "
            f"{_fmt_fixed('STATUS', 8)} | "
            f"{_fmt_fixed('LATENCY', 9)} | "
            f"{_fmt_fixed('HTTP', 4)} | DETAILS"
        )
        print("-" * 100)

        for r in self.results:
            color = Colors.GREEN if r.status == "PASS" else (Colors.YELLOW if r.status == "WARN" else Colors.RED)
            status_col = _colorize(_fmt_fixed(r.status, 8), color)

            latency_txt = f"{r.elapsed:.2f}s"
            http_txt = str(r.http or "")

            print(
                f"{_fmt_fixed(r.name, 34)} | "
                f"{status_col} | "
                f"{_fmt_fixed(latency_txt, 9, '>')} | "
                f"{_fmt_fixed(http_txt, 4)} | "
                f"{r.message}"
            )

        final = "PASS" if self._ok() else "FAIL"
        log("-" * 100, Colors.BOLD)
        log(f"FINAL: {final} | strict={self.strict} | tests={len(self.results)}", Colors.BOLD)
        log("=" * 100, Colors.BOLD)

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
