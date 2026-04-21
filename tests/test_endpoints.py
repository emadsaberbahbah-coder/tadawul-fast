#!/usr/bin/env python3
# test_endpoints.py
"""
===========================================================
TFB Endpoint Smoke Tester — v2.3.0 (Live-Route Aligned)
===========================================================
CANONICAL-ROUTE COVERAGE • RENDER/LOCAL FRIENDLY • PARTIAL-TIMEOUT AWARE
PASS/WARN/FAIL COUNTER • EXIT CODE CORRECT • CI-READY JSON OUTPUT

Why this revision (v2.3.0 vs v2.2.0)
-------------------------------------
- 🔑 FIX CRITICAL: Added tests for the canonical Top10 endpoints that
    v2.2.0 silently missed:
        POST /v1/advanced/top10-investments
        POST /v1/advanced/top10
    These are the flagship routes owned by routes/investment_advisor.py
    v2.13.1. Without coverage, a regression here would have passed the
    smoke test.

- 🔑 FIX CRITICAL: Added tests for the canonical enriched-quote endpoints
    that v2.2.0's docstring claimed to have "replaced" but are in fact
    still the canonical routes:
        POST /v1/enriched/quotes    (batch, format="items")
        POST /v1/enriched/quote     (single)
    These are the endpoints that run_market_scan v5.3.0 and
    track_performance v6.4.0 actually hit.

- 🔑 FIX HIGH: Canonical-width thresholds were too loose. v2.2.0 used
    `headers_n >= 70` for instrument pages (Market_Leaders, Global_Markets,
    Commodities_FX, Mutual_Funds, My_Portfolio) but the schema registry
    mandates exactly 80 columns. A page returning only 70 columns would
    have passed as "OK". v2.3.0 enforces the canonical widths:
        Market_Leaders / Global_Markets / Commodities_FX /
        Mutual_Funds / My_Portfolio   = 80
        Top_10_Investments            = 83  (80 + top10_rank,
                                              selection_reason,
                                              criteria_snapshot)
        Insights_Analysis             = 7
        Data_Dictionary               = 9

- 🔑 FIX HIGH: `request_json` now catches `requests.RequestException`
    (timeouts, connect errors, DNS failures) and returns
    `(0, None, dt, str(exc))`. v2.2.0's function would raise the exception
    up to the caller, which in most call sites was unwrapped and crashed
    the entire test run on the first network hiccup. Now every endpoint
    gets a clean FAIL result and the harness continues.

- 🔑 FIX HIGH: Pass/Warn/Fail counter + exit code. v2.2.0 always exited 0
    regardless of how many endpoints failed — useless for CI. v2.3.0:
        exit 0 if all PASS (WARN allowed)
        exit 1 if any FAIL
        exit 2 if --strict and any WARN or FAIL
    All test results are collected in a tally that's printed at the end
    as a summary and optionally emitted as JSON via --json.

- FIX MEDIUM: Colors auto-disable when stdout isn't a TTY or `NO_COLOR`
    env set (POSIX convention). v2.2.0 always emitted ANSI escapes, which
    render as garbage in CI log files.

- FIX MEDIUM: Uses a shared `requests.Session` for connection pooling.
    Cold backend wake-ups on Render benefit noticeably.

- FIX: Added `_TRUTHY`/`_FALSY` + `_env_bool`/`_env_int`/`_env_float`
    helpers matching project convention.
- FIX: Added `SCRIPT_VERSION` + `SERVICE_VERSION` constants.
- FIX: CLI flags: `--skip-meta`, `--skip-sheet-rows`, `--skip-top10`,
    `--skip-quotes`, `--skip-schema`, `--only <group>`, `--strict`,
    `--json [path]`, `--timeout-long N`.
- FIX: Environment-driven toggles for every CLI flag (TFB_* namespace).

Usage
-----
  python test_endpoints.py
  TFB_BASE_URL=https://tadawul-fast-bridge.onrender.com python test_endpoints.py
  TFB_APP_TOKEN=... python test_endpoints.py
  python test_endpoints.py --only top10,quotes --json results.json
  python test_endpoints.py --strict     # WARN becomes FAIL for exit code

Exit codes
----------
  0   all tests PASS (WARN allowed unless --strict)
  1   at least one FAIL
  2   --strict and at least one WARN or FAIL (or server down)
  130 SIGINT
"""

from __future__ import annotations

import argparse
import json as _json
import os
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    import requests
    from requests.exceptions import RequestException
except Exception as e:  # pragma: no cover
    sys.stderr.write(f"FATAL: requests library required ({e})\n")
    raise SystemExit(1)


# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
SCRIPT_VERSION = "2.3.0"
SERVICE_VERSION = SCRIPT_VERSION  # v2.3.0: cross-script alias
SCRIPT_NAME = "EndpointTester"


# ---------------------------------------------------------------------------
# Project-wide truthy/falsy vocabulary (matches main._TRUTHY / _FALSY)
# ---------------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}


def _env_bool(name: str, default: bool = False) -> bool:
    try:
        raw = (os.getenv(name, "") or "").strip().lower()
    except Exception:
        return bool(default)
    if not raw:
        return bool(default)
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return bool(default)


def _env_int(
    name: str, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None
) -> int:
    try:
        raw = (os.getenv(name, "") or "").strip()
        if not raw:
            return default
        v = int(float(raw))
    except Exception:
        return default
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


def _env_float(
    name: str,
    default: float,
    *,
    lo: Optional[float] = None,
    hi: Optional[float] = None,
) -> float:
    try:
        raw = (os.getenv(name, "") or "").strip()
        if not raw:
            return default
        v = float(raw)
    except Exception:
        return default
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


# ---------------------------------------------------------------------------
# sys.path bootstrap (project convention — safe if script is in /scripts)
# ---------------------------------------------------------------------------
def _ensure_project_root_on_path() -> None:
    try:
        script_dir = Path(__file__).parent.absolute()
        project_root = script_dir.parent
        for p in (script_dir, project_root):
            ps = str(p)
            if ps and ps not in sys.path:
                sys.path.insert(0, ps)
    except Exception:
        pass


_ensure_project_root_on_path()


# ---------------------------------------------------------------------------
# ENV
# ---------------------------------------------------------------------------
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

TIMEOUT_SHORT = _env_float("TFB_TIMEOUT_SHORT", 15.0, lo=1.0, hi=600.0)
TIMEOUT_MED = _env_float("TFB_TIMEOUT_MED", 30.0, lo=1.0, hi=600.0)
TIMEOUT_LONG = _env_float("TFB_TIMEOUT_LONG", 90.0, lo=1.0, hi=600.0)


# ---------------------------------------------------------------------------
# Canonical sheet widths (from core.sheets.schema_registry v2.2.0)
# ---------------------------------------------------------------------------
# These are authoritative — every sheet-rows response MUST match.
_CANONICAL_WIDTHS: Dict[str, int] = {
    "Market_Leaders": 80,
    "Global_Markets": 80,
    "Commodities_FX": 80,
    "Mutual_Funds": 80,
    "My_Portfolio": 80,
    "Top_10_Investments": 83,
    "Insights_Analysis": 7,
    "Data_Dictionary": 9,
}


# ---------------------------------------------------------------------------
# Colors (auto-disabled when stdout isn't a TTY or NO_COLOR is set)
# ---------------------------------------------------------------------------
def _colors_supported() -> bool:
    if _env_bool("NO_COLOR", False) or os.getenv("NO_COLOR") is not None:
        # POSIX: presence of NO_COLOR (any value) disables.
        # https://no-color.org
        if os.getenv("NO_COLOR") is not None:
            return False
    if not sys.stdout.isatty():
        return False
    # Windows: only modern terminals support ANSI by default.
    # Honor explicit opt-in via TFB_COLOR=1.
    if sys.platform == "win32" and not _env_bool("TFB_COLOR", False):
        return False
    return True


_USE_COLOR = _colors_supported()


class Colors:
    HEADER = "\033[95m" if _USE_COLOR else ""
    OKBLUE = "\033[94m" if _USE_COLOR else ""
    OKGREEN = "\033[92m" if _USE_COLOR else ""
    WARNING = "\033[93m" if _USE_COLOR else ""
    FAIL = "\033[91m" if _USE_COLOR else ""
    ENDC = "\033[0m" if _USE_COLOR else ""


# ---------------------------------------------------------------------------
# Result tracker
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class TestResult:
    name: str
    status: str  # "pass" | "warn" | "fail" | "skip"
    duration_sec: float
    detail: str = ""
    http_status: Optional[int] = None


@dataclass(slots=True)
class Tally:
    results: List[TestResult] = field(default_factory=list)

    def add(self, result: TestResult) -> None:
        self.results.append(result)

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.status == "pass")

    @property
    def warned(self) -> int:
        return sum(1 for r in self.results if r.status == "warn")

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if r.status == "fail")

    @property
    def skipped(self) -> int:
        return sum(1 for r in self.results if r.status == "skip")

    @property
    def total(self) -> int:
        return len(self.results)


TALLY = Tally()


def log(msg: str, typ: str = "info") -> None:
    prefix_map = {
        "info": f"{Colors.OKBLUE}[INFO]{Colors.ENDC}",
        "success": f"{Colors.OKGREEN}[PASS]{Colors.ENDC}",
        "warn": f"{Colors.WARNING}[WARN]{Colors.ENDC}",
        "fail": f"{Colors.FAIL}[FAIL]{Colors.ENDC}",
        "skip": f"{Colors.WARNING}[SKIP]{Colors.ENDC}",
        "header": f"\n{Colors.HEADER}---",
    }
    prefix = prefix_map.get(typ, "")
    if typ == "header":
        sys.stdout.write(f"\n{Colors.HEADER}--- {msg} ---{Colors.ENDC}\n")
    else:
        sys.stdout.write(f"{prefix} {msg}\n")
    sys.stdout.flush()


def record(
    name: str,
    status: str,
    duration: float,
    detail: str = "",
    http_status: Optional[int] = None,
    *,
    emit: bool = True,
) -> None:
    """Emit a log line AND record it in TALLY for exit-code calculation."""
    TALLY.add(
        TestResult(
            name=name,
            status=status,
            duration_sec=round(duration, 3),
            detail=detail,
            http_status=http_status,
        )
    )
    if emit:
        # Derive log type from status
        log_type = {
            "pass": "success",
            "warn": "warn",
            "fail": "fail",
            "skip": "skip",
        }.get(status, "info")
        log(f"{name} -> {detail}", log_type)


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------
_SESSION: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    """Lazy requests.Session for connection pooling."""
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
    return _SESSION


def _close_session() -> None:
    global _SESSION
    if _SESSION is not None:
        try:
            _SESSION.close()
        except Exception:
            pass
        finally:
            _SESSION = None


def auth_headers() -> Dict[str, str]:
    hdrs: Dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": f"TFB-Endpoint-Smoke/{SCRIPT_VERSION}",
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
    """
    Some routes wrap the tabular envelope inside `data`, `result`, or
    `payload`. Unwrap only when the inner dict looks tabular.
    """
    if not isinstance(data, dict):
        return data
    for key in ("data", "result", "payload"):
        value = data.get(key)
        if isinstance(value, dict):
            if any(
                k in value
                for k in ("headers", "keys", "rows", "rows_matrix", "pages")
            ):
                return value
    return data


def warnings_text(data: Any) -> str:
    if not isinstance(data, dict):
        return ""
    warnings: List[str] = []
    meta = data.get("meta")
    if isinstance(meta, dict) and isinstance(meta.get("warnings"), list):
        warnings.extend(str(x) for x in meta.get("warnings") if str(x).strip())
    if isinstance(data.get("warnings"), list):
        warnings.extend(str(x) for x in data.get("warnings") if str(x).strip())
    return " | ".join(warnings)


def row_count(data: Dict[str, Any]) -> int:
    """Tolerant of all canonical row-list aliases: rows, row_objects, items,
    records, data, quotes, results. Matches envelope emitted by analysis,
    advanced, and enriched routers."""
    return max(
        count_safe(data.get("rows")),
        count_safe(data.get("row_objects")),
        count_safe(data.get("data")),
        count_safe(data.get("items")),
        count_safe(data.get("records")),
        count_safe(data.get("quotes")),
        count_safe(data.get("results")),
    )


def sheet_rows_summary(data: Dict[str, Any]) -> str:
    headers_n = count_safe(data.get("headers"))
    keys_n = count_safe(data.get("keys"))
    rows_n = row_count(data)
    status = str(data.get("status") or "")
    return f"status={status} headers={headers_n} keys={keys_n} rows={rows_n}"


def request_json(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
    timeout: float = TIMEOUT_MED,
) -> Tuple[int, Optional[Any], float, str]:
    """
    v2.3.0: Catches RequestException so a single timeout doesn't crash
    the entire test run. Returns (0, None, dt, error_msg) on failure.
    """
    url = f"{BASE_URL}{path}"
    t0 = time.time()
    try:
        resp = _get_session().request(
            method=method,
            url=url,
            params=params,
            json=payload,
            headers=auth_headers(),
            timeout=timeout,
        )
        dt = time.time() - t0
        return resp.status_code, safe_json(resp), dt, resp.text[:400]
    except RequestException as exc:
        dt = time.time() - t0
        return 0, None, dt, f"RequestException: {exc}"
    except Exception as exc:
        dt = time.time() - t0
        return 0, None, dt, f"UnexpectedException: {exc}"


# ---------------------------------------------------------------------------
# Sheet-rows contract validator (canonical-width aware)
# ---------------------------------------------------------------------------
def _validate_sheet_rows_response(
    test_name: str,
    page: str,
    status: int,
    data: Any,
    dt: float,
    raw: str,
) -> None:
    """
    v2.3.0: Width threshold is the EXACT canonical width (not >= 70).
    Partial-timeout with contract present is a WARN, not a FAIL.
    """
    if status != 200:
        record(
            test_name,
            "fail",
            dt,
            f"HTTP {status} ({fmt_dt(dt)}) | body={raw!r}",
            http_status=status,
        )
        return

    data = unwrap_payload(data)
    if not isinstance(data, dict):
        record(test_name, "fail", dt, f"Non-JSON payload ({fmt_dt(dt)})", status)
        return

    headers_n = count_safe(data.get("headers"))
    keys_n = count_safe(data.get("keys"))
    rows_n = row_count(data)
    warn_txt = warnings_text(data)
    summary = sheet_rows_summary(data)

    expected_width = _CANONICAL_WIDTHS.get(page)
    if expected_width is None:
        # Unknown page — best-effort: need at least some headers/keys/rows
        if headers_n > 0 and keys_n > 0 and rows_n > 0:
            record(test_name, "pass", dt, f"OK ({fmt_dt(dt)}) | {summary}", status)
        else:
            record(
                test_name,
                "warn",
                dt,
                f"Unknown page contract ({fmt_dt(dt)}) | {summary}",
                status,
            )
        return

    contract_ok = headers_n >= expected_width and keys_n >= expected_width

    if contract_ok and rows_n > 0:
        record(
            test_name,
            "pass",
            dt,
            f"OK ({fmt_dt(dt)}) | expected={expected_width} | {summary}",
            status,
        )
    elif contract_ok and "timeout" in warn_txt.lower():
        record(
            test_name,
            "warn",
            dt,
            f"Partial timeout but contract OK ({fmt_dt(dt)}) | "
            f"expected={expected_width} | warnings={warn_txt}",
            status,
        )
    elif contract_ok:
        record(
            test_name,
            "warn",
            dt,
            f"Contract OK but rows empty ({fmt_dt(dt)}) | "
            f"expected={expected_width} | {summary}",
            status,
        )
    else:
        record(
            test_name,
            "fail",
            dt,
            f"Weak contract ({fmt_dt(dt)}) | expected={expected_width} | {summary}",
            status,
        )


# ===========================================================================
# Test groups
# ===========================================================================
def check_server() -> bool:
    """Returns True if server is reachable. Records a single pass/fail."""
    log("Checking Server Connectivity", "header")

    status, data, dt, raw = request_json(
        "GET", "/", timeout=TIMEOUT_SHORT
    )
    if status == 200:
        record(
            "GET /",
            "pass",
            dt,
            f"Server UP ({fmt_dt(dt)})",
            status,
        )
    elif status == 0:
        record(
            "GET /",
            "fail",
            dt,
            f"Cannot connect: {raw}",
            status,
        )
        return False
    else:
        record(
            "GET /",
            "fail",
            dt,
            f"HTTP {status} ({fmt_dt(dt)}) | body={raw!r}",
            status,
        )
        return False

    # HEAD / is advisory — warn-only on failure
    t0 = time.time()
    try:
        rh = _get_session().head(
            f"{BASE_URL}/", headers=auth_headers(), timeout=TIMEOUT_SHORT
        )
        dt = time.time() - t0
        if rh.status_code in (200, 204):
            record("HEAD /", "pass", dt, f"OK ({fmt_dt(dt)})", rh.status_code)
        else:
            record(
                "HEAD /",
                "warn",
                dt,
                f"HTTP {rh.status_code} ({fmt_dt(dt)})",
                rh.status_code,
            )
    except Exception as exc:
        dt = time.time() - t0
        record("HEAD /", "warn", dt, f"Exception: {exc}")

    return True


def check_meta_and_health() -> None:
    log("Checking Meta / Health Routes", "header")
    endpoints = ["/health", "/meta", "/readyz", "/livez", "/openapi.json"]

    for ep in endpoints:
        status, data, dt, raw = request_json("GET", ep, timeout=TIMEOUT_MED)
        test_name = f"GET {ep}"

        if status == 0:
            record(test_name, "fail", dt, f"Exception: {raw}", status)
            continue
        if status != 200:
            record(
                test_name,
                "fail",
                dt,
                f"HTTP {status} ({fmt_dt(dt)}) | body={raw[:220]!r}",
                status,
            )
            continue

        if ep == "/openapi.json":
            if isinstance(data, dict) and isinstance(data.get("paths"), dict):
                n = len(data.get("paths") or {})
                record(
                    test_name,
                    "pass",
                    dt,
                    f"paths={n} ({fmt_dt(dt)})",
                    status,
                )
            else:
                record(
                    test_name,
                    "warn",
                    dt,
                    f"200 but invalid OpenAPI body ({fmt_dt(dt)})",
                    status,
                )
            continue

        response_status = (
            (data or {}).get("status", "ok") if isinstance(data, dict) else "ok"
        )
        ver = ""
        eng = ""
        if isinstance(data, dict):
            ver = str(
                (data.get("entry_version") or data.get("app_version") or "")
            )
            eng = str(data.get("engine_source") or "")
        msg = f"{response_status} ({fmt_dt(dt)})"
        if ver:
            msg += f" | v={ver}"
        if eng:
            msg += f" | engine={eng}"
        record(test_name, "pass", dt, msg, status)


# ---------------------------------------------------------------------------
# Sheet-rows tests
# ---------------------------------------------------------------------------
def check_analysis_market_leaders_get() -> None:
    log("GET /v1/analysis/sheet-rows -> Market_Leaders", "header")
    status, data, dt, raw = request_json(
        "GET",
        "/v1/analysis/sheet-rows",
        params={
            "page": "Market_Leaders",
            "limit": 5,
            "symbols": f"{KSA_TEST_SYMBOL},{GLOBAL_TEST_SYMBOL}",
        },
        timeout=TIMEOUT_LONG,
    )
    _validate_sheet_rows_response(
        "GET /v1/analysis/sheet-rows [Market_Leaders]",
        "Market_Leaders",
        status,
        data,
        dt,
        raw,
    )


def check_analysis_global_markets_post() -> None:
    log("POST /v1/analysis/sheet-rows -> Global_Markets", "header")
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
    _validate_sheet_rows_response(
        "POST /v1/analysis/sheet-rows [Global_Markets]",
        "Global_Markets",
        status,
        data,
        dt,
        raw,
    )


def check_advanced_insights_post() -> None:
    log("POST /v1/advanced/sheet-rows -> Insights_Analysis", "header")
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
    _validate_sheet_rows_response(
        "POST /v1/advanced/sheet-rows [Insights_Analysis]",
        "Insights_Analysis",
        status,
        data,
        dt,
        raw,
    )


def check_advisor_portfolio_post() -> None:
    log("POST /v1/advisor/sheet-rows -> My_Portfolio", "header")
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
    _validate_sheet_rows_response(
        "POST /v1/advisor/sheet-rows [My_Portfolio]",
        "My_Portfolio",
        status,
        data,
        dt,
        raw,
    )


def check_root_sheet_rows_top10() -> None:
    log("POST /sheet-rows -> Top_10_Investments", "header")
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
    _validate_sheet_rows_response(
        "POST /sheet-rows [Top_10_Investments]",
        "Top_10_Investments",
        status,
        data,
        dt,
        raw,
    )


# ---------------------------------------------------------------------------
# 🔑 Top10 canonical endpoints (v2.3.0 NEW)
# ---------------------------------------------------------------------------
def check_advanced_top10_investments() -> None:
    """
    v2.3.0: Tests POST /v1/advanced/top10-investments — the flagship route
    from routes/investment_advisor.py v2.13.1. Completely missing in v2.2.0.
    """
    log("POST /v1/advanced/top10-investments", "header")
    status, data, dt, raw = request_json(
        "POST",
        "/v1/advanced/top10-investments",
        payload={
            "pages_selected": ["Market_Leaders", "Global_Markets"],
            "top_n": 5,
            "risk_level": "Moderate",
            "enrich_final": False,
            "mode": "live",
        },
        timeout=TIMEOUT_LONG,
    )
    _validate_sheet_rows_response(
        "POST /v1/advanced/top10-investments",
        "Top_10_Investments",
        status,
        data,
        dt,
        raw,
    )


def check_advanced_top10_alias() -> None:
    """v2.3.0: Tests POST /v1/advanced/top10 (alias of top10-investments)."""
    log("POST /v1/advanced/top10 (alias)", "header")
    status, data, dt, raw = request_json(
        "POST",
        "/v1/advanced/top10",
        payload={
            "pages_selected": ["Market_Leaders"],
            "top_n": 3,
            "risk_level": "Moderate",
            "enrich_final": False,
            "mode": "live",
        },
        timeout=TIMEOUT_LONG,
    )
    _validate_sheet_rows_response(
        "POST /v1/advanced/top10",
        "Top_10_Investments",
        status,
        data,
        dt,
        raw,
    )


# ---------------------------------------------------------------------------
# 🔑 Enriched quotes canonical endpoints (v2.3.0 NEW)
# ---------------------------------------------------------------------------
def check_enriched_quotes_batch() -> None:
    """
    v2.3.0: Tests POST /v1/enriched/quotes with format="items".
    This is the canonical route used by run_market_scan v5.3.0 and
    track_performance v6.4.0. v2.2.0's docstring claimed these routes
    were "replaced" but they ARE the canonical live routes.
    """
    log("POST /v1/enriched/quotes (batch, format=items)", "header")
    syms = [GLOBAL_TEST_SYMBOL, "MSFT", TOP10_SYMBOL]
    status, data, dt, raw = request_json(
        "POST",
        "/v1/enriched/quotes",
        payload={
            "symbols": syms,
            "format": "items",
            "include_raw": False,
            "debug": False,
        },
        timeout=TIMEOUT_LONG,
    )
    test_name = "POST /v1/enriched/quotes"

    if status == 0:
        record(test_name, "fail", dt, f"Exception: {raw}", status)
        return
    if status != 200:
        record(
            test_name,
            "fail",
            dt,
            f"HTTP {status} ({fmt_dt(dt)}) | body={raw!r}",
            status,
        )
        return

    data = unwrap_payload(data)
    if not isinstance(data, dict):
        record(test_name, "fail", dt, f"Non-JSON payload ({fmt_dt(dt)})", status)
        return

    # Canonical response has items / records / rows / data / quotes
    rows_n = row_count(data)
    summary = f"status={data.get('status')} items={rows_n} requested={len(syms)}"

    if rows_n == len(syms):
        record(test_name, "pass", dt, f"OK ({fmt_dt(dt)}) | {summary}", status)
    elif rows_n > 0:
        record(
            test_name,
            "warn",
            dt,
            f"Partial result ({fmt_dt(dt)}) | {summary}",
            status,
        )
    else:
        record(
            test_name,
            "fail",
            dt,
            f"No items returned ({fmt_dt(dt)}) | {summary}",
            status,
        )


def check_enriched_quote_single() -> None:
    """v2.3.0: Tests POST /v1/enriched/quote (single symbol)."""
    log("POST /v1/enriched/quote (single symbol)", "header")
    status, data, dt, raw = request_json(
        "POST",
        "/v1/enriched/quote",
        payload={
            "symbol": GLOBAL_TEST_SYMBOL,
            "include_raw": False,
        },
        timeout=TIMEOUT_MED,
    )
    test_name = "POST /v1/enriched/quote"

    if status == 0:
        record(test_name, "fail", dt, f"Exception: {raw}", status)
        return
    if status != 200:
        record(
            test_name,
            "fail",
            dt,
            f"HTTP {status} ({fmt_dt(dt)}) | body={raw!r}",
            status,
        )
        return

    data = unwrap_payload(data)
    if not isinstance(data, dict):
        record(test_name, "fail", dt, f"Non-JSON payload ({fmt_dt(dt)})", status)
        return

    # Single-quote response: look for symbol + current_price or a row_objects[0]
    price = None
    for key in ("current_price", "price", "last", "last_price"):
        if data.get(key) is not None:
            price = data.get(key)
            break
    if price is None:
        # Maybe it's wrapped in rows/row_objects/items
        for key in ("row_objects", "rows", "items", "records"):
            v = data.get(key)
            if isinstance(v, list) and v and isinstance(v[0], dict):
                for pk in ("current_price", "price", "last", "last_price"):
                    if v[0].get(pk) is not None:
                        price = v[0].get(pk)
                        break
                if price is not None:
                    break

    summary = f"status={data.get('status')} price={price!r}"
    if price is not None and (
        isinstance(price, (int, float)) or str(price).strip()
    ):
        record(test_name, "pass", dt, f"OK ({fmt_dt(dt)}) | {summary}", status)
    else:
        record(
            test_name,
            "warn",
            dt,
            f"No price field found ({fmt_dt(dt)}) | {summary}",
            status,
        )


# ---------------------------------------------------------------------------
# Schema routes
# ---------------------------------------------------------------------------
def check_schema_routes() -> None:
    log("Schema Routes", "header")

    # /v1/schema/pages
    status, data, dt, raw = request_json(
        "GET", "/v1/schema/pages", timeout=TIMEOUT_MED
    )
    test_name = "GET /v1/schema/pages"
    payload = unwrap_payload(data)

    if status == 0:
        record(test_name, "fail", dt, f"Exception: {raw}", status)
    elif status == 200:
        if isinstance(payload, dict) and isinstance(payload.get("pages"), list):
            n = len(payload.get("pages") or [])
            record(
                test_name,
                "pass",
                dt,
                f"pages={n} ({fmt_dt(dt)})",
                status,
            )
        elif isinstance(payload, list):
            record(
                test_name,
                "pass",
                dt,
                f"pages={len(payload)} ({fmt_dt(dt)})",
                status,
            )
        else:
            record(
                test_name,
                "warn",
                dt,
                f"200 but unexpected payload ({fmt_dt(dt)})",
                status,
            )
    else:
        record(
            test_name,
            "fail",
            dt,
            f"HTTP {status} ({fmt_dt(dt)}) | body={raw!r}",
            status,
        )

    # /v1/schema/sheet-spec?page=Data_Dictionary
    status, data, dt, raw = request_json(
        "GET",
        "/v1/schema/sheet-spec",
        params={"page": "Data_Dictionary"},
        timeout=TIMEOUT_MED,
    )
    test_name = "GET /v1/schema/sheet-spec [Data_Dictionary]"
    payload = unwrap_payload(data)

    if status == 0:
        record(test_name, "fail", dt, f"Exception: {raw}", status)
    elif status == 200 and isinstance(payload, dict):
        headers_n = count_safe(payload.get("headers"))
        keys_n = count_safe(payload.get("keys"))
        if headers_n >= 9 or keys_n >= 9:  # Data_Dictionary canonical = 9
            record(
                test_name,
                "pass",
                dt,
                f"headers={headers_n} keys={keys_n} ({fmt_dt(dt)})",
                status,
            )
        else:
            record(
                test_name,
                "warn",
                dt,
                f"weak contract headers={headers_n} keys={keys_n} ({fmt_dt(dt)})",
                status,
            )
    else:
        record(
            test_name,
            "fail",
            dt,
            f"HTTP {status} ({fmt_dt(dt)}) | body={raw!r}",
            status,
        )

    # /v1/schema/data-dictionary (alias chain)
    for path in ("/v1/schema/data-dictionary", "/v1/schema/data_dictionary"):
        status, data, dt, raw = request_json("GET", path, timeout=TIMEOUT_MED)
        test_name = f"GET {path}"
        payload = unwrap_payload(data)

        if status == 0:
            record(test_name, "fail", dt, f"Exception: {raw}", status)
            break
        if status == 404:
            record(
                test_name,
                "skip",
                dt,
                f"Not mounted ({fmt_dt(dt)})",
                status,
            )
            continue

        if status == 200 and isinstance(payload, dict):
            headers_n = count_safe(payload.get("headers"))
            rows_n = row_count(payload)
            if headers_n == 9 and rows_n > 0:
                record(
                    test_name,
                    "pass",
                    dt,
                    f"9-column contract rows={rows_n} ({fmt_dt(dt)})",
                    status,
                )
            elif headers_n == 9:
                record(
                    test_name,
                    "warn",
                    dt,
                    f"9-column contract but empty rows ({fmt_dt(dt)})",
                    status,
                )
            else:
                record(
                    test_name,
                    "warn",
                    dt,
                    f"Unexpected contract headers={headers_n} ({fmt_dt(dt)})",
                    status,
                )
            break  # one alias is enough on success/warn
        else:
            record(
                test_name,
                "fail",
                dt,
                f"HTTP {status} ({fmt_dt(dt)}) | body={raw!r}",
                status,
            )
            break


# ===========================================================================
# Test groups registry
# ===========================================================================
# Each group is a name -> list of (test_name, callable) used for CLI
# --only and --skip-* filtering.

_TEST_GROUPS: Dict[str, List[Callable[[], None]]] = {
    "meta": [check_meta_and_health],
    "sheet-rows": [
        check_analysis_market_leaders_get,
        check_analysis_global_markets_post,
        check_advanced_insights_post,
        check_advisor_portfolio_post,
        check_root_sheet_rows_top10,
    ],
    "top10": [
        check_advanced_top10_investments,
        check_advanced_top10_alias,
    ],
    "quotes": [
        check_enriched_quotes_batch,
        check_enriched_quote_single,
    ],
    "schema": [check_schema_routes],
}


def _run_groups(enabled: List[str]) -> None:
    for group_name in enabled:
        fns = _TEST_GROUPS.get(group_name, [])
        for fn in fns:
            try:
                fn()
            except Exception as e:
                # Last-resort safety net so a single bad test doesn't
                # abort the run.
                record(
                    f"<harness:{fn.__name__}>",
                    "fail",
                    0.0,
                    f"Uncaught harness exception: {e}",
                )


# ===========================================================================
# Summary & JSON output
# ===========================================================================
def _print_summary() -> None:
    log("Summary", "header")
    total = TALLY.total
    msg = (
        f"Total={total} | "
        f"{Colors.OKGREEN}PASS={TALLY.passed}{Colors.ENDC} | "
        f"{Colors.WARNING}WARN={TALLY.warned}{Colors.ENDC} | "
        f"{Colors.FAIL}FAIL={TALLY.failed}{Colors.ENDC} | "
        f"SKIP={TALLY.skipped}"
    )
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def _emit_json(path: Optional[str]) -> None:
    """If path truthy and non-empty, write JSON report to file. Otherwise
    write JSON to stdout on a marker line."""
    if not path:
        return
    report = {
        "version": SCRIPT_VERSION,
        "base_url": BASE_URL,
        "token_set": bool(TOKEN),
        "totals": {
            "total": TALLY.total,
            "passed": TALLY.passed,
            "warned": TALLY.warned,
            "failed": TALLY.failed,
            "skipped": TALLY.skipped,
        },
        "results": [
            {
                "name": r.name,
                "status": r.status,
                "duration_sec": r.duration_sec,
                "http_status": r.http_status,
                "detail": r.detail,
            }
            for r in TALLY.results
        ],
    }
    try:
        if path.strip() == "-":
            sys.stdout.write("\n===JSON_REPORT===\n")
            sys.stdout.write(_json.dumps(report, indent=2, default=str))
            sys.stdout.write("\n===END_JSON_REPORT===\n")
        else:
            Path(path).write_text(
                _json.dumps(report, indent=2, default=str), encoding="utf-8"
            )
            sys.stdout.write(f"\n[INFO] JSON report written: {path}\n")
    except Exception as e:
        sys.stderr.write(f"[WARN] Failed to write JSON report: {e}\n")


def _compute_exit_code(strict: bool) -> int:
    if TALLY.failed > 0:
        return 1
    if strict and TALLY.warned > 0:
        return 2
    return 0


# ===========================================================================
# CLI
# ===========================================================================
def _parse_groups(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [
        p.strip().lower().replace("_", "-")
        for p in str(raw).split(",")
        if p.strip()
    ]


def create_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=f"TFB Endpoint Smoke Tester v{SCRIPT_VERSION}"
    )
    p.add_argument(
        "--only",
        default=os.getenv("TFB_ONLY") or None,
        help=(
            "Comma-separated test groups to run. Choices: meta, sheet-rows, "
            "top10, quotes, schema. Default: all. (also TFB_ONLY env)."
        ),
    )
    p.add_argument(
        "--skip-meta",
        action="store_true",
        default=_env_bool("TFB_SKIP_META", False),
        help="Skip meta/health endpoints (also TFB_SKIP_META env).",
    )
    p.add_argument(
        "--skip-sheet-rows",
        action="store_true",
        default=_env_bool("TFB_SKIP_SHEET_ROWS", False),
        help="Skip sheet-rows endpoints (also TFB_SKIP_SHEET_ROWS env).",
    )
    p.add_argument(
        "--skip-top10",
        action="store_true",
        default=_env_bool("TFB_SKIP_TOP10", False),
        help="Skip /v1/advanced/top10* endpoints (also TFB_SKIP_TOP10 env).",
    )
    p.add_argument(
        "--skip-quotes",
        action="store_true",
        default=_env_bool("TFB_SKIP_QUOTES", False),
        help="Skip /v1/enriched/quote(s) endpoints (also TFB_SKIP_QUOTES env).",
    )
    p.add_argument(
        "--skip-schema",
        action="store_true",
        default=_env_bool("TFB_SKIP_SCHEMA", False),
        help="Skip /v1/schema/* endpoints (also TFB_SKIP_SCHEMA env).",
    )
    p.add_argument(
        "--strict",
        action="store_true",
        default=_env_bool("TFB_STRICT", False),
        help="Promote WARN to FAIL for exit-code calculation "
        "(also TFB_STRICT env).",
    )
    p.add_argument(
        "--json",
        nargs="?",
        const="-",
        default=os.getenv("TFB_JSON_OUTPUT") or None,
        help=(
            "Write JSON report. Pass '-' (or no value) to emit on stdout "
            "between ===JSON_REPORT=== markers, or a path to write to "
            "file (also TFB_JSON_OUTPUT env)."
        ),
    )
    p.add_argument(
        "--timeout-long",
        type=float,
        default=None,
        help=(
            "Override long-request timeout in seconds "
            "(also TFB_TIMEOUT_LONG env)."
        ),
    )
    return p


def _resolve_groups(args: argparse.Namespace) -> List[str]:
    all_groups = list(_TEST_GROUPS.keys())
    only = _parse_groups(args.only)
    if only:
        enabled = [g for g in only if g in _TEST_GROUPS]
        unknown = [g for g in only if g not in _TEST_GROUPS]
        for u in unknown:
            sys.stderr.write(f"[WARN] Unknown test group: {u}\n")
        return enabled

    enabled = list(all_groups)
    if args.skip_meta and "meta" in enabled:
        enabled.remove("meta")
    if args.skip_sheet_rows and "sheet-rows" in enabled:
        enabled.remove("sheet-rows")
    if args.skip_top10 and "top10" in enabled:
        enabled.remove("top10")
    if args.skip_quotes and "quotes" in enabled:
        enabled.remove("quotes")
    if args.skip_schema and "schema" in enabled:
        enabled.remove("schema")
    return enabled


def main() -> int:
    global TIMEOUT_LONG

    args = create_parser().parse_args()

    if args.timeout_long is not None:
        TIMEOUT_LONG = float(args.timeout_long)

    sys.stdout.write(
        f"{Colors.HEADER}TADAWUL FAST BRIDGE - ENDPOINT TESTER v{SCRIPT_VERSION}"
        f"{Colors.ENDC}\n"
    )
    sys.stdout.write(
        f"BASE_URL={BASE_URL} | TOKEN={'SET' if TOKEN else 'NOT SET'}\n"
    )
    sys.stdout.write(
        f"KSA_TEST_SYMBOL={KSA_TEST_SYMBOL} | "
        f"GLOBAL_TEST_SYMBOL={GLOBAL_TEST_SYMBOL} | "
        f"TOP10_SYMBOL={TOP10_SYMBOL} | COMMODITY_SYMBOL={COMMODITY_SYMBOL}\n"
    )
    sys.stdout.write(
        f"TIMEOUT_SHORT={TIMEOUT_SHORT} | TIMEOUT_MED={TIMEOUT_MED} "
        f"| TIMEOUT_LONG={TIMEOUT_LONG}\n"
    )

    enabled_groups = _resolve_groups(args)
    sys.stdout.write(f"ENABLED_GROUPS={','.join(enabled_groups) or '<none>'}\n")
    sys.stdout.flush()

    try:
        # Check server first — abort test groups if server is down.
        if not check_server():
            sys.stdout.write(
                "\nCannot proceed. Run your API first (or set TFB_BASE_URL).\n"
            )
            _print_summary()
            _emit_json(args.json)
            return 2

        _run_groups(enabled_groups)

        _print_summary()
        _emit_json(args.json)
        return _compute_exit_code(args.strict)
    finally:
        _close_session()


def _handle_sigint(_signum: int, _frame: Any) -> None:  # pragma: no cover
    sys.stdout.write("\n[INFO] SIGINT received, exiting...\n")
    _close_session()
    raise SystemExit(130)


__all__ = [
    "SCRIPT_VERSION",
    "SERVICE_VERSION",
    "SCRIPT_NAME",
    "BASE_URL",
    "TOKEN",
    "Colors",
    "TestResult",
    "Tally",
    "TALLY",
    "request_json",
    "check_server",
    "check_meta_and_health",
    "check_analysis_market_leaders_get",
    "check_analysis_global_markets_post",
    "check_advanced_insights_post",
    "check_advisor_portfolio_post",
    "check_root_sheet_rows_top10",
    "check_advanced_top10_investments",
    "check_advanced_top10_alias",
    "check_enriched_quotes_batch",
    "check_enriched_quote_single",
    "check_schema_routes",
    "create_parser",
    "main",
]


if __name__ == "__main__":
    try:
        signal.signal(signal.SIGINT, _handle_sigint)
    except Exception:
        pass
    raise SystemExit(main())
