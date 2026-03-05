#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/run_dashboard_sync.py
================================================================================
TADAWUL FAST BRIDGE — DASHBOARD SYNC RUNNER (v6.2.0)
================================================================================
PRODUCTION-HARDENED | ASYNC | NON-BLOCKING | COMPILEALL-SAFE | SCHEMA-FIRST

Core fixes in v6.2.0 (aligned with your March 2026 schema changes)
- ✅ Removed legacy KSA_TADAWUL and Advisor_Criteria completely (never runs by default).
- ✅ Added TOP_10_INVESTMENTS + DATA_DICTIONARY tasks.
- ✅ No longer requires symbols to run a task:
    - special/meta pages (Insights_Analysis, Top_10_Investments, Data_Dictionary) work with empty tickers
    - instrument pages still try to read symbols, but will fall back to schema-only headers if empty
- ✅ Sends BOTH "sheet" and "sheet_name" (and "page") to maximize route compatibility.
- ✅ Adds auth headers support (TFB_TOKEN / X_APP_TOKEN / BACKEND_TOKEN) for protected endpoints.
- ✅ Endpoint fallback chain is more robust (enriched → analysis → advanced).

Design rules
- No network calls at import-time.
- Conservative: skips optional modules safely, reports warnings instead of crashing deployment.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
import random
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

# -----------------------------------------------------------------------------
# Version
# -----------------------------------------------------------------------------
SCRIPT_VERSION = "6.2.0"

# -----------------------------------------------------------------------------
# Logging (Render-safe)
# -----------------------------------------------------------------------------
LOG_LEVEL = (os.getenv("LOG_LEVEL") or "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("DashboardSync")

# -----------------------------------------------------------------------------
# Helpers (safe)
# -----------------------------------------------------------------------------
_A1_CELL_RE = re.compile(r"^\$?[A-Za-z]+\$?\d+$")
_TRUTHY = {"1", "true", "yes", "y", "on"}
_FALSY = {"0", "false", "no", "n", "off"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if not s:
        return default
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _safe_int(v: Any, default: int, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        x = int(float(str(v).strip()))
    except Exception:
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x


def _validate_a1_cell(a1: str) -> str:
    s = (a1 or "").strip()
    if not s:
        return "A5"
    if not _A1_CELL_RE.match(s):
        raise ValueError(f"Invalid A1 start cell: {a1!r}")
    return s


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _canon_key(user_key: str) -> str:
    """
    Normalizes SYNC_KEYS tokens to canonical runner keys.

    Canonical runner keys (March 2026):
      MARKET_LEADERS
      GLOBAL_MARKETS
      COMMODITIES_FX
      MUTUAL_FUNDS
      MY_PORTFOLIO
      INSIGHTS_ANALYSIS
      TOP_10_INVESTMENTS
      DATA_DICTIONARY
    """
    k = (user_key or "").strip().upper().replace("-", "_").replace(" ", "_")
    aliases = {
        "LEADERS": "MARKET_LEADERS",
        "MARKET": "MARKET_LEADERS",
        "GLOBAL": "GLOBAL_MARKETS",
        "FUNDS": "MUTUAL_FUNDS",
        "ETF": "MUTUAL_FUNDS",
        "ETFS": "MUTUAL_FUNDS",
        "FX": "COMMODITIES_FX",
        "COMMODITIES": "COMMODITIES_FX",
        "PORTFOLIO": "MY_PORTFOLIO",
        "INSIGHTS": "INSIGHTS_ANALYSIS",
        "ANALYSIS": "INSIGHTS_ANALYSIS",
        "TOP10": "TOP_10_INVESTMENTS",
        "TOP_10": "TOP_10_INVESTMENTS",
        "TOP10_INVESTMENTS": "TOP_10_INVESTMENTS",
        "DATA_DICTIONARY": "DATA_DICTIONARY",
        "DICTIONARY": "DATA_DICTIONARY",
        "DATA_DICTIONARY_SHEET": "DATA_DICTIONARY",
    }
    return aliases.get(k, k)


def _is_forbidden_key(k: str) -> bool:
    kk = _canon_key(k)
    return kk in {"KSA_TADAWUL", "ADVISOR_CRITERIA"}


def _default_backend_url() -> str:
    return (os.getenv("BACKEND_BASE_URL") or os.getenv("DEFAULT_BACKEND_URL") or "http://127.0.0.1:8000").rstrip("/")


def _default_spreadsheet_id(cli_id: Optional[str]) -> str:
    if cli_id and cli_id.strip():
        return cli_id.strip()
    return (os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("SPREADSHEET_ID") or "").strip()


def _env_token() -> str:
    """
    Best-effort auth token loader.
    Supports:
      - TFB_TOKEN
      - X_APP_TOKEN
      - APP_TOKEN
      - BACKEND_TOKEN
    """
    for name in ("TFB_TOKEN", "X_APP_TOKEN", "APP_TOKEN", "BACKEND_TOKEN"):
        v = (os.getenv(name) or "").strip()
        if v:
            return v
    return ""


# -----------------------------------------------------------------------------
# Data models
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class TaskSpec:
    key: str
    sheet_name: str                  # Google Sheet tab name + backend canonical page
    gateway: str                     # enriched | analysis | advanced | argaam
    priority: int = 5
    max_symbols: int = 500
    allow_empty_symbols: bool = True # allow schema-only write when symbols list is empty


@dataclass(slots=True)
class TaskResult:
    key: str
    sheet_name: str
    status: str
    start_utc: str
    end_utc: Optional[str] = None
    duration_ms: float = 0.0
    symbols_requested: int = 0
    symbols_processed: int = 0
    rows_written: int = 0
    rows_failed: int = 0
    gateway_used: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    error: Optional[str] = None
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "sheet_name": self.sheet_name,
            "status": self.status,
            "start_utc": self.start_utc,
            "end_utc": self.end_utc,
            "duration_ms": self.duration_ms,
            "symbols_requested": self.symbols_requested,
            "symbols_processed": self.symbols_processed,
            "rows_written": self.rows_written,
            "rows_failed": self.rows_failed,
            "gateway_used": self.gateway_used,
            "warnings": self.warnings,
            "error": self.error,
            "request_id": self.request_id,
            "version": SCRIPT_VERSION,
        }


@dataclass(slots=True)
class RunSummary:
    version: str = SCRIPT_VERSION
    start_utc: str = field(default_factory=lambda: _utc_now().isoformat())
    end_utc: Optional[str] = None
    duration_ms: float = 0.0
    total_tasks: int = 0
    success: int = 0
    partial: int = 0
    failed: int = 0
    skipped: int = 0
    total_rows_written: int = 0
    total_rows_failed: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "start_utc": self.start_utc,
            "end_utc": self.end_utc,
            "duration_ms": self.duration_ms,
            "total_tasks": self.total_tasks,
            "success": self.success,
            "partial": self.partial,
            "failed": self.failed,
            "skipped": self.skipped,
            "total_rows_written": self.total_rows_written,
            "total_rows_failed": self.total_rows_failed,
        }


# -----------------------------------------------------------------------------
# Backend client (httpx preferred)
# -----------------------------------------------------------------------------
class BackendClient:
    def __init__(self, base_url: str, timeout_sec: float = 30.0, token: str = ""):
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = float(timeout_sec)
        self.token = (token or "").strip()
        self._client = None  # lazy

    def _headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json"}
        if self.token:
            # Support both styles; backend may read either
            h["Authorization"] = f"Bearer {self.token}"
            h["X-APP-TOKEN"] = self.token
        return h

    async def _get_client(self):
        if self._client is not None:
            return self._client
        try:
            import httpx
        except Exception as e:
            raise RuntimeError(f"httpx not available: {e}")
        self._client = httpx.AsyncClient(timeout=self.timeout_sec, headers=self._headers())
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                pass
            self._client = None

    async def get_json(self, path: str) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
        url = f"{self.base_url}{path}"
        try:
            client = await self._get_client()
            r = await client.get(url)
            code = int(r.status_code)
            if code != 200:
                return None, f"HTTP {code}: {r.text[:200]}", code
            return r.json(), None, code
        except Exception as e:
            return None, str(e), 0

    async def post_json(self, path: str, payload: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
        url = f"{self.base_url}{path}"
        max_retries = 3

        for attempt in range(max_retries):
            try:
                client = await self._get_client()
                r = await client.post(url, json=payload)
                code = int(r.status_code)

                # retry on rate limit / transient 5xx
                if code in (429,) or (500 <= code < 600):
                    if attempt == max_retries - 1:
                        return None, f"HTTP {code}: {r.text[:200]}", code
                    await asyncio.sleep(min(10.0, (2**attempt) + random.uniform(0, 1.0)))
                    continue

                if code != 200:
                    return None, f"HTTP {code}: {r.text[:200]}", code

                return r.json(), None, code

            except Exception as e:
                if attempt == max_retries - 1:
                    return None, str(e), 0
                await asyncio.sleep(min(10.0, (2**attempt) + random.uniform(0, 1.0)))

        return None, "Unknown error", 0


# -----------------------------------------------------------------------------
# Redis distributed lock (optional)
# -----------------------------------------------------------------------------
class RedisLock:
    def __init__(self, lock_name: str, ttl_sec: int = 300):
        self.lock_name = f"tfb:dashboard_sync:{lock_name}"
        self.ttl_sec = int(ttl_sec)
        self.value = str(uuid.uuid4())
        self._redis = None
        self.acquired = False

    async def _get_redis(self):
        if self._redis is not None:
            return self._redis
        url = (os.getenv("REDIS_URL") or "").strip()
        if not url:
            return None
        try:
            import redis.asyncio as redis_async
        except Exception:
            return None
        try:
            self._redis = redis_async.from_url(url, decode_responses=True)
            return self._redis
        except Exception:
            return None

    async def acquire(self) -> bool:
        r = await self._get_redis()
        if r is None:
            self.acquired = True  # no redis => allow run
            return True
        try:
            ok = await r.set(self.lock_name, self.value, nx=True, ex=self.ttl_sec)
            self.acquired = bool(ok)
            return self.acquired
        except Exception:
            self.acquired = False
            return False

    async def release(self) -> bool:
        r = await self._get_redis()
        if r is None:
            return True
        if not self.acquired:
            return True
        lua = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        try:
            res = await r.eval(lua, 1, self.lock_name, self.value)
            self.acquired = False
            return bool(res)
        except Exception:
            return False

    async def close(self) -> None:
        if self._redis is not None:
            try:
                await self._redis.close()
            except Exception:
                pass
            self._redis = None


# -----------------------------------------------------------------------------
# Google Sheets writer (optional, direct API)
# -----------------------------------------------------------------------------
class SheetsWriter:
    def __init__(self):
        self._service = None  # lazy

    def _load_credentials_dict(self) -> Optional[Dict[str, Any]]:
        raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or "").strip()
        if not raw:
            path = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
            if path and os.path.exists(path):
                try:
                    return json.loads(Path(path).read_text(encoding="utf-8"))
                except Exception:
                    return None
            return None

        # raw can be JSON or base64(JSON)
        try:
            if raw.startswith("{") and raw.endswith("}"):
                d = json.loads(raw)
            else:
                d = json.loads(base64.b64decode(raw).decode("utf-8"))
            if isinstance(d, dict) and "private_key" in d and isinstance(d["private_key"], str):
                d["private_key"] = d["private_key"].replace("\\n", "\n")
            return d if isinstance(d, dict) else None
        except Exception:
            return None

    def _get_service(self):
        if self._service is not None:
            return self._service

        creds_dict = self._load_credentials_dict()
        if not creds_dict:
            return None
        try:
            from google.oauth2.service_account import Credentials
            from googleapiclient.discovery import build
        except Exception:
            return None

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        self._service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        return self._service

    def _safe_sheet_a1(self, sheet_name: str) -> str:
        name = sheet_name.replace("'", "''")
        # quote always if any non-alnum/underscore
        if re.search(r"[^A-Za-z0-9_]", sheet_name):
            return f"'{name}'"
        return sheet_name

    def clear_from(self, spreadsheet_id: str, sheet_name: str, start_a1: str) -> None:
        svc = self._get_service()
        if not svc:
            return
        m = re.match(r"^\$?([A-Za-z]+)\$?(\d+)$", start_a1.strip())
        if not m:
            return
        col = m.group(1).upper()
        row = int(m.group(2))
        rng = f"{self._safe_sheet_a1(sheet_name)}!{col}{row}:ZZ"
        svc.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=rng, body={}).execute()

    def write_table(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        start_a1: str,
        headers: List[Any],
        rows: List[List[Any]],
    ) -> int:
        svc = self._get_service()
        if not svc:
            return 0

        values: List[List[Any]] = []
        if headers:
            values.append([str(h) for h in headers])
        for r in rows or []:
            values.append(list(r))

        rng = f"{self._safe_sheet_a1(sheet_name)}!{start_a1}"
        body = {"majorDimension": "ROWS", "values": values}
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueInputOption="RAW",
            body=body,
        ).execute()

        return max(0, len(values) - (1 if headers else 0))


# -----------------------------------------------------------------------------
# Symbols reading (uses repo module if present)
# -----------------------------------------------------------------------------
def _read_symbols(task_key: str, spreadsheet_id: str, max_symbols: int) -> List[str]:
    """
    Uses your repo's symbols_reader if available.
    - supports get_page_symbols(key, spreadsheet_id=?)
    - returns uppercase unique symbols
    """
    try:
        import importlib

        sym_mod = importlib.import_module("symbols_reader")
        fn = getattr(sym_mod, "get_page_symbols", None)
        if callable(fn):
            data = fn(task_key, spreadsheet_id=spreadsheet_id)
        else:
            fn2 = getattr(sym_mod, "get_universe", None)
            data = fn2([task_key], spreadsheet_id=spreadsheet_id) if callable(fn2) else {}
    except Exception as e:
        logger.warning("symbols_reader unavailable or failed: %s", e)
        return []

    symbols: List[str] = []
    if isinstance(data, dict):
        v = data.get("all") or data.get("symbols") or []
        symbols = v if isinstance(v, list) else []
    elif isinstance(data, list):
        symbols = data

    out: List[str] = []
    seen: set[str] = set()
    for s in symbols:
        t = str(s or "").strip().upper()
        if not t or t in {"SYMBOL", "TICKER"}:
            continue
        if t not in seen:
            seen.add(t)
            out.append(t)
        if max_symbols > 0 and len(out) >= max_symbols:
            break
    return out


# -----------------------------------------------------------------------------
# Task definitions (aligned with your dashboard tabs + canonical schema)
# -----------------------------------------------------------------------------
def _default_tasks() -> List[TaskSpec]:
    # IMPORTANT: no KSA_TADAWUL here (removed permanently)
    return [
        TaskSpec(key="MY_PORTFOLIO", sheet_name="My_Portfolio", gateway="enriched", priority=1, max_symbols=800, allow_empty_symbols=True),
        TaskSpec(key="MARKET_LEADERS", sheet_name="Market_Leaders", gateway="enriched", priority=2, max_symbols=800, allow_empty_symbols=True),
        TaskSpec(key="GLOBAL_MARKETS", sheet_name="Global_Markets", gateway="enriched", priority=3, max_symbols=800, allow_empty_symbols=True),
        TaskSpec(key="COMMODITIES_FX", sheet_name="Commodities_FX", gateway="enriched", priority=4, max_symbols=400, allow_empty_symbols=True),
        TaskSpec(key="MUTUAL_FUNDS", sheet_name="Mutual_Funds", gateway="enriched", priority=5, max_symbols=400, allow_empty_symbols=True),
        # Special/meta pages — do NOT require symbols
        TaskSpec(key="INSIGHTS_ANALYSIS", sheet_name="Insights_Analysis", gateway="analysis", priority=6, max_symbols=0, allow_empty_symbols=True),
        TaskSpec(key="TOP_10_INVESTMENTS", sheet_name="Top_10_Investments", gateway="analysis", priority=7, max_symbols=0, allow_empty_symbols=True),
        TaskSpec(key="DATA_DICTIONARY", sheet_name="Data_Dictionary", gateway="analysis", priority=8, max_symbols=0, allow_empty_symbols=True),
    ]


def _endpoint_candidates_for_gateway(gw: str) -> List[str]:
    """
    Endpoint candidates (first is preferred). We try /v1/... then fallback.
    """
    gw = (gw or "enriched").strip().lower()

    if gw == "analysis" or gw == "ai":
        return [
            "/v1/analysis/sheet-rows",
            "/analysis/sheet-rows",
            "/v1/advanced/sheet-rows",
            "/advanced/sheet-rows",
            "/v1/enriched/sheet-rows",
            "/enriched/sheet-rows",
        ]

    if gw == "advanced":
        return [
            "/v1/advanced/sheet-rows",
            "/advanced/sheet-rows",
            "/v1/analysis/sheet-rows",
            "/analysis/sheet-rows",
        ]

    if gw == "argaam":
        return ["/v1/argaam/sheet-rows", "/argaam/sheet-rows"]

    # enriched default
    return [
        "/v1/enriched/sheet-rows",
        "/enriched/sheet-rows",
        "/v1/analysis/sheet-rows",
        "/analysis/sheet-rows",
        "/v1/advanced/sheet-rows",
        "/advanced/sheet-rows",
    ]


def _extract_table_payload(resp: Dict[str, Any]) -> Tuple[List[Any], List[List[Any]]]:
    """
    Accepts multiple shapes:
    - {"headers":[...],"rows":[...]}
    - {"data":{"headers":[...],"rows":[...]}}
    - {"headers":[...], "rows_matrix":[...]}  (legacy)
    """
    if not isinstance(resp, dict):
        return [], []

    headers = resp.get("headers")
    rows = resp.get("rows")
    if isinstance(headers, list) and isinstance(rows, list):
        # if rows are dicts, attempt to convert to matrix using keys
        if rows and isinstance(rows[0], dict):
            keys = resp.get("keys") if isinstance(resp.get("keys"), list) else []
            if keys:
                matrix = [[r.get(k) for k in keys] for r in rows]  # type: ignore
                return headers, matrix
        return headers, rows

    # legacy matrix
    if isinstance(headers, list) and isinstance(resp.get("rows_matrix"), list):
        return headers, resp.get("rows_matrix") or []

    data = resp.get("data")
    if isinstance(data, dict) and isinstance(data.get("headers"), list) and isinstance(data.get("rows"), list):
        return data.get("headers") or [], data.get("rows") or []

    return [], []


# -----------------------------------------------------------------------------
# Run one task
# -----------------------------------------------------------------------------
async def _run_one_task(
    task: TaskSpec,
    spreadsheet_id: str,
    start_cell: str,
    max_symbols_override: int,
    clear_before_write: bool,
    dry_run: bool,
    backend: BackendClient,
    sheets: Optional[SheetsWriter],
) -> TaskResult:
    t0 = time.perf_counter()
    res = TaskResult(
        key=task.key,
        sheet_name=task.sheet_name,
        status="pending",
        start_utc=_utc_now().isoformat(),
    )

    try:
        if _is_forbidden_key(task.key):
            res.status = "skipped"
            res.warnings.append("Forbidden legacy key; skipped.")
            return res

        max_syms = max_symbols_override if max_symbols_override >= 0 else task.max_symbols
        canon_task_key = _canon_key(task.key)

        # Read symbols (best effort)
        symbols: List[str] = []
        if max_syms != 0:
            symbols = _read_symbols(canon_task_key, spreadsheet_id, max_syms)

        res.symbols_requested = len(symbols)

        if dry_run:
            res.status = "skipped"
            res.warnings.append("Dry run: no backend call, no sheet write.")
            return res

        if (not symbols) and not task.allow_empty_symbols:
            res.status = "skipped"
            res.warnings.append("No symbols found and task disallows empty symbols.")
            return res

        if not symbols:
            res.warnings.append("No symbols found; requesting schema-only payload (headers + empty rows).")

        # Payload: send multiple keys to satisfy different handlers
        payload: Dict[str, Any] = {
            "sheet": task.sheet_name,
            "sheet_name": task.sheet_name,
            "page": task.sheet_name,
            "tickers": symbols,
            "symbols": symbols,
            "refresh": True,
            "include_meta": True,
            "include_matrix": True,
            "limit": 0 if not symbols else min(5000, max(1, len(symbols))),
        }

        last_err: Optional[str] = None
        headers: List[Any] = []
        rows: List[List[Any]] = []
        used_endpoint: Optional[str] = None

        for ep in _endpoint_candidates_for_gateway(task.gateway):
            data, err, _code = await backend.post_json(ep, payload)
            if err:
                last_err = f"{ep} -> {err}"
                continue
            if not isinstance(data, dict):
                last_err = f"{ep} -> Non-dict response"
                continue

            headers, rows = _extract_table_payload(data)
            if not isinstance(headers, list) or not headers:
                last_err = f"{ep} -> Missing headers"
                continue
            if not isinstance(rows, list):
                rows = []
            used_endpoint = ep
            break

        if not headers:
            res.status = "failed"
            res.error = last_err or "All endpoints failed"
            return res

        res.gateway_used = f"{task.gateway}:{used_endpoint}" if used_endpoint else task.gateway
        res.symbols_processed = len(symbols)

        # If no sheet creds => treat as partial (data fetched but not written)
        if sheets is None or sheets._get_service() is None:
            res.status = "partial"
            res.warnings.append("No Google Sheets credentials. Backend data fetched but not written.")
            res.rows_written = 0
            res.rows_failed = len(rows or [])
            return res

        if clear_before_write:
            try:
                sheets.clear_from(spreadsheet_id, task.sheet_name, start_cell)
            except Exception as e:
                res.warnings.append(f"Clear failed: {e}")

        try:
            written = sheets.write_table(spreadsheet_id, task.sheet_name, start_cell, headers, rows)
            res.rows_written = int(written)
            # if schema-only (0 rows), that's still success
            if not rows:
                res.rows_failed = 0
                res.status = "success"
            else:
                res.rows_failed = max(0, len(rows) - res.rows_written)
                res.status = "success" if res.rows_failed == 0 else ("partial" if res.rows_written > 0 else "failed")
        except Exception as e:
            res.status = "failed"
            res.error = f"Write failed: {e}"

        return res

    except Exception as e:
        res.status = "failed"
        res.error = str(e)
        return res

    finally:
        res.end_utc = _utc_now().isoformat()
        res.duration_ms = (time.perf_counter() - t0) * 1000.0


# -----------------------------------------------------------------------------
# Main runner
# -----------------------------------------------------------------------------
async def main_async(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=f"TFB Dashboard Sync Runner v{SCRIPT_VERSION}")
    parser.add_argument("--sheet-id", default="", help="Spreadsheet ID override")
    parser.add_argument("--backend", default="", help="Backend base URL override (e.g. https://... )")
    parser.add_argument("--keys", nargs="*", default=[], help="Specific keys to run (e.g. MARKET_LEADERS GLOBAL_MARKETS)")
    parser.add_argument("--start-cell", default="A5", help="Top-left A1 cell where headers will be written (e.g. A5)")
    parser.add_argument("--max-symbols", default="-1", help="Override max symbols for all tasks (-1 = per task default)")
    parser.add_argument("--workers", default="4", help="Parallel workers")
    parser.add_argument("--clear", action="store_true", help="Clear from start-cell down before writing")
    parser.add_argument("--dry-run", action="store_true", help="Do not call backend or write sheets")
    parser.add_argument("--no-lock", action="store_true", help="Disable Redis lock even if REDIS_URL exists")
    parser.add_argument("--json-out", default="", help="Write JSON report to this file path")
    args = parser.parse_args(list(argv) if argv is not None else None)

    spreadsheet_id = _default_spreadsheet_id(args.sheet_id)
    if not spreadsheet_id:
        logger.error("DEFAULT_SPREADSHEET_ID is missing and --sheet-id not provided.")
        return 1

    backend_url = (args.backend or _default_backend_url()).rstrip("/")
    start_cell = _validate_a1_cell(args.start_cell)
    max_symbols = _safe_int(args.max_symbols, -1, lo=-1, hi=5000)
    workers = _safe_int(args.workers, 4, lo=1, hi=32)

    token = _env_token()
    if not token:
        logger.warning("No backend token found (TFB_TOKEN/X_APP_TOKEN/APP_TOKEN/BACKEND_TOKEN). Requests may 401 if protected.")

    # Select tasks
    tasks = _default_tasks()
    wanted_raw = [str(k) for k in (args.keys or []) if str(k).strip()]
    wanted = {_canon_key(k) for k in wanted_raw if not _is_forbidden_key(k)}
    forbidden_requested = [k for k in wanted_raw if _is_forbidden_key(k)]
    if forbidden_requested:
        logger.warning("Forbidden keys requested and will be ignored: %s", ", ".join(forbidden_requested))

    if wanted:
        tasks = [t for t in tasks if _canon_key(t.key) in wanted]

    tasks.sort(key=lambda t: (t.priority, t.key))
    if not tasks:
        logger.warning("No tasks selected.")
        return 0

    summary = RunSummary()
    summary.total_tasks = len(tasks)
    t0 = time.perf_counter()

    backend = BackendClient(backend_url, timeout_sec=30.0, token=token)
    sheets = SheetsWriter()

    lock_name = f"{spreadsheet_id}:{','.join([t.key for t in tasks])}"
    lock = RedisLock(lock_name, ttl_sec=600)

    try:
        # Preflight (backend)
        ready, err, _code = await backend.get_json("/readyz")
        if err:
            logger.warning("Backend /readyz not OK: %s", err)
        else:
            logger.info("Backend ready: %s", str((ready or {}).get("status") or "ok"))

        # Acquire lock
        if args.no_lock:
            acquired = True
        else:
            acquired = await lock.acquire()

        if not acquired:
            logger.error("Could not acquire Redis lock. Use --no-lock to bypass.")
            return 2

        sem = asyncio.Semaphore(workers)

        async def _guarded(task: TaskSpec) -> TaskResult:
            async with sem:
                return await _run_one_task(
                    task=task,
                    spreadsheet_id=spreadsheet_id,
                    start_cell=start_cell,
                    max_symbols_override=max_symbols,
                    clear_before_write=bool(args.clear),
                    dry_run=bool(args.dry_run),
                    backend=backend,
                    sheets=sheets,
                )

        results: List[TaskResult] = []
        out = await asyncio.gather(*[_guarded(t) for t in tasks], return_exceptions=True)

        for i, r in enumerate(out):
            if isinstance(r, Exception):
                tr = TaskResult(
                    key=tasks[i].key,
                    sheet_name=tasks[i].sheet_name,
                    status="failed",
                    start_utc=_utc_now().isoformat(),
                    end_utc=_utc_now().isoformat(),
                    duration_ms=0.0,
                    error=str(r),
                )
                results.append(tr)
            else:
                results.append(r)

        # Summarize
        for r in results:
            if r.status == "success":
                summary.success += 1
            elif r.status == "partial":
                summary.partial += 1
            elif r.status == "failed":
                summary.failed += 1
            else:
                summary.skipped += 1
            summary.total_rows_written += r.rows_written
            summary.total_rows_failed += r.rows_failed

        summary.end_utc = _utc_now().isoformat()
        summary.duration_ms = (time.perf_counter() - t0) * 1000.0

        logger.info("============================================================")
        logger.info(
            "SYNC DONE | success=%d partial=%d failed=%d skipped=%d | rows_written=%d | duration_ms=%.2f",
            summary.success,
            summary.partial,
            summary.failed,
            summary.skipped,
            summary.total_rows_written,
            summary.duration_ms,
        )
        for r in results:
            if r.status == "success":
                logger.info("✅ %s -> %s | rows=%d | %.1fms", r.key, r.sheet_name, r.rows_written, r.duration_ms)
            elif r.status == "partial":
                logger.info(
                    "⚠️  %s -> %s | rows=%d failed=%d | %.1fms | %s",
                    r.key,
                    r.sheet_name,
                    r.rows_written,
                    r.rows_failed,
                    r.duration_ms,
                    "; ".join(r.warnings[:2]),
                )
            elif r.status == "failed":
                logger.info("❌ %s -> %s | %s", r.key, r.sheet_name, r.error or "failed")
            else:
                logger.info("⏭️  %s -> %s | %s", r.key, r.sheet_name, "; ".join(r.warnings[:2]) if r.warnings else "skipped")

        # Optional JSON report
        if args.json_out:
            report = {"summary": summary.to_dict(), "results": [x.to_dict() for x in results]}
            Path(args.json_out).write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
            logger.info("Report saved: %s", args.json_out)

        # Exit code policy
        if summary.failed > 0:
            return 2
        if summary.partial > 0:
            return 1
        return 0

    finally:
        try:
            await lock.release()
        except Exception:
            pass
        await lock.close()
        await backend.close()


def main() -> int:
    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.warning("Interrupted.")
        return 130
    except Exception as e:
        logger.exception("Unhandled error: %s", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
