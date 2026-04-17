#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/run_dashboard_sync.py
================================================================================
TADAWUL FAST BRIDGE — DASHBOARD SYNC RUNNER (v6.4.0)
================================================================================
PRODUCTION-HARDENED | ASYNC | NON-BLOCKING | COMPILEALL-SAFE | SCHEMA-FIRST

v6.4.0 fixes
- FIX: Added dedup guard before Sheets write. When the backend returns duplicate
  symbol rows (e.g. a symbol appears on two source pages), all duplicates after
  the first occurrence are dropped before writing to Google Sheets.
- FIX: Added run_from_worker_payload() — programmatic entry point for worker.py.
  In v6.3.0, worker.py had to construct a fake argparse.Namespace to call
  main_async(), which broke whenever new CLI args were added. Now worker.py
  calls run_from_worker_payload(payload) directly.
- ENH: Added WORKER_PAYLOAD_SCHEMA dict — documents the contract for the
  task_type="dashboard_sync" payload sent by worker.py. Prevents silent schema
  drift between worker.py and this module.

v6.3.0 fixes
- Sheets-safe ALWAYS: backend rows (dicts or lists) -> strict 2D matrix
- JSON-safe value coercion for Google API
- Key parsing is robust: supports space, comma, semicolon, JSON array-like tokens
- Stronger backend compatibility: sends sheet/page/name/tab + tickers/symbols
- Health preflight probes /readyz + /health + /livez
- Credentials loader hardened
- Never runs forbidden legacy keys (KSA_TADAWUL / ADVISOR_CRITERIA)
- Exit codes: 0 = success, 1 = partial, 2 = failed

Design rules
- No network calls at import-time.
- Conservative: warnings instead of crashes.
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
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

# -----------------------------------------------------------------------------
# Version
# -----------------------------------------------------------------------------
SCRIPT_VERSION = "6.4.0"

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
_SHEET_SAFE_RE = re.compile(r"^[A-Za-z0-9_]+$")
_TRUTHY = {"1", "true", "yes", "y", "on"}
_FALSY = {"0", "false", "no", "n", "off"}

_ALLOWED_KEYS = {
    "MARKET_LEADERS",
    "GLOBAL_MARKETS",
    "COMMODITIES_FX",
    "MUTUAL_FUNDS",
    "MY_PORTFOLIO",
    "INSIGHTS_ANALYSIS",
    "TOP_10_INVESTMENTS",
    "DATA_DICTIONARY",
}
_FORBIDDEN_KEYS = {"KSA_TADAWUL", "ADVISOR_CRITERIA"}


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


def _canon_key(user_key: str) -> str:
    """
    Normalizes SYNC_KEYS tokens to canonical runner keys.

    Canonical runner keys (March 2026):
      MARKET_LEADERS, GLOBAL_MARKETS, COMMODITIES_FX, MUTUAL_FUNDS,
      MY_PORTFOLIO, INSIGHTS_ANALYSIS, TOP_10_INVESTMENTS, DATA_DICTIONARY
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
        "TOP_10_INVESTMENTS": "TOP_10_INVESTMENTS",
        "DATA_DICTIONARY_SHEET": "DATA_DICTIONARY",
        "DICTIONARY": "DATA_DICTIONARY",
    }
    return aliases.get(k, k)


def _is_forbidden_key(k: str) -> bool:
    return _canon_key(k) in _FORBIDDEN_KEYS


def _default_backend_url() -> str:
    return (
        os.getenv("BACKEND_BASE_URL")
        or os.getenv("DEFAULT_BACKEND_URL")
        or "http://127.0.0.1:8000"
    ).rstrip("/")


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


def _coerce_jsonable(v: Any) -> Any:
    """Make values safe for JSON/Google Sheets payloads."""
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, Enum):
        return v.value
    if isinstance(v, (datetime, date)):
        try:
            return v.isoformat()
        except Exception:
            return str(v)
    if isinstance(v, dict):
        return {str(k): _coerce_jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [_coerce_jsonable(x) for x in v]
    try:
        if hasattr(v, "model_dump"):
            return _coerce_jsonable(v.model_dump(mode="python"))  # type: ignore
        if hasattr(v, "dict"):
            return _coerce_jsonable(v.dict())  # type: ignore
    except Exception:
        pass
    return str(v)


def _parse_keys_tokens(raw_tokens: Sequence[str]) -> List[str]:
    """
    Accepts:
      --keys A B C
      --keys "A,B,C"
      --keys "A;B;C"
      --keys '["A","B"]'
    """
    flat: List[str] = []
    for t in raw_tokens or []:
        s = str(t or "").strip()
        if not s:
            continue
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    for x in arr:
                        xs = str(x or "").strip()
                        if xs:
                            flat.append(xs)
                    continue
            except Exception:
                pass
        parts = re.split(r"[,\s;|]+", s)
        for p in parts:
            pp = (p or "").strip()
            if pp:
                flat.append(pp)
    out: List[str] = []
    seen: set = set()
    for k in flat:
        ck = _canon_key(k)
        if not ck or ck in seen:
            continue
        seen.add(ck)
        out.append(ck)
    return out


# -----------------------------------------------------------------------------
# Data models
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class TaskSpec:
    key: str
    sheet_name: str                   # Google Sheet tab name + backend canonical page
    gateway: str                      # enriched | analysis | advanced | argaam
    priority: int = 5
    max_symbols: int = 500
    allow_empty_symbols: bool = True  # allow schema-only write when symbols list is empty


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
            try:
                return r.json(), None, code
            except Exception as e:
                return None, f"JSON parse error: {e}", code
        except Exception as e:
            return None, str(e), 0

    async def post_json(
        self, path: str, payload: Dict[str, Any]
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
        url = f"{self.base_url}{path}"
        max_retries = 3
        for attempt in range(max_retries):
            try:
                client = await self._get_client()
                r = await client.post(url, json=payload)
                code = int(r.status_code)

                if code in (429,) or (500 <= code < 600):
                    if attempt == max_retries - 1:
                        return None, f"HTTP {code}: {r.text[:200]}", code
                    await asyncio.sleep(min(10.0, (2**attempt) + random.uniform(0, 1.0)))
                    continue

                if code != 200:
                    return None, f"HTTP {code}: {r.text[:200]}", code

                try:
                    return r.json(), None, code
                except Exception as e:
                    return None, f"JSON parse error: {e}", code

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
            self.acquired = True
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

    def _fix_private_key(self, d: Dict[str, Any]) -> Dict[str, Any]:
        try:
            pk = d.get("private_key")
            if isinstance(pk, str) and "\\n" in pk:
                d["private_key"] = pk.replace("\\n", "\n")
        except Exception:
            pass
        return d

    def _load_credentials_dict(self) -> Optional[Dict[str, Any]]:
        raw = (
            os.getenv("GOOGLE_SHEETS_CREDENTIALS")
            or os.getenv("GOOGLE_CREDENTIALS")
            or ""
        ).strip()

        # Prefer GOOGLE_APPLICATION_CREDENTIALS file path (GitHub Actions pattern)
        path = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
        if path and os.path.exists(path):
            try:
                d = json.loads(Path(path).read_text(encoding="utf-8"))
                return self._fix_private_key(d) if isinstance(d, dict) else None
            except Exception:
                return None

        if not raw:
            return None

        try:
            if raw.startswith("{") and raw.endswith("}"):
                d = json.loads(raw)
            else:
                d = json.loads(base64.b64decode(raw).decode("utf-8"))
            return self._fix_private_key(d) if isinstance(d, dict) else None
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
        if _SHEET_SAFE_RE.match(sheet_name or ""):
            return sheet_name
        name = (sheet_name or "").replace("'", "''")
        return f"'{name}'"

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
        svc.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=rng, body={}
        ).execute()

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

        hdr = [str(h) for h in (headers or [])]
        width = len(hdr)

        matrix: List[List[Any]] = []
        for r in rows or []:
            rr = list(r) if isinstance(r, list) else [r]
            if width > 0:
                if len(rr) < width:
                    rr = rr + [None] * (width - len(rr))
                elif len(rr) > width:
                    rr = rr[:width]
            matrix.append([_coerce_jsonable(x) for x in rr])

        values: List[List[Any]] = []
        if hdr:
            values.append(hdr)
        values.extend(matrix)

        rng = f"{self._safe_sheet_a1(sheet_name)}!{start_a1}"
        body = {"majorDimension": "ROWS", "values": values}
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueInputOption="RAW",
            body=body,
        ).execute()

        return max(0, len(values) - (1 if hdr else 0))


# -----------------------------------------------------------------------------
# Symbols reading (uses repo module if present)
# -----------------------------------------------------------------------------
def _read_symbols(task_key: str, spreadsheet_id: str, max_symbols: int) -> List[str]:
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
    seen: set = set()
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
# Task definitions (aligned with canonical schema pages)
# -----------------------------------------------------------------------------
def _default_tasks() -> List[TaskSpec]:
    return [
        TaskSpec(key="MY_PORTFOLIO",       sheet_name="My_Portfolio",       gateway="enriched",  priority=1, max_symbols=800, allow_empty_symbols=True),
        TaskSpec(key="MARKET_LEADERS",     sheet_name="Market_Leaders",     gateway="enriched",  priority=2, max_symbols=800, allow_empty_symbols=True),
        TaskSpec(key="GLOBAL_MARKETS",     sheet_name="Global_Markets",     gateway="enriched",  priority=3, max_symbols=800, allow_empty_symbols=True),
        TaskSpec(key="COMMODITIES_FX",     sheet_name="Commodities_FX",     gateway="enriched",  priority=4, max_symbols=400, allow_empty_symbols=True),
        TaskSpec(key="MUTUAL_FUNDS",       sheet_name="Mutual_Funds",       gateway="enriched",  priority=5, max_symbols=400, allow_empty_symbols=True),
        # Special/meta pages — do NOT require symbols
        TaskSpec(key="INSIGHTS_ANALYSIS",  sheet_name="Insights_Analysis",  gateway="analysis",  priority=6, max_symbols=0, allow_empty_symbols=True),
        TaskSpec(key="TOP_10_INVESTMENTS", sheet_name="Top_10_Investments", gateway="analysis",  priority=7, max_symbols=0, allow_empty_symbols=True),
        TaskSpec(key="DATA_DICTIONARY",    sheet_name="Data_Dictionary",    gateway="analysis",  priority=8, max_symbols=0, allow_empty_symbols=True),
    ]


def _endpoint_candidates_for_gateway(gw: str) -> List[str]:
    gw = (gw or "enriched").strip().lower()
    if gw in {"analysis", "ai"}:
        return [
            "/v1/analysis/sheet-rows",
            "/analysis/sheet-rows",
            "/v1/ai/sheet-rows",
            "/ai/sheet-rows",
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
            "/v1/enriched/sheet-rows",
            "/enriched/sheet-rows",
        ]
    if gw == "argaam":
        return ["/v1/argaam/sheet-rows", "/argaam/sheet-rows"]
    return [
        "/v1/enriched/sheet-rows",
        "/enriched/sheet-rows",
        "/v1/analysis/sheet-rows",
        "/analysis/sheet-rows",
        "/v1/advanced/sheet-rows",
        "/advanced/sheet-rows",
        "/v1/ai/sheet-rows",
        "/ai/sheet-rows",
    ]


def _extract_table_payload(resp: Dict[str, Any]) -> Tuple[List[Any], List[List[Any]]]:
    """
    Returns (headers, rows_matrix) ALWAYS as list[list] for Sheets writing.

    Supports:
      - {"headers":[...], "rows":[list|dict]}
      - {"headers":[...], "rows_matrix":[...]}
      - {"keys":[...]} for dict->matrix conversion
      - {"data": {...}} nested
    """
    if not isinstance(resp, dict):
        return [], []

    if isinstance(resp.get("data"), dict):
        return _extract_table_payload(resp["data"])  # type: ignore[index]

    headers = resp.get("headers")
    keys = resp.get("keys")
    rows = resp.get("rows")
    rows_matrix = resp.get("rows_matrix")

    headers_list = list(headers) if isinstance(headers, list) else []
    keys_list = list(keys) if isinstance(keys, list) else []

    # Prefer explicit matrix
    if isinstance(headers_list, list) and isinstance(rows_matrix, list):
        mm = [list(r) for r in rows_matrix if isinstance(r, list)]
        return headers_list, mm

    if not isinstance(rows, list):
        rows = []

    # rows are list[list]
    if rows and isinstance(rows[0], list):
        if not headers_list and keys_list:
            headers_list = keys_list[:]
        return headers_list, [list(r) for r in rows if isinstance(r, list)]

    # rows are list[dict] -> convert to matrix using keys/headers
    if rows and isinstance(rows[0], dict):
        dict_rows: List[Dict[str, Any]] = [r for r in rows if isinstance(r, dict)]  # type: ignore[assignment]
        if not keys_list:
            if headers_list:
                keys_list = [str(h) for h in headers_list]
            else:
                keys_list = [str(k) for k in dict_rows[0].keys()]
                headers_list = keys_list[:]
        if not headers_list:
            headers_list = keys_list[:]
        matrix = [[_coerce_jsonable(r.get(k)) for k in keys_list] for r in dict_rows]
        return headers_list, matrix

    if headers_list:
        return headers_list, []

    return [], []


def _rectify_matrix(headers: List[Any], matrix: List[List[Any]]) -> List[List[Any]]:
    """Pad/truncate each row to match header length."""
    width = len(headers or [])
    if width <= 0:
        return [list(r) for r in (matrix or []) if isinstance(r, list)]
    out: List[List[Any]] = []
    for r in matrix or []:
        if not isinstance(r, list):
            continue
        rr = list(r)
        if len(rr) < width:
            rr = rr + [None] * (width - len(rr))
        elif len(rr) > width:
            rr = rr[:width]
        out.append(rr)
    return out


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
        canon_task_key = _canon_key(task.key)

        if _is_forbidden_key(canon_task_key):
            res.status = "skipped"
            res.warnings.append("Forbidden legacy key; skipped.")
            return res
        if canon_task_key not in _ALLOWED_KEYS:
            res.status = "skipped"
            res.warnings.append(f"Unknown key {canon_task_key}; skipped.")
            return res

        max_syms = max_symbols_override if max_symbols_override >= 0 else task.max_symbols

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

        safe_limit = 1 if not symbols else min(5000, max(1, len(symbols)))

        payload: Dict[str, Any] = {
            "sheet": task.sheet_name,
            "sheet_name": task.sheet_name,
            "page": task.sheet_name,
            "name": task.sheet_name,
            "tab": task.sheet_name,
            "tickers": symbols,
            "symbols": symbols,
            "refresh": True,
            "include_meta": True,
            "include_matrix": True,
            "limit": safe_limit,
            "request_id": res.request_id,
        }

        last_err: Optional[str] = None
        headers: List[Any] = []
        rows_matrix: List[List[Any]] = []
        used_endpoint: Optional[str] = None

        for ep in _endpoint_candidates_for_gateway(task.gateway):
            data, err, _code = await backend.post_json(ep, payload)
            if err:
                last_err = f"{ep} -> {err}"
                continue
            if not isinstance(data, dict):
                last_err = f"{ep} -> Non-dict response"
                continue

            headers, rows_matrix = _extract_table_payload(data)
            if not headers:
                last_err = f"{ep} -> Missing headers"
                continue

            rows_matrix = _rectify_matrix(headers, rows_matrix)
            used_endpoint = ep
            break

        if not headers:
            res.status = "failed"
            res.error = last_err or "All endpoints failed"
            return res

        res.gateway_used = f"{task.gateway}:{used_endpoint}" if used_endpoint else task.gateway
        res.symbols_processed = len(symbols)

        # FIX v6.4.0: Dedup guard before Sheets write.
        # When the backend returns duplicate symbol rows (e.g. a symbol appears
        # in two source pages, or data_engine_v2 emits the same symbol twice
        # before its dedup fix is deployed), drop all but the first occurrence
        # to prevent duplicate rows in Google Sheets.
        if rows_matrix:
            sym_col_idx: Optional[int] = None
            for col_idx, h in enumerate(headers):
                if str(h).strip().lower() in ("symbol", "ticker", "requestedsymbol"):
                    sym_col_idx = col_idx
                    break
            if sym_col_idx is not None:
                seen_syms: set = set()
                deduped: List[List[Any]] = []
                for row in rows_matrix:
                    sym_val = (
                        str(row[sym_col_idx] or "").strip().upper()
                        if sym_col_idx < len(row)
                        else ""
                    )
                    if not sym_val or sym_val not in seen_syms:
                        if sym_val:
                            seen_syms.add(sym_val)
                        deduped.append(row)
                dropped = len(rows_matrix) - len(deduped)
                if dropped > 0:
                    logger.info(
                        "Dedup: dropped %d duplicate symbol rows for %s",
                        dropped,
                        task.sheet_name,
                    )
                    res.warnings.append(
                        f"Dedup: dropped {dropped} duplicate symbol rows before write."
                    )
                rows_matrix = deduped

        if sheets is None or sheets._get_service() is None:
            res.status = "partial"
            res.warnings.append(
                "No Google Sheets credentials. Backend data fetched but not written."
            )
            res.rows_written = 0
            res.rows_failed = len(rows_matrix or [])
            return res

        if clear_before_write:
            try:
                sheets.clear_from(spreadsheet_id, task.sheet_name, start_cell)
            except Exception as e:
                res.warnings.append(f"Clear failed: {e}")

        try:
            written = sheets.write_table(
                spreadsheet_id, task.sheet_name, start_cell, headers, rows_matrix
            )
            res.rows_written = int(written)

            if not rows_matrix:
                res.rows_failed = 0
                res.status = "success"
            else:
                res.rows_failed = max(0, len(rows_matrix) - res.rows_written)
                res.status = (
                    "success"
                    if res.rows_failed == 0
                    else ("partial" if res.rows_written > 0 else "failed")
                )
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
    parser = argparse.ArgumentParser(
        description=f"TFB Dashboard Sync Runner v{SCRIPT_VERSION}"
    )
    parser.add_argument("--sheet-id", default="", help="Spreadsheet ID override")
    parser.add_argument("--backend", default="", help="Backend base URL override")
    parser.add_argument(
        "--keys",
        nargs="*",
        default=[],
        help="Specific keys (space/comma/semicolon/JSON-array supported)",
    )
    parser.add_argument(
        "--start-cell",
        default="A5",
        help="Top-left A1 cell where headers will be written (e.g. A5)",
    )
    parser.add_argument(
        "--max-symbols",
        default="-1",
        help="Override max symbols for all tasks (-1 = per task default)",
    )
    parser.add_argument("--workers", default="4", help="Parallel workers")
    parser.add_argument(
        "--clear", action="store_true", help="Clear from start-cell down before writing"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Do not call backend or write sheets"
    )
    parser.add_argument(
        "--no-lock", action="store_true", help="Disable Redis lock even if REDIS_URL exists"
    )
    parser.add_argument("--json-out", default="", help="Write JSON report to this file path")
    parser.add_argument("--timeout", default="30", help="Backend timeout seconds")
    args = parser.parse_args(list(argv) if argv is not None else None)

    spreadsheet_id = _default_spreadsheet_id(args.sheet_id)
    if not spreadsheet_id:
        logger.error("DEFAULT_SPREADSHEET_ID is missing and --sheet-id not provided.")
        return 2

    backend_url = (args.backend or _default_backend_url()).rstrip("/")
    start_cell = _validate_a1_cell(args.start_cell)
    max_symbols = _safe_int(args.max_symbols, -1, lo=-1, hi=5000)
    workers = _safe_int(args.workers, 4, lo=1, hi=32)
    timeout_sec = float(_safe_int(args.timeout, 30, lo=5, hi=180))

    token = _env_token()
    if not token:
        logger.warning(
            "No backend token found (TFB_TOKEN/X_APP_TOKEN/APP_TOKEN/BACKEND_TOKEN). "
            "Requests may 401 if protected."
        )

    tasks = _default_tasks()

    wanted = _parse_keys_tokens(args.keys or [])
    forbidden_requested = [k for k in wanted if _is_forbidden_key(k)]
    if forbidden_requested:
        logger.warning(
            "Forbidden keys requested and will be ignored: %s",
            ", ".join(forbidden_requested),
        )

    wanted_ok = [k for k in wanted if (k in _ALLOWED_KEYS and not _is_forbidden_key(k))]
    if wanted_ok:
        tasks = [t for t in tasks if _canon_key(t.key) in set(wanted_ok)]

    tasks.sort(key=lambda t: (t.priority, t.key))
    if not tasks:
        logger.warning("No tasks selected.")
        return 0

    workers = max(1, min(workers, len(tasks)))

    summary = RunSummary()
    summary.total_tasks = len(tasks)
    t0 = time.perf_counter()

    backend = BackendClient(backend_url, timeout_sec=timeout_sec, token=token)
    sheets = SheetsWriter()

    lock_name = (
        f"{spreadsheet_id}:{','.join([_canon_key(t.key) for t in tasks])}"
    )
    lock = RedisLock(lock_name, ttl_sec=600)

    results: List[TaskResult] = []
    try:
        for hp in ("/readyz", "/health", "/livez"):
            data, err, _code = await backend.get_json(hp)
            if err:
                logger.info("Backend preflight %s -> %s", hp, err)
                continue
            status_val = (data or {}).get("status") if isinstance(data, dict) else None
            logger.info("Backend preflight %s -> %s", hp, status_val or "ok")
            break

        acquired = True if args.no_lock else await lock.acquire()
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

        out = await asyncio.gather(
            *[_guarded(t) for t in tasks], return_exceptions=True
        )

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

        logger.info("=" * 60)
        logger.info(
            "SYNC DONE | success=%d partial=%d failed=%d skipped=%d "
            "| rows_written=%d | duration_ms=%.2f",
            summary.success,
            summary.partial,
            summary.failed,
            summary.skipped,
            summary.total_rows_written,
            summary.duration_ms,
        )

        for r in results:
            if r.status == "success":
                logger.info(
                    "✅ %s -> %s | rows=%d | %.1fms",
                    _canon_key(r.key), r.sheet_name, r.rows_written, r.duration_ms,
                )
            elif r.status == "partial":
                logger.info(
                    "⚠️  %s -> %s | rows=%d failed=%d | %.1fms | %s",
                    _canon_key(r.key),
                    r.sheet_name,
                    r.rows_written,
                    r.rows_failed,
                    r.duration_ms,
                    "; ".join(r.warnings[:2]),
                )
            elif r.status == "failed":
                logger.info(
                    "❌ %s -> %s | %s",
                    _canon_key(r.key), r.sheet_name, r.error or "failed",
                )
            else:
                logger.info(
                    "⏭️  %s -> %s | %s",
                    _canon_key(r.key),
                    r.sheet_name,
                    "; ".join(r.warnings[:2]) if r.warnings else "skipped",
                )

        if args.json_out:
            report = {
                "summary": summary.to_dict(),
                "results": [x.to_dict() for x in results],
            }
            Path(args.json_out).write_text(
                json.dumps(_coerce_jsonable(report), indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            logger.info("Report saved: %s", args.json_out)

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
        return 2


# =============================================================================
# Worker integration API (FIX v6.4.0)
# =============================================================================

# Documents the expected task payload schema for worker.py.
# When worker.py dispatches task_type="dashboard_sync", it should send a dict
# matching these fields. Previously undocumented, causing silent schema drift.
WORKER_PAYLOAD_SCHEMA: Dict[str, Any] = {
    "task_type": "dashboard_sync",   # (required) always "dashboard_sync"
    "keys": [],                      # (optional) list[str] — subset of ALLOWED_KEYS to sync
                                     #   e.g. ["MARKET_LEADERS", "GLOBAL_MARKETS"]
                                     #   omit or empty = sync all default tasks
    "spreadsheet_id": "",            # (optional) override DEFAULT_SPREADSHEET_ID
    "backend_url": "",               # (optional) override BACKEND_BASE_URL
    "start_cell": "A5",              # (optional) header start cell, default A5
    "max_symbols": -1,               # (optional) -1 = use per-task defaults
    "workers": 4,                    # (optional) parallel task workers
    "clear_before_write": False,     # (optional) clear sheet range before writing
    "dry_run": False,                # (optional) fetch data but don't write sheets
    "timeout_sec": 30,               # (optional) backend request timeout
    "no_lock": False,                # (optional) skip Redis distributed lock
    # --- worker tracing fields (added by worker.py, not required here) ---
    "task_id": "",                   # worker task UUID for correlation
    "queued_at": "",                 # ISO timestamp when task was queued
    "retry_count": 0,                # number of times this task has been retried
}


async def run_from_worker_payload_async(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Programmatic entry point for worker.py (async version).

    FIX v6.4.0: In v6.3.0, worker.py had to call main_async() by constructing
    a fake argparse.Namespace. This broke whenever new --args were added to the
    CLI parser. Now worker.py calls this function directly with the task payload.

    Args:
        payload: Dict matching WORKER_PAYLOAD_SCHEMA.

    Returns:
        Dict with keys: status, exit_code, summary, errors.
        status: "success" | "partial" | "failed"
        exit_code: 0 | 1 | 2
    """
    task_id = str(payload.get("task_id") or uuid.uuid4())
    logger.info("run_from_worker_payload: task_id=%s", task_id)

    task_type = str(payload.get("task_type") or "").strip()
    if task_type and task_type != "dashboard_sync":
        return {
            "status": "failed",
            "exit_code": 2,
            "errors": [
                f"run_from_worker_payload: expected task_type='dashboard_sync', "
                f"got {task_type!r}"
            ],
            "summary": {},
            "task_id": task_id,
        }

    argv: List[str] = []

    spreadsheet_id = str(payload.get("spreadsheet_id") or "").strip()
    if spreadsheet_id:
        argv += ["--sheet-id", spreadsheet_id]

    backend_url = str(payload.get("backend_url") or "").strip()
    if backend_url:
        argv += ["--backend", backend_url]

    keys = payload.get("keys") or []
    if isinstance(keys, list) and keys:
        argv += ["--keys"] + [str(k) for k in keys]
    elif isinstance(keys, str) and keys.strip():
        argv += ["--keys", keys.strip()]

    start_cell = str(payload.get("start_cell") or "A5").strip()
    argv += ["--start-cell", start_cell]

    max_symbols = payload.get("max_symbols", -1)
    argv += ["--max-symbols", str(max_symbols)]

    workers = payload.get("workers", 4)
    argv += ["--workers", str(workers)]

    timeout_sec = payload.get("timeout_sec", 30)
    argv += ["--timeout", str(timeout_sec)]

    if payload.get("clear_before_write"):
        argv.append("--clear")

    if payload.get("dry_run"):
        argv.append("--dry-run")

    if payload.get("no_lock"):
        argv.append("--no-lock")

    try:
        exit_code = await main_async(argv)
        status = "success" if exit_code == 0 else ("partial" if exit_code == 1 else "failed")
        return {
            "status": status,
            "exit_code": exit_code,
            "errors": [],
            "summary": {},
            "task_id": task_id,
        }
    except Exception as e:
        logger.exception("run_from_worker_payload failed: %s", e)
        return {
            "status": "failed",
            "exit_code": 2,
            "errors": [str(e)],
            "summary": {},
            "task_id": task_id,
        }


def run_from_worker_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sync wrapper for worker.py (which runs in a sync context).
    Calls run_from_worker_payload_async() via asyncio.run().
    """
    try:
        return asyncio.run(run_from_worker_payload_async(payload))
    except Exception as e:
        return {
            "status": "failed",
            "exit_code": 2,
            "errors": [str(e)],
            "summary": {},
            "task_id": str(payload.get("task_id") or ""),
        }


if __name__ == "__main__":
    sys.exit(main())
