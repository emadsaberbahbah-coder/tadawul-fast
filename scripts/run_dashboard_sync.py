#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/run_dashboard_sync.py
================================================================================
TADAWUL FAST BRIDGE — DASHBOARD SYNC RUNNER (v6.6.0)
================================================================================
PRODUCTION-HARDENED | ASYNC | NON-BLOCKING | COMPILEALL-SAFE | SCHEMA-FIRST

v6.6.0 fix — decision-owned (cockpit) page guard (Top_10 clobber prevention)
- WHY: Top_10_Investments is a DECISION-OWNED page — the user records BUY /
  decision state in its decision columns (the cockpit), and data_engine_v2
  already serves a FRESH Top_10 on demand via the route (advanced_analysis ->
  top10_selector.build_top10_rows). GAS protects the page from refresh-overwrite
  with isDecisionOwnedPage_ (00_Config.gs), but the Python daily sync had a
  TOP_10_INVESTMENTS write task that bypassed that guard: with clear-before-write
  the default (v6.4.0), every cycle CLEARED the sheet and rewrote it WITHOUT the
  user's decision cells — clobbering the cockpit's decisions daily. A cross-layer
  gap: the guard existed in GAS but had no Python-side enforcement.
- FIX: a Python-side mirror of isDecisionOwnedPage_. A decision-owned page is
  SKIPPED in the Hard-filters block — BEFORE the symbol read, the backend fetch
  (the expensive selector build), the clear, and the write — so nothing is
  fetched, cleared, or written for it. The page's last-good rows + the user's
  decisions are left intact, and it refreshes on demand via the route.
- WHY PAGE-LEVEL SKIP (not the column-merge of the v6.5.0 My_Portfolio guard):
  the WHOLE Top_10 page is cockpit-owned and is re-derivable on demand by the
  engine, so the sync has no business writing any of it — unlike My_Portfolio,
  whose manual INPUT columns must be preserved while the rest is refreshed.
- SCOPE / SAFETY:
    * Applies to Top_10_Investments only; every other page is byte-for-byte
      unchanged. status="skipped" (NOT partial), so the daily exit code stays 0.
    * Gated by TFB_SYNC_DECISION_GUARD (default ON; set 0/false/off/no to restore
      the v6.5.0 write-through of decision pages exactly).
    * Pages overridable via TFB_SYNC_DECISION_GUARD_PAGES (comma-separated list).
    * Check the "[v6.6.0 DECISION-GUARD]" log line for the per-page skip reason.
- UNCHANGED: every endpoint, payload, the My_Portfolio guard, other task
  definitions, matrix rectification, credential loading, exit codes, the
  clear-before-write default, and the schema-agnostic write path.

v6.5.0 fix — My_Portfolio manual-cell write guard (irreversible-loss prevention)
- WHY: My_Portfolio carries user-authored ("manual") inputs that live ONLY in
  the sheet and are NEVER re-derivable from a market feed — position quantity
  and average cost (and, downstream, the position math computed from them). The
  backend echoes those cells back in the sync payload after reading them via the
  engine's sheet rows-reader. If that upstream read transiently misses (a Sheets
  API hiccup, a cold reader), the payload returns those manual cells BLANK while
  the live sheet still holds the real values. A normal write then overwrites the
  user's real Qty/Avg Cost with blanks — irreversible data loss.
- FIX: before writing My_Portfolio (and ONLY My_Portfolio), the runner now
  independently re-reads the live sheet and checks whether any symbol that
  currently HAS manual data (Qty / Avg Cost) would be regressed to BLANK by the
  outgoing payload. If so — or if that verification read itself cannot be
  trusted — the write is SKIPPED for this cycle (status=partial + warning).
  Nothing is cleared, nothing is written; the existing row (manual inputs AND
  the computed columns derived from them) is preserved whole and self-heals on
  the next healthy sync.
- WHY WHOLE-ROW SKIP (not per-cell merge): the upstream rows-reader reads the
  grid in a single call — it gets every row or none. On a miss, the manual
  inputs AND their computed columns (position value / unrealized P&L) blank out
  together. A per-cell merge would keep Qty/Avg Cost but still write a BLANK
  position value against a FRESH price — a misleading, internally-inconsistent
  half-row. Skipping the whole write keeps the row consistent and correct.
- SCOPE / SAFETY:
    * Applies to My_Portfolio only; every other page is byte-for-byte unchanged.
    * Gated by TFB_SYNC_MANUAL_GUARD (default ON; set 0/false/off to disable —
      disabling restores pre-v6.5.0 write-through behavior exactly).
    * Pages overridable via TFB_SYNC_MANUAL_GUARD_PAGES (comma-separated list).
    * Fail-safe: any uncertainty (read error, unmappable header/symbol column,
      missing manual columns on the payload) skips the write to protect existing
      data — the guard NEVER writes blind. A persistently-skipping My_Portfolio
      therefore means the guard is protecting data, not losing it; check the
      "[v6.5.0 PORTFOLIO-GUARD]" log line for the specific reason.
    * Robust to layout: the verification read locates the header row by content
      (symbol + manual columns), so a header at row 1 OR at the A5 default with
      title rows above are both handled, and column reorder is tolerated via
      normalized header-name matching.
- UNCHANGED: every endpoint, payload, task definition, matrix rectification,
  credential loading, exit codes, the clear-before-write default, and the
  schema-agnostic write path.

v6.4.0 fix — clear-before-write is now the DEFAULT (ghost/stale-row root cause)
- ROOT CAUSE: write_table() writes via Sheets values.update, which overwrites
  cells IN PLACE and NEVER truncates trailing rows/columns. Clearing was gated
  behind the opt-in --clear flag (default OFF), and the production daily_sync
  workflow never passes it. So whenever a refresh wrote FEWER rows than the
  prior run (e.g. Top_10_Investments returning 3 rows after a previous 8-row
  write) or FEWER columns than a stale wider write, the leftover rows/columns
  survived as "ghosts": stale Top 10 picks (the 5 leftover rows) and the
  trailing ghost "Status" columns observed on Global_Markets.
- FIX: clear-before-write is now the DEFAULT. The per-task clear is driven by a
  new --no-clear opt-OUT (default: clear ON) in place of the old --clear
  opt-IN. clear_from() already clears {col}{row}:ZZ — full column width AND all
  rows to the bottom — so one default-on clear removes BOTH stale rows and
  ghost columns on every page. No other logic changed.
- BACKWARD COMPAT: --clear is still accepted (now redundant/deprecated) so any
  existing cron that passes it keeps working; --no-clear restores the old
  opt-in (append/preserve) behavior for a run that genuinely wants it.
- UNCHANGED: every endpoint, payload, task definition, matrix rectification,
  credential loading, exit codes, and the schema-agnostic write path.

v6.3.0 fixes (targets your recurring ❌ causes)
- ✅ Sheets-safe ALWAYS: backend rows (dicts or lists) -> strict 2D matrix (pads/truncates to header length)
- ✅ JSON-safe value coercion for Google API (datetime/Enum/set/etc -> primitives)
- ✅ Key parsing is robust: --keys supports space, comma, semicolon, JSON array-like tokens
- ✅ Stronger backend compatibility: sends sheet/sheet_name/page/name/tab + tickers/symbols + request_id
- ✅ Health preflight probes /readyz + /health + /livez (best-effort)
- ✅ Credentials loader hardened: supports GOOGLE_APPLICATION_CREDENTIALS file + env JSON + env base64; fixes "\\n" private_key
- ✅ Never runs forbidden legacy keys (KSA_TADAWUL / ADVISOR_CRITERIA)
- ✅ Deterministic exit codes:
    0 = all success
    1 = partial (some partial/skipped) but no hard failures
    2 = one or more failed

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
SCRIPT_VERSION = "6.6.0"

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
    # pydantic-ish
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
        # JSON array
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
        # split by common separators
        parts = re.split(r"[,\s;|]+", s)
        for p in parts:
            pp = (p or "").strip()
            if pp:
                flat.append(pp)
    # canonicalize + de-dup
    out: List[str] = []
    seen: set[str] = set()
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

    async def post_json(self, path: str, payload: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
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
        raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or "").strip()

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
        # Always quote if not safe
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

        # Ensure rectangular rows matching header length (Sheets-friendly)
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

    def read_values(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        a1_range: str = "A1:EZ2000",
    ) -> Optional[List[List[Any]]]:
        """
        Read a rectangular block of UNFORMATTED cell values from a sheet.

        Returns the list of rows on success (possibly an empty list when the
        sheet/range holds no data), or None on ANY failure (no service, API
        error) so callers can distinguish 'sheet is empty' (->[]) from 'read
        could not be performed' (->None). The write service account has full
        spreadsheets scope (read + write), so this reuses the same service the
        writer already builds.
        """
        svc = self._get_service()
        if not svc:
            return None
        try:
            rng = f"{self._safe_sheet_a1(sheet_name)}!{a1_range}"
            resp = svc.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=rng,
                majorDimension="ROWS",
                valueRenderOption="UNFORMATTED_VALUE",
            ).execute()
            vals = resp.get("values", [])
            return vals if isinstance(vals, list) else []
        except Exception:
            return None


# -----------------------------------------------------------------------------
# My_Portfolio manual-cell write guard (v6.5.0)
#
# Prevents an upstream read miss (blank Qty/Avg Cost in the payload) from
# overwriting the user's real, irreplaceable manual inputs on the live sheet.
# Degraded-payload detection -> whole-write skip. See module docstring for the
# full rationale. Fail-safe: any uncertainty skips the write to protect data.
# -----------------------------------------------------------------------------
_GUARD_TAG = "[v6.5.0 PORTFOLIO-GUARD]"

# Default page(s) the guard protects. Overridable via env (comma list).
_GUARD_DEFAULT_PAGES = ("My_Portfolio",)

# High-confidence, unambiguously user-authored columns used as the degradation
# sentinel. Deliberately limited to the position-math INPUTS (quantity +
# average cost): their blanking is the exact symptom of an upstream read miss,
# and they are never produced by a market feed (so a fresh payload that has
# them blank — while the sheet still holds them — is a reliable failure signal).
_GUARD_SENTINEL_ALIASES = frozenset({
    # quantity
    "qty", "positionqty", "quantity", "positionquantity", "shares", "units",
    # average cost / entry price
    "avgcost", "averagecost", "avgcostprice", "positionavgcost",
    "avgprice", "averageprice", "costbasis", "avgbuyprice", "averagebuyprice",
})

# Symbol/identifier column aliases (for row matching across payload <-> sheet).
_GUARD_SYMBOL_ALIASES = frozenset({
    "symbol", "ticker", "tickersymbol", "symbolticker", "code", "instrument",
})

# -----------------------------------------------------------------------------
# Decision-owned (cockpit) page guard (v6.6.0)
# -----------------------------------------------------------------------------
# Python-side mirror of the GAS isDecisionOwnedPage_ guard (00_Config.gs). A
# decision-owned page (Top_10_Investments) carries cockpit-authored decision
# columns AND is served fresh on demand by data_engine_v2 via the route, so the
# daily sync must NOT write (and clear) it — doing so blanks the user's
# decisions every cycle. Unlike the column-level My_Portfolio guard, the WHOLE
# page is owned, so the guard is a page-level SKIP taken before any fetch/write.
_DECISION_GUARD_TAG = "[v6.6.0 DECISION-GUARD]"

# Default decision-owned page(s). Overridable via env (comma list).
_DECISION_GUARD_DEFAULT_PAGES = ("Top_10_Investments",)


def _guard_norm(s: Any) -> str:
    """Lowercase + strip non-alphanumerics (matches rows_reader normalization)."""
    return re.sub(r"[^a-z0-9]+", "", str(s if s is not None else "").lower())


def _guard_is_blank(v: Any) -> bool:
    """A cell is blank iff it is None or a whitespace-only string. 0 is NOT blank."""
    if v is None:
        return True
    if isinstance(v, str):
        return v.strip() == ""
    return False


def _guard_pages() -> set:
    raw = (os.getenv("TFB_SYNC_MANUAL_GUARD_PAGES") or "").strip()
    pages = [p.strip() for p in raw.split(",") if p.strip()] if raw else list(_GUARD_DEFAULT_PAGES)
    return {_guard_norm(p) for p in pages}


def _guard_enabled() -> bool:
    return (os.getenv("TFB_SYNC_MANUAL_GUARD") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _guard_should_apply(sheet_name: str) -> bool:
    """True iff the guard is enabled AND this page is in the protected set."""
    if not _guard_enabled():
        return False
    return _guard_norm(sheet_name) in _guard_pages()


def _decision_guard_enabled() -> bool:
    """Decision-owned-page guard master switch. Default ON; set
    TFB_SYNC_DECISION_GUARD=0/false/off/no to restore the v6.5.0 behavior
    (the daily sync writes decision-owned pages again)."""
    return (os.getenv("TFB_SYNC_DECISION_GUARD") or "1").strip().lower() not in {"0", "false", "off", "no"}


def _decision_guard_pages() -> set:
    """Decision-owned (cockpit) page set. Overridable via
    TFB_SYNC_DECISION_GUARD_PAGES (comma-separated); defaults to
    Top_10_Investments."""
    raw = (os.getenv("TFB_SYNC_DECISION_GUARD_PAGES") or "").strip()
    pages = [p.strip() for p in raw.split(",") if p.strip()] if raw else list(_DECISION_GUARD_DEFAULT_PAGES)
    return {_guard_norm(p) for p in pages}


def _decision_guard_should_skip(sheet_name: str) -> bool:
    """True iff the decision-owned-page guard is enabled AND this page is
    cockpit/decision-owned. Python-side mirror of the GAS isDecisionOwnedPage_
    guard: the daily sync must not write (and clear) a page the user owns, or
    it blanks the cockpit's decision cells."""
    if not _decision_guard_enabled():
        return False
    return _guard_norm(sheet_name) in _decision_guard_pages()


def _guard_find_col(header_row: List[Any], aliases: frozenset) -> int:
    """Index of the first header whose normalized name is in aliases, else -1."""
    for i, h in enumerate(header_row or []):
        if _guard_norm(h) in aliases:
            return i
    return -1


def _guard_find_header_row(grid: List[List[Any]]) -> int:
    """
    Locate the header row within the first rows of a sheet read. Robust to any
    title/branding rows above the header (e.g. a header written at the A5
    default). The header is the first row that contains BOTH a symbol column and
    at least one sentinel (manual) column. Returns the row index, or -1.
    """
    scan = min(len(grid or []), 15)
    for r in range(scan):
        row = grid[r] if isinstance(grid[r], list) else []
        if _guard_find_col(row, _GUARD_SYMBOL_ALIASES) >= 0 and _guard_find_col(row, _GUARD_SENTINEL_ALIASES) >= 0:
            return r
    return -1


def _portfolio_write_guard(
    sheets: "SheetsWriter",
    spreadsheet_id: str,
    sheet_name: str,
    headers: List[Any],
    rows_matrix: List[List[Any]],
) -> Tuple[bool, str]:
    """
    Decide whether it is safe to write a manual-input page (My_Portfolio) now.

    Returns (allow_write, note):
      - (True,  "")    -> safe; proceed with the normal write.
      - (True,  note)  -> safe; proceed; note is informational only.
      - (False, note)  -> NOT safe; SKIP the write to protect manual cells.

    The guard reads the live sheet independently of the engine's reader and
    refuses the write if any symbol that currently holds Qty/Avg Cost would be
    blanked by the outgoing payload, or if the verification read cannot be
    trusted (fail-safe -> skip, never write blind).
    """
    # Locate sentinel + symbol columns on the OUTGOING payload.
    out_sym_idx = _guard_find_col(headers, _GUARD_SYMBOL_ALIASES)
    out_sentinels = [i for i, h in enumerate(headers or []) if _guard_norm(h) in _GUARD_SENTINEL_ALIASES]
    if out_sym_idx < 0 or not out_sentinels:
        return (False, f"{_GUARD_TAG} skip: outgoing {sheet_name} payload is missing a symbol or manual (Qty/Avg Cost) column; write skipped to protect manual cells.")

    # Read the live sheet (independent of the engine's reader path).
    grid = sheets.read_values(spreadsheet_id, sheet_name) if sheets is not None else None
    if grid is None:
        return (False, f"{_GUARD_TAG} skip: could not read live {sheet_name} to verify manual cells; write skipped to protect data.")
    if not grid:
        # Read succeeded but sheet is empty (first write) -> nothing to lose.
        return (True, "")

    hdr_idx = _guard_find_header_row(grid)
    if hdr_idx < 0:
        return (False, f"{_GUARD_TAG} skip: could not locate a header row in live {sheet_name}; write skipped to protect data.")

    ex_header = grid[hdr_idx] if isinstance(grid[hdr_idx], list) else []
    ex_sym_idx = _guard_find_col(ex_header, _GUARD_SYMBOL_ALIASES)
    if ex_sym_idx < 0:
        return (False, f"{_GUARD_TAG} skip: live {sheet_name} header has no symbol column; write skipped to protect data.")

    # Map existing sentinel columns by normalized header name so the comparison
    # is like-for-like even if column ORDER differs between writes.
    ex_sentinel_by_norm: Dict[str, int] = {}
    for i, h in enumerate(ex_header):
        n = _guard_norm(h)
        if n in _GUARD_SENTINEL_ALIASES and n not in ex_sentinel_by_norm:
            ex_sentinel_by_norm[n] = i

    # Build {SYMBOL -> {sentinel_norm -> populated?}} from existing data rows.
    existing: Dict[str, Dict[str, bool]] = {}
    for r in range(hdr_idx + 1, len(grid)):
        row = grid[r] if isinstance(grid[r], list) else []
        if ex_sym_idx >= len(row):
            continue
        sym = str(row[ex_sym_idx]).strip().upper()
        if not sym:
            continue
        flags: Dict[str, bool] = {}
        for n, ci in ex_sentinel_by_norm.items():
            val = row[ci] if ci < len(row) else None
            flags[n] = not _guard_is_blank(val)
        existing[sym] = flags

    if not existing:
        # No existing holdings carry manual data -> nothing to lose.
        return (True, "")

    # Normalized name for each outgoing sentinel column (for like-for-like cmp).
    out_sentinel_norm = {i: _guard_norm(headers[i]) for i in out_sentinels}

    regressed: List[str] = []
    for row in rows_matrix or []:
        if out_sym_idx >= len(row):
            continue
        sym = str(row[out_sym_idx]).strip().upper()
        if not sym or sym not in existing:
            continue
        ex_flags = existing[sym]
        for i, n in out_sentinel_norm.items():
            new_blank = _guard_is_blank(row[i]) if i < len(row) else True
            if new_blank and ex_flags.get(n, False):
                regressed.append(sym)
                break

    if regressed:
        uniq = sorted(set(regressed))
        shown = ", ".join(uniq[:8]) + (" …" if len(uniq) > 8 else "")
        return (False, f"{_GUARD_TAG} skip: outgoing payload would blank existing Qty/Avg Cost for {len(uniq)} holding(s) [{shown}]; write skipped to protect manual cells (self-heals on next healthy sync).")

    return (True, "")


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
    gw = (gw or "enriched").strip().lower()
    # include ai aliases because route naming can vary
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

    # empty rows, but headers exist
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
    res = TaskResult(key=task.key, sheet_name=task.sheet_name, status="pending", start_utc=_utc_now().isoformat())

    try:
        canon_task_key = _canon_key(task.key)

        # Hard filters
        if _is_forbidden_key(canon_task_key):
            res.status = "skipped"
            res.warnings.append("Forbidden legacy key; skipped.")
            return res
        if canon_task_key not in _ALLOWED_KEYS:
            res.status = "skipped"
            res.warnings.append(f"Unknown key {canon_task_key}; skipped.")
            return res

        # Decision-owned (cockpit) page guard (v6.6.0): Top_10_Investments is
        # owned by the cockpit — the user records BUY / decision state in its
        # decision columns, and data_engine_v2 serves a fresh Top_10 on demand
        # via the route, so the daily sync must NOT write (and clear) this page
        # or it blanks those decisions every cycle. Python-side mirror of the
        # GAS isDecisionOwnedPage_ guard (00_Config.gs); previously the guard
        # lived only in GAS and the sync bypassed it. Skip is taken HERE, before
        # the symbol read / backend fetch / write, so nothing is fetched,
        # cleared, or written. status="skipped" (not partial) keeps the daily
        # exit code at 0. Reversible: TFB_SYNC_DECISION_GUARD=0 restores the
        # v6.5.0 write (pages overridable via TFB_SYNC_DECISION_GUARD_PAGES).
        if _decision_guard_should_skip(task.sheet_name):
            res.status = "skipped"
            note = (
                f"{_DECISION_GUARD_TAG} {task.sheet_name} is decision-owned "
                f"(cockpit); daily sync write skipped to protect decision cells "
                f"— it refreshes on demand via the route. Set "
                f"TFB_SYNC_DECISION_GUARD=0 to override."
            )
            res.warnings.append(note)
            logger.info(note)
            return res

        max_syms = max_symbols_override if max_symbols_override >= 0 else task.max_symbols

        symbols: List[str] = []
        if max_syms != 0:
            symbols = _read_symbols(canon_task_key, spreadsheet_id, max_syms)

        res.symbols_requested = len(symbols)

        # Dry run: still success-ish but no backend call and no write
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

        # Some handlers clamp limit >= 1, so never send 0
        safe_limit = 1 if not symbols else min(5000, max(1, len(symbols)))

        payload: Dict[str, Any] = {
            # identifiers (compat)
            "sheet": task.sheet_name,
            "sheet_name": task.sheet_name,
            "page": task.sheet_name,
            "name": task.sheet_name,
            "tab": task.sheet_name,
            # symbols
            "tickers": symbols,
            "symbols": symbols,
            # behavior
            "refresh": True,
            "include_meta": True,
            "include_matrix": True,
            "limit": safe_limit,
            # tracing
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

        # No creds => partial (data fetched but not written)
        if sheets is None or sheets._get_service() is None:
            res.status = "partial"
            res.warnings.append("No Google Sheets credentials. Backend data fetched but not written.")
            res.rows_written = 0
            res.rows_failed = len(rows_matrix or [])
            return res

        # --- My_Portfolio manual-cell write guard (v6.5.0) -------------------
        # Independently verify this write will not blank user-authored Qty/Avg
        # Cost on the live sheet. On ANY doubt, skip the write (the existing row
        # is preserved whole and self-heals on the next healthy sync). Placed
        # BEFORE the clear/write so a skip performs neither — never clear-then-
        # skip. Scoped to manual pages; gated by TFB_SYNC_MANUAL_GUARD.
        if rows_matrix and _guard_should_apply(task.sheet_name):
            allow_write, guard_note = _portfolio_write_guard(
                sheets, spreadsheet_id, task.sheet_name, headers, rows_matrix
            )
            if guard_note:
                res.warnings.append(guard_note)
                logger.warning(guard_note)
            if not allow_write:
                res.status = "partial"
                res.rows_written = 0
                res.rows_failed = len(rows_matrix or [])
                return res
        # ---------------------------------------------------------------------

        if clear_before_write:
            try:
                sheets.clear_from(spreadsheet_id, task.sheet_name, start_cell)
            except Exception as e:
                res.warnings.append(f"Clear failed: {e}")

        try:
            written = sheets.write_table(spreadsheet_id, task.sheet_name, start_cell, headers, rows_matrix)
            res.rows_written = int(written)

            # schema-only (0 rows) => success
            if not rows_matrix:
                res.rows_failed = 0
                res.status = "success"
            else:
                res.rows_failed = max(0, len(rows_matrix) - res.rows_written)
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
    parser.add_argument("--keys", nargs="*", default=[], help="Specific keys (space/comma/semicolon/JSON-array supported)")
    parser.add_argument("--start-cell", default="A5", help="Top-left A1 cell where headers will be written (e.g. A5)")
    parser.add_argument("--max-symbols", default="-1", help="Override max symbols for all tasks (-1 = per task default)")
    parser.add_argument("--workers", default="4", help="Parallel workers")
    parser.add_argument("--clear", action="store_true", help="(Deprecated — clear is now the default) Clear from start-cell down before writing.")
    parser.add_argument("--no-clear", action="store_true", help="Disable clear-before-write. NOT recommended: leaves stale trailing rows/columns from prior shorter writes (the ghost-row cause). Use only for deliberate append/preserve runs.")
    parser.add_argument("--dry-run", action="store_true", help="Do not call backend or write sheets")
    parser.add_argument("--no-lock", action="store_true", help="Disable Redis lock even if REDIS_URL exists")
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
        logger.warning("No backend token found (TFB_TOKEN/X_APP_TOKEN/APP_TOKEN/BACKEND_TOKEN). Requests may 401 if protected.")

    tasks = _default_tasks()

    wanted = _parse_keys_tokens(args.keys or [])
    forbidden_requested = [k for k in wanted if _is_forbidden_key(k)]
    if forbidden_requested:
        logger.warning("Forbidden keys requested and will be ignored: %s", ", ".join(forbidden_requested))

    wanted_ok = [k for k in wanted if (k in _ALLOWED_KEYS and not _is_forbidden_key(k))]
    if wanted_ok:
        tasks = [t for t in tasks if _canon_key(t.key) in set(wanted_ok)]

    tasks.sort(key=lambda t: (t.priority, t.key))
    if not tasks:
        logger.warning("No tasks selected.")
        return 0

    # clamp workers to tasks count
    workers = max(1, min(workers, len(tasks)))

    summary = RunSummary()
    summary.total_tasks = len(tasks)
    t0 = time.perf_counter()

    backend = BackendClient(backend_url, timeout_sec=timeout_sec, token=token)
    sheets = SheetsWriter()

    lock_name = f"{spreadsheet_id}:{','.join([_canon_key(t.key) for t in tasks])}"
    lock = RedisLock(lock_name, ttl_sec=600)

    results: List[TaskResult] = []
    try:
        # Preflight health (best-effort)
        for hp in ("/readyz", "/health", "/livez"):
            data, err, _code = await backend.get_json(hp)
            if err:
                logger.info("Backend preflight %s -> %s", hp, err)
                continue
            status_val = (data or {}).get("status") if isinstance(data, dict) else None
            logger.info("Backend preflight %s -> %s", hp, status_val or "ok")
            break

        # Acquire lock
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
                    clear_before_write=(not bool(args.no_clear)),
                    dry_run=bool(args.dry_run),
                    backend=backend,
                    sheets=sheets,
                )

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
                logger.info("✅ %s -> %s | rows=%d | %.1fms", _canon_key(r.key), r.sheet_name, r.rows_written, r.duration_ms)
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
                logger.info("❌ %s -> %s | %s", _canon_key(r.key), r.sheet_name, r.error or "failed")
            else:
                logger.info("⏭️  %s -> %s | %s", _canon_key(r.key), r.sheet_name, "; ".join(r.warnings[:2]) if r.warnings else "skipped")

        if args.json_out:
            report = {"summary": summary.to_dict(), "results": [x.to_dict() for x in results]}
            Path(args.json_out).write_text(json.dumps(_coerce_jsonable(report), indent=2, ensure_ascii=False), encoding="utf-8")
            logger.info("Report saved: %s", args.json_out)

        # Exit codes
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


if __name__ == "__main__":
    raise SystemExit(main())
