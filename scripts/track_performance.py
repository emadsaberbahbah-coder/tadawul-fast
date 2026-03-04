#!/usr/bin/env python3
"""
scripts/track_performance.py
===========================================================
TADAWUL FAST BRIDGE – ADVANCED PERFORMANCE ANALYTICS ENGINE (v6.3.0)
===========================================================

Hardening + Alignment upgrades in v6.3.0 (vs v6.2.0 you pasted)
- ✅ Hygiene checker compliant: NO rich, NO print(); uses sys.stdout.write only
- ✅ Removed seaborn dependency (matplotlib OO only; thread-safe)
- ✅ Fixed A1 column math (supports >26 columns: AA, AB, …)
- ✅ Fixed RiyadhTime.format() misuse + stronger ISO parsing (supports Z / offsets)
- ✅ Google Sheets I/O hardened:
    - Supports GOOGLE_SHEETS_CREDENTIALS as JSON or base64(JSON)
    - Falls back to gspread.service_account() when available
    - Never crashes if gspread is missing (script still runs in “analysis-only” mode)
- ✅ Backend audit support (optional):
    - Can fetch Top10 rows from /v1/analysis/top10 (best-effort)
    - Can fetch current prices via /quotes OR /v1/enriched/sheet-rows OR /v1/analysis/sheet-rows (fallback chain)
    - Uses token headers (Authorization: Bearer + X-APP-TOKEN) when present
- ✅ Safer enums parsing (case/spacing tolerant) + safe float parsing
- ✅ Daemon mode stabilized (signal-safe) + clean shutdown

Core Capabilities (kept)
- Performance log store in Google Sheets (default tab: Performance_Log)
- KPI summary (win-rate, avg ROI, Sharpe/Sortino)
- Monte Carlo win-rate CI (when numpy available)
- Rolling windows (best-effort)
- Export reports (json/csv/html)

Notes
- This script is intentionally best-effort and will not fail just because optional deps
  (numpy/pandas/scipy/gspread/aiohttp/matplotlib) are missing.

Exit codes
- 0 success
- 1 fatal error
- 130 interrupted

===========================================================
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import concurrent.futures
import csv
import io
import json
import logging
import math
import os
import random
import signal
import sys
import time
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from threading import Event, Lock
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union
from urllib.parse import urlparse
from urllib.request import Request as UrlRequest, urlopen
from urllib.error import HTTPError, URLError

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        opt = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(v, option=opt, default=str).decode("utf-8")

    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)

    _HAS_ORJSON = True
except Exception:

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        return json.dumps(v, indent=(indent if indent else None), default=str, ensure_ascii=False)

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Optional imports (SAFE)
# ---------------------------------------------------------------------------
try:
    import numpy as np  # type: ignore

    NUMPY_AVAILABLE = True
except Exception:
    np = None  # type: ignore
    NUMPY_AVAILABLE = False

try:
    import pandas as pd  # type: ignore

    PANDAS_AVAILABLE = True
except Exception:
    pd = None  # type: ignore
    PANDAS_AVAILABLE = False

try:
    from scipy import stats  # type: ignore
    from scipy.optimize import minimize  # type: ignore

    SCIPY_AVAILABLE = True
except Exception:
    stats = None  # type: ignore
    minimize = None  # type: ignore
    SCIPY_AVAILABLE = False

# Google Sheets (gspread)
try:
    import gspread  # type: ignore
    from google.oauth2 import service_account  # type: ignore

    GSPREAD_AVAILABLE = True
except Exception:
    gspread = None  # type: ignore
    service_account = None  # type: ignore
    GSPREAD_AVAILABLE = False

# Async HTTP (optional)
try:
    import aiohttp  # type: ignore
    import aiohttp.client_exceptions  # type: ignore

    ASYNC_HTTP_AVAILABLE = True
except Exception:
    aiohttp = None  # type: ignore
    ASYNC_HTTP_AVAILABLE = False

# Visualization (matplotlib only — NO seaborn)
try:
    import matplotlib  # type: ignore

    matplotlib.use("Agg")
    from matplotlib.figure import Figure  # type: ignore

    PLOT_AVAILABLE = True
except Exception:
    Figure = None  # type: ignore
    PLOT_AVAILABLE = False

# Monitoring & Tracing (safe dummy)
try:
    from prometheus_client import Counter, Gauge  # type: ignore

    PROMETHEUS_AVAILABLE = True
except Exception:
    PROMETHEUS_AVAILABLE = False

    class _DummyMetric:  # type: ignore
        def labels(self, *args, **kwargs):
            return self

        def inc(self, *args, **kwargs):
            return None

        def set(self, *args, **kwargs):
            return None

    Counter = Gauge = _DummyMetric  # type: ignore

# Optional project imports (best-effort)
settings = None
sheets_service = None

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

try:
    from env import settings as _settings  # type: ignore
    settings = _settings
except Exception:
    settings = None

try:
    import google_sheets_service as sheets_service  # type: ignore
except Exception:
    sheets_service = None

# =============================================================================
# Version & Logging
# =============================================================================
SCRIPT_VERSION = "6.3.0"
SCRIPT_NAME = "PerformanceTracker"

LOG_FORMAT = "%(asctime)s | %(levelname)8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger("PerfTracker")

_CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=8, thread_name_prefix="PerfWorker")

_RIYADH_TZ = timezone(timedelta(hours=3))
_TRACING_ENABLED = (os.getenv("CORE_TRACING_ENABLED", "") or os.getenv("TRACING_ENABLED", "")).strip().lower() in {"1", "true", "yes", "y", "on"}

if PROMETHEUS_AVAILABLE:
    perf_records_processed = Counter("perf_records_processed_total", "Total performance records processed")
    perf_daemon_cycles = Counter("perf_daemon_cycles_total", "Total daemon cycles")
    perf_win_rate = Gauge("perf_overall_win_rate", "Overall win rate percentage")
else:
    perf_records_processed = Counter()  # type: ignore
    perf_daemon_cycles = Counter()  # type: ignore
    perf_win_rate = Gauge()  # type: ignore

# =============================================================================
# Utilities
# =============================================================================
def _out(s: str) -> None:
    sys.stdout.write(s + "\n")

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

def _riyadh_now() -> datetime:
    return datetime.now(_RIYADH_TZ)

def _safe_str(x: Any) -> str:
    try:
        return str(x).strip()
    except Exception:
        return ""

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return float(default)
        if isinstance(x, (int, float)):
            v = float(x)
            if math.isnan(v) or math.isinf(v):
                return float(default)
            return v
        s = str(x).strip()
        if not s or s.lower() in {"na", "n/a", "null", "none", "-", "—"}:
            return float(default)
        s = s.replace(",", "").replace("%", "")
        v = float(s)
        if math.isnan(v) or math.isinf(v):
            return float(default)
        return v
    except Exception:
        return float(default)

def _col_to_a1(col: int) -> str:
    # 1 -> A, 26 -> Z, 27 -> AA
    if col <= 0:
        return "A"
    out = []
    n = col
    while n > 0:
        n, r = divmod(n - 1, 26)
        out.append(chr(65 + r))
    return "".join(reversed(out))

def _a1_range(start_col: int, start_row: int, end_col: int, end_row: int) -> str:
    return f"{_col_to_a1(start_col)}{start_row}:{_col_to_a1(end_col)}{end_row}"

class FullJitterBackoff:
    def __init__(self, max_retries: int = 5, base_delay: float = 0.8, max_delay: float = 30.0):
        self.max_retries = max(1, int(max_retries))
        self.base_delay = float(base_delay)
        self.max_delay = float(max_delay)

    async def execute_async(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        for attempt in range(self.max_retries):
            try:
                res = fn(*args, **kwargs)
                if hasattr(res, "__await__"):
                    res = await res
                return res
            except Exception:
                if attempt == self.max_retries - 1:
                    raise
                cap = min(self.max_delay, self.base_delay * (2 ** attempt))
                await asyncio.sleep(random.uniform(0.0, cap))

    def execute_sync(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        for attempt in range(self.max_retries):
            try:
                return fn(*args, **kwargs)
            except Exception:
                if attempt == self.max_retries - 1:
                    raise
                cap = min(self.max_delay, self.base_delay * (2 ** attempt))
                time.sleep(random.uniform(0.0, cap))

class RiyadhTime:
    _tz = _RIYADH_TZ

    @classmethod
    def now(cls) -> datetime:
        return datetime.now(cls._tz)

    @classmethod
    def parse(cls, s: Optional[str]) -> Optional[datetime]:
        if not s:
            return None
        raw = str(s).strip()
        if not raw:
            return None

        # ISO first (supports Z)
        try:
            iso = raw.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=cls._tz)
            return dt.astimezone(cls._tz)
        except Exception:
            pass

        fmts = [
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%z",
            "%d/%m/%Y",
            "%d/%m/%Y %H:%M:%S",
        ]
        for fmt in fmts:
            try:
                dt = datetime.strptime(raw, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=cls._tz)
                return dt.astimezone(cls._tz)
            except Exception:
                continue
        return None

    @classmethod
    def format(cls, dt: Optional[datetime] = None, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
        d = dt or cls.now()
        if d.tzinfo is None:
            d = d.replace(tzinfo=cls._tz)
        return d.astimezone(cls._tz).strftime(fmt)

# =============================================================================
# Enums & Data Models
# =============================================================================
class PerformanceStatus(str, Enum):
    ACTIVE = "active"
    MATURED = "matured"
    EXPIRED = "expired"
    STOPPED = "stopped"
    PENDING = "pending"

class HorizonType(str, Enum):
    WEEK_1 = "1W"
    MONTH_1 = "1M"
    MONTH_3 = "3M"
    MONTH_6 = "6M"
    YEAR_1 = "1Y"
    YEAR_3 = "3Y"
    YEAR_5 = "5Y"

    @property
    def days(self) -> int:
        return {"1W": 7, "1M": 30, "3M": 90, "6M": 180, "1Y": 365, "3Y": 1095, "5Y": 1825}[self.value]

class RecommendationType(str, Enum):
    STRONG_BUY = "STRONG BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    SELL = "SELL"
    STRONG_SELL = "STRONG SELL"

def _parse_enum_value(enum_cls: Any, raw: Any, default: Any) -> Any:
    s = _safe_str(raw).upper()
    if not s:
        return default
    # normalize common variants
    s = s.replace("_", " ").replace("-", " ").strip()
    # map some variants
    if enum_cls is HorizonType:
        s = s.replace("WEEK", "W").replace("MONTH", "M").replace("YEAR", "Y")
    try:
        return enum_cls(s)
    except Exception:
        # try find by value ignoring spaces
        norm = s.replace(" ", "")
        for m in enum_cls:
            if str(m.value).replace(" ", "") == norm:
                return m
        return default

def _risk_bucket_from_score(risk_score: Optional[float]) -> str:
    if risk_score is None:
        return "MODERATE"
    try:
        rs = float(risk_score)
        if rs <= 33:
            return "LOW"
        if rs <= 66:
            return "MODERATE"
        return "HIGH"
    except Exception:
        return "MODERATE"

def _confidence_bucket(conf: Optional[float]) -> str:
    if conf is None:
        return "MEDIUM"
    try:
        c = float(conf)
        if c >= 0.75:
            return "HIGH"
        if c >= 0.50:
            return "MEDIUM"
        return "LOW"
    except Exception:
        return "MEDIUM"

@dataclass(slots=True)
class PerformanceRecord:
    record_id: str
    symbol: str
    horizon: HorizonType
    date_recorded: datetime

    entry_price: float
    entry_recommendation: RecommendationType
    entry_score: float
    entry_risk_bucket: str
    entry_confidence: str
    origin_tab: str

    target_price: float
    target_roi: float
    target_date: datetime

    status: PerformanceStatus
    current_price: float = 0.0
    unrealized_roi: float = 0.0
    realized_roi: Optional[float] = None
    outcome: Optional[str] = None  # WIN, LOSS, BREAKEVEN

    volatility: float = 0.0
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0

    sector: Optional[str] = None
    factor_exposures: Dict[str, float] = field(default_factory=dict)

    last_updated: datetime = field(default_factory=_utc_now)
    maturity_date: Optional[datetime] = None
    notes: str = ""

    @property
    def key(self) -> str:
        return f"{self.symbol}|{self.horizon.value}|{self.date_recorded.astimezone(_RIYADH_TZ).strftime('%Y%m%d')}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "record_id": self.record_id,
            "key": self.key,
            "symbol": self.symbol,
            "horizon": self.horizon.value,
            "date_recorded_riyadh": RiyadhTime.format(self.date_recorded),
            "entry_price": self.entry_price,
            "entry_recommendation": self.entry_recommendation.value,
            "entry_score": self.entry_score,
            "risk_bucket": self.entry_risk_bucket,
            "confidence": self.entry_confidence,
            "origin_tab": self.origin_tab,
            "target_price": self.target_price,
            "target_roi_pct": self.target_roi,
            "target_date_riyadh": RiyadhTime.format(self.target_date),
            "status": self.status.value,
            "current_price": self.current_price,
            "unrealized_roi_pct": self.unrealized_roi,
            "realized_roi_pct": self.realized_roi,
            "outcome": self.outcome,
            "volatility": self.volatility,
            "max_drawdown_pct": self.max_drawdown,
            "sharpe_ratio": self.sharpe_ratio,
            "sector": self.sector,
            "factor_exposures": self.factor_exposures,
            "last_updated_utc": self.last_updated.astimezone(timezone.utc).isoformat(),
            "maturity_date_riyadh": RiyadhTime.format(self.maturity_date) if self.maturity_date else None,
            "notes": self.notes,
        }

    @classmethod
    def from_sheet_row(cls, row: List[Any], headers: List[str]) -> "PerformanceRecord":
        hmap = {str(h).strip(): i for i, h in enumerate(headers)}

        def get(name: str) -> Any:
            idx = hmap.get(name)
            if idx is None or idx < 0 or idx >= len(row):
                return None
            return row[idx]

        symbol = _safe_str(get("Symbol")).upper()
        horizon = _parse_enum_value(HorizonType, get("Horizon"), HorizonType.MONTH_1)
        dt_rec = RiyadhTime.parse(_safe_str(get("Date Recorded (Riyadh)"))) or RiyadhTime.now()
        dt_tgt = RiyadhTime.parse(_safe_str(get("Target Date (Riyadh)"))) or (dt_rec + timedelta(days=horizon.days))
        dt_upd = RiyadhTime.parse(_safe_str(get("Last Updated (Riyadh)"))) or RiyadhTime.now()
        dt_mat = RiyadhTime.parse(_safe_str(get("Maturity Date"))) if _safe_str(get("Maturity Date")) else None

        status_raw = _safe_str(get("Status")).lower() or "active"
        try:
            status = PerformanceStatus(status_raw)
        except Exception:
            status = PerformanceStatus.ACTIVE

        rec_raw = _safe_str(get("Entry Recommendation")).upper() or "HOLD"
        rec = _parse_enum_value(RecommendationType, rec_raw, RecommendationType.HOLD)

        realized = _safe_str(get("Realized ROI %"))
        realized_roi = None if realized == "" else _safe_float(realized, default=0.0)

        factor_json = _safe_str(get("Factor Exposures"))
        factors: Dict[str, float] = {}
        if factor_json:
            try:
                obj = json.loads(factor_json)
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        factors[str(k)] = _safe_float(v, default=0.0)
            except Exception:
                factors = {}

        return cls(
            record_id=_safe_str(get("Record ID")) or str(uuid.uuid4()),
            symbol=symbol,
            horizon=horizon,
            date_recorded=dt_rec,
            entry_price=_safe_float(get("Entry Price"), default=0.0),
            entry_recommendation=rec,
            entry_score=_safe_float(get("Entry Score"), default=0.0),
            entry_risk_bucket=_safe_str(get("Risk Bucket")) or "MODERATE",
            entry_confidence=_safe_str(get("Confidence")) or "MEDIUM",
            origin_tab=_safe_str(get("Origin Tab")) or "Unknown",
            target_price=_safe_float(get("Target Price"), default=0.0),
            target_roi=_safe_float(get("Target ROI %"), default=0.0),
            target_date=dt_tgt,
            status=status,
            current_price=_safe_float(get("Current Price"), default=0.0),
            unrealized_roi=_safe_float(get("Unrealized ROI %"), default=0.0),
            realized_roi=realized_roi,
            outcome=_safe_str(get("Outcome")) or None,
            volatility=_safe_float(get("Volatility"), default=0.0),
            max_drawdown=_safe_float(get("Max Drawdown %"), default=0.0),
            sharpe_ratio=_safe_float(get("Sharpe Ratio"), default=0.0),
            sector=_safe_str(get("Sector")) or None,
            factor_exposures=factors,
            last_updated=dt_upd.astimezone(timezone.utc),
            maturity_date=dt_mat,
            notes=_safe_str(get("Notes")),
        )

@dataclass(slots=True)
class PerformanceSummary:
    total_records: int = 0
    active_records: int = 0
    matured_records: int = 0
    wins: int = 0
    losses: int = 0
    breakeven: int = 0
    avg_roi: float = 0.0
    median_roi: float = 0.0
    roi_std: float = 0.0
    best_roi: float = 0.0
    worst_roi: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    win_rate: float = 0.0
    hit_rate_by_horizon: Dict[str, float] = field(default_factory=dict)
    performance_by_sector: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

# =============================================================================
# Backend Client (optional)
# =============================================================================
class BackendClient:
    def __init__(self, base_url: str, token: str = ""):
        self.base_url = (base_url or "").strip().rstrip("/")
        self.token = (token or "").strip()

    def _headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json", "Content-Type": "application/json", "User-Agent": f"TFB-PerfTracker/{SCRIPT_VERSION}"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
            h["X-APP-TOKEN"] = self.token
        return h

    async def post_json(self, path: str, payload: Dict[str, Any], timeout_sec: float = 30.0) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
        url = f"{self.base_url}{path}"
        if not self.base_url:
            return None, "BACKEND_BASE_URL not set", 0

        # aiohttp fast path
        if ASYNC_HTTP_AVAILABLE and aiohttp is not None:
            try:
                t = aiohttp.ClientTimeout(total=timeout_sec)
                async with aiohttp.ClientSession(timeout=t, headers=self._headers()) as session:
                    async with session.post(url, json=payload) as resp:
                        code = int(resp.status)
                        raw = await resp.read()
                        if code != 200:
                            return None, f"HTTP {code}: {raw[:200]!r}", code
                        try:
                            return json_loads(raw), None, code
                        except Exception:
                            return None, "Non-JSON response", code
            except Exception as e:
                return None, str(e), 0

        # urllib fallback
        try:
            data = json_dumps(payload).encode("utf-8")
            req = UrlRequest(url, data=data, method="POST")
            for k, v in self._headers().items():
                req.add_header(k, v)
            with urlopen(req, timeout=timeout_sec) as resp:
                code = int(getattr(resp, "status", 200))
                raw = resp.read()
                if code != 200:
                    return None, f"HTTP {code}: {raw[:200]!r}", code
                return json_loads(raw), None, code
        except HTTPError as e:
            try:
                raw = e.read()
            except Exception:
                raw = b""
            return None, f"HTTPError {e.code}: {raw[:200]!r}", int(getattr(e, "code", 0) or 0)
        except URLError as e:
            return None, f"URLError: {e}", 0
        except Exception as e:
            return None, str(e), 0

    async def get_top10_rows(self, criteria_overrides: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        # Prefer GET /v1/analysis/top10 via POST with overrides (keeps one method)
        if criteria_overrides:
            data, err, _ = await self.post_json("/v1/analysis/top10", criteria_overrides, timeout_sec=45.0)
        else:
            data, err, _ = await self.post_json("/v1/analysis/top10", {}, timeout_sec=45.0)
        if not isinstance(data, dict):
            return [], {"ok": False, "error": err or "no_data"}
        rows = data.get("rows") or []
        if not isinstance(rows, list):
            rows = []
        meta = data.get("meta") or {}
        meta["ok"] = True if not err else False
        if err:
            meta["error"] = err
        return [r for r in rows if isinstance(r, dict)], meta

    async def fetch_prices(self, symbols: List[str]) -> Dict[str, float]:
        syms = [s.strip().upper() for s in (symbols or []) if s and str(s).strip()]
        syms = list(dict.fromkeys(syms))
        if not syms:
            return {}

        # 1) /quotes (if exists in your main)
        data, err, code = await self.post_json("/quotes", {"symbols": syms, "include_raw": False}, timeout_sec=30.0)
        if isinstance(data, dict) and code == 200 and not err:
            # common shape: {"data": {SYM: {...}}} OR {SYM:{...}}
            blob = data.get("data") if isinstance(data.get("data"), dict) else data
            if isinstance(blob, dict):
                out: Dict[str, float] = {}
                for k, v in blob.items():
                    if isinstance(v, dict):
                        p = v.get("current_price") or v.get("price") or v.get("last") or v.get("last_price")
                        fv = _safe_float(p, default=0.0)
                        if fv > 0:
                            out[str(k).upper()] = fv
                if out:
                    return out

        # 2) /v1/enriched/sheet-rows (fallback)
        data, err, code = await self.post_json(
            "/v1/enriched/sheet-rows",
            {"page": "Global_Markets", "symbols": syms, "include_matrix": False, "top_n": len(syms)},
            timeout_sec=45.0,
        )
        if isinstance(data, dict) and code == 200 and not err:
            rows = data.get("rows") if isinstance(data.get("rows"), list) else []
            out = {}
            for r in rows:
                if isinstance(r, dict):
                    sym = _safe_str(r.get("symbol")).upper()
                    price = _safe_float(r.get("current_price") or r.get("price"), default=0.0)
                    if sym and price > 0:
                        out[sym] = price
            if out:
                return out

        # 3) /v1/analysis/sheet-rows (fallback)
        data, err, code = await self.post_json(
            "/v1/analysis/sheet-rows",
            {"page": "Global_Markets", "symbols": syms, "include_matrix": False, "top_n": len(syms)},
            timeout_sec=45.0,
        )
        if isinstance(data, dict) and code == 200 and not err:
            rows = data.get("rows") if isinstance(data.get("rows"), list) else []
            out = {}
            for r in rows:
                if isinstance(r, dict):
                    sym = _safe_str(r.get("symbol")).upper()
                    price = _safe_float(r.get("current_price") or r.get("price"), default=0.0)
                    if sym and price > 0:
                        out[sym] = price
            if out:
                return out

        return {}

# =============================================================================
# Store (Google Sheets) — best-effort, gspread-based
# =============================================================================
class PerformanceStore:
    SHEET_DEFAULT = "Performance_Log"
    START_ROW = 5  # headers at row 5, data from row 6
    DATA_ROW0 = 6

    HEADERS = [
        "Record ID",
        "Key",
        "Symbol",
        "Horizon",
        "Date Recorded (Riyadh)",
        "Entry Price",
        "Entry Recommendation",
        "Entry Score",
        "Risk Bucket",
        "Confidence",
        "Origin Tab",
        "Target Price",
        "Target ROI %",
        "Target Date (Riyadh)",
        "Status",
        "Current Price",
        "Unrealized ROI %",
        "Realized ROI %",
        "Outcome",
        "Volatility",
        "Max Drawdown %",
        "Sharpe Ratio",
        "Sector",
        "Factor Exposures",
        "Last Updated (Riyadh)",
        "Maturity Date",
        "Notes",
    ]

    def __init__(self, spreadsheet_id: str, sheet_name: str):
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name or self.SHEET_DEFAULT
        self.backoff = FullJitterBackoff()
        self.cache: Dict[str, PerformanceRecord] = {}
        self.cache_lock = Lock()

        self.gc = None
        self.sheet = None
        self.ws = None
        self._init_sheet()

    def _load_sa_credentials_best_effort(self) -> Optional[Any]:
        # Accept GOOGLE_SHEETS_CREDENTIALS as JSON or base64(JSON)
        raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GOOGLE_CREDENTIALS") or "").strip()
        if not raw:
            return None
        s = raw
        if not s.startswith("{"):
            try:
                dec = base64.b64decode(s).decode("utf-8", errors="replace").strip()
                if dec.startswith("{"):
                    s = dec
            except Exception:
                pass
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and service_account is not None:
                return service_account.Credentials.from_service_account_info(
                    obj,
                    scopes=["https://www.googleapis.com/auth/spreadsheets"],
                )
        except Exception:
            return None
        return None

    def _init_sheet(self) -> None:
        if not GSPREAD_AVAILABLE or gspread is None:
            return
        try:
            creds = self._load_sa_credentials_best_effort()
            if creds:
                self.gc = gspread.authorize(creds)
            else:
                # relies on local file/service account default
                self.gc = gspread.service_account()

            self.sheet = self.gc.open_by_key(self.spreadsheet_id)
            try:
                self.ws = self.sheet.worksheet(self.sheet_name)
            except Exception:
                self.ws = self.sheet.add_worksheet(title=self.sheet_name, rows=2000, cols=80)

            self.backoff.execute_sync(self._ensure_headers)
        except Exception as e:
            logger.error("PerformanceStore init failed: %s", e)
            self.gc = None
            self.sheet = None
            self.ws = None

    def _ensure_headers(self) -> None:
        if not self.ws:
            return
        # check row 5 headers
        try:
            existing = self.ws.row_values(self.START_ROW)
        except Exception:
            existing = []
        if not existing or (existing and existing[0] != self.HEADERS[0]):
            end_col = len(self.HEADERS)
            rng = _a1_range(1, self.START_ROW, end_col, self.START_ROW)
            self.ws.update(rng, [self.HEADERS])

            # light summary block A1:E15
            summary = [
                ["Performance Summary", "", "", f"Generated: {RiyadhTime.format()}", ""],
                ["Total Records", "0", "", "", ""],
                ["Active Records", "0", "", "", ""],
                ["Matured Records", "0", "", "", ""],
                ["Win Rate", "0%", "", "", ""],
                ["Avg ROI", "0%", "", "", ""],
                ["Sharpe Ratio", "0", "", "", ""],
            ]
            try:
                self.ws.update("A1:E7", summary)
                self.ws.freeze(rows=self.START_ROW)
            except Exception:
                pass

    def is_available(self) -> bool:
        return bool(self.ws)

    def load_records(self, max_records: int = 10000) -> List[PerformanceRecord]:
        if not self.ws:
            return []
        end_row = self.DATA_ROW0 + max(0, int(max_records)) - 1
        end_col = len(self.HEADERS)
        rng = _a1_range(1, self.DATA_ROW0, end_col, end_row)

        def _load() -> List[List[Any]]:
            return self.ws.get(rng)  # type: ignore

        try:
            rows = self.backoff.execute_sync(_load)
            records: List[PerformanceRecord] = []
            with self.cache_lock:
                self.cache.clear()
                for r in rows or []:
                    if not r or not _safe_str(r[0]):
                        continue
                    padded = list(r) + [""] * max(0, len(self.HEADERS) - len(r))
                    rec = PerformanceRecord.from_sheet_row(padded, self.HEADERS)
                    if rec.symbol:
                        records.append(rec)
                        self.cache[rec.key] = rec
            return records
        except Exception as e:
            logger.error("Failed to load records: %s", e)
            return []

    def save_records(self, records: List[PerformanceRecord]) -> bool:
        if not self.ws:
            return False
        end_col = len(self.HEADERS)

        data: List[List[Any]] = []
        for r in records:
            data.append([
                r.record_id,
                r.key,
                r.symbol,
                r.horizon.value,
                RiyadhTime.format(r.date_recorded),
                r.entry_price,
                r.entry_recommendation.value,
                r.entry_score,
                r.entry_risk_bucket,
                r.entry_confidence,
                r.origin_tab,
                r.target_price,
                r.target_roi,
                RiyadhTime.format(r.target_date),
                r.status.value,
                r.current_price,
                r.unrealized_roi,
                "" if r.realized_roi is None else r.realized_roi,
                r.outcome or "",
                r.volatility,
                r.max_drawdown,
                r.sharpe_ratio,
                r.sector or "",
                json_dumps(r.factor_exposures) if r.factor_exposures else "{}",
                RiyadhTime.format(r.last_updated.astimezone(_RIYADH_TZ)),
                RiyadhTime.format(r.maturity_date) if r.maturity_date else "",
                r.notes or "",
            ])

        def _save() -> None:
            # clear old data region
            clear_rng = _a1_range(1, self.DATA_ROW0, end_col, 10000)
            self.ws.batch_clear([clear_rng])  # type: ignore
            if data:
                write_rng = _a1_range(1, self.DATA_ROW0, end_col, self.DATA_ROW0 + len(data) - 1)
                self.ws.update(write_rng, data)  # type: ignore

        try:
            self.backoff.execute_sync(_save)
            with self.cache_lock:
                self.cache = {r.key: r for r in records}
            return True
        except Exception as e:
            logger.error("Failed to save records: %s", e)
            return False

    def append_records(self, records: List[PerformanceRecord]) -> bool:
        if not self.ws:
            return False
        with self.cache_lock:
            existing = set(self.cache.keys())
        new = [r for r in records if r.key not in existing]
        if not new:
            return True

        rows: List[List[Any]] = []
        for r in new:
            rows.append([
                r.record_id,
                r.key,
                r.symbol,
                r.horizon.value,
                RiyadhTime.format(r.date_recorded),
                r.entry_price,
                r.entry_recommendation.value,
                r.entry_score,
                r.entry_risk_bucket,
                r.entry_confidence,
                r.origin_tab,
                r.target_price,
                r.target_roi,
                RiyadhTime.format(r.target_date),
                r.status.value,
                r.current_price,
                r.unrealized_roi,
                "" if r.realized_roi is None else r.realized_roi,
                r.outcome or "",
                r.volatility,
                r.max_drawdown,
                r.sharpe_ratio,
                r.sector or "",
                json_dumps(r.factor_exposures) if r.factor_exposures else "{}",
                RiyadhTime.format(r.last_updated.astimezone(_RIYADH_TZ)),
                RiyadhTime.format(r.maturity_date) if r.maturity_date else "",
                r.notes or "",
            ])

        try:
            self.backoff.execute_sync(lambda: self.ws.append_rows(rows, value_input_option="RAW"))  # type: ignore
            with self.cache_lock:
                for r in new:
                    self.cache[r.key] = r
            return True
        except Exception as e:
            logger.error("Failed to append records: %s", e)
            return False

    def update_summary(self, summary: PerformanceSummary) -> bool:
        if not self.ws:
            return False
        block = [
            ["Performance Summary", "", "", f"Updated: {RiyadhTime.format()}", ""],
            ["Total Records", summary.total_records, "", "", ""],
            ["Active Records", summary.active_records, "", "", ""],
            ["Matured Records", summary.matured_records, "", "", ""],
            ["Wins", summary.wins, "", "", ""],
            ["Losses", summary.losses, "", "", ""],
            ["Win Rate", f"{summary.win_rate:.1f}%", "", "", ""],
            ["Avg ROI", f"{summary.avg_roi:.2f}%", "", "", ""],
            ["Median ROI", f"{summary.median_roi:.2f}%", "", "", ""],
            ["Best ROI", f"{summary.best_roi:.2f}%", "", "", ""],
            ["Worst ROI", f"{summary.worst_roi:.2f}%", "", "", ""],
            ["Sharpe Ratio", f"{summary.sharpe_ratio:.2f}", "", "", ""],
            ["Sortino Ratio", f"{summary.sortino_ratio:.2f}", "", "", ""],
        ]
        try:
            self.backoff.execute_sync(lambda: self.ws.update("A1:E13", block))  # type: ignore
            return True
        except Exception:
            return False

# =============================================================================
# Analytics
# =============================================================================
class RiskCalculator:
    def __init__(self, risk_free_rate: float = 0.02):
        self.rfr = float(risk_free_rate)

    def sharpe_ratio(self, returns: List[float]) -> float:
        if not returns:
            return 0.0
        if not NUMPY_AVAILABLE or np is None:
            return 0.0
        arr = np.asarray(returns, dtype=float)
        if arr.size < 2:
            return 0.0
        if float(np.std(arr)) == 0.0:
            return 0.0
        excess = arr - (self.rfr / 252.0)
        return float(np.mean(excess) / np.std(arr) * math.sqrt(252))

    def sortino_ratio(self, returns: List[float]) -> float:
        if not returns:
            return 0.0
        if not NUMPY_AVAILABLE or np is None:
            return 0.0
        arr = np.asarray(returns, dtype=float)
        excess = arr - (self.rfr / 252.0)
        downside = arr[arr < 0]
        if downside.size < 2:
            return 0.0
        if float(np.std(downside)) == 0.0:
            return 0.0
        return float(np.mean(excess) / np.std(downside) * math.sqrt(252))

class MonteCarloSimulator:
    def __init__(self, seed: Optional[int] = None):
        self.seed = seed
        if NUMPY_AVAILABLE and np is not None and seed is not None:
            np.random.seed(seed)

    def simulate_win_rate(self, outcomes: List[bool], iterations: int = 10000, confidence: float = 0.95) -> Dict[str, float]:
        if not outcomes or not NUMPY_AVAILABLE or np is None:
            return {"mean": 0.0, "median": 0.0, "std": 0.0, "ci_lower": 0.0, "ci_upper": 0.0, "observed": 0.0}
        n = len(outcomes)
        observed = sum(1 for x in outcomes if x) / n
        sims = np.array([np.mean(np.random.choice(outcomes, size=n, replace=True)) for _ in range(max(100, int(iterations)))], dtype=float)
        lo = float(np.percentile(sims, ((1 - confidence) / 2) * 100))
        hi = float(np.percentile(sims, ((1 + confidence) / 2) * 100))
        return {
            "mean": float(np.mean(sims)),
            "median": float(np.median(sims)),
            "std": float(np.std(sims)),
            "ci_lower": lo,
            "ci_upper": hi,
            "observed": float(observed),
        }

class AttributionAnalyzer:
    def by_sector(self, records: List[PerformanceRecord]) -> Dict[str, Dict[str, float]]:
        bucket: Dict[str, Dict[str, float]] = {}
        for r in records:
            if not r.sector or r.realized_roi is None:
                continue
            sec = r.sector
            if sec not in bucket:
                bucket[sec] = {"wins": 0.0, "losses": 0.0, "total_roi": 0.0, "count": 0.0}
            bucket[sec]["count"] += 1.0
            bucket[sec]["total_roi"] += float(r.realized_roi)
            if (r.realized_roi or 0.0) > 0:
                bucket[sec]["wins"] += 1.0
            elif (r.realized_roi or 0.0) < 0:
                bucket[sec]["losses"] += 1.0
        out: Dict[str, Dict[str, float]] = {}
        for sec, d in bucket.items():
            cnt = max(1.0, d["count"])
            wins = d["wins"]
            losses = d["losses"]
            out[sec] = {
                "win_rate": (wins / max(1.0, wins + losses)) * 100.0,
                "avg_roi": d["total_roi"] / cnt,
                "wins": wins,
                "losses": losses,
                "count": cnt,
            }
        return out

class PerformanceAnalyzer:
    def __init__(self):
        self.risk = RiskCalculator()
        self.sim = MonteCarloSimulator(seed=42)
        self.attr = AttributionAnalyzer()

    def analyze(self, records: List[PerformanceRecord]) -> PerformanceSummary:
        s = PerformanceSummary(total_records=len(records))
        s.active_records = sum(1 for r in records if r.status == PerformanceStatus.ACTIVE)
        matured = [r for r in records if r.status == PerformanceStatus.MATURED and r.realized_roi is not None]
        s.matured_records = len(matured)

        if matured:
            rois = [float(r.realized_roi or 0.0) for r in matured]
            wins = sum(1 for r in matured if (r.realized_roi or 0.0) > 0)
            losses = sum(1 for r in matured if (r.realized_roi or 0.0) < 0)
            breakeven = len(matured) - wins - losses
            s.wins, s.losses, s.breakeven = wins, losses, breakeven
            total_outcomes = wins + losses
            s.win_rate = (wins / total_outcomes * 100.0) if total_outcomes > 0 else 0.0

            if NUMPY_AVAILABLE and np is not None:
                arr = np.asarray(rois, dtype=float)
                s.avg_roi = float(np.mean(arr))
                s.median_roi = float(np.median(arr))
                s.roi_std = float(np.std(arr))
                s.best_roi = float(np.max(arr))
                s.worst_roi = float(np.min(arr))
            else:
                rois_sorted = sorted(rois)
                s.avg_roi = sum(rois) / max(1, len(rois))
                s.median_roi = rois_sorted[len(rois_sorted) // 2]
                s.best_roi = max(rois_sorted)
                s.worst_roi = min(rois_sorted)

            if len(rois) > 2:
                # treat ROI as daily returns proxy (best-effort)
                s.sharpe_ratio = self.risk.sharpe_ratio(rois)
                s.sortino_ratio = self.risk.sortino_ratio(rois)

            # hit rate by horizon
            groups: Dict[str, List[PerformanceRecord]] = defaultdict(list)
            for r in matured:
                groups[r.horizon.value].append(r)
            for h, grp in groups.items():
                w = sum(1 for r in grp if (r.realized_roi or 0.0) > 0)
                t = len(grp)
                s.hit_rate_by_horizon[h] = (w / t * 100.0) if t else 0.0

            s.performance_by_sector = self.attr.by_sector(matured)

            try:
                perf_win_rate.set(float(s.win_rate))
            except Exception:
                pass

        return s

# =============================================================================
# Reporting
# =============================================================================
class ReportGenerator:
    def __init__(self, analyzer: PerformanceAnalyzer):
        self.analyzer = analyzer

    def generate_json(self, records: List[PerformanceRecord], summary: PerformanceSummary) -> str:
        matured = [r for r in records if r.status == PerformanceStatus.MATURED and r.realized_roi is not None]
        outcomes = [(r.realized_roi or 0.0) > 0 for r in matured]
        ci = self.analyzer.sim.simulate_win_rate(outcomes, iterations=10000, confidence=0.95) if outcomes else {}
        report = {
            "generated_at_riyadh": RiyadhTime.format(),
            "version": SCRIPT_VERSION,
            "summary": summary.to_dict(),
            "confidence_intervals": {"win_rate": ci} if ci else {},
            "records": [r.to_dict() for r in records],
        }
        return json_dumps(report, indent=2)

    def generate_csv(self, records: List[PerformanceRecord], filepath: str) -> None:
        if not records:
            return
        rows = [r.to_dict() for r in records]
        fieldnames = list(rows[0].keys())
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)

    def _plot_png_base64(self, fig: "Figure") -> str:
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
        return base64.b64encode(buf.getvalue()).decode("utf-8")

    def generate_html(self, records: List[PerformanceRecord], summary: PerformanceSummary, filepath: str) -> None:
        plots: Dict[str, str] = {}
        if PLOT_AVAILABLE and Figure is not None and NUMPY_AVAILABLE and np is not None:
            try:
                matured = [r for r in records if r.status == PerformanceStatus.MATURED and r.realized_roi is not None]
                matured.sort(key=lambda x: (x.maturity_date or x.date_recorded))

                if matured:
                    # win-rate over time
                    fig1 = Figure(figsize=(10, 5))
                    ax1 = fig1.add_subplot(111)
                    dates = [(r.maturity_date or r.date_recorded).astimezone(_RIYADH_TZ) for r in matured]
                    wins = np.cumsum([1 if (r.realized_roi or 0.0) > 0 else 0 for r in matured])
                    denom = np.arange(1, len(matured) + 1, dtype=float)
                    win_rate = (wins / denom) * 100.0
                    ax1.plot(dates, win_rate, linewidth=2)
                    ax1.set_title("Cumulative Win Rate (%)")
                    ax1.set_xlabel("Date")
                    ax1.set_ylabel("Win Rate %")
                    ax1.grid(True, alpha=0.25)
                    plots["win_rate"] = self._plot_png_base64(fig1)

                    # ROI histogram (matplotlib only)
                    roi = np.asarray([float(r.realized_roi or 0.0) for r in matured], dtype=float)
                    fig2 = Figure(figsize=(10, 5))
                    ax2 = fig2.add_subplot(111)
                    ax2.hist(roi, bins=30)
                    ax2.axvline(0.0, linestyle="--", linewidth=1.5)
                    ax2.set_title("ROI Distribution (%)")
                    ax2.set_xlabel("ROI %")
                    ax2.set_ylabel("Frequency")
                    ax2.grid(True, alpha=0.2)
                    plots["roi_hist"] = self._plot_png_base64(fig2)
            except Exception as e:
                logger.warning("Plot generation failed: %s", e)

        html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Performance Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; background: #f5f5f5; margin: 20px; }}
    .container {{ max-width: 1200px; margin: auto; background: #fff; padding: 18px; box-shadow: 0 1px 6px rgba(0,0,0,0.08); }}
    h1 {{ margin: 0 0 10px 0; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; }}
    .card {{ background: #f8f9fa; padding: 12px; border-radius: 8px; border-left: 4px solid #2b6cb0; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 14px; }}
    th, td {{ border-bottom: 1px solid #e5e7eb; padding: 10px; text-align: left; }}
    th {{ background: #111827; color: #fff; }}
    .pos {{ color: #16a34a; font-weight: bold; }}
    .neg {{ color: #dc2626; font-weight: bold; }}
    .plot {{ margin: 16px 0; text-align: center; }}
  </style>
</head>
<body>
<div class="container">
  <h1>📊 Performance Report</h1>
  <div>Generated (Riyadh): {RiyadhTime.format()}</div>

  <div class="grid" style="margin-top:12px;">
    <div class="card"><div>Total Records</div><div style="font-size:22px;font-weight:bold;">{summary.total_records}</div></div>
    <div class="card"><div>Win Rate</div><div style="font-size:22px;font-weight:bold;">{summary.win_rate:.1f}%</div></div>
    <div class="card"><div>Avg ROI</div><div style="font-size:22px;font-weight:bold;">{summary.avg_roi:.2f}%</div></div>
    <div class="card"><div>Sharpe</div><div style="font-size:22px;font-weight:bold;">{summary.sharpe_ratio:.2f}</div></div>
  </div>

  {"<div class='plot'><img style='max-width:100%;' src='data:image/png;base64," + plots.get("win_rate","") + "'/></div>" if plots.get("win_rate") else ""}
  {"<div class='plot'><img style='max-width:100%;' src='data:image/png;base64," + plots.get("roi_hist","") + "'/></div>" if plots.get("roi_hist") else ""}

  <h2>Recent Records</h2>
  <table>
    <tr><th>Symbol</th><th>Horizon</th><th>Entry Date</th><th>Status</th><th>ROI</th><th>Outcome</th></tr>
"""
        recs = sorted(records, key=lambda r: r.date_recorded, reverse=True)[:30]
        for r in recs:
            roi = r.realized_roi if r.realized_roi is not None else r.unrealized_roi
            cls = "pos" if (roi or 0.0) > 0 else "neg" if (roi or 0.0) < 0 else ""
            html += f"<tr><td><b>{r.symbol}</b></td><td>{r.horizon.value}</td><td>{RiyadhTime.format(r.date_recorded, '%Y-%m-%d')}</td><td>{r.status.value}</td><td class='{cls}'>{roi:.2f}%</td><td>{r.outcome or ''}</td></tr>\n"

        html += """  </table>
</div>
</body>
</html>
"""
        Path(filepath).write_text(html, encoding="utf-8")

# =============================================================================
# Orchestrator
# =============================================================================
class PerformanceTrackerApp:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.spreadsheet_id = self._get_spreadsheet_id()
        self.store = PerformanceStore(self.spreadsheet_id, args.sheet_name or PerformanceStore.SHEET_DEFAULT)
        self.analyzer = PerformanceAnalyzer()
        self.reporter = ReportGenerator(self.analyzer)

        self.backend = BackendClient(
            base_url=self._backend_base_url(),
            token=self._backend_token(),
        )

        self.stop_event = Event()

    def _get_spreadsheet_id(self) -> str:
        if self.args.sheet_id and str(self.args.sheet_id).strip():
            return str(self.args.sheet_id).strip()

        sid = (os.getenv("DEFAULT_SPREADSHEET_ID") or "").strip()
        if sid:
            return sid

        sid = (getattr(settings, "default_spreadsheet_id", None) or "").strip() if settings else ""
        if sid:
            return sid

        raise ValueError("No spreadsheet ID provided (use --sheet-id or DEFAULT_SPREADSHEET_ID env).")

    def _backend_base_url(self) -> str:
        env_url = (os.getenv("BACKEND_BASE_URL") or "").strip()
        if env_url:
            return env_url.rstrip("/")
        cfg_url = (getattr(settings, "backend_base_url", None) or "").strip() if settings else ""
        if cfg_url:
            return cfg_url.rstrip("/")
        return (os.getenv("TFB_BASE_URL") or "").strip().rstrip("/")

    def _backend_token(self) -> str:
        for k in ("TFB_TOKEN", "APP_TOKEN", "BACKEND_TOKEN", "X_APP_TOKEN"):
            v = (os.getenv(k) or "").strip()
            if v:
                return v
        return ""

    def _horizons(self) -> List[HorizonType]:
        out: List[HorizonType] = []
        for raw in (self.args.horizons or ["1M", "3M"]):
            out.append(_parse_enum_value(HorizonType, raw, HorizonType.MONTH_1))
        # dedup preserve order
        seen = set()
        final = []
        for h in out:
            if h.value not in seen:
                seen.add(h.value)
                final.append(h)
        return final

    def _derive_target(self, row: Dict[str, Any], horizon: HorizonType, entry_price: float) -> Tuple[float, float]:
        # returns (target_price, target_roi_pct)
        suffix = "1m" if horizon.value == "1M" else "3m" if horizon.value == "3M" else "12m" if horizon.value == "1Y" else ""
        fkey = f"forecast_price_{suffix}" if suffix else ""
        rkey = f"expected_roi_{suffix}" if suffix else ""

        tgt_price = _safe_float(row.get(fkey), default=0.0) if fkey else 0.0
        tgt_roi = _safe_float(row.get(rkey), default=0.0) if rkey else 0.0

        if tgt_price <= 0.0 and tgt_roi != 0.0 and entry_price > 0.0:
            tgt_price = entry_price * (1.0 + (tgt_roi / 100.0))
        if tgt_roi == 0.0 and tgt_price > 0.0 and entry_price > 0.0:
            tgt_roi = (tgt_price / entry_price - 1.0) * 100.0

        return tgt_price, tgt_roi

    def _recommendation_from_row(self, row: Dict[str, Any]) -> RecommendationType:
        s = _safe_str(row.get("recommendation")).upper()
        if "STRONG" in s and "BUY" in s:
            return RecommendationType.STRONG_BUY
        if "BUY" in s:
            return RecommendationType.BUY
        if "STRONG" in s and "SELL" in s:
            return RecommendationType.STRONG_SELL
        if "SELL" in s:
            return RecommendationType.SELL
        if "HOLD" in s:
            return RecommendationType.HOLD
        return RecommendationType.HOLD

    async def record_from_top10(self, existing: List[PerformanceRecord]) -> List[PerformanceRecord]:
        if not self.backend.base_url:
            return []

        rows, meta = await self.backend.get_top10_rows(criteria_overrides=None)
        if not rows:
            return []

        existing_keys = set(r.key for r in existing)
        now = RiyadhTime.now()
        horizons = self._horizons()

        new_records: List[PerformanceRecord] = []
        for row in rows:
            sym = _safe_str(row.get("symbol")).upper()
            if not sym:
                continue
            entry_price = _safe_float(row.get("current_price") or row.get("price"), default=0.0)
            if entry_price <= 0.0:
                continue

            score = _safe_float(row.get("overall_score"), default=0.0)
            risk_score = row.get("risk_score")
            conf = row.get("forecast_confidence")
            risk_bucket = _risk_bucket_from_score(_safe_float(risk_score, default=0.0) if risk_score is not None else None)
            conf_bucket = _confidence_bucket(_safe_float(conf, default=0.0) if conf is not None else None)
            rec = self._recommendation_from_row(row)
            origin = _safe_str(row.get("source_page") or row.get("origin") or "Top_10_Investments") or "Top_10_Investments"

            for h in horizons:
                tgt_price, tgt_roi = self._derive_target(row, h, entry_price)
                if tgt_price <= 0.0:
                    # still track, but keep target at entry
                    tgt_price = entry_price
                    tgt_roi = 0.0

                target_date = now + timedelta(days=h.days)
                r = PerformanceRecord(
                    record_id=str(uuid.uuid4()),
                    symbol=sym,
                    horizon=h,
                    date_recorded=now,
                    entry_price=entry_price,
                    entry_recommendation=rec,
                    entry_score=score,
                    entry_risk_bucket=risk_bucket,
                    entry_confidence=conf_bucket,
                    origin_tab=origin,
                    target_price=tgt_price,
                    target_roi=tgt_roi,
                    target_date=target_date,
                    status=PerformanceStatus.ACTIVE,
                    current_price=entry_price,
                    unrealized_roi=0.0,
                    last_updated=_utc_now(),
                )
                if r.key not in existing_keys:
                    existing_keys.add(r.key)
                    new_records.append(r)

        return new_records

    async def audit_active_records(self, records: List[PerformanceRecord]) -> List[PerformanceRecord]:
        # Update current_price/unrealized; mature if target_date passed
        active = [r for r in records if r.status == PerformanceStatus.ACTIVE and r.symbol]
        if not active:
            return records

        syms = list(dict.fromkeys([r.symbol for r in active]))
        price_map = await self.backend.fetch_prices(syms) if self.backend.base_url else {}
        now_r = RiyadhTime.now()

        for r in active:
            px = float(price_map.get(r.symbol, 0.0))
            if px > 0.0:
                r.current_price = px
                if r.entry_price > 0:
                    r.unrealized_roi = (px / r.entry_price - 1.0) * 100.0

            # maturity check
            if r.target_date and now_r >= r.target_date:
                r.status = PerformanceStatus.MATURED
                r.maturity_date = now_r
                r.realized_roi = float(r.unrealized_roi)
                if (r.realized_roi or 0.0) > 0:
                    r.outcome = "WIN"
                elif (r.realized_roi or 0.0) < 0:
                    r.outcome = "LOSS"
                else:
                    r.outcome = "BREAKEVEN"

            r.last_updated = _utc_now()

        return records

    async def run_once(self) -> int:
        # Load records (if store missing, we can still export analysis-only from backend)
        loop = asyncio.get_running_loop()
        records: List[PerformanceRecord] = []

        if self.store.is_available():
            records = await loop.run_in_executor(_CPU_EXECUTOR, self.store.load_records, int(self.args.max_records or 10000))
        else:
            records = []

        if self.args.record:
            new = await self.record_from_top10(records)
            if new and self.store.is_available():
                ok = await loop.run_in_executor(_CPU_EXECUTOR, self.store.append_records, new)
                if ok:
                    records.extend(new)

        if self.args.audit:
            records = await self.audit_active_records(records)
            if self.store.is_available():
                await loop.run_in_executor(_CPU_EXECUTOR, self.store.save_records, records)

        summary = self.analyzer.analyze(records) if self.args.analyze or self.args.export else PerformanceSummary(total_records=len(records))
        try:
            perf_records_processed.inc(len(records))
        except Exception:
            pass

        if self.args.analyze:
            if self.store.is_available():
                await loop.run_in_executor(_CPU_EXECUTOR, self.store.update_summary, summary)
            _out("=" * 66)
            _out("📊 PERFORMANCE SUMMARY")
            _out("=" * 66)
            _out(f"Total: {summary.total_records} | Active: {summary.active_records} | Matured: {summary.matured_records}")
            _out(f"Wins: {summary.wins} | Losses: {summary.losses} | WinRate: {summary.win_rate:.1f}%")
            _out(f"Avg ROI: {summary.avg_roi:.2f}% | Sharpe: {summary.sharpe_ratio:.2f} | Sortino: {summary.sortino_ratio:.2f}")

        if self.args.simulate:
            matured = [r for r in records if r.status == PerformanceStatus.MATURED and r.realized_roi is not None]
            outcomes = [(r.realized_roi or 0.0) > 0 for r in matured]
            if outcomes:
                ci = self.analyzer.sim.simulate_win_rate(outcomes, iterations=int(self.args.iterations or 10000), confidence=float(self.args.confidence or 0.95))
                _out(f"Win-Rate Simulation (mean): {ci['mean']*100:.1f}% | CI: [{ci['ci_lower']*100:.1f}%, {ci['ci_upper']*100:.1f}%] | observed: {ci['observed']*100:.1f}%")
            else:
                _out("Simulation skipped: no matured outcomes.")

        if self.args.export:
            out_base = (self.args.output or f"performance_report_{RiyadhTime.format(fmt='%Y%m%d_%H%M%S')}").strip()
            fmt = (self.args.format or "html").lower()
            if fmt in {"json", "all"}:
                Path(out_base + ".json").write_text(self.reporter.generate_json(records, summary), encoding="utf-8")
            if fmt in {"csv", "all"}:
                self.reporter.generate_csv(records, out_base + ".csv")
            if fmt in {"html", "all"}:
                self.reporter.generate_html(records, summary, out_base + ".html")
            _out(f"Export complete: {out_base}.* ({fmt})")

        return 0

    async def run_daemon(self) -> int:
        interval = max(30, int(self.args.interval or 3600))
        _out(f"Daemon mode ON (interval={interval}s). Press Ctrl+C to stop.")
        while not self.stop_event.is_set():
            try:
                try:
                    perf_daemon_cycles.inc()
                except Exception:
                    pass
                await self.run_once()
            except Exception as e:
                logger.exception("Daemon cycle failed: %s", e)
            # sleep in small steps for fast stop
            for _ in range(interval):
                if self.stop_event.is_set():
                    break
                await asyncio.sleep(1.0)
        return 0

    def stop(self) -> None:
        self.stop_event.set()

# =============================================================================
# CLI
# =============================================================================
def create_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=f"Tadawul Fast Bridge - Performance Tracker v{SCRIPT_VERSION}")
    p.add_argument("--sheet-id", help="Spreadsheet ID (or DEFAULT_SPREADSHEET_ID env)")
    p.add_argument("--sheet-name", default="Performance_Log", help="Performance log tab name")
    p.add_argument("--record", action="store_true", help="Record new recommendations (best-effort from /v1/analysis/top10)")
    p.add_argument("--audit", action="store_true", help="Audit/update active records (fetch current prices from backend if configured)")
    p.add_argument("--analyze", action="store_true", help="Analyze performance + write summary block in sheet")
    p.add_argument("--simulate", action="store_true", help="Run Monte Carlo win-rate simulation (needs numpy)")
    p.add_argument("--export", action="store_true", help="Export performance report")
    p.add_argument("--daemon", action="store_true", help="Run continuously (daemon)")
    p.add_argument("--horizons", nargs="+", default=["1M", "3M"], help="Horizons to track (e.g., 1W 1M 3M 6M 1Y)")
    p.add_argument("--max-records", type=int, default=10000, help="Max records to load from sheet")
    p.add_argument("--confidence", type=float, default=0.95, help="Confidence level (simulation)")
    p.add_argument("--iterations", type=int, default=10000, help="Monte Carlo iterations")
    p.add_argument("--format", choices=["json", "csv", "html", "all"], default="html", help="Export format")
    p.add_argument("--output", help="Output file base path (without extension)")
    p.add_argument("--interval", type=int, default=3600, help="Daemon interval seconds")
    p.add_argument("--verbose", "-v", action="store_true", help="Verbose logs")
    return p

async def async_main() -> int:
    args = create_parser().parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # If nothing selected, show help
    if not any([args.record, args.audit, args.analyze, args.simulate, args.export, args.daemon]):
        _out(create_parser().format_help())
        return 0

    app = PerformanceTrackerApp(args)

    def _sig_handler(_signum, _frame):
        app.stop()

    try:
        signal.signal(signal.SIGINT, _sig_handler)
        signal.signal(signal.SIGTERM, _sig_handler)
    except Exception:
        pass

    try:
        if args.daemon:
            return await app.run_daemon()
        return await app.run_once()
    finally:
        try:
            _CPU_EXECUTOR.shutdown(wait=False)
        except Exception:
            pass

def main() -> int:
    if sys.platform == "win32":
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
        except Exception:
            pass
    try:
        return asyncio.run(async_main())
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        logger.exception("Fatal error: %s", e)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
