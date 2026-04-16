#!/usr/bin/env python3
"""
scripts/run_market_scan.py
================================================================================
TADAWUL FAST BRIDGE — MARKET SCANNER (v5.2.0) — SCHEMA/ENGINE ALIGNED
================================================================================

What this scanner does (aligned with your Phase 3–7 architecture):
- Reads symbols from your Google Sheet pages via symbols_reader (single source of truth).
- Fetches enriched quotes in BULK using:
    (A) Local Engine mode (preferred when running inside repo): core.data_engine_v2.get_engine()
    (B) HTTP mode (fallback when running outside): POST /v1/enriched/quotes
- Ranks instruments using your canonical fields when present:
    overall_score, risk_score, value_score, quality_score, momentum_score,
    expected_roi_1m/3m/12m, forecast_confidence, rsi_14, volatility_30d, market_cap, volume
- Filters using CFO-safe constraints:
    min_price, min_volume, max_risk_score, min_confidence, min_expected_roi
- Exports top N to JSON/CSV/HTML (optional).

Phase alignment:
- ✅ No KSA_Tadawul dependency.
- ✅ Uses canonical page keys:
    Market_Leaders, Global_Markets, Commodities_FX, Mutual_Funds, My_Portfolio, Insights_Analysis (optional)
- ✅ Startup-safe (no network calls at import-time).
- ✅ Defensive: optional deps (numpy/pandas/aiohttp/orjson) are safe.

Environment:
  BACKEND_BASE_URL=https://tadawul-fast-bridge.onrender.com
  TFB_TOKEN=<token>   (or APP_TOKEN / BACKUP_APP_TOKEN)

CLI examples:
  python scripts/run_market_scan.py --keys Market_Leaders Global_Markets --top 50 --mode engine
  python scripts/run_market_scan.py --keys My_Portfolio --top 30 --min-confidence 0.65 --max-risk 60
  python scripts/run_market_scan.py --mode http --backend https://... --top 25 --export-json scan.json

"""

from __future__ import annotations

import argparse
import asyncio
import csv
import inspect
import logging
import math
import os
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union


# =============================================================================
# Path bootstrap (repo-safe)
# =============================================================================
def _ensure_project_root_on_path() -> None:
    try:
        here = Path(__file__).resolve()
        for p in (here.parent, here.parent.parent):
            s = str(p)
            if s not in sys.path:
                sys.path.insert(0, s)
    except Exception:
        pass


_ensure_project_root_on_path()


# =============================================================================
# Logging
# =============================================================================
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
    format="%(asctime)s | %(levelname)7s | %(name)s | %(message)s",
)
logger = logging.getLogger("MarketScan")

# =============================================================================
# Version
# FIX v5.2.0: SCRIPT_VERSION was missing; version was hardcoded in 3 places.
# =============================================================================
SCRIPT_VERSION = "5.2.0"


# =============================================================================
# Optional JSON (orjson)
# =============================================================================
try:
    import orjson  # type: ignore

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        opt = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(v, option=opt, default=str).decode("utf-8")

    def json_loads(b: Union[str, bytes]) -> Any:
        if isinstance(b, str):
            b = b.encode("utf-8")
        return orjson.loads(b)

    _HAS_ORJSON = True
except Exception:
    import json  # type: ignore

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        return json.dumps(v, indent=indent if indent else None, default=str, ensure_ascii=False)

    def json_loads(b: Union[str, bytes]) -> Any:
        if isinstance(b, (bytes, bytearray)):
            b = b.decode("utf-8", errors="replace")
        return json.loads(b)

    _HAS_ORJSON = False


# =============================================================================
# Optional async HTTP
# =============================================================================
try:
    import aiohttp  # type: ignore

    _AIOHTTP_AVAILABLE = True
except Exception:
    aiohttp = None  # type: ignore
    _AIOHTTP_AVAILABLE = False


# =============================================================================
# Project imports (safe)
# =============================================================================
try:
    from env import settings  # type: ignore
    import symbols_reader  # type: ignore
    from core.symbols.normalize import normalize_symbol  # type: ignore
    from core.data_engine_v2 import get_engine  # type: ignore

    _CORE_AVAILABLE = True
except Exception:
    settings = None  # type: ignore
    symbols_reader = None  # type: ignore
    _CORE_AVAILABLE = False

    def normalize_symbol(s: str) -> str:  # type: ignore
        return (s or "").strip().upper()

    async def get_engine() -> Any:  # type: ignore
        return None


# =============================================================================
# Time helpers
# =============================================================================
def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# =============================================================================
# Safety helpers
# =============================================================================
def _safe_str(x: Any) -> str:
    try:
        return str(x).strip()
    except Exception:
        return ""


def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            f = float(x)
        else:
            s = _safe_str(x).replace(",", "")
            if not s or s.lower() in {"na", "n/a", "null", "none"}:
                return default
            if s.endswith("%"):
                f = float(s[:-1].strip()) / 100.0
            else:
                f = float(s)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except Exception:
        return default


def _pct_to_fraction(x: Any) -> Optional[float]:
    """
    Accepts percent or fraction:
      "12%" -> 0.12
      12 -> 0.12
      0.12 -> 0.12
    """
    f = _safe_float(x)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _maybe_await(v: Any) -> Any:
    return v


def _as_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    md = getattr(obj, "model_dump", None)
    if callable(md):  # pydantic v2
        try:
            return md(mode="python")
        except Exception:
            try:
                return md()
            except Exception:
                pass
    d = getattr(obj, "dict", None)
    if callable(d):  # pydantic v1
        try:
            return d()
        except Exception:
            pass
    try:
        return dict(getattr(obj, "__dict__", {})) or {}
    except Exception:
        return {}


# =============================================================================
# Canonical keys / pages
# =============================================================================
CANON_PAGES = [
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
    # optional:
    "Insights_Analysis",
]


def _canon_key(k: str) -> str:
    """
    Normalize user inputs like:
      "Market Leaders" -> "Market_Leaders"
      "GLOBAL" -> "Global_Markets"
    """
    s = _safe_str(k)
    if not s:
        return ""
    s2 = s.strip().replace("-", "_").replace(" ", "_")
    s2u = s2.upper()

    aliases = {
        "MARKETLEADERS": "Market_Leaders",
        "MARKET_LEADERS": "Market_Leaders",
        "LEADERS": "Market_Leaders",
        "GLOBAL": "Global_Markets",
        "GLOBAL_MARKETS": "Global_Markets",
        "GLOBALMARKETS": "Global_Markets",
        "COMMODITIES": "Commodities_FX",
        "COMMODITIES_FX": "Commodities_FX",
        "COMMODITIES&FX": "Commodities_FX",
        "FX": "Commodities_FX",
        "MUTUAL": "Mutual_Funds",
        "MUTUAL_FUNDS": "Mutual_Funds",
        "FUNDS": "Mutual_Funds",
        "PORTFOLIO": "My_Portfolio",
        "MY_PORTFOLIO": "My_Portfolio",
        "INSIGHTS": "Insights_Analysis",
        "INSIGHTS_ANALYSIS": "Insights_Analysis",
        "AI_INSIGHTS": "Insights_Analysis",
    }

    key = s2u.replace("_", "")
    if key in aliases:
        return aliases[key]
    if s2u in aliases:
        return aliases[s2u]
    # already canonical-like
    for p in CANON_PAGES:
        if s2u == p.upper():
            return p
    return s2  # fallback


def _dedup_preserve(items: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for x in items:
        u = _safe_str(x).upper()
        if not u:
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


# =============================================================================
# HTTP client (fallback mode)
# =============================================================================
class HTTPClient:
    def __init__(self, base_url: str, token: str, timeout_sec: float = 60.0):
        self.base_url = (base_url or "").rstrip("/")
        self.token = (token or "").strip()
        self.timeout_sec = float(timeout_sec)
        self._session = None

    def _headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
            h["X-APP-TOKEN"] = self.token
        return h

    async def close(self) -> None:
        try:
            if self._session is not None:
                await self._session.close()
        except Exception:
            pass
        self._session = None

    async def post_json(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not _AIOHTTP_AVAILABLE:
            raise RuntimeError("aiohttp not available for HTTP mode (install aiohttp or use --mode engine).")

        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self.timeout_sec)  # type: ignore
            self._session = aiohttp.ClientSession(timeout=timeout, headers=self._headers())  # type: ignore

        url = f"{self.base_url}{path}"
        async with self._session.post(url, json=payload) as resp:  # type: ignore
            code = int(resp.status)
            raw = await resp.read()  # type: ignore
            if code != 200:
                txt = raw.decode("utf-8", errors="replace")[:400]
                raise RuntimeError(f"HTTP {code}: {txt}")
            data = json_loads(raw) if raw else {}
            return data if isinstance(data, dict) else {"data": data}


# =============================================================================
# Scan configuration + results
# =============================================================================
@dataclass(slots=True)
class ScanConfig:
    spreadsheet_id: str
    keys: List[str]
    top_n: int = 50
    max_symbols: int = 1500
    chunk_size: int = 60
    concurrency: int = 10

    mode: str = "engine"  # engine | http
    backend_base_url: str = ""
    token: str = ""

    # filters
    min_price: float = 1.0
    min_volume: float = 1000.0
    max_risk_score: float = 60.0          # <=60 => low/moderate risk
    min_confidence: float = 0.65          # forecast_confidence
    min_expected_roi: float = 0.00        # fraction (0.10=10%)
    horizon: str = "3m"                   # 1m|3m|12m

    # ranking weights
    w_roi: float = 0.55
    w_conf: float = 0.25
    w_score: float = 0.20

    # exports
    export_json: str = ""
    export_csv: str = ""
    export_html: str = ""

    @classmethod
    def from_worker_payload(cls, payload: Dict[str, Any]) -> "ScanConfig":
        """
        FIX v5.2.0: Build a ScanConfig from the worker.py task payload dict.

        Previously there was no programmatic entry point — worker.py had to
        construct a fake argparse.Namespace and call main() directly, which
        broke whenever new CLI args were added.

        Expected payload keys match WORKER_PAYLOAD_SCHEMA.
        All fields are optional; sensible defaults are used for missing keys.
        """
        def _s(k: str, default: str = "") -> str:
            return str(payload.get(k) or default).strip()

        def _f(k: str, default: float) -> float:
            try:
                v = payload.get(k)
                return float(v) if v is not None else default
            except Exception:
                return default

        def _i(k: str, default: int) -> int:
            try:
                v = payload.get(k)
                return int(v) if v is not None else default
            except Exception:
                return default

        spreadsheet_id = _s("spreadsheet_id") or _safe_str(os.getenv("DEFAULT_SPREADSHEET_ID", ""))
        if not spreadsheet_id:
            raise ValueError("ScanConfig.from_worker_payload: 'spreadsheet_id' is required.")

        raw_keys = payload.get("keys") or ["Market_Leaders"]
        if isinstance(raw_keys, str):
            raw_keys = [k.strip() for k in raw_keys.split(",") if k.strip()]
        keys = [_canon_key(k) for k in raw_keys if k]
        keys = [k for k in keys if k]
        if not keys:
            keys = ["Market_Leaders"]

        return cls(
            spreadsheet_id=spreadsheet_id,
            keys=keys,
            top_n=_i("top_n", 50),
            max_symbols=_i("max_symbols", 1500),
            chunk_size=_i("chunk_size", 60),
            concurrency=_i("concurrency", 10),
            mode=_s("mode", "engine"),
            backend_base_url=(
                _s("backend_url") or _s("backend_base_url")
                or _safe_str(os.getenv("BACKEND_BASE_URL", ""))
            ).rstrip("/"),
            token=_s("token") or _s("app_token"),
            timeout_sec=_f("timeout_sec", 60.0),
            min_price=_f("min_price", 1.0),
            min_volume=_f("min_volume", 1000.0),
            max_risk_score=_f("max_risk_score", 60.0),
            min_confidence=_f("min_confidence", 0.65),
            min_expected_roi=_f("min_expected_roi", 0.00),
            horizon=_s("horizon", "3m"),
            w_roi=_f("w_roi", 0.55),
            w_conf=_f("w_conf", 0.25),
            w_score=_f("w_score", 0.20),
            export_json=_s("export_json"),
            export_csv=_s("export_csv"),
            export_html=_s("export_html"),
        )


@dataclass(slots=True)
class ScanRow:
    rank: int
    symbol: str
    name: str
    market: str
    currency: str
    price: Optional[float]
    volume: Optional[float]

    expected_roi: Optional[float]         # fraction
    forecast_confidence: Optional[float]  # 0..1
    overall_score: Optional[float]        # 0..100
    risk_score: Optional[float]           # 0..100 (higher => riskier)

    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None

    rsi_14: Optional[float] = None
    volatility_30d: Optional[float] = None

    data_quality: str = ""
    data_source: str = ""
    last_updated_utc: str = ""

    composite_rank_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Symbols reader
# =============================================================================
def _resolve_spreadsheet_id(cli: str) -> str:
    if cli:
        return cli.strip()
    if settings is not None:
        sid = _safe_str(getattr(settings, "default_spreadsheet_id", ""))
        if sid:
            return sid
    return _safe_str(os.getenv("DEFAULT_SPREADSHEET_ID", ""))


def _resolve_backend_url(cli: str) -> str:
    if cli:
        return cli.rstrip("/")
    env = _safe_str(os.getenv("BACKEND_BASE_URL", "")).rstrip("/")
    if env:
        return env
    if settings is not None:
        s_url = _safe_str(getattr(settings, "backend_base_url", "")).rstrip("/")
        if s_url:
            return s_url
    return "http://127.0.0.1:8000"


def _resolve_token(cli: str) -> str:
    if cli:
        return cli.strip()
    for k in ("TFB_TOKEN", "APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN", "BACKEND_TOKEN"):
        v = _safe_str(os.getenv(k, ""))
        if v:
            return v
    if settings is not None:
        v2 = _safe_str(getattr(settings, "app_token", "") or getattr(settings, "token", ""))
        if v2:
            return v2
    return ""


def _read_symbols_from_keys(spreadsheet_id: str, keys: List[str], max_symbols: int) -> List[str]:
    if symbols_reader is None:
        raise RuntimeError("symbols_reader not available (run inside repo).")

    all_syms: List[str] = []
    for k in keys:
        page = _canon_key(k)
        if not page:
            continue
        try:
            data = symbols_reader.get_page_symbols(page, spreadsheet_id=spreadsheet_id)
        except Exception:
            # backward key fallbacks (best-effort)
            legacy = {
                "Market_Leaders": "MARKET_LEADERS",
                "Global_Markets": "GLOBAL_MARKETS",
                "Commodities_FX": "COMMODITIES_FX",
                "Mutual_Funds": "MUTUAL_FUNDS",
                "My_Portfolio": "MY_PORTFOLIO",
                "Insights_Analysis": "INSIGHTS_ANALYSIS",
            }.get(page, page)
            data = symbols_reader.get_page_symbols(legacy, spreadsheet_id=spreadsheet_id)

        syms: List[str] = []
        if isinstance(data, dict):
            syms = data.get("all") or data.get("symbols") or []
        elif isinstance(data, list):
            syms = data
        else:
            syms = []

        all_syms.extend([_safe_str(s) for s in syms if _safe_str(s)])

    # normalize + dedup
    normed = []
    for s in all_syms:
        try:
            normed.append(normalize_symbol(s))
        except Exception:
            normed.append(s.strip().upper())

    out = _dedup_preserve(normed)
    if max_symbols > 0:
        out = out[:max_symbols]
    return out


# =============================================================================
# Quote fetch (engine)
# =============================================================================
async def _fetch_quotes_engine(engine: Any, symbols: List[str]) -> List[Dict[str, Any]]:
    if engine is None:
        return []

    # Prefer batch API
    for m in ("get_enriched_quotes", "get_quotes", "fetch_quotes"):
        fn = getattr(engine, m, None)
        if callable(fn):
            res = fn(symbols)
            if inspect.isawaitable(res):
                res = await res
            if isinstance(res, list):
                return [_as_dict(x) for x in res]

    # fallback: per-symbol
    out: List[Dict[str, Any]] = []
    single = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_quote", None)
    if not callable(single):
        return out

    sem = asyncio.Semaphore(12)

    async def one(sym: str) -> Dict[str, Any]:
        async with sem:
            r = single(sym)
            if inspect.isawaitable(r):
                r = await r
            return _as_dict(r)

    out = await asyncio.gather(*[one(s) for s in symbols], return_exceptions=False)
    return out


# =============================================================================
# Quote fetch (http)
# =============================================================================
async def _fetch_quotes_http(http: HTTPClient, symbols: List[str]) -> List[Dict[str, Any]]:
    # Use /v1/enriched/quotes (your router supports POST)
    payload = {
        "symbols": symbols,
        "format": "items",
        "include_raw": False,
        "debug": False,
    }
    data = await http.post_json("/v1/enriched/quotes", payload)
    items = data.get("items")
    if isinstance(items, list):
        return [_as_dict(x) for x in items]
    # fallback shapes
    for k in ("data", "results", "quotes"):
        v = data.get(k)
        if isinstance(v, list):
            return [_as_dict(x) for x in v]
    return []


# =============================================================================
# Ranking logic (aligned to your fields)
# =============================================================================
def _horizon_roi_key(h: str) -> str:
    hh = (h or "").strip().lower()
    if hh in ("1m", "1month", "30d"):
        return "expected_roi_1m"
    if hh in ("12m", "12month", "1y", "365d"):
        return "expected_roi_12m"
    return "expected_roi_3m"


def _extract_expected_roi(q: Dict[str, Any], horizon: str) -> Optional[float]:
    k = _horizon_roi_key(horizon)
    v = q.get(k)
    if v is None:
        # some engines use expected_roi_1m/3m/12m, others percent keys
        alt = {
            "expected_roi_1m": ("expected_roi_1m", "roi_1m", "expected_roi_pct_1m", "expected_roi_percent_1m"),
            "expected_roi_3m": ("expected_roi_3m", "roi_3m", "expected_roi_pct_3m", "expected_roi_percent_3m"),
            "expected_roi_12m": ("expected_roi_12m", "roi_12m", "expected_roi_pct_12m", "expected_roi_percent_12m"),
        }.get(k, ())
        for a in alt:
            if a in q and q.get(a) is not None:
                v = q.get(a)
                break
    # ROI is stored as fraction in your system; still accept percent inputs
    return _pct_to_fraction(v)


def _extract_confidence(q: Dict[str, Any]) -> Optional[float]:
    c = q.get("forecast_confidence")
    if c is None:
        c = q.get("confidence_score") or q.get("ai_confidence") or q.get("confidence")
    f = _pct_to_fraction(c)
    if f is None:
        return None
    return _clamp(float(f), 0.0, 1.0)


def _composite_score(
    *,
    roi: Optional[float],
    conf: Optional[float],
    overall: Optional[float],
    risk: Optional[float],
    cfg: ScanConfig,
) -> float:
    # missing values => 0 contribution
    roi_v = float(roi or 0.0)
    conf_v = float(conf or 0.0)
    overall_v = float(overall or 0.0)

    # risk penalty: prefer <= cfg.max_risk_score
    risk_v = float(risk) if risk is not None else 60.0
    risk_penalty = 0.0
    if risk_v > cfg.max_risk_score:
        # smooth penalty, max -0.30
        risk_penalty = min(0.30, (risk_v - cfg.max_risk_score) / 100.0)

    # ROI normalized (cap between -30% and +60% for scoring)
    roi_norm = _clamp((roi_v + 0.30) / (0.60 + 0.30), 0.0, 1.0)

    score = (
        cfg.w_roi * roi_norm
        + cfg.w_conf * _clamp(conf_v, 0.0, 1.0)
        + cfg.w_score * _clamp(overall_v / 100.0, 0.0, 1.0)
        - risk_penalty
    )
    return float(_clamp(score, 0.0, 1.0) * 100.0)


def _passes_filters(q: Dict[str, Any], cfg: ScanConfig) -> Tuple[bool, str]:
    price = _safe_float(q.get("current_price") or q.get("price"))
    volume = _safe_float(q.get("volume"))
    risk = _safe_float(q.get("risk_score"))
    conf = _extract_confidence(q)
    roi = _extract_expected_roi(q, cfg.horizon)

    if price is None or price < cfg.min_price:
        return False, "min_price"
    if volume is None or volume < cfg.min_volume:
        return False, "min_volume"
    if risk is not None and risk > cfg.max_risk_score:
        return False, "max_risk"
    if conf is not None and conf < cfg.min_confidence:
        return False, "min_confidence"
    if roi is not None and roi < cfg.min_expected_roi:
        return False, "min_expected_roi"

    # if key fields missing, still allow but it will rank low
    return True, ""


def _build_rows(quotes: List[Dict[str, Any]], cfg: ScanConfig) -> List[ScanRow]:
    rows: List[ScanRow] = []
    for q in quotes:
        if not isinstance(q, dict):
            continue
        sym = _safe_str(q.get("symbol") or q.get("symbol_normalized") or q.get("requested_symbol")).upper()
        if not sym:
            continue

        ok, _why = _passes_filters(q, cfg)
        if not ok:
            continue

        price = _safe_float(q.get("current_price") or q.get("price"))
        volume = _safe_float(q.get("volume"))
        market = _safe_str(q.get("market") or q.get("exchange") or q.get("origin") or "")
        currency = _safe_str(q.get("currency") or "")
        name = _safe_str(q.get("name") or q.get("company_name") or "")

        roi = _extract_expected_roi(q, cfg.horizon)
        conf = _extract_confidence(q)
        overall = _safe_float(q.get("overall_score"))
        risk = _safe_float(q.get("risk_score"))
        value_s = _safe_float(q.get("value_score"))
        quality_s = _safe_float(q.get("quality_score"))
        momentum_s = _safe_float(q.get("momentum_score"))

        rsi_14 = _safe_float(q.get("rsi_14"))
        vol30 = _pct_to_fraction(q.get("volatility_30d"))
        if vol30 is not None:
            vol30 = float(vol30)  # fraction

        comp = _composite_score(roi=roi, conf=conf, overall=overall, risk=risk, cfg=cfg)

        rows.append(
            ScanRow(
                rank=0,
                symbol=sym,
                name=name,
                market=market or ("KSA" if sym.endswith(".SR") else "GLOBAL"),
                currency=currency,
                price=price,
                volume=volume,
                expected_roi=roi,
                forecast_confidence=conf,
                overall_score=overall,
                risk_score=risk,
                value_score=value_s,
                quality_score=quality_s,
                momentum_score=momentum_s,
                rsi_14=rsi_14,
                volatility_30d=vol30,
                data_quality=_safe_str(q.get("data_quality") or ""),
                data_source=_safe_str(q.get("data_source") or q.get("engine_source") or ""),
                last_updated_utc=_safe_str(q.get("last_updated_utc") or ""),
                composite_rank_score=comp,
            )
        )

    # rank: composite, then ROI, then confidence, then overall
    def key(r: ScanRow) -> Tuple[float, float, float, float]:
        return (
            float(r.composite_rank_score or 0.0),
            float(r.expected_roi or 0.0),
            float(r.forecast_confidence or 0.0),
            float(r.overall_score or 0.0),
        )

    rows.sort(key=key, reverse=True)
    # top N
    rows = rows[: max(1, int(cfg.top_n))]
    for i, r in enumerate(rows, 1):
        r.rank = i
    return rows


# =============================================================================
# Export
# =============================================================================
def _write_json(path: str, payload: Dict[str, Any]) -> None:
    Path(path).write_text(json_dumps(payload, indent=2), encoding="utf-8")


def _write_csv(path: str, rows: List[ScanRow]) -> None:
    if not rows:
        Path(path).write_text("", encoding="utf-8")
        return
    keys = list(rows[0].to_dict().keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            d = r.to_dict()
            # pretty ROI/conf
            if d.get("expected_roi") is not None:
                d["expected_roi"] = float(d["expected_roi"])  # fraction
            if d.get("forecast_confidence") is not None:
                d["forecast_confidence"] = float(d["forecast_confidence"])
            w.writerow(d)


def _write_html(path: str, rows: List[ScanRow], cfg: ScanConfig) -> None:
    def fmt_pct(x: Optional[float]) -> str:
        if x is None:
            return ""
        return f"{x*100:.1f}%"

    html = [
        "<!DOCTYPE html>",
        "<html><head><meta charset='utf-8'/>",
        "<title>TFB Market Scan</title>",
        "<style>",
        "body{font-family:Arial, sans-serif; margin:20px;}",
        "table{border-collapse:collapse; width:100%;}",
        "th,td{border-bottom:1px solid #ddd; padding:8px; text-align:left;}",
        "th{background:#f2f2f2;}",
        ".num{text-align:right;}",
        "</style></head><body>",
        f"<h2>TFB Market Scan — Top {len(rows)} (horizon={cfg.horizon.upper()})</h2>",
        f"<p>Generated: {_utc_iso()}</p>",
        "<table>",
        "<tr><th>#</th><th>Symbol</th><th>Name</th><th>Market</th><th class='num'>Price</th><th class='num'>ROI</th><th class='num'>Confidence</th><th class='num'>Overall</th><th class='num'>Risk</th><th class='num'>Composite</th></tr>",
    ]
    for r in rows:
        html.append(
            "<tr>"
            f"<td>{r.rank}</td>"
            f"<td>{r.symbol}</td>"
            f"<td>{(r.name or '')}</td>"
            f"<td>{r.market}</td>"
            f"<td class='num'>{'' if r.price is None else f'{r.price:.4f}'}</td>"
            f"<td class='num'>{fmt_pct(r.expected_roi)}</td>"
            f"<td class='num'>{fmt_pct(r.forecast_confidence)}</td>"
            f"<td class='num'>{'' if r.overall_score is None else f'{r.overall_score:.1f}'}</td>"
            f"<td class='num'>{'' if r.risk_score is None else f'{r.risk_score:.1f}'}</td>"
            f"<td class='num'>{r.composite_rank_score:.1f}</td>"
            "</tr>"
        )
    html += ["</table></body></html>"]
    Path(path).write_text("\n".join(html), encoding="utf-8")


# =============================================================================
# Main
# =============================================================================
async def run_scan(cfg: ScanConfig) -> Tuple[List[ScanRow], Dict[str, Any]]:
    t0 = time.time()

    symbols = _read_symbols_from_keys(cfg.spreadsheet_id, cfg.keys, cfg.max_symbols)
    if not symbols:
        return [], {"ok": False, "error": "no_symbols", "symbols": 0}

    # fetch quotes
    quotes: List[Dict[str, Any]] = []
    if cfg.mode == "engine":
        eng = await get_engine()
        if eng is None:
            raise RuntimeError("Engine mode selected but core.data_engine_v2.get_engine() returned None.")
        # chunk to reduce memory and improve responsiveness
        for i in range(0, len(symbols), max(10, cfg.chunk_size)):
            chunk = symbols[i : i + cfg.chunk_size]
            got = await _fetch_quotes_engine(eng, chunk)
            quotes.extend(got)
    else:
        http = HTTPClient(cfg.backend_base_url, cfg.token, timeout_sec=cfg.timeout_sec)  # type: ignore[attr-defined]
        try:
            for i in range(0, len(symbols), max(10, cfg.chunk_size)):
                chunk = symbols[i : i + cfg.chunk_size]
                got = await _fetch_quotes_http(http, chunk)
                quotes.extend(got)
        finally:
            await http.close()

    rows = _build_rows(quotes, cfg)

    meta = {
        "ok": True,
        "version": SCRIPT_VERSION,
        "mode": cfg.mode,
        "keys": cfg.keys,
        "horizon": cfg.horizon,
        "symbols_in": len(symbols),
        "quotes_fetched": len(quotes),
        "rows_out": len(rows),
        "filters": {
            "min_price": cfg.min_price,
            "min_volume": cfg.min_volume,
            "max_risk_score": cfg.max_risk_score,
            "min_confidence": cfg.min_confidence,
            "min_expected_roi": cfg.min_expected_roi,
        },
        "timing_ms": round((time.time() - t0) * 1000.0, 2),
        "generated_utc": _utc_iso(),
    }
    return rows, meta


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=f"TFB Market Scanner v{SCRIPT_VERSION}")
    p.add_argument("--sheet-id", default="", help="Spreadsheet ID (defaults to settings/env)")
    p.add_argument("--keys", nargs="*", default=["Market_Leaders"], help="Pages to scan (canonical keys)")
    p.add_argument("--mode", choices=["engine", "http"], default="engine", help="Fetch mode")
    p.add_argument("--backend", default="", help="Backend base URL for http mode")
    p.add_argument("--token", default="", help="Auth token for http mode")

    p.add_argument("--top", type=int, default=50, help="Top N results")
    p.add_argument("--max-symbols", type=int, default=1500, help="Max symbols to scan")
    p.add_argument("--chunk-size", type=int, default=60, help="Batch chunk size for fetches")

    p.add_argument("--horizon", choices=["1m", "3m", "12m"], default="3m", help="ROI horizon to rank/filter")
    p.add_argument("--min-price", type=float, default=1.0, help="Minimum price")
    p.add_argument("--min-volume", type=float, default=1000.0, help="Minimum volume")
    p.add_argument("--max-risk", type=float, default=60.0, help="Max risk_score (<=60 recommended)")
    p.add_argument("--min-confidence", type=float, default=0.65, help="Min forecast_confidence (0..1)")
    p.add_argument("--min-roi", type=float, default=0.0, help="Min expected ROI as FRACTION (0.10=10%)")

    p.add_argument("--export-json", default="", help="Write JSON to file")
    p.add_argument("--export-csv", default="", help="Write CSV to file")
    p.add_argument("--export-html", default="", help="Write HTML to file")

    return p.parse_args()



# =============================================================================
# Worker integration API (FIX v5.2.0)
# =============================================================================

# Documents the expected task payload for worker.py dispatching task_type="market_scan".
# Keys match ScanConfig.from_worker_payload() field names.
WORKER_PAYLOAD_SCHEMA: Dict[str, Any] = {
    "task_type": "market_scan",        # (required) always "market_scan"
    "spreadsheet_id": "",              # (required if DEFAULT_SPREADSHEET_ID not set)
    "keys": ["Market_Leaders"],        # (optional) pages to scan
    "top_n": 50,                       # (optional) top N results
    "max_symbols": 1500,               # (optional) max symbols to process
    "chunk_size": 60,                  # (optional) batch size per fetch
    "mode": "engine",                  # (optional) "engine" | "http"
    "backend_url": "",                 # (optional) override BACKEND_BASE_URL
    "token": "",                       # (optional) auth token
    "timeout_sec": 60.0,               # (optional) request timeout
    "min_price": 1.0,                  # (optional) filter: min price
    "min_volume": 1000.0,              # (optional) filter: min volume
    "max_risk_score": 60.0,            # (optional) filter: max risk score (0-100)
    "min_confidence": 0.65,            # (optional) filter: min forecast confidence (0-1)
    "min_expected_roi": 0.00,          # (optional) filter: min expected ROI (fraction)
    "horizon": "3m",                   # (optional) "1m" | "3m" | "12m"
    "w_roi": 0.55,                     # (optional) composite score weight: ROI
    "w_conf": 0.25,                    # (optional) composite score weight: confidence
    "w_score": 0.20,                   # (optional) composite score weight: overall_score
    "export_json": "",                 # (optional) path to write JSON output
    "export_csv": "",                  # (optional) path to write CSV output
    "export_html": "",                 # (optional) path to write HTML output
    # --- worker tracing fields (added by worker.py, not required here) ---
    "task_id": "",                     # worker task UUID for correlation
    "queued_at": "",                   # ISO timestamp when task was queued
    "retry_count": 0,                  # retry count
}


async def run_from_worker_payload_async(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Programmatic async entry point for worker.py.

    Accepts a task payload dict, builds ScanConfig via from_worker_payload(),
    runs the scan, and returns a result dict for the worker to log and ACK.

    Returns dict with: status, exit_code, rows_count, meta, errors, task_id.
    """
    task_id = str(payload.get("task_id") or "")
    task_type = str(payload.get("task_type") or "").strip()
    if task_type and task_type != "market_scan":
        return {
            "status": "failed",
            "exit_code": 2,
            "rows_count": 0,
            "meta": {},
            "errors": [
                f"run_from_worker_payload: expected task_type='market_scan', got {task_type!r}"
            ],
            "task_id": task_id,
        }

    try:
        cfg = ScanConfig.from_worker_payload(payload)
    except Exception as e:
        return {
            "status": "failed",
            "exit_code": 2,
            "rows_count": 0,
            "meta": {},
            "errors": [f"ScanConfig.from_worker_payload failed: {e}"],
            "task_id": task_id,
        }

    try:
        rows, meta = await run_scan(cfg)
        if rows and cfg.export_json:
            try:
                _write_json(cfg.export_json, {"meta": meta, "rows": [r.to_dict() for r in rows]})
            except Exception as e:
                logger.warning("Worker export_json failed: %s", e)
        if rows and cfg.export_csv:
            try:
                _write_csv(cfg.export_csv, rows)
            except Exception as e:
                logger.warning("Worker export_csv failed: %s", e)

        return {
            "status": "success" if rows else "partial",
            "exit_code": 0 if rows else 1,
            "rows_count": len(rows),
            "top_symbol": rows[0].symbol if rows else None,
            "meta": meta,
            "errors": [],
            "task_id": task_id,
        }
    except Exception as e:
        logger.exception("run_from_worker_payload scan failed: %s", e)
        return {
            "status": "failed",
            "exit_code": 2,
            "rows_count": 0,
            "meta": {},
            "errors": [str(e)],
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
            "rows_count": 0,
            "meta": {},
            "errors": [str(e)],
            "task_id": str(payload.get("task_id") or ""),
        }

def main() -> int:
    args = _parse_args()

    if args.mode == "engine" and not _CORE_AVAILABLE:
        logger.error("Engine mode requires running inside the repo (core.* imports missing). Use --mode http instead.")
        return 2
    if args.mode == "http" and not _AIOHTTP_AVAILABLE:
        logger.error("HTTP mode requires aiohttp installed.")
        return 2

    sid = _resolve_spreadsheet_id(args.sheet_id)
    if not sid:
        logger.error("Missing spreadsheet id. Use --sheet-id or set DEFAULT_SPREADSHEET_ID / settings.default_spreadsheet_id")
        return 2

    keys = [_canon_key(k) for k in (args.keys or [])]
    keys = [k for k in keys if k]
    if not keys:
        keys = ["Market_Leaders"]

    backend = _resolve_backend_url(args.backend)
    token = _resolve_token(args.token)

    cfg = ScanConfig(
        spreadsheet_id=sid,
        keys=keys,
        top_n=max(1, int(args.top)),
        max_symbols=max(0, int(args.max_symbols)),
        chunk_size=max(10, int(args.chunk_size)),
        concurrency=10,
        mode=args.mode,
        backend_base_url=backend,
        token=token,
        min_price=float(args.min_price),
        min_volume=float(args.min_volume),
        max_risk_score=float(args.max_risk),
        min_confidence=float(args.min_confidence),
        min_expected_roi=float(args.min_roi),
        horizon=str(args.horizon).lower(),
        export_json=str(args.export_json or ""),
        export_csv=str(args.export_csv or ""),
        export_html=str(args.export_html or ""),
    )

    logger.info("=== Market Scan v%s ===", SCRIPT_VERSION)
    logger.info("Mode: %s | Sheet: %s | Keys: %s", cfg.mode, cfg.spreadsheet_id, ",".join(cfg.keys))
    logger.info(
        "Filters: min_price=%.2f | min_volume=%.0f | max_risk=%.1f | min_conf=%.2f | min_roi=%.2f%% | horizon=%s",
        cfg.min_price,
        cfg.min_volume,
        cfg.max_risk_score,
        cfg.min_confidence,
        cfg.min_expected_roi * 100.0,
        cfg.horizon,
    )
    if cfg.mode == "http":
        if not cfg.token:
            logger.warning("No token found. If backend requires auth, request may fail with 401.")
        logger.info("Backend: %s", cfg.backend_base_url)

    try:
        rows, meta = asyncio.run(run_scan(cfg))
    except KeyboardInterrupt:
        logger.info("Interrupted.")
        return 130
    except Exception as e:
        logger.error("Scan failed: %s", e)
        return 1

    # Console summary
    logger.info("Result: rows=%d | symbols=%s | ms=%s", len(rows), meta.get("symbols_in"), meta.get("timing_ms"))
    if rows:
        top = rows[0]
        logger.info(
            "Top pick: %s | ROI=%s | Conf=%s | Overall=%s | Risk=%s | Composite=%.1f",
            top.symbol,
            "" if top.expected_roi is None else f"{top.expected_roi*100:.1f}%",
            "" if top.forecast_confidence is None else f"{top.forecast_confidence*100:.1f}%",
            "" if top.overall_score is None else f"{top.overall_score:.1f}",
            "" if top.risk_score is None else f"{top.risk_score:.1f}",
            top.composite_rank_score,
        )

    # Exports
    payload = {"meta": meta, "rows": [r.to_dict() for r in rows]}
    try:
        if cfg.export_json:
            _write_json(cfg.export_json, payload)
            logger.info("JSON saved: %s", cfg.export_json)
        if cfg.export_csv:
            _write_csv(cfg.export_csv, rows)
            logger.info("CSV saved: %s", cfg.export_csv)
        if cfg.export_html:
            _write_html(cfg.export_html, rows, cfg)
            logger.info("HTML saved: %s", cfg.export_html)
    except Exception as e:
        logger.warning("Export failed: %s", e)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
