# core/price_history_store.py
"""
core/price_history_store.py
===========================================================
Price History Store + ROI Calculators (Horizon-in-days) — v1.0.0

Purpose
- Store time-series price points per symbol (in-memory, optional JSON persistence).
- Provide trailing ROI calculators by horizon (e.g., 30D / 90D / 365D).
- Support dashboard logic where "Investment Period" is always in DAYS and mapped
  to forecast horizons (1M/3M/12M) for reporting consistency.

Design goals
- Safe + defensive: never raises during normal ROI calculation; returns None.
- Fast: O(log n) lookups via bisect for nearest historical point.
- Async-friendly: pure python, thread-safe; can be called from async code.
- Production-friendly: optional persistence to a JSON file (best effort).

Notes
- This module intentionally does NOT fetch prices. It only stores and computes.
- Integrate by calling store.record_quote(UnifiedQuote) after each successful quote.
"""

from __future__ import annotations

import json
import os
import threading
import time
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple


STORE_VERSION = "1.0.0"
UTC = timezone.utc


# -----------------------------
# Helpers
# -----------------------------

def _utcnow() -> datetime:
    return datetime.now(UTC)

def _dt_from_any(x: Any) -> Optional[datetime]:
    if x is None:
        return None
    if isinstance(x, datetime):
        if x.tzinfo is None:
            return x.replace(tzinfo=UTC)
        return x.astimezone(UTC)
    # ISO string?
    try:
        s = str(x).strip()
        if not s:
            return None
        # handle 'Z'
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return None

def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        f = float(x)
        if f != f:  # NaN
            return None
        if f == float("inf") or f == float("-inf"):
            return None
        return f
    except Exception:
        return None

def _norm_symbol(symbol: str) -> str:
    """
    Normalize symbol gently, without importing core.data_engine_v2 (avoid cycles).
    - trims
    - uppercases
    - numeric -> ####.SR
    """
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.isdigit():
        return f"{s}.SR"
    return s


# -----------------------------
# Data structures
# -----------------------------

@dataclass(frozen=True)
class PricePoint:
    ts_utc: datetime
    price: float
    source: str = ""

    def to_json(self) -> Dict[str, Any]:
        return {"ts_utc": self.ts_utc.isoformat(), "price": self.price, "source": self.source}

    @staticmethod
    def from_json(d: Dict[str, Any]) -> Optional["PricePoint"]:
        try:
            ts = _dt_from_any(d.get("ts_utc"))
            px = _safe_float(d.get("price"))
            if ts is None or px is None:
                return None
            src = str(d.get("source") or "")
            return PricePoint(ts_utc=ts, price=px, source=src)
        except Exception:
            return None


# -----------------------------
# Main store
# -----------------------------

class PriceHistoryStore:
    """
    Thread-safe per-symbol price history store.

    Storage model:
      self._series[symbol] = [(ts_epoch_seconds, price, source), ...]  sorted by ts asc
    """

    def __init__(
        self,
        max_points_per_symbol: int = 2000,
        retention_days: int = 400,
        persist_path: Optional[str] = None,
        autosave_every_sec: int = 30,
    ) -> None:
        self.max_points_per_symbol = max(100, int(max_points_per_symbol or 2000))
        self.retention_days = max(30, int(retention_days or 400))
        self.persist_path = (persist_path or "").strip() or None
        self.autosave_every_sec = max(5, int(autosave_every_sec or 30))

        self._lock = threading.RLock()
        self._series: Dict[str, List[Tuple[float, float, str]]] = {}
        self._dirty = False
        self._last_save_ts = 0.0

        if self.persist_path:
            self._load_best_effort()

    # -----------------------------
    # Persistence
    # -----------------------------

    def _load_best_effort(self) -> None:
        path = self.persist_path
        if not path:
            return
        try:
            if not os.path.exists(path):
                return
            with open(path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if not isinstance(raw, dict):
                return
            data = raw.get("data")
            if not isinstance(data, dict):
                return

            with self._lock:
                rebuilt: Dict[str, List[Tuple[float, float, str]]] = {}
                for sym, points in data.items():
                    if not isinstance(sym, str) or not isinstance(points, list):
                        continue
                    s = _norm_symbol(sym)
                    arr: List[Tuple[float, float, str]] = []
                    for p in points:
                        if not isinstance(p, dict):
                            continue
                        pp = PricePoint.from_json(p)
                        if not pp:
                            continue
                        arr.append((pp.ts_utc.timestamp(), pp.price, pp.source))
                    arr.sort(key=lambda x: x[0])
                    if arr:
                        rebuilt[s] = arr[-self.max_points_per_symbol :]
                self._series = rebuilt
                self._dirty = False
                self._last_save_ts = time.time()
        except Exception:
            # Best-effort load; never crash app init
            return

    def _save_best_effort(self, force: bool = False) -> None:
        path = self.persist_path
        if not path:
            return

        now = time.time()
        with self._lock:
            if not self._dirty and not force:
                return
            if not force and (now - self._last_save_ts) < self.autosave_every_sec:
                return

            payload: Dict[str, Any] = {
                "version": STORE_VERSION,
                "saved_utc": _utcnow().isoformat(),
                "data": {},
            }

            for sym, arr in self._series.items():
                pts = []
                for ts, px, src in arr:
                    dt = datetime.fromtimestamp(ts, tz=UTC)
                    pts.append(PricePoint(dt, px, src).to_json())
                payload["data"][sym] = pts

            tmp = f"{path}.tmp"
            try:
                os.makedirs(os.path.dirname(path), exist_ok=True)
            except Exception:
                pass

            try:
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False)
                os.replace(tmp, path)
                self._dirty = False
                self._last_save_ts = now
            except Exception:
                # Best-effort save; do not crash
                try:
                    if os.path.exists(tmp):
                        os.remove(tmp)
                except Exception:
                    pass

    def flush(self) -> None:
        """Force persistence now (best effort)."""
        self._save_best_effort(force=True)

    # -----------------------------
    # Recording
    # -----------------------------

    def record_price(
        self,
        symbol: str,
        price: Any,
        ts_utc: Optional[Any] = None,
        source: str = "",
        allow_overwrite_same_second: bool = True,
    ) -> bool:
        """
        Add a price point. Returns True if recorded, False if ignored.
        """
        sym = _norm_symbol(symbol)
        px = _safe_float(price)
        if not sym or px is None or px <= 0:
            return False

        ts = _dt_from_any(ts_utc) or _utcnow()
        ts_epoch = ts.timestamp()

        cutoff = (ts - timedelta(days=self.retention_days)).timestamp()

        with self._lock:
            arr = self._series.get(sym) or []
            # drop old
            if arr and arr[0][0] < cutoff:
                # keep only those >= cutoff
                i0 = 0
                while i0 < len(arr) and arr[i0][0] < cutoff:
                    i0 += 1
                arr = arr[i0:]

            # insert sorted by ts
            idx = bisect_right([x[0] for x in arr], ts_epoch)
            # same-second overwrite (reduces duplicates from frequent refresh)
            if allow_overwrite_same_second and arr:
                # check neighbor(s)
                if idx - 1 >= 0 and int(arr[idx - 1][0]) == int(ts_epoch):
                    arr[idx - 1] = (ts_epoch, px, source or arr[idx - 1][2])
                elif idx < len(arr) and int(arr[idx][0]) == int(ts_epoch):
                    arr[idx] = (ts_epoch, px, source or arr[idx][2])
                else:
                    arr.insert(idx, (ts_epoch, px, source))
            else:
                arr.insert(idx, (ts_epoch, px, source))

            # cap points
            if len(arr) > self.max_points_per_symbol:
                arr = arr[-self.max_points_per_symbol :]

            self._series[sym] = arr
            self._dirty = True

        self._save_best_effort(force=False)
        return True

    def record_quote(self, q: Any) -> bool:
        """
        Accepts UnifiedQuote-like objects/dicts:
          - symbol
          - current_price (or last_price)
          - last_updated_utc (optional)
          - data_source (optional)
        """
        if q is None:
            return False
        try:
            if isinstance(q, dict):
                sym = q.get("symbol") or q.get("ticker")
                px = q.get("current_price") or q.get("last_price") or q.get("price")
                ts = q.get("last_updated_utc") or q.get("timestamp") or q.get("ts_utc")
                src = str(q.get("data_source") or q.get("source") or "")
            else:
                sym = getattr(q, "symbol", None) or getattr(q, "ticker", None)
                px = getattr(q, "current_price", None) or getattr(q, "last_price", None) or getattr(q, "price", None)
                ts = getattr(q, "last_updated_utc", None) or getattr(q, "timestamp", None) or getattr(q, "ts_utc", None)
                src = str(getattr(q, "data_source", "") or getattr(q, "source", "") or "")
            return self.record_price(str(sym or ""), px, ts_utc=ts, source=src)
        except Exception:
            return False

    # -----------------------------
    # Retrieval
    # -----------------------------

    def get_latest(self, symbol: str) -> Optional[PricePoint]:
        sym = _norm_symbol(symbol)
        if not sym:
            return None
        with self._lock:
            arr = self._series.get(sym) or []
            if not arr:
                return None
            ts, px, src = arr[-1]
            return PricePoint(datetime.fromtimestamp(ts, tz=UTC), px, src)

    def get_series(self, symbol: str, since_days: Optional[int] = None) -> List[PricePoint]:
        sym = _norm_symbol(symbol)
        if not sym:
            return []
        with self._lock:
            arr = list(self._series.get(sym) or [])
        if not arr:
            return []
        if since_days is not None and since_days > 0:
            cutoff = (_utcnow() - timedelta(days=int(since_days))).timestamp()
            arr = [x for x in arr if x[0] >= cutoff]
        return [PricePoint(datetime.fromtimestamp(ts, tz=UTC), px, src) for ts, px, src in arr]

    def _price_on_or_before(self, symbol: str, ts_utc: datetime, max_backfill_days: int = 30) -> Optional[PricePoint]:
        """
        Find the last known price at or before ts_utc.
        If nothing <= ts_utc exists, returns earliest point ONLY if it is within max_backfill_days,
        else returns None.
        """
        sym = _norm_symbol(symbol)
        if not sym:
            return None
        target = (ts_utc.astimezone(UTC)).timestamp()
        with self._lock:
            arr = self._series.get(sym) or []
            if not arr:
                return None

            times = [x[0] for x in arr]
            idx = bisect_right(times, target) - 1
            if idx >= 0:
                ts, px, src = arr[idx]
                return PricePoint(datetime.fromtimestamp(ts, tz=UTC), px, src)

            # no earlier point; fallback to earliest only if close enough
            ts0, px0, src0 = arr[0]
            if max_backfill_days is not None and max_backfill_days > 0:
                if (target - ts0) > (max_backfill_days * 86400):
                    return None
            return PricePoint(datetime.fromtimestamp(ts0, tz=UTC), px0, src0)

    # -----------------------------
    # ROI calculators
    # -----------------------------

    @staticmethod
    def roi_percent(start_price: float, end_price: float) -> Optional[float]:
        if start_price is None or end_price is None:
            return None
        try:
            sp = float(start_price)
            ep = float(end_price)
            if sp <= 0:
                return None
            return (ep / sp - 1.0) * 100.0
        except Exception:
            return None

    def trailing_roi(
        self,
        symbol: str,
        horizon_days: int,
        as_of_utc: Optional[Any] = None,
        max_backfill_days: int = 30,
    ) -> Optional[float]:
        """
        Trailing ROI% over the last horizon_days.

        Example: horizon_days=30
        ROI = (Price(as_of) / Price(as_of - 30d) - 1) * 100
        """
        sym = _norm_symbol(symbol)
        if not sym:
            return None
        h = int(horizon_days or 0)
        if h <= 0:
            return None

        end_ts = _dt_from_any(as_of_utc)
        if end_ts is None:
            latest = self.get_latest(sym)
            if not latest:
                return None
            end_ts = latest.ts_utc
            end_price = latest.price
        else:
            end_pp = self._price_on_or_before(sym, end_ts, max_backfill_days=max_backfill_days)
            if not end_pp:
                return None
            end_ts = end_pp.ts_utc
            end_price = end_pp.price

        start_ts = end_ts - timedelta(days=h)
        start_pp = self._price_on_or_before(sym, start_ts, max_backfill_days=max_backfill_days)
        if not start_pp:
            return None

        return self.roi_percent(start_pp.price, end_price)

    def trailing_roi_horizons(
        self,
        symbol: str,
        horizons_days: Iterable[int] = (30, 90, 365),
        as_of_utc: Optional[Any] = None,
        max_backfill_days: int = 30,
    ) -> Dict[int, Optional[float]]:
        out: Dict[int, Optional[float]] = {}
        for h in horizons_days:
            try:
                hh = int(h)
            except Exception:
                continue
            out[hh] = self.trailing_roi(symbol, hh, as_of_utc=as_of_utc, max_backfill_days=max_backfill_days)
        return out

    # -----------------------------
    # Investment Period mapping (days)
    # -----------------------------

    @staticmethod
    def map_period_days_to_horizons(period_days: Any) -> List[int]:
        """
        Dashboard rule: Investment Period is in DAYS, mapped internally to horizons.

        Default mapping:
          - <= 45 days  -> [30]
          - <= 135 days -> [30, 90]
          - else        -> [30, 90, 365]
        """
        try:
            d = int(float(period_days))
        except Exception:
            d = 0
        if d <= 0:
            return [30, 90]  # safe default
        if d <= 45:
            return [30]
        if d <= 135:
            return [30, 90]
        return [30, 90, 365]


# -----------------------------
# Singleton accessor (optional)
# -----------------------------

_GLOBAL_STORE: Optional[PriceHistoryStore] = None

def get_price_history_store() -> PriceHistoryStore:
    """
    Global store to avoid re-loading persistence multiple times.

    Configure via env vars:
      PRICE_HISTORY_PATH        (e.g., /tmp/price_history.json)
      PRICE_HISTORY_RETENTION_DAYS
      PRICE_HISTORY_MAX_POINTS
      PRICE_HISTORY_AUTOSAVE_SEC
    """
    global _GLOBAL_STORE
    if _GLOBAL_STORE is not None:
        return _GLOBAL_STORE

    persist_path = (os.getenv("PRICE_HISTORY_PATH", "") or "").strip() or None
    retention_days = int(float(os.getenv("PRICE_HISTORY_RETENTION_DAYS", "400") or "400"))
    max_points = int(float(os.getenv("PRICE_HISTORY_MAX_POINTS", "2000") or "2000"))
    autosave = int(float(os.getenv("PRICE_HISTORY_AUTOSAVE_SEC", "30") or "30"))

    _GLOBAL_STORE = PriceHistoryStore(
        max_points_per_symbol=max_points,
        retention_days=retention_days,
        persist_path=persist_path,
        autosave_every_sec=autosave,
    )
    return _GLOBAL_STORE


__all__ = [
    "STORE_VERSION",
    "PricePoint",
    "PriceHistoryStore",
    "get_price_history_store",
]
