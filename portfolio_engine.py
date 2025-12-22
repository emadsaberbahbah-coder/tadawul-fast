# portfolio_engine.py
"""
PortfolioEngine – My Investment (Income-Statement Style) – v1.1.0 (HARDENED)
----------------------------------------------------------------------------
FULL UPDATED SCRIPT (ONE-SHOT)

Upgrades vs v1.0.0:
✅ Handles percent inputs safely (stores output as fraction for Sheets % format)
✅ Robust row parsing (pads/trims, ignores totally blank rows)
✅ Better status handling (Active/Sold + case/typos)
✅ Optional "Sell Price / Sell Quantity / Sell Date" support if columns exist
   (still works perfectly if those columns are NOT present)
✅ KPI logic counts only ACTIVE positions for exposure KPIs (configurable)
✅ No crashes on bad data / weird strings
✅ Keeps PORTFOLIO_HEADERS stable for app.py (backward compatible)
✅ Efficient: single header index map, minimal conversions

Public API used by app.py:
  PORTFOLIO_HEADERS
  build_price_map_from_market_data(market_headers, market_rows, ...)
  PortfolioEngine().compute_table(portfolio_rows, price_map) -> (updated_rows, kpi)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


# =============================================================================
# Constants (must match app.py ensure_headers / your sheet)
# =============================================================================

PORTFOLIO_HEADERS: List[str] = [
    "Ticker",
    "Buy Date",
    "Buy Price",
    "Quantity",
    "Total Cost",
    "Current Price",
    "Current Value",
    "Unrealized P/L",
    "Unrealized P/L %",
    "Realized P/L",
    "Status (Active/Sold)",
    "Notes",
    "Updated At (UTC)",
]

# Optional sell columns (ONLY used if the sheet contains them)
OPTIONAL_SELL_HEADERS = [
    "Sell Date",
    "Sell Price",
    "Sell Quantity",
]


# =============================================================================
# Helpers
# =============================================================================

def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def normalize_ticker(t: Any) -> str:
    return (str(t or "").strip().upper())


def safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def safe_float(x: Any) -> Optional[float]:
    """
    Accepts:
      123
      "123"
      "1,234.56"
      "5%"  -> 0.05   (fraction)
      "0.05" -> 0.05
      ""/NA/null -> None
    """
    try:
        if x is None or isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            v = float(x)
            if v != v:  # NaN
                return None
            return v
        s = str(x).strip().replace(",", "")
        if not s or s.lower() in {"na", "n/a", "none", "null", "-"}:
            return None
        if s.endswith("%"):
            return float(s[:-1].strip()) / 100.0
        return float(s)
    except Exception:
        return None


def header_index_map(headers: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate(headers)}


def _is_blank_row(row: List[Any]) -> bool:
    if not row:
        return True
    for v in row:
        if safe_str(v).strip() != "":
            return False
    return True


def _normalize_status(raw: Any) -> str:
    s = safe_str(raw).strip().lower()
    if not s:
        return "Active"
    # common typos/variants
    if s.startswith("a"):
        return "Active"
    if s.startswith("s"):
        return "Sold"
    if "close" in s or "closed" in s:
        return "Sold"
    if "inactive" in s:
        return "Sold"
    return "Active"


def _round_or_blank(x: Optional[float], nd: int = 2):
    return round(x, nd) if x is not None else ""


# =============================================================================
# Market Data -> Price Map
# =============================================================================

def build_price_map_from_market_data(
    market_headers: List[str],
    market_rows: List[List[Any]],
    ticker_col_name: str = "Ticker",
    price_col_name: str = "Current Price",
) -> Dict[str, float]:
    """
    Build {TICKER: current_price} from Market Data table.
    market_rows is data-only (no header) OR can include header; we ignore non-numeric prices.
    """
    if not market_headers:
        return {}

    idx = header_index_map(market_headers)
    t_i = idx.get(ticker_col_name)
    p_i = idx.get(price_col_name)
    if t_i is None or p_i is None:
        return {}

    out: Dict[str, float] = {}
    for r in (market_rows or []):
        if not r or len(r) <= max(t_i, p_i):
            continue
        t = normalize_ticker(r[t_i])
        p = safe_float(r[p_i])
        if t and p is not None:
            out[t] = float(p)
    return out


# =============================================================================
# KPI dataclass
# =============================================================================

@dataclass
class PortfolioKPI:
    total_cost: float
    current_value: float
    unrealized_pl: float
    unrealized_pl_pct: Optional[float]  # fraction
    positions: int
    active_positions: int


# =============================================================================
# Engine
# =============================================================================

class PortfolioEngine:
    """
    - Works with rows in the order of PORTFOLIO_HEADERS (data only)
    - If row is shorter, pads; if longer, keeps extra columns untouched
    - Optional sell columns are supported ONLY if they exist in the current header set
      (you can add them later without changing app.py)
    """

    def __init__(self, headers: Optional[List[str]] = None) -> None:
        self.headers = headers or PORTFOLIO_HEADERS
        self.idx = header_index_map(self.headers)

        # Optional sell indexes (only if present)
        self.sell_idx = header_index_map(self.headers)
        self.has_sell_date = "Sell Date" in self.sell_idx
        self.has_sell_price = "Sell Price" in self.sell_idx
        self.has_sell_qty = "Sell Quantity" in self.sell_idx

    def _pad_row(self, row: List[Any]) -> List[Any]:
        r = list(row or [])
        if len(r) < len(self.headers):
            r += [""] * (len(self.headers) - len(r))
        return r

    def _calc_realized_pl_optional(self, r: List[Any]) -> Optional[float]:
        """
        Optional realized P/L support if sell columns exist.
        Realized = (Sell Price - Buy Price) * Sell Qty
        If sheet doesn't include sell columns OR missing values -> None
        """
        if not (self.has_sell_price and self.has_sell_qty):
            return None

        buy_price = safe_float(r[self.idx.get("Buy Price", 2)])
        sell_price = safe_float(r[self.sell_idx.get("Sell Price")])
        sell_qty = safe_float(r[self.sell_idx.get("Sell Quantity")])

        if buy_price is None or sell_price is None or sell_qty is None:
            return None
        return (sell_price - buy_price) * sell_qty

    def compute_row(self, row: List[Any], price_map: Dict[str, float]) -> List[Any]:
        """
        Computes a single portfolio row aligned to self.headers.
        """
        r = self._pad_row(row)

        ticker = normalize_ticker(r[self.idx["Ticker"]])
        buy_price = safe_float(r[self.idx["Buy Price"]])
        qty = safe_float(r[self.idx["Quantity"]])
        status = _normalize_status(r[self.idx["Status (Active/Sold)"]])

        # Always rewrite normalized ticker/status
        r[self.idx["Ticker"]] = ticker
        r[self.idx["Status (Active/Sold)"]] = status

        # Total cost (only meaningful if active or if you still want it when sold)
        total_cost = (buy_price * qty) if (buy_price is not None and qty is not None) else None

        # Current price only for ACTIVE positions (you can still fill it for Sold if you prefer)
        cur_price = price_map.get(ticker) if ticker else None
        current_value = (cur_price * qty) if (cur_price is not None and qty is not None) else None

        # Unrealized for ACTIVE only (if Sold, unrealized blank)
        unreal_pl = None
        unreal_pl_pct = None
        if status == "Active" and current_value is not None and total_cost is not None:
            unreal_pl = current_value - total_cost
            if total_cost != 0:
                unreal_pl_pct = unreal_pl / total_cost  # fraction

        # Realized P/L:
        # - if optional sell columns exist and data present -> computed
        # - else if existing value in sheet -> keep/normalize
        realized_existing = safe_float(r[self.idx["Realized P/L"]])
        realized_calc = self._calc_realized_pl_optional(r)

        realized_pl = realized_calc if realized_calc is not None else (realized_existing if realized_existing is not None else 0.0)

        # Write outputs back
        r[self.idx["Total Cost"]] = _round_or_blank(total_cost, 2)
        r[self.idx["Current Price"]] = _round_or_blank(cur_price, 2) if status == "Active" else _round_or_blank(cur_price, 2)
        r[self.idx["Current Value"]] = _round_or_blank(current_value, 2) if status == "Active" else ""

        r[self.idx["Unrealized P/L"]] = _round_or_blank(unreal_pl, 2) if unreal_pl is not None else ""
        # Store percent as fraction for Google Sheets percent formatting
        r[self.idx["Unrealized P/L %"]] = float(unreal_pl_pct) if unreal_pl_pct is not None else ""

        r[self.idx["Realized P/L"]] = _round_or_blank(realized_pl, 2)

        # Timestamp
        r[self.idx["Updated At (UTC)"]] = now_utc_str()

        return r

    def compute_table(
        self,
        portfolio_rows: List[List[Any]],
        price_map: Dict[str, float],
        kpi_active_only: bool = True,
    ) -> Tuple[List[List[Any]], PortfolioKPI]:
        """
        Computes all rows + returns KPIs.
        Expects `portfolio_rows` WITHOUT the header row (data only).

        KPI behavior:
        - if kpi_active_only=True: totals computed from ACTIVE positions only
        - otherwise: totals computed from all rows
        """
        updated: List[List[Any]] = []

        total_cost_sum = 0.0
        current_value_sum = 0.0
        unreal_sum = 0.0
        positions = 0
        active_positions = 0

        for row in (portfolio_rows or []):
            if _is_blank_row(row):
                continue

            # If ticker is blank -> skip
            if normalize_ticker(row[0] if len(row) else "") == "":
                continue

            out = self.compute_row(row, price_map)
            updated.append(out)

            positions += 1
            status = safe_str(out[self.idx["Status (Active/Sold)"]]).strip()
            is_active = (status == "Active")
            if is_active:
                active_positions += 1

            # KPI totals
            if kpi_active_only and not is_active:
                continue

            tc = safe_float(out[self.idx["Total Cost"]])
            cv = safe_float(out[self.idx["Current Value"]])
            up = safe_float(out[self.idx["Unrealized P/L"]])

            if tc is not None:
                total_cost_sum += tc
            if cv is not None:
                current_value_sum += cv
            if up is not None:
                unreal_sum += up

        unreal_pct = (unreal_sum / total_cost_sum) if total_cost_sum > 0 else None

        kpi = PortfolioKPI(
            total_cost=round(total_cost_sum, 2),
            current_value=round(current_value_sum, 2),
            unrealized_pl=round(unreal_sum, 2),
            unrealized_pl_pct=(round(unreal_pct, 6) if unreal_pct is not None else None),
            positions=positions,
            active_positions=active_positions,
        )
        return updated, kpi
