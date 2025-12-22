# portfolio_engine.py
"""
PortfolioEngine – My Investment (Income-Statement Style) – v1.0.0
-----------------------------------------------------------------
NEXT SCRIPT (NEW FILE) – aligned with:
- setup_sheet.py (PORTFOLIO_HEADERS)
- app.py Market Data outputs (Current Price per ticker)

What it does:
✅ Reads "My Investment" rows (Ticker, Buy Price, Qty, etc.)
✅ Pulls Current Price from Market Data results (ticker -> price)
✅ Calculates:
   - Total Cost
   - Current Value
   - Unrealized P/L
   - Unrealized P/L %
   - Realized P/L (kept 0 unless you add optional sell fields later)
✅ Returns updated rows + portfolio KPIs summary

This file is intentionally provider-agnostic and fast.
You can call it from app.py after /api/analyze writes Market Data.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone


# =============================================================================
# Constants (must match setup_sheet.py)
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


# =============================================================================
# Helpers
# =============================================================================

def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def normalize_ticker(t: Any) -> str:
    return (str(t or "").strip().upper())


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", "")
        if s == "" or s.lower() in {"na", "n/a", "none", "null", "-"}:
            return None
        # allow percent like "5%" (rare)
        if s.endswith("%"):
            return float(s[:-1].strip()) / 100.0
        return float(s)
    except Exception:
        return None


def safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def header_index_map(headers: List[str]) -> Dict[str, int]:
    # name -> index
    return {h: i for i, h in enumerate(headers)}


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
    """
    if not market_headers:
        return {}

    idx = header_index_map(market_headers)
    t_i = idx.get(ticker_col_name)
    p_i = idx.get(price_col_name)
    if t_i is None or p_i is None:
        return {}

    out: Dict[str, float] = {}
    for r in market_rows:
        if not r or len(r) <= max(t_i, p_i):
            continue
        t = normalize_ticker(r[t_i])
        p = safe_float(r[p_i])
        if t and p is not None:
            out[t] = float(p)
    return out


# =============================================================================
# Portfolio Calculations
# =============================================================================

@dataclass
class PortfolioKPI:
    total_cost: float
    current_value: float
    unrealized_pl: float
    unrealized_pl_pct: Optional[float]
    positions: int
    active_positions: int


class PortfolioEngine:
    def __init__(self) -> None:
        self.headers = PORTFOLIO_HEADERS
        self.idx = header_index_map(self.headers)

    def compute_row(
        self,
        row: List[Any],
        price_map: Dict[str, float],
    ) -> List[Any]:
        """
        Computes a single portfolio row aligned to PORTFOLIO_HEADERS.
        If row is shorter, it's padded.
        """
        # pad row
        r = list(row or [])
        if len(r) < len(self.headers):
            r += [""] * (len(self.headers) - len(r))

        ticker = normalize_ticker(r[self.idx["Ticker"]])
        buy_price = safe_float(r[self.idx["Buy Price"]])
        qty = safe_float(r[self.idx["Quantity"]])
        status = (safe_str(r[self.idx["Status (Active/Sold)"]]).strip() or "Active").title()

        # total cost
        total_cost = (buy_price * qty) if (buy_price is not None and qty is not None) else None

        # current price from market map
        cur_price = price_map.get(ticker) if ticker else None

        # current value
        current_value = (cur_price * qty) if (cur_price is not None and qty is not None) else None

        # unrealized
        unreal_pl = (current_value - total_cost) if (current_value is not None and total_cost is not None) else None
        unreal_pl_pct = (unreal_pl / total_cost) if (unreal_pl is not None and total_cost not in (None, 0)) else None

        # realized P/L (kept as 0 unless you extend your portfolio sheet with sell fields)
        realized_pl = safe_float(r[self.idx["Realized P/L"]])
        if realized_pl is None:
            realized_pl = 0.0

        # write outputs back
        r[self.idx["Ticker"]] = ticker
        r[self.idx["Total Cost"]] = round(total_cost, 2) if total_cost is not None else ""
        r[self.idx["Current Price"]] = round(cur_price, 2) if cur_price is not None else ""
        r[self.idx["Current Value"]] = round(current_value, 2) if current_value is not None else ""
        r[self.idx["Unrealized P/L"]] = round(unreal_pl, 2) if unreal_pl is not None else ""
        # Store percent as fraction for Google Sheets percent formatting
        r[self.idx["Unrealized P/L %"]] = float(unreal_pl_pct) if unreal_pl_pct is not None else ""
        r[self.idx["Realized P/L"]] = round(realized_pl, 2) if realized_pl is not None else ""

        # normalize status
        r[self.idx["Status (Active/Sold)"]] = "Sold" if status.startswith("S") else "Active"

        # timestamp
        r[self.idx["Updated At (UTC)"]] = now_utc_str()

        return r

    def compute_table(
        self,
        portfolio_rows: List[List[Any]],
        price_map: Dict[str, float],
    ) -> Tuple[List[List[Any]], PortfolioKPI]:
        """
        Computes all rows + returns KPIs.
        Expects `portfolio_rows` WITHOUT the header row (data only).
        """
        updated: List[List[Any]] = []

        total_cost_sum = 0.0
        current_value_sum = 0.0
        unreal_sum = 0.0
        positions = 0
        active_positions = 0

        for row in portfolio_rows:
            if not row or (len(row) > 0 and normalize_ticker(row[0]) == ""):
                continue

            out = self.compute_row(row, price_map)
            updated.append(out)

            positions += 1
            status = safe_str(out[self.idx["Status (Active/Sold)"]]).strip().lower()
            if status == "active":
                active_positions += 1

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
            unrealized_pl_pct=(round(unreal_pct, 6) if unreal_pct is not None else None),  # keep fraction
            positions=positions,
            active_positions=active_positions,
        )
        return updated, kpi
