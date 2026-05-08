"""
portfolio_fixes.py
==================

Drop-in replacements for the four broken functions in the May 8, 2026 batch.

  1. calculate_52w_position()   — column was empty for every row
  2. calculate_upside_pct()     — currency-unit bug produced -70% floor on JSE/HKD
  3. calculate_expected_roi()   — was returning 0.33%/3%/8% constants for all
  4. calculate_unrealized_pnl() — was producing 4,614,849% on EXE.US

Each function is independent. Wire them in as direct replacements — no
shared state, no new dependencies.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# Fix #1 — 52W Position %
# ---------------------------------------------------------------------------

def calculate_52w_position(
    current_price: float,
    low_52w: float,
    high_52w: float,
) -> Optional[float]:
    """
    Position within the 52-week range as a percentage.
        0   = trading at the 52W low
      100   = trading at the 52W high
       50   = midpoint of the range

    Returns None if inputs are missing or invalid. All three inputs are
    already present in every row of the source file — this calc was simply
    never wired in.
    """
    if current_price is None or low_52w is None or high_52w is None:
        return None
    if high_52w <= low_52w:
        return None
    return round((current_price - low_52w) / (high_52w - low_52w) * 100.0, 2)


# ---------------------------------------------------------------------------
# Fix #2 — DCF / Upside % with currency-unit normalization
# ---------------------------------------------------------------------------

# Exchanges that quote in subdivisions of the major currency.
#   quote_currency -> (major_currency, units_per_major)
SUBDIVISION_CURRENCIES: dict[str, tuple[str, float]] = {
    'ZAC': ('ZAR', 100.0),    # South African cents (JSE)
    'GBX': ('GBP', 100.0),    # British pence (LSE)
    'GBp': ('GBP', 100.0),    # alternate notation
    'ILA': ('ILS', 100.0),    # Israeli agorot (TASE)
    'KWF': ('KWD', 1000.0),   # Kuwaiti fils
}


def normalize_to_intrinsic_currency(
    quote_price: float,
    quote_currency: str,
    intrinsic_currency: str,
) -> float:
    """
    Convert a market quote into the same unit the DCF model produced.
    The original bug: SBK.JSE quoted at 30,997 ZAC (= R 309.97) was being
    compared directly to a DCF output of 9,299 ZAR — guaranteeing the
    upside hit the -70% floor every time.
    """
    if quote_currency == intrinsic_currency:
        return quote_price
    if quote_currency in SUBDIVISION_CURRENCIES:
        major, divisor = SUBDIVISION_CURRENCIES[quote_currency]
        if intrinsic_currency == major:
            return quote_price / divisor
    raise ValueError(
        f"Cannot normalize {quote_currency} -> {intrinsic_currency} without FX rate"
    )


def calculate_upside_pct(
    current_price: float,
    intrinsic_value: float,
    quote_currency: str,
    intrinsic_currency: str,
) -> Optional[float]:
    """
    (intrinsic - normalized_price) / normalized_price * 100

    No artificial -70% / +200% caps. Extreme values are real signal — either
    the model is wrong, or the stock is genuinely mispriced. Capping hides
    the bug and removes useful information.
    """
    if not all([current_price, intrinsic_value]) or current_price <= 0:
        return None
    try:
        normalized_price = normalize_to_intrinsic_currency(
            current_price, quote_currency, intrinsic_currency
        )
    except ValueError:
        return None
    return round((intrinsic_value - normalized_price) / normalized_price * 100, 2)


# ---------------------------------------------------------------------------
# Fix #3 — Expected ROI: replace hardcoded constants with a real forecast
# ---------------------------------------------------------------------------

@dataclass
class ROIInputs:
    """All per-ticker inputs needed for a differentiated forecast."""
    current_price: float
    analyst_target_price: Optional[float] = None  # EODHD priceTargetConsensus
    beta: float = 1.0
    momentum_score: float = 50.0      # 0–100 percentile (50 = neutral)
    volatility_30d: float = 0.25      # annualized stdev (decimal, not %)
    sector_expected_return_12m: float = 0.08
    risk_free_rate: float = 0.045


def calculate_expected_roi(inputs: ROIInputs) -> tuple[float, float, float]:
    """
    Returns (roi_1m, roi_3m, roi_12m) as decimals.  0.05 = +5%.

    Priority:
      1. If an analyst consensus target exists, use it as the 12M anchor.
      2. Otherwise, fall back to CAPM with a momentum tilt.

    Shorter horizons use sqrt-time scaling, then are dampened by realized
    30-day volatility (high-vol names carry less of the 12M trend forward
    into 1M / 3M).

    The previous output (0.33% / 3% / 8% on every ticker regardless of
    fundamentals) was the single most misleading number in the file.
    """
    p = inputs.current_price

    # --- 12M anchor ---
    if inputs.analyst_target_price and inputs.analyst_target_price > 0:
        roi_12m = (inputs.analyst_target_price / p) - 1.0
    else:
        market_premium = inputs.sector_expected_return_12m - inputs.risk_free_rate
        capm = inputs.risk_free_rate + inputs.beta * market_premium
        momentum_tilt = (inputs.momentum_score - 50.0) / 50.0 * 0.05  # ±5%
        roi_12m = capm + momentum_tilt

    # Sanity bounds, well outside any reasonable forecast
    roi_12m = max(-0.60, min(1.50, roi_12m))

    # --- Shorter horizons ---
    # 0.25 baseline vol; higher vol -> smaller dampener -> less carry
    vol_dampener = max(0.4, min(1.2, 0.25 / max(inputs.volatility_30d, 0.05)))
    roi_3m = roi_12m * (3 / 12) ** 0.5 * vol_dampener
    roi_1m = roi_12m * (1 / 12) ** 0.5 * vol_dampener

    return round(roi_1m, 4), round(roi_3m, 4), round(roi_12m, 4)


def forecast_prices(
    current_price: float, roi_1m: float, roi_3m: float, roi_12m: float
) -> tuple[float, float, float]:
    """Convenience: compound the ROIs into forecast prices."""
    return (
        round(current_price * (1 + roi_1m), 4),
        round(current_price * (1 + roi_3m), 4),
        round(current_price * (1 + roi_12m), 4),
    )


# ---------------------------------------------------------------------------
# Fix #4 — Unrealized P/L %
# ---------------------------------------------------------------------------

def calculate_unrealized_pnl(
    quantity: float, avg_cost: float, current_price: float
) -> tuple[float, float]:
    """
    Returns (absolute_pnl, percentage_pnl).

    Old bug: was multiplying the ratio by 10,000 instead of 100, which gave
    EXE.US a P/L of 4,614,849.94% in the file.
    """
    if not all([quantity, avg_cost, current_price]) or avg_cost <= 0:
        return 0.0, 0.0
    cost_basis = quantity * avg_cost
    market_value = quantity * current_price
    pnl_abs = market_value - cost_basis
    pnl_pct = (pnl_abs / cost_basis) * 100.0
    return round(pnl_abs, 2), round(pnl_pct, 2)


# ---------------------------------------------------------------------------
# Demo — run the fixes on actual values from the May 8 batch
# ---------------------------------------------------------------------------

if __name__ == "__main__":

    bar = "=" * 78

    print(bar)
    print("Fix #1 — 52W Position %  (column was empty for ALL rows)")
    print(bar)
    samples = [
        ("STNE.US",   11.09,    10.36,    16.48),
        ("DDOG",     191.57,    98.01,   201.69),
        ("LLY.US",   953.00,   620.46,  1132.06),
        ("META",     608.38,   520.26,   794.38),
        ("BBRI.JK", 3260.00,  2980.00,  4028.15),
        ("1398.HK",    6.92,     4.96,     7.12),
    ]
    for sym, p, lo, hi in samples:
        print(f"  {sym:10s}  position = {calculate_52w_position(p, lo, hi):6.2f}%")

    print()
    print(bar)
    print("Fix #2 — Upside %  (-70% / +200% caps were hiding a currency bug)")
    print(bar)
    # SBK.JSE in the file: price 30,997 ZAC, intrinsic 9,299, upside -70.00%
    # The price is in cents; R 309.97 vs intrinsic R 9,299 is the real comparison.
    sbk = calculate_upside_pct(30997.0, 9299.10, 'ZAC', 'ZAR')
    print(f"  SBK.JSE   price 30,997 ZAC (= R 309.97)  vs  intrinsic R 9,299")
    print(f"            upside = {sbk:+.2f}%   (was capped at -70.00 in file)")
    # FSR.JSE same family of bug
    fsr = calculate_upside_pct(9193.0, 2757.90, 'ZAC', 'ZAR')
    print(f"  FSR.JSE   price 9,193 ZAC (= R 91.93)   vs  intrinsic R 2,757.90")
    print(f"            upside = {fsr:+.2f}%   (was capped at -70.00 in file)")

    print()
    print(bar)
    print("Fix #3 — Expected ROI  (was 0.33% / 3% / 8% for EVERY ticker)")
    print(bar)
    cases = [
        (
            "DDOG       — high-vol AI/cloud, strong momentum, no analyst target",
            ROIInputs(current_price=191.57, beta=1.30, momentum_score=85,
                      volatility_30d=1.04, sector_expected_return_12m=0.12),
        ),
        (
            "LLY.US     — large-cap pharma, analyst consensus $1,227.90",
            ROIInputs(current_price=953.00, analyst_target_price=1227.90,
                      beta=0.48, momentum_score=60, volatility_30d=0.42),
        ),
        (
            "1398.HK    — low-beta state bank, neutral momentum",
            ROIInputs(current_price=6.92, beta=0.15, momentum_score=55,
                      volatility_30d=0.19, sector_expected_return_12m=0.06),
        ),
        (
            "STNE.US    — small-cap LATAM fintech, oversold",
            ROIInputs(current_price=11.09, beta=1.60, momentum_score=35,
                      volatility_30d=0.63, sector_expected_return_12m=0.10),
        ),
    ]
    for label, ri in cases:
        r1, r3, r12 = calculate_expected_roi(ri)
        p1, p3, p12 = forecast_prices(ri.current_price, r1, r3, r12)
        print(f"  {label}")
        print(f"    1M:  {r1*100:+6.2f}% -> {p1:>10.2f}     "
              f"3M:  {r3*100:+6.2f}% -> {p3:>10.2f}     "
              f"12M: {r12*100:+6.2f}% -> {p12:>10.2f}")

    print()
    print(bar)
    print("Fix #4 — Unrealized P/L %  (was 4,614,849.94% on EXE.US)")
    print(bar)
    abs_pnl, pct_pnl = calculate_unrealized_pnl(
        quantity=500, avg_cost=82.50, current_price=97.25
    )
    print(f"  EXE.US  500 shares @ $82.50  ->  current $97.25")
    print(f"          P/L = ${abs_pnl:,.2f}  ({pct_pnl:+.2f}%)")
    print()
