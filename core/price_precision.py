"""Shared, default-off rounding contract for prices; no provider or Sheet I/O.

WHY v1.0.0 (WP-03, 2026-10-10 export): four-decimal forecasts erase
SHIB-USD at 5.47e-6 and distort twelve other sub-cent rows. Armed prices
below 10 retain at least seven significant digits; all legacy rounding
and score/return precision remain unchanged while the flag is disarmed.
"""
from __future__ import annotations

import math
import os

__version__ = "1.0.0"


def price_precision_enabled() -> bool:
    """Read per call so a deployment mode is shared by every price boundary."""
    return os.getenv("TFB_PRICE_PRECISION", "").strip().lower() in {
        "1", "true", "on", "yes",
    }


def price_round_digits(value: float, nd: int = 4) -> int:
    if (price_precision_enabled() and math.isfinite(value)
            and 0.0 < abs(value) < 10.0):
        return max(nd, 6 - math.floor(math.log10(abs(value))))
    return nd


def round_price(value: float, nd: int = 4) -> float:
    """Off is exactly round(value, nd), including zero and non-finite values."""
    return round(value, price_round_digits(value, nd))


def price_round_half_quantum(value: float, nd: int = 4) -> float:
    """Absolute error bound used only by an armed presentation boundary."""
    return 0.5 * 10.0 ** -price_round_digits(value, nd)
