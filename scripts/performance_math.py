"""Pure calibration math extracted from the performance tracker.

WP-12 (2026-10-11): move the existing Wilson interval and shrinkage leaves
without changing rounding, boundary handling or public names. The tracker
re-exports these functions for existing importers. No I/O or environment reads.
"""
from __future__ import annotations

import math
from typing import Tuple

__version__ = "1.0.0"

_Z95 = 1.959963984540054  # 95% two-sided normal quantile


def wilson_interval(wins: int, decided: int, z: float = _Z95) -> Tuple[float, float]:
    """Wilson score interval for a binomial proportion, returned as PERCENT.
    Stays inside [0,100] and is honest at small n. (0,0) when decided==0."""
    n = int(decided)
    if n <= 0:
        return 0.0, 0.0
    w = max(0, min(int(wins), n))
    p = w / n
    z2 = z * z
    denom = 1.0 + z2 / n
    center = (p + z2 / (2.0 * n)) / denom
    half = (z * math.sqrt((p * (1.0 - p) / n) + (z2 / (4.0 * n * n)))) / denom
    lo = max(0.0, min(100.0, (center - half) * 100.0))
    hi = max(0.0, min(100.0, (center + half) * 100.0))
    return round(lo, 1), round(hi, 1)


def shrink_factor(raw: float, decided: int, k: int) -> float:
    """Shrink a raw calibration factor toward 1.0 by sample size:
    1 + (raw-1) * n/(n+k). Small n -> ~1.0 (no correction asserted)."""
    n = max(0, int(decided))
    if n <= 0:
        return 1.0
    return 1.0 + (raw - 1.0) * (n / (n + max(1, int(k))))
