"""Exact coverage and timestamp facts shared by refresh producers and audits."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import math
from typing import Iterable

# Permit small clock differences, never arbitrary future-dated evidence.
MAX_CLOCK_SKEW_SECONDS = 300


@dataclass(frozen=True)
class CoverageValidity:
    requested: int
    fresh: int | None
    valid: bool
    reasons: tuple[str, ...]

    @property
    def percent(self) -> float | None:
        if (type(self.requested) is not int or self.requested <= 0
                or type(self.fresh) is not int
                or not 0 <= self.fresh <= self.requested):
            return None
        return float(Decimal(self.fresh) * 100 / Decimal(self.requested))


def coverage_validity(
    requested: int, fresh: int | None, minimum_percent: object = 95,
) -> CoverageValidity:
    """Compare integers with an exact decimal threshold; never display rounding."""
    reasons: list[str] = []
    try:
        minimum = Decimal(str(minimum_percent))
        if not minimum.is_finite() or not 0 <= minimum <= 100:
            raise ValueError("invalid coverage threshold")
    except (ValueError, InvalidOperation):
        minimum = Decimal(95)
        reasons.append("invalid_threshold")
    if type(requested) is not int:
        return CoverageValidity(0, None, False, tuple(reasons + ["requested_unknown"]))
    if requested <= 0:
        reasons.append("requested_unknown")
    if fresh is None:
        reasons.append("fresh_unknown")
    elif isinstance(fresh, bool) or not isinstance(fresh, int) or fresh < 0 or fresh > requested:
        reasons.append("invalid_fresh_count")
    elif requested > 0 and Decimal(fresh) * 100 < minimum * requested:
        reasons.append("coverage_below_minimum")
    return CoverageValidity(requested, fresh, not reasons, tuple(reasons))


def symbol_validity(
    requested: Iterable[str], fetched: Iterable[str], *,
    preserved: Iterable[str] = (), failed: Iterable[str] = (),
    quarantined: Iterable[str] = (), minimum_percent: object = 95,
) -> CoverageValidity:
    """Intersect actual origins with requests, excluding overlapping failures once."""
    req = set(requested)
    excluded = set(preserved) | set(failed) | set(quarantined)
    fresh = (set(fetched) & req) - excluded
    return coverage_validity(len(req), len(fresh), minimum_percent)


def timestamp_freshness(
    timestamp: datetime | None, now: datetime, *, precision: str = "datetime",
    max_age_seconds: float, clock_skew_seconds: float = MAX_CLOCK_SKEW_SECONDS,
) -> tuple[bool, float | None, str]:
    """Signed age; date-only and excessively future evidence cannot be fresh."""
    if timestamp is None or precision != "datetime":
        return False, None, "timestamp_precision_unknown"
    try:
        if (not math.isfinite(max_age_seconds) or max_age_seconds < 0
                or not math.isfinite(clock_skew_seconds) or clock_skew_seconds < 0):
            return False, None, "timestamp_policy_invalid"
        age = (now - timestamp).total_seconds()
    except (TypeError, AttributeError, ValueError):
        return False, None, "timestamp_basis_invalid"
    if age < -clock_skew_seconds:
        return False, age, "timestamp_future"
    if age > max_age_seconds:
        return False, age, "timestamp_stale"
    return True, age, ""
