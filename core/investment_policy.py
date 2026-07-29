#!/usr/bin/env python3
"""Central loader and fail-closed validators for the project-wide investment policy.

This module performs no network I/O and does not produce BUY/SELL recommendations.
It provides a single policy surface that every project component can import.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
import os
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


DEFAULT_CONFIG_PATH = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "project_wide_execution_learning_policy_v1.json"
)


@dataclass(frozen=True)
class GateResult:
    allowed: bool
    status: str
    reasons: tuple[str, ...]
    warnings: tuple[str, ...] = ()


def _number(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@lru_cache(maxsize=4)
def load_policy(path: Optional[str] = None) -> dict[str, Any]:
    config_path = Path(
        path
        or os.getenv("TFB_INVESTMENT_POLICY_PATH", "")
        or DEFAULT_CONFIG_PATH
    ).expanduser().resolve()
    if not config_path.is_file():
        raise FileNotFoundError(f"Investment policy not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as handle:
        policy = json.load(handle)
    if policy.get("policy", {}).get("scope") != "PROJECT_WIDE":
        raise ValueError("Investment policy must declare PROJECT_WIDE scope")
    return policy


def evaluate_speculation_gate(
    candidate: Mapping[str, Any],
    policy: Optional[Mapping[str, Any]] = None,
) -> GateResult:
    """Apply the anti-speculation gate.

    Expected candidate keys:
      market, exchange, market_cap, median_daily_traded_value, price,
      spread_pct, annualized_volatility_pct, one_day_return_pct,
      five_day_return_pct, reverse_split_days_ago, actual_trading_days,
      instrument_identity_verified, extreme_move_fundamental_event_verified.
    """
    cfg = dict(policy or load_policy())
    gate = cfg["speculation_gate"]
    market = str(candidate.get("market") or "").upper()
    profile = gate.get("market_profiles", {}).get(market)
    reasons: list[str] = []
    warnings: list[str] = []

    if not profile:
        reasons.append("UNKNOWN_MARKET_PROFILE")

    if candidate.get("instrument_identity_verified") is not True:
        reasons.append("INSTRUMENT_IDENTITY_UNVERIFIED")

    exchange = str(candidate.get("exchange") or "").upper()
    if exchange in {str(v).upper() for v in gate.get("blocked_venues", [])}:
        reasons.append("BLOCKED_VENUE")

    critical = {
        "market_cap": _number(candidate.get("market_cap")),
        "median_daily_traded_value": _number(candidate.get("median_daily_traded_value")),
        "spread_pct": _number(candidate.get("spread_pct")),
        "annualized_volatility_pct": _number(candidate.get("annualized_volatility_pct")),
        "actual_trading_days": _number(candidate.get("actual_trading_days")),
    }
    for field, value in critical.items():
        if value is None:
            reasons.append(f"UNKNOWN_CRITICAL_{field.upper()}")

    if profile:
        cap = critical["market_cap"]
        traded = critical["median_daily_traded_value"]
        if cap is not None and cap < float(profile["min_market_cap"]):
            reasons.append("MARKET_CAP_BELOW_FLOOR")
        if traded is not None and traded < float(profile["min_median_daily_traded_value"]):
            reasons.append("TRADED_VALUE_BELOW_FLOOR")
        price = _number(candidate.get("price"))
        if price is not None and price < float(profile.get("price_floor_warning", 0)):
            warnings.append("LOW_PRICE_WARNING_NOT_STANDALONE_BLOCK")

    spread = critical["spread_pct"]
    if spread is not None and spread > float(gate["max_spread_pct"]):
        reasons.append("SPREAD_TOO_WIDE")

    vol = critical["annualized_volatility_pct"]
    if vol is not None and vol > float(gate["max_annualized_volatility_pct"]):
        reasons.append("VOLATILITY_TOO_HIGH")

    days = critical["actual_trading_days"]
    if days is not None and days < float(gate["min_actual_trading_days"]):
        reasons.append("INSUFFICIENT_TRADING_HISTORY")

    split_days = _number(candidate.get("reverse_split_days_ago"))
    if split_days is not None and split_days <= float(gate["recent_reverse_split_days"]):
        reasons.append("RECENT_REVERSE_SPLIT")

    one_day = abs(_number(candidate.get("one_day_return_pct")) or 0.0)
    five_day = abs(_number(candidate.get("five_day_return_pct")) or 0.0)
    extreme = (
        one_day >= float(gate["extreme_one_day_move_pct"])
        or five_day >= float(gate["extreme_five_day_move_pct"])
    )
    if extreme and candidate.get("extreme_move_fundamental_event_verified") is not True:
        reasons.append("UNVERIFIED_EXTREME_MOVE")

    if reasons:
        return GateResult(False, "SPECULATION_BLOCK", tuple(sorted(set(reasons))), tuple(warnings))
    return GateResult(True, "INVESTMENT_UNIVERSE_ELIGIBLE", (), tuple(warnings))


def validate_price_plan(
    plan: Mapping[str, Any],
    *,
    side: str,
    policy: Optional[Mapping[str, Any]] = None,
) -> GateResult:
    cfg = dict(policy or load_policy())
    side_upper = side.upper()
    if side_upper not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")

    section = cfg["entry_price_policy"] if side_upper == "BUY" else cfg["exit_price_policy"]
    missing = [
        field for field in section["required_fields"]
        if plan.get(field) in (None, "", [])
    ]
    reasons = [f"MISSING_{field.upper()}" for field in missing]

    if side_upper == "BUY":
        low = _number(plan.get("buy_zone_low"))
        high = _number(plan.get("buy_zone_high"))
        max_price = _number(plan.get("max_acceptable_price"))
        if None not in (low, high) and low > high:
            reasons.append("INVALID_BUY_ZONE")
        if None not in (high, max_price) and high > max_price:
            reasons.append("BUY_ZONE_EXCEEDS_MAX_ACCEPTABLE_PRICE")
    else:
        low = _number(plan.get("sell_zone_low"))
        high = _number(plan.get("sell_zone_high"))
        if None not in (low, high) and low > high:
            reasons.append("INVALID_SELL_ZONE")

    if reasons:
        return GateResult(False, "INCOMPLETE_NOT_EXECUTABLE", tuple(sorted(set(reasons))))
    return GateResult(True, "PRICE_PLAN_VALID", ())


def validate_recommendation_card(
    card: Mapping[str, Any],
    policy: Optional[Mapping[str, Any]] = None,
) -> GateResult:
    cfg = dict(policy or load_policy())
    required: Sequence[str] = cfg["recommendation_card"]["required_fields"]
    missing = [field for field in required if card.get(field) in (None, "", [])]
    if missing:
        return GateResult(
            False,
            cfg["recommendation_card"]["missing_critical_field_status"],
            tuple(f"MISSING_{field.upper()}" for field in missing),
        )
    return GateResult(True, "RECOMMENDATION_CARD_COMPLETE", ())
