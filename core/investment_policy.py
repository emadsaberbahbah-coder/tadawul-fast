#!/usr/bin/env python3
"""Central loader, fail-closed validators, and shadow reporting for investment policy.

This module performs no network I/O and does not produce BUY/SELL recommendations.
It provides a single policy surface that every project component can import.

Shadow reporting is deliberately non-enforcing: it measures what the policy would
block, records missing evidence, and never changes a live verdict or action.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
import json
import math
import os
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence


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
    """Return a finite float; non-numeric, NaN, and infinities are unknown."""
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _is_missing(value: Any) -> bool:
    """Treat empty/content-free required values as missing without rejecting 0/False."""
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, Mapping):
        return not value or all(_is_missing(item) for item in value.values())
    if isinstance(value, (list, tuple, set, frozenset)):
        return not value or all(_is_missing(item) for item in value)
    if isinstance(value, float) and not math.isfinite(value):
        return True
    return False


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


def policy_runtime_enabled(
    policy: Optional[Mapping[str, Any]] = None,
) -> bool:
    """Return the policy runtime flag without enabling or changing it."""
    cfg = dict(policy or load_policy())
    return bool(cfg.get("policy", {}).get("runtime_enabled", False))


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
    if split_days is None:
        reasons.append("UNKNOWN_CRITICAL_REVERSE_SPLIT_HISTORY")
    elif split_days <= float(gate["recent_reverse_split_days"]):
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


def _first_present(candidate: Mapping[str, Any], names: Sequence[str]) -> Any:
    """Return the first present, non-empty alias while preserving 0 and False."""
    for name in names:
        if name not in candidate:
            continue
        value = candidate.get(name)
        if not _is_missing(value):
            return value
    return None


def _canonical_market(candidate: Mapping[str, Any]) -> str:
    raw = str(
        _first_present(candidate, ("market_profile", "market", "country", "region"))
        or ""
    ).strip().upper()
    symbol = str(
        _first_present(candidate, ("symbol", "ticker", "Symbol", "Ticker"))
        or ""
    ).strip().upper()

    if symbol.endswith(".SR") or any(token in raw for token in ("TADAWUL", "SAUDI", "KSA")):
        return "SAUDI"
    if symbol.endswith(".US") or raw in {
        "US", "USA", "UNITED STATES", "NYSE", "NASDAQ", "AMEX",
        "NYSE/NASDAQ", "NASDAQ/NYSE",
    }:
        return "US"
    return raw


def candidate_to_policy_input(candidate: Mapping[str, Any]) -> dict[str, Any]:
    """Map only semantically equivalent project fields into the policy gate.

    Important: averages are not substituted for medians, 30-day volatility is
    not assumed annualized, and identity is never inferred. Missing evidence
    remains missing so the shadow report exposes the actual integration gaps.
    """
    return {
        "market": _canonical_market(candidate),
        "exchange": _first_present(candidate, ("exchange", "venue", "listing_venue")),
        "market_cap": _first_present(
            candidate,
            ("market_cap", "market_capitalization", "market_cap_value"),
        ),
        "median_daily_traded_value": _first_present(
            candidate,
            (
                "median_daily_traded_value",
                "median_traded_value_30d",
                "median_daily_value",
            ),
        ),
        "price": _first_present(candidate, ("price", "current_price", "last_price")),
        "spread_pct": _first_present(
            candidate,
            ("spread_pct", "bid_ask_spread_pct", "quoted_spread_pct"),
        ),
        "annualized_volatility_pct": _first_present(
            candidate,
            ("annualized_volatility_pct", "volatility_annualized_pct"),
        ),
        "one_day_return_pct": _first_present(
            candidate,
            ("one_day_return_pct", "return_1d_pct", "change_1d_pct"),
        ),
        "five_day_return_pct": _first_present(
            candidate,
            ("five_day_return_pct", "return_5d_pct", "change_5d_pct"),
        ),
        "reverse_split_days_ago": _first_present(
            candidate,
            ("reverse_split_days_ago", "days_since_reverse_split"),
        ),
        "actual_trading_days": _first_present(
            candidate,
            ("actual_trading_days", "trading_history_days"),
        ),
        "instrument_identity_verified": (
            candidate.get("instrument_identity_verified") is True
        ),
        "extreme_move_fundamental_event_verified": (
            candidate.get("extreme_move_fundamental_event_verified") is True
        ),
    }


def build_policy_shadow_report(
    candidates: Iterable[Mapping[str, Any]] | None,
    policy: Optional[Mapping[str, Any]] = None,
    *,
    sample_limit: int = 25,
) -> dict[str, Any]:
    """Measure policy outcomes without changing any recommendation or action."""
    cfg = dict(policy or load_policy())
    policy_meta = dict(cfg.get("policy", {}) or {})
    reason_counts: Counter[str] = Counter()
    warning_counts: Counter[str] = Counter()
    samples: list[dict[str, Any]] = []
    evaluated = 0
    eligible = 0
    would_block = 0
    invalid_rows = 0

    for raw in candidates or ():
        if not isinstance(raw, Mapping):
            invalid_rows += 1
            reason_counts["INVALID_CANDIDATE_ROW"] += 1
            continue

        mapped = candidate_to_policy_input(raw)
        result = evaluate_speculation_gate(mapped, cfg)
        evaluated += 1
        if result.allowed:
            eligible += 1
        else:
            would_block += 1
        reason_counts.update(result.reasons)
        warning_counts.update(result.warnings)

        if len(samples) < max(0, int(sample_limit)):
            samples.append({
                "symbol": str(
                    _first_present(raw, ("symbol", "ticker", "Symbol", "Ticker"))
                    or ""
                ),
                "market": mapped.get("market") or "",
                "status": result.status,
                "would_block": not result.allowed,
                "reasons": list(result.reasons),
                "warnings": list(result.warnings),
            })

    unknown_counts = {
        reason: count
        for reason, count in sorted(reason_counts.items())
        if reason.startswith("UNKNOWN_")
        or reason in {"INSTRUMENT_IDENTITY_UNVERIFIED", "INVALID_CANDIDATE_ROW"}
    }

    return {
        "report_type": "INVESTMENT_POLICY_SHADOW",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "policy_name": policy_meta.get("name"),
        "policy_version": policy_meta.get("version"),
        "policy_scope": policy_meta.get("scope"),
        "runtime_enabled": bool(policy_meta.get("runtime_enabled", False)),
        "enforcement_applied": False,
        "decision_effect": "NONE_SHADOW_ONLY",
        "status": "ok" if evaluated or invalid_rows else "no_candidates",
        "evaluated": evaluated,
        "eligible": eligible,
        "would_block": would_block,
        "invalid_rows": invalid_rows,
        "reason_counts": dict(sorted(reason_counts.items())),
        "warning_counts": dict(sorted(warning_counts.items())),
        "unknown_or_unverified_counts": unknown_counts,
        "samples": samples,
    }


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
        if _is_missing(plan.get(field))
    ]
    reasons = [f"MISSING_{field.upper()}" for field in missing]

    if side_upper == "BUY":
        numeric_fields = {
            "buy_zone_low": _number(plan.get("buy_zone_low")),
            "buy_zone_high": _number(plan.get("buy_zone_high")),
            "max_acceptable_price": _number(plan.get("max_acceptable_price")),
        }
        for field, number in numeric_fields.items():
            if not _is_missing(plan.get(field)) and number is None:
                reasons.append(f"INVALID_NUMERIC_{field.upper()}")

        low = numeric_fields["buy_zone_low"]
        high = numeric_fields["buy_zone_high"]
        max_price = numeric_fields["max_acceptable_price"]
        if None not in (low, high) and low > high:
            reasons.append("INVALID_BUY_ZONE")
        if None not in (high, max_price) and high > max_price:
            reasons.append("BUY_ZONE_EXCEEDS_MAX_ACCEPTABLE_PRICE")
    else:
        numeric_fields = {
            "sell_zone_low": _number(plan.get("sell_zone_low")),
            "sell_zone_high": _number(plan.get("sell_zone_high")),
            "min_acceptable_exit_price": _number(plan.get("min_acceptable_exit_price")),
        }
        for field, number in numeric_fields.items():
            if not _is_missing(plan.get(field)) and number is None:
                reasons.append(f"INVALID_NUMERIC_{field.upper()}")

        low = numeric_fields["sell_zone_low"]
        high = numeric_fields["sell_zone_high"]
        min_exit = numeric_fields["min_acceptable_exit_price"]
        if None not in (low, high) and low > high:
            reasons.append("INVALID_SELL_ZONE")
        if None not in (min_exit, low) and min_exit > low:
            reasons.append("MIN_EXIT_EXCEEDS_SELL_ZONE_LOW")

    if reasons:
        return GateResult(False, "INCOMPLETE_NOT_EXECUTABLE", tuple(sorted(set(reasons))))
    return GateResult(True, "PRICE_PLAN_VALID", ())


def validate_recommendation_card(
    card: Mapping[str, Any],
    policy: Optional[Mapping[str, Any]] = None,
) -> GateResult:
    cfg = dict(policy or load_policy())
    required: Sequence[str] = cfg["recommendation_card"]["required_fields"]
    missing = [field for field in required if _is_missing(card.get(field))]
    if missing:
        return GateResult(
            False,
            cfg["recommendation_card"]["missing_critical_field_status"],
            tuple(f"MISSING_{field.upper()}" for field in missing),
        )
    return GateResult(True, "RECOMMENDATION_CARD_COMPLETE", ())
