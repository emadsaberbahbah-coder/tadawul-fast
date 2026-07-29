import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.investment_policy import (
    evaluate_speculation_gate,
    load_policy,
    validate_price_plan,
)


class InvestmentPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.policy = load_policy(
            str(ROOT / "config" / "project_wide_execution_learning_policy_v1.json")
        )

    def base_candidate(self):
        return {
            "market": "US",
            "exchange": "NASDAQ",
            "market_cap": 5_000_000_000,
            "median_daily_traded_value": 25_000_000,
            "price": 25,
            "spread_pct": 0.2,
            "annualized_volatility_pct": 35,
            "one_day_return_pct": 2,
            "five_day_return_pct": 4,
            "reverse_split_days_ago": 999,
            "actual_trading_days": 1000,
            "instrument_identity_verified": True,
            "extreme_move_fundamental_event_verified": False,
        }

    def test_extreme_move_without_verified_event_is_blocked(self):
        candidate = self.base_candidate()
        candidate["one_day_return_pct"] = 45
        result = evaluate_speculation_gate(candidate, self.policy)
        self.assertFalse(result.allowed)
        self.assertIn("UNVERIFIED_EXTREME_MOVE", result.reasons)

    def test_missing_liquidity_is_fail_closed(self):
        candidate = self.base_candidate()
        candidate["median_daily_traded_value"] = None
        result = evaluate_speculation_gate(candidate, self.policy)
        self.assertFalse(result.allowed)
        self.assertIn("UNKNOWN_CRITICAL_MEDIAN_DAILY_TRADED_VALUE", result.reasons)

    def test_low_price_alone_is_only_warning(self):
        candidate = self.base_candidate()
        candidate["price"] = 3
        result = evaluate_speculation_gate(candidate, self.policy)
        self.assertTrue(result.allowed)
        self.assertIn("LOW_PRICE_WARNING_NOT_STANDALONE_BLOCK", result.warnings)

    def test_buy_plan_requires_timestamp_and_validity(self):
        plan = {
            "buy_zone_low": 95,
            "buy_zone_high": 100,
            "max_acceptable_price": 102,
            "price_source": "VERIFIED_EOD",
            "order_type": "LIMIT",
            "full_buy_cost": 8,
            "entry_invalidation": "Thesis condition",
            "next_review_date": "2026-08-15",
        }
        result = validate_price_plan(plan, side="BUY", policy=self.policy)
        self.assertFalse(result.allowed)
        self.assertIn("MISSING_PRICE_AS_OF", result.reasons)
        self.assertIn("MISSING_ORDER_VALIDITY", result.reasons)

    def test_valid_buy_zone(self):
        plan = {
            "buy_zone_low": 95,
            "buy_zone_high": 100,
            "max_acceptable_price": 102,
            "price_as_of": "2026-07-29T10:00:00Z",
            "price_source": "VERIFIED_EOD",
            "order_type": "LIMIT",
            "order_validity": "DAY",
            "full_buy_cost": 8,
            "entry_invalidation": "Thesis condition",
            "next_review_date": "2026-08-15",
        }
        result = validate_price_plan(plan, side="BUY", policy=self.policy)
        self.assertTrue(result.allowed)


if __name__ == "__main__":
    unittest.main()
