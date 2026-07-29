import copy
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.investment_policy import (
    build_policy_shadow_report,
    candidate_to_policy_input,
    evaluate_speculation_gate,
    load_policy,
    policy_runtime_enabled,
    validate_price_plan,
    validate_recommendation_card,
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

    def test_nan_market_data_is_fail_closed(self):
        candidate = self.base_candidate()
        candidate["market_cap"] = float("nan")
        result = evaluate_speculation_gate(candidate, self.policy)
        self.assertFalse(result.allowed)
        self.assertIn("UNKNOWN_CRITICAL_MARKET_CAP", result.reasons)

    def test_infinite_spread_is_fail_closed(self):
        candidate = self.base_candidate()
        candidate["spread_pct"] = float("inf")
        result = evaluate_speculation_gate(candidate, self.policy)
        self.assertFalse(result.allowed)
        self.assertIn("UNKNOWN_CRITICAL_SPREAD_PCT", result.reasons)

    def test_unknown_reverse_split_history_is_blocked(self):
        candidate = self.base_candidate()
        candidate["reverse_split_days_ago"] = None
        result = evaluate_speculation_gate(candidate, self.policy)
        self.assertFalse(result.allowed)
        self.assertIn("UNKNOWN_CRITICAL_REVERSE_SPLIT_HISTORY", result.reasons)

    def test_malformed_buy_prices_are_rejected(self):
        plan = {
            "buy_zone_low": "garbage",
            "buy_zone_high": "nonsense",
            "max_acceptable_price": "bad",
            "price_as_of": "2026-07-29T10:00:00Z",
            "price_source": "VERIFIED_EOD",
            "order_type": "LIMIT",
            "order_validity": "DAY",
            "full_buy_cost": 8,
            "entry_invalidation": "Thesis condition",
            "next_review_date": "2026-08-15",
        }
        result = validate_price_plan(plan, side="BUY", policy=self.policy)
        self.assertFalse(result.allowed)
        self.assertIn("INVALID_NUMERIC_BUY_ZONE_LOW", result.reasons)
        self.assertIn("INVALID_NUMERIC_BUY_ZONE_HIGH", result.reasons)
        self.assertIn("INVALID_NUMERIC_MAX_ACCEPTABLE_PRICE", result.reasons)

    def test_empty_recommendation_sections_are_missing(self):
        card = {
            field: {}
            for field in self.policy["recommendation_card"]["required_fields"]
        }
        result = validate_recommendation_card(card, self.policy)
        self.assertFalse(result.allowed)
        self.assertEqual(result.status, "INCOMPLETE_NOT_EXECUTABLE")
        self.assertIn("MISSING_INSTRUMENT_IDENTITY", result.reasons)

    def test_runtime_remains_disabled(self):
        self.assertFalse(policy_runtime_enabled(self.policy))

    def test_shadow_report_has_no_decision_effect_and_does_not_mutate(self):
        candidate = self.base_candidate()
        before = copy.deepcopy(candidate)
        report = build_policy_shadow_report([candidate], self.policy)
        self.assertEqual(candidate, before)
        self.assertFalse(report["enforcement_applied"])
        self.assertEqual(report["decision_effect"], "NONE_SHADOW_ONLY")
        self.assertEqual(report["evaluated"], 1)
        self.assertEqual(report["eligible"], 1)
        self.assertEqual(report["would_block"], 0)

    def test_shadow_report_surfaces_missing_evidence(self):
        report = build_policy_shadow_report(
            [{"symbol": "AAPL.US", "market": "NASDAQ", "current_price": 200}],
            self.policy,
        )
        self.assertEqual(report["evaluated"], 1)
        self.assertEqual(report["would_block"], 1)
        self.assertIn(
            "UNKNOWN_CRITICAL_MEDIAN_DAILY_TRADED_VALUE",
            report["reason_counts"],
        )
        self.assertIn(
            "INSTRUMENT_IDENTITY_UNVERIFIED",
            report["unknown_or_unverified_counts"],
        )

    def test_adapter_does_not_replace_median_with_average(self):
        mapped = candidate_to_policy_input({
            "symbol": "AAPL.US",
            "market": "NASDAQ",
            "avg_daily_traded_value": 99_000_000,
        })
        self.assertIsNone(mapped["median_daily_traded_value"])

    def test_adapter_recognizes_saudi_market_without_inventing_identity(self):
        mapped = candidate_to_policy_input({
            "symbol": "1120.SR",
            "market": "Tadawul",
        })
        self.assertEqual(mapped["market"], "SAUDI")
        self.assertFalse(mapped["instrument_identity_verified"])


if __name__ == "__main__":
    unittest.main()
