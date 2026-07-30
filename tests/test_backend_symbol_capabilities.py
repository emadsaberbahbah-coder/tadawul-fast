from __future__ import annotations

import unittest

from scripts.verify_backend_symbol_capabilities import RULES, evaluate_table


class BackendSymbolCapabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.headers = ["Symbol", "Name", "Current Price", "Data Provider"]
        self.by_capability = {rule.capability: rule for rule in RULES}

    def test_accepts_eodhd_adx_alias_for_yahoo_ab_request(self):
        rule = self.by_capability["yahoo_ad_to_eodhd_adx"]
        result = evaluate_table(
            rule,
            self.headers,
            [["ADNOCDIST.ADX", "ADNOC Distribution PJSC", 3.71, "eodhd"]],
        )
        self.assertTrue(result["passed"])
        self.assertEqual(result["seen_symbol"], "ADNOCDIST.ADX")

    def test_accepts_eodhd_pse_alias_for_yahoo_ps_request(self):
        rule = self.by_capability["yahoo_ps_to_eodhd_pse"]
        result = evaluate_table(
            rule,
            self.headers,
            [["BPI.PSE", "Bank of the Philippine Islands", 128.4, "eodhd"]],
        )
        self.assertTrue(result["passed"])

    def test_bk_requires_exact_issuer_identity(self):
        rule = self.by_capability["bny_exact_identity"]
        good = evaluate_table(
            rule,
            self.headers,
            [["BNY.US", "The Bank of New York Mellon Corporation", 71.2, "eodhd"]],
        )
        wrong = evaluate_table(
            rule,
            self.headers,
            [["BNY.US", "Booking Holdings Inc.", 3900.0, "eodhd"]],
        )
        self.assertTrue(good["passed"])
        self.assertFalse(wrong["passed"])
        self.assertEqual(wrong["reason"], "issuer name mismatch")

    def test_blank_name_fails_closed(self):
        rule = self.by_capability["bny_exact_identity"]
        result = evaluate_table(
            rule,
            self.headers,
            [["BNY.US", "", 71.2, "eodhd"]],
        )
        self.assertFalse(result["passed"])
        self.assertEqual(result["reason"], "blank instrument name")

    def test_error_provider_marker_fails_closed(self):
        rule = self.by_capability["yahoo_ps_to_eodhd_pse"]
        result = evaluate_table(
            rule,
            self.headers,
            [["BPI.PS", "Bank of the Philippine Islands", 128.4, "fallback_error"]],
        )
        self.assertFalse(result["passed"])
        self.assertEqual(result["reason"], "provider returned an error/stub marker")

    def test_placeholder_provider_is_not_capability_proof(self):
        rule = self.by_capability["yahoo_ps_to_eodhd_pse"]
        result = evaluate_table(
            rule,
            self.headers,
            [[
                "BPI.PS",
                "Market_Leaders BPI.PS",
                101.0,
                "advanced_analysis.placeholder_fallback",
            ]],
        )
        self.assertFalse(result["passed"])
        self.assertEqual(result["reason"], "provider returned an error/stub marker")

    def test_missing_required_column_is_not_treated_as_blank_or_zero(self):
        rule = self.by_capability["yahoo_ad_to_eodhd_adx"]
        result = evaluate_table(
            rule,
            ["Symbol", "Name", "Data Provider"],
            [["ADNOCDIST.AD", "ADNOC Distribution PJSC", "eodhd"]],
        )
        self.assertFalse(result["passed"])
        self.assertIn("current_price", result["missing_columns"])
        self.assertEqual(result["reason"], "required response columns missing")


if __name__ == "__main__":
    unittest.main()
