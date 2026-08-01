from __future__ import annotations

import unittest

from scripts.verify_backend_symbol_capabilities import RULES, evaluate_table


class BackendSymbolCapabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.headers = ["Symbol", "Name", "Current Price", "Data Provider"]
        self.by_capability = {rule.capability: rule for rule in RULES}

    def test_accepts_eodhd_adx_alias_for_yahoo_ad_request(self):
        rule = self.by_capability["yahoo_ad_to_eodhd_adx"]
        result = evaluate_table(
            rule,
            self.headers,
            [["ADNOCDIST.ADX", "ADNOC Distribution PJSC", 3.71, "eodhd"]],
        )
        self.assertTrue(result["passed"])
        self.assertEqual(result["pass_mode"], "live_identity")
        self.assertTrue(result["data_available"])
        self.assertEqual(result["seen_symbol"], "ADNOCDIST.ADX")

    def test_accepts_eodhd_pse_alias_for_yahoo_ps_request(self):
        rule = self.by_capability["yahoo_ps_to_eodhd_pse"]
        result = evaluate_table(
            rule,
            self.headers,
            [["BPI.PSE", "Bank of the Philippine Islands", 128.4, "eodhd"]],
        )
        self.assertTrue(result["passed"])
        self.assertEqual(result["pass_mode"], "live_identity")

    def test_noncritical_mapping_can_pass_as_truthfully_unavailable(self):
        rule = self.by_capability["yahoo_ps_to_eodhd_pse"]
        result = evaluate_table(
            rule,
            self.headers,
            [["BPI.PS", "", None, "eodhd"]],
        )
        self.assertTrue(result["passed"])
        self.assertEqual(result["pass_mode"], "truthful_unavailable")
        self.assertFalse(result["data_available"])
        self.assertIn("without fabrication", result["reason"])

    def test_bny_requires_exact_live_issuer_identity(self):
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
        unavailable = evaluate_table(
            rule,
            self.headers,
            [["BNY.US", "", None, "eodhd"]],
        )
        self.assertTrue(good["passed"])
        self.assertEqual(good["pass_mode"], "live_identity")
        self.assertFalse(wrong["passed"])
        self.assertEqual(wrong["reason"], "issuer name mismatch")
        self.assertFalse(unavailable["passed"])
        self.assertEqual(unavailable["reason"], "blank instrument name")

    def test_partial_mixed_fact_row_does_not_count_as_truthful_unavailable(self):
        rule = self.by_capability["yahoo_ad_to_eodhd_adx"]
        result = evaluate_table(
            rule,
            self.headers,
            [["ADNOCDIST.AD", "", 3.71, "eodhd"]],
        )
        self.assertFalse(result["passed"])
        self.assertEqual(result["reason"], "blank instrument name")

    def test_error_provider_marker_fails_closed(self):
        rule = self.by_capability["yahoo_ps_to_eodhd_pse"]
        result = evaluate_table(
            rule,
            self.headers,
            [["BPI.PS", "", None, "fallback_error"]],
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
