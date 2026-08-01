from __future__ import annotations

import unittest

import core.symbols  # installs the runtime patch
from core.analysis import identity_guard
from core.symbols import normalize


class RuntimeTruthPatchTests(unittest.TestCase):
    def test_legacy_ab_uses_yahoo_ad_without_changing_canonical_symbol(self):
        self.assertEqual(normalize.normalize_symbol("ADNOCDIST.AB"), "ADNOCDIST.AB")
        self.assertEqual(normalize.to_yahoo_symbol("ADNOCDIST.AB"), "ADNOCDIST.AD")
        self.assertEqual(normalize.to_eodhd_symbol("ADNOCDIST.AB"), "ADNOCDIST.ADX")

    def test_oman_metadata_is_deterministic(self):
        metadata = normalize.infer_symbol_metadata("OQBI.OM")
        self.assertEqual(metadata["exchange"], "MSX")
        self.assertEqual(metadata["currency"], "OMR")
        self.assertEqual(metadata["country"], "Oman")
        self.assertEqual(metadata["exchange_code"], "OM")

    def test_non_numeric_sr_is_not_classified_as_tadawul(self):
        metadata = normalize.infer_symbol_metadata("ELET3.SR")
        self.assertIsNone(metadata["exchange"])
        self.assertIsNone(metadata["currency"])
        self.assertIsNone(metadata["country"])
        self.assertEqual(
            metadata["inferred_from"],
            "runtime_truth_patch:invalid_sr_shape",
        )

    def test_identity_guard_corrects_philippine_metadata_and_blocks_conflict(self):
        rows = [
            {
                "symbol": "BPI.PS",
                "name": "Bank of the Philippine Islands",
                "current_price": 120.0,
                "exchange": "NASDAQ/NYSE",
                "currency": "USD",
                "country": "USA",
                "asset_class": "Equity",
                "warnings": "",
                "block_reason": "",
            }
        ]
        plan = identity_guard.guard_sheet_rows(
            rows,
            sheet="Market_Leaders",
            run_dedup=False,
        )
        result = plan.apply()[0]
        self.assertEqual(result["exchange"], "PSE")
        self.assertEqual(result["currency"], "PHP")
        self.assertEqual(result["country"], "Philippines")
        self.assertEqual(result["investability_status"], "BLOCKED")
        self.assertEqual(result["final_action"], "DO_NOT_INVEST")
        self.assertIn("market_metadata_conflict_corrected", result["warnings"])

    def test_identity_guard_fills_abu_dhabi_metadata_without_fabricating_price(self):
        rows = [
            {
                "symbol": "ADNOCDIST.AB",
                "name": "ADNOC Distribution",
                "current_price": None,
                "exchange": "",
                "currency": "",
                "country": "",
                "asset_class": "Equity",
                "warnings": "fetch_failed:HTTP 402; provider_unhealthy:eodhd",
                "block_reason": "Missing current price",
            }
        ]
        guarded = identity_guard.guard_sheet_rows(
            rows,
            sheet="Market_Leaders",
            run_dedup=False,
        ).apply()
        self.assertEqual(len(guarded), 1)
        result = guarded[0]
        self.assertEqual(result["exchange"], "ADX")
        self.assertEqual(result["currency"], "AED")
        self.assertEqual(result["country"], "United Arab Emirates")
        self.assertIsNone(result["current_price"])
        self.assertIn("legacy_symbol_alias", result["warnings"])

    def test_invalid_sr_is_explicitly_blocked_and_false_metadata_cleared(self):
        rows = [
            {
                "symbol": "ELET3.SR",
                "name": "Eletrobras",
                "current_price": None,
                "exchange": "Tadawul",
                "currency": "SAR",
                "country": "Saudi Arabia",
                "asset_class": "Equity",
                "warnings": "",
                "block_reason": "",
            }
        ]
        result = identity_guard.guard_sheet_rows(
            rows,
            sheet="Market_Leaders",
            run_dedup=False,
        ).apply()[0]
        self.assertEqual(result["exchange"], "")
        self.assertEqual(result["currency"], "")
        self.assertEqual(result["country"], "")
        self.assertEqual(result["investability_status"], "BLOCKED")
        self.assertEqual(result["final_action"], "DO_NOT_INVEST")
        self.assertIn("invalid_symbol_shape:non_numeric_sr", result["warnings"])
        self.assertIn("must be numeric", result["block_reason"])

    def test_installation_is_idempotent(self):
        before = identity_guard.guard_sheet_rows
        from core.symbols.runtime_truth_patch import install_runtime_truth_patch

        install_runtime_truth_patch()
        self.assertIs(identity_guard.guard_sheet_rows, before)


if __name__ == "__main__":
    unittest.main()
