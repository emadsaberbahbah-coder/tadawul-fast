from __future__ import annotations

import subprocess
import sys
import textwrap
import unittest

import core.symbols  # installs the runtime patch
from core.analysis import identity_guard
from core.providers.market_truth_activation import _run_bounded
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

    def test_bounded_activation_retries_until_guard_is_ready(self):
        calls = []

        def ensure() -> bool:
            calls.append(len(calls) + 1)
            return len(calls) == 3

        armed, attempts, error = _run_bounded(
            ensure,
            attempts=5,
            delay_sec=0,
            sleeper=lambda _delay: None,
        )
        self.assertTrue(armed)
        self.assertEqual(attempts, 3)
        self.assertEqual(error, "")
        self.assertEqual(calls, [1, 2, 3])

    def test_bounded_activation_fails_closed_after_limit(self):
        armed, attempts, error = _run_bounded(
            lambda: False,
            attempts=4,
            delay_sec=0,
            sleeper=lambda _delay: None,
        )
        self.assertFalse(armed)
        self.assertEqual(attempts, 4)
        self.assertEqual(error, "")

    def test_provider_init_repairs_identity_guard_import_order(self):
        code = textwrap.dedent(
            """
            from core.analysis import identity_guard
            import core.providers

            assert getattr(
                identity_guard,
                "_TFB_MARKET_METADATA_TRUTH_PATCHED",
                False,
            ) is True

            result = identity_guard.guard_sheet_rows(
                [
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
                ],
                sheet="Market_Leaders",
                run_dedup=False,
            ).apply()[0]

            assert result["exchange"] == "PSE"
            assert result["currency"] == "PHP"
            assert result["country"] == "Philippines"
            assert result["investability_status"] == "BLOCKED"
            assert result["final_action"] == "DO_NOT_INVEST"
            """
        )
        completed = subprocess.run(
            [sys.executable, "-c", code],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            completed.returncode,
            0,
            completed.stdout + completed.stderr,
        )

    def test_production_engine_import_order_arms_guard_before_use(self):
        code = textwrap.dedent(
            """
            import time
            import core.data_engine_v2
            import core.providers
            from core.analysis import identity_guard
            from core.providers.market_truth_activation import activation_snapshot

            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if getattr(
                    identity_guard,
                    "_TFB_MARKET_METADATA_TRUTH_PATCHED",
                    False,
                ):
                    break
                time.sleep(0.01)

            assert getattr(
                identity_guard,
                "_TFB_MARKET_METADATA_TRUTH_PATCHED",
                False,
            ) is True, activation_snapshot()

            result = identity_guard.guard_sheet_rows(
                [{
                    "symbol": "OQBI.OM",
                    "name": "Oman Investment Bank",
                    "current_price": 0.12,
                    "exchange": "NASDAQ/NYSE",
                    "currency": "USD",
                    "country": "USA",
                    "asset_class": "Equity",
                    "warnings": "",
                    "block_reason": "",
                }],
                sheet="Market_Leaders",
                run_dedup=False,
            ).apply()[0]

            assert result["exchange"] == "MSX"
            assert result["currency"] == "OMR"
            assert result["country"] == "Oman"
            assert result["investability_status"] == "BLOCKED"
            assert result["final_action"] == "DO_NOT_INVEST"
            """
        )
        completed = subprocess.run(
            [sys.executable, "-c", code],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            completed.returncode,
            0,
            completed.stdout + completed.stderr,
        )


if __name__ == "__main__":
    unittest.main()
