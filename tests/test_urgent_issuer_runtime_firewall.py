from __future__ import annotations

import unittest

from core.analysis import identity_guard
from core.providers.urgent_issuer_firewall import (
    FINDING_REASON,
    WARNING_TAG,
    ensure_urgent_issuer_firewall,
)
from core.symbols.runtime_truth_patch import ensure_identity_guard_truth_patch


def _arm() -> None:
    if not ensure_identity_guard_truth_patch():
        raise AssertionError("market-truth identity wrapper did not arm")
    if not ensure_urgent_issuer_firewall():
        raise AssertionError("urgent issuer firewall did not arm")


def _row(
    symbol: str,
    name: str,
    *,
    price: float = 100.0,
    exchange: str = "NASDAQ/NYSE",
    currency: str = "USD",
    country: str = "USA",
) -> dict:
    return {
        "symbol": symbol,
        "name": name,
        "asset_class": "Equity",
        "exchange": exchange,
        "currency": currency,
        "country": country,
        "sector": "Wrong sector",
        "industry": "Wrong industry",
        "current_price": price,
        "previous_close": price - 1,
        "overall_score": 88.0,
        "forecast_price_12m": price * 1.3,
        "recommendation": "BUY",
        "recommendation_detail": "BUY",
        "rank_overall": 1,
        "data_provider": "eodhd",
        "last_updated_utc": "2026-08-01T09:00:00+00:00",
        "warnings": "",
        "block_reason": "",
        "investability_status": "INVESTABLE",
        "final_action": "INVEST",
    }


class UrgentIssuerRuntimeFirewallTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _arm()

    def test_philippine_wrong_issuer_is_cleared_and_blocked(self):
        plan = identity_guard.guard_sheet_rows(
            [_row("BPI.PS", "Equinix, Inc.", price=1034.86)],
            sheet="Market_Leaders",
            run_dedup=False,
        )
        result = plan.apply()[0]

        self.assertEqual(result["symbol"], "BPI.PS")
        self.assertEqual(result["exchange"], "PSE")
        self.assertEqual(result["currency"], "PHP")
        self.assertEqual(result["country"], "Philippines")
        self.assertIsNone(result["name"])
        self.assertIsNone(result["current_price"])
        self.assertIsNone(result["recommendation"])
        self.assertIsNone(result["overall_score"])
        self.assertEqual(result["investability_status"], "BLOCKED")
        self.assertEqual(result["final_action"], "DO_NOT_INVEST")
        self.assertIn(WARNING_TAG, result["warnings"])
        self.assertIn("Symbol/Issuer mismatch", result["block_reason"])
        self.assertIn(
            ("BPI.PS", FINDING_REASON),
            {(finding.symbol, finding.reason) for finding in plan.findings},
        )

    def test_abu_dhabi_wrong_issuer_is_cleared_and_blocked(self):
        result = identity_guard.guard_sheet_rows(
            [_row("ALDAR.AB", "Banco Santander (Brasil) S.A.", price=28.65)],
            sheet="Market_Leaders",
            run_dedup=False,
        ).apply()[0]

        self.assertEqual(result["exchange"], "ADX")
        self.assertEqual(result["currency"], "AED")
        self.assertEqual(result["country"], "United Arab Emirates")
        self.assertIsNone(result["name"])
        self.assertIsNone(result["current_price"])
        self.assertEqual(result["final_action"], "DO_NOT_INVEST")
        self.assertIn(WARNING_TAG, result["warnings"])

    def test_oman_wrong_issuer_is_cleared_and_blocked(self):
        result = identity_guard.guard_sheet_rows(
            [_row("OQGN.OM", "Marsh & McLennan Companies, Inc.", price=180.19)],
            sheet="Market_Leaders",
            run_dedup=False,
        ).apply()[0]

        self.assertEqual(result["exchange"], "MSX")
        self.assertEqual(result["currency"], "OMR")
        self.assertEqual(result["country"], "Oman")
        self.assertIsNone(result["name"])
        self.assertIsNone(result["current_price"])
        self.assertEqual(result["investability_status"], "BLOCKED")
        self.assertEqual(result["final_action"], "DO_NOT_INVEST")

    def test_correct_known_issuer_is_not_cleared(self):
        result = identity_guard.guard_sheet_rows(
            [
                _row(
                    "BPI.PS",
                    "Bank of the Philippine Islands",
                    price=120.0,
                )
            ],
            sheet="Market_Leaders",
            run_dedup=False,
        ).apply()[0]

        self.assertEqual(result["name"], "Bank of the Philippine Islands")
        self.assertEqual(result["current_price"], 120.0)
        self.assertNotIn(WARNING_TAG, result["warnings"])

    def test_unrelated_symbol_is_unchanged_by_exact_registry(self):
        result = identity_guard.guard_sheet_rows(
            [
                _row(
                    "AAPL.US",
                    "Apple Inc.",
                    price=200.0,
                    exchange="NYSE/NASDAQ",
                    currency="USD",
                    country="USA",
                )
            ],
            sheet="Global_Markets",
            run_dedup=False,
        ).apply()[0]

        self.assertEqual(result["name"], "Apple Inc.")
        self.assertEqual(result["current_price"], 200.0)
        self.assertEqual(result["recommendation"], "BUY")
        self.assertNotIn(WARNING_TAG, result["warnings"])

    def test_large_known_mismatch_cluster_fails_closed_without_guard_refusal(self):
        known = [
            ("AC.PS", "NXP Semiconductors N.V."),
            ("SCC.PS", "Stifel Financial Corp."),
            ("BDO.PS", "AECOM"),
            ("SMPH.PS", "Xiaomi Corporation"),
            ("BPI.PS", "Equinix, Inc."),
            ("TAQA.AB", "Teleperformance SE"),
            ("FAB.AB", "Booz Allen Hamilton Holding Corporation"),
            ("ALDAR.AB", "Banco Santander (Brasil) S.A."),
            ("ADNOCGAS.AB", "Nebius Group N.V."),
            ("OQGN.OM", "Marsh & McLennan Companies, Inc."),
        ]
        rows = [
            _row(symbol, name, price=100.0 + index)
            for index, (symbol, name) in enumerate(known)
        ]
        rows.extend(
            _row(
                f"CLEAN{index}.US",
                f"Clean Issuer {index}",
                price=50.0 + index,
                exchange="NYSE/NASDAQ",
                currency="USD",
                country="USA",
            )
            for index in range(10)
        )

        plan = identity_guard.guard_sheet_rows(
            rows,
            sheet="Market_Leaders",
            run_dedup=False,
        )
        output = plan.apply()
        blocked = [
            row
            for row in output
            if row.get("final_action") == "DO_NOT_INVEST"
            and WARNING_TAG in str(row.get("warnings") or "")
        ]
        self.assertEqual(len(output), 20)
        self.assertEqual(len(blocked), 10)
        self.assertTrue(all(row.get("current_price") is None for row in blocked))


if __name__ == "__main__":
    unittest.main()
