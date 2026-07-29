from __future__ import annotations

import unittest
from types import SimpleNamespace

from scripts.critical_symbol_identity import (
    CRITICAL_IDENTITY_TAG,
    build_isolated_batches,
    fail_result_on_identity,
    quarantine_critical_rows,
    sanitize_active_universe,
    validate_fresh_critical_rows,
)


HEADERS = ["Symbol", "Name", "Exchange", "Currency", "Country", "Current Price", "Warnings"]


class CriticalSymbolIdentityTests(unittest.TestCase):
    def test_universe_removes_retired_and_canonicalizes_collisions(self):
        clean, changes = sanitize_active_universe(
            ["AAPL", "BK", "BK.US", "BRK-B", "FI", "FI.US", "3001.SR", "8270.SR", "4328.SR"]
        )
        self.assertEqual(clean, ["AAPL", "BK.US", "BRK-B.US", "FISV.US"])
        actions = {(c.source_symbol, c.action, c.target_symbol) for c in changes}
        self.assertIn(("BK", "canonicalized", "BK.US"), actions)
        self.assertIn(("BRK-B", "canonicalized", "BRK-B.US"), actions)
        self.assertIn(("FI", "canonicalized", "FISV.US"), actions)
        self.assertIn(("3001.SR", "removed", ""), actions)
        self.assertIn(("8270.SR", "removed", ""), actions)
        self.assertIn(("4328.SR", "removed", ""), actions)

    def test_critical_symbols_get_single_symbol_batches_first(self):
        batches = build_isolated_batches(
            ["AAPL", "BK.US", "MSFT", "BRK-B.US", "FISV.US", "NVDA"], 2
        )
        self.assertEqual(
            batches,
            [["BK.US"], ["BRK-B.US"], ["FISV.US"], ["AAPL", "MSFT"], ["NVDA"]],
        )

    def test_correct_current_identities_pass(self):
        rows = [
            ["BK.US", "The Bank of New York Mellon Corporation", "NYSE", "USD", "USA", 116.0, ""],
            ["BRK-B.US", "Berkshire Hathaway Inc", "NYSE", "USD", "United States", 492.0, ""],
            ["FISV.US", "Fiserv, Inc.", "NASDAQ", "USD", "USA", 51.0, ""],
        ]
        out, failures = quarantine_critical_rows(HEADERS, rows)
        self.assertEqual(failures, [])
        self.assertEqual(out, rows)

    def test_known_provider_collisions_are_quarantined(self):
        rows = [
            ["BK.US", "Hanwha Aerospace Co., Ltd.", "NYSE/NASDAQ", "USD", "USA", 979000, ""],
            ["BRK-B.US", "National Bank of Bahrain B.S.C.", "NYSE/NASDAQ", "USD", "USA", 0.52, ""],
            ["FISV.US", "Western Digital Corporation", "NASDAQ/NYSE", "USD", "USA", 499.33, ""],
        ]
        out, failures = quarantine_critical_rows(HEADERS, rows)
        self.assertEqual([f.symbol for f in failures], ["BK.US", "BRK-B.US", "FISV.US"])
        for row in out:
            self.assertTrue(row[0])
            self.assertEqual(row[1], "")
            self.assertEqual(row[6], CRITICAL_IDENTITY_TAG)

    def test_response_aliases_are_canonicalized_before_identity_rules(self):
        rows = [
            ["FI.US", "Western Digital Corporation", "NASDAQ", "USD", "USA", 499.33, ""],
            ["BRK.B", "National Bank of Bahrain", "NYSE", "USD", "USA", 0.52, ""],
            ["BK", "Hanwha Aerospace", "NYSE", "USD", "USA", 979000, ""],
        ]
        _, failures = quarantine_critical_rows(HEADERS, rows)
        self.assertEqual([row[0] for row in rows], ["FISV.US", "BRK-B.US", "BK.US"])
        self.assertEqual([failure.symbol for failure in failures], ["FISV.US", "BRK-B.US", "BK.US"])

    def test_missing_fresh_critical_row_fails_even_if_predecessor_is_available(self):
        fresh_rows = [["AAPL", "Apple Inc.", "NASDAQ", "USD", "USA", 200.0, ""]]
        _, failures = validate_fresh_critical_rows(
            HEADERS, fresh_rows, ["AAPL", "FI.US"]
        )
        # Simulate persistence restoring a perfectly valid predecessor only
        # after current-run proof has already been recorded.
        fresh_rows.append(
            ["FISV.US", "Fiserv, Inc.", "NASDAQ", "USD", "USA", 51.0, ""]
        )
        result = SimpleNamespace(status="success", rows_failed=0, error=None)
        fail_result_on_identity(result, failures)
        self.assertEqual([(failure.symbol, failure.reason) for failure in failures],
                         [("FISV.US", "missing fresh response row")])
        self.assertEqual(result.status, "failed")
        self.assertEqual(fresh_rows[-1][1], "Fiserv, Inc.")

    def test_existing_bk_us_poison_is_blocked(self):
        rows = [["BK.US", "Saudi Enaya Cooperative Insurance Company", "NYSE/NASDAQ", "USD", "USA", 8.95, ""]]
        _, failures = quarantine_critical_rows(HEADERS, rows)
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0].reason, "issuer name mismatch")

    def test_blank_name_fails_closed(self):
        rows = [["BRK-B.US", "", "NYSE", "USD", "USA", 492.0, ""]]
        _, failures = quarantine_critical_rows(HEADERS, rows)
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0].reason, "blank instrument name")

    def test_wrong_currency_fails_even_with_right_name(self):
        rows = [["BK.US", "The Bank of New York Mellon Corporation", "NYSE", "KRW", "USA", 116.0, ""]]
        _, failures = quarantine_critical_rows(HEADERS, rows)
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0].reason, "currency mismatch")

    def test_page_result_cannot_remain_success_after_quarantine(self):
        rows = [["FISV.US", "Western Digital Corporation", "NASDAQ", "USD", "USA", 499.33, ""]]
        _, failures = quarantine_critical_rows(HEADERS, rows)
        result = SimpleNamespace(status="success", rows_failed=0, error=None)
        fail_result_on_identity(result, failures)
        self.assertEqual(result.status, "failed")
        self.assertEqual(result.rows_failed, 1)
        self.assertIn("FISV.US", result.error)


if __name__ == "__main__":
    unittest.main()
