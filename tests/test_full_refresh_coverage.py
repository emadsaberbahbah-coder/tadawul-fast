from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from scripts.audit_full_refresh_coverage import (
    Rule, audit_grid, ledger_symbols, parse_dt, parse_dt_precision,
)

NOW = datetime(2026, 7, 30, 8, 0, 0, tzinfo=timezone.utc)
HEADERS = ["Symbol", "Name", "Current Price", "Last Updated (UTC)", "Position Qty", "Avg Cost"]


class FullRefreshCoverageTests(unittest.TestCase):
    def test_clean_market_page_passes(self) -> None:
        grid = [HEADERS, ["AAA", "Alpha", 10, "2026-07-30 07:30:00", "", ""], ["BBB", "Beta", 20, "2026-07-30 07:00:00", "", ""]]
        result = audit_grid(grid, Rule("Market_Leaders", 2, 30, 100, 100, 100), HEADERS, NOW)
        self.assertEqual(result.status, "PASS")
        self.assertEqual(result.rows, 2)
        self.assertEqual(result.fresh_pct, 100.0)

    def test_duplicate_and_stale_page_fails(self) -> None:
        grid = [HEADERS, ["AAA", "Alpha", 10, "2026-07-20 07:30:00", "", ""], ["AAA", "Alpha", 10, "2026-07-30 07:00:00", "", ""]]
        result = audit_grid(grid, Rule("Global_Markets", 2, 30, 95, 100, 100), HEADERS, NOW)
        self.assertEqual(result.status, "FAIL")
        self.assertEqual(result.duplicates, ["AAA"])
        self.assertLess(result.fresh_pct or 0, 95)

    def test_portfolio_requires_all_active_ledger_symbols(self) -> None:
        grid = [HEADERS, ["AAA", "Alpha", 10, "2026-07-30 07:30:00", 5, 8]]
        result = audit_grid(grid, Rule("My_Portfolio", 1, 8, 100, 100, 100, portfolio=True), HEADERS, NOW, active=["AAA", "BBB"])
        self.assertEqual(result.status, "FAIL")
        self.assertEqual(result.missing_portfolio, ["BBB"])

    def test_ledger_reader_uses_active_positive_lots(self) -> None:
        grid = [["Portfolio Ledger"], [], [], ["Symbol", "Status", "Shares"], ["AAA", "Active", 5], ["BBB", "Inactive", 10], ["CCC", "Active", 0]]
        active, warnings = ledger_symbols(grid)
        self.assertEqual(active, ["AAA"])
        self.assertEqual(warnings, [])

    def test_parse_google_serial_and_iso(self) -> None:
        self.assertEqual(parse_dt(46200.5), datetime(1899, 12, 30) + timedelta(days=46200.5))
        # P-104: output is always naive Riyadh, including UTC Z inputs.
        self.assertEqual(parse_dt("2026-07-30T07:30:00Z"), datetime(2026, 7, 30, 10, 30))

    def test_same_instant_has_one_riyadh_representation(self) -> None:
        expected = datetime(2026, 7, 30, 10, 30)
        for value in (
            "2026-07-30T07:30:00Z",
            "2026-07-30T07:30:00+00:00",
            "2026-07-30T10:30:00+03:00",
            datetime(2026, 7, 30, 7, 30, tzinfo=timezone.utc),
            "2026-07-30 10:30:00",
        ):
            with self.subTest(value=value):
                self.assertEqual(parse_dt_precision(value), (expected, "datetime"))

    def test_date_only_does_not_claim_intraday_precision(self) -> None:
        self.assertEqual(parse_dt_precision("2026-07-30")[1], "date")
        self.assertEqual(parse_dt_precision(46200.0)[1], "date")
        self.assertEqual(parse_dt_precision(46200.5)[1], "datetime")
        self.assertEqual(parse_dt_precision("not-a-date"), (None, "none"))

    def test_utc_freshness_age_is_not_shifted_three_hours(self) -> None:
        for header in ("Last Updated (UTC)", "Last Updated (Riyadh)"):
            headers = ["Symbol", "Name", "Current Price", header]
            grid = [headers, ["AAA", "Alpha", 10, "2026-07-30T07:30:00Z"]]
            with self.subTest(header=header):
                result = audit_grid(grid, Rule("Global_Markets", 1, 1, 100, 100, 100), headers, NOW)
                self.assertEqual(result.status, "PASS")
                self.assertEqual(result.oldest_age_h, 0.5)


if __name__ == "__main__":
    unittest.main()
