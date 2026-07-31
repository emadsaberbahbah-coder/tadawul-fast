from __future__ import annotations

import unittest
from datetime import datetime, timezone

from scripts.audit_decision_surface_freshness import audit_surfaces

NOW = datetime(2026, 7, 31, 18, 0, 0, tzinfo=timezone.utc)  # 21:00 Riyadh
FLOORS = {
    "Market_Leaders": 1025,
    "Global_Markets": 6512,
    "Commodities_FX": 453,
    "Mutual_Funds": 4496,
}
HEADER = [
    "Page",
    "Last Updated",
    "Status",
    "Message",
    "Endpoint",
    "HTTP Code",
    "Rows",
    "Columns",
]


def status_grid(overrides=None):
    data = {
        "Market_Leaders": ["2026-07-31 20:00:00", "SUCCESS", "complete", 1356, 115],
        "Global_Markets": ["2026-07-31 20:00:00", "SUCCESS", "complete", 6512, 115],
        "Commodities_FX": ["2026-07-31 20:00:00", "SUCCESS", "complete", 453, 115],
        "Mutual_Funds": ["2026-07-31 20:00:00", "SUCCESS", "complete", 4496, 115],
        "My_Portfolio": ["2026-07-31 20:00:00", "VALID", "complete", 10, 122],
    }
    for key, value in (overrides or {}).items():
        data[key] = value
    rows = [HEADER]
    for page, (stamp, state, message, count, columns) in data.items():
        rows.append([page, stamp, state, message, "/read-only", 200, count, columns])
    return rows


def portfolio_grid(stamp="2026-07-31 20:05:00", state="ok"):
    return [
        ["MY PORTFOLIO — DECISION"],
        ["Status:", f"Last run {stamp} | status: {state} | holdings 10"],
    ]


def top10_grid(stamp="2026-07-31 20:10:00", state="ok", full=True):
    label = " (full universe)" if full else ""
    return [
        ["TOP 10 INVESTMENTS — DECISION"],
        [
            "Status:",
            (
                f"Last run {stamp} | status: {state} | sheets pool 11817 rows "
                "[Market_Leaders 1356/1356, Global_Markets 6512/6512, "
                "Commodities_FX 453/453, Mutual_Funds 4496/4496]"
                f"{label} | held=10 sent"
            ),
        ],
    ]


class DecisionSurfaceFreshnessTests(unittest.TestCase):
    def test_clean_synchronized_surfaces_pass(self):
        result = audit_surfaces(
            status_grid(),
            portfolio_grid(),
            top10_grid(),
            now_utc=NOW,
            min_rows=FLOORS,
        )
        self.assertEqual(result.exit_code, 0)
        self.assertTrue(result.executable)
        self.assertEqual(result.findings, [])

    def test_portfolio_decision_older_than_portfolio_source_fails(self):
        result = audit_surfaces(
            status_grid(),
            portfolio_grid("2026-07-31 19:00:00"),
            top10_grid(),
            now_utc=NOW,
            min_rows=FLOORS,
        )
        codes = {item.code for item in result.findings}
        self.assertIn("PF_OLDER_THAN_SOURCE", codes)
        self.assertEqual(result.exit_code, 2)
        self.assertFalse(result.executable)

    def test_full_universe_claim_fails_on_partial_stale_and_short_sources(self):
        grid = status_grid(
            {
                "Global_Markets": [
                    "2026-07-31 17:30:00",
                    "PARTIAL",
                    "Batch refresh paused at 840 of 6199",
                    6199,
                    115,
                ],
                "Mutual_Funds": [
                    "2026-07-22 11:00:00",
                    "ROW_REFRESH_OK",
                    "Selected row refreshed",
                    1,
                    115,
                ],
                "Commodities_FX": [
                    "2026-07-14 11:00:00",
                    "SUCCESS",
                    "Batch refresh completed",
                    251,
                    115,
                ],
            }
        )
        result = audit_surfaces(
            grid,
            portfolio_grid(),
            top10_grid(),
            now_utc=NOW,
            min_rows=FLOORS,
        )
        codes = {item.code for item in result.findings}
        self.assertIn("SOURCE_NOT_COMPLETE", codes)
        self.assertIn("SOURCE_STALE", codes)
        self.assertIn("SOURCE_ROW_FLOOR", codes)
        self.assertIn("FALSE_FULL_UNIVERSE_CLAIM", codes)
        self.assertEqual(result.exit_code, 2)

    def test_unparseable_status_table_is_fatal(self):
        result = audit_surfaces(
            [["wrong", "headers"]],
            portfolio_grid(),
            top10_grid(),
            now_utc=NOW,
            min_rows=FLOORS,
        )
        self.assertEqual(result.exit_code, 3)
        self.assertIn("could not be parsed", result.fatal)


if __name__ == "__main__":
    unittest.main()
