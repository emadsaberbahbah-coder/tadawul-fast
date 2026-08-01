from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from scripts import run_dashboard_sync as rds
from scripts.critical_symbol_identity import (
    CRITICAL_IDENTITY_TAG,
    build_isolated_batches,
    fail_result_on_identity,
    quarantine_critical_rows,
    sanitize_active_universe,
    validate_fresh_critical_rows,
)


HEADERS = ["Symbol", "Name", "Exchange", "Currency", "Country", "Current Price", "Warnings"]
PRODUCTION_HEADERS = HEADERS + ["Data Provider"]


class CriticalSymbolIdentityTests(unittest.TestCase):
    def test_universe_removes_retired_and_canonicalizes_collisions(self):
        clean, changes = sanitize_active_universe(
            ["AAPL", "BK", "BNY.US", "BRK-B", "FI", "FI.US", "3001.SR", "8270.SR", "4328.SR"]
        )
        self.assertEqual(clean, ["AAPL", "BNY.US", "BRK-B.US", "FISV.US"])
        actions = {(c.source_symbol, c.action, c.target_symbol) for c in changes}
        self.assertIn(("BK", "canonicalized", "BNY.US"), actions)
        self.assertIn(("BNY.US", "deduplicated", "BNY.US"), actions)
        self.assertIn(("BRK-B", "canonicalized", "BRK-B.US"), actions)
        self.assertIn(("FI", "canonicalized", "FISV.US"), actions)
        self.assertIn(("3001.SR", "removed", ""), actions)
        self.assertIn(("8270.SR", "removed", ""), actions)
        self.assertIn(("4328.SR", "removed", ""), actions)

    def test_stale_bk_us_lifecycle_alias_maps_to_bny(self):
        clean, changes = sanitize_active_universe(["BK.US"])
        self.assertEqual(clean, ["BNY.US"])
        self.assertEqual(changes[0].source_symbol, "BK.US")
        self.assertEqual(changes[0].target_symbol, "BNY.US")

    def test_critical_symbols_get_single_symbol_batches_first(self):
        batches = build_isolated_batches(
            ["AAPL", "BNY.US", "MSFT", "BRK-B.US", "FISV.US", "NVDA"], 2
        )
        self.assertEqual(
            batches,
            [["BNY.US"], ["BRK-B.US"], ["FISV.US"], ["AAPL", "MSFT"], ["NVDA"]],
        )

    def test_correct_current_identities_pass(self):
        rows = [
            ["BNY.US", "The Bank of New York Mellon Corporation", "NYSE", "USD", "USA", 116.0, ""],
            ["BRK-B.US", "Berkshire Hathaway Inc", "NYSE", "USD", "United States", 492.0, ""],
            ["FISV.US", "Fiserv, Inc.", "NASDAQ", "USD", "USA", 51.0, ""],
        ]
        out, failures = quarantine_critical_rows(HEADERS, rows)
        self.assertEqual(failures, [])
        self.assertEqual(out, rows)

    def test_known_provider_collisions_are_quarantined(self):
        rows = [
            ["BNY.US", "Hanwha Aerospace Co., Ltd.", "NYSE/NASDAQ", "USD", "USA", 979000, ""],
            ["BRK-B.US", "National Bank of Bahrain B.S.C.", "NYSE/NASDAQ", "USD", "USA", 0.52, ""],
            ["FISV.US", "Western Digital Corporation", "NASDAQ/NYSE", "USD", "USA", 499.33, ""],
        ]
        out, failures = quarantine_critical_rows(HEADERS, rows)
        self.assertEqual([f.symbol for f in failures], ["BNY.US", "BRK-B.US", "FISV.US"])
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
        self.assertEqual([row[0] for row in rows], ["FISV.US", "BRK-B.US", "BNY.US"])
        self.assertEqual([failure.symbol for failure in failures], ["FISV.US", "BRK-B.US", "BNY.US"])

    def test_missing_fresh_critical_row_fails_even_if_predecessor_is_available(self):
        fresh_rows = [["AAPL", "Apple Inc.", "NASDAQ", "USD", "USA", 200.0, ""]]
        _, failures = validate_fresh_critical_rows(
            HEADERS, fresh_rows, ["AAPL", "FI.US"]
        )
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
        rows = [["BNY.US", "Saudi Enaya Cooperative Insurance Company", "NYSE/NASDAQ", "USD", "USA", 8.95, ""]]
        _, failures = quarantine_critical_rows(HEADERS, rows)
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0].reason, "issuer name mismatch")

    def test_blank_name_fails_closed(self):
        rows = [["BRK-B.US", "", "NYSE", "USD", "USA", 492.0, ""]]
        _, failures = quarantine_critical_rows(HEADERS, rows)
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0].reason, "blank instrument name")

    def test_wrong_currency_fails_even_with_right_name(self):
        rows = [["BNY.US", "The Bank of New York Mellon Corporation", "NYSE", "KRW", "USA", 116.0, ""]]
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


class _Backend:
    def __init__(self, response_rows):
        self.response_rows = response_rows
        self.calls = []

    async def post_json(self, path, payload):
        self.calls.append((path, dict(payload)))
        return {
            "headers": list(PRODUCTION_HEADERS),
            "rows_matrix": self.response_rows(payload),
        }, None, 200


class _Sheets:
    def __init__(self, grid):
        self.grid = grid
        self.writes = []

    def _get_service(self):
        return object()

    def read_values(self, spreadsheet_id, sheet_name, a1_range="A1:EZ2000"):
        return [list(row) for row in self.grid]

    def write_table(self, spreadsheet_id, sheet_name, start_a1, headers, rows):
        self.writes.append((sheet_name, list(headers), [list(row) for row in rows]))
        return len(rows)

    def clear_from(self, *args, **kwargs):
        return None


class CriticalIdentityProductionPathTests(unittest.IsolatedAsyncioTestCase):

    def test_request_scoped_us_suffix_echoes_are_rewritten_to_requested_spelling(self):
        requested = ["HNGE", "TT.US", "ADP", "ITW.US"]
        rows = [
            ["HNGE.US", "Hinge Health", "NYSE", "USD", "USA", 50.0, "", "eodhd"],
            ["TT", "Trane Technologies", "NYSE", "USD", "USA", 400.0, "", "eodhd"],
            ["ADP.US", "Automatic Data Processing", "NASDAQ", "USD", "USA", 300.0, "", "eodhd"],
            ["ITW", "Illinois Tool Works", "NYSE", "USD", "USA", 250.0, "", "eodhd"],
        ]
        kept, dropped = rds._filter_rows_to_requested(
            PRODUCTION_HEADERS, rows, requested
        )
        self.assertEqual(dropped, [])
        self.assertEqual([row[0] for row in kept], requested)

    def test_request_scoped_alias_does_not_merge_two_exact_requested_spellings(self):
        index = rds._build_request_symbol_index(["AAPL", "AAPL.US"])
        self.assertEqual(rds._resolve_requested_symbol("AAPL", request_index=index), "AAPL")
        self.assertEqual(rds._resolve_requested_symbol("AAPL.US", request_index=index), "AAPL.US")

    async def test_batched_alias_responses_are_canonicalized(self):
        backend = _Backend(
            lambda payload: [
                [symbol, "placeholder", "NYSE", "USD", "USA", 1.0, "", "test"]
                for symbol in payload["symbols"]
            ]
        )
        task = rds.TaskSpec("MARKET_LEADERS", "Market_Leaders", "analysis", max_symbols=10)
        result = rds.TaskResult(
            key=task.key,
            sheet_name=task.sheet_name,
            status="pending",
            start_utc="2026-07-29T00:00:00+00:00",
        )
        env = {
            "TFB_SYNC_SYMBOL_BATCH_SIZE": "1",
            "TFB_SYNC_BATCH_IDENTITY": "1",
            "TFB_SYNC_BATCH_RETRY": "0",
            "TFB_SYNC_TIME_BUDGET_SEC": "0",
        }
        with patch.dict(os.environ, env, clear=False):
            headers, rows, _, _ = await rds._fetch_market_rows_batched(
                backend, task, ["FI.US", "BRK.B", "BK"], {}, "analysis", result
            )

        self.assertEqual(headers, PRODUCTION_HEADERS)
        self.assertEqual([row[0] for row in rows], ["FISV.US", "BRK-B.US", "BNY.US"])

    async def test_non_batched_membership_canonicalizes_alias_responses(self):
        rows = [
            ["FI.US", "Fiserv, Inc.", "NASDAQ", "USD", "USA", 51.0, "", "test"],
            ["BRK.B", "Berkshire Hathaway Inc.", "NYSE", "USD", "USA", 492.0, "", "test"],
            ["BK", "The Bank of New York Mellon Corporation", "NYSE", "USD", "USA", 116.0, "", "test"],
        ]
        kept, dropped = rds._filter_rows_to_requested(
            PRODUCTION_HEADERS, rows, ["FI.US", "BRK.B", "BK"]
        )
        self.assertEqual(dropped, [])
        self.assertEqual([row[0] for row in kept], ["FISV.US", "BRK-B.US", "BNY.US"])

    async def test_run_one_task_no_credentials_fails_missing_fresh_critical_proof(self):
        backend = _Backend(
            lambda payload: [
                ["AAPL", "Apple Inc.", "NASDAQ", "USD", "USA", 200.0, "", "test"]
            ]
        )
        task = rds.TaskSpec("MY_PORTFOLIO", "My_Portfolio", "enriched", max_symbols=10)

        with patch.object(rds, "_read_symbols", return_value=["FI.US"]), \
             patch.dict(os.environ, {"TFB_PORTFOLIO_REBUILD": "0"}, clear=False):
            result = await rds._run_one_task(
                task, "sheet", "A5", -1, False, False, backend, None
            )

        self.assertEqual(result.status, "failed")
        self.assertIn("FISV.US", result.error or "")
        self.assertEqual(result.rows_written, 0)

    async def test_run_one_task_preserves_last_good_but_fails_missing_fresh_proof(self):
        fresh_aapl = ["AAPL", "Apple Inc.", "NASDAQ", "USD", "USA", 200.0, "", "test"]
        old_fisv = ["FISV.US", "Fiserv, Inc.", "NASDAQ", "USD", "USA", 51.0, "", "eodhd"]
        sheets = _Sheets([PRODUCTION_HEADERS, fresh_aapl, old_fisv])
        backend = _Backend(lambda payload: [list(fresh_aapl)])
        task = rds.TaskSpec("MARKET_LEADERS", "Market_Leaders", "analysis", max_symbols=10)

        env = {
            "TFB_MARKET_SYMBOL_READBACK": "0",
            "TFB_SYNC_SYMBOL_BATCH_SIZE": "0",
            "TFB_SYNC_STRICT_MEMBERSHIP": "1",
            "TFB_SYNC_SYMBOL_PERSISTENCE": "1",
            "TFB_SYNC_PERSISTENCE_HARD": "1",
            "TFB_SYNC_FLOOR_STRICT": "0",
            "TFB_SYNC_IDFW_RUNLOG": "0",
            "TFB_SYNC_NAME_DEDUP_MODE": "off",
        }
        with patch.object(rds, "_read_symbols", return_value=["AAPL", "FISV.US"]), \
             patch.dict(os.environ, env, clear=False):
            result = await rds._run_one_task(
                task, "sheet", "A5", -1, False, False, backend, sheets
            )

        self.assertEqual(len(sheets.writes), 1, "safe persistence may still land")
        written_rows = sheets.writes[0][2]
        self.assertEqual([row[0] for row in written_rows], ["AAPL", "FISV.US"])
        self.assertEqual(
            written_rows[1],
            old_fisv,
            "a missing fresh proof may preserve only the exact verified last-good row",
        )
        self.assertEqual(result.rows_written, 2)
        self.assertEqual(result.status, "failed")
        self.assertIn("FISV.US", result.error or "")


if __name__ == "__main__":
    unittest.main()
