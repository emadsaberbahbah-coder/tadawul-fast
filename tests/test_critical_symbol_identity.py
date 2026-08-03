from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from scripts import run_dashboard_sync as rds
from scripts.critical_symbol_identity import (
    CRITICAL_FETCH_SYMBOLS,
    CRITICAL_IDENTITIES,
    CRITICAL_IDENTITY_TAG,
    POLICY_VERSION,
    build_isolated_batches,
    fail_result_on_identity,
    quarantine_critical_rows,
    sanitize_active_universe,
    validate_fresh_critical_rows,
)

# v2 (2026-08-03): version-adaptive. v1.1.0 retired the dead suffixed tickers
# (BK.US / FI.US / BJK.US -> INACTIVE) and maps bare provider aliases to the
# LIVE successors (BK->BNY.US, FI->FISV.US, BJK->GENZ.US); BNY.US replaces
# BK.US as the NYSE canary. Every expectation below derives from the module's
# own registry (or branches on _V11), so this suite is green on either policy
# and on any merge order.
_V11 = tuple(int(x) for x in str(POLICY_VERSION).split(".")) >= (1, 1, 0)
_NYSE_CANARY = "BNY.US" if _V11 else "BK.US"

HEADERS = ["Symbol", "Name", "Exchange", "Currency", "Country", "Current Price", "Warnings"]
PRODUCTION_HEADERS = HEADERS + ["Data Provider"]


def _chunks(seq, n):
    return [list(seq[i:i + n]) for i in range(0, len(seq), n)]


class CriticalSymbolIdentityTests(unittest.TestCase):
    def test_universe_removes_retired_and_canonicalizes_collisions(self):
        universe = ["AAPL", "BK", "BK.US", "BRK-B", "FI", "FI.US",
                    "3001.SR", "8270.SR", "4328.SR"]
        clean, changes = sanitize_active_universe(universe)
        actions = {(c.source_symbol, c.action, c.target_symbol) for c in changes}
        if _V11:
            self.assertEqual(clean, ["AAPL", "BNY.US", "BRK-B.US", "FISV.US"])
            self.assertIn(("BK", "canonicalized", "BNY.US"), actions)
            self.assertIn(("BK.US", "removed", ""), actions)
            self.assertIn(("FI.US", "removed", ""), actions)
        else:
            self.assertEqual(clean, ["AAPL", "BK.US", "BRK-B.US", "FISV.US"])
            self.assertIn(("BK", "canonicalized", "BK.US"), actions)
        self.assertIn(("BRK-B", "canonicalized", "BRK-B.US"), actions)
        self.assertIn(("FI", "canonicalized", "FISV.US"), actions)
        for retired in ("3001.SR", "8270.SR", "4328.SR"):
            self.assertIn((retired, "removed", ""), actions)

    def test_critical_symbols_get_single_symbol_batches_first(self):
        universe = ["AAPL", "BK.US", "MSFT", "BRK-B.US", "FISV.US", "NVDA"]
        crit = [s for s in universe if s in CRITICAL_FETCH_SYMBOLS]
        rest = [s for s in universe if s not in CRITICAL_FETCH_SYMBOLS]
        self.assertEqual(build_isolated_batches(universe, 2),
                         [[s] for s in crit] + _chunks(rest, 2))

    def test_correct_current_identities_pass(self):
        rows = [
            [_NYSE_CANARY,
             "BNY" if _V11 else "The Bank of New York Mellon Corporation",
             "NYSE", "USD", "USA", 116.0, ""],
            ["BRK-B.US", "Berkshire Hathaway Inc", "NYSE", "USD", "United States", 492.0, ""],
            ["FISV.US", "Fiserv, Inc.", "NASDAQ", "USD", "USA", 51.0, ""],
        ]
        out, failures = quarantine_critical_rows(HEADERS, [list(r) for r in rows])
        self.assertEqual(failures, [])
        self.assertEqual(out, rows)

    def test_known_provider_collisions_are_quarantined(self):
        rows = [
            [_NYSE_CANARY, "Hanwha Aerospace Co., Ltd.", "NYSE/NASDAQ", "USD", "USA", 979000, ""],
            ["BRK-B.US", "National Bank of Bahrain B.S.C.", "NYSE/NASDAQ", "USD", "USA", 0.52, ""],
            ["FISV.US", "Western Digital Corporation", "NASDAQ/NYSE", "USD", "USA", 499.33, ""],
        ]
        out, failures = quarantine_critical_rows(HEADERS, rows)
        self.assertEqual([f.symbol for f in failures],
                         [_NYSE_CANARY, "BRK-B.US", "FISV.US"])
        for row in out:
            self.assertTrue(row[0])
            self.assertEqual(row[1], "")
            self.assertEqual(row[6], CRITICAL_IDENTITY_TAG)

    def test_response_aliases_are_canonicalized_before_identity_rules(self):
        if _V11:
            rows = [
                ["FI", "Western Digital Corporation", "NASDAQ", "USD", "USA", 499.33, ""],
                ["BRK.B", "National Bank of Bahrain", "NYSE", "USD", "USA", 0.52, ""],
                ["BK", "Hanwha Aerospace", "NYSE", "USD", "USA", 979000, ""],
            ]
            expect = ["FISV.US", "BRK-B.US", "BNY.US"]
        else:
            rows = [
                ["FI.US", "Western Digital Corporation", "NASDAQ", "USD", "USA", 499.33, ""],
                ["BRK.B", "National Bank of Bahrain", "NYSE", "USD", "USA", 0.52, ""],
                ["BK", "Hanwha Aerospace", "NYSE", "USD", "USA", 979000, ""],
            ]
            expect = ["FISV.US", "BRK-B.US", "BK.US"]
        _, failures = quarantine_critical_rows(HEADERS, rows)
        self.assertEqual([row[0] for row in rows], expect)
        self.assertEqual([failure.symbol for failure in failures], expect)

    def test_missing_fresh_critical_row_fails_even_if_predecessor_is_available(self):
        fresh_rows = [["AAPL", "Apple Inc.", "NASDAQ", "USD", "USA", 200.0, ""]]
        requested = ["AAPL", "FI" if _V11 else "FI.US"]
        _, failures = validate_fresh_critical_rows(HEADERS, fresh_rows, requested)
        fresh_rows.append(["FISV.US", "Fiserv, Inc.", "NASDAQ", "USD", "USA", 51.0, ""])
        result = SimpleNamespace(status="success", rows_failed=0, error=None)
        fail_result_on_identity(result, failures)
        self.assertEqual([(f.symbol, f.reason) for f in failures],
                         [("FISV.US", "missing fresh response row")])
        self.assertEqual(result.status, "failed")
        self.assertEqual(fresh_rows[-1][1], "Fiserv, Inc.")

    def test_existing_canary_poison_is_blocked(self):
        rows = [[_NYSE_CANARY, "Saudi Enaya Cooperative Insurance Company",
                 "NYSE/NASDAQ", "USD", "USA", 8.95, ""]]
        _, failures = quarantine_critical_rows(HEADERS, rows)
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0].reason, "issuer name mismatch")

    def test_blank_name_fails_closed(self):
        rows = [["BRK-B.US", "", "NYSE", "USD", "USA", 492.0, ""]]
        _, failures = quarantine_critical_rows(HEADERS, rows)
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0].reason, "blank instrument name")

    def test_wrong_currency_fails_even_with_right_name(self):
        name = "BNY" if _V11 else "The Bank of New York Mellon Corporation"
        rows = [[_NYSE_CANARY, name, "NYSE", "KRW", "USA", 116.0, ""]]
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
    async def test_batched_alias_responses_are_canonicalized(self):
        backend = _Backend(
            lambda payload: [
                [symbol, "placeholder", "NYSE", "USD", "USA", 1.0, "", "test"]
                for symbol in payload["symbols"]
            ]
        )
        task = rds.TaskSpec("MARKET_LEADERS", "Market_Leaders", "analysis", max_symbols=10)
        result = rds.TaskResult(
            key=task.key, sheet_name=task.sheet_name,
            status="pending", start_utc="2026-07-29T00:00:00+00:00",
        )
        env = {
            "TFB_SYNC_SYMBOL_BATCH_SIZE": "1",
            "TFB_SYNC_BATCH_IDENTITY": "1",
            "TFB_SYNC_BATCH_RETRY": "0",
            "TFB_SYNC_TIME_BUDGET_SEC": "0",
        }
        aliases = ["FI", "BRK.B", "BK"] if _V11 else ["FI.US", "BRK.B", "BK"]
        expect = (["FISV.US", "BRK-B.US", "BNY.US"] if _V11
                  else ["FISV.US", "BRK-B.US", "BK.US"])
        with patch.dict(os.environ, env, clear=False):
            headers, rows, _, _ = await rds._fetch_market_rows_batched(
                backend, task, aliases, {}, "analysis", result
            )
        self.assertEqual(headers, PRODUCTION_HEADERS)
        self.assertEqual([row[0] for row in rows], expect)

    async def test_non_batched_membership_canonicalizes_alias_responses(self):
        if _V11:
            rows = [
                ["FI", "Fiserv, Inc.", "NASDAQ", "USD", "USA", 51.0, "", "test"],
                ["BRK.B", "Berkshire Hathaway Inc.", "NYSE", "USD", "USA", 492.0, "", "test"],
                ["BK", "BNY", "NYSE", "USD", "USA", 116.0, "", "test"],
            ]
            req = ["FI", "BRK.B", "BK"]
            expect = ["FISV.US", "BRK-B.US", "BNY.US"]
        else:
            rows = [
                ["FI.US", "Fiserv, Inc.", "NASDAQ", "USD", "USA", 51.0, "", "test"],
                ["BRK.B", "Berkshire Hathaway Inc.", "NYSE", "USD", "USA", 492.0, "", "test"],
                ["BK", "The Bank of New York Mellon Corporation", "NYSE", "USD", "USA", 116.0, "", "test"],
            ]
            req = ["FI.US", "BRK.B", "BK"]
            expect = ["FISV.US", "BRK-B.US", "BK.US"]
        kept, dropped = rds._filter_rows_to_requested(PRODUCTION_HEADERS, rows, req)
        self.assertEqual(dropped, [])
        self.assertEqual([row[0] for row in kept], expect)

    async def test_run_one_task_no_credentials_fails_missing_fresh_critical_proof(self):
        backend = _Backend(
            lambda payload: [
                ["AAPL", "Apple Inc.", "NASDAQ", "USD", "USA", 200.0, "", "test"]
            ]
        )
        task = rds.TaskSpec("MY_PORTFOLIO", "My_Portfolio", "enriched", max_symbols=10)
        requested = ["FI"] if _V11 else ["FI.US"]
        with patch.object(rds, "_read_symbols", return_value=requested), \
             patch.dict(os.environ, {"TFB_PORTFOLIO_REBUILD": "0"}, clear=False):
            result = await rds._run_one_task(
                task, "sheet", "A5", -1, False, False, backend, None
            )
        self.assertEqual(result.status, "failed")
        self.assertIn("FISV.US", result.error or "")
        self.assertEqual(result.rows_written, 0)

    async def test_run_one_task_successful_write_still_fails_missing_fresh_proof(self):
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
        self.assertEqual(len(sheets.writes), 1, "the write path must actually execute")
        self.assertEqual([row[0] for row in sheets.writes[0][2]], ["AAPL", "FISV.US"])
        self.assertEqual(result.rows_written, 2)
        self.assertEqual(result.status, "failed")
        self.assertIn("FISV.US", result.error or "")


if __name__ == "__main__":
    unittest.main()
