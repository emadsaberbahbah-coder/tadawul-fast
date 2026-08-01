from __future__ import annotations

import asyncio
import types
import unittest

from scripts import sync_integrity_v13 as patch


HEADERS = [
    "Symbol",
    "Name",
    "Exchange",
    "Currency",
    "Country",
    "Current Price",
    "Warnings",
    "Block Reason",
    "Investability Status",
    "Final Action",
    "Data Provider",
    "Row Source",
]


class SyncIntegrityV13Tests(unittest.TestCase):
    def setUp(self):
        patch._PATCHED_SYNC_IDS.clear()
        patch._PATCHED_CRITICAL_IDS.clear()

    def test_post_klg_market_truth_corrects_and_blocks_conflict(self):
        rows = [[
            "BPI.PS",
            "",
            "NASDAQ/NYSE",
            "USD",
            "Philippines",
            "",
            "identity_quarantined",
            "",
            "",
            "",
            "fallback_error",
            "",
        ]]
        out, corrected = patch.apply_market_truth(HEADERS, rows)
        self.assertEqual(corrected, ["BPI.PS"])
        self.assertEqual(out[0][2:5], ["PSE", "PHP", "Philippines"])
        self.assertEqual(out[0][8], "BLOCKED")
        self.assertEqual(out[0][9], "DO_NOT_INVEST")
        self.assertIn("market_metadata_conflict_corrected", out[0][6])
        self.assertEqual(out[0][5], "", "price must remain unknown")

    def test_missing_metadata_is_filled_without_inventing_price(self):
        rows = [["OQGN.OM", "", "", "", "", "", "", "", "", "", "", ""]]
        out, corrected = patch.apply_market_truth(HEADERS, rows)
        self.assertEqual(corrected, ["OQGN.OM"])
        self.assertEqual(out[0][2:5], ["MSX", "OMR", "Oman"])
        self.assertEqual(out[0][5], "")
        self.assertEqual(out[0][8], "")

    def test_invalid_non_numeric_sr_is_explicitly_blocked(self):
        rows = [["ELET3.SR", "", "Tadawul", "SAR", "Saudi Arabia", "", "", "", "", "", "", ""]]
        out, _ = patch.apply_market_truth(HEADERS, rows)
        self.assertEqual(out[0][2:5], ["", "", ""])
        self.assertEqual(out[0][8], "BLOCKED")
        self.assertEqual(out[0][9], "DO_NOT_INVEST")
        self.assertIn("invalid_symbol_shape", out[0][6])

    def test_response_completion_preserves_exact_request_order(self):
        sync = types.SimpleNamespace()
        sync.canonicalize_symbol = lambda value: str(value or "").strip().upper()
        sync._build_request_symbol_index = lambda requested: (
            {symbol: symbol for symbol in requested},
            {},
        )
        sync._resolve_requested_symbol = lambda value, request_index=None: (
            str(value or "").strip().upper()
            if str(value or "").strip().upper() in request_index[0]
            else ""
        )
        rows = [["B", "Bee", "NYSE", "USD", "USA", 2.0, "", "", "", "", "eodhd", ""]]
        completed, missing = patch.complete_response_rows(sync, HEADERS, rows, ["A", "B", "C"])
        self.assertEqual([row[0] for row in completed], ["A", "B", "C"])
        self.assertEqual(missing, ["A", "C"])
        for row in (completed[0], completed[2]):
            self.assertEqual(row[1], "")
            self.assertEqual(row[5], "")
            self.assertEqual(row[8], "BLOCKED")
            self.assertEqual(row[9], "DO_NOT_INVEST")
            self.assertEqual(row[10], "unavailable")
            self.assertIn(patch.MISSING_RESPONSE_TAG, row[6])

    def test_exact_requested_spellings_do_not_merge(self):
        sync = types.SimpleNamespace()
        sync.canonicalize_symbol = lambda value: str(value or "").strip().upper()
        sync._build_request_symbol_index = lambda requested: (
            {symbol: symbol for symbol in requested},
            {},
        )
        sync._resolve_requested_symbol = lambda value, request_index=None: (
            str(value or "").strip().upper()
            if str(value or "").strip().upper() in request_index[0]
            else ""
        )
        rows = [
            ["AAPL", "Apple", "NASDAQ", "USD", "USA", 200.0, "", "", "", "", "yahoo", ""],
            ["AAPL.US", "Apple", "NASDAQ", "USD", "USA", 200.0, "", "", "", "", "eodhd", ""],
        ]
        completed, missing = patch.complete_response_rows(sync, HEADERS, rows, ["AAPL", "AAPL.US"])
        self.assertEqual([row[0] for row in completed], ["AAPL", "AAPL.US"])
        self.assertEqual(missing, [])

    def test_async_fetch_wrapper_adds_truthful_stub(self):
        sync = types.SimpleNamespace()
        sync.canonicalize_symbol = lambda value: str(value or "").strip().upper()
        sync._build_request_symbol_index = lambda requested: (
            {symbol: symbol for symbol in requested},
            {},
        )
        sync._resolve_requested_symbol = lambda value, request_index=None: (
            str(value or "").strip().upper()
            if str(value or "").strip().upper() in request_index[0]
            else ""
        )

        async def original(_backend, _task, symbols, *_args, **_kwargs):
            return HEADERS, [
                [symbols[1], "Bee", "NYSE", "USD", "USA", 2.0, "", "", "", "", "eodhd", ""]
            ], "endpoint", None

        sync._fetch_market_rows_batched = original
        self.assertTrue(patch._patch_sync_module(sync))

        async def invoke():
            return await sync._fetch_market_rows_batched(None, None, ["A", "B"])

        headers, rows, endpoint, error = asyncio.run(invoke())
        self.assertEqual(headers, HEADERS)
        self.assertEqual([row[0] for row in rows], ["A", "B"])
        self.assertEqual(endpoint, "endpoint")
        self.assertIsNone(error)
        self.assertEqual(rows[0][5], "")

    def test_critical_wrapper_applies_truth_before_existing_guard(self):
        seen = {}
        critical = types.SimpleNamespace()

        def original(headers, rows):
            seen["row"] = list(rows[0])
            return rows, []

        critical.quarantine_critical_rows = original
        sync = types.SimpleNamespace(quarantine_critical_rows=original)
        self.assertTrue(patch._patch_critical_module(critical, sync))
        rows = [["TAQA.AB", "", "NASDAQ/NYSE", "USD", "United Arab Emirates", "", "", "", "", "", "", ""]]
        sync.quarantine_critical_rows(HEADERS, rows)
        self.assertEqual(seen["row"][2:5], ["ADX", "AED", "United Arab Emirates"])
        self.assertEqual(seen["row"][8], "BLOCKED")


if __name__ == "__main__":
    unittest.main()
