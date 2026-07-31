from __future__ import annotations

import unittest

from scripts import diagnose_data_free_rows as diagnostic


HEADERS = [
    "Symbol",
    "Name",
    "Current Price",
    "Data Provider",
    "Exchange",
    "Currency",
    "Asset Class",
    "Warnings",
    "Block Reason",
    "Row Source",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
]


def row(
    symbol: str,
    name: str = "Example Co",
    price: object = 10.0,
    provider: str = "eodhd",
    warnings: str = "",
    block_reason: str = "",
) -> list[object]:
    return [
        symbol,
        name,
        price,
        provider,
        "TEST",
        "USD",
        "Equity",
        warnings,
        block_reason,
        "engine",
        "2026-08-01T00:00:00Z",
        "2026-08-01 03:00:00",
    ]


class DataFreeDiagnosticTests(unittest.TestCase):
    def _payload(self, requested, rows_by_symbol, fresh, data_free):
        return diagnostic.build_diagnostic_payload(
            headers=HEADERS,
            requested_symbols=requested,
            rows_by_symbol=rows_by_symbol,
            collector_metrics={
                "symbols_requested": len(requested),
                "symbols_fresh": fresh,
                "symbols_data_free": data_free,
            },
            result_payload={"status": "success"},
            planned_writes=[{"sheet_name": "Market_Leaders", "rows": len(requested)}],
            clear_requests=[],
            page="Market_Leaders",
            backend_url="https://example.invalid",
        )

    def test_healthy_row_is_decision_eligible_and_not_listed(self):
        payload = self._payload(["AAA.US"], {"AAA.US": row("AAA.US")}, 1, 0)
        self.assertTrue(payload["evidence_consistent"])
        self.assertEqual(payload["summary"]["fresh_symbols"], 1)
        self.assertEqual(payload["summary"]["data_free_symbols"], 0)
        self.assertEqual(payload["data_free_rows"], [])

    def test_missing_facts_are_explicit_and_not_zero_filled(self):
        payload = self._payload(
            ["BAD.PS"],
            {"BAD.PS": row("BAD.PS", name="", price=None, provider="")},
            0,
            1,
        )
        record = payload["data_free_rows"][0]
        self.assertEqual(record["symbol"], "BAD.PS")
        self.assertEqual(record["current_price"], None)
        self.assertIn("missing_name", record["reason_codes"])
        self.assertIn("missing_price", record["reason_codes"])
        self.assertIn("missing_provider", record["reason_codes"])
        self.assertEqual(record["availability_class"], "PROVIDER_UNAVAILABLE_OR_ERROR")
        self.assertFalse(record["decision_eligible"])

    def test_identity_quarantine_has_stronger_classification(self):
        payload = self._payload(
            ["Q.AB"],
            {
                "Q.AB": row(
                    "Q.AB",
                    name="",
                    price=None,
                    provider="eodhd",
                    warnings="identity_quarantined:sheet_guard",
                )
            },
            0,
            1,
        )
        record = payload["data_free_rows"][0]
        self.assertIn("identity_blocked_or_quarantined", record["reason_codes"])
        self.assertEqual(record["availability_class"], "IDENTITY_BLOCKED")

    def test_missing_response_row_is_distinct_from_provider_stub(self):
        payload = self._payload(["MISS.OM"], {}, 0, 1)
        record = payload["data_free_rows"][0]
        self.assertEqual(record["reason_codes"], ["missing_response_row"])
        self.assertEqual(record["availability_class"], "MISSING_RESPONSE_ROW")

    def test_all_data_free_rows_are_retained_without_cap(self):
        requested = [f"S{i}.PS" for i in range(170)]
        rows = {
            symbol: row(symbol, name="", price=None, provider="unavailable")
            for symbol in requested
        }
        payload = self._payload(requested, rows, 0, 170)
        self.assertTrue(payload["evidence_consistent"])
        self.assertEqual(len(payload["data_free_rows"]), 170)
        self.assertEqual(payload["summary"]["symbol_bucket_counts"][".PS"], 170)

    def test_evidence_mismatch_fails_closed(self):
        payload = self._payload(
            ["A.US", "B.US"],
            {"A.US": row("A.US"), "B.US": row("B.US", name="")},
            2,
            0,
        )
        self.assertFalse(payload["evidence_consistent"])

    def test_symbol_bucket_vocabulary(self):
        self.assertEqual(diagnostic._symbol_bucket("AAPL.US"), ".US")
        self.assertEqual(diagnostic._symbol_bucket("USB"), "BARE")
        self.assertEqual(diagnostic._symbol_bucket("EURUSD=X"), "FX")
        self.assertEqual(diagnostic._symbol_bucket("CL=F"), "FUTURE")
        self.assertEqual(diagnostic._symbol_bucket("BTC-USD"), "CRYPTO")
        self.assertEqual(diagnostic._symbol_bucket("^GSPC"), "INDEX")

    def test_parser_keeps_sequential_default(self):
        args = diagnostic.create_parser().parse_args([])
        self.assertEqual(args.concurrency, 1)


if __name__ == "__main__":
    unittest.main()
