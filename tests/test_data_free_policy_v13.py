from __future__ import annotations

import unittest

from scripts import diagnose_data_free_rows_v13 as policy

base = policy.base

HEADERS = [
    "Symbol",
    "Name",
    "Current Price",
    "Data Provider",
    "Exchange",
    "Currency",
    "Country",
    "Asset Class",
    "Warnings",
    "Block Reason",
    "Row Source",
    "Investability Status",
    "Final Action",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
]


def row(
    symbol: str,
    *,
    name: str = "Example Co",
    price: object = 10.0,
    provider: str = "eodhd",
    exchange: str = "NYSE",
    currency: str = "USD",
    country: str = "USA",
    warnings: str = "",
) -> list[object]:
    return [
        symbol,
        name,
        price,
        provider,
        exchange,
        currency,
        country,
        "Equity",
        warnings,
        "",
        "engine",
        "INVESTABLE",
        "INVEST",
        "2026-08-01T00:00:00Z",
        "2026-08-01 03:00:00",
    ]


def payload(symbol: str, values: list[object], *, fresh: int = 1, data_free: int = 0):
    return base.build_diagnostic_payload(
        headers=HEADERS,
        requested_symbols=[symbol],
        rows_by_symbol={symbol: values},
        collector_metrics={
            "symbols_requested": 1,
            "symbols_fresh": fresh,
            "symbols_data_free": data_free,
        },
        result_payload={"status": "success"},
        planned_writes=[{"sheet_name": "Market_Leaders", "rows": 1}],
        clear_requests=[],
        page="Market_Leaders",
        backend_url="https://example.invalid",
    )


class DataFreePolicyV13Tests(unittest.TestCase):
    def test_sau_is_a_valid_tadawul_exchange_alias(self):
        result = payload(
            "1321.SR",
            row(
                "1321.SR",
                exchange="SAU",
                currency="SAR",
                country="Saudi Arabia",
            ),
        )
        record = result["decision_eligibility"][0]
        self.assertTrue(record["decision_eligible"])
        self.assertNotIn("metadata_exchange_conflict", record["reason_codes"])
        self.assertEqual(result["summary"]["metadata_conflict_rows"], 0)

    def test_verified_alternative_turns_404_into_warning_only(self):
        result = payload(
            "SUM.NZ",
            row(
                "SUM.NZ",
                name="Summerset Group Holdings Limited",
                price=8.33,
                exchange="NZX",
                currency="NZD",
                country="New Zealand",
                warnings=(
                    "fetch_failed:HTTP 404 not_found; "
                    "xprovider_verified:yahoo_chart:0.0%"
                ),
            ),
        )
        record = result["decision_eligibility"][0]
        self.assertTrue(record["decision_eligible"])
        self.assertIn(
            "provider_http_404_alternate_verified",
            record["reason_codes"],
        )
        self.assertNotIn("provider_http_404", record["reason_codes"])
        self.assertEqual(
            result["summary"]["provider_warning_counts"]["http_404_rows"],
            1,
        )

    def test_unverified_404_remains_blocked(self):
        result = payload(
            "SUM.NZ",
            row(
                "SUM.NZ",
                name="Summerset Group Holdings Limited",
                price=8.33,
                exchange="NZX",
                currency="NZD",
                country="New Zealand",
                warnings="fetch_failed:HTTP 404 not_found",
            ),
        )
        record = result["decision_eligibility"][0]
        self.assertFalse(record["decision_eligible"])
        self.assertIn("provider_http_404", record["reason_codes"])
        self.assertEqual(
            record["availability_class"],
            "PROVIDER_UNAVAILABLE_OR_ERROR",
        )

    def test_missing_facts_stay_blocked_even_with_alternative_marker(self):
        result = payload(
            "EBOS.NZ",
            row(
                "EBOS.NZ",
                name="",
                price=None,
                exchange="NZX",
                currency="NZD",
                country="New Zealand",
                warnings=(
                    "fetch_failed:HTTP 404 not_found; "
                    "xprovider_verified:yahoo_chart:0.0%"
                ),
            ),
            fresh=0,
            data_free=1,
        )
        record = result["decision_eligibility"][0]
        self.assertFalse(record["decision_eligible"])
        self.assertIn("missing_name", record["reason_codes"])
        self.assertIn("missing_price", record["reason_codes"])
        self.assertIn("provider_http_404", record["reason_codes"])


if __name__ == "__main__":
    unittest.main()
