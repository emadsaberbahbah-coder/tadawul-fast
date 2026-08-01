from __future__ import annotations

import unittest

from scripts.critical_symbol_identity import (
    CRITICAL_IDENTITY_TAG,
    canonicalize_symbol,
    quarantine_critical_rows,
)


HEADERS = [
    "Symbol",
    "Name",
    "Exchange",
    "Currency",
    "Country",
    "Current Price",
    "Warnings",
]


class UrgentMarketIdentityFirewallTests(unittest.TestCase):
    def test_live_contamination_examples_fail_closed_even_when_metadata_looks_valid(self):
        rows = [
            ["FERTIGLOBE.AB", "Saudi Aramco Base Oil Company - Luberef", "ADX", "AED", "United Arab Emirates", 3.1, ""],
            ["BPI.PS", "Equinix, Inc.", "PSE", "PHP", "Philippines", 120.0, ""],
            ["OQGN.OM", "Marsh & McLennan Companies, Inc.", "MSX", "OMR", "Oman", 0.2, ""],
            ["PRESIGHT.AB", "Market_Leaders AP.PS", "ADX", "AED", "UAE", 2.0, ""],
        ]

        out, failures = quarantine_critical_rows(HEADERS, rows)

        self.assertEqual(
            [(item.symbol, item.reason) for item in failures],
            [
                ("FERTIGLOBE.AB", "issuer name mismatch"),
                ("BPI.PS", "issuer name mismatch"),
                ("OQGN.OM", "issuer name mismatch"),
                ("PRESIGHT.AB", "issuer name mismatch"),
            ],
        )
        for row in out:
            self.assertTrue(row[0])
            self.assertTrue(all(cell == "" for cell in row[1:6]))
            self.assertEqual(row[6], CRITICAL_IDENTITY_TAG)

    def test_suffix_venue_conflicts_and_invalid_saudi_format_fail_closed(self):
        rows = [
            ["NEWCO.AB", "Newco PJSC", "NASDAQ/NYSE", "USD", "United Arab Emirates", 50.0, ""],
            ["NEWCO.PS", "Newco Inc.", "NASDAQ/NYSE", "USD", "Philippines", 50.0, ""],
            ["NEWCO.OM", "Newco SAOC", "NASDAQ/NYSE", "USD", "Oman", 50.0, ""],
            ["ELET3.SR", "TotalEnergies SE", "Tadawul", "SAR", "Saudi Arabia", 76.0, ""],
        ]

        _, failures = quarantine_critical_rows(HEADERS, rows)

        self.assertEqual(
            [(item.symbol, item.reason) for item in failures],
            [
                ("NEWCO.AB", "exchange mismatch"),
                ("NEWCO.PS", "exchange mismatch"),
                ("NEWCO.OM", "exchange mismatch"),
                ("ELET3.SR", "invalid Saudi symbol format"),
            ],
        )

    def test_verified_adx_pse_msx_and_tadawul_rows_pass(self):
        rows = [
            ["FERTIGLOBE.AB", "Fertiglobe plc", "ADX", "AED", "United Arab Emirates", 3.1, ""],
            ["BPI.PS", "Bank of the Philippine Islands", "PSE", "PHP", "Philippines", 120.0, ""],
            ["OQGN.OM", "OQ Gas Networks", "MSX", "OMR", "Oman", 0.2, ""],
            ["2222.SR", "Saudi Arabian Oil Company", "SAU", "SAR", "Saudi Arabia", 26.5, ""],
        ]

        out, failures = quarantine_critical_rows(HEADERS, rows)

        self.assertEqual(failures, [])
        self.assertEqual(out, rows)

    def test_blank_optional_venue_metadata_is_not_fabricated_or_auto_failed(self):
        rows = [["UNVERIFIED.AB", "Unverified PJSC", "", "", "", "", ""]]

        out, failures = quarantine_critical_rows(HEADERS, rows)

        self.assertEqual(failures, [])
        self.assertEqual(out, rows)

    def test_current_lifecycle_aliases_remain_canonical(self):
        self.assertEqual(canonicalize_symbol("BK"), "BNY.US")
        self.assertEqual(canonicalize_symbol("BK.US"), "BNY.US")
        self.assertEqual(canonicalize_symbol("NZYM-B.CO"), "NSIS-B.CO")


if __name__ == "__main__":
    unittest.main()
