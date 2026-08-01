from __future__ import annotations

import types
import unittest
from unittest.mock import patch

from scripts import klg_identity_gate_v131 as gate


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
]


def _stub(symbol: str) -> list[object]:
    if symbol.endswith(".PS"):
        exchange, currency, country = "PSE", "PHP", "Philippines"
    elif symbol.endswith(".OM"):
        exchange, currency, country = "MSX", "OMR", "Oman"
    else:
        exchange, currency, country = "ADX", "AED", "United Arab Emirates"
    return [
        symbol,
        "",
        exchange,
        currency,
        country,
        "",
        "identity_quarantined",
        "Identity unverified — re-fetch required",
        "BLOCKED",
        "DO_NOT_INVEST",
        "unavailable",
    ]


class KLGIdentityGateV131Tests(unittest.TestCase):
    def setUp(self):
        gate._PATCHED_MODULE_IDS.clear()

    def _sync(self, restored_row: list[object], swapped_symbol: str):
        def original(_sheets, _sid, _sheet, _headers, _rows):
            return [list(restored_row)], [swapped_symbol]

        return types.SimpleNamespace(
            _keep_last_good_rows=original,
            canonicalize_symbol=lambda value: str(value or "").strip().upper(),
            _LAST_KLG_ID_SUSPECTS=[],
        )

    def test_candidate_classifier_rejects_wrong_issuer_and_invalid_saudi_shape(self):
        wrong_bpi = [
            "BPI.PS",
            "Equinix, Inc.",
            "PSE",
            "PHP",
            "Philippines",
            1034.86,
            "",
            "",
            "WATCHLIST",
            "WATCH",
            "eodhd",
        ]
        invalid_saudi = [
            "ELET3.SR",
            "TotalEnergies SE",
            "Tadawul",
            "SAR",
            "Saudi Arabia",
            76.0,
            "",
            "",
            "WATCHLIST",
            "WATCH",
            "eodhd",
        ]

        self.assertEqual(
            gate.candidate_identity_failure(HEADERS, wrong_bpi),
            "issuer name mismatch",
        )
        self.assertEqual(
            gate.candidate_identity_failure(HEADERS, invalid_saudi),
            "invalid Saudi symbol format",
        )

    def test_poisoned_philippine_predecessor_cannot_ride_back_in(self):
        incoming = _stub("BPI.PS")
        poisoned = [
            "BPI.PS",
            "Equinix, Inc.",
            "PSE",
            "PHP",
            "Philippines",
            1034.86,
            "",
            "",
            "WATCHLIST",
            "WATCH",
            "eodhd",
        ]
        sync = self._sync(poisoned, "BPI.PS")
        self.assertTrue(gate._patch_sync_module(sync))

        rows, swapped = sync._keep_last_good_rows(
            None, "sid", "Market_Leaders", HEADERS, [incoming]
        )

        self.assertEqual(rows, [incoming])
        self.assertEqual(swapped, [])
        self.assertEqual(sync._LAST_KLG_ID_SUSPECTS, ["BPI.PS"])

    def test_poisoned_abu_dhabi_predecessor_cannot_ride_back_in(self):
        incoming = _stub("ALDAR.AB")
        poisoned = [
            "ALDAR.AB",
            "Banco Santander (Brasil) S.A.",
            "ADX",
            "AED",
            "United Arab Emirates",
            28.65,
            "",
            "",
            "WATCHLIST",
            "WATCH",
            "eodhd",
        ]
        sync = self._sync(poisoned, "ALDAR.AB")
        gate._patch_sync_module(sync)

        rows, swapped = sync._keep_last_good_rows(
            None, "sid", "Market_Leaders", HEADERS, [incoming]
        )

        self.assertEqual(rows, [incoming])
        self.assertEqual(swapped, [])
        self.assertIn("ALDAR.AB", sync._LAST_KLG_ID_SUSPECTS)

    def test_wrong_venue_predecessor_cannot_ride_back_in(self):
        incoming = _stub("BPI.PS")
        wrong_venue = [
            "BPI.PS",
            "Bank of the Philippine Islands",
            "NASDAQ/NYSE",
            "USD",
            "USA",
            120.0,
            "",
            "",
            "WATCHLIST",
            "WATCH",
            "eodhd",
        ]
        sync = self._sync(wrong_venue, "BPI.PS")
        gate._patch_sync_module(sync)

        rows, swapped = sync._keep_last_good_rows(
            None, "sid", "Market_Leaders", HEADERS, [incoming]
        )

        self.assertEqual(rows, [incoming])
        self.assertEqual(swapped, [])
        self.assertEqual(sync._LAST_KLG_ID_SUSPECTS, ["BPI.PS"])

    def test_verified_predecessor_is_still_restored(self):
        incoming = _stub("BPI.PS")
        verified = [
            "BPI.PS",
            "Bank of the Philippine Islands",
            "PSE",
            "PHP",
            "Philippines",
            120.0,
            "",
            "",
            "INVESTABLE",
            "INVEST",
            "eodhd",
        ]
        sync = self._sync(verified, "BPI.PS")
        gate._patch_sync_module(sync)

        rows, swapped = sync._keep_last_good_rows(
            None, "sid", "Market_Leaders", HEADERS, [incoming]
        )

        self.assertEqual(rows, [verified])
        self.assertEqual(swapped, ["BPI.PS"])
        self.assertEqual(sync._LAST_KLG_ID_SUSPECTS, [])

    def test_classifier_exception_fails_closed_to_incoming_stub(self):
        incoming = _stub("BPI.PS")
        restored = [
            "BPI.PS",
            "Bank of the Philippine Islands",
            "PSE",
            "PHP",
            "Philippines",
            120.0,
            "",
            "",
            "INVESTABLE",
            "INVEST",
            "eodhd",
        ]
        sync = self._sync(restored, "BPI.PS")
        gate._patch_sync_module(sync)

        with patch.object(
            gate,
            "candidate_identity_failure",
            side_effect=RuntimeError("classifier unavailable"),
        ):
            rows, swapped = sync._keep_last_good_rows(
                None, "sid", "Market_Leaders", HEADERS, [incoming]
            )

        self.assertEqual(rows, [incoming])
        self.assertEqual(swapped, [])
        self.assertEqual(sync._LAST_KLG_ID_SUSPECTS, ["BPI.PS"])

    def test_patch_is_idempotent(self):
        sync = self._sync(
            [
                "BPI.PS",
                "Bank of the Philippine Islands",
                "PSE",
                "PHP",
                "Philippines",
                120.0,
                "",
                "",
                "INVESTABLE",
                "INVEST",
                "eodhd",
            ],
            "BPI.PS",
        )
        self.assertTrue(gate._patch_sync_module(sync))
        first = sync._keep_last_good_rows
        self.assertTrue(gate._patch_sync_module(sync))
        self.assertIs(sync._keep_last_good_rows, first)


if __name__ == "__main__":
    unittest.main()
