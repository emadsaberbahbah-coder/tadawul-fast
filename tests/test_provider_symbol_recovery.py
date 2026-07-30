from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from core.symbols.normalize import get_country_from_symbol, to_eodhd_symbol, to_yahoo_symbol
from scripts.concurrent_batch_fetch import build, provider_recovery_variants
from scripts.critical_symbol_identity import canonicalize_symbol
from tests.test_concurrent_batch_fetch import Result, fake


class VariantBackend:
    async def post_json(self, endpoint, payload):
        rows = []
        for symbol in payload["symbols"]:
            if symbol.endswith(".ADX") or symbol.endswith(".PSE"):
                rows.append([symbol, symbol.lower(), 100.0, "mock"])
            elif symbol == "BK":
                rows.append([symbol, "The Bank of New York Mellon Corporation", 100.0, "mock"])
            elif symbol == "NSIS-B.CO":
                rows.append([symbol, "Novonesis A/S", 100.0, "mock"])
        return {"headers": ["Symbol", "Name", "Current Price", "Data Provider"], "rows": rows}, None, 200


class ProviderSymbolNormalizationTests(unittest.TestCase):
    def test_yahoo_to_eodhd_exchange_suffixes(self):
        self.assertEqual(to_eodhd_symbol("ADNOCDIST.AB"), "ADNOCDIST.ADX")
        self.assertEqual(to_eodhd_symbol("BPI.PS"), "BPI.PSE")

    def test_eodhd_to_yahoo_exchange_suffixes(self):
        self.assertEqual(to_yahoo_symbol("ADNOCDIST.ADX"), "ADNOCDIST.AB")
        self.assertEqual(to_yahoo_symbol("BPI.PSE"), "BPI.PS")

    def test_ab_metadata_is_uae(self):
        self.assertEqual(get_country_from_symbol("BOROUGE.AB"), "United Arab Emirates")

    def test_retired_novozymes_symbol_is_canonicalized(self):
        self.assertEqual(canonicalize_symbol("NZYM-B.CO"), "NSIS-B.CO")

    def test_recovery_variant_order(self):
        self.assertEqual(provider_recovery_variants("ADNOCDIST.AB"), ["ADNOCDIST.AB", "ADNOCDIST.ADX"])
        self.assertEqual(provider_recovery_variants("BPI.PS"), ["BPI.PS", "BPI.PSE"])
        self.assertEqual(provider_recovery_variants("BK.US"), ["BK.US", "BK"])


class ProviderVariantRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_targeted_recovery_maps_provider_aliases_back(self):
        sync = fake(size=2)
        aliases = {"NZYM-B.CO": "NSIS-B.CO", "BK": "BK.US"}
        sync.canonicalize_symbol = lambda value: aliases.get(str(value).strip().upper(), str(value).strip().upper())
        fn = build(sync)
        result = Result("provider-variants")
        requested = ["ADNOCDIST.AB", "BPI.PS", "BK.US", "NZYM-B.CO"]
        with patch.dict(os.environ, {
            "TFB_SYNC_BATCH_CONCURRENCY": "3",
            "TFB_SYNC_BATCH_OUTER_RETRIES": "0",
            "TFB_SYNC_TARGET_RECOVERY": "1",
            "TFB_SYNC_TARGET_RECOVERY_ROUNDS": "1",
            "TFB_SYNC_TARGET_RECOVERY_BATCH_SIZE": "2",
        }):
            _, rows, _, _ = await fn(VariantBackend(), SimpleNamespace(sheet_name="Market_Leaders"), requested, {}, "analysis", result)
        self.assertEqual([row[0] for row in rows], ["ADNOCDIST.AB", "BPI.PS", "BK.US", "NSIS-B.CO"])
        self.assertEqual(result.batch_metrics["symbols_fresh"], 4)
        self.assertEqual(result.batch_metrics["symbols_missing"], 0)
        self.assertEqual(result.batch_metrics["targeted_recovery_healed"], 4)


if __name__ == "__main__":
    unittest.main()
