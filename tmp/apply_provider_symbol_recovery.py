#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    if new in text and old not in text:
        return
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one occurrence, found {count}: {old[:80]!r}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


def write_exact(path: str, content: str) -> None:
    target = Path(path)
    if target.exists() and target.read_text(encoding="utf-8") == content:
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")


# Provider-normalization source of truth.
replace_once(
    "core/symbols/normalize.py",
    "Symbol Normalization — v5.4.0 (ENTERPRISE ALIGNED + METADATA INFERENCE)",
    "Symbol Normalization — v5.4.1 (PROVIDER EXCHANGE-SUFFIX ALIGNMENT)",
)
replace_once(
    "core/symbols/normalize.py",
    "formatting helpers and robust handling of share-class tickers (e.g., BRK.B).\n\nv5.4.0",
    "formatting helpers and robust handling of share-class tickers (e.g., BRK.B).\n\n"
    "v5.4.1 (over v5.4.0) — Yahoo/EODHD suffix alignment:\n"
    "- ADD Abu Dhabi mapping .AB (Yahoo) <-> .ADX (EODHD).\n"
    "- ADD Philippine mapping .PS (Yahoo) <-> .PSE (EODHD).\n"
    "- WHY: the dashboard stores provider-facing Yahoo symbols, while EODHD requires\n"
    "  its own exchange IDs. Without these mappings, healthy listed instruments are\n"
    "  repeatedly returned as missing/error stubs and Keep-Last-Good masks the stale\n"
    "  rows instead of refreshing them.\n\n"
    "v5.4.0",
)
replace_once("core/symbols/normalize.py", '__version__ = "5.4.0"', '__version__ = "5.4.1"')
replace_once(
    "core/symbols/normalize.py",
    '    ".AE": "AE", ".DFM": "AE", ".ADX": "AE",',
    '    ".AE": "AE", ".AB": "AE", ".DFM": "AE", ".ADX": "AE",',
)
replace_once(
    "core/symbols/normalize.py",
    '    "KS": "KO",     # Korea KOSPI (Yahoo .KS  -> EODHD .KO)\n}',
    '    "KS": "KO",     # Korea KOSPI (Yahoo .KS  -> EODHD .KO)\n'
    '    "AB": "ADX",    # Abu Dhabi   (Yahoo .AB  -> EODHD .ADX)\n'
    '    "PS": "PSE",    # Philippines (Yahoo .PS  -> EODHD .PSE)\n}',
)
replace_once(
    "core/symbols/normalize.py",
    '    "KO": "KS",\n}',
    '    "KO": "KS",\n    "ADX": "AB",\n    "PSE": "PS",\n}',
)

# Lifecycle and exact-identity policy.
replace_once(
    "scripts/critical_symbol_identity.py",
    'POLICY_VERSION = "1.0.0"\nCRITICAL_IDENTITY_TAG = "identity_quarantined:critical_registry:v1.0.0"',
    'POLICY_VERSION = "1.1.0"\nCRITICAL_IDENTITY_TAG = "identity_quarantined:critical_registry:v1.1.0"',
)
replace_once(
    "scripts/critical_symbol_identity.py",
    "# BRK-B.US for Berkshire Class B. Fiserv moved from FI to FISV in November 2025.\n",
    "# BRK-B.US for Berkshire Class B. Fiserv moved from FI to FISV in November 2025.\n"
    "# Novozymes changed name/ticker to Novonesis / NSIS-B.CO.\n",
)
replace_once(
    "scripts/critical_symbol_identity.py",
    '    "FISV": "FISV.US",\n}',
    '    "FISV": "FISV.US",\n    "NZYM-B.CO": "NSIS-B.CO",\n}',
)
replace_once(
    "scripts/critical_symbol_identity.py",
    '    "FISV.US": IdentityRule(\n        accepted_name_tokens=("fiserv",),\n        exchange_tokens=("nasdaq",),\n    ),\n}',
    '    "FISV.US": IdentityRule(\n        accepted_name_tokens=("fiserv",),\n        exchange_tokens=("nasdaq",),\n    ),\n'
    '    "NSIS-B.CO": IdentityRule(\n        accepted_name_tokens=("novonesis", "novozymes"),\n'
    '        currency_tokens=("dkk",),\n        country_tokens=("denmark",),\n'
    '        exchange_tokens=("copenhagen", "nasdaq"),\n    ),\n}',
)

# Bounded-concurrency targeted provider variants.
replace_once("scripts/concurrent_batch_fetch.py", 'VERSION = "1.2.0"', 'VERSION = "1.3.0"')
variant_block = '''_METRICS: dict[str, dict[str, Any]] = {}

_RECOVERY_SUFFIX_VARIANTS: tuple[tuple[str, str], ...] = (
    (".AB", ".ADX"),
    (".PS", ".PSE"),
)


def provider_recovery_variants(symbol: str) -> list[str]:
    """Return a small, deterministic provider-safe variant set for recovery.

    The original canonical symbol is always first. Variants are used only after
    the normal fetch failed or returned a data-free stub; they never broaden the
    primary page request or bypass the final identity firewall.
    """
    canonical = str(symbol or "").strip().upper()
    variants = [canonical] if canonical else []
    for source_suffix, provider_suffix in _RECOVERY_SUFFIX_VARIANTS:
        if canonical.endswith(source_suffix):
            variants.append(canonical[: -len(source_suffix)] + provider_suffix)
    if canonical == "BK.US":
        variants.append("BK")
    result: list[str] = []
    seen: set[str] = set()
    for value in variants:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result'''
replace_once(
    "scripts/concurrent_batch_fetch.py",
    '_METRICS: dict[str, dict[str, Any]] = {}',
    variant_block,
)
provider_fan = '''        return list(await asyncio.gather(*tasks)) if tasks else []

    async def provider_variant_fan(
        backend: Any,
        indexed_batches: Sequence[tuple[int, Sequence[str]]],
        payload: dict[str, Any],
        endpoints: Sequence[str],
        request_id: str,
        *,
        phase: str,
        max_concurrency: int,
        delay_ms: int,
    ) -> list[dict[str, Any]]:
        """Fetch recovery aliases and map accepted rows to requested symbols."""
        semaphore = asyncio.Semaphore(max_concurrency)

        async def guarded(
            position: int,
            idx: int,
            originals: Sequence[str],
        ) -> dict[str, Any]:
            async with semaphore:
                alias_to_original: dict[str, str] = {}
                alias_rank: dict[str, int] = {}
                expanded: list[str] = []
                for original in originals:
                    canonical_original = sync.canonicalize_symbol(original)
                    for rank, alias in enumerate(provider_recovery_variants(canonical_original)):
                        alias_key = str(alias or "").strip().upper()
                        canonical_alias = sync.canonicalize_symbol(alias)
                        for key in (alias_key, canonical_alias):
                            if key and key not in alias_to_original:
                                alias_to_original[key] = canonical_original
                                alias_rank[key] = rank
                        if alias_key and alias_key not in expanded:
                            expanded.append(alias_key)

                stagger = (position % max_concurrency) * delay_ms
                outcome = await one(
                    backend,
                    idx,
                    expanded,
                    payload,
                    endpoints,
                    f"{request_id}-{phase}-{idx + 1}",
                    delay_ms=stagger,
                    require_good=False,
                )
                item = dict(outcome)
                item["b"] = list(originals)
                item["provider_variants"] = list(expanded)
                if not item.get("h"):
                    return item

                headers = list(item["h"])
                symbol_index = _column(
                    headers, "_GUARD_SYMBOL_ALIASES", ("symbol", "ticker")
                )
                selected: dict[str, tuple[int, list[Any]]] = {}
                for raw in item.get("r", []):
                    if (
                        not isinstance(raw, (list, tuple))
                        or symbol_index < 0
                        or symbol_index >= len(raw)
                        or sync._guard_is_blank(raw[symbol_index])
                    ):
                        continue
                    row = list(raw)
                    raw_key = str(row[symbol_index] or "").strip().upper()
                    canonical_key = sync.canonicalize_symbol(raw_key)
                    original = alias_to_original.get(raw_key) or alias_to_original.get(
                        canonical_key
                    )
                    if not original:
                        continue
                    row[symbol_index] = original
                    if not _row_good(headers, row):
                        continue
                    rank = alias_rank.get(raw_key, alias_rank.get(canonical_key, 999))
                    previous = selected.get(original)
                    if previous is None or rank < previous[0]:
                        selected[original] = (rank, row)
                item["r"] = [
                    selected[sync.canonicalize_symbol(original)][1]
                    for original in originals
                    if sync.canonicalize_symbol(original) in selected
                ]
                return item

        tasks = [
            asyncio.create_task(guarded(position, idx, batch))
            for position, (idx, batch) in enumerate(indexed_batches)
        ]
        return list(await asyncio.gather(*tasks)) if tasks else []

    def normalize_outcomes('''
replace_once(
    "scripts/concurrent_batch_fetch.py",
    '        return list(await asyncio.gather(*tasks)) if tasks else []\n\n    def normalize_outcomes(',
    provider_fan,
)
replace_once(
    "scripts/concurrent_batch_fetch.py",
    '                recovered = await fan(\n                    backend,\n                    indexed_recovery,\n                    payload,\n                    tuple(endpoint_chain),\n                    request_id,\n                    phase=f"target{recovery_round}",\n                    max_concurrency=max_concurrency,\n                    delay_ms=sync._batch_delay_ms(),\n                    require_good=True,\n                )',
    '                recovered = await provider_variant_fan(\n                    backend,\n                    indexed_recovery,\n                    payload,\n                    tuple(endpoint_chain),\n                    request_id,\n                    phase=f"target{recovery_round}",\n                    max_concurrency=max_concurrency,\n                    delay_ms=sync._batch_delay_ms(),\n                )',
)
replace_once(
    "scripts/concurrent_batch_fetch.py",
    '            symbols_missing_initial=len(initial_missing),\n            targeted_recovery_requested=len(recovery_requested),\n            targeted_recovery_healed=len(recovery_healed),',
    '            symbols_missing_initial=len(initial_missing),\n'
    '            data_free_symbols_initial=initial_data_free[:100],\n'
    '            missing_symbols_initial=initial_missing[:100],\n'
    '            targeted_recovery_requested=len(recovery_requested),\n'
    '            targeted_recovery_healed=len(recovery_healed),\n'
    '            targeted_recovery_healed_symbols=sorted(recovery_healed)[:100],',
)
replace_once(
    "scripts/concurrent_batch_fetch.py",
    '            symbols_failed=len(final_data_free),\n            symbols_unattempted=symbols_unattempted,',
    '            symbols_failed=len(final_data_free),\n'
    '            data_free_symbols=final_data_free[:100],\n'
    '            missing_symbols=final_missing[:100],\n'
    '            symbols_unattempted=symbols_unattempted,',
)

TEST = '''from __future__ import annotations

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
'''
write_exact("tests/test_provider_symbol_recovery.py", TEST)
print("provider symbol recovery applied")
