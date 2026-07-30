#!/usr/bin/env python3
"""One-shot source transformer for the post-deployment capability-gate findings.

The transformer is intentionally narrow and assertion-heavy.  It updates only the
provider-symbol lifecycle rules, the EODHD error-patch binding default, the
read-only capability probe, and the route's degraded-data contract.  It does not
touch scoring, ranking formulas, portfolio arithmetic, or Sheet writers.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    (ROOT / path).write_text(text, encoding="utf-8")


def replace_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


def replace_at_least_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count < 1:
        raise RuntimeError(f"{label}: expected at least one match")
    return text.replace(old, new)


def regex_once(text: str, pattern: str, replacement: str, *, label: str) -> str:
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.S | re.M)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one regex match, found {count}")
    return updated


def patch_normalize() -> None:
    path = "core/symbols/normalize.py"
    text = read(path)
    text = replace_at_least_once(text, "v5.4.1", "v5.4.2", label="normalize version")
    text = replace_once(
        text,
        "- ADD Abu Dhabi mapping .AB (Yahoo) <-> .ADX (EODHD).",
        "- FIX Abu Dhabi mapping: .AD (Yahoo) <-> .ADX (EODHD); .AB remains a legacy input alias only.",
        label="normalize Abu Dhabi changelog",
    )
    text = replace_once(
        text,
        '    ".AE": "AE", ".AB": "AE", ".DFM": "AE", ".ADX": "AE",',
        '    ".AE": "AE", ".AD": "AE", ".AB": "AE", ".DFM": "AE", ".ADX": "AE",',
        label="normalize UAE suffix registry",
    )
    text = replace_once(
        text,
        '    "AB": "ADX",    # Abu Dhabi   (Yahoo .AB  -> EODHD .ADX)\n',
        '    "AD": "ADX",    # Abu Dhabi   (Yahoo .AD  -> EODHD .ADX)\n'
        '    "AB": "ADX",    # Legacy project alias -> EODHD .ADX\n',
        label="normalize Yahoo-to-EODHD ADX",
    )
    text = replace_once(
        text,
        '    "ADX": "AB",\n',
        '    "ADX": "AD",\n',
        label="normalize EODHD-to-Yahoo ADX",
    )
    write(path, text)


def patch_critical_identity() -> None:
    path = "scripts/critical_symbol_identity.py"
    text = read(path)
    text = replace_once(text, 'POLICY_VERSION = "1.1.0"', 'POLICY_VERSION = "1.2.0"', label="identity policy version")
    text = replace_once(
        text,
        'CRITICAL_IDENTITY_TAG = "identity_quarantined:critical_registry:v1.1.0"',
        'CRITICAL_IDENTITY_TAG = "identity_quarantined:critical_registry:v1.2.0"',
        label="identity policy tag",
    )
    text = replace_once(
        text,
        "# Provider-safe canonical identifiers. EODHD uses the .US exchange suffix and\n# BRK-B.US for Berkshire Class B. Fiserv moved from FI to FISV in November 2025.\n# Novozymes changed name/ticker to Novonesis / NSIS-B.CO.",
        "# Provider-safe canonical identifiers. BNY changed its common-stock ticker\n# from BK to BNY effective 2026-05-21; stale BK spellings are lifecycle aliases,\n# not active provider identities. EODHD uses the .US exchange suffix and BRK-B.US\n# for Berkshire Class B. Fiserv moved from FI to FISV in November 2025. Novozymes\n# changed name/ticker to Novonesis / NSIS-B.CO.",
        label="identity policy lifecycle comment",
    )
    text = replace_once(
        text,
        '    "BK": "BK.US",\n',
        '    "BK": "BNY.US",\n    "BK.US": "BNY.US",\n    "BNY": "BNY.US",\n',
        label="BK to BNY lifecycle aliases",
    )
    text = replace_once(text, '    "BK.US": IdentityRule(\n', '    "BNY.US": IdentityRule(\n', label="BNY critical identity key")
    write(path, text)


def patch_concurrent_fetch() -> None:
    path = "scripts/concurrent_batch_fetch.py"
    text = read(path)
    text = replace_once(text, 'VERSION = "1.3.0"', 'VERSION = "1.3.1"', label="concurrent fetch version")
    text = replace_once(
        text,
        '_RECOVERY_SUFFIX_VARIANTS: tuple[tuple[str, str], ...] = (\n    (".AB", ".ADX"),\n    (".PS", ".PSE"),\n)',
        '_RECOVERY_SUFFIX_VARIANTS: tuple[tuple[str, str], ...] = (\n'
        '    (".AD", ".ADX"),\n'
        '    (".AB", ".ADX"),  # legacy project spelling\n'
        '    (".PS", ".PSE"),\n'
        ')',
        label="provider recovery suffixes",
    )
    text = replace_once(
        text,
        '    if canonical == "BK.US":\n        variants.append("BK")\n',
        '    if canonical == "BNY.US":\n'
        '        # Current ticker first; stale BK spellings are last-resort lifecycle aliases.\n'
        '        variants.extend(["BNY", "BK.US", "BK"])\n',
        label="BNY recovery variants",
    )
    write(path, text)


def patch_capability_gate() -> None:
    path = "scripts/verify_backend_symbol_capabilities.py"
    text = read(path)
    text = replace_once(text, 'GATE_VERSION = "1.0.2"', 'GATE_VERSION = "1.1.0"', label="gate version")
    text = replace_once(text, 'requested_symbol="ADNOCDIST.AB",', 'requested_symbol="ADNOCDIST.AD",', label="ADNOC probe symbol")
    text = replace_once(
        text,
        'accepted_symbols=("ADNOCDIST.AB", "ADNOCDIST.ADX"),',
        'accepted_symbols=("ADNOCDIST.AD", "ADNOCDIST.ADX"),',
        label="ADNOC accepted symbols",
    )
    text = replace_once(text, 'capability="yahoo_ab_to_eodhd_adx",', 'capability="yahoo_ad_to_eodhd_adx",', label="ADNOC capability name")
    text = replace_once(text, 'requested_symbol="BK.US",', 'requested_symbol="BNY.US",', label="BNY probe symbol")
    text = replace_once(text, 'accepted_symbols=("BK.US", "BK"),', 'accepted_symbols=("BNY.US", "BNY"),', label="BNY accepted symbols")
    text = replace_once(text, 'capability="bk_exact_identity",', 'capability="bny_exact_identity",', label="BNY capability name")
    write(path, text)


def patch_eodhd_provider() -> None:
    path = "core/providers/eodhd_provider.py"
    text = read(path)
    text = replace_at_least_once(text, "4.15.1", "4.15.2", label="EODHD provider version")
    text = replace_once(
        text,
        '    return _env_str("TFB_EODHD_ENGINE_PATCH_BIND", "0").strip().lower() in _TRUTHY',
        '    return _env_str("TFB_EODHD_ENGINE_PATCH_BIND", "1").strip().lower() in _TRUTHY',
        label="EODHD patch-bind safe default",
    )
    text = replace_once(
        text,
        '    _pick_provider_callable preference order receives them. OFF =\n    legacy raise-on-err delegation (engine sees {} on failure), byte-\n    identical to v4.12.1.',
        '    _pick_provider_callable preference order receives them. Default ON so\n    HTTP 402/auth error patches reach the engine health registry. Set the flag\n    to 0 only as an explicit rollback to legacy raise-on-error behavior.',
        label="EODHD patch-bind doc",
    )
    write(path, text)


def patch_advanced_analysis() -> None:
    path = "routes/advanced_analysis.py"
    text = read(path)
    text = replace_once(
        text,
        'ADVANCED_ANALYSIS_VERSION = "4.14.1"',
        'ADVANCED_ANALYSIS_VERSION = "4.14.2"',
        label="advanced analysis version",
    )

    truthful_value_function = '''def _placeholder_value_for_key(page: str, key: str, symbol: str, row_index: int) -> Any:
    """Return an explicit unavailable stub value without fabricating market facts.

    The symbol is preserved so callers can retry it.  All prices, scores, ranks,
    names, forecasts and recommendations remain unknown (None).  Only provenance,
    timestamps and an explicit unavailability marker are populated.
    """
    kk = _normalize_key_name(key)
    if kk in {"symbol", "ticker"}:
        return symbol
    if kk == "data_provider":
        return "advanced_analysis.unavailable_stub"
    if kk in {"last_updated_utc", "last_updated_riyadh"}:
        return datetime.now(timezone.utc).isoformat()
    if kk in {"warnings", "notes", "block_reason"}:
        return "upstream_unavailable"
    if kk in {"recommendation_reason", "selection_reason"}:
        return "No usable provider data; decision fields are unavailable."
    if kk == "criteria_snapshot":
        return json.dumps({"symbol": symbol, "source": "unavailable_stub"}, ensure_ascii=False)
    return None
'''
    text = regex_once(
        text,
        r"def _placeholder_value_for_key\(.*?\n(?=def _build_placeholder_rows)",
        truthful_value_function + "\n",
        label="truthful placeholder values",
    )

    truthful_rows_function = '''def _build_placeholder_rows(*, page: str, keys: Sequence[str], requested_symbols: Sequence[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    """Build retryable symbol stubs only; never manufacture investment data."""
    if page == _TOP10_PAGE:
        return []
    symbols = [_normalize_symbol_token(x) for x in requested_symbols if _normalize_symbol_token(x)]
    if not symbols:
        return []
    symbols = symbols[offset : offset + limit] if (offset or len(symbols) > limit) else symbols[:limit]
    return [
        {str(k): _placeholder_value_for_key(page, str(k), sym, idx) for k in keys}
        for idx, sym in enumerate(symbols, start=offset + 1)
    ]
'''
    text = regex_once(
        text,
        r"def _build_placeholder_rows\(.*?\n(?=def _real_data_dictionary_rows)",
        truthful_rows_function + "\n",
        label="truthful placeholder rows",
    )

    truthful_insights = '''def _build_insights_fallback_rows(*, requested_symbols: Sequence[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    """Expose coverage status only; never emit synthetic Accumulate/Watch signals."""
    symbols = [_normalize_symbol_token(x) for x in requested_symbols if _normalize_symbol_token(x)]
    src = "advanced_analysis.unavailable_insights_stub"
    rows: List[Dict[str, Any]] = [
        {"section": "Coverage", "item": "Requested symbols", "metric": "count", "value": len(symbols), "notes": "Upstream insights unavailable", "source": src, "sort_order": 1},
        {"section": "Status", "item": "Insight generation", "metric": "availability", "value": "Unknown", "notes": "No usable upstream rows; no recommendation generated", "source": src, "sort_order": 2},
    ]
    return _slice(rows, limit=limit, offset=offset)
'''
    text = regex_once(
        text,
        r"def _build_insights_fallback_rows\(.*?\n(?=def _build_nonempty_failsoft_rows)",
        truthful_insights + "\n",
        label="truthful insights fallback",
    )

    truthful_dispatch = '''def _build_nonempty_failsoft_rows(*, page: str, headers: Sequence[str], keys: Sequence[str], requested_symbols: Sequence[str], limit: int, offset: int, top_n: int) -> List[Dict[str, Any]]:
    if page == _DICTIONARY_PAGE:
        return _build_dictionary_fallback_rows(page=page, headers=headers, keys=keys, limit=limit, offset=offset)
    if page == _INSIGHTS_PAGE:
        return _build_insights_fallback_rows(requested_symbols=requested_symbols, limit=limit, offset=offset)
    if page == _TOP10_PAGE:
        # A ranked investment list requires verified facts.  Empty is honest;
        # synthetic ranks/prices/recommendations are not.
        return []
    return _build_placeholder_rows(page=page, keys=keys, requested_symbols=requested_symbols, limit=limit, offset=offset)
'''
    text = regex_once(
        text,
        r"def _build_nonempty_failsoft_rows\(.*?\n(?=def _payload_envelope)",
        truthful_dispatch + "\n",
        label="truthful failsoft dispatch",
    )
    write(path, text)


def patch_tests() -> None:
    path = "tests/test_provider_symbol_recovery.py"
    text = read(path)
    text = text.replace("ADNOCDIST.AB", "ADNOCDIST.AD")
    text = replace_once(
        text,
        '            elif symbol == "BK":\n                rows.append([symbol, "The Bank of New York Mellon Corporation", 100.0, "mock"])',
        '            elif symbol in {"BNY", "BK"}:\n                rows.append([symbol, "The Bank of New York Mellon Corporation", 100.0, "mock"])',
        label="variant backend BNY",
    )
    text = replace_once(
        text,
        '        self.assertEqual(provider_recovery_variants("BK.US"), ["BK.US", "BK"])',
        '        self.assertEqual(provider_recovery_variants("BNY.US"), ["BNY.US", "BNY", "BK.US", "BK"])',
        label="BNY variant order test",
    )
    text = replace_once(
        text,
        '        aliases = {"NZYM-B.CO": "NSIS-B.CO", "BK": "BK.US"}',
        '        aliases = {"NZYM-B.CO": "NSIS-B.CO", "BK": "BNY.US", "BK.US": "BNY.US", "BNY": "BNY.US"}',
        label="variant test aliases",
    )
    text = replace_once(
        text,
        '        requested = ["ADNOCDIST.AD", "BPI.PS", "BK.US", "NZYM-B.CO"]',
        '        requested = ["ADNOCDIST.AD", "BPI.PS", "BK.US", "NZYM-B.CO"]',
        label="variant requested list guard",
    )
    text = replace_once(
        text,
        '        self.assertEqual([row[0] for row in rows], ["ADNOCDIST.AD", "BPI.PS", "BK.US", "NSIS-B.CO"])',
        '        self.assertEqual([row[0] for row in rows], ["ADNOCDIST.AD", "BPI.PS", "BNY.US", "NSIS-B.CO"])',
        label="variant expected canonical BNY",
    )
    write(path, text)

    path = "tests/test_backend_symbol_capabilities.py"
    text = read(path)
    text = text.replace("yahoo_ab_to_eodhd_adx", "yahoo_ad_to_eodhd_adx")
    text = text.replace("ADNOCDIST.AB", "ADNOCDIST.AD")
    text = text.replace("bk_exact_identity", "bny_exact_identity")
    text = text.replace('[["BK.US",', '[["BNY.US",')
    write(path, text)

    path = "tests/test_critical_symbol_identity.py"
    text = read(path)
    text = text.replace('"BK.US"', '"BNY.US"')
    text = replace_once(
        text,
        '        self.assertIn(("BK", "canonicalized", "BNY.US"), actions)\n',
        '        self.assertIn(("BK", "canonicalized", "BNY.US"), actions)\n'
        '        self.assertIn(("BNY.US", "deduplicated", "BNY.US"), actions)\n',
        label="critical lifecycle assertions",
    )
    insert_after = '''    def test_universe_removes_retired_and_canonicalizes_collisions(self):
'''
    if insert_after not in text:
        raise RuntimeError("critical identity test insertion anchor missing")
    lifecycle_test = '''    def test_stale_bk_us_lifecycle_alias_maps_to_bny(self):
        clean, changes = sanitize_active_universe(["BK.US"])
        self.assertEqual(clean, ["BNY.US"])
        self.assertEqual(changes[0].source_symbol, "BK.US")
        self.assertEqual(changes[0].target_symbol, "BNY.US")

'''
    anchor = "\n    def test_critical_symbols_get_single_symbol_batches_first(self):"
    text = replace_once(text, anchor, "\n" + lifecycle_test + "    def test_critical_symbols_get_single_symbol_batches_first(self):", label="insert stale BK lifecycle test")
    write(path, text)


def add_truthful_contract_test() -> None:
    path = ROOT / "tests/test_truthful_failsoft_contract.py"
    content = '''from __future__ import annotations

import ast
from pathlib import Path
import unittest


SOURCE_PATH = Path("routes/advanced_analysis.py")
SOURCE = SOURCE_PATH.read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)
FUNCTIONS = {
    node.name: node
    for node in TREE.body
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
}


def function_source(name: str) -> str:
    node = FUNCTIONS[name]
    return ast.get_source_segment(SOURCE, node) or ""


class TruthfulFailsoftContractTests(unittest.TestCase):
    def test_unavailable_stub_does_not_invent_prices_scores_or_actions(self):
        block = function_source("_placeholder_value_for_key")
        self.assertIn("advanced_analysis.unavailable_stub", block)
        self.assertNotIn("Accumulate", block)
        self.assertNotIn("Watch", block)
        self.assertNotIn("100.0 +", block)
        self.assertNotIn("forecast_price", block)

    def test_top10_failsoft_is_empty(self):
        block = function_source("_build_nonempty_failsoft_rows")
        self.assertIn("if page == _TOP10_PAGE", block)
        self.assertIn("return []", block)

    def test_insights_failsoft_has_no_synthetic_recommendation(self):
        block = function_source("_build_insights_fallback_rows")
        self.assertNotIn("Accumulate", block)
        self.assertNotIn("Watch", block)
        self.assertIn('"Unknown"', block)

    def test_eodhd_patch_binding_defaults_on(self):
        provider = Path("core/providers/eodhd_provider.py").read_text(encoding="utf-8")
        self.assertIn('TFB_EODHD_ENGINE_PATCH_BIND", "1"', provider)


if __name__ == "__main__":
    unittest.main()
'''
    path.write_text(content, encoding="utf-8")


def patch_workflow() -> None:
    path = ".github/workflows/python_batch_concurrency.yml"
    text = read(path)
    text = replace_once(
        text,
        "      - 'core/providers/eodhd_provider.py'\n",
        "      - 'core/providers/eodhd_provider.py'\n      - 'routes/advanced_analysis.py'\n",
        label="workflow PR route path",
    )
    second = "      - 'core/providers/eodhd_provider.py'\n"
    index = text.find(second, text.find(second) + 1)
    if index < 0:
        raise RuntimeError("workflow push route path anchor missing")
    text = text[: index + len(second)] + "      - 'routes/advanced_analysis.py'\n" + text[index + len(second) :]
    text = replace_once(
        text,
        "      - 'tests/test_eodhd_http402_guard.py'\n      - '.github/workflows/python_batch_concurrency.yml'",
        "      - 'tests/test_eodhd_http402_guard.py'\n      - 'tests/test_truthful_failsoft_contract.py'\n      - '.github/workflows/python_batch_concurrency.yml'",
        label="workflow PR truthful test path",
    )
    # Add the push path separately (same sequence appears twice).
    marker = "      - 'tests/test_eodhd_http402_guard.py'\n      - '.github/workflows/python_batch_concurrency.yml'"
    text = replace_once(
        text,
        marker,
        "      - 'tests/test_eodhd_http402_guard.py'\n      - 'tests/test_truthful_failsoft_contract.py'\n      - '.github/workflows/python_batch_concurrency.yml'",
        label="workflow push truthful test path",
    )
    text = replace_once(
        text,
        "            core/providers/eodhd_provider.py \\\n",
        "            core/providers/eodhd_provider.py \\\n            routes/advanced_analysis.py \\\n",
        label="workflow compile route",
    )
    text = replace_once(
        text,
        "            tests/test_eodhd_http402_guard.py\n",
        "            tests/test_eodhd_http402_guard.py \\\n            tests/test_truthful_failsoft_contract.py\n",
        label="workflow compile truthful test",
    )
    text = replace_once(
        text,
        "          python -m pytest -q tests/test_eodhd_http402_guard.py\n",
        "          python -m pytest -q tests/test_eodhd_http402_guard.py tests/test_truthful_failsoft_contract.py\n",
        label="workflow run truthful test",
    )
    text = text.replace('to_eodhd_symbol("ADNOCDIST.AB") == "ADNOCDIST.ADX"', 'to_eodhd_symbol("ADNOCDIST.AD") == "ADNOCDIST.ADX"')
    text = text.replace('to_yahoo_symbol("ADNOCDIST.ADX") == "ADNOCDIST.AB"', 'to_yahoo_symbol("ADNOCDIST.ADX") == "ADNOCDIST.AD"')
    text = text.replace('provider_recovery_variants("BK.US") == ["BK.US", "BK"]', 'provider_recovery_variants("BNY.US") == ["BNY.US", "BNY", "BK.US", "BK"]')
    text = text.replace('rule.requested_symbol == "BK.US"', 'rule.requested_symbol == "BNY.US"')
    text = text.replace('[["BK.US", "The Bank of New York Mellon Corporation"', '[["BNY.US", "The Bank of New York Mellon Corporation"')
    text = replace_once(
        text,
        '          assert eodhd_provider._err_indicates_provider_unhealthy("HTTP 402") is True\n',
        '          assert eodhd_provider._err_indicates_provider_unhealthy("HTTP 402") is True\n'
        '          os.environ.pop("TFB_EODHD_ENGINE_PATCH_BIND", None)\n'
        '          assert eodhd_provider._engine_patch_bind_enabled() is True\n',
        label="workflow EODHD patch-bind assertion",
    )
    text = replace_once(
        text,
        '          assert canonicalize_symbol("NZYM-B.CO") == "NSIS-B.CO"\n',
        '          assert canonicalize_symbol("NZYM-B.CO") == "NSIS-B.CO"\n'
        '          assert canonicalize_symbol("BK.US") == "BNY.US"\n',
        label="workflow BNY lifecycle assertion",
    )
    write(path, text)


def main() -> None:
    patch_normalize()
    patch_critical_identity()
    patch_concurrent_fetch()
    patch_capability_gate()
    patch_eodhd_provider()
    patch_advanced_analysis()
    patch_tests()
    add_truthful_contract_test()
    patch_workflow()
    print("Deployment-gate truth fix applied successfully.")


if __name__ == "__main__":
    main()
