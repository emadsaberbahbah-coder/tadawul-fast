from __future__ import annotations

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
