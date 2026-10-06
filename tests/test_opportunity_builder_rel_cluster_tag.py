#!/usr/bin/env python3
"""
tests/test_opportunity_builder_rel_cluster_tag.py
================================================================================
RELIABILITY-CLUSTER TAG BASIS CONTRACT TESTS — v1.0.0 (builder v1.22.1 [B4d])
================================================================================
Emad Bahbah – Tadawul Fast Bridge

Guards core/analysis/opportunity_builder.py v1.22.1: the Reliability Cluster
gate's new TFB_T10_REL_CLUSTER_BASIS switch.

  * unarmed (TFB_T10_EXCLUDE_REL_CLUSTER unset)  -> no "Reliability Cluster"
    gate in the list, whatever the basis env says (S-1 window law).
  * basis=values (default) -> v1.10.3 behaviour byte-for-byte: a reliability
    on the cluster fails, off the cluster passes, None passes ("Unknown"),
    required text unchanged.
  * basis=tag   -> the row's Warnings tag head `confidence_default_suspected`
    fails the gate even when the reliability value is OFF the cluster; the
    current text discloses the witness; a clean Warnings blob passes; a bare
    substring inside another tag head does NOT match (substring-safety).
  * basis=both  -> either witness fails; a row with neither passes.
  * TFB_T10_CONF_DEFAULT_TOKENS csv override is honoured; blank => default.

stdlib unittest only, no network, no prints (house style).
"""

from __future__ import annotations

import os
import sys
import unittest
from typing import Any, Dict

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)
for _p in (_REPO_ROOT, _HERE):
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

_IMPORT_ERR = None
try:
    from core.analysis import opportunity_builder as ob  # type: ignore
except Exception as _e1:
    try:
        import opportunity_builder as ob  # type: ignore
        _e1 = None
    except Exception as _e2:
        ob = None  # type: ignore
        _IMPORT_ERR = (_e1, _e2)

_ENV_KEYS = ("TFB_T10_EXCLUDE_REL_CLUSTER", "TFB_T10_REL_CLUSTER_BASIS",
             "TFB_T10_REL_CLUSTER_VALUES", "TFB_T10_CONF_DEFAULT_TOKENS")

_TAG_ROW_WARNINGS = ("quote_exchange_from_suffix; yahoo_enrichment_applied; "
                     "confidence_default_suspected; rel_path:b=B:fc=66.4:"
                     "dq=100.0:pen=SC5:os=ri_based:fs=pt:raw=71.5:cf=none:fin=71.5")
_CLEAN_WARNINGS = "quote_exchange_from_suffix; yahoo_enrichment_applied"
# a tag whose HEAD merely CONTAINS the token text must not match
_SUBSTRING_WARNINGS = "x_confidence_default_suspected_probe:1; rel_path:b=B"


def _row(**kw: Any) -> Dict[str, Any]:
    base = {
        "symbol": "ITRN.US", "name": "Ituran", "sector": "Information Technology",
        "market": "NYSE/NASDAQ", "currency": "USD", "current_price": 51.69,
        "intrinsic_value": 72.35, "target_price": 74.5,
        "forecast_reliability_score": 71.5, "data_quality_score": 100.0,
        "risk_bucket": "Low", "provider_engine_conflict": "FALSE",
        "volatility_30d": 19.5, "avg_volume_30d": 55000,
        "expected_roi_12m": 34.7, "recommendation_detailed": "BUY",
        "investability_status": "INVESTABLE", "block_reason": "",
        "forecast_source": "provider_target", "warnings": _CLEAN_WARNINGS,
    }
    base.update(kw)
    return base


class TestRelClusterTagBasis(unittest.TestCase):

    def setUp(self) -> None:
        if ob is None:
            self.fail("opportunity_builder import failed: {!r}".format(_IMPORT_ERR))
        self._saved = {k: os.environ.get(k) for k in _ENV_KEYS}
        for k in _ENV_KEYS:
            os.environ.pop(k, None)
        self.crit = ob.make_criteria({"period_months": 3, "min_reliability": 70.0,
                                      "min_dq": 80.0})
        self.fx = {"USD": 3.7559, "SAR": 1.0}

    def tearDown(self) -> None:
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    # -- helpers --------------------------------------------------------------

    def _gate(self, row: Dict[str, Any]):
        cand = ob.normalize_candidate(row, self.fx, self.crit)
        gates = ob.evaluate_gates(cand, self.crit)
        hits = [g for g in gates if g.get("gate") == "Reliability Cluster"]
        return (hits[0] if hits else None), cand

    # -- unarmed --------------------------------------------------------------

    def test_unarmed_has_no_cluster_gate_in_any_basis(self) -> None:
        for basis in ("", "values", "tag", "both", "garbage"):
            if basis:
                os.environ["TFB_T10_REL_CLUSTER_BASIS"] = basis
            g, _ = self._gate(_row(warnings=_TAG_ROW_WARNINGS))
            self.assertIsNone(g, "basis=%r must not append the gate unarmed" % basis)

    def test_basis_default_and_fallback(self) -> None:
        self.assertEqual(ob._env_rel_cluster_basis(), "values")
        os.environ["TFB_T10_REL_CLUSTER_BASIS"] = " Both "
        self.assertEqual(ob._env_rel_cluster_basis(), "both")
        os.environ["TFB_T10_REL_CLUSTER_BASIS"] = "nonsense"
        self.assertEqual(ob._env_rel_cluster_basis(), "values")

    # -- values basis (v1.10.3 byte-for-byte) ---------------------------------

    def test_values_basis_is_v1_10_3(self) -> None:
        os.environ["TFB_T10_EXCLUDE_REL_CLUSTER"] = "1"
        g, _ = self._gate(_row(forecast_reliability_score=71.5, warnings=_CLEAN_WARNINGS))
        self.assertIsNotNone(g)
        self.assertFalse(g["passed"])
        self.assertEqual(g["current"], "71.5")
        self.assertEqual(g["required"],
                         "reliability off the default-confidence cluster "
                         "{70.4, 71.5, 75.4, 76.5} (blank/Unknown passes)")
        # off-cluster value passes even WITH the tag (values basis ignores it)
        g2, _ = self._gate(_row(forecast_reliability_score=74.3, warnings=_TAG_ROW_WARNINGS))
        self.assertTrue(g2["passed"])
        self.assertEqual(g2["current"], "74.3")
        # None reliability -> "Unknown", passes
        g3, _ = self._gate(_row(forecast_reliability_score=None, warnings=_TAG_ROW_WARNINGS))
        self.assertTrue(g3["passed"])
        self.assertEqual(g3["current"], "Unknown")

    # -- tag basis --------------------------------------------------------------

    def test_tag_basis_fails_on_witness_regardless_of_value(self) -> None:
        os.environ["TFB_T10_EXCLUDE_REL_CLUSTER"] = "1"
        os.environ["TFB_T10_REL_CLUSTER_BASIS"] = "tag"
        g, _ = self._gate(_row(forecast_reliability_score=74.3, warnings=_TAG_ROW_WARNINGS))
        self.assertFalse(g["passed"])
        self.assertEqual(g["current"], "74.3 [confidence_default_suspected]")
        self.assertIn("tag basis", g["required"])
        # cluster VALUE without the tag passes under tag basis
        g2, _ = self._gate(_row(forecast_reliability_score=76.5, warnings=_CLEAN_WARNINGS))
        self.assertTrue(g2["passed"])
        self.assertEqual(g2["current"], "76.5")
        # None reliability + tag -> fails, witness disclosed
        g3, _ = self._gate(_row(forecast_reliability_score=None, warnings=_TAG_ROW_WARNINGS))
        self.assertFalse(g3["passed"])
        self.assertEqual(g3["current"], "Unknown [confidence_default_suspected]")

    def test_tag_match_is_on_tag_heads_not_substrings(self) -> None:
        os.environ["TFB_T10_EXCLUDE_REL_CLUSTER"] = "1"
        os.environ["TFB_T10_REL_CLUSTER_BASIS"] = "tag"
        g, _ = self._gate(_row(forecast_reliability_score=74.3, warnings=_SUBSTRING_WARNINGS))
        self.assertTrue(g["passed"])
        g2, _ = self._gate(_row(forecast_reliability_score=74.3, warnings=""))
        self.assertTrue(g2["passed"])

    def test_conf_default_tokens_override(self) -> None:
        os.environ["TFB_T10_EXCLUDE_REL_CLUSTER"] = "1"
        os.environ["TFB_T10_REL_CLUSTER_BASIS"] = "tag"
        os.environ["TFB_T10_CONF_DEFAULT_TOKENS"] = "analyst_lkg; confidence_default_suspected"
        g, _ = self._gate(_row(forecast_reliability_score=74.3,
                               warnings="analyst_lkg:2h; yahoo_enrichment_applied"))
        self.assertFalse(g["passed"])
        self.assertEqual(g["current"], "74.3 [analyst_lkg]")
        os.environ["TFB_T10_CONF_DEFAULT_TOKENS"] = " ; "
        self.assertEqual(ob._env_conf_default_tokens(),
                         frozenset([ob._norm_token("confidence_default_suspected")]))

    # -- both basis ------------------------------------------------------------

    def test_both_basis_either_witness(self) -> None:
        os.environ["TFB_T10_EXCLUDE_REL_CLUSTER"] = "1"
        os.environ["TFB_T10_REL_CLUSTER_BASIS"] = "both"
        g_val, _ = self._gate(_row(forecast_reliability_score=76.5, warnings=_CLEAN_WARNINGS))
        self.assertFalse(g_val["passed"])
        self.assertEqual(g_val["current"], "76.5")
        g_tag, _ = self._gate(_row(forecast_reliability_score=74.3, warnings=_TAG_ROW_WARNINGS))
        self.assertFalse(g_tag["passed"])
        self.assertEqual(g_tag["current"], "74.3 [confidence_default_suspected]")
        g_none, _ = self._gate(_row(forecast_reliability_score=82.0, warnings=_CLEAN_WARNINGS))
        self.assertTrue(g_none["passed"])
        self.assertIn("both basis", g_none["required"])
        self.assertTrue(g_none["required"].startswith(
            "reliability off the default-confidence cluster {70.4, 71.5, 75.4, 76.5}"))

    def test_gate_order_untouched_and_version(self) -> None:
        self.assertIn("Reliability Cluster", ob.GATE_ORDER)
        self.assertEqual(ob.GATE_ORDER.count("Reliability Cluster"), 1)
        self.assertEqual(ob._DEFAULT_REL_CLUSTER_VALUES, (70.4, 71.5, 75.4, 76.5))
        self.assertEqual(ob.OPPORTUNITY_BUILDER_VERSION, "1.24.1")


if __name__ == "__main__":
    unittest.main(verbosity=0)
