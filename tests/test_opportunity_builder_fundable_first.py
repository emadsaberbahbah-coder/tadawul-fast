"""opportunity_builder v1.29.0 [FUNDABLE-FIRST] (owner decision C, 8 Nov 2026).

TFB_OPP_FUNDABLE_CCY = comma/space list of 3-letter currency codes. No valid
code -> off, byte-identical v1.28.0. Armed -> the INVEST pool keeps its score
order inside two groups (listed currencies first), a non-listed name still
fills seats when too few fundable names qualify, its capacity near-miss note
names the reason, and meta.fundable_first reports the re-sort.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.analysis import opportunity_builder as ob  # noqa: E402
from decision_evidence_fixtures import build_with_observed_inputs  # noqa: E402
from test_opportunity_builder import _row  # noqa: E402

ENV = "TFB_OPP_FUNDABLE_CCY"
FX = {"EUR": 4.05, "USD": 3.75}


def _rows():
    # Three EUR names score above three SAR names (higher intrinsic value).
    eur = [_row(symbol="E%d.PA" % i, sector="SecE%d" % i, currency="EUR",
                market="Euronext Paris", intrinsic_value=150.0 + i) for i in range(3)]
    sar = [_row(symbol="S%d.SR" % i, sector="SecS%d" % i,
                intrinsic_value=130.0 - i) for i in range(3)]
    return eur + sar


def _build(env, max_selected=4):
    old = os.environ.pop(ENV, None)
    try:
        if env is not None:
            os.environ[ENV] = env
        return build_with_observed_inputs(
            ob, _rows(), criteria={"max_selected": max_selected},
            portfolio={"cash_available_sar": 500000}, fx_rates=FX)
    finally:
        os.environ.pop(ENV, None)
        if old is not None:
            os.environ[ENV] = old


def _sel(p):
    return [t["symbol"] for t in p["selected"]]


def _cap_notes(p):
    return {n["symbol"]: n.get("improve_note") for n in p["near_miss"]
            if n.get("failed_gate") == "Capacity"}


class FundableFirst(unittest.TestCase):

    def test_version(self):
        self.assertGreaterEqual(
            tuple(map(int, ob.OPPORTUNITY_BUILDER_VERSION.split("."))), (1, 29, 0))

    def test_parse(self):
        os.environ.pop(ENV, None)
        self.assertIsNone(ob._env_fundable_ccys())
        for raw, want in (("", None), ("  ", None), ("EURO,12,$$$", None),
                          ("USD,SAR", ("SAR", "USD")), ("usd sar", ("SAR", "USD")),
                          ("USD;usd, SAR", ("SAR", "USD")), ("USD", ("USD",))):
            os.environ[ENV] = raw
            try:
                self.assertEqual(ob._env_fundable_ccys(), want, raw)
            finally:
                os.environ.pop(ENV, None)

    def test_off_is_score_order_and_no_new_key(self):
        p = _build(None)
        self.assertEqual(_sel(p), ["E2.PA", "E1.PA", "E0.PA", "S0.SR"])
        self.assertNotIn("fundable_first", p["meta"])
        for note in _cap_notes(p).values():
            self.assertNotIn("fundable first", note or "")

    def test_off_values_match_absent(self):
        base = _build(None)
        for env in ("", "junk", "EURO,12"):
            p = _build(env)
            self.assertEqual(_sel(p), _sel(base), env)
            self.assertEqual(_cap_notes(p), _cap_notes(base), env)
            self.assertEqual(sorted(p["meta"]), sorted(base["meta"]), env)

    def test_armed_puts_fundable_first_and_fills_with_the_best_other(self):
        p = _build("USD,SAR")
        self.assertEqual(_sel(p), ["S0.SR", "S1.SR", "S2.SR", "E2.PA"])
        self.assertEqual(p["meta"]["fundable_first"],
                         {"ccys": ["SAR", "USD"], "pool": 6, "fundable": 3,
                          "moved_down": 3})
        notes = _cap_notes(p)
        self.assertEqual(sorted(notes), ["E0.PA", "E1.PA"])
        for sym in ("E0.PA", "E1.PA"):
            self.assertTrue(notes[sym].endswith(
                "(fundable first: EUR after SAR/USD names)"), notes[sym])

    def test_armed_full_cap_of_fundable_names_keeps_others_out(self):
        p = _build("SAR", max_selected=3)
        self.assertEqual(_sel(p), ["S0.SR", "S1.SR", "S2.SR"])

    def test_armed_with_every_currency_listed_changes_nothing(self):
        self.assertEqual(_sel(_build("EUR,SAR")), _sel(_build(None)))


if __name__ == "__main__":
    unittest.main()
