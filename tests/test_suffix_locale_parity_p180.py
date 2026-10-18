"""P-180 (data_engine_v2 v5.155.0 / eodhd_provider v4.18.4) - SUFFIX LOCALE
PARITY with the identity guard.

Live defect (Global_Markets exports 2026-10-17 and 2026-10-18): 26 of 27 .VN
rows were BLOCKED "Identity unverified - re-fetch required" although EODHD
priced every one of them. core/analysis/identity_guard.py expects VND for the
.VN venue, but neither the engine's _SUFFIX_TO_LOCALE nor the EODHD provider's
_GEO_BY_SUFFIX knew the suffix, so the fallback stamped NASDAQ/NYSE + USD and
the guard's currency-mismatch signature quarantined the row.

Properties asserted at HEAD:
  T1  .VN / .PH resolve in the engine table and both inferrers.
  T2  PARITY: every suffix the identity guard can judge (SUFFIX_CURRENCY) has an
      engine locale entry whose currency the guard accepts as consistent, so an
      engine-inferred currency can never trip the guard's signature 1.
  T3  The EODHD provider's suffix geo defaults agree with the engine for the
      suffixes added in v4.18.4 (no quote_exchange_missing /
      quote_currency_missing on those venues).
  T4  No previously mapped suffix changed (regression pins for a sample).
Run:  pytest -q tests/test_suffix_locale_parity_p180.py
"""
import importlib
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
de = importlib.import_module("core.data_engine_v2")
ig = importlib.import_module("core.analysis.identity_guard")
ep = importlib.import_module("core.providers.eodhd_provider")


def test_t1_vn_and_ph_resolve():
    assert de._suffix_locale_for("VHM.VN") == ("HOSE", "VND", "Vietnam")
    assert de._infer_exchange_from_symbol("VHM.VN") == "HOSE"
    assert de._infer_currency_from_symbol("VHM.VN") == "VND"
    assert de._suffix_locale_for("AC.PH")[1] == "PHP"
    assert de._infer_currency_from_symbol("AC.PH") == "PHP"


def test_t2_identity_guard_parity():
    """Every guard-judgeable suffix must have an engine locale whose currency
    the guard accepts (this is the invariant the .VN rows violated)."""
    # Known, pre-existing disagreement left untouched by P-180: the guard reads
    # .MA as Casablanca (MAD) while the EODHD provider's geo table reads it as
    # BME Spain (EUR). No .MA instrument exists in any live page; resolving it
    # needs a venue decision, not a parity patch.
    known_gaps = {"MA"}
    missing, conflicting = [], []
    for suffix, expected in ig.SUFFIX_CURRENCY.items():
        if suffix in known_gaps:
            continue
        locale = de._suffix_locale_for("TEST." + suffix)
        if locale is None:
            missing.append(suffix)
            continue
        ok = ig.currency_is_consistent("TEST." + suffix, locale[1])
        if ok is False:
            conflicting.append((suffix, locale[1], expected))
    assert not missing, "guard-judgeable suffixes without an engine locale: %r" % missing
    assert not conflicting, "engine currency the guard would quarantine: %r" % conflicting


def test_t3_eodhd_geo_defaults_agree_with_engine():
    for sym in ("VHM.VN", "AC.PH", "ASII.JK", "THYAO.IS", "BELA.AT", "PTT.BK",
                "MAYBANK.KL", "D05.SI", "PKO.WA"):
        geo = ep._suffix_derived_geo_defaults(sym)
        locale = de._suffix_locale_for(sym)
        assert geo["currency"] and geo["exchange"], sym
        assert locale is not None, sym
        assert geo["currency"] == locale[1], (sym, geo, locale)


def test_t4_existing_suffixes_unchanged():
    pins = {
        "AAPL.US": ("NASDAQ/NYSE", "USD", "USA"),
        "2222.SR": None,  # Tadawul handled by the numeric branch, not the table
        "7203.T": ("TSE", "JPY", "Japan"),
        "HSBA.L": ("LSE", "GBp", "United Kingdom"),
        "ASII.JK": ("IDX", "IDR", "Indonesia"),
        "OPAP.AT": ("ATHEX", "EUR", "Greece"),
    }
    for sym, want in pins.items():
        got = de._suffix_locale_for(sym)
        if want is None:
            assert got is None or got[1] == "SAR", (sym, got)
        else:
            assert got == want, (sym, got, want)
    assert ep._suffix_derived_geo_defaults("HSBA.L") == {
        "country": "United Kingdom", "currency": "GBX", "exchange": "LSE"}
    assert ep._suffix_derived_geo_defaults("AAPL.US")["currency"] == "USD"
