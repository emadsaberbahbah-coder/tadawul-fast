"""Offline FX contracts for the optional USX (USD cents) quote lane.

WHY: peg checking a rate after /100 inflated a cents quote 100 times.
Exercise both parent-rate and exact Sheet subunit inputs through the real
resolver, and preserve the default-off operator-provided FX-row behavior.
"""
import pytest

from core.analysis import opportunity_builder as ob

GATE = "TFB_OPP_FX_USX_SUBUNIT"


@pytest.fixture(autouse=True)
def isolated_fx_flags(monkeypatch):
    monkeypatch.delenv(GATE, raising=False)
    monkeypatch.delenv("TFB_OPP_FX_SANITY", raising=False)
    monkeypatch.setattr(ob, "_FX_PEG_REJECTS", {})


@pytest.mark.parametrize("setting", [None, "", "0", "false", "disabled"])
def test_default_off_needs_explicit_usx_rate_not_usd_fallback(monkeypatch, setting):
    if setting is not None:
        monkeypatch.setenv(GATE, setting)
    assert ob._resolve_fx("USX", {}) == (None, "missing")
    assert ob._resolve_fx("USX", {"USD": 3.75}) == (None, "missing")
    # The operator's immediate Sheet fix already works without a deployment.
    assert ob._resolve_fx("USX", {"USX": 0.0375}) == (0.0375, "provided")


@pytest.mark.parametrize("setting", ["1", "true", "yes", "on", " TRUE "])
def test_armed_static_rate_values_corn_quote_in_sar(monkeypatch, setting):
    monkeypatch.setenv(GATE, setting)
    rate, source = ob._resolve_fx("USX", {})
    assert source == "static/100"
    assert rate == pytest.approx(0.0375)
    assert 479.75 * rate == pytest.approx(17.990625)
    candidate = ob.normalize_candidate(
        {"symbol": "ZC=F", "currency": "USX", "current_price": 479.75}, {},
        ob.make_criteria(None))
    assert candidate["price_sar"] == pytest.approx(17.990625)


def test_armed_provided_parent_is_divided_once_and_raw_rate_wins(monkeypatch):
    monkeypatch.setenv(GATE, "1")
    assert ob._resolve_fx("USX", {"USD": 3.752}) == (3.752 / 100, "provided/100")
    assert ob._resolve_fx("USX", {"USX": 0.0376, "USD": 3.75}) == (0.0376, "provided")
    assert ob._resolve_fx("usx", {"USD": 3.752}) == (3.752 / 100, "provided/100")
    assert ob._resolve_fx("usx", {"USX": 0.0376, "USD": 3.75}) == (0.0376, "provided")
    assert ob._resolve_fx("usx", {"usx": 0.03755, "USX": 0.0376}) == (0.03755, "provided")


@pytest.mark.parametrize("provided,expected_source", [
    ({"USD": 3.75}, "provided/100"),
    ({"USX": 0.0375}, "provided"),
    ({"USD": 3.752}, "provided/100"),
    ({"USX": 0.03752}, "provided"),
])
def test_peg_sanity_accepts_correct_rates_in_parent_units(monkeypatch, provided, expected_source):
    monkeypatch.setenv(GATE, "1")
    monkeypatch.setenv("TFB_OPP_FX_SANITY", "1")
    rate, source = ob._resolve_fx("USX", provided)
    expected = provided.get("USX", provided.get("USD", 0) / 100)
    assert rate == pytest.approx(expected)
    assert source == expected_source
    assert not ob._FX_PEG_REJECTS


@pytest.mark.parametrize("provided", [{"USD": 9.0}, {"USX": 0.09}, {"USX": 3.75}])
def test_peg_sanity_repairs_bad_subunit_evidence_without_inflation(monkeypatch, provided):
    monkeypatch.setenv(GATE, "1")
    monkeypatch.setenv("TFB_OPP_FX_SANITY", "1")
    rate, source = ob._resolve_fx("USX", provided)
    assert rate == pytest.approx(0.0375)
    assert source == "static(peg-guard)/100"
    assert ob._FX_PEG_REJECTS["USD"]["last"] == pytest.approx(
        provided.get("USD", provided.get("USX", 0) * 100))


def test_peg_sanity_retains_unarmed_exact_usx_sheet_row(monkeypatch):
    monkeypatch.setenv("TFB_OPP_FX_SANITY", "1")
    assert ob._resolve_fx("USX", {"USX": 0.0375}) == (0.0375, "provided")
    assert not ob._FX_PEG_REJECTS


@pytest.mark.parametrize("currency,row_rate", [("USX", 3.75), ("usx", 3.75), ("USX", 0.09)])
def test_armed_row_override_passes_major_unit_peg_check(monkeypatch, currency, row_rate):
    monkeypatch.setenv(GATE, "1")
    monkeypatch.setenv("TFB_OPP_FX_SANITY", "1")
    candidate = ob.normalize_candidate({"symbol": "ZC=F", "currency": currency,
        "current_price": 479.75, "fx_to_sar": row_rate}, {"USD": 3.75}, ob.make_criteria(None))
    assert candidate["price_sar"] == pytest.approx(17.990625)
    assert candidate["fx_source"] == "static(peg-guard)/100"
    assert ob._FX_PEG_REJECTS["USD"]["last"] == pytest.approx(row_rate * 100)


@pytest.mark.parametrize("usx_gate,sanity,row_rate", [
    ("0", "1", 3.75), ("1", "0", 3.75), ("1", "1", 0.0375),
])
def test_row_override_keeps_existing_precedence_when_valid_or_disarmed(monkeypatch, usx_gate, sanity, row_rate):
    monkeypatch.setenv(GATE, usx_gate)
    monkeypatch.setenv("TFB_OPP_FX_SANITY", sanity)
    candidate = ob.normalize_candidate({"symbol": "ZC=F", "currency": "USX",
        "current_price": 479.75, "fx_to_sar": row_rate}, {"USD": 3.75}, ob.make_criteria(None))
    assert candidate["price_sar"] == pytest.approx(479.75 * row_rate)
    assert candidate["fx_source"] == "row"


@pytest.mark.parametrize("armed", [False, True])
@pytest.mark.parametrize("currency,provided,expected", [
    ("GBp", {}, (0.0475, "static/100")),
    ("GBp", {"GBP": 4.75}, (0.0475, "provided/100")),
    ("GBp", {"GBp": 0.0475, "GBP": 4.8}, (0.0475, "provided")),
    ("GBX", {"GBP": 4.75}, (0.0475, "provided/100")),
    ("USD", {"USD": 9.0}, (3.75, "static(peg-guard)")),
    ("SAR", {"SAR": 1.0}, (1.0, "provided")),
])
def test_gbp_and_major_currency_paths_keep_their_units(monkeypatch, armed, currency, provided, expected):
    monkeypatch.setenv(GATE, "1" if armed else "0")
    monkeypatch.setenv("TFB_OPP_FX_SANITY", "1")
    assert ob._resolve_fx(currency, provided) == expected


def test_arming_does_not_bypass_existing_watchlist_policy(monkeypatch):
    monkeypatch.setenv(GATE, "1")
    candidate = ob.normalize_candidate({
        "symbol": "ZC=F", "currency": "USX", "current_price": 479.75,
        "asset_class": "FUTURE", "investability_status": "WATCHLIST",
        "block_reason": "Broker unsupported contract",
    }, {}, ob.make_criteria(None))
    assert candidate["engine_gate"]["investability"] == "WATCHLIST"
    assert candidate["engine_gate"]["reasons"] == "Broker unsupported contract"
