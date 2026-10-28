#!/usr/bin/env python3
"""tests/test_yahoo_id_alias.py - core/symbols/normalize.py v5.7.0 (TFB_YAHOO_ID_ALIAS).

WHY (2026-10-28): the morning board was WITHHELD because Commodities_FX was
fresh on 430 of 453 rows (< 95 %). Nine stale rows fail every day because
their Yahoo ticker is dead or collides: seven crypto pairs, LBS=F and DX=F.
The provider read-back (provider_identity_probe, run 40643617016) found the
WP-05 EODHD .CC route without a price for all seven coins and a fresh,
correctly named Yahoo identifier for all nine rows. v5.7.0 maps exactly
those nine page symbols to the verified ids when TFB_YAHOO_ID_ALIAS=1
(default off, byte-identical to v5.6.0).

Pinned here (runs the REAL modules; the engine / adapter cases need the
full dependency set, like tests/test_crypto_provider_identity.py):
  A1 off (unset or 0): to_yahoo_symbol is v5.6.0 for the nine and controls
  A2 on: the nine map to the read-back ids (any case); nothing else moves
  A3 every target is idempotent through to_yahoo_symbol (the Yahoo
     adapter maps the symbol again before the call)
  A4 EODHD and Finnhub routes do not change with the flag
  A5 engine route: _provider_symbol_for('yahoo_chart', 'SUI-USD') follows
     the flag; the EODHD route stays legacy
  A6 identity guards: the adapter guard (YC-1) accepts a response declaring
     the mapped id; the engine guard (AU-1) accepts the adapter's patch,
     which declares the page symbol; a crossed coin is still refused
  A7 the adapter's own mapping (_yc_yahoo_symbol) follows the flag
  A8 the map equals the ids the probe spec carries for those rows
  A9 version, manifest pin and flag registration

v5.7.1 / engine v5.159.2 (2026-10-28, final audit of run 40673830237:
Commodities_FX name coverage 98.45 % < 99 % - the seven aliased coins were
priced but their chart-meta name was refused as identity_echo_refused):
  A10 yahoo_id_alias: the verified id for the nine only while the flag is
      on; '' for controls, for the targets themselves and for junk input
  A11 BB-1 echo gate: the echoed alias matches its page symbol only while
      the flag is on; a crossed coin and every control pair are unchanged
  A12 the real _apply_identity_rescue with a chart meta echoing the alias:
      off -> blank name + identity_echo_refused (the live defect); on ->
      the verified name; a crossed coin stays refused

Run: python tests/test_yahoo_id_alias.py   or   pytest -q <this file>
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from core.symbols import normalize  # noqa: E402

ALIAS = {
    "SUI-USD": "SUI20947-USD", "GRT-USD": "GRT6719-USD", "APT-USD": "APT21794-USD",
    "UNI-USD": "UNI7083-USD", "COMP-USD": "COMP5692-USD", "IMX-USD": "IMX10603-USD",
    "STX-USD": "STX4847-USD", "LBS=F": "LBR=F", "DX=F": "DX-Y.NYB",
}
CONTROLS = ["BTC-USD", "ETH-USD", "EURUSD=X", "XAUUSD=X", "GC=F", "LBR=F", "AAPL", "AAPL.US",
            "2222.SR", "BRK.B", "^GSPC", "VOD.L", "LINK.US", "SUI"]


@pytest.fixture(autouse=True)
def clear_cache(monkeypatch):
    monkeypatch.delenv("TFB_YAHOO_ID_ALIAS", raising=False)
    normalize.to_yahoo_symbol.cache_clear()
    yield
    normalize.to_yahoo_symbol.cache_clear()


def _arm(monkeypatch, value="1"):
    monkeypatch.setenv("TFB_YAHOO_ID_ALIAS", value)
    normalize.to_yahoo_symbol.cache_clear()


# ----------------------------------------------------------------------------- A1-A4
@pytest.mark.parametrize("value", [None, "0", "off", "false"])
def test_a1_off_is_v560(monkeypatch, value):
    if value is not None:
        _arm(monkeypatch, value)
    for sym in ALIAS:
        assert normalize.to_yahoo_symbol(sym) == sym         # v5.6.0 returns the page ticker


def test_a1_off_matches_the_untouched_mapping_for_controls(monkeypatch):
    before = {s: normalize.to_yahoo_symbol(s) for s in CONTROLS}
    _arm(monkeypatch)
    assert {s: normalize.to_yahoo_symbol(s) for s in CONTROLS} == before


def test_a2_on_maps_exactly_the_nine(monkeypatch):
    _arm(monkeypatch)
    for sym, target in ALIAS.items():
        assert normalize.to_yahoo_symbol(sym) == target
        assert normalize.to_yahoo_symbol(sym.lower()) == target


def test_a3_targets_are_idempotent(monkeypatch):
    _arm(monkeypatch)
    for target in ALIAS.values():
        assert normalize.to_yahoo_symbol(target) == target


def test_a4_other_provider_routes_do_not_move(monkeypatch):
    normalize.to_eodhd_symbol.cache_clear()
    eodhd = {s: normalize.to_eodhd_symbol(s) for s in ALIAS}
    finnhub = {s: normalize.to_finnhub_symbol(s) for s in ALIAS}
    _arm(monkeypatch)
    normalize.to_eodhd_symbol.cache_clear()
    normalize.to_finnhub_symbol.cache_clear()
    assert {s: normalize.to_eodhd_symbol(s) for s in ALIAS} == eodhd
    assert {s: normalize.to_finnhub_symbol(s) for s in ALIAS} == finnhub


# ----------------------------------------------------------------------------- A5-A7 (full deps)
def test_a5_engine_route_follows_the_flag(monkeypatch):
    engine = pytest.importorskip("core.data_engine_v2")
    assert engine._provider_symbol_for("yahoo_chart", "SUI-USD") == "SUI-USD"
    legacy_eodhd = engine._provider_symbol_for("eodhd", "SUI-USD")
    _arm(monkeypatch)
    assert engine._provider_symbol_for("yahoo_chart", "SUI-USD") == "SUI20947-USD"
    assert engine._provider_symbol_for("yahoo_chart", "LBS=F") == "LBR=F"
    assert engine._provider_symbol_for("eodhd", "SUI-USD") == legacy_eodhd


def test_a6_identity_guards_accept_the_alias_and_refuse_a_crossed_coin(monkeypatch):
    engine = pytest.importorskip("core.data_engine_v2")
    yc = pytest.importorskip("core.providers.yahoo_chart_provider")
    _arm(monkeypatch)
    mapped = yc._yc_yahoo_symbol("SUI-USD")
    assert yc._yc_declared_identity_mismatch(mapped, {"symbol": "SUI20947-USD"}, {"symbol": "SUI20947-USD"}) is None
    assert yc._yc_declared_identity_mismatch(mapped, {"symbol": "SUI-USD"}, {"symbol": "SUI-USD"}) == "SUI-USD"
    # the adapter's patch declares the page symbol (_enrich_data(sym, ...))
    assert engine._engine_patch_identity_mismatch("SUI-USD", {"symbol": "SUI-USD", "symbol_normalized": "SUI-USD"}) is None
    assert engine._engine_patch_identity_mismatch("SUI-USD", {"symbol": "BTC-USD"}) == "BTC-USD"


def test_a7_adapter_mapping_follows_the_flag(monkeypatch):
    yc = pytest.importorskip("core.providers.yahoo_chart_provider")
    monkeypatch.delenv("TFB_YC_SYMBOL_MAP", raising=False)
    assert yc._yc_yahoo_symbol("STX-USD") == "STX-USD"
    _arm(monkeypatch)
    assert yc._yc_yahoo_symbol("STX-USD") == "STX4847-USD"
    assert yc._yc_yahoo_symbol("DX=F") == "DX-Y.NYB"


# ----------------------------------------------------------------------------- A10-A12 (v5.7.1 / v5.159.2)
NAMES = {
    "SUI-USD": "Sui USD", "GRT-USD": "The Graph USD", "APT-USD": "Aptos USD",
    "UNI-USD": "Uniswap USD", "COMP-USD": "Compound USD", "IMX-USD": "Immutable USD",
    "STX-USD": "Stacks USD",
}


def test_a10_yahoo_id_alias_follows_the_flag(monkeypatch):
    for sym in list(ALIAS) + CONTROLS:
        assert normalize.yahoo_id_alias(sym) == ""          # off: nothing
    _arm(monkeypatch, "0")
    assert normalize.yahoo_id_alias("SUI-USD") == ""
    _arm(monkeypatch)
    for sym, target in ALIAS.items():
        assert normalize.yahoo_id_alias(sym) == target
        assert normalize.yahoo_id_alias(sym.lower()) == target
        assert normalize.yahoo_id_alias(target) == ""        # a target is not a page symbol
    for sym in CONTROLS:
        assert normalize.yahoo_id_alias(sym) == ""
    for junk in (None, "", "   ", 42, object()):
        assert normalize.yahoo_id_alias(junk) == ""


def test_a11_echo_gate_accepts_only_the_verified_alias(monkeypatch):
    engine = pytest.importorskip("core.data_engine_v2")
    controls = [("SUI-USD", "SUI-USD", True), ("AAPL.US", "AAPL", True), ("AAPL", "MSFT", False),
                ("7205.T", "ACU.US", False), ("SUI-USD", "", True), ("SUI-USD", "GRT6719-USD", False),
                ("BTC-USD", "SUI20947-USD", False), ("LBS=F", "DX-Y.NYB", False)]
    for sym, target in ALIAS.items():
        assert engine._bb1_echo_matches(sym, target) is False   # the live refusal
    before = [engine._bb1_echo_matches(a, b) for a, b, _ in controls]
    assert before == [want for _, _, want in controls]
    _arm(monkeypatch)
    for sym, target in ALIAS.items():
        assert engine._bb1_echo_matches(sym, target) is True
        assert engine._bb1_echo_matches(sym.lower(), target.lower()) is True
    assert [engine._bb1_echo_matches(a, b) for a, b, _ in controls] == before
    for sym, target in ALIAS.items():                        # crossed aliases stay refused
        for other, other_target in ALIAS.items():
            if other != sym and other_target != target:
                assert engine._bb1_echo_matches(sym, other_target) is False


def _rescue(engine, monkeypatch, page_symbol, meta):
    import asyncio
    from types import SimpleNamespace

    async def fetch_chart_meta(_sym):
        return dict(meta)

    monkeypatch.setattr(engine, "_import_yahoo_provider_module",
                        lambda _name: SimpleNamespace(fetch_chart_meta=fetch_chart_meta))
    monkeypatch.delenv("TFB_ENGINE_IDENTITY_RESCUE", raising=False)
    real = object.__new__(engine.DataEngineV5)
    row = {"symbol": page_symbol, "name": "", "asset_class": "Crypto", "current_price": 1.0}
    return asyncio.run(real._apply_identity_rescue(row, page_symbol))


@pytest.mark.parametrize("page_symbol", sorted(NAMES))
def test_a12_real_rescue_names_the_aliased_coins(monkeypatch, page_symbol):
    engine = pytest.importorskip("core.data_engine_v2")
    target = ALIAS[page_symbol]
    meta = {"symbol": target, "longName": NAMES[page_symbol], "shortName": NAMES[page_symbol],
            "instrumentType": "CRYPTOCURRENCY", "currency": "USD", "exchangeName": "CCC"}
    off = _rescue(engine, monkeypatch, page_symbol, meta)
    assert off["name"] == ""
    assert "identity_echo_refused" in str(off.get("warnings"))
    _arm(monkeypatch)
    on = _rescue(engine, monkeypatch, page_symbol, meta)
    assert on["name"] == NAMES[page_symbol]
    assert "name_from_chart_meta" in str(on.get("warnings"))
    assert "identity_echo_refused" not in str(on.get("warnings"))
    assert on["current_price"] == 1.0                        # the rescue never touches price


def test_a12_real_rescue_still_refuses_a_crossed_coin(monkeypatch):
    engine = pytest.importorskip("core.data_engine_v2")
    _arm(monkeypatch)
    crossed = {"symbol": "GRT6719-USD", "longName": "The Graph USD", "instrumentType": "CRYPTOCURRENCY"}
    row = _rescue(engine, monkeypatch, "SUI-USD", crossed)
    assert row["name"] == ""
    assert "identity_echo_refused" in str(row.get("warnings"))
    plain = {"symbol": "SUI-USD", "longName": "Sui USD"}         # an echo of the page symbol: unchanged
    assert _rescue(engine, monkeypatch, "SUI-USD", plain)["name"] == "Sui USD"


# ----------------------------------------------------------------------------- A8-A9
def test_a8_map_matches_the_probe_spec():
    assert normalize._YAHOO_ID_ALIAS == ALIAS
    spec = json.loads((_REPO / "data" / "provider_probe_cfx_2026-10-28.json").read_text(encoding="ascii"))
    yahoo = {i["row"]: i.get("yahoo") or [] for i in spec["items"]}
    for sym, target in ALIAS.items():
        assert target in yahoo[sym], (sym, target)


def test_a9_version_pin_and_flag_registration():
    # 2026-10-28 (stacked): 5.7.1 adds yahoo_id_alias (A10-A12).
    assert normalize.__version__ == "5.7.1"
    vd = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert "('core.symbols.normalize', '__version__', '5.7.1', 'symbol normalization')" in vd
    assert "('TFB_YAHOO_ID_ALIAS', '0'," in vd
    contract = (_REPO / "docs" / "render_env_contract.md").read_text(encoding="utf-8")
    assert "TFB_YAHOO_ID_ALIAS" in contract


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
