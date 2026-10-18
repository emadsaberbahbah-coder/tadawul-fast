"""Opt-in Yahoo share-class routing, including explicit US input and remapping."""

import socket

import pytest


def _clear_caches(module):
    for value in vars(module).values():
        if callable(value) and hasattr(value, "cache_clear"):
            value.cache_clear()


@pytest.fixture
def symbol_modules(monkeypatch):
    attempted_network = []

    def deny_network(*args, **kwargs):
        attempted_network.append(True)
        raise AssertionError("symbol mapping must not access the network")

    monkeypatch.setattr(socket.socket, "connect", deny_network)
    monkeypatch.setattr(socket.socket, "connect_ex", deny_network)
    monkeypatch.setattr(socket, "create_connection", deny_network)
    monkeypatch.setattr(socket, "getaddrinfo", deny_network)
    monkeypatch.delenv("TFB_YC_SYMBOL_MAP", raising=False)
    monkeypatch.delenv("NORMALIZE_DEFAULT_EQUITY_EXCHANGE_SUFFIX", raising=False)

    from core.providers import yahoo_chart_provider
    from core.symbols import normalize

    assert yahoo_chart_provider._to_yahoo_symbol_external is normalize.to_yahoo_symbol
    _clear_caches(normalize)
    try:
        yield normalize, yahoo_chart_provider
    finally:
        _clear_caches(normalize)
        assert not attempted_network


@pytest.mark.parametrize("dotted", ["BRK.B", "BF.B", "HEI.A", "CRD.A"])
@pytest.mark.parametrize("form", ["dot", "dash", "dot_us", "dash_us"])
def test_opt_in_maps_direct_and_repeated_provider_input(
    monkeypatch, symbol_modules, dotted, form
):
    normalize, provider = symbol_modules
    dashed = dotted.replace(".", "-")
    symbol, canonical = {
        "dot": (dotted, dotted),
        "dash": (dashed, dotted),
        "dot_us": (dotted + ".US", dotted + ".US"),
        "dash_us": (dashed + ".US", dashed + ".US"),
    }[form]
    monkeypatch.setenv("TFB_YAHOO_SHARE_CLASS_DASH", "1")

    assert normalize.normalize_symbol(symbol) == canonical
    assert normalize.to_yahoo_symbol(symbol) == dashed
    assert provider._yc_yahoo_symbol(symbol) == dashed
    assert provider._yc_yahoo_symbol(normalize.to_yahoo_symbol(symbol)) == dashed
    assert normalize.to_yahoo_symbol(normalize.to_yahoo_symbol(symbol)) == dashed
    assert normalize.normalize_symbol(symbol) == canonical


@pytest.mark.parametrize("flag", [None, "0", "off", "false"])
@pytest.mark.parametrize("symbol,first,repeated", [
    ("BRK.B", "BRK.B", "BRK.B"),
    ("BRK-B", "BRK.B", "BRK.B"),
    ("BRK.B.US", "BRK.B", "BRK.B"),
    ("BRK-B.US", "BRK-B", "BRK.B"),
])
def test_absent_or_off_keeps_legacy_mapping(
    monkeypatch, symbol_modules, flag, symbol, first, repeated
):
    normalize, provider = symbol_modules
    if flag is None:
        monkeypatch.delenv("TFB_YAHOO_SHARE_CLASS_DASH", raising=False)
    else:
        monkeypatch.setenv("TFB_YAHOO_SHARE_CLASS_DASH", flag)

    assert normalize.to_yahoo_symbol(symbol) == first
    assert provider._yc_yahoo_symbol(symbol) == first
    assert provider._yc_yahoo_symbol(first) == repeated
    assert normalize.to_yahoo_symbol(first) == repeated


@pytest.mark.parametrize("flag", [None, "off", "1"])
@pytest.mark.parametrize("symbol,expected", [
    ("VOD.L", "VOD.L"),
    ("RIO.L", "RIO.L"),
    ("VOD.L.US", "VOD.L"),
    ("RIO.L.US", "RIO.L"),
    ("BRK.B.L", "BRK.B.L"),
    ("BRK-B.TO", "BRK-B.TO"),
    ("SHOP.TO", "SHOP.TO"),
    ("ABCDEF.B.US", "ABCDEF.B"),
    ("AAPL.US", "AAPL"),
    ("2222.SR", "2222.SR"),
    ("EUR/USD", "EURUSD=X"),
    ("EURUSD=X", "EURUSD=X"),
    ("BTC-USD", "BTC-USD"),
    ("GC=F", "GC=F"),
    ("^GSPC", "^GSPC"),
])
def test_foreign_and_other_instruments_keep_existing_mapping(
    monkeypatch, symbol_modules, flag, symbol, expected
):
    normalize, provider = symbol_modules
    if flag is None:
        monkeypatch.delenv("TFB_YAHOO_SHARE_CLASS_DASH", raising=False)
    else:
        monkeypatch.setenv("TFB_YAHOO_SHARE_CLASS_DASH", flag)

    assert normalize.to_yahoo_symbol(symbol) == expected
    assert provider._yc_yahoo_symbol(symbol) == expected
    assert provider._yc_yahoo_symbol(expected) == expected


@pytest.mark.parametrize("symbol", ["BRK.B", "BRK.B.US"])
def test_registered_inner_exchange_takes_precedence(
    monkeypatch, symbol_modules, symbol
):
    normalize, provider = symbol_modules
    monkeypatch.setenv("TFB_YAHOO_SHARE_CLASS_DASH", "1")
    monkeypatch.setitem(normalize.EXCHANGE_SUFFIXES, ".B", "TEST")
    _clear_caches(normalize)

    assert normalize.to_yahoo_symbol(symbol) == "BRK.B"
    assert provider._yc_yahoo_symbol(symbol) == "BRK.B"
    assert provider._yc_yahoo_symbol(normalize.to_yahoo_symbol(symbol)) == "BRK.B"
