"""normalize v5.6.1: US share-class tickers with a .US suffix reach Yahoo in
the dash form when TFB_YAHOO_SHARE_CLASS_DASH=1 (BRK.B.US -> BRK-B).
Live 2026-10-24: Yahoo chart BRK-B 200, BRK.B 404."""
from __future__ import annotations

import pytest

from core.symbols import normalize as N

CASES = ["BRK.B", "BRK.B.US", "BF.B.US", "BRK-B.US", "HEI.A", "VOD.L", "RIO.L",
         "AAPL", "AAPL.US", "A.US", "1120.SR", "GC=F", "EURUSD=X", "BTC-USD", "SAP.DE"]


def _map(monkeypatch, flag):
    if flag is None:
        monkeypatch.delenv("TFB_YAHOO_SHARE_CLASS_DASH", raising=False)
    else:
        monkeypatch.setenv("TFB_YAHOO_SHARE_CLASS_DASH", flag)
    N.to_yahoo_symbol.cache_clear()
    try:
        return {s: N.to_yahoo_symbol(s) for s in CASES}
    finally:
        N.to_yahoo_symbol.cache_clear()


def test_flag_on_maps_us_suffixed_share_classes_to_dash(monkeypatch):
    m = _map(monkeypatch, "1")
    assert m["BRK.B.US"] == "BRK-B"
    assert m["BF.B.US"] == "BF-B"
    assert m["BRK.B"] == "BRK-B"
    assert m["BRK-B.US"] == "BRK-B"
    for s in ("VOD.L", "RIO.L", "1120.SR", "GC=F", "EURUSD=X", "BTC-USD", "SAP.DE"):
        assert m[s] == s
    assert m["AAPL.US"] == "AAPL" and m["A.US"] == "A"


def test_flag_off_is_byte_identical_to_v5_6_0(monkeypatch):
    m = _map(monkeypatch, None)
    assert m["BRK.B.US"] == "BRK.B"
    assert m["BRK.B"] == "BRK.B"
    assert m["AAPL.US"] == "AAPL"


def test_version():
    assert N.__version__ == "5.6.1"
