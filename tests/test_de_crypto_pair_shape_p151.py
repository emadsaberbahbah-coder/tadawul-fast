"""P-151 (data_engine_v2 v5.145.0) — CRYPTO-PAIR SHAPE: asset-class /
exchange / currency identity for Yahoo crypto pairs (<ROOT>-<QUOTE>).
Repo-runnable battery T1-T7 over the REAL module (no stand-ins), driving
_apply_symbol_context_defaults (the Commodities_FX identity block where the
49 mislabelled rows of the 2026-09-20 export live) and the three inferrers.

Properties asserted at HEAD:
  off (unset) => rows and inferrers byte-identical to v5.144.0; observe =>
  values untouched + ONE countable crypto_pair_shape:observe tag on a shaped
  row whose class is missing or equity-like; enforce => asset_class "Crypto",
  a NASDAQ/NYSE or blank exchange -> "Crypto", blank currency -> <QUOTE>,
  tagged crypto_pair_shape:enforce; a provider-declared non-equity class is
  never rewritten; share-class dashes (BRK-B, AKO-B.US, GRT-UN.TO), FX (=X),
  futures (=F) and indices (^) never match; idempotent; mode disclosed in
  surface_gate_states(); tags substring-safe.
Run:  python tests/test_de_crypto_pair_shape_p151.py
      (or pytest -q tests/test_de_crypto_pair_shape_p151.py)
"""
import copy
import hashlib
import importlib
import inspect
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
de = importlib.import_module("core.data_engine_v2")

ENV = "TFB_SYM_CRYPTO_PAIR_CLASS"
FORBIDDEN = ("cap", "forecast", "target", "roi", "drop", "reject",
             "provider_target", "price_bar_stale", "xprovider_price_conflict")

# 2026-09-20 Commodities_FX specimens (shape + the exported labels)
FLOW = {"symbol": "FLOW-USD", "asset_class": "Equity", "exchange": "NASDAQ/NYSE",
        "currency": "USD", "current_price": 0.03}
SHIB = {"symbol": "SHIB-USD", "asset_class": "Equity", "exchange": "", "currency": ""}
DOT_DECLARED = {"symbol": "DOT-USD", "asset_class": "CRYPTOCURRENCY", "exchange": "CCC",
                "currency": "USD"}
BARE = {"symbol": "ETC-USD"}                                  # nothing declared
BRKB = {"symbol": "BRK-B", "asset_class": "Equity", "exchange": "NASDAQ/NYSE"}
AKO = {"symbol": "AKO-B.US", "asset_class": "Equity"}
GRT = {"symbol": "GRT-UN.TO", "asset_class": "Equity"}
FX = {"symbol": "IDRUSD=X"}
FUT = {"symbol": "HG=F", "asset_class": "Commodity", "currency": "USD"}
CRC = {"symbol": "CRC.US", "asset_class": "Equity", "exchange": "NYSE/NASDAQ"}


def _env(mode=None):
    os.environ.pop(ENV, None)
    if mode is not None:
        os.environ[ENV] = mode


def _apply(row, page="Commodities_FX", mode=None):
    _env(mode)
    try:
        return de._apply_symbol_context_defaults(copy.deepcopy(row), row["symbol"], page)
    finally:
        _env(None)


def _digest(r):
    return hashlib.sha256(json.dumps(r, sort_keys=True, default=str).encode()).hexdigest()


ALL = (FLOW, SHIB, DOT_DECLARED, BARE, BRKB, AKO, GRT, FX, FUT, CRC)


# ---- T1 helpers ------------------------------------------------------------------
def test_t1_helpers():
    _env(None)
    assert de._crypto_pair_class_mode() == "off"
    for v in ("observe", "ENFORCE"):
        os.environ[ENV] = v
        assert de._crypto_pair_class_mode() == v.lower()
    os.environ[ENV] = "1"
    assert de._crypto_pair_class_mode() == "off"       # no boolean alias: explicit words only
    _env(None)
    assert de._crypto_pair_shape("FLOW-USD") == "USD"
    assert de._crypto_pair_shape("eth-usdt") == "USDT"
    assert de._crypto_pair_shape("BTC-EUR") == "EUR"
    for s in ("BRK-B", "AKO-B.US", "GRT-UN.TO", "IDRUSD=X", "HG=F", "^GSPC", "X-USD",
              "VERYLONGROOTNAME-USD", "", None, "BTC-USD.X", "BTC_USD"):
        assert de._crypto_pair_shape(s) == "", s
    assert de._crypto_pair_class_like_equity("") and de._crypto_pair_class_like_equity(None)
    assert de._crypto_pair_class_like_equity("Equity") and de._crypto_pair_class_like_equity("EQUITY")
    assert not de._crypto_pair_class_like_equity("CRYPTOCURRENCY")
    assert not de._crypto_pair_class_like_equity("FX") and not de._crypto_pair_class_like_equity("Commodity")


# ---- T2 off identity ---------------------------------------------------------------
def test_t2_off_identity():
    for row in ALL:
        a, b = _apply(row), _apply(row, mode="off")
        assert a == b
        assert "crypto_pair_shape" not in str(a.get("warnings") or "")
    _env(None)
    assert de._infer_asset_class_from_symbol("FLOW-USD") == "Equity"        # v5.144.0 behaviour kept
    assert de._infer_exchange_from_symbol("FLOW-USD") == "NASDAQ/NYSE"
    assert de._infer_currency_from_symbol("ETH-USDT") == "USD"


# ---- T3 observe ------------------------------------------------------------------
def test_t3_observe():
    for row in (FLOW, SHIB, BARE):
        off, obs = _apply(row), _apply(row, mode="observe")
        for k in ("asset_class", "exchange", "currency", "country"):
            assert off.get(k) == obs.get(k), (row["symbol"], k)
        w = str(obs.get("warnings") or "")
        assert w.count("crypto_pair_shape:observe") == 1, (row["symbol"], w)
    for row in (DOT_DECLARED, BRKB, AKO, GRT, FX, FUT, CRC):
        off, obs = _apply(row), _apply(row, mode="observe")
        assert off == obs, row["symbol"]                     # nothing to observe on these


# ---- T4 enforce ------------------------------------------------------------------
def test_t4_enforce():
    r = _apply(FLOW, mode="enforce")
    assert r["asset_class"] == "Crypto" and r["exchange"] == "Crypto" and r["currency"] == "USD"
    assert str(r["warnings"]).count("crypto_pair_shape:enforce") == 1
    r = _apply(SHIB, mode="enforce")
    assert r["asset_class"] == "Crypto" and r["exchange"] == "Crypto" and r["currency"] == "USD"
    r = _apply(BARE, mode="enforce")                          # inferrers answer the shape first
    assert r["asset_class"] == "Crypto" and r["exchange"] == "Crypto" and r["currency"] == "USD"
    assert "crypto_pair_shape:enforce" not in str(r.get("warnings") or "")  # nothing to repair: no equity label was written
    r = _apply(DOT_DECLARED, mode="enforce")                  # declared non-equity class stays
    assert r["asset_class"] == "CRYPTOCURRENCY" and r["exchange"] == "CCC"
    assert "crypto_pair_shape" not in str(r.get("warnings") or "")
    for row in (BRKB, AKO, GRT, FX, FUT, CRC):
        assert _apply(row, mode="enforce") == _apply(row), row["symbol"]
    _env("enforce")
    try:
        assert de._infer_asset_class_from_symbol("SHIB-USD") == "Crypto"
        assert de._infer_exchange_from_symbol("SHIB-USD") == "Crypto"
        assert de._infer_currency_from_symbol("ETH-USDT") == "USDT"
        assert de._infer_asset_class_from_symbol("BRK-B") == "Equity"
        assert de._infer_exchange_from_symbol("GRT-UN.TO") == "TSX"
    finally:
        _env(None)


# ---- T5 equity page never touched, idempotence ------------------------------------
def test_t5_page_scope_and_idempotence():
    # the apply site is the Commodities_FX / =F / =X block: an equity page row
    # shaped like a pair (never seen in production) is left alone there
    r = _apply(FLOW, page="Global_Markets", mode="enforce")
    assert r["asset_class"] == "Equity" and "crypto_pair_shape" not in str(r.get("warnings") or "")
    for mode in (None, "observe", "enforce"):
        for row in ALL:
            once = _apply(row, mode=mode)
            twice = de._apply_symbol_context_defaults(copy.deepcopy(once), row["symbol"], "Commodities_FX") \
                if mode is None else _apply(once, mode=mode)
            assert once == twice, (row["symbol"], mode)
            assert _digest(_apply(row, mode=mode)) == _digest(_apply(row, mode=mode))


# ---- T6 substring safety ---------------------------------------------------------
def test_t6_substring_safety():
    for tag in (de._CRYPTO_PAIR_TAG_OBSERVE, de._CRYPTO_PAIR_TAG_ENFORCE):
        assert not any(b in tag.lower() for b in FORBIDDEN), tag


# ---- T7 wiring + disclosure --------------------------------------------------------
def test_t7_wiring():
    src = inspect.getsource(de)
    assert src.count("_crypto_pair_shape_apply(out, sym)") == 1
    assert src.count('return "Crypto"  # v5.145.0') == 2
    assert src.count("return _cq  # v5.145.0") == 1
    _env(None)
    assert de.surface_gate_states().get("crypto_pair_class") == "off"
    os.environ[ENV] = "observe"
    assert de.surface_gate_states().get("crypto_pair_class") == "observe"
    _env(None)
    # v5.147.0: exact pin loosened to a floor (the file is a battery, not a version lock)
    assert tuple(int(x) for x in de.__version__.split(".")[:3]) >= (5, 145, 0)


if __name__ == "__main__":
    import traceback
    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print("PASS", name)
            except Exception:
                fails += 1
                print("FAIL", name)
                traceback.print_exc()
    print("RESULT", "FAIL %d" % fails if fails else "ALL PASS")
    sys.exit(1 if fails else 0)
