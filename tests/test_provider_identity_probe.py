#!/usr/bin/env python3
"""tests/test_provider_identity_probe.py - scripts/provider_identity_probe.py v1.0.0.

WHY (2026-10-28): the morning board was WITHHELD because Commodities_FX was
fresh on 430 of 453 rows (94.9 % < 95 %). 17 rows fail every day (7 crypto
pairs on the wrong coin, gold spot and 7 exotic crosses without a price, LBS=F
and DX=F). WP-05's TFB_EODHD_CRYPTO_MAP=1 may only be armed after a provider
read-back, and the build container cannot reach the providers. This harness
runs the REAL probe offline against a fake HTTP session with recorded-shape
EODHD real-time / search and Yahoo chart payloads.

Pinned here:
  P1 the default spec: ASCII, the 17 structural rows of 28 Oct + 2 controls
  P2 parsers: EODHD real-time (price, NA, stale), search (code.exchange,
     name, previous close), Yahoo chart (price, not found, stale)
  P3 verdicts: CONFIRMED needs an EODHD and a Yahoo price within 3 % and,
     for a curated crypto root, a name passing the WP-05 screen; a .CC code
     that is the wrong coin is a NAME_MISMATCH, never CONFIRMED; an exotic
     cross with no source is DEAD; a replacement future reads AVAILABLE
  P4 main(): table + job summary + evidence; the EODHD key never appears;
     no key -> Yahoo only; network down -> exit 1; bad spec -> exit 2;
     one retry on 429
  P5 ASCII module, version, dispatch-only read-only workflow

Run: python tests/test_provider_identity_probe.py   or   pytest -q <this file>
"""
from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts import provider_identity_probe as pp  # noqa: E402

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 28, 6, 0, tzinfo=UTC)
KEY = "secret-eodhd-key-xyz"
T1 = int((NOW - dt.timedelta(hours=1)).timestamp())
OLD = int((NOW - dt.timedelta(hours=200)).timestamp())


class Resp:
    def __init__(self, status: int = 200, payload: Any = None, text: str = ""):
        self.status_code = status
        self._payload = payload
        self.text = text if text else (json.dumps(payload) if payload is not None else "")

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


def rt(code, close, ts=T1):
    return {"code": code, "timestamp": ts, "close": close, "previousClose": close, "change_p": 0.1}


def chart(name, price, ts=T1, itype="CRYPTOCURRENCY"):
    return {"chart": {"result": [{"meta": {"symbol": "x", "longName": name, "shortName": name,
                                           "regularMarketPrice": price, "regularMarketTime": ts,
                                           "instrumentType": itype, "currency": "USD"}}], "error": None}}


NOT_FOUND = {"chart": {"result": None, "error": {"code": "Not Found",
                                                 "description": "No data found, symbol may be delisted"}}}


class FakeHTTP:
    """cc_wrong=True: EODHD's SUI-USD.CC is the colliding coin (the case the
    read-back must catch before anyone arms TFB_EODHD_CRYPTO_MAP)."""

    def __init__(self, cc_wrong: bool = False, fail_first_429: bool = False, down: bool = False):
        self.calls: List[str] = []
        self.cc_wrong = cc_wrong
        self.fail_first_429 = fail_first_429
        self.down = down

    def get(self, url, params=None, headers=None, timeout=None):
        params = params or {}
        self.calls.append(url)
        if self.down:
            raise ConnectionError("network down token=" + KEY)
        assert headers and "User-Agent" in headers
        if self.fail_first_429 and len(self.calls) == 1:
            return Resp(429, text="Too Many Requests")
        if url.startswith("https://eodhd.com/api/real-time/"):
            assert params.get("api_token") == KEY
            code = url.rsplit("/", 1)[1]
            table = {
                "SUI-USD.CC": rt(code, 0.0003 if self.cc_wrong else 2.11),
                "SUI-USD.US": {"code": code, "timestamp": "NA", "close": "NA"},
                "BTC-USD.CC": rt(code, 67000.0),
                "EURUSD.FOREX": rt(code, 1.0802),
                "XAUUSD.FOREX": rt(code, 2650.0),
            }
            if code in table:
                return Resp(200, table[code])
            return Resp(404, text="Ticker Not Found. key=" + KEY)
        if url.startswith("https://eodhd.com/api/search/"):
            q = url.rsplit("/", 1)[1]
            if q == "SUI":
                return Resp(200, [
                    {"Code": "SUI-USD", "Exchange": "CC", "Name": "Salmonation" if self.cc_wrong else "Sui",
                     "Type": "Currency", "previousClose": 0.0003 if self.cc_wrong else 2.1,
                     "previousCloseDate": "2026-10-27"},
                    {"Code": "SUI20947-USD", "Exchange": "CC", "Name": "Sui", "Type": "Currency",
                     "previousClose": 2.1, "previousCloseDate": "2026-10-27"}])
            if q == "BTC":
                return Resp(200, [{"Code": "BTC-USD", "Exchange": "CC", "Name": "Bitcoin",
                                   "previousClose": 67000, "previousCloseDate": "2026-10-27"}])
            return Resp(200, [])
        if url.startswith("https://query1.finance.yahoo.com/v8/finance/chart/"):
            t = url.rsplit("/", 1)[1]
            table = {
                "SUI20947-USD": chart("Sui USD", 2.12),
                "BTC-USD": chart("Bitcoin USD", 67100.0),
                "EURUSD=X": chart("EUR/USD", 1.081, itype="CURRENCY"),
                "GC=F": chart("Gold Dec 26", 2655.0, itype="FUTURE"),
                "LBR=F": chart("Lumber Futures", 600.0, itype="FUTURE"),
                "DX=F": chart("US Dollar Index Dec 26", 99.1, ts=OLD, itype="FUTURE"),
                "DX-Y.NYB": chart("US Dollar Index", 99.0, itype="INDEX"),
            }
            if t in table:
                return Resp(200, table[t])
            return Resp(404, NOT_FOUND)
        return Resp(404, text="no route")


def _item(row, kind, eodhd=(), search="", yahoo=()):
    return {"row": row, "kind": kind, "eodhd": list(eodhd), "search": search, "yahoo": list(yahoo), "note": ""}


def _prober(http=None, key=KEY):
    return pp.Prober(http or FakeHTTP(), key, NOW, sleep=lambda _s: None)


SUI = _item("SUI-USD", "crypto", ["SUI-USD.CC", "SUI-USD.US"], "SUI", ["SUI-USD", "SUI20947-USD"])


# ----------------------------------------------------------------------------- P1
def test_p1_default_spec_ascii_and_complete():
    raw = Path(pp.DEFAULT_SPEC).read_bytes()
    assert all(b < 128 for b in raw)
    items = pp.load_spec(pp.DEFAULT_SPEC)
    rows = {i["row"] for i in items}
    assert {"SUI-USD", "GRT-USD", "APT-USD", "UNI-USD", "COMP-USD", "IMX-USD", "STX-USD"} <= rows
    assert {"XAUUSD=X", "BHDSAR=X", "BRLBHD=X", "AEDMYR=X", "AEDPHP=X", "AEDIDR=X", "BHDPLN=X",
            "AEDMXN=X", "LBS=F", "DX=F"} <= rows
    assert {i["row"] for i in items if i["kind"] == "control"} == {"BTC-USD", "EURUSD=X"}
    sui = next(i for i in items if i["row"] == "SUI-USD")
    assert sui["eodhd"] == ["SUI-USD.CC", "SUI-USD.US"] and "SUI20947-USD" in sui["yahoo"]


def test_p1_bad_spec_items_are_refused():
    with pytest.raises(ValueError):
        pp.load_spec('{"items": [{"row": "X", "kind": "equity"}]}')
    with pytest.raises(ValueError):
        pp.load_spec('{"items": []}')


# ----------------------------------------------------------------------------- P2
def test_p2_parsers():
    ok = pp.parse_eodhd_rt(rt("BTC-USD.CC", 67000.0), NOW)
    assert ok["ok"] and ok["price"] == 67000.0 and ok["age_h"] == 1.0
    na = pp.parse_eodhd_rt({"code": "X", "timestamp": "NA", "close": "NA"}, NOW)
    assert not na["ok"] and na["error"] == "no price"
    stale = pp.parse_eodhd_rt(rt("X", 1.0, ts=OLD), NOW)
    assert not stale["ok"] and stale["error"].startswith("stale")
    s = pp.parse_eodhd_search(FakeHTTP(cc_wrong=True).get(
        pp.EODHD_SEARCH.format(query="SUI"), params={"api_token": KEY}, headers={"User-Agent": "t"}).json(), NOW)
    assert [x["code"] for x in s] == ["SUI-USD.CC", "SUI20947-USD.CC"] and s[0]["name"] == "Salmonation"
    y = pp.parse_yahoo_chart(chart("Sui USD", 2.12), NOW)
    assert y["ok"] and y["name"] == "Sui USD"
    nf = pp.parse_yahoo_chart(NOT_FOUND, NOW)
    assert not nf["ok"] and "delisted" in nf["error"]
    assert not pp.parse_yahoo_chart(chart("x", 1.0, ts=OLD), NOW)["ok"]


# ----------------------------------------------------------------------------- P3
def test_p3_cc_route_confirmed_when_it_is_the_right_coin():
    r = pp.probe_item(_prober(), SUI)
    assert r["verdict"] == "CONFIRMED"
    assert r["best"].startswith("eodhd:SUI-USD.CC = yahoo:SUI20947-USD")
    assert r["mismatches"] == []


def test_p3_cc_route_that_is_the_wrong_coin_is_never_confirmed():
    r = pp.probe_item(_prober(FakeHTTP(cc_wrong=True)), SUI)
    assert r["verdict"] == "AVAILABLE" and r["best"] == "yahoo:SUI20947-USD"
    assert r["mismatches"] == ['eodhd:SUI-USD.CC "Salmonation"']


def test_p3_fx_cross_dead_and_gold_confirmed():
    assert pp.probe_item(_prober(), _item("BHDSAR=X", "fx", ["BHDSAR.FOREX"], "", ["BHDSAR=X"]))["verdict"] == "DEAD"
    g = pp.probe_item(_prober(), _item("XAUUSD=X", "fx", ["XAUUSD.FOREX"], "", ["XAUUSD=X", "GC=F"]))
    assert g["verdict"] == "CONFIRMED" and g["best"].startswith("eodhd:XAUUSD.FOREX = yahoo:GC=F (0.2 %)")


def test_p3_replacement_future_and_stale_quote():
    lb = pp.probe_item(_prober(), _item("LBS=F", "future", [], "", ["LBS=F", "LBR=F"]))
    assert lb["verdict"] == "AVAILABLE" and lb["best"] == "yahoo:LBR=F"
    dx = pp.probe_item(_prober(), _item("DX=F", "future", [], "", ["DX=F", "DX-Y.NYB"]))
    assert dx["best"] == "yahoo:DX-Y.NYB"
    assert next(c for c in dx["candidates"] if c["code"] == "DX=F")["error"].startswith("stale")


def test_p3_crypto_without_any_name_is_not_confirmed():
    cands = [{"provider": "eodhd", "code": "SUI-USD.CC", "ok": True, "price": 2.1, "age_h": 1, "name": ""},
             {"provider": "yahoo", "code": "SUI20947-USD", "ok": True, "price": 2.1, "age_h": 1, "name": ""}]
    assert pp.judge(SUI, cands)["verdict"] == "AVAILABLE"


# ----------------------------------------------------------------------------- P4
def test_p4_main_end_to_end(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("EODHD_API_KEY", KEY)
    monkeypatch.setenv("TFB_PROBE_EVIDENCE_DIR", str(tmp_path / "ev"))
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    monkeypatch.delenv("TFB_PROBE_SPEC", raising=False)
    assert pp.main([], http=FakeHTTP(cc_wrong=True), now=NOW, sleep=lambda _s: None) == 0
    out = capsys.readouterr().out
    ev = json.loads((tmp_path / "ev" / "provider_probe.json").read_text())
    rows = {r["row"]: r for r in ev["rows"]}
    assert rows["BTC-USD"]["verdict"] == "CONFIRMED" and rows["EURUSD=X"]["verdict"] == "CONFIRMED"
    assert rows["SUI-USD"]["verdict"] == "AVAILABLE" and rows["BHDSAR=X"]["verdict"] == "DEAD"
    assert rows["GRT-USD"]["verdict"] == "DEAD"                 # nothing in the fake for GRT
    assert "| SUI-USD | crypto | AVAILABLE | yahoo:SUI20947-USD |" in out
    assert "Provider identity probe v1.0.0" in summary.read_text()
    blob = out + summary.read_text() + (tmp_path / "ev" / "provider_probe.json").read_text()
    assert KEY not in blob


def test_p4_without_key_only_yahoo(monkeypatch, capsys):
    monkeypatch.delenv("EODHD_API_KEY", raising=False)
    monkeypatch.delenv("TFB_PROBE_EVIDENCE_DIR", raising=False)
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    http = FakeHTTP()
    assert pp.main(["--spec", json.dumps({"items": [SUI]})], http=http, now=NOW, sleep=lambda _s: None) == 0
    assert all("eodhd.com" not in u for u in http.calls)
    assert "EODHD_API_KEY missing" in capsys.readouterr().out


def test_p4_network_down_is_exit_1_and_scrubbed(monkeypatch, capsys):
    monkeypatch.setenv("EODHD_API_KEY", KEY)
    monkeypatch.delenv("TFB_PROBE_EVIDENCE_DIR", raising=False)
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    assert pp.main(["--spec", json.dumps({"items": [SUI]})], http=FakeHTTP(down=True), now=NOW,
                   sleep=lambda _s: None) == 1
    out = capsys.readouterr().out
    assert KEY not in out and "ConnectionError" in out


def test_p4_bad_spec_exit_2(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text('{"items": [{"row": "X", "kind": "nope"}]}')
    assert pp.main(["--spec", str(bad)], http=FakeHTTP(), now=NOW, sleep=lambda _s: None) == 2


def test_p4_one_retry_on_429():
    slept = []
    http = FakeHTTP(fail_first_429=True)
    p = pp.Prober(http, KEY, NOW, sleep=slept.append)
    r = p.yahoo("BTC-USD")
    assert r["ok"] and len(http.calls) == 2 and slept == [5.0]


# ----------------------------------------------------------------------------- P5
def test_p5_ascii_version_and_workflow():
    src = (_REPO / "scripts" / "provider_identity_probe.py").read_bytes()
    assert all(b < 128 for b in src)
    assert pp.__version__ == "1.0.0"
    vd = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("provider_identity_probe", "1.0.0", "provider identity probe")' in vd
    wf = (_REPO / ".github" / "workflows" / "provider_identity_probe.yml").read_text(encoding="utf-8")
    assert "workflow_dispatch:" in wf and "schedule:" not in wf
    assert "permissions:\n  contents: read" in wf
    assert "EODHD_API_KEY: ${{ secrets.EODHD_API_KEY }}" in wf
    assert "GOOGLE_SHEETS_CREDENTIALS" not in wf                # read-only: no workbook access
    assert "actions/upload-artifact@v7" in wf


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
