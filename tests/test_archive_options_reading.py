"""Tests for scripts/archive_options_reading.py (v1.0.2): the options reading of US stocks and ETFs.

The fixture holds two real Cboe delayed-quote answers read on 22 Nov 2026 (prices of Friday 20 Nov), cut to a
few expiries: TSN (a liquid chain) and SUPN (a thin chain whose own root carries the adjusted 26.87-type strikes
of its merger, beside the SUPN1 root).
"""
from __future__ import annotations

import datetime as dt
import gzip  # noqa: F401 - the module imports it
import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("archive_options_reading", ROOT / "scripts" / "archive_options_reading.py")
op = importlib.util.module_from_spec(spec)
sys.modules["archive_options_reading"] = op
spec.loader.exec_module(op)

FIX = json.loads((ROOT / "tests" / "fixtures" / "cboe_options_sample.json").read_text(encoding="utf-8"))
NOW = dt.datetime(2026, 11, 20, 22, 20, tzinfo=dt.timezone.utc)


def _opt(code, bid, ask, iv, delta, oi=100.0, vol=10.0):
    return {"option": code, "bid": bid, "ask": ask, "iv": iv, "delta": delta, "open_interest": oi, "volume": vol}


def _payload(options, price=100.0, iv30=30.0, day="2026-11-20T15:59:59"):
    return {"timestamp": "2026-11-21 01:00:00", "data": {"options": options, "current_price": price, "iv30": iv30,
                                                       "last_trade_time": day}}


# --------------------------------------------------------------------------- O1
def test_o1_parse_occ():
    assert op.parse_occ("SUPN261218C00027500") == ("SUPN", dt.date(2026, 12, 18), "C", 27.5)
    assert op.parse_occ("SUPN1270617P00050000") == ("SUPN1", dt.date(2027, 6, 17), "P", 50.0)
    assert op.parse_occ("BRK.B261218C00500000")[0] == "BRK.B"
    assert op.parse_occ("nope") is None and op.parse_occ("AAA261340C00010000") is None   # month 13
    assert op.cboe_symbol("BRK.B") == "BRKB" and op.ticker_of("SUPN.US") == "SUPN" and op.ticker_of("SPY") == "SPY"


# --------------------------------------------------------------------------- O2
def test_o2_real_liquid_chain():
    f = op.chain_features(FIX["TSN"], "TSN")
    assert f["snapshot_date"] == "2026-11-20" and f["price"] == 65.12 and f["iv30"] == 25.63
    assert f["front_expiry"] == "2026-12-18" and f["front_days"] == 28
    assert f["atm_iv_30d"] == pytest.approx(25.59, abs=0.01)
    # the straddle prices about 0.8 x sigma x sqrt(t): 25.6 % a year over 28 days is 5.7 %
    assert f["implied_move_pct"] == pytest.approx(5.68, abs=0.01)
    assert f["implied_move_pct"] == pytest.approx(0.8 * f["atm_iv_30d"] * (28 / 365) ** 0.5, rel=0.03)
    assert f["skew_25d_pp"] == pytest.approx(2.37, abs=0.01) and f["cp_iv_spread_pp"] == pytest.approx(-0.19, abs=0.01)
    assert f["adjusted_left_out"] == 0 and f["contracts_used"] == 30
    assert f["pc_volume_ratio"] == pytest.approx(f["put_volume"] / f["call_volume"], abs=0.001)


# --------------------------------------------------------------------------- O3
def test_o3_real_thin_and_adjusted_chain():
    f = op.chain_features(FIX["SUPN"], "SUPN")
    # Cboe's own 30-day volatility stays; the readings derived from quotes do not, on a chain this thin
    assert f["iv30"] == 61.89 and f["call_oi"] + f["put_oi"] < op.MIN_OI
    assert f["atm_iv_30d"] is None and f["implied_move_pct"] is None and f["skew_25d_pp"] is None
    assert f["cp_iv_spread_pp"] is None
    # the merger's odd strikes on the SUPN root and the SUPN1 root are both left out
    odd = [x for x in FIX["SUPN"]["data"]["options"] if op.parse_occ(x["option"])[0] == "SUPN"
           and round(op.parse_occ(x["option"])[3] * 1000) % 500]
    assert odd and f["adjusted_left_out"] >= len(odd)


# --------------------------------------------------------------------------- O4
def test_o4_synthetic_chain():
    opts = []
    for d, ymd in ((5, "261125"), (33, "261223"), (91, "270219")):
        for k, cd, pd in ((80, 0.9, -0.08), (90, 0.75, -0.22), (95, 0.6, -0.4), (100, 0.5, -0.5), (105, 0.4, -0.6),
                          (110, 0.25, -0.75), (120, 0.1, -0.9)):
            civ, piv = 0.30 + (0.02 if k == 100 else 0.0), 0.34 if k < 100 else 0.30
            opts.append(_opt(f"XYZ{ymd}C{k * 1000:08d}", max(0, 100 - k) + 1.0, max(0, 100 - k) + 1.2, civ, cd))
            opts.append(_opt(f"XYZ{ymd}P{k * 1000:08d}", max(0, k - 100) + 1.0, max(0, k - 100) + 1.2, piv, pd))
    opts.append(_opt("XYZ1261223C00100000", 3, 4, 0.9, 0.5, oi=5000))          # adjusted root: never counted
    opts.append(_opt("XYZ261223C00100870", 3, 4, 0.9, 0.5, oi=5000))           # odd strike: never counted
    opts.append(_opt("XYZ261223C00200000", 0.05, 0.1, 0.9, 0.01, oi=5000))     # outside the band
    f = op.chain_features(_payload(opts), "XYZ")
    assert f["front_expiry"] == "2026-11-25" and f["front_days"] == 5
    assert f["adjusted_left_out"] == 2 and f["contracts_used"] == 42 and f["call_oi"] == 2100.0
    assert f["atm_iv_30d"] == pytest.approx((32 + 30) / 2) and f["cp_iv_spread_pp"] == pytest.approx(2.0)
    assert f["atm_iv_90d"] == pytest.approx(31.0)
    # 25-delta: the call at 110 (delta 0.25, iv 30) and the put at 90 (delta -0.22, iv 34)
    assert f["skew_25d_pp"] == pytest.approx(4.0)
    assert f["implied_move_pct"] == pytest.approx(2.2)       # (1.1 + 1.1) / 100
    assert f["iv30"] == 30.0
    # no iv30 from Cboe: the ATM volatility of the 30-day expiry stands in
    f2 = op.chain_features(_payload(opts, iv30=None), "XYZ")
    assert f2["iv30"] == pytest.approx(31.0)
    # nothing usable
    assert op.chain_features(_payload([]), "XYZ") is None
    assert op.chain_features({"data": {"options": opts}}, "XYZ") is None             # no price, no day


# --------------------------------------------------------------------------- O5
def test_o5_finra_and_rows():
    text = ("Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n"
            "20261120|TSN|1200|10|3000|B,Q,N\n20261120|SPY|500000|0|1000000|B,Q,N\n20261120|BAD|x|0|0|N\n")
    fin = op.parse_finra(text)
    assert fin == {"TSN": (1200.0, 3000.0), "SPY": (500000.0, 1000000.0)}
    assert op.parse_finra("<html>no file</html>") == {}
    tsn = op.chain_features(FIX["TSN"], "TSN")
    other = dict(tsn, snapshot_date="2026-11-19")
    names = [("TSN.US", False), ("OLD.US", False), ("GONE.US", False), ("SPY", True)]
    day, rows, left = op.build_rows(names, {"TSN.US": tsn, "OLD.US": other, "GONE.US": None,
                                            "SPY": dict(tsn, price=600.0)},
                                    dt.date(2026, 11, 20), fin, "2026-11-20T22:20:00.000000Z", "42")
    assert day == "2026-11-20" and left == 1 and [r["symbol"] for r in rows] == ["TSN.US", "SPY"]
    r = rows[0]
    assert r["underlying"] == "TSN" and r["is_etf"] is False and r["short_ratio_pct"] == 40.0
    assert r["short_date"] == "2026-11-20" and r["source"] == op.SOURCE and r["run_id"] == "42"
    assert set(r) == {f["name"] for f in op.SCHEMA}
    assert rows[1]["is_etf"] is True and rows[1]["short_ratio_pct"] == 50.0


# --------------------------------------------------------------------------- O6
def test_o6_finra_walks_back_to_a_posted_file():
    asked = []

    def get(url):
        asked.append(url)
        if url.endswith("20261119.txt"):
            return b"Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n20261119|TSN|1|0|2|Q\n"
        raise OSError("404")

    day, got = op.read_finra(dt.date(2026, 11, 20), get)
    assert day == dt.date(2026, 11, 19) and got == {"TSN": (1.0, 2.0)}
    assert asked[0].endswith("20261120.txt")
    day, got = op.read_finra(dt.date(2026, 11, 23), lambda u: (_ for _ in ()).throw(OSError("404")))
    assert day is None and got == {}


# --------------------------------------------------------------------------- O7
class _Resp:
    def __init__(self, code, body=None, headers=None):
        self.status_code, self._b, self.headers = code, body or {}, headers or {}

    def json(self):
        return self._b


class _Session:
    def __init__(self, table=None):
        self.table, self.posts, self.puts = table, [], []

    def get(self, url, timeout=None):
        if "/tables/options_daily" in url:
            return _Resp(200, self.table) if self.table else _Resp(404)
        if "/jobs/" in url:
            return _Resp(200, {"status": {"state": "DONE"}})
        return _Resp(200, {})

    def post(self, url, json=None, timeout=None, headers=None):  # noqa: A002
        self.posts.append((url, json))
        if url.endswith("/queries"):
            return _Resp(200, {"jobComplete": True, "schema": {"fields": [{"name": "symbol"}, {"name": "why"}]},
                               "rows": [{"f": [{"v": "TSN.US"}, {"v": "held"}]}, {"f": [{"v": "SPY"}, {"v": "large"}]}]})
        if "uploadType=resumable" in url:
            return _Resp(200, {}, {"Location": "https://upload/x"})
        return _Resp(200, {})

    def put(self, url, data=None, timeout=None, headers=None):
        self.puts.append(data)
        return _Resp(200, {"status": {"state": "DONE"}, "statistics": {"load": {"outputRows": "2"}}})


def _get_factory(fail=()):
    def get(url):
        if "finra" in url:
            return b"Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n20261120|TSN|1200|0|3000|Q\n"
        sym = url.rsplit("/", 1)[1][:-5]
        if sym in fail:
            raise OSError("HTTP Error 404")
        return json.dumps(FIX["TSN"]).replace('"TSN', '"' + sym).encode()
    return get


def test_o7_main_dry_run_write_and_gate(monkeypatch, capsys):
    for k, v in (("TFB_BQ_OPTIONS", "on"), ("TFB_BQ_PROJECT", "p"), ("TFB_BQ_DATASET", "d"), ("GITHUB_RUN_ID", "7")):
        monkeypatch.setenv(k, v)
    sess = _Session()
    assert op.main(["--dry-run", "--symbols", "TSN,SPY"], session=sess, now=NOW, get=_get_factory(), sleep=lambda s: None) == 0
    out = capsys.readouterr().out
    assert "2/2 chains read; 2 rows for 2026-11-20" in out and "dry run" in out and not sess.puts
    assert "TSN" not in out.replace("tsn", "")                        # counts only: no symbol in the log
    sess = _Session()
    assert op.main(["--write"], session=sess, now=NOW, get=_get_factory(), sleep=lambda s: None) == 0
    out = capsys.readouterr().out
    assert "loaded 2" in out and "table created" in out
    created = [j for u, j in sess.posts if u.endswith("/tables")][0]
    assert created["timePartitioning"] == {"type": "DAY", "field": "snapshot_date"}
    load = [j for u, j in sess.posts if "uploadType=resumable" in u][0]["configuration"]["load"]
    assert load["destinationTable"]["tableId"] == "options_daily$20261120" and load["writeDisposition"] == "WRITE_TRUNCATE"
    rows = [json.loads(l) for l in sess.puts[0].decode().splitlines()]
    assert {r["symbol"] for r in rows} == {"TSN.US", *op.ETFS}        # the names from BigQuery, then the fixed ETFs
    q = [j for u, j in sess.posts if u.endswith("/queries")][0]["query"]
    assert "v_my_portfolio" in q and "LIMIT 900" in q and "INVESTABLE" in q
    # fewer than half the names read: a failure, nothing loaded
    sess = _Session()
    assert op.main(["--write", "--symbols", "AAA,BBB,TSN"], session=sess, now=NOW,
                   get=_get_factory(fail=("AAA", "BBB")), sleep=lambda s: None) == 1
    assert not sess.puts
    # the gate
    monkeypatch.setenv("TFB_BQ_OPTIONS", "off")
    assert op.main(["--write"], session=_Session(), now=NOW, get=_get_factory()) == 0
    assert "nothing read" in capsys.readouterr().err
    monkeypatch.setenv("TFB_BQ_OPTIONS", "on")
    assert op.main(["--max", "0"], session=_Session(), now=NOW, get=_get_factory()) == 2


# --------------------------------------------------------------------------- O8
def test_o8_existing_table_checked():
    good = {"schema": {"fields": [dict(f) for f in op.SCHEMA]}, "timePartitioning": {"type": "DAY", "field": "snapshot_date"}}
    assert op.schema_problems(good) == []
    bad = {"schema": {"fields": [dict(f) for f in op.SCHEMA[:-1]]}, "timePartitioning": {"type": "DAY", "field": "captured_at"}}
    p = op.schema_problems(bad)
    assert len(p) == 2 and "columns" in p[0] and "partitioned" in p[1]
    names = [f["name"] for f in op.SCHEMA]
    assert len(names) == len(set(names)) == 30 and names[:2] == ["snapshot_date", "symbol"]


def test_o9_misses_are_counted_by_reason_never_named():
    """v1.0.1: a failed run says why the chains were missed (counts only), and the Cboe request carries the
    browser headers Cboe's own pages send."""
    import io
    import urllib.error
    assert op.__version__ == "1.0.2"
    e403 = urllib.error.HTTPError("u", 403, "Forbidden", {}, io.BytesIO(b""))
    e500 = urllib.error.HTTPError("u", 500, "x", {}, io.BytesIO(b""))
    assert [op.miss_reason(e) for e in (e403, e500, ValueError("bad json"), OSError("reset"))] == [
        "http 403", "other http", "unreadable", "network"]
    misses, waits = [], []
    def deny(url):
        raise e403
    assert op.read_chain("AAA", deny, waits.append, misses) is None and misses == ["http 403"] and waits == []
    def flaky(url):
        raise OSError("reset")
    assert op.read_chain("AAA", flaky, waits.append, misses) is None and misses[-1] == "network" and waits == [2.0]   # two tries, one wait
    empty = lambda url: json.dumps({"data": {"options": []}}).encode()
    assert op.read_chain("AAA", empty, waits.append, misses) is None and misses[-1] == "no usable chain"   # #1060 review
    assert waits == [2.0]                                                      # an empty answer is not retried
    assert op.CBOE_HEADERS["Origin"] == "https://www.cboe.com" and "Mozilla/5.0 (Windows" in op.UA


def test_o10_rate_limit_waits_and_retries():
    """v1.0.2: a 429 waits 5, 10 and 20 seconds between four tries; a later success is kept; three readers."""
    import io
    import urllib.error
    calls, waits = [], []
    good = FIX["TSN"]
    def limited(url):
        calls.append(url)
        if len(calls) < 3:
            raise urllib.error.HTTPError("u", 429, "Too Many Requests", {}, io.BytesIO(b""))
        return json.dumps(good).encode()
    got = op.read_chain("TSN", limited, waits.append, [])
    assert got and got["iv30"] == op.chain_features(good, "TSN")["iv30"] and waits == [5.0, 10.0] and len(calls) == 3
    misses, waits = [], []
    def always(url):
        raise urllib.error.HTTPError("u", 429, "Too Many Requests", {}, io.BytesIO(b""))
    assert op.read_chain("TSN", always, waits.append, misses) is None
    assert waits == [5.0, 10.0, 20.0] and misses == ["http 429"] and op.WORKERS == 3
