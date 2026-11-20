"""scripts/ibkr_history.py v1.0.0 - the broker's own return and trade record for Portfolio_Decision_v2 (owner, 20 Nov
2026: a full review of the file against the market; the page's return and trade record were a month old). Zero
network: a fake Drive session and a fake workbook. Every figure here is made up."""
import datetime as dt
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import ibkr_history as ih  # noqa: E402

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 20, 21, 50, tzinfo=UTC)            # 00:50 Riyadh, 21 Nov
CREATED = dt.datetime(2026, 11, 20, 21, 20, 30, tzinfo=UTC)
D = dt.date


def trade(tid, sym, side, size, when, otype="LIMIT", realized=0.0, ccy="USD", sec="STK", price=10.0, name=None):
    return {"trade_id": tid, "symbol": sym, "company_name": name or (sym + " HOLDINGS INC"), "sec_type": sec,
            "currency": ccy, "side": side, "size": size, "price": price, "order_type": otype, "trade_time": when,
            "commission": 1.0, "net_amount": size * price, "realized_pnl": realized, "order_id": 7}


TRADES = [
    trade("a1", "AAA", "BUY", 60, "2026-08-24T13:30:01Z"), trade("a2", "AAA", "BUY", 40, "2026-08-24T13:30:01Z"),
    trade("a3", "AAA", "SELL", 100, "2026-09-03T13:30:01Z", realized=250.5),                  # limit, a gain: limit TP
    trade("b1", "BBB", "BUY", 50, "2026-09-08T13:55:45Z"),
    trade("b2", "BBB", "SELL", 20, "2026-09-20T14:00:00Z", "STOP", -12.0),
    trade("b3", "BBB", "SELL", 30, "2026-10-22T19:19:56Z", "STOP", -30.0),                    # stop, two sales
    trade("c1", "CCC", "BUY", 10, "2026-11-05T14:30:00Z"),
    trade("c2", "CCC", "SELL", 10, "2026-11-05T16:45:45Z", "MARKET", -5.25),                 # same day
    trade("d1", "DDD", "BUY", 3000, "2026-08-12T06:04:20Z", "MARKET", ccy="SGD", sec="STK", price=1.5),
    trade("d2", "DDD", "SELL", 3000, "2026-08-21T05:26:58Z", "LIMIT", 127.0, ccy="SGD", price=1.55),
    trade("x1", "USD.SGD", "SELL", 3500, "2026-08-12T06:03:06Z", "MARKET", ccy="SGD", sec="CASH", price=1.28),
    trade("x2", "USD.SGD", "BUY", 3600, "2026-08-21T06:00:29Z", "MARKET", ccy="SGD", sec="CASH", price=1.27),
    trade("e1", "EEE", "SELL", 15, "2026-08-07T13:31:04Z", "LIMIT", -8.0),                   # bought before the reads
    trade("f1", "FFF", "BUY", 25, "2026-11-12T14:38:23Z"),                                    # still held
    trade("g1", "GGG", "BUY", 5, "2026-10-01T14:00:00Z"),
    trade("g2", "GGG", "SELL", 5, "2026-10-09T14:00:00Z", "LIMIT", -2.0),                    # limit, a loss
]


def perf(dates=("20261116", "20261117", "20261118", "20261119", "20261120"),
         cps=(0.010, 0.012, 0.008, -0.004, -0.010)):
    ytd = {"start_date": "20260512", "start_nav": 0.0, "cps": list(cps), "nav": [100.0] * len(cps), "dates": list(dates),
           "frequency": "D"}
    return {"portfolio_measure": "TWR", "accounts": {"account": {"base_currency": "USD", "periods": {
        "1D": {"cps": [cps[-1]], "nav": [100.0], "dates": [dates[-1]]},
        "7D": {"cps": list(cps[-3:]), "nav": [100.0] * 3, "dates": list(dates[-3:])},
        "YTD": ytd}}}}


def body(**over):
    b = {"schema": "tfb.ibkr-history/v1", "captured_at": "2026-11-21T00:20:31.5+03:00", "account_alias": "ibkr-primary",
         "producer": "scheduled-task tfb-ibkr-history", "performance": perf(), "trades_90d": {"trades": TRADES[:8]},
         "trades_last_quarter": {"trades": TRADES[8:]}}
    b.update(over)
    return b


# --------------------------------------------------------------------------- the file
def test_parse_reads_the_longest_series_and_every_fill():
    p = ih.parse(body(), CREATED, NOW, "TFB_IBKR_History_20261121-002031.json")
    assert p["ok"] and p["reason"] == "" and round(p["age_h"], 2) == 0.49
    assert p["series"] == [(D(2026, 11, 16), 0.010), (D(2026, 11, 17), 0.012), (D(2026, 11, 18), 0.008),
                           (D(2026, 11, 19), -0.004), (D(2026, 11, 20), -0.010)]           # YTD, not 7D or 1D
    assert len(p["fills"]) == len(TRADES) and {f["trade_id"] for f in p["fills"]} == {t["trade_id"] for t in TRADES}
    f = [x for x in p["fills"] if x["trade_id"] == "b3"][0]
    assert f["time"] == dt.datetime(2026, 10, 22, 19, 19, 56, tzinfo=UTC) and f["side"] == "SELL" and f["realized"] == -30.0


def test_parse_refuses_a_wrong_or_misdated_file():
    assert ih.parse({"schema": "tfb.ibkr-capture/v1"}, CREATED, NOW)["reason"] == "history schema not recognised"
    assert ih.parse(body(captured_at="soon"), CREATED, NOW)["reason"] == "captured_at unreadable"
    assert ih.parse(body(captured_at="2026-11-21T02:00:00+03:00"), None, NOW)["reason"] == "captured_at is in the future"
    late = ih.parse(body(), CREATED + dt.timedelta(minutes=11), NOW)
    assert late["reason"] == "captured_at differs from the file's creation time" and not late["ok"]
    empty = ih.parse(body(performance={}, trades_90d={"trades": []}, trades_last_quarter=None), CREATED, NOW)
    assert empty["reason"] == "no performance series and no trades in the file"
    bad = body(trades_90d={"trades": [{"trade_id": "z", "side": "HOLD", "size": 1, "price": 1,
                                       "trade_time": "2026-11-20T14:30:01Z"}, "junk"]}, trades_last_quarter=None)
    assert ih.parse(bad, CREATED, NOW)["fills"] == []                                       # no usable fill, series kept


def test_day_of_reads_every_date_shape():
    assert ih.day_of("20261120") == ih.day_of("2026-11-20") == ih.day_of("2026-11-20 00:00:00") == D(2026, 11, 20)
    assert ih.day_of(46346) == D(2026, 11, 20) and ih.day_of(dt.datetime(2026, 11, 20, 5)) == D(2026, 11, 20)
    assert ih.day_of("20 Nov") is None and ih.day_of(True) is None and ih.day_of(3.5) is None


# --------------------------------------------------------------------------- the NAV tab
SERIES = [(D(2026, 8, 10), 0.0108), (D(2026, 8, 11), 0.0109), (D(2026, 8, 12), 0.0203), (D(2026, 11, 20), -0.0370)]
START = D(2026, 8, 11)


def test_chain_nav_rebuilds_an_empty_tab_from_the_start_day():
    rows, how = ih.chain_nav([], SERIES, START)
    assert how == "rebuilt from IBKR" and rows == SERIES[1:]                    # the day before the start is not shown


def test_chain_nav_rebases_on_the_first_shared_day_and_drops_the_tabs_later_rows():
    old = [(D(2026, 8, 11), 0.0209), (D(2026, 8, 12), 0.03), (D(2026, 10, 17), 0.0099)]   # 17 Oct: not an IBKR day
    rows, how = ih.chain_nav(old, SERIES, START)
    assert how == "chained at 2026-08-11"
    assert rows[0] == (D(2026, 8, 11), 0.0209) and [d for d, _ in rows] == [D(2026, 8, 11), D(2026, 8, 12), D(2026, 11, 20)]
    k = 1.0209 / 1.0109
    assert abs(rows[1][1] - (k * 1.0203 - 1)) < 1e-12 and abs(rows[2][1] - (k * 0.963 - 1)) < 1e-12
    same, _ = ih.chain_nav([(D(2026, 8, 11), 0.0109)], SERIES, START)          # the same base: IBKR's own figures
    assert [round(v, 10) for _, v in same] == [0.0109, 0.0203, -0.037]


def test_chain_nav_keeps_the_tab_when_it_cannot_be_joined():
    old = [(D(2026, 8, 11), 0.0109)]
    assert ih.chain_nav(old, [], START) == (old, "kept (no IBKR series)")
    late = [(D(2027, 1, 4), 0.01)]
    assert ih.chain_nav(old, late, START) == (old, "kept (IBKR's series shares no day with the tab)")
    assert ih.read_nav([["date", "x"], ["2026-08-11", 0.0109], ["junk", 1], ["2026-08-12"], [46346, "-0.037"]]) == [
        (D(2026, 8, 11), 0.0109), (D(2026, 11, 20), -0.037)]


# --------------------------------------------------------------------------- the fills and the record
def _fills(ts):
    return [ih.fill(t) for t in ts]


def test_round_trips_cover_partial_sales_same_day_trips_unknown_starts_and_open_positions():
    trips = {t["symbol"]: t for t in ih.round_trips(_fills(TRADES))}
    assert sorted(trips) == ["AAA", "BBB", "CCC", "DDD", "EEE", "GGG"]                  # FFF is still held; no FX trip
    assert trips["BBB"]["realized"] == -42.0 and trips["BBB"]["fills"] == 3
    assert trips["EEE"]["start"] is None                                               # sold more than the fills show
    rows = ih.trade_rows(list(trips.values()), ih.fx_rates(_fills(TRADES)))
    assert rows == [
        ["AAA", "AAA Holdings", "USD", "limit TP", "24 Aug→3 Sep", 250.5],
        ["DDD", "DDD Holdings (SGD)", "SGD", "limit TP", "12→21 Aug", round(127.0 / 1.27, 2)],   # nearest rate
        ["GGG", "GGG Holdings", "USD", "limit exit", "1→9 Oct", -2.0],
        ["CCC", "CCC Holdings", "USD", "market exit", "5 Nov", -5.25],
        ["EEE", "EEE Holdings", "USD", "limit exit", "?→7 Aug", -8.0],
        ["BBB", "BBB Holdings", "USD", "stop", "8 Sep→22 Oct", -42.0]]


def test_a_trip_in_another_currency_stays_in_it_without_a_conversion():
    rows = ih.trade_rows(ih.round_trips(_fills(TRADES[8:10])), {})
    assert rows == [["DDD", "DDD Holdings (SGD)", "SGD", "limit TP", "12→21 Aug", 127.0]]


def test_merge_keeps_one_fill_per_id_the_later_read_winning():
    old = _fills(TRADES[:3])
    newer = dict(TRADES[2], realized_pnl=251.0)
    merged = ih.merge_fills(old, _fills([newer] + TRADES[3:5]))
    assert [f["trade_id"] for f in merged] == ["a1", "a2", "a3", "b1", "b2"]
    assert [f["realized"] for f in merged if f["trade_id"] == "a3"] == [251.0]
    back = ih.read_fills([ih.FILL_HEAD] + [ih.fill_row(f) for f in merged])
    assert [(f["trade_id"], f["time"], f["size"], f["realized"]) for f in back] == [
        (f["trade_id"], f["time"], f["size"], f["realized"]) for f in merged]


def test_names_spans_and_exit_types():
    assert ih.tidy_name("DOUBLEDOWN INTERACTIVE -ADR") == "Doubledown Interactive"
    assert ih.tidy_name("INTERNATIONAL BANCSHARES CRP") == "International Bancshares"
    assert ih.tidy_name("SBA COMMUNICATIONS CORP") == "SBA Communications" and ih.tidy_name("") == ""
    t = {"last": {"order_type": "STP LMT"}, "realized": 3.0}
    assert ih.exit_type(t) == "stop" and ih.exit_type({"last": {"order_type": "OTHER"}, "realized": 1}) == "other exit"
    assert ih.span(dt.datetime(2026, 12, 28, tzinfo=UTC), dt.datetime(2027, 1, 5, tzinfo=UTC)) == "28 Dec→5 Jan"


# --------------------------------------------------------------------------- Drive
class Resp:
    def __init__(self, status, payload=None, content=b""):
        self.status_code, self._payload, self.content = status, payload, content

    def json(self):
        return self._payload


class Drive:
    def __init__(self, files, blobs):
        self.files, self.blobs, self.calls = files, blobs, []

    def get(self, url, timeout=None, params=None):
        self.calls.append((url, dict(params or {})))
        if url.endswith("/files"):
            return Resp(200, {"files": self.files})
        fid = url.rsplit("/", 1)[-1]
        return Resp(200, content=self.blobs[fid]) if fid in self.blobs else Resp(404)


def _meta(fid, name, created="2026-11-20T21:20:30Z", size=900):
    return {"id": fid, "name": name, "createdTime": created, "size": str(size)}


def test_latest_files_reads_the_newest_usable_files_and_skips_the_rest():
    files = [_meta("n1", "TFB_IBKR_History_20261121-002031.json"),
             _meta("cap", "TFB_IBKR_Capture_20261121-001000.json"),                    # another kind of file
             _meta("big", "TFB_IBKR_History_20261120-002000.json", size=ih.MAX_BYTES + 1),
             _meta("bad", "TFB_IBKR_History_20261119-002000.json"),
             _meta("old", "TFB_IBKR_History_20261118-002000.json", created="2026-11-17T21:20:30Z")]
    old = body(captured_at="2026-11-18T00:20:31+03:00", trades_90d={"trades": TRADES[:2]}, trades_last_quarter=None)
    drive = Drive(files, {"n1": json.dumps(body()).encode(), "bad": b"{not json", "old": json.dumps(old).encode()})
    r = ih.latest_files(NOW, drive, folder="1WbO2y3xMzPU0lKQ3gHJQ28L19IIAQdNH")
    assert r["ok"] and [f["name"] for f in r["files"]] == ["TFB_IBKR_History_20261121-002031.json",
                                                            "TFB_IBKR_History_20261118-002000.json"]
    assert r["skipped"] == 2 and "big" not in [u.rsplit("/", 1)[-1] for u, _ in drive.calls]
    assert "name contains 'TFB_IBKR_History_'" in drive.calls[0][1]["q"]
    assert ih.latest_files(NOW, drive, folder="")["reason"] == "capture folder not set (TFB_IBKR_CAPTURE_FOLDER_ID)"
    none = ih.latest_files(NOW, Drive([], {}), folder="1WbO2y3xMzPU0lKQ3gHJQ28L19IIAQdNH")
    assert not none["ok"] and none["reason"].startswith("no history file in the folder")


# --------------------------------------------------------------------------- the run
class Sheet:
    def __init__(self, rows=None):
        self.values, self.row_count, self.col_count, self.writes, self.kw = list(rows or []), 200, 30, 0, []

    def get(self, rng, **kw):
        self.kw.append(kw)
        return [list(r) for r in self.values]

    def resize(self, rows=None, cols=None):
        self.row_count, self.col_count = rows or self.row_count, cols or self.col_count

    def update(self, values=None, range_name=None, value_input_option=None):
        self.writes += 1
        self.values = [list(r) for r in values if any(c != "" for c in r)]


class WorksheetNotFound(Exception):
    pass


class Book:
    def __init__(self, tabs):
        self.tabs = dict(tabs)

    def worksheet(self, name):
        if name not in self.tabs:
            raise WorksheetNotFound(name)
        return self.tabs[name]

    def add_worksheet(self, title, rows, cols):
        self.tabs[title] = Sheet()
        return self.tabs[title]


def _found(*bodies):
    return {"ok": True, "reason": "", "skipped": 0,
            "files": [ih.parse(b, None, NOW, "TFB_IBKR_History_%d.json" % i) for i, b in enumerate(bodies)]}


def test_the_run_writes_the_three_tabs_and_logs_no_amount(monkeypatch, capsys):
    monkeypatch.delenv("TFB_IBKR_HISTORY", raising=False)
    monkeypatch.delenv("TFB_IBKR_NAV_FROM", raising=False)
    nav = Sheet([["date", "twr_cum_since_inception"], ["2026-08-11", 0.0109], ["2026-11-16", 0.010]])
    kept = Sheet([ih.FILL_HEAD, ih.fill_row(ih.fill(trade("o1", "OLD", "BUY", 5, "2026-07-28T14:38:49Z"))),
                  ih.fill_row(ih.fill(trade("o2", "OLD", "SELL", 5, "2026-09-08T13:30:01Z", "MARKET", -6.5)))])
    book = Book({"_IBKR_NAV_History": nav, "_IBKR_Fills": kept})
    assert ih.run_once(False, book=book, now=NOW, found=_found(body())) == 0
    assert nav.kw[0] == {"value_render_option": "UNFORMATTED_VALUE", "date_time_render_option": "FORMATTED_STRING"}
    assert nav.values[0] == ["date", "twr_cum_since_inception"] and nav.values[1] == ["2026-08-11", 0.0109]
    assert [r[0] for r in nav.values[1:]] == ["2026-08-11", "2026-11-16", "2026-11-17", "2026-11-18", "2026-11-19",
                                              "2026-11-20"]
    assert nav.values[-1][1] == round(1.010 / 1.010 * 0.990 - 1, 8)                    # joined on 16 Nov
    fills = book.tabs["_IBKR_Fills"].values
    assert fills[0] == ih.FILL_HEAD and len(fills) == 1 + 2 + len(TRADES)
    rec = book.tabs["_IBKR_Trades"].values
    assert rec[0][:6] == ih.TRADE_HEAD and [r[0] for r in rec[1:8]] == ["AAA", "DDD", "GGG", "CCC", "OLD", "EEE", "BBB"]
    assert [r[7:9] for r in rec[:4]] == [["as_of", "2026-11-21 00:20 Riyadh"], ["source", "TFB_IBKR_History_0.json"],
                                         ["fills", 2 + len(TRADES)], ["written", "2026-11-21 00:50 Riyadh"]]
    log = capsys.readouterr().out
    assert "closed trades 7 (2 won)" in log and "fills %d (%d new)" % (2 + len(TRADES), len(TRADES)) in log
    assert "NAV 6 day(s) to 2026-11-20 (chained at 2026-11-16)" in log
    for word in ("AAA", "BBB", "OLD", "250.5", "-42", "0.0109", "3000"):
        assert word not in log


def test_the_newest_file_rules_the_nav_and_older_files_add_their_fills(monkeypatch):
    monkeypatch.delenv("TFB_IBKR_HISTORY", raising=False)
    older = body(captured_at="2026-11-18T00:20:31+03:00", performance=perf(("20261116",), (0.5,)),
                 trades_90d={"trades": [trade("z1", "ZZZ", "BUY", 1, "2026-07-01T14:00:00Z"),
                                        trade("z2", "ZZZ", "SELL", 1, "2026-07-02T14:00:00Z", "STOP", -1.0)]},
                 trades_last_quarter=None)
    book = Book({})
    assert ih.run_once(False, book=book, now=NOW, found=_found(body(), older)) == 0
    assert book.tabs["_IBKR_NAV_History"].values[-1] == ["2026-11-20", -0.01]           # rebuilt from the newest file
    assert "ZZZ" in [r[0] for r in book.tabs["_IBKR_Trades"].values]


def test_dry_run_off_switch_and_no_file_write_nothing(monkeypatch, capsys):
    monkeypatch.delenv("TFB_IBKR_HISTORY", raising=False)
    book = Book({})
    assert ih.run_once(True, book=book, now=NOW, found=_found(body())) == 0 and book.tabs == {}
    assert "dry run: nothing written" in capsys.readouterr().out
    assert ih.run_once(False, book=book, now=NOW, found={"ok": False, "reason": "no history file in the folder"}) == 0
    assert book.tabs == {} and "no history file in the folder: nothing written" in capsys.readouterr().out
    monkeypatch.setenv("TFB_IBKR_HISTORY", "off")
    assert ih.run_once(False, book=book, now=NOW, found=_found(body())) == 0 and book.tabs == {}


def test_a_failed_read_or_write_exits_2(monkeypatch, capsys):
    monkeypatch.delenv("TFB_IBKR_HISTORY", raising=False)

    class Broken(Book):
        def worksheet(self, name):
            raise RuntimeError("503")

    assert ih.run_once(False, book=Broken({}), now=NOW, found=_found(body())) == 2
    assert "nothing written" in capsys.readouterr().out

    class ReadOnly(Sheet):
        def update(self, *a, **k):
            raise RuntimeError("403")

    book = Book({"_IBKR_Fills": ReadOnly(), "_IBKR_NAV_History": Sheet()})
    assert ih.run_once(False, book=book, now=NOW, found=_found(body())) == 2
    assert book.tabs["_IBKR_NAV_History"].writes == 0 and "_IBKR_Fills was not written" in capsys.readouterr().out


def test_nav_from_reads_its_knob(monkeypatch):
    monkeypatch.setenv("TFB_IBKR_NAV_FROM", "2026-09-01")
    assert ih.nav_from() == D(2026, 9, 1)
    monkeypatch.setenv("TFB_IBKR_NAV_FROM", "soon")
    assert ih.nav_from() == D(2026, 8, 11)


def test_a_tab_that_cannot_be_joined_is_left_as_it_is(monkeypatch, capsys):
    monkeypatch.delenv("TFB_IBKR_HISTORY", raising=False)
    monkeypatch.delenv("TFB_IBKR_NAV_FROM", raising=False)
    nav = Sheet([["date", "twr_cum_since_inception"], ["2026-08-11", 0.0109], ["2026-10-17", 0.0099]])
    book = Book({"_IBKR_NAV_History": nav})
    assert ih.run_once(False, book=book, now=NOW, found=_found(body())) == 0
    assert nav.writes == 0 and "_IBKR_Trades" in book.tabs and "_IBKR_Fills" in book.tabs
    log = capsys.readouterr().out
    assert "(kept (IBKR's series shares no day with the tab))" in log
    assert "wrote _IBKR_Fills (%d row(s)), _IBKR_Trades (6 row(s))" % len(TRADES) in log
