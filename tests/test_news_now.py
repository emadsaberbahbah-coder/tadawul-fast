"""tests/test_news_now.py - scripts/news_now.py 1.1.0 (the hourly news and price reading, observe only)."""
from __future__ import annotations

import datetime as dt
import io
import json
import math
from pathlib import Path

import pytest
import yaml

from scripts import news_now as nn
from tests.test_news_board import Book, Sheet

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 24, 16, 35, tzinfo=UTC)        # Tue 24 Nov 2026, 11:35 New York, 19:35 Riyadh
NOW_TS = int(NOW.timestamp())
DS = "proj-1.tfb_archive"
PORTFOLIO = [["Symbol", "Name"], ["AAA", "Alpha"], ["BBB", "Bravo"], ["GC=F", "Gold"], ["TOTAL", ""]]
NOT_FOUND = "BigQuery refused the query (HTTP 404): Not found: Table proj-1:tfb_archive.v_symbol_news_class_now"


def sessions(n, end="2026-11-23"):
    """n weekday dates ending on `end`."""
    d, out = dt.date.fromisoformat(end), []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d -= dt.timedelta(days=1)
    return out[::-1]


def chart(closes, dates, live=False, price=None, gmtoffset=-18000):
    """A Yahoo daily chart: one bar per date (16:00 New York), the regular session of 24 Nov around NOW."""
    start = int(dt.datetime(2026, 11, 24, 14, 30, tzinfo=UTC).timestamp())
    end = int(dt.datetime(2026, 11, 24, 21, 0, tzinfo=UTC).timestamp())
    if not live:                                           # a session that has not started yet
        start, end = start + 86400, end + 86400
    ts = [int(dt.datetime.fromisoformat(d + "T21:00:00+00:00").timestamp()) - 6 * 3600 for d in dates]
    if live:
        ts.append(start + 3600)
        closes = list(closes) + [price]
    meta = {"gmtoffset": gmtoffset, "regularMarketPrice": price,
            "currentTradingPeriod": {"regular": {"start": start, "end": end}}}
    return {"chart": {"result": [{"meta": meta, "timestamp": ts,
                                  "indicators": {"quote": [{"close": closes, "open": closes, "high": closes,
                                                            "low": closes, "volume": [1000] * len(closes)}],
                                                 "adjclose": [{"adjclose": closes}]}}]}}


DATES = sessions(60)
SPY_CLOSES = [500 * (1 + 0.001 * ((i % 3) - 1)) for i in range(60)]                 # small wiggles
AAA_CLOSES = [100 * (1 + 0.002 * i) * (1 + 0.003 * ((i % 2) * 2 - 1)) for i in range(60)]   # rising, noisy
BBB_CLOSES = [50 * (1 - 0.002 * i) * (1 + 0.003 * ((i % 2) * 2 - 1)) for i in range(60)]    # falling, noisy


class Opened(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class Net:
    """Yahoo and EODHD by URL; records the paths (never the token)."""

    def __init__(self, live=True, spy_move=0.0, aaa_move=0.08, bbb_move=0.0, news=None, fail_news=(), used=1000):
        self.live, self.urls, self.fail_news, self.used = live, [], set(fail_news), used
        self.moves = {"SPY": spy_move, "AAA": aaa_move, "BBB": bbb_move}
        self.news = news if news is not None else {"AAA.US": [{"date": "2026-11-24T12:00:00+00:00",
                                                               "title": "Alpha Bank raises full-year guidance",
                                                               "link": "https://x.test/a1", "sentiment": {"polarity": 0.9}}],
                                                   "BBB.US": [], "EEE.US": []}

    def __call__(self, req, timeout):
        url = req.full_url
        self.urls.append(url.split("api_token=")[0])
        if "eodhd.com/api/user" in url:
            return Opened(json.dumps({"apiRequests": self.used, "dailyRateLimit": 100000,
                                      "apiRequestsDate": "2026-11-24"}).encode())
        if "eodhd.com/api/news" in url:
            sym = url.split("s=")[1].split("&")[0]
            if sym in self.fail_news:
                raise OSError("boom")
            return Opened(json.dumps(self.news.get(sym, [])).encode())
        if "finance.yahoo.com" in url:
            sym = url.split("/chart/")[1].split("?")[0]
            base = {"SPY": SPY_CLOSES, "AAA": AAA_CLOSES, "BBB": BBB_CLOSES}.get(sym)
            if base is None:
                err = OSError("404")
                err.code = 404
                raise err
            last = base[-1] * (1 + self.moves[sym])
            return Opened(json.dumps(chart(base, DATES, self.live, last if self.live else None)).encode())
        raise AssertionError(url)


CLASSIFIED = [
    {"symbol": "AAA.US", "news_date": "2026-11-24", "published_at": "2026-11-24 12:00:00+00",
     "title": "Alpha Bank raises full-year guidance", "event_type": "GUIDANCE_UP", "score": "2.0"},
    {"symbol": "AAA.US", "news_date": "2026-11-20", "published_at": "2026-11-20 12:00:00+00",
     "title": "old news", "event_type": "OTHER", "score": "1.0"},
    {"symbol": "EEE.US", "news_date": "2026-11-24", "published_at": "2026-11-24 10:00:00+00",
     "title": "Echo opinion piece", "event_type": "OPINION", "score": "0.0"},
]
UNIVERSE = [{"symbol": "AAA.US", "name": "Alpha Bank", "recommendation": "STRONG SELL"},
            {"symbol": "BBB.US", "name": "Bravo", "recommendation": "HOLD"},
            {"symbol": "EEE.US", "name": "Echo", "recommendation": "BUY"}]


OLD_TODAY = [  # today's partition before the run: an item pushed off the provider's first page, and a repeat
    {"snapshot_date": "2026-11-24", "symbol": "AAA.US", "published_at": "2026-11-24T08:00:00Z", "title": "Alpha early item",
     "link": "https://x.test/a0", "domain": "x.test", "polarity": "0.5", "neg": None, "neu": None, "pos": None,
     "tags": None, "captured_at": "2026-11-24T13:35:00Z", "run_id": "70"},
    {"snapshot_date": "2026-11-24", "symbol": "AAA.US", "published_at": "2026-11-24T12:00:00Z",
     "title": "Alpha Bank raises full-year guidance", "link": "https://x.test/a1", "domain": "x.test", "polarity": "0.9",
     "neg": None, "neu": None, "pos": None, "tags": None, "captured_at": "2026-11-24T13:35:00Z", "run_id": "70"}]


class Query:
    def __init__(self, classified=CLASSIFIED, board=("EEE.US", "AAA.US", "1030.SR"), class_view=True, old=None,
                 table=True, snap_age=None):
        self.sql, self.classified, self.board, self.class_view = [], classified, board, class_view
        self.old, self.table, self.snap_age = list(old or []), table, snap_age

    def __call__(self, session, project, location, sql):
        self.sql.append(sql)
        if "AS age_min FROM `" in sql:                                    # v1.1.0: the universe snapshot's age
            if self.snap_age is None:
                raise RuntimeError("BigQuery refused the query (HTTP 404): Not found: Table snap_universe_latest")
            return [{"age_min": str(self.snap_age)}]
        if ".symbol_news_intraday`" in sql:
            if not self.table:
                raise RuntimeError("BigQuery refused the query (HTTP 404): Not found: Table proj-1:tfb_archive.x")
            return [dict(r) for r in self.old]
        if "v_top10_board" in sql:
            return [{"symbol": s, "best": str(i + 1)} for i, s in enumerate(self.board)]
        if "v_symbol_news_class_now" in sql:
            if not self.class_view:
                raise RuntimeError(NOT_FOUND)
            return [dict(r) for r in self.classified]
        if "_universe_latest`" in sql:
            return [dict(r) for r in UNIVERSE]
        raise AssertionError(sql)


class BQ:
    def __init__(self, fail_load=None):
        self.ensured, self.loads, self.fail_load = [], [], fail_load

    def ensure(self, session, project, dataset, table, spec=None):
        assert spec is not None
        self.ensured.append(table)
        return False

    def load(self, session, project, dataset, location, table, day, rows, run_id, schema=None,
             disposition="WRITE_TRUNCATE"):
        if self.fail_load == table:
            raise RuntimeError("load refused")
        names = [f["name"] for f in schema]
        for r in rows:                                     # every row fits its schema
            assert set(r) <= set(names), set(r) - set(names)
            for f in schema:
                if f["mode"] == "REQUIRED":
                    assert r.get(f["name"]) is not None, f["name"]
        self.loads.append({"table": table, "day": day, "rows": rows, "disposition": disposition})
        return len(rows)


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for k in ("TFB_NEWS_NOW", "TFB_NEWS_NOW_EODHD", "TFB_DEEP_EODHD_MAX_SHARE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("TFB_BQ_PROJECT", "proj-1")
    monkeypatch.setenv("EODHD_API_KEY", "SECRET-TOKEN")
    monkeypatch.setenv("GITHUB_RUN_ID", "77")
    monkeypatch.setattr(nn, "_SLEEP", lambda s: None)


def run(net=None, query=None, bq=None, dry=False, book=None):
    net, query, bq = net or Net(), query or Query(), bq or BQ()
    book = book or Book({"My_Portfolio": Sheet(PORTFOLIO)})
    rc = nn.run_once(dry, book=book, session=object(), now=NOW, query=query, opener=net, sleep=lambda s: None,
                     ensure=bq.ensure, load=bq.load)
    return rc, book, net, query, bq


# ----------------------------------------------------------------------------- pure parts
def test_names_holdings_first_then_board():
    names = nn.pick_names([{"symbol": "AAA"}, {"symbol": "GC=F"}, {"symbol": "BP.L"}, {"symbol": "AAA"}],
                          ["EEE.US", "AAA.US", "bad sym", "1030.SR"])
    assert names == [("AAA.US", "holding"), ("BP.L", "holding"), ("EEE.US", "Top 10"), ("1030.SR", "Top 10")]
    assert len(nn.pick_names([{"symbol": "A%d" % i} for i in range(50)], [])) == nn.MAX_NAMES


def test_chart_reading_open_closed_and_before_the_open():
    live = nn.chart_reading(chart(AAA_CLOSES, DATES, live=True, price=110.0), NOW_TS)
    assert live["state"] == "open" and live["session_date"] == "2026-11-24" and live["prev_date"] == "2026-11-23"
    assert live["price"] == 110.0 and live["prev_close"] == pytest.approx(AAA_CLOSES[-1]) and len(live["bars"]) == 60
    pre = nn.chart_reading(chart(AAA_CLOSES, DATES), NOW_TS)
    assert pre["state"] == "pre" and pre["session_date"] is None and pre["prev_date"] == "2026-11-23"
    closed_dates = DATES[1:] + ["2026-11-24"]
    closed = nn.chart_reading(chart(AAA_CLOSES, closed_dates), NOW_TS)
    assert closed["state"] == "closed" and closed["session_date"] == "2026-11-24" and closed["prev_date"] == "2026-11-23"
    assert closed["price"] == pytest.approx(AAA_CLOSES[-1]) and closed["prev_close"] == pytest.approx(AAA_CLOSES[-2])
    assert nn.chart_reading({"chart": {"result": []}}, NOW_TS) is None and nn.chart_reading(None, NOW_TS) is None


def test_price_answer_matches_the_nightly_formulas():
    stock = nn.chart_reading(chart(AAA_CLOSES, DATES, live=True, price=AAA_CLOSES[-1] * 1.05), NOW_TS)
    spy = nn.chart_reading(chart(SPY_CLOSES, DATES, live=True, price=SPY_CLOSES[-1] * 1.01), NOW_TS)
    a = nn.price_answer(stock, spy)
    xs = [math.log(AAA_CLOSES[i] / AAA_CLOSES[i - 1]) - math.log(SPY_CLOSES[i] / SPY_CLOSES[i - 1]) for i in range(1, 60)]
    w = xs[-20:]
    m = sum(w) / 20
    sd = math.sqrt(sum((x - m) ** 2 for x in w) / 19)
    assert a["daily_vol_pp"] == pytest.approx(sd * 100, abs=1e-3)
    assert a["change_pct"] == pytest.approx(5.0) and a["spy_change_pct"] == pytest.approx(1.0)
    assert a["reaction_pp"] == pytest.approx(4.0)
    assert a["reaction_z"] == pytest.approx(4.0 / (sd * 100 * math.sqrt(2)), abs=1e-3)
    assert a["day_z"] == pytest.approx((math.log(1.05) - math.log(1.01)) / sd, abs=1e-3)
    assert a["trend"] == "up" and nn.price_answer(stock, None)["day_z"] is None
    pre = nn.price_answer(nn.chart_reading(chart(AAA_CLOSES, DATES), NOW_TS), nn.chart_reading(chart(SPY_CLOSES, DATES), NOW_TS))
    assert pre["daily_vol_pp"] is not None and pre["day_z"] is None and pre["reaction_pp"] is None   # no move before the open


def test_trend_rules():
    assert nn.trend_of([1.0] * 39) == "unknown"
    assert nn.trend_of([float(i) for i in range(1, 61)]) == "up"
    assert nn.trend_of([float(i) for i in range(60, 0, -1)]) == "down"
    assert nn.trend_of([10.0] * 59 + [11.0]) == "up" and nn.trend_of([10.0] * 60) == "mixed"


def test_state_rules_are_the_nightly_case():
    assert nn.news_state(None, 3, 2.0, 5.0, 5.0, 0) == "quiet"
    assert nn.news_state(1.5, 1, 2.0, 1.0, 1.4, 0) == "good news, price agreed"
    assert nn.news_state(1.5, 1, -2.0, -1.0, -1.4, 0) == "bad news, price agreed"
    assert nn.news_state(1.5, 1, 2.0, -1.2, -1.4, 0) == "good news, price fell"
    assert nn.news_state(1.5, 1, -2.0, 1.2, 1.4, 0) == "bad news, price rose"
    assert nn.news_state(1.5, 1, 1.0, 0.5, 0.5, 0) == "news, price quiet"
    assert nn.news_state(1.5, 1, 1.0, None, None, 0) == "news, price quiet"           # before the open
    assert nn.news_state(1.5, 2, 0.0, -2.5, -3.0, 0) == "unsigned news, big move"
    assert nn.news_state(1.5, 2, 0.0, 0.3, 0.3, 0) == "routine news"
    assert nn.news_state(1.5, 0, 0.0, 2.0, 2.6, 0) == "big rise, no news"
    assert nn.news_state(1.5, 0, 0.0, -2.0, -2.6, 0) == "big fall, no news"
    assert nn.news_state(1.5, 0, 0.0, 2.0, 2.6, 1) == "quiet"                         # news the session before
    assert [nn.expected_of(s, sc, rz) for s, sc, rz in (("good news, price fell", 2, -1), ("news, price quiet", -1, 0),
                                                         ("unsigned news, big move", 0, 2.4), ("quiet", 0, 0),
                                                         ("big fall, no news", 0, -3))] == [-1, -1, 1, 0, 1]
    assert nn.trend_fit(1, "up") == "with the trend" and nn.trend_fit(-1, "up") == "against the trend"
    assert nn.trend_fit(0, "up") == nn.trend_fit(1, "mixed") == "no clear trend"


def test_stance_and_agreement_follow_the_understanding_view():
    assert [nn.tfb_stance(r) for r in ("Strong Buy", "strong-sell", "REDUCE", "HOLD", None, "")] == [1, -1, -1, 0, None, None]
    assert nn.agreement(0, 1) == "no lean from the news" and nn.agreement(1, None) == "TFB has no view today"
    assert nn.agreement(-1, 0) == "TFB holds, the news leans down" and nn.agreement(1, 1) == "agrees with TFB"
    assert nn.agreement(1, -1) == "leans against TFB"


# ----------------------------------------------------------------------------- a full run
def test_a_full_run_reads_writes_and_logs_counts_only(capsys):
    rc, book, net, query, bq = run()
    assert rc == 0
    out = capsys.readouterr().out
    # the public log: counts only - no holding, no token
    for word in ("AAA", "Alpha", "BBB", "Bravo", "SECRET-TOKEN"):
        assert word not in out, word
    assert "names: 4 (2 holding(s), 2 Top 10 name(s) not held)" in out
    assert not any("SECRET" in u for u in net.urls)
    # EODHD: the budget first, then one news call per name, the last two days
    eod = [u for u in net.urls if "eodhd.com" in u]
    assert "api/user" in eod[0] and len(eod) == 5 and all("from=2026-11-22" in u for u in eod[1:])
    # BigQuery: the headlines replace today's partition; the log is appended
    assert bq.ensured == ["symbol_news_intraday", "news_now_log"]
    news, log = bq.loads
    assert news["table"] == "symbol_news_intraday" and news["day"] == "2026-11-24" and news["disposition"] == "WRITE_TRUNCATE"
    assert [r["symbol"] for r in news["rows"]] == ["AAA.US"] and news["rows"][0]["run_id"] == "77"
    assert log["table"] == "news_now_log" and log["disposition"] == "WRITE_APPEND" and len(log["rows"]) == 4
    by = {r["symbol"]: r for r in log["rows"]}
    a = by["AAA.US"]
    assert a["listed_as"] == "holding" and a["session_state"] == "open" and a["news_items"] == 1 and a["news_score"] == 2.0
    assert a["state"] == "good news, price agreed" and a["expected"] == 1 and a["trend"] == "up"
    assert a["recommendation"] == "STRONG SELL" and a["agreement"] == "leans against TFB" and a["trend_fit"] == "with the trend"
    assert a["top_headline"] == "Alpha Bank raises full-year guidance"           # the 20 Nov item is outside the window
    assert by["BBB.US"]["state"] == "quiet" and by["BBB.US"]["news_items"] == 0 and by["BBB.US"]["trend"] == "down"
    e = by["EEE.US"]                                                             # Top 10, no Yahoo price this run
    assert e["listed_as"] == "Top 10" and e["state"] is None and e["news_items"] == 1
    s = by["1030.SR"]
    assert s["state"] is None and s["price"] is None
    # the page
    page = book.tabs["News_Now"].values
    assert page[0][0].startswith("NEWS NOW - ") and "SHADOW: analysis only" in page[1][0] and "news_now 1.1.0" in page[1][0]
    assert page[2][0] == "US session: open | prices at 2026-11-24 11:35 New York"
    assert page[3][0].startswith("NEWS NOW: 4 name(s) read (2 US with a state); 2 with company news since the last "
                                 "session, 1 with a lean for the next sessions, 1 leaning against TFB's view")
    head = next(i for i, r in enumerate(page) if r[0] == "Name")
    rows = {r[0].split(" - ")[0]: r for r in page[head + 1:head + 5]}
    assert rows["AAA.US"][:4] == ["AAA.US - Alpha Bank", "holding", rows["AAA.US"][2], rows["AAA.US"][3]]
    assert rows["AAA.US"][2] == "+8.00 %" and rows["AAA.US"][5] == "1, score +2.0"
    assert rows["AAA.US"][6:11] == ["good news, price agreed", "up against SPY", "up", "STRONG SELL", "LEANS AGAINST TFB"]
    assert rows["EEE.US"][6] == "no price from Yahoo this run"
    assert rows["1030.SR"][6] == "news only: the state is read for US names against SPY"
    assert all(len(r) == nn.WIDTH for r in page)


def test_the_day_is_merged_never_cut_to_the_first_page():
    """Codex P2 on #1072: today's partition is this run's items plus every earlier item of the day it does not repeat."""
    rc, book, net, query, bq = run(query=Query(old=OLD_TODAY))
    news = bq.loads[0]
    assert news["table"] == "symbol_news_intraday" and news["disposition"] == "WRITE_TRUNCATE"
    assert [r["title"] for r in news["rows"]] == ["Alpha Bank raises full-year guidance", "Alpha early item"]
    assert news["rows"][0]["run_id"] == "77" and news["rows"][1]["run_id"] == "70"   # the new copy wins a repeat
    assert news["rows"][1]["polarity"] == 0.5 and news["rows"][1]["published_at"] == "2026-11-24T08:00:00Z"
    sql = [q for q in query.sql if "symbol_news_intraday" in q][0]
    assert "WHERE snapshot_date = DATE '2026-11-24'" in sql and "FORMAT_TIMESTAMP" in sql
    rc, book, net, query, bq = run(query=Query(table=False))                     # the first run: no table to read
    assert rc == 0 and len(bq.loads[0]["rows"]) == 1


def test_a_failed_call_is_not_read_as_no_news(capsys):
    """Codex P2 on #1072: a name EODHD did not answer is 'not refreshed' and gets no state; the others still load."""
    rc, book, net, query, bq = run(net=Net(fail_news={"BBB.US"}, bbb_move=0.2), query=Query(old=OLD_TODAY))
    assert rc == 0 and [ld["table"] for ld in bq.loads] == ["symbol_news_intraday", "news_now_log"]
    assert "1 call(s) failed" in capsys.readouterr().out
    by = {r["symbol"]: r for r in bq.loads[-1]["rows"]}
    assert by["BBB.US"]["news_coverage"] == "not refreshed" and by["BBB.US"]["state"] is None   # not 'big rise, no news'
    assert by["AAA.US"]["news_coverage"] == "fresh" and by["AAA.US"]["state"] == "good news, price agreed"
    page = book.tabs["News_Now"].values
    assert any("1 news call(s) failed" in str(r[0]) for r in page)
    bbb = [r for r in page if str(r[0]).startswith("BBB.US")][0]
    assert bbb[5] == "none (not refreshed)" and bbb[6] == "company news not refreshed this run: no state"


def test_budget_guard_and_switches(monkeypatch, capsys):
    rc, book, net, query, bq = run(net=Net(used=95000))
    assert rc == 0 and not any("api/news" in u for u in net.urls)
    assert any("EODHD budget guard" in str(r[0]) for r in book.tabs["News_Now"].values)
    assert [ld["table"] for ld in bq.loads] == ["news_now_log"]                   # nothing new: the day stays as it is
    assert all(r["state"] is None and r["news_coverage"] == "not refreshed" for r in bq.loads[0]["rows"])
    monkeypatch.setenv("TFB_NEWS_NOW_EODHD", "off")
    rc, book, net, query, bq = run()
    assert rc == 0 and not any("eodhd" in u for u in net.urls)
    monkeypatch.setenv("TFB_NEWS_NOW", "off")
    rc, book, net, query, bq = run()
    assert rc == 0 and net.urls == [] and query.sql == [] and "News_Now" not in book.tabs


def test_before_the_view_is_applied_the_page_still_builds():
    rc, book, net, query, bq = run(query=Query(class_view=False))
    assert rc == 0
    page = book.tabs["News_Now"].values
    head = next(i for i, r in enumerate(page) if r[0] == "Name")
    a = page[head + 1]
    assert a[5] == "1 headline(s), not classified yet" and a[6] == "headlines not classified yet (apply_bq_views 1.27.0)"


def test_dry_run_writes_nothing_and_a_failed_log_marks_the_run(capsys):
    rc, book, net, query, bq = run(dry=True)
    assert rc == 0 and bq.loads == [] and "News_Now" not in book.tabs
    rc, book, net, query, bq = run(bq=BQ(fail_load="news_now_log"))
    assert rc == 1 and "News_Now" in book.tabs                                  # the page is still written


def test_before_the_open_news_reads_as_unanswered():
    rc, book, net, query, bq = run(net=Net(live=False))
    by = {r["symbol"]: r for r in bq.loads[-1]["rows"]}
    assert by["AAA.US"]["session_state"] == "not open yet" and by["AAA.US"]["state"] == "news, price quiet"
    assert by["AAA.US"]["expected"] == 1 and by["AAA.US"]["change_pct"] is None


def test_source_is_ascii_and_versioned():
    src = Path(nn.__file__).read_text(encoding="utf-8")
    assert src.isascii() and nn.__version__ == "1.1.0" and "v1.1.0 (2026-11-24)" in src
    names = [f["name"] for f in nn.SCHEMA_LOG]
    assert len(names) == len(set(names)) and names[0] == "snapshot_date"


# ----------------------------------------------------------------------------- the workflow
def test_workflow_chains_to_the_exit_plan_in_the_us_hours():
    wf = yaml.safe_load(Path(".github/workflows/news_now.yml").read_text(encoding="utf-8"))
    on = wf[True] if True in wf else wf["on"]
    assert on["workflow_run"]["workflows"] == ["🎯 Exit Plan"] and "schedule" not in on
    assert set(on["workflow_dispatch"]["inputs"]) == {"write"}
    due = wf["jobs"]["due"]["steps"][0]
    assert due["env"]["CHAIN_HOURS"] == "${{ vars.TFB_NEWS_NOW_CHAIN_HOURS || '13 14 15 16 17 18 19 20 21' }}"
    job = wf["jobs"]["now"]
    assert job["needs"] == "due" and job["if"] == "needs.due.outputs.run == 'true'"
    assert job["concurrency"] == {"group": "tfb-news-now", "cancel-in-progress": False}
    env = [s for s in job["steps"] if s.get("name", "").startswith("📰")][0]["env"]
    assert env["TFB_NEWS_NOW"] == "${{ vars.TFB_NEWS_NOW || 'on' }}"
    assert env["EODHD_API_KEY"] == "${{ secrets.EODHD_API_KEY }}"
    assert wf["permissions"] == {"contents": "read"}
    assert wf["run-name"].startswith("${{ github.event_name == 'workflow_dispatch'")


@pytest.mark.parametrize("age,table", [(45, "snap_universe_latest"), (2000, "v_universe_latest"),
                                       (None, "v_universe_latest")])
def test_tfb_view_from_a_fresh_snapshot_else_the_view(age, table):
    rc, book, net, query, bq = run(query=Query(snap_age=age))
    assert rc == 0
    uni = [q for q in query.sql if "_universe_latest`" in q and "AS age_min" not in q]
    assert len(uni) == 1 and ("`proj-1.tfb_archive.%s`" % table) in uni[0]
    by = {r["symbol"]: r for r in bq.loads[-1]["rows"]}
    assert by["AAA.US"]["recommendation"] == "STRONG SELL"                       # the same rows either way
