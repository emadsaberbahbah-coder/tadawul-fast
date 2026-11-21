"""scripts/news_board.py v1.0.0 - v1.3.0 - the News Board (news learning loop, phase B): the week's world news by theme, the
sector tilt of both arms, the holdings beside their sector's tilt, the shadow list and the trial, read from the
BigQuery news views on every run. Pure functions, the SQL builders, and full runs through the real BigQuery reader on
a fake session and a fake workbook (no network). Every figure here is invented."""
from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

import pytest
import yaml

from scripts import evidence_board as eb
from scripts import news_board as nb

ROOT = Path(__file__).resolve().parents[1]
UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 19, 5, 0, tzinfo=UTC)          # Thu 19 Nov 2026, 08:00 Riyadh
TODAY = dt.date(2026, 11, 19)
DS = "proj-1.tfb_archive"


def th(week, end, feature, kind, days, closed, usable, value, prev, surprise, sd="1.0", n="59"):
    return {"week_start": week, "week_end": end, "feature": feature, "kind": kind, "days": str(days),
            "is_closed": "true" if closed else "false", "usable": "true" if usable else "false", "value": value,
            "prev_4w_avg": prev, "surprise": surprise, "sd": sd, "n": n}


THEME_ROWS = [
    th("2026-11-02", "2026-11-08", "ai_tech", "share", 7, True, True, "11.0", "12.0", "-1.0"),
    th("2026-11-09", "2026-11-15", "ai_tech", "share", 7, True, True, "13.8", "12.0", "1.8"),
    th("2026-11-16", "2026-11-22", "ai_tech", "share", 3, False, False, "15.0", "12.5", "2.5"),
    th("2026-11-09", "2026-11-15", "us_china_tone", "mean", 7, True, True, "0.90", "1.20", "-0.30", sd="0.4"),
    th("2026-11-16", "2026-11-22", "us_china_tone", "mean", 3, False, False, "1.60", "0.95", "0.65", sd="0.4"),
    th("2026-11-09", "2026-11-15", "iran_war", "share", 4, True, False, "3.1", "2.6", "0.5"),
    th("2026-11-16", "2026-11-22", "oil_price", "share", 3, False, False, "7.0", None, None),
    th("2026-11-09", "2026-11-15", "zz_new", "share", 7, True, True, "5.0", "5.0", "0.1"),
]


def tl(arm, week, a_from, a_to, etf, sector, score, pos, active, group, realized=None):
    return {"arm": arm, "week_start": week, "apply_from": a_from, "apply_to": a_to, "etf": etf, "sector": sector,
            "score": score, "pos": str(pos), "active_list": active, "tilt_group": group, "realized_excess_pp": realized}


W1 = ("2026-11-02", "2026-11-10", "2026-11-16")            # the week before
W2 = ("2026-11-09", "2026-11-17", "2026-11-23")            # in force on 19 Nov
TILT_ROWS = [
    tl("events", *W1, "XLP", "Consumer Staples", "0.16", 1, "russia_war", "favoured", "3.5"),
    tl("events", *W1, "XLF", "Financials", "-0.01", 2, "iran_war", "neutral", "2.0"),
    tl("events", *W1, "XLK", "Information Technology", "-0.05", 3, "russia_war, us_china_coercion", "avoided", "-1.9"),
    tl("events", *W2, "XLK", "Information Technology", "0.22", 1, "russia_war, us_china_coercion", "favoured"),
    tl("events", *W2, "XLF", "Financials", "-0.15", 2, "iran_war, us_china_tone", "avoided"),
    tl("events", *W2, "XLP", "Consumer Staples", "-0.63", 3, "russia_war", "avoided"),
    tl("all", *W2, "XLK", "Information Technology", "0.23", 1, "ai_tech, oil_price, zz_new", "favoured"),
    tl("all", *W2, "XLE", "Energy", "-0.13", 2, "election, oil_price", "avoided"),
    tl("all", *W2, "XLF", "Financials", "-0.21", 3, "fed_rates", "avoided"),
]
RECORD_ROWS = [
    {"arm": "events", "period": "before", "weeks": "71", "fav": "0.24", "avo": "-0.01", "spread": "0.24", "t": "0.79", "right_pct": "50.7"},
    {"arm": "all", "period": "before", "weeks": "71", "fav": "0.21", "avo": "-0.03", "spread": "0.23", "t": "0.91", "right_pct": "60.6"},
    {"arm": "all", "period": "forward", "weeks": "3", "fav": "0.5", "avo": "0.1", "spread": "0.4", "t": "1.1", "right_pct": "66.7"},
]
SECTOR_ROWS = [{"symbol": "AAA.US", "sector": "Financials"}, {"symbol": "BBB.PA", "sector": "Information Technology"},
               {"symbol": "DDD.US", "sector": "Utilities"}]


def sh(arm, symbol, name, sector, score, vol, listed, action, base="2026-11-19"):
    return {"arm": arm, "base_date": base, "symbol": symbol, "name": name, "sector": sector, "overall_score": score,
            "vol_band": vol, "in_champion": "true" if listed else "false", "news_action": action}


SHADOW_ROWS = [
    sh("events", "AAA.US", "Alpha Bank", "Financials", "74.1", "LOW", True, "out"),
    sh("events", "EEE.US", "Echo Foods", "Consumer Staples", "73.0", "MID", True, "out"),
    sh("events", "FFF.US", "Foxtrot Rail", "Industrials", "75.5", "LOW", True, "keep"),
    sh("events", "GGG.US", "Golf Chips", "Information Technology", "63.0", "MID", False, "in"),
    sh("events", "HHH.T", "Hotel Soft", "Information Technology", "65.0", "MID", False, "in"),
    sh("all", "AAA.US", "Alpha Bank", "Financials", "74.1", "LOW", True, "out"),
    sh("all", "EEE.US", "Echo Foods", "Consumer Staples", "73.0", "MID", True, "keep"),
    sh("all", "FFF.US", "Foxtrot Rail", "Industrials", "75.5", "LOW", True, "keep"),
    sh("all", "HHH.T", "Hotel Soft", "Information Technology", "65.0", "MID", False, "in"),
    sh("events", "OLD.US", "Old Day", "Financials", "70.0", "LOW", True, "out", base="2026-11-18"),
]


def tr(period, arm, line, names, days, left, weeks, edge, right=None, t=None, reading=None, horizon="1W"):
    return {"period": period, "arm": arm, "horizon": horizon, "line": line, "names": str(names), "days": str(days),
            "incomplete_days": str(left), "news_weeks": str(weeks), "edge_pp": edge, "right_pct": right, "t_adj": t,
            "reading": reading}


PAST = "in-sample, not evidence"
TRIAL_ROWS = [
    tr(PAST, "all", "OVERLAY_MINUS_CHAMPION", 339, 3, 4, 1, "-0.16", None, "-0.47", "in-sample: shows why this is a trial, not evidence"),
    tr(PAST, "events", "OVERLAY_MINUS_CHAMPION", 673, 6, 1, 2, "-0.03", None, "-0.17", "in-sample: shows why this is a trial, not evidence"),
    tr(PAST, "events", "NEWS_OVERLAY", 673, 6, 1, 2, "-0.9", "46.4"),
    tr(PAST, "events", "CHAMPION", 673, 6, 1, 2, "-0.87", "46.4"),
]
FORWARD_ROWS = [
    tr(nb.FORWARD, "events", "CHAMPION", 400, 9, 0, 3, "0.10", "51.0"),
    tr(nb.FORWARD, "events", "OVERLAY_MINUS_CHAMPION", 400, 9, 0, 3, "0.42", None, "1.31", "collecting: 9 of 20 days, 3 of 8 news weeks"),
]
def cn(symbol, company, other, score, prov, title=None, event=None, day_=None):
    return {"symbol": symbol, "company_items": str(company), "other_items": str(other), "score": score,
            "provider_mean": prov, "top_title": title, "top_event": event, "top_day": day_}


# v1.2.0 [COMPANY]: what v_symbol_news_class gives for the holdings (CCC.US has no headline at all)
COMPANY_ROWS = [
    cn("AAA.US", 3, 1, "2.5", "0.95", "Alpha Bank (AAA) Tops Q3 Earnings and Revenue Estimates", "EARNINGS_BEAT", "2026-11-17"),
    cn("BBB.PA", 0, 2, "0.0", "0.99"),
    cn("DDD.US", 1, 0, "-2.0", "0.97", "Delta Power (DDD) Lags Q3 Earnings Estimates", "EARNINGS_MISS", "2026-11-18"),
]


def ce(signal, cls, matured, mean5, reading):
    return {"signal": signal, "class": cls, "matured_5d": str(matured), "mean_5d_excess_pp": mean5, "beat_5d_pct": "60.0",
            "corr_5d": "0.02", "reading": reading}


EVAL_ROWS = [ce("company_news", "negative", 5, "3.19", "NO EDGE"), ce("company_news", "neutral", 187, "1.15", "NO EDGE"),
             ce("company_news", "positive", 51, "1.17", "NO EDGE"),
             ce("provider_sentiment", "positive", 391, "0.96", "BIASED (class > 90 %)")]


# v1.3.0 [PEERS]: the view of apply_bq_views 1.17.0 also gives each class against the same-day US names
def cp(signal, cls, matured, mean5, peer5, beat, reading, peers="491"):
    return dict(ce(signal, cls, matured, mean5, reading), mean_5d_peer_pp=peer5, matured_5d_peer=str(matured),
                beat_5d_peer_pct=beat, corr_5d_peer="0.019", peers=peers)


PEER_ROWS = [cp("company_news", "negative", 5, "3.19", "2.55", "100.0", "NO EDGE", "492"),
             cp("company_news", "neutral", 186, "1.16", "0.67", "61.8", "NO EDGE"),
             cp("company_news", "positive", 52, "1.14", "0.69", "63.5", "NO EDGE", "490"),
             cp("provider_sentiment", "positive", 391, "0.96", "0.49", "60.6", "BIASED (class > 90 %)")]
UNKNOWN_COLUMN = ('{"error": {"code": 400, "message": "Unrecognized name: mean_5d_peer_pp at [1:88]", '
                  '"status": "INVALID_ARGUMENT"}}')
PORTFOLIO = [["MY PORTFOLIO"], [], ["Symbol", "Name"], ["aaa.us", "Alpha Bank"], ["BBB.PA", "Bravo Tech"], ["", ""],
             ["CCC.US", "Charlie Unknown"], ["AAA.US", "dup"], ["not a symbol", "x"], ["TOTAL", ""], ["DDD.US", ""]]


def payload(rows):
    """Rows as the BigQuery REST API returns them (every value a string or null)."""
    names = list(rows[0]) if rows else []
    return {"jobComplete": True, "schema": {"fields": [{"name": n} for n in names]},
            "rows": [{"f": [{"v": r.get(n)} for n in names]} for r in rows]}


class Resp:
    def __init__(self, body, status=200, txt="refused"):
        self._body, self.status_code, self.text = body, status, txt

    def json(self):
        return self._body


class Session:
    """Answers each query of the board by the view it reads; records the SQL."""

    def __init__(self, trial=None, last_day="2026-11-18", fail=None, tilt=None, shadow=None, old_view=False):
        self.sql, self.trial, self.last_day, self.fail = [], TRIAL_ROWS if trial is None else trial, last_day, fail
        self.old_view = old_view                  # v1.3.0: v_company_news_eval as before apply_bq_views 1.17.0
        self.tilt, self.shadow = TILT_ROWS if tilt is None else tilt, SHADOW_ROWS if shadow is None else shadow

    def post(self, url, timeout=None, json=None):
        sql = json["query"]
        self.sql.append(sql)
        assert url.endswith("/projects/proj-1/queries") and json["useLegacySql"] is False
        if self.fail and self.fail in sql:
            return Resp({}, 403)
        if ".news_theme_daily`" in sql:
            return Resp(payload([{"last_day": self.last_day, "features": "14"}]))
        if ".v_news_theme_weekly`" in sql:
            return Resp(payload(THEME_ROWS))
        if ".v_news_sector_tilt`" in sql:
            return Resp(payload(RECORD_ROWS if "AS spread" in sql else self.tilt))
        if ".v_symbol_sector`" in sql:
            return Resp(payload(SECTOR_ROWS))
        if ".v_news_overlay_list`" in sql:
            return Resp(payload(self.shadow))
        if ".v_trial_news_overlay`" in sql:
            return Resp(payload(self.trial))
        if ".v_symbol_news_class`" in sql:
            return Resp(payload(COMPANY_ROWS))
        if ".v_company_news_eval`" in sql:
            if "mean_5d_peer_pp" not in sql:
                return Resp(payload(EVAL_ROWS))
            return Resp({}, 400, UNKNOWN_COLUMN) if self.old_view else Resp(payload(PEER_ROWS))
        raise AssertionError("unexpected query: %s" % sql[:80])


class Sheet:
    def __init__(self, rows=None):
        self.values, self.row_count, self.col_count, self.updates, self.ranges = list(rows or []), 200, 30, 0, []

    def get(self, rng):
        self.ranges.append(rng)
        return [list(r) for r in self.values]

    def resize(self, rows=None, cols=None):
        self.row_count, self.col_count = rows or self.row_count, cols or self.col_count

    def update(self, values=None, range_name=None, value_input_option=None):
        self.updates += 1
        self.values = [list(r) for r in values]


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


def _ev(eid, start, end, title, kind="summit", status="confirmed", time="", why="", markets=""):
    return {"id": eid, "start": start, "end": end, "title": title, "kind": kind, "status": status, "time": time,
            "why": why or "why " + eid, "markets": markets or "US", "source": "https://example.org/" + eid}


# an invented calendar: every run in this suite reads it, never the real file (which changes with the world)
CAL = {"version": "t", "checked": "2026-11-19", "events": [
    _ev("e", "2027-01-19", "2027-01-19", "Deadline E", "deadline"),                      # 61 days ahead: outside the window
    _ev("c", "2026-12-08", "2026-12-09", "Rate decision C", "central_bank", "tentative", "Wed 9 Dec, 22:00"),
    _ev("w", "2026-11-20", "2026-12-31", "Budget W", "budget", "date_not_set"),
    _ev("a", "2026-11-18", "2026-11-19", "Summit A"),
    _ev("z", "2026-11-10", "2026-11-18", "Summit Z"),                                    # ended yesterday
    _ev("b", "2026-11-29", "2026-11-29", "Oil meeting B", "energy"),
    _ev("d", "2027-01-18", "2027-01-18", "Deadline D", "deadline"),                      # exactly 60 days ahead
    _ev("y", "2026-12-30", "2027-01-02", "Summit Y"),
]}


@pytest.fixture(autouse=True)
def _env(monkeypatch, tmp_path):
    for name in list(__import__("os").environ):
        if name.startswith("TFB_NEWS_BOARD") or name.startswith("TFB_BQ_"):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TFB_BQ_PROJECT", "proj-1")
    path = tmp_path / "calendar.json"
    path.write_text(json.dumps(CAL), encoding="utf-8")
    monkeypatch.setattr(nb, "CALENDAR_FILE", str(path))


def col(page, first):
    return [r for r in page if r and r[0] == first]


def col_all(page):
    return [r for r in page if r and r[0]]


# ----------------------------------------------------------------------------- N1 themes
def test_n1_theme_reading_and_change():
    r = nb.theme_reading
    assert [r("share", z, 1.0, 59) for z in (0.0, 0.49, -0.49)] == ["about normal"] * 3
    assert r("share", 0.5, 1.0, 59) == "more news than normal" and r("share", 1.49, 1.0, 59) == "more news than normal"
    assert r("share", 1.5, 1.0, 59) == "far more news than normal" and r("share", -1.5, 1.0, 59) == "far less news than normal"
    assert r("share", -0.6, 1.0, 59) == "less news than normal"
    assert r("share", 3.0, 2.0, 59) == "far more news than normal" and r("share", 2.9, 2.0, 59) == "more news than normal"
    assert r("mean", 0.7, 1.0, 59) == "tone more positive than normal"
    assert r("mean", -2.0, 1.0, 59) == "tone far more negative than normal"
    assert r("share", None, 1.0, 59) == "no reading yet"
    for sd, n in ((None, 59), (0.0, 59), (-1.0, 59), (1.0, 11), (1.0, None)):
        assert r("share", 5.0, sd, n) == "too little history to compare", (sd, n)
    assert r("share", 5.0, 1.0, 12) == "far more news than normal"
    c = nb.change_text
    assert c("share", 13.8, 12.0) == "+15%" and c("share", 6.0, 12.0) == "-50%" and c("share", 5.0, 0.0) == ""
    assert c("mean", 0.9, 1.2) == "-0.30" and c("mean", -0.2, -0.5) == "+0.30"
    assert c("share", None, 1.0) == "" and c("mean", 1.0, None) == ""


def test_n1_theme_rows_take_the_last_full_week_and_the_running_week():
    rows = nb.theme_rows(THEME_ROWS)
    assert [t["feature"] for t in rows] == ["iran_war", "us_china_tone", "oil_price", "ai_tech", "zz_new"]   # THEMES order
    by = {t["feature"]: t for t in rows}
    ai = by["ai_tech"]
    assert (ai["label"], ai["news"], ai["week_start"], ai["week_end"]) == ("AI and technology", "Technology", "2026-11-09", "2026-11-15")
    assert (ai["value"], ai["prev"], ai["change"], ai["reading"]) == (13.8, 12.0, "+15%", "far more news than normal")
    assert (ai["now_value"], ai["now_days"], ai["now_reading"]) == (15.0, 3.0, "far more news than normal")
    tone = by["us_china_tone"]
    assert (tone["change"], tone["reading"], tone["now_reading"]) == ("-0.30", "tone more negative than normal",
                                                                    "tone far more positive than normal")
    iran = by["iran_war"]                                           # a closed week of four days is not read
    assert iran["value"] == 3.1 and iran["reading"] == "no reading: the week or the four before it are incomplete"
    assert iran["now_value"] is None and iran["now_reading"] == ""
    oil = by["oil_price"]                                           # only a running week, with no normal yet
    assert (oil["week_start"], oil["value"], oil["reading"], oil["now_value"], oil["now_reading"]) == (
        "", None, "no full week yet", 7.0, "")
    new = by["zz_new"]                                              # a theme added later still shows
    assert (new["label"], new["news"], new["reading"]) == ("zz_new", "Other", "about normal")
    assert nb.theme_rows([]) == [] and nb.theme_rows([{"feature": "x", "week_start": "junk"}]) == []


# ----------------------------------------------------------------------------- N2 tilt
def test_n2_pick_tilt_takes_the_week_in_force():
    t = nb.pick_tilt(TILT_ROWS, TODAY)
    ev = t["events"]
    assert (ev["week"], ev["apply_from"], ev["apply_to"], ev["in_force"], ev["prev_week"]) == (
        "2026-11-09", dt.date(2026, 11, 17), dt.date(2026, 11, 23), True, "2026-11-02")
    assert ev["sectors"]["Financials"] == {"etf": "XLF", "group": "avoided", "score": -0.15, "pos": 2.0,
                                           "active": "iran_war, us_china_tone", "realized": None}
    assert ev["prev"]["Consumer Staples"]["group"] == "favoured" and ev["prev"]["Consumer Staples"]["realized"] == 3.5
    assert t["all"]["prev_week"] == "" and t["all"]["prev"] == {} and t["all"]["in_force"] is True
    for d, week, force in ((dt.date(2026, 11, 17), "2026-11-09", True), (dt.date(2026, 11, 23), "2026-11-09", True),
                           (dt.date(2026, 11, 24), "2026-11-09", False),      # past its window, no newer week: shown, not in force
                           (dt.date(2026, 11, 16), "2026-11-02", True), (dt.date(2026, 11, 10), "2026-11-02", True)):
        got = nb.pick_tilt(TILT_ROWS, d)["events"]
        assert (got["week"], got["in_force"]) == (week, force), d
    early = nb.pick_tilt(TILT_ROWS, dt.date(2026, 11, 9))            # before any window began
    assert early == {}
    assert "all" not in nb.pick_tilt(TILT_ROWS, dt.date(2026, 11, 12)) and nb.pick_tilt([], TODAY) == {}
    assert nb.pick_tilt([dict(TILT_ROWS[0], apply_from="junk")], TODAY) == {}


def test_n2_sector_rows_and_the_record():
    rows = nb.sector_rows(nb.pick_tilt(TILT_ROWS, TODAY))
    assert [r["sector"] for r in rows] == ["Information Technology", "Financials", "Consumer Staples", "Energy"]
    it, fin, stp, en = rows
    assert (it["etf"], it["events"], it["events_score"], it["all"], it["all_score"]) == ("XLK", "favoured", 0.22, "favoured", 0.23)
    assert it["themes"] == "AI, oil price, zz_new" and (it["prev"], it["prev_realized"]) == ("avoided", -1.9)
    assert (fin["events"], fin["all"], fin["themes"], fin["prev"], fin["prev_realized"]) == ("avoided", "avoided", "rates", "neutral", 2.0)
    assert (stp["events"], stp["all"], stp["all_score"], stp["themes"]) == ("avoided", "", None, "")
    assert (en["etf"], en["events"], en["all"], en["themes"], en["prev"]) == ("XLE", "", "avoided", "elections, oil price", "")
    assert nb.sector_rows({}) == []
    only_all = nb.sector_rows({"all": nb.pick_tilt(TILT_ROWS, TODAY)["all"]})
    assert [r["sector"] for r in only_all] == ["Information Technology", "Energy", "Financials"] and only_all[0]["events"] == ""
    assert nb.themes_text("iran_war, us_china_tone,market_tone") == "Iran war, US-China tone, market tone" and nb.themes_text("") == ""
    rec = nb.record_rows(RECORD_ROWS)
    assert [(r["arm"], r["period"]) for r in rec] == [("events", "before"), ("all", "forward"), ("all", "before")]
    assert rec[0]["reading"] == "past weeks, not a clean test: no better than chance" and rec[0]["spread"] == 0.24
    assert rec[1]["reading"] == "collecting: 3 of 8 weeks"
    rr = nb.record_reading
    assert rr("forward", 8, 0.4, 2.0) == "ahead so far" and rr("forward", 8, 0.4, 1.99) == "no better than chance so far"
    assert rr("forward", 30, -0.4, 3.0) == "no better than chance so far" and rr("forward", 7, 0.9, 9.0) == "collecting: 7 of 8 weeks"
    assert rr("forward", None, None, None) == "collecting: 0 of 8 weeks"
    assert rr("before", 71, 0.5, 2.4) == "past weeks, not a clean test: ahead"
    assert rr("before", 71, 0.5, None) == "past weeks, not a clean test: no better than chance"


# ----------------------------------------------------------------------------- N3 holdings and the shadow list
def test_n3_holdings_are_read_from_the_portfolio_page():
    got = nb.read_holdings(PORTFOLIO)
    assert got == [{"symbol": "AAA.US", "name": "Alpha Bank"}, {"symbol": "BBB.PA", "name": "Bravo Tech"},
                   {"symbol": "CCC.US", "name": "Charlie Unknown"}, {"symbol": "DDD.US", "name": ""}]
    assert nb.read_holdings([]) == [] and nb.read_holdings([["Name", "Ticker"], ["x", "AAA.US"]]) == []
    assert nb.read_holdings([["Name", "Symbol"], ["Alpha", "AAA.US"]]) == [{"symbol": "AAA.US", "name": ""}]
    # a symbol needs no exchange suffix (Codex P2, #1032): plain tickers, a future and an index are holdings too
    plain = nb.read_holdings([["Symbol", "Name"], ["zzq", "Plain One"], ["GC=F", "Gold"], ["^GSPC", ""], ["TOTAL", ""],
                              ["Subtotal", ""], ["Totals", ""], ["A B", ""], ["x'--", ""], ["ZZQ", "dup"], ["QQZ", ""]])
    assert [h["symbol"] for h in plain] == ["ZZQ", "GC=F", "^GSPC", "QQZ"] and plain[0]["name"] == "Plain One"
    assert nb.aliases("zzq") == ["ZZQ", "ZZQ.US"] and nb.aliases("AAA.US") == ["AAA.US"] and nb.aliases("GC=F") == ["GC=F"]
    assert nb.aliases("^GSPC") == ["^GSPC"] and nb.aliases("") == [""]
    assert [nb.market_of(x) for x in ("ZZQ", "AAA.US", "5023.SR", "GC=F", "^GSPC", "")] == ["US", "US", "SR", "", "", ""]
    assert nb.clean_symbols(["aaa.us", "AAA.US", "x'; DROP TABLE t; --", "", None, "5023.SR", "A" * 16]) == ["AAA.US", "5023.SR"]


def test_n3_shadow_list_merges_the_two_arms_on_the_latest_day():
    last, names = nb.shadow_names(SHADOW_ROWS)
    assert last == "2026-11-19" and "OLD.US" not in names and len(names) == 5
    assert names["EEE.US"]["actions"] == {"events": "out", "all": "keep"} and names["EEE.US"]["listed"] is True
    assert names["GGG.US"]["actions"] == {"events": "in"} and names["GGG.US"]["listed"] is False
    assert nb.shadow_counts(names) == {"events": {"list": 3, "out": 2, "in": 2}, "all": {"list": 3, "out": 1, "in": 1}}
    rows = nb.shadow_rows(names)
    assert [d["symbol"] for d in rows] == ["EEE.US", "AAA.US", "HHH.T", "GGG.US"]      # leaving by sector, entering by score
    assert "FFF.US" not in [d["symbol"] for d in rows]                               # it stays in both arms
    assert nb.shadow_names([]) == ("", {}) and nb.shadow_counts({}) == {a: {"list": 0, "out": 0, "in": 0} for a in nb.ARMS}
    assert nb.shadow_rows({}) == []


def test_n3_holding_rows_show_the_tilt_of_each_holding():
    tilt = nb.pick_tilt(TILT_ROWS, TODAY)
    _, names = nb.shadow_names(SHADOW_ROWS)
    sectors = {r["symbol"]: r["sector"] for r in SECTOR_ROWS}
    rows = nb.holding_rows(nb.read_holdings(PORTFOLIO), sectors, tilt, names, "2026-11-19")
    a, b, c, d = rows
    assert (a["symbol"], a["sector"], a["events"], a["all"], a["listed"], a["shadow"], a["note"]) == (
        "AAA.US", "Financials", "avoided", "avoided", "yes", "would leave", "")
    assert (b["sector"], b["events"], b["all"], b["listed"], b["shadow"]) == ("Information Technology", "favoured", "favoured", "no", "")
    assert b["note"] == "the tilt is measured on US sector funds: indicative only for this market"
    assert (c["sector"], c["events"], c["all"], c["note"]) == ("", "", "", "sector not known to the archive")
    assert (d["sector"], d["events"], d["all"], d["note"]) == ("Utilities", "", "", "")      # a sector the tilt does not list
    stale = nb.holding_rows(nb.read_holdings(PORTFOLIO), sectors, nb.pick_tilt(TILT_ROWS, dt.date(2026, 11, 24)), names,
                            "2026-11-19")
    assert (stale[0]["events"], stale[0]["all"]) == ("", "")                               # no tilt in force: none claimed
    assert nb.holding_rows([], sectors, tilt, names) == [] and nb.market_of("5023.SR") == "SR" and nb.market_of("X") == "US"
    # a plain ticker is found under TICKER.US, in the sector map and on the list; a future has no sector
    bare = nb.holding_rows([{"symbol": "AAA", "name": ""}, {"symbol": "GC=F", "name": "Gold"}, {"symbol": "GGG", "name": ""}],
                           sectors, tilt, names, "2026-11-19")
    assert (bare[0]["sector"], bare[0]["events"], bare[0]["listed"], bare[0]["shadow"], bare[0]["note"], bare[0]["name"]) == (
        "Financials", "avoided", "yes", "would leave", "", "Alpha Bank")
    assert (bare[1]["sector"], bare[1]["listed"], bare[1]["note"]) == ("", "no", "sector not known to the archive")
    assert (bare[2]["listed"], bare[2]["shadow"], bare[2]["note"]) == ("no", "would enter", "sector not known to the archive")
    # the list day decides what may be said about the list
    assert nb.shadow_note("2026-11-19", tilt) == "" and nb.shadow_note("2026-11-17", tilt) == ""
    assert nb.shadow_note("2026-11-23", tilt) == "" and nb.shadow_note("", tilt) == "" and nb.shadow_note("2026-11-19", {}) == ""
    note = nb.shadow_note("2026-11-16", tilt)                         # yesterday's list under the tilt of the week before
    assert note == ("NOTE: this list day is outside the window of the tilt in section 2 (2026-11-17 to 2026-11-23); its "
                    "moves come from the tilt that applied on that day.")
    assert nb.shadow_note("2026-11-24", tilt) == note
    old = nb.holding_rows(nb.read_holdings(PORTFOLIO), sectors, tilt, names, "2026-11-16")
    assert (old[0]["events"], old[0]["listed"], old[0]["shadow"]) == ("avoided", "yes", "")   # the list fact stays, no action claimed
    none = nb.holding_rows(nb.read_holdings(PORTFOLIO), sectors, tilt, {}, "")
    assert (none[0]["listed"], none[0]["shadow"], none[1]["listed"]) == ("", "", "")       # no list day: nothing said


# ----------------------------------------------------------------------------- N4 trial and verdict
def test_n4_trial_order_and_verdict():
    rows = nb.trial_rows(TRIAL_ROWS + FORWARD_ROWS + [{"line": ""}])
    assert [(r["period"] == nb.FORWARD, r["arm"], r["line"]) for r in rows] == [
        (True, "events", "CHAMPION"), (True, "events", "OVERLAY_MINUS_CHAMPION"), (False, "events", "CHAMPION"),
        (False, "events", "NEWS_OVERLAY"), (False, "events", "OVERLAY_MINUS_CHAMPION"), (False, "all", "OVERLAY_MINUS_CHAMPION")]
    tilt = nb.pick_tilt(TILT_ROWS, TODAY)
    v = nb.verdict_lines(nb.trial_rows(TRIAL_ROWS), tilt, dt.date(2026, 11, 18), TODAY)
    assert len(v) == 3 and v[0].startswith("The forward trial has no counted day yet: it starts with the list of 20 Nov 2026")
    assert v[1] == "Past days (not evidence): overlay minus list -0.03 pp over 6 day(s), t -0.17 - no better than chance."
    assert v[2].startswith("Nothing on this page changes a recommendation.")
    v = nb.verdict_lines(nb.trial_rows(TRIAL_ROWS + FORWARD_ROWS), tilt, dt.date(2026, 11, 18), TODAY)
    assert v[0] == ("Forward trial (political events, 1 week): 9 day(s) counted, overlay minus list +0.42 pp, t 1.31 - "
                    "collecting: 9 of 20 days, 3 of 8 news weeks.")
    ahead = [tr(nb.FORWARD, "events", "OVERLAY_MINUS_CHAMPION", 900, 24, 0, 9, "0.80", None, "2.71",
                "news overlay ahead: the owner may consider it"),
             tr(PAST, "events", "OVERLAY_MINUS_CHAMPION", 673, 6, 1, 2, "0.50", None, "2.20", "x")]
    v = nb.verdict_lines(ahead, tilt, dt.date(2026, 11, 18), TODAY)
    assert v[0].endswith("t 2.71 - news overlay ahead: the owner may consider it.")
    assert v[1].endswith("t 2.20 - a difference the forward trial has to confirm.")
    zero = [tr(nb.FORWARD, "events", "OVERLAY_MINUS_CHAMPION", 0, 0, 2, 0, None, None, None, "collecting: 0 of 20 days, 0 of 8 news weeks")]
    assert nb.verdict_lines(zero, tilt, dt.date(2026, 11, 18), TODAY)[0].startswith("The forward trial has no counted day yet")
    other = [tr(nb.FORWARD, "all", "OVERLAY_MINUS_CHAMPION", 9, 9, 0, 3, "9", None, "9", "x"),
             tr(nb.FORWARD, "events", "OVERLAY_MINUS_CHAMPION", 9, 9, 0, 3, "9", None, "9", "x", horizon="2W")]
    assert nb.verdict_lines(other, tilt, dt.date(2026, 11, 18), TODAY)[0].startswith("The forward trial has no counted day yet")
    # warnings: a stale archive, an undated archive, no tilt in force
    assert not any("WARNING" in s for s in nb.verdict_lines([], tilt, dt.date(2026, 11, 16), TODAY))          # 3 days: fine
    late = nb.verdict_lines([], tilt, dt.date(2026, 11, 15), TODAY)
    assert late[-1] == "WARNING: the news archive stops at 2026-11-15 (4 days ago); the tilt below may be out of date."
    assert nb.verdict_lines([], tilt, None, TODAY)[-1].startswith("WARNING: the news archive could not be dated")
    gone = nb.verdict_lines([], nb.pick_tilt(TILT_ROWS, dt.date(2026, 11, 24)), dt.date(2026, 11, 23), dt.date(2026, 11, 24))
    assert gone[-1].startswith("WARNING: no sector tilt is in force for today") and len(gone) == 3
    assert nb.verdict_lines([], {}, dt.date(2026, 11, 18), TODAY)[-1].startswith("WARNING: no sector tilt is in force")


# ----------------------------------------------------------------------------- N5 SQL
def test_n5_sql_is_select_only_dated_and_clean():
    sqls = [nb.sql_last_news(DS, TODAY), nb.sql_themes(DS, TODAY), nb.sql_tilt(DS, TODAY), nb.sql_record(DS),
            nb.sql_sectors(DS, ["aaa.us", "x' OR '1'='1", "5023.SR"]), nb.sql_shadow(DS, TODAY), nb.sql_trial(DS),
            nb.sql_company_news(DS, ["aaa.us", "x' OR '1'='1", "BBB.PA"], TODAY), nb.sql_company_eval(DS)]
    for sql in sqls:
        assert re.match(r"^(WITH|SELECT)\b", sql) and "`proj-1.tfb_archive." in sql and ";" not in sql
        assert not re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE|CALL)\b", sql, re.I), sql
        assert "CURRENT_DATE" not in sql and "{" not in sql and "%s" not in sql
    assert sqls[4].endswith("WHERE symbol IN ('AAA.US', '5023.SR')")
    assert "DATE '2026-11-19'" in sqls[1] and sqls[1].count("DATE '2026-11-19'") == 2 and "INTERVAL 21 DAY" in sqls[1]
    assert "week_start >= DATE '2026-11-16'" in sqls[3] and "f IS NOT NULL AND a IS NOT NULL" in sqls[3]
    assert "o.base_date = m.d" in sqls[5] and "base_date <= DATE '2026-11-19'" in sqls[5]
    assert "CAST(week_start AS STRING)" in sqls[2] and "INTERVAL 42 DAY" in sqls[2]
    for bad in ("2026-11-19", dt.datetime(2026, 11, 19), None, 20261119):
        with pytest.raises(ValueError):
            nb.sql_themes(DS, bad)
    with pytest.raises(ValueError):
        nb.sql_sectors(DS, ["", "not a symbol"])
    with pytest.raises(ValueError):
        nb.sql_company_news(DS, ["", "not a symbol"], TODAY)
    with pytest.raises(ValueError):
        nb.sql_company_news(DS, ["AAA.US"], "2026-11-19")
    company = sqls[7]
    assert "FROM `proj-1.tfb_archive.v_symbol_news_class` WHERE symbol IN ('AAA.US', 'BBB.PA')" in company
    assert "news_date >= DATE_SUB(DATE '2026-11-19', INTERVAL 6 DAY) AND news_date <= DATE '2026-11-19'" in company
    assert company.count("ORDER BY ABS(score) DESC, published_at DESC, title LIMIT 1)[SAFE_OFFSET(0)]") == 3
    assert "COUNTIF(relevance = 'company') AS company_items" in company and company.endswith("GROUP BY symbol")
    assert sqls[8] == ("SELECT signal, class, matured_5d, mean_5d_excess_pp, beat_5d_pct, corr_5d, reading, mean_5d_peer_pp, "
                       "matured_5d_peer, beat_5d_peer_pct, corr_5d_peer, peers "
                       "FROM `proj-1.tfb_archive.v_company_news_eval` ORDER BY signal, class")
    assert nb.sql_company_eval(DS, peers=False) == ("SELECT signal, class, matured_5d, mean_5d_excess_pp, beat_5d_pct, "
                                                    "corr_5d, reading FROM `proj-1.tfb_archive.v_company_news_eval` "
                                                    "ORDER BY signal, class")               # the 1.2.0 read
    assert nb.FORWARD == "forward from 2026-11-20" and nb.FORWARD_WEEK == dt.date(2026, 11, 16)
    view = (ROOT / "sql/bigquery/95_v_trial_news_overlay.sql").read_text(encoding="utf-8")
    assert "'%s'" % nb.FORWARD in view                                       # the label is the trial view's own
    for line in nb.LINE_ORDER:
        assert "'%s'" % line in view, line
    themes = (ROOT / "scripts/archive_news_themes.py").read_text(encoding="utf-8")
    for feat in nb.THEMES:
        assert '("%s", ' % feat in themes, feat                              # every theme named here is archived


# ----------------------------------------------------------------------------- N6 the page and full runs
def test_n6_a_full_run_writes_one_page(capsys):
    book, sess = Book({"My_Portfolio": Sheet(PORTFOLIO)}), Session()
    assert nb.run_once(False, book=book, session=sess, now=NOW, query=eb.bq_query) == 0
    assert list(book.tabs) == ["My_Portfolio", "News_Board"] and book.tabs["News_Board"].updates == 1
    assert book.tabs["My_Portfolio"].updates == 0 and book.tabs["My_Portfolio"].ranges == ["A1:B200"]
    assert len(sess.sql) == 9
    assert "WHERE symbol IN ('AAA.US', 'BBB.PA', 'CCC.US', 'DDD.US')" in sess.sql[4]
    assert "v_symbol_news_class" in sess.sql[7] and "WHERE symbol IN ('AAA.US', 'BBB.PA', 'CCC.US', 'DDD.US')" in sess.sql[7]
    out = capsys.readouterr().out
    plain_book, plain_sess = Book({"My_Portfolio": Sheet([["Symbol", "Name"], ["AAA", ""], ["GC=F", "Gold"], ["TOTAL", ""]])}), Session()
    assert nb.run_once(True, book=plain_book, session=plain_sess, now=NOW, query=eb.bq_query) == 0
    assert "WHERE symbol IN ('AAA', 'AAA.US')" in plain_sess.sql[4]              # only clean names reach BigQuery
    assert "2 holding(s)" in capsys.readouterr().out
    gold_book, gold_sess = Book({"My_Portfolio": Sheet([["Symbol"], ["GC=F"]])}), Session()   # nothing BigQuery can look up
    assert nb.run_once(True, book=gold_book, session=gold_sess, now=NOW, query=eb.bq_query) == 0
    assert len(gold_sess.sql) == 7 and not any("v_symbol_sector" in q or "v_symbol_news_class" in q for q in gold_sess.sql)
    assert "1 holding(s)" in capsys.readouterr().out
    page = book.tabs["News_Board"].values
    assert all(len(r) == nb.WIDTH for r in page) and len(page) >= 120
    cells = [str(c) for r in page for c in r]
    assert not any("None" in c or c.lower() in ("nan", "+nan", "-nan", "inf") for c in cells)
    assert page[0][0].startswith("NEWS BOARD - ") and page[1][0].startswith("Built 2026-11-19 08:00 Riyadh | news_board 1.3.0 | SHADOW")
    assert "only the Today Action Plan gives orders" in page[1][0]
    heads = [r[0] for r in page if re.match(r"^(VERDICT|\d\. )", str(r[0]))]
    assert [h.split(" - ")[0].split(" (")[0].split(" for ")[0] for h in heads] == [
        "VERDICT", "1. WORLD NEWS", "2. SECTOR TILT", "3. YOUR HOLDINGS AND THE TILT", "4. SHADOW LIST", "5. TRIAL",
        "6. EVENTS AHEAD", "7. COMPANY NEWS, RE-READ"]
    assert heads[1] == ("1. WORLD NEWS - last full week (2026-11-09 to 2026-11-15) against its own previous four weeks; "
                        "news archive to 2026-11-18")
    assert heads[2] == "2. SECTOR TILT - in force 2026-11-17 to 2026-11-23, from the news of the week of 2026-11-09"
    assert heads[3] == "3. YOUR HOLDINGS AND THE TILT (4 holding(s))"
    assert heads[4] == "4. SHADOW LIST for 2026-11-19 - names the news tilt would move. It is NOT an order."
    ai = col(page, "Technology")[0]
    assert ai[:9] == ["Technology", "AI and technology", "13.80", "12.00", "+15%", "far more news than normal", "15.00", "3",
                      "far more news than normal"]
    it = col(page, "Information Technology")[0]
    assert it[:9] == ["Information Technology", "XLK", "FAVOURED", "+0.220", "FAVOURED", "+0.230", "AI, oil price, zz_new",
                      "AVOIDED", "-1.90"]
    fin = col(page, "Financials")[0]
    assert fin[2:6] == ["AVOIDED", "-0.150", "AVOIDED", "-0.210"] and fin[7:9] == ["neutral", "+2.00"]
    rec = col(page, "Political events")[0]
    assert rec[:9] == ["Political events", "before the rule was fixed", "71", "+0.24", "-0.01", "+0.24", "0.79", "50.7%",
                       "past weeks, not a clean test: no better than chance"]
    fwd = [r for r in page if r[1] == "forward (news weeks from 2026-11-16)"][0]
    assert fwd[0] == "Events + news themes" and fwd[8] == "collecting: 3 of 8 weeks"
    hold = col(page, "AAA.US")
    assert hold[0][:8] == ["AAA.US", "Alpha Bank", "Financials", "AVOIDED", "AVOIDED", "yes", "would leave", ""]
    assert hold[1][:7] == ["AAA.US", "Alpha Bank", "Financials", "74.1", "LOW", "would leave", "would leave"]   # section 4
    assert col(page, "EEE.US")[0][5:7] == ["would leave", "stays"] and col(page, "GGG.US")[0][5:7] == ["would enter", "not on the list"]
    assert col(page, "Political events: list of 3, 2 would leave, 2 would enter; Events + news themes: list of 3, 1 would leave, "
               "1 would enter")
    tri = [r for r in page if r[3] == "Overlay minus list (per day)"]
    assert tri[0] == [PAST, "Political events", "1W", "Overlay minus list (per day)", "673", "6", "1", "2", "-0.03", "", "-0.17",
                      "in-sample: shows why this is a trial, not evidence"]
    assert [r for r in page if r[3] == "List as it is"][0][8:10] == ["-0.87", "46.4%"]
    assert any(str(r[0]).startswith("Promotion rule, fixed on 19 Nov 2026") for r in page)
    assert page[-1] == [""] * nb.WIDTH                                                    # the grid is blanked below the page
    last_text = [r[0] for r in page if r[0]][-1]
    assert last_text.startswith("How to read:") and last_text.endswith("Not investment advice.")
    assert "wrote the News_Board page" in out and "4 holding(s)" in out and "4 moved name(s)" in out
    assert ("TILT events week 2026-11-09, in force 2026-11-17 to 2026-11-23 | favoured: Information Technology | avoided: "
            "Consumer Staples, Financials") in out
    assert "SHADOW events 2026-11-19: list 3, 2 would leave, 2 would enter" in out
    for sym in ("AAA.US", "BBB.PA", "CCC.US", "DDD.US", "Alpha", "Bravo"):
        assert sym not in out, sym                                                         # holdings stay out of the run log


def test_n6_dry_run_off_switch_and_failures_write_nothing(monkeypatch, capsys):
    book, sess = Book({"My_Portfolio": Sheet(PORTFOLIO)}), Session()
    assert nb.run_once(True, book=book, session=sess, now=NOW, query=eb.bq_query) == 0
    assert "News_Board" not in book.tabs and len(sess.sql) == 9
    assert "dry run:" in capsys.readouterr().out
    assert nb.main(["--dry-run"], book=book, session=Session(), now=NOW, query=eb.bq_query) == 0 and "News_Board" not in book.tabs
    for view in ("v_news_theme_weekly", "v_news_sector_tilt", "v_symbol_sector", "v_news_overlay_list", "v_trial_news_overlay",
                 "news_theme_daily", "v_symbol_news_class", "v_company_news_eval"):
        old = Book({"My_Portfolio": Sheet(PORTFOLIO), "News_Board": Sheet([["old page"]])})
        assert nb.run_once(False, book=old, session=Session(fail=view), now=NOW, query=eb.bq_query) == 2, view
        assert old.tabs["News_Board"].values == [["old page"]] and old.tabs["News_Board"].updates == 0
        assert "nothing written" in capsys.readouterr().out
    monkeypatch.setenv("TFB_NEWS_BOARD", "off")
    sess = Session()
    assert nb.run_once(False, book=book, session=sess, now=NOW, query=eb.bq_query) == 0 and sess.sql == []
    assert book.tabs["My_Portfolio"].ranges == ["A1:B200"] * 2 and "is off" in capsys.readouterr().out
    monkeypatch.setenv("TFB_NEWS_BOARD", "on")
    monkeypatch.setenv("TFB_BQ_PROJECT", "bad project;")
    assert nb.run_once(False, book=book, session=sess, now=NOW, query=eb.bq_query) == 2 and sess.sql == []
    monkeypatch.setenv("TFB_BQ_PROJECT", "proj-1")
    monkeypatch.setenv("TFB_BQ_DATASET", "x.y")
    assert nb.run_once(False, book=book, session=sess, now=NOW, query=eb.bq_query) == 2 and sess.sql == []


def test_n6_empty_and_stale_inputs_still_make_an_honest_page(capsys):
    book = Book({})                                                   # no portfolio page, no tilt, no list day, no trial
    sess = Session(trial=[], last_day=None, tilt=[], shadow=[])
    assert nb.run_once(False, book=book, session=sess, now=NOW, query=eb.bq_query) == 0
    assert len(sess.sql) == 7 and not any("v_symbol_sector" in s or "v_symbol_news_class" in s for s in sess.sql)  # nothing to look up
    page = book.tabs["News_Board"].values
    flat = [r[0] for r in page] + [r[1] for r in page]
    for want in ("WARNING: the news archive could not be dated; treat the tilt below as unconfirmed.",
                 "WARNING: no sector tilt is in force for today; sections 2 to 4 show the last one, for reference.",
                 "2. SECTOR TILT - no tilt has begun to apply yet", "No holding could be read from My_Portfolio.",
                 "4. SHADOW LIST for no list day - names the news tilt would move. It is NOT an order.",
                 "No name would move on this list day.", "The trial has no row yet.", "3. YOUR HOLDINGS AND THE TILT (0 holding(s))"):
        assert want in flat, want
    book = Book({"My_Portfolio": Sheet(PORTFOLIO)})                    # a day inside the window: no note
    assert nb.run_once(False, book=book, session=Session(), now=NOW, query=eb.bq_query) == 0
    book_first_page = book.tabs["News_Board"].values
    late = dt.datetime(2026, 11, 24, 5, 0, tzinfo=UTC)                 # the window closed and no newer week came
    book = Book({"My_Portfolio": Sheet(PORTFOLIO)})
    assert nb.run_once(False, book=book, session=Session(last_day="2026-11-19"), now=late, query=eb.bq_query) == 0
    page = book.tabs["News_Board"].values
    flat = [r[0] for r in page]
    assert "2. SECTOR TILT - NOT in force today; it applied 2026-11-17 to 2026-11-23, from the news of the week of 2026-11-09" in flat
    assert "WARNING: the news archive stops at 2026-11-19 (5 days ago); the tilt below may be out of date." in flat
    assert col(page, "AAA.US")[0][3:5] == ["", ""]                    # no tilt is claimed for a holding
    # a new tilt begins on Tuesday while the latest list is Monday's: the page says so and claims no action
    newer = TILT_ROWS + [tl("events", "2026-11-16", "2026-11-24", "2026-11-30", "XLF", "Financials", "0.30", 1, "iran_war", "favoured")]
    monday = [dict(r, base_date="2026-11-23") for r in SHADOW_ROWS if r["base_date"] == "2026-11-19"]
    book = Book({"My_Portfolio": Sheet(PORTFOLIO)})
    assert nb.run_once(False, book=book, session=Session(last_day="2026-11-23", tilt=newer, shadow=monday), now=late,
                       query=eb.bq_query) == 0
    page = book.tabs["News_Board"].values
    flat = [r[0] for r in page]
    assert "2. SECTOR TILT - in force 2026-11-24 to 2026-11-30, from the news of the week of 2026-11-16" in flat
    i = flat.index("4. SHADOW LIST for 2026-11-23 - names the news tilt would move. It is NOT an order.")
    assert flat[i + 2] == ("NOTE: this list day is outside the window of the tilt in section 2 (2026-11-24 to 2026-11-30); "
                           "its moves come from the tilt that applied on that day.") and flat[i + 3] == "Symbol"
    hold = col(page, "AAA.US")[0]
    assert hold[:8] == ["AAA.US", "Alpha Bank", "Financials", "FAVOURED", "", "yes", "", ""]
    assert not any(str(r[0]).startswith("NOTE:") for r in col_all(book_first_page))


# ----------------------------------------------------------------------------- N7 wiring
def test_n7_workflow_manifest_and_read_only_scope():
    raw = (ROOT / ".github/workflows/news_board.yml").read_text(encoding="utf-8")
    wf = yaml.safe_load(raw)
    on = wf[True] if True in wf else wf["on"]
    assert on["schedule"] == [{"cron": "55 4,13 * * *"}] and set(on) == {"schedule", "workflow_dispatch", "workflow_run"}
    assert on["workflow_run"] == {"workflows": ["\U0001F3AF Exit Plan"], "types": ["completed"]}   # 1.1.0 CHAIN (test_page_chains)
    assert on["workflow_dispatch"]["inputs"]["dry_run"]["default"] is False
    assert wf["name"] == "\U0001F4F0 News Board" and wf["permissions"] == {"contents": "read"}
    job = wf["jobs"]["board"]
    assert job["concurrency"] == {"group": "tfb-news-board", "cancel-in-progress": False} and "concurrency" not in wf
    assert job["if"] == "needs.due.outputs.run == 'true'" and job["timeout-minutes"] == 20     # 1.1.0: the gate job decides
    step = [s for s in job["steps"] if "news_board.py" in str(s.get("run", ""))][0]
    assert step["env"]["TFB_NEWS_BOARD"] == "${{ vars.TFB_NEWS_BOARD || 'on' }}"
    assert step["env"]["TFB_BQ_PROJECT"] == "${{ vars.TFB_BQ_PROJECT || 'project-2e3530e6-2311-4aa3-b36' }}"
    assert step["env"]["TFB_BQ_DATASET"] == "${{ vars.TFB_BQ_DATASET || 'tfb_archive' }}"
    assert "--dry-run" in step["run"] and 'python scripts/news_board.py "${ARGS[@]}"' in step["run"]
    src = (ROOT / "scripts/news_board.py").read_text(encoding="utf-8")
    for name in re.findall(r"(TFB_[A-Z_]+): \$\{\{", raw):
        assert '"%s"' % name in src, name
    assert src.isascii() and nb.__version__ == "1.3.0" and "v1.3.0 (2026-11-21)" in src
    assert '("news_board", "%s", "news board")' % nb.__version__ in (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert src.count("write_grid(") == 1 and "write_grid(book, TAB," in src and nb.TAB == "News_Board"
    assert "PORTFOLIO_TAB, \"A1:B200\")" in src and src.count("_raw_read(") == 1          # one read, of two columns
    assert "tests/test_news_board.py" in (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")



# ----------------------------------------------------------------------------- N8 the Sheets quota
class Quota(Exception):
    def __str__(self):
        return "APIError: [429]: Quota exceeded for quota metric 'Write requests'"


def test_n8_a_quota_answer_is_waited_out_and_other_write_errors_fail_the_run(monkeypatch, capsys):
    waits = []
    monkeypatch.setattr(nb, "_SLEEP", waits.append)

    class Flaky(Sheet):
        def __init__(self, fails, error):
            super().__init__([["old page"]])
            self.fails, self.error = fails, error

        def update(self, values=None, range_name=None, value_input_option=None):
            if self.fails > 0:
                self.fails -= 1
                raise self.error
            super().update(values=values, range_name=range_name, value_input_option=value_input_option)

    book = Book({"My_Portfolio": Sheet(PORTFOLIO), "News_Board": Flaky(1, Quota())})
    sess = Session()
    assert nb.run_once(False, book=book, session=sess, now=NOW, query=eb.bq_query) == 0
    assert waits == [65.0] and book.tabs["News_Board"].updates == 1 and len(sess.sql) == 9     # the queries are not repeated
    assert book.tabs["News_Board"].values[0][0].startswith("NEWS BOARD - ")
    assert "Sheets quota reached while writing News_Board" in capsys.readouterr().out
    waits.clear()
    book = Book({"My_Portfolio": Sheet(PORTFOLIO), "News_Board": Flaky(9, Quota())})          # the quota never clears
    assert nb.run_once(False, book=book, session=Session(), now=NOW, query=eb.bq_query) == 2
    assert waits == [65.0, 65.0] and book.tabs["News_Board"].values == [["old page"]]
    assert "the page was not written" in capsys.readouterr().out
    waits.clear()
    book = Book({"My_Portfolio": Sheet(PORTFOLIO), "News_Board": Flaky(1, RuntimeError("permission denied"))})
    assert nb.run_once(False, book=book, session=Session(), now=NOW, query=eb.bq_query) == 2 and waits == []
    assert book.tabs["News_Board"].values == [["old page"]]
    out = capsys.readouterr().out
    assert "ERROR: RuntimeError: permission denied; the page was not written" in out
    waits.clear()                                                                             # the read waits too

    class FlakyRead(Sheet):
        def __init__(self, rows):
            super().__init__(rows)
            self.fails = 1

        def get(self, rng):
            if self.fails > 0:
                self.fails -= 1
                raise Quota()
            return super().get(rng)

    book = Book({"My_Portfolio": FlakyRead(PORTFOLIO)})
    assert nb.run_once(False, book=book, session=Session(), now=NOW, query=eb.bq_query) == 0 and waits == [65.0]
    assert col(book.tabs["News_Board"].values, "AAA.US")


# ----------------------------------------------------------------------------- N9 (v1.1.0 [EVENTS]) the events calendar
def test_n9_the_real_calendar_file_is_clean():
    path = ROOT / "data" / "news_event_calendar.json"
    raw_text = path.read_text(encoding="utf-8")
    assert raw_text.isascii() and nb.CALENDAR_FILE != str(path)                # this suite reads the invented one
    raw = json.loads(raw_text)
    assert set(raw) == {"version", "checked", "about", "kinds", "statuses", "events"} and raw["version"] == "1.0.0"
    assert nb._strict_day(raw["checked"]) is not None
    assert raw["kinds"] == list(nb.KIND_TEXT) and raw["statuses"] == list(nb.STATUS_TEXT)
    fields = ["id", "start", "end", "title", "kind", "status", "time", "why", "markets", "source"]
    ids = []
    for e in raw["events"]:
        assert list(e) == fields, e
        got = nb.clean_event(e)
        assert got is not None, e                                             # every entry is one the page will show
        assert re.match(r"^[a-z0-9]+(?:-[a-z0-9]+)*$", e["id"]) and 5 <= len(e["title"]) <= 70 and e["why"] and e["markets"], e
        assert all(isinstance(e[k], str) and e[k] == e[k].strip() for k in fields), e
        assert (got["end"] - got["start"]).days <= (45 if e["status"] == "date_not_set" else 6), e   # a meeting, not a season
        ids.append(e["id"])
    assert len(ids) == len(set(ids)) >= 8
    starts = [e["start"] for e in raw["events"]]
    assert starts == sorted(starts)
    loaded = nb.load_calendar(str(path))
    assert loaded["error"] == "" and loaded["dropped"] == 0 and len(loaded["events"]) == len(ids)
    assert {e["kind"] for e in loaded["events"]} >= {"summit", "central_bank", "energy", "deadline"}
    module = (ROOT / "scripts/news_board.py").read_text(encoding="utf-8")
    assert '"data", "news_event_calendar.json"' in module and module.count("json.load(") == 1
    assert "urlopen" not in module and "requests" not in module               # the calendar is a repository file: no fetch


def test_n9_an_entry_is_kept_only_when_it_can_be_trusted(tmp_path):
    good = _ev("a", "2026-12-08", "2026-12-09", "Rate decision", "central_bank", "tentative", "Wed 9 Dec, 22:00")
    got = nb.clean_event(good)
    assert got == {"id": "a", "start": dt.date(2026, 12, 8), "end": dt.date(2026, 12, 9), "title": "Rate decision",
                   "kind": "central_bank", "status": "tentative", "time": "Wed 9 Dec, 22:00", "why": "why a", "markets": "US",
                   "source": "https://example.org/a"}
    one_day = dict(good)
    del one_day["end"]
    assert nb.clean_event(one_day)["end"] == dt.date(2026, 12, 8) and nb.clean_event(dict(good, end=""))["end"] == dt.date(2026, 12, 8)
    for bad in (dict(good, start="2026-13-01"), dict(good, start="2026-1-1"), dict(good, start="2026-12-08T10:00"),
                dict(good, start=20261208), dict(good, start=None), dict(good, end="2026-12-07"), dict(good, end="soon"),
                dict(good, title=" "), dict(good, kind="party"), dict(good, kind=""), dict(good, status="maybe"),
                dict(good, source="http://example.org/a"), dict(good, source="https://example.org/a b"),
                dict(good, source="https://localhost/a"), dict(good, source="example.org/a"), dict(good, source=""),
                "Rate decision", None, ["2026-12-08"]):
        assert nb.clean_event(bad) is None, bad
    assert nb.clean_event(dict(good, source="https://example.org"))["source"] == "https://example.org"
    # the file: missing, broken, the wrong shape - said, never raised
    assert nb.load_calendar(str(tmp_path / "none.json")) == {"checked": None, "events": [], "dropped": 0, "error": "file missing"}
    broken = tmp_path / "broken.json"
    broken.write_text("{not json", encoding="utf-8")
    assert nb.load_calendar(str(broken))["error"] == "not valid JSON"
    for shape in ([good], {"events": "none"}, {"checked": "2026-11-19"}):
        broken.write_text(json.dumps(shape), encoding="utf-8")
        assert nb.load_calendar(str(broken)) == {"checked": None, "events": [], "dropped": 0, "error": "no list of events in it"}
    broken.write_text(json.dumps({"checked": "19 Nov 2026", "events": [good, dict(good, kind="party"), "x",
                                                                     dict(good, id="0", start="2026-11-29", end="2026-11-29")]}),
                      encoding="utf-8")
    cal = nb.load_calendar(str(broken))
    assert cal["checked"] is None and cal["dropped"] == 2 and cal["error"] == ""
    assert [e["id"] for e in cal["events"]] == ["0", "a"]                      # soonest first, whatever the file's order
    broken.write_text(json.dumps({"checked": "2026-11-19T08:00:00", "events": [good]}), encoding="utf-8")
    assert nb.load_calendar(str(broken))["checked"] is None                    # a check date is a date, nothing more
    broken.write_text(json.dumps({"checked": "2026-11-19", "events": [good]}), encoding="utf-8")
    assert nb.load_calendar(str(broken))["checked"] == TODAY
    assert [e["id"] for e in nb.load_calendar()["events"]] == ["z", "a", "w", "b", "c", "y", "d", "e"]


def test_n9_rows_the_window_and_the_wording():
    cal = nb.load_calendar()
    rows = nb.calendar_rows(cal, TODAY)
    assert [r["title"] for r in rows] == ["Summit A", "Oil meeting B", "Rate decision C", "Summit Y", "Deadline D", "Budget W"]
    assert [(r["date"], r["when"]) for r in rows] == [
        ("Wed 18 Nov - Thu 19 Nov 2026", "ends today"), ("Sun 29 Nov 2026", "in 10 days"),
        ("Tue 8 Dec - Wed 9 Dec 2026", "in 19 days"), ("Wed 30 Dec 2026 - Sat 2 Jan 2027", "in 41 days"),
        ("Mon 18 Jan 2027", "in 60 days"), ("by Thu 31 Dec 2026", "date not announced")]
    c = rows[2]
    assert (c["kind"], c["time"], c["why"], c["markets"], c["status"], c["source"], c["dated"]) == (
        "Central bank", "Wed 9 Dec, 22:00", "why c", "US", "tentative", "https://example.org/c", True)
    assert (rows[1]["kind"], rows[4]["kind"], rows[5]["kind"], rows[5]["status"], rows[5]["dated"]) == (
        "Oil", "Deadline", "Budget", "date not announced", False)

    def when(day):
        return {r["title"]: r["when"] for r in nb.calendar_rows(cal, day)}
    assert when(dt.date(2026, 11, 18)) ["Summit A"] == "starts today" and when(dt.date(2026, 11, 18))["Summit Z"] == "ends today"
    assert when(dt.date(2026, 11, 12))["Summit Z"] == "under way" and "Summit Z" not in when(TODAY)
    assert when(dt.date(2026, 11, 28))["Oil meeting B"] == "tomorrow" and when(dt.date(2026, 11, 29))["Oil meeting B"] == "today"
    assert "Oil meeting B" not in when(dt.date(2026, 11, 30)) and "Summit A" not in when(dt.date(2026, 11, 20))
    assert "Deadline E" not in when(TODAY) and when(dt.date(2026, 11, 20))["Deadline E"] == "in 60 days"
    assert when(dt.date(2026, 12, 31))["Budget W"] == "date not announced" and "Budget W" not in when(dt.date(2027, 1, 1))
    assert when(dt.date(2027, 1, 1))["Summit Y"] == "under way" and when(dt.date(2027, 1, 2))["Summit Y"] == "ends today"
    assert [r["title"] for r in nb.calendar_rows(cal, TODAY, days=10)] == ["Summit A", "Oil meeting B", "Budget W"]
    assert nb.calendar_rows({"events": []}, TODAY) == [] and nb.calendar_rows({}, TODAY) == []


def test_n9_notes_say_when_the_calendar_itself_needs_care():
    cal = nb.load_calendar()
    assert nb.calendar_notes(cal, TODAY) == []
    assert nb.calendar_notes(cal, dt.date(2026, 12, 10)) == []                              # 21 days after the check
    assert nb.calendar_notes(cal, dt.date(2026, 12, 11)) == [
        "The calendar was last checked on Thu 19 Nov 2026 (22 days ago); dates may have moved since."]
    # the last dated event starts on 19 Jan 2027: fourteen days before it the file is running out
    assert nb.calendar_notes(dict(cal, checked=dt.date(2027, 1, 5)), dt.date(2027, 1, 5)) == []
    assert nb.calendar_notes(dict(cal, checked=dt.date(2027, 1, 6)), dt.date(2027, 1, 6)) == [
        "The calendar holds no dated event after Tue 19 Jan 2027; it needs new dates."]
    # ... measured on the day the last event begins, also when it runs over several days
    short = dict(cal, events=[e for e in cal["events"] if e["id"] in ("a", "y")])
    assert nb.calendar_notes(dict(short, checked=dt.date(2026, 12, 16)), dt.date(2026, 12, 16)) == []
    assert nb.calendar_notes(dict(short, checked=dt.date(2026, 12, 17)), dt.date(2026, 12, 17)) == [
        "The calendar holds no dated event after Wed 30 Dec 2026; it needs new dates."]
    undated = dict(cal, events=[e for e in cal["events"] if e["status"] == "date_not_set"])
    assert nb.calendar_notes(undated, TODAY) == ["The calendar holds no dated event after today; it needs new dates."]
    assert nb.calendar_notes(dict(cal, checked=None), TODAY) == [
        "The calendar does not say when it was last checked; dates may have moved."]
    assert nb.calendar_notes(dict(cal, dropped=1), TODAY) == ["1 calendar entry could not be read and is left out."]
    assert nb.calendar_notes(dict(cal, dropped=3), TODAY) == ["3 calendar entries could not be read and are left out."]
    both = nb.calendar_notes(dict(cal, dropped=2, checked=dt.date(2026, 10, 1)), TODAY)
    assert len(both) == 2 and both[1].startswith("The calendar was last checked on Thu 1 Oct 2026 (49 days ago)")
    assert nb.calendar_notes({"error": "file missing", "events": [], "checked": None, "dropped": 0}, TODAY) == [
        "The events calendar could not be read (file missing). No event is shown; the rest of the page is unaffected."]


def test_n9_the_line_under_the_verdict_names_the_next_dated_event():
    cal = nb.load_calendar()

    def line(day):
        return nb.next_event_line(nb.calendar_rows(cal, day), day)
    assert line(TODAY) == "NEXT EVENT (ends today): Summit A, Wed 18 Nov - Thu 19 Nov 2026. Section 6 lists the next 60 days."
    assert line(dt.date(2026, 11, 20)) == "NEXT EVENT (in 9 days): Oil meeting B, Sun 29 Nov 2026. Section 6 lists the next 60 days."
    assert line(dt.date(2026, 11, 30)).startswith("NEXT EVENT (in 8 days): Rate decision C, Tue 8 Dec - Wed 9 Dec 2026.")
    assert line(dt.date(2026, 12, 16)).startswith("NEXT EVENT (in 14 days): Summit Y,")
    assert line(dt.date(2026, 12, 10)) == ""                           # the next dated one is 20 days away; the budget has no date
    assert nb.next_event_line([], TODAY) == ""


def test_n9_the_page_shows_the_events_and_is_built_without_them(monkeypatch, tmp_path, capsys):
    book = Book({"My_Portfolio": Sheet(PORTFOLIO)})
    assert nb.run_once(False, book=book, session=Session(), now=NOW, query=eb.bq_query) == 0
    out = capsys.readouterr().out
    page = book.tabs["News_Board"].values
    firsts = [r[0] for r in page]
    verdict, world = firsts.index("VERDICT"), next(i for i, c in enumerate(firsts) if str(c).startswith("1. WORLD NEWS"))
    assert firsts[world - 2] == "NEXT EVENT (ends today): Summit A, Wed 18 Nov - Thu 19 Nov 2026. Section 6 lists the next 60 days."
    assert firsts[world - 1] == "" and world - 2 > verdict                    # the last line of the verdict block
    head = next(i for i, c in enumerate(firsts) if str(c).startswith("6. EVENTS AHEAD"))
    assert firsts[head] == ("6. EVENTS AHEAD - dated events of the next 60 days, calendar checked Thu 19 Nov 2026. "
                            "For information: no order comes from this section.")
    assert page[head + 1][:9] == ["Date", "When", "Event", "Kind", "Time (Riyadh)", "Why it matters", "Markets", "Status", "Source"]
    assert page[head + 2][:9] == ["Wed 18 Nov - Thu 19 Nov 2026", "ends today", "Summit A", "Summit", "", "why a", "US",
                                  "confirmed", "https://example.org/a"]
    assert page[head + 4][:9] == ["Tue 8 Dec - Wed 9 Dec 2026", "in 19 days", "Rate decision C", "Central bank",
                                  "Wed 9 Dec, 22:00", "why c", "US", "tentative", "https://example.org/c"]
    assert page[head + 7][:3] == ["by Thu 31 Dec 2026", "date not announced", "Budget W"]
    assert firsts[head + 8].startswith("The news gate already raises CAUTION by itself within 30 hours of a Fed decision")
    assert firsts[head + 9] == "" and firsts[head + 10].startswith("7. COMPANY NEWS, RE-READ")
    assert any(str(c).startswith("Promotion rule, fixed on 19 Nov 2026") for c in firsts[:head])     # after the trial
    assert "EVENTS 6 in the next 60 days" in out and "NEXT EVENT (ends today): Summit A" in out and "6 event(s) ahead" in out
    assert "EVENTS note" not in out

    def build(calendar_text, now=NOW):
        (tmp_path / "calendar.json").write_text(calendar_text, encoding="utf-8")
        b = Book({"My_Portfolio": Sheet(PORTFOLIO)})
        assert nb.run_once(False, book=b, session=Session(), now=now, query=eb.bq_query) == 0   # the page is always built
        cells = [r[0] for r in b.tabs["News_Board"].values]
        start = next(i for i, c in enumerate(cells) if str(c).startswith("6. EVENTS AHEAD"))
        return b.tabs["News_Board"].values, cells, start, capsys.readouterr().out
    # a broken file: said in the section and in the run log; no line under the verdict; the other sections as they were
    bad_page, cells, start, log = build("{broken")
    assert cells[start] == "6. EVENTS AHEAD - dated events of the next 60 days. For information: no order comes from this section."
    assert bad_page[start + 2][:2] == ["", "No listed event in the next 60 days."]
    assert cells[start + 3] == ("The events calendar could not be read (not valid JSON). No event is shown; the rest of the "
                                "page is unaffected.")
    assert not any(str(c).startswith("NEXT EVENT") for c in cells)
    assert "EVENTS 0 in the next 60 days" in log and "EVENTS note: The events calendar could not be read (not valid JSON)" in log
    assert [r for r in bad_page if r[0] == "AAA.US"] == [r for r in page if r[0] == "AAA.US"]
    assert bad_page[:verdict + 1] == page[:verdict + 1] and len(bad_page) == len(page)
    # an old calendar with one entry that cannot be read: both are said under the table
    old = dict(CAL, checked="2026-10-01", events=CAL["events"] + [{"id": "x", "start": "soon"}])
    _, cells, start, log = build(json.dumps(old))
    assert cells[start].startswith("6. EVENTS AHEAD - dated events of the next 60 days, calendar checked Thu 1 Oct 2026.")
    assert cells[start + 8] == "1 calendar entry could not be read and is left out."
    assert cells[start + 9] == "The calendar was last checked on Thu 1 Oct 2026 (49 days ago); dates may have moved since."
    assert cells[start + 10].startswith("The news gate already raises CAUTION")
    assert log.count("EVENTS note: ") == 2
    # a caller's own calendar is used as given
    b = Book({"My_Portfolio": Sheet(PORTFOLIO)})
    own = {"checked": TODAY, "events": [nb.clean_event(_ev("q", "2026-11-21", "2026-11-21", "Deadline Q", "deadline"))],
           "dropped": 0, "error": ""}
    assert nb.run_once(True, book=b, session=Session(), now=NOW, query=eb.bq_query, calendar=own) == 0
    log = capsys.readouterr().out
    assert "NEXT EVENT (in 2 days): Deadline Q, Sat 21 Nov 2026." in log and "1 event(s) ahead" in log
    assert "EVENTS note: The calendar holds no dated event after Sat 21 Nov 2026; it needs new dates." in log



# ----------------------------------------------------------------------------- N10 (v1.2.0 [COMPANY]) the holdings' own news
def test_n10_classes_and_readings_follow_the_view_thresholds():
    assert [nb.news_class(s, n) for s, n in ((2.5, 3), (1.0, 1), (0.5, 2), (-0.5, 1), (-1.0, 1), (None, 0), (3.0, 0))] == [
        "positive", "positive", "neutral", "neutral", "negative", "none", "none"]
    assert [nb.provider_reading(v) for v in (0.99, 0.15, 0.1, -0.15, None)] == ["positive", "positive", "neutral", "negative", ""]
    view = (ROOT / "sql/bigquery/98_v_company_news_eval.sql").read_text(encoding="utf-8")
    assert "n.x >= 1 THEN 'positive'" in view and "n.x <= -1 THEN 'negative'" in view          # the same thresholds
    assert "n.x >= 0.15 THEN 'positive'" in view and "n.x <= -0.15 THEN 'negative'" in view
    classes = (ROOT / "sql/bigquery/97_v_symbol_news_class.sql").read_text(encoding="utf-8")
    for ev in re.findall(r"'([A-Z_]+)' AS event_type|\(\d+, '([A-Z_]+)',", classes):
        name = ev[0] or ev[1]
        assert name in nb.EVENT_TEXT, name                                                    # every event has words
    rows = nb.company_rows([{"symbol": "AAA", "name": "Alpha"}, {"symbol": "BBB.PA", "name": ""},
                            {"symbol": "CCC.US", "name": "Charlie"}], COMPANY_ROWS)
    assert [r["class"] for r in rows] == ["positive", "none", "none"]
    assert rows[0]["event"] == "results above estimates" and rows[0]["day"] == "2026-11-17" and rows[0]["provider"] == "positive"
    assert rows[1]["company"] == 0 and rows[1]["other"] == 2 and rows[1]["title"] == "" and rows[1]["note"] == ""
    assert rows[2]["note"] == "no headline in the archive for these 7 days" and rows[2]["provider"] == ""
    long = [dict(COMPANY_ROWS[0], top_title="x" * 200)]
    assert nb.company_rows([{"symbol": "AAA.US", "name": ""}], long)[0]["title"] == "x" * 160 + "..."
    line = nb.company_record_line(PEER_ROWS)                          # v1.3.0: the view of apply_bq_views 1.17.0
    assert line == ("Record of the new classes (US names, the 5 sessions after the news day, against the same-day average "
                    "of the US names, about 490 of them): positive +0.69 pts on 52 day(s), ahead on 63.5 %; neutral +0.67 "
                    "pts on 186 day(s), ahead on 61.8 %; negative +2.55 pts on 5 day(s), ahead on 100.0 % - reading: NO "
                    "EDGE. Against the S&P 500 instead: positive +1.14; neutral +1.16; negative +3.19 pts. The provider's "
                    "own reading, scored the same way: BIASED (class > 90 %).")
    old = nb.company_record_line(EVAL_ROWS)                            # the view before it: the 1.2.0 line, said so
    assert old == ("Record of the new classes (US names, the 5 sessions after the news day, against the S&P 500; the "
                   "comparison with the same-day US names comes with the next BigQuery views update): positive +1.17 pts "
                   "on 51 day(s); neutral +1.15 pts on 187 day(s); negative +3.19 pts on 5 day(s) - reading: NO EDGE. "
                   "The provider's own reading, scored the same way: BIASED (class > 90 %).")
    empty = [dict(PEER_ROWS[2], mean_5d_peer_pp=None, matured_5d_peer="0", beat_5d_peer_pct=None)]
    assert nb.company_record_line(empty).startswith("Record of the new classes (US names, the 5 sessions after the news "
                                                    "day, against the same-day average of the US names, about 490 of "
                                                    "them): positive: no matured day yet - reading: NO EDGE.")
    assert nb.company_record_line([]) == "Record of the new classes: no scored day yet."


def test_n10_the_page_shows_each_holding_and_the_log_shows_counts_only(capsys):
    book = Book({"My_Portfolio": Sheet(PORTFOLIO)})
    assert nb.run_once(False, book=book, session=Session(), now=NOW, query=eb.bq_query) == 0
    out = capsys.readouterr().out
    page = book.tabs["News_Board"].values
    firsts = [r[0] for r in page]
    head = next(i for i, c in enumerate(firsts) if str(c).startswith("7. COMPANY NEWS, RE-READ"))
    assert firsts[head] == ("7. COMPANY NEWS, RE-READ - your holdings' headlines of the last 7 days, classified from their own "
                           "text. For information: no order comes from this section.")
    assert page[head + 1][:11] == ["Symbol", "Name", "News (7 days)", "Score", "Headlines about the company",
                                   "Filed under it, about others", "Main headline", "Its event", "Day", "Provider's reading (old)",
                                   "Note"]
    assert page[head + 2][:11] == ["AAA.US", "Alpha Bank", "POSITIVE", "+2.5", "3", "1",
                                   "Alpha Bank (AAA) Tops Q3 Earnings and Revenue Estimates", "results above estimates",
                                   "2026-11-17", "positive", ""]
    assert page[head + 3][:11] == ["BBB.PA", "Bravo Tech", "no company news", "", "0", "2", "", "", "", "positive", ""]
    assert page[head + 4][:11] == ["CCC.US", "Charlie Unknown", "no company news", "", "0", "0", "", "", "", "",
                                   "no headline in the archive for these 7 days"]
    assert page[head + 5][:10] == ["DDD.US", "", "NEGATIVE", "-2.0", "1", "0", "Delta Power (DDD) Lags Q3 Earnings Estimates",
                                   "results below estimates", "2026-11-18", "positive"]
    assert firsts[head + 6].startswith("Record of the new classes (US names, the 5 sessions after the news day")
    assert firsts[head + 7].startswith("A headline counts for a holding only when it names the company")
    assert firsts[head + 8] == "" and firsts[head + 9].startswith("How to read:")
    assert "COMPANY NEWS 4 holding(s): 1 positive, 1 negative, 0 neutral, 2 without company news; Record of the new classes" in out
    for sym in ("AAA", "BBB", "CCC", "DDD", "Alpha", "Delta"):
        assert sym not in out, sym                                                         # holdings stay out of the run log


# ----------------------------------------------------------------------------- N11 (v1.3.0 [PEERS]) the record against the peers
def test_n11_the_record_reads_the_peers_and_waits_for_the_view(capsys):
    book, sess = Book({"My_Portfolio": Sheet(PORTFOLIO)}), Session()
    assert nb.run_once(False, book=book, session=sess, now=NOW, query=eb.bq_query) == 0
    firsts = [str(r[0]) for r in book.tabs["News_Board"].values]
    assert any(c.startswith("Record of the new classes (US names, the 5 sessions after the news day, against the same-day "
                            "average of the US names, about 490 of them): positive +0.69 pts on 52 day(s)") for c in firsts)
    assert len(sess.sql) == 9 and "mean_5d_peer_pp" in sess.sql[8]
    capsys.readouterr()
    # the view before apply_bq_views 1.17.0: BigQuery does not know the peer columns - the 1.2.0 read, and the page says so
    old_book, old_sess = Book({"My_Portfolio": Sheet(PORTFOLIO)}), Session(old_view=True)
    assert nb.run_once(False, book=old_book, session=old_sess, now=NOW, query=eb.bq_query) == 0
    out = capsys.readouterr().out
    assert len(old_sess.sql) == 10 and "mean_5d_peer_pp" in old_sess.sql[8] and "mean_5d_peer_pp" not in old_sess.sql[9]
    assert "the view has no peer columns yet (apply_bq_views 1.17.0 not applied); read against the S&P 500 only" in out
    firsts = [str(r[0]) for r in old_book.tabs["News_Board"].values]
    assert any(c.startswith("Record of the new classes (US names, the 5 sessions after the news day, against the S&P 500; "
                            "the comparison with the same-day US names comes with the next BigQuery views update): "
                            "positive +1.17 pts on 51 day(s)") for c in firsts)
    for sym in ("AAA", "BBB", "CCC", "DDD"):
        assert sym not in out                                                    # the log names no holding
    # any other refusal of the peer read still fails the run and leaves the page as it was
    kept = Book({"My_Portfolio": Sheet(PORTFOLIO), "News_Board": Sheet([["old page"]])})
    assert nb.run_once(False, book=kept, session=Session(fail="mean_5d_peer_pp"), now=NOW, query=eb.bq_query) == 2
    assert kept.tabs["News_Board"].values == [["old page"]] and kept.tabs["News_Board"].updates == 0
    assert "nothing written" in capsys.readouterr().out
