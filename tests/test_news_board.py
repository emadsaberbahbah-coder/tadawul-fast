"""scripts/news_board.py v1.0.0 - the News Board (news learning loop, phase B): the week's world news by theme, the
sector tilt of both arms, the holdings beside their sector's tilt, the shadow list and the trial, read from the
BigQuery news views on every run. Pure functions, the SQL builders, and full runs through the real BigQuery reader on
a fake session and a fake workbook (no network). Every figure here is invented."""
from __future__ import annotations

import datetime as dt
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
PORTFOLIO = [["MY PORTFOLIO"], [], ["Symbol", "Name"], ["aaa.us", "Alpha Bank"], ["BBB.PA", "Bravo Tech"], ["", ""],
             ["CCC.US", "Charlie Unknown"], ["AAA.US", "dup"], ["not a symbol", "x"], ["TOTAL", ""], ["DDD.US", ""]]


def payload(rows):
    """Rows as the BigQuery REST API returns them (every value a string or null)."""
    names = list(rows[0]) if rows else []
    return {"jobComplete": True, "schema": {"fields": [{"name": n} for n in names]},
            "rows": [{"f": [{"v": r.get(n)} for n in names]} for r in rows]}


class Resp:
    def __init__(self, body, status=200):
        self._body, self.status_code, self.text = body, status, "refused"

    def json(self):
        return self._body


class Session:
    """Answers each query of the board by the view it reads; records the SQL."""

    def __init__(self, trial=None, last_day="2026-11-18", fail=None, tilt=None, shadow=None):
        self.sql, self.trial, self.last_day, self.fail = [], TRIAL_ROWS if trial is None else trial, last_day, fail
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


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for name in list(__import__("os").environ):
        if name.startswith("TFB_NEWS_BOARD") or name.startswith("TFB_BQ_"):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TFB_BQ_PROJECT", "proj-1")


def col(page, first):
    return [r for r in page if r and r[0] == first]


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
    rows = nb.holding_rows(nb.read_holdings(PORTFOLIO), sectors, tilt, names)
    a, b, c, d = rows
    assert (a["symbol"], a["sector"], a["events"], a["all"], a["listed"], a["shadow"], a["note"]) == (
        "AAA.US", "Financials", "avoided", "avoided", "yes", "would leave", "")
    assert (b["sector"], b["events"], b["all"], b["listed"], b["shadow"]) == ("Information Technology", "favoured", "favoured", "no", "")
    assert b["note"] == "the tilt is measured on US sector funds: indicative only for this market"
    assert (c["sector"], c["events"], c["all"], c["note"]) == ("", "", "", "sector not known to the archive")
    assert (d["sector"], d["events"], d["all"], d["note"]) == ("Utilities", "", "", "")      # a sector the tilt does not list
    stale = nb.holding_rows(nb.read_holdings(PORTFOLIO), sectors, nb.pick_tilt(TILT_ROWS, dt.date(2026, 11, 24)), names)
    assert (stale[0]["events"], stale[0]["all"]) == ("", "")                               # no tilt in force: none claimed
    assert nb.holding_rows([], sectors, tilt, names) == [] and nb.market_of("5023.SR") == "SR" and nb.market_of("X") == ""


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
            nb.sql_sectors(DS, ["aaa.us", "x' OR '1'='1", "5023.SR"]), nb.sql_shadow(DS, TODAY), nb.sql_trial(DS)]
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
    assert len(sess.sql) == 7
    page = book.tabs["News_Board"].values
    assert all(len(r) == nb.WIDTH for r in page) and len(page) >= 120
    cells = [str(c) for r in page for c in r]
    assert not any("None" in c or c.lower() in ("nan", "+nan", "-nan", "inf") for c in cells)
    assert page[0][0].startswith("NEWS BOARD - ") and page[1][0].startswith("Built 2026-11-19 08:00 Riyadh | news_board 1.0.0 | SHADOW")
    assert "only the Today Action Plan gives orders" in page[1][0]
    heads = [r[0] for r in page if re.match(r"^(VERDICT|\d\. )", str(r[0]))]
    assert [h.split(" - ")[0].split(" (")[0].split(" for ")[0] for h in heads] == [
        "VERDICT", "1. WORLD NEWS", "2. SECTOR TILT", "3. YOUR HOLDINGS AND THE TILT", "4. SHADOW LIST", "5. TRIAL"]
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
    out = capsys.readouterr().out
    assert "wrote the News_Board page" in out and "4 holding(s)" in out and "4 moved name(s)" in out
    assert ("TILT events week 2026-11-09, in force 2026-11-17 to 2026-11-23 | favoured: Information Technology | avoided: "
            "Consumer Staples, Financials") in out
    assert "SHADOW events 2026-11-19: list 3, 2 would leave, 2 would enter" in out
    for sym in ("AAA.US", "BBB.PA", "CCC.US", "DDD.US", "Alpha", "Bravo"):
        assert sym not in out, sym                                                         # holdings stay out of the run log


def test_n6_dry_run_off_switch_and_failures_write_nothing(monkeypatch, capsys):
    book, sess = Book({"My_Portfolio": Sheet(PORTFOLIO)}), Session()
    assert nb.run_once(True, book=book, session=sess, now=NOW, query=eb.bq_query) == 0
    assert "News_Board" not in book.tabs and len(sess.sql) == 7
    assert "dry run:" in capsys.readouterr().out
    assert nb.main(["--dry-run"], book=book, session=Session(), now=NOW, query=eb.bq_query) == 0 and "News_Board" not in book.tabs
    for view in ("v_news_theme_weekly", "v_news_sector_tilt", "v_symbol_sector", "v_news_overlay_list", "v_trial_news_overlay",
                 "news_theme_daily"):
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
    assert len(sess.sql) == 6 and not any("v_symbol_sector" in s for s in sess.sql)        # nothing to look up
    page = book.tabs["News_Board"].values
    flat = [r[0] for r in page] + [r[1] for r in page]
    for want in ("WARNING: the news archive could not be dated; treat the tilt below as unconfirmed.",
                 "WARNING: no sector tilt is in force for today; sections 2 to 4 show the last one, for reference.",
                 "2. SECTOR TILT - no tilt has begun to apply yet", "No holding could be read from My_Portfolio.",
                 "4. SHADOW LIST for no list day - names the news tilt would move. It is NOT an order.",
                 "No name would move on this list day.", "The trial has no row yet.", "3. YOUR HOLDINGS AND THE TILT (0 holding(s))"):
        assert want in flat, want
    late = dt.datetime(2026, 11, 24, 5, 0, tzinfo=UTC)                 # the window closed and no newer week came
    book = Book({"My_Portfolio": Sheet(PORTFOLIO)})
    assert nb.run_once(False, book=book, session=Session(last_day="2026-11-19"), now=late, query=eb.bq_query) == 0
    page = book.tabs["News_Board"].values
    flat = [r[0] for r in page]
    assert "2. SECTOR TILT - NOT in force today; it applied 2026-11-17 to 2026-11-23, from the news of the week of 2026-11-09" in flat
    assert "WARNING: the news archive stops at 2026-11-19 (5 days ago); the tilt below may be out of date." in flat
    assert col(page, "AAA.US")[0][3:5] == ["", ""]                    # no tilt is claimed for a holding


# ----------------------------------------------------------------------------- N7 wiring
def test_n7_workflow_manifest_and_read_only_scope():
    raw = (ROOT / ".github/workflows/news_board.yml").read_text(encoding="utf-8")
    wf = yaml.safe_load(raw)
    on = wf[True] if True in wf else wf["on"]
    assert on["schedule"] == [{"cron": "55 4,13 * * *"}] and set(on) == {"schedule", "workflow_dispatch"}
    assert on["workflow_dispatch"]["inputs"]["dry_run"]["default"] is False
    assert wf["name"] == "\U0001F4F0 News Board" and wf["permissions"] == {"contents": "read"}
    assert wf["concurrency"] == {"group": "tfb-news-board", "cancel-in-progress": False}
    job = wf["jobs"]["board"]
    assert "if" not in job and job["timeout-minutes"] == 20
    step = [s for s in job["steps"] if "news_board.py" in str(s.get("run", ""))][0]
    assert step["env"]["TFB_NEWS_BOARD"] == "${{ vars.TFB_NEWS_BOARD || 'on' }}"
    assert step["env"]["TFB_BQ_PROJECT"] == "${{ vars.TFB_BQ_PROJECT || 'project-2e3530e6-2311-4aa3-b36' }}"
    assert step["env"]["TFB_BQ_DATASET"] == "${{ vars.TFB_BQ_DATASET || 'tfb_archive' }}"
    assert "--dry-run" in step["run"] and 'python scripts/news_board.py "${ARGS[@]}"' in step["run"]
    src = (ROOT / "scripts/news_board.py").read_text(encoding="utf-8")
    for name in re.findall(r"(TFB_[A-Z_]+): \$\{\{", raw):
        assert '"%s"' % name in src, name
    assert src.isascii() and nb.__version__ == "1.0.0" and "v1.0.0 (2026-11-19)" in src
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
    assert waits == [65.0] and book.tabs["News_Board"].updates == 1 and len(sess.sql) == 7     # the queries are not repeated
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
