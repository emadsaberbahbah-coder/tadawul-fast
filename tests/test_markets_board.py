"""scripts/markets_board.py v1.2.0 - the Markets_Board tab: the condition of every market, their order by trend and
risk, the last 14 days, the scorecard of the forward test, (1.1.0) the ready list, the switch check and the ready
list's scorecard, and (1.2.0) the next event, the events of the next 14 days with what the earlier ones did, the last
7 days against their band and the events scorecard, observe-only. Pure functions, the SQL, full runs through
the real BigQuery reader on a fake session and a fake workbook (no network), and the workflow's gate run in bash.
Every figure here is invented."""
from __future__ import annotations

import datetime as dt
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest
import yaml

from scripts import evidence_board as eb
from scripts import markets_board as mb

ROOT = Path(__file__).resolve().parents[1]
UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 21, 8, 40, tzinfo=UTC)          # Sat 21 Nov 2026, 11:40 Riyadh
TODAY = dt.date(2026, 11, 21)
DS = "proj-1.tfb_archive"
NAMES = {"us": "United States", "sa": "Saudi Arabia", "eu": "Europe", "jp": "Japan", "cn": "China / Hong Kong",
         "gold": "Gold", "oil": "Oil", "bonds": "Bonds", "sukuk": "Sukuk", "reits_us": "US REITs",
         "reits_sa": "Saudi REITs"}
FUNDS = {"us": "SPY", "sa": "KSA", "eu": "VGK", "jp": "EWJ", "cn": "MCHI", "gold": "GLD", "oil": None, "bonds": "IEF",
         "sukuk": "SPSK", "reits_us": "VNQ", "reits_sa": "SA_REITS"}


def mrow(as_of, mid, rank, label, trend, opened=0, buyable_n=10, session=None, old=0, waiting="x under its high"):
    state = "open" if label == "calm" else "hold"
    return {"as_of": as_of, "market_id": mid, "market": NAMES[mid], "market_order": str(mb.MARKETS.index(mid) + 1),
            "reading_symbol": "R-" + mid, "reading_name": "Index of " + NAMES[mid], "fund_symbol": FUNDS[mid],
            "buyable": "false" if mid == "oil" else "true", "session": session or as_of, "days_old": str(old),
            "label": label, "state": state, "rank": str(rank), "buyable_rank": None if mid == "oil" else str(rank),
            "trend_score": str(trend), "r1_pct": "-1.23456", "r5_pct": "-2.5", "r20_pct": "1.5", "vs_50d_pct": "-0.4",
            "vol20_pct": "18.25", "dd_from_high_pct": "-9.123", "normal_vol_pct": "16.0", "k": "1.0", "vix_close": None,
            "waiting_for": "" if label == "calm" else waiting, "fund_r5_pct": None if mid == "oil" else "-1.1",
            "fund_r20_pct": None if mid == "oil" else "2.2", "open_markets": str(opened), "buyable_markets": str(buyable_n),
            "verdict": "ALL ON HOLD - cash stays cash" if opened == 0 else "%d of %d open" % (opened, buyable_n)}


# Fri 20 Nov: ten markets read (the Saudi REITs left out: no close in 6 days), every one on hold
DAY20 = [mrow("2026-11-20", m, i + 1, lab, t, session=s, old=o) for i, (m, lab, t, s, o) in enumerate([
    ("bonds", "drawdown", 0.211, None, 0), ("sa", "drawdown", -0.09, "2026-11-19", 1), ("reits_us", "drawdown", -0.33, None, 0),
    ("oil", "weak", -0.5, "2026-11-17", 3), ("eu", "crash_day", -0.587, None, 0), ("sukuk", "drawdown", -0.7, None, 0),
    ("cn", "drawdown", -0.966, None, 0), ("jp", "crash_day", -1.1, None, 0), ("gold", "drawdown", -1.333, "2026-11-19", 1),
    ("us", "crash_day", -1.903, None, 0)])]
DAY19 = [mrow("2026-11-19", m, i + 1, "drawdown", -0.1 * i) for i, m in enumerate(mb.MARKETS)]
DAY18 = [mrow("2026-11-18", m, i + 1, "weak", -0.1 * i) for i, m in enumerate(mb.MARKETS)]
DAY17 = ([mrow("2026-11-17", "gold", 1, "calm", 0.9, opened=2), mrow("2026-11-17", "bonds", 2, "calm", 0.4, opened=2)]
         + [mrow("2026-11-17", m, i + 3, "weak", -0.1 * i, opened=2) for i, m in enumerate(
             x for x in mb.MARKETS if x not in ("gold", "bonds"))])
RANK = DAY20 + DAY19 + DAY18 + DAY17


def srow(period, h, **kw):
    base = {"period": period, "horizon": str(h), "days": "755", "open_n": "3400", "open_mean_pct": "0.11",
            "open_up_pct": "53.7", "open_p5_pct": "-3.1", "hold_n": "2736", "hold_mean_pct": "0.25", "hold_up_pct": "55",
            "hold_p5_pct": "-3.67", "open_minus_hold_pp": "-0.14", "top_n": "723", "top_mean_pct": "0.09",
            "top_us_n": "253", "top_vs_spy_pp": "-0.37", "top_beat_spy_pct": "46", "cash_days": "26",
            "cash_spy_mean_pct": "0.92", "ic_days": "629", "mean_ic": "-0.004"}
    base.update({k: (None if v is None else str(v)) for k, v in kw.items()})
    return base


SUMMARY = [srow("backtest", 5), srow("backtest", 10, open_mean_pct=0.28, hold_mean_pct=0.49, mean_ic=0.011),
           srow("backtest", 20, cash_days=24, cash_spy_mean_pct=3.73, mean_ic=0.041)]


def rdy(rank, sym, mid, state, label=None, ccy="USD", us="crash_day", f1m=3.0, page="Global_Markets"):
    names = dict(NAMES)
    return {"snapshot_date": "2026-11-21", "symbol": sym, "name": "Name " + sym, "source_page": page, "currency": ccy,
            "market_country": "South Korea" if mid is None else NAMES.get(mid, ""), "market_id": mid,
            "market": names.get(mid) if mid else None, "market_state": state, "market_label": label,
            "market_waiting_for": "x under its high" if state == "hold" else "", "market_rank": "2",
            "ready_rank": str(rank), "in_ready": "true" if rank <= 3 else "false", "qualified": "6",
            "expected_roi_1m_pct": str(f1m), "overall_score": "71.25", "us_label": us,
            "waiting_for": {"hold": "calmer market", "open": "cash or a switch"}.get(state, "market not read")}


# the ready order of Sat 21 Nov: two names in open markets (one funded, one not), one switch, two held, one not read
READY = [rdy(1, "AAA.SR", "sa", "open", "calm", ccy="SAR", f1m=4.5), rdy(2, "BBB.US", "us", "open", "calm", us="calm"),
         rdy(3, "CCC.HK", "cn", "open", "calm", ccy="HKD"), rdy(4, "DDD.US", "reits_us", "hold", "drawdown"),
         rdy(5, "EEE.L", "eu", "hold", "crash_day", ccy="GBP"), rdy(6, "FFF.KS", None, "not read", ccy="KRW")]
FINALS = [["Symbol", "Decision", "Do today"], ["AAA.SR", "BUY", "Buy 12 at 31.5 (limit)"],
          ["BBB.US", "WATCH", "Do not buy today: not funded (needs 9,000 SAR, 1,200 SAR left)."],
          ["HHH.US", "HOLD", "Keep the stop at 40"], ["", ""], ["aaa.sr", "SELL", "dup"]]


def sw(sym, h, order, verdict, best="ZZZ.US", net=-0.5, chance=40, reason="the holding has the higher forecast"):
    return {"symbol": sym, "name": "Name " + sym, "horizon": h, "horizon_order": str(order), "holding_forecast_pp": "1.2",
            "best_symbol": best, "best_forecast_pp": "3.4", "net_pp": str(net), "chance_pct": str(chance),
            "verdict": verdict, "reason": reason}


SWITCH = ([sw("HHH.US", h, i + 1, "KEEP") for i, h in enumerate(("1W", "2W", "1M", "3M"))]
          + [sw("JJJ.US", "1W", 1, "KEEP", best="CCC.HK"),
             sw("JJJ.US", "1M", 3, "SWITCH CANDIDATE", best="CCC.HK", net=1.25, chance=66, reason="proven signal")])


def trow(period, h, grp, n, m, ex, up=50, p5=-4.0, days=15):
    return {"period": period, "horizon": h, "grp": grp, "n": str(n), "days": str(days), "mean_ret_pp": str(m),
            "mean_excess_pp": str(ex), "up_pct": str(up), "p5_ret_pp": str(p5)}


TRIAL = [trow("backtest", "1W", "all qualified", 1148, -0.44, -0.30),            # the first read, 21 Nov 2026
         trow("backtest", "1W", "live US rule does not", 428, 0.69, 0.86),
         trow("backtest", "1W", "live US rule holds it", 720, -1.11, -1.00),
         trow("backtest", "1W", "market rule does not", 380, 0.38, 0.56),
         trow("backtest", "1W", "market rule holds it", 768, -0.84, -0.73),
         trow("backtest", "1W", "other qualified", 1105, -0.39, -0.28),
         trow("backtest", "1W", "ready list (top 3)", 43, -1.73, -0.97)]


def erow(eid, name, at, mid, n, p10, p50, p90, kind="release", period="Oct", est=None, prev=None, p_big=0.3, absz=0.9,
         na=0, nb=0, a50=None, b50=None, nfig=0, pa=None, pi=None, pb=None):
    return {"event_id": eid, "family": eid.split(":")[0], "family_name": name, "kind": kind, "country": "US",
            "event_at": at, "period": period, "estimate": est, "previous": prev,
            "market_order": str(mb.MARKETS.index(mid) + 1), "market_id": mid, "market": NAMES[mid], "n_prior": str(n),
            "r1_p10": p10, "r1_p50": p50, "r1_p90": p90, "p_big": p_big, "mean_abs_z": absz, "n_above": str(na),
            "n_below": str(nb), "r1_p50_above": a50, "r1_p50_below": b50, "n_fig": str(nfig), "p_above": pa,
            "p_in_line": pi, "p_below": pb}


PCE = "2026-11-25 13:30|US|Core PCE Price Index|mom|Oct"
EVENTS = ([erow(PCE, "US core PCE prices (m/m)", "2026-11-25 13:30:00+00", m, 24, lo, mid, hi, est="0.2",
                prev="0.3", na=9, nb=7, a50="-0.41" if m == "us" else None, b50="0.38" if m == "us" else None,
                nfig=24, pa="0.3704", pi="0.3333", pb="0.2963", absz=z)
           for m, lo, mid, hi, z in (("us", -0.9, 0.12, 1.05, 0.85), ("sa", -0.7, -0.05, 0.6, 0.6),
                                     ("oil", -2.2, 0.3, 2.4, 0.7), ("gold", -1.0, 0.25, 1.4, 1.15),
                                     ("bonds", -0.55, 0.08, 0.6, 0.95))]
          + [erow("opec:2026-11-29", "OPEC+ meeting", "2026-11-29 12:00:00+00", m, 27, lo, mid, hi,
                  kind="meeting", period=None, absz=z)
             for m, lo, mid, hi, z in (("us", -1.2, 0.05, 1.1, 0.7), ("oil", -3.1, 0.4, 3.5, 1.35),
                                       ("sa", -0.9, 0.1, 1.0, 0.8))])


def lrow(mid, r1, p10, p90, hit):
    return {"event_id": "2026-11-17 13:30|US|Retail Sales|mom|Oct", "family_name": "US retail sales (m/m)",
            "event_at": "2026-11-17 13:30:00+00", "figure": "above", "estimate": "0.4", "actual": "0.6",
            "market_id": mid, "market_order": str(mb.MARKETS.index(mid) + 1), "r1_pct": r1, "r1_p10": p10,
            "r1_p90": p90, "band_hit": hit, "move": "normal"}


LAST = [lrow("us", "-0.42", "-1.1", "0.9", "true"), lrow("sa", "0.1", None, None, None),
        lrow("oil", "-3.1", "-2.0", "2.2", "false")]


def esum(period, family, name, events, **kw):
    r = {"period": period, "family": family, "family_name": name, "events": str(events), "band_n": "0",
         "band_pct": None, "big_n": "0", "big_pct": None, "any_pct": None, "big_skill": None, "dir_n": "0",
         "dir_events": "0", "dir_pct": None, "dir_z": None, "drift_n": "0", "drift_events": "0", "drift_pct": None,
         "drift_z": None, "fig_n": "0",
         "fig_skill": None, "verdict": "collecting" if period == "forward" else "band too few, direction too few"}
    r.update({k: str(v) for k, v in kw.items()})
    return r


EV_SUMMARY = [esum("backtest", "all", "All families", 96, band_n=719, band_pct=71.2, big_n=564, big_pct=25.7,
                   any_pct=24.9, big_skill=-0.078, dir_n=40, dir_events=6, dir_pct=55.0, dir_z=0.63, drift_n=38,
                   drift_events=5, drift_pct=47.4,
                   fig_n=60, fig_skill=0.041, verdict="band calibrated, direction not proven"),
              esum("backtest", "opec", "OPEC+ meeting", 27, band_n=216, band_pct=74.5, big_n=192, big_pct=24.0,
                   any_pct=24.8, big_skill=-0.06, verdict="band calibrated, direction too few"),
              esum("forward", "all", "All families", 2)]


def payload(rows):
    names = list(rows[0]) if rows else []
    return {"jobComplete": True, "schema": {"fields": [{"name": n} for n in names]},
            "rows": [{"f": [{"v": r.get(n)} for n in names]} for r in rows]}


class Resp:
    def __init__(self, body, status=200, text="refused"):
        self._body, self.status_code, self.text = body, status, text

    def json(self):
        return self._body


class Session:
    def __init__(self, rank=None, summary=None, fail=None, ready=None, switch=None, trial=None, events=None, last=None,
                 ev_summary=None, no_event_views=False):
        self.sql, self.fail = [], fail
        self.rank = RANK if rank is None else rank
        self.summary = SUMMARY if summary is None else summary
        self.ready = READY if ready is None else ready
        self.switch = SWITCH if switch is None else switch
        self.trial = TRIAL if trial is None else trial
        self.events = EVENTS if events is None else events                    # 1.2.0
        self.last = LAST if last is None else last
        self.ev_summary = EV_SUMMARY if ev_summary is None else ev_summary
        self.no_event_views = no_event_views

    def post(self, url, timeout=None, json=None):
        sql = json["query"]
        self.sql.append(sql)
        assert url.endswith("/projects/proj-1/queries") and json["useLegacySql"] is False
        if self.fail and self.fail in sql:
            return Resp({}, 403)
        if ".v_market_rank`" in sql:
            return Resp(payload(self.rank))
        if ".v_market_trial_summary`" in sql:
            return Resp(payload(self.summary))
        if ".v_ready_list`" in sql:
            return Resp(payload(self.ready))
        if ".v_hold_vs_switch`" in sql:
            return Resp(payload(self.switch))
        if ".v_ready_trial`" in sql:
            return Resp(payload(self.trial))
        for view, rows in ((".v_event_scenarios`", self.events), (".v_event_trial`", self.last),
                           (".v_event_trial_summary`", self.ev_summary)):
            if view in sql:
                if self.no_event_views:
                    return Resp({}, 404, "Not found: Table proj-1:tfb_archive%s was not found in location US"
                                % view.rstrip("`"))
                return Resp(payload(rows))
        raise AssertionError("unexpected query: %s" % sql[:80])


class Sheet:
    def __init__(self, rows=None, readable=False):
        self.values, self.row_count, self.col_count, self.updates = list(rows or []), 200, 30, 0
        self.readable, self.ranges = readable, []

    def get(self, rng):
        assert self.readable, "the board reads Final_Decisions only"
        self.ranges.append(rng)
        return [list(r) for r in self.values]

    def resize(self, rows=None, cols=None):
        self.row_count, self.col_count = rows or self.row_count, cols or self.col_count

    def update(self, values=None, range_name=None, value_input_option=None):
        assert value_input_option == "RAW"
        self.updates += 1
        self.values = [list(r) for r in values]


class WorksheetNotFound(Exception):
    pass


class Book:
    def __init__(self, tabs=None):
        self.tabs = dict(tabs or {})

    def worksheet(self, name):
        if name not in self.tabs:
            raise WorksheetNotFound(name)
        return self.tabs[name]

    def add_worksheet(self, title, rows, cols):
        self.tabs[title] = Sheet()
        return self.tabs[title]


def _env(monkeypatch):
    for name in list(os.environ):
        if name.startswith("TFB_MARKETS_BOARD") or name.startswith("TFB_BQ_"):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TFB_BQ_PROJECT", "proj-1")


def cells_of(page):
    return [str(c) for r in page for c in r]


def world(**tabs):
    return Book(dict({"Final_Decisions": Sheet(FINALS, readable=True)}, **tabs))


# ----------------------------------------------------------------------------- M1 the day and the verdict
def test_m1_days_verdict_and_the_run_on_hold():
    days = mb.by_day(RANK)
    assert [d for d, _ in days] == [dt.date(2026, 11, d) for d in (20, 19, 18, 17)]
    assert [r["market_id"] for r in days[0][1]][:3] == ["bonds", "sa", "reits_us"]          # in rank order
    facts = [mb.day_facts(d, r) for d, r in days]
    assert facts[0]["open"] == 0 and facts[0]["buyable"] == 10 and facts[0]["us"] == "crash_day" and facts[0]["read"] == 10
    assert facts[3]["open"] == 2 and facts[3]["open_names"] == ["Gold", "Bonds"] and facts[3]["first"] == "Gold"
    assert mb.hold_streak(facts) == (3, dt.date(2026, 11, 18), False)                    # 20, 19, 18: the 17th was open
    assert mb.hold_streak(facts[:2]) == (2, dt.date(2026, 11, 19), True)                 # reaches the start: at least
    gap = [facts[0], dict(facts[2], day=dt.date(2026, 11, 16))]
    assert mb.hold_streak(gap) == (1, dt.date(2026, 11, 20), False)                      # a calendar gap ends the run
    head, detail = mb.verdict_lines(facts[0], days[0][1], mb.hold_streak(facts))
    assert head == "ALL ON HOLD - cash stays cash"
    assert detail == ("none of the 10 markets with a fund is calm (oil is a reading only); every market on hold for "
                      "3 day(s) in a row, since Wed 18 Nov 2026")
    head, detail = mb.verdict_lines(facts[3], days[3][1], mb.hold_streak(facts[3:]))
    assert head == "2 of 10 OPEN" and detail == "open: Gold, Bonds; first: Gold - a buy would use GLD"
    one = [facts[0], facts[1], dict(facts[2], open=1)]
    assert mb.hold_streak(one) == (2, dt.date(2026, 11, 19), False)                   # one open market ends the run
    # oil has no fund: a calm oil price opens nothing, and the first open market is the first with a fund
    oil_day = ([mrow("2026-11-16", "oil", 1, "calm", 1.5, opened=1), mrow("2026-11-16", "sukuk", 2, "calm", 0.2, opened=1)]
               + [mrow("2026-11-16", m, i + 3, "weak", -0.1 * i, opened=1) for i, m in enumerate(
                   x for x in mb.MARKETS if x not in ("oil", "sukuk"))])
    f = mb.day_facts(dt.date(2026, 11, 16), oil_day)
    assert f["open"] == 1 and f["open_names"] == ["Sukuk"] and f["first"] == "Oil"
    assert mb.verdict_lines(f, oil_day, (0, None, False)) == ("1 of 10 OPEN",
                                                              "open: Sukuk; first: Sukuk - a buy would use SPSK")


# ----------------------------------------------------------------------------- M2 a market's row
def test_m2_a_market_row_says_what_it_waits_for_and_which_fund_a_buy_would_use():
    rows = {r["market_id"]: r for r in DAY20}
    bonds = mb.market_row(rows["bonds"])
    assert len(bonds) == mb.WIDTH and bonds[:5] == [1, "Bonds", "HOLD", "drawdown", "x under its high"]
    assert bonds[5:13] == [0.21, -1.23, -2.5, 1.5, -0.4, 18.25, -9.12, 16.0]
    assert bonds[13:] == ["IEF", -1.1, 2.2, "Index of Bonds", "Fri 20 Nov 2026"]
    assert mb.market_row(rows["sa"])[17] == "Thu 19 Nov 2026"                        # a day or two old says nothing more
    oil = mb.market_row(rows["oil"])
    assert oil[13:16] == ["none (reading only)", "", ""] and oil[17] == "Tue 17 Nov 2026 - 3 days old"
    calm = mb.market_row(mrow("2026-11-17", "reits_sa", 1, "calm", 0.5, opened=1))
    assert calm[2:5] == ["OPEN", "calm", "nothing - calm"] and calm[13] == "the REITs on Tadawul"
    missing = mb.missing_markets(DAY20, RANK)
    assert missing == [["", "Saudi REITs", "NO READING", "",
                        "no close in the last 6 days (a holiday, or the series stopped): left out of the order"]
                       + [""] * (mb.WIDTH - 5)]
    assert mb.missing_markets(DAY19, RANK) == []


# ----------------------------------------------------------------------------- M3 the scorecard
def test_m3_the_scorecard_and_its_reading():
    table = mb.score_table(SUMMARY)
    assert table[0] == ["Measure", "Backtest, 5 closes", "Backtest, 10 closes", "Backtest, 20 closes",
                        "Forward, 5 closes", "Forward, 10 closes", "Forward, 20 closes"]
    rows = {r[0]: r[1:] for r in table[1:]}
    assert len(table) == 1 + len(mb.SCORE_ROWS) and all(len(r) == 7 for r in table)
    assert rows["US sessions read"] == [755, 755, 755, "", "", ""]
    assert rows["Open markets: mean change of the fund %"] == [0.11, 0.28, 0.11, "", "", ""]
    assert rows["Rank correlation, trend score against the outcome (mean of days)"] == [-0.004, 0.011, 0.041, "", "", ""]
    assert rows["Days with every market on hold (cash)"] == [26, 26, 24, "", "", ""]
    back = mb.reading("backtest", SUMMARY)
    assert back == ("BACKTEST (the same rules read back on the archive, before Mon 23 Nov 2026): over the next 5 closes "
                    "the open markets' funds averaged +0.11 % and the held ones +0.25 % - the hold has not earned its "
                    "place; the worst 5 % of the held ones was -3.67 % against -3.10 % for the open ones (the label does "
                    "flag the riskier markets); the trend order: rank correlation -0.004 (no edge); switching to the "
                    "first open market other than the US changed -0.37 pp against SPY; on the 26 day(s) with every "
                    "market on hold SPY rose 0.92 % over the next 5 closes and +3.73 % over 20 (cash missed the rise).")
    assert mb.reading("forward", SUMMARY) == (
        "FORWARD (from Mon 23 Nov 2026): nothing has matured yet - a reading is scored 5 closes after the first close "
        "that follows it; the owner reviews it after 10 sessions.")
    fwd = SUMMARY + [srow("forward", 5, days=6, open_n=0, open_mean_pct=None, hold_n=40, hold_mean_pct=-1.5,
                          hold_p5_pct=-6.2, open_p5_pct=None, mean_ic=0.2, top_vs_spy_pp=None, cash_days=6,
                          cash_spy_mean_pct=-2.1)]
    assert mb.reading("forward", fwd) == "FORWARD: not enough outcomes to read yet."
    fwd[-1]["open_mean_pct"], fwd[-1]["open_n"] = "0.4", "8"
    assert mb.reading("forward", fwd) == (
        "FORWARD (from Mon 23 Nov 2026, 6 session(s) read): over the next 5 closes the open markets' funds averaged "
        "+0.40 % and the held ones -1.50 % - the hold earned its place; the trend order: rank correlation +0.200 (it "
        "helps); on the 6 day(s) with every market on hold SPY fell 2.10 % over the next 5 closes (cash avoided the "
        "fall).")


# ----------------------------------------------------------------------------- M4 the SQL
def test_m4_sql_is_select_only_dated_and_reads_the_two_views():
    q = mb.sql_rank(DS, TODAY)
    assert q.startswith("SELECT ") and "FROM `proj-1.tfb_archive.v_market_rank`" in q
    assert "WHERE as_of > DATE_SUB(DATE '2026-11-21', INTERVAL 60 DAY) AND as_of <= DATE '2026-11-21'" in q
    assert q.endswith("ORDER BY as_of DESC, rank") and "CURRENT_DATE" not in q
    s = mb.sql_summary(DS)
    assert s == "SELECT * FROM `proj-1.tfb_archive.v_market_trial_summary` ORDER BY period, horizon"
    for sql in (q, s):
        assert not re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE)\b", sql, re.I)
    with pytest.raises(ValueError):
        mb.sql_rank(DS, NOW)                                                     # a date, never a time


# ----------------------------------------------------------------------------- M5 full runs
def test_m5_a_full_run_writes_one_tab(monkeypatch, capsys):
    _env(monkeypatch)
    book, sess = world(), Session()
    assert mb.run_once(False, book=book, session=sess, now=NOW, query=eb.bq_query) == 0
    assert list(book.tabs) == ["Final_Decisions", "Markets_Board"] and book.tabs["Markets_Board"].updates == 1
    assert book.tabs["Final_Decisions"].ranges == ["A1:C400"] and book.tabs["Final_Decisions"].updates == 0
    assert len(sess.sql) == 8 and ".v_market_rank`" in sess.sql[0] and ".v_market_trial_summary`" in sess.sql[1]
    assert [q.split("FROM `proj-1.tfb_archive.", 1)[1].split("`", 1)[0] for q in sess.sql[2:]] == [
        "v_ready_list", "v_hold_vs_switch", "v_ready_trial", "v_event_scenarios", "v_event_trial",
        "v_event_trial_summary"]
    page = book.tabs["Markets_Board"].values
    assert all(len(r) == mb.WIDTH for r in page) and len(page) >= 160
    cells = cells_of(page)
    assert not any("None" in c or c.lower() in ("nan", "inf") for c in cells)
    assert page[0][0].startswith("MARKETS BOARD - ") and "OBSERVE ONLY" in page[0][0]
    assert page[1][0] == ("Built 2026-11-21 11:40 Riyadh | markets_board 1.2.0 | as of Fri 20 Nov 2026 (each market's "
                          "latest close, 6 days at most) | BigQuery v_market_rank | only the Today Action Plan gives orders")
    assert page[2][:3] == ["VERDICT", "ALL ON HOLD - cash stays cash",
                           "none of the 10 markets with a fund is calm (oil is a reading only); every market on hold for "
                           "3 day(s) in a row, since Wed 18 Nov 2026"]
    assert page[3][:2] == ["US", "crash_day"]
    assert page[4][:3] == ["NEXT EVENT", "US core PCE prices (m/m) - Oct, Wed 25 Nov 16:30 Riyadh",     # 1.2.0
                           "on its 24 earlier event(s) the US moved a median +0.12 % (middle 80 %: -0.90 .. +1.05) and "
                           "oil moved a median +0.30 % (middle 80 %: -2.20 .. +2.40) - section 7 has the next 14 days"]
    head = page.index(mb.HEAD)
    order = [r[1] for r in page[head + 1:head + 12]]
    assert order == ["Bonds", "Saudi Arabia", "US REITs", "Oil", "Europe", "Sukuk", "China / Hong Kong", "Japan", "Gold",
                     "United States", "Saudi REITs"]
    assert page[head + 11][2] == "NO READING"
    hist = [r for r in page if r[0] == "Day"][0]
    first = page.index(hist)
    assert [r[0] for r in page[first + 1:first + 5]] == ["Fri 20 Nov 2026", "Thu 19 Nov 2026", "Wed 18 Nov 2026",
                                                        "Tue 17 Nov 2026"]
    assert page[first + 4][1:5] == ["2 of 10", "2 of 10 open", "Gold, Bonds", "Gold (open)"]
    assert any(r[0] == "Measure" for r in page)
    texts = [r[0] for r in page if isinstance(r[0], str) and r[0]]
    assert any(t.startswith("BACKTEST (the same rules") for t in texts)
    assert any(t.startswith("FORWARD (from Mon 23 Nov 2026): nothing has matured yet") for t in texts)
    assert texts[-1].startswith("The forward test starts on Mon 23 Nov 2026.") and texts[-1].endswith("Not investment advice.")
    assert page[-1] == [""] * mb.WIDTH
    # 1.1.0: the ready list, the switch check and the ready list's scorecard
    four = [r for r in page if str(r[0]).startswith("4. THE READY LIST")][0]
    assert four[0].startswith("4. THE READY LIST - the 3 best qualified names of Sat 21 Nov 2026 from the top-ranked markets")
    head = page.index(mb.READY_HEAD + [""] * (mb.WIDTH - len(mb.READY_HEAD)))
    rows = {r[1]: r for r in page[head + 1:head + 7]}
    assert [r[0] for r in page[head + 1:head + 7]] == ["READY 1", "READY 2", "READY 3", 4, 5, 6]
    assert rows["AAA.SR"][3:9] == ["Saudi Arabia", "OPEN", "does not hold it", "does not hold it",
                                   "nothing: the Today Action Plan says BUY - Buy 12 at 31.5 (limit)",
                                   "BUY - Buy 12 at 31.5 (limit)"]
    assert rows["BBB.US"][6:9] == ["does not hold it",
                                   "cash: Do not buy today: not funded (needs 9,000 SAR, 1,200 SAR left).",
                                   "WATCH - Do not buy today: not funded (needs 9,000 SAR, 1,200 SAR left)."]
    assert rows["CCC.HK"][7].startswith("a switch: it would replace JJJ.US over 1M (net +1.25 pp after both trades, "
                                        "chance 66 %)") and rows["CCC.HK"][8] == "not on Final_Decisions"
    assert rows["DDD.US"][4:8] == ["HOLD (drawdown)", "holds it", "holds it (US crash_day)",
                                   "a calmer market: US REITs is drawdown (x under its high)"]
    assert rows["EEE.L"][6] == "does not hold it"                                 # a pound name: the live rule lets it in
    assert rows["FFF.KS"][3:8] == ["not read (South Korea)", "NOT READ", "cannot read it", "does not hold it",
                                   "a reading of its market: South Korea is not read yet"]
    assert rows["AAA.SR"][9:] == [4.5, 71.25, "SAR", "Global_Markets"] + [""] * (mb.WIDTH - 13)
    sw_head = page.index(mb.SWITCH_HEAD + [""] * (mb.WIDTH - len(mb.SWITCH_HEAD)))
    assert [r[:3] for r in page[sw_head + 1:sw_head + 3]] == [["HHH.US", "Name HHH.US", "KEEP"],
                                                              ["JJJ.US", "Name JJJ.US", "SWITCH CANDIDATE"]]
    assert page[sw_head + 1][3] == "1M" and page[sw_head + 2][3:10] == ["1M", 1.2, "CCC.HK", 3.4, 1.25, 66.0,
                                                                       "proven signal"]
    tr_head = page.index(mb.TRIAL_HEAD + [""] * (mb.WIDTH - len(mb.TRIAL_HEAD)))
    assert page[tr_head + 1][:9] == ["backtest", "1W", "all qualified", 1148, 15, -0.44, -0.3, 50.0, -4.0]
    ready_text = [t for t in texts if t.startswith("READY LIST, BACKTEST")][0]
    assert ready_text == ("READY LIST, BACKTEST: the ready list's names moved -1.73 % in the next week against -0.39 % "
                          "for the other qualified names (43 and 1105 name-days) - it picked the weaker ones; the names "
                          "the market rule held moved -0.84 % in the next week against +0.38 % for the ones it let "
                          "through (768 and 380 name-days) - it held the weaker ones; the names the live US rule held "
                          "moved -1.11 % in the next week against +0.69 % for the ones it let through (720 and 428 "
                          "name-days) - it held the weaker ones.")
    assert "READY LIST, FORWARD: nothing has matured yet" in " ".join(texts)
    # 1.2.0: the events ahead, the last 7 days and the events scorecard
    ev_head = page.index(mb.EVENT_HEAD + [""] * (mb.WIDTH - len(mb.EVENT_HEAD)))
    assert str(page[ev_head - 1][0]).startswith("7. EVENTS AHEAD - the Fed, ECB, Bank of Japan and OPEC+ meetings")
    assert page[ev_head + 1][:14] == ["Wed 25 Nov 16:30", "US core PCE prices (m/m) - Oct", 0.2, 0.3,
                                      "37 / 33 / 30 (from 24)", 24, "+0.12 (-0.90 .. +1.05)", "-0.41 (9) / +0.38 (7)",
                                      "-0.05 (-0.70 .. +0.60)", "+0.30 (-2.20 .. +2.40)", "+0.25 (-1.00 .. +1.40)",
                                      "+0.08 (-0.55 .. +0.60)", 30, "Gold (1.1 normal days)"]
    assert page[ev_head + 2][:14] == ["Sun 29 Nov 15:00", "OPEC+ meeting", "", "", "no forecast to read", 27,
                                      "+0.05 (-1.20 .. +1.10)", "", "+0.10 (-0.90 .. +1.00)", "+0.40 (-3.10 .. +3.50)",
                                      "", "", 30, "Oil (1.4 normal days)"]
    la_head = page.index(mb.EVENT_LAST_HEAD + [""] * (mb.WIDTH - len(mb.EVENT_LAST_HEAD)))
    assert page[la_head + 1][:10] == ["Tue 17 Nov 16:30", "US retail sales (m/m)", "above", 0.4, 0.6, "",
                                      "-0.42 (inside the band)", "", "+0.10 (no band yet)", "-3.10 (OUTSIDE the band)"]
    et_head = page.index(mb.EVENT_TRIAL_HEAD + [""] * (mb.WIDTH - len(mb.EVENT_TRIAL_HEAD)))
    assert page[et_head + 1][:16] == ["backtest", "All families", 96, 71.2, 719, 25.7, 24.9, -0.08, 55.0, 6, 0.63,
                                      47.4, 5, 0.04, 60, "band calibrated, direction not proven"]   # events, not rows
    assert page[et_head + 3][:3] == ["forward", "All families", 2] and page[et_head + 3][15] == "collecting"
    ev_text = [x for x in texts if x.startswith("EVENTS, BACKTEST")][0]
    assert ev_text == ("EVENTS, BACKTEST (the same rules read back on the archive, 96 events): the band of the earlier "
                       "moves held 71 % of 719 event-market rows (a true band holds 80 %: close); a big move came on "
                       "26 % of the event days against 25 % of any days, and the family's chance of one scored -0.08 "
                       "against any day's (it told nothing more); the direction after a figure held 55 % of 40 rows "
                       "over 6 events (z +0.63, each event once); the figure chances scored +0.04 against equal thirds "
                       "over 60 figures.")
    assert ("EVENTS, FORWARD (from Mon 23 Nov 2026): 2 event(s) scored so far - collecting; the owner reviews them "
            "with the rest after 10 sessions.") in texts
    out = capsys.readouterr().out
    assert ("MARKETS as of 2026-11-20: 10 of 11 market(s) read, 0 of 10 open (ALL ON HOLD - cash stays cash); labels "
            "crash_day 3, drawdown 6, weak 1; scorecard rows: backtest 3, forward 0") in out
    assert ("READY 3 of 6 qualified (hold 2, not read 1, open 3) | switch check: 2 holding(s), 1 with a SWITCH CANDIDATE | "
            "ready scorecard rows: 7 | Final_Decisions: 3 stock(s) read") in out
    assert "wrote the Markets_Board tab (100 row(s))" in out                   # the rows written before the blank tail
    assert ("EVENTS ahead (14 days): 2 event(s) with a scenario | last 7 days: 1 | event scorecard lines: backtest 2, "
            "forward 1") in out
    for sym in ("AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "HHH", "JJJ", "ZZZ"):
        assert sym not in out, sym                                              # no symbol in the public run log


def test_m5_empty_views_still_make_an_honest_tab(monkeypatch, capsys):
    _env(monkeypatch)
    book, sess = Book(), Session(rank=[], summary=[], ready=[], switch=[], trial=[], events=[], last=[], ev_summary=[])
    assert mb.run_once(False, book=book, session=sess, now=NOW, query=eb.bq_query) == 0
    page = book.tabs["Markets_Board"].values
    assert "no market could be read from v_market_rank" in page[1][0]
    assert any(r[0] == "Measure" for r in page) and not any(r[0] == "VERDICT" for r in page)
    cells = cells_of(page)
    assert "no qualified name in the last 7 days (v_ready_list)" in cells and "no holding read (v_hold_vs_switch)" in cells
    assert "nothing has matured yet" in cells
    assert "no event with earlier ones of its kind in the next 14 days" in cells and "no event scored yet" in cells
    assert "no event in the last 7 days" in cells and "EVENTS, BACKTEST: no event scored yet." in cells
    out = capsys.readouterr().out
    assert "MARKETS no market read" in out and "Final_Decisions: 0 stock(s) read" in out    # no tab: nothing read


def test_m5_before_the_event_views_exist_the_page_is_built_all_the_same(monkeypatch, capsys):
    """1.2.0: while views 1.21.0 are not in BigQuery, sections 7-8 say so, nothing else changes, and only the first
    event view is asked for; any other refusal of an event view still fails the run (test M6)."""
    _env(monkeypatch)
    book, sess = world(), Session(no_event_views=True)
    assert mb.run_once(False, book=book, session=sess, now=NOW, query=eb.bq_query) == 0
    assert len(sess.sql) == 6 and ".v_event_scenarios`" in sess.sql[-1]
    page = book.tabs["Markets_Board"].values
    cells = cells_of(page)
    msg = ("the event views are not in BigQuery yet: v_event_scenarios, v_event_trial and v_event_trial_summary "
           "(apply_bq_views 1.21.0)")
    assert cells.count(msg) == 3 and not any(r[0] == "NEXT EVENT" for r in page)
    assert any(str(r[0]).startswith("7. EVENTS AHEAD") for r in page) and any(r[0] == "VERDICT" for r in page)
    out = capsys.readouterr().out
    assert "a view of apply_bq_views 1.21.0 is not in BigQuery yet: v_event_scenarios" in out
    assert "EVENTS: views 1.21.0 not in BigQuery yet" in out and "wrote the Markets_Board tab" in out


# ----------------------------------------------------------------------------- M6 off, dry run and failures
def test_m6_dry_run_off_switch_and_failures_write_nothing(monkeypatch, capsys):
    _env(monkeypatch)
    book, sess = world(), Session()
    assert mb.run_once(True, book=book, session=sess, now=NOW, query=eb.bq_query) == 0
    assert list(book.tabs) == ["Final_Decisions"] and len(sess.sql) == 8 and "dry run:" in capsys.readouterr().out
    assert mb.main(["--dry-run"], book=book, session=Session(), now=NOW, query=eb.bq_query) == 0
    assert list(book.tabs) == ["Final_Decisions"]
    for view in ("v_market_rank", "v_market_trial_summary", "v_ready_list", "v_hold_vs_switch", "v_ready_trial",
                 "v_event_scenarios`", "v_event_trial`", "v_event_trial_summary`"):          # 1.2.0: a refusal fails
        old = world(Markets_Board=Sheet([["old tab"]]))
        assert mb.run_once(False, book=old, session=Session(fail=view), now=NOW, query=eb.bq_query) == 2, view
        assert old.tabs["Markets_Board"].values == [["old tab"]] and old.tabs["Markets_Board"].updates == 0
        assert "nothing written" in capsys.readouterr().out

    class Broken(Sheet):
        def get(self, rng):
            raise RuntimeError("the sheet could not be read")

    old = Book({"Final_Decisions": Broken(), "Markets_Board": Sheet([["old tab"]])})
    sess = Session()
    assert mb.run_once(False, book=old, session=sess, now=NOW, query=eb.bq_query) == 2 and sess.sql == []
    assert old.tabs["Markets_Board"].values == [["old tab"]] and "nothing written" in capsys.readouterr().out
    monkeypatch.setenv("TFB_MARKETS_BOARD", "off")
    sess = Session()
    assert mb.run_once(False, book=book, session=sess, now=NOW, query=eb.bq_query) == 0 and sess.sql == []
    assert "is off" in capsys.readouterr().out
    monkeypatch.setenv("TFB_MARKETS_BOARD", "on")
    monkeypatch.setenv("TFB_BQ_PROJECT", "bad project;")
    assert mb.run_once(False, book=book, session=sess, now=NOW, query=eb.bq_query) == 2 and sess.sql == []


class Quota(Exception):
    def __str__(self):
        return "APIError: [429]: Quota exceeded for quota metric 'Write requests'"


def test_m6_a_quota_answer_is_waited_out(monkeypatch, capsys):
    _env(monkeypatch)
    waits = []
    monkeypatch.setattr(mb, "_SLEEP", waits.append)

    class Flaky(Sheet):
        def __init__(self, fails):
            super().__init__([["old tab"]])
            self.fails = fails

        def update(self, values=None, range_name=None, value_input_option=None):
            if self.fails > 0:
                self.fails -= 1
                raise Quota()
            super().update(values=values, range_name=range_name, value_input_option=value_input_option)

    book = world(Markets_Board=Flaky(1))
    assert mb.run_once(False, book=book, session=Session(), now=NOW, query=eb.bq_query) == 0
    assert waits == [65.0] and book.tabs["Markets_Board"].updates == 1
    book = world(Markets_Board=Flaky(9))
    waits.clear()
    assert mb.run_once(False, book=book, session=Session(), now=NOW, query=eb.bq_query) == 2
    assert book.tabs["Markets_Board"].values == [["old tab"]] and "the tab was not written" in capsys.readouterr().out


# ----------------------------------------------------------------------------- M7 wiring
WF = ROOT / ".github" / "workflows" / "markets_board.yml"
ARCHIVES = ["\U0001F30D BigQuery Market Series", "\U0001F52C BigQuery Symbol Deep Data",
            "\U0001F5D3\ufe0f BigQuery Economic Calendar"]                       # 1.2.0: the economic calendar
EXIT_PLAN = "\U0001F3AF Exit Plan"


def _wf():
    return yaml.safe_load(WF.read_text(encoding="utf-8"))


def test_m7_workflow_manifest_and_read_only_scope():
    raw = WF.read_text(encoding="utf-8")
    wf = _wf()
    on = wf[True] if True in wf else wf["on"]
    assert set(on) == {"schedule", "workflow_dispatch", "workflow_run"} and on["schedule"] == [{"cron": "45 5 * * *"}]
    assert on["workflow_run"] == {"workflows": ARCHIVES + [EXIT_PLAN], "types": ["completed"]}
    names = {p.name: yaml.safe_load(p.read_text(encoding="utf-8"))["name"]
             for p in (ROOT / ".github" / "workflows").glob("*.yml")
             if p.name in ("bq_markets.yml", "bq_symbol_deep.yml", "bq_econ_calendar.yml", "exit_plan.yml")}
    assert names == {"bq_markets.yml": ARCHIVES[0], "bq_symbol_deep.yml": ARCHIVES[1],
                     "bq_econ_calendar.yml": ARCHIVES[2], "exit_plan.yml": EXIT_PLAN}
    assert on["workflow_dispatch"]["inputs"]["dry_run"]["default"] is False
    assert wf["permissions"] == {"contents": "read"} and "concurrency" not in wf
    assert set(wf["jobs"]) == {"due", "board"} and "concurrency" not in wf["jobs"]["due"]
    job = wf["jobs"]["board"]
    assert job["needs"] == "due" and job["if"] == "needs.due.outputs.run == 'true'" and job["timeout-minutes"] == 15
    assert job["concurrency"] == {"group": "tfb-markets-board", "cancel-in-progress": False}
    gate = wf["jobs"]["due"]["steps"][0]
    assert gate["env"]["WR_NAME"] == "${{ github.event.workflow_run.name }}"           # never inlined in the script
    assert gate["env"]["CHAIN_HOURS"] == "${{ vars.TFB_MARKETS_BOARD_CHAIN_HOURS || '5 13' }}"
    assert "github.event" not in gate["run"]
    step = [s for s in job["steps"] if "markets_board.py" in str(s.get("run", ""))][0]
    assert step["env"]["TFB_MARKETS_BOARD"] == "${{ vars.TFB_MARKETS_BOARD || 'on' }}"
    assert step["env"]["TFB_BQ_PROJECT"] == "${{ vars.TFB_BQ_PROJECT || 'project-2e3530e6-2311-4aa3-b36' }}"
    assert 'if [[ "$EVENT_NAME" == "workflow_dispatch" && "$IN_DRY_RUN" == "true" ]]; then' in step["run"]
    assert 'python scripts/markets_board.py "${ARGS[@]}"' in step["run"]
    # 1.1.0: the live US rule's settings, the same Variables and defaults as the Daily Plan's own step
    plan = yaml.safe_load((ROOT / ".github/workflows/daily_plan.yml").read_text(encoding="utf-8"))
    plan_env = {k: v for j in plan["jobs"].values() for st in j.get("steps", []) for k, v in (st.get("env") or {}).items()}
    for name in ("TFB_PLAN_REGIME", "TFB_PLAN_REGIME_BLOCK", "TFB_PLAN_REGIME_CCY"):
        assert step["env"][name] == plan_env[name], name
    src = (ROOT / "scripts/markets_board.py").read_text(encoding="utf-8")
    for name in re.findall(r"(TFB_[A-Z_]+): \$\{\{", raw):
        assert name in src or name.startswith("TFB_BQ_"), name
    assert src.isascii() and mb.__version__ == "1.2.0" and "v1.2.0 (2026-11-21)" in src and "v1.1.0 (2026-11-21)" in src
    assert "v1.0.0 (2026-11-21)" in src
    assert ('("markets_board", "%s", "markets board")' % mb.__version__
            in (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8"))
    assert src.count("write_grid(") == 1 and "write_grid(book, TAB," in src and mb.TAB == "Markets_Board"
    assert src.count("_raw_read(") == 1 and '_raw_read(book, FINAL_TAB, "A1:C400")' in src    # one tab, three columns
    assert mb.FINAL_TAB == "Final_Decisions" and "_open_book()" in src
    assert "tests/test_markets_board.py" in (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    # the markets of the page are the markets of the view, in its order
    view = (ROOT / "sql/bigquery/99_v_market_condition.sql").read_text(encoding="utf-8")
    ids = [m[1] for m in re.findall(r"STRUCT\((\d+)(?: AS market_order)?, '([a-z_]+)'", view)]
    assert tuple(ids) == mb.MARKETS, ids
    assert mb.FORWARD_FROM.isoformat() in (ROOT / "sql/bigquery/101_v_market_trial.sql").read_text(encoding="utf-8")
    assert mb.FORWARD_FROM.isoformat() in (ROOT / "sql/bigquery/104_v_ready_trial.sql").read_text(encoding="utf-8")
    # 1.2.0: the events shown are markets of the page; the forward test starts at 00:00 Riyadh of the board's date
    assert set(mb.EVENT_MARKETS) <= set(mb.MARKETS) and mb.EVENT_FORWARD_FROM == mb.FORWARD_FROM
    start = dt.datetime.combine(mb.EVENT_FORWARD_FROM, dt.time(0, 0), tzinfo=mb.RIYADH).astimezone(UTC)
    trial_sql = (ROOT / "sql/bigquery/112_v_event_trial.sql").read_text(encoding="utf-8")
    assert "TIMESTAMP '%s+00', 'forward', 'backtest'" % start.strftime("%Y-%m-%d %H:%M:%S") in trial_sql
    summary_sql = (ROOT / "sql/bigquery/113_v_event_trial_summary.sql").read_text(encoding="utf-8")
    assert "WHEN r.period = 'forward' AND r.events < %d THEN 'collecting'" % mb.REVIEW_SESSIONS in summary_sql
    # the Final_Decisions header the board needs is the Today Action Plan's own
    from scripts import today_action_plan as tap
    assert tap.FINAL_TAB == mb.FINAL_TAB and tap.FINAL_HEAD[:3] == ["Symbol", "Decision", "Do today"]
    assert "Do not buy today: not funded" in (ROOT / "scripts/today_action_plan.py").read_text(encoding="utf-8")


def _gate(event, utc, name="", conclusion="", hours="5 13"):
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash is not installed")
    step = _wf()["jobs"]["due"]["steps"][0]
    with tempfile.TemporaryDirectory() as d:
        out = Path(d) / "out"
        stub = 'date() { case "$2" in "+%%H") echo %s ;; *) command date "$@" ;; esac; }\n' % utc.strftime("%H")
        env = {"PATH": os.environ.get("PATH", ""), "GITHUB_OUTPUT": str(out), "EVENT_NAME": event, "CHAIN_HOURS": hours,
               "WR_NAME": name, "WR_CONCLUSION": conclusion, "EXIT_PLAN": step["env"]["EXIT_PLAN"],
               "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}
        r = subprocess.run([bash, "-e", "-c", stub + step["run"]], env=env, capture_output=True, text=True, timeout=30)
        assert r.returncode == 0, r.stderr
        return out.read_text(encoding="utf-8").strip(), r.stdout


def _at(hour):
    return dt.datetime(2026, 11, 23, hour, 40, tzinfo=UTC)


def test_m7_the_gate_runs_after_each_archive_and_in_the_chain_hours():
    assert _gate("schedule", _at(2))[0] == "run=true" and _gate("workflow_dispatch", _at(2), hours="off")[0] == "run=true"
    for name in ARCHIVES:
        run, log = _gate("workflow_run", _at(3), name, "success")
        assert run == "run=true" and "After an archive run (success): run=true" in log
        assert _gate("workflow_run", _at(5), name, "failure")[0] == "run=false"       # nothing new to read
        assert _gate("workflow_run", _at(5), name, "cancelled")[0] == "run=false"
    for hour, want in ((5, True), (13, True), (4, False), (14, False), (0, False)):
        run, log = _gate("workflow_run", _at(hour), EXIT_PLAN, "success")
        assert run == "run=%s" % str(want).lower(), hour
        assert "Chained run at UTC hour %d (hours: 5 13): run=%s" % (hour, str(want).lower()) in log
    assert _gate("workflow_run", _at(5), EXIT_PLAN, "failure")[0] == "run=true"         # the Exit Plan is the clock
    assert _gate("workflow_run", _at(5), EXIT_PLAN, hours="off")[0] == "run=false"
    assert _gate("workflow_run", _at(0), EXIT_PLAN, hours="off")[0] == "run=false"       # "off" is not hour 0
    assert _gate("workflow_run", _at(8), EXIT_PLAN, hours="08 09")[0] == "run=true"      # base 10, not octal
    assert _gate("workflow_run", _at(5), EXIT_PLAN, hours="5,13")[0] == "run=false"      # spaces only


# ----------------------------------------------------------------------------- M8 the ready list's pieces (1.1.0)
def test_m8_final_decisions_the_live_rule_and_the_waiting_words(monkeypatch):
    assert mb.read_final(FINALS) == {"AAA.SR": ("BUY", "Buy 12 at 31.5 (limit)"),
                                     "BBB.US": ("WATCH", "Do not buy today: not funded (needs 9,000 SAR, 1,200 SAR left)."),
                                     "HHH.US": ("HOLD", "Keep the stop at 40")}           # the first row of a symbol wins
    assert mb.read_final([["Name", "Decision", "Do today"], ["AAA.SR", "BUY", "x"]]) == {} and mb.read_final([]) == {}
    for name in ("TFB_PLAN_REGIME", "TFB_PLAN_REGIME_BLOCK", "TFB_PLAN_REGIME_CCY"):
        monkeypatch.delenv(name, raising=False)
    rule = mb.plan_rule()
    assert rule == {"mode": "enforce", "block": ["weak", "stress", "drawdown", "crash_day"], "ccy": ["USD"]}
    usd, sar = rdy(1, "X.US", "us", "hold", "weak", us="weak"), rdy(1, "Y.SR", "sa", "hold", "weak", ccy="SAR", us="weak")
    assert mb.live_rule_word(usd, rule) == "holds it (US weak)" and mb.live_rule_word(sar, rule) == "does not hold it"
    assert mb.live_rule_word(dict(usd, us_label="calm"), rule) == "does not hold it"
    assert mb.live_rule_word(dict(usd, us_label=None), rule) == "US not read: does not hold it"
    monkeypatch.setenv("TFB_PLAN_REGIME", "observe")
    monkeypatch.setenv("TFB_PLAN_REGIME_BLOCK", "drawdown, crash_day, calm, nonsense")
    monkeypatch.setenv("TFB_PLAN_REGIME_CCY", "usd,sar")
    rule = mb.plan_rule()
    assert rule == {"mode": "observe", "block": ["drawdown", "crash_day"], "ccy": ["USD", "SAR"]}
    assert mb.live_rule_word(usd, rule) == "does not hold it"                         # weak no longer holds a buy
    assert mb.live_rule_word(dict(sar, us_label="drawdown"), rule) == "observe: would hold it (US drawdown)"
    monkeypatch.setenv("TFB_PLAN_REGIME", "off")
    assert mb.live_rule_word(usd, mb.plan_rule()) == "off"
    monkeypatch.setenv("TFB_PLAN_REGIME", "bogus")
    assert mb.plan_rule()["mode"] == "off"
    final = mb.read_final(FINALS)
    opened = rdy(1, "AAA.SR", "sa", "open", "calm", ccy="SAR")
    assert mb.waiting_text(opened, final, []) == "nothing: the Today Action Plan says BUY - Buy 12 at 31.5 (limit)"
    assert mb.waiting_text(dict(opened, symbol="QQQ.SR"), final, []) == (                 # not picked: no cash claim
        "a pick of today's plan: it is not on Final_Decisions (the plan has not picked it, so no cash is set for it)")
    assert mb.waiting_text(dict(opened, symbol="CCC.HK"), final, SWITCH).startswith("a switch: it would replace JJJ.US")
    keep_only = [r for r in SWITCH if r["verdict"] == "KEEP"]
    assert mb.waiting_text(dict(opened, symbol="CCC.HK"), final, keep_only).startswith("a pick of today's plan")  # no switch
    assert mb.waiting_text(dict(opened, symbol="BBB.US"), final, []) == (
        "cash: Do not buy today: not funded (needs 9,000 SAR, 1,200 SAR left).")     # the plan's own shortfall: cash
    # Codex P2 (#1046): every other WATCH / DO NOT INVEST keeps the plan's own reason - never 'cash'
    other = mb.read_final(FINALS + [["VVV.US", "WATCH", "Do not buy yet: verify first - the quote is 3 days old."],
                                    ["PPP.US", "WATCH", "Not today's pick: waiting for a seat."],
                                    ["III.HK", "DO NOT INVEST", "Do not buy: the plan invalidated it today (stop)."],
                                    ["NNN.US", "WATCH", ""],
                                    ["FUN.US", "BUY", "Buy 5 shares; it is funded from the cash left."]])
    assert mb.waiting_text(dict(opened, symbol="FUN.US"), other, []) == (             # only 'not funded' is a shortfall
        "nothing: the Today Action Plan says BUY - Buy 5 shares; it is funded from the cash left.")
    for sym, want in (("VVV.US", "the Today Action Plan's own reason: WATCH - Do not buy yet: verify first - the quote "
                                 "is 3 days old."),
                      ("PPP.US", "the Today Action Plan's own reason: WATCH - Not today's pick: waiting for a seat."),
                      ("III.HK", "the Today Action Plan's own reason: DO NOT INVEST - Do not buy: the plan invalidated it "
                                 "today (stop)."),
                      ("NNN.US", "the Today Action Plan's own reason: WATCH - see Final_Decisions"),
                      ("HHH.US", "nothing new: it is held - the Today Action Plan says HOLD - Keep the stop at 40")):
        got = mb.waiting_text(dict(opened, symbol=sym), other, [])
        assert got == want and "cash" not in got, (sym, got)
    held = rdy(4, "DDD.US", "reits_us", "hold", "drawdown")
    assert mb.waiting_text(dict(held, market_waiting_for=""), final, []) == (
        "a calmer market: US REITs is drawdown (see section 1)")
    # a BUY in a held market waits for nothing: the decision stands, the observe-only market rule is named beside it
    assert mb.waiting_text(dict(held, symbol="AAA.SR"), final, []) == (
        "nothing today: the Today Action Plan says BUY - Buy 12 at 31.5 (limit) (the market rule, observe only, would "
        "wait for a calmer market: US REITs is drawdown (x under its high))")
    # Codex P2 (#1048): the decision first, the observe-only rules after it - an unfunded name in a held market
    # waits for cash, as the plan says, and the market rule is named beside it
    assert mb.waiting_text(dict(held, symbol="BBB.US"), final, []) == (
        "cash: Do not buy today: not funded (needs 9,000 SAR, 1,200 SAR left). (the market rule, observe only, would "
        "wait for a calmer market: US REITs is drawdown (x under its high))")
    # a funded BUY that the switch check also names is still a BUY, and so is one in a market not read yet
    buy_sw = mb.read_final(FINALS + [["CCC.HK", "BUY", "Buy 100 at 41.2 (limit)"]])
    assert mb.waiting_text(dict(opened, symbol="CCC.HK"), buy_sw, SWITCH) == (
        "nothing: the Today Action Plan says BUY - Buy 100 at 41.2 (limit) (the switch check, observe only: it would "
        "replace JJJ.US over 1M, net +1.25 pp after both trades, chance 66 %, the owner's review)")
    unread = rdy(6, "AAA.SR", None, "not read", ccy="SAR")
    assert mb.waiting_text(unread, final, []) == (
        "nothing: the Today Action Plan says BUY - Buy 12 at 31.5 (limit) (the market rule, observe only, waits for a "
        "reading of its market: South Korea is not read yet)")                 # the fixture's unread market
    # a DO NOT INVEST keeps its own reason even when the switch check names the name
    dni_sw = mb.read_final(FINALS + [["CCC.HK", "DO NOT INVEST", "Do not buy: the plan invalidated it today (stop)."]])
    assert mb.waiting_text(dict(opened, symbol="CCC.HK"), dni_sw, SWITCH).startswith(
        "the Today Action Plan's own reason: DO NOT INVEST - Do not buy: the plan invalidated it today (stop). "
        "(the switch check, observe only: it would replace JJJ.US")
    # not picked, held market, a switch named: the market's wait first, the switch beside it
    assert mb.waiting_text(dict(held, symbol="CCC.HK"), final, SWITCH) == (
        "a calmer market: US REITs is drawdown (x under its high) (the switch check, observe only: it would replace "
        "JJJ.US over 1M, net +1.25 pp after both trades, chance 66 %, the owner's review)")
    assert mb.switch_rows(SWITCH)[0][:4] == ["HHH.US", "Name HHH.US", "KEEP", "1M"]       # no candidate: the 1-month row
    assert mb.switch_rows([sw("KKK.US", "3M", 4, "NO DATA", best=None)])[0][2:6] == ["NO DATA", "3M", 1.2, "none"]
    assert mb.trial_reading([], "backtest") == ("READY LIST, BACKTEST: nothing has matured yet - a name is scored a week "
                                                "after the day it was listed.")
    flip = [trow("forward", "1W", "market rule holds it", 10, 1.5, 1.0), trow("forward", "1W", "market rule does not", 12, 0.5, 0.2)]
    assert mb.trial_reading(flip, "forward") == ("READY LIST, FORWARD: the names the market rule held moved +1.50 % in the "
                                                 "next week against +0.50 % for the ones it let through (10 and 12 "
                                                 "name-days) - it held the stronger ones.")
    same = [trow("forward", "1W", "ready list (top 3)", 3, 0.25, 0.1), trow("forward", "1W", "other qualified", 9, 0.25, 0.1),
            trow("forward", "1W", "market rule holds it", 4, None, None)]                # a missing mean: no claim
    assert mb.trial_reading(same, "forward") == ("READY LIST, FORWARD: the ready list's names moved +0.25 % in the next "
                                                 "week against +0.25 % for the other qualified names (3 and 9 name-days) "
                                                 "- no difference.")
    assert mb.trial_reading(TRIAL, "forward").startswith("READY LIST, FORWARD: nothing has matured yet")  # by period


# ----------------------------------------------------------------------------- M9 the events (1.2.0)
def test_m9_event_sql_is_select_only_timed_and_reads_the_three_views():
    a, b, c = mb.sql_events_ahead(DS, NOW), mb.sql_events_last(DS, NOW), mb.sql_events_summary(DS)
    assert "FROM `proj-1.tfb_archive.v_event_scenarios` WHERE NOT is_out AND event_at >= TIMESTAMP '2026-11-21 08:40:00+00'" in a
    assert "AND event_at < TIMESTAMP_ADD(TIMESTAMP '2026-11-21 08:40:00+00', INTERVAL 14 DAY)" in a
    assert a.endswith("ORDER BY event_at, event_id, market_order")
    assert "FROM `proj-1.tfb_archive.v_event_trial` WHERE event_at >= TIMESTAMP_SUB(TIMESTAMP '2026-11-21 08:40:00+00', " \
           "INTERVAL 7 DAY) AND event_at < TIMESTAMP '2026-11-21 08:40:00+00'" in b
    assert "AND market_id IN ('us', 'sa', 'oil', 'gold', 'bonds')" in b
    assert c.endswith("FROM `proj-1.tfb_archive.v_event_trial_summary` WHERE market_id = 'all' ORDER BY period, family")
    assert ", dir_n, dir_events, dir_pct, dir_z, drift_n, drift_events, drift_pct, drift_z, " in c   # events, each once
    for sql in (a, b, c):
        assert sql.startswith("SELECT ") and "CURRENT_" not in sql
        assert not re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE)\b", sql, re.I)
    with pytest.raises(ValueError):
        mb.sql_events_ahead(DS, dt.datetime(2026, 11, 21, 8, 40))                   # a time with its zone, always
    with pytest.raises(ValueError):
        mb.sql_events_last(DS, TODAY)


def test_m9_event_cells_and_readings():
    assert mb._when("2026-11-29 12:00:00+00") == "Sun 29 Nov 15:00" and mb._when("") == "" and mb._when("x" * 16) == ""
    assert mb._when("2026-11-20 13:30:00.123+00") == "Fri 20 Nov 16:30"
    assert mb.band_cell(None) == "" and mb.band_cell({"r1_p50": None}) == ""
    assert mb.band_cell({"r1_p10": "-1.234", "r1_p50": "0", "r1_p90": "2.5"}) == "+0.00 (-1.23 .. +2.50)"
    assert mb.figure_chances({"n_fig": "0", "kind": "meeting", "estimate": None}) == "no forecast to read"
    assert mb.figure_chances({"n_fig": "0", "kind": "release", "estimate": "0.2"}) == ""
    assert mb.figure_chances({"n_fig": "3", "p_above": "0.5", "p_in_line": "0.3333", "p_below": "0.1667"}) == \
        "50 / 33 / 17 (from 3)"
    assert mb.after_figure({"n_above": "0", "n_below": "0"}) == "" and mb.after_figure(None) == ""
    assert mb.after_figure({"n_above": "4", "r1_p50_above": "-0.5", "n_below": "0"}) == "-0.50 (4) / none"
    assert mb.moved_most({}) == "" and mb.moved_most({"us": {"market": "United States", "mean_abs_z": None}}) == ""
    tie = {"us": {"market": "United States", "mean_abs_z": "1.0", "market_order": "1"},
           "oil": {"market": "Oil", "mean_abs_z": "1.0", "market_order": "7"}}
    assert mb.moved_most(tie) == "United States (1.0 normal days)"                   # a tie goes to the first market
    assert mb.next_event_line([]) is None
    assert mb.next_event_line([dict(EVENTS[0], r1_p50=None)])[1] == "no earlier event of its kind to read yet"
    assert [r[:2] for r in mb.event_rows(list(reversed(EVENTS)))] == [["Sun 29 Nov 15:00", "OPEC+ meeting"],
                                                                     ["Wed 25 Nov 16:30", "US core PCE prices (m/m) - Oct"]]
    assert mb.last_cell({"r1_pct": None}) == "" and mb.last_cell(None) == ""
    assert mb.event_reading([], "backtest") == "EVENTS, BACKTEST: no event scored yet."
    assert mb.event_reading([], "forward").startswith("EVENTS, FORWARD (from Mon 23 Nov 2026): 0 event(s) scored so far")
    ten = [esum("forward", "all", "All families", 10, band_n=40, band_pct=95.0, big_n=0, dir_n=0)]
    got = mb.event_reading(ten, "forward")
    assert got.startswith("EVENTS, FORWARD (from Mon 23 Nov 2026, 10 events): the band of the earlier moves held 95 % "
                          "of 40 event-market rows (a true band holds 80 %: too wide)")
    assert got.endswith("; no direction after a figure is scored yet.")
    narrow = [esum("backtest", "all", "All families", 30, band_n=200, band_pct=61.0, big_n=150, big_pct=40.0,
                   any_pct=25.0, big_skill=0.12)]
    assert "(a true band holds 80 %: too narrow)" in mb.event_reading(narrow, "backtest")
    assert "and the family's chance of one scored +0.12 against any day's (it told more)" in \
        mb.event_reading(narrow, "backtest")
    assert mb.event_counts(None, [], []) == "EVENTS: views 1.21.0 not in BigQuery yet"
