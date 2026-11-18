#!/usr/bin/env python3
"""tests/test_holding_read.py - scripts/holding_read.py 1.0.0: what moved each holding, and the next-session read.

  H1  returns, volatility, beta
  H2  the driver: company news, sector, market, normal range, no news found
  H3  the main-news text per driver; macro themes from the gate's drivers; law-firm noise is never the item
  H4  the next-session read: a real edge is named by the direction of the last move; a share near any day's, or a
      small sample, reads NO EDGE / NOT ENOUGH HISTORY; the sample falls back from stock-and-market to stock only
  H5  pinned by an offer; earnings due
  H6  the workbook parsers: items, gate, info, session windows, the item picked
  H7  the ledger: a read is marked HIT / MISS / - when the next session closes; the score line
  H8  run(): the gate, the two tabs, a re-run replaces the session's rows, fixed income is not read, no history
  H9  the public log carries no quantity or value
  H10 the workflow, the manifest pin and the CI lane
"""
from __future__ import annotations

import datetime as dt
import json
import random
import re
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import holding_read as hr  # noqa: E402
from tests.test_exit_plan import Book, Resp, Sheet, chart  # noqa: E402

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 18, 6, 0, tzinfo=UTC)          # 09:00 Riyadh, after the session of 17 Nov


def weekdays(n, end=dt.date(2026, 11, 17)):
    out, d = [], end
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= dt.timedelta(days=1)
    return out[::-1]


def bars_from(rets, end=dt.date(2026, 11, 17), start=100.0):
    """Bars whose close-to-close returns are `rets` (percent); one more bar than returns."""
    days = weekdays(len(rets) + 1, end)
    px, out = start, [(days[0], start, start, start, start, 1.0)]
    for d, r in zip(days[1:], rets):
        px *= 1.0 + r / 100.0
        out.append((d, px, px, px, px, 1.0))
    return out


def K(**over):
    k = {"range": "5y", "vol_win": 60, "min_n": 30, "edge_pts": 5.0, "z": 1.64, "pin_pct": 0.6, "sector_map": {}}
    k.update(over)
    return k


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for name in list(__import__("os").environ):
        if name.startswith("TFB_HREAD") or name == "TFB_HOLDING_READ":
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(hr, "_SLEEP", lambda s: None)


def item(pub, direction=0, sev="M", head="Company reports results", event="earnings", domain="example.com",
         noise=False):
    return {"pub": pub, "dir": direction, "sev": sev, "event": event, "headline": head, "domain": domain,
            "noise": noise}


# ----------------------------------------------------------------------------- H1
def test_h1_returns_volatility_beta():
    b = bars_from([1.0, -2.0, 0.5])
    r = hr.returns(b)
    assert [d for d, _ in r] == weekdays(4)[1:] and [round(x, 6) for _, x in r] == [1.0, -2.0, 0.5]
    assert hr.pstdev([1.0]) is None and round(hr.pstdev([1.0, -1.0]), 6) == 1.0
    rnd = random.Random(7)
    m = [rnd.gauss(0, 1) for _ in range(200)]
    days = weekdays(200)
    mkt = dict(zip(days, m))
    assert round(hr.beta_of(list(zip(days, [1.5 * x for x in m])), mkt), 6) == 1.5
    assert hr.beta_of(list(zip(days, [-2 * x for x in m])), mkt) == 0.0            # kept in 0..3
    assert hr.beta_of(list(zip(days, [9 * x for x in m])), mkt) == 3.0
    assert hr.beta_of(list(zip(days[:40], m[:40])), mkt) is None                    # under 60 common sessions
    assert hr.beta_of(list(zip(days, m)), {d: 0.0 for d in days}) is None           # a market that never moved


# ----------------------------------------------------------------------------- H2
def test_h2_the_driver():
    neg = item(NOW, -1)
    assert hr.attribute(-4.0, -1.0, -1.2, 1.0, 1.5, neg) == "COMPANY NEWS"          # its own news, beyond the market
    assert hr.attribute(-4.0, -1.0, -1.2, 1.0, 1.5, item(NOW, 0)) == "COMPANY NEWS"  # a neutral results item counts
    # the item points the other way: it does not explain a fall the group explains
    assert hr.attribute(-2.2, -1.0, -1.9, 1.0, 1.5, item(NOW, 1)) == "SECTOR"
    assert hr.attribute(-2.2, -1.0, -1.9, 1.0, 1.5, None) == "SECTOR"               # the group explains it better
    assert hr.attribute(-1.1, -1.9, None, 1.0, 1.5, None) == "MARKET"               # no group fund read
    assert hr.attribute(-1.1, -1.0, 0.4, 1.0, 1.5, None) == "MARKET"                # the group went the other way
    assert hr.attribute(-0.4, -1.9, -2.0, 1.0, 1.5, None) == "NORMAL RANGE"
    assert hr.attribute(-0.9, -1.9, -2.0, 1.0, 1.5, None) == "MARKET"               # over half its usual move; the
    assert hr.attribute(-1.9, -1.0, -2.0, 1.0, 1.5, None) == "SECTOR"               # market explains it as well
    assert hr.attribute(-3.0, 0.8, 0.5, 1.0, 1.5, None) == "NO NEWS FOUND"          # alone, and nothing read
    assert hr.attribute(-3.0, None, None, None, None, None) == "NO NEWS FOUND"
    # held up by its news on a falling day: a flat close the market does not explain
    assert hr.attribute(0.2, -2.5, None, 1.0, 1.5, item(NOW, 1)) == "COMPANY NEWS"
    # a small item-day inside the normal range is not called company news
    assert hr.attribute(-0.3, -0.2, None, 1.0, 1.5, neg) == "NORMAL RANGE"


# ----------------------------------------------------------------------------- H3
def test_h3_main_news_text_and_macro_themes():
    g = ("market: VIX 25.5 (+11%) | macro: war_escalation/hormuz grade 1, 38 sources: x | "
         "macro: war_escalation/iran grade 1, 33 sources: y | macro: war_escalation/hormuz grade 2, 4 sources: z")
    assert hr.macro_themes(g) == "war escalation/hormuz; war escalation/iran"
    assert hr.macro_themes("") == "" and hr.macro_themes("market: VIX 20") == ""
    it = item(dt.datetime(2026, 11, 17, 14, 5, tzinfo=UTC), -1, head="Profit falls")
    assert hr.main_news("COMPANY NEWS", -4, -1, None, 1.5, it, "S&P 500", "", "") == \
        "Profit falls (example.com, 11-17 17:05 Riyadh)"
    t = hr.main_news("SECTOR", -2.2, -1.0, -1.9, 1.5, None, "S&P 500", "KRE", "war escalation/iran")
    assert t == ("No company item. Its group moved: KRE -1.9%, S&P 500 -1.0%. Macro themes in the news gate: "
                 "war escalation/iran.")
    assert hr.main_news("MARKET", -1.1, -1.9, None, 1.5, None, "S&P 500", "", "") == \
        "No company item. The market moved: S&P 500 -1.9%."
    assert "about 1.5%" in hr.main_news("NORMAL RANGE", -0.3, -0.2, None, 1.5, None, "S&P 500", "", "")
    t = hr.main_news("NO NEWS FOUND", -3.0, 0.8, None, 1.5, item(NOW, 1, head="Target raised"), "S&P 500", "", "")
    assert "it moved -3.0% against S&P 500 +0.8%" in t and "pointed the other way: Target raised" in t


# ----------------------------------------------------------------------------- H4
def _states(pattern, tail):
    """History states from (zbucket, mbucket, next) triples repeated, then today's state with no next."""
    return list(pattern) + [tail + (None,)]


def test_h4_the_next_session_read():
    k = K()
    # after a hard fall on a down market the next day was up 70% of 50 times; on other days 50%
    hist = ([("BIG DOWN", "DOWN", 1.0)] * 35 + [("BIG DOWN", "DOWN", -1.0)] * 15
            + [("FLAT", "FLAT", 0.5)] * 200 + [("FLAT", "FLAT", -0.5)] * 250)
    r = hr.next_read(_states(hist, ("BIG DOWN", "DOWN")), k)
    assert r["label"] == "REBOUND MORE LIKELY" and r["n"] == 50 and round(r["p"]) == 70 and round(r["base"]) == 47
    assert "closed up 70% of 50 times (average +0.40%); on any day it is 47%" in r["why"]
    assert "a hard fall with the market down" in r["why"]
    # the mirror: after a rise that usually fades
    hist = ([("UP", "UP", -1.0)] * 36 + [("UP", "UP", 1.0)] * 14 + [("FLAT", "FLAT", 0.5)] * 260
            + [("FLAT", "FLAT", -0.5)] * 240)
    assert hr.next_read(_states(hist, ("UP", "UP")), k)["label"] == "PULLBACK MORE LIKELY"
    hist2 = [(a, b, -c) for a, b, c in hist if a == "UP"] + [x for x in hist if x[0] != "UP"]
    assert hr.next_read(_states(hist2, ("UP", "UP")), k)["label"] == "RISE MORE LIKELY TO CONTINUE"
    # after a fall that usually goes on
    hist = ([("DOWN", "DOWN", -1.0)] * 36 + [("DOWN", "DOWN", 1.0)] * 14 + [("FLAT", "FLAT", 0.5)] * 260
            + [("FLAT", "FLAT", -0.5)] * 240)
    assert hr.next_read(_states(hist, ("DOWN", "DOWN")), k)["label"] == "FURTHER FALL MORE LIKELY"
    # a share near any day's is no edge, whatever the sample
    hist = ([("DOWN", "DOWN", 1.0)] * 52 + [("DOWN", "DOWN", -1.0)] * 48 + [("FLAT", "FLAT", 0.5)] * 250
            + [("FLAT", "FLAT", -0.5)] * 250)
    r = hr.next_read(_states(hist, ("DOWN", "DOWN")), k)
    assert r["label"] == "NO EDGE" and r["n"] == 100
    # a wide share on a sample too small to be clear is no edge: 19 of 30 (63%) against 50% is z = 1.46
    hist = ([("DOWN", "DOWN", 1.0)] * 19 + [("DOWN", "DOWN", -1.0)] * 11 + [("FLAT", "FLAT", 0.5)] * 235
            + [("FLAT", "FLAT", -0.5)] * 243)
    assert hr.next_read(_states(hist, ("DOWN", "DOWN")), k)["label"] == "NO EDGE"
    assert hr.next_read(_states(hist, ("DOWN", "DOWN")), K(z=1.2))["label"] == "REBOUND MORE LIKELY"
    # too few days like it with this market: the sample is the stock's bucket on any market
    hist = ([("DOWN", "DOWN", 1.0)] * 5 + [("DOWN", "UP", 1.0)] * 30 + [("DOWN", "UP", -1.0)] * 10
            + [("FLAT", "FLAT", 0.5)] * 200 + [("FLAT", "FLAT", -0.5)] * 250)
    r = hr.next_read(_states(hist, ("DOWN", "DOWN")), k)
    assert r["n"] == 45 and "with any market" in r["why"] and r["label"] == "REBOUND MORE LIKELY"
    # still too few: said as such, with the count
    hist = [("BIG UP", "UP", 1.0)] * 8 + [("FLAT", "FLAT", 0.5)] * 100
    r = hr.next_read(_states(hist, ("BIG UP", "UP")), k)
    assert r["label"] == "NOT ENOUGH HISTORY" and "only 8 past days like it (a strong rise); 30 are needed" in r["why"]
    assert hr.next_read(_states([("FLAT", "FLAT", 0.1)] * 10, ("FLAT", "FLAT")), k)["label"] == "NOT ENOUGH HISTORY"
    assert hr.next_read([], k)["label"] == "NOT ENOUGH HISTORY"
    assert hr.next_read([("FLAT", "FLAT", 0.1)], k)["why"] == "no completed session to read from"
    # a market with no bar on the day (NA) uses the stock's own bucket
    hist = [("FLAT", "UP", 1.0)] * 40 + [("FLAT", "DOWN", -1.0)] * 40 + [("UP", "UP", 1.0)] * 30 + [("UP", "UP", -1.0)] * 30
    assert "with any market" in hr.next_read(_states(hist, ("FLAT", "NA")), k)["why"]
    # buckets
    assert [hr.z_bucket(z) for z in (-1.0, -0.99, -0.35, -0.34, 0.34, 0.35, 0.99, 1.0)] == \
        ["BIG DOWN", "DOWN", "DOWN", "FLAT", "FLAT", "UP", "UP", "BIG UP"]
    assert [hr.m_bucket(m) for m in (None, -0.5, -0.49, 0.49, 0.5)] == ["NA", "DOWN", "FLAT", "FLAT", "UP"]


def test_h4b_states_use_the_volatility_before_the_day():
    rnd = random.Random(3)
    rets = [rnd.gauss(0, 1.0) for _ in range(80)] + [-3.0, 0.2]
    days = weekdays(len(rets))
    series = list(zip(days, rets))
    st = hr.states(series, {days[-2]: -1.0, days[-1]: 0.0}, 60)
    assert len(st) == len(rets) - 60 and st[-1][2] is None                       # the last day has no next
    assert st[-2] == ("BIG DOWN", "DOWN", 0.2)                                    # -3% over about 1%: a hard fall
    assert st[-1][:2] == ("FLAT", "FLAT")
    # the day's own move is not in its volatility: a 30% day still reads against the calm before it
    calm = [0.1, -0.1] * 40 + [30.0, 0.0]
    st = hr.states(list(zip(weekdays(len(calm)), calm)), {}, 60)
    assert st[-2][0] == "BIG UP" and st[-2][1] == "NA"


# ----------------------------------------------------------------------------- H5
def test_h5_pinned_and_earnings():
    rnd = random.Random(5)
    base = [rnd.gauss(0, 1.2) for _ in range(120)]
    quiet = base + [0.1, -0.1, 0.4, -0.5, -0.06]
    days = weekdays(len(quiet))
    series = list(zip(days, quiet))
    offer = [item(dt.datetime(2026, 11, 14, 12, 0, tzinfo=UTC), 1, event="mna", head="Analysts align with the offer")]
    assert hr.pinned(series, offer, 0.6, days[-1])
    assert not hr.pinned(series, [], 0.6, days[-1])                               # no offer item
    assert not hr.pinned(list(zip(days, base + [1.5, -2.0, 0.4, -0.5, 1.0])), offer, 0.6, days[-1])   # still moving
    old = [item(dt.datetime(2026, 9, 1, tzinfo=UTC), 1, event="mna")]
    assert not hr.pinned(series, old, 0.6, days[-1])                              # the item is older than 30 days
    worded = [item(dt.datetime(2026, 11, 10, tzinfo=UTC), 0, event="other", head="Board backs the tender offer")]
    assert hr.pinned(series, worded, 0.6, days[-1])
    assert not hr.pinned(series, [dict(offer[0], noise=True)], 0.6, days[-1])
    mk = bars_from([0.0] * len(quiet))
    r = hr.read_holding("DDI.US", "DoubleDown", bars_from(quiet), mk, [], offer, {}, {}, "", "", "S&P 500", K())
    assert r["label"] == hr.PINNED and "while the offer stands" in r["why"]
    r = hr.read_holding("DDI.US", "DoubleDown", bars_from(quiet), mk, [], offer, {"earn_days": 1}, {}, "", "",
                        "S&P 500", K())
    assert r["label"] == hr.EARNINGS and "does not cover a results reaction" in r["why"]
    r = hr.read_holding("AAA.US", "A", bars_from(base), mk, [], [], {"earn_days": 9}, {}, "", "", "S&P 500", K())
    assert r["label"] not in (hr.EARNINGS, hr.PINNED)


# ----------------------------------------------------------------------------- H6
ITEMS = [["Item ID", "First Seen (Riyadh)", "Published (UTC)", "Scope", "Event Type", "Direction", "Severity",
          "Rumour/Denial", "Source", "Domain", "Tier", "Cluster Grade", "Headline", "URL", "Cluster", "Run ID"],
         ["1", "", "2026-11-17 15:00", "AAA.US", "earnings", -1, "M", "", "google_news", "wire.com", "A", 1,
          "AAA profit falls on weak orders", "u", "c", "r"],
         ["2", "", "2026-11-17 16:00", "AAA.US", "law_firm_noise", -1, "H", "", "google_news", "pr.com", "A", 1,
          "SHAREHOLDER ALERT: law firm investigates AAA", "u", "c", "r"],
         ["3", "", "2026-11-17 22:30", "AAA.US", "rating_change", 1, "L", "", "google_news", "desk.com", "B", 2,
          "AAA upgraded after the close", "u", "c", "r"],
         ["4", "", "2026-11-17 12:00", "MACRO", "war_escalation", -1, "H", "", "google_news", "x.com", "A", 1,
          "Macro item", "u", "c", "r"],
         ["5", "", "2026-11-13 12:00", "AAA.US", "dividend", 1, "L", "", "google_news", "old.com", "B", 2,
          "Last week's item", "u", "c", "r"],
         ["6", "", "2026-11-17 14:00", "AAA.US", "rumour", -1, "H", "", "social", "chat.com", "C", 5,
          "Unconfirmed chatter", "u", "c", "r"],
         ["7", "", "bad date", "AAA.US", "x", 0, "L", "", "s", "d", "C", 1, "No time", "u", "c", "r"]]
GATE = [["Run At (Riyadh)", "Run ID", "Verdict", "Drivers", "Scheduled Events", "Market"],
        ["2026-11-17 22:42", "1", "CAUTION", "market: VIX 26 | macro: war_escalation/iran grade 2, 9 sources: a", "",
         "SPX -1.18%, VIX 26.0"],
        ["2026-11-18 04:40", "2", "NO_NEW_ENTRY", "market: VIX 25.5 | macro: war_escalation/hormuz grade 1, 38 "
         "sources: b", "", "SPX -1.03%, VIX 25.5"]]


def test_h6_parsers_and_the_item_picked():
    items = hr.read_items(ITEMS)
    assert list(items) == ["AAA.US"] and len(items["AAA.US"]) == 5                # macro and the undated row are out
    by = {i["headline"][:12]: i for i in items["AAA.US"]}
    assert by["SHAREHOLDER "]["noise"] and by["Unconfirmed "]["noise"] and not by["AAA profit f"]["noise"]
    assert hr.read_items([["Scope", "Direction"]]) == {}
    g = hr.read_gate(GATE)
    assert g["verdict"] == "NO_NEW_ENTRY" and g["market"] == "SPX -1.03%, VIX 25.5"   # the newest row
    assert hr.read_gate([]) == {"verdict": "", "drivers": "", "market": ""}
    session, prev = dt.date(2026, 11, 17), dt.date(2026, 11, 16)
    a, b = hr.close_utc("AAA.US", prev), hr.close_utc("AAA.US", session)
    assert (a.hour, b.hour) == (21, 21) and hr.close_utc("2222.SR", session).hour == 12
    assert hr.close_utc("BBOX.L", session).minute == 30
    picked = hr.pick_item(items["AAA.US"], a, b)
    assert picked["headline"] == "AAA profit falls on weak orders"                # never the law-firm notice or the rumour
    assert hr.pick_item(items["AAA.US"], b, None)["headline"] == "AAA upgraded after the close"
    assert hr.pick_item(items["AAA.US"], hr.close_utc("AAA.US", dt.date(2026, 11, 18)), None) is None
    # the most severe first, then one with a direction, then the newest
    t = dt.datetime(2026, 11, 17, 15, 0, tzinfo=UTC)
    pool = [item(t, 0, "M", head="neutral M"), item(t, -1, "M", head="negative M"),
            item(t + dt.timedelta(hours=1), -1, "M", head="newer negative M"), item(t, 1, "L", head="positive L")]
    assert hr.pick_item(pool, a, b)["headline"] == "newer negative M"
    assert hr.pick_item(pool + [item(t, 0, "H", head="H item")], a, b)["headline"] == "H item"
    assert hr.pick_item([item(t, 0, "L", head="a filing")], a, b) is None          # not material
    info = hr.read_info([["Symbol", "Name", "Asset Class", "Sector", "Industry"],
                         ["AAA.US", "Alpha", "Equity", "Financial Services", "Banks - Regional"],
                         ["AAA.US", "dup", "", "", ""], ["5023.SR", "Sukuk", "Fixed Income / Sukuk", "", ""]])
    assert info["AAA.US"] == {"name": "Alpha", "asset": "Equity", "group": "Banks - Regional Financial Services"}
    assert hr.sector_fund("AAA.US", info["AAA.US"]["group"], {}) == "KRE"
    assert hr.sector_fund("AAA.US", "Banks", {"AAA.US": "XLF"}) == "XLF"          # the owner's override wins
    assert hr.sector_fund("AAA.US", "Banks", {"AAA.US": ""}) == ""                # and can switch the group off
    assert hr.sector_fund("2222.SR", "Oil & Gas", {}) == "" and hr.sector_fund("ZZZ.US", "Toys", {}) == ""
    assert [hr.sector_fund("X.US", g, {}) for g in ("REIT - Specialty", "Marine Shipping", "Drug Manufacturers",
                                                    "Oil & Gas Midstream")] == ["VNQ", "BOAT", "XPH", "XLE"]
    assert hr.market_symbol("AAA.US") == ("SPY", "S&P 500") and hr.market_symbol("2222.SR")[0] == "^TASI.SR"
    assert hr.market_symbol("BBOX.L") == ("", "")
    assert hr.parse_map("stng.us=boat, AX.US = KRE ,bad,=x, GLPI.US=") == {"STNG.US": "BOAT", "AX.US": "KRE",
                                                                           "GLPI.US": ""}


# ----------------------------------------------------------------------------- H7
def test_h7_the_ledger_marks_each_read():
    assert hr.result_of("REBOUND MORE LIKELY", 0.4) == "HIT" and hr.result_of("REBOUND MORE LIKELY", -0.4) == "MISS"
    assert hr.result_of("PULLBACK MORE LIKELY", -0.1) == "HIT" and hr.result_of("FURTHER FALL MORE LIKELY", 0.1) == "MISS"
    assert hr.result_of("UP MORE LIKELY", 0.0) == "MISS"                          # flat is not up
    assert hr.result_of("NO EDGE", 2.0) == "-" and hr.result_of(hr.PINNED, 1.0) == "-"
    assert hr.result_of("REBOUND MORE LIKELY", None) == ""
    mk = lambda s, sym, read, res="": dict(zip(hr.LEDGER_HEADERS, [s, sym, -2.0, -1.0, "", "MARKET", "n", read, 60,
                                                                    40, 50, "", "", res, "t"]))  # noqa: E731
    led = [mk("2026-11-13", "AAA.US", "REBOUND MORE LIKELY"), mk("2026-11-16", "AAA.US", "NO EDGE"),
           mk("2026-11-17", "AAA.US", "FURTHER FALL MORE LIKELY"), mk("2026-11-13", "BBB.US", "UP MORE LIKELY", "HIT"),
           mk("2026-11-16", "ZZZ.US", "UP MORE LIKELY")]
    rets = {"AAA.US": [(dt.date(2026, 11, 13), -2.0), (dt.date(2026, 11, 16), 1.25), (dt.date(2026, 11, 17), -0.5)]}
    assert hr.settle(led, rets) == 2
    assert (led[0]["Next session"], led[0]["Next %"], led[0]["Result"]) == ("2026-11-16", 1.25, "HIT")
    assert (led[1]["Next session"], led[1]["Result"]) == ("2026-11-17", "-")
    assert led[2]["Result"] == "" and led[4]["Result"] == ""                      # no later bar; no bars at all
    assert led[3]["Next %"] == ""                                                 # a marked row is not touched
    sc = hr.score(led)
    assert sc == {"hit": 2, "miss": 0, "none": 1, "open": 2}
    assert "2 of 2 directional reads were right (100%); 1 read(s) gave no direction; 2 wait" in hr.score_line(sc)
    assert "no directional read has been marked yet" in hr.score_line({"hit": 0, "miss": 0, "none": 3, "open": 1})
    assert hr.parse_ledger([["x"]]) == [] and hr.parse_ledger([]) == []
    grid = [hr.LEDGER_HEADERS] + [[d[h] for h in hr.LEDGER_HEADERS] for d in led]
    assert hr.parse_ledger(grid)[0]["Result"] == "HIT"


# ----------------------------------------------------------------------------- H8
def _series():
    rnd = random.Random(11)
    mkt = [rnd.gauss(0.03, 0.9) for _ in range(398)] + [-1.9, -1.0]
    bank = [1.1 * m + rnd.gauss(0, 0.8) for m in mkt[:-2]] + [-0.2, -2.2]
    grp = [1.0 * m + rnd.gauss(0, 0.5) for m in mkt[:-2]] + [-0.9, -1.9]
    own = [0.8 * m + rnd.gauss(0, 1.5) for m in mkt[:-2]] + [-3.1, -4.5]
    return {"SPY": mkt, "AAA": bank, "KRE": grp, "BBB": own}


SER = _series()
LOTS = [["Symbol", "Name", "Ccy", "Status", "Buy Date", "Buy Price", "Shares"],
        ["AAA.US", "Alpha Bank", "USD", "Active", "2026-10-01", "50", "777"],
        ["BBB.US", "Beta", "USD", "Active", "2026-10-01", "20", "333"],
        ["5023.SR", "Sukuk", "SAR", "Active", "2025-11-23", "100", "100"],
        ["GONE.US", "Gone", "USD", "Active", "2026-10-01", "5", "10"],
        ["SOLD.US", "Sold", "USD", "Inactive", "2026-10-01", "5", "10"]]
PORT = [["Symbol", "Name", "Asset Class", "Sector", "Industry"],
        ["AAA.US", "Alpha Bank", "Equity", "Financial Services", "Banks - Regional"],
        ["BBB.US", "Beta", "Equity", "Consumer", "Toys"],
        ["5023.SR", "Sukuk", "Fixed Income / Sukuk", "", ""], ["GONE.US", "Gone", "Equity", "", ""]]
BBB_NEWS = [ITEMS[0], ["9", "", "2026-11-17 15:30", "BBB.US", "guidance", -1, "H", "", "google_news", "wire.com", "A",
                       1, "Beta cuts its outlook", "u", "c", "r"]]
CALLS = []


def opener(req, timeout):
    sym = req.full_url.split("/chart/")[1].split("?")[0]
    CALLS.append(req.full_url)
    if sym not in SER:
        e = Exception("404")
        e.code = 404
        raise e
    bars = bars_from(SER[sym])
    ts = [int(dt.datetime(b[0].year, b[0].month, b[0].day, 14, 30, tzinfo=UTC).timestamp()) for b in bars]
    c = [b[4] for b in bars]
    return Resp(json.dumps(chart(ts, c, c, c, c, off=0)).encode())


def wbook():
    return Book({"_Portfolio_CostBasis": Sheet(LOTS), "My_Portfolio": Sheet(PORT), "_News_Items": Sheet(BBB_NEWS),
                 "_News_Gate": Sheet(GATE)})


def test_h8_run(monkeypatch, capsys):
    CALLS.clear()
    book = wbook()
    assert hr.run(False, book=book, now=NOW, opener=opener) == 0                   # the gate is off by default
    assert "is off" in capsys.readouterr().out and CALLS == [] and "Holding_Read" not in book.tabs
    monkeypatch.setenv("TFB_HOLDING_READ", "observe")
    assert hr.run(False, book=book, now=NOW, opener=opener) == 0
    assert "observe - nothing written" in capsys.readouterr().out and "Holding_Read" not in book.tabs
    monkeypatch.setenv("TFB_HOLDING_READ", "on")
    assert hr.run(True, book=book, now=NOW, opener=opener) == 0 and "Holding_Read" not in book.tabs   # dry run
    capsys.readouterr()
    CALLS.clear()
    assert hr.run(False, book=book, now=NOW, opener=opener) == 0
    out = capsys.readouterr().out
    assert all("range=5y" in u for u in CALLS) and len(CALLS) == len(set(CALLS))  # each series is fetched once
    page = book.tabs["Holding_Read"].values
    assert page[0][0].startswith("HOLDING READ") and page[4][:len(hr.PAGE_HEAD)] == hr.PAGE_HEAD   # the blank row is dropped by the fake
    assert "last session 2026-11-17 · news gate NO_NEW_ENTRY (SPX -1.03%, VIX 25.5) · holding_read 1.0.0" in page[1][0]
    rows = {r[0]: r for r in page[5:]}
    assert list(rows) == ["5023.SR", "AAA.US", "BBB.US", "GONE.US"]                # the inactive lot is not read
    a = rows["AAA.US"]
    assert a[2] == "2026-11-17" and a[3] == "-2.20%" and a[4] == "-1.00%" and a[5] == "-1.90%" and a[6] == "SECTOR"
    assert a[7].startswith("No company item. Its group moved: KRE -1.9%, S&P 500 -1.0%. Macro themes in the news "
                           "gate: war escalation/hormuz.")
    assert a[8] in hr.UP_READS + hr.DOWN_READS + (hr.NO_EDGE, hr.NO_HISTORY) and re.match(r"\d+% of \d+ \(any day \d+%\)$", a[9])
    b = rows["BBB.US"]
    assert b[6] == "COMPANY NEWS" and b[7] == "Beta cuts its outlook (wire.com, 11-17 18:30 Riyadh)" and b[5] == ""
    assert rows["5023.SR"][6] == "NOT READ" and "fixed income" in rows["5023.SR"][7]
    assert rows["GONE.US"][6] == "NOT READ" and "no price history" in rows["GONE.US"][7]
    led = book.tabs["_Holding_Read"].values
    assert led[0] == hr.LEDGER_HEADERS and [r[1] for r in led[1:]] == ["AAA.US", "BBB.US"]
    assert led[1][0] == "2026-11-17" and led[1][5] == "SECTOR" and led[1][13] == ""
    # a re-run for the same session replaces its rows; nothing is doubled
    assert hr.run(False, book=book, now=NOW + dt.timedelta(hours=1), opener=opener) == 0
    assert [r[1] for r in book.tabs["_Holding_Read"].values[1:]] == ["AAA.US", "BBB.US"]
    # the next session closes: yesterday's reads are marked, today's are added
    for sym, nxt in (("SPY", 0.6), ("AAA", 1.4), ("KRE", 1.0), ("BBB", -2.0)):
        monkeypatch.setitem(SER, sym, SER[sym] + [nxt])
    real = bars_from
    monkeypatch.setattr(sys.modules[__name__], "bars_from", lambda rets, end=dt.date(2026, 11, 18), start=100.0:
                        real(rets, end, start))
    capsys.readouterr()
    assert hr.run(False, book=book, now=NOW + dt.timedelta(days=1), opener=opener) == 0
    led = book.tabs["_Holding_Read"].values
    assert [(r[0], r[1]) for r in led[1:]] == [("2026-11-17", "AAA.US"), ("2026-11-17", "BBB.US"),
                                                ("2026-11-18", "AAA.US"), ("2026-11-18", "BBB.US")]
    assert led[1][11] == "2026-11-18" and led[1][12] == 1.4 and led[1][13] == hr.result_of(led[1][7], 1.4)
    assert led[2][12] == -2.0 and led[3][13] == ""
    assert "2 earlier read(s) marked" in capsys.readouterr().out
    assert out                                                                     # (kept for H9)


def test_h8b_read_and_write_failures(monkeypatch, capsys):
    monkeypatch.setenv("TFB_HOLDING_READ", "on")

    class Broken(Book):
        def worksheet(self, name):
            raise ConnectionError("down")
    assert hr.run(False, book=Broken({}), now=NOW, opener=opener) == 2
    assert "cannot read the workbook: ConnectionError; nothing written" in capsys.readouterr().out
    book = wbook()

    def boom(*a, **kw):
        raise TimeoutError("slow")
    monkeypatch.setattr(hr, "write_grid", boom)
    assert hr.run(False, book=book, now=NOW, opener=opener) == 3
    assert "publication unconfirmed: TimeoutError" in capsys.readouterr().out


# ----------------------------------------------------------------------------- H9
def test_h9_the_public_log_carries_no_quantity_or_value(monkeypatch, capsys):
    monkeypatch.setenv("TFB_HOLDING_READ", "on")
    assert hr.run(False, book=wbook(), now=NOW, opener=opener) == 0
    out = capsys.readouterr().out
    assert "777" not in out and "333" not in out and "SAR" not in out and "$" not in out
    assert "AAA.US    SECTOR" in out and "stock -2.20% market -1.00% group -1.90%" in out


# ----------------------------------------------------------------------------- H10
def test_h10_workflow_manifest_ci():
    wf = yaml.safe_load((ROOT / ".github/workflows/holding_read.yml").read_text(encoding="utf-8"))
    on = wf.get("on") or wf.get(True)
    assert "workflow_dispatch" in on and len(on["schedule"]) >= 1
    step = [s for s in wf["jobs"]["holding_read"]["steps"] if "holding_read.py" in str(s.get("run", ""))][0]
    env = step["env"]
    assert env["TFB_HOLDING_READ"] == "${{ vars.TFB_HOLDING_READ || 'on' }}"
    for name, default in (("TFB_HREAD_MIN_N", "30"), ("TFB_HREAD_EDGE_PTS", "5"), ("TFB_HREAD_Z", "1.64"),
                          ("TFB_HREAD_PIN_PCT", "0.6"), ("TFB_HREAD_RANGE", "5y"), ("TFB_HREAD_SECTOR_MAP", "")):
        assert env[name] == "${{ vars.%s || '%s' }}" % (name, default), name
    assert wf["permissions"] == {"contents": "read"} and wf["concurrency"]["cancel-in-progress"] is False
    vd = (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("holding_read", "%s", "holding read")' % hr.__version__ in vd and hr.__version__ == "1.0.0"
    ci = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    assert any("tests/test_holding_read.py" in s.get("run", "") for s in ci["jobs"]["lean-unit"]["steps"])
    k = hr.knobs()
    assert (k["min_n"], k["edge_pts"], k["z"], k["pin_pct"], k["vol_win"], k["range"]) == (30, 5.0, 1.64, 0.6, 60, "5y")
