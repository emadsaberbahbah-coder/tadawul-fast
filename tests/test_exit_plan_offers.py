#!/usr/bin/env python3
"""tests/test_exit_plan_offers.py - scripts/exit_plan.py 1.7.0 and core/corporate_offers.py 1.0.0.

  O1  the offers registry: parse (bad entries left out), standing / lapsed by day, the reading (gap, days left, the
      completion the price implies), the label; load() never raises; the shipped file is valid
  O2  [OFFER] a holding under a standing cash offer, sale = manual: the stop of record is frozen at its value before
      this close, the target is the offer price, the action is HOLD where the price rule would RAISE STOP or SELL
  O3  [OFFER] REVIEW: the price doubts the offer (implied completion under the limit; with no close before the
      offer, the gap), the market pays the offer, the last days
  O4  [OFFER] sale = rule keeps the price rule, caps the target and names the offer; the gate off = 1.6.3
  O5  [OFFER] run(): the page, the ledger column, no market-turn SELL, the frozen stop on the next close, a passed
      last day, and a caller's book never reads the registry file
  J1  [STOP-JUDGMENT] the pure reading: no fall, with its market (wait), with its market but worse than the margin,
      on its own, mixed, the hard limit; None without history or a market close near either end
  J2  [STOP-JUDGMENT] run(): a note and a ledger column, never an action; off = nothing; an exempt asset class
  J3  the ledger: 19 columns, and the 1.6.x (18) and 1.5.0 (17) shapes still read
  J4  the workflow knobs, the manifest pins and the CI lane

Run: pytest -q tests/test_exit_plan_offers.py
"""
from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core import corporate_offers as co  # noqa: E402
from scripts import exit_plan as ep  # noqa: E402
from tests import test_exit_plan as base  # noqa: E402

NOW, TODAY, days, mk, Sheet, Book = base.NOW, base.TODAY, base.days, base.mk, base.Sheet, base.Book


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for name in list(__import__("os").environ):
        if name.startswith("TFB_EXIT_") or name == "TFB_CORPORATE_OFFERS_FILE":
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TFB_EXIT_PLAN", "on")
    monkeypatch.setenv("TFB_EXIT_LOG_DETAIL", "on")
    monkeypatch.setattr(ep, "_SLEEP", lambda s: None)


def K(**over):
    k = ep.knobs()
    k.update(over)
    return k


def offer(**over):
    raw = {"symbol": "AAA.US", "kind": "cash_tender", "price": 115.0, "announced": days(32)[31].isoformat(),
           "expires": (TODAY + dt.timedelta(days=20)).isoformat(), "bidder": "Bidder Co", "conditions": "a minimum",
           "sale": "manual"}
    raw.update(over)
    got = co.parse({"schema": co.SCHEMA, "offers": [raw]})
    assert len(got) == 1, raw
    return got[0]


# ----------------------------------------------------------------------------- O1
def test_o1_registry_parse_and_reading(tmp_path, monkeypatch):
    doc = {"schema": co.SCHEMA, "offers": [
        {"symbol": " aaa.us ", "kind": "CASH_TENDER", "price": "16.25", "announced": "2026-11-09",
         "expires": "2026-12-10", "sale": "MANUAL", "bidder": "B", "conditions": "c"},
        {"symbol": "BBB.US", "kind": "cash_merger", "price": 40, "expires": "2026-12-01"},            # sale -> rule
        {"symbol": "CCC.US", "kind": "share_swap", "price": 10, "expires": "2026-12-01"},             # unknown kind
        {"symbol": "DDD.US", "kind": "cash_tender", "price": 0, "expires": "2026-12-01"},             # no price
        {"symbol": "EEE.US", "kind": "cash_tender", "price": 5, "expires": "soon"},                   # no day
        {"symbol": "", "kind": "cash_tender", "price": 5, "expires": "2026-12-01"},
        {"symbol": "FFF.US", "kind": "cash_tender", "price": True, "expires": "2026-12-01"},
        {"symbol": "GGG.US", "kind": "cash_tender", "price": 9, "announced": "2026-12-05", "expires": "2026-12-01"},
        "junk"]}
    got = co.parse(doc)
    assert [o["symbol"] for o in got] == ["AAA.US", "BBB.US", "GGG.US"]
    a, b, g = got
    assert a["price"] == 16.25 and a["sale"] == "manual" and a["expires"] == dt.date(2026, 12, 10)
    assert a["announced"] == dt.date(2026, 11, 9) and b["sale"] == "rule" and b["announced"] is None
    assert g["announced"] is None                                                   # announced after the last day
    assert co.parse({"schema": "other", "offers": doc["offers"]}) == [] and co.parse([]) == [] and co.parse(None) == []
    assert co.label(a) == "cash tender by B, conditional" and co.label(b) == "cash merger"
    # by day
    assert co.standing(got, "aaa.us", dt.date(2026, 11, 9)) is a and co.standing(got, "AAA.US", dt.date(2026, 12, 10)) is a
    assert co.standing(got, "AAA.US", dt.date(2026, 11, 8)) is None                 # before it was announced
    assert co.standing(got, "AAA.US", dt.date(2026, 12, 11)) is None and co.standing(got, "ZZZ.US", TODAY) is None
    assert co.lapsed(got, "AAA.US", dt.date(2026, 12, 11)) is a and co.lapsed(got, "AAA.US", dt.date(2026, 12, 10)) is None
    assert co.lapsed(got, "ZZZ.US", dt.date(2027, 1, 1)) is None
    two = co.parse({"schema": co.SCHEMA, "offers": [dict(doc["offers"][0], price=16.25),
                                                     dict(doc["offers"][0], price=17.0, expires="2026-12-24")]})
    assert co.standing(two, "AAA.US", dt.date(2026, 12, 1))["price"] == 17.0        # the extended one
    assert co.lapsed(two, "AAA.US", dt.date(2026, 12, 15)) is None                  # one still stands
    # the reading
    closes = [(dt.date(2026, 11, 5), 13.0), (dt.date(2026, 11, 6), 13.6), (dt.date(2026, 11, 9), 15.5)]
    assert co.close_before(a, closes) == 13.6 and co.close_before(b, closes) is None
    assert co.close_before(a, closes[2:]) is None
    r = co.reading(a, 15.5, dt.date(2026, 11, 26), 13.6)
    assert r["days_left"] == 14 and r["gap_pct"] == pytest.approx((16.25 / 15.5 - 1) * 100)
    assert r["implied_pct"] == pytest.approx((15.5 - 13.6) / (16.25 - 13.6) * 100)
    assert co.reading(a, 17.0, TODAY, 13.6)["implied_pct"] == 100.0 and co.reading(a, 17.0, TODAY, 13.6)["gap_pct"] < 0
    assert co.reading(a, 12.0, TODAY, 13.6)["implied_pct"] == 0.0
    assert co.reading(a, 15.5, TODAY, None)["implied_pct"] is None
    assert co.reading(a, 15.5, TODAY, 16.5)["implied_pct"] is None                  # the close before is not under it
    assert co.reading(a, None, TODAY, 13.6)["gap_pct"] is None
    # load never raises
    assert co.load(str(tmp_path / "none.json")) == []
    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    assert co.load(str(bad)) == []
    good = tmp_path / "good.json"
    good.write_text(json.dumps(doc), encoding="utf-8")
    monkeypatch.setenv("TFB_CORPORATE_OFFERS_FILE", str(good))
    assert [o["symbol"] for o in co.load()] == ["AAA.US", "BBB.US", "GGG.US"]


def test_o1_the_shipped_registry_is_valid():
    raw = json.loads((ROOT / "config/corporate_offers.json").read_text(encoding="utf-8"))
    assert raw["schema"] == co.SCHEMA and raw["about"]
    got = co.load(str(ROOT / "config/corporate_offers.json"))
    assert len(got) == len(raw["offers"]) >= 1                                      # no entry is silently dropped
    for o in got:
        assert o["kind"] in co.KINDS and o["sale"] in co.SALES and o["price"] > 0 and o["source"]
        assert o["announced"] is None or o["announced"] <= o["expires"]
    text = json.dumps(raw).lower()
    for word in ("shares held", "quantity", "account"):                             # public file: offers, not holdings
        assert word not in text, word
    assert co.DEFAULT_PATH == str(ROOT / "config" / "corporate_offers.json")


# ----------------------------------------------------------------------------- O2
RISE = [100.0] * 30 + [104.0, 110.0, 112.0, 111.5, 111.8]
PREV = {"Stop": 97.0, "StopNorm": 97.0, "Was": 97.0, "WasNorm": 97.0, "TP1 Reached": "yes"}   # TP1 issued before


def lv(closes, info=None, prev=None, k=None, entry=100.0):
    bars = mk(closes, 0.015)
    return ep.exit_levels("AAA.US", entry, bars[29][0], bars, info or {}, prev or {}, k or K(), TODAY)


def test_o2_manual_offer_freezes_the_stop_and_caps_the_target():
    plain = lv(RISE, prev=PREV)
    assert plain["action"] == "RAISE STOP" and plain["stop"] > 101.0 and plain["target"] == pytest.approx(118.0)
    r = lv(RISE, {"offer": offer()}, PREV)
    assert r["action"] == "HOLD" and r["stop"] == 97.0 and r["target"] == 115.0 and r["offer_held"] is True
    assert r["stop_gap_pct"] == pytest.approx((1 - 97.0 / 111.8) * 100)
    o = r["offer"]
    assert o["days_left"] == 20 and o["before"] == pytest.approx(104.0)
    assert o["implied_pct"] == pytest.approx((111.8 - 104.0) / (115.0 - 104.0) * 100)
    assert r["why"][0].startswith("cash offer 115.00 a share (cash tender by Bidder Co, conditional), open until %s - "
                                  "20 days left, and the broker's cut-off comes earlier; the last close is 2.9%% under "
                                  "it, which against the close before the offer (104.00) prices about 71%% that it "
                                  "completes (a rough reading)" % (TODAY + dt.timedelta(days=20)).isoformat())
    assert r["why"][-1] == ("the sale is the owner's manual decision: no stop order is asked for; the stop of record "
                            "97.00 is frozen while the offer stands and the target is the offer price; the market-turn "
                            "rule does not apply (the price follows the offer, not the market)")
    assert len(r["why"]) == 2 and not any("raise the stop" in w or "trail" in w for w in r["why"])
    # a close under the frozen stop is not a price-rule SELL while the offer stands
    low = lv(RISE[:-1] + [96.5], {"offer": offer()}, PREV)
    assert lv(RISE[:-1] + [96.5], prev=PREV)["action"] == "SELL"
    assert low["action"] == "REVIEW" and low["stop"] == 97.0 and "in doubt" in low["why"][1]
    assert not any("at or below the stop" in w for w in low["why"])
    # Codex P2 on #1084: the target IS the offer price, also when the price rule's next step is under it
    flat = lv([100.0] * 40, {"offer": offer(announced=None)})
    assert lv([100.0] * 40)["target"] == pytest.approx(109.0) and flat["target"] == 115.0 and flat["offer_held"]
    assert lv([100.0] * 40, {"offer": offer(announced=None, sale="rule")})["target"] == pytest.approx(109.0)   # capped only
    assert lv(RISE, {"offer": offer(price=105.0, sale="rule")}, PREV)["target"] == 105.0
    # no stop on record yet: the rule's stop of this close is the one frozen
    first = lv(RISE, {"offer": offer()})
    assert first["stop"] == pytest.approx(lv(RISE)["stop"]) and first["action"] == "HOLD"
    # the stop-specific notes of the price rule are not shown: the live price, the broker stop
    quiet = lv(RISE, {"offer": offer(), "live": 99.0, "broker_stop": 90.0, "earn_days": 1, "news_neg": 2}, PREV)
    assert quiet["live"] == 99.0 and len(quiet["why"]) == 2
    # a lot with no close since its purchase waits for the first one, as the price rule does
    bars = mk(RISE, 0.015)
    fresh = ep.exit_levels("AAA.US", 111.0, bars[-1][0] + dt.timedelta(days=1), bars, {"offer": offer()}, {}, K(), TODAY)
    assert fresh["fresh"] and fresh["action"] == "HOLD" and "offer" not in fresh and "no close since" in fresh["why"][0]


# ----------------------------------------------------------------------------- O3
def test_o3_review_when_the_offer_is_in_doubt_reached_or_closing():
    doubt = lv(RISE[:-1] + [105.0], {"offer": offer()}, PREV)                      # implied (105-104)/(115-104) = 9%
    assert doubt["action"] == "REVIEW" and doubt["why"][1] == (
        "the price puts the offer in doubt (it prices under 50% that it completes): read today's news on it before "
        "anything else")
    assert lv(RISE[:-1] + [109.4], {"offer": offer()}, PREV)["action"] == "REVIEW"  # 49%
    assert lv(RISE[:-1] + [109.6], {"offer": offer()}, PREV)["action"] == "HOLD"    # 51%
    assert lv(RISE[:-1] + [105.0], {"offer": offer()}, PREV, K(offer_doubt_pct=5.0))["action"] == "HOLD"
    # with no day of announcement there is no close before it: the gap decides
    far = lv(RISE[:-1] + [105.0], {"offer": offer(announced=None)}, PREV)           # 9.5% under
    assert far["action"] == "REVIEW" and "more than 8% under it" in far["why"][1] and far["offer"]["implied_pct"] is None
    assert lv(RISE, {"offer": offer(announced=None)}, PREV)["action"] == "HOLD"     # 2.9% under
    near = lv(RISE[:-1] + [114.6], {"offer": offer()}, PREV)                        # 0.35% under
    assert near["action"] == "REVIEW" and near["why"][1].startswith("the market pays the offer now")
    above = lv(RISE[:-1] + [116.0], {"offer": offer()}, PREV)
    assert above["action"] == "REVIEW" and "the last close is 0.9% above it" in above["why"][0]
    assert above["target"] == 115.0
    for left, words in ((5, "5 days to the last day"), (1, "1 day to the last day"), (0, "the offer closes today")):
        r = lv(RISE, {"offer": offer(expires=(TODAY + dt.timedelta(days=left)).isoformat())}, PREV)
        assert r["action"] == "REVIEW" and r["why"][1] == (
            "%s: decide - sell in the market, tender at the broker, or keep the shares" % words), left
    assert lv(RISE, {"offer": offer(expires=(TODAY + dt.timedelta(days=6)).isoformat())}, PREV)["action"] == "HOLD"
    assert "the last day, and" in lv(RISE, {"offer": offer(expires=TODAY.isoformat())}, PREV)["why"][0]


# ----------------------------------------------------------------------------- O4
def test_o4_sale_rule_keeps_the_price_rule_and_the_gate_off_is_1_6_3():
    plain = lv(RISE, prev=PREV)
    r = lv(RISE, {"offer": offer(sale="rule")}, PREV)
    assert r["action"] == plain["action"] == "RAISE STOP" and r["stop"] == plain["stop"]
    assert r["target"] == 115.0 and "offer_held" not in r
    assert r["why"][:-1] == plain["why"] and r["why"][-1].endswith(
        "the target is capped at the offer price; if its shares were tendered, a sell order on them can open a short "
        "position")
    assert r["why"][-1].startswith("cash offer 115.00 a share (cash tender by Bidder Co, conditional)")
    for o in (offer(), offer(sale="rule")):
        off = lv(RISE, {"offer": o}, PREV, K(offers=False))
        assert off == plain
    assert K()["offers"] is True and K()["offer_doubt_pct"] == 50.0 and K()["offer_warn_days"] == 5


def test_o4_gate_words(monkeypatch):
    for raw, want in (("off", False), ("0", False), ("no", False), ("on", True), ("", True), ("maybe", True)):
        monkeypatch.setenv("TFB_EXIT_OFFERS", raw)
        assert ep.knobs()["offers"] is want, raw
    for raw, want in (("off", "off"), ("0", "off"), ("observe", "observe"), ("on", "observe"), ("", "observe")):
        monkeypatch.setenv("TFB_EXIT_STOP_JUDGMENT", raw)
        assert ep.stop_judgment_mode() == want and ep.knobs()["stop_judgment"] == want, raw   # there is no 'on'


# ----------------------------------------------------------------------------- O5
def ddi_offer(**over):
    return [offer(symbol="DDI.US", announced=days(32)[31].isoformat(), expires="2026-12-10", **over)]


def page(b):
    return "\n".join(" | ".join(str(c) for c in r) for r in b.tabs["Exit_Plan"].values)


def row(b, sym="DDI.US"):
    return [r for r in ep.parse_ledger(b.tabs["_Exit_Plan"].values) if r["Symbol"] == sym][-1]


def test_o5_run_with_a_standing_manual_offer(monkeypatch, capsys):
    b = base.wbook()
    assert ep.run(False, False, book=b, now=NOW, opener=base.opener, market=base.CRASH) == 0
    assert row(b)["Action"] == "TAKE PROFIT" and row(b)["Judgment"] == ""           # a caller's book: no registry read
    monkeypatch.setenv("TFB_EXIT_MARKET_TURN", "on")
    b = base.wbook()
    assert ep.run(False, False, book=b, now=NOW, opener=base.opener, market=base.CRASH, offers=ddi_offer()) == 0
    r = row(b)
    assert r["Action"] == "HOLD" and r["Judgment"] == "offer:manual" and r["Target"] == 115.0
    assert r["Market"] == "us:crash_day:2026-11-10"                                 # recorded, never acted on
    stop0 = r["Stop"]
    text = page(b)
    assert "cash offer 115.00 a share (cash tender by Bidder Co, conditional), open until 2026-12-10 - 29 days left" in text
    assert "the market turned" not in text and "market not calm" not in text
    assert "under a cash offer, manual sale: 1" in text
    assert "Cash offer: a holding under a standing cash offer whose sale is manual has no stop in force" in text
    assert "DDI.US    HOLD" in capsys.readouterr().out
    # the next close is higher: the price rule would raise the stop; the frozen one stays
    monkeypatch.setitem(base.SERIES, "DDI", base.SERIES["DDI"] + [113.0])
    later = NOW + dt.timedelta(days=1)
    assert ep.run(False, False, book=b, now=later, opener=base.opener, market=base.CRASH, offers=ddi_offer()) == 0
    assert row(b)["Stop"] == stop0 and row(b)["Action"] == "HOLD"
    plain = base.wbook()
    assert ep.run(False, False, book=plain, now=later, opener=base.opener, market=base.CRASH) == 0
    assert row(plain)["Action"] == "SELL"                                           # market turn on, no offer
    # the gate off: the registry is ignored
    monkeypatch.setenv("TFB_EXIT_OFFERS", "off")
    off = base.wbook()
    assert ep.run(False, False, book=off, now=later, opener=base.opener, market=base.CRASH, offers=ddi_offer()) == 0
    assert row(off)["Action"] == "SELL" and row(off)["Judgment"] == "" and "cash offer" not in page(off).lower()


def test_o5_a_passed_last_day_and_the_public_log(monkeypatch, capsys):
    b = base.wbook()
    gone = [offer(symbol="DDI.US", announced=days(32)[31].isoformat(), expires="2026-11-10")]
    assert ep.run(False, False, book=b, now=NOW, opener=base.opener, market=base.CRASH, offers=gone) == 0
    assert row(b)["Action"] == "TAKE PROFIT" and row(b)["Judgment"] == ""
    assert ("the cash offer at 115.00 on it had its last day on 2026-11-10: record its outcome in "
            "config/corporate_offers.json; the price rule is in force") in page(b)
    assert "Cash offer:" not in page(b)
    # the registry file is read by the live run only (no book given): a caller's book with no `offers` reads none
    monkeypatch.setattr(co, "load", lambda path=None: (_ for _ in ()).throw(AssertionError("registry read")))
    assert ep.run(False, False, book=base.wbook(), now=NOW, opener=base.opener, market=base.CRASH) == 0
    # the public run log names no holding and no offer price
    capsys.readouterr()
    monkeypatch.setenv("TFB_EXIT_LOG_DETAIL", "off")
    assert ep.run(False, False, book=base.wbook(), now=NOW, opener=base.opener, market=base.CRASH, offers=ddi_offer()) == 0
    out = capsys.readouterr().out
    assert "holding 2 of 3: HOLD (cash offer)" in out and "DDI" not in out and "115" not in out


# ----------------------------------------------------------------------------- J1
def fall(pct, n=40, window=15):
    """n closes: flat at 100, then a straight fall of pct % over the last `window` sessions."""
    flat = n - window
    return [100.0] * flat + [100.0 * (1 - pct / 100.0 * (i + 1) / window) for i in range(window)]


def series(pct, n=40):
    return [(d, c) for d, c in zip(days(n), fall(pct, n))]


def res(**over):
    out = {"ok": True, "fresh": False, "pl_pct": -9.0, "stop_gap_pct": 6.0, "action": "HOLD"}
    out.update(over)
    return out


def test_j1_stop_judgment_readings():
    k = K()
    assert (k["sj_window"], k["sj_drop"], k["sj_market"], k["sj_alone"], k["sj_margin"], k["sj_hard"], k["sj_near"]) == (
        15, 8.0, 5.0, 1.0, 5.0, 15.0, 3.0)
    bars = mk(fall(10.0))
    j = ep.stop_judgment(bars, series(6.0), res(), k, "its market (S&P 500 fund (SPY))")
    assert j["cause"] == "with_market" and j["verdict"] == "wait" and j["show"] is True
    assert j["stock_pct"] == pytest.approx(-10.0) and j["market_pct"] == pytest.approx(-6.0)
    assert j["code"] == "with_market:wait:-10.0:-6.0"
    assert j["text"] == (
        "stop judgment (a note under test): over 15 sessions the stock -10.0% and its market (S&P 500 fund (SPY)) "
        "-6.0%: it fell WITH ITS MARKET (4.0 points worse than it) - the rule under test would wait rather than sell "
        "at the stop, while it stays within 5 points of its market and the loss from the entry stays under 15%. "
        "Nothing changes: the stop of record and the stop at the broker stand")
    worse = ep.stop_judgment(mk(fall(14.0)), series(6.0), res(), k)
    assert worse["cause"] == "with_market" and worse["verdict"] == "stands" and "fell 8.0 points more" in worse["text"]
    better = ep.stop_judgment(mk(fall(8.5)), series(11.5), res(), k)
    assert better["verdict"] == "wait" and "3.0 points better than it" in better["text"]
    alone = ep.stop_judgment(bars, series(0.5), res(), k)
    assert alone["cause"] == "alone" and alone["verdict"] == "stands" and "ON ITS OWN while its market held" in alone["text"]
    mixed = ep.stop_judgment(bars, series(3.0), res(), k)
    assert mixed["cause"] == "mixed" and mixed["verdict"] == "stands" and "partly on its own" in mixed["text"]
    hard = ep.stop_judgment(bars, series(6.0), res(pl_pct=-15.0), k)
    assert hard["cause"] == "with_market" and hard["verdict"] == "hard_limit" and "past the hard limit of 15%" in hard["text"]
    # no fall: shown only when the stop is near, the action is SELL, or the hard limit is passed
    calm = ep.stop_judgment(mk(fall(4.0)), series(6.0), res(), k)
    assert calm["cause"] == "no_fall" and calm["verdict"] == "none" and calm["show"] is False
    assert calm["code"] == "no_fall:none:-4.0:-6.0"
    assert ep.stop_judgment(mk(fall(4.0)), series(6.0), res(stop_gap_pct=2.9), k)["show"] is True
    assert ep.stop_judgment(mk(fall(4.0)), series(6.0), res(action="SELL"), k)["show"] is True
    assert "no fall of 8% to judge" in ep.stop_judgment(mk(fall(4.0)), series(6.0), res(stop_gap_pct=1.0), k)["text"]
    # the limits are the knobs
    assert ep.stop_judgment(bars, series(6.0), res(), K(sj_market=7.0))["cause"] == "mixed"
    assert ep.stop_judgment(bars, series(6.0), res(), K(sj_margin=3.0))["verdict"] == "stands"
    assert ep.stop_judgment(bars, series(6.0), res(), K(sj_drop=12.0))["cause"] == "no_fall"
    # nothing to read
    assert ep.stop_judgment(bars, [], res(), k) is None
    assert ep.stop_judgment(bars[:15], series(6.0), res(), k) is None               # no 15 sessions of history
    assert ep.stop_judgment(bars, series(6.0), res(ok=False), k) is None
    assert ep.stop_judgment(bars, series(6.0), res(fresh=True), k) is None
    stale = [(d, c) for d, c in series(6.0) if d <= bars[-6][0]]                    # the market's last close a week old
    assert ep.stop_judgment(bars, stale, res(), k) is None
    late = [(d, c) for d, c in series(6.0) if d > bars[-10][0]]                     # none near the first day
    assert ep.stop_judgment(bars, late, res(), k) is None


# ----------------------------------------------------------------------------- J2
def fall_book():
    lots = [["Symbol", "Name", "Ccy", "Status", "Buy Date", "Buy Price", "Shares"],
            ["DDI.US", "DoubleDown", "USD", "Active", days(30)[29].isoformat(), "100", "240"],
            ["GOLD.US", "Gold fund", "USD", "Active", days(30)[29].isoformat(), "100", "10"]]
    port = [["Symbol", "Name", "Asset Class", "Current Price", "Forecast Price (1M)", "Forecast Reliability Score"],
            ["DDI.US", "DoubleDown", "Equity", 95, "", ""], ["GOLD.US", "Gold fund", "Commodity", 95, "", ""]]
    return Book({"_Portfolio_CostBasis": Sheet(lots), "My_Portfolio": Sheet(port), "_Daily_Plan": Sheet(base.PLAN)})


CALM = {"us": {"label": "calm", "day": dt.date(2026, 11, 10), "text": "the US market calm as of 2026-11-10"}}


def test_j2_run_logs_the_judgment_and_never_changes_the_action(monkeypatch):
    closes = [100.0] * 30 + [100.0 * (1 - 0.09 * (i + 1) / 15) for i in range(15)]  # -9% over the last 15 sessions
    monkeypatch.setitem(base.SERIES, "DDI", closes)
    monkeypatch.setitem(base.SERIES, "GOLD", closes)
    mkt = {"us": [(d, c) for d, c in zip(days(45), [100.0] * 30 + [100.0 * (1 - 0.06 * (i + 1) / 15) for i in range(15)])]}
    b = fall_book()
    assert ep.run(False, False, book=b, now=NOW, opener=base.opener, market=CALM, market_closes=mkt) == 0
    with_note = row(b)
    text = page(b)
    assert with_note["Judgment"] == "with_market:wait:-9.0:-6.0"
    assert ("stop judgment (a note under test): over 15 sessions the stock -9.0% and its market (S&P 500 fund (SPY)) "
            "-6.0%: it fell WITH ITS MARKET") in text
    assert "stop judgment: 1 would wait (a note under test)" in text
    assert "Stop judgment, under test: a fall of 8% or more in 15 sessions" in text
    assert row(b, "GOLD.US")["Judgment"] == ""                                      # no stock-market label describes it
    # the labels given without the closes: no judgment, nothing read
    b1 = fall_book()
    assert ep.run(False, False, book=b1, now=NOW, opener=base.opener, market=CALM) == 0
    assert row(b1)["Judgment"] == "" and "stop judgment" not in page(b1)
    # off: the same action, stop and target; no note, no column value, no rule line
    monkeypatch.setenv("TFB_EXIT_STOP_JUDGMENT", "off")
    b2 = fall_book()
    assert ep.run(False, False, book=b2, now=NOW, opener=base.opener, market=CALM, market_closes=mkt) == 0
    off = row(b2)
    assert off["Judgment"] == "" and "judgment" not in page(b2).lower()
    for col in ("Action", "Stop", "Target", "Entry", "Last", "TP1 Reached", "Market"):
        assert off[col] == with_note[col], col
    assert with_note["Action"] == "SELL"                                            # the price rule's own reading stands


def test_j2_the_live_run_reads_each_market_once(monkeypatch):
    calls = []
    monkeypatch.setattr(ep, "read_market", lambda now, opener: CALM)
    monkeypatch.setattr(ep, "read_market_closes", lambda mid, now, opener: calls.append(mid) or [])
    b = fall_book()
    assert ep.run(False, False, book=b, now=NOW, opener=base.opener) == 0
    assert calls == ["us"] and row(b)["Judgment"] == ""
    assert ep.market_reading_name("us") == "its market (S&P 500 fund (SPY))" and ep.market_reading_name("zz") == "its market"

    def boom(*a, **k):
        raise OSError("network")
    monkeypatch.undo()
    from core import market_condition as mc
    monkeypatch.setattr(mc, "series", boom)
    monkeypatch.setattr(ep, "_SLEEP", lambda s: None)
    assert ep.read_market_closes("us", NOW, base.opener) == []                      # a fault is an empty reading


# ----------------------------------------------------------------------------- J3
def test_j3_the_ledger_shapes():
    assert ep.LEDGER_HEADERS[-2:] == ["Market", "Judgment"] and len(ep.LEDGER_HEADERS) == 19
    assert ep.LEDGER_HEADERS_V16 == ep.LEDGER_HEADERS[:18] and ep.LEDGER_HEADERS_V15 == ep.LEDGER_HEADERS[:17]
    line = ["2026-11-10", "x", "AAA.US", "lot", 100, 10, 104, 104, 3, 9, 95.0, 109, "no", "HOLD", "", "2026-11-09", 1]
    v15 = ep.parse_ledger([ep.LEDGER_HEADERS_V15, line])
    v16 = ep.parse_ledger([ep.LEDGER_HEADERS_V16, line + ["us:calm:2026-11-09"]])
    v17 = ep.parse_ledger([ep.LEDGER_HEADERS, line + ["us:calm:2026-11-09", "alone:stands:-9.0:-0.5"]])
    assert v15[0]["Market"] == "" and v15[0]["Judgment"] == "" and v15[0]["Stop"] == 95.0
    assert v16[0]["Market"] == "us:calm:2026-11-09" and v16[0]["Judgment"] == ""
    assert v17[0]["Judgment"] == "alone:stands:-9.0:-0.5"
    assert ep.parse_ledger([["Date", "Symbol"], ["2026-11-10", "AAA.US"]]) == []


# ----------------------------------------------------------------------------- J4
def test_j4_workflow_manifest_and_ci():
    raw = (ROOT / ".github/workflows/exit_plan.yml").read_text(encoding="utf-8")
    wf = yaml.safe_load(raw)
    step = [s for s in wf["jobs"]["exit_plan"]["steps"] if "exit_plan.py" in str(s.get("run", ""))][0]
    assert step["env"]["TFB_EXIT_OFFERS"] == "${{ vars.TFB_EXIT_OFFERS || 'on' }}"
    assert step["env"]["TFB_EXIT_STOP_JUDGMENT"] == "${{ vars.TFB_EXIT_STOP_JUDGMENT || 'observe' }}"
    manifest = (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("exit_plan", "%s", "exit plan")' % ep.__version__ in manifest and ep.__version__ == "1.7.0"
    assert '("core.corporate_offers", "__version__", "%s", "corporate offers")' % co.__version__ in manifest
    assert co.__version__ == "1.0.0"
    ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "tests/test_exit_plan_offers.py" in ci
    src = (ROOT / "scripts/exit_plan.py").read_text(encoding="utf-8")
    assert "places, changes or cancels an order" in src
