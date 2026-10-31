#!/usr/bin/env python3
"""D1b post-earnings and D1c open-ticket ADD guards (portfolio_actions 1.17.0).

All holdings, broker positions, settled cash, FX and calendar inputs are
synthetic. The real certified builder supplies every off / observe / enforce
comparison; the v1.16.1 base module (loaded from git when available) proves
that off is byte-identical whether or not the new request field is sent.

v1.17.1: a malformed entry is unknown evidence (never dropped, never
coerced), an unknown EPS surprise takes the longer window, and the optional
"uncovered" list holds a listed ADD; the v1.17.0 base module (0c3d3e21)
proves off is unchanged and valid inputs decide exactly as before.
"""
from __future__ import annotations

import copy
import importlib.util
import json
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from core.analysis import portfolio_actions as pa  # noqa: E402
from tests.portfolio_reconciliation_fixtures import (  # noqa: E402
    build_certified_portfolio_actions, synthetic_quote_receipt)

PANEL = {"cash_available_sar": 50000, "target_cash_pct": 10,
         "max_position_pct": 40, "max_sector_pct": 60,
         "min_reliability_add": 70, "min_dq_add": 80,
         "rebalance_mode": "Advisory", "add_confirm_days": 1}
FX = {"USD": 4, "SAR": 1}
GUARD_ENVS = ("TFB_PF_ADD_POST_EARN", "TFB_PF_POST_EARN_SESSIONS",
              "TFB_PF_POST_EARN_SESSIONS_NEG", "TFB_PF_MAX_OPEN_TICKETS_MODE",
              "TFB_PF_MAX_OPEN_TICKETS")


@pytest.fixture(autouse=True)
def isolated_policy(monkeypatch):
    for key in GUARD_ENVS + ("TFB_PF_ADD_LOSER_VETO", "TFB_PF_ADD_LOSER_PCT",
                             "TFB_PF_ADD_STOP_PROX_PCT", "TFB_PF_DD_EXIT"):
        monkeypatch.delenv(key, raising=False)
    for key, value in {"TFB_PF_ENABLED": "1", "TFB_EXIT_BY_RULE_GATE": "0",
                       "TFB_PF_CONFIRM_PERSIST": "0", "TFB_PF_ADD_CONFIRM_DAYS": "1",
                       "TFB_PF_ENGINE_ROI_DISPLAY": "1", "TFB_FORECAST_BASIS": "observe",
                       "TFB_PF_CONFIRM_SESSION": "off"}.items():
        monkeypatch.setenv(key, value)
    saved = copy.deepcopy(pa._ADD_CONFIRM_STORE)
    pa._ADD_CONFIRM_STORE.clear()
    yield
    pa._ADD_CONFIRM_STORE.clear()
    pa._ADD_CONFIRM_STORE.update(saved)


def holding(symbol, *, price, quantity, avg_cost, sector, roi=20, sukuk=False,
            target_mult=2):
    return {**synthetic_quote_receipt(),
            "Symbol": symbol, "Name": "Synthetic Sukuk" if sukuk else "Synthetic Equity",
            "Currency": "USD", "Exchange": "NYSE/NASDAQ", "Sector": sector,
            "Asset Class": "Fixed Income / Sukuk" if sukuk else "Equity",
            "Position Qty": quantity, "Avg Cost": avg_cost, "Current Price": price,
            "Target Price": price * target_mult, "Expected ROI 12M": roi,
            "Forecast Reliability Score": 85, "Data Quality Score": 100,
            "Risk Bucket": "Low", "Investability Status": "INVESTABLE",
            "Recommendation": "BUY", "Volatility 30D": 0.2,
            "Forecast Source": "provider_target", "Buy Date": "2026-01-01"}


def riyadh_today():
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc) + timedelta(hours=3)).date()


def calendar(entries, status="ok", asof=None):
    return {"schema": "tfb.pd-post-earnings/v1", "status": status,
            "asof": asof or riyadh_today().isoformat(), "updated": "2026-10-27",
            "reports": 37, "sessions": 5, "sessions_neg": 10, "map": entries}


@pytest.fixture
def rows():
    # Both lots are in profit (P-183 never fires); both qualify for ADD.
    # EARN.US has the larger upside, so the funding order ranks it first.
    return [holding("EARN.US", price=50, quantity=20, avg_cost=45, sector="Energy", roi=25,
                    target_mult=3),
            holding("CALM.US", price=40, quantity=15, avg_cost=36, sector="Technology", roi=20)]


def build(rows, monkeypatch, *, post_earn=None, ticket=None, cap=None, sessions=None,
          sessions_neg=None, inputs="absent"):
    for key, value in {"TFB_PF_ADD_POST_EARN": post_earn,
                       "TFB_PF_MAX_OPEN_TICKETS_MODE": ticket,
                       "TFB_PF_MAX_OPEN_TICKETS": cap,
                       "TFB_PF_POST_EARN_SESSIONS": sessions,
                       "TFB_PF_POST_EARN_SESSIONS_NEG": sessions_neg}.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, str(value))
    pa._ADD_CONFIRM_STORE.clear()
    upstream = None if inputs == "absent" else {"post_earnings": copy.deepcopy(inputs)}
    result = build_certified_portfolio_actions(pa, copy.deepcopy(rows), PANEL, FX,
                                               upstream_meta=upstream)
    assert result["status"] == "ok" and result["meta"]["input_certification"]["funding_eligible"]
    result["meta"].pop("generated_utc", None)
    return result


def actions(payload):
    return {row["symbol"]: row for row in payload["actions"]}


def alerts(payload):
    return {alert["type"]: alert["count"] for alert in payload["alerts"]}


IN_WINDOW_NEG = {"EARN.US": {"date": "2026-10-22", "sessions": 4, "surprise": -22.2}}


# ----------------------------------------------------------------- readers
@pytest.mark.parametrize("value,expected", [(None, "off"), ("Enforce", "enforce"),
                                            (" observe ", "observe"), ("on", "off"),
                                            ("junk", "off")])
def test_mode_readers(monkeypatch, value, expected):
    for key in ("TFB_PF_ADD_POST_EARN", "TFB_PF_MAX_OPEN_TICKETS_MODE"):
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)
    assert pa._env_add_post_earn_mode() == expected
    assert pa._env_ticket_cap_mode() == expected
    assert tuple(map(int, pa.PORTFOLIO_ACTIONS_VERSION.split("."))) >= (1, 17, 0)


def test_number_readers(monkeypatch):
    assert (pa._env_post_earn_sessions(), pa._env_post_earn_sessions_neg(),
            pa._env_ticket_cap()) == (5, 10, 1)
    for key, raw, expected in (("TFB_PF_POST_EARN_SESSIONS", "3", 3),
                               ("TFB_PF_POST_EARN_SESSIONS", "99", 5),
                               ("TFB_PF_POST_EARN_SESSIONS", "14", 14),
                               ("TFB_PF_POST_EARN_SESSIONS", "15", 5),
                               ("TFB_PF_POST_EARN_SESSIONS_NEG", "7", 7),
                               ("TFB_PF_POST_EARN_SESSIONS_NEG", "-1", 10),
                               ("TFB_PF_POST_EARN_SESSIONS_NEG", "14", 14),
                               ("TFB_PF_POST_EARN_SESSIONS_NEG", "30", 10),
                               ("TFB_PF_MAX_OPEN_TICKETS", "2", 2),
                               ("TFB_PF_MAX_OPEN_TICKETS", "0", 1),
                               ("TFB_PF_MAX_OPEN_TICKETS", "x", 1)):
        monkeypatch.setenv(key, raw)
        reader = {"TFB_PF_POST_EARN_SESSIONS": pa._env_post_earn_sessions,
                  "TFB_PF_POST_EARN_SESSIONS_NEG": pa._env_post_earn_sessions_neg,
                  "TFB_PF_MAX_OPEN_TICKETS": pa._env_ticket_cap}[key]
        assert reader() == expected, (key, raw)
        monkeypatch.delenv(key)


def _weekdays_between(day, today, sun_thu=False):
    """Mirror of dt10WeekdaysBetween_: sessions after `day` up to `today`."""
    from datetime import timedelta
    n, d = 0, day + timedelta(days=1)
    while d <= today:
        wd = d.weekday()                      # Monday = 0 .. Sunday = 6
        n += (wd in (6, 0, 1, 2, 3)) if sun_thu else (wd <= 4)
        d += timedelta(days=1)
    return n


def test_session_cap_is_the_longest_window_the_calendar_can_prove():
    # Review fix (Codex P2, PR #869): Calendar_Events keeps a last report for
    # 21 calendar days; every report at most 14 sessions old is at most 20
    # days old, while a 15-session report can be 23 days old (already gone).
    from datetime import timedelta
    assert pa._POST_EARN_MAX_SESSIONS == 14
    for sun_thu in (False, True):
        oldest = {}
        for start in range(7):
            day = date(2026, 10, 5) + timedelta(days=start)
            for gap in range(0, 40):
                s = _weekdays_between(day, day + timedelta(days=gap), sun_thu)
                oldest[s] = max(oldest.get(s, 0), gap)
        assert oldest[14] == 20 and oldest[15] == 23, (sun_thu, oldest[14], oldest[15])
        assert max(oldest[s] for s in range(15)) <= 21


# ----------------------------------------------------------------- inputs
def test_inputs_validation():
    today = date(2026, 10, 28)
    assert pa._post_earn_inputs(None, today)["status"] == "absent"
    assert pa._post_earn_inputs({"schema": "other"}, today)["status"] == "invalid"
    assert pa._post_earn_inputs([], today)["status"] == "invalid"
    for status in ("unavailable", "stale"):
        assert pa._post_earn_inputs(calendar({}, status, "2026-10-28"), today)["status"] == status
    assert pa._post_earn_inputs(calendar({}, "weird", "2026-10-28"), today)["status"] == "invalid"
    assert pa._post_earn_inputs(calendar({}, "ok", "2026-10-25"), today)["status"] == "invalid"
    assert pa._post_earn_inputs(calendar({}, "ok", "28/10/2026"), today)["status"] == "invalid"
    assert pa._post_earn_inputs(calendar({}, "ok", "2026-10-27"), today)["status"] == "ok"
    big = {("S%d.US" % i): {"date": "2026-10-22", "sessions": 1, "surprise": None}
           for i in range(201)}
    assert pa._post_earn_inputs(calendar(big, "ok", "2026-10-28"), today)["status"] == "invalid"
    mixed = {" earn.us ": {"date": "2026-10-22", "sessions": 4, "surprise": -22.2},
             "BADDATE.US": {"date": "22/10/2026", "sessions": 4, "surprise": 1},
             "NEGSESS.US": {"date": "2026-10-22", "sessions": -1, "surprise": 1},
             "FRACT.US": {"date": "2026-10-22", "sessions": 1.5, "surprise": 1},
             "BOOL.US": {"date": "2026-10-22", "sessions": True, "surprise": 1},
             "NOSUR.US": {"date": "2026-10-20", "sessions": 6.0, "surprise": "n/a"},
             "LIST.US": ["2026-10-22", 4]}
    got = pa._post_earn_inputs(calendar(mixed, "ok", "2026-10-28"), today)
    assert got["status"] == "ok" and got["reports"] == 37 and got["updated"] == "2026-10-27"
    # v1.17.1: v1.17.0 dropped the six malformed entries (FRST-class defect);
    # they are now unknown evidence naming the rejected field. A float
    # session count (6.0) is not coerced to 6.
    assert got["map"] == {"EARN.US": {"date": "2026-10-22", "sessions": 4, "surprise": -22.2}}
    assert got["unknown"] == {
        "BADDATE.US": "earnings entry for BADDATE.US invalid (date '22/10/2026')",
        "NEGSESS.US": "earnings entry for NEGSESS.US invalid (sessions -1)",
        "FRACT.US": "earnings entry for FRACT.US invalid (sessions 1.5)",
        "BOOL.US": "earnings entry for BOOL.US invalid (sessions True)",
        "NOSUR.US": "earnings entry for NOSUR.US invalid (sessions 6.0)",
        "LIST.US": "earnings entry for LIST.US invalid (entry ['2026-10-22', 4])"}
    assert got["uncovered"] is None


@pytest.mark.parametrize("entry,expected", [
    ({"date": "2026-10-22", "sessions": 4, "surprise": -22.2}, (True, 10)),
    ({"date": "2026-10-22", "sessions": 10, "surprise": -1.0}, (True, 10)),
    ({"date": "2026-10-22", "sessions": 11, "surprise": -1.0}, (False, 10)),
    ({"date": "2026-10-22", "sessions": 5, "surprise": 3.0}, (True, 5)),
    ({"date": "2026-10-22", "sessions": 6, "surprise": 3.0}, (False, 5)),
    # v1.17.1: an unknown surprise cannot rule out a miss -> the longer window
    # (v1.17.0 read None as a beat: (False, 5)).
    ({"date": "2026-10-22", "sessions": 6, "surprise": None}, (True, 10)),
    ({"date": "2026-10-10", "sessions": 11, "surprise": None}, (False, 10)),
    ({"date": "2026-10-22", "sessions": 6, "surprise": None, "surprise_bad": "surprise 'n/a'"},
     (True, 10)),
    ({"date": "2026-10-28", "sessions": 0, "surprise": 0.0}, (True, 5)),
])
def test_window_eval(entry, expected):
    pe = {"status": "ok", "map": {"EARN.US": entry}}
    ev = pa._post_earn_eval({"symbol": "EARN.US"}, pe, 5, 10)
    assert (ev["in_window"], ev["limit"]) == expected
    assert pa._post_earn_eval({"symbol": "OTHER.US"}, pe, 5, 10)["in_window"] is False


def test_report_day_is_inside_every_window(rows, monkeypatch):
    # Review fix (Codex P1, PR #869): 10_My_Portfolio sends a report dated
    # today at 0 sessions; the guard holds the ADD on the report day itself.
    today = riyadh_today().isoformat()
    ev = pa._post_earn_eval({"symbol": "EARN.US"},
                            {"status": "ok", "map": {"EARN.US": {"date": today, "sessions": 0,
                                                             "surprise": None}}}, 5, 10)
    # v1.17.1: an unknown surprise takes the longer window (v1.17.0: 0/5, EPS n/a).
    assert ev["in_window"] and ev["trig"] == (
        "report day %s (0/10 sessions, EPS unknown, the longer window applies)" % today)
    for surprise, limit in ((None, 10), (-5.0, 10), (2.0, 5)):
        entry = {"EARN.US": {"date": today, "sessions": 0, "surprise": surprise}}
        enf = build(rows, monkeypatch, post_earn="enforce", inputs=calendar(entry))
        held = actions(enf)["EARN.US"]
        eps = ("unknown, the longer window applies" if surprise is None
               else "%+.1f%%" % surprise)
        assert held["action"] == "HOLD" and held["action_reason"].startswith(
            "ADD held [POST-EARN]: report day %s (0/%d sessions, EPS %s)" % (today, limit, eps))
        assert actions(enf)["CALM.US"]["action"] == "ADD"


# ----------------------------------------------------------------- off
def test_off_is_identical_with_or_without_inputs(rows, monkeypatch):
    plain = build(rows, monkeypatch)
    sent = build(rows, monkeypatch, inputs=calendar(IN_WINDOW_NEG))
    assert sent == plain
    assert "post_earnings" not in json.dumps(sent["meta"]["upstream"])
    text = json.dumps(sent).lower()
    for marker in ("postearn", "ticketcap", "post_earn_guard", "ticket_cap", "post-earn"):
        assert marker not in text
    assert all(row["action"] == "ADD" for row in plain["actions"])


# The untouched v1.16.1 module (main at 22658e0f, 28 Oct 2026). CI fetches
# this commit by SHA; without it the off-identity case SKIPs.
BASE_SHA = "22658e0f10ed0264feaf40e7208dc1f85b6a6e8b"


def _load_base_module():
    try:
        src = subprocess.run(["git", "show", BASE_SHA + ":core/analysis/portfolio_actions.py"],
                             cwd=ROOT, capture_output=True, text=True, check=True).stdout
    except Exception:  # noqa: BLE001 - the base commit is not in this checkout
        return None
    if 'PORTFOLIO_ACTIONS_VERSION = "1.16.1"' not in src:
        return None
    path = ROOT / "tests" / "_pa_base_v1161_tmp.py"
    path.write_text(src, encoding="utf-8")
    try:
        spec = importlib.util.spec_from_file_location("_pa_base_v1161", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    finally:
        path.unlink()


def test_off_is_byte_identical_to_v1161_base(rows, monkeypatch):
    base = _load_base_module()
    if base is None:
        pytest.skip("v1.16.1 base commit %s not in this checkout" % BASE_SHA[:8])
    pa._ADD_CONFIRM_STORE.clear()
    base._ADD_CONFIRM_STORE.clear()
    new = build_certified_portfolio_actions(pa, copy.deepcopy(rows), PANEL, FX,
                                            upstream_meta={"post_earnings": calendar(IN_WINDOW_NEG)})
    old = build_certified_portfolio_actions(base, copy.deepcopy(rows), PANEL, FX)
    for payload in (new, old):
        payload["meta"].pop("generated_utc", None)
    old_text = json.dumps(old, sort_keys=True).replace('"1.16.1"', '"<v>"')
    # v1.17.1: mask the live version (stacked pin; was the literal "1.17.0").
    new_text = json.dumps(new, sort_keys=True).replace(
        '"%s"' % pa.PORTFOLIO_ACTIONS_VERSION, '"<v>"')
    assert new_text == old_text


# ----------------------------------------------------------------- D1b
def test_post_earn_observe_tags_without_changing_anything(rows, monkeypatch):
    off = build(rows, monkeypatch, inputs=calendar(IN_WINDOW_NEG))
    obs = build(rows, monkeypatch, post_earn="observe", inputs=calendar(IN_WINDOW_NEG))
    assert obs["kpis"] == off["kpis"]
    old, new = actions(off), actions(obs)
    for symbol in old:
        row = new[symbol]
        assert row["action"] == "ADD"
        assert row["action_reason"].count("[postearn-observe]") == 1
        assert row["advisor_note"].count("[postearn-observe]") == 1
        stripped = dict(row)
        for key in ("action_reason", "advisor_note"):
            stripped[key] = old[symbol][key]
        assert stripped == old[symbol]
    assert ("reported 2026-10-22, 4/10 sessions since the report (EPS -22.2%) - would HOLD"
            in new["EARN.US"]["action_reason"])
    assert "[postearn-observe] ok;" in new["CALM.US"]["action_reason"]
    assert alerts(obs)["add_post_earn_observe"] == 1
    assert "add_post_earn_hold" not in alerts(obs)
    guard = obs["meta"]["post_earn_guard"]
    assert guard == {"mode": "observe", "sessions": 5, "sessions_neg": 10,
                     "input_status": "ok", "asof": riyadh_today().isoformat(),
                     "calendar_updated": "2026-10-27", "calendar_reports": 37,
                     "holdings_with_report": 1,
                     "invalid_entries": 0, "uncovered": None,        # v1.17.1
                     "held": 0, "would_hold": 1}
    assert "post_earnings" not in obs["meta"]["upstream"]


def test_post_earn_enforce_holds_only_the_name_inside_its_window(rows, monkeypatch):
    off = build(rows, monkeypatch, inputs=calendar(IN_WINDOW_NEG))
    enf = build(rows, monkeypatch, post_earn="enforce", inputs=calendar(IN_WINDOW_NEG))
    old, new = actions(off), actions(enf)
    held = new["EARN.US"]
    assert held["action"] == "HOLD" and held["detail"]["capped_from"] == "ADD"
    assert held["suggested_delta_sar"] in (None, 0) and held["suggested_delta_shares"] in (None, 0)
    assert held["action_reason"].startswith(
        "ADD held [POST-EARN]: reported 2026-10-22, 4/10 sessions since the report (EPS -22.2%)")
    assert "ADD held [POST-EARN]" in held["advisor_note"]
    assert new["CALM.US"]["action"] == "ADD"
    assert enf["kpis"]["adds_funded_sar"] < off["kpis"]["adds_funded_sar"]
    assert enf["kpis"]["action_counts"]["ADD"] == off["kpis"]["action_counts"]["ADD"] - 1
    assert alerts(enf)["add_post_earn_hold"] == 1
    assert alerts(enf).get("low_confidence_capped") == alerts(off).get("low_confidence_capped")
    assert enf["meta"]["post_earn_guard"]["held"] == 1
    assert old["CALM.US"]["suggested_delta_sar"] <= new["CALM.US"]["suggested_delta_sar"]


@pytest.mark.parametrize("entry,expected", [
    ({"date": "2026-10-20", "sessions": 6, "surprise": 4.0}, "ADD"),     # past 5 sessions
    ({"date": "2026-10-22", "sessions": 5, "surprise": 4.0}, "HOLD"),    # inside 5
    ({"date": "2026-10-10", "sessions": 11, "surprise": -9.0}, "ADD"),   # past 10
    ({"date": "2026-10-14", "sessions": 10, "surprise": -9.0}, "HOLD"),  # inside 10
])
def test_post_earn_session_limits(rows, monkeypatch, entry, expected):
    enf = build(rows, monkeypatch, post_earn="enforce", inputs=calendar({"EARN.US": entry}))
    assert actions(enf)["EARN.US"]["action"] == expected


def test_post_earn_session_limits_follow_the_envs(rows, monkeypatch):
    entry = {"EARN.US": {"date": "2026-10-22", "sessions": 4, "surprise": -22.2}}
    enf = build(rows, monkeypatch, post_earn="enforce", sessions_neg=3, inputs=calendar(entry))
    assert actions(enf)["EARN.US"]["action"] == "ADD"
    enf = build(rows, monkeypatch, post_earn="enforce", sessions_neg=4, inputs=calendar(entry))
    assert actions(enf)["EARN.US"]["action"] == "HOLD"


@pytest.mark.parametrize("inputs,word", [
    ("absent", "absent"),
    (calendar({}, "unavailable"), "unavailable"),
    (calendar({}, "stale"), "stale"),
    ({"schema": "tfb.pd-post-earnings/v0"}, "invalid"),
    (calendar(IN_WINDOW_NEG, "ok", "2020-01-01"), "invalid"),
])
def test_post_earn_unverifiable_inputs_hold_under_enforce_only(rows, monkeypatch, inputs, word):
    enf = build(rows, monkeypatch, post_earn="enforce", inputs=inputs)
    for row in enf["actions"]:
        assert row["action"] == "HOLD" and row["detail"]["capped_from"] == "ADD"
        assert row["action_reason"].startswith("ADD held [POST-EARN]: earnings calendar %s" % word)
    assert enf["meta"]["post_earn_guard"]["input_status"] == word
    assert enf["kpis"]["adds_funded_sar"] == 0
    obs = build(rows, monkeypatch, post_earn="observe", inputs=inputs)
    assert all(row["action"] == "ADD" for row in obs["actions"])
    assert alerts(obs)["add_post_earn_observe"] == 2


def test_post_earn_sukuk_exempt_and_non_add_untouched(monkeypatch):
    bond = [holding("SYNTHBOND.US", price=40, quantity=10, avg_cost=36, sector="Sukuk", sukuk=True)]
    entry = {"SYNTHBOND.US": {"date": "2026-10-27", "sessions": 1, "surprise": -5.0}}
    off = actions(build(bond, monkeypatch))["SYNTHBOND.US"]
    enf = actions(build(bond, monkeypatch, post_earn="enforce", inputs=calendar(entry)))["SYNTHBOND.US"]
    assert off["action"] == "ADD" and enf == off
    monkeypatch.setenv("TFB_PF_ADD_POST_EARN", "enforce")
    assert pa._apply_add_post_earn({"symbol": "EARN.US"}, pa.ACTION_HOLD, "HOLD", 0, None,
                                   {"status": "absent", "map": {}}) == (pa.ACTION_HOLD, "HOLD", 0, None)


# ----------------------------------------------------------------- D1c
def test_ticket_cap_holds_every_add_when_the_book_is_at_the_cap(monkeypatch):
    one = [holding("FIRST.US", price=50, quantity=20, avg_cost=45, sector="Energy")]
    off = build(one, monkeypatch)
    enf = build(one, monkeypatch, ticket="enforce")
    row = actions(enf)["FIRST.US"]
    assert actions(off)["FIRST.US"]["action"] == "ADD"
    assert row["action"] == "HOLD" and row["detail"]["capped_from"] == "ADD"
    assert row["action_reason"].startswith(
        "ADD held [TICKET-CAP]: 1 open ticket at the cap of 1 - no new ticket until one closes")
    assert enf["kpis"]["adds_funded_sar"] == 0
    assert alerts(enf)["add_ticket_cap_hold"] == 1
    assert alerts(enf).get("low_confidence_capped") == alerts(off).get("low_confidence_capped")
    assert enf["meta"]["ticket_cap"] == {"mode": "enforce", "cap": 1, "open_tickets": 1,
                                         "slots": 0, "held": 1, "would_hold": 0}


def test_ticket_cap_gives_free_slots_in_funding_order(rows, monkeypatch):
    enf = build(rows, monkeypatch, ticket="enforce", cap=3)
    got = actions(enf)
    assert got["EARN.US"]["action"] == "ADD"          # larger upside ranks first
    assert got["CALM.US"]["action"] == "HOLD"
    assert got["CALM.US"]["action_reason"].startswith(
        "ADD held [TICKET-CAP]: 2 open tickets and 1 higher-ranked ADD fill the cap of 3")
    assert enf["meta"]["ticket_cap"]["slots"] == 1
    roomy = build(rows, monkeypatch, ticket="enforce", cap=4)
    assert all(row["action"] == "ADD" for row in roomy["actions"])
    assert "add_ticket_cap_hold" not in alerts(roomy)


def test_ticket_cap_observe_and_sukuk_not_counted(rows, monkeypatch):
    book = rows + [holding("SYNTHBOND.US", price=40, quantity=10, avg_cost=36,
                           sector="Sukuk", sukuk=True)]
    off = build(book, monkeypatch)
    obs = build(book, monkeypatch, ticket="observe", cap=3)
    assert obs["kpis"] == off["kpis"]
    got = actions(obs)
    assert "[ticketcap-observe] ok (2 open, cap 3)" in got["EARN.US"]["action_reason"]
    assert "would HOLD under enforce" in got["CALM.US"]["action_reason"]
    assert "[ticketcap-observe]" not in got["SYNTHBOND.US"]["action_reason"]
    assert obs["meta"]["ticket_cap"] == {"mode": "observe", "cap": 3, "open_tickets": 2,
                                         "slots": 1, "held": 0, "would_hold": 1}
    assert alerts(obs)["add_ticket_cap_observe"] == 1


def test_post_earn_hold_frees_the_slot_before_the_ticket_cap(rows, monkeypatch):
    enf = build(rows, monkeypatch, post_earn="enforce", ticket="enforce", cap=3,
                inputs=calendar(IN_WINDOW_NEG))
    got = actions(enf)
    assert got["EARN.US"]["action_reason"].startswith("ADD held [POST-EARN]")
    assert got["CALM.US"]["action"] == "ADD"
    assert enf["meta"]["ticket_cap"]["held"] == 0
    assert alerts(enf)["add_post_earn_hold"] == 1 and "add_ticket_cap_hold" not in alerts(enf)


def test_repeat_runs_are_deterministic(rows, monkeypatch):
    first = build(rows, monkeypatch, post_earn="enforce", ticket="observe",
                  inputs=calendar(IN_WINDOW_NEG))
    second = build(rows, monkeypatch, post_earn="enforce", ticket="observe",
                   inputs=calendar(IN_WINDOW_NEG))
    assert first == second


# ----------------------------------------------------------------- v1.17.1
# Owner readiness review (28 Oct, priority 3): numeric "sessions": 4 held the
# ADD, the same entry with "sessions": "4" was dropped and the ADD passed.
TODAY = date(2026, 10, 28)
# 2026-10-22 in Arabic-Indic digits (str.isdigit() accepts them; the guard must not).
ARABIC_INDIC_DATE = "-".join("".join(chr(0x660 + int(d)) for d in part)
                             for part in ("2026", "10", "22"))


def _entry(**over):
    base = {"date": "2026-10-22", "sessions": 4, "surprise": -22.2}
    base.update(over)
    return base


def _without(key):
    base = _entry()
    base.pop(key)
    return base


@pytest.mark.parametrize("entry,field", [
    (_entry(sessions="4"), "sessions '4'"),
    (_entry(sessions=True), "sessions True"),
    (_entry(sessions=False), "sessions False"),
    (_entry(sessions=4.0), "sessions 4.0"),
    (_entry(sessions=4.5), "sessions 4.5"),
    (_entry(sessions=None), "sessions None"),
    (_entry(sessions=float("nan")), "sessions nan"),
    (_entry(sessions=float("inf")), "sessions inf"),
    (_entry(sessions=-1), "sessions -1"),
    (_entry(sessions=""), "sessions ''"),
    (_entry(sessions=[4]), "sessions [4]"),
    (_without("sessions"), "sessions missing"),
    (_without("date"), "date missing"),
    (_entry(date=None), "date None"),
    (_entry(date=20261022), "date 20261022"),
    (_entry(date="22/10/2026"), "date '22/10/2026'"),
    (_entry(date="2026-1-5"), "date '2026-1-5'"),
    (_entry(date="2026-10-32"), "date '2026-10-32'"),
    (_entry(date=ARABIC_INDIC_DATE), "date " + ascii(ARABIC_INDIC_DATE)[:21] + "..."),
    (_entry(date="2026-10-29", sessions=0), "date 2026-10-29 after asof 2026-10-28"),
    (_entry(date="2026-10-27", sessions=4),
     "sessions 4 above the 1 calendar day since 2026-10-27"),
    (_entry(date="2026-10-28", sessions=1),
     "sessions 1 above the 0 calendar days since 2026-10-28"),
    ("2026-10-22", "entry '2026-10-22'"),
    (None, "entry None"),
    (4, "entry 4"),
])
def test_malformed_entry_is_unknown_never_dropped(entry, field):
    raw = calendar({" frst.us ": entry, "CALM.US": _entry(sessions=6, surprise=2.0)},
                   "ok", "2026-10-28")
    got = pa._post_earn_inputs(raw, TODAY)
    assert got["status"] == "ok"
    assert got["unknown"] == {"FRST.US": "earnings entry for FRST.US invalid (%s)" % field}
    assert "FRST.US" not in got["map"] and got["map"]["CALM.US"]["sessions"] == 6
    assert all(ord(ch) < 128 for ch in got["unknown"]["FRST.US"])


@pytest.mark.parametrize("entry,expected", [
    (_entry(), {"date": "2026-10-22", "sessions": 4, "surprise": -22.2}),
    (_entry(date=" 2026-10-22 "), {"date": "2026-10-22", "sessions": 4, "surprise": -22.2}),
    (_entry(surprise=3), {"date": "2026-10-22", "sessions": 4, "surprise": 3.0}),
    (_entry(surprise=None), {"date": "2026-10-22", "sessions": 4, "surprise": None}),
    (_without("surprise"), {"date": "2026-10-22", "sessions": 4, "surprise": None}),
    (_entry(surprise="n/a"), {"date": "2026-10-22", "sessions": 4, "surprise": None,
                              "surprise_bad": "surprise 'n/a'"}),
    (_entry(surprise=True), {"date": "2026-10-22", "sessions": 4, "surprise": None,
                             "surprise_bad": "surprise True"}),
    (_entry(surprise=float("nan")), {"date": "2026-10-22", "sessions": 4, "surprise": None,
                                     "surprise_bad": "surprise nan"}),
    (_entry(date="2026-10-28", sessions=0, surprise=None),
     {"date": "2026-10-28", "sessions": 0, "surprise": None}),
    (_entry(date="2026-09-01", sessions=40, surprise=1.0),
     {"date": "2026-09-01", "sessions": 40, "surprise": 1.0}),
])
def test_well_formed_entries_keep_their_values(entry, expected):
    got = pa._post_earn_inputs(calendar({"FRST.US": entry}, "ok", "2026-10-28"), TODAY)
    assert got["status"] == "ok" and got["unknown"] == {}
    assert got["map"] == {"FRST.US": expected}


def test_unattributed_or_duplicate_entries():
    for key in ("", "   ", 7):
        got = pa._post_earn_inputs(calendar({key: _entry()}, "ok", "2026-10-28"), TODAY)
        assert got["status"] == "invalid" and got["map"] == {} and got["unknown"] == {}
    dup = pa._post_earn_inputs(calendar({"FRST.US": _entry(),
                                         " frst.us": _entry(date="2026-09-01", sessions=40)},
                                        "ok", "2026-10-28"), TODAY)
    assert dup["status"] == "ok" and dup["map"] == {}
    assert dup["unknown"] == {"FRST.US": "earnings entry for FRST.US invalid (duplicate symbol)"}


@pytest.mark.parametrize("listed,expected", [
    ([], set()),
    (["frst.us", " 2222.SR "], {"FRST.US", "2222.SR"}),
    (["X.US", "x.us"], {"X.US"}),
])
def test_uncovered_list_parsing(listed, expected):
    raw = calendar({}, "ok", "2026-10-28")
    raw["uncovered"] = listed
    got = pa._post_earn_inputs(raw, TODAY)
    assert got["status"] == "ok" and got["uncovered"] == expected
    assert pa._post_earn_inputs(calendar({}, "ok", "2026-10-28"), TODAY)["uncovered"] is None


@pytest.mark.parametrize("listed", ["FRST.US", None, {"FRST.US": 1}, [1], [""], [None], ["  "],
                                    ["S%d.US" % i for i in range(201)]])
def test_malformed_uncovered_makes_the_input_invalid(listed):
    raw = calendar({"CALM.US": _entry()}, "ok", "2026-10-28")
    raw["uncovered"] = listed
    got = pa._post_earn_inputs(raw, TODAY)
    assert got["status"] == "invalid" and got["map"] == {} and got["uncovered"] is None


def test_symbol_both_reported_and_uncovered_is_unknown():
    raw = calendar({"FRST.US": _entry(), "BAD.US": _entry(sessions="4")}, "ok", "2026-10-28")
    raw["uncovered"] = ["frst.us", "bad.us", "KSA.SR"]
    got = pa._post_earn_inputs(raw, TODAY)
    assert got["status"] == "ok" and got["map"] == {}
    assert got["unknown"] == {
        "FRST.US": "earnings entry for FRST.US invalid (also listed as uncovered)",
        "BAD.US": "earnings entry for BAD.US invalid (also listed as uncovered)"}
    assert got["uncovered"] == {"FRST.US", "BAD.US", "KSA.SR"}


def test_reproduction_string_sessions_holds_under_enforce(rows, monkeypatch):
    numeric = {"EARN.US": {"date": "2026-10-22", "sessions": 4, "surprise": -22.2}}
    text = {"EARN.US": {"date": "2026-10-22", "sessions": "4", "surprise": -22.2}}
    for entries, prefix in ((numeric, "ADD held [POST-EARN]: reported 2026-10-22, 4/10 "
                                      "sessions since the report (EPS -22.2%) - no adding"),
                            (text, "ADD held [POST-EARN]: earnings entry for EARN.US invalid "
                                   "(sessions '4') - the post-earnings window cannot be "
                                   "verified (was ADD: ")):
        enf = build(rows, monkeypatch, post_earn="enforce", inputs=calendar(entries))
        held = actions(enf)["EARN.US"]
        assert held["action"] == "HOLD" and held["detail"]["capped_from"] == "ADD"
        assert held["action_reason"].startswith(prefix)
        assert "ADD held [POST-EARN]" in held["advisor_note"]
        assert held["suggested_delta_sar"] in (None, 0)
        assert actions(enf)["CALM.US"]["action"] == "ADD"          # absent -> clean
        assert alerts(enf)["add_post_earn_hold"] == 1
    guard = enf["meta"]["post_earn_guard"]
    assert (guard["invalid_entries"], guard["holdings_with_report"], guard["held"]) == (1, 0, 1)
    off = build(rows, monkeypatch, inputs=calendar(text))
    assert off == build(rows, monkeypatch)                         # off ignores the field
    obs = build(rows, monkeypatch, post_earn="observe", inputs=calendar(text))
    assert all(row["action"] == "ADD" for row in obs["actions"])
    assert obs["kpis"] == off["kpis"]
    assert ("[postearn-observe] earnings entry for EARN.US invalid (sessions '4') - would HOLD "
            "under enforce" in actions(obs)["EARN.US"]["action_reason"])
    assert alerts(obs)["add_post_earn_observe"] == 1
    assert obs["meta"]["post_earn_guard"]["would_hold"] == 1


@pytest.mark.parametrize("entry,expected,eps", [
    ({"date": "2026-10-20", "sessions": 6, "surprise": None}, "HOLD",
     "EPS unknown, the longer window applies"),
    ({"date": "2026-10-20", "sessions": 6}, "HOLD", "EPS unknown, the longer window applies"),
    ({"date": "2026-10-20", "sessions": 6, "surprise": "n/a"}, "HOLD",
     "EPS invalid (surprise 'n/a'), the longer window applies"),
    ({"date": "2026-10-23", "sessions": 3, "surprise": None}, "HOLD",
     "EPS unknown, the longer window applies"),
    ({"date": "2026-10-10", "sessions": 11, "surprise": None}, "ADD", None),
    ({"date": "2026-10-10", "sessions": 11, "surprise": "n/a"}, "ADD", None),
    ({"date": "2026-10-20", "sessions": 6, "surprise": 2.5}, "ADD", None),
])
def test_unknown_surprise_takes_the_longer_window(rows, monkeypatch, entry, expected, eps):
    enf = build(rows, monkeypatch, post_earn="enforce", inputs=calendar({"EARN.US": entry}))
    row = actions(enf)["EARN.US"]
    assert row["action"] == expected
    if eps:
        assert row["action_reason"].startswith(
            "ADD held [POST-EARN]: reported %s, %d/10 sessions since the report (%s) - no "
            "adding inside the post-earnings window" % (entry["date"], entry["sessions"], eps))


def test_uncovered_holding_holds_under_enforce_only(rows, monkeypatch):
    raw = calendar({})
    raw["uncovered"] = ["earn.us"]
    enf = build(rows, monkeypatch, post_earn="enforce", inputs=raw)
    got = actions(enf)
    assert got["EARN.US"]["action"] == "HOLD" and got["EARN.US"]["detail"]["capped_from"] == "ADD"
    assert got["EARN.US"]["action_reason"].startswith(
        "ADD held [POST-EARN]: no earnings calendar row for EARN.US - the post-earnings window "
        "cannot be verified (was ADD: ")
    assert got["CALM.US"]["action"] == "ADD"
    assert enf["meta"]["post_earn_guard"]["uncovered"] == 1
    obs = build(rows, monkeypatch, post_earn="observe", inputs=raw)
    assert all(row["action"] == "ADD" for row in obs["actions"])
    assert ("[postearn-observe] no earnings calendar row for EARN.US - would HOLD under enforce"
            in actions(obs)["EARN.US"]["action_reason"])
    empty = calendar({})
    empty["uncovered"] = []
    clean = build(rows, monkeypatch, post_earn="enforce", inputs=empty)
    assert all(row["action"] == "ADD" for row in clean["actions"])
    assert clean["meta"]["post_earn_guard"]["uncovered"] == 0
    bad = calendar({})
    bad["uncovered"] = "EARN.US"
    inv = build(rows, monkeypatch, post_earn="enforce", inputs=bad)
    assert inv["meta"]["post_earn_guard"]["input_status"] == "invalid"
    for row in inv["actions"]:
        assert row["action"] == "HOLD"
        assert row["action_reason"].startswith("ADD held [POST-EARN]: earnings calendar invalid")


def test_sukuk_exempt_even_when_its_entry_is_invalid(monkeypatch):
    bond = [holding("SYNTHBOND.US", price=40, quantity=10, avg_cost=36, sector="Sukuk", sukuk=True)]
    raw = calendar({"SYNTHBOND.US": {"date": "2026-10-27", "sessions": "1"}})
    raw["uncovered"] = ["OTHER.US"]
    off = actions(build(bond, monkeypatch))["SYNTHBOND.US"]
    enf = actions(build(bond, monkeypatch, post_earn="enforce", inputs=raw))["SYNTHBOND.US"]
    assert off["action"] == "ADD" and enf == off


def test_invalid_entry_hold_frees_the_ticket_cap_slot(rows, monkeypatch):
    raw = calendar({"EARN.US": {"date": "2026-10-22", "sessions": "4", "surprise": -22.2}})
    enf = build(rows, monkeypatch, post_earn="enforce", ticket="enforce", cap=3, inputs=raw)
    got = actions(enf)
    assert got["EARN.US"]["action_reason"].startswith(
        "ADD held [POST-EARN]: earnings entry for EARN.US invalid (sessions '4')")
    assert got["CALM.US"]["action"] == "ADD"
    assert enf["meta"]["ticket_cap"]["held"] == 0


# The merged v1.17.0 module (main at 0c3d3e21, #869). CI fetches this commit by
# SHA; without it the two identity cases below SKIP.
BASE_1170_SHA = "0c3d3e21a523c0a275b0f9b9c33951d695afc4bb"


def _load_v1170_module():
    try:
        src = subprocess.run(["git", "show", BASE_1170_SHA + ":core/analysis/portfolio_actions.py"],
                             cwd=ROOT, capture_output=True, text=True, check=True).stdout
    except Exception:  # noqa: BLE001 - the base commit is not in this checkout
        return None
    if 'PORTFOLIO_ACTIONS_VERSION = "1.17.0"' not in src:
        return None
    path = ROOT / "tests" / "_pa_base_v1170_tmp.py"
    path.write_text(src, encoding="utf-8")
    try:
        spec = importlib.util.spec_from_file_location("_pa_base_v1170", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    finally:
        path.unlink()


def _masked(module, rows, inputs):
    module._ADD_CONFIRM_STORE.clear()
    upstream = None if inputs == "absent" else {"post_earnings": copy.deepcopy(inputs)}
    out = build_certified_portfolio_actions(module, copy.deepcopy(rows), PANEL, FX,
                                            upstream_meta=upstream)
    out["meta"].pop("generated_utc", None)
    guard = out["meta"].get("post_earn_guard")
    if guard is not None and module is pa:
        # The two v1.17.1 counters are the only new keys; benign here.
        assert guard.pop("invalid_entries") == 0 and guard.pop("uncovered") is None
    return json.dumps(out, sort_keys=True).replace(
        '"%s"' % module.PORTFOLIO_ACTIONS_VERSION, '"<v>"')


MALFORMED = calendar({"EARN.US": {"date": "2026-10-22", "sessions": "4", "surprise": -22.2},
                      "CALM.US": {"date": "2026-10-20", "sessions": 6, "surprise": None}})


def test_off_is_byte_identical_to_v1170_base(rows, monkeypatch):
    base = _load_v1170_module()
    if base is None:
        pytest.skip("v1.17.0 base commit %s not in this checkout" % BASE_1170_SHA[:8])
    for inputs in ("absent", calendar(IN_WINDOW_NEG), MALFORMED,
                   dict(MALFORMED, uncovered=["EARN.US"])):
        for ticket in (None, "enforce"):
            if ticket is None:
                monkeypatch.delenv("TFB_PF_MAX_OPEN_TICKETS_MODE", raising=False)
            else:
                monkeypatch.setenv("TFB_PF_MAX_OPEN_TICKETS_MODE", ticket)
            monkeypatch.delenv("TFB_PF_ADD_POST_EARN", raising=False)
            assert _masked(pa, rows, inputs) == _masked(base, rows, inputs)


@pytest.mark.parametrize("inputs", [
    calendar(IN_WINDOW_NEG),
    calendar({"EARN.US": {"date": "2026-10-20", "sessions": 6, "surprise": 4.0}}),
    calendar({"EARN.US": {"date": "2026-10-14", "sessions": 10, "surprise": -9.0}}),
    calendar({"EARN.US": {"date": riyadh_today().isoformat(), "sessions": 0, "surprise": 1.5}}),
    calendar({}),
    calendar({}, "stale"),
    calendar({}, "unavailable"),
    {"schema": "tfb.pd-post-earnings/v0"},
    "absent",
])
def test_valid_inputs_decide_exactly_as_v1170(rows, monkeypatch, inputs):
    base = _load_v1170_module()
    if base is None:
        pytest.skip("v1.17.0 base commit %s not in this checkout" % BASE_1170_SHA[:8])
    for mode in ("observe", "enforce"):
        monkeypatch.setenv("TFB_PF_ADD_POST_EARN", mode)
        monkeypatch.setenv("TFB_PF_MAX_OPEN_TICKETS_MODE", "observe")
        assert _masked(pa, rows, inputs) == _masked(base, rows, inputs), mode
