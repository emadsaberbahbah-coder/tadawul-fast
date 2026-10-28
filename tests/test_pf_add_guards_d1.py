#!/usr/bin/env python3
"""D1b post-earnings and D1c open-ticket ADD guards (portfolio_actions 1.17.0).

All holdings, broker positions, settled cash, FX and calendar inputs are
synthetic. The real certified builder supplies every off / observe / enforce
comparison; the v1.16.1 base module (loaded from git when available) proves
that off is byte-identical whether or not the new request field is sent.
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
                               ("TFB_PF_POST_EARN_SESSIONS_NEG", "7", 7),
                               ("TFB_PF_POST_EARN_SESSIONS_NEG", "-1", 10),
                               ("TFB_PF_MAX_OPEN_TICKETS", "2", 2),
                               ("TFB_PF_MAX_OPEN_TICKETS", "0", 1),
                               ("TFB_PF_MAX_OPEN_TICKETS", "x", 1)):
        monkeypatch.setenv(key, raw)
        reader = {"TFB_PF_POST_EARN_SESSIONS": pa._env_post_earn_sessions,
                  "TFB_PF_POST_EARN_SESSIONS_NEG": pa._env_post_earn_sessions_neg,
                  "TFB_PF_MAX_OPEN_TICKETS": pa._env_ticket_cap}[key]
        assert reader() == expected, (key, raw)
        monkeypatch.delenv(key)


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
    assert got["map"] == {"EARN.US": {"date": "2026-10-22", "sessions": 4, "surprise": -22.2},
                          "NOSUR.US": {"date": "2026-10-20", "sessions": 6, "surprise": None}}


@pytest.mark.parametrize("entry,expected", [
    ({"date": "2026-10-22", "sessions": 4, "surprise": -22.2}, (True, 10)),
    ({"date": "2026-10-22", "sessions": 10, "surprise": -1.0}, (True, 10)),
    ({"date": "2026-10-22", "sessions": 11, "surprise": -1.0}, (False, 10)),
    ({"date": "2026-10-22", "sessions": 5, "surprise": 3.0}, (True, 5)),
    ({"date": "2026-10-22", "sessions": 6, "surprise": 3.0}, (False, 5)),
    ({"date": "2026-10-22", "sessions": 6, "surprise": None}, (False, 5)),
    ({"date": "2026-10-28", "sessions": 0, "surprise": 0.0}, (True, 5)),
])
def test_window_eval(entry, expected):
    pe = {"status": "ok", "map": {"EARN.US": entry}}
    ev = pa._post_earn_eval({"symbol": "EARN.US"}, pe, 5, 10)
    assert (ev["in_window"], ev["limit"]) == expected
    assert pa._post_earn_eval({"symbol": "OTHER.US"}, pe, 5, 10)["in_window"] is False


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
    new_text = json.dumps(new, sort_keys=True).replace('"1.17.0"', '"<v>"')
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
                     "holdings_with_report": 1, "held": 0, "would_hold": 1}
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


# ----------------------------------------------------------------- route
def test_route_forwards_only_a_mapping(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from routes import advanced_analysis as advanced

    seen = []

    def fake_builder(holdings, controls=None, fx_rates=None, upstream_meta=None):
        seen.append(copy.deepcopy(upstream_meta))
        return {"version": "x", "status": "ok", "kpis": {}, "actions": [],
                "sector_summary": [], "alerts": [], "meta": {}}

    async def fake_health(*_args, **_kwargs):
        return {"unavailable": True, "reason": "synthetic-test"}

    monkeypatch.setattr(advanced, "_auth_passed", lambda **_kwargs: True)
    monkeypatch.setattr(advanced, "_portfolio_build_offloop_enabled", lambda: False)
    monkeypatch.setattr(advanced, "_provider_health_with_budget", fake_health)
    monkeypatch.setattr(advanced, "_build_portfolio_actions", fake_builder)
    assert tuple(map(int, advanced.ADVANCED_ANALYSIS_VERSION.split("."))) >= (4, 19, 0)
    app = FastAPI()
    app.include_router(advanced.router)
    body = {"rows": [{"Symbol": "EARN.US", "Position Qty": 1}], "controls": {}, "fx_rates": {}}
    with TestClient(app) as client:
        for extra in ({}, {"post_earnings": calendar(IN_WINDOW_NEG)},
                      {"post_earnings": ["not", "a", "mapping"]}, {"post_earnings": None}):
            response = client.post("/sheet-rows/portfolio-actions", json={**body, **extra})
            assert response.status_code == 200, response.text
    assert len(seen) == 4
    assert "post_earnings" not in seen[0]
    assert seen[1]["post_earnings"] == calendar(IN_WINDOW_NEG)
    assert "post_earnings" not in seen[2] and "post_earnings" not in seen[3]
    for meta in seen:
        assert "reconciliation_evidence" in meta and "versions" in meta
