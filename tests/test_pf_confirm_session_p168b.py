"""tests/test_pf_confirm_session_p168b.py — portfolio_actions v1.13.0 [P-168b]

A confirmation day is a COMPLETED TRADING SESSION, not a calendar date.
v1.12.2 keys the ADD-confirmation chain on the UTC date, so a Friday-close
signal is counted on Sunday and again on Monday before the US open ("ADD
confirmed (day 2/2)" on zero new sessions — DDI.US 2026-09-27/28). v1.13.0
adds TFB_PF_CONFIRM_SESSION = off | observe | enforce (default off =
byte-identical).

T1  off: legacy strings verbatim, no session suffix, no meta echo
T2  golden negative (off == v1.12.2): Sun 06:00Z + Mon 03:40Z -> confirmed 2/2
T3  enforce: Sun / Mon pre-open / Mon 20:40Z all frozen at 1/2 keyed on the
    Friday session; Tue 00:40Z -> confirmed 2/2 keyed 2026-09-28 (FAILS on
    a v1.12.2 tree: the env has no effect there — discriminating test)
T4  enforce: skipped session restarts; non-ADD clears the clock
T5  calendar: venue map, US/KSA keys and previous sessions, NYSE holiday,
    Tadawul National Day, TFB_PF_SESSION_HOLIDAYS csv
T6  observe: verdict unchanged, ONE tag with venue/session/legacy/session
    counts, FLIP on disagreement; raw non-ADD clears the shadow chain and
    prints nothing; an injected calendar fault exposes unavailable evidence
T7  enforce: calendar fault -> fail-closed HOLD, clock untouched + WARNING
T8  enforce: the P-165 fail-closed contract on the store path still holds
T9  integration (opt-in, TFB_TEST_MP_TSV = a My_Portfolio export): observe
    tags exactly the raw-ADD rows at BOTH payload sites with verdicts equal
    to off; enforce on Monday pre-open holds DDI pending (adds_funded 0),
    Tuesday 00:40Z funds it.
"""
from __future__ import annotations

import csv
import copy
import logging
import os
import re
from datetime import datetime, timedelta, timezone

import pytest

import core.analysis.portfolio_actions as pa

ADD, HOLD = pa.ACTION_ADD, pa.ACTION_HOLD
CTL = {"add_confirm_days": 2}
_OFFLINE_CONFIRM_CACHE = {}


@pytest.fixture(autouse=True)
def _offline_confirm_cache(monkeypatch):
    """Exercise persistence without ever loading a configured Redis client."""
    _OFFLINE_CONFIRM_CACHE.clear()
    traffic = []
    def get(key):
        traffic.append(("get", key))
        return copy.deepcopy(_OFFLINE_CONFIRM_CACHE.get(key))
    def put(key, value):
        traffic.append(("put", key))
        _OFFLINE_CONFIRM_CACHE[key] = copy.deepcopy(value)
    def delete(key):
        traffic.append(("delete", key))
        _OFFLINE_CONFIRM_CACHE.pop(key, None)
    def forbidden_client():
        raise AssertionError("session tests must not load a real Redis client")
    monkeypatch.setattr(pa, "_confirm_redis", forbidden_client)
    monkeypatch.setattr(pa, "_confirm_redis_get", get)
    monkeypatch.setattr(pa, "_confirm_redis_put", put)
    monkeypatch.setattr(pa, "_confirm_redis_del", delete)
    yield {"values": _OFFLINE_CONFIRM_CACHE, "traffic": traffic}
    _OFFLINE_CONFIRM_CACHE.clear()


def T(*a):
    return datetime(*a, tzinfo=timezone.utc)


class _Clock:
    def __init__(self, when):
        self.when, self.orig = when, pa.datetime

    def __enter__(self):
        when = self.when

        class FD(self.orig):
            @classmethod
            def now(cls, tz=None):
                return when.astimezone(tz) if tz else when.replace(tzinfo=None)
        pa.datetime = FD
        return self

    def __exit__(self, *a):
        pa.datetime = self.orig


def _reset(monkeypatch, mode=None, persist="1"):
    _OFFLINE_CONFIRM_CACHE.clear()
    monkeypatch.delenv("TFB_PF_CONFIRM_SESSION", raising=False)
    monkeypatch.delenv("TFB_PF_SESSION_HOLIDAYS", raising=False)
    monkeypatch.setenv("TFB_PF_CONFIRM_PERSIST", persist)
    monkeypatch.setenv("TFB_PF_ADD_CONFIRM_LEGACY_FAILOPEN", "0")
    if mode:
        monkeypatch.setenv("TFB_PF_CONFIRM_SESSION", mode)
    pa._ADD_CONFIRM_STORE.clear()
    if hasattr(pa, "_ADD_CONFIRM_SESSION_STORE"):
        pa._ADD_CONFIRM_SESSION_STORE.clear()


def _raise(*_a, **_k):
    raise RuntimeError("injected")


# --------------------------------------------------------------------------
def test_t1_off_is_legacy_verbatim(monkeypatch):
    _reset(monkeypatch)
    with _Clock(T(2026, 9, 28, 3, 40)):
        d1 = pa._apply_add_confirmation("DDI.US", ADD, "q", None, CTL)
        pa._ADD_CONFIRM_STORE["DDI.US"] = {"count": 1, "date": "2026-09-27"}
        d2 = pa._apply_add_confirmation("DDI.US", ADD, "q", None, CTL)
    assert d1 == (HOLD, "ADD pending confirmation (day 1/2) — signal must persist "
                        "2 consecutive days before funding; qualifying: q", ADD)
    assert d2 == (ADD, "q — ADD confirmed (day 2/2)", None)
    assert "session" not in d1[1] and "session" not in d2[1]
    assert pa._env_confirm_session_mode() == "off"


def test_t2_golden_negative_calendar_clock(monkeypatch):
    _reset(monkeypatch)
    with _Clock(T(2026, 9, 27, 6, 0)):                  # Sunday, no session
        s = pa._apply_add_confirmation("DDI.US", ADD, "q", None, CTL)
    with _Clock(T(2026, 9, 28, 3, 40)):                 # Monday BEFORE the open
        m = pa._apply_add_confirmation("DDI.US", ADD, "q", None, CTL)
    assert s[0] == HOLD and m[0] == ADD and "ADD confirmed (day 2/2)" in m[1]   # the defect


def test_t3_enforce_session_sequence(monkeypatch, _offline_confirm_cache):
    _reset(monkeypatch, "enforce")
    with _Clock(T(2026, 9, 27, 6, 0)):
        sun = pa._apply_add_confirmation("DDI.US", ADD, "q", None, CTL)
    with _Clock(T(2026, 9, 28, 3, 40)):
        mon_pre = pa._apply_add_confirmation("DDI.US", ADD, "q", None, CTL)
    with _Clock(T(2026, 9, 28, 20, 40)):
        mon_late = pa._apply_add_confirmation("DDI.US", ADD, "q", None, CTL)
    with _Clock(T(2026, 9, 29, 0, 40)):
        tue = pa._apply_add_confirmation("DDI.US", ADD, "q", None, CTL)
    with _Clock(T(2026, 9, 30, 0, 40)):
        wed = pa._apply_add_confirmation("DDI.US", ADD, "q", None, CTL)
    assert sun[0] == HOLD and sun[2] == ADD
    assert "(day 1/2; session 2026-09-25)" in sun[1]
    assert mon_pre == sun and mon_late == sun            # frozen: no completed session yet
    assert tue[0] == ADD and "ADD confirmed (day 2/2; session 2026-09-28)" in tue[1]
    assert wed[0] == ADD and "(day 3/2; session 2026-09-29)" in wed[1]
    assert pa._ADD_CONFIRM_STORE["DDI.US"] == {"count": 3, "date": "2026-09-29"}
    assert _offline_confirm_cache["values"]["DDI.US"] == pa._ADD_CONFIRM_STORE["DDI.US"]


def test_t4_enforce_restart_and_clear(monkeypatch):
    _reset(monkeypatch, "enforce")
    pa._ADD_CONFIRM_STORE["DDI.US"] = {"count": 1, "date": "2026-09-25"}
    with _Clock(T(2026, 9, 30, 0, 40)):                 # 09-28 session skipped
        g = pa._apply_add_confirmation("DDI.US", ADD, "q", None, CTL)
    assert g[0] == HOLD and "(day 1/2; session 2026-09-29)" in g[1]
    pa._apply_add_confirmation("DDI.US", HOLD, "h", None, CTL)
    assert "DDI.US" not in pa._ADD_CONFIRM_STORE


def test_t5_calendar(monkeypatch):
    _reset(monkeypatch)
    v = pa._confirm_venue
    assert (v("1050.SR"), v("YUM"), v("DDI.US"), v("BBOX.L"), v("7203.T"),
            v("QNBK.QA"), v("ORBIA.MX")) == \
           ("KSA", "US", "US", "EU", "ASIA", "GULF", "AMER")
    with pytest.raises(pa.ConfirmCalendarUnavailable, match="unknown venue suffix"):
        v("ZZZ.UNKNOWN")
    k = pa._confirm_session_key
    assert k("US", T(2026, 9, 27, 6, 0)) == "2026-09-25"
    assert k("US", T(2026, 9, 28, 20, 59)) == "2026-09-25"
    assert k("US", T(2026, 9, 28, 21, 0)) == "2026-09-28"
    assert k("US", T(2026, 9, 8, 3, 0)) == "2026-09-04"           # Labor Day 09-07 skipped
    assert pa._confirm_prev_session("US", "2026-09-08") == "2026-09-04"
    assert pa._confirm_prev_session("US", "2026-09-28") == "2026-09-25"
    assert k("KSA", T(2026, 9, 25, 10, 0)) == "2026-09-24"        # Friday -> Thursday
    assert k("KSA", T(2026, 9, 27, 13, 0)) == "2026-09-27"        # Sunday after 12:00Z
    assert k("KSA", T(2026, 9, 24, 6, 0)) == "2026-09-22"         # National Day 09-23 skipped
    assert pa._confirm_prev_session("KSA", "2026-09-24") == "2026-09-22"
    monkeypatch.setenv("TFB_PF_SESSION_HOLIDAYS", "2026-09-28, 2026-09-29")
    assert k("US", T(2026, 9, 30, 0, 40)) == "2026-09-25"


def test_t6_observe_tag(monkeypatch, caplog, _offline_confirm_cache):
    _reset(monkeypatch, "observe")
    pa._ADD_CONFIRM_STORE["DDI.US"] = {"count": 1, "date": "2026-09-27"}   # Sunday's legacy count
    with _Clock(T(2026, 9, 28, 3, 40)):
        leg = pa._apply_add_confirmation("DDI.US", ADD, "q", None, CTL)
        reason = pa._apply_confirm_session_observe({"symbol": "DDI.US"}, ADD, *leg, CTL)
    assert leg[0] == ADD                                     # verdict untouched
    m = re.search(r"\[confirm-session-observe\] venue=(\w+) session=(\S+) legacy (\d)/(\d) "
                  r"vs session (\d)/(\d)( - FLIP)?; legacy clock kept", reason)
    assert m and m.group(1) == "US" and m.group(2) == "2026-09-25"
    assert (m.group(3), m.group(5), m.group(7)) == ("2", "1", " - FLIP")
    assert reason.count("[confirm-session-observe]") == 1
    assert pa._ADD_CONFIRM_SESSION_STORE["DDI.US"] == {"count": 1, "date": "2026-09-25"}
    shadow_key = pa._CONFIRM_SESSION_SHADOW_NS + "DDI.US"
    assert _offline_confirm_cache["values"][shadow_key] == pa._ADD_CONFIRM_SESSION_STORE["DDI.US"]
    # raw non-ADD clears the shadow chain and prints nothing
    r = pa._apply_confirm_session_observe({"symbol": "DDI.US"}, HOLD, HOLD, "x", None, CTL)
    assert r == "x" and "DDI.US" not in pa._ADD_CONFIRM_SESSION_STORE
    assert shadow_key not in _offline_confirm_cache["values"]
    # gate depth <= 1 -> nothing
    assert pa._apply_confirm_session_observe({"symbol": "DDI.US"}, ADD, ADD, "q", None,
                                             {"add_confirm_days": 1}) == "q"
    # injected calendar fault -> verdict kept, unavailable evidence exposed
    monkeypatch.setattr(pa, "_confirm_session_key", _raise)
    unavailable = pa._apply_confirm_session_observe({"symbol": "DDI.US"}, ADD, ADD, "q", None, CTL)
    assert unavailable.startswith("q;") and "session evidence unavailable" in unavailable
    assert "legacy clock kept" in unavailable
    # off / enforce are pass-throughs at this seam
    monkeypatch.setenv("TFB_PF_CONFIRM_SESSION", "enforce")
    assert pa._apply_confirm_session_observe({"symbol": "DDI.US"}, ADD, ADD, "q", None, CTL) == "q"


def test_t7_enforce_calendar_fault_holds_without_fallback(monkeypatch, caplog):
    _reset(monkeypatch, "enforce")
    monkeypatch.setattr(pa, "_confirm_session_key", _raise)
    with caplog.at_level(logging.WARNING), _Clock(T(2026, 9, 28, 3, 40)):
        out = pa._apply_add_confirmation("DDI.US", ADD, "q", None, CTL)
    assert out[0] == HOLD and "calendar unavailable" in out[1] and out[2] == ADD
    assert any("[CONFIRM-FAILCLOSED" in r.getMessage() for r in caplog.records)
    assert "DDI.US" not in pa._ADD_CONFIRM_STORE


def test_t8_p165_fail_closed_preserved(monkeypatch):
    _reset(monkeypatch, "enforce")
    pa._ADD_CONFIRM_STORE["DDI.US"] = {"count": 1, "date": "2000-01-01"}   # sentinel
    monkeypatch.setattr(pa, "_confirm_prev_session", _raise)               # calendar fault ...
    monkeypatch.setattr(pa, "_add_confirm_today", _raise)                  # ... and the legacy helper raises
    out = pa._apply_add_confirmation("DDI.US", ADD, "q", None, CTL)
    assert out[0] == HOLD and "[confirm-failclosed:ConfirmCalendarUnavailable]" in out[1] and out[2] == ADD
    assert pa._ADD_CONFIRM_STORE["DDI.US"] == {"count": 1, "date": "2000-01-01"}


# --------------------------------------------------------------------------
LIVE_PANEL = {"cash_available_sar": 38047.50, "target_cash_pct": 10.0,
              "max_position_pct": 20.0, "max_sector_pct": 30.0,
              "min_reliability_add": 70.0, "min_dq_add": 80.0,
              "rebalance_mode": "Advisory"}
FX = {"USD": 3.7560, "SAR": 1.0}


def _rows(path):
    with open(path, encoding="utf-8", newline="") as fh:
        rd = csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
        return [dict(r) for r in rd if r.get("Symbol", "").strip()]


def _verdicts(p):
    return [(a["symbol"], a["action"], (a.get("detail") or {}).get("capped_from"))
            for a in p["actions"]]


def _tags(a):
    return (str(a.get("action_reason") or "") + str(a.get("advisor_note") or "")
            ).count("[confirm-session-observe]")


@pytest.mark.skipif(not os.getenv("TFB_TEST_MP_TSV"), reason="set TFB_TEST_MP_TSV to a My_Portfolio TSV export")
def test_t9_integration_real_holdings(monkeypatch):
    rows = _rows(os.environ["TFB_TEST_MP_TSV"])
    when = T(2026, 9, 28, 3, 40)
    # off
    _reset(monkeypatch)
    pa._ADD_CONFIRM_STORE["DDI.US"] = {"count": 1, "date": "2026-09-27"}
    with _Clock(when):
        off = pa.build_portfolio_actions(rows, LIVE_PANEL, FX)
    assert "confirm_session" not in off["meta"]
    # observe
    _reset(monkeypatch, "observe")
    pa._ADD_CONFIRM_STORE["DDI.US"] = {"count": 1, "date": "2026-09-27"}
    with _Clock(when):
        obs = pa.build_portfolio_actions(rows, LIVE_PANEL, FX)
    assert _verdicts(obs) == _verdicts(off)
    assert obs["kpis"]["adds_funded_sar"] == off["kpis"]["adds_funded_sar"]
    raw_add = [a["symbol"] for a in obs["actions"] if _tags(a)]
    assert raw_add == ["DDI.US"] and all(_tags(a) == 2 for a in obs["actions"] if a["symbol"] == "DDI.US")
    assert " - FLIP" in [a for a in obs["actions"] if a["symbol"] == "DDI.US"][0]["action_reason"]
    assert obs["meta"]["confirm_session"]["mode"] == "observe"
    # enforce, Monday pre-open, Sunday's run already keyed on the Friday session
    _reset(monkeypatch, "enforce")
    pa._ADD_CONFIRM_STORE["DDI.US"] = {"count": 1, "date": "2026-09-25"}
    with _Clock(when):
        enf = pa.build_portfolio_actions(rows, LIVE_PANEL, FX)
    ddi = [a for a in enf["actions"] if a["symbol"] == "DDI.US"][0]
    assert ddi["action"] == HOLD and (ddi.get("detail") or {}).get("capped_from") == ADD
    assert "session 2026-09-25" in ddi["action_reason"]
    assert (enf["kpis"]["adds_funded_sar"] or 0) == 0
    assert {al["type"]: al["count"] for al in enf["alerts"]}.get("add_confirmation_pending") == 1
    with _Clock(T(2026, 9, 29, 0, 40)):
        tue = pa.build_portfolio_actions(rows, LIVE_PANEL, FX)
    ddi2 = [a for a in tue["actions"] if a["symbol"] == "DDI.US"][0]
    assert ddi2["action"] == ADD and "session 2026-09-28" in ddi2["action_reason"]
    assert (tue["kpis"]["adds_funded_sar"] or 0) > 0
