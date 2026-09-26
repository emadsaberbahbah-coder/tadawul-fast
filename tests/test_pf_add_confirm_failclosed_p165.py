"""tests/test_pf_add_confirm_failclosed_p165.py — portfolio_actions v1.12.2 [P-165]

The ADD-confirmation gate is the one rule that turns a verdict into money.
v1.12.1 ends it with ``except Exception: return action, reason, capped_from``
— the RAW verdict — so any exception inside the gate hands an UNCONFIRMED ADD
through as ADD (fail-open). v1.12.2 fails CLOSED for ADD (HOLD, capped_from=
ADD, clock untouched, one countable alert) behind the kill-switch
TFB_PF_ADD_CONFIRM_LEGACY_FAILOPEN=1 (restores v1.12.1 byte-identically).

T1  golden negative: the legacy path (kill-switch ON) reproduces the v1.12.1
    defect — an exception on an ADD returns ADD.  (On a v1.12.1 tree the
    DEFAULT path also returns ADD, so T2 FAILS there: golden negative.)
T2  fail-closed: exception on an ADD -> HOLD, "[confirm-failclosed:<Exc>]",
    capped_from == ADD, confirmation store untouched, one WARNING logged.
T3  exception on a non-ADD verdict (TRIM) returns the verdict unchanged in
    both modes — fail-closed never suppresses a risk-reducing action.
T4  unparsable add_confirm_days: default path -> DEFAULT depth (confirmation
    required, day 1/2); legacy -> 0 (gate off, v1.12.1 verbatim).
T5  clean paths are byte-identical between legacy and default: day 1 pending,
    yesterday-dated chain -> confirmed day 2/2, same-day rerun frozen.
T6  integration (skipped unless TFB_TEST_MP_TSV points at a My_Portfolio TSV
    export): build_portfolio_actions on the real holdings under the live
    panel — clean run has no gate_error alert; with the gate forced to raise,
    every ADD-qualifying holding renders HOLD fail-closed, adds_funded == 0,
    alerts carry add_confirmation_gate_error == n and the row is NOT counted
    under low_confidence_capped / add_confirmation_pending.

Run as a script to print a JSON digest of every scenario (the dual-tree
driver compares base vs delivered trees process-by-process, because both
trees share the ``core`` package name).
"""
from __future__ import annotations

import csv
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

import core.analysis.portfolio_actions as pa

ADD, HOLD, TRIM = pa.ACTION_ADD, pa.ACTION_HOLD, pa.ACTION_TRIM
_CTL_OK = {"add_confirm_days": 2}


def _reset(monkeypatch, legacy: bool, persist: bool = False):
    monkeypatch.setenv("TFB_PF_ADD_CONFIRM_LEGACY_FAILOPEN", "1" if legacy else "0")
    monkeypatch.setenv("TFB_PF_CONFIRM_PERSIST", "1" if persist else "0")
    pa._ADD_CONFIRM_STORE.clear()


def _raise(*_a, **_k):
    raise RuntimeError("injected: confirmation store unavailable")


# --------------------------------------------------------------------------
# T1 — golden negative (legacy path == v1.12.1 defect)
# --------------------------------------------------------------------------
def test_t1_legacy_path_reproduces_fail_open(monkeypatch):
    _reset(monkeypatch, legacy=True)
    monkeypatch.setattr(pa, "_add_confirm_today", _raise)
    out = pa._apply_add_confirmation("DDI.US", ADD, "qualifying: Upside 33.9%", None, _CTL_OK)
    assert out == (ADD, "qualifying: Upside 33.9%", None)   # raw verdict through = the defect


# --------------------------------------------------------------------------
# T2 — fail-closed on ADD
# --------------------------------------------------------------------------
def test_t2_fail_closed_on_add(monkeypatch, caplog):
    _reset(monkeypatch, legacy=False)
    pa._ADD_CONFIRM_STORE["DDI.US"] = {"count": 1, "date": "2000-01-01"}   # sentinel: must survive
    monkeypatch.setattr(pa, "_add_confirm_today", _raise)
    with caplog.at_level(logging.WARNING, logger="core.analysis.portfolio_actions"):
        action, reason, capped_from = pa._apply_add_confirmation(
            "DDI.US", ADD, "qualifying: Upside 33.9%", None, _CTL_OK)
    assert action == HOLD
    assert capped_from == ADD
    assert "[confirm-failclosed:RuntimeError]" in reason
    assert "ADD held fail-closed" in reason
    assert "pending confirmation" not in reason.lower()      # counted once, by its own alert
    assert reason.endswith("qualifying: Upside 33.9%")
    assert pa._ADD_CONFIRM_STORE["DDI.US"] == {"count": 1, "date": "2000-01-01"}   # clock untouched
    warns = [r for r in caplog.records if "[CONFIRM-FAILCLOSED" in r.getMessage()]
    assert len(warns) == 1 and "RuntimeError" in warns[0].getMessage()


# --------------------------------------------------------------------------
# T3 — non-ADD verdicts are never suppressed by the error path
# --------------------------------------------------------------------------
@pytest.mark.parametrize("legacy", [False, True])
def test_t3_non_add_unchanged_on_exception(monkeypatch, legacy):
    _reset(monkeypatch, legacy=legacy)
    # force the exception inside the non-ADD branch (store pop is wrapped by the outer try)
    monkeypatch.setattr(pa, "_confirm_persist_enabled", _raise)
    out = pa._apply_add_confirmation("SBAC", TRIM, "over cap", None, _CTL_OK)
    assert out == (TRIM, "over cap", None)


# --------------------------------------------------------------------------
# T4 — unparsable depth
# --------------------------------------------------------------------------
def test_t4_unparsable_depth(monkeypatch):
    bad = {"add_confirm_days": "abc"}
    _reset(monkeypatch, legacy=False)
    a, r, c = pa._apply_add_confirmation("CARE.US", ADD, "q", None, bad)
    assert (a, c) == (HOLD, ADD) and "(day 1/%d)" % pa.DEFAULT_CONTROLS["add_confirm_days"] in r
    _reset(monkeypatch, legacy=True)
    assert pa._apply_add_confirmation("CARE.US", ADD, "q", None, bad) == (ADD, "q", None)


# --------------------------------------------------------------------------
# T5 — clean paths identical in both modes
# --------------------------------------------------------------------------
def _clean_matrix():
    today = pa._add_confirm_today()
    yday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
    out = {}
    pa._ADD_CONFIRM_STORE.clear()
    out["day1"] = pa._apply_add_confirmation("DDI.US", ADD, "q", None, _CTL_OK)
    out["same_day_rerun"] = pa._apply_add_confirmation("DDI.US", ADD, "q", None, _CTL_OK)
    pa._ADD_CONFIRM_STORE["CARE.US"] = {"count": 1, "date": yday}
    out["day2_consecutive"] = pa._apply_add_confirmation("CARE.US", ADD, "q", None, _CTL_OK)
    out["non_add_resets"] = pa._apply_add_confirmation("CARE.US", TRIM, "cap", None, _CTL_OK)
    out["store_after"] = dict(pa._ADD_CONFIRM_STORE)
    out["today"] = today
    return out


def test_t5_clean_paths_identical(monkeypatch):
    _reset(monkeypatch, legacy=False)
    a = _clean_matrix()
    _reset(monkeypatch, legacy=True)
    b = _clean_matrix()
    assert a == b
    assert a["day1"][0] == HOLD and "(day 1/2)" in a["day1"][1] and a["day1"][2] == ADD
    assert a["same_day_rerun"] == a["day1"]
    assert a["day2_consecutive"][0] == ADD and "ADD confirmed (day 2/2)" in a["day2_consecutive"][1]
    assert "CARE.US" not in a["store_after"]


# --------------------------------------------------------------------------
# T6 — integration on the real My_Portfolio export (opt-in)
# --------------------------------------------------------------------------
LIVE_PANEL = {"cash_available_sar": 24763.73, "target_cash_pct": 10.0,
              "max_position_pct": 20.0, "max_sector_pct": 30.0,
              "min_reliability_add": 70.0, "min_dq_add": 80.0,
              "rebalance_mode": "Advisory"}
FX = {"USD": 3.7558, "SAR": 1.0}


def _load_rows(path):
    with open(path, encoding="utf-8", newline="") as fh:
        rd = csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
        return [dict(r) for r in rd]


def _summ(payload):
    acts = payload.get("actions") or []
    # capped_from is emitted under detail; the reason renders at TWO payload
    # sites (action_reason + advisor_note) — count the tag per row, not per string.
    return {
        "status": payload.get("status"),
        "actions": [(a.get("symbol"), a.get("action"),
                     (a.get("detail") or {}).get("capped_from"),
                     "confirm-failclosed" in (str(a.get("action_reason") or "")
                                              + str(a.get("advisor_note") or "")).lower(),
                     "pending confirmation" in str(a.get("action_reason") or "").lower())
                    for a in acts],
        "alerts": {al["type"]: al["count"] for al in (payload.get("alerts") or [])},
        "adds_funded": (payload.get("kpis") or {}).get("adds_funded_sar"),
        "action_counts": (payload.get("kpis") or {}).get("action_counts"),
    }


def _integration(path, monkeypatch, legacy, force_raise):
    _reset(monkeypatch, legacy=legacy, persist=False)
    if force_raise:
        monkeypatch.setattr(pa, "_add_confirm_today", _raise)
    payload = pa.build_portfolio_actions(_load_rows(path), LIVE_PANEL, FX)
    return _summ(payload)


@pytest.mark.skipif(not os.getenv("TFB_TEST_MP_TSV"), reason="set TFB_TEST_MP_TSV to a My_Portfolio TSV export")
def test_t6_integration_real_holdings(monkeypatch):
    path = os.environ["TFB_TEST_MP_TSV"]
    clean = _integration(path, monkeypatch, legacy=False, force_raise=False)
    assert "add_confirmation_gate_error" not in clean["alerts"]
    # confirmation-pending rows only (YUM also carries capped_from=ADD via the
    # §4.7 precedence veto — a different, untouched class)
    pending = [s for s, a, c, fc, pend in clean["actions"] if c == ADD and pend]
    assert pending, "expected at least one ADD-qualifying holding on the export"
    forced = _integration(path, monkeypatch, legacy=False, force_raise=True)
    fc_rows = [s for s, a, c, fc, pend in forced["actions"] if fc]
    assert sorted(fc_rows) == sorted(pending)
    assert all(a == HOLD and c == ADD and not pend
               for s, a, c, fc, pend in forced["actions"] if fc)
    # every other row byte-identical between the clean and forced runs
    others_clean = [t for t in clean["actions"] if t[0] not in pending]
    others_forced = [t for t in forced["actions"] if t[0] not in pending]
    assert others_clean == others_forced
    assert forced["alerts"].get("add_confirmation_gate_error") == len(fc_rows)
    assert "add_confirmation_pending" not in forced["alerts"]
    assert forced["alerts"].get("low_confidence_capped", 0) == clean["alerts"].get("low_confidence_capped", 0)
    assert not forced["adds_funded"]
    legacy = _integration(path, monkeypatch, legacy=True, force_raise=True)
    assert all(a == ADD for s, a, c, fc, pend in legacy["actions"] if s in pending)   # the v1.12.1 defect, reproduced
    assert legacy["adds_funded"]   # ...and the funding pass sizes the unconfirmed ADDs


# --------------------------------------------------------------------------
# script mode: JSON digest for the dual-tree driver
# --------------------------------------------------------------------------
def _digest(path):
    class _MP:   # minimal monkeypatch stand-in for script mode
        def __init__(self): self._env = {}; self._attr = []
        def setenv(self, k, v): os.environ[k] = v
        def setattr(self, obj, name, val): self._attr.append((obj, name, getattr(obj, name))); setattr(obj, name, val)
        def undo(self):
            for obj, name, old in reversed(self._attr): setattr(obj, name, old)
            self._attr = []
    out = {"version": pa.PORTFOLIO_ACTIONS_VERSION}
    mp = _MP()
    for legacy in (False, True):
        _reset(mp, legacy=legacy)
        out["clean_%s" % legacy] = _clean_matrix()
        mp.setattr(pa, "_add_confirm_today", _raise)
        out["exc_add_%s" % legacy] = pa._apply_add_confirmation("DDI.US", ADD, "q", None, _CTL_OK)
        out["exc_trim_%s" % legacy] = pa._apply_add_confirmation("SBAC", TRIM, "cap", None, _CTL_OK)
        mp.undo()
        out["bad_days_%s" % legacy] = pa._apply_add_confirmation("CARE.US", ADD, "q", None, {"add_confirm_days": "abc"})
        if path:
            out["int_clean_%s" % legacy] = _integration(path, mp, legacy, False)
            out["int_raise_%s" % legacy] = _integration(path, mp, legacy, True)
            mp.undo()
    return out


if __name__ == "__main__":
    print(json.dumps(_digest(os.getenv("TFB_TEST_MP_TSV")), sort_keys=True, default=str))
