#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""tests/test_pf_add_loser_veto_p183.py

portfolio_actions v1.14.0 [P-183 ADD-ON-LOSER VETO]. REAL module end-to-end
(build_portfolio_actions on the real 2026-10-01 My_Portfolio export - 6
holdings incl. AER.US at -3.3 % with a confirmed ADD - via TFB_TEST_MP_TSV,
plus hand fixtures), production panel controls. Dual-tree: PA_BASE=<path to
the v1.13.1 file> proves OFF == base byte-for-byte and that observe differs
from base only by the per-row tag / alert / meta read-back.

L1 helpers: mode reader (off / observe / enforce / junk), thresholds,
   _add_loser_eval on the real AER row (loser), DDI (clean), a near-stop
   fixture, an at-stop fixture, a missing-basis fixture
L2 OFF == base (dual-tree) on the real export; no veto strings anywhere
L3 observe: verdicts / KPIs / funding identical to OFF; AER row carries ONE
   "[addveto-observe] ... would HOLD" tag at both sites; DDI (ADD, +4.3 %)
   carries "[addveto-observe] ok"; alert add_loser_observe=1; meta block
L4 enforce: AER -> HOLD "ADD vetoed [P-183]" with capped_from=ADD, delta 0,
   funds_from None; DDI stays ADD and keeps its funding; adds_funded drops
   by AER's ticket; alert add_loser_veto=1, low_confidence_capped unchanged;
   every other row byte-identical to OFF
L5 hand fixtures: near-stop (+1 % P&L, price 1.5 % above stop) vetoed;
   at/below stop vetoed; sukuk exempt; thresholds env-tunable (loser 5 %
   lets AER through); non-positive proximity band disables that leg
L6 env hygiene: OFF after enforce == OFF
L7 idempotent x2 (digest)
Run: python3 tests/test_pf_add_loser_veto_p183.py   (x3, digest)
"""
from __future__ import annotations

import copy, csv, hashlib, importlib, importlib.util, json, os, sys
from contextlib import ExitStack
from datetime import datetime
from unittest.mock import patch

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

ENVS = ("TFB_PF_ADD_LOSER_VETO", "TFB_PF_ADD_LOSER_PCT", "TFB_PF_ADD_STOP_PROX_PCT",
        "TFB_FORECAST_BASIS", "TFB_PF_CONFIRM_SESSION", "TFB_PF_DD_EXIT")
# production Render env for the display path (/health 2026-10-01): engine ROI
# display on, forecast basis observe, confirm session observe; confirmation
# persist off in the harness (no Redis); confirm days 1 so the real ADDs
# render as ADD on a single build (production: AER confirmed 2/2 today).
os.environ["TFB_PF_ENGINE_ROI_DISPLAY"] = "1"
os.environ["TFB_PF_CONFIRM_PERSIST"] = "0"
os.environ["TFB_PF_ENABLED"] = "1"
os.environ["TFB_PF_ADD_CONFIRM_DAYS"] = "1"

PA_FILE = os.environ.get("PA_FILE")
if PA_FILE:
    _spec = importlib.util.spec_from_file_location("core.analysis.portfolio_actions", PA_FILE)
    pa = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(pa)
else:
    import core.analysis.portfolio_actions as pa  # noqa: E402
assert pa.PORTFOLIO_ACTIONS_VERSION == "1.14.1", pa.PORTFOLIO_ACTIONS_VERSION

PANEL = {"cash_available_sar": 34166.25, "target_cash_pct": 10.0,
         "max_position_pct": 20.0, "max_sector_pct": 30.0,
         "min_reliability_add": 70.0, "min_dq_add": 80.0,
         "rebalance_mode": "Advisory", "add_confirm_days": 1}
FX = {"USD": 3.7624, "SAR": 1.0}
MP = os.environ.get("TFB_TEST_MP_TSV", "")
BASE = os.environ.get("PA_BASE", "")


def _env(mode=None, loser=None, prox=None):
    for k in ENVS:
        os.environ.pop(k, None)
    os.environ["TFB_FORECAST_BASIS"] = "observe"
    os.environ["TFB_PF_CONFIRM_SESSION"] = "observe"
    if mode is not None:
        os.environ["TFB_PF_ADD_LOSER_VETO"] = mode
    if loser is not None:
        os.environ["TFB_PF_ADD_LOSER_PCT"] = str(loser)
    if prox is not None:
        os.environ["TFB_PF_ADD_STOP_PROX_PCT"] = str(prox)


def _rows(path):
    with open(path, encoding="utf-8", newline="") as fh:
        rd = csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
        return [dict(r) for r in rd if r.get("Symbol", "").strip()]


def _build(mod, rows, mode=None, loser=None, prox=None):
    _env(mode, loser, prox)
    try:
        with ExitStack() as clocks:
            if rows is globals().get("FIX"):
                # Only the dated synthetic fixture runs at its witnessed time;
                # optional real exports keep the actual freshness policy.
                when = datetime.fromisoformat(rows[0]["Last Updated (UTC)"])
                class FixtureClock(datetime):
                    @classmethod
                    def now(cls, tz=None):
                        return when.astimezone(tz) if tz else when.replace(tzinfo=None)
                from core.analysis import opportunity_builder
                clocks.enter_context(patch.object(mod, "datetime", FixtureClock))
                clocks.enter_context(patch.object(opportunity_builder, "datetime", FixtureClock))
            mod._ADD_CONFIRM_STORE.clear()
            p = mod.build_portfolio_actions(copy.deepcopy(rows), dict(PANEL), dict(FX))
    finally:
        _env(None)
    p = json.loads(json.dumps(p, default=str, sort_keys=True))
    (p.get("meta") or {}).pop("generated_utc", None)
    return p


def _mask(p):
    p = copy.deepcopy(p); p.pop("version", None); (p.get("meta") or {}).pop("versions", None)
    return p


def _digest(p):
    return hashlib.sha256(json.dumps(p, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _act(p, sym):
    for a in p["actions"]:
        if a["symbol"] == sym:
            return a
    return None


def _alerts(p):
    return {a["type"]: a["count"] for a in p.get("alerts") or []}


def _diff(a, b, path=""):
    out = []
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            out += _diff(a.get(k), b.get(k), path + "/" + str(k))
    elif isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        for i, (x, y) in enumerate(zip(a, b)):
            out += _diff(x, y, path + "[%d]" % i)
    elif a != b:
        out.append(path)
    return out


out = []


def T(name, cond, detail=""):
    out.append(("PASS " if cond else "FAIL ") + name + (" | " + str(detail)[:300] if detail else ""))
    if not cond:
        raise AssertionError(name + " " + str(detail))


# ------------------------------------------------------------- fixtures --- #
def _fx_row(symbol, price, qty, avg_cost, tp1_mult=1.25, stop_mult=0.92, sukuk=False,
            sector="Industrials", rel=75.4):
    """Equity holding row shaped like the My_Portfolio export (TP1/stop derived
    by the ladder from Target Price; the stop is fixed by the ladder's own
    rule, so a near-stop fixture is made by choosing avg_cost/price)."""
    r = {"Symbol": symbol, "Name": symbol + " Co", "Asset Class": "Fixed Income / Sukuk" if sukuk else "Equity",
         "Exchange": "NYSE/NASDAQ", "Currency": "USD", "Sector": sector, "Industry": "x",
         "Current Price": price, "Target Price": round(price * tp1_mult * 1.6, 2),
         "Expected ROI 12M": round(tp1_mult * 1.6 - 1.0, 4), "Forecast Reliability Score": rel,
         "Data Quality Score": 100.0, "Risk Bucket": "LOW", "Investability Status": "INVESTABLE",
         "Final Action": "INVEST", "Recommendation": "BUY", "Volatility 30D": 0.2,
         "Forecast Source": "provider_target", "Position Qty": qty, "Avg Cost": avg_cost,
         "Position Cost": round(qty * avg_cost, 2), "Position Value": round(qty * price, 2),
         "Unrealized P/L": round(qty * (price - avg_cost), 2),
         "Unrealized P/L %": round((price / avg_cost - 1) * 100, 4),
         "Buy Date": "2026-09-20T00:00:00", "Last Updated (UTC)": "2026-10-01T04:25:35+00:00"}
    return r


# ------------------------------------------------------------------ L1 ---- #
_env(None); T("L1 mode default off", pa._env_add_loser_veto_mode() == "off")
_env("Enforce"); T("L1 mode case-insensitive", pa._env_add_loser_veto_mode() == "enforce")
_env("junk"); T("L1 junk -> off", pa._env_add_loser_veto_mode() == "off")
_env(None, loser=-3.5, prox=1.0); T("L1 thresholds (abs loser, prox)", (pa._env_add_loser_pct(), pa._env_add_stop_prox_pct()) == (3.5, 1.0))
_env(None); T("L1 defaults 2.0 / 2.0", (pa._env_add_loser_pct(), pa._env_add_stop_prox_pct()) == (2.0, 2.0))
ev = pa._add_loser_eval({"pnl_sar": -254.0, "cost_sar": 7814.0, "price": 143.52, "stop": 131.45}, 2.0, 2.0)
T("L1 AER-shaped loser: -3.25 % <= -2 %, not near stop (9.2 % above)", ev["loser"] and not ev["near_stop"] and abs(ev["ret_pct"] + 3.2506) < 0.01, ev)
ev2 = pa._add_loser_eval({"pnl_sar": 471.0, "cost_sar": 10979.0, "price": 13.29, "stop": 12.23}, 2.0, 2.0)
T("L1 DDI-shaped clean: +4.3 %, 8.7 % above stop", not (ev2["loser"] or ev2["near_stop"] or ev2["below_stop"]) and ev2["trig"] == "", ev2)
ev3 = pa._add_loser_eval({"pnl_sar": 100.0, "cost_sar": 10000.0, "price": 30.00, "stop": 29.56}, 2.0, 2.0)
T("L1 near-stop: +1 % P&L but 1.5 % above stop -> near_stop", ev3["near_stop"] and not ev3["loser"] and "above the stop" in ev3["trig"], ev3)
ev4 = pa._add_loser_eval({"pnl_sar": 100.0, "cost_sar": 10000.0, "price": 29.50, "stop": 29.56}, 2.0, 2.0)
T("L1 at/below stop -> below_stop", ev4["below_stop"] and "at/below the stop" in ev4["trig"], ev4)
ev5 = pa._add_loser_eval({"pnl_sar": None, "cost_sar": None, "price": None, "stop": None}, 2.0, 2.0)
T("L1 missing basis -> nothing fires", not (ev5["loser"] or ev5["near_stop"] or ev5["below_stop"]) and ev5["ret_pct"] is None, ev5)
ev6 = pa._add_loser_eval({"pnl_sar": 100.0, "cost_sar": 10000.0, "price": 30.00, "stop": 29.56}, 2.0, 0.0)
T("L1 non-positive band disables the proximity leg", not ev6["near_stop"], ev6)
_env("enforce")
a, r, pr, cf = pa._apply_add_loser_veto({"pnl_sar": -254.0, "cost_sar": 7814.0, "price": 143.52, "stop": 131.45},
                                        pa.ACTION_ADD, "ADD 20 sh", 0.0, None)
T("L1 enforce narrows ADD -> HOLD, capped_from=ADD, reason named", a == pa.ACTION_HOLD and cf == pa.ACTION_ADD
  and r.startswith("ADD vetoed [P-183]: position -3.3% vs cost") and "(was ADD: ADD 20 sh)" in r, r)
a2, r2, _, cf2 = pa._apply_add_loser_veto({"pnl_sar": -254.0, "cost_sar": 7814.0}, pa.ACTION_HOLD, "HOLD", 0.0, None)
T("L1 enforce never touches a non-ADD", (a2, r2, cf2) == (pa.ACTION_HOLD, "HOLD", None))
_env(None)
a3, r3, _, _ = pa._apply_add_loser_veto({"pnl_sar": -254.0, "cost_sar": 7814.0}, pa.ACTION_ADD, "ADD", 0.0, None)
T("L1 off is a pure pass-through", (a3, r3) == (pa.ACTION_ADD, "ADD"))

# ------------------------------------------------------------------ L2 ---- #
if MP and os.path.exists(MP):
    rows = _rows(MP)
    T("L2 real export loaded (6 holdings incl. AER.US)", len(rows) == 6 and any(r["Symbol"] == "AER.US" for r in rows), [r["Symbol"] for r in rows])
    p_off = _build(pa, rows, None)
    aer_off = _act(p_off, "AER.US"); ddi_off = _act(p_off, "DDI.US")
    T("L2 OFF: AER renders ADD (confirm days 1), DDI ADD", aer_off["action"] == "ADD" and ddi_off["action"] == "ADD",
      [(a["symbol"], a["action"]) for a in p_off["actions"]])
    T("L2 OFF: no veto strings anywhere", "addveto" not in json.dumps(p_off).lower() and "p-183" not in json.dumps(p_off).lower()
      and "add_loser" not in json.dumps(p_off))
    if BASE:
        spec = importlib.util.spec_from_file_location("pa_base_v1131", BASE)
        pab = importlib.util.module_from_spec(spec); spec.loader.exec_module(pab)
        assert pab.PORTFOLIO_ACTIONS_VERSION == "1.13.1", pab.PORTFOLIO_ACTIONS_VERSION
        p_base = _build(pab, rows, None)
        T("L2 dual-tree: OFF == base byte-for-byte (versions masked)", _digest(_mask(p_base)) == _digest(_mask(p_off)),
          _diff(_mask(p_base), _mask(p_off))[:10])
        p_base_enf = _build(pab, rows, "enforce")
        T("L2 dual-tree: the new env is inert on base", _digest(_mask(p_base_enf)) == _digest(_mask(p_base)))
    else:
        out.append("SKIP L2 dual-tree (set PA_BASE=<v1.13.1 file>)")

    # -------------------------------------------------------------- L3 ---- #
    p_obs = _build(pa, rows, "observe")
    T("L3 observe: verdicts identical to OFF", [(a["symbol"], a["action"]) for a in p_obs["actions"]] == [(a["symbol"], a["action"]) for a in p_off["actions"]])
    T("L3 observe: KPIs identical to OFF", p_obs["kpis"] == p_off["kpis"], _diff(p_obs["kpis"], p_off["kpis"]))
    aer_obs = _act(p_obs, "AER.US"); ddi_obs = _act(p_obs, "DDI.US")
    T("L3 observe: AER tag at both sites, exactly once each",
      aer_obs["action_reason"].count("[addveto-observe]") == 1 and aer_obs["advisor_note"].count("[addveto-observe]") == 1
      and "would HOLD under enforce" in aer_obs["action_reason"] and "position -3.3% vs cost" in aer_obs["action_reason"], aer_obs["action_reason"])
    T("L3 observe: DDI ADD tagged ok", "[addveto-observe] ok" in ddi_obs["action_reason"], ddi_obs["action_reason"])
    T("L3 observe: non-ADD rows carry no tag", all("[addveto" not in a["action_reason"] for a in p_obs["actions"] if a["action"] != "ADD"))
    T("L3 observe: alert add_loser_observe=1, no add_loser_veto", _alerts(p_obs).get("add_loser_observe") == 1 and "add_loser_veto" not in _alerts(p_obs), _alerts(p_obs))
    T("L3 observe: meta block", p_obs["meta"]["add_loser_veto"] == {"mode": "observe", "loser_pct": 2.0, "stop_prox_pct": 2.0, "vetoed": 0, "would_veto": 1}, p_obs["meta"].get("add_loser_veto"))
    T("L3 observe: AER funding unchanged vs OFF", aer_obs["suggested_delta_sar"] == aer_off["suggested_delta_sar"] and aer_obs["suggested_delta_shares"] == aer_off["suggested_delta_shares"], (aer_obs["suggested_delta_sar"], aer_off["suggested_delta_sar"]))
    d = [x for x in _diff(_mask(p_obs), _mask(p_off)) if not x.endswith("/action_reason") and not x.endswith("/advisor_note")]
    T("L3 observe: differs from OFF only by reason/note text, the alert and the meta block",
      all(x.startswith("/alerts") or x.startswith("/meta/add_loser_veto") for x in d), d[:12])

    # -------------------------------------------------------------- L4 ---- #
    p_enf = _build(pa, rows, "enforce")
    aer_enf = _act(p_enf, "AER.US"); ddi_enf = _act(p_enf, "DDI.US")
    T("L4 enforce: AER -> HOLD, capped_from=ADD, delta 0, no funds_from",
      aer_enf["action"] == "HOLD" and (aer_enf.get("detail") or {}).get("capped_from") == "ADD"
      and (aer_enf["suggested_delta_sar"] in (0, 0.0, None)) and (aer_enf["suggested_delta_shares"] in (0, 0.0, None)) and not aer_enf.get("funds_from"),
      (aer_enf["action"], aer_enf.get("detail"), aer_enf["suggested_delta_sar"], aer_enf["suggested_delta_shares"], aer_enf.get("funds_from")))
    T("L4 enforce: AER reason + note name the veto", aer_enf["action_reason"].startswith("ADD vetoed [P-183]") and "ADD vetoed [P-183]" in aer_enf["advisor_note"], aer_enf["action_reason"])
    T("L4 enforce: DDI stays ADD with its funding", ddi_enf["action"] == "ADD" and ddi_enf["suggested_delta_sar"] == ddi_off["suggested_delta_sar"], (ddi_enf["suggested_delta_sar"], ddi_off["suggested_delta_sar"]))
    T("L4 enforce: adds_funded drops by AER's ticket", abs((p_off["kpis"]["adds_funded_sar"] - p_enf["kpis"]["adds_funded_sar"]) - (aer_off["suggested_delta_sar"] or 0)) < 1.0,
      (p_off["kpis"]["adds_funded_sar"], p_enf["kpis"]["adds_funded_sar"], aer_off["suggested_delta_sar"]))
    T("L4 enforce: alert add_loser_veto=1; low_confidence_capped unchanged", _alerts(p_enf).get("add_loser_veto") == 1
      and _alerts(p_enf).get("low_confidence_capped") == _alerts(p_off).get("low_confidence_capped"), (_alerts(p_enf), _alerts(p_off)))
    T("L4 enforce: meta vetoed=1", p_enf["meta"]["add_loser_veto"]["vetoed"] == 1 and p_enf["meta"]["add_loser_veto"]["mode"] == "enforce")
    others = [a["symbol"] for a in p_off["actions"] if a["symbol"] != "AER.US"]
    for s_ in others:
        T("L4 enforce: %s row byte-identical to OFF" % s_, _act(p_enf, s_) == _act(p_off, s_), _diff(_act(p_enf, s_), _act(p_off, s_)))
    T("L4 enforce: action_counts A/H shift by one", p_enf["kpis"]["action_counts"].get("ADD", 0) == p_off["kpis"]["action_counts"].get("ADD", 0) - 1
      and p_enf["kpis"]["action_counts"].get("HOLD", 0) == p_off["kpis"]["action_counts"].get("HOLD", 0) + 1, (p_enf["kpis"]["action_counts"], p_off["kpis"]["action_counts"]))

    # -------------------------------------------------------------- L6/L7 - #
    p_off2 = _build(pa, rows, None)
    T("L6 OFF after enforce == OFF", _digest(p_off2) == _digest(p_off))
    T("L7 idempotent observe / enforce", _digest(_build(pa, rows, "observe")) == _digest(p_obs) and _digest(_build(pa, rows, "enforce")) == _digest(p_enf))
    real_digest = (_digest(p_off), _digest(p_obs), _digest(p_enf))
else:
    out.append("SKIP L2-L4/L6-L7 real export (set TFB_TEST_MP_TSV=<My_Portfolio.tsv>)")
    real_digest = None

# ------------------------------------------------------------------ L5 ---- #
FIX = [_fx_row("NEAR.US", 30.00, 25, 29.70, sector="Industrials"),       # +1.0 % P&L; ladder stop decides proximity
       _fx_row("LOSE.US", 40.00, 20, 42.00, sector="Energy"),            # -4.8 % loser
       _fx_row("CLEAN.US", 50.00, 15, 45.00, sector="Technology"),       # +11 %, clean
       _fx_row("5023.SR", 100.2, 100, 100.0, sukuk=True, sector="Sukuk")]  # ~19 % weight, under the 20 % cap
p_f_off = _build(pa, FIX, None)
p_f_enf = _build(pa, FIX, "enforce")
lose_enf = _act(p_f_enf, "LOSE.US"); clean_enf = _act(p_f_enf, "CLEAN.US")
T("L5 fixture: LOSE.US ADD vetoed (loser), CLEAN.US ADD kept",
  (_act(p_f_off, "LOSE.US")["action"] == "ADD") and lose_enf["action"] == "HOLD" and "loser floor" in lose_enf["action_reason"]
  and clean_enf["action"] == _act(p_f_off, "CLEAN.US")["action"] == "ADD", [(a["symbol"], a["action"]) for a in p_f_enf["actions"]])
near_off = _act(p_f_off, "NEAR.US"); near_enf = _act(p_f_enf, "NEAR.US")
near_px, near_stop = 30.00, (near_off.get("stop_sar") or 0) / FX["USD"]
T("L5 fixture: NEAR.US stop known from the ladder", near_stop > 0, near_off.get("stop_sar"))
if near_px / near_stop - 1 <= 0.02:
    T("L5 fixture: NEAR.US within 2 % of its stop -> vetoed", near_enf["action"] == "HOLD" and "above the stop" in near_enf["action_reason"], near_enf["action_reason"])
else:
    p_f_enf_wide = _build(pa, FIX, "enforce", prox=round((near_px / near_stop - 1) * 100 + 0.5, 2))
    near_w = _act(p_f_enf_wide, "NEAR.US")
    T("L5 fixture: NEAR.US vetoed once the band covers its %.1f%% distance to the stop" % ((near_px / near_stop - 1) * 100),
      near_w["action"] == "HOLD" and "above the stop" in near_w["action_reason"], near_w["action_reason"])
suk_enf = _act(p_f_enf, "5023.SR")
T("L5 fixture: sukuk exempt (no veto strings)", suk_enf is not None and "P-183" not in json.dumps(suk_enf), suk_enf and suk_enf["action_reason"])
p_f_loose = _build(pa, FIX, "enforce", loser=5.0)
T("L5 fixture: loser floor 5 % lets a -4.8 % position through", _act(p_f_loose, "LOSE.US")["action"] == "ADD", _act(p_f_loose, "LOSE.US")["action_reason"])
p_f_nob = _build(pa, FIX, "enforce", prox=0)
T("L5 fixture: proximity band 0 disables that leg (LOSE still vetoed by the floor)",
  _act(p_f_nob, "LOSE.US")["action"] == "HOLD" and "above the stop" not in json.dumps(p_f_nob), _alerts(p_f_nob))

digest = hashlib.sha256(("\n".join(out) + json.dumps(real_digest)).encode()).hexdigest()[:16]
print("\n".join(out))
print("RESULT %d PASS / %d FAIL, digest %s" % (sum(1 for l in out if l.startswith("PASS")), sum(1 for l in out if l.startswith("FAIL")), digest))


def test_all():
    assert not any(l.startswith("FAIL") for l in out)
