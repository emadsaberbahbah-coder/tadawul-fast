#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""tests/test_pf_sukuk_ladder_p153.py

portfolio_actions v1.13.1 [P-153 SUKUK ROW: NO EQUITY LADDER ON A FIXED-INCOME
HOLDING]. REAL module end-to-end (build_portfolio_actions on the real
2026-09-30 My_Portfolio export - 6 holdings incl. 5023.SR - via
TFB_TEST_MP_TSV, plus hand fixtures), REAL core.compliance_gate classifier.
Dual-tree: PA_BASE=<path to the v1.13.0 file> loads the base beside the
delivered module and proves (a) kill-switch == base byte-for-byte and (b) the
default differs from base ONLY on the sukuk row's six display fields.

S1 helpers: kill-switch parser; _sukuk_display_active on 5023.SR / YUM,
   under the kill and under TFB_PA_PROTECT_SUKUK=0 (inert)
S2 default, forecast basis observe (production): 5023.SR ladder cells None,
   advisor note carries SUKUK_LADDER_NOTE and no "stop x / TP1", action
   reason carries "[f1-observe] n/a - sukuk", detail.ladder_display =
   sukuk_na; every equity row keeps its ladder + plan-3M tag; verdicts,
   capped_from, KPIs and alerts identical to the kill-switch run
S3 kill-switch: the v1.13.0 row (92.23 / 102.70 / 105.16 SAR, plan-3M tag)
S4 TFB_PA_PROTECT_SUKUK=0: inert (== kill-switch row)
S5 dual-tree (PA_BASE): legacy payload == base payload; default payload
   differs from base only in actions[5023.SR].{stop_sar,tp1_sar,tp2_sar,
   action_reason,advisor_note,detail.ladder_display}
S6 forecast basis legacy / plan3m: no f1 text touched; only the ladder
   cells + note bit change; decide_action verdicts equal in every mode
S7 hand fixtures: an equity row without a TP ladder still prints the
   v1.7.2 B-7 "no TP ladder" line (the elif is intact); a sukuk row whose
   verdict is not ADD/HOLD prints no ladder note
S8 idempotent x2
Run: python3 tests/test_pf_sukuk_ladder_p153.py   (x3, digest)
     pytest -q tests/test_pf_sukuk_ladder_p153.py
"""
from __future__ import annotations

import copy
import csv
import hashlib
import importlib
import importlib.util
import json
import os
import re
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

ENVS = ("TFB_PA_SUKUK_LADDER_LEGACY", "TFB_PA_PROTECT_SUKUK", "TFB_FORECAST_BASIS",
        "TFB_PF_CONFIRM_SESSION", "TFB_PF_DD_EXIT")
# production Render env for the display path (health 2026-09-30): engine ROI
# display on, forecast basis observe, confirmation persist off in the harness
# (no Redis here; the counter is reset per build anyway)
os.environ["TFB_PF_ENGINE_ROI_DISPLAY"] = "1"
os.environ["TFB_PF_CONFIRM_PERSIST"] = "0"
os.environ["TFB_PF_ENABLED"] = "1"

import core.analysis.portfolio_actions as pa  # noqa: E402

PANEL = {"cash_available_sar": 34166.25, "target_cash_pct": 10.0,
         "max_position_pct": 20.0, "max_sector_pct": 30.0,
         "min_reliability_add": 70.0, "min_dq_add": 80.0,
         "rebalance_mode": "Advisory"}
FX = {"USD": 3.7555, "SAR": 1.0}
MP = os.environ.get("TFB_TEST_MP_TSV", "")
BASE = os.environ.get("PA_BASE", "")


def _env(legacy=None, protect=None, basis="observe"):
    for k in ENVS:
        os.environ.pop(k, None)
    if legacy is not None:
        os.environ["TFB_PA_SUKUK_LADDER_LEGACY"] = legacy
    if protect is not None:
        os.environ["TFB_PA_PROTECT_SUKUK"] = protect
    if basis is not None:
        os.environ["TFB_FORECAST_BASIS"] = basis


def _rows(path):
    with open(path, encoding="utf-8", newline="") as fh:
        rd = csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
        return [dict(r) for r in rd if r.get("Symbol", "").strip()]


def _build(mod, rows, legacy=None, protect=None, basis="observe"):
    _env(legacy, protect, basis)
    try:
        mod._ADD_CONFIRM_STORE.clear()
        p = mod.build_portfolio_actions(copy.deepcopy(rows), dict(PANEL), dict(FX))
    finally:
        _env(None, None, None)
    p = json.loads(json.dumps(p, default=str, sort_keys=True))
    (p.get("meta") or {}).pop("generated_utc", None)
    return p


def _mask_versions(p):
    p = copy.deepcopy(p)
    p.pop("version", None)
    m = p.get("meta") or {}
    m.pop("versions", None)
    return p


def _act(p, sym):
    for a in p["actions"]:
        if a["symbol"] == sym:
            return a
    return None


def _verdicts(p):
    return [(a["symbol"], a["action"], (a.get("detail") or {}).get("capped_from")) for a in p["actions"]]


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
digest = []


def T(name, cond, detail=""):
    out.append(("PASS " if cond else "FAIL ") + name + (" | " + str(detail)[:300] if detail else ""))
    if not cond:
        raise AssertionError(name + " " + str(detail))


# ---------------------------------------------------------------- hand fixtures
def _row(symbol, name, qty, avg, price, ccy="USD", stop=None, tp1=None, tp2=None, rel=76.5, dq=100.0,
         roi=25.0, sector="Financials"):
    r = {"Symbol": symbol, "Name": name, "Sector": sector, "Currency": ccy, "Market": "NYSE/NASDAQ",
         "Quantity": qty, "Avg Cost": avg, "Current Price": price, "Expected ROI 12M": roi,
         "Forecast Reliability Score": rel, "Data Quality Score": dq, "Recommendation": "HOLD",
         "Investability Status": "INVESTABLE", "Final Action": "HOLD", "Target Price": price * (1 + roi / 100.0)}
    if stop is not None:
        r["Stop Loss"] = stop
    if tp1 is not None:
        r["Take Profit 1"] = tp1
    if tp2 is not None:
        r["Take Profit 2"] = tp2
    return r


HAND = [
    _row("YUM", "Yum! Brands, Inc.", 24, 144.71, 137.73, stop=119.85, tp1=155.4, tp2=173.1, sector="Consumer Discretionary"),
    _row("NOLAD.US", "No Ladder Corp", 10, 20.0, 21.0, roi=-3.0),          # target below price -> stop, no TP: B-7 line
    _row("5023.SR", "", 100, 100.0, 100.25, ccy="SAR", stop=92.23, tp1=102.7, tp2=105.16, rel=26.2, dq=70.6, roi=4.9, sector="Unknown"),
]


def run_all():
    T("S0 delivered version", pa.PORTFOLIO_ACTIONS_VERSION == "1.13.1", pa.PORTFOLIO_ACTIONS_VERSION)
    base = None
    if BASE and os.path.exists(BASE):
        spec = importlib.util.spec_from_file_location("pa_base_v1130", BASE)
        base = importlib.util.module_from_spec(spec)
        sys.modules["pa_base_v1130"] = base
        spec.loader.exec_module(base)
        T("S0 base version 1.13.0 (dual-tree armed)", base.PORTFOLIO_ACTIONS_VERSION == "1.13.0"
          and not hasattr(base, "_sukuk_display_active"), base.PORTFOLIO_ACTIONS_VERSION)

    # ------------------------------------------------------------- S1
    _env(None)
    words = []
    for w in (None, "0", "1", "true", "on", "yes", "off", " TRUE "):
        if w is None:
            os.environ.pop("TFB_PA_SUKUK_LADDER_LEGACY", None)
        else:
            os.environ["TFB_PA_SUKUK_LADDER_LEGACY"] = w
        words.append(pa._env_sukuk_ladder_legacy())
    T("S1 kill-switch parser: 1/true/on/yes (trimmed, case-folded) only", words == [False, False, True, True, True, True, False, True], words)
    _env(None)
    suk = {"symbol": "5023.SR", "name": ""}
    T("S1 real classifier: 5023.SR sukuk display active; YUM not; None/garbage fail-open False",
      pa._sukuk_display_active(suk) is True and pa._sukuk_display_active({"symbol": "YUM", "name": "Yum! Brands, Inc."}) is False
      and pa._sukuk_display_active(None) is False and pa._sukuk_display_active({"symbol": None}) is False)
    _env("1")
    k1 = pa._sukuk_display_active(suk)
    _env(None, "0")
    k2 = pa._sukuk_display_active(suk)
    _env(None)
    T("S1 kill-switch and TFB_PA_PROTECT_SUKUK=0 both make the sukuk display inert", k1 is False and k2 is False, (k1, k2))
    T("S1 note constant names D-9 and the exit contract", "D-9" in pa.SUKUK_LADDER_NOTE and "maturity" in pa.SUKUK_LADDER_NOTE)
    digest.append(words)

    # ------------------------------------------------------------- S2/S3/S4 real export
    rows = _rows(MP) if MP and os.path.exists(MP) else None
    if rows is None:
        out.append("SKIP S2-S6 real export (set TFB_TEST_MP_TSV=<My_Portfolio export>)")
        rows = HAND
        real = False
    else:
        real = True
        T("S2 real 2026-09-30 export: 6 holdings incl. 5023.SR",
          len(rows) == 6 and any(r["Symbol"] == "5023.SR" for r in rows), [r["Symbol"] for r in rows])
    p_def = _build(pa, rows)
    p_leg = _build(pa, rows, legacy="1")
    p_pro = _build(pa, rows, protect="0")
    T("S2 build ok in all three modes", p_def.get("status") == "ok" and p_leg.get("status") == "ok" and p_pro.get("status") == "ok",
      (p_def.get("status"), p_def.get("reason")))
    s_def, s_leg, s_pro = _act(p_def, "5023.SR"), _act(p_leg, "5023.SR"), _act(p_pro, "5023.SR")
    T("S2 default: sukuk ladder cells None; witness ladder_display=sukuk_na",
      s_def["stop_sar"] is None and s_def["tp1_sar"] is None and s_def["tp2_sar"] is None
      and (s_def.get("detail") or {}).get("ladder_display") == "sukuk_na", (s_def["stop_sar"], s_def["tp1_sar"], s_def["tp2_sar"]))
    T("S2 default: advisor note carries the sukuk note, no equity ladder bit, engine forecast sentence kept",
      pa.SUKUK_LADDER_NOTE in s_def["advisor_note"] and " / TP1 " not in s_def["advisor_note"]
      and "stop " not in s_def["advisor_note"].split("sukuk / fixed income")[0]
      and "engine 12M forecast" in s_def["advisor_note"], s_def["advisor_note"])
    T("S2 default: action reason carries the sukuk f1 n/a tag (observe token kept), no plan-3M number",
      "[f1-observe] n/a - sukuk / fixed income (D-9)" in s_def["action_reason"] and "plan 3M ROI" not in s_def["action_reason"]
      and s_def["action_reason"].startswith("Upside"), s_def["action_reason"])
    T("S2 default: roi_pct / engine ROI cells untouched vs legacy",
      s_def["roi_pct"] == s_leg["roi_pct"] and s_def.get("engine_roi_pct") == s_leg.get("engine_roi_pct")
      and s_def.get("valuation_roi_pct") == s_leg.get("valuation_roi_pct"), (s_def["roi_pct"], s_leg["roi_pct"]))
    eq_def = [a for a in p_def["actions"] if a["symbol"] != "5023.SR"]
    eq_leg = [a for a in p_leg["actions"] if a["symbol"] != "5023.SR"]
    T("S2 equity rows: stop cell present, plan-3M observe tag present (incl. the B-7 DATA_GAP form), byte-identical to legacy",
      all(a["stop_sar"] for a in eq_def)
      and all("[f1-observe] plan 3M ROI" in a["action_reason"] for a in eq_def) and eq_def == eq_leg, len(eq_def))
    T("S2 verdicts / capped_from / KPIs / alerts identical default vs legacy vs protect=0",
      _verdicts(p_def) == _verdicts(p_leg) == _verdicts(p_pro) and p_def["kpis"] == p_leg["kpis"] == p_pro["kpis"]
      and p_def["alerts"] == p_leg["alerts"] == p_pro["alerts"] and p_def["meta"]["counts"] == p_leg["meta"]["counts"],
      (_verdicts(p_def), _verdicts(p_leg)))
    if real:
        T("S3 kill-switch reproduces the v1.13.0 row: 92.23 / 102.70 / 105.16 SAR + plan-3M tag + ladder bit",
          (s_leg["stop_sar"], s_leg["tp1_sar"], s_leg["tp2_sar"]) == (92.23, 102.7, 105.16)
          and "[f1-observe] plan 3M ROI 2.4% vs 3.0%; legacy basis kept" in s_leg["action_reason"]
          and "stop 92.2 / TP1 102.7 / TP2 105.2 SAR" in s_leg["advisor_note"]
          and "ladder_display" not in (s_leg.get("detail") or {}), (s_leg["stop_sar"], s_leg["advisor_note"]))
    T("S4 TFB_PA_PROTECT_SUKUK=0: inert (sukuk row == kill-switch row)", s_pro == s_leg)
    d = _diff(p_def, p_leg)
    T("S2/S3 default vs legacy differ ONLY on the sukuk row's six display fields",
      set(d) == {"/actions[%d]/%s" % (p_def["actions"].index(s_def), k)
                 for k in ("stop_sar", "tp1_sar", "tp2_sar", "action_reason", "advisor_note", "detail/ladder_display")}, d)
    digest.append([_verdicts(p_def), s_def["action_reason"], s_def["advisor_note"], s_leg["stop_sar"], s_leg["tp1_sar"], s_leg["tp2_sar"], d])

    # ------------------------------------------------------------- S5 dual-tree
    if base is not None:
        b_def = _build(base, rows)
        T("S5 base reproduces the defect (golden negative): equity ladder + plan-3M tag on the sukuk row",
          _act(b_def, "5023.SR")["stop_sar"] is not None and "plan 3M ROI" in _act(b_def, "5023.SR")["action_reason"])
        T("S5 legacy payload == base payload (versions masked)", _mask_versions(p_leg) == _mask_versions(b_def),
          _diff(_mask_versions(p_leg), _mask_versions(b_def))[:10])
        db = _diff(_mask_versions(p_def), _mask_versions(b_def))
        T("S5 default differs from base only on the sukuk row's six display fields", set(db) == set(d), db)
        for basis in ("legacy", "plan3m"):
            T("S5 %s basis: legacy == base; default touches only the ladder cells + note" % basis,
              _mask_versions(_build(pa, rows, legacy="1", basis=basis)) == _mask_versions(_build(base, rows, basis=basis))
              and set(_diff(_mask_versions(_build(pa, rows, basis=basis)), _mask_versions(_build(base, rows, basis=basis))))
              == {"/actions[%d]/%s" % (p_def["actions"].index(s_def), k)
                  for k in ("stop_sar", "tp1_sar", "tp2_sar", "advisor_note", "detail/ladder_display")})
        digest.append("dual-tree")
    else:
        out.append("SKIP S5 dual-tree (set PA_BASE=<v1.13.0 file>)")

    # ------------------------------------------------------------- S6 basis modes on the delivered tree
    for basis in ("legacy", "plan3m"):
        pd_, pl_ = _build(pa, rows, basis=basis), _build(pa, rows, legacy="1", basis=basis)
        sd, sl = _act(pd_, "5023.SR"), _act(pl_, "5023.SR")
        T("S6 basis=%s: no f1 observe text on either side; verdicts equal; only ladder cells + note bit differ" % basis,
          "[f1-observe]" not in sd["action_reason"] and "[f1-observe]" not in sl["action_reason"]
          and sd["action_reason"] == sl["action_reason"] and _verdicts(pd_) == _verdicts(pl_)
          and sd["stop_sar"] is None and sl["stop_sar"] is not None
          and pa.SUKUK_LADDER_NOTE in sd["advisor_note"] and pa.SUKUK_LADDER_NOTE not in sl["advisor_note"],
          (sd["action_reason"], sl["action_reason"]))
    digest.append("basis-modes")

    # ------------------------------------------------------------- S7 hand fixtures
    h_def = _build(pa, HAND)
    h_leg = _build(pa, HAND, legacy="1")
    nl = _act(h_def, "NOLAD.US")
    T("S7 equity row with stop but no TP (target below price): the v1.7.2 B-7 'no TP ladder' line still prints (elif intact) and == legacy",
      nl is not None and "no TP ladder" in nl["advisor_note"] and nl["stop_sar"] and nl["tp1_sar"] is None
      and nl == _act(h_leg, "NOLAD.US"), (nl or {}).get("advisor_note"))
    hs = _act(h_def, "5023.SR")
    T("S7 hand sukuk row: cells None, note present, verdict == legacy",
      hs["stop_sar"] is None and pa.SUKUK_LADDER_NOTE in hs["advisor_note"] and hs["action"] == _act(h_leg, "5023.SR")["action"])
    # a sukuk whose verdict is not ADD/HOLD (forced BLOCK via a rejected cost basis) prints no ladder note
    entry = {"cand": {"symbol": "5023.SR", "name": "", "fx_to_sar": 1.0, "stop": 92.23, "tp1": 102.7, "tp2": 105.16},
             "action": pa.ACTION_BLOCK, "action_reason": "cost basis rejected", "confidence_band": "Low",
             "proceeds_sar": 0.0}
    _env(None)
    sent = pa._advisor_sentence(entry, pa.make_controls(PANEL), "2026-10-30")
    T("S7 sukuk BLOCK row: neither the ladder bit nor the sukuk note (ADD/HOLD only, as v1.13.0)",
      "BLOCKED" in sent and pa.SUKUK_LADDER_NOTE not in sent and "TP1" not in sent, sent)
    entry["action"] = pa.ACTION_HOLD
    sent_h = pa._advisor_sentence(entry, pa.make_controls(PANEL), "2026-10-30")
    _env("1")
    sent_l = pa._advisor_sentence(entry, pa.make_controls(PANEL), "2026-10-30")
    _env(None)
    T("S7 _advisor_sentence HOLD sukuk: note by default, ladder bit under the kill-switch",
      pa.SUKUK_LADDER_NOTE in sent_h and "TP1" not in sent_h and "stop 92.2 / TP1 102.7 / TP2 105.2 SAR" in sent_l, (sent_h, sent_l))
    digest.append([nl["advisor_note"], sent, sent_h, sent_l])

    # ------------------------------------------------------------- S8
    T("S8 idempotent", _build(pa, rows) == p_def)
    return out


if __name__ == "__main__":
    run_all()
    print("\n".join(out))
    print("RUN-DIGEST", hashlib.sha256(json.dumps(digest, sort_keys=True, default=str).encode()).hexdigest()[:16])
else:
    def test_p153_all():
        run_all()
