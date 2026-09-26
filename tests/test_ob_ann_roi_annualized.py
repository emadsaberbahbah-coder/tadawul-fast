#!/usr/bin/env python3
"""opportunity_builder v1.19.6 O-harness — REAL module, dual-tree.
O1 kill-switch = byte-identical v1.19.5 copy | O2 default: audit Ann ROI
uses the ticket-path compound formula on the plan horizon (KRP live case:
13.8% -> 67.7%) | O3 None-TP1 stays None with the DATA_GAP note | O4
roi_pct / basis untouched by the fix | x3 identical digest."""
import sys, os, math, json, hashlib, importlib
def load(tree):
    for m in list(sys.modules):
        if m.startswith("core"): del sys.modules[m]
    sys.path.insert(0, tree)
    import core.analysis.opportunity_builder as ob
    sys.path.pop(0)
    return ob
def rec_for(tp1, price, eng):
    return {"_cand": {"tp1": tp1, "price": price}, "engine_roi_pct": eng,
            "roi_pct": None, "ann_roi_pct": None}
CRIT = {"primary_roi_basis": "plan", "period_months": 3}

os.environ.pop("TFB_OPP_ANN_LEGACY_COPY", None)
os.environ.pop("TFB_OPP_AUDIT_ROI_LEGACY", None)
base = load("ob_b"); assert base.OPPORTUNITY_BUILDER_VERSION == "1.19.5"
rev  = load("ob_r"); assert rev.OPPORTUNITY_BUILDER_VERSION == "1.19.6"

# O1 kill switch === base copy
os.environ["TFB_OPP_ANN_LEGACY_COPY"] = "1"
rb = base._audit_align_plan_roi(rec_for(14.90*1.138, 14.90, 27.5), dict(CRIT))
rr = rev._audit_align_plan_roi(rec_for(14.90*1.138, 14.90, 27.5), dict(CRIT))
assert rb["ann_roi_pct"] == rr["ann_roi_pct"] == rb["roi_pct"] == 13.8, (rb, rr)
os.environ.pop("TFB_OPP_ANN_LEGACY_COPY", None)
print("O1 PASS  kill switch: v1.19.5 copy byte-identical (ann == roi == 13.8)")

# O2 default: the live KRP case
r = rev._audit_align_plan_roi(rec_for(14.90*1.138, 14.90, 27.5), dict(CRIT))
days = 3 * rev.DAYS_PER_MONTH
want = round((math.pow(1.138, 365.0/days) - 1.0) * 100.0, 1)
assert r["roi_pct"] == 13.8 and r["ann_roi_pct"] == want, (r, want)
assert 67.0 <= r["ann_roi_pct"] <= 68.5, r["ann_roi_pct"]   # the board's 67.7
b = base._audit_align_plan_roi(rec_for(14.90*1.138, 14.90, 27.5), dict(CRIT))
assert b["ann_roi_pct"] == 13.8, "base must still show the defect"
print(f"O2 PASS  default: 13.8% plan -> {r['ann_roi_pct']}% annualized (ticket-path formula, {days:.0f}d); base still copies 13.8")

# O3 no TP1 ladder
r3 = rev._audit_align_plan_roi(rec_for(None, 14.90, 27.5), dict(CRIT))
assert r3["roi_pct"] is None and r3["ann_roi_pct"] is None
assert r3.get("roi_basis_note") == "TP1_UNAVAILABLE(DATA_GAP)"
print("O3 PASS  no ladder: None/None + DATA_GAP note preserved")

# O4 non-plan basis untouched; pure helper edges
r4 = rev._audit_align_plan_roi(rec_for(10, 10, 5), {"primary_roi_basis": "valuation"})
assert r4["ann_roi_pct"] is None and r4["roi_pct"] is None
assert rev._ann_from_plan_roi(None, CRIT) is None
assert rev._ann_from_plan_roi(-100.0, CRIT) == -100.0
assert rev._ann_from_plan_roi(0.0, CRIT) == 0.0
print("O4 PASS  non-plan basis inert; helper edges (None/-100/0) fail-soft")

print("RUN-DIGEST", hashlib.sha256(json.dumps([r["ann_roi_pct"], want, days]).encode()).hexdigest()[:16])
