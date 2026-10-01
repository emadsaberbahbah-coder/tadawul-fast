#!/usr/bin/env python3
"""tests/test_ob_nearmiss_text_p171.py — opportunity_builder v1.22.2
[P-171 / P-149 NEAR-MISS TEXT TRUTH]. REAL module end-to-end
(build_opportunity_payload on today-dated fixtures; no stand-ins). Dual-tree:
set OB_BASE=<path to the v1.22.1 file> to also prove the kill switch is
byte-identical to the base and that the default changes ONLY the two text
surfaces (near-miss rows + deferral strings), never selection/KPIs/alerts.

N1 helpers: _effective_min_ticket operator vs venue; kill-switch reader
N2 P-171 default: BBOX.L deferral names the .L venue floor; NEAR MISS
   Required prints 27,700 (venue) not 1,000 (operator); token contract kept
N3 P-149 default: held DDI.US (INVEST, structural Portfolio gate) is a
   "Portfolio / held" near-miss, not "Capacity / rank beyond Max Selected"
N4 legacy=1: v1.22.1 strings byte-for-byte (1,000 SAR; Capacity)
N5 venue floors OFF: operator floor text unchanged in both modes
N6 dual-tree (OB_BASE): legacy payload == base payload; default payload
   differs from base only in near_miss[] and deferral strings
N7 idempotence ×2
"""
import copy, hashlib, importlib, importlib.util, json, os, sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
ob = importlib.import_module("core.analysis.opportunity_builder")
assert ob.OPPORTUNITY_BUILDER_VERSION == "1.23.0", ob.OPPORTUNITY_BUILDER_VERSION

NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
FX = {"USD": 3.7555, "SAR": 1.0, "GBX": 0.0498}
CRIT = {"period_months": 3, "required_roi_pct": 12, "required_ann_roi_pct": 10,
        "min_reliability": 70, "min_dq": 80, "min_rr": 2, "max_per_sector": 2,
        "max_per_market": 10, "max_selected": 3, "include_portfolio_holdings": False,
        "min_ticket_sar": 1000.0, "near_miss_n": 12}
ENVS = ("TFB_OPP_VENUE_FLOORS", "TFB_OPP_NEARMISS_TEXT_LEGACY")


def _row(symbol, price, target, sector, currency="USD", market="NYSE/NASDAQ", rel=76.5):
    return {"Symbol": symbol, "Name": symbol + " plc", "Sector": sector, "Market": market,
            "Currency": currency, "Current Price": price, "Target Price": target,
            "Expected ROI 12M": round((target / price - 1.0) * 100.0, 2),
            "Forecast Reliability Score": rel, "Data Quality Score": 100.0,
            "Risk Bucket": "Low", "Investability Status": "INVESTABLE",
            "Final Action": "INVEST", "Recommendation": "BUY", "Volatility 30D": 2.0,
            "Forecast Source": "provider_target", "Last Updated (UTC)": NOW}


ROWS = [_row("BBOX.L", 146.30, 193.50, "Real Estate", currency="GBX", market="LSE"),
        _row("RDN.US", 32.75, 43.70, "Financials"),
        _row("PINE.US", 17.32, 21.95, "Real Estate"),
        _row("NVDA.US", 227.21, 306.00, "Information Technology"),
        _row("DDI.US", 13.06, 17.50, "Communication Services"),
        _row("VLY.US", 12.72, 16.66, "Financials")]
PF = {"cash_available_sar": 34166.25,
      "holdings": [{"symbol": "DDI.US", "sector": "Communication Services", "market": "NYSE/NASDAQ", "value_sar": 11232.0},
                   {"symbol": "YUM", "sector": "Consumer Discretionary", "market": "NYSE/NASDAQ", "value_sar": 12414.0}]}


def _env(venue="1", legacy=None):
    for k in ENVS:
        os.environ.pop(k, None)
    if venue is not None:
        os.environ["TFB_OPP_VENUE_FLOORS"] = venue
    if legacy is not None:
        os.environ["TFB_OPP_NEARMISS_TEXT_LEGACY"] = legacy


def _build(mod, venue="1", legacy=None):
    _env(venue, legacy)
    try:
        p = mod.build_opportunity_payload([copy.deepcopy(r) for r in ROWS], criteria=dict(CRIT),
                                          portfolio=copy.deepcopy(PF), fx_rates=dict(FX))
    finally:
        _env(None)
    p = json.loads(json.dumps(p, default=str, sort_keys=True))
    (p.get("meta") or {}).pop("generated_at_utc", None)
    return p


def _nm(p, sym):
    return [r for r in p["near_miss"] if r["symbol"] == sym]


def _def(p, sym):
    for r in p["candidates_rows"]:
        if r["symbol"] == sym:
            return r.get("deferral")
    return None


out = []


def T(name, cond, detail=""):
    out.append(("PASS " if cond else "FAIL ") + name + (" | " + str(detail) if detail else ""))
    if not cond:
        raise AssertionError(name + " " + str(detail))


# N1
_env("1", None)
assert ob._effective_min_ticket("BBOX.L", {"min_ticket_sar": 1000.0}) == (27700.0, "venue:.L")
assert ob._effective_min_ticket("RDN.US", {"min_ticket_sar": 1000.0}) == (5000.0, "venue:.US")
assert ob._effective_min_ticket("RDN.US", {"min_ticket_sar": 9000.0}) == (9000.0, "operator")
_env("0", None)
assert ob._effective_min_ticket("BBOX.L", {"min_ticket_sar": 1000.0}) == (1000.0, "operator")
assert ob._effective_min_ticket("BBOX.L", {}) == (0.0, "operator")
assert ob._env_nearmiss_text_legacy() is False
os.environ["TFB_OPP_NEARMISS_TEXT_LEGACY"] = "1"; assert ob._env_nearmiss_text_legacy() is True
_env(None)
T("N1 helpers: effective floor operator/venue; kill reader", True)

# N2 / N3 default
p = _build(ob)
T("N2 build ok, 3 seats funded", p.get("status") != "error" and len(p["selected"]) == 3, [t["symbol"] for t in p["selected"]])
d_bbox = _def(p, "BBOX.L") or ""
T("N2 BBOX.L deferred sub-floor with venue note",
  "below minimum ticket floor 27,700 SAR (.L venue floor; operator floor 1,000 SAR)" in d_bbox, d_bbox)
nm_b = _nm(p, "BBOX.L")
T("N2 BBOX.L near-miss = Funding with the venue floor in Required",
  len(nm_b) == 1 and nm_b[0]["failed_gate"] == "Funding"
  and nm_b[0]["required"] == "fundable amount ≥ .L venue floor (27,700 SAR; operator floor 1,000 SAR)"
  and "venue floor" in nm_b[0]["improve_note"], nm_b)
T("N2 token contract kept ('minimum ticket floor' substring)", "minimum ticket floor" in d_bbox)
nm_d = _nm(p, "DDI.US")
T("N3 DDI.US (held, structural) = Portfolio near-miss, not Capacity",
  len(nm_d) == 1 and nm_d[0]["failed_gate"] == "Portfolio" and nm_d[0]["current"] == "held"
  and nm_d[0]["required"] == "exclude holdings (Include Portfolio Holdings = No)"
  and "held position" in nm_d[0]["improve_note"], nm_d)
T("N3 no Capacity label on the held row", all(r["failed_gate"] != "Capacity" for r in nm_d))

# N4 legacy
pl = _build(ob, "1", "1")
d_bbox_l = _def(pl, "BBOX.L") or ""
T("N4 legacy deferral has no venue note", "below minimum ticket floor 27,700 SAR" in d_bbox_l and "venue floor" not in d_bbox_l, d_bbox_l)
nm_bl = _nm(pl, "BBOX.L")
T("N4 legacy Required prints the operator floor (the P-171 defect)",
  nm_bl and nm_bl[0]["required"] == "fundable amount ≥ minimum ticket floor (1,000 SAR)", nm_bl)
nm_dl = _nm(pl, "DDI.US")
T("N4 legacy DDI = Capacity (the P-149 defect)",
  nm_dl and nm_dl[0]["failed_gate"] == "Capacity" and nm_dl[0]["current"] == "rank beyond Max Selected", nm_dl)
T("N4 legacy vs default: selection, kpis, alerts identical",
  [t["symbol"] for t in pl["selected"]] == [t["symbol"] for t in p["selected"]]
  and pl["kpis"] == p["kpis"] and pl["alerts"] == p["alerts"])

# N5 venue floors OFF: operator floor 1,000 -> BBOX ticket (≈6.8k) is above it -> funded or capacity, no floor text
p0 = _build(ob, "0", None)
p0l = _build(ob, "0", "1")
T("N5 venue floors off: no venue text anywhere, both modes",
  "venue floor" not in json.dumps(p0) and "venue floor" not in json.dumps(p0l))
T("N5 venue floors off: near-miss rows identical except the DDI branch",
  [r for r in p0["near_miss"] if r["symbol"] != "DDI.US"] == [r for r in p0l["near_miss"] if r["symbol"] != "DDI.US"])

# N6 dual-tree
base_path = os.environ.get("OB_BASE")
if base_path and os.path.exists(base_path):
    spec = importlib.util.spec_from_file_location("ob_base_v1221", base_path)
    obb = importlib.util.module_from_spec(spec); spec.loader.exec_module(obb)
    assert obb.OPPORTUNITY_BUILDER_VERSION == "1.22.1"
    pb = _build(obb, "1", None)
    pl2 = _build(ob, "1", "1")
    for _p in (pb, pl2):
        _p.pop("version", None)                       # top-level builder version
        for k in list(_p.get("meta") or {}):
            if "version" in k: _p["meta"].pop(k, None)
    T("N6 legacy payload == base payload (version fields masked)", pb == pl2,
      "" if pb == pl2 else json.dumps([k for k in pb if pb[k] != pl2.get(k)]))
    pd = _build(ob, "1", None)
    pd.pop("version", None)
    for k in list(pd.get("meta") or {}):
        if "version" in k: pd["meta"].pop(k, None)
    diff_keys = [k for k in pb if pb[k] != pd.get(k)]
    T("N6 default differs from base only in near_miss + candidates_rows(deferral text)",
      set(diff_keys) <= {"near_miss", "candidates_rows"}, diff_keys)
    changed = [r["symbol"] for r, rb in zip(pd["candidates_rows"], pb["candidates_rows"]) if r != rb]
    T("N6 only BBOX.L's candidate row text changed", changed == ["BBOX.L"], changed)
    T("N6 selection/kpis/alerts equal base", pd["selected"] == pb["selected"] and pd["kpis"] == pb["kpis"] and pd["alerts"] == pb["alerts"])
else:
    out.append("SKIP N6 dual-tree (set OB_BASE=<v1.22.1 file>)")

# N7 idempotence
T("N7 idempotent", _build(ob) == _build(ob))

print("\n".join(out))
dig = hashlib.sha256(json.dumps([out, p["near_miss"], _def(p, "BBOX.L")], sort_keys=True).encode()).hexdigest()[:16]
print("RUN-DIGEST", dig)
