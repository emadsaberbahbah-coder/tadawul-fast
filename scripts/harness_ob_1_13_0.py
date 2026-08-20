#!/usr/bin/env python3
"""opportunity_builder v1.13.0 TRUST-001 harness (v1.2.0).
REAL module functions only (normalize_candidate, evaluate_gates,
build_opportunity_payload). Builder imports are stdlib-only (json/logging/
math/os/re/datetime — verified), so OLD/NEW dual-load in one process has
no shared-tree contamination surface; both files are loaded by explicit
path under distinct module names.  Paths via CLI:
    harness_ob_1_13_0.py [OLD_FILE] [NEW_FILE]
    harness_ob_1_13_0.py --require-old

v1.1.0 (2026-08-20) PORTABILITY REBUILD. v1.0 defaulted both paths to
"/home/claude/build/head2/..." and "/home/claude/build/work2/..." — session
-local paths that do not exist in a clean checkout, so the file could not be
run by anyone but its author and failed before its first assertion. NEW now
resolves from THIS FILE; OLD is optional and its absence SKIPS the
differential rather than aborting the run.
"""
import importlib.util, json, os, sys, copy

_ARGV = [a for a in sys.argv[1:] if not a.startswith("--")]
_REQUIRE_OLD = "--require-old" in sys.argv

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(_HERE) if os.path.basename(_HERE) == "scripts" else _HERE
_DEFAULT_NEW = os.path.join(
    _REPO, "core", "analysis", "opportunity_builder.py")

OLD = (_ARGV[0] if len(_ARGV) > 0 else os.environ.get("TFB_OB_OLD", ""))
NEW = (_ARGV[1] if len(_ARGV) > 1
       else os.environ.get("TFB_OB_NEW", "") or _DEFAULT_NEW)

HAVE_OLD = bool(OLD) and os.path.isfile(OLD)
if not os.path.isfile(NEW):
    print("FATAL: NEW builder not found: %r" % NEW)
    sys.exit(2)
print("NEW : %s" % NEW)
print("OLD : %s" % (OLD if HAVE_OLD else
                    "(not supplied — differential suite SKIPPED)"))
if _REQUIRE_OLD and not HAVE_OLD:
    print("  FAIL  CERT: --require-old set but OLD builder not found")
    sys.exit(1)

PASS = FAIL = 0
def check(label, cond, detail=""):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  PASS  {label}")
    else:    FAIL += 1; print(f"  FAIL  {label}  {detail}")

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod; spec.loader.exec_module(mod)
    return mod

def clear_env():
    os.environ.pop("TFB_OPP_TRUST_LINEAGE", None)

# ---- fixtures: sheet-header-shaped body_rows (GAS POST form) -------------
def row(sym, dq, warn="", rel=75.0, price=100.0, roi_target=125.0):
    return {
        "Symbol": sym, "Name": sym + " Co", "Market": "NYSE/NASDAQ",
        "Sector": "Financials", "Currency": "USD",
        "Current Price": price, "Target Price": roi_target,
        "Intrinsic Value": roi_target, "Expected ROI 12M": 0.14,
        "Forecast Reliability Score": rel, "Data Quality Score": dq,
        "Risk Level": "LOW", "Volatility 30D": 20.0,
        "Avg Volume 30D": 2_000_000, "Recommendation": "BUY",
        "Investability Status": "INVESTABLE", "Forecast Source": "provider_target",
        "Last Updated (UTC)": "2026-08-19T05:00:00+00:00",
        "Warnings": warn,
    }

FIX = [
    row("CONTRA.US", 100.0, "quote_exchange_from_suffix; low_data_trust; rank_skipped_low_trust"),
    row("LOWLOW.US", 40.0,  "low_data_trust"),
    row("CLEAN.US", 100.0, "yahoo_enrichment_applied"),
    row("MIDTR.US", 85.0,  "low_data_trust"),
]
CRIT = {"min_dq": 80.0}
FX = {"USD": 3.7528}

def payload(mod, env):
    clear_env()
    if env: os.environ["TFB_OPP_TRUST_LINEAGE"] = env
    out = mod.build_opportunity_payload(copy.deepcopy(FIX), criteria=dict(CRIT),
                                        fx_rates=dict(FX))
    clear_env()
    return out

def strip_meta(p):
    """Non-deterministic / expected-stamp exclusions only: top-level
    version string, meta subtree (compared separately), nothing else."""
    p = json.loads(json.dumps(p, sort_keys=True, default=str))
    p.pop("meta", None)
    p.pop("version", None)          # release stamp — expected delta
    return p

def audit_by_sym(p):
    return { (a.get("symbol") or ""): a
             for a in (p.get("candidates_rows") or []) if isinstance(a, dict) }

print("="*74); print("SECTION 1 — OFF: NEW vs OLD byte-identical outside meta"); print("="*74)
# v1.2.0 (pre-merge audit F8): (a) ambient TFB_OPP_*/TFB_T10_* env must
# never leak into fixture payloads — snapshot & strip here, restored at
# exit; (b) the NEW builder gets an UNCONDITIONAL era floor, previously
# asserted only when an OLD tree was supplied.
_SAVED_ENV = {k: os.environ.pop(k) for k in list(os.environ)
              if k.startswith(("TFB_OPP_", "TFB_T10_"))}
import atexit
atexit.register(lambda: os.environ.update(_SAVED_ENV))
new = load("ob_new", NEW)
def _vt(v):
    try: return tuple(int(x) for x in str(v).split("."))
    except Exception: return (0,)
check("NEW builder version >= 1.13.0 (era floor, unconditional)",
      _vt(new.OPPORTUNITY_BUILDER_VERSION) >= (1, 13, 0),
      str(new.OPPORTUNITY_BUILDER_VERSION))
if HAVE_OLD:
    old = load("ob_old", OLD)
    check("versions OLD=1.12.0 NEW=1.13.0",
          old.OPPORTUNITY_BUILDER_VERSION=="1.12.0" and new.OPPORTUNITY_BUILDER_VERSION=="1.13.0",
          f"{old.OPPORTUNITY_BUILDER_VERSION}/{new.OPPORTUNITY_BUILDER_VERSION}")
    po = payload(old, ""); pn = payload(new, "")
    check("OFF: full payload minus meta byte-identical",
          json.dumps(strip_meta(po), sort_keys=True) == json.dumps(strip_meta(pn), sort_keys=True))
    mo = json.loads(json.dumps(po.get("meta",{}), default=str))
    mn = json.loads(json.dumps(pn.get("meta",{}), default=str))
    for m in (mo, mn):               # expected stamps: version + wall clock
        m.pop("generated_at_utc", None)
        v = m.get("versions")
        if isinstance(v, dict): v.pop("opportunity_builder", None)
    tl = mn.pop("trust_lineage", None)
    check("OFF: meta delta is EXACTLY the trust_lineage subtree",
          json.dumps(mo, sort_keys=True)==json.dumps(mn, sort_keys=True)
          and tl=={"mode":"off","low_trust_rows":0,"contradictions":0}, str(tl))
else:
    # v1.1.0: guard the BODY, not just the banner. Still assert the NEW-tree
    # half so a single-file run certifies the OFF contract it can reach.
    print("  ....  SECTION 1 differential skipped (no OLD builder)")
    pn = payload(new, "")
    mn = json.loads(json.dumps(pn.get("meta",{}), default=str))
    tl = mn.get("trust_lineage")
    check("OFF: NEW trust_lineage subtree is the inert default",
          tl=={"mode":"off","low_trust_rows":0,"contradictions":0}, str(tl))
c_off = new.normalize_candidate(copy.deepcopy(FIX[0]), FX, new.make_criteria({"min_dq":80.0}))
check("OFF: candidate carries NO lineage keys",
      "trust_low_source" not in c_off and "dq_alias_key" not in c_off)

print("="*74); print("SECTION 2 — TAG mode: counters + fields, zero verdict change"); print("="*74)
pt = payload(new, "tag")
check("TAG: rows/kpis/selection identical to OFF",
      json.dumps(strip_meta(pt), sort_keys=True) == json.dumps(strip_meta(pn), sort_keys=True))
tl = pt["meta"]["trust_lineage"]
check("TAG: mode=tag, low_trust_rows=3, contradictions=2",
      tl=={"mode":"tag","low_trust_rows":3,"contradictions":2}, str(tl))
c_tag = new.normalize_candidate(copy.deepcopy(FIX[0]), FX, new.make_criteria({"min_dq":80.0}))
clear_env(); os.environ["TFB_OPP_TRUST_LINEAGE"]="tag"
c_tag = new.normalize_candidate(copy.deepcopy(FIX[0]), FX, new.make_criteria({"min_dq":80.0}))
clear_env()
check("TAG: candidate lineage fields present + alias provenance",
      c_tag.get("trust_low_source") is True
      and c_tag.get("dq_alias_key")=="dataqualityscore"
      and c_tag.get("rel_alias_key")=="forecastreliabilityscore",
      f"{c_tag.get('dq_alias_key')}/{c_tag.get('rel_alias_key')}")

print("="*74); print("SECTION 3 — GATE mode: the contradiction fails MAJOR"); print("="*74)
pg = payload(new, "gate")
ab = audit_by_sym(pg)
ca = ab.get("CONTRA.US", {})
check("GATE: CONTRA.US (low_trust + DQ100) verdict DO_NOT_INVEST",
      (ca.get("verdict") or "")=="DO_NOT_INVEST", str(ca.get("verdict")))

# Gate-list membership contract on the REAL evaluate_gates (first-fail
# ORDER is builder-owned; presence + fail-class is the TRUST-001 contract).
def lineage_gate(sym_row, env):
    clear_env(); os.environ["TFB_OPP_TRUST_LINEAGE"]=env
    cand = new.normalize_candidate(copy.deepcopy(sym_row), FX, new.make_criteria(dict(CRIT)))
    gates = new.evaluate_gates(cand, new.make_criteria(dict(CRIT)))
    clear_env()
    return next((g for g in gates if g.get("gate")=="Trust Lineage"), None)

gc = lineage_gate(FIX[0], "gate")
check("GATE: CONTRA.US gate list CONTAINS Trust Lineage, MAJOR, dq=100 shown",
      gc is not None and gc["fail_class"]=="MAJOR" and "dq=100" in str(gc["current"]), str(gc))
check("GATE: LOWLOW.US (dq40) — NO Lineage gate (DQ gate owns it)",
      lineage_gate(FIX[1], "gate") is None)
check("GATE: CLEAN.US — NO Lineage gate",
      lineage_gate(FIX[2], "gate") is None)
gm = lineage_gate(FIX[3], "gate")
check("GATE: MIDTR.US (dq85 passing) — Lineage gate present, MAJOR",
      gm is not None and gm["fail_class"]=="MAJOR", str(gm))
check("TAG mode: NO Lineage gate appended even on contradiction",
      lineage_gate(FIX[0], "tag") is None)
tl = pg["meta"]["trust_lineage"]
check("GATE: counters mode=gate/3/2", tl=={"mode":"gate","low_trust_rows":3,"contradictions":2}, str(tl))
check("GATE: zero low-trust names selected",
      all((s.get("symbol") or "") not in ("CONTRA.US","MIDTR.US")
          for s in (pg.get("selected") or [])))

print("="*74); print(f"RESULT: {PASS} passed, {FAIL} failed"); print("="*74)
sys.exit(1 if FAIL else 0)
