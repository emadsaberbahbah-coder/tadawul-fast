#!/usr/bin/env python3
"""identity_guard v1.3.0 M-harness — REAL module + the REAL exported Copper
row + the FULL real Commodities_FX page (452 rows) as the false-positive
battery. M1 22-test suite parity | M2 off byte-identical | M3 observe tag-only
on the real Copper row | M4 enforce quarantines it via the existing machinery
| M5 zero false positives across the whole real page + ML page. x3 digest."""
import csv, sys, os, json, copy, hashlib, subprocess
tree = sys.argv[1]
sys.path.insert(0, tree)
from core.analysis import identity_guard as ig

env = dict(os.environ, PYTHONPATH=tree); env.pop("TFB_IDG_SCHEMA_SHIFT", None)
r = subprocess.run([sys.executable, os.path.join(tree, "tests",
                    "test_identity_guard.py")], capture_output=True,
                   text=True, env=env)
n_pass = sum(1 for l in r.stdout.splitlines() if l.startswith("PASS  "))
assert r.returncode == 0 and "ALL PASSED" in r.stdout, r.stdout[-400:]
print(f"M1 PASS  existing standalone suite on {ig.IDENTITY_GUARD_VERSION}: {n_pass} checks, ALL PASSED")

def load(page):
    rows = list(csv.reader(open(f"/mnt/user-data/uploads/_Market_Share_Deepseek-V3_-_{page}.tsv",
                                encoding="utf-8", errors="replace"), delimiter="\t"))
    hdr = [h.strip() for h in rows[0]]
    return [dict(zip(hdr, r)) for r in rows[1:] if len(r) >= 3 and r[0].strip()]

cfx = load("Commodities_FX"); ml = load("Market_Leaders")
copper = [r for r in cfx if r.get("Symbol", "").strip() == "Copper Futures"]
assert len(copper) == 1, "real Copper row not found"

os.environ.pop("TFB_IDG_SCHEMA_SHIFT", None)
sigs = ig.schema_shift_signals(copper[0])
assert set(sigs) == {"symbol_whitespace", "currency_not_code",
                     "exchange_is_currency", "country_is_asset_class"}, sigs
assert ig.schema_shift_suspect(sigs)
p_off = ig.guard_sheet_rows(copy.deepcopy(cfx), sheet="Commodities_FX", run_dedup=False)
c_off = [r for r in p_off.rows if r.get("Symbol") == "Copper Futures"][0]
assert "schema_shift" not in str(c_off.get("Warnings", "")) + str(c_off.get("warnings", ""))
assert not [f for f in p_off.findings if f.reason == ig.Reason.SCHEMA_SHIFT]
print("M2 PASS  off: all four signals detected by the pure fn, guard output untouched")

os.environ["TFB_IDG_SCHEMA_SHIFT"] = "observe"
p_obs = ig.guard_sheet_rows(copy.deepcopy(cfx), sheet="Commodities_FX", run_dedup=False)
c_obs = [r for r in p_obs.rows if r.get("Symbol") == "Copper Futures"][0]
w = str(c_obs.get("Warnings", "")) + str(c_obs.get("warnings", ""))
assert "schema_shift_symbol_whitespace+currency_not_code+exchange_is_currency+country_is_asset_class:observe" in w, w
assert not [f for f in p_obs.findings if f.reason == ig.Reason.SCHEMA_SHIFT]
assert c_obs.get("Name") == copper[0].get("Name"), "observe must not clear fields"
print("M3 PASS  observe: real Copper row tagged (all four signals in the stamp), values + plan untouched")

os.environ["TFB_IDG_SCHEMA_SHIFT"] = "enforce"
p_enf = ig.guard_sheet_rows(copy.deepcopy(cfx), sheet="Commodities_FX", run_dedup=False)
ss = [f for f in p_enf.findings if f.reason == ig.Reason.SCHEMA_SHIFT]
assert len(ss) == 1 and ss[0].symbol.upper() == "COPPER FUTURES" and ss[0].action == ig.Action.QUARANTINE_FIELDS, ss
assert "COPPER FUTURES" in [s.upper() for s in p_enf.refetch_symbols()]
c_enf = [r for r in p_enf.rows if r.get("Symbol") == "Copper Futures"][0]
assert c_enf.get("Name") in (None, ""), "Title-Case Name must be cleared on the v1.0.0 path"
assert c_enf.get("Investability Status") == "BLOCKED"
base_reasons = {f.symbol for f in p_enf.findings if f.reason != ig.Reason.SCHEMA_SHIFT
                and f.action == ig.Action.QUARANTINE_FIELDS}
print("M4 PASS  enforce: Copper quarantined via existing machinery (fields cleared, BLOCKED, refetch-queued)")

p_ml = ig.guard_sheet_rows(copy.deepcopy(ml), sheet="Market_Leaders", run_dedup=False)
ml_ss = [f for f in p_ml.findings if f.reason == ig.Reason.SCHEMA_SHIFT]
assert ml_ss == [], ml_ss
cfx_ss_syms = {f.symbol for f in p_enf.findings if f.reason == ig.Reason.SCHEMA_SHIFT}
assert {s.upper() for s in cfx_ss_syms} == {"COPPER FUTURES"}
os.environ.pop("TFB_IDG_SCHEMA_SHIFT", None)
print(f"M5 PASS  zero false positives: CFX {len(cfx)} rows -> exactly the Copper row; ML {len(ml)} rows -> none")
print("RUN-DIGEST", hashlib.sha256(json.dumps([sigs, sorted(cfx_ss_syms), len(cfx), len(ml)]).encode()).hexdigest()[:16])
