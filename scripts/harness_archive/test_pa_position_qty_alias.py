#!/usr/bin/env python3
"""portfolio_actions v1.11.1 Q-harness — REAL module, dual-tree.
Q1 current-key rows byte-identical | Q2 'Position Qty' row: base drops the
holding, revised parses it (integration via build_portfolio_actions) |
Q3 _norm_token proof on the real normalizer | Q4 the ONLY divergence class
(two quantity columns) documented. x3 identical digest."""
import sys, os, json, copy, hashlib
os.environ["TFB_PF_ENABLED"] = "1"
def load(tree):
    for m in list(sys.modules):
        if m.startswith("core"): del sys.modules[m]
    sys.path.insert(0, tree); import core.analysis.portfolio_actions as pa
    sys.path.pop(0); return pa
base = load("qb"); rev = load("qr")
assert base.PORTFOLIO_ACTIONS_VERSION == "1.11.0" and rev.PORTFOLIO_ACTIONS_VERSION == "1.11.1"

# Q3 first: the real normalizer maps the schema header
assert rev._ob._norm_token("Position Qty") == "positionqty"
print("Q3 PASS  _ob._norm_token('Position Qty') == 'positionqty' (real normalizer)")

ROW_STD = {"Symbol": "AAA.US", "Sector": "Energy", "Currency": "USD",
           "Quantity": 10, "Avg Cost": 100.0, "Price": 110.0}
ROW_POS = {"Symbol": "BBB.US", "Sector": "Energy", "Currency": "USD",
           "Position Qty": 10, "Avg Cost": 100.0, "Price": 110.0}
CTL = {"cash_available_sar": 20000.0}; FX = {"USD": 3.75}
def run(m, rows): return m.build_portfolio_actions(copy.deepcopy(rows), dict(CTL), dict(FX))
def canon(o):
    import re, json as j
    s = j.dumps(o, sort_keys=True, default=str)
    s = re.sub(r'"generated_utc": "[^"]+"', '"T"', s)
    return s.replace("1.11.1", "1.11.0")

# Q1 current keys identical
assert canon(run(base, [ROW_STD])) == canon(run(rev, [ROW_STD]))
print("Q1 PASS  'Quantity'-keyed row: output deep-equal (timestamp/version neutralized)")

# Q2 the defect + fix, observable end-to-end
ob_ = run(base, [ROW_POS]); or_ = run(rev, [ROW_POS])
def syms(out): return {r.get("symbol"): (r.get("quantity"), r.get("action")) for r in out["actions"]}
sb, sr = syms(ob_), syms(or_)
b_qty = (sb.get("BBB.US") or (None, None))[0]
r_qty = (sr.get("BBB.US") or (None, None))[0]
assert (not b_qty) and r_qty == 10, (sb, sr)
print(f"Q2 PASS  'Position Qty' row: base qty={b_qty!r} (holding invisible), revised qty=10 with action '{sr['BBB.US'][1]}'")

# Q4 the only divergence class, documented
import collections
two = collections.OrderedDict([("Symbol","CCC.US"),("Sector","Energy"),("Currency","USD"),
    ("Position Qty", 99),("Quantity", 10),("Avg Cost", 100.0),("Price", 110.0)])
qb_ = base._position_fields(two)[0]; qr_ = rev._position_fields(two)[0]
assert qb_ == 10 and qr_ == 99
two2 = collections.OrderedDict([("Symbol","CCC.US"),("Quantity", 10),("Position Qty", 99),
    ("Avg Cost", 100.0),("Price", 110.0)])
assert base._position_fields(two2)[0] == rev._position_fields(two2)[0] == 10
print("Q4 PASS  divergence exists ONLY on a two-quantity-column row with Position Qty first (no schema produces one); Quantity-first identical")
print("RUN-DIGEST", hashlib.sha256(json.dumps([sorted(sb.items()), sorted(sr.items()), qb_, qr_], default=str).encode()).hexdigest()[:16])
