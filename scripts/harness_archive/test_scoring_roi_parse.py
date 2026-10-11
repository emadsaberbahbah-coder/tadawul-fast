#!/usr/bin/env python3
"""scoring v5.11.2 P-harness — REAL module, dual-tree.
P1 kill switch = v5.11.1 byte-identical across the battery (defect incl.)
P2 default: explicit-%% strings single-divided; bare numerics unchanged
P3 defect reproduced on base ("200%" -> 0.02) and closed on revised (2.0)
P4 non-string / junk / None edges identical both trees. x3 same digest."""
import sys, os, json, hashlib
def load(tree):
    for m in list(sys.modules):
        if m.startswith("core"): del sys.modules[m]
    sys.path.insert(0, tree); import core.scoring as sc; sys.path.pop(0); return sc
BAT = ["200%", "\u25b2 150%", "50%", "-150%", "  75 %".replace(" %","%"),
       250.0, 34.0, 1.0, 0.105, -0.4, "n/a", None, "junk", True]
os.environ.pop("TFB_SCORING_ROI_PARSE_LEGACY", None)
base = load("sb"); assert base.__version__ == "5.11.1"
rev  = load("sr"); assert rev.__version__ == "5.11.2"
def run(m): return [m._as_roi_fraction(v) for v in BAT]

# P1 kill = base-identical
os.environ["TFB_SCORING_ROI_PARSE_LEGACY"] = "1"
kb, kr = run(base), run(rev)
assert kb == kr, (kb, kr)
assert kr[0] == 0.02, "kill must preserve the defect (200% -> 0.02)"
os.environ.pop("TFB_SCORING_ROI_PARSE_LEGACY", None)
print("P1 PASS  kill switch: full battery identical to v5.11.1 (defect preserved: '200%' -> 0.02)")

# P2 + P3
rb, rr = run(base), run(rev)
assert rb[0] == 0.02 and rr[0] == 2.0, (rb[0], rr[0])
assert abs(rr[1] - 1.5) < 1e-12 and rb[1] == 0.015
assert rr[2] == 0.5 == rb[2]
assert rr[3] == -1.5 and rb[3] == -0.015
assert rr[4] == 0.75 == rb[4]
assert rr[5:] == rb[5:] == [2.5, 0.34, 1.0, 0.105, -0.4, None, None, None, None]
print("P2 PASS  default: '200%'->2.0, '\u25b2 150%'->1.5, '-150%'->-1.5; '50%'/'75%' and every bare numeric unchanged")
print("P3 PASS  defect reproduced on base (0.02 / 0.015 / -0.015) and closed on revised")
print("P4 PASS  None / 'n/a' / 'junk' / bool edges identical both trees")

# P5 [same-class extension]: _as_upside_fraction explicit-%% guard
os.environ.pop("TFB_SCORING_ROI_PARSE_LEGACY", None)
UB = ["300%", "40%", 300.0, 1.8, "▲ 260%", None]
ub, ur = [base._as_upside_fraction(v) for v in UB], [rev._as_upside_fraction(v) for v in UB]
assert ub[0] == 0.03 and ur[0] == 3.0, (ub[0], ur[0])
assert ub[1] == ur[1] == 0.4
assert ub[2] == ur[2] == 3.0 and ub[3] == ur[3] == 1.8 and ub[5] is None and ur[5] is None
assert abs(ub[4] - 0.026) < 1e-12 and abs(ur[4] - 2.6) < 1e-12
os.environ["TFB_SCORING_ROI_PARSE_LEGACY"] = "1"
assert [rev._as_upside_fraction(v) for v in UB] == ub, "kill must restore base upside parse"
os.environ.pop("TFB_SCORING_ROI_PARSE_LEGACY", None)
print("P5 PASS  upside: '300%'->3.0 / '▲ 260%'->2.6 (base 0.03/0.026); '40%', bare 300.0/1.8 unchanged; kill restores base")
print("RUN-DIGEST", hashlib.sha256(json.dumps([kb, rr], default=str).encode()).hexdigest()[:16])
