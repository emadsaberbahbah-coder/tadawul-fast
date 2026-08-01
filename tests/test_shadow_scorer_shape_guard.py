#!/usr/bin/env python3
"""tests/test_shadow_scorer_shape_guard.py — v1.6.0 shape guard, subprocess.

Drives the module's own extended selftest through the LITERAL production
invocation style (`python scripts/run_shadow_scorer.py --selftest`) from the
repo root, asserting: exit 0, the full 80/80 count, and the presence of the
eight SG checks including the named anti-Defect-A case and both S-1
input-equality proofs. Marker-required, not exit-code-only (verify_deployment
v1.0.16(C) rule)."""
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_selftest_subprocess_80_of_80():
    p = subprocess.run(
        [sys.executable, "scripts/run_shadow_scorer.py", "--selftest"],
        cwd=ROOT, capture_output=True, text=True, timeout=180)
    out = p.stdout
    assert p.returncode == 0, out + p.stderr
    m = re.search(r"SELFTEST (\d+)/(\d+)", out)
    assert m and m.group(1) == m.group(2), "selftest not fully green"
    assert int(m.group(2)) >= 80, f"expected >=80 checks, saw {m.group(2)}"
    for marker in (
        "SG: guard ON keeps exactly the 10 symbol rows",
        "SG: guard ON drops exactly the 6 live meta strings",
        "SG: anti-Defect-A",
        "SG: every live meta first-cell rejected",
        "SG: EQW over guarded rows = 10 real names (was 16)",
        "SG: S-1 unmoved — challenger set identical on/off",
        "SG: S-1 unmoved — criterion-2 violations identical",
        "SG: kill-switch OFF -> v1.5.0 list byte-for-byte",
    ):
        assert f"PASS {marker}" in out, f"missing/failed: {marker}"


if __name__ == "__main__":
    test_selftest_subprocess_80_of_80()
    print("SELFTEST subprocess wrapper PASS")
