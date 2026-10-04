#!/usr/bin/env python3
"""tests/test_export_audit_v1.py - harness for scripts/tfb_export_audit.py v1.0.0.

v1.0.1 (2026-10-04) REBUILD: the 10-03 delivery (sha 15822e74...) never
reached HEAD - the file committed under this name was a byte-copy of the
script itself (D7, Monitoring Sheet #7). This harness is a fresh build with
the same contract: it runs the REAL script as a subprocess (no stand-ins).

Contract:
  P1  --selftest exits 0 and prints "46/46 PASS cases-digest=3de969895c97a7d6"
  P2  CLI: no args -> rc 2; missing file -> rc 2; --help -> rc 0
  P3  determinism: two --selftest runs print identical digests
  P4  the script is not this file (D7 guard): different sha256, and the
      script declares SCRIPT_VERSION = "1.0.0"
Runs under pytest or directly (python tests/test_export_audit_v1.py).
"""
import hashlib
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "..", "scripts", "tfb_export_audit.py")
EXPECT = "46/46 PASS  cases-digest=3de969895c97a7d6"


def _run(*args):
    p = subprocess.run([sys.executable, SCRIPT] + list(args),
                       capture_output=True, text=True, timeout=300)
    return p.returncode, (p.stdout or "") + (p.stderr or "")


def test_p1_selftest_passes():
    rc, out = _run("--selftest")
    assert rc == 0, out[-800:]
    assert EXPECT in out, out[-400:]


def test_p2_cli_contract():
    rc, _ = _run()
    assert rc == 2
    rc, _ = _run(os.path.join(HERE, "no_such_export.xlsx"))
    assert rc == 2
    rc, out = _run("--help")
    assert rc == 0 and "--selftest" in out


def test_p3_selftest_deterministic():
    _, a = _run("--selftest")
    _, b = _run("--selftest")
    da = re.search(r"cases-digest=([0-9a-f]{16})", a)
    db = re.search(r"cases-digest=([0-9a-f]{16})", b)
    assert da and db and da.group(1) == db.group(1) == "3de969895c97a7d6"


def test_p4_not_a_copy_of_the_script():
    with open(SCRIPT, "rb") as f:
        script = f.read()
    with open(os.path.abspath(__file__), "rb") as f:
        me = f.read()
    assert hashlib.sha256(script).hexdigest() != hashlib.sha256(me).hexdigest()
    assert b'SCRIPT_VERSION = "1.0.0"' in script
    assert b"def test_" not in script


if __name__ == "__main__":
    tests = [test_p1_selftest_passes, test_p2_cli_contract,
             test_p3_selftest_deterministic, test_p4_not_a_copy_of_the_script]
    fails = 0
    for t in tests:
        try:
            t()
            print("PASS", t.__name__)
        except AssertionError as exc:
            fails += 1
            print("FAIL", t.__name__, "::", str(exc)[:300])
    print("[EXPORT-AUDIT HARNESS v1.0.1] %d/%d PASS" % (len(tests) - fails, len(tests)))
    sys.exit(1 if fails else 0)
