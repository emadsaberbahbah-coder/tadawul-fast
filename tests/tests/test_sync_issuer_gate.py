#!/usr/bin/env python3
"""tests/test_sync_issuer_gate.py — CG-6 activation + registry proof.

The decisive case: a SUBPROCESS running the LITERAL production command shape
(`python scripts/run_dashboard_sync.py`) with TFB_SYNC_IDENTITY_SELFTEST=1
must exit 0 and print the full 15/15 marker — proving the issuer checks are
armed in-process under direct file execution, the exact path PR #32's
monkeypatch could never reach. Marker-required per verify v1.0.16(C).
Plus direct registry spot-checks through the package import path.
"""
import os
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def test_subprocess_activation_on_production_command():
    env = dict(os.environ, TFB_SYNC_IDENTITY_SELFTEST="1")
    p = subprocess.run(
        [sys.executable, "scripts/run_dashboard_sync.py"],
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=180)
    out = p.stdout + p.stderr
    assert p.returncode == 0, out
    m = re.search(r"IDENTITY SELFTEST (\d+)/(\d+)", out)
    assert m and m.group(1) == m.group(2) and int(m.group(2)) >= 15, out
    for marker in (
        "live case: BF-A carrying Biofrontera refused",
        "live case: DENN.US carrying Amer Sports refused",
        "FW-2 ON: poisoned issuer row stripped",
        "kill-switch OFF: issuer row NOT stripped",
    ):
        assert f"PASS [IDENTITY-SELFTEST" in out and marker in out, marker


def test_registry_direct():
    from scripts.critical_symbol_identity import (
        identity_contradiction, row_identity_contradiction, POLICY_VERSION)
    assert POLICY_VERSION == "1.1.0"
    assert identity_contradiction("BF-A", "Biofrontera Inc")
    assert identity_contradiction("BF-A", "Brown-Forman Corporation") == ""
    assert identity_contradiction("DENN.US", "Denny's Corporation") == ""
    hdr = ["Symbol", "Name", "Exchange", "Currency", "Country"]
    assert row_identity_contradiction(hdr, ["TAQA.AB", "Abu Dhabi National Energy",
                                            "NASDAQ", "USD", ""]) != ""
    assert row_identity_contradiction(hdr, ["TAQA.AB", "Abu Dhabi National Energy",
                                            "ADX", "AED", "UAE"]) == ""


if __name__ == "__main__":
    test_registry_direct()
    test_subprocess_activation_on_production_command()
    print("SELFTEST wrapper PASS — subprocess activation + registry proven")
