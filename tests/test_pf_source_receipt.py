"""10_My_Portfolio v1.5.11 receipt, checked against the real final audit parser.

Runs tests/js/pf_source_receipt.test.js (the REAL Apps Script source in a vm,
synthetic data) and feeds every status line it produced to the parsers that
scripts/audit_decision_surface_freshness.py applies to Portfolio_Decision B2:
- enforce + ready publication -> exactly the My_Portfolio run is consumed;
- off, observe and every withheld case -> no consumed run.
The whole-suite CI job collects this file, so the native harness runs there too.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from scripts.audit_decision_surface_freshness import _consumed_source_run, parse_surface_status

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "js" / "pf_source_receipt.test.js"
GAS = ROOT / "apps_script" / "10_My_Portfolio.gs"
RUN = "4242424242"


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


@pytest.fixture(scope="module")
def dump(tmp_path_factory):
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    out = tmp_path_factory.mktemp("pf_receipt") / "dump.json"
    env = dict(os.environ, PF_DUMP=str(out), PF_GAS=str(GAS))
    env.pop("PF_WRITE_GOLDEN", None)
    proc = subprocess.run([node, str(HARNESS)], env=env, capture_output=True, text=True,
                          timeout=180)
    assert proc.returncode == 0, (proc.stdout[-2000:], proc.stderr[-2000:])
    assert "PASS 8/8 digest" in proc.stdout, proc.stdout[-2000:]
    return json.loads(out.read_text(encoding="utf-8"))


def test_enforce_receipt_is_consumed_by_the_real_audit_parser(dump):
    line = dump["enforce"]
    assert _consumed_source_run(line) == RUN
    stamp, status = parse_surface_status(line)
    assert stamp is not None and status == "OK"


def test_off_and_observe_publish_no_receipt(dump):
    for key in ("off", "observe"):
        assert _consumed_source_run(dump[key]) is None, key
        assert parse_surface_status(dump[key])[1] == "OK", key


def test_every_withheld_line_is_rejected_by_the_real_audit_parser(dump):
    assert len(dump["withheld"]) >= 20
    for label, line in dump["withheld"].items():
        assert _consumed_source_run(line) is None, label
        assert parse_surface_status(line)[0] is not None, label
