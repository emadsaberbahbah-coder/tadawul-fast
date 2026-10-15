"""10_My_Portfolio v1.5.14: Portfolio_Decision panel inputs survive a rebuild.

Runs tests/js/pf_panel_preserve.test.js (the REAL Apps Script source in a vm,
cell-level sheet model, synthetic holdings) and checks what it produced:
- enforce: the auto-build inside refreshPortfolioDecision POSTs the typed
  cash and Max Position, not the defaults;
- off (default): the POST controls are the v1.5.13 golden (cash 0, Max
  Position 15 - the reset the 2026-10-15 export shows);
- observe: logs which inputs the rebuild reset and changes nothing.
The whole-suite CI job collects this file, so the native harness runs there.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "js" / "pf_panel_preserve.test.js"
GAS = ROOT / "apps_script" / "10_My_Portfolio.gs"
GOLDEN = ROOT / "tests" / "fixtures" / "pf_panel_v1_5_13_golden.json"


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
    out = tmp_path_factory.mktemp("pf_panel") / "dump.json"
    env = dict(os.environ, PF_DUMP=str(out), PF_GAS=str(GAS))
    env.pop("PF_WRITE_GOLDEN", None)
    proc = subprocess.run([node, str(HARNESS)], env=env, capture_output=True, text=True,
                          timeout=180)
    assert proc.returncode == 0, (proc.stdout[-2000:], proc.stderr[-2000:])
    assert "PASS 13/13 digest" in proc.stdout, proc.stdout[-2000:]
    return json.loads(out.read_text(encoding="utf-8"))


def test_enforce_posts_the_typed_controls(dump):
    controls = dump["enforce_controls"]
    assert controls["PF: Cash Available SAR"] == 53769.2625
    assert controls["PF: Max Position %"] == 20
    assert dump["enforce"]["panel"] == controls


def test_off_is_the_v1_5_13_reset(dump):
    golden = json.loads(GOLDEN.read_text(encoding="utf-8"))
    assert dump["off"]["controls"] == golden["refresh_controls"]
    assert dump["off"]["panel"] == golden["operator_panel"]
    assert golden["refresh_controls"]["PF: Cash Available SAR"] == 0
    assert golden["refresh_controls"]["PF: Max Position %"] == 15


def test_observe_names_the_reset_inputs(dump):
    assert dump["observe"].endswith(
        "layout: observe -- 3 operator input(s) reset to defaults: "
        "PF: Cash Available SAR, PF: Max Position %, PF: Rebalance Mode")


def test_added_files_are_ascii():
    for path in (HARNESS, GOLDEN, Path(__file__)):
        assert path.read_bytes().isascii(), path.name
