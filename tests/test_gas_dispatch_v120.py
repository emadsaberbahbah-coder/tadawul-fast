"""24_Sync_Dispatch v1.2.0 (a fault at GitHub is tried again): run the native harnesses - the REAL Apps Script source in
a vm - so the whole-suite CI job collects them. The two older harnesses (P-170, P-170b) had no collector; they run here
too, re-pinned on 1.2.0. Mirrors tests/test_dp2_v1480_empty_finished.py."""
from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "apps_script" / "24_Sync_Dispatch.gs"


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def _run(name):
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run([node, str(ROOT / "tests" / name)], cwd=ROOT, capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-2000:]
    return result.stdout.rstrip()


def test_dispatch_retry_v120_native_harness():
    assert _run("test_gas_dispatch_retry_v120.js").endswith("PASS 34/34")


@pytest.mark.parametrize("name,total", [("test_gas_sync_dispatch_p170.js", 44), ("test_gas_s1_dispatch_p170b.js", 53)])
def test_earlier_dispatch_harnesses_still_pass(name, total):
    out = _run(name)
    assert re.search(r"SUMMARY %d/%d PASS \| digest [0-9a-f]{12}$" % (total, total), out), out[-400:]
    assert "FAIL " not in out


def test_source_says_what_it_does():
    src = SOURCE.read_text(encoding="utf-8")
    assert src.isascii()
    assert "DAILY SYNC DISPATCHER v1.2.0" in src and "var TFB_SYNC_DISPATCH_VERSION = '1.2.0';" in src
    assert "RETRY_WAITS_MS: [5000, 20000]," in src
    # every dispatch goes through the one sender; no caller fetches the dispatch URL itself any more
    assert src.count("tfbSdSendDispatch_(req, cfg.repo, cfg.workflow, cfg.token)") == 2
    body = src.split("function tfbSdDispatch_(force)")[1].split("function tfbDispatchDailySync()")[0]
    lane = src.split("function tfbS1Dispatch_(laneKey, force)")[1].split("function tfbDispatchShadowBoard()")[0]
    assert "UrlFetchApp.fetch" not in body and "UrlFetchApp.fetch" not in lane
    # a run found after a fault sets LAST_OK in both lanes, so the min gap holds the next trigger
    assert body.count("setProperty(C.PROP_LAST_OK_MS") == 2 and lane.count("setProperty(L.propLastOkMs") == 2
