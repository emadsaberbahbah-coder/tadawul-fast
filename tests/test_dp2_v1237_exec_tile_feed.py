"""27_Decision_Pages_v2 v1.2.37 (the Executable tile follows the feed; Script Property DP2_EXEC_TILE_FEED, default off):
run the native harness (the REAL Apps Script source in a vm; the v1.2.36 file rebuilt from tests/js/dp2_edits_v1237.json,
SHA-256 pinned) so the whole-suite CI job collects it too (the blocking lane runs it with node). Mirrors
tests/test_dp2_v1236_evidence_since.py; the count is a k == k check of the harness's own last line."""
from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "test_dp2_v1237_exec_tile_feed.js"


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def test_dp2_v1237_exec_tile_feed_native_harness():
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run([node, str(HARNESS)], cwd=ROOT, capture_output=True, text=True, timeout=900)
    assert result.returncode == 0, result.stdout + result.stderr
    last = result.stdout.rstrip().splitlines()[-1]
    m = re.fullmatch(r"PASS (\d+)/(\d+)", last)
    assert m and m.group(1) == m.group(2) and int(m.group(2)) >= 10, result.stdout[-2000:]


if __name__ == "__main__":
    test_dp2_v1237_exec_tile_feed_native_harness()
    print("PASS 1/1")
