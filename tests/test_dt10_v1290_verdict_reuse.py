"""pytest wrapper: runs the node harness for 16_Decision_Top10.gs v1.29.0 [VERDICT-REUSE]."""
import pathlib
import shutil
import subprocess

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "test_dt10_v1290_verdict_reuse.js"


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_dt10_v1290_verdict_reuse_harness():
    proc = subprocess.run(["node", str(HARNESS)], capture_output=True, text=True, cwd=str(ROOT), timeout=600)
    out = proc.stdout + proc.stderr
    assert proc.returncode == 0, out[-4000:]
    assert "PASS 7/7" in out, out[-4000:]
    assert "x3 identical" in out, out[-2000:]
