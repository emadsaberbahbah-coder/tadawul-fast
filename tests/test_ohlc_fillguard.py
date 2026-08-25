"""tests/test_ohlc_fillguard.py — v6.44.1 (W1A-6f2) FILL-GUARD regression gate.

Covers (DS-09): gate-off identity, observe zero-mutation, enforce clearing,
lazy FG-3 certification, forced-observe on failed certification, FAIL-CLOSED
enforce on core error, COLS allowlist rejection, case-folded headers, and a
python -O subprocess proof that the selftest survives optimization (DS-04).

The module under test defaults to scripts/run_dashboard_sync.py; override
with TFB_FILLGUARD_UNDER_TEST=<path> to certify a candidate file.
"""
from __future__ import annotations

import copy
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MOD_PATH = Path(os.getenv("TFB_FILLGUARD_UNDER_TEST")
                or ROOT / "scripts" / "run_dashboard_sync.py")

sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

_spec = importlib.util.spec_from_file_location("_fg_under_test", str(MOD_PATH))
M = importlib.util.module_from_spec(_spec)
sys.modules["_fg_under_test"] = M
_spec.loader.exec_module(M)

HDR = ["Symbol", "Name", "Open", "Day High", "Day Low", "Target Price"]


def _fixture():
    return [
        ["CRED.US", "Co A", None, 12.0, 11.0, None],
        ["HWAY.US", "Co B", 9.9, None, None, 33.3],
        ["OKAY.US", "Co C", 5.0, 5.5, 4.9, None],
    ]


def _reset_env():
    for k in ("TFB_SYNC_OHLC_FILL_GUARD", "TFB_SYNC_OHLC_FILL_GUARD_MODE",
              "TFB_SYNC_OHLC_FILL_GUARD_COLS"):
        os.environ.pop(k, None)


def test_off_is_identity():
    _reset_env()
    mx = _fixture(); ref = copy.deepcopy(mx)
    out, st = M._ohlc_fill_guard_apply(HDR, mx)
    assert out is mx and st is None and mx == ref


def test_observe_counts_without_mutation():
    _reset_env()
    os.environ["TFB_SYNC_OHLC_FILL_GUARD"] = "1"
    mx = _fixture(); ref = copy.deepcopy(mx)
    out, st = M._ohlc_fill_guard_apply(HDR, mx)
    assert mx == ref and st["armed"] and st["mode"] == "observe"
    assert st["total"] == 3 and st["nulls"]["Open"] == 1


def test_enforce_clears_only_guarded_nones():
    _reset_env()
    os.environ["TFB_SYNC_OHLC_FILL_GUARD"] = "1"
    os.environ["TFB_SYNC_OHLC_FILL_GUARD_MODE"] = "enforce"
    M._OHLC_FILLGUARD_SELFTEST_OK = True
    mx = _fixture()
    out, st = M._ohlc_fill_guard_apply(HDR, mx)
    assert mx[0][2] == "" and mx[1][3] == "" and mx[1][4] == ""
    assert mx[0][5] is None and mx[2][5] is None       # Target survives
    assert st["action"] == "cleared" and st["total"] == 3


def test_enforce_lazily_certifies_when_state_none():
    _reset_env()
    os.environ["TFB_SYNC_OHLC_FILL_GUARD"] = "1"
    os.environ["TFB_SYNC_OHLC_FILL_GUARD_MODE"] = "enforce"
    M._OHLC_FILLGUARD_SELFTEST_OK = None               # DS-03
    mx = _fixture()
    out, st = M._ohlc_fill_guard_apply(HDR, mx)
    assert M._OHLC_FILLGUARD_SELFTEST_OK is True
    assert st["mode"] == "enforce" and mx[0][2] == ""


def test_failed_certification_forces_observe():
    _reset_env()
    os.environ["TFB_SYNC_OHLC_FILL_GUARD"] = "1"
    os.environ["TFB_SYNC_OHLC_FILL_GUARD_MODE"] = "enforce"
    M._OHLC_FILLGUARD_SELFTEST_OK = False
    mx = _fixture(); ref = copy.deepcopy(mx)
    out, st = M._ohlc_fill_guard_apply(HDR, mx)
    assert mx == ref and st["mode"] == "observe"
    assert st.get("selftest") == "FAIL->observe"
    M._OHLC_FILLGUARD_SELFTEST_OK = None


def test_enforce_core_error_fails_closed():
    _reset_env()
    os.environ["TFB_SYNC_OHLC_FILL_GUARD"] = "1"
    os.environ["TFB_SYNC_OHLC_FILL_GUARD_MODE"] = "enforce"
    M._OHLC_FILLGUARD_SELFTEST_OK = True
    real = M._ohlc_fill_guard_core
    M._ohlc_fill_guard_core = lambda *a, **k: (_ for _ in ()).throw(
        ValueError("boom"))
    try:
        raised = False
        try:
            M._ohlc_fill_guard_apply(HDR, _fixture())
        except RuntimeError:
            raised = True                              # DS-02 fail-closed
        assert raised
        # observe stays fail-open on the same error
        os.environ["TFB_SYNC_OHLC_FILL_GUARD_MODE"] = "observe"
        mx = _fixture(); ref = copy.deepcopy(mx)
        out, st = M._ohlc_fill_guard_apply(HDR, mx)
        assert mx == ref and st.get("error") == "ValueError"
    finally:
        M._ohlc_fill_guard_core = real
        M._OHLC_FILLGUARD_SELFTEST_OK = None


def test_cols_env_is_allowlist_restricted():
    _reset_env()
    os.environ["TFB_SYNC_OHLC_FILL_GUARD"] = "1"
    os.environ["TFB_SYNC_OHLC_FILL_GUARD_MODE"] = "enforce"
    os.environ["TFB_SYNC_OHLC_FILL_GUARD_COLS"] = "Target Price,Open"
    M._OHLC_FILLGUARD_SELFTEST_OK = True
    mx = _fixture()
    out, st = M._ohlc_fill_guard_apply(HDR, mx)
    assert tuple(st["configured"]) == ("Open",)        # DS-06
    assert "Target Price" in (st.get("cols_rejected") or [])
    assert mx[0][2] == "" and mx[0][5] is None and mx[1][3] is None


def test_casefolded_headers_are_guarded():
    _reset_env()
    os.environ["TFB_SYNC_OHLC_FILL_GUARD"] = "1"
    os.environ["TFB_SYNC_OHLC_FILL_GUARD_MODE"] = "enforce"
    M._OHLC_FILLGUARD_SELFTEST_OK = True
    hdr = ["symbol", "open", "DAY HIGH", "day low"]    # DS-05
    mx = [["AAA", None, 2.0, 1.0]]
    out, st = M._ohlc_fill_guard_apply(hdr, mx)
    assert mx[0][1] == "" and st["total"] == 1


def test_selftest_survives_python_O():
    env = dict(os.environ)
    env["TFB_FG_PATH"] = str(MOD_PATH)
    code = (
        "import importlib.util, os, sys;"
        "sys.path.insert(0, %r); sys.path.insert(0, %r);"
        "sp = importlib.util.spec_from_file_location('m', os.environ['TFB_FG_PATH']);"
        "m = importlib.util.module_from_spec(sp); sys.modules['m'] = m;"
        "sp.loader.exec_module(m);"
        "print('SELFTEST_TRUE' if m._ohlc_fillguard_selftest_() is True else 'SELFTEST_FALSE')"
    ) % (str(ROOT), str(ROOT / "scripts"))
    r = subprocess.run([sys.executable, "-O", "-c", code], env=env,
                       capture_output=True, text=True, timeout=120)
    assert "SELFTEST_TRUE" in r.stdout, r.stdout + r.stderr


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"[FILLGUARD-TESTS] {len(fns)}/{len(fns)} PASS")
