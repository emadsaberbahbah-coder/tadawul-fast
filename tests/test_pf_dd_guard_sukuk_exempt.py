# -*- coding: utf-8 -*-
"""tests/test_pf_dd_guard_sukuk_exempt.py

portfolio_actions v1.12.1 [F-2 HONOURS D-9]: the drawdown/time guard never
force-exits (or tags "would EXIT") a SUKUK-class holding; equities, the OFF
mode and the TFB_PA_PROTECT_SUKUK=0 kill switch keep v1.12.0 behaviour.

Executes the REAL core.analysis.portfolio_actions._apply_drawdown_guard with
the REAL core.compliance_gate classifier. No doubles.

Run:  python tests/test_pf_dd_guard_sukuk_exempt.py   (from the repo root)
      pytest -q tests/test_pf_dd_guard_sukuk_exempt.py
"""
import hashlib
import json
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import core.analysis.portfolio_actions as pa  # noqa: E402

_KEYS = ("TFB_PF_DD_EXIT", "TFB_PA_PROTECT_SUKUK", "TFB_PF_DD_EXIT_PCT",
         "TFB_PF_DD_TIME_D")
SUKUK_PAR = {"symbol": "5023.SR", "name": "", "pnl_sar": -50.0,
             "cost_sar": 10000.0, "market_value_sar": 9950.0,
             "buy_date": "2025-11-23"}
SUKUK_DEEP = dict(SUKUK_PAR, pnl_sar=-900.0, market_value_sar=9100.0)
SUKUK_OK = dict(SUKUK_PAR, pnl_sar=90.0, market_value_sar=10090.0)
EQ_DD = {"symbol": "YUM", "name": "Yum! Brands, Inc.", "pnl_sar": -1200.0,
         "cost_sar": 13042.0, "market_value_sar": 11842.0,
         "buy_date": "2026-08-12"}
EQ_TIME = {"symbol": "CARE.US", "name": "Carter Bankshares, Inc.",
           "pnl_sar": -249.0, "cost_sar": 11811.0,
           "market_value_sar": 11562.0, "buy_date": "2026-01-05"}


def _env(dd=None, protect=None):
    for k in _KEYS:
        os.environ.pop(k, None)
    if dd:
        os.environ["TFB_PF_DD_EXIT"] = dd
    if protect is not None:
        os.environ["TFB_PA_PROTECT_SUKUK"] = protect


def _g(cand, action="HOLD"):
    return pa._apply_drawdown_guard(dict(cand), action, "base reason", 0.0)


def run_all():
    saved = {k: os.environ.get(k) for k in _KEYS}
    res = {}
    try:
        assert pa.PORTFOLIO_ACTIONS_VERSION >= "1.12.1"
        assert pa._is_sukuk_holding(SUKUK_PAR) is True, "real classifier"
        assert pa._is_sukuk_holding(EQ_DD) is False

        # T1 - OFF: pure pass-through for everything.
        _env(None)
        for c in (SUKUK_PAR, SUKUK_DEEP, EQ_DD, EQ_TIME):
            assert _g(c) == ("HOLD", "base reason", 0.0)
        res["T1_off"] = "pass-through"

        # T2 - ENFORCE: the sukuk stands (time rule AND drawdown rule),
        # disclosed; equities still exit with full market value.
        _env("enforce")
        for c, word in ((SUKUK_PAR, "still negative"), (SUKUK_DEEP, "drawdown")):
            a, r, p = _g(c)
            assert (a, p) == ("HOLD", 0.0) and "[dd-exempt]" in r and word in r, r
            assert "full exit" not in r
        a, r, p = _g(EQ_DD)
        assert (a, p) == ("EXIT", 11842.0) and r.startswith("Drawdown/time guard:")
        a, r, p = _g(EQ_TIME, "ADD")
        assert a == "EXIT" and "time budget" in r and "(was ADD)" in r
        assert _g(SUKUK_OK) == ("HOLD", "base reason", 0.0), "no trigger -> untouched"
        assert _g(SUKUK_DEEP, "EXIT") == ("EXIT", "base reason", 0.0), "stronger verdict stands"
        res["T2_enforce"] = _g(SUKUK_PAR)[1]

        # T3 - OBSERVE: no "would EXIT" on the sukuk; equities tagged as before.
        _env("observe")
        a, r, p = _g(SUKUK_PAR)
        assert a == "HOLD" and "[dd-exempt]" in r and "would EXIT" not in r
        a, r, p = _g(EQ_DD)
        assert a == "HOLD" and "[dd-observe]" in r and "would EXIT under enforce" in r
        res["T3_observe"] = "sukuk exempt, equity tagged"

        # T4 - KILL SWITCH restores v1.12.0 for the sukuk.
        _env("enforce", "0")
        a, r, p = _g(SUKUK_PAR)
        assert (a, p) == ("EXIT", 9950.0) and "full exit (was HOLD)" in r
        _env("observe", "0")
        assert "[dd-observe]" in _g(SUKUK_PAR)[1]
        res["T4_kill"] = "TFB_PA_PROTECT_SUKUK=0 -> legacy"
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    return res


def test_dd_guard_honours_d9():
    run_all()


if __name__ == "__main__":
    out = run_all()
    for k in sorted(out):
        print(k, "->", out[k])
    print("PASS T1-T4 | digest", hashlib.sha256(
        json.dumps({k: v for k, v in out.items() if k != "T2_enforce"},
                   sort_keys=True).encode()).hexdigest()[:16])
