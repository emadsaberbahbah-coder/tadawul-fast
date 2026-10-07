#!/usr/bin/env python3
"""Harness: run_dashboard_sync v6.64.8 portfolio minor-unit currency guard.

WHY (2026-10-07): the v6.64.6 holdings contract upper-cases both currencies,
so a .L quote in pence ('GBp') matches a pounds ledger ('GBP') and Position
Value / Unrealized P/L publish 100x. TFB_PF_MINOR_UNIT_CCY_GUARD=1 rejects a
minor-unit quote so the existing fail-closed path keeps the prior page.
Gate OFF must be byte-identical to v6.64.7.

Runs as a script ("PASS k/k") and under pytest. Pure, zero network.
Golden negative: TFB_PFG_SYNC=<path to a base copy of
scripts/run_dashboard_sync.py>; the gate-ON cases must then fail.
"""
from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.sheets.schema_registry import get_sheet_headers  # noqa: E402

HARNESS_VERSION = (1, 0, 0)
GATE = "TFB_PF_MINOR_UNIT_CCY_GUARD"
H = get_sheet_headers("My_Portfolio")


def _sync():
    alt = os.getenv("TFB_PFG_SYNC")
    if not alt:
        from scripts import run_dashboard_sync
        return run_dashboard_sync
    spec = importlib.util.spec_from_file_location("scripts._pfg_base_sync", alt)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


class _Reader:
    def __init__(self, grid):
        self.grid = grid

    def read_values(self, *_args):
        return self.grid


class _Env:
    def __init__(self, value):
        self.value = value

    def __enter__(self):
        self.saved = os.environ.get(GATE)
        if self.value is None:
            os.environ.pop(GATE, None)
        else:
            os.environ[GATE] = self.value

    def __exit__(self, *exc):
        if self.saved is None:
            os.environ.pop(GATE, None)
        else:
            os.environ[GATE] = self.saved


def _ledger(sym, ccy, cost):
    return [["My ledger"], [], [],
            ["Symbol", "Name", "Status", "Shares", "Buy Price", "Ccy"],
            [sym, "Holding", "Active", 100, cost, ccy]]


def _row(sym, ccy, price):
    row = [""] * len(H)
    row[H.index("Symbol")] = sym
    row[H.index("Name")] = "Holding"
    row[H.index("Currency")] = ccy
    row[H.index("Current Price")] = price
    return row


def _contract(sym, ledger_ccy, quote_ccy, price, gate):
    s = _sync()
    cb = s._read_cost_basis(_Reader(_ledger(sym, ledger_ccy, 25.0)), "sid")
    assert cb, "ledger snapshot must be admitted"
    with _Env(gate):
        pre = s._portfolio_holdings_contract(H, [_row(sym, quote_ccy, price)], cb,
                                             require_complete=False)
        out, _n = s._inject_portfolio_holdings(H, [_row(sym, quote_ccy, price)], cb,
                                               include_ledger_name=True)
        final = s._portfolio_holdings_contract(H, out, cb, require_complete=True)
    return pre, final, out


def test_off_reproduces_v6647_pence_pass_through():
    for gate in (None, "0", "", "false"):
        pre, final, out = _contract("VOD.L", "GBP", "GBp", 2600.0, gate)
        assert pre == (True, "") and final == (True, ""), (gate, pre, final)
        assert out[0][H.index("Position Value")] == 260000.0


def test_on_rejects_pence_quote_against_pounds_ledger():
    pre, final, _out = _contract("VOD.L", "GBP", "GBp", 2600.0, "1")
    assert pre[0] is False and "minor unit" in pre[1], pre
    assert final[0] is False, final


def test_on_rejects_every_minor_unit_spelling():
    for ccy, major in (("GBX", "GBP"), ("GBx", "GBP"), ("ZAc", "ZAR"), ("ZAC", "ZAR"),
                       ("ILA", "ILS"), ("ILa", "ILS")):
        pre, _final, _out = _contract("AAA.L", major, ccy, 100.0, "1")
        assert pre[0] is False, (ccy, pre)


def test_on_keeps_major_unit_holdings_working():
    for sym, ccy in (("VOD.L", "GBP"), ("2222.SR", "SAR"), ("AAPL.US", "USD")):
        pre, final, out = _contract(sym, ccy, ccy, 26.0, "1")
        assert pre == (True, "") and final == (True, ""), (sym, pre, final)
        assert out[0][H.index("Position Value")] == 2600.0


def test_existing_major_mismatch_message_unchanged():
    for gate in (None, "1"):
        pre, _f, _o = _contract("AAPL.US", "USD", "EUR", 26.0, gate)
        assert pre == (False, "holding quote currency does not match native ledger currency"), pre


def test_version_floor():
    v = tuple(int(x) for x in _sync().SCRIPT_VERSION.split("."))
    assert v >= (6, 64, 8), v
    assert HARNESS_VERSION >= (1, 0, 0)


TESTS = [obj for name, obj in sorted(globals().items()) if name.startswith("test_") and callable(obj)]


def main() -> int:
    passed = 0
    for fn in TESTS:
        try:
            fn()
            passed += 1
            print("ok   " + fn.__name__)
        except Exception as exc:  # noqa: BLE001
            print("FAIL " + fn.__name__ + ": " + repr(exc)[:200])
    total = len(TESTS)
    print(("PASS" if passed == total else "FAIL") + " %d/%d" % (passed, total))
    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
