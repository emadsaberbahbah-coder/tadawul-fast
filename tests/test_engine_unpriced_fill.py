#!/usr/bin/env python3
"""Harness: data_engine_v2 v5.151.4 unpriced-patch fill (TFB_ENGINE_UNPRICED_FILL).

WHY (2026-10-07): v5.151.2 (#722) stopped merging a provider patch with no
positive price, so a failed realtime quote no longer leaks its identity or
timestamp into a row priced by a later provider - but the patch's name,
sector and fundamentals were discarded too (synthetic AAPL.US: 5.151.0 kept
them, 5.151.3 publishes None). Gate ON restores those fields fill-only after
the priced merge. Gate OFF must be identical to v5.151.3.

Runs as a script ("PASS k/k") and under pytest. Zero network: provider
modules are synthetic. Golden negative: TFB_UF_ENGINE=<path to a base copy
of core/data_engine_v2.py>; the gate-ON cases must then fail.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

HARNESS_VERSION = (1, 0, 0)
GATE = "TFB_ENGINE_UNPRICED_FILL"
KEYS = ("current_price", "data_provider", "currency", "name", "sector",
        "market_cap", "pe_ttm", "eps_ttm", "exchange")


def _engine():
    alt = os.getenv("TFB_UF_ENGINE")
    if not alt:
        from core import data_engine_v2 as de
        return de
    spec = importlib.util.spec_from_file_location("core._uf_base_engine", alt)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


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


def _run(eod_patch, yah_patch, gate, symbol="AAPL.US"):
    de = _engine()
    inst = de.DataEngineV5(settings=SimpleNamespace(), providers=None)

    async def eod(sym):
        return dict(eod_patch, symbol=eod_patch.get("symbol", sym))

    async def yah(sym):
        return dict(yah_patch, symbol=sym)

    async def nohist(sym):
        return []

    async def none(*_a, **_k):
        return {}

    inst._provider_registry._modules = {
        "eodhd": SimpleNamespace(get_quote=eod, get_history=nohist),
        "yahoo_chart": SimpleNamespace(get_quote=yah, get_history=nohist),
        "finnhub": SimpleNamespace(get_quote=lambda s: None, get_history=nohist),
    }
    inst._fetch_yahoo_fundamentals_patch = none
    inst._fetch_yahoo_chart_patch = none
    inst._fetch_eodhd_fundamentals_patch = none
    with _Env(gate):
        row = asyncio.run(inst._get_enriched_quote_impl(symbol, "Global_Markets"))
    warnings = row.get("warnings")
    warnings = "; ".join(warnings) if isinstance(warnings, list) else str(warnings or "")
    return {k: row.get(k) for k in KEYS}, warnings


EOD_UNPRICED = {"name": "Apple Inc", "sector": "Technology", "exchange": "NASDAQ",
                "market_cap": 3.0e12, "pe_ttm": 31.5, "eps_ttm": 6.1,
                "currency": "EUR", "timestamp": "2020-01-01T00:00:00+00:00",
                "error": "quote_http_429"}
YAH_PRICED = {"current_price": 190.0, "previous_close": 188.0, "currency": "USD",
              "timestamp": "2026-10-07T08:09:00+00:00"}


def test_off_reproduces_v51513_loss():
    for gate in (None, "0", "", "false"):
        row, warn = _run(EOD_UNPRICED, YAH_PRICED, gate)
        assert row["current_price"] == 190.0 and row["data_provider"] == "yahoo_chart", row
        assert row["sector"] is None and row["market_cap"] is None, (gate, row)
        assert "unpriced_fill" not in warn


def test_on_restores_identity_and_fundamentals():
    row, warn = _run(EOD_UNPRICED, YAH_PRICED, "1")
    assert row["name"] == "Apple Inc", row
    assert row["sector"] == "Technology", row
    # exchange is already inferred from the .US suffix; fill-only keeps it.
    assert row["exchange"] == "NASDAQ/NYSE", row
    assert row["market_cap"] == 3.0e12 and row["pe_ttm"] == 31.5 and row["eps_ttm"] == 6.1, row
    assert "unpriced_fill:eodhd" in warn, warn


def test_on_never_takes_price_currency_provenance_or_error():
    row, warn = _run(EOD_UNPRICED, YAH_PRICED, "1")
    assert row["current_price"] == 190.0, row
    assert row["currency"] == "USD", row
    assert row["data_provider"] == "yahoo_chart", row
    assert "quote_http_429" not in warn, warn


def test_on_priced_provider_wins_conflicts():
    yah = dict(YAH_PRICED, name="Apple Inc. (Yahoo)", sector="Information Technology")
    row, _warn = _run(EOD_UNPRICED, yah, "1")
    assert row["name"] == "Apple Inc. (Yahoo)" and row["sector"] == "Information Technology", row
    assert row["market_cap"] == 3.0e12, row  # blank on the priced side -> filled


def test_on_skips_patch_with_disjoint_identity():
    crossed = dict(EOD_UNPRICED, symbol="MSFT.US", name="Microsoft Corp")
    row, warn = _run(crossed, YAH_PRICED, "1")
    assert row["name"] != "Microsoft Corp" and row["market_cap"] is None, row
    assert "unpriced_fill" not in warn, warn


def test_on_no_fill_when_no_provider_priced():
    unpriced_yah = {"currency": "USD"}
    off_row, _ = _run(EOD_UNPRICED, unpriced_yah, None)
    on_row, warn = _run(EOD_UNPRICED, unpriced_yah, "1")
    assert on_row == off_row, (on_row, off_row)
    assert "unpriced_fill" not in warn, warn


def test_version_floor():
    v = tuple(int(x) for x in _engine().__version__.split("."))
    assert v >= (5, 151, 4), v
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
            print("FAIL " + fn.__name__ + ": " + repr(exc)[:300])
    total = len(TESTS)
    print(("PASS" if passed == total else "FAIL") + " %d/%d" % (passed, total))
    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
