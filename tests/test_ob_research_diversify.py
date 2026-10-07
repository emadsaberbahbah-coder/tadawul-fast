#!/usr/bin/env python3
"""Harness: v1.24.2 research-pass diversification (TFB_OPP_RESEARCH_DIVERSIFY).

WHY (2026-10-07): the v1.24.1 research pass listed every qualified name
before the sector/market counters advanced, so the cockpit's stability fill
built an undiversified board and the allocate replay deferred the over-cap
seats, leaving cash unallocated (100,000 -> 50,000 SAR on the synthetic case
below). Gate ON makes research seats consume the cash-independent
diversification slots. Gate OFF must be byte-identical to v1.24.1.

Runs as a script ("PASS k/k") and under pytest. Zero network, synthetic rows.
Golden negative: TFB_ORD_BUILDER=<path to a base copy of
core/analysis/opportunity_builder.py> loads that file instead; the gate-ON
cases must then fail.
"""
from __future__ import annotations

import copy
import importlib.util
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

HARNESS_VERSION = (1, 0, 0)
GATE = "TFB_OPP_RESEARCH_DIVERSIFY"
_CLEAN_PREFIXES = ("TFB_OPP_", "TFB_T10_", "TFB_TICKET_")


def _load_builder():
    alt = os.getenv("TFB_ORD_BUILDER")
    if not alt:
        from core.analysis import opportunity_builder as ob
        return ob
    spec = importlib.util.spec_from_file_location("core.analysis._ord_base_builder", alt)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Env:
    """Isolate builder policy env for one case; restore afterwards."""

    def __init__(self, **extra):
        self.extra = extra

    def __enter__(self):
        self.saved = dict(os.environ)
        for key in list(os.environ):
            if key.startswith(_CLEAN_PREFIXES):
                del os.environ[key]
        os.environ.update(TFB_OPP_ENABLED="1", TFB_OPP_FUNDING_PLAN="1", APP_TOKEN="synthetic-token")
        os.environ.update(self.extra)
        return self

    def __exit__(self, *exc):
        os.environ.clear()
        os.environ.update(self.saved)


def _row(sym, sector, roi, name=None):
    return {"symbol": sym, "name": name or sym, "sector": sector, "market": "Tadawul",
            "currency": "SAR", "current_price": 100.0, "intrinsic_value": 100 * (1 + roi / 100),
            "forecast_reliability_score": 82.0, "data_quality_score": 91.0,
            "risk_bucket": "Moderate", "provider_engine_conflict": "No", "volatility_30d": 4.0,
            "avg_volume_30d": 2_500_000, "expected_roi_12m": roi,
            "recommendation_detailed": "STRONG BUY", "investability_status": "INVESTABLE",
            "block_reason": ""}


ROWS = ([_row("E%d.SR" % i, "Energy", 40 - i) for i in range(4)] +
        [_row("M%d.SR" % i, "Materials", 30 - i) for i in range(2)])
CRIT = {"max_selected": 4, "max_per_sector": 2, "max_per_market": 10, "max_weight_pct": 25.0,
        "pf_max_sector_pct": 100.0, "min_ticket_sar": 1000.0,
        "rank_by_engine_roi_enabled": True, "trust_gate_enabled": False}
PF = {"cash_available_sar": 100000.0}
FX = {"SAR": 1.0}


def _single(ob, rows=ROWS, crit=CRIT):
    return ob.build_opportunity_payload(copy.deepcopy(rows), criteria=dict(crit),
                                        portfolio=dict(PF), fx_rates=dict(FX))


def _research(ob, rows=ROWS, crit=CRIT):
    return ob.build_opportunity_payload(copy.deepcopy(rows),
                                        criteria={**crit, "board_funding_stage": "research"},
                                        portfolio=dict(PF), fx_rates=dict(FX))


def _board_then_allocate(ob, rows=ROWS, crit=CRIT):
    """Mirror the cockpit's day-1 fast-track fill: the first max_selected
    research seats in order become the board, then replay allocation."""
    res = _research(ob, rows, crit)
    board = [t["symbol"] for t in res["selected"]][:crit["max_selected"]]
    snap = res["meta"]["board_funding"]["snapshot"]
    alloc = ob.build_opportunity_payload(
        copy.deepcopy(snap["rows"]),
        criteria={**crit, "board_funding_stage": "allocate", "board_funding_symbols": board,
                  "board_funding_snapshot": copy.deepcopy(snap)},
        portfolio=dict(PF), fx_rates=dict(FX))
    return res, board, alloc


def _funded(payload):
    return [(t["symbol"], t["suggested_sar"]) for t in payload["selected"]]


def _strip_volatile(payload):
    """Drop fields that legitimately differ between two calls (timestamps,
    signed snapshot ids) so payloads can be compared for identity."""
    p = copy.deepcopy(payload)
    meta = p.get("meta", {})
    for key in ("generated_at", "generated_at_utc", "timestamp", "elapsed_ms", "timing"):
        meta.pop(key, None)
    p.pop("generated_at", None)
    bf = meta.get("board_funding")
    if isinstance(bf, dict):
        bf.pop("snapshot", None)
        bf.pop("snapshot_id", None)
    return p


# --- gate OFF: v1.24.1 behaviour preserved ------------------------------------

def test_off_research_lists_every_qualified_name():
    ob = _load_builder()
    with _Env():
        syms = [t["symbol"] for t in _research(ob)["selected"]]
    assert syms == ["E0.SR", "E1.SR", "E2.SR", "E3.SR", "M0.SR", "M1.SR"], syms


def test_off_explicit_zero_equals_unset():
    ob = _load_builder()
    with _Env():
        a = _strip_volatile(_research(ob))
    with _Env(**{GATE: "0"}):
        b = _strip_volatile(_research(ob))
    assert a == b


def test_off_regression_still_present_documents_the_bug():
    ob = _load_builder()
    with _Env():
        _res, board, alloc = _board_then_allocate(ob)
    assert board == ["E0.SR", "E1.SR", "E2.SR", "E3.SR"], board
    assert sum(s for _, s in _funded(alloc)) == 50000.0, _funded(alloc)


def test_single_pass_unaffected_by_gate():
    ob = _load_builder()
    with _Env():
        a = _strip_volatile(_single(ob))
    with _Env(**{GATE: "1"}):
        b = _strip_volatile(_single(ob))
    assert a == b
    assert _funded(_single(ob)) == [("E0.SR", 25000.0), ("E1.SR", 25000.0),
                                    ("M0.SR", 25000.0), ("M1.SR", 25000.0)]


# --- gate ON ------------------------------------------------------------------

def test_on_research_respects_sector_cap():
    ob = _load_builder()
    with _Env(**{GATE: "1"}):
        res = _research(ob)
    syms = [t["symbol"] for t in res["selected"]]
    assert syms == ["E0.SR", "E1.SR", "M0.SR", "M1.SR"], syms
    assert all(t["suggested_sar"] == 0 for t in res["selected"])


def test_on_board_is_fully_funded_like_single_pass():
    ob = _load_builder()
    with _Env(**{GATE: "1"}):
        _res, board, alloc = _board_then_allocate(ob)
        single = _funded(_single(ob))
    assert board == ["E0.SR", "E1.SR", "M0.SR", "M1.SR"], board
    assert alloc["status"] == "ok", alloc.get("status")
    assert _funded(alloc) == single, (_funded(alloc), single)
    assert sum(s for _, s in _funded(alloc)) == 100000.0


def test_on_research_respects_market_cap():
    ob = _load_builder()
    crit = {**CRIT, "max_per_sector": 10, "max_per_market": 3}
    with _Env(**{GATE: "1"}):
        syms = [t["symbol"] for t in _research(ob, crit=crit)["selected"]]
    assert syms == ["E0.SR", "E1.SR", "E2.SR"], syms


def test_on_research_respects_issuer_dedup_when_enabled():
    ob = _load_builder()
    rows = [_row("2222.SR", "Energy", 40, name="Saudi Aramco"),
            _row("ARAMCO.SR", "Materials", 39, name="Saudi Aramco"),
            _row("M0.SR", "Materials", 30)]
    crit = {**CRIT, "max_per_sector": 10, "issuer_dedup_enabled": True}
    with _Env(**{GATE: "1"}):
        on = [t["symbol"] for t in _research(ob, rows, crit)["selected"]]
        single = [s for s, _ in _funded(_single(ob, rows, crit))]
    with _Env():
        off = [t["symbol"] for t in _research(ob, rows, crit)["selected"]]
    # Research ON must drop exactly what the single pass drops.
    assert on == single, (on, single)
    assert len(off) >= len(on), (off, on)


def test_gate_is_bound_into_replay_fingerprint():
    ob = _load_builder()
    with _Env():
        base = ob._board_funding_basis(CRIT, PF, FX)
    with _Env(**{GATE: "1"}):
        armed = ob._board_funding_basis(CRIT, PF, FX)
    assert base != armed


def test_version_floor():
    ob = _load_builder()
    v = tuple(int(x) for x in ob.OPPORTUNITY_BUILDER_VERSION.split("."))
    assert v >= (1, 24, 2), v
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
