"""C4 (2026-10-17): portfolio_actions v1.18.0 — surface what the switch scan
already computes. Under TFB_PF_SWITCH_SCAN=1 the best pair per holding lands
on the row as switch_advisory ("switch: → ALT1.US Δ+x.x% (adv)") with the
section 4.7 precedence in detail; TFB_PF_SWITCH_ADVISORY_LEGACY=1 removes it.
Read-back harness for the portfolio gates together: the advisory exists only
on the certified path; TFB_PF_RESEARCH_VIEW off / observe / enforce and
TFB_PF_UNCERTIFIED_VIEW=on protective outputs never carry a switch field.
"""
from __future__ import annotations

import copy
import time

import pytest

import core.analysis.portfolio_actions as pa
from tests.portfolio_reconciliation_fixtures import build_certified_portfolio_actions, synthetic_quote_receipt

HOLD = [{"Symbol": "AAA", "Name": "A", "Sector": "Industrials", "Currency": "USD",
         "Current Price": "68.66", "Quantity": "55", "Buy Price": "72.79",
         "Target Price": "71.00", "Forecast Reliability Score": "76",
         "Data Quality Score": "100", "Risk Bucket": "High"}]
FX = {"USD": 3.7555}
OK = {"symbol": "ALT1.US", "investability_status": "INVEST", "stability_state": "",
      "recommendation_detail": "", "roi_pct": 30.0, "confidence_band": "High", "suggested_sar": 10000}
BETTER = dict(OK, symbol="ALT9.US", roi_pct=45.0)


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("TFB_PF_ENABLED", "1")
    for key in ("TFB_PF_SWITCH_SCAN", "TFB_PF_SWITCH_ADVISORY_LEGACY", "TFB_PF_RESEARCH_VIEW",
                "TFB_PF_UNCERTIFIED_VIEW"):
        monkeypatch.delenv(key, raising=False)


def _wire(monkeypatch):
    store = {}
    monkeypatch.setattr(pa, "_confirm_redis_get", store.get)
    monkeypatch.setattr(pa, "_confirm_redis_put", lambda k, v: store.__setitem__(k, v))
    monkeypatch.setattr(pa, "_rt_cost_pct_safe", lambda s, t: 0.40)
    return store


def _build():
    rows = [dict(row, **synthetic_quote_receipt()) for row in copy.deepcopy(HOLD)]
    return build_certified_portfolio_actions(pa, rows, controls=None, fx_rates=dict(FX))


# ---------------------------------------------------------------------------
# pure
# ---------------------------------------------------------------------------
def test_advisory_text_shape():
    assert pa._switch_advisory_text({"buy": "ATAT", "delta_pct": 23.98, "persist_day": 2}) == "switch: → ATAT Δ+24.0% (adv)"
    assert pa._switch_advisory_text({"buy": "ATAT", "delta_pct": 23.98, "persist_day": 1}) == "switch: → ATAT Δ+24.0% (adv, day 1/2)"
    assert pa._switch_advisory_text({"buy": "X", "delta_pct": -1.26}) == "switch: → X Δ-1.3% (adv, day 1/2)"
    assert pa._switch_advisory_text({}) == "switch: → ? Δ+0.0% (adv, day 1/2)"
    assert pa._switch_advisory_text({"buy": "X", "persist_day": "junk"}) == "switch: → X Δ+0.0% (adv, day 1/2)"


def test_stamp_picks_the_best_pair_per_holding_and_counts(monkeypatch):
    rows = [{"symbol": "AAA", "detail": {}}, {"symbol": "BBB", "detail": {}}, {"symbol": "CCC"}, "junk"]
    meta = {"proposals": [{"sell": "AAA", "buy": "P1", "delta_pct": 5.0, "hurdle_pct": 1.8, "persist_day": 2}],
            "persist_pending": [{"sell": "AAA", "buy": "P2", "delta_pct": 9.5, "hurdle_pct": 1.8, "persist_day": 1},
                                {"sell": "CCC", "buy": "P3", "delta_pct": 2.0, "hurdle_pct": 1.8, "persist_day": 1},
                                {"sell": "", "buy": "P4"}, None]}
    assert pa._stamp_switch_advisory(rows, meta) == 2
    assert rows[0]["switch_advisory"] == "switch: → P2 Δ+9.5% (adv, day 1/2)", "highest delta wins"
    assert rows[0]["detail"]["switch_advisory"] == {"buy": "P2", "delta_pct": 9.5, "hurdle_pct": 1.8, "persist_day": 1,
                                                    "precedence": pa.SWITCH_ADVISORY_PRECEDENCE}
    assert "switch_advisory" not in rows[1]
    assert rows[2]["switch_advisory"].startswith("switch: → P3") and rows[2]["detail"]["switch_advisory"]["buy"] == "P3"
    assert meta["advisory_rows"] == 2
    tie = {"proposals": [{"sell": "AAA", "buy": "C", "delta_pct": 5.0, "persist_day": 2}],
           "persist_pending": [{"sell": "AAA", "buy": "D", "delta_pct": 5.0, "persist_day": 1}]}
    r = [{"symbol": "AAA"}]
    pa._stamp_switch_advisory(r, tie)
    assert r[0]["switch_advisory"] == "switch: → C Δ+5.0% (adv)", "a confirmed pair wins a tie"
    monkeypatch.setenv("TFB_PF_SWITCH_ADVISORY_LEGACY", "1")
    r2 = [{"symbol": "AAA"}]; m2 = {k: v for k, v in tie.items() if k != "advisory_rows"}
    assert pa._stamp_switch_advisory(r2, m2) == 0 and "switch_advisory" not in r2[0] and "advisory_rows" not in m2
    assert pa._stamp_switch_advisory([{"symbol": "AAA"}], None) == 0


# ---------------------------------------------------------------------------
# end to end on the certified path
# ---------------------------------------------------------------------------
def test_scan_on_stamps_the_row_then_confirms_on_the_second_scan(monkeypatch):
    monkeypatch.setenv("TFB_PF_SWITCH_SCAN", "1")
    _wire(monkeypatch)
    pa.set_switch_candidates([dict(OK), dict(BETTER)])
    out = _build()
    assert out["status"] == "ok" and out["meta"]["execution_ready"] is True
    row = next(a for a in out["actions"] if a["symbol"] == "AAA")
    sw = out["meta"]["switch_scan"]
    assert sw["status"] in ("pending_persistence", "proposals") and sw["advisory_rows"] == 1
    assert row["switch_advisory"].startswith("switch: → ALT9.US Δ+") and row["switch_advisory"].endswith("(adv, day 1/2)")
    assert row["detail"]["switch_advisory"]["buy"] == "ALT9.US" and row["detail"]["switch_advisory"]["persist_day"] == 1
    assert row["detail"]["switch_advisory"]["precedence"] == pa.SWITCH_ADVISORY_PRECEDENCE
    assert row["detail"]["switch_advisory"]["delta_pct"] > row["detail"]["switch_advisory"]["hurdle_pct"]
    # the action surface is untouched by the advisory
    base_note = row["advisor_note"]
    assert "switch" not in base_note.lower() or "SWITCH" in base_note, "advisor_note is not rewritten"
    second = _build()
    row2 = next(a for a in second["actions"] if a["symbol"] == "AAA")
    assert row2["switch_advisory"].endswith("(adv)") and row2["detail"]["switch_advisory"]["persist_day"] == 2
    assert second["meta"]["switch_scan"]["status"] == "proposals"


def test_scan_off_or_legacy_has_no_field_and_the_rest_is_identical(monkeypatch):
    _wire(monkeypatch)
    pa.set_switch_candidates([dict(OK)])
    off = _build()
    assert "switch_scan" not in off["meta"] and not any("switch_advisory" in a for a in off["actions"])
    monkeypatch.setenv("TFB_PF_SWITCH_SCAN", "1")
    monkeypatch.setenv("TFB_PF_SWITCH_ADVISORY_LEGACY", "1")
    legacy = _build()
    assert "switch_scan" in legacy["meta"] and "advisory_rows" not in legacy["meta"]["switch_scan"]
    assert not any("switch_advisory" in a or "switch_advisory" in a.get("detail", {}) for a in legacy["actions"])
    monkeypatch.delenv("TFB_PF_SWITCH_ADVISORY_LEGACY")
    on = _build()
    strip = lambda p: [{k: v for k, v in a.items() if k not in ("switch_advisory", "detail")} for a in p["actions"]]  # noqa: E731
    assert strip(on) == strip(legacy), "every other row field identical"
    assert on["kpis"] == legacy["kpis"]


@pytest.mark.parametrize("research_view", ("off", "observe", "enforce"))
def test_protective_paths_never_carry_a_switch_field(monkeypatch, research_view):
    """The read-back the brief asks for: TFB_PF_RESEARCH_VIEW with the scan live."""
    monkeypatch.setenv("TFB_PF_SWITCH_SCAN", "1")
    if research_view != "off":
        monkeypatch.setenv("TFB_PF_RESEARCH_VIEW", research_view)
    _wire(monkeypatch)
    pa.set_switch_candidates([dict(OK)])
    rows = [dict(row, **synthetic_quote_receipt()) for row in copy.deepcopy(HOLD)]
    out = pa.build_portfolio_actions(rows, {"cash_available_sar": 0}, dict(FX), {"rows_supplied_by": "harness"})
    assert out["meta"]["execution_ready"] is False and "switch_scan" not in out["meta"]
    assert not any("switch_advisory" in a or "switch_advisory" in a.get("detail", {}) for a in out["actions"])
    assert ("research_view" in out["meta"]) is (research_view != "off")
    if research_view != "off":
        assert out["meta"]["research_view"]["mode"] == research_view


def test_uncertified_view_path_never_carries_a_switch_field(monkeypatch):
    monkeypatch.setenv("TFB_PF_SWITCH_SCAN", "1")
    monkeypatch.setenv("TFB_PF_UNCERTIFIED_VIEW", "on")
    _wire(monkeypatch)
    pa.set_switch_candidates([dict(OK)])
    rows = [dict(row, **synthetic_quote_receipt()) for row in copy.deepcopy(HOLD)]
    out = pa.build_portfolio_actions(rows, {"cash_available_sar": 0}, dict(FX), {"rows_supplied_by": "harness"})
    assert out["meta"]["uncertified_view"]["mode"] == "on" and "switch_scan" not in out["meta"]
    assert not any("switch_advisory" in a for a in out["actions"])
    assert out["version"] == pa.PORTFOLIO_ACTIONS_VERSION


def test_stale_candidates_leave_rows_untouched(monkeypatch):
    monkeypatch.setenv("TFB_PF_SWITCH_SCAN", "1")
    _wire(monkeypatch)
    pa.set_switch_candidates([dict(OK)], ts=time.time() - 7 * 3600)
    out = _build()
    assert out["meta"]["switch_scan"]["status"] == "stale_candidates" and out["meta"]["switch_scan"]["advisory_rows"] == 0
    assert not any("switch_advisory" in a for a in out["actions"])
