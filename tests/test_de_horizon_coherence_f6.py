"""F-6 horizon coherence against the actual engine in one repository tree."""
import copy

import pytest


FIXTURES = [
    ("Global_Markets", {"symbol": "A.US", "invest_period_label": "3M"}),
    ("Global_Markets", {"symbol": "B.US", "horizon_days": 30}),
    ("Global_Markets", {"symbol": "C.US"}),
    ("Global_Markets", {"symbol": "D.US", "invest_period_label": "1Y", "horizon_days": 365}),
    ("Mutual_Funds", {"symbol": "E_FUND", "invest_period_label": "3M", "horizon_days": 365}),
]


def run_all():
    from core import data_engine_v2 as de
    return [de._apply_page_row_backfill(page, copy.deepcopy(row)) for page, row in FIXTURES] + [
        de._apply_symbol_context_defaults({"symbol": "CL=F", "invest_period_label": "3M"}, page="Commodities_FX")]


def pair(row):
    return row.get("invest_period_label"), row.get("horizon_days")


def test_unset_and_off_are_identical_legacy_defaults(monkeypatch):
    monkeypatch.delenv("TFB_HORIZON_COHERENT", raising=False)
    unset = run_all()
    monkeypatch.setenv("TFB_HORIZON_COHERENT", "off")
    assert run_all() == unset
    assert [pair(row) for row in unset] == [("3M", 365), ("1Y", 30), ("1Y", 365),
                                           ("1Y", 365), ("3M", 365), ("3M", 365)]


def test_observe_changes_only_warnings_and_tags_both_defect_shapes(monkeypatch):
    monkeypatch.setenv("TFB_HORIZON_COHERENT", "off")
    off = run_all()
    monkeypatch.setenv("TFB_HORIZON_COHERENT", "observe")
    obs = run_all()
    assert [{k: v for k, v in row.items() if k != "warnings"} for row in off] == [
        {k: v for k, v in row.items() if k != "warnings"} for row in obs]
    for index in (0, 4, 5):
        assert "horizon_incoherent:3M!=365:observe" in obs[index]["warnings"]
    assert "horizon_incoherent:1Y!=30:observe" in obs[1]["warnings"]
    assert "horizon_incoherent" not in (obs[3].get("warnings") or "")


def test_enforce_derives_blank_side_and_preserves_existing_pairs(monkeypatch):
    monkeypatch.setenv("TFB_HORIZON_COHERENT", "enforce")
    rows = run_all()
    assert [pair(row) for row in rows] == [("3M", 90), ("1M", 30), ("1Y", 365),
                                         ("1Y", 365), ("3M", 365), ("3M", 90)]
    assert "horizon_coherent_fill:days_from_3M:enforce" in rows[0]["warnings"]
    assert "horizon_incoherent:3M!=365:enforce" in rows[4]["warnings"]
    for index in (2, 3):
        assert "horizon" not in (rows[index].get("warnings") or "")


@pytest.mark.parametrize("label,days", [("1D", 1), ("1W", 6), ("1M", 30), ("3M", 90), ("1Y", 365)])
def test_label_days_round_trip(label, days):
    from core import data_engine_v2 as de
    assert de._HZC_LABEL_TO_DAYS[label] == days
    assert de._hzc_label_for_days(days) == label


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
