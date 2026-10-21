"""data_engine_v2 v5.155.0 PF-SUBUNIT: My_Portfolio SAR weights for subunit-quoted holdings."""
import pytest

de = pytest.importorskip("core.data_engine_v2")


def _rows(ccy_l="GBp"):
    return [
        {"symbol": "2222.SR", "currency": "SAR", "position_qty": 1000, "avg_cost": 20.0,
         "current_price": 25.0},
        {"symbol": "VOD.L", "currency": ccy_l, "position_qty": 1000, "avg_cost": 60.0,
         "current_price": 70.0},
    ]


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for k in ("TFB_PF_FX_RATES", "TFB_PF_BASE_CCY", "TFB_OPP_FX_USX_SUBUNIT",
              "TFB_PF_WEIGHT_FX"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("TFB_PF_FX_RATES", "GBP:4.8")


def _weight(rows, sym):
    return next(r for r in rows if r["symbol"] == sym)["actual_weight"]


@pytest.mark.parametrize("tok", ["GBp", "GBX"])
def test_pence_holding_weighted_at_pounds_over_100(tok):
    rows = _rows(tok)
    de._compute_portfolio_fields(rows)
    # 700 GBP * 4.8 = 3,360 SAR vs 25,000 SAR -> 11.85%, not 93.07%.
    assert _weight(rows, "VOD.L") == pytest.approx(11.8483, abs=1e-3)
    assert _weight(rows, "2222.SR") == pytest.approx(88.1517, abs=1e-3)
    assert rows[0]["portfolio_fx_status"] == "OK:SAR"


def test_pounds_holding_unchanged():
    rows = _rows("GBP")
    rows[1]["current_price"] = 0.70
    de._compute_portfolio_fields(rows)
    assert _weight(rows, "VOD.L") == pytest.approx(11.8483, abs=1e-3)


def test_subunit_without_major_rate_fails_closed():
    rows = _rows("ILA")  # ILS has no built-in or operator rate
    de._compute_portfolio_fields(rows)
    assert all(r["actual_weight"] is None for r in rows)
    assert rows[0]["portfolio_fx_status"] == "PORTFOLIO_FX_INCOMPLETE: ILA"


def test_indicative_rate_disclosed_on_rows(monkeypatch):
    monkeypatch.delenv("TFB_PF_FX_RATES", raising=False)
    rows = _rows("GBp")
    de._compute_portfolio_fields(rows)
    assert rows[0]["portfolio_fx_status"] == "OK:SAR INDICATIVE:GBP"
    assert _weight(rows, "VOD.L") == pytest.approx(11.8483, abs=1e-3)


def test_subunit_helper():
    assert de._pf_subunit_major("GBp") == ("GBP", 100.0)
    assert de._pf_subunit_major("GBP") == ("GBP", 1.0)
    assert de._pf_subunit_major("ZAc") == ("ZAR", 100.0)
    assert de._pf_subunit_major("USX") == ("USX", 1.0)
