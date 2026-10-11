"""Real drawdown and funding seams replace the uncommitted dual-tree battery."""
import copy
from datetime import datetime, timedelta, timezone

import pytest


@pytest.mark.parametrize("pnl,age,trigger", [(-375., 10, "drawdown"), (-75., 134, "time budget")])
def test_guard_off_observe_enforce_preserves_or_exits(monkeypatch, pnl, age, trigger):
    from core.analysis import portfolio_actions as pa
    cand = {"symbol": "SYNTH.US", "cost_sar": 3750., "pnl_sar": pnl,
            "market_value_sar": 3750. + pnl,
            "buy_date": (datetime.now(timezone.utc).date() - timedelta(days=age)).isoformat()}
    original = ("HOLD", "original reason", 0.)
    monkeypatch.delenv("TFB_PF_DD_EXIT", raising=False)
    assert pa._apply_drawdown_guard(cand, *original) == original
    monkeypatch.setenv("TFB_PF_DD_EXIT", "observe")
    obs = pa._apply_drawdown_guard(cand, *original)
    assert obs[0] == original[0] and obs[2] == original[2]
    assert "[dd-observe]" in obs[1] and trigger in obs[1]
    monkeypatch.setenv("TFB_PF_DD_EXIT", "enforce")
    enforced = pa._apply_drawdown_guard(cand, *original)
    assert enforced[0] == "EXIT" and "Drawdown/time guard" in enforced[1] and trigger in enforced[1]
    assert enforced[2] == cand["market_value_sar"]
    winner = {**cand, "pnl_sar": 375.}
    assert pa._apply_drawdown_guard(winner, *original) == original


def test_fee_reserve_shrinks_the_exact_boundary_ticket(monkeypatch):
    from core.analysis import portfolio_actions as pa
    controls = {"rebalance_mode": pa.REBALANCE_NEW_CASH, "target_cash_pct": 0.,
                "lot_size": 1, "max_position_pct": 100.}
    entry = {"cand": {"symbol": "FEE.US", "price": 100., "fx_to_sar": 3.75,
                       "market_value_sar": 0., "sector": "Financials"},
             "action": "ADD", "action_reason": "qualified add", "proceeds_sar": 0.}
    monkeypatch.delenv("TFB_PF_FEE_FUNDING", raising=False)
    off = [copy.deepcopy(entry)]
    pa.fund_adds(off, controls, 755., 755.)
    monkeypatch.setenv("TFB_PF_FEE_FUNDING", "1")
    monkeypatch.setenv("TFB_PF_FEE_SAR", "9")
    on = [copy.deepcopy(entry)]
    pa.fund_adds(on, controls, 755., 755.)
    assert off[0]["suggested_delta_shares"] == 2
    assert on[0]["suggested_delta_shares"] == 1
    assert on[0]["suggested_delta_sar"] + 9 <= 755


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
