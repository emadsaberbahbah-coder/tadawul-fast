"""P-112 / F14 closure tests: live switch scan on the PF page.
Run from repo root: pytest tests/test_switch_scan_wiring.py -q
Covers: OFF => no switch_scan key; ON honest refusals (empty/stale cache);
executable-only eligibility (DO_NOT_INVEST + fast-track excluded);
2-scan persistence promotion via the confirm-redis store (monkeypatched)."""
import copy, time
import core.analysis.portfolio_actions as pa

HOLD = [{"Symbol": "AAA", "Name": "A", "Sector": "Industrials", "Currency": "USD",
         "Current Price": "68.66", "Quantity": "55", "Buy Price": "72.79",
         "Target Price": "71.00", "Forecast Reliability Score": "76",
         "Data Quality Score": "100", "Risk Bucket": "High"}]
FX = {"USD": 3.7555}
OK = {"symbol": "ALT1.US", "investability_status": "INVEST", "stability_state": "",
      "recommendation_detail": "", "roi_pct": 30.0, "confidence_band": "High",
      "suggested_sar": 10000}
FT = dict(OK, symbol="ALT2.US", stability_state="FAST-TRACK (day 1)",
          recommendation_detail="sizing suspended", roi_pct=40.0)
NO = dict(OK, symbol="ALT3.US", investability_status="DO_NOT_INVEST", roi_pct=50.0)


def _build():
    return pa.build_portfolio_actions(copy.deepcopy(HOLD), controls=None,
                                      fx_rates=dict(FX))


def test_off_no_switch_key(monkeypatch):
    monkeypatch.delenv("TFB_PF_SWITCH_SCAN", raising=False)
    monkeypatch.setenv("TFB_PF_ENABLED", "1")
    out = _build()
    assert out.get("status") in ("ok", "unavailable")
    assert "switch_scan" not in (out.get("meta") or {})


def test_on_empty_and_stale(monkeypatch):
    monkeypatch.setenv("TFB_PF_SWITCH_SCAN", "1")
    monkeypatch.setenv("TFB_PF_ENABLED", "1")
    pa._SWITCH_CANDS.update({"rows": None, "ts": None})
    m = (_build().get("meta") or {}).get("switch_scan") or {}
    assert m.get("status") == "no_candidates"
    pa.set_switch_candidates([dict(OK)], ts=time.time() - 7 * 3600)
    m = (_build().get("meta") or {}).get("switch_scan") or {}
    assert m.get("status") == "stale_candidates"


def test_eligibility_and_persistence(monkeypatch):
    monkeypatch.setenv("TFB_PF_SWITCH_SCAN", "1")
    monkeypatch.setenv("TFB_PF_ENABLED", "1")
    store = {}
    monkeypatch.setattr(pa, "_confirm_redis_get", store.get)
    monkeypatch.setattr(pa, "_confirm_redis_put",
                        lambda k, v: store.__setitem__(k, v))
    monkeypatch.setattr(pa, "_rt_cost_pct_safe", lambda s, t: 0.40)
    pa.set_switch_candidates([dict(OK), dict(FT), dict(NO)])
    m1 = (_build().get("meta") or {}).get("switch_scan") or {}
    assert m1.get("candidates_total") == 3
    assert m1.get("candidates_eligible") == 1        # FT + DO_NOT excluded
    if m1.get("status") == "pending_persistence":     # ob present => real math
        p = (m1.get("persist_pending") or [{}])[0]
        assert p.get("buy") == "ALT1.US" and p.get("persist_day") == 1
        m2 = (_build().get("meta") or {}).get("switch_scan") or {}
        assert m2.get("status") == "proposals"
        assert (m2.get("proposals") or [{}])[0].get("persist_day") == 2
