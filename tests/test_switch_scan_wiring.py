"""P-112 / F14 closure tests: live switch scan on the PF page. v2 (P-117).

v2 REWRITE (P-117, red-team MR-07 accepted 2026-09-10): v1 placed the
promotion assertions inside `if status == "pending_persistence"`, so any
unexpected status passed silently, and the feature-off test accepted an
"unavailable" module as a pass. v2 makes every positive-path assertion
UNCONDITIONAL, pins the working-dependency precondition explicitly, and
moves fail-soft behaviour into its own dedicated test.

Confirmation unit (explicit per MR-07): the persistence counter counts
SCANS via the confirm-redis store, not calendar days -- `persist_day` is
the Nth scan that produced the same sell->buy proposal.

Run from repo root: pytest tests/test_switch_scan_wiring.py -q
"""
import copy
import time

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


def _wire(monkeypatch):
    """Working-module preconditions, asserted -- never skipped (P-117)."""
    assert callable(getattr(pa, "advisor_switch_scan", None)), \
        "advisor_switch_scan dependency missing -- strict test must FAIL"
    store = {}
    monkeypatch.setattr(pa, "_confirm_redis_get", store.get)
    monkeypatch.setattr(pa, "_confirm_redis_put",
                        lambda k, v: store.__setitem__(k, v))
    monkeypatch.setattr(pa, "_rt_cost_pct_safe", lambda s, t: 0.40)
    return store


def test_off_no_switch_key_strict(monkeypatch):
    monkeypatch.delenv("TFB_PF_SWITCH_SCAN", raising=False)
    monkeypatch.setenv("TFB_PF_ENABLED", "1")
    out = _build()
    # P-117: "unavailable" is NOT an acceptable pass here -- a broken module
    # must fail this test, not slip through as fail-soft.
    assert out.get("status") == "ok"
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


def test_eligibility_and_persistence_unconditional(monkeypatch):
    monkeypatch.setenv("TFB_PF_SWITCH_SCAN", "1")
    monkeypatch.setenv("TFB_PF_ENABLED", "1")
    _wire(monkeypatch)
    pa.set_switch_candidates([dict(OK), dict(FT), dict(NO)])

    m1 = (_build().get("meta") or {}).get("switch_scan") or {}
    assert m1.get("candidates_total") == 3
    assert m1.get("candidates_eligible") == 1            # FT + DO_NOT excluded
    # P-117: promotion path asserted FLAT -- no conditional bypass.
    assert m1.get("status") == "pending_persistence", \
        "scan 1 must be pending_persistence, got %r" % m1.get("status")
    p = (m1.get("persist_pending") or [{}])[0]
    assert p.get("buy") == "ALT1.US" and p.get("persist_day") == 1
    assert not m1.get("proposals")

    m2 = (_build().get("meta") or {}).get("switch_scan") or {}
    assert m2.get("status") == "proposals", \
        "scan 2 must promote, got %r" % m2.get("status")
    p2 = (m2.get("proposals") or [{}])[0]
    assert p2.get("buy") == "ALT1.US" and p2.get("persist_day") == 2
    assert not m2.get("persist_pending")


def test_dependency_failure_is_failsoft_and_disclosed(monkeypatch):
    """Fail-soft contract, tested EXPLICITLY (P-117): a raising dependency
    must never break the PF build, and must be visible in the scan meta --
    not silently reported as a clean scan."""
    monkeypatch.setenv("TFB_PF_SWITCH_SCAN", "1")
    monkeypatch.setenv("TFB_PF_ENABLED", "1")
    _wire(monkeypatch)

    def _boom(*_a, **_k):
        raise RuntimeError("dependency down")
    monkeypatch.setattr(pa, "advisor_switch_scan", _boom)
    pa.set_switch_candidates([dict(OK)])
    out = _build()                                      # must not raise
    assert out.get("status") == "ok"
    m = (out.get("meta") or {}).get("switch_scan") or {}
    assert str(m.get("status", "")).startswith("error:"), \
        "dependency failure must be disclosed, got %r" % m.get("status")
