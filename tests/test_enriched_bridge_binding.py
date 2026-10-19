#!/usr/bin/env python3
"""tests/test_enriched_bridge_binding.py - routes/enriched_quote v8.6.0:
the bridge binds the analysis implementation's own signature, and the
instrument fallback crosses the engine publication boundary.

WHY (2026-10-19)
----------------
The dashboard sync and the Portfolio_Decision cockpit read My_Portfolio
through POST /v1/enriched/sheet-rows. That route is bridge-first: it hands
an instrument page to routes.analysis_sheet_rows._analysis_sheet_rows_impl,
whose nine parameters carry no defaults. _call_with_tolerant_signatures
(v8.5.2) tried eight FIXED keyword sets; the first two miss x_api_key, the
rest miss include_matrix_q / mode / request, so every attempt failed at
binding, the helper returned None without a log line, and the handler served
the page from _build_instrument_rows: engine get_enriched_quotes_batch rows
normalised onto the contract keys and never passed through
core.data_engine_v2._strict_project_row. The eight investability-gate
columns (Data Quality Score .. Block Reason) therefore left this route as
None on every holding: the 18 Oct sync log reconciled three buy-family rows
whose Final Action was blank and whose Block Reason read "None", the DQ audit
found the weight columns blank on 4/4 holdings (DQ-51), and the cockpit
rendered every holding with no reliability / data-quality score.

Pinned here:
  B1  the real bridge callable for My_Portfolio binds under v8.6.0
  B2  the v8.5.2 fixed ladder cannot bind it (the defect, kept as a negative)
  B3  _call_with_tolerant_signatures reaches a no-default implementation
      (attempt 0, bound=True), None passed for the parameters it fills
  B4  TFB_ENRICHED_BRIDGE_SIGBIND=0 restores the eight-attempt ladder
  B5  a **kwargs callable keeps the first attempt (bind returns None)
  B6  the sheet-rows handler dispatches bridge_sheet_rows for My_Portfolio
      (the upstream label travels as upstream_dispatch) and the gate columns
      reach rows / rows_matrix
  B7  when the bridge cannot bind, the instrument fallback still publishes
      gated decision columns and discloses the boundary in meta; the cached
      quote is not mutated
  B8  TFB_ENRICHED_FALLBACK_BOUNDARY=0 restores the v8.5.2 bare projection
      (gate columns None)
  B9  a typed mismatch logs one WARNING naming module and callable
  B10 version and CI enrolment

Run: pytest -q tests/test_enriched_bridge_binding.py
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import sys
from pathlib import Path
from types import MethodType, SimpleNamespace

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

fastapi = pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")

from core import data_engine_v2 as de  # noqa: E402
from routes import enriched_quote as eq  # noqa: E402

SYMS = ["KRP.US", "DDI.US", "CWBC.US"]
GATE = ("data_quality_score", "forecast_reliability_score", "provider_engine_conflict",
        "conflict_type", "final_decision_basis", "investability_status", "final_action",
        "block_reason")
ANALYSIS_PARAMS = ("request", "body", "mode", "include_matrix_q", "token", "x_app_token",
                   "x_api_key", "authorization", "x_request_id")


def _payload_kwargs(request=None):
    """The keyword set _delegate_sheet_rows_via_bridge builds (v8.4.0)."""
    body = {"page": "My_Portfolio", "sheet_name": "My_Portfolio", "symbols": list(SYMS),
            "tickers": list(SYMS), "include_matrix": True}
    return {
        "request": request if request is not None else object(), "body": body, "payload": body,
        "mode": "", "include_matrix_q": True, "include_matrix": True,
        "token": None, "x_app_token": None, "authorization": None, "x_request_id": "req-b",
        "page": "My_Portfolio", "sheet": "My_Portfolio", "sheet_name": "My_Portfolio",
        "name": "My_Portfolio", "tab": "My_Portfolio", "symbols": list(SYMS),
        "tickers": list(SYMS), "top_n": 200, "limit": 0, "offset": 0,
    }


def _v852_attempts(payload_kwargs):
    return [
        payload_kwargs,
        {k: payload_kwargs.get(k) for k in ("request", "body", "payload", "mode", "include_matrix_q", "token", "x_app_token", "authorization", "x_request_id")},
        {k: payload_kwargs.get(k) for k in ("request", "body", "mode")},
        {k: payload_kwargs.get(k) for k in ("request", "body")},
        {k: payload_kwargs.get(k) for k in ("body", "mode")},
        {k: payload_kwargs.get(k) for k in ("body",)},
        {k: payload_kwargs.get(k) for k in ("page", "sheet", "sheet_name", "name", "tab", "symbols", "tickers", "top_n", "limit", "offset", "mode")},
        {},
    ]


def _raw_engine_row(sym, reco="ACCUMULATE"):
    """An engine-direct quote row (get_enriched_quotes_batch shape): scored,
    priced, forecast, NO gate columns -- the factory never runs the gate."""
    return {
        "symbol": sym, "name": "Synthetic %s" % sym, "asset_class": "Equity", "exchange": "NASDAQ",
        "currency": "USD", "sector": "Financial Services", "industry": "Banks",
        "current_price": 100.0, "previous_close": 99.0, "open_price": 99.5, "day_high": 101.0,
        "day_low": 98.5, "week_52_high": 130.0, "week_52_low": 70.0, "volume": 1_000_000,
        "avg_volume_30d": 900_000, "market_cap": 5e9, "pe_ttm": 12.0, "debt_to_equity": 0.8,
        "free_cash_flow_ttm": 1.2e8, "rsi_14": 55.0, "volatility_30d": 20.0, "risk_score": 20.0,
        "risk_bucket": "Low", "intrinsic_value": 125.0, "upside_pct": 0.25, "valuation_score": 70.0,
        "forecast_price_3m": 108.0, "forecast_price_12m": 120.0, "expected_roi_3m": 0.08,
        "expected_roi_12m": 0.2, "forecast_confidence": 0.66, "confidence_score": 66.41,
        "forecast_source": "provider_target", "value_score": 70.0, "quality_score": 75.0,
        "momentum_score": 60.0, "growth_score": 65.0, "overall_score": 70.0,
        "opportunity_score": 68.0, "recommendation": reco, "recommendation_detailed": reco,
        "recommendation_reason": "synthetic", "provider_rating": "BUY",
        "scoring_schema_version": "5.11.4", "horizon_days": 365, "invest_period_label": "1Y",
        "data_provider": "yahoo_chart", "last_updated_utc": "2026-10-18T22:29:00+00:00",
        "warnings": "acquisition_status:success; acquisition_acquired_at:2026-10-18T22:29:00+00:00; "
                    "acquisition_provider:yahoo_chart; acquisition_quote_asof:2026-10-18T20:00:00+00:00",
    }


class _FakeEngine:
    """Only the engine-direct method the fallback reaches; rows handed out are
    the SAME objects kept here so B7 can prove they are not mutated."""
    def __init__(self):
        self.rows = {s: _raw_engine_row(s, "BUY" if s == "DDI.US" else "ACCUMULATE") for s in SYMS}

    async def get_enriched_quotes_batch(self, symbols, **kwargs):
        return [self.rows[s] for s in symbols if s in self.rows]


def _fake_service(engine):
    headers, keys = de.get_sheet_spec("My_Portfolio")
    svc = SimpleNamespace(
        auth_guard=lambda *a, **k: None,
        normalize_page=lambda page: page,
        route_family=lambda page: "instrument",
        contract=lambda page: (list(headers), list(keys)),
        bridge_timeout_sec=10.0,
    )
    svc.envelope = MethodType(eq._Service.envelope, svc)

    async def get_engine(request):
        return engine
    svc.get_engine = get_engine
    return svc, list(headers), list(keys)


async def _analysis_like_impl(request, body, mode, include_matrix_q, token, x_app_token,
                              x_api_key, authorization, x_request_id):
    """Stands in for routes.analysis_sheet_rows._analysis_sheet_rows_impl:
    the exact no-default signature, returning an envelope whose rows already
    crossed the engine boundary (gate columns populated)."""
    rows = []
    for s in SYMS:
        r = _raw_engine_row(s)
        r.update({"data_quality_score": 100.0, "forecast_reliability_score": 66.0,
                  "provider_engine_conflict": "FALSE", "conflict_type": "Aligned",
                  "final_decision_basis": "Engine", "investability_status": "INVESTABLE",
                  "final_action": "INVEST", "block_reason": ""})
        rows.append(r)
    return {"status": "success", "page": "My_Portfolio", "rows": rows,
            "meta": {"dispatch": "instrument_mode", "x_api_key_seen": x_api_key is None}}


async def _post_sheet_rows(body):
    app = fastapi.FastAPI()

    @app.post("/synthetic")
    async def route(request: fastapi.Request):
        return await eq._sheet_rows_handler(
            request=request, body=body, mode="", include_matrix_q=True, token=None,
            x_app_token=None, authorization=None, x_request_id="req-b")

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                 base_url="https://route.invalid") as client:
        response = await client.post("/synthetic", json={})
    return response


@pytest.fixture
def wired(monkeypatch):
    engine = _FakeEngine()
    svc, headers, keys = _fake_service(engine)
    monkeypatch.setattr(eq, "svc", svc)

    async def resolve(page, route_family):
        return _analysis_like_impl, {"module": "fake.analysis", "callable": "_analysis_like_impl"}
    monkeypatch.setattr(eq, "_resolve_bridge_impl", resolve)
    monkeypatch.delenv("TFB_ENRICHED_BRIDGE_SIGBIND", raising=False)
    monkeypatch.delenv("TFB_ENRICHED_FALLBACK_BOUNDARY", raising=False)
    monkeypatch.setenv("TFB_OPP_ENABLED", "1")
    return SimpleNamespace(engine=engine, svc=svc, headers=headers, keys=keys)


# --- B1 / B2: the real callable -----------------------------------------------

def test_b1_real_bridge_callable_binds_under_v860():
    impl, meta = asyncio.run(eq._resolve_bridge_impl("My_Portfolio", "instrument"))
    assert meta["module"] == "routes.analysis_sheet_rows"
    assert meta["callable"] == "_analysis_sheet_rows_impl"
    payload = _payload_kwargs()
    bound = eq._bind_kwargs_to_signature(impl, payload)
    assert bound is not None
    inspect.signature(impl).bind(**bound)          # raises TypeError on the defect
    assert set(bound) == set(ANALYSIS_PARAMS)
    assert bound["x_api_key"] is None and bound["body"] is payload["body"]
    assert "payload" not in bound and "page" not in bound


def test_b2_v852_ladder_cannot_bind_the_real_callable():
    impl, _ = asyncio.run(eq._resolve_bridge_impl("My_Portfolio", "instrument"))
    sig = inspect.signature(impl)
    for attempt in _v852_attempts(_payload_kwargs()):
        call_kwargs = {k: v for k, v in attempt.items() if v is not None}
        with pytest.raises(TypeError):
            sig.bind(**call_kwargs)


# --- B3 / B4 / B5: the tolerant caller ----------------------------------------

def test_b3_tolerant_caller_reaches_a_no_default_implementation(monkeypatch):
    monkeypatch.delenv("TFB_ENRICHED_BRIDGE_SIGBIND", raising=False)
    seen = {}

    async def impl(request, body, mode, include_matrix_q, token, x_app_token, x_api_key,
                   authorization, x_request_id):
        seen.update(request=request, body=body, mode=mode, include_matrix_q=include_matrix_q,
                    token=token, x_app_token=x_app_token, x_api_key=x_api_key,
                    authorization=authorization, x_request_id=x_request_id)
        return {"rows": [{"symbol": "KRP.US"}]}

    payload = _payload_kwargs()
    out, summary, outcome = asyncio.run(
        eq._call_with_tolerant_signatures(impl, timeout_seconds=5.0, kwargs=payload))
    assert outcome == "success" and out == {"rows": [{"symbol": "KRP.US"}]}
    assert summary[0]["attempt_idx"] == 0 and summary[0].get("bound") is True
    assert "x_api_key" in summary[0]["kwargs_keys"]
    assert seen["x_api_key"] is None and seen["token"] is None
    assert seen["body"] is payload["body"] and seen["x_request_id"] == "req-b"


def test_b4_kill_switch_restores_the_v852_ladder(monkeypatch):
    monkeypatch.setenv("TFB_ENRICHED_BRIDGE_SIGBIND", "0")

    async def impl(request, body, mode, include_matrix_q, token, x_app_token, x_api_key,
                   authorization, x_request_id):  # pragma: no cover - never bound
        raise AssertionError("must not be called")

    out, summary, outcome = asyncio.run(
        eq._call_with_tolerant_signatures(impl, timeout_seconds=5.0, kwargs=_payload_kwargs()))
    assert out is None and outcome == "all_signatures_typed_mismatch"
    assert len(summary) == 8 and all("bound" not in s for s in summary)
    assert [s["outcome"] for s in summary] == ["typeerror"] * 8


def test_b5_var_kwargs_callable_keeps_the_first_attempt(monkeypatch):
    monkeypatch.delenv("TFB_ENRICHED_BRIDGE_SIGBIND", raising=False)
    payload = _payload_kwargs()
    assert eq._bind_kwargs_to_signature(lambda **kw: kw, payload) is None

    async def impl(**kw):
        return kw

    out, summary, outcome = asyncio.run(
        eq._call_with_tolerant_signatures(impl, timeout_seconds=5.0, kwargs=payload))
    assert outcome == "success"
    assert out == {k: v for k, v in payload.items() if v is not None}
    assert "bound" not in summary[0]


# --- B6 / B7 / B8: the handler ------------------------------------------------

def test_b6_handler_dispatches_the_bridge_for_my_portfolio(wired):
    resp = asyncio.run(_post_sheet_rows({"page": "My_Portfolio", "symbols": SYMS}))
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload["meta"]["dispatch"] == "bridge_sheet_rows"
    assert payload["meta"]["upstream_dispatch"] == "instrument_mode"
    assert payload["meta"]["x_api_key_seen"] is True
    rows = payload["row_objects"]
    assert [r["symbol"] for r in rows] == SYMS
    for r in rows:
        assert r["final_action"] == "INVEST" and r["investability_status"] == "INVESTABLE"
        assert r["forecast_reliability_score"] == 66.0 and r["data_quality_score"] == 100.0
    ki = wired.keys.index
    assert payload["rows_matrix"][0][ki("final_action")] == "INVEST"
    assert payload["rows_matrix"][0][ki("forecast_reliability_score")] == 66.0
    assert payload["headers"][ki("forecast_reliability_score")] == "Forecast Reliability Score"


def test_b7_fallback_publishes_gated_columns_when_the_bridge_cannot_bind(wired, monkeypatch):
    monkeypatch.setenv("TFB_ENRICHED_BRIDGE_SIGBIND", "0")
    resp = asyncio.run(_post_sheet_rows({"page": "My_Portfolio", "symbols": SYMS}))
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    meta = payload["meta"]
    assert meta["dispatch"] == "instrument_sheet_rows_fallback"
    assert meta["bridge_call_outcome"] == "all_signatures_typed_mismatch"
    assert meta["publication_boundary"] == "core.data_engine_v2._strict_project_row"
    assert meta["publication_boundary_rows"] == len(SYMS)
    rows = payload["row_objects"]
    assert [r["symbol"] for r in rows] == SYMS
    for r in rows:
        assert r["investability_status"] == "INVESTABLE" and r["final_action"] == "INVEST"
        assert r["data_quality_score"] == 100.0 and r["forecast_reliability_score"] == 66.0
        assert r["conflict_type"] == "Aligned" and r["block_reason"] == ""
        assert r["recommendation"] in ("ACCUMULATE", "BUY")
    ki = wired.keys.index
    assert payload["rows_matrix"][1][ki("final_action")] == "INVEST"
    # the engine's cached quote objects were not mutated by the boundary
    for cached in wired.engine.rows.values():
        assert not any(k in cached for k in GATE)
        assert cached["recommendation"] in ("ACCUMULATE", "BUY")


def test_b8_fallback_boundary_kill_switch_restores_the_bare_projection(wired, monkeypatch):
    monkeypatch.setenv("TFB_ENRICHED_BRIDGE_SIGBIND", "0")
    monkeypatch.setenv("TFB_ENRICHED_FALLBACK_BOUNDARY", "0")
    resp = asyncio.run(_post_sheet_rows({"page": "My_Portfolio", "symbols": SYMS}))
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload["meta"]["dispatch"] == "instrument_sheet_rows_fallback"
    assert payload["meta"]["publication_boundary"] is None
    assert payload["meta"]["publication_boundary_rows"] == 0
    for r in payload["row_objects"]:
        assert all(r[k] is None for k in GATE), r   # the v8.5.2 defect, pinned as the OFF state
        assert r["current_price"] == 100.0


# --- B9: the mismatch is no longer silent ------------------------------------

def test_b9_typed_mismatch_logs_a_warning(wired, monkeypatch, caplog):
    monkeypatch.setenv("TFB_ENRICHED_BRIDGE_SIGBIND", "0")
    request = SimpleNamespace(headers={}, scope={}, state=SimpleNamespace())
    with caplog.at_level(logging.WARNING, logger="routes.enriched_quote"):
        out = asyncio.run(eq._delegate_sheet_rows_via_bridge(
            wired.svc, request, "My_Portfolio", "instrument", {"symbols": SYMS}, "", True,
            None, None, None, "req-b"))
    # no call was made: either nothing or the v8.4.0 diagnostic-only envelope
    assert out is None or out.get("_bridge_diagnostic") is True
    assert not eq._payload_has_real_rows(out)
    hits = [rec for rec in caplog.records if "bridge typed mismatch" in rec.getMessage()]
    assert len(hits) == 1 and "fake.analysis" in hits[0].getMessage()
    assert "_analysis_like_impl" in hits[0].getMessage()
    # and under v8.6.0 the same call binds, returns rows, logs nothing
    monkeypatch.delenv("TFB_ENRICHED_BRIDGE_SIGBIND", raising=False)
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="routes.enriched_quote"):
        out = asyncio.run(eq._delegate_sheet_rows_via_bridge(
            wired.svc, request, "My_Portfolio", "instrument", {"symbols": SYMS}, "", True,
            None, None, None, "req-b"))
    assert isinstance(out, dict) and out["meta"]["bridge_call_outcome"] == "success"
    assert not [rec for rec in caplog.records if "bridge typed mismatch" in rec.getMessage()]


# --- B10: version + CI ---------------------------------------------------------

def test_b10_version_and_ci_enrolment():
    assert eq.ROUTER_VERSION == "8.6.0"
    ci = (_REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "tests/test_enriched_bridge_binding.py" in ci


if __name__ == "__main__":   # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
