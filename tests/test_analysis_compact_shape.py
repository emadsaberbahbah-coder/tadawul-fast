"""routes/analysis_sheet_rows.py v4.9.0 [COMPACT-SHAPE]: the opt-in market-objects.v1 response.

C1 without the parameter, or with another value, the envelope is the legacy one (the same keys and values)
C2 compact = legacy on every field of every row (115 on Global_Markets): row_objects, display rows and the matrix
C3 the contract is the legacy keys and headers, in order; status, request id, version, page and meta carried
C4 the query string (GET or POST) and the POST body both ask for it; the caller's body is not changed
C5 the runtime fallback envelope is compacted too
C6 the compact form is smaller than the legacy one
C7 version pins
"""
from __future__ import annotations

import asyncio
import copy
import json
from types import SimpleNamespace

import pytest
from fastapi import Request

from routes import analysis_sheet_rows as route

SYMBOLS = ["SYNTH1.US", "SYNTH2.US", "SYNTH3.US"]


def quote(symbol, i):
    return {
        "symbol": symbol, "name": "Synthetic compact fixture %d" % i,
        "asset_class": "Equity", "currency": "USD", "exchange": "NASDAQ",
        "current_price": 100.0 + i, "forecast_price_12m": 120.0 + i,
        "expected_roi_12m": 0.2, "forecast_source": "provider_target",
        "recommendation": "BUY", "recommendation_detailed": "BUY",
        "investability_status": "INVESTABLE", "final_action": "INVEST",
        "overall_score": 80.0 - i, "risk_score": 30.0,
        "quality_score": 70.0, "forecast_confidence": 0.7,
        "market_cap": 1e9 * (i + 1), "pe_ttm": 20.0, "eps_ttm": (100.0 + i) / 20.0,
        "debt_to_equity": 0.5, "week_52_low": 80.0, "week_52_high": 150.0,
        "avg_volume_30d": 100000.0, "volatility_30d": 0.2,
        "warnings": "acquisition_status:success",
    }


@pytest.fixture(autouse=True)
def bounded_environment(monkeypatch):
    for name, value in (("TFB_FC_TUPLE_COHERENT", "off"), ("TFB_SCORING_SETTLE", "off"),
                        ("TFB_ROI12_SOFTCAP_GATE", "off"), ("TFB_TUPLE_NOISE_RELEASE", "off"),
                        ("TFB_INVESTABILITY_GATE", "0"), ("TFB_GATE_RECO_COHERENCE", "0"),
                        ("TFB_MARGIN_PUBLISH", "off"), ("TFB_FORECAST_PAIR_COHERENCE", "0"),
                        ("TFB_PRICE_PRECISION", "off"), ("TFB_ANALYSIS_GLOBAL_RANK", "1")):
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(route, "get_settings_cached", lambda: None)
    monkeypatch.setattr(route, "_auth_passed", lambda **kwargs: True)

    class Engine:
        async def get_enriched_quotes(self, symbols, **kwargs):
            return [quote(s, i) for i, s in enumerate(symbols)]

    async def unavailable_proxy(**kwargs):
        return None, {"proxy_call_outcome": "raised"}

    async def unavailable_adapter(**kwargs):
        return None, "synthetic-adapter", [], "raised"

    async def engine_binding(request):
        return Engine(), "synthetic-offline"

    monkeypatch.setattr(route, "_proxy_callable", unavailable_proxy)
    monkeypatch.setattr(route, "_call_core_sheet_rows_best_effort", unavailable_adapter)
    monkeypatch.setattr(route, "_get_engine", engine_binding)


def make_request(method="POST", query=b""):
    return Request({"type": "http", "method": method, "path": "/v1/analysis/sheet-rows", "headers": [],
                    "query_string": query, "app": SimpleNamespace(state=SimpleNamespace())})


def call(body=None, query=b"", method="POST", page="Global_Markets"):
    body = {"page": page, "symbols": list(SYMBOLS), "limit": 10} if body is None else body
    return asyncio.run(route._analysis_sheet_rows_impl(
        make_request(method, query), body, "", True, None, None, None, None, "req-compact-1"))


def no_clock(payload):
    out = copy.deepcopy(payload)
    out.get("meta", {}).pop("duration_ms", None)
    return out


def test_c1_default_and_other_values_are_the_legacy_envelope():
    base = call()
    assert "response_shape" not in base and "response_shape" not in base["meta"]
    assert {"row_objects", "display_row_objects", "rows_matrix", "headers", "keys"} <= set(base)
    for query in (b"response_shape=", b"response_shape=market-objects.v2", b"response_shape=legacy"):
        assert no_clock(call(query=query)) == no_clock(base), query


def test_c2_compact_equals_legacy_on_every_field():
    legacy = call()
    compact = call(query=b"response_shape=market-objects.v1")
    keys, headers = legacy["keys"], legacy["headers"]
    assert len(keys) == 115 and len(headers) == 115
    assert compact["count"] == legacy["count"] == len(SYMBOLS)
    compared = 0
    for i, row in enumerate(compact["rows"]):
        assert list(row) == keys, "a compact row carries the contract keys in contract order"
        for j, key in enumerate(keys):
            assert row[key] == legacy["row_objects"][i][key]
            assert row[key] == legacy["display_row_objects"][i][headers[j]]
            assert row[key] == legacy["rows_matrix"][i][j]
            compared += 1
    assert compared == 115 * len(SYMBOLS)
    assert [r["symbol"] for r in compact["rows"]] == [r["symbol"] for r in legacy["row_objects"]]


def test_c3_contract_status_and_meta():
    legacy = call()
    compact = call(query=b"response_shape=Market-Objects.V1")
    assert compact["response_shape"] == "market-objects.v1"
    assert compact["contract"] == {"keys": legacy["keys"], "headers": legacy["headers"], "count": 115}
    for k in ("status", "request_id", "version", "page", "route_family", "detail", "error"):
        assert compact[k] == legacy[k], k
    assert compact["request_id"] == "req-compact-1"
    assert compact["version"] == "4.9.0"
    meta = dict(compact["meta"])
    assert meta.pop("response_shape") == "market-objects.v1"
    assert no_clock({"meta": meta}) == no_clock({"meta": legacy["meta"]})
    assert set(compact) == {"response_shape", "status", "request_id", "version", "page", "route_family", "contract",
                            "rows", "count", "detail", "error", "meta"}


def test_c4_query_on_get_and_body_on_post_and_the_body_is_kept():
    via_query_get = call(method="GET", query=b"response_shape=market-objects.v1")
    body = {"page": "Global_Markets", "symbols": list(SYMBOLS), "limit": 10, "response_shape": "market-objects.v1"}
    before = copy.deepcopy(body)
    via_body = call(body=body)
    assert body == before
    assert via_query_get["response_shape"] == via_body["response_shape"] == "market-objects.v1"
    assert via_query_get["rows"] == via_body["rows"]
    assert route._without_response_shape(body) == {k: v for k, v in body.items() if k != "response_shape"}
    assert route._without_response_shape(["x"]) == ["x"]
    assert route._requested_response_shape(SimpleNamespace(), {"response_shape": " Market-Objects.V1 "}) == "market-objects.v1"
    assert route._requested_response_shape(make_request(query=b"response_shape=a"), {"response_shape": "b"}) == "a"


def test_c5_the_runtime_fallback_is_compacted(monkeypatch):
    async def boom(**kwargs):
        raise RuntimeError("synthetic failure")

    monkeypatch.setattr(route, "_analysis_sheet_rows_impl_core", boom)
    legacy = call()
    compact = call(query=b"response_shape=market-objects.v1")
    assert legacy["status"] == compact["status"] == "partial"
    assert compact["rows"] == [] and compact["count"] == 0
    assert compact["contract"]["keys"] == legacy["keys"]
    assert compact["error"] == legacy["error"]
    assert compact["meta"]["dispatch"] == "analysis_sheet_rows_emergency_fallback"


def test_c6_compact_is_smaller():
    legacy = json.dumps(call(), default=str)
    compact = json.dumps(call(query=b"response_shape=market-objects.v1"), default=str)
    assert len(compact) * 3 < len(legacy)


def test_c7_version_pins():
    assert route.ANALYSIS_SHEET_ROWS_VERSION == "4.9.0"
    assert route.RESPONSE_SHAPE_COMPACT == "market-objects.v1"
