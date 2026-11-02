"""EPS estimates must never masquerade as reported trailing earnings.

Drive the real adapter and engine; replace only the supplier HTTP boundary.
"""
import asyncio
import copy
import json
import time

import pytest

from core import data_engine_v2 as de
from core.providers import eodhd_provider as ep


def wire(monkeypatch, highlights, financials=None, shares=None):
    monkeypatch.setattr(ep.httpx, "AsyncClient", lambda **kwargs: object())
    client = ep.EODHDClient()
    calls = []

    async def request(path, params):
        calls.append(path)
        return {"General": {"Code": "SYNTH", "Exchange": "US", "CurrencyCode": "USD",
                            "UpdatedAt": "2026-10-22"},
                "Highlights": {"MostRecentQuarter": "2026-09-30", **highlights},
                "Financials": financials or {}, "SharesStats": shares or {}}, None

    monkeypatch.setattr(client, "_request_json", request)
    return client, calls


@pytest.mark.parametrize("estimate", [7.5, 0.0, -2.0])
def test_estimate_only_is_not_ttm_in_real_adapter(monkeypatch, estimate):
    client, calls = wire(monkeypatch, {"EPSEstimateCurrentYear": estimate})
    row, err = asyncio.run(client.fetch_fundamentals("SYNTH.US"))
    assert err is None and row.get("eps_ttm") is None
    assert row["eps_estimate_current_year"] == estimate
    proof = row["_eps_basis"]["eps_estimate_current_year"]
    assert proof["basis"] == "current_fiscal_year_estimate"
    assert proof["source_field"] == "Highlights.EPSEstimateCurrentYear"
    assert proof["provider_updated_at"] == "2026-10-22"
    assert proof["collected_at_utc"] == row["last_updated_utc"]
    assert "eps_ttm_estimate_excluded" in row["warnings"]
    assert calls == ["fundamentals/SYNTH.US"]


@pytest.mark.parametrize("actual", [4.0, 0.0, -3.0])
def test_reported_actual_retained_with_separate_estimate(monkeypatch, actual):
    client, _ = wire(monkeypatch, {"EarningsShare": actual, "EPSEstimateCurrentYear": 9.0})
    row, err = asyncio.run(client.fetch_fundamentals("SYNTH.US"))
    assert err is None and row["eps_ttm"] == actual
    assert row["eps_estimate_current_year"] == 9.0
    proof = row["_eps_basis"]["eps_ttm"]
    assert proof["value"] == actual and proof["basis"] == "reported_diluted_ttm"
    assert proof["source_field"] == "Highlights.EarningsShare"
    assert proof["period_end"] == "2026-09-30"


@pytest.mark.parametrize("bad", [None, "bad", True, float("nan"), float("inf")])
def test_invalid_primary_does_not_hide_reported_ttm_alias(monkeypatch, bad):
    client, _ = wire(monkeypatch, {"EarningsShare": bad, "DilutedEpsTTM": 2.5,
                                 "EPSEstimateCurrentYear": 8.0})
    row, _ = asyncio.run(client.fetch_fundamentals("SYNTH.US"))
    assert row["eps_ttm"] == 2.5
    assert row["_eps_basis"]["eps_ttm"]["source_field"] == "Highlights.DilutedEpsTTM"


@pytest.mark.parametrize("raw", [{"epsCurrentYear": 8.0}, {"EPSEstimateCurrentYear": 8.0},
                                 {"fundamentals": {"epsCurrentYear": 8.0}}])
def test_engine_aliases_keep_estimate_out_of_ttm(raw):
    before = copy.deepcopy(raw)
    row = de._canonicalize_provider_row(raw, "SYNTH.US", "SYNTH.US", "synthetic")
    assert row.get("eps_ttm") is None
    assert row["eps_estimate_current_year"] == 8.0
    assert raw == before
    again = de._canonicalize_provider_row(row, "SYNTH.US", "SYNTH.US", "synthetic")
    assert again.get("eps_ttm") is None and again["eps_estimate_current_year"] == 8.0


def test_actual_and_estimate_survive_engine_and_do_not_relax_coherence(monkeypatch):
    client, _ = wire(monkeypatch, {"EarningsShare": 2.0, "EPSEstimateCurrentYear": 20.0})
    patch, _ = asyncio.run(client.fetch_fundamentals("SYNTH.US"))
    row = de._canonicalize_provider_row(patch, "SYNTH.US", "SYNTH.US", "eodhd")
    assert row["eps_ttm"] == 2.0 and row["eps_estimate_current_year"] == 20.0
    assert row["_eps_basis"] == patch["_eps_basis"]
    monkeypatch.setenv("TFB_ENGINE_PE_COHERENCE", "1")
    row.update(current_price=200.0, pe_ttm=5.0)
    assert de._engine_apply_pe_coherence(row) == "pe_incoherent_cleared:engine"
    assert row["eps_ttm"] is None and row["pe_ttm"] is None
    assert "eps_ttm" not in row["_eps_basis"]
    assert row["_eps_basis"]["eps_estimate_current_year"]["value"] == 20.0


def test_old_provider_cache_cannot_reintroduce_estimated_ttm(monkeypatch):
    client, calls = wire(monkeypatch, {"EarningsShare": 2.0})

    async def drive():
        margin_version = ep._MARGIN_FRACTION_VERSION if ep._eodhd_unit_aware_enabled() else ep._MARGIN_LEGACY_VERSION
        await client.fund_cache.set(f"f:{margin_version}:SYNTH.US", {"eps_ttm": 99.0})
        first = await client.fetch_fundamentals("SYNTH.US")
        second = await client.fetch_fundamentals("SYNTH.US")
        return first, second

    (first, err), (second, err2) = asyncio.run(drive())
    assert err is err2 is None and first["eps_ttm"] == second["eps_ttm"] == 2.0
    assert calls == ["fundamentals/SYNTH.US"]


def test_old_persisted_fundamentals_namespace_is_not_consumed(monkeypatch):
    old = {"ts": time.time(), "fields": {"eps_ttm": 99.0, "pe_ttm": 10.0}}
    reads = []

    class Redis:
        def get(self, key):
            reads.append(key)
            return json.dumps(old) if key == "tfb:fund_lkg:v1:SYNTH.US" else None

    monkeypatch.setattr(de, "_fund_lkg_redis_client", lambda: Redis())
    assert de._fund_lkg_redis_get("SYNTH.US") is None
    assert reads and "tfb:fund_lkg:v1:SYNTH.US" not in reads


@pytest.mark.parametrize("quarters", [1, 3, 4])
def test_income_over_current_shares_proxy_never_becomes_reported_ttm(monkeypatch, quarters):
    periods = ["2026-09-30", "2026-06-30", "2026-03-31", "2025-12-31"][:quarters]
    financials = {"Income_Statement": {"quarterly": {
        date: {"date": date, "netIncome": 200.0} for date in periods
    }}}
    client, _ = wire(monkeypatch, {"EPSEstimateCurrentYear": 8.0}, financials,
                     {"SharesOutstanding": 100.0})
    patch, _ = asyncio.run(client.fetch_fundamentals("SYNTH.US"))
    row = de._canonicalize_provider_row(patch, "SYNTH.US", "SYNTH.US", "eodhd")
    assert row.get("eps_ttm") is None
    assert row["eps_net_income_current_shares"] == 2.0 * quarters
    assert row["eps_estimate_current_year"] == 8.0
    proof = row["_eps_basis"]["eps_net_income_current_shares"]
    assert proof["basis"] == "derived_net_income_over_current_shares"
    assert proof["period_end"] is None
    assert "eps_ttm_proxy_excluded" in row["warnings"]
    assert "eps_ttm_unavailable" in row["warnings"]


def test_fill_only_merge_keeps_proof_with_the_selected_scalar():
    engine = de.DataEngineV5()
    patch = {"eps_ttm": 3.0, "_eps_basis": {"eps_ttm": {"value": 3.0, "provider": "eodhd"}}}
    row = engine._merge({"eps_ttm": 2.0}, patch)
    assert row["eps_ttm"] == 2.0 and "_eps_basis" not in row
    base = {"eps_ttm": 2.0, "_eps_basis": {"eps_ttm": {"value": 2.0, "provider": "other"}}}
    row = engine._merge(base, patch)
    assert row["_eps_basis"]["eps_ttm"] == base["_eps_basis"]["eps_ttm"]
    row = engine._merge({}, patch)
    assert row["eps_ttm"] == row["_eps_basis"]["eps_ttm"]["value"] == 3.0
    row["_eps_basis"]["eps_ttm"]["provider"] = "mutated"
    assert patch["_eps_basis"]["eps_ttm"]["provider"] == "eodhd"


def test_stale_or_missing_source_proof_is_not_reattached():
    row = de._canonicalize_provider_row({"eps_ttm": 2.0,
        "_eps_basis": {"eps_ttm": {"value": 3.0}}}, "SYNTH.US", "SYNTH.US", "eodhd")
    assert row["eps_ttm"] == 2.0 and "_eps_basis" not in row


def test_snapshot_and_live_overwrite_follow_the_selected_value():
    base = {"eps_ttm": 2.0, "_eps_basis": {"eps_ttm": {"value": 2.0, "provider": "old"}}}
    patch = {"eps_ttm": 3.0, "_eps_basis": {"eps_ttm": {"value": 3.0, "provider": "eodhd"}}}
    filled = de._merge_missing_fields(base, patch)
    assert filled["eps_ttm"] == filled["_eps_basis"]["eps_ttm"]["value"] == 2.0
    assert "_eps_basis" not in de._merge_missing_fields({"eps_ttm": 2.0}, patch)
    overwritten = de._overwrite_live_fields(base, patch)
    assert overwritten["eps_ttm"] == overwritten["_eps_basis"]["eps_ttm"]["value"] == 3.0
    assert "_eps_basis" not in de._overwrite_live_fields(base, {"eps_ttm": 3.0})


def test_lkg_restore_does_not_attach_the_degraded_rows_proof(monkeypatch):
    monkeypatch.setenv("TFB_ENGINE_FUND_LKG", "1")
    monkeypatch.setenv("TFB_ENGINE_FUND_LKG_MIN_FIELDS", "8")
    monkeypatch.setitem(de._FUND_LKG_STORE, "SYNTH.US", {
        "ts": time.time(), "fields": {"eps_ttm": 3.0, "pe_ttm": 10.0}, "name": "Synthetic"})
    row = {"eps_ttm": None, "_eps_basis": {"eps_ttm": {"value": 2.0}}}
    assert de._fund_lkg_restore("SYNTH.US", row)
    assert row["eps_ttm"] == 3.0 and "_eps_basis" not in row


def test_proxy_receipt_identifies_highlights_denominator(monkeypatch):
    client, _ = wire(monkeypatch, {"SharesOutstanding": 100.0}, {
        "Income_Statement": {"quarterly": {"2026-09-30": {"netIncome": 200.0}}}})
    row, _ = asyncio.run(client.fetch_fundamentals("SYNTH.US"))
    assert row.get("eps_ttm") is None
    assert row["_eps_basis"]["eps_net_income_current_shares"]["source_field"].endswith("/Highlights.SharesOutstanding")


@pytest.mark.parametrize("existing_eps", [None, 2.0])
def test_real_fundamentals_fallback_keeps_diagnostics_and_only_landed_proof(monkeypatch, existing_eps):
    monkeypatch.setenv("TFB_EODHD_FUNDAMENTALS_FALLBACK", "1")
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "off")
    monkeypatch.setenv("TFB_FUND_UNIT_SENTRY", "off")
    engine = de.DataEngineV5()
    patch = {"symbol": "SYNTH.US", "eps_ttm": 3.0,
             "eps_estimate_current_year": 8.0, "eps_net_income_current_shares": 2.5,
             "debt_to_equity": 0.3, "free_cash_flow_ttm": 10.0,
             "_eps_basis": {key: {"value": value, "provider": "eodhd"}
                            for key, value in (("eps_ttm", 3.0),
                                ("eps_estimate_current_year", 8.0),
                                ("eps_net_income_current_shares", 2.5))}}

    async def fetch(symbol, page):
        return copy.deepcopy(patch)

    monkeypatch.setattr(engine, "_fetch_eodhd_fundamentals_patch", fetch)
    row = asyncio.run(engine._apply_eodhd_fundamentals_fallback(
        {"symbol": "SYNTH.US", "eps_ttm": existing_eps}, "SYNTH.US", "Global_Markets"))
    assert row["eps_ttm"] == (3.0 if existing_eps is None else existing_eps)
    assert row["eps_estimate_current_year"] == 8.0
    assert row["eps_net_income_current_shares"] == 2.5
    assert row["_eps_basis"]["eps_estimate_current_year"]["value"] == 8.0
    if existing_eps is None:
        assert row["_eps_basis"]["eps_ttm"]["value"] == 3.0
    else:
        assert "eps_ttm" not in row["_eps_basis"]
    row = de.DataEngineV5()._merge({"eps_ttm": None,
        "_eps_basis": {"eps_ttm": {"value": 3.0}}}, {"eps_ttm": 2.0})
    assert row["eps_ttm"] == 2.0 and "_eps_basis" not in row
