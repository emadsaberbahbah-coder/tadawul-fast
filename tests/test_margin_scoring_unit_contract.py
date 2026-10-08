"""Margin economics remain fixed across real adapter, scoring and publish seams."""
from __future__ import annotations

import asyncio
import copy
import json
import time

import pytest

from core import data_engine_v2 as de
from core import scoring


SYMBOL = "MARGINCONTRACT.US"
FIELDS = ("gross_margin", "operating_margin", "profit_margin")


@pytest.fixture(autouse=True)
def bounded_modes(monkeypatch):
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "enforce")
    monkeypatch.setenv("TFB_FUND_UNIT_SENTRY", "enforce")
    monkeypatch.setenv("TFB_EODHD_UNIT_AWARE", "1")
    monkeypatch.setenv("TFB_SCORING_SETTLE", "off")
    monkeypatch.setenv("TFB_FC_TUPLE_COHERENT", "off")
    monkeypatch.setenv("TFB_ENGINE_FUND_LKG_REDIS", "0")
    monkeypatch.setenv("TFB_ENGINE_FUND_LKG", "1")
    de._FUND_LKG_STORE.clear()
    yield
    de._FUND_LKG_STORE.clear()


def wire_fundamentals(monkeypatch, points, *, supplier_fields=None):
    from core.providers import eodhd_provider as ep

    monkeypatch.setattr(ep.httpx, "AsyncClient", lambda **kwargs: object())
    client = ep.EODHDClient()
    calls = []

    async def request_json(path, params):
        calls.append(path)
        return {
            "General": {"Code": "MARGINCONTRACT", "Exchange": "US", "CurrencyCode": "USD"},
            "Highlights": supplier_fields if supplier_fields is not None else {
                "GrossMargin": f"{points}%", "OperatingMargin": f"{points}%", "ProfitMargin": f"{points}%",
            },
        }, None

    monkeypatch.setattr(client, "_request_json", request_json)
    values, error = asyncio.run(client.fetch_fundamentals(SYMBOL))
    assert error is None and calls == ["fundamentals/" + SYMBOL]
    return values


def canonical_margin_row(monkeypatch, points, provider="eodhd_fundamentals"):
    wire = wire_fundamentals(monkeypatch, points)
    canonical = de._canonicalize_provider_row(
        wire, requested_symbol=SYMBOL, normalized_symbol=SYMBOL, provider=provider,
    )
    de._fund_unit_contract_apply(canonical, provider, "enforce")
    canonical.update(data_quality="HIGH", current_price=100.0,
                     market_cap=1_000_000.0, pe_ttm=10.0, revenue_ttm=500_000.0,
                     debt_to_equity=0.5, free_cash_flow_ttm=40_000.0,
                     eps_ttm=10.0, rsi_14=50.0, volatility_30d=0.2)
    return canonical


@pytest.mark.parametrize("points", [0.9, 0.005, -0.4, 0.0, 25.0, 180.0, -250.0])
def test_actual_producer_quality_and_full_scores_are_invariant_under_publication(monkeypatch, points):
    row = canonical_margin_row(monkeypatch, points)
    original = copy.deepcopy(row)
    quality = scoring.compute_quality_score(row)
    scores = scoring.compute_scores(row)
    assert row == original  # Scoring reads cannot mutate supplier observations.
    de._margin_publish_contract(row)
    assert row["profit_margin"] == pytest.approx(points / 100.0, abs=0.0000005)
    assert scoring.compute_quality_score(row) == quality
    replay = scoring.compute_scores(row)
    for key in ("quality_score", "overall_score", "recommendation", "recommendation_detailed"):
        assert replay[key] == scores[key], key
    again = copy.deepcopy(row)
    de._margin_publish_contract(row)
    assert row == again


def test_genuine_180_percent_fraction_reaches_the_existing_quality_plateau(monkeypatch):
    large = canonical_margin_row(monkeypatch, 180.0)
    plateau = canonical_margin_row(monkeypatch, 100.0)
    assert large["profit_margin"] == pytest.approx(1.8)
    assert de._margin_unit_valid(large, "profit_margin") == "fraction"
    assert scoring.compute_quality_score(large) == scoring.compute_quality_score(plateau)


def test_proven_ordinary_margin_retains_previous_scores_and_recommendation(monkeypatch):
    row = canonical_margin_row(monkeypatch, 25.0)
    legacy = copy.deepcopy(row)
    legacy.pop(de._MPC_UNIT_KEY)
    before, after = scoring.compute_scores(legacy), scoring.compute_scores(row)
    for key in ("quality_score", "overall_score", "recommendation", "recommendation_detailed"):
        assert after[key] == before[key]


@pytest.mark.parametrize("net,operating", [(0.2762, 0.3262), (1.8, 1.8), (-2.5, -2.5), (0.009, 0.009)])
def test_native_fraction_fields_reach_public_scores_without_changing_economics(monkeypatch, net, operating):
    # Public EODHD demo AAPL.US exposes ProfitMargin=.2762 and
    # OperatingMarginTTM=.3262. The remaining cases bound that field contract.
    wire = wire_fundamentals(monkeypatch, None, supplier_fields={
        "ProfitMargin": net, "OperatingMarginTTM": operating,
    })
    assert wire["profit_margin"] == net
    assert wire["operating_margin"] == operating
    row = de._canonicalize_provider_row(wire, SYMBOL, SYMBOL, "eodhd_fundamentals")
    row.update(data_quality="HIGH", current_price=100.0, market_cap=1_000_000.0,
               pe_ttm=10.0, revenue_ttm=500_000.0, debt_to_equity=0.5,
               free_cash_flow_ttm=40_000.0, eps_ttm=10.0, rsi_14=50.0,
               volatility_30d=0.2)
    original = copy.deepcopy(wire)
    scores = scoring.compute_scores(row)
    de._fund_unit_contract_apply(row, "eodhd_fundamentals", "enforce")
    de._margin_publish_contract(row)
    assert row["profit_margin"] == net and row["operating_margin"] == operating
    replay = scoring.compute_scores(row)
    for key in ("quality_score", "overall_score", "recommendation", "recommendation_detailed"):
        assert replay[key] == scores[key], key
    assert wire == original
    assert row[de._MPC_UNIT_KEY]["profit_margin"]["raw_unit"] == "fraction"
    assert row[de._MPC_UNIT_KEY]["profit_margin"]["unit_basis"] == "supplier_field_contract"


@pytest.mark.parametrize("points", [0.9, 0.005, -0.4, 180.0, -250.0])
def test_actual_producer_local_fallback_quality_is_invariant_under_publication(monkeypatch, points):
    stored = canonical_margin_row(monkeypatch, points)
    public = copy.deepcopy(stored)
    de._margin_publish_contract(public)
    de._compute_scores_local_fallback(stored)
    de._compute_scores_local_fallback(public)
    assert stored["quality_score"] == public["quality_score"]
    assert stored["overall_score"] == public["overall_score"]


def test_primary_canonicalization_keeps_valid_receipts_without_sentry_inference(monkeypatch):
    wire = wire_fundamentals(monkeypatch, 0.9)
    row = de._canonicalize_provider_row(wire, SYMBOL, SYMBOL, "eodhd")
    assert de._margin_unit_valid(row, "profit_margin") == "fraction"
    de._margin_publish_contract(row)
    assert row["profit_margin"] == pytest.approx(0.009)
    proof = row[de._MPC_UNIT_KEY]["profit_margin"]
    assert proof["raw_value"] == 0.9 and proof["raw_unit"] == "percent_points"
    assert proof["provider"] == "eodhd" and proof["transform_version"]


def test_supplier_proof_survives_capture_restore_when_publication_is_off(monkeypatch):
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "off")
    wire = wire_fundamentals(monkeypatch, 0.9)
    row = de._canonicalize_provider_row(wire, SYMBOL, SYMBOL, "eodhd")
    row.update(name="Margin contract", market_cap=1000.0, pe_ttm=10.0,
               revenue_ttm=500.0, current_price=20.0, debt_to_equity=0.3,
               free_cash_flow_ttm=40.0, eps_ttm=2.0, data_quality="HIGH")
    assert de._fund_lkg_capture(SYMBOL, row)
    restored = {"symbol": SYMBOL, "current_price": 20.0, "data_quality": "HIGH"}
    assert de._fund_lkg_restore(SYMBOL, restored)
    assert de._margin_unit_valid(restored, "profit_margin") == "fraction"
    quality = scoring.compute_quality_score(restored)
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "enforce")
    de._margin_publish_contract(restored)
    assert restored["profit_margin"] == pytest.approx(0.009)
    assert scoring.compute_quality_score(restored) == quality
    assert restored[de._MPC_UNIT_KEY]["profit_margin"]["raw_value"] == 0.9


def test_off_mode_sentry_rewrite_retains_actual_supplier_unit_and_lineage(monkeypatch):
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "off")
    row = de._canonicalize_provider_row(wire_fundamentals(monkeypatch, 0.9),
                                         SYMBOL, SYMBOL, "eodhd_fundamentals")
    quality = scoring.compute_quality_score(row)
    de._fund_unit_contract_apply(row, "eodhd_fundamentals", "enforce")
    assert de._margin_unit_valid(row, "profit_margin") == "percent_points"
    assert scoring.compute_quality_score(row) == quality
    assert row[de._MPC_UNIT_KEY]["profit_margin"]["raw_value"] == 0.9
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "enforce")
    de._margin_publish_contract(row)
    assert row["profit_margin"] == pytest.approx(0.009)
    assert scoring.compute_quality_score(row) == quality


def test_explicit_stale_or_unknown_proof_cannot_resort_to_margin_magnitude(monkeypatch):
    row = canonical_margin_row(monkeypatch, 0.9)
    for unit in ("fraction", "unknown"):
        invalid = copy.deepcopy(row)
        for field in FIELDS:
            invalid[field] = 0.9
            invalid[de._MPC_UNIT_KEY][field] = {"unit": unit, "value": 999.0}
        omitted = dict(invalid, gross_margin=None, operating_margin=None, profit_margin=None)
        omitted.pop(de._MPC_UNIT_KEY)
        assert scoring.compute_quality_score(invalid) == scoring.compute_quality_score(omitted)
        de._compute_scores_local_fallback(invalid)
        de._compute_scores_local_fallback(omitted)
        assert invalid["quality_score"] == omitted["quality_score"]


def test_unproved_legacy_rows_keep_existing_score_contract():
    row = {"profit_margin": 0.9, "operating_margin": 0.9, "data_quality": "HIGH"}
    before = scoring.compute_quality_score(row)
    assert before == 73.38
    assert de._MPC_UNIT_KEY not in row
    de._margin_publish_contract(row)
    assert row["profit_margin"] is None and row["operating_margin"] is None


def test_fill_only_primary_merge_never_borrows_proof_for_existing_same_value(monkeypatch):
    wire = wire_fundamentals(monkeypatch, 0.9)
    canonical = de._canonicalize_provider_row(wire, SYMBOL, SYMBOL, "eodhd")
    populated = {"profit_margin": canonical["profit_margin"]}
    landed = de.DataEngineV5()._merge(populated, canonical)
    assert de._margin_unit_valid(landed, "profit_margin") is None
    assert de._margin_unit_valid(landed, "operating_margin") == "fraction"
    de._margin_publish_contract(landed)
    assert landed["profit_margin"] is None
    assert landed["operating_margin"] == pytest.approx(0.009)


@pytest.mark.parametrize("unit,value", [("unknown", 0.009), ("fraction", 999.0)])
def test_explicit_invalid_receipts_survive_off_mode_projection_merge_and_lkg(monkeypatch, unit, value):
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "off")
    wire = wire_fundamentals(monkeypatch, 0.9)
    for field in FIELDS:
        wire[de._MPC_UNIT_KEY][field].update(unit=unit, value=value)
    row = de._canonicalize_provider_row(wire, SYMBOL, SYMBOL, "eodhd")
    row = de.DataEngineV5()._merge({}, row)
    row.update(name="Margin contract", market_cap=1000.0, pe_ttm=10.0,
               revenue_ttm=500.0, current_price=20.0, debt_to_equity=0.3,
               free_cash_flow_ttm=40.0, eps_ttm=2.0, data_quality="HIGH")
    omitted = dict(row, gross_margin=None, operating_margin=None, profit_margin=None, net_margin=None)
    omitted.pop(de._MPC_UNIT_KEY, None)
    assert scoring.compute_quality_score(row) == scoring.compute_quality_score(omitted)
    assert de._fund_lkg_capture(SYMBOL, row)
    restored = {"symbol": SYMBOL, "current_price": 20.0, "data_quality": "HIGH"}
    assert de._fund_lkg_restore(SYMBOL, restored)
    restored_omitted = dict(restored, gross_margin=None, operating_margin=None,
                            profit_margin=None, net_margin=None)
    restored_omitted.pop(de._MPC_UNIT_KEY, None)
    assert scoring.compute_quality_score(restored) == scoring.compute_quality_score(restored_omitted)
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "enforce")
    de._margin_publish_contract(restored)
    assert restored["profit_margin"] is None


def test_off_mode_landed_unproved_margin_cannot_resurrect_destination_receipt(monkeypatch):
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "off")
    base = {"profit_margin": None, de._MPC_UNIT_KEY: {
        "profit_margin": {"unit": "fraction", "value": 0.9},
    }}
    row = de.DataEngineV5()._merge(base, {"profit_margin": 0.9})
    assert de._margin_unit_valid(row, "profit_margin") is None
    assert scoring.compute_quality_score(row) == scoring.compute_quality_score({})
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "enforce")
    de._margin_publish_contract(row)
    assert row["profit_margin"] is None


@pytest.mark.parametrize("unknown", [False, True])
def test_off_mode_warm_fallback_cache_keeps_unit_or_uncertainty(monkeypatch, unknown):
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "off")
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "enforce")
    monkeypatch.setenv("TFB_EODHD_FUNDAMENTALS_FALLBACK", "1")
    de._FUND_NEG_STORE.clear()
    row = canonical_margin_row(monkeypatch, 0.9)
    row.update(name="Margin contract", industry="Software", sector="Technology")
    if unknown:
        for field in FIELDS:
            row[de._MPC_UNIT_KEY][field]["unit"] = "unknown"
    assert de._fund_lkg_capture(SYMBOL, row)
    engine = de.DataEngineV5()

    async def unexpected_provider(*args, **kwargs):
        pytest.fail("a warm fund cache must avoid provider I/O")

    monkeypatch.setattr(engine, "_fetch_eodhd_fundamentals_patch", unexpected_provider)
    restored = asyncio.run(engine._apply_eodhd_fundamentals_fallback(
        {"symbol": SYMBOL, "current_price": 100.0, "data_quality": "HIGH"},
        SYMBOL, "Global_Markets",
    ))
    assert "fund_cache:hit:" in restored["warnings"]
    assert restored[de._MPC_UNIT_KEY]["profit_margin"]["unit"] == (
        "unknown" if unknown else "percent_points")
    public = copy.deepcopy(restored)
    de._compute_scores_local_fallback(restored)
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "enforce")
    de._margin_publish_contract(public)
    de._compute_scores_local_fallback(public)
    assert restored["quality_score"] == public["quality_score"]
    assert restored["overall_score"] == public["overall_score"]
    if unknown:
        assert public["profit_margin"] is None
    else:
        assert public["profit_margin"] == pytest.approx(0.009)


def test_partial_merge_and_cache_preserve_untouched_global_uncertainty(monkeypatch):
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "off")
    patch = de._canonicalize_provider_row(wire_fundamentals(monkeypatch, 0.9),
                                           SYMBOL, SYMBOL, "eodhd")
    row = de.DataEngineV5()._merge(
        {"operating_margin": 0.9, de._MPC_UNIT_KEY: None}, patch,
    )
    row.update(name="Margin contract", market_cap=1000.0, pe_ttm=10.0,
               revenue_ttm=500.0, current_price=20.0, debt_to_equity=0.3,
               free_cash_flow_ttm=40.0, eps_ttm=2.0, data_quality="HIGH")
    omitted = dict(row, operating_margin=None)
    assert scoring.compute_quality_score(row) == scoring.compute_quality_score(omitted)
    assert de._fund_lkg_capture(SYMBOL, row)
    restored = {"symbol": SYMBOL, "current_price": 20.0, "data_quality": "HIGH"}
    assert de._fund_lkg_restore(SYMBOL, restored)
    assert restored[de._MPC_UNIT_KEY]["operating_margin"]["unit"] == "unknown"
    assert scoring.compute_quality_score(restored) == scoring.compute_quality_score(
        dict(restored, operating_margin=None))


@pytest.mark.parametrize("storage", ["memory", "redis"])
@pytest.mark.parametrize("route", ["restore", "warm_fallback"])
@pytest.mark.parametrize("publish_mode", ["off", "observe", "enforce"])
def test_pre_fix_cache_quarantines_only_margin_units(monkeypatch, storage, route, publish_mode):
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", publish_mode)
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "enforce")
    monkeypatch.setenv("TFB_EODHD_FUNDAMENTALS_FALLBACK", "1")
    de._FUND_NEG_STORE.clear()
    fields = {"name": "Margin contract", "industry": "Software", "sector": "Technology",
              "currency": "USD", "market_cap": 1000.0, "pe_ttm": 10.0,
              "revenue_ttm": 500.0, "profit_margin": 0.002762, "operating_margin": 0.003262,
              "eps_ttm": 2.0, "debt_to_equity": 0.3, "free_cash_flow_ttm": 40.0}
    entry = {"ts": time.time(), "name": fields["name"], "fields": fields,
             "margin_units": {"profit_margin": {"unit": "fraction", "value": 0.002762}}}
    original = copy.deepcopy(entry)
    if storage == "memory":
        de._FUND_LKG_STORE[SYMBOL] = entry
    else:
        class RedisReader:
            def get(self, key):
                return json.dumps(entry)

        monkeypatch.setattr(de, "_fund_lkg_redis_client", lambda: RedisReader())
    row = {"symbol": SYMBOL, "current_price": 20.0, "data_quality": "HIGH"}
    if route == "restore":
        restore_tag = de._fund_lkg_restore(SYMBOL, row)
        assert restore_tag
        # The production orchestrator appends the returned restore receipt;
        # that existing provenance gate prevents a restored row from reseeding.
        de._v573_append_warning(row, restore_tag)
    else:
        engine = de.DataEngineV5()

        async def unexpected_provider(*args, **kwargs):
            pytest.fail("non-margin cache data must remain usable without provider I/O")

        monkeypatch.setattr(engine, "_fetch_eodhd_fundamentals_patch", unexpected_provider)
        row = asyncio.run(engine._apply_eodhd_fundamentals_fallback(row, SYMBOL, "Global_Markets"))
        assert "fund_cache:hit:" in row["warnings"]
    for field, value in fields.items():
        assert row[field] == value
    for field in ("profit_margin", "operating_margin"):
        assert row[de._MPC_UNIT_KEY][field]["unit"] == "unknown"
    omitted = dict(row, profit_margin=None, operating_margin=None)
    assert scoring.compute_quality_score(row) == scoring.compute_quality_score(omitted)
    de._margin_publish_contract(row)
    if publish_mode == "enforce":
        assert row["profit_margin"] is None and row["operating_margin"] is None
    assert entry == original
    assert not de._fund_lkg_capture(SYMBOL, row)  # Restore cannot certify itself.


@pytest.mark.parametrize("version", [None, "obsolete_margin_policy"])
def test_old_cache_redis_write_cannot_upgrade_margin_receipts(monkeypatch, version):
    writes = []

    class RedisStorage:
        def setex(self, key, ttl, payload):
            writes.append(json.loads(payload))

        def get(self, key):
            return json.dumps(writes[-1])

    monkeypatch.setattr(de, "_fund_lkg_redis_client", lambda: RedisStorage())
    entry = {"ts": time.time(), "fields": {"market_cap": 1000.0, "profit_margin": 0.002762},
             "margin_units": {"profit_margin": {"unit": "fraction", "value": 0.002762}}}
    if version is not None:
        entry["margin_contract_version"] = version
    assert de._fund_lkg_redis_set(SYMBOL, entry)
    assert writes[-1]["margin_units"]["profit_margin"]["unit"] == "unknown"
    assert writes[-1].get("margin_contract_version") != "source_margin_units_v1"
    restored = de._fund_lkg_redis_get(SYMBOL)
    assert restored["fields"] == entry["fields"]
    assert restored["margin_units"]["profit_margin"]["unit"] == "unknown"


def test_coherence_compares_proven_fraction_economics_and_preserves_raw_facts(monkeypatch):
    wire = wire_fundamentals(monkeypatch, 25.0)
    row = de._canonicalize_provider_row(wire, SYMBOL, SYMBOL, "eodhd")
    row.update(market_cap=1000.0, pe_ttm=10.0, revenue_ttm=400.0)
    before = copy.deepcopy(row)
    assert de._fund_coherence_sentry(row, "enforce") is None
    assert row == before
    assert row[de._MPC_UNIT_KEY]["profit_margin"]["raw_value"] == 25.0


def test_proven_economic_divergence_cannot_be_repaired_as_a_unit_change(monkeypatch):
    wire = wire_fundamentals(monkeypatch, 0.25)
    source = copy.deepcopy(wire)
    row = de._canonicalize_provider_row(wire, SYMBOL, SYMBOL, "eodhd")
    row.update(market_cap=1000.0, pe_ttm=10.0, revenue_ttm=400.0)
    assert de._fund_coherence_sentry(row, "enforce") == "fund_coherence_quarantined:profit_margin"
    assert row["profit_margin"] is None
    assert wire == source


@pytest.mark.parametrize("receipt", [None, [], {"unit": [], "value": 0.009},
                                    {"unit": "fraction", "value": float("nan")}])
def test_malformed_explicit_receipts_exclude_margin_without_raising(monkeypatch, receipt):
    row = canonical_margin_row(monkeypatch, 0.9)
    for field in FIELDS:
        row[de._MPC_UNIT_KEY][field] = copy.deepcopy(receipt)
    omitted = dict(row, gross_margin=None, operating_margin=None, profit_margin=None, net_margin=None)
    omitted.pop(de._MPC_UNIT_KEY)
    assert scoring.compute_quality_score(row) == scoring.compute_quality_score(omitted)
