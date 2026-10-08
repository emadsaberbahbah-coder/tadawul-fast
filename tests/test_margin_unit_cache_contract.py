"""Synthetic production-path regressions for value-bound margin units.

Only provider wire calls, Redis commands and external row readers are fixtures.
The canonicalization, sentry, fill-only merge, LKG/cache and publication code
all run unchanged. Adapter wire tests also run in the pinned contract env.
"""
from __future__ import annotations

import asyncio
import copy
import json
import time
import types

import pytest

import core.data_engine_v2 as de


SYMBOL = "SYNTH.US"
UNIT_KEY = "_margin_unit_basis"


@pytest.fixture(autouse=True)
def clean_contract_state(monkeypatch):
    for key in (
        "TFB_ENGINE_FUND_LKG_MIN_FIELDS", "TFB_EODHD_FUND_CACHE_TTL_H",
        "TFB_EODHD_FUND_FALLBACK_SKIP_PAGES", "TFB_CLEAR_STALE_IDENTITY",
        "REDIS_URL",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "enforce")
    monkeypatch.setenv("TFB_FUND_UNIT_SENTRY", "enforce")
    monkeypatch.setenv("TFB_EODHD_UNIT_AWARE", "1")
    monkeypatch.setenv("TFB_ENGINE_FUND_LKG", "1")
    monkeypatch.setenv("TFB_ENGINE_FUND_LKG_REDIS", "0")
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "off")
    monkeypatch.setenv("TFB_EODHD_FUNDAMENTALS_FALLBACK", "1")
    de._FUND_LKG_STORE.clear()
    de._FUND_NEG_STORE.clear()
    previous_redis = copy.copy(de._FUND_LKG_REDIS_STATE)
    for key in de._FUND_CACHE_STATS:
        de._FUND_CACHE_STATS[key] = 0
    yield
    de._FUND_LKG_STORE.clear()
    de._FUND_NEG_STORE.clear()
    de._FUND_LKG_REDIS_STATE.clear()
    de._FUND_LKG_REDIS_STATE.update(previous_redis)


def complete_base():
    # Exactly the default eight present LKG fields, with numeric anchors.
    return {
        "symbol": SYMBOL, "name": "Synthetic Company", "industry": "Software",
        "sector": "Technology", "currency": "USD", "country": "USA",
        "market_cap": 1000.0, "pe_ttm": 10.0, "revenue_ttm": 500.0,
        "current_price": 20.0,
    }


def canonical_patch(values, provider="eodhd_fundamentals", mode="enforce"):
    patch = de._canonicalize_provider_row(
        dict(values), requested_symbol=SYMBOL, normalized_symbol=SYMBOL,
        provider=provider,
    )
    de._fund_unit_contract_apply(patch, provider, mode)
    return patch


@pytest.mark.parametrize("fraction", [0.00005, 0.005, 0.009, 0.0, -0.004, 0.25, 1.8])
def test_reused_explicit_patch_cannot_cross_unit_seam_twice(fraction):
    patch = canonical_patch({"profit_margin": fraction})
    once = copy.deepcopy(patch)
    assert de._fund_unit_contract_apply(patch, "eodhd_fundamentals", "enforce") == []
    assert patch == once
    row = de.DataEngineV5()._merge(complete_base(), patch)
    assert de._fund_lkg_capture(SYMBOL, row)
    restored = {"symbol": SYMBOL, "current_price": 20.0}
    assert de._fund_lkg_restore(SYMBOL, restored)
    de._margin_publish_contract(restored)
    assert restored["profit_margin"] == pytest.approx(round(fraction, 6))
    published = copy.deepcopy(restored)
    assert de._margin_publish_contract(restored) == 0 and restored == published


def test_changed_patch_value_cannot_reuse_or_reissue_stale_proof():
    patch = canonical_patch({"profit_margin": 0.005})
    patch["profit_margin"] = 0.9
    de._fund_unit_contract_apply(patch, "eodhd_fundamentals", "enforce")
    assert de._margin_unit_valid(patch, "profit_margin") is None
    de._margin_publish_contract(patch)
    assert patch["profit_margin"] is None
    assert "margin_publish:profit_margin:unresolved" in patch["warnings"]


def engine_with_wire(values):
    calls = []
    module = types.ModuleType("synthetic_eodhd_wire")

    def fetch_fundamentals_patch(symbol):
        calls.append(symbol)
        return copy.deepcopy(values)

    module.fetch_fundamentals_patch = fetch_fundamentals_patch
    engine = de.DataEngineV5()
    engine._provider_registry._modules["eodhd"] = module
    return engine, calls


def apply_fallback(engine, row):
    return asyncio.run(engine._apply_eodhd_fundamentals_fallback(
        copy.deepcopy(row), SYMBOL, "Global_Markets",
    ))


class RedisWire:
    """The only L2 fixture: real serialization and validation surround it."""

    def __init__(self):
        self.values = {}
        self.writes = []

    def setex(self, key, ttl, payload):
        self.writes.append((key, ttl, payload))
        self.values[key] = payload

    def get(self, key):
        return self.values.get(key)


def redis_wire(monkeypatch):
    wire = RedisWire()
    monkeypatch.setenv("TFB_ENGINE_FUND_LKG_REDIS", "1")
    monkeypatch.setenv("REDIS_URL", "redis://synthetic.invalid:6379/0")
    de._FUND_LKG_REDIS_STATE.update(
        client=wire, consecutive_errors=0, breaker_until=0.0,
    )
    return wire


@pytest.mark.parametrize("fraction", [0.009, 0.0, -0.004, 0.25, 1.8])
def test_canonical_sentry_capture_restore_and_publish_preserve_unit(fraction):
    # Synthetic provider values already use EODHD's unit-aware fraction contract.
    engine, calls = engine_with_wire({
        "profit_margin": fraction, "debt_to_equity": 0.3,
        "free_cash_flow_ttm": 40.0,
    })
    row = apply_fallback(engine, complete_base())
    assert calls == [SYMBOL]
    expected_stored = round(fraction * 100.0, 4) if abs(fraction) <= 1.5 else fraction
    assert row["profit_margin"] == expected_stored
    assert de._margin_unit_valid(row, "profit_margin") == (
        "percent_points" if abs(fraction) <= 1.5 else "fraction"
    )
    assert de._fund_lkg_min_fields() == 8
    assert de._fund_lkg_capture(SYMBOL, row) is True
    captured = copy.deepcopy(de._FUND_LKG_STORE[SYMBOL])

    direct = copy.deepcopy(row)
    de._margin_publish_contract(direct)
    assert direct["profit_margin"] == pytest.approx(fraction, abs=0.0000005)
    snapshot = copy.deepcopy(direct)
    assert de._margin_publish_contract(direct) == 0
    assert direct == snapshot

    restored = {"symbol": SYMBOL}
    assert de._fund_lkg_restore(SYMBOL, restored).startswith("fundamentals_lkg:")
    de._margin_publish_contract(restored)
    assert restored["profit_margin"] == direct["profit_margin"]
    assert de._FUND_LKG_STORE[SYMBOL] == captured
    again = copy.deepcopy(restored)
    assert de._margin_publish_contract(restored) == 0
    assert restored == again


def test_eodhd_wire_thin_points_reach_fraction_after_capture_restore(monkeypatch):
    pytest.importorskip("httpx")
    from core.providers import eodhd_provider as ep

    # No HTTP commands are sent; client construction is the external I/O seam.
    # The pinned contract env intentionally omits the optional HTTP/2 extra.
    monkeypatch.setattr(ep.httpx, "AsyncClient", lambda **kwargs: object())
    client = ep.EODHDClient()
    requests = []

    async def request_json(path, params):
        requests.append(path)
        return {
            "General": {"Code": "SYNTH", "Exchange": "US", "CurrencyCode": "USD"},
            "Highlights": {"ProfitMargin": "0.9%", "OperatingMargin": "-0.4%",
                           "GrossMargin": "0%"},
        }, None

    monkeypatch.setattr(client, "_request_json", request_json)
    values, error = asyncio.run(client.fetch_fundamentals(SYMBOL))
    assert error is None
    assert requests == ["fundamentals/SYNTH.US"]
    assert ep._pct_merge(0.9) == pytest.approx(0.009)
    assert values["profit_margin"] == pytest.approx(0.009)
    assert values["operating_margin"] == pytest.approx(-0.004)
    assert values["gross_margin"] == 0.0
    values.update(debt_to_equity=0.3, free_cash_flow_ttm=40.0)
    engine, calls = engine_with_wire(values)
    row = apply_fallback(engine, complete_base())
    assert calls == [SYMBOL]
    assert row["profit_margin"] == 0.9
    assert de._fund_lkg_capture(SYMBOL, row) is True
    restored = {"symbol": SYMBOL}
    assert de._fund_lkg_restore(SYMBOL, restored)
    de._margin_publish_contract(row)
    de._margin_publish_contract(restored)
    for field, expected in (("profit_margin", 0.009), ("operating_margin", -0.004),
                            ("gross_margin", 0.0)):
        assert row[field] == restored[field] == expected


def test_cache_first_preserves_only_landed_proof_and_never_reseeds(monkeypatch):
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "enforce")
    row = complete_base()
    row["gross_margin"] = 0.25  # Populated manual/source value, no unit proof.
    engine, calls = engine_with_wire({
        "gross_margin": 0.8, "operating_margin": 0.009, "profit_margin": -0.004,
        "debt_to_equity": 0.3, "free_cash_flow_ttm": 40.0,
    })
    cold = apply_fallback(engine, row)
    assert calls == [SYMBOL]
    assert cold["gross_margin"] == 0.25
    assert de._margin_unit_valid(cold, "gross_margin") is None
    assert de._margin_unit_valid(cold, "operating_margin") == "percent_points"
    assert de._fund_lkg_capture(SYMBOL, cold)
    captured = copy.deepcopy(de._FUND_LKG_STORE[SYMBOL])
    warm = apply_fallback(engine, row)
    assert calls == [SYMBOL]  # No new request on cache-first hit.
    assert "fund_cache:hit:" in warm["warnings"]
    assert de._margin_unit_valid(warm, "gross_margin") is None
    assert de._margin_unit_valid(warm, "operating_margin") == "percent_points"
    assert de._fund_lkg_capture(SYMBOL, warm) is False
    assert de._FUND_LKG_STORE[SYMBOL] == captured
    de._margin_publish_contract(cold)
    de._margin_publish_contract(warm)
    assert cold["operating_margin"] == warm["operating_margin"] == 0.009
    assert cold["profit_margin"] == warm["profit_margin"] == -0.004
    assert cold["gross_margin"] is warm["gross_margin"] is None


def test_yahoo_real_enrichment_stamps_fraction_only_for_filled_fields(monkeypatch):
    wire = types.ModuleType("synthetic_yahoo_wire")
    calls = []

    def fetch_fundamentals_patch(symbol):
        calls.append(symbol)
        return {"grossMargins": 0.8, "operatingMargins": 0.009,
                "profitMargins": -0.004}

    wire.fetch_fundamentals_patch = fetch_fundamentals_patch
    monkeypatch.setattr(de, "_import_yahoo_provider_module", lambda name: wire)
    monkeypatch.setenv("ENGINE_YAHOO_ENRICHMENT_ENABLED", "1")
    engine = de.DataEngineV5()
    row = complete_base()
    row["gross_margin"] = 0.25
    enriched = asyncio.run(engine._apply_yahoo_enrichment_pass(row, SYMBOL, "Global_Markets"))
    assert calls  # Real needs-check, picker and canonicalization executed.
    assert enriched["gross_margin"] == 0.25
    assert de._margin_unit_valid(enriched, "gross_margin") is None
    for field, expected in (("operating_margin", 0.009), ("profit_margin", -0.004)):
        assert enriched[field] == expected
        assert de._margin_unit_valid(enriched, field) == "fraction"
    de._margin_publish_contract(enriched)
    assert enriched["operating_margin"] == 0.009
    assert enriched["profit_margin"] == -0.004
    assert enriched["gross_margin"] is None


def test_real_l2_serialization_restore_keeps_unit_and_filters_extras(monkeypatch):
    wire = redis_wire(monkeypatch)
    engine, _ = engine_with_wire({
        "profit_margin": 0.009, "debt_to_equity": 0.3,
        "free_cash_flow_ttm": 40.0,
    })
    row = apply_fallback(engine, complete_base())
    assert de._fund_lkg_capture(SYMBOL, row)
    assert len(wire.writes) == 1
    raw = json.loads(wire.writes[0][2])
    assert raw["margin_units"]["profit_margin"] == {
        "unit": "percent_points", "value": 0.9,
    }
    assert "current_price" not in raw["fields"]
    raw["fields"]["secret_extra"] = "must disappear"
    raw["margin_units"]["secret_extra"] = {"unit": "fraction", "value": 123.0}
    wire.values[de._fund_lkg_redis_key(SYMBOL)] = json.dumps(raw)
    entry = de._fund_lkg_redis_get(SYMBOL)
    assert "secret_extra" not in entry["fields"]
    assert "secret_extra" not in entry["margin_units"]
    de._FUND_LKG_STORE.clear()
    restored = {"symbol": SYMBOL}
    assert de._fund_lkg_restore(SYMBOL, restored)
    assert de._margin_unit_valid(restored, "profit_margin") == "percent_points"
    de._margin_publish_contract(restored)
    assert restored["profit_margin"] == 0.009
    assert len(wire.writes) == 1  # L2 restore is not a fresh capture.


@pytest.mark.parametrize("units", [None, {}, {"profit_margin": {"unit": "unknown", "value": 0.9}},
                                  {"profit_margin": {"unit": "fraction", "value": 9.0}},
                                  "malformed", ["malformed"]])
@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
def test_legacy_tampered_or_unknown_l2_units_never_infer_scale(monkeypatch, units, mode):
    wire = redis_wire(monkeypatch)
    raw = {"ts": time.time(), "name": "Synthetic Company",
           "fields": {"market_cap": 1000.0, "profit_margin": 0.9}}
    if units is not None:
        raw["margin_units"] = units
    wire.values[de._fund_lkg_redis_key(SYMBOL)] = json.dumps(raw)
    entry = de._fund_lkg_redis_get(SYMBOL)
    assert entry is not None
    # Pre-fix cache entries cannot certify the old producer conversion. Bare,
    # unknown and malformed receipts all retain explicit uncertainty.
    assert entry["margin_units"]["profit_margin"] == {"unit": "unknown", "value": 0.9}
    row = {"symbol": SYMBOL}
    assert de._fund_lkg_restore(SYMBOL, row)
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", mode)
    de._margin_publish_contract(row)
    if mode == "enforce":
        assert row["profit_margin"] is None
        assert "margin_publish:profit_margin:unresolved" in row["warnings"]
    else:
        assert row["profit_margin"] == 0.9


@pytest.mark.parametrize("basis", ["malformed", ["malformed"], {"profit_margin": "malformed"},
                                 {"profit_margin": []},
                                 {"profit_margin": {"unit": "fraction", "value": True}},
                                 {"profit_margin": {"unit": "fraction", "value": float("nan")}}])
def test_malformed_basis_fails_closed_without_raising(basis):
    row = {"profit_margin": 0.9, UNIT_KEY: basis}
    de._margin_publish_contract(row)
    assert row["profit_margin"] is None
    assert "margin_publish:profit_margin:unresolved" in row["warnings"]


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), True])
def test_nonfinite_or_boolean_margin_has_no_valid_unit_or_numeric_publication(value):
    row = {"profit_margin": value,
           UNIT_KEY: {"profit_margin": {"unit": "fraction", "value": value}}}
    assert de._margin_unit_valid(row, "profit_margin") is None
    de._margin_publish_contract(row)
    assert row["profit_margin"] is None
    projected = de._strict_project_row(["profit_margin"], row)
    assert projected["profit_margin"] is None


def test_stale_publication_warning_cannot_bypass_changed_value():
    row = canonical_patch({"profit_margin": 0.009})
    de._margin_publish_contract(row)
    assert row["profit_margin"] == 0.009
    row["profit_margin"] = 0.9  # External mutation invalidates the exact-value proof.
    de._margin_publish_contract(row)
    assert row["profit_margin"] is None
    assert "margin_publish:profit_margin:unresolved" in row["warnings"]


def test_new_producer_proof_supersedes_old_marker_without_double_scaling():
    old = canonical_patch({"profit_margin": 0.009})
    de._margin_publish_contract(old)
    fresh = canonical_patch({"profit_margin": 0.004})
    fresh["warnings"] = old["warnings"]
    merged = de._overwrite_live_fields(old, fresh)
    assert de._margin_unit_valid(merged, "profit_margin") == "percent_points"
    de._margin_publish_contract(merged)
    assert merged["profit_margin"] == 0.004
    snapshot = copy.deepcopy(merged)
    assert de._margin_publish_contract(merged) == 0
    assert merged == snapshot


@pytest.mark.parametrize("merge", [de._merge_missing_fields, de._overwrite_live_fields])
def test_per_field_merge_proof_tracks_only_actual_fills_or_overwrites(merge):
    base = canonical_patch({"profit_margin": 0.009})
    base.update(gross_margin=0.25, position_qty=3.0, avg_cost=4.0,
                user_notes="preserve operator input")
    fresh = canonical_patch({"profit_margin": 0.004, "operating_margin": 0.008})
    fresh.update(position_qty=999.0, avg_cost=999.0, user_notes="provider note")
    merged = merge(base, fresh)
    assert merged["position_qty"] == 3.0
    assert merged["avg_cost"] == 4.0
    assert merged["user_notes"] == "preserve operator input"
    assert merged["gross_margin"] == 0.25
    assert de._margin_unit_valid(merged, "gross_margin") is None
    expected_profit = 0.9 if merge is de._merge_missing_fields else 0.4
    assert merged["profit_margin"] == expected_profit
    assert de._margin_unit_valid(merged, "profit_margin") == "percent_points"
    assert de._margin_unit_valid(merged, "operating_margin") == "percent_points"
    de._margin_publish_contract(merged)
    assert merged["profit_margin"] == pytest.approx(expected_profit / 100.0)
    assert merged["operating_margin"] == 0.008


def test_fill_only_merge_never_borrows_proof_for_populated_same_value():
    # Equal numeric values do not authorize unit transfer when nothing landed.
    base = {"symbol": SYMBOL, "gross_margin": 0.25}
    template = canonical_patch({"gross_margin": 0.25}, provider="yahoo_fundamentals")
    merged = de._merge_missing_fields(base, template)
    assert merged["gross_margin"] == 0.25
    assert de._margin_unit_valid(merged, "gross_margin") is None
    de._margin_publish_contract(merged)
    assert merged["gross_margin"] is None


@pytest.mark.parametrize("mode", ["off", "observe"])
def test_off_and_observe_keep_public_values_unchanged_across_producer_restore(monkeypatch, mode):
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", mode)
    monkeypatch.setenv("TFB_FUND_UNIT_SENTRY", "off")
    engine, _ = engine_with_wire({"profit_margin": 0.009, "debt_to_equity": 0.3,
                                 "free_cash_flow_ttm": 40.0})
    row = apply_fallback(engine, complete_base())
    assert de._fund_lkg_capture(SYMBOL, row)
    restored = {"symbol": SYMBOL}
    assert de._fund_lkg_restore(SYMBOL, restored)
    for target in (row, restored):
        before = {k: v for k, v in target.items() if k not in {UNIT_KEY, "warnings"}}
        de._margin_publish_contract(target)
        assert {k: v for k, v in target.items() if k not in {UNIT_KEY, "warnings"}} == before
        assert target["profit_margin"] == 0.009
        if mode == "off":
            assert UNIT_KEY not in target


@pytest.mark.parametrize("surface", ["page", "sheet"])
def test_actual_page_and_sheet_publication_consume_proof_and_preserve_schema(monkeypatch, surface):
    engine, _ = engine_with_wire({"profit_margin": 0.009, "debt_to_equity": 0.3,
                                 "free_cash_flow_ttm": 40.0})
    row = apply_fallback(engine, complete_base())
    assert de._fund_lkg_capture(SYMBOL, row)

    async def list_symbols(page):
        return [SYMBOL]

    async def quotes(symbols, page=""):
        return [copy.deepcopy(row)]

    async def external_rows(reader, page, limit, offset):
        return []

    async def no_news(rows):
        return None

    monkeypatch.setattr(engine, "list_symbols_for_page", list_symbols)
    monkeypatch.setattr(engine, "get_enriched_quotes", quotes)
    monkeypatch.setattr(engine, "_get_rows_from_external_reader", external_rows)
    monkeypatch.setattr(engine, "_apply_news_veto", no_news)
    if surface == "page":
        rows = asyncio.run(engine.get_page_rows("Global_Markets"))
    else:
        payload = asyncio.run(engine.get_sheet_rows("Global_Markets"))
        rows = payload["rows"]
        headers, keys = de.get_sheet_spec("Global_Markets")
        assert set(rows[0]) == set(keys)
        assert UNIT_KEY not in keys
        index = keys.index("profit_margin")
        assert payload["rows_matrix"][0][index] == 0.009
        assert payload["rows_display"][0][headers[index]] == 0.009
    assert len(rows) == 1
    assert rows[0]["profit_margin"] == 0.009
