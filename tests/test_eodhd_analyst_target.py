#!/usr/bin/env python3
"""tests/test_eodhd_analyst_target.py - eodhd_provider v4.19.0 + data_engine_v2
v5.159.0 (TFB_EODHD_ANALYST_TARGET, default off).

WHY (2026-10-28): since 2026-10-21 09:28Z every sync logs the Yahoo
fundamentals breaker OPEN on HTTP 429 ("EODHD fallback carries
fundamentals"). The fallback carried no analyst target, so Phase-II restored
the last known target (analyst_lkg:<age>h) and the opportunity builder held
the row ("Held target - no new money"); on 28 Oct 23 of 31 board candidates
were held that way. Armed, the EODHD fundamentals patch carries
target_mean_price (AnalystRatings.TargetPrice, else
Highlights.WallStreetTargetPrice) and analyst_count; the engine seam
canonicalizes the target behind the plausibility gate with the row's price
injected, tags analyst_consensus_eodhd, and lets the fund-LKG snapshot carry
the target so a fund-cache hit (enforce, TTL 24 h: EODHD is asked once per
symbol per day) fills it instead of forgoing it.

Pinned here (REAL provider and engine modules; the supplier HTTP boundary and
the registry module are the only replacements):
  P1 off: the patch has no target keys and the cache key is v4.18.5
  P2 on: target from AnalystRatings.TargetPrice, count = the five buckets,
     the cache key carries "at1"
  P3 on: WallStreetTargetPrice fallback; invalid or absent values add nothing
  P4 cache isolation: an unarmed cached patch is never served armed, and the
     other way round
  E1 engine off path: a patch without the key takes the v5.158.0 path
  E2 miss, plausible: forecast_price_12m / forecast_source=provider_target /
     target_mean_price land, tag analyst_consensus_eodhd, Phase-II honours it
     and never writes analyst_lkg
  E3 miss, implausible: the gate drops it against the row price (the patch
     has none), tag provider_target_implausible_dropped_12m, no source tag
  E4 capture: armed + tag + provider_target => the snapshot carries the
     block; off, or a Yahoo-sourced target, => it does not
  E5 hit (enforce): the fill carries the target, the tag has no ":nt", the
     provider is not called, Phase-II never writes analyst_lkg
  E6 hit, implausible on today's price: the block is dropped, the ratios
     still fill, the drop tag is written
  E7 off after an armed capture: the snapshot's target block is not served
  E8 L2 round trip: the block travels only while armed
  E9 version, manifest pins, FLAGS row, env-contract row, ci lane
  E10 v5.159.1 (Codex review on #865): with TFB_TARGET_MEAN_PUBLISH=0 the
     snapshot carries forecast_price_12m without target_mean_price; the hit
     still judges it on today's price (refused at 20, accepted at 100)

Run: python tests/test_eodhd_analyst_target.py   or   pytest -q <this file>
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
import types
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from core import data_engine_v2 as de  # noqa: E402
from core.providers import eodhd_provider as ep  # noqa: E402

FLAG = "TFB_EODHD_ANALYST_TARGET"
TAG = "analyst_consensus_eodhd"
DROP = "provider_target_implausible_dropped_12m"

PAYLOAD = {
    "General": {"Code": "SYNTH", "Exchange": "US", "CurrencyCode": "USD", "Name": "Synth Corp",
                "Sector": "Technology", "Industry": "Software", "UpdatedAt": "2026-10-27"},
    "Highlights": {"MarketCapitalization": 5.0e9, "EarningsShare": 4.0, "PERatio": 25.0,
                   "WallStreetTargetPrice": 118.0, "MostRecentQuarter": "2026-09-30"},
    "AnalystRatings": {"Rating": 4.1, "TargetPrice": 125.0, "StrongBuy": 7, "Buy": 9,
                       "Hold": 3, "Sell": 1, "StrongSell": 0},
}

# A row the way the storm leaves it: priced, named, no fundamentals.
ROW = {
    "symbol": "SYNTH.US", "name": "Synth Corp", "current_price": 100.0, "price": 100.0,
    "sector": "Technology", "industry": "Software", "currency": "USD", "country": "USA",
}

# What the EODHD fallback delivers besides the target (engine units).
RATIOS = {
    "market_cap": 5.0e9, "eps_ttm": 4.0, "pe_ttm": 25.0, "revenue_ttm": 1.2e9,
    "debt_to_equity": 0.3, "free_cash_flow_ttm": 2.0e8, "gross_margin": 0.6,
    "operating_margin": 0.2, "profit_margin": 0.15, "pb_ratio": 3.0, "ps_ratio": 4.0,
    "float_shares": 4.0e7, "beta_5y": 1.1, "dividend_yield": 0.01, "payout_ratio": 0.2,
}


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    for k in (FLAG, "TFB_EODHD_FUND_CACHE", "TFB_EODHD_FUND_CACHE_TTL_H",
              "TFB_EODHD_FUND_NEG_TTL_H", "TFB_EODHD_FUND_FALLBACK_SKIP_PAGES",
              "TFB_FUND_UNIT_SENTRY", "TFB_ENGINE_TARGET_KLG", "TFB_PROVIDER_TARGET_GATE_ENABLED",
              "TFB_TARGET_MEAN_PUBLISH"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("TFB_ENGINE_FUND_LKG", "1")
    monkeypatch.setenv("TFB_ENGINE_FUND_LKG_REDIS", "0")
    monkeypatch.setenv("TFB_FUND_UNIT_SENTRY", "off")
    monkeypatch.setenv("TFB_ENGINE_TARGET_KLG", "1")
    monkeypatch.setenv("TFB_ENGINE_TARGET_KLG_REDIS", "0")
    de._FUND_LKG_STORE.clear()
    de._FUND_NEG_STORE.clear()
    de._TGT_LKG_STORE.clear()
    for k in list(de._FUND_CACHE_STATS):
        de._FUND_CACHE_STATS[k] = 0
    yield
    de._FUND_LKG_STORE.clear()
    de._FUND_NEG_STORE.clear()
    de._TGT_LKG_STORE.clear()


def _arm(monkeypatch, value="1"):
    monkeypatch.setenv(FLAG, value)


# ----------------------------------------------------------------------------- provider
def _client(monkeypatch, payload=PAYLOAD):
    monkeypatch.setattr(ep.httpx, "AsyncClient", lambda **kwargs: object())
    client = ep.EODHDClient()
    calls = []

    async def request(path, params):
        calls.append(path)
        return json.loads(json.dumps(payload)), None

    monkeypatch.setattr(client, "_request_json", request)
    return client, calls


def _fetch(client):
    patch, err = asyncio.run(client.fetch_fundamentals("SYNTH.US"))
    assert err is None
    return patch


def _cache_keys(client):
    return sorted(client.fund_cache._d)


@pytest.mark.parametrize("value", [None, "0", "off", "false"])
def test_p1_off_patch_and_cache_key_are_v4185(monkeypatch, value):
    if value is not None:
        _arm(monkeypatch, value)
    client, calls = _client(monkeypatch)
    patch = _fetch(client)
    assert "target_mean_price" not in patch and "analyst_count" not in patch
    assert patch["eps_ttm"] == 4.0 and patch["name"] == "Synth Corp"
    keys = _cache_keys(client)
    assert keys and all(":at1:" not in k for k in keys)
    assert any(k.endswith(":SYNTH.US") and k.startswith("f:eps_basis_v1:") for k in keys)
    assert calls == ["fundamentals/SYNTH.US"]


def test_p2_on_target_count_and_cache_segment(monkeypatch):
    _arm(monkeypatch)
    client, _ = _client(monkeypatch)
    patch = _fetch(client)
    assert patch["target_mean_price"] == 125.0           # AnalystRatings first
    assert patch["analyst_count"] == 20                   # 7 + 9 + 3 + 1 + 0
    assert patch["eps_ttm"] == 4.0                        # nothing else moved
    keys = _cache_keys(client)
    assert keys and all(":at1:SYNTH.US" in k for k in keys)


@pytest.mark.parametrize("ratings, highlights, want_target, want_count", [
    ({}, {"WallStreetTargetPrice": 118.0}, 118.0, None),                 # fallback field
    ({"TargetPrice": 0}, {"WallStreetTargetPrice": 118.0}, 118.0, None),  # zero is absent
    ({"TargetPrice": "bad"}, {"WallStreetTargetPrice": "nan"}, None, None),
    ({"TargetPrice": -5}, {}, None, None),
    ({"StrongBuy": 2, "Buy": 0}, {}, None, 2),                           # count alone
    ({"StrongBuy": 0, "Hold": 0}, {}, None, None),                       # zero sum is no count
    ({"TargetPrice": "131.5", "Buy": "4"}, {}, 131.5, 4),                # strings parse
])
def test_p3_field_selection(monkeypatch, ratings, highlights, want_target, want_count):
    _arm(monkeypatch)
    payload = json.loads(json.dumps(PAYLOAD))
    payload["AnalystRatings"] = ratings
    payload["Highlights"] = {k: v for k, v in payload["Highlights"].items()
                             if k != "WallStreetTargetPrice"}
    payload["Highlights"].update(highlights)
    client, _ = _client(monkeypatch, payload)
    patch = _fetch(client)
    assert patch.get("target_mean_price") == want_target
    assert patch.get("analyst_count") == want_count


def test_p3_helper_direct():
    f = ep._analyst_target_fields
    assert f(None, None) == {}
    assert f({"WallStreetTargetPrice": 50}, {"TargetPrice": 60}) == {"target_mean_price": 60.0}
    assert f({}, {"StrongBuy": 1, "Buy": 1, "Hold": 1, "Sell": 1, "StrongSell": 1}) == {"analyst_count": 5}
    assert f({}, {"Buy": -3}) == {}


def test_p4_cache_isolation_between_armed_and_unarmed(monkeypatch):
    client, calls = _client(monkeypatch)
    off_patch = _fetch(client)                      # cached under the v4.18.5 key
    assert "target_mean_price" not in off_patch
    _arm(monkeypatch)
    on_patch = _fetch(client)                       # a new request, not the cached patch
    assert on_patch["target_mean_price"] == 125.0
    assert calls == ["fundamentals/SYNTH.US", "fundamentals/SYNTH.US"]
    monkeypatch.delenv(FLAG, raising=False)
    again = _fetch(client)                          # off again: served from the unarmed entry
    assert "target_mean_price" not in again
    assert len(calls) == 2


# ----------------------------------------------------------------------------- engine
class _Calls:
    def __init__(self):
        self.n = 0


def _registry_module(patch, calls):
    mod = types.ModuleType("eodhd_fixture_provider")

    def fetch_fundamentals_patch(symbol):
        calls.n += 1
        return dict(patch)

    mod.fetch_fundamentals_patch = fetch_fundamentals_patch
    return mod


def _engine(patch, calls):
    eng = de.DataEngineV5()
    eng._provider_registry._modules["eodhd"] = _registry_module(patch, calls)
    return eng


def _run(eng, row, page="Global_Markets"):
    out = asyncio.run(eng._apply_eodhd_fundamentals_fallback(dict(row), row["symbol"], page))
    de._fund_lkg_capture(row["symbol"], out)        # the pipeline seam right after the fallback
    return out


def _tags(row):
    return [p.strip() for p in (row.get("warnings") or "").split(";") if p.strip()]


def _eodhd_patch(target=125.0, count=20):
    p = {"symbol": "SYNTH.US", "symbol_normalized": "SYNTH.US", "provider": "eodhd",
         "data_source": "eodhd", "name": "Synth Corp", "exchange": "US", "currency": "USD",
         "country": "USA", "sector": "Technology", "industry": "Software"}
    p.update(RATIOS)
    if target is not None:
        p["target_mean_price"] = target
    if count is not None:
        p["analyst_count"] = count
    return p


def test_e1_patch_without_the_key_takes_the_v5158_path(monkeypatch):
    for armed in (False, True):
        if armed:
            _arm(monkeypatch)
        calls = _Calls()
        out = _run(_engine(_eodhd_patch(target=None, count=None), calls), ROW)
        assert calls.n == 1
        assert out["debt_to_equity"] == 0.3 and out["free_cash_flow_ttm"] == 2.0e8
        assert "forecast_price_12m" not in out and "forecast_source" not in out
        assert "target_mean_price" not in out
        assert TAG not in _tags(out) and DROP not in _tags(out)
        assert "eodhd_fundamentals_fallback_applied" in _tags(out)


def test_e2_miss_plausible_target_lands_and_phase_ii_honours_it(monkeypatch):
    _arm(monkeypatch)
    calls = _Calls()
    out = _run(_engine(_eodhd_patch(), calls), ROW)
    assert calls.n == 1
    assert out["forecast_price_12m"] == 125.0
    assert out["forecast_source"] == "provider_target"
    assert out["target_mean_price"] == 125.0
    assert TAG in _tags(out) and DROP not in _tags(out)
    assert out["current_price"] == 100.0                 # the injected price never travels
    de._phase_ii_quality_forecast(out)
    assert out["forecast_price_12m"] == 125.0
    assert out.get("forecast_price_3m") and out.get("forecast_price_1m")
    assert not any(t.startswith("analyst_lkg:") for t in _tags(out))
    assert de._TGT_LKG_STORE.get("SYNTH.US", {}).get("fp12") == 125.0   # captured, not restored


def test_e3_miss_implausible_target_is_dropped_against_the_row_price(monkeypatch):
    _arm(monkeypatch)
    calls = _Calls()
    out = _run(_engine(_eodhd_patch(target=417.0), calls), dict(ROW, current_price=146.0, price=146.0))
    assert calls.n == 1
    assert "forecast_price_12m" not in out
    assert _safe(out.get("forecast_source")) != "provider_target"
    assert DROP in _tags(out) and TAG not in _tags(out)
    assert out["debt_to_equity"] == 0.3                  # the ratios still fill
    snap = de._FUND_LKG_STORE.get("SYNTH.US")
    assert snap and "target_mean_price" not in snap["fields"]


def _safe(v):
    return str(v or "").strip().lower()


def test_e4_capture_carries_the_block_only_when_armed_and_eodhd_sourced(monkeypatch):
    # off: a row that somehow carries the tag and a target is not captured with it
    row = dict(ROW, **RATIOS)
    row.update({"forecast_price_12m": 125.0, "forecast_source": "provider_target",
                "target_mean_price": 125.0, "warnings": TAG})
    assert de._fund_lkg_capture("SYNTH.US", row)
    assert "target_mean_price" not in de._FUND_LKG_STORE["SYNTH.US"]["fields"]
    de._FUND_LKG_STORE.clear()
    # armed, but the target is Yahoo's (no provenance tag): not captured
    _arm(monkeypatch)
    yahoo_row = dict(row, warnings="yahoo_enrichment_applied")
    assert de._fund_lkg_capture("SYNTH.US", yahoo_row)
    assert "target_mean_price" not in de._FUND_LKG_STORE["SYNTH.US"]["fields"]
    de._FUND_LKG_STORE.clear()
    # armed, tag present, but the source was overwritten: not captured
    lkg_row = dict(row, forecast_source="analyst_lkg")
    assert de._fund_lkg_capture("SYNTH.US", lkg_row)
    assert "forecast_price_12m" not in de._FUND_LKG_STORE["SYNTH.US"]["fields"]
    de._FUND_LKG_STORE.clear()
    # armed, tag, provider_target: captured
    assert de._fund_lkg_capture("SYNTH.US", row)
    f = de._FUND_LKG_STORE["SYNTH.US"]["fields"]
    assert f["target_mean_price"] == 125.0 and f["forecast_price_12m"] == 125.0
    assert f["forecast_source"] == "provider_target"
    assert f["debt_to_equity"] == 0.3                    # the 24-field block is unchanged


def _seed_armed_snapshot(monkeypatch):
    _arm(monkeypatch)
    calls = _Calls()
    first = _run(_engine(_eodhd_patch(), calls), ROW)   # miss: provider paid, snapshot seeded
    assert calls.n == 1 and TAG in _tags(first)
    assert de._FUND_LKG_STORE["SYNTH.US"]["fields"]["target_mean_price"] == 125.0
    return calls


def test_e5_hit_fills_the_target_without_paying_the_provider(monkeypatch):
    calls = _seed_armed_snapshot(monkeypatch)
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "enforce")
    out = _run(_engine(_eodhd_patch(), calls), ROW)
    assert calls.n == 1                                   # the hit short-circuited
    hit = [t for t in _tags(out) if t.startswith("fund_cache:hit:")]
    assert hit and not hit[0].endswith(":nt")
    assert out["forecast_price_12m"] == 125.0 and out["forecast_source"] == "provider_target"
    assert out["target_mean_price"] == 125.0 and out["debt_to_equity"] == 0.3
    assert TAG in _tags(out) and DROP not in _tags(out)
    de._TGT_LKG_STORE.clear()
    de._phase_ii_quality_forecast(out)
    assert not any(t.startswith("analyst_lkg:") for t in _tags(out))
    assert out.get("forecast_price_3m")


def test_e5_hit_without_the_block_keeps_the_nt_suffix(monkeypatch):
    _arm(monkeypatch)
    calls = _Calls()
    _run(_engine(_eodhd_patch(target=None, count=None), calls), ROW)   # snapshot without target
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "enforce")
    out = _run(_engine(_eodhd_patch(), calls), ROW)
    assert calls.n == 1
    hit = [t for t in _tags(out) if t.startswith("fund_cache:hit:")]
    assert hit and hit[0].endswith(":nt")
    assert "forecast_price_12m" not in out and TAG not in _tags(out)


def test_e6_hit_target_implausible_on_todays_price_is_dropped(monkeypatch):
    calls = _seed_armed_snapshot(monkeypatch)
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "enforce")
    crashed = dict(ROW, current_price=20.0, price=20.0)   # 125 is 6.25x the price
    out = _run(_engine(_eodhd_patch(), calls), crashed)
    assert calls.n == 1
    assert "forecast_price_12m" not in out and "target_mean_price" not in out
    assert _safe(out.get("forecast_source")) != "provider_target"
    assert out["debt_to_equity"] == 0.3                   # the ratios still fill
    assert DROP in _tags(out) and TAG not in _tags(out)
    assert any(t.startswith("fund_cache:hit:") for t in _tags(out))


def test_e7_off_after_an_armed_capture_does_not_serve_the_block(monkeypatch):
    calls = _seed_armed_snapshot(monkeypatch)
    monkeypatch.delenv(FLAG, raising=False)
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "enforce")
    out = _run(_engine(_eodhd_patch(target=None, count=None), calls), ROW)
    assert calls.n == 1
    hit = [t for t in _tags(out) if t.startswith("fund_cache:hit:")]
    assert hit and hit[0].endswith(":nt")                 # v5.158.0 tag, v5.158.0 fill
    assert "forecast_price_12m" not in out and "target_mean_price" not in out
    assert TAG not in _tags(out) and DROP not in _tags(out)
    found = de._fund_cache_lookup("SYNTH.US")
    assert found and "target_mean_price" not in found[0]


def test_e8_l2_round_trip_carries_the_block_only_while_armed(monkeypatch):
    store = {}

    class Redis:
        def setex(self, key, ttl, payload):
            store[key] = payload

        def get(self, key):
            return store.get(key)

    monkeypatch.setattr(de, "_fund_lkg_redis_client", lambda: Redis())
    entry = {"ts": time.time(), "name": "Synth Corp",
             "fields": dict(RATIOS, target_mean_price=125.0, forecast_price_12m=125.0,
                            forecast_source="provider_target")}
    # off: the write drops the block
    assert de._fund_lkg_redis_set("SYNTH.US", entry)
    assert "target_mean_price" not in json.loads(store[de._fund_lkg_redis_key("SYNTH.US")])["fields"]
    # armed: the write carries it and the read returns it
    _arm(monkeypatch)
    assert de._fund_lkg_redis_set("SYNTH.US", entry)
    written = json.loads(store[de._fund_lkg_redis_key("SYNTH.US")])["fields"]
    assert written["target_mean_price"] == 125.0 and written["forecast_source"] == "provider_target"
    got = de._fund_lkg_redis_get("SYNTH.US")
    assert got and got["fields"]["forecast_price_12m"] == 125.0 and got["fields"]["eps_ttm"] == 4.0
    # off again: the same L2 entry reads back without the block
    monkeypatch.delenv(FLAG, raising=False)
    got = de._fund_lkg_redis_get("SYNTH.US")
    assert got and "target_mean_price" not in got["fields"] and got["fields"]["eps_ttm"] == 4.0


def test_e9_versions_pins_flag_registration_and_ci_lane():
    assert ep.PROVIDER_VERSION == "4.19.0"
    assert de.__version__ == "5.159.1"
    assert de._EODHD_ANALYST_TAG == TAG
    for bad in ("cap", "forecast", "target", "roi", "drop", "reject"):
        assert bad not in TAG
    vd = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert "(\"core.providers.eodhd_provider\", \"PROVIDER_VERSION\", '4.19.0'," in vd
    assert "(\"core.data_engine_v2\", \"__version__\", \"5.159.1\"," in vd
    assert "('TFB_EODHD_ANALYST_TARGET', '0'," in vd
    contract = (_REPO / "docs" / "render_env_contract.md").read_text(encoding="utf-8")
    assert "TFB_EODHD_ANALYST_TARGET" in contract
    ci = (_REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "tests/test_eodhd_analyst_target.py" in ci


def test_e10_publish_off_snapshot_is_still_gated_on_the_12m_forecast(monkeypatch):
    # Codex review on #865: TFB_TARGET_MEAN_PUBLISH=0 (supported kill-switch)
    # never publishes target_mean_price, so the armed capture stores the 12M
    # forecast and its source only; the hit must still judge that value.
    monkeypatch.setenv("TFB_TARGET_MEAN_PUBLISH", "0")
    _arm(monkeypatch)
    calls = _Calls()
    first = _run(_engine(_eodhd_patch(), calls), ROW)          # miss at 100: paid once
    assert calls.n == 1 and TAG in _tags(first)
    assert first["forecast_price_12m"] == 125.0 and first["forecast_source"] == "provider_target"
    assert "target_mean_price" not in first                      # the switch is off
    snap = de._FUND_LKG_STORE["SYNTH.US"]["fields"]
    assert snap["forecast_price_12m"] == 125.0 and "target_mean_price" not in snap
    monkeypatch.setenv("TFB_EODHD_FUND_CACHE", "enforce")
    # hit at today's 20: 125 is 6.25x the price -> refused, ratios still fill
    crashed = _run(_engine(_eodhd_patch(), calls), dict(ROW, current_price=20.0, price=20.0))
    assert calls.n == 1
    assert "forecast_price_12m" not in crashed
    assert _safe(crashed.get("forecast_source")) != "provider_target"
    assert DROP in _tags(crashed) and TAG not in _tags(crashed)
    assert crashed["debt_to_equity"] == 0.3
    # hit at today's 100: accepted, provenance tag, the fill is not "no target"
    calm = _run(_engine(_eodhd_patch(), calls), ROW)
    assert calls.n == 1
    assert calm["forecast_price_12m"] == 125.0 and calm["forecast_source"] == "provider_target"
    assert TAG in _tags(calm) and DROP not in _tags(calm)
    hit = [t for t in _tags(calm) if t.startswith("fund_cache:hit:")]
    assert hit and not hit[0].endswith(":nt")
    # the helper: either key counts, neither does not
    assert de._fill_carries_eodhd_target({"forecast_price_12m": 125.0})
    assert de._fill_carries_eodhd_target({"target_mean_price": 125.0})
    assert not de._fill_carries_eodhd_target({"debt_to_equity": 0.3})
    assert not de._fill_carries_eodhd_target(None)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
