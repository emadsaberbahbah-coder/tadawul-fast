"""yahoo_chart_provider v8.16.0: history 404 miss cache (TFB_YC_HISTORY_MISS_TTL_SEC).

Zero network. The real two-host raw ladder (_raw_chart_fetch_triple) runs;
only the HTTP seam (_raw_http_get_json) and the yfinance module are faked.

Pins:
  M1  off (unset) -> a 404 on every host still falls through to yfinance (v8.15.1).
  M2  on -> 404 on every host returns [] with NO yfinance call.
  M3  on -> the second call for the same Yahoo id makes NO HTTP at all.
  M4  on -> X and X.US share one Yahoo id, so one miss covers both spellings.
  M5  on -> a 429 on either host is never cached and still falls back.
  M6  on -> the miss expires after the TTL.
  M7  TTL parsing: unset/0/negative/garbage = off; bounded to [60, 86400].
"""
from __future__ import annotations

import asyncio

import pytest

from core.providers import yahoo_chart_provider as yc


REAL_RAW_FETCH = yc._raw_chart_fetch_triple


class _FakeTicker:
    calls = 0

    def __init__(self, sym):
        self.sym = sym

    def history(self, **kwargs):
        type(self).calls += 1
        raise RuntimeError("Too Many Requests. Rate limited. Try after a while.")


class _FakeYF:
    Ticker = _FakeTicker


@pytest.fixture
def provider(monkeypatch):
    yc._YC_HISTORY_MISS.clear()
    _FakeTicker.calls = 0
    monkeypatch.setattr(yc, "_HAS_HTTPX", True)
    monkeypatch.setattr(yc, "_HAS_YFINANCE", True)
    monkeypatch.setattr(yc, "yf", _FakeYF)
    monkeypatch.setattr(yc, "_raw_chart_enabled", lambda: True)
    monkeypatch.setattr(yc, "_raw_chart_fetch_triple", REAL_RAW_FETCH)
    monkeypatch.setenv("TFB_YF_HISTORY_RETRY", "0")
    monkeypatch.delenv("TFB_YC_SYMBOL_SKIP", raising=False)
    monkeypatch.delenv("TFB_YC_HISTORY_MISS_TTL_SEC", raising=False)
    inst = yc.YahooChartProvider(yc.YahooConfig(
        enabled=True, rate_limit_per_sec=0, threadpool_workers=2,
    ))
    yield inst
    inst._executor.shutdown(wait=True)
    yc._YC_HISTORY_MISS.clear()


def _http(monkeypatch, statuses):
    """statuses: list consumed per HTTP call (one per host)."""
    seen = []

    async def fake(url, params, timeout):
        seen.append(url)
        code = statuses[min(len(seen) - 1, len(statuses) - 1)]
        raise yc.YahooFetchError(f"http_{code}")

    monkeypatch.setattr(yc, "_raw_http_get_json", fake)
    return seen


def _hist(provider, sym):
    return asyncio.run(provider.fetch_history_raw(sym))


def test_m1_off_still_falls_back(provider, monkeypatch):
    seen = _http(monkeypatch, [404])
    assert _hist(provider, "HEWW") == []
    assert len(seen) == len(yc._RAW_CHART_HOSTS)
    assert _FakeTicker.calls == 1
    assert yc._YC_HISTORY_MISS == {}


def test_m2_on_skips_yfinance(provider, monkeypatch):
    monkeypatch.setenv("TFB_YC_HISTORY_MISS_TTL_SEC", "3600")
    seen = _http(monkeypatch, [404])
    assert _hist(provider, "HEWW") == []
    assert len(seen) == len(yc._RAW_CHART_HOSTS)
    assert _FakeTicker.calls == 0
    assert "HEWW" in yc._YC_HISTORY_MISS


def test_m3_m4_cached_no_http_and_spellings_share(provider, monkeypatch):
    monkeypatch.setenv("TFB_YC_HISTORY_MISS_TTL_SEC", "3600")
    seen = _http(monkeypatch, [404])
    _hist(provider, "KLD.US")
    first = len(seen)
    assert _hist(provider, "KLD.US") == []
    assert _hist(provider, "KLD") == []
    assert len(seen) == first
    assert _FakeTicker.calls == 0


@pytest.mark.parametrize("statuses", [[429, 404], [404, 429], [503, 503]])
def test_m5_outage_never_cached(provider, monkeypatch, statuses):
    monkeypatch.setenv("TFB_YC_HISTORY_MISS_TTL_SEC", "3600")
    _http(monkeypatch, statuses)
    assert _hist(provider, "EEX") == []
    assert _FakeTicker.calls == 1
    assert yc._YC_HISTORY_MISS == {}


def test_m6_expiry(provider, monkeypatch):
    monkeypatch.setenv("TFB_YC_HISTORY_MISS_TTL_SEC", "60")
    seen = _http(monkeypatch, [404])
    _hist(provider, "FM")
    n = len(seen)
    yc._YC_HISTORY_MISS["FM"] = 0.0  # force expiry
    _hist(provider, "FM")
    assert len(seen) == 2 * n


@pytest.mark.parametrize("raw,want", [
    (None, 0.0), ("", 0.0), ("0", 0.0), ("-5", 0.0), ("abc", 0.0),
    ("10", 60.0), ("3600", 3600.0), ("999999", 86400.0),
])
def test_m7_ttl_parse(monkeypatch, raw, want):
    if raw is None:
        monkeypatch.delenv("TFB_YC_HISTORY_MISS_TTL_SEC", raising=False)
    else:
        monkeypatch.setenv("TFB_YC_HISTORY_MISS_TTL_SEC", raw)
    assert yc._yc_history_miss_ttl_sec() == want
