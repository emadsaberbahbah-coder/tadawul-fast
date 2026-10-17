"""Real provider wrappers must receive the requested backtest history window."""
import asyncio
import builtins
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.providers import yahoo_chart_provider as yahoo
from scripts import track_performance as track


def providers(monkeypatch, first, second):
    original = builtins.__import__

    def load(name, *args, **kwargs):
        if name == "core.providers.yahoo_chart_provider":
            return first
        if name == "core.providers.eodhd_provider":
            return second
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", load)
    monkeypatch.setenv("TFB_BACKTEST_KSA_YF", "0")


def app():
    return object.__new__(track.PerformanceTrackerApp)


@pytest.mark.parametrize("days,expected", [(1, "1mo"), (28, "1mo"), (29, "3mo"),
    (90, "6mo"), (182, "1y"), (365, "1y"), (366, "2y"), (730, "2y"),
    (731, "5y"), (1826, "10y"), (3651, "max")])
def test_supported_period_never_shortens_requested_calendar_window(days, expected):
    assert track._backtest_history_period(days) == expected


def test_730_day_request_reaches_real_yahoo_http_seam_as_two_years(monkeypatch):
    calls = []

    async def fake_http(url, params, timeout):
        calls.append((url, dict(params)))
        n = 500 if params["range"] == "2y" else 22
        closes = [100 + i / 10 for i in range(n)]
        return {"chart": {"error": None, "result": [{
            "meta": {"symbol": "SYNTHETIC"},
            "timestamp": [1700000000 + i * 86400 for i in range(n)],
            "indicators": {"quote": [{"open": closes, "high": closes, "low": closes,
                                      "close": closes, "volume": [10000] * n}]},
        }]}}

    instance = object.__new__(yahoo.YahooChartProvider)
    instance.config = SimpleNamespace(timeout_sec=5)
    monkeypatch.setattr(yahoo, "get_provider", AsyncMock(return_value=instance))
    monkeypatch.setattr(yahoo, "_raw_http_get_json", fake_http)
    monkeypatch.setattr(yahoo, "_HAS_HTTPX", True)
    monkeypatch.setattr(yahoo, "_raw_chart_enabled", lambda: True)
    fallback = AsyncMock(side_effect=AssertionError("Yahoo success must retain provider priority"))
    providers(monkeypatch, yahoo, SimpleNamespace(fetch_history=fallback, __name__="eodhd_fixture"))
    history = asyncio.run(app()._fetch_backtest_history(["SYNTHETIC.US"], 730))
    assert calls[0][1] == {"range": "2y", "interval": "1d"}
    assert len(calls) == 1 and len(history["SYNTHETIC.US"]) == 500
    fallback.assert_not_awaited()
    # The unchanged 221-bar trigger can be evaluated on the synthetic trend.
    # This proves usable depth, not that any real-world trigger must fire.
    hypothesis = next(h for h in track.default_hypotheses() if h.hypothesis_id == "STABILITY_TREND_200")
    result = track.run_backtest(hypothesis, history)
    assert result.sample_size > 0


def test_days_aware_provider_receives_exact_window_without_routing_change(monkeypatch):
    calls = []

    async def history(symbol, days):
        calls.append((symbol, days))
        return [{"date": "2026-10-02", "close": 2}, {"date": "2026-10-01", "close": 1}]

    fallback = AsyncMock(side_effect=AssertionError("unnecessary provider fallback"))
    providers(monkeypatch, SimpleNamespace(fetch_history=history, __name__="days_fixture"),
              SimpleNamespace(fetch_history=fallback, __name__="fallback_fixture"))
    result = asyncio.run(app()._fetch_backtest_history(["SYNTHETIC.US"], 730))
    assert calls == [("SYNTHETIC.US", 730)]
    assert [row["close"] for row in result["SYNTHETIC.US"]] == [1, 2]
    fallback.assert_not_awaited()


def test_internal_typeerror_never_retries_without_window_or_leaks_payload(monkeypatch, caplog):
    calls = []

    async def broken(symbol, days=30):
        calls.append((symbol, days))
        raise TypeError("private-provider-payload api_token=synthetic-secret")

    async def fallback(symbol, days):
        calls.append(("fallback", days))
        return [{"date": "2026-10-01", "close": 10}]

    providers(monkeypatch, SimpleNamespace(fetch_history=broken, __name__="broken_fixture"),
              SimpleNamespace(fetch_history=fallback, __name__="fallback_fixture"))
    result = asyncio.run(app()._fetch_backtest_history(["SYNTHETIC.US"], 730))
    assert calls == [("SYNTHETIC.US", 730), ("fallback", 730)]
    assert len(result["SYNTHETIC.US"]) == 1
    assert "TypeError" in caplog.text and "api_token=" not in caplog.text


def test_shallow_returned_history_stays_insufficient_without_fabrication(monkeypatch):
    async def history(symbol, period="1mo", interval="1d"):
        assert period == "2y" and interval == "1d"
        return [{"date": f"2026-10-{i + 1:02}", "close": 10 + i} for i in range(22)]

    providers(monkeypatch, SimpleNamespace(fetch_history=history, __name__="shallow_fixture"),
              SimpleNamespace(__name__="fallback_fixture"))
    result = asyncio.run(app()._fetch_backtest_history(["SYNTHETIC.US"], 730))
    hypothesis = next(h for h in track.default_hypotheses() if h.hypothesis_id == "STABILITY_TREND_200")
    analyzed = track.run_backtest(hypothesis, result)
    assert len(result["SYNTHETIC.US"]) == 22
    assert analyzed.status == track.HypothesisStatus.INSUFFICIENT_DATA and analyzed.sample_size == 0
