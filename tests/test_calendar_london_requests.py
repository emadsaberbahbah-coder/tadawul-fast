"""Canonical London calendar requests through the actual mocked HTTP boundary.

EODHD's documented London exchange code is LSE. The request correction does
not certify a live 404 cause, and adds no EODHD primary requests or retries.
"""
import asyncio
from datetime import date

import pytest

httpx = pytest.importorskip("httpx")

from core.providers import calendar_provider as provider


TODAY = date(2026, 10, 16)
EARNINGS = "2026-10-28"
EXDIV = "2026-10-23"


@pytest.fixture
def service(monkeypatch):
    """Run real request/parsing code; all HTTP is an in-process transport."""
    requests = []
    monkeypatch.setenv("TFB_CALENDAR_ENABLED", "1")
    monkeypatch.setenv("EODHD_API_KEY", "synthetic-offline-token")
    monkeypatch.setenv("EODHD_BASE_URL", "https://calendar.invalid/api")
    monkeypatch.setenv("TFB_CALENDAR_EARNINGS_CHUNK", "50")
    monkeypatch.setenv("TFB_CAL_YAHOO_FALLBACK", "0")
    monkeypatch.setattr(provider, "_today", lambda: TODAY)

    def respond(request):
        requests.append(request)
        assert request.url.host == "calendar.invalid"
        assert request.url.params["api_token"] == "synthetic-offline-token"
        assert request.url.params["fmt"] == "json"
        assert request.url.params["from"] == TODAY.isoformat()
        if request.url.path == "/api/calendar/earnings":
            codes = request.url.params["symbols"].split(",")
            if any(code.endswith(".L") for code in codes):
                return httpx.Response(404, json={"error": "synthetic noncanonical exchange"})
            return httpx.Response(200, json={"earnings": [
                {"code": code, "report_date": EARNINGS} for code in codes]})
        assert request.url.path.startswith("/api/div/")
        if request.url.path.endswith(".L"):
            return httpx.Response(404, json={"error": "synthetic noncanonical exchange"})
        return httpx.Response(200, json=[{"date": EXDIV}])

    transport = httpx.MockTransport(respond)
    monkeypatch.setattr(provider, "_client", lambda: httpx.AsyncClient(transport=transport))
    return requests


@pytest.mark.parametrize("original", ["SYNTH.L", "SYNTH.LSE", " synth.l "])
def test_real_earnings_request_uses_lse_and_returns_original_key(service, original):
    result = asyncio.run(provider.fetch_earnings_map([original]))
    assert result == {original.strip().upper(): EARNINGS}
    assert len(service) == 1
    request = service[0]
    assert request.url.path == "/api/calendar/earnings"
    assert request.url.params["symbols"] == "SYNTH.LSE"
    assert request.url.params["to"] == "2026-11-30"


@pytest.mark.parametrize("original", ["SYNTH.L", "SYNTH.LSE"])
def test_real_dividend_request_uses_lse_and_returns_original_key(service, original):
    assert asyncio.run(provider.fetch_next_exdiv_map([original])) == {original: EXDIV}
    assert len(service) == 1
    assert service[0].url.path == "/api/div/SYNTH.LSE"
    assert service[0].url.params["to"] == "2027-01-14"


def test_mixed_earnings_batch_retains_unrelated_codes_and_saudi_exclusion(service):
    originals = ["SYNTH.L", "READY.LSE", "ACME.US", "FUND-UN.TO", "0405.HK", "2222.SR"]
    result = asyncio.run(provider.fetch_earnings_map(originals))
    assert result == {symbol: EARNINGS for symbol in originals if symbol != "2222.SR"}
    assert len(service) == 1  # One existing batch; no per-symbol retry/load.
    assert service[0].url.params["symbols"].split(",") == [
        "0405.HK", "ACME.US", "FUND-UN.TO", "READY.LSE", "SYNTH.LSE"]


@pytest.mark.parametrize("original,canonical", [
    ("ACME.US", "ACME.US"), ("FUND-UN.TO", "FUND-UN.TO"),
    ("0405.HK", "0405.HK"), ("ACME.BR", "ACME.BR"),
    # Preserve the shared helper's current treatment of this unsupported suffix.
    ("ACME.LS", "ACME.LS.US"),
    ("ACME.LN", "ACME.LN"),
    ("ACME.NS", "ACME.NSE"), ("ACME.DE", "ACME.XETRA"),
])
def test_other_exchange_request_contracts_are_unchanged(service, original, canonical):
    assert asyncio.run(provider.fetch_earnings_map([original])) == {original: EARNINGS}
    assert len(service) == 1
    assert service[0].url.params["symbols"] == canonical


def test_merged_context_keeps_original_keys_and_both_saudi_unknowns(service):
    result = asyncio.run(provider.fetch_event_context(["SYNTH.L", "2222.SR"]))
    assert result == {
        "SYNTH.L": {"next_earnings_date": EARNINGS, "next_ex_div_date": EXDIV},
        "2222.SR": {"next_earnings_date": None, "next_ex_div_date": None},
    }
    assert len(service) == 2  # Same earnings batch plus one dividend request.


def test_existing_first_original_alias_dedup_semantics_remain():
    codes, saudi = provider._split_symbols(["SYNTH.L", "SYNTH.LSE", "2222.SR"])
    assert codes == {"SYNTH.LSE": "SYNTH.L"}
    assert saudi == ["2222.SR"]


def test_merged_alias_context_keeps_uniform_shape_with_other_alias_unknown(service):
    # Existing one-code->one-original routing does not certify both aliases.
    # Yahoo fallback is disabled by the fixture, so the unknown is observable.
    result = asyncio.run(provider.fetch_event_context(["SYNTH.L", "SYNTH.LSE"]))
    assert result == {
        "SYNTH.L": {"next_earnings_date": EARNINGS, "next_ex_div_date": EXDIV},
        "SYNTH.LSE": {"next_earnings_date": None, "next_ex_div_date": None},
    }
    assert len(service) == 2  # One primary earnings and one primary dividend call.


@pytest.mark.parametrize("originals", [["SYNTH.L", "SYNTH.LSE"], ["SYNTH.LSE", "SYNTH.L"]])
def test_colliding_london_aliases_keep_first_original_only_in_one_http_request(service, originals):
    # The existing code->one-original interface uses first-wins deduplication.
    # This narrow correction does not introduce result fan-out to every alias.
    result = asyncio.run(provider.fetch_earnings_map(originals))
    assert result == {originals[0]: EARNINGS}
    assert originals[1] not in result
    assert len(service) == 1
    assert service[0].url.params["symbols"] == "SYNTH.LSE"


def test_http_failure_stays_one_attempt_and_unknown(monkeypatch, service, caplog):
    def unavailable(request):
        service.append(request)
        return httpx.Response(404, text="synthetic upstream failure")

    transport = httpx.MockTransport(unavailable)
    monkeypatch.setattr(provider, "_client", lambda: httpx.AsyncClient(transport=transport))
    assert asyncio.run(provider.fetch_earnings_map(["SYNTH.L"])) == {}
    assert len(service) == 1
    assert service[0].url.params["symbols"] == "SYNTH.LSE"
    assert "earnings batch failed" in caplog.text
    assert "synthetic-offline-token" not in caplog.text
