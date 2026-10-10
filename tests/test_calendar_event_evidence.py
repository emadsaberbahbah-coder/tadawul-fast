"""Real provider evidence paths with fake payloads and inline thread dispatch."""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from core.providers import calendar_provider as provider


TODAY = date(2026, 10, 10)
OBSERVED_AT = "2026-10-10T09:00:00Z"
PRIMARY_EARNINGS = "2026-10-20"
PRIMARY_EXDIV = "2026-10-15"
YAHOO_EARNINGS = datetime(2026, 10, 25)
YAHOO_EXDIV = date(2026, 10, 18)


def evidence(earnings=None, exdiv=None, earnings_source="unknown", exdiv_source="unknown"):
    """Expected public shape; an absent event never receives an observation."""
    return {
        "next_earnings_date": earnings,
        "next_ex_div_date": exdiv,
        "earnings_source": earnings_source,
        "earnings_observed_at": OBSERVED_AT if earnings is not None else "",
        "earnings_status": ("estimated" if earnings_source == "yahoo"
                            else "reported" if earnings is not None else "unknown"),
        "exdiv_source": exdiv_source,
        "exdiv_observed_at": OBSERVED_AT if exdiv is not None else "",
        "exdiv_status": "reported" if exdiv is not None else "unknown",
    }


@pytest.fixture(autouse=True)
def offline_clock(monkeypatch):
    real_observation_clock = provider._observed_at_utc
    monkeypatch.setattr(provider, "_today", lambda: TODAY)
    monkeypatch.setattr(provider, "_observed_at_utc", lambda: OBSERVED_AT)
    monkeypatch.setattr(provider, "_yf", None)
    monkeypatch.setenv("TFB_CALENDAR_ENABLED", "1")
    monkeypatch.setenv("TFB_CAL_YAHOO_FALLBACK", "1")
    for name in ("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_KEY"):
        monkeypatch.delenv(name, raising=False)

    def no_client():
        pytest.fail("Unexpected live calendar HTTP client")

    async def inline_thread_dispatch(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(provider, "_client", no_client)
    monkeypatch.setattr(provider.asyncio, "to_thread", inline_thread_dispatch)
    return real_observation_clock


@pytest.fixture
def primary(monkeypatch):
    """Substitute transport; keep the actual EODHD map parsers and gates."""
    def install(earnings=None, exdiv=None, fails=False):
        traffic = {"clients": 0, "requests": []}
        monkeypatch.setenv("EODHD_API_KEY", "synthetic-offline-key")

        class Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

        def client():
            traffic["clients"] += 1
            return Client()

        async def payload(client, path, params):
            traffic["requests"].append(path)
            if fails:
                raise RuntimeError("synthetic offline provider outage")
            if path == "/calendar/earnings":
                requested = set(params["symbols"].split(","))
                return {"earnings": [
                    {"code": symbol, "report_date": event}
                    for symbol, event in (earnings or {}).items() if symbol in requested]}
            if path.startswith("/div/"):
                event = (exdiv or {}).get(path.removeprefix("/div/"))
                return [{"date": event}] if event is not None else []
            pytest.fail(f"Unexpected EODHD endpoint: {path}")

        monkeypatch.setattr(provider, "_client", client)
        monkeypatch.setattr(provider, "_get_json", payload)
        return traffic

    return install


@pytest.fixture
def yahoo(monkeypatch):
    """Substitute Yahoo payloads; retain the actual tier and date logic."""
    def install(earnings=YAHOO_EARNINGS, exdiv=YAHOO_EXDIV, fails=False):
        calls = []

        class Ticker:
            def __init__(self, symbol):
                self.symbol = symbol

            def get_earnings_dates(self, limit):
                calls.append((self.symbol, "earnings", limit))
                if fails:
                    raise RuntimeError("synthetic offline Yahoo outage")
                return SimpleNamespace(index=[] if earnings is None else [earnings])

            @property
            def calendar(self):
                calls.append((self.symbol, "calendar"))
                if fails:
                    raise RuntimeError("synthetic offline Yahoo outage")
                return {"Ex-Dividend Date": exdiv}

        monkeypatch.setattr(provider, "_yf", SimpleNamespace(Ticker=Ticker))
        return calls

    return install


def test_observation_clock_reports_current_timezone_aware_utc(offline_clock):
    before = datetime.now(timezone.utc)
    observed = datetime.fromisoformat(offline_clock().replace("Z", "+00:00"))
    after = datetime.now(timezone.utc)

    assert observed.tzinfo is not None and observed.utcoffset() == timedelta(0)
    assert before - timedelta(seconds=1) <= observed <= after


def test_key_missing_yahoo_only_has_field_specific_status_and_utc_observations(yahoo):
    calls = yahoo()

    result = asyncio.run(provider.fetch_event_evidence([" acme.us ", "2222.SR"]))

    expected = evidence("2026-10-25", "2026-10-18", "yahoo", "yahoo")
    assert result == {"ACME.US": expected, "2222.SR": expected}
    assert {call[0] for call in calls} == {"ACME", "2222.SR"}
    assert len(calls) == 4
    for field in ("earnings_observed_at", "exdiv_observed_at"):
        observed = datetime.fromisoformat(result["ACME.US"][field].replace("Z", "+00:00"))
        assert observed.tzinfo is not None and observed.utcoffset() == timedelta(0)


@pytest.mark.parametrize("primary_field", ["earnings", "exdiv"])
def test_mixed_primary_and_yahoo_keep_each_event_source_and_existing_date(primary, yahoo, primary_field):
    traffic = primary(
        earnings={"ACME.US": PRIMARY_EARNINGS} if primary_field == "earnings" else {},
        exdiv={"ACME.US": PRIMARY_EXDIV} if primary_field == "exdiv" else {})
    calls = yahoo()

    result = asyncio.run(provider.fetch_event_evidence(["ACME.US"]))

    if primary_field == "earnings":
        expected = evidence(PRIMARY_EARNINGS, "2026-10-18", "eodhd", "yahoo")
    else:
        expected = evidence("2026-10-25", PRIMARY_EXDIV, "yahoo", "eodhd")
    assert result == {"ACME.US": expected}
    assert sorted(traffic["requests"]) == ["/calendar/earnings", "/div/ACME.US"]
    assert calls == [("ACME", "earnings", 12), ("ACME", "calendar")]


def test_complete_primary_events_are_reported_and_do_not_call_yahoo(primary, yahoo):
    primary(earnings={"ACME.US": PRIMARY_EARNINGS}, exdiv={"ACME.US": PRIMARY_EXDIV})
    calls = yahoo()

    assert asyncio.run(provider.fetch_event_evidence(["ACME.US"])) == {
        "ACME.US": evidence(PRIMARY_EARNINGS, PRIMARY_EXDIV, "eodhd", "eodhd")}
    assert calls == []


@pytest.mark.parametrize("failure", ["empty", "past", "errors"])
def test_absent_yahoo_events_never_create_provenance_or_observation(yahoo, failure):
    if failure == "past":
        yahoo(earnings=datetime(2026, 10, 9), exdiv=date(2026, 10, 9))
    elif failure == "errors":
        yahoo(fails=True)
    else:
        yahoo(earnings=None, exdiv=None)

    assert asyncio.run(provider.fetch_event_evidence(["ACME.US"])) == {"ACME.US": evidence()}


@pytest.mark.parametrize("missing_field", ["earnings", "exdiv"])
def test_yahoo_partial_result_does_not_attach_evidence_to_missing_sibling(yahoo, missing_field):
    yahoo(earnings=None if missing_field == "earnings" else YAHOO_EARNINGS,
          exdiv=None if missing_field == "exdiv" else YAHOO_EXDIV)

    expected = (evidence(None, "2026-10-18", "unknown", "yahoo")
                if missing_field == "earnings"
                else evidence("2026-10-25", None, "yahoo", "unknown"))
    assert asyncio.run(provider.fetch_event_evidence(["ACME.US"])) == {"ACME.US": expected}


def test_primary_outage_recovered_by_yahoo_never_labels_dates_eodhd(primary, yahoo):
    traffic = primary(fails=True)
    yahoo()

    assert asyncio.run(provider.fetch_event_evidence(["ACME.US"])) == {
        "ACME.US": evidence("2026-10-25", "2026-10-18", "yahoo", "yahoo")}
    assert sorted(traffic["requests"]) == ["/calendar/earnings", "/div/ACME.US"]


@pytest.mark.parametrize("fallback_state", ["disabled", "unavailable"])
def test_unfilled_primary_sibling_is_unknown_with_fallback_disabled_or_absent(
        monkeypatch, primary, yahoo, fallback_state):
    primary(earnings={"ACME.US": PRIMARY_EARNINGS})
    calls = yahoo()
    if fallback_state == "disabled":
        monkeypatch.setenv("TFB_CAL_YAHOO_FALLBACK", "0")
    else:
        monkeypatch.setattr(provider, "_yf", None)

    assert asyncio.run(provider.fetch_event_evidence(["ACME.US"])) == {
        "ACME.US": evidence(PRIMARY_EARNINGS, None, "eodhd", "unknown")}
    assert calls == []


def test_disabled_layer_returns_unknown_uniform_rows_without_provider_calls(monkeypatch, primary, yahoo):
    traffic = primary(earnings={"ACME.US": PRIMARY_EARNINGS})
    calls = yahoo()
    monkeypatch.setenv("TFB_CALENDAR_ENABLED", "0")

    assert asyncio.run(provider.fetch_event_evidence([" acme.us ", "2222.SR"])) == {
        "ACME.US": evidence(), "2222.SR": evidence()}
    assert traffic == {"clients": 0, "requests": []} and calls == []


def test_empty_input_returns_no_evidence_and_does_not_contact_providers(primary, yahoo):
    traffic = primary()
    calls = yahoo()

    assert asyncio.run(provider.fetch_event_evidence([None, "", " "])) == {}
    assert traffic == {"clients": 0, "requests": []} and calls == []


def test_sync_evidence_api_preserves_the_rich_shape(yahoo):
    yahoo()

    assert provider.fetch_event_evidence_sync(["ACME.US"]) == {
        "ACME.US": evidence("2026-10-25", "2026-10-18", "yahoo", "yahoo")}


@pytest.mark.parametrize("mode", ["async", "sync"])
@pytest.mark.parametrize("scenario", ["primary", "mixed", "yahoo"])
def test_legacy_event_context_exact_two_key_shape_survives_rich_evidence(
        primary, yahoo, mode, scenario):
    if scenario == "primary":
        primary(earnings={"ACME.US": PRIMARY_EARNINGS}, exdiv={"ACME.US": PRIMARY_EXDIV})
        expected_earnings, expected_exdiv = PRIMARY_EARNINGS, PRIMARY_EXDIV
    elif scenario == "mixed":
        primary(earnings={"ACME.US": PRIMARY_EARNINGS})
        expected_earnings, expected_exdiv = PRIMARY_EARNINGS, "2026-10-18"
    else:
        expected_earnings, expected_exdiv = "2026-10-25", "2026-10-18"
    yahoo()

    result = (asyncio.run(provider.fetch_event_context(["ACME.US"])) if mode == "async"
              else provider.fetch_event_context_sync(["ACME.US"]))

    assert result == {"ACME.US": {
        "next_earnings_date": expected_earnings, "next_ex_div_date": expected_exdiv}}


def test_legacy_primary_map_apis_still_return_only_date_strings(primary, yahoo):
    primary(earnings={"ACME.US": PRIMARY_EARNINGS}, exdiv={"ACME.US": PRIMARY_EXDIV})
    calls = yahoo()

    assert asyncio.run(provider.fetch_earnings_map(["ACME.US", "2222.SR"])) == {
        "ACME.US": PRIMARY_EARNINGS}
    assert asyncio.run(provider.fetch_next_exdiv_map(["ACME.US", "2222.SR"])) == {
        "ACME.US": PRIMARY_EXDIV}
    assert calls == []
