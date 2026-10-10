"""Real calendar merge/Yahoo worker regressions with inline thread dispatch."""
from __future__ import annotations

import asyncio
from datetime import date, datetime
from types import SimpleNamespace

import pytest

from core.providers import calendar_provider as provider


TODAY = date(2026, 10, 10)
PRIMARY_EARNINGS = "2026-10-20"
PRIMARY_EXDIV = "2026-10-15"
YAHOO_EARNINGS = datetime(2026, 10, 25)
YAHOO_EXDIV = date(2026, 10, 18)


@pytest.fixture(autouse=True)
def offline_clock(monkeypatch):
    monkeypatch.setattr(provider, "_today", lambda: TODAY)
    monkeypatch.setenv("TFB_CALENDAR_ENABLED", "1")
    monkeypatch.setenv("TFB_CAL_YAHOO_FALLBACK", "1")
    for name in ("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_KEY"):
        monkeypatch.delenv(name, raising=False)

    def no_network():
        pytest.fail("Calendar tests must not open a live HTTP client")

    monkeypatch.setattr(provider, "_client", no_network)

    async def inline_thread_dispatch(function, *args, **kwargs):
        return function(*args, **kwargs)

    # Keep the worker logic real while avoiding platform-dependent executor
    # startup/shutdown: these stubs return immediately and perform no I/O.
    monkeypatch.setattr(provider.asyncio, "to_thread", inline_thread_dispatch)


@pytest.fixture
def yahoo(monkeypatch):
    """Stub remote payloads while retaining actual Yahoo worker logic."""
    def install(earnings=YAHOO_EARNINGS, exdiv=YAHOO_EXDIV,
                earnings_error=None, calendar_error=None):
        calls = []

        class Ticker:
            def __init__(self, symbol):
                self.symbol = symbol

            def get_earnings_dates(self, limit):
                calls.append((self.symbol, "earnings", limit))
                if earnings_error is not None:
                    raise earnings_error
                return SimpleNamespace(index=[] if earnings is None else [earnings])

            @property
            def calendar(self):
                calls.append((self.symbol, "calendar"))
                if calendar_error is not None:
                    raise calendar_error
                return {"Ex-Dividend Date": exdiv}

        monkeypatch.setattr(provider, "_yf", SimpleNamespace(Ticker=Ticker))
        return calls

    return install


@pytest.mark.parametrize("known_earnings,known_exdiv", [
    (False, False), (False, True), (True, False), (True, True),
])
def test_context_fills_each_missing_event_without_overwriting_primary(
        monkeypatch, yahoo, known_earnings, known_exdiv):
    calls = yahoo()
    primary_calls = []

    async def earnings_map(symbols):
        primary_calls.append(("earnings", symbols))
        return {"ACME.US": PRIMARY_EARNINGS} if known_earnings else {}

    async def exdiv_map(symbols):
        primary_calls.append(("exdiv", symbols))
        return {"ACME.US": PRIMARY_EXDIV} if known_exdiv else {}

    monkeypatch.setattr(provider, "fetch_earnings_map", earnings_map)
    monkeypatch.setattr(provider, "fetch_next_exdiv_map", exdiv_map)

    context = asyncio.run(provider.fetch_event_context([" acme.us "]))

    assert context == {"ACME.US": {
        "next_earnings_date": PRIMARY_EARNINGS if known_earnings else "2026-10-25",
        "next_ex_div_date": PRIMARY_EXDIV if known_exdiv else "2026-10-18",
    }}
    assert sorted(primary_calls) == [
        ("earnings", ["ACME.US"]), ("exdiv", ["ACME.US"])]
    assert calls == ([] if known_earnings and known_exdiv else [
        ("ACME", "earnings", 12), ("ACME", "calendar")])


def test_fill_counts_only_missing_fields_across_mixed_symbols(yahoo):
    calls = yahoo()
    base = {
        "BOTH.US": {"next_earnings_date": None, "next_ex_div_date": None},
        "EARN.US": {"next_earnings_date": None, "next_ex_div_date": PRIMARY_EXDIV},
        "DIV.US": {"next_earnings_date": PRIMARY_EARNINGS, "next_ex_div_date": None},
        "FULL.US": {"next_earnings_date": PRIMARY_EARNINGS, "next_ex_div_date": PRIMARY_EXDIV},
    }

    assert asyncio.run(provider._yahoo_calendar_fill(base, TODAY)) == (2, 2)
    assert base["BOTH.US"] == {
        "next_earnings_date": "2026-10-25", "next_ex_div_date": "2026-10-18"}
    assert base["EARN.US"] == {
        "next_earnings_date": "2026-10-25", "next_ex_div_date": PRIMARY_EXDIV}
    assert base["DIV.US"] == {
        "next_earnings_date": PRIMARY_EARNINGS, "next_ex_div_date": "2026-10-18"}
    assert base["FULL.US"] == {
        "next_earnings_date": PRIMARY_EARNINGS, "next_ex_div_date": PRIMARY_EXDIV}
    assert {c[0] for c in calls} == {"BOTH", "EARN", "DIV"}
    assert len(calls) == 6


@pytest.mark.parametrize("failure", ["empty", "errors", "past"])
def test_missing_exdiv_remains_unknown_when_yahoo_cannot_supply_it(yahoo, failure):
    if failure == "errors":
        yahoo(earnings_error=RuntimeError("offline earnings failure"),
              calendar_error=RuntimeError("offline calendar failure"))
    elif failure == "past":
        yahoo(earnings=datetime(2026, 10, 9), exdiv=date(2026, 10, 9))
    else:
        yahoo(earnings=None, exdiv=None)
    base = {"ACME.US": {
        "next_earnings_date": PRIMARY_EARNINGS, "next_ex_div_date": None}}

    assert asyncio.run(provider._yahoo_calendar_fill(base, TODAY)) == (0, 0)
    assert base == {"ACME.US": {
        "next_earnings_date": PRIMARY_EARNINGS, "next_ex_div_date": None}}


def test_earnings_worker_error_still_allows_missing_exdiv_to_fill(yahoo):
    yahoo(earnings_error=RuntimeError("offline earnings failure"))
    base = {"ACME.US": {
        "next_earnings_date": PRIMARY_EARNINGS, "next_ex_div_date": None}}

    assert asyncio.run(provider._yahoo_calendar_fill(base, TODAY)) == (0, 1)
    assert base["ACME.US"] == {
        "next_earnings_date": PRIMARY_EARNINGS, "next_ex_div_date": "2026-10-18"}


def test_calendar_worker_error_still_allows_missing_earnings_to_fill(yahoo):
    yahoo(calendar_error=RuntimeError("offline calendar failure"))
    base = {"ACME.US": {
        "next_earnings_date": None, "next_ex_div_date": PRIMARY_EXDIV}}

    assert asyncio.run(provider._yahoo_calendar_fill(base, TODAY)) == (1, 0)
    assert base["ACME.US"] == {
        "next_earnings_date": "2026-10-25", "next_ex_div_date": PRIMARY_EXDIV}


@pytest.mark.parametrize("kill_switch", ["0", "false", "off", "no"])
def test_disabled_yahoo_fallback_does_not_schedule_workers(monkeypatch, yahoo, kill_switch):
    calls = yahoo()
    monkeypatch.setenv("TFB_CAL_YAHOO_FALLBACK", kill_switch)
    base = {"ACME.US": {
        "next_earnings_date": PRIMARY_EARNINGS, "next_ex_div_date": None}}

    assert asyncio.run(provider._yahoo_calendar_fill(base, TODAY)) == (0, 0)
    assert base["ACME.US"] == {
        "next_earnings_date": PRIMARY_EARNINGS, "next_ex_div_date": None}
    assert calls == []


def test_unavailable_yfinance_keeps_known_primary_date(monkeypatch):
    monkeypatch.setattr(provider, "_yf", None)
    base = {"ACME.US": {
        "next_earnings_date": PRIMARY_EARNINGS, "next_ex_div_date": None}}

    assert asyncio.run(provider._yahoo_calendar_fill(base, TODAY)) == (0, 0)
    assert base["ACME.US"] == {
        "next_earnings_date": PRIMARY_EARNINGS, "next_ex_div_date": None}


def test_disabled_calendar_layer_does_not_fetch_any_provider(monkeypatch, yahoo):
    calls = yahoo()
    monkeypatch.setenv("TFB_CALENDAR_ENABLED", "0")

    async def no_primary(*args):
        pytest.fail("Disabled calendar layer must not call primary providers")

    monkeypatch.setattr(provider, "fetch_earnings_map", no_primary)
    monkeypatch.setattr(provider, "fetch_next_exdiv_map", no_primary)

    assert asyncio.run(provider.fetch_event_context(["ACME.US"])) == {
        "ACME.US": {"next_earnings_date": None, "next_ex_div_date": None}}
    assert calls == []
