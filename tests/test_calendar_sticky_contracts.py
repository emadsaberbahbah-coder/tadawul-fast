"""Offline regressions of actual calendar sync carry and publication boundaries."""
from __future__ import annotations

from datetime import date
import importlib.util
from pathlib import Path
from types import ModuleType
import sys

import pytest


SPEC = importlib.util.spec_from_file_location(
    "calendar_sticky_contracts_target", Path(__file__).parents[1] / "scripts/run_calendar_sync.py")
sync = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sync)
TODAY = date(2026, 10, 8)
ASOF = "2026-10-06 08:15"
PRIOR_SOURCE = "synthetic prior provider"


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    monkeypatch.setattr(sync, "_today_riyadh", lambda: TODAY)
    monkeypatch.delenv("TFB_CALENDAR_TICKER_GUARD", raising=False)


def prior_row(symbol="ACME.US", earnings="2026-11-04", exdiv="", asof=ASOF, source=PRIOR_SOURCE):
    return [symbol, earnings, "", exdiv, "", asof, source]


def table(*rows):
    return [sync.HEADERS] + list(rows)


@pytest.mark.parametrize("bad", ["date unknown", "2026-02-31", "2026-10-20junk", "", None, 46200])
def test_bad_earnings_cell_does_not_erase_its_valid_exdiv_or_other_rows(bad):
    prior = sync.parse_prior(table(
        prior_row("BAD.US", bad, "2026-10-20"),
        prior_row("ACME.US", "2026-11-04"),
    ))
    assert "e" not in prior["BAD.US"]
    assert prior["BAD.US"]["x"] == "2026-10-20"
    assert prior["ACME.US"]["e"] == "2026-11-04"


def test_bad_exdiv_cell_does_not_erase_valid_earnings():
    prior = sync.parse_prior(table(prior_row(exdiv="invalid")))
    assert prior["ACME.US"]["e"] == "2026-11-04"
    assert "x" not in prior["ACME.US"]


def test_future_fields_current_day_and_past_expiry_are_independent():
    prior = sync.parse_prior(table(
        prior_row("TODAY.US", "2026-10-08", "2026-10-07"),
        prior_row("OLD.US", "2026-10-07", "2026-10-06"),
        prior_row("DIV.US", "2026-10-07", "2026-10-20"),
    ))
    assert prior["TODAY.US"]["e"] == "2026-10-08"
    assert "x" not in prior["TODAY.US"]
    assert "OLD.US" not in prior
    assert prior["DIV.US"]["x"] == "2026-10-20"
    assert "e" not in prior["DIV.US"]


@pytest.mark.parametrize("earnings,exdiv", [("2026-11-04", ""), ("", "2026-10-20"),
                                           ("2026-11-04", "2026-10-20")])
def test_vanished_symbol_with_either_event_is_carried_with_original_facts(earnings, exdiv):
    prior = sync.parse_prior(table(prior_row(earnings=earnings, exdiv=exdiv)))
    symbols, ctx, carried, filled, resurrected = sync.apply_sticky([], {}, prior)
    assert symbols == ["ACME.US"]
    assert filled == 0 and resurrected == 1 and carried == {"ACME.US"}
    row = sync.build_rows(symbols, ctx, "current provider failed", carried)[0]
    assert row[1] == earnings and row[3] == exdiv
    assert row[5] == ASOF
    assert PRIOR_SOURCE in row[6] and row[6].endswith("+carried")
    assert "current provider failed" not in row[6]


@pytest.mark.parametrize("stamp", ["", "unknown", "2026-02-31 08:15"])
def test_unknown_carried_asof_is_not_replaced_by_now(stamp):
    prior = sync.parse_prior(table(prior_row(asof=stamp)))
    symbols, ctx, carried, _, _ = sync.apply_sticky(["ACME.US"], {}, prior)
    assert sync.build_rows(symbols, ctx, "current provider failed", carried)[0][5] == ""


def test_partial_carry_keeps_dates_and_identifies_each_source():
    prior = sync.parse_prior(table(prior_row(exdiv="2026-10-20")))
    symbols, ctx, carried, filled, resurrected = sync.apply_sticky(
        ["ACME.US"], {"ACME.US": {"next_earnings_date": "2026-10-29"}}, prior)
    row = sync.build_rows(symbols, ctx, "current provider", carried)[0]
    assert row[1] == "2026-10-29" and row[3] == "2026-10-20"
    assert row[5] == ASOF  # row conservatively retains the oldest carried evidence
    assert "fresh earnings" in row[6] and "current provider" in row[6]
    assert "carried ex-div" in row[6] and PRIOR_SOURCE in row[6]
    assert filled == 1 and resurrected == 0


def test_complete_fresh_provider_facts_win_without_carrying_prior_metadata():
    prior = sync.parse_prior(table(prior_row(exdiv="2026-10-20")))
    fresh = {"ACME.US": {"next_earnings_date": "2026-10-29", "next_ex_div_date": "2026-10-21"}}
    symbols, ctx, carried, filled, resurrected = sync.apply_sticky(["ACME.US"], fresh, prior)
    row = sync.build_rows(symbols, ctx, "current provider", carried)[0]
    assert row[1] == "2026-10-29" and row[3] == "2026-10-21"
    assert carried == set() and filled == resurrected == 0
    assert row[5] and row[6] == "current provider"


def test_invalid_provider_field_cannot_displace_known_future_event():
    prior = sync.parse_prior(table(prior_row()))
    symbols, ctx, carried, _, _ = sync.apply_sticky(
        ["ACME.US"], {"ACME.US": {"next_earnings_date": "invalid",
                                  "next_ex_div_date": "2026-10-07"}}, prior)
    row = sync.build_rows(symbols, ctx, "current provider", carried)[0]
    assert row[1] == "2026-11-04" and row[3] == ""
    assert row[5] == ASOF


def test_whole_carried_row_roundtrips_without_relabeling_source_or_time():
    previous = table(prior_row())
    for _ in range(3):
        prior = sync.parse_prior(previous)
        symbols, ctx, carried, _, _ = sync.apply_sticky([], {}, prior)
        row = sync.build_rows(symbols, ctx, "latest provider failed", carried)[0]
        assert row[5] == ASOF and row[6] == PRIOR_SOURCE + " [events unknown:ex-div] +carried"
        previous = table(row)


def test_empty_failed_observation_does_not_claim_a_fresh_fact():
    row = sync.build_rows(["ACME.US"], {}, "provider error")[0]
    assert row[1] == row[3] == row[5] == ""
    assert row[6] == "provider error [events unknown:earnings/ex-div]"


def test_prior_telemetry_resets_even_for_empty_or_no_header_input():
    sync.parse_prior(table(prior_row("FORECAST")))
    assert sync.parse_prior.junk_purged == 1
    assert sync.parse_prior([]) == {} and sync.parse_prior.junk_purged == 0
    sync.parse_prior.junk_purged = 9
    assert sync.parse_prior([["not a header"]]) == {} and sync.parse_prior.junk_purged == 0


def test_actual_write_boundary_survives_bad_prior_cell_without_extra_fetch(monkeypatch):
    class Sheet:
        def __init__(self, values):
            self.values = values
            self.updates = []
        def get(self, _range):
            return self.values
        def update(self, **kwargs):
            self.updates.append(kwargs)
        def batch_clear(self, _ranges):
            pass
        def freeze(self, **_kwargs):
            pass

    page = Sheet([["Symbol"], ["ACME.US"]])
    calendar = Sheet(table(prior_row("BAD.US", "invalid", "2026-10-20"), prior_row()))
    class Book:
        def worksheet(self, name):
            return calendar if name == "Calendar_Events" else page

    fetch_calls = []
    provider = ModuleType("core.providers.calendar_provider")
    provider.__version__ = "synthetic"
    provider.is_enabled = lambda: True
    def fetch(symbols):
        fetch_calls.append(symbols)
        return {s: {"next_earnings_date": None, "next_ex_div_date": None} for s in symbols}
    provider.fetch_event_context_sync = fetch
    monkeypatch.setitem(sys.modules, "core.providers.calendar_provider", provider)
    monkeypatch.setattr(sync, "_open_book", lambda: Book())
    monkeypatch.setenv("TFB_CALENDAR_PAGES", "My_Portfolio")
    assert sync.main(["--write"]) == 0
    assert fetch_calls == [["ACME.US"]]
    published = calendar.updates[-1]
    assert published["value_input_option"] == "RAW"
    rows = {row[0]: row for row in published["values"]}
    assert rows["ACME.US"][1] == "2026-11-04"
    assert rows["BAD.US"][3] == "2026-10-20"
    assert all(row[5] == ASOF for row in rows.values())
