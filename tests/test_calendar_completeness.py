"""Calendar routing/publication regressions on actual offline sync methods."""
from __future__ import annotations

from datetime import date
import importlib.util
from pathlib import Path
import sys
from types import ModuleType

import pytest


SPEC = importlib.util.spec_from_file_location(
    "calendar_completeness_target", Path(__file__).parents[1] / "scripts/run_calendar_sync.py")
sync = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sync)
TODAY = date(2026, 10, 9)
ASOF = "2026-10-07 14:46"


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    monkeypatch.setattr(sync, "_today_riyadh", lambda: TODAY)
    monkeypatch.delenv("TFB_CALENDAR_TICKER_GUARD", raising=False)


@pytest.mark.parametrize("symbol", ["GRT-UN.TO", "BRK-B.US", "BRK.B.US", "0405.HK", "MPHASIS.NS"])
def test_canonical_venue_symbols_survive_both_harvest_and_prior_routing(symbol):
    assert sync.harvest_symbols([["Symbol"], [symbol], [symbol.lower()]]) == [symbol]
    prior = sync.parse_prior([sync.HEADERS, [symbol, "2026-10-22", 99, "", "", ASOF, "old provider"]])
    assert list(prior) == [symbol]
    symbols, ctx, carried, _, _ = sync.apply_sticky([], {}, prior)
    row = sync.build_rows(symbols, ctx, "new provider", carried)[0]
    assert row[0] == symbol and row[1:3] == ["2026-10-22", 13]
    assert row[5] == ASOF and "old provider" in row[6]


@pytest.mark.parametrize("symbol", ["FORECAST", "COUNT", "402", "VERSABANK", "BRK-B", "GC=F",
                                    "GRT--UN.TO", "GRT-.TO", "GRT..TO", "-GRT.TO", "TOOLONGNAME.US"])
def test_shape_guard_still_blocks_cockpit_junk_and_malformed_symbols(symbol):
    assert sync.harvest_symbols([["Symbol"], [symbol]]) == []
    assert sync.parse_prior([sync.HEADERS, [symbol, "2026-10-22"]]) == {}


@pytest.mark.parametrize("earnings,exdiv,note", [
    (None, None, "earnings/ex-div"),
    ("2026-10-22", None, "ex-div"),
    (None, "2026-10-15", "earnings"),
    ("2026-02-31", "2026-10-08", "earnings/ex-div"),
])
def test_absent_or_invalid_event_is_explicit_unknown_without_fabricated_observation(earnings, exdiv, note):
    ctx = {"GRT-UN.TO": {"next_earnings_date": earnings, "next_ex_div_date": exdiv}}
    row = sync.build_rows(["GRT-UN.TO"], ctx, "actual provider context")[0]
    assert f"[events unknown:{note}]" in row[6]
    if note == "earnings/ex-div":
        assert row[1:6] == ["", "", "", "", ""]


def test_repeated_carried_unknown_status_is_idempotent_and_does_not_refresh_source_time():
    values = [sync.HEADERS, ["GRT-UN.TO", "2026-10-22", 14, "", "", ASOF, "original provider"]]
    for _ in range(3):
        prior = sync.parse_prior(values)
        symbols, ctx, carried, _, _ = sync.apply_sticky([], {}, prior)
        row = sync.build_rows(symbols, ctx, "provider failed", carried)[0]
        assert row[5] == ASOF
        assert row[6] == "original provider [events unknown:ex-div] +carried"
        assert row[2] == 13
        values = [sync.HEADERS, row]


def test_new_field_does_not_retain_prior_unknown_status():
    prior = sync.parse_prior([sync.HEADERS, [
        "GRT-UN.TO", "2026-10-22", 14, "", "", ASOF,
        "original provider [events unknown:ex-div] +carried"]])
    symbols, ctx, carried, _, _ = sync.apply_sticky(
        ["GRT-UN.TO"], {"GRT-UN.TO": {"next_ex_div_date": "2026-10-15"}}, prior)
    row = sync.build_rows(symbols, ctx, "new provider", carried)[0]
    assert row[1] == "2026-10-22" and row[3] == "2026-10-15"
    assert row[5] == ASOF and "fresh ex-div: new provider" in row[6]
    assert "carried earnings: original provider" in row[6]
    assert "events unknown" not in row[6]


class Sheet:
    def __init__(self, values, row_count=1000, read_error=None):
        self.values, self.row_count, self.read_error = values, row_count, read_error
        self.reads, self.updates, self.clears = [], [], []

    def get(self, range_name):
        self.reads.append(range_name)
        if self.read_error:
            raise self.read_error
        return self.values

    def update(self, **kwargs):
        self.updates.append(kwargs)

    def batch_clear(self, ranges):
        self.clears.append(ranges)

    def freeze(self, **_kwargs):
        pass


def run_main(monkeypatch, calendar, page=None):
    page = page or Sheet([["Symbol"], ["GRT-UN.TO"], ["FORECAST"]])

    class Book:
        def worksheet(self, name):
            return calendar if name == "Calendar_Events" else page

    provider = ModuleType("core.providers.calendar_provider")
    provider.__version__ = "synthetic"
    provider.is_enabled = lambda: True
    calls = []
    def fetch(symbols):
        calls.append(list(symbols))
        return {s: {"next_earnings_date": None, "next_ex_div_date": None} for s in symbols}
    provider.fetch_event_context_sync = fetch
    monkeypatch.setitem(sys.modules, "core.providers.calendar_provider", provider)
    monkeypatch.setattr(sync, "_open_book", lambda: Book())
    monkeypatch.setenv("TFB_CALENDAR_PAGES", "Top_10_Investments")
    return sync.main(["--write"]), calls


def test_actual_write_boundary_keeps_known_event_after_old_400_row_cap(monkeypatch):
    values = [sync.HEADERS] + [[""] * 7 for _ in range(449)]
    values.append(["LATE.US", "2026-10-22", 14, "", "", ASOF, "original provider"])
    calendar = Sheet(values)
    result, calls = run_main(monkeypatch, calendar)
    assert result == 0 and calls == [["GRT-UN.TO"]]
    assert calendar.reads == ["A1:G1000"]
    rows = {r[0]: r for r in calendar.updates[-1]["values"]}
    assert rows["LATE.US"][1:3] == ["2026-10-22", 13]
    assert rows["LATE.US"][5] == ASOF and "original provider" in rows["LATE.US"][6]
    assert rows["GRT-UN.TO"][1:6] == ["", "", "", "", ""]


def test_full_replacement_erases_stale_tail_when_calendar_shrinks(monkeypatch):
    class StatefulSheet(Sheet):
        def update(self, **kwargs):
            super().update(**kwargs)
            first_row = int(kwargs["range_name"].split(":")[0][1:]) - 1
            for offset, row in enumerate(kwargs["values"]):
                self.values[first_row + offset] = list(row)

        def batch_clear(self, ranges):
            super().batch_clear(ranges)
            for range_name in ranges:
                first, last = range_name.split(":")
                for index in range(int(first[1:]) - 1, min(int(last[1:]), len(self.values))):
                    self.values[index] = [""] * 7

    values = [list(sync.HEADERS)] + [[""] * 7 for _ in range(1100)]
    # These all lie below the old write-side 1000-row clear. Only the valid
    # future event should survive, once, in the newly compacted table.
    values[1050] = ["EXPIRED.US", "2026-10-08", 1, "", "", ASOF, "expired provider"]
    values[1075] = ["FORECAST", "2026-10-22", 14, "", "", ASOF, "junk provider"]
    values[1100] = ["LATE.US", "2026-10-22", 14, "", "", ASOF, "original provider"]
    calendar = StatefulSheet(values, row_count=1101)

    result, calls = run_main(monkeypatch, calendar)

    assert result == 0 and calls == [["GRT-UN.TO"]]
    assert calendar.values[0] == sync.HEADERS
    populated = [row for row in calendar.values[1:] if row[0]]
    assert [row[0] for row in populated] == ["GRT-UN.TO", "LATE.US"]
    assert populated[0][1:6] == ["", "", "", "", ""]
    assert populated[1][1:3] == ["2026-10-22", 13]
    assert populated[1][5] == ASOF
    assert populated[1][6] == "original provider [events unknown:ex-div] +carried"
    assert all(not any(row) for row in calendar.values[3:]), "Old rows must be cleared through the full prior extent"


def test_unreadable_prior_table_never_clears_known_events(monkeypatch):
    calendar = Sheet([], read_error=RuntimeError("synthetic read failure"))
    result, _ = run_main(monkeypatch, calendar)
    assert result == 2 and calendar.updates == calendar.clears == []


def test_overbound_grid_never_silently_truncates_even_with_blank_sentinel(monkeypatch):
    calendar = Sheet([sync.HEADERS], row_count=6000)
    result, _ = run_main(monkeypatch, calendar)
    assert result == 2 and calendar.reads == []
    assert calendar.updates == calendar.clears == []


def test_oversize_returned_values_refuse_publication(monkeypatch):
    calendar = Sheet([sync.HEADERS] + [[""]] * 5001)
    result, _ = run_main(monkeypatch, calendar)
    assert result == 2 and calendar.updates == calendar.clears == []


def test_combined_harvest_and_carried_table_over_capacity_refuses_publication(monkeypatch):
    values = [sync.HEADERS] + [
        [f"A{i}.US", "2026-10-22", 14, "", "", ASOF, "original provider"]
        for i in range(5000)]
    calendar = Sheet(values, row_count=5001)
    result, _ = run_main(monkeypatch, calendar)
    assert result == 2 and calendar.updates == calendar.clears == []


def test_actual_provider_preserves_hyphenated_canonical_request():
    pytest.importorskip("httpx")
    from core.providers import calendar_provider
    codes, ksa = calendar_provider._split_symbols(["GRT-UN.TO", "2222.SR"])
    assert codes == {"GRT-UN.TO": "GRT-UN.TO"} and ksa == ["2222.SR"]
    assert calendar_provider._yahoo_symbol_for_cal("GRT-UN.TO") == "GRT-UN.TO"
