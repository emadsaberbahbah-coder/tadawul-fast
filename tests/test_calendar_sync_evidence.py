"""Actual calendar evidence merge and Yahoo-only CLI publication regressions."""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


SPEC = importlib.util.spec_from_file_location(
    "calendar_sync_evidence_target", Path(__file__).parents[1] / "scripts/run_calendar_sync.py")
sync = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sync)
TODAY = date(2026, 10, 10)
OLD_OBS = "2026-10-08T05:15:00Z"
NEW_OBS = "2026-10-10T05:15:00Z"
OLD_ASOF = "2026-10-08 08:15"


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    monkeypatch.setattr(sync, "_today_riyadh", lambda: TODAY)
    monkeypatch.delenv("TFB_CALENDAR_TICKER_GUARD", raising=False)


def rich(earnings="2026-10-22", exdiv="2026-10-16", *,
         earnings_source="eodhd", exdiv_source="yahoo", observed=NEW_OBS):
    row = {"next_earnings_date": earnings, "next_ex_div_date": exdiv}
    for prefix, event_date, source in (("earnings", earnings, earnings_source),
                                       ("exdiv", exdiv, exdiv_source)):
        row.update({prefix + "_source": source if event_date else "unknown",
                    prefix + "_observed_at": observed if event_date else "",
                    prefix + "_status": ("estimated" if prefix == "earnings" and source == "yahoo"
                                          else "reported") if event_date else "unknown"})
    return row


def prior_table(context=None):
    context = context or rich(observed=OLD_OBS)
    row = sync.build_rows(["ACME.US"], {"ACME.US": context}, "calendar provider")[0]
    row[5] = OLD_ASOF
    return [list(sync.HEADERS), row]


def merged_row(fresh, prior_values):
    symbols, ctx, carried, *_ = sync.apply_sticky(
        ["ACME.US"], {"ACME.US": fresh}, sync.parse_prior(prior_values))
    return sync.build_rows(symbols, ctx, "calendar provider", carried)[0], ctx["ACME.US"]


def test_first_seven_columns_stay_stable_and_actual_sources_are_truthful():
    row = sync.build_rows(["ACME.US"], {"ACME.US": rich()}, "wrong generic source")[0]
    assert sync.HEADERS[:7] == ["Symbol", "Next Earnings Date", "Days To Earnings",
                                "Next Ex-Div Date", "Days To ExDiv", "Updated At (Riyadh)", "Source"]
    assert len(row) == 13 and row[1:5] == ["2026-10-22", 12, "2026-10-16", 6]
    assert row[7:] == ["eodhd", NEW_OBS, "reported", "yahoo", NEW_OBS, "reported"]
    assert "earnings: eodhd (reported)" in row[6]
    assert "ex-div: yahoo (reported)" in row[6] and "wrong generic" not in row[6]


def test_legacy_source_and_publication_time_never_become_typed_evidence():
    values = [sync.HEADERS[:7], ["ACME.US", "2026-10-22", 12, "2026-10-16", 6,
                                OLD_ASOF, "issuer confirmed / EODHD"]]
    row, ctx = merged_row({}, values)
    assert row[1] == "2026-10-22" and row[3] == "2026-10-16" and row[5] == OLD_ASOF
    assert row[7:] == ["unknown", "", "unknown", "unknown", "", "unknown"]
    assert ctx["earnings_source"] == ctx["exdiv_source"] == "unknown"


@pytest.mark.parametrize("fresh_field", ["earnings", "exdiv"])
def test_partial_refresh_retains_each_siblings_original_evidence(fresh_field):
    fresh = rich(earnings="2026-10-25" if fresh_field == "earnings" else None,
                 exdiv="2026-10-18" if fresh_field == "exdiv" else None,
                 earnings_source="yahoo", exdiv_source="eodhd")
    row, ctx = merged_row(fresh, prior_table())
    carried_field = "exdiv" if fresh_field == "earnings" else "earnings"
    assert ctx[fresh_field + "_observed_at"] == NEW_OBS
    assert ctx[carried_field + "_observed_at"] == OLD_OBS
    assert row[5] == OLD_ASOF and row[6].endswith("+carried")
    assert "fresh" in row[6] and "carried" in row[6]
    assert ctx["earnings_status"] == ("estimated" if fresh_field == "earnings" else "reported")
    assert ctx["exdiv_status"] == "reported"


def test_repeated_sticky_roundtrip_preserves_both_field_observations():
    values = prior_table(rich(earnings_source="yahoo", exdiv_source="eodhd", observed=OLD_OBS))
    expected = values[1][7:]
    for _ in range(4):
        row, _ctx = merged_row({}, values)
        assert row[7:] == expected and row[5] == OLD_ASOF
        assert row[6].count("+carried") == 1
        values = [sync.HEADERS, row]


def test_alternating_partial_refresh_summary_does_not_nest_or_misattribute_fields():
    values = prior_table()
    for i in range(8):
        earnings = i % 2 == 0
        fresh = rich(earnings="2026-10-25" if earnings else None,
                     exdiv=None if earnings else "2026-10-18",
                     earnings_source="yahoo", exdiv_source="eodhd")
        row, _ctx = merged_row(fresh, values)
        assert row[6].count("earnings:") == row[6].count("ex-div:") == 1
        assert row[6].count("+carried") == 1 and len(row[6]) < 100
        assert "earnings: yahoo (estimated)" in row[6]
        assert "ex-div: " in row[6]
        values = [sync.HEADERS, row]


def test_invalid_fresh_date_cannot_attach_new_provenance_to_carried_fact():
    fresh = rich(earnings="2026-02-31", exdiv="2026-10-09", earnings_source="yahoo")
    prior = prior_table()
    row, _ctx = merged_row(fresh, prior)
    assert row[1] == prior[1][1] and row[3] == prior[1][3]
    assert row[7:] == prior[1][7:]


def test_complete_refresh_replaces_prior_metadata_with_new_field_sources():
    fresh = rich(earnings="2026-10-25", exdiv="2026-10-18",
                 earnings_source="yahoo", exdiv_source="eodhd")
    row, _ctx = merged_row(fresh, prior_table())
    assert row[7:] == ["yahoo", NEW_OBS, "estimated", "eodhd", NEW_OBS, "reported"]
    assert "carried" not in row[6]


@pytest.mark.parametrize("key,value", [
    ("earnings_source", "issuer"), ("earnings_status", "confirmed"),
    ("earnings_status", "estimated"), ("earnings_observed_at", "2026-10-08 08:15"),
    ("earnings_observed_at", "2026-10-08T08:15:00+03:00"),
])
def test_unsupported_provenance_claims_remain_unknown_per_field(key, value):
    ctx = rich()
    ctx[key] = value
    row = sync.build_rows(["ACME.US"], {"ACME.US": ctx}, "provider")[0]
    assert row[1] == "2026-10-22" and row[7:10] == ["unknown", "", "unknown"]
    assert row[10:] == ["yahoo", NEW_OBS, "reported"]


def test_expired_field_loses_its_evidence_while_future_sibling_is_carried():
    prior = prior_table(rich(earnings="2026-10-09", exdiv="2026-10-16", observed=OLD_OBS))
    row, _ctx = merged_row({}, prior)
    assert row[1] == "" and row[7:10] == ["unknown", "", "unknown"]
    assert row[3] == "2026-10-16" and row[10:] == ["yahoo", OLD_OBS, "reported"]


@pytest.mark.parametrize("conflict", ["date", "source", "observation"])
def test_conflicting_prior_generations_are_refused_instead_of_selecting_last(conflict):
    values = prior_table()
    other = list(values[1])
    index = {"date": 1, "source": 7, "observation": 8}[conflict]
    other[index] = {"date": "2026-10-23", "source": "yahoo", "observation": NEW_OBS}[conflict]
    values.append(other)
    with pytest.raises(ValueError, match="conflicting prior calendar"):
        sync.parse_prior(values)


def test_identical_prior_duplicates_are_deduplicated_without_losing_evidence():
    values = prior_table()
    values.append(list(values[1]))
    assert list(sync.parse_prior(values)) == ["ACME.US"]


def test_duplicate_prior_headers_are_refused():
    values = prior_table()
    values[0][8] = "Earnings Source"
    with pytest.raises(ValueError, match="duplicate prior calendar headers"):
        sync.parse_prior(values)


@pytest.mark.parametrize("kind", ["headerless", "missing_earnings", "missing_exdiv", "conflicting_rows"])
def test_actual_cli_preserves_prior_table_when_schema_or_generation_is_ambiguous(monkeypatch, kind):
    from core.providers import calendar_provider as provider

    values = prior_table()
    if kind == "headerless":
        values = values[1:]
    elif kind == "missing_earnings":
        values[0][1] = "Future Earnings"
    elif kind == "missing_exdiv":
        values[0][3] = "ExDiv Future"
    else:
        conflicting = list(values[1])
        conflicting[1] = "2026-10-23"
        values.append(conflicting)
    page, calendar = Sheet([["Symbol"], ["ACME.US"]]), Sheet(values, columns=13)
    original = deepcopy(calendar.values)
    book = SimpleNamespace(worksheet=lambda name: calendar if name == "Calendar_Events" else page)
    monkeypatch.setattr(sync, "_open_book", lambda: book)
    monkeypatch.setattr(provider, "fetch_event_evidence_sync", lambda _symbols: {})
    monkeypatch.setenv("TFB_CALENDAR_PAGES", "Top_10_Investments")
    assert sync.main(["--write"]) == 2
    assert calendar.values == original and not calendar.updates and not calendar.resizes


def invoke_with_prior(monkeypatch, calendar):
    from core.providers import calendar_provider as provider
    page = Sheet([["Symbol"], ["ACME.US"]])
    book = SimpleNamespace(worksheet=lambda name: calendar if name == "Calendar_Events" else page)
    monkeypatch.setattr(sync, "_open_book", lambda: book)
    monkeypatch.setattr(provider, "fetch_event_evidence_sync", lambda _symbols: {})
    monkeypatch.setenv("TFB_CALENDAR_PAGES", "Top_10_Investments")
    return sync.main(["--write"])


@pytest.mark.parametrize("column", [7, 12])
@pytest.mark.parametrize("value", ["operator note", "2026-10-22", 0, False])
def test_actual_cli_refuses_legacy_migration_over_unheaded_extension_data(monkeypatch, column, value):
    values = [sync.HEADERS[:7]] + [[""] * 13 for _ in range(999)]
    values[1][:7] = ["ACME.US", "2026-10-22", 12, "", "", OLD_ASOF, "legacy provider"]
    # Blank-separated data lies well beyond the former sampled calendar range.
    values[900][column] = value
    calendar = Sheet(values, columns=13)
    original = deepcopy(calendar.values)
    assert invoke_with_prior(monkeypatch, calendar) == 2
    assert calendar.reads == ["A1:M1000"]
    assert calendar.values == original and not calendar.resizes and not calendar.updates


@pytest.mark.parametrize("extension", [["Earnings Source"], ["Operator Notes"], [0], [False]])
def test_actual_cli_refuses_partial_or_unowned_extension_headers(monkeypatch, extension):
    values = [sync.HEADERS[:7] + extension, ["ACME.US", "2026-10-22"]]
    calendar = Sheet(values, columns=13)
    original = deepcopy(calendar.values)
    assert invoke_with_prior(monkeypatch, calendar) == 2
    assert calendar.values == original and not calendar.resizes and not calendar.updates


def test_legacy_header_with_proven_blank_existing_extension_migrates_once(monkeypatch):
    values = [sync.HEADERS[:7] + [""] * 6,
              ["ACME.US", "2026-10-22", 12, "", "", OLD_ASOF, "legacy provider"]]
    calendar = Sheet(values, columns=13)
    assert invoke_with_prior(monkeypatch, calendar) == 0
    assert calendar.reads == ["A1:M1000"] and not calendar.resizes
    assert len(calendar.updates) == 1
    row = calendar.updates[0]["values"][1]
    assert row[1] == "2026-10-22" and row[7:] == ["unknown", "", "unknown", "unknown", "", "unknown"]


@pytest.mark.parametrize("dimension", ["row_count", "col_count"])
def test_actual_cli_refuses_unknown_grid_extent_before_claiming_extension(monkeypatch, dimension):
    calendar = Sheet([sync.HEADERS[:7], ["ACME.US", "2026-10-22"]], columns=13)
    setattr(calendar, dimension, None)
    original = deepcopy(calendar.values)
    assert invoke_with_prior(monkeypatch, calendar) == 2
    assert calendar.values == original and not calendar.reads and not calendar.resizes and not calendar.updates


def test_actual_cli_preserves_legacy_grid_when_complete_extension_read_fails(monkeypatch):
    calendar = Sheet([sync.HEADERS[:7], ["ACME.US", "2026-10-22"]], columns=13)
    original = deepcopy(calendar.values)
    def failed_read(name):
        assert name == "A1:M1000"
        raise RuntimeError("complete legacy extension unreadable")
    monkeypatch.setattr(calendar, "get", failed_read)
    assert invoke_with_prior(monkeypatch, calendar) == 2
    assert calendar.values == original and not calendar.resizes and not calendar.updates


class Sheet:
    def __init__(self, values, columns=7):
        self.values, self.col_count, self.row_count = deepcopy(values), columns, 1000
        self.reads, self.resizes, self.updates = [], [], []

    def get(self, name):
        self.reads.append(name)
        return deepcopy(self.values)

    def resize(self, **dimensions):
        self.resizes.append(dimensions)
        self.col_count = dimensions.get("cols", self.col_count)
        self.row_count = dimensions.get("rows", self.row_count)

    def update(self, **request):
        self.updates.append(deepcopy(request))

    def freeze(self, **_kwargs):
        pass


def test_actual_cli_uses_yahoo_only_and_publishes_its_real_evidence(monkeypatch):
    from core.providers import calendar_provider as provider

    page = Sheet([["Symbol"], ["ACME.US"]])
    calendar = Sheet([sync.HEADERS[:7]])
    book = SimpleNamespace(worksheet=lambda name: calendar if name == "Calendar_Events" else page)

    class Ticker:
        def __init__(self, _symbol): pass
        def get_earnings_dates(self, limit):
            assert limit == 12
            return SimpleNamespace(index=[datetime(2026, 10, 22)])
        @property
        def calendar(self): return {"Ex-Dividend Date": date(2026, 10, 16)}

    async def inline(function, *args, **kwargs): return function(*args, **kwargs)

    monkeypatch.setattr(sync, "_open_book", lambda: book)
    monkeypatch.setattr(provider, "_today", lambda: TODAY)
    monkeypatch.setattr(provider, "_observed_at_utc", lambda: NEW_OBS)
    monkeypatch.setattr(provider, "_yf", SimpleNamespace(Ticker=Ticker))
    monkeypatch.setattr(provider.asyncio, "to_thread", inline)
    monkeypatch.setattr(provider, "_client", lambda: pytest.fail("Yahoo-only mode must not open EODHD HTTP"))
    for name in ("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_KEY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TFB_CALENDAR_ENABLED", "1")
    monkeypatch.setenv("TFB_CAL_YAHOO_FALLBACK", "1")
    monkeypatch.setenv("TFB_CALENDAR_PAGES", "Top_10_Investments")

    assert sync.main(["--write"]) == 0
    assert calendar.reads == ["A1:G1000"] and calendar.resizes == [{"cols": 13}]
    assert len(calendar.updates) == 1
    request = calendar.updates[0]
    assert request["range_name"] == "A1:M1000" and request["values"][0] == sync.HEADERS
    row = request["values"][1]
    assert row[7:] == ["yahoo", NEW_OBS, "estimated", "yahoo", NEW_OBS, "reported"]
    assert "yahoo" in row[6] and "eodhd" not in row[6]
    assert all(r == [""] * 13 for r in request["values"][2:])
