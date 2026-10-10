"""Actual calendar CLI publication against an atomic Sheets transport fixture.

Failures/cancellation can happen before commit or after server commit with no
acknowledgement. Readers must see a complete old or complete new generation.
No workbook or provider is contacted.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date
import importlib.util
from pathlib import Path
import re
import sys
from types import ModuleType

import pytest


SPEC = importlib.util.spec_from_file_location(
    "calendar_atomic_target", Path(__file__).parents[1] / "scripts/run_calendar_sync.py")
sync = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sync)
TODAY = date(2026, 10, 10)
ASOF = "2026-10-08 08:15"


def event(symbol="OLD.US", earnings="2026-10-23", exdiv="", source="original provider"):
    return [symbol, earnings, 13 if earnings else "", exdiv,
            10 if exdiv else "", ASOF, source]


class AtomicSheet:
    def __init__(self, rows=None, row_count=1000, failure=None, cancel=None,
                 resize_error=None, freeze_error=None, read_error=None):
        self.values = deepcopy(rows if rows is not None else [sync.HEADERS, event()])
        self.values.extend([[""] * 7 for _ in range(row_count - len(self.values))])
        self.row_count = row_count
        self.failure, self.cancel = failure, cancel
        self.resize_error, self.freeze_error, self.read_error = resize_error, freeze_error, read_error
        self.reads, self.requests, self.resizes, self.observed = [], [], [], []
        self.freezes = 0

    def get(self, range_name):
        self.reads.append(range_name)
        if self.read_error:
            raise self.read_error
        return deepcopy(self.values)

    def resize(self, *, rows):
        self.resizes.append(rows)
        self.observed.append(deepcopy(self.values))
        if self.resize_error:
            raise self.resize_error
        assert rows >= self.row_count, "Grid preparation must not remove prior cells"
        self.values.extend([[""] * 7 for _ in range(rows - self.row_count)])
        self.row_count = rows

    def update(self, *, values, range_name, value_input_option):
        # One request validates in full before the simulated server commits it.
        self.requests.append({"values": deepcopy(values), "range": range_name,
                              "value_input_option": value_input_option})
        self.observed.append(deepcopy(self.values))
        assert value_input_option == "RAW"
        match = re.fullmatch(r"A1:G([1-9][0-9]*)", range_name)
        assert match, "Header and body must share the same request"
        extent = int(match.group(1))
        assert extent == len(values) and extent <= self.row_count
        assert all(len(row) == 7 and all(cell is not None for cell in row) for row in values)
        if self.cancel == "before_commit":
            raise KeyboardInterrupt("synthetic cancellation before commit")
        if self.failure == "before_commit":
            raise RuntimeError("synthetic rejection before commit")
        # There is no observable per-cell/row mutation between these snapshots.
        new_values = deepcopy(self.values)
        new_values[:extent] = deepcopy(values)
        self.values = new_values
        self.observed.append(deepcopy(self.values))
        if self.cancel == "after_commit":
            raise KeyboardInterrupt("synthetic cancellation after commit")
        if self.failure == "after_commit":
            raise RuntimeError("synthetic lost acknowledgement after commit")

    def batch_clear(self, _ranges):
        raise AssertionError("No clear may precede publication")

    def freeze(self, *, rows):
        assert rows == 1
        self.freezes += 1
        if self.freeze_error:
            raise self.freeze_error


class WorksheetNotFound(Exception):
    pass


class Book:
    def __init__(self, calendar, symbols):
        self.calendar, self.symbols = calendar, symbols
        self.creates = []
        self.calendar_reads = 0

    def worksheet(self, name):
        if name == "Calendar_Events":
            self.calendar_reads += 1
            if self.calendar is None:
                raise WorksheetNotFound("no prior calendar")
            return self.calendar
        return type("Page", (), {"get": lambda _self, _range: [["Symbol"]] +
                                 [[symbol] for symbol in self.symbols]})()

    def add_worksheet(self, *, title, rows, cols):
        assert title == "Calendar_Events" and cols == 7
        assert self.calendar is None
        self.creates.append({"title": title, "rows": rows, "cols": cols})
        self.calendar = AtomicSheet(rows=[], row_count=rows)
        return self.calendar


def run_main(monkeypatch, calendar, symbols=None, provider_context=None, mode="--write"):
    book = Book(calendar, ["NEW.US"] if symbols is None else symbols)
    provider = ModuleType("core.providers.calendar_provider")
    provider.__version__ = "synthetic"
    provider.is_enabled = lambda: True
    provider.fetch_event_context_sync = lambda _symbols: deepcopy(provider_context or {})
    monkeypatch.setitem(sys.modules, "core.providers.calendar_provider", provider)
    monkeypatch.setattr(sync, "_open_book", lambda: book)
    monkeypatch.setattr(sync, "_today_riyadh", lambda: TODAY)
    monkeypatch.setenv("TFB_CALENDAR_PAGES", "Top_10_Investments")
    monkeypatch.delenv("TFB_CALENDAR_TICKER_GUARD", raising=False)
    return sync.main([mode]), book


def populated(sheet):
    return [row for row in sheet.values[1:] if row[0]]


@pytest.mark.parametrize("row_count", [3, 1000, 5001])
def test_rejected_publication_preserves_old_complete_generation(monkeypatch, row_count):
    old = [sync.HEADERS, event(), event("EXPIRED.US", earnings="2026-10-09")]
    calendar = AtomicSheet(old, row_count=row_count, failure="before_commit")
    original = deepcopy(calendar.values)
    code, book = run_main(monkeypatch, calendar)
    assert code == 3
    assert calendar.values[:len(original)] == original
    assert not any(any(row) for row in calendar.values[len(original):])
    assert len(calendar.requests) == 1 and book.calendar_reads == 1
    assert calendar.freezes == 0


@pytest.mark.parametrize("cancel", ["before_commit", "after_commit"])
def test_cancellation_leaves_one_complete_generation(monkeypatch, cancel):
    calendar = AtomicSheet(cancel=cancel)
    original = deepcopy(calendar.values)
    with pytest.raises(KeyboardInterrupt, match="synthetic cancellation"):
        run_main(monkeypatch, calendar)
    assert len(calendar.requests) == 1
    committed = calendar.requests[0]["values"]
    assert calendar.values == (original if cancel == "before_commit" else committed)
    assert all(snapshot in (original, committed) for snapshot in calendar.observed)
    assert calendar.freezes == 0


def test_lost_acknowledgement_reports_failure_without_retrying_complete_new_generation(monkeypatch, capsys):
    calendar = AtomicSheet(failure="after_commit")
    original = deepcopy(calendar.values)
    code, _book = run_main(monkeypatch, calendar)
    assert code == 3 and len(calendar.requests) == 1
    committed = calendar.requests[0]["values"]
    assert calendar.values == committed
    assert calendar.values != original
    assert [row[0] for row in populated(calendar)] == ["NEW.US", "OLD.US"]
    assert all(snapshot in (original, committed) for snapshot in calendar.observed)
    assert "publication unconfirmed" in capsys.readouterr().err


def test_successful_shrink_replaces_header_and_clears_entire_prior_tail(monkeypatch):
    rows = [["Symbol", "Next Earnings Date", "Days To Earnings", "Next Ex-Div Date",
             "Days To ExDiv", "Updated At (Riyadh)", "Source"]] + [[""] * 7 for _ in range(1199)]
    rows[1] = event()
    rows[1025] = event("EXPIRED.US", earnings="2026-10-09")
    rows[1100] = event("FORECAST")
    rows[1199] = event("DIV.US", earnings="", exdiv="2026-10-20")
    calendar = AtomicSheet(rows, row_count=1200)
    code, _book = run_main(monkeypatch, calendar)
    assert code == 0 and len(calendar.requests) == 1
    request = calendar.requests[0]
    assert request["range"] == "A1:G1200" and request["values"][0] == sync.HEADERS
    assert [row[0] for row in populated(calendar)] == ["NEW.US", "OLD.US", "DIV.US"]
    assert populated(calendar)[1][5] == populated(calendar)[2][5] == ASOF
    assert "original provider" in populated(calendar)[1][6]
    assert all(row == [""] * 7 for row in calendar.values[4:])
    assert isinstance(populated(calendar)[1][2], int)
    assert not calendar.resizes and calendar.freezes == 1


def test_growth_resizes_without_erasing_prior_then_publishes_once(monkeypatch):
    calendar = AtomicSheet(row_count=3)
    symbols = [f"A{i}.US" for i in range(1001)]
    code, book = run_main(monkeypatch, calendar, symbols=symbols)
    assert code == 0 and len(calendar.requests) == 1
    assert calendar.resizes == [1003]
    assert calendar.requests[0]["range"] == "A1:G1003"
    assert populated(calendar)[-1][0] == "OLD.US"
    assert populated(calendar)[-1][5] == ASOF and book.calendar_reads == 1


@pytest.mark.parametrize("error", [RuntimeError("synthetic resize rejection"),
                                  KeyboardInterrupt("synthetic resize cancellation")])
def test_resize_failure_or_cancellation_retains_original_values(monkeypatch, error):
    calendar = AtomicSheet(row_count=3, resize_error=error)
    original = deepcopy(calendar.values)
    if isinstance(error, KeyboardInterrupt):
        with pytest.raises(KeyboardInterrupt):
            run_main(monkeypatch, calendar)
    else:
        code, _book = run_main(monkeypatch, calendar)
        assert code == 3
    assert calendar.values == original and not calendar.requests


def test_first_publication_allocates_complete_grid_and_writes_once(monkeypatch):
    symbols = [f"A{i}.US" for i in range(1001)]
    code, book = run_main(monkeypatch, None, symbols=symbols)
    assert code == 0 and book.creates == [{"title": "Calendar_Events", "rows": 1002, "cols": 7}]
    assert book.calendar.values[0] == sync.HEADERS
    assert len(populated(book.calendar)) == 1001 and len(book.calendar.requests) == 1
    assert not book.calendar.resizes


def test_dry_run_retains_prior_generation_and_does_not_prepare_grid(monkeypatch):
    calendar = AtomicSheet(row_count=3)
    original = deepcopy(calendar.values)
    code, _book = run_main(monkeypatch, calendar, mode="--dry-run")
    assert code == 0 and calendar.values == original
    assert not calendar.requests and not calendar.resizes and calendar.freezes == 0


@pytest.mark.parametrize("kind", ["unreadable", "oversize_grid", "oversize_cohort"])
def test_complete_read_and_capacity_refusals_precede_all_publication_mutations(monkeypatch, kind):
    calendar = AtomicSheet(row_count=6000 if kind == "oversize_grid" else 1000,
                           read_error=RuntimeError("synthetic prior read failure")
                           if kind == "unreadable" else None)
    original = deepcopy(calendar.values)
    symbols = [f"A{i}.US" for i in range(5000)] if kind == "oversize_cohort" else None
    code, _book = run_main(monkeypatch, calendar, symbols=symbols)
    assert code == 2 and calendar.values == original
    assert not calendar.requests and not calendar.resizes and calendar.freezes == 0


def test_best_effort_freeze_failure_cannot_undo_complete_content_commit(monkeypatch):
    calendar = AtomicSheet(freeze_error=RuntimeError("synthetic freeze unavailable"))
    code, _book = run_main(monkeypatch, calendar)
    assert code == 0 and len(calendar.requests) == 1
    assert calendar.values == calendar.requests[0]["values"]
    assert [row[0] for row in populated(calendar)] == ["NEW.US", "OLD.US"]
