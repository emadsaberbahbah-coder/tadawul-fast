"""Exercise the real collector with offline news and Sheets boundaries."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripts import news_archive_collector as archive

DATE = "2026-10-17"
OBSERVATION = "NEWS_TEST_A"


class FakeWorksheet:
    def __init__(self, values, *, header_error=False, read_error=False):
        self.values = deepcopy(values)
        self.header_error = header_error
        self.read_error = read_error
        self.header_writes = []
        self.appends = []

    def row_values(self, row):
        if self.header_error:
            raise OSError("offline header read failed")
        return deepcopy(self.values[row - 1] if len(self.values) >= row else [])

    def get_all_values(self):
        if self.read_error:
            raise OSError("offline archive read failed")
        return deepcopy(self.values)

    def update(self, *, values, range_name):
        self.header_writes.append((deepcopy(values), range_name))
        while len(self.values) < archive.NewsArchiveStore.START_ROW:
            self.values.append([])
        self.values[archive.NewsArchiveStore.START_ROW - 1] = deepcopy(values[0])
        return {"updatedRows": len(values)}

    def append_rows(self, rows, *, value_input_option):
        assert value_input_option == "RAW"
        self.appends.append(deepcopy(rows))
        self.values.extend(deepcopy(rows))
        return {"updates": {"updatedRows": len(rows)}}


def grid(*data, header=None):
    return [[], [], [], [], list(archive.NewsArchiveStore.HEADERS if header is None else header),
            *[list(row) for row in data]]


def observation(symbol=OBSERVATION, *, articles=0):
    return {"symbol": symbol, "sentiment": 0, "confidence": 0, "news_boost": 0,
            "article_velocity": 0, "articles_analyzed": articles, "version": "offline"}


def install(monkeypatch, worksheet, *, items=None, status="success", errors=0,
            batch_errors=None, dry_run=False):
    book = SimpleNamespace(worksheet=lambda _: worksheet)
    client = SimpleNamespace(open_by_key=lambda _: book)
    monkeypatch.setattr(archive, "GSPREAD_AVAILABLE", True)
    monkeypatch.setattr(archive, "gspread", SimpleNamespace(authorize=lambda _: client))
    monkeypatch.setattr(archive.NewsArchiveStore, "_load_creds", lambda _: object())
    # No real service or backoff sleeps are used by these boundary cases.
    monkeypatch.setattr(archive._Backoff, "run", lambda _, operation: operation())
    batch = SimpleNamespace(items=[observation()] if items is None else items,
                            meta={"status": status, "errors": errors},
                            errors=[] if batch_errors is None else batch_errors)
    fetch = AsyncMock(return_value=batch)
    monkeypatch.setattr(archive, "_import_news_module",
                        lambda: SimpleNamespace(batch_news_intelligence=fetch))
    monkeypatch.setattr(archive, "_riyadh_date", lambda: DATE)
    monkeypatch.setenv("TFB_NEWS_ARCHIVE_SECTOR_JOIN", "0")
    args = SimpleNamespace(universe=[OBSERVATION], sheet_id="offline-book", dry_run=dry_run)
    return args, fetch


@pytest.mark.parametrize("failure", ["header", "archive"])
@pytest.mark.parametrize("values", [grid([DATE, OBSERVATION]), []])
def test_failed_archive_read_never_rewrites_or_appends(monkeypatch, failure, values):
    worksheet = FakeWorksheet(values,
                              header_error=failure == "header", read_error=failure == "archive")
    original = deepcopy(worksheet.values)
    args, _ = install(monkeypatch, worksheet)

    assert asyncio.run(archive.collect(args)) != 0
    assert worksheet.header_writes == []
    assert worksheet.appends == []
    assert worksheet.values == original


@pytest.mark.parametrize("header", [
    list(reversed(archive.NewsArchiveStore.HEADERS)),
    archive.NewsArchiveStore.HEADERS[:-1],
    archive.NewsArchiveStore.HEADERS + ["Unexpected Column"],
])
def test_populated_incompatible_schema_is_not_relabelled(monkeypatch, header):
    worksheet = FakeWorksheet(grid([DATE, OBSERVATION], header=header))
    original = deepcopy(worksheet.values)
    args, _ = install(monkeypatch, worksheet)

    assert asyncio.run(archive.collect(args)) != 0
    assert worksheet.header_writes == []
    assert worksheet.appends == []
    assert worksheet.values == original


@pytest.mark.parametrize("values", [
    grid([DATE, OBSERVATION], header=[]),
    [["existing archive note"]],
    [[0]],
])
def test_missing_header_in_nonempty_archive_cannot_be_initialized(monkeypatch, values):
    worksheet = FakeWorksheet(values)
    original = deepcopy(worksheet.values)
    args, _ = install(monkeypatch, worksheet)

    assert asyncio.run(archive.collect(args)) != 0
    assert worksheet.header_writes == []
    assert worksheet.appends == []
    assert worksheet.values == original


@pytest.mark.parametrize("status,errors", [("error", 1), ("partial", 1), (None, 0)])
@pytest.mark.parametrize("dry_run", [False, True])
def test_failed_empty_collection_is_not_a_successful_skip(monkeypatch, status, errors, dry_run):
    worksheet = FakeWorksheet(grid())
    args, _ = install(monkeypatch, worksheet, items=[], status=status, errors=errors,
                      dry_run=dry_run)

    assert asyncio.run(archive.collect(args)) != 0
    assert worksheet.header_writes == []
    assert worksheet.appends == []


@pytest.mark.parametrize("header", [archive.NewsArchiveStore.HEADERS,
                                  archive.NewsArchiveStore.HEADERS + ["", " "]])
@pytest.mark.parametrize("status,errors", [("success", 0), ("partial", 1)])
def test_canonical_archive_skips_a_confirmed_existing_observation(monkeypatch, header, status, errors):
    worksheet = FakeWorksheet(grid([DATE, OBSERVATION], header=header))
    args, fetch = install(monkeypatch, worksheet, status=status, errors=errors)

    assert asyncio.run(archive.collect(args)) == 0
    fetch.assert_awaited_once()
    assert worksheet.header_writes == []
    assert worksheet.appends == []


@pytest.mark.parametrize("status,errors", [("success", 0), ("partial", 1)])
def test_valid_observation_with_no_articles_is_still_archived(monkeypatch, status, errors):
    worksheet = FakeWorksheet(grid())
    args, _ = install(monkeypatch, worksheet, status=status, errors=errors,
                      batch_errors=[{"error": "offline failure"}] if errors else [])

    assert asyncio.run(archive.collect(args)) == 0
    assert worksheet.header_writes == []
    assert len(worksheet.appends) == 1
    row = worksheet.appends[0][0]
    assert row[:3] == [DATE, OBSERVATION, ""]
    assert row[7] == 0
    assert len(row) == len(archive.NewsArchiveStore.HEADERS)


@pytest.mark.parametrize("values", [[], [[], [], [], [], []]])
def test_confirmed_empty_archive_can_receive_its_first_header_and_observation(monkeypatch, values):
    worksheet = FakeWorksheet(values)
    args, _ = install(monkeypatch, worksheet)

    assert asyncio.run(archive.collect(args)) == 0
    assert worksheet.header_writes == [([archive.NewsArchiveStore.HEADERS], "A5:L5")]
    assert len(worksheet.appends) == 1


@pytest.mark.parametrize("dry_run", [False, True])
def test_explicitly_successful_empty_fetch_remains_successful(monkeypatch, dry_run):
    worksheet = FakeWorksheet(grid())
    args, _ = install(monkeypatch, worksheet, items=[], dry_run=dry_run)

    assert asyncio.run(archive.collect(args)) == 0
    assert worksheet.header_writes == []
    assert worksheet.appends == []


@pytest.mark.parametrize("read_values", [
    [], None, [None],
    grid([DATE, OBSERVATION], header=list(reversed(archive.NewsArchiveStore.HEADERS))),
])
def test_idempotency_snapshot_must_still_contain_the_canonical_header(monkeypatch, read_values):
    worksheet = FakeWorksheet(grid())
    args, _ = install(monkeypatch, worksheet)
    monkeypatch.setattr(worksheet, "get_all_values", lambda: deepcopy(read_values))

    assert asyncio.run(archive.collect(args)) != 0
    assert worksheet.header_writes == []
    assert worksheet.appends == []


def test_blank_header_cannot_be_initialized_from_an_unconfirmed_empty_read(monkeypatch):
    worksheet = FakeWorksheet([])
    args, _ = install(monkeypatch, worksheet)
    monkeypatch.setattr(worksheet, "get_all_values", lambda: None)

    assert asyncio.run(archive.collect(args)) != 0
    assert worksheet.header_writes == []
    assert worksheet.appends == []


def test_unacknowledged_header_initialization_cannot_authorize_an_append(monkeypatch):
    worksheet = FakeWorksheet([])
    args, _ = install(monkeypatch, worksheet)
    # A transport returning without persistence must fail the subsequent read.
    monkeypatch.setattr(worksheet, "update", lambda **_: {})

    assert asyncio.run(archive.collect(args)) != 0
    assert worksheet.appends == []


def test_empty_batch_with_contradictory_error_receipt_fails(monkeypatch):
    worksheet = FakeWorksheet(grid())
    args, _ = install(monkeypatch, worksheet, items=[],
                      batch_errors=[{"error": "offline failure"}])

    assert asyncio.run(archive.collect(args)) != 0
    assert worksheet.header_writes == []
    assert worksheet.appends == []


def test_unusable_observation_is_not_a_legitimate_empty_fetch(monkeypatch):
    worksheet = FakeWorksheet(grid())
    args, _ = install(monkeypatch, worksheet, items=[{"symbol": ""}])

    assert asyncio.run(archive.collect(args)) != 0
    assert worksheet.header_writes == []
    assert worksheet.appends == []


def test_valid_dry_run_does_not_require_or_initialize_an_archive(monkeypatch):
    worksheet = FakeWorksheet([], header_error=True, read_error=True)
    args, _ = install(monkeypatch, worksheet, dry_run=True)

    assert asyncio.run(archive.collect(args)) == 0
    assert worksheet.header_writes == []
    assert worksheet.appends == []
