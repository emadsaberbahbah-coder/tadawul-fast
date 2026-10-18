"""Exercise the actual PIT write path without credentials or network access."""
import json
from copy import deepcopy

import pytest

from scripts import pit_snapshot as pit

TODAY = "2026-10-17"


class FakeSheet:
    def __init__(self, values=(), *, update_error=False, ignore_update=False,
                 header_read_error=False, append_error=False):
        self.values = deepcopy(list(values))
        self.update_error = update_error
        self.ignore_update = ignore_update
        self.header_read_error = header_read_error
        self.append_error = append_error
        self.updates = []
        self.appends = []
        self.header_reads = 0

    def get(self, range_name):
        if range_name == "A1:X1":
            self.header_reads += 1
            if self.header_read_error:
                raise RuntimeError("synthetic header read failure")
            return deepcopy(self.values[:1])
        return deepcopy(self.values)

    def update(self, *, values, range_name):
        self.updates.append((range_name, deepcopy(values)))
        if self.update_error:
            if isinstance(self.update_error, Exception):
                raise self.update_error
            raise RuntimeError("synthetic header update failure")
        if not self.ignore_update:
            if self.values:
                self.values[0] = list(values[0])
            else:
                self.values = deepcopy(values)
        return {"updatedRows": 1, "updatedColumns": len(values[0])}

    def append_rows(self, rows, **_kwargs):
        if self.append_error:
            raise self.append_error
        self.appends.extend(deepcopy(rows))
        self.values.extend(deepcopy(rows))
        return {"updates": {"updatedRows": len(rows)}}

    def append_row(self, row, **_kwargs):
        self.values.append(deepcopy(row))

    def freeze(self, **_kwargs):
        pass


class FakeBook:
    def __init__(self, destination):
        self.destination = destination
        self.created = 0
        self.log = FakeSheet()
        self.sheets = {
            # Exercise existing source aliases and honest absent-source rows.
            "Source": FakeSheet([
                ["Symbol", "Last Price", "Last Updated"],
                ["SYNTH.US", "1", "2026-10-16 22:00:00"],
            ]),
            "Decision": FakeSheet([["Symbol"], ["SYNTH.US"], ["GHOST.XX"]]),
            "_Run_Log": self.log,
        }

    def worksheet(self, name):
        if name == "_PIT_Fundamentals":
            if self.destination is None:
                raise KeyError("missing destination")
            return self.destination
        return self.sheets[name]

    def add_worksheet(self, *, title, rows, cols):
        assert (title, rows, cols) == ("_PIT_Fundamentals", 2000, 24)
        assert self.destination is None
        self.created += 1
        self.destination = FakeSheet()
        return self.destination


def configure(monkeypatch, destination):
    book = FakeBook(destination)
    monkeypatch.setattr(pit, "_open_book", lambda: book)
    monkeypatch.setattr(pit, "_today_riyadh", lambda: TODAY)
    monkeypatch.setenv("TFB_PIT_SOURCE_PAGES", "Source")
    monkeypatch.setenv("TFB_PIT_SYMBOL_PAGES", "Decision")
    monkeypatch.setenv("TFB_PIT_SHEET", "_PIT_Fundamentals")
    return book


def assert_header_failure(book):
    assert not book.destination.appends
    assert book.log.values[-1][4] == "FAIL"
    assert json.loads(book.log.values[-1][-1]) == {"stage": "header"}
    assert all(row[4] != "OK" for row in book.log.values)


def test_failed_legacy_migration_never_appends_new_schema_rows(monkeypatch):
    historical = ["2026-10-16", "SYNTH.US"] + [""] * 14
    sheet = FakeSheet([pit.LEGACY_HEADERS_16, historical], update_error=True)
    before = deepcopy(sheet.values)
    book = configure(monkeypatch, sheet)

    assert pit.main(["--write"]) == 3

    assert sheet.values == before
    assert len(sheet.updates) == 1
    assert_header_failure(book)


@pytest.mark.parametrize("failure", ["unchanged_header", "read_failure"])
def test_unconfirmed_migration_never_appends(monkeypatch, failure):
    sheet = FakeSheet([pit.LEGACY_HEADERS_16],
                      ignore_update=failure == "unchanged_header",
                      header_read_error=failure == "read_failure")
    book = configure(monkeypatch, sheet)

    assert pit.main(["--write"]) == 3

    assert sheet.header_reads == 1
    assert_header_failure(book)


def test_legacy_migration_preserves_history_and_confirms_new_header(monkeypatch):
    historical = ["2026-10-16", "SYNTH.US"] + [""] * 12 + ["old-row-stamp", "old-capture"]
    sheet = FakeSheet([pit.LEGACY_HEADERS_16, historical])
    book = configure(monkeypatch, sheet)

    assert pit.main(["--write"]) == 0

    assert sheet.values[0] == pit.HEADERS
    assert sheet.values[1] == historical
    assert sheet.header_reads == 1
    assert len(sheet.appends) == 2
    assert all(len(row) == 24 and row[0] == TODAY for row in sheet.appends)
    assert sheet.appends[0][22] == "2026-10-16 22:00:00"
    assert sheet.appends[1][2] == "-"
    assert json.loads(book.log.values[-1][-1]) == {"written": 2, "skipped": 0, "missing": 1}


@pytest.mark.parametrize("header", [pit.HEADERS, [" " + h + " " for h in pit.HEADERS]])
def test_current_schema_appends_without_rewriting_header(monkeypatch, header):
    sheet = FakeSheet([header])
    book = configure(monkeypatch, sheet)

    assert pit.main(["--write"]) == 0

    assert not sheet.updates
    assert sheet.header_reads == 0
    assert sheet.values[0] == header
    assert len(sheet.appends) == 2
    assert book.log.values[-1][4] == "OK"


@pytest.mark.parametrize("header", [
    [],
    list(reversed(pit.HEADERS)),
    pit.HEADERS[:1] + ["Unknown"] + pit.HEADERS[2:],
    pit.HEADERS + ["Extra"],
    pit.LEGACY_HEADERS_16 + ["Extra"],
    pit.HEADERS[:-1],
])
def test_incompatible_header_is_not_overwritten_or_appended(monkeypatch, header):
    sheet = FakeSheet([header, ["historical marker"]])
    before = deepcopy(sheet.values)
    book = configure(monkeypatch, sheet)

    assert pit.main(["--write"]) == 3

    assert sheet.values == before
    assert not sheet.updates
    assert_header_failure(book)


def test_current_schema_preserves_same_day_duplicate_guard(monkeypatch):
    existing = [[TODAY, "SYNTH.US"], [TODAY, "GHOST.XX"]]
    sheet = FakeSheet([pit.HEADERS] + existing)
    before = deepcopy(sheet.values)
    book = configure(monkeypatch, sheet)

    assert pit.main(["--write"]) == 0

    assert sheet.values == before
    assert not sheet.appends
    assert json.loads(book.log.values[-1][-1]) == {"written": 0, "skipped": 2, "missing": 0}


def test_new_destination_header_is_verified_before_append(monkeypatch):
    book = configure(monkeypatch, None)

    assert pit.main(["--write"]) == 0

    assert book.created == 1
    assert book.destination.values[0] == pit.HEADERS
    assert book.destination.header_reads == 1
    assert len(book.destination.appends) == 2


def test_existing_empty_destination_is_initialized_and_verified(monkeypatch):
    sheet = FakeSheet([])
    book = configure(monkeypatch, sheet)

    assert pit.main(["--write"]) == 0

    assert book.created == 0
    assert len(sheet.updates) == 1
    assert sheet.values[0] == pit.HEADERS
    assert sheet.header_reads == 1
    assert len(sheet.appends) == 2


@pytest.mark.parametrize("stage", ["header", "append"])
def test_write_failure_reports_only_exception_class(monkeypatch, capsys, stage):
    class SyntheticSheetError(RuntimeError):
        pass

    private_message = "PRIVATE_ERROR_PAYLOAD {sensitive: synthetic-only}"
    failure = SyntheticSheetError(private_message)
    sheet = (FakeSheet([pit.LEGACY_HEADERS_16], update_error=failure)
             if stage == "header" else FakeSheet([pit.HEADERS], append_error=failure))
    book = configure(monkeypatch, sheet)

    assert pit.main(["--write"]) == 3

    stderr = capsys.readouterr().err
    assert "SyntheticSheetError" in stderr
    assert private_message not in stderr
    assert private_message not in json.dumps(book.log.values)
    assert book.log.values[-1][5] == f"[PIT-SNAPSHOT v{pit.__version__}] FAIL {stage}: SyntheticSheetError"
    assert json.loads(book.log.values[-1][-1]) == {"stage": stage}
    assert not sheet.appends
