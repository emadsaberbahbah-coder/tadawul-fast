"""PIT repair predicates and sheet writes are exercised only on synthetic grids."""
import copy
import json
import subprocess
import sys

import pytest

from scripts import pit_snapshot, quarantine_placeholder_records
from scripts import repair_pit_legacy_rows as repair


def legacy(date="2026-07-23", symbol="SYNTH.US", page="Global_Markets"):
    return [date, symbol, page, "12.25", "1000000", "12", "1", "1.02", "2", "20",
            "15", "70", "HOLD", "synthetic-provider", "2026-07-23T08:59:00Z", "2026-07-23 09:00:00"]


def clean(date="2026-07-29"):
    row = legacy(date=date)[:14] + ["provider_target", "80", "90", "70", "20", "1", "3", "ELIGIBLE",
                                  "2026-07-29T08:59:00Z", "2026-07-29 09:00:00"]
    assert len(row) == len(pit_snapshot.HEADERS)
    return row


class FakeWorksheet:
    def __init__(self, values):
        self.values = copy.deepcopy(values)
        self.calls = []

    def get(self, range_name):
        self.calls.append(("get", range_name))
        return copy.deepcopy(self.values)

    def batch_update(self, updates, *, value_input_option):
        self.calls.append(("batch_update", copy.deepcopy(updates), value_input_option))
        for update in updates:
            sheet_row = int(update["range"].split(":")[0][1:])
            self.values[sheet_row - 1] += [""] * max(0, 24 - len(self.values[sheet_row - 1]))
            self.values[sheet_row - 1][14:24] = update["values"][0]

    def delete_rows(self, index):
        self.calls.append(("delete_rows", index))
        del self.values[index - 1]


class FakeBook:
    def __init__(self, values):
        self.pit = FakeWorksheet(values)
        self.logs = []

    def worksheet(self, tab):
        if tab == repair.PIT_TAB:
            return self.pit
        assert tab == "_Run_Log"
        return self

    def append_row(self, values, *, value_input_option):
        self.logs.append((values, value_input_option))


def test_original_header_collision_is_the_timestamp_misalignment():
    assert pit_snapshot.HEADERS.index("Forecast Source") == pit_snapshot.LEGACY_HEADERS_16.index("Row Last Updated") == 14
    assert pit_snapshot.HEADERS.index("Forecast Reliability") == pit_snapshot.LEGACY_HEADERS_16.index("Captured At (UTC)") == 15
    assert pit_snapshot.HEADERS.index("Row Last Updated") == 22
    assert pit_snapshot.HEADERS.index("Captured At (UTC)") == 23


def test_exact_planner_preserves_original_values_and_leaves_diagnostics_uncaptured():
    values = [pit_snapshot.HEADERS, clean(), legacy(), legacy("2026-07-22", "FORECAST", "-"),
              legacy("2026-07-22", "UNKNOWN_ARTIFACT", "-"), [""]]
    before = copy.deepcopy(values)
    realign, delete, anomalies = repair.build_plan(values)
    assert values == before and not anomalies
    assert [row["sheet_row"] for row in realign] == [3, 4, 5]
    assert [row["sheet_row"] for row in delete] == [4]
    for item in realign:
        assert len(item["after"]) == 24
        assert item["after"][:14] == item["before"][:14]
        assert item["after"][14:22] == [""] * 8
        assert item["after"][22:24] == item["before"][14:16]


@pytest.mark.parametrize("header", [pit_snapshot.LEGACY_HEADERS_16,
                                     list(reversed(pit_snapshot.HEADERS)), pit_snapshot.HEADERS + [""],
                                     [" " + name for name in pit_snapshot.HEADERS], []])
def test_header_must_match_current_exact_column_contract(header):
    realign, delete, anomalies = repair.build_plan([header, legacy()])
    assert not realign and not delete
    assert anomalies == [{"sheet_row": 1, "reason": "header_mismatch"}]


@pytest.mark.parametrize("date", ["2026-07-21", "2026-07-29", "2026-07-22 extra", "2026-07-22T09:00:00Z"])
def test_legacy_realignment_never_matches_outside_exact_snapshot_dates(date):
    realign, _, anomalies = repair.build_plan([pit_snapshot.HEADERS, legacy(date=date)])
    assert not realign and anomalies[0]["reason"] == "legacy_predicate_mismatch"


@pytest.mark.parametrize("capture", ["", "2026-07-23", "2026-07-23 09:00", "2026-02-30 09:00:00", "not-a-timestamp"])
def test_invalid_original_capture_timestamp_is_never_moved(capture):
    row = legacy()
    row[15] = capture
    realign, _, anomalies = repair.build_plan([pit_snapshot.HEADERS, row])
    assert not realign and anomalies


@pytest.mark.parametrize("extra_column", [16, 17, 22, 23, 24, 25])
def test_populated_tail_prevents_realignment(extra_column):
    row = legacy() + [""] * 10
    row[extra_column] = "existing-evidence"
    realign, _, anomalies = repair.build_plan([pit_snapshot.HEADERS, row])
    assert not realign and anomalies


def test_trailing_blank_padding_does_not_hide_true_16_wide_legacy_row():
    realign, _, anomalies = repair.build_plan([pit_snapshot.HEADERS, legacy() + [None, "", " "] + [""] * 7])
    assert len(realign) == 1 and not anomalies


def test_delete_matches_only_eleven_explicit_date_symbol_source_keys():
    values = [pit_snapshot.HEADERS]
    for date, symbol in sorted(repair.ARTIFACT_KEYS):
        values.append(legacy(date, symbol, "-"))
    values += [legacy("2026-07-23", "FORECAST", "-"), legacy("2026-07-22", "UNKNOWN", "-")]
    _, delete, anomalies = repair.build_plan(values)
    assert len(delete) == 11 and not anomalies
    assert {(item["before"][0], item["before"][1]) for item in delete} == repair.ARTIFACT_KEYS


def test_source_drift_and_duplicate_artifact_keys_require_operator_review():
    values = [pit_snapshot.HEADERS, legacy("2026-07-22", "FORECAST", "Global_Markets"),
              legacy("2026-07-22", "COUNT", "-"), legacy("2026-07-22", "COUNT", "-")]
    _, delete, anomalies = repair.build_plan(values)
    assert len(delete) == 1
    assert {item["reason"] for item in anomalies} == {"artifact_source_drift", "duplicate_artifact_key"}


def test_exact_artifact_deletion_does_not_require_legacy_timestamp_evidence():
    """Approved artifact predicates are independent of realignment predicates."""
    values = [pit_snapshot.HEADERS, ["2026-07-22", "FORECAST", "-"],
              ["2026-07-22", "UNKNOWN", "-"]]
    realign, delete, anomalies = repair.build_plan(values)
    assert not realign and [item["sheet_row"] for item in delete] == [2]
    assert anomalies == [{"sheet_row": 3, "reason": "legacy_predicate_mismatch"}]


def test_apply_moves_original_timestamps_before_bottom_up_deletion_and_is_idempotent():
    values = [pit_snapshot.HEADERS, legacy("2026-07-22", "FORECAST", "-"), clean(), legacy(),
              legacy("2026-07-22", "COUNT", "-")]
    book = FakeBook(values)
    plan = repair.build_plan(values)
    repair.apply_plan(book, book.pit, values, *plan)
    writes = [call for call in book.pit.calls if call[0] != "get"]
    assert writes[0][0] == "batch_update" and writes[0][2] == "RAW"
    assert [call for call in writes if call[0] == "delete_rows"] == [("delete_rows", 5), ("delete_rows", 2)]
    assert book.pit.values[1] == clean()
    assert book.pit.values[2][:14] == legacy()[:14]
    assert book.pit.values[2][22:24] == legacy()[14:16]
    assert repair.build_plan(book.pit.values) == ([], [], [])
    assert len(book.logs) == 1 and book.logs[0][1] == "RAW"
    assert "[PIT-REPAIR v1.0.0] realigned=3 deleted=2 mode=APPLY anomalies=0" in book.logs[0][0]


@pytest.mark.parametrize("guard", ["realign_ceiling", "delete_ceiling", "anomaly", "drift", "forged_plan"])
def test_apply_refuses_every_unsafe_guard_before_writes(guard, monkeypatch):
    values = [pit_snapshot.HEADERS, legacy(), legacy("2026-07-22", "FORECAST", "-")]
    book = FakeBook(values)
    realign, delete, anomalies = repair.build_plan(values)
    if guard == "realign_ceiling":
        monkeypatch.setattr(repair, "MAX_REALIGNS", 1)
    elif guard == "delete_ceiling":
        monkeypatch.setattr(repair, "MAX_DELETES", 0)
    elif guard == "anomaly":
        anomalies.append({"sheet_row": 1, "reason": "header_mismatch"})
    elif guard == "drift":
        book.pit.values[1][3] = "changed quote"
    else:
        realign[0]["after"][23] = "invented-capture"
    with pytest.raises(ValueError):
        repair.apply_plan(book, book.pit, values, realign, delete, anomalies)
    assert not [call for call in book.pit.calls if call[0] != "get"] and not book.logs


def test_more_than_78_realignment_matches_refuses_apply(monkeypatch, capsys):
    book = FakeBook([pit_snapshot.HEADERS] + [legacy(symbol=f"SYNTH{index}.US") for index in range(79)])
    monkeypatch.setattr(quarantine_placeholder_records, "_open_sheet", lambda _: book)
    assert repair.main(["--apply"]) == 3
    assert "realigned=79" in capsys.readouterr().out
    assert not [call for call in book.pit.calls if call[0] != "get"] and not book.logs


@pytest.mark.parametrize("args", [[], ["--scan"]])
def test_default_and_explicit_scan_are_read_only_including_run_log(args, monkeypatch, capsys):
    book = FakeBook([pit_snapshot.HEADERS, legacy()])
    monkeypatch.setattr(quarantine_placeholder_records, "_open_sheet", lambda _: book)
    assert repair.main(args) == 0
    assert "realigned=1 deleted=0 mode=DRY anomalies=0" in capsys.readouterr().out
    assert [call[0] for call in book.pit.calls] == ["get"] and not book.logs


def test_cli_supports_offline_grid_scan_and_refuses_offline_apply(tmp_path):
    source = tmp_path / "synthetic-grid.json"
    source.write_text(json.dumps([pit_snapshot.HEADERS, legacy()]))
    command = [sys.executable, repair.__file__, "--input-json", str(source)]
    result = subprocess.run(command + ["--scan"], capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 0 and "realigned=1 deleted=0 mode=DRY anomalies=0" in result.stdout
    result = subprocess.run(command + ["--apply"], capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 2
    assert json.loads(source.read_text()) == [pit_snapshot.HEADERS, legacy()]
