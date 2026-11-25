"""pit_legacy_repair.yml 1.0.0 + backup_sheet_tab.py 1.0.0 (external audit, 25 Nov).

The repair planner already has its own tests (repair_pit_legacy_rows.py). These
pin the runner around it: dispatch only, scan by default, a backup tab before
any apply, and a backup that is never overwritten.
"""
import datetime as dt
from pathlib import Path

import pytest
import yaml

from scripts import backup_sheet_tab as bk

ROOT = Path(__file__).resolve().parents[1]
WF = ROOT / ".github" / "workflows" / "pit_legacy_repair.yml"


class _WS:
    def __init__(self, title, rows, wid):
        self.title, self._rows, self.id, self.hidden = title, rows, wid, False

    @property
    def row_count(self):
        return len(self._rows)

    def get_all_values(self):
        return [list(r) for r in self._rows]

    def hide(self):
        self.hidden = True


class _Book:
    def __init__(self, tabs):
        self._tabs = [_WS(t, rows, i) for i, (t, rows) in enumerate(tabs)]
        self.dup_calls = 0

    def worksheets(self):
        return list(self._tabs)

    def worksheet(self, name):
        return next(ws for ws in self._tabs if ws.title == name)

    def duplicate_sheet(self, source_id, new_sheet_name, insert_sheet_index):
        self.dup_calls += 1
        src = next(ws for ws in self._tabs if ws.id == source_id)
        copy = _WS(new_sheet_name, src.get_all_values(), 100 + self.dup_calls)
        self._tabs.insert(insert_sheet_index, copy)
        return copy


NOW = dt.datetime(2026, 11, 25, 22, 30, tzinfo=dt.timezone.utc)   # 26 Nov 01:30 Riyadh


def test_backup_is_hidden_dated_in_riyadh_and_complete():
    book = _Book([("_PIT_Fundamentals", [["h"], ["a"], ["b"]]), ("_Run_Log", [["x"]])])
    name, status, rows = bk.backup_tab(book, "_PIT_Fundamentals", "_PIT_Backup_", NOW)
    assert (name, status, rows) == ("_PIT_Backup_20261126", "created", 3)
    assert book.worksheet(name).hidden and book.worksheet(name).get_all_values() == [["h"], ["a"], ["b"]]


def test_existing_backup_is_never_overwritten():
    book = _Book([("_PIT_Fundamentals", [["h"], ["new"]]), ("_PIT_Backup_20261126", [["h"], ["old"], ["old2"]])])
    name, status, rows = bk.backup_tab(book, "_PIT_Fundamentals", "_PIT_Backup_", NOW)
    assert (status, rows) == ("exists", 3) and book.dup_calls == 0
    assert book.worksheet(name).get_all_values() == [["h"], ["old"], ["old2"]]


def test_prefix_equal_to_tab_is_refused():
    with pytest.raises(ValueError):
        bk.backup_tab(_Book([("T", [["h"]])]), "T", "T", NOW)


def test_workflow_is_dispatch_only_scans_by_default_and_backs_up_before_apply():
    wf = yaml.safe_load(WF.read_text(encoding="utf-8"))
    on = wf.get("on", wf.get(True))
    assert set(on) == {"workflow_dispatch"}
    assert on["workflow_dispatch"]["inputs"]["mode"]["default"] == "scan"
    steps = wf["jobs"]["pit_repair"]["steps"]
    names = [s["name"] for s in steps]
    scan, backup, repair = (names.index(n) for n in (
        "🔎 Scan (read-only)", "💾 Back up _PIT_Fundamentals (apply only)", "🧹 Repair (apply only)"))
    assert scan < backup < repair
    assert "--scan" in steps[scan]["run"] and "if" not in steps[scan]
    assert steps[backup]["if"] == steps[repair]["if"] == "${{ inputs.mode == 'apply' }}"
    assert "--apply" in steps[repair]["run"] and "--apply" not in "".join(
        s.get("run", "") for i, s in enumerate(steps) if i != repair)
    assert wf["permissions"] == {"contents": "read"}


# --- tfb_export_audit 1.2.2: a tighter sector cap is a WARN, a looser one a FAIL (same audit, P2) ---
@pytest.mark.parametrize("t10,pf,status", [(2, 30, "WARN"), (3, 40, "PASS"), (4, 40, "FAIL"), (2, 45, "FAIL"),
                                           (None, None, "PASS"), (3, 35, "WARN")])
def test_sector_cap_tighter_is_warn_looser_is_fail(t10, pf, status):
    pytest.importorskip("openpyxl")             # the export audit imports it at module level (lean CI has none)
    from scripts import tfb_export_audit as ea
    assert ea.SCRIPT_VERSION == "1.2.2"
    got, examples = ea.sector_cap_status(t10, pf, 3, 40.0)
    assert got == status and len(examples) == (t10 not in (None, 3)) + (pf not in (None, 40))
