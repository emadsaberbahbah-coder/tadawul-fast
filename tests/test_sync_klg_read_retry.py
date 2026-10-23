"""run_dashboard_sync v6.70.1 KLG-READ: a failed last-good read is retried once and recorded."""
import importlib
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(_REPO / "scripts"))
sync = importlib.import_module("run_dashboard_sync")

_H = ["Symbol", "Name", "Price", "Data Provider"]
_OLD = [list(_H), ["BK", "Bank of New York Mellon Corp", 137.16, "eodhd"]]


class _Sheets:
    def __init__(self, answers):
        self.answers = list(answers)
        self.calls = 0

    def read_values(self, sid, name, rng):
        self.calls += 1
        return self.answers.pop(0) if self.answers else None


def _stub():
    return [["BK", "", "", "fallback_error"]]


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    monkeypatch.setattr(sync, "_KLG_READ_RETRY_SLEEP_SEC", 0.0)
    monkeypatch.delenv("TFB_SYNC_FORCE_REFETCH_SYMBOLS", raising=False)
    sync._LAST_KLG_READ_FAILED.clear()


def test_transient_failure_recovers_on_retry():
    sh = _Sheets([None, _OLD])
    m, swapped = sync._keep_last_good_rows(sh, "sid", "Global_Markets", _H, _stub())
    assert sh.calls == 2
    assert swapped == ["BK"]
    assert m[0][1] == "Bank of New York Mellon Corp"
    assert sync._LAST_KLG_READ_FAILED == []


def test_double_failure_is_recorded_and_matrix_unchanged():
    sh = _Sheets([None, None])
    m, swapped = sync._keep_last_good_rows(sh, "sid", "Global_Markets", _H, _stub())
    assert sh.calls == 2
    assert swapped == []
    assert m == _stub()
    assert sync._LAST_KLG_READ_FAILED == ["Global_Markets"]


def test_empty_sheet_is_not_a_read_failure():
    sh = _Sheets([[]])
    sync._keep_last_good_rows(sh, "sid", "Global_Markets", _H, _stub())
    assert sh.calls == 1
    assert sync._LAST_KLG_READ_FAILED == []
