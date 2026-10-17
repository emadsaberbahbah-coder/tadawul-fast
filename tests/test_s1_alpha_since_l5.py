"""L5 (2026-10-17): run_shadow_scorer v1.10.1 - TFB_S1_ALPHA_SINCE adds ONE
informational "alpha since <date>" row to the S1_Gate meta block; criteria,
Shadow_History and the verdict line are byte-identical; unset -> no row."""
from __future__ import annotations

from datetime import date
import json
import os

from scripts import run_shadow_scorer as s1
import tests.test_s1_day0_seat_pairing_t6 as h   # the P-186-style harness (real main() on a stub sheet)


def _run(day0=None, alpha_since=None):
    sheet = h.fresh_sheet(h.history_scored_15(), h.CHAL_PREV)
    saved = os.environ.get("TFB_S1_ALPHA_SINCE")
    try:
        if alpha_since is None:
            os.environ.pop("TFB_S1_ALPHA_SINCE", None)
        else:
            os.environ["TFB_S1_ALPHA_SINCE"] = alpha_since
        rc, out = h.run_at(sheet, h.P16_FULL, day0=day0)
    finally:
        if saved is None:
            os.environ.pop("TFB_S1_ALPHA_SINCE", None)
        else:
            os.environ["TFB_S1_ALPHA_SINCE"] = saved
    assert rc == 0, out
    return sheet


def test_reader():
    assert s1._alpha_since("2026-08-31") == date(2026, 8, 31)
    assert s1._alpha_since("2026-08-31T00:00:00Z") == date(2026, 8, 31)
    assert s1._alpha_since("") is None and s1._alpha_since("junk") is None and s1._alpha_since("2026-13-01") is None
    assert s1.SCRIPT_VERSION == "1.10.1"


def test_unset_is_byte_identical_and_set_adds_one_informational_row():
    base = _run()
    with_row = _run(alpha_since="2026-10-16")
    assert with_row.tabs[s1.TAB_HISTORY].rows == base.tabs[s1.TAB_HISTORY].rows
    assert h.runlog_last(with_row)[5] == h.runlog_last(base)[5]
    gate_base, gate_new = base.tabs[s1.TAB_GATE].rows, with_row.tabs[s1.TAB_GATE].rows
    assert len(gate_new) == len(gate_base) + 1
    extra = [r for r in gate_new if r and str(r[0]).startswith("alpha since 2026-10-16:")]
    assert len(extra) == 1
    row = extra[0]
    assert row[4].startswith("INFORMATIONAL ONLY") and "TFB_S1_ALPHA_SINCE" in row[4]
    # the figures are window_cum from the 10-15 rows (the last index before 10-16) to today's index
    hist = s1.read_history(base)
    today = h.hist_rows(with_row)
    for basket, cell in ((s1.CHALLENGER, row[0]), (s1.CHAMPION, row[1]), (s1.BENCHMARK, row[2])):
        exp = s1.window_cum(hist, basket, date(2026, 10, 16), float(today[basket][5]))
        assert ("%+.2f%%" % exp) in cell, (basket, cell, exp)
    alpha = s1.window_cum(hist, s1.CHALLENGER, date(2026, 10, 16), float(today[s1.CHALLENGER][5])) - \
        s1.window_cum(hist, s1.BENCHMARK, date(2026, 10, 16), float(today[s1.BENCHMARK][5]))
    assert row[3] == "net alpha %+.2f%%" % alpha
    # criteria rows untouched: everything after the meta block is identical
    crit_base = gate_base[gate_base.index(next(r for r in gate_base if r and r[0] == "#")):]
    crit_new = gate_new[gate_new.index(next(r for r in gate_new if r and r[0] == "#")):]
    assert crit_new == crit_base
    assert "alpha since" not in json.dumps(gate_base)


def test_date_before_any_history_reads_na_and_junk_adds_nothing():
    sheet = _run(alpha_since="2026-01-01")
    extra = [r for r in sheet.tabs[s1.TAB_GATE].rows if r and str(r[0]).startswith("alpha since 2026-01-01:")]
    assert len(extra) == 1 and extra[0][0].endswith("challenger n/a") and extra[0][3] == "net alpha n/a"
    assert len(_run(alpha_since="junk").tabs[s1.TAB_GATE].rows) == len(_run().tabs[s1.TAB_GATE].rows)
