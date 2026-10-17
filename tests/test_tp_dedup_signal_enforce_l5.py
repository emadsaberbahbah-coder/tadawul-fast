"""L5 (2026-10-17): track_performance v6.47.0 - TRACK_DEDUP_SIGNAL_HISTORY=enforce
de-duplicates Signal_History AT WRITE TIME (last-of-day upsert of TODAY's row
per symbol; new keys append; prior days never rewritten) and fills the
publication identity on new snapshots from TRACK_PUBLICATION_ID.

Reuses the P-191 fixture (tests/test_tp_dedup_signal_p191.py: 7 rows - AAA x3
and BBB x1 today, CCC x2 yesterday, one blank-key row). Offline: fake
worksheet, no Sheets, no network.
"""
from __future__ import annotations

import asyncio
import copy
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_tp_dedup_signal_p191 as p191  # noqa: E402  (runs its battery at import; it must be green)

tp = p191.tp
H = p191.H


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for k in p191.ENVS:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.delenv("TRACK_PUBLICATION_ID", raising=False)
    monkeypatch.delenv("TRACK_SIGNAL_PUBLICATION_MODE", raising=False)
    p191._reset_rb(tp)
    yield
    p191._reset_rb(tp)


def _run(rows, mode, fail_get=False, publication_id=None, break_update=False):
    if publication_id is not None:
        os.environ["TRACK_PUBLICATION_ID"] = publication_id
    else:
        os.environ.pop("TRACK_PUBLICATION_ID", None)
    st = p191._store(tp, copy.deepcopy(rows), fail_get=fail_get)
    if break_update:
        def _boom(values=None, range_name=None):
            raise RuntimeError("simulated APIError on update")
        st.ws.update = _boom
    os.environ["TRACK_DEDUP_SIGNAL_HISTORY"] = mode
    app = p191._app(tp, st)
    written = asyncio.run(app.record_signal_snapshots(p191._rows_dicts()))
    return written, st.ws, copy.deepcopy(tp._LAST_DEDUP_SIGNAL)


def test_version_and_contract_text():
    assert tp.SCRIPT_VERSION == "6.48.0"
    src = open(tp.__file__, encoding="utf-8").read()
    assert "# v6.47.0 (2026-10-17) - L5 SIGNAL_HISTORY DEDUP ENFORCE + PUBLICATION IDENTITY" in src
    assert "enforce requested but NOT built" not in src


def test_load_key_rows_keeps_sheet_positions_across_blank_rows():
    st = p191._store(tp, copy.deepcopy(p191.FIX))
    rows = st.load_key_rows()
    assert [r for r, _ in rows] == [2, 3, 4, 5, 6, 7]          # the blank-key row 8 is skipped, positions kept
    assert [k.split("|")[0] for _, k in rows] == ["AAA.US", "AAA.US", "AAA.US", "BBB.US", "CCC.US", "CCC.US"]
    assert st.load_key_column() == [k for _, k in rows]
    assert st.ws.gets == ["B2:B19", "B2:B19"]                  # clamped to the grid, as before
    bad = p191._store(tp, copy.deepcopy(p191.FIX), fail_get=True)
    assert bad.load_key_rows() is None and bad.load_key_column() is None
    empty = object.__new__(tp.SignalHistoryStore); empty.ws = None
    assert empty.load_key_rows() == [] and empty.load_key_column() == []


def test_enforce_updates_todays_last_row_and_appends_new_keys_only():
    written, ws, rb = _run(p191.FIX, "enforce")
    assert written == 1
    assert [r[H.index("Symbol")] for r in ws.appended] == ["DDD.US"]
    assert sorted(r for r, _ in ws.updates) == ["A4:U4", "A5:U5"]       # AAA's LAST row (of 3), BBB's row
    by_range = {r: rows[0] for r, rows in ws.updates}
    assert by_range["A4:U4"][H.index("Symbol")] == "AAA.US" and by_range["A5:U5"][H.index("Symbol")] == "BBB.US"
    assert by_range["A4:U4"][H.index("Key")] == "AAA.US|" + p191.T8
    # yesterday's CCC rows (6, 7) are never written
    assert not any(r.startswith(("A6:", "A7:")) for r, _ in ws.updates)
    assert rb["applied"] is True and rb["policy"] == "last-of-day" and rb["updated"] == 2 and rb["appended"] == 1
    assert rb["would_skip"] == 2 and rb["new"] == 1 and rb["sheet_rows"] == 6 and rb["backlog"] == 3


def test_a_later_run_overwrites_todays_row_last_of_day():
    """Within one build the builder keeps the FIRST snapshot per key (its own
    in-batch dedup); "latest" is an across-runs property: a later run of the
    same Riyadh day overwrites today's row in place."""
    st = p191._store(tp, copy.deepcopy(p191.FIX))
    app = p191._app(tp, st)
    rows = p191._rows_dicts()
    rows.append(dict(rows[0], overall_score=99))      # same key twice in one batch -> builder keeps the first (60)
    os.environ["TRACK_DEDUP_SIGNAL_HISTORY"] = "enforce"
    asyncio.run(app.record_signal_snapshots(rows))
    aaa = [r for rng, (r,) in st.ws.updates if rng == "A4:U4"]
    assert len(aaa) == 1 and str(aaa[0][H.index("Overall Score")]) in ("60", "60.0")
    # a LATER run (score 99) overwrites the same row - last-of-day
    st2 = p191._store(tp, copy.deepcopy(p191.FIX))
    later = [dict(r, overall_score=99) for r in p191._rows_dicts()]
    asyncio.run(p191._app(tp, st2).record_signal_snapshots(later))
    aaa2 = [r for rng, (r,) in st2.ws.updates if rng == "A4:U4"]
    assert len(aaa2) == 1 and str(aaa2[0][H.index("Overall Score")]) in ("99", "99.0")
    assert tp._LAST_DEDUP_SIGNAL["updated"] == 2 and tp._LAST_DEDUP_SIGNAL["appended"] == 1
    # the pure upsert itself keeps the LAST snapshot when handed duplicates directly
    st3 = p191._store(tp, copy.deepcopy(p191.FIX))
    snaps = app._build_signal_snapshots(p191._rows_dicts())
    dup = copy.copy(snaps[0]); dup.overall_score = 77.0
    res = st3.upsert_snapshots(snaps + [dup], st3.load_key_rows())
    assert res == {"updated": 2, "appended": 1}
    assert str([r for rng, (r,) in st3.ws.updates if rng == "A4:U4"][0][H.index("Overall Score")]) in ("77", "77.0")


def test_publication_identity_is_filled_on_new_snapshots_under_enforce_only():
    written, ws, _ = _run(p191.FIX, "enforce", publication_id="39018080003")
    k = H.index("Publication Key"); i = H.index("Publication ID"); s = H.index("Publication Schema")
    row = ws.appended[0]
    assert row[i] == "39018080003" and row[s] == "tfb-signal-publication/v1"
    assert row[k] == tp._publication_key("DDD.US", "", "39018080003") and row[k].startswith("tfb-signal-publication/v1:")
    for _, (urow,) in ws.updates:
        assert urow[i] == "39018080003" and urow[k].startswith("tfb-signal-publication/v1:")
    # observe never fills identity, and without an id enforce leaves it blank (never guessed)
    _, ws_obs, _ = _run(p191.FIX, "observe", publication_id="39018080003")
    assert all(r[k] == "" and r[i] == "" for r in ws_obs.appended) and ws_obs.updates == []
    _, ws_noid, _ = _run(p191.FIX, "enforce")
    assert ws_noid.appended[0][k] == "" and all(u[0][k] == "" for _, u in ws_noid.updates)


def test_read_failure_falls_back_to_the_plain_append_with_the_read_failed_token():
    written, ws, rb = _run(p191.FIX, "enforce", fail_get=True)
    assert written == 3 and len(ws.appended) == 3 and ws.updates == []
    assert rb["applied"] is False and rb["read_ok"] is False
    assert tp._dedup_signal_token(rb) == " | dedup_signal enforce read-failed"


def test_apply_failure_falls_back_to_the_plain_append_and_reports_not_applied():
    written, ws, rb = _run(p191.FIX, "enforce", break_update=True)
    assert written == 3 and len(ws.appended) == 3
    assert rb["applied"] is False and rb["read_ok"] is True
    assert tp._dedup_signal_token(rb) == " | dedup_signal enforce would_skip 2/3 backlog 3"


def test_immutable_publication_mode_keeps_its_own_path(monkeypatch):
    st = p191._store(tp, copy.deepcopy(p191.FIX))
    monkeypatch.setenv("TRACK_SIGNAL_PUBLICATION_MODE", "immutable")
    assert st.upsert_snapshots([], [(2, "AAA.US|x")]) is None
    assert st.upsert_snapshots([], None) is None


def test_observe_and_off_are_unchanged():
    w_off, ws_off, rb_off = _run(p191.FIX, "off")
    assert w_off == 3 and ws_off.gets == [] and ws_off.updates == [] and rb_off == p191.DEFAULT_RB
    w_obs, ws_obs, rb_obs = _run(p191.FIX, "observe")
    assert w_obs == 3 and ws_obs.gets == ["B2:B19"] and ws_obs.updates == [] and rb_obs["applied"] is False
    assert "updated" not in rb_obs and "policy" not in rb_obs


def test_tracker_receipt_reports_the_apply(tmp_path):
    from scripts import workflow_activation_receipt as receipt
    help_path, log = tmp_path / "help.txt", tmp_path / "log.txt"
    help_path.write_text("Performance Tracker v%s" % tp.SCRIPT_VERSION)
    base_env = {"GITHUB_SHA": "0" * 40, "TFB_CHECKOUT_SHA": "0" * 40, "GITHUB_RUN_ID": "1", "GITHUB_RUN_ATTEMPT": "1",
                "GITHUB_EVENT_NAME": "schedule"}
    measure = "[v6.44.0 DEDUP-SIGNAL] mode=enforce would_skip=2/3 new=1 | sheet rows=6 keys=3 backlog=3 max_per_key=3 | sample=x\n"
    log.write_text(measure + "[v6.47.0 DEDUP-SIGNAL] enforce applied last-of-day: updated=2 appended=1 (built=3; prior days untouched; publication identity filled)\n"
                   "record_signal_snapshots: wrote 1 new daily snapshot(s)\n")
    r = receipt.tracker_receipt(0, log, help_path, True, root=Path(tp.__file__).resolve().parents[1],
                                env=base_env | {"TRACK_DEDUP_SIGNAL_HISTORY": "enforce"})
    assert r["same_day_deduplication_applied"] is True and r["signal_history_measurement_only"] is False
    assert r["signal_history_apply"] == {"policy": "last-of-day", "updated": 2, "appended": 1}
    assert r["signal_snapshot_append_acknowledged"] == 1
    log.write_text(measure.replace("enforce", "observe") + "record_signal_snapshots: wrote 3 new daily snapshot(s)\n")
    r2 = receipt.tracker_receipt(0, log, help_path, False, root=Path(tp.__file__).resolve().parents[1],
                                 env=base_env | {"TRACK_DEDUP_SIGNAL_HISTORY": "observe"})
    assert r2["same_day_deduplication_applied"] is False and r2["signal_history_measurement_only"] is True
    assert r2["signal_history_apply"] is None
