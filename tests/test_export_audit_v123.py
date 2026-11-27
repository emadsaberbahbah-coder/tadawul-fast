"""tfb_export_audit 1.2.3 - a RED that was partly the audit's own stale defaults (full project review, 27 Nov 2026).

  A1 with no --expect, a page's row count is held to the sync's own receipt in _Status (the Rows cell of its
     SUCCESS row); an explicit expectation still wins; no receipt and no expectation = no expectation
  A2 lateness is measured from the dispatcher's own accepted dispatch to the first row of the run it started;
     an export without dispatch rows is judged against the cron as before
  A3 the synthetic export and the selftest are untouched (their digests are pinned in the older test files)
"""
import datetime as dt

import pytest

pytest.importorskip("openpyxl")

from scripts import tfb_export_audit as audit

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 27, 10, 30, tzinfo=UTC)


class MemoryBook:
    def __init__(self, sheets):
        self.sheets = sheets
        self.state = {name: "visible" for name in sheets}

    def rows(self, name):
        return self.sheets.get(name)

    table = audit.Book.table


STATUS_HDR = ["Page", "Last Updated", "Status", "Message", "Endpoint", "HTTP Code", "Rows", "Columns", "Duration ms"]


def status(*rows):
    return {"_Status": [STATUS_HDR] + [list(r) for r in rows]}


# ----------------------------------------------------------------------------- A1 the sync's own receipt
def test_a1_receipts_are_read_from_the_success_rows_only():
    book = MemoryBook(status(
        ["Market_Leaders", "2026-11-27 06:27:26+03:00", "SUCCESS", "leg=success", "backend", None, 255.0, 115.0, 152020.0],
        ["Global_Markets", "2026-11-27 07:20:36+03:00", "success", "leg=success", "backend", None, 6602, 115, 3268923],
        ["Commodities_FX", "2026-11-27 06:30:58+03:00", "PARTIAL", "leg=partial", "backend", None, 453, 115, 1],
        ["Mutual_Funds", "2026-11-27 06:48:23+03:00", "SUCCESS", "leg=success", "backend", None, "2422", 115, 1],
        ["My_Portfolio", "2026-11-27 13:16:02+03:00", "SUCCESS", "leg=success", "backend", None, 7, 122, 1],
        ["Top_10_Investments", "2026-11-27 11:06:58", "OK", "Last run", "/sheet-rows", 200, 0],
    ))
    # a market page, its SUCCESS row, a whole number: nothing else is a receipt
    assert audit.status_row_receipts(book) == {"Market_Leaders": 255, "Global_Markets": 6602, "Mutual_Funds": 2422}
    for bad in (None, "", "n/a", 12.5, -1, True):
        b = MemoryBook(status(["Global_Markets", "x", "SUCCESS", "m", "e", None, bad]))
        assert audit.status_row_receipts(b) == {}, bad
    assert audit.status_row_receipts(MemoryBook(status(["Global_Markets", "x", "SUCCESS"]))) == {}     # a short row
    assert audit.status_row_receipts(MemoryBook({})) == {} and audit.status_row_receipts(MemoryBook({"_Status": []})) == {}
    assert audit.status_row_receipts(MemoryBook(status(["Global_Markets", "x", "SUCCESS", "m", "e", None, 0]))) == {
        "Global_Markets": 0}


def _rows_finding(tmp_path, expect, receipts_rows, cut=0):
    """The synthetic export, its _Status given Rows cells; -> {page: (status, count, detail)}."""
    import openpyxl
    path = tmp_path / "export.xlsx"
    syn = audit._build_synthetic(path, True)
    wb = openpyxl.load_workbook(path)
    ws = wb["_Status"]
    for row in ws.iter_rows(min_row=2):
        if row[0].value in receipts_rows:
            row[6].value = receipts_rows[row[0].value]
    if cut:
        gm = wb["Global_Markets"]
        gm.delete_rows(gm.max_row - cut + 1, cut)
    wb.save(path)
    res = audit.audit_workbook(path, expect=expect, asof="2026-10-03", now_utc=dt.datetime(2026, 10, 3, 11, tzinfo=UTC))
    got = {f["check"][:-5]: (f["status"], f["count"], f["detail"]) for f in res["findings"] if f["check"].endswith(".rows")}
    return got, syn


def test_a1_a_page_is_held_to_its_receipt_when_nothing_is_expected(tmp_path):
    got, syn = _rows_finding(tmp_path, None, {})
    assert set(got) == set(audit.MARKET_PAGES)
    assert all(v[0] == "PASS" and v[2] == "no expectation set" for v in got.values()), got    # no receipt, no --expect
    n = syn["Global_Markets"]
    # the receipt equals the page: PASS, and the detail says where the number came from
    got, _ = _rows_finding(tmp_path, None, {"Global_Markets": n, "Mutual_Funds": syn["Mutual_Funds"]})
    assert got["Global_Markets"] == ("PASS", n, "expected %d (the sync's own receipt in _Status)" % n)
    assert got["Market_Leaders"][2] == "no expectation set"                                   # no receipt for that page
    # the page is shorter than what the sync wrote: rows were lost after the write
    got, _ = _rows_finding(tmp_path, None, {"Global_Markets": n}, cut=2)
    assert got["Global_Markets"] == ("FAIL", n - 2, "expected %d (the sync's own receipt in _Status)" % n)
    # an empty dict is the CLI default (no --expect): the same as None
    got, _ = _rows_finding(tmp_path, {}, {"Global_Markets": n})
    assert got["Global_Markets"][0] == "PASS" and "receipt" in got["Global_Markets"][2]


def test_a1_an_explicit_expectation_still_wins(tmp_path):
    got, syn = _rows_finding(tmp_path, None, {})
    n = syn["Global_Markets"]
    got, _ = _rows_finding(tmp_path, {"Global_Markets": n + 7}, {"Global_Markets": n})
    assert got["Global_Markets"] == ("FAIL", n, "expected %d" % (n + 7))                      # the typed number, as before
    got, _ = _rows_finding(tmp_path, dict(syn), {"Global_Markets": n + 99})
    assert got["Global_Markets"] == ("PASS", n, "expected %d" % n)                            # the receipt is not consulted


def test_a1_cli_default_is_the_receipt_not_a_typed_universe():
    src = open(audit.__file__, encoding="utf-8").read()
    assert 'ap.add_argument("--expect", default="",' in src
    assert "expect = dict(expect or {})" in src and "dict(DEFAULT_EXPECT if expect is None" not in src
    assert audit.DEFAULT_EXPECT == {"Market_Leaders": 255, "Global_Markets": 6609, "Commodities_FX": 453,
                                    "Mutual_Funds": 2474}                                     # the example, unchanged
    assert audit.SCRIPT_VERSION == "1.2.3" and "WHY (v1.2.3, 2026-11-27)" in src


# ----------------------------------------------------------------------------- A2 the dispatcher is the clock
def T(s):
    return dt.datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=UTC)


def test_a2_lateness_from_the_dispatch_to_the_run():
    # the three dispatches of 26-27 Nov (UTC) and the runs they started
    disp = [T("2026-11-26 11:14"), T("2026-11-26 19:22"), T("2026-11-27 03:23")]
    runs = [T("2026-11-26 11:15"), T("2026-11-26 19:24"), T("2026-11-27 03:24")]
    f, worst = audit.dispatch_lateness(disp, runs, NOW)
    assert (f["status"], f["count"], worst) == ("PASS", 2, 2)
    assert f["check"] == "schedule_lateness" and f["lane"] == "PIPELINE" and f["gate"] == "e"
    assert "the dispatcher's own accepted dispatch over 3 dispatches" in f["detail"] and "cron" not in f["detail"]
    assert f["examples"][-1] == "dispatch 11-27 03:23Z started 03:24Z, +0h01m"
    # the bands of the cron check: 15 minutes PASS, to 60 WARN, beyond FAIL
    for mins, st in ((15, "PASS"), (16, "WARN"), (60, "WARN"), (61, "FAIL"), (400, "FAIL")):
        f, worst = audit.dispatch_lateness([T("2026-11-26 11:14")], [T("2026-11-26 11:14") + dt.timedelta(minutes=mins)], NOW)
        assert (f["status"], worst) == (st, mins), mins
    # a run whose first row is stamped just before the dispatch row is the run it started (the row is written after)
    f, worst = audit.dispatch_lateness([T("2026-11-26 11:14")], [T("2026-11-26 11:13")], NOW)
    assert (f["status"], worst) == ("PASS", 0)
    f, worst = audit.dispatch_lateness([T("2026-11-26 11:14")], [T("2026-11-26 11:11")], NOW)      # 3 minutes before: not its run
    assert f["status"] == "FAIL" and f["examples"] == ["dispatch 11-26 11:14Z: no run started"]
    # two dispatches a minute apart (the slot and a forced one) read the same run
    f, worst = audit.dispatch_lateness([T("2026-11-27 10:14"), T("2026-11-27 10:15")], [T("2026-11-27 10:16")], NOW)
    assert f["status"] == "PASS" and worst == 2 and "over 2 dispatches" in f["detail"]


def test_a2_a_dispatch_that_started_nothing():
    disp = [T("2026-11-26 11:14"), T("2026-11-27 03:23")]
    runs = [T("2026-11-27 03:24")]                                    # the first dispatch has no run within 12 hours
    f, worst = audit.dispatch_lateness(disp, runs, NOW)
    assert f["status"] == "FAIL" and worst == 1 and "1 dispatch(es) started no run" in f["detail"]
    assert f["examples"][0] == "dispatch 11-26 11:14Z: no run started"
    # a run 12 hours and a minute later is another slot's run, not this dispatch's
    f, _ = audit.dispatch_lateness([T("2026-11-26 11:14")], [T("2026-11-26 23:15")], NOW)
    assert f["status"] == "FAIL"
    # a dispatch sent within the last hour may not have started yet: not counted either way
    f, worst = audit.dispatch_lateness([T("2026-11-27 10:14")], [], NOW)
    assert (f["status"], worst, f["examples"]) == ("PASS", 0, []) and "over 0 dispatches" in f["detail"]
    f, _ = audit.dispatch_lateness([T("2026-11-27 09:29")], [], NOW)
    assert f["status"] == "FAIL"


RL_HDR = ["Timestamp", "Level", "Action", "Page", "Status", "Message", "Endpoint", "HTTP Code", "Duration ms", "Details JSON"]


def _runlog(extra):
    rows = [RL_HDR]
    # three sync runs, Riyadh local clock: 22:24 (26 Nov), 06:24 and 14:15 (27 Nov) -> 19:24Z, 03:24Z, 11:15Z
    for start in ("2026-11-26 22:24:10", "2026-11-27 06:24:05", "2026-11-27 14:15:30"):
        t0 = dt.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        for k in range(3):
            rows.append([t0 + dt.timedelta(minutes=20 * k), "INFO", "run_dashboard_sync", "Global_Markets", "OK",
                         "[SYNC] leg ok", "", "", "", "{}"])
    return {"_Run_Log": rows + extra}


def _late(book, cron="17 4,12,20"):
    out, metrics = audit.check_runlog(book, 3, 7, 90000, cron, dt.datetime(2026, 11, 27, 12, 0, tzinfo=UTC),
                                      dt.date(2026, 11, 27))
    f = [x for x in out if x["check"] == "schedule_lateness"]
    assert len(f) == 1
    return f[0], metrics, [x["check"] for x in out]


def test_a2_check_runlog_reads_the_dispatcher_rows_when_they_exist():
    disp = lambda ts, action="tfbDispatchDailySync", st="OK", lvl="INFO": [                    # noqa: E731
        dt.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S"), lvl, action, "daily_sync.yml", st,
        "workflow_dispatch accepted (HTTP 204) ref=main run_mode=full_sync", "https://api.github.com/x", "204.0", "5000.0", "{}"]
    # no dispatcher row: the cron check, as in 1.2.2 - and against the removed cron these runs read 7 hours late
    f, metrics, checks = _late(MemoryBook(_runlog([])))
    assert f["status"] == "FAIL" and f["count"] == 427 and "vs cron '17 4,12,20'" in f["detail"]
    assert "schedule_basis" not in metrics and metrics["schedule_worst_lateness_min"] == 427
    # the dispatcher's rows are in the log: each run started 1 to 2 minutes after its dispatch
    rows = [disp("2026-11-26 22:22:41"), disp("2026-11-27 06:23:16"), disp("2026-11-27 14:14:34")]
    f, metrics, checks2 = _late(MemoryBook(_runlog(rows)))
    assert (f["status"], f["count"]) == ("PASS", 1) and "the dispatcher is the clock" in f["detail"]
    assert metrics["schedule_basis"] == "dispatcher" and metrics["schedule_worst_lateness_min"] == 1
    assert checks2 == checks                                             # the same findings in the same order
    # a refused dispatch (status ERROR) and another lane's dispatch are not the sync's clock
    other = [disp("2026-11-26 18:47:59", action="tfbDispatchShadowScorer"),
             disp("2026-11-25 18:47:00", st="ERROR", lvl="ERROR")]
    f, metrics, _ = _late(MemoryBook(_runlog(other)))
    assert "vs cron" in f["detail"] and "schedule_basis" not in metrics
    # a forced dispatch counts (tfbDispatchDailySyncNow); one before the window does not
    f, metrics, _ = _late(MemoryBook(_runlog([disp("2026-11-27 14:15:01", action="tfbDispatchDailySyncNow"),
                                               disp("2026-11-10 06:23:00")])))
    assert f["status"] == "PASS" and "over 1 dispatches" in f["detail"]
    # the Dashboard_Audit scope finding still closes gate (e) on the dispatcher path
    book = MemoryBook({**_runlog(rows), "Dashboard_Audit": [["Page", "Check", "Status", "Count"],
                                                            ["Global_Markets", "scope.coverage", "WARN", 1500]]})
    out, _ = audit.check_runlog(book, 3, 7, 90000, "17 4,12,20", dt.datetime(2026, 11, 27, 12, 0, tzinfo=UTC), dt.date(2026, 11, 27))
    assert [x["check"] for x in out][-2:] == ["schedule_lateness", "dashboard_audit_scope_capped"] and out[-1]["status"] == "WARN"
    assert audit.DISPATCH_ACTIONS == ("tfbDispatchDailySync", "tfbDispatchDailySyncNow")


def test_a2_the_dispatcher_writes_those_action_names():
    """The action names are the dispatcher's own (apps_script/24_Sync_Dispatch.gs)."""
    from pathlib import Path
    gs = (Path(audit.__file__).resolve().parents[1] / "apps_script" / "24_Sync_Dispatch.gs").read_text(encoding="utf-8")
    for name in audit.DISPATCH_ACTIONS:
        assert "function %s(" % name in gs, name
    wf = (Path(audit.__file__).resolve().parents[1] / ".github" / "workflows" / "daily_sync.yml").read_text(encoding="utf-8")
    assert "THE DISPATCHER IS THE CLOCK (no cron here)" in wf
