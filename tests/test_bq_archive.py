"""scripts/archive_to_bigquery.py v1.1.0 - the read-only daily BigQuery
archive of the decision workbook (program register C8, Phase 1).

REAL module; the only stub is the HTTP session (an AuthorizedSession-like
fake that answers the Sheets values.get, BigQuery datasets/tables/jobs and
the resumable upload endpoints offline). No network, no google libraries.

A1 gate off / missing configuration: no request at all, exit 0 / 2
A2 dry run: every tab read, the dataset checked, nothing created or loaded
A3 write: a missing table is created DAY-partitioned on snapshot_date with
   the fixed schema; each tab loads into table$YYYYMMDD with WRITE_TRUNCATE
   and CREATE_NEVER; the uploaded NDJSON holds the expected rows; the job is
   polled until DONE; a re-run the same day targets the same partition
A4 failures: a tab the Sheets API rejects, a job errorResult, a missing
   dataset - the other tabs are still archived, exit 1 (2 for the dataset)
A5 pure helpers: table names, cell text, header names, the row builder (empty
   rows skipped, record_json, caps), the tab selection, the date option
A6 the workflow: schedule, arming, credentials, dry-run dispatch default
A7 v1.1.0: the four market pages and the three news gate tabs are archived
   with header row 1, every archive table is a source apply_bq_views knows,
   and a 115-column market row keeps every cell and its record_json
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from urllib.parse import unquote

import pytest

from scripts import archive_to_bigquery as arc

ROOT = Path(__file__).resolve().parents[1]
NOW = dt.datetime(2026, 10, 27, 23, 5, 7, 123456, tzinfo=dt.timezone.utc)
PROJECT, DATASET, SID = "proj-x", "tfb_archive", "SHEET123"

SHEETS = {
    "Top_10_Investments": [["TOP 10 INVESTMENTS - DECISION"], [], ["Status:", "OK | earn 1/9"],
                           ["Rank", "Symbol", "Advisor Note"], [1, "FRST.US", "INVEST"]],
    "_Selection_Log": [["Logged At", "Symbol", "Rank", "Engine ROI %"],
                       ["2026-10-27 10:48:41", "FRST.US", 1, 18.25],
                       ["", "", "", ""],
                       ["2026-10-27 10:48:41", "OPBK.US", 2, 19.0]],
    "Calendar_Events": [["Symbol", "Next Earnings Date", "Days To Earnings", "", "Last Earnings Date",
                         "EPS Surprise %", "Symbol"],
                        ["FRST.US", "2027-01-28", 93, "x", "2026-10-22", -22.2, "dup"]],
}


class Resp:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code = status
        self._body = body if body is not None else {}
        self.headers = headers or {}
        self.text = json.dumps(self._body)

    def json(self):
        return self._body


class FakeSession:
    """Offline Sheets + BigQuery. `tables` = existing table ids."""

    def __init__(self, sheets=None, tables=(), dataset=True, job_error=None, bad_tabs=(),
                 polls_before_done=1):
        self.sheets = dict(SHEETS if sheets is None else sheets)
        self.tables = set(tables)
        self.dataset = dataset
        self.job_error = job_error or {}
        self.bad_tabs = set(bad_tabs)
        self.polls_before_done = polls_before_done
        self.calls = []
        self.created = []
        self.jobs = {}
        self.uploads = {}

    def get(self, url, timeout=None, **_):
        self.calls.append(("GET", url))
        if url.startswith(arc.SHEETS_API):
            tab = unquote(url.split("/values/")[1].split("?")[0]).strip("'").replace("''", "'")
            if tab in self.bad_tabs or tab not in self.sheets:
                return Resp(400, {"error": {"message": "Unable to parse range: " + tab}})
            return Resp(200, {"range": tab, "values": self.sheets[tab]})
        if "/jobs/" in url:
            job_id = url.split("/jobs/")[1].split("?")[0]
            job = self.jobs[job_id]
            job["polls"] += 1
            state = "DONE" if job["polls"] >= self.polls_before_done else "RUNNING"
            status = {"state": state}
            if state == "DONE" and job["table"] in self.job_error:
                status["errorResult"] = {"message": self.job_error[job["table"]]}
            return Resp(200, {"jobReference": {"jobId": job_id}, "status": status,
                              "statistics": {"load": {"outputRows": str(job["rows"])}}})
        if url.endswith(f"/datasets/{DATASET}"):
            return Resp(200 if self.dataset else 404, {"error": {"message": "Not found: Dataset"}} if not self.dataset else {})
        if f"/datasets/{DATASET}/tables/" in url:
            tid = url.rsplit("/", 1)[1]
            return Resp(200 if tid in self.tables else 404, {})
        raise AssertionError("unexpected GET " + url)

    def post(self, url, json=None, timeout=None, headers=None, **_):
        self.calls.append(("POST", url))
        if url.endswith(f"/datasets/{DATASET}/tables"):
            self.created.append(json)
            self.tables.add(json["tableReference"]["tableId"])
            return Resp(200, json)
        if url.startswith(arc.BQ_UPLOAD) and "uploadType=resumable" in url:
            job_id = json["jobReference"]["jobId"]
            dest = json["configuration"]["load"]["destinationTable"]["tableId"]
            self.jobs[job_id] = {"config": json, "table": dest.split("$")[0], "polls": 0, "rows": 0,
                                 "length": int(headers["X-Upload-Content-Length"])}
            return Resp(200, {}, {"Location": "https://upload.invalid/session/" + job_id})
        raise AssertionError("unexpected POST " + url)

    def put(self, url, data=None, timeout=None, headers=None, **_):
        self.calls.append(("PUT", url))
        job_id = url.rsplit("/", 1)[1]
        job = self.jobs[job_id]
        assert job["length"] == len(data)
        lines = [l for l in data.decode("utf-8").split("\n") if l]
        job["rows"] = len(lines)
        self.uploads[job["table"]] = [json.loads(l) for l in lines]
        return Resp(200, {"jobReference": {"jobId": job_id}, "status": {"state": "RUNNING"}})


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("TFB_BQ_ARCHIVE", "on")
    monkeypatch.setenv("TFB_BQ_PROJECT", PROJECT)
    monkeypatch.setenv("TFB_BQ_DATASET", DATASET)
    monkeypatch.setenv("DEFAULT_SPREADSHEET_ID", SID)
    monkeypatch.setenv("GITHUB_RUN_ID", "40500000001")
    monkeypatch.setattr(arc.time, "sleep", lambda s: None)
    return monkeypatch


TABS3 = "Top_10_Investments,_Selection_Log,Calendar_Events"


# --------------------------------------------------------------------- A1
@pytest.mark.parametrize("gate", [None, "", "off", "0", "onn"])
def test_a1_gate_off_makes_no_request(env, gate):
    if gate is None:
        env.delenv("TFB_BQ_ARCHIVE")
    else:
        env.setenv("TFB_BQ_ARCHIVE", gate)
    s = FakeSession()
    assert arc.main(["--write"], session=s, now=NOW) == 0
    assert s.calls == []


def test_a1_missing_configuration(env):
    env.delenv("TFB_BQ_PROJECT")
    s = FakeSession()
    assert arc.main(["--write"], session=s, now=NOW) == 2 and s.calls == []


# --------------------------------------------------------------------- A2
def test_a2_dry_run_reads_and_creates_nothing(env, capsys):
    s = FakeSession()
    assert arc.main(["--tabs", TABS3], session=s, now=NOW) == 0
    assert [c for c in s.calls if c[0] != "GET"] == [] and s.created == [] and s.jobs == {}
    err = capsys.readouterr().err
    assert "_Selection_Log -> tfb_archive.selection_log$20261027: 3 rows (12 cells) | dry run" in err
    assert "done: 3/3 tab(s), 9 rows, snapshot 2026-10-27 (dry run)" in err


# --------------------------------------------------------------------- A3
def test_a3_write_creates_partitioned_tables_and_loads_each_day(env, capsys):
    s = FakeSession(tables={"top_10_investments"})
    assert arc.main(["--write", "--tabs", TABS3], session=s, now=NOW) == 0
    # only the two missing tables are created, DAY-partitioned on snapshot_date
    assert [c["tableReference"]["tableId"] for c in s.created] == ["selection_log", "calendar_events"]
    for c in s.created:
        assert c["timePartitioning"] == {"type": "DAY", "field": "snapshot_date"}
        assert c["schema"]["fields"] == arc.SCHEMA
        assert c["tableReference"] == {"projectId": PROJECT, "datasetId": DATASET,
                                       "tableId": c["tableReference"]["tableId"]}
    loads = {j["table"]: j["config"]["configuration"]["load"] for j in s.jobs.values()}
    assert set(loads) == {"top_10_investments", "selection_log", "calendar_events"}
    for table, load in loads.items():
        assert load["destinationTable"] == {"projectId": PROJECT, "datasetId": DATASET,
                                            "tableId": table + "$20261027"}
        assert load["writeDisposition"] == "WRITE_TRUNCATE" and load["createDisposition"] == "CREATE_NEVER"
        assert load["sourceFormat"] == "NEWLINE_DELIMITED_JSON" and load["schema"]["fields"] == arc.SCHEMA
    sel = s.uploads["selection_log"]
    assert [r["row_num"] for r in sel] == [1, 2, 4], "the blank row 3 is skipped"
    assert sel[0]["record_json"] is None, "the header row itself carries no record"
    assert json.loads(sel[1]["record_json"]) == {"Logged At": "2026-10-27 10:48:41", "Symbol": "FRST.US",
                                                 "Rank": "1", "Engine ROI %": "18.25"}
    assert sel[1]["cells"] == ["2026-10-27 10:48:41", "FRST.US", "1", "18.25"]
    assert all(r["snapshot_date"] == "2026-10-27" and r["captured_at"] == "2026-10-27T23:05:07.123456Z"
               and r["run_id"] == "40500000001" and r["tab"] == "_Selection_Log" for r in sel)
    top = s.uploads["top_10_investments"]
    assert [r["row_num"] for r in top] == [1, 3, 4, 5] and all(r["record_json"] is None for r in top)
    cal = json.loads(s.uploads["calendar_events"][1]["record_json"])
    assert cal == {"Symbol": "FRST.US", "Next Earnings Date": "2027-01-28", "Days To Earnings": "93",
                   "col_4": "x", "Last Earnings Date": "2026-10-22", "EPS Surprise %": "-22.2",
                   "Symbol_2": "dup"}, "blank and duplicate headers get stable names"
    err = capsys.readouterr().err
    assert "_Selection_Log -> tfb_archive.selection_log$20261027: 3 rows (12 cells) | loaded 3 | table created" in err
    assert "done: 3/3 tab(s), 9 rows, snapshot 2026-10-27" in err
    # a re-run the same day replaces the same partition (WRITE_TRUNCATE), creates nothing
    s2 = FakeSession(tables={"top_10_investments", "selection_log", "calendar_events"}, polls_before_done=3)
    assert arc.main(["--write", "--tabs", TABS3], session=s2, now=NOW) == 0
    assert s2.created == [] and {j["config"]["configuration"]["load"]["destinationTable"]["tableId"]
                                 for j in s2.jobs.values()} == {t + "$20261027" for t in loads}
    assert all(j["polls"] == 3 for j in s2.jobs.values()), "polled until DONE"


def test_a3_date_option_and_default_tabs(env):
    s = FakeSession(sheets={t: [["h"], ["v"]] for t, _ in arc.TABS}, tables={arc.table_name(t) for t, _ in arc.TABS})
    assert arc.main(["--write", "--date", "2026-10-20"], session=s, now=NOW) == 0
    dests = sorted(j["config"]["configuration"]["load"]["destinationTable"]["tableId"] for j in s.jobs.values())
    assert dests == sorted(arc.table_name(t) + "$20261020" for t, _ in arc.TABS) and len(dests) == 17
    assert arc.main(["--write", "--date", "27-10-2026"], session=FakeSession(), now=NOW) == 2


# --------------------------------------------------------------------- A4
def test_a4_one_bad_tab_or_job_does_not_stop_the_others(env, capsys):
    s = FakeSession(bad_tabs={"_Selection_Log"}, job_error={"calendar_events": "Provided Schema does not match"},
                    tables={"top_10_investments", "selection_log", "calendar_events"})
    assert arc.main(["--write", "--tabs", TABS3], session=s, now=NOW) == 1
    assert "top_10_investments" in s.uploads
    err = capsys.readouterr().err
    assert "ERROR _Selection_Log: read _Selection_Log: HTTP 400: Unable to parse range: _Selection_Log" in err
    assert "ERROR Calendar_Events: load calendar_events: Provided Schema does not match" in err
    assert "done: 1/3 tab(s)" in err and "FAILED: _Selection_Log, Calendar_Events" in err


def test_a4_dataset_not_readable_is_a_configuration_error(env, capsys):
    class Denied(FakeSession):
        def get(self, url, timeout=None, **kw):
            if url.endswith(f"/datasets/{DATASET}"):
                self.calls.append(("GET", url))
                return Resp(403, {"error": {"message": "Access Denied: Dataset proj-x:tfb_archive"}})
            return super().get(url, timeout=timeout, **kw)
    s = Denied()
    assert arc.main(["--write", "--tabs", TABS3], session=s, now=NOW) == 2 and len(s.calls) == 1
    assert "ERROR: dataset proj-x.tfb_archive: HTTP 403: Access Denied" in capsys.readouterr().err


def test_a4_missing_dataset_stops_before_any_read(env, capsys):
    s = FakeSession(dataset=False)
    assert arc.main(["--write", "--tabs", TABS3], session=s, now=NOW) == 2
    assert s.calls == [("GET", f"{arc.BQ_API}/projects/{PROJECT}/datasets/{DATASET}")]
    assert "dataset proj-x.tfb_archive not found" in capsys.readouterr().err


def test_a4_empty_tab_loads_nothing_and_keeps_the_partition(env, capsys):
    s = FakeSession(sheets={"_Run_Log": [[], ["", ""]]}, tables={"run_log"})
    assert arc.main(["--write", "--tabs", "_Run_Log"], session=s, now=NOW) == 0
    assert s.jobs == {} and "nothing to load" in capsys.readouterr().err


# --------------------------------------------------------------------- A5
@pytest.mark.parametrize("tab,name", [("_Selection_Log", "selection_log"), ("Top_10_Investments", "top_10_investments"),
                                      ("My Portfolio (v2)", "my_portfolio_v2"), ("___", "tab")])
def test_a5_table_name(tab, name):
    assert arc.table_name(tab) == name


@pytest.mark.parametrize("value,text", [(None, ""), ("", ""), (True, "TRUE"), (False, "FALSE"), (174, "174"),
                                        (18.0, "18"), (18.25, "18.25"), (-22.2, "-22.2"), (1e16, "1e+16"),
                                        (0.1 + 0.2, "0.30000000000000004"), ("2026-10-22", "2026-10-22")])
def test_a5_cell_text(value, text):
    assert arc.cell_text(value) == text


def test_a5_header_names_and_caps(monkeypatch):
    assert arc.header_names(["Symbol", "", "Symbol", None, "Rank"]) == ["Symbol", "col_2", "Symbol_2", "col_4", "Rank"]
    monkeypatch.setattr(arc, "MAX_ROWS_PER_TAB", 2)
    rows, n = arc.build_rows("T", [["h"], ["a"], ["b"], ["c"]], 1, "2026-10-27", "x", "")
    assert [r["row_num"] for r in rows] == [1, 2] and n["truncated"] == 2 and rows[0]["run_id"] is None
    rows, n = arc.build_rows("T", [["a", "b"], ["1", "2", "3"]], 1, "2026-10-27", "x", "r")
    assert json.loads(rows[1]["record_json"]) == {"a": "1", "b": "2", "col_3": "3"}, "cells past the header get col_N"
    assert arc._select_tabs("") == list(arc.TABS)
    assert arc._select_tabs("Calendar_Events, NewTab") == [("Calendar_Events", 1), ("NewTab", 1)]
    assert arc.ndjson([{"a": "\u0645"}]) == '{"a":"\u0645"}\n'.encode("utf-8")


def test_a5_import_is_side_effect_free():
    src = (ROOT / "scripts" / "archive_to_bigquery.py").read_text(encoding="utf-8")
    assert "\nimport google" not in src and "\nfrom google" not in src and "\nimport requests" not in src
    assert arc.__version__ == "1.1.0"


# --------------------------------------------------------------------- A6
def test_a6_workflow():
    import yaml
    wf_text = (ROOT / ".github" / "workflows" / "bq_archive.yml").read_text(encoding="utf-8")
    wf = yaml.safe_load(wf_text)
    on = wf.get(True) or wf.get("on")
    assert on["schedule"] == [{"cron": "5 23 * * *"}]
    assert on["workflow_dispatch"]["inputs"]["write"]["default"] is False
    assert wf["permissions"] == {"contents": "read"}
    step = [s for s in wf["jobs"]["archive"]["steps"] if s.get("name", "").endswith("Run the archive")][0]
    env = step["env"]
    assert env["TFB_BQ_ARCHIVE"] == "${{ vars.TFB_BQ_ARCHIVE || 'on' }}"
    assert env["TFB_BQ_PROJECT"] == "${{ vars.TFB_BQ_PROJECT || 'project-2e3530e6-2311-4aa3-b36' }}"
    assert env["TFB_BQ_DATASET"] == "${{ vars.TFB_BQ_DATASET || 'tfb_archive' }}"
    assert "secrets.GOOGLE_SHEETS_CREDENTIALS" in env["GOOGLE_SHEETS_CREDENTIALS"]
    assert 'MODE="--dry-run"' in step["run"] and "--write" in step["run"]


# --------------------------------------------------------------------- A7
def test_a7_market_and_news_tabs():
    from scripts import apply_bq_views as bv
    from core.sheets import schema_registry as reg
    added = [("Market_Leaders", 1), ("Global_Markets", 1), ("Mutual_Funds", 1), ("Commodities_FX", 1),
             ("_News_Items", 1), ("_News_Gate", 1), ("_Econ_Events", 1)]
    assert list(arc.TABS[10:]) == added and len(arc.TABS) == 17
    assert len({t for t, _ in arc.TABS}) == len({arc.table_name(t) for t, _ in arc.TABS}) == 17
    assert {arc.table_name(t) for t, _ in arc.TABS} == bv.ARCHIVE_TABLES
    head = reg.get_sheet_headers("Global_Markets")
    row = ["AAPL.US", "Apple Inc."] + [""] * 6 + [271.4, 270.1] + [""] * 104 + ["warn"]
    assert len(head) == len(row) == 115 <= arc.MAX_COLS
    rows, n = arc.build_rows("Global_Markets", [head, row], 1, "2026-10-31", "x", "r")
    assert n == {"rows": 2, "cells": 230, "truncated": 0} and rows[0]["record_json"] is None
    assert rows[1]["cells"][8] == "271.4" and rows[1]["cells"][114] == "warn"
    assert json.loads(rows[1]["record_json"]) == {"Symbol": "AAPL.US", "Name": "Apple Inc.",
                                                  "Current Price": "271.4", "Previous Close": "270.1",
                                                  "Warnings": "warn"}
