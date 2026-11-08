#!/usr/bin/env python3
# tests/test_run_corporate_events.py
"""scripts/run_corporate_events.py v1.0.0 - the Corporate_Events tab from
EDGAR 8-K items. Offline: EDGAR is a fake fetch, the workbook a fake book.

C1  us_ticker: .US and bare tickers map to EDGAR tickers (BRK.B -> BRK-B);
    every other venue is None
C2  events_from_submissions: only 8-K / 8-K/A with a CORP_ITEMS item within
    the lookback; the gravest event leads; 2.02 earnings alone is not an
    event; a 6-K is not read; the URL is the EDGAR archive path
C3  build_rows: newest first, one row per filing, header order, bounded
C4  main: no TFB_SEC_USER_AGENT -> exit 2, nothing written (a silent empty tab
    would read as 'no corporate action'); dry run writes nothing; a write
    replaces the tab whole with the header and a blank fill; the INDV case
    (8-K 2.01 + 5.03 filed 2 Nov) reaches the tab; non-US symbols are never
    sent to EDGAR; the breaker (2 consecutive failures) fails the write closed
C5  the workflow: schedule + dispatch, write only with write=true, the
    contact comes from the secret or the variable, the script path
"""
from __future__ import annotations

import datetime as dt
import os
import re
import sys
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts import run_corporate_events as ca  # noqa: E402

NOW = dt.datetime(2026, 11, 8, 10, 0, tzinfo=dt.timezone.utc)


class WorksheetNotFound(Exception):
    pass


class Sheet:
    def __init__(self, rows, cols, row_count=200):
        self.values = deepcopy(rows)
        self.col_count, self.row_count = cols, row_count
        self.requests, self.resizes = [], []

    def get(self, range_name):
        return deepcopy(self.values)

    def resize(self, rows=None, cols=None):
        self.resizes.append((rows, cols))
        if rows is not None:
            self.row_count = rows
        if cols is not None:
            self.col_count = cols

    def update(self, *, values, range_name, value_input_option):
        m = re.fullmatch(r"A1:([A-Z])(\d+)", range_name)
        width = ord(m.group(1)) - ord("A") + 1
        assert width <= self.col_count and int(m.group(2)) == len(values) <= self.row_count
        assert all(len(r) == width for r in values) and value_input_option == "RAW"
        self.requests.append({"values": deepcopy(values), "range": range_name})
        self.values = deepcopy(values)

    def freeze(self, *, rows):
        assert rows == 1


class Book:
    def __init__(self, pages, tab=None):
        self.pages, self.tab, self.added = pages, tab, []

    def worksheet(self, name):
        if name in self.pages:
            return self.pages[name]
        if name == "Corporate_Events" and self.tab is not None:
            return self.tab
        raise WorksheetNotFound(name)

    def add_worksheet(self, *, title, rows, cols):
        self.tab = Sheet([], cols, rows)
        self.added.append((title, rows, cols))
        return self.tab


def page(symbols):
    return Sheet([["Rank", "Symbol", "Name"]] + [[i + 1, s, "x"] for i, s in enumerate(symbols)], 3)


def submissions(filings):
    """filings: (form, date, items, accession, doc)"""
    return {"filings": {"recent": {
        "form": [f[0] for f in filings], "filingDate": [f[1] for f in filings],
        "acceptanceDateTime": [f[1] + "T21:05:00.000Z" for f in filings],
        "items": [f[2] for f in filings], "accessionNumber": [f[3] for f in filings],
        "primaryDocument": [f[4] for f in filings]}}}


TICKERS = {"0": {"cik_str": 1385508, "ticker": "INDV", "title": "Indivior PLC"},
           "1": {"cik_str": 9999, "ticker": "FRST", "title": "Primis Financial Corp"},
           "2": {"cik_str": 7, "ticker": "BRK-B", "title": "Berkshire"}}
INDV = submissions([("8-K", "2026-11-02", "2.01,5.03,9.01", "0001385508-26-000050", "d1.htm"),
                    ("8-K", "2026-10-29", "2.02,9.01", "0001385508-26-000048", "d2.htm"),
                    ("6-K", "2026-11-03", "2.01", "0001385508-26-000051", "d3.htm"),
                    ("8-K", "2026-09-01", "2.01", "0001385508-26-000010", "d4.htm")])
FRST = submissions([("8-K", "2026-10-22", "2.02,9.01", "0000009999-26-000020", "f.htm")])


def fake_fetch(pages=None, fail=()):
    pages = pages or {}
    calls = []

    def fetch(url):
        calls.append(url)
        if url in fail:
            raise ConnectionError(url)
        if url == ca.EDGAR_TICKERS:
            return TICKERS
        m = re.search(r"CIK(\d{10})\.json$", url)
        return pages.get(int(m.group(1)), {"filings": {"recent": {}}})
    fetch.calls = calls
    return fetch


def test_c1_us_ticker():
    assert ca.us_ticker("INDV.US") == "INDV" and ca.us_ticker("AAPL") == "AAPL"
    assert ca.us_ticker("BRK.B.US") == "BRK-B"
    for s in ("2222.SR", "BBOX.L", "ACA.PA", "2020.HK", "7203.T", "", "  "):
        assert ca.us_ticker(s) is None, s


def test_c2_events_from_submissions():
    since = NOW - dt.timedelta(days=45)
    ev = ca.events_from_submissions(INDV, "INDV.US", 1385508, since)
    assert [e["event_date"] for e in ev] == ["2026-11-02"]            # 2.02 alone: no; 6-K: no; 1 Sep: outside
    e = ev[0]
    assert e["event"] == "merger_completed" and e["events"] == ["merger_completed", "charter_change"]
    assert e["items"] == ["2.01", "5.03"] and e["form"] == "8-K"
    assert e["url"] == "https://www.sec.gov/Archives/edgar/data/1385508/000138550826000050/d1.htm"
    assert e["filed_at"] == dt.datetime(2026, 11, 2, 21, 5, tzinfo=dt.timezone.utc)
    assert ca.events_from_submissions(FRST, "FRST.US", 9999, since) == []
    assert ca.events_from_submissions({}, "X.US", 1, since) == [] and ca.events_from_submissions(None, "X.US", 1, since) == []
    # the gravest event leads whatever the item order
    grave = submissions([("8-K/A", "2026-11-01", "5.03,1.03", "a", "b")])
    assert ca.events_from_submissions(grave, "Y.US", 2, since)[0]["event"] == "bankruptcy"
    assert set(ca.CORP_ITEMS) == {"2.01", "5.01", "5.03", "3.03", "1.03", "3.01", "4.02"}
    assert "2.02" not in ca.CORP_ITEMS and "8.01" not in ca.CORP_ITEMS


def test_c3_build_rows():
    since = NOW - dt.timedelta(days=45)
    ev = ca.events_from_submissions(INDV, "INDV.US", 1385508, since)
    ev += ca.events_from_submissions(submissions([("8-K", "2026-11-05", "5.01", "c", "d.htm")]), "AAA.US", 5, since)
    rows = ca.build_rows(ev + ev, NOW)                                 # a duplicate filing is one row
    assert len(rows) == 2 and rows[0][0] == "AAA.US" and rows[1][0] == "INDV.US"   # newest first
    assert rows[1][:6] == ["INDV.US", "merger_completed+charter_change", "2026-11-02", "2026-11-02 21:05", "8-K", "2.01,5.03"]
    assert rows[1][6] == "sec.gov EDGAR submissions" and rows[1][8] == "2026-11-08 13:00"
    assert len(rows[0]) == len(ca.HEADERS) == 9
    assert ca.HEADERS == ["Symbol", "Event", "Event Date", "Filed At (UTC)", "Form", "Items", "Source", "URL", "Updated At (Riyadh)"]
    many = [dict(ev[0], filed_at=ev[0]["filed_at"] - dt.timedelta(minutes=i), symbol=f"S{i}.US") for i in range(2500)]
    assert len(ca.build_rows(many, NOW)) == ca.MAX_ROWS - 1


def _env(monkeypatch, ua="TFB research ops@example.test"):
    monkeypatch.setenv("TFB_SEC_USER_AGENT", ua)
    monkeypatch.delenv("TFB_CORP_PAGES", raising=False)
    monkeypatch.delenv("TFB_CORP_EVENTS_SHEET", raising=False)
    monkeypatch.delenv("TFB_CORP_LOOKBACK_DAYS", raising=False)


def test_c4_main(monkeypatch, capsys):
    book = Book({"Top_10_Investments": page(["INDV.US", "2222.SR", "BRK.B.US"]), "My_Portfolio": page(["FRST.US", "INDV.US"])})
    # no contact: exit 2, no EDGAR call, no tab
    monkeypatch.delenv("TFB_SEC_USER_AGENT", raising=False)
    assert ca.main(["--write"], book=book, now=NOW) == 2 and book.tab is None and book.added == []
    assert "TFB_SEC_USER_AGENT not set" in capsys.readouterr().out
    _env(monkeypatch)
    fetch = fake_fetch({1385508: INDV, 9999: FRST})
    edgar = ca.Edgar("ua", 20, fetch=fetch, sleep=lambda s: None)
    # dry run: EDGAR read, nothing written
    assert ca.main(["--dry-run"], book=book, edgar=edgar, now=NOW) == 0 and book.tab is None
    out = capsys.readouterr().out
    assert "US listings: 3" in out and "1 corporate-action filing(s), 1 row(s)" in out and "dry run" in out
    assert not any("2222" in u for u in fetch.calls)                     # a Saudi name never reaches EDGAR
    assert sum(1 for u in fetch.calls if "CIK" in u) == 3                 # INDV, BRK-B, FRST (INDV once)
    # write: the tab is created whole, header + 1 row + blank fill, frozen header
    edgar = ca.Edgar("ua", 20, fetch=fake_fetch({1385508: INDV, 9999: FRST}), sleep=lambda s: None)
    assert ca.main(["--write"], book=book, edgar=edgar, now=NOW) == 0
    assert book.added == [("Corporate_Events", 200, 9)]
    vals = book.tab.requests[-1]["values"]
    assert vals[0] == ca.HEADERS and vals[1][:3] == ["INDV.US", "merger_completed+charter_change", "2026-11-02"]
    assert len(vals) == 200 and vals[2] == [""] * 9 and book.tab.requests[-1]["range"] == "A1:I200"
    # a later write with no filing leaves only the header (the prior row is blanked, not kept)
    edgar = ca.Edgar("ua", 20, fetch=fake_fetch({9999: FRST}), sleep=lambda s: None)
    assert ca.main(["--write"], book=book, edgar=edgar, now=NOW) == 0
    vals = book.tab.requests[-1]["values"]
    assert vals[0] == ca.HEADERS and vals[1] == [""] * 9
    # the breaker: two consecutive failures skip the rest and the write fails closed, tab untouched
    before = len(book.tab.requests)
    bad = fake_fetch({}, fail={ca.EDGAR_SUBMISSIONS.format(cik=1385508), ca.EDGAR_SUBMISSIONS.format(cik=7)})
    edgar = ca.Edgar("ua", 20, fetch=bad, sleep=lambda s: None)
    assert ca.main(["--write"], book=book, edgar=edgar, now=NOW) == 2
    assert edgar.skipped == ["FRST.US"] and len(edgar.failures) == 2 and len(book.tab.requests) == before
    assert "EDGAR read incomplete" in capsys.readouterr().out
    # a prior tab beyond the bound is preserved
    book2 = Book({"Top_10_Investments": page(["INDV.US"])}, tab=Sheet([ca.HEADERS], 9, row_count=ca.MAX_ROWS + 5))
    edgar = ca.Edgar("ua", 20, fetch=fake_fetch({1385508: INDV}), sleep=lambda s: None)
    assert ca.main(["--write"], book=book2, edgar=edgar, now=NOW) == 2 and book2.tab.requests == []
    # knobs
    monkeypatch.setenv("TFB_CORP_LOOKBACK_DAYS", "500")
    assert ca.lookback_days() == 120
    monkeypatch.setenv("TFB_CORP_LOOKBACK_DAYS", "junk")
    assert ca.lookback_days() == 45
    monkeypatch.setenv("TFB_CORP_TIMEOUT_S", "2")
    assert ca.timeout_s() == 5


def test_c5_workflow():
    wf = yaml.safe_load((ROOT / ".github" / "workflows" / "corporate_events.yml").read_text(encoding="utf-8"))
    on = wf.get("on") or wf.get(True)
    assert "schedule" in on and "workflow_dispatch" in on
    assert on["workflow_dispatch"]["inputs"]["write"]["default"] in (False, "false")
    job = wf["jobs"]["corporate_events"]
    run_step = [s for s in job["steps"] if "run_corporate_events.py" in (s.get("run") or "")][0]
    env = run_step["env"]
    assert env["TFB_SEC_USER_AGENT"] == "${{ secrets.TFB_SEC_USER_AGENT || vars.TFB_SEC_USER_AGENT }}"
    assert "GOOGLE_SHEETS_CREDENTIALS" in env and env["DEFAULT_SPREADSHEET_ID"] == "${{ steps.sheet.outputs.sid }}"
    assert 'MODE="--dry-run"' in run_step["run"] and "python scripts/run_corporate_events.py $MODE" in run_step["run"]
    assert wf["permissions"] == {"contents": "read"}
    src = (ROOT / "scripts" / "run_corporate_events.py").read_text(encoding="utf-8")
    assert '__version__ = "1.0.0"' in src and "TFB_SEC_USER_AGENT" in src
