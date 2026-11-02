"""scripts/archive_market_series.py v1.1.1 - the daily bars of the world's
reference markets for the Looker global markets page (program register C8,
Phase 2b).

REAL module; the stubs are the Yahoo opener / fetch and the HTTP session (an
AuthorizedSession-like fake answering BigQuery datasets, tables, jobs and the
resumable upload offline). No network, no google libraries.

M1  SERIES: ids unique, permanent (pinned below) and inside 1..1000; symbols
    and names unique; kinds and regions from the declared lists; every
    sector ETF names its sector, every index its local currency
M2  parse_chart: venue-local dates from gmtoffset, null and non-positive
    closes skipped, a repeated date keeps the last bar, adjclose / volume
    read, output sorted
M3  the bars of a session still open at run time are left out; a closed
    session or a malformed trading period keeps every bar
M4  fetch_series: query1 then query2, a 404 is final, the symbol is encoded
    and the window passed
M5  rows: exactly the SCHEMA columns, blanks as NULL, NDJSON lines
M6  the table: the live 29 Oct layout passes schema_problems; a changed mode,
    type, column or partitioning is refused; ensure_table creates the
    range-partitioned table when missing and refuses a different one
M7  write: each series loads into market_series_daily$<id> with
    WRITE_TRUNCATE / CREATE_NEVER; a series that fails once is retried and
    loaded; one that fails twice is left untouched while the others load
    (exit 1); a job errorResult counts as a failure
M8  dry run: Yahoo read and the dataset checked; no table call, no load
M9  main: gate off (no request at all), missing project, bad --days, unknown
    symbol, missing dataset, unreadable credentials, a table with another
    layout
M10 the workflow: nightly schedule, dry-run dispatch by default, read-only
    repository, armed gate with a kill switch, pinned actions
M11 nothing here writes to the workbook or runs DML
M12 (v1.1.0) a daily answer shorter than MIN_BARS (Yahoo gives ^TASI.SR and
    000300.SS one bar) is rebuilt from hourly bars: one row per venue-local
    day - first open, high max, low min, last close, summed volume - from
    p1 on, an open session left out, source yahoo_chart_v8_1h; a full daily
    answer never asks for hourly bars
M13 (v1.1.0) a series still shorter than MIN_BARS fails and keeps its rows
    (no WRITE_TRUNCATE down to one day); the others load
M14 (v1.1.1) the minimum is window-aware: --days 30 (20-22 trading days)
    needs 11 bars, the nightly 1,100 days 30, never fewer than 2; a complete
    30-day answer loads every series (daily, or rebuilt from hourly bars,
    with no hourly call on a full daily answer) while one bar still fails
    and keeps its rows; the 10 / 11 boundary holds at both decision points,
    and months with long closures (12 and 13 sessions, a Sun-Thu week) load
"""
from __future__ import annotations

import datetime as dt
import io
import json
import urllib.error
from pathlib import Path

import pytest
import yaml

from scripts import archive_market_series as ms

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "bq_market_series_table_2026-10-29.json"
WORKFLOW = ROOT / ".github" / "workflows" / "bq_markets.yml"
NOW = dt.datetime(2026, 10, 29, 22, 35, 9, 123456, tzinfo=dt.timezone.utc)
PROJECT, DATASET = "proj-x", "tfb_archive"

# series_id -> symbol is permanent (one partition per id): a renumbered or
# reused id would mix two instruments in one partition.
PINNED_IDS = {
    1: "^GSPC", 2: "^IXIC", 3: "^DJI", 4: "^STOXX50E", 5: "^FTSE", 6: "^GDAXI", 7: "^FCHI",
    8: "000001.SS", 9: "399001.SZ", 10: "000300.SS", 11: "^HSI", 12: "^N225", 13: "^TASI.SR",
    20: "SPY", 21: "VGK", 22: "MCHI", 23: "EWJ", 24: "KSA", 25: "UAE", 26: "QAT", 27: "KWT",
    28: "ACWI", 29: "EEM",
    40: "XLK", 41: "XLF", 42: "XLE", 43: "XLV", 44: "XLI", 45: "XLY", 46: "XLP", 47: "XLU",
    48: "XLB", 49: "XLRE", 50: "XLC",
    60: "BZ=F", 61: "CL=F", 62: "GC=F",
    70: "IXN", 71: "IXG", 72: "IXC", 73: "IXJ", 74: "EXI", 75: "RXI", 76: "KXI", 77: "JXI",
    78: "MXI", 79: "REET", 80: "IXP",
}


def _ts(day: str, hh: int = 14, mm: int = 30) -> int:
    d = dt.datetime.strptime(day, "%Y-%m-%d").replace(hour=hh, minute=mm, tzinfo=dt.timezone.utc)
    return int(d.timestamp())


def _chart(ts, close, adj=None, gmtoffset=-14400, period=None, volume=None):
    q = {"open": close, "high": close, "low": close, "close": close,
         "volume": volume if volume is not None else [1000] * len(close)}
    ind = {"quote": [q]}
    if adj is not None:
        ind["adjclose"] = [{"adjclose": adj}]
    meta = {"gmtoffset": gmtoffset}
    if period is not None:
        meta["currentTradingPeriod"] = {"regular": period}
    return {"chart": {"result": [{"meta": meta, "timestamp": ts, "indicators": ind}], "error": None}}


def _bars(*days):
    return [{"date": d, "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0 + i, "adj_close": None,
             "volume": 5} for i, d in enumerate(days)]


def _days(n=40, end="2026-10-28"):
    """n consecutive weekdays ending on `end` (a full history for the tests)."""
    d = dt.date.fromisoformat(end)
    out = []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d -= dt.timedelta(days=1)
    return _bars(*reversed(out))


class Resp:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code = status
        self._body = body if body is not None else {}
        self.headers = headers or {}

    def json(self):
        return self._body


class FakeBQ:
    """Offline BigQuery: datasets.get, tables.get / insert, resumable load
    jobs (DONE after `polls` GETs) and the uploaded NDJSON per partition."""

    def __init__(self, table=None, dataset=True, job_error=None, polls=1):
        self.table = table            # None = missing; dict = tables.get body
        self.dataset = dataset
        self.job_error = job_error or {}
        self.polls = polls
        self.calls, self.created, self.jobs, self.uploads = [], [], {}, {}

    def get(self, url, timeout=None, **_):
        self.calls.append(("GET", url))
        if "/jobs/" in url:
            job_id = url.split("/jobs/")[1].split("?")[0]
            job = self.jobs[job_id]
            job["polls"] += 1
            done = job["polls"] >= self.polls
            status = {"state": "DONE" if done else "RUNNING"}
            if done and job["sid"] in self.job_error:
                status["errorResult"] = {"message": self.job_error[job["sid"]]}
            return Resp(200, {"status": status, "statistics": {"load": {"outputRows": str(job["rows"])}}})
        if url.endswith(f"/datasets/{DATASET}"):
            return Resp(200 if self.dataset else 404, {})
        if url.endswith(f"/tables/{ms.TABLE}"):
            return Resp(200, self.table) if self.table is not None else Resp(404, {})
        raise AssertionError("unexpected GET " + url)

    def post(self, url, json=None, timeout=None, headers=None, **_):
        self.calls.append(("POST", url))
        if url.endswith(f"/datasets/{DATASET}/tables"):
            self.created.append(json)
            self.table = {"schema": json["schema"], "rangePartitioning": json["rangePartitioning"]}
            return Resp(200, json)
        if url.startswith(ms.BQ_UPLOAD) and "uploadType=resumable" in url:
            job_id = json["jobReference"]["jobId"]
            dest = json["configuration"]["load"]["destinationTable"]["tableId"]
            self.jobs[job_id] = {"config": json, "dest": dest, "sid": int(dest.split("$")[1]), "polls": 0,
                                 "rows": 0, "length": int(headers["X-Upload-Content-Length"])}
            return Resp(200, {}, {"Location": "https://upload.invalid/session/" + job_id})
        raise AssertionError("unexpected POST " + url)

    def put(self, url, data=None, timeout=None, headers=None, **_):
        self.calls.append(("PUT", url))
        job = self.jobs[url.rsplit("/", 1)[1]]
        assert job["length"] == len(data)
        lines = [ln for ln in data.decode("utf-8").split("\n") if ln]
        job["rows"] = len(lines)
        self.uploads[job["sid"]] = [json.loads(ln) for ln in lines]
        return Resp(200, {"status": {"state": "RUNNING"}})


def _live_table():
    body = json.loads(FIXTURE.read_text(encoding="utf-8"))
    body.pop("_note", None)
    return body


def _env(monkeypatch, gate="on", project=PROJECT):
    monkeypatch.setenv("TFB_BQ_MARKETS", gate)
    if project is None:
        monkeypatch.delenv("TFB_BQ_PROJECT", raising=False)
    else:
        monkeypatch.setenv("TFB_BQ_PROJECT", project)
    monkeypatch.setenv("TFB_BQ_DATASET", DATASET)
    monkeypatch.setenv("TFB_BQ_LOCATION", "US")
    monkeypatch.setenv("GITHUB_RUN_ID", "777")


def _run(monkeypatch, argv, session, fetch, capsys=None):
    _env(monkeypatch)
    slept = []
    rc = ms.main(argv, session=session, now=NOW, fetch=fetch, sleep=slept.append)
    return rc, slept


# --------------------------------------------------------------------------- M1
def test_m1_series_integrity():
    ids = [s[0] for s in ms.SERIES]
    assert len(ids) == len(set(ids)) == 48
    assert {s[0]: s[1] for s in ms.SERIES} == PINNED_IDS
    assert all(1 <= i <= ms.MAX_SERIES_ID for i in ids)
    syms = [s[1] for s in ms.SERIES]
    names = [s[2] for s in ms.SERIES]
    assert len(set(syms)) == len(syms) and len(set(names)) == len(names)
    for sid, sym, name, region, country, kind, cur, sector in ms.SERIES:
        assert region in ms.REGIONS and kind in ms.KINDS, sym
        assert cur and name and sym
        if "sector" in kind:
            assert sector, sym
        if kind != "Index":
            assert cur == "USD", sym
    local = {s[1]: s[6] for s in ms.SERIES if s[5] == "Index"}
    assert local["^TASI.SR"] == "SAR" and local["^N225"] == "JPY" and local["000300.SS"] == "CNY"
    regions = {s[3] for s in ms.SERIES}
    assert {"US", "Europe", "China", "Gulf"} <= regions
    assert [r["symbol"] for r in ms.series_records("KSA, ^GSPC")] == ["KSA", "^GSPC"]
    with pytest.raises(ValueError):
        ms.series_records("NOPE")


# --------------------------------------------------------------------------- M2
def test_m2_parse_chart():
    ts = [_ts("2026-10-26"), _ts("2026-10-27"), _ts("2026-10-28"), _ts("2026-10-28", 19), _ts("2026-10-29")]
    close = [100.0, None, 101.5, 102.0, 0]
    adj = [99.0, None, 100.5, 101.0, 0]
    bars = ms.parse_chart(_chart(ts, close, adj, volume=[10, None, 30, 40, 0]))
    assert [b["date"] for b in bars] == ["2026-10-26", "2026-10-28"]
    assert bars[1]["close"] == 102.0 and bars[1]["adj_close"] == 101.0 and bars[1]["volume"] == 40
    # venue-local date: 22:00 UTC with +03:00 (Riyadh) is the next day
    sa = ms.parse_chart(_chart([_ts("2026-10-28", 22, 0)], [11000.0], gmtoffset=10800))
    assert sa[0]["date"] == "2026-10-29" and sa[0]["adj_close"] is None
    # unordered input comes back sorted; garbage payloads are empty
    rev = ms.parse_chart(_chart([_ts("2026-10-28"), _ts("2026-10-27")], [2.0, 1.0]))
    assert [b["date"] for b in rev] == ["2026-10-27", "2026-10-28"]
    for bad in ({}, {"chart": {"result": None}}, {"chart": {"result": [None]}}, None, "x"):
        assert ms.parse_chart(bad) == []


# --------------------------------------------------------------------------- M3
def test_m3_open_session_left_out():
    ts = [_ts("2026-10-28", 13, 30), _ts("2026-10-29", 13, 30)]
    period = {"start": _ts("2026-10-29", 13, 30), "end": _ts("2026-10-29", 20, 0)}
    inside = _ts("2026-10-29", 15, 0)
    after = _ts("2026-10-29", 22, 35)
    assert [b["date"] for b in ms.parse_chart(_chart(ts, [1.0, 2.0], period=period), inside)] == ["2026-10-28"]
    assert len(ms.parse_chart(_chart(ts, [1.0, 2.0], period=period), after)) == 2
    assert len(ms.parse_chart(_chart(ts, [1.0, 2.0], period=period))) == 2      # no clock given
    bad = {"start": "x", "end": None}
    assert len(ms.parse_chart(_chart(ts, [1.0, 2.0], period=bad), inside)) == 2


# --------------------------------------------------------------------------- M4
class _Body(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_m4_fetch_series_hosts():
    seen = []
    full = json.dumps(_chart([_ts(d["date"]) for d in _days(35)], [5.0] * 35)).encode()

    def opener(req, timeout):
        seen.append(req.full_url)
        if "query1" in req.full_url:
            raise urllib.error.URLError("reset")
        return _Body(full)

    slept = []
    bars = ms.fetch_series("^TASI.SR", 111, 222, None, opener=opener, sleep=slept.append)
    assert len(bars) == 35 and len(seen) == 2 and slept == [1.5]
    assert "/v8/finance/chart/%5ETASI.SR?period1=111&period2=222&interval=1d" in seen[1]
    assert "events=div%2Csplits" in seen[1] and "includeAdjustedClose=true" in seen[1]
    assert not any("interval=1h" in u for u in seen)            # full daily answer: no hourly call
    assert "%3DF" in ms.YAHOO_PATH.format(sym=ms.urllib.parse.quote("BZ=F", safe=""), p1=1, p2=2)

    def not_found(req, timeout):
        seen.append(req.full_url)
        raise urllib.error.HTTPError(req.full_url, 404, "nf", {}, None)

    seen.clear()
    assert ms.fetch_series("NOPE", 1, 2, None, opener=not_found, sleep=slept.append) == []
    assert len(seen) == 2 and "interval=1h" in seen[1]           # one try per resolution, 404 final

    def empty(req, timeout):
        return _Body(json.dumps({"chart": {"result": None}}).encode())

    assert ms.fetch_series("X", 1, 2, None, opener=empty, sleep=lambda s: None) == []


# --------------------------------------------------------------------------- M5
def test_m5_rows_and_ndjson():
    rec = ms.series_records("BZ=F")[0]
    rows = ms.build_rows(rec, _bars("2026-10-27", "2026-10-28"), "2026-10-29T22:35:09.123456Z", "")
    assert [set(r) for r in rows] == [{f["name"] for f in ms.SCHEMA}] * 2
    assert rows[0]["country"] is None and rows[0]["run_id"] is None and rows[0]["sector"] == "Energy"
    assert {r["series_id"] for r in rows} == {60} and rows[1]["close"] == 11.0
    lines = ms.ndjson(rows).decode("utf-8").splitlines()
    assert len(lines) == 2 and json.loads(lines[0])["symbol"] == "BZ=F"
    required = [f["name"] for f in ms.SCHEMA if f["mode"] == "REQUIRED"]
    assert all(rows[0][k] is not None for k in required)


# --------------------------------------------------------------------------- M6
def test_m6_table_layout():
    live = _live_table()
    assert ms.schema_problems(live) == []
    t = json.loads(json.dumps(live))
    t["schema"]["fields"][12]["mode"] = "NULLABLE"            # close
    assert any("columns differ" in p for p in ms.schema_problems(t))
    t = json.loads(json.dumps(live))
    t["schema"]["fields"][9]["type"] = "STRING"               # open
    assert ms.schema_problems(t)
    t = json.loads(json.dumps(live))
    t["schema"]["fields"].pop()
    assert ms.schema_problems(t)
    t = json.loads(json.dumps(live))
    t.pop("rangePartitioning")
    t["timePartitioning"] = {"type": "DAY", "field": "date"}
    assert any("range-partitioned" in p for p in ms.schema_problems(t))
    # FLOAT64 / INT64 spellings are the same types
    t = json.loads(json.dumps(live))
    for f in t["schema"]["fields"]:
        f["type"] = {"FLOAT": "FLOAT64", "INTEGER": "INT64"}.get(f["type"], f["type"])
    assert ms.schema_problems(t) == []

    s = FakeBQ(table=None)
    assert ms.ensure_table(s, PROJECT, DATASET) is True
    body = s.created[0]
    assert body["rangePartitioning"] == {"field": "series_id", "range": {"start": "1", "end": "1001", "interval": "1"}}
    assert body["schema"]["fields"] == ms.SCHEMA and "timePartitioning" not in body
    assert ms.ensure_table(FakeBQ(table=live), PROJECT, DATASET) is False
    bad = json.loads(json.dumps(live))
    bad["schema"]["fields"][0]["name"] = "sid"
    with pytest.raises(RuntimeError, match="differs from SCHEMA"):
        ms.ensure_table(FakeBQ(table=bad), PROJECT, DATASET)


# --------------------------------------------------------------------------- M7
def test_m7_write_loads_each_partition_and_isolates_failures(monkeypatch, capsys):
    attempts = {}

    def fetch(symbol, p1, p2, now_ts):
        attempts[symbol] = attempts.get(symbol, 0) + 1
        assert now_ts == int(NOW.timestamp()) and p2 == now_ts + 86400
        assert p1 == int((NOW - dt.timedelta(days=1100)).timestamp())
        if symbol == "^HSI" and attempts[symbol] == 1:
            raise RuntimeError("Yahoo 429")
        if symbol == "QAT":
            return []
        return _days(40)

    s = FakeBQ(table=_live_table(), polls=2)
    rc, slept = _run(monkeypatch, ["--write", "--symbols", "^GSPC,^HSI,QAT,KSA"], s, fetch)
    out = capsys.readouterr()
    assert rc == 1
    assert attempts == {"^GSPC": 1, "^HSI": 2, "QAT": 2, "KSA": 1}
    assert sorted(s.uploads) == [1, 11, 24]                    # QAT (26) untouched
    for job in s.jobs.values():
        load = job["config"]["configuration"]["load"]
        assert load["destinationTable"]["tableId"] == f"{ms.TABLE}${job['sid']}"
        assert load["writeDisposition"] == "WRITE_TRUNCATE" and load["createDisposition"] == "CREATE_NEVER"
        assert load["schema"]["fields"] == ms.SCHEMA
        assert {r["series_id"] for r in s.uploads[job["sid"]]} == {job["sid"]}
        assert job["config"]["jobReference"]["jobId"].startswith(f"tfb_market_{job['sid']}_777_")
    assert all(r["run_id"] == "777" and r["source"] == "yahoo_chart_v8" for r in s.uploads[1])
    assert "3/4 series" in out.out and "FAILED (left as they were): QAT" in out.out
    assert 20.0 in slept and not s.created                    # retry wait; table existed

    s = FakeBQ(table=_live_table(), job_error={24: "Provided Schema does not match"})
    rc, _ = _run(monkeypatch, ["--write", "--symbols", "^GSPC,KSA"], s,
                 lambda sym, p1, p2, now_ts: _days(40))
    assert rc == 1 and 1 in s.uploads and "FAILED (left as they were): KSA" in capsys.readouterr().out

    s = FakeBQ(table=None)
    rc, _ = _run(monkeypatch, ["--write", "--symbols", "^GSPC"], s,
                 lambda sym, p1, p2, now_ts: _days(40))
    assert rc == 0 and len(s.created) == 1 and "table created" in capsys.readouterr().out


# --------------------------------------------------------------------------- M8
def test_m8_dry_run(monkeypatch, capsys):
    s = FakeBQ(table=None)
    rc, _ = _run(monkeypatch, ["--symbols", "^GSPC,^TASI.SR"], s, lambda sym, p1, p2, now_ts: _days(40))
    assert rc == 0
    assert [c for c in s.calls] == [("GET", f"{ms.BQ_API}/projects/{PROJECT}/datasets/{DATASET}")]
    assert not s.jobs and not s.created
    assert "dry run, nothing loaded" in capsys.readouterr().out


# --------------------------------------------------------------------------- M9
def test_m9_main_configuration(monkeypatch):
    def never(*a, **k):
        raise AssertionError("no request expected")

    class Boom:
        get = post = put = never

    monkeypatch.setattr(ms, "make_session", never)
    _env(monkeypatch, gate="off")
    assert ms.main([], session=None, now=NOW, fetch=never) == 0
    monkeypatch.delenv("TFB_BQ_MARKETS")
    assert ms.main([], session=None, now=NOW, fetch=never) == 0
    _env(monkeypatch, project=None)
    assert ms.main([], session=Boom(), now=NOW, fetch=never) == 2
    _env(monkeypatch)
    assert ms.main(["--days", "5"], session=Boom(), now=NOW, fetch=never) == 2
    assert ms.main(["--symbols", "NOPE"], session=Boom(), now=NOW, fetch=never) == 2
    assert ms.main(["--write"], session=FakeBQ(dataset=False), now=NOW, fetch=never) == 2

    def no_key():
        raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS missing or unreadable")

    monkeypatch.setattr(ms, "make_session", no_key)
    assert ms.main(["--write"], session=None, now=NOW, fetch=never) == 2
    bad = _live_table()
    bad["schema"]["fields"][3]["mode"] = "NULLABLE"
    assert ms.main(["--write"], session=FakeBQ(table=bad), now=NOW, fetch=never) == 2


# --------------------------------------------------------------------------- M10
def test_m10_workflow():
    wf = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    on = wf.get(True) or wf.get("on")
    assert on["schedule"] == [{"cron": "35 22 * * *"}]
    inputs = on["workflow_dispatch"]["inputs"]
    assert inputs["write"]["default"] is False and inputs["write"]["type"] == "boolean"
    assert inputs["days"]["default"] == "1100" and inputs["symbols"]["default"] == ""
    assert wf["permissions"] == {"contents": "read"}
    job = wf["jobs"]["markets"]
    assert int(job["timeout-minutes"]) <= 30
    run = [st for st in job["steps"] if "run the loader" in st.get("name", "").lower()][0]
    env = run["env"]
    assert env["TFB_BQ_MARKETS"] == "${{ vars.TFB_BQ_MARKETS || 'on' }}"
    assert "secrets.GOOGLE_SHEETS_CREDENTIALS" in env["GOOGLE_SHEETS_CREDENTIALS"]
    assert 'MODE="--write"' in run["run"] and 'MODE="--dry-run"' in run["run"]
    assert "scripts/archive_market_series.py $MODE" in run["run"] and 'exit "$rc"' in run["run"]
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "actions/checkout@v6" in text and "actions/setup-python@v6" in text


# --------------------------------------------------------------------------- M11
def test_m11_no_workbook_write_no_dml():
    src = (ROOT / "scripts" / "archive_market_series.py").read_text(encoding="utf-8")
    assert "sheets.googleapis" not in src and "gspread" not in src
    code = "\n".join(ln for ln in src.splitlines() if not ln.lstrip().startswith("#"))
    for word in ("DELETE FROM", "INSERT INTO", "UPDATE `", "MERGE `", "DROP TABLE", "TRUNCATE TABLE"):
        assert word not in code, word
    assert '"tableId": f"{TABLE}${series_id}"' in src
    assert '"writeDisposition": "WRITE_TRUNCATE"' in code and '"createDisposition": "CREATE_NEVER"' in src


# --------------------------------------------------------------------------- M12
def _hourly(rows, gmtoffset=10800, period=None):
    """rows: (utc 'YYYY-MM-DD HH:MM', open, high, low, close, volume)."""
    ts = [int(dt.datetime.strptime(r[0], "%Y-%m-%d %H:%M").replace(tzinfo=dt.timezone.utc).timestamp())
          for r in rows]
    q = {"open": [r[1] for r in rows], "high": [r[2] for r in rows], "low": [r[3] for r in rows],
         "close": [r[4] for r in rows], "volume": [r[5] for r in rows]}
    meta = {"gmtoffset": gmtoffset}
    if period is not None:
        meta["currentTradingPeriod"] = {"regular": period}
    return {"chart": {"result": [{"meta": meta, "timestamp": ts, "indicators": {"quote": [q]}}], "error": None}}


def test_m12_hourly_rebuild():
    rows = [("2026-10-27 07:00", 100.0, 101.0, 99.0, 100.5, 10),
            ("2026-10-27 08:00", 100.5, 103.0, 100.0, 102.0, 20),
            ("2026-10-27 09:00", None, None, None, None, None),      # an empty hour
            ("2026-10-27 11:00", 102.0, 102.5, 98.0, 99.0, 30),
            ("2026-10-28 07:00", 99.0, 99.5, 97.0, 98.0, 5),
            ("2026-10-28 11:00", 98.0, 100.0, 97.5, 99.5, 7),
            ("2026-10-29 07:00", 99.5, 99.9, 99.0, 99.8, 3)]       # today, session open
    period = {"start": _ts("2026-10-29", 7, 0), "end": _ts("2026-10-29", 12, 0)}
    bars = ms.parse_hourly(_hourly(rows, period=period), _ts("2026-10-29", 9, 0))
    assert [b["date"] for b in bars] == ["2026-10-27", "2026-10-28"]
    d = bars[0]
    assert (d["open"], d["high"], d["low"], d["close"], d["volume"]) == (100.0, 103.0, 98.0, 99.0, 60)
    assert d["adj_close"] is None and d["source"] == "yahoo_chart_v8_1h"
    assert len(ms.parse_hourly(_hourly(rows, period=period))) == 3          # no clock: today kept
    # gmtoffset decides the trading day: 22:00 UTC is the next day in Riyadh
    late = ms.parse_hourly(_hourly([("2026-10-27 22:00", 1.0, 1.0, 1.0, 1.0, 1)]))
    assert late[0]["date"] == "2026-10-28"
    for bad in ({}, {"chart": {"result": None}}, None):
        assert ms.parse_hourly(bad) == []

    # fetch_series: one daily bar -> the hourly rebuild (from p1 on) is used
    hourly_rows = []
    for d in [b["date"] for b in _days(45)]:
        hourly_rows += [(d + " 07:00", 10.0, 11.0, 9.0, 10.0, 1), (d + " 11:00", 10.0, 12.0, 9.5, 11.0, 2)]
    hourly = json.dumps(_hourly(hourly_rows)).encode()
    one_daily = json.dumps(_chart([_ts("2026-10-29")], [12.0])).encode()
    seen = []

    def opener(req, timeout):
        seen.append(req.full_url)
        return _Body(hourly if "interval=1h" in req.full_url else one_daily)

    p1 = int(dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc).timestamp())
    got = ms.fetch_series("^TASI.SR", p1, p1 + 86400 * 90, None, opener=opener, sleep=lambda s: None)
    assert "range=730d&interval=1h" in seen[-1] and "%5ETASI.SR" in seen[-1]
    assert got and all(b["source"] == "yahoo_chart_v8_1h" and b["date"] >= "2026-09-01" for b in got)
    assert all(b["close"] == 11.0 and b["high"] == 12.0 and b["volume"] == 3 for b in got)
    rows_out = ms.build_rows(ms.series_records("^TASI.SR")[0], got, "2026-10-29T22:35:09Z", "9")
    assert {r["source"] for r in rows_out} == {"yahoo_chart_v8_1h"}


# --------------------------------------------------------------------------- M13
def test_m13_short_history_keeps_rows(monkeypatch, capsys):
    def fetch(symbol, p1, p2, now_ts):
        return _bars("2026-10-29") if symbol == "000300.SS" else _days(40)

    s = FakeBQ(table=_live_table())
    rc, _ = _run(monkeypatch, ["--write", "--symbols", "^GSPC,000300.SS"], s, fetch)
    out = capsys.readouterr()
    assert rc == 1 and sorted(s.uploads) == [1]                 # CSI 300 (10) untouched
    assert "only 1 bar(s) from Yahoo" in out.err and "FAILED (left as they were): 000300.SS" in out.out
    assert ms.MIN_BARS == 30


# --------------------------------------------------------------------------- M14
def _window(days):
    """p1, p2 exactly as run() derives them for --days `days` at NOW."""
    now_ts = int(NOW.timestamp())
    return int((NOW - dt.timedelta(days=days)).timestamp()), now_ts + 86400


def _weekdays_since(p1, end="2026-10-29"):
    """The weekdays from p1's date to `end` (a complete short history)."""
    d, out = dt.datetime.fromtimestamp(p1, tz=dt.timezone.utc).date(), []
    while d.isoformat() <= end:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += dt.timedelta(days=1)
    return out


def test_m14_window_aware_minimum(monkeypatch, capsys):
    assert ms.min_bars(*_window(30)) == 11                     # 20-22 trading days in 30
    assert ms.min_bars(*_window(ms.DEFAULT_DAYS)) == ms.MIN_BARS == 30
    assert ms.min_bars(*_window(ms.MAX_DAYS)) == 30
    assert ms.min_bars(0, 0) == ms.min_bars(1, 2) == ms.min_bars(5, 1) == 2   # never below 2
    steps = [ms.min_bars(*_window(d)) for d in range(ms.MIN_DAYS, 200)]
    assert steps == sorted(steps) and steps[-1] == 30          # grows with the window, capped

    # a complete 30-day answer (21-22 weekday bars) loads every series
    p1, p2 = _window(30)
    full = _bars(*_weekdays_since(p1, end="2026-10-28"))
    assert 20 <= len(full) <= 22 and len(full) < ms.MIN_BARS

    def fetch(symbol, a, b, now_ts):
        assert (a, b) == (p1, p2)
        return _bars("2026-10-28") if symbol == "000300.SS" else full

    s = FakeBQ(table=_live_table())
    rc, _ = _run(monkeypatch, ["--write", "--days", "30", "--symbols", "^GSPC,^TASI.SR,000300.SS"], s, fetch)
    out = capsys.readouterr()
    assert rc == 1 and sorted(s.uploads) == [1, 13]            # CSI 300 (one bar) untouched
    assert all(len(v) == len(full) for v in s.uploads.values())
    assert "2/3 series" in out.out and "last 30 days" in out.out
    assert "only 1 bar(s) from Yahoo, daily and hourly, 11 needed" in out.err

    rc, _ = _run(monkeypatch, ["--write", "--days", "30", "--symbols", "^GSPC,KSA"], FakeBQ(table=_live_table()),
                 lambda sym, a, b, now_ts: full)
    assert rc == 0 and "2/2 series" in capsys.readouterr().out

    # fetch_series on the 30-day window: a full daily answer asks no hourly bars ...
    daily = json.dumps(_chart([_ts(d["date"]) for d in full], [5.0] * len(full))).encode()
    hourly_rows = []
    for d in _weekdays_since(p1 - 86400 * 20, end="2026-10-28"):
        hourly_rows += [(d + " 07:00", 10.0, 11.0, 9.0, 10.0, 1), (d + " 11:00", 10.0, 12.0, 9.5, 11.0, 2)]
    hourly = json.dumps(_hourly(hourly_rows)).encode()
    seen = []

    def opener(req, timeout):
        seen.append(req.full_url)
        return _Body(hourly if "interval=1h" in req.full_url else daily)

    got = ms.fetch_series("^GSPC", p1, p2, int(NOW.timestamp()), opener=opener, sleep=lambda s: None)
    assert len(got) == len(full) and not any("interval=1h" in u for u in seen)

    # ... and one daily bar is rebuilt from hourly bars inside the window, which then loads
    one_daily = json.dumps(_chart([_ts("2026-10-28")], [12.0])).encode()
    seen.clear()

    def opener_one(req, timeout):
        seen.append(req.full_url)
        return _Body(hourly if "interval=1h" in req.full_url else one_daily)

    first_day = dt.datetime.fromtimestamp(p1, tz=dt.timezone.utc).strftime("%Y-%m-%d")
    got = ms.fetch_series("^TASI.SR", p1, p2, int(NOW.timestamp()), opener=opener_one, sleep=lambda s: None)
    assert "range=730d&interval=1h" in seen[-1]
    assert ms.min_bars(p1, p2) <= len(got) < ms.MIN_BARS
    assert all(b["source"] == "yahoo_chart_v8_1h" and b["date"] >= first_day for b in got)
    s = FakeBQ(table=_live_table())
    rc, _ = _run(monkeypatch, ["--write", "--days", "30", "--symbols", "^TASI.SR"], s,
                 lambda sym, a, b, now_ts: got)
    assert rc == 0 and len(s.uploads[13]) == len(got)
    assert "(rebuilt from hourly bars)" in capsys.readouterr().err


def test_m14_boundary_and_closure_months(monkeypatch, capsys):
    p1, p2 = _window(30)
    days = _weekdays_since(p1, end="2026-10-28")              # 22 weekdays
    need = ms.min_bars(p1, p2)
    assert need == 11 and len(days) == 22
    empty = json.dumps({"chart": {"result": None}}).encode()

    # the hourly trigger: 10 daily bars ask for hourly bars, 11 do not
    for n, asks in ((need - 1, True), (need, False)):
        daily = json.dumps(_chart([_ts(d) for d in days[-n:]], [5.0] * n)).encode()
        seen = []

        def opener(req, timeout, daily=daily, seen=seen):
            seen.append(req.full_url)
            return _Body(daily if "interval=1d" in req.full_url else empty)

        got = ms.fetch_series("^GSPC", p1, p2, int(NOW.timestamp()), opener=opener, sleep=lambda s: None)
        assert len(got) == n and any("interval=1h" in u for u in seen) is asks

    # the refusal: 10 bars keep the rows, 11 load
    for n, rc_want, loaded in ((need - 1, 1, []), (need, 0, [1])):
        s = FakeBQ(table=_live_table())
        rc, _ = _run(monkeypatch, ["--write", "--days", "30", "--symbols", "^GSPC"], s,
                     lambda sym, a, b, now_ts, n=n: _bars(*days[-n:]))
        assert rc == rc_want and sorted(s.uploads) == loaded
    assert "only 10 bar(s) from Yahoo, daily and hourly, 11 needed" in capsys.readouterr().err

    # months with a long closure still load: 12 sessions (the fewest the review
    # found in a real 30-day window - Shanghai, Mid-Autumn and Golden Week 2010)
    # and 13 sessions on a Sun-Thu week (Tadawul, Eid al-Fitr 2022)
    closed = [d for d in days if d >= "2026-10-13"]
    sun_thu = []
    d = dt.datetime.fromtimestamp(p1, tz=dt.timezone.utc).date()
    while d.isoformat() <= "2026-10-28":
        if d.weekday() in (6, 0, 1, 2, 3) and d.isoformat() >= "2026-10-12":
            sun_thu.append(d.isoformat())
        d += dt.timedelta(days=1)
    assert (len(closed), len(sun_thu)) == (12, 13)
    s = FakeBQ(table=_live_table())
    rc, _ = _run(monkeypatch, ["--write", "--days", "30", "--symbols", "000001.SS,^TASI.SR"], s,
                 lambda sym, a, b, now_ts: _bars(*(sun_thu if sym == "^TASI.SR" else closed)))
    assert rc == 0 and len(s.uploads[8]) == 12 and len(s.uploads[13]) == 13
    assert "2/2 series" in capsys.readouterr().out
