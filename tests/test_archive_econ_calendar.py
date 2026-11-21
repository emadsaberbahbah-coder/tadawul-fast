"""scripts/archive_econ_calendar.py v1.0.0 - the EODHD economic calendar of six
economies, history included, appended to BigQuery as a change log (one row per
release per change) for the forward event scenarios (owner, 21 Nov 2026, item 5
of the build list).

REAL module; the stubs are the EODHD opener (economic-events and /api/user) and
the HTTP session (a fake answering BigQuery datasets, tables, the resumable
stage load and query jobs offline). No network, no google libraries.

E1  parse_events: the key is the news gate's key, built the same way from the
    same payload; figures are finite numbers or None (bool, text, NaN, inf
    refused); a row without a valid date, an event name or a country is
    dropped; one key keeps the row that knows more
E2  windows: back-to-back, no overlap, at most WINDOW_DAYS days, the last one
    ends on the end day; end before start refused
E3  fetch_window: each request asks one day wider on each side, 1,000 a page,
    the offset; a full first page brings a second; a full second page cuts the
    window in two; one retry on a failure; the token never in an error;
    --max-calls stops the reading
E4  the budget guard: /api/user is read first; a room smaller than the plan,
    or an unreadable limit -> the run is SKIPPED (exit 0) and no
    economic-events call is made; the room says yes exactly when
    archive_symbol_deep.budget_check does; every call, retries and the halves
    of a cut window included, stays inside the room (Codex P2 on #1049)
E5  dry run: EODHD read and counted, BigQuery only for the dataset check; a
    failed window and the windows past the cap fail the run (exit 1)
E6  write: econ_calendar is created (month partitions on event_at, clustered
    by country and event); the stage is replaced (WRITE_TRUNCATE) with this
    run's releases; one INSERT appends the changes; the windows that failed or
    came back empty are not covered (nothing gone there)
E7  the INSERT: new / changed (null-safe on the time and every figure) / gone
    (inside a covered window, absent from this read); no covered window ->
    nothing can be gone; the run id sanitised; the table layout is checked
E8  main: gate off (no request at all), missing project or key, bad dates, a
    window out of range, bad countries, bad --max-calls, bad share
E9  the workflow: three schedules, a dry-run dispatch by default, read-only
    repository, the gate armed with a kill switch, the EODHD secret, pinned
    actions
E10 nothing here writes to the workbook; the only DML is the INSERT into
    econ_calendar; the token never reaches the output
"""
from __future__ import annotations

import datetime as dt
import io
import json
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import archive_econ_calendar as ec  # noqa: E402
from scripts import news_macro_gate as ng  # noqa: E402

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 21, 14, 10, 7, 654321, tzinfo=UTC)
PROJECT, DATASET = "proj-x", "tfb_archive"
TOKEN = "SECRET123TOKEN"
WORKFLOW = ROOT / ".github" / "workflows" / "bq_econ_calendar.yml"


# ------------------------------------------------------------------ fakes
class _Body(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class HTTPErr(Exception):
    def __init__(self, code):
        super().__init__(f"HTTP Error {code}: https://eodhd.com/api/economic-events?api_token={TOKEN}")
        self.code = code


def ev(day, hhmm="13:30", country="US", typ="Non Farm Payrolls", comp=None, period="Oct", actual=None,
       estimate=None, previous=None):
    return {"type": typ, "comparison": comp, "period": period, "country": country, "date": f"{day} {hhmm}:00",
            "actual": actual, "previous": previous, "estimate": estimate, "change": None,
            "change_percentage": None}


def make_opener(events, used=1000, limit=100000, user_fail=False, fail=None, calls=None):
    """events(country, frm, to, offset) -> list; fail: set of (country, frm) that answer HTTP 500."""
    def opener(req, timeout):
        url = req.full_url
        path = urlparse(url).path
        q = {k: v[0] for k, v in parse_qs(urlparse(url).query).items()}
        assert q.pop("api_token") == TOKEN and q.pop("fmt") == "json"
        if calls is not None:
            calls.append((path, q))
        if path.endswith("/api/user"):
            if user_fail:
                raise HTTPErr(500)
            return _Body(json.dumps({"apiRequests": used, "dailyRateLimit": limit,
                                     "apiRequestsDate": "2026-11-21"}).encode())
        assert path.endswith("/api/economic-events"), path
        if fail and (q["country"], q["from"]) in fail:
            raise HTTPErr(500)
        got = events(q["country"], q["from"], q["to"], int(q["offset"]))
        assert q["limit"] == "1000"
        return _Body(json.dumps(got).encode())
    return opener


class Resp:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code = status
        self._body = body if body is not None else {}
        self.headers = headers or {}

    def json(self):
        return self._body


class FakeBQ:
    """Offline BigQuery: datasets.get, tables.get / insert, the resumable stage
    load, query jobs (DONE at once) and their results."""

    def __init__(self, table=None, dataset=True, dml_rows=7, breakdown=(("changed", "2"), ("new", "5")),
                 insert_error=None):
        self.table = table
        self.dataset = dataset
        self.dml_rows = dml_rows
        self.breakdown = breakdown
        self.insert_error = insert_error
        self.calls, self.created, self.loads, self.queries = [], [], [], []
        self.uploads = {}

    def get(self, url, timeout=None, **_):
        self.calls.append(("GET", url))
        if url.endswith(f"/datasets/{DATASET}"):
            return Resp(200 if self.dataset else 404, {})
        if url.endswith(f"/tables/{ec.TABLE}"):
            return Resp(200, self.table) if self.table is not None else Resp(404, {})
        if "/queries/" in url:
            return Resp(200, {"rows": [{"f": [{"v": s}, {"v": n}]} for s, n in self.breakdown]})
        raise AssertionError("unexpected GET " + url)

    def post(self, url, json=None, timeout=None, headers=None, **_):
        self.calls.append(("POST", url))
        if url.endswith(f"/datasets/{DATASET}/tables"):
            self.created.append(json)
            self.table = {"schema": json["schema"], "timePartitioning": json["timePartitioning"]}
            return Resp(200, json)
        if url.startswith(ec.BQ_UPLOAD) and "uploadType=resumable" in url:
            self.loads.append(json)
            return Resp(200, {}, {"Location": "https://upload.invalid/" + json["jobReference"]["jobId"]})
        if url.endswith(f"/projects/{PROJECT}/jobs"):
            sql = json["configuration"]["query"]["query"]
            self.queries.append(sql)
            ref = json["jobReference"]
            if sql.startswith("INSERT") and self.insert_error:
                return Resp(200, {"jobReference": ref,
                                  "status": {"state": "DONE", "errorResult": {"message": self.insert_error}}})
            stats = {"query": {"numDmlAffectedRows": str(self.dml_rows)}} if sql.startswith("INSERT") else {}
            return Resp(200, {"jobReference": ref, "status": {"state": "DONE"}, "statistics": stats})
        raise AssertionError("unexpected POST " + url)

    def put(self, url, data=None, timeout=None, headers=None, **_):
        self.calls.append(("PUT", url))
        self.uploads[url.rsplit("/", 1)[1]] = [json.loads(ln) for ln in data.decode("utf-8").split("\n") if ln]
        return Resp(200, {"status": {"state": "DONE"}})


def _env(monkeypatch, gate="on", project=PROJECT, key=TOKEN, share=None):
    monkeypatch.setenv("TFB_BQ_ECON", gate)
    for name, val in (("TFB_BQ_PROJECT", project), ("EODHD_API_KEY", key), ("TFB_ECON_EODHD_MAX_SHARE", share)):
        if val is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, val)
    monkeypatch.setenv("TFB_BQ_DATASET", DATASET)
    monkeypatch.setenv("TFB_BQ_LOCATION", "US")
    monkeypatch.setenv("GITHUB_RUN_ID", "4242")


def one_window(country, frm, to, offset):
    if offset:
        return []
    if country == "US":
        return [ev("2026-11-20", typ="Core Inflation Rate", comp="mom", actual=0.3, estimate=0.2, previous=0.2),
                ev("2026-12-04", typ="Non Farm Payrolls", estimate=55.0, previous=119.0)]
    if country == "SA":
        return [ev("2026-11-23", hhmm="06:00", country="SA", typ="Inflation Rate", comp="yoy", estimate=2.1)]
    return []


def _main(monkeypatch, argv, session, opener, **env):
    _env(monkeypatch, **env)
    slept = []
    rc = ec.main(argv, session=session, now=NOW, opener=opener, sleep=slept.append)
    return rc, slept


# ------------------------------------------------------------------ E1
def test_e1_parse_matches_the_gate_key():
    payload = [
        ev("2026-11-20", typ="Core Inflation Rate", comp="mom", actual=0.3, estimate=0.2, previous=0.2),
        ev("2026-11-20", hhmm="13:30", typ="Inflation Rate", comp="yoy", period="Oct", actual="3.1", estimate=3.0),
        ev("2026-11-23", hhmm="06:00", country="sa", typ="Inflation Rate", comp="yoy", estimate=2.1),
        ev("2026-11-25", typ="GDP Growth Rate", comp="qoq", period="Q3", actual=True, estimate=float("nan"),
           previous="n/a"),
    ]
    rows = ec.parse_events(payload)
    gate = ng.parse_econ_events(payload)
    assert sorted(r["event_key"] for r in rows) == sorted(g["key"] for g in gate)
    by = {r["event_key"]: r for r in rows}
    k = "2026-11-20 13:30|US|Core Inflation Rate|mom|Oct"
    assert by[k]["actual"] == 0.3 and by[k]["estimate"] == 0.2 and by[k]["previous"] == 0.2
    assert by[k]["event_at"] == dt.datetime(2026, 11, 20, 13, 30, tzinfo=UTC)
    assert by["2026-11-20 13:30|US|Inflation Rate|yoy|Oct"]["actual"] == 3.1      # numeric text is a figure
    assert by["2026-11-23 06:00|SA|Inflation Rate|yoy|Oct"]["country"] == "SA"
    gdp = by["2026-11-25 13:30|US|GDP Growth Rate|qoq|Q3"]
    assert gdp["actual"] is None and gdp["estimate"] is None and gdp["previous"] is None
    assert [r["event_key"] for r in rows] == sorted(by)
    # comparison / period blank -> NULL, the key keeps the empty field like the gate's
    nc = ec.parse_events([ev("2026-11-20", typ="Non Farm Payrolls", comp=None, period=None, actual=64)])[0]
    assert nc["comparison"] is None and nc["period"] is None and nc["event_key"] == "2026-11-20 13:30|US|Non Farm Payrolls||"


def test_e1_drops_and_keeps_the_fuller_row():
    bad = [{"type": "CPI", "country": "US", "date": "2026-11-20"}, {"type": "", "country": "US",
           "date": "2026-11-20 13:30:00"}, {"type": "CPI", "country": "", "date": "2026-11-20 13:30:00"},
           "x", None, {"type": "CPI", "country": "US", "date": None}]
    assert ec.parse_events(bad) == [] and ec.parse_events({"error": "x"}) == [] and ec.parse_events(None) == []
    early = ev("2026-11-20", typ="CPI", estimate=0.2)
    late = ev("2026-11-20", typ="CPI", actual=0.3, estimate=0.2)
    for order in ([early, late], [late, early]):
        (row,) = ec.parse_events(order)
        assert row["actual"] == 0.3
    assert ec._num(float("inf")) is None and ec._num("1e3") == 1000.0 and ec._num(" -0.5 ") == -0.5
    assert ec._num(False) is None and ec._num("") is None and ec._num(7) == 7.0


# ------------------------------------------------------------------ E2
def test_e2_windows():
    w = ec.windows(dt.date(2023, 11, 1), dt.date(2024, 2, 10))
    assert w[0][0] == dt.date(2023, 11, 1) and w[-1][1] == dt.date(2024, 2, 10)
    for (a, b), (c, _) in zip(w, w[1:]):
        assert c == b + dt.timedelta(days=1)
    assert all((b - a).days + 1 <= ec.WINDOW_DAYS for a, b in w)
    assert ec.windows(dt.date(2026, 11, 21), dt.date(2026, 11, 21)) == [(dt.date(2026, 11, 21),) * 2]
    with pytest.raises(ValueError):
        ec.windows(dt.date(2026, 11, 22), dt.date(2026, 11, 21))


# ------------------------------------------------------------------ E3
def test_e3_request_shape_and_paging():
    calls = []

    def dense(country, frm, to, offset):
        # 1,000 on the first page, 400 on the second
        n = 1000 if offset == 0 else 400
        return [ev("2026-11-%02d" % (1 + i % 28), hhmm="%02d:%02d" % (i // 60 % 24, i % 60), typ=f"T{offset + i}")
                for i in range(n)]
    raw = ec.fetch_window(TOKEN, "US", dt.date(2026, 11, 1), dt.date(2026, 11, 30), make_opener(dense, calls=calls),
                          ec.Calls(10), sleep=lambda s: None)
    assert len(raw) == 1400
    assert [(q["from"], q["to"], q["offset"]) for _, q in calls] == [("2026-10-31", "2026-12-01", "0"),
                                                                    ("2026-10-31", "2026-12-01", "1000")]
    assert all(q["country"] == "US" and q["limit"] == "1000" for _, q in calls)


def test_e3_full_second_page_cuts_the_window():
    calls = []

    def very_dense(country, frm, to, offset):
        days = (dt.date.fromisoformat(to) - dt.date.fromisoformat(frm)).days - 1   # the window's own days
        n = 1000 if days > 10 else 300
        return [ev("2026-11-15", hhmm="%02d:%02d" % (i // 60 % 24, i % 60), typ=f"T{frm}{offset}{i}")
                for i in range(n)]
    raw = ec.fetch_window(TOKEN, "US", dt.date(2026, 11, 1), dt.date(2026, 11, 30),
                          make_opener(very_dense, calls=calls), ec.Calls(50), sleep=lambda s: None)
    asked = [(q["from"], q["to"], q["offset"]) for _, q in calls]
    # 1..30 is full twice -> 1..15 and 16..30, full twice each -> 1..8, 9..15, 16..23, 24..30 (300 each);
    # every window is asked one day wider on each side
    assert asked == [("2026-10-31", "2026-12-01", "0"), ("2026-10-31", "2026-12-01", "1000"),
                     ("2026-10-31", "2026-11-16", "0"), ("2026-10-31", "2026-11-16", "1000"),
                     ("2026-10-31", "2026-11-09", "0"), ("2026-11-08", "2026-11-16", "0"),
                     ("2026-11-15", "2026-12-01", "0"), ("2026-11-15", "2026-12-01", "1000"),
                     ("2026-11-15", "2026-11-24", "0"), ("2026-11-23", "2026-12-01", "0")]
    assert len(raw) == 4 * 300

    def always_full(country, frm, to, offset):
        return [ev("2026-11-15", typ=f"T{offset}{i}") for i in range(1000)]
    with pytest.raises(RuntimeError, match="more than 2000 releases in one day"):
        ec.fetch_window(TOKEN, "US", dt.date(2026, 11, 15), dt.date(2026, 11, 15), make_opener(always_full),
                        ec.Calls(50), sleep=lambda s: None)


def test_e3_one_retry_no_token_and_the_cap():
    calls, slept = [], []
    opener = make_opener(lambda *a: [], fail={("US", "2026-10-31")}, calls=calls)
    with pytest.raises(RuntimeError) as err:
        ec.fetch_window(TOKEN, "US", dt.date(2026, 11, 1), dt.date(2026, 11, 30), opener, ec.Calls(10),
                        sleep=slept.append, retry_wait=7.0)
    assert TOKEN not in str(err.value) and "HTTP 500" in str(err.value)
    assert len([c for c in calls if c[0].endswith("economic-events")]) == 2 and slept == [7.0]
    calls2 = ec.Calls(1)
    with pytest.raises(ec.CapReached):
        ec.fetch_window(TOKEN, "US", dt.date(2026, 11, 1), dt.date(2026, 11, 30),
                        make_opener(lambda *a: [ev("2026-11-02", typ=f"T{i}") for i in range(1000)]), calls2,
                        sleep=lambda s: None)
    assert calls2.used == 1


# ------------------------------------------------------------------ E4
@pytest.mark.parametrize("kw", [{"used": 59990, "limit": 100000}, {"user_fail": True}, {"limit": 0}])
def test_e4_budget_guard_skips(monkeypatch, capsys, kw):
    calls = []
    s = FakeBQ()
    rc, _ = _main(monkeypatch, ["--write"], s, make_opener(one_window, calls=calls, **kw))
    out = capsys.readouterr()
    assert rc == 0 and "SKIPPED" in out.out and "nothing read, nothing written" in out.out
    assert [c for c in calls if c[0].endswith("economic-events")] == []
    assert not s.loads and not s.queries and TOKEN not in out.out + out.err


@pytest.mark.parametrize("used,limit,dated", [(0, 100000, "2026-11-21"), (59990, 100000, "2026-11-21"),
                                              (59990, 100000, "2026-11-20"), (590, 1000, "2026-11-21"),
                                              (599, 1000, None), (0, 0, "2026-11-21")])
def test_e4_room_follows_the_shared_rule(used, limit, dated):
    """The room says yes to a plan exactly when archive_symbol_deep.budget_check does."""
    def opener(req, timeout):
        user = {"apiRequests": used, "dailyRateLimit": limit}
        if dated is not None:
            user["apiRequestsDate"] = dated
        return _Body(json.dumps(user).encode())
    import archive_symbol_deep as asd
    room, _ = ec.budget_room(TOKEN, 0.60, opener, today="2026-11-21")
    for planned in (1, 2, 10, 600):
        ok, _ = asd.budget_check(TOKEN, planned, 0.60, opener, today="2026-11-21")
        assert ok == (room is not None and room >= planned), (used, limit, dated, planned)


def test_e4_retries_and_splits_stay_inside_the_room(monkeypatch, capsys):
    """Codex P2 on #1049: 598 of 600 allowed calls used, two planned - a retry must not take the run past 600."""
    calls = []
    s = FakeBQ()
    rc, slept = _main(monkeypatch, ["--write", "--countries", "US"], s,
                      make_opener(one_window, used=598, limit=1000, calls=calls, fail={("US", "2026-11-17")}))
    out = capsys.readouterr()
    eco = [c for c in calls if c[0].endswith("economic-events")]
    assert len(eco) == 2 and rc == 1                       # the room of 2: the first try and its retry, no more
    assert "at most 2 this run" in out.err and "FAILED: US 2026-11-18..2026-12-12" in out.out
    calls = []
    rc, _ = _main(monkeypatch, ["--countries", "US"], FakeBQ(),
                  make_opener(lambda c, f, t, o: [ev("2026-11-20", typ=f"T{o}{i}") for i in range(1000)],
                              used=597, limit=1000, calls=calls))
    out = capsys.readouterr()
    assert len([c for c in calls if c[0].endswith("economic-events")]) == 3 and rc == 1
    assert "NOT READ (the cap of 3 call(s) spent)" in out.out


# ------------------------------------------------------------------ E5
def test_e5_dry_run(monkeypatch, capsys):
    calls = []
    s = FakeBQ()
    rc, _ = _main(monkeypatch, [], s, make_opener(one_window, calls=calls))
    out = capsys.readouterr()
    assert rc == 0 and "(dry run, nothing written)" in out.out and "3 release(s)" in out.out
    assert "6 EODHD call(s)" in out.out
    eco = [q for p, q in calls if p.endswith("economic-events")]
    assert [q["country"] for q in eco] == list(ec.COUNTRIES)
    assert {(q["from"], q["to"]) for q in eco} == {("2026-11-17", "2026-12-13")}   # 18 Nov..12 Dec, a day wider
    assert [c for c in s.calls if "/tables" in c[1] or "/jobs" in c[1]] == []
    assert "US: 1 window(s), 2 release(s), 1 with a figure, 2 with a forecast" in out.err
    assert "US Core Inflation Rate [mom]: 1" in out.err


def test_e5_failed_window_and_cap_fail_the_run(monkeypatch, capsys):
    s = FakeBQ()
    rc, _ = _main(monkeypatch, ["--countries", "US,SA"], s, make_opener(one_window, fail={("SA", "2026-11-17")}))
    out = capsys.readouterr()
    assert rc == 1 and "1 window(s) FAILED: SA 2026-11-18..2026-12-12" in out.out and TOKEN not in out.out + out.err
    rc, _ = _main(monkeypatch, ["--countries", "US,SA,JP", "--max-calls", "2"], FakeBQ(), make_opener(one_window))
    out = capsys.readouterr()
    assert rc == 1 and "1 window(s) NOT READ (the cap of 2 call(s) spent)" in out.out


# ------------------------------------------------------------------ E6
def test_e6_write(monkeypatch, capsys):
    s = FakeBQ()
    rc, _ = _main(monkeypatch, ["--write", "--countries", "US,SA,JP"], s, make_opener(one_window))
    out = capsys.readouterr()
    assert rc == 0, out
    (made,) = s.created
    assert made["timePartitioning"] == {"type": "MONTH", "field": "event_at"}
    assert made["clustering"] == {"fields": ["country", "event_type"]} and made["schema"]["fields"] == ec.SCHEMA
    (load,) = s.loads
    cfg = load["configuration"]["load"]
    assert cfg["destinationTable"]["tableId"] == ec.STAGE and cfg["writeDisposition"] == "WRITE_TRUNCATE"
    assert cfg["createDisposition"] == "CREATE_IF_NEEDED" and cfg["schema"]["fields"] == ec.STAGE_SCHEMA
    (rows,) = s.uploads.values()
    assert [r["event_key"] for r in rows] == sorted(r["event_key"] for r in rows) and len(rows) == 3
    assert set(rows[0]) == set(ec.STAGE_COLUMNS)
    assert all(r["fetched_at"] == "2026-11-21T14:10:07.654321Z" and r["run_id"] == "4242"
               and r["source"] == ec.SOURCE for r in rows)
    nfp = [r for r in rows if r["event_type"] == "Non Farm Payrolls"][0]
    assert nfp["event_at"] == "2026-12-04T13:30:00.000000Z" and nfp["actual"] is None and nfp["estimate"] == 55.0
    insert, breakdown = s.queries
    assert insert.startswith(f"INSERT INTO `{PROJECT}.{DATASET}.{ec.TABLE}`")
    # US and SA returned releases: covered; JP came back empty: not covered
    assert "STRUCT('US' AS country, DATE '2026-11-18' AS d_from, DATE '2026-12-12' AS d_to)" in insert
    assert "STRUCT('SA' AS country" in insert and "'JP'" not in insert
    assert breakdown.startswith("SELECT status, COUNT(*)") and "TIMESTAMP '2026-11-21 14:10:07.654321+00:00'" in breakdown
    assert "7 row(s) appended (changed 2, new 5) | table created" in out.out


def test_e6_failed_window_is_not_covered(monkeypatch, capsys):
    s = FakeBQ(table={"schema": {"fields": ec.SCHEMA}, "timePartitioning": {"type": "MONTH", "field": "event_at"}})
    rc, _ = _main(monkeypatch, ["--write", "--countries", "US,SA"], s,
                  make_opener(one_window, fail={("SA", "2026-11-17")}))
    out = capsys.readouterr()
    assert rc == 1 and not s.created
    insert = s.queries[0]
    assert "STRUCT('US' AS country" in insert and "'SA' AS country" not in insert
    assert "table created" not in out.out


def test_e6_bigquery_errors(monkeypatch, capsys):
    bad = {"schema": {"fields": ec.SCHEMA[:-1]}, "timePartitioning": {"type": "MONTH", "field": "event_at"}}
    rc, _ = _main(monkeypatch, ["--write"], FakeBQ(table=bad), make_opener(one_window))
    assert rc == 2 and "differs from SCHEMA" in capsys.readouterr().err
    rc, _ = _main(monkeypatch, ["--write"], FakeBQ(insert_error="boom"), make_opener(one_window))
    assert rc == 2 and "insert: boom" in capsys.readouterr().err
    rc, _ = _main(monkeypatch, ["--write"], FakeBQ(dataset=False), make_opener(one_window))
    assert rc == 2 and "not found" in capsys.readouterr().err
    # nothing read at all: no table call, no load
    s = FakeBQ()
    rc, _ = _main(monkeypatch, ["--write"], s, make_opener(lambda *a: []))
    assert rc == 0 and "nothing read, nothing written" in capsys.readouterr().out and not s.loads and not s.created


# ------------------------------------------------------------------ E7
def test_e7_insert_sql():
    sql = ec.insert_sql(PROJECT, DATASET, [("US", dt.date(2026, 11, 18), dt.date(2026, 12, 12))], NOW, "42;DROP")
    assert sql.startswith(f"INSERT INTO `{PROJECT}.{DATASET}.econ_calendar` ({', '.join(ec.COLUMNS)})")
    assert f"SELECT * FROM `{PROJECT}.{DATASET}.econ_calendar_stage`" in sql
    assert "QUALIFY ROW_NUMBER() OVER (PARTITION BY event_key ORDER BY fetched_at DESC, status) = 1" in sql
    assert "IF(l.event_key IS NULL OR l.status = 'gone', 'new', 'changed') AS status" in sql
    for col in ec.COMPARED:
        assert f"l.{col} IS DISTINCT FROM f.{col}" in sql
    assert "DATE(l.event_at) BETWEEN c.d_from AND c.d_to" in sql and "WHERE l.status != 'gone'" in sql
    assert "NOT EXISTS (SELECT 1 FROM fresh AS f WHERE f.event_key = l.event_key)" in sql
    assert "'42DROP' AS run_id" in sql and ";" not in sql
    none = ec.insert_sql(PROJECT, DATASET, [], NOW, "")
    assert "WHERE FALSE" in none and "CAST(NULL AS STRING) AS run_id" in none and "STRUCT(" not in none
    assert ec.insert_sql(PROJECT, DATASET, [("U'S", dt.date(2026, 11, 18), dt.date(2026, 11, 20))], NOW,
                         "1").count("STRUCT(") == 0
    good = {"schema": {"fields": ec.SCHEMA}, "timePartitioning": {"type": "MONTH", "field": "event_at"}}
    assert ec.schema_problems(good) == []
    day = dict(good, timePartitioning={"type": "DAY", "field": "event_at"})
    assert ec.schema_problems(day) == ["not partitioned by month on event_at"]
    moved = dict(good, schema={"fields": [dict(f, mode="NULLABLE") if f["name"] == "status" else f
                                          for f in ec.SCHEMA]})
    assert "columns differ" in ec.schema_problems(moved)[0]


# ------------------------------------------------------------------ E8
def test_e8_main_configuration(monkeypatch, capsys):
    calls = []
    s = FakeBQ()
    rc, _ = _main(monkeypatch, ["--write"], s, make_opener(one_window, calls=calls), gate="off")
    assert rc == 0 and calls == [] and s.calls == []
    for env, msg in (({"project": None}, "TFB_BQ_PROJECT"), ({"key": None}, "EODHD_API_KEY"),
                     ({"share": "1.5"}, "TFB_ECON_EODHD_MAX_SHARE"), ({"share": "x"}, "TFB_ECON_EODHD_MAX_SHARE")):
        rc, _ = _main(monkeypatch, [], FakeBQ(), make_opener(one_window), **env)
        assert rc == 2 and msg in capsys.readouterr().err
    for argv, msg in ((["--start", "21-11-2026"], "YYYY-MM-DD"), (["--start", "2014-12-31"], "within"),
                      (["--end", "2027-04-01"], "within"), (["--start", "2026-11-22", "--end", "2026-11-21"], "within"),
                      (["--countries", "USA"], "two letters"), (["--countries", "u1"], "two letters"),
                      (["--max-calls", "0"], "--max-calls"), (["--max-calls", "5001"], "--max-calls")):
        rc, _ = _main(monkeypatch, argv, FakeBQ(), make_opener(one_window))
        assert rc == 2 and msg in capsys.readouterr().err, argv
    calls = []
    rc, _ = _main(monkeypatch, ["--start", "2023-11-01", "--end", "2024-01-31", "--countries", "us,us,sa"], FakeBQ(),
                  make_opener(one_window, calls=calls))
    eco = [q for p, q in calls if p.endswith("economic-events")]
    assert rc == 0 and [q["country"] for q in eco] == ["US"] * 3 + ["SA"] * 3
    assert [q["from"] for q in eco[:3]] == ["2023-10-31", "2023-12-01", "2024-01-01"]


# ------------------------------------------------------------------ E9
def test_e9_workflow():
    wf = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    on = wf.get(True) or wf.get("on")
    assert on["schedule"] == [{"cron": "50 4 * * *"}, {"cron": "10 14 * * 1-5"}, {"cron": "30 21 * * 1-5"}]
    inputs = on["workflow_dispatch"]["inputs"]
    assert inputs["write"]["default"] is False and inputs["write"]["type"] == "boolean"
    assert inputs["start"]["default"] == "" and inputs["end"]["default"] == "" and inputs["countries"]["default"] == ""
    assert inputs["max_calls"]["default"] == "400" and inputs["list_types"]["default"] is False
    assert wf["permissions"] == {"contents": "read"}
    job = wf["jobs"]["econ"]
    assert int(job["timeout-minutes"]) <= 30
    run = [st for st in job["steps"] if "run the loader" in st.get("name", "").lower()][0]
    env = run["env"]
    assert env["TFB_BQ_ECON"] == "${{ vars.TFB_BQ_ECON || 'on' }}"
    assert env["EODHD_API_KEY"] == "${{ secrets.EODHD_API_KEY }}"
    assert env["TFB_ECON_EODHD_MAX_SHARE"] == "${{ vars.TFB_ECON_EODHD_MAX_SHARE || '0.60' }}"
    assert "secrets.GOOGLE_SHEETS_CREDENTIALS" in env["GOOGLE_SHEETS_CREDENTIALS"]
    assert 'MODE="--write"' in run["run"] and 'MODE="--dry-run"' in run["run"]
    assert "scripts/archive_econ_calendar.py $MODE" in run["run"] and 'exit "$rc"' in run["run"]
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "actions/checkout@v6" in text and "actions/setup-python@v6" in text


# ------------------------------------------------------------------ E10
def test_e10_no_workbook_one_dml_no_token(monkeypatch, capsys):
    src = (ROOT / "scripts" / "archive_econ_calendar.py").read_text(encoding="utf-8")
    assert "sheets.googleapis" not in src and "gspread" not in src
    code = "\n".join(ln for ln in src.splitlines() if not ln.lstrip().startswith("#"))
    for word in ("DELETE FROM", "UPDATE `", "MERGE ", "DROP TABLE", "TRUNCATE TABLE", "CREATE OR REPLACE"):
        assert word not in code, word
    assert code.count("INSERT INTO") == 1 and 'f"""INSERT INTO {t}' in code
    rc, _ = _main(monkeypatch, ["--write", "--list-types"], FakeBQ(), make_opener(one_window))
    out = capsys.readouterr()
    assert rc == 0 and TOKEN not in out.out + out.err
