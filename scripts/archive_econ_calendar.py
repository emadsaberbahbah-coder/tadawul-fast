#!/usr/bin/env python3
# scripts/archive_econ_calendar.py
"""
================================================================================
TFB Economic Calendar - v1.0.0 (the releases of six economies, history included,
for the forward event scenarios)
================================================================================
v1.0.0 (21 Nov 2026) - NEW. WHY (owner, 21 Nov 2026 11:03 Riyadh, item 5 of the
build list: "News: a news reading per market; forward event scenarios;
momentum/surprise; influential figures of 19 Nov; score every news signal"):
the news gate keeps the economic calendar of the US and Saudi Arabia for five
days around each of its runs (tab _Econ_Events, archived as econ_events since
26 Oct 2026). The archive therefore holds 51 kinds of release over four weeks:
too few to say how a market has moved on a release day, or after a figure
above or below its forecast. This loader keeps the EODHD economic calendar of
six economies in BigQuery, with its history, one row per release per change.

WHAT: EODHD /api/economic-events (1 API call per request, whatever the window)
for COUNTRIES (US, EU = the euro area, DE, JP, CN, SA), in windows of at most
WINDOW_DAYS days. Each request asks for one day more on each side than its
window, because EODHD does not document whether its bounds are inclusive; the
overlap is removed by the release key. A page holds 1,000 releases: a full first
page brings a second one (offset 1,000, EODHD's largest), and a window whose
second page is full too is cut in two. Every release keeps the provider's
fields: the release time (UTC), the country, the event, the comparison (mom /
qoq / yoy), the period, actual, estimate (the forecast), previous, change and
change %. The key is the news gate's: time|country|event|comparison|period.

TABLE econ_calendar - a change log. The releases read are loaded into
econ_calendar_stage (replaced on each run), and one INSERT appends to
econ_calendar what changed:
  new      a release not in the table, or one whose latest row is gone;
  changed  a release whose time, figure, forecast, previous or change differ
           from its latest row;
  gone     a release whose latest row lies inside a window read this run that
           returned at least one release, but that EODHD no longer lists
           (moved or withdrawn).
A release that did not change appends nothing, so the table records what was
known when: the forecast before the figure, then the figure. A window that
could not be read, or that came back empty, marks nothing gone.

BUDGET GUARD: the EODHD daily limit is shared with the live sync. Before
reading, the job reads /api/user (free): the room is TFB_ECON_EODHD_MAX_SHARE
(default 0.60) of the daily limit less the calls used today, by the rule of
archive_symbol_deep.budget_check (a counter dated before today counts 0; a
limit that cannot be read skips the run - fail closed). The run is skipped
(exit 0, nothing read) when the room is smaller than its planned calls (two
a window), and every call it makes - retries and the halves of a cut window
included - counts against min(room, --max-calls) (default 400): a window
past that cap is not read and the run fails (exit 1), so the gap is seen
(Codex P2 on #1049: the planned calls alone did not bound the retries).

DEFAULT WINDOW: from 3 days before today to 21 days after (UTC dates): the
releases of the last days get their figures, the next three weeks their
forecasts. A history fill passes --start (and --end).

NOTHING is written to the workbook; the live system never reads BigQuery; no
decision, gate or count depends on these tables. The token never reaches a
log line. Economic releases are public figures; the log prints counts and
event names only.

GATE: TFB_BQ_ECON = on|1|true|yes arms the job (the workflow arms it; kill it
with the repository Variable TFB_BQ_ECON=off). Without --write it is a dry run:
EODHD is read (it costs the calls) and counted; BigQuery is only read (the
dataset check).

ENV: TFB_BQ_ECON, TFB_BQ_PROJECT, TFB_BQ_DATASET (default tfb_archive),
TFB_BQ_LOCATION (default US), GOOGLE_SHEETS_CREDENTIALS (the archive's
service account: BigQuery Data Editor + Job User), EODHD_API_KEY,
TFB_ECON_EODHD_MAX_SHARE, GITHUB_RUN_ID.
================================================================================
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import math
import os
import re
import sys
import time
import uuid
from collections import Counter
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import archive_market_series as ams  # same folder: the BigQuery session and its helpers
import archive_symbol_deep as asd    # same folder: the EODHD reader and its daily-budget guard

__version__ = "1.0.0"

UTC = _dt.timezone.utc
TABLE = "econ_calendar"
STAGE = "econ_calendar_stage"
SOURCE = "eodhd_economic_events"
ENDPOINT = "economic-events"
COUNTRIES = ("US", "EU", "DE", "JP", "CN", "SA")
WINDOW_DAYS = 31
PAGE = 1000                      # EODHD's largest page
DEFAULT_BACK, DEFAULT_AHEAD = 3, 21
MIN_DATE = _dt.date(2015, 1, 1)
MAX_AHEAD = 120                  # days after today a window may reach
DEFAULT_MAX_CALLS, MAX_CALLS = 400, 5000
DEFAULT_SHARE = 0.60
STATUSES = ("new", "changed", "gone")
BQ_API = ams.BQ_API
BQ_UPLOAD = ams.BQ_UPLOAD

SCHEMA: List[Dict[str, str]] = [
    {"name": "event_key", "type": "STRING", "mode": "REQUIRED",
     "description": "release time (UTC, YYYY-MM-DD HH:MM)|country|event|comparison|period - the news gate's Key"},
    {"name": "event_at", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "release time as EODHD gives it (UTC)"},
    {"name": "country", "type": "STRING", "mode": "REQUIRED", "description": "EODHD country code (EU = the euro area)"},
    {"name": "event_type", "type": "STRING", "mode": "REQUIRED", "description": "EODHD event name, e.g. Non Farm Payrolls"},
    {"name": "comparison", "type": "STRING", "mode": "NULLABLE", "description": "mom | qoq | yoy; NULL when EODHD gives none"},
    {"name": "period", "type": "STRING", "mode": "NULLABLE", "description": "the period the figure covers, as EODHD writes it"},
    {"name": "actual", "type": "FLOAT", "mode": "NULLABLE", "description": "the figure; NULL before the release"},
    {"name": "estimate", "type": "FLOAT", "mode": "NULLABLE", "description": "the forecast EODHD shows"},
    {"name": "previous", "type": "FLOAT", "mode": "NULLABLE", "description": "the previous figure"},
    {"name": "change", "type": "FLOAT", "mode": "NULLABLE", "description": "EODHD change"},
    {"name": "change_pct", "type": "FLOAT", "mode": "NULLABLE", "description": "EODHD change_percentage"},
    {"name": "status", "type": "STRING", "mode": "REQUIRED", "description": "new | changed | gone"},
    {"name": "fetched_at", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "when this job read EODHD"},
    {"name": "source", "type": "STRING", "mode": "REQUIRED", "description": "eodhd_economic_events"},
    {"name": "run_id", "type": "STRING", "mode": "NULLABLE", "description": "GitHub Actions run id"},
]
STAGE_SCHEMA = [f for f in SCHEMA if f["name"] != "status"]
COLUMNS = [f["name"] for f in SCHEMA]
STAGE_COLUMNS = [f["name"] for f in STAGE_SCHEMA]
# the fields a change is read on (the key holds the rest)
COMPARED = ("event_at", "actual", "estimate", "previous", "change", "change_pct")
_TYPE_ALIASES = {"INT64": "INTEGER", "FLOAT64": "FLOAT", "BOOL": "BOOLEAN"}
_NUM = re.compile(r"[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?")
_COUNTRY = re.compile(r"[A-Z]{2}")
_DAY = re.compile(r"\d{4}-\d{2}-\d{2}")


class CapReached(Exception):
    """--max-calls is spent: the windows left are not read."""


def _out(msg: str) -> None:
    sys.stderr.write(f"[econ_calendar v{__version__}] {msg}\n")


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def econ_enabled() -> bool:
    return _env("TFB_BQ_ECON").lower() in ("on", "1", "true", "yes")


# ------------------------------------------------------------------ parsing
def _num(v: Any) -> Optional[float]:
    """A finite number, or None (bool, blank, text, NaN and inf are not figures)."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        f = float(v)
    elif isinstance(v, str) and _NUM.fullmatch(v.strip()):
        f = float(v.strip())
    else:
        return None
    return f if math.isfinite(f) else None


def _known(row: Dict[str, Any]) -> Tuple[bool, bool, bool]:
    return (row["actual"] is not None, row["estimate"] is not None, row["previous"] is not None)


def parse_events(payload: Any) -> List[Dict[str, Any]]:
    """EODHD economic-events JSON -> one row per release key (sorted). The key
    is built exactly as news_macro_gate.parse_econ_events builds it; a row
    without a valid date, an event name or a country is dropped. Two rows with
    one key keep the one that knows more (the figure first)."""
    out: Dict[str, Dict[str, Any]] = {}
    for r in payload if isinstance(payload, list) else []:
        if not isinstance(r, dict):
            continue
        try:
            when = _dt.datetime.strptime(str(r.get("date")), "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        except (TypeError, ValueError):
            continue
        typ = str(r.get("type") or "").strip()
        country = str(r.get("country") or "").upper()
        if not typ or not country.strip():
            continue
        comp = str(r.get("comparison") or "")
        period = str(r.get("period") or "")
        key = "|".join([when.strftime("%Y-%m-%d %H:%M"), country, typ, comp, period])
        row = {
            "event_key": key, "event_at": when, "country": country, "event_type": typ,
            "comparison": comp or None, "period": period or None,
            "actual": _num(r.get("actual")), "estimate": _num(r.get("estimate")),
            "previous": _num(r.get("previous")), "change": _num(r.get("change")),
            "change_pct": _num(r.get("change_percentage")),
        }
        old = out.get(key)
        if old is None or _known(row) >= _known(old):
            out[key] = row
    return [out[k] for k in sorted(out)]


def windows(start: _dt.date, end: _dt.date, days: int = WINDOW_DAYS) -> List[Tuple[_dt.date, _dt.date]]:
    """[start, end] cut into back-to-back windows of at most `days` days."""
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    out: List[Tuple[_dt.date, _dt.date]] = []
    d = start
    while d <= end:
        e = min(end, d + _dt.timedelta(days=days - 1))
        out.append((d, e))
        d = e + _dt.timedelta(days=1)
    return out


# -------------------------------------------------------------------- EODHD
def budget_room(token: str, share: float, opener: Callable[..., Any], *, today: str) -> Tuple[Optional[int], str]:
    """(room, why): the calls this run may still make today - share x the
    daily limit less the calls used - by the rule of
    archive_symbol_deep.budget_check: a counter dated before `today` (UTC,
    YYYY-MM-DD) is yesterday's and counts as 0 used; a missing, malformed or
    later date counts as today's. None when the usage or the limit cannot be
    read (fail closed)."""
    try:
        user = asd.eodhd_get("user", {}, token, opener)
        used = int(float(user.get("apiRequests") or 0))
        limit = int(float(user.get("dailyRateLimit") or 0))
        dated = str(user.get("apiRequestsDate") or "").strip()[:10]
    except Exception as exc:  # noqa: BLE001 - fail closed
        return None, f"EODHD usage unreadable ({asd._scrub(exc, token)[:120]})"
    if limit <= 0:
        return None, "EODHD daily limit unknown"
    note = ""
    if _DAY.fullmatch(dated) and _DAY.fullmatch(today or "") and dated < today:
        note = f"; counter dated {dated} not reset yet, its {used} not counted"
        used = 0
    ceiling = int(share * limit)
    return max(0, ceiling - used), f"EODHD {used} used of {ceiling} allowed ({share:.0%} of {limit}){note}"


class Calls:
    """The calls of one run, capped at --max-calls."""

    def __init__(self, cap: int) -> None:
        self.cap, self.used = int(cap), 0

    def take(self) -> None:
        if self.used >= self.cap:
            raise CapReached()
        self.used += 1


def _page(token: str, country: str, d_from: _dt.date, d_to: _dt.date, offset: int,
          opener: Callable[..., Any], calls: Calls, sleep: Callable[[float], None], retry_wait: float) -> List[Any]:
    """One page of one window (asked one day wider on each side); one retry."""
    params = {"country": country, "from": (d_from - _dt.timedelta(days=1)).isoformat(),
              "to": (d_to + _dt.timedelta(days=1)).isoformat(), "limit": str(PAGE), "offset": str(offset)}
    last = ""
    for attempt in range(2):
        calls.take()
        try:
            got = asd.eodhd_get(ENDPOINT, params, token, opener)
        except RuntimeError as exc:
            last = asd._scrub(exc, token)
        else:
            if isinstance(got, list):
                return got
            last = "EODHD economic-events: the answer is not a list" + (
                f" ({asd._scrub(str(got.get('message') or got.get('error') or '')[:120], token)})"
                if isinstance(got, dict) else "")
        if attempt == 0:
            sleep(retry_wait)
    raise RuntimeError(last)


def fetch_window(token: str, country: str, d_from: _dt.date, d_to: _dt.date, opener: Callable[..., Any],
                 calls: Calls, sleep: Callable[[float], None], retry_wait: float = 10.0) -> List[Any]:
    """Every raw release of one country and window: a second page when the
    first is full, the window cut in two when the second is full too."""
    raw: List[Any] = []
    for offset in (0, PAGE):
        got = _page(token, country, d_from, d_to, offset, opener, calls, sleep, retry_wait)
        raw.extend(got)
        if len(got) < PAGE:
            return raw
    if d_from >= d_to:
        raise RuntimeError(f"{country} {d_from}: more than {2 * PAGE} releases in one day")
    mid = d_from + _dt.timedelta(days=(d_to - d_from).days // 2)
    return (fetch_window(token, country, d_from, mid, opener, calls, sleep, retry_wait)
            + fetch_window(token, country, mid + _dt.timedelta(days=1), d_to, opener, calls, sleep, retry_wait))


# ------------------------------------------------------------------ rows
def _ts(t: _dt.datetime) -> str:
    return t.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def stage_rows(rows: Iterable[Dict[str, Any]], fetched_at: _dt.datetime, run_id: str) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        row = {k: r.get(k) for k in STAGE_COLUMNS}
        row["event_at"] = _ts(r["event_at"])
        row["fetched_at"] = _ts(fetched_at)
        row["source"] = SOURCE
        row["run_id"] = run_id or None
        out.append(row)
    return out


def ndjson(rows: Iterable[Dict[str, Any]]) -> bytes:
    return b"".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
                    for r in rows)


def _safe_id(run_id: str) -> str:
    return re.sub(r"[^0-9A-Za-z_-]", "", run_id or "")


def insert_sql(project: str, dataset: str, covered: Sequence[Tuple[str, _dt.date, _dt.date]],
               fetched_at: _dt.datetime, run_id: str) -> str:
    """The one INSERT of a run: what is new or changed in the stage against
    each release's latest row, and what the windows in `covered` no longer
    list. `covered` holds only windows read this run that returned at least
    one release; with none, nothing can be marked gone."""
    t = f"`{project}.{dataset}.{TABLE}`"
    s = f"`{project}.{dataset}.{STAGE}`"
    cov = ",\n    ".join(f"STRUCT('{c}' AS country, DATE '{a.isoformat()}' AS d_from, DATE '{b.isoformat()}' AS d_to)"
                         for c, a, b in covered if _COUNTRY.fullmatch(c))
    cov_cte = (f"SELECT * FROM UNNEST([\n    {cov}\n  ])" if cov else
               "SELECT CAST(NULL AS STRING) AS country, CAST(NULL AS DATE) AS d_from, CAST(NULL AS DATE) AS d_to "
               "FROM UNNEST([1]) WHERE FALSE")
    rid = _safe_id(run_id)
    rid_sql = f"'{rid}'" if rid else "CAST(NULL AS STRING)"
    at = fetched_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f+00:00")
    changed = "\n     OR ".join(f"l.{c} IS DISTINCT FROM f.{c}" for c in COMPARED)
    keep = ", ".join(f"f.{c}" for c in COLUMNS[:11])
    gone = ", ".join(f"l.{c}" for c in COLUMNS[:11])
    return f"""INSERT INTO {t} ({', '.join(COLUMNS)})
WITH cov AS (
  {cov_cte}
),
fresh AS (
  SELECT * FROM {s}
),
latest AS (
  SELECT * FROM {t}
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_key ORDER BY fetched_at DESC, status) = 1
)
SELECT {keep},
  IF(l.event_key IS NULL OR l.status = 'gone', 'new', 'changed') AS status,
  f.fetched_at, f.source, f.run_id
FROM fresh AS f
LEFT JOIN latest AS l ON l.event_key = f.event_key
WHERE l.event_key IS NULL OR l.status = 'gone'
     OR {changed}
UNION ALL
SELECT {gone},
  'gone' AS status, TIMESTAMP '{at}' AS fetched_at, l.source, {rid_sql} AS run_id
FROM latest AS l
JOIN cov AS c ON c.country = l.country AND DATE(l.event_at) BETWEEN c.d_from AND c.d_to
WHERE l.status != 'gone'
  AND NOT EXISTS (SELECT 1 FROM fresh AS f WHERE f.event_key = l.event_key)"""


def breakdown_sql(project: str, dataset: str, fetched_at: _dt.datetime) -> str:
    at = fetched_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f+00:00")
    return (f"SELECT status, COUNT(*) AS n FROM `{project}.{dataset}.{TABLE}` "
            f"WHERE fetched_at = TIMESTAMP '{at}' GROUP BY status ORDER BY status")


# ------------------------------------------------------------------ BigQuery
def schema_problems(table: Dict[str, Any]) -> List[str]:
    """[] when an existing econ_calendar has SCHEMA's columns (name, type,
    mode, in order) and the month partitioning on event_at."""
    want = [(f["name"], f["type"], f["mode"]) for f in SCHEMA]
    got = []
    for f in ((table.get("schema") or {}).get("fields") or []):
        t = str(f.get("type") or "").upper()
        got.append((f.get("name"), _TYPE_ALIASES.get(t, t), str(f.get("mode") or "NULLABLE").upper()))
    problems = []
    if got != want:
        diff = [f"{w} != {g}" for w, g in zip(want, got) if w != g]
        problems.append("columns differ: " + ("; ".join(diff[:5]) or f"{len(got)} columns, want {len(want)}"))
    tp = table.get("timePartitioning") or {}
    if tp.get("type") != "MONTH" or tp.get("field") != "event_at":
        problems.append("not partitioned by month on event_at")
    return problems


def ensure_table(session: Any, project: str, dataset: str) -> bool:
    """True when econ_calendar was created now, False when it existed with the
    right layout; RuntimeError when it exists with another one."""
    base = f"{BQ_API}/projects/{project}/datasets/{dataset}/tables"
    resp = session.get(f"{base}/{TABLE}", timeout=60)
    if resp.status_code == 200:
        problems = schema_problems(resp.json() or {})
        if problems:
            raise RuntimeError(f"table {TABLE} differs from SCHEMA: {' | '.join(problems)}")
        return False
    if resp.status_code != 404:
        raise RuntimeError(f"table {TABLE}: {ams._err(resp)}")
    body = {
        "tableReference": {"projectId": project, "datasetId": dataset, "tableId": TABLE},
        "schema": {"fields": SCHEMA},
        "timePartitioning": {"type": "MONTH", "field": "event_at"},
        "clustering": {"fields": ["country", "event_type"]},
        "description": ("TFB economic calendar (EODHD economic-events) of six economies, history included: a "
                        "change log, one row per release per change (new / changed / gone), appended by "
                        "scripts/archive_econ_calendar.py. Research only: nothing live reads it."),
    }
    resp = session.post(base, json=body, timeout=60)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"create {TABLE}: {ams._err(resp)}")
    return True


def load_stage(session: Any, project: str, dataset: str, location: str, data: bytes, run_id: str,
               poll_s: float = 2.0, timeout_s: float = 300.0,
               sleep: Callable[[float], None] = time.sleep,
               clock: Callable[[], float] = time.monotonic) -> Dict[str, Any]:
    """Replace econ_calendar_stage with this run's releases."""
    job_id = "tfb_econ_stage_%s_%s" % (_safe_id(run_id) or "local", uuid.uuid4().hex[:8])
    config = {
        "jobReference": {"projectId": project, "jobId": job_id, "location": location},
        "configuration": {"load": {
            "destinationTable": {"projectId": project, "datasetId": dataset, "tableId": STAGE},
            "sourceFormat": "NEWLINE_DELIMITED_JSON",
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_IF_NEEDED",
            "schema": {"fields": STAGE_SCHEMA},
            "destinationTableProperties": {"description": "TFB economic calendar: the releases of the last run "
                                                          "(replaced on each run; econ_calendar keeps the log)."},
            "maxBadRecords": 0,
        }, "labels": {"tfb": "econ-calendar"}},
    }
    start = session.post(f"{BQ_UPLOAD}/projects/{project}/jobs?uploadType=resumable", json=config, timeout=60,
                         headers={"X-Upload-Content-Type": "application/octet-stream",
                                  "X-Upload-Content-Length": str(len(data))})
    if start.status_code not in (200, 201):
        raise RuntimeError(f"stage load: start {ams._err(start)}")
    headers = getattr(start, "headers", {}) or {}
    upload_url = headers.get("Location") or headers.get("location")
    if not upload_url:
        raise RuntimeError("stage load: no upload session")
    put = session.put(upload_url, data=data, timeout=300, headers={"Content-Type": "application/octet-stream"})
    if put.status_code not in (200, 201):
        raise RuntimeError(f"stage load: upload {ams._err(put)}")
    return _wait(session, project, location, job_id, put.json() or {}, "stage load", poll_s, timeout_s, sleep, clock)


def _wait(session: Any, project: str, location: str, job_id: str, job: Dict[str, Any], what: str,
          poll_s: float, timeout_s: float, sleep: Callable[[float], None], clock: Callable[[], float]) -> Dict[str, Any]:
    deadline = clock() + timeout_s
    while True:
        status = job.get("status") or {}
        if status.get("state") == "DONE":
            if status.get("errorResult"):
                raise RuntimeError(f"{what}: {status['errorResult'].get('message', 'job failed')[:300]}")
            return job
        if clock() > deadline:
            raise RuntimeError(f"{what}: job {job_id} not done after {int(timeout_s)}s")
        sleep(poll_s)
        resp = session.get(f"{BQ_API}/projects/{project}/jobs/{job_id}?location={location}", timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"{what}: poll {ams._err(resp)}")
        job = resp.json() or {}


def run_query(session: Any, project: str, location: str, sql: str, run_id: str, what: str,
              poll_s: float = 2.0, timeout_s: float = 600.0,
              sleep: Callable[[float], None] = time.sleep,
              clock: Callable[[], float] = time.monotonic) -> Dict[str, Any]:
    """One query job (the INSERT, or the breakdown SELECT), waited for."""
    job_id = "tfb_econ_%s_%s_%s" % (what.replace(" ", "_"), _safe_id(run_id) or "local", uuid.uuid4().hex[:8])
    config = {
        "jobReference": {"projectId": project, "jobId": job_id, "location": location},
        "configuration": {"query": {"query": sql, "useLegacySql": False},
                          "labels": {"tfb": "econ-calendar"}},
    }
    resp = session.post(f"{BQ_API}/projects/{project}/jobs", json=config, timeout=120)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"{what}: start {ams._err(resp)}")
    return _wait(session, project, location, job_id, resp.json() or {}, what, poll_s, timeout_s, sleep, clock)


def query_rows(session: Any, project: str, location: str, job: Dict[str, Any]) -> List[List[Any]]:
    """The first page of a finished query job's rows, as lists of values."""
    ref = job.get("jobReference") or {}
    job_id = ref.get("jobId")
    if not job_id:
        return []
    resp = session.get(f"{BQ_API}/projects/{project}/queries/{job_id}?location={location}&maxResults=100",
                       timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"results: {ams._err(resp)}")
    return [[c.get("v") for c in (r.get("f") or [])] for r in ((resp.json() or {}).get("rows") or [])]


# ------------------------------------------------------------------ run
def _summary(country: str, rows: List[Dict[str, Any]], n_windows: int, list_types: bool) -> List[str]:
    released = sum(1 for r in rows if r["actual"] is not None)
    forecast = sum(1 for r in rows if r["estimate"] is not None)
    lines = [f"{country}: {n_windows} window(s), {len(rows)} release(s), {released} with a figure, "
             f"{forecast} with a forecast"]
    types = Counter((r["event_type"], r["comparison"] or "-") for r in rows)
    shown = types.most_common() if list_types else types.most_common(12)
    for (typ, comp), n in shown:
        lines.append(f"  {country} {typ} [{comp}]: {n}")
    return lines


def run(session: Any, *, token: str, project: str, dataset: str, location: str, countries: Sequence[str],
        start: _dt.date, end: _dt.date, now: _dt.datetime, run_id: str, write: bool, max_calls: int,
        share: float, opener: Callable[..., Any], list_types: bool = False,
        sleep: Callable[[float], None] = time.sleep, pause: float = 0.2, retry_wait: float = 10.0) -> int:
    try:
        if not ams.dataset_exists(session, project, dataset):
            _out(f"ERROR: dataset {project}.{dataset} not found")
            return 2
    except Exception as exc:  # noqa: BLE001
        _out(f"ERROR: {str(exc)[:400]}")
        return 2
    wins = windows(start, end)
    planned = min(int(max_calls), 2 * len(wins) * len(countries))
    room, why = budget_room(token, share, opener, today=now.astimezone(UTC).date().isoformat())
    if room is None or room < planned:
        why += "" if room is None else f": room {room} < {planned} planned"
        print(f"[econ_calendar v{__version__}] SKIPPED: {why} - nothing read, nothing written")
        return 0
    cap = min(int(max_calls), room)
    _out(f"{why}: {planned} call(s) planned, at most {cap} this run (retries and split windows included)")
    calls = Calls(cap)
    found: Dict[str, Dict[str, Any]] = {}
    covered: List[Tuple[str, _dt.date, _dt.date]] = []
    failed: List[str] = []
    unread = 0
    lines: List[str] = []
    for country in countries:
        mine: Dict[str, Dict[str, Any]] = {}
        for a, b in wins:
            try:
                rows = parse_events(fetch_window(token, country, a, b, opener, calls, sleep, retry_wait))
            except CapReached:
                unread += 1
                continue
            except Exception as exc:  # noqa: BLE001 - one window never stops the others
                failed.append(f"{country} {a}..{b}")
                _out(f"ERROR {country} {a}..{b}: {asd._scrub(exc, token)[:300]}")
                continue
            if rows:
                covered.append((country, a, b))
            for r in rows:
                old = mine.get(r["event_key"])
                if old is None or _known(r) >= _known(old):
                    mine[r["event_key"]] = r
            sleep(pause)
        found.update(mine)
        lines.extend(_summary(country, list(mine.values()), len(wins), list_types))
    for ln in lines:
        _out(ln)
    rows = [found[k] for k in sorted(found)]
    tail = (f"; {len(failed)} window(s) FAILED: {', '.join(failed[:8])}" if failed else "") + (
        f"; {unread} window(s) NOT READ (the cap of {cap} call(s) spent)" if unread else "")
    head = (f"[econ_calendar v{__version__}] {len(countries)} countr(ies), {start}..{end}, {len(rows)} release(s), "
            f"{calls.used} EODHD call(s)")
    if not write:
        print(head + " (dry run, nothing written)" + tail)
        return 1 if failed or unread else 0
    if not rows:
        print(head + " - nothing read, nothing written" + tail)
        return 1 if failed or unread else 0
    try:
        created = ensure_table(session, project, dataset)
        load_stage(session, project, dataset, location, ndjson(stage_rows(rows, now, run_id)), run_id, sleep=sleep)
        job = run_query(session, project, location, insert_sql(project, dataset, covered, now, run_id), run_id,
                        "insert", sleep=sleep)
        n = int(((job.get("statistics") or {}).get("query") or {}).get("numDmlAffectedRows") or 0)
    except Exception as exc:  # noqa: BLE001
        _out(f"ERROR: {str(exc)[:400]}")
        return 2
    split = ""
    try:
        got = query_rows(session, project, location,
                         run_query(session, project, location, breakdown_sql(project, dataset, now), run_id,
                                   "breakdown", sleep=sleep))
        split = " (" + ", ".join(f"{s} {c}" for s, c in got) + ")" if got else ""
    except Exception as exc:  # noqa: BLE001 - the counts are a courtesy; the rows are in
        _out(f"breakdown not read: {str(exc)[:200]}")
    print(head + f" | {n} row(s) appended{split}" + (" | table created" if created else "") + tail)
    return 1 if failed or unread else 0


def _date(text: str, name: str) -> Optional[_dt.date]:
    s = (text or "").strip()
    if not s:
        return None
    if not _DAY.fullmatch(s):
        raise ValueError(f"{name} must be YYYY-MM-DD, got '{s}'")
    return _dt.date.fromisoformat(s)


def main(argv: Optional[Sequence[str]] = None, session: Any = None, now: Optional[_dt.datetime] = None,
         opener: Optional[Callable[..., Any]] = None, sleep: Callable[[float], None] = time.sleep) -> int:
    ap = argparse.ArgumentParser(description="TFB economic calendar (EODHD) -> BigQuery")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true", help="append the changes to BigQuery")
    mode.add_argument("--dry-run", action="store_true", help="read EODHD and report only (default)")
    ap.add_argument("--start", default="", help=f"first day, YYYY-MM-DD (default: {DEFAULT_BACK} days ago)")
    ap.add_argument("--end", default="", help=f"last day, YYYY-MM-DD (default: {DEFAULT_AHEAD} days ahead)")
    ap.add_argument("--countries", default="", help="comma-separated two-letter codes (default: %s)"
                    % ",".join(COUNTRIES))
    ap.add_argument("--max-calls", type=int, default=DEFAULT_MAX_CALLS, help="EODHD calls this run may use")
    ap.add_argument("--list-types", action="store_true", help="print every event name read, with its count")
    args = ap.parse_args(list(argv) if argv is not None else None)
    if not econ_enabled():
        _out("TFB_BQ_ECON is off - nothing read, nothing written")
        return 0
    project = _env("TFB_BQ_PROJECT")
    dataset = _env("TFB_BQ_DATASET", "tfb_archive")
    location = _env("TFB_BQ_LOCATION", "US")
    token = _env("EODHD_API_KEY")
    if not project:
        _out("ERROR: TFB_BQ_PROJECT is required")
        return 2
    if not token:
        _out("ERROR: EODHD_API_KEY is required")
        return 2
    now = now or _dt.datetime.now(UTC)
    today = now.astimezone(UTC).date()
    try:
        start = _date(args.start, "--start") or today - _dt.timedelta(days=DEFAULT_BACK)
        end = _date(args.end, "--end") or today + _dt.timedelta(days=DEFAULT_AHEAD)
    except ValueError as exc:
        _out(f"ERROR: {exc}")
        return 2
    if start < MIN_DATE or end > today + _dt.timedelta(days=MAX_AHEAD) or end < start:
        _out(f"ERROR: the window must lie within {MIN_DATE}..{today + _dt.timedelta(days=MAX_AHEAD)} with start "
             f"<= end, got {start}..{end}")
        return 2
    countries = [c.strip().upper() for c in (args.countries or "").split(",") if c.strip()] or list(COUNTRIES)
    bad = [c for c in countries if not _COUNTRY.fullmatch(c)]
    if bad:
        _out(f"ERROR: country codes are two letters, got {', '.join(bad)}")
        return 2
    countries = list(dict.fromkeys(countries))
    if not (1 <= int(args.max_calls) <= MAX_CALLS):
        _out(f"ERROR: --max-calls must be 1..{MAX_CALLS}, got {args.max_calls}")
        return 2
    try:
        share = float(_env("TFB_ECON_EODHD_MAX_SHARE", str(DEFAULT_SHARE)))
    except ValueError:
        share = -1.0
    if not 0.0 < share <= 1.0:
        _out("ERROR: TFB_ECON_EODHD_MAX_SHARE must be a share in (0, 1]")
        return 2
    if session is None:
        try:
            session = ams.make_session()
        except Exception as exc:  # noqa: BLE001
            _out(f"ERROR: {str(exc)[:300]}")
            return 2
    return run(session, token=token, project=project, dataset=dataset, location=location, countries=countries,
               start=start, end=end, now=now, run_id=_env("GITHUB_RUN_ID"), write=bool(args.write),
               max_calls=int(args.max_calls), share=share, opener=opener or ams._default_opener,
               list_types=bool(args.list_types), sleep=sleep)


if __name__ == "__main__":
    sys.exit(main())
