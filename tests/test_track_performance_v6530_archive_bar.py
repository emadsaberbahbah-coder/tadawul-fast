#!/usr/bin/env python3
"""tests/test_track_performance_v6530_archive_bar.py - track_performance
v6.53.0 [ARCHIVE-BAR + DUE-ONLY]: the historical target-session close from
the BigQuery bar archive (TRACK_HIST_CLOSE_SOURCE), due-only audit pricing
(TRACK_AUDIT_DUE_ONLY) and the post-US-close clock slot.

WHY (2026-11-02)
----------------
Performance_Log holds 13,856 active forecast records (1,376 symbols), 5,759
overdue, and the learning loop matures a trickle: the audit prices EVERY
active symbol, so the Yahoo fallback's breaker trips before the due symbols
are reached (about 3,250 receipts 'quote_session_unknown;
quote_rate_limit_breaker'), and both audit slots land inside the US session
(about 1,450 'quote_outside_target_session', US 1,039). v6.53.0 lets the
v6.47.0 historical-close judge read its bars from tfb_archive.symbol_bars_daily
in ONE query per pass, prices only the due symbols when asked, and the clock
gains a 04:30 Riyadh slot. This harness drives the REAL audit_active_records
loop the way tests/test_tracker_hist_target_close.py does.

Substituted: the clock, the lean exchange-calendar fixture, the backend
transport (AsyncMock / a filtering fake), the corporate-action ledger index
(empty), the yfinance daily-bar pull (_hist_close_pull -> fixture bars) and
the archive session (_archive_bar_session -> a fake AuthorizedSession that
answers BigQuery REST payloads from fixture bars). No provider, workbook,
credential or network.

Cases
  S1  source=yfinance (unset and 5 other spellings): the yfinance pull runs,
      the BigQuery session is never built, the v6.47.0 line and dict are exact
  S2  source=bq_archive enforce: ONE query (named ARRAY<STRING> + DATE
      parameters, bounded to the candidates' sessions), bars shaped like
      _hist_close_bars output, a MATURED receipt with provider 'bq_archive_bar'
      and quote_asof = the target session's close; no yfinance pull
  S3  a symbol missing from the archive -> hist_close_archive_no_bar, ACTIVE
  S4  a later-session-only bar -> hist_close_archive_no_bar, ACTIVE
      (quote_outside_target_session semantics preserved); the judge's own
      refusals still apply to archive bars; S4c a day rebuilt from hourly
      prints (source yahoo_chart_v8_1h, Codex P2 on #894) ->
      hist_close_archive_not_daily, never a maturity, yfinance decides in
      the combined mode
  S5  a failed session / HTTP error / transport error / payload error /
      poll failure / over-paging -> every candidate
      hist_close_archive_unavailable, the pass continues, no exception, and
      no credential text reaches the read-back
  S6  TRACK_HIST_TARGET_CLOSE off -> nothing read whatever the source
  S7  observe + bq_archive: one query, records equal off, one extra line
  S8  bq_archive_then_yfinance: archive first, no-bar symbols fall back to
      the yfinance pull under its budget; a failed query falls back whole
  S9  the REAL _archive_bar_query over a fake session: poll while the job is
      not complete, follow pageToken, parse rows; _archive_bar_rows shapes
  D1  due-only off: every active symbol is priced, equal to main
  D2  due-only on: only due symbols are priced, oldest target first; not-due
      records are byte-identical to the existing 'not priced' path; counts
      logged; a mixed symbol is priced; an unparsable target is not due
  D3  the due-only worklist reaches the Yahoo fallback in order and the
      fallback keeps it (keep_order); the default still shuffles
  D4  pure helper table (_audit_due_symbols, _hist_close_source, env readers)
  W1  workflow files: the third cron, the five env lines in both tracker
      steps (ENV PARITY), the 1.0.7 header, the verifier pin, the version
  N1  off == main 953f84e2 on a mixed fixture (records, Notes, stats, logs)
  N2  added lines are ASCII, nothing removed, the new function set is exact

Golden negative: TFB_TEST_TP_DELIVERED=<main copy> runs every case against
v6.50.0 as the delivered module (all but D1/N1 fail).
Base copy: TFB_TEST_TP_BASE=<path to main 953f84e2 scripts/track_performance.py>,
else `git show 953f84e2:scripts/track_performance.py`; D1/N1/N2 skip without it.

Run:  python3 -m pytest -q tests/test_track_performance_v6530_archive_bar.py
  or  python3 tests/test_track_performance_v6530_archive_bar.py   (PASS k/k)
"""
from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import importlib.util
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TRACKER = ROOT / "scripts/track_performance.py"
DELIVERED = Path(os.getenv("TFB_TEST_TP_DELIVERED", "").strip() or TRACKER)
BASE_SHA = "953f84e2"   # main at the v6.50.0 pin (#888 merged)
GATES = ("TRACK_HIST_TARGET_CLOSE", "TRACK_HIST_CLOSE_BUDGET", "TRACK_HIST_CLOSE_TIME_SEC",
         "TRACK_HIST_CLOSE_SOURCE", "TRACK_AUDIT_DUE_ONLY", "TFB_BQ_PROJECT", "TFB_BQ_DATASET",
         "TFB_BQ_LOCATION", "TFB_TRACK_FALLBACK_SHUFFLE", "TFB_TRACK_FEED_LEGACY",
         "TFB_PERF_LEGACY_FEED", "TFB_TRACK_PRICE_FALLBACK")

UTC = timezone.utc
NOW = datetime(2026, 10, 8, 21, tzinfo=UTC)          # Thu, after the US close
CLOSE = datetime(2026, 10, 8, 20, tzinfo=UTC)        # today's session close
TARGET = CLOSE - timedelta(days=7)                   # Thu 2026-10-01 20:00Z
SESSION = "2026-10-01"
BQ_PROJECT = "project-2e3530e6-2311-4aa3-b36"


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


tp = _load("tp_archive_bar", DELIVERED)
_BASE = {}


def base():
    """The v6.50.0 copy (main 953f84e2), or skip."""
    if "mod" not in _BASE:
        path = os.getenv("TFB_TEST_TP_BASE", "").strip()
        if not path:
            try:
                text = subprocess.run(["git", "show", BASE_SHA + ":scripts/track_performance.py"],
                                      cwd=ROOT, capture_output=True, text=True, check=True).stdout
            except Exception:
                text = ""
            if text:
                tmp = Path(tempfile.mkdtemp()) / "tp_v6500_base.py"
                tmp.write_text(text, encoding="utf-8")
                path = str(tmp)
        _BASE["mod"] = _load("tp_archive_bar_base", path) if path else None
    if _BASE["mod"] is None:
        pytest.skip("base copy unavailable (set TFB_TEST_TP_BASE)")
    return _BASE["mod"]


def calendar_fixture(name, day):
    """Lean offline sessions (copy of tests/test_outcome_price_evidence.py)."""
    around = datetime.fromisoformat(day).replace(tzinfo=UTC)
    out = []
    for offset in range(-14, 15):
        date = around + timedelta(days=offset)
        if (date.weekday() in (4, 5) if name == "XSAU" else date.weekday() >= 5):
            continue
        opening = date.replace(hour=7 if name == "XSAU" else 13, minute=0 if name == "XSAU" else 30)
        closing = date.replace(hour=12 if name == "XSAU" else 20, minute=0)
        out.append((date.date().isoformat(), opening, closing))
    return tuple(out)


def row(symbol="SYNTH.US", price=110.0, quote=CLOSE, acquired=NOW):
    return {"symbol": symbol, "current_price": price, "data_provider": "yahoo_chart",
            "acquisition_status": "success", "acquisition_provider": "yahoo_chart",
            "acquisition_acquired_at": acquired.isoformat(),
            "acquisition_quote_asof": quote.isoformat()}


def record(mod, symbol="SYNTH.US", target=TARGET, entry=100.0, horizon="WEEK_1"):
    return mod.PerformanceRecord(
        record_id="synthetic-%s-%s" % (symbol, horizon), symbol=symbol,
        horizon=getattr(mod.HorizonType, horizon), date_recorded=target - timedelta(days=7),
        entry_price=entry, entry_recommendation=mod.RecommendationType.BUY, entry_score=70,
        entry_risk_bucket="MEDIUM", entry_confidence="HIGH", origin_tab="synthetic",
        target_price=110, target_roi=10, target_date=target,
        status=mod.PerformanceStatus.ACTIVE, last_updated=NOW)


def bars(target_close=104.0, extra=(), drop_target=False, start="2026-09-21", end="2026-10-08"):
    """Weekday daily bars start..end (US dates), close 100 + i; the target
    session's close is `target_close`."""
    out = []
    day = datetime.fromisoformat(start)
    stop = datetime.fromisoformat(end)
    i = 0
    while day <= stop:
        if day.weekday() < 5:
            d = day.strftime("%Y-%m-%d")
            if d == SESSION:
                if not drop_target:
                    out.append({"date": d, "close": target_close, "split": 0.0})
            else:
                out.append({"date": d, "close": 100.0 + i, "split": 0.0})
            i += 1
        day += timedelta(days=1)
    out.extend(extra)
    return out


# ------------------------------------------------------------ fake BigQuery --
BQ_FIELDS = ["symbol", "date", "open", "high", "low", "close", "adj_close", "source"]


class FakeResp:
    def __init__(self, code, payload):
        self.status_code = code
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class FakeSession:
    """An AuthorizedSession stand-in answering jobs.query / getQueryResults
    from `archive` ({TFB symbol: [bar, ...]}): the rows of the asked symbols
    whose date lies in since..until, as the REST API renders them (every
    value a string). `complete=False` answers jobComplete false first and
    completes on the poll; `page_size` splits the rows over pageToken pages."""

    def __init__(self, archive, *, complete=True, page_size=0, post_code=200, poll_code=200,
                 post_exc=None, payload_error=None, page_fail=False):
        self.archive = archive
        self.complete, self.page_size = complete, page_size
        self.post_code, self.poll_code = post_code, poll_code
        self.post_exc, self.payload_error, self.page_fail = post_exc, payload_error, page_fail
        self.posts, self.gets = [], []
        self._rows = []

    def _select(self, body):
        params = {p["name"]: p for p in body["queryParameters"]}
        syms = [v["value"] for v in params["symbols"]["parameterValue"]["arrayValues"]]
        since, until = params["since"]["parameterValue"]["value"], params["until"]["parameterValue"]["value"]
        rows = []
        for sym in syms:
            for bar in self.archive.get(sym, []):
                if since <= bar["date"] <= until:
                    close = bar["close"]
                    rows.append({"f": [{"v": sym}, {"v": bar["date"]}, {"v": None}, {"v": None}, {"v": None},
                                       {"v": None if close is None else str(close)}, {"v": None},
                                       {"v": bar.get("source", "yahoo_chart_v8")}]})
        return rows

    def _page(self, start):
        rows = self._rows[start:start + self.page_size] if self.page_size else self._rows
        payload = {"kind": "bigquery#queryResponse", "jobComplete": True,
                   "jobReference": {"projectId": BQ_PROJECT, "jobId": "job_tfb_1", "location": "US"},
                   "schema": {"fields": [{"name": n, "type": "STRING"} for n in BQ_FIELDS]},
                   "rows": rows, "totalRows": str(len(self._rows))}
        if self.page_size and start + self.page_size < len(self._rows):
            payload["pageToken"] = "tok%d" % (start + self.page_size)
        if self.payload_error:
            payload["errors"] = [{"message": self.payload_error}]
        return payload

    def post(self, url, json=None, timeout=None, **kw):
        self.posts.append({"url": url, "body": json, "timeout": timeout})
        if self.post_exc is not None:
            raise self.post_exc
        if self.post_code != 200:
            return FakeResp(self.post_code, {"error": {"message": "quota exceeded for this project"}})
        self._rows = self._select(json)
        if not self.complete:
            return FakeResp(200, {"jobComplete": False,
                                  "jobReference": {"projectId": BQ_PROJECT, "jobId": "job_tfb_1", "location": "US"}})
        return FakeResp(200, self._page(0))

    def get(self, url, params=None, timeout=None, **kw):
        self.gets.append({"url": url, "params": dict(params or {}), "timeout": timeout})
        if self.poll_code != 200:
            return FakeResp(self.poll_code, {"error": {"message": "backend error"}})
        token = (params or {}).get("pageToken")
        if token:
            if self.page_fail:
                return FakeResp(200, {"jobComplete": True, "errors": [{"message": "page lost"}]})
            return FakeResp(200, self._page(int(token[3:])))
        return FakeResp(200, self._page(0))


class Run:
    def __init__(self):
        self.pulls = []
        self.logs = []
        self.sessions = []
        self.requested = []
        self.fallback_calls = []


@contextlib.contextmanager
def harness(mod, env=None, bar_map=None, archive=None, session=None, session_exc=None,
            ca_guard=False, patches=None, now=NOW):
    """Swap module attributes and gate ENVs; restore everything afterwards."""
    run = Run()
    saved_env = {k: os.environ.get(k) for k in GATES}
    for k in GATES:
        os.environ.pop(k, None)
    for k, v in (env or {}).items():
        os.environ[k] = v

    def fake_pull(symbol):
        run.pulls.append(symbol)
        return [dict(b) for b in (bar_map or {}).get(symbol, [])]

    def fake_session():
        run.sessions.append(1)
        if session_exc is not None:
            raise session_exc
        return session if session is not None else FakeSession(archive or {})

    swaps = {"_utc_now": lambda: now,
             "_outcome_calendar_schedule": calendar_fixture,
             "_price_fallback_enabled": lambda: False,
             "_ca_guard_enabled": lambda: ca_guard,
             "_feed_legacy": lambda: False,
             "_ca_ledger_index": lambda: {},
             "urlopen": lambda *a, **k: (_ for _ in ()).throw(AssertionError("live transport"))}
    if hasattr(mod, "_hist_close_pull"):
        swaps["_hist_close_pull"] = fake_pull
    if hasattr(mod, "_archive_bar_session"):
        swaps["_archive_bar_session"] = fake_session
    swaps.update(patches or {})
    saved = {k: getattr(mod, k) for k in swaps}
    saved_now = mod.RiyadhTime.__dict__["now"]
    handler = logging.Handler()
    handler.emit = lambda rec: run.logs.append(rec.getMessage())
    mod.logger.addHandler(handler)
    saved_level = mod.logger.level
    mod.logger.setLevel(logging.INFO)
    try:
        for k, v in swaps.items():
            setattr(mod, k, v)
        mod.RiyadhTime.now = staticmethod(lambda: now)
        yield run
    finally:
        mod.logger.setLevel(saved_level)
        mod.logger.removeHandler(handler)
        mod.RiyadhTime.now = saved_now
        for k, v in saved.items():
            setattr(mod, k, v)
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _filtering_post(rows, run):
    """A backend transport that answers only the symbols each chunk asks for
    and records the request order (the real fetch_prices chain runs)."""
    async def post_json(endpoint, payload, timeout_sec=None, **kw):
        asked = [str(s).upper() for s in (payload or {}).get("symbols") or []]
        run.requested.append(list(asked))
        return {"rows": [r for r in rows if r["symbol"] in asked]}, None, 200
    return post_json


def audit(mod, records, rows, filtering=False, **kw):
    with harness(mod, **kw) as run:
        app = mod.PerformanceTrackerApp.__new__(mod.PerformanceTrackerApp)
        app.backend = mod.BackendClient("https://transport.invalid")
        if filtering:
            app.backend.post_json = _filtering_post(rows, run)
        else:
            app.backend.post_json = AsyncMock(return_value=({"rows": rows}, None, 200))
        asyncio.run(app.audit_active_records(records))
        run.stats = dict(getattr(app, "_last_audit_stats", {}) or {})
    return run


def receipt(mod, rec):
    return json.loads(rec.notes.rsplit(mod._OUTCOME_RECEIPT, 1)[1])


def state(rec):
    d = dataclasses.asdict(rec)
    d["notes"] = re.sub(r'"tracker_version":"[0-9.]+"', '"tracker_version":"X"', d["notes"] or "")
    return json.loads(json.dumps(d, default=str, sort_keys=True))


def hist_lines(run):
    return [m for m in run.logs if m.startswith("[HIST-CLOSE v")]


def archive_lines(run):
    return [m for m in run.logs if m.startswith("[ARCHIVE-BAR v")]


def due_lines(run):
    return [m for m in run.logs if "DUE-ONLY]" in m]


ENFORCE = {"TRACK_HIST_TARGET_CLOSE": "enforce"}
BQ = {"TRACK_HIST_TARGET_CLOSE": "enforce", "TRACK_HIST_CLOSE_SOURCE": "bq_archive"}
BQ_THEN = {"TRACK_HIST_TARGET_CLOSE": "enforce", "TRACK_HIST_CLOSE_SOURCE": "bq_archive_then_yfinance"}
V647_LINE = ("[HIST-CLOSE v6.47.0] mode=enforce candidates=1 witnessed=1 (1W:1) matured=1 "
             "refused=0 (-) pulls=1 ok=1 failed=0 breaker=closed deferred=0")
V647_STATS = {"mode": "enforce", "candidates": 1, "witnessed": 1, "by_horizon": {"1W": 1},
              "refused": {}, "matured": 1, "pulls": 1, "ok": 1, "failed": 0, "breaker": False,
              "first_error": "", "deferred": 0}


# ---------------------------------------------------------------- S1 -----
def test_s1_yfinance_source_is_the_v647_pull_and_never_builds_a_session():
    for spelling in (None, "yfinance", " YFinance ", "1", "bq", "", "archive"):
        tp._LAST_HIST_CLOSE.clear()
        rec = record(tp)
        env = dict(ENFORCE)
        if spelling is not None:
            env["TRACK_HIST_CLOSE_SOURCE"] = spelling
        run = audit(tp, [rec], [row()], env=env, bar_map={"SYNTH.US": bars()},
                    archive={"SYNTH.US": bars(50.0)})
        assert run.pulls == ["SYNTH.US"], spelling
        assert run.sessions == [], spelling                      # BigQuery never touched
        assert rec.status == tp.PerformanceStatus.MATURED and rec.realized_roi == pytest.approx(4.0), spelling
        proof = receipt(tp, rec)
        assert proof["provider"] == "yfinance-history" and proof["hist_provider"] == "yfinance-history", spelling
        assert tp._LAST_HIST_CLOSE == V647_STATS, spelling        # no new keys under yfinance
        assert hist_lines(run) == [V647_LINE] and archive_lines(run) == [], spelling
        assert tp._hist_close_source() == "yfinance", spelling


# ---------------------------------------------------------------- S2 -----
def test_s2_bq_archive_matures_on_the_archive_bar_with_one_query():
    for close, outcome in ((104.0, "WIN"), (95.0, "LOSS"), (100.0, "BREAKEVEN")):
        _s2_case(close, outcome)


def _s2_case(close, outcome):
    tp._LAST_HIST_CLOSE.clear()
    rec = record(tp)
    session = FakeSession({"SYNTH.US": bars(close), "OTHER.US": bars(1.0)})
    run = audit(tp, [rec], [row()], env=BQ, session=session, bar_map={"SYNTH.US": bars(999.0)})
    roi = (close / 100.0 - 1.0) * 100.0
    assert rec.status == tp.PerformanceStatus.MATURED and rec.outcome == outcome
    assert rec.realized_roi == pytest.approx(roi)
    assert rec.current_price == close and rec.unrealized_roi == pytest.approx(roi)
    assert run.pulls == [] and run.sessions == [1]                # no yfinance pull, one session
    assert len(session.posts) == 1 and session.gets == []         # ONE query, no poll
    post = session.posts[0]
    assert post["url"] == "https://bigquery.googleapis.com/bigquery/v2/projects/%s/queries" % BQ_PROJECT
    body = post["body"]
    assert body["useLegacySql"] is False and body["location"] == "US" and body["timeoutMs"] == 60000
    assert body["parameterMode"] == "NAMED"
    assert "`%s.tfb_archive.symbol_bars_daily`" % BQ_PROJECT in body["query"]
    assert "symbol IN UNNEST(@symbols)" in body["query"] and "date >= @since" in body["query"]
    assert "date <= @until" in body["query"]
    assert body["query"].startswith("SELECT symbol, date, open, high, low, close, adj_close, source FROM")
    params = {p["name"]: p for p in body["queryParameters"]}
    assert params["symbols"]["parameterType"] == {"type": "ARRAY", "arrayType": {"type": "STRING"}}
    assert params["symbols"]["parameterValue"] == {"arrayValues": [{"value": "SYNTH.US"}]}
    assert params["since"] == {"name": "since", "parameterType": {"type": "DATE"},
                               "parameterValue": {"value": SESSION}}
    assert params["until"]["parameterValue"] == {"value": SESSION}        # bounded to the sessions
    assert tp._HIST_ARCHIVE_CACHE == {"SYNTH.US": [{"date": SESSION, "close": close, "split": 0.0,
                                                    "source": "yahoo_chart_v8"}]}
    proof = receipt(tp, rec)
    assert proof["state"] == "MATURED" and proof["reason"] == ""
    assert proof["provider"] == "bq_archive_bar" and proof["hist_provider"] == "bq_archive_bar"
    assert proof["quote_kind"] == "historical_daily_close"
    assert proof["acquired_at"] == NOW.isoformat()
    assert proof["quote_asof"] == TARGET.isoformat() == proof["target_session_close"]
    assert proof["target_session"] == proof["quote_session"] == proof["hist_bar_date"] == SESSION
    assert proof["current_quote_session"] == "2026-10-08" and proof["current_quote_price"] == 110.0
    assert proof["price"] == close and proof["realized_roi_pp"] == pytest.approx(roi)
    assert proof["schema"] == "tfb.outcome-price.v1" and proof["tracker_version"] == tp.SCRIPT_VERSION
    assert run.stats["matured"] == 1 and run.stats["outcome_skipped_reasons"] == {}
    st = tp._LAST_HIST_CLOSE
    assert st["source"] == "bq_archive" and st["pulls"] == 0 and st["matured"] == 1
    assert st["archive"]["loaded"] is True and st["archive"]["unavailable"] is False
    assert (st["archive"]["candidates"], st["archive"]["symbols"], st["archive"]["asked"],
            st["archive"]["rows"], st["archive"]["served"], st["archive"]["no_bar"]) == (1, 1, 1, 1, 1, 0)
    assert hist_lines(run) == [V647_LINE.replace("pulls=1 ok=1", "pulls=0 ok=0")]
    assert archive_lines(run) == ["[ARCHIVE-BAR v6.53.0] source=bq_archive candidates=1 symbols=1 asked=1 "
                                  "rows=1 served=1 no_bar=0 not_daily=0 not_asked=0 fallback=0 unavailable=0 "
                                  "query_sec=%.2f" % st["archive"]["query_sec"]]


def test_s2b_maturation_is_decided_by_the_real_target_session_rule():
    """A record whose quote IS from its target session matures on the live
    quote (never a candidate, never an archive lookup); a record whose
    target session has not closed waits; only the overdue one is served."""
    in_session = CLOSE - timedelta(hours=4)
    recs = [record(tp, symbol="NOW.US", target=in_session), record(tp),
            record(tp, symbol="SOON.US", target=NOW + timedelta(days=1))]
    session = FakeSession({"SYNTH.US": bars(), "NOW.US": bars(50.0), "SOON.US": bars(50.0)})
    run = audit(tp, recs, [row(symbol="NOW.US", price=97.0), row(), row(symbol="SOON.US")],
                env=BQ, session=session)
    assert [r.status for r in recs] == [tp.PerformanceStatus.MATURED, tp.PerformanceStatus.MATURED,
                                        tp.PerformanceStatus.ACTIVE]
    assert recs[0].realized_roi == pytest.approx(-3.0) and receipt(tp, recs[0])["provider"] == "yahoo_chart"
    assert recs[1].realized_roi == pytest.approx(4.0) and receipt(tp, recs[1])["provider"] == "bq_archive_bar"
    assert tp._OUTCOME_RECEIPT not in (recs[2].notes or "")
    params = {p["name"]: p for p in session.posts[0]["body"]["queryParameters"]}
    assert params["symbols"]["parameterValue"] == {"arrayValues": [{"value": "SYNTH.US"}]}
    assert run.pulls == [] and tp._LAST_HIST_CLOSE["candidates"] == 1


def test_s2c_many_symbols_one_query_sorted_and_date_bounded():
    syms = ["C.US", "A.US", "B.US"]
    recs = [record(tp, symbol=s) for s in syms]
    recs[1].target_date = TARGET - timedelta(days=1)          # Wed 2026-09-30 session
    recs[1].date_recorded = recs[1].target_date - timedelta(days=7)
    session = FakeSession({s: bars() for s in syms})
    run = audit(tp, recs, [row(symbol=s) for s in syms], env=BQ, session=session)
    assert len(session.posts) == 1 and run.sessions == [1]
    params = {p["name"]: p for p in session.posts[0]["body"]["queryParameters"]}
    assert [v["value"] for v in params["symbols"]["parameterValue"]["arrayValues"]] == ["A.US", "B.US", "C.US"]
    assert params["since"]["parameterValue"]["value"] == "2026-09-30"
    assert params["until"]["parameterValue"]["value"] == SESSION
    assert all(r.status == tp.PerformanceStatus.MATURED for r in recs)
    assert tp._LAST_HIST_CLOSE["archive"]["rows"] == 6          # two dates x three symbols
    assert sorted(tp._HIST_ARCHIVE_CACHE) == ["A.US", "B.US", "C.US"]


# ---------------------------------------------------------------- S3 -----
def test_s3_symbol_missing_from_the_archive_is_not_matured():
    for later in (False, True):
        rec = record(tp)
        patches = {"_mature_grace_days": lambda: 0} if later else None
        run = audit(tp, [rec], [row()], env=BQ, archive={"OTHER.US": bars()},
                    bar_map={"SYNTH.US": bars()}, patches=patches)
        assert rec.status == tp.PerformanceStatus.ACTIVE and rec.outcome is None and rec.realized_roi is None
        assert rec.current_price == 110.0                        # the live mark stays
        proof = receipt(tp, rec)
        assert proof["state"] == "PENDING" and proof["reason"] == "quote_outside_target_session"
        assert proof["hist_close_reason"] == "hist_close_archive_no_bar"
        assert proof["provider"] == "yahoo_chart" and "hist_provider" not in proof
        assert run.pulls == [] and run.sessions == [1]
        assert tp._LAST_HIST_CLOSE["refused"] == {"hist_close_archive_no_bar": 1}
        assert tp._LAST_HIST_CLOSE["archive"]["no_bar"] == 1 and tp._LAST_HIST_CLOSE["archive"]["served"] == 0
        assert tp._HIST_ARCHIVE_CACHE == {"SYNTH.US": []}
        assert run.stats["outcome_skipped_reasons"] == {"quote_outside_target_session": 1}


# ---------------------------------------------------------------- S4 -----
LATER_TARGET = CLOSE - timedelta(days=3)       # Mon 2026-10-05 20:00Z, session 2026-10-05


def test_s4_later_session_only_bar_is_never_used():
    """Two candidates widen the query to 2026-10-01..2026-10-05, so SYNTH's
    only bar (2026-10-02, the session AFTER its target) comes back inside the
    result: a nearest or latest-bar lookup would mature on it; the rule
    'never a later session' holds, the reason names the archive, and the
    other candidate still matures on its own session's bar."""
    later_only = [{"date": "2026-10-02", "close": 150.0, "split": 0.0}]
    for label, synth_bars in (("later only", later_only),
                              ("earlier only", [{"date": "2026-09-30", "close": 150.0, "split": 0.0}]),
                              ("later and earlier", [{"date": "2026-09-30", "close": 150.0, "split": 0.0},
                                                     {"date": "2026-10-02", "close": 150.0, "split": 0.0}])):
        rec, other = record(tp), record(tp, symbol="OTHER.US", target=LATER_TARGET)
        session = FakeSession({"SYNTH.US": synth_bars, "OTHER.US": bars()})
        run = audit(tp, [rec, other], [row(), row(symbol="OTHER.US")], env=BQ, session=session)
        params = {p["name"]: p for p in session.posts[0]["body"]["queryParameters"]}
        assert (params["since"]["parameterValue"]["value"], params["until"]["parameterValue"]["value"]) == \
            (SESSION, "2026-10-05"), label
        assert rec.status == tp.PerformanceStatus.ACTIVE and rec.realized_roi is None, label
        assert rec.current_price == 110.0 and rec.outcome is None, label
        assert receipt(tp, rec)["hist_close_reason"] == "hist_close_archive_no_bar", label
        assert other.status == tp.PerformanceStatus.MATURED and other.realized_roi == pytest.approx(10.0), label
        assert receipt(tp, other)["hist_bar_date"] == "2026-10-05", label
        assert run.pulls == [] and len(session.posts) == 1, label
        assert tp._LAST_HIST_CLOSE["refused"] == {"hist_close_archive_no_bar": 1}, label
        cached = tp._HIST_ARCHIVE_CACHE["SYNTH.US"]
        assert [b["date"] for b in cached] == [b["date"] for b in synth_bars if SESSION <= b["date"] <= "2026-10-05"], label
    # the v6.47.0 judge still rules on archive bars that do carry the date
    for label, archive, reason in (
            ("conflict", bars(extra=[{"date": SESSION, "close": 105.0, "split": 0.0}]), "hist_close_bar_conflict"),
            ("zero close", bars(0.0), "hist_close_invalid"),
            ("null close", [{"date": SESSION, "close": None, "split": 0.0}], "hist_close_invalid"),
            ("subunit x100", bars(10400.0), "hist_close_scale_mismatch")):
        rec = record(tp)
        audit(tp, [rec], [row()], env=BQ, archive={"SYNTH.US": archive})
        assert rec.status == tp.PerformanceStatus.ACTIVE, label
        assert receipt(tp, rec)["hist_close_reason"] == reason, label


HOURLY = {"date": SESSION, "close": 104.0, "split": 0.0, "source": "yahoo_chart_v8_1h"}


def test_s4c_hourly_reconstruction_is_not_a_witnessed_close():
    """Codex P2 on #894: a symbol_bars_daily row with source yahoo_chart_v8_1h
    is a day rebuilt from hourly prints (archive_market_series.parse_hourly
    keeps the LAST print, not the close), the path archive_symbol_deep takes
    when Yahoo's daily history is short. It must never mature a record, with
    or without the CA-GUARD; the combined mode asks yfinance instead."""
    for label, synth_bars in (("hourly target bar", [b if b["date"] != SESSION else HOURLY for b in bars()]),
                              ("hourly only", [HOURLY]),
                              ("daily and hourly copies", bars(extra=[HOURLY])),
                              ("unknown source", [dict(HOURLY, source="eodhd_eod")]),
                              ("blank source", [dict(HOURLY, source="")])):
        for ca_guard in (False, True):
            rec = record(tp)
            run = audit(tp, [rec], [row()], env=BQ, archive={"SYNTH.US": synth_bars}, ca_guard=ca_guard,
                        patches={"_ca_ledger_factor": lambda *a, **k: None, "_mature_grace_days": lambda: 0})
            assert rec.status == tp.PerformanceStatus.ACTIVE and rec.realized_roi is None, (label, ca_guard)
            assert rec.outcome is None and rec.current_price == 110.0, (label, ca_guard)
            proof = receipt(tp, rec)
            assert proof["reason"] == "quote_outside_target_session", (label, ca_guard)
            assert proof["hist_close_reason"] == "hist_close_archive_not_daily", (label, ca_guard)
            assert run.pulls == [], (label, ca_guard)
            assert tp._LAST_HIST_CLOSE["refused"] == {"hist_close_archive_not_daily": 1}, (label, ca_guard)
            assert tp._LAST_HIST_CLOSE["archive"]["not_daily"] == 1, (label, ca_guard)
            assert tp._LAST_HIST_CLOSE["archive"]["served"] == 0, (label, ca_guard)
            assert "not_daily=1 not_asked=0" in archive_lines(run)[0], (label, ca_guard)
    # a daily bar on another date beside the hourly target bar changes nothing
    rec = record(tp)
    audit(tp, [rec], [row()], env=BQ, archive={"SYNTH.US": [HOURLY, {"date": "2026-09-30", "close": 103.0,
                                                                      "split": 0.0}]})
    assert rec.status == tp.PerformanceStatus.ACTIVE
    assert receipt(tp, rec)["hist_close_reason"] == "hist_close_archive_not_daily"
    # the combined mode lets yfinance decide
    rec = record(tp)
    run = audit(tp, [rec], [row()], env=BQ_THEN, archive={"SYNTH.US": [HOURLY]}, bar_map={"SYNTH.US": bars(95.0)})
    assert run.pulls == ["SYNTH.US"] and rec.status == tp.PerformanceStatus.MATURED
    assert rec.realized_roi == pytest.approx(-5.0) and receipt(tp, rec)["provider"] == "yfinance-history"
    assert tp._LAST_HIST_CLOSE["archive"]["not_daily"] == 1 and tp._LAST_HIST_CLOSE["archive"]["fallback"] == 1
    # the daily source still serves, and the parser keeps every source label
    shaped = tp._archive_bar_rows([{"symbol": "A.US", "date": SESSION, "close": "1", "source": "yahoo_chart_v8_1h"}])
    assert shaped["A.US"][0]["source"] == "yahoo_chart_v8_1h"
    assert tp._ARCHIVE_BAR_DAILY_SOURCES == frozenset({"yahoo_chart_v8"})


def test_s4b_confirmed_ledger_action_after_target_refuses_before_the_archive():
    calls = []

    def ledger(symbol, entry_date, target_session=None):
        calls.append((symbol, str(entry_date)[:10], target_session))
        return 0.5 if (str(entry_date)[:10] == SESSION and target_session is None) else None
    rec = record(tp)
    run = audit(tp, [rec], [row()], env=BQ, archive={"SYNTH.US": bars()}, patches={"_ca_ledger_factor": ledger})
    assert rec.status == tp.PerformanceStatus.ACTIVE
    assert receipt(tp, rec)["hist_close_reason"] == "hist_close_post_target_action"
    assert ("SYNTH.US", SESSION, None) in calls and run.sessions == [1]


# ---------------------------------------------------------------- S5 -----
class _Timeout(Exception):
    pass


FAILURES = [
    ("session raises", dict(session_exc=RuntimeError("GOOGLE_SHEETS_CREDENTIALS missing or unreadable")),
     "RuntimeError"),
    ("http 403", dict(session=FakeSession({"SYNTH.US": bars()}, post_code=403)), "RuntimeError"),
    ("transport timeout", dict(session=FakeSession({"SYNTH.US": bars()}, post_exc=_Timeout("read timed out"))),
     "_Timeout"),
    ("payload errors", dict(session=FakeSession({"SYNTH.US": bars()}, payload_error="Syntax error")), "RuntimeError"),
    ("poll fails", dict(session=FakeSession({"SYNTH.US": bars()}, complete=False, poll_code=500)), "RuntimeError"),
    ("page lost", dict(session=FakeSession({"SYNTH.US": bars(), "AAA.US": bars()}, page_size=1, page_fail=True)),
     "RuntimeError"),
]


def test_s5_query_failure_refuses_every_candidate_and_the_pass_continues():
    in_session = CLOSE - timedelta(hours=4)
    for label, kw, error in FAILURES:
        tp._LAST_HIST_CLOSE.clear()
        recs = [record(tp), record(tp, symbol="AAA.US"), record(tp, symbol="NOW.US", target=in_session)]
        rows = [row(), row(symbol="AAA.US"), row(symbol="NOW.US", price=97.0)]
        run = audit(tp, recs, rows, env=BQ, bar_map={"SYNTH.US": bars(), "AAA.US": bars()},
                    patches={"_mature_grace_days": lambda: 0}, **kw)
        assert [r.status for r in recs] == [tp.PerformanceStatus.ACTIVE, tp.PerformanceStatus.ACTIVE,
                                            tp.PerformanceStatus.MATURED], label   # the pass went on
        for r in recs[:2]:
            proof = receipt(tp, r)
            assert proof["state"] == "PENDING" and proof["reason"] == "quote_outside_target_session", label
            assert proof["hist_close_reason"] == "hist_close_archive_unavailable", label
            assert r.outcome is None and r.current_price == 110.0, label
        assert recs[2].realized_roi == pytest.approx(-3.0), label
        assert run.pulls == [] and run.sessions == [1], label
        st = tp._LAST_HIST_CLOSE
        assert st["refused"] == {"hist_close_archive_unavailable": 2}, label
        assert st["archive"]["unavailable"] is True and st["archive"]["loaded"] is False, label
        assert st["archive"]["error"] == error, label
        assert tp._HIST_ARCHIVE_CACHE == {}, label
        assert archive_lines(run)[0].endswith("unavailable=1 error=%s query_sec=%.2f"
                                              % (error, st["archive"]["query_sec"])), label
        assert run.stats["outcome_skipped_reasons"] == {"quote_outside_target_session": 2}, label


def test_s5b_no_credential_text_reaches_the_read_back_or_the_log():
    leak = RuntimeError('{"type": "service_account", "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIE"}')
    rec = record(tp)
    run = audit(tp, [rec], [row()], env=BQ, session_exc=leak)
    assert rec.status == tp.PerformanceStatus.ACTIVE
    assert tp._LAST_HIST_CLOSE["archive"]["detail"] == "<redacted>"
    joined = "\n".join(run.logs) + json.dumps(tp._LAST_HIST_CLOSE, default=str) + rec.notes
    assert "PRIVATE KEY" not in joined and "MIIE" not in joined and "private_key" not in joined
    assert tp._archive_bar_scrub("Bearer ya29.abc") == "<redacted>"
    assert tp._archive_bar_scrub("HTTP 403: quota exceeded") == "HTTP 403: quota exceeded"
    assert len(tp._archive_bar_scrub("x" * 500)) == 160


# ---------------------------------------------------------------- S6 -----
def test_s6_off_reads_nothing_whatever_the_source():
    tp._LAST_HIST_CLOSE.clear()
    tp._LAST_HIST_CLOSE["sentinel"] = 1
    off_rec = record(tp)
    off = audit(tp, [off_rec], [row()], bar_map={"SYNTH.US": bars()}, archive={"SYNTH.US": bars()})
    for source in ("bq_archive", "bq_archive_then_yfinance"):
        for gate in (None, "off", "1", "enforced"):
            rec = record(tp)
            env = {"TRACK_HIST_CLOSE_SOURCE": source}
            if gate is not None:
                env["TRACK_HIST_TARGET_CLOSE"] = gate
            run = audit(tp, [rec], [row()], env=env, bar_map={"SYNTH.US": bars()}, archive={"SYNTH.US": bars()})
            assert run.sessions == [] and run.pulls == [], (source, gate)
            assert state(rec) == state(off_rec), (source, gate)
            assert rec.status == tp.PerformanceStatus.ACTIVE, (source, gate)
            assert hist_lines(run) == [] and archive_lines(run) == [], (source, gate)
            assert run.logs == off.logs and run.stats == off.stats, (source, gate)
    assert tp._LAST_HIST_CLOSE == {"sentinel": 1}
    assert tp._HIST_ARCHIVE_STATE == {} or not tp._HIST_ARCHIVE_STATE.get("loaded")


# ---------------------------------------------------------------- S7 -----
def test_s7_observe_measures_from_the_archive_and_changes_nothing():
    off_rec, obs_rec = record(tp), record(tp)
    off = audit(tp, [off_rec], [row()], archive={"SYNTH.US": bars()})
    session = FakeSession({"SYNTH.US": bars()})
    run = audit(tp, [obs_rec], [row()], env={"TRACK_HIST_TARGET_CLOSE": "observe",
                                              "TRACK_HIST_CLOSE_SOURCE": " BQ_Archive "}, session=session)
    assert state(obs_rec) == state(off_rec) and obs_rec.status == tp.PerformanceStatus.ACTIVE
    assert len(session.posts) == 1 and run.pulls == [] and run.sessions == [1]
    assert run.stats == off.stats
    assert hist_lines(run) == ["[HIST-CLOSE v6.47.0] mode=observe candidates=1 witnessed=1 (1W:1) "
                               "matured=0 refused=0 (-) pulls=0 ok=0 failed=0 breaker=closed deferred=0"]
    assert archive_lines(run)[0].startswith("[ARCHIVE-BAR v6.53.0] source=bq_archive candidates=1 "
                                            "symbols=1 asked=1 rows=1 served=1 no_bar=0")
    assert [m for m in run.logs if not m.startswith(("[HIST-CLOSE v", "[ARCHIVE-BAR v"))] == off.logs
    assert tp._LAST_HIST_CLOSE["witnessed"] == 1 and tp._LAST_HIST_CLOSE["matured"] == 0
    assert tp._LAST_HIST_CLOSE["source"] == "bq_archive"


# ---------------------------------------------------------------- S8 -----
def test_s8_archive_then_yfinance_falls_back_per_symbol():
    a, b = record(tp, symbol="AAA.US"), record(tp, symbol="BBB.US")
    session = FakeSession({"AAA.US": bars(104.0)})
    run = audit(tp, [a, b], [row(symbol="AAA.US"), row(symbol="BBB.US")], env=BQ_THEN,
                session=session, bar_map={"BBB.US": bars(95.0), "AAA.US": bars(999.0)})
    assert run.pulls == ["BBB.US"]                                # only the no-bar symbol
    assert a.status == b.status == tp.PerformanceStatus.MATURED
    assert a.realized_roi == pytest.approx(4.0) and receipt(tp, a)["provider"] == "bq_archive_bar"
    assert b.realized_roi == pytest.approx(-5.0) and receipt(tp, b)["provider"] == "yfinance-history"
    st = tp._LAST_HIST_CLOSE
    assert st["source"] == "bq_archive_then_yfinance" and st["pulls"] == 1 and st["matured"] == 2
    assert st["archive"]["served"] == 1 and st["archive"]["no_bar"] == 1 and st["archive"]["fallback"] == 1
    assert archive_lines(run)[0].startswith("[ARCHIVE-BAR v6.53.0] source=bq_archive_then_yfinance "
                                            "candidates=2 symbols=2 asked=2 rows=1 served=1 no_bar=1 "
                                            "not_daily=0 not_asked=0 fallback=1 unavailable=0")
    # a later-session-only archive bar (inside a widened query) is no bar: yfinance decides
    x, y = record(tp, symbol="XXX.US"), record(tp, symbol="YYY.US", target=LATER_TARGET)
    run = audit(tp, [x, y], [row(symbol="XXX.US"), row(symbol="YYY.US")], env=BQ_THEN,
                archive={"XXX.US": [{"date": "2026-10-02", "close": 150.0, "split": 0.0}], "YYY.US": bars()},
                bar_map={"XXX.US": bars(104.0)})
    assert run.pulls == ["XXX.US"]
    assert x.status == y.status == tp.PerformanceStatus.MATURED
    assert receipt(tp, x)["provider"] == "yfinance-history" and x.realized_roi == pytest.approx(4.0)
    assert receipt(tp, y)["provider"] == "bq_archive_bar" and y.realized_roi == pytest.approx(10.0)
    # the fallback keeps the yfinance budget: budget 1 defers the second no-bar symbol
    c, d = record(tp, symbol="CCC.US"), record(tp, symbol="DDD.US")
    run = audit(tp, [c, d], [row(symbol="CCC.US"), row(symbol="DDD.US")],
                env=dict(BQ_THEN, TRACK_HIST_CLOSE_BUDGET="1"), archive={},
                bar_map={"CCC.US": bars(), "DDD.US": bars()})
    assert len(run.pulls) == 1
    statuses = sorted(r.status.value for r in (c, d))
    assert statuses == sorted([tp.PerformanceStatus.MATURED.value, tp.PerformanceStatus.ACTIVE.value])
    deferred = c if c.status == tp.PerformanceStatus.ACTIVE else d
    assert receipt(tp, deferred)["hist_close_reason"] == "hist_close_budget_exhausted"
    # a failed query falls back whole
    e = record(tp, symbol="EEE.US")
    run = audit(tp, [e], [row(symbol="EEE.US")], env=BQ_THEN, session_exc=RuntimeError("down"),
                bar_map={"EEE.US": bars(104.0)})
    assert run.pulls == ["EEE.US"] and e.status == tp.PerformanceStatus.MATURED
    assert receipt(tp, e)["provider"] == "yfinance-history"
    assert tp._LAST_HIST_CLOSE["archive"]["unavailable"] is True and tp._LAST_HIST_CLOSE["archive"]["fallback"] == 1


# ---------------------------------------------------------------- S9 -----
def test_s9_real_query_polls_pages_and_parses():
    archive = {"A.US": bars(), "B.US": bars(), "C.US": bars()}
    session = FakeSession(archive, complete=False, page_size=4)
    rows = tp._archive_bar_query(session, BQ_PROJECT, "tfb_archive", "US", ["A.US", "B.US", "C.US"],
                                 "2026-09-29", SESSION)
    assert len(rows) == 9 and rows[0] == {"symbol": "A.US", "date": "2026-09-29", "open": None, "high": None,
                                          "low": None, "close": "106.0", "adj_close": None,
                                          "source": "yahoo_chart_v8"}
    assert len(session.posts) == 1
    assert [g["params"] for g in session.gets] == [
        {"location": "US", "timeoutMs": 60000},                  # the poll while not complete
        {"location": "US", "pageToken": "tok4"}, {"location": "US", "pageToken": "tok8"}]
    assert all(g["url"].endswith("/projects/%s/queries/job_tfb_1" % BQ_PROJECT) for g in session.gets)
    shaped = tp._archive_bar_rows(rows + [{"symbol": "d.us", "date": "2026-10-01T00:00:00", "close": "abc"},
                                          {"symbol": "", "date": SESSION, "close": "1"}, "junk"])
    assert sorted(shaped) == ["A.US", "B.US", "C.US", "D.US"]
    assert shaped["A.US"][0] == {"date": "2026-09-29", "close": 106.0, "split": 0.0, "source": "yahoo_chart_v8"}
    assert shaped["D.US"] == [{"date": SESSION, "close": None, "split": 0.0, "source": ""}]
    assert tp._hist_target_close(shaped["A.US"], SESSION, 110.0) == (104.0, "")
    assert tp._hist_target_close(shaped["D.US"], SESSION) == (None, "hist_close_invalid")
    with pytest.raises(RuntimeError):
        tp._archive_bar_query(FakeSession(archive, post_code=500), BQ_PROJECT, "tfb_archive", "US", ["A.US"],
                              SESSION, SESSION)
    with pytest.raises(RuntimeError):
        tp._archive_bar_query(FakeSession(archive, complete=False, poll_code=404), BQ_PROJECT, "tfb_archive",
                              "US", ["A.US"], SESSION, SESSION)
    saved = tp._ARCHIVE_BAR_MAX_PAGES
    tp._ARCHIVE_BAR_MAX_PAGES = 1
    try:
        with pytest.raises(RuntimeError):
            tp._archive_bar_query(FakeSession(archive, page_size=2), BQ_PROJECT, "tfb_archive", "US",
                                  ["A.US", "B.US", "C.US"], "2026-09-29", SESSION)
    finally:
        tp._ARCHIVE_BAR_MAX_PAGES = saved
    if DELIVERED == TRACKER:    # a relocated copy (golden / mutant run) has no sibling module
        ams = tp._archive_bar_module()
        assert ams is not None and callable(ams.make_session) and callable(ams._err)
        assert ams.BQ_API == "https://bigquery.googleapis.com/bigquery/v2"
        err = types.SimpleNamespace(status_code=403, json=lambda: {"error": {"message": "quota"}})
        assert ams._err(err) == "HTTP 403: quota"


# ---------------------------------------------------------------- D1 -----
def _due_fixture(mod):
    """A (due, target session 10 d ago), B (due, 1 d), C (not due, +3 d), D
    (due, 5 d), E (target unparsable), F (one due 2 d + one not due +1 d),
    G (not due, +10 d). Targets sit on a session close, so every due target
    session is a PAST session and today's quote lies outside it."""
    recs = [record(mod, symbol="A.US", target=CLOSE - timedelta(days=10)),
            record(mod, symbol="B.US", target=CLOSE - timedelta(days=1)),
            record(mod, symbol="C.US", target=NOW + timedelta(days=3)),
            record(mod, symbol="D.US", target=CLOSE - timedelta(days=5)),
            record(mod, symbol="E.US"),
            record(mod, symbol="F.US", target=CLOSE - timedelta(days=2)),
            record(mod, symbol="F.US", target=NOW + timedelta(days=1), horizon="WEEK_2"),
            record(mod, symbol="G.US", target=NOW + timedelta(days=10))]
    recs[4].target_date = "not a time"
    for r in recs:
        r.last_updated = NOW - timedelta(days=1)       # so the pass's stamp is visible
    rows = [row(symbol=s) for s in ("A.US", "B.US", "C.US", "D.US", "E.US", "F.US", "G.US")]
    return recs, rows


def test_d1_due_only_off_prices_every_active_symbol():
    recs, rows = _due_fixture(tp)
    run = audit(tp, recs, rows, filtering=True)
    assert run.requested == [["A.US", "B.US", "C.US", "D.US", "E.US", "F.US", "G.US"]]
    assert all(r.current_price == 110.0 for r in recs)         # every record took the live mark
    assert run.stats["total"] == 7 and run.stats["priced"] == 7 and "due_only_skipped" not in run.stats
    assert due_lines(run) == []
    assert all(r.last_updated == NOW for r in recs)
    for spelling in ("0", "off", "false", "", "yes please"):
        recs, rows = _due_fixture(tp)
        run = audit(tp, recs, rows, filtering=True, env={"TRACK_AUDIT_DUE_ONLY": spelling})
        assert run.requested == [["A.US", "B.US", "C.US", "D.US", "E.US", "F.US", "G.US"]], spelling
        assert due_lines(run) == [], spelling


# ---------------------------------------------------------------- D2 -----
def test_d2_due_only_prices_only_due_symbols_oldest_first_and_leaves_not_due_rows_alone():
    recs, rows = _due_fixture(tp)
    before = {id(r): state(r) for r in recs}
    run = audit(tp, recs, rows, filtering=True, env={"TRACK_AUDIT_DUE_ONLY": "1"})
    assert run.requested == [["A.US", "D.US", "F.US", "B.US"]]           # oldest target first, due only
    a, b, c, d, e, f_due, f_later, g = recs
    # due symbols: priced, receipts written (their targets passed, the quote is today's)
    for r in (a, b, d, f_due):
        assert r.current_price == 110.0 and tp._OUTCOME_RECEIPT in r.notes, r.symbol
        assert receipt(tp, r)["reason"] == "quote_outside_target_session", r.symbol
    # a mixed symbol's not-due record takes the fresh mark like today
    assert f_later.current_price == 110.0 and f_later.unrealized_roi == pytest.approx(10.0)
    assert tp._OUTCOME_RECEIPT not in (f_later.notes or "")
    # not-due symbols: NOT priced -> byte-identical to the existing 'no price' path
    for r in (c, g):
        after = state(r)
        assert after["current_price"] == 0.0 and after["unrealized_roi"] == 0.0, r.symbol
        assert after["notes"] == "" and after["status"] == tp.PerformanceStatus.ACTIVE.value, r.symbol
        changed = {k for k in after if after[k] != before[id(r)][k]}
        assert changed == {"last_updated"}, (r.symbol, changed)       # the existing stamp, nothing else
        assert r.last_updated == NOW
    # an unparsable target is not due: the existing PENDING receipt without evidence
    assert e.current_price == 0.0 and receipt(tp, e)["reason"] == "outcome_target_time_unknown"
    assert "provider" not in receipt(tp, e)
    assert run.stats["total"] == 4 and run.stats["priced"] == 4 and run.stats["due_only_skipped"] == 3
    assert run.stats["outcome_skipped_reasons"] == {"quote_outside_target_session": 4,
                                                    "outcome_target_time_unknown": 1}
    assert due_lines(run) == ["[v6.53.0 DUE-ONLY] audit due-only: 4 due symbols priced, 3 not-due symbols "
                              "skipped (due records=4, oldest target=2026-09-28, witnessed=4)"]
    assert tp._LAST_DUE_ONLY == {"due_symbols": 4, "skipped": 3, "due_records": 4, "active_symbols": 7,
                                 "oldest_target": "2026-09-28", "witnessed": 4}
    # the not-due rows equal the rows a breaker-skipped (unpriced) symbol gets with the gate off
    off_recs, off_rows = _due_fixture(tp)
    audit(tp, off_recs, [r for r in off_rows if r["symbol"] not in ("C.US", "E.US", "G.US")], filtering=True)
    assert state(off_recs[2]) == state(c) and state(off_recs[7]) == state(g)
    assert state(off_recs[4]) == state(e)
    assert [state(r) for r in off_recs] == [state(r) for r in recs]     # the whole pass, row for row


def test_d2b_due_only_with_no_due_symbol_prices_nothing_and_says_so():
    recs = [record(tp, symbol="C.US", target=NOW + timedelta(days=3))]
    run = audit(tp, recs, [row(symbol="C.US")], filtering=True, env={"TRACK_AUDIT_DUE_ONLY": "on"})
    assert run.requested == [] and recs[0].current_price == 0.0
    assert run.stats["total"] == 0 and run.stats["priced"] == 0 and run.stats["due_only_skipped"] == 1
    assert due_lines(run) == ["[v6.53.0 DUE-ONLY] audit due-only: 0 due symbols priced, 1 not-due symbols "
                              "skipped (due records=0, oldest target=-, witnessed=0)"]
    assert not any(m.startswith("[v6.18.0 PRICE-FEED]") and "feed dead" in m for m in run.logs)


def test_d2c_due_only_keeps_the_hist_close_candidates():
    """Due-only prices exactly the symbols the v6.47.0 judge needs."""
    recs, rows = _due_fixture(tp)
    session = FakeSession({s: bars() for s in ("A.US", "B.US", "D.US", "F.US")})
    run = audit(tp, recs, rows, filtering=True, env=dict(BQ, TRACK_AUDIT_DUE_ONLY="1"), session=session)
    assert run.requested == [["A.US", "D.US", "F.US", "B.US"]]
    params = {p["name"]: p for p in session.posts[0]["body"]["queryParameters"]}
    assert [v["value"] for v in params["symbols"]["parameterValue"]["arrayValues"]] == ["A.US", "B.US", "D.US", "F.US"]
    M, A = tp.PerformanceStatus.MATURED, tp.PerformanceStatus.ACTIVE
    assert [r.status for r in recs] == [M, M, A, M, A, M, A, A]
    assert all(receipt(tp, r)["provider"] == "bq_archive_bar" for r in recs if r.status == M)


# ---------------------------------------------------------------- D3 -----
def test_d3_due_only_worklist_reaches_the_fallback_in_order():
    recs, rows = _due_fixture(tp)
    calls = []

    async def fake_fallback(symbols, keep_order=False):
        calls.append((list(symbols), keep_order))
        return tp.OutcomePrices()
    run = audit(tp, recs, [], filtering=True, env={"TRACK_AUDIT_DUE_ONLY": "1"},
                patches={"_price_fallback_enabled": lambda: True, "_yahoo_chart_fallback_prices": fake_fallback})
    assert calls == [(["A.US", "D.US", "F.US", "B.US"], True)]
    assert run.requested[0] == ["A.US", "D.US", "F.US", "B.US"]
    calls.clear()
    audit(tp, _due_fixture(tp)[0], [], filtering=True,
          patches={"_price_fallback_enabled": lambda: True, "_yahoo_chart_fallback_prices": fake_fallback})
    assert calls == [(["A.US", "B.US", "C.US", "D.US", "E.US", "F.US", "G.US"], False)]


def test_d3b_real_fallback_keeps_the_given_order_only_when_asked():
    shuffles = []
    launched = []
    fake_random = types.SimpleNamespace(shuffle=lambda seq: shuffles.append(list(seq)), random=lambda: 0.0,
                                        uniform=lambda a, b: a)

    async def fetch(session, sym, timeout):
        launched.append(sym)
        return None, "other", "quote_transport_failed"
    with harness(tp, patches={"random": fake_random, "_yahoo_fetch_one_outcome_status": fetch,
                              "_fallback_conc": lambda: 1, "_fallback_spacing_sec": lambda: 0.0,
                              "ASYNC_HTTP_AVAILABLE": False, "aiohttp": None}):
        out = asyncio.run(tp._yahoo_chart_fallback_prices(["Z.US", "Y.US", "X.US"], keep_order=True))
        assert shuffles == [] and launched == ["Z.US", "Y.US", "X.US"]
        assert dict(out) == {} and set(out.reasons) == {"Z.US", "Y.US", "X.US"}
        launched.clear()
        asyncio.run(tp._yahoo_chart_fallback_prices(["Z.US", "Y.US", "X.US"]))
        assert shuffles == [["Z.US", "Y.US", "X.US"]]             # the default still rotates
        launched.clear()
        asyncio.run(tp._yahoo_chart_fallback_prices(["Z.US", "Y.US", "X.US"], keep_order=False))
        assert len(shuffles) == 2


# ---------------------------------------------------------------- D4 -----
def test_d4_pure_helper_table():
    recs, _rows = _due_fixture(tp)
    due, info = tp._audit_due_symbols(recs, NOW)
    assert due == ["A.US", "D.US", "F.US", "B.US"]
    assert info == {"due_symbols": 4, "skipped": 3, "due_records": 4, "active_symbols": 7,
                    "oldest_target": "2026-09-28"}
    assert tp._audit_due_symbols([], NOW) == ([], {"due_symbols": 0, "skipped": 0, "due_records": 0,
                                                   "active_symbols": 0, "oldest_target": ""})
    exact = record(tp, symbol="X.US", target=NOW)                 # at now: due (now >= target)
    assert tp._audit_due_symbols([exact], NOW)[0] == ["X.US"]
    assert tp._audit_due_symbols([exact], NOW - timedelta(seconds=1))[0] == []
    tie_a, tie_b = record(tp, symbol="TB.US", target=NOW - timedelta(days=1)), record(tp, symbol="TA.US", target=NOW - timedelta(days=1))
    assert tp._audit_due_symbols([tie_a, tie_b], NOW)[0] == ["TB.US", "TA.US"]   # ties keep first-seen order
    for word, want in (("", "yfinance"), ("yfinance", "yfinance"), ("BQ_ARCHIVE", "bq_archive"),
                       (" bq_archive_then_yfinance ", "bq_archive_then_yfinance"), ("1", "yfinance"),
                       ("bq", "yfinance"), ("archive", "yfinance")):
        with harness(tp, env={"TRACK_HIST_CLOSE_SOURCE": word}):
            assert tp._hist_close_source() == want, word
    for word, want in (("", False), ("0", False), ("off", False), ("1", True), ("on", True), ("True", True),
                       ("yes", True), ("enforce", False)):
        with harness(tp, env={"TRACK_AUDIT_DUE_ONLY": word}):
            assert tp._audit_due_only_enabled() is want, word
    with harness(tp):
        assert (tp._bq_project(), tp._bq_dataset(), tp._bq_location()) == (BQ_PROJECT, "tfb_archive", "US")
    with harness(tp, env={"TFB_BQ_PROJECT": " p1 ", "TFB_BQ_DATASET": "d1", "TFB_BQ_LOCATION": "EU"}):
        assert (tp._bq_project(), tp._bq_dataset(), tp._bq_location()) == ("p1", "d1", "EU")
    assert tp._ARCHIVE_BAR_PROVIDER == "bq_archive_bar" and tp._HIST_CLOSE_KIND == "historical_daily_close"
    assert tp._ARCHIVE_BAR_MAX_SYMBOLS == 2000 and tp._ARCHIVE_BAR_TABLE == "symbol_bars_daily"


def test_d4b_symbol_cap_marks_the_rest_not_asked():
    saved = tp._ARCHIVE_BAR_MAX_SYMBOLS
    tp._ARCHIVE_BAR_MAX_SYMBOLS = 1
    try:
        a, b = record(tp, symbol="AAA.US"), record(tp, symbol="BBB.US")
        session = FakeSession({"AAA.US": bars(), "BBB.US": bars()})
        run = audit(tp, [a, b], [row(symbol="AAA.US"), row(symbol="BBB.US")], env=BQ, session=session)
        params = {p["name"]: p for p in session.posts[0]["body"]["queryParameters"]}
        assert params["symbols"]["parameterValue"] == {"arrayValues": [{"value": "AAA.US"}]}
        assert a.status == tp.PerformanceStatus.MATURED and b.status == tp.PerformanceStatus.ACTIVE
        assert receipt(tp, b)["hist_close_reason"] == "hist_close_archive_not_asked"
        assert tp._LAST_HIST_CLOSE["archive"]["not_asked"] == 1 and run.pulls == []
    finally:
        tp._ARCHIVE_BAR_MAX_SYMBOLS = saved


# ---------------------------------------------------------------- W1 -----
def _step(path, name):
    text = path.read_text(encoding="utf-8")
    parts = re.split(r"^\s{6}- name: ", text, flags=re.M)
    hits = [p for p in parts if p.splitlines() and name in p.splitlines()[0]]
    assert len(hits) == 1, (path.name, name, len(hits))
    return text, hits[0]


ENV_LINES = [("TRACK_HIST_CLOSE_SOURCE", "yfinance"), ("TRACK_AUDIT_DUE_ONLY", "0"),
             ("TFB_BQ_PROJECT", BQ_PROJECT), ("TFB_BQ_DATASET", "tfb_archive"), ("TFB_BQ_LOCATION", "US")]


def test_w1_workflows_crons_env_parity_version_and_pin():
    clock, step = _step(ROOT / ".github/workflows/track_performance.yml", "Run performance clock")
    daily, dstep = _step(ROOT / ".github/workflows/daily_sync.yml", "Track Performance (record + audit)")
    crons = re.findall(r'^\s+- cron: "([^"]+)"', clock, re.M)
    assert crons == ["47 11 * * *", "47 15 * * *", "30 1 * * *"], crons
    assert '- cron: "30 1 * * *"   # 04:30 Riyadh - after the US close (21:00 UTC since US DST ended 1 Nov 2026)' in clock
    assert "# VERSION 1.0.7 (2026-11-02)" in clock and clock.index("# VERSION 1.0.7") < clock.index("# VERSION 1.0.6")
    assert "TRACK_SCHEDULED_AUDIT=1" in clock.split("# VERSION 1.0.6")[0]
    for name, default in ENV_LINES:
        want = "${{ vars.%s || '%s' }}" % (name, default)
        for label, text in (("clock", clock), ("clock step", step), ("daily", daily), ("daily step", dstep)):
            assert re.findall(r"^\s+%s:\s*(.+)$" % name, text, re.M) == [want], (name, label)
    for name, _default in ENV_LINES:
        clock_line = [ln.strip() for ln in step.splitlines() if re.match(r"^\s+%s:" % name, ln)]
        daily_line = [ln.strip() for ln in dstep.splitlines() if re.match(r"^\s+%s:" % name, ln)]
        assert clock_line == daily_line and len(clock_line) == 1, name          # ENV PARITY
    assert "TRACK_HIST_TARGET_CLOSE" not in daily                               # the v6.47.0 invariant holds
    src = DELIVERED.read_text(encoding="utf-8")
    assert tp.SCRIPT_VERSION == "6.53.0"
    assert "(v%s)" % tp.SCRIPT_VERSION in src.splitlines()[4]
    assert re.search(r'os\.getenv\("TRACK_HIST_CLOSE_SOURCE"\)\s*or\s*"yfinance"', src)
    assert re.search(r'os\.getenv\("TRACK_AUDIT_DUE_ONLY"\)\s*or\s*"0"', src)
    version_line = re.search(r'^SCRIPT_VERSION = "6\.53\.0"', src, re.M)
    assert version_line and src.index("# v6.53.0 (2026-11-02)") < version_line.start()
    assert src.index("# v6.50.0 (2026-10-25)") < src.index("# v6.53.0 (2026-11-02)")
    vd = (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("track_performance", "%s", "track performance")' % tp.SCRIPT_VERSION in vd


# ---------------------------------------------------------------- N1 -----
def _mixed(mod):
    """quote_outside / matured-now / price_unavailable / stale / bad entry / not due."""
    return ([record(mod),
             record(mod, symbol="NOW.US", target=CLOSE - timedelta(hours=4)),
             record(mod, symbol="GONE.US"),
             record(mod, symbol="OLD.US", target=CLOSE - timedelta(hours=4)),
             record(mod, symbol="BAD.US", entry=0.0),
             record(mod, symbol="LATER.US", target=NOW + timedelta(days=3))],
            [row(), row(symbol="NOW.US", price=97.0),
             row(symbol="OLD.US", quote=CLOSE - timedelta(days=1)), row(symbol="BAD.US"), row(symbol="LATER.US")])


def _norm_logs(mod, logs):
    return [m.replace(mod.SCRIPT_VERSION, "X") for m in logs]


def test_n1_off_is_byte_identical_to_main():
    b = base()
    assert b.SCRIPT_VERSION == "6.50.0" and not hasattr(b, "_hist_close_source")
    want_recs, rows = _mixed(b)
    want = audit(b, want_recs, rows, bar_map={})
    for env in ({}, {"TRACK_HIST_CLOSE_SOURCE": "bq_archive"}, {"TRACK_AUDIT_DUE_ONLY": "0"},
                {"TRACK_HIST_CLOSE_SOURCE": "bq_archive_then_yfinance", "TRACK_AUDIT_DUE_ONLY": "off"}):
        got_recs, rows = _mixed(tp)
        got = audit(tp, got_recs, rows, env=env, bar_map={"SYNTH.US": bars()}, archive={"SYNTH.US": bars()})
        assert [state(r) for r in got_recs] == [state(r) for r in want_recs], env
        assert got.stats == want.stats, env
        assert _norm_logs(tp, got.logs) == _norm_logs(b, want.logs), env
        assert got.pulls == [] and got.sessions == [], env
    # the v6.47.0 gate with the default source is the v6.50.0 enforce, line for line
    for mode in ("observe", "enforce"):
        want_recs, rows = _mixed(b)
        want = audit(b, want_recs, rows, env={"TRACK_HIST_TARGET_CLOSE": mode}, bar_map={"SYNTH.US": bars()})
        got_recs, rows = _mixed(tp)
        got = audit(tp, got_recs, rows, env={"TRACK_HIST_TARGET_CLOSE": mode, "TRACK_HIST_CLOSE_SOURCE": "yfinance"},
                    bar_map={"SYNTH.US": bars()}, archive={"SYNTH.US": bars(50.0)})
        assert [state(r) for r in got_recs] == [state(r) for r in want_recs], mode
        assert got.stats == want.stats and got.pulls == want.pulls == ["SYNTH.US"], mode
        assert _norm_logs(tp, got.logs) == _norm_logs(b, want.logs) and got.sessions == [], mode


# ---------------------------------------------------------------- N2 -----
def test_n2_added_lines_are_ascii_and_nothing_removed():
    b = base()
    import difflib
    old = Path(b.__file__).read_text(encoding="utf-8").splitlines()
    new = DELIVERED.read_text(encoding="utf-8").splitlines()
    diff = list(difflib.unified_diff(old, new, lineterm="", n=0))
    added = [ln[1:] for ln in diff if ln.startswith("+") and not ln.startswith("+++")]
    removed = [ln[1:] for ln in diff if ln.startswith("-") and not ln.startswith("---")]
    title = "TADAWUL FAST BRIDGE \u2013 ADVANCED PERFORMANCE ANALYTICS ENGINE (v%s)"
    assert title % "6.50.0" in removed and 'SCRIPT_VERSION = "6.50.0"' in removed
    assert len(removed) <= 16, removed                             # the anchored replacements only
    fresh = [ln for ln in added if ln != title % "6.53.0"]
    assert fresh and all(ln.isascii() for ln in fresh), [ln for ln in fresh if not ln.isascii()][:3]
    import ast

    def fns(text):
        return {n.name for n in ast.walk(ast.parse(text)) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    gone = fns("\n".join(old)) - fns("\n".join(new))
    assert not gone, gone
    assert fns("\n".join(new)) - fns("\n".join(old)) == {
        "_hist_close_source", "_bq_project", "_bq_dataset", "_bq_location", "_audit_due_only_enabled",
        "_audit_due_symbols", "_archive_bar_module", "_archive_bar_session", "_archive_bar_scrub",
        "_archive_bar_query", "_archive_bar_rows", "_hist_archive_prefetch", "_hist_close_bars_for",
        "_hist_close_candidate_session"}


if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = skipped = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
            print("ok  ", name)
        except pytest.skip.Exception as exc:
            skipped += 1
            print("SKIP", name, "::", exc)
    print(f"PASS {passed}/{len(tests)}" + (f" (skipped {skipped})" if skipped else ""))
