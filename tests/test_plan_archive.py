"""scripts/plan_archive.py v1.0.0 - each Today Action Plan run, kept as rows in BigQuery for the IBKR dashboard
(owner, 13 Nov 2026). Figures and tickers here are invented."""
from __future__ import annotations

import datetime as dt
from pathlib import Path

from scripts import plan_archive as pa

ROOT = Path(__file__).resolve().parents[1]
NOW = dt.datetime(2026, 11, 12, 22, 30, tzinfo=dt.timezone.utc)   # 01:30 on 13 Nov in Riyadh

STEPS = [{"symbol": "AAA.US", "kind": "stop", "label": "RAISE STOP", "do": "Raise the stop on AAA.US to 9.50.",
          "levels": "was 9.10", "why": "a new high", "source": "Exit_Plan", "urgency": 2.0},
         {"symbol": "BBB.US", "kind": "buy", "label": "BUY", "do": "Buy 10 BBB.US with a limit between 20.00 and 20.40.",
          "levels": "stop 18.60", "why": "rank 1", "source": "Daily_Plan"}]
TILES = [("CASH AT IBKR", "1,000 SAR", "IBKR capture of 2026-11-13 01:00 Riyadh", "plain"),
         ("NEWS GATE", "GO", "00:30 Riyadh · observe-only", "good")]


class Resp:
    def __init__(self, status, body=None):
        self.status_code, self._body = status, body or {}

    def json(self):
        return self._body


class Session:
    """A fake BigQuery: answers in the order given, and keeps every call."""

    def __init__(self, gets=(), posts=()):
        self.gets, self.posts, self.calls = list(gets), list(posts), []

    def get(self, url, timeout=None):
        self.calls.append(("GET", url, None))
        return self.gets.pop(0)

    def post(self, url, json=None, timeout=None):
        self.calls.append(("POST", url, json))
        return self.posts.pop(0)


def _env(monkeypatch, on=True):
    monkeypatch.setenv("TFB_TAP_BQ", "on" if on else "off")
    monkeypatch.setenv("TFB_BQ_PROJECT", "proj-1")
    monkeypatch.setenv("TFB_BQ_DATASET", "ds_1")
    monkeypatch.delenv("TFB_TAP_BQ_TABLE", raising=False)


# ----------------------------------------------------------------------------- P1 the rows
def test_p1_rows_copy_the_run_and_carry_the_riyadh_day():
    rows = pa.plan_rows(STEPS, TILES, "2 step(s) on holdings", ["Exit_Plan"], [], NOW, "77-1", "1.4.0")
    assert [(r["row_type"], r["seq"]) for r in rows] == [("summary", 0), ("step", 1), ("step", 2), ("tile", 1), ("tile", 2)]
    assert all(r["plan_date"] == "2026-11-13" and r["run_at"] == "2026-11-12T22:30:00.000000Z" for r in rows)
    assert all(r["run_id"] == "77-1" and r["version"] == "1.4.0" for r in rows)
    s = rows[0]
    assert s["instruction"] == "2 step(s) on holdings" and s["levels"] == "Exit_Plan" and s["tone"] == "alert"
    b = rows[2]
    assert (b["kind"], b["label"], b["symbol"], b["source"]) == ("buy", "BUY", "BBB.US", "Daily_Plan")
    assert b["instruction"].startswith("Buy 10 BBB.US") and b["levels"] == "stop 18.60" and b["why"] == "rank 1"
    t = rows[3]
    assert (t["label"], t["instruction"], t["levels"], t["tone"]) == TILES[0]
    assert set(rows[0]) == {f["name"] for f in pa.SCHEMA}          # every row carries exactly the table's columns
    calm = pa.plan_rows([], [], "0 step(s)", [], [], NOW, "x", "v")
    assert calm[0]["tone"] == "ok" and len(calm) == 1
    flagged = pa.plan_rows([], [], "0 step(s) · NEWS GATE: RISK OFF", [], [], NOW, "x", "v", alert=True)
    assert flagged[0]["tone"] == "alert"                            # the page's own alert, with every source fresh
    assert pa.plan_rows([], [], "", [], [], NOW, "x", "v", alert=False)[0]["tone"] == "ok"
    long = pa.plan_rows([dict(STEPS[0], why="w" * 5000)], [], "", [], [], NOW, "x", "v")
    assert len(long[1]["why"]) == pa.MAX_TEXT and long[1]["why"].endswith("…")


def test_p1_run_id_from_the_workflow_or_the_clock(monkeypatch):
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
    assert pa.run_id(NOW) == "123-2"
    monkeypatch.delenv("GITHUB_RUN_ID")
    assert pa.run_id(NOW) == "local-20261112T223000Z"


# ----------------------------------------------------------------------------- P2 the table and the insert
def test_p2_creates_the_table_once_then_inserts_with_ids(monkeypatch):
    _env(monkeypatch)
    rows = pa.plan_rows(STEPS, TILES, "s", [], [], NOW, "9-1", "1.4.0")
    s = Session(gets=[Resp(404)], posts=[Resp(200), Resp(200, {})])
    logs = []
    assert pa.archive(rows, logs.append, session=s) == 5
    create = s.calls[1][2]
    assert create["timePartitioning"] == {"type": "DAY", "field": "plan_date"}
    assert create["tableReference"] == {"projectId": "proj-1", "datasetId": "ds_1", "tableId": "today_action_plan"}
    ins = s.calls[2]
    assert ins[1].endswith("/projects/proj-1/datasets/ds_1/tables/today_action_plan/insertAll")
    assert [r["insertId"] for r in ins[2]["rows"]] == ["9-1-summary-0", "9-1-step-1", "9-1-step-2", "9-1-tile-1", "9-1-tile-2"]
    assert logs == ["plan archive: 5 row(s) appended to ds_1.today_action_plan (table created)"]
    s2 = Session(gets=[Resp(200)], posts=[Resp(200, {})])
    logs.clear()
    assert pa.archive(rows, logs.append, session=s2) == 5 and len(s2.calls) == 2 and "created" not in logs[0]


def test_p2_a_new_table_answering_404_is_waited_out(monkeypatch):
    _env(monkeypatch)
    rows = pa.plan_rows(STEPS, [], "s", [], [], NOW, "9-1", "v")
    waits = []
    s = Session(gets=[Resp(404)], posts=[Resp(409), Resp(404), Resp(404), Resp(200, {})])
    assert pa.archive(rows, lambda m: None, session=s, sleep=waits.append) == 3 and waits == [15.0, 15.0]
    s = Session(gets=[Resp(200)], posts=[Resp(404)] * 4)
    logs = []
    assert pa.archive(rows, logs.append, session=s, sleep=waits.append) is None
    assert logs == ["plan archive not written: RuntimeError: insert today_action_plan: HTTP 404 "]


def test_p2_refusals_are_reported_and_never_raised(monkeypatch):
    _env(monkeypatch)
    rows = pa.plan_rows(STEPS, [], "s", [], [], NOW, "9-1", "v")
    refused = Resp(200, {"insertErrors": [{"index": 0, "errors": [{"message": "no such field: extra"}]}]})
    logs = []
    assert pa.archive(rows, logs.append, session=Session(gets=[Resp(200)], posts=[refused])) is None
    assert logs[-1] == "plan archive not written: RuntimeError: insert today_action_plan: 1 row(s) refused: no such field: extra"
    denied = Resp(403, {"error": {"message": "Access Denied"}})
    assert pa.archive(rows, logs.append, session=Session(gets=[denied])) is None
    assert logs[-1] == "plan archive not written: RuntimeError: table today_action_plan: HTTP 403 Access Denied"
    assert pa.archive(rows, logs.append, session=Session(gets=[Resp(404)], posts=[Resp(500)])) is None
    assert logs[-1].startswith("plan archive not written: RuntimeError: create today_action_plan: HTTP 500")


def test_p2_off_or_malformed_settings_write_nothing(monkeypatch):
    rows = pa.plan_rows(STEPS, [], "s", [], [], NOW, "9-1", "v")
    _env(monkeypatch, on=False)
    s, logs = Session(), []
    assert pa.archive(rows, logs.append, session=s) is None and s.calls == [] and logs == []
    monkeypatch.delenv("TFB_TAP_BQ")
    assert pa.archive(rows, logs.append, session=s) is None and s.calls == []         # off by default
    _env(monkeypatch)
    monkeypatch.setenv("TFB_TAP_BQ_TABLE", "plan; DROP")
    assert pa.archive(rows, logs.append, session=s) is None and s.calls == []
    assert logs == ["plan archive not written: TFB_BQ_PROJECT / TFB_BQ_DATASET / TFB_TAP_BQ_TABLE missing or malformed"]


# ----------------------------------------------------------------------------- P3 wiring
def test_p3_workflow_maps_on_and_the_pin_is_listed():
    wf = (ROOT / ".github/workflows/today_action_plan.yml").read_text(encoding="utf-8")
    assert "TFB_TAP_BQ: ${{ vars.TFB_TAP_BQ || 'on' }}" in wf
    assert "TFB_BQ_DATASET: ${{ vars.TFB_BQ_DATASET || 'tfb_archive' }}" in wf and "google-auth requests" in wf
    assert '("plan_archive", "%s", "plan archive")' % pa.__version__ in (ROOT / "scripts/verify_deployment.py").read_text(
        encoding="utf-8")
