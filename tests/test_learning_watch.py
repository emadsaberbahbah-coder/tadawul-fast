"""tests/test_learning_watch.py - scripts/learning_watch.py 1.0.0 (every learning record, read daily; observe only)."""
from __future__ import annotations

import datetime as dt
import re
import sys
from pathlib import Path

import pytest
import yaml

from scripts import learning_watch as lw
from tests.test_news_board import Book

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 25, 9, 40, tzinfo=UTC)          # Wed 25 Nov 2026, 12:40 Riyadh
SQL_DIR = Path("sql/bigquery")

ROWS = {
    "v_confidence_calibration": [
        {"item": "SA | HIGH", "period": None, "reading": "inverted: HIGH below MEDIUM", "n": "449", "effect": "-2.93",
         "t": None},
        {"item": "US | ALL", "period": None, "reading": None, "n": "1497", "effect": "1.21", "t": None},
        {"item": "SA | MEDIUM", "period": None, "reading": "inverted: HIGH below MEDIUM", "n": "171",
         "effect": "2.19", "t": None}],
    "v_trial_c2": [
        {"item": "forward from 2026-11-20 | 1W | C2_MINUS_CHAMPION", "period": "forward from 2026-11-20",
         "reading": "collecting: 3 of 20 days, 12 of 40 calls", "n": "12", "effect": "0.4", "t": "0.6"},
        {"item": "in-sample | 1W | C2_MINUS_CHAMPION", "period": "in-sample",
         "reading": "C-2 ahead: the owner may consider it", "n": "300", "effect": "1.1", "t": "3.0"}],
    "v_news_trend_learning": [
        {"item": "forward from 2026-11-24 | good news, price agreed | all | 5", "period": "forward from 2026-11-24",
         "reading": "works as expected", "n": "30", "effect": "0.9", "t": "2.4"},
        {"item": "history, rules fixed before reading it | big rise, no news | all | 5",
         "period": "history, rules fixed before reading it", "reading": "works as expected", "n": "80", "effect": "1.0",
         "t": "2.5"}],
    "v_signal_edge": [{"item": "US | momentum | 1M", "period": None, "reading": "inverse", "n": "25", "effect": "-0.05",
                       "t": "-2.2"}],
}
PREVIOUS = [
    {"view_name": "v_news_trend_learning", "item": "forward from 2026-11-24 | good news, price agreed | all | 5",
     "grp": "COLLECTING", "reading": "collecting", "snapshot_date": "2026-11-24"},
    {"view_name": "v_signal_edge", "item": "US | momentum | 1M", "grp": "AGAINST", "reading": "inverse",
     "snapshot_date": "2026-11-24"},
    {"view_name": "v_confidence_calibration", "item": "US | ALL", "grp": "NEUTRAL", "reading": None,
     "snapshot_date": "2026-11-24"}]


class Query:
    def __init__(self, missing=("v_driver_learning",), previous=PREVIOUS, table=True):
        self.sql, self.missing, self.previous, self.table = [], set(missing), previous, table

    def __call__(self, session, project, location, sql):
        self.sql.append(sql)
        if ".learning_watch_log`" in sql:
            if not self.table:
                raise RuntimeError("BigQuery refused the query (HTTP 404): Not found: Table x")
            return [dict(r) for r in self.previous]
        view = re.search(r"\.(v_[a-z0-9_]+)`", sql).group(1)
        if view in self.missing:
            raise RuntimeError("BigQuery refused the query (HTTP 404): Not found: Table %s" % view)
        return [dict(r) for r in ROWS.get(view, [])]


class BQ:
    def __init__(self, fail=False):
        self.loads, self.fail = [], fail

    def ensure(self, session, project, dataset, table, spec=None):
        assert spec is lw.SPEC

    def load(self, session, project, dataset, location, table, day, rows, run_id, schema=None,
             disposition="WRITE_TRUNCATE"):
        if self.fail:
            raise RuntimeError("refused")
        names = {f["name"] for f in schema}
        for r in rows:
            assert set(r) <= names
            assert all(r.get(f["name"]) is not None for f in schema if f["mode"] == "REQUIRED")
        self.loads.append({"table": table, "day": day, "rows": rows, "disposition": disposition})
        return len(rows)


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.delenv("TFB_LEARNING_WATCH", raising=False)
    monkeypatch.setenv("TFB_BQ_PROJECT", "proj-1")
    monkeypatch.setenv("GITHUB_RUN_ID", "88")
    monkeypatch.setattr(lw, "_SLEEP", lambda s: None)


def run(query=None, bq=None, dry=False):
    query, bq, book = query or Query(), bq or BQ(), Book({})
    rc = lw.run_once(dry, book=book, session=object(), now=NOW, query=query, ensure=bq.ensure, load=bq.load)
    return rc, book, query, bq


# ----------------------------------------------------------------------------- the registry and the words
def test_every_registry_column_exists_in_its_view_file():
    """Each view, key, reading, n, effect and t column of the registry is a column its SQL file outputs."""
    for view, _label, keys, reading, n, effect, t, where in lw.REGISTRY:
        f = list(SQL_DIR.glob("*_%s.sql" % view))
        assert len(f) == 1, view
        src = f[0].read_text(encoding="utf-8")
        for col in list(keys) + [reading, n, effect] + ([t] if t else []) + re.findall(r"^(\w+) =", where):
            assert re.search(r"\b%s\b" % col, src), (view, col)
    assert len(lw.REGISTRY) == 18 and len({e[0] for e in lw.REGISTRY}) == 18
    # Codex P1 on #1074: a record that defines every row as in-sample is never read as evidence
    declared = {view for view, *_ in lw.REGISTRY
                if "every row is in-sample" in list(SQL_DIR.glob("*_%s.sql" % view))[0].read_text(encoding="utf-8").lower()}
    assert declared == lw.IN_SAMPLE_VIEWS == {"v_driver_learning", "v_market_driver_learning"}


def test_in_sample_records_never_prove(monkeypatch):
    rows = {"v_driver_learning": [{"item": "momentum", "period": None, "reading": "higher is better", "n": "40",
                                   "effect": "0.05", "t": "2.4"}],
            "v_market_driver_learning": [{"item": "VIX | low", "period": None, "reading": "weaker than usual",
                                          "n": "200", "effect": "-0.4", "t": None}]}
    monkeypatch.setattr(sys.modules[__name__], "ROWS", rows)
    got, missing = lw.read_all(object(), "p", "US", "p.d", Query(missing=()))
    assert {(r["view_name"], r["grp"]) for r in got} == {("v_driver_learning", "IN-SAMPLE"),
                                                         ("v_market_driver_learning", "IN-SAMPLE")}


@pytest.mark.parametrize("reading,grp", [
    ("works as expected", "PROVEN"), ("works: beats SPY", "PROVEN"), ("holds", "PROVEN"), ("edge", "PROVEN"),
    ("BETTER THAN THE BASELINE", "PROVEN"), ("C-1 ahead: the owner may consider it", "PROVEN"), ("proven", "PROVEN"),
    ("holds: the expected way, t 2 or more", "PROVEN"), ("beats its market", "PROVEN"), ("higher is better", "PROVEN"),
    ("opposite of expected", "AGAINST"), ("inverted: HIGH below MEDIUM", "AGAINST"), ("WORSE THAN THE BASELINE", "AGAINST"),
    ("fails: trails SPY", "AGAINST"), ("inverse", "AGAINST"), ("C-1 not ahead: keep the champion", "AGAINST"),
    ("not the expected way", "AGAINST"), ("loses to its market", "AGAINST"),
    ("collecting", "COLLECTING"), ("collecting: 3 of 20 days", "COLLECTING"), ("THIN", "COLLECTING"),
    ("too few weeks", "COLLECTING"), ("TOO FEW TO JUDGE (3 of 20 days, 5 of 40 calls)", "COLLECTING"),
    ("no edge yet", "COLLECTING"), ("C-1 ahead but not yet clear (t under 2): keep collecting", "COLLECTING"),
    ("no clear edge", "NEUTRAL"), ("NO EDGE", "NEUTRAL"), ("not proven", "NEUTRAL"), ("in order", "NEUTRAL"),
    ("the expected way, not clear (t under 2)", "NEUTRAL"), ("BIASED (class > 90 %)", "NEUTRAL"), (None, "NEUTRAL"),
    ("NOT MEASURABLE (one class)", "NEUTRAL"), ("no expectation", "NEUTRAL"), ("unstable", "NEUTRAL"),
    ("in-sample, not evidence", "IN-SAMPLE"), ("CANDIDATE (forward test)", "IN-SAMPLE"),
    ("in-sample: shows why the rule was chosen, not evidence", "IN-SAMPLE")])
def test_the_fixed_words(reading, grp):
    assert lw.classify(reading) == grp


def test_history_rows_are_never_evidence():
    assert lw.classify("works as expected", "history, rules fixed before reading it") == "IN-SAMPLE"
    assert lw.classify("holds", "backtest") == "IN-SAMPLE"
    assert lw.classify("works as expected", "forward from 2026-11-24") == "PROVEN"


def test_the_view_query():
    sql = lw.sql_view("p.d", lw.REGISTRY[0])
    assert sql == ("SELECT CONCAT(IFNULL(CAST(market AS STRING), '-'), ' | ', IFNULL(CAST(confidence AS STRING), '-')) "
                   "AS item, CAST(NULL AS STRING) AS period, CAST(reading AS STRING) AS reading, SAFE_CAST(records AS "
                   "FLOAT64) AS n, SAFE_CAST(mean_excess_pp AS FLOAT64) AS effect, CAST(NULL AS FLOAT64) AS t FROM "
                   "`p.d.v_confidence_calibration` WHERE period = 'all'")
    assert re.match(r"^\s*SELECT\b", sql) and "CURRENT_" not in sql


# ----------------------------------------------------------------------------- a full run
def test_a_full_run(capsys):
    rc, book, query, bq = run()
    assert rc == 0
    out = capsys.readouterr().out
    # v1.0.1: SA | MEDIUM carries the pair reading but is the better row - NEUTRAL, not AGAINST
    assert ("LEARNING WATCH: 8 row(s) in 4 record(s); PROVEN 1, AGAINST 2, collecting 1, neutral 2, in-sample 2; "
            "6 new or changed since 2026-11-24; 1 record(s) not readable") in out
    log = bq.loads[0]
    assert log["table"] == "learning_watch_log" and log["day"] == "2026-11-25" and log["disposition"] == "WRITE_TRUNCATE"
    by = {(r["view_name"], r["item"]): r for r in log["rows"]}
    assert by[("v_trial_c2", "in-sample | 1W | C2_MINUS_CHAMPION")]["grp"] == "IN-SAMPLE"
    assert by[("v_news_trend_learning", "forward from 2026-11-24 | good news, price agreed | all | 5")]["grp"] == "PROVEN"
    page = book.tabs["Learning_Watch"].values
    firsts = [str(r[0]) for r in page]
    assert "OBSERVE ONLY" in page[1][0] and page[2][0].startswith("LEARNING WATCH: 8 row(s)")
    assert "Not readable this run: v_driver_learning" in firsts
    i = firsts.index("1. CHANGED SINCE 2026-11-24")
    changed = page[i + 2:i + 4]
    # the promoted state first (PROVEN before the rest), then a row new since the last day
    assert changed[0][:3] == ["News and trend states (nightly)", "forward from 2026-11-24 | good news, price agreed | all | 5",
                              "PROVEN"] and changed[0][7] == "COLLECTING"
    assert changed[1][2] == "AGAINST" and changed[1][7] == "new" and changed[1][1] == "SA | HIGH"
    j = firsts.index("2. PROVEN - forward rows whose fixed reading says the signal works")
    assert page[j + 2][1] == "forward from 2026-11-24 | good news, price agreed | all | 5"
    assert page[j + 3] == [""] * lw.WIDTH                                        # one PROVEN row: the history row is not one
    a = firsts.index("3. AGAINST - forward rows whose fixed reading says the signal works the other way (or worse than "
                     "its baseline)")
    assert [r[1] for r in page[a + 2:a + 4]] == ["SA | HIGH", "US | momentum | 1M"]
    k = firsts.index("4. EACH RECORD")
    tally = {r[1]: r[2:8] for r in page[k + 2:k + 2 + len(lw.REGISTRY)]}
    assert tally["v_driver_learning"][0] == "not readable" and tally["v_trial_c2"] == [2, 0, 0, 1, 0, 1]
    assert all(len(r) == lw.WIDTH for r in page)


def test_first_day_and_failures(capsys):
    rc, book, query, bq = run(query=Query(table=False))
    assert rc == 0 and "first day: nothing to compare" in capsys.readouterr().out
    assert any("the first day" in str(c) for r in book.tabs["Learning_Watch"].values for c in r)
    rc, book, query, bq = run(bq=BQ(fail=True))
    assert rc == 1 and "Learning_Watch" in book.tabs                             # the page still goes out
    rc, book, query, bq = run(query=Query(missing={e[0] for e in lw.REGISTRY}))
    assert rc == 2 and bq.loads == [] and "Learning_Watch" not in book.tabs      # nothing readable: nothing written


def test_dry_run_and_switch(monkeypatch):
    rc, book, query, bq = run(dry=True)
    assert rc == 0 and bq.loads == [] and "Learning_Watch" not in book.tabs
    monkeypatch.setenv("TFB_LEARNING_WATCH", "off")
    rc, book, query, bq = run()
    assert rc == 0 and query.sql == []


def test_source_and_workflow():
    src = Path(lw.__file__).read_text(encoding="utf-8")
    assert src.isascii() and lw.__version__ == "1.0.1" and "v1.0.1 (2026-11-24)" in src
    wf = yaml.safe_load(Path(".github/workflows/learning_watch.yml").read_text(encoding="utf-8"))
    on = wf[True] if True in wf else wf["on"]
    assert on["workflow_run"]["workflows"] == ["🎯 Exit Plan"] and "schedule" not in on
    assert wf["jobs"]["due"]["steps"][0]["env"]["CHAIN_HOURS"] == "${{ vars.TFB_LEARNING_WATCH_CHAIN_HOURS || '9' }}"
    job = wf["jobs"]["watch"]
    assert job["if"] == "needs.due.outputs.run == 'true'" and job["concurrency"]["group"] == "tfb-learning-watch"
    env = [s for s in job["steps"] if s.get("name", "").startswith("🎓")][0]["env"]
    assert env["TFB_LEARNING_WATCH"] == "${{ vars.TFB_LEARNING_WATCH || 'on' }}" and "EODHD_API_KEY" not in env
    assert wf["permissions"] == {"contents": "read"}


def test_pair_reading_and_counts():
    """v1.0.1: the calibration's pair reading is AGAINST only on the HIGH row; a count has no sign."""
    assert lw.group_of("v_confidence_calibration", "SA | HIGH", "inverted: HIGH below MEDIUM") == "AGAINST"
    assert lw.group_of("v_confidence_calibration", "SA | MEDIUM", "inverted: HIGH below MEDIUM") == "NEUTRAL"
    assert lw.group_of("v_signal_edge", "US | m | 1M", "inverse") == "AGAINST"            # other records unchanged
    assert lw._count(230.0) == "230" and lw._count(None) == ""
