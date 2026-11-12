"""scripts/evidence_board.py v1.0.0 - the Evidence Board (consolidated plan of 12 Nov 2026): market state, a shadow
score, a historical chance of finishing up, a price range, factor edge and label stability, re-measured on every run.
Pure functions, the SQL builders, the BigQuery REST reader and one full run on fakes (no network)."""
from __future__ import annotations

import datetime as dt
import math
import re
from pathlib import Path

import pytest
import yaml

from scripts import evidence_board as eb

ROOT = Path(__file__).resolve().parents[1]
UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 12, 4, 40, tzinfo=UTC)
DS = "proj-1.tfb_archive"

HIST = [  # kind, m, state, bucket, weeks, n, up, ret
    ("US", "MIXED", "STRONG", 16, 583, 0.506, 0.0094), ("US", "MIXED", "MIDDLE", 16, 8897, 0.562, 0.0128),
    ("US", "MIXED", "WEAK", 16, 2380, 0.521, 0.0041), ("SR", "WASHED_OUT", "STRONG", 25, 1092, 0.478, 0.0037),
    ("SR", "WASHED_OUT", "MIDDLE", 25, 3939, 0.4125, -0.0091), ("SR", "WASHED_OUT", "WEAK", 25, 1271, 0.4005, -0.0133),
    ("SR", "BROAD", "STRONG", 4, 177, 0.299, -0.0334), ("US", "BROAD", "STRONG", 2, 20, 0.9, 0.05)]
STATE_ROWS = [{"kind": "HIST", "m": m, "state": s, "bucket": b, "weeks": str(w), "n": str(n), "up": str(u), "ret": str(r)}
              for m, s, b, w, n, u, r in HIST] + [
    {"kind": "NOW", "m": "SR", "n": "260", "up": "0.1423", "ret": "0.1269", "date": "2026-11-09"},
    {"kind": "NOW", "m": "US", "n": "790", "up": "0.4038", "ret": "0.481", "date": "2026-11-10"}]
NAMES = [
    {"symbol": "NMM.US", "name": "Navios", "inv": "1", "sc": "74.8", "rk": "29.2", "px": "87.09", "tp": "110.25", "close": "87.09",
     "vol60": "0.0193", "pmom": "0.97", "phi": "0.55", "p200": "0.92", "pup": "0.77", "has_pe": "1", "has_g": "1", "has_fcf": "1", "has_tp": "1"},
    {"symbol": "LADR.US", "name": "Ladder", "inv": None, "sc": None, "rk": None, "px": None, "tp": None, "close": "8.84",
     "vol60": "0.015", "pmom": "0.1", "phi": "0.27", "p200": "0.17", "pup": "0.8", "has_pe": "1", "has_g": "0", "has_fcf": "1", "has_tp": "1"},
    {"symbol": "TEN.US", "name": "Tsakos", "inv": None, "sc": None, "rk": None, "px": None, "tp": None, "close": None,
     "vol60": None, "pmom": None, "phi": None, "p200": None, "pup": "0.9", "has_pe": "1", "has_g": "1", "has_fcf": "1", "has_tp": "1"},
    {"symbol": "2222.SR", "name": "Aramco", "inv": "1", "sc": "71", "rk": "20", "px": "26", "tp": None, "close": "26.1",
     "vol60": "0.01", "pmom": "0.9", "phi": "0.8", "p200": "0.7", "pup": None, "has_pe": "1", "has_g": "1", "has_fcf": "1", "has_tp": "0"},
    {"symbol": "1111.SR", "name": "NoBars", "inv": "1", "sc": "70", "rk": "20", "px": "9", "tp": None, "close": None,
     "vol60": None, "pmom": None, "phi": None, "p200": None, "pup": None, "has_pe": "0", "has_g": "0", "has_fcf": "0", "has_tp": "0"}]
FACTORS = [{"m": "SR", "feat": "hi52", "weeks": "49", "ic": "0.135", "t": "2.38", "top_up": "0.405", "top_beat": "0.553", "all_up": "0.353"},
           {"m": "US", "feat": "mom12_1", "weeks": "51", "ic": "0.051", "t": "1.11", "top_up": "0.544", "top_beat": "0.512", "all_up": "0.551"},
           {"m": "US", "feat": "volsurge", "weeks": "51", "ic": "0.012", "t": "0.66", "top_up": "0.554", "top_beat": "0.465", "all_up": "0.551"},
           {"m": "US", "feat": "ret1m", "weeks": "51", "ic": "-0.09", "t": "-2.4", "top_up": "0.5", "top_beat": "0.45", "all_up": "0.551"}]
SR_SHADOW = [{"symbol": "2222.SR", "name": "Aramco", "close": "26.1", "vol60": "0.01", "pmom": "0.9", "phi": "0.8", "p200": "0.7"},
             {"symbol": "4190.SR", "name": "Jarir", "close": "17.33", "vol60": "0.0229", "pmom": "0.93", "phi": "0.98", "p200": "0.96"},
             {"symbol": "9999.SR", "name": "", "close": None, "vol60": None, "pmom": None, "phi": None, "p200": None}]
GATE = [["S-1 LEARNING GATE", "", ""], [], ["Verdict", "FAIL", "day 28/28"], ["Challenger", "-3.58%"], ["Benchmark", "-3.16%"]]
STAB = [{"rule": "Current label", "name_days": "717", "kept": "0.8168", "drops": "122", "per_day": "11.75"},
        {"rule": "Two-level: enter 72, leave below 67", "name_days": "317", "kept": "0.9107", "drops": "26", "per_day": "5.2"}]


def fake_query(session, project, location, sql, **kw):
    session.append(sql)
    if "'HIST' kind" in sql:
        return STATE_ROWS
    if "sg0 AS" in sql:
        return NAMES
    if "LIMIT 10" in sql and "symbol LIKE '%.SR' QUALIFY" in sql:
        return SR_SHADOW
    if "UNNEST([STRUCT('mom12_1'" in sql:
        return FACTORS
    if "'Current label' rule" in sql:
        return STAB
    raise AssertionError("unexpected query")


class Sheet:
    def __init__(self, rows=None):
        self.values, self.row_count, self.col_count, self.updates = list(rows or []), 200, 30, 0

    def get(self, rng):
        return [list(r) for r in self.values]

    def resize(self, rows=None, cols=None):
        self.row_count, self.col_count = rows or self.row_count, cols or self.col_count

    def update(self, values=None, range_name=None, value_input_option=None):
        self.updates += 1
        self.values = [list(r) for r in values]

    def batch_clear(self, ranges):
        pass

    def clear(self):
        self.values = []


class WorksheetNotFound(Exception):
    pass


class Book:
    def __init__(self, tabs):
        self.tabs = dict(tabs)

    def worksheet(self, name):
        if name not in self.tabs:
            raise WorksheetNotFound(name)
        return self.tabs[name]

    def add_worksheet(self, title, rows, cols):
        self.tabs[title] = Sheet()
        return self.tabs[title]


LEDGER = [["Plan Date", "Kind", "Symbol", "Status"], ["2026-11-10", "PICK", "OLD.US", "OPEN"],
          ["2026-11-11", "PICK", "TEN.US", "WAIT"], ["2026-11-11", "PICK", "NMM.US", "WAIT"],
          ["2026-11-11", "PICK", "ladr.us", "OPEN"], ["2026-11-11", "PIPELINE", "ZTO.US", ""]]


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for name in list(__import__("os").environ):
        if name.startswith("TFB_EVIDENCE") or name.startswith("TFB_BQ_"):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TFB_BQ_PROJECT", "proj-1")


# ----------------------------------------------------------------------------- E1 pure
def test_e1_states_buckets_and_markets():
    assert [eb.state_of(x) for x in (0.0, 0.399, 0.4, 0.599, 0.6, 1.0, None)] == [
        "WASHED_OUT", "WASHED_OUT", "MIXED", "MIXED", "BROAD", "BROAD", ""]
    assert eb.bucket_of(0.8, 0.6) == "STRONG" and eb.bucket_of(0.8, 0.59) == "MIDDLE" and eb.bucket_of(0.2, 0.9) == "WEAK"
    assert eb.bucket_of(0.21, 0.1) == "MIDDLE" and eb.bucket_of(None, 0.5) == "" and eb.bucket_of(0.5, None) == ""
    assert [eb.market_of(s) for s in ("2222.SR", "nmm.us", "BBOX.L", "")] == ["SR", "US", "OTHER", "OTHER"]
    assert eb.num("12.5%") == 12.5 and eb.num("1,200") == 1200 and eb.num("x") is None and eb.num(float("nan")) is None
    assert eb.num(True) is None and eb.num(None) is None


def test_e1_history_cells_and_the_chance_of_finishing_up():
    cells = eb.history_cells(STATE_ROWS)
    us = cells[("US", "MIXED", "ALL")]
    assert us["n"] == 583 + 8897 + 2380 and us["weeks"] == 16
    assert us["up"] == pytest.approx((583 * 0.506 + 8897 * 0.562 + 2380 * 0.521) / (583 + 8897 + 2380))
    got, basis = eb.chance_up(cells, "US", "MIXED", "STRONG", 4)
    assert got == 0.506 and basis == "US, mixed market, strong trend: 583 name-weeks over 16 weeks"
    got, basis = eb.chance_up(cells, "US", "MIXED", "", 4)                       # no bucket: all names of the state
    assert got == pytest.approx(us["up"]) and "all names" in basis
    assert eb.chance_up(cells, "US", "BROAD", "STRONG", 4) == (None, "not enough history for this market state")
    assert eb.chance_up(cells, "SR", "BROAD", "STRONG", 5)[0] is None            # 4 weeks is under the minimum of 5
    assert eb.chance_up(cells, "SR", "BROAD", "STRONG", 4)[0] == 0.299
    assert eb.chance_up(cells, "OTHER", "MIXED", "STRONG", 4)[0] is None
    assert eb.history_cells([{"m": "US", "state": "MIXED", "bucket": "STRONG", "n": "0", "up": "0.5"}]) == {}


def test_e1_price_range_and_shadow_score():
    lo, hi = eb.price_range(100.0, 0.02)
    w = 1.2816 * 0.02 * math.sqrt(21)
    assert lo == pytest.approx(100 * math.exp(-w)) and hi == pytest.approx(100 * math.exp(w)) and lo < 100 < hi
    for bad in ((None, 0.02), (0, 0.02), (100, None), (100, 0), (100, 0.3)):
        assert eb.price_range(*bad) == (None, None)
    nmm = eb.shadow_score(NAMES[0])
    trend = (0.97 + 0.55 + 0.92) / 3
    assert nmm[0] == pytest.approx(round(100 * (trend + 0.77 + 1.0) / 3, 1)) and "low risk 100" in nmm[1] and "missing" not in nmm[1]
    ladr = eb.shadow_score(NAMES[1])                                             # no risk score; revenue growth missing
    assert ladr[0] == pytest.approx(round(100 * ((0.1 + 0.27 + 0.17) / 3 + 0.8) / 2 - 5, 1)) and "-5 for missing revenue growth" in ladr[1]
    ten = eb.shadow_score(NAMES[2])                                              # no price history: analyst upside only
    assert ten[0] == 90.0 and ten[1] == "analyst upside 90"
    sr = eb.shadow_score(NAMES[3])                                               # Tadawul: trend only; no analyst target
    assert sr[0] == pytest.approx(round(100 * (0.9 + 0.8 + 0.7) / 3 - 5, 1)) and "analyst" not in sr[1].split(";")[0]
    assert eb.shadow_score(NAMES[4]) == (None, "no price history: a Tadawul name is scored on trend only")
    assert eb.shadow_score({"symbol": "X.US"})[0] is None
    mid = dict(NAMES[0], rk="40")
    assert "low risk 50" in eb.shadow_score(mid)[1]
    assert "low risk 0" in eb.shadow_score(dict(NAMES[0], rk="45"))[1]
    assert eb.shadow_score(dict(NAMES[4], symbol="Z.US", rk="99", pup=None))[0] == 0.0     # never below zero
    assert eb.missing_fields(NAMES[4]) == list(eb.CORE_FIELDS)
    assert [eb.edge_reading(t, ic) for t, ic in ((2.0, 0.1), (-2.4, -0.09), (1.5, 0.05), (0.5, 0.01), (None, 0.1))] == [
        "edge", "works in reverse", "weak", "no edge", ""]


# ----------------------------------------------------------------------------- E2 SQL and REST
def test_e2_sql_is_select_only_and_symbols_are_cleaned():
    for sql in (eb.sql_state(DS), eb.sql_factors(DS), eb.sql_names(DS, ["NMM.US"]), eb.sql_stability(DS, 72, 67)):
        assert sql.startswith("WITH ") and "`proj-1.tfb_archive.v_" in sql
        assert not re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE)\b", sql, re.I)
        assert "{DS}" not in sql and "{LIST}" not in sql and "{EXTRA}" not in sql and "%s" not in sql
    assert eb.clean_symbols(["nmm.us", "NMM.US", "x'; DROP TABLE t;--", "", None, "BRK-B.US", "a b"]) == ["NMM.US", "BRK-B.US"]
    assert len(eb.clean_symbols(["S%d.US" % i for i in range(100)])) == 60
    names = eb.sql_names(DS, ["nmm.us", "x' OR 1=1 --"])
    assert "ARRAY<STRING>['NMM.US']" in names and "OR 1=1" not in names
    assert "ARRAY<STRING>[]" in eb.sql_names(DS, [])
    st = eb.sql_stability(DS, 72, 67.5)
    assert "sc<67.5 THEN 0 WHEN inv=1 AND sc>=72 THEN 1" in st and "'Two-level: enter 72, leave below 67.5'" in st
    assert "LIKE '%.SR'" in st and "LIKE '%.SR'" in eb.sql_state(DS)


class Resp:
    def __init__(self, code, payload):
        self.status_code, self._p, self.text = code, payload, str(payload)

    def json(self):
        return self._p


class Session:
    def __init__(self, posts, gets=()):
        self.posts, self.gets, self.calls = list(posts), list(gets), []

    def post(self, url, json=None, timeout=None):
        self.calls.append(("POST", url, json))
        return self.posts.pop(0)

    def get(self, url, timeout=None):
        self.calls.append(("GET", url, None))
        return self.gets.pop(0)


def test_e2_bq_query_reads_rows_polls_and_refuses_writes():
    done = {"jobComplete": True, "schema": {"fields": [{"name": "a"}, {"name": "b"}]},
            "rows": [{"f": [{"v": "1"}, {"v": None}]}, {"f": [{"v": "x"}]}]}
    s = Session([Resp(200, done)])
    assert eb.bq_query(s, "proj-1", "US", "SELECT 1") == [{"a": "1", "b": None}, {"a": "x", "b": None}]
    kind, url, body = s.calls[0]
    assert url == "https://bigquery.googleapis.com/bigquery/v2/projects/proj-1/queries"
    assert body["useLegacySql"] is False and body["location"] == "US" and body["query"] == "SELECT 1"
    pending = {"jobComplete": False, "jobReference": {"jobId": "j1", "location": "US"}}
    s = Session([Resp(200, pending)], [Resp(200, pending), Resp(200, done)])
    assert len(eb.bq_query(s, "proj-1", "US", "WITH x AS (SELECT 1) SELECT * FROM x", sleep=lambda _: None)) == 2
    assert "/queries/j1?location=US" in s.calls[1][1] and len(s.calls) == 3
    for bad in ("DELETE FROM t", "SELECT 1; DROP TABLE t", "CREATE TABLE t AS SELECT 1", "  update t set a=1"):
        with pytest.raises(RuntimeError, match="only a SELECT"):
            eb.bq_query(Session([]), "p", "US", bad)
    with pytest.raises(RuntimeError, match="HTTP 403"):
        eb.bq_query(Session([Resp(403, {"error": "denied"})]), "p", "US", "SELECT 1")
    with pytest.raises(RuntimeError, match="result read"):
        eb.bq_query(Session([Resp(200, pending)], [Resp(500, {})]), "p", "US", "SELECT 1", sleep=lambda _: None)
    with pytest.raises(RuntimeError, match="did not finish"):
        eb.bq_query(Session([Resp(200, pending)]), "p", "US", "SELECT 1", wait_s=-1, sleep=lambda _: None)
    assert eb.parse_result({}) == []


# ----------------------------------------------------------------------------- E3 the run
def test_e3_plan_picks_from_the_ledger():
    today = dt.date(2026, 11, 12)
    assert eb.plan_picks(LEDGER, today) == ["TEN.US", "NMM.US", "LADR.US"]
    assert eb.plan_picks(LEDGER, dt.date(2026, 11, 19)) == []                    # the last plan is over 7 days old
    assert eb.plan_picks([], today) == [] and eb.plan_picks([["x"]], today) == []
    assert eb.plan_picks([["Plan Date", "Kind", "Symbol"], ["bad", "PICK", "A.US"]], today) == []


def test_e3_full_run_writes_one_page_and_only_that_page():
    old_log = [list(eb.LOG_HEADERS), ["2026-11-11", "OLD.US", "US", "PICK"], ["2026-11-12", "STALE.US", "US", "PICK"]]
    book, sqls = Book({"_Daily_Plan": Sheet(LEDGER), "S1_Gate": Sheet(GATE), "_Evidence_Log": Sheet(old_log)}), []
    assert eb.run_once(False, book=book, session=sqls, now=NOW, query=fake_query) == 0
    assert len(sqls) == 5 and "ARRAY<STRING>['TEN.US','NMM.US','LADR.US']" in sqls[1]
    assert set(book.tabs) == {"_Daily_Plan", "S1_Gate", "Evidence_Board", "_Evidence_Log"}
    assert book.tabs["_Daily_Plan"].updates == 0 and book.tabs["S1_Gate"].updates == 0
    log = book.tabs["_Evidence_Log"].values
    assert log[0] == eb.LOG_HEADERS and log[1][:2] == ["2026-11-11", "OLD.US"]          # earlier days kept
    assert not any(r[1] == "STALE.US" for r in log)                                     # a re-run replaces the day
    today = [r for r in log if r[0] == "2026-11-12"]
    assert [r[1] for r in today] == ["TEN.US", "NMM.US", "LADR.US", "2222.SR", "1111.SR", "4190.SR", "9999.SR"]
    kinds = {r[1]: r[3] for r in today}
    assert kinds == {"TEN.US": "PICK", "NMM.US": "PICK", "LADR.US": "PICK", "2222.SR": "INVESTABLE", "1111.SR": "INVESTABLE",
                     "4190.SR": "SR_SHADOW", "9999.SR": "SR_SHADOW"}                    # a name is logged once a day
    nmm_log = [r for r in today if r[1] == "NMM.US"][0]
    assert nmm_log[4] == "INVESTABLE" and nmm_log[5] == 74.8 and nmm_log[7] == 0.562 and nmm_log[9] == 87.09
    assert nmm_log[12] == "MIXED" and nmm_log[14] == eb.__version__ and all(len(r) == len(eb.LOG_HEADERS) for r in log)
    jarir = [r for r in today if r[1] == "4190.SR"][0]
    assert jarir[6] == pytest.approx(round((0.93 + 0.98 + 0.96) / 3 * 100, 1)) and jarir[7] == 0.478 and jarir[12] == "WASHED_OUT"
    page = book.tabs["Evidence_Board"].values
    flat = ["|".join(str(c) for c in r) for r in page]
    assert all(len(r) == eb.WIDTH for r in page)
    assert flat[0].startswith("EVIDENCE BOARD") and "Built 2026-11-12 07:40 Riyadh" in flat[1] and "SHADOW" in flat[1]
    us = [r for r in page if r[0] == "US" and r[4] in ("MIXED", "BROAD", "WASHED OUT")][0]
    assert us[1] == "2026-11-10" and us[2] == "40.4%" and us[4] == "MIXED" and us[7] == "50.6%" and us[8] == "16" and us[9] == ""
    sr = [r for r in page if r[0] == "Tadawul" and r[4] == "WASHED OUT"][0]
    assert sr[2] == "14.2%" and sr[7] == "47.8%" and sr[8] == "25"
    names = [r for r in page if str(r[0]).endswith(".US") or str(r[0]).endswith(".SR") or "(pick)" in str(r[0])]
    assert [r[0] for r in names[:3]] == ["TEN.US (pick)", "NMM.US (pick)", "LADR.US (pick)"]      # picks first, by shadow
    nmm = [r for r in names if r[0].startswith("NMM")][0]
    assert nmm[2] == "INVESTABLE" and nmm[3] == "74.8" and nmm[5] == "56.2%" and "middle trend" in nmm[6]
    assert nmm[9] == "26.6%" and float(nmm[7]) < 87.09 < float(nmm[8])
    ladr = [r for r in names if r[0].startswith("LADR")][0]
    assert ladr[2] == "not investable" and ladr[5] == "52.1%" and "weak trend" in ladr[6]
    ten = [r for r in names if r[0].startswith("TEN")][0]
    assert ten[7] == "" and ten[8] == "" and "all names" in ten[6]                # no bars: no range, state-level chance
    aramco = [r for r in names if r[0] == "2222.SR"][0]
    assert aramco[5] == "47.8%" and "Tadawul, washed out market, strong trend" in aramco[6]
    assert any(r[0] == "Tadawul" and r[1] == "Close to 52-week high" and r[8] == "edge" for r in page)
    assert any(r[1] == "Volume surge" and r[8] == "no edge" for r in page)
    assert any(r[1] == "Last-month return" and r[8] == "works in reverse" for r in page)
    assert any(r[0] == "Current label" and r[2] == "81.7%" and r[3] == "122" for r in page)
    assert any(r[0].startswith("Two-level") and r[2] == "91.1%" for r in page)
    i5 = [i for i, r in enumerate(page) if str(r[0]).startswith("5. TADAWUL SHADOW LIST")][0]
    assert page[i5 + 2][:4] == ["2222.SR", "Aramco", "80%", "47.8%"] and page[i5 + 3][0] == "4190.SR"
    assert page[i5 + 4][0] == "9999.SR" and page[i5 + 4][2] == "" and "all names" in page[i5 + 4][4]
    i6 = [i for i, r in enumerate(page) if str(r[0]).startswith("6. LEARNING GATE")][0]
    assert page[i6 + 1][0] == "S-1 LEARNING GATE" and page[i6 + 2][:3] == ["Verdict", "FAIL", "day 28/28"]
    assert page[i6 + 4][:2] == ["Benchmark", "-3.16%"]


def test_e3_gate_copy_and_ledger_merge():
    assert eb.gate_rows(GATE) == [["S-1 LEARNING GATE", "", ""], ["Verdict", "FAIL", "day 28/28"], ["Challenger", "-3.58%"],
                                  ["Benchmark", "-3.16%"]]
    assert eb.gate_rows([]) == [] and eb.gate_rows([["", ""], [None]]) == []
    assert len(eb.gate_rows([["x"]] * 30)) == 8 and len(eb.gate_rows([["a"] * 20])[0]) == 8
    assert eb.gate_rows([["y" * 500]])[0][0] == "y" * 160
    new = [["2026-11-12", "A.US"], ["2026-11-12", "B.US"]]
    assert eb.merge_log([], new, "2026-11-12") == [eb.LOG_HEADERS] + new
    assert eb.merge_log([["junk", "header"], ["2026-11-01", "X.US"]], new, "2026-11-12") == [eb.LOG_HEADERS] + new
    kept = eb.merge_log([eb.LOG_HEADERS, ["2026-11-10", "X.US"], [], ["", "blank"], ["2026-11-12", "OLD.US"]], new, "2026-11-12")
    assert [r[1] for r in kept[1:]] == ["X.US", "A.US", "B.US"] and len(kept[1]) == len(eb.LOG_HEADERS)
    many = [eb.LOG_HEADERS] + [["2026-10-%02d" % (1 + i % 28), "S%d.US" % i] for i in range(eb.LOG_MAX_ROWS + 5)]
    out = eb.merge_log(many, new, "2026-11-12")
    assert len(out) == eb.LOG_MAX_ROWS + 1 and out[-1][1] == "B.US" and out[1][1] == "S7.US"   # the oldest rows drop
    assert eb.log_rows({"names": [], "sr_shadow": []}, "2026-11-12") == []
    sql = eb.sql_sr_shadow(DS)
    assert sql.startswith("WITH ") and "LIMIT 10" in sql and "{TOP}" not in sql and "LIMIT 50" in eb.sql_sr_shadow(DS, 999)


def test_e3_dry_run_off_switch_and_failures_write_nothing(monkeypatch, capsys):
    book = Book({"_Daily_Plan": Sheet(LEDGER)})
    assert eb.run_once(True, book=book, session=[], now=NOW, query=fake_query) == 0 and "Evidence_Board" not in book.tabs
    assert "_Evidence_Log" not in book.tabs
    assert "dry run" in capsys.readouterr().out

    def boom(*a, **k):
        raise RuntimeError("BigQuery refused the query (HTTP 403)")
    assert eb.run_once(False, book=book, session=[], now=NOW, query=boom) == 2 and "Evidence_Board" not in book.tabs
    assert "nothing written" in capsys.readouterr().out
    monkeypatch.setenv("TFB_EVIDENCE", "off")
    calls = []
    assert eb.run_once(False, book=book, session=calls, now=NOW, query=fake_query) == 0 and calls == []
    monkeypatch.setenv("TFB_EVIDENCE", "on")
    monkeypatch.setenv("TFB_BQ_PROJECT", "bad project;")
    assert eb.run_once(False, book=book, session=calls, now=NOW, query=fake_query) == 2 and calls == []
    monkeypatch.setenv("TFB_BQ_PROJECT", "proj-1")
    monkeypatch.setenv("TFB_EVIDENCE_MIN_WEEKS", "20")                            # the US state (16 weeks) is now thin
    book2, sqls = Book({}), []
    assert eb.run_once(False, book=book2, session=sqls, now=NOW, query=fake_query) == 0
    page = book2.tabs["Evidence_Board"].values
    us = [r for r in page if r[0] == "US" and r[4] == "MIXED"][0]
    assert us[9] == "seen in too few weeks to read" and "ARRAY<STRING>[]" in sqls[1]
    nmm = [r for r in page if str(r[0]).startswith("NMM")][0]
    assert nmm[5] == "" and nmm[6] == "not enough history for this market state"


# ----------------------------------------------------------------------------- E4 wiring
def test_e4_workflow_manifest_and_read_only_scope():
    raw = (ROOT / ".github/workflows/evidence_board.yml").read_text(encoding="utf-8")
    wf = yaml.safe_load(raw)
    on = wf[True] if True in wf else wf["on"]
    assert on["schedule"] == [{"cron": "40 4,13 * * *"}] and "workflow_dispatch" in on
    assert on["workflow_run"] == {"workflows": ["🗓️ Daily Plan"], "types": ["completed"]}
    assert wf["jobs"]["evidence"]["if"] == "github.event_name != 'workflow_run' || github.event.workflow_run.conclusion == 'success'"
    assert wf["permissions"] == {"contents": "read"}
    step = [s for s in wf["jobs"]["evidence"]["steps"] if "evidence_board.py" in str(s.get("run", ""))][0]
    assert step["env"]["TFB_EVIDENCE"] == "${{ vars.TFB_EVIDENCE || 'on' }}"
    assert step["env"]["TFB_BQ_PROJECT"] == "${{ vars.TFB_BQ_PROJECT || 'project-2e3530e6-2311-4aa3-b36' }}"
    assert "--dry-run" in step["run"]
    assert step["env"]["TFB_EVIDENCE_MIN_GAP_H"] == "${{ github.event_name == 'workflow_run' && (vars.TFB_EVIDENCE_MIN_GAP_H || '6') || '0' }}"
    src = (ROOT / "scripts/evidence_board.py").read_text(encoding="utf-8")
    for name in re.findall(r"(TFB_EVIDENCE[A-Z_]*): \$\{\{", raw):
        assert '"%s"' % name in src, name
    assert '("evidence_board", "%s", "evidence board")' % eb.__version__ in (ROOT / "scripts/verify_deployment.py").read_text(
        encoding="utf-8")
    assert src.count("write_grid(") == 2 and "write_grid(book, TAB," in src and "write_grid(book, LOG_TAB," in src


# ----------------------------------------------------------------------------- E5 (1.2.0)
def test_e5_a_young_board_is_not_rebuilt_on_a_follow_run(monkeypatch, capsys):
    young = [["EVIDENCE BOARD"], ["Built 2026-11-12 05:40 Riyadh · evidence_board 1.1.0"]]     # 2 h before NOW
    assert eb.board_age_h(young, NOW) == pytest.approx(2.0) and eb.board_age_h([["x"]], NOW) is None
    assert eb.board_age_h([["Built 2026-13-99 05:40 Riyadh"]], NOW) is None
    monkeypatch.setenv("TFB_EVIDENCE_MIN_GAP_H", "6")
    book, sqls = Book({"Evidence_Board": Sheet(young), "_Daily_Plan": Sheet(LEDGER)}), []
    assert eb.run_once(False, book=book, session=sqls, now=NOW, query=fake_query) == 0
    assert sqls == [] and book.tabs["Evidence_Board"].updates == 0 and "_Evidence_Log" not in book.tabs
    assert "under the 6 h gap" in capsys.readouterr().out
    old = [["EVIDENCE BOARD"], ["Built 2026-11-12 01:39 Riyadh · evidence_board 1.1.0"]]       # just over 6 h
    book = Book({"Evidence_Board": Sheet(old)})
    assert eb.run_once(False, book=book, session=sqls, now=NOW, query=fake_query) == 0 and len(sqls) == 5
    for spec in ("0", ""):                                                                    # by hand: always rebuilt
        monkeypatch.setenv("TFB_EVIDENCE_MIN_GAP_H", spec)
        book, sqls = Book({"Evidence_Board": Sheet(young)}), []
        assert eb.run_once(False, book=book, session=sqls, now=NOW, query=fake_query) == 0 and len(sqls) == 5
    monkeypatch.setenv("TFB_EVIDENCE_MIN_GAP_H", "6")
    book, sqls = Book({}), []                                                                 # no board yet: built
    assert eb.run_once(False, book=book, session=sqls, now=NOW, query=fake_query) == 0 and len(sqls) == 5
