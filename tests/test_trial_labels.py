"""scripts/trial_labels.py v1.0.0 - the Trial_Labels tab: for each stock of Final_Decisions, each holding and each buy
call of the latest call day, what trial C-2 and the news overlay would do with it, observe-only. Pure functions, the
SQL, and full runs through the real BigQuery reader on a fake session and a fake workbook (no network). Every figure
here is invented."""
from __future__ import annotations

import datetime as dt
import re
from pathlib import Path

import yaml

from scripts import evidence_board as eb
from scripts import news_board as nb
from scripts import trial_labels as tl

ROOT = Path(__file__).resolve().parents[1]
UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 19, 5, 5, tzinfo=UTC)          # Thu 19 Nov 2026, 08:05 Riyadh
TODAY = dt.date(2026, 11, 19)
DS = "proj-1.tfb_archive"


def call(sym, h, keep, eligible, why=None, up=None, rank=None, n=5, keeps=3, mg="US", d="2026-11-19"):
    return {"symbol": sym, "market_group": mg, "horizon": h, "call_day": d, "eligible": "true" if eligible else "false",
            "eligible_calls": str(n), "upside_rank": rank, "keeps": str(keeps), "c2_keep": "true" if keep else "false",
            "why_out": why, "target_upside_pp": up}


ALL = ("1W", "2W", "1M", "3M")
CALLS = ([call("AAA.US", h, True, True, up="33.47", rank="1") for h in ALL]
         + [call("BBB.US", h, False, True, up="10.76", rank="5") for h in ALL]
         + [call("2222.SR", h, False, False, why="HIGH confidence, Tadawul", mg="Other markets", n=2, keeps=1) for h in ALL]
         + [call("CCC.US", "1W", False, True, up="12.5", rank="4"), call("CCC.US", "1M", True, True, up="12.5", rank="2")]
         + [call("OLD.US", "1M", True, True, up="9", rank="1", d="2026-11-18")])


def tilt(arm, sector, group, week=("2026-11-09", "2026-11-17", "2026-11-23")):
    return {"arm": arm, "week_start": week[0], "apply_from": week[1], "apply_to": week[2], "etf": "X" + sector[:2].upper(),
            "sector": sector, "score": "0.1", "pos": "1", "active_list": "iran_war", "tilt_group": group, "realized_excess_pp": None}


TILT = [tilt("events", "Financials", "avoided"), tilt("all", "Financials", "avoided"),
        tilt("events", "Information Technology", "favoured"), tilt("all", "Information Technology", "neutral"),
        tilt("events", "Industrials", "favoured"), tilt("all", "Industrials", "avoided"),
        tilt("events", "Energy", "neutral"), tilt("all", "Energy", "neutral")]
SECTORS = [{"symbol": "AAA.US", "sector": "Financials"}, {"symbol": "BBB.US", "sector": "Information Technology"},
           {"symbol": "CCC.US", "sector": "Industrials"}, {"symbol": "2222.SR", "sector": "Energy"},
           {"symbol": "HHH.US", "sector": "Financials"}]


def sh(arm, symbol, action, base="2026-11-19"):
    return {"arm": arm, "base_date": base, "symbol": symbol, "name": symbol, "sector": "Financials", "overall_score": "70",
            "vol_band": "LOW", "in_champion": "true" if action != "in" else "false", "news_action": action}


SHADOW = [sh("events", "AAA.US", "out"), sh("all", "AAA.US", "out"), sh("events", "HHH.US", "out"), sh("all", "HHH.US", "keep"),
          sh("events", "BBB.US", "in")]
FINALS = [["Symbol", "Decision", "Do today"], ["AAA.US", "BUY", "Buy 8"], ["HHH.US", "HOLD", ""], ["2222.SR", "WATCH", ""],
          ["", ""], ["aaa.us", "SELL", "dup"], ["TOTAL", ""]]
PORTFOLIO = [["MY PORTFOLIO"], ["Symbol", "Name"], ["HHH.US", "Hotel Bank"], ["ZZZ", "Zulu"], ["TOTAL", ""]]


def payload(rows):
    names = list(rows[0]) if rows else []
    return {"jobComplete": True, "schema": {"fields": [{"name": n} for n in names]},
            "rows": [{"f": [{"v": r.get(n)} for n in names]} for r in rows]}


class Resp:
    def __init__(self, body, status=200):
        self._body, self.status_code, self.text = body, status, "refused"

    def json(self):
        return self._body


class Session:
    def __init__(self, calls=None, tilt_rows=None, shadow=None, fail=None):
        self.sql, self.fail = [], fail
        self.calls = CALLS if calls is None else calls
        self.tilt = TILT if tilt_rows is None else tilt_rows
        self.shadow = SHADOW if shadow is None else shadow

    def post(self, url, timeout=None, json=None):
        sql = json["query"]
        self.sql.append(sql)
        assert url.endswith("/projects/proj-1/queries") and json["useLegacySql"] is False
        if self.fail and self.fail in sql:
            return Resp({}, 403)
        if ".v_c2_calls`" in sql:          # the query keeps only the latest call day: the fake does the same
            last = max((r["call_day"] for r in self.calls), default=None)
            return Resp(payload([r for r in self.calls if r["call_day"] == last]))
        if ".v_news_sector_tilt`" in sql:
            return Resp(payload(self.tilt))
        if ".v_news_overlay_list`" in sql:
            return Resp(payload(self.shadow))
        if ".v_symbol_sector`" in sql:
            return Resp(payload(SECTORS))
        raise AssertionError("unexpected query: %s" % sql[:80])


class Sheet:
    def __init__(self, rows=None):
        self.values, self.row_count, self.col_count, self.updates, self.ranges = list(rows or []), 200, 30, 0, []

    def get(self, rng):
        self.ranges.append(rng)
        return [list(r) for r in self.values]

    def resize(self, rows=None, cols=None):
        self.row_count, self.col_count = rows or self.row_count, cols or self.col_count

    def update(self, values=None, range_name=None, value_input_option=None):
        self.updates += 1
        self.values = [list(r) for r in values]


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


def _env(monkeypatch):
    for name in list(__import__("os").environ):
        if name.startswith("TFB_TRIAL_LABELS") or name.startswith("TFB_BQ_"):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TFB_BQ_PROJECT", "proj-1")


def world():
    return Book({"Final_Decisions": Sheet(FINALS), "My_Portfolio": Sheet(PORTFOLIO)})


def row_of(page, sym):
    return [r for r in page if r and r[0] == sym]


# ----------------------------------------------------------------------------- L1 the stocks
def test_l1_stocks_come_from_the_decisions_the_holdings_and_the_calls():
    finals = tl.read_finals(FINALS)
    assert finals == [("AAA.US", "BUY"), ("HHH.US", "HOLD"), ("2222.SR", "WATCH")]      # order kept, duplicate and totals out
    assert tl.read_finals([["Decision", "Symbol"], ["AAA.US", "BUY"]]) == [] and tl.read_finals([]) == []
    holdings = nb.read_holdings(PORTFOLIO)
    day, calls = tl.latest_calls(CALLS)
    assert day == TODAY and sorted(calls) == ["2222.SR", "AAA.US", "BBB.US", "CCC.US"]   # the older day's call is left out
    stocks = tl.stock_list(finals, holdings, calls)
    assert stocks == [("AAA.US", "BUY"), ("HHH.US", "HOLD"), ("2222.SR", "WATCH"), ("ZZZ", ""), ("BBB.US", ""), ("CCC.US", "")]
    # a plain ticker and its .US form are one stock
    assert tl.stock_list([("AAA", "BUY")], [], {"AAA.US": []}) == [("AAA", "BUY")]
    assert tl.latest_calls([]) == (None, {})


# ----------------------------------------------------------------------------- L2 C-2
def test_l2_c2_label_says_what_the_trial_does_and_why():
    _, calls = tl.latest_calls(CALLS)
    assert tl.c2_label(calls["AAA.US"], TODAY) == \
        "KEEPS (all horizons) - analyst upside +33.5%, 1st of 5 eligible US calls, top 3 kept"
    assert tl.c2_label(calls["BBB.US"], TODAY) == \
        "DROPS (all horizons) - analyst upside +10.8%, 5th of 5 eligible US calls, top 3 kept"
    assert tl.c2_label(calls["2222.SR"], TODAY) == "DROPS (all horizons) - HIGH confidence, Tadawul"
    assert tl.c2_label(calls["CCC.US"], TODAY) == (
        "PARTLY - drops 1W: analyst upside +12.5%, 4th of 5 eligible US calls, top 3 kept; "
        "keeps 1M: analyst upside +12.5%, 2nd of 5 eligible US calls, top 3 kept")
    assert tl.c2_label(None, TODAY) == "NO BUY CALL - none on 19 Nov; C-2 judges buy calls only"
    assert tl.c2_label([], TODAY) == tl.c2_label(None, TODAY)
    assert tl.c2_label(None, None) == "NO CALL DAY - no buy call in the archive in the last 7 days"
    two = [call("X", "2W", True, True, up="5", rank="1", n=1, keeps=1, mg="Other markets"),
           call("X", "1W", True, True, up="5", rank="1", n=1, keeps=1, mg="Other markets")]
    assert tl.c2_label(two, TODAY) == "KEEPS (1W 2W) - analyst upside +5.0%, 1st of 1 eligible other-market calls, top 1 kept"
    assert tl.c2_label([call("X", "1W", False, False, why=None)], TODAY) == "DROPS (1W) - not eligible"
    assert tl.c2_label([call("X", "1W", False, True, up=None, rank=None)], TODAY) == "DROPS (1W) - upside not read"
    assert [tl._ordinal(n) for n in (1, 2, 3, 4, 11, 12, 13, 21, 22, 23, 101, 111)] == [
        "1st", "2nd", "3rd", "4th", "11th", "12th", "13th", "21st", "22nd", "23rd", "101st", "111th"]


# ----------------------------------------------------------------------------- L3 news
def test_l3_news_label_reads_the_tilt_in_force_and_the_news_list():
    t = nb.pick_tilt(TILT, TODAY)
    d, names = nb.shadow_names(SHADOW)
    assert tl.tilt_week(t) == "2026-11-09" and tl.tilt_until(t) == "2026-11-23"
    assert tl.news_label("AAA.US", "Financials", t, names, d) == (
        "AVOIDED - Financials: political events AVOIDED, events + news themes AVOIDED; news list of 19 Nov: would leave (both arms)")
    assert tl.news_label("HHH.US", "Financials", t, names, d) == (
        "AVOIDED - Financials: political events AVOIDED, events + news themes AVOIDED; "
        "news list of 19 Nov: would leave (political events)")
    assert tl.news_label("BBB.US", "Information Technology", t, names, d) == (
        "FAVOURED - Information Technology: political events FAVOURED, events + news themes neutral; "
        "news list of 19 Nov: would enter (political events)")
    assert tl.news_label("CCC.US", "Industrials", t, names, d).startswith(
        "MIXED - Industrials: political events FAVOURED, events + news themes AVOIDED")
    assert tl.news_label("2222.SR", "Energy", t, names, d) == (
        "NEUTRAL - Energy: political events neutral, events + news themes neutral; "
        "measured on US sector funds - indicative only for this market")
    assert tl.news_label("ZZZ", "", t, names, d) == "NO SECTOR - not known to the archive"
    assert tl.news_label("AAA.US", "Utilities", t, names, d).startswith("NEUTRAL - Utilities: political events not rated")
    # a list day outside the tilt's window: no move is claimed
    later = [dict(r, base_date="2026-11-24") for r in SHADOW]
    d2, names2 = nb.shadow_names(later)
    assert "news list" not in tl.news_label("AAA.US", "Financials", t, names2, d2)
    # no tilt in force (the window has closed): nothing is claimed for the sector
    late = nb.pick_tilt(TILT, dt.date(2026, 11, 25))
    assert tl.tilt_week(late) == "" and tl.tilt_until(late) == "" and tl.tilt_until({}) == "" and tl.news_label("AAA.US", "Financials", late, names, d) == \
        "NO TILT IN FORCE - no news week applies today (Financials)"
    assert tl.news_label("AAA.US", "Financials", {}, {}, "") == "NO TILT IN FORCE - no news week applies today (Financials)"


# ----------------------------------------------------------------------------- L4 SQL
def test_l4_sql_is_select_only_dated_and_reads_the_trial_view():
    q = tl.sql_c2(DS, TODAY)
    assert q.startswith("WITH c AS (SELECT symbol, market_group, horizon") and "`proj-1.tfb_archive.v_c2_calls`" in q
    assert "WHERE recorded_date >= DATE_SUB(DATE '2026-11-19', INTERVAL 7 DAY) AND recorded_date <= DATE '2026-11-19'" in q
    assert "SELECT c.* FROM c JOIN m ON c.call_day = m.d" in q
    for word in ("INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP"):
        assert word not in q.upper().replace("UPSIDE", "")
    try:
        tl.sql_c2(DS, NOW)
    except ValueError:
        pass
    else:
        raise AssertionError("a datetime must be refused")


# ----------------------------------------------------------------------------- L5 a full run
def test_l5_a_full_run_writes_one_tab(monkeypatch, capsys):
    _env(monkeypatch)
    book, sess = world(), Session()
    assert tl.run_once(False, book=book, session=sess, now=NOW, query=eb.bq_query) == 0
    assert list(book.tabs) == ["Final_Decisions", "My_Portfolio", "Trial_Labels"] and book.tabs["Trial_Labels"].updates == 1
    assert book.tabs["Final_Decisions"].ranges == ["A1:B400"] and book.tabs["My_Portfolio"].ranges == ["A1:B200"]
    assert book.tabs["Final_Decisions"].updates == 0 and book.tabs["My_Portfolio"].updates == 0
    assert len(sess.sql) == 4 and ".v_symbol_sector`" in sess.sql[3]
    assert "WHERE symbol IN ('AAA.US', 'HHH.US', '2222.SR', 'ZZZ', 'ZZZ.US', 'BBB.US', 'CCC.US')" in sess.sql[3]
    page = book.tabs["Trial_Labels"].values
    assert all(len(r) == tl.WIDTH for r in page) and len(page) >= 60
    cells = [str(c) for r in page for c in r]
    assert not any("None" in c or c.lower() in ("nan", "inf") for c in cells)
    assert page[0][0].startswith("TRIAL LABELS - ") and "OBSERVE ONLY" in page[0][0]
    assert page[1][0] == ("Built 2026-11-19 08:05 Riyadh | trial_labels 1.0.0 | C-2: buy calls of 2026-11-19 | news: tilt of the "
                          "news week of 2026-11-09, in force 2026-11-17 to 2026-11-23; list day 2026-11-19 | only the Today "
                          "Action Plan gives orders")
    head = page.index(tl.HEAD)
    assert head == 5 and [r[0] for r in page[head + 1:head + 7]] == ["AAA.US", "HHH.US", "2222.SR", "ZZZ", "BBB.US", "CCC.US"]
    aaa = row_of(page, "AAA.US")[0]
    assert aaa[1] == "BUY" and aaa[2].startswith("KEEPS (all horizons)") and aaa[3].startswith("AVOIDED - Financials")
    assert aaa[4:] == ["2026-11-19", "2026-11-09", "2026-11-23", "Financials"]
    hhh = row_of(page, "HHH.US")[0]
    assert hhh[1:3] == ["HOLD", "NO BUY CALL - none on 19 Nov; C-2 judges buy calls only"]
    zzz = row_of(page, "ZZZ")[0]
    assert zzz[1] == "" and zzz[3] == "NO SECTOR - not known to the archive" and zzz[6:] == ["2026-11-23", ""]
    assert page[-1] == [""] * tl.WIDTH
    last_text = [r[0] for r in page if r[0]][-1]
    assert last_text.startswith("How to read:") and last_text.endswith("Not investment advice.")
    out = capsys.readouterr().out
    assert "C-2: buy calls of 2026-11-19 (4 name(s) called) | news: week 2026-11-09 | list day 2026-11-19" in out
    assert ("LABELS 6 stock(s) (3 from Final_Decisions, 2 holding(s) read): c2 DROPS 2, c2 KEEPS 1, c2 NO BUY CALL 2, "
            "c2 PARTLY 1, news AVOIDED 2, news FAVOURED 1, news MIXED 1, news NEUTRAL 1, news NO SECTOR 1") in out
    assert "wrote the Trial_Labels tab (6 stock(s))" in out
    for sym in ("AAA", "HHH", "2222", "ZZZ", "BBB", "CCC", "Hotel", "Zulu"):
        assert sym not in out, sym                                              # no stock is named in the run log


def test_l5_empty_inputs_still_make_an_honest_tab(monkeypatch, capsys):
    _env(monkeypatch)
    book, sess = Book({}), Session(calls=[], tilt_rows=[], shadow=[])
    assert tl.run_once(False, book=book, session=sess, now=NOW, query=eb.bq_query) == 0
    assert len(sess.sql) == 3 and not any("v_symbol_sector" in q for q in sess.sql)      # nothing to look up
    page = book.tabs["Trial_Labels"].values
    assert "C-2: buy calls of none in the archive | news: no news tilt in force; list day none" in page[1][0]
    assert any("No stock could be read" in str(c) for r in page for c in r)
    assert "LABELS 0 stock(s) (0 from Final_Decisions, 0 holding(s) read): none" in capsys.readouterr().out


# ----------------------------------------------------------------------------- L6 off, dry run and failures
def test_l6_dry_run_off_switch_and_failures_write_nothing(monkeypatch, capsys):
    _env(monkeypatch)
    book, sess = world(), Session()
    assert tl.run_once(True, book=book, session=sess, now=NOW, query=eb.bq_query) == 0
    assert "Trial_Labels" not in book.tabs and len(sess.sql) == 4 and "dry run:" in capsys.readouterr().out
    assert tl.main(["--dry-run"], book=book, session=Session(), now=NOW, query=eb.bq_query) == 0 and "Trial_Labels" not in book.tabs
    for view in ("v_c2_calls", "v_news_sector_tilt", "v_news_overlay_list", "v_symbol_sector"):
        old = world()
        old.tabs["Trial_Labels"] = Sheet([["old tab"]])
        assert tl.run_once(False, book=old, session=Session(fail=view), now=NOW, query=eb.bq_query) == 2, view
        assert old.tabs["Trial_Labels"].values == [["old tab"]] and old.tabs["Trial_Labels"].updates == 0
        assert "nothing written" in capsys.readouterr().out
    monkeypatch.setenv("TFB_TRIAL_LABELS", "off")
    sess = Session()
    assert tl.run_once(False, book=book, session=sess, now=NOW, query=eb.bq_query) == 0 and sess.sql == []
    assert "is off" in capsys.readouterr().out
    monkeypatch.setenv("TFB_TRIAL_LABELS", "on")
    monkeypatch.setenv("TFB_BQ_PROJECT", "bad project;")
    assert tl.run_once(False, book=book, session=sess, now=NOW, query=eb.bq_query) == 2 and sess.sql == []


class Quota(Exception):
    def __str__(self):
        return "APIError: [429]: Quota exceeded for quota metric 'Write requests'"


def test_l6_a_quota_answer_is_waited_out(monkeypatch, capsys):
    _env(monkeypatch)
    waits = []
    monkeypatch.setattr(tl, "_SLEEP", waits.append)

    class Flaky(Sheet):
        def __init__(self, fails):
            super().__init__([["old tab"]])
            self.fails = fails

        def update(self, values=None, range_name=None, value_input_option=None):
            if self.fails > 0:
                self.fails -= 1
                raise Quota()
            super().update(values=values, range_name=range_name, value_input_option=value_input_option)

    book = world()
    book.tabs["Trial_Labels"] = Flaky(1)
    sess = Session()
    assert tl.run_once(False, book=book, session=sess, now=NOW, query=eb.bq_query) == 0
    assert waits == [65.0] and book.tabs["Trial_Labels"].updates == 1 and len(sess.sql) == 4
    book = world()
    book.tabs["Trial_Labels"] = Flaky(9)
    waits.clear()
    assert tl.run_once(False, book=book, session=Session(), now=NOW, query=eb.bq_query) == 2
    assert book.tabs["Trial_Labels"].values == [["old tab"]] and "the tab was not written" in capsys.readouterr().out


# ----------------------------------------------------------------------------- L7 wiring
def test_l7_workflow_manifest_and_read_only_scope():
    raw = (ROOT / ".github/workflows/trial_labels.yml").read_text(encoding="utf-8")
    wf = yaml.safe_load(raw)
    on = wf[True] if True in wf else wf["on"]
    assert set(on) == {"schedule", "workflow_dispatch", "workflow_run"} and on["schedule"] == [{"cron": "5 5,9,14 * * *"}]
    assert on["workflow_run"] == {"workflows": ["\U0001F3AF Exit Plan"], "types": ["completed"]}   # 1.1.0 CHAIN (test_page_chains)
    assert on["workflow_dispatch"]["inputs"]["dry_run"]["default"] is False
    assert wf["permissions"] == {"contents": "read"} and wf["concurrency"] == {"group": "tfb-trial-labels", "cancel-in-progress": False}
    job = wf["jobs"]["labels"]
    assert "if" not in job and job["timeout-minutes"] == 15
    step = [s for s in job["steps"] if "trial_labels.py" in str(s.get("run", ""))][0]
    assert step["env"]["TFB_TRIAL_LABELS"] == "${{ vars.TFB_TRIAL_LABELS || 'on' }}"
    assert step["env"]["TFB_BQ_PROJECT"] == "${{ vars.TFB_BQ_PROJECT || 'project-2e3530e6-2311-4aa3-b36' }}"
    assert "--dry-run" in step["run"] and 'python scripts/trial_labels.py "${ARGS[@]}"' in step["run"]
    src = (ROOT / "scripts/trial_labels.py").read_text(encoding="utf-8")
    for name in re.findall(r"(TFB_[A-Z_]+): \$\{\{", raw):
        assert name in src, name
    assert src.isascii() and tl.__version__ == "1.0.0" and "v1.0.0 (2026-11-19)" in src
    assert '("trial_labels", "%s", "trial labels")' % tl.__version__ in (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert src.count("write_grid(") == 1 and "write_grid(book, TAB," in src and tl.TAB == "Trial_Labels"
    assert src.count("_raw_read(") == 2 and '"A1:B400"' in src and '"A1:B200"' in src            # two reads, two columns each
    assert "tests/test_trial_labels.py" in (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    # the tab the column script reads: the header names it looks for are the ones written here
    gs = (ROOT / "apps_script/31_Final_Decision_Column.gs").read_text(encoding="utf-8")
    m = re.search(r"TRIALS: \{\s*// [^\n]*\n\s*SOURCE: '([A-Za-z_]+)',\s*NEED: \[([^\]]+)\]", gs)
    assert m and m.group(1) == tl.TAB
    need = [x.strip().strip("'") for x in m.group(2).split(",")]
    assert set(need) <= set(tl.HEAD) and need[0] == "Symbol"
