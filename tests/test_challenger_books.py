"""tests/test_challenger_books.py - scripts/challenger_books.py 1.0.0 (each forward book of the shadow challenger
frozen once, the day it is final; research only)."""
from __future__ import annotations

import datetime as dt
import re
from pathlib import Path

import pytest
import yaml

from scripts import challenger_books as cb
from scripts import evidence_board as eb

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 12, 2, 7, 5, tzinfo=UTC)            # Wed 2 Dec 2026, after the morning bars
MON = dt.date(2026, 11, 30)                                   # the first forward book
SQL_DIR = Path("sql/bigquery")
SYMBOLS = ["N%03d.US" % i for i in range(10)] + ["K%03d.SR" % i for i in range(6)]

# the walk-forward view's columns, read from INFORMATION_SCHEMA.COLUMNS on 27 Nov 2026 (views 1.36.0)
VIEW_COLUMNS = [
    "market", "sector", "symbol", "d", "vol63", "mom12_vs", "mom6_vs", "rs63", "low_down126", "dev_m12", "dev_m6",
    "dev_rs", "dev_dd", "sector_ranked", "bench_symbol", "entry_date", "exit_date", "fwd_pp", "bench_pp", "score_eq",
    "excess_pp", "excess_w_pp", "label_pr", "train_dates", "weights_basis", "train_ic_m12", "train_ic_m6",
    "train_ic_rs", "train_ic_dd", "w_m12", "w_m6", "w_rs", "w_dd", "score_wf", "q_wf", "q_eq", "shadow_action",
    "shadow_size_pct", "is_latest"]
RESULT_COLUMNS = {"entry_date", "exit_date", "fwd_pp", "bench_pp", "excess_pp", "excess_w_pp", "label_pr", "is_latest"}


def book(market, d, symbols, n=None, **extra):
    """One book as the reader returns it: every value a string (or None), book_n the book's own size."""
    rows = []
    for i, s in enumerate(symbols):
        q = 1 + i % 5
        r = {c: None for c in cb.BOOK_COLUMNS}
        r.update(d=d.isoformat(), market=market, symbol=s, sector="Energy", vol63="0.31", mom12_vs="0.5",
                 dev_m12="0.1", sector_ranked="1", score_eq="51.5", q_eq=str(q), train_dates="40",
                 weights_basis="trained", train_ic_m12="0.04", w_m12="0.4", w_m6="0.1", w_rs="0.3", w_dd="0.2",
                 score_wf="52.25", q_wf=str(q), shadow_size_pct="10.0" if q == 5 else "0",
                 shadow_action={5: "shadow buy", 1: "shadow avoid"}.get(q, "shadow hold"),
                 bench_symbol="SPY" if market == "US" else "^TASI.SR", book_n=str(n if n is not None else len(symbols)))
        r.update(extra)
        rows.append(r)
    return rows


def sessions(market, *days):
    return [{"market": market, "date": d.isoformat()} for d in days]


class Query:
    """The SELECT-only reader, answering by the table each query reads."""

    def __init__(self, books=(), frozen=(), sess=(), views=(), table=True, fail=None, views_fail=False):
        self.books, self.frozen, self.sess, self.views = list(books), list(frozen), list(sess), list(views)
        self.table, self.fail, self.views_fail, self.sql = table, fail, views_fail, []

    def __call__(self, session, project, location, sql):
        self.sql.append(sql)
        if self.fail and self.fail in sql:
            raise RuntimeError("BigQuery refused the query (HTTP 400): bad")
        if "INFORMATION_SCHEMA.VIEWS" in sql:
            if self.views_fail:
                raise RuntimeError("BigQuery refused the query (HTTP 403): denied")
            return list(self.views)
        if ".challenger_books` GROUP BY" in sql:
            if not self.table:
                raise RuntimeError("BigQuery refused the query (HTTP 404): Not found: Table p:d.challenger_books")
            return [dict(r) for r in self.frozen]
        if ".v_challenger_v0_walkforward` AS w" in sql:
            floor = dt.date.fromisoformat(re.search(r"w\.d >= DATE '([0-9-]+)'", sql).group(1))
            if "NOT EXISTS" in sql and not self.table:
                raise RuntimeError("BigQuery refused the query (HTTP 404): Not found: Table p:d.challenger_books")
            held = {(r["market"], r["d"]) for r in self.frozen} if "NOT EXISTS" in sql else set()
            return [dict(r) for r in self.books
                    if dt.date.fromisoformat(r["d"]) >= floor and (r["market"], r["d"]) not in held]
        if ".symbol_bars_daily`" in sql:
            return list(self.sess)
        raise AssertionError("unexpected query: " + sql[:80])


class BQ:
    def __init__(self, fail_market=None):
        self.ensured, self.loads, self.fail_market = [], [], fail_market

    def ensure(self, session, project, dataset, table, spec=None):
        self.ensured.append((table, spec))
        return False

    def load(self, session, project, dataset, location, table, day, rows, run_id, schema=None, disposition=None):
        if self.fail_market and rows and rows[0]["market"] == self.fail_market:
            raise RuntimeError("load failed for " + rows[0]["symbol"])
        self.loads.append({"table": table, "day": day, "rows": rows, "run_id": run_id, "schema": schema,
                           "disposition": disposition})
        return len(rows)


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("TFB_BQ_PROJECT", "proj-test-1")
    monkeypatch.setenv("TFB_BQ_DATASET", "tfb_archive")
    monkeypatch.setenv("GITHUB_RUN_ID", "991")
    monkeypatch.delenv("TFB_CHALLENGER_BOOKS", raising=False)


def run(query, dry=False, now=NOW, bq=None):
    bq = bq or BQ()
    rc = cb.run_once(dry, session=object(), now=now, query=query, ensure=bq.ensure, load=bq.load)
    return rc, bq


US_SESS = sessions("US", MON, MON + dt.timedelta(1))
SA_SESS = sessions("SA", MON, MON + dt.timedelta(1))


# ----------------------------------------------------------------------------- the SQL
def test_every_query_is_one_select_the_shared_reader_accepts():
    class Session:
        def __init__(self):
            self.sent = []

        def post(self, url, timeout=None, json=None):
            self.sent.append(json["query"])
            return type("R", (), {"status_code": 200, "json": lambda self: {"jobComplete": True, "rows": []}})()

    s, ds, since = Session(), "p.d", dt.date(2026, 11, 20)
    for sql in (cb.sql_books(ds, since), cb.sql_books(ds, since, unfrozen=False), cb.sql_sessions(ds, since),
                cb.sql_frozen(ds), cb.sql_views(ds)):
        assert eb.bq_query(s, "p", "US", sql) == []
    assert len(s.sent) == 5


def test_the_book_query_reads_inputs_never_results():
    sql = cb.sql_books("p.d", dt.date(2026, 11, 6))
    cols = re.search(r"^SELECT (.*), COUNT\(\*\) OVER", sql, re.M).group(1).split(", ")
    assert cols == ["w." + c for c in cb.BOOK_COLUMNS]
    assert set(cb.BOOK_COLUMNS) | RESULT_COLUMNS == set(VIEW_COLUMNS)          # every input; no result column
    assert not RESULT_COLUMNS & set(cb.BOOK_COLUMNS)
    assert "PARTITION BY w.market, w.d) AS book_n" in sql
    assert "w.d >= DATE '2026-11-27'" in sql                                  # never before the forward start
    assert "NOT EXISTS (SELECT 1 FROM `p.d.challenger_books` AS c WHERE c.market = w.market AND c.d = w.d)" in sql
    assert sql.rstrip().endswith("ORDER BY w.market, w.d, w.symbol")          # a page cut falls on the last book
    assert "NOT EXISTS" not in cb.sql_books("p.d", dt.date(2026, 11, 6), unfrozen=False)
    assert "w.d >= DATE '2027-01-09'" in cb.sql_books("p.d", dt.date(2027, 1, 9))


def test_every_book_column_is_named_in_the_challenger_views():
    src = "".join((SQL_DIR / f).read_text(encoding="utf-8") for f in
                  ("144_v_challenger_v0_features.sql", "145_v_challenger_v0_ranks.sql",
                   "146_v_challenger_v0_walkforward.sql"))
    for c in cb.BOOK_COLUMNS:
        assert re.search(r"\b%s\b" % c, src), c


def test_sessions_use_the_features_views_own_market_of_a_symbol():
    case = ("WHEN 'US' THEN 'US' WHEN 'SR' THEN 'SA' WHEN 'T' THEN 'JP' WHEN 'HK' THEN 'HK' ELSE 'OTHER' END AS market")
    assert case in (SQL_DIR / "144_v_challenger_v0_features.sql").read_text(encoding="utf-8")
    sql = cb.sql_sessions("p.d", dt.date(2026, 11, 11))
    assert case in sql and "FROM `p.d.symbol_bars_daily`" in sql and "date >= DATE '2026-11-11'" in sql
    assert "HAVING COUNT(DISTINCT symbol) >= 20" in sql and cb.MIN_SESSION_NAMES == 20


def test_views_query_names_the_chain():
    sql = cb.sql_views("p.d")
    assert "FROM `p.d.INFORMATION_SCHEMA.VIEWS`" in sql and "TO_HEX(SHA256(view_definition))" in sql
    assert sql.endswith(" WHERE table_name IN ('v_symbol_sector', 'v_bar_jumps', 'v_bars_clean', "
                        "'v_challenger_v0_features', 'v_challenger_v0_ranks', 'v_challenger_v0_walkforward')")
    assert cb.CHAIN[-1] == cb.VIEW and "v_bars_clean" in cb.CHAIN


def test_schema():
    names = [f["name"] for f in cb.SCHEMA]
    assert len(names) == len(set(names)) and names[:3] == ["d", "market", "symbol"]
    assert cb.BOOK_COLUMNS == names[:31] and names[31:] == list(cb._STAMP_COLUMNS)
    req = {f["name"] for f in cb.SCHEMA if f["mode"] == "REQUIRED"}
    assert req == {"d", "market", "symbol", "book_n", "frozen_at", "frozen_days_after"}
    schema, part, cluster, desc = cb.SPEC
    assert schema is cb.SCHEMA and part == "d" and cluster == ["market", "symbol"] and "1.0.0" in desc
    assert {f["type"] for f in cb.SCHEMA} == {"DATE", "STRING", "FLOAT", "INTEGER", "TIMESTAMP"}


# ----------------------------------------------------------------------------- the plan
def plan_of(books, sess, frozen=None):
    rows = [r for b in books for r in b]
    return {(e["market"], e["d"]): e for e in cb.plan(cb.books_by_key(rows), cb.sessions_by_market(sess),
                                                       frozen or {})}


def test_a_final_whole_book_is_frozen():
    e = plan_of([book("US", MON, SYMBOLS[:10])], US_SESS)[("US", MON)]
    assert e["action"] == "FREEZE" and e["why"] == "" and e["names"] == 10 and e["book_n"] == 10 and e["later"] == 1


def test_not_final_until_a_later_session_of_that_market_is_archived():
    e = plan_of([book("US", MON, SYMBOLS[:10])], sessions("US", MON) + sessions("SA", MON + dt.timedelta(1)))
    assert e[("US", MON)]["action"] == "SKIP" and "not final yet" in e[("US", MON)]["why"]
    e = plan_of([book("US", MON, SYMBOLS[:10])], [])
    assert e[("US", MON)]["action"] == "SKIP" and "no later session of US" in e[("US", MON)]["why"]


def test_a_short_book_is_not_frozen():
    e = plan_of([book("US", MON, SYMBOLS[:7], n=10)], US_SESS)[("US", MON)]
    assert e["action"] == "SKIP" and e["why"] == "short (7 of 10 rows came back)"


def test_frozen_before_or_outside_the_forward_test_is_skipped():
    old = dt.date(2026, 11, 23)
    e = plan_of([book("US", MON, SYMBOLS[:10]), book("US", old, SYMBOLS[:10])],
                US_SESS + sessions("US", old + dt.timedelta(1)), {("US", MON): 10})
    assert e[("US", MON)]["action"] == "SKIP" and e[("US", MON)]["why"] == "frozen already (10 rows)"
    assert e[("US", old)]["action"] == "SKIP" and e[("US", old)]["why"] == "before the forward start"


def test_one_book_per_market_and_week():
    tue = MON + dt.timedelta(1)
    e = plan_of([book("JP", tue, SYMBOLS[:10])], sessions("JP", tue, tue + dt.timedelta(1)), {("JP", MON): 10})
    assert e[("JP", tue)]["action"] == "SKIP" and "a book of this week is frozen already (d 2026-11-30)" in e[("JP", tue)]["why"]
    nxt = MON + dt.timedelta(7)                                              # the next week is its own book
    e = plan_of([book("JP", nxt, SYMBOLS[:10])], sessions("JP", nxt, nxt + dt.timedelta(1)), {("JP", MON): 10})
    assert e[("JP", nxt)]["action"] == "FREEZE"


def test_a_thin_book_waits_one_session_then_is_frozen_as_it_is():
    nxt = MON + dt.timedelta(7)
    frozen = {("US", MON): 10, ("US", dt.date(2026, 11, 23)): 99}           # the latest earlier book is the reference
    e = plan_of([book("US", nxt, SYMBOLS[:8])], sessions("US", nxt, nxt + dt.timedelta(1)), frozen)[("US", nxt)]
    assert e["action"] == "HOLD" and e["why"].startswith("thin (8 names; the book of 2026-11-30 had 10)")
    e = plan_of([book("US", nxt, SYMBOLS[:8])], sessions("US", nxt, nxt + dt.timedelta(1), nxt + dt.timedelta(2)),
                frozen)[("US", nxt)]
    assert e["action"] == "FREEZE" and e["why"] == "thin (8 names; the book of 2026-11-30 had 10), frozen as it is"


def test_thin_is_under_ninety_percent_of_the_last_frozen_book_of_the_same_market():
    nxt = MON + dt.timedelta(7)
    sess = sessions("US", nxt, nxt + dt.timedelta(1))
    assert plan_of([book("US", nxt, SYMBOLS[:9])], sess, {("US", MON): 10})[("US", nxt)]["action"] == "FREEZE"
    assert plan_of([book("US", nxt, SYMBOLS[:8])], sess, {("SA", MON): 10})[("US", nxt)]["action"] == "FREEZE"
    later = {("US", nxt + dt.timedelta(7)): 100}                             # a later book is no reference
    assert plan_of([book("US", nxt, SYMBOLS[:8])], sess, later)[("US", nxt)]["action"] == "FREEZE"
    first = plan_of([book("US", MON, SYMBOLS[:2])], US_SESS)[("US", MON)]   # the first book has no reference
    assert first["action"] == "FREEZE"


def test_rows_and_their_types():
    e = plan_of([book("US", MON, SYMBOLS[:2], vol63="")], US_SESS)[("US", MON)]
    rows = cb.book_rows(e, NOW, "991", "views 1.37.0", "v_challenger_v0_walkforward=0e39890fc235")
    assert [list(r) for r in rows] == [[f["name"] for f in cb.SCHEMA]] * 2
    r = rows[0]
    assert r["d"] == "2026-11-30" and r["market"] == "US" and r["symbol"] == "N000.US"
    assert r["vol63"] is None and r["mom6_vs"] is None and r["mom12_vs"] == 0.5 and r["q_wf"] == 1
    assert isinstance(r["sector_ranked"], int) and isinstance(r["score_wf"], float) and r["train_dates"] == 40
    assert r["book_n"] == 2 and r["frozen_at"] == "2026-12-02T07:05:00Z" and r["frozen_days_after"] == 2
    assert r["run_id"] == "991" and r["views_version"] == "views 1.37.0"
    assert r["views_sha"] == "v_challenger_v0_walkforward=0e39890fc235"
    riyadh = NOW.astimezone(dt.timezone(dt.timedelta(hours=3)))
    assert cb.book_rows(e, riyadh, "", "v", None)[0]["frozen_at"] == "2026-12-02T07:05:00Z"
    assert cb.book_rows(e, riyadh, "", "v", None)[0]["run_id"] is None
    late = dt.datetime(2026, 12, 1, 22, 30, tzinfo=UTC)                        # 01:30 on 2 Dec in Riyadh
    assert cb.book_rows(e, late.astimezone(riyadh.tzinfo), "", "v", None)[0]["frozen_days_after"] == 1


def test_views_sha_in_chain_order():
    rows = [{"table_name": "v_challenger_v0_walkforward", "sha": "0E39890FC235A201"},
            {"table_name": "v_symbol_sector", "sha": "f5018131ba52c288"},
            {"table_name": "v_other", "sha": "ffffffffffffffff"}]
    assert cb.views_sha(rows) == "v_symbol_sector=f5018131ba52;v_challenger_v0_walkforward=0e39890fc235"
    assert cb.views_sha([]) is None


def test_sessions_by_market():
    got = cb.sessions_by_market(sessions("US", MON, MON) + [{"market": "US", "date": "bad"}, {"market": "", "date": "2026-11-30"}]
                                + sessions("US", dt.date(2026, 11, 25)))
    assert got == {"US": [dt.date(2026, 11, 25), MON]}


def test_summary_counts_without_a_symbol():
    e = plan_of([book("US", MON, SYMBOLS[:10])], US_SESS)[("US", MON)]
    line = cb.summary(e)
    assert line.startswith("US    2026-11-30  FREEZE   10 names  ")
    assert "shadow avoid 2, shadow buy 2, shadow hold 6" in line and "N00" not in line


# ----------------------------------------------------------------------------- the run
def test_a_write_run_freezes_each_final_book_and_logs_counts_only(capsys):
    q = Query(books=book("US", MON, SYMBOLS[:10]) + book("SA", MON, SYMBOLS[10:]) + book("JP", MON, SYMBOLS[:6]),
              sess=US_SESS + SA_SESS + sessions("JP", MON),
              views=[{"table_name": "v_challenger_v0_walkforward", "sha": "0e39890fc235a201"}])
    rc, bq = run(q)
    assert rc == 0
    assert bq.ensured == [("challenger_books", cb.SPEC)]
    assert [(x["table"], x["day"], x["disposition"], len(x["rows"])) for x in bq.loads] == [
        ("challenger_books", "2026-11-30", "WRITE_APPEND", 6), ("challenger_books", "2026-11-30", "WRITE_APPEND", 10)]
    assert all(x["schema"] is cb.SCHEMA and x["run_id"] == "991" for x in bq.loads)
    sa = bq.loads[0]["rows"]
    assert {r["market"] for r in sa} == {"SA"} and sa[0]["views_sha"] == "v_challenger_v0_walkforward=0e39890fc235"
    assert sa[0]["views_version"].startswith("views ") and sa[0]["frozen_days_after"] == 2
    out = capsys.readouterr().out
    for s in SYMBOLS:
        assert s not in out, s
    assert "0 book(s) frozen so far; 22 row(s) of books not frozen yet came back" in out
    assert "JP    2026-11-30  SKIP" in out and "not final yet" in out
    assert "froze SA 2026-11-30: 6 row(s) into challenger_books$20261130" in out
    assert "froze US 2026-11-30: 10 row(s) into challenger_books$20261130" in out
    # the reads: the frozen keys, the books not frozen yet, the sessions, then the view hashes
    assert ".challenger_books` GROUP BY" in q.sql[0] and "NOT EXISTS" in q.sql[1]
    assert ".symbol_bars_daily`" in q.sql[2] and "INFORMATION_SCHEMA.VIEWS" in q.sql[3] and len(q.sql) == 4


def test_a_second_run_freezes_nothing_again():
    q = Query(books=book("US", MON, SYMBOLS[:10]), sess=US_SESS, frozen=[{"market": "US", "d": "2026-11-30", "n": "10"}])
    rc, bq = run(q)
    assert rc == 0 and bq.loads == [] and not any("INFORMATION_SCHEMA" in s for s in q.sql)


def test_the_window_is_three_weeks_and_starts_at_the_forward_start():
    q = Query()
    run(q, now=dt.datetime(2026, 11, 28, 7, 0, tzinfo=UTC))
    assert "w.d >= DATE '2026-11-27'" in q.sql[1] and "date >= DATE '2026-11-07'" in q.sql[2]
    q = Query()
    run(q, now=dt.datetime(2027, 1, 30, 7, 0, tzinfo=UTC))
    assert "w.d >= DATE '2027-01-09'" in q.sql[1] and "date >= DATE '2027-01-09'" in q.sql[2]


def test_a_dry_run_reads_before_the_table_exists_and_writes_nothing(capsys):
    q = Query(books=book("US", MON, SYMBOLS[:10]), sess=US_SESS, table=False)
    rc, bq = run(q, dry=True)
    assert rc == 0 and bq.ensured == [] and bq.loads == []
    assert ".challenger_books` GROUP BY" in q.sql[0]                         # refused: the table is not there yet
    assert ".v_challenger_v0_walkforward` AS w" in q.sql[1] and "NOT EXISTS" not in q.sql[1]
    out = capsys.readouterr().out
    assert "dry run: 1 book(s) would be frozen, nothing written" in out and "US    2026-11-30  FREEZE" in out


def test_a_write_run_never_reads_round_a_missing_table(capsys):
    rc, bq = run(Query(books=book("US", MON, SYMBOLS[:10]), sess=US_SESS, table=False))
    assert rc == 2 and bq.loads == []
    assert "ERROR: RuntimeError before the freeze" in capsys.readouterr().out


def test_a_failed_read_writes_nothing(capsys):
    for part in (".symbol_bars_daily`", ".v_challenger_v0_walkforward` AS w", ".challenger_books` GROUP BY"):
        rc, bq = run(Query(books=book("US", MON, SYMBOLS[:10]), sess=US_SESS, fail=part))
        assert rc == 2 and bq.loads == [], part
    assert "nothing written" in capsys.readouterr().out


def test_one_market_failing_never_stops_the_others(capsys):
    q = Query(books=book("US", MON, SYMBOLS[:10]) + book("SA", MON, SYMBOLS[10:]), sess=US_SESS + SA_SESS)
    rc, bq = run(q, bq=BQ(fail_market="SA"))
    assert rc == 1 and [x["rows"][0]["market"] for x in bq.loads] == ["US"]
    out = capsys.readouterr().out
    assert "ERROR: SA 2026-11-30 was not frozen (RuntimeError)" in out and "K000.SR" not in out


def test_a_book_is_worth_more_than_its_hash(capsys):
    q = Query(books=book("US", MON, SYMBOLS[:10]), sess=US_SESS, views_fail=True)
    rc, bq = run(q)
    assert rc == 0 and len(bq.loads) == 1 and bq.loads[0]["rows"][0]["views_sha"] is None
    assert "views_sha not read (RuntimeError); the books are frozen without it" in capsys.readouterr().out


def test_at_the_page_limit_the_whole_books_are_frozen_and_the_cut_one_waits(monkeypatch, capsys):
    monkeypatch.setattr(cb, "PAGE_LIMIT", 15)
    cut = book("US", MON, SYMBOLS[:10])[:5]                                    # the page ended inside this book
    q = Query(books=book("SA", MON, SYMBOLS[10:]) + book("SA", MON + dt.timedelta(7), SYMBOLS[10:]) + cut,
              sess=SA_SESS + sessions("SA", MON + dt.timedelta(7), MON + dt.timedelta(8)) + US_SESS)
    rc, bq = run(q, now=dt.datetime(2026, 12, 9, 7, 0, tzinfo=UTC))
    assert rc == 0 and [(x["rows"][0]["market"], x["day"]) for x in bq.loads] == [
        ("SA", "2026-11-30"), ("SA", "2026-12-07")]
    out = capsys.readouterr().out
    assert "17 row(s) of books not frozen yet came back - the reader's page limit" in out
    assert "US    2026-11-30  SKIP" in out and "short (5 of 10 rows came back)" in out


def test_nothing_final_and_nothing_at_all(capsys):
    rc, bq = run(Query(books=book("US", MON, SYMBOLS[:10]), sess=sessions("US", MON)))
    assert rc == 0 and bq.loads == [] and "nothing to freeze" in capsys.readouterr().out
    rc, bq = run(Query())
    assert rc == 0 and bq.loads == [] and "no book to freeze" in capsys.readouterr().out


def test_the_switch_and_the_configuration(monkeypatch, capsys):
    monkeypatch.setenv("TFB_CHALLENGER_BOOKS", "off")
    q = Query(books=book("US", MON, SYMBOLS[:10]), sess=US_SESS)
    rc, bq = run(q)
    assert rc == 0 and q.sql == [] and bq.ensured == [] and "is off" in capsys.readouterr().out
    monkeypatch.setenv("TFB_CHALLENGER_BOOKS", "on")
    monkeypatch.setenv("TFB_BQ_PROJECT", "bad project;")
    q = Query()
    rc, bq = run(q)
    assert rc == 2 and q.sql == [] and "missing or malformed" in capsys.readouterr().out
    monkeypatch.delenv("TFB_BQ_PROJECT")
    assert run(Query())[0] == 2


def test_main_parses_dry_run():
    q = Query(books=book("US", MON, SYMBOLS[:10]), sess=US_SESS)
    bq = BQ()
    assert cb.main(["--dry-run"], session=object(), now=NOW, query=q, ensure=bq.ensure, load=bq.load) == 0
    assert bq.loads == [] and bq.ensured == []


# ----------------------------------------------------------------------------- the source and the workflow
def test_source_is_ascii_and_versioned():
    src = Path(cb.__file__).read_text(encoding="utf-8")
    assert src.isascii() and cb.__version__ == "1.0.0" and "v1.0.0 (2026-11-27)" in src
    assert "print(" in src and src.count("print(") == 1                      # one log writer, counts only
    pins = Path("scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("challenger_books", "1.0.0", "challenger books")' in pins
    assert "tests/test_challenger_books.py" in Path(".github/workflows/ci.yml").read_text(encoding="utf-8")


def test_workflow_chains_to_the_deep_bars_and_writes_only_when_asked():
    wf = yaml.safe_load(Path(".github/workflows/challenger_books.yml").read_text(encoding="utf-8"))
    deep = yaml.safe_load(Path(".github/workflows/bq_symbol_deep.yml").read_text(encoding="utf-8"))
    on = wf[True] if True in wf else wf["on"]
    assert wf["name"] == "📒 Challenger Books" and "schedule" not in on
    assert on["workflow_run"] == {"workflows": [deep["name"]], "types": ["completed"], "branches": ["main"]}
    assert on["workflow_dispatch"]["inputs"]["write"] == {
        "description": "Freeze the final books into BigQuery (unticked = dry run)", "type": "boolean", "default": False}
    assert wf["permissions"] == {"contents": "read"}
    job = wf["jobs"]["freeze"]
    assert job["concurrency"] == {"group": "tfb-challenger-books", "cancel-in-progress": False} and "if" not in job
    step = [s for s in job["steps"] if s.get("name", "").startswith("📒")][0]
    env = step["env"]
    assert env["TFB_CHALLENGER_BOOKS"] == "${{ vars.TFB_CHALLENGER_BOOKS || 'on' }}"
    assert env["IN_WRITE"] == "${{ inputs.write }}" and env["EVENT_NAME"] == "${{ github.event_name }}"
    assert not any(k.startswith("EODHD") for k in env) and "${{" not in step["run"]
    assert '"$EVENT_NAME" == "workflow_dispatch" && "$IN_WRITE" != "true"' in step["run"]
    assert "ARGS+=(--dry-run)" in step["run"] and 'python scripts/challenger_books.py "${ARGS[@]}"' in step["run"]
