"""tests/test_regime_shadow.py - scripts/regime_shadow.py 1.0.0 (the shadow regime rule's label of each close,
frozen once when final) and its two views, v_regime_shadow (150) and v_regime_shadow_insample (151), views 1.38.0
(research only; nothing reads either)."""
from __future__ import annotations

import datetime as dt
import re
from pathlib import Path

import pytest
import yaml

from scripts import apply_bq_views as bv
from scripts import evidence_board as eb
from scripts import regime_shadow as rs

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 12, 2, 7, 5, tzinfo=UTC)            # Wed 2 Dec 2026, after the morning bars
SUN, MON, TUE = dt.date(2026, 11, 29), dt.date(2026, 11, 30), dt.date(2026, 12, 1)
SQL_DIR = Path("sql/bigquery")
VIEW_SQL = (SQL_DIR / "150_v_regime_shadow.sql").read_text(encoding="utf-8")
INSAMPLE_SQL = (SQL_DIR / "151_v_regime_shadow_insample.sql").read_text(encoding="utf-8")

# v_regime_shadow's columns in order (the view's final SELECT: j.* then the rule)
VIEW_COLUMNS = ["market_id", "market", "market_order", "reading_symbol", "date", "close", "sma200", "sma200_prev20",
                "trend", "breadth50_pct", "breadth_names", "vol20_pct", "vol_pct_rank", "adverse", "condition_label",
                "rule_version", "regime", "regime_why"]


def lab(market, d, regime="RISK_OFF", breadth="41.5", adverse="3", **extra):
    """One labelled close as the reader returns it: every value a string (or None)."""
    r = {c: None for c in rs.ROW_COLUMNS}
    r.update(date=d.isoformat(), market_id=market, market={"us": "United States", "sa": "Saudi Arabia"}.get(market, market),
             reading_symbol={"us": "SPY", "sa": "^TASI.SR"}.get(market, "^N225"), close="660.1", sma200="640.0",
             sma200_prev20="645.0", trend="mixed", breadth50_pct=breadth, breadth_names="480", vol20_pct="22.4",
             vol_pct_rank="84.0", adverse=adverse if market == "us" else None, condition_label="stress",
             rule_version="1", regime=regime, regime_why="trend mixed | volatility percentile 84")
    r.update(extra)
    return r


def closes(market, *days):
    return [{"market_id": market, "date": d.isoformat()} for d in days]


class Query:
    """The SELECT-only reader, answering by the object each query reads."""

    def __init__(self, rows=(), frozen=(), sess=(), views=(), table=True, fail=None, views_fail=False):
        self.rows, self.frozen, self.sess, self.views = list(rows), list(frozen), list(sess), list(views)
        self.table, self.fail, self.views_fail, self.sql = table, fail, views_fail, []

    def __call__(self, session, project, location, sql):
        self.sql.append(sql)
        if self.fail and self.fail in sql:
            raise RuntimeError("BigQuery refused the query (HTTP 400): bad")
        if "INFORMATION_SCHEMA.VIEWS" in sql:
            if self.views_fail:
                raise RuntimeError("BigQuery refused the query (HTTP 403): denied")
            return list(self.views)
        if ".regime_shadow_log` GROUP BY" in sql:
            if not self.table:
                raise RuntimeError("BigQuery refused the query (HTTP 404): Not found: Table p:d.regime_shadow_log")
            return [dict(r) for r in self.frozen]
        if ".v_regime_shadow` AS r" in sql:
            floor = dt.date.fromisoformat(re.search(r"r\.date >= DATE '([0-9-]+)'", sql).group(1))
            if "NOT EXISTS" in sql and not self.table:
                raise RuntimeError("BigQuery refused the query (HTTP 404): Not found: Table p:d.regime_shadow_log")
            held = {(r["market_id"], r["date"]) for r in self.frozen} if "NOT EXISTS" in sql else set()
            return [dict(r) for r in self.rows
                    if dt.date.fromisoformat(r["date"]) >= floor and (r["market_id"], r["date"]) not in held]
        if ".v_market_condition`" in sql:
            return list(self.sess)
        raise AssertionError("unexpected query: " + sql[:80])


class BQ:
    def __init__(self, fail_market=None):
        self.ensured, self.loads, self.fail_market = [], [], fail_market

    def ensure(self, session, project, dataset, table, spec=None):
        self.ensured.append((table, spec))
        return False

    def load(self, session, project, dataset, location, table, day, rows, run_id, schema=None, disposition=None):
        if self.fail_market and rows and rows[0]["market_id"] == self.fail_market:
            raise RuntimeError("load failed")
        self.loads.append({"table": table, "day": day, "rows": rows, "run_id": run_id, "schema": schema,
                           "disposition": disposition})
        return len(rows)


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("TFB_BQ_PROJECT", "proj-test-1")
    monkeypatch.setenv("TFB_BQ_DATASET", "tfb_archive")
    monkeypatch.setenv("GITHUB_RUN_ID", "992")
    monkeypatch.delenv("TFB_REGIME_SHADOW", raising=False)


def run(query, dry=False, now=NOW, bq=None):
    bq = bq or BQ()
    rc = rs.run_once(dry, session=object(), now=now, query=query, ensure=bq.ensure, load=bq.load)
    return rc, bq


US_SESS = closes("us", MON, TUE)
SA_SESS = closes("sa", SUN, MON, TUE)


# ----------------------------------------------------------------------------- the views
def _code(sql):
    return "\n".join(ln for ln in sql.splitlines() if not ln.lstrip().startswith("--"))


def test_the_views_install_in_order_after_the_challenger():
    objs = bv.load_objects()
    names = [o.name for o in objs]
    assert names[-2:] == ["v_regime_shadow", "v_regime_shadow_insample"]
    assert names.index("v_market_condition") < names.index("v_regime_shadow")
    assert names.index("v_market_drivers") < names.index("v_regime_shadow")
    assert names.index("v_bars_clean") < names.index("v_regime_shadow")
    assert bv.check_objects(objs) == [] and bv.__version__ == "1.38.0"


def test_the_label_view_reads_no_later_close():
    code = _code(VIEW_SQL)
    for bad in ("LEAD(", "FOLLOWING", "CURRENT_DATE", "CURRENT_TIMESTAMP", "f5", "f21", "fwd"):
        assert bad not in code, bad
    assert "ROWS BETWEEN 199 PRECEDING AND CURRENT ROW" in code               # the 200-close average
    assert "ROWS BETWEEN 251 PRECEDING AND CURRENT ROW" in code               # 252 closes of volatility
    assert "ROWS BETWEEN 49 PRECEDING AND CURRENT ROW" in code                # each name's 50-close average
    assert "LAG(IF(a.n200 = 200, a.a200, NULL), 20)" in code                  # the average 20 closes back
    assert "PARTITION BY n.symbol, n.basis" in code                           # inside one price basis
    assert "FROM `{{DS}}.v_bars_clean`" in code and "symbol_bars_daily" not in code


def test_the_rule_is_written_down_once_and_fixed():
    code = _code(VIEW_SQL)
    assert ("WHEN j.trend = 'down' OR j.vol_pct_rank >= 80 OR j.breadth50_pct < 30 OR j.adverse >= 3 THEN 'RISK_OFF'"
            in code)
    assert ("WHEN j.trend = 'up' AND j.vol_pct_rank < 60 AND IFNULL(j.breadth50_pct >= 50, TRUE)\n"
            "         AND (j.market_id != 'us' OR j.adverse <= 1) THEN 'BULL'") in code
    assert "ELSE 'CHOPPY'" in code and "1 AS rule_version" in code
    assert "WHEN j.trend IS NULL OR j.vol_pct_rank IS NULL THEN NULL" in code   # unknown inputs give no label
    assert "WHEN t.close > t.sma200 AND t.sma200 > t.sma200_prev20 THEN 'up'" in code
    assert "WHEN t.close < t.sma200 AND t.sma200 < t.sma200_prev20 THEN 'down'" in code
    assert "IF(b.breadth_names >= 20, b.breadth50_pct, NULL) AS breadth50_pct" in code
    assert "COUNTIF(v >= 0) < 200, NULL" in code                              # 200 known volatilities at least
    assert "IF(t.market_id = 'us', a.adverse, NULL) AS adverse" in code
    # the header states the same rule in words
    for words in ("RISK_OFF  trend = 'down', or vol_pct_rank >= 80, or breadth50_pct < 30, or adverse >= 3",
                  "rule_version 1, fixed on 28 Nov 2026"):
        assert words in VIEW_SQL, words


def test_the_adverse_count_is_the_projection_scorecards_own():
    card = (SQL_DIR / "121_v_projection_scorecard.sql").read_text(encoding="utf-8")
    formula = ("    IF(vix_level_state IN ('20 to 30', '30 and over'), 1, 0) + IF(vix_term_state = 'inverted', 1, 0)\n"
               "      + IF(vvix_state = 'high', 1, 0) + IF(move_state = 'high', 1, 0) + IF(credit_state = 'weakening', 1, 0)\n"
               "      + IF(rates_state = 'yields jumping', 1, 0) AS adverse")
    assert formula in card and formula in VIEW_SQL


def test_the_markets_and_their_names():
    code = _code(VIEW_SQL)
    assert "WHERE market_id IN ('us', 'sa', 'eu', 'jp', 'cn') AND close > 0" in code
    case = re.search(r"CASE REGEXP_EXTRACT\(symbol, r'\\\.\(\[A-Z\]\+\)\$'\)\n(.*?)END AS market_id", code, re.S).group(1)
    got = dict(re.findall(r"WHEN '([A-Z]+)' THEN '([a-z]+)'", case))
    assert {k: v for k, v in got.items() if v != "eu"} == {"US": "us", "SR": "sa", "T": "jp", "HK": "cn"}
    assert sorted(k for k, v in got.items() if v == "eu") == sorted(
        ["L", "PA", "DE", "MC", "MI", "AS", "BR", "LS", "OL", "ST", "CO", "HE", "SW", "VI", "IR", "AT", "WA"])
    assert tuple(rs.MARKETS) == ("us", "sa", "eu", "jp", "cn")


def test_the_insample_view_is_labelled_and_stops_before_the_forward_record():
    code = _code(INSAMPLE_SQL)
    assert code.count("r.date <= DATE '2026-11-28'") == 2                      # both rules, the same closes
    assert "'in-sample to 2026-11-28: the rule was fixed after this history, not evidence' AS reading" in code
    assert "SAFE_DIVIDE(LEAD(close, 21) OVER w, close) - 1 AS f21" in code
    assert "SQRT(COUNT(x21) / 21)" in code and "SQRT(COUNT(x5) / 5)" in code   # overlapping windows counted once
    assert "FROM `{{DS}}.v_regime_shadow` AS r" in code and "regime_shadow_log" not in code
    assert rs.FORWARD_START == dt.date(2026, 11, 29)


def test_the_logged_columns_are_the_views_less_the_order():
    assert [c for c in VIEW_COLUMNS if c != "market_order"] == sorted(rs.ROW_COLUMNS, key=VIEW_COLUMNS.index)
    assert set(rs.ROW_COLUMNS) == set(VIEW_COLUMNS) - {"market_order"}
    for c in rs.ROW_COLUMNS:
        assert re.search(r"\b%s\b" % c, VIEW_SQL), c


# ----------------------------------------------------------------------------- the SQL of the run
def test_every_query_is_one_select_the_shared_reader_accepts():
    class Session:
        def __init__(self):
            self.sent = []

        def post(self, url, timeout=None, json=None):
            self.sent.append(json["query"])
            return type("R", (), {"status_code": 200, "json": lambda self: {"jobComplete": True, "rows": []}})()

    s, ds, since = Session(), "p.d", dt.date(2026, 11, 20)
    for sql in (rs.sql_rows(ds, since), rs.sql_rows(ds, since, unfrozen=False), rs.sql_sessions(ds, since),
                rs.sql_frozen(ds), rs.sql_views(ds)):
        assert eb.bq_query(s, "p", "US", sql) == []
    assert len(s.sent) == 5


def test_the_row_query():
    sql = rs.sql_rows("p.d", dt.date(2026, 11, 11))
    cols = re.search(r"^SELECT (.*)$", sql, re.M).group(1).split(", ")
    assert cols == ["r." + c for c in rs.ROW_COLUMNS]
    assert "r.date >= DATE '2026-11-29'" in sql                              # never before the forward start
    assert "r.market_id IN ('us', 'sa', 'eu', 'jp', 'cn')" in sql
    assert ("NOT EXISTS (SELECT 1 FROM `p.d.regime_shadow_log` AS f WHERE f.market_id = r.market_id AND "
            "f.date = r.date)") in sql
    assert sql.rstrip().endswith("ORDER BY r.market_id, r.date")
    assert "NOT EXISTS" not in rs.sql_rows("p.d", dt.date(2026, 11, 11), unfrozen=False)
    assert "r.date >= DATE '2027-01-09'" in rs.sql_rows("p.d", dt.date(2027, 1, 9))


def test_sessions_and_views_queries():
    sql = rs.sql_sessions("p.d", dt.date(2026, 11, 11))
    assert "FROM `p.d.v_market_condition`" in sql and "date >= DATE '2026-11-11'" in sql and "close > 0" in sql
    sql = rs.sql_views("p.d")
    assert sql.endswith(" WHERE table_name IN ('v_market_condition', 'v_bar_jumps', 'v_bars_clean', "
                        "'v_market_drivers', 'v_regime_shadow')")
    assert rs.sql_frozen("p.d") == "SELECT market_id, date FROM `p.d.regime_shadow_log` GROUP BY market_id, date"


def test_schema():
    names = [f["name"] for f in rs.SCHEMA]
    assert len(names) == len(set(names)) and names[:2] == ["date", "market_id"]
    assert rs.ROW_COLUMNS == names[:17] and names[17:] == list(rs._STAMP_COLUMNS)
    req = {f["name"] for f in rs.SCHEMA if f["mode"] == "REQUIRED"}
    assert req == {"date", "market_id", "rule_version", "regime", "later_sessions", "frozen_at", "frozen_days_after"}
    schema, part, cluster, desc = rs.SPEC
    assert schema is rs.SCHEMA and part == "date" and cluster == ["market_id"] and "1.0.0" in desc
    assert {f["type"] for f in rs.SCHEMA} == {"DATE", "STRING", "FLOAT", "INTEGER", "TIMESTAMP"}


# ----------------------------------------------------------------------------- the plan
def plan_of(rows, sess, frozen=None):
    return {(e["market_id"], e["date"]): e for e in rs.plan(rows, rs.sessions_by_market(sess), frozen or {})}


def test_a_final_whole_close_is_frozen():
    e = plan_of([lab("us", MON)], US_SESS)[("us", MON)]
    assert e["action"] == "FREEZE" and e["why"] == "" and e["later"] == 1 and e["regime"] == "RISK_OFF"


def test_not_final_until_a_later_close_of_that_market_is_archived():
    e = plan_of([lab("us", TUE)], US_SESS)[("us", TUE)]
    assert e["action"] == "SKIP" and "no later close of us archived" in e["why"]
    e = plan_of([lab("us", MON)], closes("us", MON) + closes("sa", TUE))
    assert e[("us", MON)]["action"] == "SKIP" and "not final yet" in e[("us", MON)]["why"]


def test_before_the_start_frozen_already_and_no_label_are_skipped():
    e = plan_of([lab("sa", dt.date(2026, 11, 26)), lab("sa", SUN), lab("sa", MON, regime="")],
                closes("sa", dt.date(2026, 11, 26)) + SA_SESS, frozen={("sa", SUN): True})
    assert e[("sa", dt.date(2026, 11, 26))]["why"] == "before the forward start"
    assert e[("sa", SUN)]["why"] == "frozen already"
    assert e[("sa", MON)]["action"] == "SKIP" and "label not known" in e[("sa", MON)]["why"]
    assert plan_of([lab("xx", MON)], closes("xx", TUE))[("xx", MON)]["why"] == "not a market close of the rule"


def test_a_missing_breadth_or_adverse_count_waits_one_close_then_freezes_as_it_is():
    e = plan_of([lab("us", MON, breadth=None)], US_SESS)[("us", MON)]
    assert e["action"] == "HOLD" and e["why"] == "waiting for the breadth (one later close archived)"
    e = plan_of([lab("us", MON, breadth=None, adverse="")], US_SESS)[("us", MON)]
    assert e["why"] == "waiting for the breadth and adverse states (one later close archived)"
    e = plan_of([lab("us", MON, adverse=None)], closes("us", MON, TUE, TUE + dt.timedelta(1)))[("us", MON)]
    assert e["action"] == "FREEZE" and e["why"] == "frozen without the adverse states (2 later closes archived)"
    e = plan_of([lab("jp", MON)], closes("jp", MON, TUE))[("jp", MON)]       # the adverse count is the US's only
    assert e["action"] == "FREEZE" and e["why"] == ""


def test_a_key_seen_twice_is_frozen_once():
    out = rs.plan([lab("us", MON), lab("us", MON)], rs.sessions_by_market(US_SESS), {})
    assert [e["action"] for e in out] == ["FREEZE", "SKIP"] and out[1]["why"] == "frozen already"


def test_the_plan_runs_oldest_first():
    out = rs.plan([lab("us", MON), lab("sa", SUN), lab("sa", MON)],
                  rs.sessions_by_market(US_SESS + SA_SESS), {})
    assert [(e["market_id"], e["date"]) for e in out] == [("sa", SUN), ("sa", MON), ("us", MON)]


def test_rows_and_their_types():
    e = plan_of([lab("us", MON)], US_SESS)[("us", MON)]
    row = rs.frozen_row(e, NOW, "992", "views 1.38.0", "v_regime_shadow=abc")
    assert row["date"] == "2026-11-30" and row["market_id"] == "us" and row["close"] == 660.1
    assert row["breadth_names"] == 480 and row["adverse"] == 3 and row["rule_version"] == 1
    assert row["vol_pct_rank"] == 84.0 and row["regime"] == "RISK_OFF" and row["trend"] == "mixed"
    assert row["later_sessions"] == 1 and row["frozen_days_after"] == 2 and row["frozen_at"] == "2026-12-02T07:05:00Z"
    assert row["run_id"] == "992" and row["views_version"] == "views 1.38.0" and row["views_sha"] == "v_regime_shadow=abc"
    assert set(row) == {f["name"] for f in rs.SCHEMA}
    jp = rs.frozen_row(plan_of([lab("jp", MON)], closes("jp", MON, TUE))[("jp", MON)], NOW, "", "v", None)
    assert jp["adverse"] is None and jp["run_id"] is None and jp["views_sha"] is None


def test_views_sha_in_chain_order():
    rows = [{"table_name": "v_regime_shadow", "sha": "ABCDEF0123456789"},
            {"table_name": "v_market_condition", "sha": "0011223344556677"}]
    assert rs.views_sha(rows) == "v_market_condition=001122334455;v_regime_shadow=abcdef012345"
    assert rs.views_sha([]) is None


def test_summary_names_the_market_and_label_never_a_symbol():
    e = plan_of([lab("us", MON)], US_SESS)[("us", MON)]
    line = rs.summary(e)
    assert line.startswith("us 2026-11-30  FREEZE RISK_OFF") and "SPY" not in line
    assert "trend mixed | volatility percentile 84" in line


# ----------------------------------------------------------------------------- the run
def test_a_write_run_freezes_each_final_close(capsys):
    q = Query(rows=[lab("us", MON), lab("sa", SUN), lab("sa", MON), lab("us", TUE)], sess=US_SESS + SA_SESS,
              views=[{"table_name": "v_regime_shadow", "sha": "ab" * 32}])
    rc, bq = run(q)
    out = capsys.readouterr().out
    assert rc == 0 and bq.ensured == [("regime_shadow_log", rs.SPEC)]
    assert [(ld["day"], ld["rows"][0]["market_id"]) for ld in bq.loads] == [
        ("2026-11-29", "sa"), ("2026-11-30", "sa"), ("2026-11-30", "us")]
    assert all(ld["disposition"] == "WRITE_APPEND" and ld["schema"] is rs.SCHEMA and ld["run_id"] == "992"
               and len(ld["rows"]) == 1 for ld in bq.loads)
    assert bq.loads[0]["rows"][0]["views_sha"] == "v_regime_shadow=" + "ab" * 6
    assert "froze sa 2026-11-29: 1 row into regime_shadow_log$20261129" in out
    assert "us 2026-12-01  SKIP" in out and "SPY" not in out and "TASI" not in out


def test_a_second_run_freezes_nothing_again(capsys):
    frozen = [{"market_id": "us", "date": MON.isoformat()}]
    rc, bq = run(Query(rows=[lab("us", MON)], frozen=frozen, sess=US_SESS))
    out = capsys.readouterr().out
    assert rc == 0 and bq.loads == [] and "no close to freeze" in out


def test_the_window_is_three_weeks_and_starts_at_the_forward_start():
    q = Query(rows=[lab("us", MON)], sess=US_SESS)
    run(q, now=dt.datetime(2026, 12, 25, 6, 0, tzinfo=UTC))
    rows_sql = [s for s in q.sql if ".v_regime_shadow` AS r" in s][0]
    assert "r.date >= DATE '2026-12-04'" in rows_sql                           # 21 days back
    q = Query(rows=[lab("us", MON)], sess=US_SESS)
    run(q)
    assert "r.date >= DATE '2026-11-29'" in [s for s in q.sql if ".v_regime_shadow` AS r" in s][0]
    assert "date >= DATE '2026-11-11'" in [s for s in q.sql if ".v_market_condition`" in s][0]


def test_a_dry_run_reads_before_the_table_exists_and_writes_nothing(capsys):
    q = Query(rows=[lab("us", MON)], sess=US_SESS, table=False)
    rc, bq = run(q, dry=True)
    out = capsys.readouterr().out
    assert rc == 0 and bq.ensured == [] and bq.loads == []
    assert "dry run: 1 close(s) would be frozen, nothing written" in out
    assert "NOT EXISTS" not in [s for s in q.sql if ".v_regime_shadow` AS r" in s][-1]


def test_a_missing_view_is_an_error_even_in_a_dry_run(capsys):
    class NoView(Query):
        def __call__(self, session, project, location, sql):
            if ".v_regime_shadow` AS r" in sql:
                self.sql.append(sql)
                raise RuntimeError("BigQuery refused the query (HTTP 404): Not found: Table p:d.v_regime_shadow")
            return super().__call__(session, project, location, sql)

    q = NoView(sess=US_SESS)
    rc, bq = run(q, dry=True)
    assert rc == 2 and bq.loads == [] and "before the freeze" in capsys.readouterr().out
    assert sum(".v_regime_shadow` AS r" in s for s in q.sql) == 1            # no second try round a missing view


def test_a_write_run_never_reads_round_a_missing_table(capsys):
    rc, bq = run(Query(rows=[lab("us", MON)], sess=US_SESS, table=False))
    assert rc == 2 and bq.loads == [] and "nothing written" in capsys.readouterr().out


def test_a_failed_read_writes_nothing(capsys):
    rc, bq = run(Query(rows=[lab("us", MON)], sess=US_SESS, fail=".v_market_condition`"))
    assert rc == 2 and bq.loads == [] and "ERROR: RuntimeError before the freeze" in capsys.readouterr().out


def test_one_market_failing_never_stops_the_others(capsys):
    q = Query(rows=[lab("us", MON), lab("sa", MON)], sess=US_SESS + SA_SESS)
    rc, bq = run(q, bq=BQ(fail_market="sa"))
    out = capsys.readouterr().out
    assert rc == 1 and [ld["rows"][0]["market_id"] for ld in bq.loads] == ["us"]
    assert "ERROR: sa 2026-11-30 was not frozen (RuntimeError)" in out


def test_a_label_is_worth_more_than_its_hash(capsys):
    rc, bq = run(Query(rows=[lab("us", MON)], sess=US_SESS, views_fail=True))
    out = capsys.readouterr().out
    assert rc == 0 and len(bq.loads) == 1 and bq.loads[0]["rows"][0]["views_sha"] is None
    assert "views_sha not read (RuntimeError)" in out


def test_nothing_final(capsys):
    rc, bq = run(Query(rows=[lab("us", TUE)], sess=US_SESS))
    out = capsys.readouterr().out
    assert rc == 0 and bq.loads == [] and "nothing to freeze" in out


def test_the_switch_and_the_configuration(monkeypatch, capsys):
    for word, want in (("off", "off"), ("0", "off"), ("observe", "observe"), ("on", "observe"), ("", "observe")):
        monkeypatch.setenv("TFB_REGIME_SHADOW", word)
        assert rs.mode() == want, word
    monkeypatch.setenv("TFB_REGIME_SHADOW", "off")
    q = Query(rows=[lab("us", MON)], sess=US_SESS)
    rc, bq = run(q)
    assert rc == 0 and q.sql == [] and bq.ensured == [] and "is off" in capsys.readouterr().out
    monkeypatch.setenv("TFB_REGIME_SHADOW", "observe")
    monkeypatch.setenv("TFB_BQ_PROJECT", "bad project;")
    q = Query()
    rc, bq = run(q)
    assert rc == 2 and q.sql == [] and "missing or malformed" in capsys.readouterr().out
    monkeypatch.delenv("TFB_BQ_PROJECT")
    assert run(Query())[0] == 2


def test_main_parses_dry_run():
    q = Query(rows=[lab("us", MON)], sess=US_SESS)
    bq = BQ()
    assert rs.main(["--dry-run"], session=object(), now=NOW, query=q, ensure=bq.ensure, load=bq.load) == 0
    assert bq.loads == [] and bq.ensured == []


# ----------------------------------------------------------------------------- the source and the workflow
def test_source_is_ascii_and_versioned():
    src = Path(rs.__file__).read_text(encoding="utf-8")
    assert src.isascii() and rs.__version__ == "1.0.0" and "v1.0.0 (2026-11-28)" in src
    assert src.count("print(") == 1                                          # one log writer
    pins = Path("scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("regime_shadow", "1.0.0", "regime shadow")' in pins
    assert '("apply_bq_views", "1.38.0", "bigquery looker views")' in pins
    assert "tests/test_regime_shadow.py" in Path(".github/workflows/ci.yml").read_text(encoding="utf-8")


def test_workflow_chains_to_the_bars_and_the_index_closes_and_writes_only_when_asked():
    wf = yaml.safe_load(Path(".github/workflows/regime_shadow.yml").read_text(encoding="utf-8"))
    deep = yaml.safe_load(Path(".github/workflows/bq_symbol_deep.yml").read_text(encoding="utf-8"))
    markets = yaml.safe_load(Path(".github/workflows/bq_markets.yml").read_text(encoding="utf-8"))
    on = wf[True] if True in wf else wf["on"]
    assert wf["name"] == "🧭 Regime Shadow" and "schedule" not in on
    assert on["workflow_run"] == {"workflows": [deep["name"], markets["name"]], "types": ["completed"],
                                  "branches": ["main"]}
    assert on["workflow_dispatch"]["inputs"]["write"] == {
        "description": "Freeze the final closes' labels into BigQuery (unticked = dry run)", "type": "boolean",
        "default": False}
    assert wf["permissions"] == {"contents": "read"}
    job = wf["jobs"]["freeze"]
    assert job["concurrency"] == {"group": "tfb-regime-shadow", "cancel-in-progress": False} and "if" not in job
    step = [s for s in job["steps"] if s.get("name", "").startswith("🧭")][0]
    env = step["env"]
    assert env["TFB_REGIME_SHADOW"] == "${{ vars.TFB_REGIME_SHADOW || 'observe' }}"
    assert env["IN_WRITE"] == "${{ inputs.write }}" and env["EVENT_NAME"] == "${{ github.event_name }}"
    assert not any(k.startswith("EODHD") for k in env) and "${{" not in step["run"]
    assert '"$EVENT_NAME" == "workflow_dispatch" && "$IN_WRITE" != "true"' in step["run"]
    assert "ARGS+=(--dry-run)" in step["run"] and 'python scripts/regime_shadow.py "${ARGS[@]}"' in step["run"]
