"""scripts/archive_symbol_deep.py v1.1.0 - candles, analyst consensus and
per-symbol news into BigQuery (program register C8, Phase 3); since v1.1.0
also the bars of every symbol with an active forecast record (D11).

REAL module; the only stubs are the BigQuery session (an AuthorizedSession-
like fake answering datasets / queries / tables / resumable loads), the Yahoo
reader and the EODHD opener. No network, no google libraries. The fake set
query answers 'outcome' rows (v1.1.0) in EVERY test, so D3 / D7 / D8 / D9
also prove that the gate-off paths ignore them.

D1 gate off / configuration: nothing read when off; exit 2 without a project
   or with a malformed --extra symbol
D2 symbols: TFB -> Yahoo (.US stripped) and TFB -> EODHD (the repo's own
   suffix maps, read from core/ - golden: a map edit there fails here)
D3 the symbol sets: core = holdings, board, top-25 ROI, extra (cap); deep =
   core + top-50 ROI + Market_Leaders (cap); duplicates kept once
D4 analyst parsing: counts, provider rating range, no-coverage -> None
D5 news parsing: title required, one row per link, polarity range, newest
   first, limit
D6 budget guard: proceeds under the share, skips over it, skips when the
   usage cannot be read (fail closed); a counter dated before today is
   yesterday's (EODHD resets lazily) and counts as 0, so the 00:20 UTC run
   still loads - a missing, malformed or later date is counted (Codex P2 on
   #884 - fails on the first cut)
D7 dry run: every source read, nothing created or loaded, exit 0
D8 write: tables created with schema, partitioning and clustering; bars
   replace the whole table, analyst / news their day partition; rows as read
D9 safety: under 80 % of the deep set answered -> bars not loaded (table
   kept) and exit 1; the guard's skip makes no EODHD data call; the token
   never reaches the output, even on HTTP errors
D10 the workflow: schedule, dry-run dispatch default, read-only permissions,
   gates default on, the EODHD secret passed, the loader called by mode;
   since 1.1.0 the outcome_bars input (default false) and the
   TFB_DEEP_OUTCOME_BARS mapping (dispatch input, else the Variable, else off)
D11 outcome bars (v1.1.0, TFB_DEEP_OUTCOME_BARS default off):
   a) gate off (unset / off / 0 / no): with 'outcome' rows in the set query
      the sets, the Yahoo calls, every log line and the one load are
      byte-identical to a query without them (golden: v1.0.0 behaviour)
   b) gate on: the outcome set is read AFTER the deep set, capped, without
      futures / FX (=) or refused symbols or deep members; its rows carry
      set_reason 'outcome' and go into the ONE WRITE_TRUNCATE bars load;
      the core set, the EODHD calls and their loads are untouched; the
      dry run prints the outcome stage too
   c) golden negative: no outcome symbol answers while the deep share
      passes -> loaded (deep rows only), exit 0, "0/N"; the no-bars list is
      cut at 15
   d) the deep share under 80 % -> NOT loaded, exit 1, the outcome set not
      even read (write) - whatever the outcome symbols would answer; the
      dry run still reads it
   e) SET_SQL = the v1.0.0 text + the one 'outcome' union; build_sets()
      ignores 'outcome'; build_outcome_set() order, cap, skips
   f) the time budget: the outcome stage stops asking after OUTCOME_TIME_S,
      the deep rows and the answered outcome rows still load, exit 0
   g) --only analyst / news: no outcome set, the sets line as in v1.0.0
"""
from __future__ import annotations

import ast
import datetime as dt
import io
import json
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import archive_symbol_deep as asd  # noqa: E402

NOW = dt.datetime(2026, 11, 1, 0, 20, 5, tzinfo=dt.timezone.utc)
PROJECT, DATASET = "proj-x", "tfb_archive"
TOKEN = "SECRET123TOKEN"
DEEP_ROWS = ([("5023.SR", "hold"), ("FRST.US", "hold"), ("DDI.US", "board"), ("FRST.US", "board")]
             + [(f"R{i}.US", "roi25") for i in range(3)] + [("R9.US", "roi50"), ("2222.SR", "leaders")])
# v1.1.0: the symbols with an active forecast record, as the set query's
# 'outcome' union answers them - two deep members, a futures contract, an FX
# pair, a refused symbol and a repeat among them
OUTCOME_ROWS = [("FRST.US", "outcome"), ("AAPL.US", "outcome"), ("7203.T", "outcome"), ("ALI=F", "outcome"),
                ("AUDCHF=X", "outcome"), ("0700.HK", "outcome"), ("BP.L", "outcome"), ("bad sym;", "outcome"),
                ("7203.T", "outcome"), ("2222.SR", "outcome")]
SET_ROWS = DEEP_ROWS + OUTCOME_ROWS
DEEP_YAHOO = ["5023.SR", "FRST", "DDI", "R0", "R1", "R2", "R9", "2222.SR"]
OUTCOME_TFB = ["AAPL.US", "7203.T", "0700.HK", "BP.L"]      # without --extra AAPL.US
OUTCOME_YAHOO = ["AAPL", "7203.T", "0700.HK", "BP.L"]

SET_SQL_V100 = """
WITH hold AS (
  SELECT DISTINCT symbol FROM `{ds}.v_my_portfolio`
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `{ds}.v_my_portfolio`)
),
board AS (
  SELECT DISTINCT symbol FROM `{ds}.v_top10_board`
  WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
),
roi AS (
  SELECT symbol, ROW_NUMBER() OVER (ORDER BY expected_roi_12m_pct DESC, symbol) AS rk
  FROM `{ds}.v_universe_latest`
  WHERE meets_floors AND expected_roi_12m_pct IS NOT NULL
)
SELECT symbol, 'hold' AS why FROM hold
UNION ALL SELECT symbol, 'board' FROM board
UNION ALL SELECT symbol, 'roi25' FROM roi WHERE rk <= 25
UNION ALL SELECT symbol, 'roi50' FROM roi WHERE rk > 25 AND rk <= 50
UNION ALL SELECT symbol, 'leaders' FROM `{ds}.v_universe_latest` WHERE source_page = 'Market_Leaders'
"""


class Resp:
    def __init__(self, status, body=None, headers=None):
        self.status_code = status
        self._body = body if body is not None else {}
        self.headers = headers or {}

    def json(self):
        return self._body


class FakeBQ:
    def __init__(self, tables=(), fail_load=None, rows=None):
        self.tables = set(tables)
        self.created = {}
        self.loads = []
        self.queries = []
        self.fail_load = fail_load
        self.rows = list(SET_ROWS if rows is None else rows)
        self._pending = {}

    def get(self, url, timeout=None, **_):
        if url.endswith(f"/datasets/{DATASET}"):
            return Resp(200, {"id": DATASET})
        if "/tables/" in url:
            name = url.rsplit("/", 1)[1]
            if name in self.tables:
                schema, part, cluster, _ = asd.TABLES[name]
                body = {"schema": {"fields": schema}}
                if part:
                    body["timePartitioning"] = {"type": "DAY", "field": part}
                return Resp(200, body)
            return Resp(404, {"error": {"message": "not found"}})
        if "/jobs/" in url:
            return Resp(200, {"status": {"state": "DONE"}})
        raise AssertionError(url)

    def post(self, url, json=None, timeout=None, headers=None, **_):  # noqa: A002
        if url.endswith("/queries"):
            self.queries.append(json["query"])
            return Resp(200, {"schema": {"fields": [{"name": "symbol"}, {"name": "why"}]},
                              "rows": [{"f": [{"v": s}, {"v": w}]} for s, w in self.rows]})
        if url.endswith("/tables"):
            name = json["tableReference"]["tableId"]
            self.created[name] = json
            self.tables.add(name)
            return Resp(200, json)
        if "uploadType=resumable" in url:
            key = f"https://upload/{len(self._pending)}"
            self._pending[key] = json
            return Resp(200, {}, {"Location": key})
        raise AssertionError(url)

    def put(self, url, data=None, timeout=None, headers=None, **_):
        cfg = self._pending.pop(url)
        load = cfg["configuration"]["load"]
        rows = [json.loads(line) for line in data.decode("utf-8").splitlines() if line]
        self.loads.append({"table": load["destinationTable"]["tableId"], "write": load["writeDisposition"],
                           "create": load["createDisposition"], "rows": rows})
        if self.fail_load and self.fail_load in load["destinationTable"]["tableId"]:
            return Resp(200, {"status": {"state": "DONE", "errorResult": {"message": "boom"}}})
        return Resp(200, {"status": {"state": "DONE"}, "statistics": {"load": {"outputRows": str(len(rows))}}})


class _Body(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class HTTPErr(Exception):
    def __init__(self, code):
        super().__init__(f"HTTP Error {code}: see https://eodhd.com/api/x?api_token={TOKEN}")
        self.code = code


def make_opener(used=1000, limit=100000, user_fail=False, data_fail=False, calls=None, dated="2026-11-01"):
    def opener(req, timeout):
        url = req.full_url
        if calls is not None:
            calls.append(url)
        path = urlparse(url).path
        q = parse_qs(urlparse(url).query)
        assert q["api_token"] == [TOKEN] and q["fmt"] == ["json"]
        if path.endswith("/api/user"):
            if user_fail:
                raise HTTPErr(500)
            user = {"apiRequests": used, "dailyRateLimit": limit}
            if dated is not None:
                user["apiRequestsDate"] = dated
            return _Body(json.dumps(user).encode())
        if data_fail:
            raise HTTPErr(403)
        if "/api/fundamentals/" in path:
            assert q["filter"] == ["AnalystRatings"]
            sym = path.rsplit("/", 1)[1]
            if sym == "5023.SR":
                return _Body(b"[]")
            return _Body(json.dumps({"Rating": 4.2, "TargetPrice": 30.5, "StrongBuy": 3, "Buy": 2,
                                     "Hold": 1, "Sell": 0, "StrongSell": 0}).encode())
        if path.endswith("/api/news"):
            sym = q["s"][0]
            return _Body(json.dumps([
                {"date": "2026-10-30T14:05:00+00:00", "title": f"{sym} beats", "link": f"https://a.example/{sym}/1",
                 "sentiment": {"polarity": 0.6, "neg": 0.1, "neu": 0.5, "pos": 0.4}, "tags": ["earnings"]},
                {"date": "2026-10-29T09:00:00+00:00", "title": f"{sym} beats", "link": f"https://a.example/{sym}/1"},
            ]).encode())
        raise AssertionError(url)
    return opener


def bars_fetch(answered=None, calls=None):
    def fetch(ysym, p1, p2, now_ts):
        if calls is not None:
            calls.append(ysym)
        if answered is not None and ysym not in answered:
            return []
        return [{"date": "2026-10-29", "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "adj_close": 1.5,
                 "volume": 100}, {"date": "2026-10-30", "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0,
                                  "adj_close": None, "volume": None, "source": "yahoo_chart_v8_1h"}]
    return fetch


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("TFB_BQ_DEEP", "on")
    monkeypatch.setenv("TFB_BQ_PROJECT", PROJECT)
    monkeypatch.setenv("TFB_BQ_DATASET", DATASET)
    monkeypatch.setenv("EODHD_API_KEY", TOKEN)
    monkeypatch.delenv("TFB_DEEP_EODHD", raising=False)
    monkeypatch.delenv("TFB_DEEP_EODHD_MAX_SHARE", raising=False)
    monkeypatch.delenv("TFB_DEEP_OUTCOME_BARS", raising=False)
    monkeypatch.setenv("GITHUB_RUN_ID", "777")


def _main(args, bq, **kw):
    return asd.main(args, session=bq, now=NOW, fetch=kw.get("fetch", bars_fetch()),
                    opener=kw.get("opener", make_opener()), sleep=kw.get("sleep", lambda s: None),
                    clock=kw.get("clock", lambda: 0.0))


# --------------------------------------------------------------------- D1
def test_d1_gate_and_config(monkeypatch, capsys):
    monkeypatch.delenv("TFB_BQ_DEEP", raising=False)
    bq = FakeBQ()
    assert asd.main([], session=bq) == 0 and not bq.queries and "off" in capsys.readouterr().err
    monkeypatch.setenv("TFB_BQ_DEEP", "on")
    monkeypatch.delenv("TFB_BQ_PROJECT", raising=False)
    assert asd.main([], session=bq) == 2
    monkeypatch.setenv("TFB_BQ_PROJECT", PROJECT)
    assert asd.main(["--extra", "AAPL.US;DROP"], session=bq) == 2
    assert asd.clean_symbols(" 2222.sr, AAPL.US ,,2222.SR") == ["2222.SR", "AAPL.US"]


# --------------------------------------------------------------------- D2
def _dict_literal(path, name):
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(getattr(t, "id", None) == name for t in targets):
                return ast.literal_eval(node.value)
    raise AssertionError(name)


def test_d2_symbol_maps():
    assert [asd.yahoo_symbol(s) for s in ("AAPL.US", "BRK-B.US", "2222.SR", "7203.T", "bp.l")] == \
        ["AAPL", "BRK-B", "2222.SR", "7203.T", "BP.L"]
    assert [asd.eodhd_symbol(s) for s in ("AAPL.US", "2222.SR", "BP.L", "SAP.DE", "005930.KS",
                                          "RELIANCE.NS", "BHP.AX", "AAPL")] == \
        ["AAPL.US", "2222.SR", "BP.LSE", "SAP.XETRA", "005930.KO", "RELIANCE.NSE", "BHP.AU", "AAPL.US"]
    norm = _dict_literal("core/symbols/normalize.py", "_YAHOO_TO_EODHD_SUFFIX")
    canon = {k.lstrip("."): v.lstrip(".") for k, v in
             _dict_literal("core/providers/eodhd_provider.py", "_EODHD_SUFFIX_CANONICAL").items()}
    assert asd.EODHD_SUFFIX == {**norm, **canon}, (asd.EODHD_SUFFIX, norm, canon)


# --------------------------------------------------------------------- D3
def test_d3_sets():
    rows = [{"symbol": s, "why": w} for s, w in SET_ROWS]
    core, deep = asd.build_sets(rows, ["AAPL.US", "FRST.US"])
    assert core == ["5023.SR", "FRST.US", "DDI.US", "R0.US", "R1.US", "R2.US", "AAPL.US"]
    assert list(deep) == core + ["R9.US", "2222.SR"]
    assert deep["FRST.US"] == "hold" and deep["AAPL.US"] == "extra" and deep["2222.SR"] == "leaders"
    core2, deep2 = asd.build_sets(rows, [], core_cap=2, deep_cap=3)
    assert core2 == ["5023.SR", "FRST.US"] and list(deep2) == ["5023.SR", "FRST.US", "DDI.US"]
    assert asd.build_sets([{"symbol": "bad sym;", "why": "hold"}], []) == ([], {})


# --------------------------------------------------------------------- D4
def test_d4_analyst():
    row = asd.parse_analyst({"Rating": "4.4", "TargetPrice": 250, "StrongBuy": 20, "Buy": 6, "Hold": 10,
                             "Sell": 1, "StrongSell": 2})
    assert row == {"rating": 4.4, "target_price": 250.0, "strong_buy": 20, "buy": 6, "hold": 10, "sell": 1,
                   "strong_sell": 2, "analysts": 39}
    assert asd.parse_analyst({"Rating": 7, "TargetPrice": 0, "Buy": 2})["rating"] is None
    for empty in ({}, "NA", [], None, {"Rating": None, "TargetPrice": None, "Buy": 0}):
        assert asd.parse_analyst(empty) is None, empty


# --------------------------------------------------------------------- D5
def test_d5_news():
    items = [{"date": "2026-10-29T09:00:00+00:00", "title": "  Old   one ", "link": "https://b.example/x",
              "sentiment": {"polarity": -0.4}},
             {"date": "2026-10-30T14:05:00Z", "title": "New", "link": "https://a.example/y?q=1",
              "sentiment": {"polarity": 3}, "tags": ["t1", "t2"]},
             {"date": "bad", "title": "New again", "link": "https://a.example/y?q=1"},
             {"title": ""}, "junk"]
    rows = asd.parse_news(items)
    assert [r["title"] for r in rows] == ["New", "Old one"]
    assert rows[0]["published_at"] == "2026-10-30T14:05:00Z" and rows[0]["polarity"] is None
    assert rows[0]["domain"] == "a.example" and rows[0]["tags"] == "t1, t2"
    assert rows[1]["polarity"] == -0.4
    assert len(asd.parse_news([{"title": f"t{i}", "link": f"https://x/{i}"} for i in range(50)], limit=5)) == 5
    assert asd.parse_news({"error": "x"}) == []


# --------------------------------------------------------------------- D6
def test_d6_budget_guard():
    day = "2026-11-01"
    ok, why = asd.budget_check(TOKEN, 1500, 0.6, make_opener(used=10000), today=day)
    assert ok and "11500" not in why and "10000 + 1500" in why
    ok, why = asd.budget_check(TOKEN, 1500, 0.6, make_opener(used=59000), today=day)
    assert not ok and "60000" in why
    ok, why = asd.budget_check(TOKEN, 10, 0.6, make_opener(user_fail=True), today=day)
    assert not ok and "unreadable" in why and TOKEN not in why
    ok, why = asd.budget_check(TOKEN, 10, 0.6, make_opener(limit=0), today=day)
    assert not ok


def test_d6_budget_counter_date():
    """Codex P2 on #884: EODHD resets the counter lazily, so just after
    midnight /api/user still shows yesterday's calls - they are not today's."""
    day = "2026-11-01"
    ok, why = asd.budget_check(TOKEN, 1800, 0.6, make_opener(used=99000, dated="2026-10-31"), today=day)
    assert ok and "0 + 1800 planned <= 60000" in why and "counter dated 2026-10-31" in why
    # today's counter, a missing, malformed or later date: counted (fail closed)
    for dated in ("2026-11-01", None, "", "10/31/2026", "2026-1-5", "2026-11-02"):
        ok, why = asd.budget_check(TOKEN, 1800, 0.6, make_opener(used=99000, dated=dated), today=day)
        assert not ok and "99000 + 1800 planned > 60000" in why, (dated, why)
    ok, _ = asd.budget_check(TOKEN, 1800, 0.6, make_opener(used=99000, dated="2026-10-31"), today="")
    assert not ok


def test_d6_after_midnight_run(env, capsys):
    """The scheduled 00:20 UTC run with yesterday's busy counter still loads."""
    bq = FakeBQ()
    calls = []
    assert _main(["--write"], bq, opener=make_opener(used=99000, dated="2026-10-31", calls=calls)) == 0
    assert {ld["table"] for ld in bq.loads} == {"symbol_bars_daily", "symbol_analyst_daily$20261101",
                                               "symbol_news$20261101"}
    assert sum("/fundamentals/" in c for c in calls) == 6
    assert "counter dated 2026-10-31" in capsys.readouterr().err


# --------------------------------------------------------------------- D7
def test_d7_dry_run(env, capsys):
    bq = FakeBQ()
    calls = []
    assert _main([], bq, opener=make_opener(calls=calls)) == 0
    assert not bq.created and not bq.loads and len(bq.queries) == 1
    assert f"`{PROJECT}.{DATASET}.v_universe_latest`" in bq.queries[0]
    err = capsys.readouterr().err
    assert "core 6" in err and "deep 8" in err and "bars: 8/8 symbols, 16 rows | dry run" in err
    assert "analyst: 5 rows for 6 symbols | dry run" in err and "news: 6 rows for 6 symbols | dry run" in err
    assert sum("/api/user" in c for c in calls) == 1 and sum("/fundamentals/" in c for c in calls) == 6


# --------------------------------------------------------------------- D8
def test_d8_write(env, capsys):
    bq = FakeBQ()
    assert _main(["--write", "--extra", "AAPL.US"], bq) == 0
    assert set(bq.created) == {asd.TABLE_BARS, asd.TABLE_ANALYST, asd.TABLE_NEWS}
    bars_t = bq.created[asd.TABLE_BARS]
    assert "timePartitioning" not in bars_t and bars_t["clustering"] == {"fields": ["symbol"]}
    assert bq.created[asd.TABLE_NEWS]["timePartitioning"] == {"type": "DAY", "field": "snapshot_date"}
    by = {ld["table"]: ld for ld in bq.loads}
    assert set(by) == {"symbol_bars_daily", "symbol_analyst_daily$20261101", "symbol_news$20261101"}
    assert all(ld["write"] == "WRITE_TRUNCATE" and ld["create"] == "CREATE_NEVER" for ld in bq.loads)
    b = by["symbol_bars_daily"]["rows"]
    assert len(b) == 18 and {r["symbol"] for r in b} >= {"FRST.US", "AAPL.US", "2222.SR"}
    frst = [r for r in b if r["symbol"] == "FRST.US"]
    assert frst[0]["yahoo_symbol"] == "FRST" and frst[0]["set_reason"] == "hold" and frst[1]["source"] == "yahoo_chart_v8_1h"
    a = by["symbol_analyst_daily$20261101"]["rows"]
    assert {r["symbol"] for r in a} == {"FRST.US", "DDI.US", "R0.US", "R1.US", "R2.US", "AAPL.US"}
    assert a[0]["snapshot_date"] == "2026-11-01" and a[0]["analysts"] == 6 and a[0]["run_id"] == "777"
    n = by["symbol_news$20261101"]["rows"]
    assert len(n) == 7 and n[0]["polarity"] == 0.6 and n[0]["published_at"] == "2026-10-30T14:05:00Z"
    names = {f["name"] for f in asd.SCHEMA_NEWS}
    assert all(set(r) <= names for r in n) and all(set(r) <= {f["name"] for f in asd.SCHEMA_BARS} for r in b)


# --------------------------------------------------------------------- D9
def test_d9_bars_share_and_load_failure(env, capsys):
    bq = FakeBQ()
    assert _main(["--write", "--only", "bars"], bq, fetch=bars_fetch(answered={"FRST", "DDI"})) == 1
    assert not bq.loads and "NOT loaded" in capsys.readouterr().err
    bq = FakeBQ(fail_load="symbol_news")
    assert _main(["--write"], bq) == 1


def test_d9_guard_skip_and_token(env, capsys, monkeypatch):
    bq = FakeBQ()
    calls = []
    assert _main(["--write"], bq, opener=make_opener(used=99000, calls=calls)) == 0
    assert [c for c in calls if "/api/user" not in c] == []
    assert {ld["table"] for ld in bq.loads} == {"symbol_bars_daily"}
    assert "skipped by the budget guard" in capsys.readouterr().err
    bq = FakeBQ()
    assert _main(["--write", "--only", "news"], bq, opener=make_opener(data_fail=True)) == 1
    out = capsys.readouterr()
    assert TOKEN not in out.err and TOKEN not in out.out and "HTTP 403" in out.err
    monkeypatch.setenv("TFB_DEEP_EODHD", "off")
    calls = []
    assert _main(["--write"], FakeBQ(), opener=make_opener(calls=calls)) == 0 and calls == []


# --------------------------------------------------------------------- D10
def test_d10_workflow():
    import yaml
    text = (ROOT / ".github" / "workflows" / "bq_symbol_deep.yml").read_text(encoding="utf-8")
    wf = yaml.safe_load(text)
    on = wf.get(True) or wf.get("on")
    assert on["schedule"] == [{"cron": "20 0 * * *"}]
    assert on["workflow_dispatch"]["inputs"]["write"]["default"] is False
    assert wf["permissions"] == {"contents": "read"}
    step = [s for s in wf["jobs"]["deep"]["steps"] if s.get("name", "").endswith("Run the loader")][0]
    env = step["env"]
    assert env["TFB_BQ_DEEP"] == "${{ vars.TFB_BQ_DEEP || 'on' }}"
    assert env["TFB_DEEP_EODHD"] == "${{ vars.TFB_DEEP_EODHD || 'on' }}"
    assert env["EODHD_API_KEY"] == "${{ secrets.EODHD_API_KEY }}"
    assert 'MODE="--dry-run"' in step["run"] and "scripts/archive_symbol_deep.py $MODE" in step["run"]
    # 1.1.0: the outcome set is opt-in - the dispatch input for one run, the
    # repository Variable for the schedule, off otherwise
    ob = on["workflow_dispatch"]["inputs"]["outcome_bars"]
    assert ob["type"] == "boolean" and ob["default"] is False and "TFB_DEEP_OUTCOME_BARS" in ob["description"]
    assert env["TFB_DEEP_OUTCOME_BARS"] == ("${{ github.event_name == 'workflow_dispatch' && "
                                            "github.event.inputs.outcome_bars == 'true' && 'on' || "
                                            "vars.TFB_DEEP_OUTCOME_BARS || 'off' }}")
    assert wf["jobs"]["deep"]["timeout-minutes"] == 45
    assert asd.__version__ == "1.1.0"


# --------------------------------------------------------------------- D11
def _run_capture(args, bq, capsys, **kw):
    rc = _main(args, bq, **kw)
    out = capsys.readouterr()
    return rc, out.err, out.out


def test_d11a_gate_off_is_v100(env, capsys, monkeypatch):
    """Gate unset / off / 0 / no, 'outcome' rows in the set query: the Yahoo
    calls, every log line, the created tables and the loads equal a run whose
    query has no such rows (the v1.0.0 behaviour)."""
    runs = []
    for gate, rows in ((None, DEEP_ROWS), (None, SET_ROWS), ("off", SET_ROWS), ("0", SET_ROWS), ("no", SET_ROWS)):
        if gate is None:
            monkeypatch.delenv("TFB_DEEP_OUTCOME_BARS", raising=False)
        else:
            monkeypatch.setenv("TFB_DEEP_OUTCOME_BARS", gate)
        bq, calls = FakeBQ(rows=rows), []
        rc, err, out = _run_capture(["--write", "--extra", "AAPL.US"], bq, capsys, fetch=bars_fetch(calls=calls))
        runs.append((rc, err, out, calls, sorted(bq.created), bq.loads))
    assert all(r == runs[0] for r in runs[1:]), [r[1] for r in runs]
    rc, err, out, calls, created, loads = runs[0]
    assert rc == 0 and "outcome" not in err and "outcome" not in out
    assert calls == ["5023.SR", "FRST", "DDI", "R0", "R1", "R2", "AAPL", "R9", "2222.SR"]
    assert "sets: core 7 (holdings, board, top-25 ROI, extra), deep 9\n" in err
    assert "bars: 9/9 symbols, 18 rows | loaded 18\n" in err
    bars = [ld for ld in loads if ld["table"] == asd.TABLE_BARS]
    assert len(bars) == 1 and len(bars[0]["rows"]) == 18
    assert {r["set_reason"] for r in bars[0]["rows"]} == {"hold", "board", "roi25", "extra", "roi50", "leaders"}
    # the dry run too
    bq = FakeBQ()
    rc, err, _ = _run_capture([], bq, capsys)
    assert rc == 0 and "outcome" not in err and not bq.loads and "bars: 8/8 symbols, 16 rows | dry run" in err


def test_d11b_gate_on_after_deep_one_load(env, capsys, monkeypatch):
    monkeypatch.setenv("TFB_DEEP_OUTCOME_BARS", "on")
    bq, calls, eod = FakeBQ(), [], []
    rc, err, _ = _run_capture(["--write", "--extra", "AAPL.US"], bq, capsys,
                              fetch=bars_fetch(calls=calls), opener=make_opener(calls=eod))
    assert rc == 0
    assert "sets: core 7 (holdings, board, top-25 ROI, extra), deep 9, outcome 3 " \
           "(active forecast records; bars only, best effort)\n" in err
    assert "outcome bars: 3/3 symbols, 6 rows | in the bars load\n" in err
    assert "bars: 9/9 symbols, 18 rows | loaded 24 (deep + outcome rows)\n" in err
    # the deep set first, then the outcome set; AAPL.US (extra) is deep now;
    # ALI=F, AUDCHF=X and the refused symbol are never asked
    assert calls == ["5023.SR", "FRST", "DDI", "R0", "R1", "R2", "AAPL", "R9", "2222.SR", "7203.T", "0700.HK", "BP.L"]
    bars = [ld for ld in bq.loads if ld["table"] == asd.TABLE_BARS]
    assert len(bars) == 1 and bars[0]["write"] == "WRITE_TRUNCATE" and len(bq.loads) == 3
    rows = bars[0]["rows"]
    assert len(rows) == 24 and {r["set_reason"] for r in rows[:18]} == {"hold", "board", "roi25", "extra", "roi50", "leaders"}
    assert [(r["symbol"], r["yahoo_symbol"], r["set_reason"]) for r in rows[18::2]] == \
        [("7203.T", "7203.T", "outcome"), ("0700.HK", "0700.HK", "outcome"), ("BP.L", "BP.L", "outcome")]
    assert all(set(r) <= {f["name"] for f in asd.SCHEMA_BARS} and r["run_id"] == "777" for r in rows[18:])
    # the core set is untouched: the EODHD calls and the analyst / news rows are D8's
    assert sum("/fundamentals/" in c for c in eod) == 7 and not any(s in c for c in eod for s in ("7203", "0700", "BP."))
    a = [ld for ld in bq.loads if ld["table"].startswith(asd.TABLE_ANALYST)][0]["rows"]
    assert {r["symbol"] for r in a} == {"FRST.US", "DDI.US", "R0.US", "R1.US", "R2.US", "AAPL.US"}
    # the dry run reads and prints the outcome stage, loads nothing
    bq, calls = FakeBQ(), []
    rc, err, _ = _run_capture([], bq, capsys, fetch=bars_fetch(calls=calls))
    assert rc == 0 and not bq.loads and not bq.created
    assert "deep 8, outcome 4 (" in err and "outcome bars: 4/4 symbols, 8 rows | dry run\n" in err
    assert "bars: 8/8 symbols, 16 rows | dry run\n" in err and calls == DEEP_YAHOO + OUTCOME_YAHOO


def test_d11c_golden_no_outcome_answers(env, capsys, monkeypatch):
    monkeypatch.setenv("TFB_DEEP_OUTCOME_BARS", "on")
    bq = FakeBQ()
    rc, err, _ = _run_capture(["--write", "--only", "bars"], bq, capsys, fetch=bars_fetch(answered=set(DEEP_YAHOO)))
    assert rc == 0
    assert "outcome bars: 0/4 symbols, 0 rows; no bars: AAPL.US, 7203.T, 0700.HK, BP.L | in the bars load\n" in err
    assert "bars: 8/8 symbols, 16 rows | loaded 16 (deep + outcome rows)\n" in err
    assert len(bq.loads) == 1 and len(bq.loads[0]["rows"]) == 16
    assert not any(r["set_reason"] == "outcome" for r in bq.loads[0]["rows"])
    # the no-bars list is cut at 15, as the deep line's
    bq = FakeBQ(rows=DEEP_ROWS + [(f"O{i}.T", "outcome") for i in range(20)])
    rc, err, _ = _run_capture(["--write", "--only", "bars"], bq, capsys, fetch=bars_fetch(answered=set(DEEP_YAHOO)))
    assert rc == 0 and len(bq.loads) == 1
    assert "outcome bars: 0/20 symbols, 0 rows; no bars: " + ", ".join(f"O{i}.T" for i in range(15)) + " ... | in the bars load\n" in err


def test_d11d_deep_share_rules_alone(env, capsys, monkeypatch):
    """6 of 8 deep symbols (75 %) and every outcome symbol answered: a share
    over deep + outcome would pass (10/12); the rule is the deep set's."""
    monkeypatch.setenv("TFB_DEEP_OUTCOME_BARS", "on")
    answered = set(DEEP_YAHOO) - {"R9", "2222.SR"} | set(OUTCOME_YAHOO)
    bq, calls = FakeBQ(), []
    rc, err, _ = _run_capture(["--write", "--only", "bars"], bq, capsys, fetch=bars_fetch(answered=answered, calls=calls))
    assert rc == 1 and not bq.loads and not bq.created
    assert "outcome bars: not read, the deep set is under 80% (nothing loaded)\n" in err
    assert "bars: 6/8 symbols, 12 rows; no bars: R9.US, 2222.SR | NOT loaded: under 80% answered, yesterday's table kept\n" in err
    assert calls == DEEP_YAHOO        # the outcome symbols were never asked
    # the dry run still reads the outcome set and reports both
    bq, calls = FakeBQ(), []
    rc, err, _ = _run_capture(["--only", "bars"], bq, capsys, fetch=bars_fetch(answered=answered, calls=calls))
    assert rc == 0 and not bq.loads and calls == DEEP_YAHOO + OUTCOME_YAHOO
    assert "outcome bars: 4/4 symbols, 8 rows | dry run\n" in err
    assert "bars: 6/8 symbols, 12 rows; no bars: R9.US, 2222.SR | dry run\n" in err


def test_d11e_set_sql_and_builders():
    assert asd.SET_SQL == SET_SQL_V100 + \
        "UNION ALL SELECT DISTINCT symbol, 'outcome' FROM `{ds}.v_performance_log` WHERE status = 'active'\n"
    rows = [{"symbol": s, "why": w} for s, w in SET_ROWS]
    base = [{"symbol": s, "why": w} for s, w in DEEP_ROWS]
    for extra in ([], ["AAPL.US", "FRST.US"]):
        assert asd.build_sets(rows, extra) == asd.build_sets(base, extra)
    core, deep = asd.build_sets(rows, [])
    assert "7203.T" not in deep and "AAPL.US" not in core
    out = asd.build_outcome_set(rows, deep)
    assert out == {s: "outcome" for s in OUTCOME_TFB} and list(out) == OUTCOME_TFB
    assert asd.build_outcome_set(rows, deep, cap=2) == {"AAPL.US": "outcome", "7203.T": "outcome"}
    _, deep_x = asd.build_sets(rows, ["AAPL.US"])
    assert list(asd.build_outcome_set(rows, deep_x)) == ["7203.T", "0700.HK", "BP.L"]
    assert asd.build_outcome_set(base, deep) == {} and asd.build_outcome_set([], {}) == {}
    assert asd.build_outcome_set([{"symbol": " bp.l ", "why": " outcome "}, {"symbol": "ALI=F", "why": "outcome"},
                                  {"symbol": "", "why": "outcome"}, {"symbol": None, "why": "outcome"}], {}) == \
        {"BP.L": "outcome"}
    assert asd.OUTCOME_CAP == 1600 and asd.OUTCOME_REASON == "outcome"


def test_d11e_gate_parsing(monkeypatch):
    for v in ("on", "1", "true", "yes", " ON ", "True"):
        monkeypatch.setenv("TFB_DEEP_OUTCOME_BARS", v)
        assert asd.outcome_bars_enabled(), v
    for v in ("", "off", "0", "false", "no", "maybe"):
        monkeypatch.setenv("TFB_DEEP_OUTCOME_BARS", v)
        assert not asd.outcome_bars_enabled(), v
    monkeypatch.delenv("TFB_DEEP_OUTCOME_BARS", raising=False)
    assert not asd.outcome_bars_enabled()


def test_d11f_time_budget(env, capsys, monkeypatch):
    """Every symbol costs 600 s of the fake clock (the sleep after it); the
    outcome stage gets OUTCOME_TIME_S = 1500 s from its own start, so 3 of
    the 4 outcome symbols are asked; the deep set is never cut."""
    monkeypatch.setenv("TFB_DEEP_OUTCOME_BARS", "on")
    t = [0.0]

    def sleep(_s):
        t[0] += 600.0

    bq, calls = FakeBQ(), []
    rc, err, _ = _run_capture(["--write", "--only", "bars"], bq, capsys, fetch=bars_fetch(calls=calls),
                              sleep=sleep, clock=lambda: t[0])
    assert rc == 0 and calls == DEEP_YAHOO + OUTCOME_YAHOO[:3]
    assert "outcome bars: 3/4 symbols, 6 rows; 1 not asked (time budget 1500s reached) | in the bars load\n" in err
    assert "bars: 8/8 symbols, 16 rows | loaded 22 (deep + outcome rows)\n" in err
    assert len(bq.loads) == 1 and len(bq.loads[0]["rows"]) == 22
    assert asd.OUTCOME_TIME_S == 1500.0
    # the budget is the function's own: a direct call with none left asks nothing
    out = {s: "outcome" for s in OUTCOME_TFB}
    rows, missing, asked = asd.collect_outcome_bars(out, NOW, "2026-11-01T00:20:05.000000Z", "777", bars_fetch(),
                                                    lambda s: None, budget_s=-1.0, clock=lambda: 0.0)
    assert (rows, missing, asked) == ([], [], 0)


def test_d11g_only_eodhd_stages_ignore_the_gate(env, capsys, monkeypatch):
    monkeypatch.setenv("TFB_DEEP_OUTCOME_BARS", "on")
    bq, calls = FakeBQ(), []
    rc, err, _ = _run_capture(["--write", "--only", "analyst"], bq, capsys, fetch=bars_fetch(calls=calls))
    assert rc == 0 and calls == [] and "outcome" not in err
    assert "sets: core 6 (holdings, board, top-25 ROI, extra), deep 8\n" in err
    assert [ld["table"] for ld in bq.loads] == ["symbol_analyst_daily$20261101"]
