"""scripts/archive_symbol_deep.py v1.0.0 - candles, analyst consensus and
per-symbol news into BigQuery (program register C8, Phase 3).

REAL module; the only stubs are the BigQuery session (an AuthorizedSession-
like fake answering datasets / queries / tables / resumable loads), the Yahoo
reader and the EODHD opener. No network, no google libraries.

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
   gates default on, the EODHD secret passed, the loader called by mode
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
SET_ROWS = ([("5023.SR", "hold"), ("FRST.US", "hold"), ("DDI.US", "board"), ("FRST.US", "board")]
            + [(f"R{i}.US", "roi25") for i in range(3)] + [("R9.US", "roi50"), ("2222.SR", "leaders")])


class Resp:
    def __init__(self, status, body=None, headers=None):
        self.status_code = status
        self._body = body if body is not None else {}
        self.headers = headers or {}

    def json(self):
        return self._body


class FakeBQ:
    def __init__(self, tables=(), fail_load=None):
        self.tables = set(tables)
        self.created = {}
        self.loads = []
        self.queries = []
        self.fail_load = fail_load
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
                              "rows": [{"f": [{"v": s}, {"v": w}]} for s, w in SET_ROWS]})
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


def bars_fetch(answered=None):
    def fetch(ysym, p1, p2, now_ts):
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
    monkeypatch.setenv("GITHUB_RUN_ID", "777")


def _main(args, bq, **kw):
    return asd.main(args, session=bq, now=NOW, fetch=kw.get("fetch", bars_fetch()),
                    opener=kw.get("opener", make_opener()), sleep=lambda s: None)


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
    assert asd.__version__ == "1.0.0"
