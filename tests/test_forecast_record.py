#!/usr/bin/env python3
"""tests/test_forecast_record.py - the honest forecast (28 Nov 2026, observe).

  V1-V4  views 1.39.0's v_forecast_record: installed last, reads the outcome engine and the index archive only, the
         population of the scored INVESTABLE buy calls, the thirds cut once (scored by 27 Nov 2026), the forward
         period from 29 Nov 2026, the index from the last close BEFORE the recording day, the fixed reading
  F1-F6  scripts/forecast_record.py 1.0.0: one SELECT, the tab (header in its first eight rows, the markets with the
         most calls first), off / dry run / empty record / failed query write nothing, the run log gives counts only
  H1-H7  scripts/daily_plan.py 1.12.0 (TFB_PLAN_HONEST): the tab read back, the band of a promise, the honest read and
         its label, every judged name labelled (picks and waiting names), no pick changed, the run log, one read per
         cut, a tab that cannot be read
  W1-W3  the workflows, the deployment pins and the CI lane

Run: pytest -q tests/test_forecast_record.py
"""
from __future__ import annotations

import datetime as dt
import re
import sys
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

import apply_bq_views as bv  # noqa: E402
from scripts import daily_plan as dp  # noqa: E402
from scripts import forecast_record as fr  # noqa: E402
from tests.test_daily_plan import BLOCK, CAL, LOTS, NOW, QUAL, STRESS, Book, Sheet, board  # noqa: E402

UTC = dt.timezone.utc


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for name in list(__import__("os").environ):
        if name.startswith("TFB_PLAN_") or name in ("TFB_DAILY_PLAN", "TFB_FORECAST_RECORD"):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TFB_DAILY_PLAN", "on")
    monkeypatch.setenv("TFB_PLAN_LOG_DETAIL", "off")
    monkeypatch.setenv("TFB_PLAN_ROI_CAP", "off")
    monkeypatch.setenv("TFB_PLAN_IBKR_WAIT_UNTIL_H", "0")
    monkeypatch.setenv("TFB_PLAN_QUOTE_TIME", "optional")
    monkeypatch.setenv("TFB_BQ_PROJECT", "project-2e3530e6-2311-4aa3-b36")
    monkeypatch.setenv("TFB_BQ_DATASET", "tfb_archive")


# ----------------------------------------------------------------------------- the record of 28 Nov 2026 (as REST)
def _row(market, horizon, band, period, low_to, mid_to, calls, days, promised, delivered, rose, index, beat, reading,
         suffixes, through="2026-11-27"):
    return {"market": market, "horizon": horizon, "band": band, "period": period, "low_to_pp": low_to,
            "mid_to_pp": mid_to, "promise_from_pp": "0.91", "promise_to_pp": "6.3", "calls": str(calls),
            "symbols": "12", "days": str(days), "first_call": "2026-06-28", "last_call": "2026-10-28",
            "scored_through": through, "promised_pp": promised, "delivered_pp": delivered,
            "share_of_promise": None, "rose_pct": rose, "index_symbol": index, "index_pp": "-1.32",
            "excess_pp": "2.46", "beat_pct": beat, "reading": reading, "suffixes": suffixes}


def view_rows():
    us1 = ("2.67", "4.21")
    us3 = ("6.06", "10.5")
    return [
        _row("Hong Kong", "1M", "all", "all", "2.06", "3.31", 66, 36, "2.97", "3.28", "87.9", "^HSI", "83.3",
             "beat its index", "HK", "2026-11-26"),
        _row("Hong Kong", "1M", "high", "all", "2.06", "3.31", 21, 19, "4.68", "0.74", "76.2", "^HSI", "57.1",
             "too few calls", "HK", "2026-11-26"),
        _row("US", "3M", "all", "all", *us3, 160, 39, "8.99", "2.88", "53.1", "SPY", "41.9",
             "made money, behind its index", "US", "2026-11-12"),
        _row("US", "3M", "low", "all", *us3, 54, 20, "4.89", "3.48", "61.1", "SPY", "46.3",
             "made money, behind its index", "US", "2026-11-12"),
        _row("US", "3M", "mid", "all", *us3, 54, 23, "8.57", "0.82", "46.3", "SPY", "37.0",
             "made money, behind its index", "US", "2026-11-12"),
        _row("US", "3M", "high", "all", *us3, 52, 31, "13.69", "4.41", "51.9", "SPY", "42.3",
             "made money, behind its index", "US", "2026-11-10"),
        _row("US", "1M", "all", "all", *us1, 459, 86, "3.66", "1.13", "54.5", "SPY", "59.7", "beat its index", "US"),
        _row("US", "1M", "low", "all", *us1, 153, 64, "2.03", "-1.28", "37.9", "SPY", "43.1", "lost money", "US"),
        _row("US", "1M", "mid", "all", *us1, 153, 56, "3.35", "1.42", "53.6", "SPY", "65.4", "beat its index", "US"),
        _row("US", "1M", "high", "all", *us1, 153, 64, "5.61", "3.26", "71.9", "SPY", "70.6", "beat its index", "US"),
        _row("US", "1M", "high", "forward", *us1, 5, 2, "5.9", "-9.0", "0.0", "SPY", "0.0", "too few calls", "US"),
        _row("Tadawul", "1M", "all", "all", "4.19", "5.49", 204, 85, "4.64", "-4.37", "32.8", "^TASI.SR", "45.6",
             "lost money", "SR", "2026-11-26"),
        _row("Tadawul", "1M", "high", "all", "4.19", "5.49", 68, 48, "6.25", "-8.53", "20.6", "^TASI.SR", "26.5",
             "lost money", "SR", "2026-11-26"),
    ]


def tab(rows=None, formatted=True):
    page = fr.page_rows(view_rows() if rows is None else rows, "2026-11-28 12:40")
    return [[str(c) for c in r] for r in page] if formatted else page      # the read is FORMATTED_VALUE


# ----------------------------------------------------------------------------- V (the view)
def _view():
    return {o.name: o for o in bv.load_objects()}["v_forecast_record"]


def test_v1_the_view_installs_last_and_reads_two_sources():
    objs = bv.load_objects()
    assert [o.name for o in objs][-1] == "v_forecast_record" and bv.__version__ == "1.39.0"
    v = _view()
    assert v.path.name == "152_v_forecast_record.sql" and v.kind == "VIEW" and bv.check_objects(objs) == []
    code = bv._code(v.template)
    assert set(bv._REF.findall(code)) - {"v_forecast_record"} == {"v_outcome_engine", "market_series_daily"}
    assert "views 1.39.0" in v.template and "research only" in v.template
    assert "the live system never reads BigQuery" in v.template


def test_v2_the_population_and_the_fixed_thirds():
    code = bv._code(_view().template)
    assert ("engine_scored AND in_engine_cohort AND entry_investability = 'INVESTABLE' AND entry_stance = 'bullish'"
            in code)
    assert "horizon IN ('1W', '2W', '1M', '3M')" in code
    # the thirds are cut once, on the calls scored by 27 Nov 2026: later calls never move them
    assert "WHERE recorded_date <= DATE '2026-11-28' AND target_bar_date <= DATE '2026-11-27'" in code
    assert "NTILE(3) OVER (PARTITION BY market, horizon ORDER BY p, symbol, recorded_date) AS third" in code
    assert "MAX(IF(third = 1, p, NULL)) AS low_to, MAX(IF(third = 2, p, NULL)) AS mid_to" in code
    # the same band rule as daily_plan.band_of
    assert "WHEN c.p <= k.low_to THEN 'low'" in code and "WHEN k.mid_to IS NULL OR c.p <= k.mid_to THEN 'mid'" in code
    assert dp.band_of(2.67, 2.67, 4.21) == "low" and dp.band_of(2.68, 2.67, 4.21) == "mid"
    assert dp.band_of(4.21, 2.67, 4.21) == "mid" and dp.band_of(4.24, 2.67, 4.21) == "high"
    assert dp.band_of(9.0, 2.67, None) == "mid" and dp.band_of(1.0, None, None) is None
    assert "IF(b.recorded_date >= DATE '2026-11-29', ['forward'], ARRAY<STRING>[])" in code


def test_v3_the_index_window_starts_before_the_call():
    code = bv._code(_view().template)
    assert "b0.date < c.recorded_date" in code and "b0.next_date >= c.recorded_date" in code    # strictly before
    assert "b1.date <= c.target_bar_date" in code and "b1.next_date > c.target_bar_date" in code
    for market, index in (("US", "SPY"), ("Tadawul", "^TASI.SR"), ("Hong Kong", "^HSI"), ("Europe", "^STOXX"),
                          ("London", "^FTSE"), ("Tokyo", "^N225"), ("China", "000300.SS")):
        assert "WHEN '%s' THEN '%s'" % (market, index) in code, market
    assert "ELSE 'ACWI'" in code
    assert "QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY captured_at DESC) = 1" in code


def test_v4_the_reading_and_every_column_the_tab_copies():
    code = bv._code(_view().template)
    assert "WHEN g.calls < 30 OR g.days < 10 THEN 'too few calls'" in code
    assert "WHEN g.delivered <= 0 THEN 'lost money'" in code
    assert "WHEN g.excess > 0 AND g.beat >= 50 THEN 'beat its index'" in code
    final = code[code.rindex("\nSELECT"):]
    for _, col, _ in fr.COLUMNS:
        assert re.search(r"\b%s\b" % col, final), col
    # the cuts leave unrounded, so a reader places a promise exactly as the view did (Codex P2 on #1114)
    assert "g.low_to AS low_to_pp, g.mid_to AS mid_to_pp" in final and "ROUND(g.low_to" not in final


# ----------------------------------------------------------------------------- F (the tab writer)
class _Query:
    def __init__(self, rows=None, error=None):
        self.rows, self.error, self.sql = rows, error, []

    def __call__(self, session, project, location, sql):
        self.sql.append(sql)
        if self.error:
            raise self.error
        return deepcopy(self.rows if self.rows is not None else view_rows())


def test_f1_one_select_of_the_view():
    sql = fr.sql_record("p.d")
    assert sql.startswith("SELECT market, horizon, band, period, low_to_pp") and sql.endswith("FROM `p.d.v_forecast_record`")
    cols = [c.strip() for c in sql[len("SELECT "):sql.index(" FROM ")].split(",")]
    assert cols == [c for _, c, _ in fr.COLUMNS] and "symbol" not in cols             # no name is read
    assert not re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP)\b", sql)


def test_f2_the_tab_header_is_where_the_plan_looks_and_the_big_markets_lead():
    page = tab(formatted=False)
    h, col = dp.table_index(page, dp.RECORD_NEED)
    assert h == 5 and page[5] == fr.HEAD                                  # the plan scans the first eight rows
    body = page[6:6 + len(view_rows())]
    assert [(r[0], r[1], r[2], r[3]) for r in body] == [
        ("US", "1M", "all", "all"), ("US", "1M", "low", "all"), ("US", "1M", "mid", "all"),
        ("US", "1M", "high", "all"), ("US", "1M", "high", "forward"), ("US", "3M", "all", "all"),
        ("US", "3M", "low", "all"), ("US", "3M", "mid", "all"), ("US", "3M", "high", "all"),
        ("Tadawul", "1M", "all", "all"), ("Tadawul", "1M", "high", "all"),
        ("Hong Kong", "1M", "all", "all"), ("Hong Kong", "1M", "high", "all")]
    us = body[0]
    assert us[fr.HEAD.index("Calls")] == 459 and us[fr.HEAD.index("Delivered %")] == 1.13
    assert type(us[fr.HEAD.index("Calls")]) is int and type(us[fr.HEAD.index("Days")]) is int      # whole numbers
    assert [fr.cell_value(v, k) for v, k in (("12.6", "i"), ("x", "n"), (None, "t"), (" US ", "t"), ("-1.28", "n"))] == [
        13, "", "", "US", -1.28]
    assert us[fr.HEAD.index("Share Of Promise")] == "" and us[fr.HEAD.index("Scored Through")] == "2026-11-27"
    assert "calls scored through 2026-11-27" in page[1][0] and "3 market(s)" in page[1][0]
    assert page[-1][0].endswith("Not investment advice.")
    # every header the Daily Plan reads is on the tab
    for name in dp.RECORD_NEED + ("Promised %", "Rose %", "Beat %", "Index", "Low To %", "Mid To %", "Suffixes",
                                  "Scored Through"):
        assert name in fr.HEAD, name


def test_f3_off_and_a_bad_dataset_read_nothing(monkeypatch, capsys):
    q = _Query()
    monkeypatch.setenv("TFB_FORECAST_RECORD", "off")
    assert fr.run_once(False, book=Book({}), session=object(), query=q) == 0 and q.sql == []
    monkeypatch.setenv("TFB_FORECAST_RECORD", "on")
    monkeypatch.setenv("TFB_BQ_DATASET", "bad`; DROP")
    assert fr.run_once(False, book=Book({}), session=object(), query=q) == 2 and q.sql == []
    assert "malformed" in capsys.readouterr().out


def test_f4_a_failed_query_or_an_empty_record_leaves_the_tab(monkeypatch, capsys):
    b = Book({"Forecast_Record": Sheet([["old"]])})
    assert fr.run_once(False, book=b, session=object(), query=_Query(error=RuntimeError("HTTP 403"))) == 2
    assert b.tabs["Forecast_Record"].values == [["old"]]
    assert fr.run_once(False, book=b, session=object(), query=_Query(rows=[])) == 2
    assert b.tabs["Forecast_Record"].values == [["old"]]
    out = capsys.readouterr().out
    assert "RuntimeError: HTTP 403; nothing written" in out and "returned no row; the previous tab is left" in out


def test_f5_a_dry_run_writes_nothing_and_a_run_writes_the_tab(capsys):
    b = Book({})
    q = _Query()
    assert fr.run_once(True, book=b, session=object(), query=q, now=NOW) == 0 and "Forecast_Record" not in b.tabs
    assert len(q.sql) == 1 and q.sql[0] == fr.sql_record("project-2e3530e6-2311-4aa3-b36.tfb_archive")
    assert fr.run_once(False, book=b, session=object(), query=q, now=NOW) == 0
    got = b.tabs["Forecast_Record"].values                       # (the fake sheet keeps no blank row)
    h = got.index(fr.HEAD)
    assert h <= 5 and got[h + 1][:4] == ["US", "1M", "all", "all"] and dp.read_record(got)["through"] == "2026-11-27"
    assert "Built 2026-11-10 10:00 Riyadh | forecast_record 1.0.0" in got[1][0]
    out = capsys.readouterr().out
    assert "RECORD 13 row(s), 3 market(s), 1 forward row(s), calls scored through 2026-11-27" in out
    assert "wrote the Forecast_Record tab (13 row(s))" in out


def test_f6_the_tab_and_the_log_name_markets_only():
    names = {c for _, c, _ in fr.COLUMNS}
    assert "symbol" not in names and not any(n.endswith("_sar") or n in ("qty", "amount") for n in names)
    assert "symbols" in names                                                       # a count of names, not a name


# ----------------------------------------------------------------------------- H (the Daily Plan, 1.12.0)
SRC_FP = [["Symbol", "Name", "Currency", "Sector", "Current Price", "Warnings", "Forecast Price 1M", "Forecast Price 3M"],
          ["ITRN.US", "Ituran", "USD", "Technology", "51.82", "analyst_lkg:1h", "54.73", "58.90"],  # 1M high, 3M high
          ["TEN.US", "Tsakos", "USD", "Energy", "25.00", "", "25.50", ""],                       # 1M low, no 3M price
          ["ZTO.US", "ZTO", "USD", "Industrials", "20.66", "", "21.40", "22.30"],                # 1M mid, 3M mid
          ["NMM.US", "Navios", "USD", "Industrials", "60.00", "", "63.00", "66.00"],             # 1M high, 3M mid
          ["SBLK.US", "Star Bulk", "USD", "Industrials", "27.07", "", "", ""],                   # no forecast price
          ["LADR.US", "Ladder", "USD", "Real Estate", "11.00", "", "11.20", "12.00"],            # 1M low, 3M mid
          ["BBOX.L", "Tritax", "GBX", "Real Estate", "147.70", "", "150.00", "160.00"]]          # no London record
# (the board's eighth name has no source row here: no price, so neither horizon is measured)


def record():
    return dp.read_record(tab())


def test_h1_the_tab_read_back():
    rec = record()
    assert rec == dp.read_record(tab(formatted=False))                              # raw or formatted: the same
    assert rec["suffix"] == {"US": "US", "SR": "Tadawul", "HK": "Hong Kong"} and rec["through"] == "2026-11-27"
    assert rec["cuts"][("US", "1M")] == (2.67, 4.21) and rec["cuts"][("US", "3M")] == (6.06, 10.5)
    assert ("US", "1M", "high") in rec["rows"] and len(rec["rows"]) == 12            # the forward row is not read
    hi = rec["rows"][("US", "1M", "high")]
    assert (hi["calls"], hi["days"], hi["delivered"], hi["rose"], hi["beat"], hi["index"]) == (153, 64, 3.26, 71.9,
                                                                                                70.6, "SPY")
    assert dp.read_record([]) == {} and dp.read_record([["no", "table"]]) == {}
    assert dp.market_of("aapl", rec["suffix"]) == "US" and dp.market_of("2222.SR", rec["suffix"]) == "Tadawul"
    assert dp.market_of("BBOX.L", rec["suffix"]) is None


def test_h2b_a_promise_between_the_rounded_and_the_true_cut_reads_the_views_band():
    rows = [dict(r, low_to_pp="2.6744", mid_to_pp="4.2133") if r["market"] == "US" and r["horizon"] == "1M" else r
            for r in view_rows()]
    rec = dp.read_record(tab(rows))
    assert rec["cuts"][("US", "1M")] == (2.6744, 4.2133)                      # the tab keeps the full cut
    on_cut = dp.honest_read("XYZ.US", {"price": 100.0, "fp1m": 102.672}, rec, "1M")
    assert on_cut["band"] == "low" and dp.band_of(2.672, 2.6744, 4.2133) == "low"      # 2.67 would have read mid
    assert dp.honest_read("XYZ.US", {"price": 100.0, "fp1m": 104.212}, rec, "1M")["band"] == "mid"


def test_h2_the_honest_read_and_its_label():
    rec = record()
    src = lambda price, fp1=None, fp3=None: {"price": price, "fp1m": fp1, "fp3m": fp3}
    r = dp.honest_read("ITRN.US", src(51.82, 54.73), rec, "1M")
    assert (r["market"], r["band"], r["honest"], round(r["promise"], 2)) == ("US", "high", 3.26, 5.62)
    assert dp.honest_label(r) == ("[observe] honest 1M +3.3% vs page +5.6% (US buy calls like it: 72% rose, 71% beat "
                                  "SPY, 153 calls)")
    low = dp.honest_read("XYZ.US", src(80.0, 79.0), rec, "1M")                        # a promise under 0 reads low
    assert low["band"] == "low" and dp.honest_label(low).startswith("[observe] honest 1M -1.3% vs page -1.2% (US")
    hk = dp.honest_read("0700.HK", src(400.0, 420.0), rec, "1M")                       # its third has too few calls
    assert hk["band"] == "all" and hk["honest"] == 3.28
    assert dp.honest_label(hk) == ("[observe] honest 1M +3.3% vs page +5.0% (all Hong Kong buy calls: 88% rose, 83% "
                                   "beat ^HSI, 66 calls)")
    sa = dp.honest_read("1120.SR", src(100.0, 106.0), rec, "1M")
    assert (sa["band"], sa["honest"]) == ("high", -8.53)
    for sym, s, h, why in (("BBOX.L", src(147.7, 150.0), "1M", "no scored buy call of this listing"),
                           ("TEN.US", src(25.0, 25.5), "3M", "no 3M forecast price on its source page"),
                           ("TEN.US", src(None, 25.5), "1M", "no 1M forecast price on its source page"),
                           ("0700.HK", src(400.0, None, 460.0), "3M", "too few scored Hong Kong 3M buy calls")):
        r = dp.honest_read(sym, s, rec, h)
        assert r["honest"] is None and r["why"] == why, (sym, h)
        assert dp.honest_label(r) == "[observe] honest %s: not measured (%s)" % (h, why)


def _plan(k):
    return dp.decide(dp.read_qualified(board(qual=QUAL)), dp.read_sources([SRC_FP]), dp.read_calendar(CAL, []),
                     dp.read_ledger_lots(LOTS, dt.date(2026, 11, 10), 10), 33125.0, 2, k)


def _honest(p):
    return {c["symbol"]: [x for x in c["labels"] if x.startswith("[observe] honest")] for c in p["picks"] + p["pipeline"]}


def test_h3_every_judged_name_is_labelled_and_no_pick_changes(monkeypatch):
    assert dp.honest_mode() == "observe" and dp.knobs()["honest"] == "observe"
    for word, want in (("off", "off"), ("OFF", "off"), ("observe", "observe"), ("on", "observe"), ("", "observe")):
        monkeypatch.setenv("TFB_PLAN_HONEST", word)
        assert dp.honest_mode() == want, word
    base = _plan(dict(dp.knobs(), honest="off"))
    p = _plan(dict(dp.knobs(), honest="observe", record=record()))
    assert [c["symbol"] for c in p["picks"]] == [c["symbol"] for c in base["picks"]]
    for key in ("qty", "entry", "stop", "tp1", "alloc_sar"):
        assert [c[key] for c in p["picks"]] == [c[key] for c in base["picks"]], key
    assert all(not v for v in _honest(base).values())
    lab = _honest(p)
    assert set(lab) == {r[1] for r in QUAL} and all(len(v) == 2 for v in lab.values())   # picks and waiting names
    assert lab["ITRN.US"] == [
        "[observe] honest 1M +3.3% vs page +5.6% (US buy calls like it: 72% rose, 71% beat SPY, 153 calls)",
        "[observe] honest 3M +4.4% vs page +13.7% (US buy calls like it: 52% rose, 42% beat SPY, 52 calls)"]
    assert lab["TEN.US"][1] == "[observe] honest 3M: not measured (no 3M forecast price on its source page)"
    assert lab["BBOX.L"] == ["[observe] honest 1M: not measured (no scored buy call of this listing)",
                             "[observe] honest 3M: not measured (no scored buy call of this listing)"]
    assert lab["ZTO.US"][0].startswith("[observe] honest 1M +1.4% vs page +3.6%")
    # a name held by the regime gate is labelled too: the labels come before every gate
    held = _plan(dict(dp.knobs(), honest="observe", record=record(),
                      regime=dp.regime_rule(STRESS, "enforce", BLOCK, ["USD"])))
    assert held["picks"] == [] and all(len(v) == 2 for v in _honest(held).values())
    empty = _plan(dict(dp.knobs(), honest="observe", record={}))                   # no record: no label, same picks
    assert [c["symbol"] for c in empty["picks"]] == [c["symbol"] for c in base["picks"]]
    assert all(not v for v in _honest(empty).values())


def test_h4_the_run_log_gives_counts_only():
    p = _plan(dict(dp.knobs(), honest="observe", record=record()))
    line = dp.honest_line(p["picks"] + p["pipeline"], record(), "observe")
    assert line == ("honest observe: 8 name(s) labelled; 1M measured for 5 (2 under 0), 3M measured for 4 (0 under 0) | "
                    "record scored through 2026-11-27")
    assert dp.honest_line([], {}, "observe") == "honest observe: the Forecast_Record tab holds no record - no honest label"
    edge = [{"honest": [{"honest": v}, {"honest": None}]} for v in (-0.5, 0.0, 1.0)] + [{"labels": []}]
    assert dp.honest_line(edge, record(), "observe") == (
        "honest observe: 3 name(s) labelled; 1M measured for 3 (1 under 0), 3M measured for 0 (0 under 0) | record "
        "scored through 2026-11-27")                                                 # 0.0 is not under 0
    for sym in ("ITRN", "TEN", "ZTO", "NMM", "SBLK", "LADR", "BBOX"):
        assert sym not in line, sym


class _Watch(Book):
    def __init__(self, tabs):
        super().__init__(tabs)
        self.asked = []

    def worksheet(self, name):
        self.asked.append(name)
        return super().worksheet(name)


def _book(**over):
    tabs = {"Top_10_Investments": Sheet(board()), "Global_Markets": Sheet(SRC_FP), "Market_Leaders": Sheet([["x"]]),
            "Calendar_Events": Sheet(CAL), "_Portfolio_CostBasis": Sheet(LOTS), "Forecast_Record": Sheet(tab())}
    tabs.update(over)
    return _Watch(tabs)


def _ledger_labels(b):
    vals = b.tabs["_Daily_Plan"].values
    i_sym, i_lab = vals[0].index("Symbol"), vals[0].index("Labels")
    return {r[i_sym]: r[i_lab] for r in vals[1:]}


def test_h5_the_cut_reads_the_tab_once_and_freezes_the_labels(capsys):
    b = _book()
    assert dp.main([], book=b, now=NOW) == 0
    out = capsys.readouterr().out
    assert b.asked.count("Forecast_Record") == 1
    assert ("honest observe: 8 name(s) labelled; 1M measured for 5 (2 under 0), 3M measured for 4 (0 under 0) | "
            "record scored through 2026-11-27") in out
    labels = _ledger_labels(b)
    assert "[observe] honest 1M +3.3% vs page +5.6% (US buy calls like it: 72% rose, 71% beat SPY, 153 calls)" in \
        labels["ITRN.US"]
    assert all(labels[s].count("[observe] honest") == 2 for s in labels)
    b.tabs["Forecast_Record"] = Sheet(tab(rows=[]))                                 # the frozen plan is not re-read
    assert dp.main([], book=b, now=NOW + dt.timedelta(hours=1)) == 0
    assert b.asked.count("Forecast_Record") == 1 and _ledger_labels(b) == labels


def test_h6_off_reads_nothing_and_a_missing_tab_labels_nothing(monkeypatch, capsys):
    monkeypatch.setenv("TFB_PLAN_HONEST", "off")
    b = _book()
    assert dp.main([], book=b, now=NOW) == 0
    assert "Forecast_Record" not in b.asked and "honest" not in capsys.readouterr().out
    assert all("[observe] honest" not in v for v in _ledger_labels(b).values())
    monkeypatch.setenv("TFB_PLAN_HONEST", "observe")
    b = _book()
    del b.tabs["Forecast_Record"]
    assert dp.main([], book=b, now=NOW) == 0
    assert "honest observe: the Forecast_Record tab holds no record - no honest label" in capsys.readouterr().out
    assert all("[observe] honest" not in v for v in _ledger_labels(b).values())


def test_h7_a_tab_that_cannot_be_read_never_stops_the_cut(capsys):
    class _Broken(Sheet):
        def get(self, range_name):
            raise RuntimeError("boom")
    b = _book(Forecast_Record=_Broken([]))
    assert dp.main([], book=b, now=NOW) == 0
    out = capsys.readouterr().out
    assert "honest observe: the Forecast_Record tab was not read (RuntimeError)" in out and "CUT 2026-11-10" in out
    assert all("[observe] honest" not in v for v in _ledger_labels(b).values())


# ----------------------------------------------------------------------------- W (workflows, pins, CI)
def _wf(name):
    return yaml.safe_load((ROOT / ".github/workflows" / name).read_text(encoding="utf-8"))


def test_w1_the_record_follows_the_two_archives_and_writes_only_its_tab():
    wf = _wf("forecast_record.yml")
    on = wf.get("on", wf.get(True))
    assert wf["name"] == "📏 Forecast Record" and wf["permissions"] == {"contents": "read"}
    assert on["workflow_run"] == {"workflows": ["🔬 BigQuery Symbol Deep Data", "🌍 BigQuery Market Series"],
                                  "types": ["completed"], "branches": ["main"]}
    assert on["workflow_dispatch"]["inputs"]["dry_run"]["default"] is False and "schedule" not in on
    names = {_wf(p.name).get("name") for p in (ROOT / ".github/workflows").glob("*.yml")}
    assert set(on["workflow_run"]["workflows"]) <= names                             # the archives it follows exist
    job = wf["jobs"]["record"]
    assert "github.event.workflow_run.conclusion == 'success'" in job["if"]
    assert job["concurrency"] == {"group": "tfb-forecast-record", "cancel-in-progress": False}
    step = [s for s in job["steps"] if "forecast_record.py" in str(s.get("run", ""))][0]
    assert step["env"]["TFB_FORECAST_RECORD"] == "${{ vars.TFB_FORECAST_RECORD || 'on' }}"
    assert "--dry-run" in step["run"]


def test_w2_the_plan_arms_observe():
    wf = _wf("daily_plan.yml")
    env = [s for s in wf["jobs"]["daily_plan"]["steps"] if "daily_plan.py" in str(s.get("run", ""))][0]["env"]
    assert env["TFB_PLAN_HONEST"] == "${{ vars.TFB_PLAN_HONEST || 'observe' }}"
    src = (ROOT / "scripts/daily_plan.py").read_text(encoding="utf-8")
    assert dp.__version__ == "1.12.0" and "1.12.0 (2026-11-28): EACH NAME SHOWS WHAT CALLS LIKE IT DELIVERED" in src


def test_w3_the_pins_and_the_ci_lane():
    manifest = (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("forecast_record", "%s", "forecast record")' % fr.__version__ in manifest
    assert '("daily_plan", "%s", "daily plan")' % dp.__version__ in manifest
    assert '("apply_bq_views", "%s", "bigquery looker views")' % bv.__version__ in manifest
    ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "tests/test_regime_shadow.py \\\n            tests/test_forecast_record.py \\" in ci
    assert "VERSION 1.1.109 (2026-11-28) -- THE HONEST FORECAST" in ci
