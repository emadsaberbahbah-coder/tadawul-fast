#!/usr/bin/env python3
# tests/test_bq_views.py
"""
Harness for scripts/apply_bq_views.py v1.5.0 and the SQL in sql/bigquery/
(C8 Phase 2: typed BigQuery views for Looker Studio; 2b: the global markets
page; 2c: the World Top-25 ROI, My Portfolio / Top-10 evidence and symbol
pages; 2d: candles, analysts, news and the TFB rating per symbol; 2e: the
learning loop from the archive). Offline: the BigQuery session is a fake, no
google library is imported.

  V1  the 41 objects load in apply order (6 functions, then the views and
      table functions in file order) and the static checks pass
  V2  every header a view reads with f_cell exists in that tab's archived
      header row (fixture: the 2026-10-29 snapshot)
  V3  render: the placeholder is fully replaced; bad project / dataset ids
      are refused
  V4  the body each object keeps in BigQuery hashes to the definitions read
      back from INFORMATION_SCHEMA after the 29 Oct creation (installed pins);
      the objects v1.5.0 adds or changes hash to their reviewed files (pending
      pins, confirmed by bq_views --verify after the next --write; v1.2.0's
      were read back on 31 Oct, v1.3.0's on 1 Nov and v1.3.1 / 1.4.0 / 1.4.1's
      on 1 Nov 15:27 Riyadh, and are installed pins); a table function keeps
      the query after its 'AS' line
  V5  the static checks refuse a data-changing statement, a hard-coded
      project, a forward reference, two statements, a function after a view,
      a misnamed file, a CREATE TABLE and an unknown source table; they accept
      a table function after the view it reads
  V6  main(): dry run creates nothing; --write without credentials exits 2;
      --write applies every file in order then verifies; --verify reports a
      changed or missing object with exit 1
  V7  bq_views.yml is dispatch-only, read-only for the repository, defaults to
      verify, and runs the script in the chosen mode
  V8  nothing here can write to the workbook: no Sheets API in the script or
      the SQL, and every SQL object is a view or a function
  V9  v_performance_log.entry_stance mirrors track_performance's
      _reco_trade_sign (read from its source): STRONG SELL is bearish
  V10 dry run: offline without a key; with a key every statement is sent as
      a BigQuery dry run in order; a broken key exits 2
  V11 the market objects: v_market_daily reads only columns that
      scripts/archive_market_series.py SCHEMA declares, from that table only;
      the table functions take (start_date DATE, end_date DATE) and read only
      the objects before them
  V12 v_performance_log scores the CALL as track_performance's summary does
      (_directional_outcome on records loaded with a HOLD default): the
      8 tiers' signs read from its source, HOLD and aliases neutral,
      is_scored = decided, win_flag only for decided calls, one record per
      Key; fails on the first cut's price-direction flags
  V13 v_portfolio_decision.pd_ibkr_evidence recognizes every note
      pdIbkrNote_ (apps_script/10_My_Portfolio.gs) writes - SENT (...) too;
      fails on the first cut's pattern
  V14 every header a v1.2.0 view looks up is one its writer declares:
      v_universe on all four market pages (core/sheets/schema_registry.py),
      the news views on scripts/news_macro_gate.py's three headers,
      v_calendar_events on scripts/run_calendar_sync.py's; and the tables
      they read are exactly what archive_to_bigquery v1.1.0 adds (header row 1)
  V15 v_universe units and regions: a registry pct column (a fraction) is
      read x 100 or recomputed from prices, a points column never is; every
      listing suffix on the live pages (31 Oct) has exactly one region and a
      listing country; the market-series regions are reused by name
  V16 the new table functions: f_universe_return(start_date DATE, end_date
      DATE) and f_symbol_pick(q STRING, market STRING) read only earlier
      objects and document their Looker custom queries
  V17 the evidence views read typed views only (no raw archive table), the
      portfolio's bearish calls are v_performance_log's, f_col / f_at pair
      exactly as f_cell, and every caution flag has a plain-word label; the
      board is the newest v_top10_status night (an empty board stays empty),
      the decision row comes from the holdings' own night, and a market
      copy older than a day is flagged (Codex P2 x2 on #883 - fail on the
      first cut)
  V18 the symbol deep views (v1.3.0): each reads only the columns
      scripts/archive_symbol_deep.py declares for its table, the view's
      source tables are exactly that script's TABLES, every picker view
      carries symbol_label, the TFB rating's six weights sum to 1 in both
      the score and the coverage, its labels and stars fall with the
      score, and both evidence views carry the rating and read the market
      pages' STRONG_SELL spelling
  V19 v_symbol_candles keeps one candle per trading session: a symbol with
      Yahoo bars never gets a snapshot candle, a snapshot's session is the
      venue-local date of its Last Updated stamp less the opening time,
      stepped back over that venue's weekend (simulated for every weekday),
      one candle per session; the venue table covers every market_country
      v_universe can produce, with real time zones and the Sunday-Thursday
      markets (Codex P2 on #884 - fails on the first cut)
  V20 v_symbol_candles places every candle against the symbol's newest close
      (v1.3.1, for the Looker candlestick, whose axis always starts at 0):
      the reference window is the symbol's whole history in date order, so
      the newest close is 0 and each symbol keeps its own reference (simulated
      from the parsed window - fails on the default frame, which ends at the
      current row and flattens every close to 0, on a missing partition and
      on a descending order); the four OHLC columns share that one reference
  V21 v_fx_sar (v1.4.0): the riyal rate per currency reads only the FX rows
      of the Commodities_FX page, USD from SAR=X and every other currency
      through its dollar cross with the direct quote preferred, pence as a
      hundredth of a pound; the rate table is simulated from the parsed
      rules (fails on the inverse quote taken as direct, on pence as pounds)
  V22 the rating gate (v1.4.0): v_symbol_rating names the engine verdict
      beside every rating (blocked, do not invest, watch only, below the
      floors, none) in rating_gate and tfb_rating_display, names the analyst
      and news sources, and both insight views carry them; the portfolio
      view gives riyal values for every holding (the decision page's figure
      first, else the sheet's value times v_fx_sar) with its source, and the
      Top-10 view says seat and allocation in plain words (a row without a
      ticket is 'research board', never an order) - fails on the 1.3.1 files;
      v1.4.1: the riyal weight is a share of the whole book, NULL for every
      holding while any holding has no riyal value (fails on the 1.4.0 file)
  V23 v_outcome_coverage (v1.4.0, cohort in v1.4.1): per horizon and ALL,
      due / matured / scored / wins with the win rate over scored calls of
      the sheet's outcome cohort only (first valid matured record per key)
      and the matured share over due records; the overdue count is active
      records past their target; simulated on a small log (fails when the
      win rate is taken over matured instead of scored, when a key archived
      twice counts twice, when due ignores the target date, and when ALL is
      missing)
  V24 v_outcome_engine (v1.5.0): every record scored from the archived daily
      close of the FIRST bar on/after the target session, never a later one
      (a bar past the 7-day window is no_bar_at_target; the archive not yet
      there is awaiting_bar); the target session is the venue-local day,
      stepped to the next day after the venue's close; the call is scored by
      entry stance exactly as the sheet scores realized ROI (a bearish fall
      wins); HOLD is never scored; excursions, touched target and the
      1W / 2W / 1M / 3M ladder come from the same bars; the engine cohort is
      the first engine-scored record per key; the sheet's call sits beside
      the engine's as agree / disagree / sheet_only / engine_only; the view
      reads only v_performance_log and symbol_bars_daily (simulated on a
      small log - fails when a later session is accepted, when a bearish
      WIN reads LOSS, when a second copy of a key counts, when HOLD scores)
  V25 v_outcome_calibration (v1.5.0): every rate over the engine cohort
      only (records and due over the slice), one row per dimension x value
      plus ALL, the sheet's cohort rate beside the engine's, 'High confidence
      under-delivers' when the HIGH band's win rate sits below MEDIUM or LOW,
      'small sample' under 30 scored calls, roi_capture NULL when the average
      target is under 1 point (simulated - fails on rates over every record,
      on a missing ALL row, on the flag against a small sample)
  V26 v_board_archive (v1.5.0): each Top-10 panel default (DT10_PANEL in
      16_Decision_Top10.gs, read from its source) is a named gate - INVEST,
      INVESTABLE, no block, no sell-class tier, reliability 70, data quality
      80, risk Medium, 12-month ROI 12 (annualized 10), no conflict, not a
      holding, not named by the news gate - the seat order and the 3-per-
      sector / 10-per-market / 10-seat caps are simulated, executable is the
      constant FALSE and the board_kind says research only (each floor is a
      golden negative; fails when a holding or a HIGH-risk name takes a seat,
      when a fourth name of one sector seats, when executable is not FALSE)
  V27 v_outcome_coverage (v1.5.0 extension): the engine's scored / wins /
      rate and the backlog states (scorable now, awaiting bar, no bar, no bar
      at target) per horizon and summed in ALL, beside the tracker's columns
      unchanged; the file now applies after v_outcome_engine (fails when the
      ALL row drops an engine column, when the engine rate is taken over
      every scored row instead of the cohort)
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import apply_bq_views as bv  # noqa: E402

FIXTURE = ROOT / "tests" / "fixtures" / "bq_archive_headers_2026-10-29.json"
WORKFLOW = ROOT / ".github" / "workflows" / "bq_views.yml"
_KEY_ENV = ("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS")
PROJECT = bv.DEFAULT_PROJECT
DATASET = bv.DEFAULT_DATASET

# Read back on 2026-10-29 from INFORMATION_SCHEMA.VIEWS / ROUTINES of
# project-2e3530e6-2311-4aa3-b36.tfb_archive right after the objects were
# created: TO_HEX(SHA256(view_definition | routine_definition));
# v_performance_log re-applied after the Codex review (STRONG SELL bearish);
# v_market_daily / f_market_window / f_market_summary created the same day
# (apply_bq_views 1.1.0) and read back the same way; v_performance_log and
# v_portfolio_decision re-applied after the Codex review of #879 (call-scored
# learning flags; the SENT evidence note) and read back again.
INSTALLED = {
    "f_cell": "25c3a65640088f2a000cd3c0a9f8c792a7129a546e5189f6a39ec08cf2315e7d",
    "f_num": "135ec9de4bf03c52499f4edf6e2a1d9caaf7cf6df1f1b6989a426968c663189a",
    "f_ts": "a03bb5075755336cde6ba070413984e3bd43cb842cb835f9eea64751f10c7fb6",
    "v_archive_health": "9932e59c56f917b3691ff63eae0b24581049d1a25d0bed91f9eecd71d37dcb72",
    "v_board_episodes": "76a6a240891eca669a574fd50b6d61b3d41439e502d9107aaefeb3c43d386048",
    "v_my_portfolio": "3fade0a8611cd65e9106643e753c454bb01c13a4c1743caa2a98403e7ef7c5dd",
    "v_performance_log": "c8f644b51d60100f062824d4a7edd19898fedd89005e122ecce8d515d02635dc",
    "v_portfolio_decision": "98b6090df20eb1244dc9dd6d0a0054ace85ed9036eedc4679fc86e85fd87b4ce",
    "v_run_log": "4b06dfa258072044b7a8a7fd162dc9b0407879e9238f5d6b9eada60a2e5a824c",
    "v_selection_log": "3a9400566a6bdbeab9087a8950ea18dbf22150b98efc80d7a27de9af7846a234",
    "v_signal_history": "b0f74c3f849376e969ea5637993f33fa24a06e799537b8f112aed35d04a43ff7",
    "v_top10_board": "dde854ba2022d754bb9398ca4405133ae8a9c7dedce618cca8746ee5aaa30144",
    "v_top10_status": "ec55e82d26bfeae7e043cdce6a66a6c0fa5fe86fa6f2668ad5a4758cc8b36945",
    "v_market_daily": "9c541c587ec6b06a2abd5c11542a60c6356fc7e98e7d5cdf77c99f6571e34a04",
    "f_market_window": "64f1a45c397a3f8f0c66fd1495ed54be4447cd26193aab56f21363bf8e428750",
    "f_market_summary": "06c5ec61c824be4019428bc36212230f94b0fea6a3394b63c8b007cd01cda808",
    # apply_bq_views 1.2.0, created by bq_views write run 41227078785 (31 Oct) and read back
    # from INFORMATION_SCHEMA the same evening
    "f_col": "bdc2111efe102498cb71de7287bbd54f04060720c72a12055ba169850c9d0e13",
    "f_at": "0778d85d64b36c5c40183dbd19b6ef16edeb539e246443bc083bdaca71671108",
    "f_ts_utc": "2a97c7875166cc6548e2d5443bc831405d56b9069ceb91bbee6ebadd120173be",
    "v_universe": "eeb532b232eb1269e5246139004ef9eb68feb622e91ef6948ff319d3529b140a",
    "v_universe_latest": "981638d8da391f7bee9e237739cd7311c98cbc0b365b5db3ca9c61fb65303260",
    "f_universe_return": "f3a7153aa93cb74570c523ab549db903274ac9290c68be8a1c2d048ca0e7e09d",
    "v_news_items": "98d76e754284969f91c457d84c4ea59df01402bc3ce593053d0425e444aade47",
    "v_econ_events": "88f79df71ae3a0d2be5973b8d6e99af77d7d6cbed3c816443d3ad93d1a0842d6",
    "v_news_gate": "ba73984a770ff9e343fbc477ecae62cd5317dbea9d24880559ca33c3ad7a6c6f",
    "v_calendar_events": "982b2d62ea7254759c320d3af87220543ec01405b72c2e2bf99938c639f4e6f0",
    "f_symbol_pick": "32777aa976c7220b789a103de61a027892173c98c731cfb3a7dedc467affb877",
    # apply_bq_views 1.3.0, created by bq_views write run 41278750795 (1 Nov) and read back
    # from INFORMATION_SCHEMA the same morning (all nine matched; v_symbol_candles changes
    # again in 1.3.1, so it stays pending)
    "v_symbol_analyst": "32958e9e3e16cc6e52fafb4b784d12ab362f04bc4b8272739a04c1b06570a855",
    "v_symbol_news": "1e48d645856426b096cd9dc04ba594e0146f58b9a79de89bcb3b4e44a41c9bfd",
    "v_dive_signals": "70f12d9f52f86dda605931c4ddb5011382d51658cb725e51012302cba4836ed8",
    "v_dive_board": "3af206fc77283b1c9fbdff585d61d997f577c444ad5bc86088a0b33ac5ecdfaf",
    "v_dive_events": "bfcf61db2425619a8fe2f18778d573d0b506ba55a59f3f4850af067b5904f693",
    # apply_bq_views 1.3.1, 1.4.0 and 1.4.1, installed by bq_views write run 41306557892
    # (1 Nov, 15:27 Riyadh) and read back from INFORMATION_SCHEMA the same afternoon (38/38)
    "v_symbol_candles": "c30bd475ffd4bb38bdf2e60676d34c3c4181925e4a44b6b4b54c68b28f4caf93",
    "v_fx_sar": "2c6262eb53692796710a71fa045c4ad0d56d883b66a597cc0d3ec011d00040ca",
    "v_symbol_rating": "3a07d374b2b1c175e9dd040a0fbd87b6dda158b1d35edf00bf56d263a2b7d0d1",
    "v_top10_insight": "66d2facf68aadfb9088e05eed29669daf063b04bb1b82a8ccf0928911438e9cf",
    "v_portfolio_insight": "a905416997fa03335516b028edfa60a215d21d22ee6c75fdc54aba52eb64216b",
}
# The objects apply_bq_views 1.5.0 adds or changes (2 Nov 2026), hashed from the
# reviewed files: bq_views --verify after the next --write must find these exact
# bodies in INFORMATION_SCHEMA (then they move to INSTALLED). Installed body
# v_outcome_coverage replaces: ab308c8c736d (1.4.1).
PENDING = {
    "v_outcome_engine": "a7f75e0528a36bed30147611165696df9f8074f4c2551d96dc0e83b9d8f71d62",
    "v_outcome_calibration": "038aef710bcea57e18f2d8f417a7be79f9a609165130ed4098470c09148a949d",
    "v_board_archive": "406e8fb4eb5f5cc1b00f5f120bd4b7e3e682ae21868dffe50ac68bc4d5ff81bc",
    "v_outcome_coverage": "341bd1f3a44d5d35e88c309a3f3a6ac6dd6e4678d640843c373fddad87447f31",
}
EXPECTED_ORDER = [
    "f_cell", "f_num", "f_ts", "f_col", "f_at", "f_ts_utc",
    "v_board_episodes", "v_performance_log", "v_selection_log", "v_signal_history",
    "v_top10_board", "v_portfolio_decision", "v_my_portfolio", "v_top10_status",
    "v_run_log", "v_archive_health",
    "v_market_daily", "f_market_window", "f_market_summary",
    "v_universe", "v_universe_latest", "f_universe_return",
    "v_news_items", "v_econ_events", "v_news_gate", "v_calendar_events", "v_fx_sar", "f_symbol_pick",
    "v_symbol_candles", "v_symbol_analyst", "v_symbol_news", "v_symbol_rating",
    "v_dive_signals", "v_dive_board", "v_dive_events",
    "v_top10_insight", "v_portfolio_insight",
    "v_outcome_engine", "v_outcome_calibration", "v_board_archive", "v_outcome_coverage",
]
EXPECTED_KINDS = (["FUNCTION"] * 6 + ["VIEW"] * 11 + ["TABLE FUNCTION"] * 2 + ["VIEW"] * 2
                  + ["TABLE FUNCTION"] + ["VIEW"] * 5 + ["TABLE FUNCTION"] + ["VIEW"] * 13)
# view -> the archive table whose header row its f_cell calls read
VIEW_SOURCE = {
    "v_board_episodes": "board_scorecard",
    "v_performance_log": "performance_log",
    "v_selection_log": "selection_log",
    "v_signal_history": "signal_history",
    "v_top10_board": "top_10_investments",
    "v_portfolio_decision": "portfolio_decision",
    "v_my_portfolio": "my_portfolio",
    "v_run_log": "run_log",
}


class FakeResp:
    def __init__(self, status: int, body: dict):
        self.status_code = status
        self._body = body

    def json(self):
        return self._body


class FakeSession:
    """Records every statement; answers the INFORMATION_SCHEMA read with the
    hashes of `live` (name -> body text) or of what --write sent."""

    def __init__(self, live=None, fail_on=None):
        self.sent = []
        self.live = dict(live or {})
        self.fail_on = fail_on

    def post(self, url, json=None, timeout=None):  # noqa: A002 - requests API
        q = (json or {}).get("query", "")
        if "INFORMATION_SCHEMA" in q:
            rows = [{"f": [{"v": n}, {"v": bv.sha256(b)}]} for n, b in self.live.items()]
            return FakeResp(200, {"rows": rows})
        self.sent.append((q, bool((json or {}).get("dryRun"))))
        if self.fail_on and self.fail_on in q:
            return FakeResp(400, {"error": {"message": "boom"}})
        for o in bv.load_objects():
            if o.render(PROJECT, DATASET) == q:
                self.live[o.name] = o.body(PROJECT, DATASET)
        return FakeResp(200, {"jobComplete": True})


def _bodies():
    return {o.name: o.body(PROJECT, DATASET) for o in bv.load_objects()}


def case_v1():
    objs = bv.load_objects()
    assert [o.name for o in objs] == EXPECTED_ORDER, [o.name for o in objs]
    kinds = [o.kind for o in objs]
    assert kinds == EXPECTED_KINDS, kinds
    assert bv.check_objects(objs) == [], bv.check_objects(objs)


def case_v2():
    headers = json.loads(FIXTURE.read_text(encoding="utf-8"))
    checked = 0
    for o in bv.load_objects():
        if o.name not in VIEW_SOURCE:
            continue
        cols = set(headers[VIEW_SOURCE[o.name]]["cells"])
        used = bv.header_names_used(o.template)
        assert used, f"{o.name}: no f_cell call found"
        missing = sorted(set(used) - cols)
        assert not missing, f"{o.name}: headers not in {VIEW_SOURCE[o.name]}: {missing}"
        checked += len(set(used))
    assert checked >= 150, checked


def case_v3():
    for o in bv.load_objects():
        r = o.render(PROJECT, DATASET)
        assert bv.PLACEHOLDER not in r and "{{" not in r, o.name
        assert f"`{PROJECT}.{DATASET}." in r, o.name
    for bad_project, bad_dataset in (("Bad Project", DATASET), (PROJECT, "a-b"),
                                     ("", DATASET), (PROJECT, "x`; DROP")):
        try:
            bv.render("`{{DS}}.x`", bad_project, bad_dataset)
        except ValueError:
            continue
        raise AssertionError(f"accepted {bad_project!r} / {bad_dataset!r}")


def case_v4():
    got = {n: bv.sha256(b) for n, b in _bodies().items()}
    pins = {**INSTALLED, **PENDING}
    assert not set(INSTALLED) & set(PENDING)
    assert got == pins, {n: (got.get(n), pins.get(n)) for n in set(got) | set(pins)
                         if got.get(n) != pins.get(n)}
    view = "x\nCREATE OR REPLACE VIEW `p.d.v`\nOPTIONS (description = \"d\")\nAS\nSELECT 1 AS a\nFROM t;\n"
    assert bv.object_body(view, "VIEW") == "SELECT 1 AS a\nFROM t"
    fn = "CREATE OR REPLACE FUNCTION `p.d.f`(s STRING)\nRETURNS STRING\nAS (\n  TRIM(s)\n)\nOPTIONS (description = \"d\");\n"
    assert bv.object_body(fn, "FUNCTION") == "TRIM(s)"
    tvf = ("CREATE OR REPLACE TABLE FUNCTION `p.d.t`(a DATE, b DATE)\nOPTIONS (description = \"d\")\nAS\n"
           "SELECT a AS x\nFROM u;\n")
    assert bv.object_body(tvf, "TABLE FUNCTION") == "SELECT a AS x\nFROM u"


def _write(tmp: Path, files: dict) -> Path:
    for name, text in files.items():
        (tmp / name).write_text(text, encoding="utf-8")
    return tmp


GOOD_FN = ("CREATE OR REPLACE FUNCTION `{{DS}}.f_a`(s STRING)\nRETURNS STRING\nAS (\n  TRIM(s)\n)\n"
           "OPTIONS (description = \"a\");\n")
GOOD_VIEW = ("CREATE OR REPLACE VIEW `{{DS}}.v_a`\nOPTIONS (description = \"a\")\nAS\n"
             "SELECT `{{DS}}.f_a`(tab) AS t FROM `{{DS}}.run_log`;\n")


GOOD_TVF = ("CREATE OR REPLACE TABLE FUNCTION `{{DS}}.t_a`(start_date DATE, end_date DATE)\n"
            "OPTIONS (description = \"a\")\nAS\nSELECT t FROM `{{DS}}.v_a` WHERE start_date <= end_date;\n")


def case_v5():
    with tempfile.TemporaryDirectory() as d:
        ok = bv.check_objects(bv.load_objects(_write(Path(d), {"00_f_a.sql": GOOD_FN, "10_v_a.sql": GOOD_VIEW})))
        assert ok == [], ok
    with tempfile.TemporaryDirectory() as d:
        objs = bv.load_objects(_write(Path(d), {"00_f_a.sql": GOOD_FN, "10_v_a.sql": GOOD_VIEW,
                                                "20_t_a.sql": GOOD_TVF}))
        assert [o.kind for o in objs] == ["FUNCTION", "VIEW", "TABLE FUNCTION"]
        assert bv.check_objects(objs) == [], bv.check_objects(objs)
    with tempfile.TemporaryDirectory() as d:
        src = GOOD_VIEW.replace("`{{DS}}.run_log`", "`{{DS}}.market_series_daily`")
        assert bv.check_objects(bv.load_objects(_write(Path(d), {"00_f_a.sql": GOOD_FN, "10_v_a.sql": src}))) == []
    bad_sets = {
        "forbidden": {"00_f_a.sql": GOOD_FN,
                      "10_v_a.sql": GOOD_VIEW.replace("SELECT", "DELETE FROM `{{DS}}.run_log` WHERE TRUE; SELECT")},
        "hard-coded": {"00_f_a.sql": GOOD_FN,
                       "10_v_a.sql": GOOD_VIEW.replace("`{{DS}}.run_log`", "`project-2e3530e6-2311-4aa3-b36.tfb_archive.run_log`")},
        "before it is defined": {"00_f_a.sql": GOOD_FN,
                                 "10_v_a.sql": GOOD_VIEW.replace("`{{DS}}.run_log`", "`{{DS}}.v_later`")},
        "exactly one statement": {"00_f_a.sql": GOOD_FN, "10_v_a.sql": GOOD_VIEW + "SELECT 2;\n"},
        "functions must come before": {"00_v_a.sql": GOOD_VIEW.replace("`{{DS}}.f_a`(tab)", "tab"),
                                       "10_f_a.sql": GOOD_FN},
        "file name says": {"00_f_a.sql": GOOD_FN, "10_v_b.sql": GOOD_VIEW},
        "forbidden statement 'CREATE OR REPLACE TABLE'": {
            "00_f_a.sql": GOOD_FN,
            "10_v_a.sql": GOOD_VIEW.replace("CREATE OR REPLACE VIEW `{{DS}}.v_a`",
                                            "CREATE OR REPLACE VIEW `{{DS}}.v_a`\n-- x\nCREATE OR REPLACE TABLE `{{DS}}.t` AS SELECT 1;")},
        "references nope_table before it is defined": {
            "00_f_a.sql": GOOD_FN, "10_v_a.sql": GOOD_VIEW.replace("`{{DS}}.run_log`", "`{{DS}}.nope_table`")},
        "functions must come before every view and table function": {
            "00_v_a.sql": GOOD_VIEW.replace("`{{DS}}.f_a`(tab)", "tab"), "10_t_a.sql": GOOD_TVF, "20_f_a.sql": GOOD_FN},
    }
    for needle, files in bad_sets.items():
        with tempfile.TemporaryDirectory() as d:
            problems = bv.check_objects(bv.load_objects(_write(Path(d), files)))
            assert any(needle in p for p in problems), (needle, problems)


def _no_key_env():
    saved = {k: os.environ.pop(k) for k in _KEY_ENV if k in os.environ}
    return saved


def _restore_env(saved):
    for k in _KEY_ENV:
        os.environ.pop(k, None)
    os.environ.update(saved)


def case_v6():
    import io
    import contextlib

    saved = _no_key_env()
    try:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(io.StringIO()):
            rc = bv.main([])
    finally:
        _restore_env(saved)
    assert rc == 0 and "nothing created" in buf.getvalue(), (rc, buf.getvalue())
    assert "static checks only" in buf.getvalue(), buf.getvalue()

    real = bv.make_session
    try:
        def no_creds():
            raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS missing or unreadable")
        bv.make_session = no_creds
        with contextlib.redirect_stderr(io.StringIO()):
            assert bv.main(["--write"]) == 2
            assert bv.main(["--verify"]) == 2
    finally:
        bv.make_session = real

    s = FakeSession()
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        rc = bv.main(["--write"], session=s)
    assert rc == 0, rc
    names_sent = []
    for q, dry in s.sent:
        assert not dry
        for o in bv.load_objects():
            if o.render(PROJECT, DATASET) == q:
                names_sent.append(o.name)
    assert names_sent == EXPECTED_ORDER, names_sent

    s = FakeSession(fail_on="v_selection_log")
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        assert bv.main(["--write"], session=s) == 1
    assert len(s.sent) == EXPECTED_ORDER.index("v_selection_log") + 1

    bodies = _bodies()
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        assert bv.main(["--verify"], session=FakeSession(live=bodies)) == 0
    changed = dict(bodies, v_run_log=bodies["v_run_log"] + " ")
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
        assert bv.main(["--verify"], session=FakeSession(live=changed)) == 1
    assert "mismatch: v_run_log" in out.getvalue(), out.getvalue()
    missing = {k: v for k, v in bodies.items() if k != "f_ts"}
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
        assert bv.main(["--verify"], session=FakeSession(live=missing)) == 1
    assert "f_ts" in out.getvalue()


def case_v7():
    wf = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    on = wf.get(True) or wf.get("on")
    assert set(on.keys()) == {"workflow_dispatch"}, on
    mode = on["workflow_dispatch"]["inputs"]["mode"]
    assert mode["default"] == "verify" and mode["options"] == ["verify", "write"], mode
    assert wf["permissions"] == {"contents": "read"}, wf["permissions"]
    job = wf["jobs"]["views"]
    assert int(job["timeout-minutes"]) <= 20
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "scripts/apply_bq_views.py" in text
    assert '--${MODE}' in text or "--$MODE" in text
    assert "actions/checkout@v6" in text and "actions/setup-python@v6" in text
    assert "schedule" not in text.split("on:", 1)[1].split("concurrency:", 1)[0]


def case_v8():
    script = (ROOT / "scripts" / "apply_bq_views.py").read_text(encoding="utf-8")
    assert "sheets.googleapis" not in script and "spreadsheets" not in script.replace("GOOGLE_SHEETS_CREDENTIALS", "")
    for o in bv.load_objects():
        assert o.kind in ("VIEW", "FUNCTION", "TABLE FUNCTION")
        assert "sheets.googleapis" not in o.template


def _stance_lists(template):
    import re
    out = {}
    for label in ("bullish", "bearish"):
        m = re.search(r"WHEN entry_recommendation IN \(([^)]*)\) THEN '" + label + "'", template)
        assert m, label
        out[label] = set(re.findall(r"'([^']+)'", m.group(1)))
    m = re.search(r"WHEN entry_recommendation = '([^']+)' THEN 'neutral'", template)
    out["neutral"] = {m.group(1)} if m else set()
    return out


def case_v9():
    import re
    src = (ROOT / "scripts" / "track_performance.py").read_text(encoding="utf-8")
    block = src.split("_RECO_TRADE_SIGN: Dict[str, int] = {", 1)[1].split("}", 1)[0]
    sign = {name.replace("_", " "): int(v) for name, v in
            re.findall(r"RecommendationType\.([A-Z_]+)\.value:\s*(-?\d+)", block)}
    aliases = src.split("_RECO_TRADE_SIGN_ALIASES: Dict[str, int] = {", 1)[1].split("}", 1)[0]
    sign.update({k: int(v) for k, v in re.findall(r'"([A-Z ]+)":\s*(-?\d+)', aliases)})
    assert sign.get("STRONG SELL") == -1 and sign.get("HOLD") == 0, sign
    tpl = [o for o in bv.load_objects() if o.name == "v_performance_log"][0].template
    lists = _stance_lists(tpl)
    assert lists["bullish"] == {k for k, v in sign.items() if v == 1}, (lists, sign)
    assert lists["bearish"] == {k for k, v in sign.items() if v == -1}, (lists, sign)
    assert lists["neutral"] == {k for k, v in sign.items() if v == 0}, (lists, sign)


def case_v10():
    import io
    import contextlib

    saved = _no_key_env()
    real = bv.make_session
    try:
        os.environ["GOOGLE_SHEETS_CREDENTIALS"] = "{}"
        s = FakeSession()
        bv.make_session = lambda: s
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(io.StringIO()):
            assert bv.main(["--dry-run"]) == 0
        assert "BigQuery dry-run" in buf.getvalue(), buf.getvalue()
        assert [d for _, d in s.sent] == [True] * len(EXPECTED_ORDER), s.sent
        names = [o.name for q, _ in s.sent for o in bv.load_objects() if o.render(PROJECT, DATASET) == q]
        assert names == EXPECTED_ORDER, names

        def broken():
            raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS missing or unreadable")
        bv.make_session = broken
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            assert bv.main([]) == 2
    finally:
        bv.make_session = real
        _restore_env(saved)


def case_v11():
    import re
    import archive_market_series as ms
    objs = {o.name: o for o in bv.load_objects()}
    schema = [f["name"] for f in ms.SCHEMA]
    daily = objs["v_market_daily"].template
    first = daily.split("WITH s AS (", 1)[1].split("LOGICAL_AND", 1)[0]
    cols = [c.strip() for c in first.replace("SELECT", "").split(",") if c.strip()]
    assert cols and set(cols) <= set(schema), sorted(set(cols) - set(schema))
    assert set(bv._REF.findall(bv._code(daily))) == {"market_series_daily", "v_market_daily"}
    assert ms.TABLE == "market_series_daily" and ms.TABLE in bv.SOURCE_TABLES
    order = [o.name for o in bv.load_objects()]
    for name in ("f_market_window", "f_market_summary"):
        tpl = objs[name].template
        assert objs[name].kind == "TABLE FUNCTION"
        assert re.search(r"`\{\{DS\}\}\." + name + r"`\(start_date DATE, end_date DATE\)", tpl), name
        refs = set(bv._REF.findall(bv._code(tpl))) - {name}
        assert refs and all(order.index(r) < order.index(name) for r in refs), (name, refs)
        assert "@DS_START_DATE" in tpl and "@DS_END_DATE" in tpl   # the Looker custom query, documented


def case_v12():
    import re
    src = (ROOT / "scripts" / "track_performance.py").read_text(encoding="utf-8")
    # the summary path loads Entry Recommendation with a HOLD default, so the
    # EXIT / TRIM aliases of _reco_trade_sign never reach its scoring
    assert "rec = _parse_enum_value(RecommendationType, rec_raw, RecommendationType.HOLD)" in src
    assert "return _directional_outcome(getattr(r, \"entry_recommendation\", None)," in src
    block = src.split("_RECO_TRADE_SIGN: Dict[str, int] = {", 1)[1].split("}", 1)[0]
    sign = {name.replace("_", ""): int(v) for name, v in
            re.findall(r"RecommendationType\.([A-Z_]+)\.value:\s*(-?\d+)", block)}
    assert sign["STRONGSELL"] == -1 and sign["HOLD"] == 0 and len(sign) == 8, sign
    tpl = [o for o in bv.load_objects() if o.name == "v_performance_log"][0].template
    code = bv._code(tpl)
    lists = {}
    for won, label in (("> 0", "long"), ("< 0", "short")):
        m = re.search(r"IN \(([^)]*)\)\s*THEN CASE WHEN realized_roi_pp " + re.escape(won) + r" THEN 'WIN'", code)
        assert m, label
        lists[label] = set(re.findall(r"'([^']+)'", m.group(1)))
    assert lists["long"] == {k for k, v in sign.items() if v == 1}, lists
    assert lists["short"] == {k for k, v in sign.items() if v == -1}, lists
    assert not ({"HOLD", "EXIT", "TRIM"} & (lists["long"] | lists["short"]))
    assert "WHEN IFNULL(status, '') != 'matured' OR realized_roi_pp IS NULL THEN NULL" in code
    assert "IFNULL(call_outcome IN ('WIN', 'LOSS'), FALSE) AS is_scored" in code
    assert "CASE WHEN call_outcome = 'WIN' THEN 1 WHEN call_outcome = 'LOSS' THEN 0 END AS win_flag" in code
    assert "PARTITION BY record_key, matured_valid" in code
    # golden negative: the first cut scored the sheet's PRICE label
    assert "outcome IN ('WIN', 'LOSS', 'BREAKEVEN') AS is_scored" not in code
    assert "WHEN outcome = 'WIN' THEN 1" not in code


def case_v13():
    import re
    gs = (ROOT / "apps_script" / "10_My_Portfolio.gs").read_text(encoding="utf-8")
    fn = gs.split("function pdIbkrNote_(", 1)[1].split("\nfunction ", 1)[0]
    notes = {
        "SENT": "' | ibkr evidence SENT ('",
        "NOT SENT": "' | ibkr evidence NOT SENT: '",
        "WOULD SEND (observe)": "' | [ibkr-observe] would send ('",
        "WOULD NOT SEND (observe)": "' | [ibkr-observe] would not send: '",
    }
    for literal in notes.values():
        assert literal in fn, literal
    tpl = [o for o in bv.load_objects() if o.name == "v_portfolio_decision"][0].template
    rules = re.findall(r"WHEN REGEXP_CONTAINS\(st\.status_text, r'([^']+)'\) THEN '([^']+)'", tpl)
    assert len(rules) == 4, rules

    def label(text):
        for pattern, value in rules:
            if re.search(pattern, text):
                return value
        return None

    head = "Portfolio Decision | status: ok | Last run 2026-10-29 10:00:00"
    for want, literal in notes.items():
        text = head + literal.strip("'") + "capture 2026-10-29 09:58 age 2m; linked 9/9; cash 1,234.00 SAR; USD 3.75)"
        assert label(text) == want, (want, label(text))
    assert label(head) is None
    # golden negative: the first cut's pattern never matched the SENT note
    sent = head + notes["SENT"].strip("'") + "capture 2026-10-29 09:58 age 2m)"
    assert re.search(r"ibkr evidence ([A-Z ]+?)[:|]", sent) is None


def _py_list(path, name):
    """A module-level list literal (NAME = [...]) read with ast - no import."""
    import ast
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == name for t in node.targets):
            return list(ast.literal_eval(node.value))
    raise AssertionError(f"{name} not in {path}")


MARKET_PAGES = ("Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX")
HELPERS = {"f_cell", "f_num", "f_ts", "f_col", "f_at", "f_ts_utc"}
NEW_SOURCES = {
    "v_universe": {"market_leaders", "global_markets", "mutual_funds", "commodities_fx"},
    "v_news_items": {"news_items"}, "v_econ_events": {"econ_events"},
    "v_news_gate": {"news_gate"}, "v_calendar_events": {"calendar_events"},
}
# listing suffixes on the four market pages of the live workbook, 31 Oct 2026
LIVE_SUFFIXES_20261031 = (
    "US", "T", "HK", "L", "TO", "AX", "DE", "NS", "PA", "KS", "ST", "TW", "IS", "BK", "JK", "SW", "MI",
    "MC", "MX", "AS", "OL", "VN", "SI", "JO", "CO", "KL", "SN", "AT", "BR", "WA", "HE", "QA", "NZ",
    "BA", "VI", "LS", "AE", "BH", "SR")


def _registry():
    sys.path.insert(0, str(ROOT))
    from core.sheets import schema_registry as reg
    return reg


def case_v14():
    import archive_to_bigquery as arc
    reg = _registry()
    objs = {o.name: o for o in bv.load_objects()}
    gate = "scripts/news_macro_gate.py"
    writers = {
        "v_news_items": _py_list(gate, "ITEMS_HEADER"),
        "v_news_gate": _py_list(gate, "GATE_HEADER"),
        "v_econ_events": _py_list(gate, "ECON_HEADER"),
        "v_calendar_events": (_py_list("scripts/run_calendar_sync.py", "HEADERS")
                              + _py_list("scripts/run_calendar_sync.py", "LAST_REPORT_HEADERS")),
    }
    pages = [reg.get_sheet_headers(p) for p in MARKET_PAGES]
    assert all(h == pages[0] for h in pages) and len(pages[0]) == 115
    used = bv.header_names_used(objs["v_universe"].template)
    assert len(used) == len(set(used)) == 115, len(used)        # every column, looked up once
    assert sorted(used) == sorted(pages[0])
    for name, header in writers.items():
        got = bv.header_names_used(objs[name].template)
        assert got and not set(got) - set(header), (name, sorted(set(got) - set(header)))
    # the tables they read: exactly the tabs archive v1.1.0 adds, header row 1
    added = {arc.table_name(t): h for t, h in arc.TABS[10:]}
    assert set(added) == {"market_leaders", "global_markets", "mutual_funds", "commodities_fx",
                          "news_items", "news_gate", "econ_events"} and set(added.values()) == {1}
    assert {arc.table_name(t) for t, _ in arc.TABS} == bv.ARCHIVE_TABLES
    for name, tables in NEW_SOURCES.items():
        refs = set(bv._REF.findall(bv._code(objs[name].template))) - {name}
        assert refs - HELPERS == tables, (name, refs)


def case_v15():
    import re
    import archive_market_series as ms
    reg = _registry()
    code = bv._code({o.name: o for o in bv.load_objects()}["v_universe"].template)
    alias_of = dict(re.findall(r"\.f_col`\(h, '([^']+)'\) AS i_([a-z0-9_]+)", code))
    times100 = set(re.findall(
        r"ROUND\(`\{\{DS\}\}\.f_num`\(`\{\{DS\}\}\.f_at`\(s\.cells, i_([a-z0-9_]+)\)\) \* 100, 4\)", code))
    dtype = {c.header: c.dtype for c in reg.get_sheet_columns("Global_Markets")}
    derived = {"Percent Change": "change_pct", "52W Position %": "position_52w_pct",
               "Expected ROI 1M": "expected_roi_1m_pct", "Expected ROI 3M": "expected_roi_3m_pct",
               "Expected ROI 12M": "expected_roi_12m_pct", "Upside %": "intrinsic_upside_pct",
               "Upside/Downside %": "target_upside_pct"}
    pct = {h for h, d in dtype.items() if d == "pct"}
    assert len(pct) == 17, sorted(pct)
    for h in pct:               # a fraction column is read x 100 or recomputed from prices
        assert alias_of[h] in times100 or h in derived, h
    for h, col in derived.items():
        assert f" AS {col}" in code, col
    for h, a in alias_of.items():
        if a in times100:       # golden negative: a 0-100 points column is never x 100
            assert dtype[h] == "pct", (h, dtype[h])
    assert alias_of["Data Quality Score"] == "data_quality" and "data_quality" not in times100
    region_of = {}
    for body, region in re.findall(r"WHEN k\.market_code IN \(([^)]*)\) THEN '([^']+)'", code):
        for sfx in re.findall(r"'([^']+)'", body):
            assert sfx not in region_of, sfx
            region_of[sfx] = region
    countries = dict(re.findall(r"WHEN '([A-Z]+)' THEN '([^']+)'", code))
    for sfx in LIVE_SUFFIXES_20261031:
        assert sfx in region_of and sfx in countries, sfx
    assert set(region_of) == set(countries)
    assert (region_of["SR"], region_of["US"], region_of["T"], region_of["HK"], region_of["L"]) == \
        ("Gulf", "US", "Japan", "China", "Europe")
    named = set(region_of.values()) | set(re.findall(r"THEN '([A-Za-z]+)'\n", code))
    assert set(ms.REGIONS) - {"World"} <= named, set(ms.REGIONS) - named   # same names as the market series


def case_v16():
    import re
    objs = {o.name: o for o in bv.load_objects()}
    order = [o.name for o in bv.load_objects()]
    for name, sig, docs in (
            ("f_universe_return", "(start_date DATE, end_date DATE)", ("@DS_START_DATE", "@DS_END_DATE")),
            ("f_symbol_pick", "(q STRING, market STRING)", ("(@q, @market)", "WHERE pick_order = 1)"))):
        tpl = objs[name].template
        assert objs[name].kind == "TABLE FUNCTION"
        assert f"`{{{{DS}}}}.{name}`{sig}" in tpl, name
        refs = set(bv._REF.findall(bv._code(tpl))) - {name}
        assert refs and all(order.index(r) < order.index(name) for r in refs), (name, refs)
        for doc in docs:
            assert doc in tpl, (name, doc)
    pick = bv._code(objs["f_symbol_pick"].template)
    assert [int(x) for x in re.findall(r"THEN (\d)\n", pick)] == [1, 2, 3, 4, 5, 6, 7]
    assert re.search(r"QUALIFY pick_order <= 20;?$", pick.strip()) and "a.um IN ('', 'ALL', 'ANY')" in pick
    ret = bv._code(objs["f_universe_return"].template)
    assert "snapshot_date < start_date" in ret and "snapshot_date <= end_date" in ret
    assert "before_start IS NOT NULL AS covers_window" in ret


def case_v17():
    import re
    objs = {o.name: o for o in bv.load_objects()}
    for name in ("v_universe_latest", "f_universe_return", "v_top10_insight", "v_portfolio_insight",
                 "f_symbol_pick"):
        refs = set(bv._REF.findall(bv._code(objs[name].template))) - {name}
        assert refs and not refs & bv.SOURCE_TABLES, (name, refs & bv.SOURCE_TABLES)   # typed views only
    perf = bv._code(objs["v_performance_log"].template)
    bearish = set(re.findall(r"'([^']+)'", re.search(
        r"WHEN entry_recommendation IN \(([^)]*)\) THEN 'bearish'", perf).group(1)))
    pf = bv._code(objs["v_portfolio_insight"].template)
    said = set(re.findall(r"'([^']+)'", re.search(
        r"IF\(UPPER\(REPLACE\(u\.recommendation, '_', ' '\)\) IN \(([^)]*)\)", pf).group(1)))
    assert said == bearish, (said, bearish)
    cell, col, at = (bv._code(objs[n].template) for n in ("f_cell", "f_col", "f_at"))
    for frag in ("FROM UNNEST(h) AS x WITH OFFSET AS o", "WHERE TRIM(x) = name", "ORDER BY o", "LIMIT 1"):
        assert frag in cell and frag in col, frag
    assert "NULLIF(NULLIF(TRIM(c[SAFE_OFFSET(o)]), ''), '—')" in cell
    assert "NULLIF(NULLIF(TRIM(c[SAFE_OFFSET(i)]), ''), '—')" in at
    for name in ("v_top10_insight", "v_portfolio_insight"):
        code = bv._code(objs[name].template)
        labels = re.findall(r", '([^']+)', NULL\)", code)
        assert len(labels) >= 8 and all(re.fullmatch(r"[A-Za-z0-9 /.-]+", x) for x in labels), labels
        assert "ARRAY_TO_STRING(j.flags, ' · ') AS caution_flags" in code
        assert "ARRAY_LENGTH(j.flags) AS caution_count" in code
        assert "IF(u.snapshot_age_days > 1, CONCAT('market copy '" in code
    latest = bv._code(objs["v_universe_latest"].template)
    assert "DATE_DIFF(CURRENT_DATE(), r.snapshot_date, DAY) AS snapshot_age_days" in latest
    t10 = bv._code(objs["v_top10_insight"].template)
    assert "AND b.snapshot_date = (SELECT MAX(snapshot_date) FROM `{{DS}}.v_top10_status`)" in t10
    assert "MAX(b.snapshot_date) OVER ()" not in t10          # golden: the last FULL board
    assert "LEFT JOIN pd AS d ON d.symbol = h.symbol AND d.snapshot_date = h.snapshot_date" in pf
    assert "MAX(d.snapshot_date) OVER ()" not in pf           # golden: an older night's actions


def case_v18():
    import re
    import archive_symbol_deep as asd
    objs = {o.name: o for o in bv.load_objects()}
    code = {n: bv._code(objs[n].template) for n in objs}
    assert bv.DEEP_TABLES == set(asd.TABLES), (bv.DEEP_TABLES, set(asd.TABLES))
    cols = {t: {f["name"] for f in spec[0]} for t, spec in asd.TABLES.items()}
    # each view reads only the declared columns of its table
    bars_cte = re.search(r"WITH bars AS \(\s*SELECT ([^\n]+)\n\s*FROM `\{\{DS\}\}\.symbol_bars_daily`",
                         code["v_symbol_candles"]).group(1)
    assert {c.strip() for c in bars_cte.split(",")} <= cols["symbol_bars_daily"]
    assert set(re.findall(r"\bt\.([a-z_]+)", code["v_symbol_analyst"])) <= cols["symbol_analyst_daily"]
    assert set(re.findall(r"\be\.([a-z_]+)", code["v_symbol_news"])) <= cols["symbol_news"]
    used = {n: set(bv._REF.findall(c)) & bv.DEEP_TABLES for n, c in code.items()}
    assert {n: t for n, t in used.items() if t} == {
        "v_symbol_candles": {"symbol_bars_daily"}, "v_symbol_analyst": {"symbol_analyst_daily"},
        "v_symbol_news": {"symbol_news"},
        "v_outcome_engine": {"symbol_bars_daily"}}, used          # v1.5.0: the engine reads the bars too
    assert set(re.findall(r"\bb\.([a-z_]+)", code["v_outcome_engine"])) <= cols["symbol_bars_daily"]
    # every picker view carries symbol_label
    for n in ("v_symbol_candles", "v_symbol_analyst", "v_symbol_news", "v_symbol_rating",
              "v_dive_signals", "v_dive_board", "v_dive_events", "v_top10_insight", "v_portfolio_insight"):
        assert re.search(r"\bsymbol_label\b", code[n]), n
    # the TFB rating: six weights summing to 1, in the score and the coverage alike
    rating = code["v_symbol_rating"]
    present = {k: float(w) for w, k in re.findall(r"([0-9.]+) \* IF\(c\.part_([a-z]+) IS NULL", rating)}
    summed = {k: float(w) for w, k in re.findall(r"([0-9.]+) \* IFNULL\(c\.part_([a-z]+), 0\)", rating)}
    assert present == summed and set(present) == {"engine", "analysts", "target", "momentum", "news", "risk"}
    assert abs(sum(present.values()) - 1.0) < 1e-9, present
    cuts = [float(x) for x in re.findall(r"WHEN r\.tfb_rating_score >= (\d+) THEN '", rating)]
    stars = [int(x) for x in re.findall(r"WHEN r\.tfb_rating_score >= \d+ THEN (\d)\n", rating)]
    assert cuts == [75, 60, 45, 30] * 2 and stars == [5, 4, 3, 2]      # label and display agree
    assert "ELSE 'low'" in rating and "r.meets_floors THEN 'high'" in rating
    # the analyst consensus is recomputed from the counts, 5 = Strong Buy
    assert "SAFE_DIVIDE(5 * IFNULL(t.strong_buy, 0) + 4 * IFNULL(t.buy, 0)" in code["v_symbol_analyst"]
    # both evidence views carry the rating; the portfolio reads STRONG_SELL too
    for n in ("v_top10_insight", "v_portfolio_insight"):
        for col in ("rt.tfb_rating_score", "rt.tfb_rating_label", "rt.analyst_consensus_label",
                    "'analyst consensus below Hold'"):
            assert col in code[n], (n, col)
    assert "UPPER(u.recommendation) IN" not in code["v_portfolio_insight"]    # golden: STRONG_SELL missed


SUN_THU = {"Saudi Arabia", "Qatar", "Kuwait", "Bahrain", "Oman", "Egypt"}


def case_v19():
    import datetime as dt
    import re
    import zoneinfo
    objs = {o.name: o for o in bv.load_objects()}
    candles = bv._code(objs["v_symbol_candles"].template)
    universe = bv._code(objs["v_universe"].template)
    # 1. a symbol with bars takes no snapshot candle at all (not only on the bars' dates)
    anti = re.findall(r"NOT EXISTS \(SELECT 1 FROM bars AS b WHERE ([^)]*)\)", candles)
    assert anti == ["b.symbol = u.symbol"], anti
    assert "u.snapshot_date AS date" not in candles                  # golden: the capture date
    # 2. the venue table covers every country v_universe names, once, with real zones
    block = re.search(r"CASE k\.market_code\n(.*?)END AS market_country", universe, re.DOTALL).group(1)
    countries = set(re.findall(r"THEN '([^']+)'", block))
    venue = re.findall(r"\('([A-Za-z ]+)', '([A-Za-z_/]+)', (\d+), (TRUE|FALSE)\)", candles)
    names = [v[0] for v in venue]
    assert len(names) == len(set(names)) and set(names) == countries, sorted(set(names) ^ countries)
    for name, tz, minute, sun_thu in venue:
        zoneinfo.ZoneInfo(tz)                                         # raises on an unknown zone
        assert 420 <= int(minute) <= 720, (name, minute)              # 07:00 .. 12:00 local
    assert {v[0] for v in venue if v[3] == "TRUE"} == SUN_THU
    # 3. the session date: the venue-local Last Updated stamp less the opening minute
    assert ("DATE(DATETIME_SUB(DATETIME(COALESCE(u.last_updated_at, u.captured_at), COALESCE(v.tz, 'UTC')),"
            "\n                      INTERVAL COALESCE(v.open_min, 0) MINUTE)) AS local_day") in candles
    # 4. the weekend step-back, simulated for every day of the week (BigQuery DAYOFWEEK 1 = Sunday)
    step = {int(d): (int(a), int(b)) for d, a, b in
            re.findall(r"WHEN (\d) THEN IF\(s\.sun_thu, (\d), (\d)\)", candles)}
    assert set(step) == {1, 6, 7}, step
    for sun_thu, trading in ((True, {6, 0, 1, 2, 3}), (False, {0, 1, 2, 3, 4})):   # Python weekday()
        for day in range(1, 8):
            local = dt.date(2026, 11, day)                            # Sun 1 .. Sat 7 Nov 2026
            bq_dow = local.isoweekday() % 7 + 1
            back = step.get(bq_dow, (0, 0))[0 if sun_thu else 1]
            session = local - dt.timedelta(days=back)
            assert session.weekday() in trading, (sun_thu, local, session)
            if local.weekday() in trading:
                assert session == local, (sun_thu, local)
    # 5. one candle per session - the latest refresh - then the holiday copies drop
    assert "PARTITION BY symbol, date ORDER BY quoted_at DESC, captured_at DESC, source_page) = 1" in candles
    assert "FROM snap2 AS s" in candles and "WHERE prev_k IS NULL OR k != prev_k" in candles


def _window_last(rows, win, ignore_nulls):
    """LAST_VALUE(close) per row as BigQuery evaluates it under the window text `win`
    (PARTITION BY / ORDER BY / an optional ROWS frame; no frame means the default,
    which ends at the current row)."""
    import re
    part = re.search(r"PARTITION BY m\.([a-z_]+)", win)
    order = re.search(r"ORDER BY m\.([a-z_]+)( DESC)?", win)
    frame = re.search(r"ROWS BETWEEN UNBOUNDED PRECEDING AND (UNBOUNDED FOLLOWING|CURRENT ROW)", win)
    assert order, win
    assert not re.search(r"\bRANGE\b|\d+ (PRECEDING|FOLLOWING)", win), win   # outside this model
    out = []
    for r in rows:
        group = sorted((x for x in rows if not part or x[part.group(1)] == r[part.group(1)]),
                       key=lambda x: x[order.group(1)], reverse=bool(order.group(2)))
        upto = group if frame and frame.group(1) == "UNBOUNDED FOLLOWING" else group[:group.index(r) + 1]
        vals = [x["close"] for x in upto if x["close"] is not None or not ignore_nulls]
        out.append(vals[-1] if vals else None)
    return out


def case_v20():
    import re
    objs = {o.name: o for o in bv.load_objects()}
    candles = bv._code(objs["v_symbol_candles"].template)
    win = re.findall(r"\bwall AS \(([^)]*)\)", candles)
    assert len(win) == 1, win
    last = re.findall(r"LAST_VALUE\(m\.close( IGNORE NULLS)?\) OVER wall", candles)
    assert len(last) == 5 and len(set(last)) == 1, last           # last_close and the four columns
    assert "LAST_VALUE(m.close IGNORE NULLS) OVER wall AS last_close" in candles
    for col in ("open", "high", "low", "close"):
        assert (f"ROUND((SAFE_DIVIDE(m.{col}, LAST_VALUE(m.close IGNORE NULLS) OVER wall) - 1) * 100, 4)"
                f" AS {col}_vs_last_pct") in candles, col
    # simulate the window on two symbols with interleaved dates (close is REQUIRED in the bars
    # and > 0 in the snapshots; the NULL row only checks that the reference skips one)
    rows = [{"symbol": "AAA", "date": 1, "close": 10.0}, {"symbol": "BBB", "date": 1, "close": 100.0},
            {"symbol": "AAA", "date": 2, "close": 11.0}, {"symbol": "BBB", "date": 3, "close": 90.0},
            {"symbol": "AAA", "date": 4, "close": 12.0}, {"symbol": "AAA", "date": 5, "close": None}]
    ref = _window_last(rows, win[0], bool(last[0]))
    assert ref == [12.0, 90.0, 12.0, 90.0, 12.0, 12.0], ref
    pct = [round((r["close"] / x - 1) * 100, 4) if r["close"] is not None else None for r, x in zip(rows, ref)]
    assert pct[4] == 0.0 and pct[3] == 0.0 and pct[0] == -16.6667 and pct[1] == 11.1111, pct
    # golden: the default frame (to the current row) makes every close its own reference - a flat 0
    assert _window_last(rows, "PARTITION BY m.symbol ORDER BY m.date", True)[0] == 10.0


def _fx_rules(code):
    """The quote rules v_fx_sar applies, parsed from its SQL."""
    import re
    direct = re.search(r"REGEXP_EXTRACT\(symbol, r'\^\(\[A-Z\]\{3\}\)USD=X\$'\) AS ccy, price AS usd_per_unit, 'direct'", code)
    inverse = re.search(r"REGEXP_EXTRACT\(symbol, r'\^\(\[A-Z\]\{3\}\)=X\$'\) AS ccy, 1 / price AS usd_per_unit, 'inverse'", code)
    prefer = re.search(r"ORDER BY IF\(quote = 'direct', 0, 1\)\) = 1", code)
    pence = re.search(r"r\.rate_to_sar / (\d+), CONCAT\('GBP/100 via ', r\.fx_source\)", code)
    return direct, inverse, prefer, pence


def _fx_table(quotes, prefer_direct=True, pence_div=100):
    """What v_fx_sar makes of a Commodities_FX page: quotes is {symbol: price}."""
    usd_sar = quotes.get("SAR=X")
    to_usd = {}
    for sym, px in quotes.items():
        if sym.endswith("USD=X") and len(sym) == 8:
            to_usd.setdefault(sym[:3], []).append(("direct", px))
        elif sym.endswith("=X") and len(sym) == 5 and sym != "SAR=X":
            to_usd.setdefault(sym[:3], []).append(("inverse", 1 / px))
    rates = {"SAR": 1.0, "USD": usd_sar}
    for ccy, cands in to_usd.items():
        if ccy in ("SAR", "USD"):
            continue
        cands = sorted(cands, key=lambda c: 0 if (c[0] == "direct") == prefer_direct else 1)
        rates[ccy] = cands[0][1] * usd_sar
    if "GBP" in rates:
        rates["GBX"] = rates["GBp"] = rates["GBP"] / pence_div
    return rates


def case_v21():
    objs = {o.name: o for o in bv.load_objects()}
    code = bv._code(objs["v_fx_sar"].template)
    assert "source_page = 'Commodities_FX' AND asset_class = 'FX' AND current_price > 0" in code
    assert set(bv._REF.findall(code)) - {"v_fx_sar"} == {"v_universe_latest"}, bv._REF.findall(code)
    direct, inverse, prefer, pence = _fx_rules(code)
    assert direct and inverse and prefer, "the dollar-cross rules"
    assert pence and pence.group(1) == "100", "pence are a hundredth of a pound"
    assert "WHERE symbol = 'SAR=X'" in code and "symbol != 'SAR=X'" in code
    # the live page of 1 Nov 2026: a direct quote, an inverse quote, both, and pence
    quotes = {"SAR=X": 3.7526, "GBPUSD=X": 1.3393, "HKDUSD=X": 0.12748, "HKD=X": 7.8442,
              "EGP=X": 55.95, "AEDHKD=X": 2.13, "EURUSD=X": 1.11657}
    t = _fx_table(quotes, prefer_direct=prefer is not None, pence_div=int(pence.group(1)))
    assert t["USD"] == 3.7526 and t["SAR"] == 1.0
    assert abs(t["GBP"] - 5.0259) < 1e-3 and abs(t["GBX"] - 0.050259) < 1e-5 and t["GBp"] == t["GBX"]
    assert abs(t["HKD"] - 0.12748 * 3.7526) < 1e-6, "the direct quote wins over the inverse"
    assert abs(t["EGP"] - 3.7526 / 55.95) < 1e-6, "an inverse-only quote"
    assert "AED" not in t, "AEDHKD=X is not a dollar cross"
    # golden: the inverse quote read as direct puts HKD at 29 riyals
    wrong = _fx_table({k: v for k, v in quotes.items() if k != "HKDUSD=X"}, prefer_direct=True)
    assert abs(wrong["HKD"] - 3.7526 / 7.8442) < 1e-6 and abs(t["HKD"] - wrong["HKD"]) < 1e-3


def _weights(values, whole_book=True):
    """weight_sar_pct over a book: with whole_book (1.4.1) every weight is None while any
    value is None; without it (1.4.0) the valued subset is shared out as if it were the book."""
    missing = sum(1 for v in values if v is None)
    if whole_book and missing:
        return [None] * len(values), missing
    total = sum(v for v in values if v is not None)
    return [round(v / total * 100, 2) if v is not None else None for v in values], missing


def case_v22():
    import re
    objs = {o.name: o for o in bv.load_objects()}
    code = {n: bv._code(objs[n].template) for n in objs}
    rating = code["v_symbol_rating"]
    # the gate names the engine verdict, in this order, and never reads the score
    gate = re.search(r"CASE\n((?:(?!CASE\n).)*?)END AS rating_gate", rating, re.DOTALL).group(1)
    arms = re.findall(r"WHEN (.*?) THEN '([^']+)'", gate)
    assert [a[1] for a in arms] == ["blocked by the engine", "engine: do not invest", "engine: watch only",
                                    "below the reliability / data-quality floors"], arms
    assert "UPPER(r.investability_status) = 'BLOCKED'" in arms[0][0]
    assert "UPPER(r.final_action) = 'DO_NOT_INVEST'" in arms[1][0]
    assert "UPPER(r.final_action) = 'WATCH'" in arms[2][0]
    assert "NOT IFNULL(r.meets_floors, FALSE)" in arms[3][0]
    assert "ELSE 'none'" in gate and "tfb_rating_score" not in gate
    # the display carries the label and the gate together
    disp = re.search(r"CONCAT\(\n(.*?)\) AS tfb_rating_display", rating, re.DOTALL).group(1)
    for part in ("' - blocked by the engine'", "' - engine: do not invest'", "' - engine: watch only'",
                 "' - below the floors'", "ELSE 'No rating'"):
        assert part in disp, part
    # sources
    assert "CONCAT('EODHD consensus, ', CAST(IFNULL(an.analysts, 0) AS STRING), ' analysts')" in rating
    assert "THEN 'sheet analyst rating'" in rating and "ELSE 'no coverage'" in rating
    assert "u.investability_status" in rating
    # both insight views carry the gate, the display and the sources
    for n in ("v_top10_insight", "v_portfolio_insight"):
        for col in ("rt.rating_gate", "rt.tfb_rating_display", "rt.analyst_source", "rt.news_source"):
            assert col in code[n], (n, col)
    # the portfolio: riyal values for every holding, the decision page first, else the sheet x FX
    pf = code["v_portfolio_insight"]
    assert "LEFT JOIN `{{DS}}.v_fx_sar` AS fx ON fx.currency = h.currency" in pf
    assert "COALESCE(d.market_value_sar, h.position_value * fx.rate_to_sar) AS value_sar" in pf
    assert "COALESCE(d.cost_sar, h.position_cost * fx.rate_to_sar) AS cost_sar" in pf
    assert "COALESCE(d.pnl_sar, (h.position_value - h.position_cost) * fx.rate_to_sar) AS pnl_sar" in pf
    # 1.4.1: a weight is a share of the whole book - NULL for every row while any holding
    # has no riyal value, with holdings_without_value saying how many
    assert ("IF(COUNTIF(j.value_sar IS NULL) OVER () > 0, NULL,\n"
            "     ROUND(SAFE_DIVIDE(j.value_sar, SUM(j.value_sar) OVER ()) * 100, 2)) AS weight_sar_pct") in pf
    assert "COUNTIF(j.value_sar IS NULL) OVER () AS holdings_without_value" in pf
    assert _weights([100.0, 300.0]) == ([25.0, 75.0], 0)
    assert _weights([100.0, 300.0, None]) == ([None, None, None], 1)       # partial book: no weights
    assert _weights([100.0, 300.0, None], whole_book=False)[0] == [25.0, 75.0, None]   # the 1.4.0 defect
    assert "ROUND(SAFE_DIVIDE(j.pnl_sar, j.cost_sar) * 100, 2) AS pnl_sar_pct" in pf
    assert "WHEN d.market_value_sar IS NOT NULL THEN 'Portfolio_Decision'" in pf
    assert "ELSE 'no rate for the currency'" in pf
    # the Top-10: seat and allocation in plain words; a row without a ticket is research
    t10 = code["v_top10_insight"]
    assert "WHEN b.seat_rank IS NOT NULL THEN CONCAT('ranked seat ', CAST(b.seat_rank AS STRING))" in t10
    assert "WHEN UPPER(b.stability_state) = 'GRACE' THEN 'grace, no seat'" in t10
    assert "WHEN IFNULL(b.ticket_sar, 0) > 0 THEN CONCAT('ticket ', FORMAT('%.0f', b.ticket_sar), ' SAR')" in t10
    assert "ELSE 'none - research board'" in t10
    assert bv.check_objects(list(objs.values())) == []


def _coverage(rows, now, win_over="scored", due_by="target", with_all=True, cohort=True):
    """v_outcome_coverage on a small log: rows carry horizon, target (day), status,
    call (WIN / LOSS / None) and an optional cohort flag (False = a repeated key or an
    invalid matured copy, outside the sheet's outcome cohort). Returns
    {horizon: (due, matured, scored, wins, overdue, win_pct, share)}; with cohort (1.4.1)
    scored and wins count cohort rows only, without it (1.4.0) every matured copy."""
    out = {}
    hs = sorted({r["horizon"] for r in rows})
    groups = {h: [r for r in rows if r["horizon"] == h] for h in hs}
    if with_all:
        groups["ALL"] = rows
    for h, rs in groups.items():
        due = sum(1 for r in rs if (r["target"] <= now if due_by == "target" else True))
        matured = sum(1 for r in rs if r["status"] == "matured")
        inc = (lambda r: r.get("cohort", True)) if cohort else (lambda r: True)
        scored = sum(1 for r in rs if r["status"] == "matured" and r["call"] in ("WIN", "LOSS") and inc(r))
        wins = sum(1 for r in rs if r["status"] == "matured" and r["call"] == "WIN" and inc(r))
        overdue = sum(1 for r in rs if r["target"] <= now and r["status"] == "active")
        denom = scored if win_over == "scored" else matured
        out[h] = (due, matured, scored, wins, overdue,
                  round(wins / denom * 100, 1) if denom else None,
                  round(matured / due * 100, 1) if due else None)
    return out


def case_v23():
    objs = {o.name: o for o in bv.load_objects()}
    code = bv._code(objs["v_outcome_coverage"].template)
    # v1.5.0: the engine's columns join the log by record_id; the tracker's columns are unchanged
    assert set(bv._REF.findall(code)) - {"v_outcome_coverage"} == {"v_performance_log", "v_outcome_engine"}
    assert "LEFT JOIN `{{DS}}.v_outcome_engine` AS e ON e.record_id = l.record_id" in code
    assert "COUNTIF(target_at <= CURRENT_TIMESTAMP()) AS due" in code
    assert "COUNTIF(is_matured) AS matured" in code
    # 1.4.1: scored and wins over the sheet's outcome cohort (first valid matured record per
    # key), so a key archived twice never counts twice; matured stays raw for coverage
    assert "COUNTIF(in_outcome_cohort) AS matured_cohort" in code
    assert "COUNTIF(is_matured AND NOT in_outcome_cohort) AS matured_excluded" in code
    assert "COUNTIF(is_scored AND in_outcome_cohort) AS scored" in code
    assert "SUM(IF(in_outcome_cohort, IFNULL(win_flag, 0), 0)) AS wins" in code
    assert "SUM(matured_cohort) AS matured_cohort" in code and "SUM(matured_excluded) AS matured_excluded" in code
    assert "COUNTIF(target_at <= CURRENT_TIMESTAMP() AND status = 'active') AS overdue_open" in code
    assert "ROUND(SAFE_DIVIDE(u.wins, u.scored) * 100, 1) AS win_pct" in code      # over decided calls
    assert "ROUND(SAFE_DIVIDE(u.matured, u.due) * 100, 1) AS matured_share_pct" in code
    assert "'ALL' AS horizon" in code and "MIN(oldest_overdue_target)" in code
    assert "SELECT * FROM by_h\n  UNION ALL\n  SELECT * FROM all_h" in code       # the ALL row is published
    assert "snapshot_date = (SELECT MAX(snapshot_date) FROM `{{DS}}.v_performance_log`)" in code
    rows = [{"horizon": "1W", "target": 1, "status": "matured", "call": "WIN"},
            {"horizon": "1W", "target": 2, "status": "matured", "call": "LOSS"},
            {"horizon": "1W", "target": 3, "status": "matured", "call": None},      # neutral tier
            {"horizon": "1W", "target": 4, "status": "active", "call": None},       # overdue
            {"horizon": "1W", "target": 9, "status": "active", "call": None},       # not due
            {"horizon": "1M", "target": 2, "status": "active", "call": None},
            {"horizon": "1M", "target": 5, "status": "matured", "call": "WIN"},
            {"horizon": "1M", "target": 5, "status": "matured", "call": "WIN", "cohort": False}]  # the same key, archived twice
    c = _coverage(rows, now=5)
    assert c["1W"] == (4, 3, 2, 1, 1, 50.0, 75.0), c["1W"]
    assert c["1M"] == (3, 2, 1, 1, 1, 100.0, 66.7), c["1M"]      # the copy is matured, never scored
    assert c["ALL"] == (7, 5, 3, 2, 2, 66.7, 71.4), c["ALL"]
    # golden negatives: the rate over matured rows counts the neutral call as a loss; due
    # without the target date is every record; no ALL row; every matured copy counted (1.4.0):
    # two copies of one winning key beside one loss read 66.7 % where the sheet says 50 %
    assert _coverage(rows, now=5, win_over="matured")["1W"][5] == 33.3
    assert _coverage(rows, now=5, due_by="all")["1W"][0] == 5
    assert "ALL" not in _coverage(rows, now=5, with_all=False)
    dup = [{"horizon": "2W", "target": 1, "status": "matured", "call": "WIN"},
           {"horizon": "2W", "target": 1, "status": "matured", "call": "WIN", "cohort": False},
           {"horizon": "2W", "target": 1, "status": "matured", "call": "LOSS"}]
    assert _coverage(dup, now=5)["2W"][2:4] == (2, 1) and _coverage(dup, now=5)["2W"][5] == 50.0
    assert _coverage(dup, now=5, cohort=False)["2W"][5] == 66.7


# --------------------------------------------------------------------- v1.5.0
def _first_on_or_after(bars, day, within=None):
    """The first bar on/after `day` (days are ints); None beyond `within` days."""
    cands = [b for b in bars if b["date"] >= day and (within is None or b["date"] <= day + within)]
    return min(cands, key=lambda b: b["date"]) if cands else None


def _engine(records, bars, today, window_days=7, later_session=False, bearish_inverted=True,
            score_hold=False, dedupe=True, entry_day_bar=False):
    """v_outcome_engine on a small log. records: id, key, symbol, stance, entry, target_price,
    recorded (day), target (session day), status, sheet_call; bars: symbol, date, high, low,
    close. Returns {id: row}. Golden switches: later_session accepts a bar past the window,
    bearish_inverted=False scores a bearish fall as a loss, score_hold scores HOLD, dedupe=False
    counts every scored copy of a key in the cohort, entry_day_bar=True lets the recording
    day's own bar into the excursions (Codex P2 on #891: a target touched by that morning's high)."""
    out = {}
    by_sym = {}
    for b in bars:
        by_sym.setdefault(b["symbol"], []).append(b)
    for r in records:
        sb = by_sym.get(r["symbol"], [])
        row = {"state": None, "roi": None, "call": None, "scored": False, "win": None,
               "mfe": None, "mae": None, "touched": None, "ladder": {}}
        scorable = r["stance"] in ("bullish", "bearish") or (score_hold and r["stance"] == "neutral")
        if r["entry"] is None or r["entry"] <= 0:
            row["state"] = "no_entry_price"
        elif not scorable:
            row["state"] = "unscorable_stance"
        elif r["target"] > today:
            row["state"] = "target_not_due"
        elif not sb:
            row["state"] = "no_bar_for_symbol"
        else:
            tb = _first_on_or_after(sb, r["target"], None if later_session else window_days)
            last = max(b["date"] for b in sb)
            if tb is not None:
                row["state"] = "scored_from_bar"
                row["roi"] = round((tb["close"] / r["entry"] - 1) * 100, 4)
                sign = 1 if r["stance"] in ("bullish", "neutral") else (-1 if bearish_inverted else 1)
                v = row["roi"] * sign
                row["call"] = "WIN" if v > 0 else ("LOSS" if v < 0 else "BREAKEVEN")
                row["scored"] = row["call"] in ("WIN", "LOSS")
                row["win"] = 1 if row["call"] == "WIN" else (0 if row["call"] == "LOSS" else None)
            elif r["target"] + window_days > last:
                row["state"] = "awaiting_bar"
            else:
                row["state"] = "no_bar_at_target"
        if sb and r["entry"]:
            tb = _first_on_or_after(sb, r["target"], window_days)
            lo = r["recorded"] if entry_day_bar else r["recorded"] + 1
            win = [b for b in sb if lo <= b["date"] < r["target"]] + ([tb] if tb and tb["date"] >= lo else [])
            if win:
                row["mfe"] = round((max(b["high"] for b in win) / r["entry"] - 1) * 100, 4)
                row["mae"] = round((min(b["low"] for b in win) / r["entry"] - 1) * 100, 4)
                if r["stance"] == "bullish":
                    row["touched"] = any(b["high"] >= r["target_price"] for b in win)
                elif r["stance"] == "bearish":
                    row["touched"] = any(b["low"] <= r["target_price"] for b in win)
            for label, days in (("1w", 7), ("2w", 14), ("1m", 30), ("3m", 91)):
                b = _first_on_or_after(sb, r["recorded"] + days)
                row["ladder"][label] = round((b["close"] / r["entry"] - 1) * 100, 4) if b else None
        sc = r.get("sheet_call")
        if r["status"] != "matured":
            row["vs"] = None
        elif sc and row["call"]:
            row["vs"] = "agree" if sc == row["call"] else "disagree"
        elif sc:
            row["vs"] = "sheet_only"
        elif row["call"]:
            row["vs"] = "engine_only"
        else:
            row["vs"] = None
        out[r["id"]] = row
    seen = set()
    for r in sorted(records, key=lambda r: (r["recorded"], r["id"])):
        row = out[r["id"]]
        if not row["scored"]:
            row["cohort"] = False
        elif dedupe and r["key"] in seen:
            row["cohort"] = False
        else:
            row["cohort"] = True
            seen.add(r["key"])
    return out


def case_v24():
    import re
    objs = {o.name: o for o in bv.load_objects()}
    code = bv._code(objs["v_outcome_engine"].template)
    assert set(bv._REF.findall(code)) - {"v_outcome_engine"} == {"v_performance_log", "symbol_bars_daily"}
    # the first bar on/after the target session, within 7 days, never a later session
    assert "IF(b.date >= r.target_session_date AND b.date <= DATE_ADD(r.target_session_date, INTERVAL 7 DAY)," in code
    assert "IGNORE NULLS ORDER BY b.date LIMIT 1)[SAFE_OFFSET(0)] AS tb" in code
    # the target session: the venue-local day, the next day after the venue's close
    assert "DATE_ADD(DATE(rec.target_at, COALESCE(v.tz, 'America/New_York')), INTERVAL 1 DAY)" in code
    assert "> COALESCE(v.close_min, 960)" in code
    venues = re.findall(r"\('([A-Z]+)', '([^']+)', '([^']+)', (\d+)\)", code)
    assert {v[0] for v in venues} >= {"SR", "US", "HK", "L", "T", "DE", "PA", "MI", "MC", "AS", "BR", "SW", "LS",
                                      "VI", "WA", "AX", "TO", "NS", "KS", "TW", "SS", "SZ", "SI", "KL", "JK",
                                      "JO", "MX", "SA", "IS", "OL"}
    tz = {v[0]: v[2] for v in venues}
    close = {v[0]: int(v[3]) for v in venues}
    assert tz["SR"] == "Asia/Riyadh" and close["SR"] == 900 and tz["US"] == "America/New_York" and close["US"] == 960
    # the call is scored by stance exactly as the sheet scores realized ROI
    assert ("WHEN s.entry_stance = 'bearish'\n        THEN CASE WHEN s.engine_roi_pp < 0 THEN 'WIN' "
            "WHEN s.engine_roi_pp > 0 THEN 'LOSS' ELSE 'BREAKEVEN' END") in code
    assert "WHEN r.entry_stance NOT IN ('bullish', 'bearish') OR r.entry_stance IS NULL THEN 'unscorable_stance'" in code
    for state in ("no_entry_price", "unscorable_stance", "target_not_due", "no_bar_for_symbol", "scored_from_bar",
                  "awaiting_bar", "no_bar_at_target"):
        assert f"'{state}'" in code, state
    assert "ROW_NUMBER() OVER (PARTITION BY c.record_key, IFNULL(c.engine_call_outcome IN ('WIN', 'LOSS'), FALSE)" in code
    assert "ORDER BY c.recorded_at, c.sheet_row, c.record_id) = 1" in code
    assert "IF(c.call_outcome = c.engine_call_outcome, 'agree', 'disagree')" in code
    assert "MAX(IF(b.date > r.recorded_day AND b.date < r.target_session_date, b.high, NULL)) AS pre_high" in code
    assert "IF(r.entry_stance = 'bullish' AND a.tb.date > r.recorded_day AND a.tb.high >= r.target_price, a.tb.date, DATE '9999-12-31')" in code
    assert "IF(a.tb.date > r.recorded_day, IFNULL(a.tb.high, 0), 0)" in code     # the target bar on the recording day is left out
    for col in ("mfe_pct", "mae_pct", "touched_target", "days_to_target_touch", "bars_observed", "last_close",
                "bar_roi_pp", "roi_1w_pp", "roi_2w_pp", "roi_1m_pp", "roi_3m_pp", "sheet_vs_engine", "roi_gap_pp",
                "in_engine_cohort", "engine_win_flag", "engine_scored", "target_bar_date", "target_close"):
        assert f" AS {col}" in code, col
    assert "research view" in code and "Directional Win Rate" in code
    # the simulator: a small log on one symbol with bars on days 1-5, 8-12 (no weekend bars)
    bars = [{"symbol": "A", "date": d, "high": 10 + d, "low": 8 + d * 0.5, "close": 9 + d}
            for d in (1, 2, 3, 4, 5, 8, 9, 10, 11, 12)]
    recs = [
        {"id": "r1", "key": "k1", "symbol": "A", "stance": "bullish", "entry": 10, "target_price": 14,
         "recorded": 1, "target": 6, "status": "matured", "sheet_call": "WIN"},        # target on a weekend: day 8 bar
        {"id": "r2", "key": "k2", "symbol": "A", "stance": "bearish", "entry": 20, "target_price": 8.5,
         "recorded": 2, "target": 4, "status": "matured", "sheet_call": "LOSS"},       # price fell: bearish WIN, sheet disagrees
        {"id": "r3", "key": "k3", "symbol": "A", "stance": "neutral", "entry": 10, "target_price": 11,
         "recorded": 1, "target": 3, "status": "matured", "sheet_call": None},         # HOLD never scores
        {"id": "r4", "key": "k4", "symbol": "A", "stance": "bullish", "entry": 10, "target_price": 30,
         "recorded": 1, "target": 20, "status": "active", "sheet_call": None},         # not due (today 15)
        {"id": "r5", "key": "k5", "symbol": "B", "stance": "bullish", "entry": 10, "target_price": 12,
         "recorded": 1, "target": 3, "status": "active", "sheet_call": None},          # no bars for B
        {"id": "r6", "key": "k6", "symbol": "A", "stance": "bullish", "entry": 10, "target_price": 12,
         "recorded": 1, "target": 13, "status": "active", "sheet_call": None},         # archive ends day 12: awaiting
        {"id": "r7", "key": "k1", "symbol": "A", "stance": "bullish", "entry": 10, "target_price": 14,
         "recorded": 3, "target": 5, "status": "matured", "sheet_call": "WIN"},        # second copy of k1
        {"id": "r8", "key": "k8", "symbol": "A", "stance": "bullish", "entry": None, "target_price": 14,
         "recorded": 1, "target": 5, "status": "active", "sheet_call": None},          # no entry price
    ]
    e = _engine(recs, bars, today=15)
    assert e["r1"]["state"] == "scored_from_bar" and e["r1"]["roi"] == 70.0 and e["r1"]["call"] == "WIN"
    assert e["r1"]["vs"] == "agree" and e["r1"]["cohort"] is True
    assert e["r1"]["mfe"] == 80.0 and e["r1"]["mae"] == -10.0 and e["r1"]["touched"] is True   # days 2-5 + day 8; day 1 left out
    assert e["r1"]["ladder"] == {"1w": 70.0, "2w": None, "1m": None, "3m": None}
    assert e["r2"]["state"] == "scored_from_bar" and e["r2"]["roi"] == -35.0 and e["r2"]["call"] == "WIN"
    assert e["r2"]["vs"] == "disagree" and e["r2"]["touched"] is False       # lows 9.0-10.0 (days 2-4) never reach 8.5
    assert e["r3"]["state"] == "unscorable_stance" and e["r3"]["call"] is None and e["r3"]["vs"] is None
    assert e["r4"]["state"] == "target_not_due" and e["r4"]["ladder"]["1w"] == 70.0   # day 8 close 17
    assert e["r5"]["state"] == "no_bar_for_symbol"
    assert e["r6"]["state"] == "awaiting_bar"
    assert e["r7"]["scored"] is True and e["r7"]["cohort"] is False           # the repeated key counts once
    assert e["r8"]["state"] == "no_entry_price"
    # golden negatives
    far = [{"id": "r9", "key": "k9", "symbol": "A", "stance": "bullish", "entry": 10, "target_price": 14,
            "recorded": 1, "target": 6, "status": "active", "sheet_call": None}]
    gap_bars = [b for b in bars if b["date"] <= 5] + [{"symbol": "A", "date": 30, "high": 40, "low": 30, "close": 35}]
    assert _engine(far, gap_bars, today=40)["r9"]["state"] == "no_bar_at_target"            # 24 days later: never used
    assert _engine(far, gap_bars, today=40, later_session=True)["r9"]["state"] == "scored_from_bar"
    assert _engine(recs, bars, today=15, bearish_inverted=False)["r2"]["call"] == "LOSS"    # a bearish fall must win
    assert _engine(recs, bars, today=15, score_hold=True)["r3"]["scored"] is True           # HOLD must never score
    assert _engine(recs, bars, today=15, dedupe=False)["r7"]["cohort"] is True              # a copy must not count
    # the recording day's bar never counts: a spike before the forecast existed is not a touch
    spike = [{"symbol": "C", "date": 1, "high": 50, "low": 9, "close": 10}] + [
        {"symbol": "C", "date": d, "high": 10.5, "low": 9.5, "close": 10} for d in (2, 3, 4, 5, 8)]
    rc = [{"id": "r10", "key": "k10", "symbol": "C", "stance": "bullish", "entry": 10, "target_price": 20,
           "recorded": 1, "target": 5, "status": "active", "sheet_call": None}]
    assert _engine(rc, spike, today=15)["r10"]["touched"] is False and _engine(rc, spike, today=15)["r10"]["mfe"] == 5.0
    assert _engine(rc, spike, today=15, entry_day_bar=True)["r10"]["touched"] is True        # golden: contaminated
    assert _engine(recs, bars, today=15, entry_day_bar=True)["r1"]["mae"] == -15.0


def _calibration(rows, min_sample=30, cohort_only=True, with_all=True, flag_small=True):
    """v_outcome_calibration on engine rows: conf, scored (bool), cohort (bool), win (1/0/None),
    roi, target_roi; returns {value: (scored, wins, rate, capture, flag)} for the confidence
    dimension plus ALL."""
    groups = {}
    for r in rows:
        groups.setdefault(r["conf"], []).append(r)
    if with_all:
        groups["ALL"] = rows
    out = {}
    for v, rs in groups.items():
        inc = (lambda r: r["cohort"]) if cohort_only else (lambda r: r["scored"])
        sc = [r for r in rs if inc(r)]
        wins = sum(1 for r in sc if r["win"] == 1)
        rate = round(wins / len(sc) * 100, 1) if sc else None
        avg_roi = sum(r["roi"] for r in sc) / len(sc) if sc else None
        avg_t = sum(r["target_roi"] for r in sc) / len(sc) if sc else None
        capture = round(avg_roi / avg_t * 100, 1) if sc and abs(avg_t) >= 1 else None
        out[v] = [len(sc), wins, rate, capture, None]
    for v, row in out.items():
        if flag_small and row[0] < min_sample:
            row[4] = "small sample"
    h = out.get("HIGH", [0, 0, None])[2]
    m = out.get("MEDIUM", [0, 0, None])[2]
    lo = out.get("LOW", [0, 0, None])[2]
    if "HIGH" in out and out["HIGH"][4] is None and h is not None and \
            ((m is not None and h < m) or (lo is not None and h < lo)):
        out["HIGH"][4] = "High confidence under-delivers"
    return {v: tuple(r) for v, r in out.items()}


def case_v25():
    objs = {o.name: o for o in bv.load_objects()}
    code = bv._code(objs["v_outcome_calibration"].template)
    assert set(bv._REF.findall(code)) - {"v_outcome_calibration"} == {"v_outcome_engine"}
    assert "STRUCT('ALL' AS dimension, 'ALL' AS value)" in code
    for dim in ("confidence", "reliability_tier", "data_quality_tier", "sector", "market", "horizon", "origin_tab",
                "risk_bucket", "entry_recommendation", "recorded_week"):
        assert f"STRUCT('{dim}'" in code, dim
    assert "COUNTIF(in_engine_cohort) AS engine_scored" in code
    assert "SUM(IF(in_engine_cohort, IFNULL(engine_win_flag, 0), 0)) AS engine_wins" in code
    assert "ROUND(AVG(IF(in_engine_cohort, engine_roi_pp, NULL)), 4) AS avg_engine_roi_pp" in code
    assert "COUNTIF(in_outcome_cohort AND sheet_scored) AS sheet_scored" in code
    assert "ROUND(SAFE_DIVIDE(g.engine_wins, g.engine_scored) * 100, 1) AS engine_win_rate_pct" in code
    assert ("IF(ABS(g.avg_target_roi_pp) >= 1, ROUND(SAFE_DIVIDE(g.avg_engine_roi_pp, g.avg_target_roi_pp) * 100, 1), NULL) "
            "AS roi_capture_pct") in code
    assert "WHEN f.engine_scored < 30 THEN 'small sample'" in code
    assert "AND (f.high_rate < f.medium_rate OR f.high_rate < f.low_rate) THEN 'High confidence under-delivers'" in code
    assert "WHEN e.entry_forecast_reliability < 70 THEN '< 70'" in code and "ELSE '90+'" in code
    rows = ([{"conf": "HIGH", "scored": True, "cohort": True, "win": 1, "roi": 2.0, "target_roi": 4.0}] * 10
            + [{"conf": "HIGH", "scored": True, "cohort": True, "win": 0, "roi": -3.0, "target_roi": 4.0}] * 20
            + [{"conf": "HIGH", "scored": True, "cohort": False, "win": 1, "roi": 9.0, "target_roi": 4.0}] * 5   # copies
            + [{"conf": "MEDIUM", "scored": True, "cohort": True, "win": 1, "roi": 1.0, "target_roi": 0.2}] * 25
            + [{"conf": "MEDIUM", "scored": True, "cohort": True, "win": 0, "roi": -1.0, "target_roi": 0.2}] * 10
            + [{"conf": "LOW", "scored": True, "cohort": True, "win": 1, "roi": 1.0, "target_roi": 2.0}] * 3)
    c = _calibration(rows)
    assert c["HIGH"] == (30, 10, 33.3, round(((10 * 2 - 20 * 3) / 30) / 4.0 * 100, 1), "High confidence under-delivers"), c["HIGH"]
    assert c["MEDIUM"][:3] == (35, 25, 71.4) and c["MEDIUM"][3] is None        # average target 0.2: no capture
    assert c["LOW"] == (3, 3, 100.0, 50.0, "small sample")
    assert c["ALL"][:3] == (68, 38, 55.9)
    # golden negatives: every scored copy counted; no ALL row; the flag against a small sample
    assert _calibration(rows, cohort_only=False)["HIGH"][:2] == (35, 15)
    assert "ALL" not in _calibration(rows, with_all=False)
    small = [{"conf": "HIGH", "scored": True, "cohort": True, "win": 0, "roi": -1.0, "target_roi": 2.0}] * 5 + rows[35:]
    assert _calibration(small)["HIGH"][4] == "small sample"
    assert _calibration(small, flag_small=False)["HIGH"][4] == "High confidence under-delivers"


def _board(rows, holdings=(), flagged=(), per_sector=3, per_market=10, seats=10, executable=False,
           market_before_sector=False):
    """v_board_archive on universe rows: symbol, sector, market, final_action, inv, block,
    reco, rel, dq, risk, roi12, conflict, opp. Returns {symbol: (fails, seat)} and the constant."""
    out = {}
    passers = []
    for r in rows:
        fails = []
        if r["final_action"] != "INVEST":
            fails.append("engine: final action not INVEST")
        if r["inv"] != "INVESTABLE":
            fails.append("engine: not INVESTABLE")
        if r.get("block"):
            fails.append("engine: block reason")
        if (r.get("reco") or "").upper().replace("_", " ") in ("SELL", "STRONG SELL", "REDUCE", "AVOID", "EXIT", "TRIM"):
            fails.append("sell-class recommendation")
        if not (r["rel"] is not None and r["rel"] >= 70):
            fails.append("reliability below 70 (T10: Min Reliability)")
        if not (r["dq"] is not None and r["dq"] >= 80):
            fails.append("data quality below 80 (T10: Min Data Quality)")
        if (r.get("risk") or "") not in ("LOW", "MODERATE"):
            fails.append("risk above Medium (T10: Max Risk Level)")
        if not (r["roi12"] is not None and r["roi12"] >= 12):
            fails.append("12-month ROI below 12 % (T10: Required ROI %)")
        if not (r["roi12"] is not None and r["roi12"] >= 10):
            fails.append("annualized ROI below 10 % (T10: Required Annualized ROI %)")
        if str(r.get("conflict", "")).upper() == "TRUE":
            fails.append("provider/engine conflict (T10: Allow Conflict No)")
        if r["symbol"] in holdings:
            fails.append("portfolio holding (T10: Include Portfolio Holdings No)")
        if r["symbol"] in flagged:
            fails.append("named by the news gate (T10: Allow Negative News No)")
        out[r["symbol"]] = [fails, None]
        if not fails:
            passers.append(r)
    passers.sort(key=lambda r: (-r["opp"], -r["roi12"], -r["rel"], r["symbol"]))
    sec, mkt, rank = {}, {}, 0
    for r in passers:
        sec[r["sector"]] = sec.get(r["sector"], 0) + 1
        if market_before_sector:            # golden: the market cap counts sector-rejected names too
            mkt[r["market"]] = mkt.get(r["market"], 0) + 1
        if sec[r["sector"]] > per_sector:
            continue
        if not market_before_sector:
            mkt[r["market"]] = mkt.get(r["market"], 0) + 1
        if mkt[r["market"]] <= per_market:
            rank += 1
            if rank <= seats:
                out[r["symbol"]][1] = rank
    return {k: tuple(v) for k, v in out.items()}, executable


def case_v26():
    import re
    objs = {o.name: o for o in bv.load_objects()}
    code = bv._code(objs["v_board_archive"].template)
    assert set(bv._REF.findall(code)) - {"v_board_archive"} == {"v_universe_latest", "v_my_portfolio", "v_news_gate"}
    gas = (ROOT / "apps_script" / "16_Decision_Top10.gs").read_text(encoding="utf-8")
    panel = dict(re.findall(r"\{ label: '(T10: [^']+)', def: ([^,]+), kind", gas))
    assert panel["T10: Min Reliability"] == "70" and "u.reliability >= 70" in code
    assert panel["T10: Min Data Quality"] == "80" and "u.data_quality >= 80" in code
    assert panel["T10: Max Risk Level"] == "'Medium'" and "NOT IN ('LOW', 'MODERATE')" in code
    assert panel["T10: Required ROI %"] == "12" and panel["T10: Period (Months)"] == "12"
    assert "u.expected_roi_12m_pct >= 12" in code
    assert panel["T10: Required Annualized ROI %"] == "10" and "u.expected_roi_12m_pct >= 10" in code
    assert panel["T10: Max Per Sector"] == "3" and "WHERE p.sector_rank <= 3" in code
    assert "WHERE s.market_rank <= 10" in code and code.index("WHERE p.sector_rank <= 3") < code.index("WHERE s.market_rank <= 10")
    assert "FROM passers AS p\n  WHERE p.sector_rank <= 3" in code     # the market rank is taken over sector-admitted names only
    assert panel["T10: Max Per Market"] == "10" and panel["T10: Max Selected"] == "10"
    assert "IF(r.archive_rank <= 10, r.archive_rank, NULL) AS archive_seat" in code
    assert panel["T10: Allow Conflict"] == "'No'" and "provider/engine conflict (T10: Allow Conflict No)" in code
    assert panel["T10: Include Portfolio Holdings"] == "'No'"
    assert "portfolio holding (T10: Include Portfolio Holdings No)" in code
    assert panel["T10: Allow Negative News"] == "'No'" and "named by the news gate (T10: Allow Negative News No)" in code
    assert "IF(IFNULL(u.final_action, '') != 'INVEST', 'engine: final action not INVEST', NULL)" in code
    assert "IF(IFNULL(u.investability_status, '') != 'INVESTABLE', 'engine: not INVESTABLE', NULL)" in code
    assert "'sell-class recommendation'" in code and "'STRONG SELL', 'REDUCE', 'AVOID', 'EXIT', 'TRIM'" in code
    assert "FALSE AS executable" in code and "'archive board - research only, not the cockpit' AS board_kind" in code
    assert "T10: Min Risk/Reward" in code and "gates_not_applied" in code     # named, not silently applied
    assert ("PARTITION BY j.sector ORDER BY j.opportunity_score DESC, j.expected_roi_12m_pct DESC, "
            "j.reliability DESC, j.symbol") in code
    assert "WHERE j.final_action = 'INVEST' OR j.investability_status = 'INVESTABLE'" in code
    base = {"final_action": "INVEST", "inv": "INVESTABLE", "block": None, "reco": "BUY", "rel": 75, "dq": 90,
            "risk": "LOW", "roi12": 20, "conflict": "FALSE", "opp": 60, "market": "US", "sector": "Tech"}

    def mk(sym, **kw):
        r = dict(base, symbol=sym)
        r.update(kw)
        return r
    rows = [mk("A", opp=70), mk("B", opp=69), mk("C", opp=68), mk("D", opp=67),            # four Tech names
            mk("E", sector="Energy", opp=66), mk("F", sector="Energy", opp=50, rel=60),    # F: reliability
            mk("G", sector="Banks", opp=65, risk="HIGH"), mk("H", sector="Banks", opp=64, roi12=11),
            mk("I", sector="Banks", opp=63, conflict="TRUE"), mk("J", sector="Banks", opp=62, reco="SELL"),
            mk("K", sector="Banks", opp=61, final_action="WATCH", inv="WATCHLIST"),
            mk("L", sector="Banks", opp=60, block="sell class"), mk("M", sector="Banks", opp=59, dq=70),
            mk("N", sector="Retail", opp=58), mk("O", sector="Retail", opp=57)]
    b, ex = _board(rows, holdings={"O"}, flagged={"N"})
    assert ex is False
    assert [b[s][1] for s in "ABCD"] == [1, 2, 3, None]                 # the fourth Tech name never seats
    assert b["E"][1] == 4 and b["F"] == (["reliability below 70 (T10: Min Reliability)"], None)
    assert b["G"][0] == ["risk above Medium (T10: Max Risk Level)"]
    assert b["H"][0] == ["12-month ROI below 12 % (T10: Required ROI %)"]
    assert b["I"][0] == ["provider/engine conflict (T10: Allow Conflict No)"]
    assert b["J"][0] == ["sell-class recommendation"]
    assert b["K"][0] == ["engine: final action not INVEST", "engine: not INVESTABLE"]
    assert b["L"][0] == ["engine: block reason"] and b["M"][0] == ["data quality below 80 (T10: Min Data Quality)"]
    assert b["N"][0] == ["named by the news gate (T10: Allow Negative News No)"]
    assert b["O"][0] == ["portfolio holding (T10: Include Portfolio Holdings No)"]
    # golden negatives: a holding seated; a fourth name of one sector; a HIGH-risk name; executable TRUE
    assert _board(rows, holdings=set(), flagged={"N"})[0]["O"][1] == 5
    assert _board(rows, per_sector=4)[0]["D"][1] == 4
    assert _board([mk("G", risk="HIGH")])[0]["G"][1] is None
    assert _board(rows, executable=True)[1] is True and "TRUE AS executable" not in code
    # ten names of one sector must not use up a market's ten places (Codex P2 on #891)
    crowd = [mk(f"T{i}", opp=90 - i) for i in range(10)] + [mk(f"U{i}", sector=f"S{i}", opp=50 - i) for i in range(7)]
    seated = [s for s, v in _board(crowd)[0].items() if v[1]]
    assert len(seated) == 10 and seated[:3] == ["T0", "T1", "T2"] and all(f"U{i}" in seated for i in range(7))
    assert len([s for s, v in _board(crowd, market_before_sector=True)[0].items() if v[1]]) == 3


def _coverage_engine(rows, with_all=True, cohort=True):
    """the v1.5.0 engine columns of v_outcome_coverage: rows carry horizon, state, cohort, win."""
    hs = sorted({r["horizon"] for r in rows})
    groups = {h: [r for r in rows if r["horizon"] == h] for h in hs}
    if with_all:
        groups["ALL"] = rows
    out = {}
    for h, rs in groups.items():
        inc = (lambda r: r["cohort"]) if cohort else (lambda r: r["state"] == "scored_from_bar")
        scored = sum(1 for r in rs if inc(r))
        wins = sum(1 for r in rs if inc(r) and r["win"] == 1)
        out[h] = (scored, wins, round(wins / scored * 100, 1) if scored else None,
                  sum(1 for r in rs if r["state"] == "scored_from_bar"),
                  sum(1 for r in rs if r["state"] == "awaiting_bar"),
                  sum(1 for r in rs if r["state"] == "no_bar_for_symbol"),
                  sum(1 for r in rs if r["state"] == "no_bar_at_target"))
    return out


def case_v27():
    objs = bv.load_objects()
    names = [o.name for o in objs]
    assert names.index("v_outcome_coverage") > names.index("v_outcome_engine")      # applies after the engine
    assert [o.path.name for o in objs if o.name == "v_outcome_coverage"] == ["73_v_outcome_coverage.sql"]
    code = bv._code({o.name: o for o in objs}["v_outcome_coverage"].template)
    for col in ("COUNTIF(in_engine_cohort) AS engine_scored",
                "SUM(IF(in_engine_cohort, IFNULL(engine_win_flag, 0), 0)) AS engine_wins",
                "COUNTIF(engine_state = 'scored_from_bar') AS engine_scorable_now",
                "COUNTIF(engine_state = 'awaiting_bar') AS engine_awaiting_bar",
                "COUNTIF(engine_state = 'no_bar_for_symbol') AS engine_no_bar",
                "COUNTIF(engine_state = 'no_bar_at_target') AS engine_no_bar_at_target",
                "SUM(engine_scored) AS engine_scored", "SUM(engine_wins) AS engine_wins",
                "SUM(engine_scorable_now) AS engine_scorable_now", "SUM(engine_awaiting_bar) AS engine_awaiting_bar",
                "SUM(engine_no_bar) AS engine_no_bar", "SUM(engine_no_bar_at_target) AS engine_no_bar_at_target",
                "ROUND(SAFE_DIVIDE(u.engine_wins, u.engine_scored) * 100, 1) AS engine_win_rate_pct"):
        assert col in code, col
    # the tracker's columns are unchanged (V23 reads them too)
    assert "COUNTIF(is_scored AND in_outcome_cohort) AS scored" in code
    rows = [{"horizon": "1W", "state": "scored_from_bar", "cohort": True, "win": 1},
            {"horizon": "1W", "state": "scored_from_bar", "cohort": True, "win": 0},
            {"horizon": "1W", "state": "scored_from_bar", "cohort": False, "win": 1},    # a copy of a key
            {"horizon": "1W", "state": "awaiting_bar", "cohort": False, "win": None},
            {"horizon": "1M", "state": "no_bar_for_symbol", "cohort": False, "win": None},
            {"horizon": "1M", "state": "scored_from_bar", "cohort": True, "win": 1},
            {"horizon": "1M", "state": "no_bar_at_target", "cohort": False, "win": None}]
    c = _coverage_engine(rows)
    assert c["1W"] == (2, 1, 50.0, 3, 1, 0, 0) and c["1M"] == (1, 1, 100.0, 1, 0, 1, 1)
    assert c["ALL"] == (3, 2, 66.7, 4, 1, 1, 1)
    assert "ALL" not in _coverage_engine(rows, with_all=False)
    assert _coverage_engine(rows, cohort=False)["1W"][:3] == (3, 2, 66.7)     # every copy counted: wrong


CASES = [case_v1, case_v2, case_v3, case_v4, case_v5, case_v6, case_v7, case_v8, case_v9, case_v10, case_v11,
         case_v12, case_v13, case_v14, case_v15, case_v16, case_v17, case_v18, case_v19, case_v20, case_v21,
         case_v22, case_v23, case_v24, case_v25, case_v26, case_v27]


def test_v1(): case_v1()
def test_v2(): case_v2()
def test_v3(): case_v3()
def test_v4(): case_v4()
def test_v5(): case_v5()
def test_v6(): case_v6()
def test_v7(): case_v7()
def test_v8(): case_v8()
def test_v9(): case_v9()
def test_v10(): case_v10()
def test_v11(): case_v11()
def test_v12(): case_v12()
def test_v13(): case_v13()
def test_v14(): case_v14()
def test_v15(): case_v15()
def test_v16(): case_v16()
def test_v17(): case_v17()
def test_v18(): case_v18()
def test_v19(): case_v19()
def test_v20(): case_v20()
def test_v21(): case_v21()
def test_v22(): case_v22()
def test_v23(): case_v23()
def test_v24(): case_v24()
def test_v25(): case_v25()
def test_v26(): case_v26()
def test_v27(): case_v27()


if __name__ == "__main__":
    passed = 0
    for fn in CASES:
        try:
            fn()
            passed += 1
            print(f"PASS {fn.__name__}")
        except AssertionError as exc:
            print(f"FAIL {fn.__name__}: {exc}")
    print(f"{'PASS' if passed == len(CASES) else 'FAIL'} {passed}/{len(CASES)}")
    sys.exit(0 if passed == len(CASES) else 1)
