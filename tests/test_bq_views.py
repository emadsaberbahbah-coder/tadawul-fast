#!/usr/bin/env python3
# tests/test_bq_views.py
"""
Harness for scripts/apply_bq_views.py v1.7.0 and the SQL in sql/bigquery/
(C8 Phase 2: typed BigQuery views for Looker Studio; 2b: the global markets
page; 2c: the World Top-25 ROI, My Portfolio / Top-10 evidence and symbol
pages; 2d: candles, analysts, news and the TFB rating per symbol; 2e: the
learning loop from the archive; 2f: the signal edge; 2g: total return,
benchmarks and regimes). Offline: the BigQuery session is a fake, no google
library is imported.

  V1  the 47 objects load in apply order (6 functions, then the views and
      table functions in file order) and the static checks pass
  V2  every header a view reads with f_cell exists in that tab's archived
      header row (fixture: the 2026-10-29 snapshot)
  V3  render: the placeholder is fully replaced; bad project / dataset ids
      are refused
  V4  the body each object keeps in BigQuery hashes to the definitions read
      back from INFORMATION_SCHEMA after the 29 Oct creation (installed pins);
      the objects v1.6.0 / v1.7.0 add or change hash to their reviewed files (pending pins,
      confirmed by bq_views --verify after the next --write; v1.2.0's were
      read back on 31 Oct, v1.3.0's on 1 Nov, v1.3.1 / 1.4.0 / 1.4.1's on 1 Nov
      15:27 Riyadh and v1.5.0's on 8 Nov, and are installed pins); a table
      function keeps the query after its 'AS' line
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
  V28 v_signal_edge_daily (v1.6.0): one row per symbol and recording day
      from the day's first recording time; the scoring signals of the
      forecast's own run (the earliest snapshot 0-60 s after it; else the
      newest before it, at most 3 days older - never a later run); per day,
      market group (ALL / US / Tadawul / Other markets),
      feature and horizon with >= 20 names and two distinct values: the
      Spearman IC over average ranks for ties (tie keys INT64, since BigQuery
      cannot partition by FLOAT64), the top / bottom-third excess return and
      the horizon length (simulated - fails on min ranks for uneven ties, on
      the previous run's signal when the forecast's own snapshot exists, on a
      later run's signal, on 19 names, on a return beyond 300 %, on a window
      partitioned by a FLOAT64 value)
  V29 v_signal_edge (v1.6.0): mean daily IC, the overlap-adjusted t
      (mean / sd x SQRT(days / horizon_days)), first- and second-half IC and
      the fixed verdict thin / edge / inverse / no edge / unstable, thin
      first (simulated - fails when a 1M signal with 90 days reads 'edge' on
      the naive t, when a sign that flipped between halves reads 'edge', when
      a short history is judged)
  V30 v_outcome_engine 1.6.0 (views 1.7.0): every return is a TOTAL return -
      the outcome price is restated in entry-day terms with adj_close
      (outcome adj x base close / base adj; raw close when either adj is
      missing), engine_roi_raw_pp and dividend_adj_pp disclose the
      difference; a record's bars come from its alias (INDV.US -> SUPN.US)
      and bars_symbol says so; an entry price more than 35 % from the base
      bar's close is 'entry_price_mismatch', never scored (simulated: the
      SUPN $8.13 special dividend reads -4.7 %, not -26 %; a 2:1 split reads
      0; the stale 41.92 pre-merger quote is a mismatch - fails on the raw
      close, on a 10 % band, on the old symbol's own bars)
  V31 v_outcome_excess (v1.7.0): each engine-scored forecast beside its
      market benchmark by market (US SPY, Tadawul TASI, Hong Kong HSI,
      London FTSE, Europe Euro Stoxx, Tokyo Nikkei, else ACWI), from the
      base bar to the outcome bar and on the ladder; excess_call WIN when a
      bullish call beat its market or a bearish one trailed it (simulated -
      fails when a -3 % call in a -4.4 % market reads LOSS, when the
      benchmark runs from the recording day instead of the base bar)
  V32 v_market_regime (v1.7.0): SPY 1 / 5 / 20-session changes, distance
      from the 50-session average and the 250-session high, 20-session
      volatility, the VIX close, one label in a fixed order crash_day /
      drawdown / stress / weak / calm (simulated - fails when a -2 % day
      reads 'weak', when the 50-day average is judged on 49 bars)
  V33 v_seat_regime + v_regime_outcomes (v1.7.0): each seat reads the
      regime at the LAST US close before its seat date (a Monday seat reads
      Friday; never the seat day's own close); outcomes per regime and
      currency group with 'small sample' under 30 scored seats or 5 days
      (simulated - fails on the same-day close, on a missing ALL group)
"""
from __future__ import annotations

import json
import math
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
    # apply_bq_views 1.5.0, installed by bq_views write run 41441740101 (2 Nov) and read
    # back from INFORMATION_SCHEMA on 8 Nov 2026 (all four matched their reviewed files;
    # v_outcome_engine changes again in 1.7.0 and moves to PENDING)
    "v_outcome_calibration": "038aef710bcea57e18f2d8f417a7be79f9a609165130ed4098470c09148a949d",
    "v_board_archive": "406e8fb4eb5f5cc1b00f5f120bd4b7e3e682ae21868dffe50ac68bc4d5ff81bc",
    # apply_bq_views 1.6.0 - 1.20.0 (8 - 21 Nov 2026), installed by bq_views write runs and read back from
    # INFORMATION_SCHEMA by bq_views write run 44910876350 (21 Nov 2026, 13:51 UTC, main ae8c899): 76/76 objects
    # match their reviewed files. Kept with the notes they carried while pending.
    # apply_bq_views 1.20.0 (21 Nov 2026): the news by market and by person (news_entity_daily), every news signal
    # read against what the markets did next, and its scorecard (observe only)
    "v_market_news": "5ed47514ad0dad5468e0c3589043028982c898b98e2c3b2297010367309d91d6",
    "v_people_news": "1b7666e0778adc97d81d2b4acf45c109618c321a2063e5813d327c4e9989242e",
    "v_news_trial": "54527e02f322335733a88a32fb32fd7710309df9dc1b333998fa900cfd0fd746",
    "v_news_trial_summary": "832a7677fd5408d617fc36578d708cff344d76d24095db62544cdc432ee2039c",
    # apply_bq_views 1.19.0 (21 Nov 2026): the ready list - every qualified name with the market it tracks, that
    # market's state, the day's ready order - and its scorecard on the names (observe only)
    "v_ready_list": "64f7822e70358028b8a09392636d4775be69dba0678d93f7f18685a7a9e8a15e",
    "v_ready_trial": "c1a6d83a31feae9a302dc814016ea6347e650566f398ee843aa355245b9dd468",
    # apply_bq_views 1.18.0 (21 Nov 2026): the condition of every market, their order, the forward test and its
    # scorecard (observe only)
    "v_market_condition": "73a161bea603ebe303c160438f3e74039da889df2ded39b0e76e3db623c74995",
    "v_market_rank": "e393fbb23ccc0f5a8308e6c3f73bcbc3c14f4d3c9d1101652e9837cc4cc90ec1",
    "v_market_trial": "183d8109018a0d71007379bcb229ef319ee9543eb03bab2821707731474d6464",
    "v_market_trial_summary": "94a456bfff6f321693ddbe6369a5635285b20f170caf6dc5fd86f0115fb8293a",
    # apply_bq_views 1.17.0 (21 Nov 2026): the company news scoreboard against the same-day US names, not only SPY
    # (replaces the 1.16.0 body 130256c0270d)
    "v_company_news_eval": "07807483369ded1bb735dc33bfe14d31ffe3a044ce3605037c7e1ffdcb313ae7",
    # apply_bq_views 1.16.0 (20 Nov 2026): the company news re-read from the headline text, and its scoreboard
    "v_symbol_news_class": "d8386af8a947caa69ff169401a1cce69a0ca258da4bee74039b99f80900cf9f0",
    # apply_bq_views 1.15.0 (19 Nov 2026): C-2 per call, for the observe-only labels on the pages
    "v_c2_calls": "95ac9bcde2a5be87bfa95a2bb1ee42eaf1a26dec0abfa498339ecb3a137acd0f",
    # apply_bq_views 1.14.0 (19 Nov 2026): the news learning loop (themes, weights, tilt, shadow list, trial)
    "v_news_theme_weekly": "0a336a53e3b201f1852777dd940d751bd9b65c6f3b6c20fce57407e2168b71c3",
    "v_news_sector_weights": "83e866325a7047aeea81d824f91139aae3d219dfa632f45b74b838e120d876c8",
    "v_news_sector_tilt": "637933f5d28aa72c12690b5d8d379e4972b222938be9e2c262e09ff26a1a67cc",
    "v_news_overlay_list": "ce45122ead4f06ab6fcee4c3b1abf62e2a8e56a2b186337eb7f9d471f04a6aa7",
    "v_trial_news_overlay": "b2b0fdf88c772fb0239479387bcb9719f6638dfb35b64c6721ffdad5fa510b83",
    # apply_bq_views 1.13.0 (19 Nov 2026): the standing lost-opportunity comparison
    "v_forward_universe": "557dc2709d5bc349f98afa810730cb06fdee41a56a221612cf859deeb945f28d",
    "v_investable_vs_market": "fafc09c95276aae31acdc302dc739fa2f0c557b9f64702ed8982b30ede506a50",
    "v_missed_winners": "5d696fe1c1517c8e0134fffd2fa0d6fe14f5e3d9eb7dc46bba728700cdf43faf",
    # apply_bq_views 1.12.0 (19 Nov 2026): one sector per symbol, outcomes by sector, and trial C-2
    "v_symbol_sector": "f5018131ba52c2884030e246d759214473ba3ad30c5263288e9ff27575cbde64",
    # (v_outcome_sector moves to PENDING: body replaced in 1.35.0, was 2a160aee5092)
    "v_trial_c2": "a1af0a9b476971b078d159c9bfe1c19169bb9936295eecc0dbd6fd80e29056db",
    # apply_bq_views 1.11.0 (18 Nov 2026): the shadow score and its forward trial
    # (pending body replaced: v_signal_edge_daily ccf6ea331773, 1.7.0)
    "v_trial_shadow_v2": "5b6d94462ff998a3ef9cce7af91552d34f7c960ba10529740295aa69068e8cdc",
    # apply_bq_views 1.10.0 (16 Nov 2026): the news layer's scoreboard
    "v_news_signal_eval": "807584d412eb19347775641e22e8016d9d56da94e19ca89464663a5da6f16621",
    # apply_bq_views 1.9.0 (15 Nov 2026): hold or switch; 1.9.1: 1W / 2W = the 1M figure x days/30
    # (installed body replaced: v_hold_vs_switch 1abc9858278c, 1.9.0)
    "v_switch_calibration": "488ca50f38bfbbb96d235e27eba75cc562d77c68a272b1455a564e5d84a7f292",
    "v_hold_vs_switch": "b98c5ac8e6bf0c3c25416a6957a4a4f5741f75ecbfc2709331861cbb8c51a339",
    # apply_bq_views 1.8.0 (14 Nov 2026): the first controlled trial, C-1
    "v_trial_c1": "fd4bf8b9a0dbb95f5e8394dec7d873a1deb693402020e1e3bd0a8e3a91f0a507",
    "v_signal_edge_daily": "10fd1f5ac63c5c0cc5b8a159b93640b1bbfb99b50c13da5835d43c0ec32e2b38",
    "v_signal_edge": "14dbbb5db61c4a0a92e51b8c4068244105d92f34355a4613264581d50a6e74ce",
    "v_outcome_engine": "85d51279fdaa42b351c5ab8c277b507923005c6395fbf9b866aab01deed64c5e",
    "v_outcome_excess": "6a085d68f0862f1bae7d32c8ed29ae3c9c370931c6e924c98ddf52a5b61f6fc7",
    "v_market_regime": "422399a6120a712f93eae28ef9441f6f3eadabc54a24f08f086486fb9032483c",
    "v_seat_regime": "9860388614461fd67e344c04c5661f084bbc55b924eae43ceaec824c21816426",
    "v_regime_outcomes": "3a4c2de8caba465cf406f459358c7f6a492f4065e21a6d3327c3bec01c037188",
    "v_outcome_coverage": "fba5c320643c6ceecabb3b294b64527ffab44cb8b35cca89065b1386323e0f2b",
}
# The objects apply_bq_views 1.21.0 adds (21 Nov 2026), hashed from the reviewed files: bq_views --verify after the
# next --write must find these exact bodies in INFORMATION_SCHEMA (then they move to INSTALLED).
PENDING = {
    # apply_bq_views 1.21.0 (21 Nov 2026): the forward event scenarios - the meetings and releases, each market's move
    # around them, the scenarios from the earlier events of each family, the trial and its scorecard (observe only)
    "v_event_calendar": "8149d12c6e35334edef0de87eb375ad278965a22eded8094a564cc63d0dd6ce2",
    "v_event_reactions": "10d89bdd0e31593951118006848f3048b91ab4e21a7dcb3a6aa215d15048cfd6",
    "v_event_scenarios": "73f28feb2afbd02f9a5d8845891872444bec24c5001b61d87beadcdb9f569fc6",
    "v_event_trial": "518ab72be1b266a6a226ef038f1dd942b02d90c048257f947a30ac947e7fdd6d",
    "v_event_trial_summary": "9ca3cbc11e11e53dda4f3a005a20218426bd5e75f8bb0c1539c7056c13e12dfb",
    # apply_bq_views 1.22.0 (22 Nov 2026): the learning scorecard - ten rule variants fixed on 22 Nov, each against
    # its baseline on the scored calls, in-sample and forward from 23 Nov (research only)
    "v_learning_scorecard": "0505807dc0a18656b23df4a7c86c2000119247caf14ba65fac552527e26986ee",
    # apply_bq_views 1.23.0 (22 Nov 2026): the price drivers - the options reading of each name, the US market's
    # forces and their record, every stock's drivers beside its score, and each driver's record (research only)
    "v_options_reading": "75b9d7e7488f825a271f474c3b0ba06176e1eb71e0300395f1ff989466ddea16",
    "v_market_drivers": "02eae618a44e619d96f701987e67255c225478bd449ed959b1013e7589eebc41",
    "v_market_driver_learning": "11fe865115ea0a67eff37e7a74ffbb8e6067f1a7160c55bf304a13c3bc1aae62",
    "v_price_drivers": "bbecaf9802bbf3ed34a92c97d442307aee5774faccd7cb07a2494f3b500f7c16",
    "v_driver_learning": "b89918e710f562686738410f23ea3465ccf30fa1ed4df4b1b001a4d8290a0459",
    # apply_bq_views 1.24.0 (23 Nov 2026): the projection context - each promise against its own one-sigma move,
    # the adverse US market states, and the record of five fixed rules (research only)
    "v_projection_context": "eaa8f6481530e94d5b2a47acd0acf380b7822d159ab7b78e35ae3ad94164b5b8",
    "v_projection_scorecard": "f44c6cdefd5708823ae60c1d61fdd9da34cadf3d6497b85c03372667fd1c5a7f",
    # apply_bq_views 1.25.0 (23 Nov 2026): company news, the price's answer and the trend read together, each
    # state's record, and today's reading per stock beside TFB's view (research only)
    "v_news_trend_daily": "278af1f30782c4065f31594059f68716357f87d0eb8cf8a7091886a5b1b224b9",
    "v_news_trend_learning": "9db083a5dde518eaf5acba94aabc8f39f1514c195aa922ace6ad9100f91e37bd",
    "v_news_understanding": "b25a0d8630cd4c79a1af5199776d73ac8d769cafc71f755235500606d416ad11",
    # apply_bq_views 1.26.0 (24 Nov 2026): confidence by market against the market's own return, and the
    # shock-drop shadow rule with its record (research only)
    "v_confidence_calibration": "2814c17be7530e132904341ea0ce8000f65efe560a56e7585dd23ba753a64a37",
    "v_shock_drop_events": "2576f5d9f286f9d76e0b77d9a62590b54a2c9e7e894bde026272ea8a187ffa2e",
    "v_shock_drop_learning": "7d551a93e69a4bd95845ea967622a1736f452664985c4ddd2ee5adbcf28b3bfc",
    # apply_bq_views 1.27.0 (24 Nov 2026): the hourly company headlines, classified with file 97's rules (research only)
    # apply_bq_views 1.28.0 (24 Nov 2026): the hourly news reading scored against SPY, and its record (research only)
    "v_news_now_eval": "7d9046950a5325177f7368c69e9da072e1f7d9e3e1e2f8aefa25806aa717a6d9",
    "v_news_now_learning": "e369cafc130a7a5b82bad1d75f5597c4ae8a6ead7082a578516636690d112dc6",
    "v_symbol_news_class_now": "38ff5cce252f76514b47b894eaaabd9f6483597dc539aae651493f0ce585f35c",
    # apply_bq_views 1.30.0 (26 Nov 2026): the stop judgment, the value screen and the cash entry - each a shadow
    # record with its fixed reading (research only)
    "v_stop_cause_events": "8f5b013d247da32e99c8251bc8b281edc2364f9243cd1380864aa046c0a4cb70",
    "v_stop_cause_learning": "a434e428ae8d84619c3758ca1bb051b9f96f0e5aee43f63e81973f9b9e59f18f",
    "v_value_screen": "2c61919b54e971443bcf9d69c5a1160575587ddec3b08b8b8d6a57efd7c3384c",
    "v_trial_value_screen": "9b66d56d4f89c567d79e1688037198fb5a7a259f08014613364c1cdbdf8de94e",
    "v_cash_entry_events": "2914c7b31eab17989411bae88c32d2f37b3addf2f2b0d02c222c97fc9ec36fab",
    "v_cash_entry_trial": "04bee31d70cc0f981c19240ae3ff0b330e14a24ee9fd8c0bd8d1230b1fb1a1f6",
    # apply_bq_views 1.31.0 (26 Nov 2026): the forward signal ledger, the gate record and their check against the
    # outcome engine (research only)
    "v_forward_signals": "c94ff5dd391aca769481b5c72a2d815fb78dec410d0ce76299cda49960c47237",
    # (v_forward_signal_gates moves to PENDING: its horizon map replaced in 1.35.0, was f7072173c7a5)
    "v_forward_signal_recon": "a9a1af5acb318e9e39f7f7e1846350cf9bc3daf32a19101a0f715f1514dd700b",
    # apply_bq_views 1.32.0 (26 Nov 2026): the news read twice - the word lists and the language model, each scored
    # against the next sessions, and the days they part (research only)
    "v_news_llm": "a06c1b6fb679009ac58732b23bc2b247d6988290b26f8283879d811be9c68eca",
    # apply_bq_views 1.33.0 (27 Nov 2026): v_news_llm_days enters after the model's reading (audit D1)
    "v_news_llm_days": "2ff795f3b0c42d85eb25b6597e12cf009d2a88cf29de6238ca9c684433dad867",
    "v_news_llm_eval": "b31dbef2f26273328d81d6da15f82b0c6f111cfa6a5d517c21492cd51bbccd67",
    "v_news_llm_duel": "92b377b8bc3beebb12d86ec0820e25c61d03cfead4751b3ef3b22838da430cc0",
    # apply_bq_views 1.34.0 (27 Nov 2026): the shadow challenger v0 - price inputs ranked per market and sector,
    # weights learned walk-forward, a shadow action and size per name, and its scorecard (research only)
    # (1.37.0: the features read v_bars_clean and the ranks measure the result inside one price basis;
    #  were 33a6db6c984f and 2f98c250c059)
    "v_challenger_v0_features": "def69759414e36f8515b2f101b6ce5dfaa04cfad2c0b8b342c77bb9427d325e0",
    "v_challenger_v0_ranks": "df28a4b262e60095f71ba722ecafa9c0f7c18209a0c3aad101081aae1d4b6cd8",
    "v_challenger_v0_walkforward": "0e39890fc235a201810a8d0d1fcc929e5e9aaef68bbabbe00905098555cc6281",
    "v_challenger_v0_scorecard": "c0ee2aa57cf9680afa5a26c100f8fc8928684b5cba4ec7d3b76841ea161a4982",
    # apply_bq_views 1.35.0 (27 Nov 2026): v_outcome_sector reads 'beats' / 'loses' only with t_adj of 2 or more
    # ... and each horizon is divided by its own days in both views that carry the adjusted t (review of PR 1106)
    "v_outcome_sector": "f4be51391e6db23778c7edaf8a2d3050a2dcffd816912d231b163da2930ea593",
    "v_forward_signal_gates": "40e03bcfd820b90c0d6189de2bbc648a6332abb949fe5ee69a3ffee6e481b135",
    # apply_bq_views 1.36.0 (27 Nov 2026): the bar archive check - jumps of 1.5 times or more in the archived closes,
    # the jump that undid each, and a fixed reading per symbol (research only)
    "v_bar_jumps": "061b569c64d3993653678f03cd3fc55daf502c2abb215f9e9371a2f39417c804",
    "v_bar_jump_symbols": "a28abd397adb0792f8fd076f3e8f6018cc1038f0f3fe890a86e88cf6c122a522",
    # apply_bq_views 1.37.0 (27 Nov 2026): the bar archive less its spikes, with a price basis per bar, for the return maths
    "v_bars_clean": "48090d1c66997868cb71969337460bf2a1a052b0f7f8a53e6debd9405e4e2c6a",
    # apply_bq_views 1.38.0 (28 Nov 2026): one shadow regime rule per market (BULL / CHOPPY / RISK_OFF from the
    # 200-close trend, the breadth, the volatility percentile and the US adverse count) and its in-sample record
    "v_regime_shadow": "e184564c679048f513a79dcb2da50c966615c5b12fc2a0fd163bab391814bf3a",
    "v_regime_shadow_insample": "25c44ff7d8d46e4f0cc1d2a4e10d9d5ccd2cd1a1bb85b5a37044eaad617aecc2",
    # apply_bq_views 1.39.0 (28 Nov 2026): the forecast record - what the scored buy calls promised and delivered, per
    # market, horizon and promise third (cut once, on the calls scored by 27 Nov 2026), against their own index
    # (was 142caee5b170 in 1.39.0; 1.40.0 writes the 37 pairs of cuts into the view as constants - same 133 rows)
    "v_forecast_record": "367300190fdf3a53190676eaed0e80752373b19ebbf28fef57ca271b2c48d031",
}
EXPECTED_ORDER = [
    "f_cell", "f_num", "f_ts", "f_col", "f_at", "f_ts_utc",
    "v_board_episodes", "v_performance_log", "v_selection_log", "v_signal_history",
    "v_top10_board", "v_portfolio_decision", "v_my_portfolio", "v_top10_status",
    "v_run_log", "v_archive_health",
    "v_market_daily", "f_market_window", "f_market_summary",
    # views 1.37.0: the bar archive check moves ahead of every view that computes a return, with the clean bars
    "v_bar_jumps", "v_bar_jump_symbols", "v_bars_clean",
    "v_universe", "v_universe_latest", "f_universe_return",
    "v_news_items", "v_econ_events", "v_news_gate", "v_calendar_events", "v_fx_sar", "f_symbol_pick",
    "v_symbol_candles", "v_symbol_analyst", "v_symbol_news", "v_symbol_rating",
    "v_dive_signals", "v_dive_board", "v_dive_events",
    "v_top10_insight", "v_portfolio_insight",
    "v_outcome_engine", "v_outcome_calibration", "v_board_archive", "v_outcome_coverage",
    "v_signal_edge_daily", "v_signal_edge",
    "v_outcome_excess", "v_market_regime", "v_seat_regime", "v_regime_outcomes",
    "v_trial_c1",
    "v_switch_calibration", "v_hold_vs_switch",
    "v_news_signal_eval",
    "v_trial_shadow_v2",
    "v_symbol_sector", "v_outcome_sector", "v_trial_c2",
    "v_forward_universe", "v_investable_vs_market", "v_missed_winners",
    "v_news_theme_weekly", "v_news_sector_weights", "v_news_sector_tilt", "v_news_overlay_list",
    "v_trial_news_overlay",
    "v_c2_calls",
    "v_symbol_news_class", "v_company_news_eval",
    "v_market_condition", "v_market_rank", "v_market_trial", "v_market_trial_summary",
    "v_ready_list", "v_ready_trial",
    "v_market_news", "v_people_news", "v_news_trial", "v_news_trial_summary",
    "v_event_calendar", "v_event_reactions", "v_event_scenarios", "v_event_trial", "v_event_trial_summary",
    "v_learning_scorecard",
    "v_options_reading", "v_market_drivers", "v_market_driver_learning", "v_price_drivers", "v_driver_learning",
    "v_projection_context", "v_projection_scorecard",
    "v_news_trend_daily", "v_news_trend_learning", "v_news_understanding",
    "v_confidence_calibration", "v_shock_drop_events", "v_shock_drop_learning",
    "v_symbol_news_class_now", "v_news_now_eval", "v_news_now_learning",
    "v_stop_cause_events", "v_stop_cause_learning", "v_value_screen", "v_trial_value_screen",
    "v_cash_entry_events", "v_cash_entry_trial",
    "v_forward_signals", "v_forward_signal_gates", "v_forward_signal_recon",
    "v_news_llm", "v_news_llm_days", "v_news_llm_eval", "v_news_llm_duel",
    "v_challenger_v0_features", "v_challenger_v0_ranks", "v_challenger_v0_walkforward", "v_challenger_v0_scorecard",
    # views 1.38.0: the shadow regime rule and its in-sample record
    "v_regime_shadow", "v_regime_shadow_insample",
    # views 1.39.0: the forecast record behind the Daily Plan's honest forecast
    "v_forecast_record",
]
EXPECTED_KINDS = (["FUNCTION"] * 6 + ["VIEW"] * 11 + ["TABLE FUNCTION"] * 2 + ["VIEW"] * 5
                  + ["TABLE FUNCTION"] + ["VIEW"] * 5 + ["TABLE FUNCTION"] + ["VIEW"] * 90)
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
    added = {arc.table_name(t): h for t, h in arc.TABS[10:17]}
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
    assert set(re.findall(r"\be\.([a-z_]+)", code["v_symbol_news_class"])) <= cols["symbol_news"]
    used = {n: set(bv._REF.findall(c)) & bv.DEEP_TABLES for n, c in code.items()}
    assert {n: t for n, t in used.items() if t} == {
        "v_symbol_candles": {"symbol_bars_daily"}, "v_symbol_analyst": {"symbol_analyst_daily"},
        "v_symbol_news": {"symbol_news"},
        "v_outcome_engine": {"symbol_bars_daily"},           # v1.5.0: the engine reads the bars too
        "v_trial_c1": {"symbol_bars_daily"},                  # v1.8.0: the trial reads each call's 1-month price
        "v_news_signal_eval": {"symbol_bars_daily"},         # v1.10.0: each name's next 5 sessions
        "v_symbol_news_class": {"symbol_news"},              # v1.16.0: each headline re-read
        "v_company_news_eval": {"symbol_bars_daily"},        # v1.16.0: the next sessions after the news day
        "v_market_condition": {"symbol_bars_daily"},          # v1.18.0: the Saudi REITs (4330-4350) as one index
        "v_projection_scorecard": {"symbol_bars_daily"},     # v1.24.0: each call's realised volatility before it
        "v_news_trend_daily": {"symbol_bars_daily"},          # v1.25.0: each followed name's sessions around its news
        "v_shock_drop_events": {"symbol_bars_daily"},        # v1.26.0: the largest US names' falls and what followed
        "v_news_now_eval": {"symbol_bars_daily"},              # v1.28.0: the hourly reading's next sessions
        "v_stop_cause_events": {"symbol_bars_daily"},         # v1.30.0: the falls of the followed US and Saudi shares
        "v_forward_signals": {"symbol_bars_daily"},           # v1.31.0: the base bar's close, for the entry-price test
        "v_news_llm_days": {"symbol_bars_daily"},              # v1.32.0: each name's next sessions after its news day
        # v1.34.0: the challenger's price inputs - from v1.37.0 through v_bars_clean, so it reads no deep table itself
        "v_bar_jumps": {"symbol_bars_daily"},                  # v1.36.0: the archive's own closes, checked for jumps
        "v_bar_jump_symbols": {"symbol_bars_daily"},           # v1.36.0: the archive's newest bar, for 'is_new'
        "v_bars_clean": {"symbol_bars_daily"},                 # v1.37.0: the archive less its spikes, with a basis
        # v1.38.0: the regime breadth reads the archive's own closes up to each day (v_bars_clean decides with
        # later bars; Codex P2 on #1113)
        "v_regime_shadow": {"symbol_bars_daily"}}, used
    assert {"symbol", "date", "close", "volume", "source"} <= cols["symbol_bars_daily"]            # what v_bar_jumps reads
    assert set(re.findall(r"\bb\.([a-z_]+)", code["v_outcome_engine"])) <= cols["symbol_bars_daily"]
    assert set(re.findall(r"\bs\.([a-z_]+)", code["v_trial_c1"])) <= cols["symbol_bars_daily"]
    assert {"symbol", "date", "close", "adj_close"} <= cols["symbol_bars_daily"]          # what v_news_signal_eval reads
    assert {"symbol", "date", "close", "captured_at"} <= cols["symbol_bars_daily"]        # what v_projection_scorecard reads
    assert {"symbol", "date", "close", "adj_close", "captured_at"} <= cols["symbol_bars_daily"]  # what v_regime_shadow reads
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
                  "awaiting_bar", "no_bar_at_target", "entry_price_mismatch"):
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
                "COUNTIF(engine_state = 'entry_price_mismatch') AS engine_entry_mismatch",
                "SUM(engine_entry_mismatch) AS engine_entry_mismatch",
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



def _avg_ranks(vals, average=True):
    """1-based ranks of vals as the view computes them: RANK() (1 + the number of
    smaller values) plus (ties - 1) / 2; average=False is the plain RANK() mutant."""
    order = sorted(vals)
    return [order.index(v) + 1 + ((order.count(v) - 1) / 2 if average else 0) for v in vals]


def _pearson(a, b):
    n = len(a)
    ma, mb = sum(a) / n, sum(b) / n
    va = sum((x - ma) ** 2 for x in a)
    vb = sum((y - mb) ** 2 for y in b)
    if not va or not vb:
        return None
    return sum((x - ma) * (y - mb) for x, y in zip(a, b)) / (va * vb) ** 0.5


def _edge_day(xs, rs, min_n=20, average=True):
    """v_signal_edge_daily for one day / market group / feature / horizon: (n, ic,
    top_ex, bottom_ex), or None under the floor (>= 20 names, two distinct values)."""
    pairs = [(x, r) for x, r in zip(xs, rs) if x is not None and r is not None and abs(r) <= 300]
    n = len(pairs)
    xk = [int(round(x * 10000)) for x, _ in pairs]
    if n < min_n or len(set(xk)) < 2:
        return None
    rx = _avg_ranks(xk, average)
    rr = _avg_ranks([int(round(r * 10000)) for _, r in pairs], average)
    mean = sum(r for _, r in pairs) / n
    top = [r - mean for (_, r), q in zip(pairs, rx) if q > 2 * n / 3]
    bot = [r - mean for (_, r), q in zip(pairs, rx) if q <= n / 3]
    return (n, _pearson(rx, rr), sum(top) / len(top) if top else None,
            sum(bot) / len(bot) if bot else None)


def _shadow_v2(target_upside, engine_roi_1m, w=(0.6, 0.4)):
    """v_signal_edge_daily's shadow_v2 for one day and market group: PERCENT_RANK (the share of
    the other names strictly below, 0 when alone) of each input among the names that carry
    both, values tied at 4 decimals; None for a name missing either."""
    idx = [i for i, (t, e) in enumerate(zip(target_upside, engine_roi_1m)) if t is not None and e is not None]
    tk = {i: int(round(target_upside[i] * 10000)) for i in idx}
    ek = {i: int(round(engine_roi_1m[i] * 10000)) for i in idx}

    def pr(keys, i):
        return sum(1 for j in idx if keys[j] < keys[i]) / (len(idx) - 1) if len(idx) > 1 else 0.0

    out = [None] * len(target_upside)
    for i in idx:
        out[i] = round(w[0] * pr(tk, i) + w[1] * pr(ek, i), 10)
    return out


def _own_run_signal(signals, rec_at, after=60.0, days=3):
    """The signal v_signal_edge_daily joins (times in seconds): the earliest
    (recorded_at, value) 0-`after` s after the forecast - the snapshot its own
    tracker run writes seconds later - else the newest one before it, at most
    `days` older; None when there is none."""
    own = [s for s in signals if rec_at <= s[0] <= rec_at + after]
    if own:
        return min(own)[1]
    prior = [s for s in signals if rec_at - days * 86400 <= s[0] < rec_at]
    return max(prior)[1] if prior else None


def case_v28():
    import re
    objs = {o.name: o for o in bv.load_objects()}
    o = objs["v_signal_edge_daily"]
    assert o.path.name == "74_v_signal_edge_daily.sql" and o.kind == "VIEW"
    code = bv._code(o.template)
    assert set(bv._REF.findall(code)) - {"v_signal_edge_daily"} == {"v_outcome_engine", "v_signal_history"}
    for frag in (
            # the day's first recording time only: a later re-record cannot borrow the day's move
            "SELECT symbol, recorded_date, MIN(recorded_at) AS first_at",
            "ON f.symbol = e.symbol AND f.recorded_date = e.recorded_date AND f.first_at = e.recorded_at",
            # the scoring signals of the forecast's own run (written seconds after it), else the
            # newest earlier one - never a later run (Codex P2 on #932)
            "AND s.recorded_at <= TIMESTAMP_ADD(r.rec_at, INTERVAL 60 SECOND)",
            "AND s.recorded_at >= TIMESTAMP_SUB(r.rec_at, INTERVAL 3 DAY)",
            "ORDER BY IF(s.recorded_at >= r.rec_at, 0, 1),",
            "IF(s.recorded_at >= r.rec_at, UNIX_MICROS(s.recorded_at), -UNIX_MICROS(s.recorded_at))) = 1",
            "IF(j.market IN ('US', 'Tadawul'), j.market, 'Other markets') AS market_group",
            "UNPIVOT (r FOR horizon IN (r_1w AS '1W', r_2w AS '2W', r_1m AS '1M', r_3m AS '3M'))",
            "WHERE ABS(r) <= 300\n",
            "CAST(ROUND(x * 10000) AS INT64) AS xk",
            "CAST(ROUND(r * 10000) AS INT64) AS rk",
            "RANK() OVER (PARTITION BY d, market_group, feature, horizon ORDER BY xk)",
            "+ (COUNT(*) OVER (PARTITION BY d, market_group, feature, horizon, xk) - 1) / 2 AS rx",
            "RANK() OVER (PARTITION BY d, market_group, feature, horizon ORDER BY rk)",
            "+ (COUNT(*) OVER (PARTITION BY d, market_group, feature, horizon, rk) - 1) / 2 AS rr",
            "COUNT(DISTINCT xk) OVER (PARTITION BY d, market_group, feature, horizon) AS nx",
            "CORR(rx, rr) AS ic",
            "AVG(IF(rx > 2 * n / 3, r - r_mean, NULL)) AS top_ex",
            "AVG(IF(rx <= n / 3, r - r_mean, NULL)) AS bottom_ex",
            "WHERE n >= 20 AND nx >= 2",
            "CASE horizon WHEN '1W' THEN 7 WHEN '2W' THEN 14 WHEN '1M' THEN 30 WHEN '3M' THEN 91 END AS horizon_days",
            "ROUND(top_ex - bottom_ex, 4) AS spread_pp"):
        assert frag in code, frag
    feats = re.search(r"UNPIVOT \(x FOR feature IN \(([^)]*)\)\)", code).group(1)
    assert [f.strip() for f in feats.split(",")] == [
        "engine_roi_1w", "engine_roi_2w", "engine_roi_1m", "engine_roi_3m", "reliability", "data_quality",
        "confidence", "recommendation", "investability", "stance", "overall_score", "risk_score",
        "target_upside", "shadow_v2", "shadow_v2_matched", "overall_score_matched"], feats
    # shadow_v2 (1.11.0): the challenger score, ranked within the day and market group among the
    # names that carry BOTH inputs; no confidence, reliability or overall_score term; no outcome
    for frag in (
            "WHERE target_upside IS NOT NULL AND engine_roi_1m IS NOT NULL",
            "CAST(ROUND(target_upside * 10000) AS INT64) AS tk",
            "CAST(ROUND(engine_roi_1m * 10000) AS INT64) AS ek",
            "0.6 * PERCENT_RANK() OVER (PARTITION BY d, market_group ORDER BY tk)",
            "+ 0.4 * PERCENT_RANK() OVER (PARTITION BY d, market_group ORDER BY ek) AS shadow_v2",
            "LEFT JOIN sh AS s ON s.d = g.d AND s.market_group = g.market_group AND s.symbol = g.symbol",
            # the matched pair v_trial_shadow_v2 compares: each score only on the names carrying both
            # (Codex P2 on #1023)
            "IF(g.overall_score IS NOT NULL, s.shadow_v2, NULL) AS shadow_v2_matched",
            "IF(s.shadow_v2 IS NOT NULL, g.overall_score, NULL) AS overall_score_matched",
            "FROM g2\n  UNPIVOT (x FOR feature IN ("):
        assert frag in code, frag
    shadow = code[code.index("both AS ("):code.index("g2 AS (")]
    for word in ("confidence", "reliability", "overall_score", "r_1w", "r_2w", "r_1m", "r_3m", "roi_1"):
        assert word not in shadow.replace("engine_roi_1m", ""), word
    # BigQuery refuses a window partitioned by a FLOAT64: no window partitions by the raw x or r
    parts = re.findall(r"PARTITION BY ([a-z_, .]+?)(?:\s+ORDER BY|\))", code)
    assert len(parts) == 10, parts                      # the as-of pick, the two shadow ranks, the seven rank windows
    for part in parts:
        keys = [k.strip() for k in part.split(",")]
        assert "x" not in keys and "r" not in keys, part
    assert "the live system never reads BigQuery" in o.template
    # simulated: a perfect order, its reverse, the floor, the 300 % filter, the thirds
    xs = [float(i) for i in range(1, 21)]
    n, ic, top, bot = _edge_day(xs, [2.0 * x for x in xs])
    assert n == 20 and abs(ic - 1.0) < 1e-12 and top > 0 > bot
    assert abs(_edge_day(xs, [-x for x in xs])[1] + 1.0) < 1e-12
    assert _edge_day(xs[:19], xs[:19]) is None                               # 19 names: no IC that day
    # shadow_v2: 0.6 x day rank of the target upside + 0.4 x day rank of the 1M engine ROI
    sv = _shadow_v2([10.0, 20.0, 30.0, None, 40.0], [3.0, 1.0, 2.0, 9.0, None])
    assert sv == [0.4, 0.3, 0.8, None, None], sv            # a name missing either input has no score
    assert _shadow_v2([5.0], [1.0]) == [0.0]                 # alone on its day: the lowest
    assert _shadow_v2([1.0, 1.0, 2.0], [7.0, 7.0, 7.0]) == [0.0, 0.0, 0.6]   # ties share the lower rank
    assert _shadow_v2([1.0, 2.0, 3.0], [3.0, 2.0, 1.0], w=(0.5, 0.5)) == [0.5, 0.5, 0.5], "equal weights erase the order"
    assert _shadow_v2([1.0, 2.0, 3.0], [3.0, 2.0, 1.0]) == [0.4, 0.5, 0.6]   # 0.6 / 0.4: the target upside leads
    assert _edge_day([5.0] * 20, xs) is None                                 # one distinct value
    assert _edge_day(xs + [21.0], xs + [400.0])[0] == 20                     # a 400 % "return" is a data error
    x21 = [float(i) for i in range(1, 22)]
    day = _edge_day(x21, x21)
    assert day[2] == (sum(range(15, 22)) / 7 - 11) and day[3] == (sum(range(1, 8)) / 7 - 11)  # 7 + 7 of 21
    # ties: average ranks (the tie-corrected Spearman); plain RANK() is a golden negative
    # (12 names at x = 1 split 6 / 6 between the lowest and the highest return, 8 names at
    # x = 2 in the middle: no association, IC 0; min ranks of the uneven return ties invent -0.09)
    tied = [1.0] * 12 + [2.0] * 8
    ret = [1.0] * 6 + [3.0] * 6 + [2.0] * 8
    good = _edge_day(tied, ret)[1]
    assert abs(good) < 1e-12 and abs(good - _pearson(_avg_ranks(tied), _avg_ranks(ret))) < 1e-12
    assert _edge_day(tied, ret, average=False)[1] < -0.05, "min ranks must not pass for average ranks"
    # the forecast's own run: the tracker appends the forecast, then writes that run's snapshot
    # seconds later; the next run comes 68 s or more after (times in seconds)
    rec = 1000.0
    sig = [(rec - 18000, "previous run"), (rec + 2, "own run"), (rec + 3, "own run, 2nd write"),
           (rec + 68, "next run"), (rec + 28800, "later run")]
    assert _own_run_signal(sig, rec) == "own run"
    strict = max(s for s in sig if s[0] <= rec)[1]                            # the 1st version's cut
    assert strict == "previous run" != _own_run_signal(sig, rec)
    newest_in_5min = max(s for s in sig if s[0] <= rec + 300)[1]              # a wider, newest-first window
    assert newest_in_5min == "next run" != _own_run_signal(sig, rec)
    assert _own_run_signal([(rec - 600, "prior"), (rec + 68, "next run")], rec) == "prior"
    assert _own_run_signal([(rec - 4 * 86400, "too old"), (rec + 120, "next run")], rec) is None


def _edge_verdict(days, horizon_days, mean_ic, sd_ic, m1, m2):
    """v_signal_edge's t_adj and verdict, arm for arm."""
    t = mean_ic / sd_ic * (days / horizon_days) ** 0.5 if sd_ic else None
    if days < 10 or days < 2 * horizon_days:
        return "thin", t
    if t is not None and t >= 2 and m1 > 0 and m2 > 0:
        return "edge", t
    if t is not None and t <= -2 and m1 < 0 and m2 < 0:
        return "inverse", t
    if t is not None and abs(t) < 1:
        return "no edge", t
    return "unstable", t


def case_v29():
    import re
    objs = {o.name: o for o in bv.load_objects()}
    o = objs["v_signal_edge"]
    assert o.path.name == "75_v_signal_edge.sql" and o.kind == "VIEW"
    names = [x.name for x in bv.load_objects()]
    assert names.index("v_signal_edge") == names.index("v_signal_edge_daily") + 1   # applies after its source
    code = bv._code(o.template)
    assert set(bv._REF.findall(code)) - {"v_signal_edge"} == {"v_signal_edge_daily"}
    for frag in ("WHERE ic IS NOT NULL",
                 "NTILE(2) OVER (PARTITION BY market_group, feature, horizon ORDER BY signal_day) AS half",
                 "AVG(IF(half = 1, ic, NULL)) AS m1",
                 "AVG(IF(half = 2, ic, NULL)) AS m2",
                 "SAFE_DIVIDE(m, sd) * SQRT(days / horizon_days) AS t_adj",
                 "ROUND(pos * 100, 1) AS pct_days_positive",
                 "WHEN days < 10 OR days < 2 * horizon_days THEN 'thin'",
                 "WHEN t_adj >= 2 AND m1 > 0 AND m2 > 0 THEN 'edge'",
                 "WHEN t_adj <= -2 AND m1 < 0 AND m2 < 0 THEN 'inverse'",
                 "WHEN ABS(t_adj) < 1 THEN 'no edge'",
                 "ELSE 'unstable'"):
        assert frag in code, frag
    assert re.findall(r"THEN '([a-z ]+)'", code) == ["thin", "edge", "inverse", "no edge"]  # thin is judged first
    assert "the live system never reads BigQuery" in o.template
    # simulated verdicts (98 days of a 1W signal: SQRT(98 / 7) = 3.74)
    assert _edge_verdict(98, 7, 0.10, 0.15, 0.08, 0.12)[0] == "edge"
    assert _edge_verdict(98, 7, 0.10, 0.15, 0.20, -0.01)[0] == "unstable"     # the sign flipped: not an edge
    assert _edge_verdict(98, 7, -0.10, 0.15, -0.08, -0.12)[0] == "inverse"
    assert _edge_verdict(98, 7, 0.02, 0.15, 0.01, 0.03)[0] == "no edge"
    assert _edge_verdict(98, 7, 0.05, 0.15, 0.04, 0.06)[0] == "unstable"      # t 1.25
    assert _edge_verdict(9, 7, 0.30, 0.10, 0.3, 0.3)[0] == "thin"
    assert _edge_verdict(50, 30, 0.30, 0.10, 0.3, 0.3)[0] == "thin"           # under two 1M lengths
    assert _edge_verdict(150, 91, 0.30, 0.10, 0.3, 0.3)[0] == "thin"
    # golden negative: the naive t (no overlap correction) calls 90 days of a 1M signal an edge
    verdict, t = _edge_verdict(90, 30, 0.06, 0.20, 0.05, 0.07)
    naive = 0.06 / 0.20 * 90 ** 0.5
    assert naive >= 2 and verdict == "no edge" and abs(t - 0.3 * 3 ** 0.5) < 1e-12


def _total_return_price(out_close, out_adj, base_close, base_adj, use_adj=True):
    """v_outcome_engine 1.6.0: an outcome price restated in entry-day terms - outcome adj x base
    close / base adj - or the raw close when either adj is missing (use_adj=False is the 1.5.0
    raw-close mutant)."""
    if use_adj and out_adj is not None and base_adj is not None and base_adj > 0 and base_close > 0:
        return out_adj * base_close / base_adj
    return out_close


def _engine_roi(entry, out_close, out_adj, base_close, base_adj, use_adj=True):
    return round((_total_return_price(out_close, out_adj, base_close, base_adj, use_adj) / entry - 1) * 100, 4)


def _alias_bars(bars, aliases):
    """the bars CTE: an aliased old symbol's bars are the NEW symbol's bars, relabelled; every other
    symbol keeps its own; the old symbol's own partial series is set aside."""
    old = {o for o, _ in aliases}
    out = [dict(b, bars_symbol=b["symbol"]) for b in bars if b["symbol"] not in old]
    for o, n in aliases:
        out += [dict(b, symbol=o, bars_symbol=n) for b in bars if b["symbol"] == n]
    return out


def _entry_mismatch(entry, base_close, band=0.35):
    return base_close > 0 and (entry / base_close > 1 + band or entry / base_close < 1 - band)


def case_v30():
    objs = {o.name: o for o in bv.load_objects()}
    o = objs["v_outcome_engine"]
    code = bv._code(o.template)
    assert set(bv._REF.findall(code)) - {"v_outcome_engine"} == {"v_performance_log", "symbol_bars_daily"}
    for frag in (
            "('INDV.US', 'SUPN.US')",
            "LEFT JOIN alias AS al ON al.old_symbol = b.symbol",
            "WHERE al.old_symbol IS NULL",
            "SELECT al.old_symbol AS symbol, b.symbol AS bars_symbol, b.date, b.high, b.low, b.close, b.adj_close, b.source",
            "JOIN alias AS al ON al.new_symbol = b.symbol",
            "ARRAY_AGG(STRUCT(b.date AS date, b.close AS close, b.adj_close AS adj) ORDER BY b.date LIMIT 1)[SAFE_OFFSET(0)] AS base",
            "IF(a.tb.adj IS NOT NULL AND a.base.adj > 0 AND a.base.close > 0, a.tb.adj * a.base.close / a.base.adj, a.tb.close) AS target_close_adj",
            "IF(sl.last_adj IS NOT NULL AND a.base.adj > 0 AND a.base.close > 0, sl.last_adj * a.base.close / a.base.adj, sl.last_close) AS last_close_adj",
            "WHEN a.base.close > 0 AND (r.entry_price / a.base.close > 1.35 OR r.entry_price / a.base.close < 0.65) THEN 'entry_price_mismatch'",
            "ROUND((SAFE_DIVIDE(j.target_close_adj, j.entry_price) - 1) * 100, 4), NULL) AS engine_roi_pp",
            "ROUND((SAFE_DIVIDE(j.target_close, j.entry_price) - 1) * 100, 4), NULL) AS engine_roi_raw_pp",
            "ROUND(c.engine_roi_pp - c.engine_roi_raw_pp, 4), NULL) AS dividend_adj_pp",
            "SAFE_DIVIDE(c.last_close_adj, c.entry_price)",
            "c.bars_symbol, c.base_bar_date,"):
        assert frag in code, frag
    for step, days in (("l1w", 7), ("l2w", 14), ("l1m", 30), ("l3m", 91)):
        assert f"IF(b.date >= DATE_ADD(r.recorded_day, INTERVAL {days} DAY), STRUCT(b.close AS close, b.adj_close AS adj), NULL) IGNORE NULLS ORDER BY b.date LIMIT 1)[SAFE_OFFSET(0)] AS {step}" in code, step
        assert f"IF(a.{step}.adj IS NOT NULL AND a.base.adj > 0 AND a.base.close > 0, a.{step}.adj * a.base.close / a.base.adj, a.{step}.close) AS close_" in code, step
    # the mismatch test sits after the bar check and before scoring
    i_bar, i_mm, i_sc = (code.index("THEN 'no_bar_for_symbol'"), code.index("THEN 'entry_price_mismatch'"),
                         code.index("THEN 'scored_from_bar'"))
    assert i_bar < i_mm < i_sc
    # excursions and the touched target stay on raw highs and lows
    assert "a.tb.high >= r.target_price" in code and "b.adj_close AS adj, b.source" in code
    # simulated: SUPN - bought 35.43 on 2 Nov (base bar close 36.28 / adj 28.15, the $8.13 special
    # dividend of 3 Nov already in the adj series); 6 Nov close 26.16 / adj 26.16
    assert _engine_roi(35.43, 26.16, 26.16, 36.28, 28.15) == -4.8398          # the economic result
    assert _engine_roi(35.43, 26.16, 26.16, 36.28, 28.15, use_adj=False) == -26.1643   # the 1.5.0 false loss
    assert round(_engine_roi(35.43, 26.16, 26.16, 36.28, 28.15) - _engine_roi(35.43, 26.16, 26.16, 36.28, 28.15, use_adj=False), 4) == 21.3245
    # a 2:1 split: close halves, adj series continuous -> 0 %
    assert _engine_roi(100.0, 50.0, 50.0, 100.0, 50.0) == 0.0
    assert _engine_roi(100.0, 50.0, 50.0, 100.0, 50.0, use_adj=False) == -50.0
    # a missing adj on either bar falls back to the raw close
    assert _engine_roi(10.0, 11.0, None, 10.0, 10.0) == 10.0 and _engine_roi(10.0, 11.0, 11.0, 10.0, None) == 10.0
    # alias: the record keeps INDV.US, the bars are SUPN.US's; INDV's own stub is set aside
    bars = [{"symbol": "INDV.US", "date": 1, "close": 36.26, "adj_close": None},
            {"symbol": "SUPN.US", "date": 1, "close": 36.28, "adj_close": 28.15},
            {"symbol": "SUPN.US", "date": 2, "close": 28.15, "adj_close": 28.15},
            {"symbol": "FRST.US", "date": 1, "close": 15.0, "adj_close": 14.9}]
    ab = _alias_bars(bars, [("INDV.US", "SUPN.US")])
    indv = [b for b in ab if b["symbol"] == "INDV.US"]
    assert [(b["date"], b["close"], b["bars_symbol"]) for b in sorted(indv, key=lambda b: b["date"])] == \
        [(1, 36.28, "SUPN.US"), (2, 28.15, "SUPN.US")]
    assert all(b["adj_close"] is not None for b in indv)                     # the old stub (no adj) is gone
    assert [b["bars_symbol"] for b in ab if b["symbol"] == "FRST.US"] == ["FRST.US"]
    assert not [b for b in ab if b["symbol"] == "SUPN.US" and b["bars_symbol"] != "SUPN.US"]
    assert len(ab) == 5
    # entry price mismatch: the stale 41.92 pre-merger quote against the merged company's 28.15
    assert _entry_mismatch(41.92, 28.15) and not _entry_mismatch(35.43, 36.28) and not _entry_mismatch(13.0, 10.0)
    assert _entry_mismatch(6.0, 10.0) and not _entry_mismatch(6.6, 10.0)
    assert _entry_mismatch(13.0, 10.0, band=0.10), "a 10 % band would refuse an ordinary intraday move"


def _excess(roi, bench, stance):
    if roi is None or bench is None:
        return None, None
    ex = round(roi - bench, 4)
    win = (roi > bench) if stance == "bullish" else (roi < bench)
    return ex, "WIN" if win else "LOSS"


def _bench_return(series, start_date, end_date):
    """first benchmark close on/after each date (the engine's own rule for the stock)."""
    s = min((b for b in series if b["date"] >= start_date), key=lambda b: b["date"], default=None)
    e = min((b for b in series if b["date"] >= end_date), key=lambda b: b["date"], default=None)
    return None if s is None or e is None else round((e["close"] / s["close"] - 1) * 100, 4)


def case_v31():
    objs = {o.name: o for o in bv.load_objects()}
    o = objs["v_outcome_excess"]
    assert o.path.name == "76_v_outcome_excess.sql" and o.kind == "VIEW"
    code = bv._code(o.template)
    assert set(bv._REF.findall(code)) - {"v_outcome_excess"} == {"v_outcome_engine", "market_series_daily"}
    names = [x.name for x in bv.load_objects()]
    assert names.index("v_outcome_excess") > names.index("v_outcome_engine")
    for frag in ("('US', 'SPY'), ('Tadawul', '^TASI.SR'), ('Hong Kong', '^HSI'), ('London', '^FTSE'),",
                 "('Europe', '^STOXX50E'), ('Tokyo', '^N225')",
                 "COALESCE(m.benchmark, 'ACWI') AS benchmark_symbol",
                 # the benchmark runs from the engine's base bar, never from the recording day
                 "ARRAY_AGG(IF(b.date >= e.base_bar_date, b.close, NULL) IGNORE NULLS ORDER BY b.date LIMIT 1)[SAFE_OFFSET(0)] AS bench_base",
                 "ARRAY_AGG(IF(e.target_bar_date IS NOT NULL AND b.date >= e.target_bar_date, b.close, NULL) IGNORE NULLS ORDER BY b.date LIMIT 1)[SAFE_OFFSET(0)] AS bench_target",
                 "AND b.date >= e.base_bar_date",
                 # review on #934 (P2): the scan reaches the target bar for every horizon
                 # (6M / 1Y) and keeps the 98-day ladder window, as the engine's own scan does
                 "AND b.date <= GREATEST(DATE_ADD(e.recorded_day, INTERVAL 98 DAY),",
                 "DATE_ADD(COALESCE(e.target_bar_date, e.recorded_day), INTERVAL 7 DAY))",
                 "WHERE e.base_bar_date IS NOT NULL",
                 "ROUND(x.engine_roi_pp - x.bench_roi_pp, 4) AS excess_pp",
                 "WHEN x.entry_stance = 'bullish' THEN IF(x.engine_roi_pp > x.bench_roi_pp, 'WIN', 'LOSS')",
                 "WHEN x.entry_stance = 'bearish' THEN IF(x.engine_roi_pp < x.bench_roi_pp, 'WIN', 'LOSS')",
                 "ROUND(x.roi_3m_pp - x.bench_3m_pp, 4) AS excess_3m_pp"):
        assert frag in code, frag
    for days in (7, 14, 30, 91):
        assert f"IF(b.date >= DATE_ADD(e.recorded_day, INTERVAL {days} DAY), b.close, NULL) IGNORE NULLS ORDER BY b.date LIMIT 1)[SAFE_OFFSET(0)] AS bench_" in code, days
    assert "the live system never reads BigQuery" in o.template
    # simulated: the 5 Nov week - a -3 % call in a -4.4 % market beat it
    assert _excess(-3.0, -4.4, "bullish") == (1.4, "WIN")
    assert _excess(-3.0, -4.4, "bearish") == (1.4, "LOSS")                    # a short that fell less than the market lost
    assert _excess(2.0, 3.0, "bullish") == (-1.0, "LOSS") and _excess(None, 1.0, "bullish") == (None, None)
    spy = [{"date": 1, "close": 100.0}, {"date": 2, "close": 101.0}, {"date": 5, "close": 95.6}, {"date": 8, "close": 97.0}]
    assert _bench_return(spy, 2, 5) == -5.3465                                # base bar day 2 (recorded on a weekend day 1 -> first bar day 2)
    assert _bench_return(spy, 1, 5) == -4.4                                   # from the recording day: the wrong window
    assert _bench_return(spy, 2, 6) == -3.9604                                # day 6 has no bar: the first on/after (day 8)
    assert _bench_return(spy, 2, 9) is None
    # a 1Y horizon: the target bar sits far beyond 98 days; the scan must reach it
    assert "AND b.date <= DATE_ADD(e.recorded_day, INTERVAL 98 DAY)\n" not in code
    long_spy = [{"date": 2, "close": 100.0}, {"date": 90, "close": 104.0}, {"date": 366, "close": 110.0}]
    assert _bench_return(long_spy, 2, 365) == 10.0                            # found at the 1Y target bar
    assert _bench_return([b for b in long_spy if b["date"] <= 98], 2, 365) is None   # the old 98-day cut: NULL


def _regime(r1, r5, dd, vix, vol20, vs50):
    if r1 is not None and r1 <= -2:
        return "crash_day"
    if (r5 is not None and r5 <= -3) or (dd is not None and dd <= -8):
        return "drawdown"
    if (vix is not None and vix >= 25) or (vol20 is not None and vol20 >= 25):
        return "stress"
    if vs50 is not None and vs50 < 0:
        return "weak"
    return "calm"


def case_v32():
    objs = {o.name: o for o in bv.load_objects()}
    o = objs["v_market_regime"]
    assert o.path.name == "77_v_market_regime.sql" and o.kind == "VIEW"
    code = bv._code(o.template)
    assert set(bv._REF.findall(code)) - {"v_market_regime"} == {"market_series_daily"}
    for frag in ("WHERE symbol = 'SPY' AND close IS NOT NULL AND close > 0",
                 "WHERE symbol = '^VIX' AND close IS NOT NULL",
                 "LAG(s.close, 1) OVER (ORDER BY s.date) AS c1", "LAG(s.close, 5) OVER (ORDER BY s.date) AS c5",
                 "LAG(s.close, 20) OVER (ORDER BY s.date) AS c20",
                 "COUNT(*) OVER (ORDER BY l.date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS n50",
                 "AVG(l.close) OVER (ORDER BY l.date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma50",
                 "MAX(l.close) OVER (ORDER BY l.date ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) AS high250",
                 "STDDEV_SAMP(SAFE_DIVIDE(l.close, l.c1) - 1)",
                 "OVER (ORDER BY l.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sd20",
                 "IF(w.n50 = 50, ROUND((SAFE_DIVIDE(w.spy_close, w.ma50) - 1) * 100, 4), NULL) AS spy_vs_50d_pct",
                 "IF(w.n20 = 20, ROUND(w.sd20 * SQRT(252) * 100, 4), NULL) AS spy_vol20_pct",
                 "LEFT JOIN vix AS v ON v.date = w.date",
                 "WHEN m.spy_r1_pct <= -2 THEN 'crash_day'",
                 "WHEN m.spy_r5_pct <= -3 OR m.spy_dd_from_high_pct <= -8 THEN 'drawdown'",
                 "WHEN m.vix_close >= 25 OR m.spy_vol20_pct >= 25 THEN 'stress'",
                 "WHEN m.spy_vs_50d_pct < 0 THEN 'weak'",
                 "ELSE 'calm'"):
        assert frag in code, frag
    import re
    assert re.findall(r"THEN '([a-z_]+)'", code) == ["crash_day", "drawdown", "stress", "weak"]   # fixed order
    assert "the live system never reads BigQuery" in o.template
    # simulated: 5 Nov 2026 (SPY -4.41 %) is a crash day whatever else is true
    assert _regime(-4.41, -2.7, -5.4, None, 18.0, 1.0) == "crash_day"
    assert _regime(0.3, -3.1, -5.0, None, 18.0, 1.0) == "drawdown"
    assert _regime(0.3, -1.0, -8.5, None, 18.0, 1.0) == "drawdown"
    assert _regime(0.3, -1.0, -2.0, 26.0, 18.0, 1.0) == "stress"
    assert _regime(0.3, -1.0, -2.0, None, 25.0, 1.0) == "stress"
    assert _regime(0.3, -1.0, -2.0, None, 18.0, -0.5) == "weak"
    assert _regime(0.3, -1.0, -2.0, None, 18.0, 0.5) == "calm"
    assert _regime(0.3, -1.0, -2.0, None, 18.0, None) == "calm"               # the first 49 sessions: no 50-day verdict
    assert _regime(-2.0, 1.0, 0.0, 12.0, 10.0, 2.0) == "crash_day"           # the boundary belongs to the crash


def _seat_regime(seat_date, regime_days):
    """the LAST regime date strictly before the seat date (a Monday seat reads Friday)."""
    prior = [d for d in regime_days if d < seat_date]
    return max(prior) if prior else None


def _regime_outcomes(rows, min_scored=30, min_days=5):
    out = {}
    groups = {"ALL": rows, "USD": [r for r in rows if r["ccy"] == "USD"]}
    for g, rs in groups.items():
        for regime in sorted({r["regime"] for r in rs if r["regime"] is not None}):
            k = [r for r in rs if r["regime"] == regime]
            scored = [r for r in k if r["ret5"] is not None]
            days = len({r["seat_date"] for r in k})
            out[(g, regime)] = (len(k), days, len(scored),
                                round(sum(r["ret5"] for r in scored) / len(scored), 2) if scored else None,
                                "small sample" if len(scored) < min_scored or days < min_days else "")
    return out


def case_v33():
    objs = {o.name: o for o in bv.load_objects()}
    seat, outc = objs["v_seat_regime"], objs["v_regime_outcomes"]
    assert seat.path.name == "78_v_seat_regime.sql" and outc.path.name == "79_v_regime_outcomes.sql"
    code = bv._code(seat.template)
    assert set(bv._REF.findall(code)) - {"v_seat_regime"} == {"v_board_episodes", "v_market_regime"}
    for frag in ("WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `{{DS}}.v_board_episodes`)",
                 "LEAD(r.date) OVER (ORDER BY r.date) AS next_date",
                 "ON g.date < s.seat_date", "AND (g.next_date >= s.seat_date OR g.next_date IS NULL)",
                 "g.date AS regime_date", "g.regime,", "g.vix_close"):
        assert frag in code, frag
    assert "g.date <= s.seat_date" not in code                                 # never the seat day's own close
    code2 = bv._code(outc.template)
    assert set(bv._REF.findall(code2)) - {"v_regime_outcomes"} == {"v_seat_regime"}
    for frag in ("WHERE regime IS NOT NULL", "SELECT 'USD' AS ccy_group, s.* FROM s WHERE s.ccy = 'USD'",
                 "SELECT 'ALL' AS ccy_group, s.* FROM s\n", "COUNT(DISTINCT seat_date) AS days",
                 "COUNTIF(ret_plus5_pct IS NOT NULL) AS scored5", "ROUND(AVG(win_plus5) * 100, 1) AS win5_pct",
                 "IF(scored5 < 30 OR days < 5, 'small sample', '') AS sample_note"):
        assert frag in code2, frag
    names = [x.name for x in bv.load_objects()]
    assert names.index("v_market_regime") < names.index("v_seat_regime") < names.index("v_regime_outcomes")
    # simulated: regime dates are US sessions; a Monday-9 seat reads Friday-6, never Monday's own close
    assert _seat_regime(9, [5, 6, 9, 10]) == 6 and _seat_regime(7, [5, 6, 9, 10]) == 6 and _seat_regime(5, [5, 6]) is None
    rows = ([{"ccy": "USD", "regime": "calm", "seat_date": d, "ret5": 1.0} for d in range(1, 7)] * 6
            + [{"ccy": "EUR", "regime": "calm", "seat_date": 1, "ret5": 2.0}]
            + [{"ccy": "USD", "regime": "crash_day", "seat_date": 20, "ret5": -3.5}] * 7
            + [{"ccy": "USD", "regime": "weak", "seat_date": 30, "ret5": None}])
    o = _regime_outcomes(rows)
    assert o[("USD", "calm")] == (36, 6, 36, 1.0, "") and o[("ALL", "calm")] == (37, 6, 37, 1.03, "")
    assert o[("USD", "crash_day")] == (7, 1, 7, -3.5, "small sample")          # the 8 Nov state of the archive
    assert o[("USD", "weak")] == (1, 1, 0, None, "small sample")
    assert ("ALL", "crash_day") in o and ("EUR", "calm") not in o

def _matched_pair(shadow, overall):
    """shadow_v2_matched / overall_score_matched: each score only on the names carrying both."""
    keep = [s is not None and o is not None for s, o in zip(shadow, overall)]
    return ([s if k else None for s, k in zip(shadow, keep)], [o if k else None for o, k in zip(overall, keep)])


def _shadow_reading(period, days, horizon_days, m_s, dm, dsd, top_s, top_o):
    """v_trial_shadow_v2's t_adj and reading, arm for arm."""
    t = dm / dsd * (days / horizon_days) ** 0.5 if dsd else None
    need = max(20, 2 * horizon_days)
    if period != "forward from 2026-11-19":
        return "in-sample: the weights were chosen on these days, not evidence", t
    if days < need:
        return f"collecting: {days} of {need} days", t
    if t is not None and t >= 2 and m_s > 0 and top_s > top_o:
        return "shadow ahead: the owner may consider it", t
    if dm <= 0:
        return "shadow not ahead: keep the current score", t
    return "shadow ahead but not yet clear (t under 2): keep collecting", t


def case_v34():
    objs = {o.name: o for o in bv.load_objects()}
    o = objs["v_trial_shadow_v2"]
    assert o.path.name == "84_v_trial_shadow_v2.sql" and o.kind == "VIEW"
    names = [x.name for x in bv.load_objects()]
    assert names.index("v_trial_shadow_v2") > names.index("v_signal_edge_daily")      # applies after its source
    code = bv._code(o.template)
    assert set(bv._REF.findall(code)) - {"v_trial_shadow_v2"} == {"v_signal_edge_daily"}
    for frag in ("WHERE feature IN ('shadow_v2_matched', 'overall_score_matched')",   # the same names on both sides
                 "MAX(IF(feature = 'shadow_v2_matched', n, NULL)) AS n_s",
                 "MAX(IF(feature = 'overall_score_matched', n, NULL)) AS n_o",
                 "MAX(IF(feature = 'shadow_v2_matched', ic, NULL)) AS ic_s",
                 "MAX(IF(feature = 'overall_score_matched', ic, NULL)) AS ic_o",
                 "MAX(IF(feature = 'shadow_v2_matched', top_ex_pp, NULL)) AS top_s",
                 "MAX(IF(feature = 'overall_score_matched', top_ex_pp, NULL)) AS top_o",
                 "WHERE ic_s IS NOT NULL AND ic_o IS NOT NULL AND n_s = n_o",          # the same days, the same count
                 "IF(signal_day >= DATE '2026-11-19', 'forward from 2026-11-19', 'in-sample, not evidence') AS period",
                 "AVG(ic_s - ic_o) AS dm",
                 "STDDEV_SAMP(ic_s - ic_o) AS dsd",
                 "SAFE_DIVIDE(dm, dsd) * SQRT(days / horizon_days) AS t_x",
                 "GREATEST(20, 2 * horizon_days) AS need",
                 "WHEN days < need THEN CONCAT('collecting: ', CAST(days AS STRING), ' of ', CAST(need AS STRING), ' days')",
                 "WHEN t_x >= 2 AND m_s > 0 AND top_s > top_o THEN 'shadow ahead: the owner may consider it'",
                 "WHEN dm <= 0 THEN 'shadow not ahead: keep the current score'"):
        assert frag in code, frag
    assert "the live system never reads BigQuery" in o.template
    fwd, old = "forward from 2026-11-19", "in-sample, not evidence"
    # the 18 Nov read of the archive (US, 1M): strong in-sample, and still not evidence
    r, t = _shadow_reading(old, 34, 30, 0.3198, 0.5383, 0.2851, 3.91, -2.12)
    assert r.startswith("in-sample") and round(t, 2) == 2.01                 # t 2 in-sample is still not evidence
    # matched cohort (Codex P2 on #1023): on a day where a third of the names have no analyst
    # target, the plain overall_score is ranked over 30 names and the shadow over 20 - a different
    # day mean and a different top third. The matched pair keeps the 20 on both sides.
    tu = [float(i) for i in range(20)] + [None] * 10
    er = [float(i % 7) for i in range(30)]
    ov = [float(30 - i) for i in range(30)]
    sv = _shadow_v2(tu, er)
    sm, om = _matched_pair(sv, ov)
    assert sum(v is not None for v in sm) == sum(v is not None for v in om) == 20
    assert sum(v is not None for v in ov) == 30 and om[:20] == ov[:20] and set(om[20:]) == {None}
    ov_gap = ov[:5] + [None] + ov[6:]                                        # a name with no overall_score
    sm2, om2 = _matched_pair(sv, ov_gap)
    assert sm2[5] is None and om2[5] is None and sum(v is not None for v in sm2) == 19
    # forward: collecting until the larger of 20 days and two horizon lengths
    assert _shadow_reading(fwd, 19, 7, 0.2, 0.2, 0.1, 1.0, 0.0)[0] == "collecting: 19 of 20 days"
    assert _shadow_reading(fwd, 27, 14, 0.2, 0.2, 0.1, 1.0, 0.0)[0] == "collecting: 27 of 28 days"
    assert _shadow_reading(fwd, 59, 30, 0.2, 0.2, 0.1, 1.0, 0.0)[0] == "collecting: 59 of 60 days"
    assert _shadow_reading(fwd, 181, 91, 0.2, 0.2, 0.1, 1.0, 0.0)[0] == "collecting: 181 of 182 days"
    # ahead needs all three: t >= 2, a positive IC of its own, and the better top third
    assert _shadow_reading(fwd, 60, 30, 0.2, 0.2, 0.1, 1.0, 0.0)[0] == "shadow ahead: the owner may consider it"
    assert _shadow_reading(fwd, 60, 30, 0.2, 0.2, 0.1, 0.0, 1.0)[0].startswith("shadow ahead but not yet clear")
    assert _shadow_reading(fwd, 60, 30, -0.05, 0.2, 0.1, 1.0, 0.0)[0].startswith("shadow ahead but not yet clear")
    assert _shadow_reading(fwd, 60, 30, 0.2, 0.1, 0.1, 1.0, 0.0)[0].startswith("shadow ahead but not yet clear")   # t 1.41
    assert _shadow_reading(fwd, 60, 30, 0.2, 0.0, 0.1, 1.0, 0.0)[0] == "shadow not ahead: keep the current score"
    assert _shadow_reading(fwd, 60, 30, 0.2, -0.2, 0.1, 1.0, 0.0)[0] == "shadow not ahead: keep the current score"
    # the overlap correction: 20 one-week days are 20 / 7 independent weeks, not 20
    assert round(_shadow_reading(fwd, 20, 7, 0.2, 0.1, 0.1, 1.0, 0.0)[1], 3) == round((20 / 7) ** 0.5, 3)


def _c2_keep(rows):
    """v_trial_c2's membership, row for row: rows are (record_id, mg, confidence, rel, market, tup)."""
    elig = {r[0]: (str(r[2] or "").upper() != "HIGH" and (r[3] or 0) < 80 and (r[4] or "") != "Tadawul"
                   and r[5] is not None) for r in rows}
    keep = {}
    for mg in {r[1] for r in rows}:
        grp = sorted([r for r in rows if r[1] == mg and elig[r[0]]], key=lambda r: (-r[5], r[0]))
        n = len(grp)
        for i, r in enumerate(grp, start=1):
            keep[r[0]] = i <= math.ceil(n / 2)
    return {r[0]: keep.get(r[0], False) for r in rows}


def _c2_reading(period, days, calls, horizon_days, dm, dsd):
    """v_trial_c2's t_adj and reading for the C2_MINUS_CHAMPION arm."""
    t = (dm / dsd) * math.sqrt(days / horizon_days) if dsd else None
    need = max(20, 2 * horizon_days)
    if period != "forward from 2026-11-20":
        return "in-sample: shows why the rule was chosen, not evidence", t
    if days < need or calls < 40:
        return "collecting: %d of %d days, %d of 40 calls" % (days, need, calls), t
    if t is not None and t >= 2 and dm > 0:
        return "C-2 ahead: the owner may consider it", t
    if dm <= 0:
        return "C-2 not ahead: keep the champion", t
    return "C-2 ahead but not yet clear (t under 2): keep collecting", t


def _sector_reading(calls, edge, ahead, t_adj=None):
    """v_outcome_sector's reading (1.35.0: 'beats' / 'loses' also need t_adj of 2 or more; no t, no reading)."""
    if calls < 100:
        return "too few calls"
    if edge >= 0.5 and ahead >= 0.55 and t_adj is not None and t_adj >= 2:
        return "beats its market"
    if edge <= -0.5 and ahead <= 0.45 and t_adj is not None and t_adj <= -2:
        return "loses to its market"
    return "no clear edge"


def _sector_t(day_means, horizon):
    """t_adj as the view computes it: the mean of the per-date means over its standard error, over SQRT(days)."""
    n = len(day_means)
    if n < 2:
        return None
    m = sum(day_means) / n
    sd = (sum((x - m) ** 2 for x in day_means) / (n - 1)) ** 0.5
    if sd == 0:
        return None
    days = {"1W": 7, "2W": 14, "1M": 30, "3M": 91, "6M": 182, "12M": 365, "1Y": 365}.get(horizon)
    if days is None:
        return None                                              # a horizon outside the list has no t
    return round(m / (sd / n ** 0.5) / days ** 0.5, 2)


def case_v35():
    objs = {o.name: o for o in bv.load_objects()}
    names = [x.name for x in bv.load_objects()]
    # v_symbol_sector: the pages first, the board second, the six provider spellings folded to GICS
    s = objs["v_symbol_sector"]
    assert s.path.name == "85_v_symbol_sector.sql" and s.kind == "VIEW"
    code = bv._code(s.template)
    assert set(bv._REF.findall(code)) - {"v_symbol_sector"} == {"v_universe", "v_top10_board"}
    from core.sectors import YAHOO_TO_GICS_SECTOR
    for yahoo, gics in YAHOO_TO_GICS_SECTOR.items():
        assert "WHEN '%s' THEN '%s'" % (yahoo, gics) in code, yahoo
    assert code.count("    WHEN '") == len(YAHOO_TO_GICS_SECTOR)                  # no other name is renamed
    assert "ORDER BY pri, last_seen DESC, sector) = 1" in code and "IF(pri = 1, 'market pages', 'top 10 board')" in code
    # v_outcome_sector: buy calls against the benchmark, by sector, with the 100-call floor
    o = objs["v_outcome_sector"]
    assert o.path.name == "86_v_outcome_sector.sql" and o.kind == "VIEW"
    code = bv._code(o.template)
    assert set(bv._REF.findall(code)) - {"v_outcome_sector"} == {"v_outcome_excess", "v_symbol_sector"}
    assert names.index("v_outcome_sector") > names.index("v_symbol_sector") > names.index("v_universe")
    for frag in ("UPPER(IFNULL(e.entry_recommendation, '')) IN ('BUY', 'STRONG BUY', 'ACCUMULATE')",
                 "LEFT JOIN `{{DS}}.v_symbol_sector` AS s ON s.symbol = UPPER(TRIM(e.symbol))",
                 "IFNULL(s.sector, '(no sector)') AS sector",
                 "WHEN a.calls < 100 THEN 'too few calls'",
                 # 1.35.0: a reading needs its test - t over recording dates, overlap-adjusted, 2 or more either way
                 "WHEN a.m_edge >= 0.5 AND a.ahead >= 0.55 AND tt.t_adj >= 2 THEN 'beats its market'",
                 "WHEN a.m_edge <= -0.5 AND a.ahead <= 0.45 AND tt.t_adj <= -2 THEN 'loses to its market'",
                 "SELECT market_group, sector, horizon, recorded_date, AVG(edge) AS m",
                 "ROUND(SAFE_DIVIDE(AVG(m), SAFE_DIVIDE(STDDEV(m), SQRT(COUNT(*))))",
                 # review of PR 1106: every horizon divided by its own days; one outside the list has no t
                 "/ SQRT(CASE horizon WHEN '1W' THEN 7 WHEN '2W' THEN 14 WHEN '1M' THEN 30 WHEN '3M' THEN 91",
                 "WHEN '6M' THEN 182 WHEN '12M' THEN 365 WHEN '1Y' THEN 365 END), 2) AS t_adj",
                 "JOIN tt USING (market_group, sector, horizon)"):
        assert frag in code, frag
    assert "THEN 'beats its market'" in code and code.count("tt.t_adj") == 3   # the column and both readings
    assert code.index("tt.t_adj,") < code.index("END AS reading")             # reading stays the last column
    # the 19 Nov read: US financials at one month, 147 calls, -1.56 points, 46.3% ahead
    assert _sector_reading(147, -1.56, 0.463, -3.0) == "no clear edge"
    assert _sector_reading(416, 0.91, 0.611, 2.0) == "beats its market"
    assert _sector_reading(99, 9.0, 0.9, 9.0) == "too few calls"
    assert _sector_reading(100, -0.5, 0.45, -2.0) == "loses to its market"
    # 1.35.0 - the read of 27 Nov 2026: 15 cells read 'beats'; 13 of them had t between 0.22 and 1.87
    assert _sector_reading(106, 8.41, 0.60, 1.87) == "no clear edge"            # the largest edge on the page
    assert _sector_reading(155, 0.86, 0.56, 0.34) == "no clear edge"
    assert _sector_reading(111, 1.77, 0.60, 2.46) == "beats its market"         # one of the two that keep it
    assert _sector_reading(416, 0.91, 0.611) == "no clear edge"                 # no t (one date, or no spread): no reading
    assert _sector_reading(416, 0.91, 0.611, -2.5) == "no clear edge"           # the sign must agree with the edge
    assert _sector_reading(100, -0.5, 0.45, 2.5) == "no clear edge"
    assert _sector_reading(100, -0.5, 0.45, -1.99) == "no clear edge"
    # t over recording dates: 40 dates of +1.0 / +3.0 alternating - mean 2, a weekly horizon divides by SQRT(7)
    days = [1.0, 3.0] * 20
    assert _sector_t(days, "1W") == round(2.0 / (1.0127393670836666 / 40 ** 0.5) / 7 ** 0.5, 2) == 4.72
    assert _sector_t(days, "1M") == 2.28 and _sector_t(days, "3M") == 1.31       # the longer the hold, the less each date adds
    assert _sector_t([2.0], "1W") is None and _sector_t([2.0, 2.0, 2.0], "1W") is None
    # review of PR 1106: a 6M or 1Y cohort divided by 91 days would read 1.31 here; by its own days 0.93 and 0.65
    assert _sector_t(days, "6M") == 0.93 and _sector_t(days, "1Y") == _sector_t(days, "12M") == 0.65
    assert _sector_t(days, "2Y") is None and _sector_reading(416, 0.91, 0.611, _sector_t(days, "2Y")) == "no clear edge"
    for sql in ("86_v_outcome_sector.sql", "138_v_forward_signal_gates.sql"):
        assert "ELSE 91" not in (ROOT / "sql" / "bigquery" / sql).read_text(encoding="utf-8"), sql
    # v_trial_c2: the fixed rule
    c = objs["v_trial_c2"]
    assert c.path.name == "87_v_trial_c2.sql" and c.kind == "VIEW"
    code = bv._code(c.template)
    assert set(bv._REF.findall(code)) - {"v_trial_c2"} == {"v_outcome_excess", "v_signal_history"}
    assert names.index("v_trial_c2") > names.index("v_outcome_excess")
    # membership over every call of the day, scored or not; only scored calls enter the statistics (Codex P2 on #1026)
    assert "WHERE recorded_at IS NOT NULL\n    AND horizon IN" in code and "WHERE in_engine_cohort" not in code
    assert code.index("AS c2_keep") < code.index("WHERE scored")
    for frag in ("IFNULL(in_engine_cohort, FALSE) AND excess_pp IS NOT NULL AS scored",
                 "  FROM k\n  WHERE scored\n  GROUP BY period, horizon, recorded_date",
                 "UPPER(IFNULL(entry_recommendation, '')) IN ('BUY', 'STRONG BUY', 'ACCUMULATE')",   # the C-1 champion
                 "s.recorded_at <= TIMESTAMP_ADD(b.recorded_at, INTERVAL 60 SECOND)",              # no look-ahead
                 "s.recorded_at >= TIMESTAMP_SUB(b.recorded_at, INTERVAL 3 DAY)",
                 "NOT (UPPER(IFNULL(b.confidence, '')) = 'HIGH' OR IFNULL(b.rel, 0) >= 80)",
                 "AND IFNULL(b.market, '') != 'Tadawul'",
                 "AND g.tup IS NOT NULL AS eligible",
                 "ROW_NUMBER() OVER (PARTITION BY recorded_date, horizon, mg, eligible ORDER BY tup DESC, record_id) AS pos",
                 "eligible AND pos <= CAST(CEIL(n_el / 2) AS INT64) AS c2_keep",
                 "IF(b.recorded_date >= DATE '2026-11-20', 'forward from 2026-11-20', 'in-sample, not evidence') AS period",
                 "SAFE_DIVIDE(dm, dsd) * SQRT(d_c2 / horizon_days)",
                 "GREATEST(20, 2 * horizon_days) AS need",
                 "WHEN t_x >= 2 AND edge_x > 0 THEN 'C-2 ahead: the owner may consider it'",
                 "WHEN edge_x <= 0 THEN 'C-2 not ahead: keep the champion'"):
        assert frag in code, frag
    for tmpl in (s.template, o.template, c.template):
        assert "the live system never reads BigQuery" in tmpl
    # membership: HIGH, reliability 80+, Tadawul and an unknown upside are out; the top half by upside stays
    rows = [(1, "US", "MEDIUM", 70, "US", 40.0), (2, "US", "MEDIUM", 70, "US", 10.0), (3, "US", "MEDIUM", 70, "US", 25.0),
            (4, "US", "HIGH", 70, "US", 90.0), (5, "US", "MEDIUM", 85, "US", 90.0), (6, "US", "MEDIUM", 70, "US", None),
            (7, "Other markets", "MEDIUM", 60, "Tadawul", 50.0), (8, "Other markets", "LOW", 60, "Hong Kong", 5.0)]
    k = _c2_keep(rows)
    assert [i for i, v in sorted(k.items()) if v] == [1, 3, 8]        # US: 3 eligible -> top 2; a lone call is kept
    # readings
    fwd, old = "forward from 2026-11-20", "in-sample, not evidence"
    r, t = _c2_reading(old, 43, 214, 30, 1.88, 9.0)
    assert r.startswith("in-sample") and round(t, 2) == 0.25
    assert _c2_reading(fwd, 19, 50, 7, 1.0, 1.0)[0] == "collecting: 19 of 20 days, 50 of 40 calls"
    assert _c2_reading(fwd, 60, 39, 30, 1.0, 1.0)[0] == "collecting: 60 of 60 days, 39 of 40 calls"
    assert _c2_reading(fwd, 60, 40, 30, 1.5, 1.0)[0] == "C-2 ahead: the owner may consider it"            # t 2.12
    assert _c2_reading(fwd, 60, 40, 30, 1.0, 1.0)[0].startswith("C-2 ahead but not yet clear")         # t 1.41
    assert _c2_reading(fwd, 60, 40, 30, -0.1, 1.0)[0] == "C-2 not ahead: keep the champion"


def _missed_cause(status, reco, block_reason):
    """Python replica of v_missed_winners.missed_cause (the CASE order matters)."""
    if status == "INVESTABLE":
        return "picked (investable)"
    if status == "BLOCKED":
        return "blocked: " + (block_reason if block_reason is not None else "no reason given")
    if reco in ("SELL", "STRONG_SELL", "REDUCE", "AVOID"):
        return "rated " + reco
    if reco in ("BUY", "STRONG_BUY", "ACCUMULATE"):
        return "rated buy, not investable"
    return "watchlist " + ("unrated" if reco == "" else reco)


def case_v36():
    objs = {o.name: o for o in bv.load_objects()}
    names = [x.name for x in bv.load_objects()]
    f, m, w = objs["v_forward_universe"], objs["v_investable_vs_market"], objs["v_missed_winners"]
    assert (f.path.name, m.path.name, w.path.name) == ("88_v_forward_universe.sql", "89_v_investable_vs_market.sql",
                                                      "90_v_missed_winners.sql")
    assert f.kind == m.kind == w.kind == "VIEW"
    fc, mc, wc = bv._code(f.template), bv._code(m.template), bv._code(w.template)
    assert set(bv._REF.findall(fc)) - {"v_forward_universe"} == {"v_universe"}
    assert set(bv._REF.findall(mc)) - {"v_investable_vs_market"} == {"v_forward_universe"}
    assert set(bv._REF.findall(wc)) - {"v_missed_winners"} == {"v_forward_universe"}
    assert names.index("v_universe") < names.index("v_forward_universe") < names.index("v_investable_vs_market") \
        < names.index("v_missed_winners")
    # the universe: liquid market-page equities, one record per symbol and day
    for frag in ("WHERE source_page IN ('Global_Markets', 'Market_Leaders')",
                 "AND UPPER(CAST(asset_class AS STRING)) = 'EQUITY'",
                 "AND avg_volume_30d * current_price >= 1000000",
                 "QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, snapshot_date ORDER BY captured_at DESC, source_page) = 1",
                 "[('1W', 7), ('2W', 14)]",
                 "AND q.snapshot_date >= DATE_ADD(p.snapshot_date, INTERVAL h.days DAY)",
                 "AND q.snapshot_date <= DATE_ADD(p.snapshot_date, INTERVAL h.days + 6 DAY)",
                 "ARRAY_AGG(q.current_price ORDER BY q.snapshot_date LIMIT 1)[OFFSET(0)] / p.current_price - 1) * 100",
                 "WHERE ret_pp IS NOT NULL AND ABS(ret_pp) < 300",
                 "ELSE IFNULL(NULLIF(CAST(sector AS STRING), ''), '(no sector)')\n    END AS sector",
                 "CASE vb.vol_t WHEN 1 THEN 'LOW' WHEN 2 THEN 'MID' WHEN 3 THEN 'HIGH' END AS vol_band",
                 "PERCENTILE_CONT(o.ret_pp, 0.5) OVER (PARTITION BY o.base_date, o.horizon) AS market_median_pp"):
        assert frag in fc, frag
    # the groups and the scoreboard columns
    for frag in ("('MARKET', 'ALL')",
                 "IF(f.status = 'INVESTABLE', ('INVESTABLE', 'ALL'), NULL)",
                 "IF(f.reco IN ('BUY', 'STRONG_BUY', 'ACCUMULATE'), ('BUY_CALL', 'ALL'), NULL)",
                 "IF(f.ret_pos <= CAST(CEIL(f.n_day / 10) AS INT64), ('BEST_DECILE', 'ALL'), NULL)",   # Codex P2 on #1029
                 "PERCENTILE_CONT(ret_pp, 0.5) OVER (PARTITION BY base_date, horizon, group_type, group_key) AS group_median_pp",
                 "ROUND(ANY_VALUE(group_median_pp), 2) AS median_pp",
                 "('SECTOR', f.sector)",
                 "IF(f.status = 'INVESTABLE' AND f.vol_band IS NOT NULL, ('INV_VOL', f.vol_band), NULL)",
                 "ROUND(ANY_VALUE(group_median_pp) - ANY_VALUE(market_median_pp), 2) AS vs_market_pp",
                 "ROUND(100 * COUNTIF(ret_pp > market_median_pp) / COUNT(*), 1) AS beat_market_pct"):
        assert frag in mc, frag
    from core.sectors import YAHOO_TO_GICS_SECTOR                                       # the same fold as v_symbol_sector
    for yahoo, gics in YAHOO_TO_GICS_SECTOR.items():
        assert "WHEN '%s' THEN '%s'" % (yahoo, gics) in fc, yahoo
    assert fc.count("      WHEN '") == len(YAHOO_TO_GICS_SECTOR)
    assert "APPROX_QUANTILES" not in mc and "ret_pct_rank >=" not in mc                # one median definition; no tie hole
    assert "ROW_NUMBER() OVER (PARTITION BY o.base_date, o.horizon ORDER BY o.ret_pp DESC, o.symbol) AS ret_pos" in fc
    assert "COUNT(*) OVER (PARTITION BY o.base_date, o.horizon) AS n_day" in fc
    # the decile cut on the reviewer's example: 20 names, the three best tied -> the best 2 by position are kept
    assert [pos for pos in range(1, 21) if pos <= math.ceil(20 / 10)] == [1, 2]
    assert "ORDER BY ret_pp DESC, symbol) AS pos" in wc and "WHERE pos <= 20" in wc
    for tmpl in (f.template, m.template, w.template):
        assert "the live system never reads BigQuery" in tmpl
    # missed_cause: the replica against the view's CASE text, then the order of the branches
    for when, out in (("WHEN status = 'INVESTABLE' THEN 'picked (investable)'", "picked (investable)"),
                      ("WHEN status = 'BLOCKED' THEN CONCAT('blocked: ', IFNULL(block_reason, 'no reason given'))", None),
                      ("WHEN reco IN ('SELL', 'STRONG_SELL', 'REDUCE', 'AVOID') THEN CONCAT('rated ', reco)", None),
                      ("WHEN reco IN ('BUY', 'STRONG_BUY', 'ACCUMULATE') THEN 'rated buy, not investable'", None),
                      ("ELSE CONCAT('watchlist ', IF(reco = '', 'unrated', reco))", None)):
        assert when in wc, when
    order = [wc.index(x) for x in ("WHEN status = 'INVESTABLE'", "WHEN status = 'BLOCKED'",
                                   "WHEN reco IN ('SELL'", "WHEN reco IN ('BUY'", "ELSE CONCAT('watchlist ")]
    assert order == sorted(order)
    assert _missed_cause("INVESTABLE", "SELL", None) == "picked (investable)"
    assert _missed_cause("BLOCKED", "BUY", "liquidity") == "blocked: liquidity"
    assert _missed_cause("BLOCKED", "BUY", None) == "blocked: no reason given"
    assert _missed_cause("WATCHLIST", "STRONG_SELL", None) == "rated STRONG_SELL"
    assert _missed_cause("WATCHLIST", "ACCUMULATE", None) == "rated buy, not investable"
    assert _missed_cause("WATCHLIST", "HOLD", None) == "watchlist HOLD"
    assert _missed_cause("WATCHLIST", "", None) == "watchlist unrated"


def _news_usable(days, closed, prev_days, prev_weeks_back):
    """Python replica of v_news_theme_weekly.usable: prev_days are the day counts of the four rows before,
    prev_weeks_back how many weeks back the oldest of them is."""
    return bool(closed and days >= 5 and len(prev_days) == 4 and all(d >= 5 for d in prev_days)
                and prev_weeks_back == 4)


def _news_tilt_group(pos, sectors, score):
    """Python replica of v_news_sector_tilt.tilt_group."""
    if pos <= 3 and score > 0:
        return "favoured"
    if pos > sectors - 3 and score < 0:
        return "avoided"
    return "neutral"


def _news_overlay(rows, tilt):
    """Python replica of v_news_overlay_list for one arm and day: rows are (symbol, status, reco, sector,
    vol_band, overall_score); tilt maps sector -> group. Returns {symbol: action}."""
    grp = lambda sec: tilt.get(sec, "neutral")
    out = [r[0] for r in rows if r[1] == "INVESTABLE" and grp(r[3]) == "avoided"]
    pool = sorted((r for r in rows if r[1] == "WATCHLIST" and r[2] == "HOLD" and grp(r[3]) == "favoured"
                   and r[4] in ("LOW", "MID") and r[5] is not None), key=lambda r: (-r[5], r[0]))
    res = {r[0]: ("out" if r[0] in out else "keep") for r in rows if r[1] == "INVESTABLE"}
    res.update({r[0]: "in" for r in pool[:len(out)]})
    return res


def _news_trial_day(base, horizon_days, last_list_day, moved_out, n_out, moved_in, n_in, n_ch, n_ov):
    """Python replica of v_trial_news_overlay's day rule (final_day, scored, complete)."""
    import datetime as _d
    if moved_out <= 0:
        return "unchanged"
    final = _d.date.fromisoformat(base) + _d.timedelta(days=horizon_days + 6) <= _d.date.fromisoformat(last_list_day)
    if not final:
        return "pending"
    return "counted" if (n_out == moved_out and n_in == moved_in and n_ch > 0 and n_ov > 0) else "incomplete"


def _news_trial_reading(line, forward, days, need, news_weeks, t_x, edge_x):
    """Python replica of v_trial_news_overlay.reading."""
    if line != "OVERLAY_MINUS_CHAMPION":
        return None
    if not forward:
        return "in-sample: shows why this is a trial, not evidence"
    if days < need or news_weeks < 8:
        return "collecting: %d of %d days, %d of 8 news weeks" % (days, need, news_weeks)
    if t_x >= 2.5 and edge_x > 0:
        return "news overlay ahead: the owner may consider it"
    if edge_x <= 0:
        return "news overlay not ahead: keep the list as it is"
    return "news overlay ahead but not yet clear (t under 2.5): keep collecting"


def case_v37():
    import re
    import archive_news_themes as nt
    objs = {o.name: o for o in bv.load_objects()}
    names = [x.name for x in bv.load_objects()]
    order = ("v_news_theme_weekly", "v_news_sector_weights", "v_news_sector_tilt", "v_news_overlay_list",
             "v_trial_news_overlay")
    files = ("91_v_news_theme_weekly.sql", "92_v_news_sector_weights.sql", "93_v_news_sector_tilt.sql",
             "94_v_news_overlay_list.sql", "95_v_trial_news_overlay.sql")
    assert tuple(objs[n].path.name for n in order) == files and all(objs[n].kind == "VIEW" for n in order)
    assert [names.index(n) for n in order] == sorted(names.index(n) for n in order)
    code = {n: bv._code(objs[n].template) for n in order}
    refs = {n: set(bv._REF.findall(code[n])) - {n} for n in order}
    assert refs == {"v_news_theme_weekly": {"news_theme_daily"},
                    "v_news_sector_weights": {"v_news_theme_weekly", "market_series_daily"},
                    "v_news_sector_tilt": {"v_news_sector_weights"},
                    "v_news_overlay_list": {"v_universe", "v_news_sector_tilt"},
                    "v_trial_news_overlay": {"v_news_overlay_list", "v_forward_universe"}}, refs
    # the source table: the loader's own, and only its declared columns are read
    assert bv.NEWS_TABLES == {nt.TABLE, nt.ENTITY_TABLE} and bv.NEWS_TABLES <= bv.SOURCE_TABLES
    assert {n for n in names if "news_theme_daily" in set(bv._REF.findall(bv._code(objs[n].template)))} == {
        "v_news_theme_weekly"}
    # 1.20.0: the places and the people live in their own table, so the loop's every-feature rule is unchanged
    assert {n for n in names if "news_entity_daily" in set(bv._REF.findall(bv._code(objs[n].template)))} == {
        "v_market_news", "v_people_news", "v_news_trial"}
    cols = {f["name"] for f in nt.SCHEMA}
    wk = code["v_news_theme_weekly"].split("WITH wk AS (", 1)[1].split("GROUP BY week_start, feature", 1)[0]
    assert set(re.findall(r"\b(date|feature|source|kind|num|den|loaded_at|run_id)\b", wk)) <= cols
    assert {"date", "feature", "kind", "source", "num", "den"} <= set(re.findall(r"\b[a-z_]+\b", wk))
    for tmpl in (objs[n].template for n in order):
        assert "the live system never reads BigQuery" in tmpl
    # 91: the week, the value by kind, the surprise against the four weeks before, the usable rule
    w = code["v_news_theme_weekly"]
    for frag in ("DATE_TRUNC(date, WEEK(MONDAY)) AS week_start",
                 "MAX(MAX(date)) OVER (PARTITION BY feature) AS last_day",
                 "IF(kind = 'share', 1000, 1) * SAFE_DIVIDE(num, den) AS value",
                 "WINDOW p4 AS (PARTITION BY feature ORDER BY week_start ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING)",
                 "COUNTIF(days >= 5 AND value IS NOT NULL) OVER p4 AS prev_ok",
                 "value - prev_avg AS surprise",
                 "days = 7 OR last_day > DATE_ADD(week_start, INTERVAL 6 DAY) AS is_closed",
                 "(days = 7 OR last_day > DATE_ADD(week_start, INTERVAL 6 DAY)) AND days >= 5 AND value IS NOT NULL\n"
                 "    AND prev_ok = 4 AND prev_first = DATE_SUB(week_start, INTERVAL 28 DAY) AS usable"):
        assert frag in w, frag
    assert _news_usable(7, True, [7, 7, 6, 5], 4)
    assert not _news_usable(4, True, [7, 7, 7, 7], 4)          # too few days
    assert not _news_usable(6, False, [7, 7, 7, 7], 4)         # the week is still open
    assert not _news_usable(7, True, [7, 4, 7, 7], 4)          # a thin week among the four before
    assert not _news_usable(7, True, [7, 7, 7, 7], 5)          # a week missing among the four before
    assert not _news_usable(7, True, [7, 7, 7], 3)             # history too short
    # 92: the target, the link, the walk-forward weight and its own record - nothing after the news week
    g = code["v_news_sector_weights"]
    sec = dict(re.findall(r"\('(XL[A-Z]+)', '([^']+)'\)", g))
    assert sec == {"XLB": "Materials", "XLC": "Communication Services", "XLE": "Energy", "XLF": "Financials",
                   "XLI": "Industrials", "XLK": "Information Technology", "XLP": "Consumer Staples",
                   "XLRE": "Real Estate", "XLU": "Utilities", "XLV": "Health Care", "XLY": "Consumer Discretionary"}
    from core.sectors import YAHOO_TO_GICS_SECTOR
    assert set(sec.values()) - set(YAHOO_TO_GICS_SECTOR.values()) == {"Communication Services", "Energy", "Industrials",
                                                                      "Real Estate", "Utilities"}
    import archive_market_series as ms
    assert set(sec) | {"SPY"} <= {r["symbol"] for r in ms.series_records()}
    for frag in ("WHERE symbol IN ('SPY', 'XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU', 'XLV', 'XLY') "
                 "AND adj_close > 0",
                 "ARRAY_AGG(adj_close ORDER BY date ASC LIMIT 1)[OFFSET(0)] AS c",        # the week's first close
                 "LEAD(c) OVER (PARTITION BY symbol ORDER BY wk) / c - 1 AS r",
                 "100 * (a.r - s.r) AS excess_pp",
                 "WHERE a.symbol != 'SPY' AND a.r IS NOT NULL AND a.gap = 7 AND s.gap = 7",
                 "LEFT JOIN ex ON ex.etf = e.etf AND ex.entry_week = DATE_ADD(t.week_start, INTERVAL 7 DAY)",
                 "WHERE t.usable",
                 "IF(n >= 52, SAFE_DIVIDE(sxy - sx * sy / n, sxx - sx * sx / n), NULL) AS beta",
                 "beta * (x - SAFE_DIVIDE(sx, n)) AS p",
                 "CORR(IF(y IS NULL, NULL, p), y) OVER past AS val",
                 "p IS NOT NULL AND nv >= 26 AND IFNULL(val, 0) > 0 AS active"):
        assert frag in g, frag
    past = "WINDOW past AS (PARTITION BY etf, feature ORDER BY week_start ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING)"
    assert g.count(past) == 2 and "1 PRECEDING" not in g and "FOLLOWING" not in g and "CURRENT ROW" not in g
    # 93: two arms, the score, the groups, the window of list dates
    t = code["v_news_sector_tilt"]
    for frag in ("CROSS JOIN UNNEST(['events', 'all']) AS news_arm",
                 "AND (news_arm = 'all' OR w.source = 'gdelt_events')",
                 "SUM(IF(w.active, w.past_corr * w.prediction_pp, 0)) AS score_x",
                 "ROW_NUMBER() OVER (PARTITION BY arm, week_start ORDER BY score_x DESC, etf) AS pos",
                 "DATE_ADD(week_start, INTERVAL 8 DAY) AS apply_from",
                 "DATE_ADD(week_start, INTERVAL 14 DAY) AS apply_to",
                 "WHEN pos <= 3 AND score_x > 0 THEN 'favoured'",
                 "WHEN pos > sectors - 3 AND score_x < 0 THEN 'avoided'",
                 "WHERE max_active > 0"):
        assert frag in t, frag
    # the loop's table holds the two v1.0.0 sources only (archive_news_themes 1.1.0 sends its new ones elsewhere)
    assert {f[1] for f in nt.FEATURES if nt.TABLE_OF[f[1]] == nt.TABLE} == {"gdelt_events", "gdelt_gkg"}
    assert {s for s in nt.SOURCES if nt.TABLE_OF[s] == nt.TABLE} == {"gdelt_events", "gdelt_gkg"}
    assert [_news_tilt_group(p, 11, sc) for p, sc in ((1, 0.2), (3, 0.01), (4, 0.3), (3, 0.0), (9, -0.1), (11, -0.5),
                                                       (8, -0.9), (9, 0.0), (10, 0.02))] == [
        "favoured", "favoured", "neutral", "neutral", "avoided", "avoided", "neutral", "neutral", "neutral"]
    # 94: the universe is v_forward_universe's, the rule moves names one for one
    o = code["v_news_overlay_list"]
    fu = bv._code(objs["v_forward_universe"].template)
    for frag in ("WHERE source_page IN ('Global_Markets', 'Market_Leaders')",
                 "AND UPPER(CAST(asset_class AS STRING)) = 'EQUITY'",
                 "AND avg_volume_30d * current_price >= 1000000",
                 "QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, snapshot_date ORDER BY captured_at DESC, source_page) = 1",
                 "ELSE IFNULL(NULLIF(CAST(sector AS STRING), ''), '(no sector)')\n    END AS sector"):
        assert frag in o and frag in fu, frag
    for yahoo, gics in YAHOO_TO_GICS_SECTOR.items():
        assert "WHEN '%s' THEN '%s'" % (yahoo, gics) in o, yahoo
    for frag in ("NTILE(3) OVER (PARTITION BY base_date ORDER BY volatility_30d_pct, symbol) AS vol_t",
                 "ON t.arm = news_arm AND t.sector = u.sector AND u.base_date BETWEEN t.apply_from AND t.apply_to",
                 "status = 'INVESTABLE' AND tilt_group = 'avoided' AS moved_out",
                 "status = 'WATCHLIST' AND reco = 'HOLD' AND tilt_group = 'favoured'\n"
                 "      AND IFNULL(vol_band, '') IN ('LOW', 'MID') AND overall_score IS NOT NULL AS in_pool",
                 "COUNTIF(moved_out) OVER (PARTITION BY arm, base_date) AS n_out",
                 "ROW_NUMBER() OVER (PARTITION BY arm, base_date, in_pool ORDER BY overall_score DESC, symbol)",
                 "(in_champion AND NOT moved_out) OR (in_pool AND pool_pos <= n_out) AS in_overlay",
                 "CASE WHEN moved_out THEN 'out' WHEN in_champion THEN 'keep' ELSE 'in' END AS news_action",
                 "WHERE in_champion OR (in_pool AND pool_pos <= n_out)"):
        assert frag in o, frag
    day = [("A", "INVESTABLE", "BUY", "Financials", "LOW", 70.0), ("B", "INVESTABLE", "BUY", "Energy", "LOW", 72.0),
           ("C", "INVESTABLE", "ACCUMULATE", "Financials", "HIGH", 71.0),
           ("D", "WATCHLIST", "HOLD", "Utilities", "LOW", 60.0), ("E", "WATCHLIST", "HOLD", "Utilities", "MID", 66.0),
           ("F", "WATCHLIST", "HOLD", "Utilities", "HIGH", 99.0), ("G", "WATCHLIST", "SELL", "Utilities", "LOW", 98.0),
           ("H", "BLOCKED", "HOLD", "Utilities", "LOW", 97.0), ("I", "WATCHLIST", "HOLD", "Materials", "LOW", 96.0),
           ("J", "WATCHLIST", "HOLD", "Utilities", "LOW", None), ("K", "WATCHLIST", "HOLD", "Utilities", "LOW", 66.0)]
    tilt = {"Financials": "avoided", "Utilities": "favoured"}
    assert _news_overlay(day, tilt) == {"A": "out", "C": "out", "B": "keep", "E": "in", "K": "in"}
    assert _news_overlay(day, {}) == {"A": "keep", "B": "keep", "C": "keep"}                 # no tilt, no change
    assert _news_overlay(day, {"Utilities": "favoured"}) == {"A": "keep", "B": "keep", "C": "keep"}   # nothing left
    # 95: the edge, forward evidence only, the changed days, the fixed promotion rule
    r = code["v_trial_news_overlay"]
    for frag in ("f.ret_pp - f.market_median_pp AS edge",
                 "CROSS JOIN hz AS h\n  LEFT JOIN `{{DS}}.v_forward_universe` AS f\n"
                 "    ON f.symbol = o.symbol AND f.base_date = o.base_date AND f.horizon = h.horizon",
                 "[('1W', 7), ('2W', 14)]",
                 "MAX(o.base_date) OVER () AS last_list_day",
                 "DATE_ADD(base_date, INTERVAL MIN(horizon_days) + 6 DAY) <= MAX(last_list_day) AS final_day",
                 "COUNTIF(in_champion AND edge IS NOT NULL) AS n_ch, SUM(IF(in_champion, edge, NULL)) AS s_ch",
                 "COUNTIF(news_action = 'out') AS moved_out",
                 "COUNTIF(news_action = 'in') AS moved_in",
                 "n_out = moved_out AND n_in = moved_in AND n_ch > 0 AND n_ov > 0 AS complete",
                 "WHERE moved_out > 0 AND final_day",
                 "COUNTIF(complete) AS days",
                 "COUNTIF(NOT complete) AS incomplete_days",
                 "COUNT(DISTINCT IF(complete, news_week, NULL)) AS news_weeks",
                 "IF(base_date >= DATE '2026-11-20', 'forward from 2026-11-20', 'in-sample, not evidence') AS period",
                 "AVG(IF(complete, SAFE_DIVIDE(s_ov, n_ov) - SAFE_DIVIDE(s_ch, n_ch), NULL)) AS dm",
                 "STDDEV_SAMP(IF(complete, SAFE_DIVIDE(s_ov, n_ov) - SAFE_DIVIDE(s_ch, n_ch), NULL)) AS dsd",
                 "GREATEST(20, 2 * horizon_days) AS need",
                 "IF(line = 'OVERLAY_MINUS_CHAMPION', SAFE_DIVIDE(dm, dsd) * SQRT(days / horizon_days), NULL) AS t_x",
                 "UNNEST(['CHAMPION', 'NEWS_OVERLAY', 'MOVED_OUT', 'MOVED_IN', 'OVERLAY_MINUS_CHAMPION']) AS line",
                 "WHEN days < need OR news_weeks < 8 THEN CONCAT('collecting: '",
                 "WHEN t_x >= 2.5 AND edge_x > 0 THEN 'news overlay ahead: the owner may consider it'",
                 "WHEN edge_x <= 0 THEN 'news overlay not ahead: keep the list as it is'"):
        assert frag in r, frag
    assert "\n  JOIN `{{DS}}.v_forward_universe`" not in r                 # never an inner join: a missing outcome is seen
    # a day: (base, horizon days, newest list day, moved out, out scored, moved in, in scored, champion scored, overlay scored)
    cases = [("2026-11-20", 7, "2026-12-03", 3, 3, 3, 3, 40, 40, "counted"),
             ("2026-11-20", 7, "2026-12-02", 3, 3, 3, 3, 40, 40, "pending"),        # one day short of final
             ("2026-11-20", 14, "2026-12-03", 3, 3, 3, 3, 40, 40, "pending"),
             ("2026-11-20", 7, "2026-12-03", 3, 2, 3, 3, 40, 39, "incomplete"),     # a name moved out has no outcome
             ("2026-11-20", 7, "2026-12-03", 3, 3, 3, 2, 39, 40, "incomplete"),     # a name brought in has none
             ("2026-11-20", 7, "2026-12-03", 3, 3, 2, 2, 40, 39, "counted"),        # a short pool is still the day's list
             ("2026-11-20", 7, "2026-12-03", 0, 0, 0, 0, 40, 40, "unchanged")]
    for base, hd, last, mo, no, mi, ni, nch, nov, want in cases:
        assert _news_trial_day(base, hd, last, mo, no, mi, ni, nch, nov) == want, (base, hd, last, mo, no, mi, ni)
    # the 19 Nov in-sample read: -0.03 a day on 6 final days, t -0.17 -> in-sample whatever the figures
    assert _news_trial_reading("OVERLAY_MINUS_CHAMPION", False, 6, 20, 2, -0.17, -0.03).startswith("in-sample")
    assert _news_trial_reading("CHAMPION", True, 40, 20, 9, 3.0, 1.0) is None
    assert _news_trial_reading("OVERLAY_MINUS_CHAMPION", True, 19, 20, 9, 3.0, 1.0) == "collecting: 19 of 20 days, 9 of 8 news weeks"
    assert _news_trial_reading("OVERLAY_MINUS_CHAMPION", True, 30, 28, 7, 3.0, 1.0).startswith("collecting")
    assert _news_trial_reading("OVERLAY_MINUS_CHAMPION", True, 30, 28, 8, 2.5, 0.3) == "news overlay ahead: the owner may consider it"
    assert _news_trial_reading("OVERLAY_MINUS_CHAMPION", True, 30, 28, 8, 2.49, 0.3).startswith("news overlay ahead but not yet clear")
    assert _news_trial_reading("OVERLAY_MINUS_CHAMPION", True, 30, 28, 8, 3.0, 0.0) == "news overlay not ahead: keep the list as it is"
    assert round(-0.03 / 0.163 * math.sqrt(6 / 7), 2) == -0.17 and max(20, 2 * 14) == 28        # t_adj (the 19 Nov read) and need


def _c2_why_out(confidence, rel, market, tup):
    """Python replica of v_c2_calls.why_out (None for an eligible call)."""
    out = [w for w, c in (("HIGH confidence", str(confidence or "").upper() == "HIGH"), ("reliability 80+", (rel or 0) >= 80),
                          ("Tadawul", (market or "") == "Tadawul"), ("no analyst target", tup is None)) if c]
    return ", ".join(out) or None


def case_v38():
    """v_c2_calls (1.15.0): C-2 per call, on v_trial_c2's own WITH block."""
    objs = {o.name: o for o in bv.load_objects()}
    names = [x.name for x in bv.load_objects()]
    v, t = objs["v_c2_calls"], objs["v_trial_c2"]
    assert v.path.name == "96_v_c2_calls.sql" and v.kind == "VIEW"
    code, trial = bv._code(v.template), bv._code(t.template)
    assert set(bv._REF.findall(code)) - {"v_c2_calls"} == {"v_outcome_excess", "v_signal_history"}
    assert names.index("v_c2_calls") > names.index("v_trial_c2")
    # the same membership, character for character: base, sig, f, r and k of the trial
    start, end = "WITH base AS (", "\n),\ndaily AS ("
    block = trial[trial.index(start):trial.index(end)] + "\n)"
    assert code.index(start) == code.index("AS\nWITH base AS (") + 3
    assert code[code.index(start):].startswith(block + "\nSELECT\n"), "v_c2_calls must carry v_trial_c2's WITH block unchanged"
    assert "WHERE scored" not in code and "FROM k;" in code                     # every call of the day, scored or not
    for frag in ("mg AS market_group", "IF(eligible, pos, NULL) AS upside_rank", "CAST(CEIL(n_el / 2) AS INT64) AS keeps",
                 "n_el AS eligible_calls", "ROUND(tup, 2) AS target_upside_pp", "  c2_keep,\n", "  scored\nFROM k;",
                 "IF(UPPER(IFNULL(confidence, '')) = 'HIGH', 'HIGH confidence', NULL)",
                 "IF(IFNULL(rel, 0) >= 80, 'reliability 80+', NULL)",
                 "IF(IFNULL(market, '') = 'Tadawul', 'Tadawul', NULL)",
                 "IF(tup IS NULL, 'no analyst target', NULL)], ', ')) AS why_out"):
        assert frag in code, frag
    assert "the live system never reads BigQuery" in v.template and "observe-only" in v.template
    # why_out names exactly the conditions of eligibility that fail (the trial's own (a), (b) and the known upside)
    for conf, rel, mkt, tup in (("HIGH", 70, "US", 10.0), ("MEDIUM", 85, "US", 10.0), ("MEDIUM", 70, "Tadawul", 10.0),
                                ("MEDIUM", 70, "US", None), ("LOW", None, "US", 5.0), ("high", 90, "Tadawul", None)):
        elig = _c2_keep([(1, "US", conf, rel, mkt, tup)])[1]
        assert (_c2_why_out(conf, rel, mkt, tup) is None) == elig, (conf, rel, mkt, tup)
    assert _c2_why_out("high", 90, "Tadawul", None) == "HIGH confidence, reliability 80+, Tadawul, no analyst target"
    # the 19 Nov read: US 5 eligible -> 3 kept (EFC 33.47, NMM 32.61, MBIN 30.22); SB 28.85 and BNY 10.76 out
    us = [(1, "US", "MEDIUM", 60, "US", 33.47), (2, "US", "MEDIUM", 60, "US", 32.61), (3, "US", "MEDIUM", 60, "US", 30.22),
          (4, "US", "MEDIUM", 60, "US", 28.85), (5, "US", "MEDIUM", 60, "US", 10.76)]
    assert [i for i, k in sorted(_c2_keep(us).items()) if k] == [1, 2, 3] and math.ceil(5 / 2) == 3


# ----------------------------------------------------------------------------- v1.16.0: the company news re-read
def _news_cfg():
    """The literals of v_symbol_news_class, read from the file itself, so the Python reading below cannot drift from
    the SQL. On 20 Nov 2026 this reading and the view gave the same relevance, event and direction on all 1,306
    archived headlines."""
    import re
    code = bv._code({o.name: o for o in bv.load_objects()}["v_symbol_news_class"].template)
    lst = lambda name: re.findall(r"'([^']*)'", re.search(r"\[([^\]]*)\] AS %s" % name, code).group(1))
    aliases = re.search(r"\[(STRUCT\('[^\]]*)\] AS aliases", code).group(1)
    rules_src = code[code.index("[STRUCT(5 AS priority"):code.index("] AS rules")]
    rules = [(int(p), e, int(d), float(w), pat) for p, e, d, w, pat in re.findall(
        r"(\d+)(?: AS priority)?, '([A-Z_]+)'(?: AS event_type)?, (-?\d)(?: AS direction)?, ([0-9.]+)(?: AS weight)?, "
        r"r'([^']*)'", rules_src)]
    return {"suffix": set(lst("suffix")), "generic": set(lst("generic")), "acronym": set(lst("acronym")),
            "alias": re.findall(r"\('([^']+)'(?: AS symbol)?, '([^']+)'(?: AS alias)?\)", aliases),
            "rules": sorted(rules),
            "exch": dict(re.findall(r"WHEN '([A-Z]+)' THEN '([^']+)'", code)),
            "noise": re.search(r"REGEXP_CONTAINS\(h\.t, r'([^']+)'\) THEN 'noise'", code).group(1),
            "roundup": re.search(r"REGEXP_CONTAINS\(h\.t, r'([^']+)'\) THEN 'roundup'", code).group(1),
            "actor": re.findall(r"CONCAT\('\(\^\|\[\^a-z0-9\]\)', k, r'([^']+)'\)",
                                code[code.index("AS name_hit,"):code.index("AS actor_hit,")]),
            "clause": re.search(r"REGEXP_REPLACE\(y\.t, r'([^']+)', ''\) AS main", code).group(1), "code": code}


def _news_read(cfg, symbol, name, title):
    """The view's reading of one headline, in Python: (relevance, event_type, direction, score)."""
    import re
    import unicodedata

    def norm(x):
        x = unicodedata.normalize("NFD", (x or "").replace("&amp;", "&"))
        x = "".join(ch for ch in x if unicodedata.category(ch) != "Mn").lower()
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9&]+", " ", x)).strip()
    t, n = norm(title), norm(name)
    x = unicodedata.normalize("NFD", (title or "").replace("&amp;", "&"))
    x = "".join(ch for ch in x if unicodedata.category(ch) != "Mn").lower()
    ta = re.sub(r"\s+", " ", re.sub(r"[^a-z0-9&:]+", " ", x)).strip()       # the colon kept: "...: BNP Paribas"
    tick, mk = (symbol.rsplit(".", 1) if "." in symbol else (symbol, "US"))
    tick = tick.replace("-UN", ".UN")
    words = [w for w in n.split(" ") if w and w not in cfg["suffix"]]
    keys = [a for s_, a in cfg["alias"] if s_ == symbol]
    if words:
        keys.append(" ".join(words))
    if len(words) >= 2:
        keys.append(words[0] + " " + words[1])
    if words and len(words[0]) >= 4 and words[0] not in cfg["generic"]:
        keys.append(words[0])
    keys = {k for k in keys if len(k) >= 3}
    tre = re.sub(r"([.\-])", r"\\\1", tick)
    exch = cfg["exch"].get(mk, "NONE")
    ticker = not re.fullmatch(r"[0-9]+", tick) and bool(
        re.search("(" + exch + "):" + tre + r"\)", title)
        or (mk == "US" and re.search(r"\(" + tre + r"\)", title))
        or (mk == "US" and len(tick) >= 3 and tick not in cfg["acronym"]
            and re.search("(^|[^A-Za-z0-9:])" + tre + "($|[^A-Za-z0-9])", title)))
    named = any(re.search("(^|[^a-z0-9])" + k + "($|[^a-z0-9])", t) for k in keys)
    rating, move, view, own = ["(^|[^a-z0-9])" + "%s" + a for a in cfg["actor"]]
    actor = any(re.search(rating % k, t) or re.search(move % k, t)
                or (re.search(view % k, t) and not re.search(own % k, t)) or ta.endswith(": " + k) for k in keys)
    if re.search(cfg["noise"], t):
        rel = "noise"
    elif not (ticker or named):
        rel = "other"
    elif re.search(cfg["roundup"], t):
        rel = "roundup"
    elif actor:
        rel = "actor"
    else:
        rel = "company"
    main = re.sub(cfg["clause"], "", t)
    ev = next(((e, d, w) for _, e, d, w, pat in cfg["rules"] if re.search(pat, main)), ("OTHER", 0, 0.0))
    if rel == "noise":
        ev = ("OTHER", 0, 0.0)
    return rel, ev[0], ev[1], (ev[1] * ev[2] if rel == "company" else 0.0)


def case_v39():
    """v_symbol_news_class (1.16.0): relevance first, then the event of the main clause, then the score."""
    objs = {o.name: o for o in bv.load_objects()}
    names = [o.name for o in bv.load_objects()]
    v = objs["v_symbol_news_class"]
    assert v.path.name == "97_v_symbol_news_class.sql" and v.kind == "VIEW"
    cfg = _news_cfg()
    code = cfg["code"]
    assert set(bv._REF.findall(code)) - {"v_symbol_news_class"} == {"symbol_news", "v_universe_latest"}
    assert names.index("v_symbol_news_class") > names.index("v_universe_latest")
    assert "the live system never reads BigQuery" in v.template and "Research view" in v.template
    # the literals were read: 28 rules in strict priority order, every weight in {0, 0.5, 1, 2}
    pri = [r[0] for r in cfg["rules"]]
    assert len(cfg["rules"]) == 28 and pri == sorted(set(pri)), pri
    assert {r[3] for r in cfg["rules"]} == {0.0, 0.5, 1.0, 2.0}
    assert {r[1] for r in cfg["rules"] if r[3] == 0.0} == {"PREVIEW", "EARNINGS_INLINE", "PRICE_DOWN", "PRICE_UP", "RESULTS",
                                                         "DIVIDEND_REGULAR", "ACQUIRER"}     # a price move scores 0
    assert all(r[2] == 0 for r in cfg["rules"] if r[1] in ("PREVIEW", "EARNINGS_INLINE", "RESULTS", "DIVIDEND_REGULAR",
                                                           "ACQUIRER"))
    assert cfg["rules"][0][1] == "PREVIEW"                       # a forecast of results is never read as the results
    assert "cal{2}" in code and "'company', COALESCE(c.ev.direction, 0) * COALESCE(c.ev.weight, 0.0), 0.0) AS score" in code
    assert "IF(c.relevance = 'noise', 'OTHER'" in code and "c.polarity AS provider_polarity" in code
    assert "CONCAT('&amp', CHR(59))" in code and code.count(";") == 1
    # the steps the Python reading below takes in code are the view's own steps, in the view's order
    for frag in ("CONCAT('(^|[^A-Za-z0-9:])', y.tick_re, '($|[^A-Za-z0-9])')",    # never right after a colon
                 "CONCAT('(', y.exch, '):', y.tick_re, r'\\)')", "CONCAT(r'\\(', y.tick_re, r'\\)')",
                 "LENGTH(y.tick) >= 3 AND y.tick NOT IN UNNEST(y.acronym)", "NOT REGEXP_CONTAINS(y.tick, r'^[0-9]+$')",
                 "WHERE REGEXP_CONTAINS(h.main, r.pattern) ORDER BY r.priority LIMIT 1) AS ev",
                 "WHERE LENGTH(x) >= 3) AS name_keys", "LENGTH(k.words[OFFSET(0)]) >= 4"):
        assert frag in code, frag
    assert len(cfg["actor"]) == 4 and "raises" not in cfg["actor"][0] and "says" not in cfg["actor"][0]
    assert "REGEXP_CONTAINS(y.ta, CONCAT(': ', k, '$'))) AS actor_hit" in code and "ENDS_WITH(y.t" not in code
    assert "r'[^a-z0-9&:]+', ' '), r'\\s+', ' ')) AS ta" in code                      # the colon kept in ta
    assert "AND NOT REGEXP_CONTAINS(y.t, CONCAT('(^|[^a-z0-9])', k, r' (says|said|sees|expects|predicts) [a-z0-9 ]*" in code
    steps = ["THEN 'noise'", "WHEN NOT (h.ticker_hit OR h.name_hit) THEN 'other'", "THEN 'roundup'",
             "WHEN h.actor_hit THEN 'actor'", "ELSE 'company'"]
    assert [code.index(x) for x in steps] == sorted(code.index(x) for x in steps)
    cases = [
        ("AX.US", "Axos Financial, Inc.", "Axos Financial (AX) Q1 Earnings and Revenues Top Estimates",
         ("company", "EARNINGS_BEAT", 1, 2.0)),
        ("SBLK.US", "Star Bulk Carriers Corp.", "Star Bulk Carriers (SBLK) Lags Q3 Earnings and Revenue Estimates",
         ("company", "EARNINGS_MISS", -1, -2.0)),
        ("HBAN.US", "Huntington Bancshares Incorporated", "Huntington Q3 Earnings Match Estimates as NII &amp; Fee Income Rise Y/Y",
         ("company", "EARNINGS_INLINE", 0, 0.0)),
        ("ZTO.US", "ZTO Express (Cayman) Inc.", "ZTO Express beats revenue estimates despite earnings miss",
         ("company", "EARNINGS_BEAT", 1, 2.0)),                                           # the clause before "despite"
        ("PINE.US", "Alpine Income Property Trust, Inc.",
         "Alpine Income Property Trust (PINE) Stock Price Sinks Despite Raised Q3 Earnings Outlook",
         ("company", "PRICE_DOWN", -1, 0.0)),                                             # a move: context, no score
        ("SPNT.US", "SiriusPoint Ltd.", "SiriusPoint (SPNT) Expected to Beat Earnings Estimates: Can the Stock Move Higher?",
         ("company", "PREVIEW", 0, 0.0)),
        ("ASB.US", "Associated Banc-Corp", "Associated Banc-Corp raises quarterly dividend by 4.2% to $0.25/share",
         ("company", "DIVIDEND_UP", 1, 2.0)),
        ("BARC.L", "Barclays PLC", "Barclays (LSE:BARC) Faces Class Action Probe Over Alleged Misleading Disclosures",
         ("company", "LEGAL", -1, -2.0)),
        ("UMBF.US", "UMB Financial Corporation", "UMB Financial (NASDAQ:UMBF) surprises with Q3 2026 sales",
         ("company", "EARNINGS_BEAT", 1, 2.0)),
        ("ACA.PA", "Crédit Agricole S.A.", "Credit Agricole Q3 profit rises on investment banking strength",
         ("company", "EARNINGS_BEAT", 1, 2.0)),                                           # an alias, accents removed
        ("AX.US", "Axos Financial, Inc.", "Axos Financial (AX) Stock May Be Undervalued On Earnings Power Today",
         ("company", "OPINION_POS", 1, 0.5)),
        ("DDI.US", "DoubleDown Interactive Co., Ltd.", "DoubleDown Interactive Reports Third Quarter 2026 Revenue of $94.3 "
         "Million and Earnings per Fully Diluted Common Share of $7.41", ("company", "RESULTS", 0, 0.0)),
        ("BMRN.US", "BioMarin Pharmaceutical Inc.", "BioMarin Enters Into Exclusive Worldwide License Agreement for "
         "Investigational Medicines", ("company", "OTHER", 0, 0.0)),                     # not a legal matter
        ("C.US", "Citigroup Inc.", "Citi downgrades BJ's Wholesale, says optimism already priced in",
         ("actor", "OTHER", 0, 0.0)),                                                     # Citi is the one speaking
        ("BNP.PA", "BNP Paribas SA", "GE Vernova meets high bar, but slot conversions now in focus: BNP Paribas",
         ("actor", "OTHER", 0, 0.0)),                                                     # the source after a colon
        ("C.US", "Citigroup Inc.", "AI fatigue: Citi cuts U.S. and tech to neutral, turns bullish on Europe",
         ("actor", "OPINION_POS", 1, 0.0)),                                               # a rating move on others
        ("C.US", "Citigroup Inc.", "Citi raises BJ's Wholesale price target to $120", ("actor", "OTHER", 0, 0.0)),
        ("C.US", "Citigroup Inc.", "Emerging market central banks turn hawkish as inflation pressures mount, Citi says",
         ("actor", "OTHER", 0, 0.0)),
        ("KB.US", "KB Financial Group Inc.", "KB Financial Says Korea Stock Selloff Favors Buyers",
         ("actor", "PRICE_DOWN", -1, 0.0)),
        # review P2 (PR 1038): the company's own dividend, guidance or results are never "the company speaking"
        ("C.US", "Citigroup Inc.", "Citi raises quarterly dividend", ("company", "DIVIDEND_UP", 1, 2.0)),
        ("AX.US", "Axos Financial, Inc.", "Axos Financial cuts full-year guidance", ("company", "GUIDANCE_DOWN", -1, -2.0)),
        ("GSL.US", "Global Ship Lease, Inc.", "Global Ship Lease raises dividend by 10% to $0.6875",
         ("company", "DIVIDEND_UP", 1, 2.0)),
        ("SBLK.US", "Star Bulk Carriers Corp.", "Star Bulk Carriers raises dividend by 11.1% to $1.00",
         ("company", "DIVIDEND_UP", 1, 2.0)),
        ("C.US", "Citigroup Inc.", "Citi says fourth-quarter revenue will top estimates", ("company", "EARNINGS_BEAT", 1, 2.0)),
        ("ICG.L", "ICG plc", "Workhuman Legal Settlement Change The Bull Case For ICG (LSE:ICG)?",
         ("company", "OPINION_POS", 1, 0.5)),                                             # its own ticker ends it
        ("PNFP.US", "Pinnacle Financial Partners, Inc.",
         "Higher Rates Still Matter for U.S. Bank Stocks Like Pinnacle Financial Partners", ("company", "OTHER", 0, 0.0)),
        ("CTO.US", "CTO Realty Growth, Inc.", "Memory supply constraints will be even 'tighter' in 2027: Micron CTO",
         ("other", "OTHER", 0, 0.0)),                                                     # CTO here is a job title
        ("CBK.US", "Commercial Bancgroup, Inc.", "Commerzbank (XTRA:CBK) Faces A Valuation Test, Is It Still A Bargain?",
         ("other", "OPINION_POS", 1, 0.0)),                                               # Frankfurt's CBK, not CBK.US
        ("TEN.US", "Tsakos Energy Navigation Limited", "Tenaris (BIT:TEN), What Is Behind The Fresh Attention?",
         ("other", "OTHER", 0, 0.0)),
        ("BNP.PA", "BNP Paribas SA", "Colgate Palmolive (CL) Stock Fair Value Edges Higher After Q3 Earnings Beat",
         ("other", "TARGET_UP", 1, 0.0)),
        ("CRC.US", "California Resources Corporation", "Bloom Energy Stock In Focus After California Power Deal",
         ("other", "OTHER", 0, 0.0)),                                                     # a state is not the company
        ("INVP.L", "Investec Group", "Form 8.5 (EPT/RI)-Gamma Communications Plc", ("noise", "OTHER", 0, 0.0)),
        ("MBIN.US", "Merchants Bancorp", "1 Bank Stock on Our Buy List and 2 We Find Risky (MBIN)",
         ("roundup", "OPINION_NEG", -1, 0.0)),
        ("ICG.L", "ICG plc", "ICG (LSE:ICG) Closes Europe IX Fund At A Record €12 Billion", ("company", "OTHER", 0, 0.0)),
        ("GRT-UN.TO", "Granite Real Estate Investment Trust", "Granite REIT (TSX:GRT.UN) Announces Three-for-One Unit Split",
         ("company", "OTHER", 0, 0.0)),
    ]
    for sym, name, title, want in cases:
        assert _news_read(cfg, sym, name, title) == want, (sym, title, _news_read(cfg, sym, name, title), want)


def case_v40():
    """v_company_news_eval (1.16.0): no look-ahead, against the S&P 500, the two signals scored alike (the reading
    moved to the same-day peers in 1.17.0: case_v41)."""
    import re
    objs = {o.name: o for o in bv.load_objects()}
    names = [o.name for o in bv.load_objects()]
    v = objs["v_company_news_eval"]
    assert v.path.name == "98_v_company_news_eval.sql" and v.kind == "VIEW"
    code = bv._code(v.template)
    assert set(bv._REF.findall(code)) - {"v_company_news_eval"} == {"v_symbol_news_class", "symbol_bars_daily",
                                                                    "market_series_daily"}
    assert names.index("v_company_news_eval") > names.index("v_symbol_news_class")
    for frag in ("b.date > y.d AND b.date <= DATE_ADD(y.d, INTERVAL 6 DAY)",          # entry strictly after the news day
                 "ORDER BY b.date ASC LIMIT 1)[OFFSET(0)] AS e",
                 "WHERE symbol = 'SPY'", "JOIN spy AS s ON s.date = n.e.date",
                 "n.e.r5 - (s.c5 / s.close - 1) * 100 AS ex5", "n.e.r1 - (s.c1 / s.close - 1) * 100 AS ex1",
                 "WHERE relevance = 'company' AND ENDS_WITH(symbol, '.US')",
                 "WHERE provider_polarity IS NOT NULL AND ENDS_WITH(symbol, '.US')",
                 "SELECT 'company_news' AS signal, symbol, d, x FROM nd",
                 "SELECT 'provider_sentiment', symbol, d, x FROM od",
                 "WHEN top_share > 0.9 THEN 'BIASED (class > 90 %)'", "WHEN min_matured < 60 THEN 'THIN'",
                 "ELSE 'CANDIDATE (forward test)'"):
        assert frag in code, frag
    assert "b.date >= y.d" not in code and "CURRENT_DATE" not in code
    assert re.search(r"no look-ahead", v.template) and "the live system never reads BigQuery" in v.template
    # the scoreboard of 16 Nov stays as it was read
    old = bv._code(objs["v_news_signal_eval"].template)
    assert "v_symbol_news_class" not in old and "v_company_news_eval" not in old


def case_v41():
    """v_company_news_eval (1.17.0): each name-day also against the same-day average of the US names, over the same
    sessions from the same close, and the reading taken from it; the SPY columns kept, first and in their order."""
    objs = {o.name: o for o in bv.load_objects()}
    v = objs["v_company_news_eval"]
    code = bv._code(v.template)
    # the peers: every US name's own next 1 and 5 sessions from each close, averaged per day
    peer = code.split("peer AS (", 1)[1].split("),", 1)[0]
    for frag in ("AVG((c1 / close - 1) * 100) AS p1", "AVG((c5 / close - 1) * 100) AS p5", "COUNT(*) AS n_peers",
                 "FROM bars", "WHERE ENDS_WITH(symbol, '.US') AND close > 0", "GROUP BY date"):
        assert frag in peer, frag
    # set against the name on its entry close - never the news day's own close (no look-ahead)
    for frag in ("JOIN peer AS p ON p.date = n.e.date", "n.e.r1 - p.p1 AS px1", "n.e.r5 - p.p5 AS px5",
                 "LEFT JOIN spy AS s ON s.date = n.e.date",              # a day with no SPY bar keeps its peers
                 "COUNTIF(o.px5 IS NOT NULL) AS matured_5d_peer",
                 "MIN(r.matured_5d_peer) OVER (PARTITION BY r.signal) AS min_matured",
                 "CORR(x, ex5) AS c, CORR(x, px5) AS cp",
                 "ROUND(AVG(o.px1), 2) AS mean_1d_peer_pp, ROUND(AVG(o.px5), 2) AS mean_5d_peer_pp",
                 "COUNTIF(o.px5 > 0), COUNTIF(o.px5 IS NOT NULL)) * 100, 1) AS beat_5d_peer_pct",
                 "ROUND(c.cp, 3) AS corr_5d_peer",
                 "MAX(IF(r.class = 'positive', r.mean_5d_peer_pp, NULL)) OVER (PARTITION BY r.signal) AS hi_mean",
                 "MAX(IF(r.class = 'negative', r.mean_5d_peer_pp, NULL)) OVER (PARTITION BY r.signal) AS lo_mean",
                 "WHEN corr_5d_peer IS NULL OR corr_5d_peer <= 0.05 THEN 'NO EDGE'"):
        assert frag in code, frag
    assert "p.date = n.d" not in code and "p.date = y.d" not in code and "WHEN corr_5d IS NULL" not in code
    # the old columns first and in their order, the new ones after them
    final = code.rsplit("\nSELECT ", 1)[1]
    assert final.startswith("signal, class, days, matured_1d, matured_5d, mean_1d_excess_pp, mean_5d_excess_pp, "
                            "beat_5d_pct, corr_5d,\n       share_pct, first_day, last_day,"), final[:200]
    assert final.rstrip().rstrip(";").endswith("END AS reading,\n       mean_1d_peer_pp, mean_5d_peer_pp, "
                                               "matured_5d_peer, beat_5d_peer_pct, corr_5d_peer, peers\nFROM shaped"), \
        final[-200:]
    assert "the reading comes from the peer columns" in v.template and "WHY THE PEERS" in v.template
    assert "same-day average of the US names (the reading)" in v.template      # the description says it too


def _in_list(code, cte):
    """The quoted symbols of the `s.symbol IN (...)` filter inside the CTE `cte`."""
    import re
    part = code.split(cte + " AS (", 1)[1].split("\n),", 1)[0]
    return re.findall(r"'([^']+)'", part.split("s.symbol IN (", 1)[1].split(")", 1)[0])


def _mk_funds(code):
    """The fund of each market of v_market_condition's list, in its order ('' = none: a reading only)."""
    import re
    block = code.split("FROM UNNEST([", 1)[1].split("])", 1)[0]
    out = []
    for chunk in block.split("STRUCT(")[1:]:
        tok = re.findall(r"'[^']*'|CAST\(NULL AS STRING\)|\bTRUE\b|\bFALSE\b|\b\d+\b", chunk)
        assert len(tok) == 7, tok                       # order, id, market, reading, reading name, fund, buyable
        out.append("" if tok[5].startswith("CAST") else tok[5].strip("'"))
    return out


def case_v42():
    """v_market_condition (1.18.0): the five labels of v_market_regime for eleven markets; the US read exactly as the
    view (and the live rule) reads it, every other market's limits scaled by k = its normal volatility / 16."""
    import re
    import market_regime as mr
    objs = {o.name: o for o in bv.load_objects()}
    v = objs["v_market_condition"]
    assert v.path.name == "99_v_market_condition.sql" and v.kind == "VIEW"
    code = bv._code(v.template)
    assert set(bv._REF.findall(code)) - {"v_market_condition"} == {"market_series_daily", "symbol_bars_daily"}
    # the markets, the readings and the funds a buy would use
    rows = re.findall(r"STRUCT\((\d+)(?: AS market_order)?, '([a-z_]+)'(?: AS market_id)?, '([^']+)'(?: AS market)?, "
                      r"'([^']+)'", code)
    assert [(int(n), m, r) for n, m, _, r in rows] == [
        (1, "us", "SPY"), (2, "sa", "^TASI.SR"), (3, "eu", "^STOXX"), (4, "jp", "^N225"), (5, "cn", "^HSI"),
        (6, "gold", "GC=F"), (7, "oil", "BZ=F"), (8, "bonds", "IEF"), (9, "sukuk", "SPSK"), (10, "reits_us", "VNQ"),
        (11, "reits_sa", "SA_REITS")]
    assert _mk_funds(code) == ["SPY", "KSA", "VGK", "EWJ", "MCHI", "GLD", "", "IEF", "SPSK", "VNQ", "SA_REITS"]
    assert "CAST(NULL AS STRING), FALSE)" in code                                   # oil: a reading only
    import archive_market_series as ms
    archived = {s[1] for s in ms.SERIES}
    assert {r for _, _, _, r in rows} - {"SA_REITS"} <= archived                    # every reading is archived
    assert set(_mk_funds(code)) - {"", "SA_REITS"} <= archived                      # and every fund
    # the US exactly as v_market_regime: price closes, k = 1, the VIX, the same windows and the same tests
    for frag in ("IF(mk.market_id = 'us', s.close, COALESCE(s.adj_close, s.close)) AS close",
                 "IF(m.market_id = 'us', 1.0, ROUND(m.normal_vol_pct / 16, 4)) AS k",
                 "LEFT JOIN vix AS v ON w.market_id = 'us' AND v.date = w.date",
                 "WHERE symbol = '^VIX' AND close IS NOT NULL",
                 "IF(w.n50 = 50, ROUND((SAFE_DIVIDE(w.close, w.ma50) - 1) * 100, 4), NULL) AS vs_50d_pct",
                 "IF(w.n20 = 20, ROUND(w.sd20 * SQRT(252) * 100, 4), NULL) AS vol20_pct",
                 "ROUND((SAFE_DIVIDE(w.close, w.high250) - 1) * 100, 4) AS dd_from_high_pct",
                 "IF(w.n250 >= 120, ROUND(w.sd250 * SQRT(252) * 100, 4), NULL) AS normal_vol_pct",
                 "ROUND(-2 * lim.k, 4) AS limit_r1_pct", "ROUND(-3 * lim.k, 4) AS limit_r5_pct",
                 "ROUND(-8 * lim.k, 4) AS limit_dd_pct", "ROUND(25 * lim.k, 4) AS limit_vol20_pct",
                 "WHEN x.k IS NULL THEN NULL",
                 "WHEN x.r1_pct <= x.limit_r1_pct THEN 'crash_day'",
                 "WHEN x.r5_pct <= x.limit_r5_pct OR x.dd_from_high_pct <= x.limit_dd_pct THEN 'drawdown'",
                 "WHEN (x.market_id = 'us' AND x.vix_close >= 25) OR x.vol20_pct >= x.limit_vol20_pct THEN 'stress'",
                 "WHEN x.vs_50d_pct < 0 THEN 'weak'", "ELSE 'calm'"):
        assert frag in code, frag
    regime = bv._code(objs["v_market_regime"].template)
    for frame in ("ROWS BETWEEN 49 PRECEDING AND CURRENT ROW", "ROWS BETWEEN 249 PRECEDING AND CURRENT ROW",
                  "ROWS BETWEEN 19 PRECEDING AND CURRENT ROW"):
        assert frame in code and frame in regime, frame
    for lim in ("<= -2 THEN 'crash_day'", "<= -3 OR", "<= -8 THEN 'drawdown'", "vix_close >= 25", "_vol20_pct >= 25",
                "_vs_50d_pct < 0 THEN 'weak'"):
        assert lim in regime, lim                                                   # the US limits k scales
    labels = re.findall(r"THEN '([a-z_]+)'", code.split("END AS label", 1)[0].split("CASE", 1)[1]) + ["calm"]
    assert tuple(labels) == mr.LABELS                                               # the live rule's labels, its order
    # the Saudi REITs: the funds of Tadawul's REIT codes, an equal-weight index of daily total returns
    for frag in (r"REGEXP_CONTAINS(symbol, r'^43(3[0-9]|4[0-9]|50)\.SR$') AND close > 0",
                 "COALESCE(adj_close, close) AS px", "WHERE chg IS NOT NULL AND ABS(chg) <= 0.105",
                 "HAVING COUNT(*) >= 10",
                 "100 * EXP(SUM(LN(1 + chg)) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))"):
        assert frag in code, frag
    # what it waits for: every failed test, joined; the trend score in units of the normal move
    assert "IF(x.k IS NULL, NULL, ARRAY_TO_STRING([" in code and "], ' | ')) AS waiting_for" in code
    assert ("ROUND((SAFE_DIVIDE(x.r20_pct, x.normal_vol_pct * SQRT(20 / 252))\n"
            "         + SAFE_DIVIDE(x.r60_pct, x.normal_vol_pct * SQRT(60 / 252))) / 2, 3) AS trend_score") in code
    # no look-ahead
    assert "FOLLOWING" not in code and "LEAD(" not in code and "CURRENT_DATE" not in code
    assert "no look-ahead" in v.template and "the live system never reads BigQuery" in v.template
    assert "Observe only" in v.template


def case_v43():
    """v_market_rank (1.18.0): each calendar day, each market's latest labelled close (6 days at most), open only
    when calm - the Daily Plan's US test, market by market - ranked by trend, and the verdict."""
    import re
    import daily_plan as dp
    objs = {o.name: o for o in bv.load_objects()}
    names = [o.name for o in bv.load_objects()]
    v = objs["v_market_rank"]
    assert v.path.name == "100_v_market_rank.sql" and v.kind == "VIEW"
    assert names.index("v_market_rank") == names.index("v_market_condition") + 1
    code = bv._code(v.template)
    assert set(bv._REF.findall(code)) - {"v_market_rank"} == {"v_market_condition", "market_series_daily"}
    for frag in ("FROM `{{DS}}.v_market_condition`\n  WHERE label IS NOT NULL",
                 "FROM bounds, UNNEST(GENERATE_DATE_ARRAY(bounds.d0, bounds.d1)) AS as_of",
                 "LAST_VALUE(c.date IGNORE NULLS)",
                 "OVER (PARTITION BY g.market_id ORDER BY g.as_of ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                 "LEFT JOIN c ON c.market_id = g.market_id AND c.date = g.as_of",
                 "WHERE DATE_DIFF(fl.as_of, fl.session, DAY) <= 6",
                 "ON ff.symbol = c.fund_symbol AND ff.as_of = fl.as_of AND DATE_DIFF(fl.as_of, ff.fdate, DAY) <= 6",
                 "IF(c.label = 'calm', 'open', 'hold') AS state",
                 "ORDER BY IF(r.state = 'open', 0, 1), r.trend_score DESC NULLS LAST, r.market_order) AS rank",
                 "COUNTIF(r.state = 'open' AND r.buyable) OVER (PARTITION BY r.as_of) AS open_markets",
                 "COUNTIF(r.buyable) OVER (PARTITION BY r.as_of) AS buyable_markets",
                 "IF(ranked.open_markets = 0, 'ALL ON HOLD - cash stays cash',",
                 "FORMAT('%d of %d open', ranked.open_markets, ranked.buyable_markets)) AS verdict"):
        assert frag in code, frag
    # open = calm is the Daily Plan's own test: it holds a US buy on every label but calm
    wf = (ROOT / ".github" / "workflows" / "daily_plan.yml").read_text(encoding="utf-8")
    assert "vars.TFB_PLAN_REGIME_BLOCK || 'weak,stress,drawdown,crash_day'" in wf
    assert set(dp.REGIME_LABELS) - {"weak", "stress", "drawdown", "crash_day"} == {"calm"}
    # the funds read here are the funds of v_market_condition
    cond = bv._code(objs["v_market_condition"].template)
    assert _in_list(code, "fund") == [f for f in _mk_funds(cond) if f not in ("", "SA_REITS")]
    assert "SELECT 'SA_REITS', date, r5_pct, r20_pct" in code
    assert "FOLLOWING" not in code and "LEAD(" not in code and "CURRENT_DATE" not in code
    assert re.search(r"ROW_NUMBER\(\) OVER \(PARTITION BY r\.as_of, r\.buyable", code)
    assert "the live system never reads BigQuery" in v.template and "Observe only" in v.template


def case_v44():
    """v_market_trial and v_market_trial_summary (1.18.0): each US session, what each market's fund did from the
    first close AFTER the day (never the day's own close), 'forward' from 23 Nov 2026; the scorecard the review reads."""
    objs = {o.name: o for o in bv.load_objects()}
    names = [o.name for o in bv.load_objects()]
    t, s = objs["v_market_trial"], objs["v_market_trial_summary"]
    assert t.path.name == "101_v_market_trial.sql" and s.path.name == "102_v_market_trial_summary.sql"
    assert names.index("v_market_trial") == names.index("v_market_rank") + 1
    assert names.index("v_market_trial_summary") == names.index("v_market_trial") + 1
    code = bv._code(t.template)
    assert set(bv._REF.findall(code)) - {"v_market_trial"} == {"v_market_rank", "market_series_daily",
                                                              "v_market_condition"}
    for frag in ("WHERE symbol = 'SPY' AND close > 0",                              # the decision days: US sessions
                 "JOIN us ON us.d = r.as_of",
                 "COALESCE(r.fund_symbol, r.reading_symbol) AS outcome_symbol",
                 "ON e.symbol = b.outcome_symbol AND e.date > b.as_of AND e.date <= DATE_ADD(b.as_of, INTERVAL 7 DAY)",
                 "QUALIFY ROW_NUMBER() OVER (PARTITION BY b.as_of, b.market_id ORDER BY e.date) = 1",
                 "LEFT JOIN seq AS x5 ON x5.symbol = en.symbol AND x5.n = en.entry_n + 5",
                 "LEFT JOIN seq AS x10 ON x10.symbol = en.symbol AND x10.n = en.entry_n + 10",
                 "LEFT JOIN seq AS x20 ON x20.symbol = en.symbol AND x20.n = en.entry_n + 20",
                 "IF(b.as_of >= DATE '2026-11-23', 'forward', 'backtest') AS period",
                 "LEFT JOIN fwd AS s ON s.as_of = b.as_of AND s.market_id = 'us'",
                 "ROUND(f.f5_pct - s.f5_pct, 4) AS vs_spy_f5_pp"):
        assert frag in code, frag
    assert "e.date >= b.as_of" not in code and "CURRENT_DATE" not in code and "LEAD(" not in code
    cond = bv._code(objs["v_market_condition"].template)
    assert _in_list(code, "out_px") == [f for f in _mk_funds(cond) if f not in ("", "SA_REITS")] + ["BZ=F"]
    sc = bv._code(s.template)
    assert set(bv._REF.findall(sc)) - {"v_market_trial_summary"} == {"v_market_trial"}
    for frag in ("FROM `{{DS}}.v_market_trial`\n  WHERE buyable",
                 "FROM t, UNNEST([STRUCT(5 AS horizon, t.f5_pct AS f, t.spy_f5_pct AS spy),",   # one read, 3 spans
                 "STRUCT(10, t.f10_pct, t.spy_f10_pct),", "STRUCT(20, t.f20_pct, t.spy_f20_pct)]) AS z",
                 "PERCENTILE_CONT(h.f, 0.05) OVER (PARTITION BY h.period, h.horizon, h.state) AS p5",  # exact
                 "ROUND(MAX(IF(state = 'open', p5, NULL)), 2) AS open_p5_pct",
                 "ROUND(MAX(IF(state = 'hold', p5, NULL)), 2) AS hold_p5_pct",
                 "WHERE state = 'open' AND buyable_rank = 1",
                 "COUNTIF(market_id = 'us' AND f IS NOT NULL) AS top_us_n",
                 "ROUND(AVG(IF(market_id != 'us', f - spy, NULL)), 2) AS top_vs_spy_pp",
                 "WHERE open_markets = 0 AND spy IS NOT NULL",
                 "CORR(rs, rf) AS ic", "HAVING COUNT(*) >= 5",
                 "ROUND(b.open_mean_pct - b.hold_mean_pct, 2) AS open_minus_hold_pp"):
        assert frag in sc, frag
    assert "UNION ALL" not in sc and "APPROX_QUANTILES" not in sc               # one read; the same on every read
    for tpl in (t.template, s.template):
        assert "overlap" in tpl and "the live system never reads BigQuery" in tpl and "Observe only" in tpl


def case_v45():
    """apply_bq_views 1.18.0 (kept in 1.32.0): a file number may have three digits, and the files are applied in number
    order (99 before 100); every two-digit file keeps its place."""
    assert bv.__version__ == "1.40.0"
    objs = bv.load_objects()
    files = [o.path.name for o in objs]
    assert files[-52:] == ["99_v_market_condition.sql", "100_v_market_rank.sql", "101_v_market_trial.sql",
                           "102_v_market_trial_summary.sql", "103_v_ready_list.sql", "104_v_ready_trial.sql",
                           "105_v_market_news.sql", "106_v_people_news.sql", "107_v_news_trial.sql",
                           "108_v_news_trial_summary.sql", "109_v_event_calendar.sql", "110_v_event_reactions.sql",
                           "111_v_event_scenarios.sql", "112_v_event_trial.sql", "113_v_event_trial_summary.sql",
                           "114_v_learning_scorecard.sql", "115_v_options_reading.sql",
                           "116_v_market_drivers.sql", "117_v_market_driver_learning.sql",
                           "118_v_price_drivers.sql", "119_v_driver_learning.sql",
                           "120_v_projection_context.sql", "121_v_projection_scorecard.sql",
                           "122_v_news_trend_daily.sql", "123_v_news_trend_learning.sql",
                           "124_v_news_understanding.sql", "125_v_confidence_calibration.sql",
                           "126_v_shock_drop_events.sql", "127_v_shock_drop_learning.sql",
                           "128_v_symbol_news_class_now.sql", "129_v_news_now_eval.sql",
                           "130_v_news_now_learning.sql",
                           "131_v_stop_cause_events.sql", "132_v_stop_cause_learning.sql",
                           "133_v_value_screen.sql", "134_v_trial_value_screen.sql",
                           "135_v_cash_entry_events.sql", "136_v_cash_entry_trial.sql",
                           "137_v_forward_signals.sql", "138_v_forward_signal_gates.sql",
                           "139_v_forward_signal_recon.sql", "140_v_news_llm.sql", "141_v_news_llm_days.sql",
                           "142_v_news_llm_eval.sql", "143_v_news_llm_duel.sql",
                           "144_v_challenger_v0_features.sql", "145_v_challenger_v0_ranks.sql",
                           "146_v_challenger_v0_walkforward.sql", "147_v_challenger_v0_scorecard.sql",
                           "150_v_regime_shadow.sql", "151_v_regime_shadow_insample.sql",
                           "152_v_forecast_record.sql"]
    assert files[:-51] == sorted(files[:-51])                                       # the two-digit order is unchanged
    assert [bv.file_order(Path(n)) for n in ("99_a.sql", "100_b.sql", "x.sql")] == [(99, "99_a.sql"), (100, "100_b.sql"),
                                                                                   (10 ** 9, "x.sql")]
    later = GOOD_VIEW.replace("`{{DS}}.v_a`", "`{{DS}}.v_b`", 1).replace("`{{DS}}.run_log`", "`{{DS}}.v_a`")
    with tempfile.TemporaryDirectory() as d:
        got = bv.load_objects(_write(Path(d), {"00_f_a.sql": GOOD_FN, "99_v_a.sql": GOOD_VIEW, "100_v_b.sql": later}))
        assert [o.name for o in got] == ["f_a", "v_a", "v_b"] and bv.check_objects(got) == []
    for files, needle in (({"00_f_a.sql": GOOD_FN, "1000_v_a.sql": GOOD_VIEW}, "file name must be NN_<object>.sql or NNN_"),
                          ({"00_f_a.sql": GOOD_FN, "9_v_a.sql": GOOD_VIEW}, "file name must be NN_<object>.sql or NNN_"),
                          ({"00_f_a.sql": GOOD_FN, "100_v_x.sql": GOOD_VIEW}, "defines v_a, file name says v_x")):
        with tempfile.TemporaryDirectory() as d:
            problems = bv.check_objects(bv.load_objects(_write(Path(d), files)))
            assert any(needle in p for p in problems), (needle, problems)


def case_v46():
    """v_ready_list (1.19.0): each archived day, every qualified name (the panel floors) with the market it tracks -
    not its currency - that market's state, the day's ready order (top 3 = the ready list), what it waits for, and
    the live US rule beside it. Observe only."""
    import re
    objs = {o.name: o for o in bv.load_objects()}
    names = [o.name for o in bv.load_objects()]
    v = objs["v_ready_list"]
    assert v.path.name == "103_v_ready_list.sql" and v.kind == "VIEW"
    assert names.index("v_ready_list") == names.index("v_market_trial_summary") + 1
    code = bv._code(v.template)
    assert set(bv._REF.findall(code)) - {"v_ready_list"} == {"v_universe", "v_market_rank", "v_my_portfolio"}
    # not a holding: a name held that day is out (the ready list is for new money, like v_hold_vs_switch's candidates)
    for frag in ("FROM `{{DS}}.v_my_portfolio` AS p\n  WHERE p.position_qty > 0",
                 "LEFT JOIN held AS h ON h.snapshot_date = v.snapshot_date AND h.symbol = UPPER(v.symbol)",
                 "AND h.symbol IS NULL\n),"):
        assert frag in code, frag
    assert "p.position_qty > 0" in bv._code(objs["v_hold_vs_switch"].template)          # the same holdings test
    # qualified = INVESTABLE + INVEST + the floors of v_universe_latest, one row per name and day (the last capture)
    floors = "COALESCE({0}.reliability >= 70 AND {0}.data_quality >= 80, FALSE)"
    assert floors.format("v") in code and floors.format("r") in bv._code(objs["v_universe_latest"].template)
    for frag in ("WHERE UPPER(v.investability_status) = 'INVESTABLE'", "AND UPPER(v.final_action) = 'INVEST'",
                 "AND v.block_reason IS NULL"):
        assert frag in code, frag
    # the day's last copy first, then the floors (Codex P2, #1046): a name whose last copy is blocked stays out
    assert ("WITH last_copy AS (\n  SELECT v.*\n  FROM `{{DS}}.v_universe` AS v\n"
            "  QUALIFY ROW_NUMBER() OVER (PARTITION BY v.symbol, v.snapshot_date ORDER BY v.captured_at DESC, "
            "v.source_page) = 1\n),") in code
    assert ("FROM last_copy AS v\n  LEFT JOIN held AS h ON h.snapshot_date = v.snapshot_date AND h.symbol = "
            "UPPER(v.symbol)\n  WHERE UPPER(v.investability_status) = 'INVESTABLE'") in code
    assert code.count("QUALIFY ROW_NUMBER() OVER (PARTITION BY v.symbol") == 1 and code.count("v_universe`") == 1
    # the market a name tracks: every market fund of v_market_condition by name, the futures, the listings
    cond = bv._code(objs["v_market_condition"].template)
    ids = [m for _, m, _, _ in re.findall(r"STRUCT\((\d+)(?: AS market_order)?, '([a-z_]+)'(?: AS market_id)?, "
                                          r"'([^']+)'(?: AS market)?, '([^']+)'", cond)]
    for mid, fund in zip(ids, _mk_funds(cond)):
        if fund not in ("", "SPY", "SA_REITS"):
            assert "WHEN IFNULL(u.market_code, 'US') = 'US' AND u.base = '%s' THEN '%s'" % (fund, mid) in code, fund
    mapped = set(re.findall(r"THEN '([a-z_]+)'", code.split("END AS market_id", 1)[0].split("CASE", 1)[1]))
    assert mapped | {"reits_us", "us"} <= set(ids) and "oil" in mapped                # only markets that are read
    reits = r"REGEXP_CONTAINS(symbol, r'^43(3[0-9]|4[0-9]|50)\.SR$')"
    assert reits in cond and reits.replace("(symbol", "(u.symbol") in code           # the same Tadawul REITs
    for frag in ("WHEN UPPER(IFNULL(u.asset_class, '')) != 'EQUITY' THEN NULL",
                 "THEN IF(STARTS_WITH(UPPER(IFNULL(u.industry, '')), 'REIT'), 'reits_us', 'us')",
                 "AND u.base NOT IN ('EFA', 'EEM', 'ACWI', 'VEA', 'VWO', 'IEFA', 'IEMG', 'SLV')",
                 "WHEN u.market_code IN ('HK', 'SS', 'SZ') THEN 'cn'", "WHEN u.market_code = 'T' THEN 'jp'",
                 "WHEN IFNULL(u.region, '') = 'Europe' AND IFNULL(u.market_code, '') != 'IS' THEN 'eu'",
                 "ELSE NULL\n    END AS market_id"):
        assert frag in code, frag
    # the market's state that day - or on its last day, 6 days at most - by equality joins only
    for frag in ("LEAST(m.snapshot_date, l.last_as_of) AS look_day",
                 "LEFT JOIN day_us AS d ON d.as_of = t.look_day AND DATE_DIFF(t.snapshot_date, t.look_day, DAY) <= 6",
                 "LEFT JOIN mk_day AS k ON k.as_of = d.as_of AND k.market_id = t.market_id",
                 "IF(t.market_id IS NULL OR k.state IS NULL, 'not read', k.state) AS market_state",
                 "MAX(IF(k.market_id = 'us', k.label, NULL)) OVER (PARTITION BY k.as_of) AS us_label"):
        assert frag in code, frag
    # Codex P2 (#1048): the 6 days run from the market's own close, which v_market_rank carries up to 6 days, to the
    # day - for the market and for the US label beside it - so a close 12 days old cannot pass
    for frag in ("k.date AS market_close,",
                 "MAX(IF(k.market_id = 'us', k.date, NULL)) OVER (PARTITION BY k.as_of) AS us_close",
                 "SELECT DISTINCT as_of, us_label, us_close",
                 "LEFT JOIN mk_day AS k ON k.as_of = d.as_of AND k.market_id = t.market_id\n"
                 "    AND DATE_DIFF(t.snapshot_date, k.market_close, DAY) <= 6\n),",
                 "IF(DATE_DIFF(t.snapshot_date, d.us_close, DAY) <= 6, d.us_label, NULL) AS us_label"):
        assert frag in code, frag
    rank = bv._code(objs["v_market_rank"].template)
    assert "    c.*,\n" in rank and "JOIN c ON c.market_id = fl.market_id AND c.date = fl.session" in rank  # carries date
    # the ready order: open, then held, then not read; two names a market first; the market's rank; the name
    for frag in ("ROW_NUMBER() OVER (PARTITION BY j.snapshot_date, j.market_id\n"
                 "                       ORDER BY j.expected_roi_1m_pct DESC NULLS LAST, j.overall_score DESC NULLS "
                 "LAST, j.symbol)\n      AS pos_in_market",
                 "ORDER BY CASE p.market_state WHEN 'open' THEN 0 WHEN 'hold' THEN 1 ELSE 2 END,",
                 "IF(p.pos_in_market <= 2, 0, 1), p.market_rank NULLS LAST,",
                 "o.ready_rank <= 3 AS in_ready",
                 "CASE o.market_state WHEN 'hold' THEN 'calmer market' WHEN 'open' THEN 'cash or a switch'",
                 "ELSE 'market not read' END AS waiting_for",
                 "o.market_state = 'hold' AS market_rule_holds"):
        assert frag in code, frag
    # the live US rule beside it, with the Daily Plan's workflow defaults: every USD name unless the US is calm
    assert "UPPER(IFNULL(o.currency, '')) = 'USD' AND IFNULL(o.us_label, 'calm') != 'calm' AS live_us_rule_holds" in code
    wf = (ROOT / ".github" / "workflows" / "daily_plan.yml").read_text(encoding="utf-8")
    for default in ("vars.TFB_PLAN_REGIME || 'enforce'", "vars.TFB_PLAN_REGIME_CCY || 'USD'",
                    "vars.TFB_PLAN_REGIME_BLOCK || 'weak,stress,drawdown,crash_day'"):
        assert default in wf, default
    assert "FOLLOWING" not in code and "LEAD(" not in code and "CURRENT_DATE" not in code
    assert "the live system never reads BigQuery" in v.template and "Observe only" in v.template


def case_v47():
    """v_ready_trial (1.19.0): what the qualified names did over the next week and two weeks, by the state of their
    market, on or off the ready list, held or not by each rule - one pass over the names, an exact worst 5 %."""
    import markets_board as mb
    objs = {o.name: o for o in bv.load_objects()}
    names = [o.name for o in bv.load_objects()]
    v = objs["v_ready_trial"]
    assert v.path.name == "104_v_ready_trial.sql" and v.kind == "VIEW"
    assert names.index("v_ready_trial") == names.index("v_ready_list") + 1
    code = bv._code(v.template)
    assert set(bv._REF.findall(code)) - {"v_ready_trial"} == {"v_ready_list", "v_forward_universe"}
    fwd = bv._code(objs["v_forward_universe"].template)
    for col in ("AS base_date", "AS ret_pp", "AS market_median_pp"):
        assert col in fwd, col                                                      # the outcome it reads
    for frag in ("JOIN f ON f.symbol = r.symbol AND f.base_date = r.snapshot_date",
                 "IF(r.snapshot_date >= DATE '2026-11-23', 'forward', 'backtest') AS period",
                 "f.ret_pp - f.market_median_pp AS excess_pp",
                 "FROM j, UNNEST([\n    'all qualified',\n    CONCAT('market ', j.market_state),",
                 "IF(j.in_ready, 'ready list (top 3)', 'other qualified'),",
                 "IF(j.market_rule_holds, 'market rule holds it', 'market rule does not'),",
                 "IF(j.live_us_rule_holds, 'live US rule holds it', 'live US rule does not')",
                 "PERCENTILE_CONT(g.ret_pp, 0.05) OVER (PARTITION BY g.period, g.horizon, g.grp) AS p5",
                 "ROUND(MAX(p5), 2) AS p5_ret_pp", "GROUP BY period, horizon, grp"):
        assert frag in code, frag
    assert "UNION ALL" not in code and "APPROX_QUANTILES" not in code               # one pass; the same every read
    # the forward test starts with the market trial's, on the day the board names
    assert "DATE '2026-11-23'" in bv._code(objs["v_market_trial"].template)
    assert mb.FORWARD_FROM.isoformat() == "2026-11-23"
    # every group the board's reading names is a group of this view
    import inspect
    for grp in ("ready list (top 3)", "other qualified", "market rule holds it", "market rule does not",
                "live US rule holds it", "live US rule does not"):
        assert "'%s'" % grp in code and '"%s"' % grp in inspect.getsource(mb.trial_reading), grp
    assert "FOLLOWING" not in code and "CURRENT_DATE" not in code
    assert "the live system never reads BigQuery" in v.template and "Observe only" in v.template


def _structs(code, after):
    """The rows of the first UNNEST([STRUCT(...), ...]) list after `after`, each as its quoted strings and numbers."""
    import re
    block = code.split(after, 1)[1].split("FROM UNNEST([", 1)[1].split("])", 1)[0]
    return [re.findall(r"'[^']*'|-?\b\d+\b", chunk) for chunk in block.split("STRUCT(")[1:]]


def _labels(code, column):
    """The quoted labels a CASE ... END AS <column> can return."""
    import re
    body = code.split("END AS %s" % column, 1)[0].rsplit("CASE", 1)[1]
    return set(re.findall(r"'([a-z_]+)'", body))                     # the conditions quote nothing


def case_v48():
    """v_market_news (1.20.0): each news day, each market's place against its own previous 250 news days - the share
    and tone of the news naming it and the fighting there - a 5-day score signed for the market, its momentum and a
    surprise flag. Reads the new table only; nothing after the news day enters a row. Observe only."""
    import archive_news_themes as nt
    objs = {o.name: o for o in bv.load_objects()}
    names = [o.name for o in bv.load_objects()]
    v = objs["v_market_news"]
    assert v.path.name == "105_v_market_news.sql" and v.kind == "VIEW"
    assert names.index("v_market_news") == names.index("v_ready_trial") + 1
    code = bv._code(v.template)
    assert set(bv._REF.findall(code)) - {"v_market_news"} == {"news_entity_daily"}
    # the markets are v_market_condition's, in its order and with its names; each reads a place the loader archives
    rows = _structs(code, "mk AS (")
    cond = _structs(bv._code(objs["v_market_condition"].template), "WITH mk AS (")
    assert [(r[0], r[1].strip("'"), r[2].strip("'")) for r in rows] == [
        (c[0], c[1].strip("'"), c[2].strip("'")) for c in cond]
    places = {p for p, _ in nt.PLACES}
    assert {r[3].strip("'") for r in rows} == places
    sign = {r[1].strip("'"): int(r[5]) for r in rows}
    place = {r[1].strip("'"): r[3].strip("'") for r in rows}
    assert {m for m, x in sign.items() if x == -1} == {"gold", "oil", "bonds"} and set(sign.values()) == {1, -1}
    assert place["gold"] == place["oil"] == "mideast" and place["bonds"] == place["reits_us"] == place["us"] == "us"
    assert place["sukuk"] == place["reits_sa"] == place["sa"] == "sa"
    # the features it reads are the loader's own names
    feats = {f[0] for f in nt.FEATURES if nt.TABLE_OF[f[1]] == nt.ENTITY_TABLE}
    import re
    pat = re.compile(r"^(place_[a-z]+_(share|tone)|fight_[a-z]+)$")
    assert {f for f in feats if pat.match(f)} == {f for f in feats if not f.startswith("person_")}
    for frag in ("WHERE REGEXP_CONTAINS(feature, r'^(place_[a-z]+_(share|tone)|fight_[a-z]+)$')",
                 "WINDOW w AS (PARTITION BY pl.place ORDER BY pl.date ROWS BETWEEN 250 PRECEDING AND 1 PRECEDING)",
                 "IF(st.hist_days >= 120, ROUND(SAFE_DIVIDE(st.att_pct - st.att_mean, NULLIF(st.att_sd, 0)), 3), NULL)",
                 "ROUND((z.tone_z - z.fight_z) / 2, 3) AS news_score",
                 "IF(COUNT(s.news_score) OVER w5 = 5, ROUND(AVG(s.news_score) OVER w5, 3), NULL) AS score_5",
                 "WINDOW w5 AS (PARTITION BY s.place ORDER BY s.date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)",
                 "ROUND(s5.score_5 - LAG(s5.score_5, 5) OVER (PARTITION BY s5.place ORDER BY s5.date), 3) AS momentum_5",
                 "ROUND(k.sign * m.score_5, 3) AS market_score",
                 "WHEN j.market_score >= 0.5 THEN 'favourable'", "WHEN j.market_score <= -0.5 THEN 'unfavourable'",
                 "WHEN j.market_momentum >= 0.5 THEN 'improving'", "WHEN j.market_momentum <= -0.5 THEN 'worsening'",
                 "WHEN j.att_z >= 2 THEN IF(j.sign * COALESCE(j.tone_z, 0) >= 0, 'surge_favourable', 'surge_unfavourable')"):
        assert frag in code, frag
    assert _labels(code, "reading") == {"favourable", "unfavourable", "neutral"}
    assert _labels(code, "momentum_label") == {"improving", "worsening", "steady"}
    assert _labels(code, "surprise") == {"surge_favourable", "surge_unfavourable", "none"}
    assert "FOLLOWING" not in code and "LEAD(" not in code and "CURRENT_DATE" not in code
    assert "the live system never reads BigQuery" in v.template and "Observe only" in v.template


def case_v49():
    """v_people_news (1.20.0): each news day, 17 people against their own previous 250 news days, a spike flag and the
    markets each is expected to move - the loader's people, in its order. Observe only."""
    import archive_news_themes as nt
    objs = {o.name: o for o in bv.load_objects()}
    names = [o.name for o in bv.load_objects()]
    v = objs["v_people_news"]
    assert v.path.name == "106_v_people_news.sql" and v.kind == "VIEW"
    assert names.index("v_people_news") == names.index("v_market_news") + 1
    code = bv._code(v.template)
    assert set(bv._REF.findall(code)) - {"v_people_news"} == {"news_entity_daily"}
    who = _structs(code, "who AS (")
    assert [r[1].strip("'") for r in who] == [p for p, _ in nt.PEOPLE]
    assert [int(r[0]) for r in who] == list(range(1, len(nt.PEOPLE) + 1))
    markets = {r[1].strip("'") for r in _structs(bv._code(objs["v_market_condition"].template), "WITH mk AS (")}
    for r in who:
        tied = r[-1].strip("'").split(",")
        assert tied and len(tied) == len(set(tied)) and set(tied) <= markets, r
    for frag in ("WHERE REGEXP_CONTAINS(feature, r'^person_[a-z]+_(share|tone)$')",
                 "WINDOW w AS (PARTITION BY pp.person_id ORDER BY pp.date ROWS BETWEEN 250 PRECEDING AND 1 PRECEDING)",
                 "IF(st.hist_days >= 120, ROUND(SAFE_DIVIDE(st.share_pct - st.share_mean, NULLIF(st.share_sd, 0)), 3), NULL)",
                 "WHEN z.att_z >= 2 AND z.docs >= 20 THEN IF(COALESCE(z.tone_z, 0) >= 0, 'spike_positive', 'spike_negative')",
                 "JOIN who AS w ON w.person_id = z.person_id"):
        assert frag in code, frag
    assert _labels(code, "reading") == {"spike_positive", "spike_negative", "quiet"}
    assert "FOLLOWING" not in code and "LEAD(" not in code and "CURRENT_DATE" not in code
    assert "the live system never reads BigQuery" in v.template and "Observe only" in v.template


def case_v50():
    """v_news_trial (1.20.0): each US session, each market's news reading of an earlier, complete news day and what its
    fund did next (v_market_trial, read once) - every signal of 105 and 106 with the way it is expected to move the
    market. Observe only."""
    objs = {o.name: o for o in bv.load_objects()}
    names = [o.name for o in bv.load_objects()]
    v = objs["v_news_trial"]
    assert v.path.name == "107_v_news_trial.sql" and v.kind == "VIEW"
    assert names.index("v_news_trial") == names.index("v_people_news") + 1
    code = bv._code(v.template)
    assert set(bv._REF.findall(code)) - {"v_news_trial"} == {"v_market_trial", "market_series_daily",
                                                            "news_entity_daily", "v_market_news", "v_people_news"}
    for ref in ("v_market_trial", "v_market_news", "v_people_news"):
        assert code.count("`{{DS}}.%s`" % ref) == 1, ref                          # each read once
    for frag in ("WHERE symbol = 'SPY' AND close > 0",
                 "ON n.date >= DATE_SUB(us.d, INTERVAL 3 DAY) AND n.date <= DATE_SUB(us.d, INTERVAL 1 DAY)",
                 "STRUCT('market_news' AS signal, mn.reading AS reading, mn.market_score AS value)",
                 "STRUCT('momentum', mn.momentum_label, mn.market_momentum)",
                 "STRUCT('surprise', mn.surprise, CAST(NULL AS FLOAT64))",
                 "SELECT pn.date, mk, CONCAT('person_', pn.person_id), pn.reading, pn.att_z",
                 "FROM `{{DS}}.v_people_news` AS pn, UNNEST(SPLIT(pn.markets, ',')) AS mk",
                 "JOIN nd ON nd.decision_day = t.decision_day",
                 "JOIN sig ON sig.market_id = t.market_id AND sig.date = nd.news_day"):
        assert frag in code, frag
    assert "DATE_SUB(us.d, INTERVAL 0 DAY)" not in code and "n.date <= us.d" not in code   # never the day's own news
    # every reading 105 and 106 can give has its expected way: +1 / -1, and 0 for the middle ones
    import re
    plus = set(re.findall(r"'([a-z_]+)'", code.split("WHEN sig.reading IN (", 1)[1].split(") THEN 1", 1)[0]))
    minus = set(re.findall(r"'([a-z_]+)'", code.split("WHEN sig.reading IN (", 2)[2].split(") THEN -1", 1)[0]))
    mn, pn = bv._code(objs["v_market_news"].template), bv._code(objs["v_people_news"].template)
    readings = (_labels(mn, "reading") | _labels(mn, "momentum_label") | _labels(mn, "surprise")
                | _labels(pn, "reading"))
    assert plus == {"favourable", "improving", "surge_favourable", "spike_positive"}
    assert minus == {"unfavourable", "worsening", "surge_unfavourable", "spike_negative"}
    assert readings - plus - minus == {"neutral", "steady", "none", "quiet"}
    # the forward test starts with the market trial's
    assert "IF(b.as_of >= DATE '2026-11-23', 'forward', 'backtest') AS period" in bv._code(objs["v_market_trial"].template)
    assert "FOLLOWING" not in code and "LEAD(" not in code and "CURRENT_DATE" not in code
    assert "the live system never reads BigQuery" in v.template and "Observe only" in v.template


def case_v51():
    """v_news_trial_summary (1.20.0): one pass over v_news_trial - each reading against its signal's own mean, an
    overlap-adjusted t, an exact worst 5 % and the signal's rank correlation across markets; forward lines collect
    until 10 sessions. Observe only."""
    objs = {o.name: o for o in bv.load_objects()}
    names = [o.name for o in bv.load_objects()]
    v = objs["v_news_trial_summary"]
    assert v.path.name == "108_v_news_trial_summary.sql" and v.kind == "VIEW"
    assert names.index("v_news_trial_summary") == names.index("v_news_trial") + 1 == len(names) - 43
    code = bv._code(v.template)
    assert set(bv._REF.findall(code)) - {"v_news_trial_summary"} == {"v_news_trial"}
    assert code.count("`{{DS}}.v_news_trial`") == 1
    for frag in ("FROM t, UNNEST([\n    STRUCT(5 AS horizon, t.f5_pct AS f, t.vs_spy_f5_pp AS vs),",
                 "STRUCT(10, t.f10_pct, t.vs_spy_f10_pp),", "STRUCT(20, t.f20_pct, t.vs_spy_f20_pp)",
                 "PERCENTILE_CONT(h.f, 0.05) OVER (PARTITION BY h.period, h.horizon, h.signal, h.reading) AS p5",
                 "AVG(h.f) OVER (PARTITION BY h.period, h.horizon, h.signal) AS base_f",
                 "COUNT(h.value) OVER (PARTITION BY h.period, h.horizon, h.signal, h.decision_day) AS day_n",
                 "IF(q1.day_n >= 5 AND q1.day_varies AND q1.day_moves,",
                 "CORR(q1.rank_value, q1.rank_f) OVER (PARTITION BY q1.period, q1.horizon, q1.signal, q1.decision_day)",
                 "AVG(IF(q2.day_row = 1, q2.day_ic, NULL)) OVER s AS ic_mean",
                 "GROUP BY period, horizon, signal, reading",
                 "SAFE_DIVIDE(g.mean_f - g.base_f, g.sd_f / SQRT(GREATEST(g.n / g.horizon, 1))) AS t_adj",
                 "WHEN r.period = 'forward' AND r.days < 10 THEN CONCAT('collecting: ', CAST(r.days AS STRING), ' of 10 sessions')",
                 "WHEN r.expected = 0 THEN 'the middle reading (the base)'",
                 "WHEN SIGN(r.edge) = r.expected AND ABS(r.t_adj) >= 2 THEN 'holds: the expected way, t 2 or more'"):
        assert frag in code, frag
    assert "UNION ALL" not in code and "APPROX_QUANTILES" not in code               # one pass; the same every read
    assert "h.decision_day, h.value)" not in code and "h.decision_day, h.f)" not in code   # no window keyed on a float
    # tied values share their average rank: (rank up + count - rank down + 1) / 2
    assert ("+ COUNT(h.value) OVER (PARTITION BY h.period, h.horizon, h.signal, h.decision_day)\n"
            "        - RANK() OVER (PARTITION BY h.period, h.horizon, h.signal, h.decision_day ORDER BY h.value DESC) + 1) / 2"
            in code)
    assert "FOLLOWING" not in code and "CURRENT_DATE" not in code
    assert "overlap" in v.template and "the live system never reads BigQuery" in v.template and "Observe only" in v.template


import datetime as dt  # noqa: E402 - the event views' dates (1.21.0)
import re  # noqa: E402


def _event_view(name, number, refs):
    objs = {o.name: o for o in bv.load_objects()}
    v = objs[name]
    assert v.path.name == "%d_%s.sql" % (number, name) and v.kind == "VIEW"
    code = bv._code(v.template)
    assert set(bv._REF.findall(code)) - {name} == refs, set(bv._REF.findall(code))
    assert "FOLLOWING" not in code and "LEAD(" not in code and "CURRENT_DATE" not in code
    assert "the live system never reads BigQuery" in v.template and "Observe only" in v.template
    return objs, code


def case_v52():
    """v_event_calendar (1.21.0): the Fed, ECB, Bank of Japan and OPEC+ meetings on their official dates and the
    tracked releases of econ_calendar (its latest copy, gone ones out, one per family, period and day), each figure
    against its forecast in units of the family's earlier misses. The Fed's days are the news gate's; the hand-kept
    events calendar of the News Board agrees on every meeting it lists."""
    import json as _json
    import archive_econ_calendar as ec
    from scripts import news_macro_gate as ng
    objs, code = _event_view("v_event_calendar", 109, {"econ_calendar"})
    names = [o.name for o in bv.load_objects()]
    assert names.index("v_event_calendar") == names.index("v_news_trial_summary") + 1
    days = {}
    for body, block in re.findall(r"SELECT '([a-z]+)'(?: AS family)?, d.*?FROM UNNEST\(\[(.*?)\]\) AS d", code, re.S):
        days[body] = [dt.date.fromisoformat(x) for x in re.findall(r"DATE '(\d{4}-\d{2}-\d{2})'", block)]
    assert {k: len(v) for k, v in days.items()} == {"fomc": 39, "ecb": 40, "boj": 40, "opec": 31}
    for k, v in days.items():
        assert v == sorted(v) and len(set(v)) == len(v), k
    # the Fed decides on a Wednesday (7 Nov 2024, a Thursday: the meeting followed the US election), the ECB on a Thursday
    assert [d for d in days["fomc"] if d.weekday() != 2] == [dt.date(2024, 11, 7)]
    assert all(d.weekday() == 3 for d in days["ecb"])
    assert [d.isoformat() for d in days["fomc"] if d.year >= 2026] == list(ng.FOMC_DECISION_DAYS)
    assert dt.date(2027, 10, 27) not in days["fomc"] and dt.date(2027, 11, 17) in days["fomc"]   # the 12 Nov revision
    assert days["opec"][-1] == dt.date(2026, 11, 29)
    for frag in ("TIMESTAMP(DATETIME(d, TIME '14:00:00'), 'America/New_York')",
                 "TIMESTAMP(DATETIME(d, TIME '14:15:00'), 'Europe/Berlin')",
                 "TIMESTAMP(DATETIME(d, TIME '12:00:00'), 'Asia/Tokyo')", "TIMESTAMP(DATETIME(d, TIME '12:00:00'), 'UTC')"):
        assert frag in code, frag
    # the hand-kept calendar (data/news_event_calendar.json) names the same decision days
    cal = _json.loads((ROOT / "data" / "news_event_calendar.json").read_text(encoding="utf-8"))
    words = {"fomc": "Federal Reserve", "ecb": "European Central Bank", "boj": "Bank of Japan", "opec": "OPEC+"}
    checked = 0
    for e in cal["events"]:
        for body, word in words.items():
            if word in e["title"] and e["status"] == "confirmed":
                assert dt.date.fromisoformat(e["end"]) in days[body], (body, e["end"])
                checked += 1
    assert checked >= 4
    # the releases: the loader's countries, EODHD's own names, one latest copy that is not gone
    fam = _structs(code, "WITH fam AS (")
    assert len(fam) == 15 and len({f[0] for f in fam}) == 15
    assert {f[2].strip("'") for f in fam} <= set(ec.COUNTRIES)
    # one per release: the period read without its year ("Oct. 2026" = "Oct"), a first estimate and its revisions
    # (the same period within 120 days) merged, and the copy with a figure and a forecast preferred
    per_re = re.search(r"REGEXP_REPLACE\(TRIM\(IFNULL\(l\.period, ''\)\), r'([^']+)', ''\) AS per", code)
    assert per_re, "the period without its year"
    strip = re.compile(per_re.group(1))
    assert [strip.sub("", p) for p in ("Oct. 2026", "Oct", "Q3", "Q3 2025", "Sep, 2024", "2026")] == \
        ["Oct", "Oct", "Q3", "Q3", "Sep", ""]
    for frag in ("QUALIFY ROW_NUMBER() OVER (PARTITION BY event_key ORDER BY fetched_at DESC, status) = 1",
                 "WHERE status != 'gone'",
                 "LAG(event_at) OVER (PARTITION BY family, per ORDER BY event_at, event_id), DAY) < 120",
                 "SUM(starts) OVER (PARTITION BY family, per ORDER BY event_at, event_id) AS grp",
                 "PARTITION BY family, per, IF(per = '', CAST(DATE(event_at) AS STRING), CAST(grp AS STRING))",
                 "ORDER BY IF(actual IS NULL, 2, IF(estimate IS NULL, 1, 0)), event_at, fetched_at DESC, event_id) = 1",
                 "ABS(TIMESTAMP_DIFF(l.event_at, m.event_at, HOUR)) <= 36",
                 "ROWS BETWEEN 36 PRECEDING AND 1 PRECEDING", "IF(n_sd >= 6 AND sd > 0, ROUND(surprise / sd, 3), NULL)",
                 "CASE WHEN surprise / sd >= 0.5 THEN 'above' WHEN surprise / sd <= -0.5 THEN 'below' ELSE 'in_line' END",
                 "IF(kind = 'meeting', event_at <= CURRENT_TIMESTAMP(), actual IS NOT NULL) AS is_out"):
        assert frag in code, frag
    assert _labels(code, "figure") == {"above", "below", "in_line"}


def case_v53():
    """v_event_reactions (1.21.0): each market's move from the last close before an event to the first close after it,
    each close at its own time, over 5 closes and after the event close, against the market's normal daily move. The
    markets and their outcome series are v_market_trial's."""
    objs, code = _event_view("v_event_reactions", 110, {"market_series_daily", "v_market_condition", "v_event_calendar"})
    names = [o.name for o in bv.load_objects()]
    assert names.index("v_event_reactions") == names.index("v_event_calendar") + 1
    mk = _structs(code, "WITH mk AS (")
    cond = _structs(bv._code(objs["v_market_condition"].template), "WITH mk AS (")
    assert [(r[0], r[1], r[2]) for r in mk] == [(c[0], c[1], c[2]) for c in cond]
    trial = bv._code(objs["v_market_trial"].template)
    syms = re.search(r"s\.symbol IN \(([^)]*)\)", trial).group(1)
    assert "s.symbol IN (%s)" % syms in code                                       # the same outcome series
    zones = {r[1].strip("'"): (r[3].strip("'"), r[4].strip("'")) for r in mk}
    assert zones["oil"] == ("'BZ=F'".strip("'"), "Europe/London") and zones["reits_sa"] == ("SA_REITS", "Asia/Riyadh")
    assert {z for m, (_, z) in zones.items() if m not in ("oil", "reits_sa")} == {"America/New_York"}
    for frag in ("TIME '16:00:00'", "TIME '19:30:00'", "TIME '15:00:00'",
                 "s.close_at > e.event_at AND s.close_at <= TIMESTAMP_ADD(e.event_at, INTERVAL 10 DAY)",
                 "QUALIFY ROW_NUMBER() OVER (PARTITION BY e.event_id, s.market_id ORDER BY s.n) = 1",
                 "JOIN seq AS b ON b.market_id = p.market_id AND b.n = p.post_n - 1",
                 "WHERE DATE_DIFF(p.event_close_day, b.date, DAY) <= 7",
                 "ROWS BETWEEN 249 PRECEDING AND CURRENT ROW", "IF(r.n250 >= 120 AND r.sd250 > 0, r.sd250, NULL) AS sd1",
                 "c.n BETWEEN r.post_n - 250 AND r.post_n - 1",
                 "ROUND((SAFE_DIVIDE(c.px5, c.post_px) - 1) * 100, 4) AS f5_pct", "WHERE is_out"):
        assert frag in code, frag
    assert _labels(code, "move") == {"up", "down", "normal"}


def case_v54():
    """v_event_scenarios (1.21.0): for each event, held or ahead, the earlier events of its family only - the exact
    band of their event-day moves, the big-move chance, the median move after each figure, the figure chances."""
    objs, code = _event_view("v_event_scenarios", 111, {"v_event_calendar", "v_event_reactions"})
    for frag in ("JOIN re AS r ON r.family = e.family AND r.event_at < e.event_at",
                 "JOIN ev AS p ON p.family = e.family AND p.event_at < e.event_at AND p.is_out",
                 "PERCENTILE_CONT(p.r1_pct, 0.1) OVER (PARTITION BY p.event_id, p.market_id) AS q10",
                 "PERCENTILE_CONT(p.r1_pct, 0.9) OVER (PARTITION BY p.event_id, p.market_id) AS q90",
                 "PERCENTILE_CONT(p.r1_pct, 0.5) OVER (PARTITION BY p.event_id, p.market_id, p.prior_figure) AS q50_fig",
                 "ROUND(SAFE_DIVIDE(a.n_big + 1, a.n_move + 2), 4) AS p_big",
                 "ROUND((f.f_above + 1) / (f.n_fig + 3), 4)", "ROUND((f.f_below + 1) / (f.n_fig + 3), 4)"):
        assert frag in code, frag
    assert "<=" not in code.split("pairs AS (", 1)[1].split("),", 1)[0]          # strictly earlier events
    assert "APPROX_QUANTILES" not in code and "CURRENT_" not in code


def case_v55():
    """v_event_trial (1.21.0): each scenario against what came - the band, the big move against any day's, the
    direction and the drift after a figure, the figure chances; 'forward' from 23 Nov 2026 00:00 Riyadh, scored on
    the scenario logged last before the event (event_scenario_log, archive_econ_calendar 1.1.0; Codex P2 on #1050),
    the backtest on scenarios rebuilt from today's calendar."""
    objs, code = _event_view("v_event_trial", 112, {"v_event_scenarios", "v_event_reactions", "event_scenario_log"})
    start = dt.datetime(2026, 11, 23, tzinfo=dt.timezone(dt.timedelta(hours=3))).astimezone(dt.timezone.utc)
    assert "IF(event_at >= TIMESTAMP '%s+00', 'forward', 'backtest') AS trial" % start.strftime("%Y-%m-%d %H:%M:%S") in code
    assert "event_scenario_log" in bv.SOURCE_TABLES
    # the log: the last row before the event, per event and market; a forward row takes every scenario field from it
    for frag in ("JOIN s0 ON s0.event_id = l.event_id AND s0.market_id = l.market_id AND l.logged_at < s0.event_at",
                 "WHERE s0.trial = 'forward'",
                 "QUALIFY ROW_NUMBER() OVER (PARTITION BY l.event_id, l.market_id ORDER BY l.logged_at DESC, "
                 "l.run_id DESC) = 1",
                 "CASE WHEN s0.trial = 'backtest' THEN 'rebuilt' WHEN lg.event_id IS NOT NULL THEN 'logged' "
                 "ELSE 'not logged' END",
                 "IF(s0.trial = 'forward', lg.estimate, s0.estimate) AS estimate",
                 "IF(s0.trial = 'forward', ROUND(SAFE_DIVIDE(s0.actual - lg.estimate, lg.surprise_sd), 3), "
                 "s0.surprise_z)",
                 "CASE WHEN (s0.actual - lg.estimate) / lg.surprise_sd >= 0.5 THEN 'above'",
                 "WHEN (s0.actual - lg.estimate) / lg.surprise_sd <= -0.5 THEN 'below' ELSE 'in_line' END",
                 "LEFT JOIN lg USING (event_id, market_id)"):
        assert frag in code, frag
    import archive_econ_calendar as ec                                           # the log's own columns
    for col in ("n_prior", "r1_p10", "r1_p50", "r1_p90", "n_move", "p_big", "n_above", "n_below", "r1_p50_above",
                "r1_p50_below", "n_f5_above", "n_f5_below", "f5_p50_above", "f5_p50_below", "n_fig", "p_above",
                "p_in_line", "p_below"):
        assert f"IF(s0.trial = 'forward', lg.{col}, s0.{col}) AS {col}" in code, col
        assert col in ec.LOG_COLUMNS, col
    for frag in ("IF(n_prior >= 8, r1_pct BETWEEN r1_p10 AND r1_p90, NULL) AS band_hit",
                 "IF(n_move >= 8 AND big IS NOT NULL, ROUND(POW(p_big - big, 2), 6), NULL) AS brier_big",
                 "ROUND(POW(any_big - big, 2), 6), NULL) AS brier_any",
                 "WHEN 'above' THEN IF(s.n_above >= 4, SIGN(s.r1_p50_above), NULL)",
                 "WHEN 'below' THEN IF(s.n_f5_below >= 4, SIGN(s.f5_p50_below), NULL)",
                 "IF(dir_sign IS NULL OR dir_sign = 0 OR r1_pct = 0, NULL, SIGN(r1_pct) = dir_sign) AS dir_hit",
                 "IF(kind = 'release' AND n_fig >= 8 AND figure IS NOT NULL,", "WHERE is_out"):
        assert frag in code, frag
    assert "CURRENT_" not in code


def case_v56():
    """v_event_trial_summary (1.21.0): one pass over v_event_trial with GROUPING SETS - per period, family and market
    and for all; the figure lines count each event once; forward lines collect until 10 events."""
    objs, code = _event_view("v_event_trial_summary", 113, {"v_event_trial"})
    names = [o.name for o in bv.load_objects()]
    assert names.index("v_event_trial_summary") == names.index("v_learning_scorecard") - 1   # 1.22.0 adds one view after it
    assert code.count("`{{DS}}.v_event_trial`") == 1
    for frag in ("GROUP BY GROUPING SETS ((period, family, market_id), (period, family), (period, market_id), (period))",
                 "GROUP BY period, family, event_id", "ROUND(1 - AVG(brier_figure) / (2 / 3), 3) AS fig_skill",
                 "ROUND(1 - SAFE_DIVIDE(AVG(IF(brier_any IS NULL, NULL, brier_big)), AVG(brier_any)), 3) AS big_skill",
                 # the z against a coin: (hits - n/2) / sqrt(n/4) on a one-market line; on an all-markets line each
                 # event's markets are summed first, so that an event counts once (its markets move together)
                 "IF(dir_hit IS NULL, NULL, IF(dir_hit, 0.5, -0.5)) AS d_dir",
                 "SAFE_DIVIDE(POW(SUM(d_dir) OVER (PARTITION BY period, event_id), 2), "
                 "COUNT(*) OVER (PARTITION BY period, event_id))",
                 "SAFE_DIVIDE(SUM(d_dir), SQRT(SUM(d_dir * d_dir))) AS dir_z_row",
                 "SAFE_DIVIDE(SUM(d_dir), SQRT(SUM(w_dir))) AS dir_z_event",
                 "ROUND(IF(g.g_market = 1, g.dir_z_event, g.dir_z_row), 2) AS dir_z",
                 "ROUND(IF(g.g_market = 1, g.drift_z_event, g.drift_z_row), 2) AS drift_z",
                 "COUNT(DISTINCT IF(dir_hit IS NULL, NULL, event_id)) AS dir_events",
                 "WHEN r.period = 'forward' AND r.events < 10 THEN 'collecting'",
                 "WHEN r.band_pct < 70 THEN 'too narrow' WHEN r.band_pct > 90 THEN 'too wide'",
                 "CASE WHEN r.dir_events < 20 THEN 'too few' WHEN r.dir_z >= 2 THEN 'holds' ELSE 'not proven' END",
                 "IF(r.market_id = 'all', f.fig_skill, NULL)"):
        assert frag in code, frag
    assert "CURRENT_" not in code


def case_v57():
    """v_learning_scorecard (1.22.0): ten rule variants fixed on 22 Nov 2026, each against its baseline on the scored
    calls. No look-ahead (the market label of the last close BEFORE the call; the call's OWN signal snapshot, never a
    later run of that day), the forward date written into the view, the reading only at 20 call days and 40 calls,
    research only. Three review findings of 22 Nov (Codex P2 on #1058) are pinned here: the signal's day is the Riyadh
    day, as the calls' is; the snapshot is the newest one not later than 60 seconds after the call; and the stop arms
    are described as what mae_pct is - the lowest daily LOW after the recording day, not a close."""
    objs = {o.name: o for o in bv.load_objects()}
    v = objs["v_learning_scorecard"]
    assert v.path.name == "114_v_learning_scorecard.sql" and v.kind == "VIEW"
    names = [o.name for o in bv.load_objects()]
    assert names.index("v_learning_scorecard") == names.index("v_options_reading") - 1   # 1.23.0 adds five after it
    code = bv._code(v.template)
    assert set(bv._REF.findall(code)) - {"v_learning_scorecard"} == {"v_market_condition", "v_signal_history", "v_outcome_engine"}
    assert "the live system never reads BigQuery" in v.template and "FIXED ON 22 NOV 2026 BEFORE ANY FORWARD DATA" in v.template
    assert "CURRENT_" not in code and "FOLLOWING" not in code                 # no clock, no window that looks ahead
    # the label a call is judged on is the one of its market's last close before the day of the call
    assert "AND c.date < o.recorded_date AND (c.next_date IS NULL OR c.next_date >= o.recorded_date)" in code
    assert code.count("LEAD(") == 1 and "LEAD(date) OVER (PARTITION BY market_id ORDER BY date) AS next_date" in code
    # the signal: dated as the calls are (Riyadh), and the call's own snapshot - the newest of the symbol that day
    # which is not later than 60 seconds after the call (the engine writes the snapshot a second or two after it)
    assert "DATE(recorded_at, 'Asia/Riyadh') AS day" in code and "DATE(recorded_at)" not in code
    assert "ON g.symbol = b.symbol AND g.day = b.recorded_date" in code
    assert "AND g.recorded_at <= TIMESTAMP_ADD(b.recorded_at, INTERVAL 60 SECOND)" in code
    assert "ORDER BY g.recorded_at DESC LIMIT 1)[OFFSET(0)] AS s" in code and "GROUP BY b.record_id" in code
    assert "LEFT JOIN pick g ON g.record_id = o.record_id" in code
    assert "IFNULL(g.s.upside >= 15 AND g.s.risk < 30, FALSE) AS selected" in code       # no snapshot = not kept
    # the stop arms say what mae_pct is: daily LOWS after the recording day (70_v_outcome_engine.sql), not closes
    assert "lowest daily LOW after its recording day" in v.template and "lowest close" not in v.template
    outcome = objs["v_outcome_engine"].template
    assert "IFNULL(a.tb.low, 1e18)" in outcome and "SAFE_DIVIDE(c.win_low, c.entry_price)" in outcome
    # the worst 5 % is exact, so two reads of the same rows agree
    assert "APPROX_QUANTILES" not in code
    assert "SAFE_OFFSET(CAST(FLOOR(0.05 * GREATEST(COUNTIF(kept) - 1, 0)) AS INT64))] AS p5" in code
    assert ("IF(o.recorded_date >= DATE '2026-11-23', 'forward from 2026-11-23', 'in-sample, not evidence') AS period"
            in code)
    arms = re.findall(r"STRUCT\('([A-Z]\d) ", code) + re.findall(r"'(S1) sell side", code)
    assert arms == ["B0", "E1", "E2", "E3", "E4", "E5", "X1", "X2", "X3", "S1"], arms
    hold = "label NOT IN ('weak', 'stress', 'drawdown', 'crash_day')"
    assert code.count(hold) == 2 and "label NOT IN ('stress', 'drawdown', 'crash_day'), TRUE, roi, roi" in code
    for stop in (5, 8, 10):                                                    # a stop books the stop, never better
        assert f"IF(mae_pct <= -{stop}, -{stop}.0, roi), roi)" in code
    assert "WHERE c.investable AND c.stance = 'bullish'" in code
    assert "stance = 'bearish' AS kept, stance = 'bullish' AS in_base, -roi AS r, -roi AS b" in code
    # the lift is a mean over call days of the arm's mean minus the baseline's, on days that have both
    assert "AVG(IF(kept, r, NULL)) - AVG(IF(in_base, b, NULL)) AS d" in code
    assert "HAVING COUNTIF(kept) > 0 AND COUNTIF(in_base) > 0" in code
    assert "SAFE_DIVIDE(AVG(d), SAFE_DIVIDE(STDDEV_SAMP(d), SQRT(COUNT(*)))) AS t" in code
    order = [code.index(x) for x in ("WHEN STARTS_WITH(t.arm, 'B0') THEN 'the baseline'",
                                     "WHEN STARTS_WITH(t.period, 'in-sample') THEN 'in-sample, not evidence'",
                                     "WHEN IFNULL(l.days, 0) < 20 OR t.calls < 40",
                                     "WHEN l.t >= 2 THEN 'BETTER THAN THE BASELINE'",
                                     "WHEN l.t <= -2 THEN 'WORSE THAN THE BASELINE'")]
    assert order == sorted(order), order                                       # in-sample is never read as evidence
    assert "l.t >= 2" in code and "ROUND(l.t, 2) AS t_stat" in code            # judged on the unrounded statistic


def _driver_view(name, number, refs, version="1.23.0"):
    objs = {o.name: o for o in bv.load_objects()}
    v = objs[name]
    assert v.path.name == f"{number}_{name}.sql" and v.kind == "VIEW"
    code = bv._code(v.template)
    assert set(bv._REF.findall(code)) - {name} == refs, set(bv._REF.findall(code))
    assert "Research only: no decision, gate or count reads this view." in v.template
    assert "views %s" % version in v.template
    return objs, code


def case_v58():
    """v_options_reading (1.23.0): reads the new source table options_daily (the nightly options collector); the
    percentile needs 20 days of its own history; the short-volume z needs 10 earlier days and never counts the day
    itself; the cross-sectional z-scores leave the ETFs out; the tilt is clamped to -3..+3."""
    import archive_options_reading as op
    assert bv.OPTIONS_TABLES == {op.TABLE} == {"options_daily"} and bv.OPTIONS_TABLES <= bv.SOURCE_TABLES
    objs, code = _driver_view("v_options_reading", 115, {"options_daily"})
    names = [o.name for o in bv.load_objects()]
    assert names[-36:-31] == ["v_options_reading", "v_market_drivers", "v_market_driver_learning", "v_price_drivers",
                            "v_driver_learning"]
    for frag in ("IF(h.iv_days >= 20, ROUND(h.iv_pctile, 0), NULL) AS iv_pctile_1y",
                 "ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS short_mean_20",
                 "IF(short_n_20 >= 10, SAFE_DIVIDE(short_ratio_pct - short_mean_20, short_sd_20), NULL) AS short_z",
                 "IF(is_etf, NULL, SAFE_DIVIDE(skew_25d_pp - AVG(IF(is_etf, NULL, skew_25d_pp)) OVER d",
                 "GREATEST(-3, LEAST(3, IFNULL(z_cp, 0) - IFNULL(z_skew, 0) - 0.5 * IFNULL(z_pc, 0)))) AS tilt",
                 "IFNULL(term_ratio >= 1.15 AND front_days <= 45, FALSE) AS event_priced",
                 "WINDOW d AS (PARTITION BY snapshot_date)"):
        assert frag in code, frag
    assert "FOLLOWING" not in code and "CURRENT_" not in code            # nothing from a later day, no clock
    schema = {c["name"] for c in op.SCHEMA}
    for col in ("iv30", "atm_iv_front", "atm_iv_90d", "front_days", "implied_move_pct", "skew_25d_pp",
                "cp_iv_spread_pp", "call_volume", "put_volume", "short_ratio_pct", "is_etf", "source", "captured_at"):
        assert col in schema and col in code, col


def case_v59():
    """v_market_drivers and v_market_driver_learning (1.23.0): the market's forces from market_series_daily through
    v_market_daily; z-scores from the previous 252 days only (never the day itself); the forward SPY returns are the
    only look-ahead and appear only as outcomes; a state needs 50 days before it is read."""
    import archive_market_series as ms
    _, md = _driver_view("v_market_drivers", 116, {"v_market_daily"})
    pinned = {s[1] for s in ms.SERIES}
    for sym in ("^VIX9D", "^VIX3M", "^VIX6M", "^VVIX", "^SKEW", "^MOVE", "ES=F", "NQ=F", "DX-Y.NYB", "^TNX",
                "^IRX", "HYG", "IEF", "RSP", "IWM", "BZ=F", "GC=F"):
        assert f"'{sym}'" in md and sym in pinned, sym
    used = set(re.findall(r"'([A-Z^][A-Z0-9^=.-]*)'", md.split("WHERE symbol IN (", 1)[1].split(")", 1)[0]))
    assert used <= pinned and len(used) == 22, sorted(used - pinned)               # every symbol is collected
    assert "WINDOW h AS (ORDER BY date ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING)" in md
    assert md.count("LEAD(") == 2 and "AS spy_next5_pct" in md and "AS spy_next20_pct" in md
    _, ml = _driver_view("v_market_driver_learning", 117, {"v_market_drivers"})
    assert "CASE WHEN a.days < 50 THEN 'too few days'" in ml and "CAST(FLOOR(a.days / 5) AS INT64) AS indep_5d" in ml
    assert "n.state IS NOT NULL AS is_now" in ml
    # a session without its outcome yet is no loss: every hit rate leaves it out (#1059 review)
    for col in ("spy_next5_pct", "spy_next20_pct"):
        assert f"IF({col} > 0, 1, 0))" in ml and f"AVG(IF({col} > 0" not in ml, col
    assert ml.count("IS NULL, NULL, IF(spy_next") == 4


def case_v60():
    """v_price_drivers and v_driver_learning (1.23.0): the drivers sit beside TFB's own score and change nothing; the
    observe-only score with drivers is overall + 1.5 x options tilt - 1 when short selling runs high, kept 0..100;
    the record is the daily rank IC and the quintiles against the next week's excess return, read only at 8 weeks."""
    _, pd = _driver_view("v_price_drivers", 118, {
        "v_universe_latest", "v_universe", "v_market_daily", "v_options_reading", "v_symbol_analyst",
        "v_signal_history", "v_market_drivers"})
    for frag in ("ROUND(IFNULL(1.5 * opt_tilt, 0) - IF(short_ratio_z >= 2, 1, 0), 2) AS driver_tilt_pp",
                 "GREATEST(0, LEAST(100, overall_score + IFNULL(1.5 * opt_tilt, 0) - IF(short_ratio_z >= 2, 1, 0)))",
                 "WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY)",
                 "SAFE_DIVIDE(o.iv30, u.volatility_30d_pct) AS iv_hv_ratio",
                 # the ETF closes of the same sessions as the stock's two snapshots, both included (#1059 review)
                 "IF(e.date <= dd.d1, e.close, NULL)", "IF(e.date <= dd.d0, e.close, NULL)"):
        assert frag in pd, frag
    assert "e.date < dd." not in pd
    for name in ("overall_score", "recommendation", "final_action"):                 # TFB's own fields, unchanged
        assert f" {name}," in pd or f" {name}\n" in pd, name
    _, dl = _driver_view("v_driver_learning", 119, {"v_universe", "v_options_reading"})
    for frag in ("AND market_code = 'US' AND market_cap >= 2e9 AND current_price > 0",
                 "JOIN px f ON f.symbol = p.symbol AND f.snapshot_date = DATE_ADD(p.snapshot_date, INTERVAL 7 DAY)",
                 # the options reading of the snapshot's own session (both are dated by the US session; #1059 review)
                 "AND o.snapshot_date BETWEEN DATE_SUB(r.snapshot_date, INTERVAL 6 DAY) AND r.snapshot_date",
                 "r.next_week_pct - AVG(r.next_week_pct) OVER (PARTITION BY r.snapshot_date) AS next_week_excess_pp",
                 "CORR(rank_v, rank_r) AS rank_ic", "WHERE n >= 20",
                 "COUNT(DISTINCT DATE_TRUNC(snapshot_date, WEEK(MONDAY))) AS weeks",
                 "WHEN IFNULL(a.weeks, 0) < 8 THEN 'too few weeks'",
                 "WHEN a.mean_ic >= 0.03 AND a.ic_positive_share >= 0.55 THEN 'higher is better'",
                 "WHEN a.mean_ic <= -0.03 AND a.ic_positive_share <= 0.45 THEN 'lower is better'"):
        assert frag in dl, frag
    assert "CURRENT_" not in dl                                                     # the record has no clock
    drivers = re.findall(r"STRUCT\('([^']+)'", dl)
    assert len(drivers) == 26 and drivers[:13] == drivers[13:], drivers             # the list and the record agree


def case_v61():
    """v_projection_context and v_projection_scorecard (1.24.0): the promise against the stock's own one-sigma move
    (Cboe implied volatility when read, else realised), the adverse US market states; five rules fixed on 23 Nov with
    no look-ahead (the state and the volatility of the last close BEFORE the call), a within-day lift for the stock
    rule and a between-days lift for the market rules, read only at 20 call days and 40 forward calls."""
    names = [o.name for o in bv.load_objects()]
    assert names[-31:-29] == ["v_projection_context", "v_projection_scorecard"]
    _, pc = _driver_view("v_projection_context", 120, {"v_universe_latest", "v_options_reading", "v_market_drivers"},
                         "1.24.0")
    for frag in ("COALESCE(o.iv30, u.volatility_30d_pct) AS vol",
                 "IF(vol > 0, vol * SQRT(21 / 252), NULL) AS sigma_1m", "IF(vol > 0, vol * SQRT(63 / 252), NULL) AS sigma_3m",
                 "ROUND(SIGN(expected_roi_1m_pct) * LEAST(ABS(expected_roi_1m_pct), 0.7 * sigma_1m), 2) AS shadow_roi_1m_pct",
                 "WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY)",
                 "IF(market_adverse_states >= 3 AND market_code = 'US', FORMAT("):
        assert frag in pc, frag
    adverse = ("vix_level_state IN ('20 to 30', '30 and over')", "vix_term_state = 'inverted'", "vvix_state = 'high'",
               "move_state = 'high'", "credit_state = 'weakening'", "rates_state = 'yields jumping'")
    _, sc = _driver_view("v_projection_scorecard", 121, {"v_outcome_engine", "v_market_drivers", "symbol_bars_daily"},
                         "1.24.0")
    for state in adverse:
        assert state in pc and state in sc, state                                   # the same six states in both
    for frag in ("WHERE engine_scored AND entry_investability = 'INVESTABLE' AND entry_stance = 'bullish' AND market = 'US'",
                 # no look-ahead: the market close and the volatility bar BEFORE the call's recording day
                 "LEFT JOIN md m ON c.recorded_date > m.date AND (m.next_date IS NULL OR c.recorded_date <= m.next_date)",
                 "LEFT JOIN vol v ON v.symbol = c.symbol AND c.recorded_date > v.date",
                 "ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) * SQRT(252) * 100 AS v20",
                 "IF(v.n20 >= 15 AND v.v20 > 0, v.v20 * SQRT(c.hd / 252), NULL) AS sigma",
                 "IF(c.recorded_date >= DATE '2026-11-24', 'forward from 2026-11-24', 'in-sample, not evidence') AS period",
                 "IFNULL(adverse <= 1, FALSE) AS calm", "IFNULL(SAFE_DIVIDE(p, sigma) <= 0.7, FALSE) AS measured",
                 "AVG(IF(n_kept > 0, arm_mean, NULL)) - AVG(IF(n_kept = 0, day_mean, NULL)) AS timing_lift",
                 # Codex P2 on #1064: overlapping windows - every t over SQRT(mean horizon in sessions), and the
                 # forward reading waits for two horizon lengths of call days
                 "ROUND(l.t / SQRT(t.hd), 2) AS t_stat", "ROUND(l.timing_t / SQRT(t.hd), 2) AS timing_t",
                 "ROUND(SAFE_DIVIDE(t.mean_r - t.out_mean, SQRT(q.v * t.hd)), 2) AS keep_vs_out_t",
                 "WHEN IFNULL(l.days, 0) < GREATEST(20, 2 * t.hd) OR t.calls < 40 THEN 'collecting'",
                 # Codex P2 on #1064: P3 (a market state AND a stock test) is read on kept against left-out calls,
                 # the error clustered by call day
                 "SUM(IF(kept, SAFE_DIVIDE(r - mk, nk), -SAFE_DIVIDE(r - mo, no_))) AS e",
                 "SAFE_DIVIDE(COUNT(*), COUNT(*) - 1) * SUM(e * e) AS v",
                 "WHEN STARTS_WITH(t.arm, 'P3') THEN SAFE_DIVIDE(t.mean_r - t.out_mean, SQRT(q.v))"):
        assert frag in sc, frag
    assert sc.count("ELSE l.timing_t END / SQRT(t.hd)") == 2                         # both thresholds adjusted
    arms = re.findall(r"STRUCT\('([A-Z]\d) ", sc)
    assert arms == ["B0", "P1", "P2", "P3", "P4"], arms
    order = [sc.index(x) for x in ("THEN 'the baseline'", "THEN 'in-sample, not evidence'", "THEN 'collecting'")]
    assert order == sorted(order)                                                    # in-sample is never read as evidence
    assert "CURRENT_" not in sc and "FOLLOWING" not in sc


def case_v62():
    """v_news_trend_daily, v_news_trend_learning and v_news_understanding (1.25.0): company news from both feeds, the
    price's answer against SPY and the trend before, read together into ten states fixed from research before the data
    was read; the outcome starts at the close AFTER the reaction window (no look-ahead); every t is overlap-adjusted;
    only sessions from 24 Nov 2026 are evidence; today's reading sits beside TFB's own view."""
    names = [o.name for o in bv.load_objects()]
    assert names[-29:-26] == ["v_news_trend_daily", "v_news_trend_learning", "v_news_understanding"]
    _, nd = _driver_view("v_news_trend_daily", 122,
                         {"market_series_daily", "v_symbol_news_class", "v_news_items", "symbol_bars_daily"}, "1.25.0")
    _, nl = _driver_view("v_news_trend_learning", 123, {"v_news_trend_daily"}, "1.25.0")
    # 1.25.1: the understanding reads the daily view and TFB's view only - joined to the record and the projection
    # context too, BigQuery could not plan it ("too many subqueries"); those are read beside it
    _, nu = _driver_view("v_news_understanding", 124, {"v_news_trend_daily", "v_universe_latest"}, "1.25.1")
    for frag in ("WHERE relevance = 'company' AND ENDS_WITH(symbol, '.US')",
                 "QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, news_date, IFNULL(cluster, item_id) ORDER BY published_at) = 1",
                 # a weekend item belongs to the next session
                 "LEFT JOIN news n ON n.symbol = s.symbol AND n.news_date <= s.date",
                 "AND (s.prev_date IS NULL OR n.news_date > s.prev_date)",
                 # the volatility and the trend use only the sessions before t
                 "STDDEV(x) OVER (w ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) * 100 AS sd_pp",
                 "AVG(close) OVER (w ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING) AS sma50_prev",
                 "WHEN c_prev > sma50_prev AND c_prev > c_prev21 THEN 'up'",
                 # the outcome starts at the close after the two-session reaction window
                 "LEAD(close, 6) OVER w AS c_f5", "ROUND(100 * (SAFE_DIVIDE(c_f5, c_next) - SAFE_DIVIDE(s_f5, s_next)), 2) AS f5_pp",
                 "LEAD(close, 21) OVER w AS c_f20",
                 "IF(date >= DATE '2026-11-24', 'forward from 2026-11-24', 'history, rules fixed before reading it') AS period"):
        assert frag in nd, frag
    states = re.findall(r"THEN '([a-z ,]+)'\n", nd[nd.index("END AS trend,"):nd.index("END AS state")])
    assert states == ["quiet", "good news, price agreed", "bad news, price agreed", "good news, price fell",
                      "bad news, price rose", "news, price quiet", "unsigned news, big move", "routine news",
                      "big rise, no news", "big fall, no news"], states
    expected = dict(re.findall(r"WHEN '([a-z ,]+)' THEN (-?1|CAST\(SIGN\([a-z_]+\) AS INT64\))", nd))
    assert expected == {"good news, price agreed": "1", "bad news, price agreed": "-1", "good news, price fell": "-1",
                        "bad news, price rose": "1", "news, price quiet": "CAST(SIGN(news_score) AS INT64)",
                        "unsigned news, big move": "CAST(SIGN(reaction_z) AS INT64)", "big rise, no news": "-1",
                        "big fall, no news": "1"}, expected
    for frag in ("ROUND(SAFE_DIVIDE(s.e_mean, SAFE_DIVIDE(s.e_sd, SQRT(s.days))) / SQRT(a.h), 2) AS t_adj",
                 "WHEN a.events < 20 OR s.days < GREATEST(10, 2 * a.h) THEN 'collecting'",
                 "UNNEST([STRUCT(1 AS h, t.f1_pp AS f), (5, t.f5_pp), (10, t.f10_pp), (20, t.f20_pp)]) AS hz"):
        assert frag in nl, frag
    order = [nl.index(x) for x in ("THEN 'no expectation'", "THEN 'collecting'", "THEN 'works as expected'",
                                   "THEN 'opposite of expected'")]
    assert order == sorted(order)
    assert nu.count("`{{DS}}.v_news_trend_daily`") == 1                              # 1.25.1: the daily view read once
    for frag in ("SELECT t.*, DENSE_RANK() OVER (ORDER BY t.session_date DESC) AS k",
                 "ARRAY_AGG(IF(k <= 5 AND state != 'quiet',",
                 "WHEN tfb_stance = expected THEN 'agrees with TFB'", "ELSE 'leans against TFB'",
                 # Codex P2 on #1065: 'STRONG SELL' and 'STRONG_SELL' are the same view
                 "WHEN REGEXP_REPLACE(UPPER(TRIM(recommendation)), r'[ -]+', '_') IN ('SELL', 'STRONG_SELL', 'REDUCE', 'AVOID', 'EXIT') THEN -1",
                 "WHEN REGEXP_REPLACE(UPPER(TRIM(recommendation)), r'[ -]+', '_') IN ('BUY', 'STRONG_BUY', 'ACCUMULATE', 'ADD') THEN 1"):
        assert frag in nu, frag
    # Codex P2 on #1065: SPY on the same adjusted basis as the stock
    assert "SELECT date, COALESCE(adj_close, close) AS close\n  FROM `{{DS}}.market_series_daily`" in nd
    assert "COALESCE(b.adj_close, b.close) AS close" in nd
    for code in (nd, nl, nu):
        assert "CURRENT_" not in code and "FOLLOWING" not in code


def case_v63():
    """v_confidence_calibration, v_shock_drop_events and v_shock_drop_learning (1.26.0): the Model Evidence tile's
    population by market and confidence beside each market's own return over the same dates (no other benchmark);
    the shock-drop shadow rule on the 100 largest US equities with bars, two ways of entering fixed before reading,
    the outcome from the signal close only, the history read as in-sample (survivorship), forward from 24 Nov 2026."""
    names = [o.name for o in bv.load_objects()]
    assert names[-26:-23] == ["v_confidence_calibration", "v_shock_drop_events", "v_shock_drop_learning"]
    _, cc = _driver_view("v_confidence_calibration", 125, {"v_performance_log", "market_series_daily"}, "1.26.0")
    _, se = _driver_view("v_shock_drop_events", 126, {"v_universe", "market_series_daily", "symbol_bars_daily"},
                         "1.26.0")
    _, sl = _driver_view("v_shock_drop_learning", 127, {"v_shock_drop_events"}, "1.26.0")
    for frag in ("AND UPPER(entry_investability) = 'INVESTABLE' AND is_scored AND win_flag IS NOT NULL",
                 "WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `{{DS}}.v_performance_log`)",
                 "CASE symbol WHEN 'SPY' THEN 'US' WHEN '^TASI.SR' THEN 'SA' ELSE 'OTHER' END AS market",
                 "WHERE symbol IN ('SPY', '^TASI.SR', 'ACWI')",
                 # Codex P2 on #1068: the benchmark ends on the session the outcome is priced on
                 "COALESCE(DATE(target_at), DATE(matured_at)) AS end_date",
                 # the last market close on or before each date, never after
                 "LEFT JOIN bench b0 ON b0.market = p.market AND b0.date <= p.recorded_date",
                 "AND (b0.next_date IS NULL OR b0.next_date > p.recorded_date)",
                 "LEFT JOIN bench b1 ON b1.market = p.market AND b1.date <= p.end_date",
                 "ROUND(100 * SAFE_DIVIDE(COUNTIF(excess_pp > 0), COUNTIF(excess_pp IS NOT NULL)), 1) AS beat_pct",
                 "UNNEST([market, 'ALL']) AS mk, UNNEST([confidence, 'ALL']) AS cf, UNNEST([month, 'all']) AS period",
                 "WHEN pair_n < 2 OR pair_records < 30 OR pair_days < 10 THEN 'collecting'",
                 "WHEN high_hit <= medium_hit - 10 THEN 'inverted: HIGH below MEDIUM'"):
        assert frag in cc, frag
    for frag in (# Codex P2 on #1068: every page of a night is checked (v_universe, not the one-page latest view),
                 # and the 100 is read per night, so a company that leaves it after its fall keeps its signals
                 "WHERE ENDS_WITH(symbol, '.US') AND snapshot_date >= DATE '2026-11-17'\n  GROUP BY snapshot_date, symbol\n"
                 "  HAVING LOGICAL_AND(asset_class = 'EQUITY') AND LOGICAL_AND(source_page != 'Mutual_Funds') AND MAX(market_cap) > 0",
                 "QUALIFY ROW_NUMBER() OVER (PARTITION BY snapshot_date ORDER BY mc DESC, symbol) <= 100",
                 # forward evidence only when the company was in the 100 on a night BEFORE the signal
                 "m.snapshot_date BETWEEN DATE_SUB(k.date, INTERVAL 5 DAY) AND DATE_SUB(k.date, INTERVAL 1 DAY)",
                 "WHEN member_before THEN 'forward from 2026-11-24'",
                 "ELSE 'forward, not top 100 before the signal'",
                 "JOIN big g ON g.base = REGEXP_REPLACE(b.symbol, r'\\.US$', '')",
                 "SAFE_DIVIDE(x.close, MAX(x.close) OVER (w ROWS BETWEEN 15 PRECEDING AND CURRENT ROW)) - 1 AS dd",
                 "OVER (w ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING), 0) = 1 AS fall_before",
                 "OVER (w ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING), 0) = 1 AS armed",
                 "IF(f.fall AND NOT f.fall_before, 'A at the drop', NULL)",
                 "IF(f.armed AND f.close > f.prev_high, 'B after a confirmed turn', NULL)",
                 "QUALIFY IFNULL(s.i - LAG(s.i) OVER (PARTITION BY s.symbol, s.arm ORDER BY s.date), 999) > 20",
                 "ROUND(100 * (SAFE_DIVIDE(c20, close) - SAFE_DIVIDE(s20, s_close)), 2) AS f20_pp",
                 "WHEN date < DATE '2026-11-24' THEN 'history, rule fixed before reading it'"):
        assert frag in se, frag
    for frag in ("UNNEST([STRUCT(5 AS h, e.f5_pp AS f), (10, e.f10_pp), (20, e.f20_pp)]) AS hz",
                 "ROUND(SAFE_DIVIDE(s.e_mean, SAFE_DIVIDE(s.e_sd, SQRT(s.days))) / SQRT(GREATEST(a.h / 5, 1)), 2) AS t_adj",
                 "WHEN period != 'forward from 2026-11-24' THEN 'in-sample, not evidence'",
                 "WHEN events < 10 OR days < 8 THEN 'collecting'"):
        assert frag in sl, frag
    order = [sl.index(x) for x in ("THEN 'in-sample, not evidence'", "THEN 'collecting'", "THEN 'works: beats SPY'")]
    assert order == sorted(order)                                                    # history is never read as evidence
    # the outcome is the only look forward, and the drop uses the sessions up to the signal only
    assert se.count("FOLLOWING") == 0 and se.count("LEAD(") == 6
    for code in (cc, se, sl):
        assert "CURRENT_" not in code


def case_v64():
    """v_symbol_news_class_now (1.27.0): the hourly headlines of scripts/news_now.py read with file 97's rules. The two
    files differ ONLY in the view name, the source table (symbol_news_intraday) and the description, so the hourly
    reading can never drift from the nightly one; the source tables are the job's own and declared in NOW_TABLES."""
    import re
    import archive_symbol_deep as asd
    import news_now as nn
    assert bv.NOW_TABLES == {nn.TABLE_NEWS, nn.TABLE_LOG} == {"symbol_news_intraday", "news_now_log"}
    assert bv.NOW_TABLES <= bv.SOURCE_TABLES and not (bv.NOW_TABLES & bv.DEEP_TABLES)
    assert nn.SCHEMA_NEWS is asd.SCHEMA_NEWS                                  # the same columns as the nightly feed
    objs = {o.name: o for o in bv.load_objects()}
    assert [o.name for o in bv.load_objects()][-23] == "v_symbol_news_class_now"
    _, now = _driver_view("v_symbol_news_class_now", 128, {"symbol_news_intraday", "snap_universe_latest"}, "1.29.0")
    night = bv._code(objs["v_symbol_news_class"].template)
    desc = re.compile(r'OPTIONS \(description = "[^"]*"\)')
    same = desc.sub("D", night).replace("`{{DS}}.v_symbol_news_class`", "`{{DS}}.v_symbol_news_class_now`", 1) \
        .replace("FROM `{{DS}}.symbol_news` AS n", "FROM `{{DS}}.symbol_news_intraday` AS n", 1) \
        .replace("LEFT JOIN `{{DS}}.v_universe_latest` AS u", "LEFT JOIN `{{DS}}.snap_universe_latest` AS u", 1)
    # v1.29.0: the names come from the universe snapshot, the same rows as v_universe_latest
    import bq_snapshots as bs
    assert bs.SNAPSHOTS["snap_universe_latest"][0] == "v_universe_latest" and bv.SNAP_TABLES == set(bs.SNAPSHOTS)
    assert set(v for v, _ in bs.SNAPSHOTS.values()) <= set(objs) and bv.SNAP_TABLES <= bv.SOURCE_TABLES
    assert desc.sub("D", now) == same
    assert "CURRENT_" not in now and "FOLLOWING" not in now


def case_v65():
    """v_news_now_eval and v_news_now_learning (1.28.0): the hourly reading scored. The event is the FIRST reading of a
    session with refreshed news and an expected move (a later reading never replaces it); every outcome starts at or
    after that reading (the rest of the session from the price read, then from the close); the record reads the way
    v_news_trend_learning does, with the reading fixed before any outcome was seen."""
    import news_now as nn
    names = [o.name for o in bv.load_objects()]
    assert names[-22:-20] == ["v_news_now_eval", "v_news_now_learning"]
    _, ev = _driver_view("v_news_now_eval", 129, {"news_now_log", "symbol_bars_daily", "market_series_daily"}, "1.28.0")
    _, le = _driver_view("v_news_now_learning", 130, {"v_news_now_eval"}, "1.28.0")
    log_cols = {f["name"] for f in nn.SCHEMA_LOG}
    r_cte = ev.split("r AS (\n  SELECT ", 1)[1].split("\n  FROM", 1)[0]
    assert {c.strip() for c in r_cte.replace("\n", " ").split(",")} <= log_cols
    for frag in ("WHERE ENDS_WITH(symbol, '.US') AND session_state IN ('open', 'closed today') AND news_coverage = 'fresh'",
                 "AND state IS NOT NULL AND expected != 0 AND session_date IS NOT NULL AND price > 0",
                 "QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, session_date ORDER BY captured_at) = 1",
                 "SELECT date, COALESCE(adj_close, close) AS close\n  FROM `{{DS}}.market_series_daily`",
                 "SAFE_DIVIDE(sd.close, sp.close * (1 + r.spy_change_pct / 100))",
                 "LEFT JOIN spy sp ON sp.date = r.prev_date",
                 # Codex P1 on #1073: the price at the reading on the archive's adjusted basis, never the raw price
                 "SAFE_DIVIDE(q.close, qp.close * (1 + r.change_pct / 100))",
                 "LEFT JOIN bars qp ON qp.symbol = r.symbol AND qp.date = r.prev_date",
                 # Codex P2 on #1073: SPY's sessions are the targets; a missing stock bar leaves the outcome unscored
                 "LEAD(date) OVER w AS d1", "LEAD(date, 5) OVER w AS d5",
                 "LEFT JOIN bars q1 ON q1.symbol = r.symbol AND q1.date = sd.d1",
                 "LEFT JOIN bars q5 ON q5.symbol = r.symbol AND q5.date = sd.d5",
                 "ROUND(100 * (SAFE_DIVIDE(q1.close, q.close) - SAFE_DIVIDE(sd.s1, sd.close)), 2) AS f1_pp",
                 "ROUND(100 * (SAFE_DIVIDE(q5.close, q.close) - SAFE_DIVIDE(sd.s5, sd.close)), 2) AS f5_pp"):
        assert frag in ev, frag
    assert set(nn.SESSION_TEXT.values()) >= {"open", "closed today"}           # the states the event reads
    for frag in ("UNNEST([STRUCT(0 AS h, e.rest_pp AS f), (1, e.f1_pp), (5, e.f5_pp)]) AS hz",
                 "UNNEST([e.state, 'all with a lean']) AS st",
                 "AVG(IF(f != 0, IF(SIGN(f) = expected, 1, 0), NULL)) AS right_share",
                 "ROUND(SAFE_DIVIDE(s.e_mean, SAFE_DIVIDE(s.e_sd, SQRT(s.days))) / SQRT(GREATEST(a.h, 1)), 2) AS t_adj",
                 "WHEN a.events < 20 OR s.days < GREATEST(10, 2 * a.h) THEN 'collecting'"):
        assert frag in le, frag
    order = [le.index(x) for x in ("THEN 'collecting'", "THEN 'works as expected'", "THEN 'opposite of expected'")]
    assert order == sorted(order)
    for code in (ev, le):
        assert "CURRENT_" not in code and "FOLLOWING" not in code
    assert ev.count("LEAD(") == 4 and "r.price" not in ev                     # the only look forward: SPY's targets


def case_v67():
    """v_forward_signals, v_forward_signal_gates, v_forward_signal_recon (1.31.0): the forward signal ledger - every
    signal against the benchmark of its own market net of a stated cost, the names the gates kept out beside the
    names they let in, and the ledger's counts held to the outcome engine's."""
    names = [o.name for o in bv.load_objects()]
    assert names[-14:-11] == ["v_forward_signals", "v_forward_signal_gates", "v_forward_signal_recon"]
    objs, fs = _driver_view("v_forward_signals", 137,
                            {"v_outcome_engine", "symbol_bars_daily", "v_universe", "market_series_daily"}, "1.31.0")
    _, fg = _driver_view("v_forward_signal_gates", 138, {"v_forward_signals"}, "1.31.0")
    _, fr = _driver_view("v_forward_signal_recon", 139, {"v_outcome_engine", "v_forward_signals"}, "1.31.0")
    eng = bv._code(objs["v_outcome_engine"].template)
    for frag in (# one row per Key: the engine's cohort row when it has one, else the Key's first record - in the
                 # engine's own order
                 "QUALIFY ROW_NUMBER() OVER (PARTITION BY record_key ORDER BY in_engine_cohort DESC, recorded_at, sheet_row, record_id) = 1",
                 "LEFT JOIN base b ON b.symbol = e.bars_symbol AND b.date = e.base_bar_date",
                 "LEFT JOIN uni u ON u.symbol = e.symbol AND u.snapshot_date = e.recorded_date",
                 # the gate, and the reason only when the page shows the record's own tier
                 "WHEN e.entry_tier = 'INVESTABLE' THEN 'admitted'",
                 "WHEN e.entry_tier IN ('WATCHLIST', 'BLOCKED') THEN 'kept out'",
                 "WHEN e.entry_tier IN ('WATCHLIST', 'BLOCKED') AND u.tier = e.entry_tier AND u.reason_class IS NOT NULL THEN u.reason_class",
                 "WHEN e.entry_tier IN ('WATCHLIST', 'BLOCKED') THEN '(reason not archived)'",
                 r"NULLIF(TRIM(REGEXP_EXTRACT(IFNULL(CAST(block_reason AS STRING), ''), r'^[^:(\x{2014}]*')), '') AS reason_class",
                 # the benchmark of the record's own market: total return but for the Saudi index, which has none
                 "CASE symbol WHEN 'SPY' THEN 'US' WHEN '^TASI.SR' THEN 'Tadawul' ELSE 'Other' END AS market_group,",
                 "IF(symbol = '^TASI.SR', close, COALESCE(adj_close, close)) AS px,",
                 "WHERE symbol IN ('SPY', '^TASI.SR', 'ACWI') AND close > 0",
                 "QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY captured_at DESC) = 1",
                 "WHEN '^TASI.SR' THEN 'Tadawul All Share (^TASI.SR), price only - no dividends'",
                 # anchored on the last session that had CLOSED when the record was made, to the target bar's date
                 "IF(symbol = '^TASI.SR', TIMESTAMP(DATETIME(date, TIME '15:00:00'), 'Asia/Riyadh'),",
                 "TIMESTAMP(DATETIME(date, TIME '16:00:00'), 'America/New_York')) AS closes_at",
                 "LEFT JOIN bench b0 ON b0.market_group = s.market_group AND b0.closes_at <= s.recorded_at",
                 "AND (b0.next_closes_at IS NULL OR b0.next_closes_at > s.recorded_at)",
                 "LEFT JOIN bench b1 ON b1.market_group = s.market_group AND b1.date <= s.target_bar_date",
                 "AND (b1.next_date IS NULL OR b1.next_date > s.target_bar_date)",
                 # the return, the cost, the beat (long side) and the stance's own reading
                 "IF(s.outcome_state = 'scored_from_bar', ROUND((SAFE_DIVIDE(s.target_close_adj, s.entry_price) - 1) * 100, 4), NULL) AS stock_roi,",
                 "IF(s.outcome_state = 'scored_from_bar', 100 * (SAFE_DIVIDE(b1.px, b0.px) - 1), NULL) AS bench_roi",
                 "IF(stock_roi IS NOT NULL AND bench_roi IS NOT NULL, 0.5, NULL) AS cost_pp,",
                 "ROUND(stock_roi - bench_roi - 0.5, 4) AS active_net_pp,",
                 "stock_roi - bench_roi - 0.5 > 0 AS beat_benchmark,",
                 "WHEN entry_stance = 'bullish' THEN stock_roi - bench_roi - 0.5 > 0",
                 "WHEN entry_stance = 'bearish' THEN stock_roi - bench_roi < 0",
                 # every row keeps a status
                 "WHEN engine_scored AND bench_roi IS NOT NULL THEN 'scored'",
                 "WHEN engine_scored THEN 'scored, no benchmark close'",
                 "WHEN outcome_state = 'scored_from_bar' AND entry_stance IN ('bullish', 'bearish') THEN 'not scored: the price did not move'",
                 "WHEN outcome_state = 'scored_from_bar' THEN 'tracked: no direction given'",
                 "WHEN outcome_state = 'target_not_due' THEN 'open: target not due'",
                 "WHEN outcome_state = 'awaiting_bar' THEN 'open: awaiting the bar'",
                 "ELSE CONCAT('not scorable: ', outcome_state)",
                 "CASE WHEN recorded_date < DATE '2026-11-26' THEN 'history, tally fixed before reading it' "
                 "ELSE 'forward from 2026-11-26' END AS period"):
        assert frag in fs, frag
    assert "b0.date <= s.recorded_date" not in fs          # never a close of the recording day that came after the record
    assert " WHERE " not in fs.split("\ns AS (")[1]         # no record is dropped after the one-row-per-Key pick
    # the benchmark's closing times are the engine's own venue table
    assert "('SR', 'Tadawul', 'Asia/Riyadh', 900)," in eng and "('US', 'US', 'America/New_York', 960)," in eng
    assert (900, 960) == (15 * 60, 16 * 60)
    # outcome_state is the engine's engine_state without the stance line: the same tests, in the same order
    def whens(code, start, end):
        block = code.split(start)[1].split(end)[0]
        return [ln.strip() for ln in block.splitlines() if ln.strip().startswith(("WHEN", "ELSE"))]
    mine = whens(fs.split("AS market_group,")[2], "CASE", "END AS outcome_state")
    theirs = whens(eng.split("AS close_3m,")[1], "CASE", "END AS engine_state")
    assert [w for w in theirs if "entry_stance" in w] == [
        "WHEN r.entry_stance NOT IN ('bullish', 'bearish') OR r.entry_stance IS NULL THEN 'unscorable_stance'"]
    said = [w.replace("r.", "e.").replace("a.base.close", "b.close").replace("a.tb.date", "e.target_bar_date")
             .replace("sl.symbol IS NULL", "e.last_bar_date IS NULL").replace("sl.last_bar_date", "e.last_bar_date")
            for w in theirs if "entry_stance" not in w]
    assert mine == said and len(mine) == 7, (mine, said)
    # the cost is v_hold_vs_switch's: 0.10 commission + 0.15 spread a side, both ways
    hs = bv._code(objs["v_hold_vs_switch"].template)
    assert "SELECT 0.10 AS commission_side_pp, 0.15 AS spread_side_pp" in hs and abs(2 * (0.10 + 0.15) - 0.5) < 1e-12
    # the SQL carries no bare word the applier refuses (the stance column is call_right)
    assert not bv._FORBIDDEN.search(fs) and "AS call_right," in fs
    for frag in ("UNNEST(IF(s.gate = 'kept out', [s.block_reason_class, '(all reasons)'], [s.block_reason_class])) AS r",
                 "WHERE s.gate IN ('admitted', 'kept out')",
                 "COUNTIF(active_net_pp IS NOT NULL) AS with_outcome,",
                 "COUNTIF(active_net_pp > 0) AS winners,",
                 "COUNTIF(active_net_pp <= 0) AS losers,",
                 "IF(gate = 'kept out', winners, NULL) AS blocked_winners,",
                 "IF(gate = 'admitted', losers, NULL) AS admitted_losers,",
                 # a date's signals averaged first; neighbouring dates share most of their window
                 "SELECT period, market_group, horizon, gate, block_reason_class, recorded_date, AVG(active_net_pp) AS m",
                 "/ SQRT(CASE a.horizon WHEN '1W' THEN 7 WHEN '2W' THEN 14 WHEN '1M' THEN 30 WHEN '3M' THEN 91",
                 "WHEN '6M' THEN 182 WHEN '12M' THEN 365 WHEN '1Y' THEN 365 END), 2) AS t_adj",
                 "WHEN period != 'forward from 2026-11-26' THEN 'in-sample, not evidence'",
                 "WHEN with_outcome < 30 OR days < 8 THEN 'collecting'",
                 "WHEN gate = 'kept out' AND t_adj <= -2 THEN 'works: the names kept out trailed their benchmark'",
                 "WHEN gate = 'kept out' AND t_adj >= 2 THEN 'fails: the names kept out beat their benchmark'",
                 "WHEN gate = 'admitted' AND t_adj >= 2 THEN 'works: the admitted names beat their benchmark'",
                 "WHEN gate = 'admitted' AND t_adj <= -2 THEN 'fails: the admitted names trailed their benchmark'",
                 "ELSE 'no clear edge'"):
        assert frag in fg, frag
    assert "LEAD(" not in fg and "FOLLOWING" not in fg
    for frag in ("SELECT COUNT(*) AS engine_rows, COUNT(DISTINCT record_key) AS engine_keys, COUNTIF(in_engine_cohort) AS engine_cohort",
                 "COUNTIF(STARTS_WITH(status, 'scored')) AS ledger_scored,",
                 "COUNTIF(entry_stance IN ('bullish', 'bearish') AND outcome_state != engine_state) AS state_differences,",
                 "COUNTIF(engine_roi_pp IS NOT NULL AND ABS(IFNULL(stock_roi_pp, 1e9) - engine_roi_pp) > 0.0001) AS return_differences",
                 "IF(led.ledger_rows = eng.engine_keys AND led.ledger_keys = eng.engine_keys AND led.ledger_scored = eng.engine_cohort",
                 "AND led.state_differences = 0 AND led.return_differences = 0, 'reconciled', 'DIFFERENCE') AS verdict"):
        assert frag in fr, frag
    # every status the ledger writes is one the check or the gate record reads by name
    status_case = fs.split("END AS call_right,")[1].split("END AS status")[0]
    statuses = set(re.findall(r"THEN '([^']*)'", status_case))
    assert statuses == {"scored", "scored, no benchmark close", "tracked: no direction given",
                        "not scored: the price did not move", "open: target not due", "open: awaiting the bar"}
    for word in ("'scored'", "'scored, no benchmark close'", "'tracked: no direction given'"):
        assert word in fr, word


def case_v66():
    """v_stop_cause_events / _learning, v_value_screen, v_trial_value_screen, v_cash_entry_events / _trial (1.30.0):
    three shadow records of the 26 Nov 2026 review - why a share fell and what holding did, the value screen beside
    the engine's labels, cash entered after a not-calm reading. Each rule is fixed on 26 Nov 2026, every earlier row
    is in-sample, and the outcome is the only look forward."""
    names = [o.name for o in bv.load_objects()]
    assert names[-20:-14] == ["v_stop_cause_events", "v_stop_cause_learning", "v_value_screen", "v_trial_value_screen",
                            "v_cash_entry_events", "v_cash_entry_trial"]
    _, se = _driver_view("v_stop_cause_events", 131, {"market_series_daily", "symbol_bars_daily"}, "1.30.0")
    _, sl = _driver_view("v_stop_cause_learning", 132, {"v_stop_cause_events"}, "1.30.0")
    _, vs = _driver_view("v_value_screen", 133, {"v_universe"}, "1.30.0")
    _, vt = _driver_view("v_trial_value_screen", 134, {"v_forward_universe", "v_value_screen"}, "1.30.0")
    _, ce = _driver_view("v_cash_entry_events", 135, {"v_market_regime"}, "1.30.0")
    _, ct = _driver_view("v_cash_entry_trial", 136, {"v_cash_entry_events"}, "1.30.0")
    for frag in (# each share beside its own market, on the days both have a close
                 "SELECT IF(symbol = 'SPY', 'US', 'Saudi') AS market, date, COALESCE(adj_close, close) AS m_close",
                 "WHERE symbol IN ('SPY', '^TASI.SR') AND close > 0",
                 "WHERE b.close > 0 AND (ENDS_WITH(b.symbol, '.US') OR ENDS_WITH(b.symbol, '.SR'))",
                 "JOIN idx i ON i.market = x.market AND i.date = x.date",
                 # the fall: the first close at -8 % over 15 sessions, one event per share in 20 sessions
                 "LAG(x.close, 15) OVER w AS c_base, LAG(i.m_close, 15) OVER w AS m_base,",
                 "AND 100 * (SAFE_DIVIDE(s.close, s.c_base) - 1) <= -8",
                 "AND 100 * (SAFE_DIVIDE(s.c_prev, s.c_prev_base) - 1) > -8",
                 # Codex P2 on #1085: the cooldown runs from the last KEPT event - per share the crossings as an
                 # array, each one's next eligible crossing as a pointer, the pointer doubled, the chain from the first
                 "SELECT symbol, ARRAY_AGG(n ORDER BY n) AS a",
                 "ARRAY(SELECT IFNULL((SELECT MIN(k) FROM UNNEST(a) AS y WITH OFFSET k WHERE y > x + 20), -1)",
                 "h32 AS (SELECT *, ARRAY(SELECT IFNULL(p16[SAFE_OFFSET(v)], -1) FROM UNNEST(p16) AS v WITH OFFSET i ORDER BY i) AS p32 FROM h16),",
                 "g1 AS (SELECT *, ARRAY(SELECT DISTINCT v FROM UNNEST([0, p1[OFFSET(0)]]) AS v WHERE v >= 0) AS r1 FROM h32),",
                 "SELECT g.symbol, g.a[OFFSET(v)] AS n",
                 "JOIN chain USING (symbol, n)",
                 # the cause and the verdict, from figures known at the event close
                 "CASE WHEN k.m15 <= -5 THEN 'with the market' WHEN k.m15 >= -1 THEN 'on its own' ELSE 'mixed' END AS cause",
                 "WHEN k.s15 <= -15 THEN 'hard limit'",
                 "WHEN k.m15 <= -5 AND k.s15 - k.m15 >= -5 THEN 'wait'",
                 "ELSE 'stop stands'",
                 # the rule's own exits, measured from the base of the fall
                 "JOIN seq f ON f.symbol = k.symbol AND f.n BETWEEN k.n + 1 AND k.n + 20",
                 "MIN(IF(s_base <= -15 OR x_base < -5, d, NULL)) AS exit_d",
                 "ROUND(CASE WHEN r.exit_d IS NOT NULL THEN r.exit_r WHEN r.sessions_after >= 20 THEN r.r20 END, 2) AS rule_pct",
                 "WHERE k.s15 > -60 AND IFNULL(r.widest, 0) < 150",
                 "CASE WHEN k.date < DATE '2026-11-26' THEN 'history, in-sample' ELSE 'forward from 2026-11-26' END AS period"):
        assert frag in se, frag
    assert "LEAD(" not in se and "FOLLOWING" not in se             # the path join is the only look forward
    assert "QUALIFY IFNULL(e.n - LAG(e.n)" not in se               # never a cooldown from a crossing that was left out
    # every CTE of the chain reads the one before it once: the plan stays linear (a join-based doubling was refused
    # by BigQuery as too complex on 26 Nov)
    for k, prev in (("h2", "h1"), ("h4", "h2"), ("h8", "h4"), ("h16", "h8"), ("h32", "h16"), ("g1", "h32"), ("g2", "g1"),
                    ("g3", "g2"), ("g4", "g3"), ("g5", "g4"), ("g6", "g5")):
        line = [x for x in se.splitlines() if x.startswith(k + " AS (")][0]
        assert line.count(" FROM " + prev + ")") == 1 and line.rstrip().endswith("FROM " + prev + "),"), k

    # the SQL's array steps, mirrored in Python, against the plain greedy rule
    def greedy(ns, gap=20):
        out = []
        for n in sorted(ns):
            if not out or n > out[-1] + gap:
                out.append(n)
        return out

    def doubled(a, gap=20):
        nxt = lambda v, p: p[v] if 0 <= v < len(p) else -1          # p[SAFE_OFFSET(v)], NULL -> -1
        p = [next((k for k, y in enumerate(a) if y > x + gap), -1) for x in a]
        ps = [p]
        for _ in range(5):                                          # p2, p4, p8, p16, p32
            ps.append([nxt(v, ps[-1]) for v in ps[-1]])
        r = {v for v in (0, p[0]) if v >= 0}                        # r1
        for q in ps[1:]:                                            # r2 .. r6
            r |= {w for w in (nxt(u, q) for u in r) if w >= 0}
        return sorted(a[v] for v in r)
    import random
    rnd = random.Random(26112026)
    assert greedy([20, 35, 50, 65, 80]) == doubled([20, 35, 50, 65, 80]) == [20, 50, 80]     # Codex's own example
    assert doubled([44, 55, 80, 82, 85, 101, 103, 117, 131, 141, 162]) == [44, 80, 101, 131, 162]   # read on the archive
    for _ in range(300):
        a = sorted(rnd.sample(range(16, 1400), rnd.randint(1, 120)))
        assert doubled(a) == greedy(a), a
    assert len(greedy(list(range(0, 1323, 21)))) == 63 == len(doubled(list(range(0, 1323, 21))))  # the 63-step reach
    for frag in ("UNNEST([STRUCT('hold 20 sessions' AS arm, e.r20_pct AS r), ('the rule under test', "
                 "IF(e.verdict = 'wait', e.rule_pct, NULL))]) AS a",
                 "WHERE a.r IS NOT NULL AND e.r20_pct IS NOT NULL",             # both arms on the matured events only
                 "ROUND(SAFE_DIVIDE(s.e_mean, SAFE_DIVIDE(s.e_sd, SQRT(s.days))) / 2, 2) AS t_adj",
                 "WHEN period != 'forward from 2026-11-26' THEN 'in-sample, not evidence'",
                 "WHEN events < 30 OR days < 8 THEN 'collecting'",
                 "WHEN verdict = 'wait' AND t_adj >= 2 THEN 'works: waiting beat selling at the fall'",
                 "WHEN verdict = 'wait' AND t_adj <= -2 THEN 'fails: waiting lost to selling at the fall'",
                 "WHEN verdict != 'wait' AND t_adj <= -2 THEN 'works: selling at the fall was right'",
                 "WHEN verdict != 'wait' AND t_adj >= 2 THEN 'fails: it rose after the fall'"):
        assert frag in sl, frag
    # the screen: one row per share and night, every limit from that night's own row
    for frag in ("WHERE source_page IN ('Global_Markets', 'Market_Leaders')", "AND ENDS_WITH(symbol, '.US')",
                 "AND UPPER(CAST(asset_class AS STRING)) = 'EQUITY'",
                 "QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, snapshot_date ORDER BY captured_at DESC, source_page) = 1",
                 "WHERE market_cap >= 2e9", "AND pe_forward > 0 AND pe_forward <= 15", "AND free_cash_flow_ttm > 0",
                 "AND quality_score >= 65", "AND target_upside_pct >= 20", "AND position_52w_pct <= 35",
                 "AND data_quality >= 80", "AND reliability >= 70",
                 "WHEN reco IN ('STRONG_BUY', 'BUY', 'ACCUMULATE') THEN 'buy'",
                 "WHEN reco IN ('REDUCE', 'SELL', 'STRONG_SELL', 'AVOID') THEN 'sell'",
                 "CASE WHEN snapshot_date < DATE '2026-11-26' THEN 'history, rule fixed before reading it' "
                 "ELSE 'forward from 2026-11-26' END AS period"):
        assert frag in vs, frag
    # the same row selection as v_forward_universe, so a screened share joins its own forward return
    fu = bv._code({o.name: o for o in bv.load_objects()}["v_forward_universe"].template)
    pick = "QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, snapshot_date ORDER BY captured_at DESC, source_page) = 1"
    assert pick in fu and "WHERE source_page IN ('Global_Markets', 'Market_Leaders')" in fu
    for frag in ("LEFT JOIN scr s ON s.symbol = f.symbol AND s.snapshot_date = f.base_date",
                 "UNNEST([STRUCT('value screen' AS arm, d.scr_n AS n, d.scr_mean AS m), "
                 "('engine INVESTABLE', d.inv_n, d.inv_mean)]) AS a",
                 "WHERE a.n >= 5 AND d.us_n >= 100",
                 "a.m - d.us_mean AS edge_pp",
                 "/ SQRT(IF(horizon = '1W', 7, 14)), 2) AS t_adj",
                 "WHEN period != 'forward from 2026-11-26' THEN 'in-sample, not evidence'",
                 "WHEN days < 10 THEN 'collecting'",
                 "WHEN t_adj >= 2 THEN 'works: beats the US names'",
                 "WHEN t_adj <= -2 THEN 'fails: trails the US names'"):
        assert frag in vt, frag
    # the cash entry: the first not-calm reading of a spell, three ways in, cash = 0
    for frag in ("OVER (ORDER BY date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING), 0) = 1 AS hot_before",
                 "SELECT * FROM r WHERE regime IN ('stress', 'drawdown', 'crash_day') AND NOT hot_before",
                 "JOIN r f ON f.n BETWEEN s.n AND s.n + 60",
                 "MIN(IF(d >= 1 AND reg = 'calm', d, NULL)) AS calm_d",
                 "ROUND(100 * ((SAFE_DIVIDE(p60, p0) - 1) + (SAFE_DIVIDE(p60, p5) - 1) + (SAFE_DIVIDE(p60, p10) - 1)) / 3, 2) "
                 "AS three_parts_60_pct",
                 "WHEN calm_d IS NULL OR calm_d >= 20 THEN 0 ELSE 100 * (SAFE_DIVIDE(p20, p_calm) - 1) END, 2) AS after_calm_20_pct",
                 "CASE WHEN signal_date < DATE '2026-11-26' THEN 'history, in-sample' ELSE 'forward from 2026-11-26' END AS period"):
        assert frag in ce, frag
    for frag in ("('three parts: sessions 0, 5, 10', 60, e.three_parts_60_pct),",
                 "('after the first calm close', 20, e.after_calm_20_pct), ('after the first calm close', 60, e.after_calm_60_pct)",
                 "ROUND(SAFE_DIVIDE(AVG(r), SAFE_DIVIDE(STDDEV(r), SQRT(COUNT(*)))) / SQRT(h / 10), 2) AS t_adj",
                 "WHEN period != 'forward from 2026-11-26' THEN 'in-sample, not evidence'",
                 "WHEN events < 8 THEN 'collecting'",
                 "WHEN t_adj >= 2 THEN 'works: entering beat cash'",
                 "WHEN t_adj <= -2 THEN 'fails: entering lost to cash'"):
        assert frag in ct, frag
    # history is never read as evidence: its test comes first in every record
    for code, works in ((sl, "THEN 'works: waiting beat"), (vt, "THEN 'works: beats the US names'"),
                        (ct, "THEN 'works: entering beat cash'")):
        order = [code.index(x) for x in ("THEN 'in-sample, not evidence'", "THEN 'collecting'", works)]
        assert order == sorted(order)
    for code in (se, sl, vs, vt, ce, ct):
        assert "CURRENT_" not in code                                # no clock: a rerun reads the same rows
    # the ladder, mirrored: the verdict of an event from its two 15-session returns
    def verdict(s15, m15):
        if s15 <= -15:
            return "hard limit"
        return "wait" if m15 <= -5 and s15 - m15 >= -5 else "stop stands"
    assert [verdict(-9, -6), verdict(-12, -6), verdict(-9, -0.5), verdict(-9, -3), verdict(-16, -12), verdict(-10, -5)] == [
        "wait", "stop stands", "stop stands", "stop stands", "hard limit", "wait"]
    # the exit plan's note uses the same limits (scripts/exit_plan.py 1.7.0, when it is in the tree)
    ep = ROOT / "scripts" / "exit_plan.py"
    if "TFB_EXIT_SJ_WINDOW" in ep.read_text(encoding="utf-8"):
        src = ep.read_text(encoding="utf-8")
        for knob in ('"TFB_EXIT_SJ_WINDOW", 15,', '"TFB_EXIT_SJ_DROP_PCT", 8.0,', '"TFB_EXIT_SJ_MARKET_PCT", 5.0,',
                     '"TFB_EXIT_SJ_ALONE_PCT", 1.0,', '"TFB_EXIT_SJ_MARGIN_PCT", 5.0,', '"TFB_EXIT_SJ_HARD_PCT", 15.0,'):
            assert knob in src, knob


def case_v68():
    """v_news_llm, v_news_llm_days, v_news_llm_eval, v_news_llm_duel (1.32.0): the news read twice - the gate's word
    lists and the language model (news_llm_reader, tab _News_LLM) over the same headlines, each summed per day and set
    against the next sessions with v_company_news_eval's method, and the days the two part."""
    import archive_to_bigquery as arc
    import news_llm_reader as llm
    names = [o.name for o in bv.load_objects()]
    assert names[-11:-7] == ["v_news_llm", "v_news_llm_days", "v_news_llm_eval", "v_news_llm_duel"]
    # the source: the reader's tab, copied nightly by archive 1.2.0 with header row 1
    assert arc.TABS[-1] == ("_News_LLM", 1) and arc.table_name("_News_LLM") == "news_llm" == llm.TAB.strip("_").lower()
    assert "news_llm" in bv.ARCHIVE_TABLES and "news_llm" in bv.SOURCE_TABLES
    objs, typed = _driver_view("v_news_llm", 140, {"news_llm", "f_cell", "f_num", "f_ts", "f_ts_utc"}, "1.32.0")
    assert sorted(bv.header_names_used(objs["v_news_llm"].template)) == sorted(llm.HEADER)   # every column, by name
    _, days = _driver_view("v_news_llm_days", 141, {"v_news_llm", "symbol_bars_daily", "market_series_daily"}, "1.32.0")
    _, ev = _driver_view("v_news_llm_eval", 142, {"v_news_llm_days"}, "1.32.0")
    _, du = _driver_view("v_news_llm_duel", 143, {"v_news_llm_days"}, "1.32.0")
    for frag in (# one row per headline, the latest copy; a reading with no event counts 0
                 "QUALIFY ROW_NUMBER() OVER (PARTITION BY t.item_id ORDER BY t.snapshot_date DESC) = 1",
                 "IF(IFNULL(t.rule_event, '') = '', 0, IFNULL(t.rule_direction, 0)) AS rule_dir,",
                 "IF(IFNULL(t.llm_event, 'none') IN ('', 'none'), 0, IFNULL(t.llm_direction, 0)) AS llm_dir,",
                 "DATE(COALESCE(t.published_at, t.read_at), 'Asia/Riyadh') AS news_date",
                 "`{{DS}}.f_ts_utc`(`{{DS}}.f_cell`(h, s.cells, 'Published (UTC)')) AS published_at,",
                 "`{{DS}}.f_ts`(`{{DS}}.f_cell`(h, s.cells, 'Read At (Riyadh)')) AS read_at,"):
        assert frag in typed, frag
    for frag in (# US names and MACRO only; both readers over the same headlines
                 "WHERE news_date IS NOT NULL AND (is_macro OR ENDS_WITH(symbol, '.US'))",
                 "SELECT kind, symbol, d, COUNT(*) AS headlines, SUM(rule_dir) AS rules_x, SUM(llm_dir) AS model_x",
                 "CROSS JOIN UNNEST([STRUCT('rules' AS reader, mv.rules_x AS x), STRUCT('model' AS reader, mv.model_x AS x)]) AS r",
                 "CASE WHEN r.x >= 1 THEN 'positive' WHEN r.x <= -1 THEN 'negative' ELSE 'neutral' END AS class,",
                 # entry: the first close AFTER the news day and after the day's last reading (1.33.0, audit D1),
                 # within 6 calendar days - a name's and SPY's alike
                 "MAX(read_at) AS last_read_at,",
                 "IFNULL(GREATEST(d, DATE(MAX(read_at), 'America/New_York')), d) AS after_d",
                 "JOIN bars AS b ON b.symbol = y.symbol AND b.date > y.after_d AND b.date <= DATE_ADD(y.after_d, INTERVAL 6 DAY)",
                 "JOIN spy AS s ON s.date > y.after_d AND s.date <= DATE_ADD(y.after_d, INTERVAL 6 DAY)",
                 "mv.last_read_at, mv.entry_date,",
                 # a name against the same-day US names, MACRO the S&P 500 fund itself
                 "IF(y.kind = 'MACRO', m.e.r1, n.e.r1 - p.p1) AS move_1d_pp,",
                 "IF(y.kind = 'MACRO', m.e.r5, n.e.r5 - p.p5) AS move_5d_pp",
                 "WHERE ENDS_WITH(symbol, '.US') AND close > 0",
                 "FROM `{{DS}}.market_series_daily` WHERE symbol = 'SPY' AND close IS NOT NULL GROUP BY date)",
                 "WHEN t.class = 'positive' THEN t.move_5d_pp > 0",
                 "WHEN t.class = 'negative' THEN t.move_5d_pp < 0"):
        assert frag in days, frag
    for frag in ("CORR(score, move_5d_pp) AS c",
                 "WINDOW w AS (PARTITION BY r.reader, r.kind)",
                 "WHEN n_sides < 2 THEN 'NOT MEASURABLE (one side)'",
                 "WHEN top_side_share > 0.9 THEN 'BIASED (one side > 90 %)'",
                 "WHEN min_side_matured IS NULL OR min_side_matured < 30 THEN 'THIN'",
                 "WHEN corr_5d IS NULL OR corr_5d <= 0.05 THEN 'NO EDGE'",
                 "WHEN hi_mean IS NULL OR lo_mean IS NULL OR hi_mean <= lo_mean THEN 'NO EDGE'",
                 "ELSE 'CANDIDATE (forward test)'"):
        assert frag in ev, frag
    assert ev.index("< 30 THEN 'THIN'") < ev.index("<= 0.05 THEN 'NO EDGE'")      # thin before any edge call
    for frag in ("WHEN p.rules_class = p.model_class THEN 'agree'",
                 "WHEN p.rules_class != 'neutral' AND p.model_class != 'neutral' THEN 'opposite'",
                 "WHEN p.rules_class = 'neutral' THEN 'model only'",
                 "WHERE p.rules_class IS NOT NULL AND p.model_class IS NOT NULL",
                 "WHEN t.duel = 'opposite' AND t.matured_5d < 20 THEN 'THIN'",
                 "WHEN t.duel = 'opposite' AND t.model_right_pct >= 60 THEN 'MODEL AHEAD (forward test)'",
                 "WHEN t.duel = 'opposite' AND t.rules_right_pct >= 60 THEN 'RULES AHEAD (forward test)'",
                 "WHEN t.duel = 'model only' AND t.model_calls < 30 THEN 'THIN'",
                 "WHEN t.duel = 'model only' AND t.model_right_pct >= 55 THEN 'MODEL ADDS (forward test)'",
                 "WHEN t.rules_calls < 30 THEN 'THIN'",
                 "WHEN t.rules_right_pct >= 55 THEN 'RULES ADD (forward test)'"):
        assert frag in du, frag
    assert "model_right_pct <= 40" not in du        # Codex P2 on #1094: a flat day is right for neither reader
    for code in (typed, days, ev, du):
        assert "FOLLOWING" not in code and "CURRENT_" not in code                 # nothing read from a clock
    assert days.count("LEAD(") == 4                                              # the only look forward: the targets
    assert "b.date > y.d " not in days and "s.date > y.d " not in days          # 1.33.0: never the news day alone
    assert "entry after the reading: views 1.33.0" in objs["v_news_llm_days"].template
    for o in ("v_news_llm", "v_news_llm_days", "v_news_llm_eval", "v_news_llm_duel"):
        assert "never an order" in objs[o].template and "26 Nov 2026 15:34 Riyadh" in objs[o].template, o


def case_v69():
    """v_challenger_v0_features / _ranks / _walkforward / _scorecard (1.34.0): the shadow challenger v0 of the week
    plan's package 5 - price inputs ranked per market and sector, weights learned walk-forward on purged dates, a
    shadow action and size per name, and a scorecard per market against its benchmark net of cost. A view that could
    look ahead is refused by check_objects (lookahead_problems), and each planted leak below is caught."""
    objs = bv.load_objects()
    names = [o.name for o in objs]
    chain = ["v_challenger_v0_features", "v_challenger_v0_ranks", "v_challenger_v0_walkforward",
             "v_challenger_v0_scorecard"]
    assert names[-7:-3] == chain
    by, fe = _driver_view("v_challenger_v0_features", 144, {"v_bars_clean", "v_symbol_sector"}, "1.34.0")
    _, rk = _driver_view("v_challenger_v0_ranks", 145, {"v_challenger_v0_features", "market_series_daily"}, "1.34.0")
    _, wf = _driver_view("v_challenger_v0_walkforward", 146, {"v_challenger_v0_ranks"}, "1.34.0")
    _, sc = _driver_view("v_challenger_v0_scorecard", 147, {"v_challenger_v0_walkforward"}, "1.34.0")
    assert bv.lookahead_problems(objs) == [] and bv.check_objects(objs) == []
    for frag in (# the inputs: closes up to the session only
                 "LAG(x.p, 21) OVER w AS p21, LAG(x.p, 63) OVER w AS p63,",
                 "LAG(x.p, 126) OVER w AS p126, LAG(x.p, 252) OVER w AS p252",
                 # views 1.37.0: every window runs inside one price basis of the symbol (v_bars_clean)
                 "WINDOW w AS (PARTITION BY x.symbol, x.basis ORDER BY x.date)",
                 "WINDOW w63 AS (PARTITION BY l.symbol, l.basis ORDER BY l.date ROWS BETWEEN 62 PRECEDING AND CURRENT ROW),",
                 "w126 AS (PARTITION BY l.symbol, l.basis ORDER BY l.date ROWS BETWEEN 125 PRECEDING AND CURRENT ROW)",
                 "FROM `{{DS}}.v_bars_clean`",
                 "IF(n63 >= 60 AND vol63 > 0 AND p252 > 0, SAFE_DIVIDE(p21 / p252 - 1, vol63), NULL) AS mom12_vs,",
                 "IF(n63 >= 60 AND vol63 > 0 AND p126 > 0, SAFE_DIVIDE(p21 / p126 - 1, vol63), NULL) AS mom6_vs,",
                 "IF(n126 >= 120, -down126, NULL) AS low_down126",
                 "WHEN 'US' THEN 'US' WHEN 'SR' THEN 'SA' WHEN 'T' THEN 'JP' WHEN 'HK' THEN 'HK' ELSE 'OTHER' END AS market",
                 "WHERE REGEXP_CONTAINS(s.symbol, r'\\.[A-Z]+$') AND TRIM(IFNULL(s.sector, '')) != ''"):
        assert frag in fe, frag
    assert "LEAD(" not in fe and fe.count("ROWS BETWEEN") == 2
    for frag in (# the first session of each ISO week with a 12-month history (Codex P2 on #1101: tied to the
                 # calendar, so the rolling bar window never moves a date); at least 20 peers, else the market, else 0
                 "SELECT market, MIN(date) AS d",
                 "WHERE n_m12 >= 20",
                 "GROUP BY market, DATE_TRUNC(date, ISOWEEK)",
                 "HAVING COUNT(*) >= 20",
                 "CASE WHEN r.ps_m12 IS NOT NULL AND r.ns_m12 >= 20 THEN r.ps_m12 - 0.5",
                 "WHEN r.pm_m12 IS NOT NULL AND r.nm_m12 >= 20 THEN r.pm_m12 - 0.5 ELSE 0 END AS dev_m12,",
                 "50 + 100 * (x.dev_m12 + x.dev_m6 + x.dev_rs + x.dev_dd) / 4 AS score_eq,",
                 # the result: bought at the NEXT close, sold 21 sessions later, against the market's benchmark
                 "LEAD(date, 1) OVER w AS entry_date, LEAD(p, 1) OVER w AS entry_p,",
                 "LEAD(date, 22) OVER w AS exit_date, LEAD(p, 22) OVER w AS exit_p",
                 # views 1.37.0: the result is measured inside one price basis; one that spans a break is left empty
                 "WINDOW w AS (PARTITION BY symbol, basis ORDER BY date)",
                 "IF(DATE_DIFF(l.exit_date, f.date, DAY) <= 45, (l.exit_p / l.entry_p - 1) * 100, NULL) AS fwd_pp",
                 "CASE f.market WHEN 'US' THEN 'SPY' WHEN 'SA' THEN '^TASI.SR' WHEN 'JP' THEN '^N225' WHEN 'HK' THEN '^HSI'",
                 "ANY_VALUE(IF(symbol IN ('SPY', 'ACWI'), COALESCE(adj_close, close), close)) AS bp",
                 "LAST_VALUE(r.bp IGNORE NULLS) OVER (PARTITION BY d.bench_symbol ORDER BY d.day",
                 "LEAST(GREATEST(e.excess_pp, PERCENTILE_CONT(e.excess_pp, 0.01) OVER (PARTITION BY e.market, e.d)),"):
        assert frag in rk, frag
    assert rk.count("LEAD(") == 4 and rk.count("PERCENT_RANK()") == 9
    assert "ROW_NUMBER()" not in rk and "MOD(" not in rk                     # no count of sessions anchors a date
    for frag in (# weights from earlier dates whose LAST result closed before the date (purged); warm-up 12 dates
                 "MAX(exit_date) AS exit_max,",
                 "LEFT JOIN ic AS i ON i.market = f.market AND i.exit_max < f.d",
                 "IF(COUNTIF(dev_m12 != 0) >= 20, CORR(dev_m12, label_pr), NULL) AS ic_m12,",
                 "CASE WHEN train_dates < 12 THEN 'equal (warm-up)'",
                 "WHEN p_m12 + p_m6 + p_rs + p_dd <= 0 THEN 'no input positive' ELSE 'trained' END AS weights_basis,",
                 "CASE WHEN train_dates < 12 THEN 0.25 ELSE SAFE_DIVIDE(p_m12, p_m12 + p_m6 + p_rs + p_dd) END AS w_m12,",
                 # the shadow action and size: recorded, never read
                 "CASE WHEN q.q_wf IS NULL THEN 'no view' WHEN q.q_wf = 5 THEN 'shadow buy' WHEN q.q_wf = 1 THEN 'shadow avoid'",
                 "ROUND(LEAST(10, 100 * SAFE_DIVIDE(1 / a.vol63, a.inv_vol_buys)), 2), 0) AS shadow_size_pct,",
                 "a.d = MAX(a.d) OVER (PARTITION BY a.market) AS is_latest"):
        assert frag in wf, frag
    for frag in (# per date once; net of 0.50 a round trip; t over dates / SQRT(21 / 5); the fixed reading
                 "IF(w.d < DATE '2026-11-27', 'history (backtest)', 'forward from 2026-11-27') AS period,",
                 "STRUCT('walk-forward weights' AS variant, IF(w.weights_basis = 'trained', w.score_wf, NULL) AS score,",
                 "IF(COUNT(DISTINCT score) > 1, CORR(score_pr, label_pr), NULL) AS ic,",
                 "AVG(IF(q = 5, excess_w_pp, NULL)) - 0.50 AS q5_net_pp,",
                 "ROUND(sp_mean - 1.00, 3) AS spread_net_mean_pp",
                 "SAFE_DIVIDE(a.ic_mean, SAFE_DIVIDE(a.ic_sd, SQRT(a.ic_n))) / SQRT(21 / 5) AS ic_t_adj,",
                 "WHEN dates < 26 THEN 'THIN'",
                 "WHEN ic_t_adj <= -2 THEN 'REVERSED'",
                 "WHEN ic_t_adj >= 2 AND q5_mean > 0 AND spread_t_adj >= 2 THEN 'CANDIDATE (forward test)'",
                 "ELSE 'NO EDGE'"):
        assert frag in sc, frag
    assert sc.index("'THIN'") < sc.index("'REVERSED'") < sc.index("'CANDIDATE (forward test)'")
    for code in (fe, rk, wf, sc):
        assert "FOLLOWING" not in code and "CURRENT_" not in code                 # nothing read from a clock
    for o in chain:
        assert "never an order" in by[o].template and "week plan of 23 Nov 2026, package 5" in by[o].template, o
    # each planted leak is refused (the same check runs before every write)
    planted = [
        ("v_challenger_v0_features", "LAG(x.p, 21) OVER w AS p21,", "LEAD(x.p, 21) OVER w AS p21,", "LEAD in a challenger view"),
        ("v_challenger_v0_features", "ROWS BETWEEN 62 PRECEDING AND CURRENT ROW", "ROWS BETWEEN 62 PRECEDING AND 5 FOLLOWING",
         "a forward window frame"),
        ("v_challenger_v0_ranks", "WHEN r.pm_m12 IS NOT NULL AND r.nm_m12 >= 20 THEN r.pm_m12 - 0.5 ELSE 0 END AS dev_m12",
         "WHEN r.fwd_pp > 0 THEN 0.5 ELSE 0 END AS dev_m12", "a dev_* or score_eq expression reads a result"),
        ("v_challenger_v0_ranks", "ORDER BY b.mom12_vs)) AS pm_m12", "ORDER BY b.fwd_pp)) AS pm_m12",
         "a result column in the ranking CTE 'ranked'"),
        ("v_challenger_v0_ranks", "LEAD(p, 1) OVER w AS entry_p", "p AS entry_p", "bought at the next close"),
        ("v_challenger_v0_ranks", "SELECT * FROM `{{DS}}.v_challenger_v0_features`",
         "SELECT *, LEAD(p, 21) OVER (PARTITION BY symbol ORDER BY date) AS p_next FROM `{{DS}}.v_challenger_v0_features`",
         "LEAD outside the label CTE"),
        ("v_challenger_v0_walkforward", "i.exit_max < f.d", "i.d < f.d", "train only on dates whose last result closed"),
        ("v_challenger_v0_scorecard", "FROM `{{DS}}.v_challenger_v0_walkforward` AS w",
         "FROM `{{DS}}.v_challenger_v0_walkforward` AS w WHERE CURRENT_DATE() > w.d", "a clock"),
    ]
    for name, old, new, needle in planted:
        bad = [o if o.name != name else bv.SqlObject(o.path, o.kind, o.name, o.template.replace(old, new, 1)) for o in objs]
        assert [o.template for o in bad if o.name == name][0] != by[name].template, (name, old)
        got = bv.lookahead_problems(bad)
        assert any(needle in g for g in got), (name, needle, got)
        assert any(needle in g for g in bv.check_objects(bad)), (name, needle)
    # cte_bodies reads each CTE and the final SELECT
    got = bv.cte_bodies("WITH a AS (\n  SELECT (1) AS x\n),\nb AS (\n  SELECT 2\n)\nSELECT * FROM b;")
    assert got == {"a": "\n  SELECT (1) AS x\n", "b": "\n  SELECT 2\n", "": "\nSELECT * FROM b;"}, got


CASES = [case_v1, case_v2, case_v3, case_v4, case_v5, case_v6, case_v7, case_v8, case_v9, case_v10, case_v11,
         case_v12, case_v13, case_v14, case_v15, case_v16, case_v17, case_v18, case_v19, case_v20, case_v21,
         case_v22, case_v23, case_v24, case_v25, case_v26, case_v27,
         case_v28, case_v29, case_v30, case_v31, case_v32, case_v33, case_v34, case_v35, case_v36, case_v37, case_v38,
         case_v39, case_v40, case_v41, case_v42, case_v43, case_v44, case_v45, case_v46, case_v47,
         case_v48, case_v49, case_v50, case_v51, case_v52, case_v53, case_v54, case_v55, case_v56, case_v57,
         case_v58, case_v59, case_v60, case_v61, case_v62, case_v63, case_v64, case_v65, case_v66, case_v67, case_v68,
         case_v69]

# ----------------------------------------------------------------------------- v70: the bar archive check
_D0 = dt.date(2026, 1, 1)


def _days(closes, start=0):
    """Consecutive days from 1 Jan 2026 + start, one close each."""
    return [(_D0 + dt.timedelta(days=start + i), c) for i, c in enumerate(closes)]


# The series the two view bodies were RUN on in BigQuery on 27 Nov 2026 (table reference replaced by these rows):
# every row and every column below came back the same.
BAR_SERIES = {
    # two price bases: seven bars at a third of the price, then one more bar there
    "DET": _days([64, 64.2, 18.28, 18.5, 19, 19.2, 19.1, 19.3, 19.43, 65.7, 66, 65.05, 20.24, 61.3, 61, 60.5, 60]),
    "SPK": _days([90, 91, 140, 90.5, 91, 92, 93]),                           # one bar on another level
    "STEP": _days([1.0, 1.05, 39.49, 39.0, 38, 37, 36]),                     # a reverse split recorded unadjusted
    "NEW": _days([10, 10, 10, 20, 21]),                                      # too new to tell
    "THREE": _days([10, 30, 10.5, 31, 31, 31, 31, 31]),                      # out, back, and a third nothing undid
    "FAR": _days([10, 10, 10, 10, 10, 20, 20, 20, 20, 20]) + _days([10, 10, 10, 10], start=70),   # back after 65 days
    "DRIFT": _days([10, 10, 20, 20, 20, 20, 7, 7, 7, 7]),                    # the second jump overshoots (x 0.7)
    "ZERO": _days([10, 0, 30, 30, 30, 30]),                                  # a close of 0 is no bar
    "B3": _days([10, 10, 20, 20, 20, 10, 10, 10, 10]),                       # back on the third bar: a spike
    "B4": _days([10, 10, 20, 20, 20, 20, 10, 10, 10, 10]),                   # on the fourth: a detour
    # exactly 1.5 times and exactly two thirds are jumps; 224.985 / 150 is a hair under 1.5 and is not
    "EDGE": _days([100, 150, 150, 150, 150, 149.99 * 1.5, 224.985, 224.985, 224.985, 149.99, 149.99, 149.99, 149.99]),
    "FLAT": _days([10, 11, 12, 13, 12, 11, 10]),
    # review of PR 1107: a factor of 2.9994 reads 0.333 when rounded, whose inverse is 3.003 - it is under 3
    "NEAR3": _days([100, 33.34, 33.34, 33.34, 33.34]),
    "AT3": _days([30, 10, 10, 10, 10]),                                      # exactly 3 times, downwards
    # views 1.37.0: too new to tell and 3 times - v_bars_clean starts a new basis on it (run in BigQuery on 27 Nov)
    "NEW3": _days([10, 10, 10, 30, 31]),
}
BAR_JUMPS_EXPECTED = [
    # symbol, date, ratio, leg, bars_away, back_date, bars_after, kind
    ("B3", "2026-01-03", 2.0, "out", 3, "2026-01-06", 6, "spike"),
    ("B3", "2026-01-06", 0.5, "back", 3, None, 3, "spike"),
    ("B4", "2026-01-03", 2.0, "out", 4, "2026-01-07", 7, "detour"),
    ("B4", "2026-01-07", 0.5, "back", 4, None, 3, "detour"),
    ("DET", "2026-01-03", 0.285, "out", 7, "2026-01-10", 14, "detour"),
    ("DET", "2026-01-10", 3.381, "back", 7, None, 7, "detour"),
    ("DET", "2026-01-13", 0.311, "out", 1, "2026-01-14", 4, "spike"),
    ("DET", "2026-01-14", 3.029, "back", 1, None, 3, "spike"),
    ("DRIFT", "2026-01-03", 2.0, "alone", None, None, 7, "step"),
    ("DRIFT", "2026-01-07", 0.35, "alone", None, None, 3, "step"),
    ("EDGE", "2026-01-02", 1.5, "out", 8, "2026-01-10", 11, "detour"),
    ("EDGE", "2026-01-10", 0.667, "back", 8, None, 3, "detour"),
    ("FAR", "2026-01-06", 2.0, "alone", None, None, 8, "step"),
    ("FAR", "2026-03-12", 0.5, "alone", None, None, 3, "step"),
    ("NEW", "2026-01-04", 2.0, "alone", None, None, 1, "too new"),
    ("SPK", "2026-01-03", 1.538, "out", 1, "2026-01-04", 4, "spike"),
    ("SPK", "2026-01-04", 0.646, "back", 1, None, 3, "spike"),
    ("STEP", "2026-01-03", 37.61, "alone", None, None, 4, "step"),
    ("THREE", "2026-01-02", 3.0, "out", 1, "2026-01-03", 6, "spike"),
    ("THREE", "2026-01-03", 0.35, "back", 1, None, 5, "spike"),
    ("THREE", "2026-01-04", 2.952, "alone", None, None, 4, "step"),
    ("ZERO", "2026-01-03", 3.0, "alone", None, None, 3, "step"),
]
BAR_JUMPS_EXPECTED = sorted(BAR_JUMPS_EXPECTED + [
    ("NEAR3", "2026-01-02", 0.333, "alone", None, None, 3, "step"),
    ("AT3", "2026-01-02", 0.333, "alone", None, None, 3, "step"),           # the same rounded ratio as NEAR3
    ("NEW3", "2026-01-04", 3.0, "alone", None, None, 1, "too new"),
])
BAR_SYMBOLS_EXPECTED = {
    # symbol: jumps, spikes, detours, steps, too_new, bars_on_another_level, largest_factor, largest_step_factor,
    #         is_new, reading
    "B3": (2, 1, 0, 0, 0, 3, 2.0, None, False, "bad bar"),
    "B4": (2, 0, 1, 0, 0, 4, 2.0, None, False, "two price bases"),
    "DET": (4, 1, 1, 0, 0, 8, 3.51, None, False, "two price bases"),
    "DRIFT": (2, 0, 0, 2, 0, 0, 2.86, 2.86, False, "steps under 3 times"),
    "EDGE": (2, 0, 1, 0, 0, 8, 1.5, None, False, "two price bases"),
    "FAR": (2, 0, 0, 2, 0, 0, 2.0, 2.0, True, "steps under 3 times"),
    "NEW": (1, 0, 0, 0, 1, 0, 2.0, None, False, "too new"),
    "SPK": (2, 1, 0, 0, 0, 1, 1.55, None, False, "bad bar"),
    "STEP": (1, 0, 0, 1, 0, 0, 37.61, 37.61, False, "large step"),
    "THREE": (3, 1, 0, 1, 0, 1, 3.0, 2.95, False, "bad bar"),
    "ZERO": (1, 0, 0, 1, 0, 0, 3.0, 3.0, False, "large step"),
    "NEAR3": (1, 0, 0, 1, 0, 0, 3.0, 3.0, False, "steps under 3 times"),     # 2.9994 shows as 3.0 and is under 3
    "AT3": (1, 0, 0, 1, 0, 0, 3.0, 3.0, False, "large step"),
    "NEW3": (1, 0, 0, 0, 1, 0, 3.0, None, False, "too new"),
}


def _bar_jump_rows(series):
    """The rule of v_bar_jumps in Python: one tuple per jump as BAR_JUMPS_EXPECTED lists them, plus the factor
    before rounding as its last item."""
    out = []
    for sym, rows in sorted(series.items()):
        bars = sorted((d, c) for d, c in rows if c is not None and c > 0)
        n = len(bars)
        jumps = []
        for i in range(1, n):
            (_, pc), (d, c) = bars[i - 1], bars[i]
            if c >= 1.5 * pc or c * 1.5 <= pc:
                jumps.append({"date": d, "ratio": c / pc, "factor": max(c / pc, pc / c), "bar_n": i + 1})
        chains = []
        for q, j in enumerate(jumps):
            p = jumps[q - 1] if q else None
            undoes = p is not None and 0.8 <= j["ratio"] * p["ratio"] <= 1.25 and (j["date"] - p["date"]).days <= 60
            if not undoes:
                chains.append([])
            chains[-1].append(j)
        for ch in chains:
            for k, j in enumerate(ch, 1):
                if k % 2 == 0:
                    leg, away = "back", j["bar_n"] - ch[k - 2]["bar_n"]
                elif k < len(ch):
                    leg, away = "out", ch[k]["bar_n"] - j["bar_n"]
                else:
                    leg, away = "alone", None
                after = n - j["bar_n"]
                kind = ("too new" if after < 3 else "step") if leg == "alone" else ("spike" if away <= 3 else "detour")
                out.append((sym, j["date"].isoformat(), round(j["ratio"], 3), leg, away,
                            ch[k]["date"].isoformat() if leg == "out" else None, after, kind, j["factor"]))
    return out


def _bar_jumps(series):
    """The rows of _bar_jump_rows as the view shows them (the ratio rounded, the factor left out)."""
    return [r[:8] for r in _bar_jump_rows(series)]


def _bar_jump_symbols(jumps, newest):
    """The rule of v_bar_jump_symbols in Python, from the rows of _bar_jump_rows and the archive's newest bar. The
    reading tests the factor BEFORE rounding."""
    out = {}
    for sym in sorted({j[0] for j in jumps}):
        js = [j for j in jumps if j[0] == sym]
        f = [j[8] for j in js]
        fs = [j[8] for j in js if j[7] == "step"]
        spikes = sum(1 for j in js if j[7] == "spike" and j[3] == "out")
        detours = sum(1 for j in js if j[7] == "detour" and j[3] == "out")
        steps = sum(1 for j in js if j[7] == "step")
        reading = ("two price bases" if detours else "bad bar" if spikes else "large step" if fs and max(fs) >= 3
                   else "too new" if not steps else "steps under 3 times")
        last = max(dt.date.fromisoformat(j[1]) for j in js)
        out[sym] = (len(js), spikes, detours, steps, sum(1 for j in js if j[7] == "too new"),
                    sum(j[4] for j in js if j[3] == "out"), round(max(f), 2), round(max(fs), 2) if fs else None,
                    (newest - last).days <= 7, reading)
    return out


def case_v70():
    """v_bar_jumps / v_bar_jump_symbols (1.36.0): the check of the nightly bar archive. A close of 1.5 times the
    previous archived close or more (or two thirds or less) is a jump; the next jump that brings the price back
    pairs with it (spike within 3 bars, detour after more); a jump nothing undid is a step, or too new. The symbol
    view reads each series: two price bases, bad bar, large step, too new, steps under 3 times."""
    names = [o.name for o in bv.load_objects()]
    at = names.index("f_market_summary") + 1                        # 1.37.0: ahead of every view that computes a return
    assert names[at:at + 3] == ["v_bar_jumps", "v_bar_jump_symbols", "v_bars_clean"]
    objs, bj = _driver_view("v_bar_jumps", 43, {"symbol_bars_daily"}, "1.36.0")
    _, bs = _driver_view("v_bar_jump_symbols", 44, {"v_bar_jumps", "symbol_bars_daily"}, "1.36.0")
    assert bv.check_objects(bv.load_objects()) == []
    for frag in (# the jump, on the archive's own previous bar; a close of 0 or less is no bar
                 "FROM `{{DS}}.symbol_bars_daily`\n  WHERE close > 0",
                 "WHERE prev_close > 0 AND (close >= 1.5 * prev_close OR close * 1.5 <= prev_close)",
                 # undone by the next jump: the product of the two ratios near 1, at most 60 days apart
                 "IFNULL(ratio * prev_ratio BETWEEN 0.8 AND 1.25 AND DATE_DIFF(date, prev_jump_date, DAY) <= 60, FALSE) AS undoes_prev",
                 "SELECT *, COUNTIF(NOT undoes_prev) OVER (PARTITION BY symbol ORDER BY date) AS chain",
                 "WINDOW k AS (PARTITION BY symbol, chain ORDER BY date)",
                 # a chain alternates out, back; the last odd one stands alone
                 "CASE WHEN MOD(chain_n, 2) = 0 THEN 'back' WHEN chain_n < chain_len THEN 'out' ELSE 'alone' END AS leg,",
                 "CASE WHEN MOD(chain_n, 2) = 0 THEN bar_n - chain_prev_bar WHEN chain_n < chain_len THEN chain_next_bar - bar_n END AS bars_away",
                 "IF(leg = 'out', chain_next_date, NULL) AS back_date,",
                 # the factor is not rounded: the symbol view tests it against 3
                 "GREATEST(SAFE_DIVIDE(close, prev_close), SAFE_DIVIDE(prev_close, close)) AS factor,",
                 "WHEN leg = 'alone' AND bars - bar_n < 3 THEN 'too new'",
                 "WHEN leg = 'alone' THEN 'step'",
                 "WHEN bars_away <= 3 THEN 'spike'",
                 "ELSE 'detour'"):
        assert frag in bj, frag
    for frag in ("COUNTIF(kind = 'spike' AND leg = 'out') AS spikes,",
                 "COUNTIF(kind = 'detour' AND leg = 'out') AS detours,",
                 "SUM(IF(leg = 'out', bars_away, 0)) AS bars_on_another_level,",
                 "MAX(factor) AS f_max,",
                 "MAX(IF(kind = 'step', factor, NULL)) AS f_step,",
                 "SELECT MAX(date) AS d FROM `{{DS}}.symbol_bars_daily`",
                 "DATE_DIFF(n.d, a.last_jump, DAY) <= 7 AS is_new,",
                 "WHEN a.detours >= 1 THEN 'two price bases'",
                 "WHEN a.spikes >= 1 THEN 'bad bar'",
                 "WHEN a.f_step >= 3 THEN 'large step'",
                 "WHEN a.steps = 0 THEN 'too new'",
                 "ELSE 'steps under 3 times'"):
        assert frag in bs, frag
    assert "CURRENT_" not in bj + bs                                 # no clock: 'new' is measured from the newest bar
    assert bj.count("LEAD(") == 2 and "LEAD(" not in bs              # later JUMPS of the chain only, never a later price
    # the rule, on the series the SQL itself was run on
    got = _bar_jumps(BAR_SERIES)
    assert got == BAR_JUMPS_EXPECTED
    newest = max(d for rows in BAR_SERIES.values() for d, _ in rows)
    rows = _bar_jump_rows(BAR_SERIES)
    assert _bar_jump_symbols(rows, newest) == BAR_SYMBOLS_EXPECTED
    # review of PR 1107: the two series show the same rounded ratio and read differently, because the factor is not
    # rounded before it is tested against 3
    near, at = [r for r in rows if r[0] == "NEAR3"][0], [r for r in rows if r[0] == "AT3"][0]
    assert near[2] == at[2] == 0.333 and round(near[8], 4) == 2.9994 and at[8] == 3.0
    assert "GREATEST(ratio, SAFE_DIVIDE(1, ratio))" not in bs and "ROUND(" not in bs.split("FROM `{{DS}}.v_bar_jumps`")[0]
    assert "FLAT" not in {j[0] for j in got}                         # no jump, no row
    # each threshold of the rule decides a case above
    assert [j[7] for j in got if j[0] in ("B3", "B4")] == ["spike", "spike", "detour", "detour"]       # 3 bars / 4 bars
    assert [j[3] for j in got if j[0] in ("FAR", "DRIFT")] == ["alone"] * 4                            # 60 days / 0.8
    assert [j[7] for j in got if j[0] == "NEW"] == ["too new"] and [j[6] for j in got if j[0] == "ZERO"] == [3]
    # the product of 0.8 and of 1.25 still pair; 61 days apart do not
    edge = {"P80": _days([10, 10, 20, 20, 8, 8, 8, 8]), "P125": _days([10, 10, 20, 20, 12.5, 12.5, 12.5, 12.5]),
            "P79": _days([10, 10, 20, 20, 7.9, 7.9, 7.9, 7.9]), "P126": _days([10, 10, 20, 20, 12.6, 12.6, 12.6, 12.6]),
            "D60": _days([10, 20]) + _days([10, 10, 10, 10], start=61), "D61": _days([10, 20]) + _days([10, 10, 10, 10], start=62)}
    legs = {s: [j[3] for j in _bar_jumps({s: rows})] for s, rows in edge.items()}
    assert legs == {"P80": ["out", "back"], "P125": ["out", "back"], "P79": ["alone", "alone"], "P126": ["alone", "alone"],
                    "D60": ["out", "back"], "D61": ["alone", "alone"]}

# ----------------------------------------------------------------------------- v71: clean bars for the return maths
# What the body of 45_v_bars_clean.sql returned in BigQuery on 27 Nov 2026 for BAR_SERIES (with 43_v_bar_jumps.sql
# in place of the installed view): bars kept, the dates removed, and the basis of each kept bar in date order.
BARS_CLEAN_EXPECTED = {
    "AT3": (5, [], "01111"),                       # exactly 3 times and nothing undid it: a new basis
    "B3": (6, ["01-03", "01-04", "01-05"], "000000"),   # a spike of three bars: removed
    "B4": (10, [], "0011112222"),                  # a detour: a new basis at each end, nothing removed
    "DET": (16, ["01-13"], "0011111112222222"),    # a detour, then a one-bar spike
    "DRIFT": (10, [], "0000000000"),               # two steps under 3 times: kept whole
    "EDGE": (13, [], "0111111112222"),             # a detour of exactly 1.5 and two thirds
    "FAR": (14, [], "00000000000000"),
    "FLAT": (7, [], "0000000"),
    "NEAR3": (5, [], "00000"),                     # 2.9994 times: under 3, kept whole
    "NEW": (5, [], "00000"),                       # too new and under 3 times
    "SPK": (6, ["01-03"], "000000"),
    "STEP": (7, [], "0011111"),                    # 37.6 times: a new basis
    "THREE": (7, ["01-02"], "0000000"),            # a spike, then a step of 2.95 (kept)
    "ZERO": (5, [], "01111"),                      # 3 times across a close of 0 (no bar)
    "NEW3": (5, [], "00011"),                      # too new, 3 times: a new basis at once
}


def _bars_clean(series):
    """The rule of v_bars_clean in Python: {symbol: (kept, removed MM-DD dates, basis string)} from the rows of
    _bar_jump_rows - a spike's bars removed, a new basis at every detour jump and every jump of 3 times or more that
    nothing undid."""
    rows = _bar_jump_rows(series)
    out = {}
    for sym, bars in sorted(series.items()):
        js = [r for r in rows if r[0] == sym]
        spikes = [(dt.date.fromisoformat(r[1]), dt.date.fromisoformat(r[5])) for r in js if r[7] == "spike" and r[3] == "out"]
        breaks = {dt.date.fromisoformat(r[1]) for r in js if r[7] == "detour" or (r[3] == "alone" and r[8] >= 3)}
        kept, removed = [], []
        for d, c in sorted(bars):
            if c is None or c <= 0:
                continue
            if any(d0 <= d < d1 for d0, d1 in spikes):
                removed.append(d.strftime("%m-%d"))
                continue
            kept.append(sum(1 for k in breaks if k <= d))
        out[sym] = (len(kept), removed, "".join(str(b) for b in kept))
    return out


def case_v71():
    """v_bars_clean (1.37.0): the archive less the bars of its one-to-three-bar spikes, with a price basis that starts
    anew at every detour jump and every jump of 3 times or more that nothing undid; the shadow challenger reads it,
    runs every input window inside one basis and measures every result inside one basis."""
    objs = bv.load_objects()
    names = [o.name for o in objs]
    assert names.index("v_bar_jumps") < names.index("v_bar_jump_symbols") < names.index("v_bars_clean") \
        < names.index("v_outcome_engine") < names.index("v_challenger_v0_features")
    by, cl = _driver_view("v_bars_clean", 45, {"v_bar_jumps", "symbol_bars_daily"}, "1.37.0")
    for frag in ("SELECT symbol, date, back_date, leg, kind, factor\n  FROM `{{DS}}.v_bar_jumps`",
                 # removed: the bars of a spike, from its 'out' jump up to the bar before its 'back' jump
                 "WHERE kind = 'spike' AND leg = 'out'",
                 "LEFT JOIN spike_bars AS s ON s.symbol = x.symbol AND x.date >= s.d0 AND x.date < s.d1",
                 "WHERE x.close > 0 AND s.symbol IS NULL",
                 # a new basis: every detour jump, and every jump of 3 times or more that nothing undid
                 "WHERE kind = 'detour' OR (leg = 'alone' AND factor >= 3)",
                 "LEFT JOIN breaks AS k ON k.symbol = d.symbol AND k.date <= d.date",
                 "SELECT d.symbol, d.date, COUNT(k.date) AS basis",
                 "SELECT b.*, s.basis"):
        assert frag in cl, frag
    assert "CURRENT_" not in cl and "LEAD(" not in cl and "ratio" not in cl    # the unrounded factor decides (PR 1107)
    tpl = by["v_bars_clean"].template
    assert "it removes recording errors, it never reads a" in tpl and "read it from views 1.37.0" in tpl
    # the challenger: inputs and results inside one basis, from the clean bars only
    fe = bv._code(by["v_challenger_v0_features"].template)
    rk = bv._code(by["v_challenger_v0_ranks"].template)
    assert "symbol_bars_daily" not in fe and "FROM `{{DS}}.v_bars_clean`" in fe
    assert fe.count("PARTITION BY x.symbol, x.basis ORDER BY x.date") == 1 and fe.count(", l.basis ORDER BY l.date ROWS") == 2
    assert "PARTITION BY l.symbol ORDER BY" not in fe and "PARTITION BY x.symbol ORDER BY" not in fe
    assert "  market, sector, symbol, date, basis, p, lr," in fe
    assert "WINDOW w AS (PARTITION BY symbol, basis ORDER BY date)" in rk and "PARTITION BY symbol ORDER BY date" not in rk
    assert bv.lookahead_problems(objs) == [] and bv.check_objects(objs) == []
    # the outcome engine and the forward ledger still read the raw archive (the next step, not this one)
    for name in ("v_outcome_engine", "v_forward_signals"):
        assert "symbol_bars_daily" in bv._code(by[name].template) and "v_bars_clean" not in bv._code(by[name].template)
    # the rule, on the series the SQL itself was run on
    assert _bars_clean(BAR_SERIES) == BARS_CLEAN_EXPECTED
    # each part of the rule decides a case above
    got = BARS_CLEAN_EXPECTED
    assert got["SPK"][1] == ["01-03"] and got["B3"][1] == ["01-03", "01-04", "01-05"] and got["B4"][1] == []  # 3 bars / 4
    assert got["AT3"][2] == "01111" and got["NEAR3"][2] == "00000"                                         # 3 / 2.9994
    assert got["DRIFT"][2] == "0" * 10 and got["THREE"][2] == "0" * 7                                      # under 3: whole
    assert got["B4"][2] == "0011112222"                                                    # both ends of a detour
    assert got["NEW3"][2] == "00011" and got["NEW"][2] == "00000"                       # too new: 3 times / 2 times


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
def test_v28(): case_v28()
def test_v29(): case_v29()
def test_v30(): case_v30()
def test_v31(): case_v31()
def test_v32(): case_v32()
def test_v33(): case_v33()
def test_v34(): case_v34()
def test_v35(): case_v35()
def test_v36(): case_v36()
def test_v37(): case_v37()
def test_v38(): case_v38()
def test_v39(): case_v39()
def test_v40(): case_v40()
def test_v41(): case_v41()
def test_v42(): case_v42()
def test_v43(): case_v43()
def test_v44(): case_v44()
def test_v45(): case_v45()
def test_v46(): case_v46()
def test_v47(): case_v47()
def test_v48(): case_v48()
def test_v49(): case_v49()
def test_v50(): case_v50()
def test_v51(): case_v51()
def test_v52(): case_v52()
def test_v53(): case_v53()
def test_v54(): case_v54()
def test_v55(): case_v55()
def test_v56(): case_v56()
def test_v57(): case_v57()
def test_v58(): case_v58()
def test_v59(): case_v59()
def test_v60(): case_v60()
def test_v61(): case_v61()
def test_v62(): case_v62()
def test_v63(): case_v63()
def test_v64(): case_v64()
def test_v65(): case_v65()
def test_v66(): case_v66()
def test_v67(): case_v67()
def test_v68(): case_v68()
def test_v69(): case_v69()
def test_v70(): case_v70()
def test_v71(): case_v71()


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
