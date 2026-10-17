#!/usr/bin/env python3
"""
tfb_workbook_dq_audit.py - offline DQ-01..DQ-75 measurement harness over a TFB
workbook export (WP-0 of the 2026-10-11 audit framework).

v1.0.3 (2026-10-14) - preserve unknown Top-10 KPI observations.
Both Entered cash (SAR) and legacy Deployable (SAR) strips are recognized by
unique Selected/Passed headers. Missing, ambiguous or malformed Selected
observations remain None. Only explicit exec seat text or the zero-withheld
contract proves execution counts; plain Selected counts do not. DQ-45 remains
INFO and cannot issue or change a decision-feed permission.
Row evidence references retain physical worksheet row numbers through blank
symbol filtering, rather than reporting a filtered body position as a row.

v1.0.2 (2026-10-14) - count structured settlement failures as well as warnings.
PDN.AX in the reviewed export records non_converged in Scoring Errors without
a warning receipt. Parse only the explicit scoring_settlement JSON record;
count its union with warning failures once in summary and row-level flags.

v1.0.1 (2026-10-11) [WP-2bc integration] - DQ-37 READS THE NEW RECEIPT
-------------------------------------------------------------------------
Engine 5.153.0 (WP-2b, gate TFB_SETTLE_UNRESOLVED) publishes a non-converged
row under enforce WITHOUT the f7_settle_failed:non_converged receipt: the row
keeps its pass-1 scores as WATCHLIST and carries settle_unresolved:enforce:...
instead (observe adds settle_unresolved:observe:... next to the holdback). v1.0.0
counted non-convergence only by the f7_settle_failed prefix, so arming enforce
would have dropped DQ-37's non_converged count to 0 while the 46 rows (10-10:
GM 45 + CFX 1) were still unresolved - a false PASS. The predicate is now
F7_NONCONV_PREFIXES = ("f7_settle_failed", "settle_unresolved:"), matched as a
token PREFIX, never as the substring "unresolved", which other tokens contain:
the engine's enforce-only margin receipt margin_publish:<field>:unresolved
(since v5.151.4; 0 on 10-10 because margin publication runs in observe) and the
engine's ghost-ticker tag name_unresolved (data_engine_v2 v5.93.0 investability
gate; 1 GM row on 10-10). The same predicate feeds the per-row
DQ-37 flag. An INVESTABLE row among the non-converged ones (0 on 10-10, 0 by
construction under the engine's gate tier) is listed FIRST in the examples, ahead
of the INVESTABLE rc-flip examples that otherwise fill all six slots on a real
export, so the operator sees it without a schema change (review fix). On the 2026-10-10 export every
count, status and value is unchanged (0 settle_unresolved tags exist), so the
committed baseline fixture stays valid; only the version token and the DQ-37
detail text change in the report. Selftest cases are unchanged (label-based
cases-digest c1bdd1a2cc1c3927); tests/test_workbook_dq_audit.py P13 pins the
prefix rule, the substring exclusion and the INVESTABLE example.

REVIEW FIX (2026-10-11, pre-merge amendment of the same v1.0.0) [WP-0]
-----------------------------------------------------------------------
compare_to_baseline: BASELINE_HIGHER_IS_BETTER emptied. The build spec listed
DQ-23 as an inverted measure on the assumption that its count is the share of
rows at +3 h (100 %); the delivered dq_23_stamp_offset returns count = rows
whose Riyadh - UTC offset is NOT +3 h (0 on 10-10), i.e. lower is better like
every other delivered count. The reviewer reproduced baseline FAIL 5 -> current
FAIL 10 labelled as improvements ['DQ-23'] (and 5 -> 2 as a regression). Exit
code and decision_critical_regressions were never affected (DQ-23 is not
decision-critical); the '## Trend vs baseline' section and report['baseline']
were. Fixture, digests 8804aa3208d8abe6 / a92e4b7c9a54807f (0 vs 0 on 10-10)
and the selftest cases-digest c1bdd1a2cc1c3927 (label-based) are unchanged;
tests/test_workbook_dq_audit.py P6 now pins the direction (5 -> 10 regresses,
5 -> 2 improves).

WHY (v1.0.0, 2026-10-11) [WP-0]
-------------------------------
The audit framework's check catalogue (section 2.5, 75 DQ-ids) was measured by
hand on the 2026-10-11 export; no script reproduced those numbers, no tab or row
record existed (sections 2.2 / 2.3), no DQ score per field class (section 2.4),
no baseline comparison, and no workflow step produced a daily scorecard. Every
later work package must print its before/after counts from one harness, so the
harness is built first.

Evidence this build was measured against (export content date 2026-10-10,
sha256 f5d327afc9fb409c..., 22,487,265 bytes, 50 tabs / 10 visible, 9,791 live
rows 255 / 6,609 / 453 / 2,474, run 38071484709 stamped in _Status):
  275 rows without a price (0 / 211 / 10 / 54; GM 134 acquisition_status:
  unavailable + 74 identity_quarantined + 3 untagged) - 5 cross-page duplicate
  symbols (COPX.US, LIT.US, REMX.US, URA.US, XOP.US) - 14 bare-vs-.US pairs
  (GM FCX, OTIS, YUM; MF 11) - 153 GM rows with Expected ROI 12M > 0.35, all
  provider_target - 2,618 F-7 recommendation flips (45 INVESTABLE) - 21,670
  duplicate Signal_History symbol-days - Portfolio_Decision 4/4 BLOCK on 3
  consecutive _Portfolio_Action_Log runs - 11 orphan tabs, 1 ghost sheet
  (My_Investments) - 8 mis-resolved crypto identities + 2 ALTSYMBOL rows.

DESIGN
  * READ-ONLY and standalone: openpyxl + stdlib; the sibling
    scripts/tfb_export_audit.py v1.1.1 is imported LAZILY (inside _tea()) for
    its shared helpers (_num, _s, _parse_dt, Book, CashSnapshot ...). Importing
    this module performs no I/O, imports no openpyxl and never calls sys.exit.
    It never opens the live Sheet, never writes a tab, never calls a provider.
  * One function per DQ-id (dq_01 .. dq_75) returning one Check record:
    {id, name, group, cls, critical, status, count, values, examples, detail,
    rule, fix_ref}. Status PASS | WARN | FAIL | INFO | NOT_CHECKED. An input
    absent from the export gives NOT_CHECKED, never PASS. All thresholds live
    in the CONFIG block below (no magic numbers in the check bodies).
  * Reference clocks are export-derived, never the wall clock: ref_time_rows =
    max Last Updated (UTC) over the four market pages; ref_time_log = last
    parseable _Run_Log Timestamp. The report digest is therefore identical on
    every run over the same file (harness x3).
  * Decision-critical class = DECISION_CRITICAL_IDS. Exit code 1 only when
    (no --baseline and a decision-critical check is FAIL) or (--baseline and a
    decision-critical regression exists). Everything else is WARN / non-fatal.
  * REDACTION (public repository, hard rule): the report, the markdown
    scorecard, the fixture and every example string carry counts, symbols,
    dates, labels and booleans only - never a balance, NAV, quantity, cost,
    ticket or P&L amount. Panel / KPI / ledger amounts are read for computation
    and reduced to booleans or buckets. _assert_redacted() is a
    whitelist-by-construction walk (numeric leaves only under permitted
    container keys or counted-suffix keys; money-string regexes on every
    string) and runs before every --json / --md write and in --selftest.
  * Count semantics of the decision-critical ids (compare_to_baseline depends
    on them): DQ-07 differing header positions; DQ-08 sum of in-page dups;
    DQ-09 cross-page duplicate symbols; DQ-10 bare-vs-.US pairs; DQ-16
    |misclassed crypto u name-mismatch| + ALTSYMBOL rows; DQ-22 stale bars NOT
    BLOCKED; DQ-24 margin cells with |v| > 1.5 over the three margin columns
    on all four pages; DQ-25 D/E > 10 rows without the exact tag
    fund_unit_contract:yahoo:debt_to_equity; DQ-30 rows at Data Quality Score
    100 with a garbage-magnitude fundamental; DQ-33 rows with ROI 12M > 0.35;
    DQ-34 rows in (INVESTABLE u BOARD u HOLDINGS) carrying a
    sheet_tuple_conflict tag; DQ-38 INVEST-on-SELL-class + INVESTABLE without
    price/forecast; DQ-46 trailing consecutive all-BLOCK action-log runs; DQ-53
    holdings with a blank stop / tp1 / tp2 bracket; DQ-62 Target ROI %
    records with |v| > 1.5.

USAGE
  python scripts/tfb_workbook_dq_audit.py export.xlsx [--json dq_report.json]
        [--md dq_scorecard.md] [--baseline tests/fixtures/dq_baseline_2026-10-10.json]
        [--baseline-out new_fixture.json] [--expect Market_Leaders=255,...]
        [--asof YYYY-MM-DD] [--now ISO-UTC] [--tz-offset 3] [--rows]
        [--examples-private] [--quiet]
  python scripts/tfb_workbook_dq_audit.py --selftest

Exit code: 0 ok; 1 decision-critical FAIL (no baseline) or decision-critical
regression (with baseline); 2 usage / load error.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import hashlib
import json
import os
import re
import statistics
import sys
import tempfile
import time

SCRIPT_VERSION = "1.0.3"   # DQ-45 recognizes both KPI titles and preserves unknown.

# --------------------------------------------------------------------------- #
# CONFIG - every threshold, vocabulary and policy constant lives here
# --------------------------------------------------------------------------- #
MARKET_PAGES = ["Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds"]
# v1.0.1: tag PREFIXES that mark a non-converged F-7 row (engine 5.153.0 publishes the
# second one under TFB_SETTLE_UNRESOLVED). Never match the bare substring "unresolved".
F7_NONCONV_PREFIXES = ("f7_settle_failed", "settle_unresolved:")
PAGE_SHORT = {"Market_Leaders": "ML", "Global_Markets": "GM", "Commodities_FX": "CFX", "Mutual_Funds": "MF",
              "My_Portfolio": "MP"}
PORTFOLIO_PAGE = "My_Portfolio"
INVESTABLE_VIEW = "Investable Symbol"
EXPECT = {"Market_Leaders": 255, "Global_Markets": 6609, "Commodities_FX": 453, "Mutual_Funds": 2474}

THRESHOLDS = {
    "padding_rows_max": 50,              # DQ-02
    "grid_warn_pct": 60.0, "grid_fail_pct": 75.0,   # DQ-03
    "asset_class_max_distinct": 9,       # DQ-11
    "no_price_share": 0.01,              # DQ-15
    "day_range_tolerance": 0.0,          # DQ-18
    "w52_position_pts": 2.0,             # DQ-19
    "w52_sentinel": 999999.9999,         # DQ-19
    "pct_change_tolerance": 0.005,       # DQ-20 (0.5 pp as a fraction)
    "freshness_hours": 24.0,             # DQ-21
    "riyadh_offset_s": 3 * 3600, "riyadh_tolerance_s": 60,   # DQ-23
    "margin_abs_max": 1.5,               # DQ-24
    "de_max": 10.0, "de_garbage": 50.0,  # DQ-25 / DQ-30
    "yield_max": 0.25,                   # DQ-26
    "beta_abs_max": 5.0, "beta_garbage": 10.0,            # DQ-27 / DQ-30
    "growth_abs_max": 1.5, "growth_garbage": 20.0,        # DQ-27 / DQ-30
    "margin_garbage": 5.0,               # DQ-30
    "rsi_default": 50.0,                 # DQ-29
    "roi_identity_tolerance": 0.01,      # DQ-31 (1 pp)
    "roi12_soft_cap": 0.35,              # DQ-33
    "target_vintage_sessions": None,     # DQ-35 (operator sets N; None -> WARN)
    "f7_delta_warn": 5.0, "f7_delta_fail": 10.0,          # DQ-37 tiers
    "eligible_dq": 80.0, "eligible_frs": 70.0,            # DQ-38 INFO sub-metric
    "score_mode_share": 0.25,            # DQ-39
    "fallback_share_investable": 0.20,   # DQ-40
    "warnings_mean_len": 200.0, "warnings_mean_tags": 5.0,   # DQ-43
    "reason_mean_len": 120.0,            # DQ-44
    "block_runs_fail": 2, "block_runs_warn": 1,           # DQ-46
    "unknown_share": 0.20,               # DQ-48
    "calendar_fail": 0.80, "calendar_warn": 0.95,         # DQ-49
    "cash_max_age_hours": 24.0,          # DQ-57
    "nav_tolerance": 1.0,                # DQ-60 (reported as a boolean only)
    "perf_unit_abs": 1.5,                # DQ-62
    "regime_unknown_share": 0.10,        # DQ-69
    "runlog_unparseable_share": 0.01, "runlog_http_coverage": 0.95,   # DQ-70
    "s1_excluded_share": 0.10,           # DQ-73
    "err_429_warn": 10, "err_502_fail": 10,               # DQ-74
    "leg_budget_warn_share": 0.90,       # DQ-75
    "pit_min_width": 16,                 # DQ-66 junk rule
}
POLICY = {"max_per_sector": 3.0, "pf_max_sector_pct": 40.0, "pf_max_position_pct": 10.0}
PANEL_LABELS = {"T10: Max Per Sector": ("max_per_sector", "Top_10_Investments"),
                "PF: Max Sector %": ("pf_max_sector_pct", "Portfolio_Decision"),
                "PF: Max Position %": ("pf_max_position_pct", "Portfolio_Decision")}
PANEL_INFO_LABELS = ["T10: Period (Months)", "T10: Require Investable"]
LISTS_DEFAULT_KEYS = ["T10_MAX_PER_SECTOR", "PF_MAX_SECTOR_PCT", "PF_MAX_POSITION_PCT",
                      "T10_PERIOD_MONTHS", "T10_REQUIRE_INVESTABLE"]
SYNC_BUDGET_MS = 3_600_000
FX_BAND = (3.74, 3.77)
FX_TO_SAR = {"SAR": 1.0, "USD": 3.75}
PROGRAM_V2_START = "2026-09-27"
PLACEHOLDER_DATES = {"2026-01-01"}
SELL_CLASS = {"SELL", "STRONG_SELL", "REDUCE", "AVOID"}
GICS_11 = {"Energy", "Materials", "Industrials", "Consumer Discretionary", "Consumer Staples", "Health Care",
           "Financials", "Information Technology", "Communication Services", "Utilities", "Real Estate"}
SECTOR_ALLOWED_EXTRA = {"Unknown"}
YAHOO_ONLY_SECTORS = {"Basic Materials", "Financial Services", "Healthcare", "Technology", "Consumer Cyclical",
                      "Consumer Defensive"}
EXCHANGE_CANONICAL = "NYSE/NASDAQ"
EXCHANGE_MINORITY = "NASDAQ/NYSE"
EXCHANGE_PLACEHOLDER = "YHD"
CRYPTO_CLASS = "CRYPTOCURRENCY"
CRYPTO_EXCHANGE = "CCC"
ALTSYMBOL_CLASS = "ALTSYMBOL"
SECTION_ARTIFACTS = {"FORECAST", "INVESTABILITY", "COUNT", "NAME"}
MARGIN_COLS = ["Operating Margin", "Gross Margin", "Profit Margin"]
HORIZONS = {"1M": ("Forecast Price 1M", "Expected ROI 1M"), "3M": ("Forecast Price 3M", "Expected ROI 3M"),
            "12M": ("Forecast Price 12M", "Expected ROI 12M")}
DE_UNIT_TAG = "fund_unit_contract:yahoo:debt_to_equity"
DECISION_CRITICAL_IDS = {"DQ-07", "DQ-08", "DQ-09", "DQ-10", "DQ-16", "DQ-22", "DQ-24", "DQ-25", "DQ-30",
                         "DQ-33", "DQ-34", "DQ-38", "DQ-46", "DQ-53", "DQ-62"}
# baseline comparison: ids where a higher count is NOT a regression
BASELINE_HIGHER_IS_BETTER = set()   # every delivered count is lower-is-better (DQ-23 count = bad rows); DQ-01/04/07/64 are excluded instead
BASELINE_COUNT_EXCLUDED = {"DQ-01", "DQ-04", "DQ-07", "DQ-64"}   # direction-free or inverted measures
STATUS_RANK = {"PASS": 0, "INFO": 1, "NOT_CHECKED": 1, "WARN": 2, "FAIL": 3}
PRIVATE_EXAMPLE_IDS = {"DQ-17", "DQ-51", "DQ-53", "DQ-54", "DQ-55", "DQ-56", "DQ-57", "DQ-58", "DQ-59", "DQ-60"}

# Section 2.4 weights (C, A, S, T) and criticality per field class
CLASS_WEIGHTS = {"identity": (0.30, 0.20, 0.40, 0.10), "quote": (0.30, 0.25, 0.25, 0.20),
                 "forecast": (0.25, 0.20, 0.40, 0.15), "fundamentals": (0.25, 0.40, 0.25, 0.10),
                 "scores": (0.20, 0.20, 0.50, 0.10), "provenance": (0.40, 0.10, 0.30, 0.20)}
CLASS_CRITICAL = {"identity", "quote", "scores"}        # forecast: critical for the cohort only
CLASS_COLUMNS = {
    "identity": ["Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector"],
    "quote": ["Current Price", "Previous Close", "Open", "Day High", "Day Low", "52W High", "52W Low", "Volume",
              "Percent Change"],
    "forecast": ["Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M", "Expected ROI 1M",
                 "Expected ROI 3M", "Expected ROI 12M", "Horizon Days", "Invest Period Label", "Forecast Source"],
    "fundamentals": ["Market Cap", "P/E (TTM)", "EPS (TTM)", "Gross Margin", "Operating Margin", "Profit Margin",
                     "Debt/Equity", "Revenue Growth YoY", "Free Cash Flow (TTM)", "Dividend Yield", "Beta (5Y)"],
    "scores": ["Overall Score", "Risk Score", "Valuation Score", "Momentum Score", "Opportunity Score",
               "Forecast Reliability Score", "Data Quality Score", "Recommendation", "Investability Status",
               "Final Action"],
    "provenance": ["Data Provider", "Last Updated (UTC)", "Warnings", "Scoring Errors", "Row Source"],
}
SCORE_RANGE_COLS = ["Overall Score", "Risk Score", "Valuation Score", "Momentum Score", "Opportunity Score",
                    "Forecast Reliability Score", "Data Quality Score"]

# Redaction (public repository). Numeric leaves are permitted ONLY inside a container key listed in
# REDACT_NUMERIC_CONTAINERS or under a key with a counted suffix / exactly 'count'.
REDACT_NUMERIC_CONTAINERS = {"values", "per_page", "by_field", "by_horizon", "distribution", "dq_scores", "expect"}
REDACT_NUMERIC_KEY_RE = re.compile(
    r"(_count|_rows|_n|_pct|_share|_mean|_max|_min|_total|_bytes|_ms|_hours|_days|_distinct|_blank|_tagged|"
    r"_untagged|_runs|_pairs|_errors)$|^count$")
MONEY_STRING_RE = re.compile(r"(?i)\d[\d,]*(\.\d+)?\s*(SAR|USD)\b")
MONEY_WORD_RE = re.compile(r"(?i)\b(NAV|cash|balance|deployable|ticket|equity|P&L)\b\D{0,15}\d{4,}")
EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
DIGEST_EXCLUDE = ("generated_at", "digest", "export_path", "runtime_ms")

# Verbatim header contracts from the 2026-10-10 export (115 and 122 non-empty cells)
MARKET_HEADER_115 = (
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry", "Current Price",
    "Previous Close", "Open", "Day High", "Day Low", "52W High", "52W Low", "Price Change", "Percent Change",
    "52W Position %", "Volume", "Avg Volume 10D", "Avg Volume 30D", "Market Cap", "Float Shares", "Beta (5Y)",
    "P/E (TTM)", "P/E (Forward)", "EPS (TTM)", "Dividend Yield", "Payout Ratio", "Revenue (TTM)",
    "Revenue Growth YoY", "Gross Margin", "Operating Margin", "Profit Margin", "Debt/Equity",
    "Free Cash Flow (TTM)", "RSI (14)", "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y", "VaR 95% (1D)",
    "Sharpe (1Y)", "Risk Score", "Risk Bucket", "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value", "Upside %",
    "Valuation Score", "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M", "Expected ROI 1M",
    "Expected ROI 3M", "Expected ROI 12M", "Forecast Confidence", "Confidence Score", "Confidence Bucket",
    "Value Score", "Quality Score", "Momentum Score", "Growth Score", "Overall Score", "Opportunity Score",
    "Rank (Overall)", "Analyst Rating", "Target Price", "Upside/Downside %", "Signal", "Trend 1M", "Trend 3M",
    "Trend 12M", "ST Signal", "Recommendation", "Recommendation Detail", "Recommendation Reason", "Reco Priority",
    "Priority Band", "Recommendation Source", "Horizon Days", "Invest Period Label", "Sector-Adj Score",
    "Conviction Score", "Top Factors", "Top Risks", "Position Size Hint", "Candle Pattern", "Candle Signal",
    "Candle Strength", "Candle Confidence", "Recent Patterns (5D)", "Provider Rating", "Scoring Reco Source",
    "Scoring Schema Version", "Scoring Errors", "Opportunity Source", "Overall Score (Raw)",
    "Overall Penalty Factor", "Forecast Source", "Data Quality Score", "Forecast Reliability Score",
    "Provider/Engine Conflict", "Conflict Type", "Final Decision Basis", "Investability Status", "Final Action",
    "Block Reason", "Data Provider", "Provider Secondary", "Last Updated (UTC)", "Last Updated (Riyadh)",
    "Row Source", "Warnings")
PORTFOLIO_HEADER_122 = (
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry", "Current Price",
    "Previous Close", "Open", "Day High", "Day Low", "52W High", "52W Low", "Price Change", "Percent Change",
    "52W Position %", "Volume", "Avg Volume 10D", "Avg Volume 30D", "Market Cap", "Float Shares", "Beta (5Y)",
    "P/E (TTM)", "P/E (Forward)", "EPS (TTM)", "Dividend Yield", "Payout Ratio", "Revenue (TTM)",
    "Revenue Growth YoY", "Gross Margin", "Operating Margin", "Profit Margin", "Debt/Equity",
    "Free Cash Flow (TTM)", "RSI (14)", "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y", "VaR 95% (1D)",
    "Sharpe (1Y)", "Risk Score", "Risk Bucket", "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value", "Upside %",
    "Valuation Score", "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M", "Expected ROI 1M",
    "Expected ROI 3M", "Expected ROI 12M", "Forecast Confidence", "Confidence Score", "Confidence Bucket",
    "Value Score", "Quality Score", "Momentum Score", "Growth Score", "Overall Score", "Opportunity Score",
    "Rank (Overall)", "Fundamental View", "Technical View", "Risk View", "Value View", "Recommendation",
    "Recommendation Reason", "Horizon Days", "Invest Period Label", "Position Qty", "Avg Cost", "Position Cost",
    "Position Value", "Unrealized P/L", "Unrealized P/L %", "Data Provider", "Last Updated (UTC)",
    "Last Updated (Riyadh)", "Warnings", "Sector-Adj Score", "Conviction Score", "Top Factors", "Top Risks",
    "Position Size Hint", "Recommendation Detail", "Reco Priority", "Provider Rating", "Recommendation Source",
    "Priority Band", "Scoring Reco Source", "Scoring Schema Version", "Scoring Errors", "Opportunity Source",
    "Overall Score (Raw)", "Overall Penalty Factor", "Candle Pattern", "Candle Signal", "Candle Strength",
    "Candle Confidence", "Recent Patterns (5D)", "Forecast Source", "Data Quality Score",
    "Forecast Reliability Score", "Provider/Engine Conflict", "Conflict Type", "Final Decision Basis",
    "Investability Status", "Final Action", "Block Reason", "Buy Date", "Target Weight %", "Actual Weight %",
    "Weight Gap", "Rebalance Action", "Investor Decision", "User Notes")
HEADER_CONTRACT = {p: MARKET_HEADER_115 for p in MARKET_PAGES}
HEADER_CONTRACT[PORTFOLIO_PAGE] = PORTFOLIO_HEADER_122
HEADER_CONTRACT[INVESTABLE_VIEW] = MARKET_HEADER_115

# Section 2.2 tab registry: owner_lane, writer, purpose, cadence, retention, business_criticality, in_registry
KNOWN_TABS = {
    "Market_Leaders": ("GitHub/Python", "run_dashboard_sync", "market page (115-col contract)", "daily", "overwrite", "critical", True),
    "Global_Markets": ("GitHub/Python", "run_dashboard_sync", "market page (115-col contract)", "daily", "overwrite", "critical", True),
    "Commodities_FX": ("GitHub/Python", "run_dashboard_sync", "market page (115-col contract)", "daily", "overwrite", "critical", True),
    "Mutual_Funds": ("GitHub/Python", "run_dashboard_sync", "market page (115-col contract)", "daily", "overwrite", "critical", True),
    "My_Portfolio": ("GitHub/Python", "run_dashboard_sync", "holdings page (122-col contract)", "daily", "overwrite", "critical", True),
    "Investable Symbol": ("operator", "sheet formula (operator view)", "INVEST view of the four pages", "live", "n/a", "high", False),
    "Top_10_Investments": ("GAS", "16_Decision_Top10.gs", "cockpit / decision feed", "per run", "overwrite", "critical", True),
    "Portfolio_Decision": ("GAS", "10_My_Portfolio.gs + portfolio_actions", "portfolio actions", "per run", "overwrite", "critical", True),
    "_Portfolio_CostBasis": ("GAS", "21_Portfolio_Ledger.gs", "investment ledger", "operator", "append", "critical", True),
    "_Cash_Snapshot": ("operator", "typed by operator", "settled-cash evidence", "operator", "append", "critical", True),
    "_Dividend_Log": ("GAS", "21_Portfolio_Ledger.gs", "dividend log", "operator", "append", "high", True),
    "_Trade_Notes": ("GAS", "25_Trade_Notes.gs", "trade notes (Program v2)", "operator", "append", "high", True),
    "_Trade_Notes_Input": ("GAS", "25_Trade_Notes.gs", "trade-note input form", "operator", "overwrite", "low", True),
    "_Portfolio_Action_Log": ("GAS", "10_My_Portfolio.gs", "portfolio action evidence", "per run", "append", "medium", True),
    "_Selection_Log": ("GAS", "16_Decision_Top10.gs", "board selection evidence", "per run", "append", "medium", True),
    "Calendar_Events": ("GitHub/Python", "run_calendar_sync", "earnings / ex-div calendar", "daily", "overwrite", "high", True),
    "News_Archive": ("GitHub/Python", "news_archive_collector", "news archive", "daily", "append", "medium", True),
    "Insights_Analysis": ("GitHub/Python", "run_dashboard_sync (insights_builder)", "insights", "daily", "overwrite", "medium", True),
    "Data_Dictionary": ("GitHub/Python", "run_dashboard_sync", "header dictionary", "daily", "overwrite", "medium", True),
    "Dashboard_Audit": ("GitHub/Python", "run_dashboard_sync (validator)", "in-workbook validator", "daily", "overwrite", "medium", True),
    "_Status": ("GitHub/Python + GAS", "run_dashboard_sync / refresh chain", "sync status + globals", "per run", "overwrite", "high", True),
    "_Run_Log": ("GitHub/Python + GAS", "all lanes", "run log", "per run", "append", "medium", True),
    "_Sync_Control": ("GitHub/Python", "run_dashboard_sync", "sync hold", "per run", "overwrite", "high", True),
    "_Brief_State": ("GitHub/Python", "run_weekly_brief", "weekly brief state", "weekly", "overwrite", "low", True),
    "_Lists_Config": ("operator", "operator (GAS seeds)", "reference lists, FX table, panel defaults", "operator", "overwrite", "high", True),
    "Signal_History": ("GitHub/Python", "track_performance", "signal snapshots", "per run", "append", "medium", True),
    "Signal_Trends": ("GitHub/Python", "track_performance", "signal trends", "per run", "overwrite", "medium", True),
    "Performance_Log": ("GitHub/Python", "track_performance", "outcome tracker", "per run", "append", "high", True),
    "Hypothesis_Registry": ("GitHub/Python", "track_performance (backtest)", "hypothesis registry", "weekly", "overwrite", "medium", True),
    "_PIT_Fundamentals": ("GitHub/Python", "pit_snapshot", "point-in-time fundamentals", "daily", "append", "medium", True),
    "Regime_History": ("GitHub/Python", "run_shadow_board", "regime readings", "per run", "append", "medium", True),
    "Regret_Ledger": ("GitHub/Python", "run_shadow_scorer", "regret forks", "per run", "append", "medium", True),
    "Regret_Summary": ("GitHub/Python", "run_shadow_scorer", "regret summary", "per run", "overwrite", "low", True),
    "S1_Gate": ("GitHub/Python", "run_shadow_scorer", "S-1 gate verdict", "per run", "overwrite", "high", True),
    "_S1_Calibration": ("GitHub/Python", "track_performance", "calibration state", "per run", "append", "medium", True),
    "_Forecast_Scorecard": ("GitHub/Python", "track_performance (audit lane)", "forecast scorecard: cohort hit-rates", "per audited run", "overwrite", "medium", True),
    "Shadow_Board": ("GitHub/Python", "run_shadow_board", "shadow board", "per run", "overwrite", "medium", True),
    "Shadow_History": ("GitHub/Python", "run_shadow_board", "shadow baskets", "per run", "append", "medium", True),
    "_Corporate_Actions": ("GitHub/Python", "pit_snapshot / corporate_actions", "corporate actions", "daily", "append", "medium", True),
    "_Shariah_Authority": ("GitHub/Python", "refresh_shariah_authority (retired doctrine; read by run_shadow_board)", "authority list - inventoried only", "quarterly", "overwrite", "low", True),
}
# per-tab stamp column (max parseable value = last_written); None -> header/status line regex
TAB_STAMP_COLUMN = {
    "Market_Leaders": "Last Updated (UTC)", "Global_Markets": "Last Updated (UTC)", "Commodities_FX": "Last Updated (UTC)",
    "Mutual_Funds": "Last Updated (UTC)", "My_Portfolio": "Last Updated (UTC)", "Investable Symbol": "Last Updated (UTC)",
    "_Portfolio_Action_Log": "Logged At", "_Selection_Log": "Logged At", "_Dividend_Log": "Logged At",
    "_Trade_Notes": "Logged At (Riyadh)", "_Cash_Snapshot": "Date", "_Run_Log": "Timestamp", "_Status": "Last Updated",
    "Signal_History": "Recorded At (Riyadh)", "Signal_Trends": "Recorded At (Riyadh)", "Calendar_Events": "Updated At (Riyadh)",
    "Regime_History": "Captured At (UTC)", "Regret_Ledger": "Date", "_PIT_Fundamentals": "Captured At (UTC)",
    "_S1_Calibration": "As Of (Riyadh)", "Shadow_History": "Date", "_Shariah_Authority": "Fetched At (UTC)",
    "_Forecast_Scorecard": "As Of (Riyadh)",
    "_Corporate_Actions": "Detected At", "Hypothesis_Registry": "Last Backtest (UTC)",
}
TAB_SYMBOL_KEYED = set(MARKET_PAGES) | {PORTFOLIO_PAGE, INVESTABLE_VIEW, "Calendar_Events", "Signal_History",
                                        "Signal_Trends", "_PIT_Fundamentals", "_Shariah_Authority", "Regret_Ledger"}
VERSION_STAMP_RE = re.compile(r"v(\d+\.\d+\.\d+)")
GRID_CAPACITY_RE = re.compile(r"allocated=([\d,]+) \(([\d.]+)%\)")
STATUS_STAMP_RE = re.compile(r"\[STATUS-STAMP v([\d.]+)\]")
NAV_TEXT_RE = re.compile(r"NAV ([\d,]+) SAR")
SELECTED_EXEC_RE = re.compile(
    r"(?P<count>\d+)\s+exec"
    r"(?:\s*\+\s*\d+\s+(?:fast-track suspended|research|pend(?:ing)?|grace))*"
    r"(?:\s*/\s*(?P<limit>\d+))?", re.I)
S1_CRITERION1_RE = re.compile(r"(\d+)/(\d+) scored days · (\d+) excluded-infra")
RUNLOG_TOKENS = {"rows402_new": re.compile(r"rows402 new=(\d+)"), "f429": re.compile(r"\bf429=(\d+)"),
                 "f404": re.compile(r"\bf404=(\d+)"), "fb429": re.compile(r"fb=429:(\d+)"),
                 "quote_http_429": re.compile(r"quote_http_429\"?:\s*(\d+)")}
RUNLOG_PLAN_RE = re.compile(r"used=\d+/(\d+)")
RUNLOG_DETAILS_HTTP_RE = re.compile(r"\"httpCode\":\s*(402|429|502)\b")
RUNLOG_HTTP_CODES = {"402", "429", "502"}
SEVERITY_FAMILIES = {"WARN": "warning", "WARNING": "warning", "ERR": "error", "ERROR": "error",
                     "INFO": "info", "DEBUG": "debug", "CRITICAL": "critical", "FATAL": "critical"}
F7_RC_RE = re.compile(r":rc=([a-z_]+)>([a-z_]+)")
F7_OV_RE = re.compile(r"ov=([\d.\-]+)>([\d.\-]+)")
LKG_HOURS_RE = re.compile(r"^analyst_lkg:(\d+)h")
MARGIN_PUBLISH_PREFIX = "margin_publish:"
GLYPHS = "▲▼"
PIT_ROI_COLS = ["Expected ROI 12M %", "Expected ROI 1M %", "Expected ROI 3M %"]
PIT_LEGACY_WINDOW = ("2026-08-06", "2026-08-10")


class RedactionError(ValueError):
    """A report value would leak a portfolio figure into the public repository."""


# --------------------------------------------------------------------------- #
# Lazy import of the sibling audit (its import guard calls sys.exit(2) without openpyxl)
# --------------------------------------------------------------------------- #
_TEA = None
_DQBOOK = None


def _tea():
    """Return scripts.tfb_export_audit, imported on first use (never at module import)."""
    global _TEA
    if _TEA is not None:
        return _TEA
    try:
        try:
            from scripts import tfb_export_audit as tea      # repo root on sys.path (tests/conftest.py)
        except ImportError:
            here = os.path.dirname(os.path.abspath(__file__))
            if here not in sys.path:
                sys.path.insert(0, here)
            import tfb_export_audit as tea                   # standalone: scripts/ as cwd or direct path
    except (ImportError, SystemExit) as exc:                 # the guard raises SystemExit(2) without openpyxl
        raise ImportError("scripts/tfb_export_audit.py (and openpyxl) are required: pip install openpyxl") from exc
    _TEA = tea
    return tea


def _dqbook_class():
    """DQBook = tea.Book + grid_rows captured BEFORE the trailing-empty trim (ws.max_row is None in read_only)."""
    global _DQBOOK
    if _DQBOOK is not None:
        return _DQBOOK
    tea = _tea()
    import openpyxl
    from openpyxl.styles.numbers import is_datetime as excel_datetime_kind

    class DQBook(tea.Book):
        def __init__(self, path):                                   # noqa: D107 - mirrors Book.__init__ verbatim
            self.path = path
            self.sheets = {}
            self.state = {}
            self.grid_rows = {}
            self.cash_date_only_cells = set()
            wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
            for ws in wb.worksheets:
                if ws.title == "_Cash_Snapshot":
                    rows = []
                    for ri, cells in enumerate(ws.iter_rows()):
                        rows.append(tuple(cell.value for cell in cells))
                        self.cash_date_only_cells.update(
                            (ri, ci) for ci, cell in enumerate(cells)
                            if isinstance(cell.value, dt.datetime)
                            and excel_datetime_kind(cell.number_format or "") == "date"
                        )
                else:
                    rows = [tuple(r) for r in ws.iter_rows(values_only=True)]
                self.grid_rows[ws.title] = len(rows)
                while rows and all(v is None or v == "" for v in rows[-1]):
                    rows.pop()
                width = max((len(r) for r in rows), default=0)
                rows = [r + (None,) * (width - len(r)) if len(r) < width else r for r in rows]
                self.sheets[ws.title] = rows
                self.state[ws.title] = ws.sheet_state
            wb.close()

    _DQBOOK = DQBook
    return DQBook


def load_book(path):
    return _dqbook_class()(path)


# --------------------------------------------------------------------------- #
# Small helpers (thin wrappers so the check bodies read like the catalogue)
# --------------------------------------------------------------------------- #
def _s(v):
    return "" if v is None else str(v).strip()


def _num(v):
    return _tea()._num(v)


def _blank(v):
    return v is None or v == ""


def _parse_dt(v, tz_offset=3):
    return _tea()._parse_dt(v, tz_offset)


def _local_date(v):
    """Calendar date of a cell as written (never tz-converted): ledger / dividend / cash dates."""
    if isinstance(v, dt.datetime):
        return v.date().isoformat()
    if isinstance(v, dt.date):
        return v.isoformat()
    m = re.match(r"(\d{4}-\d{2}-\d{2})", _s(v))
    return m.group(1) if m else None


def _tags(cell):
    """Warnings cell -> list of tags. Split on ';' only (no '|' inside any Warnings cell on 10-10)."""
    w = _s(cell)
    return [t.strip() for t in w.split(";") if t.strip()] if w else []


def _share(n, d, nd=4):
    return round(n / d, nd) if d else 0.0


def _pct(n, d, nd=2):
    return round(100.0 * n / d, nd) if d else 0.0


def _sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _jsonable(o):
    return _tea()._jsonable(o)


def _header_index(rows, hi=0):
    ix = {}
    for i, h in enumerate(rows[hi]):
        h = _s(h)
        if h and h not in ix:
            ix[h] = i
    return ix


def _find_row(rows, pred, start=0):
    for i in range(start, len(rows)):
        if pred(rows[i]):
            return i
    return None


def _status_rank(status):
    return STATUS_RANK.get(status, 1)


def check(cid, name, group, cls, status, count=None, values=None, examples=None, detail="", rule="",
          fix_ref="", critical=None):
    """The one Check record shape (section 2.5 row + per-page values)."""
    if status == "PASS" and count is None:
        count = 0
    return {"id": cid, "name": name, "group": group, "cls": cls,
            "critical": (cid in DECISION_CRITICAL_IDS) if critical is None else bool(critical),
            "status": status, "count": count, "values": dict(values or {}),
            "examples": [str(e) for e in list(examples or [])[:8]], "detail": detail, "rule": rule,
            "fix_ref": fix_ref}


def not_checked(cid, name, group, cls, detail, rule="", fix_ref=""):
    return check(cid, name, group, cls, "NOT_CHECKED", None, {}, [], detail, rule, fix_ref)


# --------------------------------------------------------------------------- #
# Context: everything the 75 checks share, computed once per export
# --------------------------------------------------------------------------- #
class Page:
    """Header index + live body (non-blank Symbol) + pre-split Warnings tags for one symbol page."""

    def __init__(self, name, rows, grid_rows):
        self.name = name
        self.grid_rows = grid_rows
        self.header = [_s(h) for h in rows[0]] if rows else []
        self.ix = _header_index(rows) if rows else {}
        indexed = [(ri, r) for ri, r in enumerate(rows[1:], 2)
                   if _s(r[self.ix["Symbol"]])] if "Symbol" in self.ix else []
        self.body = [r for _, r in indexed]
        self.row_numbers = [ri for ri, _ in indexed]
        wi = self.ix.get("Warnings")
        self.tags = [_tags(r[wi]) if wi is not None else [] for r in self.body]
        self.syms = [_s(r[self.ix["Symbol"]]) for r in self.body] if self.body else []
        self.n = len(self.body)

    def col(self, header):
        return self.ix.get(header)

    def val(self, r, header):
        i = self.ix.get(header)
        return None if i is None or i >= len(r) else r[i]

    def num(self, r, header):
        return _num(self.val(r, header))

    def s(self, r, header):
        return _s(self.val(r, header))


def _board_and_qualified(rows):
    """Top_10 ranked board (rows after 'Rank|Symbol' until blank Symbol) and the ALL QUALIFIED table."""
    board, qualified = [], []
    bi = _find_row(rows, lambda r: _s(r[0]) == "Rank" and len(r) > 1 and _s(r[1]) == "Symbol")
    if bi is not None:
        for r in rows[bi + 1:]:
            if len(r) < 2 or not _s(r[1]):
                break
            board.append(_s(r[1]))
    qi = _find_row(rows, lambda r: _s(r[0]).startswith("ALL QUALIFIED"))
    if qi is not None:
        hi = _find_row(rows, lambda r: _s(r[0]) == "Rank" and len(r) > 1 and _s(r[1]) == "Symbol", qi + 1)
        if hi is not None:
            for r in rows[hi + 1:]:
                if len(r) < 2 or not _s(r[1]) or _num(r[0]) is None:
                    break
                qualified.append(_s(r[1]))
    return board, qualified


def _candidate_grid(rows):
    """Correction #3: header = row after the 'CANDIDATES' cell; grid stops at the first row whose Verdict is
    blank OR whose Symbol contains '|' (the cockpit footer 'route v.. | builder v.. | pool=..')."""
    ci = _find_row(rows, lambda r: _s(r[0]).startswith("CANDIDATES"))
    if ci is None or ci + 1 >= len(rows):
        return None, [], None
    hdr = [_s(h) for h in rows[ci + 1]]
    cix = {}
    for i, h in enumerate(hdr):
        if h and h not in cix:
            cix[h] = i
    if "Symbol" not in cix or "Verdict" not in cix:
        return None, [], None
    grid, footer = [], None
    for r in rows[ci + 2:]:
        sym = _s(r[cix["Symbol"]]) if cix["Symbol"] < len(r) else ""
        if "|" in sym:
            footer = sym
            break
        if not sym or not _s(r[cix["Verdict"]]):
            break
        grid.append(r)
    if footer is None:
        for r in rows[ci + 2 + len(grid):]:
            if "|" in _s(r[0]) and "route" in _s(r[0]):
                footer = _s(r[0])
                break
    return cix, grid, footer


def _action_table(rows):
    """Portfolio_Decision action table: header 'Action|Symbol', rows until a blank Action."""
    hi = _find_row(rows, lambda r: _s(r[0]) == "Action" and len(r) > 1 and _s(r[1]) == "Symbol")
    if hi is None:
        return None, []
    ix = _header_index(rows, hi)
    body = []
    for r in rows[hi + 1:]:
        if not _s(r[0]):
            break
        body.append(r)
    return ix, body


def _fx_table_codes(rows):
    """_Lists_Config '2. FX TABLE' section: codes in column 0 after the 'CCY' header until a blank row."""
    si = _find_row(rows, lambda r: _s(r[0]).startswith("2. FX TABLE"))
    hi = _find_row(rows, lambda r: _s(r[0]) == "CCY", si if si is not None else 0)
    if hi is None:
        return None
    codes = []
    for r in rows[hi + 1:]:
        if not _s(r[0]):
            break
        codes.append(_s(r[0]))
    return codes


def _lists_default(rows, key):
    for r in rows:
        if _s(r[0]) == key and len(r) > 1:
            return r[1]
    return None


def build_context(book, expect, tz_offset=3, now_utc=None):
    tea = _tea()
    ctx = {"book": book, "expect": dict(expect or EXPECT), "tz_offset": tz_offset}
    pages = {}
    for p in MARKET_PAGES:
        rows = book.rows(p)
        pages[p] = Page(p, rows, book.grid_rows.get(p, len(rows or []))) if rows else None
    ctx["pages"] = pages
    mp_rows = book.rows(PORTFOLIO_PAGE)
    ctx["portfolio"] = Page(PORTFOLIO_PAGE, mp_rows, book.grid_rows.get(PORTFOLIO_PAGE, 0)) if mp_rows else None
    iv_rows = book.rows(INVESTABLE_VIEW)
    ctx["investable_view"] = Page(INVESTABLE_VIEW, iv_rows, book.grid_rows.get(INVESTABLE_VIEW, 0)) if iv_rows else None
    # cohorts
    invest, investable, where = set(), set(), collections.defaultdict(list)
    for p, pg in pages.items():
        if not pg:
            continue
        for r, sym in zip(pg.body, pg.syms):
            where[sym].append(p)
            if pg.s(r, "Final Action").upper() == "INVEST":
                invest.add(sym)
            if pg.s(r, "Investability Status").upper() == "INVESTABLE":
                investable.add(sym)
    ctx["invest"], ctx["investable"], ctx["where"] = invest, investable, where
    t10 = book.rows("Top_10_Investments") or []
    board, qualified = _board_and_qualified(t10)
    ctx["board"], ctx["qualified"] = board, qualified
    ctx["holdings"] = list(ctx["portfolio"].syms) if ctx["portfolio"] else []
    ctx["cohort_dq34"] = investable | set(board) | set(ctx["holdings"])
    ctx["cohort_calendar"] = invest | set(board) | set(ctx["holdings"])
    ctx["cohort_rows"] = invest | investable | set(board) | set(qualified) | set(ctx["holdings"])
    # reference clocks (export-derived)
    ref_rows = None
    for pg in pages.values():
        if not pg or pg.col("Last Updated (UTC)") is None:
            continue
        for r in pg.body:
            d = _parse_dt(pg.val(r, "Last Updated (UTC)"), tz_offset)
            if d and (ref_rows is None or d > ref_rows):
                ref_rows = d
    ctx["ref_time_rows"] = now_utc or ref_rows
    rl = book.rows("_Run_Log") or []
    ref_log = None
    if rl:
        rix = _header_index(rl)
        ti = rix.get("Timestamp", 0)
        for r in rl[1:]:
            d = _parse_dt(r[ti], tz_offset) if ti < len(r) else None
            if d and (ref_log is None or d > ref_log):
                ref_log = d
    ctx["ref_time_log"] = now_utc or ref_log or ctx["ref_time_rows"]
    ctx["asof"] = (ctx["ref_time_rows"] or ctx["ref_time_log"] or dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)).strftime("%Y-%m-%d")
    st_pages, st_globals = tea._status_pages(book, tz_offset)
    ctx["status_pages"], ctx["status_globals"] = st_pages, st_globals
    ctx["fx_codes"] = _fx_table_codes(book.rows("_Lists_Config") or [])
    ctx["t10"] = t10
    ctx["candidates"] = _candidate_grid(t10) if t10 else (None, [], None)
    return ctx


# --------------------------------------------------------------------------- #
# A - Structure and capacity
# --------------------------------------------------------------------------- #
def _pages_present(ctx):
    return [p for p in MARKET_PAGES if ctx["pages"].get(p)]


def dq_01_live_rows(ctx):
    vals, bad, ex = {}, 0, []
    if not _pages_present(ctx):
        return not_checked("DQ-01", "live_rows_vs_baseline", "A", "surface", "no market page in the export")
    for p in MARKET_PAGES:
        pg = ctx["pages"].get(p)
        n = pg.n if pg else 0
        vals[p] = n
        exp = ctx["expect"].get(p)
        if exp is not None and n != exp:
            bad += 1
            ex.append(f"{p} {n} vs {exp}")
    return check("DQ-01", "live_rows_vs_baseline", "A", "surface", "PASS" if bad == 0 else "FAIL", bad, vals, ex,
                 "live rows = non-blank Symbol per page vs EXPECT", "= EXPECT per page (--expect overrides)", "-")


def dq_02_padding(ctx):
    book = ctx["book"]
    vals, over = {}, 0
    for p in MARKET_PAGES:
        pg = ctx["pages"].get(p)
        if pg:
            vals[p] = pg.grid_rows - 1 - pg.n
    for tab in ("Top_10_Investments", "_Portfolio_CostBasis", "_Cash_Snapshot", "Insights_Analysis", "Calendar_Events"):
        rows = book.rows(tab)
        if rows is not None:
            vals[tab] = book.grid_rows.get(tab, len(rows)) - len(rows)
    if not vals:
        return not_checked("DQ-02", "padding_rows", "A", "surface", "no monitored tab in the export")
    over = sum(1 for v in vals.values() if v > THRESHOLDS["padding_rows_max"])
    total = sum(vals.values())
    return check("DQ-02", "padding_rows", "A", "surface", "WARN" if over else "PASS", total, vals,
                 [f"{k} {v}" for k, v in vals.items() if v > THRESHOLDS["padding_rows_max"]],
                 f"{over} tab(s) above {THRESHOLDS['padding_rows_max']} padding rows; count = total padding rows",
                 "<= 50 padding rows per tab", "WP-11")


def dq_03_grid(ctx):
    txt = ctx["status_globals"].get("TFB Grid Capacity", "")
    m = GRID_CAPACITY_RE.search(txt)
    if not m:
        return not_checked("DQ-03", "grid_allocation", "A", "surface", "_Status global 'TFB Grid Capacity' not found")
    pct = float(m.group(2))
    cells = int(m.group(1).replace(",", ""))
    status = "FAIL" if pct > THRESHOLDS["grid_fail_pct"] else "WARN" if pct > THRESHOLDS["grid_warn_pct"] else "PASS"
    return check("DQ-03", "grid_allocation", "A", "surface", status, pct, {"allocated_cells": cells, "allocated_pct": pct},
                 [f"allocated {pct}%"], "_Status 'TFB Grid Capacity' allocated share", "<= 60 %, trend <= 0 (FAIL > 75 %)", "WP-11")


def dq_04_export_size(ctx):
    size = ctx.get("export_bytes")
    return check("DQ-04", "export_size", "A", "surface", "INFO", size, {"export_bytes": size}, [],
                 f"{size:,} bytes" if size is not None else "", "trend <= 0 after WP-6 (INFO)", "WP-6")


def dq_05_orphans(ctx):
    names = list(ctx["book"].sheets)
    orphans = sorted(set(names) - set(KNOWN_TABS))
    return check("DQ-05", "orphan_tabs", "A", "book", "FAIL" if orphans else "PASS", len(orphans),
                 {"tabs_n": len(names), "known_n": len(KNOWN_TABS)}, orphans,
                 f"{len(orphans)} tab(s) not in the 39-name registry", "0 orphan tabs", "WP-11")


def dq_06_ghosts(ctx):
    dd = ctx["book"].rows("Data_Dictionary")
    if not dd:
        return not_checked("DQ-06", "ghost_sheets", "A", "book", "Data_Dictionary not in export")
    ix = _header_index(dd)
    si = ix.get("Sheet", 0)
    counts = collections.Counter(_s(r[si]) for r in dd[1:] if _s(r[si]))
    ghosts = sorted(set(counts) - set(ctx["book"].sheets))
    vals = {"dictionary_rows": sum(counts.values()), "ghost_rows": sum(counts[g] for g in ghosts)}
    return check("DQ-06", "ghost_sheets", "A", "book", "FAIL" if ghosts else "PASS", len(ghosts), vals, ghosts,
                 "Data_Dictionary 'Sheet' values without a tab (origin: core/data_engine_v2.py page map "
                 "'My_Investments' written by scripts/run_dashboard_sync.py TaskSpec DATA_DICTIONARY)",
                 "0 ghost sheets", "WP-6")


def dq_07_header_contract(ctx):
    dd = ctx["book"].rows("Data_Dictionary") or []
    dict_hdr = collections.defaultdict(list)
    if dd:
        ix = _header_index(dd)
        si, hi = ix.get("Sheet", 0), ix.get("Header", 2)
        for r in dd[1:]:
            if _s(r[si]):
                dict_hdr[_s(r[si])].append(_s(r[hi]))
    vals, diffs, ex = {}, 0, []
    checked = 0
    for p, contract in HEADER_CONTRACT.items():
        pg = ctx["pages"].get(p) if p in MARKET_PAGES else (ctx["portfolio"] if p == PORTFOLIO_PAGE else ctx["investable_view"])
        if not pg:
            continue
        checked += 1
        hdr = [h for h in pg.header]
        while hdr and not hdr[-1]:
            hdr.pop()
        vals[p] = sum(1 for h in hdr if h)
        pos = [i for i in range(max(len(hdr), len(contract))) if (hdr[i] if i < len(hdr) else None) != (contract[i] if i < len(contract) else None)]
        if pos:
            diffs += len(pos)
            ex.append(f"{p} header differs from contract at col {pos[0]}")
        if dict_hdr.get(p) and p != INVESTABLE_VIEW:
            dpos = [i for i in range(max(len(dict_hdr[p]), len(contract))) if (dict_hdr[p][i] if i < len(dict_hdr[p]) else None) != (contract[i] if i < len(contract) else None)]
            if dpos:
                diffs += len(dpos)
                ex.append(f"{p} dictionary differs from contract at row {dpos[0]}")
    if not checked:
        return not_checked("DQ-07", "header_contract", "A", "surface", "no contract page in the export")
    return check("DQ-07", "header_contract", "A", "surface", "FAIL" if diffs else "PASS", diffs, vals, ex,
                 "non-empty header cells per page; positions differing from the verbatim 115/122 contract and from the dictionary",
                 "115/115 on the four pages and the investable view, My_Portfolio 122; dictionary sequence equal", "-")


# --------------------------------------------------------------------------- #
# B - Identity and taxonomy
# --------------------------------------------------------------------------- #
def dq_08_inpage_dups(ctx):
    vals, ex, total = {}, [], 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        c = collections.Counter(pg.syms)
        d = sum(v - 1 for v in c.values() if v > 1)
        vals[p] = d
        total += d
        ex += [f"{s} ({p})" for s, v in c.items() if v > 1][:3]
    if not vals:
        return not_checked("DQ-08", "inpage_duplicate_symbols", "B", "identity", "no market page")
    return check("DQ-08", "inpage_duplicate_symbols", "B", "identity", "FAIL" if total else "PASS", total, vals, ex,
                 "sum(count-1) over Symbol per page", "0 in-page duplicates", "-")


def dq_09_crosspage_dups(ctx):
    dups = sorted(s for s, ps in ctx["where"].items() if len(set(ps)) > 1)
    vals = collections.Counter()
    for s in dups:
        for p in set(ctx["where"][s]):
            vals[p] += 1
    if not _pages_present(ctx):
        return not_checked("DQ-09", "crosspage_duplicate_symbols", "B", "identity", "no market page")
    return check("DQ-09", "crosspage_duplicate_symbols", "B", "identity", "FAIL" if dups else "PASS", len(dups), dict(vals),
                 [f"{s} ({'+'.join(sorted(set(ctx['where'][s])))})" for s in dups], "symbols present on more than one page (exact string)",
                 "0 cross-page duplicates", "WP-5")


def dq_10_bare_vs_us(ctx):
    vals, ex, total = {}, [], 0
    for p in _pages_present(ctx):
        st = set(ctx["pages"][p].syms)
        bare = sorted(s for s in st if "." not in s and (s + ".US") in st)
        vals[p] = len(bare)
        total += len(bare)
        ex += [f"{s} ({PAGE_SHORT[p]})" for s in bare]
    if not vals:
        return not_checked("DQ-10", "bare_vs_us_pairs", "B", "identity", "no market page")
    return check("DQ-10", "bare_vs_us_pairs", "B", "identity", "FAIL" if total else "PASS", total, vals, ex,
                 "symbols without '.' whose SYM.US twin is on the same page", "0 bare-vs-.US pairs", "WP-5")


def dq_11_asset_class(ctx):
    vals, ex, bad_rows, groups_total = {}, [], 0, 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        ac = collections.Counter(pg.s(r, "Asset Class") for r in pg.body)
        blank = ac.pop("", 0)
        low = collections.defaultdict(list)
        for k, v in ac.items():
            low[k.lower()].append((v, k))
        groups = {k: sorted(v) for k, v in low.items() if len(v) > 1}
        minority = sum(sum(c for c, _ in sorted(v)[:-1]) for v in groups.values())
        vals[p + ":distinct"] = len(ac) + (1 if blank else 0)
        vals[p + ":blank"] = blank
        vals[p + ":variant_groups"] = len(groups)
        vals[p + ":variant_minority_rows"] = minority
        groups_total += len(groups)
        bad_rows += minority + blank
        ex += [f"{p} {'/'.join(k for _, k in v)}" for v in groups.values()]
        if blank:
            ex.append(f"{p} blank {blank}")
        if len(ac) > THRESHOLDS["asset_class_max_distinct"]:
            ex.append(f"{p} distinct {len(ac)}")
    if not vals:
        return not_checked("DQ-11", "asset_class_vocabulary", "B", "identity", "no market page")
    fail = groups_total > 0 or any(v > 0 for k, v in vals.items() if k.endswith(":blank")) or \
        any(v > THRESHOLDS["asset_class_max_distinct"] for k, v in vals.items() if k.endswith(":distinct"))
    return check("DQ-11", "asset_class_vocabulary", "B", "identity", "FAIL" if fail else "PASS", bad_rows, vals, ex,
                 "count = rows in minority case variants + blank rows; FAIL on any case-variant group, blank > 0 or > 9 distinct values",
                 "<= 9 values, title-case, no casing variants, no blanks", "WP-5")


def dq_12_sector(ctx):
    vals, ex, count = {}, [], 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        sec = collections.Counter(pg.s(r, "Sector") for r in pg.body)
        viol = {k: v for k, v in sec.items() if k not in GICS_11 and k not in SECTOR_ALLOWED_EXTRA}
        yahoo = sum(v for k, v in viol.items() if k in YAHOO_ONLY_SECTORS)
        blank = viol.get("", 0)
        other = sum(v for k, v in viol.items() if k and k not in YAHOO_ONLY_SECTORS)
        gics_only = sum(v for k, v in sec.items() if k in GICS_11 and k not in YAHOO_ONLY_SECTORS and k not in
                        {"Energy", "Industrials", "Utilities", "Real Estate", "Communication Services"})
        vals[p + ":violations"] = sum(viol.values())
        vals[p + ":yahoo_only"] = yahoo
        vals[p + ":blank"] = blank
        vals[p + ":other"] = other
        vals[p + ":gics_only_rows"] = gics_only
        if p in ("Market_Leaders", "Global_Markets"):
            count += sum(viol.values())
            ex += [f"{PAGE_SHORT[p]} {k or '<blank>'} {v}" for k, v in sorted(viol.items(), key=lambda kv: -kv[1])[:3]]
    if not vals:
        return not_checked("DQ-12", "sector_taxonomy", "B", "identity", "no market page")
    return check("DQ-12", "sector_taxonomy", "B", "identity", "FAIL" if count else "PASS", count, vals, ex,
                 "count = Market_Leaders + Global_Markets rows whose Sector is outside GICS-11 u {Unknown}; "
                 "Commodities_FX / Mutual_Funds vocabularies reported only (non-GICS by design)",
                 "Sector in GICS-11 u {Unknown}; blank = 0 (one taxonomy)", "WP-5")


def dq_13_exchange(ctx):
    vals, ex, count = {}, [], 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        exc = collections.Counter(pg.s(r, "Exchange") for r in pg.body)
        mino, yhd = exc.get(EXCHANGE_MINORITY, 0), exc.get(EXCHANGE_PLACEHOLDER, 0)
        vals[p + ":minority"] = mino
        vals[p + ":canonical"] = exc.get(EXCHANGE_CANONICAL, 0)
        vals[p + ":placeholder"] = yhd
        count += mino + yhd
        if mino:
            ex.append(f"{PAGE_SHORT[p]} {EXCHANGE_MINORITY} {mino}")
        if yhd:
            ex.append(f"{PAGE_SHORT[p]} {EXCHANGE_PLACEHOLDER} {yhd}")
    if not vals:
        return not_checked("DQ-13", "exchange_labels", "B", "identity", "no market page")
    return check("DQ-13", "exchange_labels", "B", "identity", "FAIL" if count else "PASS", count, vals, ex,
                 "rows carrying the minority spelling 'NASDAQ/NYSE' beside 'NYSE/NASDAQ', plus 'YHD' placeholders",
                 "one exchange label per venue, 0 placeholders", "WP-5")


def dq_14_currency(ctx):
    codes = ctx["fx_codes"]
    vals, ex = {}, []
    in_use = collections.Counter()
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        cur = collections.Counter(pg.s(r, "Currency") for r in pg.body)
        vals[p + ":blank"] = cur.pop("", 0)
        vals[p + ":distinct"] = len(cur)
        in_use.update(cur)
    if not vals:
        return not_checked("DQ-14", "currency_codes", "B", "identity", "no market page")
    if codes is None:
        return not_checked("DQ-14", "currency_codes", "B", "identity", "_Lists_Config FX table (section '2. FX TABLE', header CCY) not found",
                           "0 gaps", "WP-5 / WP-8")
    codeset = set(codes)
    gaps = {c: n for c, n in in_use.items() if c not in codeset}
    low = collections.defaultdict(set)
    for c in codes:
        low[c.lower()].add(c)
    subunit_pairs = sorted("/".join(sorted(v)) for v in low.values() if len(v) > 1)
    vals["fx_table_codes"] = len(codes)
    vals["gap_rows"] = sum(gaps.values())
    vals["subunit_pairs"] = len(subunit_pairs)
    vals["GBX_rows"] = in_use.get("GBX", 0)
    ex = [f"{c} ({n} rows) missing from FX table" for c, n in sorted(gaps.items())] + [f"pair {p}" for p in subunit_pairs]
    return check("DQ-14", "currency_codes", "B", "identity", "FAIL" if gaps else "PASS", len(gaps), vals, ex,
                 "currency codes in use with no row in the _Lists_Config FX table; sub-unit code pairs reported",
                 "canonical codes; an FX row for every currency in use (0 gaps)", "WP-5 / WP-8")


def _no_price_class(tags, r, pg):
    if any(t.startswith("identity_quarantined") for t in tags):
        return "identity_quarantined"
    if "acquisition_status:unavailable" in tags:
        return "price_unavailable"
    if any(t.startswith("empty_row_no_provider_data") for t in tags):
        return "empty_row"
    if not pg.s(r, "Data Provider") and not pg.s(r, "Last Updated (UTC)"):
        return "blank_shell"
    return "untagged"


def dq_15_rows_without_price(ctx):
    vals, ex, total, fail = {}, [], 0, False
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        cls = collections.Counter()
        for r, tg in zip(pg.body, pg.tags):
            px = pg.num(r, "Current Price")
            if px is None or px <= 0:
                c = _no_price_class(tg, r, pg)
                cls[c] += 1
                if c == "untagged" and len(ex) < 8:
                    ex.append(f"{pg.s(r, 'Symbol')} ({PAGE_SHORT[p]} untagged)")
        n = sum(cls.values())
        total += n
        vals[p] = n
        vals[p + ":share_pct"] = _pct(n, pg.n)
        for k, v in cls.items():
            vals[f"{p}:{k}"] = v
        if _share(n, pg.n) > THRESHOLDS["no_price_share"] or cls.get("untagged", 0):
            fail = True
        vals[p + ":identity_quarantined_tag_rows"] = sum(1 for tg in pg.tags if any(t.startswith("identity_quarantined") for t in tg))
    if not vals:
        return not_checked("DQ-15", "rows_without_price", "B", "quote", "no market page")
    return check("DQ-15", "rows_without_price", "B", "quote", "FAIL" if fail else "PASS", total, vals, ex,
                 "Current Price blank or <= 0, classed by Warnings tag (identity_quarantined / price_unavailable / empty_row / blank_shell / untagged)",
                 "<= 1 % per page and every row tagged with reason + date", "WP-5")


def dq_16_misresolved(ctx):
    tea = _tea()
    vals, ex, union, alt = {}, [], set(), 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        mis, name_mis, a = 0, 0, 0
        for r in pg.body:
            sym = pg.s(r, "Symbol")
            if pg.s(r, "Asset Class").upper() == ALTSYMBOL_CLASS:
                a += 1
                ex.append(f"{sym} ALTSYMBOL")
            if tea._is_crypto(sym):
                if pg.s(r, "Asset Class").upper() != CRYPTO_CLASS or pg.s(r, "Exchange") != CRYPTO_EXCHANGE:
                    mis += 1
                    union.add((p, sym))
                keys = tea.CRYPTO_NAMES.get(sym[:-4])
                if keys and not any(k in pg.s(r, "Name").lower() for k in keys):
                    name_mis += 1
                    union.add((p, sym))
        vals[p + ":misclassed_crypto"] = mis
        vals[p + ":name_mismatch"] = name_mis
        vals[p + ":altsymbol"] = a
        alt += a
    if not vals:
        return not_checked("DQ-16", "misresolved_identities", "B", "identity", "no market page")
    ex = [f"{s} ({PAGE_SHORT[p]})" for p, s in sorted(union)] + ex
    count = len(union) + alt
    vals["union_rows"] = len(union)
    return check("DQ-16", "misresolved_identities", "B", "identity", "FAIL" if count else "PASS", count, vals, ex,
                 "count = |crypto rows misclassed (Asset Class != CRYPTOCURRENCY or Exchange != CCC) u rows whose Name misses "
                 "every curated keyword| + ALTSYMBOL rows", "0 mis-resolved identities", "WP-5")


def _ledger(ctx):
    book = ctx["book"]
    rows = book.rows("_Portfolio_CostBasis")
    if not rows:
        return None, []
    ix, body = book.table("_Portfolio_CostBasis", header_pred=lambda r: _s(r[0]) == "Symbol" and "Status" in [_s(c) for c in r])
    if not ix:
        return None, []
    lots = [r for r in body if _s(r[ix["Symbol"]]) and _s(r[ix["Status"]])]
    return ix, lots


def dq_17_ledger_symbols(ctx):
    lix, lots = _ledger(ctx)
    drows = ctx["book"].rows("_Dividend_Log")
    if lix is None and not drows:
        return not_checked("DQ-17", "ledger_dividend_symbols", "B", "identity", "ledger and dividend log not in export")
    bare_l = [_s(r[lix["Symbol"]]) for r in lots if not re.search(r"[.\-=]", _s(r[lix["Symbol"]]))] if lix else []
    bare_d = []
    if drows:
        dix = _header_index(drows)
        si = dix.get("Symbol")
        if si is not None:
            bare_d = [_s(r[si]) for r in drows[1:] if _s(r[si]) and not re.search(r"[.\-=]", _s(r[si]))]
    count = len(bare_l) + len(bare_d)
    return check("DQ-17", "ledger_dividend_symbols", "B", "identity", "FAIL" if count else "PASS", count,
                 {"ledger_lots": len(bare_l), "dividend_rows": len(bare_d), "lots_total": len(lots)},
                 [f"lot {s}" for s in bare_l] + [f"dividend {s}" for s in bare_d],
                 "ledger lots and _Dividend_Log rows whose Symbol carries no '.', '-' or '='", "0 suffix-less symbols", "WP-9")


# --------------------------------------------------------------------------- #
# C - Quote coherence and timeliness
# --------------------------------------------------------------------------- #
def dq_18_day_range(ctx):
    vals, ex, total = {}, [], 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        out, inv = 0, 0
        for r in pg.body:
            px, lo, hi = pg.num(r, "Current Price"), pg.num(r, "Day Low"), pg.num(r, "Day High")
            if lo is not None and hi is not None and lo > hi:
                inv += 1
            if px and px > 0 and lo is not None and hi is not None and (px < lo or px > hi):
                out += 1
                ex.append(f"{pg.s(r, 'Symbol')} ({PAGE_SHORT[p]})")
        vals[p + ":outside"] = out
        vals[p + ":low_gt_high"] = inv
        total += out + inv
    if not vals:
        return not_checked("DQ-18", "day_range", "C", "quote", "no market page")
    return check("DQ-18", "day_range", "C", "quote", "WARN" if total else "PASS", total, vals, ex,
                 "price outside [Day Low, Day High] plus Day Low > Day High rows", "0 breaches", "WP-4")


def dq_19_w52(ctx):
    vals, ex, total, sent = {}, [], 0, 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        below = above = mism = s_ = 0
        for r in pg.body:
            px, lo, hi, pos = pg.num(r, "Current Price"), pg.num(r, "52W Low"), pg.num(r, "52W High"), pg.num(r, "52W Position %")
            for c in ("52W High", "52W Low", "Target Price"):
                v = pg.num(r, c)
                if v is not None and abs(v - THRESHOLDS["w52_sentinel"]) < 1e-3:
                    s_ += 1
            if px and px > 0:
                if lo is not None and px < lo:
                    below += 1
                    ex.append(f"{pg.s(r, 'Symbol')} below ({PAGE_SHORT[p]})")
                if hi is not None and px > hi:
                    above += 1
                    ex.append(f"{pg.s(r, 'Symbol')} above ({PAGE_SHORT[p]})")
                if pos is not None and lo is not None and hi is not None and hi > lo:
                    if abs((px - lo) / (hi - lo) * 100.0 - pos) > THRESHOLDS["w52_position_pts"]:
                        mism += 1
        vals[p + ":below_low"] = below
        vals[p + ":above_high"] = above
        vals[p + ":position_mismatch"] = mism
        vals[p + ":sentinel"] = s_
        total += below + above + mism + s_
        sent += s_
    if not vals:
        return not_checked("DQ-19", "w52_bounds", "C", "quote", "no market page")
    status = "FAIL" if sent else "WARN" if total else "PASS"
    return check("DQ-19", "w52_bounds", "C", "quote", status, total, vals, ex,
                 "price outside the 52W band, 52W Position % recomputed (> 2 pts off), 999999.9999 sentinels (FAIL)",
                 "0 breaches; position within 2 pts; no sentinel", "WP-4")


def dq_20_percent_change(ctx):
    vals, ex, total = {}, [], 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        n = mm = 0
        for r in pg.body:
            pc, px, pv = pg.num(r, "Percent Change"), pg.num(r, "Current Price"), pg.num(r, "Previous Close")
            if pc is not None and px and pv and px > 0 and pv > 0:
                n += 1
                if abs(pc - (px / pv - 1.0)) > THRESHOLDS["pct_change_tolerance"]:
                    mm += 1
                    if len(ex) < 8:
                        ex.append(f"{pg.s(r, 'Symbol')} ({PAGE_SHORT[p]})")
        vals[p + ":checked"] = n
        vals[p + ":mismatch"] = mm
        total += mm
    if not vals:
        return not_checked("DQ-20", "percent_change_identity", "C", "quote", "no market page")
    return check("DQ-20", "percent_change_identity", "C", "quote", "FAIL" if total else "PASS", total, vals, ex,
                 "Percent Change (fraction) vs Current Price / Previous Close - 1, tolerance 0.5 pp", "0 mismatches > 0.5 pp", "-")


def dq_21_freshness(ctx):
    ref = ctx["ref_time_rows"]
    vals, ex, total = {}, [], 0
    if ref is None:
        return not_checked("DQ-21", "sync_freshness", "C", "quote", "no parseable Last Updated (UTC) on any page")
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        older = unst = 0
        for r in pg.body:
            d = _parse_dt(pg.val(r, "Last Updated (UTC)"), ctx["tz_offset"])
            if d is None:
                unst += 1
                continue
            if (ref - d).total_seconds() > THRESHOLDS["freshness_hours"] * 3600:
                older += 1
                if len(ex) < 8:
                    ex.append(f"{pg.s(r, 'Symbol')} {d:%Y-%m-%d} ({PAGE_SHORT[p]})")
        vals[p + ":older_24h"] = older
        vals[p + ":unstamped"] = unst
        total += older + unst
    return check("DQ-21", "sync_freshness", "C", "quote", "WARN" if total else "PASS", total, vals, ex,
                 f"age vs ref_time_rows {ref.isoformat()} > 24 h, plus unstamped rows", "0 older than 24 h, 0 unstamped", "WP-5")


def dq_22_stale_bars(ctx):
    vals, ex, stale_total, unblocked = {}, [], 0, 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        st = [(r, tg) for r, tg in zip(pg.body, pg.tags) if any(t.startswith("price_bar_stale:") for t in tg)]
        ub = [r for r, _ in st if pg.s(r, "Investability Status").upper() != "BLOCKED"]
        vals[p + ":stale"] = len(st)
        vals[p + ":unblocked"] = len(ub)
        stale_total += len(st)
        unblocked += len(ub)
        ex += [f"{pg.s(r, 'Symbol')} ({PAGE_SHORT[p]})" for r in ub[:3]]
    if not vals:
        return not_checked("DQ-22", "stale_bar_blocked", "C", "quote", "no market page")
    vals["stale_total"] = stale_total
    return check("DQ-22", "stale_bar_blocked", "C", "quote", "FAIL" if unblocked else "PASS", unblocked, vals, ex,
                 "rows tagged price_bar_stale:<date>:<N>s whose Investability Status != BLOCKED (count); stale totals in values",
                 "price-bar age > 5 sessions => BLOCKED (0 unblocked)", "-")


def dq_23_stamp_offset(ctx):
    vals, ex, bad_total, ok_total = {}, [], 0, 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        ok = bad = 0
        for r in pg.body:
            u, ry = pg.s(r, "Last Updated (UTC)"), pg.s(r, "Last Updated (Riyadh)")
            if not u or not ry:
                continue
            try:
                ud = dt.datetime.fromisoformat(u.replace("Z", "+00:00"))
                rd = dt.datetime.fromisoformat(ry.replace("Z", "+00:00"))
            except ValueError:
                bad += 1
                continue
            diff = (rd.replace(tzinfo=None) - ud.replace(tzinfo=None)).total_seconds()
            if abs(diff - THRESHOLDS["riyadh_offset_s"]) <= THRESHOLDS["riyadh_tolerance_s"]:
                ok += 1
            else:
                bad += 1
                if len(ex) < 8:
                    ex.append(f"{pg.s(r, 'Symbol')} ({PAGE_SHORT[p]})")
        vals[p + ":ok"] = ok
        vals[p + ":bad"] = bad
        ok_total += ok
        bad_total += bad
    if not vals:
        return not_checked("DQ-23", "riyadh_utc_offset", "C", "provenance", "no market page")
    return check("DQ-23", "riyadh_utc_offset", "C", "provenance", "FAIL" if bad_total else "PASS", bad_total, vals, ex,
                 "wall-clock Riyadh - UTC must be +3 h (+/- 60 s) on rows with both stamps", "100 %", "-")


# --------------------------------------------------------------------------- #
# D - Fundamentals units and plausibility
# --------------------------------------------------------------------------- #
def dq_24_margins(ctx):
    vals, ex, total, gm_oob = {}, [], 0, 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        for col in MARGIN_COLS:
            v = [pg.num(r, col) for r in pg.body]
            v = [x for x in v if x is not None]
            oob = sum(1 for x in v if abs(x) > THRESHOLDS["margin_abs_max"])
            key = f"{p}:{col.split()[0].lower()}"
            vals[key + "_n"] = len(v)
            vals[key + "_oob"] = oob
            vals[key + "_gt1"] = sum(1 for x in v if x > 1)
            vals[key + "_ltm1"] = sum(1 for x in v if x < -1)
            total += oob
            if p == "Global_Markets":
                gm_oob += oob
            if oob and len(ex) < 8:
                ex.append(f"{PAGE_SHORT[p]} {col} oob {oob}")
        units = collections.Counter()
        rows_pts = rows_frac = 0
        for tg in pg.tags:
            u = [t.split(":")[2] for t in tg if t.startswith(MARGIN_PUBLISH_PREFIX) and t.count(":") >= 3]
            units.update(u)
            rows_pts += 1 if "pts" in u else 0
            rows_frac += 1 if "frac" in u else 0
        for u, n in units.items():
            vals[f"{p}:tag_{u}"] = n
        vals[p + ":rows_pts_tag"] = rows_pts
        vals[p + ":rows_frac_tag"] = rows_frac
    if not vals:
        return not_checked("DQ-24", "margin_units", "D", "fundamentals", "no market page")
    return check("DQ-24", "margin_units", "D", "fundamentals", "FAIL" if gm_oob else "PASS", total, vals, ex,
                 "count = margin cells with |v| > 1.5 over Operating/Gross/Profit Margin on all four pages (status driven by "
                 "Global_Markets); margin_publish unit census (pts / frac / pts_thin / oob / frac_amb) and rows whose unit is "
                 "EXACTLY 'pts' vs EXACTLY 'frac'. |v| > 1.0 is never labelled a unit error.",
                 "margins stored as fractions, |v| <= 1.5 (0 out of band or tagged oob)", "WP-4")


def dq_25_debt_equity(ctx):
    vals, ex, untagged = {}, [], 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        n = gt10 = gt100 = neg = ut = 0
        for r, tg in zip(pg.body, pg.tags):
            v = pg.num(r, "Debt/Equity")
            if v is None:
                continue
            n += 1
            if v < 0:
                neg += 1
            if v > 100:
                gt100 += 1
            if v > THRESHOLDS["de_max"]:
                gt10 += 1
                if not any(t.startswith(DE_UNIT_TAG) for t in tg):
                    ut += 1
                    if len(ex) < 8:
                        ex.append(f"{pg.s(r, 'Symbol')} ({PAGE_SHORT[p]})")
        vals[p + ":n"] = n
        vals[p + ":gt10"] = gt10
        vals[p + ":gt100"] = gt100
        vals[p + ":negative"] = neg
        vals[p + ":gt10_untagged"] = ut
        untagged += ut
    if not vals:
        return not_checked("DQ-25", "debt_equity_ratio", "D", "fundamentals", "no market page")
    return check("DQ-25", "debt_equity_ratio", "D", "fundamentals", "FAIL" if untagged else "PASS", untagged, vals, ex,
                 f"count = Debt/Equity > 10 rows WITHOUT the exact tag {DE_UNIT_TAG}; > 100 and negative counts in values",
                 "ratio, plausible: 0 rows > 10 untagged", "WP-4")


def dq_26_dividend_yield(ctx):
    vals, ex, total = {}, [], 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        hits = [(pg.s(r, "Symbol"), pg.num(r, "Dividend Yield")) for r in pg.body if (pg.num(r, "Dividend Yield") or 0) > THRESHOLDS["yield_max"]]
        vals[p] = len(hits)
        total += len(hits)
        ex += [f"{s} {v:.4g} ({PAGE_SHORT[p]})" for s, v in hits[:3]]
    if not vals:
        return not_checked("DQ-26", "dividend_yield", "D", "fundamentals", "no market page")
    return check("DQ-26", "dividend_yield", "D", "fundamentals", "WARN" if total else "PASS", total, vals, ex,
                 "Dividend Yield > 0.25 (fraction)", "0 rows above 0.25", "WP-4")


def dq_27_beta_growth(ctx):
    vals, ex, total = {}, [], 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        be = [pg.num(r, "Beta (5Y)") for r in pg.body]
        be = [x for x in be if x is not None]
        gr = [pg.num(r, "Revenue Growth YoY") for r in pg.body]
        gr = [x for x in gr if x is not None]
        b5 = sum(1 for x in be if abs(x) > THRESHOLDS["beta_abs_max"])
        g15 = sum(1 for x in gr if abs(x) > THRESHOLDS["growth_abs_max"])
        vals[p + ":beta_n"] = len(be)
        vals[p + ":beta_gt5"] = b5
        vals[p + ":beta_gt10"] = sum(1 for x in be if abs(x) > THRESHOLDS["beta_garbage"])
        vals[p + ":growth_n"] = len(gr)
        vals[p + ":growth_gt1_5"] = g15
        vals[p + ":growth_gt20"] = sum(1 for x in gr if abs(x) > THRESHOLDS["growth_garbage"])
        if be:
            vals[p + ":beta_min"] = round(min(be), 3)
            vals[p + ":beta_max"] = round(max(be), 3)
        if gr:
            vals[p + ":growth_max"] = round(max(gr), 3)
        total += b5 + g15
        if b5 or g15:
            ex.append(f"{PAGE_SHORT[p]} beta {b5} growth {g15}")
    if not vals:
        return not_checked("DQ-27", "beta_growth", "D", "fundamentals", "no market page")
    return check("DQ-27", "beta_growth", "D", "fundamentals", "WARN" if total else "PASS", total, vals, ex,
                 "|Beta (5Y)| > 5 and |Revenue Growth YoY| > 1.5 (garbage tiers > 10 / > 20 in values)", "0 untagged", "WP-4")


def dq_28_pe_zero(ctx):
    vals, ex, total = {}, [], 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        n = z = zv = lt1 = 0
        for r in pg.body:
            v = pg.num(r, "P/E (TTM)")
            if v is None:
                continue
            n += 1
            if v == 0:
                z += 1
                if pg.num(r, "Valuation Score") is not None:
                    zv += 1
                if len(ex) < 6:
                    ex.append(f"{pg.s(r, 'Symbol')} ({PAGE_SHORT[p]})")
            elif 0 < v < 1:
                lt1 += 1
        vals[p + ":n"] = n
        vals[p + ":zero"] = z
        vals[p + ":zero_with_valuation"] = zv
        vals[p + ":between_0_1"] = lt1
        total += z
    if not vals:
        return not_checked("DQ-28", "pe_zero", "D", "fundamentals", "no market page")
    return check("DQ-28", "pe_zero", "D", "fundamentals", "FAIL" if total else "PASS", total, vals, ex,
                 "P/E (TTM) == 0 rows (treated as a value, not missing); of those with a Valuation Score", "0 zero P/E", "WP-4")


def dq_29_rsi_default(ctx):
    vals, ex, total = {}, [], 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        hits = [pg.s(r, "Symbol") for r in pg.body if pg.num(r, "RSI (14)") == THRESHOLDS["rsi_default"]]
        vals[p] = len(hits)
        total += len(hits)
        ex += [f"{s} ({PAGE_SHORT[p]})" for s in hits[:2]]
    if not vals:
        return not_checked("DQ-29", "rsi_default_fill", "D", "fundamentals", "no market page")
    return check("DQ-29", "rsi_default_fill", "D", "fundamentals", "WARN" if total else "PASS", total, vals, ex,
                 "RSI (14) exactly 50.0", "0 default fills", "WP-4")


def _garbage(pg, r):
    b, g, d = pg.num(r, "Beta (5Y)"), pg.num(r, "Revenue Growth YoY"), pg.num(r, "Debt/Equity")
    ms = [pg.num(r, c) for c in MARGIN_COLS]
    return ((b is not None and abs(b) > THRESHOLDS["beta_garbage"]) or (g is not None and abs(g) > THRESHOLDS["growth_garbage"])
            or (d is not None and abs(d) > THRESHOLDS["de_garbage"]) or any(m is not None and abs(m) > THRESHOLDS["margin_garbage"] for m in ms))


def dq_30_dq100_oob(ctx):
    vals, ex, at100 = {}, [], 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        g = a = inv = 0
        for r in pg.body:
            if _garbage(pg, r):
                g += 1
                if pg.num(r, "Data Quality Score") == 100:
                    a += 1
                    if pg.s(r, "Investability Status").upper() == "INVESTABLE":
                        inv += 1
                        ex.append(f"{pg.s(r, 'Symbol')} INVESTABLE ({PAGE_SHORT[p]})")
        vals[p + ":garbage_rows"] = g
        vals[p + ":at_dq100"] = a
        vals[p + ":at_dq100_investable"] = inv
        at100 += a
    if not vals:
        return not_checked("DQ-30", "dq100_with_oob_field", "D", "scores", "no market page")
    return check("DQ-30", "dq100_with_oob_field", "D", "scores", "FAIL" if at100 else "PASS", at100, vals, ex,
                 "count = rows at Data Quality Score == 100 with a garbage-magnitude fundamental (|beta| > 10, |margin| > 5, "
                 "|growth| > 20, |D/E| > 50) over all pages", "Data Quality Score < 100 when any critical field is out of band", "WP-4")


# --------------------------------------------------------------------------- #
# E - Forecast tuple and scoring
# --------------------------------------------------------------------------- #
def dq_31_roi_identity(ctx):
    vals, ex, total = {}, [], 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        for hz, (fpc, roic) in HORIZONS.items():
            chk = mm = 0
            for r in pg.body:
                px, fp, roi = pg.num(r, "Current Price"), pg.num(r, fpc), pg.num(r, roic)
                if px and px > 0 and fp and fp > 0 and roi is not None:
                    chk += 1
                    if abs(fp / px - 1.0 - roi) > THRESHOLDS["roi_identity_tolerance"]:
                        mm += 1
                        if len(ex) < 8:
                            ex.append(f"{pg.s(r, 'Symbol')} {hz} ({PAGE_SHORT[p]})")
            vals[f"{p}:{hz}_checked"] = chk
            vals[f"{p}:{hz}_mismatch"] = mm
            total += mm
    if not vals:
        return not_checked("DQ-31", "roi_identity", "E", "forecast", "no market page")
    return check("DQ-31", "roi_identity", "E", "forecast", "FAIL" if total else "PASS", total, vals, ex,
                 "|Forecast Price / Current Price - 1 - Expected ROI| > 1 pp per horizon (fractions)", "0 mismatches > 1 pp", "-")


def dq_32_fp_without_roi(ctx):
    vals, ex, total = {}, [], 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        for hz, (fpc, roic) in HORIZONS.items():
            n = 0
            for r in pg.body:
                fp, roi = pg.num(r, fpc), pg.num(r, roic)
                if fp is not None and fp > 0 and roi is None:
                    n += 1
                    if hz == "12M" and len(ex) < 8:
                        ex.append(f"{pg.s(r, 'Symbol')} ({PAGE_SHORT[p]})")
            vals[f"{p}:{hz}"] = n
            if hz == "12M":
                total += n
        census = collections.Counter()
        tagged12 = 0
        for tg in pg.tags:
            for t in tg:
                if t.startswith("eq_roi_backfill:"):
                    parts = t.split(":")
                    census[":".join(parts[2:4])] += 1
                    if parts[1] == "expected_roi_12m":
                        tagged12 += 1
        vals[p + ":rows_tagged_12m"] = tagged12
        for k, v in census.items():
            vals[f"{p}:tag_{k.replace(':', '_')}"] = v
    if not vals:
        return not_checked("DQ-32", "forecast_without_roi", "E", "forecast", "no market page")
    return check("DQ-32", "forecast_without_roi", "E", "forecast", "FAIL" if total else "PASS", total, vals, ex,
                 "count = Forecast Price 12M > 0 with blank Expected ROI 12M (1M/3M in values); eq_roi_backfill tag census",
                 "Forecast Price present => Expected ROI present (0 blanks)", "WP-3")


def dq_33_roi12_cap(ctx):
    vals, ex, total = {}, [], 0
    cap = THRESHOLDS["roi12_soft_cap"]
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        hits = [(r, tg) for r, tg in zip(pg.body, pg.tags) if (pg.num(r, "Expected ROI 12M") or 0) > cap]
        vals[p] = len(hits)
        vals[p + ":at_cap"] = sum(1 for r in pg.body if pg.num(r, "Expected ROI 12M") == cap)
        if hits:
            src = collections.Counter(pg.s(r, "Forecast Source") or "<blank>" for r, _ in hits)
            inv = collections.Counter(pg.s(r, "Investability Status") or "<blank>" for r, _ in hits)
            for k, v in src.items():
                vals[f"{p}:source_{k}"] = v
            for k, v in inv.items():
                vals[f"{p}:status_{k}"] = v
            rois = [pg.num(r, "Expected ROI 12M") for r, _ in hits]
            vals[p + ":roi12_min"] = round(min(rois), 6)
            vals[p + ":roi12_max"] = round(max(rois), 6)
            vals[p + ":capped_tag"] = sum(1 for _, tg in hits if any(t.startswith("provider_target_capped_for_short_horizon_derivation") for t in tg))
            ex += [f"{pg.s(r, 'Symbol')} {pg.num(r, 'Expected ROI 12M'):.3f} ({PAGE_SHORT[p]})" for r, _ in hits[:5]]
        total += len(hits)
    if not vals:
        return not_checked("DQ-33", "roi12_soft_cap", "E", "forecast", "no market page")
    return check("DQ-33", "roi12_soft_cap", "E", "forecast", "FAIL" if total else "PASS", total, vals, ex,
                 "Expected ROI 12M > 0.35 strictly, by Forecast Source and Investability Status", "0 rows above the 0.35 soft cap", "WP-3")


def dq_34_tuple_conflict(ctx):
    vals, ex, rows_total, cohort_hits, qual_hits = {}, [], 0, 0, 0
    board, hold, qual = set(ctx["board"]), set(ctx["holdings"]), set(ctx["qualified"])
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        fields = collections.Counter()
        n = inv = b = h = 0
        for r, tg in zip(pg.body, pg.tags):
            tc = [t for t in tg if t.startswith("sheet_tuple_conflict:")]
            if not tc:
                continue
            n += 1
            sym = pg.s(r, "Symbol")
            for t in tc:
                fields[t.split(":")[1] if t.count(":") >= 1 else "?"] += 1
            is_inv = pg.s(r, "Investability Status").upper() == "INVESTABLE"
            in_board, in_hold = sym in board, sym in hold
            if is_inv:
                inv += 1
            if in_board:
                b += 1
            if in_hold:
                h += 1
            if is_inv or in_board or in_hold:
                cohort_hits += 1
                ex.append(f"{sym} ({PAGE_SHORT[p]}{' INVESTABLE' if is_inv else ''}{' board' if in_board else ''}{' holding' if in_hold else ''})")
            if sym in qual:
                qual_hits += 1
        vals[p + ":rows"] = n
        vals[p + ":investable"] = inv
        vals[p + ":board"] = b
        vals[p + ":holdings"] = h
        for f, v in fields.items():
            vals[f"{p}:field_{f}"] = v
        rows_total += n
    if not vals:
        return not_checked("DQ-34", "tuple_vintage_conflict", "E", "forecast", "no market page")
    mp = ctx["portfolio"]
    if mp:
        vals["My_Portfolio:rows"] = sum(1 for tg in mp.tags if any(t.startswith("sheet_tuple_conflict:") for t in tg))
    vals["rows_total"] = rows_total
    vals["qualified_hits"] = qual_hits
    status = "FAIL" if cohort_hits else "WARN" if rows_total else "PASS"
    return check("DQ-34", "tuple_vintage_conflict", "E", "forecast", status, cohort_hits, vals, ex,
                 "count = rows in (INVESTABLE u BOARD u HOLDINGS) carrying any sheet_tuple_conflict:<field> tag; all tagged rows, "
                 "fields and qualified-set hits in values", "0 on INVESTABLE / board / holdings (WARN when only other rows are tagged)", "WP-3")


def dq_35_analyst_lkg(ctx):
    vals, ex, inv_rows = {}, [], 0
    hours_inv = []
    n_sessions = THRESHOLDS["target_vintage_sessions"]
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        rows = inv = 0
        for r, tg in zip(pg.body, pg.tags):
            hs = [int(m.group(1)) for t in tg for m in [LKG_HOURS_RE.match(t)] if m]
            if not hs:
                continue
            rows += 1
            if pg.s(r, "Investability Status").upper() == "INVESTABLE":
                inv += 1
                hours_inv.append(max(hs))
                if len(ex) < 6:
                    ex.append(f"{pg.s(r, 'Symbol')} {max(hs)}h ({PAGE_SHORT[p]})")
        vals[p + ":rows"] = rows
        vals[p + ":investable"] = inv
        vals[p + ":investable_total"] = sum(1 for r in pg.body if pg.s(r, "Investability Status").upper() == "INVESTABLE")
        inv_rows += inv
    if not vals:
        return not_checked("DQ-35", "analyst_target_vintage", "E", "forecast", "no market page")
    if hours_inv:
        vals["investable_hours_max"] = max(hours_inv)
        vals["investable_hours_median"] = statistics.median(hours_inv)
    if n_sessions is None:
        status = "WARN" if inv_rows else "PASS"
        rule = "<= N sessions on INVESTABLE; N unset by the operator -> WARN while any INVESTABLE row rides analyst_lkg"
    else:
        over = sum(1 for h in hours_inv if h > n_sessions * 24)
        vals["investable_over_n"] = over
        status = "FAIL" if over else "PASS"
        rule = f"<= {n_sessions} sessions ({n_sessions * 24} h) on INVESTABLE"
    return check("DQ-35", "analyst_target_vintage", "E", "forecast", status, inv_rows, vals, ex,
                 "INVESTABLE rows carrying analyst_lkg:<N>h (count), hours distribution in values", rule, "WP-3")


def dq_36_horizon(ctx):
    vals, ex, total = {}, [], 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        pairs = collections.Counter((pg.num(r, "Horizon Days"), pg.s(r, "Invest Period Label")) for r in pg.body)
        mm = month = long_ = year = unknown = 0
        for r, tg in zip(pg.body, pg.tags):
            reason = pg.s(r, "Recommendation Reason")
            lbl = pg.s(r, "Invest Period Label").upper()
            is_month, is_long, is_year = "horizon=month" in reason, "horizon=long" in reason, "horizon=year" in reason
            month += is_month
            long_ += is_long
            year += is_year
            if "sheet_horizon_label:horizon_label:unknown" in tg:
                unknown += 1
            if lbl in ("1Y", "12M") and (is_month or is_long):
                mm += 1
        vals[p + ":mismatch"] = mm
        vals[p + ":reason_month"] = month
        vals[p + ":reason_long"] = long_
        vals[p + ":reason_year"] = year
        vals[p + ":label_unknown_tag"] = unknown
        vals[p + ":pairs_distinct"] = len(pairs)
        total += mm
        top = pairs.most_common(1)
        if top:
            ex.append(f"{PAGE_SHORT[p]} {top[0][0][0]:g}/{top[0][0][1]} x{top[0][1]}" if top[0][0][0] is not None else f"{PAGE_SHORT[p]} blank/{top[0][0][1]} x{top[0][1]}")
    if not vals:
        return not_checked("DQ-36", "horizon_coherence", "E", "forecast", "no market page")
    mp = ctx["portfolio"]
    if mp:
        mpairs = collections.Counter((mp.num(r, "Horizon Days"), mp.s(r, "Invest Period Label")) for r in mp.body)
        for (d, l), n in mpairs.items():
            vals[f"My_Portfolio:{d:g}/{l}" if d is not None else f"My_Portfolio:blank/{l}"] = n
    return check("DQ-36", "horizon_coherence", "E", "forecast", "FAIL" if total else "PASS", total, vals, ex,
                 "count = rows labelled 1Y/12M whose Recommendation Reason says horizon=month or horizon=long", "label/days agree with the reason text", "WP-3 / F-1")


def _settlement_state(pg, row, tags):
    """Read explicit failure receipts, never arbitrary 'unresolved' text.

    Scoring Errors can contain several semicolon-delimited diagnostic records.
    JSONDecoder consumes only the settlement object, so a following record is
    harmless and status text in an unrelated field never becomes evidence.
    """
    if any(tag.startswith(F7_NONCONV_PREFIXES) for tag in tags):
        return "failed"
    raw = pg.s(row, "Scoring Errors")
    if not raw:
        return "ok"
    markers = list(re.finditer(r"(?:^|[;\n])\s*scoring_settlement:\s*", raw))
    if not markers:
        return "ok"
    if len(raw) > 65536:
        return "unknown"

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("ambiguous settlement diagnostic")
            result[key] = value
        return result

    decoder = json.JSONDecoder(object_pairs_hook=unique)
    unknown = False
    for match in markers:
        try:
            record, _ = decoder.raw_decode(raw[match.end():])
        except (ValueError, RecursionError):
            unknown = True
            continue
        status = record.get("status") if isinstance(record, dict) else None
        if not isinstance(status, str):
            unknown = True
        elif status in {"non_converged", "error"}:
            return "failed"
        elif status not in {"stable", "converged"}:
            unknown = True
    return "unknown" if unknown else "ok"


def _settlement_failed(pg, row, tags):
    # Unknown structured diagnostics fail the check without being counted as
    # proven non-convergence. Row flags retain both kinds of unresolved input.
    return _settlement_state(pg, row, tags) != "ok"


def dq_37_f7(ctx):
    vals, ex, flips_total, nonconv_total, invalid_total = {}, [], 0, 0, 0
    ex_nc = []   # v1.0.1: INVESTABLE non-converged rows get their own list and precedence
    pairs = collections.Counter()
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        tagged = flips = flips_inv = d5 = d10 = nonconv = invalid = 0
        for r, tg in zip(pg.body, pg.tags):
            f7 = [t for t in tg if t.startswith("f7_settle:")]
            settlement_state = _settlement_state(pg, r, tg)
            if settlement_state == "unknown":
                invalid += 1
            if settlement_state == "failed":
                nonconv += 1
                if pg.s(r, "Investability Status").upper() == "INVESTABLE" and len(ex_nc) < 6:
                    ex_nc.append(f"{pg.s(r, 'Symbol')} INVESTABLE non-converged ({PAGE_SHORT[p]})")
            if not f7:
                continue
            tagged += 1
            flip, dmax = False, 0.0
            for t in f7:
                m = F7_RC_RE.search(t)
                if m:
                    flip = True
                    pairs[f"{m.group(1)}>{m.group(2)}"] += 1
                for a, b in F7_OV_RE.findall(t):
                    try:
                        dmax = max(dmax, abs(float(a) - float(b)))
                    except ValueError:
                        pass
            is_inv = pg.s(r, "Investability Status").upper() == "INVESTABLE"
            if flip:
                flips += 1
                if is_inv:
                    flips_inv += 1
                    if len(ex) < 6:
                        ex.append(f"{pg.s(r, 'Symbol')} INVESTABLE flip ({PAGE_SHORT[p]})")
            if dmax >= THRESHOLDS["f7_delta_warn"]:
                d5 += 1
            if dmax >= THRESHOLDS["f7_delta_fail"]:
                d10 += 1
        vals[p + ":tagged"] = tagged
        vals[p + ":flips"] = flips
        vals[p + ":flips_investable"] = flips_inv
        vals[p + ":delta_ge5"] = d5
        vals[p + ":delta_ge10"] = d10
        vals[p + ":non_converged"] = nonconv
        if invalid:
            vals[p + ":invalid_settlement_diagnostics"] = invalid
        invalid_total += invalid
        flips_total += flips
        nonconv_total += nonconv
    if not vals:
        return not_checked("DQ-37", "f7_pass_dependence", "E", "scores", "no market page")
    vals["flips_total"] = flips_total
    vals["non_converged_total"] = nonconv_total
    if invalid_total:
        vals["invalid_settlement_diagnostics_total"] = invalid_total
    for k, v in pairs.most_common(5):
        vals["pair_" + k] = v
    # v1.0.1: the 6 example slots fill with INVESTABLE rc flips long before the last
    # non-converged rows on a real export, so a non-converged INVESTABLE row goes FIRST.
    ex = ex_nc + ex[:max(0, 6 - len(ex_nc))]
    ex += [f"{k} x{v}" for k, v in pairs.most_common(3)]
    status = "FAIL" if flips_total or nonconv_total or invalid_total else "PASS"
    return check("DQ-37", "f7_pass_dependence", "E", "scores", status, flips_total, vals, ex,
                 "count = rows whose f7_settle tag carries rc=a>b (recommendation flip); |delta overall| >= 5 / >= 10 and "
                 "failed/unresolved rows in values (f7_settle_failed, settle_unresolved:, or explicit scoring_settlement JSON)",
                 "0 rc flips; 0 non-converged", "WP-2")


def dq_38_investable_coherence(ctx):
    vals, ex, on_sell, no_px = {}, [], 0, 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        inv_rows = [r for r in pg.body if pg.s(r, "Final Action").upper() == "INVEST"]
        investable = [r for r in pg.body if pg.s(r, "Investability Status").upper() == "INVESTABLE"]
        s_ = [r for r in inv_rows if pg.s(r, "Recommendation").upper() in SELL_CLASS]
        n_ = [r for r in investable if not ((pg.num(r, "Current Price") or 0) > 0 and (pg.num(r, "Forecast Price 12M") or 0) > 0)]
        vals[p + ":invest_rows"] = len(inv_rows)
        vals[p + ":investable_rows"] = len(investable)
        vals[p + ":invest_on_sell_class"] = len(s_)
        vals[p + ":investable_without_price_or_fp12"] = len(n_)
        vals[p + ":investable_without_roi12"] = sum(1 for r in investable if pg.num(r, "Expected ROI 12M") is None)
        vals[p + ":eligible_dq80_frs70"] = sum(1 for r in inv_rows if (pg.num(r, "Data Quality Score") or 0) >= THRESHOLDS["eligible_dq"]
                                              and (pg.num(r, "Forecast Reliability Score") or 0) >= THRESHOLDS["eligible_frs"])
        ex += [f"{pg.s(r, 'Symbol')} INVEST on {pg.s(r, 'Recommendation')} ({PAGE_SHORT[p]})" for r in s_[:4]]
        ex += [f"{pg.s(r, 'Symbol')} INVESTABLE unpriced ({PAGE_SHORT[p]})" for r in n_[:4]]
        on_sell += len(s_)
        no_px += len(n_)
    if not vals:
        return not_checked("DQ-38", "investable_coherence", "E", "scores", "no market page")
    count = on_sell + no_px
    return check("DQ-38", "investable_coherence", "E", "scores", "FAIL" if count else "PASS", count, vals, ex,
                 "count = INVEST rows with a SELL-class Recommendation + INVESTABLE rows without price > 0 or Forecast Price 12M; "
                 "INVESTABLE without Expected ROI 12M and the DQ>=80/FRS>=70 eligibility funnel are INFO sub-metrics (owner question open)",
                 "0 INVEST on SELL-class; price and forecast present on every INVESTABLE row", "WP-3")


def dq_39_score_discrimination(ctx):
    vals, ex, over = {}, [], 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        for col, key in (("Forecast Reliability Score", "frs"), ("Data Quality Score", "dq")):
            c = collections.Counter(pg.num(r, col) for r in pg.body if pg.num(r, col) is not None)
            if not c:
                continue
            mode, n = c.most_common(1)[0]
            share = _share(n, pg.n)
            vals[f"{p}:{key}_mode"] = mode
            vals[f"{p}:{key}_mode_rows"] = n
            vals[f"{p}:{key}_mode_share"] = share
            if share > THRESHOLDS["score_mode_share"]:
                over += 1
                ex.append(f"{PAGE_SHORT[p]} {key.upper()} {mode:g} on {_pct(n, pg.n):.1f} %")
    if not vals:
        return not_checked("DQ-39", "score_discrimination", "E", "scores", "no market page")
    return check("DQ-39", "score_discrimination", "E", "scores", "WARN" if over else "PASS", over, vals, ex,
                 "count = page x score combinations whose mode value covers > 25 % of the page", "no single FRS / DQ value on > 25 % of a page", "WP-2 / WP-4")


def dq_40_fallback_share(ctx):
    vals, ex, count, over = {}, [], 0, 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        src = collections.Counter(pg.s(r, "Opportunity Source") or "<blank>" for r in pg.body)
        inv = [r for r in pg.body if pg.s(r, "Investability Status").upper() == "INVESTABLE"]
        isrc = collections.Counter(pg.s(r, "Opportunity Source") or "<blank>" for r in inv)
        for k, v in src.most_common(4):
            vals[f"{p}:{k}"] = v
        non_roi = sum(v for k, v in isrc.items() if k != "roi_based")
        vals[p + ":investable_rows"] = len(inv)
        vals[p + ":investable_non_roi"] = non_roi
        vals[p + ":investable_non_roi_share"] = _share(non_roi, len(inv))
        count += non_roi
        if inv and _share(non_roi, len(inv)) > THRESHOLDS["fallback_share_investable"]:
            over += 1
            ex.append(f"{PAGE_SHORT[p]} INVESTABLE fallback {_pct(non_roi, len(inv)):.1f} %")
    if not vals:
        return not_checked("DQ-40", "fallback_share", "E", "scores", "no market page")
    return check("DQ-40", "fallback_share", "E", "scores", "WARN" if over else "PASS", count, vals, ex,
                 "count = INVESTABLE rows whose Opportunity Source != roi_based; WARN if that share > 20 % on any page",
                 "<= 20 % of INVESTABLE on a fallback source", "WP-2")


def dq_41_rank(ctx):
    vals, ex, total = {}, [], 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        ranks = [pg.num(r, "Rank (Overall)") for r in pg.body]
        ranks = [x for x in ranks if x is not None]
        vals[p + ":populated"] = len(ranks)
        vals[p + ":distinct"] = len(set(ranks))
        d = len(ranks) - len(set(ranks))
        total += d
        if d:
            ex.append(f"{PAGE_SHORT[p]} {len(set(ranks))} distinct on {len(ranks)}")
    if not vals:
        return not_checked("DQ-41", "page_rank", "E", "scores", "no market page")
    return check("DQ-41", "page_rank", "E", "scores", "FAIL" if total else "PASS", total, vals, ex,
                 "count = populated Rank (Overall) cells minus distinct values (a page rank has none)", "distinct values = populated rows", "WP-2")


def dq_42_dead_columns(ctx):
    vals, ex, count = {}, [], 0
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        n = pg.n or 1
        shares = {
            "row_source_blank": _share(sum(1 for r in pg.body if not pg.s(r, "Row Source")), n),
            "provider_secondary_blank": _share(sum(1 for r in pg.body if not pg.s(r, "Provider Secondary")), n),
            "peg_blank": _share(sum(1 for r in pg.body if not pg.s(r, "PEG")), n),
            "reco_detail_eq_reco": _share(sum(1 for r in pg.body if pg.s(r, "Recommendation Detail") == pg.s(r, "Recommendation")), n),
            "provider_rating_eq_analyst": _share(sum(1 for r in pg.body if pg.s(r, "Provider Rating") == pg.s(r, "Analyst Rating")), n),
        }
        for k, v in shares.items():
            vals[f"{p}:{k}"] = v
            if v >= 1.0:
                count += 1
                ex.append(f"{PAGE_SHORT[p]} {k} 100 %")
    if not vals:
        return not_checked("DQ-42", "dead_duplicate_columns", "E", "surface", "no market page")
    return check("DQ-42", "dead_duplicate_columns", "E", "surface", "FAIL" if count else "PASS", count, vals, ex,
                 "count = (page, column) findings that are 100 % blank (Row Source, Provider Secondary, PEG) or 100 % identical "
                 "(Recommendation Detail == Recommendation, Provider Rating == Analyst Rating)", "0 dead or duplicate columns", "WP-6")


# --------------------------------------------------------------------------- #
# F - Telemetry and readability
# --------------------------------------------------------------------------- #
def dq_43_warnings_telemetry(ctx):
    vals, ex, total_chars, fail = {}, [], 0, False
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        wi = pg.col("Warnings")
        lens = [len(_s(r[wi])) for r in pg.body] if wi is not None else []
        tcount = [len(tg) for tg in pg.tags]
        mean_len = round(statistics.mean(lens), 1) if lens else 0.0
        mean_tags = round(statistics.mean(tcount), 1) if tcount else 0.0
        vals[p + ":mean_len"] = mean_len
        vals[p + ":mean_tags"] = mean_tags
        vals[p + ":total_chars"] = sum(lens)
        vals[p + ":rows_gt20_tags"] = sum(1 for t in tcount if t > 20)
        total_chars += sum(lens)
        if mean_len > THRESHOLDS["warnings_mean_len"] or mean_tags > THRESHOLDS["warnings_mean_tags"]:
            fail = True
            ex.append(f"{PAGE_SHORT[p]} mean {mean_len} chars / {mean_tags} tags")
    if not vals:
        return not_checked("DQ-43", "warnings_telemetry", "F", "provenance", "no market page")
    return check("DQ-43", "warnings_telemetry", "F", "provenance", "FAIL" if fail else "PASS", total_chars, vals, ex,
                 "count = total Warnings characters per export; mean length / mean tag count per page (tags split on ';')",
                 "mean length <= 200 chars and <= 5 operator tags", "WP-6")


def dq_44_reason_length(ctx):
    vals, ex, over_rows, warn = {}, [], 0, False
    for p in _pages_present(ctx):
        pg = ctx["pages"][p]
        lens = [len(pg.s(r, "Recommendation Reason")) for r in pg.body]
        mean = round(statistics.mean(lens), 1) if lens else 0.0
        gt = sum(1 for v in lens if v > THRESHOLDS["reason_mean_len"])
        vals[p + ":mean_len"] = mean
        vals[p + ":total_chars"] = sum(lens)
        vals[p + ":rows_gt120"] = gt
        over_rows += gt
        if mean > THRESHOLDS["reason_mean_len"]:
            warn = True
            ex.append(f"{PAGE_SHORT[p]} mean {mean}")
    if not vals:
        return not_checked("DQ-44", "reason_length", "F", "provenance", "no market page")
    return check("DQ-44", "reason_length", "F", "provenance", "WARN" if warn else "PASS", over_rows, vals, ex,
                 "count = Recommendation Reason cells longer than 120 chars; WARN when a page mean exceeds 120", "<= 120 chars", "WP-6")


# --------------------------------------------------------------------------- #
# G - Decision surfaces
# --------------------------------------------------------------------------- #
def _top10_selected_observation(rows):
    """Read one unambiguous KPI strip; cash-only portfolio rows are unrelated."""
    cash_labels = {"Entered cash (SAR)", "Deployable (SAR)"}
    blocks = []
    for i, row in enumerate(rows):
        header = [_s(v) for v in row]
        cash_n = sum(h in cash_labels for h in header)
        if not cash_n or not ("Selected" in header or "Passed" in header):
            continue
        populated = [h for h in header if h]
        if (cash_n != 1 or header.count("Selected") != 1 or header.count("Passed") != 1
                or len(populated) != len(set(populated))):
            return None, "ambiguous or incomplete KPI header"
        blocks.append((i, header.index("Selected")))
    if len(blocks) != 1:
        return None, "KPI strip missing" if not blocks else "multiple KPI strips"
    ri, ci = blocks[0]
    if ri + 1 >= len(rows) or ci >= len(rows[ri + 1]):
        return None, "Selected value missing"
    return rows[ri + 1][ci], None


def _selected_exec_count(value):
    """Only execution-seat disclosure proves exec; plain Selected is not proof."""
    if not isinstance(value, str):
        return None
    selected = value.strip()
    if len(selected) > 512:
        return None
    if re.fullmatch(r"0\s*/\s*\d+\s*\(withheld\)", selected, re.I):
        return 0
    match = SELECTED_EXEC_RE.fullmatch(selected)
    if match is None:
        return None
    count = int(match.group("count"))
    limit = match.group("limit")
    qualifiers = re.findall(r"\+\s*(\d+)\s+(fast-track suspended|research|pend(?:ing)?|grace)", selected, re.I)
    labels = [label.casefold().replace("pending", "pend") for _, label in qualifiers]
    if len(labels) != len(set(labels)):
        return None
    if limit is not None and count + sum(int(n) for n, _ in qualifiers) > int(limit):
        return None
    return count


def dq_45_feed_state(ctx):
    t10 = ctx["t10"]
    feed = ctx["status_globals"].get("TFB Decision Feed", "")
    if not t10 and not feed:
        return not_checked("DQ-45", "feed_state", "G", "surface", "Top_10_Investments and the _Status feed global are absent")
    vals, ex = {}, []
    observation, selected_error = _top10_selected_observation(t10)
    exec_n = _selected_exec_count(observation) if selected_error is None else None
    if exec_n is None and selected_error is None:
        selected_error = "Selected value missing or malformed"
    selected = _s(observation) if exec_n is not None else ""
    vals["exec_seats"] = exec_n
    vals["board_seats"] = len(ctx["board"])
    vals["qualified"] = len(ctx["qualified"])
    for key, rx in (("pending", r"(\d+) pend"), ("grace", r"(\d+) grace"), ("suspended", r"(\d+) fast-track suspended")):
        mm = re.search(rx, selected)
        if mm:
            vals[key] = int(mm.group(1))
    state = feed.split("|")[0].strip() if feed else "?"
    bi = _find_row(t10, lambda r: bool(r) and "FEED" in _s(r[0]))
    banner = _s(t10[bi][0])[:60] if bi is not None else ""
    ex = [f"feed {state}", f"selected '{selected}'" if selected else f"Selected unknown: {selected_error}"]
    if banner:
        ex.append(banner.split("(")[0].strip()[:40])
    return check("DQ-45", "feed_state", "G", "surface", "INFO", vals["exec_seats"], vals, ex,
                 f"_Status 'TFB Decision Feed' = {feed[:120]}" if feed else "feed global not found", "reported with reason (INFO)", "-")


def dq_46_pf_block(ctx):
    book = ctx["book"]
    pdr = book.rows("Portfolio_Decision")
    pal = book.rows("_Portfolio_Action_Log")
    if not pdr and not pal:
        return not_checked("DQ-46", "portfolio_block_runs", "G", "book", "Portfolio_Decision and _Portfolio_Action_Log absent")
    vals, ex = {}, []
    aix, acts = _action_table(pdr or [])
    block_rows = sum(1 for r in acts if _s(r[aix["Action"]]) == "BLOCK") if aix else 0
    vals["action_rows"] = len(acts)
    vals["block_rows"] = block_rows
    kpi_blank = kpi_cells = 0
    if pdr:
        ki = _find_row(pdr, lambda r: _s(r[0]) == "Portfolio (SAR)")
        if ki is not None and ki + 1 < len(pdr):
            hdr = [_s(c) for c in pdr[ki]]
            kpi_cells = sum(1 for h in hdr if h)
            kpi_blank = sum(1 for h, v in zip(hdr, pdr[ki + 1]) if h and _blank(v))
    vals["kpi_cells"] = kpi_cells
    vals["kpi_blank"] = kpi_blank
    consecutive = 0
    if pal:
        pix = _header_index(pal)
        li, ai = pix.get("Logged At"), pix.get("Action")
        if li is not None and ai is not None:
            runs = collections.OrderedDict()
            for r in pal[1:]:
                if _blank(r[li]):
                    continue
                key = r[li].strftime("%Y-%m-%d %H:%M") if isinstance(r[li], dt.datetime) else _s(r[li])[:16]
                runs.setdefault(key, []).append(_s(r[ai]))
            for key, actions in reversed(list(runs.items())):
                if actions and all(a == "BLOCK" for a in actions):
                    consecutive += 1
                    ex.append(f"run {key} BLOCK x{len(actions)}")
                else:
                    break
            vals["log_runs"] = len(runs)
    vals["consecutive_block_runs"] = consecutive
    status = "FAIL" if consecutive >= THRESHOLDS["block_runs_fail"] else "WARN" if consecutive >= THRESHOLDS["block_runs_warn"] else "PASS"
    if block_rows and status == "PASS":
        status = "WARN"
    ex.insert(0, f"action table BLOCK {block_rows}/{len(acts)}")
    return check("DQ-46", "portfolio_block_runs", "G", "book", status, consecutive, vals, ex,
                 "count = trailing consecutive _Portfolio_Action_Log runs (keyed by Logged At minute) where every Action == BLOCK; "
                 "action-table BLOCK share and blank KPI strip cells in values", "0 consecutive BLOCK runs", "WP-1")


def dq_47_panel_vs_policy(ctx):
    tea = _tea()
    book = ctx["book"]
    t10, pdr, lc = ctx["t10"], book.rows("Portfolio_Decision") or [], book.rows("_Lists_Config") or []
    if not t10 and not pdr:
        return not_checked("DQ-47", "panel_vs_policy", "G", "surface", "cockpit panels absent")
    vals, ex, drifts = {}, [], 0
    for label, (pkey, tab) in PANEL_LABELS.items():
        rows = t10 if tab == "Top_10_Investments" else pdr
        v = _num(tea._panel_value(rows, label)) if rows else None
        if v is None:
            ex.append(f"{label} not found")
            continue
        vals[pkey + "_panel"] = v
        vals[pkey + "_policy"] = POLICY[pkey]
        if abs(v - POLICY[pkey]) > 1e-9:
            drifts += 1
            ex.append(f"{label} {v:g} vs policy {POLICY[pkey]:g}")
    for label in PANEL_INFO_LABELS:
        v = tea._panel_value(t10, label) if t10 else None
        if v is not None:
            vals["info_" + re.sub(r"\W+", "_", label.lower()).strip("_")] = _num(v) if _num(v) is not None else (1 if _s(v).lower() == "yes" else 0)
    for key in LISTS_DEFAULT_KEYS:
        v = _lists_default(lc, key)
        if v is not None:
            vals["default_" + key.lower()] = _num(v) if _num(v) is not None else (1 if _s(v).lower() == "yes" else 0)
    return check("DQ-47", "panel_vs_policy", "G", "surface", "FAIL" if drifts else "PASS", drifts, vals, ex,
                 "Top_10 'T10: Max Per Sector' and Portfolio_Decision 'PF: Max Sector %' / 'PF: Max Position %' vs POLICY 3 / 40 / 10; "
                 "period, require-investable and _Lists_Config section-4 defaults as INFO sub-values", "0 drift from policy", "WP-8 / WP-12")


def dq_48_cockpit_inputs(ctx):
    cix, grid, footer = ctx["candidates"]
    if cix is None:
        return not_checked("DQ-48", "cockpit_gate_inputs", "G", "surface", "candidate grid anchor 'CANDIDATES' not found", "", "WP-8")
    n = len(grid)
    vals, ex = {"rows": n}, []
    unknown_cells, fail = 0, False
    for col, key in (("News", "news"), ("Sector Trend", "sector_trend")):
        if col not in cix:
            continue
        c = collections.Counter(_s(r[cix[col]]) or "<blank>" for r in grid)
        unk = c.get("Unknown", 0)
        vals[key + "_unknown"] = unk
        vals[key + "_unknown_share"] = _share(unk, n)
        for k, v in c.most_common(3):
            vals[f"{key}_{k.lower().replace(' ', '_')}"] = v
        unknown_cells += unk
        if n and _share(unk, n) > THRESHOLDS["unknown_share"]:
            fail = True
            ex.append(f"{col} Unknown {unk}/{n}")
    if "Conflict" in cix:
        c = collections.Counter(_s(r[cix["Conflict"]]) for r in grid)
        vals["conflict_yes"] = c.get("Yes", 0)
        vals["conflict_no"] = c.get("No", 0)
    if "First Fail" in cix:
        c = collections.Counter(_s(r[cix["First Fail"]]) or "<blank>" for r in grid)
        for k, v in c.most_common(6):
            vals["first_fail_" + re.sub(r"\W+", "_", k.lower()).strip("_")] = v
    if "Verdict" in cix:
        c = collections.Counter(_s(r[cix["Verdict"]]) for r in grid)
        for k, v in c.most_common(3):
            vals["verdict_" + k.lower()] = v
    if footer:
        ex.append("footer: " + footer.split("|")[0].strip()[:30])
    return check("DQ-48", "cockpit_gate_inputs", "G", "surface", "FAIL" if fail else "PASS", unknown_cells, vals, ex,
                 "candidate grid = rows after the CANDIDATES header until the first blank Verdict or a Symbol containing '|' (footer); "
                 "count = Unknown News + Unknown Sector Trend cells", "Unknown <= 20 % of candidates", "WP-8")


def dq_49_calendar_coverage(ctx):
    cal = ctx["book"].rows("Calendar_Events")
    if not cal:
        return not_checked("DQ-49", "calendar_coverage", "G", "surface", "Calendar_Events not in export", "", "WP-8")
    cix = _header_index(cal)
    si, ei, xi = cix.get("Symbol"), cix.get("Next Earnings Date"), cix.get("Next Ex-Div Date")
    if si is None:
        return not_checked("DQ-49", "calendar_coverage", "G", "surface", "Calendar_Events has no Symbol column", "", "WP-8")
    body = [r for r in cal[1:] if _s(r[si])]
    dated = {_s(r[si]) for r in body if (ei is not None and not _blank(r[ei])) or (xi is not None and not _blank(r[xi]))}
    shells = sum(1 for r in body if _s(r[si]) not in dated)
    cohort = ctx["cohort_calendar"]
    covered = cohort & dated
    vals = {"calendar_rows": len(body), "dated_rows": len(dated), "shell_rows": shells,
            "earnings_rows": sum(1 for r in body if ei is not None and not _blank(r[ei])),
            "exdiv_rows": sum(1 for r in body if xi is not None and not _blank(r[xi])),
            "cohort": len(cohort), "covered": len(covered), "coverage_share": _share(len(covered), len(cohort)),
            "invest_total": len(ctx["invest"]), "invest_covered": len(ctx["invest"] & dated),
            "board_total": len(set(ctx["board"])), "board_covered": len(set(ctx["board"]) & dated),
            "holdings_total": len(set(ctx["holdings"])), "holdings_covered": len(set(ctx["holdings"]) & dated)}
    share = _share(len(covered), len(cohort)) if cohort else 1.0
    status = "FAIL" if share < THRESHOLDS["calendar_fail"] else "WARN" if share < THRESHOLDS["calendar_warn"] else "PASS"
    missing = sorted(cohort - dated)
    return check("DQ-49", "calendar_coverage", "G", "surface", status, len(missing), vals,
                 [f"{s} uncovered" for s in missing[:8]],
                 "count = (INVEST u board u holdings) symbols without a dated Calendar_Events row; coverage share in values",
                 "100 % of INVEST + board + holdings covered (FAIL < 80 %, WARN < 95 %)", "WP-8")


def dq_50_cockpit_alerts(ctx):
    t10 = ctx["t10"]
    ai = _find_row(t10, lambda r: _s(r[0]).startswith("ALERTS"))
    if ai is None:
        return not_checked("DQ-50", "cockpit_alerts", "G", "surface", "Top_10 'ALERTS' section not found", "", "WP-8")
    vals, ex = {}, []
    for r in t10[ai + 2:]:
        typ = _s(r[0])
        if not typ or typ.startswith(("DATA GAPS", "CANDIDATES", "NEAR MISS", "ALL QUALIFIED")):
            break
        n = _num(r[1]) if len(r) > 1 else None
        vals[re.sub(r"\W+", "_", typ.lower()).strip("_")] = n if n is not None else 0
    missing_fx = int(vals.get("missing_fx", 0) or 0)
    if missing_fx:
        ex.append(f"missing_fx {missing_fx}")
    return check("DQ-50", "cockpit_alerts", "G", "surface", "FAIL" if missing_fx else "PASS", missing_fx, vals, ex,
                 "ALERTS table (Type, Count); count = missing_fx alert count; other classes INFO", "0 open missing_fx alerts", "WP-8")


def dq_51_weight_basis(ctx):
    mp = ctx["portfolio"]
    if not mp:
        return not_checked("DQ-51", "weight_basis", "G", "book", "My_Portfolio not in export", "", "WP-1")
    vals, ex = {"holdings": mp.n}, []
    for col in ("Actual Weight %", "Target Weight %", "Weight Gap", "Rebalance Action"):
        b = sum(1 for r in mp.body if not mp.s(r, col))
        vals[re.sub(r"\W+", "_", col.lower()).strip("_") + "_blank"] = b
    aw = [mp.num(r, "Actual Weight %") for r in mp.body]
    aw = [v for v in aw if v is not None]
    basis = "blank"
    if aw:
        tot = sum(aw)
        basis = "holdings_basis_inferred" if 95 <= tot <= 105 or 0.95 <= tot <= 1.05 else "nav_basis_inferred"
    tgt_blank = vals["target_weight_blank"]
    ex.append(f"basis {basis}")
    return check("DQ-51", "weight_basis", "G", "book", "FAIL" if mp.n and tgt_blank == mp.n else "PASS", tgt_blank, vals, ex,
                 "count = holdings with a blank Target Weight %; blanks per weight column; basis inferred from the Actual Weight % sum; "
                 "the basis LABEL is not a cell in the export (NOT_CHECKED sub-check NC-04)", "weights labelled and NAV-consistent", "WP-1")


def dq_52_investable_view(ctx):
    iv = ctx["investable_view"]
    if not iv:
        return not_checked("DQ-52", "investable_view", "G", "surface", "'Investable Symbol' tab not in export", "", "WP-8")
    shown, invest = set(iv.syms), ctx["invest"]
    missing, extra = sorted(invest - shown), sorted(shown - invest)
    count = abs(iv.n - len(invest)) + len(missing) + len(extra)
    vals = {"view_rows": iv.n, "invest_rows": len(invest), "missing_from_view": len(missing), "extra_in_view": len(extra),
            "view_grid_rows": iv.grid_rows}
    return check("DQ-52", "investable_view", "G", "surface", "FAIL" if count else "PASS", count, vals,
                 [f"missing {s}" for s in missing[:4]] + [f"extra {s}" for s in extra[:4]],
                 "'Investable Symbol' live rows vs sum of Final Action == INVEST rows, set differences both ways; "
                 "formula ranges NOT_CHECKED (data_only read, NC-03)", "view = sum of page INVEST rows and consistent with its formula", "WP-8")


def dq_53_brackets(ctx):
    pdr = ctx["book"].rows("Portfolio_Decision")
    if not pdr:
        return not_checked("DQ-53", "holding_brackets", "G", "book", "Portfolio_Decision not in export", "", "WP-1")
    aix, acts = _action_table(pdr)
    if aix is None:
        return not_checked("DQ-53", "holding_brackets", "G", "book", "Portfolio_Decision action table not found", "", "WP-1")
    vals, ex = {"holdings": len(acts)}, []
    cols = {"Stop SAR": "stop_blank", "TP1 SAR": "tp1_blank", "TP2 SAR": "tp2_blank", "MV SAR": "mv_blank", "Weight %": "weight_blank"}
    any_blank = 0
    for r in acts:
        if any(c in aix and _blank(r[aix[c]]) for c in ("Stop SAR", "TP1 SAR", "TP2 SAR")):
            any_blank += 1
            ex.append(f"{_s(r[aix['Symbol']])} bracket blank")
    for c, k in cols.items():
        if c in aix:
            vals[k] = sum(1 for r in acts if _blank(r[aix[c]]))
    return check("DQ-53", "holding_brackets", "G", "book", "FAIL" if any_blank else "PASS", any_blank, vals, ex,
                 "count = action rows with any blank stop / tp1 / tp2 bracket cell; per-column blanks in values",
                 "every holding shows stop / TP / bracket-live (100 %)", "WP-1")


# --------------------------------------------------------------------------- #
# H - Portfolio, ledger, cash
# --------------------------------------------------------------------------- #
def dq_54_placeholder_dates(ctx):
    lix, lots = _ledger(ctx)
    if lix is None:
        return not_checked("DQ-54", "ledger_placeholder_dates", "H", "book", "_Portfolio_CostBasis ledger table not found", "", "WP-9")
    hits = [_s(r[lix["Symbol"]]) for r in lots if _local_date(r[lix.get("Buy Date", 4)]) in PLACEHOLDER_DATES]
    return check("DQ-54", "ledger_placeholder_dates", "H", "book", "FAIL" if hits else "PASS", len(hits),
                 {"lots": len(lots)}, hits, "lots whose Buy Date LOCAL calendar date is a placeholder (2026-01-01); never tz-converted",
                 "0 unflagged placeholder dates", "WP-9")


def dq_55_fees(ctx):
    lix, lots = _ledger(ctx)
    if lix is None:
        return not_checked("DQ-55", "ledger_fees", "H", "book", "_Portfolio_CostBasis ledger table not found", "", "WP-9")
    sold = [r for r in lots if not _blank(r[lix["Sell Date"]])] if "Sell Date" in lix else []
    buy_ok = sum(1 for r in lots if "Buy Fees" in lix and not _blank(r[lix["Buy Fees"]]))
    sell_ok = sum(1 for r in sold if "Sell Fees" in lix and not _blank(r[lix["Sell Fees"]]))
    missing = (len(lots) - buy_ok) + (len(sold) - sell_ok)
    vals = {"lots": len(lots), "buy_fees_populated": buy_ok, "sold_lots": len(sold), "sell_fees_populated": sell_ok}
    return check("DQ-55", "ledger_fees", "H", "book", "FAIL" if missing else "PASS", missing, vals, [],
                 "count = lots without Buy Fees + sold lots without Sell Fees", "100 % fee fields captured (actual or flagged estimate)", "WP-9")


def dq_56_lot_fx(ctx):
    lix, lots = _ledger(ctx)
    if lix is None:
        return not_checked("DQ-56", "ledger_fx_band", "H", "book", "_Portfolio_CostBasis ledger table not found", "", "WP-9")
    fxc = next((h for h in lix if h.replace("→", "->") == "FX->SAR" or h == "FX→SAR"), None)
    usd = [r for r in lots if "Ccy" in lix and _s(r[lix["Ccy"]]) == "USD"]
    out = []
    for r in usd:
        v = _num(r[lix[fxc]]) if fxc else None
        if v is not None and not (FX_BAND[0] <= v <= FX_BAND[1]):
            out.append(f"{_s(r[lix['Symbol']])} {v:.6g}")
    vals = {"usd_lots": len(usd), "fx_blank": sum(1 for r in usd if fxc is None or _num(r[lix[fxc]]) is None)}
    return check("DQ-56", "ledger_fx_band", "H", "book", "FAIL" if out else "PASS", len(out), vals, out,
                 "USD lots whose recorded FX->SAR is outside [3.74, 3.77]", "0 outside the band", "WP-9")


def dq_57_cash_snapshot(ctx):
    tea = _tea()
    book = ctx["book"]
    rows = book.rows("_Cash_Snapshot")
    if not rows:
        return not_checked("DQ-57", "cash_snapshot_contract", "H", "book", "_Cash_Snapshot not in export", "", "WP-1")
    cix = _header_index(rows)
    body = [(i + 1, r) for i, r in enumerate(rows[1:]) if any(not _blank(v) for v in r)]
    if not body or "Date" not in cix:
        return not_checked("DQ-57", "cash_snapshot_contract", "H", "book", "_Cash_Snapshot has no records", "", "WP-1")
    def col(r, name):
        i = cix.get(name)
        return None if i is None or i >= len(r) else r[i]
    snaps = [r for _, r in body if "SNAPSHOT" in _s(col(r, "Type")).upper()]
    time_blank = sum(1 for _, r in body if "Time" not in cix or _blank(col(r, "Time")))
    delta_blank = sum(1 for _, r in body if "Delta vs Prev" not in cix or _blank(col(r, "Delta vs Prev")))
    amount_blank = sum(1 for r in snaps if "Amount SAR" not in cix or _blank(col(r, "Amount SAR")))
    dates = collections.Counter(_local_date(col(r, "Date")) for _, r in body)
    dup_dates = sorted(d for d, n in dates.items() if d and n > 1)
    same = same_chg = 0
    for (_, a), (_, b) in zip(body, body[1:]):
        if _s(col(b, "Note")) and _s(col(a, "Note")) == _s(col(b, "Note")):
            same += 1
            if _num(col(a, "Balance SAR")) != _num(col(b, "Balance SAR")):
                same_chg += 1
    last_i, last = body[-1]
    date_only = {name for name, ci in cix.items() if (last_i, ci) in book.cash_date_only_cells}
    record = tea._cash_snapshot_record(cix, last, ctx["tz_offset"], date_only)
    _, errors = record.certify(ctx["ref_time_log"], THRESHOLDS["cash_max_age_hours"], ctx["tz_offset"])
    vals = {"records": len(body), "snapshot_rows": len(snaps), "time_blank": time_blank, "delta_blank": delta_blank,
            "amount_blank_on_snapshots": amount_blank, "stale_note_rows": same_chg, "identical_note_rows": same,
            "duplicate_dates": len(dup_dates), "certification_errors": len(errors)}
    count = time_blank + delta_blank + amount_blank + same_chg + len(dup_dates) + len(errors)
    ex = [f"dup date {d}" for d in dup_dates[:3]] + list(errors)
    return check("DQ-57", "cash_snapshot_contract", "H", "book", "FAIL" if count else "PASS", count, vals, ex,
                 "count = blank Time + blank Delta + blank Amount on SNAPSHOT rows + identical-note-while-balance-changed rows + "
                 "duplicate dates + certification error names of the LAST record (tfb_export_audit CashSnapshot.certify at "
                 "ref_time_log); amounts are never emitted", "every row carries time, account, native amount, delta; last record certifies", "WP-1")


def dq_58_dividends(ctx):
    rows = ctx["book"].rows("_Dividend_Log")
    if not rows:
        return not_checked("DQ-58", "dividend_log_duplicates", "H", "book", "_Dividend_Log not in export", "", "WP-9")
    dix = _header_index(rows)
    body = [r for r in rows[1:] if any(not _blank(v) for v in r)]
    if not all(k in dix for k in ("Symbol", "Ex-Date", "Amount")):
        return not_checked("DQ-58", "dividend_log_duplicates", "H", "book", "_Dividend_Log header lacks Symbol / Ex-Date / Amount", "", "WP-9")
    keys = collections.Counter((_s(r[dix["Symbol"]]), _local_date(r[dix["Ex-Date"]]), _num(r[dix["Amount"]])) for r in body)
    dups = [(k, n) for k, n in keys.items() if n > 1]
    lix, lots = _ledger(ctx)
    reconciled = None
    if lix is not None and "Dividends Recv" in lix:
        by_sym = collections.defaultdict(float)
        for r in body:
            by_sym[_s(r[dix["Symbol"]])] += _num(r[dix["Amount"]]) or 0.0
        led = collections.defaultdict(float)
        for r in lots:
            led[_s(r[lix["Symbol"]])] += _num(r[lix["Dividends Recv"]]) or 0.0
        reconciled = all(abs(by_sym[s] - led.get(s, 0.0)) <= 0.01 for s in by_sym)
    vals = {"rows": len(body), "duplicate_keys": len(dups)}
    return check("DQ-58", "dividend_log_duplicates", "H", "book", "FAIL" if dups else "PASS", sum(n - 1 for _, n in dups), vals,
                 [f"{k[0]} {k[1]} x{n}" for k, n in dups] + ([f"ledger_reconciled={reconciled}"] if reconciled is not None else []),
                 "duplicate (Symbol, Ex-Date, Amount) keys; per-symbol sums vs ledger 'Dividends Recv' reported as a boolean only",
                 "0 duplicates; sums reconcile to the ledger", "WP-9")


def dq_59_trade_notes(ctx):
    lix, lots = _ledger(ctx)
    tn = ctx["book"].rows("_Trade_Notes")
    if lix is None or tn is None:
        return not_checked("DQ-59", "trade_notes_vs_fills", "H", "book", "ledger or _Trade_Notes not in export", "", "WP-9")
    fills = 0
    for r in lots:
        if (_local_date(r[lix["Buy Date"]]) or "") >= PROGRAM_V2_START:
            fills += 1
        if "Sell Date" in lix and (_local_date(r[lix["Sell Date"]]) or "") >= PROGRAM_V2_START:
            fills += 1
    notes = sum(1 for r in tn[1:] if _s(r[0]))
    vals = {"fills_since_program_v2": fills, "notes": notes}
    return check("DQ-59", "trade_notes_vs_fills", "H", "book", "FAIL" if notes < fills else "PASS", max(fills - notes, 0), vals, [],
                 f"fill events (buy or sell LOCAL date >= {PROGRAM_V2_START}) vs _Trade_Notes rows with a Note ID", "notes >= fills", "WP-9")


def dq_60_nav_reconcile(ctx):
    lix, lots = _ledger(ctx)
    cash_rows = ctx["book"].rows("_Cash_Snapshot")
    mp = ctx["portfolio"]
    if lix is None or not cash_rows or not mp:
        return not_checked("DQ-60", "nav_reconcile", "H", "book", "ledger, _Cash_Snapshot or My_Portfolio absent", "", "-")
    mvc = "Market Value SAR"
    active = [r for r in lots if _s(r[lix["Status"]]).lower() == "active"]
    ledger_mv = sum(_num(r[lix[mvc]]) or 0.0 for r in active) if mvc in lix else None
    px = {s: mp.num(r, "Current Price") for s, r in zip(mp.syms, mp.body)}
    recomputed, unpriced = 0.0, 0
    for r in active:
        p_ = px.get(_s(r[lix["Symbol"]]))
        fx = FX_TO_SAR.get(_s(r[lix["Ccy"]])) if "Ccy" in lix else None
        sh = _num(r[lix["Shares"]]) if "Shares" in lix else None
        if p_ is None or fx is None or sh is None:
            unpriced += 1
            continue
        recomputed += sh * p_ * fx
    cix = _header_index(cash_rows)
    cbody = [r for r in cash_rows[1:] if any(not _blank(v) for v in r)]
    cash = _num(cbody[-1][cix["Balance SAR"]]) if cbody and "Balance SAR" in cix else None
    nav_txt = None
    ai = _find_row(ctx["t10"], lambda r: _s(r[0]).startswith("ALERTS"))
    if ai is not None:
        for r in ctx["t10"][ai:ai + 12]:
            m = NAV_TEXT_RE.search(" ".join(_s(v) for v in r[:4]))
            if m:
                nav_txt = float(m.group(1).replace(",", ""))
                break
    tol = THRESHOLDS["nav_tolerance"]
    ledger_ok = (ledger_mv is not None and unpriced == 0 and abs(recomputed - ledger_mv) <= tol)
    nav_ok = None
    if nav_txt is not None and cash is not None and ledger_mv is not None:
        nav_ok = abs(ledger_mv + cash - nav_txt) <= tol
    bucket = "n/a" if nav_ok is None else ("<=1" if nav_ok else ">1")
    vals = {"active_lots": len(active), "unpriced_lots": unpriced}
    ex = [f"nav_within_tolerance={nav_ok}", f"ledger_mv_recomputed_within_tolerance={ledger_ok}", f"delta_bucket={bucket}",
          f"cockpit_nav_text_present={nav_txt is not None}"]
    status = "PASS" if nav_ok else ("NOT_CHECKED" if nav_ok is None else "FAIL")
    return check("DQ-60", "nav_reconcile", "H", "book", status, (0 if nav_ok else 1) if nav_ok is not None else None, vals, ex,
                 "ledger active market value + last cash balance vs the cockpit NAV text, reduced to booleans and a bucket "
                 "(amounts are read for computation only and never emitted)", "NAV reconciles within tolerance", "-")


# --------------------------------------------------------------------------- #
# I - Evidence logs and research stores
# --------------------------------------------------------------------------- #
def dq_61_signal_history_keys(ctx):
    rows = ctx["book"].rows("Signal_History")
    if not rows:
        return not_checked("DQ-61", "signal_history_keys", "I", "store", "Signal_History not in export", "", "WP-7")
    ix = _header_index(rows)
    if "Symbol" not in ix or "Date (Riyadh)" not in ix:
        return not_checked("DQ-61", "signal_history_keys", "I", "store", "Signal_History header lacks Symbol / Date (Riyadh)", "", "WP-7")
    body = [r for r in rows[1:] if _s(r[ix["Symbol"]])]
    pairs = collections.Counter((_s(r[ix["Symbol"]]), _local_date(r[ix["Date (Riyadh)"]])) for r in body)
    dups = len(body) - len(pairs)
    vals = {"rows": len(body), "pairs": len(pairs), "max_rows_per_pair": max(pairs.values()) if pairs else 0,
            "distinct_keys": len({_s(r[ix["Key"]]) for r in body}) if "Key" in ix else 0}
    return check("DQ-61", "signal_history_keys", "I", "store", "FAIL" if dups else "PASS", dups, vals,
                 [f"max {vals['max_rows_per_pair']} rows per symbol-day"], "rows minus distinct (Symbol, Date (Riyadh) date part) pairs",
                 "key unique per symbol-day (0 duplicates)", "WP-7")


def _perf_log(ctx):
    book = ctx["book"]
    rows = book.rows("Performance_Log")
    if not rows:
        return None, []
    ix, body = book.table("Performance_Log", header_pred=lambda r: _s(r[0]) == "Record ID")
    if not ix:
        return None, []
    recs = [r for r in body if len(_s(r[0])) == 36]
    return ix, recs


def dq_62_perf_units(ctx):
    ix, recs = _perf_log(ctx)
    if ix is None:
        return not_checked("DQ-62", "performance_log_units", "I", "store", "Performance_Log 'Record ID' table not found", "", "WP-7")
    lim = THRESHOLDS["perf_unit_abs"]
    tr = [_num(r[ix["Target ROI %"]]) for r in recs if "Target ROI %" in ix]
    tr = [v for v in tr if v is not None]
    mat = [r for r in recs if "Status" in ix and _s(r[ix["Status"]]).lower() == "matured"]
    rr = [_num(r[ix["Realized ROI %"]]) for r in mat if "Realized ROI %" in ix]
    rr = [v for v in rr if v is not None]
    t_gt = sum(1 for v in tr if abs(v) > lim)
    r_gt = sum(1 for v in rr if abs(v) > lim)
    target_fraction = tr and (len(tr) - t_gt) > len(tr) / 2
    realized_points = rr and r_gt > len(rr) / 2
    mixed = bool(target_fraction and realized_points)
    vals = {"records": len(recs), "matured": len(mat), "target_le_1_5": len(tr) - t_gt, "target_gt_1_5": t_gt,
            "realized_gt_1_5": r_gt, "realized_le_1_5": len(rr) - r_gt, "mixed_units": 1 if mixed else 0}
    status = "FAIL" if (mixed or t_gt) else "PASS"
    return check("DQ-62", "performance_log_units", "I", "store", status, t_gt, vals,
                 ["target column fraction, realized column points" if mixed else "units consistent"],
                 "count = Target ROI % records with |v| > 1.5; FAIL also when a majority-fraction target column sits beside a "
                 "majority-points realized column", "one unit per % column (all points or all fractions)", "WP-7")


def dq_63_perf_dead_columns(ctx):
    ix, recs = _perf_log(ctx)
    if ix is None:
        return not_checked("DQ-63", "performance_log_dead_columns", "I", "store", "Performance_Log 'Record ID' table not found", "", "WP-7")
    dead = [c for c in ("Volatility", "Max Drawdown %", "Sharpe Ratio") if c in ix and all((_num(r[ix[c]]) or 0) == 0 for r in recs)]
    fe = sum(1 for r in recs if "Factor Exposures" in ix and _s(r[ix["Factor Exposures"]]) == "{}")
    mat = [r for r in recs if "Status" in ix and _s(r[ix["Status"]]).lower() == "matured"]
    kc = collections.Counter(_s(r[ix["Key"]]) for r in mat) if "Key" in ix else collections.Counter()
    dups = sum(n - 1 for n in kc.values() if n > 1)
    hold = sum(1 for r in recs if "Entry Recommendation" in ix and _s(r[ix["Entry Recommendation"]]).upper() == "HOLD")
    vals = {"records": len(recs), "dead_columns": len(dead), "factor_exposures_empty": fe,
            "factor_exposures_empty_share": _share(fe, len(recs)), "duplicate_matured_keys": dups, "hold_entry_rows": hold}
    return check("DQ-63", "performance_log_dead_columns", "I", "store", "FAIL" if (dead or dups) else "PASS", len(dead) + dups, vals,
                 [f"dead {c}" for c in dead] + ([f"duplicate matured keys {dups}"] if dups else []),
                 "count = all-zero risk columns + duplicate matured Keys; Factor Exposures '{}' share and HOLD-entry rows in values",
                 "0 dead columns, 0 duplicate keys", "WP-7")


def dq_64_win_vs_target(ctx):
    ix, recs = _perf_log(ctx)
    if ix is None:
        return not_checked("DQ-64", "win_vs_target_hit", "I", "store", "Performance_Log 'Record ID' table not found", "", "WP-7")
    mat = [r for r in recs if "Status" in ix and _s(r[ix["Status"]]).lower() == "matured"]
    oc = collections.Counter(_s(r[ix["Outcome"]]).upper() for r in mat) if "Outcome" in ix else collections.Counter()
    hit = chk = 0
    for r in mat:
        t, rz = _num(r[ix.get("Target ROI %", -1)]) if "Target ROI %" in ix else None, _num(r[ix["Realized ROI %"]]) if "Realized ROI %" in ix else None
        if t is None or rz is None:
            continue
        chk += 1
        if rz / 100.0 >= t:
            hit += 1
    inv3 = [r for r in mat if "Entry Investability" in ix and _s(r[ix["Entry Investability"]]).upper() == "INVESTABLE"
            and "Horizon" in ix and _s(r[ix["Horizon"]]) == "3M"]
    w3 = sum(1 for r in inv3 if _s(r[ix["Outcome"]]).upper() == "WIN")
    l3 = sum(1 for r in inv3 if _s(r[ix["Outcome"]]).upper() == "LOSS")
    wins = oc.get("WIN", 0)
    vals = {"matured": len(mat), "win": wins, "loss": oc.get("LOSS", 0), "breakeven": oc.get("BREAKEVEN", 0) + oc.get("BE", 0),
            "target_hit": hit, "target_hit_checked": chk, "target_hit_share": _share(hit, chk),
            "win_rate_sign_share": _share(wins, wins + oc.get("LOSS", 0)), "investable_3m_win": w3, "investable_3m_loss": l3,
            "investable_3m_win_share": _share(w3, w3 + l3)}
    has_col = "Target Hit" in ix
    vals["target_hit_column_present"] = 1 if has_col else 0
    return check("DQ-64", "win_vs_target_hit", "I", "store", "PASS" if has_col else "WARN", abs(wins - hit), vals,
                 [f"WIN {wins} vs target-hit {hit} of {chk}"],
                 "count = |sign-based WIN - target-hit (Realized ROI % / 100 >= Target ROI %)| on matured records; WARN until a "
                 "Target Hit column exists (WP-7)", "WIN and target-hit both reported", "WP-7")


def dq_65_outcomes_overdue(ctx):
    book = ctx["book"]
    sl = book.rows("_Selection_Log")
    pal = book.rows("_Portfolio_Action_Log")
    if not sl and not pal:
        return not_checked("DQ-65", "outcomes_overdue", "I", "store", "_Selection_Log and _Portfolio_Action_Log absent", "", "WP-7")
    vals, overdue = {}, 0
    ref = ctx["ref_time_log"]
    if sl:
        ix = _header_index(sl)
        body = [r for r in sl[1:] if any(not _blank(v) for v in r)]
        filled = sum(1 for r in body if "Outcome" in ix and _s(r[ix["Outcome"]]))
        od = 0
        for r in body:
            if "Outcome" in ix and _s(r[ix["Outcome"]]):
                continue
            rb = _parse_dt(r[ix["Review By"]], ctx["tz_offset"]) if "Review By" in ix else None
            if rb and ref and rb < ref:
                od += 1
        vals["selection_rows"] = len(body)
        vals["selection_outcome_filled"] = filled
        vals["selection_overdue"] = od
        overdue += od
    if pal:
        ix = _header_index(pal)
        body = [r for r in pal[1:] if any(not _blank(v) for v in r)]
        vals["action_log_rows"] = len(body)
        vals["action_log_outcome_filled"] = sum(1 for r in body if "Outcome" in ix and _s(r[ix["Outcome"]]))
    return check("DQ-65", "outcomes_overdue", "I", "store", "FAIL" if overdue else "PASS", overdue, vals, [],
                 "_Selection_Log rows with blank Outcome and Review By < ref_time_log; _Portfolio_Action_Log outcome fill", "0 overdue", "WP-7")


def dq_66_pit_numeric(ctx):
    rows = ctx["book"].rows("_PIT_Fundamentals")
    if not rows:
        return not_checked("DQ-66", "pit_numeric", "I", "store", "_PIT_Fundamentals not in export", "", "WP-7")
    ix = _header_index(rows)
    body = [r for r in rows[1:] if any(not _blank(v) for v in r)]
    if not body or "Symbol" not in ix:
        return not_checked("DQ-66", "pit_numeric", "I", "store", "_PIT_Fundamentals empty", "", "WP-7")
    def numeric_share(col):
        if col not in ix:
            return None
        return _share(sum(1 for r in body if isinstance(r[ix[col]], (int, float)) and not isinstance(r[ix[col]], bool)), len(body))
    vals = {"rows": len(body)}
    for col, key in (("Market Cap", "market_cap"), ("Price", "price"), ("P/E", "pe")):
        sh = numeric_share(col)
        if sh is not None:
            vals[key + "_numeric_share"] = sh
    glyph = sum(1 for r in body for c in PIT_ROI_COLS if c in ix and any(g in _s(r[ix[c]]) for g in GLYPHS))
    non_numeric_rows = junk = 0
    flagged = set()
    for i, r in enumerate(body):
        if any(c in ix and not _blank(r[ix[c]]) and not isinstance(r[ix[c]], (int, float)) for c in ("Market Cap", "Price", "P/E")):
            non_numeric_rows += 1
            flagged.add(i)
        sym = _s(r[ix["Symbol"]])
        width = sum(1 for v in r if not _blank(v))
        if sym.upper() in SECTION_ARTIFACTS or re.fullmatch(r"\d+(\.\d+)?", sym) or width < THRESHOLDS["pit_min_width"]:
            junk += 1
            flagged.add(i)
    legacy = sum(1 for r in body if "Forecast Reliability" in ix and _tea()._looks_like_timestamp(r[ix["Forecast Reliability"]]))
    dates = collections.Counter(_local_date(r[ix["Snapshot Date"]]) for r in body) if "Snapshot Date" in ix else collections.Counter()
    vals.update({"glyph_cells": glyph, "junk_rows": junk, "legacy_timestamp_rows": legacy, "non_numeric_rows": non_numeric_rows,
                 "snapshot_dates": len([d for d in dates if d]),
                 "rows_legacy_window": sum(n for d, n in dates.items() if d and PIT_LEGACY_WINDOW[0] <= d <= PIT_LEGACY_WINDOW[1])})
    count = len(flagged)
    fail = any(v < 1.0 for k, v in vals.items() if k.endswith("_numeric_share")) or junk > 0
    return check("DQ-66", "pit_numeric", "I", "store", "FAIL" if fail else "PASS", count, vals,
                 [f"glyph cells {glyph}", f"junk rows {junk}"],
                 "count = rows that are junk (section artifact, numeric Symbol, < 16 populated cells) or carry a non-numeric Market Cap / "
                 "Price / P/E; glyph ROI cells and legacy timestamp rows in values", "100 % numeric; 0 junk rows", "WP-7")


def dq_67_hypotheses(ctx):
    book = ctx["book"]
    rows = book.rows("Hypothesis_Registry")
    if not rows:
        return not_checked("DQ-67", "hypothesis_registry", "I", "store", "Hypothesis_Registry not in export", "", "WP-7")
    ix, body = book.table("Hypothesis_Registry", header_pred=lambda r: _s(r[0]) == "Hypothesis ID")
    if not ix:
        return not_checked("DQ-67", "hypothesis_registry", "I", "store", "'Hypothesis ID' header not found", "", "WP-7")
    hyps = [r for r in body if _s(r[0])]
    zero = sum(1 for r in hyps if "Sample Size" in ix and (_num(r[ix["Sample Size"]]) or 0) == 0)
    insuff = sum(1 for r in hyps if "Status" in ix and _s(r[ix["Status"]]).upper() == "INSUFFICIENT_DATA")
    vals = {"hypotheses": len(hyps), "sample_zero": zero, "insufficient_data": insuff}
    return check("DQ-67", "hypothesis_registry", "I", "store", "FAIL" if hyps and zero == len(hyps) else "PASS", zero, vals, [],
                 "hypotheses with Sample Size == 0 (FAIL when all of them)", "sample > 0 or documented", "WP-7")


def dq_68_regret_forks(ctx):
    rows = ctx["book"].rows("Regret_Ledger")
    if not rows:
        return not_checked("DQ-68", "regret_forks", "I", "store", "Regret_Ledger not in export", "", "WP-7")
    ix = _header_index(rows)
    body = [r for r in rows[1:] if any(not _blank(v) for v in r)]
    if "Fork" not in ix:
        return not_checked("DQ-68", "regret_forks", "I", "store", "Regret_Ledger has no Fork column", "", "WP-7")
    forks = collections.Counter(_s(r[ix["Fork"]]) for r in body)
    open_cb = sum(1 for r in body if _s(r[ix["Fork"]]) == "COMPLIANCE_BLOCK" and ("Scored On" not in ix or _blank(r[ix["Scored On"]])))
    vals = {"forks": len(body), "compliance_block_open": open_cb,
            "scored_on_blank": sum(1 for r in body if "Scored On" not in ix or _blank(r[ix["Scored On"]]))}
    for k, v in forks.most_common(4):
        vals["fork_" + k.lower()] = v
    return check("DQ-68", "regret_forks", "I", "store", "WARN" if open_cb else "PASS", open_cb, vals, [],
                 "Regret_Ledger rows with Fork == COMPLIANCE_BLOCK and blank Scored On (retired constraint still open)", "0 open", "WP-7")


def dq_69_saudi_regime(ctx):
    rows = ctx["book"].rows("Regime_History")
    if not rows:
        return not_checked("DQ-69", "saudi_regime", "I", "store", "Regime_History not in export", "", "WP-7")
    ix = _header_index(rows)
    if "Sleeve" not in ix or "State" not in ix:
        return not_checked("DQ-69", "saudi_regime", "I", "store", "Regime_History header lacks Sleeve / State", "", "WP-7")
    sa = [r for r in rows[1:] if _s(r[ix["Sleeve"]]) == "Saudi"]
    unk = [r for r in sa if _s(r[ix["State"]]).upper() == "UNKNOWN"]
    di = ix.get("Date (Riyadh)")
    days = {_local_date(r[di]) for r in sa} if di is not None else set()
    udays = {_local_date(r[di]) for r in unk} if di is not None else set()
    vals = {"saudi_rows": len(sa), "unknown_rows": len(unk), "unknown_share": _share(len(unk), len(sa)),
            "days": len(days), "unknown_days": len(udays)}
    status = "FAIL" if sa and _share(len(unk), len(sa)) > THRESHOLDS["regime_unknown_share"] else "PASS"
    return check("DQ-69", "saudi_regime", "I", "store", status, len(unk), vals, [], "Regime_History Sleeve == Saudi: UNKNOWN rows / rows (distinct days in values)",
                 "UNKNOWN <= 10 %", "WP-7")


def _runlog(ctx):
    rows = ctx["book"].rows("_Run_Log")
    if not rows:
        return None, []
    ix = _header_index(rows)
    body = [r for r in rows[1:] if any(not _blank(v) for v in r)]
    return ix, body


def _stamp_kind(v):
    if isinstance(v, dt.datetime):
        return "datetime"
    s = _s(v)
    if re.match(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)$", s):
        return "iso_tz_string"
    if re.match(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}", s):
        return "iso_naive_string"
    return "unparseable"


def dq_70_runlog_hygiene(ctx):
    ix, body = _runlog(ctx)
    if ix is None:
        return not_checked("DQ-70", "runlog_hygiene", "I", "ops", "_Run_Log not in export", "", "WP-10")
    ti, li, ei, hi = ix.get("Timestamp", 0), ix.get("Level"), ix.get("Endpoint"), ix.get("HTTP Code")
    kinds = collections.Counter(_stamp_kind(r[ti]) for r in body)
    levels = collections.Counter(_s(r[li]).upper() for r in body if li is not None and _s(r[li]))
    fam = collections.defaultdict(set)
    for lv in levels:
        fam[SEVERITY_FAMILIES.get(lv, lv.lower())].add(lv)
    dup_spellings = sum(len(v) - 1 for v in fam.values() if len(v) > 1)
    ep_rows = [r for r in body if ei is not None and _s(r[ei])]
    http_ok = sum(1 for r in ep_rows if hi is not None and _s(r[hi]))
    unparse = kinds.get("unparseable", 0)
    vals = {"rows": len(body), "unparseable": unparse, "unparseable_share": _share(unparse, len(body)),
            "duplicate_severity_spellings": dup_spellings, "endpoint_rows": len(ep_rows), "http_code_populated": http_ok,
            "http_code_coverage_share": _share(http_ok, len(ep_rows))}
    for k, v in kinds.items():
        vals["kind_" + k] = v
    for k, v in levels.most_common(6):
        vals["level_" + k.lower()] = v
    warn = (_share(unparse, len(body)) > THRESHOLDS["runlog_unparseable_share"] or dup_spellings > 0
            or (ep_rows and _share(http_ok, len(ep_rows)) < THRESHOLDS["runlog_http_coverage"]))
    ex = [f"{'/'.join(sorted(v))} both used" for v in fam.values() if len(v) > 1]
    if unparse:
        ex.append(f"unparseable stamps {unparse}")
    count = unparse + dup_spellings + (len(ep_rows) - http_ok)
    return check("DQ-70", "runlog_hygiene", "I", "ops", "WARN" if warn else "PASS", count, vals, ex,
                 "count = unparseable stamps + duplicate severity spellings + Endpoint rows without an HTTP Code; stamp kinds "
                 "(datetime / ISO+tz string / ISO naive string / unparseable) and level vocabulary in values",
                 "parse 100 %; one spelling per severity; HTTP Code on >= 95 % of Endpoint rows", "WP-10")


def dq_71_status_vocab(ctx):
    rows = ctx["book"].rows("_Status")
    if not rows:
        return not_checked("DQ-71", "status_vocabulary", "I", "ops", "_Status not in export", "", "WP-10")
    page_rows = [r for r in rows[1:] if _s(r[0])]
    vocab = collections.Counter(_s(r[2]) for r in page_rows if len(r) > 2 and _s(r[2]))
    kinds = collections.Counter(("tz_string" if "+" in _s(r[1]) or _s(r[1]).endswith("Z") else "naive") for r in page_rows if len(r) > 1 and not _blank(r[1]))
    vals = {"page_rows": len(page_rows), "status_spellings": len(vocab), "stamp_formats": len(kinds)}
    for k, v in vocab.items():
        vals["status_" + k.lower()] = v
    for k, v in kinds.items():
        vals["stamp_" + k] = v
    count = max(len(vocab) - 1, 0) + max(len(kinds) - 1, 0)
    return check("DQ-71", "status_vocabulary", "I", "ops", "WARN" if count else "PASS", count, vals,
                 [f"spellings {'/'.join(sorted(vocab))}"] if len(vocab) > 1 else [],
                 "count = extra Status spellings + extra Last Updated stamp formats on _Status page rows", "1 vocabulary / 1 stamp format", "WP-10")


def dq_72_validator_scope(ctx):
    rows = ctx["book"].rows("Dashboard_Audit")
    if not rows:
        return not_checked("DQ-72", "validator_scope", "I", "ops", "Dashboard_Audit not in export", "", "WP-10")
    hi = _find_row(rows, lambda r: _s(r[0]) == "Page" and len(r) > 1 and _s(r[1]) == "Check")
    if hi is None:
        return not_checked("DQ-72", "validator_scope", "I", "ops", "Dashboard_Audit header 'Page|Check' not found", "", "WP-10")
    ix = _header_index(rows, hi)
    body = rows[hi + 1:]
    vals, ex, capped = {}, [], 0
    for r in body:
        page, chk = _s(r[ix["Page"]]), _s(r[ix["Check"]])
        n = _num(r[ix["Count"]]) if "Count" in ix else None
        if chk == "scope.coverage" and n is not None:
            vals[f"{page}:coverage"] = n
            if n == 1500:
                capped += 1
                ex.append(f"{page} capped at 1500")
        if chk == "sanity.roi12_within_softcap" and n is not None:
            vals[f"{page}:roi12_audit"] = n
    gm = ctx["pages"].get("Global_Markets")
    if gm:
        vals["Global_Markets:roi12_dq33"] = sum(1 for r in gm.body if (gm.num(r, "Expected ROI 12M") or 0) > THRESHOLDS["roi12_soft_cap"])
    return check("DQ-72", "validator_scope", "I", "ops", "FAIL" if capped else "PASS", capped, vals, ex,
                 "pages whose Dashboard_Audit scope.coverage Count == 1500 (sampled); its roi12_within_softcap count vs the full-page DQ-33 count",
                 "validator reads full pages", "WP-10")


def dq_73_s1_exclusions(ctx):
    rows = ctx["book"].rows("S1_Gate")
    if not rows:
        return not_checked("DQ-73", "s1_infra_exclusions", "I", "ops", "S1_Gate not in export", "", "mapped P-204 / P-176")
    verdict = next((re.sub(r"^verdict:\s*", "", _s(v)) for v in rows[0] if _s(v).startswith("verdict")), "") if rows else ""
    detail = ""
    for r in rows:
        if _num(r[0]) == 1.0 and len(r) > 3:
            detail = _s(r[3])
            break
    m = S1_CRITERION1_RE.search(detail)
    if not m:
        return not_checked("DQ-73", "s1_infra_exclusions", "I", "ops", "criterion 1 detail 'n/m scored days - k excluded-infra' not found", "", "mapped P-204 / P-176")
    scored, needed, excluded = int(m.group(1)), int(m.group(2)), int(m.group(3))
    share = _share(excluded, scored + excluded)
    vals = {"scored_days": scored, "needed_days": needed, "excluded_infra_days": excluded, "excluded_share": share}
    return check("DQ-73", "s1_infra_exclusions", "I", "ops", "WARN" if share > THRESHOLDS["s1_excluded_share"] else "PASS", excluded, vals,
                 [f"verdict {verdict}" if verdict else "verdict ?"], "S1_Gate criterion 1 excluded-infra days / (scored + excluded)",
                 "<= 10 % of days excluded", "mapped P-204 / P-176")


def dq_74_provider_errors(ctx):
    ix, body = _runlog(ctx)
    if ix is None:
        return not_checked("DQ-74", "provider_error_signals", "I", "ops", "_Run_Log not in export", "", "WP-5 / P-154")
    ref = ctx["ref_time_log"]
    if ref is None:
        return not_checked("DQ-74", "provider_error_signals", "I", "ops", "no parseable _Run_Log Timestamp", "", "WP-5 / P-154")
    start = ref - dt.timedelta(hours=48)
    ti, mi, di, hi = ix.get("Timestamp", 0), ix.get("Message"), ix.get("Details JSON"), ix.get("HTTP Code")
    agg = collections.Counter()
    plans = collections.Counter()
    n = 0
    for r in body:
        t = _parse_dt(r[ti], ctx["tz_offset"])
        if not t or not (start <= t <= ref):
            continue
        n += 1
        txt = " ".join(_s(r[i]) for i in (mi, di) if i is not None and i < len(r))
        for key, rx in RUNLOG_TOKENS.items():
            for m in rx.finditer(txt):
                agg[key] += int(m.group(1))
        for m in RUNLOG_PLAN_RE.finditer(txt):
            plans[int(m.group(1))] += 1
        # a row is one HTTP event per code, whether the code sits in the HTTP Code column or in Details JSON
        row_codes = {m.group(1) for m in RUNLOG_DETAILS_HTTP_RE.finditer(txt)}
        code = _s(r[hi]).split(".")[0] if hi is not None else ""
        if code in RUNLOG_HTTP_CODES:
            row_codes.add(code)
        for c in row_codes:
            agg["http_rows_" + c] += 1
    vals = {"window_rows": n, "window_hours": 48}
    vals.update({k: v for k, v in agg.items()})
    for k in RUNLOG_TOKENS:
        vals.setdefault(k, 0)
    for c in RUNLOG_HTTP_CODES:
        vals.setdefault("http_rows_" + c, 0)
    if plans:
        vals["plan_limit"] = plans.most_common(1)[0][0]
    e402 = vals["rows402_new"] + vals["http_rows_402"]
    e429 = vals["f429"] + vals["fb429"] + vals["quote_http_429"] + vals["http_rows_429"]
    e502 = vals["http_rows_502"]
    vals["events_402"] = e402
    vals["events_429"] = e429
    vals["events_502"] = e502
    status = "FAIL" if (e402 > 0 or e502 > THRESHOLDS["err_502_fail"]) else "WARN" if e429 > THRESHOLDS["err_429_warn"] else "PASS"
    return check("DQ-74", "provider_error_signals", "I", "ops", status, e402 + e429 + e502, vals,
                 [f"window {start.isoformat()} -> {ref.isoformat()}", f"402 {e402} / 429 {e429} / 502 {e502}"],
                 "structured tokens in the 48 h ending at ref_time_log: rows402 new=, f429=, fb=429:, quote_http_429, HTTP Code column "
                 "and \"httpCode\" in Details JSON (digit mentions are not counted); plan limit parsed from used=N/M",
                 "402 = 0; 429 <= 10; 502 <= 10", "WP-5 (routing) / P-154")


def dq_75_leg_duration(ctx):
    rows = ctx["book"].rows("_Status")
    if not rows:
        return not_checked("DQ-75", "sync_leg_duration", "I", "ops", "_Status not in export", "", "WP-6 / WP-12")
    ix = _header_index(rows)
    di = ix.get("Duration ms", 8)
    vals, ex, over, warn = {"budget_ms": SYNC_BUDGET_MS}, [], 0, 0
    for r in rows[1:]:
        p = _s(r[0])
        if p in MARKET_PAGES and len(r) > di:
            ms = _num(r[di])
            if ms is None:
                continue
            vals[p + "_ms"] = ms
            vals[p + "_budget_share"] = _share(ms, SYNC_BUDGET_MS)
            if ms > SYNC_BUDGET_MS:
                over += 1
                ex.append(f"{PAGE_SHORT[p]} over budget")
            elif ms > SYNC_BUDGET_MS * THRESHOLDS["leg_budget_warn_share"]:
                warn += 1
                ex.append(f"{PAGE_SHORT[p]} {_pct(ms, SYNC_BUDGET_MS):.1f} % of budget")
    if len(vals) == 1:
        return not_checked("DQ-75", "sync_leg_duration", "I", "ops", "_Status carries no Duration ms for the market pages", "", "WP-6 / WP-12")
    status = "FAIL" if over else "WARN" if warn else "PASS"
    return check("DQ-75", "sync_leg_duration", "I", "ops", status, over + warn, vals, ex,
                 "_Status Duration ms per market page vs the 3,600,000 ms budget (count = legs over budget or above 90 % of it)",
                 "<= 3,600,000 ms per leg", "WP-6 / WP-12")


# --------------------------------------------------------------------------- #
# Always-emitted NOT_CHECKED / INFO records (inputs outside an export)
# --------------------------------------------------------------------------- #
def nc_records(ctx):
    out = [
        not_checked("NC-01", "render_health_gate_states", "X", "ops", "Render /health gate states are not in an export (section 5.2)"),
        not_checked("NC-02", "workbook_sharing_acl", "X", "ops", "Drive sharing ACL cannot be read offline"),
        not_checked("NC-03", "investable_symbol_formula_ranges", "X", "surface",
                    "formula text is not available under data_only=True (DQ-52 sub-check)"),
        not_checked("NC-04", "weight_basis_label", "X", "book", "the weight-basis label is not a cell in the export (DQ-51 sub-check)"),
        not_checked("NC-05", "broker_truth", "X", "book", "positions, settled cash and fills live at the broker, not in the workbook"),
    ]
    sl = ctx["book"].rows("System_Logs")
    if sl is None:
        out.append(not_checked("INFO-01", "pii_in_system_logs", "X", "ops", "System_Logs not in export"))
    else:
        n = sum(1 for r in sl for v in r if EMAIL_RE.search(_s(v)))
        out.append(check("INFO-01", "pii_in_system_logs", "X", "ops", "INFO", n, {"email_cells": n}, [],
                         "System_Logs cells matching an e-mail pattern (count only; the address is never emitted)", "INFO", "WP-11"))
    return out


CHECKS = (
    dq_01_live_rows, dq_02_padding, dq_03_grid, dq_04_export_size, dq_05_orphans, dq_06_ghosts, dq_07_header_contract,
    dq_08_inpage_dups, dq_09_crosspage_dups, dq_10_bare_vs_us, dq_11_asset_class, dq_12_sector, dq_13_exchange,
    dq_14_currency, dq_15_rows_without_price, dq_16_misresolved, dq_17_ledger_symbols,
    dq_18_day_range, dq_19_w52, dq_20_percent_change, dq_21_freshness, dq_22_stale_bars, dq_23_stamp_offset,
    dq_24_margins, dq_25_debt_equity, dq_26_dividend_yield, dq_27_beta_growth, dq_28_pe_zero, dq_29_rsi_default,
    dq_30_dq100_oob, dq_31_roi_identity, dq_32_fp_without_roi, dq_33_roi12_cap, dq_34_tuple_conflict, dq_35_analyst_lkg,
    dq_36_horizon, dq_37_f7, dq_38_investable_coherence, dq_39_score_discrimination, dq_40_fallback_share, dq_41_rank,
    dq_42_dead_columns, dq_43_warnings_telemetry, dq_44_reason_length,
    dq_45_feed_state, dq_46_pf_block, dq_47_panel_vs_policy, dq_48_cockpit_inputs, dq_49_calendar_coverage,
    dq_50_cockpit_alerts, dq_51_weight_basis, dq_52_investable_view, dq_53_brackets,
    dq_54_placeholder_dates, dq_55_fees, dq_56_lot_fx, dq_57_cash_snapshot, dq_58_dividends, dq_59_trade_notes,
    dq_60_nav_reconcile, dq_61_signal_history_keys, dq_62_perf_units, dq_63_perf_dead_columns, dq_64_win_vs_target,
    dq_65_outcomes_overdue, dq_66_pit_numeric, dq_67_hypotheses, dq_68_regret_forks, dq_69_saudi_regime,
    dq_70_runlog_hygiene, dq_71_status_vocab, dq_72_validator_scope, dq_73_s1_exclusions, dq_74_provider_errors,
    dq_75_leg_duration,
)


# --------------------------------------------------------------------------- #
# Row-level rules (shared by row records and DQ scores)
# --------------------------------------------------------------------------- #
ROW_RULE_CLASS = {
    "A": {"identity": {"DQ-11", "DQ-12", "DQ-13"}, "quote": {"DQ-18", "DQ-19"}, "forecast": {"DQ-33"},
          "fundamentals": {"DQ-24", "DQ-25", "DQ-26", "DQ-27", "DQ-28"}, "scores": {"SCORE_RANGE"}, "provenance": {"WARN_LEN"}},
    "S": {"identity": {"DQ-09", "DQ-10", "DQ-14", "DQ-16"}, "quote": {"DQ-20"}, "forecast": {"DQ-31", "DQ-32", "DQ-34", "DQ-36"},
          "fundamentals": {"DQ-30"}, "scores": {"DQ-38", "DQ-30"}, "provenance": {"DQ-23"}},
    "T": {"identity": {"QUARANTINED"}, "quote": {"DQ-21", "DQ-22"}, "forecast": {"DQ-35"}, "fundamentals": {"FUND_LKG"},
          "scores": {"DQ-37"}, "provenance": {"UNSTAMPED"}},
}


def _page_meta(ctx):
    if "page_meta" in ctx:
        return ctx["page_meta"]
    meta = {}
    cross = {s for s, ps in ctx["where"].items() if len(set(ps)) > 1}
    codes = set(ctx["fx_codes"]) if ctx["fx_codes"] else None
    cal = ctx["book"].rows("Calendar_Events")
    dated = None
    if cal:
        cix = _header_index(cal)
        si, ei, xi = cix.get("Symbol"), cix.get("Next Earnings Date"), cix.get("Next Ex-Div Date")
        if si is not None:
            dated = {_s(r[si]) for r in cal[1:] if _s(r[si]) and ((ei is not None and not _blank(r[ei])) or (xi is not None and not _blank(r[xi])))}
    for p, pg in ctx["pages"].items():
        if not pg:
            continue
        st = set(pg.syms)
        ac = collections.Counter(pg.s(r, "Asset Class") for r in pg.body)
        low = collections.defaultdict(list)
        for k, v in ac.items():
            if k:
                low[k.lower()].append((v, k))
        minority = {k for v in low.values() if len(v) > 1 for _, k in sorted(v)[:-1]}
        meta[p] = {"cross": cross, "bare": {s for s in st if "." not in s and s + ".US" in st}, "minority_ac": minority,
                   "fx_codes": codes, "dated": dated, "sector_strict": p in ("Market_Leaders", "Global_Markets")}
    ctx["page_meta"] = meta
    return meta


def _row_flags(pg, r, tg, ctx, m):
    """DQ-ids whose row-level rule this row violates (plus internal markers used by the score legs)."""
    tea = _tea()
    f = set()
    sym = pg.s(r, "Symbol")
    px, prev = pg.num(r, "Current Price"), pg.num(r, "Previous Close")
    priced = px is not None and px > 0
    if sym in m["cross"]:
        f.add("DQ-09")
    if sym in m["bare"]:
        f.add("DQ-10")
    ac = pg.s(r, "Asset Class")
    if not ac or ac in m["minority_ac"]:
        f.add("DQ-11")
    sec = pg.s(r, "Sector")
    if m["sector_strict"] and sec not in GICS_11 and sec not in SECTOR_ALLOWED_EXTRA:
        f.add("DQ-12")
    exch = pg.s(r, "Exchange")
    if exch in (EXCHANGE_MINORITY, EXCHANGE_PLACEHOLDER):
        f.add("DQ-13")
    cur = pg.s(r, "Currency")
    if m["fx_codes"] is not None and cur and cur not in m["fx_codes"]:
        f.add("DQ-14")
    if not priced:
        f.add("DQ-15")
    if ac.upper() == ALTSYMBOL_CLASS:
        f.add("DQ-16")
    if tea._is_crypto(sym):
        if ac.upper() != CRYPTO_CLASS or exch != CRYPTO_EXCHANGE:
            f.add("DQ-16")
        keys = tea.CRYPTO_NAMES.get(sym[:-4])
        if keys and not any(k in pg.s(r, "Name").lower() for k in keys):
            f.add("DQ-16")
    lo, hi = pg.num(r, "Day Low"), pg.num(r, "Day High")
    if (lo is not None and hi is not None and lo > hi) or (priced and lo is not None and hi is not None and (px < lo or px > hi)):
        f.add("DQ-18")
    wlo, whi, pos = pg.num(r, "52W Low"), pg.num(r, "52W High"), pg.num(r, "52W Position %")
    if any(pg.num(r, c) is not None and abs(pg.num(r, c) - THRESHOLDS["w52_sentinel"]) < 1e-3 for c in ("52W High", "52W Low", "Target Price")):
        f.add("DQ-19")
    if priced and ((wlo is not None and px < wlo) or (whi is not None and px > whi)):
        f.add("DQ-19")
    if priced and pos is not None and wlo is not None and whi is not None and whi > wlo and abs((px - wlo) / (whi - wlo) * 100.0 - pos) > THRESHOLDS["w52_position_pts"]:
        f.add("DQ-19")
    pc = pg.num(r, "Percent Change")
    if pc is not None and priced and prev and prev > 0 and abs(pc - (px / prev - 1.0)) > THRESHOLDS["pct_change_tolerance"]:
        f.add("DQ-20")
    lu = _parse_dt(pg.val(r, "Last Updated (UTC)"), ctx["tz_offset"])
    if lu is None:
        f.add("DQ-21")
        f.add("UNSTAMPED")
    elif ctx["ref_time_rows"] and (ctx["ref_time_rows"] - lu).total_seconds() > THRESHOLDS["freshness_hours"] * 3600:
        f.add("DQ-21")
    stale = any(t.startswith("price_bar_stale:") for t in tg)
    if stale and pg.s(r, "Investability Status").upper() != "BLOCKED":
        f.add("DQ-22")
    if stale:
        f.add("STALE")
    u, ry = pg.s(r, "Last Updated (UTC)"), pg.s(r, "Last Updated (Riyadh)")
    if u and ry:
        try:
            ud = dt.datetime.fromisoformat(u.replace("Z", "+00:00"))
            rd = dt.datetime.fromisoformat(ry.replace("Z", "+00:00"))
            if abs((rd.replace(tzinfo=None) - ud.replace(tzinfo=None)).total_seconds() - THRESHOLDS["riyadh_offset_s"]) > THRESHOLDS["riyadh_tolerance_s"]:
                f.add("DQ-23")
        except ValueError:
            f.add("DQ-23")
    if any(pg.num(r, c) is not None and abs(pg.num(r, c)) > THRESHOLDS["margin_abs_max"] for c in MARGIN_COLS):
        f.add("DQ-24")
    de = pg.num(r, "Debt/Equity")
    if de is not None and de > THRESHOLDS["de_max"] and not any(t.startswith(DE_UNIT_TAG) for t in tg):
        f.add("DQ-25")
    if (pg.num(r, "Dividend Yield") or 0) > THRESHOLDS["yield_max"]:
        f.add("DQ-26")
    b, g = pg.num(r, "Beta (5Y)"), pg.num(r, "Revenue Growth YoY")
    if (b is not None and abs(b) > THRESHOLDS["beta_abs_max"]) or (g is not None and abs(g) > THRESHOLDS["growth_abs_max"]):
        f.add("DQ-27")
    if pg.num(r, "P/E (TTM)") == 0:
        f.add("DQ-28")
    if pg.num(r, "RSI (14)") == THRESHOLDS["rsi_default"]:
        f.add("DQ-29")
    dq = pg.num(r, "Data Quality Score")
    if dq == 100 and _garbage(pg, r):
        f.add("DQ-30")
    for hz, (fpc, roic) in HORIZONS.items():
        fp, roi = pg.num(r, fpc), pg.num(r, roic)
        if priced and fp and fp > 0 and roi is not None and abs(fp / px - 1.0 - roi) > THRESHOLDS["roi_identity_tolerance"]:
            f.add("DQ-31")
        if fp is not None and fp > 0 and roi is None:
            f.add("DQ-32")
    if (pg.num(r, "Expected ROI 12M") or 0) > THRESHOLDS["roi12_soft_cap"]:
        f.add("DQ-33")
    if any(t.startswith("sheet_tuple_conflict:") for t in tg):
        f.add("DQ-34")
    if any(LKG_HOURS_RE.match(t) for t in tg):
        f.add("DQ-35")
    reason, lbl = pg.s(r, "Recommendation Reason"), pg.s(r, "Invest Period Label").upper()
    if lbl in ("1Y", "12M") and ("horizon=month" in reason or "horizon=long" in reason):
        f.add("DQ-36")
    if _settlement_failed(pg, r, tg) or any(F7_RC_RE.search(t) for t in tg if t.startswith("f7_settle:")):
        f.add("DQ-37")
    fa, inv = pg.s(r, "Final Action").upper(), pg.s(r, "Investability Status").upper()
    if (fa == "INVEST" and pg.s(r, "Recommendation").upper() in SELL_CLASS) or \
       (inv == "INVESTABLE" and not (priced and (pg.num(r, "Forecast Price 12M") or 0) > 0)):
        f.add("DQ-38")
    if m["dated"] is not None and sym in ctx["cohort_calendar"] and sym not in m["dated"]:
        f.add("DQ-49")
    if any(t.startswith("identity_quarantined") for t in tg):
        f.add("QUARANTINED")
    if any(t.startswith("fundamentals_lkg") or t.startswith("fund_lkg") for t in tg):
        f.add("FUND_LKG")
    if any(pg.num(r, c) is not None and not (0 <= pg.num(r, c) <= 100) for c in SCORE_RANGE_COLS):
        f.add("SCORE_RANGE")
    wi = pg.col("Warnings")
    if wi is not None and len(_s(r[wi])) > THRESHOLDS["warnings_mean_len"]:
        f.add("WARN_LEN")
    return f


def _all_row_flags(ctx):
    if "row_flags" in ctx:
        return ctx["row_flags"]
    meta = _page_meta(ctx)
    out = {}
    for p, pg in ctx["pages"].items():
        if pg:
            out[p] = [_row_flags(pg, r, tg, ctx, meta[p]) for r, tg in zip(pg.body, pg.tags)]
    ctx["row_flags"] = out
    return out


def _populated_share(pg, r, cols):
    cols = [c for c in cols if pg.col(c) is not None]
    return _share(sum(1 for c in cols if not _blank(pg.val(r, c))), len(cols)) if cols else 1.0


def _leg(flags, rules):
    bad = sum(1 for x in rules if x in flags)
    return _share(len(rules) - bad, len(rules)) if rules else 1.0


# --------------------------------------------------------------------------- #
# Section 2.2 tab records
# --------------------------------------------------------------------------- #
def _writer_version(name, rows, ctx):
    sp = ctx["status_pages"].get(name)
    if sp and sp.get("version"):
        return f"run_dashboard_sync {sp['version']}"
    head = " ".join(_s(v) for r in rows[:3] for v in r[:8] if _s(v)) if rows else ""
    if name == "Dashboard_Audit":
        eng = re.search(r"engine=[\w.]+=([\d.]+)", head)
        reg = re.search(r"registry=([\d.]+)", head)
        return " ".join(x for x in (f"engine {eng.group(1)}" if eng else "", f"registry {reg.group(1)}" if reg else "") if x) or None
    if name == "Top_10_Investments":
        footer = ctx["candidates"][2] or ""
        parts = re.findall(r"(route|builder) v([\d.]+)", footer)
        return " ".join(f"{k} {v}" for k, v in parts) or None
    if name == "Portfolio_Decision":
        txt = " ".join(_s(v) for r in rows for v in r[:3] if "actions v" in _s(v))
        m = re.search(r"actions v([\d.]+)", txt)
        return f"portfolio_actions {m.group(1)}" if m else None
    if name in ("_S1_Calibration", "Regime_History", "_Trade_Notes") and rows:
        ix = _header_index(rows)
        col = ix.get("Writer Version", ix.get("Regime Version"))
        if col is not None:
            vals = [_s(r[col]) for r in rows[1:] if col < len(r) and _s(r[col])]
            return vals[-1] if vals else None
    m = VERSION_STAMP_RE.search(head)
    return m.group(1) if m else None


def _last_written(name, rows, ctx):
    if not rows:
        return None
    col = TAB_STAMP_COLUMN.get(name)
    best = None
    if col:
        ix = _header_index(rows)
        ci = ix.get(col)
        if ci is not None:
            for r in rows[1:]:
                if ci < len(r):
                    d = _parse_dt(r[ci], ctx["tz_offset"])
                    if d and (best is None or d > best):
                        best = d
    if best is None:
        head = " ".join(_s(v) for r in rows[:3] for v in r[:8] if _s(v))
        m = re.search(r"(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?)", head)
        if m:
            best = _parse_dt(m.group(1), ctx["tz_offset"])
    return best.isoformat() if best else None


def tab_records(ctx):
    book = ctx["book"]
    dd = book.rows("Data_Dictionary") or []
    in_dict = set()
    if dd:
        si = _header_index(dd).get("Sheet", 0)
        in_dict = {_s(r[si]) for r in dd[1:] if _s(r[si])}
    out = []
    for name in book.sheets:
        rows = book.sheets[name]
        meta = KNOWN_TABS.get(name)
        header = [_s(h) for h in rows[0]] if rows else []
        grid = book.grid_rows.get(name, len(rows))
        width = len(rows[0]) if rows else 0
        rec = {"tab": name, "visibility": book.state.get(name, "?"),
               "owner_lane": meta[0] if meta else "unknown", "writer": meta[1] if meta else "unknown",
               "purpose": meta[2] if meta else "unknown", "cadence": meta[3] if meta else "unknown",
               "retention": meta[4] if meta else "unknown", "business_criticality": meta[5] if meta else "unknown",
               "writer_version": _writer_version(name, rows, ctx),
               "contract": {"header_sha256": hashlib.sha256("|".join(header).encode()).hexdigest()[:16],
                            "columns_n": sum(1 for h in header if h), "width_n": width},
               "row_baseline_rows": ctx["expect"].get(name), "in_registry": bool(meta and meta[6]),
               "in_dictionary": name in in_dict,
               "status": "ok" if meta else ("legacy" if re.match(r"^(Sheet\d+|_SM_TEST.*|Daily_Review_.*|Diagnostic_Report|_Data_Audit)$", name) else "orphan"),
               "last_written": _last_written(name, rows, ctx), "cells_used_total": grid * width}
        if name in TAB_SYMBOL_KEYED and rows and "Symbol" in header:
            si = header.index("Symbol")
            live = sum(1 for r in rows[1:] if si < len(r) and _s(r[si]))
            rec["live_rows"] = live
            rec["padding_rows"] = grid - 1 - live
        else:
            rec["data_rows"] = max(len(rows) - 1, 0)
            rec["padding_rows"] = grid - len(rows)
        out.append(rec)
    for ghost in sorted(in_dict - set(book.sheets)):
        out.append({"tab": ghost, "visibility": "absent", "owner_lane": "unknown", "writer": "run_dashboard_sync (dictionary only)",
                    "purpose": "dictionary entry without a tab", "cadence": "unknown", "retention": "unknown",
                    "business_criticality": "unknown", "writer_version": None,
                    "contract": {"header_sha256": "", "columns_n": 0, "width_n": 0},
                    "row_baseline_rows": None, "in_registry": False, "in_dictionary": True, "status": "ghost",
                    "last_written": None, "cells_used_total": 0, "data_rows": 0, "padding_rows": 0})
    return out


# --------------------------------------------------------------------------- #
# Section 2.3 row records (decision-critical cohort) and section 2.4 DQ scores
# --------------------------------------------------------------------------- #
def _engine_stamp(ctx):
    da = ctx["book"].rows("Dashboard_Audit") or []
    head = " ".join(_s(v) for r in da[:3] for v in r[:4])
    m = re.search(r"engine=[\w.]+=([\d.]+)", head)
    return m.group(1) if m else None


def row_records(ctx, checks, baseline=None):
    flags_all = _all_row_flags(ctx)
    meta_check = {c["id"]: c for c in checks}
    engine = _engine_stamp(ctx)
    sha16 = ctx.get("export_sha256", "")[:16]
    board, qual, hold = set(ctx["board"]), set(ctx["qualified"]), set(ctx["holdings"])
    base_rows = {}
    if isinstance(baseline, dict):
        for br in baseline.get("rows") or []:
            if isinstance(br, dict) and "page" in br and "symbol" in br:
                base_rows[(br["page"], br["symbol"])] = br
    out = []
    all_cols = sorted({c for cols in CLASS_COLUMNS.values() for c in cols})
    a_rules = {x for v in ROW_RULE_CLASS["A"].values() for x in v}
    s_rules = {x for v in ROW_RULE_CLASS["S"].values() for x in v}
    t_rules = {x for v in ROW_RULE_CLASS["T"].values() for x in v}

    def record(pg, idx, r, tg, flags, crit):
        sym = pg.s(r, "Symbol")
        failed = sorted(f for f in flags if f.startswith("DQ-"))
        crit_fail = any(meta_check.get(f, {}).get("cls") in ("identity", "quote", "scores", "forecast") for f in failed)
        state = ("identity-quarantined" if "QUARANTINED" in flags else "unpriced" if "DQ-15" in flags
                 else "priced-stale" if "STALE" in flags else "priced-fresh")
        prev = base_rows.get((pg.name, sym), {})
        return {"page": pg.name, "symbol": sym, "symbol_canonical": sym,
                "bare_us_pair": "DQ-10" in flags, "cross_page": "DQ-09" in flags,
                "contract_version": {"scoring_schema": pg.s(r, "Scoring Schema Version") or None, "engine": engine},
                "row_state": state, "business_criticality": crit,
                "dq_completeness_share": _populated_share(pg, r, all_cols), "dq_accuracy_share": _leg(flags, a_rules),
                "dq_consistency_share": _leg(flags, s_rules), "dq_timeliness_share": _leg(flags, t_rules),
                "checks_failed": failed,
                "severity": ("P0" if crit_fail and crit in ("board_seat", "holding", "investable", "invest") else "P1" if failed else "none"),
                "fix_ref": ", ".join(sorted({meta_check[f]["fix_ref"] for f in failed if f in meta_check and meta_check[f]["fix_ref"] not in ("", "-")})),
                "evidence_ref": f"{sha16}:{pg.name}!{idx}",
                "first_seen": prev.get("first_seen") or ctx["asof"], "last_seen": ctx["asof"]}

    for p in MARKET_PAGES:
        pg = ctx["pages"].get(p)
        if not pg:
            continue
        for i, (r, tg, fl) in enumerate(zip(pg.body, pg.tags, flags_all[p])):
            sym = pg.syms[i]
            if sym not in ctx["cohort_rows"]:
                continue
            inv = pg.s(r, "Investability Status").upper()
            crit = ("board_seat" if sym in board else "holding" if sym in hold else "qualified" if sym in qual
                    else "investable" if inv == "INVESTABLE" else "invest")
            out.append(record(pg, pg.row_numbers[i], r, tg, fl, crit))
    mp = ctx["portfolio"]
    if mp:
        meta = _page_meta(ctx)
        m = dict(next(iter(meta.values()))) if meta else {"cross": set(), "minority_ac": set(), "fx_codes": None, "dated": None}
        m["sector_strict"] = False
        m["bare"] = set()
        for i, (r, tg) in enumerate(zip(mp.body, mp.tags)):
            fl = _row_flags(mp, r, tg, ctx, m)
            out.append(record(mp, mp.row_numbers[i], r, tg, fl, "holding"))
    return out


def rows_summary(rows):
    c = collections.Counter(r["business_criticality"] for r in rows)
    sev = collections.Counter(r["severity"] for r in rows)
    return {"rows_total": len(rows), **{f"{k}_count": v for k, v in sorted(c.items())},
            **{f"severity_{k.lower()}_count": v for k, v in sorted(sev.items())}}


def dq_scores(ctx):
    flags_all = _all_row_flags(ctx)
    out = {}
    for p in MARKET_PAGES:
        pg = ctx["pages"].get(p)
        if not pg:
            continue
        page = {}
        cohort_idx = [i for i, s in enumerate(pg.syms) if s in ctx["cohort_rows"]]
        for cls, (wc, wa, ws_, wt) in CLASS_WEIGHTS.items():
            def legs(idxs):
                if not idxs:
                    return None
                C = statistics.mean(_populated_share(pg, pg.body[i], CLASS_COLUMNS[cls]) for i in idxs)
                A = statistics.mean(_leg(flags_all[p][i], ROW_RULE_CLASS["A"][cls]) for i in idxs)
                S = statistics.mean(_leg(flags_all[p][i], ROW_RULE_CLASS["S"][cls]) for i in idxs)
                T = statistics.mean(_leg(flags_all[p][i], ROW_RULE_CLASS["T"][cls]) for i in idxs)
                return {"C": round(C, 4), "A": round(A, 4), "S": round(S, 4), "T": round(T, 4),
                        "score": round(wc * C + wa * A + ws_ * S + wt * T, 4), "rows_n": len(idxs)}
            page[cls] = legs(range(pg.n))
            if cls == "forecast":
                page["forecast_cohort"] = legs(cohort_idx)
        crit = [page[c]["score"] for c in CLASS_CRITICAL if page.get(c)]
        if page.get("forecast_cohort"):
            crit.append(page["forecast_cohort"]["score"])
        noncrit = [page[c]["score"] for c in ("fundamentals", "provenance", "forecast") if page.get(c)]
        page["critical_score"] = round(statistics.mean(crit), 4) if crit else None
        page["non_critical_score"] = round(statistics.mean(noncrit), 4) if noncrit else None
        out[p] = page
    return out


# --------------------------------------------------------------------------- #
# Baseline comparison, digest, redaction
# --------------------------------------------------------------------------- #
def _checks_by_id(obj):
    checks = obj.get("checks") if isinstance(obj, dict) else None
    if isinstance(checks, list):
        return {c["id"]: c for c in checks if isinstance(c, dict) and "id" in c}
    if isinstance(checks, dict):
        return checks
    return {}


def compare_to_baseline(report, baseline, file_name=None):
    """Regression = status rank rose, or count moved the wrong way (lower-is-better unless listed otherwise)."""
    cur, base = _checks_by_id(report), _checks_by_id(baseline)
    regressions, improvements = [], []
    for cid in sorted(set(cur) & set(base)):
        c, b = cur[cid], base[cid]
        cs, bs = c.get("status"), b.get("status")
        cc, bc = c.get("count"), b.get("count")
        entry = {"id": cid, "baseline_status": bs, "status": cs, "baseline_count": bc, "current_count": cc}
        worse = _status_rank(cs) > _status_rank(bs)
        better = _status_rank(cs) < _status_rank(bs)
        if cid not in BASELINE_COUNT_EXCLUDED and isinstance(cc, (int, float)) and isinstance(bc, (int, float)) \
                and not isinstance(cc, bool) and not isinstance(bc, bool):
            if cid in BASELINE_HIGHER_IS_BETTER:
                worse, better = worse or cc < bc, better or cc > bc
            else:
                worse, better = worse or cc > bc, better or cc < bc
        if worse:
            regressions.append(entry)
        elif better:
            improvements.append(entry)
    name = file_name or (baseline.get("file") if isinstance(baseline, dict) else None)
    return {"file": os.path.basename(name) if name else None,
            "export_sha256": baseline.get("export_sha256") if isinstance(baseline, dict) else None,
            "asof": baseline.get("asof") if isinstance(baseline, dict) else None,
            "compared_n": len(set(cur) & set(base)), "regressions": regressions, "improvements": improvements,
            "decision_critical_regressions": sorted(e["id"] for e in regressions if e["id"] in DECISION_CRITICAL_IDS)}


def reduce_baseline(report):
    """The committed fixture form: statuses, counts and per-page values only (no examples, rows or tabs)."""
    return {"script": "tfb_workbook_dq_audit", "version": SCRIPT_VERSION, "export_sha256": report["export_sha256"],
            "asof": report["asof"], "expect": dict(report["params"]["expect"]),
            "checks": {c["id"]: {"status": c["status"], "count": c["count"], "values": dict(c["values"])}
                       for c in report["checks"] if c["id"].startswith("DQ-")}}


def digest(report):
    body = _jsonable({k: v for k, v in report.items() if k not in DIGEST_EXCLUDE})
    return hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _assert_redacted(obj, path="$", inside_container=False):
    """Whitelist-by-construction walk (correction #1). Raises RedactionError on the first violation."""
    if isinstance(obj, bool) or obj is None:
        return
    if isinstance(obj, (int, float)):
        if not inside_container:
            key = path.rsplit(".", 1)[-1]
            if not REDACT_NUMERIC_KEY_RE.search(key):
                raise RedactionError(f"numeric value under a non-whitelisted key: {path}")
        return
    if isinstance(obj, str):
        if MONEY_STRING_RE.search(obj):
            raise RedactionError(f"money-like string at {path}: {obj[:60]!r}")
        if MONEY_WORD_RE.search(obj):
            raise RedactionError(f"money word followed by a figure at {path}: {obj[:60]!r}")
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            ks = str(k)
            _assert_redacted(v, f"{path}.{ks}", inside_container or ks in REDACT_NUMERIC_CONTAINERS)
        return
    if isinstance(obj, (list, tuple, set)):
        for i, v in enumerate(obj):
            _assert_redacted(v, f"{path}[{i}]", inside_container)
        return
    _assert_redacted(str(obj), path, inside_container)


# --------------------------------------------------------------------------- #
# audit_workbook: the one entry point
# --------------------------------------------------------------------------- #
def _mode():
    m = (os.getenv("TFB_DQ_AUDIT_MODE") or "off").strip().lower()
    return m if m in ("observe", "enforce") else "off"


def _load_baseline(baseline):
    if baseline is None:
        return None, None
    if isinstance(baseline, dict):
        return baseline, baseline.get("file")
    with open(baseline, "r", encoding="utf-8") as fh:
        return json.load(fh), os.path.basename(baseline)


def audit_workbook(path, expect=None, asof=None, now_utc=None, baseline=None, mode=None, tz_offset=3,
                   include_rows=False, examples_private=False):
    t0 = time.monotonic()
    book = load_book(path)
    ctx = build_context(book, expect or EXPECT, tz_offset=tz_offset, now_utc=now_utc)
    ctx["export_bytes"] = os.path.getsize(path)
    ctx["export_sha256"] = _sha256_file(path)
    if asof:
        ctx["asof"] = asof
    checks = [fn(ctx) for fn in CHECKS] + nc_records(ctx)
    if not examples_private:
        for c in checks:
            if c["id"] in PRIVATE_EXAMPLE_IDS:
                c["examples"] = []
    base_obj, base_name = _load_baseline(baseline)
    rows = row_records(ctx, checks, base_obj)
    summary = collections.Counter(c["status"] for c in checks)
    report = {
        "script": "tfb_workbook_dq_audit", "version": SCRIPT_VERSION,
        "export": os.path.basename(path), "export_sha256": ctx["export_sha256"], "export_bytes": ctx["export_bytes"],
        "asof": ctx["asof"],
        "reference_time_rows": ctx["ref_time_rows"].isoformat() if ctx["ref_time_rows"] else None,
        "reference_time_log": ctx["ref_time_log"].isoformat() if ctx["ref_time_log"] else None,
        "mode": mode or _mode(),
        "checks": checks, "tabs": tab_records(ctx), "rows_summary": rows_summary(rows),
        "dq_scores": dq_scores(ctx), "baseline": None,
        "summary": {"checks_count": len(checks), "dq_count": sum(1 for c in checks if c["id"].startswith("DQ-")),
                    "pass_count": summary.get("PASS", 0), "warn_count": summary.get("WARN", 0),
                    "fail_count": summary.get("FAIL", 0), "info_count": summary.get("INFO", 0),
                    "not_checked_count": summary.get("NOT_CHECKED", 0),
                    "critical_fail_count": sum(1 for c in checks if c["status"] == "FAIL" and c["id"] in DECISION_CRITICAL_IDS),
                    "tabs_count": len(book.sheets), "cohort_rows_count": len(rows)},
        "params": {"expect": dict(ctx["expect"]), "asof": asof, "now": now_utc.isoformat() if now_utc else None,
                   "mode": mode or _mode(), "baseline": base_name, "tz_offset_hours": tz_offset,
                   "rows_included": bool(include_rows), "examples_private": bool(examples_private)},
    }
    if include_rows:
        report["rows"] = rows
    if base_obj is not None:
        report["baseline"] = compare_to_baseline(report, base_obj, base_name)
    report["export_path"] = os.path.abspath(path)
    report["runtime_ms"] = int((time.monotonic() - t0) * 1000)
    report["generated_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    report["digest"] = digest(report)
    return report


def exit_code(report):
    if report.get("baseline"):
        return 1 if report["baseline"]["decision_critical_regressions"] else 0
    return 1 if report["summary"]["critical_fail_count"] else 0


def readback_line(report):
    s, b = report["summary"], report.get("baseline") or {}
    return (f"[DQ-AUDIT v{SCRIPT_VERSION}] {report['export']} mode={report['mode']} export_sha={report['export_sha256'][:16]} "
            f"asof={report['asof']} tabs={s['tabs_count']} checks={s['checks_count']} pass={s['pass_count']} warn={s['warn_count']} "
            f"fail={s['fail_count']} info={s['info_count']} not_checked={s['not_checked_count']} "
            f"decision_critical_fail={s['critical_fail_count']} baseline={b.get('file') or 'none'} "
            f"regressions={len(b.get('regressions', []))} critical_regressions={len(b.get('decision_critical_regressions', []))} "
            f"digest={report['digest']}")


# --------------------------------------------------------------------------- #
# Markdown scorecard (section 3.2 layout)
# --------------------------------------------------------------------------- #
TAB_CHECKS = {
    "Top_10_Investments": ["DQ-45", "DQ-47", "DQ-48", "DQ-50", "DQ-52", "DQ-02"], "Portfolio_Decision": ["DQ-46", "DQ-53", "DQ-47"],
    "My_Portfolio": ["DQ-51", "DQ-07"], "_Portfolio_CostBasis": ["DQ-17", "DQ-54", "DQ-55", "DQ-56", "DQ-59", "DQ-02"],
    "_Cash_Snapshot": ["DQ-57", "DQ-60", "DQ-02"], "_Dividend_Log": ["DQ-58", "DQ-17"], "_Trade_Notes": ["DQ-59"],
    "Calendar_Events": ["DQ-49", "DQ-02"], "Signal_History": ["DQ-61"], "Performance_Log": ["DQ-62", "DQ-63", "DQ-64"],
    "Hypothesis_Registry": ["DQ-67"], "Regret_Ledger": ["DQ-68"], "Regime_History": ["DQ-69"], "_Run_Log": ["DQ-70", "DQ-74"],
    "_Status": ["DQ-71", "DQ-75", "DQ-03", "DQ-45"], "Dashboard_Audit": ["DQ-72"], "S1_Gate": ["DQ-73"], "_PIT_Fundamentals": ["DQ-66"],
    "_Selection_Log": ["DQ-65"], "_Portfolio_Action_Log": ["DQ-65", "DQ-46"], "Data_Dictionary": ["DQ-06", "DQ-07"],
    "_Lists_Config": ["DQ-14", "DQ-47"], "Investable Symbol": ["DQ-52", "DQ-07"], "Insights_Analysis": ["DQ-02"],
}


def _page_checks(report, page):
    out = []
    for c in report["checks"]:
        if not c["id"].startswith("DQ-"):
            continue
        if page in c["values"] or any(str(k).startswith(page + ":") for k in c["values"]):
            out.append(c)
    return out


def _grade(tab, status, checks):
    if status in ("orphan", "legacy", "ghost"):
        return "X"
    sts = [(c["status"], c["id"] in DECISION_CRITICAL_IDS) for c in checks]
    if any(s == "FAIL" and crit for s, crit in sts):
        return "D"
    if any(s == "FAIL" for s, _ in sts):
        return "C"
    if any(s == "WARN" for s, _ in sts):
        return "B"
    return "A"


def _measured(c):
    vals = c["values"]
    parts = []
    for p in MARKET_PAGES:
        if p in vals:
            parts.append(f"{PAGE_SHORT[p]} {vals[p]}")
        else:
            k = next((k for k in vals if str(k).startswith(p + ":")), None)
            if k is not None:
                parts.append(f"{PAGE_SHORT[p]} {str(k).split(':', 1)[1]}={vals[k]}")
    if not parts:
        parts = [f"{k}={v}" for k, v in list(vals.items())[:4]]
    return f"count {c['count']}" + ("; " + " / ".join(parts) if parts else "")


def render_md(report):
    s = report["summary"]
    L = [f"# TFB workbook DQ audit v{SCRIPT_VERSION} - {report['export']}", "",
         f"As of {report['asof']} - export sha256 `{report['export_sha256'][:16]}...` ({report['export_bytes']:,} bytes, "
         f"{s['tabs_count']} tabs) - mode {report['mode']} - digest `{report['digest']}`", "",
         "| Checks | PASS | WARN | FAIL | INFO | NOT_CHECKED | decision-critical FAIL |", "|---|---|---|---|---|---|---|",
         f"| {s['checks_count']} ({s['dq_count']} DQ) | {s['pass_count']} | {s['warn_count']} | {s['fail_count']} | {s['info_count']} | "
         f"{s['not_checked_count']} | {s['critical_fail_count']} |", "",
         "## Tab scorecard", "", "| Tab | Live rows x cols | Grade | Top issue (check IDs) | Fix |", "|---|---|---|---|---|"]
    by_id = {c["id"]: c for c in report["checks"]}
    for t in report["tabs"]:
        name = t["tab"]
        checks = _page_checks(report, name) if name in MARKET_PAGES else [by_id[i] for i in TAB_CHECKS.get(name, []) if i in by_id]
        bad = [c for c in checks if c["status"] in ("FAIL", "WARN")]
        bad.sort(key=lambda c: (-_status_rank(c["status"]), c["id"]))
        issue = ", ".join(f"{c['id']} {c['name']}" for c in bad[:4]) if bad else ("-" if t["status"] == "ok" else t["status"])
        fix = ", ".join(sorted({c["fix_ref"] for c in bad[:4] if c["fix_ref"] and c["fix_ref"] != "-"})) or "-"
        rows_ = t.get("live_rows", t.get("data_rows"))
        cols = t["contract"]["columns_n"] if name in TAB_SYMBOL_KEYED else t["contract"].get("width_n", t["contract"]["columns_n"])
        L.append(f"| {name} | {rows_} x {cols} | {_grade(name, t['status'], checks)} | {issue} | {fix} |")
    L += ["", "## Check catalogue", "", "| ID | Check | Pass rule | Measured (ML / GM / CFX / MF or tab) | Status | Fix |", "|---|---|---|---|---|---|"]
    for c in report["checks"]:
        L.append(f"| {c['id']} | {c['name']} | {c['rule'].replace('|', '/')} | {_measured(c).replace('|', '/')} | {c['status']} | {c['fix_ref'] or '-'} |")
    L += ["", "## DQ scores", "", "| Page | Identity | Quote | Scores | Forecast (cohort) | Forecast (all) | Fundamentals | Provenance | Critical | Non-critical |",
          "|---|---|---|---|---|---|---|---|---|---|"]
    for p, sc in report["dq_scores"].items():
        def g(k):
            v = sc.get(k)
            return f"{v['score']:.3f}" if v else "-"
        L.append(f"| {p} | {g('identity')} | {g('quote')} | {g('scores')} | {g('forecast_cohort')} | {g('forecast')} | {g('fundamentals')} | "
                 f"{g('provenance')} | {sc.get('critical_score')} | {sc.get('non_critical_score')} |")
    L += ["", "## Trend vs baseline", ""]
    b = report.get("baseline")
    if not b:
        L.append("(no baseline given)")
    else:
        L += [f"Baseline `{b['file']}` (export sha256 `{(b.get('export_sha256') or '')[:16]}`, as of {b.get('asof')}): "
              f"{len(b['regressions'])} regression(s), {len(b['improvements'])} improvement(s), "
              f"decision-critical regressions: {', '.join(b['decision_critical_regressions']) or 'none'}", "",
              "| ID | Baseline | Current | Baseline count | Current count |", "|---|---|---|---|---|"]
        for e in b["regressions"]:
            L.append(f"| {e['id']} | {e['baseline_status']} | {e['status']} | {e['baseline_count']} | {e['current_count']} |")
        if b["improvements"]:
            L.append("")
            L.append("Improvements: " + ", ".join(f"{e['id']} {e['baseline_count']} -> {e['current_count']}" for e in b["improvements"]))
    L += ["", "## Not checked", ""]
    L += [f"- {c['id']} {c['name']}: {c['detail']}" for c in report["checks"] if c["status"] == "NOT_CHECKED"] or ["(none)"]
    rs = report["rows_summary"]
    L += ["", f"Row records: {rs['rows_total']} cohort rows (" + ", ".join(f"{k[:-6]} {v}" for k, v in rs.items() if k.endswith("_count") and not k.startswith("severity")) + ")."]
    return "\n".join(L) + "\n"


# --------------------------------------------------------------------------- #
# Synthetic workbooks through the REAL audit path (--selftest and tests)
# --------------------------------------------------------------------------- #
def _mrow(H, **kw):
    r = [None] * len(H)
    for k, v in kw.items():
        r[H.index(k)] = v
    return r


SYN_UPD_UTC = "2026-10-10T18:00:00+00:00"
SYN_UPD_RIY = "2026-10-10T21:00:00+03:00"


def _syn_market_row(H, sym, i, page, invest=False, price=10.0, rank=1):
    prev = price / 1.01
    sectors = sorted(GICS_11)
    r = _mrow(H, **{
        "Symbol": sym, "Name": f"{sym} Company", "Asset Class": "Equity", "Exchange": "SAU" if page == "Market_Leaders" else EXCHANGE_CANONICAL,
        "Currency": "SAR" if page == "Market_Leaders" else "USD", "Country": "Saudi Arabia" if page == "Market_Leaders" else "USA",
        "Sector": sectors[i % len(sectors)], "Industry": "Synthetic", "Current Price": price, "Previous Close": prev, "Open": price,
        "Day High": price * 1.02, "Day Low": price * 0.98, "52W High": price * 1.3, "52W Low": price * 0.7, "Price Change": price - prev,
        "Percent Change": price / prev - 1.0, "52W Position %": 50.0, "Volume": 1000 + i, "Avg Volume 10D": 900, "Avg Volume 30D": 950,
        "Market Cap": 1_000_000_000, "Float Shares": 50_000_000, "Beta (5Y)": 1.1, "P/E (TTM)": 15.0, "P/E (Forward)": 14.0, "EPS (TTM)": 1.2,
        "Dividend Yield": 0.02, "Payout Ratio": 0.3, "Revenue (TTM)": 100_000_000, "Revenue Growth YoY": 0.1, "Gross Margin": 0.4,
        "Operating Margin": 0.2, "Profit Margin": 0.1, "Debt/Equity": 0.5, "Free Cash Flow (TTM)": 10_000_000, "RSI (14)": 55.0 + i,
        "Volatility 30D": 0.2, "Volatility 90D": 0.25, "Max Drawdown 1Y": -0.1, "VaR 95% (1D)": -0.02, "Sharpe (1Y)": 1.0, "Risk Score": 40.0,
        "Risk Bucket": "LOW", "P/B": 2.0, "P/S": 3.0, "EV/EBITDA": 9.0, "PEG": 1.2, "Intrinsic Value": price * 1.1, "Upside %": 0.1,
        "Valuation Score": 60.0, "Forecast Price 1M": price * 1.02, "Forecast Price 3M": price * 1.05, "Forecast Price 12M": price * 1.10,
        "Expected ROI 1M": 0.02, "Expected ROI 3M": 0.05, "Expected ROI 12M": 0.10, "Forecast Confidence": 0.7, "Confidence Score": 70.0,
        "Confidence Bucket": "HIGH", "Value Score": 60.0, "Quality Score": 60.0, "Momentum Score": 60.0, "Growth Score": 60.0,
        "Overall Score": 70.0 + i, "Opportunity Score": 65.0, "Rank (Overall)": float(rank), "Analyst Rating": "Buy", "Target Price": price * 1.1,
        "Upside/Downside %": 0.1, "Signal": "BUY", "Trend 1M": "UP", "Trend 3M": "UP", "Trend 12M": "UP", "ST Signal": "BUY",
        "Recommendation": "BUY", "Recommendation Detail": "BUY - synthetic detail", "Recommendation Reason": "synthetic coherent row horizon=year",
        "Reco Priority": 1.0, "Priority Band": "A", "Recommendation Source": "engine", "Horizon Days": 365.0, "Invest Period Label": "1Y",
        "Sector-Adj Score": 65.0, "Conviction Score": 60.0, "Top Factors": "value", "Top Risks": "none", "Position Size Hint": "small",
        "Candle Pattern": "none", "Candle Signal": "none", "Candle Strength": 0.0, "Candle Confidence": 0.0, "Recent Patterns (5D)": "",
        "Provider Rating": "Overweight", "Scoring Reco Source": "engine", "Scoring Schema Version": "5.11.3", "Scoring Errors": "",
        "Opportunity Source": "roi_based", "Overall Score (Raw)": 70.0, "Overall Penalty Factor": 1.0, "Forecast Source": "provider_target",
        "Data Quality Score": 100.0 - i, "Forecast Reliability Score": 80.0 + i, "Provider/Engine Conflict": "No", "Conflict Type": "",
        "Final Decision Basis": "engine", "Investability Status": "INVESTABLE" if invest else "WATCHLIST",
        "Final Action": "INVEST" if invest else "WATCH", "Block Reason": "", "Data Provider": "eodhd", "Provider Secondary": "yahoo",
        "Last Updated (UTC)": SYN_UPD_UTC, "Last Updated (Riyadh)": SYN_UPD_RIY, "Row Source": "engine",
        "Warnings": "rel_path:sheet;fund_cache:hit"})
    return r


def build_synthetic(dirpath, clean=True):
    """Write a synthetic 'Market Share Deepseek-V3' export (clean or with planted negatives); return its path."""
    import openpyxl
    H, PH = list(MARKET_HEADER_115), list(PORTFOLIO_HEADER_122)
    hx = {h: i for i, h in enumerate(H)}
    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    def sheet(name, rows, hidden=False):
        ws = wb.create_sheet(name)
        for r in rows:
            ws.append(list(r))
        if hidden:
            ws.sheet_state = "hidden"
        return ws

    def setv(row, col, v):
        row[hx[col]] = v

    ml = [_syn_market_row(H, f"S{i + 1}.SR", i, "Market_Leaders", invest=i < 2, price=10.0 + i, rank=i + 1) for i in range(6)]
    gm = [_syn_market_row(H, f"G{i + 1}.US", i, "Global_Markets", invest=i < 2, price=20.0 + i, rank=i + 1) for i in range(5)]
    cfx = [_syn_market_row(H, "BTC-USD", 0, "Commodities_FX", price=60000.0, rank=1),
           _syn_market_row(H, "GC=F", 1, "Commodities_FX", price=2400.0, rank=2),
           _syn_market_row(H, "EURUSD=X", 2, "Commodities_FX", price=1.08, rank=3),
           _syn_market_row(H, "C1.US", 3, "Commodities_FX", price=30.0, rank=4)]
    for r, (ac, ex, nm, sec) in zip(cfx, (("CRYPTOCURRENCY", "CCC", "Bitcoin USD", "Unknown"), ("FUTURE", "CMX", "Gold Futures", "Unknown"),
                                        ("FX", "CCY", "EUR/USD", "Unknown"), ("Equity", EXCHANGE_CANONICAL, "Copper Co", "Materials"))):
        setv(r, "Asset Class", ac)
        setv(r, "Exchange", ex)
        setv(r, "Name", nm)
        setv(r, "Sector", sec)
    mf = [_syn_market_row(H, f"F{i + 1}.US", i, "Mutual_Funds", price=40.0 + i, rank=i + 1) for i in range(3)]
    for r in mf:
        setv(r, "Asset Class", "ETF")
    expect = {"Market_Leaders": 6, "Global_Markets": 5, "Commodities_FX": 4, "Mutual_Funds": 3}
    invest_syms = ["S1.SR", "S2.SR", "G1.US", "G2.US"]
    board = ["S1.SR", "G1.US"]
    holding = "G1.US"
    hold_px, shares, fx, cash = 20.0, 10.0, 3.75, 1000.0
    mv = shares * hold_px * fx
    nav = mv + cash
    lists_fx = ["SAR", "USD"]
    panel = (3.0, 40.0, 10.0)
    grid_txt = "OK | allocated=3,000,000 (30.00%) | free=7,000,000 | run=1"
    cand_news = ["Neutral", "Positive"]
    alerts = [("missing_valuation", 0.0, "No target/intrinsic value"), ("cash_floor", 0.0, f"Cash floor 10% of NAV {int(nav):,} SAR = {int(nav * 0.1):,} SAR (observe)"),
              ("price_xcheck", 0.0, "Second-source price check")]
    status_vocab = ["SUCCESS"] * 4
    status_stamps = ["2026-10-10 21:19:46+03:00"] * 4
    durations = [200000.0, 3000000.0, 300000.0, 1300000.0]
    cal_syms = list(dict.fromkeys(invest_syms + board + [holding]))
    pal_runs = [("2026-10-08 03:00", ["ADD", "HOLD"]), ("2026-10-09 03:00", ["HOLD"])]
    pd_actions = [("HOLD", holding, 10.0, 11.0, 12.0)]
    ledger = [["G1.US", "Holding Co", "USD", "Active", dt.datetime(2026, 9, 28), 19.0, shares, 1.0, None, None, None, 0.0, None, 190.0, hold_px, fx, mv, mv - 190.0 * fx, None, None, None, 0.05, 12.0, None, None, None],
              ["OLD.US", "Old Co", "USD", "Inactive", dt.datetime(2026, 8, 12), 50.0, 4.0, 1.0, dt.datetime(2026, 10, 1), 55.0, 1.0, 0.0, None, 200.0, "-", fx, "-", "-", 220.0, 75.0, 75.0, 0.1, 50.0, None, None, None]]
    cash_rows = [[dt.date(2026, 10, 9), dt.time(14, 0), "OPENING", cash, cash, 0.0, "opening", "synthetic-account", "SAR", "settled_cash", cash, 1.0, None],
                 [dt.date(2026, 10, 10), dt.time(14, 30), "SNAPSHOT (broker read)", cash, cash, 0.0, "broker read 10-10", "synthetic-account", "SAR", "settled_cash", cash, 1.0, None]]
    dividends = [["2026-09-08 00:59:33", "MANUAL", "G1.US", dt.datetime(2026, 9, 1), dt.datetime(2026, 8, 23), 0.5, 10.0, 5.0, "USD", ""]]
    notes = 2
    perf_target = [0.05, 0.05, 0.05, 0.05]
    perf_realized = [0.03, -0.02, 0.06, 0.01]
    perf_keys = ["S1.SR|3M|20260601", "S2.SR|3M|20260601", "G1.US|3M|20260601", "G2.US|3M|20260601"]
    perf_risk = (0.2, -0.1, 1.0)
    sel_rows = [["2026-10-01 10:00:00", "run 1", "Global_Markets", 1.0, "G1.US", "", "", "", "USD", fx, 20.0, 75.0, "", None, None, None, None, None, 0.1, 0.1, 0.1, None, 80.0, 100.0, 70.0, "", "2026-12-01", "", "", "WIN", "", "STABLE", 10.0]]
    pit_rows = [["2026-10-10", "G1.US", "Global_Markets", 20.0, 1_000_000_000.0, 15.0, 2.0, 1.2, 2.0, 0.5, 10.0, 60.0, "BUY", "eodhd", "provider_target", 80.0, 100.0, 70.0, 40.0, 2.0, 5.0, "INVESTABLE", "2026-10-10 18:00:00", "2026-10-10T18:05:00+00:00"]]
    hyp_sample = 30.0
    regret = [["2026-10-01", "FLOOR_LOCK", "G3.US", "G1.US", "venue floor", 20.0, 21.0, 90.0, "2026-10-05", 0.05, 0.02, 0.03, ""]]
    regime_state = "RISK_ON"
    runlog_stamps = [dt.datetime(2026, 10, 10, 18, 0, 0), dt.datetime(2026, 10, 10, 18, 10, 0), dt.datetime(2026, 10, 10, 18, 35, 32)]
    runlog_levels = ["INFO", "WARNING", "INFO"]
    quota_line = "[EODHD-QUOTA v6.64.14] Global_Markets used=1000/400000 (0.3%) | rows402 new=0 carried=0 | f429=0 f404=0 | state=OK"
    http_rows = [("200", "/v1/advanced/sheet-rows")]
    dash_cov = {"Market_Leaders": 6.0, "Global_Markets": 5.0, "Commodities_FX": 4.0, "Mutual_Funds": 3.0}
    s1_detail = "20/28 scored days · 0 excluded-infra"
    orphan_tab = None
    ghost_dict = False
    padding_rows = 0
    inv_view_extra = []
    pd_kpi_blank = False
    target_weight = 25.0

    if not clean:
        setv(gm[0], "Operating Margin", 20.0)                                   # DQ-24 x100 (oob 1) / DQ-30 (G1 at DQ 100)
        setv(gm[1], "Asset Class", "EQUITY")                                   # DQ-11 variant
        setv(gm[2], "Sector", "Financial Services")                            # DQ-12
        setv(gm[3], "Exchange", EXCHANGE_MINORITY)                             # DQ-13
        setv(cfx[3], "Currency", "USX")                                        # DQ-14 gap
        setv(mf[0], "Current Price", None)                                     # DQ-15 tagged
        setv(mf[0], "Warnings", "acquisition_status:unavailable;rel_path:sheet")
        setv(mf[1], "Current Price", 0)                                        # DQ-15 untagged
        sui = _syn_market_row(H, "SUI-USD", 4, "Commodities_FX", price=3.0, rank=5)
        setv(sui, "Name", "Salmonation USD")                                   # DQ-16 (Equity / NYSE-NASDAQ keep)
        cfx.append(sui)
        copx = _syn_market_row(H, "COPX.US", 5, "Commodities_FX", price=45.0, rank=6)
        cfx.append(copx)
        copx2 = _syn_market_row(H, "COPX.US", 3, "Mutual_Funds", price=45.0, rank=4)
        mf.append(copx2)                                                       # DQ-09 cross-page
        fcx = _syn_market_row(H, "FCX", 5, "Global_Markets", price=50.0, rank=6)
        fcx2 = _syn_market_row(H, "FCX.US", 6, "Global_Markets", price=50.0, rank=7)
        gm += [fcx, fcx2]                                                      # DQ-10 bare pair, DQ-01 (counts off)
        setv(gm[4], "Day High", 23.0)                                          # DQ-18 price 24 above Day High (low 22 < high 23)
        setv(gm[4], "Day Low", 22.0)
        setv(ml[0], "52W High", THRESHOLDS["w52_sentinel"])                    # DQ-19 sentinel (+ position mismatch)
        setv(ml[1], "Percent Change", 1.0)                                     # DQ-20 x100
        setv(ml[2], "Last Updated (UTC)", "2026-10-07T18:00:00+00:00")         # DQ-21 three days old
        setv(ml[2], "Last Updated (Riyadh)", "2026-10-07T21:00:00+03:00")
        setv(ml[3], "Warnings", "price_bar_stale:2026-10-01:7s;rel_path:sheet")   # DQ-22 stale on WATCHLIST (S4 is WATCH)
        setv(ml[4], "Last Updated (Riyadh)", "2026-10-10T20:00:00+03:00")      # DQ-23 +2h
        setv(gm[1], "Debt/Equity", 250.0)                                      # DQ-25 untagged (G2 DQ 99 -> not DQ-30)
        setv(ml[5], "Dividend Yield", 0.3)                                     # DQ-26
        setv(ml[1], "Beta (5Y)", 40.0)                                         # DQ-27 (S2 DQ 99 -> not DQ-30)
        setv(gm[2], "P/E (TTM)", 0.0)                                          # DQ-28 with Valuation Score
        setv(gm[3], "RSI (14)", 50.0)                                          # DQ-29
        setv(mf[2], "Expected ROI 12M", 0.20)                                  # DQ-31 (fp/price-1 = 0.10)
        setv(gm[4], "Expected ROI 12M", None)                                  # DQ-32 FP12 present, ROI blank
        setv(gm[2], "Forecast Price 12M", 22.0 * 1.40)                         # DQ-33 ROI12 0.40 provider_target
        setv(gm[2], "Expected ROI 12M", 0.40)
        setv(gm[0], "Warnings", "sheet_tuple_conflict:expected_roi_12m;rel_path:sheet")   # DQ-34 on INVESTABLE G1
        setv(gm[1], "Warnings", "analyst_lkg:200h;rel_path:sheet")             # DQ-35 on INVESTABLE G2
        setv(ml[2], "Recommendation Reason", "synthetic row horizon=month")    # DQ-36
        setv(ml[3], "Warnings", "price_bar_stale:2026-10-01:7s;f7_settle:observe:st2:p3:ov=70.0>62.0:rc=hold>sell")  # DQ-37 flip
        setv(ml[4], "Warnings", "f7_settle_failed:non_converged;rel_path:sheet")   # DQ-37 non-converged
        setv(ml[0], "Recommendation", "SELL")                                  # DQ-38 INVEST on SELL
        for r in ml[:4]:
            setv(r, "Forecast Reliability Score", 59.3)                        # DQ-39 mode > 25 %
        for r in (ml[0], ml[1], gm[0], gm[1]):
            setv(r, "Opportunity Source", "both_present_fallback")             # DQ-40
        setv(gm[1], "Rank (Overall)", 1.0)                                     # DQ-41 duplicate rank
        for r in ml + gm + cfx + mf:
            setv(r, "Row Source", None)                                        # DQ-42 dead column
            filler = ";".join(f"rel_path:p{j}:{'x' * 40}" for j in range(23))
            setv(r, "Warnings", (_s(r[hx["Warnings"]]) + ";" + filler).strip(";"))   # DQ-43 >1,200 chars / 25 tags
            setv(r, "Recommendation Reason", _s(r[hx["Recommendation Reason"]]) + " " + "x" * 120)   # DQ-44
        grid_txt = "OK | allocated=7,000,000 (70.00%) | free=3,000,000 | run=1"   # DQ-03
        panel = (2.0, 30.0, 20.0)                                              # DQ-47
        cand_news = ["Unknown", "Unknown"]                                     # DQ-48
        cal_syms = [s for s in cal_syms if s != "S1.SR"]                       # DQ-49 board seat uncovered
        alerts.insert(0, ("missing_fx", 13.0, "Add FX rate(s) to _Lists_Config: USX"))   # DQ-50
        alerts[2] = ("cash_floor", 0.0, f"Cash floor 10% of NAV {int(nav) + 50:,} SAR = {int(nav * 0.1):,} SAR (observe)")   # DQ-60 off by 50
        target_weight = None                                                   # DQ-51
        inv_view_extra = ["X9.US"]                                             # DQ-52
        pd_actions = [("BLOCK", holding, None, None, None), ("BLOCK", "OLD.US", None, None, None)]   # DQ-46 / DQ-53
        pd_kpi_blank = True
        pal_runs = [("2026-10-08 03:00", ["ADD", "HOLD"]), ("2026-10-09 03:00", ["BLOCK", "BLOCK"]),
                    ("2026-10-10 03:20", ["BLOCK", "BLOCK"]), ("2026-10-10 09:50", ["BLOCK", "BLOCK"])]   # DQ-46 consecutive 3
        ledger.append(["YUM", "Yum", "USD", "Inactive", dt.datetime(2026, 1, 1), 100.0, 2.0, None, dt.datetime(2026, 10, 1), 110.0, 1.0, 0.0, None, 200.0, "-", 3.809, "-", "-", 220.0, 20.0, 20.0, 0.1, 273.0, None, None, None])
        # DQ-17 bare YUM, DQ-54 placeholder date, DQ-55 blank Buy Fees, DQ-56 FX 3.809, DQ-59 extra fill
        cash_rows[1] = [dt.date(2026, 10, 9), None, "SNAPSHOT (broker read)", None, cash + 10.0, None, "opening", None, "SAR", "settled_cash", cash + 10.0, 1.0, None]
        # DQ-57: no Time / no Account, Delta blank, Amount blank, duplicate date, identical note while balance changed
        dividends.append(list(dividends[0]))                                   # DQ-58 duplicate key
        dividends[1][0] = "2026-09-08 01:09:12"
        notes = 0                                                              # DQ-59
        perf_target[0] = 12.5                                                  # DQ-62 points beside fraction
        perf_keys[1] = perf_keys[0]                                            # DQ-63 duplicate matured key
        perf_risk = (0.0, 0.0, 0.0)                                            # DQ-63 dead columns
        sel_rows[0][26] = "2026-09-01"                                         # DQ-65 overdue
        sel_rows[0][29] = ""
        pit_rows[0][10] = "▲ 26.32%"                                      # DQ-66 glyph
        pit_rows.append(["2026-10-10", "FORECAST", "", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None])  # junk row
        hyp_sample = 0.0                                                       # DQ-67
        regret.append(["2026-10-02", "COMPLIANCE_BLOCK", "G4.US", "G1.US", "retired screen", 20.0, 21.0, 90.0, None, 0.05, 0.02, 0.03, ""])   # DQ-68
        regime_state = "UNKNOWN"                                               # DQ-69
        runlog_stamps[1] = "08/31/2026 13:51:09"                               # DQ-70 unparseable
        runlog_levels = ["INFO", "WARN", "WARNING"]                            # DQ-70 WARN/WARNING
        status_vocab = ["SUCCESS", "success", "OK", "SUCCESS"]                 # DQ-71
        dash_cov["Global_Markets"] = 1500.0                                    # DQ-72
        s1_detail = "2/10 scored days · 8 excluded-infra"                 # DQ-73
        quota_line = quota_line.replace("rows402 new=0", "rows402 new=3")      # DQ-74 402 events
        http_rows = [("502", "/v1/advanced/sheet-rows")] * 11                  # DQ-74 502 x11
        durations[1] = 3_700_000.0                                             # DQ-75
        orphan_tab = "Sheet97"                                                 # DQ-05
        ghost_dict = True                                                      # DQ-06
        padding_rows = 60                                                      # DQ-02

    # ---- market pages, investable view, portfolio ---------------------------------------------------
    ws = sheet("Market_Leaders", [H] + ml)
    for _ in range(padding_rows):
        ws.append([""] + [None] * (len(H) - 1))
    sheet("Global_Markets", [H] + gm)
    sheet("Commodities_FX", [H] + cfx)
    sheet("Mutual_Funds", [H] + mf)
    inv_rows = [r for r in ml + gm + cfx + mf if r[hx["Final Action"]] == "INVEST"]
    inv_rows += [_syn_market_row(H, s, 9, "Global_Markets", invest=True, price=9.0, rank=9) for s in inv_view_extra]
    sheet(INVESTABLE_VIEW, [H] + inv_rows, hidden=True)
    px = {h: i for i, h in enumerate(PH)}
    hold_row = [None] * len(PH)
    for k, v in {"Symbol": holding, "Name": "Holding Co", "Asset Class": "Equity", "Exchange": EXCHANGE_CANONICAL, "Currency": "USD",
                 "Country": "USA", "Sector": "Energy", "Industry": "Synthetic", "Current Price": hold_px, "Previous Close": hold_px / 1.01,
                 "Horizon Days": 365.0, "Invest Period Label": "1Y", "Position Qty": shares, "Avg Cost": 19.0, "Data Provider": "eodhd",
                 "Last Updated (UTC)": SYN_UPD_UTC, "Last Updated (Riyadh)": SYN_UPD_RIY, "Warnings": "rel_path:sheet",
                 "Scoring Schema Version": "5.11.3", "Data Quality Score": 100.0, "Forecast Reliability Score": 80.0,
                 "Investability Status": "INVESTABLE", "Final Action": "INVEST", "Buy Date": "2026-09-28", "Target Weight %": target_weight,
                 "Actual Weight %": 100.0, "Weight Gap": 75.0 if target_weight is not None else None, "Rebalance Action": "HOLD",
                 "Recommendation": "BUY", "Recommendation Reason": "synthetic horizon=year"}.items():
        hold_row[px[k]] = v
    sheet(PORTFOLIO_PAGE, [PH, hold_row])
    # ---- Data_Dictionary -------------------------------------------------------------------------
    dd = [["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]]
    for p in MARKET_PAGES:
        dd += [[p, "g", h, h.lower(), "str", "", "No", "engine", ""] for h in H]
    dd += [[PORTFOLIO_PAGE, "g", h, h.lower(), "str", "", "No", "engine", ""] for h in PH]
    if ghost_dict:
        dd += [["My_Investments", "g", h, h.lower(), "str", "", "No", "engine", ""] for h in H]
    sheet("Data_Dictionary", dd, hidden=True)
    # ---- _Status ---------------------------------------------------------------------------------
    st = [["Page", "Last Updated", "Status", "Message", "Endpoint", "HTTP Code", "Rows", "Columns", "Duration ms", "Warnings", None, "Global Key", "Value"]]
    globals_ = [("TFB Grid Capacity", grid_txt), ("TFB Decision Feed", "ACTIONABLE | run=1 | 2026-10-10 21:37:23+03:00 | ok"),
                ("Backend URL", "https://example.invalid"), ("Token Loaded", "Yes")]
    for i, p in enumerate(MARKET_PAGES):
        g = globals_[i] if i < len(globals_) else (None, None)
        st.append([p, status_stamps[i], status_vocab[i], f"[STATUS-STAMP v6.64.14] leg=success written={expect[p]} failed=0", "backend:run_dashboard_sync",
                   None, float(expect[p]), 115.0, durations[i], 0.0, None, g[0], g[1]])
    sheet("_Status", st, hidden=True)
    # ---- Top_10 ----------------------------------------------------------------------------------
    t10 = [["TOP 10 INVESTMENTS - DECISION"], ["Status:", "Last run 2026-10-10 19:28:22 | status: ok | output: EXEC"], [],
           ["CONTROL PANEL"], ["T10: Universe Scope", "All Main Sheets", None, "T10: Max Selected", 10.0, None, "T10: Period (Months)", 12.0],
           ["T10: Min Reliability", 70.0, None, "T10: Min Data Quality", 80.0, None, "T10: Max Per Sector", panel[0]],
           ["T10: Require Investable", "No", None, "T10: Stability Enabled", "Yes"], ["KPIs"],
           ["Deployable (SAR)", "Exp. Gain 12M (SAR)", "Selected", "Blended Reliability", "Blended R/R (TP2)", "Scanned", "Passed", "Unallocated (SAR)"],
           [cash, 0.0, "2 exec + 0 pend / 10", 80.0, 2.5, 18.0, 4.0, 0.0], [],
           ["FEED ACTIONABLE - EXECUTABLE"], ["Rank", "Symbol", "Name", "Market", "Sector", "Ccy", "FX→SAR", "Price", "Ticket SAR", "Shares", "Stop SAR", "TP1 SAR", "TP2 SAR"]]
    for i, s in enumerate(board):
        t10.append([float(i + 1), s, f"{s} Company", "X", "Energy", "USD", fx, 20.0, 100.0, 5.0, 18.0, 22.0, 24.0])
    t10 += [[], [f"ALL QUALIFIED - INVEST opportunity set ({len(invest_syms)})"], ["Rank", "Symbol", "Name", "Market"]]
    t10 += [[float(i + 1), s, f"{s} Company", "X"] for i, s in enumerate(invest_syms)]
    t10 += [[], [f"ALERTS ({len(alerts)})"], ["Type", "Count", "Required Action"]]
    t10 += [[a, n, txt] for a, n, txt in alerts]
    t10 += [[], ["DATA GAPS - WHY CANDIDATES DIDN'T QUALIFY"], ["Blocking Gate", "Candidates Failed", "Share %", "Example Symbols"], ["Investability", 1.0, 50.0, "X"],
            [], ["CANDIDATES - FULL AUDIT - top rows"],
            ["Symbol", "Name", "Market", "Sector", "Ccy", "Price", "Price SAR", "ROI % (TP1)", "Engine ROI % (12M)", "Ann ROI %", "R/R (TP2)", "Rel", "DQ", "Risk",
             "News", "Sector Trend", "Conflict", "Verdict", "Conf", "Score", "First Fail", "Failure Reason", "Structural", "Selected", "Deferral"]]
    for s, news in zip(board, cand_news):
        t10.append([s, f"{s} Company", "X", "Energy", "USD", 20.0, 75.0, 10.0, 10.0, 10.0, 2.5, 80.0, 100.0, "LOW", news, "Positive", "No", "INVEST", "HIGH", 70.0, "", "", "No", "Yes", ""])
    t10.append(["route v4.18.1 | builder v1.25.0 | pool=body_rows/18 | funding=final/2exec | 100ms | req synthetic"])
    sheet("Top_10_Investments", t10)
    # ---- Portfolio_Decision ----------------------------------------------------------------------
    pdr = [["MY PORTFOLIO - DECISION"], ["Status:", "Last run 2026-10-10 19:30:11 | status: ok | route v4.18.1 | actions v1.15.1"], [],
           ["CONTROL PANEL"], ["PF: Cash Available SAR", cash, None, "PF: Target Cash %", 10.0, None, "PF: Max Position %", panel[2]],
           ["PF: Max Sector %", panel[1], None, "PF: Min Reliability to Add", 70.0, None, "PF: Min DQ to Add", 80.0], ["KPIs"],
           ["Portfolio (SAR)", "Holdings (SAR)", "Cash (SAR)", "Cash %", "P&L (SAR)", "P&L %", "Deployable (SAR)", "Adds Funded (SAR)", "Unallocated (SAR)", "Actions A/H/T/E/B"],
           [None] * 6 + [0.0, 0.0, 0.0, "0 / 0 / 0 / 0 / 2"] if pd_kpi_blank else [nav, mv, cash, 0.5, 10.0, 0.01, 0.0, 0.0, 0.0, "0 / 1 / 0 / 0 / 0"],
           [f"ACTIONS - one per holding ({len(pd_actions)})"],
           ["Action", "Symbol", "Name", "Sector", "Ccy", "Qty", "Avg Cost", "Price", "Price SAR", "MV SAR", "Cost SAR", "P&L SAR", "P&L %", "Weight %", "Δ SAR",
            "Δ Shares", "Funds From", "Stop SAR", "TP1 SAR", "TP2 SAR", "ROI %", "Engine ROI %", "Rel", "DQ", "Conf", "Advisor Note"]]
    for act, s, stop, tp1, tp2 in pd_actions:
        pdr.append([act, s, f"{s} Company", "Energy", "USD", shares, 19.0, hold_px, hold_px * fx, mv, 190.0 * fx, 10.0, 0.05, 100.0, 0.0, 0.0, "", stop, tp1, tp2, 0.1, 0.1, 80.0, 100.0, 70.0, ""])
    pdr += [[], ["ALERTS (0)"], ["Type", "Count", "Required Action"], ["route v4.18.1 | actions v1.15.1 | builder v1.25.0 | 8ms"]]
    sheet("Portfolio_Decision", pdr)
    # ---- _Portfolio_Action_Log --------------------------------------------------------------------
    pal = [["Logged At", "Run Info", "Source Page", "Action", "Symbol", "Name", "Sector", "Ccy", "Qty", "Avg Cost", "Price", "Price SAR", "MV SAR", "Cost SAR",
            "P&L SAR", "P&L %", "Weight %", "Δ SAR", "Δ Shares", "Funds From", "Stop SAR", "TP1 SAR", "TP2 SAR", "ROI %", "Engine ROI %", "Rel", "DQ", "Conf",
            "Advisor Note", "Panel Snapshot", "Outcome", "Review Notes"]]
    for stamp, acts in pal_runs:
        for j, a in enumerate(acts):
            pal.append([stamp + ":00", "run", "Portfolio_Decision", a, f"H{j}.US"] + [None] * 25 + ["", ""])
    sheet("_Portfolio_Action_Log", pal, hidden=True)
    # ---- ledger, cash, dividends, trade notes -----------------------------------------------------
    led = [["_Portfolio_CostBasis - INVESTMENT LEDGER"], ["Status:", "Last refresh 2026-10-10 19:00:00 | synthetic"], ["BLUE = operator input"],
           ["Symbol", "Name", "Ccy", "Status", "Buy Date", "Buy Price", "Shares", "Buy Fees", "Sell Date", "Sell Price", "Sell Fees", "Dividends Recv", "Notes",
            "Cost Basis", "Current Price", "FX→SAR", "Market Value SAR", "Unrealized G/L SAR", "Sell Proceeds", "Realized G/L SAR", "Total Return SAR",
            "Return %", "Holding Days", None, None, None]] + ledger
    sheet("_Portfolio_CostBasis", led)
    sheet("_Cash_Snapshot", [["Date", "Time", "Type", "Amount SAR", "Balance SAR", "Delta vs Prev", "Note", "Account", "Currency", "Balance Type", "Balance", "FX to SAR", "FX As Of"]] + cash_rows)
    sheet("_Dividend_Log", [["Logged At", "Type", "Symbol", "Lot Buy Date", "Ex-Date", "Amount/Share", "Shares", "Amount", "Ccy", "Note"]] + dividends, hidden=True)
    tn = [["Note ID", "Logged At (Riyadh)", "Type", "Trade Date", "Symbol", "Side", "Qty", "Price", "Ccy", "Venue", "Board Ref", "Origin", "Divergence", "Reason Class",
           "Reason Ref", "System Verdict At Time", "1 Thesis", "2 Evidence", "3 Benchmark", "4 Exit Rule", "5 Size Rationale", "Size % NAV", "Outcome", "Outcome P&L SAR",
           "Scored At (Riyadh)", "Scored By", "Ledger Ref", "Writer Version"]]
    tn += [[f"TN-{i}", "2026-10-01 10:00:00", "FILL", "2026-09-28", "G1.US", "BUY", 10.0, 19.0, "USD", "NYSE", "", "board", "No", "", "", "INVEST",
            "t", "e", "b", "x", "s", 0.05, "", None, "", "", "", "1.1.0"] for i in range(notes)]
    sheet("_Trade_Notes", tn, hidden=True)
    # ---- Calendar, Signal_History, Performance_Log, _Selection_Log, PIT, Hypothesis, Regret, Regime ---
    sheet("Calendar_Events", [["Symbol", "Next Earnings Date", "Days To Earnings", "Next Ex-Div Date", "Days To ExDiv", "Updated At (Riyadh)", "Source"]]
          + [[s, "2026-11-05", 26.0, "", None, "2026-10-10 19:40:00", "yahoo"] for s in cal_syms], hidden=True)
    sh = [["Snapshot ID", "Key", "Symbol", "Date (Riyadh)", "Recorded At (Riyadh)", "Recommendation", "Final Action", "Investability", "Overall Score",
           "Forecast Reliability", "Data Quality", "Risk Score", "Price", "Origin Tab", "Analyst Rating", "Target Price", "Days To Earnings", "Days To ExDiv"]]
    for i, s in enumerate(invest_syms):
        sh.append([f"snap-{i}", f"{s}|20261010", s, "2026-10-10", "2026-10-10 19:39:40", "BUY", "INVEST", "INVESTABLE", 70.0, 80.0, 100.0, 40.0, 20.0, "Global_Markets", "Buy", 22.0, 26.0, None])
    if not clean:
        sh.append(list(sh[1]))                                                 # DQ-61 duplicate symbol-day
    sheet("Signal_History", sh, hidden=True)
    pl = [[None] * 32 + ["Performance Summary", None, None, "Updated: 2026-10-10 21:35:34"], [None] * 32 + ["Total Records", 4.0],
          [None] * 32 + ["Active Records", 0.0], [None] * 32 + ["Matured Records", 4.0],
          ["Record ID", "Key", "Symbol", "Horizon", "Date Recorded (Riyadh)", "Entry Price", "Entry Recommendation", "Entry Score", "Risk Bucket", "Confidence", "Origin Tab",
           "Target Price", "Target ROI %", "Target Date (Riyadh)", "Status", "Current Price", "Unrealized ROI %", "Realized ROI %", "Outcome", "Volatility", "Max Drawdown %",
           "Sharpe Ratio", "Sector", "Factor Exposures", "Last Updated (Riyadh)", "Maturity Date", "Notes", "Entry Data Quality", "Entry Forecast Reliability",
           "Entry Investability", "Entry Final Action", "Entry Selected", "Wins", 2.0]]
    for i, (k, t, rz) in enumerate(zip(perf_keys, perf_target, perf_realized)):
        pl.append([f"{i:08d}-0000-4000-8000-000000000000", k, k.split("|")[0], "3M", "2026-06-01 10:00:00", 20.0, "BUY", 70.0, "LOW", "HIGH", "Top_10_Investments",
                   21.0, t, "2026-09-01 10:00:00", "matured", 20.6, rz, rz, "WIN" if rz > 0 else "LOSS", perf_risk[0], perf_risk[1], perf_risk[2], "Energy",
                   '{"mom": 0.1}' if perf_risk[0] else "{}", "2026-09-01 19:00:00", "2026-09-01 19:00:00", "", 100.0, 80.0, "INVESTABLE", "INVEST", "", None, None])
    sheet("Performance_Log", pl, hidden=True)
    sheet("_Selection_Log", [["Logged At", "Run Info", "Source Page", "Rank", "Symbol", "Name", "Market", "Sector", "Ccy", "FX→SAR", "Price", "Price SAR", "Entry Zone",
                              "Ticket SAR", "Shares", "Stop SAR", "TP1 SAR", "TP2 SAR", "ROI %", "Engine ROI %", "Ann ROI %", "Gain 12M SAR", "Rel", "DQ", "Conf", "Funds From",
                              "Review By", "Advisor Note", "Panel Snapshot", "Outcome", "Review Notes", "Stability", "Days"]] + sel_rows, hidden=True)
    sheet("_PIT_Fundamentals", [["Snapshot Date", "Symbol", "Source Page", "Price", "Market Cap", "P/E", "P/B", "EPS", "Dividend Yield %", "Debt Ratio %", "Expected ROI 12M %",
                                 "Value Score", "Recommendation", "Data Provider", "Forecast Source", "Forecast Reliability", "Data Quality", "Confidence Score", "Risk Score",
                                 "Expected ROI 1M %", "Expected ROI 3M %", "Investability", "Row Last Updated", "Captured At (UTC)"]] + pit_rows, hidden=True)
    hyp = [["HYPOTHESIS REGISTRY v6.43.0"], ["as of 2026-10-10"], ["-"], ["-"],
           ["Hypothesis ID", "Name", "Type", "Source Field", "Trigger Value", "Direction", "Horizon (days)", "Min Sample", "Min Effect (bps)", "Min t-stat", "Status", "Sample Size",
            "Hit Count", "Hit Rate %", "Hit Rate Lo %", "Hit Rate Hi %", "Mean Signal ROI %", "Mean Baseline ROI %", "Effect (bps)", "t-stat", "Universe Size", "History Days",
            "Last Backtest (UTC)", "Notes"]]
    for i in range(2):
        hyp.append([f"HYP_{i}", "name", "PRICE_STRUCTURE", "flow", "x", "BULLISH", 20.0, 30.0, 50.0, 2.0, "INSUFFICIENT_DATA" if hyp_sample == 0 else "TESTED", hyp_sample,
                    0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 18.0, 90.0, "2026-10-04T10:00:00+00:00", ""])
    sheet("Hypothesis_Registry", hyp, hidden=True)
    sheet("Regret_Ledger", [["Date", "Fork", "Symbol", "Counterparty", "Reason", "Ref Price", "Alt Price", "Horizon Days", "Scored On", "Forgone %", "Held %", "Regret %", "Notes"]] + regret, hidden=True)
    sheet("Regime_History", [["Captured At (UTC)", "Date (Riyadh)", "Sleeve", "State", "Distance Pct", "Months In State", "Abs Momentum Pct", "Suggested Weight", "Errors", "Regime Version"]]
          + [["2026-10-10T18:00:00+00:00", "2026-10-10", "Saudi", regime_state, 1.0, 2.0, 1.0, 0.5, "", "1.5.0"],
             ["2026-10-10T18:00:00+00:00", "2026-10-10", "Global", "RISK_ON", 1.0, 2.0, 1.0, 0.5, "", "1.5.0"]], hidden=True)
    # ---- _Run_Log, Dashboard_Audit, S1_Gate, _Lists_Config, small tabs -----------------------------
    rl = [["Timestamp", "Level", "Action", "Page", "Status", "Message", "Endpoint", "HTTP Code", "Duration ms", "Details JSON"]]
    for stamp, lvl in zip(runlog_stamps, runlog_levels):
        rl.append([stamp, lvl, "run_dashboard_sync", "Global_Markets", "OK", quota_line if lvl == "INFO" else "sync note", "", "", 1000.0, "{}"])
    for code, ep in http_rows:
        rl.append([dt.datetime(2026, 10, 10, 18, 20, 0), "INFO", "refreshPage", "Insights_Analysis", "OK", "refresh", ep, code, 500.0, "{\"httpCode\":" + code + "}"])
    sheet("_Run_Log", rl, hidden=True)
    da = [["Dashboard Validation", "Generated: 2026-10-10 21:19:49"], ["registry=2.15.0", "engine=core.data_engine_v2=5.151.9", "reader=read_range"],
          ["Page", "Check", "Status", "Count", "Examples"]]
    for p in MARKET_PAGES:
        da.append([p, "scope.coverage", "PASS", dash_cov[p], ""])
        da.append([p, "sanity.roi12_within_softcap", "PASS", 0.0, ""])
    sheet("Dashboard_Audit", da, hidden=True)
    sheet("S1_Gate", [["S-1 GATE v1.9.3", "as of 2026-10-09 Riyadh", "verdict: PASS"], ["criteria ok"], [], [], [], [], [], ["#", "Criterion", "Status", "Detail"],
                      [1.0, "4+ weeks shadow evidence", "PASS", s1_detail], [2.0, "zero compliance violations", "PASS", "none observed"]], hidden=True)
    lc = [["_Lists_Config - TFB reference lists (v1.3.3)"], ["1. DROPDOWN LISTS"], ["x", "y"], [], ["2. FX TABLE → SAR"],
          ["CCY", "Live Rate (GOOGLEFINANCE)", "Static Fallback (operator-editable)", "Effective → SAR", "Source", "Subunit Of"]]
    lc += [[c, 1.0 if c == "SAR" else 3.75, 1.0 if c == "SAR" else 3.75, 1.0 if c == "SAR" else 3.75, "LIVE", ""] for c in lists_fx]
    lc += [[], ["4. CONTROL PANEL DEFAULTS"], ["T10_MAX_PER_SECTOR", 3.0, "4.1"], ["PF_MAX_SECTOR_PCT", 40.0, "3.2"], ["PF_MAX_POSITION_PCT", 10.0, "3.2"],
           ["T10_PERIOD_MONTHS", 12.0, "4.1"], ["T10_REQUIRE_INVESTABLE", "No", "4.1"]]
    sheet("_Lists_Config", lc, hidden=True)
    sheet("Shadow_Board", [["SHADOW BOARD v1.5.0", "as of 2026-10-10 21:32 Riyadh"], ["evaluated=2"], ["Symbol", "Weight"], ["G1.US", 0.5], ["S1.SR", 0.5]], hidden=True)
    sheet("_S1_Calibration", [["As Of (Riyadh)", "State", "N Checkpoints", "Mean Abs Error (pp)", "Mean Signed Error (pp)", "Band (pp)", "Min Sample", "By Horizon", "Detail", "Writer Version"],
                              ["2026-10-10 21:35:18", "PASS", 10.0, 3.0, -0.4, 10.0, 20.0, "", "", "6.43.0"]], hidden=True)
    sheet("_Sync_Control", [["Key", "Value"], ["backend sync hold until", "2026-10-10T18:20:25+00:00", "[SYNC-HOLD v6.64.14] write done"]], hidden=True)
    sheet("Regret_Summary", [["REGRET SUMMARY v1.0.0", "as of 2026-10-09 23:08", "open forks 1", "pending 0"]], hidden=True)
    sheet("Insights_Analysis", [["Section", "Item", "Metric", "Value", "Notes", "Source", "Sort Order"], ["Executive Summary", "Overview", "headline", "2 top pick(s)", "", "insights_builder", 1.0]], hidden=True)
    sheet("_Corporate_Actions", [["Symbol", "Action Type", "Effective Date", "Ratio", "Old Symbol", "New Symbol", "Source", "Detected At", "Notes"]], hidden=True)
    if orphan_tab:
        sheet(orphan_tab, [["stray", 1.0], ["x", 2.0]], hidden=True)
    path = os.path.join(dirpath, "synthetic_clean.xlsx" if clean else "synthetic_defects.xlsx")
    wb.save(path)
    return path


SYN_EXPECT = {"Market_Leaders": 6, "Global_Markets": 5, "Commodities_FX": 4, "Mutual_Funds": 3}   # clean-book live rows
SYN_NOW = dt.datetime(2026, 10, 10, 19, 0, tzinfo=dt.timezone.utc)


def selftest():
    checks = []

    def expect_(ok, label):
        checks.append((bool(ok), label))

    with tempfile.TemporaryDirectory() as tmp:
        clean_path = build_synthetic(tmp, clean=True)
        defect_path = build_synthetic(tmp, clean=False)
        cexp = SYN_EXPECT
        clean = audit_workbook(clean_path, expect=cexp, mode="off")
        clean2 = audit_workbook(clean_path, expect=cexp, mode="off")
        defect = audit_workbook(defect_path, expect=cexp, mode="off")
        defect2 = audit_workbook(defect_path, expect=cexp, mode="off")
        C = {c["id"]: c for c in clean["checks"]}
        D = {c["id"]: c for c in defect["checks"]}
        ids = [c["id"] for c in clean["checks"] if c["id"].startswith("DQ-")]
        expect_(sorted(ids) == [f"DQ-{i:02d}" for i in range(1, 76)] and len(ids) == 75, "catalogue: every DQ-01..DQ-75 id appears exactly once")
        expect_(len(clean["checks"]) == 81, "catalogue: 75 DQ records + 5 NOT_CHECKED + 1 INFO record")
        expect_(all(c["status"] != "PASS" for c in clean["checks"] if c["id"].startswith("NC-")), "NOT_CHECKED records never PASS")
        fails = sorted(k for k, c in C.items() if c["status"] == "FAIL")
        expect_(not fails, "clean book: zero FAIL (" + (", ".join(fails) if fails else "none") + ")")
        expect_(clean["summary"]["critical_fail_count"] == 0 and exit_code(clean) == 0, "clean book: exit code 0")
        expect_(clean["digest"] == clean2["digest"], "determinism: clean digest identical on two runs")
        expect_(defect["digest"] == defect2["digest"], "determinism: defect digest identical on two runs")
        expect_(clean["digest"] != defect["digest"], "determinism: clean and defect digests differ")
        expect_(C["DQ-01"]["status"] == "PASS" and D["DQ-01"]["status"] == "FAIL", "DQ-01 live rows vs EXPECT flips to FAIL")
        expect_(D["DQ-02"]["status"] == "WARN" and D["DQ-02"]["values"].get("Market_Leaders") == 60, "DQ-02 60 padding rows -> WARN")
        expect_(D["DQ-03"]["status"] == "WARN" and D["DQ-03"]["count"] == 70.0, "DQ-03 grid 70 % -> WARN")
        expect_(C["DQ-04"]["status"] == "INFO" and D["DQ-05"]["count"] == 1 and D["DQ-05"]["examples"] == ["Sheet97"], "DQ-05 orphan Sheet97 = 1")
        expect_(D["DQ-06"]["count"] == 1 and D["DQ-06"]["examples"] == ["My_Investments"], "DQ-06 ghost My_Investments = 1")
        expect_(C["DQ-07"]["status"] == "PASS" and C["DQ-07"]["values"].get("Market_Leaders") == 115 and C["DQ-07"]["values"].get("My_Portfolio") == 122, "DQ-07 header contract 115 / 122 PASS")
        expect_(C["DQ-08"]["status"] == "PASS", "DQ-08 in-page duplicates PASS")
        expect_(D["DQ-09"]["count"] == 1 and D["DQ-09"]["status"] == "FAIL", "DQ-09 COPX.US on two pages = 1")
        expect_(D["DQ-10"]["count"] == 1 and D["DQ-10"]["status"] == "FAIL", "DQ-10 bare FCX beside FCX.US = 1")
        expect_(D["DQ-11"]["status"] == "FAIL" and D["DQ-11"]["values"].get("Global_Markets:variant_groups") == 1, "DQ-11 EQUITY/Equity variant group")
        expect_(D["DQ-12"]["count"] == 1 and D["DQ-12"]["status"] == "FAIL", "DQ-12 Financial Services = 1")
        expect_(D["DQ-13"]["count"] == 1 and D["DQ-13"]["status"] == "FAIL", "DQ-13 NASDAQ/NYSE = 1")
        expect_(D["DQ-14"]["count"] == 1 and D["DQ-14"]["status"] == "FAIL", "DQ-14 USX gap = 1")
        expect_(D["DQ-15"]["count"] == 2 and D["DQ-15"]["values"].get("Mutual_Funds:untagged") == 1 and D["DQ-15"]["status"] == "FAIL", "DQ-15 tagged + untagged blank price = 2")
        expect_(D["DQ-16"]["count"] == 1 and D["DQ-16"]["status"] == "FAIL", "DQ-16 SUI-USD Salmonation = 1")
        expect_(D["DQ-17"]["count"] == 1 and D["DQ-17"]["status"] == "FAIL", "DQ-17 bare YUM lot = 1")
        expect_(D["DQ-18"]["count"] == 1 and D["DQ-18"]["status"] == "WARN", "DQ-18 price above Day High = 1")
        expect_(D["DQ-19"]["status"] == "FAIL" and D["DQ-19"]["values"].get("Market_Leaders:sentinel") == 1, "DQ-19 52W sentinel -> FAIL")
        expect_(D["DQ-20"]["count"] == 1 and D["DQ-20"]["status"] == "FAIL", "DQ-20 Percent Change x100 = 1")
        expect_(D["DQ-21"]["count"] == 1 and D["DQ-21"]["status"] == "WARN", "DQ-21 three-day-old stamp = 1")
        expect_(D["DQ-22"]["count"] == 1 and D["DQ-22"]["status"] == "FAIL", "DQ-22 stale bar on WATCHLIST = 1")
        expect_(D["DQ-23"]["count"] == 1 and D["DQ-23"]["status"] == "FAIL", "DQ-23 Riyadh +2h = 1")
        expect_(D["DQ-24"]["count"] == 1 and D["DQ-24"]["status"] == "FAIL", "DQ-24 x100 margin oob = 1")
        expect_(D["DQ-25"]["count"] == 1 and D["DQ-25"]["status"] == "FAIL", "DQ-25 D/E 250 untagged = 1")
        expect_(D["DQ-26"]["count"] == 1 and D["DQ-26"]["status"] == "WARN", "DQ-26 yield 0.3 = 1")
        expect_(D["DQ-27"]["count"] == 1 and D["DQ-27"]["status"] == "WARN", "DQ-27 beta 40 = 1")
        expect_(D["DQ-28"]["count"] == 1 and D["DQ-28"]["values"].get("Global_Markets:zero_with_valuation") == 1, "DQ-28 P/E 0 with Valuation Score = 1")
        expect_(D["DQ-29"]["count"] == 1 and D["DQ-29"]["status"] == "WARN", "DQ-29 RSI 50.0 = 1")
        expect_(D["DQ-30"]["count"] == 1 and D["DQ-30"]["status"] == "FAIL", "DQ-30 garbage fundamental at DQ 100 = 1")
        expect_(D["DQ-31"]["count"] == 1 and D["DQ-31"]["status"] == "FAIL", "DQ-31 ROI identity mismatch = 1")
        expect_(D["DQ-32"]["count"] == 1 and D["DQ-32"]["status"] == "FAIL", "DQ-32 FP12 with blank ROI12 = 1")
        expect_(D["DQ-33"]["count"] == 1 and D["DQ-33"]["status"] == "FAIL" and D["DQ-33"]["values"].get("Global_Markets:source_provider_target") == 1, "DQ-33 ROI12 0.40 provider_target = 1")
        expect_(D["DQ-34"]["count"] == 1 and D["DQ-34"]["status"] == "FAIL", "DQ-34 tuple conflict on INVESTABLE cohort = 1")
        expect_(D["DQ-35"]["count"] == 1 and D["DQ-35"]["status"] == "WARN", "DQ-35 analyst_lkg 200h on INVESTABLE = 1 (WARN, N unset)")
        expect_(D["DQ-36"]["count"] == 1 and D["DQ-36"]["status"] == "FAIL", "DQ-36 label 1Y with horizon=month = 1")
        expect_(D["DQ-37"]["count"] == 1 and D["DQ-37"]["values"].get("non_converged_total") == 1 and D["DQ-37"]["status"] == "FAIL", "DQ-37 rc flip 1, non-converged 1")
        expect_(D["DQ-38"]["count"] == 1 and D["DQ-38"]["status"] == "FAIL", "DQ-38 INVEST on SELL = 1")
        expect_(D["DQ-39"]["status"] == "WARN" and D["DQ-39"]["values"].get("Market_Leaders:frs_mode") == 59.3, "DQ-39 FRS 59.3 mode > 25 % -> WARN")
        expect_(D["DQ-40"]["status"] == "WARN" and D["DQ-40"]["count"] == 4, "DQ-40 fallback on all INVESTABLE -> WARN")
        expect_(D["DQ-41"]["count"] == 1 and D["DQ-41"]["status"] == "FAIL", "DQ-41 duplicate rank = 1")
        expect_(D["DQ-42"]["count"] == 4 and D["DQ-42"]["status"] == "FAIL", "DQ-42 Row Source blank on 4 pages = 4")
        expect_(D["DQ-43"]["status"] == "FAIL" and D["DQ-43"]["values"].get("Global_Markets:mean_tags", 0) >= 25, "DQ-43 1,200-char 25-tag Warnings -> FAIL")
        expect_(D["DQ-44"]["status"] == "WARN", "DQ-44 160-char reasons -> WARN")
        expect_(C["DQ-45"]["status"] == "INFO" and C["DQ-45"]["values"].get("exec_seats") == 2, "DQ-45 feed state INFO, exec seats parsed")
        expect_(D["DQ-46"]["count"] == 3 and D["DQ-46"]["status"] == "FAIL" and D["DQ-46"]["values"].get("block_rows") == 2, "DQ-46 three consecutive BLOCK runs -> FAIL")
        expect_(D["DQ-47"]["count"] == 3 and D["DQ-47"]["status"] == "FAIL", "DQ-47 panel 2/30/20 = 3 drifts")
        expect_(D["DQ-48"]["status"] == "FAIL" and D["DQ-48"]["values"].get("news_unknown") == 2 and C["DQ-48"]["values"].get("rows") == 2, "DQ-48 News Unknown on all candidates -> FAIL; footer excluded")
        expect_(D["DQ-49"]["status"] == "FAIL" and D["DQ-49"]["count"] == 1 and C["DQ-49"]["values"].get("coverage_share") == 1.0, "DQ-49 board seat uncovered -> FAIL")
        expect_(D["DQ-50"]["count"] == 13 and D["DQ-50"]["status"] == "FAIL", "DQ-50 missing_fx 13 -> FAIL")
        expect_(D["DQ-51"]["count"] == 1 and D["DQ-51"]["status"] == "FAIL", "DQ-51 blank Target Weight % -> FAIL")
        expect_(D["DQ-52"]["status"] == "FAIL" and D["DQ-52"]["values"].get("extra_in_view") == 1, "DQ-52 extra row in the investable view -> FAIL")
        expect_(D["DQ-53"]["count"] == 2 and D["DQ-53"]["status"] == "FAIL", "DQ-53 blank stop / tp brackets = 2")
        expect_(D["DQ-54"]["count"] == 1 and D["DQ-54"]["status"] == "FAIL", "DQ-54 Buy Date 2026-01-01 = 1 (local date)")
        expect_(D["DQ-55"]["count"] == 1 and D["DQ-55"]["status"] == "FAIL", "DQ-55 blank Buy Fees = 1")
        expect_(D["DQ-56"]["count"] == 1 and D["DQ-56"]["status"] == "FAIL", "DQ-56 FX 3.809 = 1")
        expect_(D["DQ-57"]["status"] == "FAIL" and D["DQ-57"]["values"].get("certification_errors", 0) >= 1 and D["DQ-57"]["values"].get("duplicate_dates") == 1, "DQ-57 cash row without Time/Account -> FAIL")
        expect_(C["DQ-57"]["status"] == "PASS", "DQ-57 v2-style cash row certifies (clean)")
        expect_(D["DQ-58"]["count"] == 1 and D["DQ-58"]["status"] == "FAIL", "DQ-58 duplicate dividend = 1")
        expect_(D["DQ-59"]["count"] == 3 and D["DQ-59"]["status"] == "FAIL" and C["DQ-59"]["status"] == "PASS", "DQ-59 fills without notes -> FAIL")
        expect_(C["DQ-60"]["status"] == "PASS" and D["DQ-60"]["status"] == "FAIL", "DQ-60 NAV text off by 50 -> FAIL")
        expect_(D["DQ-61"]["count"] == 1 and D["DQ-61"]["status"] == "FAIL", "DQ-61 duplicate Signal_History key = 1")
        expect_(D["DQ-62"]["count"] == 1 and D["DQ-62"]["status"] == "FAIL", "DQ-62 Target ROI 12.5 beside fraction realized = 1")
        expect_(D["DQ-63"]["count"] == 4 and D["DQ-63"]["status"] == "FAIL", "DQ-63 three dead risk columns + duplicate key = 4")
        expect_(C["DQ-64"]["status"] == "WARN", "DQ-64 WARN until a Target Hit column exists")
        expect_(D["DQ-65"]["count"] == 1 and D["DQ-65"]["status"] == "FAIL", "DQ-65 overdue Review By = 1")
        expect_(D["DQ-66"]["status"] == "FAIL" and D["DQ-66"]["values"].get("glyph_cells") == 1 and D["DQ-66"]["values"].get("junk_rows") == 1, "DQ-66 glyph cell + FORECAST junk row -> FAIL")
        expect_(D["DQ-67"]["status"] == "FAIL" and C["DQ-67"]["status"] == "PASS", "DQ-67 Sample Size 0 on all -> FAIL")
        expect_(D["DQ-68"]["count"] == 1 and D["DQ-68"]["status"] == "WARN", "DQ-68 COMPLIANCE_BLOCK fork = 1")
        expect_(D["DQ-69"]["status"] == "FAIL" and D["DQ-69"]["count"] == 1, "DQ-69 Saudi UNKNOWN -> FAIL")
        expect_(D["DQ-70"]["status"] == "WARN" and D["DQ-70"]["values"].get("unparseable") == 1 and D["DQ-70"]["values"].get("duplicate_severity_spellings") == 1, "DQ-70 US-format stamp + WARN/WARNING -> WARN")
        expect_(D["DQ-71"]["count"] == 2 and D["DQ-71"]["status"] == "WARN", "DQ-71 success/OK spellings -> WARN")
        expect_(D["DQ-72"]["count"] == 1 and D["DQ-72"]["status"] == "FAIL", "DQ-72 scope.coverage 1500 = 1")
        expect_(D["DQ-73"]["status"] == "WARN" and D["DQ-73"]["count"] == 8, "DQ-73 8 excluded-infra days -> WARN")
        expect_(D["DQ-74"]["status"] == "FAIL" and D["DQ-74"]["values"].get("events_402") == 3 and D["DQ-74"]["values"].get("events_502") == 11, "DQ-74 rows402 new=3 + 502 x11 -> FAIL")
        expect_(D["DQ-75"]["count"] == 1 and D["DQ-75"]["status"] == "FAIL", "DQ-75 leg 3,700,000 ms -> FAIL")
        # redaction
        try:
            _assert_redacted(clean)
            _assert_redacted(defect)
            _assert_redacted(render_md(clean))
            _assert_redacted(render_md(defect))
            ok = True
        except RedactionError:
            ok = False
        expect_(ok, "redaction: both reports and scorecards pass the guard")
        bad = 0
        for inj in ({"checks": {"DQ-60": {"cash_sar": 1234.5}}}, {"detail": "12,345 SAR"}, {"detail": "12345.6789 SAR"}, {"x": {"y": "equity=99,999 SAR"}}):
            try:
                _assert_redacted(inj)
            except RedactionError:
                bad += 1
        expect_(bad == 4, "redaction: injected balance key and money strings are rejected")
        real = os.getenv("TFB_TEST_EXPORT_XLSX")
        ok_real = True
        if real and os.path.exists(real):
            try:
                _assert_redacted(audit_workbook(real))
            except RedactionError:
                ok_real = False
        expect_(ok_real, "redaction: real-export report passes when TFB_TEST_EXPORT_XLSX is set (vacuous otherwise)")
        # baseline
        cmp_dc = compare_to_baseline(defect, reduce_baseline(clean), "clean.json")
        expect_({"DQ-33", "DQ-38", "DQ-46"} <= set(cmp_dc["decision_critical_regressions"]), "baseline: defect vs clean -> critical regressions include DQ-33, DQ-38, DQ-46")
        expect_(compare_to_baseline(clean, reduce_baseline(clean))["regressions"] == [], "baseline: clean vs clean -> 0 regressions")
        expect_(compare_to_baseline(defect, reduce_baseline(defect))["regressions"] == [], "baseline: defect vs defect -> 0 regressions")
        with_base = audit_workbook(defect_path, expect=cexp, baseline=reduce_baseline(clean), mode="off")
        expect_(exit_code(with_base) == 1 and exit_code(defect) == 1 and exit_code(clean) == 0, "exit codes: 1 on critical regression / critical FAIL, 0 clean")
        # params / digest contract (correction #2)
        j1, j2 = os.path.join(tmp, "a", "out1.json"), os.path.join(tmp, "b", "out2.json")
        os.makedirs(os.path.dirname(j1))
        os.makedirs(os.path.dirname(j2))
        import contextlib
        import io
        exp_arg = ",".join(f"{k}={v}" for k, v in cexp.items())
        with contextlib.redirect_stdout(io.StringIO()):
            rc1 = main([clean_path, "--json", j1, "--md", j1 + ".md", "--quiet", "--expect", exp_arg])
            rc2 = main([clean_path, "--json", j2, "--md", j2 + ".md", "--quiet", "--expect", exp_arg])
        d1 = json.load(open(j1, encoding="utf-8"))
        d2 = json.load(open(j2, encoding="utf-8"))
        expect_(rc1 == 0 and rc2 == 0 and d1["digest"] == d2["digest"], "digest: identical when the same book is written to two output paths")
        expect_(not any(os.sep in str(v) for v in d1["params"].values() if isinstance(v, str)) and d1["export"] == os.path.basename(clean_path), "params and export carry basenames only")
        bo = os.path.join(tmp, "base.json")
        with contextlib.redirect_stdout(io.StringIO()):
            rc3 = main([clean_path, "--baseline-out", bo, "--quiet", "--expect", exp_arg])
        fx = json.load(open(bo, encoding="utf-8"))
        expect_(rc3 == 0 and sorted(fx["checks"]) == [f"DQ-{i:02d}" for i in range(1, 76)] and "examples" not in json.dumps(fx), "baseline-out: reduced fixture holds 75 ids and no examples")
        md = render_md(with_base)
        expect_(all(h in md for h in ("## Tab scorecard", "## Check catalogue", "## DQ scores", "## Trend vs baseline", "## Not checked")), "markdown: every section renders")
        expect_(len(clean["tabs"]) >= 30 and all(t["status"] == "ok" for t in clean["tabs"]) and any(t["status"] == "legacy" for t in defect["tabs"]), "tab records: all known on clean, legacy Sheet97 on defect")
        expect_(clean["rows_summary"]["rows_total"] == 5 and all(c in {"board_seat", "holding", "qualified", "investable", "invest"} for c in {k[:-6] for k in clean["rows_summary"] if k.endswith("_count") and not k.startswith("severity") and k != "rows_total"}), "row records: cohort rows = 4 page rows + 1 holding")
        expect_(all(0 <= v["score"] <= 1 for p in clean["dq_scores"].values() for k, v in p.items() if isinstance(v, dict)), "dq scores: every class score within [0, 1]")
    passed = sum(1 for ok, _ in checks if ok)
    for ok, label in checks:
        print(("PASS " if ok else "FAIL ") + label)
    h = hashlib.sha256("\n".join(label for _, label in checks).encode()).hexdigest()[:16]
    print(f"[DQ-SELFTEST v{SCRIPT_VERSION}] {passed}/{len(checks)} PASS  cases-digest={h}")
    return passed == len(checks)


# --------------------------------------------------------------------------- #
def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("export", nargs="?", help="path to the .xlsx export of the workbook")
    ap.add_argument("--json", help="write the full dq_report.json here (redaction-guarded)")
    ap.add_argument("--md", help="write the markdown scorecard here")
    ap.add_argument("--baseline", help="dq_baseline_*.json or dq_report.json to compare against")
    ap.add_argument("--baseline-out", help="write the reduced fixture (statuses, counts, values) here")
    ap.add_argument("--expect", default=",".join(f"{k}={v}" for k, v in EXPECT.items()), help="expected live rows, Page=N,...")
    ap.add_argument("--asof", help="as-of date YYYY-MM-DD (default: derived from max Last Updated (UTC))")
    ap.add_argument("--now", help="override the reference clocks as ISO UTC (replays)")
    ap.add_argument("--tz-offset", type=int, default=3, help="workbook local offset (Riyadh = 3)")
    ap.add_argument("--rows", action="store_true", help="include the cohort row records in dq_report.json (opt-in)")
    ap.add_argument("--examples-private", action="store_true", help="keep symbol examples on the ledger / cash / holdings checks")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    return ap.parse_args(argv)


def main(argv=None):
    a = parse_args(argv)
    if a.selftest:
        try:
            return 0 if selftest() else 1
        except ImportError as exc:
            sys.stderr.write(f"{exc}\n")
            return 2
    if not a.export:
        sys.stderr.write("usage: tfb_workbook_dq_audit.py export.xlsx [--json ...] [--md ...] [--baseline ...] | --selftest\n")
        return 2
    if not os.path.exists(a.export):
        sys.stderr.write(f"not found: {a.export}\n")
        return 2
    if a.baseline and not os.path.exists(a.baseline):
        sys.stderr.write(f"baseline not found: {a.baseline}\n")
        return 2
    expect = {}
    for part in a.expect.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            try:
                expect[k.strip()] = int(v)
            except ValueError:
                sys.stderr.write(f"bad --expect entry: {part!r}\n")
                return 2
    try:
        now = _parse_dt(a.now, 0) if a.now else None
        report = audit_workbook(a.export, expect=expect or None, asof=a.asof, now_utc=now, baseline=a.baseline,
                                tz_offset=a.tz_offset, include_rows=a.rows, examples_private=a.examples_private)
    except ImportError as exc:
        sys.stderr.write(f"{exc}\n")
        return 2
    md = render_md(report)
    _assert_redacted(report)
    _assert_redacted(md)
    if a.json:
        with open(a.json, "w", encoding="utf-8") as fh:
            json.dump(_jsonable(report), fh, indent=1, default=str)
    if a.md:
        with open(a.md, "w", encoding="utf-8") as fh:
            fh.write(md)
    if a.baseline_out:
        fixture = reduce_baseline(report)
        _assert_redacted(fixture)
        with open(a.baseline_out, "w", encoding="utf-8") as fh:
            json.dump(_jsonable(fixture), fh, indent=1, sort_keys=True, default=str)
    if not a.quiet:
        print(md)
    print(readback_line(report))
    return exit_code(report)


if __name__ == "__main__":
    sys.exit(main())
