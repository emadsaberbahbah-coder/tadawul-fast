#!/usr/bin/env python3
"""
scripts/score_selection_log.py — Selection_Log ticket-vs-reality scorer
=======================================================================
VERSION 1.2.0 (2026-10-27) - [CHALLENGER] (additive; gate default off)

WHY: program register A2 (project doc
TFB_Self_Correcting_Recommendation_Program_2026-10-27.md). The backend now
computes a pre-registered challenger order of the INVEST pool next to the
live order (opportunity_builder v1.28.0, TFB_OPP_RANK_MODE=shadow) and the
cockpit logs both every UTC day (16_Decision_Top10 v1.17.0,
DT10_SHADOW_RANK_LOG=on -> the hidden _Shadow_Rank_Log tab: CHAMPION,
CHALLENGER and BOARD rows). The promotion rule was fixed BEFORE any shadow
data (program doc section 4): primary metric = hit rate of the excess vs the
sector/venue reference at +5 sessions on the names ranked 1-5 by each order,
measured on the same days. Nothing computed it; this version does, nightly.
WHAT (all of it only when TFB_BOARD_SCORECARD_CHALLENGER=on):
  * shadow_events() reads _Shadow_Rank_Log (live: the tab; offline:
    --shadow-tsv), keeps the FIRST event of each UTC Day (the pre-registered
    choice) and the CHAMPION / CHALLENGER rows ranked 1..5; each order
    becomes a sequence of daily snapshots and build_episodes() turns it into
    episodes exactly like board seats (an episode opens on the first day a
    name is in that order's top 5 and closes on the first day it is not).
  * score_shadow_episode(): the base price is the venue's last close BEFORE
    the first session after the logged moment (the log carries no price by
    design), then score_episode() as for seats: +1/+5/+10/+20 and now, vs
    SPUS and vs the episode's sector/venue reference (ref_symbol on the
    logged Sector). Prices ride the same path as the board (the shadow
    symbols and references join the fetch; board price coverage is unchanged).
  * challenger_compare(): per order - trading days (distinct logged UTC
    days Monday-Friday; a weekend log still yields episodes, never a day),
    episodes, scored at +5, hit % at +5 vs reference (vs SPUS when no
    reference), mean and worst excess at +5 vs reference, entries per
    trading day (churn), mean +5 vs SPUS - and ONE verdict by the
    pre-registered rule: NO_DATA (no trading day logged); INSUFFICIENT_DATA
    (< 20 trading days or < 40 scored +5 episodes in either order, window
    < 40 days); PROMOTE_ELIGIBLE (>= 20 days, >= 40 scored each, challenger
    hit >= champion + 10 points, challenger mean >= champion mean, worst not
    lower by > 3 points, churn not higher by > 20%); KEEP_CHAMPION (40
    trading days reached without PROMOTE_ELIGIBLE - inconclusive keeps the
    champion); else CONTINUE. PROMOTE_ELIGIBLE is a recommendation to the
    owner, never an automatic switch.
  * Output: a CHALLENGER vs CHAMPION block (stdout, MD artifact, step
    summary, a fourth block in the summary CSV, the tab below RANKING HEALTH
    or CHURN, above DETAIL); the meta line gains ' | challenger <verdict>';
    the final stdout line ' challenger=<verdict>'; in write mode the _Run_Log
    message ' | challenger: <verdict> (days d, scored c/k)' and the details
    JSON a 'challenger' object. A missing or empty _Shadow_Rank_Log reads as
    NO_DATA, never an error. The live read uses the same raw render as
    _Selection_Log (numbers as numbers, dates as serials): the cockpit's
    'Logged At' / 'Day' text is stored by Sheets as dates.
GATE TFB_BOARD_SCORECARD_CHALLENGER = off (default) | on. Off -> no shadow
read, no extra fetch, every v1.1.0 output byte for byte. _Shadow_Rank_Log is
READ ONLY. Workflow v1.2.0 arms the gate (a NO_DATA block until the backend
and the cockpit log are armed).
ROLLBACK: TFB_BOARD_SCORECARD_CHALLENGER=off. Prior WHY blocks verbatim.
-----------------------------------------------------------------------
VERSION 1.1.0 (2026-10-27) - [RANKING HEALTH] (additive; gate default off)

WHY: the October review (claude/TFB_October_2026_Recommendation_Review.md,
27 Oct 2026) found that the engine's expected-ROI rank runs BACKWARDS: over
the 71 names newly seated in October, Engine ROI at the seat vs the realised
excess return had Spearman -0.38; ranks 1-3 beat the market 38% of the time
vs 70% for ranks 7-10; names at the ROI cap (>= 34) 43% vs 70%. It also
found that the picks beat SPUS only through SECTOR EXPOSURE (73% of seats
were banks, insurers and REITs in a month those rallied): against their own
sector ETF only 44% won, mean -1.1 pp. The nightly scorecard measured the
first effect not at all and the second against SPUS only, so the learning
loop could not see either. v1.1.0 makes the scorecard measure both, every
night, from the same _Selection_Log read, so the loop can correct itself.
WHAT (all of it only when TFB_BOARD_SCORECARD_RANKING=on):
  * (review fix, Codex P2 on PR #847) reference indexes follow their own
    venue clock in _final_bars (REF_VENUE_SUFFIX), so a manual run between
    a foreign close and the US close keeps the index's completed bar.
  * board_events() also reads Rank, Engine ROI %, Rel, Conf and Market
    (tolerant of missing columns); build_episodes() carries the FIRST-seat
    values (rank_at_seat, eng_roi_at_seat, rel_at_seat, conf_at_seat,
    market_at_seat).
  * REFERENCE BENCHMARK per episode next to SPUS: .US names -> the sector
    ETF of the seat's Sector text (Financials XLF, Real Estate VNQ, Energy
    XLE, Information Technology XLK, Health Care XLV, Industrials XLI,
    Consumer Discretionary XLY, Consumer Staples XLP, Utilities XLU,
    Materials XLB, Communication Services XLC; the Yahoo spellings
    'Financial Services', 'Technology', 'Healthcare', 'Consumer Cyclical',
    'Consumer Defensive', 'Basic Materials' match on the leading word(s),
    case-insensitively); other venues -> the venue index (.L ^FTSE,
    .BR/.PA/.MC/.LS/.AS/.DE/.MI ^STOXX50E, .HK ^HSI, .TO ^GSPTSE,
    .AX ^AXJO, .MX ^MXX, .SR ^TASI.SR, .SI ^STI, .T ^N225); unmatched ->
    no reference. The series come through the same price path as SPUS
    (--fetch-yahoo / --prices-csv / --fetch) and the excess uses the same
    date alignment (_bench_ret). DETAIL gains seven columns at the END:
    Rank, Engine ROI %, Rel, Conf, Ref, vs Ref +5 pp, vs Ref Now pp.
  * RANKING HEALTH section (stdout, MD artifact, step summary, summary CSV,
    the tab below CHURN): per window (Since --since, then each seat month)
    episodes / with data / hit % / mean excess vs ref (vs SPUS when the
    episode has no reference series) by Rank 1-3 / 4-6 / 7-10, Engine ROI
    >= 34 / < 34 (ROI_CAP_BUCKET), Conf High / Medium / Other and Sector
    (>= 5 episodes, the rest pooled as Other); Spearman(Engine ROI at seat,
    vs Ref Now pp) and Spearman(Rank at seat, vs Ref Now pp) - plain
    Python: average-tie ranks, then Pearson; ONE verdict row per window:
    RANKING_INVERTED (n >= 30 and Spearman(ROI) <= -0.20, or both the 1-3
    and 7-10 rank buckets have >= 8 scored episodes and the 1-3 hit rate
    is lower by >= 20 points), RANKING_OK (n >= 30, Spearman(ROI) >= +0.10
    and 1-3 hit rate >= 7-10 hit rate), INSUFFICIENT_DATA (n < 30), else
    RANKING_NEUTRAL. In write mode the Since-window verdict is appended to
    the _Run_Log message: ' | ranking: RANKING_INVERTED (spearman -0.38,
    n=71)'; the final stdout line gains ' ranking=<verdict>'.
GATE TFB_BOARD_SCORECARD_RANKING = off (default) | on. Off -> every v1.0.0
output (stdout, CSV/MD artifacts, step summary, tab rectangle, _Run_Log
row) is byte-identical to v1.0.0 (tests/test_board_scorecard_p182.py P9).
_Selection_Log stays READ ONLY; the v0.1.0 ticket path is untouched; no
function removed. Workflow v1.1.0 arms the gate and moves the scheduled
run from observe to write (>20 green observe runs; owner approval 27 Oct).
USAGE (v1.1.0): as v1.0.0 plus TFB_BOARD_SCORECARD_RANKING=on, e.g.
  TFB_BOARD_SCORECARD_RANKING=on python3 scripts/score_selection_log.py \
      --scorecard --tsv _Selection_Log.tsv --prices-csv closes.csv \
      --asof 2026-10-27T06:00:00Z --out-dir /tmp/sc
  (closes.csv may carry the reference series: XLF.US, VNQ.US, ^FTSE, ...)
ROLLBACK: TFB_BOARD_SCORECARD_RANKING=off (or unset) restores the v1.0.0
outputs byte for byte; TFB_BOARD_SCORECARD=observe stops the sheet writes;
nothing else depends on either. Prior WHY blocks below are kept verbatim.
-----------------------------------------------------------------------
VERSION 1.0.0 (2026-10-01) - [P-182 BOARD SCORECARD] (OLD file, rebuilt)

WHY: the Top-10 board has seated a few hundred symbols since 06-20 and
nothing measures what a seat was worth afterwards. v0.1.0 (below) scores
TICKET GEOMETRY (stop/TP walk) for sized rows only: it cannot say whether
the board's picks beat doing nothing, and it never reads the exit rows.
v1.0.0 adds a separate, opt-in --scorecard path; the v0.1.0 ticket path
(--tsv without --scorecard), its CSV and its stdout are byte-unchanged.
  * EPISODES from _Selection_Log snapshots: the seat rows sharing one
    'Logged At' are the full printed board (EMPTY_BOARD sentinel = empty
    board). A symbol's episode opens at the first snapshot that shows it
    and closes at the first later snapshot that does not (DROPPED / BOARD
    EMPTY) or at its exit row; a stability exit row of the same run
    ('[membership exit]', Outcome EXIT: hard|soft) names the reason. Exit
    rows alone cannot close episodes: early-board drops and empty boards
    were never exit-logged (counts in the commit sheet).
  * RETURNS from the board's printed seat price to the venue's k-th session
    close after the seat (k = 1, 5, 10, 20), to the exit close and to the
    latest close. Bar 1 is read on the VENUE's clock (zoneinfo, DST-aware):
    the first session whose close came after the seat moment. Live/partial
    bars are dropped. Guards (no returns): no seat price -> NO_PRICE, no
    closes -> NO_DATA, no closed session yet -> PENDING, first close more
    than 14 days after the seat -> GAP, first close / seat price outside
    0.2..5 -> UNIT? (pence/cents quotes).
  * BENCHMARK SPUS.US: excess pp vs SPUS from its last close before bar 1
    to its last close on/before bar k (date-aligned).
  * OUTPUT: SUMMARY (All / since --since / each seat month x +1 +5 +10 +20
    To Exit To Now: episodes, with data, hit %, mean, median, mean vs SPUS,
    best, worst), CHURN (exit kinds, exits before the first close, median
    hours on board) and DETAIL (one row per episode, newest first).
GATE TFB_BOARD_SCORECARD = off | observe (default) | write
  off     -> --scorecard prints one line and exits.
  observe -> stdout + $GITHUB_STEP_SUMMARY + CSV/MD artifacts (--out-dir);
             ZERO sheet writes (not even _Run_Log).
  write   -> with --live: rewrites the NEW tab _Board_Scorecard as ONE
             rectangle padded to the previous extent + one _Run_Log row
             '[BOARD-SCORECARD v1.0.0] ...'. Price coverage below
             TFB_BOARD_SCORECARD_MIN_COVERAGE (default 0.5) -> the tab is NOT
             overwritten, a DEGRADED _Run_Log row is written, exit code 1.
_Selection_Log stays READ ONLY. Prices: Yahoo chart API (--fetch-yahoo, no
key, no EODHD quota), an offline CSV (--prices-csv symbol,date,close) or
EODHD (--fetch: spends the shared EODHD day budget - not used in CI).
USAGE (v1.0.0):
  python3 scripts/score_selection_log.py --scorecard --live --fetch-yahoo
  python3 scripts/score_selection_log.py --scorecard --tsv _Selection_Log.tsv \
      --prices-csv closes.csv --asof 2026-10-01T06:00:00Z --out-dir /tmp/sc
Workflow: .github/workflows/board_scorecard.yml (NEW, observe by default).
ROLLBACK: TFB_BOARD_SCORECARD=off or delete the workflow; nothing in the
v0.1.0 path depends on the scorecard.
-----------------------------------------------------------------------
v0.1.0 (2026-08-22) — NEW SCRIPT (read-only, artifact-only)

WHY: _Selection_Log holds 763 board selections across 43 days with full
ticket geometry (entry price, Stop SAR, TP1/TP2 SAR) and 714 blank
Outcomes. Nothing in the system evaluates whether those tickets' geometry
worked. This script scores every SIZED ticket against subsequent daily
closes and emits an append-ready CSV artifact.

WHAT IT IS NOT (adjudicated 2026-08-22 before build):
  * NOT an S-1 criterion-1 instrument. Criterion 1 is fed by
    run_shadow_scorer.py over Shadow_History; its blocker is price-feed
    freshness at scoring time (DAY_EXCLUDED_INFRA), not missing labels.
  * NOT a writer to _Selection_Log. The 'Outcome' column has an existing
    writer ("EXIT: soft/hard") whose owner is not identified in the repo;
    this script never touches the sheet at all in v0.1.0.

METHOD:
  * Input: the _Selection_Log TSV export (--tsv) — the same artifact the
    morning audit already uses. Sized rows only (numeric Ticket SAR).
  * Dedup: one scoring unit per (entry_date, symbol), keeping the LAST
    log entry of that day (the day's final ticket geometry).
  * Levels: Stop/TP are logged in SAR; each is converted to the venue
    currency via the row's own FX→SAR so comparisons happen against
    venue-currency closes: level_local = level_SAR / fx.
  * Prices: EODHD EOD closes (fetch mode) or an offline prices CSV
    (--prices-csv: columns symbol,date,close) for CI-less runs and tests.
  * Outcome per ticket, walking closes strictly AFTER entry_date:
        STOP_HIT(d)  first close <= stop_local
        TP1_HIT(d) / TP2_HIT(d)  first close >= tp_local
        same-day stop+TP  -> STOP_HIT (conservative; no intraday path)
        neither yet      -> OPEN(ret%)   |  no price data -> NO_DATA
    TP1 and TP2 are tracked independently; the headline outcome is the
    first terminal event (STOP vs TP1).
  * Output: selection_outcomes.csv + a summary block (hit-rates,
    median days-to-event, TP1-vs-TP2 asymmetry) on stdout. Append-only
    philosophy: the artifact is regenerated whole; nothing is mutated.

ENV: TFB_EODHD_API_KEY only (fetch mode). Offline mode needs none.
USAGE:
  python3 scripts/score_selection_log.py --selftest
  python3 scripts/score_selection_log.py --tsv _Selection_Log.tsv \
      --prices-csv closes.csv --out selection_outcomes.csv
  python3 scripts/score_selection_log.py --tsv _Selection_Log.tsv \
      --fetch --out selection_outcomes.csv        # CI: uses EODHD
"""
from __future__ import annotations

import argparse
import base64
import csv
import json
import math
import os
import re
import statistics
import sys
import time
import urllib.request
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_VERSION = "1.2.0"
EODHD_URL = "https://eodhd.com/api/eod/{sym}?from={frm}&api_token={tok}&fmt=json&period=d"
SKIP_SUFFIX = ("=F",)          # futures: no reliable EOD mapping
SKIP_CONTAINS = ("-USD",)      # crypto pairs: out of scope v0.1.0


def _num(s) -> Optional[float]:
    t = str(s if s is not None else "").replace(",", "").replace("\u2014", "").strip()
    if not t or t in ("-", "—", "N/A"):
        return None
    if t.startswith("(") and t.endswith(")"):
        t = "-" + t[1:-1]
    try:
        return float(t)
    except Exception:
        return None


def load_log(path: str) -> List[Dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as fh:
        rows = list(csv.reader(fh, delimiter="\t"))
    hdr = [c.strip() for c in rows[0]]
    need = ("Logged At", "Symbol", "Price", "FX\u2192SAR", "Ticket SAR",
            "Stop SAR", "TP1 SAR", "TP2 SAR")
    for k in need:
        if k not in hdr:
            raise SystemExit(f"missing column {k!r} in {path}")
    h = {c: i for i, c in enumerate(hdr)}
    out = []
    for r in rows[1:]:
        if len(r) < len(hdr) or not r[h["Symbol"]].strip():
            continue
        out.append({k: r[h[k]] for k in need})
    return out


def sized_units(log_rows: List[Dict[str, str]]) -> "OrderedDict[Tuple[str,str],Dict]":
    """One unit per (entry_date, symbol); last sized entry of the day wins."""
    units: "OrderedDict[Tuple[str,str],Dict]" = OrderedDict()
    for r in log_rows:
        ticket = _num(r["Ticket SAR"])
        px, fx = _num(r["Price"]), _num(r["FX\u2192SAR"])
        stop, tp1, tp2 = (_num(r["Stop SAR"]), _num(r["TP1 SAR"]),
                          _num(r["TP2 SAR"]))
        if not ticket or ticket <= 0 or not px or not fx or fx <= 0:
            continue
        if not stop or not tp1:
            continue
        sym = r["Symbol"].strip()
        if sym.endswith(SKIP_SUFFIX) or any(t in sym for t in SKIP_CONTAINS):
            continue
        d = r["Logged At"][:10]
        units[(d, sym)] = {
            "entry_date": d, "symbol": sym, "entry_px": px, "fx": fx,
            "stop_l": stop / fx, "tp1_l": tp1 / fx,
            "tp2_l": (tp2 / fx) if tp2 else None,
        }
    return units


def load_prices_csv(path: str) -> Dict[str, List[Tuple[str, float]]]:
    px: Dict[str, List[Tuple[str, float]]] = {}
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            c = _num(row.get("close"))
            if c is None:
                continue
            px.setdefault(row["symbol"].strip(), []).append(
                (row["date"].strip(), c))
    for s in px:
        px[s].sort()
    return px


def fetch_prices(symbols: List[str], frm: str,
                 token: str) -> Dict[str, List[Tuple[str, float]]]:
    out: Dict[str, List[Tuple[str, float]]] = {}
    for sym in symbols:
        url = EODHD_URL.format(sym=urllib.request.quote(sym), frm=frm, tok=token)
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8", "replace"))
            out[sym] = sorted((d["date"], float(d["close"])) for d in data
                              if d.get("close") is not None)
        except Exception as exc:                      # noqa: BLE001
            print(f"  [warn] fetch failed {sym}: {exc}", file=sys.stderr)
            out[sym] = []
    return out


def score_unit(u: Dict, closes: List[Tuple[str, float]]) -> Dict:
    """Walk closes strictly after entry_date; conservative same-day rule."""
    res = {"outcome": "NO_DATA", "days": None, "tp1": "", "tp2": "",
           "last_close": None, "ret_pct": None}
    path = [(d, c) for d, c in closes if d > u["entry_date"]]
    if not path:
        return res
    tp1_d = tp2_d = stop_d = None
    for i, (d, c) in enumerate(path, 1):
        if stop_d is None and c <= u["stop_l"]:
            stop_d = i
        if tp1_d is None and c >= u["tp1_l"]:
            tp1_d = i
        if u["tp2_l"] and tp2_d is None and c >= u["tp2_l"]:
            tp2_d = i
        if stop_d is not None or tp1_d is not None:
            break
    res["last_close"] = path[-1][1]
    res["ret_pct"] = round((path[-1][1] / u["entry_px"] - 1) * 100, 2)
    if stop_d is not None and (tp1_d is None or stop_d <= tp1_d):
        res["outcome"], res["days"] = "STOP_HIT", stop_d
    elif tp1_d is not None:
        res["outcome"], res["days"] = "TP1_HIT", tp1_d
    else:
        res["outcome"] = "OPEN"
    res["tp1"] = f"day{tp1_d}" if tp1_d else ""
    res["tp2"] = f"day{tp2_d}" if tp2_d else ""
    return res


def run(tsv: str, out_path: str, prices_csv: Optional[str],
        do_fetch: bool) -> int:
    units = sized_units(load_log(tsv))
    print(f"sized scoring units: {len(units)}")
    syms = sorted({u["symbol"] for u in units.values()})
    if prices_csv:
        prices = load_prices_csv(prices_csv)
    elif do_fetch:
        tok = os.getenv("TFB_EODHD_API_KEY", "").strip()
        if not tok:
            raise SystemExit("TFB_EODHD_API_KEY missing (fetch mode)")
        frm = min(u["entry_date"] for u in units.values())
        prices = fetch_prices(syms, frm, tok)
    else:
        raise SystemExit("need --prices-csv or --fetch")
    rows, agg = [], {"STOP_HIT": 0, "TP1_HIT": 0, "OPEN": 0, "NO_DATA": 0}
    for u in units.values():
        r = score_unit(u, prices.get(u["symbol"], []))
        agg[r["outcome"]] += 1
        rows.append([u["entry_date"], u["symbol"],
                     f'{u["entry_px"]:.4f}', f'{u["stop_l"]:.4f}',
                     f'{u["tp1_l"]:.4f}',
                     f'{u["tp2_l"]:.4f}' if u["tp2_l"] else "",
                     r["outcome"], r["days"] or "", r["tp1"], r["tp2"],
                     r["ret_pct"] if r["ret_pct"] is not None else ""])
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["entry_date", "symbol", "entry_px", "stop_local",
                    "tp1_local", "tp2_local", "outcome", "days_to_event",
                    "tp1_hit", "tp2_hit", "open_ret_pct"])
        w.writerows(rows)
    total = max(1, sum(agg.values()))
    print(f"outcomes: {agg}  | TP1 hit-rate "
          f"{agg['TP1_HIT']/total*100:.1f}%  stop-rate "
          f"{agg['STOP_HIT']/total*100:.1f}%  -> {out_path}")
    return 0


# =========================================================================== #
# v1.0.0 [P-182] BOARD SCORECARD - one row per board seat episode             #
# Opt-in (--scorecard). Nothing above this banner is changed by it.           #
# =========================================================================== #
SCORECARD_TAB = "_Board_Scorecard"
SELLOG_TAB = "_Selection_Log"
BENCH_SYMBOL = "SPUS.US"
HORIZONS = (1, 5, 10, 20)
EMPTY_SENTINELS = ("EMPTY_BOARD",)
EXIT_TAG = "[membership exit]"
EXIT_ATTACH_SECONDS = 600      # an exit row names the reason for a drop <=10 min old
MAX_BAR1_GAP_DAYS = 14         # first close later than this after the seat -> GAP
UNIT_GUARD = (0.2, 5.0)        # first close / seat price outside -> UNIT? (pence etc.)
DEFAULT_SINCE = "2026-09-01"
DEFAULT_OUT_DIR = os.path.join("artifacts", "board_scorecard")
RIYADH_TZ = timezone(timedelta(hours=3))   # Asia/Riyadh: UTC+3, no DST
YAHOO_HOSTS = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")
YAHOO_PATH = ("/v8/finance/chart/{sym}?period1={p1}&period2={p2}"
              "&interval=1d&events=history")
YAHOO_UA = "Mozilla/5.0 (compatible; TFB-board-scorecard/1.0)"
# Venue clock: IANA zone + regular-session close (local). The bar rule reads
# the seat moment on the VENUE's clock, so DST shifts are handled by zoneinfo.
VENUE_CLOSE: Dict[str, Tuple[str, str]] = {
    "SR": ("Asia/Riyadh", "15:00"), "QA": ("Asia/Qatar", "13:00"),
    "AE": ("Asia/Dubai", "15:00"), "KW": ("Asia/Kuwait", "12:40"),
    "BH": ("Asia/Bahrain", "13:00"), "CA": ("Africa/Cairo", "14:30"),
    "T": ("Asia/Tokyo", "15:30"), "HK": ("Asia/Hong_Kong", "16:00"),
    "SS": ("Asia/Shanghai", "15:00"), "SZ": ("Asia/Shanghai", "15:00"),
    "SI": ("Asia/Singapore", "17:00"), "KS": ("Asia/Seoul", "15:30"),
    "KQ": ("Asia/Seoul", "15:30"), "TW": ("Asia/Taipei", "13:30"),
    "TWO": ("Asia/Taipei", "13:30"), "NS": ("Asia/Kolkata", "15:30"),
    "BO": ("Asia/Kolkata", "15:30"), "JK": ("Asia/Jakarta", "16:00"),
    "BK": ("Asia/Bangkok", "16:30"), "KL": ("Asia/Kuala_Lumpur", "17:00"),
    "VN": ("Asia/Ho_Chi_Minh", "14:45"), "PS": ("Asia/Manila", "15:00"),
    "AX": ("Australia/Sydney", "16:00"), "NZ": ("Pacific/Auckland", "16:45"),
    "L": ("Europe/London", "16:30"), "IR": ("Europe/Dublin", "16:30"),
    "DE": ("Europe/Berlin", "17:30"), "F": ("Europe/Berlin", "17:30"),
    "PA": ("Europe/Paris", "17:30"), "AS": ("Europe/Amsterdam", "17:30"),
    "BR": ("Europe/Brussels", "17:30"), "MC": ("Europe/Madrid", "17:30"),
    "MI": ("Europe/Rome", "17:30"), "SW": ("Europe/Zurich", "17:30"),
    "ST": ("Europe/Stockholm", "17:30"), "OL": ("Europe/Oslo", "16:20"),
    "CO": ("Europe/Copenhagen", "17:00"), "HE": ("Europe/Helsinki", "18:30"),
    "LS": ("Europe/Lisbon", "16:30"), "VI": ("Europe/Vienna", "17:30"),
    "WA": ("Europe/Warsaw", "17:00"), "AT": ("Europe/Athens", "17:20"),
    "IS": ("Europe/Istanbul", "18:00"), "JO": ("Africa/Johannesburg", "17:00"),
    "TO": ("America/Toronto", "16:00"), "V": ("America/Toronto", "16:00"),
    "MX": ("America/Mexico_City", "15:00"), "SA": ("America/Sao_Paulo", "17:00"),
    "BA": ("America/Argentina/Buenos_Aires", "17:00"),
    "SN": ("America/Santiago", "16:00"), "US": ("America/New_York", "16:00"),
}
DEFAULT_VENUE = ("America/New_York", "16:00")    # bare tickers / unknown suffix
SUMMARY_HEADER = ["Window", "Horizon", "Episodes", "With Data", "Hit %",
                  "Mean %", "Median %", "Mean vs SPUS pp", "Best %", "Worst %"]
CHURN_HEADER = ["Window", "Episodes", "Closed", "Open", "Exit hard",
                "Exit soft", "Dropped", "Board empty",
                "Exited before 1st close", "Median hours on board"]
DETAIL_HEADER = ["Seat Date", "Seat Time", "Symbol", "Name", "Sector",
                 "Seat Price", "Ccy", "Output at Seat", "Stability at Seat",
                 "Exit Date", "Exit Kind", "Days on Board", "Hours on Board",
                 "+1 %", "+5 %", "+10 %", "+20 %", "To Exit %", "To Now %",
                 "vs SPUS +5 pp", "vs SPUS Now pp", "Status"]
_ZONES: Dict[str, object] = {}
# ---- v1.1.0 [RANKING HEALTH] - all of it behind TFB_BOARD_SCORECARD_RANKING=on
RANKING_ENV = "TFB_BOARD_SCORECARD_RANKING"
ROI_CAP_BUCKET = 34.0          # Engine ROI % at/above the engine's cap -> one bucket
RANKING_MIN_N = 30             # (Engine ROI, excess) pairs needed for a verdict
RANKING_MIN_BUCKET = 8         # scored episodes per rank bucket for the hit-rate rule
RANKING_INVERTED_RHO = -0.20   # Spearman(Engine ROI, excess) at/below -> INVERTED
RANKING_OK_RHO = 0.10          # Spearman(Engine ROI, excess) at/above -> OK (with hits)
RANKING_HIT_GAP_PP = 20.0      # 1-3 hit rate below 7-10 by this much -> INVERTED
SECTOR_MIN_EPISODES = 5        # a sector with fewer episodes is pooled as Other
RANK_BUCKETS: Tuple[Tuple[str, int, int], ...] = (("1-3", 1, 3), ("4-6", 4, 6),
                                                  ("7-10", 7, 10))
RANKING_HEADER = ["Window", "Cut", "Bucket", "Episodes", "With Data",
                  "Hit % (vs ref)", "Mean vs Ref pp", "Mean vs SPUS pp"]
RANKING_TITLE = ("RANKING HEALTH - does the board's order predict the excess "
                 "return? vs ref = the seat's sector ETF (.US) or venue index, "
                 "vs SPUS when the episode has no reference series")
DETAIL_HEADER_RANKING = DETAIL_HEADER + ["Rank", "Engine ROI %", "Rel", "Conf",
                                         "Ref", "vs Ref +5 pp", "vs Ref Now pp"]
# .US names: the seat's Sector text -> US sector ETF. Matched on the leading
# word(s), case-insensitively, GICS and Yahoo spellings alike; the first key
# that the normalised text starts with wins. A bare 'Consumer' is ambiguous
# (Discretionary vs Staples) and gets no reference.
US_SECTOR_REF: Tuple[Tuple[str, str], ...] = (
    ("financ", "XLF.US"), ("real estate", "VNQ.US"), ("energy", "XLE.US"),
    ("information technology", "XLK.US"), ("technology", "XLK.US"),
    ("health", "XLV.US"), ("industrial", "XLI.US"),
    ("consumer discretionary", "XLY.US"), ("consumer cyclical", "XLY.US"),
    ("consumer staples", "XLP.US"), ("consumer defensive", "XLP.US"),
    ("utilit", "XLU.US"), ("basic materials", "XLB.US"), ("materials", "XLB.US"),
    ("communication", "XLC.US"))
# Other venues: the venue index on Yahoo (symbol suffix -> index symbol).
VENUE_REF: Dict[str, str] = {
    "L": "^FTSE", "BR": "^STOXX50E", "PA": "^STOXX50E", "MC": "^STOXX50E",
    "LS": "^STOXX50E", "AS": "^STOXX50E", "DE": "^STOXX50E", "MI": "^STOXX50E",
    "HK": "^HSI", "TO": "^GSPTSE", "AX": "^AXJO", "MX": "^MXX", "SR": "^TASI.SR",
    "SI": "^STI", "T": "^N225"}
# v1.1.0 review fix (Codex P2 on PR #847): a bare index symbol ('^HSI') has no
# suffix, so _venue() used to give it the New York clock and _final_bars()
# dropped its latest completed bar on a manual run between that venue's close
# and the US close (the stock kept the day, its reference lost it -> wrong
# same-day excess). Each reference index now follows the clock of the venue it
# stands for. The scheduled 22:23 UTC run was never affected (every venue is
# closed by then).
REF_VENUE_SUFFIX: Dict[str, str] = {
    "^FTSE": "L", "^STOXX50E": "DE", "^HSI": "HK", "^GSPTSE": "TO",
    "^AXJO": "AX", "^MXX": "MX", "^TASI.SR": "SR", "^STI": "SI", "^N225": "T"}
# ---- v1.2.0 [CHALLENGER] - all of it behind TFB_BOARD_SCORECARD_CHALLENGER=on.
# The rule constants are the program doc's section 4, fixed 27 Oct 2026
# before any shadow data; changing one is a new evaluation.
CHALLENGER_ENV = "TFB_BOARD_SCORECARD_CHALLENGER"
SHADOW_TAB = "_Shadow_Rank_Log"
SHADOW_ORDERS = ("CHAMPION", "CHALLENGER")
CHAL_TOP_N = 5                 # names ranked 1..5 by each order
CHAL_MIN_SCORED = 40           # scored +5 episodes per order
CHAL_MIN_DAYS = 20             # trading days logged (UTC Mon-Fri)
CHAL_MAX_DAYS = 40             # inconclusive at this length -> keep the champion
CHAL_HIT_EDGE_PP = 10.0        # challenger hit % >= champion + this
CHAL_WORST_TOL_PP = 3.0        # worst +5 excess not lower by more than this
CHAL_CHURN_TOL = 1.20          # entries per day not higher than champion x this
CHALLENGER_HEADER = ["Order", "Trading Days", "Episodes", "Scored +5",
                     "Hit % +5 (vs ref)", "Mean +5 vs Ref pp",
                     "Worst +5 vs Ref pp", "Entries / Day", "Mean +5 vs SPUS pp"]
CHALLENGER_TITLE = ("CHALLENGER vs CHAMPION - the backend's two orders of the "
                    "INVEST pool, names ranked 1-5 by each, first logged event "
                    "of each UTC day (pre-registered rule, program doc section 4)")


def _scorecard_mode() -> str:
    """TFB_BOARD_SCORECARD: off | observe (default) | write. Unknown -> observe
    (the safe side: computes, never writes)."""
    v = os.getenv("TFB_BOARD_SCORECARD", "").strip().lower()
    if v in ("off", "0", "false", "no"):
        return "off"
    if v == "write":
        return "write"
    return "observe"


def _min_coverage() -> float:
    v = _num(os.getenv("TFB_BOARD_SCORECARD_MIN_COVERAGE", ""))
    return v if v is not None and 0.0 <= v <= 1.0 else 0.5


def _ranking_enabled() -> bool:
    """v1.1.0: TFB_BOARD_SCORECARD_RANKING = off (default) | on. Only on / 1 /
    true / yes arm it; anything else (unset, 'off', junk) keeps the v1.0.0
    outputs byte-identical."""
    return os.getenv(RANKING_ENV, "").strip().lower() in ("on", "1", "true", "yes")


def _challenger_enabled() -> bool:
    """v1.2.0 gate - DEFAULT OFF (exactly 'on' arms it, any case)."""
    return os.getenv(CHALLENGER_ENV, "off").strip().lower() == "on"


def _num_pct(s) -> Optional[float]:
    """v1.1.0: _num() that also accepts a trailing '%' ('34.5%' -> 34.5)."""
    t = str(s if s is not None else "").strip()
    if t.endswith("%"):
        t = t[:-1]
    return _num(t)


def _rank_int(s) -> Optional[int]:
    """v1.1.0: board rank cell -> int ('1', 1.0, '3.0') or None (blank, '-')."""
    v = _num(s)
    return None if v is None else int(round(v))


def ref_symbol(sym: str, sector: str) -> str:
    """v1.1.0: the episode's reference series. .US (or bare) names -> the US
    sector ETF of the seat's Sector text (US_SECTOR_REF, leading word(s),
    case-insensitive); other suffixes -> the venue index (VENUE_REF);
    '' when neither matches (the episode then falls back to SPUS)."""
    s = str(sym).strip().upper()
    suf = s.rsplit(".", 1)[1] if "." in s else "US"
    if suf != "US":
        return VENUE_REF.get(suf, "")
    t = " ".join(str(sector or "").lower().split())
    for key, etf in US_SECTOR_REF:
        if t.startswith(key):
            return etf
    return ""


def _avg_ranks(vals: List[float]) -> List[float]:
    """v1.1.0: 1-based ranks; tied values share the average of their ranks."""
    order = sorted(range(len(vals)), key=lambda i: vals[i])
    ranks = [0.0] * len(vals)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and vals[order[j + 1]] == vals[order[i]]:
            j += 1
        r = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = r
        i = j + 1
    return ranks


def spearman(xs: List[float], ys: List[float]) -> Optional[float]:
    """v1.1.0: Spearman rank correlation in plain Python - average-tie ranks,
    then Pearson on the ranks. None when n < 2, lengths differ or one side is
    constant (no numpy / scipy)."""
    n = len(xs)
    if n != len(ys) or n < 2:
        return None
    rx, ry = _avg_ranks(list(xs)), _avg_ranks(list(ys))
    mx, my = sum(rx) / n, sum(ry) / n
    sxy = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    sxx = sum((a - mx) ** 2 for a in rx)
    syy = sum((b - my) ** 2 for b in ry)
    if sxx <= 0.0 or syy <= 0.0:
        return None
    return sxy / math.sqrt(sxx * syy)


def _rho_text(rho: Optional[float], digits: int = 2) -> str:
    return "n/a" if rho is None else "%.*f" % (digits, round(rho, digits) + 0.0)


def _parse_logged_at(v) -> Optional[datetime]:
    """'Logged At' -> Riyadh wall time (naive). Accepts a Sheets serial number
    (live read, SERIAL_NUMBER render), ISO text ('T' or space, optional
    offset -> converted to Riyadh) and m/d/Y text. Anything else -> None."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        f = float(v)
    else:
        s = str(v).strip()
        if not s:
            return None
        f = float(s) if re.fullmatch(r"\d{5}(\.\d+)?", s) else None
        if f is None:
            dt = None
            try:
                dt = datetime.fromisoformat(s.replace(" ", "T", 1))
            except ValueError:
                for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M"):
                    try:
                        dt = datetime.strptime(s, fmt)
                        break
                    except ValueError:
                        continue
            if dt is None:
                return None
            if dt.tzinfo is not None:
                dt = dt.astimezone(RIYADH_TZ).replace(tzinfo=None)
            return dt.replace(microsecond=0)
    if not 20000.0 < f < 80000.0:
        return None
    return datetime(1899, 12, 30) + timedelta(seconds=round(f * 86400.0))


def _parse_asof(s: Optional[str]) -> datetime:
    """--asof (UTC unless an offset is given) or the wall clock, tz-aware."""
    if not s:
        return datetime.now(timezone.utc).replace(microsecond=0)
    dt = datetime.fromisoformat(str(s).strip().replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _zone(name: str):
    if name not in _ZONES:
        try:
            from zoneinfo import ZoneInfo
            _ZONES[name] = ZoneInfo(name)
        except Exception:  # noqa: BLE001 - no tzdata: Riyadh clock fallback
            _ZONES[name] = None
    return _ZONES[name]


def _venue(sym: str) -> Tuple[str, str]:
    s = str(sym).strip().upper()
    if s in REF_VENUE_SUFFIX:                 # v1.1.0 review fix: index clock
        return VENUE_CLOSE.get(REF_VENUE_SUFFIX[s], DEFAULT_VENUE)
    suf = s.rsplit(".", 1)[1] if "." in s else "US"
    return VENUE_CLOSE.get(suf, DEFAULT_VENUE)


def _venue_local(sym: str, riyadh_naive: datetime) -> Tuple[str, bool]:
    """(venue-local date 'YYYY-MM-DD', at-or-after the venue close) for a
    Riyadh wall-clock moment."""
    zone_name, hhmm = _venue(sym)
    tz = _zone(zone_name)
    aware = riyadh_naive.replace(tzinfo=RIYADH_TZ)
    loc = aware.astimezone(tz) if tz is not None else aware
    hh, mm = (int(x) for x in hhmm.split(":"))
    return loc.strftime("%Y-%m-%d"), (loc.hour, loc.minute) >= (hh, mm)


def _first_bar_index(sym: str, seat_at: datetime,
                     closes: List[Tuple[str, float]]) -> Optional[int]:
    """Bar 1 = the venue's first session whose close came after the seat."""
    d, after = _venue_local(sym, seat_at)
    for i, (bd, _c) in enumerate(closes):
        if bd > d or (bd == d and not after):
            return i
    return None


def _exit_bar_index(sym: str, exit_at: datetime,
                    closes: List[Tuple[str, float]]) -> Optional[int]:
    """Last bar whose session closed at or before the exit moment."""
    d, after = _venue_local(sym, exit_at)
    idx = None
    for i, (bd, _c) in enumerate(closes):
        if bd < d or (bd == d and after):
            idx = i
        else:
            break
    return idx


def _final_bars(sym: str, closes: List[Tuple[str, float]],
                asof_utc: datetime) -> List[Tuple[str, float]]:
    """Drop a live/partial bar: keep only sessions already closed at asof."""
    if not closes:
        return []
    now_riyadh = asof_utc.astimezone(RIYADH_TZ).replace(tzinfo=None)
    d, after = _venue_local(sym, now_riyadh)
    return [(bd, c) for bd, c in closes if bd < d or (bd == d and after)]


def _cell(r: List[Any], h: Dict[str, int], k: str) -> Any:
    i = h.get(k)
    return r[i] if i is not None and i < len(r) else ""


def board_events(values: List[List[Any]]) -> Tuple[List[Dict[str, Any]],
                                                   Dict[str, int]]:
    """_Selection_Log grid (header + rows; TSV strings or live UNFORMATTED
    values) -> time-ordered seat / exit events. Short rows are tolerated
    (the 30/29-cell exit rows of the 2026-10-01 export)."""
    if not values:
        raise SystemExit("_Selection_Log is empty")
    hdr = [str(c).strip() for c in values[0]]
    need = ("Logged At", "Run Info", "Symbol", "Price", "Outcome")
    miss = [k for k in need if k not in hdr]
    if miss:
        raise SystemExit(f"_Selection_Log missing column(s) {miss}")
    h = {c: i for i, c in enumerate(hdr)}
    stats = {"rows": 0, "events": 0, "bad_time": 0}
    ev: List[Dict[str, Any]] = []
    for r in values[1:]:
        stats["rows"] += 1
        sym = str(_cell(r, h, "Symbol")).strip()
        if not sym:
            continue
        at = _parse_logged_at(_cell(r, h, "Logged At"))
        if at is None:
            stats["bad_time"] += 1
            continue
        info = str(_cell(r, h, "Run Info"))
        outcome = str(_cell(r, h, "Outcome")).strip()
        m_run = re.match(r"\s*Last run (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", info)
        m_out = re.search(r"output: ([A-Z_]+)", info)
        ev.append({
            "at": at, "symbol": sym, "outcome": outcome,
            "is_exit": EXIT_TAG in info or outcome.upper().startswith("EXIT"),
            "run": m_run.group(1) if m_run else "",
            "output": m_out.group(1) if m_out else "",
            "price": _num(_cell(r, h, "Price")),
            "name": str(_cell(r, h, "Name")).strip(),
            "sector": str(_cell(r, h, "Sector")).strip(),
            "ccy": str(_cell(r, h, "Ccy")).strip(),
            "stability": str(_cell(r, h, "Stability")).strip(),
            # v1.1.0 [RANKING HEALTH]: the seat's own ranking facts; every
            # column is optional (_cell -> '' when the header is absent).
            "rank": _rank_int(_cell(r, h, "Rank")),
            "eng_roi": _num_pct(_cell(r, h, "Engine ROI %")),
            "rel": _num_pct(_cell(r, h, "Rel")),
            "conf": str(_cell(r, h, "Conf")).strip(),
            "market": str(_cell(r, h, "Market")).strip(),
        })
    ev.sort(key=lambda e: e["at"])      # stable: same-moment rows keep order
    for k, e in enumerate(ev):
        e["seq"] = k                    # board order inside one snapshot
    stats["events"] = len(ev)
    return ev, stats


def build_episodes(events: List[Dict[str, Any]],
                   now_riyadh: datetime) -> Tuple[List[Dict[str, Any]],
                                                  Dict[str, int]]:
    """Seat rows sharing one 'Logged At' are the full printed board; the
    EMPTY_BOARD sentinel is an empty board. An episode opens at the first
    snapshot showing the symbol and closes at the first later snapshot that
    does not (DROPPED / BOARD EMPTY) or at its exit row; an exit row of the
    same run (or <= EXIT_ATTACH_SECONDS later) names the reason."""
    open_ep: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
    closed: List[Dict[str, Any]] = []
    recent: Dict[str, Dict[str, Any]] = {}
    stats = {"snapshots": 0, "empty_snapshots": 0, "orphan_exits": 0}
    i, n = 0, len(events)
    while i < n:
        j = i
        while j < n and events[j]["at"] == events[i]["at"]:
            j += 1
        group, i = events[i:j], j
        at = group[0]["at"]
        seats = [e for e in group if not e["is_exit"]]
        if seats:
            stats["snapshots"] += 1
            board: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
            for e in seats:
                if e["symbol"] not in EMPTY_SENTINELS:
                    board.setdefault(e["symbol"], e)
            if not board:
                stats["empty_snapshots"] += 1
            for sym in [s for s in open_ep if s not in board]:
                ep = open_ep.pop(sym)
                ep.update(exit_at=at, exit_run=seats[0]["run"],
                          exit_kind="DROPPED" if board else "BOARD EMPTY")
                closed.append(ep)
                recent[sym] = ep
            for sym, e in board.items():
                ep = open_ep.get(sym)
                if ep is None:
                    open_ep[sym] = {
                        "symbol": sym, "seat_at": at, "seat_px": e["price"],
                        "name": e["name"], "sector": e["sector"],
                        "ccy": e["ccy"], "output": e["output"],
                        "stability": e["stability"], "days": {at.date()},
                        "seq": e["seq"], "exit_at": None, "exit_kind": "",
                        "exit_run": "",
                        # v1.1.0: FIRST-seat ranking facts (never updated by
                        # later snapshots of the same episode)
                        "rank_at_seat": e.get("rank"),
                        "eng_roi_at_seat": e.get("eng_roi"),
                        "rel_at_seat": e.get("rel"),
                        "conf_at_seat": e.get("conf", ""),
                        "market_at_seat": e.get("market", "")}
                else:
                    ep["days"].add(at.date())
        for e in group:
            if not e["is_exit"]:
                continue
            sym, kind = e["symbol"], (e["outcome"] or "EXIT")
            ep = open_ep.pop(sym, None)
            if ep is not None:          # exit logged while still on the board
                ep.update(exit_at=at, exit_run=e["run"], exit_kind=kind)
                closed.append(ep)
                recent[sym] = ep
                continue
            ep = recent.get(sym)
            if (ep is not None and ep["exit_kind"] in ("DROPPED", "BOARD EMPTY")
                    and ((e["run"] and e["run"] == ep["exit_run"])
                         or (at - ep["exit_at"]).total_seconds()
                         <= EXIT_ATTACH_SECONDS)):
                ep["exit_kind"] = kind
            else:
                stats["orphan_exits"] += 1
    eps = closed + list(open_ep.values())
    for ep in eps:
        end = ep["exit_at"] or now_riyadh
        ep["hours"] = max(0.0, (end - ep["seat_at"]).total_seconds() / 3600.0)
    eps.sort(key=lambda e: (e["seat_at"], e["seq"]))
    return eps, stats


def _day_key(v) -> Optional[str]:
    """v1.2.0: a _Shadow_Rank_Log 'Day' cell -> 'YYYY-MM-DD' or None. The
    cockpit writes the UTC day as text and Sheets stores it as a date, so a
    live read (SERIAL_NUMBER render) returns a serial; an exported TSV returns
    the displayed text (ISO or m/d/Y)."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)) or re.fullmatch(r"\d{5}(\.\d+)?", str(v).strip()):
        f = float(v)
        if not 20000.0 < f < 80000.0:
            return None
        return (datetime(1899, 12, 30) + timedelta(days=int(f))).strftime("%Y-%m-%d")
    s = str(v).strip()
    m = re.match(r"(\d{4}-\d{2}-\d{2})(?:[ T].*)?$", s)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            return None
    try:
        return datetime.strptime(s.split(" ")[0], "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _trading_day(day: str) -> bool:
    """v1.2.0: a logged UTC day counts toward the rule's 'trading days' only
    Monday-Friday (a weekend log still yields episodes, never a day)."""
    try:
        return datetime.strptime(day, "%Y-%m-%d").weekday() < 5
    except (TypeError, ValueError):
        return False


def shadow_events(values: List[List[Any]]) -> Tuple[Dict[str, List[Dict[str, Any]]],
                                                   Dict[str, int]]:
    """v1.2.0: _Shadow_Rank_Log grid -> {order: board-like events}. Only the
    FIRST event of each UTC Day (earliest Logged At) counts, and only the
    CHAMPION / CHALLENGER rows ranked 1..CHAL_TOP_N. Every row of one event
    shares its Logged At, so build_episodes() reads each kept event as one
    snapshot. An unreadable Day falls back to the UTC date of Logged At
    (Riyadh wall time - 3 h). A missing tab, an empty grid or a missing
    column -> no events."""
    out: Dict[str, List[Dict[str, Any]]] = {o: [] for o in SHADOW_ORDERS}
    stats = {"rows": 0, "days": 0, "events_kept": 0, "bad_time": 0}
    if not values:
        return out, stats
    hdr = [str(c).strip() for c in values[0]]
    if any(k not in hdr for k in ("Logged At", "Day", "Order", "Rank", "Symbol")):
        return out, stats
    h = {c: i for i, c in enumerate(hdr)}
    first_at: Dict[str, datetime] = {}
    rows: List[Tuple[str, datetime, List[Any]]] = []
    for r in values[1:]:
        stats["rows"] += 1
        at = _parse_logged_at(_cell(r, h, "Logged At"))
        if at is None:
            stats["bad_time"] += 1
            continue
        day = _day_key(_cell(r, h, "Day")) or (at - timedelta(hours=3)).strftime("%Y-%m-%d")
        rows.append((day, at, r))
        if day not in first_at or at < first_at[day]:
            first_at[day] = at
    stats["days"] = len(first_at)
    for day, at, r in rows:
        if at != first_at[day]:
            continue
        order = str(_cell(r, h, "Order")).strip().upper()
        rank = _rank_int(_cell(r, h, "Rank"))
        sym = str(_cell(r, h, "Symbol")).strip()
        if order not in out or rank is None or not 1 <= rank <= CHAL_TOP_N or not sym:
            continue
        out[order].append({
            "at": at, "symbol": sym, "outcome": "", "is_exit": False,
            "run": day, "output": order, "price": None, "name": "",
            "sector": str(_cell(r, h, "Sector")).strip(), "ccy": "",
            "stability": "", "rank": rank,
            "eng_roi": _num_pct(_cell(r, h, "Engine ROI %")),
            "rel": _num_pct(_cell(r, h, "Rel")),
            "conf": str(_cell(r, h, "Conf")).strip(),
            "market": str(_cell(r, h, "Market")).strip()})
        stats["events_kept"] += 1
    for order, ev in out.items():
        ev.sort(key=lambda e: (e["at"], e["rank"]))
        for k, e in enumerate(ev):
            e["seq"] = k
    return out, stats


def score_shadow_episode(ep: Dict[str, Any], closes: List[Tuple[str, float]],
                         bench: List[Tuple[str, float]],
                         ref: Optional[List[Tuple[str, float]]] = None) -> Dict[str, Any]:
    """v1.2.0: base = the venue's last close BEFORE the first session after
    the logged moment (the shadow log carries no price), then score_episode()
    exactly as for a board seat. No prior close -> NO_PRICE."""
    if not closes:
        res = score_episode(dict(ep, seat_px=None), closes, bench, ref)
        res["status"] = "NO_DATA"           # no series at all, not a missing base
        return res
    b1 = _first_bar_index(ep["symbol"], ep["seat_at"], closes)
    if b1 is not None and b1 >= 1:
        return score_episode(dict(ep, seat_px=closes[b1 - 1][1]), closes, bench, ref)
    return score_episode(dict(ep, seat_px=None), closes, bench, ref)


def challenger_verdict(days: int, n_champ: int, n_chal: int,
                       champ: Dict[str, Any], chal: Dict[str, Any]) -> Tuple[str, str]:
    """v1.2.0: the pre-registered rule (program doc section 4). Returns
    (verdict, reason)."""
    if days <= 0:
        return "NO_DATA", "no trading day logged in " + SHADOW_TAB
    enough = days >= CHAL_MIN_DAYS and min(n_champ, n_chal) >= CHAL_MIN_SCORED
    if enough:
        ok_hit = (chal["hit"] is not None and champ["hit"] is not None
                  and chal["hit"] >= champ["hit"] + CHAL_HIT_EDGE_PP)
        ok_mean = (chal["mean"] is not None and champ["mean"] is not None
                   and chal["mean"] >= champ["mean"])
        ok_worst = (chal["worst"] is not None and champ["worst"] is not None
                    and chal["worst"] >= champ["worst"] - CHAL_WORST_TOL_PP)
        ok_churn = (chal["churn"] is not None and champ["churn"] is not None
                    and chal["churn"] <= champ["churn"] * CHAL_CHURN_TOL)
        if ok_hit and ok_mean and ok_worst and ok_churn:
            return "PROMOTE_ELIGIBLE", "every condition met - owner decides"
        failed = [n for n, ok in (("hit edge", ok_hit), ("mean", ok_mean),
                                  ("worst", ok_worst), ("churn", ok_churn)) if not ok]
        if days >= CHAL_MAX_DAYS:
            return "KEEP_CHAMPION", ("inconclusive at %d days: %s"
                                     % (days, ", ".join(failed)))
        return "CONTINUE", "not yet: " + ", ".join(failed)
    if days >= CHAL_MAX_DAYS:
        return "KEEP_CHAMPION", ("inconclusive at %d days: scored %d/%d < %d"
                                 % (days, n_champ, n_chal, CHAL_MIN_SCORED))
    return "INSUFFICIENT_DATA", ("days %d/%d, scored +5 %d/%d (need %d each)"
                                 % (days, CHAL_MIN_DAYS, n_champ, n_chal, CHAL_MIN_SCORED))


def challenger_compare(scored_by_order: Dict[str, List[Dict[str, Any]]],
                       days_by_order: Dict[str, int]) -> Tuple[List[List[Any]], Dict[str, Any]]:
    """v1.2.0: CHALLENGER_HEADER rows (one per order + one verdict row) and the
    statistics. Hit/mean/worst use the +5 excess vs the reference (vs SPUS
    when the episode has no reference); churn = episodes opened per trading
    day logged."""
    rows: List[List[Any]] = []
    per: Dict[str, Dict[str, Any]] = {}
    for order in SHADOW_ORDERS:
        sub = scored_by_order.get(order, [])
        xs = []
        for sc in sub:
            v = sc["xr"].get(5)
            xs.append(sc["x"].get(5) if v is None else v)
        xs = [v for v in xs if v is not None]
        sp = [sc["x"].get(5) for sc in sub if sc["x"].get(5) is not None]
        days = days_by_order.get(order, 0)
        st = {"days": days, "episodes": len(sub), "scored": len(xs),
              "hit": (100.0 * sum(1 for v in xs if v > 0) / len(xs)) if xs else None,
              "mean": statistics.fmean(xs) if xs else None,
              "worst": min(xs) if xs else None,
              "churn": (len(sub) / days) if days else None,
              "spus": statistics.fmean(sp) if sp else None}
        per[order] = st
        rows.append([order, days, len(sub), len(xs), _r2(st["hit"]), _r2(st["mean"]),
                     _r2(st["worst"]), _r2(st["churn"]), _r2(st["spus"])])
    days = min(days_by_order.get(o, 0) for o in SHADOW_ORDERS)
    verdict, reason = challenger_verdict(days, per["CHAMPION"]["scored"],
                                         per["CHALLENGER"]["scored"],
                                         per["CHAMPION"], per["CHALLENGER"])
    text = "%s (days %d, scored %d/%d; %s)" % (verdict, days, per["CHAMPION"]["scored"],
                                              per["CHALLENGER"]["scored"], reason)
    rows.append(["Verdict", text, "", "", "", "", "", "", ""])
    return rows, {"verdict": verdict, "reason": reason, "text": text, "days": days,
                  "champion": per["CHAMPION"], "challenger": per["CHALLENGER"]}


def _pct(px: float, base: float) -> float:
    return (px / base - 1.0) * 100.0


def _bench_ret(bench: List[Tuple[str, float]], d1: str,
               dk: str) -> Optional[float]:
    """SPUS % from its last close before d1 to its last close on/before dk."""
    ref = val = None
    for bd, c in bench:
        if bd < d1:
            ref = c
        if bd <= dk:
            val = c
        else:
            break
    if ref is None or val is None or ref <= 0:
        return None
    return _pct(val, ref)


def score_episode(ep: Dict[str, Any], closes: List[Tuple[str, float]],
                  bench: List[Tuple[str, float]],
                  ref: Optional[List[Tuple[str, float]]] = None) -> Dict[str, Any]:
    """v1.1.0: `ref` (optional) is the episode's reference series (sector ETF /
    venue index); its excess ('xr' per horizon, 'xr_now') uses the same date
    alignment as SPUS. Without it the result is the v1.0.0 one."""
    sym, px = ep["symbol"], ep["seat_px"]
    res: Dict[str, Any] = {
        "ep": ep, "seat_date": ep["seat_at"].strftime("%Y-%m-%d"),
        "r": {k: None for k in HORIZONS}, "x": {k: None for k in HORIZONS},
        "r_exit": None, "r_now": None, "x_now": None,
        "xr": {k: None for k in HORIZONS}, "xr_now": None,
        "pre_close_exit": False, "status": ""}
    if px is None or px <= 0:
        res["status"] = "NO_PRICE"
        return res
    if not closes:
        res["status"] = "NO_DATA"
        return res
    b1 = _first_bar_index(sym, ep["seat_at"], closes)
    if b1 is None:
        res["status"] = "PENDING"
        res["pre_close_exit"] = ep["exit_at"] is not None
        return res
    seat_local = _venue_local(sym, ep["seat_at"])[0]
    gap = (datetime.strptime(closes[b1][0], "%Y-%m-%d")
           - datetime.strptime(seat_local, "%Y-%m-%d")).days
    if gap > MAX_BAR1_GAP_DAYS:
        res["status"] = "GAP"
        return res
    ratio = closes[b1][1] / px
    if not UNIT_GUARD[0] <= ratio <= UNIT_GUARD[1]:
        res["status"] = "UNIT?"
        return res
    d1 = closes[b1][0]
    for k in HORIZONS:
        i = b1 + k - 1
        if i < len(closes):
            res["r"][k] = _pct(closes[i][1], px)
            b = _bench_ret(bench, d1, closes[i][0])
            res["x"][k] = None if b is None else res["r"][k] - b
            if ref:
                rb = _bench_ret(ref, d1, closes[i][0])
                res["xr"][k] = None if rb is None else res["r"][k] - rb
    if ep["exit_at"] is not None:
        ei = _exit_bar_index(sym, ep["exit_at"], closes)
        if ei is None or ei < b1:
            res["pre_close_exit"] = True
        else:
            res["r_exit"] = _pct(closes[ei][1], px)
    res["r_now"] = _pct(closes[-1][1], px)
    b = _bench_ret(bench, d1, closes[-1][0])
    res["x_now"] = None if b is None else res["r_now"] - b
    if ref:
        rb = _bench_ret(ref, d1, closes[-1][0])
        res["xr_now"] = None if rb is None else res["r_now"] - rb
    res["status"] = "OPEN" if ep["exit_at"] is None else "CLOSED"
    return res


def _r2(v: Optional[float]) -> Any:
    return "" if v is None else round(v, 2) + 0.0     # + 0.0: no "-0.0"


def _horizon_getters() -> List[Tuple[str, Any, Any]]:
    out: List[Tuple[str, Any, Any]] = []
    for k in HORIZONS:
        out.append(("+%d" % k, (lambda s, k=k: s["r"][k]),
                    (lambda s, k=k: s["x"][k])))
    out.append(("To Exit", lambda s: s["r_exit"], lambda s: None))
    out.append(("To Now", lambda s: s["r_now"], lambda s: s["x_now"]))
    return out


def summarize(scored: List[Dict[str, Any]],
              since: str) -> Tuple[List[List[Any]], List[List[Any]]]:
    """SUMMARY rows (window x horizon) and CHURN rows (All / since)."""
    windows: List[Tuple[str, List[Dict[str, Any]]]] = [
        ("All", list(scored)),
        ("Since " + since, [s for s in scored if s["seat_date"] >= since])]
    for mo in sorted({s["seat_date"][:7] for s in scored}, reverse=True):
        windows.append((mo, [s for s in scored if s["seat_date"][:7] == mo]))
    summary: List[List[Any]] = []
    for wname, sub in windows:
        for hname, rf, xf in _horizon_getters():
            vals = [rf(s) for s in sub if rf(s) is not None]
            xs = [xf(s) for s in sub if xf(s) is not None]
            summary.append([
                wname, hname, len(sub), len(vals),
                _r2(100.0 * sum(1 for v in vals if v > 0) / len(vals)) if vals else "",
                _r2(statistics.fmean(vals)) if vals else "",
                _r2(statistics.median(vals)) if vals else "",
                _r2(statistics.fmean(xs)) if xs else "",
                _r2(max(vals)) if vals else "",
                _r2(min(vals)) if vals else ""])
    churn: List[List[Any]] = []
    for wname, sub in windows[:2]:
        kinds = [s["ep"]["exit_kind"] for s in sub]
        churn.append([
            wname, len(sub), sum(1 for s in sub if s["ep"]["exit_at"] is not None),
            sum(1 for s in sub if s["ep"]["exit_at"] is None),
            sum(1 for k in kinds if k.upper() == "EXIT: HARD"),
            sum(1 for k in kinds if k.upper() == "EXIT: SOFT"),
            kinds.count("DROPPED"), kinds.count("BOARD EMPTY"),
            sum(1 for s in sub if s["pre_close_exit"]),
            _r2(statistics.median([s["ep"]["hours"] for s in sub])) if sub else ""])
    return summary, churn


def detail_rows(scored: List[Dict[str, Any]],
                ranking: bool = False) -> List[List[Any]]:
    """v1.1.0: `ranking=True` appends the seven DETAIL_HEADER_RANKING columns
    at the END of every row (Rank, Engine ROI %, Rel, Conf, Ref, vs Ref +5
    pp, vs Ref Now pp); False -> the v1.0.0 rows."""
    rows: List[List[Any]] = []
    for s in sorted(scored, key=lambda s: (s["ep"]["seat_at"], -s["ep"]["seq"]),
                    reverse=True):
        ep = s["ep"]
        row = [
            s["seat_date"], ep["seat_at"].strftime("%H:%M"), ep["symbol"],
            ep["name"], ep["sector"],
            "" if ep["seat_px"] is None else ep["seat_px"], ep["ccy"],
            ep["output"], ep["stability"],
            ep["exit_at"].strftime("%Y-%m-%d") if ep["exit_at"] else "",
            ep["exit_kind"], len(ep["days"]), round(ep["hours"], 1),
            _r2(s["r"][1]), _r2(s["r"][5]), _r2(s["r"][10]), _r2(s["r"][20]),
            _r2(s["r_exit"]), _r2(s["r_now"]), _r2(s["x"][5]),
            _r2(s["x_now"]), s["status"]]
        if ranking:
            rank = ep.get("rank_at_seat")
            row += ["" if rank is None else rank, _r2(ep.get("eng_roi_at_seat")),
                    _r2(ep.get("rel_at_seat")), ep.get("conf_at_seat", ""),
                    ep.get("ref", ""), _r2(s.get("xr", {}).get(5)),
                    _r2(s.get("xr_now"))]
        rows.append(row)
    return rows


def _excess_vs_ref(s: Dict[str, Any]) -> Optional[float]:
    """v1.1.0: 'vs Ref Now pp', falling back to 'vs SPUS Now pp' when the
    episode has no reference excess (no mapping, or the series is missing)."""
    v = s.get("xr_now")
    return s.get("x_now") if v is None else v


def _ranking_bucket_row(wname: str, cut: str, bucket: str,
                        sub: List[Dict[str, Any]]) -> List[Any]:
    """One RANKING_HEADER row: episodes, with data, hit % (excess vs ref > 0),
    mean excess vs ref, mean excess vs SPUS."""
    xs = [v for v in (_excess_vs_ref(s) for s in sub) if v is not None]
    sp = [s["x_now"] for s in sub if s.get("x_now") is not None]
    return [wname, cut, bucket, len(sub), len(xs),
            _r2(100.0 * sum(1 for v in xs if v > 0) / len(xs)) if xs else "",
            _r2(statistics.fmean(xs)) if xs else "",
            _r2(statistics.fmean(sp)) if sp else ""]


def ranking_verdict(rho: Optional[float], n: int, hit_13: Optional[float],
                    n_13: int, hit_710: Optional[float], n_710: int) -> str:
    """v1.1.0: RANKING_INVERTED when n >= RANKING_MIN_N and Spearman(Engine
    ROI, excess) <= RANKING_INVERTED_RHO, or when both the 1-3 and 7-10 rank
    buckets hold >= RANKING_MIN_BUCKET scored episodes and the 1-3 hit rate
    is lower by >= RANKING_HIT_GAP_PP; RANKING_OK when n >= RANKING_MIN_N,
    Spearman >= RANKING_OK_RHO and the 1-3 hit rate >= the 7-10 hit rate;
    INSUFFICIENT_DATA when n < RANKING_MIN_N; otherwise RANKING_NEUTRAL."""
    by_rho = n >= RANKING_MIN_N and rho is not None and rho <= RANKING_INVERTED_RHO
    by_hit = (n_13 >= RANKING_MIN_BUCKET and n_710 >= RANKING_MIN_BUCKET
              and hit_13 is not None and hit_710 is not None
              and hit_13 <= hit_710 - RANKING_HIT_GAP_PP)
    if by_rho or by_hit:
        return "RANKING_INVERTED"
    if (n >= RANKING_MIN_N and rho is not None and rho >= RANKING_OK_RHO
            and hit_13 is not None and hit_710 is not None and hit_13 >= hit_710):
        return "RANKING_OK"
    if n < RANKING_MIN_N:
        return "INSUFFICIENT_DATA"
    return "RANKING_NEUTRAL"


def ranking_health(scored: List[Dict[str, Any]],
                   since: str) -> Tuple[List[List[Any]], Dict[str, Dict[str, Any]]]:
    """v1.1.0: RANKING HEALTH rows (window x cut x bucket, two Spearman rows,
    one verdict row) and the per-window statistics keyed by window name.
    Windows: 'Since <since>', then each seat month (newest first). Cuts: Rank
    1-3 / 4-6 / 7-10, Engine ROI >= / < ROI_CAP_BUCKET, Conf High / Medium /
    Other, Sector (>= SECTOR_MIN_EPISODES episodes, the rest pooled as
    Other). Hit % = share of scored episodes with excess vs ref > 0 (vs SPUS
    when the episode has no reference)."""
    windows: List[Tuple[str, List[Dict[str, Any]]]] = [
        ("Since " + since, [s for s in scored if s["seat_date"] >= since])]
    for mo in sorted({s["seat_date"][:7] for s in scored}, reverse=True):
        windows.append((mo, [s for s in scored if s["seat_date"][:7] == mo]))
    rows: List[List[Any]] = []
    stats: Dict[str, Dict[str, Any]] = {}
    for wname, sub in windows:
        hit: Dict[str, Tuple[int, Optional[float]]] = {}
        for label, lo, hi in RANK_BUCKETS:
            b = [s for s in sub if s["ep"].get("rank_at_seat") is not None
                 and lo <= s["ep"]["rank_at_seat"] <= hi]
            row = _ranking_bucket_row(wname, "Rank", label, b)
            rows.append(row)
            hit[label] = (row[4], None if row[5] == "" else row[5])
        roi = [s for s in sub if s["ep"].get("eng_roi_at_seat") is not None]
        rows.append(_ranking_bucket_row(
            wname, "Engine ROI", ">= %g" % ROI_CAP_BUCKET,
            [s for s in roi if s["ep"]["eng_roi_at_seat"] >= ROI_CAP_BUCKET]))
        rows.append(_ranking_bucket_row(
            wname, "Engine ROI", "< %g" % ROI_CAP_BUCKET,
            [s for s in roi if s["ep"]["eng_roi_at_seat"] < ROI_CAP_BUCKET]))
        conf = [(str(s["ep"].get("conf_at_seat", "")).strip().lower(), s) for s in sub]
        for label in ("High", "Medium"):
            rows.append(_ranking_bucket_row(
                wname, "Conf", label, [s for c, s in conf if c == label.lower()]))
        rows.append(_ranking_bucket_row(
            wname, "Conf", "Other", [s for c, s in conf if c not in ("high", "medium")]))
        sec = [(str(s["ep"].get("sector", "")).strip() or "Other", s) for s in sub]
        count: Dict[str, int] = {}
        for name, _s in sec:
            count[name] = count.get(name, 0) + 1
        big = sorted((k for k, c in count.items()
                      if c >= SECTOR_MIN_EPISODES and k != "Other"),
                     key=lambda k: (-count[k], k))
        for name in big:
            rows.append(_ranking_bucket_row(wname, "Sector", name,
                                            [s for k, s in sec if k == name]))
        rows.append(_ranking_bucket_row(wname, "Sector", "Other",
                                        [s for k, s in sec if k not in big]))
        pairs = [(s["ep"]["eng_roi_at_seat"], _excess_vs_ref(s)) for s in sub
                 if s["ep"].get("eng_roi_at_seat") is not None
                 and _excess_vs_ref(s) is not None]
        rho_roi = spearman([p[0] for p in pairs], [p[1] for p in pairs])
        n_roi = len(pairs)
        rows.append([wname, "Spearman",
                     "Engine ROI at seat vs excess: rho %s (n=%d)"
                     % (_rho_text(rho_roi, 3), n_roi), n_roi, n_roi, "", "", ""])
        pairs_rk = [(s["ep"]["rank_at_seat"], _excess_vs_ref(s)) for s in sub
                    if s["ep"].get("rank_at_seat") is not None
                    and _excess_vs_ref(s) is not None]
        rho_rk = spearman([p[0] for p in pairs_rk], [p[1] for p in pairs_rk])
        n_rk = len(pairs_rk)
        rows.append([wname, "Spearman",
                     "Rank at seat vs excess: rho %s (n=%d)"
                     % (_rho_text(rho_rk, 3), n_rk), n_rk, n_rk, "", "", ""])
        verdict = ranking_verdict(rho_roi, n_roi, hit["1-3"][1], hit["1-3"][0],
                                  hit["7-10"][1], hit["7-10"][0])
        text = "%s (spearman %s, n=%d)" % (verdict, _rho_text(rho_roi), n_roi)
        rows.append([wname, "Verdict", text, n_roi, "", "", "", ""])
        stats[wname] = {
            "verdict": verdict, "text": text, "spearman_roi": rho_roi,
            "n_roi": n_roi, "spearman_rank": rho_rk, "n_rank": n_rk,
            "hit_1_3": hit["1-3"][1], "n_1_3": hit["1-3"][0],
            "hit_7_10": hit["7-10"][1], "n_7_10": hit["7-10"][0],
            "episodes": len(sub)}
    return rows, stats


def scorecard_rect(meta: str, summary: List[List[Any]], churn: List[List[Any]],
                   detail: List[List[Any]],
                   ranking: Optional[List[List[Any]]] = None,
                   challenger: Optional[List[List[Any]]] = None) -> List[List[Any]]:
    """The whole tab as ONE rectangle (uniform width), written in one call.
    v1.1.0: `ranking` rows (when not None) go below CHURN under
    RANKING_HEADER and DETAIL takes the DETAIL_HEADER_RANKING header."""
    rect: List[List[Any]] = (
        [[meta], [],
         ["SUMMARY - % from the board's printed seat price to later venue "
          "closes (+k = k-th session close after the seat)"], SUMMARY_HEADER]
        + summary
        + [[], ["CHURN - how long seats lasted and how they ended"], CHURN_HEADER]
        + churn)
    if ranking is not None:
        rect += [[], [RANKING_TITLE], RANKING_HEADER] + ranking
    if challenger is not None:             # v1.2.0 [CHALLENGER]
        rect += [[], [CHALLENGER_TITLE], CHALLENGER_HEADER] + challenger
    rect += ([[], ["DETAIL - one row per board seat episode, newest first"],
              DETAIL_HEADER_RANKING if ranking is not None else DETAIL_HEADER]
             + detail)
    width = max(len(r) for r in rect)
    return [list(r) + [""] * (width - len(r)) for r in rect]


def _pad_rect(rect: List[List[Any]], rows: int, cols: int) -> List[List[Any]]:
    """Pad to (rows x cols) with blanks so a shorter scorecard also clears the
    tail of the previous one."""
    out = [list(r) + [""] * (cols - len(r)) for r in rect]
    out += [[""] * cols for _ in range(rows - len(out))]
    return out


def scorecard_markdown(meta: str, summary: List[List[Any]],
                       churn: List[List[Any]], since: str,
                       ranking: Optional[List[List[Any]]] = None,
                       challenger: Optional[List[List[Any]]] = None) -> str:
    """v1.1.0: `ranking` rows (when not None) add a RANKING HEALTH table: the
    'Since <since>' window in full plus the Verdict row of every seat month
    (the tab and the summary CSV carry every window)."""
    keep = ("All", "Since " + since)
    lines = ["### " + meta, "",
             "| " + " | ".join(SUMMARY_HEADER) + " |",
             "|" + "---|" * len(SUMMARY_HEADER)]
    lines += ["| " + " | ".join(str(c) for c in r) + " |"
              for r in summary if r[0] in keep]
    lines += ["", "| " + " | ".join(CHURN_HEADER) + " |",
              "|" + "---|" * len(CHURN_HEADER)]
    lines += ["| " + " | ".join(str(c) for c in r) + " |" for r in churn]
    if ranking is not None:
        lines += ["", RANKING_TITLE, "",
                  "| " + " | ".join(RANKING_HEADER) + " |",
                  "|" + "---|" * len(RANKING_HEADER)]
        lines += ["| " + " | ".join(str(c) for c in r) + " |" for r in ranking
                  if r[0] == "Since " + since or r[1] == "Verdict"]
    if challenger is not None:             # v1.2.0 [CHALLENGER]
        lines += ["", CHALLENGER_TITLE, "",
                  "| " + " | ".join(CHALLENGER_HEADER) + " |",
                  "|" + "---|" * len(CHALLENGER_HEADER)]
        lines += ["| " + " | ".join(str(c) for c in r) + " |" for r in challenger]
    return "\n".join(lines) + "\n"


def _yahoo_symbol(sym: str) -> str:
    s = str(sym).strip()
    return s[:-3] if s.upper().endswith(".US") else s


def parse_yahoo_chart(payload: Any) -> List[Tuple[str, float]]:
    """Yahoo v8 chart JSON -> sorted [(venue-local date, close)]. Dates come
    from timestamp + meta.gmtoffset; None/NaN/<=0 closes are skipped; a
    repeated date keeps the LAST value (the live bar)."""
    try:
        r0 = (payload["chart"]["result"] or [None])[0]
    except (KeyError, TypeError, IndexError):
        return []
    if not r0:
        return []
    off = int((r0.get("meta") or {}).get("gmtoffset") or 0)
    try:
        q = r0["indicators"]["quote"][0]
    except (KeyError, TypeError, IndexError):
        return []
    out: Dict[str, float] = {}
    for t, c in zip(r0.get("timestamp") or [], q.get("close") or []):
        if c is None:
            continue
        try:
            c = float(c)
        except (TypeError, ValueError):
            continue
        if not c > 0:                   # also rejects NaN
            continue
        out[datetime.fromtimestamp(int(t) + off, tz=timezone.utc)
            .strftime("%Y-%m-%d")] = c
    return sorted(out.items())


def fetch_yahoo(symbols: List[str], frm: str, asof_utc: datetime,
                pause: float = 0.25) -> Dict[str, List[Tuple[str, float]]]:
    """Daily closes from the Yahoo chart API (no key, no EODHD quota).
    query1 then query2; a 404 (unknown symbol) is not retried."""
    p1 = int(datetime.strptime(frm, "%Y-%m-%d")
             .replace(tzinfo=timezone.utc).timestamp())
    p2 = int(asof_utc.timestamp()) + 86400
    out: Dict[str, List[Tuple[str, float]]] = {}
    for sym in symbols:
        ysym = urllib.request.quote(_yahoo_symbol(sym), safe="")
        bars: List[Tuple[str, float]] = []
        for host in YAHOO_HOSTS:
            url = "https://" + host + YAHOO_PATH.format(sym=ysym, p1=p1, p2=p2)
            req = urllib.request.Request(
                url, headers={"User-Agent": YAHOO_UA, "Accept": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=20) as resp:
                    bars = parse_yahoo_chart(
                        json.loads(resp.read().decode("utf-8", "replace")))
                break
            except Exception as exc:                  # noqa: BLE001
                if getattr(exc, "code", None) == 404:
                    break
                time.sleep(1.5)
        if not bars:
            print(f"  [warn] no Yahoo closes for {sym}", file=sys.stderr)
        out[sym] = bars
        time.sleep(pause)
    return out


def _read_sellog_tsv(path: str) -> List[List[Any]]:
    with open(path, newline="", encoding="utf-8") as fh:
        return [list(r) for r in csv.reader(fh, delimiter="\t")]


def _read_sellog_live(book) -> List[List[Any]]:
    """Raw values: numbers as numbers, 'Logged At' as a serial number."""
    return book.worksheet(SELLOG_TAB).get_all_values(
        value_render_option="UNFORMATTED_VALUE",
        date_time_render_option="SERIAL_NUMBER")


def _read_shadow_live(book) -> List[List[Any]]:
    """v1.2.0: _Shadow_Rank_Log, same raw render ('Logged At' and 'Day' as
    serial numbers). READ ONLY."""
    return book.worksheet(SHADOW_TAB).get_all_values(
        value_render_option="UNFORMATTED_VALUE",
        date_time_render_option="SERIAL_NUMBER")


def _sheets_credentials():
    from google.oauth2 import service_account
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if path and os.path.exists(path):
        return service_account.Credentials.from_service_account_file(
            path, scopes=scopes)
    raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS", "").strip()
           or os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64", "").strip())
    if not raw:
        return None
    if not raw.startswith("{"):
        try:
            dec = base64.b64decode(raw).decode("utf-8", errors="replace").strip()
            if dec.startswith("{"):
                raw = dec
        except Exception:                             # noqa: BLE001
            pass
    return service_account.Credentials.from_service_account_info(
        json.loads(raw), scopes=scopes)


def _open_book():
    import gspread
    creds = _sheets_credentials()
    gc = gspread.authorize(creds) if creds else gspread.service_account()
    sid = (os.getenv("DEFAULT_SPREADSHEET_ID", "").strip()
           or os.getenv("SPREADSHEET_ID", "").strip())
    if not sid:
        raise SystemExit("DEFAULT_SPREADSHEET_ID / SPREADSHEET_ID not set")
    return gc.open_by_key(sid)


def write_scorecard_tab(book, rect: List[List[Any]]) -> Tuple[int, int]:
    """Rewrite _Board_Scorecard (created on first use) in ONE update call,
    padded to the previous extent. Returns the written (rows, cols)."""
    width = max(len(r) for r in rect)
    try:
        ws = book.worksheet(SCORECARD_TAB)
        prev = ws.get_all_values()
    except Exception as exc:                          # noqa: BLE001
        if type(exc).__name__ != "WorksheetNotFound":
            raise
        ws = book.add_worksheet(title=SCORECARD_TAB, rows=max(len(rect) + 20, 200),
                                cols=max(width, 26))
        prev = []
    rows = max(len(rect), len(prev))
    cols = max(width, max((len(r) for r in prev), default=0))
    if ws.row_count < rows:
        ws.add_rows(rows - ws.row_count)
    if ws.col_count < cols:
        ws.add_cols(cols - ws.col_count)
    ws.update(values=_pad_rect(rect, rows, cols), range_name="A1",
              value_input_option="RAW")
    return rows, cols


def _append_run_log(book, asof_utc: datetime, status: str, message: str,
                    details: Dict[str, Any]) -> None:
    try:
        book.worksheet("_Run_Log").append_row(
            [asof_utc.strftime("%Y-%m-%d %H:%M:%S"),
             "INFO" if status == "OK" else "WARNING", "score_selection_log",
             SCORECARD_TAB, status, message, "", "", "",
             json.dumps(details, sort_keys=True)[:900]],
            value_input_option="RAW")
    except Exception as exc:                          # noqa: BLE001
        print(f"WARN: _Run_Log append failed (scorecard unaffected): {exc}")


def _write_artifacts(out_dir: str, summary: List[List[Any]],
                     churn: List[List[Any]], detail: List[List[Any]],
                     md: str, ranking: Optional[List[List[Any]]] = None,
                     challenger: Optional[List[List[Any]]] = None) -> List[str]:
    """v1.1.0: with `ranking` rows the detail CSV takes DETAIL_HEADER_RANKING
    and the summary CSV gains a third block (RANKING_HEADER + rows)."""
    os.makedirs(out_dir, exist_ok=True)
    p_det = os.path.join(out_dir, "board_scorecard_detail.csv")
    p_sum = os.path.join(out_dir, "board_scorecard_summary.csv")
    p_md = os.path.join(out_dir, "board_scorecard.md")
    with open(p_det, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(DETAIL_HEADER_RANKING if ranking is not None else DETAIL_HEADER)
        w.writerows(detail)
    with open(p_sum, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(SUMMARY_HEADER)
        w.writerows(summary)
        w.writerow([])
        w.writerow(CHURN_HEADER)
        w.writerows(churn)
        if ranking is not None:
            w.writerow([])
            w.writerow(RANKING_HEADER)
            w.writerows(ranking)
        if challenger is not None:         # v1.2.0 [CHALLENGER]
            w.writerow([])
            w.writerow(CHALLENGER_HEADER)
            w.writerows(challenger)
    with open(p_md, "w", encoding="utf-8") as fh:
        fh.write(md)
    return [p_det, p_sum, p_md]


def run_scorecard(tsv: Optional[str] = None, live: bool = False,
                  prices_csv: Optional[str] = None, fetch_yahoo_: bool = False,
                  fetch_eodhd: bool = False, since: str = DEFAULT_SINCE,
                  out_dir: str = DEFAULT_OUT_DIR,
                  asof: Optional[str] = None,
                  shadow_tsv: Optional[str] = None) -> int:
    tag = f"[BOARD-SCORECARD v{SCRIPT_VERSION}]"
    mode = _scorecard_mode()
    if mode == "off":
        print(f"{tag} mode=off - nothing computed "
              "(TFB_BOARD_SCORECARD=observe|write to run)")
        return 0
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", since or ""):
        raise SystemExit(f"--since must be YYYY-MM-DD, got {since!r}")
    asof_utc = _parse_asof(asof)
    now_riyadh = asof_utc.astimezone(RIYADH_TZ).replace(tzinfo=None)
    book = None
    if live:
        book = _open_book()
        values, source = _read_sellog_live(book), "live"
    elif tsv:
        values, source = _read_sellog_tsv(tsv), "tsv"
    else:
        raise SystemExit("--scorecard needs --live or --tsv")
    events, ev_stats = board_events(values)
    episodes, ep_stats = build_episodes(events, now_riyadh)
    if not episodes:
        print(f"{tag} mode={mode} source={source} episodes=0 - nothing to score")
        return 0
    syms = sorted({e["symbol"] for e in episodes})
    frm = (min(e["seat_at"] for e in episodes).date()
           - timedelta(days=10)).strftime("%Y-%m-%d")
    # v1.2.0 [CHALLENGER]: the logged orders become episodes like seats; their
    # symbols and references join the fetch (board coverage is unchanged).
    chal_on = _challenger_enabled()
    sh_eps: Dict[str, List[Dict[str, Any]]] = {o: [] for o in SHADOW_ORDERS}
    sh_days: Dict[str, int] = {o: 0 for o in SHADOW_ORDERS}
    sh_extra: List[str] = []
    if chal_on:
        sh_values: List[List[Any]] = []
        try:
            if shadow_tsv:
                sh_values = _read_sellog_tsv(shadow_tsv)
            elif book is not None:
                sh_values = _read_shadow_live(book)
        except Exception as exc:                      # noqa: BLE001
            if type(exc).__name__ != "WorksheetNotFound":
                print(f"WARN: {SHADOW_TAB} not read ({exc}) - challenger NO_DATA")
            sh_values = []
        sh_ev, _sh_stats = shadow_events(sh_values)
        for order in SHADOW_ORDERS:
            sh_eps[order], _st = build_episodes(sh_ev[order], now_riyadh)
            sh_days[order] = len({e["run"] for e in sh_ev[order] if _trading_day(e["run"])})
            for ep in sh_eps[order]:
                ep["ref"] = ref_symbol(ep["symbol"], ep["sector"])
                sh_extra += [ep["symbol"]] + ([ep["ref"]] if ep["ref"] else [])
        firsts = [e["seat_at"] for o in SHADOW_ORDERS for e in sh_eps[o]]
        if firsts:
            frm = min(frm, (min(firsts).date() - timedelta(days=10)).strftime("%Y-%m-%d"))
        sh_extra = sorted(set(sh_extra) - set(syms) - {BENCH_SYMBOL})
    # v1.1.0 [RANKING HEALTH]: reference series ride the same price path as
    # SPUS; they never count towards price coverage (that stays per seat).
    ranking_on = _ranking_enabled()
    refs: List[str] = []
    if ranking_on:
        for ep in episodes:
            ep["ref"] = ref_symbol(ep["symbol"], ep["sector"])
        refs = sorted({ep["ref"] for ep in episodes} - {""})
    if prices_csv:
        prices, psrc = load_prices_csv(prices_csv), "csv"
    elif fetch_yahoo_:
        prices, psrc = fetch_yahoo(syms + [BENCH_SYMBOL] + refs + sorted(set(sh_extra) - set(refs)),
                                   frm, asof_utc), "yahoo"
    elif fetch_eodhd:
        tok = os.getenv("TFB_EODHD_API_KEY", "").strip()
        if not tok:
            raise SystemExit("TFB_EODHD_API_KEY missing (fetch mode)")
        prices, psrc = fetch_prices(syms + [BENCH_SYMBOL] + refs + sorted(set(sh_extra) - set(refs)),
                                    frm, tok), "eodhd"
    else:
        raise SystemExit("--scorecard needs --prices-csv, --fetch-yahoo or --fetch")
    prices = {s: _final_bars(s, sorted(v), asof_utc) for s, v in prices.items()}
    bench = prices.get(BENCH_SYMBOL, [])
    scored = [score_episode(ep, prices.get(ep["symbol"], []), bench,
                            ref=prices.get(ep["ref"], []) if ep.get("ref") else None)
              for ep in episodes]
    covered = sum(1 for s in syms if prices.get(s))
    coverage = covered / len(syms)
    summary, churn = summarize(scored, since)
    detail = detail_rows(scored, ranking=ranking_on)
    ranking: Optional[List[List[Any]]] = None
    rk: Dict[str, Any] = {}
    refs_ok = 0
    if ranking_on:
        ranking, rk_stats = ranking_health(scored, since)
        rk = rk_stats.get("Since " + since, {})
        refs_ok = sum(1 for r in refs if prices.get(r))
    challenger: Optional[List[List[Any]]] = None
    ch: Dict[str, Any] = {}
    if chal_on:
        sh_scored = {o: [score_shadow_episode(ep, prices.get(ep["symbol"], []), bench,
                                              ref=prices.get(ep["ref"], []) if ep.get("ref") else None)
                         for ep in sh_eps[o]] for o in SHADOW_ORDERS}
        challenger, ch = challenger_compare(sh_scored, sh_days)
    n_open = sum(1 for e in episodes if e["exit_at"] is None)
    n_since = sum(1 for s in scored if s["seat_date"] >= since)
    n_priced = sum(1 for s in scored if s["status"] in ("OPEN", "CLOSED"))
    meta = (f"{tag} generated {asof_utc:%Y-%m-%d %H:%M}Z | source {source} "
            f"{ev_stats['rows']} rows | episodes {len(episodes)} (open {n_open}, "
            f"since {since}: {n_since}) | priced {n_priced} | prices {psrc} "
            f"{covered}/{len(syms)} symbols | bench SPUS "
            f"{'ok' if bench else 'missing'} | mode {mode}")
    if ranking_on:
        meta += (f" | refs {refs_ok}/{len(refs)} | ranking "
                 f"{rk.get('verdict', 'n/a')}")
    if chal_on:
        meta += f" | challenger {ch.get('verdict', 'n/a')}"
    rect = scorecard_rect(meta, summary, churn, detail, ranking=ranking,
                          challenger=challenger)
    md = scorecard_markdown(meta, summary, churn, since, ranking=ranking,
                            challenger=challenger)
    print(md)
    paths = _write_artifacts(out_dir, summary, churn, detail, md, ranking=ranking,
                             challenger=challenger)
    step = os.getenv("GITHUB_STEP_SUMMARY", "").strip()
    if step:
        try:
            with open(step, "a", encoding="utf-8") as fh:
                fh.write(md)
        except OSError as exc:
            print(f"WARN: step summary not written: {exc}")
    degraded = coverage < _min_coverage()
    details = {"version": SCRIPT_VERSION, "mode": mode, "source": source,
               "rows": ev_stats["rows"], "episodes": len(episodes),
               "open": n_open, "since": since, "since_episodes": n_since,
               "priced": n_priced, "prices": psrc, "covered": covered,
               "symbols": len(syms), "bench": bool(bench),
               "orphan_exits": ep_stats["orphan_exits"]}
    if ranking_on:
        rho = rk.get("spearman_roi")
        details["ranking"] = {
            "verdict": rk.get("verdict"), "n": rk.get("n_roi"),
            "spearman_roi": None if rho is None else round(rho, 4),
            "hit_1_3": rk.get("hit_1_3"), "hit_7_10": rk.get("hit_7_10"),
            "refs": refs_ok, "refs_total": len(refs)}
    if chal_on:
        def _rd(v):
            return None if v is None else round(v, 2)
        details["challenger"] = {
            "verdict": ch.get("verdict"), "days": ch.get("days"),
            "champion": {k: _rd(v) if isinstance(v, float) else v
                         for k, v in (ch.get("champion") or {}).items()},
            "challenger": {k: _rd(v) if isinstance(v, float) else v
                           for k, v in (ch.get("challenger") or {}).items()}}
    wrote = "0"
    rc = 0
    if mode == "write":
        if book is None:
            print("NOTE: write mode needs --live - artifacts only, no sheet write")
        elif degraded:
            msg = (f"{tag} DEGRADED price coverage {covered}/{len(syms)} < "
                   f"{_min_coverage():.0%} - {SCORECARD_TAB} NOT overwritten")
            print("::warning::" + msg)
            _append_run_log(book, asof_utc, "DEGRADED", msg, details)
            rc = 1
        else:
            r_, c_ = write_scorecard_tab(book, rect)
            wrote = f"{r_}x{c_}"
            _append_run_log(book, asof_utc, "OK",
                            f"{tag} episodes={len(episodes)} open={n_open} "
                            f"since={since}({n_since}) priced={n_priced} "
                            f"prices={psrc} {covered}/{len(syms)} wrote={wrote}"
                            + (f" | ranking: {rk.get('text', 'n/a')}"
                               if ranking_on else "")
                            + (" | challenger: %s (days %s, scored %s/%s)"
                               % (ch.get("verdict", "n/a"), ch.get("days", 0),
                                  (ch.get("champion") or {}).get("scored", 0),
                                  (ch.get("challenger") or {}).get("scored", 0))
                               if chal_on else ""),
                            details)
    elif degraded:
        print(f"::warning::{tag} price coverage {covered}/{len(syms)} below "
              f"{_min_coverage():.0%} (observe - nothing written)")
    print(f"{tag} mode={mode} source={source} episodes={len(episodes)} "
          f"open={n_open} since={since}({n_since}) priced={n_priced} "
          f"prices={psrc} {covered}/{len(syms)} bench={'ok' if bench else 'missing'} "
          f"orphan_exits={ep_stats['orphan_exits']} wrote={wrote} "
          f"artifacts={len(paths)}"
          + (f" ranking={rk.get('verdict', 'n/a')}" if ranking_on else "")
          + (f" challenger={ch.get('verdict', 'n/a')}" if chal_on else ""))
    return rc


def selftest() -> int:
    u = {"entry_date": "2026-08-01", "symbol": "T.US", "entry_px": 100.0,
         "fx": 1.0, "stop_l": 90.0, "tp1_l": 110.0, "tp2_l": 120.0}
    ok = 0
    # 1: TP1 then TP2
    r = score_unit(u, [("2026-08-01", 100), ("2026-08-02", 105),
                       ("2026-08-03", 111), ("2026-08-04", 121)])
    assert (r["outcome"], r["days"], r["tp1"]) == ("TP1_HIT", 2, "day2"), r
    ok += 1
    # 2: stop first
    r = score_unit(u, [("2026-08-02", 95), ("2026-08-03", 89),
                       ("2026-08-04", 130)])
    assert (r["outcome"], r["days"]) == ("STOP_HIT", 2), r
    ok += 1
    # 3: same-day both -> conservative STOP
    r = score_unit(u, [("2026-08-02", 89.0)])
    assert r["outcome"] == "STOP_HIT", r
    ok += 1
    # 4: open with return
    r = score_unit(u, [("2026-08-02", 104.0)])
    assert r["outcome"] == "OPEN" and r["ret_pct"] == 4.0, r
    ok += 1
    # 5: entry-day close ignored (strictly after)
    r = score_unit(u, [("2026-08-01", 80.0)])
    assert r["outcome"] == "NO_DATA", r
    ok += 1
    # 6: parser handles parens/dash/comma
    assert _num("(82)") == -82.0 and _num("1,234.5") == 1234.5
    assert _num("\u2014") is None
    ok += 1
    # 7: sized_units filters, dedups, converts SAR->local
    log = [
        {"Logged At": "2026-08-21 16:35", "Symbol": "1050.SR", "Price": "20.8",
         "FX\u2192SAR": "1", "Ticket SAR": "\u2014", "Stop SAR": "\u2014",
         "TP1 SAR": "\u2014", "TP2 SAR": "\u2014"},
        {"Logged At": "2026-08-21 23:10", "Symbol": "1050.SR", "Price": "20.79",
         "FX\u2192SAR": "1", "Ticket SAR": "9252", "Stop SAR": "18.69",
         "TP1 SAR": "23.29", "TP2 SAR": "25.80"},
        {"Logged At": "2026-08-21 23:10", "Symbol": "6804.T", "Price": "1500",
         "FX\u2192SAR": "0.0261", "Ticket SAR": "3900", "Stop SAR": "36.54",
         "TP1 SAR": "43.07", "TP2 SAR": ""},
        {"Logged At": "2026-08-21 23:10", "Symbol": "KE=F", "Price": "5",
         "FX\u2192SAR": "3.75", "Ticket SAR": "1000", "Stop SAR": "17",
         "TP1 SAR": "20", "TP2 SAR": ""},
    ]
    un = sized_units(log)
    assert len(un) == 2, un                       # grace + futures dropped
    key = ("2026-08-21", "6804.T")
    assert abs(un[key]["stop_l"] - 36.54 / 0.0261) < 1e-6
    ok += 1
    # ---- v1.0.0 [P-182] board scorecard cases (8-16) ----
    # 8: 'Logged At' parser - ISO 'T'/space, serial number, offset, garbage
    ref = datetime(2026, 9, 30, 8, 55, 21)
    ser = (ref - datetime(1899, 12, 30)).total_seconds() / 86400.0
    assert _parse_logged_at("2026-09-30T08:55:21") == ref
    assert _parse_logged_at("2026-09-30 08:55:21") == ref
    assert _parse_logged_at(ser) == ref and _parse_logged_at(str(ser)) == ref
    assert _parse_logged_at("2026-09-30T05:55:21+00:00") == ref
    assert _parse_logged_at("n/a") is None and _parse_logged_at("") is None
    ok += 1
    # 9: episodes - snapshot drop + same-run exit reason, empty board,
    #    re-seat, orphan exit, sentinel never an episode
    hdr = ["Logged At", "Run Info", "Symbol", "Name", "Sector", "Ccy",
           "Price", "Outcome", "Stability"]

    def _row(at, run, sym, px="", out="", ex=False, outp="HELD"):
        info = (f"Last run {run} | status: ok | output: {outp}"
                + (" [membership exit]" if ex else ""))
        return [at, info, sym, sym + " Inc", "Tech", "USD", px, out,
                "" if ex else "FAST-TRACK (day 1)"]
    grid = [hdr,
            _row("2026-09-01T10:00:00", "2026-09-01 10:00:00", "AAA.US", "10"),
            _row("2026-09-01T10:00:00", "2026-09-01 10:00:00", "BBB.US", "20"),
            _row("2026-09-01T14:00:00", "2026-09-01 14:00:00", "AAA.US", "10.5"),
            _row("2026-09-01T14:00:03", "2026-09-01 14:00:00", "BBB.US",
                 out="EXIT: hard", ex=True),
            _row("2026-09-02T09:00:00", "2026-09-02 09:00:00", "EMPTY_BOARD",
                 outp="EMPTY"),
            _row("2026-09-02T12:00:00", "2026-09-02 12:00:00", "AAA.US", "11",
                 outp="EXECUTABLE"),
            _row("2026-09-02T12:00:00", "2026-09-02 12:00:00", "CCC.US", ""),
            _row("2026-09-02T13:00:00", "2026-09-02 13:00:00", "ZZZ.US",
                 out="EXIT: soft", ex=True)]
    evs, _est = board_events(grid)
    eps, pst = build_episodes(evs, datetime(2026, 9, 3, 12, 0, 0))
    got = [(e["symbol"], e["seat_at"].strftime("%m-%d %H"), e["exit_kind"],
            len(e["days"])) for e in eps]
    assert got == [("AAA.US", "09-01 10", "BOARD EMPTY", 1),
                   ("BBB.US", "09-01 10", "EXIT: hard", 1),
                   ("AAA.US", "09-02 12", "", 1),
                   ("CCC.US", "09-02 12", "", 1)], got
    assert (pst["orphan_exits"], pst["empty_snapshots"], pst["snapshots"]) == (1, 1, 4), pst
    assert eps[2]["output"] == "EXECUTABLE" and eps[3]["seat_px"] is None
    assert abs(eps[0]["hours"] - 23.0) < 1e-9 and abs(eps[3]["hours"] - 24.0) < 1e-9
    ok += 1
    # 10: venue bar rule on the venue's own clock (DST-aware)
    b2 = [("2026-09-29", 1.0), ("2026-09-30", 2.0), ("2026-10-01", 3.0)]
    assert _first_bar_index("BHF.US", datetime(2026, 9, 30, 16, 2, 4), b2) == 1
    assert _first_bar_index("PINE.US", datetime(2026, 9, 30, 3, 56, 4), b2) == 1
    assert _first_bar_index("X.US", datetime(2026, 9, 30, 23, 30), b2) == 2
    bn = [("2026-11-02", 1.0), ("2026-11-03", 2.0)]
    assert _first_bar_index("X.US", datetime(2026, 11, 2, 23, 30), bn) == 0
    assert _first_bar_index("7203.T", datetime(2026, 9, 30, 23, 30), b2) == 2
    assert _first_bar_index("2222.SR", datetime(2026, 9, 30, 16, 0), b2) == 2
    assert _first_bar_index("2222.SR", datetime(2026, 9, 30, 10, 0), b2) == 1
    assert _exit_bar_index("BHF.US", datetime(2026, 9, 30, 20, 11, 23), b2) == 0
    assert _exit_bar_index("NVDA.US", datetime(2026, 10, 1, 0, 16, 15), b2) == 1
    ok += 1
    # 11: returns / exit / excess vs SPUS + every guard
    sess, d = [], date(2026, 9, 1)
    while len(sess) < 22:                  # NYSE sessions; 09-07 Labor Day
        if d.weekday() < 5 and d != date(2026, 9, 7):
            sess.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    closes = [(dd, 100.0 + i) for i, dd in enumerate(sess, 1)]
    bench = [("2026-08-31", 50.0)] + [(dd, 50.0 + 0.1 * i)
                                      for i, dd in enumerate(sess, 1)]
    ep = {"symbol": "AAA.US", "seat_at": datetime(2026, 9, 1, 10, 0),
          "seat_px": 100.0, "exit_at": datetime(2026, 9, 8, 23, 30)}
    s = score_episode(ep, closes, bench)
    assert (s["status"], _r2(s["r"][1]), _r2(s["r"][5]), _r2(s["r"][10]),
            _r2(s["r"][20])) == ("CLOSED", 1.0, 5.0, 10.0, 20.0), s
    assert (_r2(s["r_exit"]), _r2(s["r_now"]), _r2(s["x"][5]),
            _r2(s["x_now"])) == (5.0, 22.0, 4.0, 17.6), s
    ep2 = dict(ep, symbol="BHF.US", seat_at=datetime(2026, 9, 30, 16, 2, 4),
               seat_px=52.0, exit_at=datetime(2026, 9, 30, 20, 11, 23))
    s2 = score_episode(ep2, [("2026-09-29", 52.0), ("2026-09-30", 49.9)], [])
    assert (s2["status"], _r2(s2["r"][1]), s2["pre_close_exit"], s2["r_exit"],
            s2["x"][1]) == ("CLOSED", -4.04, True, None, None), s2
    assert score_episode(dict(ep, seat_px=1.0), closes, bench)["status"] == "UNIT?"
    assert score_episode(dict(ep, seat_at=datetime(2026, 8, 1, 10, 0)),
                         closes, bench)["status"] == "GAP"
    s3 = score_episode(dict(ep, seat_at=datetime(2026, 10, 1, 8, 27),
                            exit_at=None), closes[:21], bench)
    assert s3["status"] == "PENDING" and not s3["pre_close_exit"], s3
    assert score_episode(dict(ep, seat_px=None), closes, bench)["status"] == "NO_PRICE"
    assert score_episode(ep, [], bench)["status"] == "NO_DATA"
    ok += 1
    # 12: summary windows x horizons + churn
    nil = {k: None for k in HORIZONS}
    sc = [{"ep": {"exit_kind": "EXIT: hard", "exit_at": datetime(2026, 9, 2),
                  "hours": 10.0}, "seat_date": "2026-09-01",
           "r": {**nil, 1: 2.0}, "x": {**nil, 1: 1.0},
           "r_exit": 2.0, "r_now": 3.0, "x_now": 2.5, "pre_close_exit": False},
          {"ep": {"exit_kind": "DROPPED", "exit_at": datetime(2026, 9, 3),
                  "hours": 20.0}, "seat_date": "2026-09-02",
           "r": {**nil, 1: -1.0}, "x": {**nil, 1: -2.0},
           "r_exit": None, "r_now": -3.0, "x_now": None, "pre_close_exit": True},
          {"ep": {"exit_kind": "", "exit_at": None, "hours": 30.0},
           "seat_date": "2026-08-30", "r": dict(nil), "x": dict(nil),
           "r_exit": None, "r_now": None, "x_now": None, "pre_close_exit": False}]
    summ, ch = summarize(sc, "2026-09-01")
    row = [r for r in summ if r[0] == "All" and r[1] == "+1"][0]
    assert row == ["All", "+1", 3, 2, 50.0, 0.5, 0.5, -0.5, 2.0, -1.0], row
    row = [r for r in summ if r[0] == "Since 2026-09-01" and r[1] == "To Now"][0]
    assert row == ["Since 2026-09-01", "To Now", 2, 2, 50.0, 0.0, 0.0, 2.5,
                   3.0, -3.0], row
    assert [r[0] for r in summ[::6]] == ["All", "Since 2026-09-01", "2026-09",
                                         "2026-08"], summ
    assert ch[0] == ["All", 3, 2, 1, 1, 0, 1, 0, 1, 20.0], ch
    ok += 1
    # 13: Yahoo chart JSON - gmtoffset dates, None skipped, live bar last wins
    t0 = int(datetime(2026, 9, 29, 13, 30, tzinfo=timezone.utc).timestamp())
    pay = {"chart": {"result": [{
        "meta": {"gmtoffset": -14400},
        "timestamp": [t0, t0 + 86400, t0 + 2 * 86400, t0 + 2 * 86400 + 3600],
        "indicators": {"quote": [{"close": [32.75, 30.61, None, 30.9]}]}}],
        "error": None}}
    assert parse_yahoo_chart(pay) == [("2026-09-29", 32.75),
                                      ("2026-09-30", 30.61),
                                      ("2026-10-01", 30.9)]
    assert parse_yahoo_chart({"chart": {"result": None,
                                        "error": {"code": "Not Found"}}}) == []
    assert parse_yahoo_chart({}) == []
    assert (_yahoo_symbol("BRK-B.US"), _yahoo_symbol("2222.SR")) == ("BRK-B", "2222.SR")
    ok += 1
    # 14: the tab is ONE uniform rectangle, padded to the previous extent
    rect = scorecard_rect("[BOARD-SCORECARD v1.0.0] t",
                          [["All", "+1"] + [""] * 8], [],
                          [["2026-09-01"] + [""] * 21])
    assert len({len(r) for r in rect}) == 1 and len(rect[0]) == len(DETAIL_HEADER)
    assert rect[0][0].startswith("[BOARD-SCORECARD v1.0.0]") and rect[-1][0] == "2026-09-01"
    big = _pad_rect(rect, len(rect) + 5, len(DETAIL_HEADER) + 2)
    assert len(big) == len(rect) + 5 and {len(r) for r in big} == {len(DETAIL_HEADER) + 2}
    assert big[-1] == [""] * (len(DETAIL_HEADER) + 2)
    ok += 1
    # 15: gate - unset/unknown -> observe, off, write
    saved = os.environ.get("TFB_BOARD_SCORECARD")
    try:
        seen = []
        for v in (None, "off", "WRITE", "bogus", "observe"):
            if v is None:
                os.environ.pop("TFB_BOARD_SCORECARD", None)
            else:
                os.environ["TFB_BOARD_SCORECARD"] = v
            seen.append(_scorecard_mode())
        assert seen == ["observe", "off", "write", "observe", "observe"], seen
    finally:
        if saved is None:
            os.environ.pop("TFB_BOARD_SCORECARD", None)
        else:
            os.environ["TFB_BOARD_SCORECARD"] = saved
    ok += 1
    # 16: a live/partial bar is dropped until the venue session has closed
    us = [("2026-09-29", 1.0), ("2026-09-30", 2.0)]
    assert _final_bars("RDN.US", us, datetime(2026, 9, 30, 15, 0, tzinfo=timezone.utc)) == us[:1]
    assert _final_bars("RDN.US", us, datetime(2026, 9, 30, 21, 0, tzinfo=timezone.utc)) == us
    assert _final_bars("2222.SR", us, datetime(2026, 9, 30, 11, 0, tzinfo=timezone.utc)) == us[:1]
    assert _final_bars("2222.SR", us, datetime(2026, 9, 30, 12, 30, tzinfo=timezone.utc)) == us
    ok += 1
    # ---- v1.1.0 [RANKING HEALTH] cases (17-20) ----
    # 17: Spearman in plain Python. Hand check (6 points, one tie):
    #     x = [1,2,2,4,5,6] -> ranks [1,2.5,2.5,4,5,6]; y = [2,1,3,4,6,5]
    #     -> ranks [2,1,3,4,6,5]; both means 3.5; sum dx*dy = 14.5,
    #     sum dx^2 = 17.0, sum dy^2 = 17.5 -> rho = 14.5/sqrt(297.5) = 0.84067.
    #     Textbook 10-point case (no ties): rho = -29/165 = -0.17576.
    assert _avg_ranks([1, 2, 2, 4, 5, 6]) == [1.0, 2.5, 2.5, 4.0, 5.0, 6.0]
    assert abs(spearman([1, 2, 2, 4, 5, 6], [2, 1, 3, 4, 6, 5])
               - 14.5 / math.sqrt(17.0 * 17.5)) < 1e-12
    assert abs(spearman([106, 86, 100, 101, 99, 103, 97, 113, 112, 110],
                        [7, 0, 27, 50, 28, 29, 20, 12, 6, 17]) + 29.0 / 165.0) < 1e-12
    assert spearman([1, 2, 3, 4, 5, 6], [6, 5, 4, 3, 2, 1]) == -1.0
    assert spearman([1, 2, 3], [1, 1, 1]) is None and spearman([1], [2]) is None
    assert spearman([1, 2], [1, 2, 3]) is None
    assert (_rho_text(-0.38123), _rho_text(None), _rho_text(-0.001),
            _rho_text(0.8406680017, 3)) == ("-0.38", "n/a", "0.00", "0.841")
    ok += 1
    # 18: reference series - US sector words (GICS + Yahoo spellings), venue
    #     index for other suffixes, unmatched -> ''
    for sector, etf in (("Financials", "XLF.US"), ("Financial Services", "XLF.US"),
                        ("Real Estate", "VNQ.US"), ("Energy", "XLE.US"),
                        ("Information Technology", "XLK.US"), ("Technology", "XLK.US"),
                        ("Health Care", "XLV.US"), ("Healthcare", "XLV.US"),
                        ("Industrials", "XLI.US"),
                        ("Consumer Discretionary", "XLY.US"), ("Consumer Cyclical", "XLY.US"),
                        ("Consumer Staples", "XLP.US"), ("Consumer Defensive", "XLP.US"),
                        ("Utilities", "XLU.US"), ("Materials", "XLB.US"),
                        ("Basic Materials", "XLB.US"), ("Communication Services", "XLC.US"),
                        ("  communication   services ", "XLC.US"),
                        ("Consumer", ""), ("Banks", ""), ("", "")):
        assert ref_symbol("AAA.US", sector) == etf, (sector, ref_symbol("AAA.US", sector))
    assert ref_symbol("BRK-B", "Financials") == "XLF.US"          # bare ticker = US
    for sym, idx in (("PHP.L", "^FTSE"), ("WDP.BR", "^STOXX50E"), ("URW.PA", "^STOXX50E"),
                     ("SAN.MC", "^STOXX50E"), ("EDP.LS", "^STOXX50E"), ("ASML.AS", "^STOXX50E"),
                     ("SAP.DE", "^STOXX50E"), ("ENI.MI", "^STOXX50E"), ("1972.HK", "^HSI"),
                     ("RY.TO", "^GSPTSE"), ("BHP.AX", "^AXJO"), ("AMXL.MX", "^MXX"),
                     ("2222.SR", "^TASI.SR"), ("D05.SI", "^STI"), ("7203.T", "^N225"),
                     ("005930.KS", ""), ("AEFES.IS", "")):
        assert ref_symbol(sym, "Financials") == idx, (sym, ref_symbol(sym, "Financials"))
    ok += 1
    # 19: ranking health rows + the four verdicts on synthetic scored sets
    def _sc(i, rank, roi, xr, conf="High", sector="Financials", x_now=None):
        return {"ep": {"rank_at_seat": rank, "eng_roi_at_seat": roi, "conf_at_seat": conf,
                       "sector": sector, "exit_kind": "", "exit_at": None, "hours": 1.0,
                       "seq": i, "seat_at": datetime(2026, 10, 1, 10, 0)},
                "seat_date": "2026-10-01", "xr_now": xr,
                "x_now": xr if x_now is None else x_now}
    # INVERTED by rho: ROI 10+i; ranks 1-3 (i%10<3) lose -(i+1), the rest
    #     gain 40-i (falling with ROI) -> rho -0.461, n 32; 1-3 hit 0 %,
    #     7-10 hit 100 % (ranks cycle 1..10: 1-3 = 11 seats, 7-10 = 12)
    inv = [_sc(i, 1 + i % 10, 10.0 + i, -float(i + 1) if i % 10 < 3 else 40.0 - i)
           for i in range(32)]
    rows, st = ranking_health(inv, "2026-09-01")
    w = "Since 2026-09-01"
    assert [r[:3] for r in rows[:3]] == [[w, "Rank", "1-3"], [w, "Rank", "4-6"],
                                         [w, "Rank", "7-10"]], rows[:3]
    r13 = [r for r in rows if r[:3] == [w, "Rank", "1-3"]][0]
    r710 = [r for r in rows if r[:3] == [w, "Rank", "7-10"]][0]
    assert r13[3:6] == [11, 11, 0.0] and r710[3:6] == [12, 12, 100.0], (r13, r710)
    assert st[w]["verdict"] == "RANKING_INVERTED" and st[w]["n_roi"] == 32
    assert abs(st[w]["spearman_roi"] + 0.4611436950) < 1e-9, st[w]
    assert st[w]["text"] == "RANKING_INVERTED (spearman -0.46, n=32)", st[w]
    assert [r[1] for r in rows if r[0] == w].count("Spearman") == 2
    assert rows[10][2] == "Engine ROI at seat vs excess: rho -0.461 (n=32)", rows[10]
    assert rows[11][2].startswith("Rank at seat vs excess: rho 0.585 (n=32)"), rows[11]
    assert [r for r in rows if r[0] == w][-1][1:3] == ["Verdict", st[w]["text"]]
    assert [r[0] for r in rows] == [w] * 13 + ["2026-10"] * 13, [r[0] for r in rows]
    cap = [r for r in rows if r[:2] == [w, "Engine ROI"]]
    assert [c[2] for c in cap] == [">= 34", "< 34"] and cap[0][3] + cap[1][3] == 32
    assert cap[0][3] == 8 and cap[1][3] == 24, cap     # ROI 10..41: 8 at/above 34
    sec = [r for r in rows if r[:2] == [w, "Sector"]]
    assert [c[2] for c in sec] == ["Financials", "Other"] and sec[1][3] == 0, sec
    cf = [r for r in rows if r[:2] == [w, "Conf"]]
    assert [(c[2], c[3]) for c in cf] == [("High", 32), ("Medium", 0), ("Other", 0)], cf
    # OK: excess rises with ROI (rho 1.0); every rank bucket hits -> 1-3 >= 7-10
    okset = [_sc(i, 1 + i % 10, 10.0 + i, float(i + 1)) for i in range(32)]
    _rows, st2 = ranking_health(okset, "2026-09-01")
    assert st2[w]["verdict"] == "RANKING_OK" and st2[w]["spearman_roi"] == 1.0, st2[w]
    # INSUFFICIENT: n < 30 even with an inverted sample
    _rows, st3 = ranking_health(inv[:12], "2026-09-01")
    assert st3[w]["verdict"] == "INSUFFICIENT_DATA" and st3[w]["n_roi"] == 12, st3[w]
    assert st3[w]["text"] == "INSUFFICIENT_DATA (spearman -0.27, n=12)", st3[w]
    # INVERTED by the hit-rate rule alone: no ROI at all, 9 + 9 scored seats
    hits = [_sc(i, 1 + i % 10, None, -1.0 if i % 10 < 3 else 1.0)
            for i in range(27)]
    _rows, st4 = ranking_health(hits, "2026-09-01")
    assert (st4[w]["n_1_3"], st4[w]["n_7_10"], st4[w]["verdict"]) == (9, 9, "RANKING_INVERTED"), st4[w]
    assert st4[w]["text"] == "RANKING_INVERTED (spearman n/a, n=0)", st4[w]
    # NEUTRAL: rho -0.054 (ROI 1..32 vs alternating excess), no rank data
    neu = [_sc(i, None, 1.0 + i, 1.0 if i % 2 == 0 else -1.0) for i in range(32)]
    _rows, st5 = ranking_health(neu, "2026-09-01")
    assert st5[w]["verdict"] == "RANKING_NEUTRAL" and abs(st5[w]["spearman_roi"]
                                                           + 0.0541530361) < 1e-9, st5[w]
    # fallback: no reference excess -> vs SPUS feeds the hit rate
    fb = [_sc(i, None, 1.0 + i, None, x_now=2.0) for i in range(3)]
    _rows, st6 = ranking_health(fb, "2026-09-01")
    assert st6[w]["n_roi"] == 3 and _excess_vs_ref(fb[0]) == 2.0
    assert ranking_verdict(None, 0, None, 0, None, 0) == "INSUFFICIENT_DATA"
    assert ranking_verdict(0.5, 30, 50.0, 8, 60.0, 8) == "RANKING_NEUTRAL"
    assert ranking_verdict(0.5, 30, 60.0, 8, 60.0, 8) == "RANKING_OK"
    assert ranking_verdict(-0.2, 30, 60.0, 8, 60.0, 8) == "RANKING_INVERTED"
    assert ranking_verdict(0.0, 30, 40.0, 8, 60.0, 8) == "RANKING_INVERTED"
    assert ranking_verdict(0.0, 30, 40.0, 7, 60.0, 8) == "RANKING_NEUTRAL"
    ok += 1
    # 20: gate parsing; board_events captures Rank / Engine ROI % / Rel /
    #     Conf / Market (and tolerates their absence); build_episodes keeps
    #     the FIRST-seat values; detail / rectangle / markdown wiring
    saved = os.environ.get(RANKING_ENV)
    try:
        seen = []
        for v in (None, "off", "on", "ON", "1", "true", "yes", "bogus", ""):
            if v is None:
                os.environ.pop(RANKING_ENV, None)
            else:
                os.environ[RANKING_ENV] = v
            seen.append(_ranking_enabled())
        assert seen == [False, False, True, True, True, True, True, False, False], seen
    finally:
        if saved is None:
            os.environ.pop(RANKING_ENV, None)
        else:
            os.environ[RANKING_ENV] = saved
    hdr2 = hdr + ["Rank", "Engine ROI %", "Rel", "Conf", "Market"]
    g2 = [hdr2,
          _row("2026-09-01T10:00:00", "2026-09-01 10:00:00", "AAA.US", "10") + [3.0, "36.5%", 74.2, "Medium", "US"],
          _row("2026-09-01T14:00:00", "2026-09-01 14:00:00", "AAA.US", "10.5") + ["1", 20.0, 80.0, "High", "US"],
          _row("2026-09-01T14:00:00", "2026-09-01 14:00:00", "BBB.US", "20") + ["\u2014", "", "", "", ""]]
    ev2, _ = board_events(g2)
    assert (ev2[0]["rank"], ev2[0]["eng_roi"], ev2[0]["rel"], ev2[0]["conf"],
            ev2[0]["market"]) == (3, 36.5, 74.2, "Medium", "US"), ev2[0]
    assert (ev2[2]["rank"], ev2[2]["eng_roi"], ev2[2]["conf"]) == (None, None, ""), ev2[2]
    eps2, _ = build_episodes(ev2, datetime(2026, 9, 2, 12, 0))
    assert (eps2[0]["rank_at_seat"], eps2[0]["eng_roi_at_seat"], eps2[0]["rel_at_seat"],
            eps2[0]["conf_at_seat"], eps2[0]["market_at_seat"]) == (3, 36.5, 74.2, "Medium", "US")
    assert eps2[1]["rank_at_seat"] is None and eps2[1]["conf_at_seat"] == ""
    ev3, _ = board_events(grid)                       # the v1.0.0 grid: no such columns
    assert all(e["rank"] is None and e["eng_roi"] is None and e["conf"] == "" for e in ev3)
    sc1 = _sc(0, 2, 36.5, 1.25)
    sc1["ep"].update(symbol="AAA.US", name="AAA", ccy="USD", output="HELD",
                     stability="", days={date(2026, 10, 1)}, rel_at_seat=74.2,
                     seat_px=10.0, ref="XLF.US")
    sc1.update(r={k: None for k in HORIZONS}, x={k: None for k in HORIZONS},
               xr={**{k: None for k in HORIZONS}, 5: 0.5}, r_exit=None, r_now=3.0,
               status="OPEN")
    d0, d1_ = detail_rows([sc1])[0], detail_rows([sc1], ranking=True)[0]
    assert len(d0) == len(DETAIL_HEADER) and d1_[:len(d0)] == d0
    assert d1_[len(d0):] == [2, 36.5, 74.2, "High", "XLF.US", 0.5, 1.25], d1_[len(d0):]
    assert len(DETAIL_HEADER_RANKING) == len(DETAIL_HEADER) + 7 == len(d1_)
    rk_rows = [["Since 2026-09-01", "Rank", "1-3", 1, 1, 100.0, 1.25, 1.25],
               ["2026-10", "Verdict", "INSUFFICIENT_DATA (spearman n/a, n=1)", 1, "", "", "", ""]]
    rect0 = scorecard_rect("m", [], [], [d0])
    rect1 = scorecard_rect("m", [], [], [d1_], ranking=rk_rows)
    assert len(rect0[0]) == len(DETAIL_HEADER) and RANKING_HEADER not in rect0
    assert len(rect1[0]) == len(DETAIL_HEADER_RANKING)
    i_ch, i_rk, i_dt = (rect1.index([c] + [""] * (len(rect1[0]) - 1)) for c in
                        ("CHURN - how long seats lasted and how they ended", RANKING_TITLE,
                         "DETAIL - one row per board seat episode, newest first"))
    assert i_ch < i_rk < i_dt and rect1[i_rk + 1][:8] == RANKING_HEADER
    assert rect1[i_rk + 2][:8] == rk_rows[0] and rect1[i_dt + 1][:len(DETAIL_HEADER_RANKING)] == DETAIL_HEADER_RANKING
    md0 = scorecard_markdown("m", [], [], "2026-09-01")
    md1 = scorecard_markdown("m", [], [], "2026-09-01", ranking=rk_rows)
    assert "RANKING HEALTH" not in md0 and md1.startswith(md0)
    assert "| Since 2026-09-01 | Rank | 1-3 | 1 | 1 | 100.0 | 1.25 | 1.25 |" in md1
    assert "| 2026-10 | Verdict | INSUFFICIENT_DATA (spearman n/a, n=1) |" in md1
    ok += 1
    # 21: reference indexes keep their venue's clock in _final_bars (Codex P2
    #     on PR #847): at 10:00Z on 26 Oct the HK, Tokyo and Saudi(after 12Z)
    #     sessions are closed, London / Toronto / New York are not
    ref_bars = [("2026-10-23", 1.0), ("2026-10-26", 2.0)]
    t10 = datetime(2026, 10, 26, 10, 0, tzinfo=timezone.utc)
    assert _final_bars("^HSI", ref_bars, t10) == ref_bars
    assert _final_bars("^N225", ref_bars, t10) == ref_bars
    assert _final_bars("^FTSE", ref_bars, t10) == ref_bars[:1]
    assert _final_bars("^GSPTSE", ref_bars, t10) == ref_bars[:1]
    assert _final_bars("^TASI.SR", ref_bars, t10) == ref_bars[:1]
    assert _final_bars("^TASI.SR", ref_bars,
                       datetime(2026, 10, 26, 12, 30, tzinfo=timezone.utc)) == ref_bars
    assert _final_bars("SPUS.US", ref_bars, t10) == ref_bars[:1]
    assert _venue("^HSI") == VENUE_CLOSE["HK"] and _venue("^STOXX50E") == VENUE_CLOSE["DE"]
    assert _venue("1972.HK") == VENUE_CLOSE["HK"] and _venue("RDN.US") == VENUE_CLOSE["US"]
    assert all(v in VENUE_CLOSE for v in REF_VENUE_SUFFIX.values())
    assert set(REF_VENUE_SUFFIX) == set(VENUE_REF.values())
    ok += 1
    # 22: shadow log parsing (v1.2.0): first event of each UTC Day only,
    #     CHAMPION / CHALLENGER rows ranked 1..5, BOARD and rank 6+ ignored,
    #     a missing column or an empty grid -> no events; live-render serials,
    #     Day fallback and the Mon-Fri trading-day count
    sh_hdr = ["Logged At", "Day", "Cohort", "Order", "Rank", "Symbol", "Market",
              "Sector", "Seat", "Live Rank", "Shadow Rank", "Engine ROI %", "Rel",
              "Conf"]

    def _shrow(at, day, order, rank, sym, sector="Financials", roi=20.0):
        return [at, day, "run", order, rank, sym, "NYSE", sector, "", "", "",
                roi, 80.0, "High"]
    sh_vals = [sh_hdr,
               _shrow("2026-10-27 08:47:00", "2026-10-27", "CHAMPION", 1, "A.US", roi=35.0),
               _shrow("2026-10-27 08:47:00", "2026-10-27", "CHAMPION", 6, "Z.US"),
               _shrow("2026-10-27 08:47:00", "2026-10-27", "CHALLENGER", 1, "B.US"),
               _shrow("2026-10-27 08:47:00", "2026-10-27", "BOARD", 1, "A.US"),
               _shrow("2026-10-27 15:00:00", "2026-10-27", "CHALLENGER", 1, "C.US"),
               _shrow("2026-10-28 08:47:00", "2026-10-28", "CHALLENGER", 1, "B.US"),
               _shrow("2026-10-28 08:47:00", "2026-10-28", "CHAMPION", 1, "D.US"),
               _shrow("bad", "2026-10-28", "CHAMPION", 2, "E.US")]
    sh_ev, sh_st = shadow_events(sh_vals)
    assert [e["symbol"] for e in sh_ev["CHAMPION"]] == ["A.US", "D.US"]
    assert [e["symbol"] for e in sh_ev["CHALLENGER"]] == ["B.US", "B.US"]
    assert sh_st == {"rows": 8, "days": 2, "events_kept": 4, "bad_time": 1}
    assert sh_ev["CHAMPION"][0]["eng_roi"] == 35.0 and sh_ev["CHAMPION"][0]["run"] == "2026-10-27"
    assert shadow_events([])[0] == {"CHAMPION": [], "CHALLENGER": []}
    assert shadow_events([["Logged At", "Order"]])[0]["CHALLENGER"] == []
    ch_eps, _x = build_episodes(sh_ev["CHALLENGER"], datetime(2026, 10, 29))
    assert len(ch_eps) == 1 and ch_eps[0]["exit_at"] is None, "one open episode (B on both days)"
    cm_eps, _x = build_episodes(sh_ev["CHAMPION"], datetime(2026, 10, 29))
    assert [(e["symbol"], e["exit_kind"]) for e in cm_eps] == [("A.US", "DROPPED"), ("D.US", "")]
    # the live render: 'Logged At' and 'Day' come back as serials; an
    # unreadable Day falls back to the UTC date of Logged At (Riyadh - 3 h),
    # so Sat 02:00 Riyadh belongs to Friday's UTC day (not its first event)

    def _ser(dt):
        return (dt - datetime(1899, 12, 30)).total_seconds() / 86400.0
    sh_live = [sh_hdr,
               [_ser(datetime(2026, 10, 29, 8, 47)), float(int(_ser(datetime(2026, 10, 29)))),
                "run", "CHAMPION", 1.0, "D.US", "NYSE", "Tech", "", "", "", 20.0, 80.0, "High"],
               [_ser(datetime(2026, 10, 30, 8, 47)), "", "run", "CHAMPION", 2.0, "F.US", "NYSE",
                "Tech", "", "", "", 20.0, 80.0, "High"],
               [_ser(datetime(2026, 10, 31, 2, 0)), "", "run", "CHAMPION", 1.0, "G.US", "NYSE",
                "Tech", "", "", "", 20.0, 80.0, "High"],
               _shrow("2026-11-01 09:00:00", "11/01/2026", "CHALLENGER", 1, "H.US")]
    lv_ev, lv_st = shadow_events(sh_live)
    assert [(e["symbol"], e["run"]) for e in lv_ev["CHAMPION"]] == [
        ("D.US", "2026-10-29"), ("F.US", "2026-10-30")], lv_ev["CHAMPION"]
    assert [(e["symbol"], e["run"]) for e in lv_ev["CHALLENGER"]] == [("H.US", "2026-11-01")]
    assert lv_st == {"rows": 4, "days": 3, "events_kept": 3, "bad_time": 0}, lv_st
    assert [_day_key(v) for v in ("2026-10-27", "2026-10-27 00:00:00", "10/27/2026",
                                  _ser(datetime(2026, 10, 27)), "46322", "junk", 5, None)] == [
        "2026-10-27", "2026-10-27", "2026-10-27", "2026-10-27", "2026-10-27", None, None, None]
    assert [_trading_day(d) for d in ("2026-10-30", "2026-10-31", "2026-11-01", "2026-11-02",
                                      "bad")] == [True, False, False, True, False]
    ok += 1
    # 23: shadow episodes take the last close BEFORE the first session after
    #     the logged moment as their base; no prior close -> NO_PRICE
    sh_closes = [("2026-10-26", 10.0), ("2026-10-27", 11.0), ("2026-10-28", 12.1),
                 ("2026-10-29", 12.0), ("2026-10-30", 12.5), ("2026-11-02", 13.0),
                 ("2026-11-03", 13.2)]
    sh_bench = [(d, 100.0) for d, _c in sh_closes]
    ep23 = {"symbol": "B.US", "seat_at": datetime(2026, 10, 27, 8, 47), "seat_px": None,
            "exit_at": None}
    r23 = score_shadow_episode(ep23, sh_closes, sh_bench)
    assert r23["status"] == "OPEN" and abs(r23["r"][1] - 10.0) < 1e-9, r23["r"]
    assert abs(r23["r"][5] - 30.0) < 1e-9 and abs(r23["x"][5] - 30.0) < 1e-9
    assert score_shadow_episode(dict(ep23, seat_at=datetime(2026, 10, 25, 8, 0)),
                                sh_closes, sh_bench)["status"] == "NO_PRICE"
    assert score_shadow_episode(ep23, [], sh_bench)["status"] == "NO_DATA"
    ok += 1
    # 24: the pre-registered verdict: NO_DATA, INSUFFICIENT_DATA, CONTINUE,
    #     PROMOTE_ELIGIBLE, KEEP_CHAMPION; each guardrail can block alone
    cp = {"hit": 50.0, "mean": 0.5, "worst": -8.0, "churn": 1.0}
    good = {"hit": 61.0, "mean": 0.6, "worst": -10.5, "churn": 1.2}
    assert challenger_verdict(0, 0, 0, cp, good)[0] == "NO_DATA"
    assert challenger_verdict(19, 80, 80, cp, good)[0] == "INSUFFICIENT_DATA"
    assert challenger_verdict(25, 39, 80, cp, good)[0] == "INSUFFICIENT_DATA"
    assert challenger_verdict(20, 40, 40, cp, good)[0] == "PROMOTE_ELIGIBLE"
    for k, bad in (("hit", 59.9), ("mean", 0.49), ("worst", -11.1), ("churn", 1.21)):
        v, why = challenger_verdict(25, 50, 50, cp, dict(good, **{k: bad}))
        assert v == "CONTINUE" and why.startswith("not yet:"), (k, v, why)
        assert challenger_verdict(40, 50, 50, cp, dict(good, **{k: bad}))[0] == "KEEP_CHAMPION"
    assert challenger_verdict(40, 10, 10, cp, good)[0] == "KEEP_CHAMPION"
    rows24, st24 = challenger_compare(
        {"CHAMPION": [r23], "CHALLENGER": []}, {"CHAMPION": 1, "CHALLENGER": 0})
    assert [r[0] for r in rows24] == ["CHAMPION", "CHALLENGER", "Verdict"]
    assert rows24[0][1:5] == [1, 1, 1, 100.0] and st24["verdict"] == "NO_DATA"
    assert all(len(r) == len(CHALLENGER_HEADER) for r in rows24)
    ok += 1
    print(f"SELFTEST {ok}/24: ALL GREEN")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tsv")
    ap.add_argument("--out", default="selection_outcomes.csv")
    ap.add_argument("--prices-csv")
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--selftest", action="store_true")
    # v1.0.0 [P-182] board scorecard - opt-in; the flags above are unchanged.
    # v1.1.0: TFB_BOARD_SCORECARD_RANKING=on adds the RANKING HEALTH section.
    ap.add_argument("--scorecard", action="store_true",
                    help="score every board seat episode (TFB_BOARD_SCORECARD; "
                         "TFB_BOARD_SCORECARD_RANKING=on adds RANKING HEALTH)")
    ap.add_argument("--live", action="store_true",
                    help="scorecard: read _Selection_Log from the workbook")
    ap.add_argument("--fetch-yahoo", action="store_true",
                    help="scorecard: daily closes from the Yahoo chart API")
    ap.add_argument("--since", default=DEFAULT_SINCE)
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    ap.add_argument("--asof", help="UTC moment treated as now (replays/tests)")
    # v1.2.0: TFB_BOARD_SCORECARD_CHALLENGER=on adds CHALLENGER vs CHAMPION.
    ap.add_argument("--shadow-tsv",
                    help="scorecard: _Shadow_Rank_Log export (offline; --live "
                         "reads the tab)")
    a = ap.parse_args(argv)
    if a.selftest:
        return selftest()
    if a.scorecard:
        return run_scorecard(tsv=a.tsv, live=a.live, prices_csv=a.prices_csv,
                             fetch_yahoo_=a.fetch_yahoo, fetch_eodhd=a.fetch,
                             since=a.since, out_dir=a.out_dir, asof=a.asof,
                             shadow_tsv=a.shadow_tsv)
    if not a.tsv:
        ap.error("--tsv required (or --selftest)")
    return run(a.tsv, a.out, a.prices_csv, a.fetch)


if __name__ == "__main__":
    sys.exit(main())
