#!/usr/bin/env python3
"""
scripts/score_selection_log.py — Selection_Log ticket-vs-reality scorer
=======================================================================
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
import io
import json
import os
import re
import statistics
import sys
import time
import urllib.request
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_VERSION = "1.0.0"
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
                        "exit_run": ""}
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
                  bench: List[Tuple[str, float]]) -> Dict[str, Any]:
    sym, px = ep["symbol"], ep["seat_px"]
    res: Dict[str, Any] = {
        "ep": ep, "seat_date": ep["seat_at"].strftime("%Y-%m-%d"),
        "r": {k: None for k in HORIZONS}, "x": {k: None for k in HORIZONS},
        "r_exit": None, "r_now": None, "x_now": None,
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
    if ep["exit_at"] is not None:
        ei = _exit_bar_index(sym, ep["exit_at"], closes)
        if ei is None or ei < b1:
            res["pre_close_exit"] = True
        else:
            res["r_exit"] = _pct(closes[ei][1], px)
    res["r_now"] = _pct(closes[-1][1], px)
    b = _bench_ret(bench, d1, closes[-1][0])
    res["x_now"] = None if b is None else res["r_now"] - b
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


def detail_rows(scored: List[Dict[str, Any]]) -> List[List[Any]]:
    rows: List[List[Any]] = []
    for s in sorted(scored, key=lambda s: (s["ep"]["seat_at"], -s["ep"]["seq"]),
                    reverse=True):
        ep = s["ep"]
        rows.append([
            s["seat_date"], ep["seat_at"].strftime("%H:%M"), ep["symbol"],
            ep["name"], ep["sector"],
            "" if ep["seat_px"] is None else ep["seat_px"], ep["ccy"],
            ep["output"], ep["stability"],
            ep["exit_at"].strftime("%Y-%m-%d") if ep["exit_at"] else "",
            ep["exit_kind"], len(ep["days"]), round(ep["hours"], 1),
            _r2(s["r"][1]), _r2(s["r"][5]), _r2(s["r"][10]), _r2(s["r"][20]),
            _r2(s["r_exit"]), _r2(s["r_now"]), _r2(s["x"][5]),
            _r2(s["x_now"]), s["status"]])
    return rows


def scorecard_rect(meta: str, summary: List[List[Any]], churn: List[List[Any]],
                   detail: List[List[Any]]) -> List[List[Any]]:
    """The whole tab as ONE rectangle (uniform width), written in one call."""
    rect: List[List[Any]] = (
        [[meta], [],
         ["SUMMARY - % from the board's printed seat price to later venue "
          "closes (+k = k-th session close after the seat)"], SUMMARY_HEADER]
        + summary
        + [[], ["CHURN - how long seats lasted and how they ended"], CHURN_HEADER]
        + churn
        + [[], ["DETAIL - one row per board seat episode, newest first"],
           DETAIL_HEADER]
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
                       churn: List[List[Any]], since: str) -> str:
    keep = ("All", "Since " + since)
    lines = ["### " + meta, "",
             "| " + " | ".join(SUMMARY_HEADER) + " |",
             "|" + "---|" * len(SUMMARY_HEADER)]
    lines += ["| " + " | ".join(str(c) for c in r) + " |"
              for r in summary if r[0] in keep]
    lines += ["", "| " + " | ".join(CHURN_HEADER) + " |",
              "|" + "---|" * len(CHURN_HEADER)]
    lines += ["| " + " | ".join(str(c) for c in r) + " |" for r in churn]
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
                     md: str) -> List[str]:
    os.makedirs(out_dir, exist_ok=True)
    p_det = os.path.join(out_dir, "board_scorecard_detail.csv")
    p_sum = os.path.join(out_dir, "board_scorecard_summary.csv")
    p_md = os.path.join(out_dir, "board_scorecard.md")
    with open(p_det, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(DETAIL_HEADER)
        w.writerows(detail)
    with open(p_sum, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(SUMMARY_HEADER)
        w.writerows(summary)
        w.writerow([])
        w.writerow(CHURN_HEADER)
        w.writerows(churn)
    with open(p_md, "w", encoding="utf-8") as fh:
        fh.write(md)
    return [p_det, p_sum, p_md]


def run_scorecard(tsv: Optional[str] = None, live: bool = False,
                  prices_csv: Optional[str] = None, fetch_yahoo_: bool = False,
                  fetch_eodhd: bool = False, since: str = DEFAULT_SINCE,
                  out_dir: str = DEFAULT_OUT_DIR,
                  asof: Optional[str] = None) -> int:
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
    if prices_csv:
        prices, psrc = load_prices_csv(prices_csv), "csv"
    elif fetch_yahoo_:
        prices, psrc = fetch_yahoo(syms + [BENCH_SYMBOL], frm, asof_utc), "yahoo"
    elif fetch_eodhd:
        tok = os.getenv("TFB_EODHD_API_KEY", "").strip()
        if not tok:
            raise SystemExit("TFB_EODHD_API_KEY missing (fetch mode)")
        prices, psrc = fetch_prices(syms + [BENCH_SYMBOL], frm, tok), "eodhd"
    else:
        raise SystemExit("--scorecard needs --prices-csv, --fetch-yahoo or --fetch")
    prices = {s: _final_bars(s, sorted(v), asof_utc) for s, v in prices.items()}
    bench = prices.get(BENCH_SYMBOL, [])
    scored = [score_episode(ep, prices.get(ep["symbol"], []), bench)
              for ep in episodes]
    covered = sum(1 for s in syms if prices.get(s))
    coverage = covered / len(syms)
    summary, churn = summarize(scored, since)
    detail = detail_rows(scored)
    n_open = sum(1 for e in episodes if e["exit_at"] is None)
    n_since = sum(1 for s in scored if s["seat_date"] >= since)
    n_priced = sum(1 for s in scored if s["status"] in ("OPEN", "CLOSED"))
    meta = (f"{tag} generated {asof_utc:%Y-%m-%d %H:%M}Z | source {source} "
            f"{ev_stats['rows']} rows | episodes {len(episodes)} (open {n_open}, "
            f"since {since}: {n_since}) | priced {n_priced} | prices {psrc} "
            f"{covered}/{len(syms)} symbols | bench SPUS "
            f"{'ok' if bench else 'missing'} | mode {mode}")
    rect = scorecard_rect(meta, summary, churn, detail)
    md = scorecard_markdown(meta, summary, churn, since)
    print(md)
    paths = _write_artifacts(out_dir, summary, churn, detail, md)
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
                            f"prices={psrc} {covered}/{len(syms)} wrote={wrote}",
                            details)
    elif degraded:
        print(f"::warning::{tag} price coverage {covered}/{len(syms)} below "
              f"{_min_coverage():.0%} (observe - nothing written)")
    print(f"{tag} mode={mode} source={source} episodes={len(episodes)} "
          f"open={n_open} since={since}({n_since}) priced={n_priced} "
          f"prices={psrc} {covered}/{len(syms)} bench={'ok' if bench else 'missing'} "
          f"orphan_exits={ep_stats['orphan_exits']} wrote={wrote} "
          f"artifacts={len(paths)}")
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
    print(f"SELFTEST {ok}/16: ALL GREEN")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tsv")
    ap.add_argument("--out", default="selection_outcomes.csv")
    ap.add_argument("--prices-csv")
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--selftest", action="store_true")
    # v1.0.0 [P-182] board scorecard - opt-in; the flags above are unchanged.
    ap.add_argument("--scorecard", action="store_true",
                    help="score every board seat episode (TFB_BOARD_SCORECARD)")
    ap.add_argument("--live", action="store_true",
                    help="scorecard: read _Selection_Log from the workbook")
    ap.add_argument("--fetch-yahoo", action="store_true",
                    help="scorecard: daily closes from the Yahoo chart API")
    ap.add_argument("--since", default=DEFAULT_SINCE)
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    ap.add_argument("--asof", help="UTC moment treated as now (replays/tests)")
    a = ap.parse_args(argv)
    if a.selftest:
        return selftest()
    if a.scorecard:
        return run_scorecard(tsv=a.tsv, live=a.live, prices_csv=a.prices_csv,
                             fetch_yahoo_=a.fetch_yahoo, fetch_eodhd=a.fetch,
                             since=a.since, out_dir=a.out_dir, asof=a.asof)
    if not a.tsv:
        ap.error("--tsv required (or --selftest)")
    return run(a.tsv, a.out, a.prices_csv, a.fetch)


if __name__ == "__main__":
    sys.exit(main())
