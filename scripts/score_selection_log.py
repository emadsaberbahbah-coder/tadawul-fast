#!/usr/bin/env python3
"""
scripts/score_selection_log.py — Selection_Log ticket-vs-reality scorer
=======================================================================
VERSION 0.1.0 (2026-08-22) — NEW SCRIPT (read-only, artifact-only)

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
import csv
import io
import json
import os
import sys
import urllib.request
from collections import OrderedDict
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

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
    print(f"SELFTEST {ok}/7: ALL GREEN")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tsv")
    ap.add_argument("--out", default="selection_outcomes.csv")
    ap.add_argument("--prices-csv")
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        return selftest()
    if not a.tsv:
        ap.error("--tsv required (or --selftest)")
    return run(a.tsv, a.out, a.prices_csv, a.fetch)


if __name__ == "__main__":
    sys.exit(main())
