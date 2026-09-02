#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tfb_reco_coherence.py — v1.0.0 (2026-09-02)
================================================================================
WHY THIS EXISTS
    The 2026-09-02 six-gate audit found the judgement layer broken while the
    plumbing was healthy: 79.7% of Global_Markets carried a SELL-tier
    recommendation, 0.45% BUY-tier, 979 rows recommended SELL against the
    engine's own +10% forecast, and the published Target Price disagreed with
    the row's own Upside % on 89.9% of rows. None of it was visible to any
    daily instrument; it was re-derived by hand. This script prints those
    numbers every day as a measured table (the tfb_acceptance.py pattern) and
    writes them to docs/evidence so Phase 2 (ladder recalibration) has a
    "before" baseline and a daily "after".

WHAT IT MEASURES (per pool page, Global_Markets + Market_Leaders)
    C1  SELL-tier share            (SELL + REDUCE + STRONG_SELL + AVOID) / rows
    C2  BUY-tier share             (BUY + ACCUMULATE + STRONG_BUY) / rows
    C3  forecast-vs-reco clash     SELL-tier reco AND Expected ROI 12M > +10%
    C4  analyst-vs-reco polar clash BUY-tier analyst vs SELL-tier reco (or inverse)
    C5  score/ladder headroom      p50 Overall Score (Raw) vs BUY cutoff 70
    B1  target/upside disagreement |Upside% - (Target/Price - 1)| > 5pp
    B2  target outliers            Target/Price outside [0.25, 3.0] (post v5.135.0 => ~0)
    B3  P/E identity               |P/E - Price/EPS| > 5%
    B4  reliability cluster share  provider_target rows on {70.4,71.5,75.4,76.5}

VERDICTS are thresholds, not opinions:
    PASS / WARN / FAIL bands are written next to each row and can be tuned by
    env (TFB_COH_<ID>_WARN / _FAIL). Defaults are the 2026-09-02 baseline
    tightened to what a coherent engine should produce.

USAGE
    python3 scripts/tfb_reco_coherence.py --exports <dir with the browser TSVs>
    python3 scripts/tfb_reco_coherence.py --exports <dir> --json docs/evidence/reco_coherence_2026-09-02.json
    python3 scripts/tfb_reco_coherence.py --selftest
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import os
import re
import sys
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

VERSION = "1.0.0"
PAGES = ("Global_Markets", "Market_Leaders")
CLUSTER = {70.4, 71.5, 75.4, 76.5}
SELL_TIER = {"SELL", "REDUCE", "STRONG_SELL", "AVOID"}
BUY_TIER = {"BUY", "ACCUMULATE", "STRONG_BUY"}
HOLD_TIER = {"HOLD", "NEUTRAL", "WATCH"}

# id -> (warn, fail, direction)  direction "max": measured must be <= ; "min": >=
DEFAULT_BANDS: Dict[str, tuple] = {
    "C1": (40.0, 60.0, "max"),   # SELL-tier share %
    "C2": (5.0, 2.0, "min"),     # BUY-tier share %
    "C3": (2.0, 5.0, "max"),     # forecast-vs-reco clash %
    "C4": (15.0, 25.0, "max"),   # analyst polar clash %
    "C5": (60.0, 55.0, "min"),   # p50 raw score (BUY cutoff 70)
    "B1": (10.0, 25.0, "max"),   # target/upside disagreement %
    "B2": (1.0, 5.0, "max"),     # target outliers %
    "B3": (20.0, 40.0, "max"),   # P/E identity mismatch %
    "B4": (40.0, 60.0, "max"),   # cluster share of provider_target %
}


@dataclass
class Check:
    cid: str
    page: str
    criterion: str
    verdict: str
    measured: Optional[float]
    evidence: str


def _num(s: Any) -> Optional[float]:
    if s is None:
        return None
    t = str(s).replace("\u25b2", "").replace("\u25bc", "").replace(",", "").replace("%", "").strip()
    if t in ("", "-", "N/A", "n/a", "None", "#N/A", "#DIV/0!", "\u2014"):
        return None
    try:
        return float(t)
    except ValueError:
        return None


def _band(cid: str, measured: Optional[float]) -> str:
    if measured is None:
        return "NA"
    warn, fail, direction = DEFAULT_BANDS[cid]
    w = _num(os.getenv(f"TFB_COH_{cid}_WARN")) or warn
    f = _num(os.getenv(f"TFB_COH_{cid}_FAIL")) or fail
    if direction == "max":
        return "PASS" if measured <= w else ("WARN" if measured <= f else "FAIL")
    return "PASS" if measured >= w else ("WARN" if measured >= f else "FAIL")


def _load_tsv(path: str) -> List[Dict[str, str]]:
    csv.field_size_limit(10 ** 9)
    with open(path, newline="", encoding="utf-8") as fh:
        rd = csv.reader(fh, delimiter="\t")
        hdr = [h.strip() for h in next(rd)]
        return [dict(zip(hdr, r)) for r in rd]


def _find_export(exports: str, page: str) -> Optional[str]:
    cands = sorted(glob.glob(os.path.join(exports, f"*{page}*.tsv")), key=os.path.getmtime)
    return cands[-1] if cands else None


def _pct(n: int, d: int) -> Optional[float]:
    return None if d <= 0 else round(n / d * 100.0, 2)


def measure_page(page: str, rows: List[Dict[str, str]]) -> List[Check]:
    n = len(rows)
    out: List[Check] = []
    sell = buy = clash = polar = polar_n = 0
    b1 = b1n = b2 = b2n = b3 = b3n = 0
    cl = cln = 0
    raw: List[float] = []
    for r in rows:
        rec = (r.get("Recommendation") or "").strip().upper()
        ar = (r.get("Analyst Rating") or "").strip().upper()
        e = _num(r.get("Expected ROI 12M"))
        if e is not None and abs(e) < 3:
            e *= 100.0
        if rec in SELL_TIER:
            sell += 1
            if e is not None and e > 10.0:
                clash += 1
        if rec in BUY_TIER:
            buy += 1
        if ar and rec and (ar in BUY_TIER or ar in SELL_TIER or ar in HOLD_TIER) and \
                (rec in BUY_TIER or rec in SELL_TIER or rec in HOLD_TIER):
            polar_n += 1
            if (ar in BUY_TIER and rec in SELL_TIER) or (ar in SELL_TIER and rec in BUY_TIER):
                polar += 1
        s = _num(r.get("Overall Score (Raw)"))
        if s is not None:
            raw.append(s)
        p = _num(r.get("Current Price"))
        t = _num(r.get("Target Price"))
        up = _num(r.get("Upside %"))
        if p and p > 0 and t and t > 0:
            b2n += 1
            ratio = t / p
            if ratio < 0.25 or ratio > 3.0:
                b2 += 1
            if up is not None:
                b1n += 1
                u = up / 100.0 if abs(up) > 1.5 else up
                if abs((ratio - 1.0) - u) > 0.05:
                    b1 += 1
        pe = _num(r.get("P/E (TTM)"))
        eps = _num(r.get("EPS (TTM)"))
        if p and pe and eps and eps != 0:
            b3n += 1
            if abs(pe - p / eps) > max(0.05 * abs(pe), 0.05):
                b3 += 1
        if (r.get("Forecast Source") or "").strip() == "provider_target":
            rel = _num(r.get("Forecast Reliability Score"))
            if rel is not None:
                cln += 1
                if round(rel, 1) in CLUSTER:
                    cl += 1
    raw.sort()
    p50 = raw[len(raw) // 2] if raw else None
    m = {
        "C1": (_pct(sell, n), f"sell_tier={sell}/{n}"),
        "C2": (_pct(buy, n), f"buy_tier={buy}/{n}"),
        "C3": (_pct(clash, n), f"sell_reco_with_forecast_gt_10pct={clash}/{n}"),
        "C4": (_pct(polar, polar_n), f"polar={polar}/{polar_n} rated rows"),
        "C5": (None if p50 is None else round(p50, 2), f"p50_raw={p50} n={len(raw)} BUY_cutoff=70 ACCUMULATE=60"),
        "B1": (_pct(b1, b1n), f"disagree={b1}/{b1n} priced+targeted rows"),
        "B2": (_pct(b2, b2n), f"outliers={b2}/{b2n} (band 0.25x-3.0x)"),
        "B3": (_pct(b3, b3n), f"mismatch={b3}/{b3n} rows with P/E and EPS"),
        "B4": (_pct(cl, cln), f"cluster={cl}/{cln} provider_target rows"),
    }
    names = {
        "C1": "SELL-tier share of recommendations", "C2": "BUY-tier share of recommendations",
        "C3": "SELL-tier reco vs engine forecast > +10%", "C4": "analyst vs recommendation polar clash",
        "C5": "p50 Overall Score (Raw) vs BUY cutoff", "B1": "Upside% vs Target/Price disagreement",
        "B2": "Target/Price outliers", "B3": "P/E != Price/EPS", "B4": "reliability cluster share (provider_target)",
    }
    for cid in ("C1", "C2", "C3", "C4", "C5", "B1", "B2", "B3", "B4"):
        val, ev = m[cid]
        out.append(Check(cid, page, names[cid], _band(cid, val), val, ev))
    return out


def run(exports: str) -> List[Check]:
    out: List[Check] = []
    for page in PAGES:
        path = _find_export(exports, page)
        if not path:
            out.append(Check("NA", page, "export present", "NA", None, "no TSV for page"))
            continue
        out.extend(measure_page(page, _load_tsv(path)))
    return out


def render(checks: List[Check]) -> str:
    lines = [f"TFB RECO COHERENCE v{VERSION}", f"{'id':4} {'page':16} {'criterion':46} {'verdict':7} {'measured':>9}  evidence"]
    for c in checks:
        mv = "" if c.measured is None else f"{c.measured:g}"
        lines.append(f"{c.cid:4} {c.page:16} {c.criterion:46} {c.verdict:7} {mv:>9}  {c.evidence}")
    tally = {k: sum(1 for c in checks if c.verdict == k) for k in ("PASS", "WARN", "FAIL", "NA")}
    lines.append("TALLY " + " ".join(f"{k}={v}" for k, v in tally.items()))
    return "\n".join(lines)


def _selftest() -> int:
    good = [{"Recommendation": "BUY", "Analyst Rating": "BUY", "Expected ROI 12M": "0.2", "Overall Score (Raw)": "72",
             "Current Price": "100", "Target Price": "120", "Upside %": "0.2", "P/E (TTM)": "20", "EPS (TTM)": "5",
             "Forecast Source": "provider_target", "Forecast Reliability Score": "82.0"}] * 10
    bad = [{"Recommendation": "SELL", "Analyst Rating": "BUY", "Expected ROI 12M": "0.3", "Overall Score (Raw)": "45",
            "Current Price": "100", "Target Price": "5000", "Upside %": "0.1", "P/E (TTM)": "30", "EPS (TTM)": "-1",
            "Forecast Source": "provider_target", "Forecast Reliability Score": "76.5"}] * 10
    g = {c.cid: c for c in measure_page("Global_Markets", good)}
    b = {c.cid: c for c in measure_page("Global_Markets", bad)}
    assert all(g[k].verdict == "PASS" for k in ("C1", "C2", "C3", "C4", "C5", "B1", "B2", "B3", "B4")), g
    assert all(b[k].verdict == "FAIL" for k in ("C1", "C2", "C3", "C4", "C5", "B1", "B2", "B3", "B4")), b
    assert b["C3"].measured == 100.0 and b["B2"].measured == 100.0 and g["B4"].measured == 0.0
    print("selftest: PASS 2/2 fixtures (all-good, all-bad)")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--exports", help="directory holding the browser TSV exports")
    ap.add_argument("--json", help="write the checks to this JSON path (docs/evidence/...)")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args(argv)
    if args.selftest:
        return _selftest()
    if not args.exports:
        ap.error("--exports is required (or --selftest)")
    checks = run(args.exports)
    print(render(checks))
    if args.json:
        os.makedirs(os.path.dirname(args.json) or ".", exist_ok=True)
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump({"version": VERSION, "checks": [asdict(c) for c in checks]}, fh, indent=2)
        print(f"wrote {args.json}")
    return 1 if any(c.verdict == "FAIL" for c in checks) else 0


if __name__ == "__main__":
    sys.exit(main())
