#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tfb_backtest.py — v1.3.0 (2026-11-26)
================================================================================
WHY v1.3.0 (week plan of 23 Nov, package 4): two things v1.2.0 left. (1) A numeric
signal was cut into quintiles of ALL rows pooled. A score whose level moves with
the market then puts whole weeks in one quintile, and the table reads the market,
not the choice between names; and a pooled cut point is made of later rows too.
Quintiles are now cut per formation date: a row's quintile is its rank among the
rows recorded the same day (five or more values that day; fewer -> the row is left
out and counted), so no value of another day can enter it. The day's rows are
ALL the records made that day (formation_records: one per Key, the same horizon,
filter and date scope, no test of status or outcome) - not only the ones that
later matured as WIN or LOSS: who else was on the list is known at formation,
which of them matured is not (Codex P2 on #1086). Rollback: --quintiles
pooled (or TFB_BACKTEST_QUINTILES=pooled) restores the v1.2.0 cuts. (2) Every
number but the walk-forward block is a description of rows already seen. Each
result now says so: "evidence" lists its keys as descriptive or out_of_sample,
"verdict_basis" says what the verdict stands on, the console tags each line, and
the JSON carries the same legend. The walk-forward also reports the smallest gap
between a fold's last training entry and its first test day (wf_min_gap_days): a
training row is usable only once its own horizon has passed (purge), plus
--embargo-days. Functions added: 3 (formation_records, date_quintiles,
_quintile_mode).

WHY v1.2.0 (external audit of 25 Nov, P1 "validation"): the CV Brier that decides
SEPARATES shuffled rows at random into five folds. Cohorts recorded on the same
day, and overlapping 1W/2W/1M windows, then sat on both sides of a fold, and the
quintile cut points were fitted on every row, test rows included. That is a
look-ahead: the score could only be earned by knowing later outcomes. v1.2.0
scores in date order instead. Rows are cut into k+1 contiguous date blocks; each
of the last k blocks is tested on a model fitted ONLY on rows whose outcome was
known (Target Date, else Date Recorded + horizon) at least --embargo-days before
the block's first entry date (purge + embargo). Quintile cuts are refitted on that
training set. The baseline is scored on the same test rows with the training win
rate (an out-of-sample prevalence baseline, identical cohort). The verdict uses
the walk-forward gain; the random K-fold numbers stay in the report as a labelled
legacy diagnostic. Rollback: --cv legacy (or TFB_BACKTEST_CV=legacy) restores the
v1.1.1 verdict exactly. Functions added: 4 (_day, label_days, wf_brier, _cv_mode).

WHY: the Strategy's rule is "register a hypothesis, backtest it, then change a
weight or gate". H-28 (stated reliability predicts outcomes) was rejected by an
ad-hoc computation; this makes that computation a repeatable instrument so any
column of Performance_Log can be tested the same way (H-29: which signal DOES
separate winners), by either AI, on the same export, with the same numbers.

WHAT IT DOES
  - loads Performance_Log (browser TSV export, xlsx, or --live), keeps matured
    cohorts with a WIN/LOSS outcome, DEDUPLICATED by Key (the v6.35.0 rule);
  - for each --signal column: numeric -> bands (custom edges, or quintiles cut
    per formation date - v1.3.0), categorical -> groups; per group n, win %,
    mean/median realized ROI % (all descriptive);
  - predictive power: Brier of the raw value as a probability (numeric 0-100
    only) vs naive 0.5 vs constant base rate; 5-fold cross-validated Brier of a
    group-calibrated probability (shrinkage k=20) vs the base rate;
    Spearman(signal, realized ROI) for numeric signals, using Pearson
    correlation of average tie ranks; undefined rank IC is JSON null with an
    explicit status (fewer than three pairs or a constant input); win-rate spread
    (max-min group win %, groups with n >= min_n);
  - verdict per signal: SEPARATES (the purged walk-forward Brier beats the
    training base rate by >= 0.002 - the only out-of-sample number - AND
    spread >= 5 pp with n >= min_n in both extremes) / WEAK / NONE.

USAGE
  python scripts/tfb_backtest.py --export-dir DIR [--xlsx F] --signal "Entry Forecast Reliability" --signal Confidence ...
  python scripts/tfb_backtest.py --live --signal ... (env: DEFAULT_SPREADSHEET_ID + Google creds)
  python scripts/tfb_backtest.py --export-dir DIR --all-signals      # every Entry*/Horizon/Origin column
  python scripts/tfb_backtest.py --selftest
Options: --edges "50,70,85" (numeric bands) --min-n 100 --json out.json --horizon 1W,2W,1M
         --filter "Origin Tab=Top_10_Investments" (repeatable, exact match)   [v1.1.0]
         --since 2026-08-01 / --until 2026-08-31  (Date Recorded window)       [v1.1.0]
         --cv wf|legacy (default wf) --embargo-days 1                         [v1.2.0]
         --quintiles date|pooled (default date)                               [v1.3.0]
READ-ONLY. No writes, ever.
"""
from __future__ import annotations

import argparse
import base64
import csv
import datetime as _dt
import json
import math
import os
import random
import statistics
import sys
from typing import Any, Dict, List, Optional, Tuple

VERSION = "1.3.0"
DEFAULT_SIGNALS = ["Entry Forecast Reliability", "Entry Score", "Confidence", "Entry Investability",
                   "Entry Recommendation", "Entry Risk Bucket", "Horizon", "Origin Tab"]


def _s(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _f(v: Any) -> Optional[float]:
    t = _s(v).replace("%", "").replace(",", "").replace("\u25b2", "").replace("\u25bc", "").strip()
    if not t:
        return None
    try:
        x = float(t)
        return x if math.isfinite(x) else None
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# loading                                                                     #
# --------------------------------------------------------------------------- #
def _rows_from_tsv(path: str) -> List[List[str]]:
    with open(path, encoding="utf-8", newline="") as fh:
        return [list(r) for r in csv.reader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)]


def _rows_from_xlsx(path: str, tab: str = "Performance_Log") -> List[List[str]]:
    from openpyxl import load_workbook  # optional dependency
    wb = load_workbook(path, read_only=True, data_only=True)
    if tab not in wb.sheetnames:
        return []
    return [[_s(c) for c in r] for r in wb[tab].iter_rows(values_only=True)]


def _rows_live(sheet_id: str, tab: str = "Performance_Log") -> List[List[str]]:
    import gspread
    from google.oauth2 import service_account
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    path = _s(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    if path and os.path.exists(path):
        creds = service_account.Credentials.from_service_account_file(path, scopes=scopes)
    else:
        raw = _s(os.getenv("GOOGLE_SHEETS_CREDENTIALS")) or _s(os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64"))
        if not raw.startswith("{"):
            raw = base64.b64decode(raw).decode("utf-8", errors="replace").strip()
        creds = service_account.Credentials.from_service_account_info(json.loads(raw), scopes=scopes)
    ws = gspread.authorize(creds).open_by_key(sheet_id).worksheet(tab)
    rc = int(getattr(ws, "row_count", 0) or 0)
    return [[_s(c) for c in r] for r in (ws.get(f"A1:AF{max(2, rc)}") or [])]


def load_records(rows: List[List[str]]) -> Tuple[List[str], List[Dict[str, str]]]:
    """Find the header row (first cell 'Record ID'), return (headers, records)."""
    hi = next((i for i, r in enumerate(rows[:12]) if r and _s(r[0]) == "Record ID"), None)
    if hi is None:
        return [], []
    hdr = [_s(h) for h in rows[hi]]
    out = []
    for r in rows[hi + 1:]:
        if not r or not _s(r[0]):
            continue
        out.append({h: (r[i] if i < len(r) else "") for i, h in enumerate(hdr) if h})
    return hdr, out


def decided_cohorts(records: List[Dict[str, str]], horizons: Optional[List[str]] = None,
                    filters: Optional[Dict[str, str]] = None, since: str = "", until: str = "") -> List[Dict[str, str]]:
    """Matured WIN/LOSS, one record per Key (first occurrence); optional horizon,
    exact-match column filters and a Date Recorded window (v1.1.0)."""
    seen, out = set(), []
    for r in records:
        if _s(r.get("Status")).lower() != "matured" or _s(r.get("Outcome")) not in ("WIN", "LOSS"):
            continue
        # dedup FIRST: the canonical cohort for a Key is its first occurrence,
        # whatever window or filter is applied afterwards (v1.1.0 fix).
        k = _s(r.get("Key"))
        if k in seen:
            continue
        seen.add(k)
        if horizons and _s(r.get("Horizon")) not in horizons:
            continue
        if filters and any(_s(r.get(k2)) != v for k2, v in filters.items()):
            continue
        d = _s(r.get("Date Recorded (Riyadh)"))[:10]
        if since and d and d < since:
            continue
        if until and d and d > until:
            continue
        if _f(r.get("Realized ROI %")) is None:
            continue
        out.append(r)
    return out


def formation_records(records: List[Dict[str, str]], horizons: Optional[List[str]] = None,
                      filters: Optional[Dict[str, str]] = None, since: str = "", until: str = "") -> List[Dict[str, str]]:
    """v1.3.0 (Codex P2 on #1086): every record as it stood when it was made - one per Key (first
    occurrence), in the same horizon, column and Date Recorded scope as decided_cohorts, with NO test of
    status, outcome or realised return. It is the cross-section a date quintile is cut from: who else was
    recorded that day is known at formation; which of them later matured, won, lost or broke even is not."""
    seen, out = set(), []
    for r in records:
        k = _s(r.get("Key"))
        if k in seen:
            continue
        seen.add(k)
        if horizons and _s(r.get("Horizon")) not in horizons:
            continue
        if filters and any(_s(r.get(k2)) != v for k2, v in filters.items()):
            continue
        d = _s(r.get("Date Recorded (Riyadh)"))[:10]
        if since and d and d < since:
            continue
        if until and d and d > until:
            continue
        out.append(r)
    return out


# --------------------------------------------------------------------------- #
# statistics (pure)                                                           #
# --------------------------------------------------------------------------- #
def brier(pairs: List[Tuple[float, float]]) -> float:
    return sum((p - y) ** 2 for p, y in pairs) / len(pairs) if pairs else float("nan")


def spearman(a: List[float], b: List[float]) -> float:
    """Pearson correlation of average tie ranks for aligned finite pairs.

    Return NaN for fewer than three pairs or zero rank variance, preserving the
    float API's small-sample convention. Reject unequal lengths/nonfinite input
    instead of manufacturing ranks for invalid observations. The no-ties
    squared-rank-difference shortcut is not valid when either input has ties.
    """
    if len(a) != len(b):
        raise ValueError("Spearman inputs must have equal lengths")
    try:
        finite = all(math.isfinite(x) for x in a) and all(math.isfinite(x) for x in b)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("Spearman inputs must contain finite numbers") from exc
    if not finite:
        raise ValueError("Spearman inputs must contain finite numbers")
    n = len(a)
    if n < 3:
        return float("nan")
    def ranks(x):
        order = sorted(range(n), key=lambda i: x[i])
        r = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j + 1 < n and x[order[j + 1]] == x[order[i]]:
                j += 1
            avg = (i + j) / 2.0 + 1
            for k in range(i, j + 1):
                r[order[k]] = avg
            i = j + 1
        return r
    ra, rb = ranks(a), ranks(b)
    center = (n + 1) / 2.0
    da, db = [r - center for r in ra], [r - center for r in rb]
    va = math.fsum(d * d for d in da)
    vb = math.fsum(d * d for d in db)
    if va == 0.0 or vb == 0.0:
        return float("nan")
    correlation = math.fsum(x * y for x, y in zip(da, db)) / math.sqrt(va * vb)
    return max(-1.0, min(1.0, correlation))


def cv_brier(groups: List[Any], ys: List[float], k: int = 5, shrink: float = 20.0, seed: int = 7) -> float:
    """LEGACY (v1.1.1) random K-fold. Leaks time: same-day and overlapping-window
    cohorts land on both sides of a fold. Kept only as a labelled diagnostic and
    as the --cv legacy rollback; the verdict uses wf_brier()."""
    idx = list(range(len(ys)))
    random.Random(seed).shuffle(idx)
    folds = [set(idx[i::k]) for i in range(k)]
    out = []
    for f in folds:
        train = [i for i in idx if i not in f]
        base = sum(ys[i] for i in train) / max(1, len(train))
        agg: Dict[Any, List[float]] = {}
        for i in train:
            a = agg.setdefault(groups[i], [0.0, 0.0])
            a[0] += ys[i]
            a[1] += 1
        for i in f:
            s, n = agg.get(groups[i], [0.0, 0.0])
            p = (s + base * shrink) / (n + shrink) if n else base
            out.append((p, ys[i]))
    return brier(out)


_HZ_DAYS = {"1W": 7, "2W": 14, "1M": 30, "3M": 90, "6M": 180, "1Y": 365, "3Y": 1095, "5Y": 1825}
WF_MIN_TRAIN = 30


def _day(v: Any) -> Optional[_dt.date]:
    """v1.2.0: the YYYY-MM-DD prefix of a cell as a date; None when absent or invalid."""
    t = _s(v)[:10]
    try:
        return _dt.date.fromisoformat(t) if len(t) == 10 else None
    except ValueError:
        return None


# v1.2.0 (review): an outcome is settled at the first exchange-session close at or after the target
# (track_performance._outcome_target_session), so a weekend or holiday target is known later than its date.
# Without a recorded Maturity Date the known day is padded by this many calendar days (a full weekend).
AVAILABILITY_PAD_DAYS = 3


def label_days(cohorts: List[Dict[str, str]]) -> Tuple[List[Optional[_dt.date]], List[Optional[_dt.date]]]:
    """v1.2.0: (entry day, day the outcome became known) per cohort. The nominal target is Target
    Date (Riyadh), else Date Recorded + the horizon's calendar days. The known day is the recorded
    Maturity Date (when the outcome was finalised) if it is on or after that target; otherwise the
    target + AVAILABILITY_PAD_DAYS, so a weekend or holiday target never enters training before the
    session that settled it. None when no target can be read (the row then stays out of the walk-forward)."""
    entry, known = [], []
    for r in cohorts:
        e = _day(r.get("Date Recorded (Riyadh)"))
        t = _day(r.get("Target Date (Riyadh)"))
        if t is None and e is not None and _s(r.get("Horizon")) in _HZ_DAYS:
            t = e + _dt.timedelta(days=_HZ_DAYS[_s(r.get("Horizon"))])
        if e is not None and t is not None and t < e:
            t = None
        if t is not None:
            m = _day(r.get("Maturity Date"))
            t = m if (m is not None and m >= t) else t + _dt.timedelta(days=AVAILABILITY_PAD_DAYS)
        entry.append(e)
        known.append(t)
    return entry, known


def wf_brier(ys: List[float], entry: List[Optional[_dt.date]], known: List[Optional[_dt.date]],
             labeler_for: Any, k: int = 5, shrink: float = 20.0, embargo_days: int = 1,
             min_train: int = WF_MIN_TRAIN) -> Dict[str, Any]:
    """v1.2.0 purged walk-forward Brier of the group-calibrated probability.

    Rows with an entry and a known day are ordered by entry day and cut into k+1
    contiguous blocks (a day never straddles two blocks). Block j (j >= 1) is
    scored with a model fitted on rows whose outcome was known on or before
    (block start - embargo_days); ``labeler_for(train_idx)`` returns the group
    labeller fitted on those rows only. The baseline is the training win rate,
    scored on the same test rows. A fold with fewer than ``min_train`` training
    rows is skipped. Returns brier, base_brier, gain, scored, folds, status, and
    (v1.3.0) wf_min_gap_days - the smallest distance, over the folds, between the
    last training entry day and the first test day - beside wf_embargo_days."""
    usable = sorted((i for i in range(len(ys)) if entry[i] is not None and known[i] is not None),
                    key=lambda i: (entry[i], i))
    out: Dict[str, Any] = {"wf_brier": None, "wf_base_brier": None, "wf_gain_vs_base": None,
                           "wf_scored": 0, "wf_folds": 0, "wf_status": "insufficient_dated_rows",
                           "wf_min_gap_days": None, "wf_embargo_days": max(0, int(embargo_days))}
    days = sorted({entry[i] for i in usable})
    if len(usable) < 2 * min_train or len(days) < k + 1:
        return out
    target = len(usable) / (k + 1)
    by_day: Dict[_dt.date, List[int]] = {}
    for i in usable:
        by_day.setdefault(entry[i], []).append(i)
    blocks: List[List[int]] = [[] for _ in range(k + 1)]
    done = 0
    for d in days:
        blocks[min(k, int(done // target))].extend(by_day[d])
        done += len(by_day[d])
    blocks = [b for b in blocks if b]
    model, base = [], []
    folds = 0
    gaps: List[int] = []
    for test in blocks[1:]:
        start = min(entry[i] for i in test)
        cutoff = start - _dt.timedelta(days=max(0, int(embargo_days)))
        train = [i for i in usable if known[i] <= cutoff and entry[i] < start]
        if len(train) < min_train:
            continue
        folds += 1
        gaps.append((start - max(entry[i] for i in train)).days)   # v1.3.0: the purge, as measured
        label = labeler_for(train)
        rate = sum(ys[i] for i in train) / len(train)
        agg: Dict[Any, List[float]] = {}
        for i in train:
            a = agg.setdefault(label(i), [0.0, 0.0])
            a[0] += ys[i]
            a[1] += 1
        for i in test:
            s_, n_ = agg.get(label(i), [0.0, 0.0])
            model.append(((s_ + rate * shrink) / (n_ + shrink) if n_ else rate, ys[i]))
            base.append((rate, ys[i]))
    if folds < 2 or not model:
        out.update({"wf_folds": folds, "wf_status": "insufficient_training_history"})
        return out
    mb, bb = brier(model), brier(base)
    out.update({"wf_brier": round(mb, 4), "wf_base_brier": round(bb, 4), "wf_gain_vs_base": round(bb - mb, 4),
                "wf_scored": len(model), "wf_folds": folds, "wf_status": "ok", "wf_min_gap_days": min(gaps)})
    return out


QUINTILE_MIN_NAMES = 5


def date_quintiles(xs: List[float], days: List[Optional[_dt.date]],
                   min_names: int = QUINTILE_MIN_NAMES) -> List[Optional[int]]:
    """v1.3.0: the quintile (1..5) of each value among the values of its own formation day.

    Equal values share the quintile of their average rank, so the order of the rows never matters. A row
    with no day, or on a day with fewer than ``min_names`` values, gets None. Nothing from another day is
    read, so a later row can never move an earlier row's quintile."""
    by_day: Dict[_dt.date, List[int]] = {}
    for i, d in enumerate(days):
        if d is not None:
            by_day.setdefault(d, []).append(i)
    out: List[Optional[int]] = [None] * len(xs)
    for idx in by_day.values():
        n = len(idx)
        if n < max(1, int(min_names)):
            continue
        order = sorted(idx, key=lambda i: xs[i])
        j = 0
        while j < n:
            k = j
            while k + 1 < n and xs[order[k + 1]] == xs[order[j]]:
                k += 1
            q = min(5, 1 + int(5 * ((j + k) / 2.0 + 0.5) / n))
            for t in range(j, k + 1):
                out[order[t]] = q
            j = k + 1
    return out


def _quintile_mode(explicit: str = "") -> str:
    """v1.3.0: date (default) or pooled; --quintiles wins over TFB_BACKTEST_QUINTILES."""
    raw = (explicit or os.getenv("TFB_BACKTEST_QUINTILES") or "date").strip().lower()
    return "pooled" if raw == "pooled" else "date"


# v1.3.0: what each number is. Only the walk-forward block is scored on rows the model had not seen.
DESCRIPTIVE_KEYS = ("base_win_pct", "base_brier", "groups", "raw_brier_as_probability", "spearman_vs_roi",
                    "win_spread_pp", "spread_z", "cv_brier_group_calibrated", "cv_gain_vs_base")
OUT_OF_SAMPLE_KEYS = ("wf_brier", "wf_base_brier", "wf_gain_vs_base")
OUTPUT_LEGEND = {
    "descriptive": "computed on rows already seen: says what happened, not what will - never evidence on its own "
                   "(the legacy random K-fold belongs here: it leaks time)",
    "out_of_sample": "purged walk-forward: each block scored by a model fitted only on outcomes known before it",
}


def _cv_mode(explicit: str = "") -> str:
    """v1.2.0: wf (default) or legacy; --cv wins over TFB_BACKTEST_CV."""
    raw = (explicit or os.getenv("TFB_BACKTEST_CV") or "wf").strip().lower()
    return "legacy" if raw == "legacy" else "wf"


def evaluate_signal(cohorts: List[Dict[str, str]], signal: str, edges: Optional[List[float]] = None,
                    min_n: int = 100, cv: str = "", embargo_days: int = 1, quintiles: str = "",
                    formation: Optional[List[Dict[str, str]]] = None) -> Dict[str, Any]:
    """Summarize one signal on the supplied decided, finite-return cohort.

    Numeric rank IC is a descriptive pooled association, not an out-of-sample
    estimate or a probability. ``spearman_vs_roi`` is a rounded float when
    ``spearman_status`` is ``defined``; otherwise it is None (JSON null) with
    status ``insufficient_samples``, ``constant_signal``, ``constant_roi``, or
    ``constant_signal_and_roi``. Finite numeric scores retain their paired ROI.

    v1.3.0: a numeric signal without ``edges`` is cut into quintiles per formation
    date (``bands`` = ``date_quintiles``; rows on a day with fewer than five values
    are left out and counted in ``rows_without_date_quintile``). ``formation``
    is the day's whole cross-section (formation_records: every record made, decided
    or not); a cohort takes the quintile its Key holds there, so a same-day record
    maturing later cannot move it (``quintile_universe`` = ``formation records``).
    Without ``formation`` the cohorts themselves are the cross-section
    (``decided rows only``) - right only when every record of the day is decided.
    When no day has five values the pooled cut is used and ``bands`` says
    ``pooled_quintiles (no formation day with 5 values)``. ``evidence`` names every
    key as descriptive or out_of_sample; ``verdict_basis`` says what the verdict used.
    """
    vals = [r.get(signal) for r in cohorts]
    nums = [_f(v) for v in vals]
    numeric = sum(1 for x in nums if x is not None) >= 0.9 * max(1, len(vals))
    ys = [1.0 if _s(r.get("Outcome")) == "WIN" else 0.0 for r in cohorts]
    rois = [_f(r.get("Realized ROI %")) or 0.0 for r in cohorts]
    entry_all, known_all = label_days(cohorts)
    bands, dropped = "categories", 0
    universe, universe_n = "decided rows only", 0
    if numeric:
        keep = [i for i, x in enumerate(nums) if x is not None]
        xs = [nums[i] for i in keep]
        dq: List[Optional[int]] = []
        if edges is None and _quintile_mode(quintiles) == "date" and formation is not None:
            # v1.3.0 (Codex P2 on #1086): the quintile of a row among ALL the records made the same day,
            # whatever became of them; a cohort takes the quintile its Key holds in that cross-section
            f_rows = [(r, _f(r.get(signal))) for r in formation]
            f_rows = [(r, x) for r, x in f_rows if x is not None]
            f_q = date_quintiles([x for _, x in f_rows], [_day(r.get("Date Recorded (Riyadh)")) for r, _ in f_rows])
            by_key = {_s(r.get("Key")): q for (r, _), q in zip(f_rows, f_q)}
            dq = [by_key.get(_s(cohorts[i].get("Key"))) for i in keep]
            universe, universe_n = "formation records", len(f_rows)
        elif edges is None and _quintile_mode(quintiles) == "date":
            # no formation set given: the cohorts are their own cross-section
            dq = date_quintiles(xs, [entry_all[i] for i in keep])
            universe_n = len(xs)
        if edges is None and any(q is not None for q in dq):
            bands = "date_quintiles"
            dropped = sum(1 for q in dq if q is None)
            groups = ["Q%d" % q for q in dq if q is not None]
            keep = [i for i, q in zip(keep, dq) if q is not None]
        elif edges is None:  # pooled quintiles (the v1.2.0 cut): asked for, or no day has five values
            bands = "pooled_quintiles" if _quintile_mode(quintiles) == "pooled" else \
                "pooled_quintiles (no formation day with %d values)" % QUINTILE_MIN_NAMES
            qs = sorted(xs)
            cuts = [qs[int(len(qs) * q)] for q in (0.2, 0.4, 0.6, 0.8)]
            label = lambda x: "Q%d" % (1 + sum(1 for c in cuts if x >= c))
            groups = [label(nums[i]) for i in keep]
        else:
            bands = "edges"
            e = sorted(edges)
            def label(x):
                for j, c in enumerate(e):
                    if x < c:
                        return f"<{c:g}" if j == 0 else f"{e[j-1]:g}-{c:g}"
                return f">={e[-1]:g}"
            groups = [label(nums[i]) for i in keep]
    else:
        keep = [i for i, v in enumerate(vals) if _s(v)]
        groups = [_s(vals[i]) for i in keep]
    y = [ys[i] for i in keep]
    roi = [rois[i] for i in keep]
    per: Dict[str, Dict[str, Any]] = {}
    for g, yy, rr in zip(groups, y, roi):
        d = per.setdefault(g, {"n": 0, "wins": 0.0, "rois": []})
        d["n"] += 1
        d["wins"] += yy
        d["rois"].append(rr)
    table = []
    for g in sorted(per, key=lambda k: (-per[k]["n"] if not numeric else 0, k)):
        d = per[g]
        table.append({"group": g, "n": d["n"], "win_pct": round(100 * d["wins"] / d["n"], 1),
                      "mean_roi_pct": round(statistics.mean(d["rois"]), 2),
                      "median_roi_pct": round(statistics.median(d["rois"]), 2)})
    base = sum(y) / max(1, len(y))
    base_brier = brier([(base, yy) for yy in y])
    res: Dict[str, Any] = {"signal": signal, "type": "numeric" if numeric else "categorical", "n": len(y),
                           "base_win_pct": round(100 * base, 1), "base_brier": round(base_brier, 4), "groups": table}
    if numeric:
        xs = [nums[i] for i in keep]
        if xs and 0 <= min(xs) and max(xs) <= 100:
            res["raw_brier_as_probability"] = round(brier([(x / 100.0, yy) for x, yy in zip(xs, y)]), 4)
        ic = spearman(xs, roi)
        res["spearman_vs_roi"] = round(ic, 3) if math.isfinite(ic) else None
        if len(xs) < 3:
            res["spearman_status"] = "insufficient_samples"
        elif len(set(xs)) == 1 and len(set(roi)) == 1:
            res["spearman_status"] = "constant_signal_and_roi"
        elif len(set(xs)) == 1:
            res["spearman_status"] = "constant_signal"
        elif len(set(roi)) == 1:
            res["spearman_status"] = "constant_roi"
        else:
            res["spearman_status"] = "defined"
    res["cv_brier_group_calibrated"] = round(cv_brier(groups, y), 4)
    big = [t for t in table if t["n"] >= min_n]
    spread, z = 0.0, 0.0
    if len(big) >= 2:
        hi = max(big, key=lambda t: t["win_pct"]); lo = min(big, key=lambda t: t["win_pct"])
        spread = hi["win_pct"] - lo["win_pct"]
        p = base
        se = math.sqrt(max(1e-9, p * (1 - p) * (1.0 / hi["n"] + 1.0 / lo["n"])))
        z = (spread / 100.0) / se
    res["win_spread_pp"] = round(spread, 1)
    res["spread_z"] = round(z, 2)
    gain = base_brier - res["cv_brier_group_calibrated"]
    res["cv_gain_vs_base"] = round(gain, 4)
    # v1.2.0: purged walk-forward on the same cohort; pooled quintile cuts refitted per fold.
    # v1.3.0: a date quintile needs no fitting - it reads its own day only.
    entry = [entry_all[i] for i in keep]
    known = [known_all[i] for i in keep]
    if numeric and edges is None and bands != "date_quintiles":
        xk = [nums[i] for i in keep]

        def labeler_for(train: List[int]):
            qs = sorted(xk[i] for i in train)
            cuts = [qs[int(len(qs) * q)] for q in (0.2, 0.4, 0.6, 0.8)]
            return lambda i: 1 + sum(1 for c in cuts if xk[i] >= c)
    else:
        def labeler_for(train: List[int]):
            return lambda i: groups[i]
    res.update(wf_brier(y, entry, known, labeler_for, embargo_days=embargo_days))
    res["cv_mode"] = _cv_mode(cv)
    # SEPARATES: out-of-sample Brier gain AND a spread that is not chance (z >= 3);
    # WEAK: one of the two; NONE: neither. Chance spreads across quintiles never reach z >= 3.
    # v1.2.0: the gain is the walk-forward gain (None -> no gain); legacy = the random K-fold.
    if res["cv_mode"] == "legacy":
        strong_gain = gain >= 0.002
    else:
        strong_gain = res["wf_status"] == "ok" and res["wf_gain_vs_base"] >= 0.002
    strong_spread = z >= 3.0 and spread >= 5.0
    res["verdict"] = "SEPARATES" if (strong_gain and strong_spread) else ("WEAK" if (strong_gain or strong_spread) else "NONE")
    # v1.3.0: what each number is, and what the verdict stands on
    res["bands"] = bands
    if bands == "date_quintiles":
        res["rows_without_date_quintile"] = dropped
        res["quintile_universe"] = universe
        res["formation_rows"] = universe_n
    res["verdict_basis"] = ("legacy random K-fold gain and win spread - both descriptive" if res["cv_mode"] == "legacy"
                            else "walk-forward gain (out-of-sample) and win spread (descriptive)")
    res["evidence"] = {"descriptive": [k for k in DESCRIPTIVE_KEYS if k in res],
                       "out_of_sample": [k for k in OUT_OF_SAMPLE_KEYS if k in res]}
    return res


def render(results: List[Dict[str, Any]], title: str) -> str:
    lines = [f"TFB BACKTEST v{VERSION} — {title}",
             "  [descriptive] = rows already seen, never evidence on its own | [out-of-sample] = purged walk-forward"]
    for r in results:
        lines.append(f"\n[{r['verdict']:9s}] {r['signal']} ({r['type']}, n={r['n']}, base win {r['base_win_pct']}%, base Brier {r['base_brier']})")
        if "raw_brier_as_probability" in r:
            lines.append(f"    [descriptive] raw value as probability: Brier {r['raw_brier_as_probability']}")
        if "spearman_vs_roi" in r:
            ic_text = str(r["spearman_vs_roi"]) if r["spearman_vs_roi"] is not None else f"undefined ({r['spearman_status']})"
            lines.append(f"    [descriptive] Spearman vs ROI {ic_text}")
        if r.get("wf_status") == "ok":
            wf_txt = (f"Walk-forward Brier {r['wf_brier']} vs base {r['wf_base_brier']} "
                      f"(gain {r['wf_gain_vs_base']:+.4f}, {r['wf_scored']} rows, {r['wf_folds']} folds, "
                      f"smallest train-to-test gap {r.get('wf_min_gap_days')} d)")
        else:
            wf_txt = f"Walk-forward Brier undefined ({r.get('wf_status', 'n/a')})"
        lines.append(f"    [out-of-sample] {wf_txt}")
        lines.append(f"    [descriptive] win spread {r['win_spread_pp']} pp (z={r['spread_z']}) | verdict basis: "
                     f"{r.get('verdict_basis', r.get('cv_mode', 'wf'))}")
        lines.append(f"    [descriptive] legacy random K-fold Brier {r['cv_brier_group_calibrated']} (gain {r['cv_gain_vs_base']:+.4f}; leaks time, diagnostic only)")
        band_txt = r.get("bands", "")
        if band_txt == "date_quintiles":
            band_txt = ("quintiles per formation date, cut among %d %s; %d row(s) on thin or undated days left out"
                        % (r.get("formation_rows", 0), r.get("quintile_universe", "decided rows only"),
                           r.get("rows_without_date_quintile", 0)))
        lines.append(f"    [descriptive] groups: {band_txt}")
        lines.append(f"    {'group':14s}{'n':>6s}{'win%':>7s}{'meanROI%':>10s}{'medROI%':>9s}")
        for t in r["groups"][:12]:
            lines.append(f"    {t['group'][:14]:14s}{t['n']:6d}{t['win_pct']:7.1f}{t['mean_roi_pct']:10.2f}{t['median_roi_pct']:9.2f}")
    return "\n".join(lines)


def _selftest() -> int:
    random.seed(1)
    recs = []
    for i in range(3000):
        sig = random.uniform(0, 100)            # informative signal: P(win) rises with it
        noise = random.uniform(0, 100)          # uninformative
        win = random.random() < 0.35 + 0.5 * (sig / 100.0)
        recs.append({"Key": f"K{i}", "Status": "matured", "Outcome": "WIN" if win else "LOSS",
                     "Realized ROI %": str(round((random.gauss(2, 6) if win else random.gauss(-3, 5)), 2)),
                     "Good": str(round(sig, 1)), "Noise": str(round(noise, 1)),
                     "Cat": "A" if sig > 60 else "B", "Horizon": "1W"})
    recs.append(dict(recs[0], Key="K0"))        # duplicate key must be dropped
    coh = decided_cohorts(recs)
    assert len(coh) == 3000, len(coh)
    assert len(decided_cohorts(recs, filters={"Cat": "A"})) == sum(1 for r in recs[:3000] if r["Cat"] == "A")
    for r in recs[:1500]:
        r["Date Recorded (Riyadh)"] = "2026-07-15 09:00:00"
    for r in recs[1500:]:
        r["Date Recorded (Riyadh)"] = "2026-08-20 09:00:00"
    assert len(decided_cohorts(recs, since="2026-08-01")) == 1500 and len(decided_cohorts(recs, until="2026-07-31")) == 1500
    # v1.2.0: spread the 3,000 cohorts over 60 entry days (1W horizon, known 7 + 3 days later) for the walk-forward.
    d0 = _dt.date(2026, 7, 1)
    for i, r in enumerate(recs[:3000]):
        r["Date Recorded (Riyadh)"] = (d0 + _dt.timedelta(days=i // 50)).isoformat() + " 09:00:00"
    coh = decided_cohorts(recs)
    good = evaluate_signal(coh, "Good", edges=[50, 70, 85]); noise = evaluate_signal(coh, "Noise")
    cat = evaluate_signal(coh, "Cat")
    assert good["verdict"] == "SEPARATES" and good["win_spread_pp"] >= 20, good
    assert noise["verdict"] == "NONE", noise["verdict"]
    assert cat["verdict"] == "SEPARATES" and cat["type"] == "categorical", cat
    assert good["raw_brier_as_probability"] < noise["raw_brier_as_probability"]
    assert good["wf_status"] == "ok" and good["wf_folds"] >= 4 and good["wf_gain_vs_base"] > 0.002, good
    # The leak the audit named: cohorts recorded on the same day share that day's market move. A label that
    # only identifies the entry day "separates" under the random K-fold (the day's other cohorts sit in the
    # training folds) but carries nothing for a day not yet seen, so the walk-forward gives it no gain.
    rng = random.Random(3)
    leak = []
    for i in range(3000):
        day = d0 + _dt.timedelta(days=i // 50)
        p_win = 0.8 if (i // 50) % 2 == 0 else 0.3
        leak.append({"Key": f"L{i}", "Status": "matured", "Outcome": "WIN" if rng.random() < p_win else "LOSS",
                     "Realized ROI %": "1", "Horizon": "1W", "Day": day.isoformat(),
                     "Date Recorded (Riyadh)": day.isoformat() + " 09:00:00"})
    lk_wf, lk_legacy = evaluate_signal(leak, "Day"), evaluate_signal(leak, "Day", cv="legacy")
    assert lk_legacy["cv_gain_vs_base"] >= 0.02 and lk_legacy["verdict"] == "WEAK", lk_legacy["verdict"]
    assert lk_wf["wf_status"] == "ok" and lk_wf["wf_gain_vs_base"] < 0.002 and lk_wf["verdict"] == "NONE", lk_wf["verdict"]
    # v1.3.0: a quintile is cut inside its own formation day. A score whose LEVEL follows the market (high on the
    # good days, low on the bad ones) and says nothing about which name does better that day fills the pooled
    # quintiles with whole days: a spread that is the market's, not the choice's. Cut per day it shows none.
    rng = random.Random(11)
    lvl = []
    for i in range(3000):
        day = d0 + _dt.timedelta(days=i // 50)
        up = (i // 50) % 2 == 0
        lvl.append({"Key": f"V{i}", "Status": "matured", "Outcome": "WIN" if rng.random() < (0.8 if up else 0.3) else "LOSS",
                    "Realized ROI %": "1", "Horizon": "1W", "Level": str(round((70 if up else 30) + rng.uniform(-10, 10), 2)),
                    "Date Recorded (Riyadh)": day.isoformat() + " 09:00:00"})
    lv_date, lv_pool = evaluate_signal(lvl, "Level"), evaluate_signal(lvl, "Level", quintiles="pooled")
    assert lv_pool["bands"] == "pooled_quintiles" and lv_pool["win_spread_pp"] >= 30 and lv_pool["verdict"] != "NONE", lv_pool
    assert lv_date["bands"] == "date_quintiles" and lv_date["win_spread_pp"] < 8 and lv_date["verdict"] == "NONE", lv_date
    assert noise["bands"] == "date_quintiles" and good["bands"] == "edges" and cat["bands"] == "categories"
    # Codex P2 on #1086: a day's quintiles are cut among every record made that day, not only the ones decided so
    # far. Ten records a day, scores 1..10; only the five lowest have matured, and of those only score 5 wins.
    # Among all ten, score 5 is the middle (Q3); among the decided five it would be the top (Q5).
    half = []
    for d in range(40):
        day = (d0 + _dt.timedelta(days=d)).isoformat()
        for v in range(1, 11):
            done = v <= 5
            half.append({"Key": f"H{d}-{v}", "Status": "matured" if done else "open",
                         "Outcome": ("WIN" if v == 5 else "LOSS") if done else "", "Realized ROI %": "1" if done else "",
                         "Horizon": "1W", "Score": str(v), "Date Recorded (Riyadh)": day + " 09:00:00"})
    hc, hf = decided_cohorts(half), formation_records(half)
    assert len(hc) == 200 and len(hf) == 400
    top = lambda res: [g["group"] for g in res["groups"] if g["win_pct"] == 100.0]
    with_f, without_f = evaluate_signal(hc, "Score", formation=hf), evaluate_signal(hc, "Score")
    assert top(with_f) == ["Q3"] and with_f["quintile_universe"] == "formation records" and with_f["formation_rows"] == 400
    assert top(without_f) == ["Q5"] and without_f["quintile_universe"] == "decided rows only"
    assert good["wf_min_gap_days"] >= 7 + AVAILABILITY_PAD_DAYS + 1, good["wf_min_gap_days"]   # horizon + pad + embargo
    assert good["evidence"]["out_of_sample"] == ["wf_brier", "wf_base_brier", "wf_gain_vs_base"]
    assert "groups" in good["evidence"]["descriptive"] and "cv_gain_vs_base" in good["evidence"]["descriptive"]
    print(render([good, noise, cat], "selftest"))
    print("selftest: PASS 10/10 (informative numeric SEPARATES, noise NONE, categorical SEPARATES; duplicate key dropped; "
          "filter/since/until; walk-forward >= 4 folds; an entry-day label gains under the legacy K-fold, not walk-forward; "
          "a market-level score separates only under pooled quintiles, not per formation date; a day's quintiles are cut "
          "among every record made that day, decided or not; the train-to-test gap covers the horizon; every output is "
          "named descriptive or out-of-sample)")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Cohort-outcome backtester for hypothesis-registry items (read-only).")
    ap.add_argument("--export-dir", default="")
    ap.add_argument("--xlsx", default="")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--sheet-id", default="")
    ap.add_argument("--signal", action="append", default=[])
    ap.add_argument("--all-signals", action="store_true")
    ap.add_argument("--edges", default="")
    ap.add_argument("--horizon", default="")
    ap.add_argument("--filter", action="append", default=[], help='COL=VALUE exact match, repeatable (v1.1.0)')
    ap.add_argument("--since", default="", help="Date Recorded >= YYYY-MM-DD (v1.1.0)")
    ap.add_argument("--until", default="", help="Date Recorded <= YYYY-MM-DD (v1.1.0)")
    ap.add_argument("--min-n", type=int, default=100)
    ap.add_argument("--cv", default="", choices=["", "wf", "legacy"], help="verdict basis (v1.2.0; default wf)")
    ap.add_argument("--embargo-days", type=int, default=1, help="walk-forward embargo after a known outcome (v1.2.0)")
    ap.add_argument("--quintiles", default="", choices=["", "date", "pooled"],
                    help="numeric bands without --edges: per formation date (default) or pooled (v1.3.0)")
    ap.add_argument("--json", default="")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args(argv)
    if a.selftest:
        return _selftest()
    rows: List[List[str]] = []
    if a.live:
        sid = a.sheet_id or _s(os.getenv("DEFAULT_SPREADSHEET_ID")) or _s(os.getenv("SPREADSHEET_ID"))
        rows = _rows_live(sid)
        title = f"live …{sid[-6:]}"
    else:
        d = a.export_dir
        tsv = next((os.path.join(d, fn) for fn in sorted(os.listdir(d)) if fn.endswith(".tsv") and "Performance_Log" in fn), "") if d else ""
        xl = a.xlsx or (next((os.path.join(d, fn) for fn in sorted(os.listdir(d)) if fn.lower().endswith(".xlsx")), "") if d else "")
        if tsv:
            rows = _rows_from_tsv(tsv); title = os.path.basename(tsv)
        elif xl:
            rows = _rows_from_xlsx(xl); title = os.path.basename(xl)
        else:
            ap.error("no Performance_Log TSV / xlsx found; use --live, --export-dir or --xlsx")
    hdr, recs = load_records(rows)
    if not recs:
        print("FATAL: Performance_Log header row not found / no records", file=sys.stderr)
        return 2
    hz = [h.strip() for h in a.horizon.split(",") if h.strip()] or None
    flt = {}
    for item in a.filter:
        if "=" in item:
            k, v = item.split("=", 1)
            flt[k.strip()] = v.strip()
    coh = decided_cohorts(recs, hz, flt or None, a.since.strip(), a.until.strip())
    # v1.3.0: the day's whole cross-section, decided or not - what a date quintile is cut from
    formed = formation_records(recs, hz, flt or None, a.since.strip(), a.until.strip())
    signals = a.signal or (DEFAULT_SIGNALS if a.all_signals else ["Entry Forecast Reliability"])
    edges = [float(x) for x in a.edges.split(",") if x.strip()] or None
    results = [evaluate_signal(coh, sg, edges=edges if sg == signals[0] and edges else None, min_n=a.min_n,
                               cv=a.cv, embargo_days=a.embargo_days, quintiles=a.quintiles, formation=formed)
               for sg in signals if sg in hdr]
    missing = [sg for sg in signals if sg not in hdr]
    scope = (f" | horizons {','.join(hz)}" if hz else "") + (f" | filter {flt}" if flt else "") + \
            (f" | since {a.since}" if a.since else "") + (f" | until {a.until}" if a.until else "")
    print(render(results, f"{title} | decided cohorts n={len(coh)} of {len(formed)} formation records{scope}"))
    if missing:
        print("signals not in header:", ", ".join(missing))
    if a.json:
        os.makedirs(os.path.dirname(os.path.abspath(a.json)), exist_ok=True)
        with open(a.json, "w", encoding="utf-8") as fh:
            json.dump({"version": VERSION, "title": title, "n": len(coh), "formation_records": len(formed),
                       "output_legend": OUTPUT_LEGEND,
                       "results": results, "missing": missing}, fh, indent=2, ensure_ascii=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
