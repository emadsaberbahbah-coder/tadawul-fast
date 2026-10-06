#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tfb_backtest.py — v1.2.0 (2026-10-06)
================================================================================
WHY: the Strategy's rule is "register a hypothesis, backtest it, then change a
weight or gate". H-28 (stated reliability predicts outcomes) was rejected by an
ad-hoc computation; this makes that computation a repeatable instrument so any
column of Performance_Log can be tested the same way (H-29: which signal DOES
separate winners), by either AI, on the same export, with the same numbers.

WHAT IT DOES
  - loads Performance_Log (browser TSV export, xlsx, or --live), selects matured
    WIN/LOSS cohorts and retains duplicate-source evidence before descriptive
    first-occurrence deduplication;
  - tests whole daily cohorts chronologically with expanding training history;
    purges labels unavailable before each test-day boundary and optional gap;
  - fits numeric cuts, group calibration and baseline rates on training rows
    only; model/base Brier use identical heldout rows and expose feature coverage;
  - primary verdict GAIN / NO_GAIN requires enough observed training/heldout
    features and valid ledger provenance as of one UTC evaluation time. Unknown,
    duplicate, future-dated or insufficient evidence yields PENDING;
  - in-sample groups, Spearman, spread, historical randomized-fold CV and its
    SEPARATES / WEAK / NONE labels remain explicitly exploratory. Brier gain
    is descriptive and cannot establish significance or authorize a gate.

USAGE
  python scripts/tfb_backtest.py --export-dir DIR [--xlsx F] --signal "Entry Forecast Reliability" --signal Confidence ...
  python scripts/tfb_backtest.py --live --signal ... (env: DEFAULT_SPREADSHEET_ID + Google creds)
  python scripts/tfb_backtest.py --export-dir DIR --all-signals      # every Entry*/Horizon/Origin column
  python scripts/tfb_backtest.py --selftest
Options: --edges "50,70,85" (numeric bands) --min-n 100 --json out.json --horizon 1W,2W,1M
         --min-train 20 --min-test-days 2 --gap-days 0 --as-of-utc ISO_TIMESTAMP
         --filter "Origin Tab=Top_10_Investments" (repeatable, exact match)   [v1.1.0]
         --since 2026-08-01 / --until 2026-08-31  (Date Recorded window)       [v1.1.0]
Read-only source access; optional local JSON export, no sheet mutations.

v1.2.0 (2026-10-06): daily expanding validation purges unavailable training
labels, fits transformations/calibration on training rows and compares a
training-only baseline on identical heldout rows. Unknown ledger provenance
or insufficient history yields PENDING. Compatibility in-sample statistics
and the randomized-fold diagnostic remain explicitly exploratory.
v1.1.1: Spearman uses Pearson correlation of averaged ranks, including ties;
constant/nonfinite/short inputs are explicitly undefined.
"""
from __future__ import annotations

import argparse
import base64
from collections import Counter
import csv
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
import math
import os
import random
import statistics
import sys
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

# Keep the direct script invocation using the same clock bound as refresh audits.
if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.data_validity import MAX_CLOCK_SKEW_SECONDS

VERSION = "1.2.0"
_RIYADH = ZoneInfo("Asia/Riyadh")
DEFAULT_SIGNALS = ["Entry Forecast Reliability", "Entry Score", "Confidence", "Entry Investability",
                   "Entry Recommendation", "Entry Risk Bucket", "Horizon", "Origin Tab"]
_ENTRY_FIELDS = set(DEFAULT_SIGNALS) | {
    "Risk Bucket", "Entry Price", "Entry Data Quality", "Entry Final Action",
    "Entry Selected", "Target Price", "Target ROI %", "Symbol",
}
_NUMERIC_ENTRY_FIELDS = {
    "Entry Score", "Entry Forecast Reliability", "Entry Data Quality",
    "Entry Price", "Target Price", "Target ROI %",
}


class _SelectedCohorts(list):
    """Legacy descriptive selection retaining ambiguous source identities."""

    def __init__(self, rows, duplicate_keys=()):
        super().__init__(rows)
        self.duplicate_keys = frozenset(duplicate_keys)


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
    key_counts = Counter(_s(row.get("Key")) for row in records)
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
    # Descriptive compatibility keeps first occurrence, while primary
    # validation must not certify a cohort whose source identity is ambiguous.
    return _SelectedCohorts(out, (_s(row.get("Key")) for row in out
                                 if key_counts[_s(row.get("Key"))] > 1))


# --------------------------------------------------------------------------- #
# statistics (pure)                                                           #
# --------------------------------------------------------------------------- #
def brier(pairs: List[Tuple[float, float]]) -> float:
    return sum((p - y) ** 2 for p, y in pairs) / len(pairs) if pairs else float("nan")


def spearman(a: List[float], b: List[float]) -> float:
    """Tie-correct rank correlation; undefined results propagate as NaN.

    Require at least three paired finite observations. Do not silently drop
    invalid observations, which would change the evaluated cohort.
    """
    n = len(a)
    if n != len(b) or n < 3:
        return float("nan")
    try:
        a = [float(value) for value in a]
        b = [float(value) for value in b]
    except (TypeError, ValueError, OverflowError):
        return float("nan")
    if not all(math.isfinite(value) for value in a + b):
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
    mean_a, mean_b = statistics.mean(ra), statistics.mean(rb)
    centered_a = [value - mean_a for value in ra]
    centered_b = [value - mean_b for value in rb]
    variance_a = math.fsum(value * value for value in centered_a)
    variance_b = math.fsum(value * value for value in centered_b)
    if variance_a == 0 or variance_b == 0:
        return float("nan")
    covariance = math.fsum(x * y for x, y in zip(centered_a, centered_b))
    return max(-1.0, min(1.0, covariance / math.sqrt(variance_a * variance_b)))


def cv_brier(groups: List[Any], ys: List[float], k: int = 5, shrink: float = 20.0, seed: int = 7) -> float:
    """Legacy randomized-fold diagnostic; exploratory, not chronological skill."""
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


def _ledger_timestamp(value: Any, *, availability: bool = False) -> Optional[datetime]:
    """Parse ledger time without inventing dates; naive values are Riyadh time.

    For date-only availability use the end of that day as a conservative upper
    bound. Test days always begin at midnight, before any same-day decision.
    """
    text = _s(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if availability:
            try:
                day = date.fromisoformat(text)
            except ValueError:
                pass
            else:
                parsed = datetime.combine(day, time.max)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_RIYADH)
        return parsed.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        return None


def _evaluation_as_of(as_of: Optional[datetime]) -> datetime:
    if as_of is None:
        return datetime.now(timezone.utc)
    if not isinstance(as_of, datetime) or as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("evaluation as_of must be a timezone-aware datetime")
    return as_of.astimezone(timezone.utc)


def walk_forward_brier(
    cohorts: List[Dict[str, str]], signal: str, edges: Optional[List[float]] = None,
    *, min_train: int = 20, min_test_days: int = 2, gap_days: int = 0,
    shrink: float = 20.0, as_of: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Expanding daily tests, purged by recorded label-availability upper bounds.

    Model and constant training-base predictions use exactly the same heldout
    observations. No test feature or label fits bins or group calibration.
    Missing/invalid ledger provenance blocks a skill verdict even when a
    usable subset permits descriptive heldout metrics.
    """
    if (any(isinstance(value, bool) or not isinstance(value, int)
            for value in (min_train, min_test_days, gap_days))
            or min_train < 1 or min_test_days < 1 or gap_days < 0
            or isinstance(shrink, bool) or not isinstance(shrink, (int, float))
            or not math.isfinite(shrink) or shrink < 0):
        raise ValueError("invalid walk-forward limits")
    if edges is not None and (not edges or any(
            isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
            for value in edges)):
        raise ValueError("numeric edges must be finite and nonempty")

    as_of = _evaluation_as_of(as_of)
    future_bound = as_of + timedelta(seconds=MAX_CLOCK_SKEW_SECONDS)
    key_counts = Counter(_s(row.get("Key")) for row in cohorts)
    source_duplicates = getattr(cohorts, "duplicate_keys", frozenset())
    excluded: Counter = Counter()
    observations = []
    excluded_rows = 0
    for row in cohorts:
        reasons = []
        key, record_id, symbol = (_s(row.get(name)) for name in ("Key", "Record ID", "Symbol"))
        if not key or not record_id or not symbol:
            reasons.append("identity_unknown")
        elif key_counts[key] > 1 or key in source_duplicates:
            reasons.append("duplicate_cohort")
        decision = _ledger_timestamp(row.get("Date Recorded (Riyadh)"))
        target = _ledger_timestamp(row.get("Target Date (Riyadh)"), availability=True)
        maturity = _ledger_timestamp(row.get("Maturity Date"), availability=True)
        updated = _ledger_timestamp(row.get("Last Updated (Riyadh)"), availability=True)
        target_lower = _ledger_timestamp(row.get("Target Date (Riyadh)"))
        maturity_lower = _ledger_timestamp(row.get("Maturity Date"))
        updated_lower = _ledger_timestamp(row.get("Last Updated (Riyadh)"))
        if any(value is None for value in (decision, target, maturity, updated)):
            reasons.append("label_provenance_unknown")
        # A date-only field is an interval, not an exact midnight/day-end
        # event. Reject only contradictions provable from its bounds; retain
        # the upper bound separately for conservative training availability.
        elif (target <= decision or maturity < max(decision, target_lower)
              or updated < max(decision, target_lower, maturity_lower)):
            reasons.append("invalid_label_chronology")
        # Date-only availability ends at day end for train purging, but that
        # upper bound cannot prove a same-day event occurred in the future.
        if any(value is not None and value > future_bound
               for value in (decision, target_lower, maturity_lower, updated_lower)):
            reasons.append("future_label_provenance")
        outcome, roi = _s(row.get("Outcome")), _f(row.get("Realized ROI %"))
        if _s(row.get("Status")).lower() != "matured":
            reasons.append("outcome_unresolved")
        if (outcome not in ("WIN", "LOSS") or roi is None
                or (outcome == "WIN" and roi <= 0) or (outcome == "LOSS" and roi >= 0)):
            reasons.append("outcome_invalid")
        if reasons:
            excluded.update(reasons)
            excluded_rows += 1
            continue
        observations.append({
            "key": key, "record_id": record_id, "symbol": symbol,
            "decision_day": decision.astimezone(_RIYADH).date(),
            "available_at": max(target, maturity, updated),
            "value": _s(row.get(signal)), "number": _f(row.get(signal)),
            "label": 1.0 if outcome == "WIN" else 0.0,
        })
    observations.sort(key=lambda row: (row["decision_day"], row["key"], row["record_id"]))
    days = sorted({row["decision_day"] for row in observations})
    folds, predictions = [], []
    warmup_rows = 0
    feature_warmup_rows = 0
    for day in days:
        test_start = datetime.combine(day, time.min, _RIYADH).astimezone(timezone.utc)
        cutoff = test_start - timedelta(days=gap_days)
        past = [row for row in observations if row["decision_day"] < day]
        train = [row for row in past if row["available_at"] < cutoff]
        test = [row for row in observations if row["decision_day"] == day]
        if len(train) < min_train:
            warmup_rows += len(test)
            continue
        base = math.fsum(row["label"] for row in train) / len(train)
        numeric_count = sum(row["number"] is not None for row in train)
        nonempty_count = sum(bool(row["value"]) for row in train)
        numeric = (signal in _NUMERIC_ENTRY_FIELDS or edges is not None
                   or (nonempty_count > 0 and numeric_count >= 0.9 * nonempty_count))
        observed_train = [row for row in train if
                          (row["number"] is not None if numeric else bool(row["value"]))]
        if len(observed_train) < min_train:
            warmup_rows += len(test)
            feature_warmup_rows += len(test)
            continue
        cuts = None
        if numeric:
            numbers = sorted(row["number"] for row in train if row["number"] is not None)
            cuts = sorted(edges) if edges is not None else [
                numbers[min(len(numbers) - 1, int(len(numbers) * quantile))]
                for quantile in (0.2, 0.4, 0.6, 0.8)
            ]

        def group(row):
            if numeric:
                if row["number"] is None:
                    return None
                return sum(row["number"] >= cut for cut in cuts)
            return row["value"] or None

        aggregates: Dict[Any, List[float]] = {}
        for row in train:
            bucket = group(row)
            if bucket is None:
                continue
            counts = aggregates.setdefault(bucket, [0.0, 0.0])
            counts[0] += row["label"]
            counts[1] += 1
        for row in test:
            bucket = group(row)
            wins, count = aggregates.get(bucket, [0.0, 0.0])
            model = (wins + base * shrink) / (count + shrink) if count else base
            predictions.append({
                "key": row["key"], "record_id": row["record_id"], "symbol": row["symbol"],
                "decision_day": day.isoformat(), "label": row["label"],
                "model_probability": model, "baseline_probability": base,
                "feature_observed": bucket is not None,
                "baseline_fallback": not bool(count),
            })
        train_digest = hashlib.sha256(json.dumps(
            sorted(row["key"] for row in train), separators=(",", ":"),
        ).encode()).hexdigest()
        folds.append({
            "decision_day": day.isoformat(), "test_start_utc": test_start.isoformat(),
            "availability_cutoff_utc": cutoff.isoformat(), "train_count": len(train),
            "training_feature_observed_count": len(observed_train),
            "training_feature_missing_count": len(train) - len(observed_train),
            "purged_past_count": len(past) - len(train), "test_count": len(test),
            "latest_training_label_utc": max(row["available_at"] for row in train).isoformat(),
            "train_keys_sha256": train_digest, "feature_type": "numeric" if numeric else "categorical",
            "training_cuts": cuts, "training_base_probability": base,
            "test_feature_observed_count": sum(group(row) is not None for row in test),
        })

    model_brier = brier([(row["model_probability"], row["label"]) for row in predictions])
    baseline_brier = brier([(row["baseline_probability"], row["label"]) for row in predictions])
    pending = sorted(excluded)
    if signal not in _ENTRY_FIELDS:
        pending.append("feature_not_known_at_entry")
    if len(folds) < min_test_days:
        pending.append("insufficient_heldout_days")
    if not predictions:
        pending.append("insufficient_training_history")
        if feature_warmup_rows:
            pending.append("insufficient_training_feature_history")
    observed_predictions = [row for row in predictions if row["feature_observed"]]
    observed_days = len({row["decision_day"] for row in observed_predictions})
    if observed_days < min_test_days:
        pending.append("insufficient_observed_heldout_days")
    return {
        "status": "PENDING" if pending else "COMPLETE", "pending_reasons": pending,
        "method": "daily_expanding_purged_walk_forward", "gap_days": gap_days,
        "min_train": min_train, "min_test_days": min_test_days,
        "as_of_utc": as_of.isoformat(), "max_clock_skew_seconds": MAX_CLOCK_SKEW_SECONDS,
        "availability_policy": "max(target,maturity,last_updated) strictly before test-day start minus gap",
        "date_only_availability_policy": "Riyadh day end (conservative upper bound)",
        "valid_rows": len(observations), "excluded_rows": excluded_rows,
        "exclusions": dict(sorted(excluded.items())), "warmup_rows": warmup_rows,
        "feature_warmup_rows": feature_warmup_rows,
        "heldout_rows": len(predictions), "heldout_days": len(folds),
        "heldout_feature_observed_rows": len(observed_predictions),
        "heldout_feature_observed_days": observed_days,
        "heldout_feature_missing_rows": len(predictions) - len(observed_predictions),
        "heldout_baseline_fallback_rows": sum(row["baseline_fallback"] for row in predictions),
        "model_brier": model_brier if predictions else None,
        "baseline_brier": baseline_brier if predictions else None,
        "paired_brier_gain": baseline_brier - model_brier if predictions else None,
        "folds": folds, "predictions": predictions,
        "limitations": [
            "Ledger timestamps do not certify immutable historical revisions, complete CA/PIT coverage or external data freshness.",
            "Rows sharing an instrument/day/horizon can be dependent; Brier gain is descriptive, not a significance test or gate approval.",
        ],
    }


def evaluate_signal(cohorts: List[Dict[str, str]], signal: str, edges: Optional[List[float]] = None,
                    min_n: int = 100, *, min_train: int = 20,
                    min_test_days: int = 2, gap_days: int = 0,
                    as_of: Optional[datetime] = None) -> Dict[str, Any]:
    if isinstance(min_n, bool) or not isinstance(min_n, int) or min_n < 1:
        raise ValueError("minimum heldout rows must be a positive integer")
    # Preserve deterministic diagnostics and fold assignments on exported rows
    # whose order may differ between TSV, XLSX and in-memory callers.
    cohorts = _SelectedCohorts(sorted(cohorts, key=lambda row: (
        _s(row.get("Key")), _s(row.get("Record ID")), json.dumps(row, sort_keys=True),
    )), getattr(cohorts, "duplicate_keys", frozenset()))
    walk = walk_forward_brier(cohorts, signal, edges, min_train=min_train,
                             min_test_days=min_test_days, gap_days=gap_days, as_of=as_of)
    vals = [r.get(signal) for r in cohorts]
    nums = [_f(v) for v in vals]
    numeric = (signal in _NUMERIC_ENTRY_FIELDS or edges is not None
               or sum(1 for x in nums if x is not None) >= 0.9 * max(1, len(vals)))
    ys = [1.0 if _s(r.get("Outcome")) == "WIN" else 0.0 for r in cohorts]
    rois = [_f(r.get("Realized ROI %")) or 0.0 for r in cohorts]
    if numeric:
        keep = [i for i, x in enumerate(nums) if x is not None]
        xs = [nums[i] for i in keep]
        if edges is None and xs:  # descriptive whole-cohort quintiles
            qs = sorted(xs)
            cuts = [qs[int(len(qs) * q)] for q in (0.2, 0.4, 0.6, 0.8)]
            label = lambda x: "Q%d" % (1 + sum(1 for c in cuts if x >= c))
        elif edges is not None:
            e = sorted(edges)
            def label(x):
                for j, c in enumerate(e):
                    if x < c:
                        return f"<{c:g}" if j == 0 else f"{e[j-1]:g}-{c:g}"
                return f">={e[-1]:g}"
        else:
            label = lambda x: ""  # no observations
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
        correlation = spearman(xs, roi)
        res["spearman_vs_roi"] = round(correlation, 3) if math.isfinite(correlation) else None
    legacy_cv = cv_brier(groups, y)
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
    gain = base_brier - legacy_cv
    # Historical labels remain exploratory: randomized folds and selected
    # whole-cohort extremes cannot establish chronological model skill.
    strong_gain, strong_spread = gain >= 0.002, (z >= 3.0 and spread >= 5.0)
    res["exploratory_verdict"] = "SEPARATES" if (strong_gain and strong_spread) else ("WEAK" if (strong_gain or strong_spread) else "NONE")
    res["exploratory_random_fold_brier"] = round(legacy_cv, 4) if math.isfinite(legacy_cv) else None
    res["compatibility_fields_scope"] = {
        "base_win_pct": "in_sample_exploratory", "base_brier": "in_sample_exploratory",
        "groups": "in_sample_exploratory", "win_spread_pp": "in_sample_exploratory",
        "spread_z": "in_sample_exploratory", "raw_brier_as_probability": "in_sample_exploratory",
        "spearman_vs_roi": "in_sample_exploratory", "exploratory_verdict": "in_sample_exploratory",
        "cv_brier_group_calibrated": "purged_walk_forward_heldout_model",
        "cv_gain_vs_base": "paired_same_heldout_rows_training_only_base",
    }
    res["walk_forward"] = walk
    res["cv_brier_group_calibrated"] = None if walk["model_brier"] is None else round(walk["model_brier"], 4)
    res["cv_gain_vs_base"] = None if walk["paired_brier_gain"] is None else round(walk["paired_brier_gain"], 4)
    if walk["heldout_rows"] < min_n:
        walk["status"] = "PENDING"
        walk["pending_reasons"].append("insufficient_heldout_rows")
    if walk["heldout_feature_observed_rows"] < min_n:
        walk["status"] = "PENDING"
        walk["pending_reasons"].append("insufficient_observed_heldout_rows")
    res["gain_threshold_brier"] = 0.002
    res["verdict"] = "PENDING" if walk["status"] != "COMPLETE" else (
        "GAIN" if walk["paired_brier_gain"] >= res["gain_threshold_brier"] else "NO_GAIN")
    if not math.isfinite(base_brier):
        res["base_brier"] = None
    return res


def render(results: List[Dict[str, Any]], title: str) -> str:
    lines = [f"TFB BACKTEST v{VERSION} — {title}",
             "Scope: conservative ledger-time validation. Gain is descriptive; immutable revisions, complete CA/PIT coverage and statistical significance remain unverified."]
    for r in results:
        lines.append(f"\n[{r['verdict']:9s}] {r['signal']} (exploratory {r['type']}, n={r['n']}, in-sample base win {r['base_win_pct']}%, in-sample base Brier {r['base_brier']})")
        if "raw_brier_as_probability" in r:
            correlation = r["spearman_vs_roi"]
            correlation_label = "undefined" if correlation is None else str(correlation)
            lines.append(f"    raw value as probability: Brier {r['raw_brier_as_probability']} | Spearman vs ROI {correlation_label}")
        walk = r["walk_forward"]
        gain = "undefined" if walk["paired_brier_gain"] is None else f"{walk['paired_brier_gain']:+.4f}"
        lines.append(f"    Purged walk-forward: {walk['status']} | model Brier {r['cv_brier_group_calibrated']} vs training-base Brier {walk['baseline_brier']} | paired gain {gain} | heldout {walk['heldout_rows']} rows / {walk['heldout_days']} days")
        lines.append(f"    Heldout feature coverage: {walk['heldout_feature_observed_rows']} observed / {walk['heldout_rows']} rows; {walk['heldout_feature_missing_rows']} missing; {walk['heldout_baseline_fallback_rows']} baseline fallbacks")
        if walk["pending_reasons"]:
            lines.append("    Pending: " + ", ".join(walk["pending_reasons"]))
        lines.append(f"    Exploratory in-sample spread {r['win_spread_pp']} pp (z={r['spread_z']}); randomized CV {r['exploratory_random_fold_brier']}; label {r['exploratory_verdict']} (not a skill verdict)")
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
        roi = max(0.01, round(abs(random.gauss(2 if win else 3, 6 if win else 5)), 2))
        recs.append({"Key": f"K{i}", "Status": "matured", "Outcome": "WIN" if win else "LOSS",
                     "Realized ROI %": str(roi if win else -roi),
                     "Good": str(round(sig, 1)), "Noise": str(round(noise, 1)),
                     "Cat": "A" if sig > 60 else "B", "Horizon": "1W"})
    recs.append(dict(recs[0], Key="K0"))        # duplicate key must be dropped
    coh = decided_cohorts(recs)
    assert len(coh) == 3000, len(coh)
    assert coh.duplicate_keys == frozenset({"K0"})
    assert len(decided_cohorts(recs, filters={"Cat": "A"})) == sum(1 for r in recs[:3000] if r["Cat"] == "A")
    for i, r in enumerate(recs[:3000]):
        decision = datetime(2026, 7, 1, 9, tzinfo=_RIYADH) + timedelta(days=i // 50)
        available = decision + timedelta(days=7)
        r.update({"Record ID": f"R{i}", "Symbol": f"S{i}.US",
                  "Date Recorded (Riyadh)": decision.isoformat(),
                  "Target Date (Riyadh)": available.isoformat(),
                  "Maturity Date": available.isoformat(),
                  "Last Updated (Riyadh)": available.isoformat(),
                  "Entry Score": r["Good"], "Entry Forecast Reliability": r["Noise"],
                  "Confidence": r["Cat"]})
    assert len(decided_cohorts(recs, since="2026-07-31")) == 1500 and len(decided_cohorts(recs, until="2026-07-30")) == 1500
    # Validate the unique synthetic source separately from the deliberate
    # duplicate-selection fixture; ambiguity must block the actual evaluator.
    as_of = datetime(2026, 10, 6, tzinfo=timezone.utc)
    assert evaluate_signal(coh, "Entry Score", as_of=as_of)["verdict"] == "PENDING"
    coh = decided_cohorts(recs[:3000])
    good = evaluate_signal(coh, "Entry Score", edges=[50, 70, 85], as_of=as_of)
    noise = evaluate_signal(coh, "Entry Forecast Reliability", as_of=as_of)
    cat = evaluate_signal(coh, "Confidence", as_of=as_of)
    assert good["verdict"] == "GAIN" and good["win_spread_pp"] >= 20, good
    assert noise["verdict"] == "NO_GAIN", noise["verdict"]
    assert cat["verdict"] == "GAIN" and cat["type"] == "categorical", cat
    assert good["raw_brier_as_probability"] < noise["raw_brier_as_probability"]
    print(render([good, noise, cat], "selftest"))
    print("selftest: PASS 4/4 (informative numeric GAIN, noise NO_GAIN, categorical GAIN; duplicate-source PENDING; filter/since/until)")
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
    ap.add_argument("--min-n", type=int, default=100, help="Minimum heldout rows with observed signal (also exploratory group minimum)")
    ap.add_argument("--min-train", type=int, default=20, help="Minimum available training rows with observed signal per daily test")
    ap.add_argument("--min-test-days", type=int, default=2, help="Minimum heldout decision days with observed signal")
    ap.add_argument("--gap-days", type=int, default=0, help="Additional gap before each test-day boundary")
    ap.add_argument("--as-of-utc", default="", help="Evaluation cutoff: timezone-aware ISO timestamp (default: current UTC once per run)")
    ap.add_argument("--json", default="")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args(argv)
    if a.min_train < 1 or a.min_test_days < 1 or a.gap_days < 0 or a.min_n < 1:
        ap.error("sample limits must be positive and --gap-days nonnegative")
    if a.selftest:
        return _selftest()
    try:
        as_of = _evaluation_as_of(datetime.fromisoformat(a.as_of_utc.replace("Z", "+00:00"))
                                 if a.as_of_utc else None)
    except (ValueError, TypeError, OverflowError) as exc:
        ap.error(str(exc))
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
    signals = a.signal or (DEFAULT_SIGNALS if a.all_signals else ["Entry Forecast Reliability"])
    edges = [float(x) for x in a.edges.split(",") if x.strip()] or None
    results = [evaluate_signal(coh, sg, edges=edges if sg == signals[0] and edges else None,
                               min_n=a.min_n, min_train=a.min_train,
                               min_test_days=a.min_test_days, gap_days=a.gap_days, as_of=as_of)
               for sg in signals if sg in hdr]
    missing = [sg for sg in signals if sg not in hdr]
    scope = (f" | horizons {','.join(hz)}" if hz else "") + (f" | filter {flt}" if flt else "") + \
            (f" | since {a.since}" if a.since else "") + (f" | until {a.until}" if a.until else "")
    print(render(results, f"{title} | decided cohorts n={len(coh)}{scope}"))
    if missing:
        print("signals not in header:", ", ".join(missing))
    if a.json:
        os.makedirs(os.path.dirname(os.path.abspath(a.json)), exist_ok=True)
        with open(a.json, "w", encoding="utf-8") as fh:
            json.dump({"version": VERSION, "title": title, "n": len(coh), "as_of_utc": as_of.isoformat(),
                       "results": results, "missing": missing}, fh, indent=2, ensure_ascii=False, allow_nan=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
