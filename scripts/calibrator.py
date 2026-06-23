# -*- coding: utf-8 -*-
"""
scripts/calibrator.py — TFB Calibrator
======================================
Version: 1.0.0

WHY THIS SCRIPT EXISTS
----------------------
This is the only legitimate forecast-quality number the system will ever
produce. It does NOT assert forecast accuracy. It MEASURES calibration: when the
tool claimed "75% reliable", did that happen ~75% of the time?

It compares CLAIMED reliability (logged daily in Signal_History) against REALIZED
outcomes (logged per horizon in Performance_Log), and reports — per 1M / 3M / 12M
horizon — the Brier score and its decomposition, the Expected Calibration Error,
a reliability curve with Wilson confidence bands, a Spiegelhalter significance
test, and an isotonic recalibration map that can be fed back into scoring.

HONEST FRAMING (built into the output, not bolted on)
-----------------------------------------------------
* Calibration is NOT hit-rate. A tool can be perfectly calibrated about its own
  uncertainty (97% honest) without forecasting at 97%. Calibration — claimed
  matching realized — is the defensible quality metric for the forecast layer.
* Nothing is reported until a horizon's cohort has both MATURED (enough calendar
  time has passed) and reached a MINIMUM SAMPLE SIZE. Until then the horizon
  reads INSUFFICIENT_DATA with the date it first becomes measurable. The 1M
  cohort first matures ~2026-07-18; 12M ~2027.
* Directional claims only. HOLD / WATCH-type calls make no falsifiable
  directional probability and are excluded from the calibration set (reported).

ARCHITECTURE
------------
1. CONFIG  - the ONLY surface that depends on live sheet headers. Column names
             are alias lists resolved case-insensitively; confirm/adjust to the
             live Signal_History / Performance_Log header rows.
2. PURE    - parsing, direction classification, outcome derivation, the join, and
             CalibrationEngine are pure functions of plain rows. They use
             core.stats and need no Sheets access, so they are fully unit-tested.
3. I/O     - SheetReader mirrors track_performance.py's gspread PerformanceStore
             pattern (open_by_key -> worksheet -> .get), degrades gracefully when
             gspread/creds are absent, and is the single Sheets touch point.

Run:  python scripts/calibrator.py --as-of 2026-07-18 --dry-run
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
from typing import Any, Dict, List, Optional, Tuple

# --- core.stats (the verified primitive library) -----------------------------
try:
    from core import stats as st  # production location
except Exception:  # pragma: no cover - standalone/test fallback
    import stats as st  # type: ignore

# --- gspread (optional, mirrors track_performance.py) ------------------------
try:
    import gspread  # type: ignore
    from google.oauth2 import service_account  # type: ignore

    GSPREAD_AVAILABLE = True
except Exception:
    gspread = None  # type: ignore
    service_account = None  # type: ignore
    GSPREAD_AVAILABLE = False

__version__ = "1.0.0"

# =============================================================================
# 1) CONFIG  ===  CONFIRM AGAINST LIVE HEADERS
# =============================================================================
# Paste your live Signal_History / Performance_Log header rows and I will lock
# these alias lists to the exact columns. Resolution is case-insensitive and
# tolerant of surrounding spaces; the FIRST matching alias wins.

WORKBOOK_ID = "1QaOSNHqKlCaqlnaB45O4RlajWeMNe5cAE-r2HGwsG04"
SIGNAL_HISTORY_TAB = "Signal_History"
PERFORMANCE_LOG_TAB = "Performance_Log"
REPORT_TAB = "Calibration_Report"  # where a written summary would land (confirm)

# Signal_History: one claim per decision symbol per day.
SIGNAL_HISTORY_COLUMNS: Dict[str, List[str]] = {
    "symbol": ["symbol", "ticker"],
    "date": ["date", "snapshot_date", "as_of", "timestamp", "run_date"],
    "action": ["action", "recommendation", "signal", "call"],
    "claimed_reliability": ["reliability", "claimed_reliability", "confidence",
                            "reliability_pct", "confidence_pct", "claimed"],
    "horizon": ["horizon", "period"],  # OPTIONAL on the claim side
}
SIGNAL_HISTORY_REQUIRED = ["symbol", "date", "action", "claimed_reliability"]

# Performance_Log: realized outcome per (symbol, date, horizon).
PERFORMANCE_LOG_COLUMNS: Dict[str, List[str]] = {
    "symbol": ["symbol", "ticker"],
    "date": ["date", "entry_date", "snapshot_date", "as_of", "run_date"],
    "horizon": ["horizon", "period"],
    "realized_outcome": ["outcome", "hit", "realized_hit", "success", "correct",
                         "is_hit"],  # OPTIONAL pre-computed 0/1
    "realized_return": ["realized_return", "return", "return_pct", "forward_return",
                        "pnl_pct", "ret"],  # used if outcome not pre-computed
    "entry_price": ["entry_price", "price_at_entry", "entry"],  # last-resort
    "exit_price": ["exit_price", "current_price", "price_now", "exit"],  # derive ret
}
PERFORMANCE_LOG_REQUIRED = ["symbol", "date", "horizon"]

# Horizon maturity in CALENDAR days (used to decide whether a claim can be scored).
HORIZONS_DAYS: Dict[str, int] = {"1M": 30, "3M": 91, "12M": 365}

# A horizon is only reported once it has at least this many usable, matured pairs.
MIN_SAMPLE_PER_HORIZON = 50

# A directional move must exceed this (fractional) threshold to count as realized.
DIRECTIONAL_THRESHOLD = 0.0

# Action -> direction. Anything not listed is treated as neutral (excluded).
BULLISH_ACTIONS = {"BUY", "ADD", "INVEST", "ACCUMULATE", "STRONG_BUY", "STRONGBUY",
                   "OVERWEIGHT", "LONG", "ENTER"}
BEARISH_ACTIONS = {"SELL", "TRIM", "REDUCE", "EXIT", "UNDERWEIGHT", "STRONG_SELL",
                   "STRONGSELL", "SHORT", "BLOCK"}
NEUTRAL_ACTIONS = {"HOLD", "WATCH", "REVIEW", "IDEA", "NEUTRAL", "WAIT"}


# =============================================================================
# 2) PURE HELPERS  (no Sheets access -> unit-tested)
# =============================================================================

def _norm(s: Any) -> str:
    return str(s).strip().lower() if s is not None else ""


def resolve_header_map(header_row: List[Any], alias_map: Dict[str, List[str]],
                       required: List[str]) -> Dict[str, int]:
    """
    Map each logical field to a column index by case-insensitive alias match.
    Raises ValueError listing any REQUIRED field whose aliases are all absent —
    a loud, actionable failure rather than a silent wrong-column read.
    """
    norm_headers = {_norm(h): i for i, h in enumerate(header_row)}
    out: Dict[str, int] = {}
    for logical, aliases in alias_map.items():
        for alias in aliases:
            idx = norm_headers.get(_norm(alias))
            if idx is not None:
                out[logical] = idx
                break
    missing = [f for f in required if f not in out]
    if missing:
        raise ValueError(
            "Header resolution failed for required field(s) %s. "
            "Present headers: %s. Update the alias lists in CONFIG to match."
            % (missing, list(header_row))
        )
    return out


def parse_table(values: List[List[Any]], alias_map: Dict[str, List[str]],
                required: List[str]) -> List[Dict[str, Any]]:
    """Turn a raw [header_row, *data_rows] grid into a list of logical-field dicts."""
    if not values or len(values) < 2:
        return []
    hmap = resolve_header_map(values[0], alias_map, required)
    rows: List[Dict[str, Any]] = []
    for raw in values[1:]:
        rec: Dict[str, Any] = {}
        for logical, idx in hmap.items():
            rec[logical] = raw[idx] if idx < len(raw) else None
        rows.append(rec)
    return rows


def parse_date(v: Any) -> Optional[_dt.date]:
    """Parse the common date encodings Sheets emits; None if unparseable."""
    if v is None or v == "":
        return None
    if isinstance(v, _dt.datetime):
        return v.date()
    if isinstance(v, _dt.date):
        return v
    s = str(v).strip()
    # take the date portion of an ISO datetime
    s_date = s.split("T")[0].split(" ")[0]
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return _dt.datetime.strptime(s_date, fmt).date()
        except Exception:
            continue
    try:
        return _dt.date.fromisoformat(s_date)
    except Exception:
        return None


def normalize_reliability(v: Any, scale_hint: str = "auto") -> Optional[float]:
    """
    Coerce a claimed-reliability value to a probability in [0, 1]. Accepts 0-1,
    0-100, and '75%' strings. scale_hint: 'auto' | '0-1' | '0-100'.
    """
    if v is None or v == "":
        return None
    s = str(v).strip().rstrip("%").strip()
    try:
        x = float(s)
    except Exception:
        return None
    if scale_hint == "0-100" or (scale_hint == "auto" and x > 1.0):
        x = x / 100.0
    return min(max(x, 0.0), 1.0)


def classify_direction(action: Any) -> Optional[int]:
    """+1 bullish, -1 bearish, 0 neutral (excluded), None if unrecognised."""
    a = _norm(action).upper().replace(" ", "_").replace("-", "_")
    if a in BULLISH_ACTIONS:
        return 1
    if a in BEARISH_ACTIONS:
        return -1
    if a in NEUTRAL_ACTIONS:
        return 0
    return None


def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    s = str(v).strip().rstrip("%").replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def derive_outcome(perf: Dict[str, Any], direction: int,
                   threshold: float = DIRECTIONAL_THRESHOLD) -> Optional[int]:
    """
    Realized binary outcome (1 = the directional claim came true).

    Priority: (1) a pre-computed outcome/hit column; else (2) realized_return vs
    the claim direction; else (3) exit/entry price ratio. Neutral claims
    (direction == 0) are excluded upstream and never reach here.
    """
    # (1) pre-computed hit
    if "realized_outcome" in perf and perf["realized_outcome"] not in (None, ""):
        o = _to_float(perf["realized_outcome"])
        if o is not None:
            return 1 if o >= 0.5 else 0
        truthy = _norm(perf["realized_outcome"])
        if truthy in ("true", "yes", "hit", "win", "correct"):
            return 1
        if truthy in ("false", "no", "miss", "loss", "wrong"):
            return 0
    # (2) realized return
    ret = _to_float(perf.get("realized_return"))
    # (3) derive return from prices
    if ret is None:
        ep = _to_float(perf.get("entry_price"))
        xp = _to_float(perf.get("exit_price"))
        if ep and ep != 0 and xp is not None:
            ret = xp / ep - 1.0
    if ret is None:
        return None
    if direction > 0:
        return 1 if ret > threshold else 0
    if direction < 0:
        return 1 if ret < -threshold else 0
    return None


# =============================================================================
# 3) JOIN  (claims left-joined to realized outcomes, one-to-many over horizons)
# =============================================================================

def join_claims_outcomes(signal_rows: List[Dict[str, Any]],
                         perf_rows: List[Dict[str, Any]],
                         scale_hint: str = "auto") -> List[Dict[str, Any]]:
    """
    For each Signal_History claim, attach every matching Performance_Log realized
    outcome by (symbol, date[, horizon]). Returns joined records:
        {symbol, date, horizon, action, direction, claimed_prob,
         outcome (0/1 or None), matured_eligible(False placeholder)}
    Neutral and unrecognised-direction claims are dropped (counted by caller).
    """
    # index perf rows by (symbol, date) -> list of (horizon, perf_dict)
    perf_index: Dict[Tuple[str, Optional[_dt.date]], List[Tuple[str, Dict[str, Any]]]] = {}
    for p in perf_rows:
        sym = _norm(p.get("symbol")).upper()
        d = parse_date(p.get("date"))
        hz = str(p.get("horizon")).strip() if p.get("horizon") not in (None, "") else None
        perf_index.setdefault((sym, d), []).append((hz, p))

    joined: List[Dict[str, Any]] = []
    for c in signal_rows:
        sym = _norm(c.get("symbol")).upper()
        d = parse_date(c.get("date"))
        direction = classify_direction(c.get("action"))
        if direction is None or direction == 0:
            continue  # unrecognised or neutral -> not a directional probability
        claimed = normalize_reliability(c.get("claimed_reliability"), scale_hint)
        if claimed is None or d is None:
            continue
        claim_hz = str(c.get("horizon")).strip() if c.get("horizon") not in (None, "") else None
        matches = perf_index.get((sym, d), [])
        if not matches:
            continue
        for hz, p in matches:
            horizon = claim_hz or hz
            if horizon is None:
                continue
            if claim_hz is not None and hz is not None and claim_hz != hz:
                continue
            outcome = derive_outcome(p, direction)
            joined.append({
                "symbol": sym,
                "date": d,
                "horizon": horizon,
                "action": c.get("action"),
                "direction": direction,
                "claimed_prob": claimed,
                "outcome": outcome,
            })
    return joined


# =============================================================================
# 4) CALIBRATION ENGINE  (pure; uses core.stats)
# =============================================================================

class CalibrationEngine:
    """Turns joined (claim, outcome) records into a per-horizon calibration report."""

    def __init__(self, as_of: Optional[_dt.date] = None,
                 min_sample: int = MIN_SAMPLE_PER_HORIZON,
                 horizons_days: Optional[Dict[str, int]] = None):
        self.as_of = as_of or _dt.date.today()
        self.min_sample = int(min_sample)
        self.horizons_days = dict(horizons_days or HORIZONS_DAYS)

    def _matured(self, claim_date: _dt.date, horizon_days: int) -> bool:
        return (self.as_of - claim_date).days >= horizon_days

    def _next_measurable_date(self, records_for_h: List[Dict[str, Any]],
                              horizon_days: int) -> Optional[str]:
        """
        The date the (min_sample)-th claim for this horizon will have matured —
        i.e. the earliest the horizon could be reported. None if too few claims
        have ever been logged to reach the minimum.
        """
        mat_dates = sorted(r["date"] + _dt.timedelta(days=horizon_days)
                           for r in records_for_h)
        if len(mat_dates) < self.min_sample:
            return None
        return mat_dates[self.min_sample - 1].isoformat()

    def calibrate(self, pairs: List[Tuple[float, int]]) -> Dict[str, Any]:
        """Full calibration statistics for one horizon's matured (prob, outcome) pairs."""
        n = len(pairs)
        if n < self.min_sample:
            return {"verdict": "INSUFFICIENT_DATA", "n": n,
                    "min_sample": self.min_sample}
        probs = [p for p, _o in pairs]
        outs = [o for _p, o in pairs]
        brier = st.brier_score(probs, outs)
        ece = st.expected_calibration_error(probs, outs)
        curve = st.reliability_curve(probs, outs, n_bins=10, strategy="quantile")
        spieg = st.spiegelhalter_z(probs, outs)
        iso = st.isotonic_fit(probs, outs)
        claimed_mean = sum(probs) / n
        realized_mean = sum(outs) / n
        sig = (spieg.get("p_value") is not None
               and not (spieg["p_value"] != spieg["p_value"])  # not NaN
               and spieg["p_value"] < 0.05)
        return {
            "verdict": "MEASURED",
            "n": n,
            # headline calibration metrics
            "claimed_mean": round(claimed_mean, 4),
            "realized_mean": round(realized_mean, 4),
            "calibration_gap": round(claimed_mean - realized_mean, 4),
            "ece": ece.get("ece"),
            "mce": ece.get("mce"),
            "brier": brier.get("brier"),
            "brier_skill_score": brier.get("skill_score"),
            "reliability_component": brier.get("reliability"),
            "resolution_component": brier.get("resolution"),
            "spiegelhalter_z": spieg.get("z"),
            "spiegelhalter_p": spieg.get("p_value"),
            "miscalibration_significant": bool(sig),
            "well_calibrated": (not bool(sig)),
            "reliability_curve": curve,
            "recalibration_map": iso,  # feed back into scoring once stable
            # context only — NOT the calibration metric
            "context_hit_rate": round(realized_mean, 4),
        }

    def run(self, joined: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Per-horizon report with maturity + min-sample gating and honest framing."""
        per_horizon: Dict[str, Any] = {}
        any_measured = False
        for horizon, days in self.horizons_days.items():
            recs_h = [r for r in joined if str(r.get("horizon")).strip() == horizon]
            usable = [(r["claimed_prob"], r["outcome"]) for r in recs_h
                      if r.get("outcome") is not None and self._matured(r["date"], days)]
            res = self.calibrate(usable)
            res["horizon"] = horizon
            res["horizon_days"] = days
            res["claims_seen"] = len(recs_h)
            res["matured_usable"] = len(usable)
            if res["verdict"] == "INSUFFICIENT_DATA":
                res["next_measurable_date"] = self._next_measurable_date(recs_h, days)
            else:
                any_measured = True
            per_horizon[horizon] = res

        all_insufficient = all(
            v["verdict"] == "INSUFFICIENT_DATA" for v in per_horizon.values())
        status = ("NOT_YET_MEASURABLE" if all_insufficient
                  else "MEASURED" if all(v["verdict"] == "MEASURED"
                                         for v in per_horizon.values())
                  else "PARTIAL")
        return {
            "version": __version__,
            "as_of": self.as_of.isoformat(),
            "min_sample_per_horizon": self.min_sample,
            "status": status,
            "metric_meaning": (
                "Calibration = does claimed reliability match realized frequency. "
                "This is NOT hit-rate; a tool can be well-calibrated about its own "
                "uncertainty without forecasting at a high hit-rate. Lower ECE and "
                "non-significant Spiegelhalter p indicate good calibration."),
            "scope_note": (
                "Directional claims only; HOLD/WATCH-type calls are excluded as they "
                "make no falsifiable directional probability."),
            "horizons": per_horizon,
        }


# =============================================================================
# 5) SHEETS I/O  (mirrors track_performance.py gspread pattern; single touch point)
# =============================================================================

class SheetReader:
    """
    Minimal gspread reader/writer matching track_performance.py's PerformanceStore
    auth/open pattern. Degrades gracefully: if gspread or creds are unavailable,
    `available` is False and reads return None so the engine can still run on
    injected data (tests, --dry-run with a JSON input).
    """

    def __init__(self, workbook_id: str = WORKBOOK_ID):
        self.workbook_id = workbook_id
        self.available = False
        self.sheet = None
        self._authorize()

    def _credentials(self):
        # same env-based service-account mechanism the existing scripts use
        raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or os.environ.get(
            "GCP_SERVICE_ACCOUNT_JSON")
        if raw and service_account is not None:
            try:
                info = json.loads(raw)
                return service_account.Credentials.from_service_account_info(
                    info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
            except Exception:
                return None
        return None

    def _authorize(self) -> None:
        if not GSPREAD_AVAILABLE or gspread is None:
            return
        try:
            creds = self._credentials()
            gc = gspread.authorize(creds) if creds is not None else gspread.service_account()
            self.sheet = gc.open_by_key(self.workbook_id)
            self.available = True
        except Exception:
            self.available = False

    def read_tab(self, tab_name: str) -> Optional[List[List[Any]]]:
        """Return [header_row, *data_rows] for a tab, or None if unavailable/missing."""
        if not self.available or self.sheet is None:
            return None
        try:
            ws = self.sheet.worksheet(tab_name)
            return ws.get_all_values()  # list of lists, header first
        except Exception:
            return None

    def write_report(self, report: Dict[str, Any], tab_name: str = REPORT_TAB) -> bool:
        """Write a flat summary of the report. Best-effort; confirm tab/layout live."""
        if not self.available or self.sheet is None:
            return False
        try:
            try:
                ws = self.sheet.worksheet(tab_name)
            except Exception:
                ws = self.sheet.add_worksheet(title=tab_name, rows=200, cols=20)
            ws.update("A1", report_to_rows(report))
            return True
        except Exception:
            return False


# =============================================================================
# 6) REPORT FORMATTING
# =============================================================================

def report_to_rows(report: Dict[str, Any]) -> List[List[Any]]:
    """Flatten the report into a small grid for writing to the Calibration tab."""
    rows: List[List[Any]] = [
        ["Calibrator", report.get("version"), "as_of", report.get("as_of"),
         "status", report.get("status")],
        ["Horizon", "Verdict", "N", "Claimed%", "Realized%", "ECE",
         "BrierSkill", "Well-calibrated", "NextMeasurable"],
    ]
    for hz, v in report.get("horizons", {}).items():
        if v.get("verdict") == "MEASURED":
            rows.append([hz, "MEASURED", v.get("n"),
                         _pct(v.get("claimed_mean")), _pct(v.get("realized_mean")),
                         _r(v.get("ece")), _r(v.get("brier_skill_score")),
                         "yes" if v.get("well_calibrated") else "no", ""])
        else:
            rows.append([hz, "INSUFFICIENT_DATA", v.get("n"), "", "", "", "", "",
                         v.get("next_measurable_date") or "need more snapshots"])
    return rows


def _pct(x: Optional[float]) -> str:
    return "" if x is None else f"{100.0 * x:.1f}%"


def _r(x: Optional[float]) -> str:
    return "" if x is None else f"{x:.4f}"


def format_report_text(report: Dict[str, Any]) -> str:
    """Human-readable console summary."""
    lines = [
        f"TFB Calibrator v{report.get('version')}  as_of={report.get('as_of')}",
        f"STATUS: {report.get('status')}",
        f"  {report.get('metric_meaning')}",
        f"  {report.get('scope_note')}",
        "",
    ]
    for hz, v in report.get("horizons", {}).items():
        if v.get("verdict") == "MEASURED":
            sig = "NOT significant" if v.get("well_calibrated") else "SIGNIFICANT"
            lines.append(
                f"[{hz}] MEASURED  n={v['n']}  "
                f"claimed {_pct(v['claimed_mean'])} vs realized {_pct(v['realized_mean'])}  "
                f"(gap {v['calibration_gap']:+.3f})")
            lines.append(
                f"      ECE={_r(v['ece'])}  MCE={_r(v['mce'])}  "
                f"BrierSkill={_r(v['brier_skill_score'])}  "
                f"miscalibration {sig} (Spiegelhalter p={_r(v['spiegelhalter_p'])})")
        else:
            nm = v.get("next_measurable_date") or "more daily snapshots needed first"
            lines.append(
                f"[{hz}] INSUFFICIENT_DATA  matured/usable={v.get('matured_usable')} "
                f"(min {v.get('min_sample')})  -> first measurable: {nm}")
    return "\n".join(lines)


# =============================================================================
# 7) CLI
# =============================================================================

def _load_from_json_input(path: str) -> Tuple[List[List[Any]], List[List[Any]]]:
    """For --dry-run without Sheets: a JSON file with {signal_history, performance_log}
    each as a [header_row, *rows] grid."""
    with open(path, "r") as f:
        blob = json.load(f)
    return blob.get("signal_history", []), blob.get("performance_log", [])


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="TFB Calibrator — claimed vs realized reliability")
    ap.add_argument("--as-of", default=None, help="evaluation date YYYY-MM-DD (default today)")
    ap.add_argument("--min-sample", type=int, default=MIN_SAMPLE_PER_HORIZON)
    ap.add_argument("--workbook", default=WORKBOOK_ID)
    ap.add_argument("--input-json", default=None,
                    help="read sheet grids from a local JSON file instead of Sheets")
    ap.add_argument("--json-out", default=None, help="write the full report as JSON here")
    ap.add_argument("--no-write", action="store_true", help="do not write to the Sheet")
    ap.add_argument("--dry-run", action="store_true", help="compute and print only")
    args = ap.parse_args(argv)

    as_of = parse_date(args.as_of) if args.as_of else _dt.date.today()

    # ----- load raw grids -----
    if args.input_json:
        sig_vals, perf_vals = _load_from_json_input(args.input_json)
        reader = None
    else:
        reader = SheetReader(args.workbook)
        if not reader.available:
            print("Sheets unavailable (gspread/creds). Use --input-json for an offline run.")
            return 2
        sig_vals = reader.read_tab(SIGNAL_HISTORY_TAB)
        perf_vals = reader.read_tab(PERFORMANCE_LOG_TAB)
        if sig_vals is None or perf_vals is None:
            print("Could not read Signal_History / Performance_Log. Confirm tab names.")
            return 2

    # ----- pure pipeline -----
    signal_rows = parse_table(sig_vals, SIGNAL_HISTORY_COLUMNS, SIGNAL_HISTORY_REQUIRED)
    perf_rows = parse_table(perf_vals, PERFORMANCE_LOG_COLUMNS, PERFORMANCE_LOG_REQUIRED)
    joined = join_claims_outcomes(signal_rows, perf_rows)
    report = CalibrationEngine(as_of=as_of, min_sample=args.min_sample).run(joined)

    # ----- output -----
    print(format_report_text(report))
    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(report, f, indent=2)
        print(f"\nfull report -> {args.json_out}")
    if reader is not None and not args.no_write and not args.dry_run:
        ok = reader.write_report(report)
        print(f"\nwrite to '{REPORT_TAB}': {'ok' if ok else 'skipped/failed'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
