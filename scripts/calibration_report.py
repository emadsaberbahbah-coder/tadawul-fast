#!/usr/bin/env python3
"""Offline outcome/calibration evidence from a read-only workbook export.

WHY (v1.0.0, 2026-10-11): WP-14 requires reproducible model-quality evidence
without a notebook or live Sheets/provider calls. Keep the first decided
MATURED record per Key (matching TRACK_DEDUP_MATURED's decided-copy rule),
exclude missing/non-finite outcomes, and disclose missing reliability rather
than inventing a probability. Risk blanks and all-zero triplets are unavailable
measurements; they do not remove otherwise valid realized outcomes. Probability
is ENTRY Forecast Reliability / 100, the existing calibrator's advisory
interpretation, not a claim that reliability is a defined P(win). Signal_History
provides entry-date coverage/population evidence only: later snapshots never
replace entry forecasts or acquire outcomes by an ambiguous symbol-day join.

Usage: python scripts/calibration_report.py export.xlsx --as-of 2026-10-19 --json report.json --md report.md
Requires only openpyxl. All workbook reads use read_only=True, data_only=True.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import re
import sys

import openpyxl

SCRIPT_VERSION = "1.1.0"
RISK_COLUMNS = ("Volatility", "Max Drawdown %", "Sharpe Ratio")
RIYADH = dt.timezone(dt.timedelta(hours=3))
OUTCOME_WITNESS = "[OUTCOME-WITNESS v1]"


def text(value):
    return "" if value is None else str(value).strip()


def number(value):
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        n = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return n if math.isfinite(n) else None


def date(value):
    if isinstance(value, (dt.date, dt.datetime)):
        return value.strftime("%Y-%m-%d")
    match = re.match(r"^(\d{4}-\d{2}-\d{2})(?:$|[ T])", text(value))
    if not match:
        return None
    try:
        return dt.date.fromisoformat(match.group(1)).isoformat()
    except ValueError:
        return None


def riyadh_timestamp(value):
    """Read a declared Riyadh timestamp without inventing a missing time.

    Naive times in these workbook columns are explicitly Riyadh local times.
    Date-only cells can describe target days, but cannot prove when an outcome
    was evaluated or calibration was published.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, dt.datetime):
        stamp = value
    else:
        raw = text(value)
        if ":" not in raw:
            return None
        try:
            stamp = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None
    return (stamp.replace(tzinfo=RIYADH) if stamp.tzinfo is None else stamp.astimezone(RIYADH))


def outcome_witness(notes):
    """Read only reason/time from the last receipt; never emit private notes."""
    receipts = [part[len(OUTCOME_WITNESS):] for part in text(notes).split(" | ")
                if part.startswith(OUTCOME_WITNESS)]
    if not receipts:
        return "no_witness", None
    try:
        receipt = json.loads(receipts[-1])
        if not isinstance(receipt, dict) or receipt.get("schema") != "tfb.outcome-price.v1":
            return "invalid_witness", None
        reason = receipt.get("reason")
        if not isinstance(reason, str) or not re.fullmatch(r"[a-z0-9_]+(?:;[a-z0-9_]+)*", reason):
            reason = "unspecified_witness_reason"
        return reason, riyadh_timestamp(receipt.get("evaluated_at"))
    except (ValueError, TypeError):
        return "invalid_witness", None


def age_days(stamp, as_of):
    if stamp is None or as_of is None or stamp.date() > as_of:
        return None
    return (as_of - stamp.date()).days


def calibration_snapshot(workbook, as_of):
    """Bounded read of the existing checkpoint publication, not a new score."""
    result = {"available": "_S1_Calibration" in workbook.sheetnames,
              "published_at_riyadh": None, "age_calendar_days": None,
              "checkpoint_state": None, "n_checkpoints": None,
              "timestamp_after_as_of": False}
    if not result["available"]:
        return result
    rows = list(workbook["_S1_Calibration"].iter_rows(
        min_row=1, max_row=2, min_col=1, max_col=12, values_only=True))
    if len(rows) != 2:
        return result
    index = {text(value): i for i, value in enumerate(rows[0]) if text(value)}
    stamp = riyadh_timestamp(cell(rows[1], index, "As Of (Riyadh)"))
    state = text(cell(rows[1], index, "State"))
    sample = number(cell(rows[1], index, "N Checkpoints"))
    result.update(
        published_at_riyadh=stamp.isoformat() if stamp else None,
        age_calendar_days=age_days(stamp, as_of),
        checkpoint_state=state if state in {"PASS", "FAIL", "PENDING", "NOT_DECIDABLE"} else None,
        n_checkpoints=int(sample) if sample is not None and sample >= 0 and sample.is_integer() else None,
        timestamp_after_as_of=bool(stamp and as_of and stamp.date() > as_of),
    )
    return result


def table(ws, first_header):
    """Find the known header (Performance_Log deliberately starts at row 5)."""
    rows = ws.iter_rows(values_only=True)
    for row_number, row in enumerate(rows, 1):
        if row and text(row[0]) == first_header:
            index = {}
            for i, value in enumerate(row):
                if isinstance(value, str) and value.strip() and value.strip() not in index:
                    index[value.strip()] = i
            return row_number, index, rows
    raise ValueError(f"{ws.title}: cannot find {first_header!r} header")


def cell(row, index, *aliases):
    for alias in aliases:
        if alias in index:
            i = index[alias]
            return row[i] if i < len(row) else None
    return None


def wilson(wins, n):
    if not n:
        return [None, None]
    z = 1.959963984540054
    p, z2 = wins / n, z * z
    center = (p + z2 / (2 * n)) / (1 + z2 / n)
    radius = z * math.sqrt(p * (1 - p) / n + z2 / (4 * n * n)) / (1 + z2 / n)
    return [round(100 * (center - radius), 6), round(100 * (center + radius), 6)]


def summarize(records, min_sample=20):
    decided = [r for r in records if r["roi"] != 0]
    wins = sum(r["roi"] > 0 for r in decided)
    scored = [r for r in decided if r["reliability"] is not None]
    mean_claim = sum(r["reliability"] for r in scored) / len(scored) if scored else None
    scored_wr = 100 * sum(r["roi"] > 0 for r in scored) / len(scored) if scored else None
    brier = sum((r["reliability"] / 100 - int(r["roi"] > 0)) ** 2 for r in scored) / len(scored) if scored else None
    return {
        "matured": len(records), "decided": len(decided), "wins": wins,
        "losses": len(decided) - wins, "breakeven": len(records) - len(decided),
        "win_rate_pct": round(100 * wins / len(decided), 6) if decided else None,
        "win_rate_ci95_pct": wilson(wins, len(decided)),
        "brier_n": len(scored), "brier": round(brier, 8) if brier is not None else None,
        "brier_baseline": 0.25, "brier_mean_claimed_pct": round(mean_claim, 6) if mean_claim is not None else None,
        "brier_realized_win_rate_pct": round(scored_wr, 6) if scored_wr is not None else None,
        "calibration_gap_pp": round(mean_claim - scored_wr, 6) if scored else None,
        "missing_or_invalid_reliability": len(decided) - len(scored),
        "sufficient": len(scored) >= min_sample,
    }


def grouped(records, fields, min_sample):
    buckets = defaultdict(list)
    for record in records:
        buckets[tuple(record[f] for f in fields)].append(record)
    return [dict(zip(fields, key), **summarize(rows, min_sample)) for key, rows in sorted(buckets.items())]


def analyze_workbook(path, min_sample=20, as_of=None):
    if min_sample < 1:
        raise ValueError("min_sample must be positive")
    try:
        as_of_day = dt.date.fromisoformat(str(as_of)) if as_of is not None else None
    except ValueError as exc:
        raise ValueError("as_of must be a YYYY-MM-DD Riyadh date") from exc
    sha = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            sha.update(block)
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        if "Performance_Log" not in workbook.sheetnames:
            raise ValueError("Performance_Log is required")
        header_row, index, rows = table(workbook["Performance_Log"], "Record ID")
        required = {"Key", "Symbol", "Horizon", "Status", "Realized ROI %"}
        missing = required - index.keys()
        if missing:
            raise ValueError("Performance_Log missing columns: " + ", ".join(sorted(missing)))
        counts = Counter()
        records, first_decided = [], {}
        perf_dates, perf_updated = Counter(), Counter()
        maturity_dates, active_target_dates, overdue_reasons = Counter(), Counter(), Counter()
        latest_recorded = latest_maturity = latest_evaluated = None
        active_records = active_unknown_target = matured_unknown_time = 0
        risk = Counter()
        for row in rows:
            if not text(cell(row, index, "Record ID")):
                continue
            counts["performance_rows"] += 1
            recorded = date(cell(row, index, "Date Recorded (Riyadh)", "Date"))
            updated = date(cell(row, index, "Last Updated (Riyadh)"))
            if recorded:
                perf_dates[recorded] += 1
            if updated:
                perf_updated[updated] += 1
            recorded_at = riyadh_timestamp(cell(row, index, "Date Recorded (Riyadh)", "Date"))
            if recorded_at and (latest_recorded is None or recorded_at > latest_recorded):
                latest_recorded = recorded_at
            status = text(cell(row, index, "Status")).lower()
            reason, evaluated = outcome_witness(cell(row, index, "Notes"))
            if evaluated and (latest_evaluated is None or evaluated > latest_evaluated):
                latest_evaluated = evaluated
            if status == "active":
                active_records += 1
                raw_target = cell(row, index, "Target Date (Riyadh)")
                target_at = riyadh_timestamp(raw_target)
                target = target_at.date().isoformat() if target_at else date(raw_target)
                if target:
                    active_target_dates[target] += 1
                    if as_of_day and target < as_of_day.isoformat():
                        overdue_reasons[reason] += 1
                else:
                    active_unknown_target += 1
            if status != "matured":
                counts["non_matured_excluded"] += 1
                continue
            counts["matured_rows_raw"] += 1
            roi = number(cell(row, index, "Realized ROI %"))
            if roi is None:
                counts["matured_missing_or_invalid_roi_excluded"] += 1
                continue
            key = text(cell(row, index, "Key"))
            if key and key in first_decided:
                counts["duplicate_matured_excluded"] += 1
                if first_decided[key] != roi:
                    counts["duplicate_matured_conflicting_roi"] += 1
                continue
            if key:
                first_decided[key] = roi
            else:
                counts["matured_missing_key_retained"] += 1
            # Evidence age follows the same accepted outcome population as
            # calibration. A duplicate or missing-ROI row with a newer stamp
            # cannot make unchanged usable outcomes look freshly matured.
            maturity = riyadh_timestamp(cell(row, index, "Maturity Date"))
            if maturity:
                maturity_dates[maturity.date().isoformat()] += 1
                if latest_maturity is None or maturity > latest_maturity:
                    latest_maturity = maturity
            else:
                matured_unknown_time += 1
            reliability = number(cell(row, index, "Entry Forecast Reliability"))
            if reliability is not None and not 0 <= reliability <= 100:
                reliability = None
            measured = [number(cell(row, index, h)) for h in RISK_COLUMNS]
            if all(v is None for v in measured):
                risk["blank_or_invalid_triplets_excluded"] += 1
            elif all(v is not None and v == 0 for v in measured):
                risk["all_zero_placeholder_triplets_excluded"] += 1
            else:
                risk["partially_or_fully_measured_triplets"] += 1
            for h, v in zip(RISK_COLUMNS, measured):
                if v is None:
                    risk[h + "_blank_or_invalid_cells"] += 1
            outcome = text(cell(row, index, "Outcome")).upper()
            expected_outcome = "WIN" if roi > 0 else "LOSS" if roi < 0 else "BREAKEVEN"
            if outcome and outcome != expected_outcome:
                counts["outcome_label_disagrees_with_roi"] += 1
            records.append({"symbol": text(cell(row, index, "Symbol")), "date": recorded,
                            "horizon": text(cell(row, index, "Horizon")) or "UNSPECIFIED",
                            "page": text(cell(row, index, "Origin Tab")) or "UNSPECIFIED",
                            "investability": text(cell(row, index, "Entry Investability")).upper() or "UNSPECIFIED",
                            "roi": roi, "reliability": reliability})
        signal_counts = Counter()
        signal_dates = Counter()
        signal_days, signal_entry_keys = Counter(), Counter()
        signals_by_page = Counter()
        if "Signal_History" in workbook.sheetnames:
            signal_header, si, rows = table(workbook["Signal_History"], "Snapshot ID")
            for row in rows:
                symbol = text(cell(row, si, "Symbol"))
                if not symbol:
                    continue
                signal_counts["rows"] += 1
                day = date(cell(row, si, "Date (Riyadh)"))
                page = text(cell(row, si, "Origin Tab")) or "UNSPECIFIED"
                signals_by_page[page] += 1
                if day:
                    signal_dates[day] += 1
                    signal_days[(symbol, day)] += 1
                    signal_entry_keys[(symbol, day, page)] += 1
                else:
                    signal_counts["missing_date"] += 1
            signal_counts["symbol_days"] = len(signal_days)
            signal_counts["multi_version_symbol_days"] = sum(n > 1 for n in signal_days.values())
            signal_counts["entry_date_matches"] = sum(signal_entry_keys[(r["symbol"], r["date"], r["page"])] > 0 for r in records)
            signal_counts["ambiguous_entry_date_matches"] = sum(signal_entry_keys[(r["symbol"], r["date"], r["page"])] > 1 for r in records)
        else:
            signal_header = None
        market_dates = Counter()
        if "Global_Markets" in workbook.sheetnames:
            _, mi, rows = table(workbook["Global_Markets"], "Symbol")
            for row in rows:
                day = date(cell(row, mi, "Last Updated (UTC)"))
                if day:
                    market_dates[day] += 1
        def extent(dates):
            return {"earliest": min(dates) if dates else None, "latest": max(dates) if dates else None}
        return {
            "script": "calibration_report", "version": SCRIPT_VERSION,
            "source": {"export": Path(path).name, "sha256": sha.hexdigest(),
                       "read_only": True, "data_only": True,
                       "performance_header_row": header_row, "signal_header_row": signal_header,
                       "performance_recorded_dates": extent(perf_dates), "performance_updated_dates": extent(perf_updated),
                       "performance_maturity_dates": extent(maturity_dates),
                       "signal_dates": extent(signal_dates),
                       "global_markets_updated_dates": dict(sorted(market_dates.items())),
                       "global_markets_dominant_date": market_dates.most_common(1)[0][0] if market_dates else None},
            "method": {"min_sample": min_sample, "dedup": "first matured finite-realized-ROI record per nonblank Key",
                       "outcome": "sign of stored Realized ROI %; breakeven excluded from win rate/Brier",
                       "probability": "Entry Forecast Reliability / 100; missing/nonfinite/out-of-range excluded",
                       "risk": "blank/invalid and all-zero triplets excluded as risk measurements; outcomes retained",
                       "signal_join": "coverage on symbol + entry date + origin page; never used to impute probabilities/outcomes",
                       "promotion_gate": "advisory evidence only; Brier gating requires owner decision"},
            "exclusions": dict(sorted(counts.items())), "risk_measurement_exclusions": dict(sorted(risk.items())),
            "overall": summarize(records, min_sample),
            "learning_clock": {
                "as_of_riyadh_date": as_of_day.isoformat() if as_of_day else None,
                "latest_recorded_at_riyadh": latest_recorded.isoformat() if latest_recorded else None,
                "latest_maturity_at_riyadh": latest_maturity.isoformat() if latest_maturity else None,
                "maturity_age_calendar_days": age_days(latest_maturity, as_of_day),
                "maturity_timestamp_after_as_of": bool(latest_maturity and as_of_day and latest_maturity.date() > as_of_day),
                "eligible_matured_rows_missing_maturity_timestamp": matured_unknown_time,
                "latest_outcome_evaluation_at_riyadh": latest_evaluated.isoformat() if latest_evaluated else None,
                "outcome_evaluation_age_calendar_days": age_days(latest_evaluated, as_of_day),
                "active_records": active_records,
                "active_target_before_as_of_date": sum(n for day, n in active_target_dates.items()
                                                       if day < as_of_day.isoformat()) if as_of_day else None,
                "active_target_on_as_of_date": active_target_dates[as_of_day.isoformat()] if as_of_day else None,
                "active_missing_or_invalid_target_date": active_unknown_target,
                "overdue_by_target_date": {day: n for day, n in sorted(active_target_dates.items())
                                           if as_of_day and day < as_of_day.isoformat()},
                "overdue_outcome_witness_reasons": dict(sorted(overdue_reasons.items())),
                "calibration_snapshot": calibration_snapshot(workbook, as_of_day),
                "limits": "A new recording, report export or calibration publication does not prove a new matured outcome. "
                          "Calendar target days do not prove exchange-session completion or supply a target-session price. "
                          "Missing outcomes are not inferred. Ages/backlog need the explicitly supplied as-of date.",
            },
            "by_horizon_page": grouped(records, ("horizon", "page"), min_sample),
            "by_investability": grouped(records, ("investability",), min_sample),
            "signals": {"available": "Signal_History" in workbook.sheetnames,
                        **dict(sorted(signal_counts.items())), "by_page": dict(sorted(signals_by_page.items()))},
        }
    finally:
        workbook.close()


def render_md(report):
    source, overall = report["source"], report["overall"]
    clock = report["learning_clock"]
    def fmt(value):
        return "unavailable" if value is None else f"{value:.4f}" if isinstance(value, float) else str(value)
    lines = ["# Offline calibration evidence", "",
             f"Export: `{source['export']}`; SHA-256 `{source['sha256']}`.",
             f"Observed Global_Markets dominant update date: {source['global_markets_dominant_date']}; "
             f"latest Signal_History date: {source['signal_dates']['latest']}. These are source data dates, not a live read-back.",
             f"Performance_Log header row {source['performance_header_row']} is intentional.", "",
             "## Learning evidence dates", "",
             f"As-of Riyadh date: {fmt(clock['as_of_riyadh_date'])}. Latest recorded recommendation: "
             f"{fmt(clock['latest_recorded_at_riyadh'])}; latest matured outcome: "
             f"{fmt(clock['latest_maturity_at_riyadh'])} "
             f"(age {fmt(clock['maturity_age_calendar_days'])} calendar days).",
             f"Active targets before that date: {fmt(clock['active_target_before_as_of_date'])}; "
             f"targets on that date: {fmt(clock['active_target_on_as_of_date'])}; "
             f"active rows with unknown target dates: {clock['active_missing_or_invalid_target_date']}.",
             f"Existing checkpoint calibration publication: "
             f"{fmt(clock['calibration_snapshot']['published_at_riyadh'])} "
             f"(age {fmt(clock['calibration_snapshot']['age_calendar_days'])} calendar days); "
             f"checkpoint state {fmt(clock['calibration_snapshot']['checkpoint_state'])}. "
             "This is the stored checkpoint measurement, not the overall S-1 promotion decision.",
             clock["limits"], "",
             "## Outcome calibration", "",
             f"After deduplication: {overall['matured']:,} matured records; {overall['wins']:,} wins / "
             f"{overall['losses']:,} losses, win rate {fmt(overall['win_rate_pct'])}%. "
             f"Brier {fmt(overall['brier'])} over {overall['brier_n']:,} eligible records (baseline 0.25).", "",
             "Reliability is treated as an advisory implied probability. Blank/zero risk placeholders supply no risk evidence; "
             "their realized outcomes remain eligible. Signal_History supplies coverage only; ambiguous or later versions never replace entry predictions.", "",
             "| Horizon | Origin page | Matured | Wins | Losses | Win rate % | Brier n | Brier | Mean claim % | Claim minus wins pp | Sufficient |",
             "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for row in report["by_horizon_page"]:
        values = [row[k] for k in ("horizon", "page", "matured", "wins", "losses", "win_rate_pct", "brier_n", "brier",
                                   "brier_mean_claimed_pct", "calibration_gap_pp", "sufficient")]
        lines.append("| " + " | ".join(fmt(v).replace("|", "/") for v in values) + " |")
    lines.extend(["", "Exclusions and coverage:", "", "```json", json.dumps({
        "exclusions": report["exclusions"], "risk": report["risk_measurement_exclusions"],
        "signals": report["signals"], "learning_clock": clock,
    }, indent=2, sort_keys=True), "```", "", "Owner decision remains open: whether Brier above 0.25 should gate promotion. This report changes no production gate."])
    return "\n".join(lines) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("export")
    parser.add_argument("--json")
    parser.add_argument("--md")
    parser.add_argument("--min-sample", type=int, default=20)
    parser.add_argument("--as-of", help="Explicit Riyadh date (YYYY-MM-DD) for calendar-day evidence ages and backlog; no wall-clock default")
    args = parser.parse_args(argv)
    try:
        report = analyze_workbook(args.export, args.min_sample, args.as_of)
    except (OSError, ValueError) as exc:
        sys.stderr.write(f"calibration_report: {exc}\n")
        return 2
    if args.json:
        Path(args.json).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown = render_md(report)
    if args.md:
        Path(args.md).write_text(markdown, encoding="utf-8")
    if not args.md:
        print(markdown, end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
