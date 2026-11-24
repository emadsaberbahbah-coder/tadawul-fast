#!/usr/bin/env python3
"""
scripts/learning_watch.py
================================================================================
TFB Learning Watch - every learning record in BigQuery, read every day, in one place
--------------------------------------------------------------------------------
v1.0.0 (2026-11-24)
  WHY: owner, 24 Nov 2026 13:07 Riyadh, in Arabic: how can BigQuery be used more, faster and more accurately - what do
       we need to develop? Item 1 of the answer: BigQuery holds 19 learning records (each signal, trial and gate
       scored against the market with a fixed reading), and none is seen until someone opens it. A reading that turns
       'works' or 'opposite' waits for a manual review. This job reads them all every day.
  WHAT, each run (chained to the hourly Exit Plan at the UTC hours of TFB_LEARNING_WATCH_CHAIN_HOURS, default 9 =
  12:35 Riyadh, after the night's collectors; or dispatched):
    1. each view of REGISTRY is read with one SELECT of its key columns, its reading, its sample size, its effect and
       its t (where it has one) - a view that is not in BigQuery is reported, never fatal;
    2. each row's reading is put in one of five groups by fixed words (classify): PROVEN (works / holds / beats /
       better / edge / ahead), AGAINST (opposite / inverted / worse / fails / inverse / loses / not ahead), COLLECTING
       (collecting / too few / thin / no edge yet), NEUTRAL (no clear edge, no edge, not proven, in order ...), and
       IN-SAMPLE (a history or backtest row, a backtest 'candidate', or a reading that says so: never evidence);
    3. the rows go to learning_watch_log (one partition per Riyadh day, replaced by a rerun the same day), and are
       compared with the latest earlier day: a row whose group changed is listed first;
    4. one page, Learning_Watch: the counts, what changed since the last day, every forward PROVEN and AGAINST row,
       and each record's tally.
  OBSERVE ONLY: nothing here changes a recommendation, a gate or an order; a PROVEN row is a candidate for the owner's
  review, nothing more. LOGS: counts per record only (no holding is named by these records).
  ROLLBACK: repository Variable TFB_LEARNING_WATCH=off.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import os
import re
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import archive_symbol_deep as asd  # noqa: E402 - same folder: the tested BigQuery loader
import news_board as nb  # noqa: E402 - same folder: text helpers and the optional-view reader

__version__ = "1.0.0"

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
TAB = "Learning_Watch"
WIDTH = 9
TABLE = "learning_watch_log"
_F = "FLOAT"
SCHEMA: List[Dict[str, str]] = [
    {"name": "snapshot_date", "type": "DATE", "mode": "REQUIRED", "description": "Riyadh date of the run (one partition per day)"},
    {"name": "captured_at", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "when the job ran"},
    {"name": "view_name", "type": "STRING", "mode": "REQUIRED", "description": "the learning view read"},
    {"name": "item", "type": "STRING", "mode": "REQUIRED", "description": "the row's key columns, joined with ' | '"},
    {"name": "reading", "type": "STRING", "mode": "NULLABLE", "description": "the view's own reading, as it stands"},
    {"name": "grp", "type": "STRING", "mode": "REQUIRED", "description": "PROVEN | AGAINST | COLLECTING | NEUTRAL | IN-SAMPLE"},
    {"name": "n", "type": _F, "mode": "NULLABLE", "description": "the row's sample size (events, calls or days, per view)"},
    {"name": "effect", "type": _F, "mode": "NULLABLE", "description": "the row's effect (edge, lift or IC, per view)"},
    {"name": "t", "type": _F, "mode": "NULLABLE", "description": "the row's t, where the view has one"},
    {"name": "run_id", "type": "STRING", "mode": "NULLABLE", "description": "GitHub Actions run id"},
]
SPEC = (SCHEMA, "snapshot_date", ["view_name"],
        "TFB Learning Watch: every learning view's rows and fixed readings, one partition per Riyadh day, written by "
        "scripts/learning_watch.py. Research only.")

# view -> (label, key columns, reading column, n column, effect column, t column or None, WHERE or "")
REGISTRY: List[Tuple[str, str, Tuple[str, ...], str, str, str, Optional[str], str]] = [
    ("v_confidence_calibration", "Confidence against the market", ("market", "confidence"), "reading", "records",
     "mean_excess_pp", None, "period = 'all'"),
    ("v_learning_scorecard", "Rule variants against their baseline", ("period", "horizon", "arm"), "reading", "calls",
     "lift_pp", "t_stat", ""),
    ("v_trial_c1", "Trial C-1 against the champion", ("period", "horizon", "arm"), "reading", "calls", "edge_pp",
     "t_stat", ""),
    ("v_trial_c2", "Trial C-2 against the champion", ("period", "horizon", "arm"), "reading", "calls", "edge_pp",
     "t_adj", ""),
    ("v_trial_shadow_v2", "Shadow score v2 against the current score", ("period", "market_group", "horizon"),
     "reading", "days", "ic_diff", "t_adj", ""),
    ("v_trial_news_overlay", "News overlay against the list", ("period", "arm", "horizon", "line"), "reading", "names",
     "edge_pp", "t_adj", ""),
    ("v_projection_scorecard", "Projection rules", ("period", "horizon", "arm"), "reading", "calls", "lift_pp",
     "t_stat", ""),
    ("v_outcome_sector", "Calls by sector against their market", ("market_group", "sector", "horizon"), "reading",
     "calls", "edge_pp", None, ""),
    ("v_signal_edge", "Score features (rank IC)", ("market_group", "feature", "horizon"), "verdict", "days",
     "mean_ic", "t_adj", ""),
    ("v_switch_calibration", "Hold or switch calibration", ("market_group", "horizon"), "verdict", "days", "mean_ic",
     "t_adj", ""),
    ("v_driver_learning", "Price drivers (rank IC)", ("driver",), "reading", "days", "mean_ic", "ic_t", ""),
    ("v_market_driver_learning", "US market forces", ("driver", "state"), "reading", "days", "lift_5d_pp", None, ""),
    ("v_news_signal_eval", "Provider news sentiment", ("signal", "class"), "reading", "days", "mean_5d_pct", None, ""),
    ("v_company_news_eval", "Company news classes", ("signal", "class"), "reading", "days", "mean_5d_excess_pp", None,
     ""),
    ("v_news_trial_summary", "News signals by market", ("period", "horizon", "key"), "verdict", "n", "edge_pp",
     "t_adj", ""),
    ("v_event_trial_summary", "Event scenarios", ("period", "family", "market"), "verdict", "events", "dir_pct",
     "dir_z", ""),
    ("v_news_trend_learning", "News and trend states (nightly)", ("period", "state", "trend_fit", "horizon_sessions"),
     "reading", "events", "edge_pp", "t_adj", ""),
    ("v_news_now_learning", "News Now (hourly)", ("period", "state", "horizon_sessions"), "reading", "events",
     "edge_pp", "t_adj", ""),
    ("v_shock_drop_learning", "Shock-drop rule", ("period", "arm", "horizon_sessions"), "reading", "events", "mean_pp",
     "t_adj", ""),
]
GROUPS = ("PROVEN", "AGAINST", "COLLECTING", "NEUTRAL", "IN-SAMPLE")
_ID_RE = re.compile(r"^[A-Za-z0-9_\-]+$")
_IN_SAMPLE_PERIOD = re.compile(r"history|in-sample|backtest|before reading", re.I)
# the fixed words, tried in this order (the first that matches decides)
_RULES: Tuple[Tuple[str, "re.Pattern[str]"], ...] = (
    ("IN-SAMPLE", re.compile(r"in-sample|not evidence|candidate", re.I)),     # a backtest candidate awaits its forward test
    ("COLLECTING", re.compile(r"collect|too few|^thin\b|\bthin$|no edge yet", re.I)),
    ("NEUTRAL", re.compile(r"not proven|no clear|not clear|^no edge|\bno edge\b|close to usual|^close$|in order|unstable|"
                           r"biased|not measurable|the middle reading|no expectation", re.I)),
    ("AGAINST", re.compile(r"opposite|inverted|worse|fails|inverse|loses|not ahead|not the expected way|weaker", re.I)),
    ("PROVEN", re.compile(r"works|holds|beats|better|proven|^edge$|ahead: the owner may consider|stronger", re.I)),
)
num, text = nb.num, nb.text
_SLEEP = time.sleep


def _out(msg: str) -> None:
    print("[learning_watch %s] %s" % (__version__, msg), flush=True)


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def enabled() -> bool:
    return _env("TFB_LEARNING_WATCH", "on").lower() not in ("off", "0", "false", "no")


def classify(reading: Any, period: Any = None) -> str:
    """The group of a reading by fixed words; a history or backtest row is IN-SAMPLE whatever it reads."""
    if period is not None and _IN_SAMPLE_PERIOD.search(text(period)):
        return "IN-SAMPLE"
    r = text(reading)
    for grp, rx in _RULES:
        if rx.search(r):
            return grp
    return "NEUTRAL"


def sql_view(ds: str, entry: Tuple[str, str, Tuple[str, ...], str, str, str, Optional[str], str]) -> str:
    view, _, keys, reading, n, effect, t, where = entry
    key = "CONCAT(%s)" % ", ' | ', ".join("IFNULL(CAST(%s AS STRING), '-')" % k for k in keys)
    period = "CAST(period AS STRING)" if "period" in keys else "CAST(NULL AS STRING)"
    return ("SELECT %s AS item, %s AS period, CAST(%s AS STRING) AS reading, SAFE_CAST(%s AS FLOAT64) AS n, "
            "SAFE_CAST(%s AS FLOAT64) AS effect, %s AS t FROM `%s.%s`%s"
            % (key, period, reading, n, effect, "SAFE_CAST(%s AS FLOAT64)" % t if t else "CAST(NULL AS FLOAT64)",
               ds, view, " WHERE " + where if where else ""))


def sql_previous(ds: str, day: str) -> str:
    return ("SELECT view_name, item, grp, reading, CAST(snapshot_date AS STRING) AS snapshot_date FROM `%s.%s` "
            "WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `%s.%s` WHERE snapshot_date < DATE '%s')"
            % (ds, TABLE, ds, TABLE, day))


def read_all(session: Any, project: str, location: str, ds: str, query: Any
             ) -> Tuple[List[Dict[str, Any]], List[str]]:
    """(rows, views not readable). A view that is missing or refused is listed, never fatal."""
    rows: List[Dict[str, Any]] = []
    missing: List[str] = []
    for entry in REGISTRY:
        try:
            got = query(session, project, location, sql_view(ds, entry))
        except Exception:  # noqa: BLE001 - one record never stops the others
            missing.append(entry[0])
            continue
        for r in got:
            rows.append({"view_name": entry[0], "label": entry[1], "item": text(r.get("item")) or "-",
                         "reading": text(r.get("reading")) or None, "n": num(r.get("n")), "effect": num(r.get("effect")),
                         "t": num(r.get("t")), "grp": classify(r.get("reading"), r.get("period"))})
    return rows, missing


def changes(rows: Sequence[Dict[str, Any]], previous: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """The rows whose group differs from the last earlier day, and the rows that are new; most important first."""
    before = {(text(p.get("view_name")), text(p.get("item"))): p for p in previous}
    out = []
    for r in rows:
        p = before.get((r["view_name"], r["item"]))
        if p is None:
            if previous:
                out.append(dict(r, was="new"))
        elif text(p.get("grp")) != r["grp"]:
            out.append(dict(r, was=text(p.get("grp"))))
    rank = {g: i for i, g in enumerate(GROUPS)}
    return sorted(out, key=lambda r: (rank.get(r["grp"], 9), r["view_name"], r["item"]))


def _fmt(v: Optional[float], nd: int = 2) -> str:
    return "" if v is None else ("%+." + str(nd) + "f") % v


def tally(rows: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    return {g: sum(1 for r in rows if r["grp"] == g) for g in GROUPS}


def verdict_line(rows: Sequence[Dict[str, Any]], changed: Sequence[Dict[str, Any]], missing: Sequence[str],
                 prev_day: str) -> str:
    """Counts only - this line also goes to the public run log."""
    t = tally(rows)
    return ("LEARNING WATCH: %d row(s) in %d record(s); PROVEN %d, AGAINST %d, collecting %d, neutral %d, in-sample %d; "
            "%s%s" % (len(rows), len({r["view_name"] for r in rows}), t["PROVEN"], t["AGAINST"], t["COLLECTING"],
                      t["NEUTRAL"], t["IN-SAMPLE"],
                      ("%d new or changed since %s" % (len(changed), prev_day)) if prev_day else "first day: nothing to compare",
                      ("; %d record(s) not readable" % len(missing)) if missing else ""))


def page_rows(rows: Sequence[Dict[str, Any]], changed: Sequence[Dict[str, Any]], missing: Sequence[str],
              prev_day: str, stamp: str) -> List[List[Any]]:
    pad = [""] * WIDTH

    def line(*cells: Any) -> List[Any]:
        r = list(cells)[:WIDTH]
        return r + pad[len(r):]

    head = line("Record", "Item", "Group", "Reading", "n", "Effect", "t", "Was")
    out = [line("LEARNING WATCH - every learning record in BigQuery, read every day: what is proven, what works "
                "against, what is still collecting, and what changed since the last day"),
           line("Built %s Riyadh | learning_watch %s | OBSERVE ONLY: a PROVEN row is a candidate for the owner's review; "
                "nothing on this page changes a recommendation, a gate or an order." % (stamp, __version__)),
           line(verdict_line(rows, changed, missing, prev_day))]
    if missing:
        out.append(line("Not readable this run: " + ", ".join(missing)))
    out += [line(), line("1. CHANGED SINCE %s" % (prev_day or "THE LAST DAY (none yet)")), head]
    if not changed:
        out.append(line("", "nothing new or changed" if prev_day else "the first day: tomorrow's run compares with today"))
    for r in changed:
        out.append(line(r["label"], r["item"], r["grp"], r["reading"] or "", _fmt(r["n"], 0), _fmt(r["effect"]),
                        _fmt(r["t"], 1), r["was"]))
    for no, grp, title in ((2, "PROVEN", "PROVEN - forward rows whose fixed reading says the signal works"),
                           (3, "AGAINST", "AGAINST - forward rows whose fixed reading says the signal works the "
                                          "other way (or worse than its baseline)")):
        out += [line(), line("%d. %s" % (no, title)), head]
        sel = [r for r in rows if r["grp"] == grp]
        if not sel:
            out.append(line("", "none"))
        for r in sorted(sel, key=lambda r: (r["view_name"], r["item"])):
            out.append(line(r["label"], r["item"], r["grp"], r["reading"] or "", _fmt(r["n"], 0), _fmt(r["effect"]),
                            _fmt(r["t"], 1)))
    out += [line(), line("4. EACH RECORD"), line("Record", "View", "Rows", *GROUPS)]
    for entry in REGISTRY:
        sel = [r for r in rows if r["view_name"] == entry[0]]
        t = tally(sel)
        out.append(line(entry[1], entry[0], len(sel) if entry[0] not in missing else "not readable",
                        *[t[g] for g in GROUPS]))
    out.append(line("How to read: each record keeps its own fixed reading (written before its outcomes were read); this "
                    "page only sorts the readings into groups by their words. IN-SAMPLE rows (history, backtests) are "
                    "never evidence. 'n' is the record's own sample (events, calls or days) and 'effect' its own measure "
                    "(edge or lift in points against the market, or a rank IC)."))
    return out


def run_once(dry_run: bool, book=None, session=None, now: Optional[_dt.datetime] = None, query=None,
             ensure: Optional[Callable[..., Any]] = None, load: Optional[Callable[..., Any]] = None) -> int:
    now = now or _dt.datetime.now(UTC)
    if not enabled():
        _out("TFB_LEARNING_WATCH is off: nothing read, nothing written")
        return 0
    project, dataset = _env("TFB_BQ_PROJECT"), _env("TFB_BQ_DATASET", "tfb_archive")
    location = _env("TFB_BQ_LOCATION", "US")
    if not _ID_RE.match(project) or not _ID_RE.match(dataset):
        _out("ERROR: TFB_BQ_PROJECT / TFB_BQ_DATASET missing or malformed; nothing written")
        return 2
    ds = "%s.%s" % (project, dataset)
    ensure = ensure or asd.ensure_table
    load = load or asd.load
    day = now.astimezone(RIYADH).date().isoformat()
    captured_at = now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    run_id = _env("GITHUB_RUN_ID", "local")
    try:
        if query is None:
            from scripts.evidence_board import bq_query as query  # noqa: PLC0415 - one SELECT-only reader
        if session is None:
            from scripts.archive_to_bigquery import make_session  # noqa: PLC0415
            session = make_session()
        rows, missing = read_all(session, project, location, ds, query)
        try:
            previous = query(session, project, location, sql_previous(ds, day))
        except RuntimeError as e:
            if "Not found" not in text(e):
                raise
            previous = []
    except Exception as e:  # noqa: BLE001
        _out("ERROR: %s while reading BigQuery; nothing written" % type(e).__name__)
        return 2
    prev_day = text(previous[0].get("snapshot_date")) if previous else ""
    changed = changes(rows, previous)
    _out(verdict_line(rows, changed, missing, prev_day))
    for entry in REGISTRY:
        t = tally([r for r in rows if r["view_name"] == entry[0]])
        _out("%-26s %s" % (entry[0], "not readable" if entry[0] in missing else
                           " ".join("%s %d" % (g, t[g]) for g in GROUPS)))
    if not rows:
        _out("ERROR: no record was readable; nothing written")
        return 2
    page = page_rows(rows, changed, missing, prev_day, now.astimezone(RIYADH).strftime("%Y-%m-%d %H:%M"))
    if dry_run:
        _out("dry run: %d log row(s) and %d page row(s) computed, nothing written" % (len(rows), len(page)))
        return 0
    log = [{"snapshot_date": day, "captured_at": captured_at, "view_name": r["view_name"], "item": r["item"],
            "reading": r["reading"], "grp": r["grp"], "n": r["n"], "effect": r["effect"], "t": r["t"],
            "run_id": run_id or None} for r in rows]
    rc = 0
    try:
        ensure(session, project, dataset, TABLE, spec=SPEC)
        n = load(session, project, dataset, location, TABLE, day, log, run_id, schema=SCHEMA)
        _out("loaded %d row(s) into %s$%s" % (n, TABLE, day.replace("-", "")))
    except Exception as e:  # noqa: BLE001 - the page is still written; the run is marked failed
        _out("ERROR: the log was not loaded (%s: %s)" % (type(e).__name__, text(e)[:200]))
        rc = 1
    try:
        if book is None:
            from scripts.run_calendar_sync import _open_book  # noqa: PLC0415
            book = _open_book()
        from scripts.daily_plan import write_grid  # noqa: PLC0415
        from scripts.sheets_quota import call as _quota_call  # noqa: PLC0415
        _quota_call(lambda: write_grid(book, TAB, page, WIDTH, 80), _SLEEP, _out, "writing %s" % TAB)
    except Exception as e:  # noqa: BLE001
        _out("ERROR: %s: the page was not written" % type(e).__name__)
        return 2
    _out("wrote the %s page (%d rows)" % (TAB, len(page)))
    return rc


def main(argv: Optional[Sequence[str]] = None, **kw) -> int:
    ap = argparse.ArgumentParser(description="TFB Learning Watch (every learning record, read daily; observe only)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(list(argv) if argv is not None else None)
    return run_once(a.dry_run, **kw)


if __name__ == "__main__":
    sys.exit(main())
