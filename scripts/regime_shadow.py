#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/regime_shadow.py
================================================================================
TFB shadow regime rule - each final close's label frozen once, the forward record
--------------------------------------------------------------------------------
v1.0.0 (2026-11-28)
  WHY: week plan of 23 Nov 2026, package 6 ("the audit's bull / choppy / risk-off regime and our adverse-state
       count become one shadow rule"; exit test "regime labels are logged daily"; owner, 23 Nov 2026: "go and Start
       build"). v_regime_shadow (views 1.38.0) labels every close of the five equity markets from the archive, and
       the archive is reloaded every night (Yahoo's bars and the market series), so a label read in
       January can differ from the label the rule gave on the day. A rule is judged forward on the labels as they
       were given. The rule was fixed on 28 Nov 2026; the forward record starts with the next close (Sun 29 Nov
       on Tadawul, Mon 30 Nov elsewhere).
  WHAT: each run reads the rows of v_regime_shadow (close date d on or after FORWARD_START and within the last
       LOOKBACK_DAYS) that regime_shadow_log does not hold yet, and freezes each (market, d) that is final - the
       archive holds a later close of that market's index. Rows are appended into the partition of d; a frozen row
       is never rewritten or deleted by this script.
  NOT FROZEN (each says why in the log):
       label not known  - the rule's inputs are short (trend or volatility percentile not known);
       waiting          - the breadth (or, for the US, the adverse-state count) is not in yet while only one later
                          close is archived; with two later closes the row is frozen as it is, and says so.
  ROWS: one per market and close - the inputs (close, 200-close average now and 20 closes back, trend, breadth and
       its names, volatility and its percentile, the US adverse count), TFB's own condition label, rule_version,
       the label and its reasons, later_sessions (closes archived after d at the freeze), frozen_at,
       frozen_days_after, run_id, views_version and views_sha (name=first 12 hex of SHA-256 of each installed view
       the row is read through, from INFORMATION_SCHEMA.VIEWS - the same hashes tests/test_bq_views.py pins).
       No result column: those close 5 and 21 closes later, and the forward scorecard reads them then.
  READS: SELECTs only (the shared reader). WRITES: regime_shadow_log only. Nothing reads it yet; no page, gate,
       pick, count or order changes.
  LOGS: one line per market and close (the market, the date, the label and its reasons) - never a symbol.
  SWITCH: repository Variable TFB_REGIME_SHADOW = observe (default: log the labels) | off (read and write nothing).
"""
from __future__ import annotations

import argparse
import datetime as _dt
import os
import re
import sys
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)
for _p in (_REPO_ROOT, _HERE):                   # scripts/archive_symbol_deep.py imports its neighbours by name
    if _p not in sys.path:
        sys.path.insert(0, _p)

__version__ = "1.0.0"

TABLE = "regime_shadow_log"
VIEW = "v_regime_shadow"
CHAIN = ("v_market_condition", "v_market_drivers", VIEW)    # the views a label is read through
MARKETS = ("us", "sa", "eu", "jp", "cn")
FORWARD_START = _dt.date(2026, 11, 29)          # the first close after the rule was fixed (28 Nov 2026)
LOOKBACK_DAYS = 21                               # a close not frozen within three weeks is left out (a late freeze
                                                 # would no longer be the label as given)
_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")

_F, _I, _S, _D, _T = "FLOAT", "INTEGER", "STRING", "DATE", "TIMESTAMP"


def _col(name: str, kind: str, mode: str = "NULLABLE", description: str = "") -> Dict[str, str]:
    c = {"name": name, "type": kind, "mode": mode}
    if description:
        c["description"] = description
    return c


SCHEMA: List[Dict[str, str]] = [
    _col("date", _D, "REQUIRED", "the close the label is for"),
    _col("market_id", _S, "REQUIRED", "us, sa, eu, jp or cn"),
    _col("market", _S), _col("reading_symbol", _S, description="the index the label reads"),
    _col("close", _F), _col("sma200", _F), _col("sma200_prev20", _F),
    _col("trend", _S, description="up, down or mixed"),
    _col("breadth50_pct", _F), _col("breadth_names", _I),
    _col("vol20_pct", _F), _col("vol_pct_rank", _F),
    _col("adverse", _I, description="the US only: adverse US market states, 0-6"),
    _col("condition_label", _S, description="v_market_condition's label that close"),
    _col("rule_version", _I, "REQUIRED"),
    _col("regime", _S, "REQUIRED", "BULL, CHOPPY or RISK_OFF"),
    _col("regime_why", _S),
    _col("later_sessions", _I, "REQUIRED", "closes of the market archived after the date when it was frozen"),
    _col("frozen_at", _T, "REQUIRED"),
    _col("frozen_days_after", _I, "REQUIRED", "days from the date to the freeze"),
    _col("run_id", _S),
    _col("views_version", _S, description="scripts/apply_bq_views.py of the checkout that froze the row"),
    _col("views_sha", _S, description="name=first 12 hex of SHA-256 of each installed view the row was read through"),
]
SPEC = (SCHEMA, "date", ["market_id"],
        "TFB shadow regime rule, each final close's label frozen once (regime_shadow " + __version__
        + "); the forward record; research only")
_STAMP_COLUMNS = ("later_sessions", "frozen_at", "frozen_days_after", "run_id", "views_version", "views_sha")
ROW_COLUMNS = [f["name"] for f in SCHEMA if f["name"] not in _STAMP_COLUMNS]
_KIND = {f["name"]: f["type"] for f in SCHEMA}


def _out(msg: str) -> None:
    print("[regime_shadow %s] %s" % (__version__, msg), flush=True)


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def mode() -> str:
    """observe (default; 'on' reads as observe) | off; any other word is observe."""
    v = _env("TFB_REGIME_SHADOW", "observe").lower()
    return "off" if v in ("off", "0", "false", "no") else "observe"


def text(v: Any) -> str:
    return "" if v is None else str(v)


# ----------------------------------------------------------------------------- pure: the SQL
def _markets() -> str:
    return ", ".join("'%s'" % m for m in MARKETS)


def sql_rows(ds: str, since: _dt.date, unfrozen: bool = True) -> str:
    """The labelled closes of the view from `since` on - only those regime_shadow_log does not hold yet
    (unfrozen=False before the table exists)."""
    keep = ("\n  AND NOT EXISTS (SELECT 1 FROM `%s.%s` AS f WHERE f.market_id = r.market_id AND f.date = r.date)"
            % (ds, TABLE) if unfrozen else "")
    return ("SELECT %s\nFROM `%s.%s` AS r\nWHERE r.date >= DATE '%s' AND r.market_id IN (%s)%s\n"
            "ORDER BY r.market_id, r.date"
            % (", ".join("r." + c for c in ROW_COLUMNS), ds, VIEW, max(since, FORWARD_START).isoformat(),
               _markets(), keep))


def sql_sessions(ds: str, since: _dt.date) -> str:
    """Each market's archived index closes from `since` on (the reading v_regime_shadow labels)."""
    return ("SELECT market_id, date FROM `%s.v_market_condition`\n"
            "WHERE market_id IN (%s) AND close > 0 AND date >= DATE '%s'\nORDER BY market_id, date"
            % (ds, _markets(), since.isoformat()))


def sql_frozen(ds: str) -> str:
    return "SELECT market_id, date FROM `%s.%s` GROUP BY market_id, date" % (ds, TABLE)


def sql_views(ds: str) -> str:
    return ("SELECT table_name, TO_HEX(SHA256(view_definition)) AS sha FROM `%s.INFORMATION_SCHEMA.VIEWS` "
            "WHERE table_name IN (%s)" % (ds, ", ".join("'%s'" % v for v in CHAIN)))


# ----------------------------------------------------------------------------- pure: the plan
def _date(v: Any) -> Optional[_dt.date]:
    try:
        return _dt.date.fromisoformat(text(v)[:10])
    except ValueError:
        return None


def sessions_by_market(rows: Sequence[Dict[str, Any]]) -> Dict[str, List[_dt.date]]:
    out: Dict[str, List[_dt.date]] = {}
    for r in rows:
        d = _date(r.get("date"))
        if d is not None and text(r.get("market_id")):
            out.setdefault(text(r.get("market_id")), []).append(d)
    return {m: sorted(set(v)) for m, v in out.items()}


def views_sha(rows: Sequence[Dict[str, Any]]) -> Optional[str]:
    got = {text(r.get("table_name")): text(r.get("sha")).lower()[:12] for r in rows}
    parts = ["%s=%s" % (v, got[v]) for v in CHAIN if got.get(v)]
    return ";".join(parts) or None


def missing_inputs(row: Dict[str, Any]) -> List[str]:
    """The inputs that may still arrive after the close: the breadth, and the US adverse-state count."""
    out = []
    if text(row.get("breadth50_pct")) == "":
        out.append("breadth")
    if text(row.get("market_id")) == "us" and text(row.get("adverse")) == "":
        out.append("adverse states")
    return out


def judge(row: Dict[str, Any], sessions: Dict[str, List[_dt.date]],
          frozen: Dict[Tuple[str, _dt.date], bool]) -> Dict[str, Any]:
    """One close: FREEZE, HOLD (an input may still arrive) or SKIP, with the reason."""
    m, d = text(row.get("market_id")), _date(row.get("date"))
    later = sum(1 for s in sessions.get(m, []) if d is not None and s > d)
    e = {"market_id": m, "date": d, "row": row, "later": later, "regime": text(row.get("regime")), "why": ""}
    missing = missing_inputs(row)
    if d is None or m not in MARKETS:
        e["action"], e["why"] = "SKIP", "not a market close of the rule"
    elif (m, d) in frozen:
        e["action"], e["why"] = "SKIP", "frozen already"
    elif d < FORWARD_START:
        e["action"], e["why"] = "SKIP", "before the forward start"
    elif later < 1:
        e["action"], e["why"] = "SKIP", "not final yet (no later close of %s archived)" % m
    elif not e["regime"]:
        e["action"], e["why"] = "SKIP", "label not known (the rule's inputs are short)"
    elif missing and later < 2:
        e["action"], e["why"] = "HOLD", "waiting for the %s (one later close archived)" % " and ".join(missing)
    else:
        e["action"] = "FREEZE"
        if missing:
            e["why"] = "frozen without the %s (%d later closes archived)" % (" and ".join(missing), later)
    return e


def plan(rows: Sequence[Dict[str, Any]], sessions: Dict[str, List[_dt.date]],
         frozen: Dict[Tuple[str, _dt.date], bool]) -> List[Dict[str, Any]]:
    """One entry per (market, close), oldest first; a key seen twice is judged once."""
    seen, out = dict(frozen), []
    for r in sorted(rows, key=lambda x: (text(x.get("date")), text(x.get("market_id")))):
        e = judge(r, sessions, seen)
        if e["action"] == "FREEZE":
            seen[(e["market_id"], e["date"])] = True
        out.append(e)
    return out


def _cell(name: str, v: Any) -> Any:
    if v is None or text(v) == "":
        return None
    kind = _KIND[name]
    if kind == _F:
        return float(v)
    if kind == _I:
        return int(float(v))
    if kind == _D:
        return text(v)[:10]
    return text(v)


def frozen_row(entry: Dict[str, Any], now: _dt.datetime, run_id: str, views_version: str,
               sha: Optional[str]) -> Dict[str, Any]:
    utc = now.astimezone(_dt.timezone.utc)
    row = {c: _cell(c, entry["row"].get(c)) for c in ROW_COLUMNS}
    row.update(later_sessions=entry["later"], frozen_at=utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
               frozen_days_after=(utc.date() - entry["date"]).days, run_id=run_id or None,
               views_version=views_version, views_sha=sha)
    return row


def summary(entry: Dict[str, Any]) -> str:
    d = entry["date"].isoformat() if entry["date"] else "?"
    reasons = text(entry["row"].get("regime_why"))
    tail = "  (" + entry["why"] + ")" if entry["why"] else ""
    return "%-2s %s  %-6s %-8s %s%s" % (entry["market_id"], d, entry["action"], entry["regime"] or "-",
                                         reasons, tail)


# ----------------------------------------------------------------------------- the run
def _views_version() -> str:
    try:
        from scripts import apply_bq_views as bv  # noqa: PLC0415
        return "views " + bv.__version__
    except Exception:  # noqa: BLE001
        return "views ?"


def run_once(dry_run: bool, session=None, now: Optional[_dt.datetime] = None, query=None,
             ensure: Optional[Callable[..., Any]] = None, load: Optional[Callable[..., Any]] = None) -> int:
    now = now or _dt.datetime.now(_dt.timezone.utc)
    if mode() == "off":
        _out("TFB_REGIME_SHADOW is off: nothing read, nothing written")
        return 0
    project, dataset = _env("TFB_BQ_PROJECT"), _env("TFB_BQ_DATASET", "tfb_archive")
    location = _env("TFB_BQ_LOCATION", "US")
    if not _ID_RE.match(project) or not _ID_RE.match(dataset):
        _out("ERROR: TFB_BQ_PROJECT / TFB_BQ_DATASET missing or malformed; nothing written")
        return 2
    ds = "%s.%s" % (project, dataset)
    run_id = _env("GITHUB_RUN_ID", "local")
    since = now.astimezone(_dt.timezone.utc).date() - _dt.timedelta(days=LOOKBACK_DAYS)
    try:
        if ensure is None or load is None:
            import archive_symbol_deep as asd  # noqa: PLC0415 - the tested loader (as challenger_books uses it)
            ensure = ensure or asd.ensure_table
            load = load or asd.load
        if query is None:
            from scripts.evidence_board import bq_query as query  # noqa: PLC0415 - the SELECT-only reader
        if session is None:
            from scripts.archive_to_bigquery import make_session  # noqa: PLC0415
            session = make_session()
        if not dry_run:
            ensure(session, project, dataset, TABLE, spec=SPEC)
        try:
            fz = query(session, project, location, sql_frozen(ds))
            rows = query(session, project, location, sql_rows(ds, since))
        except RuntimeError as e:
            if not dry_run or "Not found" not in text(e) or TABLE not in text(e):
                raise
            fz = []                                      # a dry run before the table exists
            rows = query(session, project, location, sql_rows(ds, since, unfrozen=False))
        sess = sessions_by_market(query(session, project, location, sql_sessions(ds, since)))
    except Exception as e:  # noqa: BLE001
        _out("ERROR: %s before the freeze (%s); nothing written" % (type(e).__name__, text(e)[:240]))
        return 2
    frozen = {(text(r.get("market_id")), _date(r.get("date"))): True for r in fz}
    _out("%d close(s) frozen so far; %d labelled close(s) not frozen yet came back" % (len(frozen), len(rows)))
    if not rows:
        _out("no close to freeze (the forward record starts with the closes of %s and later)"
             % FORWARD_START.isoformat())
        return 0
    rc, todo, version, sha, sha_read = 0, 0, _views_version(), None, False
    for e in plan(rows, sess, frozen):
        _out(summary(e))
        if e["action"] != "FREEZE":
            continue
        todo += 1
        if dry_run:
            continue
        if not sha_read:
            sha_read = True
            try:
                sha = views_sha(query(session, project, location, sql_views(ds)))
            except Exception as ex:  # noqa: BLE001 - the label is worth more than the hash: frozen without it
                _out("views_sha not read (%s); the labels are frozen without it" % type(ex).__name__)
        try:
            n = load(session, project, dataset, location, TABLE, e["date"].isoformat(),
                     [frozen_row(e, now, run_id, version, sha)], run_id, schema=SCHEMA, disposition="WRITE_APPEND")
            _out("froze %s %s: %d row into %s$%s" % (e["market_id"], e["date"].isoformat(), n, TABLE,
                                                     e["date"].isoformat().replace("-", "")))
        except Exception as ex:  # noqa: BLE001 - one market never stops the others; the run is marked failed
            _out("ERROR: %s %s was not frozen (%s)" % (e["market_id"], e["date"].isoformat(), type(ex).__name__))
            rc = 1
    if dry_run:
        _out("dry run: %d close(s) would be frozen, nothing written" % todo)
        return 0
    if not todo:
        _out("nothing to freeze")
    return rc


def main(argv: Optional[Sequence[str]] = None, **kw) -> int:
    ap = argparse.ArgumentParser(description="TFB shadow regime rule: freeze each final close's label once")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(list(argv) if argv is not None else None)
    return run_once(a.dry_run, **kw)


if __name__ == "__main__":
    sys.exit(main())
