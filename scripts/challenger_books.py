#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/challenger_books.py
================================================================================
TFB shadow challenger v0 - each forward book frozen once, the day it is final
--------------------------------------------------------------------------------
v1.0.0 (2026-11-27)
  WHY: full project review of 27 Nov 2026, plan item 'save the challenger's weekly books' (pulled forward from
       Thu 3 Dec; owner, 27 Nov 2026 16:30 Riyadh: "ok let's start do your recommdadtions and optimzation phase").
       v_challenger_v0_walkforward recomputes every book from the archive each time it is read. symbol_bars_daily
       is Yahoo's history, reloaded whole every night, and v_symbol_sector holds today's followed names, so a book
       read in January can differ from the book formed on 30 Nov (a revised bar, a name added or dropped). A
       forward test is judged on the books as they were formed. The first forward book forms on Mon 30 Nov 2026.
  WHAT: each run reads the forward books of v_challenger_v0_walkforward (formation date d on or after
       FORWARD_START and within the last LOOKBACK_DAYS) that challenger_books does not hold yet, and freezes
       each (market, d) that is final - the archive holds a later session of that market. Rows are appended
       into the partition of d; a frozen book is never rewritten or deleted by this script.
  ORDER: oldest first; each book is judged against the books frozen before it, this run's own included (a run
       catching up on two weeks judges the second against the first; Codex P2 on #1109).
  NOT FROZEN (each says why in the log):
       short  - fewer rows came back than the book's own size (the reader's one page of PAGE_LIMIT rows);
       thin   - under THIN_SHARE of the names of the market's last frozen book while only one later session is
                archived (a night Yahoo answered fewer names); with two later sessions it is frozen as it is;
       week   - the market already has a frozen book of that ISO week (a formation date that moved).
  ROWS: one per name of a book - the inputs, ranks, scores, quintiles, the trained ICs and weights and their
       basis, the shadow action and size and the benchmark (no result column: those close 22 sessions later),
       book_n, frozen_at, frozen_days_after (days from d to the freeze), run_id, views_version (this checkout's
       scripts/apply_bq_views.py) and views_sha (name=first 12 hex of SHA-256 of each installed view the book is
       read through, from INFORMATION_SCHEMA.VIEWS - the same hashes tests/test_bq_views.py pins).
  READS: SELECTs only (the shared reader). WRITES: challenger_books only. Nothing reads it yet; a scorecard of
       the frozen books follows when their first results close (22 sessions after the first book).
  LOGS: counts per market only - never a symbol.
  SWITCH: repository Variable TFB_CHALLENGER_BOOKS=off reads and writes nothing (default on).
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

TABLE = "challenger_books"
VIEW = "v_challenger_v0_walkforward"
CHAIN = ("v_symbol_sector", "v_bar_jumps", "v_bars_clean", "v_challenger_v0_features", "v_challenger_v0_ranks",
         VIEW)                                   # the views a book is read through (absent ones are left out)
FORWARD_START = _dt.date(2026, 11, 27)          # the scorecard's 'forward from 2026-11-27'
MIN_SESSION_NAMES = 20                           # a market's session: a date with at least 20 of its closes
LOOKBACK_DAYS = 21                               # a book not frozen within three weeks is left out (a late freeze
                                                 # would no longer be the book as formed)
THIN_SHARE = 0.9                                 # under 90 % of the last frozen book's names = thin
PAGE_LIMIT = 5000                                # the shared reader returns one page of this many rows
_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")

_F, _I, _S, _D, _T = "FLOAT", "INTEGER", "STRING", "DATE", "TIMESTAMP"


def _col(name: str, kind: str, mode: str = "NULLABLE", description: str = "") -> Dict[str, str]:
    c = {"name": name, "type": kind, "mode": mode}
    if description:
        c["description"] = description
    return c


SCHEMA: List[Dict[str, str]] = [
    _col("d", _D, "REQUIRED", "the formation date of the book"),
    _col("market", _S, "REQUIRED", "US, SA, JP, HK or OTHER"),
    _col("symbol", _S, "REQUIRED"),
    _col("sector", _S),
    _col("vol63", _F), _col("mom12_vs", _F), _col("mom6_vs", _F), _col("rs63", _F), _col("low_down126", _F),
    _col("dev_m12", _F), _col("dev_m6", _F), _col("dev_rs", _F), _col("dev_dd", _F),
    _col("sector_ranked", _I),
    _col("score_eq", _F), _col("q_eq", _I),
    _col("train_dates", _I), _col("weights_basis", _S),
    _col("train_ic_m12", _F), _col("train_ic_m6", _F), _col("train_ic_rs", _F), _col("train_ic_dd", _F),
    _col("w_m12", _F), _col("w_m6", _F), _col("w_rs", _F), _col("w_dd", _F),
    _col("score_wf", _F), _col("q_wf", _I),
    _col("shadow_action", _S), _col("shadow_size_pct", _F), _col("bench_symbol", _S),
    _col("book_n", _I, "REQUIRED", "the number of names of this book when it was frozen"),
    _col("frozen_at", _T, "REQUIRED"),
    _col("frozen_days_after", _I, "REQUIRED", "days from d to the freeze"),
    _col("run_id", _S),
    _col("views_version", _S, description="scripts/apply_bq_views.py of the checkout that froze the book"),
    _col("views_sha", _S, description="name=first 12 hex of SHA-256 of each installed view the book was read through"),
]
SPEC = (SCHEMA, "d", ["market", "symbol"],
        "TFB shadow challenger v0 forward books, frozen once the day each is final (challenger_books "
        + __version__ + "); research only")
_STAMP_COLUMNS = ("book_n", "frozen_at", "frozen_days_after", "run_id", "views_version", "views_sha")
BOOK_COLUMNS = [f["name"] for f in SCHEMA if f["name"] not in _STAMP_COLUMNS]
_KIND = {f["name"]: f["type"] for f in SCHEMA}


def _out(msg: str) -> None:
    print("[challenger_books %s] %s" % (__version__, msg), flush=True)


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def enabled() -> bool:
    return _env("TFB_CHALLENGER_BOOKS", "on").lower() not in ("off", "0", "false", "no")


def text(v: Any) -> str:
    return "" if v is None else str(v)


# ----------------------------------------------------------------------------- pure: the SQL
def sql_books(ds: str, since: _dt.date, unfrozen: bool = True) -> str:
    """The forward books of the walk-forward view from `since` on - only those challenger_books does not hold yet
    (unfrozen=False before the table exists) - each row carrying the size of its own book."""
    keep = ("\n  AND NOT EXISTS (SELECT 1 FROM `%s.%s` AS c WHERE c.market = w.market AND c.d = w.d)" % (ds, TABLE)
            if unfrozen else "")
    return ("SELECT %s, COUNT(*) OVER (PARTITION BY w.market, w.d) AS book_n\n"
            "FROM `%s.%s` AS w\nWHERE w.d >= DATE '%s'%s\nORDER BY w.market, w.d, w.symbol"
            % (", ".join("w." + c for c in BOOK_COLUMNS), ds, VIEW, max(since, FORWARD_START).isoformat(), keep))


def sql_sessions(ds: str, since: _dt.date) -> str:
    """Each market's archived sessions from `since` on (a date with at least MIN_SESSION_NAMES of its closes), with
    the challenger's own market of a symbol (its exchange suffix, as in v_challenger_v0_features)."""
    return ("SELECT market, date FROM (\n"
            "  SELECT CASE REGEXP_EXTRACT(symbol, r'\\.([A-Z]+)$')\n"
            "           WHEN 'US' THEN 'US' WHEN 'SR' THEN 'SA' WHEN 'T' THEN 'JP' WHEN 'HK' THEN 'HK' ELSE 'OTHER' END AS market,\n"
            "         date\n"
            "  FROM `%s.symbol_bars_daily`\n"
            "  WHERE close > 0 AND REGEXP_CONTAINS(symbol, r'\\.[A-Z]+$') AND date >= DATE '%s'\n"
            "  GROUP BY market, date\n"
            "  HAVING COUNT(DISTINCT symbol) >= %d\n"
            ") ORDER BY market, date" % (ds, since.isoformat(), MIN_SESSION_NAMES))


def sql_frozen(ds: str) -> str:
    return "SELECT market, d, COUNT(*) AS n FROM `%s.%s` GROUP BY market, d" % (ds, TABLE)


def sql_views(ds: str) -> str:
    return ("SELECT table_name, TO_HEX(SHA256(view_definition)) AS sha FROM `%s.INFORMATION_SCHEMA.VIEWS` "
            "WHERE table_name IN (%s)" % (ds, ", ".join("'%s'" % v for v in CHAIN)))


# ----------------------------------------------------------------------------- pure: the plan
def _date(v: Any) -> Optional[_dt.date]:
    try:
        return _dt.date.fromisoformat(text(v)[:10])
    except ValueError:
        return None


def _int(v: Any) -> int:
    try:
        return int(float(text(v) or 0))
    except ValueError:
        return 0


def books_by_key(rows: Sequence[Dict[str, Any]]) -> Dict[Tuple[str, _dt.date], List[Dict[str, Any]]]:
    out: Dict[Tuple[str, _dt.date], List[Dict[str, Any]]] = {}
    for r in rows:
        d = _date(r.get("d"))
        m = text(r.get("market"))
        if d is None or not m or not text(r.get("symbol")):
            continue
        out.setdefault((m, d), []).append(r)
    return out


def sessions_by_market(rows: Sequence[Dict[str, Any]]) -> Dict[str, List[_dt.date]]:
    out: Dict[str, List[_dt.date]] = {}
    for r in rows:
        d = _date(r.get("date"))
        if d is not None and text(r.get("market")):
            out.setdefault(text(r.get("market")), []).append(d)
    return {m: sorted(set(v)) for m, v in out.items()}


def views_sha(rows: Sequence[Dict[str, Any]]) -> Optional[str]:
    got = {text(r.get("table_name")): text(r.get("sha")).lower()[:12] for r in rows}
    parts = ["%s=%s" % (v, got[v]) for v in CHAIN if got.get(v)]
    return ";".join(parts) or None


def _week(d: _dt.date) -> Tuple[int, int]:
    return d.isocalendar()[:2]


def judge(m: str, d: _dt.date, rows: List[Dict[str, Any]], sessions: Dict[str, List[_dt.date]],
          frozen: Dict[Tuple[str, _dt.date], int]) -> Dict[str, Any]:
    """One book: FREEZE, HOLD (thin, try again) or SKIP, with the reason, against the books frozen so far."""
    want = max((_int(r.get("book_n")) for r in rows), default=0)
    later = sum(1 for s in sessions.get(m, []) if s > d)
    e = {"market": m, "d": d, "rows": rows, "names": len(rows), "book_n": want, "later": later, "why": ""}
    same_week = sorted(fd for (fm, fd) in frozen if fm == m and fd != d and _week(fd) == _week(d))
    prev = max(((fd, n) for (fm, fd), n in frozen.items() if fm == m and fd < d), default=None)
    if (m, d) in frozen:
        e["action"], e["why"] = "SKIP", "frozen already (%d rows)" % frozen[(m, d)]
    elif d < FORWARD_START:
        e["action"], e["why"] = "SKIP", "before the forward start"
    elif later < 1:
        e["action"], e["why"] = "SKIP", "not final yet (no later session of %s archived)" % m
    elif len(rows) != want or not rows:
        e["action"], e["why"] = "SKIP", "short (%d of %d rows came back)" % (len(rows), want)
    elif same_week:
        e["action"], e["why"] = "SKIP", "a book of this week is frozen already (d %s)" % same_week[0].isoformat()
    elif prev is not None and len(rows) < THIN_SHARE * prev[1] and later < 2:
        e["action"], e["why"] = "HOLD", ("thin (%d names; the book of %s had %d) - held until a second later "
                                         "session is archived" % (len(rows), prev[0].isoformat(), prev[1]))
    else:
        e["action"] = "FREEZE"
        if prev is not None and len(rows) < THIN_SHARE * prev[1]:
            e["why"] = "thin (%d names; the book of %s had %d), frozen as it is" % (
                len(rows), prev[0].isoformat(), prev[1])
    return e


def plan(books: Dict[Tuple[str, _dt.date], List[Dict[str, Any]]], sessions: Dict[str, List[_dt.date]],
         frozen: Dict[Tuple[str, _dt.date], int]) -> List[Dict[str, Any]]:
    """One entry per (market, d), oldest first, each judged against the books frozen before it - those frozen
    earlier and those this pass freezes ahead of it (Codex P2 on #1109: a run catching up on two weeks judges the
    second against the first)."""
    held, out = dict(frozen), []
    for (m, d), rows in sorted(books.items()):
        e = judge(m, d, rows, sessions, held)
        if e["action"] == "FREEZE":
            held[(m, d)] = e["names"]
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


def book_rows(entry: Dict[str, Any], now: _dt.datetime, run_id: str, views_version: str,
              sha: Optional[str]) -> List[Dict[str, Any]]:
    utc = now.astimezone(_dt.timezone.utc)
    stamp, after = utc.strftime("%Y-%m-%dT%H:%M:%SZ"), (utc.date() - entry["d"]).days
    out = []
    for r in entry["rows"]:
        row = {c: _cell(c, r.get(c)) for c in BOOK_COLUMNS}
        row.update(book_n=entry["book_n"], frozen_at=stamp, frozen_days_after=after, run_id=run_id or None,
                   views_version=views_version, views_sha=sha)
        out.append(row)
    return out


def summary(entry: Dict[str, Any]) -> str:
    acts: Dict[str, int] = {}
    for r in entry["rows"]:
        a = text(r.get("shadow_action")) or "none"
        acts[a] = acts.get(a, 0) + 1
    tally = ", ".join("%s %d" % (k, acts[k]) for k in sorted(acts))
    return "%-5s %s  %-6s %4d names  %s%s" % (entry["market"], entry["d"].isoformat(), entry["action"],
                                               entry["names"], tally, ("  (" + entry["why"] + ")") if entry["why"] else "")


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
    if not enabled():
        _out("TFB_CHALLENGER_BOOKS is off: nothing read, nothing written")
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
            import archive_symbol_deep as asd  # noqa: PLC0415 - the tested loader (as scripts/news_now.py uses it)
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
            rows = query(session, project, location, sql_books(ds, since))
        except RuntimeError as e:
            if not dry_run or "Not found" not in text(e):
                raise
            fz = []                                      # a dry run before the table exists
            rows = query(session, project, location, sql_books(ds, since, unfrozen=False))
        sess = sessions_by_market(query(session, project, location, sql_sessions(ds, since)))
    except Exception as e:  # noqa: BLE001
        _out("ERROR: %s before the freeze (%s); nothing written" % (type(e).__name__, text(e)[:240]))
        return 2
    frozen = {(text(r.get("market")), _date(r.get("d"))): _int(r.get("n")) for r in fz}
    _out("%d book(s) frozen so far; %d row(s) of books not frozen yet came back%s"
         % (len(frozen), len(rows), " - the reader's page limit: the books past it wait for the next run"
            if len(rows) >= PAGE_LIMIT else ""))
    books = books_by_key(rows)
    if not books:
        _out("no book to freeze (the first forward book forms on the first session of the week after %s)"
             % FORWARD_START.isoformat())
        return 0
    rc, todo, version, sha, sha_read = 0, 0, _views_version(), None, False
    for key in sorted(books):                        # oldest first: each book is judged against those frozen
        e = judge(key[0], key[1], books[key], sess, frozen)       # before it, this run's own included
        _out(summary(e))
        if e["action"] != "FREEZE":
            continue
        todo += 1
        if dry_run:
            frozen[key] = e["names"]                 # as a write run would see it
            continue
        if not sha_read:
            sha_read = True
            try:
                sha = views_sha(query(session, project, location, sql_views(ds)))
            except Exception as ex:  # noqa: BLE001 - the book is worth more than the hash: frozen without it
                _out("views_sha not read (%s); the books are frozen without it" % type(ex).__name__)
        try:
            n = load(session, project, dataset, location, TABLE, e["d"].isoformat(),
                     book_rows(e, now, run_id, version, sha), run_id, schema=SCHEMA, disposition="WRITE_APPEND")
            frozen[key] = e["names"]                 # only a book that was written is a reference
            _out("froze %s %s: %d row(s) into %s$%s" % (e["market"], e["d"].isoformat(), n, TABLE,
                                                        e["d"].isoformat().replace("-", "")))
        except Exception as ex:  # noqa: BLE001 - one market never stops the others; the run is marked failed
            _out("ERROR: %s %s was not frozen (%s)" % (e["market"], e["d"].isoformat(), type(ex).__name__))
            rc = 1
    if dry_run:
        _out("dry run: %d book(s) would be frozen, nothing written" % todo)
        return 0
    if not todo:
        _out("nothing to freeze")
    return rc


def main(argv: Optional[Sequence[str]] = None, **kw) -> int:
    ap = argparse.ArgumentParser(description="TFB shadow challenger: freeze each forward book once, when final")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(list(argv) if argv is not None else None)
    return run_once(a.dry_run, **kw)


if __name__ == "__main__":
    sys.exit(main())
