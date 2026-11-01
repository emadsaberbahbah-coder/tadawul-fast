#!/usr/bin/env python3
# scripts/apply_bq_views.py
"""
================================================================================
TFB BigQuery Looker views - v1.4.1 (program register C8, Phase 2)
================================================================================
v1.4.1 (1 Nov 2026, review follow-up on #886): v_outcome_coverage counts
scored calls and wins over the sheet's outcome cohort only (in_outcome_cohort:
the first valid matured record per stable key, as track_performance's
_summary_outcome_cohort counts them) and publishes matured_cohort and
matured_excluded beside the raw matured count, so a key archived twice can
never count twice in the win rate; v_portfolio_insight publishes
weight_sar_pct only when every holding has a riyal value (NULL for all rows
otherwise, with holdings_without_value saying how many), so a holding with no
rate can no longer leave the others sharing 100 % of a partial book. No other
object changes.
v1.4.0 (1 Nov 2026, full review: data accuracy and honest presentation on the
Looker pages). New: v_fx_sar (currency -> riyal from the Commodities_FX page)
and v_outcome_coverage (how much of the Performance_Log has matured, per
horizon, so a win rate always sits next to its denominator). Changed:
v_symbol_rating adds rating_gate / tfb_rating_display (the engine's verdict
beside every rating: blocked, do not invest, watch only, below the floors,
none - a Positive rating never overrides it) and the analyst / news sources;
v_top10_insight adds seat_kind and allocation (a row without a ticket reads
'none - research board', never an order); v_portfolio_insight gives riyal
value, cost, P&L and weight for every holding (the decision page's figures
first, else the sheet's values times v_fx_sar) with value_source. WHY (owner
request 1 Nov 2026): "avoid any data presentation interpretation ...
continuous presenting recommendation, avoid misleading".
v1.3.1 (1 Nov 2026, the one-symbol page): v_symbol_candles also gives
last_close (the symbol's newest close) and open / high / low /
close_vs_last_pct, every candle in percent points against that close. The
Looker candlestick chart always draws its axis from zero, so candles in
prices sit squeezed in a thin band at the top; against the newest close
they straddle zero and fill the chart. No other object changes.
v1.3.0 (31 Oct 2026, owner request: candles, news and an IBKR-like rating
in My Portfolio and on the one-symbol page): the three tables of
scripts/archive_symbol_deep.py (symbol_bars_daily, symbol_analyst_daily,
symbol_news) are source tables, for v_symbol_candles, v_symbol_analyst,
v_symbol_news, v_symbol_rating (the TFB rating) and the symbol-picker views
v_dive_signals / v_dive_board / v_dive_events; v_top10_insight and
v_portfolio_insight (now files 67 / 68, after the rating) carry the rating
and an 'analyst consensus below Hold' caution, and the portfolio's
bearish-call flag reads the market pages' STRONG_SELL spelling too.
v1.2.0 (31 Oct 2026, Looker pages 3-6): the seven tabs archive_to_bigquery
v1.1.0 adds (four market pages, three news gate tabs) are source tables, for
v_universe / v_universe_latest / f_universe_return (World Top-25 ROI),
v_news_items / v_econ_events / v_news_gate / v_calendar_events, the
v_top10_insight and v_portfolio_insight evidence views and f_symbol_pick
(the one-symbol page). Two helpers, f_col and f_at, read a wide tab by
position after one header lookup per snapshot; header_names_used() reports
the headers a view looks up either way (f_cell or f_col). WHY (owner request
31 Oct 2026): "the top companies that have best ROI world wide ... the page
of my portfolio and the page of top 10 ... insights ... that will make the
decision more reliable ... deep analysis for one selected symbol".
v1.1.0 (29 Oct 2026, global markets page): table functions (CREATE OR REPLACE
TABLE FUNCTION - the Looker date range reaches them through a custom query)
and market_series_daily (scripts/archive_market_series.py) as a source
table, for v_market_daily, f_market_window and f_market_summary. Scalar
functions still come before every view and table function.
WHY (owner request 29 Oct 2026, "can we also build Google Looker Studio"):
the nightly archive (scripts/archive_to_bigquery.py, C8 Phase 1) keeps each
learning tab as raw rows - cells[] plus record_json - one partition per day.
Looker Studio needs typed columns, one row per record and stated units. The
SQL in sql/bigquery/ turns the raw rows into those columns (three helper
functions, views, table functions); this script is how that SQL reaches BigQuery and
how anyone can prove the live objects are the reviewed ones.

WHAT:
  --dry-run (default)  checks every file (one CREATE OR REPLACE VIEW, FUNCTION
                       or TABLE FUNCTION, the {{DS}} placeholder, no data-
                       changing statement, scalar functions before views,
                       every referenced object defined earlier or a source
                       table) and prints
                       the plan with each object's body hash. When the
                       service-account key is configured it also asks
                       BigQuery to dry-run each statement (a broken key
                       exits 2); without one the checks stay offline.
                       Nothing is created.
  --write              creates or replaces every object in file order.
  --verify             reads the live definitions (INFORMATION_SCHEMA VIEWS
                       and ROUTINES, table functions included) and compares
                       their SHA-256 with the files;
                       exit 1 on any missing or different object.
Views and functions only: no table is created, changed or deleted, nothing
is written to the workbook, and the live system never reads BigQuery.

ENV: TFB_BQ_PROJECT (default project-2e3530e6-2311-4aa3-b36), TFB_BQ_DATASET
(default tfb_archive), TFB_BQ_LOCATION (default US), GOOGLE_SHEETS_CREDENTIALS
(the archive's service account: BigQuery Data Editor + Job User).
Exit: 0 done / everything matches, 1 a check, statement or comparison failed,
2 configuration missing (credentials for --write / --verify).

Not a licensed financial adviser; the owner decides and executes every trade.
================================================================================
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

__version__ = "1.4.1"

PLACEHOLDER = "{{DS}}"
DEFAULT_PROJECT = "project-2e3530e6-2311-4aa3-b36"
DEFAULT_DATASET = "tfb_archive"
DEFAULT_LOCATION = "US"
SQL_DIR = Path(__file__).resolve().parent.parent / "sql" / "bigquery"
BQ_API = "https://bigquery.googleapis.com/bigquery/v2"
# the tables scripts/archive_to_bigquery.py v1.1.0 creates (TABS -> table_name)
ARCHIVE_TABLES = frozenset({
    "top_10_investments", "selection_log", "shadow_rank_log", "board_scorecard",
    "calendar_events", "signal_history", "performance_log", "my_portfolio",
    "portfolio_decision", "run_log",
    # v1.2.0: the market pages and the news gate tabs (archive v1.1.0)
    "market_leaders", "global_markets", "mutual_funds", "commodities_fx",
    "news_items", "news_gate", "econ_events",
})
# the table scripts/archive_market_series.py v1.0.0 loads (global markets page)
MARKET_TABLES = frozenset({"market_series_daily"})
# the tables scripts/archive_symbol_deep.py v1.0.0 loads (v1.3.0)
DEEP_TABLES = frozenset({"symbol_bars_daily", "symbol_analyst_daily", "symbol_news"})
SOURCE_TABLES = ARCHIVE_TABLES | MARKET_TABLES | DEEP_TABLES
_CREATE = re.compile(
    r"CREATE OR REPLACE (VIEW|TABLE FUNCTION|FUNCTION) `\{\{DS\}\}\.([a-z][a-z0-9_]*)`")
_REF = re.compile(r"`\{\{DS\}\}\.([a-z][a-z0-9_]*)`")
_FORBIDDEN = re.compile(
    r"\b(INSERT\s+INTO|UPDATE\s+`|DELETE\s+FROM|MERGE\s+`|TRUNCATE\s+TABLE|"
    r"DROP\s+(?:TABLE|VIEW|SCHEMA|FUNCTION)|ALTER\s+(?:TABLE|VIEW|SCHEMA)|"
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE(?!\s+FUNCTION)|SCHEMA|MATERIALIZED)|GRANT|REVOKE|"
    r"EXPORT\s+DATA|LOAD\s+DATA|CALL)\b", re.IGNORECASE)


class SqlObject:
    __slots__ = ("path", "kind", "name", "template")

    def __init__(self, path: Path, kind: str, name: str, template: str):
        self.path, self.kind, self.name, self.template = path, kind, name, template

    def render(self, project: str, dataset: str) -> str:
        return render(self.template, project, dataset)

    def body(self, project: str, dataset: str) -> str:
        return object_body(self.render(project, dataset), self.kind)


def _out(msg: str) -> None:
    sys.stderr.write(f"[bq_views v{__version__}] {msg}\n")


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _has_credentials() -> bool:
    """The archive's service-account key is configured (raw JSON or base64)."""
    return bool(_env("GOOGLE_SHEETS_CREDENTIALS") or _env("GOOGLE_CREDENTIALS"))


def _code(sql: str) -> str:
    """The SQL without its comment lines (checks must not read the prose)."""
    return "\n".join(ln for ln in sql.splitlines() if not ln.lstrip().startswith("--"))


def render(template: str, project: str, dataset: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9\-]{4,61}[a-z0-9]", project or ""):
        raise ValueError(f"bad project id {project!r}")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,1023}", dataset or ""):
        raise ValueError(f"bad dataset id {dataset!r}")
    return template.replace(PLACEHOLDER, f"{project}.{dataset}")


def object_body(rendered: str, kind: str) -> str:
    """The text BigQuery keeps as the definition: for a view or a table
    function the query after the line 'AS' (INFORMATION_SCHEMA.VIEWS
    .view_definition / ROUTINES.routine_definition), for a scalar SQL
    function the expression inside 'AS ( ... )' before OPTIONS, stripped
    (INFORMATION_SCHEMA.ROUTINES.routine_definition)."""
    if kind in ("VIEW", "TABLE FUNCTION"):
        if "\nAS\n" not in rendered:
            raise ValueError(f"{kind.lower()} without an 'AS' line")
        body = rendered.split("\nAS\n", 1)[1].rstrip()
        return body[:-1] if body.endswith(";") else body
    start = rendered.index("\nAS (") + len("\nAS (")
    end = rendered.rindex(")\nOPTIONS")
    return rendered[start:end].strip()


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load_objects(sql_dir: Path = SQL_DIR) -> List[SqlObject]:
    objs: List[SqlObject] = []
    for path in sorted(sql_dir.glob("*.sql")):
        text = path.read_text(encoding="utf-8")
        found = _CREATE.findall(_code(text))
        if len(found) != 1:
            raise ValueError(f"{path.name}: expected exactly one CREATE OR REPLACE "
                             f"VIEW|FUNCTION|TABLE FUNCTION, found {len(found)}")
        kind, name = found[0]
        objs.append(SqlObject(path, kind, name, text))
    return objs


def check_objects(objs: Sequence[SqlObject]) -> List[str]:
    """Static checks; [] when every file is safe to apply in this order."""
    problems: List[str] = []
    defined: Dict[str, str] = {}
    seen_view = False
    for o in objs:
        code = _code(o.template)
        label = o.path.name
        if not re.fullmatch(r"\d\d_[a-z0-9_]+\.sql", label):
            problems.append(f"{label}: file name must be NN_<object>.sql")
        elif label[3:-4] != o.name:
            problems.append(f"{label}: defines {o.name}, file name says {label[3:-4]}")
        if PLACEHOLDER not in code:
            problems.append(f"{label}: no {PLACEHOLDER} placeholder")
        if re.search(r"project-[0-9a-f]{8}-|tfb_archive\.", code):
            problems.append(f"{label}: hard-coded project or dataset (use {PLACEHOLDER})")
        bad = _FORBIDDEN.search(code)
        if bad:
            problems.append(f"{label}: forbidden statement {bad.group(1)!r}")
        if code.count(";") != 1 or not code.rstrip().endswith(";"):
            problems.append(f"{label}: exactly one statement ending in ';'")
        if o.kind == "FUNCTION" and seen_view:
            problems.append(f"{label}: functions must come before every view and table function")
        if o.kind in ("VIEW", "TABLE FUNCTION"):
            seen_view = True
            if "\nAS\n" not in o.template:
                problems.append(f"{label}: the {o.kind.lower()} body must follow a line 'AS'")
        else:
            if "\nAS (" not in o.template or ")\nOPTIONS" not in o.template:
                problems.append(f"{label}: function layout must be 'AS (' ... ')' then 'OPTIONS'")
        if o.name in defined:
            problems.append(f"{label}: {o.name} already defined in {defined[o.name]}")
        for ref in _REF.findall(code):
            if ref == o.name:
                continue
            if ref not in SOURCE_TABLES and ref not in defined:
                problems.append(f"{label}: references {ref} before it is defined")
        defined[o.name] = label
    return problems


def header_names_used(template: str) -> List[str]:
    """Every header literal a view looks up - with f_cell, or once per
    snapshot with f_col (v1.2.0) - for the header tests."""
    return (re.findall(r"\.f_cell`\(h, (?:s\.cells|c), '([^']+)'\)", template)
            + re.findall(r"\.f_col`\(h, '([^']+)'\)", template))


# --------------------------------------------------------------------- BigQuery
def _archive_module():  # pragma: no cover - thin loader
    path = Path(__file__).resolve().parent / "archive_to_bigquery.py"
    spec = importlib.util.spec_from_file_location("_tfb_archive_to_bigquery", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def make_session():  # pragma: no cover - needs google-auth and a real key
    return _archive_module().make_session()


def _err(resp: Any) -> str:
    try:
        body = resp.json()
        msg = ((body or {}).get("error") or {}).get("message") or ""
    except Exception:
        msg = ""
    return f"HTTP {getattr(resp, 'status_code', '?')}" + (f": {msg[:300]}" if msg else "")


def run_query(session: Any, project: str, location: str, sql: str,
              dry_run: bool = False) -> Dict[str, Any]:
    payload = {"query": sql, "useLegacySql": False, "location": location,
               "dryRun": bool(dry_run), "timeoutMs": 60000,
               "labels": {"tfb": "looker-views"}}
    resp = session.post(f"{BQ_API}/projects/{project}/queries", json=payload, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(_err(resp))
    body = resp.json() or {}
    errs = body.get("errors") or []
    if errs:
        raise RuntimeError(str(errs[0].get("message") or errs[0])[:300])
    return body


def live_definitions(session: Any, project: str, dataset: str,
                     location: str) -> Dict[str, str]:
    """{object name: sha256 of its live definition} for views and routines."""
    sql = (f"SELECT table_name AS name, TO_HEX(SHA256(view_definition)) AS sha "
           f"FROM `{project}.{dataset}.INFORMATION_SCHEMA.VIEWS` "
           f"UNION ALL SELECT routine_name, TO_HEX(SHA256(routine_definition)) "
           f"FROM `{project}.{dataset}.INFORMATION_SCHEMA.ROUTINES`")
    body = run_query(session, project, location, sql)
    out: Dict[str, str] = {}
    for row in body.get("rows") or []:
        f = row.get("f") or []
        if len(f) >= 2:
            out[str(f[0].get("v"))] = str(f[1].get("v"))
    return out


def compare(objs: Sequence[SqlObject], project: str, dataset: str,
            live: Dict[str, str]) -> List[Tuple[str, str]]:
    """[(name, 'match' | 'missing' | 'different')] for every file."""
    rows = []
    for o in objs:
        want = sha256(o.body(project, dataset))
        got = live.get(o.name)
        rows.append((o.name, "match" if got == want else ("missing" if got is None else "different")))
    return rows


def main(argv: Optional[Sequence[str]] = None, session: Any = None,
         sql_dir: Optional[Path] = None) -> int:
    ap = argparse.ArgumentParser(description="TFB BigQuery Looker views")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="check and print the plan (default)")
    mode.add_argument("--write", action="store_true", help="create or replace every object")
    mode.add_argument("--verify", action="store_true", help="compare live definitions with the files")
    args = ap.parse_args(list(argv) if argv is not None else None)

    project = _env("TFB_BQ_PROJECT", DEFAULT_PROJECT)
    dataset = _env("TFB_BQ_DATASET", DEFAULT_DATASET)
    location = _env("TFB_BQ_LOCATION", DEFAULT_LOCATION)
    try:
        objs = load_objects(sql_dir or SQL_DIR)
        problems = check_objects(objs)
        for o in objs:
            o.render(project, dataset)
    except (OSError, ValueError) as exc:
        _out(f"FAIL {exc}")
        return 1
    if problems:
        for p in problems:
            _out(f"FAIL {p}")
        return 1
    for o in objs:
        _out(f"plan {o.kind.lower():14s} {o.name:22s} sha {sha256(o.body(project, dataset))[:12]}")

    if args.write or args.verify:
        if session is None:
            try:
                session = make_session()
            except Exception as exc:  # noqa: BLE001
                _out(f"CONFIG {exc}")
                return 2
        if args.write:
            for o in objs:
                try:
                    run_query(session, project, location, o.render(project, dataset))
                except Exception as exc:  # noqa: BLE001
                    _out(f"FAIL {o.name}: {exc}")
                    return 1
                _out(f"applied {o.name}")
        try:
            live = live_definitions(session, project, dataset, location)
        except Exception as exc:  # noqa: BLE001
            _out(f"FAIL reading live definitions: {exc}")
            return 1
        rows = compare(objs, project, dataset, live)
        for name, state in rows:
            _out(f"verify {name:22s} {state}")
        bad = [r for r in rows if r[1] != "match"]
        print(f"[bq_views v{__version__}] {len(rows) - len(bad)}/{len(rows)} objects match "
              f"{project}.{dataset}" + ("" if not bad else f"; mismatch: {', '.join(n for n, _ in bad)}"))
        return 1 if bad else 0

    if session is None and _has_credentials():
        try:
            session = make_session()
        except Exception as exc:  # noqa: BLE001
            _out(f"CONFIG {exc}")
            return 2
    if session is not None:
        for o in objs:
            try:
                run_query(session, project, location, o.render(project, dataset), dry_run=True)
            except Exception as exc:  # noqa: BLE001
                _out(f"FAIL dry-run {o.name}: {exc}")
                return 1
    checked = "static checks and BigQuery dry-run" if session is not None else "static checks only (no credentials)"
    print(f"[bq_views v{__version__}] dry run: {len(objs)} objects, {checked}, for "
          f"{project}.{dataset}; nothing created")
    return 0


if __name__ == "__main__":
    sys.exit(main())
