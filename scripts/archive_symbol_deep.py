#!/usr/bin/env python3
# scripts/archive_symbol_deep.py
"""
================================================================================
TFB Symbol Deep Data - v1.0.0 (program register C8, Phase 3)
================================================================================
WHY (owner request 31 Oct 2026): "candle charts in my portfolio ... news in my
portfolio ... investor recommendation rating to invest like what in IBKR ...
from different institutions or make our own one ... the deep data,
information, analysis, news, candle charts ... for one symbol that I select".
The workbook keeps one snapshot of each instrument per sync and the nightly
archive (scripts/archive_to_bigquery.py) keeps one per night, so a symbol has
no price history before 31 Oct 2026, no per-institution analyst counts and
no per-symbol headlines. This job adds those three to BigQuery for the
symbols that matter; the views in sql/bigquery/ turn them into candles, a
news feed, the analyst consensus and the TFB rating.

WHAT - three loads, nightly after the workbook archive:
  1. bars:    two years of daily OHLC from the Yahoo chart API (no key, no
              EODHD quota - the reader of scripts/archive_market_series.py,
              hourly fallback included) for the DEEP set: the holdings, every
              Top-10 board name of the last 30 days, the 50 investable names
              (v_universe_latest.meets_floors) with the highest expected
              12-month ROI, the Market_Leaders page and --extra symbols, at
              most 600. symbol_bars_daily is replaced as a whole (WRITE_TRUNCATE,
              clustered by symbol) only when at least MIN_BARS_SHARE of the set
              answered; otherwise yesterday's table stays. Every other symbol
              still gets one candle per trading session from the archived
              market pages (v_symbol_candles).
  2. analyst: EODHD fundamentals, filter AnalystRatings (consensus 1-5, target
              price, Strong Buy / Buy / Hold / Sell / Strong Sell counts) for
              the CORE set: the holdings, the Top-10 names of the last 30 days,
              the 25 best investable names and --extra, at most 120; one
              partition per day in symbol_analyst_daily.
  3. news:    EODHD news (last NEWS_DAYS days, NEWS_LIMIT items, with the
              provider's sentiment polarity) for the core set; one partition
              per day in symbol_news.
BUDGET GUARD: the EODHD daily limit (100,000 calls) is shared with the live
sync and was exhausted once (5 Jul 2026). Before loads 2 and 3 the job reads
/api/user (free) and skips BOTH when the calls already used today plus this
run's cost (10 per fundamentals call, 5 per news call) would pass
TFB_DEEP_EODHD_MAX_SHARE (default 0.60) of the daily limit, or when the
limit cannot be read (fail closed). The provider resets its counter lazily,
so a counter dated before today (apiRequestsDate) is yesterday's and counts
as 0 (as in run_dashboard_sync; Codex review of #884). The bars never touch
EODHD.
NOTHING is written to the workbook; the live system never reads BigQuery; no
decision, gate or count depends on these tables. The token never reaches a
log line.

GATE: TFB_BQ_DEEP = on|1|true|yes arms the job (default off here - the
workflow arms it; kill with the repository Variable TFB_BQ_DEEP=off).
TFB_DEEP_EODHD = off skips loads 2 and 3 only. Without --write it is a dry
run: it reads the sources and prints what it would load; BigQuery is only
read (the dataset check and the symbol sets).
ENV: TFB_BQ_PROJECT, TFB_BQ_DATASET (default tfb_archive), TFB_BQ_LOCATION
(default US), GOOGLE_SHEETS_CREDENTIALS (the archive's service account),
EODHD_API_KEY, TFB_DEEP_EODHD_MAX_SHARE, GITHUB_RUN_ID.
CLI: --write | --dry-run (default) ; --extra A,B (TFB symbols, e.g. 2222.SR,
AAPL.US) ; --only bars|analyst|news (repeatable; default all three).
Exit: 0 loaded (or skipped by the guard / gate), 1 a load failed, under
MIN_BARS_SHARE of the deep set answered Yahoo (table kept), or more than
MAX_EODHD_FAIL_SHARE of the EODHD calls failed, 2 configuration missing.

Not a licensed financial adviser; the owner decides and executes every trade.
================================================================================
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import uuid
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import archive_market_series as ams  # same folder: the Yahoo reader and the BigQuery session

__version__ = "1.0.0"

BQ_API = "https://bigquery.googleapis.com/bigquery/v2"
BQ_UPLOAD = "https://bigquery.googleapis.com/upload/bigquery/v2"
EODHD_API = "https://eodhd.com/api"
EODHD_UA = "TFB-symbol-deep/1.0"
BARS_DAYS = 760                 # two years of sessions and a margin
DEEP_CAP = 600
CORE_CAP = 120
MIN_BARS_SHARE = 0.8            # replace symbol_bars_daily only when this share answered
MAX_EODHD_FAIL_SHARE = 0.2      # more failed EODHD calls than this share -> exit 1
NEWS_DAYS = 30
NEWS_LIMIT = 20
COST_FUNDAMENTALS = 10          # EODHD calls charged per fundamentals request
COST_NEWS = 5                   # EODHD calls charged per news request
DEFAULT_MAX_SHARE = 0.60
STAGES = ("bars", "analyst", "news")
TABLE_BARS = "symbol_bars_daily"
TABLE_ANALYST = "symbol_analyst_daily"
TABLE_NEWS = "symbol_news"

# TFB (Yahoo-style) listing suffix -> the suffix EODHD expects: the union of
# core/symbols/normalize.py _YAHOO_TO_EODHD_SUFFIX and
# core/providers/eodhd_provider.py _EODHD_SUFFIX_CANONICAL (the test reads both).
EODHD_SUFFIX = {"NS": "NSE", "BO": "BSE", "DE": "XETRA", "AX": "AU", "KS": "KO",
                "L": "LSE", "XETR": "XETRA", "ETR": "XETRA", "TASE": "TA"}

_F = "FLOAT"
SCHEMA_BARS: List[Dict[str, str]] = [
    {"name": "symbol", "type": "STRING", "mode": "REQUIRED", "description": "TFB symbol (2222.SR, AAPL.US)"},
    {"name": "yahoo_symbol", "type": "STRING", "mode": "REQUIRED", "description": "symbol asked of Yahoo"},
    {"name": "date", "type": "DATE", "mode": "REQUIRED", "description": "venue-local trading day"},
    {"name": "open", "type": _F, "mode": "NULLABLE", "description": "open"},
    {"name": "high", "type": _F, "mode": "NULLABLE", "description": "high"},
    {"name": "low", "type": _F, "mode": "NULLABLE", "description": "low"},
    {"name": "close", "type": _F, "mode": "REQUIRED", "description": "close (local currency)"},
    {"name": "adj_close", "type": _F, "mode": "NULLABLE", "description": "close adjusted for splits and dividends"},
    {"name": "volume", "type": "INTEGER", "mode": "NULLABLE", "description": "shares traded"},
    {"name": "source", "type": "STRING", "mode": "REQUIRED", "description": "yahoo_chart_v8 | yahoo_chart_v8_1h"},
    {"name": "set_reason", "type": "STRING", "mode": "REQUIRED", "description": "why the symbol is in the deep set"},
    {"name": "captured_at", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "when the job ran"},
    {"name": "run_id", "type": "STRING", "mode": "NULLABLE", "description": "GitHub Actions run id"},
]
SCHEMA_ANALYST: List[Dict[str, str]] = [
    {"name": "snapshot_date", "type": "DATE", "mode": "REQUIRED", "description": "UTC date of the read (one partition per day)"},
    {"name": "symbol", "type": "STRING", "mode": "REQUIRED", "description": "TFB symbol"},
    {"name": "eodhd_symbol", "type": "STRING", "mode": "REQUIRED", "description": "symbol asked of EODHD"},
    {"name": "rating", "type": _F, "mode": "NULLABLE", "description": "analyst consensus, 1 (strong sell) .. 5 (strong buy)"},
    {"name": "target_price", "type": _F, "mode": "NULLABLE", "description": "mean analyst target price (local currency)"},
    {"name": "strong_buy", "type": "INTEGER", "mode": "NULLABLE", "description": "analysts at Strong Buy"},
    {"name": "buy", "type": "INTEGER", "mode": "NULLABLE", "description": "analysts at Buy"},
    {"name": "hold", "type": "INTEGER", "mode": "NULLABLE", "description": "analysts at Hold"},
    {"name": "sell", "type": "INTEGER", "mode": "NULLABLE", "description": "analysts at Sell"},
    {"name": "strong_sell", "type": "INTEGER", "mode": "NULLABLE", "description": "analysts at Strong Sell"},
    {"name": "analysts", "type": "INTEGER", "mode": "NULLABLE", "description": "sum of the five counts"},
    {"name": "captured_at", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "when the job ran"},
    {"name": "run_id", "type": "STRING", "mode": "NULLABLE", "description": "GitHub Actions run id"},
]
SCHEMA_NEWS: List[Dict[str, str]] = [
    {"name": "snapshot_date", "type": "DATE", "mode": "REQUIRED", "description": "UTC date of the read (one partition per day)"},
    {"name": "symbol", "type": "STRING", "mode": "REQUIRED", "description": "TFB symbol the item was asked for"},
    {"name": "published_at", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "publication time (UTC)"},
    {"name": "title", "type": "STRING", "mode": "REQUIRED", "description": "headline (at most 300 characters)"},
    {"name": "link", "type": "STRING", "mode": "NULLABLE", "description": "article URL"},
    {"name": "domain", "type": "STRING", "mode": "NULLABLE", "description": "publisher host"},
    {"name": "polarity", "type": _F, "mode": "NULLABLE", "description": "provider sentiment -1 (negative) .. 1 (positive)"},
    {"name": "neg", "type": _F, "mode": "NULLABLE", "description": "negative share"},
    {"name": "neu", "type": _F, "mode": "NULLABLE", "description": "neutral share"},
    {"name": "pos", "type": _F, "mode": "NULLABLE", "description": "positive share"},
    {"name": "tags", "type": "STRING", "mode": "NULLABLE", "description": "provider tags, comma-separated (at most 10)"},
    {"name": "captured_at", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "when the job ran"},
    {"name": "run_id", "type": "STRING", "mode": "NULLABLE", "description": "GitHub Actions run id"},
]
TABLES = {
    TABLE_BARS: (SCHEMA_BARS, None, ["symbol"],
                 "TFB two-year daily bars (Yahoo chart API) of the deep-dive symbol set; replaced as a "
                 "whole each night by scripts/archive_symbol_deep.py."),
    TABLE_ANALYST: (SCHEMA_ANALYST, "snapshot_date", ["symbol"],
                    "TFB analyst consensus per symbol and night (EODHD AnalystRatings); one partition "
                    "per snapshot_date, written by scripts/archive_symbol_deep.py."),
    TABLE_NEWS: (SCHEMA_NEWS, "snapshot_date", ["symbol"],
                 "TFB per-symbol headlines of the last 30 days with sentiment (EODHD news); one "
                 "partition per snapshot_date, written by scripts/archive_symbol_deep.py."),
}
_TYPE_ALIASES = {"INT64": "INTEGER", "FLOAT64": "FLOAT", "BOOL": "BOOLEAN"}
_SYMBOL_RE = re.compile(r"^[A-Z0-9][A-Z0-9.\-^=]{0,24}$")


def _out(msg: str) -> None:
    sys.stderr.write(f"[symbol_deep v{__version__}] {msg}\n")


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def deep_enabled() -> bool:
    return _env("TFB_BQ_DEEP").lower() in ("on", "1", "true", "yes")


def eodhd_enabled() -> bool:
    return _env("TFB_DEEP_EODHD", "on").lower() not in ("off", "0", "false", "no")


def max_share() -> float:
    try:
        v = float(_env("TFB_DEEP_EODHD_MAX_SHARE", str(DEFAULT_MAX_SHARE)))
    except ValueError:
        return DEFAULT_MAX_SHARE
    return min(max(v, 0.0), 0.9)


def clean_symbols(text: str) -> List[str]:
    """'2222.sr, AAPL.US ,,' -> ['2222.SR', 'AAPL.US']; refuses anything that
    is not a plain market symbol."""
    out: List[str] = []
    for raw in str(text or "").split(","):
        s = raw.strip().upper()
        if not s:
            continue
        if not _SYMBOL_RE.match(s):
            raise ValueError(f"not a market symbol: {raw.strip()[:30]!r}")
        if s not in out:
            out.append(s)
    return out


def yahoo_symbol(symbol: str) -> str:
    """TFB -> Yahoo: AAPL.US -> AAPL, BRK-B.US -> BRK-B; every other listing
    already carries the Yahoo suffix (2222.SR, 7203.T, BP.L, SAP.DE)."""
    s = str(symbol or "").strip().upper()
    return s[:-3] if s.endswith(".US") else s


def eodhd_symbol(symbol: str) -> str:
    """TFB -> EODHD: AAPL.US and 2222.SR as they are, the Yahoo suffixes EODHD
    names differently translated (BP.L -> BP.LSE, SAP.DE -> SAP.XETRA)."""
    s = str(symbol or "").strip().upper()
    if "." not in s:
        return s + ".US"
    base, suf = s.rsplit(".", 1)
    return f"{base}.{EODHD_SUFFIX.get(suf, suf)}"


def build_sets(rows: Iterable[Dict[str, Any]], extra: Sequence[str],
               core_cap: int = CORE_CAP, deep_cap: int = DEEP_CAP
               ) -> Tuple[List[str], Dict[str, str]]:
    """(core, deep) from the set query's rows {symbol, why}: core = holdings,
    board, roi25, extra (in that priority, at most core_cap); deep = core then
    roi50 and leaders (at most deep_cap) as {symbol: reason}."""
    by: Dict[str, List[str]] = {}
    for r in rows:
        sym = str(r.get("symbol") or "").strip().upper()
        why = str(r.get("why") or "").strip()
        if sym and why and _SYMBOL_RE.match(sym):
            by.setdefault(why, [])
            if sym not in by[why]:
                by[why].append(sym)
    core: List[str] = []
    deep: Dict[str, str] = {}
    for why, syms in (("hold", by.get("hold", [])), ("board", by.get("board", [])),
                      ("roi25", by.get("roi25", [])), ("extra", list(extra))):
        for s in syms:
            if s not in deep and len(deep) < deep_cap:
                deep[s] = why
            if s not in core and len(core) < core_cap:
                core.append(s)
    for why in ("roi50", "leaders"):
        for s in by.get(why, []):
            if s not in deep and len(deep) < deep_cap:
                deep[s] = why
    return core, deep


SET_SQL = """
WITH hold AS (
  SELECT DISTINCT symbol FROM `{ds}.v_my_portfolio`
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `{ds}.v_my_portfolio`)
),
board AS (
  SELECT DISTINCT symbol FROM `{ds}.v_top10_board`
  WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
),
roi AS (
  SELECT symbol, ROW_NUMBER() OVER (ORDER BY expected_roi_12m_pct DESC, symbol) AS rk
  FROM `{ds}.v_universe_latest`
  WHERE meets_floors AND expected_roi_12m_pct IS NOT NULL
)
SELECT symbol, 'hold' AS why FROM hold
UNION ALL SELECT symbol, 'board' FROM board
UNION ALL SELECT symbol, 'roi25' FROM roi WHERE rk <= 25
UNION ALL SELECT symbol, 'roi50' FROM roi WHERE rk > 25 AND rk <= 50
UNION ALL SELECT symbol, 'leaders' FROM `{ds}.v_universe_latest` WHERE source_page = 'Market_Leaders'
"""


# ----------------------------------------------------------------------- EODHD
def _scrub(text: Any, token: str) -> str:
    s = str(text or "")
    return s.replace(token, "***") if token else s


def eodhd_get(path: str, params: Dict[str, Any], token: str,
              opener: Callable[..., Any], timeout: float = 30.0) -> Any:
    """GET an EODHD endpoint as JSON; errors never carry the token."""
    q = dict(params)
    q.update({"api_token": token, "fmt": "json"})
    req = urllib.request.Request(f"{EODHD_API}/{path}?{urllib.parse.urlencode(q)}",
                                 headers={"User-Agent": EODHD_UA, "Accept": "application/json"})
    try:
        with opener(req, timeout) as resp:
            return json.loads(resp.read().decode("utf-8", "replace"))
    except Exception as exc:  # noqa: BLE001
        code = getattr(exc, "code", None)
        raise RuntimeError(_scrub(f"EODHD {path.split('/')[0]}: " + (f"HTTP {code}" if code else type(exc).__name__),
                                  token)) from None


_DAY = re.compile(r"\d{4}-\d{2}-\d{2}")


def budget_check(token: str, planned: int, share: float,
                 opener: Callable[..., Any], *, today: str) -> Tuple[bool, str]:
    """(ok, why): ok only when today's used + planned <= share x the daily limit.
    EODHD resets the counter lazily (on the first request after midnight GMT),
    so at 00:20 UTC /api/user can still report yesterday's calls: a counter
    dated before `today` (UTC, YYYY-MM-DD) counts as 0 used - the rule of
    run_dashboard_sync._eodhd_quota_parse. A missing, malformed or later date
    counts as today's (fail closed)."""
    try:
        user = eodhd_get("user", {}, token, opener)
        used = int(float(user.get("apiRequests") or 0))
        limit = int(float(user.get("dailyRateLimit") or 0))
        dated = str(user.get("apiRequestsDate") or "").strip()[:10]
    except Exception as exc:  # noqa: BLE001 - fail closed
        return False, f"EODHD usage unreadable ({_scrub(exc, token)[:120]})"
    if limit <= 0:
        return False, "EODHD daily limit unknown"
    note = ""
    if _DAY.fullmatch(dated) and _DAY.fullmatch(today or "") and dated < today:
        note = f"; counter dated {dated} not reset yet, its {used} not counted"
        used = 0
    ceiling = int(share * limit)
    if used + planned > ceiling:
        return False, f"EODHD {used} + {planned} planned > {ceiling} ({share:.0%} of {limit}){note}"
    return True, f"EODHD {used} + {planned} planned <= {ceiling} ({share:.0%} of {limit}){note}"


def _int(v: Any) -> Optional[int]:
    f = ams._num(v)
    return int(f) if f is not None and f >= 0 else None


def parse_analyst(payload: Any) -> Optional[Dict[str, Any]]:
    """EODHD fundamentals?filter=AnalystRatings -> the row fields, None when
    the symbol has no coverage (empty, 'NA', no rating and no counts)."""
    if not isinstance(payload, dict):
        return None
    counts = {k: _int(payload.get(src)) for k, src in (("strong_buy", "StrongBuy"), ("buy", "Buy"),
                                                       ("hold", "Hold"), ("sell", "Sell"),
                                                       ("strong_sell", "StrongSell"))}
    rating = ams._num(payload.get("Rating"))
    if rating is not None and not 1.0 <= rating <= 5.0:
        rating = None
    target = ams._num(payload.get("TargetPrice"))
    if target is not None and not target > 0:
        target = None
    known = [v for v in counts.values() if v is not None]
    if rating is None and target is None and not any(known):
        return None
    row: Dict[str, Any] = {"rating": rating, "target_price": target}
    row.update(counts)
    row["analysts"] = sum(known) if known else None
    return row


def _ts(text: Any) -> Optional[str]:
    s = str(text or "").strip()
    if not s:
        return None
    try:
        d = _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=_dt.timezone.utc)
    return d.astimezone(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_news(payload: Any, limit: int = NEWS_LIMIT) -> List[Dict[str, Any]]:
    """EODHD news list -> rows (title required; one row per link; newest first)."""
    if not isinstance(payload, list):
        return []
    rows: List[Dict[str, Any]] = []
    seen = set()
    for it in payload:
        if not isinstance(it, dict):
            continue
        title = " ".join(str(it.get("title") or "").split())[:300]
        if not title:
            continue
        link = str(it.get("link") or "").strip()[:1000] or None
        key = link or title
        if key in seen:
            continue
        seen.add(key)
        sent = it.get("sentiment") if isinstance(it.get("sentiment"), dict) else {}
        pol = ams._num(sent.get("polarity"))
        tags = it.get("tags") if isinstance(it.get("tags"), list) else []
        rows.append({
            "published_at": _ts(it.get("date")),
            "title": title,
            "link": link,
            "domain": (urllib.parse.urlparse(link).hostname or None) if link else None,
            "polarity": pol if pol is not None and -1.0 <= pol <= 1.0 else None,
            "neg": ams._num(sent.get("neg")), "neu": ams._num(sent.get("neu")), "pos": ams._num(sent.get("pos")),
            "tags": ", ".join(str(t)[:40] for t in tags[:10]) or None,
        })
    rows.sort(key=lambda r: r["published_at"] or "", reverse=True)
    return rows[:limit]


# -------------------------------------------------------------------- BigQuery
def run_query(session: Any, project: str, location: str, sql: str) -> List[Dict[str, Any]]:
    resp = session.post(f"{BQ_API}/projects/{project}/queries", timeout=120,
                        json={"query": sql, "useLegacySql": False, "location": location,
                              "timeoutMs": 60000, "labels": {"tfb": "symbol-deep"}})
    if resp.status_code != 200:
        raise RuntimeError(f"query: {ams._err(resp)}")
    body = resp.json() or {}
    if body.get("errors"):
        raise RuntimeError("query: " + str(body["errors"][0].get("message") or "failed")[:300])
    names = [f["name"] for f in ((body.get("schema") or {}).get("fields") or [])]
    return [{n: (c or {}).get("v") for n, c in zip(names, row.get("f") or [])} for row in body.get("rows") or []]


def table_problems(table: Dict[str, Any], schema: List[Dict[str, str]], part: Optional[str]) -> List[str]:
    want = [(f["name"], f["type"], f["mode"]) for f in schema]
    got = []
    for f in ((table.get("schema") or {}).get("fields") or []):
        t = str(f.get("type") or "").upper()
        got.append((f.get("name"), _TYPE_ALIASES.get(t, t), str(f.get("mode") or "NULLABLE").upper()))
    problems = []
    if got != want:
        problems.append("columns differ from the schema")
    if part and (table.get("timePartitioning") or {}).get("field") != part:
        problems.append(f"not DAY-partitioned on {part}")
    return problems


def ensure_table(session: Any, project: str, dataset: str, table: str) -> bool:
    """True when created now; RuntimeError when it exists with another layout."""
    schema, part, cluster, desc = TABLES[table]
    base = f"{BQ_API}/projects/{project}/datasets/{dataset}/tables"
    resp = session.get(f"{base}/{table}", timeout=60)
    if resp.status_code == 200:
        problems = table_problems(resp.json() or {}, schema, part)
        if problems:
            raise RuntimeError(f"table {table}: {' | '.join(problems)}")
        return False
    if resp.status_code != 404:
        raise RuntimeError(f"table {table}: {ams._err(resp)}")
    body: Dict[str, Any] = {
        "tableReference": {"projectId": project, "datasetId": dataset, "tableId": table},
        "schema": {"fields": schema}, "clustering": {"fields": cluster}, "description": desc,
    }
    if part:
        body["timePartitioning"] = {"type": "DAY", "field": part}
    resp = session.post(base, json=body, timeout=60)
    if resp.status_code not in (200, 201):
        if resp.status_code == 409:
            return False
        raise RuntimeError(f"create {table}: {ams._err(resp)}")
    return True


def load(session: Any, project: str, dataset: str, location: str, table: str, day: Optional[str],
         rows: List[Dict[str, Any]], run_id: str, poll_s: float = 2.0, timeout_s: float = 600.0,
         sleep: Callable[[float], None] = time.sleep,
         clock: Callable[[], float] = time.monotonic) -> int:
    """WRITE_TRUNCATE `rows` into table (day=None: the whole table) or into
    its day partition table$YYYYMMDD; the number of rows loaded."""
    schema = TABLES[table][0]
    target = table if day is None else f"{table}${day.replace('-', '')}"
    job_id = "tfb_deep_%s_%s_%s" % (table, re.sub(r"[^0-9A-Za-z_-]", "", run_id or "local") or "local",
                                     uuid.uuid4().hex[:8])
    data = ams.ndjson(rows)
    config = {
        "jobReference": {"projectId": project, "jobId": job_id, "location": location},
        "configuration": {"load": {
            "destinationTable": {"projectId": project, "datasetId": dataset, "tableId": target},
            "sourceFormat": "NEWLINE_DELIMITED_JSON", "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_NEVER", "schema": {"fields": schema}, "maxBadRecords": 0,
        }, "labels": {"tfb": "symbol-deep"}},
    }
    start = session.post(f"{BQ_UPLOAD}/projects/{project}/jobs?uploadType=resumable", json=config, timeout=60,
                         headers={"X-Upload-Content-Type": "application/octet-stream",
                                  "X-Upload-Content-Length": str(len(data))})
    if start.status_code not in (200, 201):
        raise RuntimeError(f"load {table}: start {ams._err(start)}")
    headers = getattr(start, "headers", {}) or {}
    url = headers.get("Location") or headers.get("location")
    if not url:
        raise RuntimeError(f"load {table}: no upload session")
    put = session.put(url, data=data, timeout=600, headers={"Content-Type": "application/octet-stream"})
    if put.status_code not in (200, 201):
        raise RuntimeError(f"load {table}: upload {ams._err(put)}")
    deadline = clock() + timeout_s
    job = put.json() or {}
    while True:
        status = job.get("status") or {}
        if status.get("state") == "DONE":
            if status.get("errorResult"):
                raise RuntimeError(f"load {table}: " + str(status["errorResult"].get("message", "failed"))[:300])
            return int(((job.get("statistics") or {}).get("load") or {}).get("outputRows") or len(rows))
        if clock() > deadline:
            raise RuntimeError(f"load {table}: job {job_id} not done after {int(timeout_s)}s")
        sleep(poll_s)
        resp = session.get(f"{BQ_API}/projects/{project}/jobs/{job_id}?location={location}", timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"load {table}: poll {ams._err(resp)}")
        job = resp.json() or {}


# ------------------------------------------------------------------------ run
def collect_bars(deep: Dict[str, str], now: _dt.datetime, captured_at: str, run_id: str,
                 fetch: Callable[..., List[Dict[str, Any]]], sleep: Callable[[float], None],
                 pause: float = 0.25) -> Tuple[List[Dict[str, Any]], List[str]]:
    now_ts = int(now.timestamp())
    p1, p2 = int((now - _dt.timedelta(days=BARS_DAYS)).timestamp()), now_ts + 86400
    rows: List[Dict[str, Any]] = []
    missing: List[str] = []
    for sym, why in deep.items():
        ysym = yahoo_symbol(sym)
        try:
            bars = fetch(ysym, p1, p2, now_ts)
        except Exception:  # noqa: BLE001 - one symbol never stops the others
            bars = []
        if not bars:
            missing.append(sym)
        for b in bars:
            rows.append({"symbol": sym, "yahoo_symbol": ysym, "date": b["date"], "open": b.get("open"),
                         "high": b.get("high"), "low": b.get("low"), "close": b["close"],
                         "adj_close": b.get("adj_close"), "volume": b.get("volume"),
                         "source": b.get("source") or "yahoo_chart_v8", "set_reason": why,
                         "captured_at": captured_at, "run_id": run_id or None})
        sleep(pause)
    return rows, missing


def collect_eodhd(core: Sequence[str], token: str, day: str, captured_at: str, run_id: str,
                  stages: Sequence[str], opener: Callable[..., Any], sleep: Callable[[float], None],
                  pause: float = 0.3) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    analyst: List[Dict[str, Any]] = []
    news: List[Dict[str, Any]] = []
    failed: List[str] = []
    since = (_dt.date.fromisoformat(day) - _dt.timedelta(days=NEWS_DAYS)).isoformat()
    base = {"snapshot_date": day, "captured_at": captured_at, "run_id": run_id or None}
    for sym in core:
        esym = eodhd_symbol(sym)
        if "analyst" in stages:
            try:
                row = parse_analyst(eodhd_get(f"fundamentals/{urllib.parse.quote(esym)}",
                                              {"filter": "AnalystRatings"}, token, opener))
                if row:
                    analyst.append(dict(base, symbol=sym, eodhd_symbol=esym, **row))
            except Exception as exc:  # noqa: BLE001
                failed.append(f"{sym} analyst ({_scrub(exc, token)[:60]})")
            sleep(pause)
        if "news" in stages:
            try:
                items = parse_news(eodhd_get("news", {"s": esym, "from": since, "limit": NEWS_LIMIT, "offset": 0},
                                             token, opener))
                news.extend(dict(base, symbol=sym, **it) for it in items)
            except Exception as exc:  # noqa: BLE001
                failed.append(f"{sym} news ({_scrub(exc, token)[:60]})")
            sleep(pause)
    return analyst, news, failed


def run(session: Any, *, project: str, dataset: str, location: str, extra: Sequence[str],
        stages: Sequence[str], now: _dt.datetime, run_id: str, write: bool, token: str,
        fetch: Callable[..., List[Dict[str, Any]]] = ams.fetch_series,
        opener: Callable[..., Any] = ams._default_opener,
        sleep: Callable[[float], None] = time.sleep) -> int:
    try:
        if not ams.dataset_exists(session, project, dataset):
            _out(f"ERROR: dataset {project}.{dataset} not found")
            return 2
        rows = run_query(session, project, location, SET_SQL.format(ds=f"{project}.{dataset}"))
    except Exception as exc:  # noqa: BLE001
        _out(f"ERROR: {str(exc)[:400]}")
        return 2
    core, deep = build_sets(rows, extra)
    day = now.astimezone(_dt.timezone.utc).strftime("%Y-%m-%d")
    captured_at = now.astimezone(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    _out(f"sets: core {len(core)} (holdings, board, top-25 ROI, extra), deep {len(deep)}")
    rc = 0
    if "bars" in stages:
        bars, missing = collect_bars(deep, now, captured_at, run_id, fetch, sleep)
        answered = len(deep) - len(missing)
        share = answered / len(deep) if deep else 0.0
        msg = f"bars: {answered}/{len(deep)} symbols, {len(bars)} rows"
        if missing:   # a few unknown listings are normal; the share below decides
            msg += f"; no bars: {', '.join(missing[:15])}{' ...' if len(missing) > 15 else ''}"
        if not write:
            _out(msg + " | dry run")
        elif share < MIN_BARS_SHARE:
            _out(msg + f" | NOT loaded: under {MIN_BARS_SHARE:.0%} answered, yesterday's table kept")
            rc = 1
        else:
            try:
                ensure_table(session, project, dataset, TABLE_BARS)
                _out(msg + f" | loaded {load(session, project, dataset, location, TABLE_BARS, None, bars, run_id, sleep=sleep)}")
            except Exception as exc:  # noqa: BLE001
                _out(f"ERROR bars: {str(exc)[:300]}")
                rc = 1
    eod = [s for s in ("analyst", "news") if s in stages]
    if eod:
        if not eodhd_enabled():
            _out("analyst/news: TFB_DEEP_EODHD is off - skipped")
        elif not token:
            _out("analyst/news: EODHD_API_KEY missing - skipped")
            rc = max(rc, 1)
        else:
            planned = len(core) * ((COST_FUNDAMENTALS if "analyst" in eod else 0) + (COST_NEWS if "news" in eod else 0))
            ok, why = budget_check(token, planned, max_share(), opener, today=day)
            if not ok:
                _out(f"analyst/news: skipped by the budget guard - {why}")
            else:
                _out(f"analyst/news: {why}")
                analyst, news, failed = collect_eodhd(core, token, day, captured_at, run_id, eod, opener, sleep)
                if failed:
                    _out(f"analyst/news: {len(failed)} call(s) failed: {'; '.join(failed[:8])}")
                    calls = len(core) * len(eod)
                    if calls and len(failed) > MAX_EODHD_FAIL_SHARE * calls:
                        rc = 1
                for table, stage, data in ((TABLE_ANALYST, "analyst", analyst), (TABLE_NEWS, "news", news)):
                    if stage not in eod:
                        continue
                    msg = f"{stage}: {len(data)} rows for {len(core)} symbols"
                    if not write:
                        _out(msg + " | dry run")
                        continue
                    try:
                        ensure_table(session, project, dataset, table)
                        _out(msg + f" | loaded {load(session, project, dataset, location, table, day, data, run_id, sleep=sleep)}")
                    except Exception as exc:  # noqa: BLE001
                        _out(f"ERROR {stage}: {_scrub(exc, token)[:300]}")
                        rc = 1
    print(f"[symbol_deep v{__version__}] done ({'write' if write else 'dry run'}), exit {rc}")
    return rc


def main(argv: Optional[Sequence[str]] = None, session: Any = None, now: Optional[_dt.datetime] = None,
         fetch: Optional[Callable[..., List[Dict[str, Any]]]] = None,
         opener: Optional[Callable[..., Any]] = None,
         sleep: Callable[[float], None] = time.sleep) -> int:
    ap = argparse.ArgumentParser(description="TFB symbol deep data -> BigQuery")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true", help="load into BigQuery")
    mode.add_argument("--dry-run", action="store_true", help="read the sources and report only (default)")
    ap.add_argument("--extra", default="", help="comma-separated TFB symbols added to the core set")
    ap.add_argument("--only", action="append", choices=STAGES, help="run only these loads (repeatable)")
    args = ap.parse_args(list(argv) if argv is not None else None)
    if not deep_enabled():
        _out("TFB_BQ_DEEP is off - nothing read, nothing written")
        return 0
    project = _env("TFB_BQ_PROJECT")
    dataset = _env("TFB_BQ_DATASET", "tfb_archive")
    location = _env("TFB_BQ_LOCATION", "US")
    if not project:
        _out("ERROR: TFB_BQ_PROJECT is required")
        return 2
    try:
        extra = clean_symbols(args.extra)
    except ValueError as exc:
        _out(f"ERROR: {exc}")
        return 2
    if session is None:
        try:
            session = ams.make_session()
        except Exception as exc:  # noqa: BLE001
            _out(f"ERROR: {str(exc)[:300]}")
            return 2
    return run(session, project=project, dataset=dataset, location=location, extra=extra,
               stages=tuple(args.only or STAGES), now=now or _dt.datetime.now(_dt.timezone.utc),
               run_id=_env("GITHUB_RUN_ID"), write=bool(args.write), token=_env("EODHD_API_KEY"),
               fetch=fetch or ams.fetch_series, opener=opener or ams._default_opener, sleep=sleep)


if __name__ == "__main__":
    sys.exit(main())
