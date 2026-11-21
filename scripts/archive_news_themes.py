#!/usr/bin/env python3
# scripts/archive_news_themes.py
"""
================================================================================
TFB News Themes - v1.1.0 (news learning loop, phase A: the daily theme archive)
================================================================================
v1.1.0 (21 Nov 2026) - [PLACES / PEOPLE] three more sources, so the news can be
read market by market and person by person. WHY (owner, 21 Nov 2026 11:03
Riyadh, item 5 of the build list: "News: a news reading per market; forward
event scenarios; momentum/surprise; influential figures of 19 Nov; score every
news signal"; the people were part 4 of the 19 Nov stage, left out of News
Board 1.0.0):
  gdelt_events_places  the share of the day's events that are fights (CAMEO
                       18-20) located (ActionGeo_CountryCode, FIPS 10-4) in
                       each place of PLACES: fight_<place>.
  gdelt_gkg_places     the share of the day's documents that name a place of
                       PLACES (the country field of the V1 Locations list) and
                       their mean tone: place_<place>_share, place_<place>_tone.
  gdelt_gkg_people     the share of the day's documents that name a person of
                       PEOPLE (the V1 Persons list, lower case) and their mean
                       tone: person_<id>_share, person_<id>_tone. Every name
                       was read in GDELT before it was listed (Macron is not:
                       GDELT's Persons list almost never holds him); the
                       holders of each office were checked on 21 Nov 2026
                       (Fed chair Kevin Warsh since 22 May 2026; Iran's leader
                       Mojtaba Khamenei since 8 Mar 2026; Netanyahu caretaker
                       and Eisenkot asked to form a government, 7 Nov 2026;
                       Lagarde leaves the ECB on 7 Feb 2027).
  The three read only Locations, Persons, V2Tone and the events' place and
  root code - about 0.11 GB a day; the three-year fill about 125 GB. The
  existing sources are unchanged, row for row. Views v_market_news,
  v_people_news, v_news_trial and v_news_trial_summary (apply_bq_views
  1.20.0) read them; nothing live does.
v1.0.0 (19 Nov 2026)
WHY (owner, 19 Nov 2026: world news - wars, politics, the economy, the fear of
AI, elections, summits - is part of valuing any stock, so classify it, weight
it by kind, test the weights on the past and let them correct themselves; then
13:06 Riyadh, verbatim: "Let's Start build the new news and it's effect in the
file by the best way that you propse and before doing that make more final
test then apply the changes by the best possible and effective way"). The
final test of that day (three years of GDELT against the US sector ETFs, no
look-ahead) found no news weight that beat chance yet, so the loop is built to
LEARN and to be MEASURED, not to move a recommendation: this job keeps the
daily history the weights are learned from; views v_news_theme_weekly,
v_news_sector_weights, v_news_sector_tilt, v_news_overlay_list and
v_trial_news_overlay (apply_bq_views 1.14.0) do the learning and the scoring.

WHAT: for each UTC day of the window the job asks BigQuery to count, in the
public GDELT tables, the events and news documents of each theme in FEATURES
and MERGEs one row per day and theme into <dataset>.news_theme_daily:
  gdelt_events  gdelt-bq.gdeltv2.events_partitioned, by the day an event was
                ADDED (DATEADDED), events dated that day or the day before
                only (98.7 % of them; "a week / a month / a year ago" echoes
                are left out), so a day never holds later news:
                iran_war, russia_war (fights and assaults, CAMEO 18-20, with
                Iran / Russia on either side, share of all events),
                us_china_coercion (CAMEO 16-17 between the US and China),
                us_china_tone (mean Goldstein scale of US-China events).
  gdelt_gkg     gdelt-bq.gdeltv2.gkg_partitioned, by publication day: the
                share of documents carrying a theme - fed_rates, trade_policy,
                uncertainty, election, ai_tech, inflation, oil_price,
                armed_conflict - and the mean tone of two of them (ai_tone,
                market_tone).
A 'share' row is num = matching, den = all of the day; a 'mean' row is num =
the sum, den = the count - so a week is SUM(num) / SUM(den), exact. The MERGE
is one atomic statement per source: a day loaded twice is the same row.
NOTHING is written to the workbook; the live system never reads BigQuery; no
decision, gate or count depends on this table.

COST GUARD: every source is first sent as a BigQuery dry run; a window that
would read more than --max-gb is refused, and the MERGE itself carries
maximumBytesBilled. The daily window (3 days) reads under 1 GB; the one-off
history fill (--start 2023-10-02) reads about 240 GB and needs --max-gb 300.
v1.1.0's three sources add about 0.35 GB to the daily window; their own fill
from 2023-10-02 reads about 125 GB (dry runs of 21 Nov 2026: gdelt_gkg_places
79, gdelt_gkg_people 43, gdelt_events_places 3) and needs --max-gb 100 with
--sources gdelt_events_places,gdelt_gkg_places,gdelt_gkg_people.

GATE: TFB_BQ_NEWS_THEMES = on|1|true|yes arms the job (default off here - the
workflow arms it; kill with the repository Variable TFB_BQ_NEWS_THEMES=off).
Without --write it is a dry run: it sizes the queries and loads nothing.
ENV: TFB_BQ_PROJECT, TFB_BQ_DATASET (default tfb_archive), TFB_BQ_LOCATION
(default US), GOOGLE_SHEETS_CREDENTIALS (the archive's service account:
BigQuery Data Editor + Job User), GITHUB_RUN_ID.
CLI: --write | --dry-run (default) ; --days N (default 3, 1..30: the window
ends yesterday UTC) ; --start YYYY-MM-DD [--end YYYY-MM-DD] (a history fill;
replaces --days) ; --max-gb G (default 5, 1..400) ; --sources a,b (a subset
of gdelt_events, gdelt_gkg, gdelt_events_places, gdelt_gkg_places,
gdelt_gkg_people).
Exit: 0 every source loaded (or sized, in a dry run), 1 one or more sources
failed (the others are loaded), 2 configuration missing, a window refused or
the table differs from SCHEMA.

Not a licensed financial adviser; the owner decides and executes every trade.
================================================================================
"""
from __future__ import annotations

import argparse
import datetime as _dt
import importlib.util
import os
import re
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

__version__ = "1.1.0"

TABLE = "news_theme_daily"
GDELT_EVENTS = "gdelt-bq.gdeltv2.events_partitioned"
GDELT_GKG = "gdelt-bq.gdeltv2.gkg_partitioned"
SOURCES = ("gdelt_events", "gdelt_gkg", "gdelt_events_places", "gdelt_gkg_places", "gdelt_gkg_people")
KINDS = ("share", "mean")
DEFAULT_DAYS = 3
MIN_DAYS, MAX_DAYS = 1, 30
MIN_DATE = _dt.date(2017, 1, 1)
DEFAULT_MAX_GB = 5.0
MIN_MAX_GB, MAX_MAX_GB = 1.0, 400.0
GIB = 1 << 30
BQ_API = "https://bigquery.googleapis.com/bigquery/v2"

SCHEMA: List[Dict[str, str]] = [
    {"name": "date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "feature", "type": "STRING", "mode": "REQUIRED"},
    {"name": "source", "type": "STRING", "mode": "REQUIRED"},
    {"name": "kind", "type": "STRING", "mode": "REQUIRED"},
    {"name": "num", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "den", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "loaded_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "run_id", "type": "STRING", "mode": "NULLABLE"},
]
_TYPE_ALIASES = {"INT64": "INTEGER", "FLOAT64": "FLOAT", "BOOL": "BOOLEAN"}

# ---------------------------------------------------------------- the themes
_FIGHT = "EventRootCode IN ('18', '19', '20')"
_US_CN = ("((Actor1CountryCode = 'USA' AND Actor2CountryCode = 'CHN') "
          "OR (Actor1CountryCode = 'CHN' AND Actor2CountryCode = 'USA'))")
_TONE = "SAFE_CAST(SPLIT(V2Tone, ',')[SAFE_OFFSET(0)] AS FLOAT64)"


def _theme(codes: str) -> str:
    """A GKG document carries one of the exact theme codes (a whole token of
    the ';'-separated Themes list - '_' is a word character, so
    ELECTION never matches ELECTION_FRAUD or TAX_FNCACT_ELECTION)."""
    return "REGEXP_CONTAINS(Themes, r'\\b(%s)\\b')" % codes


_AI = _theme("SOC_EMERGINGTECH|TECH_AUTOMATION")
_STOCK = _theme("ECON_STOCKMARKET")

# (feature, source, kind, numerator SQL, denominator SQL)
FEATURES: Tuple[Tuple[str, str, str, str, str], ...] = (
    ("iran_war", "gdelt_events", "share",
     f"COUNTIF({_FIGHT} AND 'IRN' IN (Actor1CountryCode, Actor2CountryCode))", "COUNT(*)"),
    ("russia_war", "gdelt_events", "share",
     f"COUNTIF({_FIGHT} AND 'RUS' IN (Actor1CountryCode, Actor2CountryCode))", "COUNT(*)"),
    ("us_china_coercion", "gdelt_events", "share",
     f"COUNTIF(EventRootCode IN ('16', '17') AND {_US_CN})", "COUNT(*)"),
    ("us_china_tone", "gdelt_events", "mean",
     f"SUM(IF({_US_CN}, GoldsteinScale, NULL))", f"COUNTIF({_US_CN} AND GoldsteinScale IS NOT NULL)"),
    ("fed_rates", "gdelt_gkg", "share",
     f"COUNTIF({_theme('ECON_CENTRALBANK|EPU_CATS_MONETARY_POLICY|ECON_INTEREST_RATES')})", "COUNT(*)"),
    ("trade_policy", "gdelt_gkg", "share", f"COUNTIF({_theme('EPU_CATS_TRADE_POLICY')})", "COUNT(*)"),
    ("uncertainty", "gdelt_gkg", "share", f"COUNTIF({_theme('EPU_UNCERTAINTY')})", "COUNT(*)"),
    ("election", "gdelt_gkg", "share", f"COUNTIF({_theme('ELECTION')})", "COUNT(*)"),
    ("ai_tech", "gdelt_gkg", "share", f"COUNTIF({_AI})", "COUNT(*)"),
    ("ai_tone", "gdelt_gkg", "mean",
     f"SUM(IF({_AI}, {_TONE}, NULL))", f"COUNTIF({_AI} AND {_TONE} IS NOT NULL)"),
    ("market_tone", "gdelt_gkg", "mean",
     f"SUM(IF({_STOCK}, {_TONE}, NULL))", f"COUNTIF({_STOCK} AND {_TONE} IS NOT NULL)"),
    ("inflation", "gdelt_gkg", "share", f"COUNTIF({_theme('ECON_INFLATION')})", "COUNT(*)"),
    ("oil_price", "gdelt_gkg", "share", f"COUNTIF({_theme('ECON_OILPRICE')})", "COUNT(*)"),
    ("armed_conflict", "gdelt_gkg", "share", f"COUNTIF({_theme('ARMEDCONFLICT')})", "COUNT(*)"),
)


# ------------------------------------------------- v1.1.0: places and people
# (id, FIPS 10-4 country codes): GDELT writes a country as its FIPS code, in
# the events (ActionGeo_CountryCode) and in the GKG Locations list alike.
PLACES: Tuple[Tuple[str, str], ...] = (
    ("us", "US"),
    ("sa", "SA"),
    ("mideast", "IR|IZ|YM|IS|LE|SY|JO|KU|QA|AE|BA|MU"),     # the war zone around Saudi Arabia and the Gulf
    ("eu", "UK|FR|GM|IT|SP|NL|SZ|SW|BE|DA|NO|FI|AU|EI|PO|PL"),
    ("jp", "JA"),
    ("cn", "CH|HK"),
)
# (id, regular expression on the lower-case V1 Persons list): first and last
# name, so a namesake (Glen Powell, Eric Trump) does not count; 'jinping'
# alone because GDELT often splits Xi off ("b jinping"); 'putin', 'khamenei',
# 'netanyahu' and 'eisenkot' alone because each is one family in the news
# (Ali and, since March 2026, Mojtaba Khamenei). The Saudi energy minister is
# written several ways ("abdulaziz bin salman", "abdulaziz al-salman").
# Emmanuel Macron is left out: GDELT's Persons list almost never holds him (3
# garbled entries in 17-19 Nov 2026, while 522 documents of 19 Nov alone name
# him in AllNames), and reading AllNames would triple this source's bytes.
PEOPLE: Tuple[Tuple[str, str], ...] = (
    ("trump", r"\bdonald (j\.? )?trump\b"),
    ("warsh", r"\bkevin warsh\b"),
    ("bessent", r"\bscott bessent\b"),
    ("musk", r"\belon musk\b"),
    ("huang", r"\bjensen huang\b"),
    ("altman", r"\bsam altman\b"),
    ("xi", r"\bjinping\b"),
    ("mbs", r"\b(mohammed|mohammad|muhammad|mohamed) (bin|ben) salman\b"),
    ("abdulaziz", r"\babdul ?aziz (bin|ben|al-?) ?salman\b"),
    ("putin", r"\bputin\b"),
    ("khamenei", r"\bkhamenei\b"),
    ("netanyahu", r"\bnetanyahu\b"),
    ("eisenkot", r"\beisenkot\b"),
    ("lagarde", r"\bchristine lagarde\b"),
    ("merz", r"\b(friedrich|frederick) merz\b"),
    ("takaichi", r"\b(sanae takaichi|takaichi sanae)\b"),
    ("ueda", r"\b(kazuo ueda|ueda kazuo)\b"),
)


def _place_doc(codes: str) -> str:
    """A GKG document names the place: one of the codes is the country field
    of a V1 location (TYPE#NAME#COUNTRY#ADM1#...)."""
    return "REGEXP_CONTAINS(Locations, r'#(%s)#')" % codes


def _person_doc(pattern: str) -> str:
    return "REGEXP_CONTAINS(Persons, r'%s')" % pattern


def _place_features() -> Tuple[Tuple[str, str, str, str, str], ...]:
    out: List[Tuple[str, str, str, str, str]] = []
    for pid, codes in PLACES:
        listed = ", ".join("'%s'" % c for c in codes.split("|"))
        out.append(("fight_%s" % pid, "gdelt_events_places", "share",
                    f"COUNTIF({_FIGHT} AND ActionGeo_CountryCode IN ({listed}))", "COUNT(*)"))
    for pid, codes in PLACES:
        doc = _place_doc(codes)
        out.append(("place_%s_share" % pid, "gdelt_gkg_places", "share", f"COUNTIF({doc})", "COUNT(*)"))
        out.append(("place_%s_tone" % pid, "gdelt_gkg_places", "mean",
                    f"SUM(IF({doc}, {_TONE}, NULL))", f"COUNTIF({doc} AND {_TONE} IS NOT NULL)"))
    for pid, pattern in PEOPLE:
        doc = _person_doc(pattern)
        out.append(("person_%s_share" % pid, "gdelt_gkg_people", "share", f"COUNTIF({doc})", "COUNT(*)"))
        out.append(("person_%s_tone" % pid, "gdelt_gkg_people", "mean",
                    f"SUM(IF({doc}, {_TONE}, NULL))", f"COUNTIF({doc} AND {_TONE} IS NOT NULL)"))
    return tuple(out)


FEATURES = FEATURES + _place_features()


def _out(msg: str) -> None:
    sys.stderr.write(f"[news_themes v{__version__}] {msg}\n")


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def themes_enabled() -> bool:
    return _env("TFB_BQ_NEWS_THEMES").lower() in ("on", "1", "true", "yes")


def features_of(source: str) -> List[Tuple[str, str, str, str, str]]:
    return [f for f in FEATURES if f[1] == source]


# ------------------------------------------------------------------- the SQL
def _day(d: _dt.date) -> str:
    if not isinstance(d, _dt.date) or isinstance(d, _dt.datetime):
        raise ValueError(f"not a date: {d!r}")
    return d.isoformat()


def source_select(source: str, start: _dt.date, end: _dt.date) -> str:
    """The SELECT that yields (date, feature, source, kind, num, den) for one
    source and the days start..end (inclusive). Dates come from date objects
    only, never from text."""
    feats = features_of(source)
    if not feats:
        raise ValueError(f"unknown source {source!r}")
    _day(start), _day(end)                               # date objects only
    if start > end:
        raise ValueError(f"start {start} after end {end}")
    aggs = ",\n      ".join(f"{num} AS n{i},\n      {den} AS d{i}" for i, (_, _, _, num, den) in enumerate(feats))
    structs = ",\n    ".join(
        ("STRUCT('%s' AS feature, '%s' AS kind, CAST(IFNULL(n%d, 0) AS FLOAT64) AS num, CAST(d%d AS FLOAT64) AS den)"
         if i == 0 else "('%s', '%s', CAST(IFNULL(n%d, 0) AS FLOAT64), CAST(d%d AS FLOAT64))") % (name, kind, i, i)
        for i, (name, _, kind, _, _) in enumerate(feats))
    if source.startswith("gdelt_events"):                   # v1.1.0: gdelt_events_places reads the same table
        added = "PARSE_DATE('%Y%m%d', CAST(DIV(DATEADDED, 1000000) AS STRING))"
        lo = _day(start - _dt.timedelta(days=1))
        inner = (
            f"    SELECT\n      {added} AS date,\n      {aggs}\n"
            f"    FROM `{GDELT_EVENTS}`\n"
            f"    WHERE _PARTITIONTIME >= TIMESTAMP('{lo}') AND _PARTITIONTIME <= TIMESTAMP('{_day(end)}')\n"
            f"      AND DATEADDED >= {start.strftime('%Y%m%d')}000000 AND DATEADDED <= {end.strftime('%Y%m%d')}235959\n"
            f"      AND DATE_DIFF({added}, DATE(_PARTITIONTIME), DAY) BETWEEN 0 AND 1\n"
            f"    GROUP BY date")
    else:
        inner = (
            f"    SELECT\n      DATE(_PARTITIONTIME) AS date,\n      {aggs}\n"
            f"    FROM `{GDELT_GKG}`\n"
            f"    WHERE _PARTITIONTIME >= TIMESTAMP('{_day(start)}') AND _PARTITIONTIME <= TIMESTAMP('{_day(end)}')\n"
            f"    GROUP BY date")
    return (
        f"SELECT g.date, f.feature, '{source}' AS source, f.kind, f.num, f.den\n"
        f"FROM (\n{inner}\n  ) AS g,\n  UNNEST([\n    {structs}]) AS f\n"
        f"WHERE g.date BETWEEN DATE '{_day(start)}' AND DATE '{_day(end)}'")


def merge_sql(project: str, dataset: str, source: str, start: _dt.date, end: _dt.date, run_id: str) -> str:
    """One atomic statement: the window's rows of one source replace the rows
    of the same day and theme, and new days are added."""
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", project or ""):
        raise ValueError(f"bad project id {project!r}")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,1023}", dataset or ""):
        raise ValueError(f"bad dataset id {dataset!r}")
    rid = re.sub(r"[^0-9A-Za-z_-]", "", run_id or "")[:64]
    return (
        f"MERGE `{project}.{dataset}.{TABLE}` AS t\nUSING (\n{source_select(source, start, end)}\n) AS s\n"
        f"ON t.date = s.date AND t.feature = s.feature\n"
        f"WHEN MATCHED THEN UPDATE SET source = s.source, kind = s.kind, num = s.num, den = s.den,\n"
        f"  loaded_at = CURRENT_TIMESTAMP(), run_id = '{rid}'\n"
        f"WHEN NOT MATCHED THEN INSERT (date, feature, source, kind, num, den, loaded_at, run_id)\n"
        f"  VALUES (s.date, s.feature, s.source, s.kind, s.num, s.den, CURRENT_TIMESTAMP(), '{rid}')")


def window(today: _dt.date, days: int, start: str = "", end: str = "") -> Tuple[_dt.date, _dt.date]:
    """The days to load. Never today (a UTC day still running is not final);
    --start/--end replace --days."""
    last = today - _dt.timedelta(days=1)
    if start:
        try:
            d1 = _dt.date.fromisoformat(start)
            d2 = _dt.date.fromisoformat(end) if end else last
        except ValueError as exc:
            raise ValueError(f"dates must be YYYY-MM-DD: {exc}") from None
    elif end:
        raise ValueError("--end needs --start")
    else:
        if not (MIN_DAYS <= int(days) <= MAX_DAYS):
            raise ValueError(f"--days must be {MIN_DAYS}..{MAX_DAYS}, got {days}")
        d1, d2 = last - _dt.timedelta(days=int(days) - 1), last
    if d2 > last:
        raise ValueError(f"the window must end by yesterday UTC ({last}), got {d2}")
    if d1 > d2:
        raise ValueError(f"start {d1} after end {d2}")
    if d1 < MIN_DATE:
        raise ValueError(f"start {d1} before {MIN_DATE}")
    return d1, d2


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
        msg = (((resp.json() or {}).get("error") or {}).get("message") or "")
    except Exception:
        msg = ""
    return f"HTTP {getattr(resp, 'status_code', '?')}" + (f": {msg[:300]}" if msg else "")


def dataset_exists(session: Any, project: str, dataset: str) -> bool:
    resp = session.get(f"{BQ_API}/projects/{project}/datasets/{dataset}", timeout=60)
    if resp.status_code == 200:
        return True
    if resp.status_code == 404:
        return False
    raise RuntimeError(f"dataset {project}.{dataset}: {_err(resp)}")


def schema_problems(table: Dict[str, Any]) -> List[str]:
    """[] when an existing table has SCHEMA's columns (name, type, mode, in order)."""
    want = [(f["name"], f["type"], f["mode"]) for f in SCHEMA]
    got = []
    for f in ((table.get("schema") or {}).get("fields") or []):
        t = str(f.get("type") or "").upper()
        got.append((f.get("name"), _TYPE_ALIASES.get(t, t), str(f.get("mode") or "NULLABLE").upper()))
    if got == want:
        return []
    diff = [f"{w} != {g}" for w, g in zip(want, got) if w != g]
    return ["columns differ: " + ("; ".join(diff[:5]) or f"{len(got)} columns, want {len(want)}")]


def ensure_table(session: Any, project: str, dataset: str) -> bool:
    """True when the table was created now, False when it existed with the
    right schema; RuntimeError when it exists with another layout."""
    base = f"{BQ_API}/projects/{project}/datasets/{dataset}/tables"
    resp = session.get(f"{base}/{TABLE}", timeout=60)
    if resp.status_code == 200:
        problems = schema_problems(resp.json() or {})
        if problems:
            raise RuntimeError(f"table {TABLE} differs from SCHEMA: {' | '.join(problems)}")
        return False
    if resp.status_code != 404:
        raise RuntimeError(f"table {TABLE}: {_err(resp)}")
    body = {
        "tableReference": {"projectId": project, "datasetId": dataset, "tableId": TABLE},
        "schema": {"fields": SCHEMA},
        "description": ("TFB daily world-news themes counted in the public GDELT tables (events and "
                        "documents): one row per UTC day and theme with a numerator and a denominator, "
                        "merged by scripts/archive_news_themes.py."),
    }
    resp = session.post(base, json=body, timeout=60)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"create {TABLE}: {_err(resp)}")
    return True


def dry_run_bytes(session: Any, project: str, location: str, sql: str) -> int:
    """The bytes a query would read, from a BigQuery dry run (nothing runs)."""
    payload = {"query": sql, "useLegacySql": False, "location": location, "dryRun": True,
               "labels": {"tfb": "news-themes"}}
    resp = session.post(f"{BQ_API}/projects/{project}/queries", json=payload, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"dry run: {_err(resp)}")
    return int((resp.json() or {}).get("totalBytesProcessed") or 0)


def run_merge(session: Any, project: str, location: str, source: str, sql: str, max_bytes: int, run_id: str,
              poll_s: float = 3.0, timeout_s: float = 1500.0,
              sleep: Callable[[float], None] = time.sleep,
              clock: Callable[[], float] = time.monotonic) -> Dict[str, Any]:
    """Run one MERGE as a query job capped at max_bytes and wait for it."""
    job_id = "tfb_news_%s_%s_%s" % (source, re.sub(r"[^0-9A-Za-z_-]", "", run_id or "local") or "local",
                                    uuid.uuid4().hex[:8])
    config = {
        "jobReference": {"projectId": project, "jobId": job_id, "location": location},
        "configuration": {"query": {"query": sql, "useLegacySql": False,
                                    "maximumBytesBilled": str(int(max_bytes))},
                          "labels": {"tfb": "news-themes"}},
    }
    resp = session.post(f"{BQ_API}/projects/{project}/jobs", json=config, timeout=120)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"{source}: start {_err(resp)}")
    deadline = clock() + timeout_s
    job = resp.json() or {}
    while True:
        status = job.get("status") or {}
        if status.get("state") == "DONE":
            if status.get("errorResult"):
                raise RuntimeError(f"{source}: {status['errorResult'].get('message', 'job failed')[:300]}")
            return job
        if clock() > deadline:
            raise RuntimeError(f"{source}: job {job_id} not done after {int(timeout_s)}s")
        sleep(poll_s)
        resp = session.get(f"{BQ_API}/projects/{project}/jobs/{job_id}?location={location}", timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"{source}: poll {_err(resp)}")
        job = resp.json() or {}


def run(session: Any, *, project: str, dataset: str, location: str, sources: Sequence[str],
        start: _dt.date, end: _dt.date, max_gb: float, run_id: str, write: bool,
        sleep: Callable[[float], None] = time.sleep) -> int:
    try:
        if not dataset_exists(session, project, dataset):
            _out(f"ERROR: dataset {project}.{dataset} not found")
            return 2
    except Exception as exc:  # noqa: BLE001
        _out(f"ERROR: {str(exc)[:400]}")
        return 2
    max_bytes = int(max_gb * GIB)
    sized: List[Tuple[str, int]] = []
    for source in sources:
        try:
            got = dry_run_bytes(session, project, location, source_select(source, start, end))
        except Exception as exc:  # noqa: BLE001
            _out(f"ERROR: {source}: {str(exc)[:400]}")
            return 2
        _out(f"{source}: {start}..{end} would read {got / GIB:.2f} GB")
        if got > max_bytes:
            _out(f"ERROR: {source} would read {got / GIB:.1f} GB, over --max-gb {max_gb:g}: nothing loaded "
                 f"(a history fill needs a larger --max-gb)")
            return 2
        sized.append((source, got))
    total_gb = sum(b for _, b in sized) / GIB
    if not write:
        print(f"[news_themes v{__version__}] {len(sized)} source(s), {start}..{end}, would read "
              f"{total_gb:.2f} GB (dry run, nothing loaded)")
        return 0
    try:
        created = ensure_table(session, project, dataset)
    except Exception as exc:  # noqa: BLE001
        _out(f"ERROR: {str(exc)[:400]}")
        return 2
    failed: List[str] = []
    rows = 0
    for source, _ in sized:
        try:
            job = run_merge(session, project, location, source,
                            merge_sql(project, dataset, source, start, end, run_id), max_bytes, run_id, sleep=sleep)
            q = ((job.get("statistics") or {}).get("query") or {})
            n = int(q.get("numDmlAffectedRows") or 0)
            rows += n
            _out(f"{source}: {n} row(s) merged, {int(q.get('totalBytesBilled') or 0) / GIB:.2f} GB billed")
        except Exception as exc:  # noqa: BLE001 - one source never stops the other
            failed.append(source)
            _out(f"ERROR {str(exc)[:400]}")
    print(f"[news_themes v{__version__}] {len(sized) - len(failed)}/{len(sized)} source(s), {rows} row(s), "
          f"{start}..{end}" + (" | table created" if created else "")
          + (f"; FAILED: {', '.join(failed)}" if failed else ""))
    return 1 if failed else 0


def main(argv: Optional[Sequence[str]] = None, session: Any = None,
         today: Optional[_dt.date] = None,
         sleep: Callable[[float], None] = time.sleep) -> int:
    ap = argparse.ArgumentParser(description="TFB news themes (GDELT) -> BigQuery")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true", help="merge into BigQuery")
    mode.add_argument("--dry-run", action="store_true", help="size the queries and report only (default)")
    ap.add_argument("--days", type=int, default=DEFAULT_DAYS, help=f"days ending yesterday UTC ({MIN_DAYS}..{MAX_DAYS})")
    ap.add_argument("--start", default="", help="history fill: first day, YYYY-MM-DD")
    ap.add_argument("--end", default="", help="history fill: last day, YYYY-MM-DD (default yesterday UTC)")
    ap.add_argument("--max-gb", type=float, default=DEFAULT_MAX_GB, help="refuse a source that would read more")
    ap.add_argument("--sources", default="", help="comma-separated subset of " + ", ".join(SOURCES))
    args = ap.parse_args(list(argv) if argv is not None else None)
    if not themes_enabled():
        _out("TFB_BQ_NEWS_THEMES is off - nothing read, nothing written")
        return 0
    project = _env("TFB_BQ_PROJECT")
    dataset = _env("TFB_BQ_DATASET", "tfb_archive")
    location = _env("TFB_BQ_LOCATION", "US")
    if not project:
        _out("ERROR: TFB_BQ_PROJECT is required")
        return 2
    if not (MIN_MAX_GB <= float(args.max_gb) <= MAX_MAX_GB):
        _out(f"ERROR: --max-gb must be {MIN_MAX_GB:g}..{MAX_MAX_GB:g}, got {args.max_gb}")
        return 2
    sources = [s.strip() for s in args.sources.split(",") if s.strip()] or list(SOURCES)
    unknown = [s for s in sources if s not in SOURCES]
    if unknown:
        _out(f"ERROR: unknown source(s): {', '.join(unknown)} (known: {', '.join(SOURCES)})")
        return 2
    sources = [s for s in SOURCES if s in sources]
    today = today or _dt.datetime.now(_dt.timezone.utc).date()
    try:
        start, end = window(today, int(args.days), args.start.strip(), args.end.strip())
        merge_sql(project, dataset, sources[0], start, end, "")       # refuses a bad project or dataset id early
    except ValueError as exc:
        _out(f"ERROR: {exc}")
        return 2
    if session is None:
        try:
            session = make_session()
        except Exception as exc:  # noqa: BLE001
            _out(f"ERROR: {str(exc)[:300]}")
            return 2
    return run(session, project=project, dataset=dataset, location=location, sources=sources,
               start=start, end=end, max_gb=float(args.max_gb), run_id=_env("GITHUB_RUN_ID"),
               write=bool(args.write), sleep=sleep)


if __name__ == "__main__":
    sys.exit(main())
