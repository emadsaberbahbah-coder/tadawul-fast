"""scripts/archive_news_themes.py v1.1.0 - the daily world-news themes the
news learning loop is taught from (owner request 19 Nov 2026), and since
v1.1.0 the news by place and by person (owner, 21 Nov 2026, item 5).

REAL module; the only stub is the HTTP session (an AuthorizedSession-like fake
answering BigQuery datasets, tables, dry-run queries and query jobs offline).
No network, no google libraries.

N1  FEATURES: the 14 themes and v1.1.0's 52 place and person features are
    pinned by name, source and kind; a GKG theme is matched as a whole code
    of the Themes list
N2  source_select: the events source is bucketed by the day an event was
    ADDED, reads the partitions from the day before the window, keeps events
    dated that day or the day before only and never reads past the window;
    the GKG source reads exactly the window's partitions; one struct a theme
N3  merge_sql: one MERGE into news_theme_daily keyed on (date, feature); the
    run id is cleaned; a bad project or dataset id is refused
N4  window: 3 days ending yesterday UTC by default; never today; --start /
    --end replace --days; bad windows are refused
N5  dry run: the dataset is checked and each source is sized with a BigQuery
    dry run; no table call, no job
N6  write: the table is created when missing (SCHEMA), then one MERGE job a
    source, capped with maximumBytesBilled, polled to DONE; rows are summed
N7  the cost guard: a source over --max-gb stops the run before any job or
    table call; --max-gb outside 1..400 is refused
N8  a failing source (job error, or a refused start) leaves the other loaded
    and exits 1
N9  main: gate off (no request at all), missing project, unknown source,
    missing dataset, a table with another layout, unreadable credentials
N10 the workflow: daily schedule, dry-run dispatch by default, read-only
    repository, armed gate with a kill switch, pinned actions, checked inputs
N11 nothing here writes to the workbook; the only DML is the MERGE into the
    job's own table and the only tables read are the two GDELT ones
N12 v1.1.0: the places are FIPS country codes read from the country field of
    a location (events: ActionGeo_CountryCode); each person's pattern counts
    the forms GDELT writes (read from GDELT, 17-19 Nov 2026) and no namesake
N13 v1.1.0: the three new sources merge into their own table,
    news_entity_daily, never into news_theme_daily - the news loop of 19 Nov
    2026 reads every feature of that table
"""
from __future__ import annotations

import datetime as dt
import re
from pathlib import Path

import pytest
import yaml

from scripts import archive_news_themes as nt

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "bq_news_themes.yml"
TODAY = dt.date(2026, 11, 19)
PROJECT, DATASET = "proj-x1", "tfb_archive"

PINNED = {
    "iran_war": ("gdelt_events", "share"), "russia_war": ("gdelt_events", "share"),
    "us_china_coercion": ("gdelt_events", "share"), "us_china_tone": ("gdelt_events", "mean"),
    "fed_rates": ("gdelt_gkg", "share"), "trade_policy": ("gdelt_gkg", "share"),
    "uncertainty": ("gdelt_gkg", "share"), "election": ("gdelt_gkg", "share"),
    "ai_tech": ("gdelt_gkg", "share"), "ai_tone": ("gdelt_gkg", "mean"),
    "market_tone": ("gdelt_gkg", "mean"), "inflation": ("gdelt_gkg", "share"),
    "oil_price": ("gdelt_gkg", "share"), "armed_conflict": ("gdelt_gkg", "share"),
}
PLACE_IDS = ("us", "sa", "mideast", "eu", "jp", "cn")
PERSON_IDS = ("trump", "warsh", "bessent", "musk", "huang", "altman", "xi", "mbs", "abdulaziz", "putin", "khamenei",
              "netanyahu", "eisenkot", "lagarde", "merz", "takaichi", "ueda")
PINNED_V11 = dict(
    [("fight_%s" % p, ("gdelt_events_places", "share")) for p in PLACE_IDS]
    + [(name % p, ("gdelt_gkg_places", kind)) for p in PLACE_IDS
       for name, kind in (("place_%s_share", "share"), ("place_%s_tone", "mean"))]
    + [(name % p, ("gdelt_gkg_people", kind)) for p in PERSON_IDS
       for name, kind in (("person_%s_share", "share"), ("person_%s_tone", "mean"))])
DEFAULT_BYTES = {"gdelt_events": 9_000_000, "gdelt_gkg": 600_000_000, "gdelt_events_places": 3_000_000,
                 "gdelt_gkg_places": 75_000_000, "gdelt_gkg_people": 40_000_000}


class Resp:
    def __init__(self, status=200, body=None):
        self.status_code = status
        self._body = body if body is not None else {}

    def json(self):
        return self._body


class FakeBQ:
    """Offline BigQuery: datasets.get, tables.get / insert, dry-run queries
    (bytes per source) and query jobs (DONE after `polls` GETs)."""

    def __init__(self, table=None, dataset=True, bytes_by_source=None, job_error=None, start_error=None, polls=2,
                 entity_table=None):
        self.tables = {nt.TABLE: table, nt.ENTITY_TABLE: entity_table if entity_table is not None else table}
        self.dataset = dataset
        self.bytes_by_source = dict(DEFAULT_BYTES, **(bytes_by_source or {}))
        self.job_error = job_error or {}
        self.start_error = start_error or set()
        self.polls = polls
        self.calls, self.created, self.dry, self.jobs = [], [], [], {}

    @staticmethod
    def _source_of(sql):
        return re.search(r"'(gdelt_[a-z_]+)' AS source", sql).group(1)

    def get(self, url, timeout=None, **_):
        self.calls.append(("GET", url))
        if "/jobs/" in url:
            job = self.jobs[url.split("/jobs/")[1].split("?")[0]]
            job["polls"] += 1
            done = job["polls"] >= self.polls
            status = {"state": "DONE" if done else "RUNNING"}
            if done and job["source"] in self.job_error:
                status["errorResult"] = {"message": self.job_error[job["source"]]}
            return Resp(200, {"status": status, "statistics": {"query": {
                "numDmlAffectedRows": str(job["rows"]), "totalBytesBilled": str(job["bytes"])}}})
        if url.endswith(f"/datasets/{DATASET}"):
            return Resp(200 if self.dataset else 404, {})
        for name, table in self.tables.items():
            if url.endswith(f"/tables/{name}"):
                return Resp(200, table) if table is not None else Resp(404, {})
        raise AssertionError("unexpected GET " + url)

    def post(self, url, json=None, timeout=None, **_):
        self.calls.append(("POST", url))
        if url.endswith(f"/datasets/{DATASET}/tables"):
            self.created.append(json)
            self.tables[json["tableReference"]["tableId"]] = {"schema": json["schema"]}
            return Resp(200, json)
        if url.endswith(f"/projects/{PROJECT}/queries"):
            assert json["dryRun"] is True and json["useLegacySql"] is False
            self.dry.append(json["query"])
            return Resp(200, {"totalBytesProcessed": str(self.bytes_by_source[self._source_of(json["query"])])})
        if url.endswith(f"/projects/{PROJECT}/jobs"):
            sql = json["configuration"]["query"]["query"]
            source = self._source_of(sql)
            if source in self.start_error:
                return Resp(403, {"error": {"message": "quota"}})
            feats = len(nt.features_of(source))
            self.jobs[json["jobReference"]["jobId"]] = {
                "config": json, "sql": sql, "source": source, "polls": 0,
                "rows": 3 * feats, "bytes": self.bytes_by_source[source]}
            return Resp(200, {"status": {"state": "RUNNING"}})
        raise AssertionError("unexpected POST " + url)


def _env(monkeypatch, gate="on", project=PROJECT):
    monkeypatch.setenv("TFB_BQ_NEWS_THEMES", gate)
    if project is None:
        monkeypatch.delenv("TFB_BQ_PROJECT", raising=False)
    else:
        monkeypatch.setenv("TFB_BQ_PROJECT", project)
    monkeypatch.setenv("TFB_BQ_DATASET", DATASET)
    monkeypatch.setenv("TFB_BQ_LOCATION", "US")
    monkeypatch.setenv("GITHUB_RUN_ID", "777")


def _run(monkeypatch, argv, session):
    _env(monkeypatch)
    slept = []
    return nt.main(argv, session=session, today=TODAY, sleep=slept.append), slept


# --------------------------------------------------------------------------- N1
def test_n1_features_pinned():
    got = {name: (source, kind) for name, source, kind, _, _ in nt.FEATURES}
    assert got == dict(PINNED, **PINNED_V11) and len(nt.FEATURES) == len(got) == 14 + 52
    assert [f[0] for f in nt.FEATURES[:14]] == list(PINNED)           # v1.0.0's themes first, unchanged
    assert nt.SOURCES == ("gdelt_events", "gdelt_gkg", "gdelt_events_places", "gdelt_gkg_places", "gdelt_gkg_people")
    assert nt.KINDS == ("share", "mean") and {f[2] for f in nt.FEATURES} == set(nt.KINDS)
    assert all(nt.features_of(s) for s in nt.SOURCES)
    assert [f[0] for f in nt.features_of("gdelt_events")] == ["iran_war", "russia_war", "us_china_coercion",
                                                              "us_china_tone"]
    by = {name: (num, den) for name, _, _, num, den in nt.FEATURES}
    assert by["iran_war"] == ("COUNTIF(EventRootCode IN ('18', '19', '20') AND 'IRN' IN "
                              "(Actor1CountryCode, Actor2CountryCode))", "COUNT(*)")
    assert "'RUS' IN (Actor1CountryCode, Actor2CountryCode)" in by["russia_war"][0]
    assert by["us_china_coercion"][0].startswith("COUNTIF(EventRootCode IN ('16', '17') AND ((Actor1CountryCode = 'USA'")
    assert by["us_china_tone"][0].startswith("SUM(IF(") and "GoldsteinScale IS NOT NULL" in by["us_china_tone"][1]
    codes = {"fed_rates": "ECON_CENTRALBANK|EPU_CATS_MONETARY_POLICY|ECON_INTEREST_RATES",
             "trade_policy": "EPU_CATS_TRADE_POLICY", "uncertainty": "EPU_UNCERTAINTY", "election": "ELECTION",
             "ai_tech": "SOC_EMERGINGTECH|TECH_AUTOMATION", "inflation": "ECON_INFLATION",
             "oil_price": "ECON_OILPRICE", "armed_conflict": "ARMEDCONFLICT"}
    for name, code in codes.items():
        assert by[name] == ("COUNTIF(REGEXP_CONTAINS(Themes, r'\\b(%s)\\b'))" % code, "COUNT(*)"), name
    for name, code in (("ai_tone", "SOC_EMERGINGTECH|TECH_AUTOMATION"), ("market_tone", "ECON_STOCKMARKET")):
        num, den = by[name]
        assert num.startswith("SUM(IF(REGEXP_CONTAINS(Themes, r'\\b(%s)\\b'), SAFE_CAST(SPLIT(V2Tone" % code)
        assert den.startswith("COUNTIF(REGEXP_CONTAINS(Themes, r'\\b(%s)\\b') AND SAFE_CAST(" % code)
    # the theme pattern is a whole code: '_' is a word character, so neighbours never match
    pat = re.compile(r"\b(ELECTION)\b")
    assert pat.search("TAX_X;ELECTION;LEADER") and pat.search("ELECTION")
    assert not pat.search("ELECTION_FRAUD;TAX_FNCACT_ELECTION;PRE_ELECTION_X")


# --------------------------------------------------------------------------- N2
def test_n2_source_select():
    a, b = dt.date(2026, 11, 16), dt.date(2026, 11, 18)
    ev = nt.source_select("gdelt_events", a, b)
    assert "FROM `gdelt-bq.gdeltv2.events_partitioned`" in ev and nt.GDELT_GKG not in ev
    assert "PARSE_DATE('%Y%m%d', CAST(DIV(DATEADDED, 1000000) AS STRING)) AS date" in ev      # the day it was added
    assert "WHERE _PARTITIONTIME >= TIMESTAMP('2026-11-15') AND _PARTITIONTIME <= TIMESTAMP('2026-11-18')" in ev
    assert "AND DATEADDED >= 20261116000000 AND DATEADDED <= 20261118235959" in ev
    assert ("AND DATE_DIFF(PARSE_DATE('%Y%m%d', CAST(DIV(DATEADDED, 1000000) AS STRING)), DATE(_PARTITIONTIME), DAY) "
            "BETWEEN 0 AND 1") in ev
    assert ev.rstrip().endswith("WHERE g.date BETWEEN DATE '2026-11-16' AND DATE '2026-11-18'")
    assert ev.startswith("SELECT g.date, f.feature, 'gdelt_events' AS source, f.kind, f.num, f.den\n")
    gk = nt.source_select("gdelt_gkg", a, b)
    assert "FROM `gdelt-bq.gdeltv2.gkg_partitioned`" in gk and nt.GDELT_EVENTS not in gk
    assert "DATE(_PARTITIONTIME) AS date" in gk
    assert "WHERE _PARTITIONTIME >= TIMESTAMP('2026-11-16') AND _PARTITIONTIME <= TIMESTAMP('2026-11-18')" in gk
    assert "DATEADDED" not in gk
    evp = nt.source_select("gdelt_events_places", a, b)                  # v1.1.0: same table, same day rule
    assert evp.replace("'gdelt_events_places' AS source", "'gdelt_events' AS source").split("SELECT\n")[1].split(
        "AS date,")[0] == ev.split("SELECT\n")[1].split("AS date,")[0]
    for marker in ("FROM `gdelt-bq.gdeltv2.events_partitioned`",
                   "WHERE _PARTITIONTIME >= TIMESTAMP('2026-11-15') AND _PARTITIONTIME <= TIMESTAMP('2026-11-18')",
                   "AND DATEADDED >= 20261116000000 AND DATEADDED <= 20261118235959", "BETWEEN 0 AND 1"):
        assert marker in evp, marker
    pl, pe = nt.source_select("gdelt_gkg_places", a, b), nt.source_select("gdelt_gkg_people", a, b)
    for sql in (pl, pe):
        assert "FROM `gdelt-bq.gdeltv2.gkg_partitioned`" in sql and "DATEADDED" not in sql
        assert "WHERE _PARTITIONTIME >= TIMESTAMP('2026-11-16') AND _PARTITIONTIME <= TIMESTAMP('2026-11-18')" in sql
    assert "Locations" in pl and "Persons" not in pl and "Themes" not in pl
    assert "Persons" in pe and "Locations" not in pe and "Themes" not in pe
    for source, sql in (("gdelt_events", ev), ("gdelt_gkg", gk), ("gdelt_events_places", evp),
                        ("gdelt_gkg_places", pl), ("gdelt_gkg_people", pe)):
        feats = nt.features_of(source)
        assert sql.count("STRUCT('") == 1
        for i, (name, _, kind, num, den) in enumerate(feats):
            assert sql.count(f"'{name}'") == 1, name
            assert f"{num} AS n{i},\n      {den} AS d{i}" in sql, name
            head = "STRUCT('%s' AS feature, '%s' AS kind, " if i == 0 else "('%s', '%s', "
            assert (head % (name, kind)) in sql, name
            assert f"CAST(IFNULL(n{i}, 0) AS FLOAT64)" in sql and f"CAST(d{i} AS FLOAT64)" in sql
        assert sql.count("GROUP BY date") == 1
        for word in ("INSERT", "UPDATE ", "DELETE", "MERGE", "DROP", "CREATE"):
            assert word not in sql, word
    one = nt.source_select("gdelt_events", b, b)
    assert "TIMESTAMP('2026-11-17') AND _PARTITIONTIME <= TIMESTAMP('2026-11-18')" in one
    with pytest.raises(ValueError):
        nt.source_select("gdelt_events", b, a)
    with pytest.raises(ValueError):
        nt.source_select("reuters", a, b)
    with pytest.raises(ValueError):
        nt.source_select("gdelt_gkg", "2026-11-16", b)                     # text is never a date


# --------------------------------------------------------------------------- N3
def test_n3_merge_sql():
    a, b = dt.date(2026, 11, 16), dt.date(2026, 11, 18)
    sql = nt.merge_sql(PROJECT, DATASET, "gdelt_gkg", a, b, "777'; DROP TABLE x --")
    assert sql.startswith(f"MERGE `{PROJECT}.{DATASET}.news_theme_daily` AS t\nUSING (\n")
    assert nt.source_select("gdelt_gkg", a, b) in sql
    assert ") AS s\nON t.date = s.date AND t.feature = s.feature\n" in sql
    assert ("WHEN MATCHED THEN UPDATE SET source = s.source, kind = s.kind, num = s.num, den = s.den,\n"
            "  loaded_at = CURRENT_TIMESTAMP(), run_id = '777DROPTABLEx--'\n") in sql
    assert ("WHEN NOT MATCHED THEN INSERT (date, feature, source, kind, num, den, loaded_at, run_id)\n"
            "  VALUES (s.date, s.feature, s.source, s.kind, s.num, s.den, CURRENT_TIMESTAMP(), '777DROPTABLEx--')") in sql
    assert sql.count("MERGE") == 1 and "DELETE" not in sql and ";" not in sql.replace("';'", "")
    assert [f["name"] for f in nt.SCHEMA] == ["date", "feature", "source", "kind", "num", "den", "loaded_at", "run_id"]
    for bad_project, bad_dataset in (("Bad Project", DATASET), (PROJECT, "a-b"), ("", DATASET), (PROJECT, "x`; DROP")):
        with pytest.raises(ValueError):
            nt.merge_sql(bad_project, bad_dataset, "gdelt_gkg", a, b, "1")


# --------------------------------------------------------------------------- N4
def test_n4_window():
    assert nt.window(TODAY, 3) == (dt.date(2026, 11, 16), dt.date(2026, 11, 18))
    assert nt.window(TODAY, 1) == (dt.date(2026, 11, 18), dt.date(2026, 11, 18))
    assert nt.window(TODAY, 30)[0] == dt.date(2026, 10, 20)
    assert nt.window(TODAY, 3, "2023-10-02") == (dt.date(2023, 10, 2), dt.date(2026, 11, 18))
    assert nt.window(TODAY, 3, "2026-11-01", "2026-11-05") == (dt.date(2026, 11, 1), dt.date(2026, 11, 5))
    for days in (0, 31, -1):
        with pytest.raises(ValueError):
            nt.window(TODAY, days)
    for start, end in (("2026-11-01", "2026-11-19"),      # today is never loaded
                       ("2026-11-19", ""), ("2026-11-10", "2026-11-09"), ("2016-12-31", "2026-11-01"),
                       ("", "2026-11-05"), ("11/01/2026", ""), ("2026-11-01", "soon")):
        with pytest.raises(ValueError):
            nt.window(TODAY, 3, start, end)


# --------------------------------------------------------------------------- N5
def test_n5_dry_run(monkeypatch, capsys):
    bq = FakeBQ()
    rc, _ = _run(monkeypatch, [], bq)
    out = capsys.readouterr()
    assert rc == 0 and "dry run, nothing loaded" in out.out and "2026-11-16..2026-11-18" in out.out
    assert [m for m, _ in bq.calls] == ["GET"] + ["POST"] * 5
    assert bq.calls[0][1].endswith(f"/datasets/{DATASET}") and not bq.jobs and not bq.created
    assert bq.dry == [nt.source_select(s, dt.date(2026, 11, 16), dt.date(2026, 11, 18)) for s in nt.SOURCES]
    assert all(s + ":" in out.err for s in nt.SOURCES) and "gdelt_gkg: 2026-11-16..2026-11-18 would read 0.56 GB" in out.err
    assert "5 source(s), 2026-11-16..2026-11-18, would read 0.68 GB (dry run" in out.out


# --------------------------------------------------------------------------- N6
def test_n6_write(monkeypatch, capsys):
    bq = FakeBQ()
    rc, slept = _run(monkeypatch, ["--write", "--max-gb", "5"], bq)
    out = capsys.readouterr()
    assert rc == 0, out.err
    assert [b["tableReference"]["tableId"] for b in bq.created] == ["news_theme_daily", "news_entity_daily"]
    for body in bq.created:
        assert body["tableReference"]["projectId"] == PROJECT and body["tableReference"]["datasetId"] == DATASET
        assert body["schema"]["fields"] == nt.SCHEMA and "rangePartitioning" not in body and "timePartitioning" not in body
    assert "by place and by person" in bq.created[1]["description"] and "themes" in bq.created[0]["description"]
    jobs = list(bq.jobs.values())
    assert [j["source"] for j in jobs] == list(nt.SOURCES)
    for j in jobs:
        cfg = j["config"]
        assert cfg["jobReference"]["location"] == "US" and cfg["jobReference"]["jobId"].startswith("tfb_news_" + j["source"])
        q = cfg["configuration"]["query"]
        assert q["useLegacySql"] is False and q["maximumBytesBilled"] == str(5 * (1 << 30))
        assert q["query"] == nt.merge_sql(PROJECT, DATASET, j["source"], dt.date(2026, 11, 16), dt.date(2026, 11, 18), "777")
        assert j["polls"] == 2
    assert slept == [3.0] * 10
    assert ("5/5 source(s), 198 row(s), 2026-11-16..2026-11-18 | table created: news_theme_daily, news_entity_daily"
            in out.out)                                                                       # 3 days x 66 features
    # the table already there with the right columns: no create, same load
    bq2 = FakeBQ(table={"schema": {"fields": [dict(f, type={"FLOAT": "FLOAT64"}.get(f["type"], f["type"]))
                                              for f in nt.SCHEMA]}})
    rc2, _ = _run(monkeypatch, ["--write"], bq2)
    assert rc2 == 0 and not bq2.created and len(bq2.jobs) == 5
    # only the tables of the sources loaded are read or created
    bq5 = FakeBQ()
    assert _run(monkeypatch, ["--write", "--sources", "gdelt_gkg"], bq5)[0] == 0
    assert [b["tableReference"]["tableId"] for b in bq5.created] == ["news_theme_daily"]
    assert all(not u.endswith("/tables/news_entity_daily") for _, u in bq5.calls)
    # a history fill carries its own cap and window
    bq3 = FakeBQ(bytes_by_source={"gdelt_events": 6 * (1 << 30), "gdelt_gkg": 230 * (1 << 30)})
    rc3, _ = _run(monkeypatch, ["--write", "--start", "2023-10-02", "--max-gb", "300", "--sources",
                                "gdelt_events,gdelt_gkg"], bq3)
    assert rc3 == 0 and [j["source"] for j in bq3.jobs.values()] == ["gdelt_events", "gdelt_gkg"]
    q3 = list(bq3.jobs.values())[1]["config"]["configuration"]["query"]
    assert q3["maximumBytesBilled"] == str(300 * (1 << 30)) and "TIMESTAMP('2023-10-02')" in q3["query"]
    # v1.1.0: the fill of the three new sources alone (dry runs of 21 Nov 2026: 3, 79 and 43 GiB) fits --max-gb 100
    gib = 1 << 30
    bq4 = FakeBQ(bytes_by_source={"gdelt_events_places": 3 * gib, "gdelt_gkg_places": 79 * gib,
                                  "gdelt_gkg_people": 43 * gib, "gdelt_events": 400 * gib, "gdelt_gkg": 400 * gib})
    rc4, _ = _run(monkeypatch, ["--write", "--start", "2023-10-02", "--max-gb", "100", "--sources",
                                "gdelt_gkg_people,gdelt_events_places,gdelt_gkg_places"], bq4)
    assert rc4 == 0 and [j["source"] for j in bq4.jobs.values()] == ["gdelt_events_places", "gdelt_gkg_places",
                                                                      "gdelt_gkg_people"]
    assert len(bq4.dry) == 3 and all(nt.GDELT_GKG in q or "'gdelt_events_places' AS source" in q for q in bq4.dry)
    for j in bq4.jobs.values():
        assert j["config"]["configuration"]["query"]["maximumBytesBilled"] == str(100 * gib)


# --------------------------------------------------------------------------- N7
def test_n7_cost_guard(monkeypatch, capsys):
    bq = FakeBQ(bytes_by_source={"gdelt_events": 6 * (1 << 30), "gdelt_gkg": 230 * (1 << 30)})
    rc, _ = _run(monkeypatch, ["--write", "--start", "2023-10-02"], bq)
    err = capsys.readouterr().err
    assert rc == 2 and "over --max-gb 5" in err and "nothing loaded" in err
    assert not bq.jobs and not bq.created and all("/tables" not in u for _, u in bq.calls)
    # exactly at the cap is allowed, one byte over is not
    edge = FakeBQ(bytes_by_source={"gdelt_events": 5 * (1 << 30), "gdelt_gkg": 1})
    assert _run(monkeypatch, ["--write"], edge)[0] == 0
    over = FakeBQ(bytes_by_source={"gdelt_events": 5 * (1 << 30) + 1, "gdelt_gkg": 1})
    assert _run(monkeypatch, ["--write"], over)[0] == 2 and not over.jobs
    for bad in ("0", "0.5", "401", "5000"):
        bq = FakeBQ()
        assert _run(monkeypatch, ["--max-gb", bad], bq)[0] == 2 and not bq.calls


# --------------------------------------------------------------------------- N8
def test_n8_failures_are_isolated(monkeypatch, capsys):
    bq = FakeBQ(job_error={"gdelt_events": "Resources exceeded"})
    rc, _ = _run(monkeypatch, ["--write"], bq)
    out = capsys.readouterr()
    assert rc == 1 and "Resources exceeded" in out.err
    assert "4/5 source(s), 186 row(s)" in out.out and "FAILED: gdelt_events" in out.out   # 3 days x (66 - 4)
    assert "FAILED: gdelt_events_places" not in out.out
    assert [j["source"] for j in bq.jobs.values()] == list(nt.SOURCES)
    bq = FakeBQ(start_error={"gdelt_gkg"})
    rc, _ = _run(monkeypatch, ["--write"], bq)
    out = capsys.readouterr()
    assert rc == 1 and "gdelt_gkg: start HTTP 403: quota" in out.err and "FAILED: gdelt_gkg" in out.out
    assert "4/5 source(s), 168 row(s)" in out.out                                           # 3 days x (66 - 10)
    # one source alone, by name
    bq = FakeBQ()
    rc, _ = _run(monkeypatch, ["--write", "--sources", "gdelt_events"], bq)
    assert rc == 0 and [j["source"] for j in bq.jobs.values()] == ["gdelt_events"] and len(bq.dry) == 1


# --------------------------------------------------------------------------- N9
def test_n9_main_configuration(monkeypatch, capsys):
    bq = FakeBQ()
    _env(monkeypatch, gate="off")
    assert nt.main(["--write"], session=bq, today=TODAY) == 0 and bq.calls == []
    _env(monkeypatch, project=None)
    assert nt.main(["--write"], session=bq, today=TODAY) == 2 and bq.calls == []
    _env(monkeypatch, project="Bad Project")
    assert nt.main(["--write"], session=bq, today=TODAY) == 2 and bq.calls == []
    assert _run(monkeypatch, ["--sources", "reuters"], bq)[0] == 2 and bq.calls == []
    assert _run(monkeypatch, ["--sources", "gdelt_gkg_people,gdelt_people"], bq)[0] == 2 and bq.calls == []
    assert _run(monkeypatch, ["--days", "31"], bq)[0] == 2 and bq.calls == []
    assert _run(monkeypatch, ["--start", "2026-11-19"], bq)[0] == 2 and bq.calls == []
    assert _run(monkeypatch, ["--write"], FakeBQ(dataset=False))[0] == 2
    other = FakeBQ(table={"schema": {"fields": [dict(f) for f in nt.SCHEMA[:-1]]}})
    assert _run(monkeypatch, ["--write"], other)[0] == 2 and not other.jobs
    assert "differs from SCHEMA" in capsys.readouterr().err
    wrong_type = [dict(f) for f in nt.SCHEMA]
    wrong_type[4]["type"] = "INTEGER"
    assert nt.schema_problems({"schema": {"fields": wrong_type}})
    assert nt.schema_problems({"schema": {"fields": nt.SCHEMA}}) == []
    _env(monkeypatch)
    real = nt.make_session
    try:
        def boom():
            raise RuntimeError("no key")
        nt.make_session = boom
        assert nt.main(["--write"], today=TODAY) == 2
    finally:
        nt.make_session = real


# --------------------------------------------------------------------------- N10
def test_n10_workflow():
    wf = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    on = wf.get(True) or wf.get("on")
    assert on["schedule"] == [{"cron": "20 1 * * *"}]
    inputs = on["workflow_dispatch"]["inputs"]
    assert inputs["write"]["default"] is False and inputs["write"]["type"] == "boolean"
    assert inputs["days"]["default"] == "3" and inputs["max_gb"]["default"] == "5"
    assert inputs["start"]["default"] == "" and inputs["end"]["default"] == ""
    assert inputs["sources"]["default"] == "" and inputs["sources"]["type"] == "string"          # v1.1.0
    assert wf["permissions"] == {"contents": "read"}
    job = wf["jobs"]["themes"]
    assert int(job["timeout-minutes"]) <= 40
    run = [st for st in job["steps"] if "run the loader" in st.get("name", "").lower()][0]
    env = run["env"]
    assert env["TFB_BQ_NEWS_THEMES"] == "${{ vars.TFB_BQ_NEWS_THEMES || 'on' }}"
    assert "secrets.GOOGLE_SHEETS_CREDENTIALS" in env["GOOGLE_SHEETS_CREDENTIALS"]
    script = run["run"]
    assert 'MODE="--write"' in script and 'MODE="--dry-run"' in script and 'exit "$rc"' in script
    assert ('python scripts/archive_news_themes.py $MODE --days "$DAYS" --start "${INPUT_START:-}" '
            '--end "${INPUT_END:-}" --max-gb "$MAX_GB" --sources "${INPUT_SOURCES:-}"') in script
    assert env["INPUT_SOURCES"] == "${{ github.event.inputs.sources }}"
    assert '[[ ! "${INPUT_SOURCES:-}" =~ ^[a-z_,]*$ ]]' in script and "${{ github.event.inputs.sources }}" not in script
    assert '[[ ! "$DAYS" =~ ^[0-9]+$ ]]' in script and '[[ ! "$MAX_GB" =~ ^[0-9]+$ ]]' in script
    assert "^[0-9]{4}-[0-9]{2}-[0-9]{2}$" in script
    assert "${{ github.event.inputs.start }}" not in script and "${{ github.event.inputs.max_gb }}" not in script
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "actions/checkout@v6" in text and "actions/setup-python@v6" in text
    assert nt.DEFAULT_DAYS == 3 and nt.DEFAULT_MAX_GB == 5.0


# --------------------------------------------------------------------------- N11
def test_n11_no_workbook_write_and_one_dml():
    src = (ROOT / "scripts" / "archive_news_themes.py").read_text(encoding="utf-8")
    assert "sheets.googleapis" not in src and "gspread" not in src and "spreadsheets" not in src
    code = src.split('"""', 2)[2]
    for word in ("DELETE FROM", "DROP TABLE", "TRUNCATE TABLE", "CREATE TABLE", "CREATE OR REPLACE", "WRITE_TRUNCATE"):
        assert word not in code, word
    assert code.count("MERGE `") == 1 and 'f"MERGE `{project}.{dataset}.{table}` AS t' in code
    assert set(re.findall(r"gdelt-bq\.[a-z0-9_]+\.[a-z_]+", code)) == {nt.GDELT_EVENTS, nt.GDELT_GKG}
    assert nt.TABLE == "news_theme_daily" and nt.ENTITY_TABLE == "news_entity_daily" and nt.__version__ == "1.1.0"


# --------------------------------------------------------------------------- N12
def test_n12_places_and_people():
    assert tuple(p for p, _ in nt.PLACES) == PLACE_IDS and tuple(p for p, _ in nt.PEOPLE) == PERSON_IDS
    codes = dict(nt.PLACES)
    assert codes["us"] == "US" and codes["sa"] == "SA" and codes["jp"] == "JA" and codes["cn"] == "CH|HK"
    assert set(codes["mideast"].split("|")) == {"IR", "IZ", "YM", "IS", "LE", "SY", "JO", "KU", "QA", "AE", "BA", "MU"}
    assert set(codes["eu"].split("|")) == {"UK", "FR", "GM", "IT", "SP", "NL", "SZ", "SW", "BE", "DA", "NO", "FI", "AU",
                                           "EI", "PO", "PL"}
    every = [c for _, cs in nt.PLACES for c in cs.split("|")]
    assert len(every) == len(set(every)) and all(re.fullmatch(r"[A-Z]{2}", c) for c in every)   # no code in two places
    by = {name: (num, den) for name, _, _, num, den in nt.FEATURES}
    assert by["fight_mideast"][0] == ("COUNTIF(EventRootCode IN ('18', '19', '20') AND ActionGeo_CountryCode IN ("
                                      + ", ".join("'%s'" % c for c in codes["mideast"].split("|")) + "))")
    assert by["place_sa_share"] == ("COUNTIF(REGEXP_CONTAINS(Locations, r'#(SA)#'))", "COUNT(*)")
    assert by["place_cn_tone"][0].startswith("SUM(IF(REGEXP_CONTAINS(Locations, r'#(CH|HK)#'), SAFE_CAST(SPLIT(V2Tone")
    assert by["person_trump_share"] == ("COUNTIF(REGEXP_CONTAINS(Persons, r'\\bdonald (j\\.? )?trump\\b'))", "COUNT(*)")
    assert by["person_ueda_tone"][1].startswith("COUNTIF(REGEXP_CONTAINS(Persons, r'\\b(kazuo ueda|ueda kazuo)\\b') AND ")
    # a V1 location is TYPE#NAME#COUNTRY#ADM1#LAT#LON#FEATURE; the country field (or a country-level ADM1) matches
    loc = {
        "us": "3#Chicago, Illinois, United States#US#USIL#41.85#-87.65#423587",
        "sa": "4#Riyadh, Ar Riyad, Saudi Arabia#SA#SA10#24.6408#46.7728#-3092186",
        "mideast": "1#Qatar#QA#QA#25.5#51.25#QA",
        "eu": "1#Germany#GM#GM#51#9#GM",
        "jp": "4#Tokyo, Tokyo, Japan#JA#JA40#35.685#139.751#-246227",
        "cn": "4#Hong Kong, Hong Kong (general), Hong Kong#HK#HK00#22.2833#114.15#-1649239",
    }
    pats = {p: re.compile(r"#(%s)#" % cs) for p, cs in nt.PLACES}
    for place, text in loc.items():
        assert [p for p, rx in pats.items() if rx.search(text)] == [place], place
    assert not pats["sa"].search("2#Texas, United States#US#USTX#31.106#-97.6475#TX")    # an ADM1 is never a country
    # each pattern on the lower-case Persons forms GDELT wrote in 17-19 Nov 2026 (and namesakes it must not count)
    people = {p: re.compile(rx) for p, rx in nt.PEOPLE}
    yes = {"trump": ["donald trump", "donald j trump", "donald j. trump"], "warsh": ["kevin warsh"],
           "bessent": ["scott bessent", "lutnick scott bessent"], "musk": ["elon musk"], "huang": ["jensen huang"],
           "altman": ["sam altman"], "xi": ["xi jinping", "putin i b jinping", "jinping carlos barria"],
           "mbs": ["muhammad ben salman", "mohammed bin salman", "mohammad bin salman al saud"],
           "abdulaziz": ["abdulaziz bin salman", "abdulaziz al-salman", "abdul aziz bin salman"],
           "putin": ["vladimir putin", "putin i b jinping"], "khamenei": ["ali khamenei", "mojtaba khamenei",
                                                                         "mujtaba khamenei"],
           "netanyahu": ["benjamin netanyahu"], "eisenkot": ["gadi eisenkot"], "lagarde": ["christine lagarde"],
           "merz": ["friedrich merz"], "takaichi": ["sanae takaichi", "takaichi sanae"],
           "ueda": ["kazuo ueda", "ueda kazuo"]}
    no = {"trump": ["eric trump", "donald trumpet", "melania trump"], "warsh": ["ben dwarshuis", "jeff warshaw"],
          "xi": ["xi wang"], "mbs": ["king salman", "salman khan", "salman rushdie"],
          "abdulaziz": ["abdulaziz yari", "king abdul aziz", "abdulaziz al saud"],
          "lagarde": ["eva lagarde"], "ueda": ["rueda molins", "ueda shinya"], "huang": ["huang wei"],
          "merz": ["merzouk"], "musk": ["musk ox"]}
    for pid, names in yes.items():
        for name in names:
            assert people[pid].search(name), (pid, name)
    for pid, names in no.items():
        for name in names:
            assert not people[pid].search(name), (pid, name)
    assert "macron" not in dict(nt.PEOPLE)                         # GDELT's Persons list almost never holds him


# --------------------------------------------------------------------------- N13
def test_n13_new_sources_have_their_own_table(monkeypatch, capsys):
    assert nt.TABLE_OF == {"gdelt_events": "news_theme_daily", "gdelt_gkg": "news_theme_daily",
                           "gdelt_events_places": "news_entity_daily", "gdelt_gkg_places": "news_entity_daily",
                           "gdelt_gkg_people": "news_entity_daily"}
    assert set(nt.TABLE_OF) == set(nt.SOURCES)
    a, b = dt.date(2026, 11, 16), dt.date(2026, 11, 18)
    for source in nt.SOURCES:
        sql = nt.merge_sql(PROJECT, DATASET, source, a, b, "1")
        assert sql.startswith("MERGE `%s.%s.%s` AS t\n" % (PROJECT, DATASET, nt.TABLE_OF[source])), source
    # the 14 themes of 19 Nov 2026 are all the loop's table holds: every v1.0.0 feature, nothing else
    loop = {f[0] for f in nt.FEATURES if nt.TABLE_OF[f[1]] == nt.TABLE}
    assert loop == set(PINNED)
    with pytest.raises(ValueError):
        nt.merge_sql(PROJECT, DATASET, "reuters", a, b, "1")
    with pytest.raises(ValueError):
        nt.ensure_table(FakeBQ(), PROJECT, DATASET, "news_other")
    # a table with another layout stops the run before any job, whichever table it is
    other = FakeBQ(table={"schema": {"fields": nt.SCHEMA}}, entity_table={"schema": {"fields": nt.SCHEMA[:-1]}})
    assert _run(monkeypatch, ["--write"], other)[0] == 2 and not other.jobs
    assert "table news_entity_daily differs from SCHEMA" in capsys.readouterr().err
    # the loop's sources alone never touch the new table, and the new ones never touch the loop's
    old = FakeBQ(table={"schema": {"fields": nt.SCHEMA}}, entity_table={"schema": {"fields": nt.SCHEMA[:-1]}})
    assert _run(monkeypatch, ["--write", "--sources", "gdelt_events,gdelt_gkg"], old)[0] == 0
    assert {j["source"] for j in old.jobs.values()} == {"gdelt_events", "gdelt_gkg"}
    new = FakeBQ(table={"schema": {"fields": nt.SCHEMA[:-1]}}, entity_table={"schema": {"fields": nt.SCHEMA}})
    assert _run(monkeypatch, ["--write", "--sources", "gdelt_gkg_people"], new)[0] == 0
    assert all(nt.TABLE_OF[j["source"]] == nt.ENTITY_TABLE for j in new.jobs.values()) and len(new.jobs) == 1
