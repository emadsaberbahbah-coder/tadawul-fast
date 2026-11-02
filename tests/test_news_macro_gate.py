#!/usr/bin/env python3
"""tests/test_news_macro_gate.py - scripts/news_macro_gate.py v1.0.2 (v1.0.1 + G7 memory, English wires).

WHY (2026-10-28): the owner asked for the hand-made macro and news check
before every GO/NO-GO to run by itself, grade its sources, and keep a record
that can be scored later (forecasts vs outcomes, source accuracy), using
free sources. This harness runs the REAL module offline: every HTTP call goes
to a fake that returns recorded-shape payloads (SEC EDGAR, GDELT, Google-News
RSS, the Federal Reserve RSS, EODHD economic events and real-time quotes) and
every Sheets call to an in-memory workbook.

Pinned here:
  G1 gate off / blank / junk -> no HTTP call, no Sheets call, exit 0
  G2 taxonomy: ASCII file, 23 event types; bilingual classification
     (war, de-escalation, Fed, earnings direction, law-firm noise, rumour,
     Arabic denial, Arabic earnings)
  G3 every parser on recorded shapes (GDELT, Google-News RSS with source
     tags, Fed RSS, EDGAR ticker map + submissions window, EODHD econ
     surprise + importance, EODHD real-time)
  G4 information grade 1..6 and source tiers (registrable domain, parent
     suffix, two-level suffixes, unknown = F)
  G5 the verdict: FOMC day -> CAUTION with the Riyadh time; quiet -> GO;
     Gulf escalation from 3 domains -> NO_NEW_ENTRY; Russia/Ukraine alone
     -> no change; unconfirmed Gulf story -> CAUTION; market stress
     thresholds; names BLOCK (8-K 4.02) / WATCH (negative confirmed news) /
     INFO (8-K 2.02)
  G6 FOMC decision times follow US daylight saving (18:00 / 19:00 UTC)
  G7 (v1.0.2) memory: a situation recorded by an earlier run stays in the
     clusters while it can move the verdict (24 h macro, 72 h a name), so a
     second fetch that misses the wire story keeps NO_NEW_ENTRY; English
     Google-News confirms a Gulf story when GDELT is throttled; INFO flags
     reach the Names Flagged cell
  G7 end to end: write mode creates the three tabs with headers, appends,
     deduplicates items on the next run, upserts an economic release when
     its actual arrives; dry run writes nothing; a failing source is named
     and the run still exits 0; every source failing -> exit 1; the EODHD key
     never appears in a message
  G8 main(): --write without a spreadsheet id -> 2; evidence JSON +
     markdown; job summary appended; env symbols join the scope
  G9 ASCII-only module, version pin in verify_deployment, workflow shape

Run: python tests/test_news_macro_gate.py   or   pytest -q <this file>
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import unquote

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts import news_macro_gate as ng  # noqa: E402

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 28, 12, 47, tzinfo=UTC)          # 15:47 Riyadh, FOMC day
QUIET = dt.datetime(2026, 10, 30, 12, 47, tzinfo=UTC)
KEY = "secret-eodhd-key-123"
AR = {
    "drone_hormuz": "\u0647\u062c\u0648\u0645 \u0628\u0637\u0627\u0626\u0631\u0629 \u0645\u0633\u064a\u0631\u0629 \u0639\u0644\u0649 \u0646\u0627\u0642\u0644\u0629 \u0641\u064a \u0645\u0636\u064a\u0642 \u0647\u0631\u0645\u0632",
    "aramco_profit_up": "\u0623\u0631\u0627\u0645\u0643\u0648 \u062a\u0639\u0644\u0646 \u0627\u0631\u062a\u0641\u0627\u0639 \u0635\u0627\u0641\u064a \u0627\u0644\u0631\u0628\u062d \u0644\u0644\u0631\u0628\u0639 \u0627\u0644\u062b\u0627\u0644\u062b",
    "company_denies": "\u0627\u0644\u0634\u0631\u0643\u0629 \u062a\u0646\u0641\u064a \u0627\u0644\u0623\u0646\u0628\u0627\u0621 \u0639\u0646 \u0627\u0633\u062a\u062d\u0648\u0627\u0630",
    "argaam": "\u0623\u0631\u0642\u0627\u0645",
}


# ----------------------------------------------------------------------------- fakes
class Resp:
    def __init__(self, status: int = 200, payload: Any = None, text: str = ""):
        self.status_code = status
        self._payload = payload
        self.text = text if text else (json.dumps(payload) if payload is not None else "")

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


TICKERS = {"0": {"cik_str": 1325670, "ticker": "FRST", "title": "Primis Financial Corp."},
           "1": {"cik_str": 1000001, "ticker": "BADCO", "title": "Bad Co Inc"}}


def submissions(form_rows):
    cols = {k: [] for k in ("form", "filingDate", "acceptanceDateTime", "items",
                            "accessionNumber", "primaryDocument")}
    for r in form_rows:
        for k in cols:
            cols[k].append(r.get(k, ""))
    return {"cik": "x", "filings": {"recent": cols}}


FRST_SUB = submissions([
    {"form": "8-K", "filingDate": "2026-10-22", "acceptanceDateTime": "2026-10-22T20:31:05.000Z",
     "items": "2.02,9.01", "accessionNumber": "0001325670-26-000041", "primaryDocument": "frst-8k.htm"},
    {"form": "10-Q", "filingDate": "2026-10-23", "acceptanceDateTime": "2026-10-23T10:00:00.000Z",
     "items": "", "accessionNumber": "0001325670-26-000042", "primaryDocument": "q.htm"},
    {"form": "8-K", "filingDate": "2026-08-01", "acceptanceDateTime": "2026-08-01T10:00:00.000Z",
     "items": "5.02", "accessionNumber": "0001325670-26-000030", "primaryDocument": "old.htm"},
])
BAD_SUB = submissions([
    {"form": "8-K", "filingDate": "2026-10-20", "acceptanceDateTime": "2026-10-20T12:00:00.000Z",
     "items": "4.02,9.01", "accessionNumber": "0001000001-26-000009", "primaryDocument": "r.htm"},
])


def gdelt_articles(rows):
    return {"articles": [{"url": u, "title": t, "seendate": s, "domain": d, "language": "English"}
                         for (u, t, s, d) in rows]}


GDELT_GEO = gdelt_articles([
    ("https://www.reuters.com/world/a1", "Iran seizes tanker, fires missiles near Strait of Hormuz", "20261028T090000Z", "reuters.com"),
    ("https://www.cnbc.com/a2", "Missile strike on tanker in Strait of Hormuz lifts oil", "20261028T091500Z", "cnbc.com"),
    ("https://www.aljazeera.com/a3", "Houthi drone attack hits tanker near Hormuz", "20261028T093000Z", "aljazeera.com"),
    ("https://www.bbc.com/a4", "Russia launches missile and drone attack on Kyiv", "20261028T080000Z", "bbc.com"),
])
GDELT_EMPTY = {"articles": []}
GDELT_PRIMIS = gdelt_articles([
    ("https://www.marketbeat.com/p1", "Primis Financial shares fall after earnings miss", "20261027T150000Z", "marketbeat.com"),
])

GNEWS_RSS = f"""<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel>
<item><title>{AR['aramco_profit_up']} - {AR['argaam']}</title><link>https://news.google.com/rss/articles/abc</link>
<pubDate>Tue, 27 Oct 2026 07:00:00 GMT</pubDate><source url="https://www.argaam.com">{AR['argaam']}</source></item>
<item><title>{AR['drone_hormuz']} - Al Arabiya</title><link>https://news.google.com/rss/articles/def</link>
<pubDate>Wed, 28 Oct 2026 08:00:00 GMT</pubDate><source url="https://www.alarabiya.net">Al Arabiya</source></item>
</channel></rss>"""
FED_RSS = """<?xml version="1.0"?><rss version="2.0"><channel>
<item><title>Federal Reserve issues FOMC statement</title><link>https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm</link>
<pubDate>Wed, 16 Sep 2026 18:00:00 GMT</pubDate></item>
<item><title>Agencies issue joint statement on resolution plans</title><link>https://www.federalreserve.gov/x.htm</link>
<pubDate>Tue, 27 Oct 2026 14:00:00 GMT</pubDate></item>
</channel></rss>"""
ECON_US = [
    {"type": "Fed Interest Rate Decision", "comparison": None, "period": None, "country": "US",
     "date": "2026-10-28 18:00:00", "actual": None, "previous": 3.75, "estimate": 3.5,
     "change": None, "change_percentage": None},
    {"type": "Nonfarm Payrolls", "comparison": "mom", "period": "Oct", "country": "US",
     "date": "2026-10-27 12:30:00", "actual": 120000, "previous": 150000, "estimate": 100000,
     "change": -30000, "change_percentage": -20.0},
    {"type": "Building Permits", "comparison": "mom", "period": "Sep", "country": "US",
     "date": "2026-10-27 12:30:00", "actual": 1.4, "previous": 1.38, "estimate": 1.39,
     "change": 0.02, "change_percentage": 1.4},
]
ECON_SA = [{"type": "Inflation Rate", "comparison": "yoy", "period": "Sep", "country": "SA",
            "date": "2026-10-27 09:00:00", "actual": None, "previous": 2.1, "estimate": 2.0,
            "change": None, "change_percentage": None}]
GNEWS_EN_RSS = """<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel>
<item><title>Iran fires missiles at tanker in Strait of Hormuz - Reuters</title><link>https://news.google.com/rss/articles/en1</link>
<pubDate>Wed, 28 Oct 2026 09:00:00 GMT</pubDate><source url="https://www.reuters.com">Reuters</source></item>
<item><title>Missile strike sets tanker ablaze near Hormuz - AP News</title><link>https://news.google.com/rss/articles/en2</link>
<pubDate>Wed, 28 Oct 2026 09:20:00 GMT</pubDate><source url="https://apnews.com">AP News</source></item>
<item><title>Tanker hit by missile in Strait of Hormuz - BBC</title><link>https://news.google.com/rss/articles/en3</link>
<pubDate>Wed, 28 Oct 2026 09:40:00 GMT</pubDate><source url="https://www.bbc.com">BBC</source></item>
</channel></rss>"""
MARKET = [{"code": "GSPC.INDX", "close": 6650.1, "previousClose": 6700.0, "change_p": -0.74},
          {"code": "VIX.INDX", "close": 18.2, "previousClose": 17.0, "change_p": 7.06}]


class FakeHTTP:
    def __init__(self, fail: Optional[Dict[str, int]] = None, econ_us=None, en_rss: str = ""):
        self.calls: List[str] = []
        self.fail = fail or {}
        self.econ_us = econ_us if econ_us is not None else ECON_US
        self.en_rss = en_rss

    def get(self, url, params=None, headers=None, timeout=None):
        params = params or {}
        self.calls.append(url)
        for frag, code in self.fail.items():
            if frag in url:
                if code == -1:
                    raise ConnectionError("boom key=" + KEY)
                return Resp(code, text="err")
        if url == ng.EDGAR_TICKERS:
            assert headers and "User-Agent" in headers
            return Resp(200, TICKERS)
        if "CIK0001325670" in url:
            return Resp(200, FRST_SUB)
        if "CIK0001000001" in url:
            return Resp(200, BAD_SUB)
        if url == ng.GDELT_API:
            q = params.get("query", "")
            if "Hormuz" in q:
                return Resp(200, GDELT_GEO)
            if "Primis" in q:
                return Resp(200, GDELT_PRIMIS)
            if '"Bad"' in q:
                return Resp(200, text="The specified phrase is too short")
            return Resp(200, GDELT_EMPTY)
        if url == ng.GNEWS_RSS and self.en_rss and params.get("hl") == "en-US" \
                and "Hormuz" in params.get("q", ""):
            return Resp(200, text=self.en_rss)
        if url == ng.GNEWS_RSS:
            return Resp(200, text=GNEWS_RSS if params.get("hl") == "ar" and "\u0647\u062c\u0648\u0645" in params.get("q", "") else
                        "<rss><channel></channel></rss>")
        if url == ng.FED_RSS:
            return Resp(200, text=FED_RSS)
        if url == ng.EODHD_ECON:
            assert params.get("api_token") == KEY
            return Resp(200, self.econ_us if params.get("country") == "US" else ECON_SA)
        if url.startswith("https://eodhd.com/api/real-time/"):
            return Resp(200, MARKET)
        return Resp(404, text="no route")


def _a1(cells: str):
    """'A2:M' -> (row0, col0, row1|None, col1); 'A1' -> single cell."""
    def split(ref):
        col = "".join(ch for ch in ref if ch.isalpha())
        row = "".join(ch for ch in ref if ch.isdigit())
        c = 0
        for ch in col:
            c = c * 26 + (ord(ch) - 64)
        return (int(row) if row else None), c - 1
    if ":" in cells:
        a, b = cells.split(":")
        r0, c0 = split(a)
        r1, c1 = split(b)
        return (r0 or 1), c0, r1, c1
    r0, c0 = split(cells)
    return r0, c0, r0, c0


class FakeSheets:
    SID = "sid-1"

    def __init__(self, tabs: Optional[Dict[str, List[List[Any]]]] = None):
        self.tabs = {k: [list(r) for r in v] for k, v in (tabs or {}).items()}
        self.writes: List[str] = []

    def _tab_cells(self, url: str):
        rng = unquote(url.split("/values/", 1)[1].split("?", 1)[0].split(":append", 1)[0])
        tab, _, cells = rng.partition("!")
        return tab.strip("'").replace("''", "'"), cells

    def get(self, url, timeout=None):
        tab, cells = self._tab_cells(url)
        if tab not in self.tabs:
            return Resp(400, {"error": {"message": "Unable to parse range"}})
        r0, c0, r1, c1 = _a1(cells or "A1:ZZ")
        rows = self.tabs[tab]
        end = len(rows) if r1 is None else min(r1, len(rows))
        out = []
        for r in rows[r0 - 1:end]:
            out.append(list(r[c0:c1 + 1]))
        while out and not any(str(x) for x in out[-1]):
            out.pop()
        return Resp(200, {"values": out})

    def post(self, url, json=None, timeout=None):
        if url.endswith(":batchUpdate") and "/values" not in url:
            for req in json["requests"]:
                self.tabs.setdefault(req["addSheet"]["properties"]["title"], [])
            self.writes.append("addSheet")
            return Resp(200, {})
        if url.endswith("/values:batchUpdate"):
            for d in json["data"]:
                tab, _, cell = d["range"].partition("!")
                tab = tab.strip("'")
                row = int(cell[1:])
                while len(self.tabs[tab]) < row:
                    self.tabs[tab].append([])
                self.tabs[tab][row - 1] = list(d["values"][0])
            self.writes.append("update")
            return Resp(200, {})
        if ":append" in url:
            tab, _ = self._tab_cells(url)
            self.tabs[tab].extend(list(r) for r in json["values"])
            self.writes.append("append:" + tab)
            return Resp(200, {})
        return Resp(404, {})

    def put(self, url, json=None, timeout=None):
        tab, _ = self._tab_cells(url)
        rows = self.tabs.setdefault(tab, [])
        if rows:
            rows[0] = list(json["values"][0])
        else:
            rows.append(list(json["values"][0]))
        self.writes.append("header:" + tab)
        return Resp(200, {})


def workbook():
    return FakeSheets({
        "My_Portfolio": [["Symbol", "Name"], ["FRST.US", "Primis Financial Corp."],
                         ["5023.SR", "Arabian Centres Sukuk 5023"]],
        "_Shadow_Rank_Log": [
            ["Logged At", "Day", "Cohort", "Order", "Rank", "Symbol", "Market", "Sector"],
            ["2026-10-26 08:00", "2026-10-26", "1", "BOARD", "1", "OLD.US", "", ""],
            ["2026-10-27 10:48", "2026-10-27", "2", "BOARD", "1", "BADCO.US", "", ""],
            ["2026-10-27 10:48", "2026-10-27", "2", "CHAMPION", "1", "FRST.US", "", ""],
        ],
        "Global_Markets": [["TOP"], [], ["Symbol", "Name"], ["BADCO.US", "Bad Co Inc"]],
    })


@pytest.fixture
def armed(monkeypatch, tmp_path):
    monkeypatch.setenv("TFB_NEWS_GATE", "observe")
    monkeypatch.setenv("TFB_SEC_USER_AGENT", "TFB research test@example.com")
    monkeypatch.setenv("EODHD_API_KEY", KEY)
    monkeypatch.setenv("DEFAULT_SPREADSHEET_ID", FakeSheets.SID)
    monkeypatch.setenv("TFB_NEWS_GATE_GDELT_SPACING_S", "0")
    for k in ("TFB_NEWS_GATE_SOURCES", "TFB_NEWS_GATE_SYMBOLS", "GITHUB_STEP_SUMMARY",
              "TFB_NEWS_GATE_EVIDENCE_DIR", "GITHUB_RUN_ID", "TFB_NEWS_GATE_MAX_NAMES",
              "GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS"):
        monkeypatch.delenv(k, raising=False)
    return tmp_path


def _sleep(_s):
    return None


def _run(http=None, sheets=None, write=True, now=NOW, sources=None, scope=None):
    sh = sheets if sheets is not None else workbook()
    if scope is None:
        scope = ng.read_scope(sh, FakeSheets.SID, 15)
    return ng.run(http=http or FakeHTTP(), sheets=sh, sid=FakeSheets.SID, now=now, write=write,
                  sources=sources or list(ng.ALL_SOURCES), scope=scope, sleep=_sleep,
                  run_id="r1", mode="observe"), sh


# ----------------------------------------------------------------------------- G1
@pytest.mark.parametrize("val", ["", "off", "0", "junk", "true"])
def test_g1_gate_off_touches_nothing(monkeypatch, val, capsys):
    monkeypatch.setenv("TFB_NEWS_GATE", val)
    http, sheets = FakeHTTP(), workbook()
    assert ng.main(["--write"], http=http, sheets=sheets, now=NOW, sleep=_sleep) == 0
    assert http.calls == [] and sheets.writes == []
    assert "TFB_NEWS_GATE is off" in capsys.readouterr().out


# ----------------------------------------------------------------------------- G2
def test_g2_taxonomy_ascii_and_complete():
    raw = Path(ng.TAXONOMY_PATH).read_bytes()
    assert all(b < 128 for b in raw)
    tax = ng.load_taxonomy()
    assert len(tax["events"]) == 23
    types = [e["type"] for e in tax["events"]]
    assert types.index("law_firm_noise") < types.index("legal_regulatory")
    assert types.index("earnings") < types.index("guidance")


@pytest.mark.parametrize("title,scope,etype,direction,extra", [
    ("Iran fires missiles at tanker in Strait of Hormuz", "MACRO", "war_escalation", -1, {"gulf": True}),
    (AR["drone_hormuz"], "MACRO", "war_escalation", -1, {"gulf": True}),
    ("Israel and Hamas agree ceasefire deal", "MACRO", "de_escalation", 1, {}),
    ("Fed holds rates steady as Powell signals patience", "MACRO", "central_bank", 0, {}),
    ("Primis Financial reports third quarter results; net income falls", "FRST.US", "earnings", -1, {}),
    ("Shareholder alert: Pomerantz investigates claims on behalf of investors", "X.US", "law_firm_noise", 0, {}),
    ("Acme is considering a sale, people familiar with the matter said", "ACME.US", "", 0, {"rumor": True}),
    (AR["company_denies"], "2222.SR", "mna", 1, {"denial": True}),
    (AR["aramco_profit_up"], "2222.SR", "earnings", 1, {}),
    ("CPI rises 0.3% as inflation data beats forecasts", "MACRO", "inflation_data", 1, {}),
])
def test_g2_classification(title, scope, etype, direction, extra):
    """A same-story Arabic and English pair must share the anchor (cluster)."""
    clf = ng.Classifier(ng.load_taxonomy())
    got = clf.classify(title, scope)
    assert got["event_type"] == etype
    assert got["direction"] == direction
    if extra.get("gulf"):
        assert got["anchor"] in ng.GULF_ANCHORS
    if extra.get("rumor"):
        assert got["rumor"] is True
    if extra.get("denial"):
        assert got["denial"] is True


def test_g2_short_ascii_words_match_whole_words_only():
    clf = ng.Classifier(ng.load_taxonomy())
    assert clf.classify("Fed signals a pause", "MACRO")["event_type"] == "central_bank"
    assert clf.classify("Federated Hermes launches a new fund", "MACRO")["event_type"] == ""
    assert clf.classify("Aramco says capex stable", "MACRO")["event_type"] == ""


# ----------------------------------------------------------------------------- G3
def test_g2_arabic_and_english_anchor_share_a_cluster():
    clf = ng.Classifier(ng.load_taxonomy())
    assert clf.classify(AR["drone_hormuz"], "MACRO")["anchor"] == "hormuz"
    assert clf.classify("Missile strike near Strait of Hormuz", "MACRO")["anchor"] == "hormuz"


@pytest.mark.parametrize("title,etype", [
    ("Iran launches missile strikes as diplomatic efforts fail", "war_escalation"),
    ("Missile strikes go on after diplomatic efforts failed", "war_escalation"),
    ("Airstrikes resume after peace talks stall", "war_escalation"),
    ("Houthis reject truce and fire missiles at tankers", "war_escalation"),
    ("Missile barrage follows breakdown in ceasefire talks", "war_escalation"),
    ("\u0647\u062c\u0648\u0645 \u0635\u0627\u0631\u0648\u062e\u064a \u0628\u0639\u062f \u0641\u0634\u0644 \u0627\u0644\u062c\u0647\u0648\u062f \u0627\u0644\u062f\u0628\u0644\u0648\u0645\u0627\u0633\u064a\u0629", "war_escalation"),
    ("\u063a\u0627\u0631\u0627\u062a \u062c\u062f\u064a\u062f\u0629 \u0645\u0639 \u062a\u0639\u062b\u0631 \u0645\u062d\u0627\u062f\u062b\u0627\u062a \u0627\u0644\u0647\u062f\u0646\u0629", "war_escalation"),
    ("Diplomatic efforts gather pace to contain the conflict", "de_escalation"),
    ("US-brokered ceasefire halts missile strikes", "de_escalation"),
    ("Missile shield installed as the truce holds", "de_escalation"),
])
def test_g2_failed_diplomacy_stays_escalation(title, etype):
    """Codex P2 on #861: a diplomacy term must not turn strikes into easing
    when the headline says the diplomacy failed (fails on 3d84c263); the
    word-start terms keep "US-brokered" and "installed" out of it."""
    clf = ng.Classifier(ng.load_taxonomy())
    assert clf.classify(title, "MACRO")["event_type"] == etype


def test_g2_leading_space_marks_a_word_start():
    terms = ng._norm_terms([" stall", "broke ", "fail", " fail"])
    assert terms == [" stall", "broke ", " fail ", " fail"]
    assert ng._hit(ng.normalize_text("Talks stalled again"), terms[:1])
    assert not ng._hit(ng.normalize_text("Radar installed at the base"), terms[:1])
    assert not ng._hit(ng.normalize_text("A US-brokered truce"), terms[1:2])
    assert ng._hit(ng.normalize_text("Efforts failed"), terms[3:])
    assert not ng._hit(ng.normalize_text("Efforts failed"), terms[2:3])


@pytest.mark.parametrize("title,etype", [
    ("\u0623\u0633\u0639\u0627\u0631 \u0627\u0644\u0646\u0641\u0637 \u062a\u062a\u0631\u0627\u062c\u0639 \u0648\u0633\u0637 \u062a\u0632\u0627\u064a\u062f \u0627\u0644\u062c\u0647\u0648\u062f \u0627\u0644\u062f\u0628\u0644\u0648\u0645\u0627\u0633\u064a\u0629 \u0644\u0627\u062d\u062a\u0648\u0627\u0621 \u062d\u0631\u0628 \u0625\u064a\u0631\u0627\u0646", "de_escalation"),
    ("\u0627\u0646\u062a\u0647\u0627\u0621 \u0627\u0644\u062d\u0631\u0628 \u0633\u064a\u062f\u0641\u0639 \u0627\u0644\u0646\u0641\u0637 \u0625\u0644\u0649 \u062f\u0648\u0646 100 \u062f\u0648\u0644\u0627\u0631", "de_escalation"),
    ("\u0625\u064a\u0631\u0627\u0646 \u0645\u0628\u0627\u0634\u0631.. \u0627\u0634\u062a\u0639\u0627\u0644 \u0646\u0627\u0642\u0644\u0629 \u0628\u0645\u0636\u064a\u0642 \u0647\u0631\u0645\u0632 \u0648\u0648\u0627\u0634\u0646\u0637\u0646 \u062a\u062a\u0647\u0645 \u0637\u0647\u0631\u0627\u0646 \u0628\u0634\u0646 \u062d\u0631\u0628 \u0627\u0642\u062a\u0635\u0627\u062f\u064a\u0629", "war_escalation"),
    ("Saudi-led coalition says it intercepts three Houthi missiles, four drones", "war_escalation"),
])
def test_g2_live_headlines_28_oct(title, etype):
    """v1.0.1: headlines from the first live dry run (40595558188)."""
    clf = ng.Classifier(ng.load_taxonomy())
    assert clf.classify(title, "MACRO")["event_type"] == etype


@pytest.mark.parametrize("title,etype", [
    ("Iran and Saudi Arabia agree de-escalation deal", "de_escalation"),
    ("Ceasefire agreed in Strait of Hormuz", "de_escalation"),
    ("Missile strike breaks ceasefire near Hormuz", "war_escalation"),
    ("\u0627\u062a\u0641\u0627\u0642 \u0639\u0644\u0649 \u062e\u0641\u0636 \u0627\u0644\u062a\u0635\u0639\u064a\u062f \u0641\u064a \u0645\u0636\u064a\u0642 \u0647\u0631\u0645\u0632", "de_escalation"),
])
def test_g2_de_escalation_is_not_war(title, etype):
    """Codex P2 on #859: 'escalation' inside 'de-escalation' and a bare
    'hormuz' used to classify a truce as war news (golden negative on
    the first push of this branch)."""
    clf = ng.Classifier(ng.load_taxonomy())
    assert clf.classify(title, "MACRO")["event_type"] == etype


def test_g3_parsers():
    g = ng.parse_gdelt(GDELT_GEO, "MACRO", "geo")
    assert len(g) == 4 and g[0]["domain"] == "reuters.com"
    assert g[0]["published"] == dt.datetime(2026, 10, 28, 9, 0, tzinfo=UTC)

    r = ng.parse_rss(GNEWS_RSS, "MACRO", "ar", "google_news", "ar")
    assert [i["domain"] for i in r] == ["argaam.com", "alarabiya.net"]
    assert r[0]["title"] == AR["aramco_profit_up"]           # " - <source>" stripped
    assert r[1]["published"] == dt.datetime(2026, 10, 28, 8, 0, tzinfo=UTC)
    assert ng.parse_rss("<not xml", "MACRO", "x", "s", "en") == []

    tmap = ng.parse_ticker_map(TICKERS)
    assert tmap["FRST"] == (1325670, "Primis Financial Corp.")

    since = NOW - dt.timedelta(days=30)
    e = ng.parse_edgar_submissions(FRST_SUB, "FRST.US", 1325670, since)
    assert len(e) == 1                                        # 10-Q and the August 8-K are out
    assert e[0]["items"] == ["2.02", "9.01"]
    assert e[0]["title"].startswith("8-K 2.02 results of operations")
    assert e[0]["url"].endswith("/1325670/000132567026000041/frst-8k.htm")

    ev = {x["type"]: x for x in ng.parse_econ_events(ECON_US)}
    assert ev["Nonfarm Payrolls"]["importance"] == "HIGH"
    assert ev["Nonfarm Payrolls"]["surprise"] == 20000 and ev["Nonfarm Payrolls"]["surprise_pct"] == 20.0
    assert ev["Building Permits"]["importance"] == "NORMAL"
    assert ev["Fed Interest Rate Decision"]["surprise"] is None

    m = ng.parse_market(MARKET)
    assert m["VIX.INDX"]["close"] == 18.2 and m["GSPC.INDX"]["change_p"] == -0.74


# ----------------------------------------------------------------------------- G4
def test_g4_tiers():
    assert ng.domain_of("https://www.argaam.com/ar/article/1") == "argaam.com"
    assert ng.domain_of("https://news.cma.org.sa/x") == "cma.org.sa"
    assert ng.tier_of("argaam.com") == "B" and ng.tier_of("cma.org.sa") == "A"
    assert ng.tier_of("finance.yahoo.com") == "D" and ng.tier_of("x.com") == "E"
    assert ng.tier_of("unknownblog.io") == "F"


def _m(domain, tier=None, rumor=False, denial=False):
    return {"domain": domain, "tier": tier or ng.tier_of(domain), "rumor": rumor, "denial": denial}


def test_g4_information_grade():
    assert ng.information_grade([_m("sec.gov")]) == 1
    assert ng.information_grade([_m("reuters.com"), _m("cnbc.com"), _m("bbc.com")]) == 1
    assert ng.information_grade([_m("reuters.com")]) == 2
    assert ng.information_grade([_m("cnbc.com"), _m("bbc.com")]) == 2
    assert ng.information_grade([_m("cnbc.com")]) == 3
    assert ng.information_grade([_m("reuters.com", rumor=True)]) == 4
    assert ng.information_grade([_m("reuters.com"), _m("cnbc.com", denial=True)]) == 5
    assert ng.information_grade([_m("x.com"), _m("reddit.com")]) == 6


def test_g4_unrelated_denial_does_not_wipe_out_a_situation():
    """Codex P2 on #859: one denial in a cluster used to force grade 5."""
    base = [_m("reuters.com"), _m("cnbc.com"), _m("bbc.com")]
    assert ng.information_grade(base + [_m("aljazeera.com", denial=True)]) == 1
    assert ng.information_grade([_m("reuters.com"), _m("cnbc.com", denial=True),
                                 _m("bbc.com", denial=True)]) == 5


# ----------------------------------------------------------------------------- G5
def _clusters(items, now=NOW):
    clf = ng.Classifier(ng.load_taxonomy())
    return ng.cluster_items([i for i in ng.classify_all(items, clf, now)
                             if i.get("event_type") and i["event_type"] != "law_firm_noise"])


def test_g5_fomc_day_is_caution_with_riyadh_time():
    g = ng.evaluate([], [], {}, NOW)
    assert g["verdict"] == "CAUTION"
    assert g["drivers"] == ["scheduled: FOMC rate decision (today 21:00 Riyadh)"]


def test_g5_eodhd_fomc_statement_is_not_a_second_driver():
    econ = ng.parse_econ_events([{"type": "FOMC Statement", "comparison": None, "period": None,
                                  "country": "US", "date": "2026-10-28 18:00:00", "actual": None,
                                  "previous": None, "estimate": None}])
    g = ng.evaluate([], econ, {}, NOW)
    assert g["drivers"] == ["scheduled: FOMC rate decision (today 21:00 Riyadh)"]


def test_g5_quiet_day_is_go():
    g = ng.evaluate([], [], {}, QUIET)
    assert g["verdict"] == "GO" and g["drivers"] == [] and g["names"] == {}


def test_g5_gulf_escalation_three_domains_is_no_new_entry():
    items = ng.parse_gdelt(GDELT_GEO, "MACRO", "geo")
    later = dt.datetime(2026, 10, 30, 10, 0, tzinfo=UTC)      # stories 49 h old, no FOMC near
    assert ng.evaluate(_clusters(items, later), [], {}, later)["verdict"] == "GO"
    same = dt.datetime(2026, 10, 28, 10, 0, tzinfo=UTC)
    g = ng.evaluate(_clusters(items, same), [], {}, same)
    assert g["verdict"] == "NO_NEW_ENTRY"
    assert any(d.startswith("macro: war_escalation/hormuz grade 1, 3 sources") for d in g["drivers"])
    assert not any("russia" in d for d in g["drivers"])


def test_g5_failed_diplomacy_from_three_wires_is_no_new_entry():
    """Codex P2 on #861: the same story read as easing gave GO."""
    items = ng.parse_gdelt(gdelt_articles([
        ("https://www.reuters.com/f1", "Iran launches missile strikes as diplomatic efforts fail", "20261030T100000Z", "reuters.com"),
        ("https://apnews.com/f2", "Iran fires missiles at Gulf targets as diplomatic efforts fail", "20261030T101000Z", "apnews.com"),
        ("https://www.bbc.com/f3", "Iran missile strikes follow failed diplomatic efforts", "20261030T102000Z", "bbc.com"),
    ]), "MACRO", "geo")
    g = ng.evaluate(_clusters(items, QUIET), [], {}, QUIET)
    assert g["verdict"] == "NO_NEW_ENTRY"
    assert g["drivers"][0].startswith("macro: war_escalation/iran grade 1, 3 sources")


def test_g5_russia_ukraine_alone_does_not_move_the_gate():
    items = ng.parse_gdelt(gdelt_articles([
        ("https://www.reuters.com/r1", "Russia launches missile attack on Kyiv", "20261030T080000Z", "reuters.com"),
        ("https://www.bbc.com/r2", "Ukraine says Russian missile strike hits Kharkiv", "20261030T081000Z", "bbc.com"),
        ("https://www.cnn.com/r3", "Russian drone attack on Ukraine energy grid", "20261030T082000Z", "cnn.com"),
    ]), "MACRO", "geo")
    g = ng.evaluate(_clusters(items, QUIET), [], {}, QUIET)
    assert g["verdict"] == "GO"


def test_g5_unconfirmed_gulf_story_is_caution():
    items = ng.parse_gdelt(gdelt_articles([
        ("https://someblog.io/x", "Unconfirmed reports of missile attack near Hormuz", "20261030T100000Z", "someblog.io"),
    ]), "MACRO", "geo")
    g = ng.evaluate(_clusters(items, QUIET), [], {}, QUIET)
    assert g["verdict"] == "CAUTION"
    assert g["drivers"][0].startswith("macro (unconfirmed): war_escalation/hormuz grade 3")


def test_g5_no_new_entry_needs_a_licensed_wire():
    items = ng.parse_gdelt(gdelt_articles([
        ("https://www.cnbc.com/c1", "Missile strike on tanker near Hormuz", "20261030T100000Z", "cnbc.com"),
        ("https://www.bbc.com/c2", "Missiles hit tanker near Hormuz", "20261030T101000Z", "bbc.com"),
        ("https://www.cnn.com/c3", "Tanker hit by missile near Hormuz", "20261030T102000Z", "cnn.com"),
    ]), "MACRO", "geo")
    g = ng.evaluate(_clusters(items, QUIET), [], {}, QUIET)
    assert g["verdict"] == "CAUTION"                         # grade 2 (three C outlets)
    assert g["drivers"][0].startswith("macro: war_escalation/hormuz grade 2, 3 sources")


@pytest.mark.parametrize("market,verdict", [
    ({"GSPC.INDX": {"change_p": -3.2}}, "NO_NEW_ENTRY"),
    ({"GSPC.INDX": {"change_p": -1.8}}, "CAUTION"),
    ({"VIX.INDX": {"close": 36.0, "change_p": 5.0}}, "NO_NEW_ENTRY"),
    ({"VIX.INDX": {"close": 27.0, "change_p": 3.0}}, "CAUTION"),
    ({"VIX.INDX": {"close": 19.0, "change_p": 16.0}}, "CAUTION"),
    ({"GSPC.INDX": {"change_p": -0.5}, "VIX.INDX": {"close": 17.0, "change_p": 1.0}}, "GO"),
])
def test_g5_market_thresholds(market, verdict):
    assert ng.evaluate([], [], market, QUIET)["verdict"] == verdict


def test_g5_name_flags():
    since = QUIET - dt.timedelta(days=30)
    items = ng.parse_edgar_submissions(FRST_SUB, "FRST.US", 1325670, since) \
        + ng.parse_edgar_submissions(BAD_SUB, "BADCO.US", 1000001, since) \
        + ng.parse_gdelt(gdelt_articles([
            ("https://www.reuters.com/z1", "Zeta Corp shares plunge after guidance cut", "20261030T100000Z", "reuters.com"),
        ]), "ZETA.US", "name")
    names = ng.evaluate(_clusters(items, QUIET), [], {}, QUIET)["names"]
    assert names["BADCO.US"] == [{"level": "BLOCK", "why": "8-K item 4.02 (10d ago)"}]
    assert names["FRST.US"] == [{"level": "INFO", "why": "earnings released (8-K 2.02, 8d ago)"}]
    assert names["ZETA.US"][0]["level"] == "WATCH"


# ----------------------------------------------------------------------------- G6
def test_g6_fomc_times_follow_dst():
    times = {t.date().isoformat(): t for t in ng.fomc_decision_times()}
    assert times["2026-10-28"] == dt.datetime(2026, 10, 28, 18, 0, tzinfo=UTC)
    assert times["2026-12-09"] == dt.datetime(2026, 12, 9, 19, 0, tzinfo=UTC)
    assert times["2026-03-18"] == dt.datetime(2026, 3, 18, 18, 0, tzinfo=UTC)
    assert times["2026-01-28"] == dt.datetime(2026, 1, 28, 19, 0, tzinfo=UTC)


# ----------------------------------------------------------------------------- G7
def test_g7_end_to_end_write_then_dedupe_and_upsert(armed):
    (rc, rep), sh = _run()
    assert rc == 0 and rep["wrote"] is True
    assert sh.tabs[ng.GATE_TAB][0] == ng.GATE_HEADER
    assert sh.tabs[ng.ITEMS_TAB][0] == ng.ITEMS_HEADER
    assert sh.tabs[ng.ECON_TAB][0] == ng.ECON_HEADER
    gate = sh.tabs[ng.GATE_TAB][1]
    assert gate[2] == "NO_NEW_ENTRY"                          # Gulf escalation, 3 domains, today
    assert "FOMC rate decision" in gate[4]
    assert "BADCO.US BLOCK" in gate[6]
    assert "FRST.US INFO: earnings released" in gate[6]                 # v1.0.2
    assert gate[6].index("BADCO.US BLOCK") < gate[6].index("FRST.US INFO")
    assert rep["scope"][0]["symbol"] == "FRST.US" and rep["scope"][0]["role"] == "holding"
    assert {s["symbol"] for s in rep["scope"]} == {"FRST.US", "5023.SR", "BADCO.US"}   # OLD.US: older day
    assert "gdelt" in rep["sources_failed"] and "non-JSON reply" in rep["sources_failed"]["gdelt"]
    n_items = len(sh.tabs[ng.ITEMS_TAB]) - 1
    assert n_items == rep["counts"]["new"] > 0
    econ_rows = sh.tabs[ng.ECON_TAB][1:]
    assert {r[3] for r in econ_rows} == {"Fed Interest Rate Decision", "Nonfarm Payrolls", "Inflation Rate"}

    later = [dict(e) for e in ECON_US]
    later[0]["actual"] = 3.5
    (rc2, rep2), sh2 = _run(http=FakeHTTP(econ_us=later), sheets=sh, now=NOW + dt.timedelta(hours=7))
    assert rc2 == 0 and rep2["counts"]["new"] == 0           # same headlines -> nothing new
    assert len(sh2.tabs[ng.ITEMS_TAB]) - 1 == n_items
    fed = [r for r in sh2.tabs[ng.ECON_TAB][1:] if r[3] == "Fed Interest Rate Decision"]
    assert len(fed) == 1 and fed[0][8] == 3.5 and fed[0][10] == 0.0
    assert len(sh2.tabs[ng.GATE_TAB]) == 3


def test_g7_memory_keeps_a_confirmed_situation_across_runs(armed):
    """v1.0.2: on 27 Oct runs eight minutes apart read NO_NEW_ENTRY, then
    CAUTION, because the second fetch missed the wire story (GDELT
    throttled). The second run must still see what the first recorded."""
    (rc, rep), sh = _run()
    assert rep["gate"]["verdict"] == "NO_NEW_ENTRY"
    writes = list(sh.writes)
    later = NOW + dt.timedelta(hours=2)
    (rc2, rep2), _ = _run(http=FakeHTTP(fail={"gdeltproject": 429}), sheets=sh, now=later, write=False)
    assert rep2["gate"]["verdict"] == "NO_NEW_ENTRY"          # v1.0.1: CAUTION (one Arabic outlet left)
    assert rep["counts"]["carried"] == 0
    assert any(d.startswith("macro: war_escalation/hormuz grade 1, 4 sources") for d in rep2["gate"]["drivers"])
    assert rep2["counts"]["carried"] >= 3                     # reuters, cnbc, aljazeera from run 1
    assert sh.writes == writes                                # a dry run only reads the memory
    (rc3, rep3), sh3 = _run(http=FakeHTTP(fail={"gdeltproject": 429}), sheets=sh, now=later)
    assert rep3["counts"]["new"] == 0                         # nothing carried is written again
    assert sh3.tabs[ng.GATE_TAB][-1][7].endswith(" carried")


def test_g7_memory_respects_the_verdict_windows(armed):
    (rc, rep), sh = _run()
    late = NOW + dt.timedelta(hours=30)       # the Hormuz stories are > 24 h old by then
    mem = ng.read_memory(sh, FakeSheets.SID, late, 1500)
    assert mem and {m["scope"] for m in mem} == {"FRST.US"}   # a name story keeps 72 h
    assert {m["source"] for m in mem} <= set(ng.MEMORY_SOURCES)   # EDGAR / Fed re-read whole
    assert all(m["memory"] and m["published"] >= late - dt.timedelta(hours=72) for m in mem)
    (rc2, rep2), _ = _run(http=FakeHTTP(fail={"gdeltproject": 429}), sheets=sh, now=late, write=False)
    assert not any("war_escalation/hormuz grade 1" in d for d in rep2["gate"]["drivers"])
    assert ng.read_memory(sh, FakeSheets.SID, late, 0) == []                 # 0 = off
    assert ng.read_memory(workbook(), FakeSheets.SID, late, 1500) == []      # no tab yet


def test_g7_memory_carries_only_the_selected_sources(armed):
    """Codex P2 on #862: a market-only run must not be moved by remembered
    GDELT / Google-News headlines."""
    (rc, rep), sh = _run()
    later = NOW + dt.timedelta(hours=2)
    (rc2, rep2), _ = _run(sheets=sh, now=later, write=False, sources=["market"])
    assert rep2["counts"]["carried"] == 0
    assert rep2["gate"]["verdict"] == "CAUTION"               # the static FOMC schedule only
    assert not any(d.startswith("macro") for d in rep2["gate"]["drivers"])
    only_gn = ng.read_memory(sh, FakeSheets.SID, later, 1500, ["google_news"])
    assert only_gn and {m["source"] for m in only_gn} == {"google_news"}
    assert ng.read_memory(sh, FakeSheets.SID, later, 1500, ["edgar", "market"]) == []


def test_g7_memory_drops_a_name_that_left_the_scope(armed):
    (rc, rep), sh = _run()
    later = NOW + dt.timedelta(hours=2)
    every = ng.read_memory(sh, FakeSheets.SID, later, 1500)
    assert "FRST.US" in {m["scope"] for m in every}
    kept = ng.read_memory(sh, FakeSheets.SID, later, 1500, ng.MEMORY_SOURCES, ["5023.SR", "BADCO.US"])
    assert "FRST.US" not in {m["scope"] for m in kept}
    assert {m["scope"] for m in kept} >= {"MACRO"}                       # macro is always carried


def test_g7_english_wires_confirm_without_gdelt(armed):
    """v1.0.2: GDELT throttled the runner on 27 Oct; Google-News in English
    reaches Reuters / AP / BBC on its own."""
    http = FakeHTTP(fail={"gdeltproject": 429}, en_rss=GNEWS_EN_RSS)
    (rc, rep), _ = _run(http=http, write=False)
    assert rep["gate"]["verdict"] == "NO_NEW_ENTRY"
    assert any(d.startswith("macro: war_escalation/hormuz grade 1") for d in rep["gate"]["drivers"])
    scope = ng.read_scope(workbook(), FakeSheets.SID, 15)
    gq, nq = ng.build_queries(scope)
    assert len(gq) == 7                                       # GDELT unchanged
    en = [q for q in nq if q[3] == "en"]
    assert [q[1] for q in en if q[0] == "MACRO"] == ["geopolitics en", "oil en", "rates en", "markets en"]
    assert {q[0] for q in en if q[0] != "MACRO"} == {"FRST.US", "BADCO.US"}
    assert [q[0] for q in nq if q[3] == "en-sa"] == ["5023.SR"]


def test_g7_article_keeps_macro_and_company_scope():
    """Codex P2 on #859: URL-only dedupe dropped the company association."""
    clf = ng.Classifier(ng.load_taxonomy())
    url = "https://www.reuters.com/acme"
    raw = [{"source": "gdelt", "scope": "MACRO", "title": "Acme earnings miss rattles Wall Street",
            "url": url, "domain": "reuters.com", "published": QUIET},
           {"source": "gdelt", "scope": "ACME.US", "title": "Acme earnings miss rattles Wall Street",
            "url": url, "domain": "reuters.com", "published": QUIET}]
    items = ng.classify_all(raw, clf, QUIET)
    assert [i["scope"] for i in items] == ["MACRO", "ACME.US"]
    assert items[1]["event_type"] == "earnings" and items[1]["direction"] == -1
    names = ng.evaluate(ng.cluster_items([i for i in items if i.get("event_type")]), [], {}, QUIET)["names"]
    assert names["ACME.US"][0]["level"] == "WATCH"


def test_g7_breaker_stops_a_dead_source(armed):
    """v1.0.1: dry run 40595558188 sat in collection > 10 min; a provider
    that fails twice in a row is skipped for the rest of the run."""
    http = FakeHTTP(fail={"gdeltproject": -1})
    (rc, rep), _ = _run(http=http, write=False, sources=["gdelt"])
    gd = [u for u in http.calls if "gdeltproject" in u]
    assert len(gd) == 2 * ng.BREAKER_FAILURES              # two queries, two attempts each
    note = rep["sources_failed"]["gdelt"]
    assert "circuit open after 2 consecutive failures" in note and "queries skipped" in note


def test_g7_time_budget_bounds_a_slow_source(armed):
    t = [0.0]

    class SlowHTTP(FakeHTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            if "gdeltproject" in url:
                t[0] += 100.0
            return super().get(url, params=params, headers=headers, timeout=timeout)

    http = SlowHTTP()
    scope = ng.read_scope(workbook(), FakeSheets.SID, 15)
    col = ng.Collector(http, _sleep, NOW, clock=lambda: t[0])
    gq, _ = ng.build_queries(scope)
    assert len(gq) == 7
    col.gdelt(gq, 0.0)
    assert len([u for u in http.calls if "gdeltproject" in u]) == 3   # starts at 0, 100, 200 s
    assert "time budget 240s spent; 4 queries skipped" in col.failed["gdelt"]


def test_g7_request_timeout_is_bounded(armed, monkeypatch):
    seen = []

    class T(FakeHTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            seen.append(timeout)
            return super().get(url, params=params, headers=headers, timeout=timeout)

    col = ng.Collector(T(), _sleep, NOW)
    col.fed_rss()
    monkeypatch.setenv("TFB_NEWS_GATE_TIMEOUT_S", "500")
    ng.Collector(T(), _sleep, NOW).fed_rss()
    assert seen == [20.0, 60.0]


def test_g7_dry_run_writes_nothing(armed):
    (rc, rep), sh = _run(write=False)
    assert rc == 0 and rep["wrote"] is False
    assert sh.writes == [] and ng.GATE_TAB not in sh.tabs


def test_g7_failures_are_named_and_key_scrubbed(armed):
    (rc, rep), _ = _run(http=FakeHTTP(fail={"eodhd.com": -1}))
    assert rc == 0
    for src in ("econ", "market"):
        assert src in rep["sources_failed"]
        assert KEY not in rep["sources_failed"][src]
    (rc, rep), _ = _run(http=FakeHTTP(fail={"sec.gov": 500, "gdeltproject": 500, "news.google": 500,
                                            "federalreserve": 500, "eodhd.com": 500}))
    assert rc == 1 and rep["sources_ok"] == {}


# ----------------------------------------------------------------------------- G8
def test_g8_main_paths(armed, monkeypatch, capsys):
    monkeypatch.delenv("DEFAULT_SPREADSHEET_ID")
    assert ng.main(["--write"], http=FakeHTTP(), sheets=None, now=NOW, sleep=_sleep) == 2
    monkeypatch.setenv("DEFAULT_SPREADSHEET_ID", FakeSheets.SID)
    ev_dir = armed / "ev"
    summary = armed / "summary.md"
    monkeypatch.setenv("TFB_NEWS_GATE_EVIDENCE_DIR", str(ev_dir))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    monkeypatch.setenv("TFB_NEWS_GATE_SYMBOLS", "ACME.US:Acme Corp;2222.SR")
    monkeypatch.setenv("TFB_NEWS_GATE_SOURCES", "edgar,econ,market,fed_rss")
    sh = workbook()
    rc = ng.main(["--dry-run"], http=FakeHTTP(), sheets=sh, now=NOW, sleep=_sleep)
    assert rc == 0 and sh.writes == []
    out = capsys.readouterr().out
    assert "**Verdict: CAUTION**" in out and "verdict=CAUTION" in out
    data = json.loads((ev_dir / "news_gate.json").read_text())
    assert data["gate"]["verdict"] == "CAUTION"
    assert {s["symbol"] for s in data["scope"]} >= {"ACME.US", "2222.SR", "FRST.US"}
    assert "Verdict: CAUTION" in summary.read_text()
    assert (ev_dir / "news_gate.md").read_text().startswith("## News & macro gate v1.0.2")


def test_g8_write_without_credentials_fails_loudly(armed, capsys):
    """Codex P2 on #859: --write with a sheet id but no usable credentials
    used to run as a quiet dry run and exit 0."""
    http = FakeHTTP()
    assert ng.main(["--write"], http=http, sheets=None, now=NOW, sleep=_sleep) == 2
    assert http.calls == []
    assert "--write needs readable GOOGLE_SHEETS_CREDENTIALS" in capsys.readouterr().out


def test_g8_bad_now(armed):
    assert ng.main(["--now", "yesterday"], http=FakeHTTP(), sheets=workbook(), sleep=_sleep) == 2


def test_g8_selected_sources():
    assert ng.selected_sources("") == list(ng.ALL_SOURCES)
    assert ng.selected_sources("GDELT, econ ,nope") == ["gdelt", "econ"]


# ----------------------------------------------------------------------------- G9
def test_g9_ascii_module_version_pin_and_workflow():
    src = (_REPO / "scripts" / "news_macro_gate.py").read_bytes()
    assert all(b < 128 for b in src)
    assert ng.__version__ == "1.0.2"
    vd = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("news_macro_gate", "1.0.2", "news macro gate")' in vd
    import yaml
    wf = yaml.safe_load((_REPO / ".github" / "workflows" / "news_macro_gate.yml").read_text(encoding="utf-8"))
    on = wf.get("on") or wf.get(True)
    crons = [c["cron"] for c in on["schedule"]]
    assert crons == ["41 6 * * 0-4", "47 12 * * 1-5", "23 19 * * 1-5"]
    assert on["workflow_dispatch"]["inputs"]["write"]["default"] is False
    assert wf["permissions"] == {"contents": "read"}
    steps = wf["jobs"]["gate"]["steps"]
    run_step = next(s for s in steps if s.get("name", "").endswith("Run the gate"))
    env = run_step["env"]
    assert env["TFB_NEWS_GATE"] == "${{ vars.TFB_NEWS_GATE || 'observe' }}"
    assert env["EODHD_API_KEY"] == "${{ secrets.EODHD_API_KEY }}"
    assert "secrets.TFB_SEC_USER_AGENT" in env["TFB_SEC_USER_AGENT"]
    assert '--dry-run' in run_step["run"] and 'python scripts/news_macro_gate.py $MODE' in run_step["run"]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
