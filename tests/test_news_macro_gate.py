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
    assert len(tax["events"]) == 27 and tax["version"] == "1.2.0"     # v1.3.0 [TRADE-RELIEF]: three word lists, no new type
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
    assert len(gq) == 10                                      # 8 macro (v1.2.0: +3 topics) + 2 names
    en = [q for q in nq if q[3] == "en"]
    assert [q[1] for q in en if q[0] == "MACRO"] == ["geopolitics en", "oil en", "rates en", "markets en",
                                                     "trade china en", "ai tech en", "politics en"]
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
    assert len(gq) == 10
    col.gdelt(gq, 0.0)
    assert len([u for u in http.calls if "gdeltproject" in u]) == 3   # starts at 0, 100, 200 s
    assert "time budget 240s spent; 7 queries skipped" in col.failed["gdelt"]


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
    assert (ev_dir / "news_gate.md").read_text().startswith("## News & macro gate v1.3.0")


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
    assert ng.__version__ == "1.3.0"   # 2026-11-19: v1.3.0 [TRADE-RELIEF]
    vd = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("news_macro_gate", "1.3.0", "news macro gate")' in vd
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


# ----------------------------------------------------------------------------- G10 (v1.2.0 [TOPICS])
TOPIC_CASES = [
    # subjects the gate had no word for (archive, 12 - 19 Nov 2026)
    ("Trump, Xi and Putin open first three-way summit in Shenzhen", "summit_diplomacy", 0),
    ("US and China agree trade deal as stocks rise", "trade_deal", 1),
    ("Washington eases export controls on chips to China", "trade_deal", 1),
    ("APEC leaders meet as exports drop", "summit_diplomacy", -1),
    ("G20 finance chiefs gather for high-level talks", "summit_diplomacy", 0),
    ("AI bubble fears grow as Nvidia falls", "ai_tech", -1),
    ("OpenAI spending plans surge to a record", "ai_tech", 1),
    ("Election results due as polls close", "election_politics", 0),
    ("Government shutdown deal reached in Senate", "election_politics", 0),
    ("\u062a\u0631\u0627\u0645\u0628 \u0648\u0634\u064a \u0648\u0628\u0648\u062a\u064a\u0646 \u0641\u064a \u0642\u0645\u0629 \u062b\u0644\u0627\u062b\u064a\u0629", "summit_diplomacy", 0),
    ("\u0648\u0627\u0634\u0646\u0637\u0646 \u0648\u0628\u0643\u064a\u0646 \u062a\u062a\u0648\u0635\u0644\u0627\u0646 \u0625\u0644\u0649 \u0627\u062a\u0641\u0627\u0642 \u062a\u062c\u0627\u0631\u064a", "trade_deal", 1),
    ("\u0645\u062e\u0627\u0648\u0641 \u0645\u0646 \u0641\u0642\u0627\u0639\u0629 \u0627\u0644\u0630\u0643\u0627\u0621 \u0627\u0644\u0627\u0635\u0637\u0646\u0627\u0639\u064a", "ai_tech", 0),
    ("\u0646\u062a\u0627\u0626\u062c \u0627\u0644\u0627\u0646\u062a\u062e\u0627\u0628\u0627\u062a \u0627\u0644\u0623\u0645\u0631\u064a\u0643\u064a\u0629", "election_politics", 0),
]


@pytest.mark.parametrize("title,etype,direction", TOPIC_CASES)
def test_g10_topics_are_named(title, etype, direction):
    got = ng.Classifier(ng.load_taxonomy()).classify(title, "MACRO")
    assert (got["event_type"], got["direction"]) == (etype, direction), got


OLDER_MACRO = ["war_escalation", "de_escalation", "emergency_policy", "market_turmoil", "oil_supply", "central_bank",
               "inflation_data", "jobs_data", "trade_sanctions", "credit_stress"]
LABEL_ONLY = ["trade_deal", "summit_diplomacy", "ai_tech", "election_politics"]

# A headline that names an older subject beside a new one keeps the older reading (Codex P2, #1031).
MIXED_CASES = [
    ("China faces recession as election uncertainty grows", "credit_stress"),
    ("Election result sparks recession fears", "credit_stress"),
    ("Summit overshadowed as debt default looms", "credit_stress"),
    ("Nvidia and AI stocks slide as default risk rises", "credit_stress"),
    ("AI bubble bursts as bank run hits lenders", "emergency_policy"),
    ("Trade deal collapses as emergency rate cut announced", "emergency_policy"),
    ("Circuit breaker halts trading after trade deal hopes fade", "market_turmoil"),
    ("Stocks plunge as summit ends without trade agreement", "market_turmoil"),
    ("Trade agreement signed as ceasefire holds", "de_escalation"),
    ("Israel and Hamas agree ceasefire deal", "de_escalation"),
    ("Truce holds in Gaza for a third day", "de_escalation"),
    ("Missile strike reported as leaders gather for summit", "war_escalation"),
    ("FOMC minutes and APEC Summit in focus.", "central_bank"),
    ("OPEC+ summit ends with output cut", "oil_supply"),
    ("Trump threatens new tariff on China ahead of summit", "trade_sanctions"),
    ("Sanctions on chip export widen as AI trade cools", "trade_sanctions"),
    ("Fed holds rates steady as Powell signals patience", "central_bank"),
    ("Recession fears return", "credit_stress"),
]


def _older_only(tax):
    """The taxonomy as it was before the label-only types: the same file without them."""
    return dict(tax, events=[e for e in tax["events"] if not e.get("label_only")])


def test_g10_order_keeps_the_older_readings():
    tax = ng.load_taxonomy()
    macro = [e["type"] for e in tax["events"] if e["scope"] == "MACRO"]
    assert macro == OLDER_MACRO + LABEL_ONLY                     # every older type first, in its old order
    assert [e["type"] for e in tax["events"] if "label_only" in e] == LABEL_ONLY
    new = {e["type"]: e for e in tax["events"] if e["type"] in LABEL_ONLY}
    assert {t: (e["scope"], e["direction"], e["severity"], e["label_only"]) for t, e in new.items()} == {
        "trade_deal": ("MACRO", 1, "M", True), "summit_diplomacy": ("MACRO", 0, "M", True),
        "ai_tech": ("MACRO", 0, "M", True), "election_politics": ("MACRO", 0, "M", True)}
    assert all(e["en"] and e["ar"] for e in new.values())
    clf = ng.Classifier(tax)
    assert [e["type"] for e in clf.events if e["label_only"]] == LABEL_ONLY
    # only a JSON true marks a label: false or a word does not
    loose = dict(tax, events=[dict(e, label_only={"ai_tech": "yes", "summit_diplomacy": False}.get(e["type"], True))
                              if e["type"] in LABEL_ONLY else e for e in tax["events"]])
    assert [e["type"] for e in ng.Classifier(loose).events if e["label_only"]] == ["trade_deal", "election_politics"]
    for title, etype in MIXED_CASES:
        assert clf.classify(title, "MACRO")["event_type"] == etype, title
    # short words are whole words: 'apec' / 'g7' / 'g20' never match inside another word
    for title in ("Capecitabine trial data due", "Airbag7 recall widens", "Dog20 token lists"):
        assert clf.classify(title, "MACRO")["event_type"] == "", title
    # a company's own story keeps its company type
    assert clf.classify("Nvidia quarterly results beat forecasts", "NVDA.US")["event_type"] == "earnings"


def test_g10_label_only_types_change_no_older_reading():
    tax = ng.load_taxonomy()
    now, was = ng.Classifier(tax), ng.Classifier(_older_only(tax))
    assert [e["type"] for e in was.events if e["scope"] == "MACRO"] == OLDER_MACRO
    titles = [t for t, _ in MIXED_CASES] + [t for t, _, _ in TOPIC_CASES] + [r["title"] for r in GDELT_GEO["articles"]]
    kept = named = 0
    for title in titles:
        before, after = was.classify(title, "MACRO"), now.classify(title, "MACRO")
        if before["event_type"]:
            assert after == before, title                        # it had a reading: the same reading, whole
            kept += 1
        else:
            assert after["event_type"] in LABEL_ONLY + [""], title   # it had none: a label or still none
            named += after["event_type"] in LABEL_ONLY
        # a name's own headline is read exactly as before, so no new flag can reach a name
        assert now.classify(title, "NVDA.US") == was.classify(title, "NVDA.US"), title
    assert kept >= len(MIXED_CASES) and named == len(TOPIC_CASES)
    # every term of a label-only type is alive: alone, it is read as its own type
    for e in tax["events"]:
        if e.get("label_only"):
            for term in e["en"] + e["ar"]:
                assert now.classify(term, "MACRO")["event_type"] == e["type"], (e["type"], term)
                own = now.classify(term, "NVDA.US")
                assert own == was.classify(term, "NVDA.US") and own["event_type"] not in LABEL_ONLY, (e["type"], term)


def test_g10_new_types_never_move_the_verdict():
    src = (_REPO / "scripts" / "news_macro_gate.py").read_text(encoding="utf-8")
    body = src.split("def evaluate(", 1)[1].split("\n# ---", 1)[0]
    for t in ("trade_deal", "summit_diplomacy", "ai_tech", "election_politics"):
        assert t not in body, t
    items = ng.parse_gdelt(gdelt_articles([
        ("https://www.reuters.com/s1", "Trump, Xi and Putin open summit in Shenzhen, China", "20261030T100000Z", "reuters.com"),
        ("https://apnews.com/s2", "China hosts Trump and Putin for summit", "20261030T101000Z", "apnews.com"),
        ("https://www.bbc.com/s3", "China summit: leaders meet in Shenzhen", "20261030T102000Z", "bbc.com"),
        ("https://www.reuters.com/a1", "AI bubble fears grow as Nvidia falls on China chip export worries", "20261030T100500Z", "reuters.com"),
        ("https://apnews.com/a2", "Nvidia falls as AI bubble fears hit China chip export hopes", "20261030T101500Z", "apnews.com"),
        ("https://www.bbc.com/a3", "China election politics weigh on markets", "20261030T102500Z", "bbc.com"),
    ]), "MACRO", "geo")
    cl = _clusters(items, QUIET)
    assert {c["event_type"] for c in cl} == {"summit_diplomacy", "ai_tech", "election_politics"}
    g = ng.evaluate(cl, [], {}, QUIET)
    assert g["verdict"] == "GO" and g["drivers"] == [] and g["positives"] == [] and g["names"] == {}
    # an older subject named beside a new one still counts: a recession story with an election word is CAUTION
    mixed = ng.parse_gdelt(gdelt_articles([
        ("https://www.reuters.com/m1", "China faces recession as election uncertainty grows", "20261030T100000Z", "reuters.com"),
        ("https://apnews.com/m2", "China faces recession as election uncertainty grows", "20261030T101000Z", "apnews.com"),
    ]), "MACRO", "politics")
    cm = _clusters(mixed, QUIET)
    assert [(c["event_type"], c["anchor"]) for c in cm] == [("credit_stress", "china")]
    gm = ng.evaluate(cm, [], {}, QUIET)
    assert gm["verdict"] == "CAUTION" and len(gm["drivers"]) == 1
    assert gm["drivers"][0].startswith("macro: credit_stress/china grade ")
    # a name's own headline on AI or an election takes no label-only type, so the name is not flagged
    own = ng.parse_gdelt(gdelt_articles([
        ("https://www.reuters.com/n1", "AI bubble fears grow as Nvidia falls", "20261030T100000Z", "reuters.com"),
        ("https://apnews.com/n2", "Nvidia shares drop on election uncertainty", "20261030T101000Z", "apnews.com"),
    ]), "NVDA.US", "name")
    assert _clusters(own, QUIET) == []
    assert ng.evaluate(_clusters(own, QUIET), [], {}, QUIET)["names"] == {}


# ----------------------------------------------------------------------------- G11 (v1.3.0 [TRADE-RELIEF])
TRADE_LISTS = ("trade_truce", "trade_relief", "trade_hedge")
EVALUATE_SHA = "9304bc0e24abe27b4c18f0f37b7902887f36175968af74db89c0e1ed5ec485be"      # evaluate() of v1.1.0, v1.2.0 and now

# (headline, reading before v1.3.0, reading now, direction now). World headlines of 5 - 18 Nov 2026 (GDELT) and the
# gate's own archive; the last block is written to probe the guards.
TRADE_CASES = [
    # (A) a trade truce is not a ceasefire
    ("What's at stake for Trump and Xi in Shenzhen: A trade truce deadline, new export controls, and Iran",
     "de_escalation", "trade_deal", 1),
    ("China extends US trade truce by two months", "de_escalation", "trade_deal", 1),
    ("China extends its trade truce with Trump for 2 more months. The rare-earth dispute is still unresolved.",
     "de_escalation", "trade_deal", 1),
    ("Nvidia fallout puts trade truce to test as Xi Jinping welcomes Trump", "de_escalation", "trade_deal", -1),
    ("China extends 24% US tariff suspension, but Section 301 threat clouds trade truce", "de_escalation", "trade_deal", -1),
    ("US presses China to honor Busan trade truce as rare earth shipments fall short.", "de_escalation", "trade_deal", -1),
    ("How Trump and Xi went from tariff war to trade truce", "de_escalation", "trade_deal", -1),
    ("Tariff truce extended by 90 days", "de_escalation", "trade_deal", 1),
    ("US-China trade truce falters", "de_escalation", "trade_deal", -1),                       # a breach word alone
    ("\u0627\u0644\u0635\u064a\u0646 \u062a\u0645\u062f\u062f \u0627\u0644\u0647\u062f\u0646\u0629 \u0627\u0644\u062a\u062c\u0627\u0631\u064a\u0629 \u0645\u0639 \u0648\u0627\u0634\u0646\u0637\u0646",
     "de_escalation", "trade_deal", 1),                                       # China extends the trade truce with Washington
    ("\u0647\u062f\u0646\u0629 \u062a\u062c\u0627\u0631\u064a\u0629 \u0628\u064a\u0646 \u0648\u0627\u0634\u0646\u0637\u0646 \u0648\u0628\u0643\u064a\u0646",
     "de_escalation", "trade_deal", 1),                                       # a trade truce between Washington and Beijing
    # ... and a real truce stays one, also beside a trade truce
    ("Israel and Hamas agree ceasefire deal", "de_escalation", "de_escalation", 1),
    ("Gaza truce holds as US-China trade truce nears", "de_escalation", "de_escalation", 1),
    ("Peace talks resume as trade truce holds", "de_escalation", "de_escalation", 1),
    ("\u062a\u0631\u0645\u0628 \u064a\u062a\u0631\u0627\u062c\u0639 \u0639\u0646 \u0647\u062f\u0646\u0629 \u0625\u064a\u0631\u0627\u0646",
     "de_escalation", "de_escalation", 1),                                    # Trump backs away from the Iran truce
    # (B) a tariff cut is not trade pressure
    ("China and Canada agree on tariff relief on key exports", "trade_sanctions", "trade_deal", 1),
    ("China cuts tariffs on Canadian exports as Xi meets Carney", "trade_sanctions", "trade_deal", 1),
    ("China eases tariffs on Canadian pork | The Paris Star", "trade_sanctions", "trade_deal", 1),
    ("Canada, China roll back more tariffs as Carney meets with Xi", "trade_sanctions", "trade_deal", 1),
    ("China and Canada agree to lift more tariffs", "trade_sanctions", "trade_deal", 1),
    ("Mark Carney and Xi Jinping strike new trade deal to remove tariffs on Canadian goods", "trade_sanctions", "trade_deal", 1),
    ("Thailand to remove tariffs on US exports under trade deal, Trump says", "trade_sanctions", "trade_deal", 1),
    ("China rolls back tariffs on Canadian pork, seafood in new trade deal with Ottawa", "trade_sanctions", "trade_deal", 1),
    ("Ottawa amends, extends tariff relief program beyond U.S. steel imports", "trade_sanctions", "trade_deal", 1),
    ("US assures India lower tariffs than peers in trade deal", "trade_sanctions", "trade_deal", 1),
    ("US and China agree trade deal as tariff relief lifts stocks higher", "trade_sanctions", "trade_deal", 1),
    ("China extends tariff suspension on US goods", "trade_sanctions", "trade_deal", 1),
    ("China cuts tariff on Canadian pork", "trade_sanctions", "trade_deal", 1),
    ("Trump lifts all steel tariffs on Canada", "trade_sanctions", "trade_deal", 1),          # two words between verb and 'tariffs'
    # ... and a hedged, undone or mixed one keeps its older, cautious reading
    ("China open to increasing US agricultural imports, contingent on 30 billion reciprocal tariff cuts: MOFCOM",
     "trade_sanctions", "trade_sanctions", -1),
    ("With 400 jobs on hold, RYAM employees hope U.S. election results spark tariff relief", "trade_sanctions", "trade_sanctions", -1),
    ("DTI seeks lower US tariffs on PH exports | The Manila Times", "trade_sanctions", "trade_sanctions", -1),
    ("Thailand to seek US tariff cut after reaching key terms of trade deal, says minister", "trade_sanctions", "trade_sanctions", -1),
    ("Germany urges RI to cut tariffs, expand investment in industry, energy", "trade_sanctions", "trade_sanctions", -1),
    ("China, US discuss tariff cuts, agricultural trade ahead of leaders' meeting", "trade_sanctions", "trade_sanctions", -1),
    ("Ajmal slams Assam government over rising prices, demands market regulation and tariff rollback",
     "trade_sanctions", "trade_sanctions", -1),
    ("Exclusive | Trump Administration Promised Tariff Relief in Talks to Secure U.S. Audi Factory",
     "trade_sanctions", "trade_sanctions", -1),
    ("Sen.-elect Josh Turek says ending Iran war, rolling back Trump tariffs should be top priorities",
     "trade_sanctions", "trade_sanctions", -1),
    ("Trump threatens new tariffs on China ahead of summit", "trade_sanctions", "trade_sanctions", -1),
    ("Trump raises tariffs on China after Canada cuts tariffs", "trade_sanctions", "trade_sanctions", -1),
    ("China suspends tariffs on US farm goods as sanctions on Iran tighten", "trade_sanctions", "trade_sanctions", -1),
    ("Trump drops plan to cut tariffs on steel", "trade_sanctions", "trade_sanctions", -1),
    ("Will China cut tariffs on US soybeans?", "trade_sanctions", "trade_sanctions", -1),
    ("China cuts tariffs on US pork\u061f", "trade_sanctions", "trade_sanctions", -1),            # an Arabic question mark
    ("China will not cut tariffs, ministry says", "trade_sanctions", "trade_sanctions", -1),
    ("China won't cut tariffs, ministry says", "trade_sanctions", "trade_sanctions", -1),
    ("China denies it will cut tariffs", "trade_sanctions", "trade_sanctions", -1),
    ("China is considering tariff cuts, sources said", "trade_sanctions", "trade_sanctions", -1),
    ("Tariff relief ends as exemptions lapse", "trade_sanctions", "trade_sanctions", -1),
    ("White House scraps tariff relief for steel", "trade_sanctions", "trade_sanctions", -1),
    ("China ends the tariff pause on US goods", "trade_sanctions", "trade_sanctions", -1),
    ("Beijing cuts tariff exemptions for US chips", "trade_sanctions", "trade_sanctions", -1),
    ("Trump lifts all the steel tariffs on Canada", "trade_sanctions", "trade_sanctions", -1),   # three words: out of reach
    ("China reportedly cuts tariffs on US pork", "trade_sanctions", "trade_sanctions", -1),       # a rumour is not relief
    ("China will cut tariffs if US lifts chip curbs", "trade_sanctions", "trade_sanctions", -1),  # a condition is not relief
    ("Tariff cut talks collapse", "trade_sanctions", "trade_sanctions", -1),
    ("Tariff relief deal falters in Senate", "trade_sanctions", "trade_sanctions", -1),          # a breach word alone
    # Codex P2 x2 on #1033: a pressure word between the verb and 'tariffs' still counts; a rejection in any tense
    ("China lifts sanctions and tariffs on Iran", "trade_sanctions", "trade_sanctions", -1),
    ("Trump lifts embargo and tariffs on Cuba", "trade_sanctions", "trade_sanctions", -1),
    ("China tariff cuts ruled out by officials", "trade_sanctions", "trade_sanctions", -1),
    ("China rules out tariff cuts", "trade_sanctions", "trade_sanctions", -1),
    ("Officials ruling out tariff cuts", "trade_sanctions", "trade_sanctions", -1),
    ("Beijing rule out tariff cuts for now", "trade_sanctions", "trade_sanctions", -1),
    # relief undone after the phrase: a past form of a relief verb
    ("Tariff pause lifted as deadline passes", "trade_sanctions", "trade_sanctions", -1),
    ("Tariff cuts dropped from the bill", "trade_sanctions", "trade_sanctions", -1),
    ("Tariff relief scrapped by White House", "trade_sanctions", "trade_sanctions", -1),
    ("Tariff exemptions removed for chips", "trade_sanctions", "trade_sanctions", -1),
    # a relief that is not there, not yet, or not what it seems
    ("No tariff relief for Canada, Trump says", "trade_sanctions", "trade_sanctions", -1),
    ("Tariff relief unlikely this year, minister says", "trade_sanctions", "trade_sanctions", -1),
    ("China would cut tariffs on US pork", "trade_sanctions", "trade_sanctions", -1),
    ("China cuts tariffs after talks with Canada", "trade_sanctions", "trade_sanctions", -1),     # cautious: 'talks'
    ("Senate pushed for tariff relief", "trade_sanctions", "trade_sanctions", -1),
    ("Lobby called for tariff cuts", "trade_sanctions", "trade_sanctions", -1),
    ("Exporters sought tariff relief", "trade_sanctions", "trade_sanctions", -1),
    ("Ministers eyed tariff cuts", "trade_sanctions", "trade_sanctions", -1),
    ("Farmers asked for tariff relief", "trade_sanctions", "trade_sanctions", -1),
    ("China hiked duties as Canada cuts tariffs", "trade_sanctions", "trade_sanctions", -1),
    ("Canada cuts tariff-free quota for steel", "trade_sanctions", "trade_sanctions", -1),
    ("Tariff pause expiring next week", "trade_sanctions", "trade_sanctions", -1),
    ("Senators oppose tariff relief for China", "trade_sanctions", "trade_sanctions", -1),
    ("White House walks back tariff relief", "trade_sanctions", "trade_sanctions", -1),
    # sanctions relief is not in this version; plain sanctions and tariffs read as before
    ("EU lifts sanctions on Syria", "trade_sanctions", "trade_sanctions", -1),
    ("US sanctions Hezbollah-linked charity in bid to squeeze Iran", "trade_sanctions", "trade_sanctions", -1),
    ("Stocks surge as tariff concerns ease, oil prices fall", "oil_supply", "oil_supply", 1),
    # known limit, as before: a fighting headline that names a trade truce is not read as fighting
    ("Missile strike reported as US-China trade truce holds", "de_escalation", "trade_deal", 1),
    ("Missile strike breaks calm as trade truce collapses", "war_escalation", "war_escalation", -1),
]


def _v120(tax):
    """The taxonomy as v1.2.0 read it: the same file without the three trade lists."""
    return {k: v for k, v in tax.items() if k not in TRADE_LISTS}


@pytest.mark.parametrize("title,before,after,direction", TRADE_CASES)
def test_g11_trade_truce_and_tariff_relief(title, before, after, direction):
    tax = ng.load_taxonomy()
    was, now = ng.Classifier(_v120(tax)).classify(title, "MACRO"), ng.Classifier(tax).classify(title, "MACRO")
    assert was["event_type"] == before, was
    assert (now["event_type"], now["direction"]) == (after, direction), now
    if before == after:
        assert now == was                                         # untouched: the same reading, whole
    else:
        assert (before in ("de_escalation", "trade_sanctions")) and after == "trade_deal"
        assert (now["severity"], now["anchor"], now["rumor"], now["denial"]) == ("M", was["anchor"], was["rumor"], was["denial"])
    # a name's own headline: the moved reading is dropped, every other one is the same
    own_was, own_now = ng.Classifier(_v120(tax)).classify(title, "ZZZ.US"), ng.Classifier(tax).classify(title, "ZZZ.US")
    if before != after:
        assert own_was["event_type"] == before and own_now["event_type"] == "" and own_now["direction"] == 0
    else:
        assert own_now == own_was


def test_g11_a_reading_can_only_leave_a_verdict_rule():
    tax = ng.load_taxonomy()
    for key in TRADE_LISTS:
        assert tax[key]["en"] and tax[key]["ar"], key
    assert tax["trade_relief"]["verbs"] and tax["trade_relief"]["objects"] == ["tariff", "tariffs"]
    hedge = {h.strip() for h in tax["trade_hedge"]["en"]}
    assert not hedge & set(tax["trade_relief"]["verbs"])            # a relief verb is never itself a hedge
    # a rejection or a request guards in every tense (Codex P2, #1033: 'ruled out' was missing)
    c0 = ng.Classifier(tax)
    guard = c0.hedge
    for stem, forms in (("rule out", ("rule out", "rules out", "ruled out", "ruling out")),
                        ("call for", ("call for", "calls for", "called for", "calling for")),
                        ("push", ("push for", "pushes for", "pushed for", "pushing for")),
                        ("seek", ("seek", "seeks", "seeking", "sought")), ("ask", ("ask", "asks", "asked", "asking")),
                        ("eye", ("eye", "eyes", "eyed", "eyeing")), ("hike", ("hike", "hikes", "hiked", "hiking")),
                        ("raise", ("raise", "raises", "raised", "raising")), ("expire", ("expire", "expires", "expired", "expiring", "expiry")),
                        ("shelve", ("shelve", "shelves", "shelved", "shelving")), ("withdraw", ("withdraw", "withdraws", "withdrew", "withdrawn")),
                        ("urge", ("urge", "urges", "urged", "urging")), ("weigh", ("weigh", "weighs", "weighed", "weighing")),
                        ("threaten", ("threat", "threatens", "threatened", "threatening")), ("walk back", ("walk back", "walks back", "walked back")),
                        ("fall short", ("fall short", "falls short", "fell short", "falling short of")),
                        ("end", ("end", "ends", "ended", "ending")), ("delay", ("delay", "delays", "delayed", "delaying")),
                        ("warn", ("warn", "warns", "warned", "warning")), ("veto", ("veto", "vetoes", "vetoed")),
                        ("demand", ("demand", "demands", "demanded")), ("reject", ("reject", "rejects", "rejected")),
                        ("plan", ("plan", "plans", "planned", "planning")), ("discuss", ("discuss", "discusses", "discussed")),
                        ("block", ("block", "blocks", "blocked")), ("cancel", ("cancel", "cancels", "cancelled", "canceled")),
                        ("pledge", ("pledge", "pledges", "pledged", "pledging")), ("promise", ("promise", "promises", "promised", "promising")),
                        ("hope", ("hope", "hopes", "hoped", "hoping")), ("lapse", ("lapse", "lapses", "lapsed", "lapsing")),
                        ("propose", ("propose", "proposes", "proposed", "proposing", "proposal")),
                        ("consider", ("consider", "considers", "considered", "considering")),
                        ("oppose", ("oppose", "opposes", "opposed", "opposing")), ("deny", ("deny", "denies", "denied", "denying")),
                        ("refuse", ("refuse", "refuses", "refused", "refusing")), ("collapse", ("collapse", "collapses", "collapsed"))):
        for form in forms:
            norm = ng.normalize_text("x %s y" % form)
            assert ng._hit(norm, guard) or ng._hit(norm, c0.breach) or ng._hit(norm, c0.denial), (stem, form)
    # the words between the verb and 'tariffs' are kept when the match is set aside
    c = ng.Classifier(tax)
    assert c._without(" china lifts sanctions and tariffs on iran ", c.relief, c.relief_re) == " china sanctions and on iran "
    assert c._without(" china cuts tariffs on pork ", c.relief, c.relief_re) == " china on pork "
    assert c._without(" a trade truce deadline ", c.truce) == " a deadline "
    now, was = ng.Classifier(tax), ng.Classifier(_v120(tax))
    assert was.truce == [] and was.relief == [] and was.hedge == [] and was.relief_re is None and was.undo_re is None
    titles = ([t for t, *_ in TRADE_CASES] + [t for t, _ in MIXED_CASES] + [t for t, _, _ in TOPIC_CASES]
              + [r["title"] for r in GDELT_GEO["articles"]] + [c[0] for c in ISSUER_CASES])
    moved = 0
    for title in titles:
        for scope in ("MACRO", "ZZZ.US"):
            a, b = was.classify(title, scope), now.classify(title, scope)
            if a != b:                                           # the only move there is
                assert a["event_type"] in ("de_escalation", "trade_sanctions"), (title, scope)
                assert b["event_type"] == ("trade_deal" if scope == "MACRO" else ""), (title, scope)
                moved += 1
    assert moved == 2 * sum(1 for _, b, a, _ in TRADE_CASES if b != a)
    assert now.by_type["trade_deal"]["label_only"] is True
    # the rules stand on trade_deal being a label: without the type, or with it un-marked, nothing moves
    no_type = dict(tax, events=[e for e in tax["events"] if e["type"] != "trade_deal"])
    unmarked = dict(tax, events=[dict(e, label_only=False) if e["type"] == "trade_deal" else e for e in tax["events"]])
    for odd in (no_type, unmarked):
        clf = ng.Classifier(odd)
        assert clf.classify("China extends US trade truce by two months", "MACRO")["event_type"] == "de_escalation"
        assert clf.classify("China and Canada agree on tariff relief on key exports", "MACRO")["event_type"] == "trade_sanctions"
    # no verdict rule was touched: evaluate() is byte-identical to v1.1.0 and v1.2.0
    src = (_REPO / "scripts" / "news_macro_gate.py").read_text(encoding="utf-8")
    body = "def evaluate(" + src.split("def evaluate(", 1)[1].split("\n# ---", 1)[0]
    import hashlib
    assert hashlib.sha256(body.encode("utf-8")).hexdigest() == EVALUATE_SHA
    assert "trade_deal" not in body


def _clusters_with(tax, items, now):
    clf = ng.Classifier(tax)
    return ng.cluster_items([i for i in ng.classify_all([dict(i) for i in items], clf, now)
                             if i.get("event_type") and i["event_type"] != "law_firm_noise"])


def _wires(scope, *titles):
    doms = [("https://www.reuters.com/", "reuters.com"), ("https://apnews.com/", "apnews.com"), ("https://www.bbc.com/", "bbc.com")]
    rows = []
    for n, title in enumerate(titles):
        for k, (base, dom) in enumerate(doms):
            rows.append((base + "t%d%d" % (n, k), title, "20261030T10%02d00Z" % (n * 3 + k), dom))
    return ng.parse_gdelt(gdelt_articles(rows), scope, "trade china")


def test_g11_the_verdict_can_fall_and_never_rise():
    tax = ng.load_taxonomy()

    def both(items):
        return (ng.evaluate(_clusters_with(_v120(tax), items, QUIET), [], {}, QUIET),
                ng.evaluate(_clusters_with(tax, items, QUIET), [], {}, QUIET))
    # sanctions relief beside tariffs, and a relief ruled out, are CAUTION before and now (Codex P2 x2, #1033)
    for title in ("China lifts sanctions and tariffs on Iran", "China tariff cuts ruled out by officials"):
        was, now = both(_wires("MACRO", title))
        assert was["verdict"] == now["verdict"] == "CAUTION" and was["drivers"] == now["drivers"], title
    # tariff relief from three wires on China: it raised CAUTION, now it raises nothing
    was, now = both(_wires("MACRO", "China cuts tariffs on Canadian exports as Xi meets Carney"))
    assert was["verdict"] == "CAUTION" and was["drivers"][0].startswith("macro: trade_sanctions/china grade ")
    assert now["verdict"] == "GO" and now["drivers"] == [] and now["positives"] == []
    # a trade truce from three wires: it stood on the EASING line as war de-escalation, now it does not
    was, now = both(_wires("MACRO", "China extends US trade truce by two months"))
    assert was["verdict"] == "GO" and len(was["positives"]) == 1 and was["positives"][0].startswith("de_escalation/china grade ")
    assert now["verdict"] == "GO" and now["positives"] == [] and now["drivers"] == []
    # a tariff threat, a hedged relief and a relief beside new tariffs are CAUTION before and now
    for title in ("Trump threatens new tariffs on China ahead of summit", "China, US discuss tariff cuts ahead of leaders' meeting",
                  "Trump raises tariffs on China after Canada cuts tariffs"):
        was, now = both(_wires("MACRO", title))
        assert was["verdict"] == now["verdict"] == "CAUTION" and was["drivers"] == now["drivers"], title
    # a real ceasefire still stands on the EASING line
    was, now = both(_wires("MACRO", "Israel and Hamas agree ceasefire deal in Gaza"))
    assert was["positives"] == now["positives"] and len(now["positives"]) == 1
    # one run with every headline of the corpus: no driver, easing line or name flag that was not there before
    rank = {"GO": 0, "CAUTION": 1, "NO_NEW_ENTRY": 2}
    corpus = [t for t, *_ in TRADE_CASES]
    was, now = both(_wires("MACRO", *corpus) + _wires("ZZZ.US", *corpus))
    assert rank[now["verdict"]] <= rank[was["verdict"]]
    key = lambda line: line.split(" grade ")[0]                    # noqa: E731 - the cluster a line speaks of
    assert {key(d) for d in now["drivers"]} <= {key(d) for d in was["drivers"]}
    assert {key(p) for p in now["positives"]} <= {key(p) for p in was["positives"]}
    assert set(now["names"]) <= set(was["names"])
    for sym, flags in now["names"].items():
        assert {f["level"] for f in flags} <= {f["level"] for f in was["names"][sym]}
    # a name's own tariff-relief headline flagged the name WATCH; now it does not
    was, now = both(_wires("ZZZ.US", "Zeta rises as US lifts tariffs on steel"))
    assert was["names"]["ZZZ.US"][0]["level"] == "WATCH" and "trade_sanctions negative" in was["names"]["ZZZ.US"][0]["why"]
    assert now["names"] == {}
    # ... and a name's own tariff-threat headline flags it before and now
    was, now = both(_wires("ZZZ.US", "Zeta falls as US threatens new tariffs on steel"))
    assert was["names"] == now["names"] and now["names"]["ZZZ.US"][0]["level"] == "WATCH"


def test_g10_topic_searches():
    assert [l for l, _ in ng.GDELT_MACRO_QUERIES] == ["geopolitics", "oil", "central banks", "market stress", "saudi",
                                                    "trade china", "ai tech", "politics"]
    assert [l for l, _ in ng.GNEWS_AR_QUERIES] == ["saudi market ar", "oil ar", "rates ar", "geopolitics ar",
                                                  "trade china ar", "ai tech ar"]
    assert [l for l, _ in ng.GNEWS_EN_QUERIES] == ["geopolitics en", "oil en", "rates en", "markets en",
                                                  "trade china en", "ai tech en", "politics en"]
    by = dict(ng.GDELT_MACRO_QUERIES)
    for word in ("tariff", '"trade deal"', "summit", "APEC"):
        assert word in by["trade china"]
    assert "Nvidia" in by["ai tech"] and "election" in by["politics"]
    en = dict(ng.GNEWS_EN_QUERIES)
    for label, words in (("trade china en", ("tariff", '"trade deal"', "summit", "APEC")),
                         ("ai tech en", ('"artificial intelligence"', "Nvidia", "OpenAI")),
                         ("politics en", ("election", '"government shutdown"', '"debt ceiling"'))):
        for word in words:
            assert word in en[label], (label, word)
    ar = dict(ng.GNEWS_AR_QUERIES)
    assert "\u0627\u0644\u0630\u0643\u0627\u0621 \u0627\u0644\u0627\u0635\u0637\u0646\u0627\u0639\u064a" in ar["ai tech ar"]   # 'artificial intelligence'
    assert ar["trade china ar"].count(" OR ") == 3 and ar["ai tech ar"].count(" OR ") == 2
    assert all(q.endswith("sourcelang:english") for q in by.values())
    assert all(q.endswith("when:1d") for _, q in ng.GNEWS_AR_QUERIES + ng.GNEWS_EN_QUERIES)
    assert "\u0642\u0645\u0629" in dict(ng.GNEWS_AR_QUERIES)["trade china ar"]            # 'summit' in Arabic
    scope = ng.read_scope(workbook(), FakeSheets.SID, 15)
    gq, nq = ng.build_queries(scope)
    assert [q[0] for q in gq[:8]] == ["MACRO"] * 8 and all(q[0] != "MACRO" for q in gq[8:])     # topics before names
    macro_n = [i for i, q in enumerate(nq) if q[0] == "MACRO"]
    assert macro_n == list(range(13))                                                          # 6 ar + 7 en, then names


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))


# ----------------------------------------------------------------------------- G8 (v1.1.0 [ISSUER-LINK])
# Headlines seen under the wrong name in v_symbol_news, 25 Oct - 16 Nov 2026 (owner reviews of 15 and 16 Nov), and
# headlines that are about the issuer.
ISSUER_CASES = [
    ("Farmers & Merchants Bancorp stock: Q3 profit rises 30.3%", "MBIN.US", "Merchants Bancorp", False),
    ("TransAct Technologies Incorporated (TACT) Reports Q3 Loss, Beats Revenue Estimates", "MBIN.US", "Merchants Bancorp", False),
    ("Regional banks stocks Q3 recap: Benchmarking Columbia Banking System", "MBIN.US", "Merchants Bancorp", False),
    ("Q3 rundown: Axos Financial (NYSE:AX) vs other regional banks stocks", "MBIN.US", "Merchants Bancorp", False),
    ("Q3 earnings recap: Merchants Bancorp (NASDAQ:MBIN) tops regional banks stocks", "MBIN.US", "Merchants Bancorp", True),
    ("Merchants Bancorp stock: Q3 profit rises 92% to USD 105.2 million", "MBIN.US", "Merchants Bancorp", True),
    ("Basic earnings per share hit $2.07 as Q3 profit climbs at Merchants Bancorp (MBIN).", "MBIN.US", "Merchants Bancorp", True),
    ("Inter & Co, Inc. Q3 2026 Earnings Call Summary", "IFS.US", "Intercorp Financial Services Inc.", False),
    ("Inflection Point Q3 2026 Earnings Results and Report", "IFS.US", "Intercorp Financial Services Inc.", False),
    ("Intercorp Financial Services: Soft Q3, But Clear Signs Of An Inflection Point (NYSE:IFS)", "IFS.US", "Intercorp Financial Services Inc.", True),
    ("Intercorp Financial Services Q3FY26 Results: Net income 457m soles, ROE 14%", "IFS.US", "Intercorp Financial Services Inc.", True),
    ("KMB Maintained by BNP Paribas -- Price Target Raised to $112", "BNP.PA", "BNP Paribas SA", False),
    ("BNP Paribas raises price target for DNB to 410 Norwegian kroner (390), reiterates outperform", "BNP.PA", "BNP Paribas SA", False),
    ("Sanofi Shares Rise After Upgrade From BNP Paribas", "BNP.PA", "BNP Paribas SA", False),
    ("BNP Paribas stock enters buyback week as wealth unit targets growth", "BNP.PA", "BNP Paribas SA", True),
    ("BNP Paribas (ENXTPA:BNP) Stock May Be 14% Undervalued On Buyback Launch", "BNP.PA", "BNP Paribas SA", True),
    ("Barclays maintains Coty at Underweight, raises price target to $3 from $2", "BARC.L", "Barclays PLC", False),
    ("Barclays Opens Riyadh Office, Appoints Majed Al Hassoun As CEO Of Saudi Unit", "BARC.L", "Barclays PLC", True),
    ("Tritax Big Box (LSE:BBOX) Goes Ex-Dividend: FTSE 100 Watch", "BBOX.L", "Tritax Big Box Ord", True),
    ("UK dividends calendar - next 7 days", "BBOX.L", "Tritax Big Box Ord", False),
    ("FedEx Corp. stock outperforms competitors on strong trading day", "ZTO.US", "ZTO Express (Cayman) Inc.", False),
    ("ZTO Express Q3 2026 Preview: Macro Headwinds Vs. Micro Execution (NYSE:ZTO)", "ZTO.US", "ZTO Express (Cayman) Inc.", True),
    ("PENN Entertainment, Inc. Reports Third Quarter Results", "GLPI.US", "Gaming and Leisure Properties, Inc.", False),
    ("Gaming and Leisure Properties (NASDAQ:GLPI) Given New $49.00 Price Target at Citigroup", "GLPI.US", "Gaming and Leisure Properties, Inc.", True),
    ("Supernus and Indivior Complete Merger to form 11-product CNS company", "SUPN.US", "Supernus, Inc.", True),
    ("Meridian Bank (MRBK) Lags Q3 Earnings and Revenue Estimates", "CFBK.US", "CF Bankshares Inc.", False),
    ("What DRI Healthcare Trust (DHT.UN) Said on Its Q3 Earnings Call", "DHT.US", "DHT Holdings, Inc.", False),
    ("DHT Holdings: Q3 Earnings Snapshot", "DHT.US", "DHT Holdings, Inc.", True),
    ("STAR BULK CARRIERS ($SBLK) Releases Q1 2026 Earnings, Stock Rises", "SBLK.US", "Star Bulk Carriers Corp.", True),
    ("Al Rajhi Bank (1120) profit rises", "1120.SR", "Al Rajhi Bank", True),
    ("Banco Santander (SAN.MC) cuts capital", "SAN.MC", "Banco Santander, S.A.", True),
    ("Some headline", "GLE.PA", "", None),
    ("Zed Trust (ZED.UN) cuts its distribution", "ZED.US", "Acme Holdings Inc.", False),   # another listing of the ticker
    ("Acme (ZED.US) raises guidance", "ZED.US", "Acme Holdings Inc.", True),
    # Codex P2 (#1006): without analyst-note words the issuer is the subject, not a broker
    ("Barclays raises dividend after earnings beat", "BARC.L", "Barclays PLC", True),
    ("Profit falls at Merchants Bancorp", "MBIN.US", "Merchants Bancorp", True),
    ("Barclays upgraded to Overweight at JPMorgan", "BARC.L", "Barclays PLC", True),
    ("Fresenius Medical downgraded by Barclays", "BARC.L", "Barclays PLC", False),
    ("Barclays downgrades Michelin", "BARC.L", "Barclays PLC", False),
]


@pytest.mark.parametrize("title,symbol,name,want", ISSUER_CASES)
def test_g8_issuer_linked(title, symbol, name, want):
    assert ng.issuer_linked(title, symbol, name) is want


def test_g8_apply_modes_and_scope():
    items = [{"source": "gdelt", "scope": "MBIN.US", "title": "Farmers & Merchants Bancorp stock: Q3 profit rises"},
             {"source": "google_news", "scope": "MBIN.US", "title": "Merchants Bancorp stock: Q3 profit rises 92%"},
             {"source": "edgar", "scope": "MBIN.US", "title": "8-K 2.02 results of operations"},
             {"source": "gdelt", "scope": "MACRO", "title": "Farmers & Merchants Bancorp"},
             {"source": "gdelt", "scope": "XYZ.US", "title": "anything"}]
    names = {"MBIN.US": "Merchants Bancorp"}
    kept, info = ng.apply_issuer_link(items, names, "on")
    assert [i["title"] for i in kept] == [items[1]["title"], items[2]["title"], items[3]["title"], items[4]["title"]]
    assert info == {"mode": "on", "checked": 2, "unlinked": 1,
                    "examples": ["MBIN.US: Farmers & Merchants Bancorp stock: Q3 profit rises"]}
    kept, info = ng.apply_issuer_link(items, names, "observe")
    assert kept == items and info["unlinked"] == 1 and info["mode"] == "observe"
    kept, info = ng.apply_issuer_link(items, names, "off")
    assert kept == items and info == {"mode": "off", "checked": 0, "unlinked": 0, "examples": []}


class _WrongNameHTTP(FakeHTTP):
    def get(self, url, params=None, headers=None, timeout=None):
        if url == ng.GDELT_API and "Primis" in (params or {}).get("query", ""):
            return Resp(200, gdelt_articles([
                ("https://www.marketbeat.com/p1", "Primis Financial shares fall after earnings miss", "20261027T150000Z", "marketbeat.com"),
                ("https://www.example.com/x", "Regional banks stocks Q3 recap: benchmarking First Busey", "20261027T160000Z", "example.com"),
            ]))
        return super().get(url, params=params, headers=headers, timeout=timeout)


def _link_run(monkeypatch, mode):
    monkeypatch.setenv("TFB_NEWS_ISSUER_LINK", mode)
    (rc, rep), _sh = _run(http=_WrongNameHTTP(), write=False)
    assert rc == 0
    return rep


@pytest.mark.parametrize("mode", ["on", "junk", "observe"])
def test_g8_run_drops_a_headline_about_another_company(monkeypatch, armed, mode):
    rep, off = _link_run(monkeypatch, mode), _link_run(monkeypatch, "off")
    li = rep["issuer_link"]
    assert li["mode"] == ("observe" if mode == "observe" else "on")
    assert li["unlinked"] == 1 and li["examples"] == ["FRST.US: Regional banks stocks Q3 recap: benchmarking First Busey"]
    assert li["checked"] >= 2
    dropped = 0 if mode == "observe" else 1
    assert rep["counts"]["collected"] == off["counts"]["collected"] - dropped
    assert rep["gate"]["verdict"] == off["gate"]["verdict"]
    digest = ng.render_digest(rep)
    assert ("not about the issuer 1 of %d (%s)" % (li["checked"], "kept, observe" if mode == "observe" else "dropped")) in digest
    assert off["issuer_link"] == {"mode": "off", "checked": 0, "unlinked": 0, "examples": []}
    assert "not about the issuer" not in ng.render_digest(off)


def test_g8_default_is_on(monkeypatch, armed):
    monkeypatch.delenv("TFB_NEWS_ISSUER_LINK", raising=False)
    (rc, rep), _sh = _run(http=_WrongNameHTTP(), write=False)
    assert rc == 0 and rep["issuer_link"]["mode"] == "on" and rep["issuer_link"]["unlinked"] == 1
