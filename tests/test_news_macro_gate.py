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
  G15 (v1.6.0 [FINNHUB]) market and company news from the Finnhub key the
     project already holds: header-borne key never in a message; publisher
     read from the URL or from 'source'; US names only; a refusal is named;
     without the key the source is skipped and named
  G14 (v1.5.0 [SOURCES]) five more sources: the feed reader (RSS 2.0, RSS 1.0,
     Atom; bytes with a byte-order mark; a reply that is not a feed raises and
     is named under Sources Failed - the Fed feed read 0 items in 134 runs
     without a fault being named); Bing News items filed under their
     publisher; Yahoo per name under the issuer-link rule; a dead feed does
     not stop its group and is asked once; undated and old items left out;
     one story from one publisher stays one member (fold_new_sources); the
     new sources alone confirm a Gulf situation and carry it in the memory;
     with every new feed empty the run equals the six-source run

Run: python tests/test_news_macro_gate.py   or   pytest -q <this file>
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import unquote, urlparse

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts import news_macro_gate as ng  # noqa: E402

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 28, 12, 47, tzinfo=UTC)          # 15:47 Riyadh, FOMC day
QUIET = dt.datetime(2026, 10, 30, 12, 47, tzinfo=UTC)
KEY = "secret-eodhd-key-123"
FKEY = "secret-finnhub-key-456"
MKEY = "secret-marketaux-key-789"
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


EMPTY_FEED = "<rss><channel></channel></rss>"


class FakeHTTP:
    def __init__(self, fail: Optional[Dict[str, int]] = None, econ_us=None, en_rss: str = "",
                 feeds: Optional[Dict[str, Any]] = None):
        self.calls: List[str] = []
        self.fail = fail or {}
        self.econ_us = econ_us if econ_us is not None else ECON_US
        self.en_rss = en_rss
        # v1.5.0 [SOURCES]: replies of the new sources, keyed by a FEEDS id, 'bing' / 'bing:<word in the
        # search>' or 'yahoo:<code>'; text, bytes (served as .content) or a Resp. Unlisted = an empty feed.
        self.feeds = feeds or {}
        self.seen: List[Dict[str, Any]] = []

    def _new_source(self, url, params):
        if url == ng.MARKETAUX_API:                  # v1.7.0: replies keyed 'marketaux:<code>'
            body = self.feeds.get("marketaux:" + params.get("symbols", ""), {"meta": {}, "data": []})
            return body if isinstance(body, Resp) else Resp(200, body)
        if url.startswith(ng.FINNHUB_API):           # v1.6.0: replies keyed 'finnhub:news' / 'finnhub:<TICKER>'
            key = "finnhub:" + (params.get("symbol") or "news")
            body = self.feeds.get(key, [])
            return body if isinstance(body, Resp) else Resp(200, body)
        if url == ng.BING_NEWS_RSS:
            key = next((k for k in self.feeds if k.startswith("bing:") and k[5:] in params.get("q", "")), "bing")
        elif url == ng.YAHOO_RSS:
            key = "yahoo:" + params.get("s", "")
        else:
            key = next((f[0] for f in ng.FEEDS if f[2] == url), None)
        if key is None:
            return None
        body = self.feeds.get(key, EMPTY_FEED)
        if isinstance(body, Resp):
            return body
        if isinstance(body, bytes):
            out = Resp(200, text="(bytes)")
            out.content = body
            return out
        return Resp(200, text=body)

    def get(self, url, params=None, headers=None, timeout=None):
        params = params or {}
        self.calls.append(url)
        self.seen.append({"url": url, "params": dict(params), "headers": dict(headers or {}), "timeout": timeout})
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
        new = self._new_source(url, params)
        if new is not None:
            return new
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
    monkeypatch.setenv("FINNHUB_API_KEY", FKEY)
    monkeypatch.setenv("MARKETAUX_API_KEY", MKEY)
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
    dead = {"sec.gov": 500, "gdeltproject": 500, "news.google": 500, "federalreserve": 500, "eodhd.com": 500,
            "bing.com": 500, "yahoo.com": 500, "finnhub.io": 500, "marketaux.com": 500}
    dead.update({urlparse(f[2]).hostname: 500 for f in ng.FEEDS})       # v1.5.0: the new sources too
    (rc, rep), _ = _run(http=FakeHTTP(fail=dead))
    assert rc == 1 and rep["sources_ok"] == {}
    assert set(rep["sources_failed"]) == set(ng.ALL_SOURCES)


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
    assert (ev_dir / "news_gate.md").read_text().startswith("## News & macro gate v1.8.0")


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
    assert ng.__version__ == "1.8.0"   # 2026-11-26: v1.5.0 [SOURCES] .. v1.8.0 [LLM-SHADOW]
    vd = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("news_macro_gate", "1.8.0", "news macro gate")' in vd
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
# evaluate() without its easing block: the rules that RAISE a verdict, byte-identical from v1.1.0 to now
RAISE_RULES_SHA = "68bed4f220d1dcae14d5f17eb98ce609400e5d43ec580ae0ca151c2f82dc53ce"
EVALUATE_SHA = "99430618bbb085518c3c81665d000dca0462eadef17255abc05e12b5ecdb8a4c"      # evaluate() of v1.4.0 [TRUCE-READ]

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
    # no rule that raises a verdict was touched: evaluate() without its easing block is byte-identical to v1.1.0
    src = (_REPO / "scripts" / "news_macro_gate.py").read_text(encoding="utf-8")
    body = "def evaluate(" + src.split("def evaluate(", 1)[1].split("\n# ---", 1)[0]
    import hashlib
    import re
    block = re.compile(r'        if c\["event_type"\] == "de_escalation".*?(?=        if c\["severity"\] == "H")', re.S)
    easing = block.findall(body)
    assert len(easing) == 1                                       # one easing block, and it is the only part that differs
    assert hashlib.sha256(block.sub("", body).encode("utf-8")).hexdigest() == RAISE_RULES_SHA
    assert hashlib.sha256(body.encode("utf-8")).hexdigest() == EVALUATE_SHA
    assert "trade_deal" not in body
    # the easing block can write the EASING line and nothing else: no verdict step, no driver, no name flag
    for word in ("bump(", "drivers", "names[", "level"):
        assert word not in easing[0], word
    assert "positives.append(" in easing[0] and easing[0].rstrip().endswith("continue")


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


# ----------------------------------------------------------------------------- G12 (v1.4.0 [TRUCE-READ])
# (headline, reading with a taxonomy that has no trade-truce list, reading now, direction now)
FIGHT_CASES = [
    ("Missile strike reported as US-China trade truce holds", "de_escalation", "war_escalation", -1),
    ("Drone attack hits tanker near Hormuz as tariff truce is extended", "de_escalation", "war_escalation", -1),
    ("Airstrike kills 12 as trade truce talks continue", "de_escalation", "war_escalation", -1),
    # a real truce beside the fighting still turns it into a truce story (the older rule, unchanged) ...
    ("Missile fire stops as Gaza truce takes hold", "de_escalation", "de_escalation", 1),
    ("Missile fire stops as Gaza truce takes hold amid US-China trade truce", "de_escalation", "de_escalation", 1),
    # ... unless the truce is reported broken (unchanged)
    ("Missile strike breaks calm as trade truce collapses", "war_escalation", "war_escalation", -1),
    ("Airstrike hits Gaza, violating ceasefire", "war_escalation", "war_escalation", -1),
]
# (headline, direction for the market, direction as a name's own headline)
BREAKING_CASES = [
    ("Trump's Board of Peace Warns Gaza Ceasefire Could Collapse", -1, 1),
    ("Board of Peace delivers bleak report on Gaza, warning of ceasefire collapse without immediate action", -1, 1),
    ("Israeli army launches military offensive in central Gaza despite ceasefire", -1, 1),
    ("Iran attacks US bases in Gulf countries despite diplomatic efforts", -1, 1),
    ("Trump says US could devastate Iran in three days as diplomatic efforts stall", -1, 1),
    ("Oil Prices Rise 5% on Fears of US-Iran Ceasefire Collapse", -1, 1),
    ("Iran defends two-day proposal for peace talks after US rejects conditions", -1, 1),
    ("\u0627\u0633\u062a\u0645\u0631\u0627\u0631 \u062d\u0631\u0627\u0626\u0642 \u0627\u0644\u0646\u0627\u0642\u0644\u0627\u062a \u0641\u064a \u0647\u0631\u0645\u0632 \u0631\u063a\u0645 \u0622\u0645\u0627\u0644 \u0627\u0644\u062a\u0647\u062f\u0626\u0629", -1, 1),   # tanker fires go on in Hormuz despite hopes of calm
    ("Israel and Hamas agree ceasefire deal", 1, 1),
    ("Truce holds in Gaza for a third day", 1, 1),
    ("Peace talks resume in Doha", 1, 1),
    ("Gaza truce holds as US-China trade truce nears", 1, 1),
    # the gate's own archive, 27 Oct - 19 Nov: each of these was recorded as easing
    ("Arab parties endorse Eisenkot for Israeli PM, IDF reports 52 ceasefire violations Oct. 16-Nov. 5", -1, 1),
    ("Crude Oil Prices Finish Higher as Progress Stalls in US-Iran Peace Talks", -1, 1),
    ("Israeli army blows up Palestinian homes in central Gaza amid continued ceasefire violations", -1, 1),
    ("Israel launches assault targeting residential areas in Gaza despite ceasefire", -1, 1),
    # a denied breach is not a breach (archive: the first one); one word may stand between, not two
    ("Russia and Ukraine confirm no ceasefire violations in Ukrainian town of Oleshky", 1, 1),
    ("No ceasefire violations reported in Gaza, monitors say", 1, 1),
    ("Ceasefire holds, no violations reported", 1, 1),
    ("Truce holds without any breach, UN says", 1, 1),
    ("Gaza truce holds with zero reported breaches", 1, 1),
    ("Monitors report no major ceasefire violations", -1, 1),
    ("Ceasefire talks end with no deal after violations", -1, 1),
    # the denial covers that word only: another breach word beside it still counts
    ("Hamas says no breach of truce as Israel resumes attacks", -1, 1),
    ("Ceasefire talks: no deal, no progress", -1, 1),
    ("Peace talks end without progress in Doha", -1, 1),
    ("Gaza ceasefire holds despite sporadic clashes", -1, 1),
]


@pytest.mark.parametrize("title,before,after,direction", FIGHT_CASES)
def test_g12_fighting_beside_a_trade_truce_is_fighting(title, before, after, direction):
    tax = ng.load_taxonomy()
    for scope in ("MACRO", "ZZZ.US"):
        was, now = ng.Classifier(_v120(tax)).classify(title, scope), ng.Classifier(tax).classify(title, scope)
        assert was["event_type"] == before, (scope, was)
        assert (now["event_type"], now["direction"]) == (after, direction), (scope, now)
        if before != after:
            assert (before, after) == ("de_escalation", "war_escalation") and now["severity"] == "H"


@pytest.mark.parametrize("title,market,own", BREAKING_CASES)
def test_g12_a_ceasefire_that_is_breaking_is_read_as_falling(title, market, own):
    clf = ng.Classifier(ng.load_taxonomy())
    got = clf.classify(title, "MACRO")
    assert (got["event_type"], got["direction"]) == ("de_escalation", market), got
    name = clf.classify(title, "ZZZ.US")                      # a name's own headline is left as it was
    assert (name["event_type"], name["direction"]) == ("de_escalation", own), name


@pytest.mark.parametrize("title,event,direction", [
    ("OPEC+ raises output despite price collapse", "oil_supply", 1),
    ("Nvidia beats estimates despite chip curbs", "ai_tech", 1),
    ("Trump and Xi sign trade deal despite tensions", "trade_deal", 1),
    ("G20 summit opens despite boycott", "summit_diplomacy", 0),
])
def test_g12_only_a_ceasefire_reading_is_turned(title, event, direction):
    # 'despite', 'collapse' and the other breach words turn a ceasefire headline and no other reading
    got = ng.Classifier(ng.load_taxonomy()).classify(title, "MACRO")
    assert (got["event_type"], got["direction"]) == (event, direction), got


def test_g12_the_easing_line_and_the_one_reading_that_can_rise():
    tax = ng.load_taxonomy()

    def run(items, t=None):
        return ng.evaluate(_clusters_with(t or tax, items, QUIET), [], {}, QUIET)
    # a ceasefire that is holding stands on the EASING line, exactly as before
    calm = run(_wires("MACRO", "Israel and Hamas agree ceasefire deal in Gaza"))
    assert calm["verdict"] == "GO" and calm["drivers"] == []
    assert calm["positives"] == ["de_escalation/israel grade 1, 3 sources: Israel and Hamas agree ceasefire deal in Gaza"]
    # one reported as breaking does not - and raises nothing
    for title in ("Board of Peace warns Gaza ceasefire could collapse", "Israeli army launches military offensive in central Gaza despite ceasefire"):
        got = run(_wires("MACRO", title))
        assert (got["verdict"], got["drivers"], got["positives"], got["names"]) == ("GO", [], [], {}), title
    # mixed cluster: the holding reports outnumber the breaking one - listed, with a holding headline and their own count
    wires = _wires("MACRO", "Gaza ceasefire holds for a third day", "Board of Peace warns Gaza ceasefire could collapse")
    holding, breaking = wires[:3], wires[3:]
    assert len(holding) == len(breaking) == 3 and len({w["url"] for w in wires}) == 6
    for calm_n, breaking_n, grade in ((3, 1, 1), (3, 2, 1), (2, 1, 2)):   # the grade and the count are the holding reports' own
        mixed = run(holding[:calm_n] + breaking[:breaking_n])
        assert mixed["positives"] == ["de_escalation/gaza grade %d, %d sources: Gaza ceasefire holds for a third day" % (grade, calm_n)], (calm_n, breaking_n)
        assert mixed["verdict"] == "GO" and mixed["drivers"] == []
    # ... the breaking reports outnumber the holding ones, or match them: not listed
    for calm_n, breaking_n in ((1, 2), (3, 3), (2, 2), (1, 1), (0, 3)):
        assert run(holding[:calm_n] + breaking[:breaking_n])["positives"] == [], (calm_n, breaking_n)
    # ... and the holding ones must be confirmed on their own: one unranked site beside a wire's warning is not
    weak = ng.parse_gdelt(gdelt_articles([
        ("https://a.example.org/1", "Gaza ceasefire holds for a third day", "20261030T100000Z", "a.example.org"),
        ("https://b.example.org/2", "Gaza ceasefire holds, residents say", "20261030T100500Z", "b.example.org"),
        ("https://www.reuters.com/3", "Board of Peace warns Gaza ceasefire could collapse", "20261030T101000Z", "reuters.com"),
    ]), "MACRO", "geopolitics")
    assert run(weak)["positives"] == []
    # fighting beside a trade truce from three wires on a Gulf anchor: it raised nothing, now it is the top verdict
    strike = _wires("MACRO", "Missile strike on tanker in Hormuz as US-China trade truce holds")
    was, now = run(strike, _v120(tax)), run(strike)
    assert was["verdict"] == "GO" and was["drivers"] == [] and len(was["positives"]) == 1
    assert now["verdict"] == "NO_NEW_ENTRY" and now["positives"] == []
    assert now["drivers"] == ["macro: war_escalation/hormuz grade 1, 3 sources: Missile strike on tanker in Hormuz as US-China trade truce holds"]
    # the same strike without the truce words reads the same now, with either taxonomy
    plain = _wires("MACRO", "Missile strike on tanker in Hormuz")
    assert run(plain)["verdict"] == run(plain, _v120(tax))["verdict"] == "NO_NEW_ENTRY"
    # a name's own headline about a breaking ceasefire gains no flag
    assert run(_wires("ZZZ.US", "Zeta slides as Gaza ceasefire collapses"))["names"] == {}


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


# ----------------------------------------------------------------------------- G13 (v1.4.1 [FOMC-2027])
def test_g13_the_fed_revised_2027_schedule():
    """The Fed's 12 Nov 2026 revision: no decision on 27 Oct 2027 (a strategy meeting), 8 Dec moved to 17 Nov; 2026
    unchanged. A decision day is a Wednesday at 14:00 New York."""
    days = [dt.date.fromisoformat(d) for d in ng.FOMC_DECISION_DAYS]
    assert "2027-10-27" not in ng.FOMC_DECISION_DAYS and "2027-12-08" not in ng.FOMC_DECISION_DAYS
    assert ng.FOMC_DECISION_DAYS[-2:] == ("2027-09-15", "2027-11-17") and len(ng.FOMC_DECISION_DAYS) == 15
    assert [d for d in days if d.year == 2026] == [dt.date(2026, m, d) for m, d in (
        (1, 28), (3, 18), (4, 29), (6, 17), (7, 29), (9, 16), (10, 28), (12, 9))]
    assert days == sorted(days) and all(d.weekday() == 2 for d in days)
    times = ng.fomc_decision_times()
    assert times[-1] == dt.datetime(2027, 11, 17, 19, 0, tzinfo=dt.timezone.utc)       # 14:00 EST


# ----------------------------------------------------------------------------- G14 (v1.5.0 [SOURCES])
OLD_SIX = ["edgar", "gdelt", "google_news", "fed_rss", "econ", "market"]
HORMUZ_REUTERS = "Iran fires missiles at tanker in Strait of Hormuz"


def _feed(*items, root="rss"):
    """An RSS 2.0 reply: items are (title, link, date text or None)."""
    body = "".join(f"<item><title>{t}</title><link>{u}</link>" + (f"<pubDate>{d}</pubDate>" if d else "") + "</item>"
                   for t, u, d in items)
    return f'<?xml version="1.0" encoding="UTF-8"?><{root} version="2.0"><channel>{body}</channel></{root}>'


def _bing(url):
    return "http://www.bing.com/news/apiclick.aspx?ref=FexRss&amp;aid=&amp;url=" + \
        url.replace(":", "%3a").replace("/", "%2f") + "&amp;c=1&amp;mkt=en-us"


ATOM = """<?xml version="1.0" encoding="utf-8"?><feed xmlns="http://www.w3.org/2005/Atom">
<entry><title>ECB publishes its decision</title><link rel="self" href="https://example.org/self"/>
<link rel="alternate" href="https://www.ecb.europa.eu/press/pr/a1.html"/><updated>2026-10-28T09:20:49Z</updated></entry>
<entry><title>Second entry</title><link href="https://www.ecb.europa.eu/press/pr/a2.html"/>
<published>2026-10-28T12:20:49+03:00</published></entry></feed>"""
RDF = """<?xml version="1.0"?><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
 xmlns="http://purl.org/rss/1.0/" xmlns:dc="http://purl.org/dc/elements/1.1/">
<item><title>Statement on monetary policy</title><link>https://www.ecb.europa.eu/b1.html</link>
<dc:date>2026-10-28T08:00:00+00:00</dc:date></item></rdf:RDF>"""


def test_g14_the_registry_is_whole():
    ids = [f[0] for f in ng.FEEDS]
    assert len(ids) == len(set(ids)) == 17
    assert {f[1] for f in ng.FEEDS} == set(ng.FEED_GROUPS)
    assert [len([f for f in ng.FEEDS if f[1] == g]) for g in ng.FEED_GROUPS] == [7, 6, 4]
    for fid, group, url, domain, lang, hours in ng.FEEDS:
        assert url.startswith("https://") and lang in ("en", "ar") and hours in (24.0, 72.0)
        assert ng.tier_of(domain) != "F", fid                      # a publisher the tiers know
        assert ng.domain_of("https://" + domain + "/x") == domain or domain == "ecb.europa.eu", fid
    assert {ng.tier_of(f[3]) for f in ng.FEEDS if f[1] == "official_rss"} == {"A"}
    assert ng.ALL_SOURCES == tuple(OLD_SIX) + ng.NEW_SOURCES and len(ng.ALL_SOURCES) == 13
    assert set(ng.NEW_SOURCES) <= set(ng.SOURCE_BUDGET_S) and set(ng.FEED_GROUPS) < set(ng.NEW_SOURCES)
    assert set(ng.MEMORY_SOURCES) == {"gdelt", "google_news", "bing_news", "yahoo_rss", "press_rss", "gulf_rss", "finnhub", "marketaux"}
    assert ng.selected_sources("") == list(ng.ALL_SOURCES)
    assert ng.selected_sources(",".join(OLD_SIX)) == OLD_SIX          # the rollback named in the header
    assert sum(ng.SOURCE_BUDGET_S[x] for x in ng.NEW_SOURCES) <= 450   # the job has 20 minutes
    wf = (_REPO / ".github" / "workflows" / "news_macro_gate.yml").read_text(encoding="utf-8")
    assert "blank = all: " + ",".join(ng.ALL_SOURCES) in wf


def test_g14_the_reader_takes_rss_atom_and_rdf():
    rss = _feed(("S&amp;amp;P 500 falls", "https://www.wsj.com/a", "Wed, 28 Oct 2026 09:00:00 GMT"),
                ("BEA release", "https://apps.bea.gov/b", "Wed, 28 Oct 2026 08:30:00 EDT"),
                ("Investing item", "https://www.investing.com/c", "2026-10-28 09:20:49"),
                ("No date", "https://www.wsj.com/d", None),
                ("Bad date", "https://www.wsj.com/e", "yesterday"),
                ("", "https://www.wsj.com/f", "Wed, 28 Oct 2026 09:00:00 GMT"))
    got = ng.parse_feed(rss, "MACRO", "t", "press_rss", "en")
    assert [i["title"] for i in got] == ["S&P 500 falls", "BEA release", "Investing item", "No date", "Bad date"]
    assert got[0]["domain"] == "wsj.com" and got[0]["source"] == "press_rss" and got[0]["query"] == "t"
    assert got[0]["published"] == dt.datetime(2026, 10, 28, 9, 0, tzinfo=UTC)
    assert got[1]["published"] == dt.datetime(2026, 10, 28, 12, 30, tzinfo=UTC)       # EDT
    assert got[2]["published"] == dt.datetime(2026, 10, 28, 9, 20, 49, tzinfo=UTC)    # no zone = UTC
    assert got[3]["published"] is None and got[4]["published"] is None
    assert {i["domain"] for i in ng.parse_feed(rss, "MACRO", "t", "press_rss", "en", "marketwatch.com")} \
        == {"marketwatch.com"}                                     # a publisher's feed names its publisher

    atom = ng.parse_feed(ATOM, "MACRO", "ecb", "official_rss", "en")
    assert [(i["title"], i["url"]) for i in atom] == [
        ("ECB publishes its decision", "https://www.ecb.europa.eu/press/pr/a1.html"),   # rel=alternate, not self
        ("Second entry", "https://www.ecb.europa.eu/press/pr/a2.html")]
    assert atom[0]["published"] == dt.datetime(2026, 10, 28, 9, 20, 49, tzinfo=UTC)
    assert atom[1]["published"] == dt.datetime(2026, 10, 28, 9, 20, 49, tzinfo=UTC)
    rdf = ng.parse_feed(RDF, "MACRO", "ecb", "official_rss", "en")
    assert len(rdf) == 1 and rdf[0]["published"] == dt.datetime(2026, 10, 28, 8, 0, tzinfo=UTC)

    media = ('<rss xmlns:media="http://search.yahoo.com/mrss/"><channel><item><media:title>caption</media:title>'
             '<title>The headline</title><link>https://www.cnbc.com/x</link></item></channel></rss>')
    assert [i["title"] for i in ng.parse_feed(media, "MACRO", "t", "press_rss", "en")] == ["The headline"]
    only_media = media.replace("<title>The headline</title>", "")
    assert ng.parse_feed(only_media, "MACRO", "t", "press_rss", "en") == []
    foreign = '<rss xmlns:x="urn:other"><channel><x:item><title>not a feed item</title></x:item></channel></rss>'
    assert ng.parse_feed(foreign, "MACRO", "t", "press_rss", "en") == []
    # only whole character references are undone: 'AT&T', 'M&A' and a bare '&not' are left as written
    amp = _feed(("AT&amp;T and M&amp;A &amp;notes &amp;#8211; S&amp;amp;P &amp;quot;rally&amp;quot;", "https://www.wsj.com/a", None))
    assert ng.parse_feed(amp, "MACRO", "t", "s", "en")[0]["title"] == 'AT&T and M&A &notes \u2013 S&P "rally"'
    # the Google-News shape reads exactly as parse_rss() always read it
    assert ng.parse_feed(GNEWS_RSS, "MACRO", "ar", "google_news", "ar") == ng.parse_rss(GNEWS_RSS, "MACRO", "ar", "google_news", "ar")
    assert ng.parse_feed(EMPTY_FEED, "MACRO", "t", "s", "en") == []          # an empty feed is a feed


def test_g14_a_byte_order_mark_no_longer_empties_a_feed():
    """requests decodes a text/xml reply without a charset as ISO-8859-1, so a UTF-8 byte-order mark reached
    the XML parser as three stray characters and parse_rss() returned [] - a silent zero."""
    raw = b"\xef\xbb\xbf" + FED_RSS.encode("utf-8")
    as_latin1 = raw.decode("iso-8859-1")
    import xml.etree.ElementTree as ET
    with pytest.raises(ET.ParseError):
        ET.fromstring(as_latin1)                                   # what v1.4.1 handed the parser
    for payload in (raw, bytearray(raw), as_latin1, chr(0xFEFF) + FED_RSS, "\n \n" + FED_RSS, b"\r\n" + FED_RSS.encode()):
        assert len(ng.parse_feed(payload, "MACRO", "fed press", "fed_rss", "en")) == 2
    ar = GNEWS_RSS.encode("utf-8")                                 # bytes keep the feed's own encoding
    assert ng.parse_feed(ar, "MACRO", "ar", "google_news", "ar")[0]["title"] == AR["aramco_profit_up"]
    assert len(ng.parse_rss(as_latin1, "MACRO", "fed press", "fed_rss", "en")) == 2


@pytest.mark.parametrize("payload, why", [
    ("<html><head><title>Access Denied</title></head><body>blocked</body></html>", "not a feed (root <html>)"),
    ("<!DOCTYPE html><html><body>Just a moment...<br></body></html>", "not a feed ("),
    ("<not xml", "not a feed ("),
    ("", "empty reply"),
    (b"", "empty reply"),
    (None, "empty reply"),
    ('<?xml version="1.0"?><!DOCTYPE rss [<!ENTITY a "aaaa">]><rss><channel><item><title>&a;</title></item>'
     '</channel></rss>', "declares XML entities"),
    ('{"error": "rate limited"}', "not a feed ("),
])
def test_g14_a_reply_that_is_not_a_feed_raises(payload, why):
    with pytest.raises(ValueError) as err:
        ng.parse_feed(payload, "MACRO", "t", "press_rss", "en")
    assert why in str(err.value)
    assert all(ord(c) < 128 for c in str(err.value))
    assert ng.parse_rss(payload, "MACRO", "t", "press_rss", "en") == []       # the old contract


def test_g14_an_oversized_reply_is_not_parsed(monkeypatch):
    monkeypatch.setattr(ng, "MAX_FEED_BYTES", 200)
    with pytest.raises(ValueError) as err:
        ng.parse_feed(_feed(*[("t%d" % n, "https://www.wsj.com/%d" % n, None) for n in range(20)]), "MACRO", "t", "s", "en")
    assert "over the limit" in str(err.value)


def test_g14_bing_target_and_yahoo_symbol():
    pub = "https://www.reuters.com/world/middle-east/x-2026-11-26/"
    click = _bing(pub).replace("&amp;", "&")
    assert ng.bing_target(click) == pub and ng.domain_of(ng.bing_target(click)) == "reuters.com"
    assert ng.bing_target("https://www.reuters.com/a?url=https://evil.example/x") == "https://www.reuters.com/a?url=https://evil.example/x"
    assert ng.bing_target("https://notbing.com/apiclick.aspx?url=https%3a%2f%2fx.com%2f1") == "https://notbing.com/apiclick.aspx?url=https%3a%2f%2fx.com%2f1"
    no_url = "http://www.bing.com/news/apiclick.aspx?ref=FexRss&c=1"
    assert ng.bing_target(no_url) == no_url and ng.tier_of(ng.domain_of(no_url)) == "D"
    assert ng.bing_target("http://www.bing.com/news/apiclick.aspx?url=javascript%3aalert(1)") \
        == "http://www.bing.com/news/apiclick.aspx?url=javascript%3aalert(1)"
    assert ng.bing_target("") == ""
    assert [ng.yahoo_symbol(x) for x in ("supn.us", "BRK.B.US", "5023.SR", "VOD.L", ".US", "", "MACRO")] \
        == ["SUPN", "BRK-B", "5023.SR", "", "", "", ""]


def test_g14_within_window_leaves_out_undated_and_old():
    def it(hours):
        return {"title": "t", "published": None if hours is None else NOW - dt.timedelta(hours=hours)}
    rows = [it(0), it(23.9), it(24.1), it(100), it(None), it(-0.9), it(-1.1)]
    kept, left = ng.within_window(rows, NOW, 24.0)
    assert kept == [rows[0], rows[1], rows[5]] and left == {"outside": 3, "undated": 1}
    kept72, left72 = ng.within_window(rows, NOW, 72.0)
    assert len(kept72) == 4 and left72 == {"outside": 2, "undated": 1}
    # the reason: evaluate() reads a headline with no date as published now
    old = {"source": "press_rss", "scope": "MACRO", "title": HORMUZ_REUTERS, "url": "https://www.wsj.com/o",
           "domain": "wsj.com", "published": None}
    clf = ng.Classifier(ng.load_taxonomy())
    undated = ng.classify_all([dict(old, url="https://www.wsj.com/o%d" % n, domain=d) for n, d in
                               enumerate(("wsj.com", "reuters.com", "apnews.com"))], clf, QUIET)
    assert ng.evaluate(ng.cluster_items(undated), [], {}, QUIET)["verdict"] == "NO_NEW_ENTRY"


def _it(source, title, domain, scope="MACRO", url=None, memory=False, iid=None):
    row = {"source": source, "scope": scope, "title": title, "domain": domain,
           "url": url or f"https://{domain}/{source}/{abs(hash(title)) % 997}", "published": NOW}
    if memory:
        row["memory"] = True
    if iid:
        row["id"] = iid
    return row


def test_g14_one_story_from_one_publisher_is_one_member():
    g = _it("google_news", HORMUZ_REUTERS, "reuters.com")
    b = _it("bing_news", HORMUZ_REUTERS + "!", "reuters.com")              # punctuation and case do not matter
    p = _it("press_rss", HORMUZ_REUTERS.upper(), "reuters.com")
    kept, left = ng.fold_new_sources([g, b, p])
    assert kept == [g] and left == {"bing_news": 1, "press_rss": 1}
    kept, left = ng.fold_new_sources([b, p, g])                            # whatever the order of collection
    assert kept == [g] and left == {"bing_news": 1, "press_rss": 1}
    kept, left = ng.fold_new_sources([b, p])                               # two new sources: the first keeps it
    assert kept == [b] and left == {"press_rss": 1}
    # another publisher, another scope, another headline: all members
    others = [_it("bing_news", HORMUZ_REUTERS, "apnews.com"), _it("bing_news", HORMUZ_REUTERS, "reuters.com", scope="FRST.US"),
              _it("press_rss", "Oil jumps after tanker attack", "reuters.com")]
    kept, left = ng.fold_new_sources([g] + others)
    assert kept == [g] + others and left == {}
    # the six older sources are never left out, also when they repeat each other
    gd = _it("gdelt", HORMUZ_REUTERS, "reuters.com")
    kept, left = ng.fold_new_sources([gd, g, dict(g)])
    assert len(kept) == 3 and left == {}
    # a row recorded by an earlier run keeps the story against a fresh copy from another new source ...
    mem = _it("bing_news", HORMUZ_REUTERS, "reuters.com", memory=True, iid="aaaa")
    kept, left = ng.fold_new_sources([p, mem])
    assert kept == [mem] and left == {"press_rss": 1}
    # ... the same item met again is not a copy (classify_all keeps the fresh one on the same ID) ...
    again = _it("bing_news", HORMUZ_REUTERS, "reuters.com", iid="aaaa")
    kept, left = ng.fold_new_sources([again, mem])
    assert kept == [again, mem] and left == {}
    # ... and an older source that now carries the story takes it over from the recorded row
    kept, left = ng.fold_new_sources([g, mem])
    assert kept == [g] and left == {"bing_news": 1}
    # nothing to compare without a publisher or a headline
    blank = [_it("bing_news", HORMUZ_REUTERS, ""), _it("press_rss", HORMUZ_REUTERS, ""), _it("bing_news", " ?! ", "x.com"),
             _it("press_rss", " ?! ", "x.com")]
    assert ng.fold_new_sources(blank) == (blank, {})


def test_g14_source_ages():
    rows = [{"source": "bing_news", "published": NOW - dt.timedelta(minutes=m)} for m in (5, 65, 30)]
    rows += [{"source": "press_rss", "published": NOW - dt.timedelta(minutes=m)} for m in (10, 20)]
    rows += [{"source": "press_rss", "published": NOW + dt.timedelta(minutes=30)},       # ahead of the run: age 0
             {"source": "bing_news", "published": NOW - dt.timedelta(minutes=1), "memory": True},
             {"source": "edgar", "published": NOW - dt.timedelta(days=9)}, {"source": "gulf_rss", "published": None}]
    assert ng.source_ages(rows, NOW) == {"bing_news": {"n": 3, "newest_min": 5, "median_min": 30},
                                         "press_rss": {"n": 3, "newest_min": 0, "median_min": 10}}


def test_g14_bing_searches():
    scope = ng.read_scope(workbook(), FakeSheets.SID, 15)
    bq = ng.build_bing_queries(scope)
    macro = [q for q in bq if q[0] == "MACRO"]
    assert [q[1] for q in macro] == ["geopolitics bing", "oil bing", "rates bing", "markets bing", "trade china bing",
                                     "ai tech bing", "politics bing"]
    assert all("when:" not in q[2] for q in bq) and macro[0][2] == '"Strait of Hormuz" OR Iran OR Houthi OR missile OR ceasefire'
    assert [q[2] for q in macro] == [q.rsplit(" when:", 1)[0] for _l, q in ng.GNEWS_EN_QUERIES]
    assert {q[0] for q in bq if q[0] != "MACRO"} == {"FRST.US", "5023.SR", "BADCO.US"}


def _hours_ago(h):
    return (NOW - dt.timedelta(hours=h)).strftime("%a, %d %b %Y %H:%M:%S GMT")


BING_GEO = _feed((HORMUZ_REUTERS, _bing("https://www.reuters.com/world/a1"), _hours_ago(2)),
                 ("Old strike report", _bing("https://www.reuters.com/world/old"), _hours_ago(30)),
                 ("Undated strike report", _bing("https://www.reuters.com/world/nodate"), None),
                 ("Unreadable link story about a missile", "http://www.bing.com/news/apiclick.aspx?ref=FexRss", _hours_ago(1)))


def test_g14_bing_news_files_each_item_under_its_publisher(armed):
    http = FakeHTTP(feeds={"bing:Hormuz": BING_GEO,
                           "bing:Primis": _feed(("Primis Financial shares fall after earnings miss",
                                                 _bing("https://www.benzinga.com/p1"), _hours_ago(40)))})
    col = ng.Collector(http, _sleep, NOW)
    got = col.bing_news(ng.build_bing_queries(ng.read_scope(workbook(), FakeSheets.SID, 15)))
    assert [(i["scope"], i["domain"], i["url"]) for i in got] == [
        ("MACRO", "reuters.com", "https://www.reuters.com/world/a1"),
        ("MACRO", "bing.com", "http://www.bing.com/news/apiclick.aspx?ref=FexRss"),
        ("FRST.US", "benzinga.com", "https://www.benzinga.com/p1")]          # a name keeps 72 h
    assert col.ok == {"bing_news": 3} and col.failed == {}
    assert col.detail["bing_news"] == {"asked": 10, "read": 5, "outside": 1, "undated": 1}
    asked = [c for c in http.seen if c["url"] == ng.BING_NEWS_RSS]
    assert len(asked) == 10 and all(c["params"]["format"] == "rss" and c["params"]["qft"] == 'sortbydate="1"'
                                    and c["timeout"] == ng.FEED_TIMEOUT_S for c in asked)


def test_g14_a_dead_search_source_is_asked_once_per_search_and_stopped(armed):
    http = FakeHTTP(fail={"bing.com": 503})
    col = ng.Collector(http, _sleep, NOW)
    assert col.bing_news(ng.build_bing_queries([])) == []
    assert len(http.calls) == ng.BREAKER_FAILURES                  # no retry, and the breaker opens after two
    assert col.ok == {} and col.failed["bing_news"].startswith("geopolitics bing: HTTP 503; oil bing: HTTP 503")
    assert "circuit open after 2 consecutive failures; 5 queries skipped" in col.failed["bing_news"]
    block = FakeHTTP(feeds={"bing": "<html><body>Unusual traffic</body></html>"})
    col = ng.Collector(block, _sleep, NOW)
    assert col.bing_news(ng.build_bing_queries([])) == [] and len(block.calls) == 2
    assert "geopolitics bing: not a feed (root <html>)" in col.failed["bing_news"] and col.ok == {}


def test_g14_yahoo_is_asked_per_listed_name_and_judged_by_the_issuer_rule(armed, monkeypatch):
    monkeypatch.setenv("TFB_NEWS_ISSUER_LINK", "on")
    feeds = {"yahoo:FRST": _feed(("Primis Financial (NASDAQ:FRST) shares fall after earnings miss",
                                  "https://finance.yahoo.com/news/frst-1.html", _hours_ago(5)),
                                 ("5 bank stocks to watch as rates fall", "https://finance.yahoo.com/news/list.html", _hours_ago(3)),
                                 ("Primis Financial old story", "https://finance.yahoo.com/news/old.html", _hours_ago(80)))}
    http = FakeHTTP(feeds=feeds)
    scope = ng.read_scope(workbook(), FakeSheets.SID, 15) + [{"symbol": "VOD.L", "name": "Vodafone", "role": "env"}]
    (rc, rep) = ng.run(http=http, sheets=None, sid="", now=NOW, write=False, sources=["yahoo_rss"], scope=scope,
                       sleep=_sleep, run_id="r1", mode="observe")
    asked = [c["params"] for c in http.seen if c["url"] == ng.YAHOO_RSS]
    assert [p["s"] for p in asked] == ["FRST", "5023.SR", "BADCO"]            # VOD.L is not asked
    assert all(p["region"] == "US" and p["lang"] == "en-US" for p in asked)
    assert rc == 0 and rep["sources_ok"] == {"yahoo_rss": 2} and rep["sources_failed"] == {}
    assert rep["sources_detail"]["yahoo_rss"] == {"asked": 3, "read": 3, "outside": 1, "undated": 0}
    assert rep["issuer_link"]["checked"] == 2 and rep["issuer_link"]["unlinked"] == 1     # the list article is dropped
    assert rep["counts"]["collected"] == 1
    # yahoo.com is tier D: on its own it is grade 3 and raises no flag
    assert rep["clusters"][0]["tier"] == "D" and rep["clusters"][0]["grade"] == 3 and rep["gate"]["names"] == {}


def test_g14_one_dead_feed_does_not_stop_its_group(armed):
    feeds = {"marketwatch": _feed(("Stocks slide as oil jumps", "https://www.marketwatch.com/s1", _hours_ago(1))),
             "cnbc": Resp(403, text="Forbidden"),
             "investing": "<html><body>Just a moment...</body></html>",
             "nyt_business": _feed(("Fed officials signal patience", "https://www.nytimes.com/n1", _hours_ago(2)),
                                   ("Yesterday's column", "https://www.nytimes.com/n0", _hours_ago(26))),
             "seekingalpha": b"\xef\xbb\xbf" + _feed(("Wall Street opens lower", "https://seekingalpha.com/news/1",
                                                     _hours_ago(0.2))).encode("utf-8")}
    http = FakeHTTP(feeds=feeds)
    col = ng.Collector(http, _sleep, NOW)
    got = col.feeds("press_rss")
    assert [(i["domain"], i["query"]) for i in got] == [("marketwatch.com", "marketwatch"), ("nytimes.com", "nyt_business"),
                                                        ("seekingalpha.com", "seekingalpha")]
    assert all(i["source"] == "press_rss" and i["scope"] == "MACRO" for i in got)
    assert col.ok == {"press_rss": 3}
    assert col.failed == {"press_rss": "cnbc: HTTP 403; investing: not a feed (root <html>)"}
    assert col.detail["press_rss"] == {"asked": 5, "read": 4, "outside": 1, "undated": 0, "failed": 2}
    press = [f for f in ng.FEEDS if f[1] == "press_rss"]
    assert http.calls == [f[2] for f in press]                     # every feed asked once, in order: no retry
    assert {c["timeout"] for c in http.seen} == {ng.FEED_TIMEOUT_S}
    assert all("User-Agent" not in c["headers"] for c in http.seen)


def test_g14_every_feed_of_a_group_failing_is_named_feed_by_feed(armed):
    http = FakeHTTP(fail={urlparse(f[2]).hostname: -1 for f in ng.FEEDS if f[1] == "gulf_rss"})
    col = ng.Collector(http, _sleep, NOW)
    assert col.feeds("gulf_rss") == [] and col.ok == {}
    note = col.failed["gulf_rss"]
    assert [p.split(":")[0] for p in note.split("; ")] == ["asharq_business", "arabnews", "thenational", "aljazeera"]
    assert len(http.calls) == 4 and len(note) <= 420


def test_g14_sec_is_asked_with_the_declared_contact_or_not_at_all(armed):
    http = FakeHTTP()
    col = ng.Collector(http, _sleep, NOW)
    col.feeds("official_rss", "TFB research test@example.com")
    sec = [c for c in http.seen if "sec.gov" in c["url"]]
    assert len(sec) == 1 and sec[0]["headers"]["User-Agent"] == "TFB research test@example.com"
    assert all("User-Agent" not in c["headers"] for c in http.seen if "sec.gov" not in c["url"])
    assert col.ok == {"official_rss": 0} and col.failed == {}
    http2 = FakeHTTP()
    col2 = ng.Collector(http2, _sleep, NOW)
    col2.feeds("official_rss", "")
    assert not [u for u in http2.calls if "sec.gov" in u] and len(http2.calls) == 5
    assert col2.failed == {"official_rss": "sec_press: TFB_SEC_USER_AGENT not set"}


def test_g14_a_group_keeps_to_its_time_budget(armed):
    t = [0.0]

    class SlowHTTP(FakeHTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            t[0] += 30.0
            return super().get(url, params=params, headers=headers, timeout=timeout)

    http = SlowHTTP()
    col = ng.Collector(http, _sleep, NOW, clock=lambda: t[0])
    col.feeds("press_rss")
    assert len(http.calls) == 3                                    # starts at 0, 30, 60 s of 75
    assert col.failed["press_rss"] == "time budget 75s spent: 4 feeds not asked"

    class SlowAndDead(SlowHTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            super().get(url, params=params, headers=headers, timeout=timeout)
            return Resp(403, text="Forbidden")

    t[0] = 0.0
    col = ng.Collector(SlowAndDead(), _sleep, NOW, clock=lambda: t[0])
    col.feeds("press_rss")
    assert col.failed["press_rss"] == ("marketwatch: HTTP 403; wsj_markets: HTTP 403; cnbc: HTTP 403; "
                                       "time budget 75s spent: 4 feeds not asked")
    assert col.detail["press_rss"] == {"failed": 3}


def test_g14_the_fed_feed_is_read_from_bytes_and_a_block_page_is_named(armed):
    """27 Oct - 26 Nov: fed_rss=0 in all 134 runs that answered, 12 - 15 Nov included, and no fault named."""
    fresh = FED_RSS.replace("Tue, 27 Oct 2026 14:00:00 GMT", _hours_ago(20))

    class Bytes(FakeHTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            out = super().get(url, params=params, headers=headers, timeout=timeout)
            if url == ng.FED_RSS:
                out.content = b"\xef\xbb\xbf" + fresh.encode("utf-8")
                out.text = out.content.decode("iso-8859-1")          # what requests gives without a charset
            return out

    col = ng.Collector(Bytes(), _sleep, NOW)
    got = col.fed_rss()
    assert [i["title"] for i in got] == ["Agencies issue joint statement on resolution plans"]
    assert got[0]["domain"] == "federalreserve.gov" and col.ok == {"fed_rss": 1} and col.failed == {}

    class Blocked(FakeHTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            out = super().get(url, params=params, headers=headers, timeout=timeout)
            if url in (ng.FED_RSS, ng.GNEWS_RSS):
                out.text = "<html><head><title>Access Denied</title></head><body></body></html>"
            return out

    http = Blocked()
    (rc, rep), _ = _run(http=http, write=False, sources=["fed_rss", "google_news", "market"])
    assert rc == 0 and rep["sources_ok"] == {"market": 2}
    assert rep["sources_failed"]["fed_rss"] == "not a feed (root <html>)"
    assert rep["sources_failed"]["google_news"] == "saudi market ar: not a feed (root <html>)"
    # the main source is not cut short by a reply it could not read: every search is still asked (as for GDELT)
    assert len([u for u in http.calls if u == ng.GNEWS_RSS]) == 16 == len(ng.build_queries(rep["scope"])[1])

    class HalfBlocked(FakeHTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            out = super().get(url, params=params, headers=headers, timeout=timeout)
            if url == ng.GNEWS_RSS and (params or {}).get("hl") == "ar":
                out.text = "<html><body>Before you continue</body></html>"
            return out

    (rc, half), _ = _run(http=HalfBlocked(en_rss=GNEWS_EN_RSS), write=False, sources=["google_news", "market"])
    assert half["sources_ok"]["google_news"] == 3 and "not a feed" in half["sources_failed"]["google_news"]
    assert half["gate"]["verdict"] == "NO_NEW_ENTRY"               # the English wires still confirm the situation


GULF_FEEDS = {
    "bing:Hormuz": _feed((HORMUZ_REUTERS, _bing("https://www.reuters.com/world/a1"), _hours_ago(3))),
    "wsj_markets": _feed(("Missile strike sets tanker ablaze near Hormuz", "https://www.wsj.com/w1", _hours_ago(2))),
    "arabnews": _feed(("Tanker hit by missile in Strait of Hormuz", "https://www.arabnews.com/n1", _hours_ago(1))),
}
NEW_ONLY = ["bing_news", "press_rss", "gulf_rss", "market"]


def _macro(rep):
    return [d for d in rep["gate"]["drivers"] if d.startswith("macro")]


def test_g14_the_new_sources_alone_confirm_a_gulf_situation_and_carry_it(armed):
    """The archive shows the gate on one source (Google News). With Google and GDELT away, two licensed wires
    and a Gulf paper reached through Bing and the publishers' feeds are the three domains of the rule."""
    (rc, rep), sh = _run(http=FakeHTTP(feeds=GULF_FEEDS), sources=NEW_ONLY)
    assert rc == 0 and rep["gate"]["verdict"] == "NO_NEW_ENTRY"
    assert any(d.startswith("macro: war_escalation/hormuz grade 1, 3 sources") for d in rep["gate"]["drivers"])
    rows = sh.tabs[ng.ITEMS_TAB][1:]
    assert sorted((r[8], r[9], r[10]) for r in rows) == [("bing_news", "reuters.com", "B"), ("gulf_rss", "arabnews.com", "C"),
                                                         ("press_rss", "wsj.com", "B")]
    assert [r[13] for r in rows if r[8] == "bing_news"] == ["https://www.reuters.com/world/a1"]
    gate = sh.tabs[ng.GATE_TAB][1]
    assert gate[8] == "bing_news=1, gulf_rss=1, market=2, press_rss=1" and gate[9] == "" and gate[11] == ng.__version__
    # two hours later the feeds have moved on: the recorded rows still hold the situation ...
    later = NOW + dt.timedelta(hours=2)
    (rc2, rep2), _ = _run(http=FakeHTTP(), sheets=sh, now=later, sources=NEW_ONLY, write=False)
    assert rep2["gate"]["verdict"] == "NO_NEW_ENTRY" and rep2["counts"]["carried"] == 3
    # ... but not without their sources in the run, and not after the 24 h of the rule
    assert _macro(rep2) == _macro(rep) != []
    (_rc, rep3), _ = _run(http=FakeHTTP(), sheets=sh, now=later, sources=["market"], write=False)
    assert rep3["gate"]["verdict"] == "CAUTION" and rep3["counts"]["carried"] == 0 and _macro(rep3) == []   # FOMC day
    (_rc, rep4), _ = _run(http=FakeHTTP(), sheets=sh, now=NOW + dt.timedelta(hours=26), sources=NEW_ONLY, write=False)
    assert rep4["gate"]["verdict"] == "GO" and rep4["counts"]["carried"] == 0
    # two of the three are not enough, as for the older sources
    two = {k: v for k, v in GULF_FEEDS.items() if k != "arabnews"}
    (_rc, rep5), _ = _run(http=FakeHTTP(feeds=two), sources=NEW_ONLY, write=False)
    assert rep5["gate"]["verdict"] == "CAUTION" and _macro(rep5) == []


def test_g14_a_story_google_already_carries_is_not_counted_twice(armed):
    same = {"bing:Hormuz": _feed((HORMUZ_REUTERS, _bing("https://www.reuters.com/world/a1"), _hours_ago(3)),
                                 ("Tanker hit by missile in Strait of Hormuz", _bing("https://www.bbc.com/news/b3"), _hours_ago(3)),
                                 ("Hormuz missile strike: what we know", _bing("https://www.ft.com/f1"), _hours_ago(2)))}
    base_http = FakeHTTP(fail={"gdeltproject": 429}, en_rss=GNEWS_EN_RSS)
    (_rc, base), _ = _run(http=base_http, write=False, sources=OLD_SIX)
    (rc, rep), _ = _run(http=FakeHTTP(fail={"gdeltproject": 429}, en_rss=GNEWS_EN_RSS, feeds=same), write=False)

    def hormuz(r):
        return next(c for c in r["clusters"] if c["scope"] == "MACRO" and c["event_type"] == "war_escalation"
                    and c["anchor"] == "hormuz")
    assert len(hormuz(rep)["members"]) == len(hormuz(base)["members"]) + 1     # only the FT story is new
    assert hormuz(rep)["domains"] == sorted(set(hormuz(base)["domains"]) | {"ft.com"})
    assert rep["sources_detail"]["bing_news"]["repeated"] == 2 and rep["sources_ok"]["bing_news"] == 3
    assert rep["counts"]["collected"] == base["counts"]["collected"] + 1
    assert rep["gate"]["verdict"] == base["gate"]["verdict"] == "NO_NEW_ENTRY"


def test_g14_with_every_new_feed_empty_the_run_equals_the_six_source_run(armed):
    def strip(rep):
        return {"gate": rep["gate"], "counts": rep["counts"], "issuer_link": rep["issuer_link"],
                "clusters": [(c["id"], c["grade"], c["tier"], c["domains"], len(c["members"]), c["headline"])
                             for c in rep["clusters"]]}
    (rc_a, a), sh_a = _run(sources=OLD_SIX)
    (rc_b, b), sh_b = _run()
    assert rc_a == rc_b == 0 and strip(a) == strip(b)
    assert sh_a.tabs[ng.ITEMS_TAB] == sh_b.tabs[ng.ITEMS_TAB] and sh_a.tabs[ng.ECON_TAB] == sh_b.tabs[ng.ECON_TAB]
    ga, gb = sh_a.tabs[ng.GATE_TAB][1], sh_b.tabs[ng.GATE_TAB][1]
    assert ga[:8] == gb[:8] and ga[9:] == gb[9:]                  # only the Sources OK cell differs
    assert set(gb[8].split(", ")) == set(ga[8].split(", ")) | {"bing_news=0", "gulf_rss=0", "official_rss=0",
                                                            "press_rss=0", "yahoo_rss=0", "finnhub=0", "marketaux=0"}
    assert b["sources_detail"]["bing_news"] == {"asked": 10, "read": 0, "outside": 0, "undated": 0}
    assert "sources_detail" in a and a["sources_detail"] == {} and set(a["source_ages"]) <= set(OLD_SIX)
    assert a["sources_failed"] == b["sources_failed"]


def test_g14_the_digest_and_the_evidence_carry_one_line_per_source(armed, monkeypatch, tmp_path):
    feeds = dict(GULF_FEEDS, cnbc=Resp(403, text="Forbidden"))
    (rc, rep), _ = _run(http=FakeHTTP(feeds=feeds), sources=NEW_ONLY, write=False)
    assert rep["source_ages"] == {"bing_news": {"n": 1, "newest_min": 180, "median_min": 180},
                                  "gulf_rss": {"n": 1, "newest_min": 60, "median_min": 60},
                                  "press_rss": {"n": 1, "newest_min": 120, "median_min": 120}}
    assert rep["sources_detail"]["press_rss"] == {"asked": 6, "read": 1, "outside": 0, "undated": 0, "failed": 1}
    digest = ng.render_digest(rep)
    assert "| Source | Dated items kept | Newest (min old) | Median (min old) |" in digest
    assert "| bing_news | 1 | 180 | 180 | 0 / 0 / 0 | 0 |" in digest
    assert "| press_rss | 1 | 120 | 120 | 0 / 0 / 0 | 1 |" in digest
    assert "press_rss: cnbc: HTTP 403" in digest
    ng.write_evidence(rep, str(tmp_path))
    data = json.loads((tmp_path / "news_gate.json").read_text())
    assert data["sources_detail"]["bing_news"]["asked"] == 10 and data["source_ages"]["gulf_rss"]["newest_min"] == 60
    row = ng.gate_row(dict(rep, sources_failed={"press_rss": "x" * 420, "gulf_rss": "y" * 420, "bing_news": "z" * 420}))
    assert len(row[9]) == 1000                                     # the failed-sources cell holds 1000 characters


# ----------------------------------------------------------------------------- G15 (v1.6.0 [FINNHUB])
def _fh(headline, url, hours_ago, source="Reuters", related=""):
    return {"category": "company", "datetime": int((NOW - dt.timedelta(hours=hours_ago)).timestamp()),
            "headline": headline, "id": abs(hash(headline)) % 10 ** 9, "image": "", "related": related,
            "source": source, "summary": "", "url": url}


def test_g15_parse_finnhub():
    rows = [_fh("Iran fires missiles at tanker in Strait of Hormuz", "https://www.reuters.com/world/x", 2),
            _fh("Primis Financial cuts outlook", "https://finnhub.io/api/news?id=abc", 5, source="SeekingAlpha"),
            _fh("Unknown wire item", "https://finnhub.io/api/news?id=def", 5, source="SomeBlog"),
            _fh("", "https://www.reuters.com/empty", 1),
            dict(_fh("No time", "https://www.cnbc.com/t", 1), datetime=0), "junk"]
    got = ng.parse_finnhub(rows, "MACRO", "market news finnhub")
    assert [(i["domain"], i["source"]) for i in got] == [("reuters.com", "finnhub"), ("seekingalpha.com", "finnhub"),
                                                         ("finnhub.io", "finnhub"), ("cnbc.com", "finnhub")]
    assert got[0]["published"] == NOW - dt.timedelta(hours=2) and got[3]["published"] is None
    assert ng.tier_of("finnhub.io") == "D" and ng.parse_finnhub([], "MACRO", "x") == []
    for bad, why in (({"error": "Invalid API key."}, "Invalid API key"), ("text", "not a news list"), (None, "not a news list")):
        with pytest.raises(ValueError) as err:
            ng.parse_finnhub(bad, "MACRO", "x")
        assert why in str(err.value)
    assert [ng.finnhub_symbol(x) for x in ("supn.us", "BRK-B.US", "5023.SR", "VOD.L", ".US", "")] == \
        ["SUPN", "BRK.B", "", "", "", ""]


def test_g15_the_key_travels_in_a_header_and_never_in_a_message(armed):
    http = FakeHTTP(feeds={"finnhub:news": [_fh("Iran fires missiles at tanker in Strait of Hormuz",
                                                  "https://www.reuters.com/world/x", 2),
                                              _fh("Old story", "https://www.reuters.com/old", 30)],
                           "finnhub:FRST": [_fh("Primis Financial (NASDAQ:FRST) shares fall after earnings miss",
                                                "https://www.benzinga.com/p1", 40, source="Benzinga")]})
    scope = ng.read_scope(workbook(), FakeSheets.SID, 15)
    col = ng.Collector(http, _sleep, NOW)
    got = col.finnhub([s["symbol"] for s in scope], FKEY)
    asked = [c for c in http.seen if c["url"].startswith(ng.FINNHUB_API)]
    assert [c["url"].rsplit("/", 1)[1] for c in asked] == ["news", "company-news", "company-news"]   # 5023.SR not asked
    assert [c["params"].get("symbol") for c in asked[1:]] == ["FRST", "BADCO"]
    assert asked[1]["params"]["from"] == "2026-10-25" and asked[1]["params"]["to"] == "2026-10-28"
    assert all(c["headers"] == {"X-Finnhub-Token": FKEY} and FKEY not in str(c["params"]) and
               c["timeout"] == ng.FEED_TIMEOUT_S for c in asked)
    assert [(i["scope"], i["domain"]) for i in got] == [("MACRO", "reuters.com"), ("FRST.US", "benzinga.com")]
    assert col.ok == {"finnhub": 2} and col.failed == {}
    assert col.detail["finnhub"] == {"asked": 3, "read": 3, "outside": 1, "undated": 0}

    class Leaky(FakeHTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            if url.startswith(ng.FINNHUB_API):
                raise ConnectionError("refused for token " + (headers or {}).get("X-Finnhub-Token", ""))
            return super().get(url, params=params, headers=headers, timeout=timeout)

    col = ng.Collector(Leaky(), _sleep, NOW)
    assert col.finnhub(["FRST.US", "BADCO.US", "ZZZ.US"], FKEY) == []
    note = col.failed["finnhub"]
    assert FKEY not in note and "***" in note and "circuit open after 2 consecutive failures" in note


def test_g15_a_refusal_is_named_and_no_key_skips_the_source(armed, monkeypatch):
    http = FakeHTTP(feeds={"finnhub:news": Resp(200, {"error": "You don't have access to this resource."}),
                           "finnhub:FRST": Resp(429, text="limit")})
    col = ng.Collector(http, _sleep, NOW)
    assert col.finnhub(["FRST.US"], FKEY) == []
    assert col.failed["finnhub"].startswith("market news finnhub: not a news list: You don't have access")
    assert "name FRST.US finnhub: HTTP 429" in col.failed["finnhub"]
    assert len([u for u in http.calls if u.startswith(ng.FINNHUB_API)]) == 2      # asked once each: no retry
    monkeypatch.delenv("FINNHUB_API_KEY")
    http2 = FakeHTTP()
    (rc, rep), _ = _run(http=http2, write=False, sources=["finnhub", "market"])
    assert rc == 0 and rep["sources_failed"] == {"finnhub": "FINNHUB_API_KEY not set"}
    assert not [u for u in http2.calls if u.startswith(ng.FINNHUB_API)]


def test_g15_finnhub_joins_the_gate_like_the_other_new_sources(armed):
    """A Reuters story carried by Google and by Finnhub is one member; a name item is judged by the issuer rule."""
    feeds = {"finnhub:news": [_fh("Iran fires missiles at tanker in Strait of Hormuz", "https://www.reuters.com/x", 3),
                              _fh("Missile strike on tanker near Hormuz lifts oil", "https://www.ft.com/f1", 2)],
             "finnhub:FRST": [_fh("5 bank stocks to watch as rates fall", "https://www.zacks.com/l", 3, source="Zacks")]}
    (rc, rep), sh = _run(http=FakeHTTP(fail={"gdeltproject": 429}, en_rss=GNEWS_EN_RSS, feeds=feeds), write=True)
    assert rc == 0 and rep["sources_ok"]["finnhub"] == 3
    assert rep["sources_detail"]["finnhub"]["repeated"] == 1                      # the Reuters copy Google carries
    assert rep["issuer_link"]["unlinked"] >= 1                                     # the list article is not about FRST
    rows = [r for r in sh.tabs[ng.ITEMS_TAB][1:] if r[8] == "finnhub"]
    assert [(r[9], r[10]) for r in rows] == [("ft.com", "B")]
    assert rep["gate"]["verdict"] == "NO_NEW_ENTRY"
    wf = (_REPO / ".github" / "workflows" / "news_macro_gate.yml").read_text(encoding="utf-8")
    assert "FINNHUB_API_KEY: ${{ secrets.FINNHUB_API_KEY }}" in wf


# ----------------------------------------------------------------------------- G16 (v1.7.0 [MARKETAUX])
def _mx(*rows):
    return {"meta": {"found": len(rows), "returned": len(rows), "limit": 3, "page": 1},
            "data": [{"uuid": "u%d" % n, "title": t, "url": u, "source": src, "language": "en",
                      "published_at": (NOW - dt.timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M:%S.000000Z"),
                      "entities": [{"symbol": "FRST", "sentiment_score": -0.6}]} for n, (t, u, src, h) in enumerate(rows)]}


def test_g16_parse_marketaux():
    got = ng.parse_marketaux(_mx(("Primis Financial shares fall after earnings miss", "https://www.benzinga.com/a", "benzinga.com", 5),
                                 ("No link story", "", "reuters.com", 1), ("", "https://x.com/e", "x.com", 1)), "FRST.US", "lbl")
    assert [(i["domain"], i["source"], i["scope"]) for i in got] == [("benzinga.com", "marketaux", "FRST.US"),
                                                                    ("reuters.com", "marketaux", "FRST.US")]
    assert got[0]["published"] == NOW - dt.timedelta(hours=5)
    assert ng.parse_marketaux({"meta": {}, "data": []}, "X.US", "l") == []
    for bad, why in (({"error": {"code": "usage_limit_reached", "message": "m"}}, "refused: usage_limit_reached"),
                     ({"meta": {}}, "not a news reply"), ([], "not a news reply"), (None, "not a news reply")):
        with pytest.raises(ValueError) as err:
            ng.parse_marketaux(bad, "X.US", "l")
        assert why in str(err.value)


def test_g16_bounded_requests_and_the_key_never_in_a_message(armed):
    http = FakeHTTP(feeds={"marketaux:FRST": _mx(("Primis Financial (NASDAQ:FRST) shares fall after earnings miss",
                                                  "https://www.benzinga.com/a", "benzinga.com", 5),
                                                 ("Primis Financial old item", "https://www.benzinga.com/o", "benzinga.com", 90))})
    col = ng.Collector(http, _sleep, NOW)
    syms = ["FRST.US", "5023.SR", "VOD.L", "BADCO.US"] + ["N%d.US" % n for n in range(20)]
    got = col.marketaux(syms, MKEY, 3)
    asked = [c for c in http.seen if c["url"] == ng.MARKETAUX_API]
    assert [c["params"]["symbols"] for c in asked] == ["FRST", "5023.SR", "BADCO"]      # the first three it lists
    assert all(c["params"]["api_token"] == MKEY and c["params"]["filter_entities"] == "true"
               and c["params"]["published_after"] == "2026-10-25T12:47:00" and c["timeout"] == ng.FEED_TIMEOUT_S for c in asked)
    assert [i["url"] for i in got] == ["https://www.benzinga.com/a"] and col.ok == {"marketaux": 1}
    assert col.detail["marketaux"] == {"asked": 3, "read": 2, "outside": 1, "undated": 0}
    assert ng.Collector(FakeHTTP(), _sleep, NOW).marketaux(syms, MKEY, 0) == []

    class Leaky(FakeHTTP):
        def get(self, url, params=None, headers=None, timeout=None):
            if url == ng.MARKETAUX_API:
                raise ConnectionError("refused api_token=%s" % (params or {}).get("api_token"))
            return super().get(url, params=params, headers=headers, timeout=timeout)

    leaky = Leaky()
    col = ng.Collector(leaky, _sleep, NOW)
    assert col.marketaux(syms, MKEY, 8) == [] and len(leaky.calls) == 0     # raised before the call is recorded
    assert col.detail["marketaux"] == {"failed": 2}                           # asked once each: no retry
    assert MKEY not in col.failed["marketaux"] and "***" in col.failed["marketaux"]
    assert "circuit open after 2 consecutive failures; 6 queries skipped" in col.failed["marketaux"]
    limit = FakeHTTP(feeds={"marketaux:FRST": Resp(402, text="limit"), "marketaux:BADCO": Resp(402, text="limit")})
    col = ng.Collector(limit, _sleep, NOW)
    col.marketaux(["FRST.US", "BADCO.US", "ZZZ.US"], MKEY, 8)
    assert col.failed["marketaux"].startswith("FRST.US: HTTP 402; BADCO.US: HTTP 402") and len(limit.calls) == 2
    busy = FakeHTTP(feeds={"marketaux:FRST": Resp(429, text="slow down")})
    ng.Collector(busy, _sleep, NOW).marketaux(["FRST.US"], MKEY, 8)
    assert len(busy.calls) == 1                                               # a 429 is not retried within the run


def test_g16_marketaux_in_the_run_and_without_a_key(armed, monkeypatch):
    feeds = {"marketaux:FRST": _mx(("Primis Financial (NASDAQ:FRST) shares fall after earnings miss", "https://www.wsj.com/a", "wsj.com", 5),
                                   ("5 bank stocks to watch as rates fall", "https://www.zacks.com/l", "zacks.com", 3))}
    (rc, rep), sh = _run(http=FakeHTTP(feeds=feeds), sources=["marketaux", "market"])
    assert rc == 0 and rep["sources_ok"]["marketaux"] == 2 and rep["issuer_link"]["unlinked"] == 1
    rows = [r for r in sh.tabs[ng.ITEMS_TAB][1:] if r[8] == "marketaux"]
    assert [(r[3], r[9], r[10]) for r in rows] == [("FRST.US", "wsj.com", "B")]
    assert rep["gate"]["names"]["FRST.US"][0]["level"] == "WATCH"          # a confirmed negative item, as for any source
    monkeypatch.setenv("TFB_NEWS_GATE_MARKETAUX_MAX", "1")
    http = FakeHTTP()
    _run(http=http, sources=["marketaux"], write=False)
    assert len([u for u in http.calls if u == ng.MARKETAUX_API]) == 1
    monkeypatch.delenv("MARKETAUX_API_KEY")
    http = FakeHTTP()
    (rc, rep), _ = _run(http=http, sources=["marketaux", "market"], write=False)
    assert rep["sources_failed"] == {"marketaux": "MARKETAUX_API_KEY not set"} and ng.MARKETAUX_API not in http.calls
    wf = (_REPO / ".github" / "workflows" / "news_macro_gate.yml").read_text(encoding="utf-8")
    assert "MARKETAUX_API_KEY: ${{ secrets.MARKETAUX_API_KEY }}" in wf
