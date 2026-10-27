#!/usr/bin/env python3
# scripts/news_macro_gate.py
"""
================================================================================
TFB News & Macro Gate - v1.0.0 (observe-only; program register A7 / news layer)
================================================================================
WHY (owner request 28 Oct 2026, 00:01-00:15 Riyadh): every GO/NO-GO since the
first ticket (26 Oct) has needed a hand-made macro and news check (war news,
energy prices, the Fed, each name's own news), and the existing news archive
(news_archive_collector: Google-News RSS + a lexicon score for a rotating 60
names) never reaches the decision. The FRST post-mortem showed the cost: a
22% EPS miss filed with the SEC two sessions before the entry was not seen
by any rule. The owner asked for the check to run by itself, to grade each
source before believing it, to analyse the economic and financial meaning,
and to measure afterwards whether each call and each source was right -
using free sources (decision 28 Oct 00:11).

WHAT: one run =
  1. SCOPE - holdings (My_Portfolio) + the latest _Shadow_Rank_Log day
     (champion, challenger and board seats), plus TFB_NEWS_GATE_SYMBOLS.
  2. COLLECT (free or already paid; each source optional and fail-open):
       edgar      SEC EDGAR submissions per US symbol (8-K / 6-K items; A)
       gdelt      GDELT DOC 2.0 article lists: macro queries + scope names
       google_news Google-News RSS, Arabic macro queries + Saudi names
       fed_rss    Federal Reserve press releases (A)
       econ       EODHD economic events, US + SA (estimate / actual)
       market     EODHD real-time S&P 500 and VIX
  3. CLASSIFY every headline with the bilingual taxonomy
     (data/news_gate_taxonomy.json: 23 event types, rumour and denial
     markers, positive / negative words, anchors) and GRADE it: source tier
     A (official / issuer wire) .. F (unknown) from SOURCE_TIERS, and an
     Admiralty-style information grade 1 (confirmed) .. 6 (social only) per
     cluster of copies of one story.
  4. GATE - a deterministic verdict GO / CAUTION / NO_NEW_ENTRY with the
     drivers named (EVALUATE below), plus per-name flags BLOCK / WATCH /
     INFO (8-K items, negative confirmed news, an earnings release).
  5. RECORD (with --write): _News_Gate (one row per run), _News_Items
     (new classified headlines, deduplicated by Item ID, first-seen and
     published times kept so lead time and confirmation can be scored),
     _Econ_Events (US / SA releases upserted with estimate, actual and the
     surprise - the forecast-accuracy record the owner asked for). Evidence
     JSON + a markdown digest go to TFB_NEWS_GATE_EVIDENCE_DIR and the job
     summary.
OBSERVE-ONLY: nothing in the live system reads these tabs yet; the verdict
informs the owner's GO/NO-GO by hand until its rules are scored (a rule
earns enforcement only on measured value, owner decision).

GATE: TFB_NEWS_GATE = observe | on (armed) ; anything else = off (default):
no network call, exit 0. Without --write the run is a dry run: it collects
and prints the verdict, and writes nothing.
ENV: DEFAULT_SPREADSHEET_ID, GOOGLE_SHEETS_CREDENTIALS (service account JSON
or base64), TFB_SEC_USER_AGENT (EDGAR contact; EDGAR is skipped without it),
EODHD_API_KEY (econ + market; skipped without it), TFB_NEWS_GATE_SOURCES
(comma list, default all), TFB_NEWS_GATE_SYMBOLS ("SYM[:Name];..."),
TFB_NEWS_GATE_MAX_NAMES (default 15), TFB_NEWS_GATE_MAX_ITEMS (default 200),
TFB_NEWS_GATE_GDELT_SPACING_S (default 6), TFB_NEWS_GATE_EVIDENCE_DIR.
Exit: 0 verdict computed (and written with --write); 1 no source answered or
a write failed; 2 configuration error.

Not a licensed financial adviser; the owner decides and executes every trade.
================================================================================
"""
from __future__ import annotations

import argparse
import base64
import datetime as _dt
import hashlib
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote, quote_plus, urlparse

__version__ = "1.0.0"

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TAXONOMY_PATH = os.path.join(_REPO, "data", "news_gate_taxonomy.json")

SHEETS_API = "https://sheets.googleapis.com/v4/spreadsheets"
SCOPES = ("https://www.googleapis.com/auth/spreadsheets",)
GDELT_API = "https://api.gdeltproject.org/api/v2/doc/doc"
GNEWS_RSS = "https://news.google.com/rss/search"
FED_RSS = "https://www.federalreserve.gov/feeds/press_all.xml"
EDGAR_TICKERS = "https://www.sec.gov/files/company_tickers.json"
EDGAR_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
EODHD_ECON = "https://eodhd.com/api/economic-events"
EODHD_RT = "https://eodhd.com/api/real-time/{code}"

ALL_SOURCES = ("edgar", "gdelt", "google_news", "fed_rss", "econ", "market")

GATE_TAB = "_News_Gate"
ITEMS_TAB = "_News_Items"
ECON_TAB = "_Econ_Events"
GATE_HEADER = ["Run At (Riyadh)", "Run ID", "Verdict", "Drivers", "Scheduled Events",
               "Market", "Names Flagged", "Items (new/classified/collected)",
               "Sources OK", "Sources Failed", "Mode", "Version"]
ITEMS_HEADER = ["Item ID", "First Seen (Riyadh)", "Published (UTC)", "Scope", "Event Type",
                "Direction", "Severity", "Rumour/Denial", "Source", "Domain", "Tier",
                "Cluster Grade", "Headline", "URL", "Cluster", "Run ID"]
ECON_HEADER = ["Key", "Date (UTC)", "Country", "Event", "Comparison", "Period", "Importance",
               "Estimate", "Actual", "Previous", "Surprise", "Surprise %", "Updated At (Riyadh)"]

VERDICTS = ("GO", "CAUTION", "NO_NEW_ENTRY")

# Source tiers: A official / regulator / exchange / issuer wire; B licensed
# wire or specialist financial press; C mainstream; D aggregator; E social;
# F unknown (default). Matched on the registrable domain or a parent suffix.
SOURCE_TIERS: Dict[str, str] = {
    # A
    "sec.gov": "A", "federalreserve.gov": "A", "saudiexchange.sa": "A",
    "tadawul.com.sa": "A", "cma.org.sa": "A", "sama.gov.sa": "A", "spa.gov.sa": "A",
    "opec.org": "A", "eia.gov": "A", "bls.gov": "A", "bea.gov": "A",
    "treasury.gov": "A", "ecb.europa.eu": "A", "imf.org": "A", "whitehouse.gov": "A",
    "globenewswire.com": "A", "prnewswire.com": "A", "businesswire.com": "A",
    "accessnewswire.com": "A", "newsfilecorp.com": "A",
    # B
    "reuters.com": "B", "bloomberg.com": "B", "wsj.com": "B", "ft.com": "B",
    "apnews.com": "B", "argaam.com": "B", "asharqbusiness.com": "B", "aleqt.com": "B",
    "barrons.com": "B", "economist.com": "B", "nikkei.com": "B", "benzinga.com": "B",
    "dowjones.com": "B", "spglobal.com": "B", "argusmedia.com": "B",
    # C
    "cnbc.com": "C", "marketwatch.com": "C", "bbc.com": "C", "bbc.co.uk": "C",
    "nytimes.com": "C", "washingtonpost.com": "C", "theguardian.com": "C",
    "aljazeera.net": "C", "aljazeera.com": "C", "alarabiya.net": "C", "arabnews.com": "C",
    "saudigazette.com.sa": "C", "zawya.com": "C", "gulfnews.com": "C",
    "thenationalnews.com": "C", "mubasher.info": "C", "investing.com": "C",
    "foxbusiness.com": "C", "cnn.com": "C", "axios.com": "C", "politico.com": "C",
    "okaz.com.sa": "C", "alriyadh.com": "C", "al-jazirah.com": "C", "sabq.org": "C",
    "aawsat.com": "C", "skynewsarabia.com": "C", "independent.co.uk": "C",
    "fortune.com": "C", "forbes.com": "C", "businessinsider.com": "C", "cnbcarabia.com": "C",
    # D
    "yahoo.com": "D", "msn.com": "D", "seekingalpha.com": "D", "fool.com": "D",
    "zacks.com": "D", "tipranks.com": "D", "marketbeat.com": "D", "news.google.com": "D",
    "insidermonkey.com": "D", "simplywall.st": "D", "stocktitan.net": "D",
    "gurufocus.com": "D", "finviz.com": "D", "investorsobserver.com": "D",
    # E
    "x.com": "E", "twitter.com": "E", "reddit.com": "E", "stocktwits.com": "E",
    "t.me": "E", "bsky.app": "E", "facebook.com": "E", "linkedin.com": "E",
}
_TIER_RANK = {"A": 0, "B": 1, "C": 2, "D": 3, "E": 4, "F": 5}
_TWO_LEVEL_SUFFIXES = ("com.sa", "gov.sa", "org.sa", "net.sa", "edu.sa", "co.uk",
                       "org.uk", "com.au", "co.jp", "com.hk", "com.eg", "co.za")

# Anchors whose escalation can move this book (Gulf / oil / Fed / China).
MARKET_ANCHORS = frozenset([
    "hormuz", "iran", "israel", "lebanon", "yemen", "houthi", "saudi", "aramco", "opec",
    "fed", "china", "taiwan", "tariff",
    "\u0647\u0631\u0645\u0632", "\u0625\u064a\u0631\u0627\u0646",
    "\u0625\u0633\u0631\u0627\u0626\u064a\u0644", "\u0644\u0628\u0646\u0627\u0646",
    "\u0627\u0644\u064a\u0645\u0646", "\u0627\u0644\u062d\u0648\u062b\u064a",
    "\u0627\u0644\u0635\u064a\u0646", "\u0623\u0648\u0628\u0643",
    "\u0627\u0644\u0641\u064a\u062f\u0631\u0627\u0644\u064a",
    "\u0627\u0644\u0633\u0639\u0648\u062f\u064a\u0629", "\u0623\u0631\u0627\u0645\u0643\u0648",
])
GULF_ANCHORS = frozenset([
    "hormuz", "iran", "yemen", "houthi", "saudi", "aramco", "opec",
    "\u0647\u0631\u0645\u0632", "\u0625\u064a\u0631\u0627\u0646", "\u0627\u0644\u064a\u0645\u0646",
    "\u0627\u0644\u062d\u0648\u062b\u064a", "\u0627\u0644\u0633\u0639\u0648\u062f\u064a\u0629",
    "\u0623\u0631\u0627\u0645\u0643\u0648", "\u0623\u0648\u0628\u0643",
])

# FOMC decision days (second day of each meeting; 14:00 America/New_York).
# Source: federalreserve.gov calendar as summarised by fedratecalc.com, read
# 28 Oct 2026 (2027 is the Fed's tentative schedule).
FOMC_DECISION_DAYS = (
    "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17", "2026-07-29",
    "2026-09-16", "2026-10-28", "2026-12-09",
    "2027-01-27", "2027-03-17", "2027-04-28", "2027-06-09", "2027-07-28",
    "2027-09-15", "2027-10-27", "2027-12-08",
)
HIGH_ECON_RE = re.compile(
    r"(nonfarm|non farm|payrolls|\bcpi\b|consumer price|inflation rate|interest rate decision|"
    r"fed funds|fomc|gdp growth|gdp annualized|\bpce\b|retail sales|unemployment rate|"
    r"ism manufacturing)", re.I)

# SEC 8-K items: (event_type, direction, severity, label)
EIGHT_K_ITEMS: Dict[str, Tuple[str, int, str, str]] = {
    "1.01": ("contract", 1, "L", "material agreement"),
    "1.02": ("contract", -1, "M", "agreement terminated"),
    "1.03": ("bankruptcy", -1, "H", "bankruptcy or receivership"),
    "1.05": ("legal_regulatory", -1, "M", "cybersecurity incident"),
    "2.01": ("mna", 0, "M", "acquisition or disposition completed"),
    "2.02": ("earnings", 0, "M", "results of operations"),
    "2.03": ("offering", 0, "L", "new debt obligation"),
    "2.04": ("credit_event", -1, "H", "debt acceleration trigger"),
    "2.05": ("restructuring", -1, "M", "exit or restructuring costs"),
    "2.06": ("impairment", -1, "M", "material impairment"),
    "3.01": ("delisting", -1, "H", "delisting or listing-rule notice"),
    "3.02": ("offering", -1, "L", "unregistered equity sale"),
    "4.01": ("auditor_change", -1, "M", "auditor change"),
    "4.02": ("restatement", -1, "H", "non-reliance on prior financials"),
    "5.01": ("mna", 0, "M", "change in control"),
    "5.02": ("management_change", 0, "L", "officer or director change"),
    "5.07": ("shareholder_vote", 0, "L", "shareholder vote"),
    "7.01": ("disclosure", 0, "L", "Regulation FD disclosure"),
    "8.01": ("disclosure", 0, "L", "other events"),
}
BLOCK_8K = frozenset(["1.03", "2.04", "3.01", "4.02"])
WATCH_8K = frozenset(["1.02", "1.05", "2.05", "2.06", "4.01"])
_SEV_RANK = {"H": 0, "M": 1, "L": 2}


# ----------------------------------------------------------------------------- #
# small helpers
# ----------------------------------------------------------------------------- #
def _out(msg: str) -> None:
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def gate_mode() -> str:
    v = _env("TFB_NEWS_GATE").lower()
    return v if v in ("observe", "on") else "off"


def _int_env(name: str, default: int, lo: int, hi: int) -> int:
    try:
        v = int(_env(name, str(default)))
    except ValueError:
        v = default
    return max(lo, min(hi, v))


def _float_env(name: str, default: float, lo: float, hi: float) -> float:
    try:
        v = float(_env(name, str(default)))
    except ValueError:
        v = default
    return max(lo, min(hi, v))


def selected_sources(spec: str) -> List[str]:
    if not spec.strip():
        return list(ALL_SOURCES)
    want = [s.strip().lower() for s in spec.split(",") if s.strip()]
    return [s for s in ALL_SOURCES if s in want]


def item_id(url: str, title: str, source: str, scope: str = "") -> str:
    """One row per article AND scope: an article returned by a macro query
    and by a company query keeps both associations (Codex P2, #859)."""
    basis = (url or "").strip() or (source + "|" + (title or "").strip().lower())
    return hashlib.sha1((scope + "|" + basis).encode("utf-8")).hexdigest()[:16]


def domain_of(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        host = ""
    if host.startswith("www."):
        host = host[4:]
    if not host:
        return ""
    parts = host.split(".")
    if len(parts) >= 3 and ".".join(parts[-2:]) in _TWO_LEVEL_SUFFIXES:
        return ".".join(parts[-3:])
    if host.endswith("news.google.com"):
        return "news.google.com"
    return ".".join(parts[-2:]) if len(parts) >= 2 else host


def tier_of(domain: str) -> str:
    d = (domain or "").lower()
    while d:
        if d in SOURCE_TIERS:
            return SOURCE_TIERS[d]
        if "." not in d:
            break
        d = d.split(".", 1)[1]
    return "F"


def best_tier(tiers: Iterable[str]) -> str:
    ts = [t for t in tiers if t in _TIER_RANK]
    return min(ts, key=lambda t: _TIER_RANK[t]) if ts else "F"


def fmt_riyadh(ts: _dt.datetime) -> str:
    return ts.astimezone(RIYADH).strftime("%Y-%m-%d %H:%M:%S")


def fmt_utc(ts: Optional[_dt.datetime]) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%d %H:%M") if ts else ""


# ----------------------------------------------------------------------------- #
# taxonomy + classification
# ----------------------------------------------------------------------------- #
_PUNCT = re.compile(r"[\s\"'`.,;:!?()\[\]{}<>|/\\*#@%^&=+~\u060c\u061b\u061f\u00ab\u00bb\u201c\u201d\u2018\u2019-]+")


def load_taxonomy(path: Optional[str] = None) -> Dict[str, Any]:
    with open(path or TAXONOMY_PATH, "r", encoding="utf-8") as fh:
        tax = json.load(fh)
    if not isinstance(tax, dict) or not tax.get("events"):
        raise ValueError("taxonomy has no events")
    return tax


def normalize_text(text: str) -> str:
    t = _PUNCT.sub(" ", (text or "").lower())
    return " " + " ".join(t.split()) + " "


def _norm_terms(terms: Iterable[str]) -> List[str]:
    """Short ASCII words ("cpi", "fed", "eps") match whole words only; a
    trailing space in the taxonomy marks a word end (Arabic words keep their
    attached prefixes, e.g. al- or wa-, so only the end is bounded)."""
    out = []
    for term in terms or []:
        n = normalize_text(term).strip()
        if not n:
            continue
        if all(ord(ch) < 128 for ch in n) and len(n) <= 4:
            out.append(" " + n + " ")
        elif term.endswith(" "):
            out.append(n + " ")
        else:
            out.append(n)
    return out


def _hit(norm: str, terms: Sequence[str]) -> bool:
    return any(t in norm for t in terms)


def _count(norm: str, terms: Sequence[str]) -> int:
    return sum(1 for t in terms if t in norm)


class Classifier:
    """Bilingual keyword classifier over data/news_gate_taxonomy.json."""

    def __init__(self, tax: Dict[str, Any]):
        self.events: List[Dict[str, Any]] = []
        for ev in tax.get("events") or []:
            self.events.append({
                "type": ev["type"], "scope": ev["scope"], "direction": int(ev["direction"]),
                "severity": ev["severity"],
                "terms": _norm_terms(list(ev.get("en") or []) + list(ev.get("ar") or [])),
            })
        def pair(key: str) -> List[str]:
            block = tax.get(key) or {}
            return _norm_terms(list(block.get("en") or []) + list(block.get("ar") or []))
        self.by_type = {e["type"]: e for e in self.events}
        self.breach = pair("breach")
        self.pos = pair("positive")
        self.neg = pair("negative")
        self.rumor = pair("rumor")
        self.denial = pair("denial")
        self.anchors = [(a, normalize_text(a).strip()) for a in tax.get("anchors") or []]
        # Arabic anchors map to the English name, so one story told in both
        # languages lands in one cluster (and one source count).
        self.canon = dict(tax.get("anchor_canon") or {})

    def anchor(self, norm: str) -> str:
        for raw, n in self.anchors:
            if n and (" " + n) in norm:
                return self.canon.get(raw, raw)
        return ""

    def classify(self, title: str, scope: str) -> Dict[str, Any]:
        norm = normalize_text(title)
        company_first = scope != "MACRO"
        order = ([e for e in self.events if e["scope"] == "COMPANY"]
                 + [e for e in self.events if e["scope"] == "MACRO"]) if company_first \
            else [e for e in self.events if e["scope"] == "MACRO"]
        found = None
        for ev in order:
            if _hit(norm, ev["terms"]):
                found = ev
                break
        # A truce or de-escalation story that also names an attack word is
        # de-escalation unless it says the truce was broken (Codex P2, #859).
        deesc = self.by_type.get("de_escalation")
        if found is not None and found["type"] == "war_escalation" and deesc is not None \
                and _hit(norm, deesc["terms"]) and not _hit(norm, self.breach):
            found = deesc
        pos, neg = _count(norm, self.pos), _count(norm, self.neg)
        if found is None:
            return {"event_type": "", "direction": 0, "severity": "",
                    "rumor": _hit(norm, self.rumor), "denial": _hit(norm, self.denial),
                    "anchor": self.anchor(norm)}
        direction = found["direction"]
        if direction == 0:
            direction = 1 if pos > neg else (-1 if neg > pos else 0)
        elif found["type"] in ("earnings", "guidance", "rating_change", "oil_supply", "central_bank"):
            direction = 1 if pos > neg else (-1 if neg > pos else direction)
        return {"event_type": found["type"], "direction": direction,
                "severity": found["severity"], "rumor": _hit(norm, self.rumor),
                "denial": _hit(norm, self.denial), "anchor": self.anchor(norm)}


# ----------------------------------------------------------------------------- #
# source parsers (pure functions; tests feed recorded payloads)
# ----------------------------------------------------------------------------- #
def _parse_gdelt_time(s: str) -> Optional[_dt.datetime]:
    try:
        return _dt.datetime.strptime(s, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except (TypeError, ValueError):
        return None


def parse_gdelt(payload: Any, scope: str, label: str) -> List[Dict[str, Any]]:
    arts = (payload or {}).get("articles") if isinstance(payload, dict) else None
    out = []
    for a in arts or []:
        url = str(a.get("url") or "")
        title = " ".join(str(a.get("title") or "").split())
        if not title:
            continue
        dom = (str(a.get("domain") or "") or domain_of(url)).lower()
        if dom.startswith("www."):
            dom = dom[4:]
        out.append({"source": "gdelt", "query": label, "scope": scope, "title": title,
                    "url": url, "domain": dom, "published": _parse_gdelt_time(a.get("seendate")),
                    "lang": str(a.get("language") or "").lower()[:2] or "en"})
    return out


def _rfc822(s: str) -> Optional[_dt.datetime]:
    try:
        d = parsedate_to_datetime(s)
        return d.astimezone(UTC) if d.tzinfo else d.replace(tzinfo=UTC)
    except (TypeError, ValueError, IndexError):
        return None


def parse_rss(xml_text: str, scope: str, label: str, source: str, lang: str) -> List[Dict[str, Any]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    out = []
    for it in root.iter("item"):
        title = " ".join((it.findtext("title") or "").split())
        link = (it.findtext("link") or "").strip()
        if not title:
            continue
        src = it.find("source")
        src_url = (src.get("url") if src is not None else "") or ""
        src_name = (src.text or "").strip() if src is not None and src.text else ""
        if src_name and title.endswith(" - " + src_name):
            title = title[: -(len(src_name) + 3)].strip()
        dom = domain_of(src_url) if src_url else domain_of(link)
        out.append({"source": source, "query": label, "scope": scope, "title": title,
                    "url": link, "domain": dom, "published": _rfc822(it.findtext("pubDate") or ""),
                    "lang": lang})
    return out


def parse_ticker_map(payload: Any) -> Dict[str, Tuple[int, str]]:
    out: Dict[str, Tuple[int, str]] = {}
    rows = payload.values() if isinstance(payload, dict) else (payload or [])
    for row in rows:
        try:
            out[str(row["ticker"]).upper()] = (int(row["cik_str"]), str(row.get("title") or ""))
        except (KeyError, TypeError, ValueError):
            continue
    return out


def _edgar_time(row_date: str, accept: str) -> Optional[_dt.datetime]:
    for s, fmt in ((accept, "%Y-%m-%dT%H:%M:%S.%fZ"), (accept, "%Y-%m-%dT%H:%M:%SZ"),
                   (row_date, "%Y-%m-%d")):
        try:
            return _dt.datetime.strptime(s, fmt).replace(tzinfo=UTC)
        except (TypeError, ValueError):
            continue
    return None


def parse_edgar_submissions(payload: Any, symbol: str, cik: int,
                            since: _dt.datetime) -> List[Dict[str, Any]]:
    rec = (((payload or {}).get("filings") or {}).get("recent") or {}) if isinstance(payload, dict) else {}
    forms = rec.get("form") or []
    out = []
    for i, form in enumerate(forms):
        if form not in ("8-K", "8-K/A", "6-K", "6-K/A"):
            continue
        def col(name: str) -> str:
            vals = rec.get(name) or []
            return str(vals[i]) if i < len(vals) and vals[i] is not None else ""
        when = _edgar_time(col("filingDate"), col("acceptanceDateTime"))
        if when is None or when < since:
            continue
        items = [x.strip() for x in col("items").split(",") if x.strip()]
        acc = col("accessionNumber")
        doc = col("primaryDocument")
        url = (f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc.replace('-', '')}/{doc}"
               if acc and doc else "")
        labels = [f"{x} {EIGHT_K_ITEMS[x][3]}" for x in items if x in EIGHT_K_ITEMS]
        title = f"{form} " + ("; ".join(labels) if labels else (", ".join(items) or "filing"))
        out.append({"source": "edgar", "query": "submissions", "scope": symbol, "title": title,
                    "url": url, "domain": "sec.gov", "published": when, "lang": "en",
                    "form": form, "items": items})
    return out


def parse_econ_events(payload: Any) -> List[Dict[str, Any]]:
    out = []
    for r in payload if isinstance(payload, list) else []:
        try:
            when = _dt.datetime.strptime(str(r.get("date")), "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        except (TypeError, ValueError):
            continue
        typ = str(r.get("type") or "").strip()
        if not typ:
            continue
        est, act, prev = r.get("estimate"), r.get("actual"), r.get("previous")
        surprise = surprise_pct = None
        if isinstance(est, (int, float)) and isinstance(act, (int, float)):
            surprise = round(act - est, 6)
            if est:
                surprise_pct = round((act - est) / abs(est) * 100.0, 2)
        country = str(r.get("country") or "").upper()
        comp = str(r.get("comparison") or "")
        period = str(r.get("period") or "")
        out.append({
            "key": "|".join([when.strftime("%Y-%m-%d %H:%M"), country, typ, comp, period]),
            "date": when, "country": country, "type": typ, "comparison": comp, "period": period,
            "importance": "HIGH" if HIGH_ECON_RE.search(typ) else "NORMAL",
            "estimate": est, "actual": act, "previous": prev,
            "surprise": surprise, "surprise_pct": surprise_pct,
        })
    return out


def parse_market(payload: Any) -> Dict[str, Dict[str, Any]]:
    rows = payload if isinstance(payload, list) else ([payload] if isinstance(payload, dict) else [])
    out = {}
    for r in rows:
        code = str((r or {}).get("code") or "")
        if not code:
            continue
        def num(k: str) -> Optional[float]:
            v = r.get(k)
            try:
                return float(v)
            except (TypeError, ValueError):
                return None
        out[code] = {"close": num("close"), "previous_close": num("previousClose"),
                     "change_p": num("change_p")}
    return out


# ----------------------------------------------------------------------------- #
# clustering + grading
# ----------------------------------------------------------------------------- #
def information_grade(members: Sequence[Dict[str, Any]]) -> int:
    """Admiralty-style: 1 confirmed .. 6 social only."""
    denials = [m for m in members if m.get("denial")]
    if denials:
        # A denial weighs against the story only when denials are at least
        # half of the cluster; otherwise the denying items are left out, so
        # an unrelated denial cannot wipe out a confirmed situation (Codex
        # P2, #859).
        if len(denials) * 2 >= len(members):
            return 5
        members = [m for m in members if not m.get("denial")]
    tiers = [m.get("tier", "F") for m in members]
    domains = {m.get("domain") for m in members if m.get("domain")}
    if tiers and all(t == "E" for t in tiers):
        return 6
    if "A" in tiers or (len(domains) >= 3 and "B" in tiers):
        grade = 1
    elif "B" in tiers or len([d for d in domains if tier_of(d) in ("B", "C")]) >= 2:
        grade = 2
    else:
        grade = 3
    if members and all(m.get("rumor") for m in members) and grade > 1:
        grade = 4
    return grade


def cluster_items(items: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
    for it in items:
        if not it.get("event_type") or it["event_type"] == "law_firm_noise":
            continue
        key = (it["scope"], it["event_type"],
               (it.get("anchor") or "general") if it["scope"] == "MACRO" else "")
        groups.setdefault(key, []).append(it)
    clusters = []
    for (scope, etype, anchor), members in groups.items():
        dirs = [m.get("direction", 0) for m in members]
        direction = 1 if sum(dirs) > 0 else (-1 if sum(dirs) < 0 else 0)
        sev = min((m.get("severity") or "L" for m in members), key=lambda s: _SEV_RANK.get(s, 3))
        pubs = [m["published"] for m in members if m.get("published")]
        cid = hashlib.sha1(f"{scope}|{etype}|{anchor}".encode("utf-8")).hexdigest()[:10]
        grade = information_grade(members)
        for m in members:
            m["cluster"] = cid
            m["cluster_grade"] = grade
        clusters.append({
            "id": cid, "scope": scope, "event_type": etype, "anchor": anchor,
            "members": members, "domains": sorted({m.get("domain") for m in members if m.get("domain")}),
            "tier": best_tier(m.get("tier", "F") for m in members), "grade": grade,
            "direction": direction, "severity": sev,
            "latest": max(pubs) if pubs else None,
            "headline": sorted(members, key=lambda m: _TIER_RANK.get(m.get("tier", "F"), 9))[0]["title"],
        })
    clusters.sort(key=lambda c: (_SEV_RANK.get(c["severity"], 3), c["grade"], -len(c["members"])))
    return clusters


# ----------------------------------------------------------------------------- #
# gate
# ----------------------------------------------------------------------------- #
def _ny_offset_hours(day: _dt.date) -> int:
    """US Eastern: -4 between the 2nd Sunday of March and the 1st Sunday of
    November, else -5 (fixed rule since 2007)."""
    def nth_sunday(year: int, month: int, n: int) -> _dt.date:
        d = _dt.date(year, month, 1)
        d += _dt.timedelta(days=(6 - d.weekday()) % 7)
        return d + _dt.timedelta(days=7 * (n - 1))
    start, end = nth_sunday(day.year, 3, 2), nth_sunday(day.year, 11, 1)
    return -4 if start <= day < end else -5


def fomc_decision_times() -> List[_dt.datetime]:
    out = []
    for s in FOMC_DECISION_DAYS:
        d = _dt.date.fromisoformat(s)
        out.append(_dt.datetime(d.year, d.month, d.day, 14, 0,
                                tzinfo=_dt.timezone(_dt.timedelta(hours=_ny_offset_hours(d)))).astimezone(UTC))
    return out


def scheduled_events(now: _dt.datetime, econ: Sequence[Dict[str, Any]],
                     ahead_h: float = 30.0, behind_h: float = 2.0) -> List[Dict[str, Any]]:
    out = []
    for t in fomc_decision_times():
        if now - _dt.timedelta(hours=behind_h) <= t <= now + _dt.timedelta(hours=ahead_h):
            out.append({"when": t, "country": "US", "event": "FOMC rate decision", "source": "fed calendar"})
    seen = {("US", "FOMC rate decision", e["when"].date()) for e in out}
    for e in econ:
        if e["importance"] != "HIGH" or e["country"] != "US":
            continue
        if now - _dt.timedelta(hours=behind_h) <= e["date"] <= now + _dt.timedelta(hours=ahead_h):
            name = e["type"]
            if "interest rate decision" in name.lower() and ("US", "FOMC rate decision", e["date"].date()) in seen:
                continue
            out.append({"when": e["date"], "country": e["country"], "event": name, "source": "eodhd"})
    out.sort(key=lambda x: x["when"])
    return out


def evaluate(clusters: Sequence[Dict[str, Any]], econ: Sequence[Dict[str, Any]],
             market: Dict[str, Dict[str, Any]], now: _dt.datetime) -> Dict[str, Any]:
    """Deterministic, observe-only verdict. Rules (v1.0.0):
    NO_NEW_ENTRY - S&P 500 day change <= -3% or VIX >= 35; or a Gulf-anchored
      high-severity macro story (war escalation / emergency policy / market
      turmoil) graded 1 (an A source, or >= 3 domains including a licensed
      wire) from >= 3 independent domains in the last 24 h.
    CAUTION - a FOMC decision or a high-impact US release within the next
      30 h or the last 2 h; S&P <= -1.5%, VIX >= 25 or VIX +15% on the day;
      any market-anchored high-severity macro story graded 1-2 from >= 3
      domains, or Gulf-anchored graded 3-4; a confirmed (grade <= 2)
      sanctions / credit-stress story on a market anchor.
    Clusters are situations (event type + anchor within the window), not
    single stories: several incidents around one Gulf anchor in 24 h are one
    situation by design (see the #859 review thread).
    Otherwise GO. Per name: BLOCK (8-K 1.03 / 2.04 / 3.01 / 4.02 within 30
    days, or a bankruptcy / delisting / restatement story graded <= 2),
    WATCH (8-K 1.02 / 1.05 / 2.05 / 2.06 / 4.01 within 10 days, or a negative
    story graded <= 2 in 72 h), INFO (8-K 2.02 within 10 days)."""
    level = 0
    drivers: List[str] = []
    positives: List[str] = []

    def bump(to: int, why: str) -> None:
        nonlocal level
        level = max(level, to)
        drivers.append(why)

    sched = scheduled_events(now, econ)
    for s in sched:
        when = fmt_riyadh(s["when"])[11:16]
        day = "today" if s["when"].astimezone(RIYADH).date() == now.astimezone(RIYADH).date() else s["when"].astimezone(RIYADH).strftime("%a %d %b")
        bump(1, f"scheduled: {s['event']} ({day} {when} Riyadh)")

    spx = market.get("GSPC.INDX") or {}
    vix = market.get("VIX.INDX") or {}
    spx_chg, vix_lvl, vix_chg = spx.get("change_p"), vix.get("close"), vix.get("change_p")
    if spx_chg is not None and spx_chg <= -3.0:
        bump(2, f"market: S&P 500 {spx_chg:+.1f}% on the day")
    elif spx_chg is not None and spx_chg <= -1.5:
        bump(1, f"market: S&P 500 {spx_chg:+.1f}% on the day")
    if vix_lvl is not None and vix_lvl >= 35:
        bump(2, f"market: VIX {vix_lvl:.1f}")
    elif (vix_lvl is not None and vix_lvl >= 25) or (vix_chg is not None and vix_chg >= 15):
        bump(1, f"market: VIX {vix_lvl if vix_lvl is not None else float('nan'):.1f} ({(vix_chg or 0):+.0f}%)")

    since = now - _dt.timedelta(hours=24)
    for c in clusters:
        if c["scope"] != "MACRO":
            continue
        recent = [m for m in c["members"] if (m.get("published") or now) >= since]
        if not recent:
            continue
        doms = {m.get("domain") for m in recent if m.get("domain")}
        grade = information_grade(recent)
        anchor = c["anchor"]
        tag = f"{c['event_type']}/{anchor} grade {grade}, {len(doms)} sources: {c['headline'][:90]}"
        if c["event_type"] == "de_escalation" and grade <= 2:
            positives.append(tag)
            continue
        if c["severity"] == "H" and c["event_type"] in ("war_escalation", "emergency_policy", "market_turmoil"):
            if anchor in GULF_ANCHORS and grade == 1 and len(doms) >= 3:
                bump(2, "macro: " + tag)
            elif anchor in MARKET_ANCHORS and grade <= 2 and len(doms) >= 3:
                bump(1, "macro: " + tag)
            elif anchor in GULF_ANCHORS and grade in (3, 4):
                bump(1, "macro (unconfirmed): " + tag)
        elif c["event_type"] in ("trade_sanctions", "credit_stress") and anchor in MARKET_ANCHORS \
                and grade <= 2 and len(doms) >= 2:
            bump(1, "macro: " + tag)

    names: Dict[str, List[Dict[str, str]]] = {}

    def flag(sym: str, lvl: str, why: str) -> None:
        names.setdefault(sym, []).append({"level": lvl, "why": why})

    for c in clusters:
        if c["scope"] == "MACRO":
            continue
        sym = c["scope"]
        for m in c["members"]:
            if m.get("source") != "edgar" or not m.get("published"):
                continue
            age_d = (now - m["published"]).total_seconds() / 86400.0
            items = set(m.get("items") or [])
            hit_block = sorted(items & BLOCK_8K)
            hit_watch = sorted(items & WATCH_8K)
            if hit_block and age_d <= 30:
                flag(sym, "BLOCK", f"{m['form']} item {', '.join(hit_block)} ({age_d:.0f}d ago)")
            elif hit_watch and age_d <= 10:
                flag(sym, "WATCH", f"{m['form']} item {', '.join(hit_watch)} ({age_d:.0f}d ago)")
            if "2.02" in items and age_d <= 10:
                flag(sym, "INFO", f"earnings released ({m['form']} 2.02, {age_d:.0f}d ago)")
        news = [m for m in c["members"] if m.get("source") != "edgar"
                and (m.get("published") or now) >= now - _dt.timedelta(hours=72)]
        if not news:
            continue
        grade = information_grade(news)
        if c["event_type"] in ("bankruptcy", "delisting", "restatement") and grade <= 2:
            flag(sym, "BLOCK", f"{c['event_type']} grade {grade}: {c['headline'][:80]}")
        elif c["direction"] < 0 and grade <= 2:
            flag(sym, "WATCH", f"{c['event_type']} negative, grade {grade}: {c['headline'][:80]}")

    for sym in list(names):
        seen, uniq = set(), []
        for f in names[sym]:
            k = (f["level"], f["why"])
            if k not in seen:
                seen.add(k)
                uniq.append(f)
        names[sym] = uniq
    return {"verdict": VERDICTS[level], "drivers": drivers, "positives": positives,
            "scheduled": sched, "market": {"spx_change_p": spx_chg, "vix": vix_lvl, "vix_change_p": vix_chg},
            "names": names}


# ----------------------------------------------------------------------------- #
# HTTP collectors (injected session: .get(url, params=, headers=, timeout=))
# ----------------------------------------------------------------------------- #
class Collector:
    def __init__(self, http: Any, sleep: Callable[[float], None], now: _dt.datetime):
        self.http, self.sleep, self.now = http, sleep, now
        self.ok: Dict[str, int] = {}
        self.failed: Dict[str, str] = {}

    def _get(self, src: str, url: str, *, params: Optional[Dict[str, Any]] = None,
             headers: Optional[Dict[str, str]] = None, retry_wait: float = 8.0) -> Any:
        for attempt in (1, 2):
            try:
                resp = self.http.get(url, params=params, headers=headers or {}, timeout=25)
            except Exception as exc:  # network error
                if attempt == 2:
                    raise RuntimeError(f"{type(exc).__name__}: {exc}")
                self.sleep(retry_wait)
                continue
            code = getattr(resp, "status_code", 0)
            if code == 200:
                return resp
            if code in (429, 500, 502, 503, 504) and attempt == 1:
                self.sleep(retry_wait)
                continue
            raise RuntimeError(f"HTTP {code}")
        raise RuntimeError("unreachable")

    def _note(self, src: str, n: int) -> None:
        self.ok[src] = self.ok.get(src, 0) + n

    def _fail(self, src: str, why: str) -> None:
        self.failed.setdefault(src, why[:160])

    # -- GDELT ------------------------------------------------------------------
    def gdelt(self, queries: Sequence[Tuple[str, str, str]], spacing: float,
              timespan: str = "24h") -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for i, (scope, label, q) in enumerate(queries):
            if i:
                self.sleep(spacing)
            try:
                resp = self._get("gdelt", GDELT_API, params={
                    "query": q, "mode": "artlist", "format": "json", "maxrecords": "75",
                    "timespan": timespan if scope == "MACRO" else "3d", "sort": "datedesc"},
                    retry_wait=max(10.0, spacing * 2))
                try:
                    payload = resp.json()
                except ValueError:
                    # GDELT answers a malformed query with a plain-text message
                    self._fail("gdelt", f"{label}: non-JSON reply: {str(getattr(resp, 'text', ''))[:100]}")
                    continue
                got = parse_gdelt(payload, scope, label)
                out.extend(got)
                self._note("gdelt", len(got))
            except Exception as exc:
                self._fail("gdelt", f"{label}: {exc}")
        return out

    # -- Google News RSS ---------------------------------------------------------
    def google_news(self, queries: Sequence[Tuple[str, str, str, str]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for i, (scope, label, q, lang) in enumerate(queries):
            if i:
                self.sleep(1.0)
            params = {"ar": {"q": q, "hl": "ar", "gl": "SA", "ceid": "SA:ar"},
                      "en-sa": {"q": q, "hl": "en", "gl": "SA", "ceid": "SA:en"}}.get(
                lang, {"q": q, "hl": "en-US", "gl": "US", "ceid": "US:en"})
            try:
                resp = self._get("google_news", GNEWS_RSS, params=params)
                got = parse_rss(resp.text, scope, label, "google_news", lang[:2])
                out.extend(got)
                self._note("google_news", len(got))
            except Exception as exc:
                self._fail("google_news", f"{label}: {exc}")
        return out

    # -- Fed RSS ----------------------------------------------------------------
    def fed_rss(self) -> List[Dict[str, Any]]:
        try:
            resp = self._get("fed_rss", FED_RSS)
            got = [i for i in parse_rss(resp.text, "MACRO", "fed press", "fed_rss", "en")
                   if not i.get("published") or i["published"] >= self.now - _dt.timedelta(days=3)]
            for i in got:
                i["domain"] = "federalreserve.gov"
            self._note("fed_rss", len(got))
            return got
        except Exception as exc:
            self._fail("fed_rss", str(exc))
            return []

    # -- EDGAR ------------------------------------------------------------------
    def edgar(self, symbols: Sequence[str], user_agent: str) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
        names: Dict[str, str] = {}
        if not user_agent:
            self._fail("edgar", "TFB_SEC_USER_AGENT not set")
            return [], names
        hdr = {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"}
        try:
            tmap = parse_ticker_map(self._get("edgar", EDGAR_TICKERS, headers=hdr).json())
        except Exception as exc:
            self._fail("edgar", f"ticker map: {exc}")
            return [], names
        out: List[Dict[str, Any]] = []
        since = self.now - _dt.timedelta(days=30)
        for sym in symbols:
            if not sym.upper().endswith(".US"):
                continue
            tick = sym[:-3].upper().replace(".", "-")
            hit = tmap.get(tick) or tmap.get(tick.replace("-", "."))
            if not hit:
                continue
            cik, title = hit
            names[sym] = title
            self.sleep(0.15)
            try:
                payload = self._get("edgar", EDGAR_SUBMISSIONS.format(cik=cik), headers=hdr).json()
                got = parse_edgar_submissions(payload, sym, cik, since)
                out.extend(got)
                self._note("edgar", len(got))
            except Exception as exc:
                self._fail("edgar", f"{sym}: {exc}")
        if "edgar" not in self.ok and "edgar" not in self.failed:
            self.ok["edgar"] = 0
        return out, names

    # -- EODHD ------------------------------------------------------------------
    def econ(self, key: str) -> List[Dict[str, Any]]:
        if not key:
            self._fail("econ", "EODHD_API_KEY not set")
            return []
        out: List[Dict[str, Any]] = []
        frm = (self.now - _dt.timedelta(days=2)).strftime("%Y-%m-%d")
        to = (self.now + _dt.timedelta(days=3)).strftime("%Y-%m-%d")
        for country in ("US", "SA"):
            try:
                resp = self._get("econ", EODHD_ECON, params={
                    "api_token": key, "fmt": "json", "from": frm, "to": to,
                    "country": country, "limit": "1000"})
                got = parse_econ_events(resp.json())
                out.extend(got)
                self._note("econ", len(got))
            except Exception as exc:
                self._fail("econ", f"{country}: {_scrub(str(exc), key)}")
        return out

    def market(self, key: str) -> Dict[str, Dict[str, Any]]:
        if not key:
            self._fail("market", "EODHD_API_KEY not set")
            return {}
        try:
            resp = self._get("market", EODHD_RT.format(code="GSPC.INDX"), params={
                "api_token": key, "fmt": "json", "s": "VIX.INDX"})
            got = parse_market(resp.json())
            self._note("market", len(got))
            return got
        except Exception as exc:
            self._fail("market", _scrub(str(exc), key))
            return {}


def _scrub(text: str, secret: str) -> str:
    return text.replace(secret, "***") if secret else text


# ----------------------------------------------------------------------------- #
# queries
# ----------------------------------------------------------------------------- #
GDELT_MACRO_QUERIES: Tuple[Tuple[str, str], ...] = (
    ("geopolitics", '(missile OR airstrike OR "military strike" OR invasion OR "Strait of Hormuz" OR Houthi OR ceasefire OR escalation) sourcelang:english'),
    ("oil", '(OPEC OR "oil prices" OR "crude oil" OR "Brent crude" OR "oil supply") sourcelang:english'),
    ("central banks", '("Federal Reserve" OR FOMC OR "rate cut" OR "rate hike" OR "Treasury yields" OR "interest rates") sourcelang:english'),
    ("market stress", '("stock market" OR "Wall Street" OR stocks) (plunge OR selloff OR "sell-off" OR crash OR "circuit breaker" OR rout) sourcelang:english'),
    ("saudi", '("Saudi Arabia" OR Tadawul OR "Saudi Aramco" OR "Saudi stocks" OR Riyadh) sourcelang:english'),
)
# Arabic Google-News queries (escaped: the repository keeps added lines ASCII)
GNEWS_AR_QUERIES: Tuple[Tuple[str, str], ...] = (
    ("saudi market ar", "\u062a\u062f\u0627\u0648\u0644 OR \u0627\u0644\u0633\u0648\u0642 \u0627\u0644\u0633\u0639\u0648\u062f\u064a\u0629 when:1d"),
    ("oil ar", "\u0623\u0633\u0639\u0627\u0631 \u0627\u0644\u0646\u0641\u0637 OR \u0623\u0648\u0628\u0643 when:1d"),
    ("rates ar", "\u0627\u0644\u0641\u064a\u062f\u0631\u0627\u0644\u064a OR \u0633\u0639\u0631 \u0627\u0644\u0641\u0627\u0626\u062f\u0629 when:1d"),
    ("geopolitics ar", "\u0647\u062c\u0648\u0645 OR \u062a\u0635\u0639\u064a\u062f OR \u0645\u0636\u064a\u0642 \u0647\u0631\u0645\u0632 when:1d"),
)


def _name_query(name: str) -> str:
    cleaned = re.sub(r"\b(inc|corp|corporation|co|company|ltd|plc|holdings?|group|sa|nv|ag|se)\b\.?",
                     " ", name, flags=re.I)
    cleaned = " ".join(cleaned.replace(",", " ").split())
    return f'"{cleaned}"' if cleaned else ""


def build_queries(scope: Sequence[Dict[str, str]]) -> Tuple[List[Tuple[str, str, str]], List[Tuple[str, str, str, str]]]:
    gdelt = [("MACRO", label, q) for label, q in GDELT_MACRO_QUERIES]
    gnews = [("MACRO", label, q, "ar") for label, q in GNEWS_AR_QUERIES]
    for s in scope:
        name = s.get("name") or ""
        nq = _name_query(name)
        if not nq:
            continue
        if s["symbol"].upper().endswith(".SR"):
            gnews.append((s["symbol"], f"name {s['symbol']}", f"{nq} when:3d", "en-sa"))
        else:
            gdelt.append((s["symbol"], f"name {s['symbol']}", nq))
    return gdelt, gnews


# ----------------------------------------------------------------------------- #
# Google Sheets (injected AuthorizedSession-like session)
# ----------------------------------------------------------------------------- #
def _credentials_info() -> Optional[Dict[str, Any]]:
    raw = _env("GOOGLE_SHEETS_CREDENTIALS") or _env("GOOGLE_CREDENTIALS")
    if not raw:
        return None
    try:
        if not raw.startswith("{"):
            raw = base64.b64decode(raw).decode("utf-8")
        info = json.loads(raw)
        return info if isinstance(info, dict) else None
    except Exception:
        return None


def make_sheets_session():  # pragma: no cover - needs google-auth and a real key
    from google.oauth2 import service_account
    from google.auth.transport.requests import AuthorizedSession
    info = _credentials_info()
    if not info:
        raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS missing or unreadable")
    creds = service_account.Credentials.from_service_account_info(info, scopes=list(SCOPES))
    return AuthorizedSession(creds)


def make_http_session():  # pragma: no cover - needs requests
    import requests
    s = requests.Session()
    s.headers.update({"User-Agent": f"TFB-news-gate/{__version__}"})
    return s


def _rng(tab: str, cells: str = "") -> str:
    return quote("'" + tab.replace("'", "''") + "'" + (("!" + cells) if cells else ""), safe="")


def read_values(sheets: Any, sid: str, tab: str, cells: str = "") -> Optional[List[List[Any]]]:
    resp = sheets.get(f"{SHEETS_API}/{sid}/values/{_rng(tab, cells)}?majorDimension=ROWS"
                      "&valueRenderOption=UNFORMATTED_VALUE&dateTimeRenderOption=FORMATTED_STRING",
                      timeout=120)
    if resp.status_code == 400:
        return None  # tab missing
    if resp.status_code != 200:
        raise RuntimeError(f"read {tab}: HTTP {resp.status_code}")
    return list((resp.json() or {}).get("values") or [])


def ensure_tab(sheets: Any, sid: str, tab: str, header: Sequence[str]) -> None:
    vals = read_values(sheets, sid, tab, "A1:Z1")
    if vals is None:
        resp = sheets.post(f"{SHEETS_API}/{sid}:batchUpdate", json={
            "requests": [{"addSheet": {"properties": {"title": tab}}}]}, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"create {tab}: HTTP {resp.status_code}")
        vals = []
    if not vals or not vals[0]:
        resp = sheets.put(f"{SHEETS_API}/{sid}/values/{_rng(tab, 'A1')}?valueInputOption=RAW",
                          json={"values": [list(header)]}, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"header {tab}: HTTP {resp.status_code}")


def append_rows(sheets: Any, sid: str, tab: str, rows: Sequence[Sequence[Any]]) -> None:
    if not rows:
        return
    resp = sheets.post(f"{SHEETS_API}/{sid}/values/{_rng(tab, 'A1')}:append"
                       "?valueInputOption=RAW&insertDataOption=INSERT_ROWS",
                       json={"values": [list(r) for r in rows]}, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"append {tab}: HTTP {resp.status_code}")


def update_rows(sheets: Any, sid: str, tab: str, updates: Sequence[Tuple[int, Sequence[Any]]]) -> None:
    if not updates:
        return
    data = [{"range": "'" + tab.replace("'", "''") + f"'!A{row}", "values": [list(vals)]}
            for row, vals in updates]
    resp = sheets.post(f"{SHEETS_API}/{sid}/values:batchUpdate",
                       json={"valueInputOption": "RAW", "data": data}, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"update {tab}: HTTP {resp.status_code}")


def _header_index(rows: Sequence[Sequence[Any]], wanted: str) -> Tuple[int, int]:
    for r, row in enumerate(rows[:10]):
        for c, cell in enumerate(row):
            if str(cell).strip().lower() == wanted.lower():
                return r, c
    return -1, -1


def read_scope(sheets: Any, sid: str, max_names: int) -> List[Dict[str, str]]:
    """Holdings first, then the latest _Shadow_Rank_Log day (order kept)."""
    out: List[Dict[str, str]] = []
    seen = set()

    def add(sym: str, name: str, role: str) -> None:
        sym = (sym or "").strip().upper()
        if not sym or sym in seen or not re.match(r"^[A-Z0-9][A-Z0-9.\-]*\.[A-Z]{1,4}$", sym):
            return
        seen.add(sym)
        out.append({"symbol": sym, "name": (name or "").strip(), "role": role})

    pf = read_values(sheets, sid, "My_Portfolio", "A1:B200") or []
    hr, sc = _header_index(pf, "Symbol")
    if hr >= 0:
        for row in pf[hr + 1:]:
            if len(row) > sc and row[sc]:
                add(str(row[sc]), str(row[sc + 1]) if len(row) > sc + 1 else "", "holding")
    log = read_values(sheets, sid, "_Shadow_Rank_Log", "A:H") or []
    hr, sc = _header_index(log, "Symbol")
    _, dc = _header_index(log, "Day")
    if hr >= 0 and dc >= 0:
        rows = [r for r in log[hr + 1:] if len(r) > max(sc, dc) and r[sc]]
        days = sorted({str(r[dc]) for r in rows})
        if days:
            for r in rows:
                if str(r[dc]) == days[-1]:
                    add(str(r[sc]), "", "board")
    names: Dict[str, str] = {}
    for tab in ("Market_Leaders", "Global_Markets"):
        vals = read_values(sheets, sid, tab, "A:B") or []
        h, c = _header_index(vals, "Symbol")
        if h < 0:
            continue
        for row in vals[h + 1:]:
            if len(row) > c + 1 and row[c]:
                names.setdefault(str(row[c]).strip().upper(), str(row[c + 1]).strip())
    for s in out:
        if not s["name"]:
            s["name"] = names.get(s["symbol"], "")
    return out[:max_names]


def parse_symbols_env(spec: str) -> List[Dict[str, str]]:
    out = []
    for part in [p for p in (spec or "").split(";") if p.strip()]:
        sym, _, name = part.partition(":")
        if sym.strip():
            out.append({"symbol": sym.strip().upper(), "name": name.strip(), "role": "env"})
    return out


# ----------------------------------------------------------------------------- #
# run
# ----------------------------------------------------------------------------- #
def classify_all(items: List[Dict[str, Any]], clf: Classifier, now: _dt.datetime) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for it in items:
        iid = item_id(it.get("url", ""), it.get("title", ""), it.get("source", ""), it.get("scope", ""))
        if iid in seen:
            continue
        seen.add(iid)
        it["id"] = iid
        it["tier"] = "A" if it.get("source") in ("edgar", "fed_rss") else tier_of(it.get("domain", ""))
        it["first_seen"] = now
        if it.get("source") == "edgar":
            worst = None
            for code in it.get("items") or []:
                meta = EIGHT_K_ITEMS.get(code)
                if meta and (worst is None or _SEV_RANK[meta[2]] < _SEV_RANK[worst[2]]):
                    worst = meta
            it.update({"event_type": worst[0] if worst else "disclosure",
                       "direction": worst[1] if worst else 0, "severity": worst[2] if worst else "L",
                       "rumor": False, "denial": False, "anchor": ""})
        else:
            it.update(clf.classify(it.get("title", ""), it.get("scope", "MACRO")))
        out.append(it)
    return out


def item_row(it: Dict[str, Any], run_id: str) -> List[Any]:
    rd = "denial" if it.get("denial") else ("rumour" if it.get("rumor") else "")
    return [it["id"], fmt_riyadh(it["first_seen"]), fmt_utc(it.get("published")), it["scope"],
            it.get("event_type", ""), it.get("direction", 0), it.get("severity", ""), rd,
            it.get("source", ""), it.get("domain", ""), it.get("tier", "F"),
            it.get("cluster_grade", ""), (it.get("title") or "")[:250], (it.get("url") or "")[:500],
            it.get("cluster", ""), run_id]


def econ_row(e: Dict[str, Any], now: _dt.datetime) -> List[Any]:
    def v(x: Any) -> Any:
        return "" if x is None else x
    return [e["key"], fmt_utc(e["date"]), e["country"], e["type"], e["comparison"], e["period"],
            e["importance"], v(e["estimate"]), v(e["actual"]), v(e["previous"]),
            v(e["surprise"]), v(e["surprise_pct"]), fmt_riyadh(now)]


def gate_row(rep: Dict[str, Any]) -> List[Any]:
    g = rep["gate"]
    names = "; ".join(f"{s} {f['level']}: {f['why']}" for s, fl in sorted(g["names"].items()) for f in fl
                      if f["level"] != "INFO")
    sched = "; ".join(f"{s['event']} {fmt_riyadh(s['when'])[5:16]}" for s in g["scheduled"])
    mk = g["market"]
    market = ", ".join(x for x in [
        f"SPX {mk['spx_change_p']:+.2f}%" if mk["spx_change_p"] is not None else "",
        f"VIX {mk['vix']:.1f}" if mk["vix"] is not None else ""] if x)
    c = rep["counts"]
    return [fmt_riyadh(rep["now"]), rep["run_id"], g["verdict"], " | ".join(g["drivers"])[:1500],
            sched[:500], market, names[:1500],
            f"{c['new']}/{c['classified']}/{c['collected']}",
            ", ".join(f"{k}={v}" for k, v in sorted(rep["sources_ok"].items())),
            "; ".join(f"{k}: {v}" for k, v in sorted(rep["sources_failed"].items()))[:500],
            rep["mode"], __version__]


def render_digest(rep: Dict[str, Any]) -> str:
    g = rep["gate"]
    lines = [f"## News & macro gate v{__version__} - {fmt_riyadh(rep['now'])} Riyadh",
             "", f"**Verdict: {g['verdict']}** (observe-only; mode {rep['mode']}, "
             f"{'written' if rep['wrote'] else 'dry run'})", ""]
    lines.append("**Drivers:** " + ("; ".join(g["drivers"]) if g["drivers"] else "none"))
    if g["positives"]:
        lines.append("**Easing:** " + "; ".join(g["positives"]))
    flagged = [(s, f) for s, fl in sorted(g["names"].items()) for f in fl]
    lines.append("")
    lines.append("**Names:** " + ("; ".join(f"{s} {f['level']} - {f['why']}" for s, f in flagged)
                                   if flagged else "no flags"))
    lines.append("")
    lines.append("| Cluster | Scope | Grade | Tier | Sources | Headline |")
    lines.append("|---|---|---|---|---|---|")
    for c in rep["clusters"][:15]:
        lines.append(f"| {c['event_type']}{'/' + c['anchor'] if c['anchor'] else ''} | {c['scope']} | "
                     f"{c['grade']} | {c['tier']} | {len(c['domains'])} | "
                     f"{c['headline'][:110].replace('|', '/')} |")
    lines.append("")
    c = rep["counts"]
    lines.append(f"Items: collected {c['collected']}, classified {c['classified']}, new {c['new']}. "
                 f"Sources OK: {', '.join(f'{k}={v}' for k, v in sorted(rep['sources_ok'].items())) or 'none'}. "
                 f"Failed: {'; '.join(f'{k}: {v}' for k, v in sorted(rep['sources_failed'].items())) or 'none'}.")
    lines.append("")
    lines.append("Not a licensed financial adviser; the owner decides and executes every trade.")
    return "\n".join(lines) + "\n"


def run(*, http: Any, sheets: Any, sid: str, now: _dt.datetime, write: bool,
        sources: Sequence[str], scope: List[Dict[str, str]], sleep: Callable[[float], None],
        run_id: str, mode: str, tax: Optional[Dict[str, Any]] = None) -> Tuple[int, Dict[str, Any]]:
    clf = Classifier(tax or load_taxonomy())
    col = Collector(http, sleep, now)
    raw: List[Dict[str, Any]] = []
    econ: List[Dict[str, Any]] = []
    market: Dict[str, Dict[str, Any]] = {}
    symbols = [s["symbol"] for s in scope]
    if "edgar" in sources:
        got, sec_names = col.edgar(symbols, _env("TFB_SEC_USER_AGENT"))
        raw.extend(got)
        for s in scope:
            if not s.get("name") and s["symbol"] in sec_names:
                s["name"] = sec_names[s["symbol"]]
    gq, nq = build_queries(scope)
    if "gdelt" in sources:
        raw.extend(col.gdelt(gq, _float_env("TFB_NEWS_GATE_GDELT_SPACING_S", 6.0, 0.0, 60.0)))
    if "google_news" in sources:
        raw.extend(col.google_news(nq))
    if "fed_rss" in sources:
        raw.extend(col.fed_rss())
    key = _env("EODHD_API_KEY")
    if "econ" in sources:
        econ = col.econ(key)
    if "market" in sources:
        market = col.market(key)

    items = classify_all(raw, clf, now)
    classified = [i for i in items if i.get("event_type") and i["event_type"] != "law_firm_noise"]
    clusters = cluster_items(classified)
    gate = evaluate(clusters, econ, market, now)
    rep: Dict[str, Any] = {
        "version": __version__, "now": now, "run_id": run_id, "mode": mode, "wrote": False,
        "scope": scope, "gate": gate, "clusters": clusters,
        "sources_ok": dict(col.ok), "sources_failed": dict(col.failed),
        "counts": {"collected": len(items), "classified": len(classified), "new": 0},
        "econ": econ,
    }
    answered = bool(col.ok)
    rc = 0 if answered else 1
    if write and sheets is not None and sid:
        try:
            max_items = _int_env("TFB_NEWS_GATE_MAX_ITEMS", 200, 1, 2000)
            ensure_tab(sheets, sid, ITEMS_TAB, ITEMS_HEADER)
            existing = {str(r[0]) for r in (read_values(sheets, sid, ITEMS_TAB, "A2:A") or []) if r}
            fresh = [i for i in classified if i["id"] not in existing]
            fresh.sort(key=lambda i: (_SEV_RANK.get(i.get("severity") or "L", 3), i.get("cluster_grade") or 9))
            fresh = fresh[:max_items]
            append_rows(sheets, sid, ITEMS_TAB, [item_row(i, run_id) for i in fresh])
            rep["counts"]["new"] = len(fresh)
            if econ:
                ensure_tab(sheets, sid, ECON_TAB, ECON_HEADER)
                rows = read_values(sheets, sid, ECON_TAB, "A2:M") or []
                index = {str(r[0]): (n + 2, r) for n, r in enumerate(rows) if r}
                keep = [e for e in econ if e["importance"] == "HIGH" or e["country"] == "SA"]
                upd, new = [], []
                for e in keep:
                    row = econ_row(e, now)
                    if e["key"] in index:
                        at, old = index[e["key"]]
                        if [str(x) for x in old[7:12]] != [str(x) for x in row[7:12]]:
                            upd.append((at, row))
                    else:
                        new.append(row)
                update_rows(sheets, sid, ECON_TAB, upd)
                append_rows(sheets, sid, ECON_TAB, new)
            ensure_tab(sheets, sid, GATE_TAB, GATE_HEADER)
            append_rows(sheets, sid, GATE_TAB, [gate_row(rep)])
            rep["wrote"] = True
        except Exception as exc:
            rep["write_error"] = str(exc)[:300]
            rc = 1
    return rc, rep


def _json_default(o: Any) -> Any:
    if isinstance(o, _dt.datetime):
        return o.astimezone(UTC).isoformat()
    return str(o)


def write_evidence(rep: Dict[str, Any], directory: str) -> None:
    if not directory:
        return
    os.makedirs(directory, exist_ok=True)
    slim = dict(rep)
    slim["clusters"] = [{k: v for k, v in c.items() if k != "members"} | {"size": len(c["members"])}
                        for c in rep["clusters"]]
    with open(os.path.join(directory, "news_gate.json"), "w", encoding="utf-8") as fh:
        json.dump(slim, fh, default=_json_default, ensure_ascii=True, indent=1)
    with open(os.path.join(directory, "news_gate.md"), "w", encoding="utf-8") as fh:
        fh.write(render_digest(rep))


def main(argv: Optional[Sequence[str]] = None, *, http: Any = None, sheets: Any = None,
         now: Optional[_dt.datetime] = None, sleep: Optional[Callable[[float], None]] = None) -> int:
    ap = argparse.ArgumentParser(description="TFB news & macro gate (observe-only)")
    mode_g = ap.add_mutually_exclusive_group()
    mode_g.add_argument("--write", action="store_true", help="append to the workbook tabs")
    mode_g.add_argument("--dry-run", action="store_true", help="collect and print only (default)")
    ap.add_argument("--now", default="", help="ISO time for replays (UTC if no offset)")
    args = ap.parse_args(argv)
    mode = gate_mode()
    if mode == "off":
        _out(f"[news_gate v{__version__}] TFB_NEWS_GATE is off - nothing collected, nothing written.")
        return 0
    if args.now:
        try:
            now = _dt.datetime.fromisoformat(args.now)
        except ValueError:
            _out(f"[news_gate v{__version__}] --now is not an ISO time: {args.now!r}")
            return 2
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
    now = (now or _dt.datetime.now(UTC)).astimezone(UTC)
    sid = _env("DEFAULT_SPREADSHEET_ID")
    if args.write and not sid:
        _out(f"[news_gate v{__version__}] --write needs DEFAULT_SPREADSHEET_ID")
        return 2
    if http is None:  # pragma: no cover - live run
        http = make_http_session()
    if sheets is None and sid and _credentials_info():  # pragma: no cover - live run
        sheets = make_sheets_session()
    max_names = _int_env("TFB_NEWS_GATE_MAX_NAMES", 15, 0, 60)
    scope: List[Dict[str, str]] = []
    if sheets is not None and sid:
        try:
            scope = read_scope(sheets, sid, max_names)
        except Exception as exc:
            _out(f"[news_gate v{__version__}] scope read failed ({exc}); continuing with env symbols")
    for s in parse_symbols_env(_env("TFB_NEWS_GATE_SYMBOLS")):
        if s["symbol"] not in {x["symbol"] for x in scope}:
            scope.append(s)
    sources = selected_sources(_env("TFB_NEWS_GATE_SOURCES"))
    run_id = _env("GITHUB_RUN_ID") or now.strftime("local-%Y%m%dT%H%M%S")
    rc, rep = run(http=http, sheets=sheets, sid=sid, now=now, write=bool(args.write),
                  sources=sources, scope=scope, sleep=sleep or time.sleep, run_id=run_id, mode=mode)
    digest = render_digest(rep)
    _out(digest)
    write_evidence(rep, _env("TFB_NEWS_GATE_EVIDENCE_DIR"))
    summary = _env("GITHUB_STEP_SUMMARY")
    if summary:
        try:
            with open(summary, "a", encoding="utf-8") as fh:
                fh.write(digest)
        except OSError:
            pass
    g = rep["gate"]
    _out(f"[news_gate v{__version__}] verdict={g['verdict']} drivers={len(g['drivers'])} "
         f"names_flagged={len(g['names'])} scope={len(scope)} "
         f"sources_ok={sorted(rep['sources_ok'])} sources_failed={sorted(rep['sources_failed'])} "
         f"wrote={rep['wrote']}" + (f" write_error={rep['write_error']}" if rep.get("write_error") else ""))
    return rc


if __name__ == "__main__":
    sys.exit(main())
