# core/news_intelligence.py
"""
core/news_intelligence.py
============================================================
Tadawul Fast Bridge — News Intelligence (v0.6.0)
(QUERY RSS + MULTI-SOURCE FALLBACK + RECENCY WEIGHTED SENTIMENT + LRU TTL CACHE + RIYADH LOCALIZATION)

Goal
- Fetch recent news (headlines/snippets) for each symbol/company
- Score sentiment [-1..+1] and confidence [0..1]
- Provide a "news_boost" value you can add into advisor_score (default clamp: -5..+5)
- Localize timestamps to Riyadh time

Safety / PROD Rules
- If anything fails (network blocked, RSS unavailable), return NEUTRAL scores.
- No heavy ML dependencies. Uses explainable lexicon scorer + light heuristics.
- Uses stdlib XML parsing (ElementTree) and robust date parsing.

What’s improved vs v0.4.0
- ✅ Query-based RSS (Google News RSS search) + fallback to global RSS sources
- ✅ ElementTree parsing (RSS + Atom) with namespace handling (more reliable than regex)
- ✅ Recency-weighted aggregation (newer articles matter more)
- ✅ Relevance ranking (title/snippet matches) before scoring
- ✅ Negation handling + intensifiers + severe-risk penalties
- ✅ LRU TTL cache with max size + pruning
- ✅ Stronger typing + consistent article normalization
"""

from __future__ import annotations

import asyncio
import math
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus
from collections import OrderedDict

try:
    import httpx  # lightweight async client
except Exception:
    httpx = None  # type: ignore

import xml.etree.ElementTree as ET

TT_NEWS_VERSION = "0.6.0"

# -----------------------------------------------------------------------------
# Config (env overrides)
# -----------------------------------------------------------------------------
DEFAULT_TIMEOUT_SECONDS = 8.0
DEFAULT_MAX_ARTICLES = 10
DEFAULT_CACHE_TTL_SECONDS = 300  # 5 min
DEFAULT_CONCURRENCY = 8
DEFAULT_MAX_CACHE_ITEMS = 2000

# Global RSS sources (fallback mode)
DEFAULT_RSS_SOURCES: List[str] = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://www.cnbc.com/id/10001147/device/rss/rss.html",
]

# Query RSS provider (primary)
# Google News RSS search is very practical for ticker/company queries.
DEFAULT_NEWS_QUERY_MODE = "google+rss"  # "google" | "rss" | "google+rss"
DEFAULT_GOOGLE_HL = "en"
DEFAULT_GOOGLE_GL = "SA"
DEFAULT_GOOGLE_CEID = "SA:en"  # can be SA:en or SA:ar
DEFAULT_GOOGLE_FRESH_DAYS = 7  # limit query recency
DEFAULT_ALLOW_NETWORK = True

# sentiment -> boost clamp
DEFAULT_BOOST_CLAMP = 5.0

# recency weighting half-life (hours): lower = more weight to fresh news
DEFAULT_RECENCY_HALFLIFE_HOURS = 36.0


def _env_float(name: str, default: float) -> float:
    try:
        v = (os.getenv(name) or "").strip()
        return float(v) if v else default
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        v = (os.getenv(name) or "").strip()
        return int(float(v)) if v else default
    except Exception:
        return default


def _env_str(name: str, default: str) -> str:
    v = (os.getenv(name) or "").strip()
    return v or default


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok"}


def _env_list(name: str, default: List[str]) -> List[str]:
    v = (os.getenv(name) or "").strip()
    if not v:
        return default
    parts = [p.strip() for p in v.split(",") if p.strip()]
    return parts or default


NEWS_TIMEOUT_SECONDS = _env_float("NEWS_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)
NEWS_MAX_ARTICLES = max(1, min(30, _env_int("NEWS_MAX_ARTICLES", DEFAULT_MAX_ARTICLES)))
NEWS_CACHE_TTL_SECONDS = max(0, _env_int("NEWS_CACHE_TTL_SECONDS", DEFAULT_CACHE_TTL_SECONDS))
NEWS_CONCURRENCY = max(1, min(20, _env_int("NEWS_CONCURRENCY", DEFAULT_CONCURRENCY)))
NEWS_MAX_CACHE_ITEMS = max(50, min(10000, _env_int("NEWS_MAX_CACHE_ITEMS", DEFAULT_MAX_CACHE_ITEMS)))

NEWS_RSS_SOURCES = _env_list("NEWS_RSS_SOURCES", DEFAULT_RSS_SOURCES)

NEWS_QUERY_MODE = _env_str("NEWS_QUERY_MODE", DEFAULT_NEWS_QUERY_MODE).strip().lower()
NEWS_GOOGLE_HL = _env_str("NEWS_GOOGLE_HL", DEFAULT_GOOGLE_HL).strip()
NEWS_GOOGLE_GL = _env_str("NEWS_GOOGLE_GL", DEFAULT_GOOGLE_GL).strip()
NEWS_GOOGLE_CEID = _env_str("NEWS_GOOGLE_CEID", DEFAULT_GOOGLE_CEID).strip()
NEWS_GOOGLE_FRESH_DAYS = max(1, min(30, _env_int("NEWS_GOOGLE_FRESH_DAYS", DEFAULT_GOOGLE_FRESH_DAYS)))

NEWS_ALLOW_NETWORK = _env_bool("NEWS_ALLOW_NETWORK", DEFAULT_ALLOW_NETWORK)
NEWS_BOOST_CLAMP = max(1.0, min(10.0, _env_float("NEWS_BOOST_CLAMP", DEFAULT_BOOST_CLAMP)))
NEWS_RECENCY_HALFLIFE_HOURS = max(6.0, min(168.0, _env_float("NEWS_RECENCY_HALFLIFE_HOURS", DEFAULT_RECENCY_HALFLIFE_HOURS)))

# -----------------------------------------------------------------------------
# Time helpers
# -----------------------------------------------------------------------------
def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()


def _to_riyadh_iso(dt_utc: Optional[datetime]) -> Optional[str]:
    if not dt_utc:
        return None
    try:
        tz_riyadh = timezone(timedelta(hours=3))
        return dt_utc.astimezone(tz_riyadh).isoformat()
    except Exception:
        return None


def _parse_any_datetime(x: Optional[str]) -> Optional[datetime]:
    """
    Tries common RSS/Atom formats:
    - RFC 2822: "Mon, 15 Jan 2026 12:34:56 GMT"
    - ISO 8601: "2026-01-15T12:34:56Z" or "+00:00"
    Returns aware UTC datetime when possible.
    """
    if not x:
        return None
    s = str(x).strip()
    if not s:
        return None

    # RFC 2822 (RSS pubDate)
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # ISO 8601
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Data structures
# -----------------------------------------------------------------------------
@dataclass
class NewsArticle:
    title: str
    url: str = ""
    source: str = ""
    published_utc: Optional[str] = None
    published_riyadh: Optional[str] = None
    snippet: str = ""


@dataclass
class NewsResult:
    symbol: str
    query: str
    sentiment: float  # -1..+1
    confidence: float  # 0..1
    news_boost: float  # points to add to advisor score (e.g., -5..+5)
    articles: List[NewsArticle] = field(default_factory=list)


# -----------------------------------------------------------------------------
# Text normalization
# -----------------------------------------------------------------------------
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _strip_html(s: str) -> str:
    s = s or ""
    s = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", s, flags=re.DOTALL)
    s = _TAG_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def _norm_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = _WS_RE.sub(" ", s)
    return s


def _canon_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    # remove common tracking fragments
    u = u.split("#", 1)[0]
    return u


# -----------------------------------------------------------------------------
# Sentiment Lexicon (explainable + finance-aware)
# -----------------------------------------------------------------------------
_POS_WORDS = {
    # results/earnings
    "beat", "beats", "surge", "surges", "soar", "soars", "strong", "record", "growth",
    "profit", "profits", "upgrade", "upgraded", "outperform", "buy", "bullish",
    "rebound", "expansion", "wins", "win", "contract", "contracts", "award", "awarded",
    "raises", "raise", "raised", "higher", "guidance", "buyback", "dividend", "hike",
    "jump", "jumps", "gain", "gains", "rally", "rallies", "positive", "success",
    "merger", "acquisition", "partnership", "deal", "approval", "approved",
    # finance
    "cashflow", "cash", "margin", "resilient", "upside", "undervalued",
    "accelerate", "accelerates", "improve", "improves", "improved",
    "pipeline", "backlog", "order", "orders",
}

_NEG_WORDS = {
    "miss", "misses", "plunge", "plunges", "weak", "warning", "downgrade", "downgraded",
    "sell", "bearish", "lawsuit", "probe", "investigation", "fraud", "default",
    "loss", "losses", "cuts", "cut", "cutting", "layoff", "layoffs",
    "bankruptcy", "recall", "halt", "suspends", "suspended", "sanction", "sanctions",
    "drop", "drops", "fall", "falls", "slide", "slides", "negative", "fail", "failure",
    "scandal", "litigation", "fine", "fined", "breach", "violation",
    # finance
    "dilution", "dilutive", "downtime", "risk", "risks", "volatility",
    "cashburn", "crisis", "liquidity", "defaulted",
}

# high-severity words (extra penalty when present)
_SEVERE_NEG = {"fraud", "bankruptcy", "default", "scandal", "sanction", "sanctions", "probe", "investigation", "lawsuit", "recall"}

_POS_PHRASES = {
    "record profit": 2.5,
    "raises guidance": 2.5,
    "share buyback": 2.0,
    "share repurchase": 2.0,
    "sales beat": 2.0,
    "net profit": 1.8,
    "record revenue": 2.2,
    "strategic partnership": 1.5,
    "contract award": 1.5,
}

_NEG_PHRASES = {
    "profit warning": -2.5,
    "guidance cut": -2.5,
    "regulatory probe": -2.0,
    "accounting scandal": -3.0,
    "sales miss": -2.0,
    "lower guidance": -2.0,
    "net loss": -1.8,
    "going concern": -3.0,
}

_NEGATIONS = {"not", "no", "never", "without", "hardly", "rarely"}
_INTENSIFIERS_POS = {"strongly", "significantly", "sharp", "surging", "soaring", "record"}
_INTENSIFIERS_NEG = {"sharply", "significantly", "plunging", "crashing", "severe", "serious"}


_TOKEN_RE = re.compile(r"[A-Za-z]+|[0-9]+|[\u0600-\u06FF]+")


def _tokenize(s: str) -> List[str]:
    return _TOKEN_RE.findall(_norm_text(s))


def _lexicon_score_one(text: str) -> Tuple[float, float, int, int]:
    """
    Returns:
      sentiment [-1..+1], confidence [0..1], pos_hits, neg_hits
    """
    t = _norm_text(text)
    if not t:
        return 0.0, 0.0, 0, 0

    score = 0.0
    pos_hits = 0
    neg_hits = 0

    # phrase weights (higher impact)
    for p, w in _POS_PHRASES.items():
        if p in t:
            score += w
            pos_hits += 2
    for p, w in _NEG_PHRASES.items():
        if p in t:
            score += w
            neg_hits += 2

    toks = _tokenize(t)
    if not toks:
        return 0.0, 0.0, 0, 0

    # Negation window: flip the next 3 tokens sentiment words
    flip_window = 0

    for i, tok in enumerate(toks):
        if tok in _NEGATIONS:
            flip_window = 3
            continue

        local = tok
        w = 0.0
        if local in _POS_WORDS:
            w = 1.0
        elif local in _NEG_WORDS:
            w = -1.0

        # intensifiers (small)
        prev = toks[i - 1] if i > 0 else ""
        if w > 0 and (prev in _INTENSIFIERS_POS):
            w *= 1.2
        if w < 0 and (prev in _INTENSIFIERS_NEG):
            w *= 1.2

        if w != 0.0:
            if flip_window > 0:
                w = -w
            if w > 0:
                pos_hits += 1
            else:
                neg_hits += 1
            score += w

        if flip_window > 0:
            flip_window -= 1

    hits = pos_hits + neg_hits
    if hits == 0:
        return 0.0, 0.1, 0, 0

    # Normalize: bounded by hits
    raw = score / max(3.0, float(hits))
    sentiment = max(-1.0, min(1.0, raw))

    # Confidence: more hits => higher; cap at 1.0
    confidence = max(0.2, min(1.0, hits / 10.0))

    # Severe negatives reduce confidence + add slight negative pull
    if any(w in t for w in _SEVERE_NEG):
        confidence = max(0.2, confidence * 0.9)
        sentiment = min(sentiment, sentiment - 0.05)

    return sentiment, confidence, pos_hits, neg_hits


def _sentiment_to_boost(sentiment: float, confidence: float) -> float:
    """
    Convert sentiment into advisor score boost.
    Target range: about [-NEWS_BOOST_CLAMP..+NEWS_BOOST_CLAMP]
    """
    # strong confidence increases magnitude
    magnitude = 2.0 + 3.0 * max(0.0, min(1.0, confidence))
    boost = sentiment * magnitude
    return float(max(-NEWS_BOOST_CLAMP, min(NEWS_BOOST_CLAMP, boost)))


# -----------------------------------------------------------------------------
# Cache (LRU + TTL)
# -----------------------------------------------------------------------------
# key -> (ts, NewsResult)
_CACHE: "OrderedDict[str, Tuple[float, NewsResult]]" = OrderedDict()


def _cache_get(key: str) -> Optional[NewsResult]:
    ttl = NEWS_CACHE_TTL_SECONDS
    if ttl <= 0:
        return None
    item = _CACHE.get(key)
    if not item:
        return None
    ts, val = item
    if (time.time() - ts) <= ttl:
        # refresh LRU
        try:
            _CACHE.move_to_end(key)
        except Exception:
            pass
        return val
    # expired
    try:
        _CACHE.pop(key, None)
    except Exception:
        pass
    return None


def _cache_set(key: str, val: NewsResult) -> None:
    ttl = NEWS_CACHE_TTL_SECONDS
    if ttl <= 0:
        return
    _CACHE[key] = (time.time(), val)
    try:
        _CACHE.move_to_end(key)
    except Exception:
        pass

    # prune
    while len(_CACHE) > NEWS_MAX_CACHE_ITEMS:
        try:
            _CACHE.popitem(last=False)
        except Exception:
            break


# -----------------------------------------------------------------------------
# Fetch + Parse (RSS + Atom)
# -----------------------------------------------------------------------------
async def _fetch_text(url: str, timeout_s: float) -> str:
    if httpx is None:
        raise RuntimeError("httpx is not installed/available.")
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept": "application/rss+xml,application/atom+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)
    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True, headers=headers, limits=limits) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.text


def _et_text(node: Optional[ET.Element]) -> str:
    if node is None:
        return ""
    return _strip_html("".join(node.itertext() or "").strip())


def _find_first(parent: ET.Element, tags: List[str]) -> Optional[ET.Element]:
    for t in tags:
        n = parent.find(t)
        if n is not None:
            return n
    # namespace-agnostic search
    for child in list(parent):
        tag = child.tag.split("}")[-1].lower()
        if tag in {x.split("}")[-1].lower() for x in tags}:
            return child
    return None


def _extract_link_et(item: ET.Element) -> str:
    # RSS: <link>URL</link>
    n = _find_first(item, ["link"])
    if n is not None:
        # Atom: <link href="..."/>
        href = n.attrib.get("href") or ""
        if href.strip():
            return _canon_url(href.strip())
        txt = _et_text(n)
        if txt:
            return _canon_url(txt)

    # Atom: multiple links, choose rel=alternate
    for child in item.findall(".//"):
        if child.tag.split("}")[-1].lower() == "link":
            href = (child.attrib.get("href") or "").strip()
            rel = (child.attrib.get("rel") or "").strip().lower()
            if href and (rel in {"", "alternate"}):
                return _canon_url(href)
    return ""


def _parse_feed(xml_text: str, source_url: str, max_items: int) -> List[NewsArticle]:
    txt = xml_text or ""
    out: List[NewsArticle] = []
    if not txt.strip():
        return out

    try:
        root = ET.fromstring(txt)
    except Exception:
        return out

    # Identify items: RSS -> channel/item, Atom -> entry
    items: List[ET.Element] = []
    # RSS
    channel = None
    if root.tag.split("}")[-1].lower() == "rss":
        for ch in root:
            if ch.tag.split("}")[-1].lower() == "channel":
                channel = ch
                break
    if channel is not None:
        for it in channel:
            if it.tag.split("}")[-1].lower() == "item":
                items.append(it)
    else:
        # Atom or RSS without wrapper
        for it in root.findall(".//"):
            if it.tag.split("}")[-1].lower() == "entry":
                items.append(it)

    for it in items[: max(1, max_items)]:
        title = _et_text(_find_first(it, ["title"]))
        link = _extract_link_et(it)

        pub = _et_text(_find_first(it, ["pubDate", "published", "updated"]))
        dt = _parse_any_datetime(pub) if pub else None
        pub_utc_iso = dt.astimezone(timezone.utc).isoformat() if dt else None
        pub_riyadh = _to_riyadh_iso(dt)

        desc = _et_text(_find_first(it, ["description", "summary", "content"]))
        snippet = (desc or "")[:260]

        if not title and not snippet:
            continue

        out.append(
            NewsArticle(
                title=title or snippet[:80],
                url=link,
                source=source_url,
                published_utc=pub_utc_iso,
                published_riyadh=pub_riyadh,
                snippet=snippet,
            )
        )
    return out


def _build_google_news_rss_url(query: str) -> str:
    # Google News RSS search supports q= and hl/gl/ceid.
    # Use "when:Xd" to bias freshness.
    q = (query or "").strip()
    if NEWS_GOOGLE_FRESH_DAYS > 0:
        q = f'{q} when:{NEWS_GOOGLE_FRESH_DAYS}d'
    return (
        "https://news.google.com/rss/search?q="
        + quote_plus(q)
        + f"&hl={quote_plus(NEWS_GOOGLE_HL)}&gl={quote_plus(NEWS_GOOGLE_GL)}&ceid={quote_plus(NEWS_GOOGLE_CEID)}"
    )


def _make_query_terms(symbol: str, company_name: str) -> List[str]:
    sym = (symbol or "").strip().upper()
    nm = (company_name or "").strip()

    terms: List[str] = []
    if sym:
        # handle KSA suffix variants
        terms.append(sym)
        if sym.endswith(".SR"):
            terms.append(sym[:-3])
        if sym.endswith(".SA"):
            terms.append(sym[:-3])
        # some feeds prefer without punctuation
        terms.append(sym.replace(".", ""))

    if nm:
        # keep only meaningful words
        words = re.findall(r"[A-Za-z0-9]{3,}", nm)
        for w in words:
            lw = w.lower().strip()
            if lw in {"the", "and", "for", "inc", "ltd", "llc", "group", "co", "company"}:
                continue
            terms.append(w)

    # unique preserve order
    seen = set()
    out: List[str] = []
    for t in terms:
        k = t.strip().lower()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(t.strip())
    return out[:8]


def _relevance_score(article: NewsArticle, terms: List[str]) -> float:
    """
    Simple relevance: term matches in title count more than snippet.
    """
    if not terms:
        return 1.0
    title = _norm_text(article.title)
    snip = _norm_text(article.snippet)
    score = 0.0
    for t in terms:
        tt = _norm_text(t)
        if not tt:
            continue
        if tt in title:
            score += 2.0
        if tt in snip:
            score += 1.0
    return score


def _dedupe_articles(articles: List[NewsArticle]) -> List[NewsArticle]:
    seen = set()
    out: List[NewsArticle] = []
    for a in articles:
        key = (_norm_text(a.title)[:160], _canon_url(a.url))
        if key in seen:
            continue
        seen.add(key)
        out.append(a)
    return out


def _age_hours(published_utc_iso: Optional[str]) -> Optional[float]:
    dt = _parse_any_datetime(published_utc_iso) if published_utc_iso else None
    if not dt:
        # already iso? try parse as iso
        try:
            dt = datetime.fromisoformat((published_utc_iso or "").replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
        except Exception:
            return None
    now = datetime.now(timezone.utc)
    return max(0.0, (now - dt).total_seconds() / 3600.0)


def _recency_weight(hours_old: Optional[float]) -> float:
    """
    Exponential decay using half-life in hours.
    """
    if hours_old is None:
        return 0.6  # unknown time => medium weight
    hl = float(NEWS_RECENCY_HALFLIFE_HOURS)
    return float(0.5 ** (hours_old / max(1.0, hl)))


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
async def get_news_intelligence(
    symbol: str,
    company_name: str = "",
    *,
    rss_sources: Optional[List[str]] = None,
    max_articles: int = NEWS_MAX_ARTICLES,
    timeout_s: float = NEWS_TIMEOUT_SECONDS,
    use_cache: bool = True,
) -> NewsResult:
    """
    Returns NewsResult for one symbol.
    Query strategy:
      - If NEWS_QUERY_MODE includes 'google': use Google News RSS search for (symbol + name terms)
      - If NEWS_QUERY_MODE includes 'rss': also scan generic RSS sources as fallback
    """
    sym = (symbol or "").strip().upper()
    nm = (company_name or "").strip()

    q_terms = _make_query_terms(sym, nm)
    query = " ".join(q_terms).strip() or sym or nm or ""

    cache_key = f"{sym}|{_norm_text(nm)[:40]}|{max_articles}|{NEWS_QUERY_MODE}|{NEWS_GOOGLE_CEID}"
    if use_cache:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

    # Network guard
    if not NEWS_ALLOW_NETWORK or httpx is None:
        r = NewsResult(symbol=sym, query=query, sentiment=0.0, confidence=0.0, news_boost=0.0, articles=[])
        if use_cache:
            _cache_set(cache_key, r)
        return r

    max_articles = max(1, min(30, int(max_articles or NEWS_MAX_ARTICLES)))
    timeout_s = float(timeout_s or NEWS_TIMEOUT_SECONDS)

    sources = rss_sources or NEWS_RSS_SOURCES

    # Build fetch plan
    fetch_urls: List[str] = []
    mode = (NEWS_QUERY_MODE or "google+rss").lower()

    if "google" in mode:
        if query:
            fetch_urls.append(_build_google_news_rss_url(query))

    if "rss" in mode:
        fetch_urls.extend([s for s in (sources or []) if isinstance(s, str) and s.strip()])

    # Fetch sequentially (resilient) but keep list small.
    # If you want faster, you can parallelize sources, but sequential is safer under blocks.
    all_articles: List[NewsArticle] = []
    for url in fetch_urls:
        try:
            xml = await _fetch_text(url, timeout_s=timeout_s)
            arts = _parse_feed(xml, source_url=url, max_items=max_articles * 2)
            all_articles.extend(arts)
        except Exception:
            continue

    all_articles = _dedupe_articles(all_articles)

    # Rank by relevance, then keep top
    if all_articles and q_terms:
        scored = [(a, _relevance_score(a, q_terms)) for a in all_articles]
        scored.sort(key=lambda x: x[1], reverse=True)
        all_articles = [a for a, s in scored if s > 0.0] or [a for a, _ in scored]
    all_articles = all_articles[: max_articles]

    # If nothing found => neutral
    if not all_articles:
        r = NewsResult(symbol=sym, query=query, sentiment=0.0, confidence=0.1, news_boost=0.0, articles=[])
        if use_cache:
            _cache_set(cache_key, r)
        return r

    # Score each article (title + snippet), aggregate with recency weights
    w_sum = 0.0
    s_sum = 0.0
    conf_sum = 0.0
    sentiments: List[float] = []

    for a in all_articles:
        text = f"{a.title}. {a.snippet}".strip()
        s_i, c_i, pos_hits, neg_hits = _lexicon_score_one(text)

        # age/recency weight
        age_h = _age_hours(a.published_utc)
        w = _recency_weight(age_h)

        # extra penalty if severe negative present in very recent headline
        if age_h is not None and age_h <= 24.0:
            tnorm = _norm_text(text)
            if any(x in tnorm for x in _SEVERE_NEG):
                s_i = min(s_i, s_i - 0.15)
                c_i = min(1.0, c_i + 0.05)

        w_sum += w
        s_sum += w * s_i
        conf_sum += w * c_i
        sentiments.append(s_i)

    sentiment = (s_sum / w_sum) if w_sum > 0 else 0.0
    sentiment = float(max(-1.0, min(1.0, sentiment)))

    # confidence: combine avg confidence + sample size + consistency
    avg_conf = (conf_sum / w_sum) if w_sum > 0 else 0.1
    n = len(all_articles)
    size_factor = 0.6 + 0.4 * min(1.0, n / 6.0)

    # consistency factor: lower variance => higher confidence
    if sentiments:
        mean = sum(sentiments) / len(sentiments)
        var = sum((x - mean) ** 2 for x in sentiments) / max(1, len(sentiments))
        # map variance into [0.6..1.0]
        consistency = 1.0 - min(0.4, var)
    else:
        consistency = 0.8

    confidence = float(max(0.0, min(1.0, avg_conf * size_factor * consistency)))

    boost = _sentiment_to_boost(sentiment, confidence)

    r = NewsResult(
        symbol=sym,
        query=query,
        sentiment=sentiment,
        confidence=confidence,
        news_boost=boost,
        articles=all_articles,
    )

    if use_cache:
        _cache_set(cache_key, r)
    return r


async def batch_news_intelligence(
    items: List[Dict[str, str]],
    *,
    rss_sources: Optional[List[str]] = None,
    max_articles: int = NEWS_MAX_ARTICLES,
    timeout_s: float = NEWS_TIMEOUT_SECONDS,
    concurrency: int = NEWS_CONCURRENCY,
) -> Dict[str, Any]:
    """
    items: [{"symbol": "...", "name": "..."}, ...]
    Returns:
      {"items": [NewsResult-as-dict...], "meta": {...}}
    """
    rss_sources = rss_sources or NEWS_RSS_SOURCES
    max_articles = max(1, min(30, int(max_articles or NEWS_MAX_ARTICLES)))
    timeout_s = float(timeout_s or NEWS_TIMEOUT_SECONDS)
    concurrency = max(1, min(20, int(concurrency or NEWS_CONCURRENCY)))

    sem = asyncio.Semaphore(concurrency)

    async def _one(it: Dict[str, str]) -> Optional[Dict[str, Any]]:
        sym = str(it.get("symbol", "") or "").strip()
        nm = str(it.get("name", "") or "").strip()
        if not sym:
            return None
        async with sem:
            r = await get_news_intelligence(
                sym,
                nm,
                rss_sources=rss_sources,
                max_articles=max_articles,
                timeout_s=timeout_s,
                use_cache=True,
            )
        return {
            "symbol": r.symbol,
            "query": r.query,
            "sentiment": r.sentiment,
            "confidence": r.confidence,
            "news_boost": r.news_boost,
            "articles": [
                {
                    "title": a.title,
                    "url": a.url,
                    "source": a.source,
                    "published_utc": a.published_utc,
                    "published_riyadh": a.published_riyadh,
                    "snippet": a.snippet,
                }
                for a in r.articles
            ],
        }

    t0 = time.time()
    tasks = [_one(it) for it in (items or [])]
    raw = await asyncio.gather(*tasks, return_exceptions=True)

    results: List[Dict[str, Any]] = []
    for x in raw:
        if x is None:
            continue
        if isinstance(x, Exception):
            continue
        results.append(x)

    ms = int((time.time() - t0) * 1000)

    return {
        "items": results,
        "meta": {
            "version": TT_NEWS_VERSION,
            "count": len(results),
            "elapsed_ms": ms,
            "mode": NEWS_QUERY_MODE,
            "google": {"hl": NEWS_GOOGLE_HL, "gl": NEWS_GOOGLE_GL, "ceid": NEWS_GOOGLE_CEID, "fresh_days": NEWS_GOOGLE_FRESH_DAYS},
            "sources_count": len(rss_sources or []),
            "cache_ttl_s": NEWS_CACHE_TTL_SECONDS,
            "cache_max_items": NEWS_MAX_CACHE_ITEMS,
            "concurrency": concurrency,
            "timeout_s": timeout_s,
        },
    }


__all__ = ["get_news_intelligence", "batch_news_intelligence", "TT_NEWS_VERSION", "NewsResult", "NewsArticle"]
