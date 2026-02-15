"""
core/news_intelligence.py
============================================================
Tadawul Fast Bridge — News Intelligence (v0.4.0)
(SENTIMENT + QUALITATIVE BOOST + RESILIENT FETCH + LOCALIZATION)

Goal
- Fetch recent news (headlines/snippets) for each symbol/company
- Score sentiment [-1..+1] and confidence [0..1]
- Provide a "news_boost" value you can add into advisor_score
- Localize timestamps to Riyadh time

Safety
- If anything fails (network blocked, RSS unavailable), return neutral scores.
- No heavy ML dependencies. Uses a lexicon-based scorer (fast + stable).

v0.4.0 Enhancements:
- ✅ Riyadh Localization: Adds 'published_riyadh' to articles.
- ✅ Expanded Lexicon: More financial terms.
- ✅ Robust Typing: Stronger dataclasses.
"""

from __future__ import annotations

import asyncio
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta

try:
    import httpx  # lightweight async client
except Exception:
    httpx = None  # type: ignore


TT_NEWS_VERSION = "0.4.0"

# -----------------------------------------------------------------------------
# Config (env overrides)
# -----------------------------------------------------------------------------
DEFAULT_TIMEOUT_SECONDS = 8.0
DEFAULT_MAX_ARTICLES = 8
DEFAULT_CACHE_TTL_SECONDS = 300  # 5 min
DEFAULT_CONCURRENCY = 6

# Defaults kept small + stable.
DEFAULT_RSS_SOURCES: List[str] = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://www.cnbc.com/id/10001147/device/rss/rss.html",
]


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


def _env_list(name: str, default: List[str]) -> List[str]:
    v = (os.getenv(name) or "").strip()
    if not v:
        return default
    parts = [p.strip() for p in v.split(",") if p.strip()]
    return parts or default


NEWS_TIMEOUT_SECONDS = _env_float("NEWS_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)
NEWS_MAX_ARTICLES = _env_int("NEWS_MAX_ARTICLES", DEFAULT_MAX_ARTICLES)
NEWS_CACHE_TTL_SECONDS = _env_int("NEWS_CACHE_TTL_SECONDS", DEFAULT_CACHE_TTL_SECONDS)
NEWS_CONCURRENCY = max(1, min(20, _env_int("NEWS_CONCURRENCY", DEFAULT_CONCURRENCY)))
NEWS_RSS_SOURCES = _env_list("NEWS_RSS_SOURCES", DEFAULT_RSS_SOURCES)


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()

def _to_riyadh_iso(utc_str: Optional[str]) -> Optional[str]:
    if not utc_str: return None
    try:
        # Simple/naive conversion if isoformat
        if utc_str.endswith("Z"):
            dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(utc_str)
        
        tz_riyadh = timezone(timedelta(hours=3))
        return dt.astimezone(tz_riyadh).isoformat()
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
# Sentiment Lexicon (simple, explainable)
# -----------------------------------------------------------------------------
_POS_WORDS = {
    "beat", "beats", "surge", "surges", "soar", "soars", "strong", "record", "growth",
    "profit", "profits", "upgrade", "upgraded", "outperform", "buy", "bullish",
    "rebound", "expansion", "wins", "win", "contract", "contracts", "award", "awarded",
    "raises", "raise", "raised", "higher", "guidance", "buyback", "dividend", "hike",
    "jump", "jumps", "gain", "gains", "rally", "rallies", "positive", "success",
    "merger", "acquisition", "partnership", "deal", "approval", "approved"
}
_NEG_WORDS = {
    "miss", "misses", "plunge", "plunges", "weak", "warning", "downgrade", "downgraded",
    "sell", "bearish", "lawsuit", "probe", "investigation", "fraud", "default",
    "loss", "losses", "cuts", "cut", "cutting", "layoff", "layoffs",
    "bankruptcy", "recall", "halt", "suspends", "suspended", "sanction", "sanctions",
    "drop", "drops", "fall", "falls", "slide", "slides", "negative", "fail", "failure",
    "scandal", "litigation", "fine", "fined", "breach", "violation"
}

_NEG_PHRASES = {"profit warning", "guidance cut", "regulatory probe", "accounting scandal", "sales miss", "lower guidance", "net loss"}
_POS_PHRASES = {"record profit", "raises guidance", "share buyback", "share repurchase", "sales beat", "net profit", "record revenue"}


def _normalize_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _strip_html(s: str) -> str:
    s = s or ""
    s = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", s, flags=re.DOTALL)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _extract_xml_tag(block: str, tag: str) -> str:
    m = re.search(rf"<{tag}\b[^>]*>(.*?)</{tag}>", block, flags=re.IGNORECASE | re.DOTALL)
    return m.group(1).strip() if m else ""


def _extract_link(block: str) -> str:
    """
    RSS: <link>URL</link>
    Atom: <link href="URL" />
    Some feeds put link inside CDATA or include extra whitespace.
    """
    link = _extract_xml_tag(block, "link")
    link = _strip_html(link)
    if link:
        return link

    m = re.search(r'<link[^>]+href="([^"]+)"', block, flags=re.IGNORECASE)
    return m.group(1).strip() if m else ""


def _lexicon_sentiment_score(text: str) -> Tuple[float, float]:
    """
    Returns (sentiment, confidence)
      sentiment: -1..+1
      confidence: 0..1 based on signal strength
    """
    t = _normalize_text(text)
    if not t:
        return 0.0, 0.0

    score = 0.0
    hits = 0

    # phrase weights
    for p in _POS_PHRASES:
        if p in t:
            score += 2.0
            hits += 2
    for p in _NEG_PHRASES:
        if p in t:
            score -= 2.0
            hits += 2

    words = re.findall(r"[a-zA-Z]+", t)
    for w in words:
        if w in _POS_WORDS:
            score += 1.0
            hits += 1
        elif w in _NEG_WORDS:
            score -= 1.0
            hits += 1

    if hits == 0:
        return 0.0, 0.1  # neutral, low confidence

    sentiment = max(-1.0, min(1.0, score / max(3.0, hits)))
    confidence = max(0.2, min(1.0, hits / 10.0))
    return sentiment, confidence


def _to_news_boost(sentiment: float, confidence: float) -> float:
    """
    Convert sentiment into advisor score boost.
    Range: about [-5..+5]
    """
    boost = sentiment * (2.5 + 2.5 * confidence)
    return float(max(-5.0, min(5.0, boost)))


# -----------------------------------------------------------------------------
# Simple in-memory TTL cache
# -----------------------------------------------------------------------------
_CACHE: Dict[str, Tuple[float, NewsResult]] = {}


def _cache_get(key: str) -> Optional[NewsResult]:
    ttl = NEWS_CACHE_TTL_SECONDS
    if ttl <= 0:
        return None
    item = _CACHE.get(key)
    if not item:
        return None
    ts, val = item
    if (time.time() - ts) <= ttl:
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


# -----------------------------------------------------------------------------
# RSS Fetch + Parse
# -----------------------------------------------------------------------------
async def _fetch_text(url: str, timeout_s: float) -> str:
    if httpx is None:
        raise RuntimeError("httpx is not installed/available.")
    
    # Browser-like headers to reduce blocks
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    
    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        return r.text


def _parse_rss_items(xml_text: str, source_url: str, max_items: int) -> List[NewsArticle]:
    txt = xml_text or ""
    # Try items (RSS 2.0)
    items = re.findall(r"<item\b.*?</item>", txt, flags=re.IGNORECASE | re.DOTALL)
    # Try entries (Atom)
    if not items:
        items = re.findall(r"<entry\b.*?</entry>", txt, flags=re.IGNORECASE | re.DOTALL)

    out: List[NewsArticle] = []
    for blk in items[: max(1, max_items)]:
        title = _strip_html(_extract_xml_tag(blk, "title"))
        link = _extract_link(blk)
        pub = _extract_xml_tag(blk, "pubDate") or _extract_xml_tag(blk, "updated")
        desc = _extract_xml_tag(blk, "description") or _extract_xml_tag(blk, "summary") or _extract_xml_tag(blk, "content")
        
        pub_clean = _strip_html(pub) if pub else None

        out.append(
            NewsArticle(
                title=title,
                url=link,
                source=source_url,
                published_utc=pub_clean,
                published_riyadh=_to_riyadh_iso(pub_clean),
                snippet=_strip_html(desc)[:240] if desc else "",
            )
        )
    return out


def _match_relevance(articles: List[NewsArticle], query_terms: List[str]) -> List[NewsArticle]:
    q = [t.lower() for t in query_terms if t]
    if not q:
        return articles[:]
    matched: List[NewsArticle] = []
    for a in articles:
        text = f"{a.title} {a.snippet}".lower()
        if any(term in text for term in q):
            matched.append(a)
    return matched if matched else articles[:]


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
    """
    sym = (symbol or "").strip().upper()
    nm = (company_name or "").strip()

    cache_key = f"{sym}|{nm[:40].lower()}|{max_articles}"
    if use_cache:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

    rss_sources = rss_sources or NEWS_RSS_SOURCES

    # Query terms: symbol + significant words in name
    terms = [sym] if sym else []
    if nm:
        terms += [w for w in re.findall(r"[A-Za-z0-9]+", nm) if len(w) > 3]
    terms = list(dict.fromkeys([t.strip() for t in terms if t.strip()]))[:6]

    # If httpx not available, return neutral
    if httpx is None:
        r = NewsResult(symbol=sym, query=" ".join(terms), sentiment=0.0, confidence=0.0, news_boost=0.0, articles=[])
        if use_cache:
            _cache_set(cache_key, r)
        return r

    all_articles: List[NewsArticle] = []

    # Sequential fetch to be polite but resilient
    for src in rss_sources:
        try:
            xml = await _fetch_text(src, timeout_s=timeout_s)
            arts = _parse_rss_items(xml, source_url=src, max_items=max_articles)
            arts = _match_relevance(arts, terms)
            all_articles.extend(arts)
        except Exception:
            # silent fail => neutral overall later
            continue

    # De-dup by title
    seen = set()
    deduped: List[NewsArticle] = []
    for a in all_articles:
        key = _normalize_text(a.title)[:140]
        if key and key not in seen:
            seen.add(key)
            deduped.append(a)

    deduped = deduped[:max_articles]

    corpus = " | ".join([f"{a.title}. {a.snippet}" for a in deduped])
    sentiment, confidence = _lexicon_sentiment_score(corpus)
    boost = _to_news_boost(sentiment, confidence)

    r = NewsResult(
        symbol=sym,
        query=" ".join(terms),
        sentiment=sentiment,
        confidence=confidence,
        news_boost=boost,
        articles=deduped,
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
    max_articles = max(1, int(max_articles or NEWS_MAX_ARTICLES))
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
            "sources_count": len(rss_sources or []),
            "cache_ttl_s": NEWS_CACHE_TTL_SECONDS,
            "concurrency": concurrency,
        },
    }


__all__ = ["get_news_intelligence", "batch_news_intelligence", "TT_NEWS_VERSION"]
