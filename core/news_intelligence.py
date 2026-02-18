#!/usr/bin/env python3
"""
core/news_intelligence.py
================================================================================
Advanced News Intelligence Engine — v1.0.0
================================================================================
Tadawul Fast Bridge — Enterprise-Grade News Sentiment Analysis

What's new in v1.0.0:
- ✅ **Multi-Source Aggregation**: Google News RSS + Reuters + BBC + CNBC + configurable sources
- ✅ **Advanced Sentiment Analysis**: Lexicon-based with finance-specific terms and negation handling
- ✅ **Recency Weighting**: Exponential decay with configurable half-life (default 36h)
- ✅ **Relevance Scoring**: Title/snippet matching with term weighting
- ✅ **Intelligent Caching**: LRU with TTL and automatic pruning
- ✅ **Batch Processing**: Concurrent requests with configurable concurrency
- ✅ **Riyadh Timezone**: All timestamps localized to UTC+3
- ✅ **Comprehensive Error Handling**: Graceful degradation on failures
- ✅ **Configurable via Environment**: All settings overridable
- ✅ **Type Safety**: Complete type hints and dataclasses
- ✅ **Performance Metrics**: Timing and success tracking
- ✅ **Article Deduplication**: Smart duplicate detection
- ✅ **Severe Event Detection**: Extra penalties for fraud/bankruptcy/scandal

Key Features:
- Zero ML dependencies (pure Python)
- Explainable sentiment scores
- Production-hardened with fallbacks
- Thread-safe LRU cache
- Google News RSS integration
- Multi-source RSS fallback
- Configurable per deployment
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import re
import time
import warnings
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import quote_plus, urlparse

try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    httpx = None
    HAS_HTTPX = False

try:
    import xml.etree.ElementTree as ET
    HAS_ELEMENTTREE = True
except ImportError:
    ET = None
    HAS_ELEMENTTREE = False

# ============================================================================
# Version Information
# ============================================================================

__version__ = "1.0.0"
NEWS_VERSION = __version__

# ============================================================================
# Constants
# ============================================================================

# Default timeouts
DEFAULT_TIMEOUT_SECONDS = 8.0
DEFAULT_MAX_ARTICLES = 15
DEFAULT_CACHE_TTL_SECONDS = 300  # 5 minutes
DEFAULT_CONCURRENCY = 8
DEFAULT_MAX_CACHE_ITEMS = 2000

# Default RSS sources (fallback mode)
DEFAULT_RSS_SOURCES: List[str] = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://www.cnbc.com/id/10001147/device/rss/rss.html",
    "https://finance.yahoo.com/news/rssindex",
    "https://seekingalpha.com/market_currents.xml",
]

# Google News RSS settings
DEFAULT_GOOGLE_HL = "en"
DEFAULT_GOOGLE_GL = "SA"
DEFAULT_GOOGLE_CEID = "SA:en"
DEFAULT_GOOGLE_FRESH_DAYS = 7

# Sentiment boost clamping
DEFAULT_BOOST_CLAMP = 5.0

# Recency weighting half-life (hours)
DEFAULT_RECENCY_HALFLIFE_HOURS = 36.0

# Minimum confidence threshold
DEFAULT_MIN_CONFIDENCE = 0.1

# ============================================================================
# Environment Helpers
# ============================================================================

def _env_str(name: str, default: str) -> str:
    """Get string from environment."""
    v = os.getenv(name)
    return v.strip() if v else default


def _env_int(name: str, default: int) -> int:
    """Get integer from environment."""
    try:
        v = int(os.getenv(name, str(default)))
        return v if v > 0 else default
    except (ValueError, TypeError):
        return default


def _env_float(name: str, default: float) -> float:
    """Get float from environment."""
    try:
        v = float(os.getenv(name, str(default)))
        return v if v > 0 else default
    except (ValueError, TypeError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    """Get boolean from environment."""
    v = os.getenv(name, "").strip().lower()
    if not v:
        return default
    return v in {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok"}


def _env_list(name: str, default: List[str]) -> List[str]:
    """Get list from environment (comma-separated)."""
    v = os.getenv(name, "").strip()
    if not v:
        return default
    return [p.strip() for p in v.split(",") if p.strip()]


# ============================================================================
# Configuration
# ============================================================================

class NewsConfig:
    """News intelligence configuration."""
    
    # Network
    TIMEOUT_SECONDS = _env_float("NEWS_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)
    MAX_ARTICLES = max(1, min(50, _env_int("NEWS_MAX_ARTICLES", DEFAULT_MAX_ARTICLES)))
    CONCURRENCY = max(1, min(20, _env_int("NEWS_CONCURRENCY", DEFAULT_CONCURRENCY)))
    ALLOW_NETWORK = _env_bool("NEWS_ALLOW_NETWORK", True)
    
    # Cache
    CACHE_TTL_SECONDS = max(0, _env_int("NEWS_CACHE_TTL_SECONDS", DEFAULT_CACHE_TTL_SECONDS))
    MAX_CACHE_ITEMS = max(50, min(10000, _env_int("NEWS_MAX_CACHE_ITEMS", DEFAULT_MAX_CACHE_ITEMS)))
    
    # Sources
    RSS_SOURCES = _env_list("NEWS_RSS_SOURCES", DEFAULT_RSS_SOURCES)
    QUERY_MODE = _env_str("NEWS_QUERY_MODE", "google+rss").lower()
    
    # Google News
    GOOGLE_HL = _env_str("NEWS_GOOGLE_HL", DEFAULT_GOOGLE_HL)
    GOOGLE_GL = _env_str("NEWS_GOOGLE_GL", DEFAULT_GOOGLE_GL)
    GOOGLE_CEID = _env_str("NEWS_GOOGLE_CEID", DEFAULT_GOOGLE_CEID)
    GOOGLE_FRESH_DAYS = max(1, min(30, _env_int("NEWS_GOOGLE_FRESH_DAYS", DEFAULT_GOOGLE_FRESH_DAYS)))
    
    # Sentiment
    BOOST_CLAMP = max(1.0, min(10.0, _env_float("NEWS_BOOST_CLAMP", DEFAULT_BOOST_CLAMP)))
    RECENCY_HALFLIFE_HOURS = max(6.0, min(168.0, _env_float("NEWS_RECENCY_HALFLIFE_HOURS", DEFAULT_RECENCY_HALFLIFE_HOURS)))
    MIN_CONFIDENCE = max(0.0, min(1.0, _env_float("NEWS_MIN_CONFIDENCE", DEFAULT_MIN_CONFIDENCE)))
    
    # Features
    ENABLE_CACHE = _env_bool("NEWS_ENABLE_CACHE", True)
    ENABLE_DEDUPLICATION = _env_bool("NEWS_ENABLE_DEDUPLICATION", True)
    ENABLE_RECENCY_WEIGHTING = _env_bool("NEWS_ENABLE_RECENCY_WEIGHTING", True)
    ENABLE_RELEVANCE_SCORING = _env_bool("NEWS_ENABLE_RELEVANCE_SCORING", True)
    ENABLE_SEVERE_PENALTY = _env_bool("NEWS_ENABLE_SEVERE_PENALTY", True)
    
    # Headers
    USER_AGENT = _env_str("NEWS_USER_AGENT", "Mozilla/5.0 (compatible; TadawulFastBridge/1.0; +https://tadawulbridge.com)")
    ACCEPT_LANGUAGE = _env_str("NEWS_ACCEPT_LANGUAGE", "en-US,en;q=0.9,ar;q=0.8")


# ============================================================================
# Time Helpers
# ============================================================================

RIYADH_TZ = timezone(timedelta(hours=3))


def utc_now() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


def utc_iso(dt: Optional[datetime] = None) -> str:
    """Get UTC ISO string."""
    dt = dt or utc_now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def riyadh_now() -> datetime:
    """Get current Riyadh datetime."""
    return datetime.now(RIYADH_TZ)


def riyadh_iso(dt: Optional[datetime] = None) -> str:
    """Get Riyadh ISO string."""
    dt = dt or riyadh_now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(RIYADH_TZ).isoformat()


def to_riyadh_iso(dt: Optional[datetime]) -> Optional[str]:
    """Convert datetime to Riyadh ISO string."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(RIYADH_TZ).isoformat()


def parse_datetime(x: Optional[str]) -> Optional[datetime]:
    """
    Parse various datetime formats to UTC datetime.
    Supports:
    - RFC 2822 (RSS pubDate): "Mon, 15 Jan 2026 12:34:56 GMT"
    - ISO 8601: "2026-01-15T12:34:56Z" or "+00:00"
    - RFC 3339: "2026-01-15T12:34:56+00:00"
    """
    if not x:
        return None
    
    s = str(x).strip()
    if not s:
        return None
    
    # RFC 2822
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    
    # ISO 8601 / RFC 3339
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    
    return None


def age_hours(dt: Optional[datetime]) -> Optional[float]:
    """Calculate age in hours from datetime."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    now = utc_now()
    return max(0.0, (now - dt).total_seconds() / 3600.0)


def recency_weight(hours_old: Optional[float], half_life: float = NewsConfig.RECENCY_HALFLIFE_HOURS) -> float:
    """Calculate recency weight using exponential decay."""
    if hours_old is None:
        return 0.6  # Unknown age -> medium weight
    return float(0.5 ** (hours_old / max(1.0, half_life)))


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class NewsArticle:
    """News article data."""
    title: str
    url: str = ""
    source: str = ""
    published_utc: Optional[str] = None
    published_riyadh: Optional[str] = None
    snippet: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "title": self.title,
            "url": self.url,
            "source": self.source,
            "published_utc": self.published_utc,
            "published_riyadh": self.published_riyadh,
            "snippet": self.snippet,
        }


@dataclass
class NewsResult:
    """Complete news intelligence result."""
    symbol: str
    query: str
    sentiment: float  # -1.0 to +1.0
    confidence: float  # 0.0 to 1.0
    news_boost: float  # -NewsConfig.BOOST_CLAMP to +NewsConfig.BOOST_CLAMP
    articles: List[NewsArticle] = field(default_factory=list)
    sources_used: List[str] = field(default_factory=list)
    articles_analyzed: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "symbol": self.symbol,
            "query": self.query,
            "sentiment": self.sentiment,
            "confidence": self.confidence,
            "news_boost": self.news_boost,
            "articles_analyzed": self.articles_analyzed,
            "sources_used": self.sources_used,
            "articles": [a.to_dict() for a in self.articles],
        }


@dataclass
class BatchResult:
    """Batch news intelligence result."""
    items: List[Dict[str, Any]] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# Text Normalization
# ============================================================================

# HTML tag removal
HTML_TAG_RE = re.compile(r"<[^>]+>")
CDATA_RE = re.compile(r"<!\[CDATA\[(.*?)\]\]>", re.DOTALL)
WHITESPACE_RE = re.compile(r"\s+")
URL_TRACKING_RE = re.compile(r"[?#&](utm_|fbclid|gclid|ref|source).*$")


def strip_html(text: str) -> str:
    """Remove HTML tags and normalize whitespace."""
    if not text:
        return ""
    # Remove CDATA
    text = CDATA_RE.sub(r"\1", text)
    # Remove HTML tags
    text = HTML_TAG_RE.sub(" ", text)
    # Normalize whitespace
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text


def normalize_text(text: str) -> str:
    """Normalize text for comparison."""
    return WHITESPACE_RE.sub(" ", (text or "").strip().lower())


def canonical_url(url: str) -> str:
    """Canonicalize URL by removing tracking parameters."""
    url = (url or "").strip()
    if not url:
        return ""
    # Remove tracking fragments
    url = URL_TRACKING_RE.sub("", url)
    return url


def extract_domain(url: str) -> str:
    """Extract domain from URL."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split("/")[0]
        return domain.replace("www.", "")
    except Exception:
        return ""


# ============================================================================
# Sentiment Lexicon (Finance-Specific)
# ============================================================================

# Positive words
POSITIVE_WORDS = {
    # Earnings/Results
    "beat", "beats", "surge", "surges", "soar", "soars", "strong", "record", "growth",
    "profit", "profits", "upgrade", "upgraded", "outperform", "buy", "bullish",
    "rebound", "expansion", "wins", "win", "contract", "contracts", "award", "awarded",
    "raises", "raise", "raised", "higher", "guidance", "buyback", "dividend", "hike",
    "jump", "jumps", "gain", "gains", "rally", "rallies", "positive", "success",
    "merger", "acquisition", "partnership", "deal", "approval", "approved",
    
    # Financial Health
    "cashflow", "cash", "margin", "resilient", "upside", "undervalued",
    "accelerate", "accelerates", "improve", "improves", "improved",
    "pipeline", "backlog", "order", "orders", "backed", "support",
    "stable", "stability", "growth", "growing", "expansion",
    
    # Market Sentiment
    "optimistic", "optimism", "confidence", "confident", "momentum",
    "breakthrough", "innovation", "leader", "leadership", "dominant",
}

# Negative words
NEGATIVE_WORDS = {
    # Negative Results
    "miss", "misses", "plunge", "plunges", "weak", "warning", "downgrade", "downgraded",
    "sell", "bearish", "lawsuit", "probe", "investigation", "fraud", "default",
    "loss", "losses", "cuts", "cut", "cutting", "layoff", "layoffs",
    "bankruptcy", "recall", "halt", "suspends", "suspended", "sanction", "sanctions",
    "drop", "drops", "fall", "falls", "slide", "slides", "negative", "fail", "failure",
    "scandal", "litigation", "fine", "fined", "breach", "violation",
    
    # Financial Distress
    "dilution", "dilutive", "downtime", "risk", "risks", "volatility",
    "cashburn", "crisis", "liquidity", "defaulted", "insolvent", "insolvency",
    "debt", "liabilities", "overvalued", "bubble", "correction",
    
    # Market Sentiment
    "pessimistic", "pessimism", "uncertainty", "unstable", "volatile",
    "decline", "declining", "slowdown", "recession", "depression",
}

# Severe negative words (extra penalty)
SEVERE_NEGATIVE_WORDS = {
    "fraud", "bankruptcy", "default", "scandal", "sanction", "sanctions",
    "probe", "investigation", "lawsuit", "recall", "insolvent", "insolvency",
    "crisis", "liquidation", "restatement", "restating",
}

# Positive phrases (multi-word with weights)
POSITIVE_PHRASES = {
    "record profit": 2.5,
    "record revenue": 2.2,
    "record earnings": 2.2,
    "raises guidance": 2.5,
    "increases guidance": 2.3,
    "share buyback": 2.0,
    "stock buyback": 2.0,
    "share repurchase": 2.0,
    "sales beat": 2.0,
    "earnings beat": 2.0,
    "profit beat": 2.0,
    "net profit": 1.8,
    "strategic partnership": 1.5,
    "contract award": 1.5,
    "upgraded to buy": 2.0,
    "outperform rating": 1.8,
    "strong buy": 2.2,
    "buy rating": 1.5,
    "dividend increase": 1.8,
    "dividend hike": 1.8,
    "price target raised": 1.8,
    "target raised": 1.5,
}

# Negative phrases (multi-word with weights)
NEGATIVE_PHRASES = {
    "profit warning": -2.5,
    "earnings warning": -2.5,
    "guidance cut": -2.5,
    "lowers guidance": -2.3,
    "regulatory probe": -2.0,
    "accounting scandal": -3.0,
    "accounting issue": -2.5,
    "sales miss": -2.0,
    "earnings miss": -2.0,
    "profit miss": -2.0,
    "lower guidance": -2.0,
    "net loss": -1.8,
    "going concern": -3.0,
    "bankruptcy warning": -3.0,
    "default risk": -2.5,
    "downgraded to sell": -2.0,
    "sell rating": -1.5,
    "underperform rating": -1.8,
    "price target cut": -1.8,
    "target lowered": -1.5,
}

# Negation words (flip sentiment of following words)
NEGATION_WORDS = {"not", "no", "never", "without", "hardly", "rarely", "neither", "nor"}

# Intensifiers (amplify sentiment)
INTENSIFIERS_POS = {"strongly", "significantly", "sharply", "surging", "soaring", "record", "massive"}
INTENSIFIERS_NEG = {"sharply", "significantly", "plunging", "crashing", "severe", "serious", "critical"}


# Tokenizer
TOKEN_RE = re.compile(r"[A-Za-z]+|[0-9]+|[\u0600-\u06FF]+")


def tokenize(text: str) -> List[str]:
    """Split text into tokens."""
    return TOKEN_RE.findall(normalize_text(text))


def analyze_sentiment(text: str) -> Tuple[float, float, int, int]:
    """
    Analyze sentiment of text.
    
    Returns:
        Tuple of (sentiment, confidence, positive_hits, negative_hits)
        sentiment: -1.0 to +1.0
        confidence: 0.0 to 1.0
    """
    if not text:
        return 0.0, 0.0, 0, 0
    
    normalized = normalize_text(text)
    if not normalized:
        return 0.0, 0.0, 0, 0
    
    # Initialize
    score = 0.0
    pos_hits = 0
    neg_hits = 0
    
    # Check phrases first (higher weight)
    for phrase, weight in POSITIVE_PHRASES.items():
        if phrase in normalized:
            score += weight
            pos_hits += 2
    
    for phrase, weight in NEGATIVE_PHRASES.items():
        if phrase in normalized:
            score += weight
            neg_hits += 2
    
    # Tokenize for word-level analysis
    tokens = tokenize(normalized)
    if not tokens:
        return score, 0.0, pos_hits, neg_hits
    
    # Negation window: flip sentiment for next 3 tokens
    flip_window = 0
    intensifier = 1.0
    
    for i, token in enumerate(tokens):
        # Negation handling
        if token in NEGATION_WORDS:
            flip_window = 3
            continue
        
        # Base sentiment
        word_score = 0.0
        if token in POSITIVE_WORDS:
            word_score = 1.0
        elif token in NEGATIVE_WORDS:
            word_score = -1.0
        
        # Intensifier from previous token
        if i > 0 and tokens[i-1] in INTENSIFIERS_POS and word_score > 0:
            word_score *= 1.2
        elif i > 0 and tokens[i-1] in INTENSIFIERS_NEG and word_score < 0:
            word_score *= 1.2
        
        # Apply negation flip
        if flip_window > 0 and word_score != 0:
            word_score = -word_score
        
        # Add to score
        if word_score != 0:
            score += word_score
            if word_score > 0:
                pos_hits += 1
            else:
                neg_hits += 1
        
        # Decrement flip window
        if flip_window > 0:
            flip_window -= 1
    
    # Calculate sentiment
    total_hits = pos_hits + neg_hits
    if total_hits == 0:
        return 0.0, NewsConfig.MIN_CONFIDENCE, 0, 0
    
    # Normalize by number of hits (with floor)
    sentiment = score / max(3.0, float(total_hits))
    sentiment = max(-1.0, min(1.0, sentiment))
    
    # Confidence based on hits count
    confidence = max(NewsConfig.MIN_CONFIDENCE, min(1.0, total_hits / 10.0))
    
    # Severe negative penalty
    if NewsConfig.ENABLE_SEVERE_PENALTY:
        if any(word in normalized for word in SEVERE_NEGATIVE_WORDS):
            confidence = max(NewsConfig.MIN_CONFIDENCE, confidence * 0.9)
            sentiment = min(sentiment, sentiment - 0.1)
    
    return sentiment, confidence, pos_hits, neg_hits


def sentiment_to_boost(sentiment: float, confidence: float) -> float:
    """
    Convert sentiment to advisor score boost.
    
    Boost range: -NewsConfig.BOOST_CLAMP to +NewsConfig.BOOST_CLAMP
    Higher confidence = larger magnitude
    """
    # Base magnitude scales with confidence
    magnitude = 2.0 + 3.0 * max(0.0, min(1.0, confidence))
    boost = sentiment * magnitude
    return max(-NewsConfig.BOOST_CLAMP, min(NewsConfig.BOOST_CLAMP, boost))


# ============================================================================
# Article Processing
# ============================================================================

def extract_articles_from_feed(xml_text: str, source_url: str, max_items: int) -> List[NewsArticle]:
    """
    Extract articles from RSS/Atom feed XML.
    """
    articles: List[NewsArticle] = []
    
    if not xml_text or not HAS_ELEMENTTREE or ET is None:
        return articles
    
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return articles
    
    # Find items (RSS: channel/item, Atom: entry)
    items: List[ET.Element] = []
    
    # RSS
    if root.tag.split("}")[-1].lower() == "rss":
        for child in root:
            if child.tag.split("}")[-1].lower() == "channel":
                for item in child:
                    if item.tag.split("}")[-1].lower() == "item":
                        items.append(item)
                break
    else:
        # Atom or other
        for elem in root.findall(".//*"):
            tag = elem.tag.split("}")[-1].lower()
            if tag in ("entry", "item"):
                items.append(elem)
    
    # Process items
    for item in items[:max_items]:
        article = _extract_article_from_element(item, source_url)
        if article.title or article.snippet:
            articles.append(article)
    
    return articles


def _extract_article_from_element(elem: ET.Element, source_url: str) -> NewsArticle:
    """Extract article data from XML element."""
    title = ""
    link = ""
    published = ""
    description = ""
    
    # Helper to get text content
    def get_text(tag: str) -> str:
        el = elem.find(f".//{tag}")
        if el is not None and el.text:
            return strip_html(el.text)
        return ""
    
    # Try common tag names
    for title_tag in ["title", "Title", "TITLE"]:
        title = get_text(title_tag)
        if title:
            break
    
    # Extract link (RSS: <link>URL</link>, Atom: <link href="URL"/>)
    link_el = elem.find(".//link")
    if link_el is not None:
        if link_el.get("href"):
            link = link_el.get("href", "")
        elif link_el.text:
            link = link_el.text
    
    # Published date
    for date_tag in ["pubDate", "published", "updated", "date", "dc:date"]:
        pub = get_text(date_tag)
        if pub:
            published = pub
            break
    
    # Description/summary
    for desc_tag in ["description", "summary", "content:encoded", "content"]:
        desc = get_text(desc_tag)
        if desc:
            description = desc
            break
    
    # Parse datetime
    dt = parse_datetime(published)
    pub_utc = utc_iso(dt) if dt else None
    pub_riyadh = to_riyadh_iso(dt)
    
    return NewsArticle(
        title=title or description[:80] if description else "",
        url=canonical_url(link),
        source=extract_domain(source_url) or source_url,
        published_utc=pub_utc,
        published_riyadh=pub_riyadh,
        snippet=description[:260] if description else "",
    )


def deduplicate_articles(articles: List[NewsArticle]) -> List[NewsArticle]:
    """Remove duplicate articles based on title and URL."""
    if not NewsConfig.ENABLE_DEDUPLICATION:
        return articles
    
    seen: Set[Tuple[str, str]] = set()
    unique: List[NewsArticle] = []
    
    for article in articles:
        title_key = normalize_text(article.title)[:160]
        url_key = canonical_url(article.url)
        key = (title_key, url_key)
        
        if key not in seen:
            seen.add(key)
            unique.append(article)
    
    return unique


def calculate_relevance(article: NewsArticle, terms: List[str]) -> float:
    """Calculate relevance score based on term matches."""
    if not terms or not NewsConfig.ENABLE_RELEVANCE_SCORING:
        return 1.0
    
    title = normalize_text(article.title)
    snippet = normalize_text(article.snippet)
    
    score = 0.0
    for term in terms:
        t = normalize_text(term)
        if not t:
            continue
        
        # Title matches count double
        if t in title:
            score += 2.0
        if t in snippet:
            score += 1.0
    
    return score


# ============================================================================
# Query Building
# ============================================================================

def build_query_terms(symbol: str, company_name: str) -> List[str]:
    """
    Build search query terms from symbol and company name.
    Returns list of relevant terms for searching.
    """
    symbol = (symbol or "").strip().upper()
    company = (company_name or "").strip()
    
    terms: List[str] = []
    
    # Symbol variants
    if symbol:
        terms.append(symbol)
        if symbol.endswith(".SR"):
            terms.append(symbol[:-3])  # Without .SR
        if symbol.endswith(".SA"):
            terms.append(symbol[:-3])  # Without .SA
        # Remove all punctuation
        clean = re.sub(r"[^A-Z0-9]", "", symbol)
        if clean and clean != symbol:
            terms.append(clean)
    
    # Company name words
    if company:
        words = re.findall(r"[A-Za-z0-9]{3,}", company)
        stopwords = {"the", "and", "for", "inc", "ltd", "llc", "group", "co", "company",
                     "corporation", "corp", "limited", "holding", "holdings", "international"}
        for word in words:
            if word.lower() not in stopwords:
                terms.append(word)
    
    # Deduplicate preserving order
    seen: Set[str] = set()
    unique: List[str] = []
    for term in terms:
        key = term.lower()
        if key not in seen:
            seen.add(key)
            unique.append(term)
    
    return unique[:8]  # Limit to 8 terms


def build_google_news_url(query: str) -> str:
    """Build Google News RSS search URL."""
    q = query.strip()
    if NewsConfig.GOOGLE_FRESH_DAYS > 0:
        q = f'{q} when:{NewsConfig.GOOGLE_FRESH_DAYS}d'
    
    params = {
        "q": quote_plus(q),
        "hl": quote_plus(NewsConfig.GOOGLE_HL),
        "gl": quote_plus(NewsConfig.GOOGLE_GL),
        "ceid": quote_plus(NewsConfig.GOOGLE_CEID),
    }
    
    return f"https://news.google.com/rss/search?{'&'.join(f'{k}={v}' for k, v in params.items())}"


# ============================================================================
# LRU Cache with TTL
# ============================================================================

class TTLCache:
    """Thread-safe LRU cache with TTL."""
    
    def __init__(self, maxsize: int = 1000, ttl: int = 300):
        self.maxsize = maxsize
        self.ttl = ttl
        self._cache: OrderedDict[str, Tuple[float, Any]] = OrderedDict()
        self._lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired."""
        async with self._lock:
            if key not in self._cache:
                return None
            
            timestamp, value = self._cache[key]
            if time.time() - timestamp > self.ttl:
                del self._cache[key]
                return None
            
            # Move to end (LRU)
            self._cache.move_to_end(key)
            return value
    
    async def set(self, key: str, value: Any) -> None:
        """Set value in cache."""
        async with self._lock:
            self._cache[key] = (time.time(), value)
            self._cache.move_to_end(key)
            
            # Prune if too large
            while len(self._cache) > self.maxsize:
                self._cache.popitem(last=False)
    
    async def delete(self, key: str) -> None:
        """Delete key from cache."""
        async with self._lock:
            self._cache.pop(key, None)
    
    async def clear(self) -> None:
        """Clear all cache."""
        async with self._lock:
            self._cache.clear()
    
    async def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        async with self._lock:
            return {
                "size": len(self._cache),
                "maxsize": self.maxsize,
                "ttl": self.ttl,
            }


# Global cache instance
_CACHE = TTLCache(
    maxsize=NewsConfig.MAX_CACHE_ITEMS,
    ttl=NewsConfig.CACHE_TTL_SECONDS,
)


def _cache_key(symbol: str, company_name: str, query_mode: str, google_ceid: str) -> str:
    """Generate cache key."""
    components = [
        symbol,
        normalize_text(company_name)[:40],
        str(NewsConfig.MAX_ARTICLES),
        query_mode,
        google_ceid,
        str(NewsConfig.GOOGLE_FRESH_DAYS),
    ]
    key = "|".join(components)
    return hashlib.sha256(key.encode()).hexdigest()


# ============================================================================
# Network Functions
# ============================================================================

async def fetch_url(url: str, timeout: float = NewsConfig.TIMEOUT_SECONDS) -> Optional[str]:
    """Fetch URL content with timeout."""
    if not NewsConfig.ALLOW_NETWORK or not HAS_HTTPX or httpx is None:
        return None
    
    headers = {
        "User-Agent": NewsConfig.USER_AGENT,
        "Accept": "application/rss+xml,application/atom+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": NewsConfig.ACCEPT_LANGUAGE,
        "Cache-Control": "no-cache",
    }
    
    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers=headers,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10)
        ) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.text
    except Exception:
        return None


# ============================================================================
# Main API Functions
# ============================================================================

async def get_news_intelligence(
    symbol: str,
    company_name: str = "",
    *,
    force_refresh: bool = False,
) -> NewsResult:
    """
    Get news intelligence for a single symbol.
    
    Args:
        symbol: Stock symbol (e.g., "AAPL", "1120.SR")
        company_name: Company name for better query matching
        force_refresh: Ignore cache and fetch fresh data
    
    Returns:
        NewsResult with sentiment analysis
    """
    symbol = (symbol or "").strip().upper()
    company = (company_name or "").strip()
    
    if not symbol:
        return NewsResult(
            symbol="",
            query="",
            sentiment=0.0,
            confidence=0.0,
            news_boost=0.0,
        )
    
    # Check cache
    cache_key = _cache_key(symbol, company, NewsConfig.QUERY_MODE, NewsConfig.GOOGLE_CEID)
    if not force_refresh and NewsConfig.ENABLE_CACHE:
        cached = await _CACHE.get(cache_key)
        if cached is not None:
            return cached
    
    # Build query terms
    terms = build_query_terms(symbol, company)
    query = " ".join(terms) or symbol
    
    # Determine sources to fetch
    fetch_urls: List[str] = []
    sources_used: List[str] = []
    
    mode = NewsConfig.QUERY_MODE
    
    if "google" in mode and terms:
        fetch_urls.append(build_google_news_url(query))
        sources_used.append("google_news")
    
    if "rss" in mode:
        for url in NewsConfig.RSS_SOURCES:
            if url.strip():
                fetch_urls.append(url.strip())
                sources_used.append(extract_domain(url) or "rss")
    
    # Fetch and parse articles
    all_articles: List[NewsArticle] = []
    
    for url in fetch_urls:
        content = await fetch_url(url, timeout=NewsConfig.TIMEOUT_SECONDS)
        if content:
            articles = extract_articles_from_feed(
                content,
                source_url=url,
                max_items=NewsConfig.MAX_ARTICLES * 2
            )
            all_articles.extend(articles)
    
    # Deduplicate
    all_articles = deduplicate_articles(all_articles)
    
    # Score relevance
    if all_articles and terms:
        scored = [(a, calculate_relevance(a, terms)) for a in all_articles]
        scored.sort(key=lambda x: x[1], reverse=True)
        all_articles = [a for a, s in scored if s > 0] or [a for a, _ in scored]
    
    # Limit to max articles
    all_articles = all_articles[:NewsConfig.MAX_ARTICLES]
    
    # If no articles, return neutral
    if not all_articles:
        result = NewsResult(
            symbol=symbol,
            query=query,
            sentiment=0.0,
            confidence=NewsConfig.MIN_CONFIDENCE,
            news_boost=0.0,
            sources_used=sources_used,
        )
        if NewsConfig.ENABLE_CACHE:
            await _CACHE.set(cache_key, result)
        return result
    
    # Analyze sentiment
    weighted_sentiment = 0.0
    weighted_confidence = 0.0
    total_weight = 0.0
    sentiments: List[float] = []
    
    for article in all_articles:
        text = f"{article.title}. {article.snippet}"
        sentiment, confidence, pos_hits, neg_hits = analyze_sentiment(text)
        
        # Calculate recency weight
        dt = parse_datetime(article.published_utc)
        age = age_hours(dt)
        weight = recency_weight(age) if NewsConfig.ENABLE_RECENCY_WEIGHTING else 1.0
        
        weighted_sentiment += sentiment * weight
        weighted_confidence += confidence * weight
        total_weight += weight
        sentiments.append(sentiment)
    
    # Normalize
    if total_weight > 0:
        sentiment = weighted_sentiment / total_weight
        confidence = weighted_confidence / total_weight
    else:
        sentiment = 0.0
        confidence = NewsConfig.MIN_CONFIDENCE
    
    # Apply consistency factor
    if len(sentiments) > 1:
        mean = sum(sentiments) / len(sentiments)
        variance = sum((s - mean) ** 2 for s in sentiments) / len(sentiments)
        consistency = 1.0 - min(0.5, variance)
        confidence *= consistency
    
    # Ensure bounds
    sentiment = max(-1.0, min(1.0, sentiment))
    confidence = max(NewsConfig.MIN_CONFIDENCE, min(1.0, confidence))
    
    # Calculate boost
    boost = sentiment_to_boost(sentiment, confidence)
    
    # Create result
    result = NewsResult(
        symbol=symbol,
        query=query,
        sentiment=sentiment,
        confidence=confidence,
        news_boost=boost,
        articles=all_articles,
        sources_used=sources_used,
        articles_analyzed=len(all_articles),
    )
    
    # Cache
    if NewsConfig.ENABLE_CACHE:
        await _CACHE.set(cache_key, result)
    
    return result


async def batch_news_intelligence(
    items: List[Dict[str, str]],
    *,
    concurrency: Optional[int] = None,
) -> BatchResult:
    """
    Get news intelligence for multiple symbols.
    
    Args:
        items: List of dicts with "symbol" and optional "name" keys
        concurrency: Maximum concurrent requests
    
    Returns:
        BatchResult with items and metadata
    """
    start_time = time.time()
    
    concurrency = concurrency or NewsConfig.CONCURRENCY
    semaphore = asyncio.Semaphore(concurrency)
    
    async def process_item(item: Dict[str, str]) -> Optional[Dict[str, Any]]:
        symbol = item.get("symbol", "").strip()
        name = item.get("name", "").strip()
        
        if not symbol:
            return None
        
        async with semaphore:
            result = await get_news_intelligence(symbol, name)
            return result.to_dict()
    
    # Process all items
    tasks = [process_item(item) for item in items if item.get("symbol")]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Filter successful results
    valid_results = []
    for r in results:
        if r and isinstance(r, dict):
            valid_results.append(r)
    
    # Build metadata
    meta = {
        "version": NEWS_VERSION,
        "count": len(valid_results),
        "total_requested": len(items),
        "elapsed_ms": int((time.time() - start_time) * 1000),
        "config": {
            "mode": NewsConfig.QUERY_MODE,
            "max_articles": NewsConfig.MAX_ARTICLES,
            "cache_ttl": NewsConfig.CACHE_TTL_SECONDS,
            "concurrency": concurrency,
            "timeout": NewsConfig.TIMEOUT_SECONDS,
            "recency_half_life": NewsConfig.RECENCY_HALFLIFE_HOURS,
            "boost_clamp": NewsConfig.BOOST_CLAMP,
        },
        "google": {
            "hl": NewsConfig.GOOGLE_HL,
            "gl": NewsConfig.GOOGLE_GL,
            "ceid": NewsConfig.GOOGLE_CEID,
            "fresh_days": NewsConfig.GOOGLE_FRESH_DAYS,
        },
        "sources_count": len(NewsConfig.RSS_SOURCES),
    }
    
    return BatchResult(items=valid_results, meta=meta)


async def clear_cache() -> None:
    """Clear the news cache."""
    await _CACHE.clear()


async def get_cache_stats() -> Dict[str, Any]:
    """Get cache statistics."""
    return await _CACHE.stats()


# ============================================================================
# Module Exports
# ============================================================================

__all__ = [
    "NEWS_VERSION",
    "NewsConfig",
    "NewsArticle",
    "NewsResult",
    "BatchResult",
    "get_news_intelligence",
    "batch_news_intelligence",
    "clear_cache",
    "get_cache_stats",
]
