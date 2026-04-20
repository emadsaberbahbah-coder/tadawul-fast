#!/usr/bin/env python3
"""
core/news_intelligence.py
================================================================================
Advanced News Intelligence Engine -- v5.1.0
================================================================================
RENDER-SAFE • IMPORT-SAFE • LAZY-ML • RSS/GOOGLE SAFE • CACHE-BACKED • ASYNC-SAFE

v5.1.0 changes (what moved from v5.0.0)
---------------------------------------
- CRITICAL FIX: `import importlib.util` is now explicit. v5.0.0 had only
  `import importlib`, and calling `importlib.util.find_spec(...)` raises
  `AttributeError` in that state (no submodule loaded). The except clause
  silently caught AttributeError → `_module_exists(...)` always returned
  False → every HAS_* flag was False → every optional feature (fuzzy
  dedup, FinBERT, translation, Redis) was silently disabled at import
  time with no error and no log. Proof:
    >>> import importlib; importlib.util
    AttributeError: module 'importlib' has no attribute 'util'

- CRITICAL FIX: `SingleFlight.execute` no longer awaits the shared future
  while holding the dedup lock. v5.0.0 did `async with lock: return await
  self._calls[key]`, which serialized all callers on the lock and could
  deadlock nested SingleFlight calls. v5.1.0 reads the existing future
  under the lock, releases the lock, then awaits.

- FIX: `_translate_if_needed` handles both async (`py_googletrans`) and
  sync (`googletrans`) translators. v5.0.0 unconditionally `await`ed the
  translator result — sync variants raise TypeError, which the outer
  `except` swallowed, so translation appeared enabled but never worked.

- FIX: severe-term handling now pulls the aggregate sentiment the right
  direction. v5.0.0 multiplied the article weight by 0.75 when severe
  terms were present, which makes a negative article count LESS in the
  weighted average (the opposite of a penalty). v5.1.0 (a) folds the
  severe-term contribution directly into the lexicon score so words like
  "scandal", "insolvency", "restatement" that aren't in NEGATIVE_WORDS
  actually move the score, and (b) increases the weight multiplier to
  1.25 for severe articles so they count MORE in the aggregate.

- FIX: `_HTTPX_LOCK` is lazy-initialized rather than constructed at module
  import time. `asyncio.Lock()` is typically fine at module level on
  Python 3.10+, but lazy-init is more robust across loop policies and
  under pytest-asyncio's per-test event loops.

Public API preserved: NEWS_VERSION, NewsConfig, NewsArticle, NewsResult,
BatchResult, SentimentBreakdown, get_news_intelligence,
batch_news_intelligence, clear_cache, get_cache_stats, warmup_cache,
health_check, DeepLearningSentiment, analyze_sentiment_lexicon,
build_query_terms. All endpoint shapes unchanged.
================================================================================
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import importlib.util  # v5.1.0: REQUIRED so importlib.util.find_spec(...) works
import inspect
import json
import logging
import math
import os
import pickle
import random
import re
import threading
import time
import xml.etree.ElementTree as ET
import zlib
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from enum import Enum
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)
from urllib.parse import quote_plus, urlparse

# =============================================================================
# Optional Dependencies (Import-Safe)
# =============================================================================

def _module_exists(name: str) -> bool:
    """Check if a module exists without importing it.

    v5.1.0: relies on the explicit `import importlib.util` above. v5.0.0
    had `import importlib` only; `importlib.util` is a submodule and is
    NOT automatically loaded by importing the parent package, so this
    function returned False for every module.
    """
    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, AttributeError, ValueError):
        return False


HAS_HTTPX = _module_exists("httpx")
HAS_REDIS = _module_exists("redis.asyncio")
HAS_TRANSFORMERS = _module_exists("transformers") and _module_exists("torch")
HAS_NLTK = _module_exists("nltk")
HAS_TRANSLATOR = _module_exists("googletrans") or _module_exists("py_googletrans")
HAS_RAPIDFUZZ = _module_exists("rapidfuzz") or _module_exists("fuzzywuzzy")
HAS_PROMETHEUS = _module_exists("prometheus_client")
HAS_OTEL = _module_exists("opentelemetry")

# =============================================================================
# Logging Setup
# =============================================================================

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# =============================================================================
# Version
# =============================================================================

__version__ = "5.1.0"
NEWS_VERSION = __version__

# =============================================================================
# Configuration
# =============================================================================

@dataclass(frozen=True)
class NewsConfig:
    """Configuration for news intelligence."""
    # Network
    timeout_seconds: float = 10.0
    max_articles: int = 25
    concurrency: int = 10
    allow_network: bool = True
    retry_attempts: int = 2
    retry_delay: float = 0.5

    # Cache
    cache_ttl_seconds: int = 300
    max_cache_items: int = 5000
    cache_compression: bool = True

    # Redis
    enable_redis: bool = False
    redis_url: str = "redis://localhost:6379/0"

    # RSS
    rss_sources: List[str] = field(default_factory=lambda: [
        "https://feeds.reuters.com/reuters/businessNews",
        "https://feeds.bbci.co.uk/news/business/rss.xml",
        "https://www.cnbc.com/id/10001147/device/rss/rss.html",
        "https://finance.yahoo.com/news/rssindex",
        "https://www.marketwatch.com/rss/news",
        "https://www.investopedia.com/feedbuilder/all/feed.xml",
    ])
    query_mode: str = "google+rss"

    # Google News
    google_hl: str = "en"
    google_gl: str = "SA"
    google_ceid: str = "SA:en"
    google_fresh_days: int = 7

    # Scoring
    boost_clamp: float = 8.0
    recency_halflife_hours: float = 36.0
    min_confidence: float = 0.15

    # ML
    enable_deep_learning: bool = False
    dl_model_name: str = "ProsusAI/finbert"
    dl_batch_size: int = 8
    dl_use_gpu: bool = False

    # Feature flags
    enable_cache: bool = True
    enable_deduplication: bool = True
    enable_recency_weighting: bool = True
    enable_relevance_scoring: bool = True
    enable_severe_penalty: bool = True
    enable_topic_classification: bool = True
    enable_source_credibility: bool = True
    enable_translation: bool = False
    enable_fuzzy_matching: bool = True
    enable_entity_recognition: bool = True

    # HTTP
    user_agent: str = "Mozilla/5.0 (compatible; TadawulFastBridge/5.0)"
    accept_language: str = "en-US,en;q=0.9,ar;q=0.8"

    @classmethod
    def from_env(cls) -> "NewsConfig":
        """Load configuration from environment variables."""
        def _env_str(name: str, default: str) -> str:
            v = os.getenv(name)
            return str(v).strip() if v else default

        def _env_int(name: str, default: int) -> int:
            try:
                return int(os.getenv(name, str(default)))
            except Exception:
                return default

        def _env_float(name: str, default: float) -> float:
            try:
                return float(os.getenv(name, str(default)))
            except Exception:
                return default

        def _env_bool(name: str, default: bool) -> bool:
            raw = (os.getenv(name) or "").strip().lower()
            if not raw:
                return default
            return raw in {"1", "true", "yes", "y", "on", "t", "enable", "enabled"}

        def _env_list(name: str, default: List[str]) -> List[str]:
            v = (os.getenv(name) or "").strip()
            if not v:
                return list(default)
            return [p.strip() for p in v.replace(";", ",").split(",") if p.strip()]

        return cls(
            timeout_seconds=max(2.0, min(60.0, _env_float("NEWS_TIMEOUT_SECONDS", 10.0))),
            max_articles=max(5, min(100, _env_int("NEWS_MAX_ARTICLES", 25))),
            concurrency=max(1, min(50, _env_int("NEWS_CONCURRENCY", 10))),
            allow_network=_env_bool("NEWS_ALLOW_NETWORK", True),
            retry_attempts=max(0, min(5, _env_int("NEWS_RETRY_ATTEMPTS", 2))),
            retry_delay=max(0.1, min(5.0, _env_float("NEWS_RETRY_DELAY", 0.5))),
            cache_ttl_seconds=max(0, min(3600, _env_int("NEWS_CACHE_TTL_SECONDS", 300))),
            max_cache_items=max(100, min(20000, _env_int("NEWS_MAX_CACHE_ITEMS", 5000))),
            cache_compression=_env_bool("NEWS_CACHE_COMPRESSION", True),
            enable_redis=_env_bool("NEWS_ENABLE_REDIS", False) and HAS_REDIS,
            redis_url=_env_str("REDIS_URL", "redis://localhost:6379/0"),
            rss_sources=_env_list("NEWS_RSS_SOURCES", []),
            query_mode=_env_str("NEWS_QUERY_MODE", "google+rss").lower(),
            google_hl=_env_str("NEWS_GOOGLE_HL", "en"),
            google_gl=_env_str("NEWS_GOOGLE_GL", "SA"),
            google_ceid=_env_str("NEWS_GOOGLE_CEID", "SA:en"),
            google_fresh_days=max(1, min(30, _env_int("NEWS_GOOGLE_FRESH_DAYS", 7))),
            boost_clamp=max(1.0, min(15.0, _env_float("NEWS_BOOST_CLAMP", 8.0))),
            recency_halflife_hours=max(6.0, min(168.0, _env_float("NEWS_RECENCY_HALFLIFE_HOURS", 36.0))),
            min_confidence=max(0.0, min(1.0, _env_float("NEWS_MIN_CONFIDENCE", 0.15))),
            enable_deep_learning=_env_bool("NEWS_ENABLE_DEEP_LEARNING", False) and HAS_TRANSFORMERS,
            dl_model_name=_env_str("NEWS_DL_MODEL_NAME", "ProsusAI/finbert"),
            dl_batch_size=max(1, min(32, _env_int("NEWS_DL_BATCH_SIZE", 8))),
            dl_use_gpu=_env_bool("NEWS_DL_USE_GPU", False),
            enable_cache=_env_bool("NEWS_ENABLE_CACHE", True),
            enable_deduplication=_env_bool("NEWS_ENABLE_DEDUPLICATION", True),
            enable_recency_weighting=_env_bool("NEWS_ENABLE_RECENCY_WEIGHTING", True),
            enable_relevance_scoring=_env_bool("NEWS_ENABLE_RELEVANCE_SCORING", True),
            enable_severe_penalty=_env_bool("NEWS_ENABLE_SEVERE_PENALTY", True),
            enable_topic_classification=_env_bool("NEWS_ENABLE_TOPIC_CLASSIFICATION", True),
            enable_source_credibility=_env_bool("NEWS_ENABLE_SOURCE_CREDIBILITY", True),
            enable_translation=_env_bool("NEWS_ENABLE_TRANSLATION", False) and HAS_TRANSLATOR,
            enable_fuzzy_matching=_env_bool("NEWS_ENABLE_FUZZY_MATCHING", True) and HAS_RAPIDFUZZ,
            enable_entity_recognition=_env_bool("NEWS_ENABLE_ENTITY_RECOGNITION", True),
            user_agent=_env_str("NEWS_USER_AGENT", "Mozilla/5.0 (compatible; TadawulFastBridge/5.1)"),
            accept_language=_env_str("NEWS_ACCEPT_LANGUAGE", "en-US,en;q=0.9,ar;q=0.8"),
        )


_CONFIG = NewsConfig.from_env()

# =============================================================================
# Custom Exceptions
# =============================================================================

class NewsIntelligenceError(Exception):
    """Base exception for news intelligence."""
    pass


class NewsFetchError(NewsIntelligenceError):
    """Raised when news fetch fails."""
    pass


class SentimentAnalysisError(NewsIntelligenceError):
    """Raised when sentiment analysis fails."""
    pass


# =============================================================================
# Enums
# =============================================================================

class Language(str, Enum):
    """Supported languages."""
    ENGLISH = "en"
    ARABIC = "ar"


class SentimentSource(str, Enum):
    """Sentiment analysis source."""
    LEXICON = "lexicon"
    DEEP_LEARNING = "deep_learning"
    HYBRID = "hybrid"


# =============================================================================
# Constants
# =============================================================================

RIYADH_TZ = timezone(timedelta(hours=3))

# Source credibility weights
SOURCE_CREDIBILITY: Dict[str, float] = {
    "reuters.com": 1.0,
    "bloomberg.com": 1.0,
    "wsj.com": 1.0,
    "ft.com": 0.95,
    "economist.com": 0.95,
    "cnbc.com": 0.90,
    "bbc.com": 0.90,
    "yahoo.com": 0.80,
    "marketwatch.com": 0.80,
    "investopedia.com": 0.70,
    "seekingalpha.com": 0.70,
    "default": 0.60,
}

# Topic keywords
TOPIC_KEYWORDS: Dict[str, List[str]] = {
    "earnings": ["earnings", "profit", "loss", "revenue", "quarter", "guidance"],
    "merger": ["merger", "acquisition", "takeover", "buyout", "merge"],
    "product": ["product", "launch", "release", "announce", "unveil"],
    "regulatory": ["regulator", "sec", "fda", "approval", "investigation", "probe", "fine"],
    "macro": ["economy", "fed", "interest rate", "inflation", "gdp", "employment"],
    "management": ["ceo", "cfo", "executive", "management", "leadership", "resign", "appoint"],
    "analyst": ["analyst", "rating", "upgrade", "downgrade", "target", "outperform"],
    "dividend": ["dividend", "buyback", "repurchase", "payout", "yield"],
}

# Sentiment lexicons
POSITIVE_WORDS = {
    "beat", "beats", "surge", "surges", "soar", "soars", "strong", "record", "growth",
    "profit", "profits", "upgrade", "upgraded", "outperform", "buy", "bullish", "rebound",
    "expansion", "wins", "win", "contract", "contracts", "award", "awarded", "raises",
    "raise", "raised", "higher", "guidance", "buyback", "dividend", "hike", "jump", "jumps",
    "gain", "gains", "rally", "positive", "success", "approval", "approved", "resilient",
    "upside", "undervalued", "improve", "improves", "improved", "support", "stable", "momentum",
}

NEGATIVE_WORDS = {
    "miss", "misses", "plunge", "plunges", "weak", "warning", "downgrade", "downgraded",
    "sell", "bearish", "lawsuit", "probe", "investigation", "fraud", "default", "loss", "losses",
    "cuts", "cut", "layoff", "layoffs", "bankruptcy", "recall", "halt", "sanction", "sanctions",
    "drop", "drops", "fall", "falls", "slide", "slides", "negative", "fail", "failure",
    "fine", "breach", "dilution", "risk", "risks", "volatility", "crisis", "liquidity",
    "overvalued", "uncertainty", "unstable", "volatile", "decline", "slowdown", "recession",
    "underperform", "underperformed", "weaker", "weakening", "deteriorate", "pressure", "challenge",
}

SEVERE_NEGATIVE_WORDS = {
    "fraud", "bankruptcy", "default", "scandal", "sanction", "probe", "investigation",
    "lawsuit", "insolvent", "insolvency", "crisis", "liquidation", "restatement", "misconduct",
}

POSITIVE_PHRASES = {
    "record profit": 2.5,
    "record revenue": 2.2,
    "raises guidance": 2.5,
    "share buyback": 2.0,
    "earnings beat": 2.0,
    "strong buy": 2.2,
    "dividend increase": 1.8,
    "price target raised": 1.8,
    "beats estimates": 2.2,
}

NEGATIVE_PHRASES = {
    "profit warning": -2.5,
    "guidance cut": -2.5,
    "regulatory probe": -2.0,
    "accounting scandal": -3.0,
    "earnings miss": -2.0,
    "net loss": -1.8,
    "bankruptcy warning": -3.0,
    "downgraded to sell": -2.0,
    "price target cut": -1.8,
}

NEGATION_WORDS = {"not", "no", "never", "without", "hardly", "rarely", "neither", "nor", "none"}

INTENSIFIERS_POS = {"strongly", "significantly", "sharply", "surging", "soaring", "record", "massive"}
INTENSIFIERS_NEG = {"sharply", "significantly", "plunging", "crashing", "severe", "critical", "drastic"}

# Regex patterns
HTML_TAG_RE = re.compile(r"<[^>]+>")
CDATA_RE = re.compile(r"<!\[CDATA\[(.*?)\]\]>", re.DOTALL)
WHITESPACE_RE = re.compile(r"\s+")
URL_TRACKING_RE = re.compile(r"[?#&](utm_|fbclid|gclid|ref|source|mc_cid|mc_eid).*$")
TOKEN_RE = re.compile(r"[A-Za-z]+|[0-9]+|[\u0600-\u06FF]+")


# =============================================================================
# Data Classes
# =============================================================================

@dataclass(slots=True)
class NewsArticle:
    """Represents a single news article."""
    title: str
    url: str = ""
    source: str = ""
    source_domain: str = ""
    published_utc: Optional[str] = None
    published_riyadh: Optional[str] = None
    crawled_utc: str = field(default_factory=lambda: _utc_iso())
    snippet: str = ""
    full_text: Optional[str] = None
    sentiment: float = 0.0
    sentiment_raw: float = 0.0
    sentiment_ml: Optional[float] = None
    confidence: float = 0.0
    relevance: float = 1.0
    topics: List[str] = field(default_factory=list)
    entities: List[str] = field(default_factory=list)
    impact_score: float = 0.0
    language: str = Language.ENGLISH.value
    word_count: int = 0
    credibility_weight: float = 1.0
    is_headline: bool = False
    duplicate_of: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NewsArticle":
        """Create from dictionary."""
        return cls(**data)


@dataclass(slots=True)
class SentimentBreakdown:
    """Detailed sentiment breakdown."""
    lexicon_score: float = 0.0
    ml_score: Optional[float] = None
    weighted_score: float = 0.0
    positive_hits: int = 0
    negative_hits: int = 0
    neutral_hits: int = 0
    severe_terms: List[str] = field(default_factory=list)
    confidence_factors: Dict[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class NewsResult:
    """Complete news analysis result."""
    symbol: str
    query: str
    sentiment: float
    confidence: float
    news_boost: float
    breakdown: SentimentBreakdown = field(default_factory=SentimentBreakdown)
    articles: List[NewsArticle] = field(default_factory=list)
    sources_used: List[str] = field(default_factory=list)
    articles_analyzed: int = 0
    articles_filtered: int = 0
    sentiment_trend: List[float] = field(default_factory=list)
    article_velocity: float = 0.0
    emerging_topics: List[str] = field(default_factory=list)
    processing_time_ms: float = 0.0
    cached: bool = False
    cache_key: Optional[str] = None
    version: str = NEWS_VERSION

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        d = asdict(self)
        d["sentiment_trend"] = self.sentiment_trend[-10:]
        d["emerging_topics"] = self.emerging_topics[:5]
        d["articles"] = [a.to_dict() for a in self.articles[:5]]
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NewsResult":
        """Create from dictionary."""
        articles = [NewsArticle.from_dict(a) for a in data.get("articles", [])]
        breakdown = SentimentBreakdown(**data.get("breakdown", {}))
        return cls(
            symbol=data["symbol"],
            query=data["query"],
            sentiment=float(data["sentiment"]),
            confidence=float(data["confidence"]),
            news_boost=float(data["news_boost"]),
            breakdown=breakdown,
            articles=articles,
            sources_used=data.get("sources_used", []),
            articles_analyzed=data.get("articles_analyzed", 0),
            articles_filtered=data.get("articles_filtered", 0),
            sentiment_trend=data.get("sentiment_trend", []),
            article_velocity=float(data.get("article_velocity", 0.0)),
            emerging_topics=data.get("emerging_topics", []),
            processing_time_ms=float(data.get("processing_time_ms", 0.0)),
            cached=bool(data.get("cached", False)),
            cache_key=data.get("cache_key"),
            version=data.get("version", NEWS_VERSION),
        )


@dataclass(slots=True)
class BatchResult:
    """Batch processing result."""
    items: List[Dict[str, Any]] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    errors: List[Dict[str, str]] = field(default_factory=list)


# =============================================================================
# Pure Utility Functions
# =============================================================================

def _utc_now() -> datetime:
    """Get current UTC time."""
    return datetime.now(timezone.utc)


def _utc_iso(dt: Optional[datetime] = None) -> str:
    """Get UTC time in ISO format."""
    dt = dt or _utc_now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _riyadh_now() -> datetime:
    """Get current Riyadh time."""
    return datetime.now(RIYADH_TZ)


def _to_riyadh_iso(dt: Optional[datetime]) -> Optional[str]:
    """Convert datetime to Riyadh ISO format."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(RIYADH_TZ).isoformat()


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse datetime from string."""
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None

    # Try RFC 2822
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # Try ISO 8601
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


def _age_hours(dt: Optional[datetime]) -> Optional[float]:
    """Calculate age in hours."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (_utc_now() - dt).total_seconds() / 3600.0)


def _recency_weight(hours_old: Optional[float], half_life: float = _CONFIG.recency_halflife_hours) -> float:
    """Calculate recency weight."""
    if hours_old is None:
        return 0.6
    return float(0.5 ** (hours_old / max(1.0, half_life)))


def _strip_html(text: str) -> str:
    """Remove HTML tags from text."""
    if not text:
        return ""
    text = CDATA_RE.sub(r"\1", text)
    text = HTML_TAG_RE.sub(" ", text)
    return WHITESPACE_RE.sub(" ", text).strip()


def _normalize_text(text: str, remove_special: bool = True) -> str:
    """Normalize text for analysis."""
    if not text:
        return ""
    text = WHITESPACE_RE.sub(" ", text.lower().strip())
    if remove_special:
        text = re.sub(r"[^\w\s\u0600-\u06FF]", "", text)
    return text


def _canonical_url(url: str) -> str:
    """Remove tracking parameters from URL."""
    url = (url or "").strip()
    if not url:
        return ""
    return URL_TRACKING_RE.sub("", url)


def _extract_domain(url: str) -> str:
    """Extract domain from URL."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split("/")[0]
        return domain.replace("www.", "").split(":")[0].lower()
    except Exception:
        return ""


def _get_credibility_weight(domain: str) -> float:
    """Get credibility weight for domain."""
    if not _CONFIG.enable_source_credibility:
        return 1.0
    domain = (domain or "").lower()
    for key, weight in SOURCE_CREDIBILITY.items():
        if key != "default" and key in domain:
            return weight
    return SOURCE_CREDIBILITY.get("default", 0.6)


def _detect_language(text: str) -> str:
    """Detect language of text."""
    return Language.ARABIC.value if text and re.search(r"[\u0600-\u06FF]", text) else Language.ENGLISH.value


def _tokenize(text: str) -> List[str]:
    """Tokenize text into words."""
    return TOKEN_RE.findall(_normalize_text(text))


def _symbol_aliases(symbol: str) -> List[str]:
    """Generate aliases for symbol."""
    s = (symbol or "").strip().upper()
    aliases = [s]
    if s.endswith(".SR"):
        aliases.append(s[:-3])
    elif s.isdigit() and 3 <= len(s) <= 6:
        aliases.append(f"{s}.SR")
    return [x for x in aliases if x]


def build_query_terms(symbol: str, company_name: str = "") -> List[str]:
    """Build query terms for news search."""
    terms = _symbol_aliases(symbol)
    if company_name:
        terms.append(company_name.strip())
    return list(dict.fromkeys([t for t in terms if t]))


def _build_google_news_url(query: str) -> str:
    """Build Google News RSS URL."""
    encoded = quote_plus(query)
    return (
        f"https://news.google.com/rss/search?q={encoded}"
        f"&hl={_CONFIG.google_hl}&gl={_CONFIG.google_gl}&ceid={_CONFIG.google_ceid}"
    )


# =============================================================================
# Sentiment Analysis
# =============================================================================

# Per-severe-term score contribution (applied in addition to any token match).
# This ensures severe terms like "scandal", "insolvency", "restatement" that
# aren't in NEGATIVE_WORDS still move the score; terms that ARE also in
# NEGATIVE_WORDS (fraud, bankruptcy, default, lawsuit, probe, investigation,
# sanction) effectively carry more weight than ordinary negatives.
_SEVERE_TERM_SCORE = -1.5


def analyze_sentiment_lexicon(text: str) -> Tuple[float, float, int, int, int, List[str]]:
    """
    Analyze sentiment using lexicon-based approach.

    Returns:
        Tuple of (sentiment, confidence, positive_hits, negative_hits, neutral_hits, severe_terms)
    """
    if not text:
        return 0.0, 0.0, 0, 0, 0, []

    normalized = _normalize_text(text)
    if not normalized:
        return 0.0, 0.0, 0, 0, 0, []

    score = 0.0
    pos_hits = 0
    neg_hits = 0
    neutral_hits = 0
    severe_terms: List[str] = []

    # Check phrases first
    for phrase, weight in POSITIVE_PHRASES.items():
        if phrase in normalized:
            score += weight
            pos_hits += 2
    for phrase, weight in NEGATIVE_PHRASES.items():
        if phrase in normalized:
            score += weight
            neg_hits += 2

    # Check severe terms — v5.1.0: these now contribute to the score directly.
    # v5.0.0 only tracked them in a reporting list; terms like "scandal" that
    # aren't in NEGATIVE_WORDS had no effect on the sentiment score at all.
    for term in SEVERE_NEGATIVE_WORDS:
        if term in normalized:
            severe_terms.append(term)
            score += _SEVERE_TERM_SCORE
            neg_hits += 1

    # Token analysis
    tokens = _tokenize(normalized)
    if not tokens:
        return score, _CONFIG.min_confidence, pos_hits, neg_hits, neutral_hits, severe_terms

    flip_window = 0
    for i, token in enumerate(tokens):
        if token in NEGATION_WORDS:
            flip_window = 3
            continue

        word_score = 0.0
        if token in POSITIVE_WORDS:
            word_score = 1.0
            pos_hits += 1
        elif token in NEGATIVE_WORDS:
            word_score = -1.0
            neg_hits += 1
        else:
            neutral_hits += 1

        # Apply intensifiers
        if i > 0 and tokens[i - 1] in INTENSIFIERS_POS and word_score > 0:
            word_score *= 1.3
        elif i > 0 and tokens[i - 1] in INTENSIFIERS_NEG and word_score < 0:
            word_score *= 1.3

        # Apply negation
        if flip_window > 0 and word_score != 0:
            word_score = -word_score
            flip_window -= 1

        score += word_score

    total_hits = pos_hits + neg_hits
    if total_hits == 0:
        return 0.0, _CONFIG.min_confidence, 0, 0, neutral_hits, severe_terms

    sentiment = max(-1.0, min(1.0, score / max(5.0, float(total_hits))))
    confidence = max(_CONFIG.min_confidence, min(1.0, total_hits / 15.0))

    return sentiment, confidence, pos_hits, neg_hits, neutral_hits, severe_terms


# =============================================================================
# Deep Learning Sentiment (Lazy Loading)
# =============================================================================

class DeepLearningSentiment:
    """Deep learning sentiment analyzer using transformers."""

    _instance: Optional["DeepLearningSentiment"] = None
    _lock: Optional[asyncio.Lock] = None

    def __init__(self) -> None:
        self.model = None
        self.tokenizer = None
        self.pipeline = None
        self.device = "cpu"
        self.initialized = False
        self.model_name = _CONFIG.dl_model_name

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    @classmethod
    async def get_instance(cls) -> "DeepLearningSentiment":
        """Get singleton instance."""
        if cls._instance is None:
            async with cls._get_lock():
                if cls._instance is None:
                    inst = cls()
                    await inst.initialize()
                    cls._instance = inst
        return cls._instance

    async def initialize(self) -> None:
        """Initialize the model (lazy loading)."""
        if not _CONFIG.enable_deep_learning or not HAS_TRANSFORMERS:
            self.initialized = False
            return

        loop = asyncio.get_running_loop()

        def _load() -> Tuple[Any, Any, Any, str]:
            import torch
            from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline

            use_gpu = bool(_CONFIG.dl_use_gpu and torch.cuda.is_available())
            device = "cuda" if use_gpu else "cpu"
            tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
            if use_gpu:
                model = model.cuda()
            pipe = pipeline(
                "sentiment-analysis",
                model=model,
                tokenizer=tokenizer,
                device=0 if use_gpu else -1,
                return_all_scores=True,
            )
            return tokenizer, model, pipe, device

        try:
            self.tokenizer, self.model, self.pipeline, self.device = await loop.run_in_executor(None, _load)
            self.initialized = True
        except Exception as e:
            logger.warning("DeepLearningSentiment initialization skipped: %s", e)
            self.initialized = False
            self.tokenizer = None
            self.model = None
            self.pipeline = None
            self.device = "cpu"

    async def analyze(self, texts: Sequence[str]) -> List[Optional[float]]:
        """Analyze sentiment of texts using deep learning."""
        if not self.initialized or not self.pipeline or not texts:
            return [None for _ in texts]

        loop = asyncio.get_running_loop()

        def _run(batch: List[str]) -> List[Optional[float]]:
            results = self.pipeline(batch, truncation=True, max_length=256, batch_size=_CONFIG.dl_batch_size)
            scores: List[Optional[float]] = []
            for item in results:
                pos = None
                neg = None
                if isinstance(item, list):
                    for row in item:
                        label = str(row.get("label", "")).lower()
                        score = float(row.get("score", 0.0))
                        if "pos" in label:
                            pos = score
                        elif "neg" in label:
                            neg = score
                if pos is None and neg is None:
                    scores.append(None)
                else:
                    p = pos or 0.0
                    n = neg or 0.0
                    scores.append(max(-1.0, min(1.0, p - n)))
            return scores

        try:
            return await loop.run_in_executor(None, _run, list(texts))
        except Exception as e:
            logger.warning("DeepLearningSentiment analyze failed: %s", e)
            return [None for _ in texts]


# =============================================================================
# Retry and Backoff
# =============================================================================

class FullJitterBackoff:
    """Full jitter backoff for retries."""

    def __init__(self, max_retries: int = 3, base_delay: float = 0.5, max_delay: float = 8.0):
        self.max_retries = max(1, max_retries)
        self.base_delay = max(0.05, base_delay)
        self.max_delay = max(self.base_delay, max_delay)

    async def execute_async(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute function with retries."""
        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                result = fn(*args, **kwargs)
                if inspect.isawaitable(result):
                    result = await result
                return result
            except Exception as e:
                last_exc = e
                if attempt >= self.max_retries - 1:
                    raise
                cap = min(self.max_delay, self.base_delay * (2 ** attempt))
                await asyncio.sleep(random.uniform(0.0, cap))
        raise last_exc  # type: ignore


_BACKOFF = FullJitterBackoff(
    max_retries=_CONFIG.retry_attempts + 1,
    base_delay=_CONFIG.retry_delay,
)


# =============================================================================
# Lazy Imports
# =============================================================================

class _LazyImports:
    """Lazy imports for optional dependencies."""

    _httpx = None
    _redis = None
    _translator_cls = None
    _fuzz = None

    @classmethod
    def httpx(cls):
        """Get httpx module."""
        if cls._httpx is None and HAS_HTTPX:
            try:
                import httpx  # type: ignore
                cls._httpx = httpx
            except Exception:
                cls._httpx = False
        return None if cls._httpx is False else cls._httpx

    @classmethod
    def redis(cls):
        """Get redis module."""
        if cls._redis is None and HAS_REDIS:
            try:
                from redis.asyncio import Redis  # type: ignore
                cls._redis = Redis
            except Exception:
                cls._redis = False
        return None if cls._redis is False else cls._redis

    @classmethod
    def translator_cls(cls):
        """Get translator class."""
        if cls._translator_cls is None and HAS_TRANSLATOR:
            try:
                from googletrans import Translator  # type: ignore
                cls._translator_cls = Translator
            except Exception:
                cls._translator_cls = False
        return None if cls._translator_cls is False else cls._translator_cls

    @classmethod
    def fuzz_partial_ratio(cls) -> Optional[Callable[[str, str], int]]:
        """Get fuzzy matching function."""
        if cls._fuzz is None and HAS_RAPIDFUZZ:
            try:
                from rapidfuzz import fuzz  # type: ignore
                cls._fuzz = fuzz.partial_ratio
            except Exception:
                try:
                    from fuzzywuzzy import fuzz  # type: ignore
                    cls._fuzz = fuzz.partial_ratio
                except Exception:
                    cls._fuzz = False
        return None if cls._fuzz is False else cls._fuzz


# =============================================================================
# HTTP Client
# =============================================================================

_HTTPX_CLIENT = None
# v5.1.0: lazy-init rather than `asyncio.Lock()` at module import time.
# asyncio.Lock() at module scope is usually OK on Python 3.10+ but fragile
# under pytest-asyncio per-test loops and alt event-loop policies (uvloop).
_HTTPX_LOCK: Optional[asyncio.Lock] = None


def _get_httpx_lock() -> asyncio.Lock:
    global _HTTPX_LOCK
    if _HTTPX_LOCK is None:
        _HTTPX_LOCK = asyncio.Lock()
    return _HTTPX_LOCK


async def _get_httpx_client():
    """Get HTTPX client singleton."""
    global _HTTPX_CLIENT
    if _HTTPX_CLIENT is not None:
        return _HTTPX_CLIENT

    async with _get_httpx_lock():
        if _HTTPX_CLIENT is not None:
            return _HTTPX_CLIENT

        httpx = _LazyImports.httpx()
        if httpx is None:
            return None

        _HTTPX_CLIENT = httpx.AsyncClient(
            timeout=_CONFIG.timeout_seconds,
            headers={
                "User-Agent": _CONFIG.user_agent,
                "Accept": "application/rss+xml, application/xml, text/xml, application/json, text/html;q=0.7, */*;q=0.5",
                "Accept-Language": _CONFIG.accept_language,
            },
            follow_redirects=True,
        )
        return _HTTPX_CLIENT


# =============================================================================
# Cache
# =============================================================================

class AdvancedCache:
    """Advanced cache with memory and Redis backends.

    Values are pickled and optionally zlib-compressed. Note: pickle-based
    caches are unsafe when the backing store (Redis) is shared with
    untrusted writers, since unpickling attacker-controlled bytes is
    equivalent to arbitrary code execution. For single-tenant deployments
    this is acceptable; for shared Redis, consider switching _pack/_unpack
    to a JSON codec.
    """

    def __init__(self, max_size: int, ttl_sec: int, use_compression: bool = True):
        self.max_size = max(100, max_size)
        self.ttl_sec = max(0, ttl_sec)
        self.use_compression = use_compression
        self._data: OrderedDict[str, Tuple[bytes, float]] = OrderedDict()
        self._lock = threading.RLock()
        self._redis = None
        self._stats = {"hits": 0, "misses": 0, "sets": 0, "evictions": 0, "size": 0}

    async def _get_redis(self):
        """Get Redis client."""
        if not _CONFIG.enable_redis:
            return None
        if self._redis is not None:
            return self._redis
        RedisCls = _LazyImports.redis()
        if RedisCls is None:
            return None
        self._redis = RedisCls.from_url(_CONFIG.redis_url, decode_responses=False)
        return self._redis

    def _pack(self, value: Any) -> bytes:
        """Serialize and compress value."""
        raw = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        return zlib.compress(raw, level=6) if self.use_compression else raw

    def _unpack(self, payload: bytes) -> Any:
        """Decompress and deserialize value."""
        blob = zlib.decompress(payload) if self.use_compression else payload
        return pickle.loads(blob)

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        now = time.time()

        # Check memory cache
        with self._lock:
            item = self._data.get(key)
            if item is not None:
                packed, exp = item
                if exp >= now:
                    self._data.move_to_end(key)
                    self._stats["hits"] += 1
                    self._stats["size"] = len(self._data)
                    return self._unpack(packed)
                self._data.pop(key, None)

        # Check Redis
        redis_cli = await self._get_redis()
        if redis_cli is not None:
            try:
                raw = await redis_cli.get(key)
                if raw is not None:
                    value = self._unpack(raw)
                    with self._lock:
                        self._data[key] = (raw, now + self.ttl_sec)
                        self._data.move_to_end(key)
                        self._stats["hits"] += 1
                        self._stats["size"] = len(self._data)
                    return value
            except Exception:
                pass

        with self._lock:
            self._stats["misses"] += 1
        return None

    async def set(self, key: str, value: Any) -> None:
        """Set value in cache."""
        packed = self._pack(value)
        exp = time.time() + self.ttl_sec

        with self._lock:
            self._data[key] = (packed, exp)
            self._data.move_to_end(key)
            while len(self._data) > self.max_size:
                self._data.popitem(last=False)
                self._stats["evictions"] += 1
            self._stats["sets"] += 1
            self._stats["size"] = len(self._data)

        # Update Redis
        redis_cli = await self._get_redis()
        if redis_cli is not None:
            try:
                await redis_cli.setex(key, self.ttl_sec, packed)
            except Exception:
                pass

    async def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._data.clear()
            self._stats["size"] = 0

    async def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            return dict(self._stats)


_CACHE = AdvancedCache(_CONFIG.max_cache_items, _CONFIG.cache_ttl_seconds, _CONFIG.cache_compression)


# =============================================================================
# Single Flight Request Deduplication
# =============================================================================

class SingleFlight:
    """Deduplicate concurrent requests.

    v5.1.0: the existing future is retrieved under the dedup lock and then
    awaited OUTSIDE the lock. v5.0.0 awaited inside the lock, which (a)
    serialized all callers (even for unrelated keys) on that one lock and
    (b) could deadlock nested SingleFlight.execute calls.
    """

    def __init__(self) -> None:
        self._calls: Dict[str, "asyncio.Future[Any]"] = {}
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def execute(self, key: str, coro_func: Callable[[], Awaitable[Any]]) -> Any:
        """Execute coroutine, deduplicating concurrent calls."""
        lock = self._get_lock()

        existing: Optional["asyncio.Future[Any]"] = None
        future: Optional["asyncio.Future[Any]"] = None

        async with lock:
            if key in self._calls:
                existing = self._calls[key]
            else:
                future = asyncio.get_running_loop().create_future()
                self._calls[key] = future

        # v5.1.0: await OUTSIDE the lock so we don't block unrelated keys.
        if existing is not None:
            return await existing

        assert future is not None  # for type checkers
        try:
            result = await coro_func()
            if not future.done():
                future.set_result(result)
            return result
        except Exception as e:
            if not future.done():
                future.set_exception(e)
            raise
        finally:
            async with lock:
                self._calls.pop(key, None)


_SINGLE_FLIGHT = SingleFlight()


# =============================================================================
# Translation Helper
# =============================================================================

async def _translate_if_needed(text: str, language: str) -> str:
    """Translate text to English if needed.

    v5.1.0: handles both sync (`googletrans`) and async (`py_googletrans`)
    translator implementations. v5.0.0 always `await`ed the result, which
    raises TypeError on sync translators — silently swallowed by the outer
    try/except, so translation never actually worked.
    """
    if not text or not language or language == Language.ENGLISH.value or not _CONFIG.enable_translation:
        return text

    Translator = _LazyImports.translator_cls()
    if Translator is None:
        return text

    try:
        translator = Translator()
        maybe_result = translator.translate(text, dest="en")
        if inspect.isawaitable(maybe_result):
            result = await maybe_result
        else:
            # Sync translator (most googletrans versions). Run it in a
            # thread executor since google's HTTP client is blocking.
            if callable(getattr(translator, "translate", None)):
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    None, lambda: translator.translate(text, dest="en")
                )
            else:
                result = maybe_result
        return getattr(result, "text", text) or text
    except Exception:
        return text


# =============================================================================
# News Fetching
# =============================================================================

async def _fetch_text(url: str) -> str:
    """Fetch text from URL."""
    if not _CONFIG.allow_network:
        return ""
    client = await _get_httpx_client()
    if client is None:
        return ""
    response = await client.get(url)
    response.raise_for_status()
    return response.text or ""


def _parse_rss(xml_text: str, source_hint: str) -> List[NewsArticle]:
    """Parse RSS feed XML."""
    if not xml_text.strip():
        return []

    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return []

    articles: List[NewsArticle] = []
    for item in root.findall(".//item"):
        title = _strip_html(item.findtext("title") or "")
        if not title:
            continue

        link = _canonical_url(_strip_html(item.findtext("link") or ""))
        description = _strip_html(item.findtext("description") or "")
        pub_raw = _strip_html(item.findtext("pubDate") or item.findtext("published") or "")
        pub_dt = _parse_datetime(pub_raw)
        source = _strip_html(item.findtext("source") or source_hint or "") or source_hint
        domain = _extract_domain(link) or _extract_domain(source_hint)

        article = NewsArticle(
            title=title,
            url=link,
            source=source or domain,
            source_domain=domain,
            published_utc=_utc_iso(pub_dt) if pub_dt else None,
            published_riyadh=_to_riyadh_iso(pub_dt),
            snippet=description,
            language=_detect_language(f"{title} {description}"),
            word_count=len(_tokenize(f"{title} {description}")),
            credibility_weight=_get_credibility_weight(domain),
            is_headline=True,
        )
        articles.append(article)

    return articles


def _deduplicate_articles(articles: Sequence[NewsArticle]) -> List[NewsArticle]:
    """Deduplicate articles by URL and title similarity."""
    if not _CONFIG.enable_deduplication:
        return list(articles)

    result: List[NewsArticle] = []
    seen_urls: set = set()
    seen_titles: set = set()
    partial_ratio = _LazyImports.fuzz_partial_ratio()

    for article in articles:
        url = _canonical_url(article.url)
        title_key = _normalize_text(article.title)

        if url and url in seen_urls:
            continue
        if title_key in seen_titles:
            continue

        # Check similarity with existing titles
        if partial_ratio is not None:
            is_duplicate = False
            for prev in seen_titles:
                try:
                    if partial_ratio(title_key, prev) >= 95:
                        is_duplicate = True
                        break
                except Exception:
                    pass
            if is_duplicate:
                continue

        if url:
            seen_urls.add(url)
        if title_key:
            seen_titles.add(title_key)
        result.append(article)

    return result


def _article_topics(text: str) -> List[str]:
    """Extract topics from text."""
    if not _CONFIG.enable_topic_classification:
        return []

    t = _normalize_text(text)
    topics: List[str] = []
    for topic, keywords in TOPIC_KEYWORDS.items():
        if any(kw in t for kw in keywords):
            topics.append(topic)
    return topics[:5]


def _article_entities(text: str, symbol: str, company_name: str = "") -> List[str]:
    """Extract entities from text."""
    if not _CONFIG.enable_entity_recognition:
        return []

    entities: List[str] = []
    lower_text = text.lower()
    for candidate in build_query_terms(symbol, company_name):
        if candidate and candidate.lower() in lower_text:
            entities.append(candidate)
    return list(dict.fromkeys(entities))[:5]


def _compute_relevance(article: NewsArticle, symbol: str, company_name: str = "") -> float:
    """Compute relevance score for article."""
    if not _CONFIG.enable_relevance_scoring:
        return 1.0

    hay = f"{article.title} {article.snippet}".lower()
    score = 0.0
    for term in build_query_terms(symbol, company_name):
        low = term.lower()
        if not low:
            continue
        if low in hay:
            score += 1.0 if len(low) > 3 else 0.5
    return max(0.2, min(2.0, score if score > 0 else 0.6))


def _impact_score(sentiment: float, relevance: float, credibility: float, hours_old: Optional[float]) -> float:
    """Compute impact score for article."""
    recency = _recency_weight(hours_old) if _CONFIG.enable_recency_weighting else 1.0
    return max(-1.0, min(1.0, sentiment * relevance * credibility * recency * 1.25))


def _news_boost_from_sentiment(sentiment: float, confidence: float, article_count: int) -> float:
    """Compute news boost from sentiment."""
    if article_count <= 0:
        return 0.0
    strength = min(1.0, article_count / max(1.0, float(_CONFIG.max_articles)))
    boost = sentiment * confidence * (0.35 + 0.65 * strength)
    return max(-_CONFIG.boost_clamp, min(_CONFIG.boost_clamp, boost * _CONFIG.boost_clamp))


def _cache_key(symbol: str, company_name: str, include_articles: bool, max_articles: int) -> str:
    """Generate cache key."""
    raw = f"{symbol}|{company_name}|{int(include_articles)}|{max_articles}|{_CONFIG.query_mode}|{NEWS_VERSION}"
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"news:{symbol}:{h[:24]}"


async def _fetch_source_articles(symbol: str, company_name: str, source_url: str) -> List[NewsArticle]:
    """Fetch articles from a single source."""
    try:
        xml_text = await _BACKOFF.execute_async(_fetch_text, source_url)
    except Exception as e:
        logger.debug("News source fetch failed for %s: %s", source_url, e)
        return []

    source_hint = _extract_domain(source_url)
    articles = _parse_rss(xml_text, source_hint)

    # Filter by relevance
    result: List[NewsArticle] = []
    query_terms = build_query_terms(symbol, company_name)
    for article in articles:
        hay = f"{article.title} {article.snippet}".lower()
        if any(term.lower() in hay for term in query_terms):
            result.append(article)

    return result


async def _collect_articles(symbol: str, company_name: str, max_articles: int) -> Tuple[List[NewsArticle], List[str]]:
    """Collect articles from all sources."""
    sources_used: List[str] = []
    articles: List[NewsArticle] = []

    urls: List[Tuple[str, str]] = []

    # Add Google News
    if "google" in _CONFIG.query_mode:
        query = " OR ".join(build_query_terms(symbol, company_name))
        urls.append(("google_news", _build_google_news_url(query)))

    # Add RSS sources
    if "rss" in _CONFIG.query_mode:
        for url in _CONFIG.rss_sources:
            urls.append((_extract_domain(url) or "rss", url))

    semaphore = asyncio.Semaphore(_CONFIG.concurrency)

    async def _fetch_one(label: str, url: str) -> None:
        async with semaphore:
            try:
                found = await _fetch_source_articles(symbol, company_name, url)
                if found:
                    sources_used.append(label)
                    articles.extend(found)
            except Exception as e:
                logger.debug("News collection failed for %s: %s", label, e)

    await asyncio.gather(*[_fetch_one(label, url) for label, url in urls], return_exceptions=True)

    # Deduplicate and sort
    deduped = _deduplicate_articles(articles)
    deduped.sort(
        key=lambda a: _parse_datetime(a.published_utc) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    return deduped[:max_articles], list(dict.fromkeys(sources_used))


async def _score_articles(
    symbol: str,
    company_name: str,
    articles: List[NewsArticle],
) -> Tuple[List[NewsArticle], SentimentBreakdown, List[float]]:
    """Score articles with sentiment analysis.

    v5.1.0: severe articles now get a weight multiplier of 1.25 (more
    influence in the weighted average), not 0.75 (less influence). The
    v5.0.0 value pulled the aggregate AWAY from the severe article's
    sign, which is the opposite of a penalty.
    """
    if not articles:
        return [], SentimentBreakdown(), []

    # Prepare texts for analysis
    texts: List[str] = []
    for article in articles:
        base_text = f"{article.title}. {article.snippet}".strip()
        if article.language != Language.ENGLISH.value:
            base_text = await _translate_if_needed(base_text, article.language)
        texts.append(base_text)

    # Deep learning scores
    ml_scores: List[Optional[float]] = [None for _ in texts]
    if _CONFIG.enable_deep_learning:
        try:
            dl = await DeepLearningSentiment.get_instance()
            ml_scores = await dl.analyze(texts)
        except Exception as e:
            logger.debug("Deep learning scoring skipped: %s", e)

    # Score each article
    trend: List[float] = []
    severe_terms_all: List[str] = []
    pos_total = 0
    neg_total = 0
    neutral_total = 0
    weighted_sum = 0.0
    weight_total = 0.0
    lexicon_sum = 0.0
    ml_count = 0
    ml_sum = 0.0

    for idx, article in enumerate(articles):
        text = texts[idx]
        lex_score, lex_conf, pos_hits, neg_hits, neutral_hits, severe_terms = analyze_sentiment_lexicon(text)
        severe_terms_all.extend(severe_terms)
        pos_total += pos_hits
        neg_total += neg_hits
        neutral_total += neutral_hits
        lexicon_sum += lex_score

        ml_score = ml_scores[idx]
        if ml_score is not None:
            ml_count += 1
            ml_sum += ml_score

        combined = (0.65 * lex_score + 0.35 * ml_score) if ml_score is not None else lex_score
        article.sentiment_raw = lex_score
        article.sentiment_ml = ml_score
        article.topics = _article_topics(text)
        article.entities = _article_entities(text, symbol, company_name)
        article.relevance = _compute_relevance(article, symbol, company_name)

        published_dt = _parse_datetime(article.published_utc)
        hours_old = _age_hours(published_dt)
        article.impact_score = _impact_score(combined, article.relevance, article.credibility_weight, hours_old)
        article.sentiment = combined
        article.confidence = max(_CONFIG.min_confidence, min(1.0, lex_conf * article.credibility_weight))
        trend.append(combined)

        # Weighted scoring.
        # v5.1.0: severe articles get 1.25x weight (amplify their influence
        # in the weighted aggregate). v5.0.0 used 0.75 which REDUCED their
        # influence — that dilutes negative signals, not penalizes them.
        recency = _recency_weight(hours_old) if _CONFIG.enable_recency_weighting else 1.0
        severe_multiplier = 1.25 if _CONFIG.enable_severe_penalty and severe_terms else 1.0
        weight = max(0.05, article.relevance * article.credibility_weight * recency * severe_multiplier)
        weighted_sum += combined * weight
        weight_total += weight

    # Compute overall scores
    weighted_score = weighted_sum / weight_total if weight_total > 0 else 0.0
    breakdown = SentimentBreakdown(
        lexicon_score=(lexicon_sum / len(articles)) if articles else 0.0,
        ml_score=(ml_sum / ml_count) if ml_count else None,
        weighted_score=weighted_score,
        positive_hits=pos_total,
        negative_hits=neg_total,
        neutral_hits=neutral_total,
        severe_terms=list(dict.fromkeys(severe_terms_all))[:10],
        confidence_factors={
            "article_count": min(1.0, len(articles) / max(1.0, float(_CONFIG.max_articles))),
            "source_diversity": min(1.0, len({a.source_domain for a in articles if a.source_domain}) / 5.0),
            "weight_total": min(1.0, weight_total / 10.0),
        },
    )

    return articles, breakdown, trend[-10:]


async def _compute_news_result(
    symbol: str,
    company_name: str = "",
    include_articles: bool = True,
    max_articles: Optional[int] = None,
) -> NewsResult:
    """Compute news result for a symbol."""
    started = time.perf_counter()
    max_articles = max_articles or _CONFIG.max_articles
    query = " OR ".join(build_query_terms(symbol, company_name)) or symbol
    cache_key = _cache_key(symbol, company_name, include_articles, max_articles)

    # Check cache
    if _CONFIG.enable_cache:
        cached = await _CACHE.get(cache_key)
        if isinstance(cached, dict):
            try:
                result = NewsResult.from_dict(cached)
                result.cached = True
                result.cache_key = cache_key
                return result
            except Exception:
                pass

    # Collect and score articles
    articles, sources_used = await _collect_articles(symbol, company_name, max_articles=max_articles)
    scored_articles, breakdown, trend = await _score_articles(symbol, company_name, articles)

    # Compute final metrics
    weighted_score = breakdown.weighted_score
    confidence = max(
        _CONFIG.min_confidence,
        min(
            1.0,
            0.35 * breakdown.confidence_factors.get("article_count", 0.0)
            + 0.25 * breakdown.confidence_factors.get("source_diversity", 0.0)
            + 0.25 * breakdown.confidence_factors.get("weight_total", 0.0)
            + 0.15 * (1.0 if scored_articles else 0.0),
        ),
    )
    news_boost = _news_boost_from_sentiment(weighted_score, confidence, len(scored_articles))
    article_velocity = (
        len([a for a in scored_articles if (_age_hours(_parse_datetime(a.published_utc)) or 9999) <= 24.0])
        / 24.0
    )

    # Emerging topics
    topic_counts: Dict[str, int] = {}
    for a in scored_articles:
        for t in a.topics:
            topic_counts[t] = topic_counts.get(t, 0) + 1
    emerging_topics = [k for k, _v in sorted(topic_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:5]]

    result = NewsResult(
        symbol=symbol,
        query=query,
        sentiment=weighted_score,
        confidence=confidence,
        news_boost=news_boost,
        breakdown=breakdown,
        articles=scored_articles if include_articles else [],
        sources_used=sources_used,
        articles_analyzed=len(scored_articles),
        articles_filtered=max(0, len(articles) - len(scored_articles)),
        sentiment_trend=trend,
        article_velocity=article_velocity,
        emerging_topics=emerging_topics,
        processing_time_ms=(time.perf_counter() - started) * 1000.0,
        cached=False,
        cache_key=cache_key,
    )

    # Cache result
    if _CONFIG.enable_cache:
        try:
            await _CACHE.set(cache_key, result.to_dict())
        except Exception:
            pass

    return result


# =============================================================================
# Public API Functions
# =============================================================================

async def get_news_intelligence(
    payload: Union[str, Dict[str, Any]],
    include_articles: bool = True,
    max_articles: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Get news intelligence for a symbol.

    Args:
        payload: Symbol string or dict with symbol and optional company_name
        include_articles: Whether to include full articles in response
        max_articles: Maximum number of articles to return

    Returns:
        NewsResult as dictionary
    """
    # Parse input
    if isinstance(payload, str):
        symbol = payload.strip().upper()
        company_name = ""
    elif isinstance(payload, dict):
        symbol = str(payload.get("symbol") or payload.get("ticker") or payload.get("code") or "").strip().upper()
        company_name = str(payload.get("name") or payload.get("company_name") or "").strip()
    else:
        symbol = ""
        company_name = ""

    if not symbol:
        return NewsResult(
            symbol="",
            query="",
            sentiment=0.0,
            confidence=0.0,
            news_boost=0.0,
            processing_time_ms=0.0,
        ).to_dict()

    started = time.perf_counter()

    try:
        result = await _SINGLE_FLIGHT.execute(
            f"news:{symbol}:{company_name}:{int(include_articles)}:{max_articles or _CONFIG.max_articles}",
            lambda: _compute_news_result(
                symbol, company_name, include_articles=include_articles, max_articles=max_articles
            ),
        )
        return result.to_dict()
    except Exception as e:
        logger.exception("get_news_intelligence failed for %s: %s", symbol, e)
        fallback = NewsResult(
            symbol=symbol,
            query=symbol,
            sentiment=0.0,
            confidence=0.0,
            news_boost=0.0,
            processing_time_ms=(time.perf_counter() - started) * 1000.0,
        )
        return fallback.to_dict()


async def batch_news_intelligence(
    items: List[Dict[str, Any]],
    include_articles: bool = False,
) -> BatchResult:
    """
    Get news intelligence for multiple symbols in batch.

    Args:
        items: List of dicts with symbol and optional name
        include_articles: Whether to include full articles

    Returns:
        BatchResult with items, meta, and errors
    """
    started = time.perf_counter()
    valid_results: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []
    semaphore = asyncio.Semaphore(_CONFIG.concurrency)

    async def _process_one(item: Dict[str, Any]) -> None:
        async with semaphore:
            symbol = str(item.get("symbol") or item.get("ticker") or item.get("code") or "").strip().upper()
            if not symbol:
                errors.append({"symbol": "", "error": "missing_symbol"})
                return
            try:
                result = await get_news_intelligence(item, include_articles=include_articles)
                valid_results.append(result)
            except Exception as e:
                errors.append({"symbol": symbol, "error": str(e)})

    await asyncio.gather(*[_process_one(item) for item in items], return_exceptions=True)

    meta = {
        "status": "success" if not errors else ("partial" if valid_results else "error"),
        "version": NEWS_VERSION,
        "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
        "requested": len(items),
        "returned": len(valid_results),
        "errors": len(errors),
        "config": {
            "mode": _CONFIG.query_mode,
            "max_articles": _CONFIG.max_articles,
            "cache_ttl": _CONFIG.cache_ttl_seconds,
            "concurrency": _CONFIG.concurrency,
            "timeout": _CONFIG.timeout_seconds,
            "recency_half_life": _CONFIG.recency_halflife_hours,
            "boost_clamp": _CONFIG.boost_clamp,
            "deep_learning": _CONFIG.enable_deep_learning,
        },
        "google": {
            "hl": _CONFIG.google_hl,
            "gl": _CONFIG.google_gl,
            "ceid": _CONFIG.google_ceid,
            "fresh_days": _CONFIG.google_fresh_days,
        },
        "sources_count": len(_CONFIG.rss_sources),
        "timestamp_utc": _utc_iso(),
        "timestamp_riyadh": _to_riyadh_iso(_utc_now()),
    }

    return BatchResult(items=valid_results, meta=meta, errors=errors)


async def clear_cache() -> None:
    """Clear the news cache."""
    await _CACHE.clear()
    logger.info("News cache cleared")


async def get_cache_stats() -> Dict[str, Any]:
    """Get cache statistics."""
    return await _CACHE.stats()


async def warmup_cache(symbols: List[str]) -> Dict[str, Any]:
    """Warm up cache for symbols."""
    result = await batch_news_intelligence([{"symbol": s} for s in symbols], include_articles=False)
    return {
        "warmed": len(result.items),
        "elapsed_ms": result.meta.get("elapsed_ms", 0),
        "cache_stats": await get_cache_stats(),
    }


async def health_check() -> Dict[str, Any]:
    """Health check for news intelligence."""
    health: Dict[str, Any] = {
        "status": "healthy",
        "version": NEWS_VERSION,
        "timestamp_utc": _utc_iso(),
        "timestamp_riyadh": _to_riyadh_iso(_utc_now()),
        "config": {
            "mode": _CONFIG.query_mode,
            "max_articles": _CONFIG.max_articles,
            "cache_ttl": _CONFIG.cache_ttl_seconds,
            "concurrency": _CONFIG.concurrency,
            "deep_learning": _CONFIG.enable_deep_learning,
            "redis": _CONFIG.enable_redis,
            "allow_network": _CONFIG.allow_network,
        },
        "dependencies": {
            "httpx": HAS_HTTPX,
            "elementtree": True,
            "transformers": HAS_TRANSFORMERS,
            "nltk": HAS_NLTK,
            "rapidfuzz": HAS_RAPIDFUZZ,
            "redis": HAS_REDIS and _CONFIG.enable_redis,
        },
    }

    try:
        health["cache"] = await get_cache_stats()
    except Exception as e:
        health["cache"] = {"error": str(e)}

    if _CONFIG.enable_deep_learning:
        try:
            dl = await DeepLearningSentiment.get_instance()
            health["deep_learning"] = {
                "initialized": dl.initialized,
                "model": _CONFIG.dl_model_name,
                "device": dl.device,
            }
        except Exception as e:
            health["deep_learning"] = {"error": str(e)}

    return health


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    "NEWS_VERSION",
    "NewsConfig",
    "NewsArticle",
    "NewsResult",
    "BatchResult",
    "SentimentBreakdown",
    "get_news_intelligence",
    "batch_news_intelligence",
    "clear_cache",
    "get_cache_stats",
    "warmup_cache",
    "health_check",
    "DeepLearningSentiment",
    "analyze_sentiment_lexicon",
    "build_query_terms",
]
