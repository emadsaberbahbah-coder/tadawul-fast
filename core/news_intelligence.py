#!/usr/bin/env python3
"""
core/news_intelligence.py
================================================================================
Advanced News Intelligence Engine — v4.0.0
================================================================================
RENDER-SAFE • IMPORT-SAFE • LAZY-ML • RSS/GOOGLE SAFE • CACHE-BACKED • ASYNC-SAFE

Why this revision
-----------------
- Removes import-time heavy initialization that can hurt Render startup.
- No nltk.download() / transformer model loading / Redis connections at import-time.
- Optional dependencies are discovered lazily and loaded only when used.
- Keeps backward-compatible public APIs:
    get_news_intelligence, batch_news_intelligence, clear_cache, get_cache_stats,
    warmup_cache, health_check, DeepLearningSentiment,
    analyze_sentiment_lexicon, build_query_terms.
- Uses lexicon scoring by default and optional transformer scoring only when
  explicitly enabled and available.

Notes
-----
- This module intentionally prefers reliability over feature sprawl.
- Network fetching is best-effort and fail-soft.
- Sentiment outputs remain normalized to [-1, +1].
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import inspect
import logging
import math
import os
import pickle
import random
import re
import threading
import time
import zlib
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union
from urllib.parse import quote_plus, urlparse
import xml.etree.ElementTree as ET

try:
    from core.utils.compat import json_dumps, json_loads, json_safe
except Exception:
    try:
        import orjson  # type: ignore

        def json_loads(data: Union[str, bytes]) -> Any:
            if isinstance(data, str):
                data = data.encode("utf-8")
            return orjson.loads(data)

        def json_dumps(obj: Any, *, indent: int = 0) -> str:
            opt = orjson.OPT_INDENT_2 if indent else 0
            return orjson.dumps(obj, option=opt, default=str).decode("utf-8")

    except Exception:
        import json

        def json_loads(data: Union[str, bytes]) -> Any:
            if isinstance(data, (bytes, bytearray)):
                data = data.decode("utf-8", errors="replace")
            return json.loads(data)

        def json_dumps(obj: Any, *, indent: int = 0) -> str:
            return json.dumps(obj, indent=indent if indent else None, default=str, ensure_ascii=False)

    def json_safe(obj: Any) -> Any:
        if obj is None or isinstance(obj, (str, int, bool)):
            return obj
        if isinstance(obj, float):
            return None if math.isnan(obj) or math.isinf(obj) else obj
        if isinstance(obj, dict):
            return {str(k): json_safe(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [json_safe(v) for v in obj]
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            try:
                return json_safe(obj.model_dump(mode="python"))
            except Exception:
                pass
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            try:
                return json_safe(obj.dict())
            except Exception:
                pass
        if hasattr(obj, "__dict__"):
            try:
                return json_safe(vars(obj))
            except Exception:
                pass
        return str(obj)

__version__ = "4.0.0"
NEWS_VERSION = __version__

logger = logging.getLogger("core.news_intelligence")
logger.addHandler(logging.NullHandler())

# =============================================================================
# Optional dependency flags (import-safe only)
# =============================================================================

def _module_exists(name: str) -> bool:
    try:
        return importlib.util.find_spec(name) is not None  # type: ignore[attr-defined]
    except Exception:
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
# Metrics / tracing (safe dummy)
# =============================================================================
if HAS_PROMETHEUS:
    try:
        from prometheus_client import Counter, Gauge, Histogram  # type: ignore
    except Exception:
        HAS_PROMETHEUS = False

if HAS_PROMETHEUS:
    news_requests_total = Counter("news_requests_total", "Total news intelligence requests", ["symbol", "source", "status"])
    news_request_duration = Histogram("news_request_duration_seconds", "News request duration", ["symbol", "source"])
    news_cache_hits_total = Counter("news_cache_hits_total", "News cache hits", ["symbol"])
    news_cache_misses_total = Counter("news_cache_misses_total", "News cache misses", ["symbol"])
    news_articles_total = Counter("news_articles_total", "Total news articles processed", ["source"])
    news_sentiment_score = Gauge("news_sentiment_score", "News sentiment score", ["symbol"])
    news_confidence_score = Gauge("news_confidence_score", "News confidence score", ["symbol"])
else:
    class _DummyMetric:
        def labels(self, *args: Any, **kwargs: Any) -> "_DummyMetric":
            return self
        def inc(self, *args: Any, **kwargs: Any) -> None:
            return None
        def observe(self, *args: Any, **kwargs: Any) -> None:
            return None
        def set(self, *args: Any, **kwargs: Any) -> None:
            return None

    news_requests_total = _DummyMetric()
    news_request_duration = _DummyMetric()
    news_cache_hits_total = _DummyMetric()
    news_cache_misses_total = _DummyMetric()
    news_articles_total = _DummyMetric()
    news_sentiment_score = _DummyMetric()
    news_confidence_score = _DummyMetric()

_TRACING_ENABLED = (os.getenv("CORE_TRACING_ENABLED", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
if HAS_OTEL:
    try:
        from opentelemetry import trace  # type: ignore
        from opentelemetry.trace import Status, StatusCode  # type: ignore
        _TRACER = trace.get_tracer(__name__)
    except Exception:
        HAS_OTEL = False
        _TRACER = None
        Status = None  # type: ignore
        StatusCode = None  # type: ignore
else:
    _TRACER = None
    Status = None  # type: ignore
    StatusCode = None  # type: ignore


class TraceContext:
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._cm = None
        self._span = None

    def __enter__(self) -> "TraceContext":
        if HAS_OTEL and _TRACING_ENABLED and _TRACER is not None:
            try:
                self._cm = _TRACER.start_as_current_span(self.name)
                self._span = self._cm.__enter__()
                for k, v in self.attributes.items():
                    try:
                        self._span.set_attribute(str(k), v)
                    except Exception:
                        pass
            except Exception:
                self._cm = None
                self._span = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        try:
            if self._span is not None and exc_val is not None and Status is not None and StatusCode is not None:
                try:
                    self._span.record_exception(exc_val)
                except Exception:
                    pass
                try:
                    self._span.set_status(Status(StatusCode.ERROR, str(exc_val)))
                except Exception:
                    pass
        finally:
            if self._cm is not None:
                try:
                    return bool(self._cm.__exit__(exc_type, exc_val, exc_tb))
                except Exception:
                    return False
        return False

    async def __aenter__(self) -> "TraceContext":
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        return self.__exit__(exc_type, exc_val, exc_tb)


# =============================================================================
# Settings / constants
# =============================================================================
DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_MAX_ARTICLES = 25
DEFAULT_CACHE_TTL_SECONDS = 300
DEFAULT_CONCURRENCY = 10
DEFAULT_MAX_CACHE_ITEMS = 5000
DEFAULT_GOOGLE_HL = "en"
DEFAULT_GOOGLE_GL = "SA"
DEFAULT_GOOGLE_CEID = "SA:en"
DEFAULT_GOOGLE_FRESH_DAYS = 7
DEFAULT_BOOST_CLAMP = 8.0
DEFAULT_RECENCY_HALFLIFE_HOURS = 36.0
DEFAULT_MIN_CONFIDENCE = 0.15

DEFAULT_RSS_SOURCES: List[str] = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://www.cnbc.com/id/10001147/device/rss/rss.html",
    "https://finance.yahoo.com/news/rssindex",
    "https://www.marketwatch.com/rss/news",
    "https://www.investopedia.com/feedbuilder/all/feed.xml",
]

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


# =============================================================================
# Helpers
# =============================================================================
def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return str(v).strip() if v is not None and str(v).strip() else default


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
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
    return raw in {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok"}


def _env_list(name: str, default: List[str]) -> List[str]:
    v = (os.getenv(name) or "").strip()
    if not v:
        return list(default)
    return [p.strip() for p in v.replace(";", ",").split(",") if p.strip()]


class NewsConfig:
    TIMEOUT_SECONDS = max(2.0, min(60.0, _env_float("NEWS_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)))
    MAX_ARTICLES = max(5, min(100, _env_int("NEWS_MAX_ARTICLES", DEFAULT_MAX_ARTICLES)))
    CONCURRENCY = max(1, min(50, _env_int("NEWS_CONCURRENCY", DEFAULT_CONCURRENCY)))
    ALLOW_NETWORK = _env_bool("NEWS_ALLOW_NETWORK", True)
    RETRY_ATTEMPTS = max(0, min(5, _env_int("NEWS_RETRY_ATTEMPTS", 2)))
    RETRY_DELAY = max(0.1, min(5.0, _env_float("NEWS_RETRY_DELAY", 0.5)))

    CACHE_TTL_SECONDS = max(0, min(3600, _env_int("NEWS_CACHE_TTL_SECONDS", DEFAULT_CACHE_TTL_SECONDS)))
    MAX_CACHE_ITEMS = max(100, min(20000, _env_int("NEWS_MAX_CACHE_ITEMS", DEFAULT_MAX_CACHE_ITEMS)))
    CACHE_COMPRESSION = _env_bool("NEWS_CACHE_COMPRESSION", True)

    ENABLE_REDIS = _env_bool("NEWS_ENABLE_REDIS", False) and HAS_REDIS
    REDIS_URL = _env_str("REDIS_URL", "redis://localhost:6379/0")

    RSS_SOURCES = _env_list("NEWS_RSS_SOURCES", DEFAULT_RSS_SOURCES)
    QUERY_MODE = _env_str("NEWS_QUERY_MODE", "google+rss").lower()
    SOURCE_WEIGHTS = SOURCE_CREDIBILITY.copy()

    GOOGLE_HL = _env_str("NEWS_GOOGLE_HL", DEFAULT_GOOGLE_HL)
    GOOGLE_GL = _env_str("NEWS_GOOGLE_GL", DEFAULT_GOOGLE_GL)
    GOOGLE_CEID = _env_str("NEWS_GOOGLE_CEID", DEFAULT_GOOGLE_CEID)
    GOOGLE_FRESH_DAYS = max(1, min(30, _env_int("NEWS_GOOGLE_FRESH_DAYS", DEFAULT_GOOGLE_FRESH_DAYS)))

    BOOST_CLAMP = max(1.0, min(15.0, _env_float("NEWS_BOOST_CLAMP", DEFAULT_BOOST_CLAMP)))
    RECENCY_HALFLIFE_HOURS = max(6.0, min(168.0, _env_float("NEWS_RECENCY_HALFLIFE_HOURS", DEFAULT_RECENCY_HALFLIFE_HOURS)))
    MIN_CONFIDENCE = max(0.0, min(1.0, _env_float("NEWS_MIN_CONFIDENCE", DEFAULT_MIN_CONFIDENCE)))

    ENABLE_DEEP_LEARNING = _env_bool("NEWS_ENABLE_DEEP_LEARNING", False) and HAS_TRANSFORMERS
    DL_MODEL_NAME = _env_str("NEWS_DL_MODEL_NAME", "ProsusAI/finbert")
    DL_BATCH_SIZE = max(1, min(32, _env_int("NEWS_DL_BATCH_SIZE", 8)))
    DL_USE_GPU = _env_bool("NEWS_DL_USE_GPU", False)

    ENABLE_CACHE = _env_bool("NEWS_ENABLE_CACHE", True)
    ENABLE_DEDUPLICATION = _env_bool("NEWS_ENABLE_DEDUPLICATION", True)
    ENABLE_RECENCY_WEIGHTING = _env_bool("NEWS_ENABLE_RECENCY_WEIGHTING", True)
    ENABLE_RELEVANCE_SCORING = _env_bool("NEWS_ENABLE_RELEVANCE_SCORING", True)
    ENABLE_SEVERE_PENALTY = _env_bool("NEWS_ENABLE_SEVERE_PENALTY", True)
    ENABLE_TOPIC_CLASSIFICATION = _env_bool("NEWS_ENABLE_TOPIC_CLASSIFICATION", True)
    ENABLE_SOURCE_CREDIBILITY = _env_bool("NEWS_ENABLE_SOURCE_CREDIBILITY", True)
    ENABLE_TRANSLATION = _env_bool("NEWS_ENABLE_TRANSLATION", False) and HAS_TRANSLATOR
    ENABLE_FUZZY_MATCHING = _env_bool("NEWS_ENABLE_FUZZY_MATCHING", True) and HAS_RAPIDFUZZ
    ENABLE_ENTITY_RECOGNITION = _env_bool("NEWS_ENABLE_ENTITY_RECOGNITION", True)

    USER_AGENT = _env_str("NEWS_USER_AGENT", "Mozilla/5.0 (compatible; TadawulFastBridge/4.0)")
    ACCEPT_LANGUAGE = _env_str("NEWS_ACCEPT_LANGUAGE", "en-US,en;q=0.9,ar;q=0.8")


RIYADH_TZ = timezone(timedelta(hours=3))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(dt: Optional[datetime] = None) -> str:
    dt = dt or utc_now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def riyadh_now() -> datetime:
    return datetime.now(RIYADH_TZ)


def to_riyadh_iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(RIYADH_TZ).isoformat()


def parse_datetime(x: Optional[str]) -> Optional[datetime]:
    if not x:
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
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


def age_hours(dt: Optional[datetime]) -> Optional[float]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (utc_now() - dt).total_seconds() / 3600.0)


def recency_weight(hours_old: Optional[float], half_life: float = NewsConfig.RECENCY_HALFLIFE_HOURS) -> float:
    if hours_old is None:
        return 0.6
    return float(0.5 ** (hours_old / max(1.0, half_life)))


@dataclass(slots=True)
class NewsArticle:
    title: str
    url: str = ""
    source: str = ""
    source_domain: str = ""
    published_utc: Optional[str] = None
    published_riyadh: Optional[str] = None
    crawled_utc: str = field(default_factory=utc_iso)
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
    language: str = "en"
    word_count: int = 0
    credibility_weight: float = 1.0
    is_headline: bool = False
    duplicate_of: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "NewsArticle":
        return cls(**d)


@dataclass(slots=True)
class SentimentBreakdown:
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
        d = asdict(self)
        d["sentiment_trend"] = self.sentiment_trend[-10:]
        d["emerging_topics"] = self.emerging_topics[:5]
        d["articles"] = [a.to_dict() for a in self.articles[:5]]
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "NewsResult":
        articles = [NewsArticle.from_dict(a) for a in d.get("articles", [])]
        breakdown = SentimentBreakdown(**d.get("breakdown", {}))
        return cls(
            symbol=d["symbol"],
            query=d["query"],
            sentiment=float(d["sentiment"]),
            confidence=float(d["confidence"]),
            news_boost=float(d["news_boost"]),
            breakdown=breakdown,
            articles=articles,
            sources_used=d.get("sources_used", []),
            articles_analyzed=d.get("articles_analyzed", 0),
            articles_filtered=d.get("articles_filtered", 0),
            sentiment_trend=d.get("sentiment_trend", []),
            article_velocity=float(d.get("article_velocity", 0.0)),
            emerging_topics=d.get("emerging_topics", []),
            processing_time_ms=float(d.get("processing_time_ms", 0.0)),
            cached=bool(d.get("cached", False)),
            cache_key=d.get("cache_key"),
            version=d.get("version", NEWS_VERSION),
        )


@dataclass(slots=True)
class BatchResult:
    items: List[Dict[str, Any]] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    errors: List[Dict[str, str]] = field(default_factory=list)


class SingleFlight:
    def __init__(self) -> None:
        self._calls: Dict[str, asyncio.Future[Any]] = {}
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def execute(self, key: str, coro_func: Callable[[], Any]) -> Any:
        lock = self._get_lock()
        async with lock:
            if key in self._calls:
                return await self._calls[key]
            fut = asyncio.get_running_loop().create_future()
            self._calls[key] = fut

        try:
            result = await coro_func()
            if not fut.done():
                fut.set_result(result)
            return result
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
            raise
        finally:
            async with lock:
                self._calls.pop(key, None)


_SINGLE_FLIGHT = SingleFlight()
HTML_TAG_RE = re.compile(r"<[^>]+>")
CDATA_RE = re.compile(r"<!\[CDATA\[(.*?)\]\]>", re.DOTALL)
WHITESPACE_RE = re.compile(r"\s+")
URL_TRACKING_RE = re.compile(r"[?#&](utm_|fbclid|gclid|ref|source|mc_cid|mc_eid).*$")
TOKEN_RE = re.compile(r"[A-Za-z]+|[0-9]+|[\u0600-\u06FF]+")


def strip_html(text: str) -> str:
    if not text:
        return ""
    text = CDATA_RE.sub(r"\1", text)
    text = HTML_TAG_RE.sub(" ", text)
    return WHITESPACE_RE.sub(" ", text).strip()


def normalize_text(text: str, remove_special: bool = True) -> str:
    if not text:
        return ""
    text = WHITESPACE_RE.sub(" ", text.lower().strip())
    if remove_special:
        text = re.sub(r"[^\w\s\u0600-\u06FF]", "", text)
    return text


def canonical_url(url: str) -> str:
    url = (url or "").strip()
    return URL_TRACKING_RE.sub("", url) if url else ""


def extract_domain(url: str) -> str:
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split("/")[0]
        return domain.replace("www.", "").split(":")[0].lower()
    except Exception:
        return ""


def get_credibility_weight(domain: str) -> float:
    if not NewsConfig.ENABLE_SOURCE_CREDIBILITY:
        return 1.0
    domain = (domain or "").lower()
    for key, weight in NewsConfig.SOURCE_WEIGHTS.items():
        if key != "default" and key in domain:
            return weight
    return NewsConfig.SOURCE_WEIGHTS.get("default", 0.6)


def detect_language(text: str) -> str:
    return "ar" if text and re.search(r"[\u0600-\u06FF]", text) else "en"


def tokenize(text: str) -> List[str]:
    return TOKEN_RE.findall(normalize_text(text))


def analyze_sentiment_lexicon(text: str) -> Tuple[float, float, int, int, int, List[str]]:
    if not text:
        return 0.0, 0.0, 0, 0, 0, []
    normalized = normalize_text(text)
    if not normalized:
        return 0.0, 0.0, 0, 0, 0, []

    score = 0.0
    pos_hits = 0
    neg_hits = 0
    neutral_hits = 0
    severe_terms: List[str] = []

    for phrase, weight in POSITIVE_PHRASES.items():
        if phrase in normalized:
            score += weight
            pos_hits += 2
    for phrase, weight in NEGATIVE_PHRASES.items():
        if phrase in normalized:
            score += weight
            neg_hits += 2
    for term in SEVERE_NEGATIVE_WORDS:
        if term in normalized:
            severe_terms.append(term)

    tokens = tokenize(normalized)
    if not tokens:
        return score, NewsConfig.MIN_CONFIDENCE, pos_hits, neg_hits, neutral_hits, severe_terms

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
        if i > 0 and tokens[i - 1] in INTENSIFIERS_POS and word_score > 0:
            word_score *= 1.3
        elif i > 0 and tokens[i - 1] in INTENSIFIERS_NEG and word_score < 0:
            word_score *= 1.3
        if flip_window > 0 and word_score != 0:
            word_score = -word_score
        score += word_score
        if flip_window > 0:
            flip_window -= 1

    total_hits = pos_hits + neg_hits
    if total_hits == 0:
        return 0.0, NewsConfig.MIN_CONFIDENCE, 0, 0, neutral_hits, severe_terms

    sentiment = max(-1.0, min(1.0, score / max(5.0, float(total_hits))))
    confidence = max(NewsConfig.MIN_CONFIDENCE, min(1.0, total_hits / 15.0))
    return sentiment, confidence, pos_hits, neg_hits, neutral_hits, severe_terms


class _LazyImports:
    _httpx = None
    _redis = None
    _translator_cls = None
    _fuzz = None

    @classmethod
    def httpx(cls):
        if cls._httpx is None and HAS_HTTPX:
            try:
                import httpx  # type: ignore
                cls._httpx = httpx
            except Exception:
                cls._httpx = False
        return None if cls._httpx is False else cls._httpx

    @classmethod
    def redis(cls):
        if cls._redis is None and HAS_REDIS:
            try:
                from redis.asyncio import Redis  # type: ignore
                cls._redis = Redis
            except Exception:
                cls._redis = False
        return None if cls._redis is False else cls._redis

    @classmethod
    def translator_cls(cls):
        if cls._translator_cls is None and HAS_TRANSLATOR:
            try:
                from googletrans import Translator  # type: ignore
                cls._translator_cls = Translator
            except Exception:
                cls._translator_cls = False
        return None if cls._translator_cls is False else cls._translator_cls

    @classmethod
    def fuzz_partial_ratio(cls) -> Optional[Callable[[str, str], int]]:
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


class DeepLearningSentiment:
    _instance: Optional["DeepLearningSentiment"] = None
    _lock: Optional[asyncio.Lock] = None

    def __init__(self) -> None:
        self.model = None
        self.tokenizer = None
        self.pipeline = None
        self.device = "cpu"
        self.initialized = False
        self.model_name = NewsConfig.DL_MODEL_NAME

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    @classmethod
    async def get_instance(cls) -> "DeepLearningSentiment":
        if cls._instance is None:
            async with cls._get_lock():
                if cls._instance is None:
                    inst = cls()
                    await inst.initialize()
                    cls._instance = inst
        return cls._instance

    async def initialize(self) -> None:
        if not NewsConfig.ENABLE_DEEP_LEARNING or not HAS_TRANSFORMERS:
            self.initialized = False
            return
        loop = asyncio.get_running_loop()

        def _load() -> Tuple[Any, Any, Any, str]:
            import torch  # type: ignore
            from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline  # type: ignore
            use_gpu = bool(NewsConfig.DL_USE_GPU and getattr(torch, "cuda", None) and torch.cuda.is_available())
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
        if not self.initialized or not self.pipeline or not texts:
            return [None for _ in texts]
        loop = asyncio.get_running_loop()

        def _run(batch: List[str]) -> List[Optional[float]]:
            results = self.pipeline(batch, truncation=True, max_length=256, batch_size=NewsConfig.DL_BATCH_SIZE)
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


class FullJitterBackoff:
    def __init__(self, max_retries: int = 3, base_delay: float = 0.5, max_delay: float = 8.0):
        self.max_retries = max(1, int(max_retries))
        self.base_delay = max(0.05, float(base_delay))
        self.max_delay = max(self.base_delay, float(max_delay))

    async def execute_async(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        last_exc: Optional[BaseException] = None
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
        raise last_exc  # type: ignore[misc]


class _AdvancedCache:
    def __init__(self, maxsize: int, ttl_sec: int, use_compression: bool = True):
        self.maxsize = max(100, int(maxsize))
        self.ttl_sec = max(0, int(ttl_sec))
        self.use_compression = bool(use_compression)
        self._data: "OrderedDict[str, Tuple[bytes, float]]" = OrderedDict()
        self._lock = threading.RLock()
        self._redis = None
        self._stats = {"hits": 0, "misses": 0, "sets": 0, "evictions": 0, "size": 0}

    async def _get_redis(self):
        if not NewsConfig.ENABLE_REDIS:
            return None
        if self._redis is not None:
            return self._redis
        RedisCls = _LazyImports.redis()
        if RedisCls is None:
            return None
        self._redis = RedisCls.from_url(NewsConfig.REDIS_URL, decode_responses=False)  # type: ignore[attr-defined]
        return self._redis

    def _pack(self, value: Any) -> bytes:
        raw = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        return zlib.compress(raw, level=6) if self.use_compression else raw

    def _unpack(self, payload: bytes) -> Any:
        blob = zlib.decompress(payload) if self.use_compression else payload
        return pickle.loads(blob)

    async def get(self, key: str) -> Optional[Any]:
        now = time.time()
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
        packed = self._pack(value)
        exp = time.time() + self.ttl_sec
        with self._lock:
            self._data[key] = (packed, exp)
            self._data.move_to_end(key)
            while len(self._data) > self.maxsize:
                self._data.popitem(last=False)
                self._stats["evictions"] += 1
            self._stats["sets"] += 1
            self._stats["size"] = len(self._data)
        redis_cli = await self._get_redis()
        if redis_cli is not None:
            try:
                await redis_cli.setex(key, self.ttl_sec, packed)
            except Exception:
                pass

    async def clear(self) -> None:
        with self._lock:
            self._data.clear()
            self._stats["size"] = 0
        redis_cli = await self._get_redis()
        if redis_cli is not None:
            try:
                # best-effort, no wildcard delete to avoid expensive scans in production
                pass
            except Exception:
                pass

    async def stats(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._stats)


_CACHE = _AdvancedCache(NewsConfig.MAX_CACHE_ITEMS, NewsConfig.CACHE_TTL_SECONDS, NewsConfig.CACHE_COMPRESSION)
_BACKOFF = FullJitterBackoff(max_retries=max(1, NewsConfig.RETRY_ATTEMPTS + 1), base_delay=NewsConfig.RETRY_DELAY)
_HTTPX_CLIENT = None
_HTTPX_LOCK = asyncio.Lock()


async def _get_httpx_client():
    global _HTTPX_CLIENT
    if _HTTPX_CLIENT is not None:
        return _HTTPX_CLIENT
    async with _HTTPX_LOCK:
        if _HTTPX_CLIENT is not None:
            return _HTTPX_CLIENT
        httpx = _LazyImports.httpx()
        if httpx is None:
            return None
        _HTTPX_CLIENT = httpx.AsyncClient(
            timeout=NewsConfig.TIMEOUT_SECONDS,
            headers={
                "User-Agent": NewsConfig.USER_AGENT,
                "Accept": "application/rss+xml, application/xml, text/xml, application/json, text/html;q=0.7, */*;q=0.5",
                "Accept-Language": NewsConfig.ACCEPT_LANGUAGE,
            },
            follow_redirects=True,
        )
        return _HTTPX_CLIENT


def _symbol_aliases(symbol: str) -> List[str]:
    s = (symbol or "").strip().upper()
    aliases = [s]
    if s.endswith(".SR"):
        aliases.append(s[:-3])
    elif s.isdigit() and 3 <= len(s) <= 6:
        aliases.append(f"{s}.SR")
    return [x for x in aliases if x]


def build_query_terms(symbol: str, company_name: str = "") -> List[str]:
    terms = _symbol_aliases(symbol)
    if company_name:
        terms.append(company_name.strip())
    return list(dict.fromkeys([t for t in terms if t]))


def _build_google_news_url(query: str) -> str:
    encoded = quote_plus(query)
    return (
        f"https://news.google.com/rss/search?q={encoded}"
        f"&hl={NewsConfig.GOOGLE_HL}&gl={NewsConfig.GOOGLE_GL}&ceid={NewsConfig.GOOGLE_CEID}"
    )


async def _fetch_text(url: str) -> str:
    if not NewsConfig.ALLOW_NETWORK:
        return ""
    client = await _get_httpx_client()
    if client is None:
        return ""
    async with TraceContext("news_fetch", {"url": url}):
        r = await client.get(url)
        r.raise_for_status()
        return r.text or ""


def _parse_rss(xml_text: str, source_hint: str) -> List[NewsArticle]:
    if not xml_text.strip():
        return []
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return []
    items: List[NewsArticle] = []
    for item in root.findall(".//item"):
        title = strip_html(item.findtext("title") or "")
        if not title:
            continue
        link = canonical_url(strip_html(item.findtext("link") or ""))
        desc = strip_html(item.findtext("description") or "")
        pub_raw = strip_html(item.findtext("pubDate") or item.findtext("published") or "")
        pub_dt = parse_datetime(pub_raw)
        source = strip_html(item.findtext("source") or source_hint or "") or source_hint
        domain = extract_domain(link) or extract_domain(source_hint)
        art = NewsArticle(
            title=title,
            url=link,
            source=source or domain,
            source_domain=domain,
            published_utc=utc_iso(pub_dt) if pub_dt else None,
            published_riyadh=to_riyadh_iso(pub_dt),
            snippet=desc,
            language=detect_language(f"{title} {desc}"),
            word_count=len(tokenize(f"{title} {desc}")),
            credibility_weight=get_credibility_weight(domain),
            is_headline=True,
        )
        items.append(art)
    return items


def _dedupe_articles(items: Sequence[NewsArticle]) -> List[NewsArticle]:
    if not NewsConfig.ENABLE_DEDUPLICATION:
        return list(items)
    out: List[NewsArticle] = []
    seen_urls: set[str] = set()
    seen_titles: set[str] = set()
    partial_ratio = _LazyImports.fuzz_partial_ratio()
    for article in items:
        url = canonical_url(article.url)
        title_key = normalize_text(article.title)
        if url and url in seen_urls:
            continue
        if title_key in seen_titles:
            continue
        if partial_ratio is not None:
            dup = False
            for prev in seen_titles:
                try:
                    if partial_ratio(title_key, prev) >= 95:
                        dup = True
                        break
                except Exception:
                    pass
            if dup:
                continue
        if url:
            seen_urls.add(url)
        if title_key:
            seen_titles.add(title_key)
        out.append(article)
    return out


def _article_topics(text: str) -> List[str]:
    if not NewsConfig.ENABLE_TOPIC_CLASSIFICATION:
        return []
    t = normalize_text(text)
    topics: List[str] = []
    for topic, words in TOPIC_KEYWORDS.items():
        if any(w in t for w in words):
            topics.append(topic)
    return topics[:5]


def _article_entities(text: str, symbol: str, company_name: str = "") -> List[str]:
    if not NewsConfig.ENABLE_ENTITY_RECOGNITION:
        return []
    entities: List[str] = []
    for candidate in build_query_terms(symbol, company_name):
        if candidate and candidate.lower() in text.lower():
            entities.append(candidate)
    return list(dict.fromkeys(entities))[:5]


def _compute_relevance(article: NewsArticle, symbol: str, company_name: str = "") -> float:
    if not NewsConfig.ENABLE_RELEVANCE_SCORING:
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
    rec = recency_weight(hours_old)
    return max(-1.0, min(1.0, sentiment * relevance * credibility * rec * 1.25))


def _news_boost_from_sentiment(sentiment: float, confidence: float, article_count: int) -> float:
    if article_count <= 0:
        return 0.0
    strength = min(1.0, article_count / max(1.0, float(NewsConfig.MAX_ARTICLES)))
    boost = sentiment * confidence * (0.35 + 0.65 * strength)
    return max(-NewsConfig.BOOST_CLAMP, min(NewsConfig.BOOST_CLAMP, boost * NewsConfig.BOOST_CLAMP))


def _cache_key(symbol: str, company_name: str, include_articles: bool, max_articles: int) -> str:
    raw = f"{symbol}|{company_name}|{int(include_articles)}|{max_articles}|{NewsConfig.QUERY_MODE}|{NEWS_VERSION}"
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"news:{symbol}:{h[:24]}"


async def _translate_if_needed(text: str, language: str) -> str:
    if not text or not language or language == "en" or not NewsConfig.ENABLE_TRANSLATION:
        return text
    Translator = _LazyImports.translator_cls()
    if Translator is None:
        return text
    try:
        translator = Translator()
        result = translator.translate(text, dest="en")
        return getattr(result, "text", text) or text
    except Exception:
        return text


async def _fetch_source_articles(symbol: str, company_name: str, source_url: str) -> List[NewsArticle]:
    try:
        xml_text = await _BACKOFF.execute_async(_fetch_text, source_url)
    except Exception as e:
        logger.debug("news source fetch failed for %s: %s", source_url, e)
        return []
    source_hint = extract_domain(source_url)
    articles = _parse_rss(xml_text, source_hint)
    out: List[NewsArticle] = []
    for art in articles:
        hay = f"{art.title} {art.snippet}".lower()
        if any(term.lower() in hay for term in build_query_terms(symbol, company_name)):
            out.append(art)
    return out


async def _collect_articles(symbol: str, company_name: str, max_articles: int) -> Tuple[List[NewsArticle], List[str]]:
    sources_used: List[str] = []
    articles: List[NewsArticle] = []

    urls: List[Tuple[str, str]] = []
    if "google" in NewsConfig.QUERY_MODE:
        query = " OR ".join(build_query_terms(symbol, company_name))
        urls.append(("google_news", _build_google_news_url(query)))
    if "rss" in NewsConfig.QUERY_MODE:
        urls.extend((extract_domain(u) or "rss", u) for u in NewsConfig.RSS_SOURCES)

    sem = asyncio.Semaphore(NewsConfig.CONCURRENCY)

    async def _one(label: str, url: str) -> None:
        nonlocal articles
        async with sem:
            try:
                found = await _fetch_source_articles(symbol, company_name, url)
                if found:
                    sources_used.append(label)
                    articles.extend(found)
                    news_articles_total.labels(source=label).inc(len(found))
            except Exception as e:
                logger.debug("news collection failed for %s: %s", label, e)

    await asyncio.gather(*[_one(label, url) for label, url in urls], return_exceptions=True)

    deduped = _dedupe_articles(articles)
    deduped.sort(key=lambda a: parse_datetime(a.published_utc or "") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return deduped[:max_articles], list(dict.fromkeys(sources_used))


async def _score_articles(symbol: str, company_name: str, articles: List[NewsArticle]) -> Tuple[List[NewsArticle], SentimentBreakdown, List[float]]:
    if not articles:
        return [], SentimentBreakdown(), []

    texts: List[str] = []
    for a in articles:
        base_text = f"{a.title}. {a.snippet}".strip()
        if a.language != "en":
            base_text = await _translate_if_needed(base_text, a.language)
        texts.append(base_text)

    ml_scores: List[Optional[float]] = [None for _ in texts]
    if NewsConfig.ENABLE_DEEP_LEARNING:
        try:
            dl = await DeepLearningSentiment.get_instance()
            ml_scores = await dl.analyze(texts)
        except Exception as e:
            logger.debug("Deep learning scoring skipped: %s", e)

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
        published_dt = parse_datetime(article.published_utc)
        hours_old = age_hours(published_dt)
        article.impact_score = _impact_score(combined, article.relevance, article.credibility_weight, hours_old)
        article.sentiment = combined
        article.confidence = max(NewsConfig.MIN_CONFIDENCE, min(1.0, lex_conf * article.credibility_weight))
        trend.append(combined)

        rec = recency_weight(hours_old) if NewsConfig.ENABLE_RECENCY_WEIGHTING else 1.0
        sev_pen = 0.75 if NewsConfig.ENABLE_SEVERE_PENALTY and severe_terms else 1.0
        w = max(0.05, article.relevance * article.credibility_weight * rec * sev_pen)
        weighted_sum += combined * w
        weight_total += w

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
            "article_count": min(1.0, len(articles) / max(1.0, float(NewsConfig.MAX_ARTICLES))),
            "source_diversity": min(1.0, len({a.source_domain for a in articles if a.source_domain}) / 5.0),
            "weight_total": min(1.0, weight_total / 10.0),
        },
    )
    return articles, breakdown, trend[-10:]


async def _compute_news_result(symbol: str, company_name: str = "", include_articles: bool = True, max_articles: Optional[int] = None) -> NewsResult:
    started = time.perf_counter()
    max_articles = max_articles or NewsConfig.MAX_ARTICLES
    query = " OR ".join(build_query_terms(symbol, company_name)) or symbol
    cache_key = _cache_key(symbol, company_name, include_articles, max_articles)

    if NewsConfig.ENABLE_CACHE:
        cached = await _CACHE.get(cache_key)
        if isinstance(cached, dict):
            try:
                result = NewsResult.from_dict(cached)
                result.cached = True
                result.cache_key = cache_key
                news_cache_hits_total.labels(symbol=symbol).inc()
                return result
            except Exception:
                pass
        news_cache_misses_total.labels(symbol=symbol).inc()

    articles, sources_used = await _collect_articles(symbol, company_name, max_articles=max_articles)
    scored_articles, breakdown, trend = await _score_articles(symbol, company_name, articles)

    weighted_score = breakdown.weighted_score
    confidence = max(
        NewsConfig.MIN_CONFIDENCE,
        min(
            1.0,
            0.35 * breakdown.confidence_factors.get("article_count", 0.0)
            + 0.25 * breakdown.confidence_factors.get("source_diversity", 0.0)
            + 0.25 * breakdown.confidence_factors.get("weight_total", 0.0)
            + 0.15 * (1.0 if scored_articles else 0.0),
        ),
    )
    news_boost = _news_boost_from_sentiment(weighted_score, confidence, len(scored_articles))
    article_velocity = len([a for a in scored_articles if (age_hours(parse_datetime(a.published_utc)) or 9999) <= 24.0]) / 24.0

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

    if NewsConfig.ENABLE_CACHE:
        try:
            await _CACHE.set(cache_key, result.to_dict())
        except Exception:
            pass

    news_sentiment_score.labels(symbol=symbol).set(result.sentiment)
    news_confidence_score.labels(symbol=symbol).set(result.confidence)
    return result


async def get_news_intelligence(payload: Union[str, Dict[str, Any]], include_articles: bool = True, max_articles: Optional[int] = None) -> Dict[str, Any]:
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
    status = "success"
    source_label = NewsConfig.QUERY_MODE
    try:
        async with TraceContext("get_news_intelligence", {"symbol": symbol, "source": source_label}):
            result = await _SINGLE_FLIGHT.execute(
                f"news:{symbol}:{company_name}:{int(include_articles)}:{max_articles or NewsConfig.MAX_ARTICLES}",
                lambda: _compute_news_result(symbol, company_name, include_articles=include_articles, max_articles=max_articles),
            )
            news_requests_total.labels(symbol=symbol, source=source_label, status="success").inc()
            news_request_duration.labels(symbol=symbol, source=source_label).observe((time.perf_counter() - started))
            return result.to_dict()
    except Exception as e:
        status = "error"
        logger.exception("get_news_intelligence failed for %s: %s", symbol, e)
        news_requests_total.labels(symbol=symbol, source=source_label, status=status).inc()
        news_request_duration.labels(symbol=symbol, source=source_label).observe((time.perf_counter() - started))
        fallback = NewsResult(
            symbol=symbol,
            query=symbol,
            sentiment=0.0,
            confidence=0.0,
            news_boost=0.0,
            processing_time_ms=(time.perf_counter() - started) * 1000.0,
        )
        return fallback.to_dict()


async def batch_news_intelligence(items: List[Dict[str, Any]], include_articles: bool = False) -> BatchResult:
    started = time.perf_counter()
    valid_results: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []
    sem = asyncio.Semaphore(NewsConfig.CONCURRENCY)

    async def _one(item: Dict[str, Any]) -> None:
        async with sem:
            symbol = str(item.get("symbol") or item.get("ticker") or item.get("code") or "").strip().upper()
            if not symbol:
                errors.append({"symbol": "", "error": "missing_symbol"})
                return
            try:
                out = await get_news_intelligence(item, include_articles=include_articles)
                valid_results.append(out)
            except Exception as e:
                errors.append({"symbol": symbol, "error": str(e)})

    await asyncio.gather(*[_one(item) for item in items], return_exceptions=True)

    meta = {
        "status": "success" if not errors else ("partial" if valid_results else "error"),
        "version": NEWS_VERSION,
        "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
        "requested": len(items),
        "returned": len(valid_results),
        "errors": len(errors),
        "config": {
            "mode": NewsConfig.QUERY_MODE,
            "max_articles": NewsConfig.MAX_ARTICLES,
            "cache_ttl": NewsConfig.CACHE_TTL_SECONDS,
            "concurrency": NewsConfig.CONCURRENCY,
            "timeout": NewsConfig.TIMEOUT_SECONDS,
            "recency_half_life": NewsConfig.RECENCY_HALFLIFE_HOURS,
            "boost_clamp": NewsConfig.BOOST_CLAMP,
            "deep_learning": NewsConfig.ENABLE_DEEP_LEARNING,
        },
        "google": {
            "hl": NewsConfig.GOOGLE_HL,
            "gl": NewsConfig.GOOGLE_GL,
            "ceid": NewsConfig.GOOGLE_CEID,
            "fresh_days": NewsConfig.GOOGLE_FRESH_DAYS,
        },
        "sources_count": len(NewsConfig.RSS_SOURCES),
        "timestamp_utc": utc_iso(),
        "timestamp_riyadh": to_riyadh_iso(utc_now()),
    }
    return BatchResult(items=valid_results, meta=meta, errors=errors)


async def clear_cache() -> None:
    await _CACHE.clear()
    logger.info("News cache cleared")


async def get_cache_stats() -> Dict[str, Any]:
    return await _CACHE.stats()


async def warmup_cache(symbols: List[str]) -> Dict[str, Any]:
    result = await batch_news_intelligence([{"symbol": s} for s in symbols], include_articles=False)
    return {
        "warmed": len(result.items),
        "elapsed_ms": result.meta.get("elapsed_ms", 0),
        "cache_stats": await get_cache_stats(),
    }


async def health_check() -> Dict[str, Any]:
    health: Dict[str, Any] = {
        "status": "healthy",
        "version": NEWS_VERSION,
        "timestamp_utc": utc_iso(),
        "timestamp_riyadh": to_riyadh_iso(utc_now()),
        "config": {
            "mode": NewsConfig.QUERY_MODE,
            "max_articles": NewsConfig.MAX_ARTICLES,
            "cache_ttl": NewsConfig.CACHE_TTL_SECONDS,
            "concurrency": NewsConfig.CONCURRENCY,
            "deep_learning": NewsConfig.ENABLE_DEEP_LEARNING,
            "redis": NewsConfig.ENABLE_REDIS,
            "allow_network": NewsConfig.ALLOW_NETWORK,
        },
        "dependencies": {
            "httpx": HAS_HTTPX,
            "elementtree": True,
            "transformers": HAS_TRANSFORMERS,
            "nltk": HAS_NLTK,
            "rapidfuzz": HAS_RAPIDFUZZ,
            "redis": HAS_REDIS and NewsConfig.ENABLE_REDIS,
        },
    }
    try:
        health["cache"] = await get_cache_stats()
    except Exception as e:
        health["cache"] = {"error": str(e)}

    if NewsConfig.ENABLE_DEEP_LEARNING:
        try:
            dl = await DeepLearningSentiment.get_instance()
            health["deep_learning"] = {
                "initialized": dl.initialized,
                "model": NewsConfig.DL_MODEL_NAME,
                "device": dl.device,
            }
        except Exception as e:
            health["deep_learning"] = {"error": str(e)}
    return health


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
