"""Offline regressions for the live RSS-to-brief contracts, without event models."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from xml.sax.saxutils import escape

import pytest

from core import news_intelligence as news


NOW = datetime(2026, 10, 8, 12, tzinfo=timezone.utc)


def rss(*, title="Acme Test Corporation announces quarterly results - Reuters",
        link="https://news.google.com/rss/articles/synthetic",
        source_name="Reuters", source_url="https://www.reuters.com",
        published="Thu, 08 Oct 2026 10:00:00 GMT"):
    src = (f'<source url="{escape(source_url)}">{escape(source_name)}</source>'
           if source_url else f"<source>{escape(source_name)}</source>")
    pub = f"<pubDate>{escape(published)}</pubDate>" if published else ""
    return ("<rss><channel><item>" + f"<title>{escape(title)}</title>"
            + f"<link>{escape(link)}</link>" + pub + src
            + "</item></channel></rss>")


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(news, "_utc_now", lambda: NOW)
    monkeypatch.setattr(news, "_CONFIG", replace(
        news._CONFIG, allow_network=False, enable_cache=False,
        enable_deep_learning=False, enable_translation=False,
        enable_fuzzy_matching=False, enable_source_credibility=True,
        rss_sources=[], query_mode="google"))
    monkeypatch.setattr(news, "_SINGLE_FLIGHT", news.SingleFlight())
    monkeypatch.setattr(news, "_get_httpx_client", AsyncMock(
        side_effect=AssertionError("offline regression must not use HTTP")))


def result(articles):
    return news.NewsResult("ACME.US", "Acme Test Corporation", 0.0, 0.0, 0.0,
                           articles=articles, articles_analyzed=len(articles))


def test_brief_helper_uses_documented_payload_and_real_analysis(monkeypatch):
    fetch = AsyncMock(return_value=rss())
    monkeypatch.setattr(news, "_fetch_text", fetch)
    summary = asyncio.run(news.summarize_symbol_news(
        "ACME.US", company_name="Acme Test Corporation", days=7))
    fetch.assert_awaited_once()
    assert "Acme+Test+Corporation" in fetch.await_args.args[0]
    assert summary["headline_count"] == summary["recent_count"] == 1
    assert "Acme Test Corporation announces quarterly results" in summary["latest_headline"]
    assert "reuters.com" in summary["latest_headline"]
    assert summary["display_line"].endswith("Context only — not a signal.")


def test_native_article_timestamp_and_publisher_survive_display():
    article = news._parse_rss(rss(), "news.google.com")[0]
    summary = news.summarize_for_display(result([article]))
    assert summary["recent_count"] == 1
    assert summary["latest_headline"].endswith("— reuters.com (10-08)")


def test_display_cutoff_uses_utc_instant_and_keeps_unknown_time_unknown():
    cutoff = NOW - timedelta(days=7)
    articles = [
        news.NewsArticle("At cutoff", published_utc=cutoff.isoformat()),
        news.NewsArticle("Same UTC instant", published_utc="2026-10-01T08:00:00-04:00"),
        news.NewsArticle("Before cutoff", published_utc=(cutoff - timedelta(seconds=1)).isoformat()),
        news.NewsArticle("Unknown publication", crawled_utc=NOW.isoformat()),
    ]
    assert news.summarize_for_display(result(articles))["recent_count"] == 2
    unknown = news.summarize_for_display(result(articles[-1:]))
    assert unknown["recent_count"] == 0
    assert unknown["latest_headline"] is None


@pytest.mark.parametrize("time_key", ["published_at", "published"])
def test_display_retains_legacy_timestamp_aliases(time_key):
    summary = news.summarize_for_display({
        "symbol": "ACME.US", "articles_analyzed": 1,
        "articles": [{"title": "Legacy fact", time_key: NOW.isoformat(),
                      "source": "legacy-publisher"}],
    })
    assert summary["recent_count"] == 1
    assert "legacy-publisher" in summary["latest_headline"]


@pytest.mark.parametrize("host", [
    "reuters.com.evil.example", "notreuters.com", "reuters.com@evil.example",
    "bloomberg.com.evil.example", "fakecnbc.com",
])
def test_spoofed_source_host_never_gets_trusted_weight(host):
    parsed = news._parse_rss(rss(link="https://" + host + "/news",
                                 source_name="", source_url=""), "synthetic.example")[0]
    assert parsed.credibility_weight == news.SOURCE_CREDIBILITY["default"]


@pytest.mark.parametrize("url", ["https://reuters.com/news", "https://markets.reuters.com/news",
                                  "https://www.reuters.com:443/news", "https://REUTERS.COM./news"])
def test_real_source_hosts_keep_existing_weight(url):
    assert news._get_credibility_weight(news._extract_domain(url)) == news.SOURCE_CREDIBILITY["reuters.com"]


def test_google_rss_preserves_asserted_publisher_and_transport():
    article = news._parse_rss(rss(), "news.google.com")[0]
    assert article.source_domain == "reuters.com"
    assert article.transport_domain == "news.google.com"
    assert article.publisher_url == "https://www.reuters.com"
    assert article.url == "https://news.google.com/rss/articles/synthetic"
    assert article.source == "Reuters"
    assert article.title == "Acme Test Corporation announces quarterly results"
    assert article.credibility_weight == news.SOURCE_CREDIBILITY["reuters.com"]


def test_publisher_name_alone_does_not_replace_aggregator_host():
    article = news._parse_rss(rss(source_url=""), "news.google.com")[0]
    assert article.source_domain == "news.google.com"
    assert article.publisher_url == ""
    assert article.credibility_weight == news.SOURCE_CREDIBILITY["default"]


def test_asserted_spoofed_publisher_is_retained_without_trusted_weight():
    article = news._parse_rss(rss(source_url="https://reuters.com.evil.example"),
                              "news.google.com")[0]
    assert article.source_domain == "reuters.com.evil.example"
    assert article.transport_domain == "news.google.com"
    assert article.credibility_weight == news.SOURCE_CREDIBILITY["default"]


def test_nonaggregator_source_tag_does_not_override_actual_publisher():
    article = news._parse_rss(rss(link="https://www.cnbc.com/synthetic"), "cnbc.com")[0]
    assert article.source_domain == "cnbc.com"
    assert article.transport_domain == "cnbc.com"
    assert article.credibility_weight == news.SOURCE_CREDIBILITY["cnbc.com"]


def test_aggregator_link_in_unknown_feed_cannot_assert_a_trusted_publisher():
    article = news._parse_rss(rss(), "synthetic.example")[0]
    assert article.source_domain == "news.google.com"
    assert article.transport_domain == "synthetic.example"
    assert article.publisher_url == ""
    assert article.credibility_weight == news.SOURCE_CREDIBILITY["default"]


def test_direct_article_keeps_origin_separate_from_feed_transport():
    article = news._parse_rss(rss(link="https://www.reuters.com/synthetic"), "cnbc.com")[0]
    assert article.source_domain == "reuters.com"
    assert article.transport_domain == "cnbc.com"
    assert article.credibility_weight == news.SOURCE_CREDIBILITY["reuters.com"]


def test_cache_roundtrip_preserves_provenance_and_old_article_payloads():
    article = news._parse_rss(rss(), "news.google.com")[0]
    restored = news.NewsResult.from_dict(result([article]).to_dict()).articles[0]
    assert restored.source_domain == "reuters.com"
    assert restored.transport_domain == "news.google.com"
    assert restored.publisher_url == "https://www.reuters.com"
    legacy = news.NewsArticle.from_dict({"title": "Old cache fact", "source_domain": "cnbc.com"})
    assert legacy.transport_domain == legacy.publisher_url == ""
