"""Offline news acquisition truth: empty feeds differ from failed fetches."""
from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core import news_intelligence as news
from scripts import news_archive_collector as archive


NOW = datetime(2026, 10, 23, 7, tzinfo=timezone.utc)
FEED = """<rss><channel><item><title>ACME.US new factory</title>
<link>https://news.example/acme-factory</link>
<pubDate>Fri, 23 Oct 2026 06:00:00 GMT</pubDate>
</item></channel></rss>"""


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(news, "_utc_now", lambda: NOW)
    monkeypatch.setattr(news, "_CONFIG", replace(
        news._CONFIG, allow_network=False, enable_cache=False,
        enable_deep_learning=False, enable_translation=False,
        enable_fuzzy_matching=False, rss_sources=[], query_mode="google"))
    monkeypatch.setattr(news, "_SINGLE_FLIGHT", news.SingleFlight())
    monkeypatch.setattr(news, "_get_httpx_client", AsyncMock(
        side_effect=AssertionError("offline test attempted HTTP")))
    monkeypatch.setattr(archive, "_utc_now", lambda: NOW)
    monkeypatch.setattr(archive, "_riyadh_date", lambda: "2026-10-23")


def acquire(monkeypatch, *, text=None, error=None):
    fetch = AsyncMock(return_value=text, side_effect=error)
    monkeypatch.setattr(news._BACKOFF, "execute_async", fetch)
    return asyncio.run(news.get_news_intelligence("ACME.US", include_articles=True))


def test_valid_empty_feed_is_successful_empty_not_failed(monkeypatch):
    result = acquire(monkeypatch, text="<rss><channel/></rss>")
    evidence = result["collection_evidence"]
    assert (evidence["status"], evidence["succeeded"], evidence["failed"]) == ("EMPTY", 1, 0)
    assert result["articles_analyzed"] == 0
    assert archive._collection_report(["ACME.US"], [result], "2026-10-23")["complete"]


@pytest.mark.parametrize("text", ["", "not XML", "<html><body>blocked</body></html>",
                                '<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>ACME.US</title></entry></feed>',
                                '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"/>'])
def test_empty_or_invalid_response_is_not_a_neutral_success(monkeypatch, text):
    result = acquire(monkeypatch, text=text)
    assert result["collection_evidence"]["status"] == "ERROR"
    report = archive._collection_report(["ACME.US"], [result], "2026-10-23")
    assert report["complete"] is False


def test_failed_source_emits_safe_code_without_exception_or_feed_url(monkeypatch):
    result = acquire(monkeypatch, error=RuntimeError("token=VERY_PRIVATE https://secret/feed"))
    assert result["collection_evidence"]["status"] == "ERROR"
    rendered = json.dumps(result["collection_evidence"])
    assert "news_fetch_failed" in rendered
    assert "VERY_PRIVATE" not in rendered and "https://secret" not in rendered


def test_success_retains_source_evidence_and_bounded_article_metadata(monkeypatch):
    result = acquire(monkeypatch, text=FEED)
    assert result["collection_evidence"]["status"] == "OK"
    report = archive._collection_report(["ACME.US"], [result], "2026-10-23")
    article = report["symbols"][0]["article_provenance"][0]
    assert article["url"] == "https://news.example/acme-factory"
    assert article["published_utc"] == "2026-10-23T06:00:00+00:00"
    assert "snippet" not in article and "sentiment" not in article


def test_partial_sources_are_reported_without_dropping_successful_articles(monkeypatch):
    monkeypatch.setattr(news, "_CONFIG", replace(news._CONFIG, query_mode="google,rss",
                                               rss_sources=["https://other.example/rss"]))

    async def fetch(_fn, url):
        if "other.example" in url:
            raise RuntimeError("private provider failure")
        return FEED

    monkeypatch.setattr(news._BACKOFF, "execute_async", fetch)
    result = asyncio.run(news.get_news_intelligence("ACME.US"))
    assert result["articles_analyzed"] == 1
    evidence = result["collection_evidence"]
    assert (evidence["status"], evidence["succeeded"], evidence["failed"]) == ("PARTIAL", 1, 1)
    assert not archive._collection_report(["ACME.US"], [result], "2026-10-23")["complete"]


def test_no_configured_sources_remains_unavailable(monkeypatch):
    monkeypatch.setattr(news, "_CONFIG", replace(news._CONFIG, query_mode="rss", rss_sources=[]))
    result = asyncio.run(news.get_news_intelligence("ACME.US"))
    assert result["collection_evidence"]["status"] == "UNAVAILABLE"


def test_failed_fetch_is_not_cached_as_empty_news(monkeypatch):
    monkeypatch.setattr(news, "_CONFIG", replace(news._CONFIG, enable_cache=True))
    monkeypatch.setattr(news._CACHE, "get", AsyncMock(return_value=None))
    cache_set = AsyncMock()
    monkeypatch.setattr(news._CACHE, "set", cache_set)
    acquire(monkeypatch, error=RuntimeError("not available"))
    cache_set.assert_not_awaited()


def test_cache_roundtrip_retains_evidence_and_legacy_cache_stays_unknown(monkeypatch):
    result = acquire(monkeypatch, text=FEED)
    assert news.NewsResult.from_dict(result).to_dict()["collection_evidence"] == result["collection_evidence"]
    del result["collection_evidence"]
    restored = news.NewsResult.from_dict(result).to_dict()
    assert archive._collection_status(restored) == "UNKNOWN"


def test_missing_symbol_cannot_make_collection_complete():
    result = {"symbol": "ACME.US", "collection_evidence": {"status": "OK"}}
    report = archive._collection_report(["ACME.US", "MISSING.US"], [result], "2026-10-23")
    assert report["status_counts"] == {"OK": 1, "UNKNOWN": 1}
    assert report["complete"] is False


def test_batch_reports_source_failures_as_errors(monkeypatch):
    monkeypatch.setattr(news._BACKOFF, "execute_async", AsyncMock(side_effect=RuntimeError("failure")))
    batch = asyncio.run(news.batch_news_intelligence([{"symbol": "ACME.US"}]))
    assert batch.meta["status"] == "error"
    assert batch.meta["errors"] == 1
    assert batch.errors == [{"symbol": "ACME.US", "error": "news_collection_error"}]


@pytest.mark.parametrize("status", ["ERROR", "UNAVAILABLE"])
def test_display_exposes_collection_failure_instead_of_absent_news(status):
    summary = news.summarize_for_display({"symbol": "ACME.US", "collection_evidence": {"status": status}})
    assert summary["collection_status"] == status
    assert summary["sentiment_label"] == "Unavailable"
    assert "collection unavailable" in summary["display_line"]
    assert "No recent headlines found" not in summary["display_line"]


def test_display_marks_partial_collection_as_incomplete():
    summary = news.summarize_for_display({"symbol": "ACME.US", "articles_analyzed": 2,
                                        "collection_evidence": {"status": "PARTIAL"}})
    assert summary["display_line"].startswith("News collection incomplete.")
    assert summary["headline_count"] == 2


@pytest.mark.parametrize("initial_status,initial_articles", [
    ("ERROR", 0), ("PARTIAL", 0), ("PARTIAL", 3), ("UNKNOWN", 0), ("UNAVAILABLE", 0)])
def test_collector_does_not_archive_failed_fetch_and_can_retry_later(
        monkeypatch, tmp_path, initial_status, initial_articles):
    calls = []
    written = []

    async def batch(_items, include_articles=False):
        calls.append(include_articles)
        status = initial_status if len(calls) == 1 else "OK"
        count = initial_articles if len(calls) == 1 else 4
        return SimpleNamespace(items=[{"symbol": "ACME.US", "articles_analyzed": count,
                                      "sentiment": 0.8, "collection_evidence": {"status": status}}], meta={})

    class Store:
        sheet_name = "News_Archive"

        def __init__(self, *_args):
            pass

        def is_available(self):
            return True

        def existing_keys_for_date(self, _date):
            return {row[1] for row in written}

        def append(self, rows):
            written.extend(rows)
            return True

    monkeypatch.setattr(archive, "_import_news_module", lambda: SimpleNamespace(batch_news_intelligence=batch))
    monkeypatch.setattr(archive, "NewsArchiveStore", Store)
    args = argparse.Namespace(universe=["ACME.US"], sheet_id="test", dry_run=False,
                              evidence_json=str(tmp_path / "acquisition.json"))
    assert asyncio.run(archive.collect(args)) == 1
    assert written == []
    assert asyncio.run(archive.collect(args)) == 0
    assert len(written) == 1
    assert written[0][3] == 0.8 and written[0][7] == 4
    assert calls == [True, True]
    assert json.loads((tmp_path / "acquisition.json").read_text())["complete"]


def test_sector_join_uses_real_sector_header_not_asset_class_column():
    canonical = ["Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry"]
    values = {
        "Market_Leaders": [canonical, ["ACME.US", "Acme", "EQUITY", "US", "USD", "US", "Industrials"]],
        "Global_Markets": [["Symbol", "Name", "Sector"], ["ACME.US", "Acme", "Other"],
                           ["SECOND.US", "Second", "Technology"]],
    }
    assert archive._sector_map_from_values(values) == {"ACME.US": "Industrials", "SECOND.US": "Technology"}


@pytest.mark.parametrize("grid", [
    [["Symbol", "Name", "Asset Class"], ["ACME.US", "Acme", "EQUITY"]],
    [["Symbol", "Sector", "Sector"], ["ACME.US", "Industrials", "Other"]],
    [["ACME.US", "Acme", "Industrials"]],
])
def test_sector_join_unproven_headers_leave_sector_unknown(grid):
    assert archive._sector_map_from_values({"Global_Markets": grid}) == {}
